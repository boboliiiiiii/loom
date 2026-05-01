package llc.ethereal.loom.tarantool

import org.msgpack.core.MessagePack
import org.msgpack.value.Value
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * Owns the TCP connection to a Tarantool instance and demultiplexes responses
 * by sync-id.
 *
 * Auto-reconnect: when the reader thread detects a broken socket, it spawns a
 * background reconnect loop with exponential backoff. Pending in-flight calls
 * are failed (the caller can retry). Active watch subscriptions are re-sent
 * automatically once the new connection is established.
 */
internal class TarantoolConnection(
    private val host: String,
    private val port: Int,
    private val auth: AuthCredentials? = null,
    private val maxBackoffMs: Long = 30_000,
) {
    private val log = Logger.getLogger("loom.tarantool.${host}:${port}")

    @Volatile private var socket: Socket? = null
    @Volatile private var out: DataOutputStream? = null
    @Volatile private var ins: DataInputStream? = null
    @Volatile private var connected: Boolean = false
    @Volatile private var closed: Boolean = false

    private val nextSync = AtomicLong(0)
    private val pending = ConcurrentHashMap<Long, CompletableFuture<Value?>>()
    private val eventHandlers = ConcurrentHashMap<String, CopyOnWriteArrayList<(Any?) -> Unit>>()
    private val sendLock = Any()
    private val reconnectLock = Any()
    @Volatile private var readerThread: Thread? = null

    var salt: String = ""
        private set

    data class AuthCredentials(val user: String, val password: String)

    init {
        connect()
    }

    /** Open a fresh socket, authenticate, start reader. Throws on failure. */
    private fun connect() {
        val sock = Socket(host, port).apply {
            soTimeout = 0
            tcpNoDelay = true
        }
        val outStream = DataOutputStream(sock.getOutputStream())
        val inStream = DataInputStream(sock.getInputStream())

        val greeting = ByteArray(128)
        inStream.readFully(greeting)
        salt = ChapSha1.saltFromGreeting(greeting)

        socket = sock; out = outStream; ins = inStream

        if (auth != null) {
            authenticate(auth.user, auth.password)
        }

        connected = true
        readerThread = Thread({ readerLoop() }, "tarantool-reader-$host:$port").apply {
            isDaemon = true
            start()
        }
    }

    private fun authenticate(user: String, password: String) {
        val scramble = ChapSha1.scramble(password, salt)
        val sync = nextSync.incrementAndGet()

        val buf = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buf)
        packer.packMapHeader(2)
        packer.packInt(Iproto.CODE).packInt(Iproto.REQ_AUTH)
        packer.packInt(Iproto.SYNC).packLong(sync)
        packer.packMapHeader(2)
        packer.packInt(Iproto.USER_NAME).packString(user)
        packer.packInt(Iproto.TUPLE)
        packer.packArrayHeader(2)
        packer.packString("chap-sha1")
        packer.packBinaryHeader(scramble.size); packer.writePayload(scramble)
        packer.flush(); packer.close()

        writeRaw(buf.toByteArray())

        val sizeBuf = ByteArray(5)
        ins!!.readFully(sizeBuf)
        require(sizeBuf[0] == 0xce.toByte()) { "bad size marker after AUTH" }
        val len = ByteBuffer.wrap(sizeBuf, 1, 4).int
        val resp = ByteArray(len)
        ins!!.readFully(resp)
        val pieces = MsgPack.decode(resp)
        val header = pieces.getOrNull(0)
        val body = pieces.getOrNull(1)
        val code = MsgPack.mapGetLong(header, Iproto.CODE.toLong())
        if (code != Iproto.OK) {
            val err = MsgPack.mapGetString(body, Iproto.ERROR_LEGACY.toLong()) ?: "code $code"
            throw RuntimeException("tarantool auth failed: $err")
        }
    }

    private fun readerLoop() {
        try {
            while (!closed) {
                val sizeBuf = ByteArray(5)
                ins!!.readFully(sizeBuf)
                if (sizeBuf[0] != 0xce.toByte()) error("bad size marker: ${sizeBuf[0]}")
                val len = ByteBuffer.wrap(sizeBuf, 1, 4).int
                val resp = ByteArray(len)
                ins!!.readFully(resp)

                val pieces = MsgPack.decode(resp)
                val header = pieces.getOrNull(0)
                val body = pieces.getOrNull(1)

                val code = MsgPack.mapGetLong(header, Iproto.CODE.toLong())
                val sync = MsgPack.mapGetLong(header, Iproto.SYNC.toLong())

                if (code == Iproto.RESP_EVENT.toLong()) {
                    val key = MsgPack.mapGetString(body, Iproto.EVENT_KEY.toLong()) ?: continue
                    val data = MsgPack.mapGet(body, Iproto.EVENT_DATA.toLong())
                    val payload = MsgPack.toKotlin(data)
                    eventHandlers[key]?.toList()?.forEach {
                        try { it(payload) } catch (_: Throwable) {}
                    }
                    sendWatch(key)
                } else {
                    val fut = pending.remove(sync) ?: continue
                    if (code == Iproto.OK) {
                        val data = MsgPack.mapGet(body, Iproto.DATA.toLong())
                        fut.complete(data)
                    } else {
                        val err = MsgPack.mapGetString(body, Iproto.ERROR_LEGACY.toLong()) ?: "code $code"
                        fut.completeExceptionally(RuntimeException("tarantool error: $err"))
                    }
                }
            }
        } catch (e: Throwable) {
            if (!closed) {
                log.warning("reader: connection lost (${e.javaClass.simpleName}: ${e.message}); scheduling reconnect")
                handleConnectionLoss(e)
            }
        }
    }

    /**
     * Mark connection as broken, fail pending CALLs, spawn reconnect thread.
     * Idempotent — multiple losses (reader + sender) all funnel through here.
     */
    private fun handleConnectionLoss(cause: Throwable) {
        synchronized(reconnectLock) {
            if (!connected || closed) return
            connected = false

            try { socket?.close() } catch (_: Throwable) {}

            val toFail = pending.toMap()
            pending.clear()
            for ((_, fut) in toFail) {
                fut.completeExceptionally(RuntimeException("tarantool connection lost: ${cause.message}", cause))
            }

            Thread({ reconnectLoop() }, "tarantool-reconnect-$host:$port").apply {
                isDaemon = true
                start()
            }
        }
    }

    private fun reconnectLoop() {
        var attempt = 0
        var backoff = 500L
        while (!closed) {
            attempt += 1
            try {
                log.info("reconnect attempt #$attempt to $host:$port")
                connect()
                log.info("reconnect succeeded; resubscribing ${eventHandlers.size} watch key(s)")
                for (key in eventHandlers.keys) {
                    try { sendWatch(key) } catch (e: Throwable) {
                        log.warning("re-subscribe failed for '$key': ${e.message}")
                    }
                }
                return
            } catch (e: Throwable) {
                log.warning("reconnect failed (#$attempt): ${e.message}; backoff ${backoff}ms")
                try { Thread.sleep(backoff) } catch (_: InterruptedException) { return }
                backoff = (backoff * 2).coerceAtMost(maxBackoffMs)
            }
        }
    }

    fun sendAsync(buildBody: (org.msgpack.core.MessagePacker) -> Unit, code: Int): CompletableFuture<Value?> {
        val fut = CompletableFuture<Value?>()
        if (closed) { fut.completeExceptionally(RuntimeException("connection closed")); return fut }
        if (!connected) {
            fut.completeExceptionally(RuntimeException("tarantool unavailable; reconnect in progress"))
            return fut
        }

        val sync = nextSync.incrementAndGet()
        pending[sync] = fut

        val buf = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buf)
        packer.packMapHeader(2)
        packer.packInt(Iproto.CODE).packInt(code)
        packer.packInt(Iproto.SYNC).packLong(sync)
        buildBody(packer)
        packer.flush(); packer.close()

        try {
            writeRaw(buf.toByteArray())
        } catch (e: Throwable) {
            pending.remove(sync)
            fut.completeExceptionally(e)
            handleConnectionLoss(e)
        }
        return fut
    }

    fun sendAndWait(buildBody: (org.msgpack.core.MessagePacker) -> Unit, code: Int): Value? {
        val fut = sendAsync(buildBody, code)
        return fut.get(30, java.util.concurrent.TimeUnit.SECONDS)
    }

    fun registerEventHandler(key: String, handler: (Any?) -> Unit) {
        eventHandlers.computeIfAbsent(key) { CopyOnWriteArrayList() }.add(handler)
        if (connected) {
            try { sendWatch(key) } catch (_: Throwable) {} // reconnect loop will re-subscribe
        }
    }

    fun sendWatch(key: String) {
        if (!connected) return
        val sync = nextSync.incrementAndGet()
        val buf = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buf)
        packer.packMapHeader(2)
        packer.packInt(Iproto.CODE).packInt(Iproto.REQ_WATCH)
        packer.packInt(Iproto.SYNC).packLong(sync)
        packer.packMapHeader(1)
        packer.packInt(Iproto.EVENT_KEY).packString(key)
        packer.flush(); packer.close()
        try { writeRaw(buf.toByteArray()) } catch (_: Throwable) {}
    }

    private fun writeRaw(payload: ByteArray) {
        synchronized(sendLock) {
            val o = out ?: throw java.io.IOException("not connected")
            val prefix = ByteBuffer.allocate(5)
            prefix.put(0xce.toByte())
            prefix.putInt(payload.size)
            o.write(prefix.array())
            o.write(payload)
            o.flush()
        }
    }

    fun isConnected(): Boolean = connected && !closed

    fun close() {
        closed = true
        try { socket?.close() } catch (_: Throwable) {}
        pending.values.forEach { it.completeExceptionally(RuntimeException("connection closed")) }
        pending.clear()
    }
}
