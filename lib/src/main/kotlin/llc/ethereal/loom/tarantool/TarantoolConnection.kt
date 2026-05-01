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

/**
 * Owns the TCP connection to a Tarantool instance and demultiplexes responses
 * by sync-id. Clients above this layer interact via [send], [sendAndWait], and
 * [registerEventHandler].
 */
internal class TarantoolConnection(
    host: String,
    port: Int,
    auth: AuthCredentials? = null,
) {
    private val socket: Socket = Socket(host, port).apply {
        soTimeout = 0
        tcpNoDelay = true
    }
    private val out = DataOutputStream(socket.getOutputStream())
    private val ins = DataInputStream(socket.getInputStream())
    private val nextSync = AtomicLong(0)
    private val pending = ConcurrentHashMap<Long, CompletableFuture<Value?>>()
    private val eventHandlers = ConcurrentHashMap<String, CopyOnWriteArrayList<(Any?) -> Unit>>()
    @Volatile private var closed = false

    val salt: String

    data class AuthCredentials(val user: String, val password: String)

    init {
        val greeting = ByteArray(128)
        ins.readFully(greeting)
        salt = ChapSha1.saltFromGreeting(greeting)

        if (auth != null) {
            authenticate(auth.user, auth.password)
        }

        Thread({ readerLoop() }, "tarantool-reader").apply {
            isDaemon = true
            start()
        }
    }

    private fun authenticate(user: String, password: String) {
        // We are still synchronous here (reader thread not started yet).
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

        sendPacketSynchronized(buf.toByteArray())

        // Read response inline (no reader thread yet)
        val sizeBuf = ByteArray(5)
        ins.readFully(sizeBuf)
        require(sizeBuf[0] == 0xce.toByte()) { "bad size marker after AUTH" }
        val len = ByteBuffer.wrap(sizeBuf, 1, 4).int
        val resp = ByteArray(len)
        ins.readFully(resp)
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
                ins.readFully(sizeBuf)
                if (sizeBuf[0] != 0xce.toByte()) error("bad size marker: ${sizeBuf[0]}")
                val len = ByteBuffer.wrap(sizeBuf, 1, 4).int
                val resp = ByteArray(len)
                ins.readFully(resp)

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
                    // ACK by re-sending WATCH on the same key
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
                pending.values.forEach { it.completeExceptionally(e) }
                pending.clear()
            }
        }
    }

    /** Async variant: returns the IPROTO_DATA Value as a future, no blocking. */
    fun sendAsync(buildBody: (org.msgpack.core.MessagePacker) -> Unit, code: Int): CompletableFuture<Value?> {
        val sync = nextSync.incrementAndGet()
        val fut = CompletableFuture<Value?>()
        pending[sync] = fut

        val buf = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buf)
        packer.packMapHeader(2)
        packer.packInt(Iproto.CODE).packInt(code)
        packer.packInt(Iproto.SYNC).packLong(sync)
        buildBody(packer)
        packer.flush(); packer.close()

        try {
            sendPacketSynchronized(buf.toByteArray())
        } catch (e: Throwable) {
            pending.remove(sync)
            fut.completeExceptionally(e)
        }
        return fut
    }

    /** Sync wrapper around [sendAsync] used by AUTH and synchronous CALLs. */
    fun sendAndWait(buildBody: (org.msgpack.core.MessagePacker) -> Unit, code: Int): Value? {
        val fut = sendAsync(buildBody, code)
        return try {
            fut.get(30, java.util.concurrent.TimeUnit.SECONDS)
        } catch (e: Exception) {
            // future already removed from pending if sendAsync failed; otherwise leave for reader to settle
            throw e
        }
    }

    fun registerEventHandler(key: String, handler: (Any?) -> Unit) {
        eventHandlers.computeIfAbsent(key) { CopyOnWriteArrayList() }.add(handler)
        sendWatch(key)
    }

    fun sendWatch(key: String) {
        val sync = nextSync.incrementAndGet()
        val buf = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buf)
        packer.packMapHeader(2)
        packer.packInt(Iproto.CODE).packInt(Iproto.REQ_WATCH)
        packer.packInt(Iproto.SYNC).packLong(sync)
        packer.packMapHeader(1)
        packer.packInt(Iproto.EVENT_KEY).packString(key)
        packer.flush(); packer.close()
        sendPacketSynchronized(buf.toByteArray())
    }

    @Synchronized
    private fun sendPacketSynchronized(payload: ByteArray) {
        val prefix = ByteBuffer.allocate(5)
        prefix.put(0xce.toByte())
        prefix.putInt(payload.size)
        out.write(prefix.array())
        out.write(payload)
        out.flush()
    }

    fun close() {
        closed = true
        try { socket.close() } catch (_: Exception) {}
    }
}
