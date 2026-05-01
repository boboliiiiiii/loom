package llc.ethereal.loom.tarantool

/**
 * High-level Tarantool client. Wraps a [TarantoolConnection] and exposes
 * `call` and `watch` operations.
 */
class TarantoolClient internal constructor(private val connection: TarantoolConnection) {

    /** Synchronous CALL — blocks the caller until the response arrives (up to 30s). */
    fun call(func: String, args: List<Any?> = emptyList()): List<Any?> {
        val data = connection.sendAndWait({ packer ->
            packer.packMapHeader(2)
            packer.packInt(Iproto.FUNCTION_NAME).packString(func)
            packer.packInt(Iproto.TUPLE)
            MsgPack.packArray(packer, args)
        }, Iproto.REQ_CALL)
        return MsgPack.arrayToList(data)
    }

    fun watch(key: String, handler: (Any?) -> Unit) {
        connection.registerEventHandler(key, handler)
    }

    /**
     * Asynchronous CALL — returns a CompletableFuture completed by the reader
     * thread when the response arrives. Caller must arrange execution context
     * for callbacks (e.g. dispatch to Bukkit main via the scheduler).
     */
    fun callAsync(func: String, args: List<Any?> = emptyList()): java.util.concurrent.CompletableFuture<List<Any?>> {
        return connection.sendAsync({ packer ->
            packer.packMapHeader(2)
            packer.packInt(Iproto.FUNCTION_NAME).packString(func)
            packer.packInt(Iproto.TUPLE)
            MsgPack.packArray(packer, args)
        }, Iproto.REQ_CALL).thenApply { MsgPack.arrayToList(it) }
    }

    fun close() = connection.close()

    companion object {
        @JvmStatic
        @JvmOverloads
        fun connect(host: String, port: Int, user: String? = null, password: String? = null): TarantoolClient {
            val auth = if (user != null && password != null) {
                TarantoolConnection.AuthCredentials(user, password)
            } else null
            return TarantoolClient(TarantoolConnection(host, port, auth))
        }

        @JvmStatic
        @JvmOverloads
        fun connectWithRetry(
            host: String, port: Int,
            user: String? = null, password: String? = null,
            attempts: Int = 30, delayMs: Long = 2000,
        ): TarantoolClient {
            var last: Throwable? = null
            for (i in 1..attempts) {
                try { return connect(host, port, user, password) }
                catch (e: Throwable) { last = e; Thread.sleep(delayMs) }
            }
            throw RuntimeException("Tarantool unreachable", last)
        }
    }
}
