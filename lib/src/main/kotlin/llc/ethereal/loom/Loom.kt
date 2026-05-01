package llc.ethereal.loom

import llc.ethereal.loom.tarantool.TarantoolClient

/**
 * Static entry point for the Loom library.
 *
 * Loom ships as a plain library jar (drop into Paper's `libraries/` folder) — it is
 * NOT a Bukkit plugin. Consumer plugins use [Loom.client] to obtain the connected
 * TarantoolClient and bind their config sections.
 *
 * Connection details come from environment variables:
 *   TARANTOOL_URL  — host:port (default: tarantool.loom.svc.cluster.local:3301)
 *   LOOM_USER      — Tarantool user (optional)
 *   LOOM_PASSWORD  — Tarantool password (optional)
 *
 * Example:
 * ```
 * class MyPlugin : JavaPlugin() {
 *     override fun onEnable() {
 *         MyConfig.bind(Loom.client())
 *     }
 * }
 * ```
 */
object Loom {

    @Volatile private var client: TarantoolClient? = null
    private val lock = Any()

    /** Returns the connected client, lazily connecting on first call. */
    @JvmStatic
    fun client(): TarantoolClient {
        client?.let { return it }
        synchronized(lock) {
            client?.let { return it }
            val c = connect()
            client = c
            return c
        }
    }

    /** Disconnects the shared client. Safe to call multiple times. */
    @JvmStatic
    fun shutdown() {
        synchronized(lock) {
            client?.close()
            client = null
        }
    }

    private fun connect(): TarantoolClient {
        val url = System.getenv("TARANTOOL_URL") ?: "tarantool.loom.svc.cluster.local:3301"
        val user = System.getenv("LOOM_USER")?.takeIf { it.isNotBlank() }
        val pass = System.getenv("LOOM_PASSWORD")?.takeIf { it.isNotBlank() }
        val (host, portStr) = url.split(":", limit = 2)
        return TarantoolClient.connectWithRetry(host, portStr.toInt(), user, pass)
    }
}
