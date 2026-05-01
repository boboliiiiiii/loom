package llc.ethereal.loom.api

import llc.ethereal.loom.tarantool.TarantoolClient
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Base class for declarative config sections backed by Tarantool.
 *
 * ```
 * object PvpConfig : LoomSection("pvp", "config") {
 *     val maxRange  by config(100).inRange(1..1000)
 *     val cooldown  by config(20).reloadable()
 *     val pvpEnabled by config(true).reloadable()
 *
 *     init {
 *         onChange("cooldown") { new -> rebuildCooldown() }
 *     }
 * }
 *
 * PvpConfig.bind(client)
 * println(PvpConfig.maxRange)
 * ```
 */
abstract class LoomSection(val plugin: String, val file: String = "config") {

    private val log = Logger.getLogger("loom.section.$plugin.$file")
    private val delegates = ConcurrentHashMap<String, LoomDelegate<*>>()
    private val changeHandlers = ConcurrentHashMap<String, MutableList<(Any?) -> Unit>>()
    @Volatile private var bound = false

    fun <T : Any> config(default: T): LoomDelegate<T> = LoomDelegate(this, default, isReloadable = false)

    fun int(default: Int) = config(default)
    fun long(default: Long) = config(default)
    fun bool(default: Boolean) = config(default)
    fun string(default: String) = config(default)
    fun double(default: Double) = config(default)

    internal fun register(name: String, delegate: LoomDelegate<*>) {
        delegates[name] = delegate
    }

    fun onChange(propName: String, handler: (Any?) -> Unit) {
        changeHandlers.computeIfAbsent(propName) { mutableListOf() }.add(handler)
    }

    fun onChange(prop: kotlin.reflect.KProperty<*>, handler: (Any?) -> Unit) {
        onChange(prop.name, handler)
    }

    /** Fields marked `.reloadable()`. */
    fun reloadableFields(): Set<String> =
        delegates.entries.filter { it.value.isReloadable }.map { it.key }.toSet()

    /**
     * Connect to Tarantool, push schema (only inserting NEW defaults — existing values
     * survive), perform initial fetch and subscribe to broadcast updates. Idempotent.
     */
    fun bind(client: TarantoolClient) {
        if (bound) return
        synchronized(this) {
            if (bound) return

            // 1. push schema: defaults for any field that doesn't have a value yet.
            val schema = delegates.map { (name, d) ->
                mapOf(
                    "path" to name,
                    "value" to d.default,
                    "value_type" to d.typeName,
                )
            }
            client.call("loom.upsert_schema", listOf(plugin, file, schema))

            // 2. fetch current state.
            val outer = client.call("loom.tree_select", listOf(plugin, file))
            @Suppress("UNCHECKED_CAST")
            val tree = (outer.firstOrNull() as? List<Any?>) ?: emptyList()
            applyValues(treeToValues(tree))

            // 3. subscribe to subsequent broadcasts.
            val key = "loom.tree:$plugin:$file"
            client.watch(key) { payload ->
                @Suppress("UNCHECKED_CAST")
                val list = payload as? List<Any?> ?: return@watch
                val now = mutableMapOf<String, Any?>()
                for (item in list) {
                    @Suppress("UNCHECKED_CAST")
                    val m = item as? Map<String, Any?> ?: continue
                    val path = m["path"] as? String ?: continue
                    now[path] = m["value"]
                }
                val changed = applyValues(now)
                for (name in changed) {
                    val newVal = delegates[name]?.value
                    changeHandlers[name]?.toList()?.forEach {
                        try { it(newVal) } catch (_: Throwable) {}
                    }
                }
            }

            bound = true
        }
    }

    private fun applyValues(values: Map<String, Any?>): List<String> {
        val changed = mutableListOf<String>()
        for ((name, delegate) in delegates) {
            val raw = values[name] ?: continue
            val coerced = delegate.coerce(raw)
            if (!delegate.isValid(coerced)) {
                log.warning("rejecting value for '$name' (=$coerced): ${delegate.validationMessage}")
                continue
            }
            val cur = delegate.value
            if (cur != coerced) {
                delegate.value = coerced
                changed += name
            }
        }
        return changed
    }

    private fun treeToValues(tree: List<Any?>): Map<String, Any?> {
        val out = mutableMapOf<String, Any?>()
        for (raw in tree) {
            @Suppress("UNCHECKED_CAST")
            val tuple = raw as? List<Any?> ?: continue
            if (tuple.size < 4) continue
            val path = tuple[2] as? String ?: continue
            out[path] = tuple[3]
        }
        return out
    }
}
