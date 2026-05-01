package llc.ethereal.loom.api

import kotlin.properties.ReadOnlyProperty
import kotlin.reflect.KProperty

/**
 * Property delegate backing a single config field.
 *
 * Lifecycle:
 *  - construction: stores [default], starts with `value = default`
 *  - `provideDelegate` is called by Kotlin on first init, registering this delegate with the parent [LoomSection]
 *  - on `bind`, parent fetches values from Tarantool and writes them into the delegate's `value`
 *  - on each broadcast event, parent re-applies values, optionally validating via [validator]
 */
class LoomDelegate<T : Any> internal constructor(
    private val parent: LoomSection,
    val default: T,
    var isReloadable: Boolean,
) {
    @Volatile internal var value: Any? = default
    internal var validator: Validator<Any?>? = null
    internal var validationMessage: String = "invalid"

    val typeName: String = when (default) {
        is Boolean -> "boolean"
        is Int, is Long, is Short, is Byte -> "integer"
        is Float, is Double -> "number"
        is String -> "string"
        else -> "any"
    }

    /** Mark this field as eligible for hot-reload (it is, by default; we keep this for clarity in the DSL). */
    fun reloadable(): LoomDelegate<T> { isReloadable = true; return this }

    /**
     * Attach a validation predicate. Values failing validation are rejected on apply
     * (logged and reverted), and rejected synchronously when set via [setValue].
     */
    @Suppress("UNCHECKED_CAST")
    fun validate(message: String = "invalid", check: (T) -> Boolean): LoomDelegate<T> {
        validator = { v -> check(v as T) }
        validationMessage = message
        return this
    }

    /** Convenience for ranges. */
    fun inRange(range: IntRange): LoomDelegate<T> = validate("must be in $range") { v ->
        when (v) {
            is Int -> v in range
            is Long -> v in range.first..range.last
            else -> false
        }
    }

    @Suppress("UNCHECKED_CAST")
    operator fun getValue(thisRef: Any?, prop: KProperty<*>): T = (value ?: default) as T

    operator fun provideDelegate(thisRef: LoomSection, prop: KProperty<*>): ReadOnlyProperty<Any?, T> {
        thisRef.register(prop.name, this)
        return object : ReadOnlyProperty<Any?, T> {
            @Suppress("UNCHECKED_CAST")
            override fun getValue(thisRef: Any?, property: KProperty<*>): T = (value ?: default) as T
        }
    }

    /** Coerce a raw incoming value (from Tarantool, often a Long/Double/String/Boolean) to [T]'s class. */
    @Suppress("UNCHECKED_CAST")
    internal fun coerce(raw: Any?): Any? = when (default) {
        is Boolean -> when (raw) {
            is Boolean -> raw; is Number -> raw.toLong() != 0L
            is String -> raw.toBoolean(); else -> false
        }
        is Int -> (raw as? Number)?.toInt() ?: raw.toString().toInt()
        is Long -> (raw as? Number)?.toLong() ?: raw.toString().toLong()
        is Double -> (raw as? Number)?.toDouble() ?: raw.toString().toDouble()
        is Float -> (raw as? Number)?.toFloat() ?: raw.toString().toFloat()
        is String -> raw?.toString() ?: ""
        else -> raw
    }

    /** True if the value passes the validator (or no validator is set). */
    internal fun isValid(value: Any?): Boolean {
        val v = validator ?: return true
        return try { v(value) } catch (_: Throwable) { false }
    }
}
