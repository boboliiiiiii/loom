package llc.ethereal.loom.tarantool

import org.msgpack.core.MessagePack
import org.msgpack.core.MessagePacker
import org.msgpack.value.Value

/**
 * Thin msgpack encode/decode utilities used by the iproto layer.
 * Hides org.msgpack types from higher layers.
 */
internal object MsgPack {

    /** Append [args] as a msgpack array to [packer]. */
    fun packArray(packer: MessagePacker, args: List<Any?>) {
        packer.packArrayHeader(args.size)
        for (a in args) packAny(packer, a)
    }

    fun packAny(packer: MessagePacker, v: Any?) {
        when (v) {
            null -> packer.packNil()
            is Boolean -> packer.packBoolean(v)
            is Byte -> packer.packLong(v.toLong())
            is Short -> packer.packLong(v.toLong())
            is Int -> packer.packLong(v.toLong())
            is Long -> packer.packLong(v)
            is Float -> packer.packDouble(v.toDouble())
            is Double -> packer.packDouble(v)
            is String -> packer.packString(v)
            is ByteArray -> {
                packer.packBinaryHeader(v.size)
                packer.writePayload(v)
            }
            is List<*> -> packArray(packer, v)
            is Map<*, *> -> {
                packer.packMapHeader(v.size)
                for ((k, vv) in v) { packAny(packer, k); packAny(packer, vv) }
            }
            else -> packer.packString(v.toString())
        }
    }

    fun decode(payload: ByteArray, offset: Int = 0, length: Int = payload.size - offset): List<Value> {
        val out = mutableListOf<Value>()
        val unpacker = MessagePack.newDefaultUnpacker(payload, offset, length)
        while (unpacker.hasNext()) out += unpacker.unpackValue()
        return out
    }

    fun mapGet(map: Value?, key: Long): Value? {
        if (map == null || !map.isMapValue) return null
        val kv = map.asMapValue().keyValueArray
        var i = 0
        while (i < kv.size) {
            val k = kv[i]
            if (k.isIntegerValue && k.asIntegerValue().toLong() == key) return kv[i + 1]
            i += 2
        }
        return null
    }

    fun mapGetLong(map: Value?, key: Long): Long {
        val v = mapGet(map, key) ?: return 0L
        return if (v.isIntegerValue) v.asIntegerValue().toLong() else 0L
    }

    fun mapGetString(map: Value?, key: Long): String? {
        val v = mapGet(map, key) ?: return null
        return if (v.isStringValue) v.asStringValue().asString() else v.toString()
    }

    /** Recursive msgpack Value → Kotlin native types. */
    fun toKotlin(v: Value?): Any? = when {
        v == null -> null
        v.isNilValue -> null
        v.isBooleanValue -> v.asBooleanValue().boolean
        v.isIntegerValue -> v.asIntegerValue().toLong()
        v.isFloatValue -> v.asFloatValue().toDouble()
        v.isStringValue -> v.asStringValue().asString()
        v.isBinaryValue -> v.asBinaryValue().asByteArray()
        v.isArrayValue -> v.asArrayValue().list().map { toKotlin(it) }
        v.isMapValue -> {
            val kv = v.asMapValue().keyValueArray
            val r = LinkedHashMap<String, Any?>()
            var i = 0
            while (i < kv.size) {
                val k = (toKotlin(kv[i]) as? String) ?: kv[i].toString()
                r[k] = toKotlin(kv[i + 1])
                i += 2
            }
            r
        }
        else -> v.toString()
    }

    fun arrayToList(v: Value?): List<Any?> =
        if (v == null || !v.isArrayValue) emptyList()
        else v.asArrayValue().list().map { toKotlin(it) }
}
