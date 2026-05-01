package llc.ethereal.loom.tarantool

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.msgpack.core.MessagePack
import java.io.ByteArrayOutputStream

class MsgPackTest {

    @Test fun `pack and decode primitives`() {
        val buf = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buf)
        MsgPack.packAny(packer, 42L)
        MsgPack.packAny(packer, "hello")
        MsgPack.packAny(packer, true)
        MsgPack.packAny(packer, null)
        packer.flush(); packer.close()

        val pieces = MsgPack.decode(buf.toByteArray())
        assertEquals(4, pieces.size)
        assertEquals(42L, MsgPack.toKotlin(pieces[0]))
        assertEquals("hello", MsgPack.toKotlin(pieces[1]))
        assertEquals(true, MsgPack.toKotlin(pieces[2]))
        assertNull(MsgPack.toKotlin(pieces[3]))
    }

    @Test fun `pack array roundtrip`() {
        val input = listOf(1L, "two", 3.5, listOf("nested", "list"))
        val buf = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buf)
        MsgPack.packArray(packer, input)
        packer.flush(); packer.close()

        val decoded = MsgPack.arrayToList(MsgPack.decode(buf.toByteArray()).first())
        assertEquals(4, decoded.size)
        assertEquals(1L, decoded[0])
        assertEquals("two", decoded[1])
        assertEquals(3.5, decoded[2])
        @Suppress("UNCHECKED_CAST")
        val nested = decoded[3] as List<Any?>
        assertEquals(listOf("nested", "list"), nested)
    }

    @Test fun `pack map roundtrip preserves keys and values`() {
        val input = mapOf("name" to "loom", "version" to 1L, "active" to true)
        val buf = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buf)
        MsgPack.packAny(packer, input)
        packer.flush(); packer.close()

        @Suppress("UNCHECKED_CAST")
        val decoded = MsgPack.toKotlin(MsgPack.decode(buf.toByteArray()).first()) as Map<String, Any?>
        assertEquals("loom", decoded["name"])
        assertEquals(1L, decoded["version"])
        assertEquals(true, decoded["active"])
    }

    @Test fun `mapGet retrieves by integer key`() {
        val buf = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buf)
        packer.packMapHeader(3)
        packer.packInt(0).packInt(100)
        packer.packInt(1).packString("body")
        packer.packInt(2).packBoolean(false)
        packer.flush(); packer.close()

        val v = MsgPack.decode(buf.toByteArray()).first()
        assertEquals(100L, MsgPack.mapGetLong(v, 0L))
        assertEquals("body", MsgPack.mapGetString(v, 1L))
        assertEquals(false, MsgPack.toKotlin(MsgPack.mapGet(v, 2L)))
    }

    @Test fun `bytearray roundtrip via binary`() {
        val bytes = byteArrayOf(0x01, 0x02, 0x7f, 0x80.toByte(), 0xff.toByte())
        val buf = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buf)
        MsgPack.packAny(packer, bytes)
        packer.flush(); packer.close()

        val decoded = MsgPack.toKotlin(MsgPack.decode(buf.toByteArray()).first()) as ByteArray
        assertTrue(bytes.contentEquals(decoded))
    }
}
