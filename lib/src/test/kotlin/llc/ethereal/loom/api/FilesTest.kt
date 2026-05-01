package llc.ethereal.loom.api

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue

/**
 * Unit tests for `Files.Companion.parseTuple`. The parser is `internal` so
 * test code in the same module can call it without reflection or a live
 * Tarantool instance.
 */
class FilesTest {
    private fun parse(input: Any?): FileRevision? = Files.parseTuple(input)

    @Test fun `current row layout is decoded`() {
        // configs_files: (plugin, filename, raw, mtime, size, version)
        val tuple = listOf("bStats", "config.yml", byteArrayOf(1, 2, 3), 1700_000_000L, 3L, 4L)
        val rev = parse(tuple)
        assertNotNull(rev)
        assertEquals("bStats", rev!!.plugin)
        assertEquals("config.yml", rev.filename)
        assertEquals(4L, rev.version)
        assertEquals(3L, rev.size)
        assertEquals(1700_000_000L, rev.mtime)
        assertTrue(byteArrayOf(1, 2, 3).contentEquals(rev.raw))
        assertEquals("", rev.author)
    }

    @Test fun `history row layout is decoded`() {
        // configs_history: (plugin, filename, version, raw, mtime, size, author)
        val tuple = listOf("bStats", "config.yml", 3L, byteArrayOf(9), 1699_000_000L, 1L, "ops@me")
        val rev = parse(tuple)
        assertNotNull(rev)
        assertEquals(3L, rev!!.version)
        assertEquals("ops@me", rev.author)
        assertEquals(1L, rev.size)
        assertTrue(byteArrayOf(9).contentEquals(rev.raw))
    }

    @Test fun `string raw bytes are coerced to ByteArray`() {
        val tuple = listOf("p", "f", "hello", 1L, 5L, 1L)
        val rev = parse(tuple)
        assertNotNull(rev)
        assertTrue("hello".toByteArray().contentEquals(rev!!.raw))
    }

    @Test fun `empty input returns null`() {
        assertNull(parse(null))
        assertNull(parse(emptyList<Any?>()))
        // 5 fields is the legacy pre-versioning shape; we no longer accept it
        assertNull(parse(listOf("p", "f", byteArrayOf(), 0L, 0L)))
    }

    @Test fun `non-list input returns null`() {
        assertNull(parse("not a list"))
        assertNull(parse(42L))
    }
}
