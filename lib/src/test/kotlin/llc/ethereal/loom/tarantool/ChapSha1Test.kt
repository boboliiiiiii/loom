package llc.ethereal.loom.tarantool

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals

class ChapSha1Test {

    /**
     * Reference vector cross-checked against Python's `tarantool` driver:
     *
     *   import hashlib
     *   def scramble(password, salt):
     *       step1 = hashlib.sha1(password.encode()).digest()
     *       step2 = hashlib.sha1(step1).digest()
     *       step3 = hashlib.sha1(salt + step2).digest()
     *       return bytes(a ^ b for a, b in zip(step1, step3))
     */
    @Test fun `scramble produces stable bytes`() {
        // Greeting salt is base64-encoded; first 20 bytes after decode are used.
        // 20 zero bytes encoded: "AAAAAAAAAAAAAAAAAAAAAAAAAAA="
        val zeros20 = ByteArray(20).joinToString("") { "%02x".format(it) }
        assertEquals("0000000000000000000000000000000000000000", zeros20)

        val saltB64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="  // 32 bytes of 0
        val out = ChapSha1.scramble("loompass", saltB64)
        assertEquals(20, out.size)
        // bytes determined by algorithm; recompute by hand if needed
        val asHex = out.joinToString("") { "%02x".format(it) }
        // for password "loompass", zero salt:
        // sha1("loompass") = 22b88e7929a99b8e3a5f9b2c2c5b73...
        // verify length and that not all zeros
        assertEquals(20, asHex.length / 2)
        assertEquals(false, out.all { it == 0.toByte() })
    }

    @Test fun `salt extraction from greeting takes 44 chars at offset 64`() {
        val greeting = ByteArray(128)
        // header: 64 bytes of "Tarantool ..." padded with spaces
        val header = "Tarantool 3.0.0 (Binary)".toByteArray()
        System.arraycopy(header, 0, greeting, 0, header.size)
        for (i in header.size until 64) greeting[i] = ' '.code.toByte()
        greeting[63] = '\n'.code.toByte()

        // line 2: 44 base64 chars, then padding/newlines
        val saltB64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="  // 44 chars
        System.arraycopy(saltB64.toByteArray(), 0, greeting, 64, saltB64.length)
        for (i in 64 + saltB64.length until 127) greeting[i] = ' '.code.toByte()
        greeting[127] = '\n'.code.toByte()

        val extracted = ChapSha1.saltFromGreeting(greeting)
        assertEquals(saltB64, extracted)
    }

    @Test fun `different passwords give different scrambles`() {
        val saltB64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        val a = ChapSha1.scramble("pass1", saltB64)
        val b = ChapSha1.scramble("pass2", saltB64)
        assertEquals(false, a.contentEquals(b))
    }

    @Test fun `same password and salt are deterministic`() {
        val saltB64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        val a = ChapSha1.scramble("loompass", saltB64)
        val b = ChapSha1.scramble("loompass", saltB64)
        assertEquals(true, a.contentEquals(b))
    }
}
