package llc.ethereal.loom.tarantool

import java.security.MessageDigest
import java.util.Base64

/**
 * CHAP-SHA1 auth scramble used by Tarantool iproto.
 *
 * scramble = SHA1(password) XOR SHA1(salt || SHA1(SHA1(password)))
 * where salt is the first 20 bytes of base64-decoded greeting line 2.
 */
internal object ChapSha1 {

    fun scramble(password: String, base64Salt: String): ByteArray {
        val saltDecoded = Base64.getDecoder().decode(base64Salt)
        val salt = saltDecoded.copyOf(20)
        val sha = MessageDigest.getInstance("SHA-1")

        val step1 = sha.digest(password.toByteArray())
        sha.reset()

        val step2 = sha.digest(step1)
        sha.reset()

        sha.update(salt)
        sha.update(step2)
        val step3 = sha.digest()

        val out = ByteArray(20)
        for (i in 0 until 20) out[i] = (step1[i].toInt() xor step3[i].toInt()).toByte()
        return out
    }

    /** Extract the base64 salt from a 128-byte Tarantool greeting. */
    fun saltFromGreeting(greeting: ByteArray): String {
        require(greeting.size >= 128)
        // Line 2 occupies bytes 64..127, ends with newline-padded base64 (44 chars + spaces).
        // We take chars 64..63+44 and trim spaces.
        return String(greeting, 64, 44).trim()
    }
}
