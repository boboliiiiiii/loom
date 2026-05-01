package llc.ethereal.loom.tarantool

/**
 * Tarantool iproto wire-protocol constants used by [TarantoolConnection].
 * Source-of-truth: https://github.com/tarantool/tarantool/blob/master/src/box/iproto_constants.h
 */
internal object Iproto {
    // header keys
    const val CODE = 0x00
    const val SYNC = 0x01

    // body keys
    const val DATA = 0x30
    const val ERROR_LEGACY = 0x31
    const val TUPLE = 0x21
    const val FUNCTION_NAME = 0x22
    const val USER_NAME = 0x23
    const val EVENT_KEY = 0x57
    const val EVENT_DATA = 0x58

    // request/response codes
    const val REQ_AUTH = 0x07
    const val REQ_CALL = 0x0a
    const val REQ_WATCH = 0x4a
    const val REQ_UNWATCH = 0x4b
    const val RESP_EVENT = 0x4c

    const val OK = 0L
}
