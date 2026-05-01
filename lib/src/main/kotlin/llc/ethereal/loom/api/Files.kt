package llc.ethereal.loom.api

import llc.ethereal.loom.tarantool.TarantoolClient

/**
 * Versioned snapshot of a config file as stored in Tarantool. Returned by
 * [Files.history] (one per revision) and [Files.getVersion] (single revision).
 *
 * `raw` is the file contents as bytes — the same payload the FUSE sidecar
 * serves to Paper through `/plugins/<plugin>/<filename>`.
 *
 * @property plugin    plugin namespace
 * @property filename  path inside the plugin dir (may contain `/`)
 * @property version   monotonic version number; the current row carries the
 *                     latest, history rows carry every previous revision
 * @property raw       file contents
 * @property mtime     unix timestamp when this revision was written
 * @property size      `raw.size` (kept as a separate column for index seeks)
 * @property author    free-form author tag stamped by the next upsert; "" for
 *                     the current row (still subject to be overwritten)
 */
data class FileRevision(
    val plugin: String,
    val filename: String,
    val version: Long,
    val raw: ByteArray,
    val mtime: Long,
    val size: Long,
    val author: String,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FileRevision) return false
        return plugin == other.plugin && filename == other.filename &&
            version == other.version && mtime == other.mtime && size == other.size &&
            author == other.author && raw.contentEquals(other.raw)
    }

    override fun hashCode(): Int {
        var h = plugin.hashCode()
        h = 31 * h + filename.hashCode()
        h = 31 * h + version.hashCode()
        h = 31 * h + raw.contentHashCode()
        h = 31 * h + mtime.hashCode()
        h = 31 * h + size.hashCode()
        h = 31 * h + author.hashCode()
        return h
    }
}

/**
 * Versioned file API — thin Kotlin facade over the `loom.file_*` Lua
 * functions. Useful when a plugin wants to inspect or revert a config
 * without touching the FUSE mount directly.
 */
class Files(private val client: TarantoolClient) {

    /** Latest revision of `<plugin>/<filename>`, or null if it doesn't exist. */
    fun get(plugin: String, filename: String): FileRevision? {
        val r = client.call("loom.file_get", listOf(plugin, filename))
        return parseTuple(r.firstOrNull())
    }

    /** Specific revision by version number. */
    fun getVersion(plugin: String, filename: String, version: Long): FileRevision? {
        val r = client.call("loom.file_get_version", listOf(plugin, filename, version))
        return parseTuple(r.firstOrNull())
    }

    /**
     * History of past revisions, newest first, capped at [limit].
     * Does NOT include the current row — query [get] for that.
     */
    @JvmOverloads
    fun history(plugin: String, filename: String, limit: Int = 50): List<FileRevision> {
        val r = client.call("loom.file_history", listOf(plugin, filename, limit))
        // Lua wraps array results in an outer 1-element array; unwrap if so.
        val rows = when (val first = r.firstOrNull()) {
            is List<*> -> first
            else -> r
        }
        return rows.mapNotNull { parseTuple(it) }
    }

    /**
     * Write a new revision, bumping the version. The previous row moves to
     * `configs_history`, optionally stamped with [author] for audit purposes.
     */
    @JvmOverloads
    fun upsert(plugin: String, filename: String, raw: ByteArray, author: String = ""): FileRevision? {
        val r = client.call("loom.file_upsert", listOf(plugin, filename, raw, author))
        return parseTuple(r.firstOrNull())
    }

    /**
     * Make `version` the current revision again. Implemented as an upsert
     * with the historical bytes, so the current version number bumps forward
     * (history is append-only, not rewindable).
     */
    @JvmOverloads
    fun revert(plugin: String, filename: String, version: Long, author: String? = null): FileRevision? {
        val args = if (author != null) listOf(plugin, filename, version, author)
        else listOf<Any?>(plugin, filename, version)
        val r = client.call("loom.file_revert", args)
        return parseTuple(r.firstOrNull())
    }

    /** Delete the current revision. The deleted row is archived to history first. */
    fun delete(plugin: String, filename: String) {
        client.call("loom.file_delete", listOf(plugin, filename))
    }

    companion object {
        /**
         * Parse a tuple coming back from any of the `loom.file_*` Lua functions.
         * Field count discriminates the two storage shapes:
         *   6 fields → configs_files: (plugin, filename, raw, mtime, size, version)
         *   7 fields → configs_history: (plugin, filename, version, raw, mtime, size, author)
         * Visible to test code via `internal`.
         */
        internal fun parseTuple(t: Any?): FileRevision? {
            val fields = t as? List<*> ?: return null
            val plugin = fields.getOrNull(0) as? String ?: return null
            val filename = fields.getOrNull(1) as? String ?: return null
            return when (fields.size) {
                6 -> {
                    val raw = bytesAt(fields, 2) ?: return null
                    FileRevision(
                        plugin = plugin,
                        filename = filename,
                        version = (fields[5] as? Number)?.toLong() ?: 0L,
                        raw = raw,
                        mtime = (fields[3] as? Number)?.toLong() ?: 0L,
                        size = (fields[4] as? Number)?.toLong() ?: raw.size.toLong(),
                        author = "",
                    )
                }
                7 -> {
                    val raw = bytesAt(fields, 3) ?: return null
                    FileRevision(
                        plugin = plugin,
                        filename = filename,
                        version = (fields[2] as? Number)?.toLong() ?: 0L,
                        raw = raw,
                        mtime = (fields[4] as? Number)?.toLong() ?: 0L,
                        size = (fields[5] as? Number)?.toLong() ?: raw.size.toLong(),
                        author = (fields[6] as? String) ?: "",
                    )
                }
                else -> null
            }
        }

        private fun bytesAt(fields: List<*>, idx: Int): ByteArray? = when (val r = fields.getOrNull(idx)) {
            is ByteArray -> r
            is String -> r.toByteArray()
            else -> null
        }
    }
}
