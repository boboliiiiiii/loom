-- llc.ethereal.loom — Tarantool initialization
-- Config storage for Minecraft Paper plugins.

box.cfg{
    listen = 3301,
    memtx_memory = 256 * 1024 * 1024,
    log_level = 5,
}

-- ============================================================================
-- Spaces
-- ============================================================================

box.schema.space.create('configs_files', {
    if_not_exists = true,
})

-- Migration: pre-versioning rows had 5 fields (no `version`). Pad with v=1
-- before locking in the new format so the schema check stays satisfied.
do
    local current_fmt = box.space.configs_files:format()
    if #current_fmt == 0 or #current_fmt < 6 then
        for _, t in box.space.configs_files:pairs() do
            if #t < 6 then
                box.space.configs_files:update(
                    {t.plugin, t.filename},
                    {{'!', 6, 1}}
                )
            end
        end
    end
end

box.space.configs_files:format({
    {name = 'plugin',   type = 'string'},
    {name = 'filename', type = 'string'},
    {name = 'raw',      type = 'any'},
    {name = 'mtime',    type = 'unsigned'},
    {name = 'size',     type = 'unsigned'},
    {name = 'version',  type = 'unsigned'},
})
box.space.configs_files:create_index('pk', {
    if_not_exists = true,
    parts = {'plugin', 'filename'},
})

-- Append-only history of every file revision. Latest copy still lives in
-- `configs_files`; previous revisions land here on every upsert/delete.
box.schema.space.create('configs_history', {
    if_not_exists = true,
    format = {
        {name = 'plugin',   type = 'string'},
        {name = 'filename', type = 'string'},
        {name = 'version',  type = 'unsigned'},
        {name = 'raw',      type = 'any'},
        {name = 'mtime',    type = 'unsigned'},
        {name = 'size',     type = 'unsigned'},
        {name = 'author',   type = 'string'},
    },
})
box.space.configs_history:create_index('pk', {
    if_not_exists = true,
    parts = {'plugin', 'filename', 'version'},
})
box.space.configs_history:create_index('by_plugin_file', {
    if_not_exists = true,
    parts = {'plugin', 'filename'},
    unique = false,
})

box.schema.space.create('configs_tree', {
    if_not_exists = true,
    format = {
        {name = 'plugin',     type = 'string'},
        {name = 'filename',   type = 'string'},
        {name = 'path',       type = 'string'},
        {name = 'value',      type = 'any'},
        {name = 'value_type', type = 'string'},
    },
})
box.space.configs_tree:create_index('pk', {
    if_not_exists = true,
    parts = {'plugin', 'filename', 'path'},
})
box.space.configs_tree:create_index('by_plugin_file', {
    if_not_exists = true,
    parts = {'plugin', 'filename'},
    unique = false,
})

box.schema.space.create('configs_migrations', {
    if_not_exists = true,
    format = {
        {name = 'plugin',     type = 'string'},
        {name = 'version',    type = 'string'},
        {name = 'applied_at', type = 'unsigned'},
    },
})
box.space.configs_migrations:create_index('pk', {
    if_not_exists = true,
    parts = {'plugin', 'version'},
})

-- ============================================================================
-- API
-- ============================================================================

loom = {}

-- ---- Files: raw bytes, consumed by FUSE sidecar ----

function loom.file_get(plugin, filename)
    return box.space.configs_files:get{plugin, filename}
end

function loom.file_upsert(plugin, filename, raw, author)
    author = author or ''
    box.begin()
    local existing = box.space.configs_files:get{plugin, filename}
    local new_version
    if existing ~= nil then
        new_version = existing.version + 1
        box.space.configs_history:replace{
            existing.plugin, existing.filename, existing.version,
            existing.raw, existing.mtime, existing.size, ''
        }
    else
        new_version = 1
    end
    local now = os.time()
    local result = box.space.configs_files:replace{
        plugin, filename, raw, now, #raw, new_version
    }
    -- Stamp the author on the freshly archived predecessor (if any). The
    -- current row stays unstamped until the next upsert promotes it into
    -- history.
    if existing ~= nil and author ~= '' then
        box.space.configs_history:update(
            {existing.plugin, existing.filename, existing.version},
            {{'=', 7, author}}
        )
    end
    box.commit()
    return result
end

function loom.file_delete(plugin, filename)
    box.begin()
    local existing = box.space.configs_files:get{plugin, filename}
    if existing ~= nil then
        box.space.configs_history:replace{
            existing.plugin, existing.filename, existing.version,
            existing.raw, existing.mtime, existing.size, ''
        }
    end
    local result = box.space.configs_files:delete{plugin, filename}
    box.commit()
    return result
end

function loom.file_history(plugin, filename, limit)
    limit = limit or 50
    local result = {}
    -- Newest first via reverse iterator on the secondary index.
    for _, t in box.space.configs_history.index.by_plugin_file:pairs(
        {plugin, filename}, {iterator = box.index.LE}
    ) do
        if t.plugin ~= plugin or t.filename ~= filename then break end
        table.insert(result, t)
        if #result >= limit then break end
    end
    return result
end

function loom.file_get_version(plugin, filename, version)
    local current = box.space.configs_files:get{plugin, filename}
    if current ~= nil and current.version == version then
        return current
    end
    return box.space.configs_history:get{plugin, filename, version}
end

function loom.file_revert(plugin, filename, version, author)
    local target = loom.file_get_version(plugin, filename, version)
    if target == nil then
        error('version not found: ' .. plugin .. '/' .. filename .. '@' .. version)
    end
    return loom.file_upsert(plugin, filename, target.raw, author or ('revert@' .. version))
end

function loom.file_list(plugin)
    return box.space.configs_files:select{plugin}
end

function loom.file_list_plugins()
    local seen, result = {}, {}
    for _, t in box.space.configs_files:pairs() do
        if not seen[t.plugin] then
            seen[t.plugin] = true
            table.insert(result, t.plugin)
        end
    end
    return result
end

-- ---- Tree: structured values, consumed by Loom Kotlin lib ----

function loom.tree_select(plugin, filename)
    return box.space.configs_tree.index.by_plugin_file:select{plugin, filename}
end

function loom.tree_get(plugin, filename, path)
    return box.space.configs_tree:get{plugin, filename, path}
end

function loom.tree_upsert(plugin, filename, path, value, value_type)
    return box.space.configs_tree:replace{plugin, filename, path, value, value_type}
end

function loom.tree_delete(plugin, filename, path)
    return box.space.configs_tree:delete{plugin, filename, path}
end

-- Inserts defaults only for paths that don't exist yet.
-- defaults: array of {path = '...', value = ..., value_type = '...'}
-- Returns count of newly added keys.
function loom.upsert_schema(plugin, filename, defaults)
    local added = 0
    box.begin()
    for _, entry in ipairs(defaults) do
        local existing = box.space.configs_tree:get{plugin, filename, entry.path}
        if existing == nil then
            box.space.configs_tree:insert{
                plugin, filename, entry.path, entry.value, entry.value_type
            }
            added = added + 1
        end
    end
    box.commit()
    return added
end

-- ---- Migration tracking ----

function loom.migration_record(plugin, version)
    return box.space.configs_migrations:replace{plugin, version, os.time()}
end

function loom.migration_check(plugin, version)
    return box.space.configs_migrations:get{plugin, version} ~= nil
end

-- ============================================================================
-- Auth
-- ============================================================================

-- TODO before production:
--   box.schema.user.create('loom', {password = os.getenv('LOOM_DB_PASSWORD')})
--   box.schema.user.grant('loom', 'execute', 'universe')
box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

print('loom: schema initialized')
