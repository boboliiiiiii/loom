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
    format = {
        {name = 'plugin',   type = 'string'},
        {name = 'filename', type = 'string'},
        {name = 'raw',      type = 'any'},
        {name = 'mtime',    type = 'unsigned'},
        {name = 'size',     type = 'unsigned'},
    },
})
box.space.configs_files:create_index('pk', {
    if_not_exists = true,
    parts = {'plugin', 'filename'},
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

function loom.file_upsert(plugin, filename, raw)
    return box.space.configs_files:replace{plugin, filename, raw, os.time(), #raw}
end

function loom.file_delete(plugin, filename)
    return box.space.configs_files:delete{plugin, filename}
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
