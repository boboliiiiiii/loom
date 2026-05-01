//! loom-sidecar — FUSE filesystem for Minecraft Paper plugin directory.
//!
//! Routing rules at the mount root (e.g. /plugins):
//!   - Plain directory names (e.g. `bStats`) → Tarantool `configs_files`.
//!   - Files at the root level (e.g. `loom-demo.jar`) → real disk passthrough.
//!   - Directories starting with `.` or `tmp-` → real disk passthrough (Paper internals).
//!
//! Inside Tarantool plugin dirs, nested subdirs are virtual (filename has `/`).
//! Inside passthrough dirs, everything is real FS.
//!
//! Concurrency: state is split across two short-lived mutexes (`InodeState`
//! and `open_files`) so Tarantool I/O *never* runs under a FS lock. The lock
//! is taken once to read the inode entry, dropped before any iproto call,
//! and re-taken briefly to commit the result. This lets multi-threaded
//! fuser configurations (or just a slow Tarantool) avoid serializing the
//! whole filesystem behind a single Mutex.

mod tarantool;

use anyhow::Result;
use fuser::{
    BsdFileFlags, FileAttr, FileHandle, FileType, Filesystem, INodeNo, LockOwner, MountOption,
    OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use libc::{EIO, ENODATA, ENOENT, ENOTDIR, EROFS};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info};

const TTL: Duration = Duration::from_secs(1);
const ROOT_INO: u64 = 1;
const FIRST_INO: u64 = 100;

#[derive(Debug, Clone)]
enum Entry {
    Root,
    Dir { plugin: String, prefix: String },
    File { plugin: String, filename: String },
    Pass { real: PathBuf, is_dir: bool },
}

struct WriteBuf {
    bytes: Vec<u8>,
    plugin: String,
    filename: String,
    pass_real: Option<PathBuf>,
    dirty: bool,
}

/// Inode/path bookkeeping. Held under [LoomFS::state] in a short critical
/// section — never across an iproto call.
struct InodeState {
    inodes: HashMap<u64, Entry>,
    paths: HashMap<String, u64>,
    next_ino: u64,
    next_fh: u64,
    ghost_dirs: HashSet<(String, String)>,
}

impl InodeState {
    fn new() -> Self {
        let mut s = Self {
            inodes: HashMap::new(),
            paths: HashMap::new(),
            next_ino: FIRST_INO,
            next_fh: 1,
            ghost_dirs: HashSet::new(),
        };
        s.inodes.insert(ROOT_INO, Entry::Root);
        s.paths.insert("/".into(), ROOT_INO);
        s
    }

    fn alloc_inode(&mut self, path: &str, entry: Entry) -> u64 {
        if let Some(&i) = self.paths.get(path) {
            self.inodes.insert(i, entry);
            return i;
        }
        let i = self.next_ino;
        self.next_ino += 1;
        self.inodes.insert(i, entry);
        self.paths.insert(path.to_string(), i);
        i
    }

    fn next_fh(&mut self) -> u64 {
        let fh = self.next_fh;
        self.next_fh += 1;
        fh
    }
}

struct LoomFS {
    api: Arc<tarantool::LoomApi>,
    rt: tokio::runtime::Handle,
    pass_root: Arc<PathBuf>,
    state: Mutex<InodeState>,
    open_files: Mutex<HashMap<u64, WriteBuf>>,
}

fn is_passthrough_root_name(name: &str) -> bool {
    name.starts_with('.') || name.starts_with("tmp-")
}

fn ino_u64(ino: INodeNo) -> u64 {
    ino.0
}
fn fh_u64(fh: FileHandle) -> u64 {
    fh.0
}

impl LoomFS {
    fn new(api: Arc<tarantool::LoomApi>, rt: tokio::runtime::Handle, pass_root: PathBuf) -> Self {
        if !pass_root.exists() {
            let _ = fs::create_dir_all(&pass_root);
        }
        Self {
            api,
            rt,
            pass_root: Arc::new(pass_root),
            state: Mutex::new(InodeState::new()),
            open_files: Mutex::new(HashMap::new()),
        }
    }

    // === Tarantool wrappers — never hold any lock here. ===

    fn list_plugins(&self) -> Vec<String> {
        let api = Arc::clone(&self.api);
        self.rt
            .block_on(async move { api.list_plugins().await })
            .unwrap_or_default()
    }

    fn list_files(&self, plugin: &str) -> Vec<tarantool::FileMeta> {
        let api = Arc::clone(&self.api);
        let plugin = plugin.to_string();
        self.rt
            .block_on(async move { api.list_files(&plugin).await })
            .unwrap_or_default()
    }

    fn get_file(&self, plugin: &str, filename: &str) -> Option<tarantool::FileEntry> {
        let api = Arc::clone(&self.api);
        let plugin = plugin.to_string();
        let filename = filename.to_string();
        self.rt
            .block_on(async move { api.get_file(&plugin, &filename).await })
            .ok()
            .flatten()
    }

    fn upsert_file(&self, plugin: &str, filename: &str, raw: &[u8]) -> Result<()> {
        let api = Arc::clone(&self.api);
        let plugin = plugin.to_string();
        let filename = filename.to_string();
        let raw = raw.to_vec();
        self.rt
            .block_on(async move { api.upsert_file(&plugin, &filename, &raw).await })
    }

    fn delete_file(&self, plugin: &str, filename: &str) -> Result<()> {
        let api = Arc::clone(&self.api);
        let plugin = plugin.to_string();
        let filename = filename.to_string();
        self.rt
            .block_on(async move { api.delete_file(&plugin, &filename).await })
    }

    // === Inode state helpers — brief lock, no I/O. ===

    fn get_inode(&self, ino: u64) -> Option<Entry> {
        self.state.lock().unwrap().inodes.get(&ino).cloned()
    }

    fn lookup_path(&self, path: &str) -> Option<u64> {
        self.state.lock().unwrap().paths.get(path).copied()
    }

    fn alloc_inode(&self, path: &str, entry: Entry) -> u64 {
        self.state.lock().unwrap().alloc_inode(path, entry)
    }

    fn remove_path(&self, path: &str) {
        let mut s = self.state.lock().unwrap();
        if let Some(ino) = s.paths.remove(path) {
            s.inodes.remove(&ino);
        }
    }

    fn add_ghost_dir(&self, plugin: &str, dir: &str) {
        self.state
            .lock()
            .unwrap()
            .ghost_dirs
            .insert((plugin.to_string(), dir.to_string()));
    }

    fn remove_ghost_dir(&self, plugin: &str, dir: &str) {
        self.state
            .lock()
            .unwrap()
            .ghost_dirs
            .remove(&(plugin.to_string(), dir.to_string()));
    }

    fn has_ghost_dir(&self, plugin: &str, dir: &str) -> bool {
        self.state
            .lock()
            .unwrap()
            .ghost_dirs
            .contains(&(plugin.to_string(), dir.to_string()))
    }

    fn ghost_dirs_under(&self, plugin: &str, prefix: &str) -> Vec<String> {
        let s = self.state.lock().unwrap();
        s.ghost_dirs
            .iter()
            .filter(|(p, d)| p == plugin && d.starts_with(prefix))
            .map(|(_, d)| d.clone())
            .collect()
    }

    fn ghost_dirs_for_plugin(&self, plugin: &str) -> Vec<String> {
        let s = self.state.lock().unwrap();
        s.ghost_dirs
            .iter()
            .filter(|(p, _)| p == plugin)
            .map(|(_, d)| d.clone())
            .collect()
    }

    fn next_fh(&self) -> u64 {
        self.state.lock().unwrap().next_fh()
    }

    // === Composite operations. ===

    /// Merge Tarantool file list with locally-tracked ghost dirs into a flat
    /// list of `(name, is_file)` children of `prefix` inside `plugin`.
    fn list_dir_children(&self, plugin: &str, prefix: &str) -> Vec<(String, bool)> {
        let pws = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", prefix)
        };
        let files = self.list_files(plugin); // no lock held
        let ghost_dirs = self.ghost_dirs_for_plugin(plugin); // brief lock

        let mut seen: HashSet<String> = HashSet::new();
        let mut out: Vec<(String, bool)> = Vec::new();
        for f in &files {
            if !f.filename.starts_with(&pws) && !pws.is_empty() {
                continue;
            }
            let rest = &f.filename[pws.len()..];
            if rest.is_empty() {
                continue;
            }
            match rest.find('/') {
                None => {
                    if seen.insert(format!("F:{}", rest)) {
                        out.push((rest.to_string(), true));
                    }
                }
                Some(idx) => {
                    let dirname = &rest[..idx];
                    if seen.insert(format!("D:{}", dirname)) {
                        out.push((dirname.to_string(), false));
                    }
                }
            }
        }
        for gd in &ghost_dirs {
            let parent = match gd.rfind('/') {
                Some(i) => &gd[..i],
                None => "",
            };
            if parent == prefix {
                let child = match gd.rfind('/') {
                    Some(i) => &gd[i + 1..],
                    None => gd.as_str(),
                };
                if seen.insert(format!("D:{}", child)) {
                    out.push((child.to_string(), false));
                }
            }
        }
        out
    }

    /// Flush a dirty open file to its backing store. Locks are released
    /// around the actual upsert so concurrent fh's don't pile up.
    fn persist_fh(&self, fh: u64) -> Result<()> {
        // Phase 1: snapshot under open_files lock.
        let snapshot = {
            let of = self.open_files.lock().unwrap();
            match of.get(&fh) {
                Some(b) if b.dirty => Some((
                    b.pass_real.clone(),
                    b.plugin.clone(),
                    b.filename.clone(),
                    b.bytes.clone(),
                )),
                _ => None,
            }
        };
        let (pass_real, plugin, filename, bytes) = match snapshot {
            Some(s) => s,
            None => return Ok(()),
        };

        // Phase 2: I/O without any lock.
        if let Some(real) = pass_real {
            if let Some(parent) = real.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let mut f = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&real)?;
            f.write_all(&bytes)?;
            f.flush()?;
        } else {
            self.upsert_file(&plugin, &filename, &bytes)?;
            // Drop ghost-dir prefixes that were rooted at this file's path.
            let mut s = self.state.lock().unwrap();
            s.ghost_dirs
                .retain(|(p, d)| !(p == &plugin && filename.starts_with(&format!("{}/", d))));
        }

        // Phase 3: clear dirty flag.
        let mut of = self.open_files.lock().unwrap();
        if let Some(b) = of.get_mut(&fh) {
            b.dirty = false;
        }
        Ok(())
    }
}

fn join_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", prefix, name)
    }
}

fn fs_path_for(plugin: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        format!("/{}", plugin)
    } else {
        format!("/{}/{}", plugin, prefix)
    }
}

fn pass_fs_path(real: &Path) -> String {
    format!("//pass{}", real.display())
}

fn dir_attr(ino: u64) -> FileAttr {
    FileAttr {
        ino: INodeNo(ino),
        size: 0,
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: FileType::Directory,
        perm: 0o755,
        nlink: 2,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn file_attr(ino: u64, size: u64, mtime_secs: u64) -> FileAttr {
    let mtime = UNIX_EPOCH + Duration::from_secs(mtime_secs);
    FileAttr {
        ino: INodeNo(ino),
        size,
        blocks: size.div_ceil(512),
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: mtime,
        kind: FileType::RegularFile,
        perm: 0o644,
        nlink: 1,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn pass_attr(ino: u64, real: &Path) -> Option<FileAttr> {
    let m = fs::symlink_metadata(real).ok()?;
    let ftype = if m.is_dir() {
        FileType::Directory
    } else {
        FileType::RegularFile
    };
    let perm = (m.permissions().mode() & 0o7777) as u16;
    let mtime = m.modified().unwrap_or(UNIX_EPOCH);
    Some(FileAttr {
        ino: INodeNo(ino),
        size: m.size(),
        blocks: m.blocks(),
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: mtime,
        kind: ftype,
        perm,
        nlink: m.nlink() as u32,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    })
}

impl Filesystem for LoomFS {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let parent = ino_u64(parent);
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(fuser::Errno::from_i32(ENOENT));
                return;
            }
        };
        debug!(parent, name = name_str, "lookup");

        let parent_entry = self.get_inode(parent);
        match parent_entry {
            Some(Entry::Root) => {
                let path = format!("/{}", name_str);

                // Check inode cache first.
                if let Some(ino) = self.lookup_path(&path)
                    && let Some(entry) = self.get_inode(ino)
                {
                    match entry {
                        Entry::Dir { .. } => {
                            reply.entry(&TTL, &dir_attr(ino), fuser::Generation(0));
                            return;
                        }
                        Entry::Pass { real, .. } => {
                            if let Some(attr) = pass_attr(ino, &real) {
                                reply.entry(&TTL, &attr, fuser::Generation(0));
                                return;
                            }
                        }
                        _ => {}
                    }
                }

                // Try passthrough disk.
                let real = self.pass_root.join(name_str);
                if let Ok(meta) = fs::symlink_metadata(&real) {
                    let is_dir = meta.is_dir();
                    let ino = self.alloc_inode(
                        &path,
                        Entry::Pass {
                            real: real.clone(),
                            is_dir,
                        },
                    );
                    if let Some(attr) = pass_attr(ino, &real) {
                        reply.entry(&TTL, &attr, fuser::Generation(0));
                        return;
                    }
                }

                // Fallback to Tarantool plugin list (no lock held during the call).
                let plugins = self.list_plugins();
                if plugins.iter().any(|p| p == name_str) {
                    let ino = self.alloc_inode(
                        &path,
                        Entry::Dir {
                            plugin: name_str.to_string(),
                            prefix: String::new(),
                        },
                    );
                    reply.entry(&TTL, &dir_attr(ino), fuser::Generation(0));
                } else {
                    reply.error(fuser::Errno::from_i32(ENOENT));
                }
            }
            Some(Entry::Dir { plugin, prefix }) => {
                let child_path = join_path(&prefix, name_str);

                // Try as a file (Tarantool call without lock).
                if let Some(entry) = self.get_file(&plugin, &child_path) {
                    let fp = fs_path_for(&plugin, &child_path);
                    let ino = self.alloc_inode(
                        &fp,
                        Entry::File {
                            plugin: plugin.clone(),
                            filename: child_path,
                        },
                    );
                    reply.entry(
                        &TTL,
                        &file_attr(ino, entry.meta.size, entry.meta.mtime),
                        fuser::Generation(0),
                    );
                    return;
                }

                // Try as a virtual directory.
                let pwc = format!("{}/", child_path);
                let files = self.list_files(&plugin);
                let is_real_dir = files.iter().any(|f| f.filename.starts_with(&pwc));
                let is_ghost = self.has_ghost_dir(&plugin, &child_path);
                if is_real_dir || is_ghost {
                    let fp = fs_path_for(&plugin, &child_path);
                    let ino = self.alloc_inode(
                        &fp,
                        Entry::Dir {
                            plugin,
                            prefix: child_path,
                        },
                    );
                    reply.entry(&TTL, &dir_attr(ino), fuser::Generation(0));
                } else {
                    reply.error(fuser::Errno::from_i32(ENOENT));
                }
            }
            Some(Entry::Pass {
                real: parent_real,
                is_dir: true,
            }) => {
                let real = parent_real.join(name_str);
                if fs::symlink_metadata(&real).is_ok() {
                    let is_dir = real.is_dir();
                    let key = pass_fs_path(&real);
                    let ino = self.alloc_inode(
                        &key,
                        Entry::Pass {
                            real: real.clone(),
                            is_dir,
                        },
                    );
                    if let Some(attr) = pass_attr(ino, &real) {
                        reply.entry(&TTL, &attr, fuser::Generation(0));
                        return;
                    }
                }
                reply.error(fuser::Errno::from_i32(ENOENT));
            }
            Some(Entry::Pass { is_dir: false, .. }) | Some(Entry::File { .. }) => {
                reply.error(fuser::Errno::from_i32(ENOTDIR))
            }
            None => reply.error(fuser::Errno::from_i32(ENOENT)),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let ino = ino_u64(ino);
        let entry = self.get_inode(ino);
        match entry {
            Some(Entry::File { plugin, filename }) => {
                if let Some(new_size) = size {
                    let mut bytes = self
                        .get_file(&plugin, &filename)
                        .map(|e| e.raw)
                        .unwrap_or_default();
                    bytes.resize(new_size as usize, 0);
                    let _ = self.upsert_file(&plugin, &filename, &bytes);

                    // Propagate size to in-flight write buffers (separate lock).
                    let mut of = self.open_files.lock().unwrap();
                    for buf in of.values_mut() {
                        if buf.pass_real.is_none()
                            && buf.plugin == plugin
                            && buf.filename == filename
                        {
                            buf.bytes.resize(new_size as usize, 0);
                            buf.dirty = true;
                        }
                    }
                }
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let sz = self
                    .get_file(&plugin, &filename)
                    .map(|e| e.meta.size)
                    .unwrap_or(0);
                reply.attr(&TTL, &file_attr(ino, sz, now));
            }
            Some(Entry::Pass {
                real,
                is_dir: false,
            }) => {
                if let Some(new_size) = size {
                    if let Ok(f) = fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(&real)
                    {
                        let _ = f.set_len(new_size);
                    }
                    let mut of = self.open_files.lock().unwrap();
                    for buf in of.values_mut() {
                        if buf.pass_real.as_deref() == Some(real.as_path()) {
                            buf.bytes.resize(new_size as usize, 0);
                            buf.dirty = true;
                        }
                    }
                }
                if let Some(attr) = pass_attr(ino, &real) {
                    reply.attr(&TTL, &attr);
                } else {
                    reply.error(fuser::Errno::from_i32(ENOENT));
                }
            }
            Some(Entry::Pass { is_dir: true, real }) => {
                if let Some(attr) = pass_attr(ino, &real) {
                    reply.attr(&TTL, &attr);
                } else {
                    reply.attr(&TTL, &dir_attr(ino));
                }
            }
            Some(Entry::Root) | Some(Entry::Dir { .. }) => reply.attr(&TTL, &dir_attr(ino)),
            None => reply.error(fuser::Errno::from_i32(ENOENT)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let ino = ino_u64(ino);
        let entry = self.get_inode(ino);
        match entry {
            Some(Entry::Root) | Some(Entry::Dir { .. }) => reply.attr(&TTL, &dir_attr(ino)),
            Some(Entry::File { plugin, filename }) => {
                if let Some(e) = self.get_file(&plugin, &filename) {
                    reply.attr(&TTL, &file_attr(ino, e.meta.size, e.meta.mtime));
                } else {
                    reply.error(fuser::Errno::from_i32(ENOENT));
                }
            }
            Some(Entry::Pass { real, .. }) => {
                if let Some(attr) = pass_attr(ino, &real) {
                    reply.attr(&TTL, &attr);
                } else {
                    reply.error(fuser::Errno::from_i32(ENOENT));
                }
            }
            None => reply.error(fuser::Errno::from_i32(ENOENT)),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let ino = ino_u64(ino);
        let entry = self.get_inode(ino);

        let mut entries: Vec<(u64, FileType, String)> = vec![
            (ino, FileType::Directory, ".".into()),
            (ino, FileType::Directory, "..".into()),
        ];
        match entry {
            Some(Entry::Root) => {
                // Tarantool listing without lock.
                for plugin in self.list_plugins() {
                    let path = format!("/{}", plugin);
                    let i = self.alloc_inode(
                        &path,
                        Entry::Dir {
                            plugin: plugin.clone(),
                            prefix: String::new(),
                        },
                    );
                    entries.push((i, FileType::Directory, plugin));
                }
                let pass_root = Arc::clone(&self.pass_root);
                if let Ok(rd) = fs::read_dir(pass_root.as_path()) {
                    for de in rd.flatten() {
                        let name = de.file_name().to_string_lossy().to_string();
                        let real = de.path();
                        let is_dir = real.is_dir();
                        let key = format!("/{}", name);
                        let i = self.alloc_inode(&key, Entry::Pass { real, is_dir });
                        let kind = if is_dir {
                            FileType::Directory
                        } else {
                            FileType::RegularFile
                        };
                        entries.push((i, kind, name));
                    }
                }
            }
            Some(Entry::Dir { plugin, prefix }) => {
                for (name, is_file) in self.list_dir_children(&plugin, &prefix) {
                    let child = join_path(&prefix, &name);
                    let fp = fs_path_for(&plugin, &child);
                    if is_file {
                        let i = self.alloc_inode(
                            &fp,
                            Entry::File {
                                plugin: plugin.clone(),
                                filename: child,
                            },
                        );
                        entries.push((i, FileType::RegularFile, name));
                    } else {
                        let i = self.alloc_inode(
                            &fp,
                            Entry::Dir {
                                plugin: plugin.clone(),
                                prefix: child,
                            },
                        );
                        entries.push((i, FileType::Directory, name));
                    }
                }
            }
            Some(Entry::Pass { real, is_dir: true }) => {
                if let Ok(rd) = fs::read_dir(&real) {
                    for de in rd.flatten() {
                        let name = de.file_name().to_string_lossy().to_string();
                        let p = de.path();
                        let is_dir = p.is_dir();
                        let key = pass_fs_path(&p);
                        let i = self.alloc_inode(&key, Entry::Pass { real: p, is_dir });
                        let kind = if is_dir {
                            FileType::Directory
                        } else {
                            FileType::RegularFile
                        };
                        entries.push((i, kind, name));
                    }
                }
            }
            _ => {
                reply.error(fuser::Errno::from_i32(ENOTDIR));
                return;
            }
        }
        for (i, e) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(e.0), (i + 1) as u64, e.1, &e.2) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let ino = ino_u64(ino);
        let flags_raw: i32 = unsafe { std::mem::transmute::<OpenFlags, i32>(flags) };
        let truncate = (flags_raw & libc::O_TRUNC) != 0;
        let entry = self.get_inode(ino);

        match entry {
            Some(Entry::File { plugin, filename }) => {
                let bytes = if truncate {
                    Vec::new()
                } else {
                    self.get_file(&plugin, &filename)
                        .map(|e| e.raw)
                        .unwrap_or_default()
                };
                let fh = self.next_fh();
                self.open_files.lock().unwrap().insert(
                    fh,
                    WriteBuf {
                        bytes,
                        plugin,
                        filename,
                        pass_real: None,
                        dirty: truncate,
                    },
                );
                reply.opened(FileHandle(fh), fuser::FopenFlags::empty());
            }
            Some(Entry::Pass {
                real,
                is_dir: false,
            }) => {
                let bytes = if truncate {
                    Vec::new()
                } else {
                    fs::read(&real).unwrap_or_default()
                };
                let fh = self.next_fh();
                self.open_files.lock().unwrap().insert(
                    fh,
                    WriteBuf {
                        bytes,
                        plugin: String::new(),
                        filename: real.display().to_string(),
                        pass_real: Some(real),
                        dirty: truncate,
                    },
                );
                reply.opened(FileHandle(fh), fuser::FopenFlags::empty());
            }
            _ => reply.error(fuser::Errno::from_i32(ENOENT)),
        }
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let fh = fh_u64(fh);
        let of = self.open_files.lock().unwrap();
        let buf = match of.get(&fh) {
            Some(b) => b,
            None => {
                reply.error(fuser::Errno::from_i32(EIO));
                return;
            }
        };
        let off = offset as usize;
        if off >= buf.bytes.len() {
            reply.data(&[]);
            return;
        }
        let end = (off + size as usize).min(buf.bytes.len());
        reply.data(&buf.bytes[off..end]);
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _wf: WriteFlags,
        _flags: OpenFlags,
        _lo: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let fh = fh_u64(fh);
        let mut of = self.open_files.lock().unwrap();
        let buf = match of.get_mut(&fh) {
            Some(b) => b,
            None => {
                reply.error(fuser::Errno::from_i32(EIO));
                return;
            }
        };
        let off = offset as usize;
        let end = off + data.len();
        if end > buf.bytes.len() {
            buf.bytes.resize(end, 0);
        }
        buf.bytes[off..end].copy_from_slice(data);
        buf.dirty = true;
        reply.written(data.len() as u32);
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock: LockOwner,
        reply: ReplyEmpty,
    ) {
        let fh = fh_u64(fh);
        if self.persist_fh(fh).is_err() {
            reply.error(fuser::Errno::from_i32(EIO));
            return;
        }
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lo: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let fh = fh_u64(fh);
        let _ = self.persist_fh(fh);
        self.open_files.lock().unwrap().remove(&fh);
        reply.ok();
    }

    /// We don't store extended attributes. Return ENODATA so the kernel knows
    /// "no such xattr on this file" instead of "FS doesn't support xattrs" —
    /// otherwise fuser logs a WARN on every getxattr (which Linux fires on
    /// every open() to check security.capability).
    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(fuser::Errno::from_i32(ENODATA));
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: ReplyXattr) {
        reply.size(0);
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let parent = ino_u64(parent);
        let name_str = match name.to_str() {
            Some(s) => s.to_string(),
            None => {
                reply.error(fuser::Errno::from_i32(EIO));
                return;
            }
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let parent_entry = self.get_inode(parent);

        match parent_entry {
            Some(Entry::Root) => {
                let real = self.pass_root.join(&name_str);
                if let Some(p) = real.parent() {
                    let _ = fs::create_dir_all(p);
                }
                if let Err(e) = fs::File::create(&real) {
                    error!(error = %e, "pass create");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                let key = format!("/{}", name_str);
                let ino = self.alloc_inode(
                    &key,
                    Entry::Pass {
                        real: real.clone(),
                        is_dir: false,
                    },
                );
                let attr = pass_attr(ino, &real).unwrap_or_else(|| file_attr(ino, 0, now));
                let fh = self.next_fh();
                self.open_files.lock().unwrap().insert(
                    fh,
                    WriteBuf {
                        bytes: Vec::new(),
                        plugin: String::new(),
                        filename: real.display().to_string(),
                        pass_real: Some(real),
                        dirty: false,
                    },
                );
                reply.created(
                    &TTL,
                    &attr,
                    fuser::Generation(0),
                    FileHandle(fh),
                    fuser::FopenFlags::empty(),
                );
            }
            Some(Entry::Dir { plugin, prefix }) => {
                let filename = join_path(&prefix, &name_str);
                if let Err(e) = self.upsert_file(&plugin, &filename, &[]) {
                    error!(error = %e, "create upsert");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                // Drop ghost-dirs that just became real prefixes of this file.
                {
                    let mut s = self.state.lock().unwrap();
                    let mut p = String::new();
                    for seg in filename.split('/').take(filename.matches('/').count()) {
                        if !p.is_empty() {
                            p.push('/');
                        }
                        p.push_str(seg);
                        s.ghost_dirs.remove(&(plugin.clone(), p.clone()));
                    }
                }
                let fp = fs_path_for(&plugin, &filename);
                let ino = self.alloc_inode(
                    &fp,
                    Entry::File {
                        plugin: plugin.clone(),
                        filename: filename.clone(),
                    },
                );
                let attr = file_attr(ino, 0, now);
                let fh = self.next_fh();
                self.open_files.lock().unwrap().insert(
                    fh,
                    WriteBuf {
                        bytes: Vec::new(),
                        plugin,
                        filename,
                        pass_real: None,
                        dirty: false,
                    },
                );
                reply.created(
                    &TTL,
                    &attr,
                    fuser::Generation(0),
                    FileHandle(fh),
                    fuser::FopenFlags::empty(),
                );
            }
            Some(Entry::Pass {
                real: parent_real,
                is_dir: true,
            }) => {
                let real = parent_real.join(&name_str);
                if let Some(p) = real.parent() {
                    let _ = fs::create_dir_all(p);
                }
                if let Err(e) = fs::File::create(&real) {
                    error!(error = %e, "pass-sub create");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                let key = pass_fs_path(&real);
                let ino = self.alloc_inode(
                    &key,
                    Entry::Pass {
                        real: real.clone(),
                        is_dir: false,
                    },
                );
                let attr = pass_attr(ino, &real).unwrap_or_else(|| file_attr(ino, 0, now));
                let fh = self.next_fh();
                self.open_files.lock().unwrap().insert(
                    fh,
                    WriteBuf {
                        bytes: Vec::new(),
                        plugin: String::new(),
                        filename: real.display().to_string(),
                        pass_real: Some(real),
                        dirty: false,
                    },
                );
                reply.created(
                    &TTL,
                    &attr,
                    fuser::Generation(0),
                    FileHandle(fh),
                    fuser::FopenFlags::empty(),
                );
            }
            _ => reply.error(fuser::Errno::from_i32(ENOTDIR)),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let parent = ino_u64(parent);
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(fuser::Errno::from_i32(ENOENT));
                return;
            }
        };
        let parent_entry = self.get_inode(parent);
        match parent_entry {
            Some(Entry::Root) => {
                let real = self.pass_root.join(name_str);
                if let Err(e) = fs::remove_file(&real) {
                    error!(error = %e, "pass unlink");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                self.remove_path(&format!("/{}", name_str));
                reply.ok();
            }
            Some(Entry::Dir { plugin, prefix }) => {
                let filename = join_path(&prefix, name_str);
                if let Err(e) = self.delete_file(&plugin, &filename) {
                    error!(error = %e, "delete");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                self.remove_path(&fs_path_for(&plugin, &filename));
                reply.ok();
            }
            Some(Entry::Pass {
                real: parent_real,
                is_dir: true,
            }) => {
                let real = parent_real.join(name_str);
                if let Err(e) = fs::remove_file(&real) {
                    error!(error = %e, "pass-sub unlink");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                self.remove_path(&pass_fs_path(&real));
                reply.ok();
            }
            _ => reply.error(fuser::Errno::from_i32(ENOTDIR)),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let parent = ino_u64(parent);
        let name_str = match name.to_str() {
            Some(s) => s.to_string(),
            None => {
                reply.error(fuser::Errno::from_i32(EIO));
                return;
            }
        };
        let parent_entry = self.get_inode(parent);
        match parent_entry {
            Some(Entry::Dir { plugin, prefix }) => {
                let dirpath = join_path(&prefix, &name_str);
                self.add_ghost_dir(&plugin, &dirpath);
                let fp = fs_path_for(&plugin, &dirpath);
                let ino = self.alloc_inode(
                    &fp,
                    Entry::Dir {
                        plugin,
                        prefix: dirpath,
                    },
                );
                reply.entry(&TTL, &dir_attr(ino), fuser::Generation(0));
            }
            Some(Entry::Root) => {
                if is_passthrough_root_name(&name_str) {
                    let real = self.pass_root.join(&name_str);
                    if let Err(e) = fs::create_dir_all(&real) {
                        error!(error = %e, "pass mkdir");
                        reply.error(fuser::Errno::from_i32(EIO));
                        return;
                    }
                    let key = format!("/{}", name_str);
                    let ino = self.alloc_inode(
                        &key,
                        Entry::Pass {
                            real: real.clone(),
                            is_dir: true,
                        },
                    );
                    let attr = pass_attr(ino, &real).unwrap_or_else(|| dir_attr(ino));
                    reply.entry(&TTL, &attr, fuser::Generation(0));
                } else {
                    let path = format!("/{}", name_str);
                    let ino = self.alloc_inode(
                        &path,
                        Entry::Dir {
                            plugin: name_str.clone(),
                            prefix: String::new(),
                        },
                    );
                    reply.entry(&TTL, &dir_attr(ino), fuser::Generation(0));
                }
            }
            Some(Entry::Pass {
                real: parent_real,
                is_dir: true,
            }) => {
                let real = parent_real.join(&name_str);
                if let Err(e) = fs::create_dir_all(&real) {
                    error!(error = %e, "pass-sub mkdir");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                let key = pass_fs_path(&real);
                let ino = self.alloc_inode(
                    &key,
                    Entry::Pass {
                        real: real.clone(),
                        is_dir: true,
                    },
                );
                let attr = pass_attr(ino, &real).unwrap_or_else(|| dir_attr(ino));
                reply.entry(&TTL, &attr, fuser::Generation(0));
            }
            _ => reply.error(fuser::Errno::from_i32(EROFS)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let parent = ino_u64(parent);
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(fuser::Errno::from_i32(ENOENT));
                return;
            }
        };
        let parent_entry = self.get_inode(parent);
        match parent_entry {
            Some(Entry::Dir { plugin, prefix }) => {
                let dirpath = join_path(&prefix, name_str);
                let pref = format!("{}/", dirpath);
                let files = self.list_files(&plugin); // no lock
                if files.iter().any(|f| f.filename.starts_with(&pref)) {
                    reply.error(fuser::Errno::from_i32(libc::ENOTEMPTY));
                    return;
                }
                let nested = self.ghost_dirs_under(&plugin, &pref);
                if !nested.is_empty() {
                    reply.error(fuser::Errno::from_i32(libc::ENOTEMPTY));
                    return;
                }
                self.remove_ghost_dir(&plugin, &dirpath);
                self.remove_path(&fs_path_for(&plugin, &dirpath));
                reply.ok();
            }
            Some(Entry::Root) => {
                let real = self.pass_root.join(name_str);
                if let Err(e) = fs::remove_dir(&real) {
                    error!(error = %e, "pass rmdir");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                self.remove_path(&format!("/{}", name_str));
                reply.ok();
            }
            Some(Entry::Pass {
                real: parent_real,
                is_dir: true,
            }) => {
                let real = parent_real.join(name_str);
                if let Err(e) = fs::remove_dir(&real) {
                    error!(error = %e, "pass-sub rmdir");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                self.remove_path(&pass_fs_path(&real));
                reply.ok();
            }
            _ => reply.error(fuser::Errno::from_i32(ENOTDIR)),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let parent = ino_u64(parent);
        let newparent = ino_u64(newparent);
        let n1 = match name.to_str() {
            Some(s) => s.to_string(),
            None => {
                reply.error(fuser::Errno::from_i32(ENOENT));
                return;
            }
        };
        let n2 = match newname.to_str() {
            Some(s) => s.to_string(),
            None => {
                reply.error(fuser::Errno::from_i32(EIO));
                return;
            }
        };

        // Snapshot both parent inode entries in one short critical section.
        let (p1, p2) = {
            let s = self.state.lock().unwrap();
            (
                s.inodes.get(&parent).cloned(),
                s.inodes.get(&newparent).cloned(),
            )
        };
        let pass_root = Arc::clone(&self.pass_root);

        let from_pass = match &p1 {
            Some(Entry::Root) => Some(pass_root.join(&n1)),
            Some(Entry::Pass { real, is_dir: true }) => Some(real.join(&n1)),
            _ => None,
        };
        let to_pass = match &p2 {
            Some(Entry::Root) => Some(pass_root.join(&n2)),
            Some(Entry::Pass { real, is_dir: true }) => Some(real.join(&n2)),
            _ => None,
        };

        if let (Some(from), Some(to)) = (from_pass, to_pass)
            && from.exists()
        {
            if let Err(e) = fs::rename(&from, &to) {
                error!(error = %e, "pass rename");
                reply.error(fuser::Errno::from_i32(EIO));
                return;
            }
            let from_key = match &p1 {
                Some(Entry::Root) => format!("/{}", n1),
                _ => pass_fs_path(&from),
            };
            self.remove_path(&from_key);
            reply.ok();
            return;
        }

        if let (
            Some(Entry::Dir {
                plugin: pa,
                prefix: pra,
            }),
            Some(Entry::Dir {
                plugin: pb,
                prefix: prb,
            }),
        ) = (p1, p2)
        {
            let old_full = join_path(&pra, &n1);
            let new_full = join_path(&prb, &n2);
            // Both reads/writes happen without any lock held.
            let entry = match self.get_file(&pa, &old_full) {
                Some(e) => e,
                None => {
                    reply.error(fuser::Errno::from_i32(ENOENT));
                    return;
                }
            };
            if let Err(e) = self.upsert_file(&pb, &new_full, &entry.raw) {
                error!(error = %e, "rename upsert");
                reply.error(fuser::Errno::from_i32(EIO));
                return;
            }
            if !(pa == pb && old_full == new_full) {
                let _ = self.delete_file(&pa, &old_full);
            }
            self.remove_path(&fs_path_for(&pa, &old_full));
            reply.ok();
            return;
        }

        reply.error(fuser::Errno::from_i32(EIO));
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let tarantool_url = std::env::var("TARANTOOL_URL").unwrap_or_else(|_| "127.0.0.1:3301".into());
    let mount_point = std::env::var("MOUNT_POINT").unwrap_or_else(|_| "/plugins".into());
    let pass_dir =
        PathBuf::from(std::env::var("PASSTHROUGH_DIR").unwrap_or_else(|_| "/passthrough".into()));
    let user = std::env::var("LOOM_USER").ok().filter(|s| !s.is_empty());
    let pass = std::env::var("LOOM_PASSWORD")
        .ok()
        .filter(|s| !s.is_empty());

    info!(url = %tarantool_url, user = ?user, "connecting to tarantool");

    // Multi-thread tokio runtime: tarantool I/O is async, FUSE callbacks are sync.
    // The runtime handle bridges them via `block_on` — but we never hold a state
    // lock across one of those calls, so different fh's don't serialize.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let handle = rt.handle().clone();

    let auth = match (&user, &pass) {
        (Some(u), Some(p)) => Some((u.as_str(), p.as_str())),
        _ => None,
    };
    let client = handle.block_on(async {
        tarantool::Client::connect_auth_with_retry(&tarantool_url, auth, 30, 2000).await
    })?;
    info!("tarantool connected");

    let api = Arc::new(tarantool::LoomApi { client });
    let fs_impl = LoomFS::new(api, handle, pass_dir.clone());
    info!(mount_point, pass_dir = %pass_dir.display(), "mounting FUSE");
    let mut config = fuser::Config::default();
    // Allow any uid in the pod to access the FUSE mount (Paper container may have a
    // different uid; kernel-side ACL == "allow_other").
    config.acl = fuser::SessionACL::All;
    config
        .mount_options
        .push(MountOption::FSName("loom".into()));
    config.mount_options.push(MountOption::DefaultPermissions);
    fuser::mount2(fs_impl, &mount_point, &config)?;
    Ok(())
}
