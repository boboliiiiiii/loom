//! loom-sidecar — FUSE filesystem for Minecraft Paper plugin directory.
//!
//! Routing rules at the mount root (e.g. /plugins):
//!   - Plain directory names (e.g. `bStats`) → Tarantool `configs_files`.
//!   - Files at the root level (e.g. `loom-demo.jar`) → real disk passthrough.
//!   - Directories starting with `.` or `tmp-` → real disk passthrough (Paper internals).
//!
//! Inside Tarantool plugin dirs, nested subdirs are virtual (filename has `/`).
//! Inside passthrough dirs, everything is real FS.

mod tarantool;

use anyhow::Result;
use fuser::{
    BsdFileFlags, FileAttr, FileHandle, FileType, Filesystem, INodeNo, LockOwner, MountOption,
    OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow, WriteFlags,
};
use libc::{EIO, ENOENT, ENOTDIR, EROFS};
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

/// Mutable state. All Filesystem callbacks lock the parent [LoomFS]'s
/// `Mutex<Inner>` once and operate on this. fuser dispatches callbacks
/// from a single thread by default, so contention is nil.
struct Inner {
    api: Arc<tarantool::LoomApi>,
    rt: tokio::runtime::Handle,
    pass_root: PathBuf,
    inodes: HashMap<u64, Entry>,
    paths: HashMap<String, u64>,
    next_ino: u64,
    next_fh: u64,
    open_files: HashMap<u64, WriteBuf>,
    ghost_dirs: HashSet<(String, String)>,
}

struct LoomFS {
    state: Mutex<Inner>,
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

impl Inner {
    fn new(api: Arc<tarantool::LoomApi>, rt: tokio::runtime::Handle, pass_root: PathBuf) -> Self {
        if !pass_root.exists() {
            let _ = fs::create_dir_all(&pass_root);
        }
        let mut s = Self {
            api,
            rt,
            pass_root,
            inodes: HashMap::new(),
            paths: HashMap::new(),
            next_ino: FIRST_INO,
            next_fh: 1,
            open_files: HashMap::new(),
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

    fn list_plugins(&mut self) -> Vec<String> {
        let api = Arc::clone(&self.api);
        self.rt
            .block_on(async move { api.list_plugins().await })
            .unwrap_or_default()
    }

    fn list_files(&mut self, plugin: &str) -> Vec<tarantool::FileMeta> {
        let api = Arc::clone(&self.api);
        let plugin = plugin.to_string();
        self.rt
            .block_on(async move { api.list_files(&plugin).await })
            .unwrap_or_default()
    }

    fn get_file(&mut self, plugin: &str, filename: &str) -> Option<tarantool::FileEntry> {
        let api = Arc::clone(&self.api);
        let plugin = plugin.to_string();
        let filename = filename.to_string();
        self.rt
            .block_on(async move { api.get_file(&plugin, &filename).await })
            .ok()
            .flatten()
    }

    fn upsert_file(&mut self, plugin: &str, filename: &str, raw: &[u8]) -> Result<()> {
        let api = Arc::clone(&self.api);
        let plugin = plugin.to_string();
        let filename = filename.to_string();
        let raw = raw.to_vec();
        self.rt
            .block_on(async move { api.upsert_file(&plugin, &filename, &raw).await })
    }

    fn delete_file(&mut self, plugin: &str, filename: &str) -> Result<()> {
        let api = Arc::clone(&self.api);
        let plugin = plugin.to_string();
        let filename = filename.to_string();
        self.rt
            .block_on(async move { api.delete_file(&plugin, &filename).await })
    }

    fn list_dir_children(&mut self, plugin: &str, prefix: &str) -> Vec<(String, bool)> {
        let pws = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", prefix)
        };
        let files = self.list_files(plugin);
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
        for (gp, gd) in &self.ghost_dirs {
            if gp != plugin {
                continue;
            }
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

    fn persist_fh(&mut self, fh: u64) -> Result<()> {
        let need = matches!(self.open_files.get(&fh), Some(b) if b.dirty);
        if !need {
            return Ok(());
        }
        let (pass_real, plugin, filename, bytes) = {
            let b = self.open_files.get(&fh).unwrap();
            (
                b.pass_real.clone(),
                b.plugin.clone(),
                b.filename.clone(),
                b.bytes.clone(),
            )
        };
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
            self.ghost_dirs
                .retain(|(p, d)| !(p == &plugin && filename.starts_with(&format!("{}/", d))));
        }
        if let Some(b) = self.open_files.get_mut(&fh) {
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
        let mut s = self.state.lock().unwrap();

        match s.inodes.get(&parent).cloned() {
            Some(Entry::Root) => {
                let path = format!("/{}", name_str);
                if let Some(&ino) = s.paths.get(&path) {
                    match s.inodes.get(&ino).cloned() {
                        Some(Entry::Dir { .. }) => {
                            reply.entry(&TTL, &dir_attr(ino), fuser::Generation(0));
                            return;
                        }
                        Some(Entry::Pass { real, .. }) => {
                            if let Some(attr) = pass_attr(ino, &real) {
                                reply.entry(&TTL, &attr, fuser::Generation(0));
                                return;
                            }
                        }
                        _ => {}
                    }
                }
                let real = s.pass_root.join(name_str);
                if let Ok(meta) = fs::symlink_metadata(&real) {
                    let is_dir = meta.is_dir();
                    let ino = s.alloc_inode(
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
                let plugins = s.list_plugins();
                if plugins.iter().any(|p| p == name_str) {
                    let ino = s.alloc_inode(
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
                if let Some(entry) = s.get_file(&plugin, &child_path) {
                    let fp = fs_path_for(&plugin, &child_path);
                    let ino = s.alloc_inode(
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
                let pwc = format!("{}/", child_path);
                let files = s.list_files(&plugin);
                let is_real_dir = files.iter().any(|f| f.filename.starts_with(&pwc));
                let is_ghost = s.ghost_dirs.contains(&(plugin.clone(), child_path.clone()));
                if is_real_dir || is_ghost {
                    let fp = fs_path_for(&plugin, &child_path);
                    let ino = s.alloc_inode(
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
                if let Ok(_meta) = fs::symlink_metadata(&real) {
                    let is_dir = real.is_dir();
                    let key = pass_fs_path(&real);
                    let ino = s.alloc_inode(
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
        let mut s = self.state.lock().unwrap();
        match s.inodes.get(&ino).cloned() {
            Some(Entry::File { plugin, filename }) => {
                if let Some(new_size) = size {
                    let mut bytes = s
                        .get_file(&plugin, &filename)
                        .map(|e| e.raw)
                        .unwrap_or_default();
                    bytes.resize(new_size as usize, 0);
                    let _ = s.upsert_file(&plugin, &filename, &bytes);
                    for buf in s.open_files.values_mut() {
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
                let sz = s
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
                    for buf in s.open_files.values_mut() {
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
        let mut s = self.state.lock().unwrap();
        match s.inodes.get(&ino).cloned() {
            Some(Entry::Root) | Some(Entry::Dir { .. }) => reply.attr(&TTL, &dir_attr(ino)),
            Some(Entry::File { plugin, filename }) => {
                if let Some(e) = s.get_file(&plugin, &filename) {
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
        let mut s = self.state.lock().unwrap();
        let mut entries: Vec<(u64, FileType, String)> = vec![
            (ino, FileType::Directory, ".".into()),
            (ino, FileType::Directory, "..".into()),
        ];
        match s.inodes.get(&ino).cloned() {
            Some(Entry::Root) => {
                for plugin in s.list_plugins() {
                    let path = format!("/{}", plugin);
                    let i = s.alloc_inode(
                        &path,
                        Entry::Dir {
                            plugin: plugin.clone(),
                            prefix: String::new(),
                        },
                    );
                    entries.push((i, FileType::Directory, plugin));
                }
                let pass_root = s.pass_root.clone();
                if let Ok(rd) = fs::read_dir(&pass_root) {
                    for de in rd.flatten() {
                        let name = de.file_name().to_string_lossy().to_string();
                        let real = de.path();
                        let is_dir = real.is_dir();
                        let key = format!("/{}", name);
                        let i = s.alloc_inode(&key, Entry::Pass { real, is_dir });
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
                for (name, is_file) in s.list_dir_children(&plugin, &prefix) {
                    let child = join_path(&prefix, &name);
                    let fp = fs_path_for(&plugin, &child);
                    if is_file {
                        let i = s.alloc_inode(
                            &fp,
                            Entry::File {
                                plugin: plugin.clone(),
                                filename: child,
                            },
                        );
                        entries.push((i, FileType::RegularFile, name));
                    } else {
                        let i = s.alloc_inode(
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
                        let i = s.alloc_inode(&key, Entry::Pass { real: p, is_dir });
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
        let mut s = self.state.lock().unwrap();
        match s.inodes.get(&ino).cloned() {
            Some(Entry::File { plugin, filename }) => {
                let bytes = if truncate {
                    Vec::new()
                } else {
                    s.get_file(&plugin, &filename)
                        .map(|e| e.raw)
                        .unwrap_or_default()
                };
                let fh = s.next_fh;
                s.next_fh += 1;
                s.open_files.insert(
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
                let fh = s.next_fh;
                s.next_fh += 1;
                s.open_files.insert(
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
        let s = self.state.lock().unwrap();
        let buf = match s.open_files.get(&fh) {
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
        let mut s = self.state.lock().unwrap();
        let buf = match s.open_files.get_mut(&fh) {
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
        let mut s = self.state.lock().unwrap();
        if s.persist_fh(fh).is_err() {
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
        let mut s = self.state.lock().unwrap();
        let _ = s.persist_fh(fh);
        s.open_files.remove(&fh);
        reply.ok();
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
        let mut s = self.state.lock().unwrap();
        match s.inodes.get(&parent).cloned() {
            Some(Entry::Root) => {
                let real = s.pass_root.join(&name_str);
                if let Some(p) = real.parent() {
                    let _ = fs::create_dir_all(p);
                }
                if let Err(e) = fs::File::create(&real) {
                    error!(error = %e, "pass create");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                let key = format!("/{}", name_str);
                let ino = s.alloc_inode(
                    &key,
                    Entry::Pass {
                        real: real.clone(),
                        is_dir: false,
                    },
                );
                let attr = pass_attr(ino, &real).unwrap_or_else(|| file_attr(ino, 0, now));
                let fh = s.next_fh;
                s.next_fh += 1;
                s.open_files.insert(
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
                if let Err(e) = s.upsert_file(&plugin, &filename, &[]) {
                    error!(error = %e, "create upsert");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                let mut p = String::new();
                for seg in filename.split('/').take(filename.matches('/').count()) {
                    if !p.is_empty() {
                        p.push('/');
                    }
                    p.push_str(seg);
                    s.ghost_dirs.remove(&(plugin.clone(), p.clone()));
                }
                let fp = fs_path_for(&plugin, &filename);
                let ino = s.alloc_inode(
                    &fp,
                    Entry::File {
                        plugin: plugin.clone(),
                        filename: filename.clone(),
                    },
                );
                let attr = file_attr(ino, 0, now);
                let fh = s.next_fh;
                s.next_fh += 1;
                s.open_files.insert(
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
                let ino = s.alloc_inode(
                    &key,
                    Entry::Pass {
                        real: real.clone(),
                        is_dir: false,
                    },
                );
                let attr = pass_attr(ino, &real).unwrap_or_else(|| file_attr(ino, 0, now));
                let fh = s.next_fh;
                s.next_fh += 1;
                s.open_files.insert(
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
        let mut s = self.state.lock().unwrap();
        match s.inodes.get(&parent).cloned() {
            Some(Entry::Root) => {
                let real = s.pass_root.join(name_str);
                if let Err(e) = fs::remove_file(&real) {
                    error!(error = %e, "pass unlink");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                let key = format!("/{}", name_str);
                if let Some(ino) = s.paths.remove(&key) {
                    s.inodes.remove(&ino);
                }
                reply.ok();
            }
            Some(Entry::Dir { plugin, prefix }) => {
                let filename = join_path(&prefix, name_str);
                if let Err(e) = s.delete_file(&plugin, &filename) {
                    error!(error = %e, "delete");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                let fp = fs_path_for(&plugin, &filename);
                if let Some(ino) = s.paths.remove(&fp) {
                    s.inodes.remove(&ino);
                }
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
                let key = pass_fs_path(&real);
                if let Some(ino) = s.paths.remove(&key) {
                    s.inodes.remove(&ino);
                }
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
        let mut s = self.state.lock().unwrap();
        match s.inodes.get(&parent).cloned() {
            Some(Entry::Dir { plugin, prefix }) => {
                let dirpath = join_path(&prefix, &name_str);
                s.ghost_dirs.insert((plugin.clone(), dirpath.clone()));
                let fp = fs_path_for(&plugin, &dirpath);
                let ino = s.alloc_inode(
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
                    let real = s.pass_root.join(&name_str);
                    if let Err(e) = fs::create_dir_all(&real) {
                        error!(error = %e, "pass mkdir");
                        reply.error(fuser::Errno::from_i32(EIO));
                        return;
                    }
                    let key = format!("/{}", name_str);
                    let ino = s.alloc_inode(
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
                    let ino = s.alloc_inode(
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
                let ino = s.alloc_inode(
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
        let mut s = self.state.lock().unwrap();
        match s.inodes.get(&parent).cloned() {
            Some(Entry::Dir { plugin, prefix }) => {
                let dirpath = join_path(&prefix, name_str);
                let pref = format!("{}/", dirpath);
                let files = s.list_files(&plugin);
                if files.iter().any(|f| f.filename.starts_with(&pref)) {
                    reply.error(fuser::Errno::from_i32(libc::ENOTEMPTY));
                    return;
                }
                if s.ghost_dirs
                    .iter()
                    .any(|(p, d)| p == &plugin && d.starts_with(&pref))
                {
                    reply.error(fuser::Errno::from_i32(libc::ENOTEMPTY));
                    return;
                }
                s.ghost_dirs.remove(&(plugin.clone(), dirpath.clone()));
                let fp = fs_path_for(&plugin, &dirpath);
                if let Some(ino) = s.paths.remove(&fp) {
                    s.inodes.remove(&ino);
                }
                reply.ok();
            }
            Some(Entry::Root) => {
                let real = s.pass_root.join(name_str);
                if let Err(e) = fs::remove_dir(&real) {
                    error!(error = %e, "pass rmdir");
                    reply.error(fuser::Errno::from_i32(EIO));
                    return;
                }
                let key = format!("/{}", name_str);
                if let Some(ino) = s.paths.remove(&key) {
                    s.inodes.remove(&ino);
                }
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
                let key = pass_fs_path(&real);
                if let Some(ino) = s.paths.remove(&key) {
                    s.inodes.remove(&ino);
                }
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
            Some(s) => s,
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
        let mut s = self.state.lock().unwrap();
        let p1 = s.inodes.get(&parent).cloned();
        let p2 = s.inodes.get(&newparent).cloned();
        let pass_root = s.pass_root.clone();

        let from_pass = match &p1 {
            Some(Entry::Root) => Some(pass_root.join(n1)),
            Some(Entry::Pass { real, is_dir: true }) => Some(real.join(n1)),
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
            if let Some(ino) = s.paths.remove(&from_key) {
                s.inodes.remove(&ino);
            }
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
            let old_full = join_path(&pra, n1);
            let new_full = join_path(&prb, &n2);
            let entry = match s.get_file(&pa, &old_full) {
                Some(e) => e,
                None => {
                    reply.error(fuser::Errno::from_i32(ENOENT));
                    return;
                }
            };
            if let Err(e) = s.upsert_file(&pb, &new_full, &entry.raw) {
                error!(error = %e, "rename upsert");
                reply.error(fuser::Errno::from_i32(EIO));
                return;
            }
            if !(pa == pb && old_full == new_full) {
                let _ = s.delete_file(&pa, &old_full);
            }
            let old_fs = fs_path_for(&pa, &old_full);
            if let Some(ino) = s.paths.remove(&old_fs) {
                s.inodes.remove(&ino);
            }
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

    // Build a multi-thread tokio runtime: tarantool I/O is async, FUSE callbacks are sync,
    // and the runtime handle bridges them via `block_on`.
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
    let inner = Inner::new(api, handle, pass_dir.clone());
    let fs_impl = LoomFS {
        state: Mutex::new(inner),
    };
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
