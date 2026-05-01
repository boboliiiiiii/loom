//! Minimal synchronous Tarantool iproto client.
//! Only supports CALL — sufficient for talking to our `loom.*` stored procs.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use rmpv::Value;

const IPROTO_CODE: u8 = 0x00;
const IPROTO_SYNC: u8 = 0x01;
const IPROTO_DATA: u8 = 0x30;
const IPROTO_ERROR_LEGACY: u8 = 0x31;
const IPROTO_TUPLE: u8 = 0x21;
const IPROTO_FUNCTION_NAME: u8 = 0x22;
const REQUEST_TYPE_CALL: u8 = 0x0a;
const RESPONSE_OK: u64 = 0;

pub struct Client {
    stream: TcpStream,
    next_sync: u64,
}

impl Client {
    pub fn connect(addr: &str) -> Result<Self> {
        Self::connect_auth(addr, None)
    }

    pub fn connect_auth(addr: &str, auth: Option<(&str, &str)>) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        stream.set_nodelay(true)?;

        let mut s = Self { stream, next_sync: 0 };
        let mut greeting = [0u8; 128];
        s.stream.read_exact(&mut greeting)?;

        if let Some((user, pass)) = auth {
            // greeting bytes 64..107 are base64-encoded salt (44 chars + spaces)
            let salt_b64 = std::str::from_utf8(&greeting[64..108])
                .map_err(|e| anyhow!("bad greeting: {e}"))?
                .trim();
            s.authenticate(user, pass, salt_b64)?;
        }
        Ok(s)
    }

    pub fn connect_with_retry_auth(
        addr: &str, auth: Option<(&str, &str)>, attempts: u32, delay_ms: u64,
    ) -> Result<Self> {
        let mut last = None;
        for i in 1..=attempts {
            match Self::connect_auth(addr, auth) {
                Ok(c) => return Ok(c),
                Err(e) => {
                    tracing::warn!(attempt = i, error = %e, "tarantool connect failed, retrying");
                    last = Some(e);
                    std::thread::sleep(Duration::from_millis(delay_ms));
                }
            }
        }
        Err(last.unwrap_or_else(|| anyhow!("connect failed")))
    }

    pub fn connect_with_retry(addr: &str, attempts: u32, delay_ms: u64) -> Result<Self> {
        let mut last = None;
        for i in 1..=attempts {
            match Self::connect(addr) {
                Ok(c) => return Ok(c),
                Err(e) => {
                    tracing::warn!(attempt = i, error = %e, "tarantool connect failed, retrying");
                    last = Some(e);
                    std::thread::sleep(Duration::from_millis(delay_ms));
                }
            }
        }
        Err(last.unwrap_or_else(|| anyhow!("connect failed")))
    }

    fn authenticate(&mut self, user: &str, password: &str, salt_b64: &str) -> Result<()> {
        use sha1::{Sha1, Digest};
        use base64::{Engine, engine::general_purpose::STANDARD};

        let salt = STANDARD.decode(salt_b64).map_err(|e| anyhow!("bad salt: {e}"))?;
        let salt = &salt[..20];

        let mut sha = Sha1::new();
        sha.update(password.as_bytes());
        let step1: [u8; 20] = sha.finalize_reset().into();

        sha.update(step1);
        let step2: [u8; 20] = sha.finalize_reset().into();

        sha.update(salt);
        sha.update(step2);
        let step3: [u8; 20] = sha.finalize().into();

        let mut scramble = [0u8; 20];
        for i in 0..20 { scramble[i] = step1[i] ^ step3[i]; }

        self.next_sync = self.next_sync.wrapping_add(1);
        let sync = self.next_sync;

        const REQUEST_TYPE_AUTH: u8 = 0x07;
        const IPROTO_USER_NAME: u8 = 0x23;

        let header = Value::Map(vec![
            (Value::from(IPROTO_CODE), Value::from(REQUEST_TYPE_AUTH)),
            (Value::from(IPROTO_SYNC), Value::from(sync)),
        ]);
        let body = Value::Map(vec![
            (Value::from(IPROTO_USER_NAME), Value::from(user)),
            (Value::from(IPROTO_TUPLE), Value::Array(vec![
                Value::from("chap-sha1"),
                Value::Binary(scramble.to_vec()),
            ])),
        ]);

        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &header)?;
        rmpv::encode::write_value(&mut buf, &body)?;
        let mut prefix = [0u8; 5];
        prefix[0] = 0xce;
        prefix[1..].copy_from_slice(&(buf.len() as u32).to_be_bytes());
        self.stream.write_all(&prefix)?;
        self.stream.write_all(&buf)?;
        self.stream.flush()?;

        let mut size_buf = [0u8; 5];
        self.stream.read_exact(&mut size_buf)?;
        let resp_len = u32::from_be_bytes(size_buf[1..].try_into().unwrap()) as usize;
        let mut resp = vec![0u8; resp_len];
        self.stream.read_exact(&mut resp)?;
        let mut cur = std::io::Cursor::new(&resp[..]);
        let h = rmpv::decode::read_value(&mut cur)?;
        let b = rmpv::decode::read_value(&mut cur)?;
        let code = map_get_u64(&h, IPROTO_CODE).unwrap_or(0);
        if code != RESPONSE_OK {
            let err = map_get(&b, IPROTO_ERROR_LEGACY).map(|v| v.to_string()).unwrap_or_default();
            bail!("tarantool auth failed: {err}");
        }
        Ok(())
    }

    /// Call a Tarantool stored function. `args` must be an array Value.
    /// Returns the IPROTO_DATA part of the response (typically the function's return value
    /// wrapped in an outer array, since Tarantool returns multiple values as array).
    pub fn call(&mut self, func: &str, args: Value) -> Result<Value> {
        self.next_sync = self.next_sync.wrapping_add(1);
        let sync = self.next_sync;

        let header = Value::Map(vec![
            (Value::from(IPROTO_CODE), Value::from(REQUEST_TYPE_CALL)),
            (Value::from(IPROTO_SYNC), Value::from(sync)),
        ]);
        let body = Value::Map(vec![
            (Value::from(IPROTO_FUNCTION_NAME), Value::from(func)),
            (Value::from(IPROTO_TUPLE), args),
        ]);

        let mut buf = Vec::with_capacity(256);
        rmpv::encode::write_value(&mut buf, &header)?;
        rmpv::encode::write_value(&mut buf, &body)?;

        // 5-byte length prefix: msgpack uint32 marker (0xce) + 4-byte BE size.
        let total: u32 = buf.len() as u32;
        let mut prefix = [0u8; 5];
        prefix[0] = 0xce;
        prefix[1..].copy_from_slice(&total.to_be_bytes());

        self.stream.write_all(&prefix)?;
        self.stream.write_all(&buf)?;
        self.stream.flush()?;

        // Read response: 5-byte prefix, then header+body of that length.
        let mut size_buf = [0u8; 5];
        self.stream.read_exact(&mut size_buf)?;
        if size_buf[0] != 0xce {
            bail!("unexpected response size marker: 0x{:02x}", size_buf[0]);
        }
        let resp_len = u32::from_be_bytes(size_buf[1..].try_into().unwrap()) as usize;

        let mut resp = vec![0u8; resp_len];
        self.stream.read_exact(&mut resp)?;

        let mut cur = std::io::Cursor::new(&resp[..]);
        let resp_header = rmpv::decode::read_value(&mut cur)?;
        let resp_body = rmpv::decode::read_value(&mut cur)?;

        let code = map_get_u64(&resp_header, IPROTO_CODE).unwrap_or(0);
        if code != RESPONSE_OK {
            let err = map_get(&resp_body, IPROTO_ERROR_LEGACY)
                .map(|v| v.to_string())
                .unwrap_or_else(|| format!("code {}", code));
            bail!("tarantool error: {}", err);
        }

        Ok(map_get(&resp_body, IPROTO_DATA).cloned().unwrap_or(Value::Nil))
    }
}

fn map_get<'a>(map: &'a Value, key: u8) -> Option<&'a Value> {
    if let Value::Map(entries) = map {
        for (k, v) in entries {
            if let Some(kn) = k.as_u64() {
                if kn == key as u64 {
                    return Some(v);
                }
            }
        }
    }
    None
}

fn map_get_u64(map: &Value, key: u8) -> Option<u64> {
    map_get(map, key).and_then(|v| v.as_u64())
}

// ---------- Loom-specific wrappers ----------

pub struct LoomApi<'a> {
    pub client: &'a mut Client,
}

impl<'a> LoomApi<'a> {
    pub fn list_plugins(&mut self) -> Result<Vec<String>> {
        let r = self.client.call("loom.file_list_plugins", Value::Array(vec![]))?;
        let arr = unwrap_call_result(r);
        Ok(arr.into_iter().filter_map(|v| v.as_str().map(String::from)).collect())
    }

    pub fn list_files(&mut self, plugin: &str) -> Result<Vec<FileMeta>> {
        let r = self.client.call(
            "loom.file_list",
            Value::Array(vec![Value::from(plugin)]),
        )?;
        let arr = unwrap_call_result(r);
        let mut out = Vec::new();
        for tuple in arr {
            if let Some(meta) = parse_file_tuple(&tuple) {
                out.push(meta);
            }
        }
        Ok(out)
    }

    pub fn get_file(&mut self, plugin: &str, filename: &str) -> Result<Option<FileEntry>> {
        let r = self.client.call(
            "loom.file_get",
            Value::Array(vec![Value::from(plugin), Value::from(filename)]),
        )?;
        // Tarantool returns the tuple directly, but iproto wraps in array; result here is
        // [tuple] or [].
        if let Value::Array(arr) = r {
            if let Some(tuple) = arr.into_iter().next() {
                if matches!(tuple, Value::Nil) {
                    return Ok(None);
                }
                return Ok(parse_file_tuple_full(&tuple));
            }
        }
        Ok(None)
    }

    pub fn upsert_file(&mut self, plugin: &str, filename: &str, raw: &[u8]) -> Result<()> {
        self.client.call(
            "loom.file_upsert",
            Value::Array(vec![
                Value::from(plugin),
                Value::from(filename),
                Value::from(raw),
            ]),
        )?;
        Ok(())
    }

    pub fn delete_file(&mut self, plugin: &str, filename: &str) -> Result<()> {
        self.client.call(
            "loom.file_delete",
            Value::Array(vec![Value::from(plugin), Value::from(filename)]),
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FileMeta {
    pub plugin: String,
    pub filename: String,
    pub mtime: u64,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub meta: FileMeta,
    pub raw: Vec<u8>,
}

fn unwrap_call_result(v: Value) -> Vec<Value> {
    // Tarantool CALL returns multiple results as an outer array.
    // For our functions returning a single table, we get [[...]].
    match v {
        Value::Array(arr) => {
            if arr.len() == 1 {
                if let Value::Array(inner) = arr.into_iter().next().unwrap() {
                    return inner;
                } else {
                    return vec![];
                }
            }
            arr
        }
        _ => vec![],
    }
}

fn parse_file_tuple(t: &Value) -> Option<FileMeta> {
    if let Value::Array(fields) = t {
        if fields.len() >= 5 {
            return Some(FileMeta {
                plugin: fields[0].as_str()?.to_string(),
                filename: fields[1].as_str()?.to_string(),
                mtime: fields[3].as_u64().unwrap_or(0),
                size: fields[4].as_u64().unwrap_or(0),
            });
        }
    }
    None
}

fn parse_file_tuple_full(t: &Value) -> Option<FileEntry> {
    if let Value::Array(fields) = t {
        if fields.len() >= 5 {
            let plugin = fields[0].as_str()?.to_string();
            let filename = fields[1].as_str()?.to_string();
            let raw = match &fields[2] {
                Value::String(s) => s.as_bytes().to_vec(),
                Value::Binary(b) => b.clone(),
                _ => return None,
            };
            let mtime = fields[3].as_u64().unwrap_or(0);
            let size = fields[4].as_u64().unwrap_or(raw.len() as u64);
            return Some(FileEntry {
                meta: FileMeta { plugin, filename, mtime, size },
                raw,
            });
        }
    }
    None
}
