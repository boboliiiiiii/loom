//! Async Tarantool iproto client backed by tokio.
//! Supports CALL with sync-id multiplexing on a single connection.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use rmpv::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, oneshot};

const IPROTO_CODE: u8 = 0x00;
const IPROTO_SYNC: u8 = 0x01;
const IPROTO_DATA: u8 = 0x30;
const IPROTO_ERROR_LEGACY: u8 = 0x31;
const IPROTO_TUPLE: u8 = 0x21;
const IPROTO_FUNCTION_NAME: u8 = 0x22;
const REQUEST_TYPE_CALL: u8 = 0x0a;
const REQUEST_TYPE_AUTH: u8 = 0x07;
const IPROTO_USER_NAME: u8 = 0x23;
const RESPONSE_OK: u64 = 0;

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>;

#[derive(Clone)]
pub struct Client {
    write: Arc<Mutex<OwnedWriteHalf>>,
    pending: Pending,
    next_sync: Arc<AtomicU64>,
}

impl Client {
    #[allow(dead_code)]
    pub async fn connect(addr: &str) -> Result<Self> {
        Self::connect_auth(addr, None).await
    }

    pub async fn connect_auth(addr: &str, auth: Option<(&str, &str)>) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (mut read_half, mut write_half) = stream.into_split();

        let mut greeting = [0u8; 128];
        read_half.read_exact(&mut greeting).await?;

        if let Some((user, pass)) = auth {
            let salt_b64 = std::str::from_utf8(&greeting[64..108])
                .map_err(|e| anyhow!("bad greeting: {e}"))?
                .trim();
            authenticate(&mut write_half, &mut read_half, user, pass, salt_b64).await?;
        }

        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let next_sync = Arc::new(AtomicU64::new(1));

        let pending_for_reader = Arc::clone(&pending);
        tokio::spawn(async move {
            if let Err(e) = reader_loop(read_half, pending_for_reader.clone()).await {
                tracing::warn!(error = %e, "tarantool reader exited");
                let mut p = pending_for_reader.lock().await;
                for (_, tx) in p.drain() {
                    let _ = tx.send(Err(anyhow!("tarantool connection lost: {e}")));
                }
            }
        });

        Ok(Self {
            write: Arc::new(Mutex::new(write_half)),
            pending,
            next_sync,
        })
    }

    pub async fn connect_auth_with_retry(
        addr: &str,
        auth: Option<(&str, &str)>,
        attempts: u32,
        delay_ms: u64,
    ) -> Result<Self> {
        let mut last: Option<anyhow::Error> = None;
        for i in 1..=attempts {
            match Self::connect_auth(addr, auth).await {
                Ok(c) => return Ok(c),
                Err(e) => {
                    tracing::warn!(attempt = i, error = %e, "tarantool connect failed, retrying");
                    last = Some(e);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
        Err(last.unwrap_or_else(|| anyhow!("connect failed")))
    }

    /// Call a Tarantool stored function. `args` must be an array Value.
    /// Returns the IPROTO_DATA part of the response.
    pub async fn call(&self, func: &str, args: Value) -> Result<Value> {
        let sync = self.next_sync.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(sync, tx);

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

        let mut prefix = [0u8; 5];
        prefix[0] = 0xce;
        prefix[1..].copy_from_slice(&(buf.len() as u32).to_be_bytes());

        {
            let mut w = self.write.lock().await;
            if let Err(e) = w.write_all(&prefix).await {
                self.pending.lock().await.remove(&sync);
                bail!("tarantool write failed (prefix): {e}");
            }
            if let Err(e) = w.write_all(&buf).await {
                self.pending.lock().await.remove(&sync);
                bail!("tarantool write failed (body): {e}");
            }
            if let Err(e) = w.flush().await {
                self.pending.lock().await.remove(&sync);
                bail!("tarantool flush failed: {e}");
            }
        }

        match rx.await {
            Ok(r) => r,
            Err(_) => bail!("tarantool reader closed before reply"),
        }
    }
}

async fn reader_loop(mut read: OwnedReadHalf, pending: Pending) -> Result<()> {
    loop {
        let mut size_buf = [0u8; 5];
        read.read_exact(&mut size_buf).await?;
        if size_buf[0] != 0xce {
            bail!("unexpected response size marker: 0x{:02x}", size_buf[0]);
        }
        let resp_len = u32::from_be_bytes(size_buf[1..].try_into().unwrap()) as usize;

        let mut resp = vec![0u8; resp_len];
        read.read_exact(&mut resp).await?;

        let mut cur = std::io::Cursor::new(&resp[..]);
        let resp_header = rmpv::decode::read_value(&mut cur)?;
        let resp_body = rmpv::decode::read_value(&mut cur)?;

        let code = map_get_u64(&resp_header, IPROTO_CODE).unwrap_or(0);
        let sync = map_get_u64(&resp_header, IPROTO_SYNC).unwrap_or(0);

        let tx = match pending.lock().await.remove(&sync) {
            Some(t) => t,
            None => continue,
        };

        let result = if code == RESPONSE_OK {
            let data = map_get(&resp_body, IPROTO_DATA)
                .cloned()
                .unwrap_or(Value::Nil);
            Ok(data)
        } else {
            let err = map_get(&resp_body, IPROTO_ERROR_LEGACY)
                .map(|v| v.to_string())
                .unwrap_or_else(|| format!("code {code}"));
            Err(anyhow!("tarantool error: {err}"))
        };
        let _ = tx.send(result);
    }
}

async fn authenticate(
    write: &mut OwnedWriteHalf,
    read: &mut OwnedReadHalf,
    user: &str,
    password: &str,
    salt_b64: &str,
) -> Result<()> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use sha1::{Digest, Sha1};

    let salt = STANDARD
        .decode(salt_b64)
        .map_err(|e| anyhow!("bad salt: {e}"))?;
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
    for i in 0..20 {
        scramble[i] = step1[i] ^ step3[i];
    }

    let header = Value::Map(vec![
        (Value::from(IPROTO_CODE), Value::from(REQUEST_TYPE_AUTH)),
        (Value::from(IPROTO_SYNC), Value::from(0u64)),
    ]);
    let body = Value::Map(vec![
        (Value::from(IPROTO_USER_NAME), Value::from(user)),
        (
            Value::from(IPROTO_TUPLE),
            Value::Array(vec![
                Value::from("chap-sha1"),
                Value::Binary(scramble.to_vec()),
            ]),
        ),
    ]);

    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &header)?;
    rmpv::encode::write_value(&mut buf, &body)?;

    let mut prefix = [0u8; 5];
    prefix[0] = 0xce;
    prefix[1..].copy_from_slice(&(buf.len() as u32).to_be_bytes());
    write.write_all(&prefix).await?;
    write.write_all(&buf).await?;
    write.flush().await?;

    let mut size_buf = [0u8; 5];
    read.read_exact(&mut size_buf).await?;
    let resp_len = u32::from_be_bytes(size_buf[1..].try_into().unwrap()) as usize;
    let mut resp = vec![0u8; resp_len];
    read.read_exact(&mut resp).await?;
    let mut cur = std::io::Cursor::new(&resp[..]);
    let h = rmpv::decode::read_value(&mut cur)?;
    let b = rmpv::decode::read_value(&mut cur)?;
    let code = map_get_u64(&h, IPROTO_CODE).unwrap_or(0);
    if code != RESPONSE_OK {
        let err = map_get(&b, IPROTO_ERROR_LEGACY)
            .map(|v| v.to_string())
            .unwrap_or_default();
        bail!("tarantool auth failed: {err}");
    }
    Ok(())
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

#[derive(Debug, Clone)]
#[allow(dead_code)]
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

pub struct LoomApi {
    pub client: Client,
}

impl LoomApi {
    pub async fn list_plugins(&self) -> Result<Vec<String>> {
        let r = self
            .client
            .call("loom.file_list_plugins", Value::Array(vec![]))
            .await?;
        let arr = unwrap_call_result(r);
        Ok(arr
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect())
    }

    pub async fn list_files(&self, plugin: &str) -> Result<Vec<FileMeta>> {
        let r = self
            .client
            .call("loom.file_list", Value::Array(vec![Value::from(plugin)]))
            .await?;
        let arr = unwrap_call_result(r);
        Ok(arr.iter().filter_map(parse_file_tuple).collect())
    }

    pub async fn get_file(&self, plugin: &str, filename: &str) -> Result<Option<FileEntry>> {
        let r = self
            .client
            .call(
                "loom.file_get",
                Value::Array(vec![Value::from(plugin), Value::from(filename)]),
            )
            .await?;
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

    pub async fn upsert_file(&self, plugin: &str, filename: &str, raw: &[u8]) -> Result<()> {
        self.client
            .call(
                "loom.file_upsert",
                Value::Array(vec![
                    Value::from(plugin),
                    Value::from(filename),
                    Value::from(raw),
                ]),
            )
            .await?;
        Ok(())
    }

    pub async fn delete_file(&self, plugin: &str, filename: &str) -> Result<()> {
        self.client
            .call(
                "loom.file_delete",
                Value::Array(vec![Value::from(plugin), Value::from(filename)]),
            )
            .await?;
        Ok(())
    }
}

fn unwrap_call_result(v: Value) -> Vec<Value> {
    match v {
        Value::Array(arr) => {
            if arr.len() == 1 {
                if let Value::Array(inner) = arr.into_iter().next().unwrap() {
                    return inner;
                }
                return vec![];
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
                meta: FileMeta {
                    plugin,
                    filename,
                    mtime,
                    size,
                },
                raw,
            });
        }
    }
    None
}
