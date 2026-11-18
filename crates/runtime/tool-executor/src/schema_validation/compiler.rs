//! One bounded compiler per process. JSON Schema's recursive meta-schema and
//! regex compilation must not borrow a Run/transaction thread's small stack.
//! Immutable validators remain keyed by the exact Snapshot schema hash.
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, OnceLock, RwLock};

use musteragent_content_ref::Sha256;
use musteragent_runtime_contract::ErrorCode;

type Compiled = Result<Arc<jsonschema::Validator>, ErrorCode>;
const CACHE_CAPACITY: usize = 512;
const QUEUE_CAPACITY: usize = 16;
// Includes the library's cold Draft 2020-12 meta-schema compiler in debug builds.
const COMPILER_STACK_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
struct Cache {
    entries: BTreeMap<Sha256, Compiled>,
    order: VecDeque<Sha256>,
}
impl Cache {
    fn insert(&mut self, hash: Sha256, compiled: Compiled) {
        if self.entries.len() == CACHE_CAPACITY
            && let Some(oldest) = self.order.pop_front()
        {
            self.entries.remove(&oldest);
        }
        self.entries.insert(hash, compiled);
        self.order.push_back(hash);
    }
}

struct Request {
    hash: Sha256,
    bytes: Vec<u8>,
    reply: flume::Sender<Compiled>,
}
struct Compiler {
    queue: flume::Sender<Request>,
    cache: Arc<RwLock<Cache>>,
}

impl Compiler {
    fn start() -> Result<Self, ErrorCode> {
        let (queue, receiver) = flume::bounded::<Request>(QUEUE_CAPACITY);
        let cache = Arc::new(RwLock::new(Cache::default()));
        let worker_cache = cache.clone();
        std::thread::Builder::new()
            .name("tool-schema-compiler".to_owned())
            .stack_size(COMPILER_STACK_BYTES)
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    // A cancelled waiter conveys no authority and needs no work.
                    if request.reply.is_disconnected() {
                        continue;
                    }
                    let compiled = match lookup(&worker_cache, request.hash) {
                        Ok(Some(cached)) => cached,
                        Ok(None) => {
                            let compiled = compile(&request.bytes);
                            match worker_cache.write() {
                                Ok(mut cache) => cache.insert(request.hash, compiled.clone()),
                                Err(_) => {
                                    let _ = request
                                        .reply
                                        .send(Err(ErrorCode::InternalInvariantViolated));
                                    continue;
                                }
                            }
                            compiled
                        }
                        Err(error) => Err(error),
                    };
                    let _ = request.reply.send(compiled);
                }
            })
            .map_err(|_| ErrorCode::DependencyUnavailable)?;
        Ok(Self { queue, cache })
    }
}

fn compiler() -> Result<&'static Compiler, ErrorCode> {
    static COMPILER: OnceLock<Result<Compiler, ErrorCode>> = OnceLock::new();
    COMPILER
        .get_or_init(Compiler::start)
        .as_ref()
        .map_err(|e| *e)
}

fn lookup(cache: &RwLock<Cache>, hash: Sha256) -> Result<Option<Compiled>, ErrorCode> {
    Ok(cache
        .read()
        .map_err(|_| ErrorCode::InternalInvariantViolated)?
        .entries
        .get(&hash)
        .cloned())
}

fn compile(bytes: &[u8]) -> Compiled {
    let schema: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
    jsonschema::draft202012::meta::validate(&schema)
        .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
    jsonschema::draft202012::options()
        .build(&schema)
        .map(Arc::new)
        .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)
}

fn prepare(hash: Sha256, bytes: &[u8]) -> Result<(&'static Compiler, Option<Compiled>), ErrorCode> {
    // Even a cache hit must verify the supplied Snapshot material.
    if Sha256::digest(bytes) != hash {
        return Err(ErrorCode::ContentIntegrityFailed);
    }
    let owner = compiler()?;
    Ok((owner, lookup(&owner.cache, hash)?))
}

pub(super) fn get(hash: Sha256, bytes: &[u8]) -> Compiled {
    let (owner, cached) = prepare(hash, bytes)?;
    if let Some(cached) = cached {
        return cached;
    }
    let (reply, result) = flume::bounded(1);
    owner
        .queue
        .send(Request {
            hash,
            bytes: bytes.to_vec(),
            reply,
        })
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    result
        .recv()
        .map_err(|_| ErrorCode::DependencyUnavailable)?
}

pub(super) async fn get_async(hash: Sha256, bytes: &[u8]) -> Compiled {
    let (owner, cached) = prepare(hash, bytes)?;
    if let Some(cached) = cached {
        return cached;
    }
    let (reply, result) = flume::bounded(1);
    owner
        .queue
        .send_async(Request {
            hash,
            bytes: bytes.to_vec(),
            reply,
        })
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    result
        .recv_async()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_compilation_does_not_use_the_callers_stack_and_cache_is_hash_pinned() {
        // Run this test alone as well: a prewarmed JSON Schema global would hide
        // the original cold compiler stack overflow.
        std::thread::Builder::new().stack_size(512 * 1024).spawn(|| {
            let bytes = br#"{"type":"object","properties":{"name":{"type":"string","pattern":"^[a-z]+$"}},"required":["name"],"additionalProperties":false}"#;
            let hash = Sha256::digest(bytes);
            let one = get(hash, bytes).unwrap();
            assert!(one.is_valid(&serde_json::json!({"name":"valid"})));
            assert!(!one.is_valid(&serde_json::json!({"name":42})));
            let two = get(hash, bytes).unwrap();
            assert!(Arc::ptr_eq(&one, &two));
            assert!(matches!(get(hash, b"{}"), Err(ErrorCode::ContentIntegrityFailed)));
            let bad = br#"{"type":17}"#;
            assert!(matches!(get(Sha256::digest(bad), bad), Err(ErrorCode::RuntimeSnapshotInvalid)));
        }).unwrap().join().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_cold_requests_share_one_validator_without_blocking_the_reactor() {
        let bytes =
            br#"{"type":"object","properties":{"unique_async_cache_probe":{"type":"boolean"}}}"#;
        let hash = Sha256::digest(bytes);
        // More requests than the bounded queue capacity exercises async backpressure.
        let mut jobs = Vec::new();
        for _ in 0..QUEUE_CAPACITY * 2 {
            jobs.push(tokio::spawn(async move {
                get_async(hash, bytes).await.unwrap()
            }));
        }
        tokio::task::yield_now().await;
        let expected = get_async(hash, bytes).await.unwrap();
        for job in jobs {
            assert!(Arc::ptr_eq(&expected, &job.await.unwrap()));
        }
    }
}
