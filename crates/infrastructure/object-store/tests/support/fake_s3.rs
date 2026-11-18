//! Loopback S3 fixture. Unguarded writes really replace the stored bytes.
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use musteragent_object_store::{ObjectStoreBlobStore, S3ObjectStoreConfig, WriteOnceStrategy};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, task::JoinHandle};

#[derive(Default)]
pub struct Bucket {
    pub objects: HashMap<String, Bytes>,
    uploads: HashMap<String, BTreeMap<usize, Bytes>>,
    pub guarded_puts: usize,
    pub guarded_completions: usize,
    pub copy_parts: usize,
    pub standard_rejections: usize,
    // Model a bucket or proxy which silently ignores a precondition.
    pub ignore_put: bool,
    pub ignore_complete: bool,
    pub ignore_standard: bool,
    pub collision_code: Option<&'static str>,
    header: &'static str,
}

pub struct Server {
    endpoint: String,
    pub bucket: Arc<Mutex<Bucket>>,
    task: JoinHandle<()>,
}

impl Server {
    pub async fn start(header: &'static str) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("loopback listener");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let bucket = Arc::new(Mutex::new(Bucket {
            header,
            ..Bucket::default()
        }));
        let router = Router::new()
            .fallback(handle)
            .layer(DefaultBodyLimit::max(10 << 20))
            .with_state(bucket.clone());
        let task =
            tokio::spawn(async move { axum::serve(listener, router).await.expect("fake S3") });
        Self {
            endpoint,
            bucket,
            task,
        }
    }

    pub fn store(&self, write_once: WriteOnceStrategy) -> ObjectStoreBlobStore {
        ObjectStoreBlobStore::from_s3(S3ObjectStoreConfig {
            bucket: "test-bucket".into(),
            region: "test-region".into(),
            endpoint: self.endpoint.clone(),
            prefix: "content".into(),
            access_key_id: "fixture".into(),
            secret_access_key: "fixture".into(),
            allow_http: true,
            virtual_hosted_style: false,
            write_once,
        })
        .expect("fixture configuration")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn failure(status: StatusCode, code: &str) -> Response {
    (status, format!("<Error><Code>{code}</Code></Error>")).into_response()
}

async fn handle(
    State(shared): State<Arc<Mutex<Bucket>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut bucket = shared.lock().expect("test bucket");
    let key = uri.path().trim_start_matches('/').to_owned();
    let query: HashMap<_, _> =
        url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes()).collect();
    let put = method == Method::PUT && query.is_empty();
    let completion = method == Method::POST && query.contains_key("uploadId");
    if headers.contains_key("if-none-match") && !bucket.ignore_standard {
        bucket.standard_rejections += 1;
        return failure(StatusCode::BAD_REQUEST, "NotImplemented");
    }
    if let Some(guard) = headers.get(bucket.header) {
        let signed = headers["authorization"]
            .to_str()
            .expect("signature")
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|s| s.split(',').next())
            .expect("signed headers");
        if guard != "true"
            || headers.contains_key("if-none-match")
            || !signed.split(';').any(|field| field == bucket.header)
            || headers["x-amz-content-sha256"] != hex::encode(Sha256::digest(&body))
            || !(put || completion)
        {
            return failure(StatusCode::BAD_REQUEST, "InvalidConditionalRequest");
        }
        if put {
            bucket.guarded_puts += 1;
        } else {
            bucket.guarded_completions += 1;
        }
        if bucket.objects.contains_key(&key)
            && !(put && bucket.ignore_put || completion && bucket.ignore_complete)
        {
            return failure(
                StatusCode::CONFLICT,
                bucket.collision_code.unwrap_or("FileAlreadyExists"),
            );
        }
    }
    if put {
        bucket.objects.insert(key, body);
        return ([("etag", "\"fixture\"")], "").into_response();
    }
    if method == Method::GET {
        return match bucket.objects.get(&key) {
            Some(bytes) => (
                [
                    ("last-modified", "Thu, 01 Jan 2026 00:00:00 GMT"),
                    ("etag", "\"fixture\""),
                ],
                bytes.clone(),
            )
                .into_response(),
            None => failure(StatusCode::NOT_FOUND, "NoSuchKey"),
        };
    }
    if method == Method::POST && query.contains_key("delete") {
        let xml = std::str::from_utf8(&body).expect("delete XML");
        let path = xml
            .split_once("<Key>")
            .and_then(|(_, s)| s.split_once("</Key>"))
            .expect("delete one object")
            .0;
        bucket.objects.remove(&format!("test-bucket/{path}"));
        return format!("<DeleteResult><Deleted><Key>{path}</Key></Deleted></DeleteResult>")
            .into_response();
    }
    if method == Method::POST && query.contains_key("uploads") {
        let id = uuid::Uuid::now_v7().to_string();
        bucket.uploads.insert(id.clone(), BTreeMap::new());
        return format!("<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>").into_response();
    }
    if let Some(id) = query.get("uploadId") {
        if method == Method::DELETE {
            bucket.uploads.remove(id.as_ref());
            return StatusCode::NO_CONTENT.into_response();
        }
        if let Some(part) = query.get("partNumber") {
            let source = headers.get("x-amz-copy-source");
            let bytes = match source {
                Some(src) => {
                    bucket.copy_parts += 1;
                    bucket.objects[src.to_str().expect("copy source").trim_start_matches('/')]
                        .clone()
                }
                None => body,
            };
            bucket
                .uploads
                .get_mut(id.as_ref())
                .expect("active upload")
                .insert(part.parse().expect("part number"), bytes);
            return if source.is_some() {
                "<CopyPartResult><ETag>fixture</ETag></CopyPartResult>".into_response()
            } else {
                ([("etag", "\"fixture\"")], "").into_response()
            };
        }
        if completion {
            let parts = bucket.uploads.remove(id.as_ref()).expect("active upload");
            let bytes = parts
                .into_values()
                .flat_map(Bytes::into_iter)
                .collect::<Vec<_>>();
            bucket.objects.insert(key, bytes.into());
            return "<CompleteMultipartUploadResult><ETag>fixture</ETag></CompleteMultipartUploadResult>".into_response();
        }
    }
    if method == Method::DELETE {
        bucket.objects.remove(&key);
        return StatusCode::NO_CONTENT.into_response();
    }
    failure(StatusCode::BAD_REQUEST, "UnexpectedRequest")
}
