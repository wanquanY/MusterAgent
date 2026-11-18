//! Keep the S3 client's request construction, retries and multipart aborts. Translate
//! the destination precondition at its HTTP connector and sign the final request.
use crate::{ObjectStoreConfigError, S3ObjectStoreConfig};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use http::{HeaderName, HeaderValue, Method, StatusCode, header};
use object_store::aws::{
    AmazonS3, AmazonS3Builder, AwsAuthorizer, AwsCredential, AwsCredentialProvider,
    S3CopyIfNotExists,
};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService,
    ReqwestConnector,
};
use object_store::{ClientOptions, StaticCredentialProvider};
use serde::Deserialize;
use std::sync::Arc;

/// Destination precondition for immutable S3 publications.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "strategy", rename_all = "kebab-case", deny_unknown_fields)]
pub enum WriteOnceStrategy {
    /// S3 If-None-Match: * on PutObject and CompleteMultipartUpload.
    Standard {},
    /// Send a signed, host-selected extension header with value `true`.
    ForbidOverwriteHeader {
        /// HTTP field name documented by the storage service.
        header: String,
    },
}

impl Default for WriteOnceStrategy {
    fn default() -> Self {
        Self::Standard {}
    }
}

pub(super) fn build(config: &S3ObjectStoreConfig) -> Result<AmazonS3, ObjectStoreConfigError> {
    let credentials: AwsCredentialProvider =
        Arc::new(StaticCredentialProvider::new(AwsCredential {
            key_id: config.access_key_id.clone(),
            secret_key: config.secret_access_key.clone(),
            token: None,
        }));
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_endpoint(&config.endpoint)
        .with_credentials(credentials.clone())
        .with_allow_http(config.allow_http)
        .with_virtual_hosted_style_request(config.virtual_hosted_style)
        .with_copy_if_not_exists(S3CopyIfNotExists::Multipart);
    if let WriteOnceStrategy::ForbidOverwriteHeader { header } = &config.write_once {
        let header: HeaderName = header
            .parse()
            .map_err(|_| ObjectStoreConfigError::InvalidWriteOnceHeader)?;
        // Reserve S3 protocol, metadata and signing fields. Only a dedicated
        // extension may replace the destination precondition.
        if !header.as_str().starts_with("x-") || header.as_str().starts_with("x-amz-") {
            return Err(ObjectStoreConfigError::InvalidWriteOnceHeader);
        }
        builder = builder.with_http_connector(HeaderStrategy {
            header,
            region: config.region.clone(),
            credentials,
        });
    }
    builder.build().map_err(|_| ObjectStoreConfigError::Build)
}

#[derive(Clone)]
struct HeaderStrategy {
    header: HeaderName,
    region: String,
    credentials: AwsCredentialProvider,
}

impl std::fmt::Debug for HeaderStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderStrategy")
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}

impl HttpConnector for HeaderStrategy {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(ConditionalClient {
            transport: ReqwestConnector::default().connect(options)?,
            strategy: self.clone(),
        }))
    }
}

#[derive(Debug)]
struct ConditionalClient {
    transport: HttpClient,
    strategy: HeaderStrategy,
}

fn protocol_error(message: &'static str) -> HttpError {
    HttpError::new(HttpErrorKind::Decode, std::io::Error::other(message))
}

#[async_trait]
impl HttpService for ConditionalClient {
    async fn call(&self, mut req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let guarded = (req.method() == Method::PUT || req.method() == Method::POST)
            && req
                .headers()
                .get(header::IF_NONE_MATCH)
                .is_some_and(|v| v == "*");
        if guarded {
            let headers = req.headers_mut();
            headers.remove(header::IF_NONE_MATCH);
            headers.remove(header::AUTHORIZATION);
            headers.insert(
                self.strategy.header.clone(),
                HeaderValue::from_static("true"),
            );
            // The builder uses signed payloads. Preserve the digest of the original
            // body, including PutPayload streams, without re-reading the bytes.
            let mut digest = [0; 32];
            let hash = headers
                .get("x-amz-content-sha256")
                .ok_or_else(|| protocol_error("missing signed payload digest"))?;
            hex::decode_to_slice(hash.as_bytes(), &mut digest)
                .map_err(|_| protocol_error("invalid signed payload digest"))?;
            let credential = self
                .strategy
                .credentials
                .get_credential()
                .await
                .map_err(|e| HttpError::new(HttpErrorKind::Unknown, e))?;
            AwsAuthorizer::new(&credential, "s3", &self.strategy.region)
                .try_authorize(&mut req, Some(&digest))
                .map_err(|e| HttpError::new(HttpErrorKind::Unknown, e))?;
        }

        let response = self.transport.execute(req).await?;
        if response.status() == StatusCode::CONFLICT {
            // Upstream treats every 409, even on UploadPartCopy, as AlreadyExists.
            // Only a guarded destination reporting the vendor collision may enter
            // the adapter's existing reread-and-verify path.
            if !guarded {
                return Err(protocol_error(
                    "unexpected S3 conflict before conditional publication",
                ));
            }
            return file_already_exists(response).await;
        }
        if guarded
            && matches!(
                response.status(),
                StatusCode::PRECONDITION_FAILED | StatusCode::NOT_MODIFIED
            )
        {
            return Err(protocol_error(
                "unexpected response to forbid-overwrite precondition",
            ));
        }
        Ok(response)
    }
}

#[derive(Deserialize)]
enum ErrorEnvelope {
    Error {
        #[serde(rename = "Code")]
        code: String,
    },
}

async fn file_already_exists(response: HttpResponse) -> Result<HttpResponse, HttpError> {
    let (parts, body) = response.into_parts();
    let mut stream = body.bytes_stream();
    let mut xml = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if chunk.len() > (64 << 10) - xml.len() {
            return Err(protocol_error("oversize S3 conflict response"));
        }
        xml.extend_from_slice(&chunk);
    }
    match quick_xml::de::from_reader(xml.as_slice()) {
        Ok(ErrorEnvelope::Error { code }) if code == "FileAlreadyExists" => {
            Ok(HttpResponse::from_parts(parts, Bytes::from(xml).into()))
        }
        _ => Err(protocol_error(
            "S3 conflict did not report FileAlreadyExists",
        )),
    }
}
