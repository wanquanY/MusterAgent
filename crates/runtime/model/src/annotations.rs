//! Host annotations on a physical request. They never alter admitted authority.
use std::collections::BTreeMap;

/// Validated additional HTTP headers. Credential, routing and framing headers
/// remain owned by the transport; annotations cannot replace them.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ModelRequestAnnotations {
    headers: BTreeMap<String, String>,
}
impl ModelRequestAnnotations {
    /// Insert/replace an extension header. Repeated writes compose in hook order.
    /// An explicit `idempotency-key` is host-owned: derive it from the persisted
    /// invocation identity and preserve it across all retries/process generations.
    /// Omit it to use the native provider's stable invocation-derived key.
    pub fn insert_header(&mut self, name: &str, value: &str) -> Result<(), ModelAnnotationError> {
        let name = name.to_ascii_lowercase();
        if name.is_empty()
            || name.len() > 128
            || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || value.len() > 8192
            || !value.bytes().all(|b| (32..=126).contains(&b))
            || matches!(
                name.as_str(),
                "authorization"
                    | "proxy-authorization"
                    | "host"
                    | "content-length"
                    | "content-type"
                    | "transfer-encoding"
                    | "connection"
                    | "cookie"
                    | "accept"
                    | "x-api-key"
                    | "api-key"
            )
            || name.starts_with("x-amz-")
            || name.starts_with("x-goog-")
            || name.starts_with("x-musteragent-")
            || (self.headers.len() >= 32 && !self.headers.contains_key(&name))
        {
            return Err(ModelAnnotationError);
        }
        self.headers.insert(name, value.into());
        Ok(())
    }
    /// Headers for the native transport to apply before dispatch.
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }
    /// Whether the operation can safely use an unannotated adapter.
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }
}

/// Invalid or authority-changing annotation.
#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
#[error("invalid or reserved model request header")]
pub struct ModelAnnotationError;
