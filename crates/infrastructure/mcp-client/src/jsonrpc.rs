//! Strict JSON-RPC 2.0 framing for MCP.
//!
//! The client only ever issues positive integer request ids. Inbound ids may
//! be integers or strings (server-to-client requests); anything else is a
//! protocol violation.

use serde_json::{Map, Value, json};

use crate::McpClientError;
use crate::error::{MAX_REMOTE_ERROR_MESSAGE_BYTES, McpRemoteError};
use crate::untrusted::UntrustedText;

pub const JSONRPC_VERSION: &str = "2.0";
pub const METHOD_NOT_FOUND: i64 = -32_601;
const MAX_METHOD_BYTES: usize = 256;
const MAX_STRING_ID_BYTES: usize = 256;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum RequestId {
    Number(i64),
    String(String),
}

impl RequestId {
    fn to_value(&self) -> Value {
        match self {
            Self::Number(value) => json!(value),
            Self::String(value) => json!(value),
        }
    }
}

/// One decoded inbound JSON-RPC message.
#[derive(Debug, Clone, PartialEq)]
pub enum Incoming {
    Response {
        id: RequestId,
        outcome: Result<Value, McpRemoteError>,
    },
    /// An error response with `id: null` (the server could not parse or
    /// identify a request). It cannot be correlated.
    UncorrelatedError(McpRemoteError),
    Request {
        id: RequestId,
        method: String,
        params: Option<Value>,
    },
    Notification {
        method: String,
        params: Option<Value>,
    },
}

fn finish(value: &Value, limit: usize) -> Result<Vec<u8>, McpClientError> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| McpClientError::InvalidRequest("unserializable"))?;
    if bytes.len() > limit {
        return Err(McpClientError::MessageTooLarge { limit });
    }
    Ok(bytes)
}

fn validate_method(method: &str) -> Result<(), McpClientError> {
    if method.is_empty() || method.len() > MAX_METHOD_BYTES || method.chars().any(char::is_control)
    {
        return Err(McpClientError::InvalidRequest("method"));
    }
    Ok(())
}

fn validate_params(params: Option<&Value>) -> Result<(), McpClientError> {
    if params.is_some_and(|value| !value.is_object() && !value.is_array()) {
        return Err(McpClientError::InvalidRequest("params"));
    }
    Ok(())
}

pub fn encode_request(
    id: u64,
    method: &str,
    params: Option<Value>,
    limit: usize,
) -> Result<Vec<u8>, McpClientError> {
    validate_method(method)?;
    validate_params(params.as_ref())?;
    if id == 0 || id > i64::MAX as u64 {
        return Err(McpClientError::InvalidRequest("request id"));
    }
    let mut message = Map::new();
    message.insert("jsonrpc".into(), json!(JSONRPC_VERSION));
    message.insert("id".into(), json!(id));
    message.insert("method".into(), json!(method));
    if let Some(params) = params {
        message.insert("params".into(), params);
    }
    finish(&Value::Object(message), limit)
}

pub fn encode_notification(
    method: &str,
    params: Option<Value>,
    limit: usize,
) -> Result<Vec<u8>, McpClientError> {
    validate_method(method)?;
    validate_params(params.as_ref())?;
    let mut message = Map::new();
    message.insert("jsonrpc".into(), json!(JSONRPC_VERSION));
    message.insert("method".into(), json!(method));
    if let Some(params) = params {
        message.insert("params".into(), params);
    }
    finish(&Value::Object(message), limit)
}

pub fn encode_result(
    id: &RequestId,
    result: Value,
    limit: usize,
) -> Result<Vec<u8>, McpClientError> {
    finish(
        &json!({"jsonrpc": JSONRPC_VERSION, "id": id.to_value(), "result": result}),
        limit,
    )
}

pub fn encode_error(
    id: &RequestId,
    code: i64,
    message: &str,
    limit: usize,
) -> Result<Vec<u8>, McpClientError> {
    finish(
        &json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": id.to_value(),
            "error": {"code": code, "message": message}
        }),
        limit,
    )
}

/// Decodes one framed message: a single JSON-RPC object or a bounded batch.
pub fn decode_incoming(
    bytes: &[u8],
    max_message_bytes: usize,
    max_batch: usize,
) -> Result<Vec<Incoming>, McpClientError> {
    if bytes.len() > max_message_bytes {
        return Err(McpClientError::MessageTooLarge {
            limit: max_message_bytes,
        });
    }
    let value = serde_json::from_slice::<Value>(bytes)
        .map_err(|_| McpClientError::ProtocolViolation("message is not JSON"))?;
    match value {
        Value::Array(items) => {
            if items.is_empty() || items.len() > max_batch {
                return Err(McpClientError::ProtocolViolation("batch size"));
            }
            items.into_iter().map(decode_one).collect()
        }
        value => Ok(vec![decode_one(value)?]),
    }
}

fn decode_id(value: &Value) -> Result<RequestId, McpClientError> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .map(RequestId::Number)
            .ok_or(McpClientError::ProtocolViolation("non-integer id")),
        Value::String(text) if text.len() <= MAX_STRING_ID_BYTES => {
            Ok(RequestId::String(text.clone()))
        }
        _ => Err(McpClientError::ProtocolViolation("id type")),
    }
}

fn decode_remote_error(value: &Value) -> Result<McpRemoteError, McpClientError> {
    let object = value
        .as_object()
        .ok_or(McpClientError::ProtocolViolation("error object"))?;
    let code = object
        .get("code")
        .and_then(Value::as_i64)
        .ok_or(McpClientError::ProtocolViolation("error code"))?;
    let message = object.get("message").and_then(Value::as_str).unwrap_or("");
    Ok(McpRemoteError {
        code,
        message: UntrustedText::bounded(message, MAX_REMOTE_ERROR_MESSAGE_BYTES),
    })
}

fn decode_one(value: Value) -> Result<Incoming, McpClientError> {
    let Value::Object(mut object) = value else {
        return Err(McpClientError::ProtocolViolation(
            "message is not an object",
        ));
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some(JSONRPC_VERSION) {
        return Err(McpClientError::ProtocolViolation("jsonrpc version"));
    }
    let params = object.remove("params");
    if params
        .as_ref()
        .is_some_and(|value| !value.is_object() && !value.is_array())
    {
        return Err(McpClientError::ProtocolViolation("params type"));
    }
    if let Some(method) = object.get("method") {
        let method = method
            .as_str()
            .ok_or(McpClientError::ProtocolViolation("method type"))?;
        if method.is_empty() || method.len() > MAX_METHOD_BYTES {
            return Err(McpClientError::ProtocolViolation("method"));
        }
        if object.contains_key("result") || object.contains_key("error") {
            return Err(McpClientError::ProtocolViolation("request with result"));
        }
        return Ok(match object.get("id") {
            None => Incoming::Notification {
                method: method.to_owned(),
                params,
            },
            Some(id) => Incoming::Request {
                id: decode_id(id)?,
                method: method.to_owned(),
                params,
            },
        });
    }
    let result = object.remove("result");
    let error = object.remove("error");
    let id = object
        .get("id")
        .ok_or(McpClientError::ProtocolViolation("response without id"))?;
    match (result, error) {
        (Some(result), None) => Ok(Incoming::Response {
            id: decode_id(id)?,
            outcome: Ok(result),
        }),
        (None, Some(error)) => {
            let error = decode_remote_error(&error)?;
            if id.is_null() {
                Ok(Incoming::UncorrelatedError(error))
            } else {
                Ok(Incoming::Response {
                    id: decode_id(id)?,
                    outcome: Err(error),
                })
            }
        }
        _ => Err(McpClientError::ProtocolViolation(
            "response must carry exactly one of result or error",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: usize = 1024;

    #[test]
    fn requests_and_notifications_are_framed_exactly() {
        let bytes = encode_request(7, "tools/list", Some(json!({})), LIMIT).expect("encode");
        let value: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(
            value,
            json!({"jsonrpc":"2.0","id":7,"method":"tools/list","params":{}})
        );
        assert!(!bytes.contains(&b'\n'));
        let bytes = encode_notification("notifications/initialized", None, LIMIT).expect("encode");
        let value: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(
            value,
            json!({"jsonrpc":"2.0","method":"notifications/initialized"})
        );
        assert_eq!(
            encode_request(0, "ping", None, LIMIT),
            Err(McpClientError::InvalidRequest("request id"))
        );
        assert_eq!(
            encode_request(1, "ping", Some(json!(3)), LIMIT),
            Err(McpClientError::InvalidRequest("params"))
        );
        assert_eq!(
            encode_request(1, "x", Some(json!({"a": "b".repeat(LIMIT)})), LIMIT),
            Err(McpClientError::MessageTooLarge { limit: LIMIT })
        );
    }

    #[test]
    fn responses_requests_and_notifications_are_classified() {
        let decoded = decode_incoming(
            br#"[{"jsonrpc":"2.0","id":1,"result":{}},
                {"jsonrpc":"2.0","id":"s-1","method":"ping"},
                {"jsonrpc":"2.0","method":"notifications/tools/list_changed"},
                {"jsonrpc":"2.0","id":2,"error":{"code":-32602,"message":"bad"}},
                {"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse"}}]"#,
            LIMIT,
            8,
        )
        .expect("decode");
        assert_eq!(decoded.len(), 5);
        assert!(matches!(
            &decoded[0],
            Incoming::Response {
                id: RequestId::Number(1),
                outcome: Ok(_)
            }
        ));
        assert!(
            matches!(&decoded[1], Incoming::Request { id: RequestId::String(id), method, .. } if id == "s-1" && method == "ping")
        );
        assert!(
            matches!(&decoded[2], Incoming::Notification { method, .. } if method == "notifications/tools/list_changed")
        );
        assert!(matches!(
            &decoded[3],
            Incoming::Response {
                outcome: Err(McpRemoteError { code: -32602, .. }),
                ..
            }
        ));
        assert!(matches!(&decoded[4], Incoming::UncorrelatedError(_)));
    }

    #[test]
    fn malformed_messages_are_protocol_violations() {
        for bytes in [
            &br#"{"jsonrpc":"1.0","id":1,"result":{}}"#[..],
            br#"{"jsonrpc":"2.0","id":1}"#,
            br#"{"jsonrpc":"2.0","id":1,"result":{},"error":{"code":1}}"#,
            br#"{"jsonrpc":"2.0","id":1.5,"result":{}}"#,
            br#"{"jsonrpc":"2.0","id":{},"result":{}}"#,
            br#"{"jsonrpc":"2.0","result":{}}"#,
            br#"{"jsonrpc":"2.0","method":"x","params":3}"#,
            br#"[]"#,
            br#"not json"#,
            br#"7"#,
        ] {
            assert!(
                matches!(
                    decode_incoming(bytes, LIMIT, 8),
                    Err(McpClientError::ProtocolViolation(_))
                ),
                "{}",
                String::from_utf8_lossy(bytes)
            );
        }
        let oversized = vec![b' '; LIMIT + 1];
        assert_eq!(
            decode_incoming(&oversized, LIMIT, 8),
            Err(McpClientError::MessageTooLarge { limit: LIMIT })
        );
        let batch = format!("[{}]", [r#"{"jsonrpc":"2.0","method":"n"}"#; 3].join(","));
        assert_eq!(
            decode_incoming(batch.as_bytes(), LIMIT, 2),
            Err(McpClientError::ProtocolViolation("batch size"))
        );
    }

    #[test]
    fn replies_to_server_requests_echo_the_server_id() {
        let bytes = encode_result(&RequestId::String("abc".into()), json!({}), LIMIT).expect("ok");
        let value: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(value, json!({"jsonrpc":"2.0","id":"abc","result":{}}));
        let bytes =
            encode_error(&RequestId::Number(4), METHOD_NOT_FOUND, "nope", LIMIT).expect("ok");
        let value: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(value["error"]["code"], json!(METHOD_NOT_FOUND));
        assert_eq!(value["id"], json!(4));
    }
}
