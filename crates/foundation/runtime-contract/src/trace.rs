use thiserror::Error;

const TRACEPARENT_BYTES: usize = 55;
const TRACESTATE_MAX_BYTES: usize = 512;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TraceParent(String);

impl TraceParent {
    pub fn new(value: impl Into<String>) -> Result<Self, TraceContextError> {
        let value = value.into();
        let bytes = value.as_bytes();
        let valid_shape = bytes.len() == TRACEPARENT_BYTES
            && bytes[2] == b'-'
            && bytes[35] == b'-'
            && bytes[52] == b'-'
            && bytes
                .iter()
                .enumerate()
                .filter(|(index, _)| !matches!(index, 2 | 35 | 52))
                .all(|(_, byte)| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
        if !valid_shape
            || &value[0..2] == "ff"
            || value[3..35].bytes().all(|byte| byte == b'0')
            || value[36..52].bytes().all(|byte| byte == b'0')
        {
            return Err(TraceContextError::InvalidTraceParent);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TraceState(String);

impl TraceState {
    pub fn new(value: impl Into<String>) -> Result<Self, TraceContextError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > TRACESTATE_MAX_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_graphic() || byte == b' ')
        {
            return Err(TraceContextError::InvalidTraceState);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TraceContext {
    pub traceparent: TraceParent,
    pub tracestate: Option<TraceState>,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum TraceContextError {
    #[error("traceparent must be a non-zero lowercase W3C version 00 value")]
    InvalidTraceParent,
    #[error("tracestate must be bounded non-empty visible ASCII")]
    InvalidTraceState,
}
