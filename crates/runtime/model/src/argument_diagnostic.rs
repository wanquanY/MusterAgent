//! Value-free evidence of the exact argument representation rejected by a
//! provider adapter. Never serialize raw arguments or parser error prose here.
use crate::{GenerationLimit, ModelUsage, StopReason};
use musteragent_content_ref::Sha256;
use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgumentSource {
    Stream,
    InitialSnapshot,
    ArgumentsDone,
    OutputItemDone,
    TerminalSnapshot,
    CanonicalValue,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonParseCategory {
    Eof,
    Syntax,
    Data,
    Io,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonParseIssue {
    pub category: JsonParseCategory,
    pub line: u64,
    pub column: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArgumentDiagnostic {
    pub version: u8,
    pub source: ArgumentSource,
    pub received_bytes: u64,
    pub sha256: String,
    #[serde(deserialize_with = "required_option")]
    pub parse: Option<JsonParseIssue>,
    /// The observed limit signal. A configured budget marks finalization;
    /// before finalization no terminal fact is available.
    #[serde(deserialize_with = "required_option")]
    pub generation_limit: Option<GenerationLimit>,
    #[serde(deserialize_with = "required_option")]
    pub configured_output_tokens: Option<u64>,
    #[serde(deserialize_with = "required_option")]
    pub reported_output_tokens: Option<u64>,
    #[serde(deserialize_with = "required_option")]
    pub configured_argument_bytes: Option<u64>,
}

fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

impl ArgumentDiagnostic {
    pub const MAX_ENCODED_BYTES: usize = 1024;

    #[must_use]
    pub fn observed(
        arguments: &str,
        source: ArgumentSource,
        parse: Option<JsonParseIssue>,
    ) -> Self {
        Self {
            version: 1,
            source,
            received_bytes: arguments.len() as u64,
            sha256: Sha256::digest(arguments.as_bytes()).to_string(),
            parse,
            generation_limit: None,
            configured_output_tokens: None,
            reported_output_tokens: None,
            configured_argument_bytes: None,
        }
    }

    pub fn finish(
        &mut self,
        stop: StopReason,
        max_tokens: u64,
        max_bytes: usize,
        usage: Option<ModelUsage>,
    ) {
        self.generation_limit = stop.generation_limit();
        self.configured_output_tokens = Some(max_tokens);
        self.configured_argument_bytes = Some(max_bytes as u64);
        self.reported_output_tokens = usage.map(|usage| usage.output_tokens);
    }

    pub fn validate(&self) -> Result<(), ArgumentDiagnosticError> {
        if self.version != 1
            || self.received_bytes > 16 * 1_048_576
            || self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.parse.as_ref().is_some_and(|p| {
                p.line == 0
                    || p.line > self.received_bytes + 1
                    || p.column > self.received_bytes + 1
            })
            || self.configured_output_tokens == Some(0)
            || self
                .configured_argument_bytes
                .is_some_and(|n| n == 0 || n > 16 * 1_048_576 || n < self.received_bytes)
            || (self.configured_output_tokens.is_some() != self.configured_argument_bytes.is_some())
            || ((self.generation_limit.is_some() || self.reported_output_tokens.is_some())
                && self.configured_output_tokens.is_none())
        {
            return Err(ArgumentDiagnosticError);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, ArgumentDiagnosticError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| ArgumentDiagnosticError)?;
        if bytes.len() > Self::MAX_ENCODED_BYTES {
            return Err(ArgumentDiagnosticError);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ArgumentDiagnosticError> {
        if bytes.len() > Self::MAX_ENCODED_BYTES {
            return Err(ArgumentDiagnosticError);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| ArgumentDiagnosticError)?;
        value.validate()?;
        Ok(value)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("tool argument diagnostic is invalid")]
pub struct ArgumentDiagnosticError;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ambiguous_limit_stays_ambiguous_regardless_of_usage() {
        for output_tokens in [None, Some(1), Some(16384)] {
            let mut value = ArgumentDiagnostic::observed("{", ArgumentSource::Stream, None);
            value.finish(
                StopReason::TokenLimit,
                16384,
                1048576,
                output_tokens.map(|output_tokens| ModelUsage {
                    output_tokens,
                    ..ModelUsage::default()
                }),
            );
            assert_eq!(
                value.generation_limit,
                Some(GenerationLimit::OutputOrContextWindow)
            );
            assert_eq!(
                ArgumentDiagnostic::decode(&value.encode().expect("encode diagnostic"))
                    .expect("decode diagnostic"),
                value
            );
        }
    }

    #[test]
    fn diagnostic_codec_is_bounded_closed_and_value_free() {
        let mut value = ArgumentDiagnostic::observed(
            "{\"private\":",
            ArgumentSource::Stream,
            Some(JsonParseIssue {
                category: JsonParseCategory::Eof,
                line: 1,
                column: 11,
            }),
        );
        value.finish(StopReason::ToolCalls, 16384, 1048576, None);
        let bytes = value.encode().expect("valid evidence");
        assert_eq!(
            ArgumentDiagnostic::decode(&bytes).expect("roundtrip"),
            value
        );
        assert!(
            !String::from_utf8(bytes.clone())
                .expect("UTF-8")
                .contains("private")
        );
        for (field, replacement) in [
            ("version", serde_json::json!(2)),
            ("sha256", serde_json::json!("invalid")),
            ("received_bytes", serde_json::json!(16777217)),
            ("configured_argument_bytes", serde_json::json!(1)),
            ("raw_arguments", serde_json::json!("private")),
        ] {
            let mut encoded = serde_json::to_value(&value).expect("JSON");
            encoded[field] = replacement;
            assert!(
                ArgumentDiagnostic::decode(encoded.to_string().as_bytes()).is_err(),
                "{field}"
            );
        }
        let mut stripped = serde_json::to_value(&value).expect("JSON");
        stripped.as_object_mut().expect("object").remove("parse");
        assert!(ArgumentDiagnostic::decode(stripped.to_string().as_bytes()).is_err());
        assert!(ArgumentDiagnostic::decode(&vec![b' '; 1025]).is_err());
        value.parse.as_mut().expect("parse").line = 0;
        assert!(value.encode().is_err());
    }
}
