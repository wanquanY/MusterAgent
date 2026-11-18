//! The named method and output contract implemented by one backend adapter.
use super::{BuiltinIdempotency, ToolCatalogError};
use musteragent_policy::Effect;
use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BackendOperation {
    name: Arc<str>,
    media_artifact: Option<MediaArtifactKind>,
}

impl BackendOperation {
    /// Exactly two ASCII identifiers, with no paths, wildcards or generic dispatcher.
    pub fn new(
        name: &str,
        media_artifact: Option<MediaArtifactKind>,
    ) -> Result<Self, ToolCatalogError> {
        let valid_identifier = |part: &str| {
            part.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
                && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        };
        if name.len() > 128 {
            return Err(ToolCatalogError::InvalidBackendOperation);
        }
        let (service, method) = name
            .split_once('.')
            .ok_or(ToolCatalogError::InvalidBackendOperation)?;
        if !valid_identifier(service)
            || !valid_identifier(method)
            || method
                .bytes()
                .filter(|b| *b != b'_')
                .map(|b| b.to_ascii_lowercase())
                .eq(b"executetool".iter().copied())
        {
            return Err(ToolCatalogError::InvalidBackendOperation);
        }
        Ok(Self {
            name: name.into(),
            media_artifact,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.name
    }

    pub fn media_artifact(&self) -> Option<MediaArtifactKind> {
        self.media_artifact
    }
}

/// The artifact and private source proof the Runtime must materialize.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MediaArtifactKind {
    Image,
    Video,
}

impl MediaArtifactKind {
    fn parse(value: &str) -> Result<Self, ToolCatalogError> {
        match value {
            "image" => Ok(Self::Image),
            "video" => Ok(Self::Video),
            _ => Err(ToolCatalogError::InvalidExecutionTarget),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Video => "video",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CapabilityExecutionTarget {
    RustRuntime,
    BackendNamed(BackendOperation),
}

impl CapabilityExecutionTarget {
    pub fn as_str(&self) -> &str {
        match self {
            Self::RustRuntime => "rust_runtime",
            Self::BackendNamed(operation) => operation.as_str(),
        }
    }

    pub fn backend_operation(&self) -> Option<&BackendOperation> {
        match self {
            Self::RustRuntime => None,
            Self::BackendNamed(operation) => Some(operation),
        }
    }

    pub fn media_artifact(&self) -> Option<MediaArtifactKind> {
        self.backend_operation()
            .and_then(BackendOperation::media_artifact)
    }

    pub fn validate_effect(
        &self,
        effect: Effect,
        idempotency: BuiltinIdempotency,
    ) -> Result<(), ToolCatalogError> {
        if self.media_artifact().is_some()
            && (effect != Effect::WriteExternal
                || idempotency != BuiltinIdempotency::ProviderTaskRecord)
        {
            return Err(ToolCatalogError::InvalidExecutionTarget);
        }
        Ok(())
    }

    /// Restore both fields of a sealed target. Missing artifact type denotes an ordinary call.
    pub fn parse(value: &str, artifact_type: Option<&str>) -> Result<Self, ToolCatalogError> {
        match (value, artifact_type) {
            ("rust_runtime", None) => Ok(Self::RustRuntime),
            ("rust_runtime", Some(_)) => Err(ToolCatalogError::InvalidExecutionTarget),
            _ => {
                let kind = artifact_type.map(MediaArtifactKind::parse).transpose()?;
                BackendOperation::new(value, kind).map(Self::BackendNamed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_names_are_bounded_exact_identifiers() {
        for name in ["Reader2.Read_one", "a.B", &format!("A.{}", "x".repeat(126))] {
            assert_eq!(BackendOperation::new(name, None).unwrap().as_str(), name);
        }
        for name in [
            "",
            "ExecuteTool",
            "Host.ExecuteTool",
            "Host.execute_tool",
            "A.",
            ".B",
            "A.B.C",
            "1Svc.Read",
            "Svc.1Read",
            "A.*",
            "A./path",
            "S.读",
            "A.Read ",
            "A.B\0",
            &format!("A.{}", "x".repeat(127)),
        ] {
            assert!(BackendOperation::new(name, None).is_err(), "{name:?}");
        }
    }
}
