//! Bounded, value-free failures of the original strict delivery decoder.
use musteragent_content_ref::Sha256;

pub const MAX_DELIVERY_DIAGNOSTICS: usize = 3;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum PublicDeliveryDiagnosticKind {
    MalformedJson,
    UnexpectedEnd,
    ObjectRequired,
    MissingRequired,
    DuplicateProperty,
    UnexpectedProperty,
    InvalidType,
    InvalidValue,
    ReferenceSyntax,
    ReferenceUnavailable,
    TooLarge,
}

impl PublicDeliveryDiagnosticKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedJson => "malformed_json",
            Self::UnexpectedEnd => "unexpected_end",
            Self::ObjectRequired => "object_required",
            Self::MissingRequired => "missing_required",
            Self::DuplicateProperty => "duplicate_property",
            Self::UnexpectedProperty => "unexpected_property",
            Self::InvalidType => "invalid_type",
            Self::InvalidValue => "invalid_value",
            Self::ReferenceSyntax => "reference_syntax",
            Self::ReferenceUnavailable => "reference_unavailable",
            Self::TooLarge => "too_large",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        [
            Self::MalformedJson,
            Self::UnexpectedEnd,
            Self::ObjectRequired,
            Self::MissingRequired,
            Self::DuplicateProperty,
            Self::UnexpectedProperty,
            Self::InvalidType,
            Self::InvalidValue,
            Self::ReferenceSyntax,
            Self::ReferenceUnavailable,
            Self::TooLarge,
        ]
        .into_iter()
        .find(|kind| kind.as_str() == value)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct PublicDeliveryDiagnostic {
    kind: PublicDeliveryDiagnosticKind,
    path: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("invalid private delivery repair contract")]
pub struct PublicDeliveryRepairError;

impl PublicDeliveryDiagnostic {
    pub fn new(
        kind: PublicDeliveryDiagnosticKind,
        path: impl Into<String>,
    ) -> Result<Self, PublicDeliveryRepairError> {
        let path = path.into();
        if !valid_path(&path) {
            return Err(PublicDeliveryRepairError);
        }
        Ok(Self { kind, path })
    }
    pub const fn kind(&self) -> PublicDeliveryDiagnosticKind {
        self.kind
    }
    pub fn path(&self) -> &str {
        &self.path
    }
}

fn valid_path(path: &str) -> bool {
    if path == "/" {
        return true;
    }
    if path.len() > 256 || !path.starts_with("/deliveries") {
        return false;
    }
    let parts = path[1..].split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        ["deliveries"] => true,
        ["deliveries", ordinal] => valid_ordinal(ordinal),
        ["deliveries", ordinal, field] => {
            valid_ordinal(ordinal)
                && matches!(
                    *field,
                    "kind"
                        | "purpose"
                        | "text"
                        | "interaction_id"
                        | "artifact_ref"
                        | "artifact_tool_call_id"
                        | "creation_command_id"
                        | "partner_tool_call_id"
                )
        }
        ["deliveries", ordinal, "artifact_ref", field] => {
            valid_ordinal(ordinal) && matches!(*field, "kind" | "id")
        }
        _ => false,
    }
}
fn valid_ordinal(value: &str) -> bool {
    value
        .parse::<usize>()
        .is_ok_and(|n| n < crate::MAX_PUBLIC_DELIVERIES && n.to_string() == value)
}

/// Canonical ordering is part of the durable diagnostic fingerprint. Never
/// include parser prose, unknown property names or rejected values.
pub fn public_delivery_diagnostics_hash(
    issues: &[PublicDeliveryDiagnostic],
) -> Result<Sha256, PublicDeliveryRepairError> {
    if issues.is_empty()
        || issues.len() > MAX_DELIVERY_DIAGNOSTICS
        || issues.windows(2).any(|pair| pair[0] >= pair[1])
        || issues.iter().any(|issue| !valid_path(issue.path()))
    {
        return Err(PublicDeliveryRepairError);
    }
    let mut bytes = b"musteragent.delivery-diagnostic.v1\0".to_vec();
    for issue in issues {
        for value in [issue.kind().as_str(), issue.path()] {
            bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
    }
    Ok(Sha256::digest(&bytes))
}
