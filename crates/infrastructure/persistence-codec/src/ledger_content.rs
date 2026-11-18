//! Reference discovery follows the canonical IDL, never strings or result JSON.
//! This is a retention projection, not validation of an event or authorization.
use crate::bounded_protobuf::{BudgetError, Profile};
use crate::content_references::ContentReferences;
use musteragent_content_ref::ContentRef;
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::ContentId;
use musteragent_ledger::MAX_RECORD_PAYLOAD_BYTES;
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum LedgerContentReferencesError {
    #[error("Ledger reference coverage exceeds the supported bounds")]
    Bounds,
    #[error("Ledger reference coverage is incomplete or invalid")]
    Incomplete,
    #[error("Ledger reference schema is unavailable")]
    Schema,
}

/// Returns every explicit typed ContentRef, sorted and deduplicated by identity.
/// Unknown fields, ambiguous encodings or invalid references return an error,
/// never an empty successful set. Embedded bytes/JSON are opaque: their semantic
/// dependencies must be declared by their own owner, e.g. Tool authorized_content
/// or Artifact dependency_refs. This index grants no content access.
pub fn ledger_content_references(
    kind: LedgerRecordKind,
    payload: &[u8],
) -> Result<Vec<ContentRef>, LedgerContentReferencesError> {
    let name = match kind {
        LedgerRecordKind::DomainEvent => "musteragent.v1.DomainEvent",
        LedgerRecordKind::OutputFrame => "musteragent.v1.OutputFrame",
    };
    let mut references = ContentReferences::default();
    references
        .visit(
            payload,
            NonZeroUsize::new(MAX_RECORD_PAYLOAD_BYTES).expect("positive Ledger bound"),
            name,
            Profile::LedgerReferences,
        )
        .map_err(|error| match error {
            BudgetError::Bounds => LedgerContentReferencesError::Bounds,
            BudgetError::Invalid => LedgerContentReferencesError::Incomplete,
            BudgetError::Schema => LedgerContentReferencesError::Schema,
        })?;
    Ok(references.finish())
}

/// True for every name under which the current record contract was written.
#[must_use]
pub fn ledger_record_schema_is_current(schema: &str) -> bool {
    schema == musteragent_event_envelope::RECORD_SCHEMA_VERSION
        || schema == musteragent_event_envelope::LEGACY_RECORD_SCHEMA_VERSION
}

/// Every identity that canonical UUID text anywhere in `payload` could denote,
/// sorted and unique. `ContentId` has only ever been encoded as UUID text, so
/// this is a superset of the typed roots, including any inside opaque bytes.
/// It is the retention fallback when the typed projection is unavailable:
/// callers keep only candidates that resolve to their own Content metadata,
/// because most UUIDs in a record are other identities.
#[must_use]
pub fn ledger_content_reference_candidates(payload: &[u8]) -> Vec<ContentId> {
    const LENGTH: usize = 36;
    let mut found = std::collections::BTreeSet::new();
    let mut start = 0;
    while start + LENGTH <= payload.len() {
        let window = &payload[start..start + LENGTH];
        let canonical = window.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        });
        if canonical
            && let Some(id) = std::str::from_utf8(window)
                .ok()
                .and_then(|text| text.parse::<ContentId>().ok())
        {
            found.insert(id);
        }
        start += 1;
    }
    found.into_iter().collect()
}

#[cfg(test)]
mod tests;
