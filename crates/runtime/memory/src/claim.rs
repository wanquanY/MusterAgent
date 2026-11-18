use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use musteragent_content_ref::Sha256;
use musteragent_evolution::{
    BranchEvidenceState, EvidenceSpan, ExperienceKind, LearningOutcome, RunTerminalEvidence,
};
use musteragent_identity::{ContextBranchId, ConversationId, LedgerOffset, LedgerRecordId, RunId};

pub const EXPERIENCE_CLAIM_SCHEMA: &str = "musteragent.experience-claim.v2";
pub const EXPERIENCE_CLAIM_MEDIA_TYPE: &str =
    "application/vnd.musteragent.experience-claim.v2+json";
const LEGACY_EXPERIENCE_CLAIM_SCHEMA: &str = "musteragent.legacy.experience-claim.v1";
const LEGACY_EXPERIENCE_CLAIM_MEDIA_TYPE: &str =
    "application/vnd.musteragent.legacy.experience-claim+json";

#[must_use]
pub fn is_experience_claim_media_type(value: &str) -> bool {
    matches!(
        value,
        EXPERIENCE_CLAIM_MEDIA_TYPE | LEGACY_EXPERIENCE_CLAIM_MEDIA_TYPE
    )
}

/// Producer identity is data, not an implicit JSON shape. Every candidate
/// therefore crosses Evaluation, Approval, Publication and Retrieval through
/// the same versioned claim contract.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperienceClaimOrigin {
    Manual,
    DreamEvidence,
}

/// Product-facing materialization state for one immutable Experience Claim.
///
/// The state is explicit so a missing blob and an invalid canonical document
/// can never be presented as an empty (or archived) experience.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ExperienceClaimInspectionState {
    Ready,
    ContentUnavailable,
    ContractInvalid,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExperienceClaimConditionInspection {
    pub key: String,
    pub value: String,
}

/// Product-safe provenance for one exact source Run. Record identities and
/// hashes remain inside Runtime; the bounded cursor range and count retain a
/// real, auditable source boundary without leaking raw Ledger internals.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExperienceEvidenceInspection {
    pub conversation_id: ConversationId,
    pub run_id: RunId,
    pub branch_id: Option<ContextBranchId>,
    pub branch_state: BranchEvidenceState,
    pub cursor_from: u64,
    pub cursor_to: u64,
    pub record_count: u32,
    pub terminal: RunTerminalEvidence,
    pub outcome: LearningOutcome,
}

impl ExperienceEvidenceInspection {
    pub fn validate(&self) -> Result<(), ExperienceClaimError> {
        let terminal_matches = matches!(
            (self.terminal, self.outcome),
            (RunTerminalEvidence::Completed, LearningOutcome::Success)
                | (RunTerminalEvidence::Completed, LearningOutcome::Unknown)
                | (RunTerminalEvidence::Failed, LearningOutcome::Failure)
                | (RunTerminalEvidence::Cancelled, LearningOutcome::Cancelled)
                | (
                    RunTerminalEvidence::Interrupted,
                    LearningOutcome::Interrupted
                )
        );
        if self.cursor_from >= self.cursor_to
            || self.record_count == 0
            || !terminal_matches
            || (self.outcome == LearningOutcome::Success
                && self.branch_state != BranchEvidenceState::Active)
        {
            return Err(ExperienceClaimError::InvalidDocument);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExperienceClaimInspection {
    pub state: ExperienceClaimInspectionState,
    pub origin: Option<ExperienceClaimOrigin>,
    pub claim: String,
    pub outcome: String,
    pub conditions: Vec<ExperienceClaimConditionInspection>,
    pub evidence: Vec<ExperienceEvidenceInspection>,
}

impl ExperienceClaimInspection {
    #[must_use]
    pub const fn content_unavailable() -> Self {
        Self {
            state: ExperienceClaimInspectionState::ContentUnavailable,
            origin: None,
            claim: String::new(),
            outcome: String::new(),
            conditions: Vec::new(),
            evidence: Vec::new(),
        }
    }

    #[must_use]
    pub const fn contract_invalid() -> Self {
        Self {
            state: ExperienceClaimInspectionState::ContractInvalid,
            origin: None,
            claim: String::new(),
            outcome: String::new(),
            conditions: Vec::new(),
            evidence: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), ExperienceClaimError> {
        match self.state {
            ExperienceClaimInspectionState::Ready => {
                if self.origin.is_none()
                    || self.claim.trim().is_empty()
                    || self.outcome.trim().is_empty()
                    || self.conditions.len() > 50
                    || self.evidence.len() > 128
                    || self.conditions.iter().any(|condition| {
                        condition.key.trim().is_empty() || condition.value.trim().is_empty()
                    })
                    || self.evidence.iter().any(|item| item.validate().is_err())
                {
                    return Err(ExperienceClaimError::InvalidDocument);
                }
                match self.origin {
                    Some(ExperienceClaimOrigin::Manual) if self.evidence.is_empty() => {}
                    Some(ExperienceClaimOrigin::DreamEvidence) if !self.evidence.is_empty() => {}
                    _ => return Err(ExperienceClaimError::InvalidDocument),
                }
            }
            ExperienceClaimInspectionState::ContentUnavailable
            | ExperienceClaimInspectionState::ContractInvalid => {
                if self.origin.is_some()
                    || !self.claim.is_empty()
                    || !self.outcome.is_empty()
                    || !self.conditions.is_empty()
                    || !self.evidence.is_empty()
                {
                    return Err(ExperienceClaimError::InvalidDocument);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct MaterializedExperienceClaim {
    pub inspection: ExperienceClaimInspection,
    pub source_run_ids: BTreeSet<RunId>,
    pub evidence_record_ids: BTreeSet<LedgerRecordId>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedEvidenceSpan {
    conversation_id: String,
    run_id: String,
    branch_id: Option<String>,
    branch_state: String,
    cursor_from: u64,
    cursor_to: u64,
    record_ids: Vec<String>,
    record_hashes: Vec<String>,
    terminal: String,
    outcome: String,
    #[serde(default)]
    signal_id: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperienceClaimDocument {
    pub schema: String,
    pub origin: ExperienceClaimOrigin,
    pub claim: String,
    #[serde(default)]
    pub conditions: Map<String, Value>,
    pub outcome: String,
    pub sensitivity: String,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub metadata: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<Value>,
}

impl ExperienceClaimDocument {
    #[must_use]
    pub fn manual(
        claim: String,
        conditions: Map<String, Value>,
        outcome: String,
        sensitivity: String,
    ) -> Self {
        Self {
            schema: EXPERIENCE_CLAIM_SCHEMA.to_owned(),
            origin: ExperienceClaimOrigin::Manual,
            claim,
            conditions,
            outcome,
            sensitivity,
            metadata: Map::new(),
            evidence: Vec::new(),
        }
    }

    #[must_use]
    pub fn dream_evidence(
        claim: String,
        conditions: Map<String, Value>,
        outcome: String,
        metadata: Map<String, Value>,
        evidence: Vec<Value>,
    ) -> Self {
        Self {
            schema: EXPERIENCE_CLAIM_SCHEMA.to_owned(),
            origin: ExperienceClaimOrigin::DreamEvidence,
            claim,
            conditions,
            outcome,
            sensitivity: "internal".to_owned(),
            metadata,
            evidence,
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ExperienceClaimError> {
        let document: Self =
            serde_json::from_slice(bytes).map_err(|_| ExperienceClaimError::InvalidEncoding)?;
        document.validate()?;
        Ok(document)
    }

    pub fn encode(&self) -> Result<Vec<u8>, ExperienceClaimError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|_| ExperienceClaimError::InvalidEncoding)
    }

    pub fn validate(&self) -> Result<(), ExperienceClaimError> {
        if !matches!(
            self.schema.as_str(),
            EXPERIENCE_CLAIM_SCHEMA | LEGACY_EXPERIENCE_CLAIM_SCHEMA
        ) || self.claim.trim().is_empty()
            || self.claim.len() > 8_000
            || self.conditions.len() > 50
            || self.outcome.trim().is_empty()
            || self.outcome.len() > 128
            || !matches!(self.sensitivity.as_str(), "private" | "internal")
            || self.metadata.len() > 32
            || self.evidence.len() > 128
        {
            return Err(ExperienceClaimError::InvalidDocument);
        }
        match self.origin {
            ExperienceClaimOrigin::Manual => {
                if !self.metadata.is_empty() || !self.evidence.is_empty() {
                    return Err(ExperienceClaimError::InvalidDocument);
                }
            }
            ExperienceClaimOrigin::DreamEvidence => {
                if self.evidence.is_empty()
                    || [
                        "signal_id",
                        "signal_kind",
                        "target_agent_id",
                        "extractor_version_ref",
                        "policy_version_ref",
                    ]
                    .iter()
                    .any(|key| {
                        self.metadata
                            .get(*key)
                            .and_then(Value::as_str)
                            .is_none_or(str::is_empty)
                    })
                {
                    return Err(ExperienceClaimError::InvalidDocument);
                }
            }
        }
        Ok(())
    }

    /// Validates the one publication contract shared by Candidate generation,
    /// Evaluation, Approval inspection and the final publication boundary.
    ///
    /// The canonical document contract is deliberately kind-agnostic. This
    /// second layer owns semantic constraints that depend on the governed
    /// artifact kind so no producer can create a Candidate that Evaluation
    /// accepts but Publication must reject.
    pub fn validate_for_kind(&self, kind: ExperienceKind) -> Result<(), ExperienceClaimError> {
        self.validate()?;
        validate_experience_claim_kind(kind, self.conditions.len())
    }

    #[must_use]
    pub fn searchable_text(&self) -> String {
        format!(
            "{} {} {}",
            self.claim,
            self.outcome,
            Value::Object(self.conditions.clone()),
        )
    }

    pub(crate) fn materialize_inspection(
        &self,
    ) -> Result<MaterializedExperienceClaim, ExperienceClaimError> {
        self.validate()?;
        let conditions = self
            .conditions
            .iter()
            .map(|(key, value)| {
                Ok(ExperienceClaimConditionInspection {
                    key: key.clone(),
                    value: condition_value(value)?,
                })
            })
            .collect::<Result<Vec<_>, ExperienceClaimError>>()?;
        let mut evidence = Vec::with_capacity(self.evidence.len());
        let mut source_run_ids = BTreeSet::new();
        let mut evidence_record_ids = BTreeSet::new();
        for value in &self.evidence {
            let persisted: PersistedEvidenceSpan = serde_json::from_value(value.clone())
                .map_err(|_| ExperienceClaimError::InvalidDocument)?;
            let span = persisted.into_span()?;
            span.validate()
                .map_err(|_| ExperienceClaimError::InvalidDocument)?;
            let record_count = u32::try_from(span.record_ids.len())
                .map_err(|_| ExperienceClaimError::InvalidDocument)?;
            source_run_ids.insert(span.run_id);
            evidence_record_ids.extend(span.record_ids.iter().copied());
            evidence.push(ExperienceEvidenceInspection {
                conversation_id: span.conversation_id,
                run_id: span.run_id,
                branch_id: span.branch_id,
                branch_state: span.branch_state,
                cursor_from: span.cursor_from.get(),
                cursor_to: span.cursor_to.get(),
                record_count,
                terminal: span.terminal,
                outcome: span.outcome,
            });
        }
        let inspection = ExperienceClaimInspection {
            state: ExperienceClaimInspectionState::Ready,
            origin: Some(self.origin),
            claim: self.claim.trim().to_owned(),
            outcome: self.outcome.trim().to_owned(),
            conditions,
            evidence,
        };
        inspection.validate()?;
        Ok(MaterializedExperienceClaim {
            inspection,
            source_run_ids,
            evidence_record_ids,
        })
    }
}

impl PersistedEvidenceSpan {
    fn into_span(self) -> Result<EvidenceSpan, ExperienceClaimError> {
        let _signal_id = self.signal_id;
        Ok(EvidenceSpan {
            conversation_id: parse_id(&self.conversation_id)?,
            run_id: parse_id(&self.run_id)?,
            branch_id: self.branch_id.as_deref().map(parse_id).transpose()?,
            branch_state: match self.branch_state.as_str() {
                "Active" => BranchEvidenceState::Active,
                "Preserved" => BranchEvidenceState::Preserved,
                "Abandoned" => BranchEvidenceState::Abandoned,
                _ => return Err(ExperienceClaimError::InvalidDocument),
            },
            cursor_from: LedgerOffset::new(self.cursor_from),
            cursor_to: LedgerOffset::new(self.cursor_to),
            record_ids: self
                .record_ids
                .iter()
                .map(String::as_str)
                .map(parse_id)
                .collect::<Result<Vec<_>, _>>()?,
            record_hashes: self
                .record_hashes
                .iter()
                .map(|value| parse_sha256(value))
                .collect::<Result<Vec<_>, _>>()?,
            terminal: match self.terminal.as_str() {
                "Completed" => RunTerminalEvidence::Completed,
                "Failed" => RunTerminalEvidence::Failed,
                "Cancelled" => RunTerminalEvidence::Cancelled,
                "Interrupted" => RunTerminalEvidence::Interrupted,
                _ => return Err(ExperienceClaimError::InvalidDocument),
            },
            outcome: match self.outcome.as_str() {
                "Success" => LearningOutcome::Success,
                "Failure" => LearningOutcome::Failure,
                "Cancelled" => LearningOutcome::Cancelled,
                "Interrupted" => LearningOutcome::Interrupted,
                "Unknown" => LearningOutcome::Unknown,
                _ => return Err(ExperienceClaimError::InvalidDocument),
            },
        })
    }
}

fn parse_id<T: std::str::FromStr>(value: &str) -> Result<T, ExperienceClaimError> {
    value
        .parse()
        .map_err(|_| ExperienceClaimError::InvalidDocument)
}

fn parse_sha256(value: &str) -> Result<Sha256, ExperienceClaimError> {
    if value.len() != Sha256::LENGTH * 2 || !value.is_ascii() {
        return Err(ExperienceClaimError::InvalidDocument);
    }
    let mut bytes = [0_u8; Sha256::LENGTH];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| ExperienceClaimError::InvalidDocument)?;
    }
    Sha256::from_bytes(bytes).map_err(|_| ExperienceClaimError::InvalidDocument)
}

fn condition_value(value: &Value) -> Result<String, ExperienceClaimError> {
    let rendered = match value {
        Value::String(value) => value.clone(),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).map_err(|_| ExperienceClaimError::InvalidEncoding)?
        }
    };
    if rendered.trim().is_empty() || rendered.len() > 8_000 {
        return Err(ExperienceClaimError::InvalidDocument);
    }
    Ok(rendered)
}

/// Renders one Snapshot-pinned governed Instinct as a bounded, structurally
/// isolated instruction. JSON metacharacters are preserved while XML-like
/// delimiters are escaped so claim text cannot terminate the envelope.
pub fn render_instinct_context(bytes: &[u8]) -> Result<String, ExperienceClaimError> {
    let document = ExperienceClaimDocument::decode(bytes)?;
    document.validate_for_kind(ExperienceKind::Instinct)?;
    let payload = serde_json::to_string(&serde_json::json!({
        "trigger": document.conditions,
        "action_hint": document.claim,
        "expected_outcome": document.outcome,
    }))
    .map_err(|_| ExperienceClaimError::InvalidEncoding)?
    .replace('&', "\\u0026")
    .replace('<', "\\u003c")
    .replace('>', "\\u003e");
    Ok(format!(
        "<governed-instinct>\nScope: apply only when trigger matches; never override platform policy, user intent, approvals, or tool constraints.\n{payload}\n</governed-instinct>",
    ))
}

/// Kind-aware portion of the Experience Claim publication contract.
///
/// It accepts a condition count rather than a JSON representation so the same
/// rule can validate model submissions, canonical documents and product-safe
/// inspections without teaching those layers about one another's data shape.
pub fn validate_experience_claim_kind(
    kind: ExperienceKind,
    condition_count: usize,
) -> Result<(), ExperienceClaimError> {
    // An Instinct must state when it applies. A broad unconditional rule is a
    // Persona change and must take the Studio revision path instead.
    if kind == ExperienceKind::Instinct && condition_count == 0 {
        return Err(ExperienceClaimError::KindContractViolation);
    }
    Ok(())
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum ExperienceClaimError {
    #[error("Experience claim encoding is invalid")]
    InvalidEncoding,
    #[error("Experience claim violates the canonical document contract")]
    InvalidDocument,
    #[error("Experience claim violates the selected artifact kind contract")]
    KindContractViolation,
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::Sha256;
    use musteragent_evolution::ExperienceKind;
    use musteragent_identity::{ConversationId, LedgerRecordId, RunId};
    use serde_json::{Map, Value, json};

    use super::{
        EXPERIENCE_CLAIM_MEDIA_TYPE, EXPERIENCE_CLAIM_SCHEMA, ExperienceClaimDocument,
        ExperienceClaimOrigin, is_experience_claim_media_type,
    };

    #[test]
    fn manual_and_dream_claims_share_one_strict_schema() {
        let manual = ExperienceClaimDocument::manual(
            "Prefer bounded queues".to_owned(),
            Map::new(),
            "lower overload risk".to_owned(),
            "private".to_owned(),
        );
        assert_eq!(
            ExperienceClaimDocument::decode(&manual.encode().expect("manual claim"))
                .expect("decode manual claim"),
            manual
        );

        let mut metadata = Map::new();
        for (key, value) in [
            ("signal_id", "signal"),
            ("signal_kind", "tool_failure"),
            ("target_agent_id", "agent"),
            ("extractor_version_ref", "extractor@1"),
            ("policy_version_ref", "policy@1"),
        ] {
            metadata.insert(key.to_owned(), json!(value));
        }
        let dream = ExperienceClaimDocument::dream_evidence(
            "Avoid repeating a failed tool strategy".to_owned(),
            Map::new(),
            "reuse only after evaluation".to_owned(),
            metadata,
            vec![json!({"run_id":"run","record_ids":["record"]})],
        );
        assert_eq!(dream.origin, ExperienceClaimOrigin::DreamEvidence);
        assert!(dream.encode().is_ok());
    }

    #[test]
    fn writers_publish_musteragent_v2_and_readers_accept_frozen_legacy_v1() {
        let current = ExperienceClaimDocument::manual(
            "Prefer bounded queues".to_owned(),
            Map::new(),
            "lower overload risk".to_owned(),
            "private".to_owned(),
        );
        assert_eq!(current.schema, EXPERIENCE_CLAIM_SCHEMA);
        assert_eq!(EXPERIENCE_CLAIM_SCHEMA, "musteragent.experience-claim.v2");
        assert_eq!(
            EXPERIENCE_CLAIM_MEDIA_TYPE,
            "application/vnd.musteragent.experience-claim.v2+json"
        );
        assert!(is_experience_claim_media_type(EXPERIENCE_CLAIM_MEDIA_TYPE));
        assert!(is_experience_claim_media_type(
            "application/vnd.musteragent.legacy.experience-claim+json"
        ));

        let mut legacy: Value = serde_json::to_value(current).expect("claim value");
        legacy["schema"] = json!("musteragent.legacy.experience-claim.v1");
        assert!(
            ExperienceClaimDocument::decode(&serde_json::to_vec(&legacy).expect("legacy claim"))
                .is_ok()
        );
    }

    #[test]
    fn origin_specific_fields_fail_closed() {
        let mut manual = ExperienceClaimDocument::manual(
            "fact".to_owned(),
            Map::new(),
            "reuse".to_owned(),
            "internal".to_owned(),
        );
        manual.evidence.push(json!({"run_id":"unexpected"}));
        assert!(manual.validate().is_err());
    }

    #[test]
    fn kind_contract_rejects_unbounded_instinct_but_allows_persona_revision() {
        let claim = ExperienceClaimDocument::manual(
            "Keep replies concise".to_owned(),
            Map::new(),
            "match the user's communication style".to_owned(),
            "internal".to_owned(),
        );

        assert_eq!(
            claim.validate_for_kind(ExperienceKind::Instinct),
            Err(super::ExperienceClaimError::KindContractViolation),
        );
        assert_eq!(
            claim.validate_for_kind(ExperienceKind::PersonaPatch),
            Ok(())
        );
    }

    #[test]
    fn search_projection_excludes_provenance_metadata_and_evidence() {
        let mut metadata = Map::new();
        for (key, value) in [
            ("signal_id", "private-signal-identifier"),
            ("signal_kind", "tool_failure"),
            ("target_agent_id", "private-agent-identifier"),
            ("extractor_version_ref", "extractor@1"),
            ("policy_version_ref", "policy@1"),
        ] {
            metadata.insert(key.to_owned(), json!(value));
        }
        let document = ExperienceClaimDocument::dream_evidence(
            "Prefer bounded queues".to_owned(),
            Map::from_iter([("task".to_owned(), json!("database migration"))]),
            "lower overload risk".to_owned(),
            metadata,
            vec![json!({
                "run_id":"private-run-identifier",
                "record_ids":["private-record-identifier"],
                "record_hashes":["private-record-hash"],
            })],
        );

        let searchable = document.searchable_text();
        assert!(searchable.contains("bounded queues"));
        assert!(searchable.contains("database migration"));
        assert!(!searchable.contains("private-signal-identifier"));
        assert!(!searchable.contains("private-record-identifier"));
        assert!(!searchable.contains("private-record-hash"));
    }

    #[test]
    fn runtime_materializes_current_and_legacy_claims_with_run_level_evidence() {
        let conversation_id = ConversationId::generate();
        let run_ids = [RunId::generate(), RunId::generate()];
        let record_ids = [LedgerRecordId::generate(), LedgerRecordId::generate()];
        let mut metadata = Map::new();
        for (key, value) in [
            ("signal_id", "signal"),
            ("signal_kind", "tool_failure"),
            ("target_agent_id", "agent"),
            ("extractor_version_ref", "extractor@1"),
            ("policy_version_ref", "policy@1"),
        ] {
            metadata.insert(key.to_owned(), json!(value));
        }
        let evidence = run_ids
            .iter()
            .zip(record_ids)
            .enumerate()
            .map(|(index, (run_id, record_id))| {
                json!({
                    "conversation_id": conversation_id.to_string(),
                    "run_id": run_id.to_string(),
                    "branch_id": null,
                    "branch_state": "Active",
                    "cursor_from": index * 10 + 1,
                    "cursor_to": index * 10 + 3,
                    "record_ids": [record_id.to_string()],
                    "record_hashes": [Sha256::digest(record_id.to_string()).to_string()],
                    "terminal": "Failed",
                    "outcome": "Failure",
                })
            })
            .collect();
        let current = ExperienceClaimDocument::dream_evidence(
            "Verify delegation capability first".to_owned(),
            Map::from_iter([("signal_kind".to_owned(), json!("tool_failure"))]),
            "Avoid invalid child Runs".to_owned(),
            metadata,
            evidence,
        );

        for schema in [
            EXPERIENCE_CLAIM_SCHEMA,
            "musteragent.legacy.experience-claim.v1",
        ] {
            let mut value = serde_json::to_value(&current).expect("claim JSON");
            value["schema"] = json!(schema);
            let decoded = ExperienceClaimDocument::decode(
                &serde_json::to_vec(&value).expect("encoded claim JSON"),
            )
            .expect("canonical claim");
            let materialized = decoded.materialize_inspection().expect("product detail");
            assert_eq!(materialized.inspection.claim, current.claim);
            assert_eq!(materialized.inspection.evidence.len(), 2);
            assert_eq!(materialized.source_run_ids.len(), 2);
            assert_eq!(materialized.evidence_record_ids.len(), 2);
            assert!(
                materialized
                    .inspection
                    .evidence
                    .iter()
                    .all(|source| source.conversation_id == conversation_id)
            );
        }
    }

    #[test]
    fn malformed_evidence_cannot_be_materialized_as_empty_product_copy() {
        let mut metadata = Map::new();
        for (key, value) in [
            ("signal_id", "signal"),
            ("signal_kind", "tool_failure"),
            ("target_agent_id", "agent"),
            ("extractor_version_ref", "extractor@1"),
            ("policy_version_ref", "policy@1"),
        ] {
            metadata.insert(key.to_owned(), json!(value));
        }
        let claim = ExperienceClaimDocument::dream_evidence(
            "Verify delegation capability first".to_owned(),
            Map::new(),
            "Avoid invalid child Runs".to_owned(),
            metadata,
            vec![json!({"conversation_id": "missing-run-and-ledger-range"})],
        );

        assert_eq!(
            claim.materialize_inspection(),
            Err(super::ExperienceClaimError::InvalidDocument),
        );
    }
}
