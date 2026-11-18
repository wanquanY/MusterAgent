//! Versioned internal Confirmation presentation. This is constructed by the Task
//! owner after resolving immutable evidence, never by a model-authored question.
use crate::{StructuredInteractionError, StructuredInteractionRequest};
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_identity::ContentId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

const SCHEMA: &str = "work-item.acceptance-review@1";
const MAX_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskReviewContentRef {
    pub content_id: Uuid,
    pub media_type: String,
    pub byte_length: String,
    pub sha256: String,
}
impl From<&ContentRef> for TaskReviewContentRef {
    fn from(value: &ContentRef) -> Self {
        Self {
            content_id: value.content_id.into_uuid(),
            media_type: value.media_type.as_str().into(),
            byte_length: value.byte_length.to_string(),
            sha256: value.sha256.to_string(),
        }
    }
}
impl TaskReviewContentRef {
    pub fn content_ref(&self) -> Result<ContentRef, StructuredInteractionError> {
        Ok(ContentRef {
            content_id: ContentId::from_uuid(self.content_id).map_err(|_| invalid())?,
            media_type: MediaType::new(self.media_type.clone()).map_err(|_| invalid())?,
            byte_length: decimal(&self.byte_length, false)?,
            sha256: Sha256::from_hex(&self.sha256).map_err(|_| invalid())?,
        })
    }
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskReviewEvidenceKind {
    Message,
    Artifact,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskReviewEvidence {
    pub entry_id: Uuid,
    pub dialogue_id: Uuid,
    pub agent_id: Uuid,
    pub kind: TaskReviewEvidenceKind,
    pub content_ref: TaskReviewContentRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_version_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskReviewCriterion {
    pub index: u16,
    pub text: String,
    pub evidence_entry_ids: Vec<Uuid>,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAcceptanceReview {
    pub schema: String,
    pub task_id: Uuid,
    pub goal_revision: String,
    pub control_revision: String,
    pub assignment_epoch: String,
    pub producing_run_id: Uuid,
    pub report_content_ref: TaskReviewContentRef,
    pub goal_content_ref: TaskReviewContentRef,
    pub objective: String,
    pub evidence_boundary: String,
    pub criteria: Vec<TaskReviewCriterion>,
    pub evidence: Vec<TaskReviewEvidence>,
}
impl TaskAcceptanceReview {
    pub fn validate(&self) -> Result<(), StructuredInteractionError> {
        if self.schema != SCHEMA
            || self.task_id.is_nil()
            || self.producing_run_id.is_nil()
            || !text(&self.objective, 8_000)
            || !text(&self.evidence_boundary, 4_000)
            || self.criteria.is_empty()
            || self.criteria.len() > 100
            || self.evidence.len() > 3200
        {
            return Err(invalid());
        }
        for value in [
            &self.goal_revision,
            &self.control_revision,
            &self.assignment_epoch,
        ] {
            decimal(value, true)?;
        }
        self.report_content_ref.content_ref()?;
        self.goal_content_ref.content_ref()?;
        let mut references = BTreeSet::new();
        for (index, criterion) in self.criteria.iter().enumerate() {
            if usize::from(criterion.index) != index
                || !text(&criterion.text, 2_000)
                || criterion.evidence_entry_ids.is_empty()
                || criterion.evidence_entry_ids.len() > 32
                || criterion.evidence_entry_ids.iter().any(Uuid::is_nil)
                || criterion
                    .evidence_entry_ids
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != criterion.evidence_entry_ids.len()
            {
                return Err(invalid());
            }
            references.extend(criterion.evidence_entry_ids.iter().copied());
        }
        let mut actual = BTreeSet::new();
        for evidence in &self.evidence {
            if evidence.entry_id.is_nil()
                || evidence.dialogue_id.is_nil()
                || evidence.agent_id.is_nil()
                || !actual.insert(evidence.entry_id)
                || evidence
                    .display_name
                    .as_ref()
                    .is_some_and(|v| !text(v, 500))
            {
                return Err(invalid());
            }
            evidence.content_ref.content_ref()?;
            match (
                evidence.kind,
                evidence.artifact_id,
                evidence.artifact_version_id,
                evidence.artifact_version.as_deref(),
            ) {
                (TaskReviewEvidenceKind::Message, None, None, None) => {}
                (TaskReviewEvidenceKind::Artifact, Some(artifact), Some(version), Some(number))
                    if !artifact.is_nil() && !version.is_nil() =>
                {
                    decimal(number, true)?;
                }
                _ => return Err(invalid()),
            }
        }
        if actual != references {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAcceptancePrompt {
    kind: String,
    title: String,
    description: String,
    required: bool,
    allow_other: bool,
    pub task_review: TaskAcceptanceReview,
}
impl TaskAcceptancePrompt {
    pub fn new(task_review: TaskAcceptanceReview) -> Result<Self, StructuredInteractionError> {
        task_review.validate()?;
        Ok(Self {
            kind: "confirm".into(),
            title: "请确认任务结果是否符合要求".into(),
            description:
                "请查看本次任务的完整目标、验收标准和对应交付内容。确认只适用于这里显示的结果。"
                    .into(),
            required: true,
            allow_other: false,
            task_review,
        })
    }
    pub fn parse(bytes: &[u8]) -> Result<Self, StructuredInteractionError> {
        if bytes.len() > MAX_BYTES {
            return Err(invalid());
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        if value.kind != "confirm"
            || !value.required
            || value.allow_other
            || !text(&value.title, 200)
            || !text(&value.description, 2000)
        {
            return Err(invalid());
        }
        value.task_review.validate()?;
        Ok(value)
    }
    pub fn canonical_prompt_bytes(&self) -> Result<Vec<u8>, StructuredInteractionError> {
        self.task_review.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid())?;
        if bytes.len() > MAX_BYTES {
            return Err(invalid());
        }
        Ok(bytes)
    }
    pub const fn interaction_kind(&self) -> musteragent_approval::InteractionKind {
        musteragent_approval::InteractionKind::Confirmation
    }
    pub fn response_schema(&self) -> Result<SchemaRef, StructuredInteractionError> {
        // Reuse the original positive/negative Confirmation response language.
        StructuredInteractionRequest::parse(
            br#"{"kind":"confirm","title":"Review","required":true,"allow_other":false}"#,
        )?
        .response_schema()
    }
}
fn decimal(value: &str, positive: bool) -> Result<u64, StructuredInteractionError> {
    let n: u64 = value.parse().map_err(|_| invalid())?;
    if n.to_string() != value || (positive && n == 0) {
        return Err(invalid());
    }
    Ok(n)
}
fn text(value: &str, limit: usize) -> bool {
    !value.is_empty() && value.chars().count() <= limit
}
fn invalid() -> StructuredInteractionError {
    StructuredInteractionError::InvalidRequest
}

#[cfg(test)]
mod tests {
    use super::*;
    fn review() -> TaskAcceptanceReview {
        let content = TaskReviewContentRef {
            content_id: Uuid::now_v7(),
            media_type: "text/plain".into(),
            byte_length: "3".into(),
            sha256: Sha256::digest(b"yes").to_string(),
        };
        let entry = Uuid::now_v7();
        TaskAcceptanceReview {
            schema: SCHEMA.into(),
            task_id: Uuid::now_v7(),
            goal_revision: "1".into(),
            control_revision: "1".into(),
            assignment_epoch: "1".into(),
            producing_run_id: Uuid::now_v7(),
            report_content_ref: content.clone(),
            goal_content_ref: content.clone(),
            objective: "完整目标".repeat(1500),
            evidence_boundary: "本次授权的用户资料".into(),
            criteria: vec![TaskReviewCriterion {
                index: 0,
                text: "所有结论有对应证据".into(),
                evidence_entry_ids: vec![entry],
            }],
            evidence: vec![TaskReviewEvidence {
                entry_id: entry,
                dialogue_id: Uuid::now_v7(),
                agent_id: Uuid::now_v7(),
                kind: TaskReviewEvidenceKind::Message,
                content_ref: content,
                artifact_id: None,
                artifact_version_id: None,
                artifact_version: None,
                display_name: None,
            }],
        }
    }
    #[test]
    fn long_review_is_lossless_and_does_not_expand_model_question_contract() {
        let prompt = TaskAcceptancePrompt::new(review()).unwrap();
        let bytes = prompt.canonical_prompt_bytes().unwrap();
        assert_eq!(TaskAcceptancePrompt::parse(&bytes).unwrap(), prompt);
        assert!(
            StructuredInteractionRequest::parse_tool_arguments("interaction.request@1", &bytes)
                .is_err()
        );
        assert_eq!(
            prompt.response_schema().unwrap().schema_key.as_str(),
            "interaction.confirm.response@1"
        );
    }
    #[test]
    fn review_rejects_uncovered_duplicate_and_noncanonical_evidence() {
        let original = review();
        for case in 0..5 {
            let mut value = original.clone();
            match case {
                0 => value.evidence.clear(),
                1 => value.evidence.push(value.evidence[0].clone()),
                2 => value.goal_revision = "01".into(),
                3 => value.evidence[0].artifact_id = Some(Uuid::now_v7()),
                4 => value.evidence[0].content_ref.sha256 = "bad".into(),
                _ => unreachable!(),
            }
            assert!(TaskAcceptancePrompt::new(value).is_err(), "case {case}");
        }
    }
}
