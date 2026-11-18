use std::collections::BTreeSet;
use std::num::NonZeroU64;

use musteragent_content_ref::ContentRef;
use musteragent_identity::{InvocationId, RunId, UtcTimestamp};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const INHERITED_EXECUTION_PLAN_SCHEMA: &str = "musteragent.runtime.execution-plan@3";
pub const EXECUTION_PLAN_SCHEMA: &str = "musteragent.runtime.execution-plan@2";
pub const LEGACY_EXECUTION_PLAN_SCHEMA: &str = "musteragent.legacy.runtime.execution-plan@1";
pub const MAX_EXECUTION_PLAN_STEPS: usize = 64;
pub const MAX_PLAN_STEP_ID_BYTES: usize = 64;
pub const MAX_PLAN_STEP_CONTENT_BYTES: usize = 512;

/// Stable logical identity for one step inside a Run-scoped plan.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlanStepId(String);

impl PlanStepId {
    pub fn new(value: impl Into<String>) -> Result<Self, ExecutionPlanError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_PLAN_STEP_ID_BYTES
            || value.chars().any(char::is_control)
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(ExecutionPlanError::InvalidStepId);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStepStatus {
    Pending,
    InProgress,
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlanStep {
    pub step_id: PlanStepId,
    pub content: String,
    pub status: PlanStepStatus,
}

impl ExecutionPlanStep {
    pub fn validate(&self) -> Result<(), ExecutionPlanError> {
        if self.content.is_empty()
            || self.content.len() > MAX_PLAN_STEP_CONTENT_BYTES
            || self.content.chars().any(char::is_control)
        {
            return Err(ExecutionPlanError::InvalidStepContent);
        }
        Ok(())
    }
}

/// Exact immutable predecessor evidence; it never grants control of the old Run.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlanLineage {
    pub source_run_id: String,
    pub source_revision: NonZeroU64,
    pub source_plan_sha256: String,
}

/// Canonical immutable JSON stored behind `ExecutionPlan.plan_ref`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlanDocument {
    pub schema_version: String,
    pub steps: Vec<ExecutionPlanStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<ExecutionPlanLineage>,
}

impl ExecutionPlanDocument {
    #[must_use]
    pub fn new(steps: Vec<ExecutionPlanStep>) -> Self {
        Self {
            schema_version: EXECUTION_PLAN_SCHEMA.to_owned(),
            steps,
            lineage: None,
        }
    }

    pub fn validate(&self) -> Result<(), ExecutionPlanError> {
        if !matches!(
            self.schema_version.as_str(),
            EXECUTION_PLAN_SCHEMA | LEGACY_EXECUTION_PLAN_SCHEMA | INHERITED_EXECUTION_PLAN_SCHEMA
        ) || self.steps.is_empty()
            || self.steps.len() > MAX_EXECUTION_PLAN_STEPS
        {
            return Err(ExecutionPlanError::InvalidDocument);
        }
        if self.lineage.as_ref().is_some_and(|lineage| {
            self.schema_version != INHERITED_EXECUTION_PLAN_SCHEMA
                || lineage.source_run_id.parse::<RunId>().is_err()
                || lineage.source_plan_sha256.len() != 64
                || !lineage
                    .source_plan_sha256
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        }) {
            return Err(ExecutionPlanError::InvalidDocument);
        }
        let mut ids = BTreeSet::new();
        let mut in_progress = 0_usize;
        for step in &self.steps {
            step.validate()?;
            if !ids.insert(step.step_id.clone()) {
                return Err(ExecutionPlanError::DuplicateStepId);
            }
            if step.status == PlanStepStatus::InProgress {
                in_progress = in_progress.saturating_add(1);
            }
        }
        if in_progress > 1 {
            return Err(ExecutionPlanError::MultipleInProgressSteps);
        }
        Ok(())
    }
}

/// Current durable projection for one Run. The step body has one owner: the
/// immutable document referenced by `plan_ref`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionPlan {
    pub run_id: RunId,
    pub revision: NonZeroU64,
    pub plan_ref: ContentRef,
    pub updated_by_invocation_id: InvocationId,
    pub updated_at: UtcTimestamp,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ExecutionPlanError {
    #[error("execution plan document is invalid")]
    InvalidDocument,
    #[error("execution plan step id is invalid")]
    InvalidStepId,
    #[error("execution plan step content is invalid")]
    InvalidStepContent,
    #[error("execution plan step ids must be unique")]
    DuplicateStepId,
    #[error("execution plan can contain at most one in-progress step")]
    MultipleInProgressSteps,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(id: &str, status: PlanStepStatus) -> ExecutionPlanStep {
        ExecutionPlanStep {
            step_id: PlanStepId::new(id).expect("step id"),
            content: format!("Do {id}"),
            status,
        }
    }

    #[test]
    fn execution_plan_is_bounded_and_has_one_active_step() {
        let valid = ExecutionPlanDocument::new(vec![
            step("inspect", PlanStepStatus::Completed),
            step("implement", PlanStepStatus::InProgress),
        ]);
        assert_eq!(valid.validate(), Ok(()));

        let duplicate = ExecutionPlanDocument::new(vec![
            step("inspect", PlanStepStatus::Pending),
            step("inspect", PlanStepStatus::Completed),
        ]);
        assert_eq!(
            duplicate.validate(),
            Err(ExecutionPlanError::DuplicateStepId)
        );

        let concurrent = ExecutionPlanDocument::new(vec![
            step("inspect", PlanStepStatus::InProgress),
            step("implement", PlanStepStatus::InProgress),
        ]);
        assert_eq!(
            concurrent.validate(),
            Err(ExecutionPlanError::MultipleInProgressSteps)
        );
    }
    #[test]
    fn inheritance_is_versioned_and_keeps_exact_source_evidence() {
        let mut document =
            ExecutionPlanDocument::new(vec![step("continue", PlanStepStatus::Pending)]);
        let legacy = serde_json::to_vec(&document).expect("legacy bytes");
        assert!(!String::from_utf8_lossy(&legacy).contains("lineage"));
        document.lineage = Some(ExecutionPlanLineage {
            source_run_id: RunId::generate().to_string(),
            source_revision: NonZeroU64::new(7).expect("revision"),
            source_plan_sha256: "a".repeat(64),
        });
        assert_eq!(
            document.validate(),
            Err(ExecutionPlanError::InvalidDocument)
        );
        document.schema_version = INHERITED_EXECUTION_PLAN_SCHEMA.to_owned();
        document.validate().expect("v3 lineage");
        let decoded: ExecutionPlanDocument =
            serde_json::from_slice(&serde_json::to_vec(&document).expect("encode"))
                .expect("decode");
        assert_eq!(decoded, document);
        document.lineage.as_mut().expect("lineage").source_run_id =
            "untrusted-not-a-run".to_owned();
        assert_eq!(
            document.validate(),
            Err(ExecutionPlanError::InvalidDocument)
        );
    }
}
