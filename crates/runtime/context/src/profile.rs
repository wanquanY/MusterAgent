//! Trusted activity policy supplied before the original ContextPlan is sealed.
use musteragent_activity::ActivityKind;
use musteragent_content_ref::SchemaRef;
use musteragent_runtime_model::{CanonicalToolDefinition, JsonValue, ModelText};

/// Runtime-owned additions to one activity's canonical request. The planner
/// binds these to the admitted activity, includes them in capacity planning,
/// and seals them with the ordinary ContextPlan. They cannot change a committed
/// request. Absence of a profile preserves the ordinary request schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ContextPlanProfile {
    pub activity_kind: ActivityKind,
    pub instructions: Vec<ModelText>,
    pub tools: Vec<CanonicalToolDefinition>,
    pub response_schema: Option<(SchemaRef, JsonValue)>,
}
