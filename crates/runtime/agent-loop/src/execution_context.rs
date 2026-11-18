//! Data operations for the existing policy, Context and control ports.
//! The planner and every physical model call remain on the Executor; Home
//! material reads and original plan commits have separate request lifetimes.
use crate::{PinnedRunExecutionPolicy, ToolExecutionPolicyDecision};
use musteragent_context::{
    ContextCompactionFailureCommit, ContextCompactionResolutionCommit,
    ContextCompactionStartCommit, ContextMaterial, ContextPlanCommit, ContextPlanDisposition,
    ContextPlanInput, ContextPlanResolution,
};
use musteragent_runtime_snapshot::ToolExecutionMode;
use musteragent_tool::WireName;

#[derive(Debug, Clone, PartialEq)]
pub enum AgentContextCommand {
    CompactionModel(crate::AgentCompactionCommand),
    ReadFiles {
        invocation_id: musteragent_identity::InvocationId,
    },
    Policy,
    ToolPolicy {
        wire_name: WireName,
        default_mode: ToolExecutionMode,
    },
    ReadMaterial(Box<ContextPlanInput>),
    BeginCompaction(Box<ContextCompactionStartCommit>),
    FailCompaction(Box<ContextCompactionFailureCommit>),
    ResolveCompaction(Box<ContextCompactionResolutionCommit>),
    CommitPlan {
        plan: Box<ContextPlanCommit>,
        compaction: Option<Box<ContextCompactionResolutionCommit>>,
    },
    DrainSteering,
    DrainFollowUp,
    WritePlanContent(Box<musteragent_context::ContextPlanContentWrite>),
    ReadInputContent(musteragent_content_ref::ContentRef),
}
#[derive(Debug, Clone, PartialEq)]
pub enum AgentContextReply {
    SupervisedModelStart(Box<musteragent_runtime_model::SupervisedModelStart>),
    Files {
        json: Vec<u8>,
        authorized_content: Vec<musteragent_content_ref::ContentRef>,
    },
    CompactionGeneration(Option<musteragent_context::CompactionGenerationObservation>),
    CompactionSlot(Option<Box<musteragent_context::PersistedContextCompactionSlot>>),
    CompactionRecovery(Option<musteragent_context::ContextCompactionRecovery>),
    ModelDisposition(musteragent_runtime_model::ModelPersistenceDisposition),
    Failure(crate::LoopPortError),
    Policy(Box<PinnedRunExecutionPolicy>),
    ToolPolicy(ToolExecutionPolicyDecision),
    Material(Box<ContextMaterial>),
    Compaction(ContextPlanDisposition),
    Plan(ContextPlanResolution),
    ControlsConsumed(u32),
    PlanContent(musteragent_content_ref::ContentRef),
    InputContent(musteragent_context::MaterializedContent),
}
