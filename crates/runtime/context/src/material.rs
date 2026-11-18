use musteragent_content_ref::ContentRef;
use musteragent_identity::{
    ActivityId, ContextBranchId, ContextSnapshotId, ConversationSeq, DelegationDeliveryId,
    DelegationId, InstinctId, InvocationId, LedgerRecordId, MemoryVersionId, RunId, SkillVersionId,
    VersionNumber, VersionedKey,
};
use musteragent_model::{ModelKey, SignedModelRouteHash};
use musteragent_run::RunContext;
use musteragent_runtime_model::{CanonicalMessage, CanonicalRole};
use musteragent_runtime_snapshot::RuntimeSnapshotContent;
use musteragent_tool::{ToolKey, WireName};
use std::num::{NonZeroU32, NonZeroU64};

/// Immutable bytes resolved from a Snapshot-pinned ContentRef.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MaterializedContent {
    pub content_ref: ContentRef,
    pub bytes: Vec<u8>,
}

/// Latest durable compaction boundary that can seed the active model Context.
///
/// The immutable Ledger remains the source of truth. This boundary is only a
/// projection checkpoint: `summary` replaces the summarized prefix,
/// `retained_record_ids` identifies the exact suffix that was kept when the
/// checkpoint was committed, and records after `source_seq_to` are the new
/// tail. Keeping these three facts together prevents a later Tool iteration
/// from rebuilding and re-compacting the pre-checkpoint history.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MaterializedContextBaseline {
    pub snapshot_id: ContextSnapshotId,
    pub source_seq_to: ConversationSeq,
    pub retained_record_ids: Vec<LedgerRecordId>,
    pub summary: MaterializedContent,
}

/// One canonical model-context entry with exact Ledger provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct ContextEntry {
    /// Original storage-page coverage and typed content. Host-only: the model
    /// request carries the individual messages and data parts, never this page.
    pub history_page: Option<crate::BoundConversationHistory>,
    /// Typed private Runtime observations. For these entries `message` is only
    /// the bounded text view used for sizing/compaction, never a participant
    /// message. Provider history excludes it and emits these typed events.
    pub runtime_events: Vec<musteragent_runtime_model::CanonicalRuntimeEvent>,
    pub source_sequence: ConversationSeq,
    /// Semantic Provider-input order. A completed Tool exchange shares the
    /// Assistant ToolCall record's anchor and uses model-source ordinals for
    /// its Tool Results, while `source_sequence` continues to identify each
    /// fact's actual durable commit position.
    pub presentation_order: ContextPresentationOrder,
    pub source_record_ids: Vec<LedgerRecordId>,
    /// Run that authored this durable fact, when the Ledger record is
    /// Run-scoped. This is execution provenance, not transcript ownership.
    /// The planner uses it to restore logical turn order when multiple root
    /// Chat Runs were admitted before a predecessor finished.
    pub source_run_id: Option<RunId>,
    /// Exact physical model source for an Assistant-authored entry.
    ///
    /// `None` is valid for user/tool entries and for legacy Assistant history
    /// whose physical invocation identity cannot be proven. Unknown provenance
    /// is always projected portably; it must never inherit the active route.
    pub assistant_origin: Option<AssistantOrigin>,
    pub message: CanonicalMessage,
    /// The Provider invocation whose opaque continuation precedes this entry.
    /// Normally this marks Tool results returned after that model response.
    /// On a no-Tool Assistant entry, a marker matching `assistant_origin`
    /// proves a durable generation token-limit terminal and requests continuation.
    pub continuation_delta_for: Option<InvocationId>,
}

/// Immutable source identity of one physical Assistant model completion.
///
/// The signed hash commits every field of the admitted [`ModelRoute`]. The
/// requested and Provider-reported response model keys are retained as an
/// additional issuer guard for signed/opaque Provider artifacts.
/// A provider-visible message and the proof governing private replay. A
/// bound transcript comes from a different Run and never grants its actions.
pub(crate) struct MessageViewMut<'a> {
    pub message: &'a mut CanonicalMessage,
    pub assistant_origin: &'a mut Option<AssistantOrigin>,
    pub source_run_id: Option<RunId>,
}

impl ContextEntry {
    pub fn validate_messages(&self) -> Result<(), crate::ContextPlanError> {
        if let Some(page) = &self.history_page {
            if !self.runtime_events.is_empty()
                || !self.message.content.is_empty()
                || !self.message.tool_calls.is_empty()
                || page.from_seq == 0
                || page.through_seq < page.from_seq
            {
                return Err(crate::ContextPlanError::invariant());
            }
            for item in &page.history.items {
                if let crate::ConversationHistoryItem::Message(message, origin) = item {
                    if origin.is_some()
                        && message.role != musteragent_runtime_model::CanonicalRole::Assistant
                    {
                        return Err(crate::ContextPlanError::invariant());
                    }
                    message
                        .validate()
                        .map_err(|_| crate::ContextPlanError::invariant())?;
                }
            }
        } else {
            self.message
                .validate()
                .map_err(|_| crate::ContextPlanError::invariant())?;
        }
        Ok(())
    }

    pub(crate) fn model_messages_mut(&mut self) -> impl Iterator<Item = MessageViewMut<'_>> {
        let own = (self.history_page.is_none() && self.runtime_events.is_empty()).then_some(
            MessageViewMut {
                message: &mut self.message,
                assistant_origin: &mut self.assistant_origin,
                source_run_id: self.source_run_id,
            },
        );
        own.into_iter()
            .chain(self.history_page.iter_mut().flat_map(|page| {
                page.history.items.iter_mut().filter_map(|item| match item {
                    crate::ConversationHistoryItem::Message(message, origin) => {
                        Some(MessageViewMut {
                            message,
                            assistant_origin: origin,
                            source_run_id: None,
                        })
                    }
                    crate::ConversationHistoryItem::Reference(_) => None,
                })
            }))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AssistantOrigin {
    pub invocation_id: InvocationId,
    pub model_route_ref: VersionedKey,
    pub route_identity_hash: SignedModelRouteHash,
    pub requested_model_key: ModelKey,
    pub response_model_key: ModelKey,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ContextPresentationOrder {
    pub anchor_sequence: ConversationSeq,
    pub exchange_ordinal: u32,
}

impl ContextPresentationOrder {
    #[must_use]
    pub const fn ledger(sequence: ConversationSeq) -> Self {
        Self {
            anchor_sequence: sequence,
            exchange_ordinal: 0,
        }
    }

    #[must_use]
    pub const fn tool_result(anchor_sequence: ConversationSeq, source_ordinal: u32) -> Self {
        Self {
            anchor_sequence,
            exchange_ordinal: source_ordinal.saturating_add(1),
        }
    }
}

/// Last durable main-model usage that can anchor the next context estimate.
/// Entries after `source_sequence` are estimated locally; all earlier prompt
/// material is already represented by the Provider's physical token count.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextUsageAnchor {
    pub source_sequence: ConversationSeq,
    pub total_tokens: u64,
}

/// One completed provider request's actual input, including cached input.
/// A token-limited empty response is a valid usage observation even though
/// the task needs recovery rather than recording successful progress.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MeasuredContextUsage {
    pub source_sequence: ConversationSeq,
    pub input_tokens: u64,
    /// Only for the current authorized recovery invocation, which may have no
    /// Assistant entry. Home validates its terminal, usage and admitted route.
    pub recovery_origin: Option<AssistantOrigin>,
}

/// A Memory version selected at admission and materialized by exact version ID.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MaterializedMemory {
    pub memory_version_id: MemoryVersionId,
    pub content: MaterializedContent,
}

/// Exact governed Instinct revision pinned into the immutable Run Snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MaterializedInstinct {
    pub instinct_id: InstinctId,
    pub revision: VersionNumber,
    pub payload: MaterializedContent,
}

/// One authorized Skill disclosed to the model in monotonic Run order.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MaterializedSkill {
    pub skill_ref: VersionedKey,
    pub skill_version_id: SkillVersionId,
    pub instructions_hash: musteragent_content_ref::Sha256,
    pub disclosure_ordinal: std::num::NonZeroU32,
    pub instructions: MaterializedContent,
}

/// Current durable Run execution plan. Its immutable body is re-materialized
/// on every iteration, so compaction and Attempt takeover cannot erase it.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MaterializedExecutionPlan {
    pub revision: NonZeroU64,
    pub content: MaterializedContent,
}

/// One durable asynchronous ChildReport awaiting consumption by the parent
/// Run. This is private Runtime context with exact delegation provenance; it
/// is never projected as a user or peer message in the visible conversation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MaterializedDelegationCompletion {
    pub delivery_id: DelegationDeliveryId,
    pub delegation_id: DelegationId,
    pub child_activity_id: ActivityId,
    pub child_run_id: RunId,
    pub report: MaterializedContent,
}

/// Durable consumption cut of one typed Run-control stimulus.
///
/// A follow-up or steering message becomes a durable Conversation fact when it
/// is admitted, but it becomes model input only when the owning Attempt
/// atomically records its `ControlConsumed` fence. A message admitted while an
/// older model invocation is still in flight therefore precedes that
/// invocation's committed reply in the immutable Ledger even though it belongs
/// to the *next* logical turn. This cut binds the stimulus' exact Ledger
/// identity to its consumption boundary so presentation can restore the causal
/// turn order without rewriting Ledger order, message identity, or role.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ConsumedStimulusCut {
    /// Immutable Ledger record of the consumed Conversation message.
    pub stimulus_record_id: LedgerRecordId,
    /// Ledger position of the Run's `ControlConsumed` fence. Everything durable
    /// before this position (including the in-flight reply that follows the
    /// stimulus in Ledger order) belongs to earlier logical turns.
    pub consumed_sequence: ConversationSeq,
    /// Typed control identity of the stimulus. Presentation keeps the
    /// message's own role and provenance; this is never projected as a
    /// synthetic user turn.
    pub kind: musteragent_run::RunControlKind,
}

/// Latest consecutive model-authored action rejection that must be projected
/// as private corrective context. It is never a conversation message and
/// never grants Tool authority.
#[derive(Debug, Clone, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The public enum exposes these payloads by value to host implementations."
)]
pub enum MaterializedModelActionRejection {
    InvalidPublicDeliveryWithCandidate {
        contract_ref: musteragent_identity::VersionedKey,
        issue: musteragent_execution_context::PublicDeliveryIssue,
        repair: musteragent_execution_context::PublicDeliveryRejectionV1,
        candidate: MaterializedContent,
        consecutive_count: NonZeroU32,
    },
    InvalidPublicDelivery {
        contract_ref: musteragent_identity::VersionedKey,
        issue: musteragent_execution_context::PublicDeliveryIssue,
        consecutive_count: NonZeroU32,
    },
    ToolNotExposed {
        attempted_name: WireName,
        consecutive_count: NonZeroU32,
    },
    InvalidToolArguments {
        attempted_name: WireName,
        issues: Vec<musteragent_runtime_contract::ToolArgumentValidationIssue>,
        consecutive_count: NonZeroU32,
    },
}

/// Frozen Runtime-core guard against repeatedly receiving the same rejected
/// model action. Two corrective turns are allowed: weaker models often repeat
/// a mistake once before following specific feedback. A third consecutive
/// rejection with the same durable fingerprint terminates with that
/// rejection's real protocol code. A materially different rejection starts a
/// fresh correction boundary.
pub const MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS: u32 = 3;

/// Complete durable source required to rebuild one model iteration.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentInstructionMaterial {
    pub scene_members: Vec<musteragent_execution_context::AgentSceneMember>,
    pub agent_id: musteragent_identity::AgentId,
    pub revision: NonZeroU64,
    /// None explicitly clears previously admitted instructions.
    pub content: Option<MaterializedContent>,
}

/// Planning locator derived from the original signed input.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DesktopSourceHost {
    pub membership_id: uuid::Uuid,
    pub membership_epoch: NonZeroU64,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ContextMaterial {
    /// Immutable orchestration lineage, verified by the source owner. A derived
    /// task retains source permissions without claiming its Goal history window.
    pub derived_source_run_id: Option<RunId>,
    pub desktop_source_host: Option<DesktopSourceHost>,
    /// Current account-owned instructions sampled at this physical planning boundary.
    /// Kept separate from the immutable admission snapshot and preserved in the sealed request.
    pub agent_instructions: Option<AgentInstructionMaterial>,
    /// Current original approval facts; never summarized into narrative history.
    pub dialogue_approval_context: Option<musteragent_execution_context::DialogueApprovalContext>,
    /// Current bounded execution ownership; outside lossy narrative summaries.
    pub dialogue_execution_context: Option<musteragent_execution_context::DialogueExecutionContext>,
    /// Consumed input that superseded the immutable first compression window.
    pub superseding_control_id: Option<LedgerRecordId>,
    /// Original source Owner's observed prepared-plan lineage. None retains
    /// legacy generation-1 behavior; durable forward stores supply a selection
    /// made by the shared rule and revalidate it at commit and model begin.
    pub plan_selection: Option<crate::ContextPlanSelection>,
    /// Successful source reads captured for the final transaction's current
    /// content authorization check. This is metadata, never a cache permit.
    pub source_content_refs: Vec<ContentRef>,
    pub run_context: RunContext,
    /// Durable admission boundary of the target Run. Records authored by this
    /// Run at or before this sequence are its initial turn stimulus; later
    /// records are its own execution suffix.
    pub admitted_sequence: ConversationSeq,
    /// Exact original user-message Ledger record bound by this Run's retry
    /// admission. Reused as the turn stimulus without changing its provenance
    /// or creating a second conversation message.
    pub retry_stimulus_record_id: Option<LedgerRecordId>,
    /// Typed Run-control stimuli consumed by this Run, each with its exact
    /// durable consumption cut. Sourced from the Run's own ControlQueued /
    /// ControlConsumed facts; unconsumed control messages are not part of the
    /// material at all. Retried or replanned iterations reload the same cuts,
    /// so a failed model call never loses an already-consumed stimulus.
    pub consumed_stimulus_cuts: Vec<ConsumedStimulusCut>,
    pub runtime_snapshot: RuntimeSnapshotContent,
    pub resolved_tools: Vec<crate::ResolvedContextTool>,
    /// Physical Tool surface policy for this model request. `DenyAll` is used
    /// by independent evaluation Runs and is enforced again by the planner;
    /// it is not an advisory prompt instruction.
    pub tool_policy: ContextToolPolicy,
    /// Exact Tool projection rebuilt from the Run Snapshot and discovery
    /// Ledger facts in first-disclosure order.
    pub exposed_tool_keys: Vec<ToolKey>,
    pub prompt_sections: Vec<MaterializedContent>,
    pub disclosed_skills: Vec<MaterializedSkill>,
    pub memories: Vec<MaterializedMemory>,
    pub instincts: Vec<MaterializedInstinct>,
    pub execution_plan: Option<MaterializedExecutionPlan>,
    /// Pending or delivered-but-unacknowledged asynchronous ChildReports,
    /// ordered by their durable delivery timestamp and stable ID.
    pub delegation_completions: Vec<MaterializedDelegationCompletion>,
    /// Private durable feedback derived from the latest physical model
    /// invocation. `None` means the latest invocation was not rejected before
    /// Tool admission.
    pub model_action_rejection: Option<MaterializedModelActionRejection>,
    /// Latest primary invocation is a durable, explicitly classified context
    /// overflow. The pinned compaction policy decides whether to recover it.
    pub context_capacity_exceeded: bool,
    /// Failed primary invocation whose durable retry/repair release authorizes
    /// this turn. Cleared by the next primary start; never sourced from prose.
    pub model_recovery: Option<InvocationId>,
    /// Exact persisted canonical bytes of the last rejected primary request.
    pub rejected_request_bytes: Option<u64>,
    pub entries: Vec<ContextEntry>,
    /// One-based identity of the next product-visible Assistant response
    /// owned by this Run. Physical model iterations within one response turn
    /// share the same ordinal; a follow-up/control or another durable wake-up
    /// after a committed response advances it.
    pub next_assistant_response_ordinal: NonZeroU32,
    pub usage_anchor: Option<ContextUsageAnchor>,
    /// Successful current-Run input usage, invalidated by an installed checkpoint.
    pub measured_usage: Option<MeasuredContextUsage>,
    /// Latest applicable durable compaction projection. This is loaded by
    /// exact ContentRef so rolling summaries and post-compaction continuation
    /// never depend on an in-memory Worker or a mutable projection cache.
    pub previous_context: Option<MaterializedContextBaseline>,
    /// Active immutable conversation lineage at the sealed Ledger cutoff.
    pub active_branch_id: Option<ContextBranchId>,
    /// Highest Ledger sequence considered by this rebuild. A persisted
    /// compaction slot freezes this value across replacement Attempts.
    pub ledger_cutoff: ConversationSeq,
    pub source_seq_from: ConversationSeq,
    pub source_seq_to: ConversationSeq,
}

/// Transient dependencies of a candidate plan. The plan store checks these
/// under its existing commit lane; they do not change persisted request bytes
/// or immutable ContextSnapshot identity.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextSourceFence {
    pub content_refs: Vec<ContentRef>,
    pub active_branch_id: Option<ContextBranchId>,
    pub ledger_cutoff: ConversationSeq,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContextToolPolicy {
    SnapshotDiscovery,
    DenyAll,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextSummaryItem {
    pub source_sequence: ConversationSeq,
    pub source_record_ids: Vec<LedgerRecordId>,
    pub role: ContextSummaryRole,
    pub excerpt: String,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContextSummaryRole {
    Runtime,
    User,
    Assistant,
    Tool,
}

impl From<CanonicalRole> for ContextSummaryRole {
    fn from(value: CanonicalRole) -> Self {
        match value {
            CanonicalRole::User => Self::User,
            CanonicalRole::Assistant => Self::Assistant,
            CanonicalRole::Tool => Self::Tool,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct ContextFileOperations {
    pub read: Vec<String>,
    pub written: Vec<String>,
    pub edited: Vec<String>,
}

/// Structured, provenance-complete compaction projection. The narrative is
/// model-generated; `items` remains a bounded inspectable index. Neither is a
/// replacement for the immutable Ledger source range.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContextSummaryDocument {
    pub schema_version: &'static str,
    pub compacted: bool,
    pub source_seq_from: ConversationSeq,
    pub source_seq_to: ConversationSeq,
    pub summary: Option<String>,
    pub generator_invocation_id: Option<InvocationId>,
    pub previous_summary_ref: Option<ContentRef>,
    pub summarized_record_ids: Vec<LedgerRecordId>,
    pub file_operations: ContextFileOperations,
    pub items: Vec<ContextSummaryItem>,
    pub omitted_item_count: u64,
}

pub const LEGACY_CONTEXT_SUMMARY_SCHEMA: &str = "musteragent.legacy.context.summary.v1";
pub const CONTEXT_SUMMARY_SCHEMA: &str = "musteragent.legacy.context.summary.v2";
/// Summary produced exclusively from execution-isolated context sources.
pub const ISOLATED_CONTEXT_SUMMARY_SCHEMA: &str = "musteragent.context.summary.v3";
pub const NATIVE_CONTEXT_SUMMARY_SCHEMA: &str = "musteragent.context.summary.v4";

impl ContextMaterial {
    /// Closed logical identity; a hosted caller cannot label another cut as steering.
    pub fn runtime_compaction_source(
        &self,
    ) -> Result<crate::CompactionSource, crate::ContextPlanError> {
        let Some(id) = self.superseding_control_id else {
            return Ok(crate::CompactionSource::RuntimeLedger);
        };
        let cut = self
            .plan_selection
            .and_then(|selection| selection.source_control)
            .ok_or_else(crate::ContextPlanError::invariant)?;
        if cut.record_id != id || cut.sequence != self.ledger_cutoff {
            return Err(crate::ContextPlanError::invariant());
        }
        Ok(crate::CompactionSource::AfterControl(id))
    }
}
