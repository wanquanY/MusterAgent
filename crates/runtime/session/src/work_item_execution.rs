//! Task execution uses the ordinary Runtime admission and a separate typed
//! business source. A WorkItem is never converted into a synthetic UserIntent.
use crate::WorkItemAuthorityRecord;
use crate::{AdmissionCommit, AgentAdmissionIdentity, DialogueStoreError};
use musteragent_definition::owned_agent::{OwnedAgent, OwnedAgentConfiguration, OwnedAgentStatus};
use musteragent_execution_context::DialogueContextCut;
use musteragent_identity::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_work_item::{
    OwnedAgentWorkItemScope, WorkItemAgentReceiver, WorkItemAssignee, WorkItemControlRevision,
    WorkItemExecutionIdentity, WorkItemGoalRevision, WorkItemReturnTarget, WorkItemScope,
};

mod device;
pub use device::*;
mod device_continuity;
pub use device_continuity::*;
mod device_admission;
pub use device_admission::*;
mod device_control;
pub use device_control::*;
mod device_control_port;
pub use device_control_port::*;
mod device_material;
pub use device_material::*;
mod device_memory;
pub use device_memory::*;
mod device_grant;
pub use device_grant::*;
mod device_publication_cut;
pub use device_publication_cut::*;
mod dependency;
pub use dependency::*;
mod result;
pub use result::*;
mod report;
pub use report::*;
mod control;
pub use control::*;
mod dispatch;
pub use dispatch::*;
mod coordination;
pub use coordination::*;
mod feedback;
pub use feedback::*;
mod followup;
pub use followup::*;
mod start;
pub use start::*;
mod mutation;
pub use mutation::*;
mod query;
pub use query::*;
mod facts;
mod grant;
mod identity;
pub use facts::WorkItemDelegationCeiling;
pub use facts::WorkItemExecutionGrantFacts;
pub use grant::*;
pub use identity::{WorkItemExecutionIdentities, WorkItemExecutionKey, WorkItemExecutionRequest};

/// Immutable reservation made by the original WorkItem owner. Current Agent,
/// privacy and capability authority is rechecked by admission before commit.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemExecutionPreparation {
    pub key: WorkItemExecutionKey,
    pub work_item: WorkItemAuthorityRecord,
    pub executing_agent: OwnedAgent,
    pub configuration: OwnedAgentConfiguration,
    pub assistant_identity: AgentAdmissionIdentity,
    pub default_agent_id: AgentId,
    pub context_cut: DialogueContextCut,
    pub delegation: WorkItemDelegationCeiling,
    pub target: WorkItemExecutionTarget,
    /// Display metadata is reloaded from the immutable accepted input owner;
    /// exact byte authority is bounded by delegation.input_refs.
    pub input_attachments: Vec<musteragent_runtime_contract::AttachmentRef>,
    pub feedback: Option<WorkItemFeedbackContext>,
    pub report: Option<WorkItemExecutionReport>,
    /// Exact verified prerequisites, supplied by the original Task authority.
    pub dependency_results: Vec<WorkItemDependencyResult>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemFeedbackContext {
    pub previous_result: musteragent_content_ref::ContentRef,
    /// Exact files from the preceding submission, readable on demand by its original executor.
    pub files: Vec<musteragent_content_ref::ContentRef>,
    pub request: musteragent_content_ref::ContentRef,
}
impl WorkItemExecutionPreparation {
    pub fn input_content_ref(&self) -> &musteragent_content_ref::ContentRef {
        self.report
            .as_ref()
            .map(|report| &report.content_ref)
            .or_else(|| self.feedback.as_ref().map(|feedback| &feedback.request))
            .unwrap_or(&self.work_item.goal_content_ref)
    }
    pub fn actor(&self) -> Actor {
        self.key.actor
    }
    pub fn identities(&self) -> WorkItemExecutionIdentities {
        self.key.identities()
    }
    pub fn binding(&self) -> Result<WorkItemExecutionIdentity, DialogueStoreError> {
        self.validate()?;
        if self.report.is_some() {
            return Err(DialogueStoreError::Corrupt);
        }
        let scope = self.key.scope;
        Ok(WorkItemExecutionIdentity {
            tenant_id: self.key.actor.tenant_id,
            scope: WorkItemScope::OwnedAgent(scope),
            work_item_id: self.key.work_item_id,
            goal_revision: self.key.goal_revision,
            control_revision: self.key.control_revision,
            assignment_epoch: self.key.assignment_epoch,
            source_run_id: self.identities().run_id()?,
            assignee: if self.executing_agent.agent_id == scope.agent_id {
                WorkItemAssignee::SelfAgent {
                    published_version_id: self.executing_agent.published_version_id,
                }
            } else {
                WorkItemAssignee::DelegatedAgent {
                    agent_id: self.executing_agent.agent_id,
                    published_version_id: self.executing_agent.published_version_id,
                }
            },
            return_target: WorkItemReturnTarget::OwnedAgent(WorkItemAgentReceiver {
                tenant_id: self.key.actor.tenant_id,
                scope,
                dialogue_id: self.work_item.source_dialogue_id,
            }),
        })
    }
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.key.validate()?;
        self.delegation.validate()?;
        self.target.validate()?;
        if let Some(feedback) = &self.feedback
            && (self.report.is_some()
                || self.key.assignment_epoch.get() < 2
                || feedback.files.len() > 16
                || feedback.previous_result.media_type.as_str()
                    != "application/vnd.musteragent.task-report+json"
                || feedback.previous_result.content_id == feedback.request.content_id)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        if self.report.is_some() && self.target != WorkItemExecutionTarget::Cloud {
            return Err(DialogueStoreError::Corrupt);
        }
        musteragent_execution_context::validate_dialogue_attachments(&self.input_attachments)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        if self
            .input_attachments
            .iter()
            .map(|attachment| &attachment.content_ref)
            .ne(self.delegation.input_refs.iter())
        {
            return Err(DialogueStoreError::Corrupt);
        }
        // Empty remains decodable for retained historical device sources. New
        // preparation always supplies every prerequisite before admission.
        if !self.dependency_results.is_empty() {
            if self.report.is_some()
                || self
                    .dependency_results
                    .iter()
                    .map(|d| d.work_item_id)
                    .ne(self.work_item.core.spec.dependencies.iter().copied())
            {
                return Err(DialogueStoreError::Corrupt);
            }
            for dependency in &self.dependency_results {
                dependency.validate()?;
            }
        }
        match (&self.report, self.key.report_from_run_id) {
            (None, None) => {}
            (Some(r), Some(id))
                if r.producing_run_id == id
                    && self.key.executing_agent_id == self.key.scope.agent_id => {}
            _ => return Err(DialogueStoreError::Corrupt),
        }
        let t = &self.work_item;
        let a = &self.executing_agent;
        if t.core.work_item_id != self.key.work_item_id
            || t.core.tenant_id != self.key.actor.tenant_id
            || t.core.scope != WorkItemScope::OwnedAgent(self.key.scope)
            || t.core.revisions.goal != self.key.goal_revision
            || t.core.revisions.control != self.key.control_revision
            || t.core.assignment_epoch != Some(self.key.assignment_epoch)
            || (t.core.status.is_terminal() && self.report.is_none())
            || a.space_id != self.key.scope.space_id
            || a.agent_id != self.key.executing_agent_id
            || a.status != OwnedAgentStatus::Active
            || self.configuration.agent_id != a.agent_id
            || self.context_cut.tenant_id != self.key.actor.tenant_id
            || self.context_cut.owner_id != self.key.scope.owner_id
            || self.context_cut.agent_id != a.agent_id
            || self.context_cut.dialogue_id != a.dialogue_id
        {
            return Err(DialogueStoreError::Corrupt);
        }
        self.context_cut
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        a.validate_definition(
            &self.assistant_identity.definition,
            &self.assistant_identity.version,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?;
        self.configuration
            .validate()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkItemExecutionAdmissionCommit {
    pub preparation: WorkItemExecutionPreparation,
    pub admission: AdmissionCommit,
}
impl WorkItemExecutionAdmissionCommit {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        self.preparation.validate()?;
        let p = &self.preparation;
        let b = &self.admission.batch;
        let identity = p.identities();
        b.validate().map_err(|_| DialogueStoreError::Corrupt)?;
        let ids = musteragent_runtime_admission::AdmissionIdentities::derive(identity.command_id)
            .ok_or(DialogueStoreError::Corrupt)?;
        if p.delegation
            .product_release
            .as_ref()
            .is_some_and(|r| b.runtime_snapshot.execution_grant.product_release.as_ref() != Some(r))
            || b.actor != p.actor()
            || b.run.context.conversation_id != identity.conversation_id
            || b.run.context.executor_participant_id != identity.executor
            || b.run.context.requested_by_participant_id != identity.requested_by
            || b.run.context.run_id != ids.run_id
            || b.runtime_snapshot.runtime_snapshot_id != ids.runtime_snapshot_id
            || b.runtime_snapshot.memory_agent_id != p.executing_agent.agent_id
            || b.message.message_id != identity.message_id
            || b.message.content != *p.input_content_ref()
            || b.run.context.parent_run_id.is_some()
            || b.run.context.delegation_id.is_some()
            || b.conversation_title.is_some()
            || b.append.expected_offset != LedgerOffset::ZERO
            || b.run.context.home_epoch.get() != 1
            || self.admission.content_stage.content != *p.input_content_ref()
            || self.admission.content_stage.command_fingerprint != b.append.command_fingerprint
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkItemExecutionReceipt {
    pub request: WorkItemExecutionRequest,
    pub run_id: RunId,
    pub conversation_id: ConversationId,
}

/// Preparation is a read of a durable reservation, not a claim or an execution.
pub trait WorkItemExecutionStore: Send + Sync {
    fn prepare(
        &self,
        request: WorkItemExecutionRequest,
    ) -> ApplicationFuture<'_, Result<WorkItemExecutionPreparation, DialogueStoreError>>;
    /// Grant signing reads physical placement from the original reservation.
    /// A legacy implementation cannot assert uncaptured Device facts as None.
    fn prepare_grant_facts(
        &self,
        request: WorkItemExecutionRequest,
    ) -> ApplicationFuture<'_, Result<WorkItemExecutionGrantFacts, DialogueStoreError>> {
        Box::pin(async move {
            let value = self.prepare(request).await?;
            if matches!(value.target, WorkItemExecutionTarget::RemoteDevice(_)) {
                return Err(DialogueStoreError::Unavailable);
            }
            value.grant_fact_binding()
        })
    }
    fn lookup(
        &self,
        request: WorkItemExecutionRequest,
    ) -> ApplicationFuture<'_, Result<Option<WorkItemExecutionReceipt>, DialogueStoreError>>;
    fn admit(
        &self,
        commit: WorkItemExecutionAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<WorkItemExecutionReceipt, DialogueStoreError>>;
}

mod verification;
pub use verification::*;
