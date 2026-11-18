//! Original signing projection shared by Cloud facts, Device transport and issuer.
use crate::wire;
use musteragent_content_ref::Sha256;
use musteragent_runtime_session::{
    DialogueStoreError, WorkItemDeviceExecutionPreparation, WorkItemExecutionGrantFacts,
    WorkItemExecutionPreparation, WorkItemExecutionTarget,
};
use prost::Message;
pub fn facts_from_preparation(
    value: &WorkItemExecutionPreparation,
) -> Result<wire::GetWorkItemExecutionGrantFactsResponse, DialogueStoreError> {
    facts_from_binding(&value.grant_fact_binding()?)
}
pub fn facts_from_device_preparation(
    value: &WorkItemDeviceExecutionPreparation,
) -> Result<wire::GetWorkItemExecutionGrantFactsResponse, DialogueStoreError> {
    value.validate()?;
    let mut facts = value.preparation().grant_fact_binding()?;
    facts.device_workspace_id = value.device_workspace_id();
    facts_from_binding(&facts)
}
pub fn facts_from_binding(
    f: &WorkItemExecutionGrantFacts,
) -> Result<wire::GetWorkItemExecutionGrantFactsResponse, DialogueStoreError> {
    if f.device_workspace_id.is_some()
        && (f.report.is_some() || !matches!(f.target, WorkItemExecutionTarget::RemoteDevice(_)))
    {
        return Err(DialogueStoreError::Corrupt);
    }
    let k = f.key;
    macro_rules! id {
        ($kind:ident, $value:expr) => {
            Some(wire::$kind {
                value: $value.to_string(),
            })
        };
    }
    let facts = wire::WorkItemExecutionGrantFacts {
        device_workspace_id: f.device_workspace_id.map(|id| wire::WorkspaceId {
            value: id.to_string(),
        }),
        actor: Some(wire::Actor {
            tenant_id: id!(TenantId, k.actor.tenant_id),
            principal_id: id!(PrincipalId, k.actor.principal_id),
            principal_kind: wire::PrincipalKind::User as i32,
        }),
        work_item_id: id!(WorkItemId, k.work_item_id),
        goal_revision: k.goal_revision.get(),
        control_revision: k.control_revision.get(),
        assignment_epoch: k.assignment_epoch.get(),
        space_id: id!(OwnerSpaceId, k.scope.space_id),
        owner_id: id!(PrincipalId, k.scope.owner_id),
        requesting_agent_id: id!(AgentId, k.scope.agent_id),
        executing_agent_id: id!(AgentId, k.executing_agent_id),
        source_dialogue_id: id!(AgentDialogueId, f.source_dialogue_id),
        source_intent_id: id!(UserIntentId, f.source_intent_id),
        executing_dialogue_id: id!(AgentDialogueId, f.executing_dialogue_id),
        executing_agent_version_id: id!(AgentVersionId, f.executing_agent_version_id),
        command_id: id!(CommandId, f.identities.command_id),
        conversation_id: id!(ConversationId, f.identities.conversation_id),
        requested_by: id!(ParticipantId, f.identities.requested_by),
        user: id!(ParticipantId, f.identities.user),
        executor: id!(ParticipantId, f.identities.executor),
        message_id: id!(MessageId, f.identities.message_id),
        run_id: id!(RunId, f.run_id),
        context_watermark: Some(wire::AgentDialogueCursor {
            epoch: f.context_watermark.epoch,
            seq: f.context_watermark.seq,
        }),
        context_source_sha256: f.context_source_sha256.into_bytes().to_vec(),
        manifest_revision: f.manifest_revision,
        instructions_revision: f.instructions_revision,
        permission_revision: f.permission_revision,
        permission_mode: f.permission_mode,
        default_agent_id: id!(AgentId, f.default_agent_id),
        delegation: Some(super::delegation(&f.delegation)),
        source_sha256: f.source_sha256.into_bytes().to_vec(),
        device_target: match &f.target {
            musteragent_runtime_session::WorkItemExecutionTarget::Cloud => None,
            musteragent_runtime_session::WorkItemExecutionTarget::RemoteDevice(target) => {
                Some(super::target(target))
            }
        },
        report: f
            .report
            .as_ref()
            .map(|report| wire::WorkItemExecutionReport {
                producing_run_id: id!(RunId, report.producing_run_id),
                producing_snapshot_id: id!(RuntimeSnapshotId, report.producing_snapshot_id),
                producing_agent_id: id!(AgentId, report.producing_agent_id),
                content_ref: Some(crate::encode_content_ref(&report.content_ref)),
                outcome: report.outcome.as_str().to_owned(),
            }),
    };
    Ok(wire::GetWorkItemExecutionGrantFactsResponse {
        facts_sha256: Sha256::digest(facts.encode_to_vec()).into_bytes().to_vec(),
        facts: Some(facts),
    })
}
