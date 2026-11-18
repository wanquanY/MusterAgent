use musteragent_activity::ActivityKind;
use musteragent_conversation::SubmitMessage;
use musteragent_run::RunContext;
use musteragent_runtime_contract::CommandEnvelope;

use crate::RuntimeHomePolicy;
use crate::identity::AdmissionIdentities;

pub(super) fn resolved_request_run_context(
    home_policy: &RuntimeHomePolicy,
    authority: &crate::AdmissionAuthority,
    command: &CommandEnvelope<SubmitMessage>,
    ids: &AdmissionIdentities,
) -> RunContext {
    RunContext {
        tenant_id: authority.tenant_id,
        conversation_id: authority.conversation_id,
        activity_id: ids.activity_id,
        activity_kind: ActivityKind::Chat,
        run_id: ids.run_id,
        executor_participant_id: authority.executor_participant_id,
        requested_by_participant_id: authority.requested_by_participant_id,
        parent_run_id: None,
        delegation_id: None,
        execution_scope: command.payload.execution_scope,
        control_home: home_policy.control_home(),
        execution_home: home_policy.execution_home(),
        home_epoch: authority.home_epoch,
        runtime_snapshot_id: ids.runtime_snapshot_id,
    }
}
