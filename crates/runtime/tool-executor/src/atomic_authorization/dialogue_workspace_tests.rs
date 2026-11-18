use super::dialogue_workspace_scope_matches as accepts;
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::*;
use musteragent_run::RunContext;
use musteragent_runtime_snapshot::DIALOGUE_DEVICE_DISCOVERY_POLICY as DIALOGUE_DEVICE_POLICY;

fn run(device: DeviceId, workspace: Option<WorkspaceId>) -> RunContext {
    RunContext {
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        activity_id: ActivityId::generate(),
        activity_kind: musteragent_activity::ActivityKind::Chat,
        run_id: RunId::generate(),
        executor_participant_id: ParticipantId::generate(),
        requested_by_participant_id: ParticipantId::generate(),
        parent_run_id: None,
        delegation_id: None,
        execution_scope: ExecutionScope::DevicePrivate {
            device_id: device,
            workspace_id: workspace,
        },
        control_home: RuntimeHome::Device { device_id: device },
        execution_home: RuntimeHome::Device { device_id: device },
        home_epoch: HomeEpoch::new(1).unwrap(),
        runtime_snapshot_id: RuntimeSnapshotId::generate(),
    }
}

#[test]
fn workspace_qualified_dialogue_tools_require_exact_original_device_scope() {
    let device = DeviceId::generate();
    let workspace = WorkspaceId::generate();
    let mut context = run(device, Some(workspace));
    assert!(accepts(
        &context,
        Some(DIALOGUE_DEVICE_POLICY),
        true,
        Some(workspace)
    ));
    assert!(!accepts(&context, Some(DIALOGUE_DEVICE_POLICY), true, None));
    assert!(!accepts(
        &context,
        Some(DIALOGUE_DEVICE_POLICY),
        true,
        Some(WorkspaceId::generate())
    ));
    assert!(!accepts(
        &context,
        Some(DIALOGUE_DEVICE_POLICY),
        false,
        Some(workspace)
    ));
    assert!(!accepts(&context, None, true, Some(workspace)));
    assert!(!accepts(
        &context,
        Some("musteragent.dialogue-discovery@13"),
        true,
        Some(workspace)
    ));
    context.execution_home = RuntimeHome::Cloud;
    assert!(!accepts(
        &context,
        Some(DIALOGUE_DEVICE_POLICY),
        true,
        Some(workspace)
    ));
    context = run(device, Some(workspace));
    context.control_home = RuntimeHome::Device {
        device_id: DeviceId::generate(),
    };
    assert!(!accepts(
        &context,
        Some(DIALOGUE_DEVICE_POLICY),
        true,
        Some(workspace)
    ));
    context = run(device, Some(workspace));
    context.execution_scope = ExecutionScope::Workspace {
        workspace_id: workspace,
    };
    assert!(!accepts(
        &context,
        Some(DIALOGUE_DEVICE_POLICY),
        true,
        Some(workspace)
    ));
    assert!(!accepts(&context, Some(DIALOGUE_DEVICE_POLICY), true, None));
}

#[test]
fn legacy_and_cloud_dialogue_tools_do_not_acquire_a_directory() {
    let mut context = run(DeviceId::generate(), None);
    assert!(accepts(&context, None, false, None));
    assert!(!accepts(
        &context,
        Some(DIALOGUE_DEVICE_POLICY),
        true,
        Some(WorkspaceId::generate())
    ));
    context.execution_scope = ExecutionScope::Tenant {
        tenant_id: context.tenant_id,
    };
    context.control_home = RuntimeHome::Cloud;
    context.execution_home = RuntimeHome::Cloud;
    assert!(accepts(&context, None, false, None));
    assert!(!accepts(
        &context,
        Some(DIALOGUE_DEVICE_POLICY),
        true,
        Some(WorkspaceId::generate())
    ));
}
