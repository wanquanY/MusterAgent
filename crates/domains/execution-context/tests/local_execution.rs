#![cfg(test)]

use musteragent_content_ref::Sha256;
use musteragent_execution_context::{local_execution::*, *};
use musteragent_identity::*;

fn fixture() -> (DialogueExecutionFactBinding, DialogueInputOrigin, DeviceId) {
    let id = "019b2f80-0000-7000-8000-000000000001";
    (
        DialogueExecutionFactBinding {
            product_release: None,
            device_workspace_id: None,
            actor: Actor {
                tenant_id: id.parse().unwrap(),
                principal_id: id.parse().unwrap(),
                principal_kind: PrincipalKind::User,
            },
            intent_id: id.parse().unwrap(),
            agent_id: id.parse().unwrap(),
            agent_version_id: id.parse().unwrap(),
            dialogue_id: id.parse().unwrap(),
            context_conversation_id: id.parse().unwrap(),
            command_id: id.parse().unwrap(),
            run_id: id.parse().unwrap(),
            context_watermark: DialogueCursor { epoch: 1, seq: 3 },
            manifest_revision: 2,
            instructions_revision: 1,
            permission_revision: 1,
            permission_mode: 1,
            source_sha256: Sha256::digest(b"frozen-dialogue-source"),
            default_agent_direct_intent: true,
        },
        DialogueInputOrigin {
            surface: DialogueInputSurface::Desktop,
            installation_id: Some(id.parse().unwrap()),
        },
        id.parse().unwrap(),
    )
}
fn binding(
    source: DialogueExecutionFactBinding,
    origin: DialogueInputOrigin,
    device: DeviceId,
) -> LocalExecutionBinding {
    let install = origin.installation_id.unwrap();
    LocalExecutionBinding::new(source, origin, device, install).unwrap()
}
#[test]
fn local_binding_is_distinct_from_executor_assignment_and_stable_for_backend_signer() {
    let (source, origin, device) = fixture();
    let local = binding(source, origin, device);
    assert_eq!(
        local.digest().to_string(),
        "12987937eb2a30ac9e56d004aa1ae2240a411e81692eba8f7b3f87fcecbae6a8"
    );
    assert_eq!(local.digest(), Sha256::digest(local.canonical_bytes()));
    assert_eq!(
        local.grant_nonce(),
        format!("device_source_{}", local.digest())
    );
}
#[test]
fn every_canonical_source_field_and_native_target_changes_signed_nonce() {
    let (source, origin, device) = fixture();
    let original = binding(source.clone(), origin.clone(), device);
    let changes: &[fn(&mut DialogueExecutionFactBinding)] = &[
        |v| v.actor.tenant_id = TenantId::generate(),
        |v| v.actor.principal_id = PrincipalId::generate(),
        |v| v.intent_id = UserIntentId::generate(),
        |v| v.agent_id = AgentId::generate(),
        |v| v.agent_version_id = AgentVersionId::generate(),
        |v| v.dialogue_id = AgentDialogueId::generate(),
        |v| v.context_conversation_id = ConversationId::generate(),
        |v| v.command_id = CommandId::generate(),
        |v| v.run_id = RunId::generate(),
        |v| v.context_watermark.epoch += 1,
        |v| v.context_watermark.seq += 1,
        |v| v.manifest_revision += 1,
        |v| v.instructions_revision += 1,
        |v| v.permission_revision += 1,
        |v| v.permission_mode = 4,
        |v| v.source_sha256 = Sha256::digest(b"changed"),
        |v| v.default_agent_direct_intent = false,
    ];
    for change in changes {
        let mut changed = source.clone();
        change(&mut changed);
        assert_ne!(
            original.grant_nonce(),
            binding(changed, origin.clone(), device).grant_nonce()
        );
    }
    assert_ne!(
        original.digest(),
        binding(source.clone(), origin.clone(), DeviceId::generate()).digest()
    );
    let mut other = origin.clone();
    other.installation_id = Some(DeviceInstallationId::generate());
    assert_ne!(
        original.digest(),
        binding(source.clone(), other, device).digest()
    );
    let mut mobile = origin;
    mobile.surface = DialogueInputSurface::Mobile;
    assert_ne!(original.digest(), binding(source, mobile, device).digest());
}
#[test]
fn local_admission_rejects_foreign_installation_non_native_origin_and_malformed_source() {
    let (source, origin, device) = fixture();
    assert_eq!(
        LocalExecutionBinding::new(
            source.clone(),
            origin.clone(),
            device,
            DeviceInstallationId::generate()
        ),
        Err(LocalExecutionError::Origin)
    );
    let install = origin.installation_id.unwrap();
    let web = DialogueInputOrigin {
        surface: DialogueInputSurface::Web,
        installation_id: None,
    };
    assert_eq!(
        LocalExecutionBinding::new(source.clone(), web, device, install),
        Err(LocalExecutionError::Origin)
    );
    let changes: &[fn(&mut DialogueExecutionFactBinding)] = &[
        |v| v.actor.principal_kind = PrincipalKind::Service,
        |v| v.context_watermark.epoch = 0,
        |v| v.context_watermark.seq = 0,
        |v| v.manifest_revision = 0,
        |v| v.instructions_revision = u64::MAX,
        |v| v.permission_revision = 0,
        |v| v.permission_mode = 3,
        |v| v.permission_mode = 0,
        |v| v.permission_mode = 5,
        |v| v.source_sha256 = Sha256::from_bytes([0; 32]).unwrap(),
    ];
    for change in changes {
        let mut changed = source.clone();
        change(&mut changed);
        assert_eq!(
            LocalExecutionBinding::new(changed, origin.clone(), device, install),
            Err(LocalExecutionError::Source)
        );
    }
}

#[test]
fn physical_workspace_has_one_exact_forward_commitment_and_never_upgrades_none() {
    let (mut source, origin, device) = fixture();
    let legacy = binding(source.clone(), origin.clone(), device);
    let installation = origin.installation_id.unwrap();
    let workspace: WorkspaceId = "019b2f80-0000-7000-8000-000000000099".parse().unwrap();
    source.device_workspace_id = Some(workspace);
    source.source_sha256 = Sha256::from_bytes([
        102, 20, 40, 103, 139, 133, 128, 245, 144, 68, 62, 233, 84, 148, 26, 170, 142, 195, 36, 53,
        89, 149, 55, 103, 161, 217, 117, 195, 160, 79, 14, 233,
    ])
    .unwrap();
    assert!(
        LocalExecutionBinding::new(source.clone(), origin.clone(), device, installation).is_err()
    );
    assert!(
        LocalExecutionBinding::new_in_workspace(
            source.clone(),
            origin.clone(),
            device,
            installation,
            Some(WorkspaceId::generate())
        )
        .is_err()
    );
    let placed = LocalExecutionBinding::new_in_workspace(
        source,
        origin,
        device,
        installation,
        Some(workspace),
    )
    .unwrap();
    assert_eq!(placed.device_workspace_id(), Some(workspace));
    assert_eq!(
        placed.digest().to_string(),
        "21eb4f85d018b3a460457cb7953fd88b79cea573462acf8ab884ec474c0199de"
    );
    assert_ne!(placed.digest(), legacy.digest());
    assert_eq!(legacy.device_workspace_id(), None);
    assert_eq!(
        legacy.digest().to_string(),
        "12987937eb2a30ac9e56d004aa1ae2240a411e81692eba8f7b3f87fcecbae6a8"
    );
}
