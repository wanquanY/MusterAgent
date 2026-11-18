use super::*;
use musteragent_identity::{PrincipalId, TenantId};
fn control() -> NativeDialogueExecutionControl {
    NativeDialogueExecutionControl {
        cancellation_id: CommandId::generate(),
        command_id: CommandId::generate(),
        observed_activity_revision: 1,
        created_at: UtcTimestamp::from_unix_millis(100),
        target: NativeDialogueExecutionControlTarget {
            actor: Actor {
                tenant_id: TenantId::generate(),
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            agent_id: AgentId::generate(),
            dialogue_id: AgentDialogueId::generate(),
            intent_id: UserIntentId::generate(),
            run_id: RunId::generate(),
            conversation_id: ConversationId::generate(),
            device_id: DeviceId::generate(),
            installation_id: DeviceInstallationId::generate(),
            source_digest: Sha256::digest(b"original custody"),
            grant_revision: 0,
            grant_proof_hash: None,
        },
    }
}
#[test]
fn empty_grant_custody_roundtrips_without_inventing_a_snapshot_or_admission() {
    let c = control();
    let b = c.canonical_bytes().expect("valid test fixture");
    assert_eq!(
        NativeDialogueExecutionControl::decode(&b).expect("valid test fixture"),
        c
    );
    let n = NativeDialogueExecutionNotAdmittedReceipt { control: c };
    let b = n.canonical_bytes().expect("valid test fixture");
    assert_eq!(
        NativeDialogueExecutionNotAdmittedReceipt::decode(&b).expect("valid test fixture"),
        n
    );
    for end in [0, 1, b.len() - 1] {
        assert!(NativeDialogueExecutionNotAdmittedReceipt::decode(&b[..end]).is_err());
    }
    let mut trailing = b;
    trailing.push(0);
    assert!(NativeDialogueExecutionNotAdmittedReceipt::decode(&trailing).is_err());
}
#[test]
fn grant_cut_size_is_constant_after_more_than_256_renewals() {
    let mut c = control();
    let old = Sha256::digest(b"old original local proof");
    c.target.grant_revision = 257;
    c.target.grant_proof_hash = Some(Sha256::digest(b"cut head"));
    let size = c.canonical_bytes().expect("valid test fixture").len();
    c.target.grant_revision = 100_000;
    assert_eq!(c.canonical_bytes().expect("valid test fixture").len(), size);
    let receipt = DeviceDeliveryBinding {
        work_item: None,
        intent_id: c.target.intent_id,
        run_id: c.target.run_id,
        device_id: c.target.device_id,
        installation_id: c.target.installation_id,
        source_digest: c.target.source_digest,
        snapshot_hash: Sha256::digest(b"snapshot"),
        grant_proof_hash: old,
    };
    assert!(
        c.target.matches_admission(&receipt),
        "scope matches; original proof owner must still authenticate its stored signature"
    );
    c.target.grant_revision = 0;
    assert!(c.validate().is_err());
    c.target.grant_proof_hash = None;
    assert!(c.validate().is_ok());
    assert!(!c.target.matches_admission(&receipt));
}
#[test]
fn control_pages_are_ordered_coverage_not_optimistic_acknowledgements() {
    let c = control();
    let at = NativeDialogueExecutionControlCursor {
        created_at: c.created_at,
        command_id: c.command_id,
    };
    let page = NativeDialogueExecutionControlPage {
        controls: vec![c.clone()],
        next_cursor: Some(at),
        scan_exhausted: false,
    };
    assert!(page.validate_after(None).is_ok());
    assert!(page.validate_after(Some(at)).is_err());
    let mut page = page;
    page.controls.push(c);
    assert!(page.validate_after(None).is_err());
    page.controls.clear();
    page.scan_exhausted = true;
    assert!(page.validate_after(None).is_err());
    page.next_cursor = None;
    assert!(page.validate_after(Some(at)).is_ok());
}
