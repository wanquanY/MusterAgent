use super::*;
use musteragent_event_envelope::{HostedHomeBinding, HostedProjectScope};
use musteragent_identity::{HomeBindingId, ProjectId, WorkspaceId};

#[test]
fn current_hosted_authority_pins_home_executor_and_dispatch_together() {
    let identity = FixtureIdentity::new();
    let home = DeviceId::generate();
    let scope = HostedProjectScope {
        project_id: ProjectId::generate(),
        workspace_id: WorkspaceId::generate(),
        resource_tenant_id: identity.tenant_id,
        owner_principal_id: PrincipalId::generate(),
        home: HostedHomeBinding {
            binding_id: HomeBindingId::generate(),
            device_id: home,
            home_epoch: HomeEpoch::new(1).expect("home epoch"),
        },
    };
    for assigned in [false, true] {
        let executor = if assigned { DeviceId::generate() } else { home };
        let mut batch = admission_batch(
            identity,
            RuntimeHome::Device { device_id: home },
            RuntimeHome::Device {
                device_id: executor,
            },
            ExecutionScope::HostedProject { scope },
        );
        let grant = &mut batch.runtime_snapshot.execution_grant;
        grant.audience = DottedKey::new(format!("musteragent.runtime.device.id-{executor}"))
            .expect("executor audience");
        grant.permission_mode = Some(PermissionMode::Supervised);
        grant.identity_binding = Some(ExecutionGrantIdentityBinding {
            principal_id: identity.principal_id,
            authority_scope: if assigned {
                ExecutionGrantAuthorityScope::HostedProjectExecution {
                    scope,
                    assignment_digest: [19; 32],
                }
            } else {
                ExecutionGrantAuthorityScope::HostedProject { scope }
            },
            device_id: Some(executor),
            device_workspace_id: Some(scope.workspace_id),
        });
        grant.validate().expect("current hosted grant");
        batch.append.outbox[0].topic =
            DottedKey::new(musteragent_ledger::run_dispatch_topic(grant)).expect("dispatch topic");
        let signed = SignedExecutionGrant {
            context_transport_annex: vec![],
            key_id: DottedKey::new("fixture.grant-key.v1").expect("key"),
            claims: grant.clone(),
            signature: [7; 64],
        };
        batch.runtime_snapshot.execution_grant_proof = Some(
            EncodedExecutionGrantProof::seal(
                DottedKey::new(EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1).expect("proof schema"),
                b"signed current hosted fixture".to_vec(),
                &signed,
            )
            .expect("matching proof"),
        );
        assert_eq!(batch.validate(), Ok(()), "assigned={assigned}");
        let mut wrong_home = batch.clone();
        wrong_home.run.context.control_home = RuntimeHome::Device {
            device_id: DeviceId::generate(),
        };
        assert!(wrong_home.validate().is_err());
        let mut wrong_executor = batch.clone();
        wrong_executor.run.context.execution_home = RuntimeHome::Device {
            device_id: DeviceId::generate(),
        };
        assert!(wrong_executor.validate().is_err());
        let mut wrong_topic = batch;
        wrong_topic.append.outbox[0].topic = DottedKey::new(if assigned {
            "run.dispatch"
        } else {
            "run.execution.dispatch"
        })
        .expect("wrong topic");
        assert!(wrong_topic.validate().is_err());
    }
}
