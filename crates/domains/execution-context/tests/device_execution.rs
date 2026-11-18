#![cfg(test)]

use musteragent_content_ref::Sha256;
use musteragent_execution_context::{device_execution::*, *};
use musteragent_identity::*;
use musteragent_runtime_contract::{execution_assignment::*, execution_claim::*};

fn fixture() -> DeviceDelegationFacts {
    let id = "019b2f80-0000-7000-8000-000000000001";
    let run_id = id.parse().unwrap();
    let owner_id = id.parse().unwrap();
    let installation_id = id.parse().unwrap();
    DeviceDelegationFacts {
        source: DialogueExecutionFactBinding {
            product_release: None,
            device_workspace_id: None,
            actor: Actor {
                tenant_id: id.parse().unwrap(),
                principal_id: owner_id,
                principal_kind: PrincipalKind::User,
            },
            intent_id: id.parse().unwrap(),
            agent_id: id.parse().unwrap(),
            agent_version_id: id.parse().unwrap(),
            dialogue_id: id.parse().unwrap(),
            context_conversation_id: id.parse().unwrap(),
            command_id: id.parse().unwrap(),
            run_id,
            context_watermark: DialogueCursor { epoch: 1, seq: 3 },
            manifest_revision: 2,
            instructions_revision: 1,
            permission_revision: 1,
            permission_mode: 1,
            source_sha256: Sha256::digest(b"frozen-dialogue-source"),
            default_agent_direct_intent: true,
        },
        origin: DialogueInputOrigin {
            surface: DialogueInputSurface::Desktop,
            installation_id: Some(installation_id),
        },
        origin_login_session_id: id.parse().unwrap(),
        target_installation_id: "019b2f80-0000-7000-8000-000000000002".parse().unwrap(),
        attempt: ExecutionAttemptIdentity {
            assignment_id: id.parse().unwrap(),
            run_id,
            attempt_id: id.parse().unwrap(),
            run_attempt_epoch: RunAttemptEpoch::new(1).unwrap(),
        },
        executor: ExecutorBinding {
            device_id: id.parse().unwrap(),
            owner_principal_id: owner_id,
            instance_id: id.parse().unwrap(),
            runtime_generation: FencingToken::new(1).unwrap(),
            registration_revision: VersionNumber::new(1).unwrap(),
            environment_spec_digest: [2; 32],
        },
        qualification: DeviceDelegationQualification {
            account_auth_epoch: 1,
            applications_revision: 1,
            executor_device_revision: 1,
            consent_id: id.parse().unwrap(),
            consent_digest: [4; 32],
            target_login_session_id: id.parse().unwrap(),
            target_membership_digest: [3; 32],
        },
    }
}
fn binding(facts: DeviceDelegationFacts) -> DeviceDelegationBinding {
    DeviceDelegationBinding::from_facts(facts).unwrap()
}
fn request(binding: &DeviceDelegationBinding) -> ExecutionLeaseRequest<DeviceDelegationSubject> {
    ExecutionLeaseRequest {
        scope: binding.subject(),
        run_id: binding.facts().source.run_id,
        assignment_digest: binding.digest().into_bytes(),
        request_id: AuthorityRequestId::generate(),
        executor_run_authority_digest: [9; 32],
        operation: ExecutionLeaseOperation::Claim {
            expected_run_sequence: RunSeq::new(12).unwrap(),
        },
    }
}

#[test]
fn delegated_desktop_web_and_mobile_inputs_keep_the_original_source() {
    let original = binding(fixture());
    original.check_current(&original).unwrap();
    original.check_lease_request(&request(&original)).unwrap();
    for surface in [
        DialogueInputSurface::Desktop,
        DialogueInputSurface::Web,
        DialogueInputSurface::Mobile,
    ] {
        let mut facts = fixture();
        facts.origin.installation_id = match surface {
            DialogueInputSurface::Web => None,
            _ => Some(DeviceInstallationId::generate()),
        };
        facts.origin.surface = surface.clone();
        let remote = binding(facts);
        assert_eq!(remote.facts().origin.surface, surface);
        assert_eq!(remote.facts().source, original.facts().source);
        assert_ne!(remote.digest(), original.digest());
        assert!(original.check_lease_request(&request(&remote)).is_err());
    }
}

#[test]
fn caller_cannot_promote_foreign_owner_or_invalid_origin_into_a_device_binding() {
    let mut facts = fixture();
    facts.executor.owner_principal_id = PrincipalId::generate();
    assert_eq!(
        DeviceDelegationBinding::from_facts(facts),
        Err(DeviceDelegationError::ExecutorOwner)
    );
    for surface in [DialogueInputSurface::Desktop, DialogueInputSurface::Mobile] {
        let mut facts = fixture();
        facts.origin.surface = surface;
        facts.origin.installation_id = None;
        assert_eq!(
            DeviceDelegationBinding::from_facts(facts),
            Err(DeviceDelegationError::Origin)
        );
    }
}

#[test]
fn malformed_sources_and_missing_qualification_are_rejected() {
    let mutations: [fn(&mut DeviceDelegationFacts); 8] = [
        |v| v.source.actor.principal_kind = PrincipalKind::Service,
        |v| v.source.run_id = RunId::generate(),
        |v| v.source.context_watermark.epoch = 0,
        |v| v.source.context_watermark.seq = 0,
        |v| v.source.manifest_revision = 0,
        |v| v.source.instructions_revision = u64::MAX,
        |v| v.source.permission_revision = 0,
        |v| v.source.source_sha256 = Sha256::from_bytes([0; 32]).unwrap(),
    ];
    for mutate in mutations {
        let mut facts = fixture();
        mutate(&mut facts);
        assert_eq!(
            DeviceDelegationBinding::from_facts(facts),
            Err(DeviceDelegationError::Source)
        );
    }
    let mutations: [fn(&mut DeviceDelegationFacts); 5] = [
        |v| v.qualification.account_auth_epoch = 0,
        |v| v.qualification.applications_revision = u64::MAX,
        |v| v.qualification.executor_device_revision = 0,
        |v| v.qualification.target_membership_digest = [0; 32],
        |v| v.qualification.consent_digest = [0; 32],
    ];
    for mutate in mutations {
        let mut facts = fixture();
        mutate(&mut facts);
        assert_eq!(
            DeviceDelegationBinding::from_facts(facts),
            Err(DeviceDelegationError::Qualification)
        );
    }
}

#[test]
fn every_scope_input_target_and_authority_pin_is_committed_and_rechecked() {
    let original = binding(fixture());
    let mutations: &[fn(&mut DeviceDelegationFacts)] = &[
        |v| v.source.actor.tenant_id = TenantId::generate(),
        |v| {
            v.source.actor.principal_id = PrincipalId::generate();
            v.executor.owner_principal_id = v.source.actor.principal_id;
        },
        |v| v.source.agent_id = AgentId::generate(),
        |v| v.source.agent_version_id = AgentVersionId::generate(),
        |v| v.source.dialogue_id = AgentDialogueId::generate(),
        |v| v.source.intent_id = UserIntentId::generate(),
        |v| v.source.context_conversation_id = ConversationId::generate(),
        |v| v.source.command_id = CommandId::generate(),
        |v| {
            v.source.run_id = RunId::generate();
            v.attempt.run_id = v.source.run_id;
        },
        |v| v.source.context_watermark.epoch += 1,
        |v| v.source.context_watermark.seq += 1,
        |v| v.source.manifest_revision += 1,
        |v| v.source.instructions_revision += 1,
        |v| v.source.permission_revision += 1,
        |v| v.source.permission_mode = 4,
        |v| v.source.default_agent_direct_intent = false,
        |v| v.source.source_sha256 = Sha256::digest(b"other source material"),
        |v| v.origin_login_session_id = LoginSessionId::generate(),
        |v| v.target_installation_id = DeviceInstallationId::generate(),
        |v| v.attempt.assignment_id = ExecutionAssignmentId::generate(),
        |v| v.attempt.attempt_id = AttemptId::generate(),
        |v| v.attempt.run_attempt_epoch = RunAttemptEpoch::new(2).unwrap(),
        |v| v.executor.device_id = DeviceId::generate(),
        |v| v.executor.instance_id = ExecutorInstanceId::generate(),
        |v| v.executor.runtime_generation = FencingToken::new(2).unwrap(),
        |v| v.executor.registration_revision = VersionNumber::new(2).unwrap(),
        |v| v.executor.environment_spec_digest = [4; 32],
        |v| v.qualification.account_auth_epoch += 1,
        |v| v.qualification.applications_revision += 1,
        |v| v.qualification.executor_device_revision += 1,
        |v| v.qualification.target_login_session_id = LoginSessionId::generate(),
        |v| v.qualification.target_membership_digest = [4; 32],
        |v| v.qualification.consent_id = ExecutorConsentId::generate(),
        |v| v.qualification.consent_digest = [5; 32],
        |v| v.origin.installation_id = Some(DeviceInstallationId::generate()),
        |v| v.origin.surface = DialogueInputSurface::Mobile,
    ];
    for (index, mutate) in mutations.iter().enumerate() {
        let mut changed = fixture();
        mutate(&mut changed);
        let changed = binding(changed);
        assert_ne!(
            original.digest(),
            changed.digest(),
            "uncommitted field {index}"
        );
        assert!(
            original.check_current(&changed).is_err(),
            "stale field {index}"
        );
        assert!(
            original.check_lease_request(&request(&changed)).is_err(),
            "foreign claim {index}"
        );
    }
}

#[test]
fn request_scope_and_run_are_checked_even_when_caller_reuses_the_real_digest() {
    let binding = binding(fixture());
    let mutations: [fn(&mut ExecutionLeaseRequest<DeviceDelegationSubject>); 7] = [
        |r| r.scope.tenant_id = TenantId::generate(),
        |r| r.scope.owner_id = PrincipalId::generate(),
        |r| r.scope.agent_id = AgentId::generate(),
        |r| r.scope.dialogue_id = AgentDialogueId::generate(),
        |r| r.scope.intent_id = UserIntentId::generate(),
        |r| r.run_id = RunId::generate(),
        |r| r.executor_run_authority_digest = [0; 32],
    ];
    for mutate in mutations {
        let mut request = request(&binding);
        mutate(&mut request);
        assert_eq!(
            binding.check_lease_request(&request),
            Err(DeviceDelegationError::LeaseBinding)
        );
    }
}

#[test]
fn recovery_needs_new_allocation_and_cannot_change_source_or_resurrect_the_old_worker() {
    let original = binding(fixture());
    let mut next = fixture();
    next.attempt.assignment_id = ExecutionAssignmentId::generate();
    next.attempt.attempt_id = AttemptId::generate();
    next.attempt.run_attempt_epoch = RunAttemptEpoch::new(2).unwrap();
    next.executor.instance_id = ExecutorInstanceId::generate();
    next.executor.runtime_generation = FencingToken::new(2).unwrap();
    let successor = binding(next.clone());
    successor.check_successor(&original).unwrap();
    assert!(original.check_current(&successor).is_err());
    assert!(original.check_successor(&original).is_err());
    let worker = ExecutionWorkerBinding {
        assignment_digest: original.digest().into_bytes(),
        attempt_id: original.facts().attempt.attempt_id,
        attempt_number: AttemptNumber::new(1).unwrap(),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(1).unwrap(),
    };
    worker.check_initial(&original).unwrap();
    assert_eq!(
        worker.check_assignment(&successor),
        Err(ExecutionWorkerBindingError::Assignment)
    );
    let continuation = ExecutionWorkerBinding {
        attempt_id: AttemptId::generate(),
        attempt_number: AttemptNumber::new(2).unwrap(),
        ..worker
    };
    continuation.check_assignment(&original).unwrap();
    assert_eq!(
        continuation.check_initial(&original),
        Err(ExecutionWorkerBindingError::InitialAttempt)
    );
    next.source.source_sha256 = Sha256::digest(b"new task");
    assert_eq!(
        binding(next).check_successor(&original),
        Err(DeviceDelegationError::RecoveryInputChanged)
    );
}

#[test]
fn canonical_dialogue_binding_has_a_stable_domain_separated_commitment() {
    let binding = binding(fixture());
    assert!(
        binding
            .canonical_bytes()
            .starts_with(b"musteragent.device-delegation.v1\0")
    );
    assert_eq!(binding.digest(), Sha256::digest(binding.canonical_bytes()));
    // Independent Python fixed-width UUID/u64 framing vector; keep stable for v1.
    assert_eq!(
        binding.digest().to_string(),
        "b925432a4a30d32587ca22b4b2fbd2301f4ca7c06908b10479895274cb9edbb3"
    );
}
