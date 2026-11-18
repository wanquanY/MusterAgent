//! Identical before-output faults against the original fenced journal in either store.
use musteragent_loop::{
    AgentLoopJournal, ModelFailure, ModelFailureCommit, ModelFailureResolution,
    ModelInvocationObservation, ModelIteration,
};
use musteragent_run::AttemptContext;
use musteragent_runtime_model::{ProviderErrorCode, ProviderRecoveryCapability};
use musteragent_runtime_snapshot::RecoveryPolicySnapshot;

pub fn seal(
    mut snapshot: musteragent_runtime_snapshot::RuntimeSnapshotContent,
) -> musteragent_runtime_snapshot::RuntimeSnapshotContent {
    let (primary, backup) = routes(&snapshot.model.route);
    snapshot.model.route = primary;
    snapshot.model.credential_grant_ref =
        musteragent_model::CredentialGrantRef::new("grant_round_primary")
            .expect("seal must succeed");
    snapshot.model.fallback_candidates = vec![musteragent_runtime_snapshot::PinnedModelRoute {
        route: backup,
        credential_grant_ref: musteragent_model::CredentialGrantRef::new("grant_round_backup")
            .expect("seal must succeed"),
    }];
    snapshot.execution_policy.provider_failover_v1 = true;
    snapshot.execution_policy.recovery_policy = RecoveryPolicySnapshot::production_default();
    snapshot
        .validate_sealed()
        .expect("two pinned candidates in the original sealed model policy");
    snapshot
}

fn routes(
    first: &musteragent_model::ModelRoute,
) -> (musteragent_model::ModelRoute, musteragent_model::ModelRoute) {
    let mut backup = first.clone();
    backup.route_ref = musteragent_identity::VersionedKey::new("model.round.backup@1")
        .expect("routes must succeed");
    backup.model_entry_id = "019fa6de-38fd-7000-8000-000000000463"
        .parse()
        .expect("routes must succeed");
    backup.fallback_route_refs.clear();
    let mut primary = first.clone();
    primary.fallback_route_refs = vec![backup.route_ref.clone()];
    (primary, backup)
}

pub async fn exercise(
    pool: &sqlx::PgPool,
    journal: &dyn AgentLoopJournal,
    attempt: &AttemptContext,
    mut iterations: [ModelIteration; 2],
    content: &dyn musteragent_runtime_content::ContentStore,
    permanent: bool,
) {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let (first, last) = routes(&iterations[0].request.route);
    for ((i, route), credential) in iterations
        .iter_mut()
        .zip([first, last])
        .zip(["grant_round_primary", "grant_round_backup"])
    {
        i.request.route = route;
        i.request.credential_grant_ref =
            musteragent_model::CredentialGrantRef::new(credential).expect("exercise must succeed");
        let bytes = musteragent_context::ContextCodec::encode_model_request(
            &musteragent_wire::CanonicalJsonContextCodec,
            &i.request,
        )
        .expect("exercise must succeed");
        i.request_ref = content
            .put(musteragent_runtime_content::PutContent {
                tenant_id: i.request.invocation.run_context.tenant_id,
                media_type: musteragent_content_ref::MediaType::new(
                    musteragent_runtime_model::MODEL_REQUEST_MEDIA_TYPE,
                )
                .expect("exercise must succeed"),
                bytes,
            })
            .await
            .expect("exercise must succeed");
    }
    journal
        .load_checkpoint(attempt)
        .await
        .expect("initialize the original attempt checkpoint");
    let primary_error = if permanent {
        ProviderErrorCode::AuthenticationFailed
    } else {
        ProviderErrorCode::RateLimited
    };
    for (index, (iteration, cause)) in iterations
        .into_iter()
        .zip([primary_error, ProviderErrorCode::PaymentRequired])
        .enumerate()
    {
        journal
            .begin_model(
                attempt,
                &iteration,
                ProviderRecoveryCapability::NonResumable,
            )
            .await
            .unwrap_or_else(|error| panic!("begin sealed route {index}: {error:?}"));
        journal
            .mark_model_dispatched(attempt, iteration.invocation_id)
            .await
            .expect("dispatch");
        musteragent_conformance::fixtures::model_journal::confirm_model_stop(
            pool,
            attempt,
            iteration.invocation_id,
        )
        .await;
        let budget = journal
            .recovery_budget(attempt)
            .await
            .expect("persisted facts");
        let target = (!permanent).then_some(0);
        if index == 1 {
            assert_eq!(
                ModelFailureResolution::retry_ordinal(cause, 1, budget),
                target
            );
        }
        let resolution = if index == 0 {
            ModelFailureResolution::ProviderFallbackV1 {
                next_route_ordinal: 1,
                cause,
            }
        } else {
            ModelFailureResolution::ExhaustedProviderRoundV1 {
                cause,
                retry_route_ordinal: target,
            }
        };
        let commit = ModelFailureCommit {
            recovery_policy: RecoveryPolicySnapshot::production_default(),
            observation: ModelInvocationObservation {
                invocation_id: iteration.invocation_id,
                provider_response_ref: None,
                response_model_key: None,
                usage: None,
                public_output_observed: false,
                replay_barrier_observed: false,
            },
            provider_failure: ModelFailure {
                code: cause.runtime_code(),
                retryable: cause.permits_dependency_recovery(),
            },
            retry_after_ms: None,
            resolution,
        };
        if index == 0 {
            for forged in [
                ModelFailureCommit {
                    resolution: ModelFailureResolution::ExhaustedProviderRoundV1 {
                        cause,
                        retry_route_ordinal: target,
                    },
                    ..commit.clone()
                },
                ModelFailureCommit {
                    resolution: ModelFailureResolution::ProviderFallbackV1 {
                        cause,
                        next_route_ordinal: 2,
                    },
                    ..commit.clone()
                },
                ModelFailureCommit {
                    recovery_policy: std::num::NonZeroU32::MIN.into(),
                    ..commit.clone()
                },
            ] {
                assert!(
                    journal.finish_failed_model(attempt, forged).await.is_err(),
                    "reject an unfrozen transition before touching the original invocation"
                );
            }
        }
        let checkpoint = journal
            .finish_failed_model(attempt, commit)
            .await
            .expect("atomically release or move within original run");
        assert_eq!(checkpoint.run_id, attempt.run_id);
        assert_eq!(checkpoint.phase, musteragent_loop::LoopPhase::ReadyForModel);
        assert_eq!(
            checkpoint.active_model_route_ordinal,
            if index == 0 { 1 } else { 0 }
        );
    }
}

#[path = "run_repair.rs"]
mod run_repair;

pub async fn resume_after_repair(
    store: &dyn musteragent_runtime_session::RuntimeApplicationStore,
    dispatch: &dyn musteragent_scheduler::DispatchStore,
    attempt: &AttemptContext,
    actor: musteragent_identity::Actor,
    participant: musteragent_identity::ParticipantId,
) {
    use musteragent_identity::{CorrelationId, HomeEpoch};
    use musteragent_scheduler::ClaimOutcome;
    let correlation = CorrelationId::generate();
    let authority = store
        .authorize_run_control(
            actor,
            attempt.run_id,
            participant,
            HomeEpoch::new(1).expect("resume after repair must succeed"),
            correlation,
        )
        .await
        .expect("resume after repair must succeed");
    let commit = run_repair::resume_commit(
        authority,
        actor,
        attempt.attempt_number,
        musteragent_run::RunRecoveryAction::RetryDependency,
    );
    store
        .resume_run(correlation, commit)
        .await
        .expect("resume after repair must succeed");
    let next = dispatch
        .claim_one(&attempt.worker_id, std::time::Duration::from_secs(60))
        .await
        .expect("resume after repair must succeed");
    let ClaimOutcome::Claimed(next) = next else {
        panic!("the original repaired run must be claimable");
    };
    assert_eq!(next.fence.run_id, attempt.run_id);
    assert_eq!(
        next.fence.attempt_number.get(),
        attempt.attempt_number.get() + 1
    );
}
