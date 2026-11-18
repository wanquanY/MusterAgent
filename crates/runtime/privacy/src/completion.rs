//! A request to attest completion, not an already completed fact. The store
//! owns the completion instant and builds proof only in its final transaction.
use crate::*;
use musteragent_content_ref::Sha256;
use musteragent_identity::{ErasureJobId, ErasureReceiptId, TenantId, UtcTimestamp};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CompleteErasureExecution {
    pub tenant_id: TenantId,
    pub erasure_job_id: ErasureJobId,
    pub request_fingerprint: Sha256,
    pub plan_hash: Sha256,
    pub expected_sequence: ErasureTransitionSequence,
    pub started_at: UtcTimestamp,
}
impl CompleteErasureExecution {
    pub fn from_job(
        job: &ErasureJob,
        started_at: UtcTimestamp,
    ) -> Result<Self, ErasureContractError> {
        job.validate()?;
        let last = job
            .transitions
            .last()
            .ok_or(ErasureContractError::MissingInitialTransition)?;
        if last.status != ErasureJobStatus::Executing || started_at < job.request.requested_at {
            return Err(ErasureContractError::InvalidTransition);
        }
        Ok(Self {
            tenant_id: job.request.tenant_id,
            erasure_job_id: job.request.erasure_job_id,
            request_fingerprint: job.request_fingerprint,
            plan_hash: job.latest_plan()?.plan_hash,
            expected_sequence: last.sequence,
            started_at,
        })
    }
    /// A matching terminal aggregate is a durable replay, regardless of the
    /// current wall clock or now-expired retention exception.
    pub fn is_replay(&self, job: &ErasureJob) -> Result<bool, ErasureContractError> {
        job.validate()?;
        if self.started_at < job.request.requested_at {
            return Err(ErasureContractError::InvalidTransition);
        }
        if self.tenant_id != job.request.tenant_id
            || self.erasure_job_id != job.request.erasure_job_id
            || self.request_fingerprint != job.request_fingerprint
            || self.plan_hash != job.latest_plan()?.plan_hash
        {
            return Err(ErasureContractError::PlanLineageMismatch);
        }
        let last = job
            .transitions
            .last()
            .ok_or(ErasureContractError::MissingInitialTransition)?;
        if job.status().is_some_and(ErasureJobStatus::is_terminal) {
            if last.sequence.get()
                != self
                    .expected_sequence
                    .get()
                    .checked_add(1)
                    .ok_or(ErasureContractError::InvalidTransitionSequence)?
            {
                return Err(ErasureContractError::InvalidTransitionSequence);
            }
            return Ok(true);
        }
        if last.sequence != self.expected_sequence || last.status != ErasureJobStatus::Executing {
            return Err(ErasureContractError::InvalidTransition);
        }
        Ok(false)
    }
    /// `completed_at` must come from the persistence owner's transaction clock
    /// after physical provider work; never from this command or a provider.
    pub fn build(
        &self,
        job: &ErasureJob,
        completed_at: UtcTimestamp,
    ) -> Result<(ErasureTransition, ErasureReceipt), ErasureContractError> {
        if self.is_replay(job)? {
            return Err(ErasureContractError::InvalidTransition);
        }
        let plan = job.latest_plan()?;
        let last = job
            .transitions
            .last()
            .ok_or(ErasureContractError::MissingInitialTransition)?;
        if completed_at < self.started_at || completed_at < last.occurred_at {
            return Err(ErasureContractError::InvalidTransition);
        }
        let exceptions = plan
            .retention_exceptions
            .iter()
            .map(|e| e.data_class)
            .collect::<std::collections::BTreeSet<_>>();
        let retained = !exceptions.is_empty();
        let receipt = ErasureReceipt {
            schema_version: plan.schema_version.clone(),
            erasure_receipt_id: ErasureReceiptId::generate(),
            erasure_job_id: self.erasure_job_id,
            tenant_id: self.tenant_id,
            outcome: if retained {
                ErasureReceiptOutcome::ErasedWithRetentionException
            } else {
                ErasureReceiptOutcome::FullyErased
            },
            plan_hash: plan.plan_hash,
            completed_data_classes: plan
                .data_classes
                .iter()
                .filter(|c| !exceptions.contains(c))
                .copied()
                .collect(),
            retention_exceptions: plan.retention_exceptions.clone(),
            proof_digest: erasure_proof_digest(job, completed_at),
            completed_at,
        };
        receipt.validate_against(self.tenant_id, plan)?;
        Ok((
            ErasureTransition {
                sequence: ErasureTransitionSequence::new(
                    self.expected_sequence
                        .get()
                        .checked_add(1)
                        .ok_or(ErasureContractError::InvalidTransitionSequence)?,
                )?,
                status: if retained {
                    ErasureJobStatus::CompletedWithRetentionException
                } else {
                    ErasureJobStatus::Completed
                },
                occurred_at: completed_at,
            },
            receipt,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::{ContentRef, MediaType};
    use musteragent_identity::{
        Actor, ContentId, DottedKey, IdempotencyKey, PrincipalId, PrincipalKind,
    };
    use musteragent_runtime_contract::{DataRetentionPolicyRef, ResourceRef};
    fn time(n: i64) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(n)
    }
    fn job() -> ErasureJob {
        let tenant_id = TenantId::generate();
        let request = ErasureRequest {
            schema_version: DottedKey::new("musteragent.legacy.privacy.erasure.v1").unwrap(),
            tenant_id,
            erasure_job_id: ErasureJobId::generate(),
            requested_by: Actor {
                tenant_id,
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            root_resource_refs: vec![ResourceRef::Content(ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain").unwrap(),
                byte_length: 1,
                sha256: Sha256::digest(b"x"),
            })],
            retention_policy_ref: DataRetentionPolicyRef::new("privacy.standard@1").unwrap(),
            idempotency_key: IdempotencyKey::new("completion:test").unwrap(),
            requested_at: time(10),
            complete_by: time(100),
        };
        let plan = ErasurePlanSnapshot::new(
            request.schema_version.clone(),
            request.erasure_job_id,
            ErasurePlanVersion::new(1).unwrap(),
            request.root_resource_refs.clone(),
            vec![
                ErasureDataClass::ContentCiphertext,
                ErasureDataClass::Backup,
            ],
            vec![ErasureRetentionException {
                data_class: ErasureDataClass::Backup,
                policy_ref: DataRetentionPolicyRef::new("backup.expiry@1").unwrap(),
                owner_principal_id: request.requested_by.principal_id,
                expires_at: time(200),
            }],
            request.fingerprint().unwrap(),
            time(20),
        )
        .unwrap();
        let transitions = [
            (ErasureJobStatus::PendingPlan, 10),
            (ErasureJobStatus::Planned, 20),
            (ErasureJobStatus::Fenced, 21),
            (ErasureJobStatus::Executing, 30),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, (status, t))| ErasureTransition {
            sequence: ErasureTransitionSequence::new(i as u64 + 1).unwrap(),
            status,
            occurred_at: time(t),
        })
        .collect();
        let job = ErasureJob {
            request_fingerprint: request.fingerprint().unwrap(),
            request,
            plans: vec![plan],
            transitions,
            receipt: None,
        };
        job.validate().unwrap();
        job
    }
    #[test]
    fn delayed_completion_proof_uses_observed_finish_not_execution_start() {
        let j = job();
        let c = CompleteErasureExecution::from_job(&j, time(29)).unwrap();
        let (t, r) = c.build(&j, time(75)).unwrap();
        assert_eq!(t.occurred_at, time(75));
        assert_eq!(r.completed_at, time(75));
        assert_eq!(r.proof_digest, erasure_proof_digest(&j, time(75)));
        assert_ne!(r.proof_digest, erasure_proof_digest(&j, time(29)));
        assert_eq!(r.retention_exceptions, j.plans[0].retention_exceptions);
    }
    #[test]
    fn completion_rejects_backwards_clock_and_expired_retention() {
        let j = job();
        let c = CompleteErasureExecution::from_job(&j, time(35)).unwrap();
        assert!(c.build(&j, time(34)).is_err());
        assert!(c.build(&j, time(200)).is_err());
        assert!(c.build(&j, time(29)).is_err());
    }
    #[test]
    fn completion_is_bound_to_the_original_actor_request_plan_and_sequence() {
        let j = job();
        let c = CompleteErasureExecution::from_job(&j, time(30)).unwrap();
        for changed in [
            CompleteErasureExecution {
                tenant_id: TenantId::generate(),
                ..c
            },
            CompleteErasureExecution {
                erasure_job_id: ErasureJobId::generate(),
                ..c
            },
            CompleteErasureExecution {
                request_fingerprint: Sha256::digest(b"other"),
                ..c
            },
            CompleteErasureExecution {
                plan_hash: Sha256::digest(b"other"),
                ..c
            },
            CompleteErasureExecution {
                expected_sequence: ErasureTransitionSequence::new(2).unwrap(),
                ..c
            },
            CompleteErasureExecution {
                started_at: time(9),
                ..c
            },
        ] {
            assert!(changed.build(&j, time(40)).is_err());
        }
    }
    #[test]
    fn exact_completed_fact_replays_without_replacing_clock_or_expired_exception() {
        let mut j = job();
        let c = CompleteErasureExecution::from_job(&j, time(30)).unwrap();
        let (t, r) = c.build(&j, time(40)).unwrap();
        j.transitions.push(t);
        j.receipt = Some(r.clone());
        assert!(c.is_replay(&j).unwrap());
        assert!(c.build(&j, time(300)).is_err());
        assert_eq!(j.receipt, Some(r));
        assert!(
            CompleteErasureExecution {
                expected_sequence: ErasureTransitionSequence::new(3).unwrap(),
                ..c
            }
            .is_replay(&j)
            .is_err()
        );
    }
}
