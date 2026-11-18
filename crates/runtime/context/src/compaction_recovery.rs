//! Recovery of logical Compression work across distinct physical invocations.
//! A live owner never redispatches an ambiguous call. Replacement ownership can
//! select a new generation under the admission-sealed persistent policy.
use std::num::NonZeroU32;

use musteragent_content_ref::Sha256;
use musteragent_identity::{AttemptId, FencingToken, UtcTimestamp};
use musteragent_runtime_snapshot::RecoveryPolicySnapshot;

use crate::{ContextCompactionRequest, ContextPlanError};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompactionGenerationObservation {
    pub generation: NonZeroU32,
    /// Highest sealed generation across all nodes in this loop iteration.
    pub generation_high_watermark: NonZeroU32,
    pub invocation_id: musteragent_identity::InvocationId,
    pub correlation_id: musteragent_identity::CorrelationId,
    pub plan_fingerprint: Sha256,
    pub logical_plan_fingerprint: Option<Sha256>,
    pub sealed_ledger_cutoff: Option<musteragent_identity::ConversationSeq>,
    /// The primary request estimate at the original compaction decision. It
    /// is not the summary model's input or permission to skip current budgets.
    pub sealed_input_tokens_before: Option<u64>,
    pub owner_attempt_id: AttemptId,
    pub owner_fencing_token: FencingToken,
    pub timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
    pub state: CompactionGenerationState,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CompactionGenerationState {
    SealedNotDispatched,
    DispatchedUnknown,
    Succeeded,
    Failed(ContextPlanError),
    Cancelled,
}

/// Only the physical route and its output limit may change across candidates.
/// The Home's retained fingerprint authenticates every other source field.
#[derive(Debug, Clone)]
pub struct CompactionCandidatePredecessor {
    pub invocation_id: musteragent_identity::InvocationId,
    pub(crate) generation: NonZeroU32,
    pub(crate) model: musteragent_runtime_snapshot::PinnedModelRoute,
    /// Present only for a persistent recovery probe after all admitted candidates returned terminal failures.
    completed_round: Option<NonZeroU32>,
    max_output_tokens: std::num::NonZeroU64,
}

impl From<&ContextCompactionRequest> for CompactionCandidatePredecessor {
    fn from(request: &ContextCompactionRequest) -> Self {
        Self {
            invocation_id: request.invocation.invocation_id,
            generation: request.slot.generation,
            completed_round: None,
            model: request.model.clone(),
            max_output_tokens: request.max_output_tokens,
        }
    }
}

impl CompactionCandidatePredecessor {
    pub fn after_completed_round(mut self, highest_generation: NonZeroU32) -> Self {
        self.completed_round = Some(highest_generation);
        self
    }

    pub(crate) fn restore_route(
        &self,
        mut source: ContextCompactionRequest,
    ) -> Result<ContextCompactionRequest, ContextPlanError> {
        source.model = self.model.clone();
        source.max_output_tokens = self.max_output_tokens;
        source.capture_provider_failure = true;
        source.slot.plan_fingerprint = source.expected_plan_fingerprint()?;
        Ok(source)
    }
}

impl ContextCompactionRequest {
    /// Select a same-role physical route under the original source and fence. A
    /// confirmed predecessor is the only authority to append a new candidate.
    pub fn select_role_candidate(
        mut self,
        latest: Option<CompactionGenerationObservation>,
        predecessor: Option<CompactionCandidatePredecessor>,
        now: UtcTimestamp,
    ) -> Result<(Self, Option<ContextPlanError>), ContextPlanError> {
        self.validate()?;
        let Some(latest) = latest else {
            return if predecessor.is_none() {
                Ok((self, None))
            } else {
                Err(ContextPlanError::idempotency_conflict())
            };
        };
        if latest.generation_high_watermark < latest.generation {
            return Err(ContextPlanError::invariant());
        }
        if matches!(
            latest.state,
            CompactionGenerationState::DispatchedUnknown | CompactionGenerationState::Cancelled
        ) {
            return Err(ContextPlanError::indeterminate());
        }
        if let Some(probe) = predecessor.as_ref()
            && let Some(highest) = probe.completed_round
        {
            // Recovered terminal facts bind the same source, route and absolute Home cooldown.
            // A replacement Attempt alone cannot reissue a partial or ambiguous operation.
            if !matches!(
                self.recovery_policy,
                RecoveryPolicySnapshot::Persistent { .. }
            ) || highest != latest.generation_high_watermark
                || latest.invocation_id != probe.invocation_id
                || !self.matches_sealed_plan(&latest)?
            {
                return Err(ContextPlanError::idempotency_conflict());
            }
            let CompactionGenerationState::Failed(error) = &latest.state else {
                return Err(ContextPlanError::idempotency_conflict());
            };
            let same_owner = self.invocation.attempt_context.attempt_id == latest.owner_attempt_id
                && self.invocation.attempt_context.fencing_token == latest.owner_fencing_token;
            if !error.permits_role_fallback()
                || !error.retryable
                || error.retry_after_ms.is_some_and(|delay| delay > 0)
                || same_owner
            {
                return Err(error.clone());
            }
            return self
                .with_physical_generation(next_generation(highest)?)
                .map(|request| (request, None));
        }
        if self.matches_sealed_plan(&latest)? {
            let retained = match &latest.state {
                CompactionGenerationState::Succeeded => Some(None),
                CompactionGenerationState::Failed(error) if error.permits_role_fallback() => {
                    Some(Some(error.clone()))
                }
                _ => None,
            };
            if let Some(failure) = retained {
                // Retain each terminal physical identity across replacement Attempts, including
                // the successful standby under a historical bounded recovery policy.
                let mut original = self.with_physical_generation(latest.generation)?;
                original.invocation.invocation_id = latest.invocation_id;
                original.invocation.correlation_id = latest.correlation_id;
                original.slot.ledger_cutoff = latest
                    .sealed_ledger_cutoff
                    .unwrap_or(original.slot.ledger_cutoff);
                original.slot.input_tokens_before = latest
                    .sealed_input_tokens_before
                    .unwrap_or(original.slot.input_tokens_before);
                original.slot.plan_fingerprint = original.expected_plan_fingerprint()?;
                return Ok((original, failure));
            }
            return self
                .select_recovery_generation(Some(latest), now)
                .map(|request| (request, None));
        }
        let predecessor = predecessor.ok_or_else(ContextPlanError::idempotency_conflict)?;
        if predecessor.invocation_id != latest.invocation_id
            || latest.generation != latest.generation_high_watermark
            || !matches!(latest.state, CompactionGenerationState::Failed(ref error) if error.permits_role_fallback())
        {
            return Err(ContextPlanError::idempotency_conflict());
        }
        let new_model = std::mem::replace(&mut self.model, predecessor.model);
        let new_budget =
            std::mem::replace(&mut self.max_output_tokens, predecessor.max_output_tokens);
        if !self.matches_sealed_plan(&latest)? {
            return Err(ContextPlanError::idempotency_conflict());
        }
        self.model = new_model;
        self.max_output_tokens = new_budget;
        self.with_physical_generation(next_generation(latest.generation_high_watermark)?)
            .map(|request| (request, None))
    }

    /// Called after the persistence port validates the current Attempt fence.
    /// Selecting a generation does not grant dispatch authority: sealing and
    /// dispatch each validate the same fence in their own transaction.
    pub fn select_recovery_generation(
        self,
        latest: Option<CompactionGenerationObservation>,
        now: UtcTimestamp,
    ) -> Result<Self, ContextPlanError> {
        self.validate()?;
        let bounded = self.recovery_policy.terminal_limit().is_some();
        if bounded
            && !musteragent_runtime_snapshot::uses_provider_capacity_recovery(
                self.slot.compaction_policy_ref.as_str(),
            )
        {
            return Ok(self);
        }
        let Some(latest) = latest else {
            return Ok(self);
        };
        if latest.generation_high_watermark < latest.generation {
            return Err(ContextPlanError::invariant());
        }
        let activity = matches!(
            latest.timing,
            musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(
                _
            )
        );
        // Replacement ownership is not proof that the prior provider stopped.
        if activity
            && matches!(
                latest.state,
                CompactionGenerationState::DispatchedUnknown | CompactionGenerationState::Cancelled
            )
        {
            return Err(ContextPlanError::indeterminate());
        }
        let mut candidate = self.with_physical_generation(latest.generation)?;
        candidate.invocation.invocation_id = latest.invocation_id;
        candidate.invocation.correlation_id = latest.correlation_id;
        let same_plan = candidate.matches_sealed_plan(&latest)?;
        if same_plan {
            // Dynamic primary instructions can change the current estimate,
            // but are not summary input. Keep the exact original v1 slot and
            // Started event; the planner independently budgets its fresh request.
            candidate.slot.ledger_cutoff = latest
                .sealed_ledger_cutoff
                .unwrap_or(candidate.slot.ledger_cutoff);
            candidate.slot.input_tokens_before = latest
                .sealed_input_tokens_before
                .unwrap_or(candidate.slot.input_tokens_before);
            candidate.slot.plan_fingerprint = candidate.expected_plan_fingerprint()?;
        }
        let chunk_policy = musteragent_runtime_snapshot::uses_provider_capacity_recovery(
            candidate.slot.compaction_policy_ref.as_str(),
        );
        let next_chunk = chunk_policy
            && !same_plan
            && matches!(
                &latest.state,
                CompactionGenerationState::Succeeded
                    | CompactionGenerationState::Failed(ContextPlanError {
                        code: musteragent_runtime_contract::ErrorCode::ModelContextExceeded,
                        ..
                    })
            );
        if same_plan
            && (bounded
                || (chunk_policy
                    && matches!(
                        latest.state,
                        CompactionGenerationState::Failed(ContextPlanError {
                            code: musteragent_runtime_contract::ErrorCode::ModelContextExceeded,
                            ..
                        })
                    )))
        {
            // A retained capacity rejection is an exact node result. Replaying
            // it resumes the original fragment tree; it never dispatches that
            // physical invocation again or creates a new root Started event.
            return Ok(candidate);
        }
        if !next_chunk
            && let CompactionGenerationState::Failed(error) = &latest.state
            && (!error.retryable || error.retry_after_ms.is_some_and(|delay| delay > 0))
        {
            return Err(error.clone());
        }
        let replacement_owner = candidate.invocation.attempt_context.attempt_id
            != latest.owner_attempt_id
            || candidate.invocation.attempt_context.fencing_token != latest.owner_fencing_token;
        if !same_plan && !replacement_owner && !next_chunk {
            return Err(ContextPlanError::idempotency_conflict());
        }
        let needs_replacement = !same_plan
            || match latest.state {
                CompactionGenerationState::Succeeded => false,
                CompactionGenerationState::SealedNotDispatched => latest
                    .timing
                    .legacy_deadline()
                    .is_some_and(|deadline| deadline <= now),
                CompactionGenerationState::Failed(error) => error.retryable,
                CompactionGenerationState::DispatchedUnknown
                | CompactionGenerationState::Cancelled => true,
            };
        if !needs_replacement || (!replacement_owner && !next_chunk) {
            return Ok(candidate);
        }
        // The shared storage contract uses PostgreSQL INTEGER as its narrowest
        // generation domain. Never wrap an identity or overwrite prior facts.
        candidate.with_physical_generation(next_generation(latest.generation_high_watermark)?)
    }

    /// Semantic identity excludes recovery generation and the ledger's
    /// bookkeeping-only cutoff. Every source record, history byte, route,
    /// policy, prior summary and budget remains part of the hash.
    pub fn logical_plan_fingerprint(&self) -> Result<Sha256, ContextPlanError> {
        let fingerprint = crate::compaction_slot::compaction_plan_fingerprint_with_identity(
            self,
            NonZeroU32::MIN,
            self.slot.source_seq_to,
        )?;
        let mut encoded = b"musteragent.context-compaction-logical-plan.v1\0".to_vec();
        encoded.extend_from_slice(&fingerprint.into_bytes());
        Ok(Sha256::digest(encoded))
    }

    /// Proves that this exact summary operation is the original sealed node.
    /// Storage must first authenticate its Run/source and current Attempt. Only
    /// original observational metadata is restored; history, record sets, prior
    /// summary, route, credentials, policy and every actual budget remain exact.
    /// Old observations without an estimate retain their previous exact check.
    pub fn matches_sealed_plan(
        &self,
        original: &CompactionGenerationObservation,
    ) -> Result<bool, ContextPlanError> {
        let before = original
            .sealed_input_tokens_before
            .unwrap_or(self.slot.input_tokens_before);
        if before == 0 {
            return Err(ContextPlanError::invariant());
        }
        let cutoff = original
            .sealed_ledger_cutoff
            .unwrap_or(self.slot.ledger_cutoff);
        if let Some(logical) = original.logical_plan_fingerprint {
            if original.sealed_ledger_cutoff.is_none() {
                return Err(ContextPlanError::invariant());
            }
            let fingerprint = crate::compaction_slot::compaction_plan_fingerprint_with_observation(
                self,
                NonZeroU32::MIN,
                self.slot.source_seq_to,
                before,
            )?;
            let mut encoded = b"musteragent.context-compaction-logical-plan.v1\0".to_vec();
            encoded.extend_from_slice(&fingerprint.into_bytes());
            if Sha256::digest(encoded) != logical {
                return Ok(false);
            }
        }
        let complete = crate::compaction_slot::compaction_plan_fingerprint_with_observation(
            self,
            original.generation,
            cutoff,
            before,
        )?;
        if complete != original.plan_fingerprint {
            return if original.logical_plan_fingerprint.is_some() {
                Err(ContextPlanError::idempotency_conflict())
            } else {
                Ok(false)
            };
        }
        Ok(true)
    }

    fn with_physical_generation(
        mut self,
        generation: NonZeroU32,
    ) -> Result<Self, ContextPlanError> {
        let (invocation_id, correlation_id) = self.slot.source.generation_ids(
            self.invocation.run_context.run_id,
            self.slot.loop_iteration,
            generation,
        )?;
        self.slot.generation = generation;
        self.invocation.invocation_id = invocation_id;
        self.invocation.correlation_id = correlation_id;
        self.slot.plan_fingerprint = self.expected_plan_fingerprint()?;
        Ok(self)
    }
}

fn next_generation(high_watermark: NonZeroU32) -> Result<NonZeroU32, ContextPlanError> {
    high_watermark
        .get()
        .checked_add(1)
        .filter(|value| *value <= i32::MAX as u32)
        .and_then(NonZeroU32::new)
        .ok_or_else(ContextPlanError::quota)
}
