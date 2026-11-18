use super::*;

impl DurableContextPlanner {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn generate_summary(
        &self,
        input: &ContextPlanInput,
        material: &ContextMaterial,
        source: crate::CompactionSource,
        model: &PinnedModelRoute,
        compacted: &[&ContextEntry],
        retained: &[&ContextEntry],
        previous: Option<&ContextSummaryDocument>,
        uncompressed_tokens: u64,
        input_budget: u64,
        summary_budget: u64,
    ) -> Result<SummaryGeneration, ContextPlanError> {
        let mut delta = compacted
            .iter()
            .map(|entry| (*entry).clone())
            .collect::<Vec<_>>();
        if delta.is_empty()
            && previous.is_none_or(|summary| {
                summary.summary.is_none() || summary.summarized_record_ids.is_empty()
            })
        {
            return Err(ContextPlanError::invariant());
        }
        let mut summarized_record_ids = previous
            .map(|summary| summary.summarized_record_ids.clone())
            .unwrap_or_default();
        summarized_record_ids.extend(
            compacted
                .iter()
                .flat_map(|entry| entry.source_record_ids.iter().copied()),
        );
        if summarized_record_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != summarized_record_ids.len()
        {
            return Err(ContextPlanError::invariant());
        }
        let max_output_tokens = model_capacity::compression_output_budget(
            material
                .runtime_snapshot
                .context_policy
                .compaction_policy_ref
                .as_str(),
            &model.route,
            summary_budget,
        )?;
        let previous_summary = previous
            .and_then(|summary| summary.summary.as_deref())
            .map(ModelText::new)
            .transpose()
            .map_err(|_| ContextPlanError::invariant())?;
        let causation_id = compacted
            .last()
            .and_then(|entry| entry.source_record_ids.last())
            .or_else(|| previous.and_then(|summary| summary.summarized_record_ids.last()))
            .copied()
            .ok_or_else(ContextPlanError::invariant)?;
        // A completed step of the current task may start at a tool exchange.
        // Give the summarizer its original assignment so it can identify what
        // the tool accomplished. It stays verbatim in the primary request and
        // is not added to the checkpoint's covered record IDs.
        if musteragent_runtime_snapshot::uses_native_history(
            material
                .runtime_snapshot
                .context_policy
                .compaction_policy_ref
                .as_str(),
        ) && compacted.iter().any(|entry| {
            entry.history_page.is_none() && entry.source_run_id == Some(input.attempt.run_id)
        }) {
            let mut working: Vec<_> = retained
                .iter()
                .copied()
                .filter(|entry| {
                    entry.message.role == musteragent_runtime_model::CanonicalRole::User
                        && entry.runtime_events.is_empty()
                        && authority::protected(material, entry)
                })
                .cloned()
                .collect();
            working.append(&mut delta);
            delta = working;
        }
        crate::model_tool_views::project_compaction_names(&mut delta, material)?;
        let serialized_history = self.codec.encode_compaction_history(&delta)?;
        let retained_record_ids = retained
            .iter()
            .flat_map(|entry| entry.source_record_ids.iter().copied())
            .collect::<Vec<_>>();
        let generation = CONTEXT_PLAN_GENERATION;
        let previous_summary_ref = previous.and_then(|_| {
            material
                .previous_context
                .as_ref()
                .map(|context| context.summary.content_ref.clone())
        });
        let stream_budget = material.runtime_snapshot.execution_policy.model_stream;
        let compaction_policy_ref = material
            .runtime_snapshot
            .context_policy
            .compaction_policy_ref
            .clone();
        let (source_seq_from, source_seq_to) =
            super::summary_projection::coverage_range(compacted, previous)?;
        let plan_fingerprint = compaction_plan_fingerprint(&CompactionPlanFingerprintInput {
            source: &source,
            run_context: &material.run_context,
            loop_iteration: input.next_iteration,
            generation,
            active_branch_id: material.active_branch_id,
            ledger_cutoff: material.ledger_cutoff,
            source_seq_from,
            source_seq_to,
            summarized_record_ids: &summarized_record_ids,
            serialized_history: &serialized_history,
            retained_record_ids: &retained_record_ids,
            previous_summary_ref: previous_summary_ref.as_ref(),
            model,
            stream_budget,
            input_budget,
            input_tokens_before: uncompressed_tokens,
            summary_budget,
            max_output_tokens,
            compaction_policy_ref: &compaction_policy_ref,
            causation_id,
        })?;
        let (compaction_invocation_id, compaction_correlation_id) = source.generation_ids(
            material.run_context.run_id,
            input.next_iteration,
            generation,
        )?;
        let request = ContextCompactionRequest {
            capture_provider_failure: false,
            continuation: None,
            recovery_policy: material.runtime_snapshot.execution_policy.recovery_policy,
            slot: ContextCompactionSlotV1 {
                source: source.clone(),
                loop_iteration: input.next_iteration,
                generation,
                active_branch_id: material.active_branch_id,
                ledger_cutoff: material.ledger_cutoff,
                source_seq_from,
                source_seq_to,
                retained_record_ids,
                previous_summary_ref,
                input_budget,
                input_tokens_before: uncompressed_tokens,
                summary_budget,
                compaction_policy_ref,
                plan_fingerprint,
            },
            invocation: InvocationContext {
                invocation_id: compaction_invocation_id,
                run_context: material.run_context,
                attempt_context: input.attempt.clone(),
                parent_invocation_id: None,
                causation_id,
                correlation_id: compaction_correlation_id,
                timing: input.invocation_timing.clone(),
            },
            model: model.clone(),
            stream_budget,
            serialized_history,
            previous_summary,
            summarized_record_ids,
            max_output_tokens,
        };
        let candidates: Vec<_> = material
            .runtime_snapshot
            .auxiliary_models
            .iter()
            .filter(|candidate| {
                candidate.role == musteragent_model::AuxiliaryModelRole::Compression
            })
            .map(|candidate| &candidate.model)
            .collect();
        if candidates.first().is_none_or(|primary| *primary != model) {
            return Err(ContextPlanError::invariant());
        }
        if candidates.len() == 1 {
            let prepared = self.compactor.prepare_invocation(request).await?;
            let chunked = musteragent_runtime_snapshot::uses_provider_capacity_recovery(
                prepared.slot.compaction_policy_ref.as_str(),
            );
            return self.perform_summary(input, prepared, chunked, None).await;
        }
        let mut failures: Vec<(crate::CompactionCandidatePredecessor, ContextPlanError)> =
            Vec::new();
        let mut all_retained = true;
        for (ordinal, candidate) in candidates.into_iter().enumerate() {
            let mut physical = request.clone();
            physical.capture_provider_failure = true;
            if ordinal > 0 {
                physical.model = candidate.clone();
                physical.max_output_tokens = model_capacity::compression_output_budget(
                    physical.slot.compaction_policy_ref.as_str(),
                    &candidate.route,
                    summary_budget,
                )?;
                physical.slot.plan_fingerprint = physical.expected_plan_fingerprint()?;
                if !complete_source_fits(&physical)? {
                    continue;
                }
            }
            let predecessor = failures.last().map(|(prior, _)| prior.clone());
            let (prepared, retained_failure) = self
                .compactor
                .prepare_role_candidate(physical, predecessor)
                .await?;
            all_retained &= retained_failure.is_some();
            // Standbys must accept the exact original source as a single input.
            let chunked = ordinal == 0
                && musteragent_runtime_snapshot::uses_provider_capacity_recovery(
                    prepared.slot.compaction_policy_ref.as_str(),
                );
            let physical = crate::CompactionCandidatePredecessor::from(&prepared);
            match self
                .perform_summary(input, prepared, chunked, retained_failure)
                .await?
            {
                SummaryGeneration::FailedAfterStart { error, .. }
                    if error.permits_role_fallback() =>
                {
                    failures.push((physical, error));
                }
                other => return Ok(other),
            }
        }
        // A permanently rejected final route must not mask an earlier recoverable one.
        let (prior, error) = failures
            .iter()
            .filter(|(_, error)| error.retryable)
            .min_by_key(|(_, error)| error.retry_after_ms.unwrap_or(0))
            .or_else(|| failures.last())
            .ok_or_else(ContextPlanError::invariant)?;
        if all_retained
            && error.retryable
            && error.retry_after_ms.unwrap_or(0) == 0
            && matches!(
                request.recovery_policy,
                musteragent_runtime_snapshot::RecoveryPolicySnapshot::Persistent { .. }
            )
        {
            let highest = failures
                .iter()
                .map(|(fact, _)| fact.generation)
                .max()
                .ok_or_else(ContextPlanError::invariant)?;
            let probe = prior.clone().after_completed_round(highest);
            let physical = probe.restore_route(request)?;
            let chunked = probe.model == *model
                && musteragent_runtime_snapshot::uses_provider_capacity_recovery(
                    physical.slot.compaction_policy_ref.as_str(),
                );
            match self
                .compactor
                .prepare_role_candidate(physical, Some(probe))
                .await
            {
                Ok((prepared, retained)) => {
                    return self
                        .perform_summary(input, prepared, chunked, retained)
                        .await;
                }
                Err(verified) if verified.permits_role_fallback() => {
                    return Ok(SummaryGeneration::FailedAfterStart {
                        invocation_id: prior.invocation_id,
                        error: verified,
                    });
                }
                Err(other) => return Err(other),
            }
        }
        Ok(SummaryGeneration::FailedAfterStart {
            invocation_id: prior.invocation_id,
            error: error.clone(),
        })
    }

    async fn perform_summary(
        &self,
        input: &ContextPlanInput,
        request: ContextCompactionRequest,
        chunked: bool,
        retained_failure: Option<ContextPlanError>,
    ) -> Result<SummaryGeneration, ContextPlanError> {
        let model_route = request.model.route.route_ref.clone();
        let previous_summary_ref = request.slot.previous_summary_ref.clone();
        let invocation_id = request.invocation.invocation_id;
        self.plans
            .begin_compaction(ContextCompactionStartCommit {
                attempt: input.attempt.clone(),
                invocation_id,
                loop_iteration: input.next_iteration,
                source_seq_from: request.slot.source_seq_from,
                source_seq_to: request.slot.source_seq_to,
                causation_id: request.invocation.causation_id,
                model_route_ref: musteragent_runtime_model::ModelRouteRef::from_key(
                    model_route.clone(),
                ),
                input_tokens_before: request.slot.input_tokens_before,
            })
            .await?;
        let result = if let Some(error) = retained_failure {
            Err(error)
        } else if chunked {
            super::compaction_chunks::compact_prepared(self.compactor.as_ref(), request).await
        } else {
            self.compactor.compact(request).await
        };
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                if !error.is_indeterminate()
                    && error.code != musteragent_runtime_contract::ErrorCode::IdempotencyConflict
                {
                    self.plans
                        .fail_compaction(ContextCompactionFailureCommit {
                            attempt: input.attempt.clone(),
                            invocation_id,
                            error: compaction_error_info(&error)?,
                        })
                        .await?;
                }
                return Ok(SummaryGeneration::FailedAfterStart {
                    invocation_id,
                    error,
                });
            }
        };
        if (!chunked && result.invocation_id != invocation_id)
            || result.model_route_ref.as_str() != model_route.as_str()
        {
            return Ok(SummaryGeneration::FailedAfterStart {
                invocation_id,
                error: ContextPlanError::invariant(),
            });
        }
        Ok(SummaryGeneration::Ready(GeneratedSummary {
            summary: result.summary,
            generator_invocation_id: Some(result.invocation_id),
            performed_invocation_id: Some(invocation_id),
            previous_summary_ref,
        }))
    }
}

/// Without a provider-specific tokenizer, reserve one token per UTF-8 byte of
/// actual prompt plus framing. A provider capacity rejection still fails the
/// intact request and cannot publish an incomplete summary.
fn complete_source_fits(request: &ContextCompactionRequest) -> Result<bool, ContextPlanError> {
    let canonical =
        crate::model_compaction::canonical_request(request, request.invocation.clone())?;
    let mut budget = canonical.max_output_tokens.get().saturating_add(32);
    for text in &canonical.instructions {
        budget = budget
            .saturating_add(text.as_str().len() as u64)
            .saturating_add(8);
    }
    for message in &canonical.input {
        budget = budget.saturating_add(8);
        for part in &message.content {
            let musteragent_runtime_model::CanonicalContentPart::Text(value) = part else {
                return Err(ContextPlanError::invariant());
            };
            budget = budget.saturating_add(value.as_str().len() as u64);
        }
    }
    Ok(budget <= request.model.route.context_window.get())
}

#[cfg(test)]
mod candidate_tests {
    use super::*;

    #[test]
    fn capacity_includes_prior_summary_exact_history_instructions_and_output() {
        let mut request = crate::model_compaction::tests::request();
        request.model.route.context_window = NonZeroU64::new(12_000).unwrap();
        let original = request.serialized_history.clone();
        assert!(complete_source_fits(&request).unwrap());
        request.previous_summary = Some(ModelText::new("🦀".repeat(4000)).unwrap());
        assert!(!complete_source_fits(&request).unwrap());
        request.model.route.context_window = NonZeroU64::new(64_000).unwrap();
        assert!(complete_source_fits(&request).unwrap());
        assert_eq!(request.serialized_history, original);
    }
}
