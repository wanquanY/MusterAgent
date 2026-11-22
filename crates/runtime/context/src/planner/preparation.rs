//! Prepare one model request from the sealed run context.
use super::*;

impl DurableContextPlanner {
    pub(super) async fn prepare(
        &self,
        mut input: ContextPlanInput,
        profile: Option<ContextPlanProfile>,
    ) -> Result<ModelIteration, ContextPlanError> {
        if input.next_iteration == 0
            || input.continuation_ref.is_some() != input.continuation_source_invocation_id.is_some()
        {
            return Err(log_context_plan_error(
                input.attempt.run_id,
                "input_validation",
                ContextPlanError::invariant(),
            ));
        }
        // Clear continuation before even selecting the material source view: a
        // source may otherwise fetch only the opaque provider's delta.
        if self.projection.is_some() {
            input.continuation_ref = None;
            input.continuation_source_invocation_id = None;
        }
        let mut planner = self.clone();
        if let Some(source) = self.source.preparation_view(&input) {
            planner.source = source;
        }
        let run_id = input.attempt.run_id;
        let material_reuse = observe_context_plan_stage(run_id, "material_cache_begin", || {
            Ok(incremental_material::MaterialReuse::begin(
                self.material_cache.clone(),
            ))
        })?;
        let dialogue_cut = observe_context_plan_future(
            run_id,
            "dialogue_prepare",
            Box::pin(planner.prepare_dialogue_context(&input, &material_reuse)),
        )
        .await?;
        // History catch-up can run the full compression graph. Finish that
        // phase before polling the separate, large primary-request planner.
        Box::pin(planner.prepare_request(input, profile, dialogue_cut, material_reuse, false)).await
    }

    pub(super) fn project_for_model(
        &self,
        material: &mut ContextMaterial,
        material_reuse: &incremental_material::MaterialReuse,
    ) -> Result<(), ContextPlanError> {
        crate::model_tool_views::project_material(material)?;
        let tenant_id = material.run_context.tenant_id;
        crate::tool_result_visibility::project_material(material, |message, view| {
            material_reuse.project_tool_view(tenant_id, message, view)
        })
    }

    async fn prepare_request(
        &self,
        mut input: ContextPlanInput,
        profile: Option<ContextPlanProfile>,
        dialogue_cut: Option<musteragent_execution_context::DialogueContextCut>,
        material_reuse: incremental_material::MaterialReuse,
        shared_prefix_compacted: bool,
    ) -> Result<ModelIteration, ContextPlanError> {
        let run_id = input.attempt.run_id;
        let mut material =
            observe_context_plan_future(run_id, "material_load", self.source.load(&input)).await?;
        observe_context_plan_stage(run_id, "material_validation", || {
            validate_material(&input, &material)
        })?;
        if profile
            .as_ref()
            .is_some_and(|profile| profile.activity_kind != material.run_context.activity_kind)
        {
            return Err(ContextPlanError::invariant());
        }
        observe_context_plan_stage(run_id, "dialogue_primary_cut", || {
            dialogue::validate_primary_cut(&material, dialogue_cut.as_ref())
        })?;
        authority::bind_selected_reply(&mut material)?;
        // Opaque provider continuations may retain the old system instructions.
        // A consumed control after a sealed compaction also changes the exact
        // provider input. An opaque tool delta cannot carry that new message.
        if material.agent_instructions.is_some() || material.superseding_control_id.is_some() {
            input.continuation_ref = None;
            input.continuation_source_invocation_id = None;
        }
        // An opaque continuation without a selected, same-Run delta has no
        // provenance under isolation. Rebuild the complete canonical input;
        // ordinary prefix caching remains safe because it hashes that input.
        if material
            .runtime_snapshot
            .context_policy
            .effective_source_selection()
            == musteragent_runtime_snapshot::ContextSourceSelectionPolicy::ExecutionIsolatedV1
            && input
                .continuation_source_invocation_id
                .is_some_and(|anchor| {
                    !material
                        .entries
                        .iter()
                        .any(|entry| entry.continuation_delta_for == Some(anchor))
                })
        {
            input.continuation_ref = None;
            input.continuation_source_invocation_id = None;
        }
        observe_context_plan_stage(run_id, "tool_result_visibility", || {
            self.project_for_model(&mut material, &material_reuse)
        })?;
        let (active_route, credential_grant_ref) =
            observe_context_plan_stage(run_id, "active_model_route", || {
                active_model_route(&material, input.active_model_route_ordinal)
            })?;
        let repair = observe_context_plan_stage(run_id, "delivery_repair_material", || {
            delivery_repair::load(&material, active_route)
        })?;
        if repair.is_some() {
            input.continuation_ref = None;
            input.continuation_source_invocation_id = None;
        }
        let plan_generation =
            observe_context_plan_stage(run_id, "plan_lineage", || match material.plan_selection {
                Some(selection) => {
                    selection.validate()?;
                    if selection
                        .source_control
                        .is_some_and(|c| c.sequence > material.ledger_cutoff)
                    {
                        return Err(ContextPlanError::invariant());
                    }
                    Ok(selection.generation())
                }
                None => Ok(CONTEXT_PLAN_GENERATION),
            })?;
        let ids = observe_context_plan_stage(run_id, "iteration_identity", || {
            IterationIds::derive(
                &input,
                material.next_assistant_response_ordinal,
                plan_generation,
            )
        })?;
        let mut instructions =
            observe_context_plan_stage(run_id, "instruction_materialization", || {
                material_reuse.instructions(&material)
            })?;
        let mut runtime_events =
            observe_context_plan_stage(run_id, "runtime_event_projection", || {
                delegation_completion_events(&material.delegation_completions)
            })?;
        if let Some(source_invocation_id) = material.model_recovery {
            runtime_events.push(
                musteragent_runtime_model::CanonicalRuntimeEvent::ModelRecovery {
                    source_invocation_id,
                },
            );
        }
        if let Some(repair) = &repair {
            runtime_events.push(repair.runtime_event());
        }
        let mut tools = observe_context_plan_stage(run_id, "tool_surface_materialization", || {
            material_reuse.tools(self.codec.as_ref(), &material)
        })?;
        if let Some(profile) = &profile {
            instructions.extend(profile.instructions.iter().cloned());
            tools.extend(profile.tools.iter().cloned());
        }
        if delivery_repair::format_only(repair.as_ref()) {
            tools.clear();
        }
        if material.tool_policy == ContextToolPolicy::SnapshotDiscovery
            && !delivery_repair::format_only(repair.as_ref())
        {
            observe_context_plan_stage(run_id, "tool_surface_authority_instruction", || {
                append_tool_surface_authority_instruction(&mut instructions)
            })?;
            observe_context_plan_stage(run_id, "file_tool_guidance", || {
                file_tool_guidance::append_for_snapshot(
                    &mut instructions,
                    &tools,
                    &material.runtime_snapshot,
                )?;
                terminal_tool_guidance::append_for_snapshot(
                    &mut instructions,
                    &tools,
                    &material.runtime_snapshot,
                )
            })?;
        }
        native_publication::append_reply_guidance(&mut instructions, &material, &tools)?;
        crate::progress_note::append_instruction(&mut instructions, &material, &tools)?;
        let progress = observe_context_plan_future(
            run_id,
            "run_progress_directive",
            self.progress.directive(&input.attempt),
        )
        .await?;
        instructions.extend(progress.private_instructions);
        observe_context_plan_stage(run_id, "empty_response_recovery_instruction", || {
            append_empty_response_recovery_instruction(
                &mut instructions,
                input.consecutive_empty_responses,
            )
        })?;
        let mut rejection_directive = Vec::new();
        observe_context_plan_stage(run_id, "model_action_rejection_instruction", || {
            super::instruction_rendering::append_model_action_rejection_with_catalog(
                &mut rejection_directive,
                material.model_action_rejection.as_ref(),
                &tools,
                &material.runtime_snapshot.tools,
                material.tool_policy,
                material.runtime_snapshot.application_catalogs.is_empty(),
            )
        })?;
        // A delivery repair brings its own final turn, so its directive stays
        // with the instructions; otherwise the directive closes the input.
        if repair.is_some() {
            instructions.append(&mut rejection_directive);
        }
        let model_action_correction_turn =
            observe_context_plan_stage(run_id, "model_action_correction_turn", || match &repair {
                Some(repair) => Ok(Some(repair.turn.clone())),
                None => model_action_rejection_turn(
                    material.model_action_rejection.as_ref(),
                    rejection_directive.first(),
                ),
            })?;
        if material.entries.is_empty() {
            return Err(ContextPlanError::invariant());
        }
        let mut previous_summary = observe_context_plan_stage(run_id, "summary_decode", || {
            material
                .previous_context
                .as_ref()
                .map(|previous| {
                    verify_content(&previous.summary.content_ref, &previous.summary.bytes)
                        .map_err(|_| ContextPlanError::invariant())?;
                    self.codec.decode_summary(&previous.summary.bytes)
                })
                .transpose()
        })?;
        let previous_context = if source_baseline::can_reuse(&material, previous_summary.as_ref()) {
            material.previous_context.as_ref()
        } else {
            previous_summary = None;
            None
        };
        let material_entries = order_entries_for_run_turn(&material);
        let baseline = observe_context_plan_stage(run_id, "baseline_resolution", || {
            resolve_context_baseline(
                previous_context,
                &material_entries,
                previous_summary.as_ref(),
            )
        })?;
        // Repeated progress reads are durable audit facts, but unchanged or
        // superseded observations are not independent model-history turns.
        // Apply this semantic projection after the durable baseline so an
        // observation already represented by the summary cannot influence
        // suffix coalescing.
        let logical_entries =
            observe_context_plan_stage(run_id, "logical_history_projection", || {
                coalesce_progress_observations(&baseline.entries)
            })?;
        if logical_entries.is_empty() {
            return Err(ContextPlanError::invariant());
        }
        runtime_events.extend(output_continuation::event(&logical_entries, run_id));
        if let Some(event) =
            native_publication::event(&input, &material, &logical_entries, &runtime_events)?
        {
            runtime_events.push(event);
        }
        let provider_entries =
            observe_context_plan_stage(run_id, "provider_input_selection", || {
                select_provider_input(&input, &material, &logical_entries)
            })?;
        let base_prefix_tokens = estimate_instruction_tokens(&instructions)
            .saturating_add(estimate_tool_tokens(&tools))
            .saturating_add(estimate_runtime_event_tokens(&runtime_events))
            .saturating_add(
                model_action_correction_turn
                    .as_ref()
                    .map_or(0, estimate_message),
            );
        let previous_summary_text = observe_context_plan_stage(run_id, "summary_text", || {
            baseline
                .summary
                .map(|summary| render_history_summary(summary, u64::MAX, true))
                .transpose()
        })?;
        let effective_prefix_tokens = previous_summary_text
            .as_ref()
            .map_or(base_prefix_tokens, |summary| {
                base_prefix_tokens.saturating_add(estimate_text_tokens(summary))
            });
        let planning_budgets =
            observe_context_plan_stage(run_id, "planning_budget_resolution", || {
                planning_budgets(&material, active_route)
            })?;
        let input_budget = planning_budgets.hard_input_budget();
        let provider_driven = musteragent_runtime_snapshot::uses_provider_capacity_recovery(
            material
                .runtime_snapshot
                .context_policy
                .compaction_policy_ref
                .as_str(),
        );
        let recover_capacity = provider_driven && material.context_capacity_exceeded;
        // The prior rolling summary remains compressible; the private repair
        // source itself is irreducible and must fit before any summary call.
        if (repair.is_some() && base_prefix_tokens >= input_budget)
            || (repair.is_none() && !provider_driven && effective_prefix_tokens >= input_budget)
        {
            return Err(ContextPlanError::quota());
        }

        // A route switch is a projection boundary, not a compaction policy.
        // First build the exact target-route input and size that result. Item
        // count and foreign Assistant provenance do not consume an independent
        // budget; only the target route's ordinary token policy may compact it.
        let uncompressed_tool_history =
            observe_context_plan_stage(run_id, "provider_history_projection", || {
                material_reuse.history(&material, &logical_entries, active_route, false)
            })?;
        let uncompressed_provider_messages = uncompressed_tool_history
            .messages
            .iter()
            .collect::<Vec<_>>();
        let uncompressed_provider_input =
            observe_context_plan_stage(run_id, "route_presentation", || {
                present_messages_for_route(&uncompressed_provider_messages, active_route)
            })?;
        let uncompressed_tokens = effective_prefix_tokens
            .saturating_add(estimate_messages(&uncompressed_provider_input))
            .saturating_add(estimate_runtime_event_tokens(
                &logical_entries
                    .iter()
                    .flat_map(|entry| entry.runtime_events.iter().cloned())
                    .collect::<Vec<_>>(),
            ));

        // Provider usage remains the best trigger anchor only when its
        // completion belongs to this exact signed route. Source-model usage is
        // not physically comparable after a model or protocol conversion.
        let target_usage_anchor = if uncompressed_tool_history.has_portable_history() {
            None
        } else if let Some(anchor) = material.usage_anchor.as_ref() {
            observe_context_plan_stage(run_id, "usage_anchor_validation", || {
                target_route_owns_usage_anchor(
                    &logical_entries,
                    anchor.source_sequence,
                    active_route,
                )
            })?
            .then_some(anchor)
        } else {
            None
        };
        let trigger_tokens = target_usage_anchor.map_or(uncompressed_tokens, |anchor| {
            estimate_context_tokens(effective_prefix_tokens, &logical_entries, Some(anchor))
        });

        let measured_trigger = musteragent_runtime_snapshot::uses_measured_context_usage(
            material
                .runtime_snapshot
                .context_policy
                .compaction_policy_ref
                .as_str(),
        ) && capacity::measured_trigger(
            &logical_entries,
            material.measured_usage.as_ref(),
            active_route,
            planning_budgets.trigger_budget(),
        )?;
        let native_history = musteragent_runtime_snapshot::uses_native_history(
            material
                .runtime_snapshot
                .context_policy
                .compaction_policy_ref
                .as_str(),
        );
        // A newly admitted task has no supplier usage of its own. Its first
        // request can exceed the target even when history fits one storage
        // page. Size the actual target-route projection before dispatch.
        let first_request_pressure = native_history
            && input.next_iteration == 1
            && trigger_tokens >= planning_budgets.trigger_budget();
        let threshold_reached =
            !shared_prefix_compacted && (measured_trigger || first_request_pressure);
        let required_savings = trigger_tokens
            .max(
                material
                    .measured_usage
                    .as_ref()
                    .filter(|_| measured_trigger)
                    .map_or(0, |usage| usage.input_tokens),
            )
            .saturating_sub(
                planning_budgets.compaction_target(
                    material
                        .runtime_snapshot
                        .context_policy
                        .compaction_policy_ref
                        .as_str(),
                ),
            )
            .saturating_add(1);
        // Models mirror their own past calls: a history of many note-less
        // work calls is compacted once so progress notes can start.
        let narration_bootstrap = native_history
            && !shared_prefix_compacted
            && input.next_iteration == 1
            && crate::progress_note::needs_bootstrap(
                &material,
                logical_entries.iter().flat_map(|entry| {
                    let paged = entry.history_page.iter().flat_map(|page| {
                        page.history.items.iter().flat_map(|item| match item {
                            crate::ConversationHistoryItem::Message(message, _) => {
                                message.tool_calls.as_slice()
                            }
                            crate::ConversationHistoryItem::Reference(_) => &[],
                        })
                    });
                    entry.message.tool_calls.iter().chain(paged)
                }),
            );
        if native_history
            && !shared_prefix_compacted
            && (threshold_reached || recover_capacity || narration_bootstrap)
            && baseline.summary.is_none_or(|summary| !summary.compacted)
            && logical_entries
                .iter()
                .any(|entry| entry.history_page.is_some())
            && let Some(window) = self.source.dialogue_window(&input).await?
            && window.complete()
            && window.has_uncompacted_prefix()
            && self
                .compact_dialogue_prefix(&input, &window, &material, Some(required_savings))
                .await?
        {
            input.continuation_ref = None;
            input.continuation_source_invocation_id = None;
            return Box::pin(self.prepare_request(
                input,
                profile,
                dialogue_cut,
                material_reuse,
                true,
            ))
            .await;
        }
        let compress_for_provider = (recover_capacity && !shared_prefix_compacted)
            || threshold_reached
            || (repair.is_some() && uncompressed_tokens > input_budget);
        let (logical_retained, mut compacted) =
            observe_context_plan_stage(run_id, "compaction_selection", || {
                if provider_driven && !compress_for_provider {
                    return Ok((logical_entries.clone(), Vec::new()));
                }
                if compress_for_provider {
                    if native_history {
                        return capacity::select_native(
                            |entry| authority::protected(&material, entry),
                            &logical_entries,
                            recover_capacity
                                || (measured_trigger
                                    && material
                                        .measured_usage
                                        .as_ref()
                                        .is_some_and(|usage| usage.input_tokens >= input_budget)),
                            required_savings,
                        );
                    }
                    return capacity::select(&logical_entries);
                }
                compact_entries(
                    &logical_entries,
                    base_prefix_tokens,
                    CompactionSelectionBudgets {
                        hard_input_budget: input_budget,
                        physical_trigger_tokens: if recover_capacity {
                            u64::MAX
                        } else {
                            trigger_tokens
                        },
                        deterministic_uncompressed_tokens: uncompressed_tokens,
                        soft_trigger_budget: planning_budgets.trigger_budget(),
                        recent_tail_budget: planning_budgets.recent_tail_budget(),
                    },
                    false,
                )
            })?;
        // Approval facts and the original current assignment are exact input,
        // not narrative that a summary may replace. Preserve whole Tool units.
        let (logical_retained, protected_compacted) =
            authority::retain(&material, &logical_entries, logical_retained, compacted)?;
        compacted = protected_compacted;
        // Opaque provider continuation is only a transport optimization for an
        // uncompressed incremental request. Compaction is authorized by the
        // complete logical Context Units and creates a fresh provider scope;
        // the old continuation must never cross that boundary.
        let mut retained = if compacted.is_empty() && !compress_for_provider {
            provider_entries.clone()
        } else {
            logical_retained
        };
        let recovered_tail = if recover_capacity {
            Some(capacity::bounded_tool_entries(
                &retained,
                tools
                    .iter()
                    .any(|tool| tool.name.as_str() == "context_read"),
            )?)
        } else {
            None
        };
        if let Some(tail) = &recovered_tail {
            retained = tail.iter().collect();
        }
        let mut request_continuation_ref = compacted
            .is_empty()
            .then(|| input.continuation_ref.clone())
            .flatten();
        if compress_for_provider {
            request_continuation_ref = None;
        }
        let mut summary = observe_context_plan_stage(run_id, "summary_projection", || {
            build_summary(&material, &compacted, baseline.summary)
        })?;
        let mut pending_compaction_resolution = None;
        let mut generated_summary_budget = None;
        // With only the newest atomic exchange left, the prior checkpoint may
        // still dominate the rejected request. Recompress that checkpoint with
        // its original provenance instead of declaring the request irreducible.
        let recompress_checkpoint = (recover_capacity
            || (repair.is_some() && uncompressed_tokens > input_budget))
            && !shared_prefix_compacted
            && compacted.is_empty()
            && baseline.summary.is_some_and(|summary| {
                summary.summary.is_some() && !summary.summarized_record_ids.is_empty()
            });
        if !compacted.is_empty() || recompress_checkpoint {
            let summarized_record_count = if recompress_checkpoint {
                u64::try_from(summary.summarized_record_ids.len())
                    .map_err(|_| ContextPlanError::invariant())?
            } else {
                record_count(&compacted)?
            };
            let retained_record_count = record_count(&retained)?;
            let compression_model = auxiliary_model_route(
                &material.runtime_snapshot.auxiliary_models,
                AuxiliaryModelRole::Compression,
            )?;
            let summary_source_tokens = rolling_summary_source_tokens(baseline.summary, &compacted);
            let summary_budget = if model_capacity::uses_model_capacity(&material) {
                model_capacity::summary_target(planning_budgets, summary_source_tokens)
            } else if provider_driven && repair.is_none() {
                compression_model.route.max_output_tokens.get().min(8192)
            } else {
                compaction_summary_budget(
                    planning_budgets,
                    base_prefix_tokens,
                    &retained,
                    summary_source_tokens,
                )?
            };
            generated_summary_budget = Some(summary_budget);
            let compaction = observe_context_plan_future(
                run_id,
                "summary_generation",
                self.generate_summary(
                    &input,
                    &material,
                    material.runtime_compaction_source()?,
                    compression_model,
                    &compacted,
                    &retained,
                    baseline.summary,
                    uncompressed_tokens,
                    input_budget,
                    summary_budget,
                ),
            )
            .await;
            match compaction {
                Ok(SummaryGeneration::Ready(generated)) => {
                    summary.summary = Some(generated.summary.as_str().to_owned());
                    summary.generator_invocation_id = generated.generator_invocation_id;
                    summary.previous_summary_ref = generated.previous_summary_ref;
                    if let Some(invocation_id) = generated.performed_invocation_id {
                        pending_compaction_resolution = Some(PendingCompactionResolution {
                            invocation_id,
                            outcome: ContextCompactionOutcome::Succeeded,
                            loop_iteration: input.next_iteration,
                            input_tokens_before: uncompressed_tokens,
                            summarized_record_count,
                            retained_record_count,
                            error: None,
                        });
                    }
                }
                Ok(SummaryGeneration::FailedAfterStart {
                    invocation_id,
                    error,
                }) if !provider_driven
                    && can_fallback_to_uncompressed(&error, uncompressed_tokens, input_budget) =>
                {
                    pending_compaction_resolution = Some(PendingCompactionResolution {
                        invocation_id,
                        outcome: ContextCompactionOutcome::FailedContinuedUncompressed,
                        loop_iteration: input.next_iteration,
                        input_tokens_before: uncompressed_tokens,
                        summarized_record_count,
                        retained_record_count: record_count(&provider_entries)?,
                        error: Some(compaction_error_info(&error)?),
                    });
                    // A failed compaction must never discard source history.
                    // If the exact uncompressed request still fits, continue
                    // losslessly and let the durable failure remain observable.
                    retained = provider_entries.clone();
                    compacted.clear();
                    request_continuation_ref = input.continuation_ref.clone();
                    summary = build_summary(&material, &compacted, baseline.summary)?;
                    generated_summary_budget = None;
                }
                Ok(SummaryGeneration::FailedAfterStart {
                    invocation_id,
                    error,
                }) => {
                    let (outcome, error) = compaction_resolution::failure_resolution(
                        error,
                        material.runtime_snapshot.execution_policy.recovery_policy,
                    );
                    self.plans
                        .resolve_compaction(ContextCompactionResolutionCommit {
                            attempt: input.attempt.clone(),
                            resolution: ContextCompactionResolution {
                                invocation_id,
                                outcome,
                                loop_iteration: input.next_iteration,
                                context_snapshot_id: None,
                                dialogue_checkpoint_id: None,
                                input_tokens_before: uncompressed_tokens,
                                input_tokens_after: None,
                                summarized_record_count,
                                retained_record_count,
                                error: Some(compaction_error_info(&error)?),
                            },
                        })
                        .await?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        let reused_summary = (compacted.is_empty() && !recompress_checkpoint)
            .then_some(baseline.context)
            .flatten();
        let (summary_bytes, summary_ref) = if let Some(previous) = reused_summary {
            (
                previous.summary.bytes.clone(),
                previous.summary.content_ref.clone(),
            )
        } else {
            let bytes = observe_context_plan_stage(run_id, "summary_encoding", || {
                self.codec.encode_summary(&summary)
            })?;
            let reference = observe_context_plan_future(
                run_id,
                "summary_persist",
                self.content.put(
                    &input.attempt,
                    material.run_context.tenant_id,
                    ContextPlanContentKind::StructuredSummary,
                    bytes.clone(),
                ),
            )
            .await?;
            (bytes, reference)
        };
        let summary_text = if compacted.is_empty() && !recompress_checkpoint {
            previous_summary_text.clone()
        } else {
            Some(render_history_summary(
                &summary,
                if provider_driven {
                    u64::MAX
                } else {
                    generated_summary_budget.ok_or_else(ContextPlanError::invariant)?
                },
                !provider_driven && planning_budgets.uses_deterministic_token_budget(),
            )?)
        };
        let provider_tool_history =
            observe_context_plan_stage(run_id, "retained_provider_history_projection", || {
                material_reuse.history(
                    &material,
                    &retained,
                    active_route,
                    uncompressed_tool_history.has_portable_history()
                        && (!compacted.is_empty() || recompress_checkpoint),
                )
            })?;
        let provider_messages = provider_tool_history.messages.iter().collect::<Vec<_>>();
        let mut provider_input =
            observe_context_plan_stage(run_id, "retained_route_presentation", || {
                present_messages_for_route(&provider_messages, active_route)
            })?;
        if request_continuation_ref.is_none() {
            attach_history_summary(&mut provider_input, summary_text.as_deref())?;
        }
        if let Some(turn) = &model_action_correction_turn {
            provider_input.push(turn.clone());
        }
        let causation_id = observe_context_plan_stage(run_id, "causation_resolution", || {
            last_causation_id(&material)
        })?;
        let max_output_tokens =
            observe_context_plan_stage(run_id, "output_budget_resolution", || {
                output_budget(&material, active_route)
            })?;
        let original_runtime_events = if recover_capacity {
            let mut original = runtime_events.clone();
            original.extend(
                logical_entries
                    .iter()
                    .flat_map(|entry| entry.runtime_events.iter().cloned()),
            );
            dialogue_images::present_history(
                active_route,
                &uncompressed_provider_input,
                &mut original,
            )?;
            Some(original)
        } else {
            None
        };
        runtime_events.extend(
            retained
                .iter()
                .flat_map(|entry| entry.runtime_events.iter().cloned()),
        );
        observe_context_plan_stage(run_id, "dialogue_image_presentation", || {
            dialogue_images::present_history(active_route, &provider_input, &mut runtime_events)
        })?;
        let mut request = CanonicalModelRequest {
            invocation: InvocationContext {
                invocation_id: ids.invocation_id,
                run_context: material.run_context,
                attempt_context: input.attempt.clone(),
                parent_invocation_id: None,
                causation_id,
                correlation_id: ids.correlation_id,
                timing: input.invocation_timing.clone(),
            },
            route: active_route.clone(),
            route_role: ModelInvocationRouteRole::Chat,
            credential_grant_ref: credential_grant_ref.clone(),
            continuation_ref: request_continuation_ref,
            instructions,
            input: provider_input,
            runtime_events,
            tools,
            response_schema: match profile {
                Some(profile) => profile.response_schema,
                None => observe_context_plan_stage(run_id, "dialogue_response_schema", || {
                    crate::public_delivery::response_schema(
                        &material,
                        active_route,
                        self.codec.as_ref(),
                    )
                })?,
            },
            reasoning_policy: reasoning_policy(&material),
            cache_policy: CachePolicy::ExplicitEphemeralPrefix,
            stream_budget: material.runtime_snapshot.execution_policy.model_stream,
            max_output_tokens,
            temperature: temperature(&material),
        };
        if let Some(projection) = &self.projection {
            let view = projection
                .project(
                    &input.attempt,
                    &request,
                    crate::ModelContextView {
                        instructions: request.instructions.clone(),
                        input: request.input.clone(),
                    },
                )
                .await?;
            request.instructions = view.instructions;
            request.input = view.input;
        }
        if repair.is_some() {
            observe_context_plan_stage(run_id, "delivery_repair_request", || {
                delivery_repair::validate_material_request(&material, &request)
            })?;
        }
        observe_context_plan_stage(run_id, "dialogue_input_capacity", || {
            dialogue::validate_input_capacity(&material, &request)
        })?;
        observe_context_plan_stage(run_id, "canonical_request_validation", || {
            request.validate().map_err(|error| {
                tracing::error!(
                    run_id = %request.invocation.run_context.run_id,
                    invocation_id = %request.invocation.invocation_id,
                    tool_count = request.tools.len(),
                    context.plan.error = error.as_str(),
                    error = ?error,
                    "Context Planner rejected the canonical model request",
                );
                ContextPlanError::invariant()
            })
        })?;
        let request_bytes = observe_context_plan_stage(run_id, "model_request_encoding", || {
            self.codec.encode_model_request(&request)
        })?;
        if recover_capacity {
            let mut original = request.clone();
            original.input = uncompressed_provider_input;
            if input.continuation_ref.is_none() {
                attach_history_summary(&mut original.input, previous_summary_text.as_deref())?;
            }
            if let Some(turn) = model_action_correction_turn {
                original.input.push(turn);
            }
            original.runtime_events =
                original_runtime_events.ok_or_else(ContextPlanError::invariant)?;
            original.continuation_ref = input.continuation_ref.clone();
            let previous_bytes = material
                .rejected_request_bytes
                .unwrap_or(self.codec.encode_model_request(&original)?.len() as u64);
            if (request_bytes.len() as u64).saturating_add(128) >= previous_bytes {
                let error = capacity::exceeded();
                if let Some(pending) = &pending_compaction_resolution {
                    self.plans
                        .fail_compaction(ContextCompactionFailureCommit {
                            attempt: input.attempt.clone(),
                            invocation_id: pending.invocation_id,
                            error: compaction_error_info(&error)?,
                        })
                        .await?;
                }
                return Err(error);
            }
        }
        let request_ref = observe_context_plan_future(run_id, "model_request_persist", async {
            self.content
                .put(
                    &input.attempt,
                    material.run_context.tenant_id,
                    ContextPlanContentKind::ModelRequest,
                    request_bytes.clone(),
                )
                .await
        })
        .await?;
        let after_tokens = estimate_instruction_tokens(&request.instructions)
            .saturating_add(estimate_tool_tokens(&request.tools))
            .saturating_add(estimate_runtime_event_tokens(&request.runtime_events))
            .saturating_add(estimate_messages(&request.input));
        if (!provider_driven || repair.is_some())
            && (after_tokens > input_budget
                || (!compacted.is_empty() && planning_budgets.requires_hysteresis(after_tokens)))
        {
            return Err(ContextPlanError::quota());
        }
        let retained_record_ids = retained
            .iter()
            .flat_map(|entry| entry.source_record_ids.iter().copied())
            .collect::<Vec<_>>();
        let context_snapshot =
            observe_context_plan_stage(run_id, "context_snapshot_construction", || {
                build_logical_context_snapshot(
                    self.codec.as_ref(),
                    &request_bytes,
                    &summary_bytes,
                    plan_generation,
                    material.run_context.run_id,
                    input.next_iteration,
                    ids.snapshot_id,
                    baseline.context.map(|context| context.snapshot_id),
                    &material,
                    repair.as_ref().map(|repair| repair.binding.clone()),
                    retained_record_ids,
                    summary_ref,
                    request_ref.clone(),
                    uncompressed_tokens,
                    after_tokens,
                    musteragent_runtime_model::ModelRouteRef::from_key(
                        active_route.route_ref.clone(),
                    ),
                )
            })?;
        let context_snapshot_id = context_snapshot.snapshot_id;
        let compaction_resolution =
            pending_compaction_resolution.map(|pending| ContextCompactionResolutionCommit {
                attempt: input.attempt.clone(),
                resolution: pending.committed(context_snapshot_id, after_tokens),
            });
        let resolution = observe_context_plan_future(
            run_id,
            "context_plan_commit",
            self.plans.resolve_or_commit_with_compaction(
                ContextPlanCommit {
                    plan_selection: material.plan_selection,
                    source_fence: crate::ContextSourceFence {
                        content_refs: material.source_content_refs.clone(),
                        active_branch_id: material.active_branch_id,
                        ledger_cutoff: material.ledger_cutoff,
                    },
                    attempt: input.attempt.clone(),
                    generation: plan_generation,
                    snapshot: context_snapshot,
                    invocation_id: ids.invocation_id,
                    assistant_message_id: ids.assistant_message_id,
                    assistant_output_id: ids.assistant_output_id,
                    reasoning_output_id: ids.reasoning_output_id,
                    delegation_delivery_ids: material
                        .delegation_completions
                        .iter()
                        .map(|completion| completion.delivery_id)
                        .collect(),
                },
                compaction_resolution,
            ),
        )
        .await
        .inspect_err(|_| material_reuse.discard(&material))?;
        let stored = resolution.identity;
        observe_context_plan_stage(run_id, "stored_identity_validation", || {
            ids.validate_stored(material.run_context.run_id, input.next_iteration, &stored)
        })?;
        Ok(ModelIteration {
            delivery_repair: repair.map(|repair| repair.binding),
            iteration: input.next_iteration,
            invocation_id: stored.invocation_id,
            assistant_message_id: stored.assistant_message_id,
            assistant_output_id: stored.assistant_output_id,
            reasoning_output_id: stored.reasoning_output_id,
            request_ref,
            request,
        })
    }
}
