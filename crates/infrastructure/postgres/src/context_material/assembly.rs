//! Assemble the frozen execution/source material independently of fresh Dialogue facts.
use super::*;

impl PostgresContextMaterialSource {
    pub(super) fn load_material_for_source<'a>(
        &'a self,
        input: &'a ContextPlanInput,
        source: RunSource,
        dialogue_window: Option<DialogueContextWindow>,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<ContextMaterial, PostgresContextMaterialError>,
    > {
        // Keep the materialization state machine off callers' stack frames.
        // Bounded concurrency must not multiply large nested async frames.
        Box::pin(async move {
            let runtime_snapshot = &source.snapshot;
            let source_selector = source_selection::selector(
                &source.run_context,
                source.visibility,
                runtime_snapshot.context_policy.effective_source_selection(),
            );
            let isolated = source_selector.policy
                != musteragent_runtime_snapshot::ContextSourceSelectionPolicy::LegacyAdmissionV1;
            // Independent reads share the already frozen Run/cut. Keep fan-out
            // bounded; each reader releases its connection before further I/O.
            let (mut facts, next_assistant_response_ordinal, resolved) = tokio::try_join!(
                Self::load_ledger_facts_on(
                    &self.pool,
                    self.ledger_decoder.as_ref(),
                    source.run_context.tenant_id,
                    source.run_context.conversation_id,
                    source_selector,
                    source.ledger_cutoff,
                ),
                load_next_assistant_response_ordinal(
                    &self.pool,
                    &source.run_context,
                    source.ledger_cutoff,
                ),
                async {
                    musteragent_runtime_snapshot::application_catalog::resolve_tool_catalog(
                        runtime_snapshot,
                        self.application_catalogs.as_deref(),
                    )
                    .await
                    .map_err(application_material_error)
                },
            )?;
            if source.run_context.activity_kind == ActivityKind::Evaluation {
                // Keep the evaluator's own durable history while excluding sibling
                // execution transcripts; source evidence enters through its envelope.
                facts
                    .messages
                    .retain(|_, message| message.source_run_id == Some(source.run_context.run_id));
                facts
                    .model_completions
                    .retain(|_, completion| completion.run_id == Some(source.run_context.run_id));
            }
            let active_branch_id = facts.active_branch_id;
            runtime_snapshot
                .validate_resolved_tool_catalog(&resolved.catalog)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let (mut exposed_tool_keys, mut disclosed_skill_refs) =
                discovery_projection(runtime_snapshot, &facts, &resolved.catalog)?;
            let private_evaluation = source.run_context.activity_kind == ActivityKind::Evaluation
                && runtime_snapshot.uses_closed_evaluation_profile();
            let tool_policy = if private_evaluation {
                exposed_tool_keys.clear();
                disclosed_skill_refs.clear();
                ContextToolPolicy::DenyAll
            } else {
                ContextToolPolicy::SnapshotDiscovery
            };
            let (prompt_sections, disclosed_skills, memories, instincts, execution_plan) = tokio::try_join!(
                self.materialize_contents(
                    source.run_context.tenant_id,
                    runtime_snapshot
                        .prompt
                        .sections
                        .iter()
                        .map(|section| &section.content_ref),
                ),
                self.materialize_skills(
                    source.run_context.tenant_id,
                    &runtime_snapshot.skills,
                    &disclosed_skill_refs,
                ),
                self.materialize_memories(
                    source.run_context.tenant_id,
                    source.run_context.run_id,
                    &runtime_snapshot.memory.selected,
                ),
                self.materialize_instincts(
                    source.run_context.tenant_id,
                    &runtime_snapshot.instincts
                ),
                self.materialize_execution_plan(
                    source.run_context.tenant_id,
                    source.run_context.run_id
                ),
            )?;
            let evaluation = private_evaluation;
            let usage_anchor = if evaluation || isolated {
                None
            } else {
                self.load_usage_anchor(source.run_context.tenant_id, &facts)
                    .await?
            };
            let assistant_origins =
                load_assistant_origins(&self.pool, source.run_context.tenant_id, &facts).await?;
            let tool_exchanges = self
                .materialize_tool_exchanges(
                    source.run_context.tenant_id,
                    source.run_context.conversation_id,
                    &facts,
                    &assistant_origins,
                    provenance::pending_native_reply(
                        source.run_context.activity_kind,
                        source.run_context.run_id,
                        &runtime_snapshot.prompt.sections,
                        &facts,
                    ),
                )
                .await?;
            let has_run_local_message = facts
                .messages
                .values()
                .any(|message| message.source_run_id == Some(source.run_context.run_id));
            let retry_stimulus_record_id = facts
                .retry_source_message_id
                .map(|id| {
                    facts
                        .messages
                        .get(&id)
                        .filter(|message| message.role == CanonicalRole::User)
                        .map(|message| message.record_id)
                        .ok_or(PostgresContextMaterialError::CorruptStateAt(
                            "retry_stimulus",
                        ))
                })
                .transpose()?;
            // A stimulus whose message left the visible set (retraction,
            // evaluation isolation) drops its consumption cut with it.
            let consumed_stimulus_cuts: Vec<_> = std::mem::take(&mut facts.consumed_control_cuts)
                .into_iter()
                .filter_map(|(message_id, (consumed_sequence, kind))| {
                    facts
                        .messages
                        .get(&message_id)
                        .map(|message| ConsumedStimulusCut {
                            stimulus_record_id: message.record_id,
                            consumed_sequence,
                            kind,
                        })
                })
                .collect();
            let messages = std::mem::take(&mut facts.messages);
            let (mut entries, dialogue_entries) = tokio::try_join!(
                self.materialize_messages(
                    source.run_context.tenant_id,
                    messages,
                    &tool_exchanges.assistant_content_ids,
                    &facts.model_completions,
                    &assistant_origins,
                ),
                self.materialize_dialogue_history(input, dialogue_window.as_ref()),
            )?;

            if source.run_context.activity_kind == ActivityKind::Delegation {
                entries.push(
                    self.materialize_delegation_input(
                        source.run_context.tenant_id,
                        facts
                            .delegation_input
                            .take()
                            .ok_or(PostgresContextMaterialError::NoModelInput)?,
                    )
                    .await?,
                );
            }
            if source.run_context.activity_kind == ActivityKind::Coordination {
                entries.push(
                    self.materialize_coordination_input(
                        source.run_context.tenant_id,
                        facts
                            .coordination_input
                            .take()
                            .ok_or(PostgresContextMaterialError::NoModelInput)?,
                    )
                    .await?,
                );
            }
            if source.run_context.activity_kind == ActivityKind::Automation {
                if let Some(goal_input) = facts.standing_goal_input.take() {
                    entries.push(
                        self.materialize_standing_goal_input(
                            source.run_context.tenant_id,
                            goal_input,
                        )
                        .await?,
                    );
                } else if !has_run_local_message {
                    // Generic Automation is admitted with a Run-bound model input;
                    // a Standing Goal continuation is deliberately message-free
                    // and must instead carry its immutable context projection.
                    return Err(PostgresContextMaterialError::NoModelInput);
                }
            }
            if evaluation {
                entries.push(
                    self.materialize_standing_goal_judge_input(
                        source.run_context.tenant_id,
                        facts
                            .standing_goal_judge_input
                            .take()
                            .ok_or(PostgresContextMaterialError::NoModelInput)?,
                    )
                    .await?,
                );
            }
            entries.extend(
                self.delegation_report_history(&source.run_context, source.ledger_cutoff, &facts)
                    .await?,
            );
            entries.extend(dialogue_entries);
            entries.extend(tool_exchanges.entries);
            for entry in facts.approval_observations.entries() {
                let mut reachable = true;
                for id in &entry.source_record_ids {
                    reachable &= facts.record_reachable_from(*id, facts.active_branch_id)?;
                }
                if reachable {
                    entries.push(entry.clone());
                }
            }
            self.project_public_delivery_refs(&source.run_context, runtime_snapshot, &mut entries)
                .await?;
            entries.sort_by_key(|entry| entry.presentation_order);
            ensure_entry_provenance(&entries)?;
            let source_seq_from = entries
                .iter()
                .min_by_key(|entry| entry.source_sequence)
                .map(|entry| entry.source_sequence)
                .ok_or(PostgresContextMaterialError::NoModelInput)?;
            let source_seq_to = entries
                .iter()
                .max_by_key(|entry| entry.source_sequence)
                .map(|entry| entry.source_sequence)
                .ok_or(PostgresContextMaterialError::NoModelInput)?;
            let plan_selection = crate::context_plan::lifecycle::observe(
                &mut *self.pool.acquire().await?,
                source.run_context.tenant_id,
                source.run_context.run_id,
                input.next_iteration,
                ConversationSeq::new(source.ledger_cutoff)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                ConversationSeq::new(source.observed_ledger_head)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                // An unfinished, governed dialogue history page is sealed independently
                // of later runtime steering. Primary planning must first finish that page.
                ConversationSeq::new(if dialogue_window.as_ref().is_some_and(|w| !w.complete()) {
                    source.ledger_cutoff
                } else {
                    source.observed_ledger_head
                })
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            )
            .await
            .map_err(|error| match error {
                crate::PostgresContextPlanError::Database(error) => {
                    PostgresContextMaterialError::Database(error)
                }
                crate::PostgresContextPlanError::SourceChanged => {
                    PostgresContextMaterialError::StaleFence
                }
                _ => PostgresContextMaterialError::CorruptState,
            })?;
            let previous_plan = self
                .load_previous_plan(
                    source.run_context.tenant_id,
                    source.run_context.conversation_id,
                    source.run_context.run_id,
                    super::baseline::PlanPredecessor {
                        next_iteration: input.next_iteration,
                        source_seq_to,
                        same_iteration_predecessor: plan_selection
                            .lineage
                            .and_then(|l| l.predecessor)
                            .map(|p| p.snapshot_id),
                    },
                    &facts,
                )
                .await?;
            let previous_context = previous_plan.map(|plan| MaterializedContextBaseline {
                snapshot_id: plan.snapshot_id,
                source_seq_to: plan.source_seq_to,
                retained_record_ids: plan.retained_record_ids,
                summary: plan.summary,
            });
            let model_action_rejection = self
                .load_model_action_rejection(
                    source.run_context.tenant_id,
                    source.run_context.run_id,
                    ConversationSeq::new(source.ledger_cutoff)
                        .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                )
                .await?;
            let measured_usage = if musteragent_runtime_snapshot::uses_measured_context_usage(
                runtime_snapshot
                    .context_policy
                    .compaction_policy_ref
                    .as_str(),
            ) {
                self.load_measured_usage(
                    &source.run_context,
                    &entries,
                    previous_context.as_ref(),
                    facts
                        .model_recovery
                        .and_then(|id| assistant_origins.get(&id)),
                )
                .await?
            } else {
                None
            };
            let resolved_tools = musteragent_context::resolve_context_tools(
                runtime_snapshot,
                &resolved,
                self.application_catalogs.as_deref(),
                &exposed_tool_keys,
            )
            .await
            .map_err(application_material_error)?;
            Ok(ContextMaterial {
                derived_source_run_id: self.derived_source(&source.run_context).await?,
                superseding_control_id: source.superseding_control_id,
                resolved_tools,
                agent_instructions: None,
                desktop_source_host: None,
                dialogue_approval_context: None,
                dialogue_execution_context: None,
                plan_selection: Some(plan_selection),
                source_content_refs: Vec::new(),
                run_context: source.run_context,
                admitted_sequence: source.visibility.admitted_sequence,
                retry_stimulus_record_id,
                consumed_stimulus_cuts,
                runtime_snapshot: source.snapshot,
                tool_policy,
                exposed_tool_keys,
                prompt_sections,
                disclosed_skills,
                memories,
                instincts,
                execution_plan,
                delegation_completions: self.delegation_completions(&source.run_context).await?,
                model_action_rejection,
                context_capacity_exceeded: facts.context_capacity_exceeded,
                model_recovery: facts.model_recovery,
                rejected_request_bytes: self
                    .rejected_request_bytes(
                        &source.run_context,
                        facts.context_capacity_exceeded,
                        facts.primary_invocation_id,
                    )
                    .await?,
                measured_usage,
                entries,
                next_assistant_response_ordinal,
                usage_anchor,
                previous_context,
                active_branch_id,
                ledger_cutoff: ConversationSeq::new(source.ledger_cutoff)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                source_seq_from,
                source_seq_to,
            })
        })
    }
}
