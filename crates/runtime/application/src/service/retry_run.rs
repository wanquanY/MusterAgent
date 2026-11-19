use super::*;
use crate::RunDiscoveryQuery;

impl RuntimeApplication {
    pub(super) async fn retry_run_inner(
        &self,
        command: CommandEnvelope<RetryRun>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| invalid_retry(&command))?;
        let validation = self.home_policy.validate_execution_scope(
            command.metadata.actor.tenant_id,
            command.payload.execution_scope,
        );
        validation.map_err(|_| invalid_retry(&command))?;
        if !selection.requested_skill_refs.is_empty() {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                command.metadata.correlation_id,
                false,
                "Retry Run cannot change the frozen Skill selection",
            ));
        }
        let guard = command
            .metadata
            .conversation_guard
            .ok_or_else(|| invalid_retry(&command))?;
        let command_fingerprint = retry_run_command_fingerprint(&command, &selection);
        let ids = admission_identities_for_command(&command).ok_or_else(|| invariant(&command))?;
        {
            let replay = self
                .store
                .load_admission_replay(
                    CommandReplayQuery {
                        tenant_id: command.metadata.actor.tenant_id,
                        conversation_id: guard.conversation_id,
                        home_epoch: guard.expected_home_epoch,
                        idempotency_key: command.metadata.idempotency_key.clone(),
                        command_fingerprint,
                    },
                    ids.runtime_snapshot_id,
                    command.metadata.correlation_id,
                )
                .await
                .map_err(|error| trace_admission_stage_error("retry_admission_replay", error))?;
            if let Some(replay) = replay {
                self.store
                    .get_conversation(command.metadata.actor, guard.conversation_id)
                    .await?;
                return Ok(duplicate_ack(
                    command.metadata.command_id,
                    replay.ledger,
                    vec![
                        ResourceRef::Activity(ids.activity_id),
                        ResourceRef::Run(ids.run_id),
                    ],
                ));
            }
        }
        let now = self.clock.now();
        if let Some(error) = self
            .load_admission_denial(
                command.metadata.actor,
                command.metadata.command_id,
                guard.conversation_id,
                command_fingerprint,
                command.metadata.correlation_id,
            )
            .await
            .map_err(|error| trace_admission_stage_error("retry_admission_denial_read", error))?
        {
            return Err(error);
        }

        let source = match self
            .store
            .get_run(command.metadata.actor, command.payload.source_run_id)
            .await
        {
            Ok(source) => source,
            Err(error) => {
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        let expected_next_attempt_value = command
            .payload
            .expected_attempt_number
            .get()
            .checked_add(1)
            .ok_or_else(|| invalid_retry(&command))?;
        let expected_next_attempt =
            AttemptNumber::new(expected_next_attempt_value).map_err(|_| invalid_retry(&command))?;
        if source.run.context.tenant_id != command.metadata.actor.tenant_id
            || source.run.context.conversation_id != guard.conversation_id
            || source.run.context.activity_kind != ActivityKind::Chat
            || source.run.context.requested_by_participant_id != command.payload.participant_id
            || source.run.context.execution_scope != command.payload.execution_scope
            || source.run.state.status() != RunStatus::Failed
            || source.run.next_attempt_number != expected_next_attempt
            || source.active_attempt.is_some()
            || !source.pending_interaction_ids.is_empty()
        {
            let error = runtime_error(
                ErrorCode::PolicyDenied,
                command.metadata.correlation_id,
                false,
                "Retry Run source is not the expected terminal failed Chat Run",
            );
            return Err(self
                .record_submission_denial(&command, command_fingerprint, now, error)
                .await);
        }

        let authority = match self
            .store
            .authorize_admission_for_home(AdmissionAuthorizationRequest {
                actor: command.metadata.actor,
                conversation_id: guard.conversation_id,
                participant_id: command.payload.participant_id,
                activity_kind: ActivityKind::Chat,
                expected_home_epoch: guard.expected_home_epoch,
                expected_control_home: self.home_policy.control_home(),
                correlation_id: command.metadata.correlation_id,
                audit_context: AdmissionAuditContext {
                    command_id: command.metadata.command_id,
                    trace_context: command.metadata.trace_context.clone(),
                    occurred_at: now,
                },
            })
            .await
        {
            Ok(authority) => authority,
            Err(error) => {
                let error = trace_admission_stage_error("retry_admission_authority", error);
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        let expected_offset = guard
            .expected_conversation_seq
            .map_or(authority.current_offset, |value| {
                LedgerOffset::new(value.get())
            });

        let execution_home = self.home_policy.execution_home();
        let run_context = RunContext {
            tenant_id: authority.tenant_id,
            conversation_id: authority.conversation_id,
            activity_id: ids.activity_id,
            activity_kind: ActivityKind::Chat,
            run_id: ids.run_id,
            executor_participant_id: authority.executor_participant_id,
            requested_by_participant_id: authority.requested_by_participant_id,
            parent_run_id: Some(command.payload.source_run_id),
            delegation_id: None,
            execution_scope: command.payload.execution_scope,
            control_home: self.home_policy.control_home(),
            execution_home,
            home_epoch: authority.home_epoch,
            runtime_snapshot_id: ids.runtime_snapshot_id,
        };
        let source_snapshot = match self
            .store
            .load_run_runtime_snapshot(
                command.metadata.actor.tenant_id,
                guard.conversation_id,
                command.payload.source_run_id,
                source.run.context.runtime_snapshot_id,
                None,
                command.metadata.correlation_id,
            )
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let error = trace_admission_stage_error("retry_source_snapshot", error);
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        if source_snapshot.model.requested_model_class != command.payload.requested_model_class {
            let error = runtime_error(
                ErrorCode::PolicyDenied,
                command.metadata.correlation_id,
                false,
                "Retry Run cannot change the source Run model configuration",
            );
            return Err(self
                .record_submission_denial(&command, command_fingerprint, now, error)
                .await);
        }
        let source_discovery = match self
            .store
            .load_run_discovery(
                RunDiscoveryQuery {
                    tenant_id: authority.tenant_id,
                    conversation_id: authority.conversation_id,
                    home_epoch: authority.home_epoch,
                    source_run_id: command.payload.source_run_id,
                    lineage_head_branch_id: authority.active_context_branch_id,
                    through_offset: authority.current_offset,
                },
                command.metadata.correlation_id,
            )
            .await
        {
            Ok(discovery) => discovery,
            Err(error) => {
                let error = trace_admission_stage_error("retry_source_discovery", error);
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        let execution_grant = match self.grant_authenticator.authenticate(&signed_grant) {
            Ok(grant) => grant,
            Err(code) => {
                let error = runtime_error(
                    code,
                    command.metadata.correlation_id,
                    false,
                    "Retry Run execution grant authentication failed",
                );
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        let (identity_binding, runtime_audience) = execution_grant_identity_binding(
            authority.tenant_id,
            command.metadata.actor.principal_id,
            command.payload.execution_scope,
        )
        .map(|binding| (binding, self.home_policy.runtime_audience().clone()))
        .map_err(|_| invalid_retry(&command))?;
        if execution_grant
            .authorize(&GrantAuthorizationRequest {
                runtime_audience,
                tenant_id: authority.tenant_id,
                identity_binding,
                run_id: ids.run_id,
                model_provider_id: source_snapshot.model.route.model_provider_id,
                model_entry_id: source_snapshot.model.route.model_entry_id,
                now,
            })
            .is_err()
        {
            let error = runtime_error(
                ErrorCode::PolicyDenied,
                command.metadata.correlation_id,
                false,
                "Execution Grant is not authorized for the source Retry Run configuration",
            );
            return Err(self
                .record_submission_denial(&command, command_fingerprint, now, error)
                .await);
        }
        let source_snapshot = match rebind_retry_snapshot_authority(
            source_snapshot,
            &execution_grant,
            execution_home,
            command.metadata.correlation_id,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        let source_snapshot = match crate::snapshot::rebind_retry_discovery(
            source_snapshot,
            source_discovery.as_ref(),
            command.payload.source_run_id,
            None,
            authority.tenant_id,
            self.content.as_ref(),
            command.metadata.correlation_id,
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let error = trace_admission_stage_error("retry_discovery_rebind", error);
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        if let Err(error) = self
            .snapshots
            .retain_retry_catalog(
                command.metadata.actor,
                command.payload.source_run_id,
                ids.run_id,
                &source_snapshot,
                command.metadata.correlation_id,
            )
            .await
        {
            let error = trace_admission_stage_error("retry_component_catalog", error);
            return Err(self
                .record_submission_denial(&command, command_fingerprint, now, error)
                .await);
        }
        let resolved = match self
            .snapshots
            .reseal_retry(
                authority.tenant_id,
                ids.runtime_snapshot_id,
                source_snapshot,
                now,
                command.metadata.correlation_id,
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => {
                let error = trace_admission_stage_error("retry_snapshot_reseal", error);
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        if resolved.snapshot.runtime_snapshot_id != ids.runtime_snapshot_id
            || resolved.snapshot.created_at != now
            || resolved.snapshot.content.model.requested_model_class
                != command.payload.requested_model_class
        {
            return Err(invariant(&command));
        }
        let execution_grant_proof =
            self.execution_grant_proof(&signed_grant, command.metadata.correlation_id)?;

        let activity = Activity {
            activity_id: ids.activity_id,
            conversation_id: authority.conversation_id,
            kind: ActivityKind::Chat,
            requested_by_participant_id: authority.requested_by_participant_id,
            parent_activity_id: None,
            root_activity_id: ids.activity_id,
            status: ActivityStatus::Active,
            policy_snapshot_ref: resolved.snapshot.content.policy.policy_ref.clone(),
            created_at: now,
            completed_at: None,
        };
        let run = Run {
            context: run_context,
            state: RunState::queued(),
            next_attempt_number: AttemptNumber::new(1).map_err(|_| invariant(&command))?,
            not_before: None,
            cancel_requested_at: None,
            admitted_at: now,
            started_at: None,
            finished_at: None,
            terminal_error: None,
        };
        let records = vec![
            domain_record(
                self.encoder.as_ref(),
                RecordIdentity {
                    record_id: ids.activity_record_id,
                    activity_id: Some(ids.activity_id),
                    run_id: None,
                    run_seq: None,
                    participant_id: Some(authority.requested_by_participant_id),
                },
                DomainEvent::Activity(ActivityEvent::Started {
                    kind: ActivityKind::Chat,
                    root_activity_id: ids.activity_id,
                    parent_activity_id: None,
                    policy_snapshot_ref: activity.policy_snapshot_ref.clone(),
                }),
                now,
                command.metadata.correlation_id,
            )
            .map_err(|code| build_error(code, &command))?,
            domain_record(
                self.encoder.as_ref(),
                RecordIdentity {
                    record_id: ids.run_record_id,
                    activity_id: Some(ids.activity_id),
                    run_id: Some(ids.run_id),
                    run_seq: Some(RunSeq::new(1).map_err(|_| invariant(&command))?),
                    participant_id: Some(authority.executor_participant_id),
                },
                DomainEvent::Run(RunEvent::Admitted {
                    runtime_snapshot_id: ids.runtime_snapshot_id,
                    execution_home,
                    parent_run_id: Some(command.payload.source_run_id),
                    delegation_id: None,
                    input_message_id: Some(command.payload.source_message_id),
                    retry_source_message_id: Some(command.payload.source_message_id),
                    retry_source_attempt_number: Some(command.payload.expected_attempt_number),
                }),
                now,
                command.metadata.correlation_id,
            )
            .map_err(|code| build_error(code, &command))?,
        ];
        let dispatch = format!("run:{}", ids.run_id).into_bytes();
        let append = AppendRequest {
            tenant_id: authority.tenant_id,
            conversation_id: authority.conversation_id,
            expected_home_epoch: authority.home_epoch,
            expected_offset,
            idempotency_key: command.metadata.idempotency_key.clone(),
            command_fingerprint,
            records,
            outbox: vec![ProposedOutbox {
                message_id: ids.dispatch_id,
                topic: DottedKey::new(musteragent_ledger::run_dispatch_topic(&execution_grant))
                    .map_err(|_| invariant(&command))?,
                payload_hash: Sha256::digest(&dispatch),
                payload: dispatch,
            }],
        };
        let memory_selection = resolved.snapshot.content.memory.clone();
        let instinct_selections = resolved.snapshot.content.instincts.clone();
        let memory_agent_id = resolved.snapshot.content.agent.agent_id;
        let batch = RetryAdmissionBatch {
            actor: command.metadata.actor,
            trace_context: command.metadata.trace_context.clone(),
            append,
            activity_record_id: ids.activity_record_id,
            run_record_id: ids.run_record_id,
            source_run_id: command.payload.source_run_id,
            source_message_id: command.payload.source_message_id,
            source_attempt_number: command.payload.expected_attempt_number,
            activity,
            run,
            runtime_snapshot: RuntimeSnapshotBlob {
                runtime_snapshot_id: ids.runtime_snapshot_id,
                schema_version: resolved.snapshot.content.schema_version.clone(),
                encoded_snapshot: resolved.snapshot.encoded_content,
                content_hash: resolved.snapshot.content_hash,
                created_at: resolved.snapshot.created_at,
                memory_selection,
                instinct_selections,
                memory_agent_id,
                execution_grant,
                execution_grant_proof,
            },
            sandbox_routing: selection.sandbox_routing,
        };
        let outcome = self
            .store
            .admit_retry(command.metadata.correlation_id, batch)
            .await;
        let receipt = match outcome {
            Ok(receipt) => receipt,
            Err(error) => {
                let error = trace_admission_stage_error("retry_admission_durable_commit", error);
                return Err(self
                    .record_submission_denial(&command, command_fingerprint, now, error)
                    .await);
            }
        };
        Ok(CommandAck {
            command_id: command.metadata.command_id,
            disposition: match receipt.disposition {
                AppendDisposition::Committed => CommandDisposition::Committed,
                AppendDisposition::Duplicate => CommandDisposition::Duplicate,
            },
            committed_at: receipt.committed_at,
            first_conversation_seq: Some(receipt.first_sequence),
            last_conversation_seq: Some(receipt.last_sequence),
            resource_refs: vec![
                ResourceRef::Activity(ids.activity_id),
                ResourceRef::Run(ids.run_id),
            ],
        })
    }
}

/// Rebinds only the fresh credential identities required by the new Run.
/// Every executable or policy-bearing source fact must still be authorized by
/// the new signed Grant byte-semantically; catalog drift cannot silently turn
/// a retry into a differently configured execution.
fn rebind_retry_snapshot_authority(
    mut source: musteragent_runtime_snapshot::RuntimeSnapshotContent,
    grant: &ExecutionGrant,
    execution_home: musteragent_event_envelope::RuntimeHome,
    correlation_id: musteragent_identity::CorrelationId,
) -> Result<musteragent_runtime_snapshot::RuntimeSnapshotContent, RuntimeError> {
    if let Some(reference) = &grant.product_release {
        musteragent_tool::require_product_release_admission(reference).map_err(|_| {
            runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Historical evidence publication cannot authorize a new Retry Run",
            )
        })?;
    }
    if source.product_release.is_none()
        && let Some(reference) = &grant.product_release
    {
        // This RetryRun owner is the Work conversation command. Owner-scoped tasks
        // resume through their source-bound WorkItem/Intent owner.
        source = source
            .qualify_legacy_retry(reference, musteragent_identity::ProductId::KERNEL)
            .map_err(|_| {
                runtime_error(
                    ErrorCode::PolicyDenied,
                    correlation_id,
                    false,
                    "Legacy retry contracts are not qualified by this Work publication",
                )
            })?;
    }
    let runtime = match execution_home {
        musteragent_event_envelope::RuntimeHome::Cloud => crate::ProductSnapshotRuntime::Cloud,
        musteragent_event_envelope::RuntimeHome::Device { .. } => {
            crate::ProductSnapshotRuntime::Device
        }
    };
    let capabilities = grant
        .capability_entitlements
        .iter()
        .filter(|entitlement| {
            crate::capability_available_on_runtime(runtime, entitlement.key.as_str())
        })
        .filter_map(|entitlement| entitlement.grant_snapshot())
        .collect::<Vec<_>>();
    let fallbacks_match = source.model.fallback_candidates.len()
        == grant.fallback_model_routes.len()
        && source
            .model
            .fallback_candidates
            .iter()
            .zip(&grant.fallback_model_routes)
            .all(|(snapshot, granted)| snapshot.route == *granted);
    let auxiliary_matches = source.auxiliary_models.len() == grant.auxiliary_model_routes.len()
        && source
            .auxiliary_models
            .iter()
            .zip(&grant.auxiliary_model_routes)
            .all(|(snapshot, granted)| {
                snapshot.role == granted.role && snapshot.model.route == granted.route
            });
    let agent_contract_matches = source.agent.resolved_v5.as_ref().is_some_and(|resolved| {
        grant.agent_contract.as_ref() == Some(&resolved.supported_runtime_contract)
    });
    if source.product_release != grant.product_release
        || source.model.requested_model_class != grant.requested_model_class
        || source.model.route != grant.model_route
        || !fallbacks_match
        || !auxiliary_matches
        || source.capabilities != capabilities
        || grant.permission_mode.as_ref() != Some(&source.policy.permission_mode)
        || !agent_contract_matches
    {
        return Err(runtime_error(
            ErrorCode::PolicyDenied,
            correlation_id,
            false,
            "Retry Run grant no longer authorizes the exact source Runtime configuration",
        ));
    }

    source.model.credential_grant_ref = grant.grant_ref.clone();
    for fallback in &mut source.model.fallback_candidates {
        fallback.credential_grant_ref = grant.grant_ref.clone();
    }
    for (snapshot, granted) in source
        .auxiliary_models
        .iter_mut()
        .zip(&grant.auxiliary_model_routes)
    {
        snapshot.model.credential_grant_ref = granted.credential_grant_ref.clone();
    }
    source.with_explicit_retry_recovery().map_err(|_| {
        runtime_error(
            ErrorCode::InternalInvariantViolated,
            correlation_id,
            false,
            "Retry Run Snapshot recovery transition violates its immutable contract",
        )
    })
}
