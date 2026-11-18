//! Versioned validation remains in the original output/publication Owners. A parser
//! diagnosis cannot authorize a reference or make a candidate user-visible.
use super::*;
use musteragent_execution_context::{
    PublicDeliveryCandidateProof, PublicDeliveryContract, PublicDeliveryDiagnostic,
    PublicDeliveryDiagnosticKind, PublicDeliveryDraft, PublicDeliveryFormatRejection,
    PublicDeliveryIssue, PublicDeliveryRejectionV1, PublicDeliveryRepairDisposition,
    PublicDeliveryUnreplayableOutput, parse_public_delivery_v6, parse_public_delivery_v7,
};
use musteragent_identity::{ConversationSeq, VersionedKey};
use musteragent_runtime_content::verify_content;
use musteragent_runtime_session::{DialoguePublicationError, DialogueStoreError};
use std::collections::BTreeSet;

impl PostgresAgentLoopJournal {
    pub(super) async fn validate_versioned_delivery(
        &self,
        state: &JournalState,
        completion: &ModelCompletionCommit,
        source: &ContentRef,
        contract: PublicDeliveryContract,
    ) -> Result<Option<ModelActionRejection>, PostgresAgentLoopJournalError> {
        let cutoff = ConversationSeq::new(state.conversation_offset.get())
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let cut = crate::public_delivery_repair::cut(
            &mut *self.pool.acquire().await?,
            state.tenant_id,
            state.checkpoint.run_id,
            cutoff,
        )
        .await
        .map_err(repair_error)?;
        let contract_ref = VersionedKey::new(contract.key())
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let message = state
            .assistant_message_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let output = state
            .assistant_output_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        if source.byte_length > musteragent_execution_context::MAX_PUBLIC_DELIVERY_BYTES as u64 {
            return Ok(Some(
                ModelActionRejection::InvalidPublicDeliveryUnreplayable {
                    contract_ref,
                    output: PublicDeliveryUnreplayableOutput {
                        invocation_id: completion.invocation_id,
                        assistant_message_id: message,
                        assistant_output_id: output,
                        ledger_cutoff: cutoff,
                        byte_length: source.byte_length,
                        sha256: source.sha256,
                        cut,
                    },
                },
            ));
        }
        let bytes = self.content.read(state.tenant_id, source).await?;
        verify_content(source, &bytes).map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let candidate = PublicDeliveryCandidateProof {
            invocation_id: completion.invocation_id,
            assistant_message_id: message,
            assistant_output_id: output,
            ledger_cutoff: cutoff,
            content_ref: source.clone(),
        };
        let reject = |issue, diagnostics: Vec<PublicDeliveryDiagnostic>, prerequisite: bool| {
            if contract == PublicDeliveryContract::V7 && !prerequisite {
                let rejection = PublicDeliveryFormatRejection {
                    candidate: candidate.clone(),
                    diagnostics,
                    cut,
                };
                rejection
                    .validate()
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                return Ok(Some(ModelActionRejection::PublicDeliveryFormatRejected {
                    contract_ref: contract_ref.clone(),
                    issue,
                    rejection,
                }));
            }
            let disposition = if contract == PublicDeliveryContract::V7 {
                PublicDeliveryRepairDisposition::PrerequisiteRecovery
            } else {
                PublicDeliveryRepairDisposition::for_diagnostics(&diagnostics)
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
            };
            let repair = PublicDeliveryRejectionV1 {
                candidate: candidate.clone(),
                diagnostics,
                disposition,
                cut,
            };
            repair
                .validate_for(contract)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
            Ok(Some(
                ModelActionRejection::InvalidPublicDeliveryWithCandidate {
                    contract_ref: contract_ref.clone(),
                    issue,
                    repair,
                },
            ))
        };
        let decoded = match contract {
            PublicDeliveryContract::V6 => parse_public_delivery_v6(&bytes),
            PublicDeliveryContract::V7 => parse_public_delivery_v7(&bytes),
            _ => return Err(PostgresAgentLoopJournalError::CorruptState),
        };
        let drafts = match decoded {
            Ok(value) => value,
            Err(failure) => return reject(failure.issue, failure.diagnostics, false),
        };
        let store = crate::PostgresDialoguePublicationStore::new(self.ledger.database().clone());
        let mut artifacts = BTreeSet::new();
        let mut missing = Vec::new();
        let mut strict_objects = if contract == PublicDeliveryContract::V7 {
            Some(
                store
                    .resolve_candidate_objects(state.checkpoint.run_id, &drafts)
                    .await
                    .map_err(super::public_delivery::publication_error)?
                    .into_iter(),
            )
        } else {
            None
        };
        for (ordinal, draft) in drafts.iter().enumerate() {
            let resolved = if let Some(objects) = &mut strict_objects {
                match objects.next().ok_or(PostgresAgentLoopJournalError::CorruptState)? {
                    crate::dialogue_publication::candidate_resolution::CandidateObject::Available(object) => Ok(object),
                    crate::dialogue_publication::candidate_resolution::CandidateObject::ReferenceUnavailable => Err(DialoguePublicationError::Store(DialogueStoreError::Unavailable)),
                }
            } else {
                store
                    .resolve_candidate_object(state.checkpoint.run_id, &draft.content)
                    .await
            };
            let available = match resolved {
                Ok(Some(musteragent_execution_context::PublicDeliveryContent::Artifact {
                    artifact_id,
                    ..
                })) => {
                    if !artifacts.insert(artifact_id) {
                        // The authoritative object already exists. Repeating its
                        // card is a shape error, never permission to run tools.
                        let diagnostic = PublicDeliveryDiagnostic::new(
                            PublicDeliveryDiagnosticKind::InvalidValue,
                            "/deliveries",
                        )
                        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                        return reject(PublicDeliveryIssue::InvalidSchema, vec![diagnostic], false);
                    }
                    true
                }
                Ok(_) => true,
                Err(DialoguePublicationError::Store(DialogueStoreError::Unavailable)) => false,
                Err(error) => return Err(super::public_delivery::publication_error(error)),
            };
            if !available {
                let field = match &draft.content {
                    PublicDeliveryDraft::Interaction(_) => "interaction_id",
                    PublicDeliveryDraft::PartnerCreated { .. } => "creation_command_id",
                    PublicDeliveryDraft::ArtifactFromTool { .. }
                    | PublicDeliveryDraft::Artifact { .. } => "artifact_ref",
                    PublicDeliveryDraft::Message(_) => {
                        return Err(PostgresAgentLoopJournalError::CorruptState);
                    }
                };
                let diagnostic = PublicDeliveryDiagnostic::new(
                    PublicDeliveryDiagnosticKind::ReferenceUnavailable,
                    format!("/deliveries/{ordinal}/{field}"),
                )
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                if contract == PublicDeliveryContract::V6 {
                    return reject(
                        PublicDeliveryIssue::InvalidReference,
                        vec![diagnostic],
                        true,
                    );
                }
                // Diagnostics are bounded display evidence, never a decision
                // shortcut: every resolved object is checked before recovery.
                if missing.len() < musteragent_execution_context::MAX_DELIVERY_DIAGNOSTICS {
                    missing.push(diagnostic);
                }
            }
        }
        if !missing.is_empty() {
            return reject(PublicDeliveryIssue::InvalidReference, missing, true);
        }
        Ok(None)
    }
}
fn repair_error(
    error: crate::public_delivery_repair::RepairStoreError,
) -> PostgresAgentLoopJournalError {
    match error {
        crate::public_delivery_repair::RepairStoreError::Database(error) => error.into(),
        crate::public_delivery_repair::RepairStoreError::Changed => {
            PostgresAgentLoopJournalError::CheckpointConflict
        }
        crate::public_delivery_repair::RepairStoreError::Corrupt => {
            PostgresAgentLoopJournalError::CorruptState
        }
    }
}

/// Insert the typed record and its retained ContentRef in the same fenced
/// transaction that closes the exact physical invocation.
pub(super) async fn persist(
    conn: &mut sqlx::PgConnection,
    state: &JournalState,
    invocation: InvocationId,
    rejection: &ModelActionRejection,
    now: UtcTimestamp,
) -> Result<bool, PostgresAgentLoopJournalError> {
    use musteragent_persistence_codec::public_delivery_repair_codec as codec;
    if !matches!(
        rejection,
        ModelActionRejection::InvalidPublicDeliveryWithCandidate { .. }
            | ModelActionRejection::InvalidPublicDeliveryUnreplayable { .. }
            | ModelActionRejection::PublicDeliveryFormatRejected { .. }
            | ModelActionRejection::PublicDeliveryRepairViolation { .. }
    ) {
        return Ok(false);
    }
    let contract = super::dialogue_contract::load(conn, state).await?;
    let (
        kind,
        bytes,
        issue,
        source,
        message,
        output,
        cutoff,
        content,
        hash,
        length,
        cut,
        violation,
    ) = match rejection {
        ModelActionRejection::InvalidPublicDeliveryWithCandidate {
            contract_ref,
            issue,
            repair,
        } => {
            if contract_ref.as_str() != contract.key()
                || repair.candidate.invocation_id != invocation
            {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
            repair
                .validate_for(contract)
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
            let p = &repair.candidate;
            (
                "candidate",
                codec::encode_public_delivery_rejection(repair),
                *issue,
                p.invocation_id,
                p.assistant_message_id,
                p.assistant_output_id,
                p.ledger_cutoff,
                Some(&p.content_ref),
                p.content_ref.sha256,
                p.content_ref.byte_length,
                repair.cut,
                None,
            )
        }
        ModelActionRejection::PublicDeliveryFormatRejected {
            contract_ref,
            issue,
            rejection,
        } => {
            if contract != PublicDeliveryContract::V7
                || contract_ref.as_str() != contract.key()
                || !matches!(
                    issue,
                    PublicDeliveryIssue::InvalidSchema | PublicDeliveryIssue::TooLarge
                )
                || rejection.candidate.invocation_id != invocation
            {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
            let p = &rejection.candidate;
            (
                "format_terminal",
                codec::encode_public_delivery_format_rejection(rejection),
                *issue,
                p.invocation_id,
                p.assistant_message_id,
                p.assistant_output_id,
                p.ledger_cutoff,
                Some(&p.content_ref),
                p.content_ref.sha256,
                p.content_ref.byte_length,
                rejection.cut,
                None,
            )
        }
        ModelActionRejection::InvalidPublicDeliveryUnreplayable {
            contract_ref,
            output: p,
        } => {
            if contract_ref.as_str() != contract.key() || p.invocation_id != invocation {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
            (
                "unreplayable",
                codec::encode_public_delivery_unreplayable(p),
                PublicDeliveryIssue::TooLarge,
                p.invocation_id,
                p.assistant_message_id,
                p.assistant_output_id,
                p.ledger_cutoff,
                None,
                p.sha256,
                p.byte_length,
                p.cut,
                None,
            )
        }
        ModelActionRejection::PublicDeliveryRepairViolation { binding, violation } => {
            if contract != PublicDeliveryContract::V6
                || binding.disposition != PublicDeliveryRepairDisposition::FormatOnly
            {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
            use musteragent_execution_context::PublicDeliveryRepairViolation;
            match violation {
                PublicDeliveryRepairViolation::EmptyCompletion
                    if state.next_assistant_byte_offset != 0
                        || !matches!(
                            state.terminal_stop_reason,
                            Some(
                                musteragent_runtime_model::StopReason::Stop
                                    | musteragent_runtime_model::StopReason::ToolCalls
                            )
                        ) =>
                {
                    return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
                }
                PublicDeliveryRepairViolation::IncompleteCompletion
                    if !state
                        .terminal_stop_reason
                        .is_some_and(|reason| reason.is_token_limited()) =>
                {
                    return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
                }
                _ => {}
            }
            validate_repair_plan(conn, state, invocation, binding).await?;
            let p = &binding.candidate;
            (
                "violation",
                codec::encode_public_delivery_repair_binding(binding),
                PublicDeliveryIssue::InvalidSchema,
                p.invocation_id,
                p.assistant_message_id,
                p.assistant_output_id,
                p.ledger_cutoff,
                Some(&p.content_ref),
                p.content_ref.sha256,
                p.content_ref.byte_length,
                binding.cut,
                Some(violation.as_str()),
            )
        }
        _ => return Ok(false),
    };
    let bytes = bytes.map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
    if !matches!(
        contract,
        PublicDeliveryContract::V6 | PublicDeliveryContract::V7
    ) || cutoff.get() > state.conversation_offset.get()
        || crate::public_delivery_repair::cut(
            conn,
            state.tenant_id,
            state.checkpoint.run_id,
            ConversationSeq::new(state.conversation_offset.get())
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
        )
        .await
        .map_err(repair_error)?
            != cut
    {
        return Err(PostgresAgentLoopJournalError::CheckpointConflict);
    }
    if let Some(content) = content {
        crate::dialogue_codec::lock_ready_content_reference(conn, state.tenant_id, content)
            .await
            .map_err(|error| {
                super::public_delivery::publication_error(DialoguePublicationError::Store(error))
            })?;
    }
    sqlx::query("INSERT INTO runtime.model_action_rejections(
        invocation_id,error_code,attempted_wire_name,validation_issues,rejected_at_ms,
        rejection_kind,delivery_contract_key,delivery_issue,repair_kind,repair_record,repair_violation,
        repair_source_invocation_id,repair_message_id,repair_output_id,repair_cutoff,repair_content_id,
        repair_content_hash,repair_content_bytes,repair_progress_sequence,repair_control_sequence)
        VALUES($1,'model_response_invalid',NULL,'[]'::jsonb,$2,'public_delivery',$16,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)")
        .bind(invocation.into_uuid()).bind(now.unix_millis()).bind(issue.as_str()).bind(kind).bind(bytes).bind(violation)
        .bind(source.into_uuid()).bind(message.into_uuid()).bind(output.into_uuid()).bind(cutoff.get().to_string().parse::<i64>().map_err(|_|PostgresAgentLoopJournalError::NumericRange)?)
        .bind(content.map(|c|c.content_id.into_uuid())).bind(hash.into_bytes().as_slice()).bind(i64::try_from(length).map_err(|_|PostgresAgentLoopJournalError::NumericRange)?)
        .bind(cut.progress_sequence.map(|s|s.get() as i64)).bind(cut.control_sequence.map(|s|s.get() as i64))
        .bind(contract.key()).execute(&mut *conn).await?;
    Ok(true)
}

pub(super) async fn validate_repair_plan(
    conn: &mut sqlx::PgConnection,
    state: &JournalState,
    invocation: InvocationId,
    binding: &musteragent_execution_context::PublicDeliveryRepairBinding,
) -> Result<(), PostgresAgentLoopJournalError> {
    let stored = crate::public_delivery_repair::stored_plan_binding(
        conn,
        state.tenant_id,
        state.checkpoint.run_id,
        invocation,
    )
    .await
    .map_err(repair_error)?;
    if stored.as_ref() != Some(binding) {
        return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
    }
    let cutoff = ConversationSeq::new(state.conversation_offset.get())
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    crate::public_delivery_repair::validate_binding(
        conn,
        state.tenant_id,
        state.checkpoint.run_id,
        binding,
        cutoff,
    )
    .await
    .map_err(repair_error)
}

pub(super) async fn validate_model_begin(
    journal: &PostgresAgentLoopJournal,
    conn: &mut sqlx::PgConnection,
    state: &JournalState,
    iteration: &ModelIteration,
) -> Result<(), PostgresAgentLoopJournalError> {
    if !state.dialogue_model_output
        || !matches!(
            super::dialogue_contract::load_output(conn, state)
                .await?
                .response_contract(),
            Some(PublicDeliveryContract::V6 | PublicDeliveryContract::V7)
        )
    {
        return if iteration.delivery_repair.is_none() {
            Ok(())
        } else {
            Err(PostgresAgentLoopJournalError::InvalidModelBatch)
        };
    }
    let stored = crate::public_delivery_repair::stored_plan_binding(
        conn,
        state.tenant_id,
        state.checkpoint.run_id,
        iteration.invocation_id,
    )
    .await
    .map_err(repair_error)?;
    if stored != iteration.delivery_repair {
        return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
    }
    let cutoff = ConversationSeq::new(state.conversation_offset.get())
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    let expected = crate::public_delivery_repair::current_binding(
        conn,
        state.tenant_id,
        state.checkpoint.run_id,
        cutoff,
    )
    .await
    .map_err(repair_error)?;
    if expected != stored {
        return Err(PostgresAgentLoopJournalError::CheckpointConflict);
    }
    if let Some(binding) = stored {
        validate_repair_plan(conn, state, iteration.invocation_id, &binding).await?;
        let (_, repair) = crate::public_delivery_repair::load_candidate(
            conn,
            state.tenant_id,
            state.checkpoint.run_id,
            binding.rejection_invocation_id,
        )
        .await
        .map_err(repair_error)?;
        let candidate = journal
            .content
            .read(state.tenant_id, &binding.candidate.content_ref)
            .await?;
        verify_content(&binding.candidate.content_ref, &candidate)
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let material = crate::public_delivery_repair::authoritative_material(
            journal.ledger.database().clone(),
            journal.content.clone(),
            journal.decoder.clone(),
            &iteration.request,
            iteration.iteration,
            journal.application_catalogs.clone(),
        )
        .await
        .map_err(|error| match error {
            crate::PostgresContextMaterialError::Database(error) => {
                PostgresAgentLoopJournalError::Database(error)
            }
            crate::PostgresContextMaterialError::Transaction(error) => {
                PostgresAgentLoopJournalError::Transaction(error)
            }
            crate::PostgresContextMaterialError::Content(error) => {
                PostgresAgentLoopJournalError::Content(error)
            }
            crate::PostgresContextMaterialError::StaleFence => {
                PostgresAgentLoopJournalError::StaleAttempt
            }
            _ => PostgresAgentLoopJournalError::CorruptState,
        })?;
        musteragent_context::validate_public_delivery_repair_request(
            &repair,
            &candidate,
            &material,
            &iteration.request,
        )
        .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
    }
    Ok(())
}
