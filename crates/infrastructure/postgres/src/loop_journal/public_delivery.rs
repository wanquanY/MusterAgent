//! Explicit model candidates are recorded in the original completion commit.
//! No public HTTP entry point, event-log text scraping, or second Run owner.
use super::*;
use musteragent_execution_context::*;
use musteragent_identity::CommandId;
use musteragent_runtime_session::{
    DialoguePublicationError, DialoguePublicationStore, PublicEntryAppend,
};

pub(super) struct PreparedPublicDeliveries {
    source: ContentRef,
    contract: PublicDeliveryContract,
    entries: Vec<(DialoguePublicationPurpose, PublicDeliveryContent)>,
}

pub(super) async fn validate_request(
    conn: &mut sqlx::PgConnection,
    state: &JournalState,
    iteration: &ModelIteration,
    request_bytes: &[u8],
) -> Result<(), PostgresAgentLoopJournalError> {
    let dialogue: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_sessions s JOIN conversation.conversations c USING(conversation_id) WHERE s.conversation_id=$1 AND s.tenant_id=$2 AND c.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report','owner_goal_execution') AND c.created_by=s.owner_id)")
        .bind(state.conversation_id.into_uuid()).bind(state.tenant_id.into_uuid()).fetch_one(&mut *conn).await?;
    let dialogue = dialogue && state.activity_kind != 6;
    let protocol = if dialogue {
        Some(super::dialogue_contract::load_output(conn, state).await?)
    } else {
        None
    };
    let expected = protocol
        .and_then(RunOutputContract::response_contract)
        .map(|contract| {
            musteragent_context::public_delivery_schema_for(
                contract,
                &musteragent_persistence_codec::CanonicalJsonContextCodec,
            )
        })
        .transpose()
        .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
    let required_capability = match protocol {
        Some(contract) if contract.response_contract().is_none() => "tools",
        _ => "json_schema_output",
    };
    use musteragent_context::ContextCodec as _;
    if dialogue
        && musteragent_persistence_codec::CanonicalJsonContextCodec
            .encode_model_request(&iteration.request)
            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?
            != request_bytes
    {
        return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
    }
    if dialogue != state.dialogue_model_output
        || (dialogue
            && (iteration.request.response_schema.as_ref() != expected.as_ref()
                || !iteration
                    .request
                    .route
                    .capabilities
                    .iter()
                    .any(|c| c.as_str() == required_capability)))
        || (!dialogue
            && iteration
                .request
                .response_schema
                .as_ref()
                .is_some_and(|(s, _)| {
                    PublicDeliveryContract::from_key(s.schema_key.as_str()).is_some()
                }))
    {
        return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
    }
    Ok(())
}

impl PostgresAgentLoopJournal {
    pub(super) async fn has_committed_dialogue_completion(
        &self,
        state: &JournalState,
    ) -> Result<bool, PostgresAgentLoopJournalError> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.public_delivery_candidates c JOIN runtime.model_invocations i ON i.invocation_id=c.invocation_id AND i.tenant_id=c.tenant_id AND i.run_id=c.run_id JOIN runtime.context_plans p ON p.invocation_id=i.invocation_id AND p.tenant_id=i.tenant_id AND p.run_id=i.run_id WHERE c.run_id=$1 AND c.tenant_id=$2 AND p.iteration=$3 AND i.status=2)")
            .bind(state.checkpoint.run_id.into_uuid()).bind(state.tenant_id.into_uuid()).bind(i64::from(state.checkpoint.iteration)).fetch_one(&self.pool).await?)
    }

    pub(super) async fn validate_dialogue_completion(
        &self,
        attempt: &AttemptContext,
        completion: &ModelCompletionCommit,
    ) -> Result<Option<ModelActionRejection>, PostgresAgentLoopJournalError> {
        let state = self.load_state(attempt).await?;
        if !state.dialogue_model_output
            || completion.kind != ModelCompletionKind::AssistantResponse
            || !matches!(state.checkpoint.phase, LoopPhase::ModelInFlight { invocation_id } if invocation_id==completion.invocation_id)
        {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        let observed = state.model_observation(completion.invocation_id);
        if observed.provider_response_ref.as_ref() != Some(&completion.provider_response_ref)
            || observed.response_model_key.as_ref() != Some(&completion.response_model_key)
            || observed.usage != completion.usage
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let Some(contract) =
            super::dialogue_contract::load_output(&mut *self.pool.acquire().await?, &state)
                .await?
                .response_contract()
        else {
            // Native output is private prose. It is never a candidate payload.
            return Ok(None);
        };
        let rejection = |issue| {
            Some(ModelActionRejection::InvalidPublicDelivery {
                contract_ref: musteragent_identity::VersionedKey::new(contract.key())
                    .expect("static contract"),
                issue,
            })
        };
        let output = state
            .assistant_output_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let bytes = self
            .load_output_bytes(&state, completion.invocation_id, output)
            .await?;
        let segment = self
            .content
            .put(PutContent {
                tenant_id: state.tenant_id,
                media_type: MediaType::new("text/plain;charset=utf-8")
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                bytes,
            })
            .await?;
        let message = state
            .assistant_message_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let source = self
            .final_assistant_message_ref(&state, message, message, &segment)
            .await?;
        if matches!(
            contract,
            musteragent_execution_context::PublicDeliveryContract::V6
                | musteragent_execution_context::PublicDeliveryContract::V7
        ) {
            return self
                .validate_versioned_delivery(&state, completion, &source, contract)
                .await;
        }
        if source.byte_length > MAX_PUBLIC_DELIVERY_BYTES as u64 {
            return Ok(rejection(PublicDeliveryIssue::TooLarge));
        }
        let bytes = self.content.read(state.tenant_id, &source).await?;
        verify_content(&source, &bytes).map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let drafts = match contract.parse(&bytes) {
            Ok(value) => value,
            Err(error) => return Ok(rejection(error.into())),
        };
        let store = crate::PostgresDialoguePublicationStore::new(self.ledger.database().clone());
        let mut artifacts = BTreeSet::new();
        for draft in &drafts {
            match store
                .resolve_candidate_object(attempt.run_id, &draft.content)
                .await
            {
                Ok(Some(PublicDeliveryContent::Artifact { artifact_id, .. })) => {
                    if !artifacts.insert(artifact_id) {
                        return Ok(rejection(PublicDeliveryIssue::InvalidReference));
                    }
                }
                Ok(_) => (),
                Err(DialoguePublicationError::Store(
                    musteragent_runtime_session::DialogueStoreError::Unavailable,
                )) => return Ok(rejection(PublicDeliveryIssue::InvalidReference)),
                Err(error) => return Err(publication_error(error)),
            }
        }
        Ok(None)
    }

    pub(super) async fn prepare_public_deliveries(
        &self,
        state: &JournalState,
        kind: ModelCompletionKind,
        segment: &ContentRef,
    ) -> Result<Option<PreparedPublicDeliveries>, PostgresAgentLoopJournalError> {
        if !state.dialogue_model_output || kind != ModelCompletionKind::AssistantResponse {
            return Ok(None);
        }
        let Some(contract) =
            super::dialogue_contract::load_output(&mut *self.pool.acquire().await?, state)
                .await?
                .response_contract()
        else {
            return Ok(None);
        };
        let message = state
            .assistant_message_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        // Reuse the canonical physical-continuation chain. Intermediate tool
        // iterations are excluded by its invocation/message identity boundary.
        let source = self
            .final_assistant_message_ref(state, message, message, segment)
            .await?;
        if source.byte_length > MAX_PUBLIC_DELIVERY_BYTES as u64 {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        let bytes = self.content.read(state.tenant_id, &source).await?;
        verify_content(&source, &bytes).map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let drafts = contract
            .parse(&bytes)
            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
        let mut entries = Vec::with_capacity(drafts.len());
        let mut artifacts = BTreeSet::new();
        for draft in drafts {
            let content = match draft.content {
                PublicDeliveryDraft::Message(text) => PublicDeliveryContent::Message(
                    self.content
                        .put(PutContent {
                            tenant_id: state.tenant_id,
                            media_type: MediaType::new("text/markdown;charset=utf-8")
                                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                            bytes: text.into_bytes(),
                        })
                        .await?,
                ),
                object => {
                    crate::PostgresDialoguePublicationStore::new(self.ledger.database().clone())
                        .resolve_candidate_object(state.checkpoint.run_id, &object)
                        .await
                        .map_err(publication_error)?
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?
                }
            };
            if let PublicDeliveryContent::Artifact { artifact_id, .. } = &content
                && !artifacts.insert(*artifact_id)
            {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
            entries.push((draft.purpose, content));
        }
        Ok(Some(PreparedPublicDeliveries {
            source,
            contract,
            entries,
        }))
    }

    pub(super) async fn drain_public_deliveries(
        &self,
        attempt: &AttemptContext,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        let state = self.load_state(attempt).await?;
        if !state.dialogue_model_output {
            return Ok(());
        }
        let Some(contract) =
            super::dialogue_contract::load_output(&mut *self.pool.acquire().await?, &state)
                .await?
                .response_contract()
        else {
            return Ok(());
        };
        let rows=sqlx::query("SELECT c.* FROM agent.public_delivery_candidates c LEFT JOIN agent.dialogue_publications p ON p.delivery_id=COALESCE(c.satisfied_publication_id,c.delivery_id) AND p.tenant_id=c.tenant_id AND p.source_run_id=c.run_id WHERE c.tenant_id=$1 AND c.run_id=$2 AND p.delivery_id IS NULL ORDER BY c.created_at_ms,c.invocation_id,c.ordinal LIMIT 17")
            .bind(state.tenant_id.into_uuid()).bind(attempt.run_id.into_uuid()).fetch_all(&self.pool).await?;
        if rows.len() > MAX_PUBLIC_DELIVERIES {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }
        let store = crate::PostgresDialoguePublicationStore::new(self.ledger.database().clone());
        for row in rows {
            if row.try_get::<String, _>("contract_key")? != contract.key()
                || row.try_get::<Vec<u8>, _>("contract_sha256")? != contract.hash().into_bytes()
            {
                return Err(PostgresAgentLoopJournalError::CorruptState);
            }
            let purpose = purpose(row.try_get::<&str, _>("purpose")?)?;
            let content = match row.try_get::<&str, _>("kind")? {
                "message" => {
                    let value = crate::dialogue_codec::content_from_bytes(
                        row.try_get::<Vec<u8>, _>("body_content_ref")?,
                    )
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                    if value.content_id.into_uuid()
                        != row.try_get::<uuid::Uuid, _>("body_content_id")?
                    {
                        return Err(PostgresAgentLoopJournalError::CorruptState);
                    }
                    PublicDeliveryContent::Message(value)
                }
                "interaction" => PublicDeliveryContent::Interaction(
                    InteractionId::from_uuid(row.try_get("interaction_id")?)
                        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                ),
                "artifact" => PublicDeliveryContent::Artifact {
                    artifact_id: musteragent_identity::ArtifactId::from_uuid(
                        row.try_get("artifact_id")?,
                    )
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                    artifact_version_id: musteragent_identity::ArtifactVersionId::from_uuid(
                        row.try_get("artifact_version_id")?,
                    )
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                },
                _ => return Err(PostgresAgentLoopJournalError::CorruptState),
            };
            let revision = row.try_get::<i64, _>("permission_revision")?;
            let revision = u64::try_from(revision)
                .ok()
                .and_then(std::num::NonZeroU64::new)
                .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
            store
                .append(PublicEntryAppend {
                    delivery_id: CommandId::from_uuid(row.try_get("delivery_id")?)
                        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                    fence: attempt_fence(attempt, state.home_epoch),
                    expected_last_run_sequence: state.last_run_sequence,
                    expected_permission_revision: revision,
                    purpose,
                    content,
                })
                .await
                .map_err(publication_error)?;
        }
        let delivered: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_publications WHERE tenant_id=$1 AND source_run_id=$2)")
            .bind(state.tenant_id.into_uuid()).bind(attempt.run_id.into_uuid()).fetch_one(&self.pool).await?;
        if !delivered {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        Ok(())
    }
}

pub(super) async fn stage_candidates(
    conn: &mut sqlx::PgConnection,
    state: &JournalState,
    invocation_id: InvocationId,
    prepared: Option<&PreparedPublicDeliveries>,
    now: UtcTimestamp,
) -> Result<(), PostgresAgentLoopJournalError> {
    let Some(prepared) = prepared else {
        return Ok(());
    };
    if !state.dialogue_model_output
        || super::dialogue_contract::load(conn, state).await? != prepared.contract
    {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    let revision:i64=sqlx::query_scalar("SELECT permission_revision FROM agent.dialogue_sessions WHERE conversation_id=$1 AND tenant_id=$2")
        .bind(state.conversation_id.into_uuid()).bind(state.tenant_id.into_uuid()).fetch_one(&mut *conn).await?;
    let mut references = vec![&prepared.source];
    references.extend(
        prepared
            .entries
            .iter()
            .filter_map(|(_, content)| match content {
                PublicDeliveryContent::Message(r) => Some(r),
                _ => None,
            }),
    );
    references.sort_by_key(|r| r.content_id);
    for reference in references {
        crate::dialogue_codec::lock_ready_content_reference(conn, state.tenant_id, reference)
            .await
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    }
    let output = state
        .assistant_output_id
        .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
    for (ordinal, (purpose, content)) in prepared.entries.iter().enumerate() {
        let (kind, body_ref, body_id, interaction, artifact, version, satisfied) = match content {
            PublicDeliveryContent::PartnerCreated(_) => {
                return Err(PostgresAgentLoopJournalError::CorruptState);
            }
            PublicDeliveryContent::Message(r) => (
                "message",
                Some(crate::dialogue_codec::content_bytes(r)),
                Some(r.content_id.into_uuid()),
                None,
                None,
                None,
                None,
            ),
            PublicDeliveryContent::Interaction(id) => (
                "interaction",
                None,
                None,
                Some(id.into_uuid()),
                None,
                None,
                None,
            ),
            PublicDeliveryContent::Artifact {
                artifact_id,
                artifact_version_id,
            } => (
                "artifact",
                None,
                None,
                None,
                Some(artifact_id.into_uuid()),
                Some(artifact_version_id.into_uuid()),
                None,
            ),
        };
        sqlx::query("INSERT INTO agent.public_delivery_candidates (delivery_id,tenant_id,conversation_id,run_id,invocation_id,output_id,ordinal,contract_key,contract_sha256,source_content_id,source_sha256,kind,purpose,body_content_ref,body_content_id,interaction_id,artifact_id,artifact_version_id,permission_revision,created_at_ms,satisfied_publication_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)")
            .bind(CommandId::generate().into_uuid()).bind(state.tenant_id.into_uuid()).bind(state.conversation_id.into_uuid()).bind(state.checkpoint.run_id.into_uuid()).bind(invocation_id.into_uuid()).bind(output.into_uuid()).bind(i16::try_from(ordinal)?)
            .bind(prepared.contract.key()).bind(prepared.contract.hash().into_bytes().to_vec()).bind(prepared.source.content_id.into_uuid()).bind(prepared.source.sha256.into_bytes().to_vec())
            .bind(kind).bind(purpose.as_str()).bind(body_ref).bind(body_id).bind(interaction).bind(artifact).bind(version).bind(revision).bind(now.unix_millis()).bind(satisfied as Option<uuid::Uuid>).execute(&mut *conn).await?;
    }
    Ok(())
}
fn purpose(value: &str) -> Result<DialoguePublicationPurpose, PostgresAgentLoopJournalError> {
    Ok(match value {
        "reply" => DialoguePublicationPurpose::Reply,
        "result" => DialoguePublicationPurpose::Result,
        "important_update" => DialoguePublicationPurpose::ImportantUpdate,
        "clarification" => DialoguePublicationPurpose::Clarification,
        "required_decision" => DialoguePublicationPurpose::RequiredDecision,
        "blocked" => DialoguePublicationPurpose::Blocked,
        _ => return Err(PostgresAgentLoopJournalError::CorruptState),
    })
}
pub(super) fn publication_error(value: DialoguePublicationError) -> PostgresAgentLoopJournalError {
    match value {
        DialoguePublicationError::StaleFence => {
            PostgresAgentLoopJournalError::Execution(PostgresExecutionError::StaleFence)
        }
        other => PostgresAgentLoopJournalError::Publication(other),
    }
}
