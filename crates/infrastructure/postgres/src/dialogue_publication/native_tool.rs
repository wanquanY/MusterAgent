//! Native publication shares the original per-Run commit lane and the Dialogue
//! Authority transaction. A committed group is inseparable from Tool success.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_ledger::{DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord};
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{CapabilityResult, RuntimeClock};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::ToolEvent;
use std::sync::Arc;
type Failure = DialogueToolPublicationError;
mod preparation;
use preparation::Prepared;

#[derive(Clone)]
pub struct PostgresFencedDialogueToolPublication {
    ledger: crate::PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    content: Arc<dyn ContentStore>,
    reference_keys: Arc<OpaqueRefKeyring>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
    clock: Arc<dyn RuntimeClock>,
    liveness_clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
}

impl PostgresFencedDialogueToolPublication {
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        content: Arc<dyn ContentStore>,
        reference_keys: Arc<OpaqueRefKeyring>,
        audit: Arc<crate::PostgresSecurityAuditPipeline>,
        clock: Arc<dyn RuntimeClock>,
        liveness_clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
    ) -> Self {
        Self {
            ledger: crate::PostgresLedgerStore::new(database),
            encoder,
            content,
            reference_keys,
            audit,
            clock,
            liveness_clock,
        }
    }

    async fn commit_inner(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
    ) -> Result<CapabilityResult, Failure> {
        let prepared = self.prepare(attempt, invocation_id).await?;
        // Replay is a read of the original result, even if this physical lease
        // expired after commit. It can never create another public entry.
        if self.committed(&prepared, invocation_id).await? {
            return Ok(prepared.result);
        }
        let (mut tx, lane) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(lane_error)?;
        if lane.tenant_id != prepared.tenant_id {
            return Err(Failure::Forbidden);
        }
        validate_live(&mut tx, attempt, lane).await?;
        // A peer may have committed while we waited for the original lane.
        if self.committed_on(&mut tx, &prepared, invocation_id).await? {
            return Ok(prepared.result);
        }
        crate::resource_commit::lock_invocation(&mut tx, lane.tenant_id, invocation_id, attempt)
            .await
            .map_err(resource_error)?;
        let source = load_source(&mut tx, attempt.run_id)
            .await
            .map_err(publication_error)?;
        // Original account/configuration authority precedes Resource locks.
        if !prepared.files.is_empty() {
            lock_publication_authority(&mut tx, &source)
                .await
                .map_err(publication_error)?;
        }
        let now = self.clock.now();
        if matches!(source.subject, PublicationSubject::WorkItem(_)) {
            crate::work_item_execution::requester::publication(
                &mut tx,
                lane.tenant_id,
                attempt.run_id,
                invocation_id,
                prepared.publication.purpose.as_str(),
                now,
            )
            .await
            .map_err(|e| publication_error(DialoguePublicationError::Store(e)))?;
        }
        let mut contents = Vec::new();
        if let Some(reference) = &prepared.message {
            crate::artifact::authorize_content_ref(&mut tx, lane.tenant_id, reference)
                .await
                .map_err(artifact_error)?;
            contents.push((PublicDeliveryContent::Message(reference.clone()), None));
        }
        let mut artifacts = std::collections::BTreeSet::new();
        for reference in &prepared.publication.artifacts {
            if matches!(source.subject, PublicationSubject::WorkItem(_))
                && prepared.publication.purpose == DialoguePublicationPurpose::Result
                && let Some((id, returned)) = task_result::selected_file(
                    &mut tx,
                    &source,
                    attempt.run_id,
                    invocation_id,
                    reference,
                )
                .await
                .map_err(|e| publication_error(e.into()))?
            {
                if !artifacts.insert(id) {
                    return Err(Failure::InvalidArguments);
                }
                contents.push((
                    returned.content.clone(),
                    Some(PublicationSource::ReturnedFileToolOwner(
                        invocation_id,
                        returned,
                    )),
                ));
                continue;
            }
            let resolved = candidate_resolution::resolve(
                &mut tx,
                &source,
                attempt.run_id,
                &reference.as_draft(),
            )
            .await
            .map_err(publication_error)?
            .ok_or(Failure::ReferenceUnavailable)?;
            let PublicDeliveryContent::Artifact { artifact_id, .. } = &resolved else {
                return Err(Failure::Corrupt);
            };
            if !artifacts.insert(*artifact_id) {
                return Err(Failure::InvalidArguments);
            }
            contents.push((resolved, None));
        }
        for reference in &prepared.files {
            let authorized =
                file_reference::resolve(&mut tx, &source, attempt.run_id, invocation_id, reference)
                    .await
                    .map_err(publication_error)?;
            contents.push((
                authorized.content(),
                Some(PublicationSource::FileToolOwner(invocation_id, authorized)),
            ));
        }
        for reference in &prepared.publication.evidence {
            candidate_resolution::resolve(&mut tx, &source, attempt.run_id, &reference.as_draft())
                .await
                .map_err(publication_error)?
                .ok_or(Failure::ReferenceUnavailable)?;
        }
        sqlx::query("INSERT INTO agent.dialogue_tool_publication_groups
            (invocation_id,tenant_id,source_run_id,source_intent_id,source_goal_id,protocol_ref,tool_ref,request_hash,purpose,entry_count,committed_at_ms)
            VALUES($1,$2,$3,$4,$10,'musteragent.tool-delivery@1',$5,$6,$7,$8,$9)")
            .bind(invocation_id.into_uuid()).bind(lane.tenant_id.into_uuid())
            .bind(attempt.run_id.into_uuid()).bind(source.subject.intent().map(UserIntentId::into_uuid))
            .bind(&prepared.tool_key).bind(prepared.request.sha256.into_bytes().to_vec())
            .bind(prepared.publication.purpose.as_str())
            .bind(i16::try_from(contents.len()).map_err(|_| Failure::Corrupt)?)
            .bind(now.unix_millis()).bind(source.subject.goal().map(GoalId::into_uuid)).execute(&mut *tx).await.map_err(database_error)?;
        for (ordinal, (content, authorized)) in contents.into_iter().enumerate() {
            let delivery_id = CommandId::from_uuid(uuid::Uuid::new_v5(
                &invocation_id.into_uuid(),
                format!("public-tool-entry@1:{ordinal}").as_bytes(),
            ))
            .map_err(|_| Failure::Corrupt)?;
            let purpose = prepared.publication.purpose;
            content
                .validate(purpose)
                .map_err(|_| Failure::InvalidArguments)?;
            let fingerprint = public_delivery_fingerprint(
                delivery_id,
                attempt.run_id,
                NonZeroU64::new(source.permission_revision).ok_or(Failure::Corrupt)?,
                purpose,
                &content,
            );
            publish_in_transaction(
                &mut tx,
                &source,
                &Delivery {
                    delivery_id,
                    run_id: attempt.run_id,
                    purpose,
                    content,
                    fingerprint,
                    source: match authorized {
                        Some(authorized) => authorized,
                        None => PublicationSource::ToolOwner(invocation_id),
                    },
                },
            )
            .await
            .map_err(publication_error)?;
        }
        crate::artifact::authorize_result_content(&mut tx, lane.tenant_id, &prepared.result)
            .await
            .map_err(artifact_error)?;
        let encoded = self
            .encoder
            .encode(LedgerPayload::DomainEvent(DomainEvent::Tool(
                ToolEvent::InvocationSucceeded {
                    invocation_id,
                    result_ref: prepared.result.result_ref.clone(),
                    result_hash: prepared.result.result_hash,
                    provider_request_ref: None,
                    authorized_content: Vec::new(),
                },
            )));
        let record = ProposedRecord {
            schema_version: DottedKey::new("musteragent.runtime.record.v1")
                .map_err(|_| Failure::Corrupt)?,
            record_id: LedgerRecordId::from_uuid(uuid::Uuid::new_v5(
                &invocation_id.into_uuid(),
                b"dialogue-tool-publication@1",
            ))
            .map_err(|_| Failure::Corrupt)?,
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: Some(lane.activity_id),
            run_id: Some(attempt.run_id),
            run_seq: Some(
                RunSeq::new(lane.next_sequence(1).map_err(lane_error)?)
                    .map_err(|_| Failure::Corrupt)?,
            ),
            attempt_id: Some(attempt.attempt_id),
            participant_id: Some(prepared.executor),
            producer: Producer::Worker,
            occurred_at: now,
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            payload_hash: Sha256::digest(&encoded),
            payload: encoded,
        };
        let mut append = lane
            .append(format!("dialogue-publish:{invocation_id}"), vec![record])
            .map_err(lane_error)?;
        let liveness = crate::execution_liveness::stage_tool_terminal(
            &mut tx,
            Some(self.liveness_clock.as_ref()),
            self.encoder.as_ref(),
            attempt,
            invocation_id,
            &mut append,
        )
        .await
        .map_err(|_| Failure::Corrupt)?;
        validate_live(&mut tx, attempt, lane).await?;
        let mut transaction = match self
            .ledger
            .begin_append_in_transaction(tx, &append)
            .await
            .map_err(|_| Failure::Infrastructure)?
        {
            crate::ledger::BeginAppend::Ready(value) => value,
            crate::ledger::BeginAppend::Duplicate(_) => {
                return if self.committed(&prepared, invocation_id).await? {
                    Ok(prepared.result)
                } else {
                    Err(Failure::Corrupt)
                };
            }
        };
        crate::artifact::complete_tool(
            transaction.transaction_mut(),
            &self.reference_keys,
            lane.tenant_id,
            invocation_id,
            &prepared.result,
            now,
            true,
        )
        .await
        .map_err(artifact_error)?;
        crate::tool_security_audit::append_succeeded_in_transaction(
            &self.audit,
            transaction.transaction_mut(),
            lane.tenant_id,
            invocation_id,
        )
        .await
        .map_err(|_| Failure::Infrastructure)?;
        if let Some(change) = liveness {
            change
                .persist(transaction.transaction_mut())
                .await
                .map_err(|_| Failure::Corrupt)?;
        }
        crate::resource_commit::advance_run(
            transaction.transaction_mut(),
            lane.tenant_id,
            attempt.run_id,
            lane.next_sequence(append.records.len() as u64)
                .map_err(lane_error)?,
        )
        .await
        .map_err(resource_error)?;
        transaction
            .commit(&append)
            .await
            .map_err(|_| Failure::Infrastructure)?;
        Ok(prepared.result)
    }

    async fn committed(
        &self,
        prepared: &Prepared,
        invocation_id: InvocationId,
    ) -> Result<bool, Failure> {
        let mut connection = self
            .ledger
            .database()
            .pool()
            .acquire()
            .await
            .map_err(database_error)?;
        self.committed_on(&mut connection, prepared, invocation_id)
            .await
    }

    async fn committed_on(
        &self,
        conn: &mut sqlx::PgConnection,
        prepared: &Prepared,
        invocation_id: InvocationId,
    ) -> Result<bool, Failure> {
        let row = sqlx::query("SELECT i.status,i.result_content_id,r.result_hash,r.result_schema_key,r.result_schema_hash,
                g.request_hash,g.entry_count,g.purpose,
                (SELECT count(*) FROM agent.dialogue_publications p WHERE p.source_tool_invocation_id=i.invocation_id
                  AND p.tenant_id=i.tenant_id AND p.source_run_id=i.run_id) AS actual_entries
             FROM tool.tool_invocations i LEFT JOIN tool.tool_results r USING(invocation_id)
             LEFT JOIN agent.dialogue_tool_publication_groups g USING(invocation_id)
             WHERE i.invocation_id=$1 AND i.tenant_id=$2 AND i.run_id=$3")
            .bind(invocation_id.into_uuid()).bind(prepared.tenant_id.into_uuid())
            .bind(prepared.run_id.into_uuid()).fetch_optional(conn).await.map_err(database_error)?
            .ok_or(Failure::Forbidden)?;
        if row.try_get::<i16, _>("status").map_err(database_error)? != 4 {
            return Ok(false);
        }
        let result = &prepared.result;
        if row
            .try_get::<uuid::Uuid, _>("result_content_id")
            .map_err(database_error)?
            != result.result_ref.content_id.into_uuid()
            || row
                .try_get::<Vec<u8>, _>("result_hash")
                .map_err(database_error)?
                != result.result_hash.into_bytes()
            || row
                .try_get::<String, _>("result_schema_key")
                .map_err(database_error)?
                != result.result_schema.schema_key.as_str()
            || row
                .try_get::<Vec<u8>, _>("result_schema_hash")
                .map_err(database_error)?
                != result.result_schema.schema_hash.into_bytes()
            || row
                .try_get::<Vec<u8>, _>("request_hash")
                .map_err(database_error)?
                != prepared.request.sha256.into_bytes()
            || row
                .try_get::<i16, _>("entry_count")
                .map_err(database_error)? as usize
                != prepared.entry_count()
            || row
                .try_get::<i64, _>("actual_entries")
                .map_err(database_error)? as usize
                != prepared.entry_count()
            || row
                .try_get::<String, _>("purpose")
                .map_err(database_error)?
                != prepared.publication.purpose.as_str()
        {
            return Err(Failure::Corrupt);
        }
        Ok(true)
    }
}

impl FencedDialogueToolPublication for PostgresFencedDialogueToolPublication {
    fn commit<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        invocation_id: InvocationId,
    ) -> ApplicationFuture<'a, Result<DialogueToolPublicationReceipt, Failure>> {
        Box::pin(async move {
            let result = self.commit_inner(attempt, invocation_id).await?;
            Ok(DialogueToolPublicationReceipt {
                result_ref: result.result_ref,
                result_schema: result.result_schema,
            })
        })
    }
}

pub(super) fn database_error(error: sqlx::Error) -> Failure {
    if let sqlx::Error::Database(database) = &error {
        // Database metadata is enough to diagnose an uninstalled contract or
        // constraint failure. Never log request text, SQL arguments or details.
        tracing::error!(
            sqlstate = database.code().as_deref().unwrap_or("unknown"),
            constraint = database.constraint().unwrap_or("unknown"),
            table = database.table().unwrap_or("unknown"),
            "Dialogue publication database operation failed"
        );
    }
    match error {
        sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Decode(_)
        | sqlx::Error::RowNotFound => Failure::Corrupt,
        _ => Failure::Infrastructure,
    }
}
fn artifact_error(error: crate::artifact::PostgresArtifactError) -> Failure {
    match error {
        crate::artifact::PostgresArtifactError::Sql(error) => database_error(error),
        crate::artifact::PostgresArtifactError::Transaction(_) => Failure::Infrastructure,
        _ => Failure::Corrupt,
    }
}
fn resource_error(error: crate::resource_commit::PostgresFencedResourceToolError) -> Failure {
    use crate::resource_commit::PostgresFencedResourceToolError as E;
    match error {
        E::Sql(error) => database_error(error),
        E::Transaction(_) => Failure::Infrastructure,
        E::Execution(error) => execution_error(error),
        E::StaleFence | E::ToolTransitionConflict => Failure::StaleFence,
        _ => Failure::Corrupt,
    }
}
fn execution_error(error: crate::execution::PostgresExecutionError) -> Failure {
    use crate::execution::PostgresExecutionError as E;
    match error {
        E::Database(error) => database_error(error),
        E::Transaction(_) => Failure::Infrastructure,
        E::StaleFence => Failure::StaleFence,
        _ => Failure::Corrupt,
    }
}
fn lane_error(error: crate::commit_lane::CommitLaneError) -> Failure {
    match error {
        crate::commit_lane::CommitLaneError::StaleFence => Failure::StaleFence,
        crate::commit_lane::CommitLaneError::Database(_)
        | crate::commit_lane::CommitLaneError::Transaction(_) => Failure::Infrastructure,
        _ => Failure::Corrupt,
    }
}
pub(super) fn publication_error(error: DialoguePublicationError) -> Failure {
    match error {
        DialoguePublicationError::StaleFence | DialoguePublicationError::Cancelled => {
            Failure::StaleFence
        }
        DialoguePublicationError::Store(DialogueStoreError::Unavailable) => {
            Failure::ReferenceUnavailable
        }
        DialoguePublicationError::Store(DialogueStoreError::Infrastructure) => {
            Failure::Infrastructure
        }
        DialoguePublicationError::Store(DialogueStoreError::Corrupt) => Failure::Corrupt,
        _ => Failure::Forbidden,
    }
}

async fn validate_live(
    tx: &mut sqlx::PgConnection,
    attempt: &AttemptContext,
    lane: crate::commit_lane::CommitLaneState,
) -> Result<(), Failure> {
    let now = crate::ledger::database_now_ms(tx)
        .await
        .map_err(|_| Failure::Infrastructure)?;
    crate::execution::lock_and_validate_fence(
        tx,
        &crate::resource_commit::attempt_fence(attempt, lane.home_epoch),
        lane.last_run_sequence,
        now,
    )
    .await
    .map_err(execution_error)?;
    let stopped: bool = sqlx::query_scalar(
        "SELECT status<>2 OR cancel_requested_at_ms IS NOT NULL FROM runtime.runs WHERE run_id=$1",
    )
    .bind(attempt.run_id.into_uuid())
    .fetch_one(tx)
    .await
    .map_err(database_error)?;
    if stopped {
        Err(Failure::StaleFence)
    } else {
        Ok(())
    }
}
