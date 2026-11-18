//! Server-side derived admissions. Only the orchestration owners call this
//! module, after fencing the schedule or the live parent. The source Snapshot
//! is the permission ceiling, never an installed catalog or tool argument.
use crate::{GovernedPostgresTransaction, PostgresDatabaseRuntime};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_event_envelope::*;
use musteragent_identity::*;
use musteragent_run::*;
use musteragent_runtime_automation::ScheduleServiceError as Error;
use musteragent_runtime_contract::TraceContext;
use musteragent_runtime_snapshot::*;
use sqlx::Row;

mod records;
mod snapshot;
pub(super) use records::schedule_batch;
pub(super) use snapshot::derive_child;

pub(super) struct Source {
    pub run_id: RunId,
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub workspace: Option<WorkspaceId>,
    pub snapshot: RuntimeSnapshotContent,
    pub snapshot_hash: Sha256,
    pub trace: TraceContext,
}

pub(super) async fn source(tx: &mut sqlx::PgConnection, run: RunId) -> Result<Source, Error> {
    let row=sqlx::query("SELECT r.tenant_id,r.conversation_id,r.workspace_id,r.traceparent,r.tracestate,c.created_by,s.encoded_snapshot,s.content_hash FROM runtime.runs r JOIN conversation.conversations c ON c.conversation_id=r.conversation_id AND c.tenant_id=r.tenant_id JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id WHERE r.run_id=$1 AND r.execution_home_kind=1 AND r.control_home_kind=1 AND r.execution_scope_kind IN (1,2) AND c.lifecycle=1")
        .bind(run.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::NotFound)?;
    let bytes: Vec<u8> = row.try_get("encoded_snapshot").map_err(db)?;
    let hash = Sha256::digest(&bytes);
    if row.try_get::<Vec<u8>, _>("content_hash").map_err(db)? != hash.into_bytes() {
        return Err(Error::Corrupt);
    }
    let snapshot = musteragent_persistence_codec::ProtobufSnapshotCodec
        .decode_content(&bytes)
        .map_err(|error| {
            tracing::warn!(error = ?error, "source decode rejected");
            Error::Corrupt
        })?;
    snapshot.validate_sealed().map_err(|error| {
        tracing::warn!(error = ?error, "source validate rejected");
        Error::Corrupt
    })?;
    if let Some(authority) = &snapshot.owner_goal_authority {
        crate::standing_goal::owner_goal::run::validate_derived_authority(tx, authority, true)
            .await
            .map_err(|error| match error {
                musteragent_runtime_session::DialogueStoreError::Infrastructure => {
                    Error::Unavailable
                }
                musteragent_runtime_session::DialogueStoreError::Corrupt => Error::Corrupt,
                _ => Error::SourceRevoked,
            })?;
    }
    Ok(Source {
        run_id: run,
        actor: Actor {
            tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
            principal_id: PrincipalId::from_uuid(row.try_get("created_by").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
            principal_kind: PrincipalKind::User,
        },
        conversation_id: ConversationId::from_uuid(row.try_get("conversation_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        workspace: row
            .try_get::<Option<uuid::Uuid>, _>("workspace_id")
            .map_err(db)?
            .map(WorkspaceId::from_uuid)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
        snapshot,
        snapshot_hash: hash,
        trace: TraceContext {
            traceparent: musteragent_runtime_contract::TraceParent::new(
                row.try_get::<String, _>("traceparent").map_err(db)?,
            )
            .map_err(|_| Error::Corrupt)?,
            tracestate: row
                .try_get::<Option<String>, _>("tracestate")
                .map_err(db)?
                .map(musteragent_runtime_contract::TraceState::new)
                .transpose()
                .map_err(|_| Error::Corrupt)?,
        },
    })
}

/// Persist the original admission projections and Ledger in the caller's
/// transaction. No worker-visible Run exists without its occurrence link.
pub(super) async fn admit_schedule(
    database: &PostgresDatabaseRuntime,
    tx: GovernedPostgresTransaction<'static>,
    batch: musteragent_ledger::AdmissionBatch,
    occurrence: uuid::Uuid,
    source: &Source,
) -> Result<(), Error> {
    use crate::admission::*;
    batch.validate().map_err(|error| {
        tracing::warn!(?error, "derived admission contract rejected");
        Error::Invalid
    })?;
    let admission = PostgresAdmissionStore::new(
        database.clone(),
        std::sync::Arc::new(musteragent_persistence_codec::ProtobufSnapshotCodec),
    );
    let mut prepared = match admission
        .ledger
        .begin_append_in_transaction(tx, &batch.append)
        .await
        .map_err(|_| Error::Unavailable)?
    {
        crate::ledger::BeginAppend::Ready(p) => p,
        crate::ledger::BeginAppend::Duplicate(_) => return Err(Error::Conflict),
    };
    let sequence = prepared.sequences()[0].get();
    let snapshot = admission
        .validate_derived_admission_prefix(prepared.transaction_mut(), &batch, &source.snapshot)
        .await
        .map_err(|_| Error::Corrupt)?;
    validate_admission_authority(prepared.transaction_mut(), &batch)
        .await
        .map_err(|_| Error::NotFound)?;
    authorize_content(prepared.transaction_mut(), &batch)
        .await
        .map_err(|_| Error::Corrupt)?;
    insert_message(prepared.transaction_mut(), &batch, sequence)
        .await
        .map_err(|_| Error::Unavailable)?;
    insert_activity(prepared.transaction_mut(), &batch)
        .await
        .map_err(|_| Error::Unavailable)?;
    consume_execution_grant(prepared.transaction_mut(), &batch)
        .await
        .map_err(|_| Error::Unavailable)?;
    insert_execution_grant_routes(prepared.transaction_mut(), &batch)
        .await
        .map_err(|_| Error::Unavailable)?;
    persist_interactive_run_projection(
        prepared.transaction_mut(),
        (&batch).into(),
        sequence,
        &snapshot,
    )
    .await
    .map_err(|_| Error::Unavailable)?;
    sqlx::query("UPDATE agent.owner_schedule_occurrences SET status='running',run_id=$2,conversation_id=$3,claim_attempt=1 WHERE occurrence_id=$1 AND status='pending'")
        .bind(occurrence).bind(batch.run.context.run_id.into_uuid()).bind(batch.append.conversation_id.into_uuid()).execute(prepared.transaction_mut()).await.map_err(db)?;
    sqlx::query("INSERT INTO runtime.derived_run_authorities(run_id,source_run_id,billing_subject,source_snapshot_hash,derived_snapshot_hash) VALUES($1,$2,$3,$4,$5)")
        .bind(batch.run.context.run_id.into_uuid()).bind(source.run_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).bind(source.snapshot_hash.into_bytes().as_slice()).bind(batch.runtime_snapshot.content_hash.into_bytes().as_slice()).execute(prepared.transaction_mut()).await.map_err(db)?;
    // P3: background execution routes to the server regardless of where the
    // foreground owner created the schedule (surface=API, trigger=Scheduled).
    sqlx::query("INSERT INTO runtime.sandbox_routing_contexts(run_id,tenant_id,principal_id,surface,trigger,policy) VALUES($1,$2,$3,4,3,3)")
        .bind(batch.run.context.run_id.into_uuid()).bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).execute(prepared.transaction_mut()).await.map_err(db)?;
    let mut staged = prepared
        .stage(&batch.append)
        .await
        .map_err(|_| Error::Unavailable)?;
    admission
        .audit
        .append_admission_allowed_in_transaction(
            staged.transaction_mut(),
            batch.actor.tenant_id,
            batch.run.context.run_id,
        )
        .await
        .map_err(|_| Error::Unavailable)?;
    staged.commit().await.map_err(|_| Error::Unavailable)?;
    Ok(())
}

pub(super) async fn session(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    context: RunContext,
    now: UtcTimestamp,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO conversation.conversations(conversation_id,tenant_id,workspace_id,kind,control_home_kind,home_epoch,lifecycle,created_by,created_at_ms,updated_at_ms,execution_owner_participant_id) VALUES($1,$2,$3,1,1,1,1,$4,$5,$5,$6)")
        .bind(context.conversation_id.into_uuid()).bind(context.tenant_id.into_uuid()).bind(source.workspace.map(WorkspaceId::into_uuid)).bind(source.actor.principal_id.into_uuid()).bind(now.unix_millis()).bind(context.executor_participant_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
    sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,principal_ref,agent_ref,display_name,joined_at_ms) VALUES($1,$2,$3,1,$4,NULL,'Schedule owner',$7),($5,$2,$3,2,NULL,$6,'Scheduled agent',$7)")
        .bind(context.requested_by_participant_id.into_uuid()).bind(context.tenant_id.into_uuid()).bind(context.conversation_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).bind(context.executor_participant_id.into_uuid()).bind(source.snapshot.agent.agent_id.into_uuid()).bind(now.unix_millis()).execute(tx).await.map_err(db)?;
    Ok(())
}

fn db(error: sqlx::Error) -> Error {
    tracing::warn!(code=?error.as_database_error().and_then(|e|e.code()),"cloud admission storage failed");
    Error::Unavailable
}
