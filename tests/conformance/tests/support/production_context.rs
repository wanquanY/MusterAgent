use std::sync::Arc;

use musteragent_context::{
    ContextCompactionRequest, ContextCompactionResult, ContextCompactor, ContextPlanError,
    ContextPlanner, DurableContextPlanner,
};
use musteragent_conversation::{ConversationEvent, MessageRole};
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_postgres::{PostgresContextMaterialSource, PostgresContextPlanStore};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_snapshot::{RuntimeSnapshot, RuntimeSnapshotContent};
use musteragent_wire::{CanonicalJsonContextCodec, ProtobufSnapshotCodec};

use crate::running_loop::{Fixture, database_now};

#[derive(Debug, Copy, Clone)]
struct UnexpectedCompactor;

impl ContextCompactor for UnexpectedCompactor {
    fn compact(
        &self,
        _request: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionResult, ContextPlanError>> {
        Box::pin(async { Err(ContextPlanError::invariant()) })
    }
}

/// Installs a sealed Runtime Snapshot and canonical User fact, then returns
/// the production PostgreSQL Context planner used by Cloud Worker.
pub async fn install_production_context(
    pool: &sqlx::PgPool,
    content: Arc<dyn ContentStore>,
    fixture: Fixture,
    codec: Arc<musteragent_wire::ProtobufLedgerPayloadCodec>,
    snapshot_content: RuntimeSnapshotContent,
    user_text: &[u8],
) -> Arc<dyn ContextPlanner> {
    install_production_context_with_compactor(
        pool,
        content,
        fixture,
        codec,
        snapshot_content,
        user_text,
        Arc::new(UnexpectedCompactor),
    )
    .await
}

pub async fn install_production_context_with_compactor(
    pool: &sqlx::PgPool,
    content: Arc<dyn ContentStore>,
    fixture: Fixture,
    codec: Arc<musteragent_wire::ProtobufLedgerPayloadCodec>,
    snapshot_content: RuntimeSnapshotContent,
    user_text: &[u8],
    compactor: Arc<dyn ContextCompactor>,
) -> Arc<dyn ContextPlanner> {
    let now = database_now(pool).await;
    let snapshot = RuntimeSnapshot::seal(
        fixture.runtime_snapshot_id,
        snapshot_content,
        now,
        &ProtobufSnapshotCodec,
    )
    .expect("seal production Context Runtime Snapshot");
    sqlx::query(
        "UPDATE capability.runtime_snapshots
         SET schema_version = $2, encoded_snapshot = $3, content_hash = $4
         WHERE runtime_snapshot_id = $1",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(snapshot.content.schema_version.as_str())
    .bind(&snapshot.encoded_content)
    .bind(snapshot.content_hash.into_bytes().to_vec())
    .execute(pool)
    .await
    .expect("install production Context Runtime Snapshot");

    // Built-in Skill instruction bodies are served by the Content store's
    // runtime-builtin special case, but that path still requires the reference
    // row. Migrations seed only the original (legacy one-line) materials, so
    // newly released or re-authored built-in Skills must be registered here
    // the same way the production Runtime Release pipeline would.
    for skill in &snapshot.content.skills {
        sqlx::query(
            "INSERT INTO artifact.content_refs (
                 content_id, tenant_id, media_type, byte_length, content_hash,
                 storage_ref, storage_ref_protected, state, created_at_ms, updated_at_ms,
                 ownership_kind
             ) VALUES ($1, NULL, $2, $3::numeric, $4, $5, NULL, 2, $6, $6, 2)
             ON CONFLICT (content_id) DO UPDATE
             SET media_type = EXCLUDED.media_type,
                 byte_length = EXCLUDED.byte_length,
                 content_hash = EXCLUDED.content_hash,
                 storage_ref = EXCLUDED.storage_ref,
                 state = EXCLUDED.state,
                 updated_at_ms = EXCLUDED.updated_at_ms",
        )
        .bind(skill.instructions_ref.content_id.into_uuid())
        .bind(skill.instructions_ref.media_type.as_str())
        .bind(skill.instructions_ref.byte_length.to_string())
        .bind(skill.instructions_ref.sha256.into_bytes().to_vec())
        .bind(format!(
            "runtime-builtin://skill/{}/instructions",
            skill.skill_ref.as_str()
        ))
        .bind(now.unix_millis())
        .execute(pool)
        .await
        .expect("seed built-in Skill instruction content");
    }

    // Admission data must be written once by seed_running_loop_with_input;
    // installing a planner never rewrites an existing Ledger identity.
    use musteragent_projection::LedgerPayloadDecoder;
    let encoded: Vec<u8> = sqlx::query_scalar(
        "SELECT event.encoded_event FROM event_store.domain_events event JOIN event_store.ledger_records record USING (record_id)
         WHERE record.tenant_id=$1 AND record.conversation_id=$2 AND record.conversation_seq=1")
        .bind(fixture.tenant_id.into_uuid()).bind(fixture.conversation_id.into_uuid()).fetch_one(pool).await.expect("production context fixture");
    let LedgerPayload::DomainEvent(DomainEvent::Conversation(ConversationEvent::MessageAccepted {
        role: MessageRole::User,
        content_ref,
        ..
    })) = codec
        .decode(
            musteragent_event_envelope::LedgerRecordKind::DomainEvent,
            &encoded,
        )
        .expect("admission payload")
    else {
        panic!("production context requires seed_running_loop_with_input");
    };
    assert_eq!(
        content
            .read(fixture.tenant_id, &content_ref)
            .await
            .expect("production context fixture"),
        user_text
    );

    Arc::new(DurableContextPlanner::new(
        Arc::new(PostgresContextMaterialSource::new(
            pool.clone(),
            content.clone(),
            Arc::new(ProtobufSnapshotCodec),
            codec.clone(),
        )),
        content,
        Arc::new(PostgresContextPlanStore::new(pool.clone(), codec)),
        Arc::new(CanonicalJsonContextCodec),
        compactor,
    ))
}
