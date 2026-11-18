//! Deterministically commit a source deletion after successful materialization
//! and before the actual planner builds/persists its candidate request.
use musteragent_content_ref::ContentRef;
use musteragent_context::{
    ContextCompactionRequest, ContextCompactionResult, ContextCompactor, ContextMaterial,
    ContextMaterialSource, ContextPlanError, ContextPlanInput,
};
use musteragent_runtime_contract::ApplicationFuture;
use std::sync::Arc;

pub struct DeleteAfterLoad {
    pub source: Arc<dyn ContextMaterialSource>,
    pub target: ContentRef,
    pub database: Database,
    pub mutation: SourceMutation,
}
#[derive(Clone, Copy)]
pub enum SourceMutation {
    DeleteContent,
    ActivateBranch,
    RetractMessage,
}
pub enum Database {
    Postgres(sqlx::PgPool),
}
impl ContextMaterialSource for DeleteAfterLoad {
    fn load(
        &self,
        input: &ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ContextMaterial, ContextPlanError>> {
        let input = input.clone();
        Box::pin(async move {
            let material = self.source.load(&input).await?;
            assert!(
                material.source_content_refs.contains(&self.target),
                "original source dependency must be captured"
            );
            if !matches!(self.mutation, SourceMutation::DeleteContent) {
                change_projection(&self.database, &material, self.mutation).await;
                return Ok(material);
            }
            let rows = match &self.database {
                Database::Postgres(pool) => {
                    sqlx::query("UPDATE artifact.content_refs SET state=3 WHERE content_id=$1")
                        .bind(self.target.content_id.into_uuid())
                        .execute(pool)
                        .await
                        .expect("load fixture")
                        .rows_affected()
                }
            };
            assert_eq!(rows, 1);
            Ok(material)
        })
    }
}
pub struct NeverCompact;
impl ContextCompactor for NeverCompact {
    fn compact(
        &self,
        _: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionResult, ContextPlanError>> {
        Box::pin(async { panic!("small fixture must remain losslessly uncompressed") })
    }
}

async fn change_projection(
    database: &Database,
    material: &ContextMaterial,
    change: SourceMutation,
) {
    let tenant = material.run_context.tenant_id;
    let conversation = material.run_context.conversation_id;
    let branch = uuid::Uuid::now_v7();
    let next = material.ledger_cutoff.get() + 1;
    if matches!(change, SourceMutation::RetractMessage) {
        seed_visible_message(database, material).await;
    }
    match database {
        Database::Postgres(pool) => {
            let mut tx = pool.begin().await.expect("change projection fixture");
            if matches!(change, SourceMutation::ActivateBranch) {
                sqlx::query("INSERT INTO conversation.context_branches
                    (branch_id,tenant_id,conversation_id,fork_from_record_id,fork_from_sequence,created_by,created_sequence,created_at_ms,origin)
                    SELECT $1,$2,$3,record_id,conversation_seq,$1,$4,1000,1 FROM event_store.ledger_records
                    WHERE tenant_id=$2 AND conversation_id=$3 ORDER BY conversation_seq LIMIT 1")
                    .bind(branch).bind(tenant.into_uuid()).bind(conversation.into_uuid()).bind(next as i64)
                    .execute(&mut *tx).await.expect("change projection fixture");
                sqlx::query(
                    "INSERT INTO conversation.context_branch_state VALUES($1,$2,$3,$4,1000)",
                )
                .bind(tenant.into_uuid())
                .bind(conversation.into_uuid())
                .bind(branch)
                .bind(next as i64)
                .execute(&mut *tx)
                .await
                .expect("change projection fixture");
            } else {
                sqlx::query("INSERT INTO conversation.message_retractions
                    (retraction_record_id,tenant_id,conversation_id,message_id,retracted_sequence,retracted_at_ms)
                    SELECT (SELECT record_id FROM event_store.ledger_records WHERE tenant_id=$1 AND conversation_id=$2
                        ORDER BY conversation_seq LIMIT 1),$1,$2,message_id,$3,1000 FROM conversation.messages
                    WHERE tenant_id=$1 AND conversation_id=$2 LIMIT 1")
                    .bind(tenant.into_uuid()).bind(conversation.into_uuid()).bind(next as i64)
                    .execute(&mut *tx).await.expect("change projection fixture");
            }
            tx.commit().await.expect("change projection fixture");
        }
    }
}

async fn seed_visible_message(database: &Database, material: &ContextMaterial) {
    use musteragent_projection::LedgerPayloadDecoder;
    let entry = &material.entries[0];
    let source = entry.source_record_ids[0];
    let bytes: Vec<u8> = match database {
        Database::Postgres(pool) => sqlx::query_scalar(
            "SELECT encoded_event FROM event_store.domain_events WHERE record_id=$1",
        )
        .bind(source.into_uuid())
        .fetch_one(pool)
        .await
        .expect("seed visible message fixture"),
    };
    let event = musteragent_wire::ProtobufLedgerPayloadCodec
        .decode(
            musteragent_event_envelope::LedgerRecordKind::DomainEvent,
            &bytes,
        )
        .expect("seed visible message fixture");
    let musteragent_ledger::LedgerPayload::DomainEvent(
        musteragent_ledger::DomainEvent::Conversation(
            musteragent_conversation::ConversationEvent::MessageAccepted {
                message_id,
                content_ref,
                ..
            },
        ),
    ) = event
    else {
        panic!("expected actual accepted user message")
    };
    let context = &material.run_context;
    match database {
        Database::Postgres(pool) => {
            sqlx::query("INSERT INTO conversation.messages
                (message_id,tenant_id,conversation_id,participant_id,run_id,role,content_id,content_hash,content_bytes,lifecycle,committed_sequence,committed_at_ms)
                VALUES($1,$2,$3,$4,$5,1,$6,$7,$8,1,$9,1000) ON CONFLICT(message_id) DO NOTHING")
                .bind(message_id.into_uuid()).bind(context.tenant_id.into_uuid()).bind(context.conversation_id.into_uuid())
                .bind(context.requested_by_participant_id.into_uuid()).bind(context.run_id.into_uuid())
                .bind(content_ref.content_id.into_uuid()).bind(content_ref.sha256.into_bytes().to_vec())
                .bind(content_ref.byte_length as i64).bind(entry.source_sequence.get() as i64).execute(pool).await.expect("seed visible message fixture");
        }
    }
}
