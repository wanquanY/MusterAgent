//! Shared database fixture facts, independent of any released execution Home.
#![allow(
    dead_code,
    unused_imports,
    reason = "Each database test binary imports a different subset of this shared fixture."
)]
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_runtime_session::*;
use sqlx::PgPool;
mod activation;
pub(crate) use activation::{prepared, publish_successor};
mod blobs;
pub(crate) use blobs::MemoryBlobs;
pub(crate) mod membership;
pub(crate) use membership::{host_authority, host_authority_at};

pub(crate) fn actor() -> Actor {
    Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    }
}
pub(crate) fn content(kind: &str) -> ContentRef {
    let id = ContentId::generate();
    let bytes = format!("{kind}:{id}");
    ContentRef {
        content_id: id,
        media_type: MediaType::new("text/plain").unwrap(),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}
pub(crate) async fn register_content(pool: &PgPool, owner: Actor, c: &ContentRef) {
    sqlx::query(
            "INSERT INTO artifact.content_refs(content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms) VALUES($1,$2,$3,$4::bigint,$5,$6,1)",
        )
.bind(c.content_id.into_uuid())
.bind(owner.tenant_id.into_uuid())
.bind(c.media_type.as_str())
.bind(c.byte_length.to_string())
.bind(c.sha256.into_bytes().as_slice())
.bind(format!("test://{}",c.content_id))
.execute(pool).await.unwrap();
}
pub(crate) fn input(owner: Actor, dialogue_id: AgentDialogueId) -> AcceptDialogueIntent {
    AcceptDialogueIntent {
        reply_target: None,

        input_attachments: vec![],
        actor: owner,
        command_id: CommandId::generate(),
        dialogue_id,
        content_ref: content("user task"),
        origin: DialogueInputOrigin {
            surface: DialogueInputSurface::Web,
            installation_id: None,
        },
        observed_cursor: DialogueCursor { epoch: 1, seq: 0 },
        reply_to_intent_id: None,
        task_target: None,
        accepted_at: UtcTimestamp::from_unix_millis(200),
    }
}
pub(crate) async fn count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM agent.{table}"))
        .fetch_one(pool)
        .await
        .unwrap()
}
