//! Pre-Run publication owner: original queue lease + canonical input/config +
//! signed policy refusal, never a fabricated Run or Model-delivery fence.
use super::*;
use crate::dialogue_codec::*;
use musteragent_content_ref::MediaType;
use musteragent_execution_context::*;
use musteragent_persistence_codec::{dialogue_admission_codec as codec, wire};
use musteragent_runtime_content::PutContent;
use prost::Message;
use sqlx::PgConnection;
use std::num::NonZeroU64;
type Error = DialogueStoreError;
type ResultState = DialogueAdmissionTermination;

fn db(error: sqlx::Error) -> Error {
    // Diagnostic identifiers only: SQL messages and query parameters may
    // contain user content. The public outcome remains unknown on failure.
    let database = error.as_database_error();
    tracing::warn!(
        database.sqlstate = database
            .and_then(|value| value.code())
            .as_deref()
            .unwrap_or("unknown"),
        "Dialogue admission terminal storage operation failed"
    );
    Error::Infrastructure
}

impl PostgresDialogueIntentHandoffStore {
    pub(super) async fn reject_inner(
        &self,
        claim: DialogueIntentClaim,
        preparation: DialogueExecutionPreparation,
        proof: SignedDialogueAdmissionRejection,
    ) -> Result<ResultState, Error> {
        let (keys, content) = self
            .rejection_authority
            .as_ref()
            .ok_or(Error::Infrastructure)?;
        preparation.validate()?;
        let facts = codec::facts(&preparation.grant_fact_binding()?);
        if preparation.actor() != claim.actor
            || preparation.scope.source_intent_id != claim.intent_id
            || proof.claims.actor != claim.actor
            || proof.claims.intent_id != claim.intent_id
            || proof.claims.facts_sha256.into_bytes().as_slice() != facts.facts_sha256.as_slice()
        {
            return Err(Error::Corrupt);
        }
        // Authenticate even on replay. Expiry is rechecked before a new commit;
        // an exact acknowledged decision remains replayable after its TTL.
        keys.verify_dialogue_admission_rejection(&proof, proof.claims.issued_at)
            .map_err(|_| Error::Corrupt)?;
        let encoded = codec::rejection(&proof).encode_to_vec();
        let reference = content
            .put(PutContent {
                tenant_id: claim.actor.tenant_id,
                media_type: MediaType::new("text/plain").map_err(|_| Error::Corrupt)?,
                bytes: proof.claims.reason.message().as_bytes().to_vec(),
            })
            .await
            .map_err(|_| Error::Infrastructure)?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Admission)
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Acquire, error))?;
        super::progress::lock_account(&mut tx, claim.actor).await?;
        lock_input(&mut tx, claim).await?;
        if let Some(row) = sqlx::query(
            "SELECT signed_rejection FROM agent.dialogue_admission_outcomes WHERE intent_id=$1",
        )
        .bind(claim.intent_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        {
            if row
                .try_get::<Option<Vec<u8>>, _>("signed_rejection")
                .map_err(db)?
                .as_deref()
                != Some(encoded.as_slice())
            {
                return Err(Error::IdempotencyConflict);
            }
            return Ok(ResultState::Replayed);
        }
        if admitted(&mut tx, claim.intent_id).await? {
            return Ok(ResultState::AlreadyAdmitted);
        }
        if !lock_claim(&mut tx, claim).await? {
            return Ok(ResultState::LeaseLost);
        }
        lock_space(&mut tx, claim, preparation.scope.agent_id).await?;
        // UPDATE lock before load_preparation's shared locks prevents a dialogue
        // lock-upgrade deadlock with other Dialogue owner writers.
        sqlx::query(
            "SELECT dialogue_id FROM agent.agent_dialogues WHERE dialogue_id=$1 FOR UPDATE",
        )
        .bind(preparation.scope.dialogue_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let current = match crate::dialogue_execution::load_preparation(
            &mut tx,
            claim.actor,
            claim.intent_id,
            true,
        )
        .await
        {
            Ok(value) => value,
            Err(Error::Inactive | Error::Unavailable | Error::RevisionConflict) => {
                return Ok(ResultState::StalePreparation);
            }
            Err(error) => return Err(error),
        };
        if current != preparation {
            return Ok(ResultState::StalePreparation);
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        if now >= claim.lease_expires_at || !proof.claims.is_valid_at(now) {
            return Ok(ResultState::LeaseLost);
        }
        let entry = insert_message(
            &mut tx,
            claim,
            preparation.scope.agent_id,
            preparation.scope.dialogue_id,
            &reference,
            now,
        )
        .await?;
        sqlx::query("INSERT INTO agent.dialogue_admission_outcomes(intent_id,tenant_id,owner_id,agent_id,dialogue_id,outcome,reason,claim_generation,decision_id,facts_sha256,signed_rejection,entry_id,created_at_ms) VALUES($1,$2,$3,$4,$5,'rejected',$6,$7,$8,$9,$10,$11,$12)")
            .bind(claim.intent_id.into_uuid()).bind(entry.tenant_id.into_uuid()).bind(entry.owner_id.into_uuid()).bind(preparation.scope.agent_id.into_uuid()).bind(entry.dialogue_id.into_uuid()).bind(proof.claims.reason.as_str()).bind(integer(claim.generation)?).bind(proof.claims.decision_id.into_uuid()).bind(proof.claims.facts_sha256.into_bytes().to_vec()).bind(encoded).bind(entry.entry_id.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        if !finish(&mut tx, claim, "rejected", now).await? {
            return Ok(ResultState::LeaseLost);
        }
        // A content/owner lock may have waited. Check authority TTL at commit.
        let at = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        if !proof.claims.is_valid_at(at) {
            return Ok(ResultState::StalePreparation);
        }
        tx.commit()
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
        Ok(ResultState::Committed)
    }

    pub(super) async fn cancel_inactive_inner(
        &self,
        claim: DialogueIntentClaim,
    ) -> Result<ResultState, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Admission)
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Acquire, error))?;
        super::progress::lock_account(&mut tx, claim.actor).await?;
        lock_input(&mut tx, claim).await?;
        if sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM agent.dialogue_admission_outcomes WHERE intent_id=$1 AND outcome='cancelled')").bind(claim.intent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)? { return Ok(ResultState::Replayed); }
        if admitted(&mut tx, claim.intent_id).await? {
            return Ok(ResultState::AlreadyAdmitted);
        }
        if !lock_claim(&mut tx, claim).await? {
            return Ok(ResultState::LeaseLost);
        }
        let ids:(uuid::Uuid,uuid::Uuid)=sqlx::query_as("SELECT a.agent_id,d.dialogue_id FROM agent.dialogue_user_intents i JOIN agent.agent_dialogues d USING(dialogue_id) JOIN agent.owned_agents a ON a.agent_id=d.agent_id WHERE i.intent_id=$1").bind(claim.intent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        let agent = AgentId::from_uuid(ids.0).map_err(|_| Error::Corrupt)?;
        lock_space(&mut tx, claim, agent).await?;
        let status: String =
            sqlx::query_scalar("SELECT status FROM agent.owned_agents WHERE agent_id=$1 FOR SHARE")
                .bind(ids.0)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if status == "active" {
            return Ok(ResultState::StillActive);
        }
        if status != "archived" && status != "deleted" {
            return Err(Error::Corrupt);
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        sqlx::query("INSERT INTO agent.dialogue_admission_outcomes(intent_id,tenant_id,owner_id,agent_id,dialogue_id,outcome,reason,claim_generation,created_at_ms) VALUES($1,$2,$3,$4,$5,'cancelled','agent_inactive',$6,$7)").bind(claim.intent_id.into_uuid()).bind(claim.actor.tenant_id.into_uuid()).bind(claim.actor.principal_id.into_uuid()).bind(ids.0).bind(ids.1).bind(integer(claim.generation)?).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        if !finish(&mut tx, claim, "cancelled", now).await? {
            return Ok(ResultState::LeaseLost);
        }
        tx.commit()
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
        Ok(ResultState::Committed)
    }
}
pub(super) async fn lock_input(
    tx: &mut PgConnection,
    claim: DialogueIntentClaim,
) -> Result<(), Error> {
    require_user(claim.actor)?;
    sqlx::query("SELECT intent_id FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR NO KEY UPDATE").bind(claim.intent_id.into_uuid()).bind(claim.actor.tenant_id.into_uuid()).bind(claim.actor.principal_id.into_uuid()).fetch_optional(tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    Ok(())
}
pub(super) async fn admitted(tx: &mut PgConnection, intent: UserIntentId) -> Result<bool, Error> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent.dialogue_sessions WHERE source_intent_id=$1)",
    )
    .bind(intent.into_uuid())
    .fetch_one(tx)
    .await
    .map_err(db)
}
pub(super) async fn lock_claim(
    tx: &mut PgConnection,
    claim: DialogueIntentClaim,
) -> Result<bool, Error> {
    Ok(sqlx::query("SELECT intent_id FROM agent.dialogue_intent_admissions WHERE intent_id=$1 AND status='claimed' AND claim_owner=$2 AND claim_generation=$3 AND lease_expires_at_ms=$4 AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint FOR UPDATE").bind(claim.intent_id.into_uuid()).bind(claim.owner).bind(integer(claim.generation)?).bind(claim.lease_expires_at.unix_millis()).fetch_optional(tx).await.map_err(db)?.is_some())
}
pub(super) async fn lock_space(
    tx: &mut PgConnection,
    claim: DialogueIntentClaim,
    agent: AgentId,
) -> Result<(), Error> {
    sqlx::query("SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 FOR UPDATE OF s").bind(agent.into_uuid()).bind(claim.actor.tenant_id.into_uuid()).bind(claim.actor.principal_id.into_uuid()).fetch_optional(tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    Ok(())
}
pub(super) async fn finish(
    tx: &mut PgConnection,
    claim: DialogueIntentClaim,
    status: &str,
    now: UtcTimestamp,
) -> Result<bool, Error> {
    Ok(sqlx::query("UPDATE agent.dialogue_intent_admissions SET status=$4,claim_owner=NULL,lease_expires_at_ms=NULL,last_error_code=NULL,terminal_at_ms=$5 WHERE intent_id=$1 AND status='claimed' AND claim_owner=$2 AND claim_generation=$3 AND lease_expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint").bind(claim.intent_id.into_uuid()).bind(claim.owner).bind(integer(claim.generation)?).bind(status).bind(now.unix_millis()).execute(tx).await.map_err(db)?.rows_affected()==1)
}

pub(super) async fn insert_message(
    tx: &mut PgConnection,
    claim: DialogueIntentClaim,
    agent_id: AgentId,
    dialogue_id: AgentDialogueId,
    reference: &musteragent_content_ref::ContentRef,
    now: UtcTimestamp,
) -> Result<AgentDialogueEntry, Error> {
    insert_message_for_intent(
        tx,
        claim.actor,
        claim.intent_id,
        agent_id,
        dialogue_id,
        reference,
        now,
    )
    .await
}

/// The original publication owner also serves an authenticated receipt ACK.
/// An ACK proves its own custody and never invents a queue lease to publish.
pub(super) async fn insert_message_for_intent(
    tx: &mut PgConnection,
    actor: Actor,
    intent: UserIntentId,
    agent_id: AgentId,
    dialogue_id: AgentDialogueId,
    reference: &musteragent_content_ref::ContentRef,
    now: UtcTimestamp,
) -> Result<AgentDialogueEntry, Error> {
    let head = sqlx::query("SELECT * FROM agent.agent_dialogues WHERE dialogue_id=$1")
        .bind(dialogue_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
    let cursor = dialogue_row(&head)?
        .head
        .next()
        .map_err(|_| Error::Corrupt)?;
    let entry = AgentDialogueEntry {
        entry_id: DialogueEntryId::from_uuid(uuid::Uuid::new_v5(
            &intent.into_uuid(),
            b"dialogue-admission-outcome-entry@1",
        ))
        .map_err(|_| Error::Corrupt)?,
        dialogue_id,
        tenant_id: actor.tenant_id,
        owner_id: actor.principal_id,
        cursor,
        source_intent_id: Some(intent),
        author_agent_id: Some(agent_id),
        visible_revision: NonZeroU64::MIN,
        content: AgentDialogueContent::AgentMessage {
            content_ref: reference.clone(),
        },
        created_at: now,
        presentation: Default::default(),
    };
    retain_content_refs(
        &mut *tx,
        actor,
        agent_id,
        vec![("public_entry", entry.entry_id.into_uuid(), reference)],
    )
    .await?;
    sqlx::query("INSERT INTO agent.agent_dialogue_entries(entry_id,tenant_id,owner_id,dialogue_id,epoch,seq,kind,source_intent_id,author_agent_id,content_ref,visible_revision,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,'agent_message',$7,$8,$9,1,$10)")
            .bind(entry.entry_id.into_uuid()).bind(entry.tenant_id.into_uuid()).bind(entry.owner_id.into_uuid()).bind(entry.dialogue_id.into_uuid()).bind(integer(cursor.epoch)?).bind(integer(cursor.seq)?).bind(intent.into_uuid()).bind(agent_id.into_uuid()).bind(content_bytes(reference)).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
    sqlx::query("UPDATE agent.agent_dialogues SET head_seq=$2 WHERE dialogue_id=$1")
        .bind(entry.dialogue_id.into_uuid())
        .bind(integer(cursor.seq)?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    outbox(
        &mut *tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "agent_dialogue",
            aggregate: entry.dialogue_id.into_uuid(),
            cursor,
            event: "public_entry_published",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::PublicEntryPublished(
                    musteragent_persistence_codec::dialogue_codec::entry(&entry),
                )),
            },
            at: now,
        },
    )
    .await?;
    Ok(entry)
}
