//! Immutable public-source reads for private Context. Current UI revisions are
//! never substituted into a frozen source chain. Authorization is rechecked on
//! every bounded read, independently of the caller's previously returned cut.
use crate::dialogue_codec::{content_bytes, integer, nz, unsigned};
use musteragent_content_ref::Sha256;
use musteragent_execution_context::*;
use musteragent_identity::{Actor, AgentDialogueId, AgentId, PrincipalKind};
use musteragent_runtime_session::DialogueStoreError as Error;
use sqlx::{PgConnection, Row};

#[derive(Clone)]
pub struct PostgresDialogueContextSourceStore {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresDialogueContextSourceStore {
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }

    async fn read(&self) -> Result<crate::GovernedPostgresTransaction<'static>, Error> {
        self.database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Context,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(transaction_error)
    }

    pub async fn cut(
        &self,
        actor: Actor,
        agent: AgentId,
        dialogue: AgentDialogueId,
        through: DialogueCursor,
    ) -> Result<DialogueContextCut, Error> {
        let mut tx = self.read().await?;
        let cut = load_cut(&mut tx, actor, agent, dialogue, through).await?;
        tx.commit().await.map_err(transaction_error)?;
        Ok(cut)
    }

    /// At most one bounded page. The caller must consume every contiguous page
    /// through the cut before it may issue a primary-model request.
    pub async fn page(
        &self,
        actor: Actor,
        cut: &DialogueContextCut,
        from_seq: u64,
    ) -> Result<DialogueContextSourcePage, Error> {
        let mut tx = self.read().await?;
        let page = load_page(&mut tx, actor, cut, from_seq).await?;
        tx.commit().await.map_err(transaction_error)?;
        Ok(page)
    }

    /// Upgrade only the in-memory view of an explicitly legacy binding. Both
    /// current authorization and its historical complete source chain are
    /// checked in one snapshot. The durable page/ref/Ledger remains unchanged.
    pub(crate) async fn legacy_bound_page(
        &self,
        run: musteragent_identity::RunId,
        cut: &DialogueContextCut,
        from_seq: u64,
        bytes: &[u8],
    ) -> Result<DialogueContextSourcePage, Error> {
        let mut tx = self.read().await?;
        if run::run_cut(&mut tx, run, false).await?.as_ref() != Some(cut) {
            return Err(Error::Unavailable);
        }
        let actor = Actor {
            tenant_id: cut.tenant_id,
            principal_id: cut.owner_id,
            principal_kind: PrincipalKind::User,
        };
        let page = load_page(&mut tx, actor, cut, from_seq).await?;
        musteragent_persistence_codec::dialogue_context_codec::verify_legacy_page(bytes, &page)
            .map_err(|_| Error::Corrupt)?;
        tx.commit().await.map_err(transaction_error)?;
        Ok(page)
    }
}

/// A bounded fact from the same authoritative source snapshot, including pages
/// already covered by compaction. Callers first authorize and load this cut.
pub(crate) async fn has_input_attachments(
    tx: &mut PgConnection,
    cut: &DialogueContextCut,
) -> Result<bool, Error> {
    cut.validate().map_err(|_| Error::Corrupt)?;
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.agent_dialogue_entries WHERE tenant_id=$1 AND owner_id=$2 AND dialogue_id=$3 AND epoch=$4 AND seq<=$5 AND input_attachment_count>0)")
        .bind(cut.tenant_id.into_uuid()).bind(cut.owner_id.into_uuid())
        .bind(cut.dialogue_id.into_uuid()).bind(integer(cut.through.epoch)?)
        .bind(integer(cut.through.seq)?).fetch_one(&mut *tx).await.map_err(|_| Error::Infrastructure)
}

pub(crate) async fn load_cut(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    dialogue: AgentDialogueId,
    through: DialogueCursor,
) -> Result<DialogueContextCut, Error> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(Error::Unavailable);
    }
    through.validate().map_err(|_| Error::Corrupt)?;
    // Explicit ownership and active lifecycle, not knowledge of a dialogue ID.
    let row = sqlx::query("SELECT d.epoch,d.head_seq FROM agent.agent_dialogues d JOIN agent.owned_agents a ON a.agent_id=d.agent_id JOIN agent.owner_spaces s ON s.space_id=a.space_id AND s.tenant_id=d.tenant_id AND s.owner_id=d.owner_id WHERE d.dialogue_id=$1 AND d.tenant_id=$2 AND d.owner_id=$3 AND d.agent_id=$4 AND a.status='active'")
        .bind(dialogue.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(agent.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    let head = DialogueCursor {
        epoch: unsigned(row.try_get("epoch").map_err(db)?)?,
        seq: unsigned(row.try_get("head_seq").map_err(db)?)?,
    };
    through
        .require_at_or_before(head)
        .map_err(|_| Error::RevisionConflict)?;
    let mut cut = DialogueContextCut {
        tenant_id: actor.tenant_id,
        owner_id: actor.principal_id,
        agent_id: agent,
        dialogue_id: dialogue,
        through,
        entry_count: through.seq,
        source_chain_sha256: Sha256::digest([]),
    };
    cut.source_chain_sha256 = if through.seq == 0 {
        cut.empty_chain_hash()
    } else {
        chain_at(tx, &cut, through.seq).await?
    };
    cut.validate().map_err(|_| Error::Corrupt)?;
    Ok(cut)
}

pub(crate) async fn chain_at(
    tx: &mut PgConnection,
    cut: &DialogueContextCut,
    seq: u64,
) -> Result<Sha256, Error> {
    if seq == 0 {
        return Ok(cut.empty_chain_hash());
    }
    let bytes:Vec<u8> = sqlx::query_scalar("SELECT chain_sha256 FROM agent.dialogue_context_source_entries WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3 AND dialogue_id=$4 AND epoch=$5 AND seq=$6")
        .bind(cut.tenant_id.into_uuid()).bind(cut.owner_id.into_uuid()).bind(cut.agent_id.into_uuid()).bind(cut.dialogue_id.into_uuid()).bind(integer(cut.through.epoch)?).bind(integer(seq)?)
        .fetch_optional(tx).await.map_err(db)?.ok_or(Error::Corrupt)?;
    Sha256::from_bytes(bytes).map_err(|_| Error::Corrupt)
}

mod pages;
pub(super) use pages::{load_page, load_pages};

// Keep this classification local to the Context source owner. Query and row
// decode failures share SQLx's type, but only actual transient database faults
// permit dependency recovery; a missing column is never an outage.
fn db(error: sqlx::Error) -> Error {
    match crate::database_failure::classify(&error) {
        crate::database_failure::PostgresFailureClass::Unavailable => Error::Infrastructure,
        crate::database_failure::PostgresFailureClass::Invalid => Error::Corrupt,
    }
}
fn transaction_error(error: crate::PostgresTransactionError) -> Error {
    // The kernel's coarse failure_class is for telemetry/definite-abort policy,
    // not source integrity. Preserve the original SQLx recovery classification.
    match error {
        crate::PostgresTransactionError::Database(error) => db(error),
        crate::PostgresTransactionError::DeadlineExceeded { .. }
        | crate::PostgresTransactionError::CommitOutcomeUnknown { .. } => Error::Infrastructure,
        crate::PostgresTransactionError::Policy(_) => Error::Corrupt,
    }
}

pub(crate) mod run;

pub(crate) mod checkpoint;
pub(crate) mod work_item_erasure;

#[cfg(test)]
mod tests {
    use super::{Error, db, transaction_error};

    #[tokio::test]
    async fn transaction_wrapping_preserves_database_recovery_classification() {
        let pg = musteragent_testkit::IsolatedPostgres::start_with_pgvector()
            .await
            .unwrap();
        let pool = pg.pool().await.unwrap();
        for (code, expected) in [
            ("57P01", Error::Infrastructure),
            ("57P02", Error::Infrastructure),
            ("57P03", Error::Infrastructure),
            ("53300", Error::Infrastructure),
            ("55006", Error::Infrastructure),
            ("57014", Error::Infrastructure),
            ("40001", Error::Infrastructure),
            ("55P03", Error::Infrastructure),
            ("42703", Error::Corrupt),
            ("42501", Error::Corrupt),
            ("23514", Error::Corrupt),
        ] {
            for wrapped in [false, true] {
                let sql = format!(
                    "DO $$ BEGIN RAISE SQLSTATE '{code}' USING MESSAGE = 'isolated classification fixture'; END $$"
                );
                let database = sqlx::query(&sql).execute(&pool).await.unwrap_err();
                let error = if wrapped {
                    transaction_error(crate::PostgresTransactionError::Database(database))
                } else {
                    db(database)
                };
                assert_eq!(error, expected, "{code}, wrapped={wrapped}");
            }
        }
        pool.close().await;
    }
}

pub(crate) mod approval_context;
mod device;
mod device_content;
mod device_contents;
mod device_erasure;
pub use device::PostgresDeviceContextStore;
