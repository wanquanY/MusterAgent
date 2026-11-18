//! Read authorization reuses the same input snapshot owner as acceptance.
use musteragent_runtime_contract::{ApplicationFuture, AttachmentRef};
use musteragent_runtime_session::{
    DialogueInputAttachmentAccess, DialogueInputAttachmentQuery, DialogueStoreError,
};

pub struct PostgresDialogueInputAttachmentAccess {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresDialogueInputAttachmentAccess {
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
}
impl DialogueInputAttachmentAccess for PostgresDialogueInputAttachmentAccess {
    fn resolve(
        &self,
        query: DialogueInputAttachmentQuery,
    ) -> ApplicationFuture<'_, Result<AttachmentRef, DialogueStoreError>> {
        Box::pin(async move {
            query.validate()?;
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Context,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            let reference = crate::agent_dialogue::attachments::resolve(
                &mut tx,
                query.actor,
                query.dialogue_id,
                query.entry_id,
                query.cursor,
                query.ordinal,
            )
            .await?;
            tx.commit()
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?;
            Ok(reference)
        })
    }
}
