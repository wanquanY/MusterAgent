use musteragent_identity::{ConversationSeq, MessageId, RunId};
use musteragent_projection::{
    ProjectionRunInputQuery, ProjectionRunInputSource, RetainedProjectionRunInput,
    SubscriptionError,
};
use musteragent_runtime_contract::ApplicationFuture;
use sqlx::{PgPool, Row as _};

use crate::numeric::{decode_u64, encode_u64};

pub(super) struct RetainedRunInputs<'a>(pub &'a PgPool);

impl ProjectionRunInputSource for RetainedRunInputs<'_> {
    fn load_run_inputs(
        &self,
        query: ProjectionRunInputQuery,
    ) -> ApplicationFuture<'_, Result<Vec<RetainedProjectionRunInput>, SubscriptionError>> {
        Box::pin(async move {
            let home_epoch = encode_u64(query.home_epoch.get(), "home_epoch")
                .map_err(|_| SubscriptionError::CorruptRecord)?;
            let through_sequence = encode_u64(query.through_sequence.get(), "through_sequence")
                .map_err(|_| SubscriptionError::CorruptRecord)?;
            let rows = sqlx::query(
                "SELECT run.run_id,
                        message.message_id,
                        run.admitted_sequence
                 FROM runtime.runs AS run
                 JOIN conversation.messages AS message
                   ON message.tenant_id = run.tenant_id
                  AND message.conversation_id = run.conversation_id
                  AND message.run_id = run.run_id
                  AND message.committed_sequence = run.admitted_sequence
                 WHERE run.tenant_id = $1
                   AND run.conversation_id = $2
                   AND run.home_epoch = $3
                   AND run.admitted_sequence <= $4
                   AND run.delegation_id IS NULL
                   AND message.role = 1
                   AND NOT EXISTS (
                       SELECT 1
                       FROM runtime.loop_controls AS control
                       WHERE control.tenant_id = message.tenant_id
                         AND control.message_id = message.message_id
                   )
                 ORDER BY run.admitted_sequence ASC, run.run_id ASC",
            )
            .bind(query.tenant_id.into_uuid())
            .bind(query.conversation_id.into_uuid())
            .bind(home_epoch)
            .bind(through_sequence)
            .fetch_all(self.0)
            .await
            .map_err(|_| SubscriptionError::SourceUnavailable)?;

            rows.into_iter()
                .map(|row| {
                    let run_id = RunId::from_uuid(
                        row.try_get("run_id")
                            .map_err(|_| SubscriptionError::CorruptRecord)?,
                    )
                    .map_err(|_| SubscriptionError::CorruptRecord)?;
                    let input_message_id = MessageId::from_uuid(
                        row.try_get("message_id")
                            .map_err(|_| SubscriptionError::CorruptRecord)?,
                    )
                    .map_err(|_| SubscriptionError::CorruptRecord)?;
                    let admitted_sequence_raw: i64 = row
                        .try_get("admitted_sequence")
                        .map_err(|_| SubscriptionError::CorruptRecord)?;
                    let admitted_sequence = ConversationSeq::new(
                        decode_u64(admitted_sequence_raw, "admitted_sequence")
                            .map_err(|_| SubscriptionError::CorruptRecord)?,
                    )
                    .map_err(|_| SubscriptionError::CorruptRecord)?;
                    Ok(RetainedProjectionRunInput {
                        run_id,
                        input_message_id,
                        admitted_sequence,
                    })
                })
                .collect()
        })
    }
}
