use musteragent_content_ref::Sha256;
use musteragent_identity::ContextBranchId;
use musteragent_projection::{
    PROJECTION_FAILURE_BACKFILL_PAGE_SIZE, ProjectionRunFailureQuery, ProjectionRunFailureSource,
    RetainedProjectionRunEvent, SubscriptionError,
};
use musteragent_runtime_contract::ApplicationFuture;
use sqlx::{PgPool, Postgres, QueryBuilder, Row as _};

use crate::ledger::decode_record_metadata;
use crate::numeric::encode_u64;

pub(super) struct RetainedRunEvents<'a>(pub &'a PgPool);

impl ProjectionRunFailureSource for RetainedRunEvents<'_> {
    fn load_run_events(
        &self,
        query: ProjectionRunFailureQuery,
    ) -> ApplicationFuture<'_, Result<Vec<RetainedProjectionRunEvent>, SubscriptionError>> {
        Box::pin(async move {
            // ledger_records_run_idx provides the descending Run sequence
            // cursor. LEFT JOIN preserves missing physical payload authority.
            let mut sql = QueryBuilder::<Postgres>::new(
                "SELECT record.*, event.encoded_event AS domain_payload,
                        event.payload_hash AS domain_payload_hash
                 FROM event_store.ledger_records AS record
                 LEFT JOIN event_store.domain_events AS event ON event.record_id = record.record_id
                 WHERE record.tenant_id = ",
            );
            sql.push_bind(query.tenant_id.into_uuid())
                .push(" AND record.conversation_id = ")
                .push_bind(query.conversation_id.into_uuid())
                .push(" AND record.run_id = ")
                .push_bind(query.run_id.into_uuid())
                .push(" AND record.home_epoch = ")
                .push_bind(
                    encode_u64(query.home_epoch.get(), "home_epoch")
                        .map_err(|_| SubscriptionError::CorruptRecord)?,
                )
                .push(" AND record.record_kind = 1 AND record.conversation_seq <= ")
                .push_bind(
                    encode_u64(query.through_sequence.get(), "through_sequence")
                        .map_err(|_| SubscriptionError::CorruptRecord)?,
                );
            if let Some(before) = query.before_run_sequence {
                sql.push(" AND record.run_seq < ").push_bind(
                    encode_u64(before.get(), "before_run_sequence")
                        .map_err(|_| SubscriptionError::CorruptRecord)?,
                );
            }
            sql.push(" ORDER BY record.run_seq DESC LIMIT ")
                .push_bind(PROJECTION_FAILURE_BACKFILL_PAGE_SIZE as i64);
            sql.build()
                .fetch_all(self.0)
                .await
                .map_err(|_| SubscriptionError::SourceUnavailable)?
                .into_iter()
                .map(|row| {
                    let metadata = decode_record_metadata(&row)
                        .map_err(|_| SubscriptionError::CorruptRecord)?;
                    let context_branch_id = row
                        .try_get::<Option<uuid::Uuid>, _>("context_branch_id")
                        .map_err(|_| SubscriptionError::CorruptRecord)?
                        .map(ContextBranchId::from_uuid)
                        .transpose()
                        .map_err(|_| SubscriptionError::CorruptRecord)?;
                    let payload = row
                        .try_get::<Option<Vec<u8>>, _>("domain_payload")
                        .map_err(|_| SubscriptionError::CorruptRecord)?;
                    let child_hash = row
                        .try_get::<Option<Vec<u8>>, _>("domain_payload_hash")
                        .map_err(|_| SubscriptionError::CorruptRecord)?;
                    let payload = match (payload, child_hash) {
                        (None, None) => None,
                        (Some(payload), Some(hash)) => {
                            let hash = Sha256::from_bytes(hash)
                                .map_err(|_| SubscriptionError::CorruptRecord)?;
                            if hash != metadata.payload_hash {
                                return Err(SubscriptionError::CorruptRecord);
                            }
                            Some(payload)
                        }
                        _ => return Err(SubscriptionError::CorruptRecord),
                    };
                    Ok(RetainedProjectionRunEvent {
                        metadata,
                        context_branch_id,
                        payload,
                    })
                })
                .collect()
        })
    }
}
