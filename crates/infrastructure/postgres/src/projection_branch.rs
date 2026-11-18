use std::collections::BTreeMap;

use musteragent_identity::{
    ContextBranchId, ConversationId, ConversationSeq, LedgerOffset, LedgerRecordId, TenantId,
};
use sqlx::{PgConnection, PgPool, Row};

use musteragent_projection::{
    PROJECTION_SNAPSHOT_BRANCH_KIND_PREFIX, PROJECTION_SNAPSHOT_ROOT_KIND, SubscriptionError,
    supported_projection_snapshot_kinds,
};

/// Immutable reachability view for one product projection build.
///
/// The active branch, lineage and Ledger head are captured within one
/// repeatable-read authority boundary before replay, and all records are
/// filtered against that immutable view.
pub(super) struct ProjectionBranchRoute {
    active_branch_id: Option<ContextBranchId>,
    branch_ceilings: BTreeMap<ContextBranchId, ConversationSeq>,
    root_ceiling: Option<ConversationSeq>,
}

impl ProjectionBranchRoute {
    pub(crate) async fn retracted_runs(
        &self,
        tx: &mut PgConnection,
        tenant_id: TenantId,
        conversation_id: ConversationId,
    ) -> Result<std::collections::BTreeSet<musteragent_identity::RunId>, SubscriptionError> {
        let rows = sqlx::query("SELECT m.run_id AS source_run_id, r.context_branch_id, r.retracted_sequence::text AS retracted_sequence
            FROM conversation.message_retractions r JOIN conversation.messages m ON m.tenant_id=r.tenant_id AND m.conversation_id=r.conversation_id AND m.message_id=r.message_id
            WHERE r.tenant_id=$1 AND r.conversation_id=$2")
            .bind(tenant_id.into_uuid()).bind(conversation_id.into_uuid()).fetch_all(&mut *tx).await.map_err(|_|SubscriptionError::SourceUnavailable)?;
        let mut runs = std::collections::BTreeSet::new();
        for row in rows {
            let branch = row
                .try_get::<Option<uuid::Uuid>, _>("context_branch_id")
                .map_err(|_| SubscriptionError::CorruptRecord)?
                .map(ContextBranchId::from_uuid)
                .transpose()
                .map_err(|_| SubscriptionError::CorruptRecord)?;
            let sequence = ConversationSeq::new(
                row.try_get::<String, _>("retracted_sequence")
                    .map_err(|_| SubscriptionError::CorruptRecord)?
                    .parse()
                    .map_err(|_| SubscriptionError::CorruptRecord)?,
            )
            .map_err(|_| SubscriptionError::CorruptRecord)?;
            if self.includes(branch, sequence)
                && let Some(run) = row
                    .try_get::<Option<uuid::Uuid>, _>("source_run_id")
                    .map_err(|_| SubscriptionError::CorruptRecord)?
            {
                runs.insert(
                    musteragent_identity::RunId::from_uuid(run)
                        .map_err(|_| SubscriptionError::CorruptRecord)?,
                );
            }
        }
        Ok(runs)
    }

    pub(super) async fn load(
        connection: &mut PgConnection,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        head: LedgerOffset,
        active_branch_id: Option<ContextBranchId>,
    ) -> Result<Self, SubscriptionError> {
        let Some(active_branch_id) = active_branch_id else {
            return Ok(Self {
                active_branch_id: None,
                branch_ceilings: BTreeMap::new(),
                root_ceiling: None,
            });
        };

        let rows = sqlx::query(
            "SELECT branch_id, parent_branch_id,
                    fork_from_sequence::text AS fork_from_sequence
             FROM conversation.context_branches
             WHERE tenant_id = $1 AND conversation_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_all(connection)
        .await
        .map_err(|_| SubscriptionError::SourceUnavailable)?;
        let mut branches = BTreeMap::new();
        for row in rows {
            let branch_id = ContextBranchId::from_uuid(
                row.try_get("branch_id")
                    .map_err(|_| SubscriptionError::CorruptRecord)?,
            )
            .map_err(|_| SubscriptionError::CorruptRecord)?;
            let parent_branch_id = row
                .try_get::<Option<uuid::Uuid>, _>("parent_branch_id")
                .map_err(|_| SubscriptionError::CorruptRecord)?
                .map(ContextBranchId::from_uuid)
                .transpose()
                .map_err(|_| SubscriptionError::CorruptRecord)?;
            let fork_from_sequence = row
                .try_get::<String, _>("fork_from_sequence")
                .map_err(|_| SubscriptionError::CorruptRecord)?
                .parse::<u64>()
                .ok()
                .and_then(|value| ConversationSeq::new(value).ok())
                .ok_or(SubscriptionError::CorruptRecord)?;
            if branches
                .insert(branch_id, (parent_branch_id, fork_from_sequence))
                .is_some()
            {
                return Err(SubscriptionError::CorruptRecord);
            }
        }

        let mut branch_ceilings = BTreeMap::new();
        let mut current = active_branch_id;
        let mut ceiling =
            ConversationSeq::new(head.get()).map_err(|_| SubscriptionError::CorruptRecord)?;
        let root_ceiling = loop {
            if branch_ceilings.insert(current, ceiling).is_some() {
                return Err(SubscriptionError::CorruptRecord);
            }
            let (parent, fork_from_sequence) = branches
                .get(&current)
                .copied()
                .ok_or(SubscriptionError::CorruptRecord)?;
            let inherited_ceiling = std::cmp::min(ceiling, fork_from_sequence);
            let Some(parent) = parent else {
                break inherited_ceiling;
            };
            current = parent;
            ceiling = inherited_ceiling;
        };

        Ok(Self {
            active_branch_id: Some(active_branch_id),
            branch_ceilings,
            root_ceiling: Some(root_ceiling),
        })
    }

    pub(super) fn snapshot_kind(&self) -> String {
        self.active_branch_id.map_or_else(
            || PROJECTION_SNAPSHOT_ROOT_KIND.to_owned(),
            |branch_id| format!("{PROJECTION_SNAPSHOT_BRANCH_KIND_PREFIX}{branch_id}"),
        )
    }

    pub(super) fn supported_snapshot_kinds(&self) -> Vec<String> {
        supported_projection_snapshot_kinds(self.active_branch_id)
    }

    pub(super) fn is_unbranched(&self) -> bool {
        self.active_branch_id.is_none()
    }

    pub(super) fn includes(
        &self,
        branch_id: Option<ContextBranchId>,
        sequence: ConversationSeq,
    ) -> bool {
        if self.active_branch_id.is_none() {
            return true;
        }
        match branch_id {
            Some(branch_id) => self
                .branch_ceilings
                .get(&branch_id)
                .is_some_and(|ceiling| sequence <= *ceiling),
            None => self.root_ceiling.is_some_and(|ceiling| sequence <= ceiling),
        }
    }

    pub(super) async fn record_branches(
        &self,
        pool: &PgPool,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        after: LedgerOffset,
        through: LedgerOffset,
    ) -> Result<BTreeMap<LedgerRecordId, Option<ContextBranchId>>, SubscriptionError> {
        if self.is_unbranched() {
            return Ok(BTreeMap::new());
        }
        let rows = sqlx::query(
            "SELECT record_id, context_branch_id
             FROM event_store.ledger_records
             WHERE tenant_id = $1
               AND conversation_id = $2
               AND conversation_seq > $3::bigint
               AND conversation_seq <= $4::bigint",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(after.get().to_string())
        .bind(through.get().to_string())
        .fetch_all(pool)
        .await
        .map_err(|_| SubscriptionError::SourceUnavailable)?;
        rows.into_iter()
            .map(|row| {
                let record_id = LedgerRecordId::from_uuid(
                    row.try_get("record_id")
                        .map_err(|_| SubscriptionError::CorruptRecord)?,
                )
                .map_err(|_| SubscriptionError::CorruptRecord)?;
                let branch_id = row
                    .try_get::<Option<uuid::Uuid>, _>("context_branch_id")
                    .map_err(|_| SubscriptionError::CorruptRecord)?
                    .map(ContextBranchId::from_uuid)
                    .transpose()
                    .map_err(|_| SubscriptionError::CorruptRecord)?;
                Ok((record_id, branch_id))
            })
            .collect()
    }
}
