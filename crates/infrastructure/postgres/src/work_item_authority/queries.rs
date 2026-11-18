use super::*;

impl PostgresWorkItemAuthority {
    pub(super) async fn inspect_inner(
        &self,
        r: InspectAgentWorkItem,
    ) -> Result<Option<WorkItemAuthorityRecord>> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Projection,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(super::codec::transaction)?;
        authorize(&mut tx, r.actor, r.scope, false).await?;
        let record = load(&mut tx, r.actor, r.scope, r.work_item_id, None).await?;
        tx.commit().await.map_err(super::codec::transaction)?;
        Ok(record)
    }

    pub(super) async fn list_inner(&self, r: ListAgentWorkItems) -> Result<WorkItemDirectoryPage> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Projection,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(super::codec::transaction)?;
        let result = list_in_transaction(&mut tx, r).await?;
        tx.commit().await.map_err(super::codec::transaction)?;
        Ok(result)
    }
}

pub(crate) async fn list_in_transaction(
    tx: &mut sqlx::PgConnection,
    r: ListAgentWorkItems,
) -> Result<WorkItemDirectoryPage> {
    if r.limit == 0 || r.limit > 100 {
        return Err(WorkItemAuthorityError::InvalidRequest);
    }
    authorize(tx, r.actor, r.scope, false).await?;
    let maximum: i64 = sqlx::query_scalar(
        "SELECT COALESCE(max(directory_seq),0) FROM collaboration.work_items \
            WHERE tenant_id=$1 AND owner_id=$2 AND space_id=$3 AND agent_id=$4",
    )
    .bind(r.actor.tenant_id.into_uuid())
    .bind(r.actor.principal_id.into_uuid())
    .bind(r.scope.space_id.into_uuid())
    .bind(r.scope.agent_id.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    let maximum = u64::try_from(maximum).map_err(|_| WorkItemAuthorityError::Corrupt)?;
    let cursor = r.cursor.unwrap_or(WorkItemDirectoryCursor {
        through: maximum,
        after: 0,
    });
    if cursor.after > cursor.through || cursor.through > maximum {
        return Err(WorkItemAuthorityError::InvalidRequest);
    }
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM collaboration.work_items \
            WHERE tenant_id=$1 AND owner_id=$2 AND space_id=$3 AND agent_id=$4 AND directory_seq<=$5")
            .bind(r.actor.tenant_id.into_uuid()).bind(r.actor.principal_id.into_uuid())
            .bind(r.scope.space_id.into_uuid()).bind(r.scope.agent_id.into_uuid()).bind(signed(cursor.through)?)
            .fetch_one(&mut *tx).await.map_err(db)?;
    let rows = sqlx::query(&format!(
        "{SELECT} WHERE t.tenant_id=$1 AND t.owner_id=$2 AND t.space_id=$3 AND t.agent_id=$4 \
            AND r.state_revision=t.state_revision AND t.directory_seq>$5 AND t.directory_seq<=$6 \
            ORDER BY t.directory_seq LIMIT $7"
    ))
    .bind(r.actor.tenant_id.into_uuid())
    .bind(r.actor.principal_id.into_uuid())
    .bind(r.scope.space_id.into_uuid())
    .bind(r.scope.agent_id.into_uuid())
    .bind(signed(cursor.after)?)
    .bind(signed(cursor.through)?)
    .bind(i64::from(r.limit) + 1)
    .fetch_all(&mut *tx)
    .await
    .map_err(db)?;
    let mut records = rows.iter().map(decode).collect::<Result<Vec<_>>>()?;
    let more = records.len() > usize::from(r.limit);
    records.truncate(usize::from(r.limit));
    let next = if more {
        records.last().map(|x| WorkItemDirectoryCursor {
            through: cursor.through,
            after: x.directory_seq,
        })
    } else {
        None
    };

    Ok(WorkItemDirectoryPage {
        records,
        through: cursor.through,
        total_count: u64::try_from(count).map_err(|_| WorkItemAuthorityError::Corrupt)?,
        next,
    })
}
