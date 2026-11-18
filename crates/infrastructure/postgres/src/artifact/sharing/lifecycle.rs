use super::*;

pub(super) async fn transition(
    store: &PostgresArtifactStore,
    source: ArtifactPublicationSourceRef,
    retain: bool,
) -> Result<View, Error> {
    let mut tx = store
        .ledger
        .database()
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
        .await
        .map_err(|_| Error::Unavailable)?;
    let row = read::load(&mut tx, source, true).await?;
    let current = read::view(&mut tx, &row, true).await?;
    let state = match current.state {
        State::Revoked => 3_i16,
        State::Expired => 4,
        State::Prepared | State::Retained => {
            if retain {
                2
            } else {
                3
            }
        }
    };
    sqlx::query("UPDATE artifact.dialogue_publication_sources SET state=CASE WHEN $2=2 AND state=1 AND prepare_expires_at_ms<=artifact.dialogue_publication_now_ms() THEN 4 ELSE $2 END,title=CASE WHEN $2 IN(3,4) THEN NULL ELSE title END WHERE binding_id=$1")
        .bind(source.binding_id).bind(state).execute(&mut *tx).await.map_err(sql)?;
    let row = read::load(&mut tx, source, false).await?;
    let result = read::view(&mut tx, &row, true).await?;
    tx.commit().await.map_err(|_| Error::Unavailable)?;
    Ok(result)
}
pub(super) async fn reclaim(store: &PostgresArtifactStore, limit: u32) -> Result<u32, Error> {
    if !(1..=256).contains(&limit) {
        return Err(Error::Invalid);
    }
    let mut tx = store
        .ledger
        .database()
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
        .await
        .map_err(|_| Error::Unavailable)?;
    let at = now(&mut tx).await?;
    let result=sqlx::query("WITH expired AS(SELECT binding_id FROM artifact.dialogue_publication_sources
       WHERE state=1 AND prepare_expires_at_ms<=$1 ORDER BY prepare_expires_at_ms,binding_id LIMIT $2 FOR UPDATE SKIP LOCKED)
       UPDATE artifact.dialogue_publication_sources SET state=4,title=NULL WHERE binding_id IN(SELECT binding_id FROM expired)")
       .bind(at).bind(i64::from(limit)).execute(&mut *tx).await.map_err(sql)?.rows_affected();
    tx.commit().await.map_err(|_| Error::Unavailable)?;
    u32::try_from(result).map_err(|_| Error::Invariant)
}
