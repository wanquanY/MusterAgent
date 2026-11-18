use super::*;

pub(super) async fn load(
    db: &mut PgConnection,
    source: ArtifactPublicationSourceRef,
    lock: bool,
) -> Result<PgRow, Error> {
    source.validate()?;
    let query = if lock {
        "SELECT * FROM artifact.dialogue_publication_sources WHERE binding_id=$1 FOR UPDATE"
    } else {
        "SELECT * FROM artifact.dialogue_publication_sources WHERE binding_id=$1"
    };
    let row = sqlx::query(query)
        .bind(source.binding_id)
        .fetch_optional(db)
        .await
        .map_err(sql)?
        .ok_or(Error::Forbidden)?;
    if codec::source(&row)? != source {
        return Err(Error::Forbidden);
    }
    Ok(row)
}
/// A prepared delegation remains read-only and exact. This is a fresh custody
/// check, not a forged User input or a current-execution grant.
async fn current(db: &mut PgConnection, row: &PgRow) -> Result<bool, Error> {
    let tenant: Uuid = row.try_get("tenant_id").map_err(sql)?;
    let owner: Uuid = row.try_get("owner_id").map_err(sql)?;
    let agent: Uuid = row.try_get("agent_id").map_err(sql)?;
    let dialogue: Uuid = row.try_get("dialogue_id").map_err(sql)?;
    let entry: Uuid = row.try_get("entry_id").map_err(sql)?;
    let artifact: Uuid = row.try_get("artifact_id").map_err(sql)?;
    let version: Uuid = row.try_get("artifact_version_id").map_err(sql)?;
    // Existing Original Artifact custody predicates; archived only disables
    // Agent execution. Permanent deletion or invalid lineage denies sharing.
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owned_agents a
      JOIN agent.owner_spaces s USING(space_id)
      JOIN agent.agent_dialogues d ON d.agent_id=a.agent_id AND d.dialogue_id=a.dialogue_id
        AND d.tenant_id=s.tenant_id AND d.owner_id=s.owner_id
      JOIN agent.agent_dialogue_entries e ON e.dialogue_id=d.dialogue_id AND e.tenant_id=d.tenant_id AND e.owner_id=d.owner_id
      JOIN artifact.artifacts f ON f.artifact_id=$6 AND f.tenant_id=s.tenant_id
      JOIN artifact.artifact_versions v ON v.artifact_id=f.artifact_id AND v.artifact_version_id=$7
      WHERE s.tenant_id=$1 AND s.owner_id=$2 AND a.agent_id=$3 AND d.dialogue_id=$4 AND e.entry_id=$5
       AND a.status IN('active','archived') AND f.lifecycle IN(1,2) AND v.work_item_erasure_job_id IS NULL
       AND ((e.kind='artifact' AND e.artifact_id=f.artifact_id AND resource.agent_artifact_owned(s.tenant_id,a.agent_id,f.artifact_id))
         OR (e.kind='erased' AND resource.account_file_owned(s.tenant_id,s.owner_id,a.agent_id,d.dialogue_id,e.entry_id,f.artifact_id))))")
       .bind(tenant).bind(owner).bind(agent).bind(dialogue).bind(entry).bind(artifact).bind(version)
       .fetch_one(db).await.map_err(sql)
}
pub(super) async fn view(
    db: &mut PgConnection,
    row: &PgRow,
    check_current: bool,
) -> Result<View, Error> {
    let source = codec::source(row)?;
    let mut state = codec::state(row)?;
    if state == State::Prepared
        && now(db).await?
            >= row
                .try_get::<i64, _>("prepare_expires_at_ms")
                .map_err(sql)?
    {
        state = State::Expired;
    }
    if matches!(state, State::Prepared | State::Retained)
        && check_current
        && !current(db, row).await?
    {
        state = State::Revoked;
    }
    if matches!(state, State::Expired | State::Revoked) {
        return Ok(View {
            source,
            state,
            descriptor: None,
        });
    }
    let refs=sqlx::query("SELECT x.content_index,x.content_id,x.content_media_type,x.content_byte_length::text AS content_byte_length,x.content_hash,
       c.state=2 AND c.tenant_id=$2 AND c.media_type=x.content_media_type AND c.byte_length=x.content_byte_length AND c.content_hash=x.content_hash AS ready
       FROM artifact.dialogue_publication_source_contents x JOIN artifact.content_refs c USING(content_id)
       WHERE x.binding_id=$1 ORDER BY x.content_index")
       .bind(source.binding_id).bind(row.try_get::<Uuid,_>("tenant_id").map_err(sql)?)
       .fetch_all(&mut *db).await.map_err(sql)?;
    if refs.is_empty() || refs.len() > MAX_PUBLICATION_DEPENDENCIES + 1 {
        return Err(Error::Invariant);
    }
    let mut contents = Vec::with_capacity(refs.len());
    for (index, r) in refs.iter().enumerate() {
        if !r.try_get::<bool, _>("ready").map_err(sql)? {
            return Ok(View {
                source,
                state: State::Revoked,
                descriptor: None,
            });
        }
        if r.try_get::<i32, _>("content_index").map_err(sql)? != index as i32 {
            return Err(Error::Invariant);
        }
        contents.push(codec::reference(r)?);
    }
    let agent_id =
        AgentId::from_uuid(row.try_get("agent_id").map_err(sql)?).map_err(|_| Error::Invariant)?;
    let artifact_id = ArtifactId::from_uuid(row.try_get("artifact_id").map_err(sql)?)
        .map_err(|_| Error::Invariant)?;
    let title = row
        .try_get::<Option<String>, _>("title")
        .map_err(sql)?
        .ok_or(Error::Invariant)?;
    let prepared = row.try_get::<i64, _>("prepared_at_ms").map_err(sql)?;
    let expires = row
        .try_get::<i64, _>("prepare_expires_at_ms")
        .map_err(sql)?;
    let request_hash =
        Sha256::from_bytes(row.try_get::<Vec<u8>, _>("request_sha256").map_err(sql)?)
            .map_err(|_| Error::Invariant)?;
    if codec::source_hash(
        request_hash,
        source.binding_id,
        agent_id,
        artifact_id,
        &title,
        &contents,
        super::codec::SharingWindow { prepared, expires },
    ) != source.source_sha256
    {
        return Err(Error::Invariant);
    }
    let descriptor = ArtifactPublicationSourceDescriptor {
        source,
        agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(sql)?)
            .map_err(|_| Error::Invariant)?,
        artifact_id: ArtifactId::from_uuid(row.try_get("artifact_id").map_err(sql)?)
            .map_err(|_| Error::Invariant)?,
        artifact_version_id: ArtifactVersionId::from_uuid(
            row.try_get("artifact_version_id").map_err(sql)?,
        )
        .map_err(|_| Error::Invariant)?,
        title: row
            .try_get::<Option<String>, _>("title")
            .map_err(sql)?
            .ok_or(Error::Invariant)?,
        root: contents.remove(0),
        dependencies: contents,
        prepared_at: UtcTimestamp::from_unix_millis(row.try_get("prepared_at_ms").map_err(sql)?),
        prepare_expires_at: UtcTimestamp::from_unix_millis(
            row.try_get("prepare_expires_at_ms").map_err(sql)?,
        ),
    };
    Ok(View {
        source,
        state,
        descriptor: Some(descriptor),
    })
}
pub(super) async fn inspect(
    store: &PostgresArtifactStore,
    source: ArtifactPublicationSourceRef,
) -> Result<View, Error> {
    let mut tx = store
        .ledger
        .database()
        .read_unit_of_work(
            musteragent_telemetry::DbCommitKind::Resource,
            crate::PostgresTransactionIsolation::ReadCommitted,
        )
        .await
        .map_err(|_| Error::Unavailable)?;
    let row = load(&mut tx, source, false).await?;
    let result = view(&mut tx, &row, true).await?;
    tx.commit().await.map_err(|_| Error::Unavailable)?;
    Ok(result)
}
