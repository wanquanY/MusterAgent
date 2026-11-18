use super::*;

pub(super) async fn prepare(
    store: &PostgresArtifactStore,
    request: Prepare,
) -> Result<View, Error> {
    request.validate()?;
    let mut tx = store
        .ledger
        .database()
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
        .await
        .map_err(|_| Error::Unavailable)?;
    // Original owner order: Space -> Dialogue -> Agent -> Artifact -> Content.
    // The per-command advisory lock only deduplicates this command identity.
    let scope=sqlx::query("SELECT s.space_id,d.agent_id FROM agent.agent_dialogues d
       JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id
       JOIN agent.owner_spaces s ON s.space_id=a.space_id AND s.tenant_id=d.tenant_id AND s.owner_id=d.owner_id
       WHERE d.tenant_id=$1 AND d.owner_id=$2 AND d.dialogue_id=$3 AND s.product='musteragent' FOR SHARE OF s")
       .bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(request.dialogue_id.into_uuid())
       .fetch_optional(&mut *tx).await.map_err(sql)?.ok_or(Error::Forbidden)?;
    let agent = AgentId::from_uuid(scope.try_get("agent_id").map_err(sql)?)
        .map_err(|_| Error::Invariant)?;
    sqlx::query("SELECT dialogue_id FROM agent.agent_dialogues WHERE dialogue_id=$1 FOR SHARE")
        .bind(request.dialogue_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(sql)?;
    sqlx::query("SELECT agent_id FROM agent.owned_agents WHERE agent_id=$1 FOR SHARE")
        .bind(agent.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(sql)?;
    let command_key = format!(
        "dialogue-publication:{}:{}:{}",
        request.actor.tenant_id, request.actor.principal_id, request.command_id
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(command_key)
        .execute(&mut *tx)
        .await
        .map_err(sql)?;
    let hash = codec::request_hash(&request);
    if let Some(row)=sqlx::query("SELECT * FROM artifact.dialogue_publication_sources WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3 FOR UPDATE")
      .bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(request.command_id.into_uuid())
      .fetch_optional(&mut *tx).await.map_err(sql)? {
        if row.try_get::<Vec<u8>,_>("request_sha256").map_err(sql)?.as_slice()!=hash.into_bytes().as_slice(){return Err(Error::Conflict);}
        let result=read::view(&mut tx,&row,true).await?;
        tx.commit().await.map_err(|_|Error::Unavailable)?;
        return Ok(result);
    }
    let lookup = super::super::dialogue_artifact::bind_with_custody(
        &mut tx,
        request.actor,
        request.dialogue_id,
        request.entry_id,
        true,
    )
    .await
    .map_err(authority)?;
    sqlx::query("SELECT artifact_id FROM artifact.artifacts WHERE tenant_id=$1 AND artifact_id=$2 FOR SHARE")
        .bind(request.actor.tenant_id.into_uuid()).bind(lookup.artifact.artifact_id.into_uuid()).fetch_one(&mut *tx).await.map_err(sql)?;
    let binding = super::super::dialogue_artifact::bind_with_custody(
        &mut tx,
        request.actor,
        request.dialogue_id,
        request.entry_id,
        true,
    )
    .await
    .map_err(authority)?;
    if binding.agent_id != agent
        || binding.artifact.kind != ArtifactKind::File
        || matches!(binding.artifact.origin, ArtifactOrigin::GovernedImport)
    {
        return Err(Error::Forbidden);
    }
    let version = super::super::dialogue_artifact::versions(
        &mut tx,
        &binding,
        request.actor,
        binding.artifact.current_version,
        None,
        None,
        1,
    )
    .await
    .map_err(authority)?
    .pop()
    .ok_or(Error::SourceChanged)?;
    if version.artifact_version_id != request.expected_head
        || version.version != binding.artifact.current_version
    {
        return Err(Error::SourceChanged);
    }
    let AgentArtifactContent::Ready {
        root,
        mut dependencies,
    } = version.content
    else {
        return Err(Error::Revoked);
    };
    if !musteragent_runtime_session::agent_artifact_sharing_supported(&root.media_type) {
        return Err(Error::Forbidden);
    }
    if dependencies.len() > MAX_PUBLICATION_DEPENDENCIES {
        return Err(Error::Invalid);
    }
    dependencies.sort_by_key(|c| c.content_id);
    let mut refs = vec![root];
    refs.extend(dependencies);
    let mut ids = refs
        .iter()
        .map(|c| c.content_id.into_uuid())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    let ready:Vec<Uuid>=sqlx::query_scalar("SELECT content_id FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=ANY($2) AND state=2 ORDER BY content_id FOR SHARE")
        .bind(request.actor.tenant_id.into_uuid()).bind(&ids).fetch_all(&mut *tx).await.map_err(sql)?;
    if ids != ready {
        return Err(Error::Revoked);
    }
    // Keep title provenance with the frozen presentation metadata. An unknown
    // historical name uses a neutral label, never an unprovable private copy.
    let metadata=sqlx::query("SELECT CASE WHEN n.node_id IS NOT NULL THEN n.name_source_known ELSE a.title_source_known END AS known,
       CASE WHEN n.node_id IS NOT NULL THEN n.name_source_invocation_id ELSE a.title_source_invocation_id END AS producer
       FROM artifact.artifacts a LEFT JOIN resource.nodes n ON n.tenant_id=a.tenant_id AND n.file_artifact_id=a.artifact_id
        AND n.scope_kind=4 AND n.scope_id=$2 AND n.kind=2 AND n.lifecycle=1 AND n.name_erasure_job_id IS NULL
       WHERE a.artifact_id=$1")
       .bind(binding.artifact.artifact_id.into_uuid()).bind(agent.into_uuid()).fetch_one(&mut *tx).await.map_err(sql)?;
    let known: bool = metadata.try_get("known").map_err(sql)?;
    let title = if known {
        binding.artifact.title.clone()
    } else {
        "File".to_owned()
    };
    let producer: Option<Uuid> = if known {
        metadata.try_get("producer").map_err(sql)?
    } else {
        None
    };
    let prepared = now(&mut tx).await?;
    let expires = prepared
        .checked_add(PUBLICATION_PREPARE_TTL_MILLIS)
        .ok_or(Error::Invariant)?;
    let binding_id = Uuid::now_v7();
    let digest = codec::source_hash(
        hash,
        binding_id,
        agent,
        binding.artifact.artifact_id,
        &title,
        &refs,
        super::codec::SharingWindow { prepared, expires },
    );
    sqlx::query("INSERT INTO artifact.dialogue_publication_sources(binding_id,tenant_id,owner_id,agent_id,dialogue_id,entry_id,artifact_id,artifact_version_id,command_id,share_id,publication_revision_id,request_sha256,source_sha256,state,title,title_source_invocation_id,prepared_at_ms,prepare_expires_at_ms)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,1,$14,$15,$16,$17)")
       .bind(binding_id).bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(agent.into_uuid())
       .bind(request.dialogue_id.into_uuid()).bind(request.entry_id.into_uuid()).bind(binding.artifact.artifact_id.into_uuid()).bind(request.expected_head.into_uuid())
       .bind(request.command_id.into_uuid()).bind(request.audience.share_id).bind(request.audience.publication_revision_id)
       .bind(hash.into_bytes().as_slice()).bind(digest.into_bytes().as_slice()).bind(&title).bind(producer).bind(prepared).bind(expires)
       .execute(&mut *tx).await.map_err(sql)?;
    for (index, reference) in refs.iter().enumerate() {
        sqlx::query("INSERT INTO artifact.dialogue_publication_source_contents(binding_id,content_index,content_id,content_media_type,content_byte_length,content_hash) VALUES($1,$2,$3,$4,$5::numeric,$6)")
          .bind(binding_id).bind(index as i32).bind(reference.content_id.into_uuid()).bind(reference.media_type.as_str())
          .bind(reference.byte_length.to_string()).bind(reference.sha256.into_bytes().as_slice()).execute(&mut *tx).await.map_err(sql)?;
        sqlx::query("INSERT INTO agent.dialogue_content_references(tenant_id,owner_id,agent_id,reference_kind,reference_id,content_id) VALUES($1,$2,$3,'artifact_publication_source',$4,$5) ON CONFLICT DO NOTHING")
          .bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(agent.into_uuid()).bind(binding_id).bind(reference.content_id.into_uuid())
          .execute(&mut *tx).await.map_err(sql)?;
    }
    let source = ArtifactPublicationSourceRef {
        binding_id,
        audience: request.audience,
        source_sha256: digest,
    };
    let row = read::load(&mut tx, source, false).await?;
    let result = read::view(&mut tx, &row, true).await?;
    tx.commit().await.map_err(|_| Error::Unavailable)?;
    Ok(result)
}
