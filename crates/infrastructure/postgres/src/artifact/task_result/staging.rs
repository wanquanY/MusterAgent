use super::*;
use capsule::{Capsule, Change, Placement};
use musteragent_runtime_artifact::{ArtifactMutation, CommitPreparedArtifactTool};

/// Select the version from the immutable reservation. A stale verified task
/// cannot fall through to the historical public writer.
pub(crate) async fn verified_scope(
    tx: &mut PgConnection,
    run: RunId,
) -> Result<Option<WorkItemResultScope>, Error> {
    scope_for_read(tx, run, true).await
}
pub(super) async fn scope_for_read(
    tx: &mut PgConnection,
    run: RunId,
    lock: bool,
) -> Result<Option<WorkItemResultScope>, Error> {
    let row=sqlx::query("SELECT x.result_protocol,x.tenant_id,x.owner_id,x.work_item_id,x.goal_revision,x.control_revision,x.assignment_epoch,x.requesting_agent_id,x.executing_agent_id,t.source_dialogue_id,t.source_intent_id,t.erasure_job_id,r.runtime_snapshot_id
        FROM collaboration.work_item_execution_reservations x JOIN collaboration.work_items t USING(work_item_id)
        JOIN runtime.runs r ON r.run_id=x.run_id AND r.tenant_id=x.tenant_id AND r.conversation_id=x.conversation_id WHERE x.run_id=$1")
        .bind(run.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    match row.try_get::<&str, _>("result_protocol").map_err(db)? {
        "public_result_v1" => return Ok(None),
        "verified_result_v1" => {}
        _ => return Err(Error::Corrupt),
    };
    if row
        .try_get::<Option<Uuid>, _>("erasure_job_id")
        .map_err(db)?
        .is_some()
    {
        return Err(Error::Unavailable);
    }
    let cut = crate::dialogue_context::run::run_cut(tx, run, lock)
        .await?
        .ok_or(Error::Unavailable)?;
    let scope = WorkItemResultScope {
        tenant_id: id(row.try_get("tenant_id").map_err(db)?)?,
        owner_id: id(row.try_get("owner_id").map_err(db)?)?,
        work_item_id: id(row.try_get("work_item_id").map_err(db)?)?,
        producing_run_id: run,
        producing_snapshot_id: id(row.try_get("runtime_snapshot_id").map_err(db)?)?,
        producing_agent_id: id(row.try_get("executing_agent_id").map_err(db)?)?,
        requesting_agent_id: id(row.try_get("requesting_agent_id").map_err(db)?)?,
        source_dialogue_id: id(row.try_get("source_dialogue_id").map_err(db)?)?,
        source_intent_id: id(row.try_get("source_intent_id").map_err(db)?)?,
        producer_home: WorkItemResultHome::Cloud,
        goal_revision: crate::dialogue_codec::unsigned(row.try_get("goal_revision").map_err(db)?)?,
        control_revision: crate::dialogue_codec::unsigned(
            row.try_get("control_revision").map_err(db)?,
        )?,
        assignment_epoch: crate::dialogue_codec::unsigned(
            row.try_get("assignment_epoch").map_err(db)?,
        )?,
    };
    if cut.owner_id != scope.owner_id || cut.agent_id != scope.producing_agent_id {
        return Err(Error::Unavailable);
    }
    Ok(Some(scope))
}

pub(super) async fn base(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    artifact: Uuid,
    node: Option<Uuid>,
) -> Result<(u64, Uuid, ContentRef), Error> {
    let row=sqlx::query("SELECT v.version::text AS version,v.artifact_version_id,c.content_id,c.media_type,c.byte_length::text AS bytes,c.content_hash
        FROM artifact.artifacts a JOIN artifact.artifact_versions v ON v.artifact_id=a.artifact_id AND v.version=a.current_version
        JOIN artifact.content_refs c ON c.tenant_id=a.tenant_id AND c.content_id=v.content_id AND c.content_hash=v.content_hash
        WHERE a.tenant_id=$1 AND a.artifact_id=$2 AND a.lifecycle IN(1,2) AND v.work_item_erasure_job_id IS NULL AND c.state=2
        AND resource.agent_artifact_owned(a.tenant_id,$3,a.artifact_id)
        AND ($4::uuid IS NULL OR EXISTS(SELECT 1 FROM resource.nodes n WHERE n.tenant_id=a.tenant_id AND n.scope_kind=4 AND n.scope_id=$3
             AND n.node_id=$4 AND n.file_artifact_id=a.artifact_id AND n.kind=2 AND n.lifecycle=1 AND n.name_erasure_job_id IS NULL)) FOR SHARE OF a,v,c")
        .bind(s.tenant_id.into_uuid()).bind(artifact).bind(s.producing_agent_id.into_uuid()).bind(node).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    let root = ContentRef {
        content_id: id(row.try_get("content_id").map_err(db)?)?,
        media_type: musteragent_content_ref::MediaType::new(
            row.try_get::<String, _>("media_type").map_err(db)?,
        )
        .map_err(|_| Error::Corrupt)?,
        byte_length: row
            .try_get::<String, _>("bytes")
            .map_err(db)?
            .parse()
            .map_err(|_| Error::Corrupt)?,
        sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
    };
    Ok((
        row.try_get::<String, _>("version")
            .map_err(db)?
            .parse()
            .map_err(|_| Error::Corrupt)?,
        row.try_get("artifact_version_id").map_err(db)?,
        root,
    ))
}

pub(crate) async fn stage_artifact(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    c: &CommitPreparedArtifactTool,
) -> Result<(), Error> {
    if s.producing_snapshot_id != c.run_context.runtime_snapshot_id
        || s.producing_run_id != c.run_context.run_id
        || s.tenant_id != c.run_context.tenant_id
    {
        return Err(Error::Unavailable);
    }
    let actual = crate::agent_resource::resolve_commit_execution(
        tx,
        actor(s),
        c.run_context.conversation_id,
        c.invocation_id,
    )
    .await
    .map_err(db)?
    .ok_or(Error::Unavailable)?;
    if actual.0 != s.producing_agent_id {
        return Err(Error::Unavailable);
    }
    let change = match &c.mutation {
        ArtifactMutation::Create { artifact, .. } => Change::Create {
            artifact_kind: super::super::kind_code(artifact.kind),
            lifecycle: super::super::lifecycle_code(artifact.lifecycle),
            title: artifact.title.clone(),
            created_at: artifact.created_at.unix_millis(),
        },
        ArtifactMutation::Update {
            artifact_id,
            expected_version,
            title,
            ..
        } => {
            let node = c
                .resource_replacement
                .as_ref()
                .map(|r| r.file.node_id.into_uuid());
            let (expected, expected_id, root) = base(tx, s, artifact_id.into_uuid(), node).await?;
            if expected != expected_version.get()
                || c.resource_replacement.as_ref().is_some_and(|r| {
                    r.file.artifact_version_id.into_uuid() != expected_id
                        || r.file.content_ref != root
                })
            {
                return Err(Error::RevisionConflict);
            }
            if let Some(r) = &c.resource_replacement
                && !crate::resource_authority::lock_current_resource_context(tx, &r.context)
                    .await
                    .map_err(resource_error)?
            {
                return Err(Error::Unavailable);
            }
            if node.is_none() && !sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM artifact.artifacts WHERE artifact_id=$1 AND tenant_id=$2 AND conversation_id=$3)")
                .bind(artifact_id.into_uuid()).bind(s.tenant_id.into_uuid()).bind(c.run_context.conversation_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)? {return Err(Error::Unavailable);}
            Change::Revise {
                expected,
                expected_id,
                expected_root: content_bytes(&root),
                title: title.clone(),
            }
        }
    };
    let placement = if let Some(p) = &c.resource_placement {
        if !crate::resource_authority::lock_current_resource_context(tx, &p.context)
            .await
            .map_err(resource_error)?
        {
            return Err(Error::Unavailable);
        }
        let revision:Option<i64>=sqlx::query_scalar("SELECT revision FROM resource.scope_heads WHERE tenant_id=$1 AND scope_kind=4 AND scope_id=$2 FOR SHARE")
            .bind(s.tenant_id.into_uuid()).bind(s.producing_agent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        Some(Placement::capture(
            p,
            crate::dialogue_codec::unsigned(revision.unwrap_or(0))?,
            s,
        )?)
    } else {
        None
    };
    let cap = Capsule::capture(c, s, change, placement)?;
    persist(tx, s, &cap).await
}

pub(super) async fn persist(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    cap: &Capsule,
) -> Result<(), Error> {
    let canonical = cap.encode()?;
    let hash = Sha256::digest(&canonical);
    let mutation = cap.mutation()?;
    let version = mutation.version();
    let mut refs = vec![("work_item_result_asset", cap.version, &version.content_ref)];
    refs.extend(
        version
            .dependency_refs
            .iter()
            .map(|c| ("work_item_result_asset", cap.version, c)),
    );
    retain_content_refs(tx, actor(s), s.producing_agent_id, refs).await?;
    sqlx::query("INSERT INTO artifact.work_item_result_asset_candidates(version_id,artifact_id,producer_run_id,original_invocation_id,producer_home,tenant_id,task_id,goal_revision,control_revision,assignment_epoch,producer_snapshot_id,root_content_id,file_node_id,candidate_version,capsule,capsule_sha256)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::text::numeric,$15,$16)")
        .bind(cap.version).bind(cap.artifact).bind(cap.producer).bind(cap.invocation).bind(s.producer_home.key()).bind(cap.tenant).bind(cap.task)
        .bind(integer(s.goal_revision)?).bind(integer(s.control_revision)?).bind(integer(s.assignment_epoch)?).bind(cap.snapshot)
        .bind(version.content_ref.content_id.into_uuid()).bind(cap.node()).bind(cap.number.to_string()).bind(canonical).bind(hash.into_bytes().to_vec())
        .execute(&mut *tx).await.map_err(|e|if e.as_database_error().is_some_and(|x|x.is_unique_violation()){Error::IdempotencyConflict}else{db(e)})?;
    Ok(())
}

pub(super) async fn recheck_head(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    cap: &Capsule,
) -> Result<(), Error> {
    match &cap.change {
        Change::Create { .. } => {
            if sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM artifact.artifacts WHERE artifact_id=$1)",
            )
            .bind(cap.artifact)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?
            {
                return Err(Error::RevisionConflict);
            }
        }
        Change::Revise {
            expected,
            expected_id,
            expected_root,
            ..
        } => {
            let current = base(tx, s, cap.artifact, cap.base_node).await?;
            if current.0 != *expected
                || current.1 != *expected_id
                || content_bytes(&current.2) != *expected_root
            {
                return Err(Error::RevisionConflict);
            }
        }
    }
    Ok(())
}

/// A repeated producer receipt must describe the same immutable command even
/// after another edit has advanced the public head.
pub(crate) async fn verify_duplicate(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    c: &CommitPreparedArtifactTool,
) -> Result<(), Error> {
    let (cap, _) =
        super::load_by_version(tx, s, c.mutation.version().artifact_version_id, false).await?;
    if cap.invocation != c.invocation_id.into_uuid()
        || cap.mutation()? != c.mutation
        || cap.base_node
            != c.resource_replacement
                .as_ref()
                .map(|r| r.file.node_id.into_uuid())
    {
        return Err(Error::IdempotencyConflict);
    }
    let placement = match (&c.resource_placement, &cap.placement) {
        (Some(p), Some(recorded)) => Some(Placement::capture(p, recorded.revision, s)?),
        (None, None) => None,
        _ => return Err(Error::IdempotencyConflict),
    };
    let result:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations i JOIN tool.tool_results r USING(invocation_id)
        WHERE i.tenant_id=$1 AND i.run_id=$2 AND i.invocation_id=$3 AND i.status=4 AND i.erasure_job_id IS NULL AND i.result_content_id=$4 AND r.result_content_id=$4 AND r.result_hash=$5)")
        .bind(s.tenant_id.into_uuid()).bind(s.producing_run_id.into_uuid()).bind(c.invocation_id.into_uuid())
        .bind(c.result.result_ref.content_id.into_uuid()).bind(c.result.result_hash.into_bytes().to_vec()).fetch_one(tx).await.map_err(db)?;
    if !result || placement != cap.placement {
        return Err(Error::IdempotencyConflict);
    }
    Ok(())
}
