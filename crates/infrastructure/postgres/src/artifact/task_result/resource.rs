//! Resource authoring shares the private Asset authority used by Artifact Tools.
use super::*;
use capsule::{Capsule, Change, Placement};
use musteragent_artifact::ArtifactVersionAuthor;
use musteragent_identity::VersionNumber;
use musteragent_runtime_resource::{
    FileEditError, FinalizeFileEdit, PreparedResourceFilePlacement, ResourceCommandContext,
    ResourceCommandSource,
};

struct ResultFileChange {
    change: Change,
    placement: Option<Placement>,
    base_node: Option<Uuid>,
    activity: Option<Uuid>,
    creation_run: Uuid,
}

pub(crate) async fn operation_scope(
    tx: &mut PgConnection,
    c: &ResourceCommandContext,
) -> Result<Option<WorkItemResultScope>, Error> {
    match c.source {
        ResourceCommandSource::DialogueExecution {
            conversation_id, ..
        } => {
            let run:Uuid=sqlx::query_scalar("SELECT run_id FROM tool.tool_invocations WHERE tenant_id=$1 AND conversation_id=$2 AND invocation_id=$3")
                .bind(c.tenant_id.into_uuid()).bind(conversation_id.into_uuid()).bind(c.invocation_id.ok_or(Error::Unavailable)?.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
            verified_scope(tx, id(run)?).await
        }
        ResourceCommandSource::DeviceExecution {
            work_item: Some(task),
            run_id,
            conversation_id,
            source_digest,
            snapshot_hash,
            grant_proof_hash,
            agent_id,
            intent_id,
            ..
        } => {
            let row=sqlx::query("SELECT x.result_protocol,x.requesting_agent_id,t.source_dialogue_id,d.runtime_snapshot_id,t.erasure_job_id
              FROM collaboration.work_item_execution_reservations x JOIN collaboration.work_items t USING(work_item_id)
              JOIN collaboration.work_item_device_admitted_custody d USING(run_id) WHERE x.run_id=$1 AND x.tenant_id=$2 AND x.owner_id=$3 AND x.executing_agent_id=$4 AND x.work_item_id=$5
              AND x.goal_revision=$6 AND x.control_revision=$7 AND x.assignment_epoch=$8 AND d.conversation_id=$9 AND d.source_digest=$10 AND d.admitted_snapshot_hash=$11 AND d.signed_grant_hash=$12 AND t.source_intent_id=$13")
                .bind(run_id.into_uuid()).bind(c.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid()).bind(agent_id.into_uuid()).bind(task.work_item_id.into_uuid())
                .bind(integer(task.goal_revision)?).bind(integer(task.control_revision)?).bind(integer(task.assignment_epoch)?).bind(conversation_id.into_uuid()).bind(source_digest.into_bytes().to_vec()).bind(snapshot_hash.into_bytes().to_vec()).bind(grant_proof_hash.into_bytes().to_vec()).bind(intent_id.into_uuid())
                .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
            match row.try_get::<&str, _>("result_protocol").map_err(db)? {
                "public_result_v1" => return Ok(None),
                "verified_result_v1" => {}
                _ => return Err(Error::Corrupt),
            };
            if row
                .try_get::<Option<Uuid>, _>("erasure_job_id")
                .map_err(db)?
                .is_some()
                || !crate::resource_authority::lock_current_resource_context(tx, c)
                    .await
                    .map_err(resource_error)?
            {
                return Err(Error::Unavailable);
            }
            Ok(Some(WorkItemResultScope {
                tenant_id: c.tenant_id,
                owner_id: c.actor.principal_id,
                work_item_id: task.work_item_id,
                goal_revision: task.goal_revision,
                control_revision: task.control_revision,
                assignment_epoch: task.assignment_epoch,
                producing_run_id: run_id,
                producing_snapshot_id: id(row.try_get("runtime_snapshot_id").map_err(db)?)?,
                producing_agent_id: agent_id,
                requesting_agent_id: id(row.try_get("requesting_agent_id").map_err(db)?)?,
                source_dialogue_id: id(row.try_get("source_dialogue_id").map_err(db)?)?,
                source_intent_id: intent_id,
                producer_home: WorkItemResultHome::Device,
            }))
        }
        _ => Ok(None),
    }
}
async fn witness(
    tx: &mut PgConnection,
    c: &ResourceCommandContext,
    artifact: Uuid,
    version: Uuid,
    number: u64,
    creation_run: Uuid,
) -> Result<Option<native::Witness>, Error> {
    let ResourceCommandSource::DeviceExecution {
        device_id,
        installation_id,
        source_digest,
        snapshot_hash,
        grant_proof_hash,
        command_commitment,
        ..
    } = c.source
    else {
        return Ok(None);
    };
    let invocation = c.invocation_id.ok_or(Error::Corrupt)?.into_uuid();
    let call:String=sqlx::query_scalar("SELECT tool_call_id FROM resource.device_commands WHERE invocation_id=$1 AND tenant_id=$2 AND owner_id=$3 AND command_commitment=$4 AND admission_kind='work_item'")
        .bind(invocation).bind(c.tenant_id.into_uuid()).bind(c.actor.principal_id.into_uuid()).bind(command_commitment.into_bytes().to_vec()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    Ok(Some(native::Witness {
        root_revision: None,
        transport: invocation,
        commitment: command_commitment.into_bytes(),
        manifest: command_commitment.into_bytes(),
        device: device_id.into_uuid(),
        installation: installation_id.into_uuid(),
        source_digest: source_digest.into_bytes(),
        snapshot: snapshot_hash.into_bytes(),
        grant: grant_proof_hash.into_bytes(),
        source_artifact: artifact,
        source_version: version,
        local_number: number,
        creation_run,
        call,
        producer_proof: command_commitment.into_bytes(),
        kind: "canonical_resource".into(),
    }))
}
async fn stage(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    c: &ResourceCommandContext,
    v: &ArtifactVersion,
    ResultFileChange {
        change,
        placement,
        base_node,
        activity,
        creation_run,
    }: ResultFileChange,
) -> Result<(), Error> {
    let inv = c.invocation_id.ok_or(Error::Corrupt)?;
    let author = v.committed_by.participant_id().ok_or(Error::Unavailable)?;
    if v.source_invocation_id != Some(inv) || s.tenant_id != c.tenant_id || actor(s) != c.actor {
        return Err(Error::Unavailable);
    }
    let witness = witness(
        tx,
        c,
        v.artifact_id.into_uuid(),
        v.artifact_version_id.into_uuid(),
        v.version.get(),
        creation_run,
    )
    .await?;
    let cap = Capsule {
        schema: 1,
        native: witness,
        tenant: s.tenant_id.into_uuid(),
        task: s.work_item_id.into_uuid(),
        producer: s.producing_run_id.into_uuid(),
        snapshot: s.producing_snapshot_id.into_uuid(),
        conversation: c.conversation_id().ok_or(Error::Corrupt)?.into_uuid(),
        activity,
        invocation: inv.into_uuid(),
        author: author.into_uuid(),
        artifact: v.artifact_id.into_uuid(),
        version: v.artifact_version_id.into_uuid(),
        number: v.version.get(),
        at: v.committed_at.unix_millis(),
        root: content_bytes(&v.content_ref),
        dependencies: v.dependency_refs.iter().map(content_bytes).collect(),
        change,
        placement,
        base_node,
    };
    staging::persist(tx, s, &cap).await?;
    if let Some(w) = &cap.native {
        native::persist_witness(tx, &cap, w).await?;
    }
    Ok(())
}
pub(crate) async fn stage_native_created(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    a: &Artifact,
    v: &ArtifactVersion,
    p: &PreparedResourceFilePlacement,
) -> Result<(), Error> {
    if a.tenant_id != s.tenant_id
        || a.created_by_run_id() != Some(s.producing_run_id)
        || a.source.conversation_id() != p.context.conversation_id()
        || a.artifact_id != v.artifact_id
        || v.version.get() != 1
    {
        return Err(Error::Unavailable);
    }
    let rev:Option<i64>=sqlx::query_scalar("SELECT revision FROM resource.scope_heads WHERE tenant_id=$1 AND scope_kind=4 AND scope_id=$2 FOR SHARE").bind(s.tenant_id.into_uuid()).bind(s.producing_agent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let placement = Placement::capture(p, crate::dialogue_codec::unsigned(rev.unwrap_or(0))?, s)?;
    stage(
        tx,
        s,
        &p.context,
        v,
        ResultFileChange {
            change: Change::Create {
                artifact_kind: super::super::kind_code(a.kind),
                lifecycle: super::super::lifecycle_code(a.lifecycle),
                title: a.title.clone(),
                created_at: a.created_at.unix_millis(),
            },
            placement: Some(placement),
            base_node: None,
            activity: Some(a.activity_id().ok_or(Error::Corrupt)?.into_uuid()),
            creation_run: s.producing_run_id.into_uuid(),
        },
    )
    .await
}
pub(crate) async fn stage_finalized_edit(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    c: &FinalizeFileEdit,
    artifact: ArtifactId,
    node: ResourceNodeId,
    number: VersionNumber,
    root: ContentRef,
) -> Result<(), Error> {
    let (expected, expected_id, old) =
        staging::base(tx, s, artifact.into_uuid(), Some(node.into_uuid())).await?;
    if expected.checked_add(1) != Some(number.get()) {
        return Err(Error::RevisionConflict);
    }
    let author = match c.committed_by {
        ArtifactVersionAuthor::Participant(p) => p,
        _ => return Err(Error::Unavailable),
    };
    let v = ArtifactVersion {
        artifact_id: artifact,
        artifact_version_id: c.artifact_version_id,
        version: number,
        content_hash: root.sha256,
        content_ref: root,
        dependency_refs: vec![],
        source_invocation_id: c.context.invocation_id,
        committed_by: ArtifactVersionAuthor::Participant(author),
        committed_at: c.context.now,
    };
    stage(
        tx,
        s,
        &c.context,
        &v,
        ResultFileChange {
            change: Change::Revise {
                expected,
                expected_id,
                expected_root: content_bytes(&old),
                title: None,
            },
            placement: None,
            base_node: Some(node.into_uuid()),
            activity: None,
            creation_run: s.producing_run_id.into_uuid(),
        },
    )
    .await
}
pub(crate) fn edit_error(e: Error) -> FileEditError {
    match e {
        Error::Infrastructure => FileEditError::DependencyUnavailable,
        Error::Corrupt => FileEditError::Corrupt,
        Error::RevisionConflict | Error::IdempotencyConflict => FileEditError::BaseVersionConflict,
        _ => FileEditError::IdentityMismatch,
    }
}
