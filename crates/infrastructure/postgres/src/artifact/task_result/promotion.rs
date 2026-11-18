use super::*;
use crate::work_item_execution::result_verification::AcceptedWorkItemResult;
use musteragent_runtime_artifact::ArtifactMutation;
use musteragent_runtime_resource::PreparedResourceFilePlacement;

/// Only verified Task promotion constructs this exact file-placement command.
pub(crate) struct AcceptedPlacement {
    placement: PreparedResourceFilePlacement,
    artifact_id: ArtifactId,
}
impl AcceptedPlacement {
    pub(crate) fn material(&self) -> (&PreparedResourceFilePlacement, ArtifactId) {
        (&self.placement, self.artifact_id)
    }
}

pub(crate) struct AcceptedNativePlacement<'a> {
    scope: &'a WorkItemResultScope,
    artifact: ArtifactId,
    title: &'a str,
    at: musteragent_identity::UtcTimestamp,
    revision: u64,
}
impl<'a> AcceptedNativePlacement<'a> {
    pub(crate) fn material(
        &self,
    ) -> (
        Actor,
        musteragent_identity::AgentId,
        ArtifactId,
        &str,
        musteragent_identity::UtcTimestamp,
        u64,
    ) {
        (
            actor(self.scope),
            self.scope.producing_agent_id,
            self.artifact,
            self.title,
            self.at,
            self.revision,
        )
    }
}
async fn placement_revision(
    tx: &mut PgConnection,
    accepted: &AcceptedWorkItemResult<'_>,
    item: &WorkItemResultItem,
    observed: u64,
) -> Result<u64, Error> {
    let earlier:i64=sqlx::query_scalar("SELECT count(*) FROM artifact.work_item_result_asset_returns r
        JOIN artifact.work_item_result_asset_candidates c ON c.version_id=r.version_id AND c.artifact_id=r.artifact_id AND c.candidate_version=1
        JOIN resource.nodes n ON n.node_id=c.file_node_id AND n.file_artifact_id=c.artifact_id AND n.scope_kind=4 AND n.scope_id=r.producer_id AND n.tenant_id=r.tenant_id AND n.lifecycle=1
        WHERE r.result_id=$1 AND r.producer_id=$2 AND r.item_id<>$3")
        .bind(accepted.result_id()).bind(accepted.scope().producing_agent_id.into_uuid()).bind(item.item_id).fetch_one(tx).await.map_err(db)?;
    observed
        .checked_add(crate::dialogue_codec::unsigned(earlier)?)
        .ok_or(Error::Corrupt)
}
pub(crate) async fn promote_result_asset(
    tx: &mut PgConnection,
    accepted: &AcceptedWorkItemResult<'_>,
    item: &WorkItemResultItem,
    entry: DialogueEntryId,
) -> Result<(), Error> {
    let scope = accepted.scope();
    revalidate_result_asset(tx, scope, item).await?;
    let asset = item.asset.as_ref().ok_or(Error::Corrupt)?;
    if !has_candidate(tx, scope, asset.artifact_version_id).await? {
        let version = visible::revalidate(tx, scope, item).await?;
        return record_return(tx, accepted, item, entry, version).await;
    }
    let (cap, _) = load_by_version(tx, scope, asset.artifact_version_id, true).await?;
    let mutation = cap.mutation()?;
    let v = mutation.version();
    record_return(tx, accepted, item, entry, v.version.get()).await?;
    match &mutation {
        ArtifactMutation::Create { artifact, .. } => super::super::insert_artifact(tx, artifact)
            .await
            .map_err(artifact_error)?,
        ArtifactMutation::Update {
            artifact_id,
            expected_version,
            title,
            ..
        } => {
            if let Some(node) = cap.base_node {
                // The accepted return replaces the exact producer-owned node and
                // exact frozen base. The producer's execution Attempt has ended.
                let capsule::Change::Revise {
                    expected_id,
                    expected_root,
                    ..
                } = &cap.change
                else {
                    return Err(Error::Corrupt);
                };
                let base = content_from_bytes(expected_root.clone())?;
                let count=sqlx::query("UPDATE artifact.artifacts a SET current_version=$1::text::numeric,updated_at_ms=$2
                    WHERE a.tenant_id=$3 AND a.artifact_id=$4 AND a.kind=6 AND a.lifecycle IN(1,2) AND a.current_version=$5::text::numeric
                     AND EXISTS(SELECT 1 FROM resource.nodes n WHERE n.tenant_id=a.tenant_id AND n.node_id=$6 AND n.scope_id=$7 AND n.scope_kind=4 AND n.kind=2 AND n.lifecycle=1 AND n.file_artifact_id=a.artifact_id AND n.name_erasure_job_id IS NULL)
                     AND EXISTS(SELECT 1 FROM artifact.artifact_versions base WHERE base.artifact_id=a.artifact_id AND base.version=a.current_version AND base.artifact_version_id=$8 AND base.content_id=$9 AND base.content_hash=$10 AND base.work_item_erasure_job_id IS NULL)")
                    .bind(v.version.get().to_string()).bind(cap.at).bind(scope.tenant_id.into_uuid()).bind(artifact_id.into_uuid())
                    .bind(expected_version.get().to_string()).bind(node).bind(scope.producing_agent_id.into_uuid()).bind(*expected_id).bind(base.content_id.into_uuid())
                    .bind(base.sha256.into_bytes().to_vec()).execute(&mut *tx).await.map_err(db)?.rows_affected();
                if count != 1 {
                    return Err(Error::RevisionConflict);
                }
            } else {
                super::super::update_artifact(
                    tx,
                    scope.tenant_id,
                    id(cap.conversation)?,
                    *artifact_id,
                    *expected_version,
                    v,
                    title.as_deref(),
                )
                .await
                .map_err(artifact_error)?;
            }
        }
    }
    super::super::insert_version(tx, v)
        .await
        .map_err(artifact_error)?;

    if let (Some(revision), ArtifactMutation::Create { artifact, .. }) =
        (cap.native.as_ref().and_then(|w| w.root_revision), &mutation)
    {
        let revision = placement_revision(tx, accepted, item, revision).await?;
        let file = AcceptedNativePlacement {
            scope,
            artifact: artifact.artifact_id,
            title: &artifact.title,
            at: v.committed_at,
            revision,
        };
        crate::resource::dialogue_device::place_verified(tx, &file)
            .await
            .map_err(resource_error)?;
    }

    if let Some(p) = &cap.placement {
        let mut placement = p.material(&cap, scope)?;
        // A manifest may atomically install several previously reserved files in
        // one directory. Only earlier placements from this accepted delivery
        // may advance their common observed directory revision.
        let expected = placement_revision(tx, accepted, item, p.revision).await?;
        placement.expected_revision = Some(musteragent_identity::ResourceRevision::new(expected));
        crate::resource::place_verified_task_file(
            tx,
            &AcceptedPlacement {
                placement,
                artifact_id: v.artifact_id,
            },
        )
        .await
        .map_err(resource_error)?;
    }

    if matches!(cap.change, capsule::Change::Revise { .. }) {
        // Refresh a previously delivered, mutable card if one exists. An
        // exact cross-partner return never follows future producer heads.
        let old:Option<(Uuid,Uuid)>=sqlx::query_as("SELECT e.dialogue_id,e.entry_id FROM agent.agent_dialogue_entries e JOIN agent.agent_dialogues d USING(dialogue_id,tenant_id,owner_id)
            WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.artifact_id=$3 AND e.kind='artifact' AND d.agent_id=$4 ORDER BY e.seq,e.entry_id LIMIT 1")
            .bind(scope.tenant_id.into_uuid()).bind(scope.owner_id.into_uuid()).bind(cap.artifact).bind(scope.producing_agent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        if let Some((dialogue, older)) = old {
            crate::dialogue_publication::artifact::head_changed(
                tx,
                actor(scope),
                v.artifact_id,
                id(dialogue)?,
                id(older)?,
            )
            .await?;
        }
    }
    Ok(())
}
pub(super) async fn record_return(
    tx: &mut PgConnection,
    accepted: &AcceptedWorkItemResult<'_>,
    item: &WorkItemResultItem,
    entry: DialogueEntryId,
    version: u64,
) -> Result<(), Error> {
    let s = accepted.scope();
    let a = item.asset.as_ref().ok_or(Error::Corrupt)?;
    sqlx::query("INSERT INTO artifact.work_item_result_asset_returns(entry_id,result_id,item_id,tenant_id,owner_id,dialogue_id,requester_id,producer_id,task_id,producer_run_id,report_run_id,verify_invocation_id,accepted_state_revision,artifact_id,version_id,version,authority_sha256,publication_invocation_id)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::text::numeric,$17,$18)")
        .bind(entry.into_uuid()).bind(accepted.result_id()).bind(item.item_id).bind(s.tenant_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(s.source_dialogue_id.into_uuid())
        .bind(s.requesting_agent_id.into_uuid()).bind(s.producing_agent_id.into_uuid()).bind(s.work_item_id.into_uuid()).bind(s.producing_run_id.into_uuid())
        .bind(accepted.report_run_id().map(|run|run.into_uuid())).bind(accepted.verification_invocation_id().map(|invocation|invocation.into_uuid())).bind(integer(accepted.accepted_state_revision())?)
        .bind(a.artifact_id.into_uuid()).bind(a.artifact_version_id.into_uuid()).bind(version.to_string()).bind(a.authority_sha256.into_bytes().to_vec()).bind(accepted.publication_invocation_id().map(|id| id.into_uuid())).execute(tx).await.map_err(db)?;
    Ok(())
}
