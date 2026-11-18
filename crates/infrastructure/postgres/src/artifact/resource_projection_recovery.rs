//! Upgrade missing Resource/Artifact projection facts from pre-atomic commits.
//! Only completed invocations retained by the effective branch projection may
//! contribute, and only their exact immutable versions. Never read global heads.
use super::*;
use musteragent_artifact::{ArtifactEvent, ArtifactLifecycle};
use musteragent_projection::{
    ConversationProjectionSnapshot, ProjectedArtifact, ProjectedToolStatus,
};

impl PostgresArtifactStore {
    pub async fn recover_resource_file_projection(
        pool: &sqlx::PgPool,
        snapshot: &mut ConversationProjectionSnapshot,
    ) -> Result<(), PostgresArtifactError> {
        let mut candidates = snapshot
            .tool_timeline
            .iter()
            .enumerate()
            .filter(|(_, tool)| {
                tool.status == ProjectedToolStatus::Completed
                    && tool.output_artifact_ids.is_empty()
                    && matches!(
                        tool.tool_ref.as_str(),
                        "resource.change-set.commit@1"
                            | "resource.change-set.commit@2"
                            | "resource.change-set.commit@3"
                            | "resource.file.edit.finalize@1"
                            | "resource.file.edit.finalize@2"
                    )
            })
            .map(|(index, tool)| (tool.requested_conversation_seq, index))
            .collect::<Vec<_>>();
        candidates.sort();
        if candidates.is_empty() {
            return Ok(());
        }
        let mut connection = pool.acquire().await?;
        for (_, index) in candidates {
            let tool = &snapshot.tool_timeline[index];
            let invocation = tool.invocation_id;
            let run = tool.run_id;
            let Some(finished) = tool.finished_at else {
                continue;
            };
            let activity = snapshot
                .runs
                .iter()
                .find(|item| item.run_id == run)
                .map(|item| item.activity_id)
                .ok_or(PostgresArtifactError::Corrupt)?;
            let events =
                super::invocation_events(&mut connection, snapshot.tenant_id, invocation).await?;
            let mut created = std::collections::BTreeMap::new();
            for event in events {
                match event {
                    ArtifactEvent::Created {
                        artifact_id,
                        kind,
                        title,
                        ..
                    } => {
                        created.insert(artifact_id, (kind, title.as_str().to_owned()));
                    }
                    ArtifactEvent::VersionCommitted {
                        artifact_id,
                        version,
                        content_ref,
                        dependency_refs,
                        ..
                    } => {
                        if let Some(artifact) = snapshot
                            .artifacts
                            .iter_mut()
                            .find(|a| a.artifact_id == artifact_id)
                        {
                            if version > artifact.current_version {
                                artifact.lifecycle = ArtifactLifecycle::Published;
                                artifact.current_version = version;
                                artifact.current_content_ref = content_ref;
                                artifact.current_dependency_refs = dependency_refs;
                                artifact.updated_by_run_id = Some(run);
                                artifact.presentation_run_id = Some(run);
                                artifact.updated_at = finished;
                            }
                        } else if let Some((kind, title)) = created.remove(&artifact_id) {
                            snapshot.artifacts.push(ProjectedArtifact {
                                artifact_id,
                                activity_id: Some(activity),
                                created_by_run_id: Some(run),
                                updated_by_run_id: Some(run),
                                presentation_run_id: Some(run),
                                kind,
                                lifecycle: ArtifactLifecycle::Published,
                                current_version: version,
                                title,
                                current_content_ref: content_ref,
                                current_dependency_refs: dependency_refs,
                                updated_at: finished,
                            });
                        }
                        snapshot.tool_timeline[index]
                            .output_artifact_ids
                            .push(artifact_id);
                    }
                    _ => return Err(PostgresArtifactError::Corrupt),
                }
            }
            snapshot.tool_timeline[index].output_artifact_ids.sort();
            snapshot.tool_timeline[index].output_artifact_ids.dedup();
        }
        snapshot
            .artifacts
            .sort_by_key(|item| (item.updated_at, item.artifact_id));
        snapshot.content_manifest = musteragent_projection::canonical_content_manifest(snapshot)
            .map_err(|_| PostgresArtifactError::Corrupt)?;
        Ok(())
    }
}
