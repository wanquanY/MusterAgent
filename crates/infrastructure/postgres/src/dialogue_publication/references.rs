use super::{Error, Source};
use crate::dialogue_codec::{db, retain_content_refs};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_execution_context::{AgentArtifactLocator, PublicDeliveryContent};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ContentId, DialogueEntryId, RunId, ToolCallId,
};
use musteragent_runtime_session::DialogueStoreError;
use sqlx::{Row, postgres::PgRow};

pub(super) async fn validate_and_pin(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    run_id: RunId,
    entry_id: DialogueEntryId,
    content: &PublicDeliveryContent,
    publication: &super::PublicationSource,
) -> Result<(), Error> {
    if let PublicDeliveryContent::PartnerCreated(_) = content {
        return Err(DialogueStoreError::Unavailable.into());
    }
    let reference = match publication {
        super::PublicationSource::TaskResultOwner(item)
        | super::PublicationSource::ReturnedFileToolOwner(_, item) => {
            item.reference_for(source, run_id, entry_id, content)?
        }
        super::PublicationSource::FileToolOwner(invocation, authorized) => {
            authorized.reference_for(source, run_id, *invocation, content)?
        }
        _ => resolve_reference(tx, source, run_id, content).await?,
    };
    retain_content_refs(
        tx,
        source.actor,
        source.agent_id,
        vec![("public_entry", entry_id.into_uuid(), &reference)],
    )
    .await?;
    Ok(())
}

pub(super) async fn resolve_reference(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    run_id: RunId,
    content: &PublicDeliveryContent,
) -> Result<ContentRef, Error> {
    let reference = match content {
        PublicDeliveryContent::PartnerCreated(_) => {
            return Err(DialogueStoreError::Unavailable.into());
        }
        PublicDeliveryContent::Message(reference) => reference.clone(),
        PublicDeliveryContent::Interaction(id) => {
            if matches!(source.execution_home, super::PublicationHome::Device(_)) {
                return super::native_interaction::publication::prompt(tx, source, run_id, *id)
                    .await;
            }
            // The public card only points at the original Interaction Owner.
            // Never create options or accept a model-supplied approval state.
            let row = sqlx::query(
                "SELECT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
                 FROM approval.interactions i JOIN artifact.content_refs c
                   ON c.tenant_id=i.tenant_id AND c.content_id=i.prompt_content_id
                 WHERE i.interaction_id=$1 AND i.tenant_id=$2 AND i.conversation_id=$3
                   AND i.run_id=$4 AND i.status=1
                   AND (i.expires_at_ms IS NULL OR i.expires_at_ms > (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint)
                   AND c.state=2 FOR SHARE OF i",
            ).bind(id.into_uuid()).bind(source.actor.tenant_id.into_uuid())
                .bind(source.conversation_id.into_uuid()).bind(run_id.into_uuid())
                .fetch_optional(&mut *tx).await.map_err(db)?
                .ok_or(DialogueStoreError::Unavailable)?;
            content_ref(&row)?
        }
        PublicDeliveryContent::Artifact {
            artifact_id,
            artifact_version_id,
        } => {
            // Persistent ownership permits a later task of this same Agent;
            // the particular delivered version must still come from this Run's
            // successful original Tool receipt. Partner return needs its own grant.
            let row = sqlx::query(
                "SELECT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
                 FROM artifact.artifacts a JOIN artifact.artifact_versions v USING(artifact_id)
                 JOIN artifact.content_refs c ON c.tenant_id=a.tenant_id AND c.content_id=v.content_id
                 WHERE a.artifact_id=$1 AND v.artifact_version_id=$2 AND a.tenant_id=$3
                   AND resource.agent_artifact_owned(a.tenant_id,$6,a.artifact_id) AND a.lifecycle IN (1,2) AND c.state=2
                   AND v.content_hash=c.content_hash
                   AND (a.origin<>1 OR EXISTS (SELECT 1 FROM tool.tool_invocations tool
                     WHERE tool.invocation_id=v.source_invocation_id AND tool.tenant_id=a.tenant_id
                       AND tool.conversation_id=$4 AND tool.run_id=$5 AND tool.status=4)
                    OR EXISTS(SELECT 1 FROM artifact.device_versions proof
                      WHERE proof.artifact_version_id=v.artifact_version_id AND proof.artifact_id=a.artifact_id
                       AND proof.tenant_id=a.tenant_id AND proof.agent_id=$6 AND proof.owner_id=$7
                       AND proof.conversation_id=$4 AND proof.run_id=$5
                       AND v.source_invocation_id IS NULL AND v.committed_by=proof.source_participant_id
                       AND artifact.device_version_owned(proof.tenant_id,proof.owner_id,proof.agent_id,a.artifact_id,v.artifact_version_id))
                    OR EXISTS(SELECT 1 FROM artifact.device_resource_versions proof
                     JOIN resource.device_commands command USING(invocation_id)
                     JOIN agent.device_resource_publications published ON published.producer_invocation_id=command.invocation_id AND published.artifact_version_id=proof.artifact_version_id AND published.role='delivery'
                     JOIN agent.dialogue_tool_publication_groups g ON g.invocation_id=published.publication_invocation_id AND g.source_run_id=command.run_id AND g.source_intent_id=command.intent_id AND g.device_resource_commitment IS NOT NULL
                     WHERE proof.artifact_version_id=v.artifact_version_id AND proof.artifact_id=a.artifact_id
                      AND command.tenant_id=a.tenant_id AND command.owner_id=$7 AND command.agent_id=$6
                      AND command.conversation_id=$4 AND command.run_id=$5 AND v.source_invocation_id IS NULL
                      AND v.committed_by=proof.source_participant_id)) FOR SHARE OF a,v",
            ).bind(artifact_id.into_uuid()).bind(artifact_version_id.into_uuid())
                .bind(source.actor.tenant_id.into_uuid()).bind(source.conversation_id.into_uuid()).bind(run_id.into_uuid()).bind(source.agent_id.into_uuid()).bind(source.actor.principal_id.into_uuid())
                .fetch_optional(&mut *tx).await.map_err(db)?
                .ok_or(DialogueStoreError::Unavailable)?;
            content_ref(&row)?
        }
    };
    Ok(reference)
}

fn content_ref(row: &PgRow) -> Result<ContentRef, Error> {
    Ok(ContentRef {
        content_id: ContentId::from_uuid(row.try_get("content_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        media_type: MediaType::new(row.try_get::<String, _>("media_type").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        byte_length: row
            .try_get::<String, _>("byte_length")
            .map_err(db)?
            .parse()
            .map_err(|_| DialogueStoreError::Corrupt)?,
        sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
    })
}

/// File, Note and media all share the original Artifact version owner. A read
/// result, another Run's call, an unsuccessful call, or an ambiguous multi-
/// version mutation cannot stand in for the specific successful write receipt.
pub(super) async fn resolve_tool_artifact(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    run_id: RunId,
    artifact: AgentArtifactLocator,
    tool_call_id: &ToolCallId,
) -> Result<PublicDeliveryContent, Error> {
    let (id, file_node) = match artifact {
        AgentArtifactLocator::Artifact(id) => (id.into_uuid(), false),
        AgentArtifactLocator::FileNode(id) => (id.into_uuid(), true),
    };
    let versions: Vec<(uuid::Uuid, uuid::Uuid)> = sqlx::query_as(
        "SELECT artifact.artifact_id,version.artifact_version_id FROM tool.tool_invocations invocation
         JOIN tool.tool_results receipt ON receipt.invocation_id=invocation.invocation_id
           AND receipt.result_content_id=invocation.result_content_id
         JOIN artifact.content_refs result ON result.content_id=receipt.result_content_id
           AND result.tenant_id=invocation.tenant_id AND result.content_hash=receipt.result_hash AND result.state=2
         JOIN artifact.artifact_versions version ON version.source_invocation_id=invocation.invocation_id
         JOIN artifact.artifacts artifact ON artifact.artifact_id=version.artifact_id
           AND artifact.tenant_id=invocation.tenant_id
           AND resource.agent_artifact_owned(artifact.tenant_id,$7,artifact.artifact_id)
         WHERE invocation.tenant_id=$1 AND invocation.conversation_id=$2 AND invocation.run_id=$3
           AND invocation.tool_call_id=$4 AND invocation.status=4
           AND artifact.lifecycle IN (1,2)
           AND ((NOT $6 AND artifact.artifact_id=$5) OR ($6 AND EXISTS (
             SELECT 1 FROM resource.nodes node WHERE node.node_id=$5
               AND node.file_artifact_id=artifact.artifact_id AND node.tenant_id=artifact.tenant_id
               AND node.scope_kind=4 AND node.scope_id=$7
               AND node.kind=2 AND node.lifecycle=1)))
         LIMIT 2 FOR SHARE OF invocation,artifact,version"
    ).bind(source.actor.tenant_id.into_uuid()).bind(source.conversation_id.into_uuid())
        .bind(run_id.into_uuid()).bind(tool_call_id.as_str()).bind(id).bind(file_node).bind(source.agent_id.into_uuid())
        .fetch_all(&mut *tx).await.map_err(db)?;
    if versions.len() != 1 {
        return Err(DialogueStoreError::Unavailable.into());
    }
    Ok(PublicDeliveryContent::Artifact {
        artifact_id: ArtifactId::from_uuid(versions[0].0)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        artifact_version_id: ArtifactVersionId::from_uuid(versions[0].1)
            .map_err(|_| DialogueStoreError::Corrupt)?,
    })
}
