//! Exact canonical-file delivery extends the ORIGINAL Publication owner. It
//! does not assert a producer receipt, change a file, or grant arbitrary content.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_runtime_file_query::{
    FileQueryTarget, FileReference, FileSource, resource_file_revision,
};

/// Constructed only by the locked resolver below. Not serializable or supplied
/// by Native/model arguments. The original public entry retains this exact
/// ArtifactVersion/content in the same transaction; no parallel receipt table.
#[derive(Clone)]
pub(super) struct AuthorizedFileDelivery {
    actor: Actor,
    agent_id: AgentId,
    dialogue_id: AgentDialogueId,
    run_id: RunId,
    invocation_id: InvocationId,
    content: PublicDeliveryContent,
    reference: ContentRef,
}
impl AuthorizedFileDelivery {
    pub(super) fn content(&self) -> PublicDeliveryContent {
        self.content.clone()
    }
    pub(super) fn reference_for(
        &self,
        source: &Source,
        run: RunId,
        invocation: InvocationId,
        content: &PublicDeliveryContent,
    ) -> Result<ContentRef, Error> {
        if self.actor != source.actor
            || self.agent_id != source.agent_id
            || self.dialogue_id != source.dialogue_id
            || self.run_id != run
            || self.invocation_id != invocation
            || &self.content != content
        {
            return Err(DialogueStoreError::Unavailable.into());
        }
        Ok(self.reference.clone())
    }
}

pub(super) async fn resolve(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    run: RunId,
    invocation: InvocationId,
    file: &FileReference,
) -> Result<AuthorizedFileDelivery, Error> {
    file.validate_target(FileQueryTarget::OwnedAgent(source.agent_id))
        .map_err(|_| DialogueStoreError::Unavailable)?;
    let FileSource::Resource { node_id } = &file.source else {
        return Err(DialogueStoreError::Unavailable.into());
    };
    let node: ResourceNodeId = node_id
        .parse()
        .map_err(|_| DialogueStoreError::Unavailable)?;
    let material = resolve_exact(tx, source.actor, source.agent_id, Some(node), None, true).await?;
    if file.revision != material.revision(source.agent_id) {
        return Err(DialogueStoreError::Unavailable.into());
    }
    let ExactFile {
        artifact_id,
        version,
        reference,
        ..
    } = material;
    Ok(AuthorizedFileDelivery {
        actor: source.actor,
        agent_id: source.agent_id,
        dialogue_id: source.dialogue_id,
        run_id: run,
        invocation_id: invocation,
        content: PublicDeliveryContent::Artifact {
            artifact_id,
            artifact_version_id: version,
        },
        reference,
    })
}

pub(crate) struct ExactFile {
    pub(crate) artifact_id: ArtifactId,
    pub(crate) version: ArtifactVersionId,
    pub(crate) number: u64,
    pub(crate) node: ResourceNodeId,
    pub(crate) reference: ContentRef,
    pub(crate) projected_hash: Sha256,
}
impl ExactFile {
    pub(crate) fn revision(&self, agent: AgentId) -> String {
        resource_file_revision(&agent.to_string(), self.version, self.projected_hash)
    }
}
/// For a verified result the caller separately establishes the original Task
/// scope. This resolver reads only the executing Agent's exact visible file.
pub(crate) async fn task_file(
    tx: &mut sqlx::PgConnection,
    scope: &musteragent_runtime_session::WorkItemResultScope,
    node: Option<ResourceNodeId>,
    version: Option<ArtifactVersionId>,
    lock: bool,
) -> Result<ExactFile, DialogueStoreError> {
    let actor = Actor {
        tenant_id: scope.tenant_id,
        principal_id: scope.owner_id,
        principal_kind: PrincipalKind::User,
    };
    resolve_exact(tx, actor, scope.producing_agent_id, node, version, lock)
        .await
        .map_err(|e| match e {
            Error::Store(s) => s,
            _ => DialogueStoreError::Infrastructure,
        })
}
async fn resolve_exact(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    agent: AgentId,
    node: Option<ResourceNodeId>,
    exact: Option<ArtifactVersionId>,
    lock: bool,
) -> Result<ExactFile, Error> {
    if node.is_some() == exact.is_some() {
        return Err(DialogueStoreError::Corrupt.into());
    }
    // Caller holds original publication authority locks. Resource identity,
    // current head, immutable version, content integrity and erasure state must
    // agree. A stale observed revision fails; it never selects a newer version.
    // file_versions is a LEFT JOIN projection, not a lockable owner. Its output
    // and dependency facts are immutable; lock the actual owner/content rows.
    let mut statement=String::from("SELECT a.artifact_id,v.artifact_version_id,n.node_id,v.version::text AS number,
        c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash,
        projected.content_hash AS projected_hash
        FROM resource.nodes n
        JOIN artifact.artifacts a ON a.tenant_id=n.tenant_id AND a.artifact_id=n.file_artifact_id
        JOIN resource.file_versions f ON f.artifact_id=a.artifact_id AND f.version=a.current_version
        JOIN artifact.artifact_versions v ON v.artifact_id=a.artifact_id AND v.artifact_version_id=f.artifact_version_id
        JOIN artifact.content_refs c ON c.tenant_id=a.tenant_id AND c.content_id=v.content_id
        JOIN artifact.content_refs projected ON projected.tenant_id=a.tenant_id AND projected.content_id=f.content_id
        WHERE ($1::uuid IS NULL OR n.node_id=$1) AND ($4::uuid IS NULL OR v.artifact_version_id=$4) AND n.tenant_id=$2 AND n.scope_kind=4 AND n.scope_id=$3
          AND n.lifecycle=1 AND n.kind=2 AND a.lifecycle IN(1,2)
          AND resource.agent_artifact_owned(a.tenant_id,$3,a.artifact_id)
          AND v.work_item_erasure_job_id IS NULL AND c.state=2 AND projected.state=2
          AND v.content_hash=c.content_hash AND f.content_hash=projected.content_hash
          AND a.kind=6 AND f.content_id=v.content_id AND f.content_hash=v.content_hash
");
    if lock {
        statement.push_str(" FOR SHARE OF n,a,v,c,projected");
    }
    let row = sqlx::query(&statement)
        .bind(node.map(ResourceNodeId::into_uuid))
        .bind(actor.tenant_id.into_uuid())
        .bind(agent.into_uuid())
        .bind(exact.map(ArtifactVersionId::into_uuid))
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(DialogueStoreError::Unavailable)?;
    let artifact_id = ArtifactId::from_uuid(row.try_get("artifact_id").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let version = ArtifactVersionId::from_uuid(row.try_get("artifact_version_id").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let reference = ContentRef {
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
    };
    // A Presentation FileReference identifies the exact exported bytes. Its
    // public ArtifactVersion still retains the original manifest, whose sealed
    // dependency/output rows select those bytes; neither identity is substituted.
    let projected_hash =
        Sha256::from_bytes(row.try_get::<Vec<u8>, _>("projected_hash").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?;
    Ok(ExactFile {
        artifact_id,
        version,
        reference,
        projected_hash,
        number: row
            .try_get::<String, _>("number")
            .map_err(db)?
            .parse()
            .map_err(|_| DialogueStoreError::Corrupt)?,
        node: ResourceNodeId::from_uuid(row.try_get("node_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
    })
}
