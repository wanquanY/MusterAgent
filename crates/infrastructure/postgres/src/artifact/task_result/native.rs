//! Native export authority is the signed submit command, admitted Device
//! custody and the original successful local Tool/Ledger record.
use super::*;
use capsule::{Capsule, Change};
use musteragent_runtime_session::{
    DeviceArtifactExport, DeviceArtifactExportRole, DeviceResourceCommand,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Witness {
    pub transport: Uuid,
    pub commitment: [u8; 32],
    pub manifest: [u8; 32],
    pub device: Uuid,
    pub installation: Uuid,
    pub source_digest: [u8; 32],
    pub snapshot: [u8; 32],
    pub grant: [u8; 32],
    pub source_artifact: Uuid,
    pub source_version: Uuid,
    pub local_number: u64,
    pub creation_run: Uuid,
    pub root_revision: Option<u64>,
    pub call: String,
    pub producer_proof: [u8; 32],
    pub kind: String,
}
async fn material(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    command: &DeviceResourceCommand,
    export: &DeviceArtifactExport,
) -> Result<Capsule, Error> {
    let binding = command.admission();
    let run = command.run_context();
    let manifest = command
        .result_artifact_manifest()
        .ok_or(Error::Unavailable)?;
    let task = binding.work_item.ok_or(Error::Unavailable)?;
    if s.producer_home != WorkItemResultHome::Device
        || binding.intent_id != s.source_intent_id
        || task.work_item_id != s.work_item_id
        || task.goal_revision != s.goal_revision
        || task.control_revision != s.control_revision
        || task.assignment_epoch != s.assignment_epoch
        || run.run_id != s.producing_run_id
        || run.runtime_snapshot_id != s.producing_snapshot_id
        || run.tenant_id != s.tenant_id
        || manifest.scope.tenant_id != s.tenant_id
        || manifest.scope.agent_id != s.producing_agent_id
        || manifest.scope.owner_id != s.owner_id
        || manifest.scope.conversation_id != run.conversation_id
        || export.source_conversation_id != run.conversation_id
        || export.version.committed_by.participant_id() != Some(run.executor_participant_id)
        || export.role != DeviceArtifactExportRole::Delivery
        || manifest
            .entries
            .iter()
            .filter(|item| *item == export)
            .count()
            != 1
        || command.invocation().tool_key.as_str() != "tasks.submit_result@1"
    {
        return Err(Error::Unavailable);
    }
    let admitted:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_device_admitted_custody d JOIN collaboration.work_item_execution_reservations x USING(run_id)
        JOIN collaboration.work_items t ON t.work_item_id=x.work_item_id AND t.erasure_job_id IS NULL JOIN collaboration.work_item_revisions current ON current.work_item_id=t.work_item_id AND current.state_revision=t.state_revision
        WHERE d.run_id=$1 AND d.work_item_id=$2 AND d.runtime_snapshot_id=$3 AND d.source_digest=$4 AND d.admitted_snapshot_hash=$5 AND d.signed_grant_hash=$6
         AND x.result_protocol='verified_result_v1' AND x.owner_id=$7 AND x.tenant_id=$8 AND x.executing_agent_id=$9 AND x.requesting_agent_id=$10 AND x.executing_dialogue_id=$11
         AND (x.goal_revision,x.control_revision,x.assignment_epoch)=(current.goal_revision,current.control_revision,current.assignment_epoch))")
        .bind(run.run_id.into_uuid()).bind(task.work_item_id.into_uuid()).bind(run.runtime_snapshot_id.into_uuid()).bind(binding.source_digest.into_bytes().to_vec())
        .bind(binding.snapshot_hash.into_bytes().to_vec()).bind(binding.grant_proof_hash.into_bytes().to_vec()).bind(s.owner_id.into_uuid()).bind(s.tenant_id.into_uuid())
        .bind(s.producing_agent_id.into_uuid()).bind(s.requesting_agent_id.into_uuid()).bind(manifest.scope.dialogue_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    if !admitted {
        return Err(Error::Unavailable);
    }
    let artifact = super::super::device_export_identity::canonical_artifact(binding, export)
        .map_err(|_| Error::Unavailable)?;
    let version = Uuid::new_v5(
        &artifact.into_uuid(),
        export.version.artifact_version_id.into_uuid().as_bytes(),
    );
    let fingerprint = super::super::device_export_identity::version_fingerprint(export)
        .map_err(|_| Error::Unavailable)?;
    let preceding=sqlx::query("SELECT a.current_version::text AS head, v.artifact_version_id,c.content_id,c.content_hash,c.media_type,c.byte_length::text AS len,
        greatest((SELECT max(ns.source_version) FROM artifact.work_item_result_native_origins ns JOIN artifact.work_item_result_asset_returns p ON p.version_id=ns.version_id
           WHERE ns.device_id=$2 AND ns.installation_id=$3 AND ns.source_artifact_id=$4 AND ns.source_participant_id=$5 AND p.artifact_id=a.artifact_id),
          (SELECT max(ns.source_version) FROM artifact.device_versions ns WHERE ns.tenant_id=a.tenant_id AND ns.device_id=$2 AND ns.installation_id=$3 AND ns.source_artifact_id=$4 AND ns.artifact_id=a.artifact_id))::text AS source_head
        FROM artifact.artifacts a JOIN artifact.artifact_versions v ON v.artifact_id=a.artifact_id AND v.version=a.current_version JOIN artifact.content_refs c ON c.content_id=v.content_id AND c.tenant_id=a.tenant_id AND c.state=2 AND c.content_hash=v.content_hash
        WHERE a.artifact_id=$1 AND a.tenant_id=$6 AND a.conversation_id=$7 AND a.kind=$8 AND a.lifecycle IN(1,2) AND v.work_item_erasure_job_id IS NULL")
        .bind(artifact.into_uuid()).bind(binding.device_id.into_uuid()).bind(binding.installation_id.into_uuid()).bind(export.artifact_id.into_uuid()).bind(run.executor_participant_id.into_uuid())
        .bind(s.tenant_id.into_uuid()).bind(run.conversation_id.into_uuid()).bind(super::super::kind_code(export.kind)).fetch_optional(&mut *tx).await.map_err(db)?;
    let versions = preceding
        .as_ref()
        .map(|p| {
            Ok::<_, Error>((
                p.try_get::<String, _>("head")
                    .map_err(db)?
                    .parse()
                    .map_err(|_| Error::Corrupt)?,
                p.try_get::<Option<String>, _>("source_head")
                    .map_err(db)?
                    .ok_or(Error::Unavailable)?
                    .parse()
                    .map_err(|_| Error::Corrupt)?,
            ))
        })
        .transpose()?;
    let (_, next) =
        super::super::device_export_identity::next_versions(versions, export.version.version.get())
            .map_err(|_| Error::RevisionConflict)?;
    let change = if let Some(p) = preceding {
        let root = ContentRef {
            content_id: id(p.try_get("content_id").map_err(db)?)?,
            media_type: musteragent_content_ref::MediaType::new(
                p.try_get::<String, _>("media_type").map_err(db)?,
            )
            .map_err(|_| Error::Corrupt)?,
            byte_length: p
                .try_get::<String, _>("len")
                .map_err(db)?
                .parse()
                .map_err(|_| Error::Corrupt)?,
            sha256: Sha256::from_bytes(p.try_get::<Vec<u8>, _>("content_hash").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
        };
        Change::Revise {
            expected: next.get() - 1,
            expected_id: p.try_get("artifact_version_id").map_err(db)?,
            expected_root: content_bytes(&root),
            title: None,
        }
    } else {
        Change::Create {
            artifact_kind: super::super::kind_code(export.kind),
            lifecycle: super::super::lifecycle_code(
                musteragent_artifact::ArtifactLifecycle::Published,
            ),
            title: export.title.clone(),
            created_at: export.created_at.unix_millis(),
        }
    };
    let root_revision = if matches!(change, Change::Create { .. })
        && export.kind == musteragent_artifact::ArtifactKind::File
    {
        let revision:Option<i64>=sqlx::query_scalar("SELECT revision FROM resource.scope_heads WHERE tenant_id=$1 AND scope_kind=4 AND scope_id=$2")
            .bind(s.tenant_id.into_uuid()).bind(s.producing_agent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        Some(crate::dialogue_codec::unsigned(revision.unwrap_or(0))?)
    } else {
        None
    };
    let witness = Witness {
        root_revision,
        transport: command.invocation().invocation_id.into_uuid(),
        commitment: command.commitment().into_bytes(),
        manifest: command
            .result_artifact_commitment()
            .ok_or(Error::Unavailable)?
            .into_bytes(),
        device: binding.device_id.into_uuid(),
        installation: binding.installation_id.into_uuid(),
        source_digest: binding.source_digest.into_bytes(),
        snapshot: binding.snapshot_hash.into_bytes(),
        grant: binding.grant_proof_hash.into_bytes(),
        source_artifact: export.artifact_id.into_uuid(),
        source_version: export.version.artifact_version_id.into_uuid(),
        local_number: export.version.version.get(),
        creation_run: export.source_created_by_run_id.into_uuid(),
        call: export.producer.tool_call_id.as_str().into(),
        producer_proof: fingerprint.into_bytes(),
        kind: "local_export".into(),
    };
    Ok(Capsule {
        schema: 1,
        native: Some(witness),
        tenant: s.tenant_id.into_uuid(),
        task: s.work_item_id.into_uuid(),
        producer: s.producing_run_id.into_uuid(),
        snapshot: s.producing_snapshot_id.into_uuid(),
        conversation: run.conversation_id.into_uuid(),
        activity: Some(export.source_activity_id.into_uuid()),
        invocation: export.producer.invocation_id.into_uuid(),
        author: run.executor_participant_id.into_uuid(),
        artifact: artifact.into_uuid(),
        version,
        number: next.get(),
        at: export.version.committed_at.unix_millis(),
        root: content_bytes(&export.version.content_ref),
        dependencies: export
            .version
            .dependency_refs
            .iter()
            .map(content_bytes)
            .collect(),
        change,
        placement: None,
        base_node: None,
    })
}
fn item(cap: &Capsule) -> Result<(ContentRef, WorkItemResultAsset), Error> {
    Ok((
        content_from_bytes(cap.root.clone())?,
        WorkItemResultAsset {
            artifact_id: id(cap.artifact)?,
            artifact_version_id: id(cap.version)?,
            authority_sha256: Sha256::digest(cap.encode()?),
        },
    ))
}
pub(crate) async fn prepare_native_result_asset(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    c: &DeviceResourceCommand,
    export: &DeviceArtifactExport,
) -> Result<(ContentRef, WorkItemResultAsset), Error> {
    item(&material(tx, s, c, export).await?)
}
pub(crate) async fn stage_native_result_asset(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
    c: &DeviceResourceCommand,
    export: &DeviceArtifactExport,
) -> Result<(ContentRef, WorkItemResultAsset), Error> {
    let cap = material(tx, s, c, export).await?;
    let w = cap.native.as_ref().ok_or(Error::Corrupt)?;
    staging::persist(tx, s, &cap).await?;
    persist_witness(tx, &cap, w).await?;
    item(&cap)
}
pub(super) async fn persist_witness(
    tx: &mut PgConnection,
    cap: &Capsule,
    w: &Witness,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO artifact.work_item_result_native_origins(version_id,transport_invocation_id,command_sha256,device_id,installation_id,source_sha256,snapshot_sha256,grant_sha256,source_artifact_id,source_version_id,source_version,source_participant_id,source_call_id,proof_sha256,origin_kind) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::text::numeric,$12,$13,$14,$15)")
        .bind(cap.version).bind(w.transport).bind(w.commitment.to_vec()).bind(w.device).bind(w.installation).bind(w.source_digest.to_vec()).bind(w.snapshot.to_vec()).bind(w.grant.to_vec())
        .bind(w.source_artifact).bind(w.source_version).bind(w.local_number.to_string()).bind(cap.author).bind(&w.call).bind(w.producer_proof.to_vec()).bind(&w.kind).execute(tx).await.map_err(db)?;
    Ok(())
}
