//! Explicit original Native command custody. This is provenance, never a Run,
//! Tool journal, permission cache or a second task registry.
use super::*;
use musteragent_capability::{CapabilityEntitlement, CapabilityLocation};
use musteragent_content_ref::Sha256;
use musteragent_execution_grant::ExecutionGrant;
use musteragent_persistence_codec::{execution_grant_decode, wire};
use prost::Message;

pub(super) struct NativeSource {
    pub source: WorkItemStartSource,
    pub preparation: DialogueExecutionPreparation,
    pub claims: ExecutionGrant,
}

pub(super) async fn load(
    tx: &mut PgConnection,
    actor: Actor,
    invocation: InvocationId,
) -> Result<Option<NativeSource>, DialogueStoreError> {
    let row = sqlx::query("SELECT n.snapshot_id,n.source_digest,n.snapshot_hash,n.grant_proof_hash,
        c.tenant_id,c.owner_id,c.agent_id,c.dialogue_id,c.intent_id,c.run_id,c.tool_key,
        d.source_envelope,d.source_sha256,d.device_id,d.installation_id,
        v.signed_grant_proof FROM collaboration.work_item_native_sources n
        JOIN resource.device_commands c ON c.invocation_id=n.invocation_id
        JOIN agent.device_admissions d ON d.intent_id=c.intent_id AND d.run_id=c.run_id
          AND d.tenant_id=c.tenant_id AND d.owner_id=c.owner_id
        JOIN agent.device_grant_heads h ON h.intent_id=d.intent_id
        JOIN agent.device_grants_for_receipts v ON v.intent_id=h.intent_id AND v.revision=h.admitted_revision
        WHERE n.invocation_id=$1 AND c.tenant_id=$2 AND c.owner_id=$3 AND c.admission_kind='intent'
          AND d.source_digest=n.source_digest AND d.admitted_snapshot_hash=n.snapshot_hash
          AND v.signed_grant_hash=n.grant_proof_hash AND d.admitted_at_ms IS NOT NULL")
        .bind(invocation.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    let bytes: Vec<u8> = row.try_get("source_envelope").map_err(db)?;
    if bytes.len() > 4 * 1024 * 1024
        || Sha256::digest(&bytes).into_bytes().as_slice()
            != row
                .try_get::<Vec<u8>, _>("source_sha256")
                .map_err(db)?
                .as_slice()
    {
        return Err(DialogueStoreError::Corrupt);
    }
    let (device, preparation) = musteragent_persistence_codec::device_source_codec::decode(&bytes)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    preparation.validate()?;
    let binding = preparation.local_execution_binding(device)?;
    let ids = musteragent_runtime_admission::AdmissionIdentities::derive(
        preparation.identities.command_id,
    )
    .ok_or(DialogueStoreError::Corrupt)?;
    if preparation.actor() != actor
        || device.into_uuid() != row.try_get::<uuid::Uuid, _>("device_id").map_err(db)?
        || binding.installation_id().into_uuid()
            != row
                .try_get::<uuid::Uuid, _>("installation_id")
                .map_err(db)?
        || binding.digest().into_bytes().as_slice()
            != row
                .try_get::<Vec<u8>, _>("source_digest")
                .map_err(db)?
                .as_slice()
        || preparation.scope.agent_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("agent_id").map_err(db)?
        || preparation.scope.dialogue_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("dialogue_id").map_err(db)?
        || preparation.intent.intent_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("intent_id").map_err(db)?
        || preparation.identities.run_id()?.into_uuid()
            != row.try_get::<uuid::Uuid, _>("run_id").map_err(db)?
        || ids.runtime_snapshot_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("snapshot_id").map_err(db)?
    {
        return Err(DialogueStoreError::Corrupt);
    }
    let proof: Vec<u8> = row.try_get("signed_grant_proof").map_err(db)?;
    if proof.len() > 4 * 1024 * 1024
        || Sha256::digest(&proof).into_bytes().as_slice()
            != row
                .try_get::<Vec<u8>, _>("grant_proof_hash")
                .map_err(db)?
                .as_slice()
    {
        return Err(DialogueStoreError::Corrupt);
    }
    let signed = execution_grant_decode::signed_execution_grant(Some(
        wire::SignedExecutionGrant::decode(proof.as_slice())
            .map_err(|_| DialogueStoreError::Corrupt)?,
    ))
    .map_err(|_| DialogueStoreError::Corrupt)?;
    validate_device_grant(&preparation, device, &signed.claims)?;
    let scope = OwnedAgentWorkItemScope {
        owner_id: actor.principal_id,
        space_id: preparation.agent.space_id,
        agent_id: preparation.scope.agent_id,
    };
    let source = WorkItemStartSource {
        actor,
        scope,
        source_dialogue_id: preparation.scope.dialogue_id,
        source_intent_id: preparation.intent.intent_id,
        source_run_id: preparation.identities.run_id()?,
        source_invocation_id: invocation,
        source_snapshot_id: ids.runtime_snapshot_id,
    };
    Ok(Some(NativeSource {
        source,
        preparation,
        claims: signed.claims,
    }))
}

pub(super) async fn require_current(
    tx: &mut PgConnection,
    original: &NativeSource,
) -> Result<(), DialogueStoreError> {
    let p = &original.preparation;
    crate::agent_resource::device_custody::current_agent(
        tx,
        p.actor(),
        p.scope.agent_id,
        p.scope.dialogue_id,
        p.configuration.permission_revision.get(),
        p.configuration.permission_mode,
    )
    .await?;
    let facts = crate::agent_dialogue::capability::verified_root_id(
        tx,
        p.actor(),
        p.scope.agent_id,
        p.intent.intent_id,
    )
    .await?;
    if facts.current_intent != p.intent {
        return Err(DialogueStoreError::Unavailable);
    }
    Ok(())
}

/// Reverse only the deployment-neutral provider's admitted placement, and only
/// for a shared contract in this exact frozen publication's Cloud catalog.
/// No current release, unknown key or Device-only tool can enter the ceiling.
pub(super) fn cloud_ceiling(
    claims: &ExecutionGrant,
) -> Result<Vec<CapabilityEntitlement>, DialogueStoreError> {
    project_cloud_ceiling(
        claims
            .product_release
            .as_ref()
            .ok_or(DialogueStoreError::Unavailable)?,
        &claims.capability_entitlements,
    )
}
fn project_cloud_ceiling(
    reference: &ProductReleaseRef,
    capabilities: &[CapabilityEntitlement],
) -> Result<Vec<CapabilityEntitlement>, DialogueStoreError> {
    use musteragent_tool::{BuiltinToolCatalog, CapabilityExecutionTarget, ProductExecutionTarget};
    if reference.product != ProductId::KERNEL {
        return Err(DialogueStoreError::Unavailable);
    }
    let definitions =
        BuiltinToolCatalog::retained_product_definitions(reference, ProductExecutionTarget::Cloud)
            .map_err(|_| DialogueStoreError::Unavailable)?;
    let rust: CapabilityProviderId = musteragent_tool::BUILTIN_RUST_PROVIDER_ID
        .parse()
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let backend: CapabilityProviderId = musteragent_tool::BUILTIN_BACKEND_PROVIDER_ID
        .parse()
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let mut values = Vec::new();
    for accepted in capabilities {
        let Some(definition) = definitions
            .iter()
            .find(|d| d.capability_key.as_str() == accepted.key.as_str())
        else {
            continue;
        };
        if accepted.effect != definition.effect {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut value = accepted.clone();
        match (&definition.target, accepted.provider_id, accepted.location) {
            (CapabilityExecutionTarget::RustRuntime, p, CapabilityLocation::Device)
                if p == rust =>
            {
                value.location = CapabilityLocation::Cloud
            }
            (CapabilityExecutionTarget::BackendNamed(_), p, CapabilityLocation::Backend)
                if p == backend => {}
            _ => return Err(DialogueStoreError::Corrupt),
        }
        values.push(value);
    }
    values.sort_by(|a, b| a.key.as_str().cmp(b.key.as_str()));
    if values.is_empty() {
        return Err(DialogueStoreError::Unavailable);
    }
    Ok(values)
}

#[cfg(test)]
#[path = "native/source_tests.rs"]
mod tests;
