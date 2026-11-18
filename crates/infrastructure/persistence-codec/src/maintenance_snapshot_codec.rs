use musteragent_evolution::DreamWorkload;
use musteragent_identity::{DottedKey, VersionedKey};
use musteragent_runtime_memory::{MaintenanceRuntimeSnapshotContent, MaintenanceSnapshotCodec};
use musteragent_runtime_snapshot::PinnedModelRoute;
use prost::Message;

use crate::snapshot_codec::SnapshotCodecError;
use crate::snapshot_codec::model_route::{decode_pinned_model_route, encode_pinned_model_route};
use crate::wire;
use crate::wire_common::{content_ref, decode_content_ref, decode_timestamp, timestamp};

#[derive(Debug, Copy, Clone, Default)]
pub struct ProtobufMaintenanceSnapshotCodec;

impl MaintenanceSnapshotCodec for ProtobufMaintenanceSnapshotCodec {
    type Error = SnapshotCodecError;

    fn encode_content(
        &self,
        content: &MaintenanceRuntimeSnapshotContent,
    ) -> Result<Vec<u8>, Self::Error> {
        content
            .validate()
            .map_err(|_| SnapshotCodecError::InvalidContract)?;
        Ok(wire::MaintenanceRuntimeSnapshotContent {
            schema_version: content.schema_version.as_str().to_owned(),
            tenant_id: Some(wire::TenantId {
                value: content.tenant_id.to_string(),
            }),
            target_agent_id: Some(wire::AgentId {
                value: content.target_agent_id.to_string(),
            }),
            agent_version_id: Some(wire::AgentVersionId {
                value: content.agent_version_id.to_string(),
            }),
            agent_contract: Some(versioned_ref(&content.agent_contract)),
            policy_version_ref: Some(versioned_ref(&content.policy_version_ref)),
            growth_profile_ref: Some(versioned_ref(&content.growth_profile_ref)),
            growth_profile_hash: Some(wire::Sha256 {
                value: content.growth_profile_hash.into_bytes().to_vec(),
            }),
            evolution_model: Some(encode_pinned_model_route(&PinnedModelRoute {
                route: content.evolution_route.clone(),
                credential_grant_ref: content.credential_grant_ref.clone(),
            })),
            dream_job_id: Some(wire::DreamJobId {
                value: content.dream_job_id.to_string(),
            }),
            invocation_id: Some(wire::InvocationId {
                value: content.invocation_id.to_string(),
            }),
            input_manifest_ref: Some(content_ref(&content.input_manifest_ref)),
            model_input_ref: Some(content_ref(&content.model_input_ref)),
            // Legacy V1 field names remain for recovery compatibility. They
            // mirror route/context capacity and never form a Dream budget.
            max_input_tokens: content.workload.evidence_context_tokens,
            max_output_tokens: content.evolution_route.max_output_tokens.get(),
            max_proposals: u32::from(content.workload.candidate_batch_capacity),
            deadline: Some(timestamp(content.deadline)),
            execution_policy: content.execution_policy.map(|policy| {
                crate::snapshot_codec::execution_policy(
                    crate::snapshot_codec::SnapshotWireSchema::Current,
                    &policy,
                )
            }),
        }
        .encode_to_vec())
    }

    fn decode_content(
        &self,
        bytes: &[u8],
    ) -> Result<MaintenanceRuntimeSnapshotContent, Self::Error> {
        let value = wire::MaintenanceRuntimeSnapshotContent::decode(bytes)
            .map_err(|_| SnapshotCodecError::MalformedProtobuf)?;
        let evolution = decode_pinned_model_route(
            value
                .evolution_model
                .ok_or(SnapshotCodecError::MissingField)?,
        )?;
        let content = MaintenanceRuntimeSnapshotContent {
            schema_version: DottedKey::new(value.schema_version)
                .map_err(|_| SnapshotCodecError::InvalidScalar)?,
            tenant_id: parse_id(value.tenant_id.map(|id| id.value))?,
            target_agent_id: parse_id(value.target_agent_id.map(|id| id.value))?,
            agent_version_id: parse_id(value.agent_version_id.map(|id| id.value))?,
            agent_contract: decode_versioned(value.agent_contract)?,
            policy_version_ref: decode_versioned(value.policy_version_ref)?,
            growth_profile_ref: decode_versioned(value.growth_profile_ref)?,
            growth_profile_hash: crate::wire_common::decode_sha256(value.growth_profile_hash)
                .map_err(|_| SnapshotCodecError::InvalidScalar)?,
            evolution_route: evolution.route,
            credential_grant_ref: evolution.credential_grant_ref,
            dream_job_id: parse_id(value.dream_job_id.map(|id| id.value))?,
            invocation_id: parse_id(value.invocation_id.map(|id| id.value))?,
            input_manifest_ref: decode_content_ref(
                value
                    .input_manifest_ref
                    .ok_or(SnapshotCodecError::MissingField)?,
            )
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
            model_input_ref: decode_content_ref(
                value
                    .model_input_ref
                    .ok_or(SnapshotCodecError::MissingField)?,
            )
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
            workload: DreamWorkload {
                evidence_context_tokens: value.max_input_tokens,
                candidate_batch_capacity: value
                    .max_proposals
                    .try_into()
                    .map_err(|_| SnapshotCodecError::InvalidScalar)?,
            },
            execution_policy: value
                .execution_policy
                .map(crate::snapshot_codec::decode_execution_policy)
                .transpose()?,
            deadline: decode_timestamp(value.deadline)
                .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        };
        content
            .validate()
            .map_err(|_| SnapshotCodecError::InvalidContract)?;
        Ok(content)
    }
}

fn versioned_ref(value: &VersionedKey) -> wire::VersionedRef {
    wire::VersionedRef {
        key: value.as_str().to_owned(),
    }
}

fn decode_versioned(value: Option<wire::VersionedRef>) -> Result<VersionedKey, SnapshotCodecError> {
    VersionedKey::new(value.ok_or(SnapshotCodecError::MissingField)?.key)
        .map_err(|_| SnapshotCodecError::InvalidScalar)
}

fn parse_id<T>(value: Option<String>) -> Result<T, SnapshotCodecError>
where
    T: std::str::FromStr,
{
    value
        .ok_or(SnapshotCodecError::MissingField)?
        .parse()
        .map_err(|_| SnapshotCodecError::InvalidScalar)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroU64;

    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{
        AgentId, AgentVersionId, ContentId, DottedKey, DreamJobId, InvocationId, TenantId,
        UtcTimestamp, VersionedKey,
    };
    use musteragent_model::{
        ApiFormat, CredentialGrantRef, ModelRoute, PricingVersionRef, ProviderStatePolicy,
    };
    use musteragent_runtime_memory::{
        DREAM_MODEL_INPUT_MEDIA_TYPE, MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
        MaintenanceRuntimeSnapshot, MaintenanceRuntimeSnapshotContent, MaintenanceSnapshotCodec,
    };

    use super::{DreamWorkload, ProtobufMaintenanceSnapshotCodec};

    #[test]
    fn minimal_maintenance_snapshot_round_trips_without_interactive_authority() {
        let codec = ProtobufMaintenanceSnapshotCodec;
        let content = content();
        let sealed = MaintenanceRuntimeSnapshot::seal(
            content.clone(),
            UtcTimestamp::from_unix_millis(1_000),
            &codec,
        )
        .expect("seal Maintenance Snapshot");
        let restored = MaintenanceRuntimeSnapshot::restore(
            sealed.encoded_content().to_vec(),
            sealed.content_hash(),
            sealed.created_at(),
            &codec,
        )
        .expect("restore Maintenance Snapshot");
        assert_eq!(restored.content(), &content);
        assert!(
            codec
                .decode_content(&sealed.encoded_content()[..sealed.encoded_content().len() - 1])
                .is_err()
        );
    }

    fn content() -> MaintenanceRuntimeSnapshotContent {
        MaintenanceRuntimeSnapshotContent {
            execution_policy: Some(musteragent_runtime_memory::dream_execution_policy_defaults()),
            schema_version: DottedKey::new(MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION)
                .expect("schema"),
            tenant_id: TenantId::generate(),
            target_agent_id: AgentId::generate(),
            agent_version_id: AgentVersionId::generate(),
            agent_contract: VersionedKey::new("musteragent.legacy.runtime-contract@1")
                .expect("contract"),
            policy_version_ref: VersionedKey::new("musteragent.legacy.dream.policy@1")
                .expect("policy"),
            growth_profile_ref: VersionedKey::new("musteragent.legacy.growth-profile-default@1")
                .expect("Growth Profile"),
            growth_profile_hash: Sha256::digest(b"musteragent.legacy.growth-profile-default@1"),
            evolution_route: ModelRoute {
                route_ref: VersionedKey::new("cloud.evolution@1").expect("route"),
                resolved_model_key: "evolution-model".to_owned(),
                model_provider_id: "019fa6de-38fd-7000-8000-000000000711"
                    .parse()
                    .expect("provider"),
                model_entry_id: "019fa6de-38fd-7000-8000-000000000712"
                    .parse()
                    .expect("entry"),
                api_format: ApiFormat::OpenAiResponses,
                adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
                protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
                context_window: NonZeroU64::new(32_768).expect("context"),
                max_output_tokens: NonZeroU64::new(8_192).expect("output"),
                capabilities: BTreeSet::new(),
                parameters: BTreeMap::new(),
                fallback_route_refs: Vec::new(),
                pricing_version_ref: PricingVersionRef::new("catalog:evolution.price@1")
                    .expect("pricing"),
                provider_state_policy: ProviderStatePolicy::Stateless,
            },
            credential_grant_ref: CredentialGrantRef::new("grant_evolution").expect("credential"),
            dream_job_id: DreamJobId::generate(),
            invocation_id: InvocationId::generate(),
            input_manifest_ref: content_ref(
                "application/vnd.musteragent.legacy.dream-input-manifest+json",
                b"manifest",
            ),
            model_input_ref: content_ref(DREAM_MODEL_INPUT_MEDIA_TYPE, b"input"),
            workload: DreamWorkload {
                evidence_context_tokens: 16_384,
                candidate_batch_capacity: 32,
            },
            deadline: UtcTimestamp::from_unix_millis(10_000),
        }
    }

    fn content_ref(media_type: &str, bytes: &[u8]) -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(media_type).expect("media type"),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(bytes),
        }
    }
}
