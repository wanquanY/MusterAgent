//! Canonical reconstruction of discovery exposure state carried between Runs.
//!
//! Persistence adapters own reachability and durable row decoding. This module
//! owns the shared domain validation so Cloud and Device admission cannot drift
//! in how they interpret the same Snapshot and ordered exposure prefix.

use std::collections::BTreeSet;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_discovery::{
    DiscoveryCarryForwardEvidence, ExposureState, PriorToolExposure, ToolExposureItem,
};
use musteragent_identity::{CorrelationId, RunId, TenantId};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{ErrorCode, RuntimeError};
use musteragent_runtime_snapshot::{RuntimeSnapshotContent, authorized_tool_catalog};
use musteragent_tool::ToolKey;
use thiserror::Error;

use crate::PriorRunDiscovery;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RebuiltPriorRunDiscovery {
    pub discovery: Option<PriorRunDiscovery>,
    pub carry_forward_source_run_id: Option<RunId>,
    pub carry_forward_decision_ref: Option<ContentRef>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum PriorDiscoveryMaterialError {
    #[error("immutable application catalogue is temporarily unavailable")]
    SourceUnavailable,
    #[error("prior Runtime Snapshot content hash is invalid")]
    InvalidSnapshotContentHash,
    #[error("prior Runtime Snapshot is invalid")]
    InvalidSnapshot,
    #[error("prior discovery catalog binding is invalid")]
    InvalidCatalogBinding,
    #[error("prior discovery exposure projection is invalid")]
    InvalidExposureProjection,
    #[error("prior discovery carry-forward evidence is invalid")]
    InvalidCarryForwardEvidence,
}

/// Rebuilds one complete prior exposure set across legacy Runs. Decisions
/// emitted before v2 were not exhaustive, so their omissions are inherited
/// from older reachable Runs while explicit rejections remain authoritative.
#[derive(Debug, Default)]
pub struct PriorDiscoveryAccumulator {
    source_run_id: Option<RunId>,
    exposures: Vec<PriorToolExposure>,
    seen: BTreeSet<ToolKey>,
    rejected: BTreeSet<ToolKey>,
}

impl PriorDiscoveryAccumulator {
    pub fn apply(
        &mut self,
        rebuilt: RebuiltPriorRunDiscovery,
        evidence: Option<&DiscoveryCarryForwardEvidence>,
    ) -> Result<bool, PriorDiscoveryMaterialError> {
        if evidence.is_some() != rebuilt.carry_forward_decision_ref.is_some()
            || evidence.is_some_and(|value| {
                Some(value.source_run_id) != rebuilt.carry_forward_source_run_id
            })
        {
            return Err(PriorDiscoveryMaterialError::InvalidCarryForwardEvidence);
        }
        let candidate_source_run_id = rebuilt
            .discovery
            .as_ref()
            .map(|discovery| discovery.source_run_id);
        let candidate_tools = rebuilt
            .discovery
            .as_ref()
            .map(|discovery| {
                discovery
                    .tool_exposures
                    .iter()
                    .map(|exposure| exposure.tool_ref.clone())
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        // The newest reachable Snapshot is the consolidation head even when
        // it contains no dynamic exposure. The accumulated Tools may come
        // from older compatibility history, but the next decision must bind
        // to the newest Run that authorized this reconstruction.
        if self.source_run_id.is_none() {
            self.source_run_id = candidate_source_run_id;
        }
        if let Some(discovery) = rebuilt.discovery {
            for exposure in discovery.tool_exposures {
                if !self.rejected.contains(&exposure.tool_ref)
                    && self.seen.insert(exposure.tool_ref.clone())
                {
                    self.exposures.push(exposure);
                }
            }
        }
        let Some(evidence) = evidence else {
            return Ok(false);
        };
        if evidence
            .accepted_tool_refs
            .iter()
            .any(|tool| !candidate_tools.contains(tool))
        {
            return Err(PriorDiscoveryMaterialError::InvalidCarryForwardEvidence);
        }
        // A rejected prior identity may still be exposed independently by the
        // current Product surface (for example after a schema upgrade). Such
        // current authorization wins; only an absent rejected key fences
        // resurrection from older carry history.
        for tool in &evidence.rejected_tool_refs {
            if !self.seen.contains(tool) {
                self.rejected.insert(tool.clone());
            }
        }
        // Even an all-rejected V1 decision is durable negative evidence. Keep
        // its Run as the consolidation head so the next admission emits an
        // exhaustive V2 boundary instead of resurrecting older exposures.
        Ok(evidence.exhaustive)
    }

    #[must_use]
    pub fn finish(self) -> Option<PriorRunDiscovery> {
        self.source_run_id.map(|source_run_id| PriorRunDiscovery {
            source_run_id,
            tool_exposures: self.exposures,
        })
    }
}

/// Reconstructs a previous Run's complete exposed Tool set from canonical
/// admission material plus its reachable, ordered discovery projection.
///
/// Bootstrap Tools are excluded because the next admission resolves them from
/// current policy. Every non-bootstrap Tool remains subject to the existing
/// carry-forward re-authorization performed by `ProductSnapshotBuilder`.
pub fn rebuild_prior_run_discovery(
    decoder: &dyn RuntimeSnapshotDecoder,
    source_run_id: RunId,
    encoded_snapshot: &[u8],
    persisted_snapshot_hash: Sha256,
    projected_exposures: &[ToolExposureItem],
) -> Result<RebuiltPriorRunDiscovery, PriorDiscoveryMaterialError> {
    if Sha256::digest(encoded_snapshot) != persisted_snapshot_hash {
        return Err(PriorDiscoveryMaterialError::InvalidSnapshotContentHash);
    }
    let snapshot = decoder
        .decode_runtime_snapshot(encoded_snapshot)
        .map_err(|_| PriorDiscoveryMaterialError::InvalidSnapshot)?;
    snapshot
        .validate_sealed()
        .map_err(|_| PriorDiscoveryMaterialError::InvalidSnapshot)?;
    let catalog = authorized_tool_catalog(&snapshot.tools, &snapshot.capabilities)
        .map_err(|_| PriorDiscoveryMaterialError::InvalidCatalogBinding)?;
    rebuild_resolved(source_run_id, &snapshot, &catalog, projected_exposures)
}

/// The same original exposure reconstruction, resolving only immutable public
/// metadata for indexed snapshots; schemas are not needed to carry identities.
pub async fn rebuild_prior_run_discovery_with_applications(
    decoder: &dyn RuntimeSnapshotDecoder,
    source_run_id: RunId,
    encoded_snapshot: &[u8],
    persisted_snapshot_hash: Sha256,
    projected_exposures: &[ToolExposureItem],
    source: Option<
        &dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
    >,
) -> Result<RebuiltPriorRunDiscovery, PriorDiscoveryMaterialError> {
    if Sha256::digest(encoded_snapshot) != persisted_snapshot_hash {
        return Err(PriorDiscoveryMaterialError::InvalidSnapshotContentHash);
    }
    let snapshot = decoder
        .decode_runtime_snapshot(encoded_snapshot)
        .map_err(|_| PriorDiscoveryMaterialError::InvalidSnapshot)?;
    snapshot
        .validate_sealed()
        .map_err(|_| PriorDiscoveryMaterialError::InvalidSnapshot)?;
    let catalog = musteragent_runtime_snapshot::application_catalog::resolve_tool_catalog(&snapshot, source).await.map_err(|error| match error {
        musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialError::Unavailable => PriorDiscoveryMaterialError::SourceUnavailable,
        _ => PriorDiscoveryMaterialError::InvalidCatalogBinding,
    })?;
    snapshot
        .validate_resolved_tool_catalog(&catalog.catalog)
        .map_err(|_| PriorDiscoveryMaterialError::InvalidCatalogBinding)?;
    rebuild_resolved(
        source_run_id,
        &snapshot,
        &catalog.catalog,
        projected_exposures,
    )
}

fn rebuild_resolved(
    source_run_id: RunId,
    snapshot: &RuntimeSnapshotContent,
    catalog: &musteragent_discovery::AuthorizedToolCatalog,
    projected_exposures: &[ToolExposureItem],
) -> Result<RebuiltPriorRunDiscovery, PriorDiscoveryMaterialError> {
    let Some(discovery) = snapshot.discovery.as_ref() else {
        return Ok(RebuiltPriorRunDiscovery {
            discovery: None,
            carry_forward_source_run_id: None,
            carry_forward_decision_ref: None,
        });
    };

    if catalog.catalog_hash() != discovery.authorized_tool_catalog_hash {
        return Err(PriorDiscoveryMaterialError::InvalidCatalogBinding);
    }
    let exposure_state = ExposureState::from_projection(
        catalog,
        &discovery.initial_exposed_tool_keys,
        projected_exposures,
    )
    .map_err(|_| PriorDiscoveryMaterialError::InvalidExposureProjection)?;
    let bootstrap_keys = discovery
        .bootstrap_tool_keys
        .iter()
        .collect::<BTreeSet<_>>();
    let tool_exposures = exposure_state
        .items()
        .iter()
        .filter(|item| !bootstrap_keys.contains(&item.tool_ref))
        .map(|item| {
            let descriptor = catalog
                .get_by_key(item.tool_ref.as_str())
                .ok_or(PriorDiscoveryMaterialError::InvalidExposureProjection)?;
            Ok(PriorToolExposure {
                tool_ref: item.tool_ref.clone(),
                schema_hash: item.schema_hash,
                provider_id: descriptor.provider_id,
                location: descriptor.location,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RebuiltPriorRunDiscovery {
        discovery: Some(PriorRunDiscovery {
            source_run_id,
            tool_exposures,
        }),
        carry_forward_source_run_id: discovery.carry_forward_source_run_id,
        carry_forward_decision_ref: discovery.carry_forward_decision_ref.clone(),
    })
}

/// Promotes the exact durable exposure state of a failed source Run into the
/// Retry Snapshot's initial exposure prefix. The source catalog and schema
/// bytes remain immutable; only the fresh signed Grant bindings may already
/// have been replaced by the caller.
pub async fn rebind_retry_discovery(
    mut source: RuntimeSnapshotContent,
    prior: Option<&PriorRunDiscovery>,
    expected_source_run_id: RunId,
    applications: Option<
        &dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
    >,
    tenant_id: TenantId,
    content: &dyn ContentStore,
    correlation_id: CorrelationId,
) -> Result<RuntimeSnapshotContent, RuntimeError> {
    let Some(prior) = prior else {
        return Ok(source);
    };
    if prior.source_run_id != expected_source_run_id {
        return Err(retry_discovery_error(
            correlation_id,
            "Retry discovery material does not belong to the requested source Run",
        ));
    }

    let catalog = musteragent_runtime_snapshot::application_catalog::resolve_tool_catalog(&source, applications).await.map_err(|error| match error {
        musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialError::Unavailable => crate::runtime_error(ErrorCode::DependencyUnavailable, correlation_id, true, "Immutable retry Tool catalogue is temporarily unavailable"),
        _ => retry_discovery_error(correlation_id, "Retry source Tool catalogue is invalid"),
    })?.catalog;
    let discovery = source.discovery.as_mut().ok_or_else(|| {
        retry_discovery_error(
            correlation_id,
            "Retry source Snapshot has no discovery contract",
        )
    })?;
    if catalog.catalog_hash() != discovery.authorized_tool_catalog_hash {
        return Err(retry_discovery_error(
            correlation_id,
            "Retry source Tool catalog hash is invalid",
        ));
    }

    let available_placements = catalog
        .definitions()
        .iter()
        .map(|definition| (definition.provider_id, definition.location))
        .collect::<std::collections::BTreeSet<_>>();
    let available_locations = available_placements
        .iter()
        .map(|(_, location)| *location)
        .collect();
    // These contexts are not inferred from mutable deployment state. Each is
    // copied only from the exact source catalog whose exposure projection was
    // already validated for the same immutable execution scope.
    let available_contexts = catalog
        .definitions()
        .iter()
        .flat_map(|definition| definition.required_contexts.iter().copied())
        .collect();
    let decision = musteragent_discovery::reauthorize_tool_exposures(
        prior.source_run_id,
        &prior.tool_exposures,
        &catalog,
        &musteragent_discovery::ActivationConstraints {
            available_locations,
            available_contexts,
            max_exposed_tools: discovery.max_exposed_tools,
        },
        &available_placements,
        &std::collections::BTreeMap::new(),
    );
    if !decision.rejected_tools.is_empty()
        || decision.accepted_tool_refs.len() != prior.tool_exposures.len()
    {
        return Err(retry_discovery_error(
            correlation_id,
            "Retry source Tool exposure no longer matches its immutable Snapshot",
        ));
    }

    let mut exposed = discovery
        .initial_exposed_tool_keys
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for tool_ref in &decision.accepted_tool_refs {
        if exposed.insert(tool_ref.clone()) {
            discovery.initial_exposed_tool_keys.push(tool_ref.clone());
        }
    }
    if discovery.initial_exposed_tool_keys.len()
        > usize::try_from(discovery.max_exposed_tools.get()).map_err(|_| {
            retry_discovery_error(correlation_id, "Retry Tool exposure limit is invalid")
        })?
        || ExposureState::from_initial(&catalog, &discovery.initial_exposed_tool_keys).is_err()
    {
        return Err(crate::runtime_error(
            ErrorCode::CapabilityUnavailable,
            correlation_id,
            false,
            "Retry source Tool exposure exceeds its frozen discovery policy",
        ));
    }

    let decision_ref = content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/json").map_err(|_| {
                retry_discovery_error(
                    correlation_id,
                    "Retry discovery evidence media type is invalid",
                )
            })?,
            bytes: decision.encode_canonical_json().map_err(|_| {
                retry_discovery_error(correlation_id, "Retry discovery evidence cannot be encoded")
            })?,
        })
        .await
        .map_err(|_| {
            crate::runtime_error(
                ErrorCode::DependencyUnavailable,
                correlation_id,
                true,
                "Retry discovery evidence could not be persisted",
            )
        })?;
    discovery.carry_forward_source_run_id = Some(prior.source_run_id);
    discovery.carry_forward_decision_ref = Some(decision_ref);
    source.validate().map_err(|_| {
        retry_discovery_error(
            correlation_id,
            "Retry Snapshot is invalid after exact discovery reconstruction",
        )
    })?;
    Ok(source)
}

fn retry_discovery_error(correlation_id: CorrelationId, message: &'static str) -> RuntimeError {
    crate::runtime_error(
        ErrorCode::RuntimeSnapshotInvalid,
        correlation_id,
        false,
        message,
    )
}

#[cfg(test)]
mod tests {
    use musteragent_capability::CapabilityLocation;
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_discovery::{DiscoveryCarryForwardEvidence, PriorToolExposure};
    use musteragent_identity::{CapabilityProviderId, ContentId, RunId, VersionedKey};
    use musteragent_tool::ToolKey;

    use super::{PriorDiscoveryAccumulator, RebuiltPriorRunDiscovery};
    use crate::PriorRunDiscovery;

    #[test]
    fn legacy_rejection_prevents_older_exposure_resurrection() {
        let current_run_id = RunId::generate();
        let source_run_id = RunId::generate();
        let accepted = exposure("artifact.note.read@1");
        let rejected = exposure("system.time.current@1");
        let mut accumulator = PriorDiscoveryAccumulator::default();

        let exhaustive = accumulator
            .apply(
                RebuiltPriorRunDiscovery {
                    discovery: Some(PriorRunDiscovery {
                        source_run_id: current_run_id,
                        tool_exposures: vec![accepted.clone()],
                    }),
                    carry_forward_source_run_id: Some(source_run_id),
                    carry_forward_decision_ref: Some(decision_ref()),
                },
                Some(&DiscoveryCarryForwardEvidence {
                    source_run_id,
                    accepted_tool_refs: vec![accepted.tool_ref.clone()],
                    rejected_tool_refs: vec![rejected.tool_ref.clone()],
                    exhaustive: false,
                }),
            )
            .expect("valid legacy decision");
        assert!(!exhaustive);
        accumulator
            .apply(
                RebuiltPriorRunDiscovery {
                    discovery: Some(PriorRunDiscovery {
                        source_run_id,
                        tool_exposures: vec![rejected],
                    }),
                    carry_forward_source_run_id: None,
                    carry_forward_decision_ref: None,
                },
                None,
            )
            .expect("valid older source");

        let rebuilt = accumulator.finish().expect("consolidated discovery");
        assert_eq!(rebuilt.source_run_id, current_run_id);
        assert_eq!(rebuilt.tool_exposures, vec![accepted]);
    }

    #[test]
    fn current_explicit_exposure_supersedes_a_prior_identity_rejection() {
        let current_run_id = RunId::generate();
        let source_run_id = RunId::generate();
        let current = exposure("system.time.current@1");
        let mut accumulator = PriorDiscoveryAccumulator::default();

        accumulator
            .apply(
                RebuiltPriorRunDiscovery {
                    discovery: Some(PriorRunDiscovery {
                        source_run_id: current_run_id,
                        tool_exposures: vec![current.clone()],
                    }),
                    carry_forward_source_run_id: Some(source_run_id),
                    carry_forward_decision_ref: Some(decision_ref()),
                },
                Some(&DiscoveryCarryForwardEvidence {
                    source_run_id,
                    accepted_tool_refs: Vec::new(),
                    rejected_tool_refs: vec![current.tool_ref.clone()],
                    exhaustive: true,
                }),
            )
            .expect("independently authorized current Tool");

        assert_eq!(
            accumulator
                .finish()
                .expect("current discovery")
                .tool_exposures,
            vec![current]
        );
    }

    fn exposure(key: &str) -> PriorToolExposure {
        PriorToolExposure {
            tool_ref: ToolKey::from_versioned_key(VersionedKey::new(key).expect("Tool key")),
            schema_hash: Sha256::digest(key.as_bytes()),
            provider_id: CapabilityProviderId::generate(),
            location: CapabilityLocation::Device,
        }
    }

    fn decision_ref() -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").expect("media type"),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        }
    }
}
