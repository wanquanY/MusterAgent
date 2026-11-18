use std::collections::BTreeSet;

use musteragent_content_ref::Sha256;
use musteragent_discovery::DiscoveryCarryForwardEvidence;
use musteragent_identity::{ConversationId, CorrelationId, HomeEpoch, LedgerOffset, TenantId};
use musteragent_runtime_content::ContentStore as _;
use musteragent_runtime_contract::{ErrorCode, RuntimeError};
use musteragent_runtime_session::{
    PriorDiscoveryAccumulator, PriorRunDiscovery, rebuild_prior_run_discovery_with_applications,
};
use sqlx::Row as _;

use super::PostgresRuntimeApplicationStore;
use super::context_reachability::{self, ReachableContextPoint};
use super::support::{id, parse_u64};
use crate::runtime_application_error::{corrupt, database_dependency, dependency};

impl PostgresRuntimeApplicationStore {
    pub(super) async fn load_prior_discovery(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        home_epoch: HomeEpoch,
        reachable_at: ReachableContextPoint,
        correlation_id: CorrelationId,
    ) -> Result<Option<PriorRunDiscovery>, RuntimeError> {
        self.load_discovery_from(
            tenant_id,
            conversation_id,
            home_epoch,
            reachable_at,
            None,
            correlation_id,
        )
        .await
    }

    pub(super) async fn load_run_discovery(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        home_epoch: HomeEpoch,
        reachable_at: ReachableContextPoint,
        source_run_id: musteragent_identity::RunId,
        correlation_id: CorrelationId,
    ) -> Result<Option<PriorRunDiscovery>, RuntimeError> {
        self.load_discovery_from(
            tenant_id,
            conversation_id,
            home_epoch,
            reachable_at,
            Some(source_run_id),
            correlation_id,
        )
        .await
    }

    async fn load_discovery_from(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        home_epoch: HomeEpoch,
        reachable_at: ReachableContextPoint,
        initial_source_run_id: Option<musteragent_identity::RunId>,
        correlation_id: CorrelationId,
    ) -> Result<Option<PriorRunDiscovery>, RuntimeError> {
        let mut legacy_activation_before = None;
        let mut accumulated = PriorDiscoveryAccumulator::default();
        let mut source_run_hint = initial_source_run_id;
        let mut visited = BTreeSet::new();
        let mut previous_sequence = None;
        loop {
            let hinted_source = source_run_hint.take();
            let source = match hinted_source {
                Some(source_run_id) => {
                    context_reachability::load_source_run(
                        &self.pool,
                        tenant_id,
                        conversation_id,
                        home_epoch,
                        source_run_id,
                    )
                    .await
                }
                None => {
                    context_reachability::load_prior_discovery_source(
                        &self.pool,
                        tenant_id,
                        conversation_id,
                        home_epoch,
                        reachable_at,
                        legacy_activation_before,
                    )
                    .await
                }
            }
            .map_err(|error| {
                database_dependency(error, "admission.load_prior_discovery", correlation_id)
            })?;
            let Some(source) = source else {
                return Ok(accumulated.finish());
            };
            let source_run_id = match hinted_source {
                Some(source_run_id) => source_run_id,
                None => id(
                    source
                        .try_get("run_id")
                        .map_err(|_| dependency(correlation_id))?,
                    correlation_id,
                )?,
            };
            let admitted_sequence = LedgerOffset::new(parse_u64(
                source
                    .try_get("admitted_sequence")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?);
            if hinted_source.is_some() {
                let after_source = LedgerOffset::new(
                    admitted_sequence
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| corrupt(correlation_id))?,
                );
                let reachable_source = context_reachability::load_prior_discovery_source(
                    &self.pool,
                    tenant_id,
                    conversation_id,
                    home_epoch,
                    reachable_at,
                    Some(after_source),
                )
                .await
                .map_err(|_| dependency(correlation_id))?;
                let reachable_source_id = reachable_source
                    .map(|row| {
                        let value = row
                            .try_get("run_id")
                            .map_err(|_| dependency(correlation_id))?;
                        id(value, correlation_id).map_err(RuntimeError::from)
                    })
                    .transpose()?;
                if reachable_source_id != Some(source_run_id) {
                    tracing::warn!(
                        tenant_id = %tenant_id,
                        conversation_id = %conversation_id,
                        source_run_id = %source_run_id,
                        error.code = ErrorCode::InternalInvariantViolated.as_str(),
                        error.stage = "prior_discovery_source_reachability",
                        "Prior discovery decision points outside the reachable context"
                    );
                    return Err(corrupt(correlation_id));
                }
            }
            if previous_sequence.is_some_and(|previous| admitted_sequence >= previous)
                || admitted_sequence > reachable_at.through_offset
                || !visited.insert(source_run_id)
            {
                tracing::warn!(
                    tenant_id = %tenant_id,
                    conversation_id = %conversation_id,
                    source_run_id = %source_run_id,
                    admitted_sequence = admitted_sequence.get(),
                    previous_sequence = previous_sequence.map(LedgerOffset::get),
                    error.code = ErrorCode::InternalInvariantViolated.as_str(),
                    error.stage = "prior_discovery_source_order",
                    "Prior discovery reconstruction rejected an invalid source chain"
                );
                return Err(corrupt(correlation_id));
            }
            previous_sequence = Some(admitted_sequence);
            let encoded_snapshot: Vec<u8> = source
                .try_get("encoded_snapshot")
                .map_err(|_| dependency(correlation_id))?;
            let persisted_hash = Sha256::from_bytes(
                source
                    .try_get::<Vec<u8>, _>("content_hash")
                    .map_err(|_| dependency(correlation_id))?,
            )
            .map_err(|error| {
                tracing::warn!(
                    tenant_id = %tenant_id,
                    conversation_id = %conversation_id,
                    source_run_id = %source_run_id,
                    error.code = ErrorCode::InternalInvariantViolated.as_str(),
                    error.stage = "prior_discovery_persisted_hash_decode",
                    error = %error,
                    "Prior discovery reconstruction found an invalid persisted Snapshot hash"
                );
                corrupt(correlation_id)
            })?;
            let rows = context_reachability::load_reachable_tool_exposures(
                &self.pool,
                tenant_id,
                conversation_id,
                home_epoch,
                source_run_id,
                reachable_at,
            )
            .await
            .map_err(|_| dependency(correlation_id))?;
            let projected_exposures = rows
                .into_iter()
                .map(|row| {
                    let key = row
                        .try_get::<String, _>("tool_key")
                        .map_err(|_| dependency(correlation_id))?;
                    let ordinal = row
                        .try_get::<i32, _>("ordinal")
                        .map_err(|_| dependency(correlation_id))?;
                    let ordinal = usize::try_from(ordinal).map_err(|_| corrupt(correlation_id))?;
                    let schema_hash = Sha256::from_bytes(
                        row.try_get::<Vec<u8>, _>("schema_hash")
                            .map_err(|_| dependency(correlation_id))?,
                    )
                    .map_err(|_| corrupt(correlation_id))?;
                    Ok(musteragent_discovery::ToolExposureItem {
                        tool_ref: musteragent_tool::ToolKey::from_versioned_key(
                            musteragent_identity::VersionedKey::new(key)
                                .map_err(|_| corrupt(correlation_id))?,
                        ),
                        schema_hash,
                        ordinal: std::num::NonZeroU32::new(
                            u32::try_from(ordinal).map_err(|_| corrupt(correlation_id))?,
                        )
                        .ok_or_else(|| corrupt(correlation_id))?,
                    })
                })
                .collect::<Result<Vec<_>, RuntimeError>>()?;
            let rebuilt = rebuild_prior_run_discovery_with_applications(
                self.snapshots.as_ref(),
                source_run_id,
                &encoded_snapshot,
                persisted_hash,
                &projected_exposures,
                self.application_catalogs.as_deref(),
            )
            .await
            .map_err(|error| {
                if error
                    == musteragent_runtime_session::PriorDiscoveryMaterialError::SourceUnavailable
                {
                    return dependency(correlation_id);
                }
                tracing::warn!(
                    tenant_id = %tenant_id,
                    conversation_id = %conversation_id,
                    source_run_id = %source_run_id,
                    admitted_sequence = admitted_sequence.get(),
                    error.code = ErrorCode::InternalInvariantViolated.as_str(),
                    error.stage = "prior_discovery_snapshot_rebuild",
                    error = %error,
                    "Prior discovery reconstruction rejected persisted Snapshot material"
                );
                corrupt(correlation_id)
            })?;
            let evidence = match rebuilt.carry_forward_decision_ref.as_ref() {
                Some(reference) => {
                    let content = self.content.as_ref().ok_or_else(|| {
                        tracing::warn!(
                            tenant_id = %tenant_id,
                            conversation_id = %conversation_id,
                            source_run_id = %source_run_id,
                            error.code = ErrorCode::InternalInvariantViolated.as_str(),
                            error.stage = "prior_discovery_content_store",
                            "Prior discovery decision cannot be verified without its content store"
                        );
                        corrupt(correlation_id)
                    })?;
                    let encoded_evidence = content
                        .read(tenant_id, reference)
                        .await
                        .map_err(|error| {
                            tracing::warn!(
                                tenant_id = %tenant_id,
                                conversation_id = %conversation_id,
                                source_run_id = %source_run_id,
                                content_id = %reference.content_id,
                                error.code = ErrorCode::InternalInvariantViolated.as_str(),
                                error.stage = "prior_discovery_evidence_read",
                                error = %error,
                                "Prior discovery reconstruction could not read carry-forward evidence"
                            );
                            corrupt(correlation_id)
                        })?;
                    Some(
                        DiscoveryCarryForwardEvidence::decode_canonical_json(&encoded_evidence)
                            .map_err(|error| {
                                tracing::warn!(
                                    tenant_id = %tenant_id,
                                    conversation_id = %conversation_id,
                                    source_run_id = %source_run_id,
                                    content_id = %reference.content_id,
                                    error.code = ErrorCode::InternalInvariantViolated.as_str(),
                                    error.stage = "prior_discovery_evidence_decode",
                                    error = %error,
                                    "Prior discovery reconstruction rejected carry-forward evidence"
                                );
                                corrupt(correlation_id)
                            })?,
                    )
                }
                None => None,
            };
            let exhaustive = accumulated
                .apply(rebuilt, evidence.as_ref())
                .map_err(|error| {
                    tracing::warn!(
                        tenant_id = %tenant_id,
                        conversation_id = %conversation_id,
                        source_run_id = %source_run_id,
                        error.code = ErrorCode::InternalInvariantViolated.as_str(),
                        error.stage = "prior_discovery_accumulation",
                        error = %error,
                        "Prior discovery reconstruction rejected invalid carry-forward evidence"
                    );
                    corrupt(correlation_id)
                })?;
            if exhaustive {
                return Ok(accumulated.finish());
            }
            if let Some(evidence) = evidence {
                source_run_hint = Some(evidence.source_run_id);
            } else {
                legacy_activation_before = Some(admitted_sequence);
            }
        }
    }
}
