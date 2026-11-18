//! PostgreSQL authority for Browser provider leases and Job receipts.
//!
//! Browser execution remains client-side. This adapter exists so Cloud API,
//! Cloud Worker, and reconnecting pages agree on one fence and one terminal
//! result even when they do not share a process.

#[path = "browser_gateway_recovery.rs"]
mod recovery;

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use musteragent_capability::{
    BrowserDiagnostic, BrowserDiagnosticSeverity, BrowserJobMetrics, BrowserJobRequest,
    BrowserJobResult, BrowserJobStatus, BrowserLeaseProof, BrowserProviderContract,
    BrowserProviderLease, CapabilityKey,
};
use musteragent_identity::{
    BrowserJobId, BrowserLeaseEpoch, BrowserLeaseId, BrowserSessionId, CapabilityProviderId,
    FencingToken, IdentityError, PageInstanceId, PrincipalId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_persistence_codec::{
    decode_browser_job_request, decode_browser_job_result, encode_browser_feature_probe,
    encode_browser_job_request, encode_browser_job_result,
};
use musteragent_runtime_capability::{
    BrowserGatewayAdmission, BrowserGatewayStore, BrowserJobDelivery, BrowserJobDeliveryState,
    BrowserJobTerminalReceipt, BrowserLeaseRegistration, BrowserProviderRuntimeError,
    DurableBrowserJob, MAX_BROWSER_LEASE_TTL_MILLIS,
};
use musteragent_runtime_contract::ApplicationFuture;
use prost::Message as _;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row as _};

struct TransitionSequence {
    delivery_sequence: Option<u64>,
    provider_sequence: u64,
}

const MAX_DELIVERY_BATCH: u16 = 64;
const STATE_QUEUED: i16 = 1;
const STATE_CLAIMED: i16 = 2;
const STATE_RUNNING: i16 = 3;
const STATE_CANCEL_REQUESTED: i16 = 4;
const STATE_TERMINAL: i16 = 5;

#[derive(Clone)]
pub struct PostgresBrowserGatewayStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    routing: Option<std::sync::Arc<crate::PostgresSandboxRoutingStore>>,
}

impl PostgresBrowserGatewayStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            pool,
            database,
            routing: None,
        }
    }

    /// Attach the fenced Invocation routing owner used by sandbox tools.
    pub fn with_sandbox_routing(
        mut self,
        routing: std::sync::Arc<crate::PostgresSandboxRoutingStore>,
    ) -> Self {
        self.routing = Some(routing);
        self
    }

    async fn write_unit_of_work(
        &self,
    ) -> Result<crate::GovernedPostgresTransaction<'static>, BrowserProviderRuntimeError> {
        self.database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::BrowserGateway)
            .await
            .map_err(|_| BrowserProviderRuntimeError::DurabilityUnavailable)
    }

    async fn consume_gateway_ticket_inner(
        &self,
        admission: BrowserGatewayAdmission,
    ) -> Result<(), BrowserProviderRuntimeError> {
        if admission.expires_at <= admission.issued_at {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }
        let result = sqlx::query(
            "WITH authority_clock AS (
                 SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             )
             INSERT INTO capability.browser_gateway_ticket_consumptions (
                 ticket_id, tenant_id, principal_id, browser_session_id,
                 issued_at_ms, expires_at_ms, consumed_at_ms
             )
             SELECT $1, $2, $3, $4, $5, $6, now_ms
             FROM authority_clock
             WHERE $5 <= now_ms AND now_ms < $6",
        )
        .bind(admission.ticket_id.into_uuid())
        .bind(admission.tenant_id.into_uuid())
        .bind(admission.principal_id.into_uuid())
        .bind(admission.browser_session_id.into_uuid())
        .bind(admission.issued_at.unix_millis())
        .bind(admission.expires_at.unix_millis())
        .execute(&self.pool)
        .await;
        match result {
            Ok(value) if value.rows_affected() == 1 => Ok(()),
            Ok(_) => Err(BrowserProviderRuntimeError::LeaseExpired),
            Err(error) if unique_violation(&error) => {
                Err(BrowserProviderRuntimeError::TicketReplayed)
            }
            Err(error) => Err(map_database(error)),
        }
    }

    async fn register_provider_inner(
        &self,
        registration: BrowserLeaseRegistration,
    ) -> Result<BrowserProviderLease, BrowserProviderRuntimeError> {
        registration.validate()?;
        let ttl = bounded_i64(registration.ttl_millis.get())?;
        let lease_id = BrowserLeaseId::generate();
        let capabilities = registration
            .capabilities
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect::<Vec<_>>();
        let feature_probe = encode_browser_feature_probe(&registration.feature_probe)
            .map_err(|_| BrowserProviderRuntimeError::InvalidRegistration)?
            .encode_to_vec();
        let mut transaction = self.write_unit_of_work().await?;
        let (scope, scope_tenant, scope_principal) = registration.authority();
        sqlx::query("INSERT INTO capability.browser_provider_identities (provider_id, executor_kind, registration_scope, tenant_id, principal_id) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (provider_id) DO NOTHING")
            .bind(registration.provider_id.into_uuid()).bind(registration.executor.as_str()).bind(scope)
            .bind(scope_tenant.map(|t|t.into_uuid())).bind(scope_principal.map(|p|p.into_uuid()))
            .execute(&mut *transaction).await.map_err(map_database)?;
        let identity_matches: bool = sqlx::query_scalar("SELECT executor_kind=$2 AND registration_scope=$3 AND tenant_id IS NOT DISTINCT FROM $4 AND principal_id IS NOT DISTINCT FROM $5 FROM capability.browser_provider_identities WHERE provider_id=$1 FOR SHARE")
            .bind(registration.provider_id.into_uuid()).bind(registration.executor.as_str()).bind(scope)
            .bind(scope_tenant.map(|t|t.into_uuid())).bind(scope_principal.map(|p|p.into_uuid()))
            .fetch_one(&mut *transaction).await.map_err(map_database)?;
        if !identity_matches {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }

        let row = sqlx::query(
            "WITH authority_clock AS (
                 SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             )
             INSERT INTO capability.browser_provider_slots (
                 tenant_id, principal_id, browser_session_id, page_instance_id,
                 last_epoch, last_fence, active_lease_id, provider_id,
                 runtime_release_ref, capability_keys, resource_profile_ref,
                 feature_probe_proto, issued_at_ms, expires_at_ms, last_delivery_sequence, executor_kind
             )
             SELECT $1, $2, $3, $4, 1, 1, $5, $6, $7, $8, $9, $10,
                    now_ms, now_ms + $11, 0, $12
             FROM authority_clock
             ON CONFLICT (tenant_id, principal_id, browser_session_id, page_instance_id)
             DO UPDATE SET
                 last_epoch = browser_provider_slots.last_epoch + 1,
                 last_fence = browser_provider_slots.last_fence + 1,
                 active_lease_id = EXCLUDED.active_lease_id,
                 provider_id = EXCLUDED.provider_id,
                 executor_kind = EXCLUDED.executor_kind,
                 runtime_release_ref = EXCLUDED.runtime_release_ref,
                 capability_keys = EXCLUDED.capability_keys,
                 resource_profile_ref = EXCLUDED.resource_profile_ref,
                 feature_probe_proto = EXCLUDED.feature_probe_proto,
                 issued_at_ms = EXCLUDED.issued_at_ms,
                 expires_at_ms = EXCLUDED.expires_at_ms,
                 last_delivery_sequence = 0
             WHERE browser_provider_slots.executor_kind = EXCLUDED.executor_kind
               AND browser_provider_slots.last_epoch < 9223372036854775807
               AND browser_provider_slots.last_fence < 9223372036854775807
             RETURNING last_epoch, last_fence, issued_at_ms, expires_at_ms",
        )
        .bind(registration.tenant_id.into_uuid())
        .bind(registration.principal_id.into_uuid())
        .bind(registration.browser_session_id.into_uuid())
        .bind(registration.page_instance_id.into_uuid())
        .bind(lease_id.into_uuid())
        .bind(registration.provider_id.into_uuid())
        .bind(registration.runtime_release_ref.as_str())
        .bind(&capabilities)
        .bind(registration.resource_profile_ref.as_str())
        .bind(feature_probe)
        .bind(ttl)
        .bind(registration.executor.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_database)?;
        let Some(row) = row else {
            let executor:String=sqlx::query_scalar("SELECT executor_kind FROM capability.browser_provider_slots WHERE tenant_id=$1 AND principal_id=$2 AND browser_session_id=$3 AND page_instance_id=$4")
                .bind(registration.tenant_id.into_uuid()).bind(registration.principal_id.into_uuid()).bind(registration.browser_session_id.into_uuid()).bind(registration.page_instance_id.into_uuid()).fetch_one(&mut *transaction).await.map_err(map_database)?;
            return Err(if executor != registration.executor.as_str() {
                BrowserProviderRuntimeError::InvalidRegistration
            } else {
                BrowserProviderRuntimeError::FenceExhausted
            });
        };

        let lease = BrowserProviderLease {
            lease_id,
            tenant_id: registration.tenant_id,
            principal_id: registration.principal_id,
            browser_session_id: registration.browser_session_id,
            page_instance_id: registration.page_instance_id,
            provider_id: registration.provider_id,
            runtime_release_ref: registration.runtime_release_ref,
            capabilities: registration.capabilities,
            resource_profile_ref: registration.resource_profile_ref,
            epoch: decode_epoch(row.try_get("last_epoch").map_err(map_database)?)?,
            fence: decode_fence(row.try_get("last_fence").map_err(map_database)?)?,
            issued_at: timestamp(row.try_get("issued_at_ms").map_err(map_database)?),
            expires_at: timestamp(row.try_get("expires_at_ms").map_err(map_database)?),
        };
        lease
            .validate()
            .map_err(BrowserProviderRuntimeError::from)?;
        let requeued_sequences = sqlx::query_scalar::<_, i64>(
            "WITH locked AS MATERIALIZED (
                 SELECT job_id, created_at_ms
                 FROM capability.browser_jobs
                 WHERE tenant_id = $1 AND principal_id = $2
                   AND runtime_release_ref = $6 AND capability_key = ANY($7)
                   AND browser_session_id = $3 AND page_instance_id = $4
                   AND provider_id = $5 AND state = 1
                 ORDER BY created_at_ms, job_id
                 FOR UPDATE
             ), sequenced AS (
                 SELECT job_id,
                        row_number() OVER (ORDER BY created_at_ms, job_id) AS next_sequence
                 FROM locked
             )
             UPDATE capability.browser_jobs AS job
             SET lease_id = $8, lease_epoch = $9, lease_fence = $10,
                 browser_session_id = $3, page_instance_id = $4, provider_id = $5, state = 1,
                 delivery_sequence = sequenced.next_sequence,
                 last_provider_sequence = 0, updated_at_ms = $11
             FROM sequenced
             WHERE job.job_id = sequenced.job_id
             RETURNING job.delivery_sequence",
        )
        .bind(lease.tenant_id.into_uuid())
        .bind(lease.principal_id.into_uuid())
        .bind(lease.browser_session_id.into_uuid())
        .bind(lease.page_instance_id.into_uuid())
        .bind(lease.provider_id.into_uuid())
        .bind(lease.runtime_release_ref.as_str())
        .bind(&capabilities)
        .bind(lease.lease_id.into_uuid())
        .bind(bounded_i64(lease.epoch.get())?)
        .bind(bounded_i64(lease.fence.get())?)
        .bind(lease.issued_at.unix_millis())
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_database)?;
        let requeued_count = i64::try_from(requeued_sequences.len())
            .map_err(|_| BrowserProviderRuntimeError::SequenceExhausted)?;
        let affected = sqlx::query(
            "UPDATE capability.browser_provider_slots
             SET last_delivery_sequence = $9
             WHERE tenant_id = $1 AND principal_id = $2
               AND browser_session_id = $3 AND page_instance_id = $4
               AND active_lease_id = $5 AND provider_id = $6
               AND last_epoch = $7 AND last_fence = $8",
        )
        .bind(lease.tenant_id.into_uuid())
        .bind(lease.principal_id.into_uuid())
        .bind(lease.browser_session_id.into_uuid())
        .bind(lease.page_instance_id.into_uuid())
        .bind(lease.lease_id.into_uuid())
        .bind(lease.provider_id.into_uuid())
        .bind(bounded_i64(lease.epoch.get())?)
        .bind(bounded_i64(lease.fence.get())?)
        .bind(requeued_count)
        .execute(&mut *transaction)
        .await
        .map_err(map_database)?
        .rows_affected();
        if affected != 1 {
            return Err(BrowserProviderRuntimeError::StaleLease);
        }
        transaction
            .commit()
            .await
            .map_err(|_| BrowserProviderRuntimeError::DurabilityUnavailable)?;
        Ok(lease)
    }

    async fn renew_provider_inner(
        &self,
        proof: BrowserLeaseProof,
        ttl_millis: NonZeroU64,
    ) -> Result<BrowserProviderLease, BrowserProviderRuntimeError> {
        if ttl_millis.get() > MAX_BROWSER_LEASE_TTL_MILLIS {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }
        let ttl = bounded_i64(ttl_millis.get())?;
        let row = sqlx::query(
            "WITH authority_clock AS (
                 SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             )
             UPDATE capability.browser_provider_slots AS slot
             SET expires_at_ms = authority_clock.now_ms + $9
             FROM authority_clock
             WHERE slot.tenant_id = $1 AND slot.principal_id = $2
               AND slot.browser_session_id = $3 AND slot.page_instance_id = $4
               AND slot.active_lease_id = $5 AND slot.provider_id = $6
               AND slot.last_epoch = $7 AND slot.last_fence = $8
               AND slot.issued_at_ms <= authority_clock.now_ms
               AND slot.expires_at_ms > authority_clock.now_ms
             RETURNING slot.runtime_release_ref, slot.capability_keys,
                       slot.resource_profile_ref, slot.issued_at_ms, slot.expires_at_ms",
        )
        .bind(proof.tenant_id.into_uuid())
        .bind(proof.principal_id.into_uuid())
        .bind(proof.browser_session_id.into_uuid())
        .bind(proof.page_instance_id.into_uuid())
        .bind(proof.lease_id.into_uuid())
        .bind(proof.provider_id.into_uuid())
        .bind(bounded_i64(proof.epoch.get())?)
        .bind(bounded_i64(proof.fence.get())?)
        .bind(ttl)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_database)?
        .ok_or(BrowserProviderRuntimeError::LeaseExpired)?;
        lease_from_active_row(proof, &row)
    }

    async fn release_provider_inner(
        &self,
        proof: BrowserLeaseProof,
    ) -> Result<(), BrowserProviderRuntimeError> {
        let affected = sqlx::query(
            "UPDATE capability.browser_provider_slots
             SET active_lease_id = NULL, provider_id = NULL, runtime_release_ref = NULL,
                 capability_keys = NULL, resource_profile_ref = NULL,
                 feature_probe_proto = NULL, issued_at_ms = NULL, expires_at_ms = NULL,
                 last_delivery_sequence = 0
             WHERE tenant_id = $1 AND principal_id = $2
               AND browser_session_id = $3 AND page_instance_id = $4
               AND active_lease_id = $5 AND provider_id = $6
               AND last_epoch = $7 AND last_fence = $8",
        )
        .bind(proof.tenant_id.into_uuid())
        .bind(proof.principal_id.into_uuid())
        .bind(proof.browser_session_id.into_uuid())
        .bind(proof.page_instance_id.into_uuid())
        .bind(proof.lease_id.into_uuid())
        .bind(proof.provider_id.into_uuid())
        .bind(bounded_i64(proof.epoch.get())?)
        .bind(bounded_i64(proof.fence.get())?)
        .execute(&self.pool)
        .await
        .map_err(map_database)?
        .rows_affected();
        if affected == 1 {
            Ok(())
        } else {
            Err(BrowserProviderRuntimeError::StaleLease)
        }
    }

    async fn route_and_enqueue_inner(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        contract: BrowserProviderContract,
        request: BrowserJobRequest,
    ) -> Result<DurableBrowserJob, BrowserProviderRuntimeError> {
        // The legacy browser-only port cannot select a server executor.
        let mut tx = self.write_unit_of_work().await?;
        let job = self
            .enqueue_on(
                &mut tx,
                musteragent_sandbox::SandboxExecutor::Browser,
                tenant_id,
                principal_id,
                contract,
                request,
            )
            .await?;
        tx.commit()
            .await
            .map_err(|_| BrowserProviderRuntimeError::DurabilityUnavailable)?;
        Ok(job)
    }

    pub(crate) async fn enqueue_on(
        &self,
        transaction: &mut sqlx::PgConnection,
        executor: musteragent_sandbox::SandboxExecutor,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        contract: BrowserProviderContract,
        request: BrowserJobRequest,
    ) -> Result<DurableBrowserJob, BrowserProviderRuntimeError> {
        request.validate()?;
        if request.security_profile == musteragent_capability::BrowserSecurityProfile::ProxyEgress
            && executor != musteragent_sandbox::SandboxExecutor::Server
        {
            return Err(BrowserProviderRuntimeError::ProviderContractMismatch);
        }
        if !contract.accepts_request(&request) {
            return Err(BrowserProviderRuntimeError::ProviderContractMismatch);
        }
        let capability = contract.capability_key();
        let request_proto = encode_browser_job_request(&request)
            .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?
            .encode_to_vec();
        let slot = sqlx::query(
            "WITH authority_clock AS (
                 SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             )
             SELECT slot.browser_session_id, slot.page_instance_id, slot.active_lease_id,
                    slot.provider_id, slot.last_epoch, slot.last_fence,
                    authority_clock.now_ms
             FROM capability.browser_provider_slots AS slot, authority_clock
             WHERE slot.tenant_id = $1 AND slot.principal_id = $2
               AND slot.executor_kind = $5
               AND slot.runtime_release_ref = $3
               AND $4 = ANY(slot.capability_keys)
               AND slot.active_lease_id IS NOT NULL
               AND slot.issued_at_ms <= authority_clock.now_ms
               AND slot.expires_at_ms > authority_clock.now_ms
             ORDER BY slot.issued_at_ms DESC, slot.last_epoch DESC,
                      slot.last_fence DESC, slot.active_lease_id DESC
             FOR UPDATE OF slot
             LIMIT 1",
        )
        .bind(tenant_id.into_uuid())
        .bind(principal_id.into_uuid())
        .bind(request.runtime_release_ref.as_str())
        .bind(capability.as_str())
        .bind(executor.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_database)?
        .ok_or(BrowserProviderRuntimeError::ProviderUnavailable)?;
        let proof = proof_from_route_row(tenant_id, principal_id, &slot)?;
        let now_ms: i64 = slot.try_get("now_ms").map_err(map_database)?;
        let delivery_sequence: i64 = sqlx::query_scalar(
            "UPDATE capability.browser_provider_slots
             SET last_delivery_sequence = last_delivery_sequence + 1
             WHERE tenant_id = $1 AND principal_id = $2
               AND browser_session_id = $3 AND page_instance_id = $4
               AND active_lease_id = $5 AND provider_id = $6
               AND last_epoch = $7 AND last_fence = $8
               AND last_delivery_sequence < 9223372036854775807
             RETURNING last_delivery_sequence",
        )
        .bind(proof.tenant_id.into_uuid())
        .bind(proof.principal_id.into_uuid())
        .bind(proof.browser_session_id.into_uuid())
        .bind(proof.page_instance_id.into_uuid())
        .bind(proof.lease_id.into_uuid())
        .bind(proof.provider_id.into_uuid())
        .bind(bounded_i64(proof.epoch.get())?)
        .bind(bounded_i64(proof.fence.get())?)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_database)?
        .ok_or(BrowserProviderRuntimeError::SequenceExhausted)?;
        let insert = sqlx::query(
            "INSERT INTO capability.browser_jobs (
                 job_id, tenant_id, principal_id, invocation_id, capability_key,
                 runtime_release_ref, lease_id, browser_session_id, page_instance_id,
                 provider_id, lease_epoch, lease_fence, delivery_sequence, state,
                 last_provider_sequence, request_proto, created_at_ms, updated_at_ms
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                 1, 0, $14, $15, $15
             )",
        )
        .bind(request.job_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(principal_id.into_uuid())
        .bind(request.invocation_id.into_uuid())
        .bind(capability.as_str())
        .bind(request.runtime_release_ref.as_str())
        .bind(proof.lease_id.into_uuid())
        .bind(proof.browser_session_id.into_uuid())
        .bind(proof.page_instance_id.into_uuid())
        .bind(proof.provider_id.into_uuid())
        .bind(bounded_i64(proof.epoch.get())?)
        .bind(bounded_i64(proof.fence.get())?)
        .bind(delivery_sequence)
        .bind(request_proto)
        .bind(now_ms)
        .execute(&mut *transaction)
        .await;
        if let Err(error) = insert {
            return Err(if unique_violation(&error) {
                BrowserProviderRuntimeError::DuplicateJob
            } else {
                map_database(error)
            });
        }
        for input in &request.inputs {
            sqlx::query("INSERT INTO capability.browser_job_content_refs(job_id, content_id) VALUES ($1,$2) ON CONFLICT DO NOTHING")
                .bind(request.job_id.into_uuid()).bind(input.content_ref.content_id.into_uuid())
                .execute(&mut *transaction).await.map_err(map_database)?;
        }
        let at = timestamp(now_ms);
        Ok(DurableBrowserJob {
            delivery: BrowserJobDelivery {
                request,
                capability,
                lease: proof,
                state: BrowserJobDeliveryState::Queued,
                last_provider_sequence: 0,
                terminal_result: None,
            },
            delivery_sequence: decode_positive_sequence(delivery_sequence)?,
            created_at: at,
            updated_at: at,
        })
    }

    async fn list_deliverable_inner(
        &self,
        proof: BrowserLeaseProof,
        limit: u16,
    ) -> Result<Vec<DurableBrowserJob>, BrowserProviderRuntimeError> {
        if limit == 0 || limit > MAX_DELIVERY_BATCH {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }
        let rows = sqlx::query(
            "WITH authority_clock AS (
                 SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             )
             SELECT job.*, slot.runtime_release_ref AS active_runtime_release_ref
             FROM capability.browser_jobs AS job
             JOIN capability.browser_provider_slots AS slot
               ON slot.tenant_id = job.tenant_id
              AND slot.principal_id = job.principal_id
              AND slot.browser_session_id = job.browser_session_id
              AND slot.page_instance_id = job.page_instance_id
             CROSS JOIN authority_clock
             WHERE job.tenant_id = $1 AND job.principal_id = $2
               AND job.browser_session_id = $3 AND job.page_instance_id = $4
               AND job.lease_id = $5 AND job.provider_id = $6
               AND job.lease_epoch = $7 AND job.lease_fence = $8
               AND job.state = 1
               AND slot.active_lease_id = job.lease_id
               AND slot.provider_id = job.provider_id
               AND slot.last_epoch = job.lease_epoch
               AND slot.last_fence = job.lease_fence
               AND slot.issued_at_ms <= authority_clock.now_ms
               AND slot.expires_at_ms > authority_clock.now_ms
             ORDER BY job.delivery_sequence
             LIMIT $9",
        )
        .bind(proof.tenant_id.into_uuid())
        .bind(proof.principal_id.into_uuid())
        .bind(proof.browser_session_id.into_uuid())
        .bind(proof.page_instance_id.into_uuid())
        .bind(proof.lease_id.into_uuid())
        .bind(proof.provider_id.into_uuid())
        .bind(bounded_i64(proof.epoch.get())?)
        .bind(bounded_i64(proof.fence.get())?)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_database)?;
        rows.iter().map(decode_durable_job).collect()
    }

    async fn list_cancellations_inner(
        &self,
        proof: BrowserLeaseProof,
        limit: u16,
    ) -> Result<Vec<DurableBrowserJob>, BrowserProviderRuntimeError> {
        if limit == 0 || limit > MAX_DELIVERY_BATCH {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }
        let rows = sqlx::query(
            "WITH authority_clock AS (
                 SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             )
             SELECT job.*
             FROM capability.browser_jobs AS job
             JOIN capability.browser_provider_slots AS slot
               ON slot.tenant_id = job.tenant_id
              AND slot.principal_id = job.principal_id
              AND slot.browser_session_id = job.browser_session_id
              AND slot.page_instance_id = job.page_instance_id
             CROSS JOIN authority_clock
             WHERE job.tenant_id = $1 AND job.principal_id = $2
               AND job.browser_session_id = $3 AND job.page_instance_id = $4
               AND job.lease_id = $5 AND job.provider_id = $6
               AND job.lease_epoch = $7 AND job.lease_fence = $8
               AND job.state = 4
               AND slot.active_lease_id = job.lease_id
               AND slot.provider_id = job.provider_id
               AND slot.last_epoch = job.lease_epoch
               AND slot.last_fence = job.lease_fence
               AND slot.issued_at_ms <= authority_clock.now_ms
               AND slot.expires_at_ms > authority_clock.now_ms
             ORDER BY job.delivery_sequence
             LIMIT $9",
        )
        .bind(proof.tenant_id.into_uuid())
        .bind(proof.principal_id.into_uuid())
        .bind(proof.browser_session_id.into_uuid())
        .bind(proof.page_instance_id.into_uuid())
        .bind(proof.lease_id.into_uuid())
        .bind(proof.provider_id.into_uuid())
        .bind(bounded_i64(proof.epoch.get())?)
        .bind(bounded_i64(proof.fence.get())?)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_database)?;
        rows.iter().map(decode_durable_job).collect()
    }

    async fn transition_inner(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        TransitionSequence {
            delivery_sequence,
            provider_sequence,
        }: TransitionSequence,
        allowed_states: &[i16],
        next_state: Option<i16>,
        progress: Option<serde_json::Value>,
    ) -> Result<(), BrowserProviderRuntimeError> {
        let provider_sequence = bounded_i64(provider_sequence)?;
        let delivery_sequence = delivery_sequence.map(bounded_i64).transpose()?;
        let affected = sqlx::query(
            "WITH authority_clock AS (
                 SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             )
             UPDATE capability.browser_jobs AS job
             SET state = COALESCE($13, job.state),
                 last_provider_sequence = $10,
                 progress_history = CASE WHEN $14::jsonb IS NULL THEN job.progress_history ELSE
                   (SELECT COALESCE(jsonb_agg(item.value ORDER BY item.ordinality), '[]'::jsonb)
                    FROM jsonb_array_elements(job.progress_history) WITH ORDINALITY AS item(value, ordinality)
                    WHERE item.ordinality > jsonb_array_length(job.progress_history) - 63) || jsonb_build_array($14::jsonb) END,
                 updated_at_ms = authority_clock.now_ms
             FROM authority_clock
             WHERE job.job_id = $1 AND job.tenant_id = $2 AND job.principal_id = $3
               AND job.browser_session_id = $4 AND job.page_instance_id = $5
               AND job.lease_id = $6 AND job.provider_id = $7
               AND job.lease_epoch = $8 AND job.lease_fence = $9
               AND job.last_provider_sequence + 1 = $10
               AND ($11::bigint IS NULL OR job.delivery_sequence = $11)
               AND job.state = ANY($12)
               AND EXISTS (
                   SELECT 1 FROM capability.browser_provider_slots AS slot
                   WHERE slot.tenant_id = job.tenant_id
                     AND slot.principal_id = job.principal_id
                     AND slot.browser_session_id = job.browser_session_id
                     AND slot.page_instance_id = job.page_instance_id
                     AND slot.active_lease_id = job.lease_id
                     AND slot.provider_id = job.provider_id
                     AND slot.last_epoch = job.lease_epoch
                     AND slot.last_fence = job.lease_fence
                     AND slot.issued_at_ms <= authority_clock.now_ms
                     AND slot.expires_at_ms > authority_clock.now_ms
               )",
        )
        .bind(job_id.into_uuid())
        .bind(proof.tenant_id.into_uuid())
        .bind(proof.principal_id.into_uuid())
        .bind(proof.browser_session_id.into_uuid())
        .bind(proof.page_instance_id.into_uuid())
        .bind(proof.lease_id.into_uuid())
        .bind(proof.provider_id.into_uuid())
        .bind(bounded_i64(proof.epoch.get())?)
        .bind(bounded_i64(proof.fence.get())?)
        .bind(provider_sequence)
        .bind(delivery_sequence)
        .bind(allowed_states)
        .bind(next_state)
        .bind(progress)
        .execute(&self.pool)
        .await
        .map_err(map_database)?
        .rows_affected();
        if affected == 1 {
            Ok(())
        } else {
            Err(self
                .classify_transition(job_id, proof, provider_sequence)
                .await?)
        }
    }

    async fn classify_transition(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        provider_sequence: i64,
    ) -> Result<BrowserProviderRuntimeError, BrowserProviderRuntimeError> {
        let row = sqlx::query(
            "SELECT tenant_id, principal_id, browser_session_id, page_instance_id,
                    lease_id, provider_id, lease_epoch, lease_fence,
                    last_provider_sequence
             FROM capability.browser_jobs WHERE job_id = $1",
        )
        .bind(job_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_database)?;
        let Some(row) = row else {
            return Ok(BrowserProviderRuntimeError::JobNotFound);
        };
        if !row_matches_proof(&row, proof)? {
            return Ok(BrowserProviderRuntimeError::StaleLease);
        }
        let last: i64 = row
            .try_get("last_provider_sequence")
            .map_err(map_database)?;
        if last.checked_add(1) != Some(provider_sequence) {
            return Ok(BrowserProviderRuntimeError::InvalidProviderSequence);
        }
        Ok(BrowserProviderRuntimeError::InvalidJobTransition)
    }

    async fn request_cancel_inner(
        &self,
        job_id: BrowserJobId,
    ) -> Result<(), BrowserProviderRuntimeError> {
        let mut transaction = self.write_unit_of_work().await?;
        let row = sqlx::query(
            "SELECT job.*,
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             FROM capability.browser_jobs AS job
             WHERE job.job_id = $1
             FOR UPDATE",
        )
        .bind(job_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_database)?
        .ok_or(BrowserProviderRuntimeError::JobNotFound)?;
        let state: i16 = row.try_get("state").map_err(map_database)?;
        let now_ms: i64 = row.try_get("now_ms").map_err(map_database)?;
        match state {
            STATE_QUEUED => {
                let request = decode_request(&row)?;
                let result = BrowserJobResult {
                    job_id,
                    status: BrowserJobStatus::Cancelled,
                    outputs: Vec::new(),
                    diagnostics: vec![BrowserDiagnostic {
                        code: "browser_job_cancelled_before_claim".to_owned(),
                        severity: BrowserDiagnosticSeverity::Info,
                        message: "Browser job was cancelled before execution began".to_owned(),
                        path: None,
                        line: None,
                        column: None,
                    }],
                    stdout_tail: String::new(),
                    stderr_tail: String::new(),
                    metrics: BrowserJobMetrics {
                        wall_time_millis: 0,
                        input_bytes: 0,
                        output_bytes: 0,
                        output_files: 0,
                    },
                    result_hash: musteragent_content_ref::Sha256::digest(b"unsealed"),
                }
                .with_canonical_hash();
                request.validate_result(&result)?;
                let result_proto = encode_browser_job_result(&result)
                    .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?
                    .encode_to_vec();
                let affected = sqlx::query(
                    "UPDATE capability.browser_jobs
                     SET state = 5, terminal_result_proto = $2,
                         terminal_result_hash = $3, updated_at_ms = $4,
                         terminal_at_ms = $4
                     WHERE job_id = $1 AND state = 1 AND last_provider_sequence = 0",
                )
                .bind(job_id.into_uuid())
                .bind(result_proto)
                .bind(result.result_hash.into_bytes().as_slice())
                .bind(now_ms)
                .execute(&mut *transaction)
                .await
                .map_err(map_database)?
                .rows_affected();
                if affected != 1 {
                    return Err(BrowserProviderRuntimeError::InvalidJobTransition);
                }
            }
            STATE_CLAIMED | STATE_RUNNING => {
                let affected = sqlx::query(
                    "UPDATE capability.browser_jobs
                     SET state = 4, updated_at_ms = $2
                     WHERE job_id = $1 AND state = $3",
                )
                .bind(job_id.into_uuid())
                .bind(now_ms)
                .bind(state)
                .execute(&mut *transaction)
                .await
                .map_err(map_database)?
                .rows_affected();
                if affected != 1 {
                    return Err(BrowserProviderRuntimeError::InvalidJobTransition);
                }
            }
            STATE_CANCEL_REQUESTED | STATE_TERMINAL => {}
            _ => return Err(BrowserProviderRuntimeError::CorruptDurableState),
        }
        transaction
            .commit()
            .await
            .map_err(|_| BrowserProviderRuntimeError::DurabilityUnavailable)?;
        Ok(())
    }

    async fn seal_terminal_inner(
        &self,
        proof: BrowserLeaseProof,
        provider_sequence: u64,
        result: BrowserJobResult,
    ) -> Result<BrowserJobTerminalReceipt, BrowserProviderRuntimeError> {
        result.validate()?;
        let provider_sequence_i64 = bounded_i64(provider_sequence)?;
        let mut transaction = self.write_unit_of_work().await?;
        let row = sqlx::query(
            "SELECT job.*, floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms,
                    EXISTS (
                        SELECT 1 FROM capability.browser_provider_slots AS slot
                        WHERE slot.tenant_id = job.tenant_id
                          AND slot.principal_id = job.principal_id
                          AND slot.browser_session_id = job.browser_session_id
                          AND slot.page_instance_id = job.page_instance_id
                          AND slot.active_lease_id = job.lease_id
                          AND slot.provider_id = job.provider_id
                          AND slot.last_epoch = job.lease_epoch
                          AND slot.last_fence = job.lease_fence
                          AND slot.issued_at_ms <= floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
                          AND slot.expires_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
                    ) AS lease_active
             FROM capability.browser_jobs AS job
             WHERE job.job_id = $1
             FOR UPDATE",
        )
        .bind(result.job_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_database)?
        .ok_or(BrowserProviderRuntimeError::JobNotFound)?;
        if !row_matches_proof(&row, proof)? {
            return Err(BrowserProviderRuntimeError::StaleLease);
        }
        let request = decode_request(&row)?;
        request.validate_result(&result)?;
        let state: i16 = row.try_get("state").map_err(map_database)?;
        let last_sequence: i64 = row
            .try_get("last_provider_sequence")
            .map_err(map_database)?;
        let result_proto = encode_browser_job_result(&result)
            .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?
            .encode_to_vec();
        if state == STATE_TERMINAL {
            let stored_sequence = decode_nonnegative_sequence(last_sequence)?;
            let stored_proto: Vec<u8> =
                row.try_get("terminal_result_proto").map_err(map_database)?;
            if stored_sequence == provider_sequence && stored_proto == result_proto {
                return terminal_receipt_from_row(&row, proof);
            }
            return Err(BrowserProviderRuntimeError::InvalidJobTransition);
        }
        if !row
            .try_get::<bool, _>("lease_active")
            .map_err(map_database)?
        {
            return Err(BrowserProviderRuntimeError::LeaseExpired);
        }
        if !matches!(state, STATE_RUNNING | STATE_CANCEL_REQUESTED) {
            return Err(BrowserProviderRuntimeError::InvalidJobTransition);
        }
        if last_sequence.checked_add(1) != Some(provider_sequence_i64) {
            return Err(BrowserProviderRuntimeError::InvalidProviderSequence);
        }
        // Cancellation is a request. The original owner may have completed
        // before observing it, or may only prove an indeterminate outcome.
        let now_ms: i64 = row.try_get("now_ms").map_err(map_database)?;
        let affected = sqlx::query(
            "UPDATE capability.browser_jobs
             SET state = 5, last_provider_sequence = $2,
                 terminal_result_proto = $3, terminal_result_hash = $4,
                 updated_at_ms = $5, terminal_at_ms = $5
             WHERE job_id = $1 AND state = $6 AND last_provider_sequence = $7",
        )
        .bind(result.job_id.into_uuid())
        .bind(provider_sequence_i64)
        .bind(result_proto)
        .bind(result.result_hash.into_bytes().as_slice())
        .bind(now_ms)
        .bind(state)
        .bind(last_sequence)
        .execute(&mut *transaction)
        .await
        .map_err(map_database)?
        .rows_affected();
        if affected != 1 {
            return Err(BrowserProviderRuntimeError::InvalidJobTransition);
        }
        for output in &result.outputs {
            sqlx::query("INSERT INTO capability.browser_job_content_refs(job_id, content_id) VALUES ($1,$2) ON CONFLICT DO NOTHING")
                .bind(result.job_id.into_uuid()).bind(output.content_ref.content_id.into_uuid())
                .execute(&mut *transaction).await.map_err(map_database)?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| BrowserProviderRuntimeError::DurabilityUnavailable)?;
        Ok(BrowserJobTerminalReceipt {
            job_id: result.job_id,
            invocation_id: request.invocation_id,
            lease: proof,
            provider_sequence,
            result,
            sealed_at: timestamp(now_ms),
        })
    }

    async fn load_terminal_receipt_inner(
        &self,
        tenant_id: TenantId,
        job_id: BrowserJobId,
    ) -> Result<Option<BrowserJobTerminalReceipt>, BrowserProviderRuntimeError> {
        let row = sqlx::query(
            "SELECT * FROM capability.browser_jobs
             WHERE tenant_id = $1 AND job_id = $2 AND state = 5",
        )
        .bind(tenant_id.into_uuid())
        .bind(job_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_database)?;
        row.as_ref()
            .map(|value| terminal_receipt_from_row(value, proof_from_job_row(value)?))
            .transpose()
    }
}

impl BrowserGatewayStore for PostgresBrowserGatewayStore {
    fn sandbox_routing(
        &self,
    ) -> Result<&dyn musteragent_runtime_capability::SandboxRoutingStore, BrowserProviderRuntimeError>
    {
        self.routing
            .as_deref()
            .map(|r| r as &dyn musteragent_runtime_capability::SandboxRoutingStore)
            .ok_or(BrowserProviderRuntimeError::SandboxRoutingUnavailable)
    }

    fn lease_is_current(
        &self,
        proof: BrowserLeaseProof,
    ) -> ApplicationFuture<'_, Result<bool, BrowserProviderRuntimeError>> {
        Box::pin(async move {
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM capability.browser_provider_slots WHERE tenant_id=$1 AND principal_id=$2 AND browser_session_id=$3 AND page_instance_id=$4 AND active_lease_id=$5 AND provider_id=$6 AND last_epoch=$7 AND last_fence=$8 AND expires_at_ms > floor(extract(epoch FROM clock_timestamp())*1000)::bigint)")
                .bind(proof.tenant_id.into_uuid()).bind(proof.principal_id.into_uuid()).bind(proof.browser_session_id.into_uuid()).bind(proof.page_instance_id.into_uuid()).bind(proof.lease_id.into_uuid()).bind(proof.provider_id.into_uuid()).bind(bounded_i64(proof.epoch.get())?).bind(bounded_i64(proof.fence.get())?).fetch_one(&self.pool).await.map_err(map_database)
        })
    }

    fn consume_gateway_ticket(
        &self,
        admission: BrowserGatewayAdmission,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(self.consume_gateway_ticket_inner(admission))
    }

    fn register_provider(
        &self,
        registration: BrowserLeaseRegistration,
    ) -> ApplicationFuture<'_, Result<BrowserProviderLease, BrowserProviderRuntimeError>> {
        Box::pin(self.register_provider_inner(registration))
    }

    fn renew_provider(
        &self,
        proof: BrowserLeaseProof,
        ttl_millis: NonZeroU64,
    ) -> ApplicationFuture<'_, Result<BrowserProviderLease, BrowserProviderRuntimeError>> {
        Box::pin(self.renew_provider_inner(proof, ttl_millis))
    }

    fn release_provider(
        &self,
        proof: BrowserLeaseProof,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(self.release_provider_inner(proof))
    }

    fn recover_candidate(
        &self,
        proof: BrowserLeaseProof,
        job_id: BrowserJobId,
        request_hash: musteragent_content_ref::Sha256,
    ) -> ApplicationFuture<'_, Result<Option<DurableBrowserJob>, BrowserProviderRuntimeError>> {
        Box::pin(self.recover_candidate_inner(proof, job_id, request_hash))
    }

    fn candidate_settlement(
        &self,
        proof: BrowserLeaseProof,
        job_id: BrowserJobId,
    ) -> ApplicationFuture<
        '_,
        Result<
            musteragent_runtime_capability::BrowserCandidateSettlement,
            BrowserProviderRuntimeError,
        >,
    > {
        Box::pin(self.candidate_settlement_inner(proof, job_id))
    }

    fn load_job(
        &self,
        tenant_id: TenantId,
        job_id: BrowserJobId,
    ) -> ApplicationFuture<'_, Result<Option<DurableBrowserJob>, BrowserProviderRuntimeError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT * FROM capability.browser_jobs WHERE tenant_id = $1 AND job_id = $2",
            )
            .bind(tenant_id.into_uuid())
            .bind(job_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_database)?;
            row.as_ref().map(decode_durable_job).transpose()
        })
    }

    fn route_and_enqueue(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        contract: BrowserProviderContract,
        request: BrowserJobRequest,
    ) -> ApplicationFuture<'_, Result<DurableBrowserJob, BrowserProviderRuntimeError>> {
        Box::pin(self.route_and_enqueue_inner(tenant_id, principal_id, contract, request))
    }

    fn list_deliverable(
        &self,
        proof: BrowserLeaseProof,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<Vec<DurableBrowserJob>, BrowserProviderRuntimeError>> {
        Box::pin(self.list_deliverable_inner(proof, limit))
    }

    fn claim_job(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        delivery_sequence: u64,
        provider_sequence: u64,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(self.transition_inner(
            job_id,
            proof,
            TransitionSequence {
                delivery_sequence: Some(delivery_sequence),
                provider_sequence,
            },
            &[STATE_QUEUED],
            Some(STATE_CLAIMED),
            None,
        ))
    }

    fn list_cancellations(
        &self,
        proof: BrowserLeaseProof,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<Vec<DurableBrowserJob>, BrowserProviderRuntimeError>> {
        Box::pin(self.list_cancellations_inner(proof, limit))
    }

    fn start_job(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        provider_sequence: u64,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(self.transition_inner(
            job_id,
            proof,
            TransitionSequence {
                delivery_sequence: None,
                provider_sequence,
            },
            &[STATE_CLAIMED],
            Some(STATE_RUNNING),
            None,
        ))
    }

    fn load_progress(
        &self,
        tenant_id: TenantId,
        job_id: BrowserJobId,
    ) -> ApplicationFuture<
        '_,
        Result<
            Vec<musteragent_runtime_capability::BrowserExecutionProgress>,
            BrowserProviderRuntimeError,
        >,
    > {
        Box::pin(async move {
            let row = sqlx::query("SELECT progress_history FROM capability.browser_jobs WHERE tenant_id = $1 AND job_id = $2")
                .bind(tenant_id.into_uuid()).bind(job_id.into_uuid()).fetch_optional(&self.pool).await.map_err(map_database)?;
            let Some(row) = row else {
                return Err(BrowserProviderRuntimeError::JobNotFound);
            };
            serde_json::from_value(row.try_get("progress_history").map_err(map_database)?)
                .map_err(|_| BrowserProviderRuntimeError::InvalidJobTransition)
        })
    }
    fn record_execution_progress(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        sequence: u64,
        detail: Option<musteragent_runtime_capability::BrowserExecutionProgress>,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(async move {
            if let Some(ref detail) = detail {
                detail.validate()?;
            }
            let progress = detail
                .map(serde_json::to_value)
                .transpose()
                .map_err(|_| BrowserProviderRuntimeError::InvalidJobTransition)?;
            self.transition_inner(
                job_id,
                proof,
                TransitionSequence {
                    delivery_sequence: None,
                    provider_sequence: sequence,
                },
                &[STATE_RUNNING, STATE_CANCEL_REQUESTED],
                None,
                progress,
            )
            .await
        })
    }

    fn record_progress(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        provider_sequence: u64,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(self.transition_inner(
            job_id,
            proof,
            TransitionSequence {
                delivery_sequence: None,
                provider_sequence,
            },
            &[STATE_RUNNING, STATE_CANCEL_REQUESTED],
            None,
            None,
        ))
    }

    fn request_cancel(
        &self,
        job_id: BrowserJobId,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(self.request_cancel_inner(job_id))
    }

    fn seal_terminal(
        &self,
        proof: BrowserLeaseProof,
        provider_sequence: u64,
        result: BrowserJobResult,
    ) -> ApplicationFuture<'_, Result<BrowserJobTerminalReceipt, BrowserProviderRuntimeError>> {
        Box::pin(self.seal_terminal_inner(proof, provider_sequence, result))
    }

    fn load_terminal_receipt(
        &self,
        tenant_id: TenantId,
        job_id: BrowserJobId,
    ) -> ApplicationFuture<'_, Result<Option<BrowserJobTerminalReceipt>, BrowserProviderRuntimeError>>
    {
        Box::pin(self.load_terminal_receipt_inner(tenant_id, job_id))
    }
}

fn lease_from_active_row(
    proof: BrowserLeaseProof,
    row: &PgRow,
) -> Result<BrowserProviderLease, BrowserProviderRuntimeError> {
    let capabilities = row
        .try_get::<Vec<String>, _>("capability_keys")
        .map_err(map_database)?
        .into_iter()
        .map(|value| {
            VersionedKey::new(value)
                .map(CapabilityKey::from_versioned_key)
                .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let lease = BrowserProviderLease {
        lease_id: proof.lease_id,
        tenant_id: proof.tenant_id,
        principal_id: proof.principal_id,
        browser_session_id: proof.browser_session_id,
        page_instance_id: proof.page_instance_id,
        provider_id: proof.provider_id,
        runtime_release_ref: VersionedKey::new(
            row.try_get::<String, _>("runtime_release_ref")
                .map_err(map_database)?,
        )
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?,
        capabilities,
        resource_profile_ref: VersionedKey::new(
            row.try_get::<String, _>("resource_profile_ref")
                .map_err(map_database)?,
        )
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?,
        epoch: proof.epoch,
        fence: proof.fence,
        issued_at: timestamp(row.try_get("issued_at_ms").map_err(map_database)?),
        expires_at: timestamp(row.try_get("expires_at_ms").map_err(map_database)?),
    };
    lease
        .validate()
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?;
    Ok(lease)
}

fn proof_from_route_row(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    row: &PgRow,
) -> Result<BrowserLeaseProof, BrowserProviderRuntimeError> {
    Ok(BrowserLeaseProof {
        lease_id: BrowserLeaseId::from_uuid(row.try_get("active_lease_id").map_err(map_database)?)
            .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?,
        tenant_id,
        principal_id,
        browser_session_id: BrowserSessionId::from_uuid(
            row.try_get("browser_session_id").map_err(map_database)?,
        )
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?,
        page_instance_id: PageInstanceId::from_uuid(
            row.try_get("page_instance_id").map_err(map_database)?,
        )
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?,
        provider_id: CapabilityProviderId::from_uuid(
            row.try_get("provider_id").map_err(map_database)?,
        )
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?,
        epoch: decode_epoch(row.try_get("last_epoch").map_err(map_database)?)?,
        fence: decode_fence(row.try_get("last_fence").map_err(map_database)?)?,
    })
}

fn proof_from_job_row(row: &PgRow) -> Result<BrowserLeaseProof, BrowserProviderRuntimeError> {
    Ok(BrowserLeaseProof {
        lease_id: parse_id(row, "lease_id", BrowserLeaseId::from_uuid)?,
        tenant_id: parse_id(row, "tenant_id", TenantId::from_uuid)?,
        principal_id: parse_id(row, "principal_id", PrincipalId::from_uuid)?,
        browser_session_id: parse_id(row, "browser_session_id", BrowserSessionId::from_uuid)?,
        page_instance_id: parse_id(row, "page_instance_id", PageInstanceId::from_uuid)?,
        provider_id: parse_id(row, "provider_id", CapabilityProviderId::from_uuid)?,
        epoch: decode_epoch(row.try_get("lease_epoch").map_err(map_database)?)?,
        fence: decode_fence(row.try_get("lease_fence").map_err(map_database)?)?,
    })
}

fn decode_durable_job(row: &PgRow) -> Result<DurableBrowserJob, BrowserProviderRuntimeError> {
    let request = decode_request(row)?;
    let state = decode_state(row.try_get("state").map_err(map_database)?)?;
    let terminal_result = row
        .try_get::<Option<Vec<u8>>, _>("terminal_result_proto")
        .map_err(map_database)?
        .map(|payload| {
            let wire =
                musteragent_persistence_codec::wire::BrowserJobResult::decode(payload.as_slice())
                    .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?;
            decode_browser_job_result(wire)
                .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)
        })
        .transpose()?;
    Ok(DurableBrowserJob {
        delivery: BrowserJobDelivery {
            capability: CapabilityKey::from_versioned_key(
                VersionedKey::new(
                    row.try_get::<String, _>("capability_key")
                        .map_err(map_database)?,
                )
                .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?,
            ),
            lease: proof_from_job_row(row)?,
            state,
            last_provider_sequence: decode_nonnegative_sequence(
                row.try_get("last_provider_sequence")
                    .map_err(map_database)?,
            )?,
            terminal_result,
            request,
        },
        delivery_sequence: decode_positive_sequence(
            row.try_get("delivery_sequence").map_err(map_database)?,
        )?,
        created_at: timestamp(row.try_get("created_at_ms").map_err(map_database)?),
        updated_at: timestamp(row.try_get("updated_at_ms").map_err(map_database)?),
    })
}

fn decode_request(row: &PgRow) -> Result<BrowserJobRequest, BrowserProviderRuntimeError> {
    let payload: Vec<u8> = row.try_get("request_proto").map_err(map_database)?;
    let wire = musteragent_persistence_codec::wire::BrowserJobRequest::decode(payload.as_slice())
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?;
    decode_browser_job_request(wire).map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)
}

fn terminal_receipt_from_row(
    row: &PgRow,
    proof: BrowserLeaseProof,
) -> Result<BrowserJobTerminalReceipt, BrowserProviderRuntimeError> {
    let payload: Vec<u8> = row.try_get("terminal_result_proto").map_err(map_database)?;
    let wire = musteragent_persistence_codec::wire::BrowserJobResult::decode(payload.as_slice())
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?;
    let result = decode_browser_job_result(wire)
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?;
    let request = decode_request(row)?;
    request.validate_result(&result)?;
    Ok(BrowserJobTerminalReceipt {
        job_id: result.job_id,
        invocation_id: request.invocation_id,
        lease: proof,
        provider_sequence: decode_nonnegative_sequence(
            row.try_get("last_provider_sequence")
                .map_err(map_database)?,
        )?,
        result,
        sealed_at: timestamp(
            row.try_get::<Option<i64>, _>("terminal_at_ms")
                .map_err(map_database)?
                .ok_or(BrowserProviderRuntimeError::CorruptDurableState)?,
        ),
    })
}

fn row_matches_proof(
    row: &PgRow,
    proof: BrowserLeaseProof,
) -> Result<bool, BrowserProviderRuntimeError> {
    Ok(proof_from_job_row(row)? == proof)
}

fn decode_state(value: i16) -> Result<BrowserJobDeliveryState, BrowserProviderRuntimeError> {
    match value {
        STATE_QUEUED => Ok(BrowserJobDeliveryState::Queued),
        STATE_CLAIMED => Ok(BrowserJobDeliveryState::Claimed),
        STATE_RUNNING => Ok(BrowserJobDeliveryState::Running),
        STATE_CANCEL_REQUESTED => Ok(BrowserJobDeliveryState::CancelRequested),
        STATE_TERMINAL => Ok(BrowserJobDeliveryState::Terminal),
        _ => Err(BrowserProviderRuntimeError::CorruptDurableState),
    }
}

fn parse_id<T>(
    row: &PgRow,
    column: &str,
    parse: impl FnOnce(uuid::Uuid) -> Result<T, IdentityError>,
) -> Result<T, BrowserProviderRuntimeError> {
    parse(row.try_get(column).map_err(map_database)?)
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)
}

fn decode_epoch(value: i64) -> Result<BrowserLeaseEpoch, BrowserProviderRuntimeError> {
    BrowserLeaseEpoch::new(decode_positive_sequence(value)?)
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)
}

fn decode_fence(value: i64) -> Result<FencingToken, BrowserProviderRuntimeError> {
    FencingToken::new(decode_positive_sequence(value)?)
        .map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)
}

fn decode_positive_sequence(value: i64) -> Result<u64, BrowserProviderRuntimeError> {
    let value =
        u64::try_from(value).map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)?;
    if value == 0 {
        Err(BrowserProviderRuntimeError::CorruptDurableState)
    } else {
        Ok(value)
    }
}

fn decode_nonnegative_sequence(value: i64) -> Result<u64, BrowserProviderRuntimeError> {
    u64::try_from(value).map_err(|_| BrowserProviderRuntimeError::CorruptDurableState)
}

fn bounded_i64(value: u64) -> Result<i64, BrowserProviderRuntimeError> {
    i64::try_from(value).map_err(|_| BrowserProviderRuntimeError::SequenceExhausted)
}

fn timestamp(value: i64) -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(value)
}

fn unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(value) if value.code().as_deref() == Some("23505"))
}

fn map_database(_error: sqlx::Error) -> BrowserProviderRuntimeError {
    BrowserProviderRuntimeError::DurabilityUnavailable
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_codec_is_closed() {
        assert_eq!(
            decode_state(STATE_CANCEL_REQUESTED).expect("state"),
            BrowserJobDeliveryState::CancelRequested
        );
        assert!(matches!(
            decode_state(0),
            Err(BrowserProviderRuntimeError::CorruptDurableState)
        ));
    }

    #[test]
    fn postgres_sequence_range_is_explicit() {
        assert_eq!(bounded_i64(i64::MAX as u64).expect("max"), i64::MAX);
        assert!(matches!(
            bounded_i64(i64::MAX as u64 + 1),
            Err(BrowserProviderRuntimeError::SequenceExhausted)
        ));
    }
}
