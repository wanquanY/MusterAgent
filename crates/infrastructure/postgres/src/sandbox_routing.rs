//! PostgreSQL route authority: authenticated Run context, pinned Skills, live
//! executor leases, immutable Invocation decisions and fenced Job admission.
use crate::{
    PostgresBrowserGatewayStore, PostgresDatabaseRuntime, PostgresFencedToolInvocationJournal,
};
use musteragent_capability::{BrowserJobRequest, BrowserProviderContract};
use musteragent_content_ref::Sha256;
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_identity::{Actor, PrincipalKind, RunId};
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    BrowserProviderRuntimeError as Error, CapabilityInvocation, DurableBrowserJob,
    DurableToolInvocation, SandboxRouteCommit, SandboxRoutingStore, ToolExecutionMaterial,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_sandbox::*;
use sqlx::{PgConnection, Row};
use std::sync::Arc;

/// Durable route authority bound to the original Run, snapshot and Attempt fence.
#[derive(Clone)]
pub struct PostgresSandboxRoutingStore {
    database: PostgresDatabaseRuntime,
    journal: PostgresFencedToolInvocationJournal,
    snapshots: Arc<dyn RuntimeSnapshotDecoder>,
}
impl PostgresSandboxRoutingStore {
    /// All owners must address the same database. Snapshot decoding is the
    /// admission codec, and the journal owns the canonical event encoding.
    pub fn new(
        journal: PostgresFencedToolInvocationJournal,
        snapshots: Arc<dyn RuntimeSnapshotDecoder>,
    ) -> Self {
        Self {
            database: journal.database().clone(),
            journal,
            snapshots,
        }
    }

    /// Capture the authenticated host's routing context for an admitted Run.
    /// Call from trusted host admission, never with model-supplied surface or
    /// policy. Identity is checked against the original Run requester. Exact
    /// replays are accepted; changing a captured context is forbidden.
    pub async fn bind_run_context(
        &self,
        actor: Actor,
        run: RunId,
        surface: SandboxClientSurface,
        trigger: SandboxTrigger,
        policy: SandboxRoutingPolicy,
    ) -> Result<(), Error> {
        if actor.principal_kind != PrincipalKind::User {
            return Err(Error::CapabilityNotAuthorized);
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::BrowserGateway)
            .await
            .map_err(|_| Error::DurabilityUnavailable)?;
        require_owner(&mut tx, actor, run).await?;
        let (surface, trigger, policy) = (
            surface_code(surface),
            trigger_code(trigger),
            policy_code(policy),
        );
        let changed=sqlx::query("INSERT INTO runtime.sandbox_routing_contexts(run_id,tenant_id,principal_id,surface,trigger,policy) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(run_id) DO NOTHING")
            .bind(run.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(surface).bind(trigger).bind(policy).execute(&mut *tx).await.map_err(db)?;
        if changed.rows_affected() == 0 {
            let existing:(uuid::Uuid,uuid::Uuid,i16,i16,i16)=sqlx::query_as("SELECT tenant_id,principal_id,surface,trigger,policy FROM runtime.sandbox_routing_contexts WHERE run_id=$1")
                .bind(run.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
            if existing
                != (
                    actor.tenant_id.into_uuid(),
                    actor.principal_id.into_uuid(),
                    surface,
                    trigger,
                    policy,
                )
            {
                return Err(Error::RouteConflict);
            }
        }
        tx.commit().await.map_err(|_| Error::DurabilityUnavailable)
    }

    /// Publish trusted sandbox metadata for one tenant-owned immutable Skill
    /// version. Replays must preserve both the instruction digest and declaration.
    /// Hosts invoke this after validating the Skill manifest; model input is not
    /// an authority to downgrade a server-required Skill.
    pub async fn publish_skill_compatibility(
        &self,
        tenant: musteragent_identity::TenantId,
        version: musteragent_identity::SkillVersionId,
        instructions_hash: Sha256,
        compatibility: SandboxCompatibility,
    ) -> Result<(), Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::BrowserGateway)
            .await
            .map_err(|_| Error::DurabilityUnavailable)?;
        let owned:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM skill.skill_versions v JOIN skill.skills s USING(skill_id) WHERE s.tenant_id=$1 AND v.skill_version_id=$2 AND v.instructions_hash=$3)")
            .bind(tenant.into_uuid()).bind(version.into_uuid()).bind(instructions_hash.into_bytes().to_vec()).fetch_one(&mut *tx).await.map_err(db)?;
        if !owned {
            return Err(Error::CapabilityNotAuthorized);
        }
        sqlx::query("INSERT INTO skill.sandbox_declarations(tenant_id,skill_version_id,instructions_hash,compatibility) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
            .bind(tenant.into_uuid()).bind(version.into_uuid()).bind(instructions_hash.into_bytes().to_vec()).bind(compatibility.as_str()).execute(&mut *tx).await.map_err(db)?;
        let existing:(Vec<u8>,String)=sqlx::query_as("SELECT instructions_hash,compatibility FROM skill.sandbox_declarations WHERE tenant_id=$1 AND skill_version_id=$2")
            .bind(tenant.into_uuid()).bind(version.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if existing.0 != instructions_hash.into_bytes() || existing.1 != compatibility.as_str() {
            return Err(Error::RouteConflict);
        }
        tx.commit().await.map_err(|_| Error::DurabilityUnavailable)
    }

    async fn context_inner(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
        contract: BrowserProviderContract,
        request: &BrowserJobRequest,
    ) -> Result<SandboxRouteInput, Error> {
        request.validate()?;
        if !contract.accepts_request(request) {
            return Err(Error::ProviderContractMismatch);
        }
        if request.invocation_id != invocation.invocation_id
            || request.conversation_id != invocation.conversation_id
        {
            return Err(Error::CapabilityNotAuthorized);
        }
        let (mut tx, state) = crate::commit_lane::acquire(&self.database, attempt)
            .await
            .map_err(lane)?;
        validate_invocation(&mut tx, attempt, state, invocation).await?;
        if material.actor != invocation.actor
            || material.run_context.run_id != attempt.run_id
            || material.run_context.tenant_id != state.tenant_id
            || material.tool.manifest.key != invocation.tool_key
        {
            return Err(Error::CapabilityNotAuthorized);
        }
        let row=sqlx::query("SELECT c.surface,c.trigger,c.policy,s.encoded_snapshot,s.content_hash FROM runtime.runs r JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id AND s.tenant_id=r.tenant_id JOIN runtime.sandbox_routing_contexts c ON c.run_id=r.run_id AND c.tenant_id=r.tenant_id WHERE r.run_id=$1 AND c.principal_id=$2")
            .bind(attempt.run_id.into_uuid()).bind(invocation.actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::SandboxRoutingUnavailable)?;
        let bytes: Vec<u8> = row.try_get("encoded_snapshot").map_err(db)?;
        let hash: Vec<u8> = row.try_get("content_hash").map_err(db)?;
        if Sha256::digest(&bytes).into_bytes().as_slice() != hash.as_slice() {
            return Err(Error::CorruptDurableState);
        }
        let snapshot = self
            .snapshots
            .decode_runtime_snapshot(&bytes)
            .map_err(|_| Error::CorruptDurableState)?;
        snapshot
            .validate_sealed()
            .map_err(|_| Error::CorruptDurableState)?;
        if snapshot.agent.agent_id != material.agent_id
            || snapshot.product_release != material.product_release
        {
            return Err(Error::CapabilityNotAuthorized);
        }
        // Unknown host Skill metadata is an explicit unavailable dependency.
        // Never infer browser compatibility from a Skill's text or capability name.
        let release = snapshot
            .product_release
            .as_ref()
            .and_then(|r| musteragent_skill::ProductSkillRelease::resolve(r).ok());
        let mut declarations = Vec::new();
        for skill in &snapshot.skills {
            if let Some(release) = release
                && release
                    .exact_entry(skill.skill_version_id)
                    .map_err(|_| Error::CorruptDurableState)?
                    .is_some_and(|entry| entry.definition == *skill)
            {
                declarations.extend(release.sandbox_compatibility(&skill.skill_ref));
                continue;
            }
            let declaration:Option<String>=sqlx::query_scalar("SELECT compatibility FROM skill.sandbox_declarations WHERE tenant_id=$1 AND skill_version_id=$2 AND instructions_hash=$3")
                .bind(state.tenant_id.into_uuid()).bind(skill.skill_version_id.into_uuid()).bind(skill.instructions_hash.into_bytes().to_vec()).fetch_optional(&mut *tx).await.map_err(db)?;
            declarations.push(
                SandboxCompatibility::parse(
                    declaration
                        .as_deref()
                        .ok_or(Error::SandboxRoutingUnavailable)?,
                )
                .map_err(|_| Error::CorruptDurableState)?,
            );
        }
        let availability:(i64,bool)=sqlx::query_as("SELECT count(*) FILTER(WHERE executor_kind='browser'),COALESCE(bool_or(executor_kind='server'),false) FROM capability.browser_provider_slots WHERE tenant_id=$1 AND principal_id=$2 AND $3=ANY(capability_keys) AND runtime_release_ref=$4 AND active_lease_id IS NOT NULL AND issued_at_ms<=floor(extract(epoch FROM clock_timestamp())*1000)::bigint AND expires_at_ms>floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .bind(state.tenant_id.into_uuid()).bind(invocation.actor.principal_id.into_uuid()).bind(contract.capability_key().as_str()).bind(request.runtime_release_ref.as_str()).fetch_one(&mut *tx).await.map_err(db)?;
        let surface = match row.try_get::<i16, _>("surface").map_err(db)? {
            1 => SandboxClientSurface::Web,
            2 => SandboxClientSurface::Desktop,
            3 => SandboxClientSurface::Mobile,
            4 => SandboxClientSurface::Api,
            _ => return Err(Error::CorruptDurableState),
        };
        let trigger = match row.try_get::<i16, _>("trigger").map_err(db)? {
            1 => SandboxTrigger::Interactive,
            2 => SandboxTrigger::Background,
            3 => SandboxTrigger::Scheduled,
            _ => return Err(Error::CorruptDurableState),
        };
        let policy = match row.try_get::<i16, _>("policy").map_err(db)? {
            1 => SandboxRoutingPolicy::PreferBrowser,
            2 => SandboxRoutingPolicy::BrowserOnly,
            3 => SandboxRoutingPolicy::ServerOnly,
            _ => return Err(Error::CorruptDurableState),
        };
        tx.commit()
            .await
            .map_err(|_| Error::DurabilityUnavailable)?;
        Ok(SandboxRouteInput {
            surface,
            trigger,
            policy,
            compatibility: SandboxCompatibility::combine(declarations),
            availability: SandboxAvailability {
                online_browser_clients: u32::try_from(availability.0)
                    .map_err(|_| Error::CorruptDurableState)?,
                server_available: availability.1,
            },
        })
    }

    async fn enqueue_inner(
        &self,
        attempt: &AttemptContext,
        invocation: &CapabilityInvocation,
        route: SandboxRouteRecord,
        contract: BrowserProviderContract,
        request: BrowserJobRequest,
    ) -> Result<DurableBrowserJob, Error> {
        route.validate().map_err(|_| Error::CorruptDurableState)?;
        if request.invocation_id != invocation.invocation_id
            || request.conversation_id != invocation.conversation_id
        {
            return Err(Error::CapabilityNotAuthorized);
        }
        let (mut tx, state) = crate::commit_lane::acquire(&self.database, attempt)
            .await
            .map_err(lane)?;
        validate_invocation(&mut tx, attempt, state, invocation).await?;
        let row=sqlx::query("SELECT sandbox_route_hash FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2 FOR UPDATE")
            .bind(state.tenant_id.into_uuid()).bind(invocation.invocation_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        let hash: Option<Vec<u8>> = row.try_get("sandbox_route_hash").map_err(db)?;
        if hash.as_deref()
            != Some(
                Sha256::digest(route.canonical_bytes())
                    .into_bytes()
                    .as_slice(),
            )
        {
            return Err(Error::RouteConflict);
        }
        let gateway = PostgresBrowserGatewayStore::new(self.database.clone());
        let result = gateway
            .enqueue_on(
                &mut tx,
                route.decision.executor,
                state.tenant_id,
                invocation.actor.principal_id,
                contract,
                request,
            )
            .await?;
        // Slot locks can wait. Expiry and cancellation must be rechecked using
        // database time after that wait, before making either effect visible.
        crate::commit_lane::validate_active_fence(&mut tx, attempt, state)
            .await
            .map_err(lane)?;
        tx.commit()
            .await
            .map_err(|_| Error::DurabilityUnavailable)?;
        Ok(result)
    }
}
impl SandboxRoutingStore for PostgresSandboxRoutingStore {
    fn context<'a>(
        &'a self,
        a: &'a AttemptContext,
        m: &'a ToolExecutionMaterial,
        i: &'a CapabilityInvocation,
        c: BrowserProviderContract,
        q: &'a BrowserJobRequest,
    ) -> ApplicationFuture<'a, Result<SandboxRouteInput, Error>> {
        Box::pin(self.context_inner(a, m, i, c, q))
    }
    fn commit_route<'a>(
        &'a self,
        a: &'a AttemptContext,
        c: SandboxRouteCommit,
    ) -> ApplicationFuture<'a, Result<DurableToolInvocation, Error>> {
        Box::pin(self.journal.commit_sandbox_route(a, c))
    }
    fn enqueue<'a>(
        &'a self,
        a: &'a AttemptContext,
        i: &'a CapabilityInvocation,
        r: SandboxRouteRecord,
        c: BrowserProviderContract,
        q: BrowserJobRequest,
    ) -> ApplicationFuture<'a, Result<DurableBrowserJob, Error>> {
        Box::pin(self.enqueue_inner(a, i, r, c, q))
    }
}
async fn require_owner(tx: &mut PgConnection, actor: Actor, run: RunId) -> Result<(), Error> {
    let allowed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.runs r JOIN conversation.participants p ON p.participant_id=r.requested_by_participant_id AND p.conversation_id=r.conversation_id AND p.tenant_id=r.tenant_id LEFT JOIN agent.dialogue_sessions s ON s.conversation_id=r.conversation_id AND s.tenant_id=r.tenant_id WHERE r.run_id=$1 AND r.tenant_id=$2 AND COALESCE(s.owner_id,p.principal_ref)=$3)")
        .bind(run.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_one(tx).await.map_err(db)?;
    if allowed {
        Ok(())
    } else {
        Err(Error::CapabilityNotAuthorized)
    }
}
async fn validate_invocation(
    tx: &mut PgConnection,
    attempt: &AttemptContext,
    state: crate::commit_lane::CommitLaneState,
    i: &CapabilityInvocation,
) -> Result<(), Error> {
    if i.actor.principal_kind != PrincipalKind::User
        || i.actor.tenant_id != i.tenant_id
        || i.tenant_id != state.tenant_id
        || i.run_id != attempt.run_id
        || i.conversation_id != state.conversation_id
        || i.execution_attempt_id != attempt.attempt_id
        || i.execution_attempt_number != attempt.attempt_number
    {
        return Err(Error::CapabilityNotAuthorized);
    }
    crate::commit_lane::validate_active_fence(tx, attempt, state)
        .await
        .map_err(lane)?;
    require_owner(tx, i.actor, i.run_id).await?;
    let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2 AND run_id=$3 AND execution_attempt_id=$4 AND status=3 AND tool_ref=$5 AND capability_provider_id=$6 AND request_content_id=$7 AND request_hash=$8 AND requested_by_attempt_id=$9 AND tool_call_id=$10 AND idempotency_key=$11)")
        .bind(i.tenant_id.into_uuid()).bind(i.invocation_id.into_uuid()).bind(i.run_id.into_uuid()).bind(attempt.attempt_id.into_uuid()).bind(i.tool_key.as_str()).bind(i.capability_provider_id.into_uuid()).bind(i.request_ref.content_id.into_uuid()).bind(i.request_ref.sha256.into_bytes().to_vec()).bind(i.requested_by_attempt_id.into_uuid()).bind(i.tool_call_id.as_str()).bind(i.idempotency_key.as_str()).fetch_one(tx).await.map_err(db)?;
    if valid {
        Ok(())
    } else {
        Err(Error::CapabilityNotAuthorized)
    }
}
fn db(_: sqlx::Error) -> Error {
    Error::DurabilityUnavailable
}
fn lane(e: crate::commit_lane::CommitLaneError) -> Error {
    match e {
        crate::commit_lane::CommitLaneError::StaleFence => Error::StaleLease,
        _ => Error::DurabilityUnavailable,
    }
}
/// Insert the trusted routing context of a newly admitted Run inside its
/// admission transaction. The caller has already authenticated `actor` as the
/// Run's requester.
pub(crate) async fn insert_run_context(
    tx: &mut PgConnection,
    run: RunId,
    actor: Actor,
    routing: SandboxRoutingContext,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO runtime.sandbox_routing_contexts(run_id,tenant_id,principal_id,surface,trigger,policy) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(run.into_uuid())
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(surface_code(routing.surface))
        .bind(trigger_code(routing.trigger))
        .bind(policy_code(routing.policy))
        .execute(&mut *tx)
        .await
        .map(|_| ())
}

/// Copy a source Run's routing context to a derived Run (retry or delegated
/// child) in the derived Run's admission transaction. Only a context owned by
/// the same tenant and principal is inherited; returns whether one was copied.
pub(crate) async fn inherit_run_context(
    tx: &mut PgConnection,
    run: RunId,
    source: RunId,
    actor: Actor,
) -> Result<bool, sqlx::Error> {
    let copied = sqlx::query("INSERT INTO runtime.sandbox_routing_contexts(run_id,tenant_id,principal_id,surface,trigger,policy) SELECT $1,tenant_id,principal_id,surface,trigger,policy FROM runtime.sandbox_routing_contexts WHERE run_id=$2 AND tenant_id=$3 AND principal_id=$4")
        .bind(run.into_uuid())
        .bind(source.into_uuid())
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .execute(&mut *tx)
        .await?;
    Ok(copied.rows_affected() == 1)
}

pub(crate) fn surface_code(s: SandboxClientSurface) -> i16 {
    match s {
        SandboxClientSurface::Web => 1,
        SandboxClientSurface::Desktop => 2,
        SandboxClientSurface::Mobile => 3,
        SandboxClientSurface::Api => 4,
    }
}
pub(crate) fn trigger_code(s: SandboxTrigger) -> i16 {
    match s {
        SandboxTrigger::Interactive => 1,
        SandboxTrigger::Background => 2,
        SandboxTrigger::Scheduled => 3,
    }
}
pub(crate) fn policy_code(s: SandboxRoutingPolicy) -> i16 {
    match s {
        SandboxRoutingPolicy::PreferBrowser => 1,
        SandboxRoutingPolicy::BrowserOnly => 2,
        SandboxRoutingPolicy::ServerOnly => 3,
    }
}
