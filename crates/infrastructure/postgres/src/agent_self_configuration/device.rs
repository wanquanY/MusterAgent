//! Native retains the original SQLite Tool. Only account configuration commits
//! here; no Cloud Run, surrogate Tool, or local configuration mirror is created.
use super::*;
use crate::agent_resource::device_custody::{self, DeviceCustody};
use musteragent_persistence_codec::{execution_grant_decode, wire};
use prost::Message;
#[derive(Clone)]
pub struct PostgresAgentSelfDeviceConfiguration {
    database: PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
}
impl PostgresAgentSelfDeviceConfiguration {
    pub fn new(
        database: impl Into<PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
    ) -> Self {
        Self {
            database: database.into(),
            content,
        }
    }
    async fn execute_inner(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        request: &AgentSelfDeviceRequest,
        lookup: bool,
    ) -> Result<Option<AgentSelfDeviceReceipt>, Error> {
        request.validate()?;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(|_| Error::Infrastructure)?;
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        if now >= request.attempt.lease_expires_at {
            return Err(Error::StaleFence);
        }
        let DeviceCustody::Intent(prepared) =
            device_custody::load(&mut tx, actor, target, &request.admission, None, true)
                .await
                .map_err(|_| Error::Forbidden)?
        else {
            return Err(Error::Forbidden);
        };
        validate_grant(&mut tx, request, &prepared).await?;
        // Match the same space -> Agent -> configuration lock order as Settings.
        sqlx::query("SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 FOR UPDATE OF s").bind(prepared.scope.agent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(|_|Error::Infrastructure)?.ok_or(Error::Forbidden)?;
        device_custody::current_agent(
            &mut tx,
            actor,
            prepared.scope.agent_id,
            prepared.scope.dialogue_id,
            prepared.configuration.permission_revision.get(),
            prepared.configuration.permission_mode,
        )
        .await
        .map_err(map_store)?;
        let origin=crate::dialogue_codec::intent_row(&sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3").bind(request.admission.intent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(|_|Error::Infrastructure)?).map_err(map_store)?.origin;
        let bytes = if request.tool_key == "agents.self.inspect@1" {
            // An inspection has no mutation to replay; receipt recovery reads current settings.
            let config = crate::dialogue_codec::config_row(
                &sqlx::query("SELECT * FROM agent.owned_agent_configurations WHERE agent_id=$1")
                    .bind(prepared.scope.agent_id.into_uuid())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|_| Error::Infrastructure)?,
            )
            .map_err(map_store)?;
            let instructions = match &config.instructions_ref {
                Some(reference) => {
                    let bytes = self
                        .content
                        .read(actor.tenant_id, reference)
                        .await
                        .map_err(|_| Error::Infrastructure)?;
                    verify_content(reference, &bytes).map_err(|_| Error::Corrupt)?;
                    String::from_utf8(bytes).map_err(|_| Error::Corrupt)?
                }
                None => String::new(),
            };
            serde_json::to_vec(&serde_json::json!({"display_name":config.profile.display_name,"profile_revision":config.profile.revision.get(),"instructions":instructions,"instructions_revision":config.instructions_revision.get()})).map_err(|_|Error::Corrupt)?
        } else {
            let change = AgentSelfChange::parse(&request.request_bytes)?;
            let update = ConfigureOwnedAgent {
                actor,
                command_id: command_id(request.invocation_id)?,
                agent_id: prepared.scope.agent_id,
                expected_revision: NonZeroU64::new(change.expected_revision())
                    .ok_or(Error::InvalidArguments)?,
                change: prepare_change(
                    self.content.as_ref(),
                    actor,
                    request.invocation_id,
                    &change,
                )
                .await?,
                origin,
                changed_at: now,
            };
            if lookup {
                if crate::dialogue_codec::command_receipt(
                    &mut tx,
                    actor,
                    update.command_id,
                    "configure_agent",
                    update.fingerprint(),
                )
                .await
                .map_err(map_store)?
                .is_none()
                {
                    return Ok(None);
                }
            } else {
                crate::owned_agent::configure_in_transaction(&mut tx, &update)
                    .await
                    .map_err(map_store)?;
            }
            change.result_bytes()
        };
        let input = PutContent {
            tenant_id: actor.tenant_id,
            media_type: MediaType::new("application/json").map_err(|_| Error::Corrupt)?,
            bytes: bytes.clone(),
        };
        let reference = if request.tool_key == "agents.self.inspect@1" {
            self.content.put(input).await
        } else {
            self.content
                .put_at(
                    ContentId::from_uuid(uuid::Uuid::new_v5(
                        &request.invocation_id.into_uuid(),
                        b"agent-self-configuration-result@1",
                    ))
                    .map_err(|_| Error::Corrupt)?,
                    input,
                )
                .await
        }
        .map_err(|_| Error::Infrastructure)?;
        let current = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        target
            .require_current_at(current)
            .map_err(|_| Error::Forbidden)?;
        if current >= request.attempt.lease_expires_at {
            return Err(Error::StaleFence);
        }
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        let receipt = AgentSelfDeviceReceipt {
            invocation_id: request.invocation_id,
            request_hash: request.request_ref.sha256,
            result_ref: reference,
            result_bytes: bytes,
        };
        receipt.validate(request)?;
        Ok(Some(receipt))
    }
}
impl AgentSelfDeviceStore for PostgresAgentSelfDeviceConfiguration {
    fn execute<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a AgentSelfDeviceRequest,
        lookup: bool,
    ) -> ApplicationFuture<'a, Result<Option<AgentSelfDeviceReceipt>, Error>> {
        Box::pin(self.execute_inner(actor, target, request, lookup))
    }
}
async fn validate_grant(
    tx: &mut sqlx::PgConnection,
    request: &AgentSelfDeviceRequest,
    prepared: &DialogueExecutionPreparation,
) -> Result<(), Error> {
    let bytes:Vec<u8>=sqlx::query_scalar("SELECT v.signed_grant_proof FROM agent.device_grant_heads h JOIN agent.device_grant_versions v ON v.intent_id=h.intent_id AND v.revision=h.admitted_revision WHERE h.intent_id=$1 AND v.signed_grant_hash=$2").bind(request.admission.intent_id.into_uuid()).bind(request.admission.grant_proof_hash.into_bytes().to_vec()).fetch_one(&mut *tx).await.map_err(|_|Error::Infrastructure)?;
    if bytes.len() > 4 * 1024 * 1024 || Sha256::digest(&bytes) != request.admission.grant_proof_hash
    {
        return Err(Error::Corrupt);
    }
    let signed = execution_grant_decode::signed_execution_grant(Some(
        wire::SignedExecutionGrant::decode(bytes.as_slice()).map_err(|_| Error::Corrupt)?,
    ))
    .map_err(|_| Error::Corrupt)?;
    validate_device_grant(prepared, request.admission.device_id, &signed.claims)
        .map_err(|_| Error::Forbidden)?;
    let key = musteragent_tool::ToolKey::from_versioned_key(
        VersionedKey::new(&request.tool_key).map_err(|_| Error::InvalidArguments)?,
    );
    let def = musteragent_tool::BuiltinToolCatalog::resolve_historical_contract(
        signed.claims.product_release.as_ref(),
        musteragent_identity::ProductId::KERNEL,
        musteragent_tool::ProductExecutionTarget::Cloud,
        &key,
        &request.request_schema,
        &request.result_schema,
    )
    .map_err(|_| Error::Forbidden)?;
    if def.input_schema != request.request_schema
        || def.output_schema != request.result_schema
        || !signed.claims.capability_entitlements.iter().any(|g| {
            g.provider_id.to_string() == musteragent_tool::BUILTIN_RUST_PROVIDER_ID
                && g.key.as_str() == def.capability_key.as_str()
                && g.location == musteragent_capability::CapabilityLocation::Device
                && g.effect == def.effect
                && def.required_scopes.is_subset(&g.allowed_scopes)
                && g.policy_decision != musteragent_policy::Decision::Deny
        })
        || signed
            .claims
            .tool_contract_releases
            .iter()
            .any(|r| r.tool_key.as_str() == request.tool_key && !r.available)
    {
        return Err(Error::Forbidden);
    }
    Ok(())
}
