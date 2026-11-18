//! Canonical Runtime failures have their own source authority. A service error
//! string, invalid signature or network failure cannot construct this request.
use super::termination::*;
use super::*;
use musteragent_content_ref::MediaType;
use musteragent_execution_context::DialogueOutputContract;
use musteragent_persistence_codec::dialogue_admission_codec;
use musteragent_runtime_content::PutContent;
use musteragent_runtime_session::DeviceIntentClaim;

type Error = DialogueStoreError;
type State = DialogueAdmissionTermination;
impl PostgresDialogueIntentHandoffStore {
    pub(super) async fn fail_local_inner(
        &self,
        claim: DialogueIntentClaim,
        failure: LocalAdmissionFailure,
    ) -> Result<State, Error> {
        self.fail_local_at_destination(claim, failure, None).await
    }

    /// Only the server-side source capture owner calls this after a canonical
    /// preparation failure. No RPC request supplies a failure classification.
    pub(super) async fn fail_device_preparation_corrupt(
        &self,
        requested: DeviceIntentClaim,
    ) -> Result<State, Error> {
        self.fail_local_at_destination(
            requested.claim,
            LocalAdmissionFailure::preparation_corrupt(),
            Some(requested),
        )
        .await
    }

    async fn fail_local_at_destination(
        &self,
        claim: DialogueIntentClaim,
        failure: LocalAdmissionFailure,
        device: Option<DeviceIntentClaim>,
    ) -> Result<State, Error> {
        let (_, content) = self
            .rejection_authority
            .as_ref()
            .ok_or(Error::Infrastructure)?;
        let reference = content
            .put(PutContent {
                tenant_id: claim.actor.tenant_id,
                media_type: MediaType::new("text/plain").map_err(|_| Error::Corrupt)?,
                bytes: failure.message().as_bytes().to_vec(),
            })
            .await
            .map_err(|_| Error::Infrastructure)?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Admission)
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Acquire, error))?;
        super::progress::lock_account(&mut tx, claim.actor).await?;
        lock_input(&mut tx, claim).await?;
        // Destination is immutable acceptance custody and remains checkable on
        // replay after the original lease is released. Cloud callers cannot
        // settle a Device source and another installation cannot replay it.
        let target = sqlx::query("SELECT execution_target,target_device_id,target_installation_id FROM agent.dialogue_intent_admissions WHERE intent_id=$1")
            .bind(claim.intent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        let destination_matches = match device {
            None => target.try_get::<&str, _>("execution_target").map_err(db)? == "cloud",
            Some(requested) => {
                target.try_get::<&str, _>("execution_target").map_err(db)? == "origin_device"
                    && target
                        .try_get::<Option<uuid::Uuid>, _>("target_device_id")
                        .map_err(db)?
                        == Some(requested.device_id.into_uuid())
                    && target
                        .try_get::<Option<uuid::Uuid>, _>("target_installation_id")
                        .map_err(db)?
                        == Some(requested.installation_id.into_uuid())
            }
        };
        if !destination_matches {
            return Err(Error::Unavailable);
        }
        if let Some((status, code)) = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT outcome,internal_failure_code FROM agent.dialogue_admission_outcomes WHERE intent_id=$1",
        )
        .bind(claim.intent_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        {
            return if status == "failed" && code.as_deref() == Some(failure.kind().as_str()) {
                Ok(State::Replayed)
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        // Once Device source custody exists, a signer/local admission may have
        // observed it even if its ACK was lost. This pre-Run owner cannot revoke
        // or overwrite that frozen source; its original recovery owner applies.
        let native_custody: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent.device_admissions WHERE intent_id=$1)",
        )
        .bind(claim.intent_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if native_custody || admitted(&mut tx, claim.intent_id).await? {
            return Ok(State::AlreadyAdmitted);
        }
        let live_claim = match device {
            None => lock_claim(&mut tx, claim).await?,
            Some(requested) => super::device::lock_device_claim(&mut tx, requested).await?,
        };
        if !live_claim {
            return Ok(State::LeaseLost);
        }
        let (agent_id,dialogue_id):(uuid::Uuid,uuid::Uuid)=sqlx::query_as("SELECT d.agent_id,d.dialogue_id FROM agent.dialogue_user_intents i JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id WHERE i.intent_id=$1").bind(claim.intent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        let agent = AgentId::from_uuid(agent_id).map_err(|_| Error::Corrupt)?;
        let dialogue = AgentDialogueId::from_uuid(dialogue_id).map_err(|_| Error::Corrupt)?;
        lock_space(&mut tx, claim, agent).await?;
        sqlx::query(
            "SELECT dialogue_id FROM agent.agent_dialogues WHERE dialogue_id=$1 FOR UPDATE",
        )
        .bind(dialogue_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let status: String =
            sqlx::query_scalar("SELECT status FROM agent.owned_agents WHERE agent_id=$1 FOR SHARE")
                .bind(agent_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if status != "active" {
            return Ok(State::StalePreparation);
        }
        let current = match device {
            None => {
                crate::dialogue_execution::load_preparation(
                    &mut tx,
                    claim.actor,
                    claim.intent_id,
                    true,
                )
                .await
            }
            Some(_) => {
                crate::dialogue_execution::load_preparation_source(
                    &mut tx,
                    claim.actor,
                    claim.intent_id,
                    true,
                    DialogueOutputContract::CURRENT,
                )
                .await
            }
        };
        let facts = match (failure.preparation(), current) {
            (Some(expected), Ok(current)) if *expected == current => {
                Some(dialogue_admission_codec::facts(&current.grant_fact_binding()?).facts_sha256)
            }
            (None, Err(Error::Corrupt))
                if failure.kind() == LocalFailureKind::PreparationCorrupt =>
            {
                None
            }
            (_, Err(Error::Infrastructure)) => return Err(Error::Infrastructure),
            _ => return Ok(State::StalePreparation),
        };
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        if now >= claim.lease_expires_at {
            return Ok(State::LeaseLost);
        }
        let entry = insert_message(&mut tx, claim, agent, dialogue, &reference, now).await?;
        sqlx::query("INSERT INTO agent.dialogue_admission_outcomes(intent_id,tenant_id,owner_id,agent_id,dialogue_id,outcome,reason,claim_generation,facts_sha256,entry_id,created_at_ms,internal_failure_code) VALUES($1,$2,$3,$4,$5,'failed','runtime_contract_failure',$6,$7,$8,$9,$10)")
            .bind(claim.intent_id.into_uuid()).bind(claim.actor.tenant_id.into_uuid()).bind(claim.actor.principal_id.into_uuid()).bind(agent_id).bind(dialogue_id).bind(integer(claim.generation)?).bind(facts).bind(entry.entry_id.into_uuid()).bind(now.unix_millis()).bind(failure.kind().as_str()).execute(&mut *tx).await.map_err(db)?;
        if !finish(&mut tx, claim, "failed", now).await? {
            return Ok(State::LeaseLost);
        }
        // The original progress owner projects the actual pre-Run failure; it
        // must never claim an execution was admitted merely to stop a spinner.
        if failure.kind() == LocalFailureKind::PreparationCorrupt {
            super::progress::fail_before_admission(&mut tx, claim.actor, claim.intent_id).await?;
        }
        tx.commit()
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
        Ok(State::Committed)
    }
}
