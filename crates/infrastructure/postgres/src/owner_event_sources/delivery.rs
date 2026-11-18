use super::*;
use musteragent_runtime_automation::{AcceptStandingGoalInput, StandingGoalInputOutcome};
use std::num::NonZeroU64;

impl PostgresOwnerEventSourceStore {
    pub(super) async fn deliver_inner(
        &self,
        request: DeliverDialogueSourceEvent,
    ) -> Result<OwnerSourceInputReceipt, Error> {
        let identity = &request.identity;
        let observation = codec::observation(&request.observation);
        codec::decode_observation(observation.clone()).map_err(|_| Error::Corrupt)?;
        let hash = fingerprint(
            identity,
            request.goal_id,
            request.subscription_id,
            &[
                request.activation_epoch.get().to_be_bytes().to_vec(),
                observation.encode_to_vec(),
                request.content.content_id.to_string().into_bytes(),
                request.content.sha256.into_bytes().to_vec(),
                request.content.byte_length.to_be_bytes().to_vec(),
                request.content.media_type.as_str().as_bytes().to_vec(),
            ],
        );
        let mut tx = self.write().await?;
        if let Some(receipt) = replay(&mut tx, identity, DELIVER, hash).await? {
            let wire::owner_command_receipt::Result::SourceInput(value) = receipt else {
                return Err(Error::Corrupt);
            };
            let receipt = codec::decode_input_receipt(value).map_err(|_| Error::Corrupt)?;
            check_receipt(&receipt, &request)?;
            tx.commit().await.map_err(|_| Error::Infrastructure)?;
            return Ok(receipt);
        }
        // Account lock serializes current App authority and Source controls;
        // the original Goal lock serializes its accepted sequence and rules.
        let (space, goal) = record::locked_scope(&mut tx, identity, request.goal_id, true).await?;
        if let Some(row) = sqlx::query("SELECT payload,evidence_sha256,input_sequence FROM agent.owner_source_input_receipts WHERE tenant_id=$1 AND owner_id=$2 AND goal_id=$3 AND event_id=$4")
            .bind(identity.actor.tenant_id.into_uuid()).bind(identity.actor.principal_id.into_uuid()).bind(request.goal_id.into_uuid())
            .bind(request.observation.event_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)? {
            let bytes: Vec<u8> = row.try_get("payload").map_err(db)?;
            let receipt = codec::decode_input_receipt(wire::OwnerSourceInputReceipt::decode(bytes.as_slice()).map_err(|_| Error::Corrupt)?).map_err(|_| Error::Corrupt)?;
            if receipt.input_sequence.get() as i64 != row.try_get::<i64,_>("input_sequence").map_err(db)?
                || receipt.evidence_digest.into_bytes().as_slice() != row.try_get::<Vec<u8>,_>("evidence_sha256").map_err(db)? {
                return Err(Error::Corrupt);
            }
            check_receipt(&receipt, &request)?;
            save_input_command(&mut tx, identity, hash, &receipt).await?;
            tx.commit().await.map_err(|_| Error::Infrastructure)?;
            return Ok(receipt);
        }
        super::authority::scope(&mut tx, identity.actor, identity.agent_id, true, false).await?;
        if goal.status != StandingGoalStatus::Active {
            return Err(Error::NotReady);
        }
        let sources = record::list(&mut tx, &goal).await?;
        let primary = sources
            .iter()
            .find(|source| source.subscription_id == request.subscription_id)
            .ok_or(Error::NotFound)?;
        let mut applications = record::ApplicationReadSet::new();
        let binding = record::current_application(&mut tx, &mut applications, primary).await?;
        if primary.check_observation(request.activation_epoch, &request.observation, binding)?
            != SourceInputEligibility::Eligible
        {
            return Err(Error::Conflict);
        }
        let mut matches = Vec::new();
        for source in &sources {
            if source.desired != EventSubscriptionIntent::Watching {
                if source.required && source.desired != EventSubscriptionIntent::Ended {
                    return Err(Error::NotReady);
                }
                continue;
            }
            let binding = record::current_application(&mut tx, &mut applications, source).await;
            if let Err(error) = &binding
                && !matches!(error, Error::Forbidden | Error::NotFound)
            {
                return Err(error.clone());
            }
            let authority = binding
                .as_ref()
                .map(|binding| source.authorize(binding))
                .ok();
            let ready = source.boundary.is_some()
                && source
                    .source_status
                    .as_ref()
                    .is_some_and(|status| status.readiness == SourceReadiness::Ready)
                && matches!(authority, Some(Ok(())));
            if source.required && source.desired != EventSubscriptionIntent::Ended && !ready {
                return Err(Error::NotReady);
            }
            let binding = match binding {
                Ok(value) => value,
                Err(Error::Forbidden | Error::NotFound) => continue,
                Err(error) => return Err(error),
            };
            if ready
                && source.check_observation(
                    source.guard.activation_epoch,
                    &request.observation,
                    binding,
                ) == Ok(SourceInputEligibility::Eligible)
            {
                matches.push(SourceInputMatch {
                    subscription_id: source.subscription_id,
                    guard: source.guard,
                });
            }
        }
        if matches.is_empty() {
            return Err(Error::Conflict);
        }
        let input = AcceptStandingGoalInput {
            tenant_id: identity.actor.tenant_id,
            principal_id: identity.actor.principal_id,
            goal_id: goal.goal_id,
            expected_goal_revision: goal.revision,
            input_key: format!("source-event:{}", request.observation.event_id),
            content: request.content,
        };
        let sequence = match crate::standing_goal::input::accept_input_on(&mut tx, input)
            .await
            .map_err(|_| Error::Infrastructure)?
        {
            StandingGoalInputOutcome::Accepted { sequence } => sequence,
            // This input namespace belongs solely to this source owner. An
            // input without its admission receipt would violate atomicity.
            StandingGoalInputOutcome::Existing { .. } => return Err(Error::Corrupt),
            StandingGoalInputOutcome::Rejected => return Err(Error::Conflict),
        };
        let receipt = OwnerSourceInputReceipt {
            source_observation: Some(request.observation.clone()),
            goal_id: goal.goal_id,
            event_id: request.observation.event_id,
            evidence_digest: request.observation.evidence_digest,
            input_sequence: NonZeroU64::new(sequence).ok_or(Error::Corrupt)?,
            matches,
            source_object: request.observation.source_object.clone(),
        };
        let payload = codec::input_receipt(&receipt)
            .map_err(|_| Error::Corrupt)?
            .encode_to_vec();
        sqlx::query("INSERT INTO agent.owner_source_input_receipts(tenant_id,owner_id,space_id,goal_id,event_id,evidence_sha256,input_sequence,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(goal.tenant_id.into_uuid()).bind(goal.owner.into_uuid()).bind(space).bind(goal.goal_id.into_uuid()).bind(receipt.event_id.into_uuid())
            .bind(receipt.evidence_digest.into_bytes().as_slice()).bind(sequence as i64).bind(payload).execute(&mut *tx).await.map_err(db)?;
        save_input_command(&mut tx, identity, hash, &receipt).await?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        Ok(receipt)
    }
}

fn check_receipt(
    receipt: &OwnerSourceInputReceipt,
    request: &DeliverDialogueSourceEvent,
) -> Result<(), Error> {
    if receipt.goal_id != request.goal_id || receipt.event_id != request.observation.event_id {
        return Err(Error::Corrupt);
    }
    if receipt.evidence_digest != request.observation.evidence_digest
        || receipt.source_object != request.observation.source_object
        || receipt
            .source_observation
            .as_ref()
            .is_some_and(|original| original != &request.observation)
    {
        return Err(Error::IdempotencyConflict);
    }
    Ok(())
}
async fn save_input_command(
    tx: &mut PgConnection,
    identity: &GoalSourceCommandIdentity,
    hash: Sha256,
    receipt: &OwnerSourceInputReceipt,
) -> Result<(), Error> {
    save_receipt(
        tx,
        identity,
        DELIVER,
        hash,
        wire::owner_command_receipt::Result::SourceInput(
            codec::input_receipt(receipt).map_err(|_| Error::Corrupt)?,
        ),
    )
    .await
}
