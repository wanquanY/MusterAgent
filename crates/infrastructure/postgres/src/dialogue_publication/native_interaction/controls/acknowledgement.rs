use super::*;
use musteragent_runtime_contract::{CommandDisposition, ResourceRef};
impl PostgresNativeDialogueInteractionStore {
    pub(in crate::dialogue_publication::native_interaction) async fn acknowledge_inner(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        command: &NativeDialogueInteractionCommand,
        p: &NativeDialogueInteractionProjection,
        outcome: Result<CommandAck, ErrorCode>,
    ) -> Result<NativeDialogueInteractionAcknowledgement, RuntimeError> {
        p.validate().map_err(fail)?;
        if command.interaction_id != p.interaction.interaction_id
            || command.admission != p.admission
        {
            return Err(fail(ErrorCode::PolicyDenied));
        }
        if let Ok(ack) = &outcome {
            validate_outcome(command, p, ack)?;
        }
        let proof = self
            .stage_qualified_projection(actor, target, p, true)
            .await?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(database)?;
        source::lock(&mut tx, &p.admission)
            .await
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        let s = self.publication_source(&mut tx, actor, &proof, p).await?;
        let row = command_row(&mut tx, actor, command.command_id, true)
            .await?
            .ok_or_else(|| fail(ErrorCode::InteractionNotFound))?;
        let bytes = native_codec::command(command).encode_to_vec();
        if row
            .try_get::<Vec<u8>, _>("command_bytes")
            .map_err(database)?
            != bytes
            || row
                .try_get::<Vec<u8>, _>("command_sha256")
                .map_err(database)?
                != Sha256::digest(&bytes).into_bytes()
            || row_id::<DialogueEntryId>(&row, "entry_id")? != command.target.entry_id
        {
            return Err(fail(ErrorCode::IdempotencyConflict));
        }
        let prior = control_receipt(&row)?;
        // Lost-ACK recovery retains the first immutable receipt. The original
        // ledger may label a replay Duplicate while preserving the committed facts.
        if !matches!(prior.delivery, DialogueInteractionControlDelivery::Queued) {
            let matches = match (&prior.delivery, &outcome) {
                (DialogueInteractionControlDelivery::Applied(existing), Ok(incoming)) => {
                    let mut existing = existing.clone();
                    let mut incoming = incoming.clone();
                    existing.disposition = CommandDisposition::Committed;
                    incoming.disposition = CommandDisposition::Committed;
                    existing == incoming
                }
                (DialogueInteractionControlDelivery::Rejected(existing), Err(incoming)) => {
                    existing == incoming
                }
                _ => false,
            };
            if !matches {
                return Err(fail(ErrorCode::IdempotencyConflict));
            }
            let current = sqlx::query("SELECT * FROM agent.native_dialogue_interaction_projections WHERE interaction_id=$1")
                .bind(command.interaction_id.into_uuid()).fetch_one(&mut *tx).await.map_err(database)?;
            let publication = receipt(&mut tx, &row_projection(&current)?).await?;
            return Ok(NativeDialogueInteractionAcknowledgement {
                publication,
                control: prior,
            });
        }
        let publication = self.observe(&mut tx, &s, p).await?;
        if publication.entry_id != command.target.entry_id
            || publication.dialogue_id != command.target.dialogue_id
        {
            return Err(fail(ErrorCode::PolicyDenied));
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(database)?;
        let (kind, bytes, rejected, delivery) = match outcome {
            Ok(ack) => {
                // committed_at belongs to the original Device ledger clock.
                // Cloud records its own receipt time; comparing these clocks
                // cannot prove authority and would strand a committed response.
                (
                    "applied",
                    Some(native_codec::command_ack(&ack).encode_to_vec()),
                    None,
                    DialogueInteractionControlDelivery::Applied(ack),
                )
            }
            Err(code) => (
                "rejected",
                None,
                Some(code.as_str()),
                DialogueInteractionControlDelivery::Rejected(code),
            ),
        };
        sqlx::query("UPDATE agent.native_dialogue_interaction_commands SET outcome=$4,owner_ack=$5,rejected_code=$6,acknowledged_at_ms=$7 WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3 AND outcome IS NULL")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(command.command_id.into_uuid())
            .bind(kind).bind(bytes).bind(rejected).bind(now.unix_millis()).execute(&mut *tx).await.map_err(database)?;
        target
            .require_current_at(
                crate::ledger::database_now_ms(&mut tx)
                    .await
                    .map_err(database)?,
            )
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        tx.commit().await.map_err(database)?;
        Ok(NativeDialogueInteractionAcknowledgement {
            publication,
            control: DialogueInteractionControlReceipt {
                command_id: command.command_id,
                delivery,
            },
        })
    }
}
fn validate_outcome(
    command: &NativeDialogueInteractionCommand,
    p: &NativeDialogueInteractionProjection,
    ack: &CommandAck,
) -> Result<(), RuntimeError> {
    ack.validate().map_err(corrupt)?;
    if ack.command_id != command.command_id
        || !ack
            .resource_refs
            .contains(&ResourceRef::Interaction(command.interaction_id))
        || p.control_open
        || ack.first_conversation_seq.is_none()
    {
        return Err(fail(ErrorCode::InvalidCommand));
    }
    let matched = match (&command.response, p.interaction.status, &p.resolution) {
        (
            DialogueInteractionControlResponse::Answer(expected),
            InteractionStatus::Responded,
            Some(DialogueInteractionResolution::Answer { bytes, .. }),
        ) => bytes == expected,
        (
            DialogueInteractionControlResponse::ApproveOnce,
            InteractionStatus::Responded,
            Some(DialogueInteractionResolution::Approved),
        ) => true,
        (
            DialogueInteractionControlResponse::Deny,
            InteractionStatus::Responded,
            Some(DialogueInteractionResolution::Denied),
        ) => true,
        (DialogueInteractionControlResponse::Cancel, InteractionStatus::Cancelled, None) => true,
        (
            DialogueInteractionControlResponse::CompleteCapability(bytes),
            InteractionStatus::Responded,
            None,
        ) => {
            let request =
                musteragent_runtime_interaction::CapabilityRequest::parse(&p.prompt_bytes);
            let descriptor = serde_json::from_slice::<
                musteragent_runtime_interaction::CapabilityDescriptor,
            >(bytes);
            request
                .ok()
                .zip(descriptor.ok())
                .is_some_and(|(request, descriptor)| {
                    request.capability == descriptor
                        && request.response_schema().ok().as_ref()
                            == Some(&p.interaction.response_schema)
                })
        }
        _ => false,
    };
    if !matched {
        return Err(fail(ErrorCode::InteractionResponseInvalid));
    }
    Ok(())
}
