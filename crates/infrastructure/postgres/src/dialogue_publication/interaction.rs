//! Called only inside original Interaction Owner commits, after its native
//! semantic ledger and execution-fence validation. No model/log scraping.
use super::*;
use musteragent_approval::InteractionKind;
use musteragent_content_ref::Sha256;
use musteragent_runtime_interaction::RequestInteractionCommit;

/// Original Memory terminal commits update their existing approval card, never
/// insert a status bubble. Called under the same Run transaction/fence.
pub(crate) async fn memory_tool_changed(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    invocation: musteragent_identity::InvocationId,
) -> Result<(), crate::PostgresInteractionError> {
    let row = sqlx::query("SELECT t.interaction_id,t.run_id FROM tool.tool_invocations t
        JOIN approval.interactions i ON i.interaction_id=t.interaction_id AND i.tenant_id=t.tenant_id
          AND i.run_id=t.run_id AND i.requested_by_invocation_id=t.invocation_id AND i.kind=1
        WHERE t.tenant_id=$1 AND t.invocation_id=$2 AND t.tool_ref IN ('user_memory.remember@2','experience.publish@1')")
        .bind(tenant.into_uuid()).bind(invocation.into_uuid()).fetch_optional(&mut *tx).await?;
    if let Some(row) = row {
        changed(
            tx,
            tenant,
            InteractionId::from_uuid(row.try_get("interaction_id")?)
                .map_err(|_| crate::PostgresInteractionError::Corrupt)?,
            RunId::from_uuid(row.try_get("run_id")?)
                .map_err(|_| crate::PostgresInteractionError::Corrupt)?,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn requested(
    tx: &mut sqlx::PgConnection,
    command: &RequestInteractionCommit,
) -> Result<(), crate::PostgresInteractionError> {
    let dialogue:bool=sqlx::query_scalar("SELECT purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report') FROM conversation.conversations WHERE tenant_id=$1 AND conversation_id=$2")
        .bind(command.interaction.tenant_id.into_uuid()).bind(command.interaction.conversation_id.into_uuid())
        .fetch_one(&mut *tx).await?;
    if !dialogue {
        return Ok(());
    }
    let source = load_source(tx, command.interaction.run_id)
        .await
        .map_err(map_error)?;
    let now = crate::ledger::database_now_ms(&mut *tx).await?;
    lock_and_validate_fence(tx, &command.fence, command.expected_last_run_sequence, now).await?;
    let i = &command.interaction;
    let mut bytes = b"musteragent.interaction-owner-publication.v1\0".to_vec();
    bytes.extend_from_slice(i.interaction_id.into_uuid().as_bytes());
    bytes.extend_from_slice(i.run_id.into_uuid().as_bytes());
    bytes.extend_from_slice(&i.prompt_ref.sha256.into_bytes());
    bytes.extend_from_slice(&i.response_schema.schema_hash.into_bytes());
    let fingerprint = Sha256::digest(&bytes);
    let id = CommandId::from_uuid(
        uuid::Builder::from_sha1_bytes(
            fingerprint.into_bytes()[..16]
                .try_into()
                .expect("hash prefix"),
        )
        .into_uuid(),
    )
    .map_err(|_| crate::PostgresInteractionError::Corrupt)?;
    publish_in_transaction(
        tx,
        &source,
        &Delivery {
            delivery_id: id,
            run_id: i.run_id,
            purpose: if i.kind == InteractionKind::Clarification {
                DialoguePublicationPurpose::Clarification
            } else {
                DialoguePublicationPurpose::RequiredDecision
            },
            content: PublicDeliveryContent::Interaction(i.interaction_id),
            fingerprint,
            source: PublicationSource::InteractionOwner(i.interaction_id),
        },
    )
    .await
    .map_err(map_error)?;
    let now = crate::ledger::database_now_ms(&mut *tx).await?;
    lock_and_validate_fence(tx, &command.fence, command.expected_last_run_sequence, now).await?;
    Ok(())
}

/// State lives in approval.interactions; only the existing card revision and
/// account change stream advance. Historical content/cursors remain immutable.
pub(crate) async fn changed(
    tx: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    interaction_id: InteractionId,
    run_id: RunId,
) -> Result<(), crate::PostgresInteractionError> {
    let row=sqlx::query("SELECT e.entry_id,e.dialogue_id,e.owner_id,d.agent_id FROM agent.agent_dialogue_entries e JOIN agent.agent_dialogues d USING(dialogue_id) JOIN agent.dialogue_publications p USING(entry_id) WHERE e.tenant_id=$1 AND e.interaction_id=$2 AND p.source_run_id=$3")
        .bind(tenant_id.into_uuid()).bind(interaction_id.into_uuid()).bind(run_id.into_uuid())
        .fetch_optional(&mut *tx).await?;
    let Some(row) = row else { return Ok(()) };
    let actor = Actor {
        tenant_id,
        principal_id: PrincipalId::from_uuid(row.try_get("owner_id")?)
            .map_err(|_| crate::PostgresInteractionError::Corrupt)?,
        principal_kind: PrincipalKind::User,
    };
    let locked:Option<uuid::Uuid>=sqlx::query_scalar("SELECT space_id FROM agent.owner_spaces WHERE tenant_id=$1 AND owner_id=$2 AND product='musteragent' FOR UPDATE")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await?;
    if locked.is_none() {
        return Err(crate::PostgresInteractionError::ActorForbidden);
    }
    let entry_row=sqlx::query("UPDATE agent.agent_dialogue_entries SET visible_revision=visible_revision+1 WHERE entry_id=$1 AND tenant_id=$2 AND kind='interaction' RETURNING *")
        .bind(row.try_get::<uuid::Uuid,_>("entry_id")?).bind(actor.tenant_id.into_uuid()).fetch_one(&mut *tx).await?;
    let entry = crate::agent_dialogue::entry_row(&entry_row)
        .map_err(|_| crate::PostgresInteractionError::Corrupt)?;
    let now = crate::ledger::database_now_ms(&mut *tx).await?;
    outbox(
        tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "dialogue_entry",
            aggregate: entry.entry_id.into_uuid(),
            cursor: DialogueCursor {
                epoch: 1,
                seq: entry.visible_revision.get(),
            },
            event: "public_entry_changed",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::PublicEntryChanged(
                    codec::entry(&entry),
                )),
            },
            at: now,
        },
    )
    .await
    .map_err(|_| crate::PostgresInteractionError::Corrupt)?;
    Ok(())
}
fn map_error(error: Error) -> crate::PostgresInteractionError {
    match error {
        Error::StaleFence => {
            crate::PostgresInteractionError::Execution(crate::PostgresExecutionError::StaleFence)
        }
        Error::Store(
            DialogueStoreError::RevisionConflict
            | DialogueStoreError::Unavailable
            | DialogueStoreError::Inactive,
        ) => crate::PostgresInteractionError::ActorForbidden,
        _ => crate::PostgresInteractionError::Corrupt,
    }
}
