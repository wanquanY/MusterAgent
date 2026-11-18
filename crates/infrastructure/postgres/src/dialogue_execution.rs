//! Admission authority for the account-owned Owner product. Public Workbench
//! creation and its built-in Agent resolver are deliberately not involved.
use crate::dialogue_codec::*;
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use sqlx::{PgConnection, Row};

pub(crate) mod agent;
pub(crate) mod commit;
pub(crate) mod snapshot;

#[derive(Clone)]
pub struct PostgresDialogueExecutionStore {
    pub(super) admission: crate::PostgresAdmissionStore,
    delivery_contract: DialogueOutputContract,
}
impl PostgresDialogueExecutionStore {
    pub fn new(admission: crate::PostgresAdmissionStore) -> Self {
        Self {
            admission,
            delivery_contract: DialogueOutputContract::CURRENT,
        }
    }

    /// Immutable historical release source for conformance. Both prepare and
    /// transactional revalidation use this source; production has no override.
    #[cfg(feature = "test-support")]
    pub fn for_published_delivery_contract_test(
        admission: crate::PostgresAdmissionStore,
        delivery_contract: impl Into<DialogueOutputContract>,
    ) -> Self {
        Self {
            admission,
            delivery_contract: delivery_contract.into(),
        }
    }
}
impl DialogueExecutionStore for PostgresDialogueExecutionStore {
    fn lookup(
        &self,
        actor: Actor,
        intent_id: UserIntentId,
    ) -> ApplicationFuture<'_, Result<Option<DialogueExecutionReceipt>, DialogueStoreError>> {
        Box::pin(async move {
            require_user(actor)?;
            let mut connection = self.admission.ledger.pool().acquire().await.map_err(db)?;
            lookup_in_connection(&mut connection, actor, intent_id).await
        })
    }
    fn prepare(
        &self,
        actor: Actor,
        intent_id: UserIntentId,
    ) -> ApplicationFuture<'_, Result<DialogueExecutionPreparation, DialogueStoreError>> {
        Box::pin(async move {
            require_user(actor)?;
            let mut tx = self
                .admission
                .ledger
                .database()
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Admission,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            let value = load_preparation_for_contract(
                &mut tx,
                actor,
                intent_id,
                false,
                self.delivery_contract,
            )
            .await?;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(value)
        })
    }
    fn admit(
        &self,
        commit: DialogueExecutionAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AdmissionReceipt, DialogueStoreError>>
    {
        Box::pin(async move { self.commit(commit).await })
    }
}

pub(super) async fn load_preparation(
    tx: &mut PgConnection,
    actor: Actor,
    intent_id: UserIntentId,
    lock: bool,
) -> Result<DialogueExecutionPreparation, DialogueStoreError> {
    load_preparation_for_contract(tx, actor, intent_id, lock, DialogueOutputContract::CURRENT).await
}
async fn load_preparation_for_contract(
    tx: &mut PgConnection,
    actor: Actor,
    intent_id: UserIntentId,
    lock: bool,
    delivery_contract: DialogueOutputContract,
) -> Result<DialogueExecutionPreparation, DialogueStoreError> {
    let cloud: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_intent_admissions q JOIN agent.dialogue_user_intents i USING(intent_id) WHERE i.intent_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3 AND q.execution_target='cloud')")
        .bind(intent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .fetch_one(&mut *tx).await.map_err(db)?;
    if !cloud {
        return Err(DialogueStoreError::Unavailable);
    }
    load_preparation_source(tx, actor, intent_id, lock, delivery_contract).await
}

/// Shared material owner. A caller must first prove its exact immutable
/// acceptance destination; choosing a source never grants execution authority.
pub(super) async fn load_preparation_source(
    tx: &mut PgConnection,
    actor: Actor,
    intent_id: UserIntentId,
    lock: bool,
    delivery_contract: DialogueOutputContract,
) -> Result<DialogueExecutionPreparation, DialogueStoreError> {
    require_user(actor)?;
    let ir=sqlx::query("SELECT i.*,CASE WHEN i.origin_surface='desktop' AND q.execution_target='cloud' THEN (to_jsonb(q)->>'origin_tool_device_id')::uuid END AS origin_tool_device_id FROM agent.dialogue_user_intents i LEFT JOIN agent.dialogue_intent_admissions q USING(intent_id) WHERE i.intent_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3")
        .bind(intent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let intent = intent_row(&ir)?;
    let current = agent::load(tx, actor, intent.dialogue_id, lock).await?;
    // Lifecycle refusal is an original source state, not malformed material.
    // Existing frozen Native custody still needs its separate no-admission proof.
    if current.agent.status != musteragent_definition::owned_agent::OwnedAgentStatus::Active {
        return Err(DialogueStoreError::Inactive);
    }

    intent
        .accepted_cursor
        .require_at_or_before(current.dialogue.head)
        .map_err(|_| DialogueStoreError::RevisionConflict)?;
    let agent = current.agent;
    if intent.origin.surface == DialogueInputSurface::Capability {
        // This exact accepted current input and active Agent were just resolved
        // by their owner. A missing/invalid ancestor or exhausted proof budget
        // is a deterministic preparation failure, not an inactive account or a
        // transient grant error. Keep infrastructure failures distinct.
        crate::agent_dialogue::capability::verified_root(tx, actor, agent.agent_id, &intent)
            .await
            .map_err(|error| match error {
                DialogueStoreError::Unavailable | DialogueStoreError::Corrupt => {
                    DialogueStoreError::Corrupt
                }
                other => other,
            })?;
    }
    let configuration = current.configuration;
    let scope = DialogueExecutionScope {
        tenant_id: actor.tenant_id,
        owner_id: actor.principal_id,
        agent_id: agent.agent_id,
        dialogue_id: intent.dialogue_id,
        source_intent_id: intent.intent_id,
        context_watermark: intent.accepted_cursor,
    };
    let context_cut = crate::dialogue_context::load_cut(
        tx,
        actor,
        scope.agent_id,
        scope.dialogue_id,
        DialogueCursor {
            epoch: scope.context_watermark.epoch,
            seq: scope
                .context_watermark
                .seq
                .checked_sub(1)
                .ok_or(DialogueStoreError::Corrupt)?,
        },
    )
    .await?;
    let context_has_input_attachments =
        crate::dialogue_context::has_input_attachments(tx, &context_cut).await?;
    let product_release = if intent.origin.surface == DialogueInputSurface::Schedule {
        crate::owner_schedule::release::for_intent(tx, actor, intent_id)
            .await
            .map_err(|_| DialogueStoreError::Corrupt)?
    } else {
        None
    };
    let value = DialogueExecutionPreparation {
        origin_tool_device_id: ir
            .try_get::<Option<uuid::Uuid>, _>("origin_tool_device_id")
            .map_err(db)?
            .map(DeviceId::from_uuid)
            .transpose()
            .map_err(|_| DialogueStoreError::Corrupt)?,
        product_release,
        device_workspace_id: None,
        scope,
        default_agent_id: current.default_agent_id,
        identities: DialogueExecutionIdentities::derive(scope)?,
        intent,
        agent,
        configuration,
        assistant_identity: current.identity,
        context_cut,
        context_has_input_attachments,
        delivery_contract,
    };
    value.validate()?;
    Ok(value)
}

/// Admission and terminal pre-Run decisions serialize on the accepted input.
pub(crate) async fn lock_unresolved_intent(
    tx: &mut PgConnection,
    actor: Actor,
    intent: UserIntentId,
) -> Result<(), DialogueStoreError> {
    lock_unsettled_intent(tx, actor, intent).await?;
    crate::dialogue_execution_control::source::require_not_cancelled(tx, intent).await
}

/// Lock original input custody without reading its ancestry. Native capture
/// first authenticates its exact queue claim, then classifies preparation
/// failure through the original owner before applying the cancellation fence.
pub(crate) async fn lock_unsettled_intent(
    tx: &mut PgConnection,
    actor: Actor,
    intent: UserIntentId,
) -> Result<(), DialogueStoreError> {
    let exists=sqlx::query("SELECT intent_id FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR NO KEY UPDATE")
        .bind(intent.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    if exists.is_none() {
        return Err(DialogueStoreError::Unavailable);
    }
    let terminal: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent.dialogue_admission_outcomes WHERE intent_id=$1)",
    )
    .bind(intent.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    if terminal {
        return Err(DialogueStoreError::Inactive);
    }
    Ok(())
}

/// Shared original admission lookup; publication uses the same authority while
/// holding the accepted-intent lock.
pub(crate) async fn lookup_in_connection(
    connection: &mut PgConnection,
    actor: Actor,
    intent_id: UserIntentId,
) -> Result<Option<DialogueExecutionReceipt>, DialogueStoreError> {
    let row=sqlx::query("SELECT s.conversation_id,r.run_id FROM agent.dialogue_sessions s JOIN conversation.conversations c ON c.conversation_id=s.conversation_id AND c.tenant_id=s.tenant_id AND c.created_by=s.owner_id AND c.purpose=s.conversation_purpose JOIN runtime.runs r ON r.conversation_id=s.conversation_id AND r.tenant_id=s.tenant_id WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.source_intent_id=$3 AND s.purpose='intent_coordination'")
                .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(intent_id.into_uuid())
                .fetch_optional(&mut *connection).await.map_err(db)?;
    row.map(|r| {
        Ok(DialogueExecutionReceipt {
            conversation_id: ConversationId::from_uuid(r.try_get("conversation_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
            run_id: RunId::from_uuid(r.try_get("run_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?,
        })
    })
    .transpose()
}

pub(crate) mod authority;
