//! Original Dialogue Native custody projected to the account stop snapshot.
//! No ACK predicate: the device may already have committed admission offline.
use crate::dialogue_codec::{db, integer};
use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_runtime_session::{
    DialogueExecutionCancellationRequest, DialogueStoreError, device_control::*,
};
use sqlx::{PgConnection, Row};

// The account projection/freezer consumes the original candidate VIEW in SQL.
// Only delivery decodes one frozen target; no parallel Rust observation path.
fn decode(
    row: &sqlx::postgres::PgRow,
) -> Result<NativeDialogueExecutionControlTarget, DialogueStoreError> {
    let bad = DialogueStoreError::Corrupt;
    let id = |name| -> Result<uuid::Uuid, DialogueStoreError> { row.try_get(name).map_err(db) };
    let value = NativeDialogueExecutionControlTarget {
        actor: Actor {
            tenant_id: TenantId::from_uuid(id("tenant_id")?).map_err(|_| bad)?,
            principal_id: PrincipalId::from_uuid(id("owner_id")?).map_err(|_| bad)?,
            principal_kind: PrincipalKind::User,
        },
        agent_id: AgentId::from_uuid(id("agent_id")?).map_err(|_| bad)?,
        dialogue_id: AgentDialogueId::from_uuid(id("dialogue_id")?).map_err(|_| bad)?,
        intent_id: UserIntentId::from_uuid(id("intent_id")?).map_err(|_| bad)?,
        run_id: RunId::from_uuid(id("run_id")?).map_err(|_| bad)?,
        conversation_id: ConversationId::from_uuid(id("conversation_id")?).map_err(|_| bad)?,
        device_id: DeviceId::from_uuid(id("device_id")?).map_err(|_| bad)?,
        installation_id: DeviceInstallationId::from_uuid(id("installation_id")?)
            .map_err(|_| bad)?,
        source_digest: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("source_digest").map_err(db)?)
            .map_err(|_| bad)?,
        grant_revision: u64::try_from(row.try_get::<i64, _>("grant_revision").map_err(db)?)
            .map_err(|_| bad)?,
        grant_proof_hash: row
            .try_get::<Option<Vec<u8>>, _>("grant_proof_hash")
            .map_err(db)?
            .map(|h| Sha256::from_bytes(h).map_err(|_| bad))
            .transpose()?,
    };
    value.validate()?;
    Ok(value)
}
/// Account lock is held by the original request owner. Taking the same Intent
/// and custody locks as grant issuance freezes the complete grant history.
pub(crate) async fn enqueue(
    tx: &mut PgConnection,
    request: &DialogueExecutionCancellationRequest,
    command_id: CommandId,
    target: &NativeDialogueExecutionControlTarget,
) -> Result<(), DialogueStoreError> {
    request.validate()?;
    target.validate()?;
    if target.actor != request.actor
        || target.agent_id != request.agent_id
        || target.dialogue_id != request.dialogue_id
    {
        return Err(DialogueStoreError::Unavailable);
    }
    sqlx::query("SELECT intent_id FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR UPDATE")
        .bind(target.intent_id.into_uuid()).bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    sqlx::query("SELECT intent_id FROM agent.device_admissions WHERE intent_id=$1 FOR UPDATE")
        .bind(target.intent_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(DialogueStoreError::Unavailable)?;
    let rows=sqlx::query("SELECT d.*,coalesce(h.current_revision,0) AS grant_revision,v.signed_grant_hash AS grant_proof_hash FROM agent.device_admissions d LEFT JOIN agent.device_grant_heads h ON h.intent_id=d.intent_id LEFT JOIN agent.device_grant_versions v ON v.intent_id=h.intent_id AND v.revision=h.current_revision AND v.source_digest=d.source_digest WHERE d.intent_id=$1")
        .bind(target.intent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    // An observation may be older than a just-issued renewal. It must never
    // silently widen its immutable grant set: the user must refresh the revision.
    if decode(&rows)? != *target {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let at: i64 = sqlx::query_scalar(
        "SELECT created_at_ms FROM agent.dialogue_execution_cancellations WHERE command_id=$1",
    )
    .bind(request.command_id.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    let control = NativeDialogueExecutionControl {
        cancellation_id: request.command_id,
        command_id,
        observed_activity_revision: request.observed_activity_revision,
        created_at: UtcTimestamp::from_unix_millis(at),
        target: target.clone(),
    };
    let bytes = control.canonical_bytes()?;
    let changed=sqlx::query("INSERT INTO agent.native_execution_controls(command_id,cancellation_id,intent_id,run_id,tenant_id,owner_id,agent_id,dialogue_id,device_id,installation_id,source_digest,control_sha256,canonical_control,created_at_ms,grant_revision,grant_proof_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) ON CONFLICT(command_id) DO NOTHING")
        .bind(command_id.into_uuid()).bind(request.command_id.into_uuid()).bind(target.intent_id.into_uuid()).bind(target.run_id.into_uuid()).bind(target.actor.tenant_id.into_uuid()).bind(target.actor.principal_id.into_uuid()).bind(target.agent_id.into_uuid()).bind(target.dialogue_id.into_uuid()).bind(target.device_id.into_uuid()).bind(target.installation_id.into_uuid()).bind(target.source_digest.into_bytes().to_vec()).bind(Sha256::digest(&bytes).into_bytes().to_vec()).bind(&bytes).bind(at).bind(integer(target.grant_revision)?).bind(target.grant_proof_hash.map(|h|h.into_bytes().to_vec())).execute(&mut *tx).await.map_err(db)?.rows_affected();
    if changed == 0 {
        let old: Vec<u8> = sqlx::query_scalar(
            "SELECT canonical_control FROM agent.native_execution_controls WHERE command_id=$1",
        )
        .bind(command_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if old != bytes {
            return Err(DialogueStoreError::IdempotencyConflict);
        }
    }
    Ok(())
}
/// Called under the original account/Intent lock, before returning or renewing
/// a signed grant. Replaying a grant after a retained stop would reopen admission.
pub(crate) async fn reject_new_grant(
    tx: &mut PgConnection,
    intent: UserIntentId,
) -> Result<(), DialogueStoreError> {
    crate::dialogue_execution_control::source::require_not_cancelled(tx, intent).await
}
