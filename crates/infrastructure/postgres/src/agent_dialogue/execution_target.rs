//! Captures the sole destination and signed input origin in acceptance.
//! No post-acceptance Device/Cloud race or fallback reassignment is possible.
use super::*;

pub(super) fn require_release() -> Result<(), Error> {
    if musteragent_runtime_snapshot::dialogue_device_discovery_policy_for_current_release()
        .is_none()
    {
        return Err(Error::Unavailable);
    }
    Ok(())
}
pub(super) async fn stage(
    tx: &mut sqlx::PgConnection,
    intent: &UserIntent,
    target: &DialogueIntentExecutionTarget,
) -> Result<(), Error> {
    if let (Some(binding), DialogueIntentExecutionTarget::OriginDevice(device)) =
        (intent.reply_binding.as_ref(), target)
        && binding.task_result().is_some()
    {
        // The qualified input contract and publication owner have already
        // authenticated the control subject. The ordinary Cloud admission
        // owner still revalidates current Agent permission before any Run.
        sqlx::query("INSERT INTO agent.dialogue_intent_admissions(intent_id,status,available_at_ms,reply_origin_device_id,reply_origin_installation_id,reply_origin_subject_proof) VALUES($1,'pending',$2,$3,$4,$5)")
                .bind(intent.intent_id.into_uuid()).bind(intent.accepted_at.unix_millis())
                .bind(device.device_id().into_uuid()).bind(device.installation_id().into_uuid())
                .bind(device.membership_authority()).execute(&mut *tx).await.map_err(db)?;
        return Ok(());
    }
    match target {
        DialogueIntentExecutionTarget::DesktopTools(device) => {
            sqlx::query("INSERT INTO agent.dialogue_intent_admissions(intent_id,status,available_at_ms,origin_tool_device_id) VALUES($1,'pending',$2,$3)")
                .bind(intent.intent_id.into_uuid()).bind(intent.accepted_at.unix_millis()).bind(device.into_uuid())
                .execute(&mut *tx).await.map_err(db)?;
        }
        DialogueIntentExecutionTarget::CloudFromDesktop(host) => {
            sqlx::query("INSERT INTO agent.dialogue_intent_admissions(intent_id,status,available_at_ms,desktop_source_membership_id,desktop_source_membership_epoch) VALUES($1,'pending',$2,$3,$4)")
                .bind(intent.intent_id.into_uuid()).bind(intent.accepted_at.unix_millis())
                .bind(host.membership_id).bind(i64::try_from(host.expected_membership_epoch).map_err(|_| Error::Corrupt)?)
                .execute(&mut *tx).await.map_err(db)?;
        }
        DialogueIntentExecutionTarget::Cloud => {
            sqlx::query("INSERT INTO agent.dialogue_intent_admissions(intent_id,status,available_at_ms) VALUES($1,'pending',$2)")
                .bind(intent.intent_id.into_uuid()).bind(intent.accepted_at.unix_millis())
                .execute(&mut *tx).await.map_err(db)?;
        }
        DialogueIntentExecutionTarget::OriginDevice(device) => {
            sqlx::query("INSERT INTO agent.dialogue_intent_admissions(intent_id,status,available_at_ms,execution_target,target_device_id,target_installation_id,native_subject_proof) VALUES($1,'pending',$2,'origin_device',$3,$4,$5)")
                .bind(intent.intent_id.into_uuid()).bind(intent.accepted_at.unix_millis())
                .bind(device.device_id().into_uuid()).bind(device.installation_id().into_uuid())
                .bind(device.membership_authority()).execute(&mut *tx).await.map_err(db)?;
        }
    }
    Ok(())
}

/// Reads immutable source custody for receipt verification only. The retained
/// envelope is not current authorization and must never issue a new grant.
pub(super) async fn retained(
    tx: &mut sqlx::PgConnection,
    command: &AcceptDialogueIntent,
) -> Result<Option<DialogueIntentExecutionTarget>, Error> {
    let row = retained_row(tx, command).await?;
    row.as_ref()
        .map(|row| retained_from_row(row, command))
        .transpose()
}

async fn retained_row(
    tx: &mut sqlx::PgConnection,
    command: &AcceptDialogueIntent,
) -> Result<Option<sqlx::postgres::PgRow>, Error> {
    sqlx::query("SELECT q.execution_target,q.target_device_id,q.target_installation_id,q.native_subject_proof,q.reply_origin_device_id,q.reply_origin_installation_id,q.reply_origin_subject_proof,q.desktop_source_membership_id,q.desktop_source_membership_epoch,CASE WHEN i.origin_surface='desktop' AND q.execution_target='cloud' THEN (to_jsonb(q)->>'origin_tool_device_id')::uuid END AS origin_tool_device_id,i.reply_binding FROM agent.dialogue_user_intents i LEFT JOIN agent.dialogue_intent_admissions q USING(intent_id) WHERE i.tenant_id=$1 AND i.owner_id=$2 AND i.command_id=$3")
        .bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid())
        .bind(command.command_id.into_uuid()).fetch_optional(tx).await.map_err(db)
}

/// Decode the original retained admission from an already scoped source row.
/// A caller may join these four columns to its own atomic source read; decoding
/// and signed Native subject validation still have this one original owner.
pub(super) fn retained_from_row(
    row: &sqlx::postgres::PgRow,
    command: &AcceptDialogueIntent,
) -> Result<DialogueIntentExecutionTarget, Error> {
    let kind: Option<String> = row.try_get("execution_target").map_err(db)?;
    let device: Option<uuid::Uuid> = row.try_get("target_device_id").map_err(db)?;
    let installation: Option<uuid::Uuid> = row.try_get("target_installation_id").map_err(db)?;
    let proof: Option<Vec<u8>> = row.try_get("native_subject_proof").map_err(db)?;
    let reply_origin_device: Option<uuid::Uuid> =
        row.try_get("reply_origin_device_id").map_err(db)?;
    let reply_origin_installation: Option<uuid::Uuid> =
        row.try_get("reply_origin_installation_id").map_err(db)?;
    let reply_origin_proof: Option<Vec<u8>> =
        row.try_get("reply_origin_subject_proof").map_err(db)?;
    let binding = crate::dialogue_codec::decode_reply_binding_bytes(
        row.try_get("reply_binding").map_err(db)?,
    )?;
    let origin = match (
        reply_origin_device,
        reply_origin_installation,
        reply_origin_proof,
    ) {
        (None, None, None) => None,
        (Some(device), Some(installation), Some(proof))
            if kind.as_deref() == Some("cloud")
                && binding.as_ref().and_then(|b| b.task_result()).is_some()
                && command.reply_target == binding.as_ref().map(|b| b.target) =>
        {
            Some((device, installation, proof))
        }
        _ => return Err(Error::Corrupt),
    };
    let target = match (kind.as_deref(), device, installation, proof, origin) {
        (Some("cloud"), None, None, None, None) => DialogueIntentExecutionTarget::Cloud,
        (Some("cloud"), None, None, None, Some((device, installation, proof))) => {
            native_origin(device, installation, proof)?
        }
        (Some("origin_device"), Some(device), Some(installation), Some(proof), None) => {
            DialogueIntentExecutionTarget::OriginDevice(
                OriginDeviceExecutionTarget::from_trusted_native_transport(
                    DeviceId::from_uuid(device).map_err(|_| Error::Corrupt)?,
                    DeviceInstallationId::from_uuid(installation).map_err(|_| Error::Corrupt)?,
                    proof,
                )
                .map_err(|_| Error::Corrupt)?,
            )
        }
        _ => return Err(Error::Corrupt),
    };
    match (
        row.try_get::<Option<uuid::Uuid>, _>("desktop_source_membership_id")
            .map_err(db)?,
        row.try_get::<Option<i64>, _>("desktop_source_membership_epoch")
            .map_err(db)?,
    ) {
        (Some(membership_id), Some(epoch))
            if matches!(target, DialogueIntentExecutionTarget::Cloud) =>
        {
            let target = DialogueIntentExecutionTarget::CloudFromDesktop(
                musteragent_runtime_session::WorkItemDeviceSelection {
                    membership_id,
                    expected_membership_epoch: u64::try_from(epoch).map_err(|_| Error::Corrupt)?,
                },
            );
            target.validate_for(command).map_err(|_| Error::Corrupt)?;
            return Ok(target);
        }
        (None, None) => {}
        _ => return Err(Error::Corrupt),
    }
    if let Some(device) = row
        .try_get::<Option<uuid::Uuid>, _>("origin_tool_device_id")
        .map_err(db)?
    {
        let target = DialogueIntentExecutionTarget::DesktopTools(
            DeviceId::from_uuid(device).map_err(|_| Error::Corrupt)?,
        );
        target.validate_for(command).map_err(|_| Error::Corrupt)?;
        return Ok(target);
    }
    if let DialogueIntentExecutionTarget::OriginDevice(device) = &target {
        device
            .require_actor(command.actor)
            .map_err(|_| Error::Corrupt)?;
    }
    Ok(target)
}

/// Authorization is freshly checked outside; a renewed authority envelope must
/// not change an already accepted business command's exact retained fingerprint.
pub(super) async fn acceptance_fingerprint(
    tx: &mut sqlx::PgConnection,
    command: &AcceptDialogueIntent,
    requested: &DialogueIntentExecutionTarget,
) -> Result<musteragent_content_ref::Sha256, Error> {
    lock_dialogue_command(tx, command.actor, command.command_id).await?;
    let Some(row) = retained_row(tx, command).await? else {
        return Ok(requested.fingerprint(command));
    };
    let binding = crate::dialogue_codec::decode_reply_binding_bytes(
        row.try_get("reply_binding").map_err(db)?,
    )?;
    // The stored binding is canonical authority; a valid new request using an
    // existing command ID is a caller conflict, not corruption of that binding.
    // Check this before decoding dual placement, whose stricter invariant is
    // also used when verifying an immutable receipt or signed source snapshot.
    if command.reply_target != binding.as_ref().map(|value| value.target)
        || binding
            .as_ref()
            .is_some_and(|value| command.dialogue_id != value.dialogue_id)
    {
        return Err(Error::IdempotencyConflict);
    }
    let original = retained_from_row(&row, command)?;
    let same = match (&original, requested) {
        (
            DialogueIntentExecutionTarget::DesktopTools(a),
            DialogueIntentExecutionTarget::DesktopTools(b),
        ) => a == b,
        // Older desktop intents keep their original sealed receipt on replay.
        (
            DialogueIntentExecutionTarget::Cloud
            | DialogueIntentExecutionTarget::CloudFromDesktop(_),
            DialogueIntentExecutionTarget::DesktopTools(_),
        ) => true,
        (
            DialogueIntentExecutionTarget::Cloud
            | DialogueIntentExecutionTarget::CloudFromDesktop(_),
            DialogueIntentExecutionTarget::Cloud
            | DialogueIntentExecutionTarget::CloudFromDesktop(_),
        ) => true,
        (
            DialogueIntentExecutionTarget::OriginDevice(a),
            DialogueIntentExecutionTarget::OriginDevice(b),
        ) => a.device_id() == b.device_id() && a.installation_id() == b.installation_id(),
        _ => false,
    };
    if !same {
        return Err(Error::IdempotencyConflict);
    }
    // Reconstruct from the immutable accepted binding, never today's edited
    // public message. A replay proves the same command, not renewed read access.
    accepted_fingerprint(command, &original, binding.as_ref())
}

fn native_origin(
    device: uuid::Uuid,
    installation: uuid::Uuid,
    proof: Vec<u8>,
) -> Result<DialogueIntentExecutionTarget, Error> {
    Ok(DialogueIntentExecutionTarget::OriginDevice(
        OriginDeviceExecutionTarget::from_trusted_native_transport(
            DeviceId::from_uuid(device).map_err(|_| Error::Corrupt)?,
            DeviceInstallationId::from_uuid(installation).map_err(|_| Error::Corrupt)?,
            proof,
        )
        .map_err(|_| Error::Corrupt)?,
    ))
}

/// Both the immutable signed input placement and the authoritative control
/// destination are committed with the exact accepted publication binding.
/// Absent binding keeps every historical fingerprint byte unchanged.
pub(super) fn accepted_fingerprint(
    command: &AcceptDialogueIntent,
    requested: &DialogueIntentExecutionTarget,
    binding: Option<&DialogueReplyBinding>,
) -> Result<musteragent_content_ref::Sha256, Error> {
    let Some(binding) = binding else {
        if command.reply_target.is_some() {
            return Err(Error::Corrupt);
        }
        return Ok(requested.fingerprint(command));
    };
    binding.validate().map_err(|_| Error::Corrupt)?;
    if command.reply_target != Some(binding.target) || command.dialogue_id != binding.dialogue_id {
        return Err(Error::Corrupt);
    }
    let mut material = b"musteragent.dialogue-intent.reply-placement.v1\0".to_vec();
    material.extend_from_slice(&requested.fingerprint(command).into_bytes());
    material.extend_from_slice(
        &musteragent_content_ref::Sha256::digest(
            crate::dialogue_codec::reply_binding_bytes(Some(binding)).ok_or(Error::Corrupt)?,
        )
        .into_bytes(),
    );
    material.push(
        if binding.task_result().is_some()
            || !matches!(requested, DialogueIntentExecutionTarget::OriginDevice(_))
        {
            0
        } else {
            1
        },
    );
    Ok(musteragent_content_ref::Sha256::digest(material))
}
