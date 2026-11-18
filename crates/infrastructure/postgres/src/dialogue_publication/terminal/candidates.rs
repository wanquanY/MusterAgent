//! A terminal lease cannot invent a result. Recover only a whole, verified
//! completion group frozen by the original Model-completion transaction.
use super::*;
pub(super) enum Recovery {
    PreviouslyPublished,
    Published(DialogueEntryId),
}
pub(super) async fn recover(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    run: RunId,
    proof: &proof::TerminalProof,
) -> Result<Option<Recovery>, Error> {
    // A private producer submission cannot be recorded as already delivered.
    // Failure reconciliation may emit only its fixed terminal notice.
    if super::super::task_result::verified_subject(tx, source.actor.tenant_id, run).await? {
        return Ok(None);
    }
    if !proof.requires_effect_reconciliation
        && super::super::completion::native_tools_ready(
            tx,
            source.actor.tenant_id,
            run,
            proof.iteration,
            None,
        )
        .await?
    {
        return Ok(Some(Recovery::PreviouslyPublished));
    }
    let rows = sqlx::query("SELECT c.*,p.entry_id,p.payload_sha256 AS published_fingerprint FROM agent.public_delivery_candidates c
        JOIN runtime.model_invocations i ON i.tenant_id=c.tenant_id AND i.run_id=c.run_id AND i.invocation_id=c.invocation_id AND i.status=2
        JOIN runtime.context_plans cp ON cp.tenant_id=i.tenant_id AND cp.run_id=i.run_id AND cp.invocation_id=i.invocation_id AND cp.iteration=$3
        LEFT JOIN agent.dialogue_publications p ON p.tenant_id=c.tenant_id AND p.source_run_id=c.run_id AND p.delivery_id=COALESCE(c.satisfied_publication_id,c.delivery_id)
        WHERE c.tenant_id=$1 AND c.run_id=$2 ORDER BY c.ordinal LIMIT 17")
        .bind(source.actor.tenant_id.into_uuid()).bind(run.into_uuid()).bind(proof.iteration).fetch_all(&mut *tx).await.map_err(db)?;
    if rows.len() > MAX_PUBLIC_DELIVERIES {
        return Err(DialogueStoreError::Corrupt.into());
    }
    if rows.is_empty() {
        return Ok(None);
    }
    let covered = rows.iter().any(|row| {
        matches!(
            row.try_get::<&str, _>("kind"),
            Ok("message" | "artifact" | "partner_created")
        ) && matches!(
            row.try_get::<&str, _>("purpose"),
            Ok("reply" | "result" | "blocked")
        )
    });
    if !covered {
        return Ok(None);
    }
    let invocation: uuid::Uuid = rows[0].try_get("invocation_id").map_err(db)?;
    let mut pending = Vec::new();
    for (ordinal, row) in rows.iter().enumerate() {
        if row.try_get::<uuid::Uuid, _>("invocation_id").map_err(db)? != invocation
            || row.try_get::<i16, _>("ordinal").map_err(db)? != ordinal as i16
        {
            return Err(DialogueStoreError::Corrupt.into());
        }
        let contract = PublicDeliveryContract::from_key(row.try_get("contract_key").map_err(db)?)
            .ok_or(DialogueStoreError::Corrupt)?;
        if row.try_get::<Vec<u8>, _>("contract_sha256").map_err(db)? != contract.hash().into_bytes()
        {
            return Err(DialogueStoreError::Corrupt.into());
        }

        let published = row
            .try_get::<Option<uuid::Uuid>, _>("entry_id")
            .map_err(db)?
            .is_some();
        let revision = unsigned(row.try_get("permission_revision").map_err(db)?)?;
        // Configuration changes never grant authority to salvage prior resources.
        if !published && revision != source.permission_revision {
            return Ok(None);
        }
        let purpose = match row.try_get::<&str, _>("purpose").map_err(db)? {
            "reply" => DialoguePublicationPurpose::Reply,
            "result" => DialoguePublicationPurpose::Result,
            "blocked" => DialoguePublicationPurpose::Blocked,
            "important_update" => DialoguePublicationPurpose::ImportantUpdate,
            "clarification" => DialoguePublicationPurpose::Clarification,
            "required_decision" => DialoguePublicationPurpose::RequiredDecision,
            _ => return Err(DialogueStoreError::Corrupt.into()),
        };
        let content = match row.try_get::<&str, _>("kind").map_err(db)? {
            "message" => {
                let reference = content_from_bytes(row.try_get("body_content_ref").map_err(db)?)?;
                if reference.content_id.into_uuid()
                    != row
                        .try_get::<uuid::Uuid, _>("body_content_id")
                        .map_err(db)?
                {
                    return Err(DialogueStoreError::Corrupt.into());
                }
                PublicDeliveryContent::Message(reference)
            }
            "artifact" => PublicDeliveryContent::Artifact {
                artifact_id: ArtifactId::from_uuid(row.try_get("artifact_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                artifact_version_id: ArtifactVersionId::from_uuid(
                    row.try_get("artifact_version_id").map_err(db)?,
                )
                .map_err(|_| DialogueStoreError::Corrupt)?,
            },
            // New questions cannot be made actionable after their Run ended.
            "interaction" if published => PublicDeliveryContent::Interaction(
                InteractionId::from_uuid(row.try_get("interaction_id").map_err(db)?)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
            ),
            "interaction" => return Ok(None),
            _ => return Err(DialogueStoreError::Corrupt.into()),
        };
        let delivery_id = CommandId::from_uuid(row.try_get("delivery_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let fingerprint = public_delivery_fingerprint(
            delivery_id,
            run,
            NonZeroU64::new(revision).ok_or(DialogueStoreError::Corrupt)?,
            purpose,
            &content,
        );
        if published {
            if row
                .try_get::<Vec<u8>, _>("published_fingerprint")
                .map_err(db)?
                != fingerprint.into_bytes()
            {
                return Err(DialogueStoreError::Corrupt.into());
            }
            continue;
        }
        pending.push(Delivery {
            delivery_id,
            run_id: run,
            purpose,
            fingerprint,
            content,
            source: PublicationSource::TerminalOwner(proof.record_id),
        });
    }
    if pending.is_empty() {
        return Ok(Some(Recovery::PreviouslyPublished));
    }
    // Preflight the complete group before inserting any entry. A withdrawn
    // resource can no longer cover the request; a useful notice remains owed.
    // Infrastructure or corrupt proofs retain the obligation for investigation.
    let mut references = Vec::new();
    for delivery in &pending {
        match super::super::references::resolve_reference(tx, source, run, &delivery.content).await
        {
            Ok(reference) => references.push(reference),
            Err(Error::Store(DialogueStoreError::Unavailable)) => return Ok(None),
            Err(error) => return Err(error),
        }
    }
    references.sort_by_key(|reference| reference.content_id);
    for reference in references {
        match crate::dialogue_codec::lock_ready_content_reference(
            tx,
            source.actor.tenant_id,
            &reference,
        )
        .await
        {
            Ok(()) => {}
            Err(DialogueStoreError::Unavailable) => return Ok(None),
            Err(error) => return Err(error.into()),
        }
    }
    let mut last = None;
    for delivery in pending {
        last = Some(
            publish_in_transaction(tx, source, &delivery)
                .await?
                .entry_id,
        );
    }
    Ok(last.map(Recovery::Published))
}
