//! Retention of accepted, read-only result references. Privacy may remove only
//! these exact pins; the accepted input and its immutable binding remain facts.
//! This is retained custody verification, never live read authorization.
use crate::dialogue_publication::work_item_erasure::WorkItemPublicClosure;
use crate::{PostgresPrivacyErasureError as Error, privacy_erasure::WorkItemProviderScope};
use musteragent_content_ref::ContentRef;
use musteragent_execution_context::DialogueReplySubject;
use musteragent_identity::*;
use sqlx::{PgConnection, Row};
use std::collections::{BTreeSet, HashMap};

pub(crate) struct ReplyRetentionClosure {
    pub(crate) intents: Vec<uuid::Uuid>,
    pub(crate) contents: Vec<uuid::Uuid>,
}

pub(crate) async fn collect(
    tx: &mut PgConnection,
    actor: Actor,
    scope: &WorkItemProviderScope,
    public: &WorkItemPublicClosure,
) -> Result<ReplyRetentionClosure, Error> {
    if actor.tenant_id.into_uuid() != scope.tenant() || actor.principal_kind != PrincipalKind::User
    {
        return Err(Error::WorkItemUnauthorized);
    }
    let content_ids = public
        .contents
        .iter()
        .chain(scope.report_contents())
        .map(|v| v.content_id.into_uuid())
        .collect::<Vec<_>>();
    let publications = sqlx::query("SELECT p.*,e.content_ref FROM agent.dialogue_publications p
        JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.tenant_id=p.tenant_id AND e.owner_id=p.owner_id
        WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.entry_id=ANY($3)")
        .bind(scope.tenant()).bind(actor.principal_id.into_uuid()).bind(&public.entries)
        .fetch_all(&mut *tx).await?;
    let publications = publications
        .into_iter()
        .map(|r| Ok((r.try_get::<uuid::Uuid, _>("entry_id")?, r)))
        .collect::<Result<HashMap<_, _>, sqlx::Error>>()?;
    let mut pairs = BTreeSet::new();
    let mut after: Option<uuid::Uuid> = None;
    loop {
        // Keyset pages bound each read, not the total number of lawful references.
        // A pin alone is not proof, and a canonical
        // intent whose body/attachment happens to use these bytes is not disposable.
        let rows = sqlx::query("SELECT i.*,e.kind AS proof_entry_kind,e.source_intent_id AS proof_entry_intent,
        e.content_ref AS proof_entry_content,r.command_kind AS proof_command_kind,
        r.receipt AS proof_receipt,r.payload_sha256 AS proof_fingerprint,
        q.execution_target,q.target_device_id,q.target_installation_id,q.native_subject_proof,
        q.reply_origin_device_id,q.reply_origin_installation_id,q.reply_origin_subject_proof,q.desktop_source_membership_id,q.desktop_source_membership_epoch,CASE WHEN i.origin_surface='desktop' AND q.execution_target='cloud' THEN (to_jsonb(q)->>'origin_tool_device_id')::uuid END AS origin_tool_device_id,
        ARRAY(SELECT pin.content_id FROM agent.dialogue_content_references pin
          WHERE pin.tenant_id=i.tenant_id AND pin.owner_id=i.owner_id AND pin.reference_kind='user_intent'
            AND pin.reference_id=i.intent_id AND pin.content_id=ANY($3) ORDER BY pin.content_id) AS reply_pins,
        EXISTS(SELECT 1 FROM agent.owner_schedule_occurrences o JOIN agent.owner_schedules s USING(schedule_id)
          WHERE o.occurrence_id=i.command_id AND o.intent_id=i.intent_id AND o.status='accepted'
            AND s.tenant_id=i.tenant_id AND s.owner_id=i.owner_id AND s.agent_id=d.agent_id) AS proof_schedule
        FROM agent.dialogue_user_intents i
        JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id
        JOIN agent.agent_dialogue_entries e ON e.entry_id=i.source_entry_id AND e.tenant_id=i.tenant_id
          AND e.owner_id=i.owner_id AND e.dialogue_id=i.dialogue_id AND e.epoch=i.accepted_epoch AND e.seq=i.accepted_seq
        LEFT JOIN agent.owner_command_receipts r ON r.tenant_id=i.tenant_id AND r.owner_id=i.owner_id AND r.command_id=i.command_id
        LEFT JOIN agent.dialogue_intent_admissions q ON q.intent_id=i.intent_id
        WHERE i.tenant_id=$1 AND i.owner_id=$2 AND i.reply_binding IS NOT NULL
          AND EXISTS(SELECT 1 FROM agent.dialogue_content_references pin WHERE pin.tenant_id=i.tenant_id
            AND pin.owner_id=i.owner_id AND pin.reference_kind='user_intent' AND pin.reference_id=i.intent_id AND pin.content_id=ANY($3))
          AND ($4::uuid IS NULL OR i.intent_id>$4)
        ORDER BY i.intent_id LIMIT 256")
        .bind(scope.tenant()).bind(actor.principal_id.into_uuid()).bind(&content_ids).bind(after)
        .fetch_all(&mut *tx).await?;
        if rows.is_empty() {
            break;
        }
        after = Some(
            rows.last()
                .ok_or_else(corrupt)?
                .try_get::<uuid::Uuid, _>("intent_id")?,
        );
        for row in rows {
            let intent = crate::dialogue_codec::intent_row(&row).map_err(|_| corrupt())?;
            let binding = intent.reply_binding.as_ref().ok_or_else(corrupt)?;
            let DialogueReplySubject::WorkItemResult {
                target,
                producing_run_id,
                report_run_id,
                report_content_ref,
                ..
            } = &binding.subject
            else {
                continue;
            };
            if target.work_item_id.into_uuid() != scope.task() {
                continue;
            }
            // The source owner checks the exact canonical receipt, the entire input
            // including both placements, and its original acceptance fingerprint.
            // No active-Agent/current-epoch check can block privacy after revocation.
            super::capability::verify_retained_acceptance(actor, &intent, &row)
                .map_err(|_| corrupt())?;
            let publication = binding.publication.as_ref().ok_or_else(corrupt)?;
            let original = publications
                .get(&binding.target.entry_id.into_uuid())
                .ok_or_else(corrupt)?;
            let original_content = crate::dialogue_codec::content_from_bytes(
                original
                    .try_get::<Option<Vec<u8>>, _>("content_ref")?
                    .ok_or_else(corrupt)?,
            )
            .map_err(|_| corrupt())?;
            if !scope.runs().contains(&producing_run_id.into_uuid())
                || !scope.runs().contains(&publication.run_id.into_uuid())
                || original.try_get::<uuid::Uuid, _>("delivery_id")?
                    != publication.delivery_id.into_uuid()
                || original.try_get::<uuid::Uuid, _>("source_run_id")?
                    != publication.run_id.into_uuid()
                || original.try_get::<uuid::Uuid, _>("source_conversation_id")?
                    != publication.conversation_id.into_uuid()
                || original.try_get::<uuid::Uuid, _>("agent_id")? != binding.agent_id.into_uuid()
                || original.try_get::<uuid::Uuid, _>("dialogue_id")?
                    != binding.dialogue_id.into_uuid()
                || original.try_get::<Vec<u8>, _>("payload_sha256")?
                    != publication.payload_sha256.into_bytes()
                || original_content != binding.content_ref
                || report_run_id.is_some_and(|id| id != publication.run_id)
                || report_content_ref
                    .as_ref()
                    .is_some_and(|r| !scope.report_contents().contains(r))
            {
                return Err(corrupt());
            }
            let pins: Vec<uuid::Uuid> = row.try_get("reply_pins")?;
            for reference in binding.content_refs() {
                if pins.contains(&reference.content_id.into_uuid()) && !authored(&intent, reference)
                {
                    pairs.insert((
                        intent.intent_id.into_uuid(),
                        reference.content_id.into_uuid(),
                    ));
                }
            }
        }
    }
    let (intents, contents) = pairs.into_iter().unzip();
    Ok(ReplyRetentionClosure { intents, contents })
}
fn authored(intent: &musteragent_execution_context::UserIntent, reference: &ContentRef) -> bool {
    intent.content_ref.content_id == reference.content_id
        || intent
            .input_attachments
            .iter()
            .any(|a| a.attachment.content_ref.content_id == reference.content_id)
}
fn corrupt() -> Error {
    Error::CorruptState("accepted result reference custody")
}

pub(crate) async fn erase(
    tx: &mut PgConnection,
    actor: Actor,
    closure: &ReplyRetentionClosure,
) -> Result<(), Error> {
    sqlx::query(
        "DELETE FROM agent.dialogue_content_references pin
        USING UNNEST($3::uuid[],$4::uuid[]) AS allowed(intent_id,content_id)
        WHERE pin.tenant_id=$1 AND pin.owner_id=$2 AND pin.reference_kind='user_intent'
          AND pin.reference_id=allowed.intent_id AND pin.content_id=allowed.content_id",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(&closure.intents)
    .bind(&closure.contents)
    .execute(tx)
    .await?;
    Ok(())
}
