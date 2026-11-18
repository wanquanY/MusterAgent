//! Shared PostgreSQL authority for the original private delivery rejection.
//! Candidate bytes remain in ContentStore; these helpers validate identities
//! and episode boundaries without creating another execution owner.
use musteragent_content_ref::Sha256;
use musteragent_execution_context::PublicDeliveryRepairCut;
use musteragent_identity::{ConversationSeq, RunId, TenantId};
use sqlx::{PgConnection, Row};

#[derive(Debug, thiserror::Error)]
pub(crate) enum RepairStoreError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("invalid private delivery source")]
    Corrupt,
    #[error("private delivery source changed")]
    Changed,
}
pub(crate) const PROGRESS_CTE: &str = r#"WITH dialogue_session AS (
            SELECT session.*,run.run_id FROM agent.dialogue_sessions session
            JOIN runtime.runs run ON run.tenant_id=session.tenant_id AND run.conversation_id=session.conversation_id
            JOIN conversation.conversations conversation ON conversation.tenant_id=session.tenant_id
              AND conversation.conversation_id=session.conversation_id AND conversation.created_by=session.owner_id
              AND conversation.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report')
            WHERE run.tenant_id=$1 AND run.run_id=$2
        ), progress AS (
            SELECT model.started_conversation_seq AS sequence
            FROM dialogue_session JOIN agent.public_delivery_candidates candidate USING(tenant_id,conversation_id)
            JOIN agent.dialogue_publications publication ON publication.delivery_id=candidate.delivery_id
              AND publication.tenant_id=candidate.tenant_id AND publication.source_run_id=candidate.run_id
              AND publication.source_kind='model_delivery'
            JOIN runtime.model_invocations model ON model.tenant_id=candidate.tenant_id
              AND model.run_id=candidate.run_id AND model.invocation_id=candidate.invocation_id AND model.status=2
            WHERE candidate.run_id=$2 AND candidate.kind IN ('message','artifact')
              AND candidate.purpose IN ('reply','result','blocked')
              AND NOT EXISTS(SELECT 1 FROM agent.public_delivery_candidates pending
                  WHERE pending.tenant_id=candidate.tenant_id AND pending.invocation_id=candidate.invocation_id
                    AND NOT EXISTS(SELECT 1 FROM agent.dialogue_publications published
                        WHERE published.tenant_id=pending.tenant_id AND published.delivery_id=pending.delivery_id))
            UNION ALL
            SELECT record.conversation_seq AS sequence
            FROM dialogue_session JOIN approval.interactions interaction USING(tenant_id,conversation_id)
            JOIN agent.dialogue_publications publication ON publication.tenant_id=interaction.tenant_id
              AND publication.source_run_id=interaction.run_id AND publication.source_interaction_id=interaction.interaction_id
              AND publication.source_kind='interaction_owner'
            JOIN approval.interaction_responses response ON response.interaction_id=interaction.interaction_id
              AND response.responded_by=interaction.responded_by AND response.responded_at_ms=interaction.responded_at_ms
              AND response.responded_by=dialogue_session.owner_id
            JOIN event_store.ledger_records record ON record.record_id=response.response_ledger_record_id
              AND record.tenant_id=interaction.tenant_id AND record.run_id=interaction.run_id
              AND record.conversation_id=interaction.conversation_id AND record.record_kind=1
              AND record.payload_hash=response.response_ledger_payload_hash
            JOIN conversation.participants responder ON responder.tenant_id=record.tenant_id
              AND responder.conversation_id=record.conversation_id AND responder.participant_id=record.participant_id
              AND responder.kind=1 AND responder.principal_ref=dialogue_session.owner_id
            WHERE interaction.run_id=$2 AND interaction.status=2
        )"#;

pub(crate) async fn cut(
    conn: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    cutoff: ConversationSeq,
) -> Result<PublicDeliveryRepairCut, RepairStoreError> {
    let progress: Option<i64> = sqlx::query_scalar(&format!(
        "{PROGRESS_CTE} SELECT max(sequence)::bigint FROM progress WHERE sequence <= $3"
    ))
    .bind(tenant.into_uuid())
    .bind(run.into_uuid())
    .bind(i64::try_from(cutoff.get()).map_err(|_| RepairStoreError::Corrupt)?)
    .fetch_one(&mut *conn)
    .await?;
    let control = crate::control::consumed::latest(conn, tenant, run, cutoff)
        .await
        .map_err(|error| match error {
            crate::control::consumed::ConsumedControlError::Database(error) => {
                RepairStoreError::Database(error)
            }
            crate::control::consumed::ConsumedControlError::Corrupt => RepairStoreError::Corrupt,
        })?
        .map(|proof| proof.sequence);
    Ok(PublicDeliveryRepairCut {
        progress_sequence: progress.map(sequence).transpose()?,
        control_sequence: control,
    })
}

fn sequence(value: i64) -> Result<ConversationSeq, RepairStoreError> {
    let value = u64::try_from(value).map_err(|_| RepairStoreError::Corrupt)?;
    ConversationSeq::new(value).map_err(|_| RepairStoreError::Corrupt)
}

use musteragent_execution_context::{
    PublicDeliveryContract, PublicDeliveryIssue, PublicDeliveryRejectionV1,
    PublicDeliveryRepairBinding,
};
use musteragent_identity::InvocationId;
use musteragent_persistence_codec::public_delivery_repair_codec::{
    decode_public_delivery_rejection, decode_public_delivery_repair_binding,
};

pub(crate) async fn load_candidate(
    conn: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    invocation: InvocationId,
) -> Result<(PublicDeliveryIssue, PublicDeliveryRejectionV1), RepairStoreError> {
    let row = sqlx::query(
        "SELECT rejection.*,source.tenant_id,source.conversation_id,source.run_id,
                source.assistant_message_id,source.assistant_output_id,source.started_conversation_seq
         FROM runtime.model_action_rejections rejection
         JOIN runtime.model_invocations source ON source.invocation_id=rejection.invocation_id
         JOIN agent.dialogue_sessions dialogue_session ON dialogue_session.tenant_id=source.tenant_id AND dialogue_session.conversation_id=source.conversation_id
         JOIN conversation.conversations conversation ON conversation.tenant_id=source.tenant_id
           AND conversation.conversation_id=source.conversation_id AND conversation.created_by=dialogue_session.owner_id
           AND conversation.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report')
         WHERE source.tenant_id=$1 AND source.run_id=$2 AND source.invocation_id=$3 AND source.purpose=1
           AND source.status=3 AND source.terminal_error=convert_to(rejection.error_code,'UTF8')
           AND rejection.error_code='model_response_invalid' AND rejection.rejection_kind='public_delivery'
           AND rejection.delivery_contract_key IN ('musteragent.public-delivery@6','musteragent.public-delivery@7') AND rejection.repair_kind='candidate'"
    ).bind(tenant.into_uuid()).bind(run.into_uuid()).bind(invocation.into_uuid())
     .fetch_optional(&mut *conn).await?.ok_or(RepairStoreError::Corrupt)?;
    let bytes: Vec<u8> = row.try_get("repair_record")?;
    let repair = decode_public_delivery_rejection(&bytes).map_err(|_| RepairStoreError::Corrupt)?;
    let declared = PublicDeliveryContract::from_key(row.try_get("delivery_contract_key")?)
        .ok_or(RepairStoreError::Corrupt)?;
    if contract(conn, tenant, run).await? != declared {
        return Err(RepairStoreError::Corrupt);
    }
    repair
        .validate_for(declared)
        .map_err(|_| RepairStoreError::Corrupt)?;
    let proof = &repair.candidate;
    if proof.invocation_id != invocation
        || proof.assistant_message_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("assistant_message_id")?
        || proof.assistant_output_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("assistant_output_id")?
        || proof.invocation_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("repair_source_invocation_id")?
        || proof.assistant_message_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("repair_message_id")?
        || proof.assistant_output_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("repair_output_id")?
        || proof.ledger_cutoff != sequence(row.try_get("repair_cutoff")?)?
        || proof.content_ref.content_id.into_uuid()
            != row.try_get::<uuid::Uuid, _>("repair_content_id")?
        || proof.content_ref.sha256.into_bytes().as_slice()
            != row.try_get::<Vec<u8>, _>("repair_content_hash")?
        || proof.content_ref.byte_length
            != u64::try_from(row.try_get::<i64, _>("repair_content_bytes")?)
                .map_err(|_| RepairStoreError::Corrupt)?
        || repair.cut.progress_sequence
            != row
                .try_get::<Option<i64>, _>("repair_progress_sequence")?
                .map(sequence)
                .transpose()?
        || repair.cut.control_sequence
            != row
                .try_get::<Option<i64>, _>("repair_control_sequence")?
                .map(sequence)
                .transpose()?
    {
        return Err(RepairStoreError::Corrupt);
    }
    let issue = PublicDeliveryIssue::parse(row.try_get("delivery_issue")?)
        .ok_or(RepairStoreError::Corrupt)?;
    Ok((issue, repair))
}

pub(crate) async fn contract(
    conn: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
) -> Result<PublicDeliveryContract, RepairStoreError> {
    use musteragent_context::RuntimeSnapshotDecoder;
    let row = sqlx::query(
        "SELECT snapshot.encoded_snapshot,snapshot.content_hash FROM runtime.runs run
         JOIN capability.runtime_snapshots snapshot ON snapshot.runtime_snapshot_id=run.runtime_snapshot_id
           AND snapshot.tenant_id=run.tenant_id AND snapshot.run_id=run.run_id AND snapshot.conversation_id=run.conversation_id
         JOIN agent.dialogue_sessions dialogue_session ON dialogue_session.tenant_id=run.tenant_id AND dialogue_session.conversation_id=run.conversation_id
         JOIN conversation.conversations conversation ON conversation.tenant_id=run.tenant_id
           AND conversation.conversation_id=run.conversation_id AND conversation.created_by=dialogue_session.owner_id
           AND conversation.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report')
         WHERE run.tenant_id=$1 AND run.run_id=$2"
    ).bind(tenant.into_uuid()).bind(run.into_uuid()).fetch_optional(&mut *conn).await?.ok_or(RepairStoreError::Corrupt)?;
    let bytes: Vec<u8> = row.try_get("encoded_snapshot")?;
    if row.try_get::<Vec<u8>, _>("content_hash")? != Sha256::digest(&bytes).into_bytes() {
        return Err(RepairStoreError::Corrupt);
    }
    let snapshot = musteragent_persistence_codec::ProtobufSnapshotCodec
        .decode_runtime_snapshot(&bytes)
        .map_err(|_| RepairStoreError::Corrupt)?;
    snapshot
        .validate_sealed()
        .map_err(|_| RepairStoreError::Corrupt)?;
    let mut pins = snapshot
        .prompt
        .sections
        .iter()
        .filter(|p| p.section_key.as_str() == "dialogue.output-contract");
    let pin = pins.next().ok_or(RepairStoreError::Corrupt)?;
    if pins.next().is_some() {
        return Err(RepairStoreError::Corrupt);
    }
    PublicDeliveryContract::from_prompt_ref(&pin.content_ref).ok_or(RepairStoreError::Corrupt)
}

/// Read both cuts from the original owners while the caller holds the Run
/// commit lane. A changed control invalidates a plan without resetting budget.
pub(crate) async fn validate_binding(
    conn: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    binding: &PublicDeliveryRepairBinding,
    current_cutoff: ConversationSeq,
) -> Result<(), RepairStoreError> {
    binding.validate().map_err(|_| RepairStoreError::Corrupt)?;
    let sealed = contract(conn, tenant, run).await?;
    if binding.contract_ref.as_str() != sealed.key() {
        return Err(RepairStoreError::Corrupt);
    }
    let (_, repair) = load_candidate(conn, tenant, run, binding.rejection_invocation_id).await?;
    if repair
        .binding_for(sealed)
        .map_err(|_| RepairStoreError::Corrupt)?
        != *binding
    {
        return Err(RepairStoreError::Corrupt);
    }
    if cut(conn, tenant, run, current_cutoff).await? != binding.cut {
        return Err(RepairStoreError::Changed);
    }
    let latest: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT r.invocation_id FROM runtime.model_action_rejections r JOIN runtime.model_invocations i USING(invocation_id)
         WHERE i.tenant_id=$1 AND i.run_id=$2 AND i.purpose=1 AND i.status=3
           AND r.rejection_kind='public_delivery' AND i.terminal_error=convert_to(r.error_code,'UTF8')
         ORDER BY i.started_conversation_seq DESC LIMIT 1"
    ).bind(tenant.into_uuid()).bind(run.into_uuid()).fetch_optional(&mut *conn).await?;
    if latest != Some(binding.rejection_invocation_id.into_uuid()) {
        return Err(RepairStoreError::Changed);
    }
    let content = &binding.candidate.content_ref;
    let ready: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=$2 AND state=2 AND content_hash=$3 AND byte_length=$4 AND media_type=$5)")
        .bind(tenant.into_uuid()).bind(content.content_id.into_uuid()).bind(content.sha256.into_bytes().as_slice())
        .bind(i64::try_from(content.byte_length).map_err(|_|RepairStoreError::Corrupt)?).bind(content.media_type.as_str())
        .fetch_one(&mut *conn).await?;
    if !ready {
        return Err(RepairStoreError::Changed);
    }
    Ok(())
}

pub(crate) async fn stored_plan_binding(
    conn: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    invocation: InvocationId,
) -> Result<Option<PublicDeliveryRepairBinding>, RepairStoreError> {
    let row=sqlx::query("SELECT delivery_repair_binding,delivery_repair_source_invocation_id FROM runtime.context_plans WHERE tenant_id=$1 AND run_id=$2 AND invocation_id=$3")
        .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(invocation.into_uuid()).fetch_optional(&mut *conn).await?.ok_or(RepairStoreError::Corrupt)?;
    let bytes: Option<Vec<u8>> = row.try_get("delivery_repair_binding")?;
    let source: Option<uuid::Uuid> = row.try_get("delivery_repair_source_invocation_id")?;
    match (bytes, source) {
        (None, None) => Ok(None),
        (Some(bytes), Some(source)) => {
            let binding = decode_public_delivery_repair_binding(&bytes)
                .map_err(|_| RepairStoreError::Corrupt)?;
            if binding.rejection_invocation_id.into_uuid() != source {
                return Err(RepairStoreError::Corrupt);
            }
            Ok(Some(binding))
        }
        _ => Err(RepairStoreError::Corrupt),
    }
}

/// Expected binding is derived from durable episode facts, so omitting the
/// optional field cannot bypass a pending repair or an exhausted budget.
pub(crate) async fn current_binding(
    conn: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    cutoff: ConversationSeq,
) -> Result<Option<PublicDeliveryRepairBinding>, RepairStoreError> {
    let rows=sqlx::query(&format!("{PROGRESS_CTE} SELECT r.invocation_id,r.delivery_contract_key,r.repair_kind
        FROM runtime.model_action_rejections r JOIN runtime.model_invocations i USING(invocation_id)
        JOIN dialogue_session ON dialogue_session.tenant_id=i.tenant_id AND dialogue_session.run_id=i.run_id
        WHERE i.tenant_id=$1 AND i.run_id=$2 AND i.purpose=1 AND i.status=3
          AND r.rejection_kind='public_delivery' AND r.error_code='model_response_invalid'
          AND i.terminal_error=convert_to(r.error_code,'UTF8')
          AND i.started_conversation_seq>COALESCE((SELECT max(sequence) FROM progress WHERE sequence<=$3),0)
          AND i.started_conversation_seq<=$3 ORDER BY i.started_conversation_seq DESC LIMIT $4"))
        .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(i64::try_from(cutoff.get()).map_err(|_|RepairStoreError::Corrupt)?)
        .bind(i64::from(musteragent_context::MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS))
        .fetch_all(&mut *conn).await?;
    let Some(latest) = rows.first() else {
        return Ok(None);
    };
    let declared =
        PublicDeliveryContract::from_key(latest.try_get::<&str, _>("delivery_contract_key")?)
            .ok_or(RepairStoreError::Corrupt)?;
    if !matches!(
        declared,
        PublicDeliveryContract::V6 | PublicDeliveryContract::V7
    ) {
        return Ok(None);
    }
    if rows.len() >= musteragent_context::MAX_CONSECUTIVE_MODEL_ACTION_REJECTIONS as usize {
        return Err(RepairStoreError::Changed);
    }
    if latest.try_get::<Option<&str>, _>("repair_kind")? != Some("candidate") {
        return Err(RepairStoreError::Corrupt);
    }
    let invocation = InvocationId::from_uuid(latest.try_get("invocation_id")?)
        .map_err(|_| RepairStoreError::Corrupt)?;
    let (_, repair) = load_candidate(conn, tenant, run, invocation).await?;
    if repair.cut != cut(conn, tenant, run, cutoff).await? {
        return Ok(None);
    }
    Ok(Some(
        repair
            .binding_for(declared)
            .map_err(|_| RepairStoreError::Corrupt)?,
    ))
}

/// Reuse the original material source for task and answered-Interaction facts;
/// never reconstruct their roles or content from a model-authored envelope.
pub(crate) async fn authoritative_material(
    database: crate::PostgresDatabaseRuntime,
    content: std::sync::Arc<dyn musteragent_runtime_content::ContentStore>,
    decoder: std::sync::Arc<dyn musteragent_projection::LedgerPayloadDecoder>,
    request: &musteragent_runtime_model::CanonicalModelRequest,
    iteration: u32,
    applications: Option<
        std::sync::Arc<
            dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
        >,
    >,
) -> Result<musteragent_context::ContextMaterial, crate::PostgresContextMaterialError> {
    use musteragent_context::{ContextPlanInput, ContextPlanMode};
    let source = crate::PostgresContextMaterialSource::new(
        database,
        content,
        std::sync::Arc::new(musteragent_persistence_codec::ProtobufSnapshotCodec),
        decoder,
    )
    .with_application_catalogs(applications);
    // The material source uses original Attempt identity and iteration. Route
    // and continuation are model-planning concerns, not source authority.
    source
        .load_inner(&ContextPlanInput {
            mode: ContextPlanMode::ToolCapable,
            attempt: request.invocation.attempt_context.clone(),
            next_iteration: iteration,
            active_model_route_ordinal: 0,
            invocation_timing: request.invocation.timing.clone(),
            continuation_ref: None,
            continuation_source_invocation_id: None,
            consecutive_empty_responses: 0,
        })
        .await
}
