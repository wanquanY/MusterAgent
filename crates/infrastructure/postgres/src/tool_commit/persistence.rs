//! Durable invocation rows and terminal result storage in the caller transaction.
use super::*;

pub(super) async fn insert_invocation(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    invocation: &ToolInvocation,
    retry_contract: ToolRetryContract,
) -> Result<(), ToolCommitError> {
    let timing =
        musteragent_persistence_codec::encode_invocation_timing_columns(&invocation.timing);
    let inserted = sqlx::query(
        "INSERT INTO tool.tool_invocations (
             invocation_id, tenant_id, conversation_id, run_id,
             requested_by_attempt_id, tool_call_id, tool_ref,
             capability_provider_id, status, effect, approval_policy,
             retry_contract, idempotency_key, request_content_id,
             request_hash, deadline_ms, activity_timing, activity_timing_hash
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8,
             1, $9, $10, $11, $12, $13, $14, $15, $16, $17
         )",
    )
    .bind(invocation.invocation_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(invocation.run_id.into_uuid())
    .bind(invocation.requested_by_attempt_id.into_uuid())
    .bind(invocation.tool_call_id.as_str())
    .bind(invocation.tool_key.as_str())
    .bind(invocation.capability_provider_id.into_uuid())
    .bind(effect_code(invocation.effect))
    .bind(approval_code(invocation.approval))
    .bind(retry_code(retry_contract))
    .bind(invocation.idempotency_key.as_str())
    .bind(invocation.request_ref.content_id.into_uuid())
    .bind(invocation.request_ref.sha256.into_bytes().to_vec())
    .bind(timing.legacy_deadline_ms)
    .bind(timing.activity)
    .bind(timing.activity_hash)
    .execute(&mut *transaction)
    .await?;
    require_one(inserted.rows_affected())
}

pub(super) async fn persist_terminal(
    transaction: &mut sqlx::PgConnection,
    keys: &OpaqueRefKeyring,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    outcome: &ToolTerminalOutcome,
    finished_at: UtcTimestamp,
) -> Result<(), ToolCommitError> {
    let (status, finished_at_ms, terminal_error, terminal_reason, reconciliation_ref) =
        match outcome {
            ToolTerminalOutcome::Succeeded(_)
            | ToolTerminalOutcome::SucceededWithAuthorizedContent { .. } => {
                (4_i16, Some(finished_at.unix_millis()), None, None, None)
            }
            ToolTerminalOutcome::Failed { error, .. } => (
                5_i16,
                Some(finished_at.unix_millis()),
                Some(error.code.as_str().as_bytes().to_vec()),
                None,
                None,
            ),
            ToolTerminalOutcome::Indeterminate {
                reason,
                reconciliation_ref,
                observed_error,
                ..
            } => (
                6_i16,
                None,
                observed_error
                    .as_ref()
                    .map(|error| error.code.as_str().as_bytes().to_vec()),
                Some(reason.as_str().as_bytes().to_vec()),
                Some(protect(
                    keys,
                    invocation_id,
                    "reconciliation",
                    reconciliation_ref.as_str(),
                )?),
            ),
            ToolTerminalOutcome::Cancelled(_) => {
                (7_i16, Some(finished_at.unix_millis()), None, None, None)
            }
        };
    let updated = sqlx::query(
        "UPDATE tool.tool_invocations AS invocation
         SET status = $3, finished_at_ms = $4, terminal_error = $5,
             terminal_reason = $6, reconciliation_ref_protected = $7,
             indeterminate_observed_at_ms = CASE WHEN $3 = 6 THEN $8
                 ELSE indeterminate_observed_at_ms END
         WHERE tenant_id = $1 AND invocation_id = $2
           AND (
               status = 3
               OR (
                   status = 1
                   AND $3 = 5
                   AND execution_attempt_id IS NULL
                   AND interaction_id IS NULL
               )
               OR (
                   status = 6
                   AND retry_contract = 3
                   AND $3 <> 6
               )
               OR (
                   status = 2
                   AND EXISTS (
                       SELECT 1
                       FROM approval.interactions AS interaction
                       LEFT JOIN approval.interaction_responses AS response
                         ON response.interaction_id = interaction.interaction_id
                       WHERE interaction.interaction_id = invocation.interaction_id
                         AND (
                             (interaction.status = 2
                              AND response.response_kind = 1
                              AND response.approval_decision = 2)
                             OR interaction.status IN (3, 4)
                         )
                   )
               )
           )",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(status)
    .bind(finished_at_ms)
    .bind(terminal_error)
    .bind(terminal_reason)
    .bind(reconciliation_ref)
    .bind(finished_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    require_one(updated.rows_affected())?;
    let result = outcome.retained_result();
    if let Some(result) = result {
        authorize_content(transaction, tenant_id, &result.result_ref).await?;
        let provider = result
            .provider_request_ref
            .as_ref()
            .map(|value| protect(keys, invocation_id, "provider-request", value.as_str()))
            .transpose()?;
        sqlx::query(
            "INSERT INTO tool.tool_results (
                 invocation_id, result_content_id, result_hash,
                 result_schema_key, result_schema_hash,
                 provider_request_ref_protected, committed_at_ms
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (invocation_id) DO UPDATE SET
                 result_content_id = EXCLUDED.result_content_id, result_hash = EXCLUDED.result_hash,
                 result_schema_key = EXCLUDED.result_schema_key, result_schema_hash = EXCLUDED.result_schema_hash,
                 provider_request_ref_protected = EXCLUDED.provider_request_ref_protected,
                 committed_at_ms = EXCLUDED.committed_at_ms",
        )
        .bind(invocation_id.into_uuid())
        .bind(result.result_ref.content_id.into_uuid())
        .bind(result.result_hash.into_bytes().to_vec())
        .bind(result.result_schema.schema_key.as_str())
        .bind(result.result_schema.schema_hash.into_bytes().to_vec())
        .bind(provider)
        .bind(finished_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE tool.tool_invocations SET result_content_id = $3
             WHERE tenant_id = $1 AND invocation_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .bind(result.result_ref.content_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }
    if let ToolTerminalOutcome::SucceededWithAuthorizedContent {
        authorized_content, ..
    } = outcome
    {
        for content in authorized_content {
            authorize_content(transaction, tenant_id, content).await?;
        }
    }
    crate::dialogue_publication::interaction::memory_tool_changed(
        transaction,
        tenant_id,
        invocation_id,
    )
    .await?;
    Ok(())
}
