use super::*;
use musteragent_runtime_contract::execution_liveness::{
    ExecutionOperationKind, execution_operation_identity,
};
pub(super) struct Operation {
    pub kind: ExecutionOperationKind,
    pub request_hash: Sha256,
    pub tool_key: Option<String>,
    pub terminal: bool,
}
pub(super) async fn operation(
    tx: &mut sqlx::PgConnection,
    binding: ExecutionBinding,
) -> Result<Operation, StoreError> {
    let (_, invocation) = scope_parts(binding.scope);
    let rows=sqlx::query("SELECT 1 AS kind,request_hash,tool_ref AS tool_key,(status IN (4,5,7)) AS terminal FROM tool.tool_invocations WHERE tenant_id=$1 AND run_id=$2 AND invocation_id=$3 UNION ALL SELECT CASE WHEN assistant_message_id IS NOT NULL THEN 2 ELSE 3 END,request_hash,NULL,(status!=1) FROM runtime.model_invocations WHERE tenant_id=$1 AND run_id=$2 AND invocation_id=$3")
        .bind(binding.tenant_id.into_uuid()).bind(binding.run_id.into_uuid()).bind(invocation.into_uuid()).fetch_all(&mut *tx).await?;
    if rows.len() != 1 {
        return Err(StoreError::Invalid);
    }
    let row = &rows[0];
    let kind = match row.try_get::<i32, _>("kind")? {
        1 => ExecutionOperationKind::Tool,
        2 => ExecutionOperationKind::Model,
        3 => ExecutionOperationKind::AuxiliaryModel,
        _ => return Err(StoreError::Corrupt),
    };
    Ok(Operation {
        kind,
        request_hash: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("request_hash")?)
            .map_err(|_| StoreError::Corrupt)?,
        tool_key: row.try_get("tool_key")?,
        terminal: row.try_get("terminal")?,
    })
}
pub(super) async fn validate(
    journal: &PostgresFencedExecutionLivenessJournal,
    tx: &mut sqlx::PgConnection,
    state: &CommitLaneState,
    owner: &AttemptContext,
    admission: &ExecutionAdmission,
) -> Result<(), StoreError> {
    let binding = admission.binding;
    let source = operation(tx, binding).await?;
    if matches!(binding.scope, ExecutionScope::RunHandoff { .. }) != source.terminal {
        return Err(StoreError::Invalid);
    }
    let (profile, hash) = run_profile(journal, tx, state, owner).await?;
    let policy = match binding.scope {
        ExecutionScope::RunHandoff { .. } => profile.handoff_policy(),
        ExecutionScope::Invocation(_) => match source.kind {
            ExecutionOperationKind::Tool => {
                let key = source.tool_key.as_deref().ok_or(StoreError::Invalid)?;
                if let Some(private) = private_tool(journal, tx, state, owner, key).await? {
                    private.liveness.as_ref().clone()
                } else {
                    let (snapshot, hash) = snapshot(journal, tx, state, owner).await?;
                    let tool = match snapshot
                        .tools
                        .iter()
                        .find(|tool| tool.manifest.key.as_str() == key)
                    {
                        Some(tool) => tool,
                        None => journal.prepared_tool(hash, key)?,
                    };
                    tool.manifest
                        .execution_policy
                        .liveness()
                        .ok_or(StoreError::Invalid)?
                        .as_ref()
                        .clone()
                }
            }
            ExecutionOperationKind::Model | ExecutionOperationKind::AuxiliaryModel => {
                profile.model_policy()
            }
        },
    };
    if matches!(binding.scope, ExecutionScope::Invocation(_))
        && source.kind == ExecutionOperationKind::Tool
    {
        let (_, invocation) = scope_parts(binding.scope);
        let row = sqlx::query("SELECT deadline_ms,activity_timing,activity_timing_hash FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2")
            .bind(state.tenant_id.into_uuid()).bind(invocation.into_uuid()).fetch_one(&mut *tx).await?;
        let timing = musteragent_persistence_codec::decode_invocation_timing_columns(
            musteragent_persistence_codec::EncodedInvocationTiming {
                legacy_deadline_ms: row.try_get("deadline_ms")?,
                activity: row.try_get("activity_timing")?,
                activity_hash: row.try_get("activity_timing_hash")?,
            },
        )
        .map_err(|_| StoreError::Corrupt)?;
        if !matches!(timing, musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(ref value)
            if &value.policy == policy.reference() && value.business_deadline == admission.business_deadline)
        {
            return Err(StoreError::Invalid);
        }
    }
    if admission.policy.as_ref() != &policy
        || binding.operation_identity
            != execution_operation_identity(
                state.tenant_id,
                owner.run_id,
                binding.scope,
                source.kind,
                hash,
                source.request_hash,
            )
    {
        return Err(StoreError::Invalid);
    }
    Ok(())
}

pub(super) fn validate_initial(
    state: &CommitLaneState,
    owner: &AttemptContext,
    admission: &ExecutionAdmission,
) -> Result<(), StoreError> {
    let binding = admission.binding;
    validate_owner(binding, state, owner)?;
    if binding.producer_generation.get() != 1 || admission.business_deadline.is_some() {
        return Err(StoreError::Invalid);
    }
    Ok(())
}

pub(super) async fn snapshot_bytes(
    tx: &mut sqlx::PgConnection,
    state: &CommitLaneState,
    owner: &AttemptContext,
) -> Result<(String, Vec<u8>, Sha256), StoreError> {
    let row=sqlx::query("SELECT snapshot.schema_version,snapshot.encoded_snapshot,snapshot.content_hash FROM runtime.runs run JOIN capability.runtime_snapshots snapshot ON snapshot.runtime_snapshot_id=run.runtime_snapshot_id AND snapshot.tenant_id=run.tenant_id AND snapshot.run_id=run.run_id WHERE run.tenant_id=$1 AND run.run_id=$2")
        .bind(state.tenant_id.into_uuid()).bind(owner.run_id.into_uuid()).fetch_one(&mut *tx).await?;
    let bytes: Vec<u8> = row.try_get("encoded_snapshot")?;
    let hash = Sha256::digest(&bytes);
    if row.try_get::<Vec<u8>, _>("content_hash")? != hash.into_bytes() {
        return Err(StoreError::Corrupt);
    }
    Ok((row.try_get("schema_version")?, bytes, hash))
}
pub(super) async fn run_profile(
    journal: &PostgresFencedExecutionLivenessJournal,
    tx: &mut sqlx::PgConnection,
    state: &CommitLaneState,
    owner: &AttemptContext,
) -> Result<(musteragent_runtime_snapshot::RunLivenessPolicyV1, Sha256), StoreError> {
    let (schema, bytes, hash) = snapshot_bytes(tx, state, owner).await?;
    let policy = if let Some(policy) =
        musteragent_persistence_codec::decode_private_run_execution_policy(&schema, &bytes)
            .map_err(|_| StoreError::Corrupt)?
    {
        policy
    } else {
        let snapshot = journal
            .snapshots
            .decode_runtime_snapshot(&bytes)
            .map_err(|_| StoreError::Corrupt)?;
        snapshot
            .validate_liveness_schema()
            .map_err(|_| StoreError::Invalid)?;
        snapshot.execution_policy
    };
    Ok((policy.liveness.ok_or(StoreError::Invalid)?, hash))
}
pub(super) async fn snapshot(
    journal: &PostgresFencedExecutionLivenessJournal,
    tx: &mut sqlx::PgConnection,
    state: &CommitLaneState,
    owner: &AttemptContext,
) -> Result<(musteragent_runtime_snapshot::RuntimeSnapshotContent, Sha256), StoreError> {
    let (_, bytes, hash) = snapshot_bytes(tx, state, owner).await?;
    let snapshot = journal
        .snapshots
        .decode_runtime_snapshot(&bytes)
        .map_err(|_| StoreError::Corrupt)?;
    Ok((snapshot, hash))
}

/// Resolve the original Tool intent against the frozen Snapshot, before any
/// start clock or effect exists. The caller owns the existing Tool transaction.
pub(super) async fn tool_intent(
    journal: &PostgresFencedExecutionLivenessJournal,
    tx: &mut sqlx::PgConnection,
    state: &CommitLaneState,
    owner: &AttemptContext,
    invocation: &musteragent_tool::ToolInvocation,
    retry_contract: musteragent_tool::ToolRetryContract,
) -> Result<ExecutionAdmission, StoreError> {
    use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
    let InvocationExecutionTiming::ActivityV1(_) = &invocation.timing else {
        return Err(StoreError::Invalid);
    };
    let (_, _, snapshot_hash) = snapshot_bytes(tx, state, owner).await?;
    let policy = if let Some(private) =
        private_tool(journal, tx, state, owner, invocation.tool_key.as_str()).await?
    {
        if private.provider_id != invocation.capability_provider_id
            || private.retry != retry_contract
            || invocation.effect != musteragent_policy::Effect::Read
            || invocation.approval != musteragent_policy::ApprovalRequirement::Never
        {
            return Err(StoreError::Invalid);
        }
        private.liveness
    } else {
        let (snapshot, hash) = snapshot(journal, tx, state, owner).await?;
        snapshot
            .validate_liveness_schema()
            .map_err(|_| StoreError::Invalid)?;
        let tool = match snapshot
            .tools
            .iter()
            .find(|tool| tool.manifest.key == invocation.tool_key)
        {
            Some(tool) => tool,
            None => journal.prepared_tool(hash, invocation.tool_key.as_str())?,
        };
        if tool.manifest.capability_provider_id != invocation.capability_provider_id
            || tool.manifest.retry_contract != retry_contract
            || tool.manifest.effect != invocation.effect
            || tool.manifest.approval != invocation.approval
        {
            return Err(StoreError::Invalid);
        }
        tool.manifest
            .execution_policy
            .liveness()
            .ok_or(StoreError::Invalid)?
            .clone()
    };
    bind_tool(state, owner, invocation, snapshot_hash, policy)
}

fn bind_tool(
    state: &CommitLaneState,
    owner: &AttemptContext,
    invocation: &musteragent_tool::ToolInvocation,
    snapshot_hash: Sha256,
    policy: Arc<musteragent_runtime_contract::execution_liveness::ExecutionLivenessPolicy>,
) -> Result<ExecutionAdmission, StoreError> {
    let musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(
        timing,
    ) = &invocation.timing
    else {
        return Err(StoreError::Invalid);
    };
    // A source label alone is not proof of a Job/user deadline. This path
    // remains closed until the original admission freezes that control.
    if timing.policy != *policy.reference()
        || timing.business_deadline.is_some()
        || invocation.run_id != owner.run_id
    {
        return Err(StoreError::Invalid);
    }
    let scope = ExecutionScope::Invocation(invocation.invocation_id);
    Ok(ExecutionAdmission {
        binding: ExecutionBinding {
            tenant_id: state.tenant_id,
            run_id: owner.run_id,
            scope,
            operation_identity: execution_operation_identity(
                state.tenant_id,
                owner.run_id,
                scope,
                ExecutionOperationKind::Tool,
                snapshot_hash,
                invocation.request_ref.sha256,
            ),
            owner_attempt_id: owner.attempt_id,
            owner_fence: owner.fencing_token,
            producer_generation: std::num::NonZeroU64::new(1).expect("first generation"),
        },
        policy,
        business_deadline: None,
    })
}

pub(super) async fn private_tool(
    journal: &PostgresFencedExecutionLivenessJournal,
    tx: &mut sqlx::PgConnection,
    state: &CommitLaneState,
    owner: &AttemptContext,
    key: &str,
) -> Result<Option<musteragent_runtime_snapshot::PrivateToolPolicy>, StoreError> {
    let (schema, bytes, _) = snapshot_bytes(tx, state, owner).await?;
    let evaluation_run: bool = sqlx::query_scalar(
        "SELECT activity_kind=6 FROM runtime.runs WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(state.tenant_id.into_uuid())
    .bind(owner.run_id.into_uuid())
    .fetch_one(&mut *tx)
    .await?;
    musteragent_persistence_codec::decode_private_tool_policy(
        &schema,
        &bytes,
        key,
        evaluation_run,
        journal.snapshots.as_ref(),
    )
    .map_err(|_| StoreError::Invalid)
}

/// Terminal custody uses the already-admitted immutable policy and original
/// invocation identity, never a new executable lookup or dispatch permission.
pub(super) async fn retained_tool_intent(
    tx: &mut sqlx::PgConnection,
    state: &CommitLaneState,
    owner: &AttemptContext,
    invocation: &musteragent_tool::ToolInvocation,
    prior: &ExecutionRecord,
) -> Result<ExecutionAdmission, StoreError> {
    let source = operation(tx, prior.binding()).await?;
    if source.kind != ExecutionOperationKind::Tool
        || source.tool_key.as_deref() != Some(invocation.tool_key.as_str())
        || source.request_hash != invocation.request_ref.sha256
        || prior.binding().scope != ExecutionScope::Invocation(invocation.invocation_id)
    {
        return Err(StoreError::Invalid);
    }
    let (_, _, hash) = snapshot_bytes(tx, state, owner).await?;
    let admission = bind_tool(
        state,
        owner,
        invocation,
        hash,
        Arc::new(prior.policy().clone()),
    )?;
    if admission.binding.operation_identity != prior.binding().operation_identity {
        return Err(StoreError::Invalid);
    }
    Ok(admission)
}
