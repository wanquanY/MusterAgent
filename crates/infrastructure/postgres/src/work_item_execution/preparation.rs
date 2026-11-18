use super::*;
use musteragent_capability::CapabilityEntitlement;
use musteragent_content_ref::Sha256;

pub(super) async fn load(
    tx: &mut PgConnection,
    request: WorkItemExecutionRequest,
    lock: bool,
    decoder: &dyn musteragent_context::RuntimeSnapshotDecoder,
) -> Result<WorkItemExecutionPreparation, DialogueStoreError> {
    require_user(request.actor)?;
    let r=sqlx::query("SELECT x.*,t.source_dialogue_id,t.source_intent_id,t.erasure_job_id,s.default_agent_id FROM collaboration.work_item_execution_reservations x JOIN collaboration.work_items t ON t.work_item_id=x.work_item_id AND t.tenant_id=x.tenant_id AND t.owner_id=x.owner_id JOIN agent.owner_spaces s ON s.space_id=x.space_id AND s.tenant_id=x.tenant_id AND s.owner_id=x.owner_id WHERE x.tenant_id=$1 AND x.owner_id=$2 AND x.work_item_id=$3 AND x.goal_revision=$4 AND x.control_revision=$5 AND x.assignment_epoch=$6")
 .bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(request.work_item_id.into_uuid()).bind(integer(request.goal_revision.get())?).bind(integer(request.control_revision.get())?).bind(integer(request.assignment_epoch.get())?).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    if r.try_get::<Option<uuid::Uuid>, _>("erasure_job_id")
        .map_err(db)?
        .is_some()
    {
        return Err(DialogueStoreError::Inactive);
    }
    macro_rules! id {
        ($t:ty,$name:literal) => {
            <$t>::from_uuid(r.try_get($name).map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?
        };
    }
    let scope = OwnedAgentWorkItemScope {
        space_id: id!(OwnerSpaceId, "space_id"),
        owner_id: request.actor.principal_id,
        agent_id: id!(AgentId, "requesting_agent_id"),
    };
    let report_row = if let Some(run) = request.report_from_run_id {
        Some(sqlx::query("SELECT * FROM collaboration.work_item_execution_reports WHERE producing_run_id=$1 AND tenant_id=$2 AND owner_id=$3 AND work_item_id=$4 AND goal_revision=$5 AND control_revision=$6 AND assignment_epoch=$7 AND requesting_agent_id=$8")
   .bind(run.into_uuid()).bind(request.actor.tenant_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(request.work_item_id.into_uuid()).bind(integer(request.goal_revision.get())?).bind(integer(request.control_revision.get())?).bind(integer(request.assignment_epoch.get())?).bind(scope.agent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?)
    } else {
        None
    };
    let target = if report_row.is_some() {
        scope.agent_id
    } else {
        id!(AgentId, "executing_agent_id")
    };
    if lock {
        lock_agents(tx, request.actor, scope, target).await?;
    }
    let work_item =
        crate::work_item_authority::load(tx, request.actor, scope, request.work_item_id, None)
            .await
            .map_err(authority_error)?
            .ok_or(DialogueStoreError::Unavailable)?;
    let fenced: bool = sqlx::query_scalar(
        "SELECT erasure_job_id IS NOT NULL FROM collaboration.work_items WHERE work_item_id=$1",
    )
    .bind(request.work_item_id.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    if fenced {
        return Err(DialogueStoreError::Inactive);
    }
    let suffix = if lock { " FOR SHARE" } else { "" };
    let ar = sqlx::query(&format!(
        "SELECT * FROM agent.owned_agents WHERE agent_id=$1 AND space_id=$2{suffix}"
    ))
    .bind(target.into_uuid())
    .bind(scope.space_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(DialogueStoreError::Unavailable)?;
    let agent = agent_row(&ar)?;
    let cr = sqlx::query(&format!(
        "SELECT * FROM agent.owned_agent_configurations WHERE agent_id=$1{suffix}"
    ))
    .bind(target.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    let configuration = config_row(&cr)?;
    let definition = crate::agent_definition::decode_definition(
        &sqlx::query(&format!(
            "SELECT * FROM agent.agent_definitions WHERE agent_id=$1{suffix}"
        ))
        .bind(target.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?,
    )
    .map_err(|_| DialogueStoreError::Corrupt)?;
    let version = crate::agent_definition::decode_version(
        &sqlx::query(crate::agent_definition::VERSION_SELECT_BY_ID)
            .bind(agent.published_version_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?,
    )
    .map_err(|_| DialogueStoreError::Corrupt)?;
    let pointer=sqlx::query(&format!("SELECT agent_version_id,published_by FROM agent.published_agent_version_pointers WHERE agent_id=$1 ORDER BY pointer_version DESC LIMIT 1{suffix}"))
 .bind(target.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::RevisionConflict)?;
    if pointer
        .try_get::<uuid::Uuid, _>("agent_version_id")
        .map_err(db)?
        != agent.published_version_id.into_uuid()
        || pointer
            .try_get::<uuid::Uuid, _>("published_by")
            .map_err(db)?
            != request.actor.principal_id.into_uuid()
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let context_row = report_row.as_ref().unwrap_or(&r);
    let context_cut = crate::dialogue_context::load_cut(
        tx,
        request.actor,
        target,
        agent.dialogue_id,
        DialogueCursor {
            epoch: unsigned(context_row.try_get("context_epoch").map_err(db)?)?,
            seq: unsigned(context_row.try_get("context_seq").map_err(db)?)?,
        },
    )
    .await?;
    if context_cut.source_chain_sha256.into_bytes().as_slice()
        != context_row
            .try_get::<Vec<u8>, _>("context_chain_sha256")
            .map_err(db)?
            .as_slice()
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let source_run_id = id!(RunId, "source_run_id");
    let source_snapshot_id = id!(RuntimeSnapshotId, "source_snapshot_id");
    let source_invocation_id = id!(InvocationId, "source_invocation_id");
    let (source_release, mut capabilities, execution_source_intent_id) = if let Some(native) =
        native_source::load(tx, request.actor, source_invocation_id).await?
    {
        if native.source.scope != scope
            || native.source.source_run_id != source_run_id
            || native.source.source_snapshot_id != source_snapshot_id
        {
            return Err(DialogueStoreError::Corrupt);
        }
        native_source::require_current(tx, &native).await?;
        (
            native.claims.product_release.clone(),
            native_source::cloud_ceiling(&native.claims)?,
            native.source.source_intent_id,
        )
    } else {
        // Delegation survives coordinator completion, never source authority
        // revocation. Reuse the original live Owned configuration/application check.
        start_source::retained_authority(
            tx,
            &WorkItemStartSource {
                actor: request.actor,
                scope,
                source_dialogue_id: id!(AgentDialogueId, "source_dialogue_id"),
                source_intent_id: id!(UserIntentId, "source_intent_id"),
                source_run_id,
                source_invocation_id,
                source_snapshot_id,
            },
            request.work_item_id,
        )
        .await?;
        // The immutable ceiling is reconstructed from the actual admitted source
        // snapshot, with exact invocation/run/owner provenance. Reservation data alone
        // cannot invent a capability or cross an Agent boundary.
        let snapshot=sqlx::query("SELECT s.source_intent_id AS execution_source_intent_id,snap.encoded_snapshot,snap.content_hash FROM capability.runtime_snapshots snap JOIN tool.tool_invocations i ON i.tenant_id=snap.tenant_id AND i.run_id=snap.run_id JOIN agent.dialogue_sessions s ON s.tenant_id=snap.tenant_id AND s.conversation_id=snap.conversation_id WHERE snap.runtime_snapshot_id=$1 AND snap.tenant_id=$2 AND snap.run_id=$3 AND i.invocation_id=$4 AND s.owner_id=$5 AND s.agent_id=$6 AND (s.subject_kind='intent' OR (s.subject_kind='work_item_report' AND s.work_item_id=$7))")
 .bind(source_snapshot_id.into_uuid()).bind(request.actor.tenant_id.into_uuid()).bind(source_run_id.into_uuid()).bind(source_invocation_id.into_uuid()).bind(request.actor.principal_id.into_uuid()).bind(scope.agent_id.into_uuid()).bind(request.work_item_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        let bytes: Vec<u8> = snapshot.try_get("encoded_snapshot").map_err(db)?;
        if Sha256::digest(&bytes).into_bytes().as_slice()
            != snapshot
                .try_get::<Vec<u8>, _>("content_hash")
                .map_err(db)?
                .as_slice()
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let source = decoder
            .decode_runtime_snapshot(&bytes)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        let capabilities = source
            .capabilities
            .iter()
            .map(|c| CapabilityEntitlement {
                key: c.key.clone(),
                provider_id: c.provider_id,
                location: c.location,
                effect: c.effect,
                allowed_scopes: c.allowed_scopes.clone(),
                policy_decision: c.policy_decision,
            })
            .collect::<Vec<_>>();
        let source_intent =
            UserIntentId::from_uuid(snapshot.try_get("execution_source_intent_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?;
        (source.product_release.clone(), capabilities, source_intent)
    };
    capabilities.sort_by(|a, b| a.key.as_str().cmp(b.key.as_str()));
    let input_refs = r
        .try_get::<Vec<Vec<u8>>, _>("delegated_content_refs")
        .map_err(db)?
        .into_iter()
        .map(content_from_bytes)
        .collect::<Result<Vec<_>, _>>()?;
    let input_attachments = inputs::delegated(
        tx,
        request.actor,
        source_run_id,
        scope.agent_id,
        execution_source_intent_id,
        &input_refs,
    )
    .await?;
    let key = WorkItemExecutionKey {
        actor: request.actor,
        scope,
        work_item_id: request.work_item_id,
        goal_revision: request.goal_revision,
        control_revision: request.control_revision,
        assignment_epoch: request.assignment_epoch,
        executing_agent_id: target,
        report_from_run_id: request.report_from_run_id,
    };
    let ids = key.identities();
    let report = if let Some(report) = report_row.as_ref() {
        if ids.command_id.into_uuid()
            != report
                .try_get::<uuid::Uuid, _>("report_command_id")
                .map_err(db)?
            || ids.conversation_id.into_uuid()
                != report
                    .try_get::<uuid::Uuid, _>("report_conversation_id")
                    .map_err(db)?
            || ids.run_id()?.into_uuid()
                != report
                    .try_get::<uuid::Uuid, _>("report_run_id")
                    .map_err(db)?
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Some(WorkItemExecutionReport {
            producing_run_id: request
                .report_from_run_id
                .ok_or(DialogueStoreError::Corrupt)?,
            producing_snapshot_id: RuntimeSnapshotId::from_uuid(
                report.try_get("producing_snapshot_id").map_err(db)?,
            )
            .map_err(|_| DialogueStoreError::Corrupt)?,
            producing_agent_id: AgentId::from_uuid(
                report.try_get("producing_agent_id").map_err(db)?,
            )
            .map_err(|_| DialogueStoreError::Corrupt)?,
            content_ref: content_from_bytes(report.try_get("report_content_ref").map_err(db)?)?,
            outcome: match report.try_get::<&str, _>("outcome").map_err(db)? {
                "completed" => WorkItemExecutionReportOutcome::Completed,
                "failed" => WorkItemExecutionReportOutcome::Failed,
                "cancelled" => WorkItemExecutionReportOutcome::Cancelled,
                _ => return Err(DialogueStoreError::Corrupt),
            },
        })
    } else {
        if ids.command_id != id!(CommandId, "command_id")
            || ids.conversation_id != id!(ConversationId, "conversation_id")
            || ids.run_id()? != id!(RunId, "run_id")
        {
            return Err(DialogueStoreError::Corrupt);
        }
        None
    };
    let dependency_results = if report.is_none() {
        dependencies::load(
            tx,
            request.actor,
            scope,
            &work_item.core.spec.dependencies,
            lock,
        )
        .await?
    } else {
        Vec::new()
    };
    let feedback = if report.is_none() && request.assignment_epoch.get() > 1 {
        requester::load_feedback(tx, request.actor.tenant_id, ids.run_id()?).await?
    } else {
        None
    };
    let result = WorkItemExecutionPreparation {
        feedback,
        dependency_results,
        report,
        key,
        work_item,
        input_attachments,
        executing_agent: agent,
        configuration,
        assistant_identity: AgentAdmissionIdentity {
            definition,
            version,
        },
        default_agent_id: id!(AgentId, "default_agent_id"),
        context_cut,
        target: if report_row.is_some() {
            WorkItemExecutionTarget::Cloud
        } else {
            device_target::load(tx, request).await?
        },
        delegation: WorkItemDelegationCeiling {
            product_release: source_release,
            source_run_id,
            source_invocation_id,
            source_snapshot_id,
            capabilities,
            input_refs,
        },
    };
    result.validate()?;
    Ok(result)
}

/// Every participant in a cross-Agent task takes the same ordered authority
/// locks; opposite partner dispatches cannot lock A then B versus B then A.
pub(crate) async fn lock_agents(
    tx: &mut PgConnection,
    actor: Actor,
    scope: OwnedAgentWorkItemScope,
    target: AgentId,
) -> Result<(), DialogueStoreError> {
    sqlx::query("SELECT space_id FROM agent.owner_spaces WHERE space_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR SHARE")
 .bind(scope.space_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let mut agents = vec![scope.agent_id.into_uuid(), target.into_uuid()];
    agents.sort();
    agents.dedup();
    let ds=sqlx::query("SELECT dialogue_id FROM agent.agent_dialogues WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=ANY($3) ORDER BY dialogue_id FOR UPDATE")
 .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(&agents).fetch_all(&mut *tx).await.map_err(db)?;
    if ds.len() != agents.len() {
        return Err(DialogueStoreError::Unavailable);
    }
    let rows=sqlx::query("SELECT agent_id FROM agent.owned_agents WHERE space_id=$1 AND agent_id=ANY($2) AND status='active' ORDER BY agent_id FOR UPDATE")
 .bind(scope.space_id.into_uuid()).bind(&agents).fetch_all(&mut *tx).await.map_err(db)?;
    if rows.len() != agents.len() {
        return Err(DialogueStoreError::Inactive);
    }
    Ok(())
}
