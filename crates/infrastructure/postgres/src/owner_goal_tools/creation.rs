//! Shared Tool argument mapping to the original account Goal creation command.
use super::*;

pub(crate) async fn prepare(
    conn: &mut sqlx::PgConnection,
    actor: Actor,
    agent_id: AgentId,
    intent_id: UserIntentId,
    dialogue_id: AgentDialogueId,
    bytes: &[u8],
    now: UtcTimestamp,
) -> Result<PreparedOwnerGoalDraft, Error> {
    let definition: Definition =
        serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
    definition.validate()?;
    let row = sqlx::query("SELECT a.space_id,a.dialogue_id FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id)
            WHERE a.agent_id=$1 AND a.dialogue_id=$2 AND s.tenant_id=$3 AND s.owner_id=$4 AND a.status='active'")
            .bind(agent_id.into_uuid()).bind(dialogue_id.into_uuid()).bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid()).fetch_optional(&mut *conn).await.map_err(|_|Error::Infrastructure)?.ok_or(Error::Forbidden)?;
    let command_id = creation_command_id(intent_id, &definition.action_key)?;
    let goal_id = GoalId::from_uuid(uuid::Uuid::new_v5(&command_id.into_uuid(), b"owner-goal@1"))
        .map_err(|_| Error::Corrupt)?;
    let scope = OwnerGoalScope {
        tenant_id: actor.tenant_id,
        owner_id: actor.principal_id,
        agent_id,
        goal_id,
        space_id: OwnerSpaceId::from_uuid(row.try_get("space_id").map_err(|_| Error::Corrupt)?)
            .map_err(|_| Error::Corrupt)?,
        dialogue_id: AgentDialogueId::from_uuid(
            row.try_get("dialogue_id").map_err(|_| Error::Corrupt)?,
        )
        .map_err(|_| Error::Corrupt)?,
    };
    let prepared = CreateOwnerGoal {
        actor,
        command_id,
        binding: OwnerGoalBinding {
            scope,
            revision: 1,
            requesting_agent_id: agent_id,
            source_dialogue_id: scope.dialogue_id,
        },
        title: definition.title,
        objective: definition.instructions.into_bytes(),
        constraints: definition.constraints,
        completion: GoalCompletionContract {
            risk_class: if definition.high_risk {
                GoalRiskClass::HighRisk
            } else {
                GoalRiskClass::Standard
            },
            success_criteria: definition.success_criteria,
            evidence_requirements: definition.evidence_requirements,
            failure_conditions: definition.failure_conditions,
            minimum_evidence_items: 1,
        },
        occurred_at: now,
    }
    .prepare()
    .map_err(creation_error)?;
    Ok(prepared)
}

pub(crate) fn result(
    receipt: &OwnerGoalDraftReceipt,
    goal_id: GoalId,
    title: &str,
) -> serde_json::Value {
    serde_json::json!({"goal_id":goal_id.to_string(), "creation_revision":1, "creation_status":"draft", "title":title,
        "source_configuration_required":true, "creation_receipt_id":receipt.command_id.to_string(), "created_at_unix_ms":receipt.created_at.unix_millis()})
}
