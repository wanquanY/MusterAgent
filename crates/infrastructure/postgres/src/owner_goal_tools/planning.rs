//! One account Goal plan shared by Cloud and native command transports.
use super::*;
use musteragent_content_ref::ContentRef;
use musteragent_goal::{GoalPursuit, StandingGoal, StandingGoalStatus};
use musteragent_runtime_automation::{
    ChangeStandingGoalStatus, ReplaceStandingGoal, StandingGoalDefinition,
    StandingGoalProductApplication,
};
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Get {
    #[serde(deserialize_with = "goal_id")]
    goal_id: GoalId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct List {
    limit: u16,
    include_finished: bool,
    cursor: Option<Cursor>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    through_unix_ms: i64,
    before_created_at_unix_ms: i64,
    before_id: uuid::Uuid,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Control {
    #[serde(deserialize_with = "goal_id")]
    goal_id: GoalId,
    expected_revision: u64,
    expected_pursuit_revision: Option<u64>,
    action: Action,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Start,
    Pause,
    Resume,
    Stop,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Update {
    #[serde(deserialize_with = "goal_id")]
    goal_id: GoalId,
    expected_revision: u64,
    title: String,
    instructions: String,
}

#[derive(Clone, Copy)]
pub(crate) struct GoalToolScope {
    pub actor: Actor,
    pub agent_id: AgentId,
    /// A background Goal can inspect only its own root and cannot change policy.
    pub own_goal: Option<GoalId>,
}
pub(crate) struct GoalToolPlanner {
    pub database: PostgresDatabaseRuntime,
    pub content: Arc<dyn ContentStore>,
}
pub(crate) struct PreparedGoalTool {
    pub value: Value,
    pub authorized_content: Vec<ContentRef>,
    pub effect: Option<super::effect::GoalToolEffect>,
}
impl GoalToolPlanner {
    async fn page(
        &self,
        scope: GoalToolScope,
        reference: Option<GoalId>,
        list: List,
    ) -> Result<AgentWorkPage, Error> {
        if !(1..=50).contains(&list.limit) || (reference.is_some() && list.cursor.is_some()) {
            return Err(Error::InvalidArguments);
        }
        crate::PostgresAgentWorkQuery::new(self.database.clone())
            .read_for_goal_tools(ReadAgentWork {
                actor: scope.actor,
                agent_id: scope.agent_id,
                kind: Some(AgentWorkKind::Goal),
                category: None,
                include_finished: list.include_finished,
                limit: list.limit,
                reference: reference.map(|id| AgentWorkReference {
                    kind: AgentWorkKind::Goal,
                    id: id.into_uuid(),
                    agent_id: scope.agent_id,
                }),
                cursor: list.cursor.map(|c| AgentWorkCursor {
                    through: UtcTimestamp::from_unix_millis(c.through_unix_ms),
                    before_created_at: UtcTimestamp::from_unix_millis(c.before_created_at_unix_ms),
                    before_kind: AgentWorkKind::Goal,
                    before_id: c.before_id,
                }),
            })
            .await
            .map_err(store_error)
    }

    async fn owned(
        &self,
        scope: GoalToolScope,
        id: GoalId,
    ) -> Result<(StandingGoal, Option<GoalPursuit>), Error> {
        let page = self
            .page(
                scope,
                Some(id),
                List {
                    limit: 1,
                    include_finished: true,
                    cursor: None,
                },
            )
            .await?;
        match page.records.into_iter().next() {
            Some(AgentWorkRecord::Goal { value, pursuit }) => Ok((value, pursuit)),
            _ => Err(Error::Forbidden),
        }
    }

    pub async fn prepare(
        &self,
        scope: GoalToolScope,
        key: &str,
        bytes: &[u8],
        now: UtcTimestamp,
    ) -> Result<PreparedGoalTool, Error> {
        let own_goal = scope.own_goal;
        if own_goal.is_some() && !matches!(key, "goal.get@3" | "goal.list@2") {
            return Err(Error::Forbidden);
        }
        if key == "goal.draft.save@1" {
            let request: super::draft_registration::Request =
                serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
            let (goal, _) = self.owned(scope, request.goal_id).await?;
            let registration = super::draft_registration::prepare(
                &self.database,
                &goal,
                request.expected_revision,
            )
            .await?;
            return Ok(PreparedGoalTool {
                value: registration.result(),
                authorized_content: Vec::new(),
                effect: Some(super::effect::GoalToolEffect::RegisterDraft(registration)),
            });
        }
        let (value, authorized_content, prepared) = match key {
            "goal.get@3" => {
                let request: Get =
                    serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
                if own_goal.is_some_and(|id| id != request.goal_id) {
                    return Err(Error::Forbidden);
                }
                let (goal, pursuit) = self.owned(scope, request.goal_id).await?;
                (
                    json!({"goal":goal_json(&goal,pursuit.as_ref())?}),
                    vec![goal.objective_ref],
                    None,
                )
            }
            "goal.list@2" => {
                let request: List =
                    serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
                let page = self.page(scope, own_goal, request).await?;
                let mut goals = Vec::new();
                let mut refs = Vec::new();
                for item in page.records {
                    let AgentWorkRecord::Goal { value, pursuit } = item else {
                        return Err(Error::Corrupt);
                    };
                    goals.push(goal_json(&value, pursuit.as_ref())?);
                    refs.push(value.objective_ref);
                }
                (
                    json!({"goals":goals,"total_count":page.total_count,"next_cursor":page.next.map(|c|json!({
                        "through_unix_ms":c.through.unix_millis(),"before_created_at_unix_ms":c.before_created_at.unix_millis(),"before_id":c.before_id,
                    }))}),
                    refs,
                    None,
                )
            }
            "goal.change_status@2" => {
                let request: Control =
                    serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
                let (goal, _) = self.owned(scope, request.goal_id).await?;
                // Start and resume are deliberately distinct user intentions.
                match request.action {
                    Action::Start if goal.status != StandingGoalStatus::Draft => {
                        return Err(Error::Conflict);
                    }
                    Action::Resume if goal.status != StandingGoalStatus::Paused => {
                        return Err(Error::Conflict);
                    }
                    _ => {}
                }
                let prepared = self
                    .application()
                    .prepare_status(ChangeStandingGoalStatus {
                        tenant_id: scope.actor.tenant_id,
                        principal_id: scope.actor.principal_id,
                        goal_id: request.goal_id,
                        expected_goal_revision: request.expected_revision,
                        expected_pursuit_revision: request.expected_pursuit_revision,
                        new_pursuit_id: None,
                        initial_run_id: None,
                        initial_message_id: None,
                        status: match request.action {
                            Action::Start | Action::Resume => StandingGoalStatus::Active,
                            Action::Pause => StandingGoalStatus::Paused,
                            Action::Stop => StandingGoalStatus::Cancelled,
                        },
                        occurred_at: now,
                    })
                    .await
                    .map_err(product_error)?;
                (
                    json!({"goal":goal_json(&prepared.inspection.goal,prepared.inspection.pursuit.as_ref())?}),
                    Vec::new(),
                    Some(prepared),
                )
            }
            "goal.update@3" => {
                let request: Update =
                    serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)?;
                let (goal, _) = self.owned(scope, request.goal_id).await?;
                let prepared = self
                    .application()
                    .prepare_replace(ReplaceStandingGoal {
                        tenant_id: scope.actor.tenant_id,
                        principal_id: scope.actor.principal_id,
                        goal_id: request.goal_id,
                        expected_revision: request.expected_revision,
                        occurred_at: now,
                        definition: StandingGoalDefinition {
                            mode: goal.mode,
                            target_agent_id: goal.target_agent_id,
                            conversation_id: goal.conversation_id,
                            title: request.title,
                            objective: request.instructions.into_bytes(),
                            constraints: goal.constraints,
                            completion: goal.completion,
                            budget: goal.budget,
                        },
                    })
                    .await
                    .map_err(product_error)?;
                (
                    json!({"goal":goal_json(&prepared.inspection.goal,prepared.inspection.pursuit.as_ref())?}),
                    Vec::new(),
                    Some(prepared),
                )
            }
            _ => return Err(Error::InvalidArguments),
        };

        Ok(PreparedGoalTool {
            value,
            authorized_content,
            effect: prepared.map(|change| super::effect::GoalToolEffect::Change(Box::new(change))),
        })
    }
    fn application(&self) -> StandingGoalProductApplication {
        StandingGoalProductApplication::new(
            Arc::new(crate::PostgresStandingGoalProductStore::new(
                self.database.clone(),
            )),
            self.content.clone(),
        )
    }
}
fn goal_json(goal: &StandingGoal, pursuit: Option<&GoalPursuit>) -> Result<Value, Error> {
    Ok(
        json!({"goal_id":goal.goal_id.to_string(),"title":goal.title,"revision":goal.revision,
        "status":match goal.status {StandingGoalStatus::Draft=>"draft",StandingGoalStatus::Active=>"active",StandingGoalStatus::Paused=>"paused",StandingGoalStatus::Achieved=>"achieved",StandingGoalStatus::Failed=>"failed",StandingGoalStatus::Cancelled=>"cancelled"},
        "instructions_content_id":goal.objective_ref.content_id.to_string(),
        "pursuit_revision":pursuit.map(|p|p.revision),"updated_at_unix_ms":goal.updated_at.unix_millis()}),
    )
}

pub(super) fn goal_id<'de, D: serde::Deserializer<'de>>(d: D) -> Result<GoalId, D::Error> {
    GoalId::from_uuid(uuid::Uuid::deserialize(d)?).map_err(serde::de::Error::custom)
}
