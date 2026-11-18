//! Current Goal/source projection for the interactive Tool owner.
use super::*;
use serde_json::{Value, json};

pub(crate) async fn read(
    tx: &mut PgConnection,
    actor: Actor,
    agent: AgentId,
    id: GoalId,
) -> Result<(StandingGoal, Vec<OwnerEventSubscription>, Value), Error> {
    super::authority::scope(tx, actor, agent, false, false).await?;
    let goal = record::goal(tx, actor, agent, id).await?;
    let sources = record::list(tx, &goal).await?;
    let mut reads = record::ApplicationReadSet::new();
    let mut values = Vec::with_capacity(sources.len());
    for source in &sources {
        let current = match record::current_application(tx, &mut reads, source).await {
            Ok(binding) => source.authorize(binding).is_ok(),
            Err(Error::Forbidden | Error::NotFound | Error::Conflict) => false,
            Err(error) => return Err(error),
        };
        values.push(item(source, current)?);
    }
    let value = page(&goal, values);
    Ok((goal, sources, value))
}
pub(crate) fn page(goal: &StandingGoal, sources: Vec<Value>) -> Value {
    json!({"goal_id":goal.goal_id.to_string(),"goal_revision":goal.revision.to_string(),
        "status":match goal.status { StandingGoalStatus::Draft=>"draft",StandingGoalStatus::Active=>"active",StandingGoalStatus::Paused=>"paused",StandingGoalStatus::Achieved=>"achieved",StandingGoalStatus::Failed=>"failed",StandingGoalStatus::Cancelled=>"cancelled" },"sources":sources})
}
pub(crate) fn item(
    value: &OwnerEventSubscription,
    application_current: bool,
) -> Result<Value, Error> {
    value.validate()?;
    let readiness = if !application_current {
        "authorization_required"
    } else {
        match value.source_status.as_ref().map(|v| v.readiness) {
            None => "preparing",
            Some(SourceReadiness::Ready) => "ready",
            Some(SourceReadiness::Retrying) => "retrying",
            Some(SourceReadiness::AuthorizationRequired) => "authorization_required",
            Some(SourceReadiness::CoverageGap) => "coverage_gap",
            Some(SourceReadiness::Retired) => "retired",
        }
    };
    Ok(json!({
        "subscription_id":value.subscription_id.to_string(),
        "application_binding_id":value.configuration.application_binding_id.to_string(),
        "capability_ref":value.configuration.capability_ref.as_str(),"capability_sha256":value.configuration.capability_digest.to_string(),
        "selection_json":String::from_utf8(value.configuration.selection.json_bytes()).map_err(|_|Error::Corrupt)?,
        "event_kinds":value.configuration.event_kinds.iter().map(|v|v.as_str()).collect::<Vec<_>>(),
        "required":value.required,"revision":value.guard.revision.get().to_string(),"activation_epoch":value.guard.activation_epoch.get().to_string(),
        "desired":match value.desired {EventSubscriptionIntent::Watching=>"watching",EventSubscriptionIntent::Paused=>"paused",EventSubscriptionIntent::Ended=>"ended"},
        "boundary_ready":application_current && value.boundary.is_some() && readiness=="ready",
        "readiness":readiness,"source_acknowledged_at_unix_ms":value.source_acknowledged_at.map(|v|v.unix_millis())
    }))
}
