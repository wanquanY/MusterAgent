//! Shared logical control plan. Preview is read-only; the same owner replans
//! under its original locks and commits alongside the calling Tool receipt.
use super::control::intent;
use super::*;
fn control_hash(request: &ControlOwnerEventSubscription) -> Sha256 {
    let mut fields = vec![request.expected_goal_revision.get().to_be_bytes().to_vec()];
    match &request.control {
        EventSubscriptionControl::Prepare {
            configuration,
            required,
        } => {
            fields.extend([
                vec![1, u8::from(*required)],
                configuration.fingerprint().into_bytes().to_vec(),
            ]);
        }
        EventSubscriptionControl::Change {
            expected,
            desired,
            configuration,
        } => {
            fields.extend([
                vec![2, intent(*desired)],
                expected.revision.get().to_be_bytes().to_vec(),
                expected.activation_epoch.get().to_be_bytes().to_vec(),
                configuration
                    .as_ref()
                    .map(|c| c.fingerprint().into_bytes().to_vec())
                    .unwrap_or_default(),
            ]);
        }
        EventSubscriptionControl::SetRequired { expected, required } => {
            fields.extend([
                vec![3, u8::from(*required)],
                expected.revision.get().to_be_bytes().to_vec(),
                expected.activation_epoch.get().to_be_bytes().to_vec(),
            ]);
        }
    }
    fingerprint(
        &request.identity,
        request.goal_id,
        request.subscription_id,
        &fields,
    )
}

struct Plan {
    next: OwnerEventSubscription,
    changed: bool,
    fresh: Option<(uuid::Uuid, StandingGoal, bool)>,
}
async fn plan(
    tx: &mut PgConnection,
    request: &ControlOwnerEventSubscription,
) -> Result<Plan, Error> {
    let identity = &request.identity;
    if let Some(receipt) = replay(tx, identity, CONTROL, control_hash(request)).await? {
        return Ok(Plan {
            next: subscription_receipt(
                receipt,
                identity,
                request.goal_id,
                request.subscription_id,
            )?,
            changed: false,
            fresh: None,
        });
    }
    let withdrawal = matches!(
        &request.control,
        EventSubscriptionControl::Change {
            desired: EventSubscriptionIntent::Paused | EventSubscriptionIntent::Ended,
            configuration: None,
            ..
        }
    );
    let (space, goal) = record::locked_scope(tx, identity, request.goal_id, withdrawal).await?;
    if goal.revision != request.expected_goal_revision.get()
        || goal.status.terminal() && !withdrawal
    {
        return Err(Error::Conflict);
    }
    let sources = record::list(tx, &goal).await?;
    let previous = sources
        .iter()
        .find(|s| s.subscription_id == request.subscription_id);
    let physical = !matches!(
        &request.control,
        EventSubscriptionControl::SetRequired { .. }
    );
    let next = match &request.control {
        EventSubscriptionControl::Prepare {
            configuration,
            required,
        } => {
            if previous.is_some() || sources.len() >= MAX_GOAL_EVENT_SUBSCRIPTIONS {
                return Err(Error::Conflict);
            }
            let binding = record::application(
                tx,
                identity.subject(),
                configuration.application_binding_id,
                true,
            )
            .await?;
            OwnerEventSubscription::prepare(
                request.subscription_id,
                request.goal_id,
                identity.subject(),
                configuration.clone(),
                *required,
                &binding,
                identity.changed_at,
            )?
        }
        EventSubscriptionControl::Change {
            expected,
            desired,
            configuration,
        } => {
            let previous = previous.ok_or(Error::NotFound)?;
            let id = configuration
                .as_ref()
                .unwrap_or(&previous.configuration)
                .application_binding_id;
            let binding = record::application(tx, identity.subject(), id, !withdrawal).await?;
            previous.change(
                *expected,
                *desired,
                configuration.clone(),
                &binding,
                identity.changed_at,
            )?
        }
        EventSubscriptionControl::SetRequired { expected, required } => previous
            .ok_or(Error::NotFound)?
            .set_required(*expected, *required, identity.changed_at)?,
    };
    Ok(Plan {
        changed: previous != Some(&next),
        next,
        fresh: Some((space, goal, physical)),
    })
}
pub(crate) async fn preview(
    tx: &mut PgConnection,
    request: &ControlOwnerEventSubscription,
) -> Result<OwnerEventSubscription, Error> {
    Ok(plan(tx, request).await?.next)
}
pub(crate) async fn commit(
    tx: &mut PgConnection,
    request: &ControlOwnerEventSubscription,
    expected: Option<&OwnerEventSubscription>,
) -> Result<OwnerEventSubscription, Error> {
    let plan = plan(tx, request).await?;
    if expected.is_some_and(|value| value != &plan.next) {
        return Err(Error::Conflict);
    }
    if let Some((space, goal, physical)) = plan.fresh {
        if plan.changed {
            record::store(tx, space, &plan.next).await?;
            record::publish(tx, &plan.next, &goal, physical, request.identity.changed_at).await?;
        }
        save_receipt(
            tx,
            &request.identity,
            CONTROL,
            control_hash(request),
            wire::owner_command_receipt::Result::EventSubscription(
                codec::subscription(&plan.next).map_err(|_| Error::Corrupt)?,
            ),
        )
        .await?;
        crate::agent_work_creations::readiness::mark_ready(tx, &goal, request.identity.changed_at)
            .await?;
    }
    Ok(plan.next)
}
