use super::*;

pub(super) fn intent(value: EventSubscriptionIntent) -> u8 {
    match value {
        EventSubscriptionIntent::Watching => 1,
        EventSubscriptionIntent::Paused => 2,
        EventSubscriptionIntent::Ended => 3,
    }
}

impl PostgresOwnerEventSourceStore {
    pub(super) async fn control_inner(
        &self,
        request: ControlOwnerEventSubscription,
    ) -> Result<OwnerEventSubscription, Error> {
        let mut tx = self.write().await?;
        let result = super::tool_control::commit(&mut tx, &request, None).await?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        Ok(result)
    }

    pub(super) async fn observe_inner(
        &self,
        request: ObserveOwnerEventSource,
    ) -> Result<OwnerEventSubscription, Error> {
        let fields = match &request.fact {
            EventSourceFact::Activated(value) => {
                vec![vec![1], codec::boundary(value).encode_to_vec()]
            }
            EventSourceFact::Readiness {
                activation_epoch,
                observation,
            } => vec![
                vec![2],
                activation_epoch.get().to_be_bytes().to_vec(),
                codec::readiness(observation).encode_to_vec(),
            ],
            EventSourceFact::Released {
                activation_epoch,
                desired,
            } => vec![
                vec![3, intent(*desired)],
                activation_epoch.get().to_be_bytes().to_vec(),
            ],
        };
        let identity = &request.identity;
        let hash = fingerprint(identity, request.goal_id, request.subscription_id, &fields);
        let mut tx = self.write().await?;
        if let Some(receipt) = replay(&mut tx, identity, OBSERVE, hash).await? {
            let result =
                subscription_receipt(receipt, identity, request.goal_id, request.subscription_id)?;
            tx.commit().await.map_err(|_| Error::Infrastructure)?;
            return Ok(result);
        }
        let release = matches!(request.fact, EventSourceFact::Released { .. });
        let (space, goal) =
            record::locked_scope(&mut tx, identity, request.goal_id, release).await?;
        let sources = record::list(&mut tx, &goal).await?;
        let source = sources
            .iter()
            .find(|source| source.subscription_id == request.subscription_id)
            .ok_or(Error::NotFound)?;
        let next = match request.fact {
            EventSourceFact::Activated(boundary) => {
                if !matches!(
                    goal.status,
                    StandingGoalStatus::Draft | StandingGoalStatus::Active
                ) {
                    return Err(Error::NotReady);
                }
                let binding = record::application(
                    &mut tx,
                    identity.subject(),
                    source.configuration.application_binding_id,
                    true,
                )
                .await?;
                source.activate(boundary, &binding, identity.changed_at)?
            }
            EventSourceFact::Readiness {
                activation_epoch,
                observation,
            } => source.observe_readiness(activation_epoch, observation, identity.changed_at)?,
            EventSourceFact::Released {
                activation_epoch,
                desired,
            } => source.acknowledge_release(activation_epoch, desired, identity.changed_at)?,
        };
        if *source != next {
            record::store(&mut tx, space, &next).await?;
            record::publish(&mut tx, &next, &goal, false, identity.changed_at).await?;
        }
        save_receipt(
            &mut tx,
            identity,
            OBSERVE,
            hash,
            wire::owner_command_receipt::Result::EventSubscription(
                codec::subscription(&next).map_err(|_| Error::Corrupt)?,
            ),
        )
        .await?;
        crate::agent_work_creations::readiness::mark_ready(&mut tx, &goal, identity.changed_at)
            .await?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        Ok(next)
    }
}
