//! Independent source tools share original account/Goal control, immutable
//! command replay and the original Cloud/Native Tool receipt transaction.
use super::*;
use musteragent_goal::event_subscription::*;
use musteragent_runtime_session::owner_event_sources::*;
use serde_json::Value;
mod cloud;
mod input;

pub(crate) const KEYS: [&str; 4] = [
    "goal.sources.catalog@1",
    "goal.sources.resources@1",
    "goal.sources.configure@1",
    "goal.sources.list@1",
];
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceScope {
    pub subject: OwnerGoalSourceSubject,
    pub intent_id: UserIntentId,
}
pub(crate) struct SourcePlanner {
    pub database: PostgresDatabaseRuntime,
    pub selection: Option<Arc<dyn OwnerGoalSourceSelection>>,
}
pub(crate) struct PreparedSourceTool {
    pub value: Value,
    pub change: Option<(ControlOwnerEventSubscription, OwnerEventSubscription)>,
}
impl SourcePlanner {
    fn selection(&self) -> Result<&dyn OwnerGoalSourceSelection, Error> {
        self.selection.as_deref().ok_or(Error::Infrastructure)
    }
    pub async fn prepare(
        &self,
        scope: SourceScope,
        key: &str,
        bytes: &[u8],
        at: UtcTimestamp,
    ) -> Result<PreparedSourceTool, Error> {
        if scope.subject.surface == DialogueInputSurface::Schedule {
            return Err(Error::Forbidden);
        }
        if bytes.len() > 64 * 1024 {
            return Err(Error::InvalidArguments);
        }
        match key {
            "goal.sources.catalog@1" => {
                let _: input::Empty = decode(bytes)?;
                let result = self
                    .selection()?
                    .catalog(scope.subject)
                    .await
                    .map_err(store_error)?;
                Ok(PreparedSourceTool {
                    value: decode(&result)?,
                    change: None,
                })
            }
            "goal.sources.resources@1" => {
                let input: input::Resources = decode(bytes)?;
                input::cursor(input.cursor.as_deref())?;
                let selector = input.selector()?;
                let result = self
                    .selection()?
                    .resources(scope.subject, selector, input.cursor)
                    .await
                    .map_err(store_error)?;
                Ok(PreparedSourceTool {
                    value: decode(&result)?,
                    change: None,
                })
            }
            "goal.sources.list@1" => {
                let input: input::Goal = decode(bytes)?;
                let id = GoalId::from_uuid(input::id(&input.goal_id)?)
                    .map_err(|_| Error::InvalidArguments)?;
                let mut tx = self
                    .database
                    .read_unit_of_work(
                        musteragent_telemetry::DbCommitKind::Projection,
                        crate::PostgresTransactionIsolation::RepeatableRead,
                    )
                    .await
                    .map_err(|_| Error::Infrastructure)?;
                let (_, _, value) = crate::owner_event_sources::tool_read::read(
                    &mut tx,
                    scope.subject.actor,
                    scope.subject.agent_id,
                    id,
                )
                .await
                .map_err(store_error)?;
                tx.commit().await.map_err(|_| Error::Infrastructure)?;
                Ok(PreparedSourceTool {
                    value,
                    change: None,
                })
            }
            "goal.sources.configure@1" => self.configure(scope, decode(bytes)?, at).await,
            _ => Err(Error::InvalidArguments),
        }
    }
    async fn configure(
        &self,
        scope: SourceScope,
        input: input::Configure,
        at: UtcTimestamp,
    ) -> Result<PreparedSourceTool, Error> {
        let request = input.command(&scope, at)?;
        let store = crate::PostgresOwnerEventSourceStore::new(self.database.clone());
        // A retained command is compared by its original owner before any new
        // provider I/O. Lost replies remain recoverable after App withdrawal.
        let replay = store
            .read_control_receipt(scope.subject.actor, request.identity.command_id)
            .await
            .map_err(store_error)?;
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Projection,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| Error::Infrastructure)?;
        let (goal, sources, _) = crate::owner_event_sources::tool_read::read(
            &mut tx,
            scope.subject.actor,
            scope.subject.agent_id,
            request.goal_id,
        )
        .await
        .map_err(store_error)?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        if replay.is_none() {
            let configuration = match &request.control {
                EventSubscriptionControl::Prepare { configuration, .. } => {
                    Some(configuration.clone())
                }
                EventSubscriptionControl::Change {
                    configuration: Some(configuration),
                    ..
                } => Some(configuration.clone()),
                EventSubscriptionControl::Change {
                    desired: EventSubscriptionIntent::Watching,
                    configuration: None,
                    ..
                } => Some(
                    sources
                        .iter()
                        .find(|s| s.subscription_id == request.subscription_id)
                        .ok_or(Error::SourceRevisionConflict)?
                        .configuration
                        .clone(),
                ),
                _ => None,
            };
            if let Some(configuration) = configuration {
                self.selection()?
                    .qualify(scope.subject, configuration)
                    .await
                    .map_err(store_error)?;
            }
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(|_| Error::Infrastructure)?;
        let expected = crate::owner_event_sources::tool_control::preview(&mut tx, &request)
            .await
            .map_err(store_error)?;
        // Preview takes the original locks but writes no record or event.
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        let mut value = crate::owner_event_sources::tool_read::page(
            &goal,
            vec![
                crate::owner_event_sources::tool_read::item(&expected, true)
                    .map_err(store_error)?,
            ],
        );
        value["receipt_kind"] = Value::String("historical_command_receipt".into());
        Ok(PreparedSourceTool {
            value,
            change: Some((request, expected)),
        })
    }
}
pub(crate) async fn commit(
    tx: &mut sqlx::PgConnection,
    plan: PreparedSourceTool,
) -> Result<(), Error> {
    if let Some((request, expected)) = plan.change {
        crate::owner_event_sources::tool_control::commit(tx, &request, Some(&expected))
            .await
            .map_err(store_error)?;
    }
    Ok(())
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(bytes).map_err(|_| Error::InvalidArguments)
}
fn source_error(error: EventSubscriptionError) -> Error {
    store_error(EventSourceStoreError::Domain(error))
}
fn store_error(error: EventSourceStoreError) -> Error {
    match error {
        EventSourceStoreError::NotFound | EventSourceStoreError::Forbidden => Error::Forbidden,
        EventSourceStoreError::Conflict | EventSourceStoreError::NotReady => {
            Error::SourceRevisionConflict
        }
        EventSourceStoreError::IdempotencyConflict => Error::Conflict,
        EventSourceStoreError::Corrupt => Error::Corrupt,
        EventSourceStoreError::Infrastructure => Error::Infrastructure,
        EventSourceStoreError::Domain(error) => match error {
            EventSubscriptionError::InvalidConfiguration => Error::InvalidArguments,
            EventSubscriptionError::SubjectMismatch | EventSubscriptionError::AuthorityChanged => {
                Error::Forbidden
            }
            _ => Error::SourceRevisionConflict,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_revision_and_original_command_conflicts_keep_distinct_meanings() {
        assert!(matches!(
            store_error(EventSourceStoreError::Conflict),
            Error::SourceRevisionConflict
        ));
        assert!(matches!(
            store_error(EventSourceStoreError::NotReady),
            Error::SourceRevisionConflict
        ));
        assert!(matches!(
            store_error(EventSourceStoreError::IdempotencyConflict),
            Error::Conflict
        ));
        assert!(matches!(
            store_error(EventSourceStoreError::Infrastructure),
            Error::Infrastructure
        ));
    }
}
