use musteragent_goal::ScheduleSpec;
use musteragent_identity::{AgentId, UtcTimestamp};
use musteragent_persistence_codec::wire;
use musteragent_runtime_session::*;
use prost::Message;
use sqlx::{Row, postgres::PgRow};
type Error = OwnerScheduleError;
pub fn owner_schedule_definition(
    value: wire::OwnerScheduleDefinition,
) -> Result<OwnerScheduleDefinition, Error> {
    let timing = match value.timing.ok_or(Error::Invalid)? {
        wire::owner_schedule_definition::Timing::OnceAtUnixMs(at) => ScheduleSpec::Once {
            at: UtcTimestamp::from_unix_millis(at),
        },
        wire::owner_schedule_definition::Timing::CronExpression(expression) => {
            ScheduleSpec::Cron { expression }
        }
    };
    let result = OwnerScheduleDefinition {
        title: value.title,
        instructions: value.instructions,
        timezone: value.timezone,
        timing,
    };
    result.validate()?;
    Ok(result)
}
pub(crate) fn definition_wire(value: &OwnerScheduleDefinition) -> wire::OwnerScheduleDefinition {
    wire::OwnerScheduleDefinition {
        title: value.title.clone(),
        instructions: value.instructions.clone(),
        timezone: value.timezone.clone(),
        timing: Some(match &value.timing {
            ScheduleSpec::Once { at } => {
                wire::owner_schedule_definition::Timing::OnceAtUnixMs(at.unix_millis())
            }
            ScheduleSpec::Cron { expression } => {
                wire::owner_schedule_definition::Timing::CronExpression(expression.clone())
            }
            ScheduleSpec::Interval { .. } => unreachable!("validated owner-scoped schedule"),
        }),
    }
}
pub fn owner_schedule_wire(value: &OwnerSchedule) -> wire::OwnerSchedule {
    wire::OwnerSchedule {
        schedule_id: value.schedule_id.to_string(),
        agent_id: Some(wire::AgentId {
            value: value.agent_id.to_string(),
        }),
        revision: value.revision,
        definition: Some(definition_wire(&value.definition)),
        status: value.status.clone(),
        next_run_at_unix_ms: value.next_run_at.map(|v| v.unix_millis()),
        recent_runs: value
            .recent_runs
            .iter()
            .map(|v| wire::OwnerScheduleOccurrence {
                occurrence_id: v.occurrence_id.to_string(),
                scheduled_at_unix_ms: v.scheduled_at.unix_millis(),
                status: v.status.clone(),
                entry_id: v.entry_id.map(|id| id.to_string()),
                error_code: v.error_code.clone(),
            })
            .collect(),
    }
}
pub(super) fn decode_receipt(bytes: &[u8]) -> Result<OwnerSchedule, Error> {
    let v = wire::OwnerSchedule::decode(bytes).map_err(|_| Error::Unavailable)?;
    Ok(OwnerSchedule {
        schedule_id: v.schedule_id.parse().map_err(|_| Error::Unavailable)?,
        agent_id: v
            .agent_id
            .ok_or(Error::Unavailable)?
            .value
            .parse()
            .map_err(|_| Error::Unavailable)?,
        revision: v.revision,
        definition: owner_schedule_definition(v.definition.ok_or(Error::Unavailable)?)?,
        status: v.status,
        next_run_at: v.next_run_at_unix_ms.map(UtcTimestamp::from_unix_millis),
        recent_runs: v
            .recent_runs
            .into_iter()
            .map(|r| {
                Ok(OwnerScheduleOccurrence {
                    occurrence_id: r.occurrence_id.parse().map_err(|_| Error::Unavailable)?,
                    scheduled_at: UtcTimestamp::from_unix_millis(r.scheduled_at_unix_ms),
                    status: r.status,
                    entry_id: r
                        .entry_id
                        .map(|v| v.parse())
                        .transpose()
                        .map_err(|_| Error::Unavailable)?,
                    error_code: r.error_code,
                })
            })
            .collect::<Result<_, Error>>()?,
    })
}
pub(crate) fn row(row: &PgRow) -> Result<OwnerSchedule, Error> {
    let bytes: Vec<u8> = row.try_get("definition").map_err(db)?;
    Ok(OwnerSchedule {
        schedule_id: row.try_get("schedule_id").map_err(db)?,
        agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
            .map_err(|_| Error::Unavailable)?,
        revision: u64::try_from(row.try_get::<i64, _>("revision").map_err(db)?)
            .map_err(|_| Error::Unavailable)?,
        definition: owner_schedule_definition(
            wire::OwnerScheduleDefinition::decode(bytes.as_slice())
                .map_err(|_| Error::Unavailable)?,
        )?,
        status: row.try_get("status").map_err(db)?,
        next_run_at: row
            .try_get::<Option<i64>, _>("next_run_at_ms")
            .map_err(db)?
            .map(UtcTimestamp::from_unix_millis),
        recent_runs: vec![],
    })
}
pub(super) fn db(error: sqlx::Error) -> Error {
    tracing::warn!(code=?error.as_database_error().and_then(|v|v.code()), "Owner-scoped schedule storage failed");
    Error::Unavailable
}
pub(super) fn transaction(error: crate::PostgresTransactionError) -> Error {
    match error {
        crate::PostgresTransactionError::Database(error) => db(error),
        error => {
            tracing::warn!(failure=?error.failure_class(), "Owner-scoped schedule transaction failed");
            Error::Unavailable
        }
    }
}
