//! Deterministic schedule timing and execution policies shared by all runtimes.
#![forbid(unsafe_code)]
use chrono::{DateTime, TimeZone as _, Timelike as _, Utc};
use chrono_tz::Tz;
use croner::Cron;
use musteragent_identity::{ConversationId, UtcTimestamp, WorkspaceId};
use std::str::FromStr as _;
use thiserror::Error;
pub const MAX_CRON_EXPRESSION_BYTES: usize = 256;
pub const MAX_TIMEZONE_BYTES: usize = 128;
pub const MIN_INTERVAL_MILLIS: u64 = 60_000;
pub const MAX_INTERVAL_MILLIS: u64 = 365 * 24 * 60 * 60 * 1_000;
mod policy;
pub use policy::*;
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum ScheduleSpec {
    Once {
        #[serde(with = "timestamp")]
        at: UtcTimestamp,
    },
    Interval {
        every_millis: u64,
        #[serde(with = "timestamp")]
        anchor: UtcTimestamp,
    },
    Cron {
        expression: String,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SessionTarget {
    Current,
    Isolated,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum UnattendedApprovalPolicy {
    Supervised,
    Deny,
    Trusted,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentTurnExecution {
    pub requested_model_class: String,
    pub workspace_id: Option<WorkspaceId>,
    pub workdir: Option<String>,
    pub conversation_id: Option<ConversationId>,
    pub session_target: SessionTarget,
    pub approval_policy: UnattendedApprovalPolicy,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ExecutionSpec {
    AgentTurn(AgentTurnExecution),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
pub enum ConcurrencyPolicy {
    Skip,
    Allow,
    Replace,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
pub enum LatenessPolicy {
    CatchUp,
    Skip,
    Fail,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
pub struct RetrySpec {
    pub max_attempts: u16,
    pub base_backoff_millis: u64,
    pub factor: u16,
    pub max_backoff_millis: u64,
}

impl Default for RetrySpec {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_backoff_millis: 1_000,
            factor: 2,
            max_backoff_millis: 60_000,
        }
    }
}

pub fn initial_next_run(
    schedule: &ScheduleSpec,
    timezone: &str,
    now: UtcTimestamp,
) -> Result<Option<UtcTimestamp>, ScheduleContractError> {
    validate_schedule(schedule, timezone)?;
    match schedule {
        ScheduleSpec::Once { at } => Ok((*at > now).then_some(*at)),
        ScheduleSpec::Interval {
            every_millis,
            anchor,
        } => interval_next(*anchor, *every_millis, now).map(Some),
        ScheduleSpec::Cron { expression } => cron_next(expression, timezone, now).map(Some),
    }
}

pub fn next_run_after(
    schedule: &ScheduleSpec,
    timezone: &str,
    after: UtcTimestamp,
) -> Result<Option<UtcTimestamp>, ScheduleContractError> {
    validate_schedule(schedule, timezone)?;
    match schedule {
        ScheduleSpec::Once { .. } => Ok(None),
        ScheduleSpec::Interval {
            every_millis,
            anchor,
        } => interval_next(*anchor, *every_millis, after).map(Some),
        ScheduleSpec::Cron { expression } => cron_next(expression, timezone, after).map(Some),
    }
}

pub fn latest_run_not_after(
    schedule: &ScheduleSpec,
    timezone: &str,
    now: UtcTimestamp,
) -> Result<Option<UtcTimestamp>, ScheduleContractError> {
    validate_schedule(schedule, timezone)?;
    match schedule {
        ScheduleSpec::Once { at } => Ok((*at <= now).then_some(*at)),
        ScheduleSpec::Interval {
            every_millis,
            anchor,
        } => {
            if *anchor > now {
                return Ok(None);
            }
            let every =
                i64::try_from(*every_millis).map_err(|_| ScheduleContractError::InvalidSchedule)?;
            let elapsed = i128::from(now.unix_millis()) - i128::from(anchor.unix_millis());
            let steps = elapsed / i128::from(every);
            Ok(Some(UtcTimestamp::from_unix_millis(
                i64::try_from(i128::from(anchor.unix_millis()) + steps * i128::from(every))
                    .map_err(|_| ScheduleContractError::ScheduleOverflow)?,
            )))
        }
        ScheduleSpec::Cron { expression } => cron_previous(expression, timezone, now).map(Some),
    }
}

pub fn validate_schedule(
    schedule: &ScheduleSpec,
    timezone: &str,
) -> Result<(), ScheduleContractError> {
    validate_timezone(timezone)?;
    match schedule {
        ScheduleSpec::Once { .. } => Ok(()),
        ScheduleSpec::Interval { every_millis, .. }
            if (MIN_INTERVAL_MILLIS..=MAX_INTERVAL_MILLIS).contains(every_millis) =>
        {
            Ok(())
        }
        ScheduleSpec::Interval { .. } => Err(ScheduleContractError::InvalidSchedule),
        ScheduleSpec::Cron { expression } => {
            if expression.is_empty() || expression.len() > MAX_CRON_EXPRESSION_BYTES {
                return Err(ScheduleContractError::InvalidSchedule);
            }
            validate_timezone(timezone)?;
            Cron::from_str(expression)
                .map(|_| ())
                .map_err(|_| ScheduleContractError::InvalidSchedule)
        }
    }
}

pub fn validate_timezone(value: &str) -> Result<Tz, ScheduleContractError> {
    if value.is_empty() || value.len() > MAX_TIMEZONE_BYTES {
        return Err(ScheduleContractError::InvalidTimezone);
    }
    value
        .parse()
        .map_err(|_| ScheduleContractError::InvalidTimezone)
}

fn interval_next(
    anchor: UtcTimestamp,
    every_millis: u64,
    after: UtcTimestamp,
) -> Result<UtcTimestamp, ScheduleContractError> {
    if !(MIN_INTERVAL_MILLIS..=MAX_INTERVAL_MILLIS).contains(&every_millis) {
        return Err(ScheduleContractError::InvalidSchedule);
    }
    if after < anchor {
        return Ok(anchor);
    }
    let every = i64::try_from(every_millis).map_err(|_| ScheduleContractError::InvalidSchedule)?;
    let elapsed = i128::from(after.unix_millis()) - i128::from(anchor.unix_millis());
    let steps = elapsed / i128::from(every) + 1;
    i64::try_from(i128::from(anchor.unix_millis()) + steps * i128::from(every))
        .map(UtcTimestamp::from_unix_millis)
        .map_err(|_| ScheduleContractError::ScheduleOverflow)
}

/// Cron follows local wall time: missing times are skipped and a repeated
/// wall time fires once, at its first UTC instant. Searching a naive calendar
/// prevents the timezone library's fold choice from returning a past instant.
fn cron_next(
    expression: &str,
    timezone: &str,
    after: UtcTimestamp,
) -> Result<UtcTimestamp, ScheduleContractError> {
    let cron = Cron::from_str(expression).map_err(|_| ScheduleContractError::InvalidSchedule)?;
    let zone = validate_timezone(timezone)?;
    let mut cursor = utc(after)?
        .with_timezone(&zone)
        .naive_local()
        .and_utc()
        .with_nanosecond(0)
        .ok_or(ScheduleContractError::InvalidTimestamp)?;
    loop {
        let candidate = cron
            .find_next_occurrence(&cursor, false)
            .map_err(|_| ScheduleContractError::ScheduleUnavailable)?;
        if candidate <= cursor {
            return Err(ScheduleContractError::ScheduleUnavailable);
        }
        if let Some(instant) = zone.from_local_datetime(&candidate.naive_utc()).earliest()
            && instant.timestamp_millis() > after.unix_millis()
        {
            return Ok(UtcTimestamp::from_unix_millis(instant.timestamp_millis()));
        }
        cursor = candidate;
    }
}

fn cron_previous(
    expression: &str,
    timezone: &str,
    now: UtcTimestamp,
) -> Result<UtcTimestamp, ScheduleContractError> {
    let cron = Cron::from_str(expression).map_err(|_| ScheduleContractError::InvalidSchedule)?;
    let zone = validate_timezone(timezone)?;
    let local = utc(now)?.with_timezone(&zone).naive_local();
    let mut cursor = local
        .and_utc()
        .with_nanosecond(0)
        .ok_or(ScheduleContractError::InvalidTimestamp)?;
    // In a fold, a later local reading in the first pass can precede `now`.
    // Include that interval when searching backwards, then filter by UTC.
    if let chrono::LocalResult::Ambiguous(first, second) = zone.from_local_datetime(&local) {
        cursor = cursor
            .checked_add_signed(second.signed_duration_since(first))
            .ok_or(ScheduleContractError::ScheduleOverflow)?;
    }
    let mut inclusive = true;
    loop {
        let candidate = cron
            .find_previous_occurrence(&cursor, inclusive)
            .map_err(|_| ScheduleContractError::ScheduleUnavailable)?;
        if candidate > cursor || (!inclusive && candidate == cursor) {
            return Err(ScheduleContractError::ScheduleUnavailable);
        }
        if let Some(instant) = zone.from_local_datetime(&candidate.naive_utc()).earliest()
            && instant.timestamp_millis() <= now.unix_millis()
        {
            return Ok(UtcTimestamp::from_unix_millis(instant.timestamp_millis()));
        }
        cursor = candidate;
        inclusive = false;
    }
}

fn utc(value: UtcTimestamp) -> Result<DateTime<Utc>, ScheduleContractError> {
    Utc.timestamp_millis_opt(value.unix_millis())
        .single()
        .ok_or(ScheduleContractError::InvalidTimestamp)
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ScheduleContractError {
    #[error("invalid schedule")]
    InvalidSchedule,
    #[error("invalid IANA timezone")]
    InvalidTimezone,
    #[error("timestamp cannot be represented")]
    InvalidTimestamp,
    #[error("schedule exceeded the timestamp range")]
    ScheduleOverflow,
    #[error("schedule has no computable occurrence")]
    ScheduleUnavailable,
}
#[cfg(test)]
mod tests {
    use chrono::{Datelike as _, Timelike as _};

    use super::{ScheduleSpec, initial_next_run, next_run_after};
    use musteragent_identity::UtcTimestamp;

    #[test]
    fn interval_uses_anchor_and_never_drifts_from_now() {
        let schedule = ScheduleSpec::Interval {
            every_millis: 60_000,
            anchor: UtcTimestamp::from_unix_millis(1_000_000),
        };
        assert_eq!(
            next_run_after(&schedule, "UTC", UtcTimestamp::from_unix_millis(1_130_000))
                .expect("next interval"),
            Some(UtcTimestamp::from_unix_millis(1_180_000))
        );
    }

    #[test]
    fn cron_preserves_local_hour_across_dst_transition() {
        let schedule = ScheduleSpec::Cron {
            expression: "0 9 * * *".to_owned(),
        };
        let before_dst = UtcTimestamp::from_unix_millis(1_741_413_600_000); // 2025-03-07
        let first = initial_next_run(&schedule, "America/New_York", before_dst)
            .expect("first cron")
            .expect("first instant");
        let after_transition = UtcTimestamp::from_unix_millis(1_741_586_400_000); // 2025-03-09
        let second = initial_next_run(&schedule, "America/New_York", after_transition)
            .expect("second cron")
            .expect("second instant");
        let timezone: chrono_tz::Tz = "America/New_York".parse().expect("timezone");
        for instant in [first, second] {
            let local = chrono::DateTime::from_timestamp_millis(instant.unix_millis())
                .expect("timestamp")
                .with_timezone(&timezone);
            assert_eq!(local.hour(), 9);
            assert!(local.year() >= 2025);
        }
    }
}

mod timestamp {
    use musteragent_identity::UtcTimestamp;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(
        value: &UtcTimestamp,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(value.unix_millis())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<UtcTimestamp, D::Error> {
        Ok(UtcTimestamp::from_unix_millis(i64::deserialize(
            deserializer,
        )?))
    }
}

#[cfg(test)]
mod boundary_tests;
