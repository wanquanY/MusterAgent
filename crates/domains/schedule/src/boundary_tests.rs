use super::*;
fn at(value: &str) -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(
        chrono::DateTime::parse_from_rfc3339(value)
            .expect("RFC3339 fixture")
            .timestamp_millis(),
    )
}
#[test]
fn gap_is_skipped_and_fold_fires_once_at_first_instant() {
    let gap = ScheduleSpec::Cron {
        expression: "30 2 * * *".into(),
    };
    assert_eq!(
        next_run_after(&gap, "America/New_York", at("2025-03-09T00:00:00Z")),
        Ok(Some(at("2025-03-10T06:30:00Z")))
    );
    let fold = ScheduleSpec::Cron {
        expression: "30 1 * * *".into(),
    };
    assert_eq!(
        next_run_after(&fold, "America/New_York", at("2025-11-02T00:00:00Z")),
        Ok(Some(at("2025-11-02T05:30:00Z")))
    );
    for cursor in [
        "2025-11-02T05:30:00Z",
        "2025-11-02T06:00:00Z",
        "2025-11-02T06:29:59Z",
    ] {
        assert_eq!(
            next_run_after(&fold, "America/New_York", at(cursor)),
            Ok(Some(at("2025-11-03T06:30:00Z")))
        );
    }
    assert_eq!(
        latest_run_not_after(&fold, "America/New_York", at("2025-11-02T06:15:00Z")),
        Ok(Some(at("2025-11-02T05:30:00Z")))
    );
}
#[test]
fn minute_schedule_never_replays_the_repeated_hour() {
    let timing = ScheduleSpec::Cron {
        expression: "* * * * *".into(),
    };
    assert_eq!(
        next_run_after(&timing, "America/New_York", at("2025-11-02T05:59:00Z")),
        Ok(Some(at("2025-11-02T07:00:00Z")))
    );
    assert_eq!(
        latest_run_not_after(&timing, "America/New_York", at("2025-11-02T06:15:00Z")),
        Ok(Some(at("2025-11-02T05:59:00Z")))
    );
}
#[test]
fn interval_invariants_cover_negative_anchors_boundaries_and_extreme_instants() {
    for anchor in [i64::MIN, -1_000_000, 0, 1_000_000, i64::MAX - 1_000_000] {
        for period in [MIN_INTERVAL_MILLIS, 61_001, MAX_INTERVAL_MILLIS] {
            let spec = ScheduleSpec::Interval {
                every_millis: period,
                anchor: UtcTimestamp::from_unix_millis(anchor),
            };
            for current in [i64::MIN, -1, 0, 999_999, i64::MAX] {
                let now = UtcTimestamp::from_unix_millis(current);
                if let Ok(Some(next)) = next_run_after(&spec, "UTC", now) {
                    assert!(next > now);
                    assert!(next.unix_millis() >= anchor);
                    assert_eq!(
                        (i128::from(next.unix_millis()) - i128::from(anchor)) % i128::from(period),
                        0
                    );
                    assert_eq!(latest_run_not_after(&spec, "UTC", next), Ok(Some(next)));
                }
                if let Ok(Some(previous)) = latest_run_not_after(&spec, "UTC", now) {
                    assert!(previous <= now);
                    assert_eq!(
                        (i128::from(previous.unix_millis()) - i128::from(anchor))
                            % i128::from(period),
                        0
                    );
                }
            }
        }
    }
}
#[test]
fn every_entrypoint_rejects_invalid_timezone_and_zero_interval() {
    let zero = ScheduleSpec::Interval {
        every_millis: 0,
        anchor: UtcTimestamp::from_unix_millis(0),
    };
    let once = ScheduleSpec::Once {
        at: UtcTimestamp::from_unix_millis(1),
    };
    for f in [initial_next_run, next_run_after, latest_run_not_after] {
        assert_eq!(
            f(&zero, "UTC", UtcTimestamp::from_unix_millis(0)),
            Err(ScheduleContractError::InvalidSchedule)
        );
        assert_eq!(
            f(&once, "Mars/Olympus", UtcTimestamp::from_unix_millis(0)),
            Err(ScheduleContractError::InvalidTimezone)
        );
    }
}

#[test]
fn cron_occurrences_do_not_inherit_fractional_polling_time() {
    let spec = ScheduleSpec::Cron {
        expression: "*/5 * * * * *".into(),
    };
    for millis in ["2025-01-01T00:00:00.001Z", "2025-01-01T00:00:00.999Z"] {
        assert_eq!(
            next_run_after(&spec, "UTC", at(millis)),
            Ok(Some(at("2025-01-01T00:00:05Z")))
        );
        assert_eq!(
            latest_run_not_after(&spec, "UTC", at(millis)),
            Ok(Some(at("2025-01-01T00:00:00Z")))
        );
    }
}
