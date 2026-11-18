use std::collections::BTreeSet;

pub fn assert_pre_wait_parallel_events(events: &[String]) {
    assert_eq!(events.len(), 4);
    assert_eq!(
        events[..2]
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["start:parallel-a", "start:parallel-b"]),
        "both parallel Tools before the waiting barrier must start"
    );
    assert_eq!(
        events[2..],
        ["end:parallel-b", "end:parallel-a"],
        "the controlled physical completion order must remain out of model order"
    );
}

pub fn assert_mixed_batch_barriers(events: &[String]) {
    let position = |event: &str| {
        events
            .iter()
            .position(|candidate| candidate == event)
            .unwrap_or_else(|| panic!("missing provider event {event}: {events:?}"))
    };
    let first_parallel_end = position("end:parallel-a").max(position("end:parallel-b"));
    let second_parallel_start = position("start:parallel-c").min(position("start:parallel-d"));
    assert!(first_parallel_end < position("start:write-barrier"));
    assert!(position("end:write-barrier") < second_parallel_start);
    assert!(position("end:parallel-b") < position("end:parallel-a"));
    assert!(position("end:parallel-d") < position("end:parallel-c"));
}
