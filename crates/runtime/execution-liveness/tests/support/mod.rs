use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use musteragent_content_ref::Sha256;
use musteragent_execution_liveness::{
    ActivityObservation, ClockSample, Command, Event, ExecutionLiveness, LocalInstant,
    ObservationOrigin, Transition,
};
use musteragent_identity::{
    AttemptId, FencingToken, HostClockEpochId, InvocationId, RunId, TenantId, UtcTimestamp,
    VersionedKey,
};
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, BusinessDeadline, ExecutionBinding, ExecutionLivenessPolicy,
    ExecutionLivenessWindows, ExecutionScope,
};

pub fn positive(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("positive test scalar")
}

pub fn policy(idle_ms: u64, sources: &[ActivitySource]) -> Arc<ExecutionLivenessPolicy> {
    Arc::new(
        ExecutionLivenessPolicy::new(
            VersionedKey::new("test.execution-liveness@1").expect("versioned key"),
            ExecutionLivenessWindows {
                startup: Duration::from_millis(20),
                idle: Duration::from_millis(idle_ms),
                max_delivery_lag: Duration::from_millis(10),
                checkpoint_interval: Duration::from_millis(1),
                observation_request: Duration::from_millis(1),
                cleanup: Duration::from_millis(10),
                post_exit_drain: Duration::from_millis(10),
            },
            sources.iter().copied(),
        )
        .expect("valid test policy"),
    )
}

pub struct Rig {
    pub binding: ExecutionBinding,
    pub epoch: HostClockEpochId,
}

impl Rig {
    pub fn new() -> Self {
        Self {
            binding: ExecutionBinding {
                tenant_id: TenantId::generate(),
                run_id: RunId::generate(),
                scope: ExecutionScope::Invocation(InvocationId::generate()),
                operation_identity: Sha256::digest(b"original-physical-operation"),
                owner_attempt_id: AttemptId::generate(),
                owner_fence: FencingToken::new(1).expect("fence"),
                producer_generation: positive(1),
            },
            epoch: HostClockEpochId::generate(),
        }
    }

    pub fn at(&self, ms: u64) -> LocalInstant {
        LocalInstant {
            epoch: self.epoch,
            elapsed: Duration::from_millis(ms),
        }
    }

    pub fn admitted(
        &self,
        policy: Arc<ExecutionLivenessPolicy>,
        deadline: Option<BusinessDeadline>,
    ) -> ExecutionLiveness {
        ExecutionLiveness::new(
            self.binding,
            policy,
            deadline,
            ClockSample {
                monotonic: self.at(0),
                utc: UtcTimestamp::from_unix_millis(0),
            },
        )
        .expect("valid dispatch")
    }

    pub fn starting(&self) -> ExecutionLiveness {
        self.admitted(
            policy(100, &[ActivitySource::Stdout, ActivitySource::Stderr]),
            None,
        )
    }

    pub fn running(&self) -> ExecutionLiveness {
        self.apply(
            &self.starting(),
            0,
            Event::Started {
                observed_at: self.at(0),
            },
        )
        .next
    }

    pub fn command(&self, state: &ExecutionLiveness, ms: u64, event: Event) -> Command {
        Command {
            binding: self.binding,
            expected_revision: state.revision(),
            now: self.at(ms),
            event,
        }
    }

    pub fn apply(&self, state: &ExecutionLiveness, ms: u64, event: Event) -> Transition {
        state
            .reduce(self.command(state, ms, event))
            .expect("valid transition")
    }

    pub fn observation(&self, ms: u64, sequence: u64, work: u64) -> ActivityObservation {
        ActivityObservation {
            source: ActivitySource::Stdout,
            sequence: positive(sequence),
            work_cursor: positive(work),
            observed_at: self.at(ms),
            origin: ObservationOrigin::Live,
        }
    }
}
