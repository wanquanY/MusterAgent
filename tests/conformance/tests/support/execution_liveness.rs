//! A committed-checkpoint crash fixture shared by both real Home databases.
//! Admission is tested separately; this fixture starts at an already sealed row.
use musteragent_content_ref::Sha256;
use musteragent_execution_liveness::{
    ClockSample, ExecutionLiveness, ExecutionRecord, LocalInstant,
};
use musteragent_identity::{HostClockEpochId, InvocationId, TenantId, UtcTimestamp, VersionedKey};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, ExecutionBinding, ExecutionLivenessPolicy, ExecutionLivenessWindows,
    ExecutionScope,
};
use std::{num::NonZeroU64, sync::Arc, time::Duration};

pub fn record(tenant: TenantId, owner: &AttemptContext) -> ExecutionRecord {
    let windows = ExecutionLivenessWindows {
        startup: Duration::from_millis(100),
        idle: Duration::from_millis(1000),
        max_delivery_lag: Duration::from_millis(100),
        checkpoint_interval: Duration::from_millis(25),
        observation_request: Duration::from_millis(100),
        cleanup: Duration::from_millis(100),
        post_exit_drain: Duration::from_millis(50),
    };
    let policy = ExecutionLivenessPolicy::new(
        VersionedKey::new("test.committed-liveness@1").expect("valid committed liveness fixture"),
        windows,
        [ActivitySource::Stdout],
    )
    .expect("valid committed liveness fixture");
    let binding = ExecutionBinding {
        tenant_id: tenant,
        run_id: owner.run_id,
        scope: ExecutionScope::Invocation(InvocationId::generate()),
        operation_identity: Sha256::digest(b"original admitted operation"),
        owner_attempt_id: owner.attempt_id,
        owner_fence: owner.fencing_token,
        producer_generation: NonZeroU64::new(1).expect("valid committed liveness fixture"),
    };
    let now = ClockSample {
        monotonic: LocalInstant {
            epoch: HostClockEpochId::generate(),
            elapsed: Duration::ZERO,
        },
        utc: UtcTimestamp::from_unix_millis(0),
    };
    ExecutionLiveness::new(binding, Arc::new(policy), None, now)
        .expect("valid committed liveness fixture")
        .checkpoint(now)
        .expect("valid committed liveness fixture")
        .into()
}
pub fn clock(record: &ExecutionRecord, millis: u64) -> ClockSample {
    let ExecutionRecord::Checkpoint(checkpoint) = record else {
        panic!("initial checkpoint")
    };
    ClockSample {
        monotonic: LocalInstant {
            epoch: checkpoint.saved_at().monotonic.epoch,
            elapsed: Duration::from_millis(millis),
        },
        utc: UtcTimestamp::from_unix_millis(
            i64::try_from(millis).expect("valid committed liveness fixture"),
        ),
    }
}

pub struct ManualHomeClock {
    current: std::sync::Mutex<ClockSample>,
    samples: std::sync::atomic::AtomicUsize,
}
impl ManualHomeClock {
    pub fn new(value: ClockSample) -> Self {
        Self {
            current: std::sync::Mutex::new(value),
            samples: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    pub fn set(&self, value: ClockSample) {
        *self.current.lock().expect("test clock lock") = value;
    }
    pub fn samples(&self) -> usize {
        self.samples.load(std::sync::atomic::Ordering::SeqCst)
    }
}
impl musteragent_execution_liveness::LivenessHomeClock for ManualHomeClock {
    fn sample(&self) -> Result<ClockSample, musteragent_execution_liveness::LivenessError> {
        self.samples
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(*self.current.lock().expect("test clock lock"))
    }
}
/// Only a test convenience: the real Journal API accepts no caller clock.
pub struct ClockedJournal<J> {
    pub inner: J,
    pub clock: Arc<ManualHomeClock>,
}
impl<J> std::ops::Deref for ClockedJournal<J> {
    type Target = J;
    fn deref(&self) -> &J {
        &self.inner
    }
}
impl<J: musteragent_execution_liveness::FencedExecutionLivenessJournal<Owner = AttemptContext>>
    ClockedJournal<J>
{
    pub async fn mutate(
        &self,
        owner: &AttemptContext,
        scope: ExecutionScope,
        revision: u64,
        mutation: musteragent_execution_liveness::RecordMutation,
        now: ClockSample,
    ) -> Result<
        musteragent_execution_liveness::RecordTransition,
        musteragent_execution_liveness::LivenessJournalError,
    > {
        self.clock.set(now);
        self.inner.mutate(owner, scope, revision, mutation).await
    }
}
