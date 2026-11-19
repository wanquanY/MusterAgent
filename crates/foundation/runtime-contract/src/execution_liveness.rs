//! Execution-liveness contracts and canonical immutable policy identity.
//! They never reinterpret a released Tool deadline or Runtime Snapshot.

use std::num::NonZeroU64;
use std::time::Duration;

use musteragent_content_ref::Sha256;
use musteragent_identity::{
    AttemptId, FencingToken, InvocationId, RunId, RunSeq, TenantId, UtcTimestamp, VersionedKey,
};
use thiserror::Error;

/// A Child uses its own RunId; no parent/sibling activity shares this scope.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ExecutionScope {
    Invocation(InvocationId),
    /// Supervises the gap after a completed invocation, not the whole Run.
    RunHandoff {
        previous_invocation: InvocationId,
    },
}

/// The original journal that owns an invocation. Values are canonical v1 tags.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ExecutionOperationKind {
    Tool,
    Model,
    AuxiliaryModel,
}

/// Seal an original request to its tenant, Run, scope, and frozen Snapshot.
/// Reconciliation keeps this digest and producer generation across owners.
#[must_use]
pub fn execution_operation_identity(
    tenant: TenantId,
    run: RunId,
    scope: ExecutionScope,
    kind: ExecutionOperationKind,
    snapshot_hash: Sha256,
    request_hash: Sha256,
) -> Sha256 {
    let mut bytes = b"musteragent.execution-operation.v1\0".to_vec();
    bytes.extend_from_slice(tenant.into_uuid().as_bytes());
    bytes.extend_from_slice(run.into_uuid().as_bytes());
    let (tag, invocation) = match scope {
        ExecutionScope::Invocation(id) => (1, id),
        ExecutionScope::RunHandoff {
            previous_invocation,
        } => (2, previous_invocation),
    };
    bytes.push(tag);
    bytes.extend_from_slice(invocation.into_uuid().as_bytes());
    bytes.push(match kind {
        ExecutionOperationKind::Tool => 1,
        ExecutionOperationKind::Model => 2,
        ExecutionOperationKind::AuxiliaryModel => 3,
    });
    bytes.extend_from_slice(&snapshot_hash.into_bytes());
    bytes.extend_from_slice(&request_hash.into_bytes());
    Sha256::digest(bytes)
}

/// Identity of one physical execution under one authoritative owner.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ExecutionBinding {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub scope: ExecutionScope,
    /// Digest of the original operation/reconciliation identity, never a new
    /// side-effect identity minted by an observer or a polling request.
    pub operation_identity: Sha256,
    pub owner_attempt_id: AttemptId,
    pub owner_fence: FencingToken,
    pub producer_generation: NonZeroU64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LivenessPolicyRef {
    pub key: VersionedKey,
    pub digest: Sha256,
}

/// Frozen execution timing carried by the original invocation. New activity
/// calls have no hidden wall-clock lifetime. An explicit business deadline
/// stays separate from both the idle policy and authorization expiry.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InvocationExecutionTiming {
    LegacyFixed { deadline: UtcTimestamp },
    ActivityV1(ActivityExecutionTiming),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ActivityExecutionTiming {
    pub policy: LivenessPolicyRef,
    pub business_deadline: Option<BusinessDeadline>,
}

impl InvocationExecutionTiming {
    /// Legacy adapters must explicitly reject None. It is never an infinite
    /// timestamp, nor permission to reinterpret idle as a fixed deadline.
    #[must_use]
    pub const fn legacy_deadline(&self) -> Option<UtcTimestamp> {
        match self {
            Self::LegacyFixed { deadline } => Some(*deadline),
            Self::ActivityV1(_) => None,
        }
    }
}

/// Only facts from a task-bound observer qualify. A connection ping, PID check,
/// lease renewal, or model-authored heartbeat has no variant here.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ActivitySource {
    Stdout,
    Stderr,
    CanonicalModelEvent,
    StructuredProgress,
    CommittedCheckpoint,
    ExecutorWork,
}

impl ActivitySource {
    pub const COUNT: usize = 6;
    /// Canonical order used by policy v1. Append-only changes require a new
    /// policy encoding version; enum declaration order is not wire authority.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Stdout,
        Self::Stderr,
        Self::CanonicalModelEvent,
        Self::StructuredProgress,
        Self::CommittedCheckpoint,
        Self::ExecutorWork,
    ];

    /// Internal fixed-array index, not a protocol discriminator.
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::Stdout => 0,
            Self::Stderr => 1,
            Self::CanonicalModelEvent => 2,
            Self::StructuredProgress => 3,
            Self::CommittedCheckpoint => 4,
            Self::ExecutorWork => 5,
        }
    }
}

/// Independent windows, not an accumulated execution-time budget. All values
/// are exact positive u64 milliseconds in the portable v1 contract.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ExecutionLivenessWindows {
    pub startup: Duration,
    pub idle: Duration,
    pub max_delivery_lag: Duration,
    pub checkpoint_interval: Duration,
    pub observation_request: Duration,
    pub cleanup: Duration,
    pub post_exit_drain: Duration,
}

impl ExecutionLivenessWindows {
    /// Product defaults for one call. The caller selects the silence window;
    /// none of these windows limits the accumulated duration of active work.
    /// Adapters may supply a fully explicit profile instead.
    #[must_use]
    pub fn for_idle_window(idle: Duration) -> Self {
        let unit = Duration::from_millis((idle.as_millis() / 10).clamp(1, 10_000) as u64);
        Self {
            startup: idle,
            idle,
            checkpoint_interval: unit.min(Duration::from_secs(1)),
            max_delivery_lag: (unit * 2).min(Duration::from_secs(5)),
            observation_request: unit.min(Duration::from_secs(10)),
            cleanup: Duration::from_secs(5),
            post_exit_drain: Duration::from_secs(10),
        }
    }

    /// Canonical v1 field order, also shared by validation and hashing.
    #[must_use]
    pub const fn ordered(self) -> [Duration; 7] {
        [
            self.startup,
            self.idle,
            self.max_delivery_lag,
            self.checkpoint_interval,
            self.observation_request,
            self.cleanup,
            self.post_exit_drain,
        ]
    }

    pub fn validate(self) -> Result<Self, LivenessPolicyError> {
        for value in self.ordered() {
            if value.is_zero() {
                return Err(LivenessPolicyError::ZeroWindow);
            }
            if value.subsec_nanos() % 1_000_000 != 0 || u64::try_from(value.as_millis()).is_err() {
                return Err(LivenessPolicyError::NonPortableWindow);
            }
        }
        if self.max_delivery_lag >= self.idle {
            return Err(LivenessPolicyError::DeliveryLagExceedsIdleWindow);
        }
        let margin = self
            .checkpoint_interval
            .checked_add(self.max_delivery_lag)
            .and_then(|value| value.checked_add(self.observation_request))
            .ok_or(LivenessPolicyError::InsufficientObservationMargin)?;
        if margin >= self.idle {
            return Err(LivenessPolicyError::InsufficientObservationMargin);
        }
        if self.checkpoint_interval >= self.max_delivery_lag {
            return Err(LivenessPolicyError::InsufficientDeliveryMargin);
        }
        Ok(self)
    }
}

/// An immutable capability profile pins both windows and acceptable evidence.
/// The digest is computed here, never supplied by an unverified caller. There
/// is deliberately no default or accumulated execution-time limit.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionLivenessPolicy {
    reference: LivenessPolicyRef,
    windows: ExecutionLivenessWindows,
    accepted_sources: [bool; ActivitySource::COUNT],
}

impl ExecutionLivenessPolicy {
    pub fn new(
        key: VersionedKey,
        windows: ExecutionLivenessWindows,
        sources: impl IntoIterator<Item = ActivitySource>,
    ) -> Result<Self, LivenessPolicyError> {
        let windows = windows.validate()?;
        let mut accepted_sources = [false; ActivitySource::COUNT];
        for source in sources {
            accepted_sources[source.index()] = true;
        }
        // Empty is intentional: finite silent-only capabilities cannot renew.
        let digest = Sha256::digest(canonical_policy_bytes(&key, windows, accepted_sources));
        Ok(Self {
            reference: LivenessPolicyRef { key, digest },
            windows,
            accepted_sources,
        })
    }

    /// Transport-independent canonical v1 bytes, excluding the digest itself.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        canonical_policy_bytes(&self.reference.key, self.windows, self.accepted_sources)
    }

    #[must_use]
    pub fn reference(&self) -> &LivenessPolicyRef {
        &self.reference
    }

    #[must_use]
    pub const fn windows(&self) -> ExecutionLivenessWindows {
        self.windows
    }

    #[must_use]
    pub const fn startup_window(&self) -> Duration {
        self.windows.startup
    }

    #[must_use]
    pub const fn idle_window(&self) -> Duration {
        self.windows.idle
    }

    #[must_use]
    pub const fn max_delivery_lag(&self) -> Duration {
        self.windows.max_delivery_lag
    }

    #[must_use]
    pub const fn cleanup_window(&self) -> Duration {
        self.windows.cleanup
    }

    #[must_use]
    pub const fn accepts(&self, source: ActivitySource) -> bool {
        self.accepted_sources[source.index()]
    }
}

fn canonical_policy_bytes(
    key: &VersionedKey,
    windows: ExecutionLivenessWindows,
    sources: [bool; ActivitySource::COUNT],
) -> Vec<u8> {
    let mut bytes = b"musteragent.execution-liveness-policy.v1\0".to_vec();
    let key = key.as_str().as_bytes();
    bytes.extend_from_slice(
        &u64::try_from(key.len())
            .expect("key length fits u64")
            .to_be_bytes(),
    );
    bytes.extend_from_slice(key);
    for value in windows.ordered() {
        bytes.extend_from_slice(
            &u64::try_from(value.as_millis())
                .expect("validated window")
                .to_be_bytes(),
        );
    }
    // One byte bitset, bits 0..5 match ActivitySource::ALL; upper bits zero.
    bytes.push(
        sources
            .iter()
            .enumerate()
            .fold(0_u8, |bits, (index, accepted)| {
                bits | (u8::from(*accepted) << index)
            }),
    );
    bytes
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum LivenessPolicyError {
    #[error("execution-liveness windows must be positive")]
    ZeroWindow,
    #[error("execution-liveness windows must be exact u64 milliseconds")]
    NonPortableWindow,
    #[error("activity delivery lag must be shorter than the idle window")]
    DeliveryLagExceedsIdleWindow,
    #[error(
        "checkpoint interval plus delivery lag and observation request must be shorter than idle"
    )]
    InsufficientObservationMargin,
    #[error("checkpoint coalescing must leave time inside the activity delivery bound")]
    InsufficientDeliveryMargin,
}

/// A separate user, organization or Job control; never inferred from an RPC,
/// credential, execution lease or old Tool timeout.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BusinessDeadline {
    pub at: UtcTimestamp,
    pub source: VersionedKey,
}

/// Reference to an already committed wait, whose reason, dependencies and wake
/// conditions belong to the existing Run Journal. It is not another wait store.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DurableWaitBinding {
    pub checkpoint_sequence: RunSeq,
    pub identity: Sha256,
}

/// Minimum startup/silence budget for newly admitted Tools. Observation cadence
/// and cancellation cleanup are control-plane bounds, not execution timeouts.
pub const MIN_TOOL_EXECUTION_WINDOW: Duration = Duration::from_secs(60);
/// Silence window for Tools that run an arbitrary local process (a shell
/// command, a Python program, a build). Their output and observed process work
/// renew it; only this long without either stops them. A legitimate download
/// or dependency install may be quiet for minutes, so it is generous on
/// purpose. There is no total lifetime unless the invocation carries an
/// explicit deadline.
pub const LONG_WORK_SILENCE_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Communication window for discovering frozen execution policy/material.
/// No tool has been dispatched yet. Once loaded, its original policy governs
/// each control request; this window never wraps Agent or physical execution.
pub const DISCOVERY_REQUEST_WINDOW: Duration = MIN_TOOL_EXECUTION_WINDOW;
