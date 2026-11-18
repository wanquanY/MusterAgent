//! Logical source consumers of existing Owner-scoped StandingGoals.
//!
//! Source capture, credentials and provider cursors remain with the source
//! owner. These references do not grant authority or create a worker/queue.
use std::{collections::BTreeSet, fmt, num::NonZeroU64};

use musteragent_content_ref::Sha256;
use musteragent_identity::{
    GoalId, OwnerApplicationBindingId as GoalSourceBindingId, OwnerEventSubscriptionId,
    SourceBoundaryId, SourceCaptureId, SourceEventId, UtcTimestamp, VersionedKey,
};
use thiserror::Error;

use super::source_binding::{
    GoalSourceBinding, GoalSourceBindingGuard, GoalSourceBindingStatus, GoalSourceSubject,
};

pub const MAX_EVENT_SELECTION_BYTES: usize = 16 * 1024;
pub const MAX_SUBSCRIBED_EVENT_KINDS: usize = 32;
const MAX_COUNTER: u64 = i64::MAX as u64;

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum EventSubscriptionError {
    #[error("source selection or event contract is invalid")]
    InvalidConfiguration,
    #[error("source consumer belongs to another subject")]
    SubjectMismatch,
    #[error("source consumer revision or activation boundary conflicts")]
    Conflict,
    #[error("source consumer counter is exhausted")]
    CounterExhausted,
    #[error("source application authority changed or was withdrawn")]
    AuthorityChanged,
    #[error("source baseline is not ready")]
    NotReady,
    #[error("an ended source consumer cannot be resumed")]
    Ended,
    #[error("source observation does not belong to the active capture")]
    ForeignCapture,
    #[error("source consumer timestamp precedes its current state")]
    InvalidTime,
}

/// Provider-specific selection, validated against the *published* source
/// capability schema by the application resolver before preparation. The
/// domain applies transport budgets as well; it never interprets provider ACLs.
#[derive(Clone, Eq, PartialEq)]
pub struct EventSourceSelection(serde_json::Value);

impl fmt::Debug for EventSourceSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EventSourceSelection(<private>)")
    }
}

impl EventSourceSelection {
    pub fn from_json(bytes: &[u8]) -> Result<Self, EventSubscriptionError> {
        if bytes.is_empty() || bytes.len() > MAX_EVENT_SELECTION_BYTES {
            return Err(EventSubscriptionError::InvalidConfiguration);
        }
        let mut value: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|_| EventSubscriptionError::InvalidConfiguration)?;
        fn bounded(value: &serde_json::Value, depth: usize, count: &mut usize) -> bool {
            *count += 1;
            if depth > 16 || *count > 1024 {
                return false;
            }
            match value {
                serde_json::Value::Object(values) => {
                    values.len() <= 64
                        && values.iter().all(|(key, value)| {
                            key.len() <= 256 && bounded(value, depth + 1, count)
                        })
                }
                serde_json::Value::Array(values) => {
                    values.len() <= 128
                        && values.iter().all(|value| bounded(value, depth + 1, count))
                }
                serde_json::Value::String(value) => value.len() <= 4096,
                _ => true,
            }
        }
        if !value.is_object() || !bounded(&value, 0, &mut 0) {
            return Err(EventSubscriptionError::InvalidConfiguration);
        }
        value.sort_all_objects();
        let result = Self(value);
        if result.json_bytes().len() > MAX_EVENT_SELECTION_BYTES {
            return Err(EventSubscriptionError::InvalidConfiguration);
        }
        Ok(result)
    }

    #[must_use]
    pub fn json_bytes(&self) -> Vec<u8> {
        // A parsed JSON Value cannot contain a failing custom serializer.
        self.0.to_string().into_bytes()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EventSubscriptionConfiguration {
    pub application_binding_id: GoalSourceBindingId,
    pub application_guard: GoalSourceBindingGuard,
    pub capability_ref: VersionedKey,
    pub capability_digest: Sha256,
    pub selection: EventSourceSelection,
    pub event_kinds: BTreeSet<VersionedKey>,
}

impl EventSubscriptionConfiguration {
    pub fn validate(&self) -> Result<(), EventSubscriptionError> {
        if self.event_kinds.is_empty()
            || self.event_kinds.len() > MAX_SUBSCRIBED_EVENT_KINDS
            || self.application_guard.revision.get() > MAX_COUNTER
            || self.application_guard.authority_epoch.get() > MAX_COUNTER
            || self.application_guard.authority_epoch > self.application_guard.revision
        {
            return Err(EventSubscriptionError::InvalidConfiguration);
        }
        Ok(())
    }

    /// A source preparation receipt covers the entire logical configuration.
    /// Physical sharing has its own source-owned identity and must not use this
    /// digest as a credential, provider cursor or connection key.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut kinds = self
            .event_kinds
            .iter()
            .map(VersionedKey::as_str)
            .collect::<Vec<_>>();
        kinds.sort_unstable();
        let mut document = serde_json::json!({
            "schema": "musteragent.owner-event-configuration.v1",
            "application_binding_id": self.application_binding_id.to_string(),
            "application_revision": self.application_guard.revision.get().to_string(),
            "application_authority_epoch": self.application_guard.authority_epoch.get().to_string(),
            "capability_ref": self.capability_ref.as_str(),
            "capability_digest": self.capability_digest.to_string(),
            "selection": self.selection.0,
            "event_kinds": kinds,
        });
        document.sort_all_objects();
        document.to_string().into_bytes()
    }

    #[must_use]
    pub fn fingerprint(&self) -> Sha256 {
        Sha256::digest(self.canonical_bytes())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct EventSubscriptionGuard {
    pub revision: NonZeroU64,
    pub activation_epoch: NonZeroU64,
}

/// Desired input admission, independent of Goal control and physical cleanup.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum EventSubscriptionIntent {
    Watching,
    Paused,
    Ended,
}

/// Receipt of a source-owned baseline/catch-up operation. Sequence zero is
/// valid for an empty baseline. A timestamp alone is never a boundary proof.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceActivationBoundary {
    pub boundary_id: SourceBoundaryId,
    pub subscription_id: OwnerEventSubscriptionId,
    pub activation_epoch: NonZeroU64,
    pub configuration_digest: Sha256,
    pub capture_id: SourceCaptureId,
    pub capture_generation: NonZeroU64,
    pub after_source_sequence: u64,
    pub source_status_revision: NonZeroU64,
    pub evidence_digest: Sha256,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SourceReadiness {
    Ready,
    Retrying,
    AuthorizationRequired,
    CoverageGap,
    Retired,
}

/// Source-owned health is an observation, not another permission grant.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceReadinessObservation {
    pub capture_id: SourceCaptureId,
    pub capture_generation: NonZeroU64,
    pub revision: NonZeroU64,
    pub readiness: SourceReadiness,
    pub evidence_digest: Sha256,
}

/// Canonical provider object supplied by the Source owner, never by the model.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceObjectIdentity {
    pub provider: String,
    pub connection: String,
    pub resource: String,
    pub version: String,
    pub observed_at: UtcTimestamp,
}
impl SourceObjectIdentity {
    pub fn validate(&self) -> Result<(), EventSubscriptionError> {
        for (value, maximum) in [
            (&self.provider, 200),
            (&self.connection, 500),
            (&self.resource, 2000),
            (&self.version, 500),
        ] {
            if value.trim().is_empty()
                || value.len() > maximum
                || value.chars().any(char::is_control)
            {
                return Err(EventSubscriptionError::InvalidConfiguration);
            }
        }
        if self.observed_at.unix_millis() < 0 {
            return Err(EventSubscriptionError::InvalidTime);
        }
        Ok(())
    }
}

/// Immutable metadata of a durable source observation. Provider payloads are
/// not embedded here; reading them still requires current source authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceObservationIdentity {
    pub capture_id: SourceCaptureId,
    pub capture_generation: NonZeroU64,
    pub event_id: SourceEventId,
    pub source_sequence: NonZeroU64,
    pub event_kind: VersionedKey,
    pub evidence_digest: Sha256,
    pub source_object: Option<SourceObjectIdentity>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SourceInputEligibility {
    Eligible,
    NotWatching,
    SupersededActivation,
    BeforeActivationBoundary,
    EventKindNotSelected,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerEventSubscription {
    pub subscription_id: OwnerEventSubscriptionId,
    pub goal_id: GoalId,
    pub subject: GoalSourceSubject,
    pub configuration: EventSubscriptionConfiguration,
    /// Goal coordination, not capture authority. Changing this requirement
    /// must not skip observations by establishing a new source baseline.
    pub required: bool,
    pub guard: EventSubscriptionGuard,
    pub desired: EventSubscriptionIntent,
    pub boundary: Option<SourceActivationBoundary>,
    pub source_status: Option<SourceReadinessObservation>,
    /// Acknowledgement of the *current* desired epoch. Absence after pause/end
    /// means source cleanup is still pending, even though input is closed.
    pub source_acknowledged_at: Option<UtcTimestamp>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl OwnerEventSubscription {
    pub fn validate(&self) -> Result<(), EventSubscriptionError> {
        self.configuration.validate()?;
        if self.guard.revision.get() > MAX_COUNTER
            || self.guard.activation_epoch.get() > MAX_COUNTER
            || self.guard.activation_epoch > self.guard.revision
        {
            return Err(EventSubscriptionError::InvalidConfiguration);
        }
        if self.created_at > self.updated_at
            || self
                .source_acknowledged_at
                .is_some_and(|at| at < self.created_at || at > self.updated_at)
        {
            return Err(EventSubscriptionError::InvalidTime);
        }
        if let Some(boundary) = &self.boundary {
            if self.desired != EventSubscriptionIntent::Watching
                || self.source_acknowledged_at.is_none()
                || boundary.subscription_id != self.subscription_id
                || boundary.activation_epoch != self.guard.activation_epoch
                || boundary.configuration_digest != self.configuration.fingerprint()
                || boundary.capture_generation.get() > MAX_COUNTER
                || boundary.after_source_sequence > MAX_COUNTER
                || boundary.source_status_revision.get() > MAX_COUNTER
            {
                return Err(EventSubscriptionError::InvalidConfiguration);
            }
            let status = self
                .source_status
                .as_ref()
                .ok_or(EventSubscriptionError::InvalidConfiguration)?;
            if status.capture_id != boundary.capture_id
                || status.capture_generation != boundary.capture_generation
                || status.revision < boundary.source_status_revision
                || status.revision.get() > MAX_COUNTER
            {
                return Err(EventSubscriptionError::InvalidConfiguration);
            }
        } else if self.desired == EventSubscriptionIntent::Watching
            && self.source_acknowledged_at.is_some()
        {
            return Err(EventSubscriptionError::InvalidConfiguration);
        }
        if self.boundary.is_none() && self.source_status.is_some() {
            return Err(EventSubscriptionError::InvalidConfiguration);
        }
        Ok(())
    }

    pub fn prepare(
        subscription_id: OwnerEventSubscriptionId,
        goal_id: GoalId,
        subject: GoalSourceSubject,
        configuration: EventSubscriptionConfiguration,
        required: bool,
        application: &GoalSourceBinding,
        now: UtcTimestamp,
    ) -> Result<Self, EventSubscriptionError> {
        configuration.validate()?;
        let result = Self {
            subscription_id,
            goal_id,
            subject,
            configuration,
            required,
            guard: EventSubscriptionGuard {
                revision: NonZeroU64::MIN,
                activation_epoch: NonZeroU64::MIN,
            },
            desired: EventSubscriptionIntent::Watching,
            boundary: None,
            source_status: None,
            source_acknowledged_at: None,
            created_at: now,
            updated_at: now,
        };
        result.authorize(application)?;
        Ok(result)
    }

    pub fn authorize(&self, application: &GoalSourceBinding) -> Result<(), EventSubscriptionError> {
        if application.subject != self.subject {
            return Err(EventSubscriptionError::SubjectMismatch);
        }
        if application.binding_id != self.configuration.application_binding_id
            || application.guard != self.configuration.application_guard
            || application.status != GoalSourceBindingStatus::Enabled
        {
            return Err(EventSubscriptionError::AuthorityChanged);
        }
        Ok(())
    }

    fn check_change(
        &self,
        expected: EventSubscriptionGuard,
        now: UtcTimestamp,
    ) -> Result<(), EventSubscriptionError> {
        self.validate()?;
        if expected != self.guard {
            return Err(EventSubscriptionError::Conflict);
        }
        if now < self.updated_at {
            return Err(EventSubscriptionError::InvalidTime);
        }
        Ok(())
    }

    fn next_counter(value: NonZeroU64) -> Result<NonZeroU64, EventSubscriptionError> {
        if value.get() >= MAX_COUNTER {
            return Err(EventSubscriptionError::CounterExhausted);
        }
        NonZeroU64::new(value.get() + 1).ok_or(EventSubscriptionError::CounterExhausted)
    }

    pub fn set_required(
        &self,
        expected: EventSubscriptionGuard,
        required: bool,
        now: UtcTimestamp,
    ) -> Result<Self, EventSubscriptionError> {
        self.check_change(expected, now)?;
        if self.desired == EventSubscriptionIntent::Ended {
            return Err(EventSubscriptionError::Ended);
        }
        if self.required == required {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.required = required;
        next.guard.revision = Self::next_counter(self.guard.revision)?;
        next.updated_at = now;
        Ok(next)
    }

    /// Reconfiguration and explicit resume invalidate the previous boundary.
    /// A provider callback cannot reactivate a paused/ended logical consumer.
    pub fn change(
        &self,
        expected: EventSubscriptionGuard,
        desired: EventSubscriptionIntent,
        configuration: Option<EventSubscriptionConfiguration>,
        application: &GoalSourceBinding,
        now: UtcTimestamp,
    ) -> Result<Self, EventSubscriptionError> {
        self.check_change(expected, now)?;
        if self.desired == EventSubscriptionIntent::Ended {
            return Err(EventSubscriptionError::Ended);
        }
        let mut next = self.clone();
        let reconfiguring = configuration.is_some();
        if let Some(configuration) = configuration {
            configuration.validate()?;
            next.configuration = configuration;
        }
        // Withdrawal must remain possible after the original authority is gone.
        if reconfiguring || desired == EventSubscriptionIntent::Watching {
            next.authorize(application)?;
        }
        next.guard = EventSubscriptionGuard {
            revision: Self::next_counter(self.guard.revision)?,
            activation_epoch: Self::next_counter(self.guard.activation_epoch)?,
        };
        next.desired = desired;
        next.boundary = None;
        next.source_status = None;
        next.source_acknowledged_at = None;
        next.updated_at = now;
        Ok(next)
    }

    /// Close the old input boundary when the parent Goal resumes. The Goal
    /// owner calls this in its control transaction, preserving each source's
    /// independent watch/pause intent. It grants no source authority: a new
    /// baseline must still pass `activate` and current application admission.
    pub fn invalidate_activation(
        &self,
        expected: EventSubscriptionGuard,
        now: UtcTimestamp,
    ) -> Result<Self, EventSubscriptionError> {
        self.check_change(expected, now)?;
        if self.desired != EventSubscriptionIntent::Watching {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.guard = EventSubscriptionGuard {
            revision: Self::next_counter(self.guard.revision)?,
            activation_epoch: Self::next_counter(self.guard.activation_epoch)?,
        };
        next.boundary = None;
        next.source_status = None;
        next.source_acknowledged_at = None;
        next.updated_at = now;
        Ok(next)
    }

    pub fn activate(
        &self,
        boundary: SourceActivationBoundary,
        application: &GoalSourceBinding,
        now: UtcTimestamp,
    ) -> Result<Self, EventSubscriptionError> {
        self.check_change(self.guard, now)?;
        self.authorize(application)?;
        if self.desired != EventSubscriptionIntent::Watching
            || boundary.subscription_id != self.subscription_id
            || boundary.activation_epoch != self.guard.activation_epoch
            || boundary.configuration_digest != self.configuration.fingerprint()
            || boundary.capture_generation.get() > MAX_COUNTER
            || boundary.after_source_sequence > MAX_COUNTER
            || boundary.source_status_revision.get() > MAX_COUNTER
        {
            return Err(EventSubscriptionError::Conflict);
        }
        if let Some(current) = &self.boundary {
            return if *current == boundary {
                Ok(self.clone())
            } else {
                Err(EventSubscriptionError::Conflict)
            };
        }
        let mut next = self.clone();
        next.source_status = Some(SourceReadinessObservation {
            capture_id: boundary.capture_id,
            capture_generation: boundary.capture_generation,
            revision: boundary.source_status_revision,
            readiness: SourceReadiness::Ready,
            evidence_digest: boundary.evidence_digest,
        });
        next.boundary = Some(boundary);
        next.source_acknowledged_at = Some(now);
        next.updated_at = now;
        Ok(next)
    }

    pub fn acknowledge_release(
        &self,
        activation_epoch: NonZeroU64,
        desired: EventSubscriptionIntent,
        now: UtcTimestamp,
    ) -> Result<Self, EventSubscriptionError> {
        self.check_change(self.guard, now)?;
        if activation_epoch != self.guard.activation_epoch
            || self.desired != desired
            || desired == EventSubscriptionIntent::Watching
        {
            return Err(EventSubscriptionError::Conflict);
        }
        if self.source_acknowledged_at.is_some() {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.source_acknowledged_at = Some(now);
        next.updated_at = now;
        Ok(next)
    }

    pub fn observe_readiness(
        &self,
        activation_epoch: NonZeroU64,
        observation: SourceReadinessObservation,
        now: UtcTimestamp,
    ) -> Result<Self, EventSubscriptionError> {
        self.check_change(self.guard, now)?;
        if activation_epoch != self.guard.activation_epoch {
            return Err(EventSubscriptionError::Conflict);
        }
        let boundary = self
            .boundary
            .as_ref()
            .ok_or(EventSubscriptionError::NotReady)?;
        if observation.capture_id != boundary.capture_id
            || observation.capture_generation != boundary.capture_generation
            || observation.revision.get() > MAX_COUNTER
        {
            return Err(EventSubscriptionError::ForeignCapture);
        }
        let current = self
            .source_status
            .as_ref()
            .ok_or(EventSubscriptionError::InvalidConfiguration)?;
        if observation.revision < current.revision {
            return Ok(self.clone());
        }
        if observation.revision == current.revision {
            return if observation == *current {
                Ok(self.clone())
            } else {
                Err(EventSubscriptionError::Conflict)
            };
        }
        if current.readiness == SourceReadiness::Retired {
            return Err(EventSubscriptionError::Conflict);
        }
        let mut next = self.clone();
        next.source_status = Some(observation);
        next.updated_at = now;
        Ok(next)
    }

    /// Existing receipts are checked first by the original command owner.
    /// For a NEW input the caller also locks/rechecks Goal control, current
    /// source invalidation and all required-source readiness in the same UoW
    /// as StandingGoal::accept_input_on. This method alone is not admission.
    pub fn check_observation(
        &self,
        activation_epoch: NonZeroU64,
        observation: &SourceObservationIdentity,
        application: &GoalSourceBinding,
    ) -> Result<SourceInputEligibility, EventSubscriptionError> {
        self.validate()?;
        if self.desired != EventSubscriptionIntent::Watching {
            return Ok(SourceInputEligibility::NotWatching);
        }
        if activation_epoch != self.guard.activation_epoch {
            return Ok(SourceInputEligibility::SupersededActivation);
        }
        self.authorize(application)?;
        let boundary = self
            .boundary
            .as_ref()
            .ok_or(EventSubscriptionError::NotReady)?;
        if observation.capture_id != boundary.capture_id
            || observation.capture_generation != boundary.capture_generation
            || observation.source_sequence.get() > MAX_COUNTER
        {
            return Err(EventSubscriptionError::ForeignCapture);
        }
        if observation.source_sequence.get() <= boundary.after_source_sequence {
            return Ok(SourceInputEligibility::BeforeActivationBoundary);
        }
        if !self
            .configuration
            .event_kinds
            .contains(&observation.event_kind)
        {
            return Ok(SourceInputEligibility::EventKindNotSelected);
        }
        if self
            .source_status
            .as_ref()
            .is_none_or(|status| status.readiness != SourceReadiness::Ready)
        {
            return Err(EventSubscriptionError::NotReady);
        }
        Ok(SourceInputEligibility::Eligible)
    }
}

#[cfg(test)]
mod tests;
