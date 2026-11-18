//! Shared dependency health is operational coordination, never Run authority.
//! One real model request probes a failed endpoint; no synthetic inference,
//! task termination, Tool replay, or process-local retry counter lives here.
use std::{future::Future, num::NonZeroU32, pin::Pin, time::Duration};

use musteragent_content_ref::Sha256;
use musteragent_identity::{InvocationId, TenantId, UtcTimestamp};
use musteragent_model::{ModelEntryId, ModelProviderId};
use musteragent_runtime_snapshot::RecoveryPolicySnapshot;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ModelDependencyKey {
    pub tenant_id: TenantId,
    pub provider_id: ModelProviderId,
    /// None denotes shared provider credential health across all model entries.
    pub model_entry_id: Option<ModelEntryId>,
    /// Domain-separated fingerprint of the owner revision and physical scope.
    pub endpoint_hash: Sha256,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DependencyPermit {
    pub generation: InvocationId,
    pub invocation_id: InvocationId,
    pub probe: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DependencyAdmission {
    Ready(DependencyPermit),
    Deferred {
        retry_after_ms: u64,
    },
    RepairRequired {
        reason: DependencyRepair,
        retry_after_ms: u64,
    },
}

/// Only verified upstream HTTP outcomes may create a repair cooldown. Product
/// authority and gateway failures do not determine provider credential health.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DependencyRepair {
    Credential,
    Payment,
    Quota,
}
/// A rare real probe also detects out-of-band upstream repair with an unchanged revision.
pub const REPAIR_PROBE_MINIMUM_MS: u64 = 300_000;
impl DependencyRepair {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Credential => "credential",
            Self::Payment => "payment",
            Self::Quota => "quota",
        }
    }
    pub fn decode(value: &str) -> Result<Self, DependencyRecoveryError> {
        match value {
            "credential" => Ok(Self::Credential),
            "payment" => Ok(Self::Payment),
            "quota" => Ok(Self::Quota),
            _ => Err(DependencyRecoveryError::InvalidState),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DependencyObservation {
    Healthy,
    Unavailable {
        retry_after_ms: Option<u64>,
    },
    RepairNeeded {
        reason: DependencyRepair,
        retry_after_ms: Option<u64>,
    },
    /// Request-local authority/protocol failure proves no endpoint health.
    Abandoned,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
pub enum DependencyRecoveryError {
    #[error("shared model dependency store is unavailable")]
    Unavailable,
    #[error("shared model dependency state is invalid")]
    InvalidState,
}

pub type DependencyFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DependencyRecoveryError>> + Send + 'a>>;

pub trait ModelDependencyRecoveryStore: Send + Sync {
    /// Uses database time and an atomic row lock. The probe bounds only
    /// response startup; successful streams never retain this lease.
    fn acquire<'a>(
        &'a self,
        key: &'a ModelDependencyKey,
        invocation_id: InvocationId,
        probe_lifetime: Duration,
    ) -> DependencyFuture<'a, DependencyAdmission>;
    /// Old generations and expired/replaced probes have no effect. A dropped
    /// caller leaves a bounded lease, recoverable by another process.
    fn observe<'a>(
        &'a self,
        key: &'a ModelDependencyKey,
        permit: DependencyPermit,
        observation: DependencyObservation,
    ) -> DependencyFuture<'a, ()>;
}

/// Portable state transition shared by SQL adapters. Adapters decode validated
/// scalar columns and commit a mutation while holding their database fence.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DependencyRecoveryState {
    pub generation: InvocationId,
    pub failures: u32,
    pub repair: Option<DependencyRepair>,
    pub retry_not_before: UtcTimestamp,
    pub probe_invocation: Option<InvocationId>,
    pub probe_expires_at: Option<UtcTimestamp>,
}

impl Default for DependencyRecoveryState {
    fn default() -> Self {
        Self {
            generation: InvocationId::generate(),
            failures: 0,
            repair: None,
            retry_not_before: UtcTimestamp::from_unix_millis(0),
            probe_invocation: None,
            probe_expires_at: None,
        }
    }
}

impl DependencyRecoveryState {
    pub fn validate(&self) -> Result<(), DependencyRecoveryError> {
        if (self.failures == 0 && self.repair.is_some())
            || self.probe_invocation.is_some() != self.probe_expires_at.is_some()
            || self.retry_not_before.unix_millis() < 0
            || (self.failures == 0
                && (self.retry_not_before.unix_millis() != 0 || self.probe_invocation.is_some()))
            || self
                .probe_expires_at
                .is_some_and(|expires| expires <= self.retry_not_before)
        {
            return Err(DependencyRecoveryError::InvalidState);
        }
        Ok(())
    }

    pub fn acquire(
        &mut self,
        invocation_id: InvocationId,
        now: UtcTimestamp,
        probe_lifetime: Duration,
    ) -> Result<DependencyAdmission, DependencyRecoveryError> {
        self.validate()?;
        if self.failures > 0 {
            let blocked_until = self.probe_expires_at.unwrap_or(self.retry_not_before);
            if blocked_until > now {
                let retry_after_ms = (blocked_until.unix_millis() - now.unix_millis()) as u64;
                return Ok(match self.repair {
                    Some(reason) => DependencyAdmission::RepairRequired {
                        reason,
                        retry_after_ms,
                    },
                    None => DependencyAdmission::Deferred { retry_after_ms },
                });
            }
            let lifetime = i64::try_from(probe_lifetime.as_millis())
                .ok()
                .filter(|v| *v > 0)
                .ok_or(DependencyRecoveryError::InvalidState)?;
            self.probe_invocation = Some(invocation_id);
            self.probe_expires_at = Some(
                now.checked_add_millis(lifetime)
                    .ok_or(DependencyRecoveryError::InvalidState)?,
            );
        }
        Ok(DependencyAdmission::Ready(DependencyPermit {
            generation: self.generation,
            invocation_id,
            probe: self.failures > 0,
        }))
    }

    /// True only if durable state changed. Late outcomes cannot close a newer
    /// outage, increment its failure count, or release a replacement probe.
    pub fn observe(
        &mut self,
        permit: DependencyPermit,
        observation: DependencyObservation,
        now: UtcTimestamp,
    ) -> Result<bool, DependencyRecoveryError> {
        self.validate()?;
        let owns = self.generation == permit.generation
            && if permit.probe {
                self.probe_invocation == Some(permit.invocation_id)
                    && self.probe_expires_at.is_some_and(|expires| expires > now)
            } else {
                self.failures == 0
            };
        if !owns {
            return Ok(false);
        }
        match observation {
            DependencyObservation::Abandoned => {
                if !permit.probe {
                    return Ok(false);
                }
                self.probe_invocation = None;
                self.probe_expires_at = None;
            }
            DependencyObservation::Healthy => {
                if !permit.probe {
                    return Ok(false);
                }
                *self = Self::default();
            }
            DependencyObservation::Unavailable { retry_after_ms }
            | DependencyObservation::RepairNeeded { retry_after_ms, .. } => {
                let repair = match observation {
                    DependencyObservation::RepairNeeded { reason, .. } => Some(reason),
                    _ => None,
                };
                let failures = self.failures.saturating_add(1);
                let number =
                    NonZeroU32::new(failures).ok_or(DependencyRecoveryError::InvalidState)?;
                let (floor, ceiling) =
                    RecoveryPolicySnapshot::production_default().delay_window_ms(number);
                let bytes = permit.invocation_id.into_uuid().into_bytes();
                let seed = u64::from_be_bytes(
                    bytes[8..]
                        .try_into()
                        .map_err(|_| DependencyRecoveryError::InvalidState)?,
                );
                let jitter = floor + seed % (ceiling - floor + 1);
                // Account repair may happen without a config edit. A rare real
                // probe can discover restored upstream credit or credentials.
                let floor = if repair.is_some() {
                    REPAIR_PROBE_MINIMUM_MS
                } else {
                    0
                };
                let delay = jitter.max(floor).max(retry_after_ms.unwrap_or(0));
                let retry_not_before = now
                    .unix_millis()
                    .saturating_add(i64::try_from(delay).unwrap_or(i64::MAX));
                *self = Self {
                    generation: InvocationId::generate(),
                    failures,
                    repair,
                    retry_not_before: UtcTimestamp::from_unix_millis(retry_not_before),
                    probe_invocation: None,
                    probe_expires_at: None,
                };
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests;
