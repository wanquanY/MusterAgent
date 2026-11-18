//! Bounded, session-owned network decisions retained independently of job code.
use super::DenyReason;
use std::sync::{Arc, Mutex};

/// Where destination address policy was enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionMode {
    CheckedAddress,
    UpstreamResolves,
}

/// One proxy policy fact. No URL path, request body or credentials are retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressEvent {
    pub host: String,
    pub port: u16,
    pub mode: ResolutionMode,
    pub denial: Option<DenyReason>,
}

/// One audit per job/session; never shared between tenants.
#[derive(Debug, Clone, Default)]
pub struct EgressAudit(Arc<Mutex<Vec<EgressEvent>>>);
impl EgressAudit {
    pub fn record(&self, event: EgressEvent) -> Result<(), DenyReason> {
        let mut events = self.0.lock().map_err(|_| DenyReason::AuditLimit)?;
        if events.len() >= 127 {
            if events.len() == 127 {
                events.push(EgressEvent {
                    denial: Some(DenyReason::AuditLimit),
                    ..event
                });
                tracing::warn!("sandbox egress audit limit reached; further egress denied");
            }
            return Err(DenyReason::AuditLimit);
        }
        tracing::info!(host = %event.host, port = event.port, mode = ?event.mode,
            denial = ?event.denial, "sandbox egress decision");
        events.push(event);
        Ok(())
    }
    #[must_use]
    pub fn events(&self) -> Vec<EgressEvent> {
        self.0
            .lock()
            .map(|events| events.clone())
            .unwrap_or_default()
    }
}
