//! Explicit execution-location routing for sandboxed code execution.
//!
//! Sandboxed code (`data.python.run`, `sandbox.*`) can run in one of two
//! executors that share the same job protocol, `ContentRef` inputs/outputs and
//! result receipts:
//!
//! * [`SandboxExecutor::Browser`] — the user's own browser client (for example
//!   a Pyodide worker attached through the browser gateway). This is the
//!   preferred executor: it spreads compute onto user devices.
//! * [`SandboxExecutor::Server`] — a server-side sandbox pool operated by the
//!   host.
//!
//! Routing is **explicit, never a silent fallback**. Every invocation first
//! obtains a [`SandboxRouteDecision`] from [`route`]; the decision carries the
//! chosen executor *and* the reason, and the caller must persist it with the
//! invocation record and emit it in the invocation's events so clients and
//! diagnostics can show where code ran and why. When no permitted executor is
//! available, routing fails with [`SandboxRouteError`] instead of guessing.
#![forbid(unsafe_code)]

use musteragent_identity::UtcTimestamp;
use thiserror::Error;

/// Where sandboxed code executes.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SandboxExecutor {
    Browser,
    Server,
}

impl SandboxExecutor {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Browser => "browser",
            Self::Server => "server",
        }
    }

    pub fn parse(value: &str) -> Result<Self, SandboxContractError> {
        match value {
            "browser" => Ok(Self::Browser),
            "server" => Ok(Self::Server),
            _ => Err(SandboxContractError::UnknownExecutor),
        }
    }
}

/// Compatibility declared by a capability release or a Skill manifest.
///
/// `BrowserCompatible`: pure Python or packages available in the browser
/// runtime; network access goes through the platform proxy.
/// `ServerRequired`: native extensions, browser automation (for example
/// playwright), long-running jobs, or anything the browser runtime cannot run.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SandboxCompatibility {
    BrowserCompatible,
    ServerRequired,
}

impl SandboxCompatibility {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BrowserCompatible => "browser-compatible",
            Self::ServerRequired => "server-required",
        }
    }

    pub fn parse(value: &str) -> Result<Self, SandboxContractError> {
        match value {
            "browser-compatible" => Ok(Self::BrowserCompatible),
            "server-required" => Ok(Self::ServerRequired),
            _ => Err(SandboxContractError::UnknownCompatibility),
        }
    }

    /// Combine the declarations of everything a single execution depends on
    /// (the Tool release and every active Skill that requests code execution).
    /// Any `ServerRequired` declaration makes the whole execution server-only.
    pub fn combine(declarations: impl IntoIterator<Item = Self>) -> Self {
        if declarations
            .into_iter()
            .any(|declaration| declaration == Self::ServerRequired)
        {
            Self::ServerRequired
        } else {
            Self::BrowserCompatible
        }
    }
}

/// Host routing policy for a tenant, deployment or product.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Default)]
pub enum SandboxRoutingPolicy {
    /// Use an online browser client when the work allows it; otherwise route
    /// to the server sandbox with an explicit reason.
    #[default]
    PreferBrowser,
    /// Never use the server sandbox (for example before it is deployed).
    BrowserOnly,
    /// Never use browser clients.
    ServerOnly,
}

/// What started the invocation. Background and scheduled work cannot rely on
/// an attended browser tab.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum SandboxTrigger {
    Interactive,
    Background,
    Scheduled,
}

/// Client surface of the conversation that owns the invocation.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum SandboxClientSurface {
    Web,
    Desktop,
    Mobile,
    Api,
}

/// Live executor availability observed by the router at decision time.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SandboxAvailability {
    /// Number of browser clients for this principal that are online and have
    /// advertised the sandbox capability.
    pub online_browser_clients: u32,
    /// Whether the host operates a server sandbox that accepts jobs.
    pub server_available: bool,
}

/// Trusted host routing context of one Run: where its conversation is
/// attended, what started it and which executors the host allows.
///
/// Hosts supply it with run admission (submit or retry) so the route authority
/// becomes durable in the same transaction as the Run. Delegated child runs
/// inherit their parent's context. It is never model-supplied.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SandboxRoutingContext {
    pub surface: SandboxClientSurface,
    pub trigger: SandboxTrigger,
    pub policy: SandboxRoutingPolicy,
}

impl SandboxRoutingContext {
    /// Stable one-byte-per-field encoding used in admission idempotency
    /// fingerprints. Codes never change meaning once published.
    #[must_use]
    pub const fn canonical_bytes(self) -> [u8; 3] {
        [
            match self.surface {
                SandboxClientSurface::Web => 1,
                SandboxClientSurface::Desktop => 2,
                SandboxClientSurface::Mobile => 3,
                SandboxClientSurface::Api => 4,
            },
            match self.trigger {
                SandboxTrigger::Interactive => 1,
                SandboxTrigger::Background => 2,
                SandboxTrigger::Scheduled => 3,
            },
            match self.policy {
                SandboxRoutingPolicy::PreferBrowser => 1,
                SandboxRoutingPolicy::BrowserOnly => 2,
                SandboxRoutingPolicy::ServerOnly => 3,
            },
        ]
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SandboxRouteInput {
    pub compatibility: SandboxCompatibility,
    pub policy: SandboxRoutingPolicy,
    pub trigger: SandboxTrigger,
    pub surface: SandboxClientSurface,
    pub availability: SandboxAvailability,
}

/// Why the router chose an executor. Persisted and shown to users.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SandboxRouteReason {
    /// A browser client is online and the work is browser-compatible.
    BrowserClientOnline,
    /// The capability or a Skill declares `server-required`.
    CompatibilityRequiresServer,
    /// No browser client with the sandbox capability is online.
    NoOnlineBrowserClient,
    /// The owning surface cannot host a browser sandbox (mobile).
    MobileSurface,
    /// Background work has no attended browser client.
    BackgroundTrigger,
    /// Scheduled work has no attended browser client.
    ScheduledTrigger,
    /// API callers have no browser client.
    ApiSurface,
    /// Host policy forces the server sandbox.
    PolicyServerOnly,
    /// Host policy forces browser clients.
    PolicyBrowserOnly,
}

impl SandboxRouteReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BrowserClientOnline => "browser_client_online",
            Self::CompatibilityRequiresServer => "compatibility_requires_server",
            Self::NoOnlineBrowserClient => "no_online_browser_client",
            Self::MobileSurface => "mobile_surface",
            Self::BackgroundTrigger => "background_trigger",
            Self::ScheduledTrigger => "scheduled_trigger",
            Self::ApiSurface => "api_surface",
            Self::PolicyServerOnly => "policy_server_only",
            Self::PolicyBrowserOnly => "policy_browser_only",
        }
    }

    pub fn parse(value: &str) -> Result<Self, SandboxContractError> {
        Ok(match value {
            "browser_client_online" => Self::BrowserClientOnline,
            "compatibility_requires_server" => Self::CompatibilityRequiresServer,
            "no_online_browser_client" => Self::NoOnlineBrowserClient,
            "mobile_surface" => Self::MobileSurface,
            "background_trigger" => Self::BackgroundTrigger,
            "scheduled_trigger" => Self::ScheduledTrigger,
            "api_surface" => Self::ApiSurface,
            "policy_server_only" => Self::PolicyServerOnly,
            "policy_browser_only" => Self::PolicyBrowserOnly,
            _ => return Err(SandboxContractError::UnknownReason),
        })
    }
}

/// The routing outcome that must be recorded with the invocation.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SandboxRouteDecision {
    pub executor: SandboxExecutor,
    pub reason: SandboxRouteReason,
    pub compatibility: SandboxCompatibility,
}

/// No permitted executor is available. The invocation fails with this error
/// (and the reason that excluded each executor) instead of falling back.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum SandboxRouteError {
    #[error("the server sandbox is required ({0:?}) but unavailable")]
    ServerUnavailable(SandboxRouteReason),
    #[error("host policy allows only browser clients, but none is online")]
    BrowserUnavailable,
    #[error("the work requires the server sandbox but host policy allows only browser clients")]
    PolicyConflict,
}

impl SandboxRouteError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::ServerUnavailable(_) => "sandbox_server_unavailable",
            Self::BrowserUnavailable => "sandbox_browser_unavailable",
            Self::PolicyConflict => "sandbox_policy_conflict",
        }
    }
}

/// Choose the executor for one invocation. Pure and deterministic: the same
/// input always yields the same decision, so recovery can re-validate it.
pub fn route(input: SandboxRouteInput) -> Result<SandboxRouteDecision, SandboxRouteError> {
    let decide = |executor, reason| SandboxRouteDecision {
        executor,
        reason,
        compatibility: input.compatibility,
    };
    let server = |reason| {
        if input.availability.server_available {
            Ok(decide(SandboxExecutor::Server, reason))
        } else {
            Err(SandboxRouteError::ServerUnavailable(reason))
        }
    };
    let browser_online = input.availability.online_browser_clients > 0;
    match input.policy {
        SandboxRoutingPolicy::ServerOnly => return server(SandboxRouteReason::PolicyServerOnly),
        SandboxRoutingPolicy::BrowserOnly => {
            if input.compatibility == SandboxCompatibility::ServerRequired {
                return Err(SandboxRouteError::PolicyConflict);
            }
            return if browser_online {
                Ok(decide(
                    SandboxExecutor::Browser,
                    SandboxRouteReason::PolicyBrowserOnly,
                ))
            } else {
                Err(SandboxRouteError::BrowserUnavailable)
            };
        }
        SandboxRoutingPolicy::PreferBrowser => {}
    }
    if input.compatibility == SandboxCompatibility::ServerRequired {
        return server(SandboxRouteReason::CompatibilityRequiresServer);
    }
    match input.surface {
        SandboxClientSurface::Mobile => return server(SandboxRouteReason::MobileSurface),
        SandboxClientSurface::Api => return server(SandboxRouteReason::ApiSurface),
        SandboxClientSurface::Web | SandboxClientSurface::Desktop => {}
    }
    match input.trigger {
        SandboxTrigger::Background => return server(SandboxRouteReason::BackgroundTrigger),
        SandboxTrigger::Scheduled => return server(SandboxRouteReason::ScheduledTrigger),
        SandboxTrigger::Interactive => {}
    }
    if browser_online {
        Ok(decide(
            SandboxExecutor::Browser,
            SandboxRouteReason::BrowserClientOnline,
        ))
    } else {
        server(SandboxRouteReason::NoOnlineBrowserClient)
    }
}

/// Durable record of a routing decision, stored with the invocation and
/// emitted in its events.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SandboxRouteRecord {
    pub decision: SandboxRouteDecision,
    pub decided_at: UtcTimestamp,
}

pub const SANDBOX_ROUTE_RECORD_SCHEMA: &str = "musteragent.sandbox-route.v1";

impl SandboxRouteRecord {
    /// Length-delimited canonical bytes, domain-separated by the schema id.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for part in [
            SANDBOX_ROUTE_RECORD_SCHEMA.as_bytes(),
            self.decision.executor.as_str().as_bytes(),
            self.decision.reason.as_str().as_bytes(),
            self.decision.compatibility.as_str().as_bytes(),
        ] {
            out.extend_from_slice(&(part.len() as u64).to_be_bytes());
            out.extend_from_slice(part);
        }
        out.extend_from_slice(&self.decided_at.unix_millis().to_be_bytes());
        out
    }

    /// Validate a persisted record: the reason must be one the executor can
    /// legitimately be chosen for.
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        let browser_reason = matches!(
            self.decision.reason,
            SandboxRouteReason::BrowserClientOnline | SandboxRouteReason::PolicyBrowserOnly
        );
        let consistent = match self.decision.executor {
            SandboxExecutor::Browser => {
                browser_reason
                    && self.decision.compatibility == SandboxCompatibility::BrowserCompatible
            }
            SandboxExecutor::Server => !browser_reason,
        };
        if consistent {
            Ok(())
        } else {
            Err(SandboxContractError::InconsistentRecord)
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum SandboxContractError {
    #[error("unknown sandbox executor")]
    UnknownExecutor,
    #[error("unknown sandbox compatibility declaration")]
    UnknownCompatibility,
    #[error("unknown sandbox routing reason")]
    UnknownReason,
    #[error("sandbox route record is internally inconsistent")]
    InconsistentRecord,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_context_canonical_bytes_are_stable_per_field() {
        let context = SandboxRoutingContext {
            surface: SandboxClientSurface::Web,
            trigger: SandboxTrigger::Interactive,
            policy: SandboxRoutingPolicy::BrowserOnly,
        };
        assert_eq!(context.canonical_bytes(), [1, 1, 2]);
        let api = SandboxRoutingContext {
            surface: SandboxClientSurface::Api,
            trigger: SandboxTrigger::Scheduled,
            policy: SandboxRoutingPolicy::ServerOnly,
        };
        assert_eq!(api.canonical_bytes(), [4, 3, 3]);
    }

    fn input() -> SandboxRouteInput {
        SandboxRouteInput {
            compatibility: SandboxCompatibility::BrowserCompatible,
            policy: SandboxRoutingPolicy::PreferBrowser,
            trigger: SandboxTrigger::Interactive,
            surface: SandboxClientSurface::Web,
            availability: SandboxAvailability {
                online_browser_clients: 1,
                server_available: true,
            },
        }
    }

    fn decided(input: SandboxRouteInput) -> (SandboxExecutor, SandboxRouteReason) {
        let decision = route(input).expect("route");
        (decision.executor, decision.reason)
    }

    #[test]
    fn the_browser_is_the_primary_executor() {
        assert_eq!(
            decided(input()),
            (
                SandboxExecutor::Browser,
                SandboxRouteReason::BrowserClientOnline
            )
        );
    }

    #[test]
    fn every_server_route_names_its_reason() {
        let mut offline = input();
        offline.availability.online_browser_clients = 0;
        assert_eq!(
            decided(offline),
            (
                SandboxExecutor::Server,
                SandboxRouteReason::NoOnlineBrowserClient
            )
        );
        let mut native = input();
        native.compatibility = SandboxCompatibility::ServerRequired;
        assert_eq!(
            decided(native),
            (
                SandboxExecutor::Server,
                SandboxRouteReason::CompatibilityRequiresServer
            )
        );
        let mut mobile = input();
        mobile.surface = SandboxClientSurface::Mobile;
        assert_eq!(
            decided(mobile),
            (SandboxExecutor::Server, SandboxRouteReason::MobileSurface)
        );
        let mut scheduled = input();
        scheduled.trigger = SandboxTrigger::Scheduled;
        assert_eq!(
            decided(scheduled),
            (
                SandboxExecutor::Server,
                SandboxRouteReason::ScheduledTrigger
            )
        );
        let mut background = input();
        background.trigger = SandboxTrigger::Background;
        assert_eq!(
            decided(background),
            (
                SandboxExecutor::Server,
                SandboxRouteReason::BackgroundTrigger
            )
        );
        let mut forced = input();
        forced.policy = SandboxRoutingPolicy::ServerOnly;
        assert_eq!(
            decided(forced),
            (
                SandboxExecutor::Server,
                SandboxRouteReason::PolicyServerOnly
            )
        );
    }

    #[test]
    fn unavailable_executors_fail_instead_of_falling_back() {
        let mut no_server = input();
        no_server.availability = SandboxAvailability {
            online_browser_clients: 0,
            server_available: false,
        };
        assert_eq!(
            route(no_server),
            Err(SandboxRouteError::ServerUnavailable(
                SandboxRouteReason::NoOnlineBrowserClient
            ))
        );
        let mut browser_only = input();
        browser_only.policy = SandboxRoutingPolicy::BrowserOnly;
        browser_only.availability.online_browser_clients = 0;
        assert_eq!(
            route(browser_only),
            Err(SandboxRouteError::BrowserUnavailable)
        );
        browser_only.compatibility = SandboxCompatibility::ServerRequired;
        assert_eq!(route(browser_only), Err(SandboxRouteError::PolicyConflict));
    }

    #[test]
    fn any_server_required_declaration_wins() {
        assert_eq!(
            SandboxCompatibility::combine([
                SandboxCompatibility::BrowserCompatible,
                SandboxCompatibility::ServerRequired,
            ]),
            SandboxCompatibility::ServerRequired
        );
        assert_eq!(
            SandboxCompatibility::combine([]),
            SandboxCompatibility::BrowserCompatible
        );
    }

    #[test]
    fn records_round_trip_their_codes_and_reject_inconsistency() {
        for reason in [
            SandboxRouteReason::BrowserClientOnline,
            SandboxRouteReason::CompatibilityRequiresServer,
            SandboxRouteReason::NoOnlineBrowserClient,
            SandboxRouteReason::MobileSurface,
            SandboxRouteReason::BackgroundTrigger,
            SandboxRouteReason::ScheduledTrigger,
            SandboxRouteReason::ApiSurface,
            SandboxRouteReason::PolicyServerOnly,
            SandboxRouteReason::PolicyBrowserOnly,
        ] {
            assert_eq!(SandboxRouteReason::parse(reason.as_str()), Ok(reason));
        }
        let decision = route(input()).expect("route");
        let record = SandboxRouteRecord {
            decision,
            decided_at: UtcTimestamp::from_unix_millis(1_790_000_000_000),
        };
        assert!(record.validate().is_ok());
        assert!(!record.canonical_bytes().is_empty());
        let mut forged = record;
        forged.decision.executor = SandboxExecutor::Server;
        assert_eq!(
            forged.validate(),
            Err(SandboxContractError::InconsistentRecord)
        );
    }
}
