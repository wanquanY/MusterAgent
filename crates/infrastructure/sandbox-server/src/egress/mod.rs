//! Egress control for sandbox jobs that are allowed network access.
//!
//! Jobs have no network interface or default route. A loopback listener
//! carries HTTP proxy traffic over attached Docker exec streams into this
//! policy; private, metadata and host-denied destinations are refused.

pub mod filter;
pub mod proxy;

pub use filter::{DenyReason, EgressPolicy, classify};
pub use proxy::{CheckedTarget, EgressDecision, EgressProxy, bind};

pub mod audit;
pub mod resolver;
pub mod upstream;
pub use audit::{EgressAudit, EgressEvent, ResolutionMode};
pub use resolver::{ResolveError, Resolver, SystemResolver};
pub use upstream::UpstreamProxy;

pub(crate) mod tunnel;
