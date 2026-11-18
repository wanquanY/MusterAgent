//! Host identity is resolved before any tenant-owned resource is read or changed.
use crate::{Result, config::DevelopmentIdentity};
use musteragent_identity::{Actor, PrincipalKind};
use std::collections::{BTreeMap, BTreeSet};

/// Authenticated host subject and the admission ceiling selected by that host.
#[derive(Clone)]
pub struct Subject {
    pub actor: Actor,
    pub agents: BTreeSet<String>,
    pub routes: BTreeSet<String>,
}
/// Authentication failure without exposing credentials or provider internals.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// The presented credential does not establish a host subject.
    #[error("host identity was not authenticated")]
    Unauthenticated,
    /// The host cannot currently verify identity; the request must fail closed.
    #[error("host identity verifier is unavailable")]
    Unavailable,
}
/// Replace this port with the production host's session/OIDC verification.
pub trait HostIdentity: Send + Sync {
    /// Verify a bearer token and return trusted authority, never caller-supplied IDs.
    fn authenticate(&self, token: &str) -> std::result::Result<Subject, IdentityError>;
}
/// Development only. Tokens come from named environment variables, never HTTP bodies.
pub struct StaticDevelopmentIdentity(BTreeMap<String, Subject>);
impl StaticDevelopmentIdentity {
    /// Load distinct nonempty development tokens with immutable actor bindings.
    pub fn load(identities: &[DevelopmentIdentity]) -> Result<Self> {
        if identities.is_empty() {
            return Err("the development executable requires explicit static identities".into());
        }
        let mut entries = BTreeMap::new();
        for identity in identities {
            let token = std::env::var(&identity.token_env)?;
            if token.len() < 16 || token.chars().any(char::is_whitespace) {
                return Err(
                    "development tokens must have at least 16 non-whitespace characters".into(),
                );
            }
            let subject = Subject {
                actor: Actor {
                    tenant_id: identity.tenant_id.parse()?,
                    principal_id: identity.user_id.parse()?,
                    principal_kind: PrincipalKind::User,
                },
                agents: identity.agents.iter().cloned().collect(),
                routes: identity.routes.iter().cloned().collect(),
            };
            if entries.insert(token, subject).is_some() {
                return Err("duplicate development token".into());
            }
        }
        Ok(Self(entries))
    }
}
impl HostIdentity for StaticDevelopmentIdentity {
    fn authenticate(&self, token: &str) -> std::result::Result<Subject, IdentityError> {
        self.0
            .get(token)
            .cloned()
            .ok_or(IdentityError::Unauthenticated)
    }
}
