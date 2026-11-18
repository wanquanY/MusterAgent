//! Host-owned DNS port. A failed resolution never authorizes forwarding.
use musteragent_runtime_contract::ApplicationFuture;
use std::net::IpAddr;

/// Resolves all addresses for a name. The proxy checks every answer and pins
/// the connection to a checked address, including when chaining upstream.
pub trait Resolver: Send + Sync + std::fmt::Debug {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> ApplicationFuture<'a, Result<Vec<IpAddr>, ResolveError>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("destination resolution failed")]
pub struct ResolveError;

#[derive(Debug, Default)]
pub struct SystemResolver;
impl Resolver for SystemResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> ApplicationFuture<'a, Result<Vec<IpAddr>, ResolveError>> {
        Box::pin(async move {
            tokio::net::lookup_host((host, port))
                .await
                .map(|answers| answers.map(|a| a.ip()).collect())
                .map_err(|_| ResolveError)
        })
    }
}
