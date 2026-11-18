//! A prepared body retains its original route and Invocation while a short
//! scoped credential is renewed. Personal/API-key credentials keep their path.
use super::*;
use musteragent_identity::InvocationId;
use musteragent_model::{CredentialGrantRef, ModelRoute};
use musteragent_runtime_model::ModelInvocationRouteRole;

pub(super) struct PreparedCredentialLease {
    deadline: Instant,
    grant: CredentialGrantRef,
    route: ModelRoute,
    role: ModelInvocationRouteRole,
    invocation: InvocationId,
}
fn rejected() -> ProviderError {
    provider_error(
        ProviderErrorKind::Authorization,
        ProviderErrorCode::CredentialResolutionFailed,
        false,
        None,
    )
}
fn deadline(value: &CredentialMaterial) -> Result<Option<Instant>, ProviderError> {
    let CredentialMaterial::ScopedBearer { expires_at, .. } = value else {
        return Ok(None);
    };
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| rejected())?;
    let now = i64::try_from(now.as_millis()).map_err(|_| rejected())?;
    let remaining = expires_at
        .unix_millis()
        .checked_sub(now)
        .filter(|value| *value > 0)
        .ok_or_else(rejected)?;
    Ok(Some(
        Instant::now()
            .checked_add(Duration::from_millis(remaining as u64))
            .ok_or_else(rejected)?,
    ))
}
impl PreparedCredentialLease {
    pub(super) fn retain(
        request: &CanonicalModelRequest,
        credentials: &CredentialMaterial,
    ) -> Result<Option<Self>, ProviderError> {
        Ok(deadline(credentials)?.map(|deadline| Self {
            deadline,
            grant: request.credential_grant_ref.clone(),
            route: request.route.clone(),
            role: request.route_role,
            invocation: request.invocation.invocation_id,
        }))
    }
    pub(super) async fn refresh(
        self,
        resolver: &dyn CredentialResolver,
        outbound: &mut crate::PreparedModelHttpRequest,
        recovery_headers: &mut reqwest::header::HeaderMap,
    ) -> Result<(), ProviderError> {
        if self.deadline.saturating_duration_since(Instant::now()) >= Duration::from_secs(1) {
            return Ok(());
        }
        let credentials = resolver
            .resolve(&self.grant, &self.route, self.role, self.invocation)
            .await
            .map_err(super::failures::credential_resolution)?;
        let expiry = deadline(&credentials)?.ok_or_else(rejected)?;
        let CredentialMaterial::ScopedBearer { secret, .. } = credentials else {
            return Err(rejected());
        };
        let mut header = HeaderValue::from_str(&format!("Bearer {}", secret.expose_secret()))
            .map_err(|_| rejected())?;
        header.set_sensitive(true);
        if Instant::now() >= expiry {
            return Err(rejected());
        }
        recovery_headers.insert(reqwest::header::AUTHORIZATION, header.clone());
        outbound.replace_bearer_credential(header);
        Ok(())
    }
}
