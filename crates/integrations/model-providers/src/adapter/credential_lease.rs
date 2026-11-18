//! A prepared body retains its original route and Invocation while a short
//! scoped credential is renewed. Personal/API-key credentials keep their path.
use super::*;
use musteragent_runtime_model::ModelRequestAuthority;
use reqwest::header::HeaderName;

pub(super) struct PreparedCredentialLease {
    deadline: Instant,
    authority: ModelRequestAuthority,
    dynamic_headers: Vec<HeaderName>,
}
fn rejected() -> ProviderError {
    provider_error(
        ProviderErrorKind::Authorization,
        ProviderErrorCode::CredentialResolutionFailed,
        false,
        None,
    )
}
pub(crate) fn credential_deadline(
    value: &CredentialMaterial,
) -> Result<Option<Instant>, ProviderError> {
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
    pub(super) fn new(
        deadline: Instant,
        authority: ModelRequestAuthority,
        dynamic_headers: Vec<HeaderName>,
    ) -> Self {
        Self {
            deadline,
            authority,
            dynamic_headers,
        }
    }

    pub(super) async fn refresh(
        self,
        provider: &DirectModelProvider,
        outbound: &mut crate::PreparedModelHttpRequest,
        recovery_headers: &mut reqwest::header::HeaderMap,
    ) -> Result<(), ProviderError> {
        if self.deadline.saturating_duration_since(Instant::now()) >= Duration::from_secs(1) {
            return Ok(());
        }
        let authority = &self.authority;
        let credentials = provider
            .credentials
            .resolve(
                &authority.credential_grant_ref,
                &authority.route,
                authority.route_role,
                authority.invocation.invocation_id,
            )
            .await
            .map_err(super::failures::credential_resolution)?;
        let expiry = credential_deadline(&credentials)?.ok_or_else(rejected)?;
        let CredentialMaterial::ScopedBearer { secret, .. } = credentials else {
            return Err(rejected());
        };
        let mut header = HeaderValue::from_str(&format!("Bearer {}", secret.expose_secret()))
            .map_err(|_| rejected())?;
        header.set_sensitive(true);
        recovery_headers.insert(reqwest::header::AUTHORIZATION, header.clone());
        outbound.replace_bearer_credential(header);
        if provider.per_request_authorization
            && let crate::PreparedModelHttpRequest::Native(native) = outbound
        {
            let headers = native.headers_mut();
            for name in self
                .dynamic_headers
                .iter()
                .chain(provider.host_headers.static_headers.keys())
            {
                headers.remove(name);
            }
            provider.host_headers.apply(authority, headers).await?;
            recovery_headers.clone_from(headers);
        }
        if Instant::now() >= expiry {
            return Err(rejected());
        }
        Ok(())
    }
}
