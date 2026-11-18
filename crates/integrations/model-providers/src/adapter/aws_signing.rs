//! Optional direct-upstream AWS request signing. Gateway response decoding is
//! intentionally not part of this feature.
use super::{protocol_error, provider_error};
use crate::{AwsCredentialMaterial, ProviderEndpoint};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    SignableBody, SignableRequest, SigningParams, SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use musteragent_model::ApiFormat;
use musteragent_runtime_model::{ProviderError, ProviderErrorCode, ProviderErrorKind};
use reqwest::header::{HeaderName, HeaderValue};
use secrecy::ExposeSecret as _;
use std::time::SystemTime;

pub(super) fn sign_aws(
    endpoint: &ProviderEndpoint,
    material: &AwsCredentialMaterial,
    body: &[u8],
    accept: &str,
    content_type: &str,
) -> Result<Vec<(HeaderName, HeaderValue)>, ProviderError> {
    if !matches!(
        endpoint.api_format,
        ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel
    ) {
        return Err(provider_error(
            ProviderErrorKind::Authorization,
            ProviderErrorCode::RouteConfigurationInvalid,
            false,
            None,
        ));
    }
    let credentials = Credentials::new(
        material.access_key_id.expose_secret().to_owned(),
        material.secret_access_key.expose_secret().to_owned(),
        material
            .session_token
            .as_ref()
            .map(|value| value.expose_secret().to_owned()),
        None,
        "musteragent-execution-grant",
    );
    let identity = credentials.into();
    let region = endpoint.aws_region.as_deref().ok_or_else(|| {
        provider_error(
            ProviderErrorKind::InvalidRequest,
            ProviderErrorCode::RouteConfigurationInvalid,
            false,
            None,
        )
    })?;
    let params: SigningParams<'_> = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("bedrock")
        .time(SystemTime::now())
        .settings(SigningSettings::default())
        .build()
        .map_err(|_| protocol_error())?
        .into();
    let headers = [("accept", accept), ("content-type", content_type)];
    let signable = SignableRequest::new(
        "POST",
        endpoint.invocation_url.as_str(),
        headers.into_iter(),
        SignableBody::Bytes(body),
    )
    .map_err(|_| protocol_error())?;
    let output = sign(signable, &params).map_err(|_| protocol_error())?;
    output
        .output()
        .headers()
        .map(|(name, value)| {
            Ok((
                HeaderName::from_bytes(name.as_bytes()).map_err(|_| protocol_error())?,
                HeaderValue::from_str(value).map_err(|_| protocol_error())?,
            ))
        })
        .collect()
}
