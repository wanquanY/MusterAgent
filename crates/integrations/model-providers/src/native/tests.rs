use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_model::{ApiFormat, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::{
    ModelCredentialAuthorization, ModelCredentialAuthorizationError, ModelInvocationRouteRole,
};
use secrecy::{ExposeSecret as _, SecretString};
use url::Url;

use super::*;
use crate::{
    CredentialMaterial, CredentialResolver, CredentialResolverError, EndpointRegistry,
    EndpointRegistryError,
};

fn route(api_format: ApiFormat) -> ModelRoute {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroU64;

    use musteragent_identity::VersionedKey;
    use musteragent_model::{PricingVersionRef, ProviderStatePolicy};

    ModelRoute {
        route_ref: VersionedKey::new("registry.model.route@1").expect("route ref"),
        resolved_model_key: "gpt-5".to_owned(),
        model_provider_id: ModelProviderId::from_uuid(
            uuid::Uuid::parse_str("019fcfd6-90c7-75e0-b8cf-f4638feb1701").expect("provider UUID"),
        )
        .expect("provider ID"),
        model_entry_id: ModelEntryId::from_uuid(
            uuid::Uuid::parse_str("019fcfd6-90c7-75e0-b8cf-f4638feb1702").expect("entry UUID"),
        )
        .expect("entry ID"),
        api_format,
        adapter_version: VersionedKey::new("musteragent.provider-adapter@2")
            .expect("adapter version"),
        protocol_version: VersionedKey::new("musteragent.provider-protocol@2")
            .expect("protocol version"),
        context_window: NonZeroU64::new(128_000).expect("context window"),
        max_output_tokens: NonZeroU64::new(16_000).expect("output limit"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:device.price@1").expect("pricing"),
        provider_state_policy: ProviderStatePolicy::Stateless,
        fallback_route_refs: Vec::new(),
    }
}

fn now() -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(1_000)
}

#[test]
fn token_expiry_rotation_and_revocation_are_account_scoped() {
    let token = Arc::new(NativeModelCredentialAuthority::new(
        "old".into(),
        UtcTimestamp::from_unix_millis(2_000),
        now,
    ));
    assert_eq!(
        token
            .credential()
            .expect("valid credential")
            .expose_secret(),
        "old"
    );
    token
        .rotate("equal".into(), UtcTimestamp::from_unix_millis(2_000))
        .expect("idempotent renewal");
    assert_eq!(
        token.rotate("stale".into(), UtcTimestamp::from_unix_millis(1_999)),
        Err(CredentialResolverError::Expired)
    );
    assert_eq!(
        token.rotate("".into(), UtcTimestamp::from_unix_millis(3_000)),
        Err(CredentialResolverError::Expired)
    );
    let previous = token.snapshot().expect("snapshot");
    token
        .rotate("new".into(), UtcTimestamp::from_unix_millis(3_000))
        .expect("renew");
    token
        .restore(previous.0, previous.1)
        .expect("coordinated rollback");
    let retained = Arc::clone(&token);
    token.revoke();
    assert_eq!(
        retained.credential().err(),
        Some(CredentialResolverError::Unavailable)
    );
    assert!(retained.snapshot().is_err());
    assert!(
        retained
            .rotate("revive".into(), UtcTimestamp::from_unix_millis(4_000))
            .is_err()
    );
    assert!(
        retained
            .restore("revive".into(), UtcTimestamp::from_unix_millis(4_000))
            .is_err()
    );
    for expiry in [999, 1_000] {
        assert_eq!(
            NativeModelCredentialAuthority::new(
                "expired".into(),
                UtcTimestamp::from_unix_millis(expiry),
                now
            )
            .credential()
            .err(),
            Some(CredentialResolverError::Expired)
        );
    }
}

#[test]
fn concurrent_renewal_cannot_move_expiry_backwards() {
    let token = Arc::new(NativeModelCredentialAuthority::new(
        "old".into(),
        UtcTimestamp::from_unix_millis(2_000),
        now,
    ));
    std::thread::scope(|scope| {
        for expiry in 2_001..2_065 {
            let token = token.clone();
            scope.spawn(move || {
                let _ = token.rotate(
                    SecretString::from("new"),
                    UtcTimestamp::from_unix_millis(expiry),
                );
            });
        }
    });
    assert_eq!(
        token.snapshot().expect("retained token").1.unix_millis(),
        2_064
    );
}

#[tokio::test]
async fn proxy_protocol_routing_is_shared_and_preserves_identity() {
    let registry = NativeProxyEndpointRegistry::new(
        Url::parse("https://gateway.example/api/v1/llm-proxy/v1/").expect("URL"),
        false,
    );
    for (format, suffix) in [
        (ApiFormat::OpenAiResponses, "responses"),
        (ApiFormat::OpenAiChatCompletions, "chat/completions"),
        (ApiFormat::AnthropicMessages, "messages"),
        (ApiFormat::BedrockConverse, "bedrock/invoke"),
        (ApiFormat::BedrockInvokeModel, "bedrock/invoke"),
        (ApiFormat::TypeSafeSystemOne, "decisions"),
        (ApiFormat::OpenRouterDecisions, "decisions"),
        (ApiFormat::GoogleGenerateContent, "google/generate"),
        (ApiFormat::GoogleInteractions, "google/interactions"),
    ] {
        let route = route(format);
        let endpoint = registry.resolve(&route).await.expect("supported route");
        assert_eq!(
            endpoint.invocation_url.as_str(),
            format!("https://gateway.example/api/v1/llm-proxy/v1/{suffix}")
        );
        assert_eq!(endpoint.model_entry_id, route.model_entry_id);
        assert_eq!(endpoint.model_provider_id, route.model_provider_id);
        assert!(endpoint.origin.forwards_route_identity());
    }
}

#[tokio::test]
async fn proxy_rejects_unsafe_origins_and_explicitly_gates_development_http() {
    let route = route(ApiFormat::OpenAiResponses);
    for url in [
        "http://gateway.example/v1",
        "https://user:secret@gateway.example/v1",
        "https://gateway.example/v1#fragment",
        "https://gateway.example/v1?token=secret",
    ] {
        assert!(
            NativeProxyEndpointRegistry::new(Url::parse(url).expect("URL"), false)
                .resolve(&route)
                .await
                .is_err()
        );
    }
    let url = Url::parse("http://127.0.0.1:9/v1").expect("URL");
    assert!(
        NativeProxyEndpointRegistry::new(url.clone(), false)
            .resolve(&route)
            .await
            .is_err()
    );
    assert!(
        NativeProxyEndpointRegistry::new(url, true)
            .resolve(&route)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn direct_routes_never_receive_proxy_endpoint_or_proxy_token() {
    let registry = NativeProxyEndpointRegistry::new(
        Url::parse("https://gateway.example/v1").expect("URL"),
        false,
    );
    let token = NativeModelCredentialAuthority::new(
        "proxy-secret".into(),
        UtcTimestamp::from_unix_millis(2_000),
        now,
    );
    for prefix in [
        musteragent_execution_grant::DEVICE_DIRECT_ROUTE_REF_PREFIX,
        musteragent_execution_grant::DEVICE_DIRECT_COMPRESSION_ROUTE_REF_PREFIX,
        musteragent_execution_grant::DEVICE_DIRECT_VISION_ROUTE_REF_PREFIX,
    ] {
        let mut route = route(ApiFormat::OpenAiResponses);
        route.route_ref = musteragent_identity::VersionedKey::new(format!("{prefix}fixture@1"))
            .expect("route ref");
        assert_eq!(
            registry.resolve(&route).await.err(),
            Some(EndpointRegistryError::RouteNotFound)
        );
        assert_eq!(
            token
                .resolve_material(&route, InvocationId::generate())
                .await
                .err(),
            Some(CredentialResolverError::BindingMismatch)
        );
    }
}

type ObservedBinding = (
    String,
    String,
    ModelInvocationRouteRole,
    InvocationId,
    UtcTimestamp,
);
struct AdmissionProbe {
    result: Result<(), ModelCredentialAuthorizationError>,
    seen: Mutex<Option<ObservedBinding>>,
}
impl ModelCredentialAuthorization for AdmissionProbe {
    fn authorize<'a>(
        &'a self,
        grant: &'a CredentialGrantRef,
        route: &'a ModelRoute,
        role: ModelInvocationRouteRole,
        invocation: InvocationId,
        at: UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<(), ModelCredentialAuthorizationError>> {
        Box::pin(async move {
            *self.seen.lock().expect("probe lock") = Some((
                grant.as_str().to_owned(),
                route.route_ref.as_str().to_owned(),
                role,
                invocation,
                at,
            ));
            self.result
        })
    }
}
struct CredentialProbe(AtomicUsize);
impl CredentialMaterialSource for CredentialProbe {
    fn resolve_material<'a>(
        &'a self,
        _route: &'a ModelRoute,
        _invocation: InvocationId,
    ) -> ApplicationFuture<'a, Result<CredentialMaterial, CredentialResolverError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(CredentialMaterial::Bearer("fixture".into()))
        })
    }
}

#[tokio::test]
async fn all_admission_failures_precede_any_credential_access() {
    use ModelCredentialAuthorizationError as E;
    let grant = CredentialGrantRef::new("grant_fixture_model").expect("grant");
    let route = route(ApiFormat::OpenAiResponses);
    for error in [
        E::InvocationNotAdmitted,
        E::RouteBindingMismatch,
        E::GrantRouteNotAdmitted,
        E::GrantAuthorityIncomplete,
        E::InvocationAlreadyConsumed,
        E::WriterFenceStale,
        E::Unavailable,
    ] {
        let admission = Arc::new(AdmissionProbe {
            result: Err(error),
            seen: Mutex::default(),
        });
        let source = Arc::new(CredentialProbe(AtomicUsize::new(0)));
        let resolver = AdmittedCredentialResolver::new(admission, source.clone(), now);
        let result = resolver
            .resolve(
                &grant,
                &route,
                ModelInvocationRouteRole::Chat,
                InvocationId::generate(),
            )
            .await;
        assert_eq!(
            result.err(),
            Some(if matches!(error, E::WriterFenceStale | E::Unavailable) {
                CredentialResolverError::Unavailable
            } else {
                CredentialResolverError::BindingMismatch
            })
        );
        assert_eq!(source.0.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn admitted_resolver_forwards_exact_invocation_binding_and_clock() {
    let admission = Arc::new(AdmissionProbe {
        result: Ok(()),
        seen: Mutex::default(),
    });
    let source = Arc::new(CredentialProbe(AtomicUsize::new(0)));
    let resolver = AdmittedCredentialResolver::new(admission.clone(), source.clone(), now);
    let grant = CredentialGrantRef::new("grant_fixture_model").expect("grant");
    let route = route(ApiFormat::OpenAiResponses);
    let invocation = InvocationId::generate();
    let role = ModelInvocationRouteRole::Auxiliary(musteragent_model::AuxiliaryModelRole::Vision);
    resolver
        .resolve(&grant, &route, role, invocation)
        .await
        .expect("admitted material");
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        *admission.seen.lock().expect("probe"),
        Some((
            grant.as_str().to_owned(),
            route.route_ref.as_str().to_owned(),
            role,
            invocation,
            now()
        ))
    );
}
