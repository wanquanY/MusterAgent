use super::*;
use std::collections::HashMap;

#[derive(Default)]
struct PartitionedStore(Mutex<HashMap<ModelDependencyKey, DependencyRecoveryState>>);

impl ModelDependencyRecoveryStore for PartitionedStore {
    fn acquire<'a>(
        &'a self,
        key: &'a ModelDependencyKey,
        id: InvocationId,
        lease: Duration,
    ) -> DependencyFuture<'a, DependencyAdmission> {
        Box::pin(async move {
            self.0
                .lock()
                .expect("rows")
                .entry(key.clone())
                .or_default()
                .acquire(id, now(), lease)
        })
    }
    fn observe<'a>(
        &'a self,
        key: &'a ModelDependencyKey,
        permit: DependencyPermit,
        observation: DependencyObservation,
    ) -> DependencyFuture<'a, ()> {
        Box::pin(async move {
            self.0
                .lock()
                .expect("rows")
                .get_mut(key)
                .expect("admitted")
                .observe(permit, observation, now())
                .map(|_| ())
        })
    }
}
#[derive(Default)]
struct Vault(AtomicUsize);
impl CredentialResolver for Vault {
    fn resolve<'a>(
        &'a self,
        _: &'a CredentialGrantRef,
        _: &'a ModelRoute,
        _: musteragent_runtime_model::ModelInvocationRouteRole,
        _: InvocationId,
    ) -> Pin<
        Box<dyn Future<Output = Result<CredentialMaterial, CredentialResolverError>> + Send + 'a>,
    > {
        Box::pin(async {
            Ok(CredentialMaterial::VersionedApiKey {
                secret: SecretString::from("fixture only"),
                revision: self.0.load(Ordering::Acquire).to_string(),
            })
        })
    }
}

#[tokio::test]
async fn original_credential_blocks_sibling_model_and_new_revision_dispatches_immediately() {
    let server = MockProviderWire::start(ProviderWireScenario {
        status: 401,
        content_type: "application/json".into(),
        chunks: vec![b"{}".to_vec()],
        hang_after_chunks: false,
    })
    .await
    .expect("HTTP");
    let primary = fixture_model_entry_id();
    let sibling = "019fa6de-38fd-7000-8000-000000000303"
        .parse()
        .expect("entry");
    let endpoints = [primary, sibling].map(|model_entry_id| ProviderEndpoint {
        model_entry_id,
        model_provider_id: fixture_model_provider_id(),
        api_format: ApiFormat::OpenAiResponses,
        invocation_url: Url::parse(server.endpoint()).expect("URL"),
        connect_timeout: Duration::from_secs(1),
        read_timeout: Duration::from_secs(1),
        total_timeout: Duration::from_secs(2),
        anthropic_version: None,
        aws_region: None,
        origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
    });
    let vault = Arc::new(Vault::default());
    let provider = DirectModelProvider::new(
        Arc::new(StaticEndpointRegistry::new(endpoints, true).expect("routes")),
        vault.clone(),
        Arc::new(NoContent),
        ProviderAdapterConfig::default(),
    )
    .expect("provider")
    .with_dependency_recovery(Arc::new(PartitionedStore::default()));
    let mut invocation = request(fixture_model_provider_id(), primary);
    for (entry, revision, expected_requests) in [
        (primary, 1, 1),
        (sibling, 1, 1),
        (sibling, 2, 2),
        (primary, 2, 2),
    ] {
        invocation.route.model_entry_id = entry;
        invocation.invocation.invocation_id = InvocationId::generate();
        vault.0.store(revision, Ordering::Release);
        let events = provider
            .stream(invocation.clone())
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(events.as_slice(), [Err(error)] if error.safe_code == ProviderErrorCode::AuthenticationFailed),
            "{events:?}"
        );
        assert_eq!(server.captured_requests().await.len(), expected_requests);
    }
    server.shutdown().await.expect("shutdown");
}
