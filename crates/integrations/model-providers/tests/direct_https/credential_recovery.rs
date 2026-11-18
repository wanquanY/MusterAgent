use super::*;
use musteragent_runtime_model::ModelInvocationRouteRole;

struct ExpiredThenRenewed(AtomicUsize);
impl CredentialResolver for ExpiredThenRenewed {
    fn resolve<'a>(
        &'a self,
        _: &'a CredentialGrantRef,
        _: &'a ModelRoute,
        _: ModelInvocationRouteRole,
        _: InvocationId,
    ) -> Pin<
        Box<dyn Future<Output = Result<CredentialMaterial, CredentialResolverError>> + Send + 'a>,
    > {
        Box::pin(async move {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(CredentialResolverError::Expired)
            } else {
                Ok(CredentialMaterial::Bearer(SecretString::new(
                    "renewed-fixture-token".into(),
                )))
            }
        })
    }
}

#[tokio::test]
async fn expired_credentials_recover_on_the_same_model_without_premature_http_dispatch() {
    for role in [
        ModelInvocationRouteRole::Chat,
        ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Vision),
        ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Compression),
        ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::GoalJudge),
    ] {
        let server = MockProviderSse::start(ProviderSseScenario::successful(vec![ProviderSseFrame::new(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"resp_renewed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#,
        )])).await.expect("mock provider");
        let endpoints = StaticEndpointRegistry::new(
            [ProviderEndpoint {
                model_provider_id: fixture_model_provider_id(),
                model_entry_id: fixture_model_entry_id(),
                api_format: ApiFormat::OpenAiResponses,
                invocation_url: Url::parse(server.endpoint()).expect("mock endpoint"),
                connect_timeout: Duration::from_secs(1),
                read_timeout: Duration::from_secs(1),
                total_timeout: Duration::from_secs(3),
                anthropic_version: None,
                aws_region: None,
                origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
            }],
            true,
        )
        .expect("registry");
        let provider = DirectModelProvider::new(
            Arc::new(endpoints),
            Arc::new(ExpiredThenRenewed(AtomicUsize::new(0))),
            Arc::new(NoContent),
            ProviderAdapterConfig::default(),
        )
        .expect("provider");
        let mut request = request(fixture_model_provider_id(), fixture_model_entry_id());
        request.route_role = role;
        request.cache_policy = CachePolicy::Disabled;
        request.reasoning_policy = ReasoningPolicy::Disabled;
        let failure = provider
            .preflight(&request)
            .await
            .expect_err("expired credential must not dispatch");
        assert_eq!(failure.safe_code, ProviderErrorCode::CredentialExpired);
        assert!(failure.permits_dependency_recovery());
        assert!(server.captured_requests().await.is_empty());
        provider
            .preflight(&request)
            .await
            .expect("renewed credential");
        let events = provider.stream(request).collect::<Vec<_>>().await;
        assert!(events.iter().all(Result::is_ok), "{role:?}: {events:?}");
        assert!(matches!(
            events.last(),
            Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
        ));
        assert_eq!(server.captured_requests().await.len(), 1);
        server.shutdown().await.expect("stop mock provider");
    }
}
