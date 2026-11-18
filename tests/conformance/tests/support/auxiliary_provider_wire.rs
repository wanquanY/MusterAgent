//! Two isolated HTTP endpoints using the production Responses streaming adapter.
use musteragent_content_ref::ContentRef;
use musteragent_identity::{DottedKey, InvocationId, TenantId, VersionedKey};
use musteragent_model::{ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelRoute};
use musteragent_model_providers::{
    ContentResolver, ContentResolverError, CredentialMaterial, CredentialResolver,
    CredentialResolverError, DirectModelProvider, ProviderAdapterConfig, ProviderEndpoint,
    ProviderEndpointOrigin, ResolvedContent, StaticEndpointRegistry,
};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::{ModelInvocationRouteRole, ModelProvider};
use musteragent_runtime_snapshot::PinnedModelRoute;
use musteragent_testkit::{MockProviderWire, ProviderWireScenario};
use std::{sync::Arc, time::Duration};

pub struct Providers {
    pub primary: MockProviderWire,
    pub standby: MockProviderWire,
    pub adapter: Arc<dyn ModelProvider>,
}

pub fn routes(template: PinnedModelRoute, role: AuxiliaryModelRole) -> [PinnedModelRoute; 2] {
    [0, 1].map(|ordinal| {
        let mut model = template.clone();
        let route = &mut model.route;
        route.api_format = ApiFormat::OpenAiResponses;
        route.model_entry_id =
            musteragent_model::ModelEntryId::from_uuid(uuid::Uuid::now_v7()).unwrap();
        route.model_provider_id =
            musteragent_model::ModelProviderId::from_uuid(uuid::Uuid::now_v7()).unwrap();
        route.route_ref = VersionedKey::new(format!(
            "registry.aux.{}.id-{}@1",
            role.as_str(),
            route.model_entry_id
        ))
        .unwrap();
        route.resolved_model_key = format!("local-aux-{ordinal}");
        route
            .capabilities
            .insert(DottedKey::new("streaming").unwrap());
        if role == AuxiliaryModelRole::Compression {
            route.provider_state_policy = musteragent_model::ProviderStatePolicy::Stateless;
            route.parameters.clear();
        }
        if role == AuxiliaryModelRole::Vision {
            route.capabilities.insert(DottedKey::new("vision").unwrap());
            route
                .capabilities
                .insert(DottedKey::new("input.image").unwrap());
        }
        model.credential_grant_ref =
            CredentialGrantRef::new(format!("grant_local_{}_{ordinal}", role.as_str())).unwrap();
        model
    })
}

struct Access {
    role: AuxiliaryModelRole,
    routes: [PinnedModelRoute; 2],
    content: Arc<dyn ContentStore>,
}

impl ContentResolver for Access {
    fn resolve<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<ResolvedContent, ContentResolverError>> {
        Box::pin(async move {
            let bytes = self
                .content
                .read(tenant, reference)
                .await
                .map_err(|_| ContentResolverError::NotFound)?;
            musteragent_runtime_content::verify_content(reference, &bytes)
                .map_err(|_| ContentResolverError::LengthMismatch)?;
            Ok(ResolvedContent::Inline {
                media_type: reference.media_type.clone(),
                bytes: bytes.into(),
            })
        })
    }
}
impl CredentialResolver for Access {
    fn resolve<'a>(
        &'a self,
        key: &'a CredentialGrantRef,
        route: &'a ModelRoute,
        role: ModelInvocationRouteRole,
        _: InvocationId,
    ) -> ApplicationFuture<'a, Result<CredentialMaterial, CredentialResolverError>> {
        Box::pin(async move {
            if role != ModelInvocationRouteRole::Auxiliary(self.role)
                || !self
                    .routes
                    .iter()
                    .any(|p| &p.route == route && &p.credential_grant_ref == key)
            {
                return Err(CredentialResolverError::BindingMismatch);
            }
            Ok(CredentialMaterial::Bearer(
                "only-accepted-by-loopback-fixture".into(),
            ))
        })
    }
}

const STANDBY_CHECKPOINT: &str = "## Goal\nContinue the original task.\n\n## Constraints & Preferences\n- Preserve durable evidence.\n\n## Progress\n### Done\n- The original task and the remaining work are preserved.\n\n### In Progress\n- Continue from retained history.\n\n### Blocked\n- (none)\n\n## Key Decisions\n- Canonical provenance remains authoritative.\n\n## Next Steps\n1. Continue the conversation.\n\n## Critical Context\n- Use the retained tool results.";

impl Providers {
    pub async fn start(
        role: AuxiliaryModelRole,
        routes: [PinnedModelRoute; 2],
        content: Arc<dyn ContentStore>,
    ) -> Self {
        Self::start_with_standby(role, routes, content, true).await
    }

    pub async fn start_with_standby(
        role: AuxiliaryModelRole,
        routes: [PinnedModelRoute; 2],
        content: Arc<dyn ContentStore>,
        standby_succeeds: bool,
    ) -> Self {
        let rejected = || ProviderWireScenario {
            status: 503,
            content_type: "application/json".into(),
            chunks: vec![
                br#"{"error":{"code":"service_unavailable","message":"fixture"}}"#.to_vec(),
            ],
            hang_after_chunks: false,
        };
        let primary = MockProviderWire::start(rejected()).await.unwrap();
        let frames = [
            (
                "response.created",
                serde_json::json!({"type":"response.created","response":{"id":"local-role-response","model":routes[1].route.resolved_model_key}}),
            ),
            (
                "response.output_text.delta",
                // A compaction checkpoint must carry every required section;
                // unstructured text is rejected since 0181a49fd5.
                serde_json::json!({"type":"response.output_text.delta","output_index":0,"delta":STANDBY_CHECKPOINT}),
            ),
            (
                "response.completed",
                serde_json::json!({"type":"response.completed","response":{"id":"local-role-response","model":routes[1].route.resolved_model_key,"usage":{"input_tokens":11,"output_tokens":9,"total_tokens":20}}}),
            ),
        ];
        let response = ProviderWireScenario::successful(
            "text/event-stream",
            frames
                .map(|(kind, body)| format!("event: {kind}\ndata: {body}\n\n").into_bytes())
                .to_vec(),
        );
        let standby = MockProviderWire::start(if standby_succeeds {
            response
        } else {
            rejected()
        })
        .await
        .unwrap();
        let endpoints =
            [(&routes[0], &primary), (&routes[1], &standby)].map(|(m, server)| ProviderEndpoint {
                model_provider_id: m.route.model_provider_id,
                model_entry_id: m.route.model_entry_id,
                api_format: m.route.api_format,
                invocation_url: server.endpoint().parse().unwrap(),
                connect_timeout: Duration::from_secs(1),
                read_timeout: Duration::from_secs(3),
                total_timeout: Duration::from_secs(5),
                anthropic_version: None,
                aws_region: None,
                origin: ProviderEndpointOrigin::Direct,
            });
        let access = Arc::new(Access {
            role,
            routes,
            content,
        });
        let adapter = Arc::new(
            DirectModelProvider::new(
                Arc::new(StaticEndpointRegistry::new(endpoints, true).unwrap()),
                access.clone(),
                access,
                ProviderAdapterConfig::default(),
            )
            .unwrap(),
        );
        Self {
            primary,
            standby,
            adapter,
        }
    }

    pub async fn assert_same_input(&self) -> serde_json::Value {
        let a = self.primary.captured_requests().await;
        let b = self.standby.captured_requests().await;
        assert_eq!((a.len(), b.len()), (1, 1));
        let a: serde_json::Value = serde_json::from_slice(&a[0]).unwrap();
        let b: serde_json::Value = serde_json::from_slice(&b[0]).unwrap();
        assert_ne!(a["model"], b["model"]);
        assert_eq!(a["input"], b["input"]);
        assert_eq!(a["instructions"], b["instructions"]);
        b
    }
}
