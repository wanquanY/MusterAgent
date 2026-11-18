use super::*;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::*;
use musteragent_runtime_content::ContentStoreError;
use serde_json::{Value, json};
use std::sync::Mutex;

pub(crate) mod fixture;
use fixture::Fixture;

struct Clock;
impl RuntimeClock for Clock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(1_791_870_000_000)
    }
}
struct Content {
    actor: Actor,
    request: ContentRef,
    bytes: Vec<u8>,
    output: Mutex<Option<Vec<u8>>>,
}
fn reference(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").unwrap(),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}
impl ContentStore for Content {
    fn read<'a>(
        &'a self,
        tenant: TenantId,
        request: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        assert_eq!(tenant, self.actor.tenant_id);
        assert_eq!(request, &self.request);
        Box::pin(async { Ok(self.bytes.clone()) })
    }
    fn put(
        &self,
        command: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        assert_eq!(command.tenant_id, self.actor.tenant_id);
        assert_eq!(command.media_type.as_str(), "application/json");
        let reference = reference(&command.bytes);
        *self.output.lock().unwrap() = Some(command.bytes);
        Box::pin(async { Ok(reference) })
    }
}
struct Directory {
    calls: Mutex<Vec<(Actor, AgentId, ExecutorEnvironmentQuery)>>,
    result: Result<NativeEnvironmentPage, DialogueStoreError>,
}
impl ExecutorEnvironmentDirectory for Directory {
    fn read(
        &self,
        actor: Actor,
        agent: AgentId,
        query: ExecutorEnvironmentQuery,
    ) -> ApplicationFuture<'_, Result<NativeEnvironmentPage, DialogueStoreError>> {
        self.calls.lock().unwrap().push((actor, agent, query));
        Box::pin(async { self.result.clone() })
    }
}
fn page() -> NativeEnvironmentPage {
    NativeEnvironmentPage {
        environments: vec![NativeEnvironment {
            membership_id: uuid::Uuid::from_u128(1),
            membership_epoch: 3,
            device_id: DeviceId::generate(),
            installation_id: DeviceInstallationId::generate(),
            surface: Some("desktop".into()),
            display_name: None,
            platform: None,
            connection_observation: None,
            last_authenticated_request_at_ms: Some(90),
            consent: ExecutorEnvironmentConsent::Active,
            capabilities: vec![ExecutorEnvironmentCapability {
                key: "files.read@1".into(),
                policy_decision: "allow".into(),
                allowed_scopes: vec!["files.read".into()],
            }],
            capabilities_complete: true,
        }],
        next_membership_id: None,
        observed_at_ms: 100,
        permission_revision: 2,
    }
}

#[tokio::test]
async fn registered_list_injects_sealed_identity_and_does_not_claim_online_or_capacity() {
    for after in [None, Some(uuid::Uuid::from_u128(2))] {
        let mut page = page();
        page.environments[0].connection_observation = Some((90, 135));
        let f = Fixture::new(
            "environments.list@1",
            json!({"after_membership_id":after,"limit":5}),
            Ok(page),
        );
        let output = f.success().await;
        let calls = f.directory.calls.lock().unwrap();
        assert_eq!(
            *calls,
            vec![(
                f.material.actor,
                f.material.agent_id,
                ExecutorEnvironmentQuery::List { after, limit: 5 }
            )]
        );
        let rows = output["environments"].as_array().unwrap();
        assert_eq!(rows.len(), if after.is_none() { 2 } else { 1 });
        assert_eq!(rows.last().unwrap()["connection"]["state"], "unknown");
        assert_eq!(rows.last().unwrap()["capacity"]["state"], "unknown");
        assert_eq!(
            rows.last().unwrap()["execution"],
            json!({
                "consent": "active", "dispatch_requires_fresh_validation": true
            })
        );
        assert_eq!(output["coverage"]["web_local_execution"], false);
    }
}

#[tokio::test]
async fn inspect_uses_same_registered_schema_for_cloud_and_native_without_inventing_authority() {
    for kind in ["cloud", "native"] {
        for view in ["summary", "capabilities", "eligibility"] {
            let reference = if kind == "cloud" {
                json!({"kind":kind})
            } else {
                json!({"kind":kind,"membership_id":uuid::Uuid::from_u128(1),"membership_epoch":3})
            };
            let f = Fixture::new(
                "environments.inspect@1",
                json!({"environment_ref":reference,"view":view}),
                Ok(page()),
            );
            let output = f.success().await;
            let result = &output["environment"];
            assert_eq!(result["environment_ref"], reference);
            assert_eq!(
                f.directory.calls.lock().unwrap().len(),
                usize::from(kind == "native")
            );
            assert_eq!(
                result["execution"]["dispatch_requires_fresh_validation"],
                true
            );
            if kind == "native" {
                assert_eq!(result["capabilities_complete"], view != "summary");
                assert_eq!(
                    result["capabilities"].as_array().unwrap().len(),
                    usize::from(view != "summary")
                );
            } else {
                assert_eq!(result["admitted_tools"].is_array(), view != "summary");
            }
        }
    }
}

#[tokio::test]
async fn v1_projection_preserves_its_closed_environment_contract() {
    let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
    let reference =
        json!({"kind":"native","membership_id":uuid::Uuid::from_u128(1),"membership_epoch":3});
    for (key, args) in [
        ("environments.list@1", json!({})),
        (
            "environments.inspect@1",
            json!({"environment_ref":reference,"view":"eligibility"}),
        ),
    ] {
        let f = Fixture::with_release(release.clone(), key, args, Ok(page()));
        let output = f.success().await;
        let device = output
            .get("environment")
            .unwrap_or_else(|| &output["environments"][1]);
        assert_eq!(
            device["execution"],
            json!({"consent":"active","dispatch_requires_fresh_validation":true})
        );
        assert_eq!(device["connection"]["state"], "unknown");
    }
}

#[tokio::test]
async fn current_list_distinguishes_unknown_consent_from_a_reachable_device() {
    let mut page = page();
    let device = &mut page.environments[0];
    device.connection_observation = Some((50, 45_050));
    device.consent = ExecutorEnvironmentConsent::Unknown;
    device.capabilities.clear();
    device.capabilities_complete = false;
    let f = Fixture::new("environments.list@2", json!({}), Ok(page));
    let result = f.success().await;
    let device = &result["environments"][1];
    assert_eq!(device["connection"]["state"], "reachable");
    assert_eq!(
        device["execution"],
        json!({"consent":"unknown",
        "consent_next_step":"inspect_environment","dispatch_requires_fresh_validation":true})
    );
}

#[tokio::test]
async fn current_eligibility_keeps_consent_actions_independent_of_presence() {
    use ExecutorEnvironmentConsent as Consent;

    let reference =
        json!({"kind":"native","membership_id":uuid::Uuid::from_u128(1),"membership_epoch":3});
    for (consent, step) in [
        (Consent::Active, "none"),
        (Consent::NotGranted, "obtain_explicit_consent"),
        (Consent::Expired, "obtain_explicit_consent"),
        (Consent::Changed, "review_changed_consent"),
        (Consent::Unknown, "inspect_environment"),
        (Consent::Unavailable, "retry_status_check"),
    ] {
        for (observation, state) in [
            (Some((100, 45_100)), "reachable"),
            (Some((99, 45_099)), "unreachable"),
            (None, "unknown"),
        ] {
            let mut page = page();
            page.observed_at_ms = 45_099;
            let device = &mut page.environments[0];
            device.consent = consent;
            device.connection_observation = observation;
            if consent != Consent::Active {
                device.capabilities_complete = false;
                device.capabilities.clear();
            }
            let f = Fixture::new(
                "environments.inspect@2",
                json!({"environment_ref":reference,"view":"eligibility"}),
                Ok(page),
            );
            let output = f.success().await;
            let device = &output["environment"];
            assert_eq!(device["connection"]["state"], state);
            assert_eq!(
                device["connection"]["expires_at_ms"],
                json!(observation.map(|(_, end)| end))
            );
            assert_eq!(
                device["execution"],
                json!({"consent":consent,"consent_next_step":step,
                "dispatch_requires_fresh_validation":true})
            );
            assert_eq!(device["capacity"]["state"], "unknown");
        }
    }
}

#[tokio::test]
async fn sealed_scope_and_closed_arguments_are_checked_before_directory_access() {
    for mutation in 0..6 {
        let args = if mutation == 0 {
            json!({"agent_id":AgentId::generate().to_string()})
        } else {
            json!({})
        };
        let mut f = Fixture::new("environments.list@1", args, Ok(page()));
        match mutation {
            1 => f.invocation.actor.principal_id = PrincipalId::generate(),
            2 => f.invocation.workspace_id = Some(WorkspaceId::generate()),
            3 => f.material.run_context.parent_run_id = Some(RunId::generate()),
            4 => f.material.run_context.delegation_id = Some(DelegationId::generate()),
            5 => f.material.authorization.actor_scopes.clear(),
            _ => {}
        }
        assert!(f.invoke().await.is_err(), "{mutation}");
        assert!(f.directory.calls.lock().unwrap().is_empty());
        assert!(f.content.output.lock().unwrap().is_none());
    }
}

#[tokio::test]
async fn unavailable_directory_is_failure_not_empty_or_successful_cloud_fallback() {
    for (error, expected, retryable) in [
        (
            DialogueStoreError::Unavailable,
            ErrorCode::CapabilityUnavailable,
            false,
        ),
        (
            DialogueStoreError::Infrastructure,
            ErrorCode::DependencyUnavailable,
            true,
        ),
        (
            DialogueStoreError::Corrupt,
            ErrorCode::RuntimeSnapshotInvalid,
            false,
        ),
    ] {
        let f = Fixture::new("environments.list@1", json!({}), Err(error));
        let result = f.invoke().await.unwrap_err();
        assert_eq!(result.code, expected);
        assert_eq!(result.retryable, retryable);
        assert!(f.content.output.lock().unwrap().is_none());
    }
}

#[tokio::test]
async fn invalid_directory_facts_cannot_be_committed_as_a_successful_model_observation() {
    let mut page = page();
    page.permission_revision = 0;
    let f = Fixture::new("environments.list@1", json!({}), Ok(page));
    assert_eq!(
        f.invoke().await.unwrap_err().code,
        ErrorCode::RuntimeSnapshotInvalid
    );
    assert!(f.content.output.lock().unwrap().is_none());
}
