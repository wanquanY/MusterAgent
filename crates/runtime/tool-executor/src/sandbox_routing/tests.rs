use super::*;
use crate::executor_environment::tests::fixture::Fixture;
use musteragent_capability::{
    BrowserJobEngine, BrowserJobProgram, BrowserLeaseProof, BrowserResourceLimits,
    BrowserSecurityProfile,
};
use musteragent_identity::*;
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_runtime_capability::{
    BrowserJobDelivery, BrowserJobDeliveryState, DialogueStoreError, SandboxRoutingStore,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_sandbox::*;
use musteragent_tool::ToolEvent;
use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Mutex,
};

struct Clock;
impl RuntimeClock for Clock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(1_791_870_000_000)
    }
}
struct State {
    invocation: DurableToolInvocation,
    events: Vec<LedgerPayload>,
    executors: Vec<SandboxExecutor>,
    job: Option<DurableBrowserJob>,
}
struct Store {
    attempt: AttemptContext,
    input: SandboxRouteInput,
    fail_commit: bool,
    fail_enqueue: bool,
    state: Mutex<State>,
}
impl Store {
    fn new(f: &Fixture, input: SandboxRouteInput) -> Self {
        Self {
            attempt: f.attempt.clone(),
            input,
            fail_commit: false,
            fail_enqueue: false,
            state: Mutex::new(State {
                invocation: f.durable.clone(),
                events: vec![],
                executors: vec![],
                job: None,
            }),
        }
    }
}
impl SandboxRoutingStore for Store {
    fn context<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        _: &'a ToolExecutionMaterial,
        _: &'a CapabilityInvocation,
        contract: BrowserProviderContract,
        request: &'a BrowserJobRequest,
    ) -> ApplicationFuture<'a, Result<SandboxRouteInput, BrowserProviderRuntimeError>> {
        Box::pin(async move {
            assert_eq!(attempt, &self.attempt);
            assert!(contract.accepts_request(request));
            Ok(self.input)
        })
    }
    fn commit_route<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        commit: SandboxRouteCommit,
    ) -> ApplicationFuture<'a, Result<DurableToolInvocation, BrowserProviderRuntimeError>> {
        Box::pin(async move {
            assert_eq!(attempt, &self.attempt);
            if self.fail_commit {
                return Err(BrowserProviderRuntimeError::DurabilityUnavailable);
            }
            commit.route.validate().expect("canonical route");
            let mut state = self.state.lock().expect("state");
            assert_eq!(
                commit.invocation_id,
                state.invocation.invocation.invocation_id
            );
            match state.invocation.sandbox_route {
                Some(old) if old != commit.route => {
                    return Err(BrowserProviderRuntimeError::CorruptDurableState);
                }
                Some(_) => {}
                None => {
                    state
                        .events
                        .push(LedgerPayload::DomainEvent(DomainEvent::Tool(
                            commit.event(),
                        )));
                    state.invocation.sandbox_route = Some(commit.route);
                }
            }
            Ok(state.invocation.clone())
        })
    }
    fn enqueue<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        invocation: &'a CapabilityInvocation,
        route: SandboxRouteRecord,
        contract: BrowserProviderContract,
        request: BrowserJobRequest,
    ) -> ApplicationFuture<'a, Result<DurableBrowserJob, BrowserProviderRuntimeError>> {
        Box::pin(async move {
            assert_eq!(attempt, &self.attempt);
            let mut state = self.state.lock().expect("state");
            assert_eq!(
                state.invocation.sandbox_route,
                Some(route),
                "persist before dispatch"
            );
            assert_eq!(
                state.events,
                vec![LedgerPayload::DomainEvent(DomainEvent::Tool(
                    ToolEvent::SandboxRouted {
                        invocation_id: invocation.invocation_id,
                        route,
                    }
                ))]
            );
            if self.fail_enqueue {
                return Err(BrowserProviderRuntimeError::ProviderUnavailable);
            }
            if let Some(job) = &state.job {
                assert_eq!(job.delivery.request, request);
                return Ok(job.clone());
            }
            request.validate()?;
            assert!(contract.accepts_request(&request));
            let job = DurableBrowserJob {
                delivery: BrowserJobDelivery {
                    request,
                    capability: musteragent_capability::CapabilityKey::from_versioned_key(
                        VersionedKey::new(contract.as_str()).expect("key"),
                    ),
                    lease: BrowserLeaseProof {
                        lease_id: BrowserLeaseId::generate(),
                        tenant_id: invocation.tenant_id,
                        principal_id: invocation.actor.principal_id,
                        browser_session_id: BrowserSessionId::generate(),
                        page_instance_id: PageInstanceId::generate(),
                        provider_id: invocation.capability_provider_id,
                        epoch: BrowserLeaseEpoch::new(1).expect("epoch"),
                        fence: FencingToken::new(1).expect("fence"),
                    },
                    state: BrowserJobDeliveryState::Queued,
                    last_provider_sequence: 0,
                    terminal_result: None,
                },
                delivery_sequence: 1,
                created_at: Clock.now(),
                updated_at: Clock.now(),
            };
            state.executors.push(route.decision.executor);
            state.job = Some(job.clone());
            Ok(job)
        })
    }
}
fn fixture() -> Fixture {
    Fixture::new(
        "data.python.run@11",
        serde_json::json!({}),
        Err(DialogueStoreError::Unavailable),
    )
}
fn input() -> SandboxRouteInput {
    SandboxRouteInput {
        compatibility: SandboxCompatibility::BrowserCompatible,
        policy: SandboxRoutingPolicy::PreferBrowser,
        trigger: SandboxTrigger::Interactive,
        surface: SandboxClientSurface::Web,
        availability: SandboxAvailability {
            online_browser_clients: 1,
            server_available: true,
        },
    }
}
fn request(f: &Fixture) -> BrowserJobRequest {
    BrowserJobRequest {
        execution_liveness: None,
        job_id: f
            .invocation
            .invocation_id
            .to_string()
            .parse()
            .expect("job identity"),
        invocation_id: f.invocation.invocation_id,
        conversation_id: f.invocation.conversation_id,
        engine: BrowserJobEngine::Python,
        runtime_release_ref: VersionedKey::new("browser-runtime@2").expect("key"),
        program: BrowserJobProgram::Python {
            source_ref: f.invocation.request_ref.clone(),
            entrypoint: "main.py".into(),
            environment: None,
            require_prepared_environment: false,
        },
        inputs: vec![musteragent_capability::BrowserInput {
            path: "main.py".into(),
            content_ref: f.invocation.request_ref.clone(),
        }],
        declared_outputs: vec![],
        security_profile: BrowserSecurityProfile::SafeOffline,
        limits: BrowserResourceLimits {
            wall_time_millis: NonZeroU64::new(1_000).expect("limit"),
            maximum_input_bytes: NonZeroU64::new(4096).expect("limit"),
            maximum_output_bytes: NonZeroU64::new(4096).expect("limit"),
            maximum_files: NonZeroU32::MIN,
            maximum_stdout_bytes: NonZeroU64::new(1024).expect("limit"),
            maximum_stderr_bytes: NonZeroU64::new(1024).expect("limit"),
        },
    }
}
async fn dispatch(
    store: &Store,
    f: &Fixture,
    durable: &DurableToolInvocation,
) -> Result<DurableBrowserJob, BrowserProviderRuntimeError> {
    SandboxDispatch {
        clock: &Clock,
        attempt: &f.attempt,
        material: &f.material,
        durable,
        invocation: &f.invocation,
        contract: BrowserProviderContract::PythonExecute,
        request: request(f),
    }
    .enqueue_to(store)
    .await
}
#[tokio::test]
async fn browser_route_is_persisted_and_visible_in_ledger_before_dispatch() {
    let f = fixture();
    let store = Store::new(&f, input());
    let job = dispatch(&store, &f, &f.durable).await.expect("browser job");
    let state = store.state.lock().expect("state");
    assert_eq!(state.executors, vec![SandboxExecutor::Browser]);
    assert_eq!(
        job.delivery.request.invocation_id,
        f.invocation.invocation_id
    );
    let route = state.invocation.sandbox_route.expect("persisted route");
    assert_eq!(
        route.decision.reason,
        SandboxRouteReason::BrowserClientOnline
    );
    let LedgerPayload::DomainEvent(DomainEvent::Tool(ToolEvent::SandboxRouted {
        route: event,
        ..
    })) = &state.events[0]
    else {
        panic!("route event")
    };
    assert_eq!(route.canonical_bytes(), event.canonical_bytes());
}
#[tokio::test]
async fn server_routing_covers_skills_background_mobile_api_and_browser_absence() {
    for (change, reason) in [
        (0, SandboxRouteReason::CompatibilityRequiresServer),
        (1, SandboxRouteReason::BackgroundTrigger),
        (2, SandboxRouteReason::MobileSurface),
        (3, SandboxRouteReason::ApiSurface),
        (4, SandboxRouteReason::NoOnlineBrowserClient),
        (5, SandboxRouteReason::ScheduledTrigger),
    ] {
        let f = fixture();
        let mut context = input();
        match change {
            0 => context.compatibility = SandboxCompatibility::ServerRequired,
            1 => context.trigger = SandboxTrigger::Background,
            2 => context.surface = SandboxClientSurface::Mobile,
            3 => context.surface = SandboxClientSurface::Api,
            4 => context.availability.online_browser_clients = 0,
            _ => context.trigger = SandboxTrigger::Scheduled,
        }
        let store = Store::new(&f, context);
        dispatch(&store, &f, &f.durable).await.expect("server job");
        let state = store.state.lock().expect("state");
        assert_eq!(state.executors, vec![SandboxExecutor::Server]);
        assert_eq!(
            state
                .invocation
                .sandbox_route
                .expect("route")
                .decision
                .reason,
            reason
        );
    }
}
#[tokio::test]
async fn no_executor_returns_typed_error_without_persistence_or_dispatch() {
    let f = fixture();
    let mut context = input();
    context.availability = SandboxAvailability {
        online_browser_clients: 0,
        server_available: false,
    };
    let store = Store::new(&f, context);
    assert!(matches!(
        dispatch(&store, &f, &f.durable).await,
        Err(BrowserProviderRuntimeError::SandboxRoute(
            SandboxRouteError::ServerUnavailable(SandboxRouteReason::NoOnlineBrowserClient)
        ))
    ));
    let state = store.state.lock().expect("state");
    assert!(state.events.is_empty());
    assert!(state.executors.is_empty());
    assert!(state.invocation.sandbox_route.is_none());
}
#[tokio::test]
async fn failed_atomic_route_commit_cannot_dispatch() {
    let f = fixture();
    let mut store = Store::new(&f, input());
    store.fail_commit = true;
    assert!(matches!(
        dispatch(&store, &f, &f.durable).await,
        Err(BrowserProviderRuntimeError::DurabilityUnavailable)
    ));
    let state = store.state.lock().expect("state");
    assert!(state.events.is_empty());
    assert!(state.job.is_none());
}
#[tokio::test]
async fn restart_reuses_route_and_event_without_switching_to_available_server() {
    let f = fixture();
    let mut store = Store::new(&f, input());
    dispatch(&store, &f, &f.durable)
        .await
        .expect("initial dispatch");
    let reloaded = store.state.lock().expect("state").invocation.clone();
    store.input.surface = SandboxClientSurface::Api;
    dispatch(&store, &f, &reloaded).await.expect("original job");
    store.fail_enqueue = true;
    assert!(matches!(
        dispatch(&store, &f, &reloaded).await,
        Err(BrowserProviderRuntimeError::ProviderUnavailable)
    ));
    let state = store.state.lock().expect("state");
    assert_eq!(state.events.len(), 1);
    assert_eq!(state.executors, vec![SandboxExecutor::Browser]);
}

#[tokio::test]
async fn server_required_tool_metadata_cannot_be_downgraded_by_context() {
    let f = Fixture::new(
        "sandbox.environment.prepare@1",
        serde_json::json!({}),
        Err(DialogueStoreError::Unavailable),
    );
    let mut context = input();
    context.policy = SandboxRoutingPolicy::BrowserOnly;
    let store = Store::new(&f, context);
    assert!(matches!(
        dispatch(&store, &f, &f.durable).await,
        Err(BrowserProviderRuntimeError::SandboxRoute(
            SandboxRouteError::PolicyConflict
        ))
    ));
    assert!(store.state.lock().expect("state").events.is_empty());
}

#[tokio::test]
async fn mismatched_pinned_tool_contract_cannot_choose_or_dispatch_an_executor() {
    let mut f = fixture();
    f.material.tool.manifest.input_schema.schema_hash =
        musteragent_content_ref::Sha256::digest(b"changed contract");
    let store = Store::new(&f, input());
    assert!(matches!(
        dispatch(&store, &f, &f.durable).await,
        Err(BrowserProviderRuntimeError::CorruptDurableState)
    ));
    let state = store.state.lock().expect("state");
    assert!(state.events.is_empty());
    assert!(state.executors.is_empty());
}

#[test]
fn unavailable_executor_error_remains_visible_in_tool_failure() {
    let outcome =
        crate::browser_execution::gateway_failure(BrowserProviderRuntimeError::SandboxRoute(
            SandboxRouteError::ServerUnavailable(SandboxRouteReason::NoOnlineBrowserClient),
        ))
        .expect("failure outcome");
    let musteragent_runtime_capability::CapabilityOutcome::Failed {
        error,
        retry_disposition,
        ..
    } = outcome
    else {
        panic!("explicit failure")
    };
    assert_eq!(
        error.code,
        musteragent_runtime_contract::ErrorCode::CapabilityUnavailable
    );
    assert!(error.safe_message.as_str().contains("unavailable"));
    assert!(
        error
            .safe_message
            .as_str()
            .contains("NoOnlineBrowserClient")
    );
    assert_eq!(
        retry_disposition,
        musteragent_runtime_contract::RetryDisposition::DoNotRetry
    );
}
