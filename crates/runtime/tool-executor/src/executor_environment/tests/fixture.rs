use super::*;
use musteragent_capability::{CapabilityGrantSnapshot, CapabilityLocation};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_policy::{Decision, PermissionMode};
use musteragent_run::{RunContext, WorkerId};
use musteragent_runtime_capability::InvocationAuthorization;
use musteragent_runtime_contract::{TraceContext, TraceParent};
use musteragent_runtime_snapshot::{
    ModelRouteSnapshot, RunExecutionPolicySnapshot, ToolDefinitionSnapshot,
};
use musteragent_tool::{BuiltinToolCatalog, ToolInvocation, ToolInvocationStatus, ToolKey};
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};

pub(crate) struct Fixture {
    handler: ExecutorEnvironmentCapabilityHandler,
    pub(crate) attempt: AttemptContext,
    pub material: ToolExecutionMaterial,
    pub(crate) durable: DurableToolInvocation,
    pub invocation: CapabilityInvocation,
    pub(super) content: Arc<Content>,
    pub(super) directory: Arc<Directory>,
}

impl Fixture {
    pub fn new(
        key: &str,
        arguments: Value,
        result: Result<NativeEnvironmentPage, DialogueStoreError>,
    ) -> Self {
        let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
        Self::with_release(release, key, arguments, result)
    }

    pub fn with_release(
        release: ProductReleaseRef,
        key: &str,
        arguments: Value,
        result: Result<NativeEnvironmentPage, DialogueStoreError>,
    ) -> Self {
        let tenant_id = TenantId::generate();
        let actor = Actor {
            tenant_id,
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        };
        let run_context = RunContext {
            tenant_id,
            conversation_id: ConversationId::generate(),
            activity_id: ActivityId::generate(),
            activity_kind: musteragent_activity::ActivityKind::Chat,
            run_id: RunId::generate(),
            executor_participant_id: ParticipantId::generate(),
            requested_by_participant_id: ParticipantId::generate(),
            parent_run_id: None,
            delegation_id: None,
            execution_scope: ExecutionScope::Tenant { tenant_id },
            control_home: RuntimeHome::Cloud,
            execution_home: RuntimeHome::Cloud,
            home_epoch: HomeEpoch::new(1).unwrap(),
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
        };
        let attempt = AttemptContext {
            attempt_id: AttemptId::generate(),
            run_id: run_context.run_id,
            attempt_number: AttemptNumber::new(1).unwrap(),
            worker_id: WorkerId::new("environment-fixture").unwrap(),
            lease_id: LeaseId::generate(),
            fencing_token: FencingToken::new(1).unwrap(),
            lease_expires_at: Clock.now(),
        };
        let provider_id = CapabilityProviderId::generate();
        let fixtures: Value = serde_json::from_str(include_str!(
            "../../../../../../contracts/fixtures/executor-environment-tools.json"
        ))
        .unwrap();
        let definition = if fixtures.get(key).is_some() {
            musteragent_tool::BuiltinToolDefinition::from_released_document(fixtures[key].clone())
                .unwrap()
        } else {
            BuiltinToolCatalog::load_cloud()
                .unwrap()
                .by_key(&ToolKey::from_versioned_key(
                    VersionedKey::new(key).unwrap(),
                ))
                .unwrap()
                .clone()
        };
        let manifest = crate::builtin_capability_manifest_at_location(
            &definition,
            provider_id,
            CapabilityLocation::Cloud,
        )
        .unwrap();
        let tool = ToolDefinitionSnapshot {
            manifest: definition.pin(provider_id),
            execution_target: definition.target,
            concurrency_class: definition.concurrency,
            idempotency: definition.idempotency,
            file_operation: definition.file_operation.clone(),
            title: definition.title.clone(),
            description: definition.description.clone(),
            input_schema_json: definition.input_schema_json.clone(),
            output_schema_json: definition.output_schema_json.clone(),
        };
        let grant = CapabilityGrantSnapshot {
            key: manifest.key.clone(),
            provider_id,
            location: manifest.location,
            effect: manifest.effect,
            allowed_scopes: manifest.required_scopes.clone(),
            policy_decision: Decision::Allow,
        };
        let authorized_tools = musteragent_runtime_snapshot::authorized_tool_catalog(
            std::slice::from_ref(&tool),
            std::slice::from_ref(&grant),
        )
        .unwrap();
        let exposure_state =
            musteragent_discovery::ExposureState::from_initial(&authorized_tools, &[]).unwrap();
        let material = ToolExecutionMaterial {
            application_capabilities: vec![],
            product_release: Some(release),
            run_context,
            recovery_policy: NonZeroU32::new(8).unwrap().into(),
            actor,
            agent_id: AgentId::generate(),
            model: model(),
            active_model_route_ordinal: 0,
            auxiliary_models: vec![],
            model_stream_budget: RunExecutionPolicySnapshot::production_defaults().model_stream,
            model_liveness: None,
            tool,
            authorized_tools,
            discovery: None,
            exposure_state,
            capability_grant: grant,
            authorization: InvocationAuthorization {
                actor_scopes: manifest.required_scopes.clone(),
                available_contexts: BTreeSet::new(),
                policy_requires_approval: false,
                approval_forbidden: false,
                manifest_approval_not_applicable: false,
                approval_satisfied: false,
            },
            permission_mode: PermissionMode::Supervised,
            policy_basis: None,
            authorized_content: vec![],
            prior_tool_results: vec![],
            execution_grant_proof: None,
        };
        let bytes = serde_json::to_vec(&arguments).unwrap();
        let reference = reference(&bytes);
        let durable = DurableToolInvocation {
            sandbox_route: None,
            request_record_id: LedgerRecordId::generate(),
            invocation: ToolInvocation {
                invocation_id: InvocationId::generate(), run_id: run_context.run_id,
                requested_by_attempt_id: attempt.attempt_id, execution_attempt_id: Some(attempt.attempt_id),
                tool_call_id: ToolCallId::new("environment-read").unwrap(), tool_key: material.tool.manifest.key.clone(),
                capability_provider_id: provider_id, status: ToolInvocationStatus::Running, effect: manifest.effect, approval: manifest.approval,
                idempotency_key: IdempotencyKey::new("environment-read").unwrap(), request_ref: reference.clone(),
                result_ref: None, interaction_id: None,
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::ActivityV1(
                    musteragent_runtime_contract::execution_liveness::ActivityExecutionTiming {
                        policy: material.tool.manifest.execution_policy.liveness().unwrap().reference().clone(), business_deadline: None,
                    }),
            },
            retry_contract: material.tool.manifest.retry_contract, prebound_reconciliation_ref: None, recorded_outcome: None, interaction_resolution: None,
        };
        let i = &durable.invocation;
        let invocation = CapabilityInvocation {
            actor,
            tenant_id,
            conversation_id: run_context.conversation_id,
            run_id: run_context.run_id,
            requested_by_attempt_id: attempt.attempt_id,
            execution_attempt_id: attempt.attempt_id,
            execution_attempt_number: attempt.attempt_number,
            invocation_id: i.invocation_id,
            tool_call_id: i.tool_call_id.clone(),
            tool_key: i.tool_key.clone(),
            capability_key: manifest.key.clone(),
            capability_provider_id: provider_id,
            idempotency_key: i.idempotency_key.clone(),
            request_ref: reference.clone(),
            request_schema: material.tool.manifest.input_schema.clone(),
            result_schema: material.tool.manifest.output_schema.clone(),
            authorized_content: vec![],
            workspace_id: None,
            timing: i.timing.clone(),
            trace_context: TraceContext {
                traceparent: TraceParent::new(
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                )
                .unwrap(),
                tracestate: None,
            },
            execution_grant_proof: None,
        };
        let content = Arc::new(Content {
            actor,
            request: reference,
            bytes,
            output: Mutex::new(None),
        });
        let directory = Arc::new(Directory {
            calls: Mutex::new(vec![]),
            result,
        });
        let handler = ExecutorEnvironmentCapabilityHandler::new(
            manifest,
            content.clone(),
            directory.clone(),
            Arc::new(Clock),
        );
        Self {
            handler,
            attempt,
            material,
            durable,
            invocation,
            content,
            directory,
        }
    }
    pub async fn invoke(&self) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        self.handler
            .invoke(
                &self.attempt,
                &self.material,
                &self.durable,
                self.invocation.clone(),
            )
            .await
    }
    pub async fn success(&self) -> Value {
        let DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Succeeded(result)) =
            self.invoke().await.unwrap()
        else {
            panic!("not succeeded")
        };
        result.validate().unwrap();
        let output = self.content.output.lock().unwrap();
        let bytes = output.as_ref().unwrap();
        verify_content(&result.result_ref, bytes).unwrap();
        let output: Value = serde_json::from_slice(bytes).unwrap();
        let schema: Value = serde_json::from_slice(&self.material.tool.output_schema_json).unwrap();
        jsonschema::validator_for(&schema)
            .unwrap()
            .validate(&output)
            .unwrap();
        output
    }
}

fn model() -> ModelRouteSnapshot {
    use musteragent_model::*;
    let key = |s| VersionedKey::new(s).unwrap();
    ModelRouteSnapshot {
        requested_model_class: musteragent_runtime_contract::ModelClass::new("chat.balanced")
            .unwrap(),
        route: ModelRoute {
            route_ref: key("fixture.route@1"),
            resolved_model_key: "fixture".into(),
            model_provider_id: ModelProviderId::from_uuid(uuid::Uuid::now_v7()).unwrap(),
            model_entry_id: ModelEntryId::from_uuid(uuid::Uuid::now_v7()).unwrap(),
            api_format: ApiFormat::OpenAiResponses,
            adapter_version: key("fixture.adapter@1"),
            protocol_version: key("fixture.protocol@1"),
            context_window: NonZeroU64::new(4096).unwrap(),
            max_output_tokens: NonZeroU64::new(256).unwrap(),
            capabilities: BTreeSet::new(),
            parameters: BTreeMap::new(),
            fallback_route_refs: vec![],
            pricing_version_ref: PricingVersionRef::new("catalog:fixture.price@1").unwrap(),
            provider_state_policy: ProviderStatePolicy::Stateless,
        },
        credential_grant_ref: CredentialGrantRef::new("grant_fixture").unwrap(),
        fallback_candidates: vec![],
    }
}
