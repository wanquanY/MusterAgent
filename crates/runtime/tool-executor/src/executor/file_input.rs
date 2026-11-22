//! Successor file reads restore exact durably authorized media. Text references
//! alone never authorize IO; parts the active route cannot receive natively are
//! degraded into route-presentation notices without reading their source bytes.
use super::*;
use musteragent_context::present_file_read_parts_for_route;
use musteragent_model::AuxiliaryModelRole;
use musteragent_runtime_model::{InputAttachmentKind, input_attachment_kind};

#[derive(serde::Deserialize)]
struct Output {
    #[serde(default)]
    source_content: Vec<VisionToolContentRef>,
    #[serde(default)]
    visual_content: Vec<VisionToolContentRef>,
}
fn parts(
    text: &str,
    authorized: &[ContentRef],
) -> Result<Vec<CanonicalContentPart>, LoopPortError> {
    let output: Output =
        serde_json::from_str(text).map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
    let mut references = output
        .source_content
        .into_iter()
        .chain(output.visual_content)
        .map(VisionToolContentRef::decode)
        .collect::<Result<Vec<_>, _>>()?;
    if references.len() > 64 || references.len() != authorized.len() {
        return Err(port(ErrorCode::ContentIntegrityFailed, false));
    }
    let mut ids = std::collections::BTreeSet::new();
    references
        .drain(..)
        .map(|reference| {
            if !authorized.contains(&reference) || !ids.insert(reference.content_id) {
                return Err(port(ErrorCode::ContentIntegrityFailed, false));
            }
            Ok(match input_attachment_kind(&reference) {
                InputAttachmentKind::Image => CanonicalContentPart::Image(reference),
                InputAttachmentKind::Audio => CanonicalContentPart::Audio(reference),
                InputAttachmentKind::Video => CanonicalContentPart::Video(reference),
                InputAttachmentKind::File => CanonicalContentPart::File(reference),
                InputAttachmentKind::Text => {
                    return Err(port(ErrorCode::ContentIntegrityFailed, false));
                }
            })
        })
        .collect()
}
fn vision_tool_available(material: &ToolExecutionMaterial) -> bool {
    let exposed = material
        .authorized_tools
        .get_by_logical_key("vision.analyze")
        .is_some_and(|tool| {
            material
                .exposure_state
                .ordinal_for(&tool.tool_ref)
                .is_some()
        });
    let has_auxiliary_vision = material.auxiliary_models.iter().any(|auxiliary| {
        auxiliary.role == AuxiliaryModelRole::Vision && auxiliary.model.route.accepts_native_image()
    });
    exposed && has_auxiliary_vision
}

impl DurableToolExecutor {
    pub(super) async fn file_input_content(
        &self,
        material: &ToolExecutionMaterial,
        authorized: &[ContentRef],
        text: String,
    ) -> Result<Vec<CanonicalContentPart>, LoopPortError> {
        let active = active_pinned_model(&material.model, material.active_model_route_ordinal)
            .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        materialize(
            self.content.as_ref(),
            material.run_context.tenant_id,
            &active.route,
            vision_tool_available(material),
            authorized,
            text,
        )
        .await
    }
}
async fn materialize(
    store: &dyn ContentStore,
    tenant: musteragent_identity::TenantId,
    route: &musteragent_model::ModelRoute,
    vision_tool_available: bool,
    authorized: &[ContentRef],
    text: String,
) -> Result<Vec<CanonicalContentPart>, LoopPortError> {
    let parts = parts(&text, authorized)?;
    let presented = present_file_read_parts_for_route(&parts, route, vision_tool_available)
        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
    let mut content = text_content(text)?;
    for part in presented {
        match &part {
            CanonicalContentPart::Text(_) => content.push(part),
            CanonicalContentPart::Image(reference)
            | CanonicalContentPart::Audio(reference)
            | CanonicalContentPart::Video(reference)
            | CanonicalContentPart::File(reference) => {
                let bytes = store
                    .read(tenant, reference)
                    .await
                    .map_err(port_from_content_read)?;
                verify_content(reference, &bytes)
                    .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
                content.push(part);
            }
        }
    }
    Ok(content)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_media_requires_same_complete_durable_grant() {
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("audio/wav").unwrap(),
            byte_length: 4,
            sha256: Sha256::digest(b"wave"),
        };
        let value = serde_json::json!({"content_id":reference.content_id.to_string(),"media_type":reference.media_type.as_str(),"byte_length":reference.byte_length,"sha256":reference.sha256.to_string()});
        let text = serde_json::json!({"source_content":[value.clone()]}).to_string();
        assert_eq!(
            parts(&text, std::slice::from_ref(&reference)).unwrap(),
            vec![CanonicalContentPart::Audio(reference.clone())]
        );
        assert!(parts(&text, &[]).is_err());
        let duplicate = serde_json::json!({"source_content":[value.clone(),value]}).to_string();
        assert!(parts(&duplicate, &[reference.clone(), reference.clone()]).is_err());
        let mut changed = reference;
        changed.byte_length += 1;
        assert!(parts(&text, &[changed]).is_err());
    }
    fn route() -> musteragent_model::ModelRoute {
        use musteragent_model::*;
        let id = "11111111-1111-4111-8111-111111111111";
        ModelRoute {
            route_ref: musteragent_identity::VersionedKey::new("fixture.route@1").unwrap(),
            resolved_model_key: "fixture".into(),
            model_provider_id: id.parse().unwrap(),
            model_entry_id: id.parse().unwrap(),
            api_format: ApiFormat::GoogleGenerateContent,
            adapter_version: musteragent_identity::VersionedKey::new("fixture.adapter@1").unwrap(),
            protocol_version: musteragent_identity::VersionedKey::new("fixture.protocol@1")
                .unwrap(),
            context_window: std::num::NonZeroU64::new(10000).unwrap(),
            max_output_tokens: std::num::NonZeroU64::new(1000).unwrap(),
            capabilities: [
                "input.image",
                "input.audio",
                "input.video",
                "input.document",
            ]
            .into_iter()
            .map(|cap| musteragent_identity::DottedKey::new(cap).unwrap())
            .collect(),
            parameters: Default::default(),
            fallback_route_refs: vec![],
            pricing_version_ref: PricingVersionRef::new("catalog:fixture.price@1").unwrap(),
            provider_state_policy: ProviderStatePolicy::Stateless,
        }
    }
    #[tokio::test]
    async fn source_media_reconstruction_checks_route_before_io_and_hash_before_observation() {
        use musteragent_runtime_content::PutContent;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Source {
            reads: AtomicUsize,
            bytes: Vec<u8>,
        }
        impl ContentStore for Source {
            fn put(
                &self,
                _: PutContent,
            ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
                Box::pin(async { panic!("read only") })
            }
            fn read<'a>(
                &'a self,
                _: musteragent_identity::TenantId,
                _: &'a ContentRef,
            ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
                Box::pin(async move {
                    self.reads.fetch_add(1, Ordering::SeqCst);
                    Ok(self.bytes.clone())
                })
            }
        }
        let references = ["image/png", "audio/wav", "video/mp4", "application/pdf"]
            .into_iter()
            .map(|mime| ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new(mime).unwrap(),
                byte_length: 4,
                sha256: Sha256::digest(b"data"),
            })
            .collect::<Vec<_>>();
        let text = serde_json::json!({"source_content": references.iter().map(|r| serde_json::json!({"content_id":r.content_id.to_string(), "media_type":r.media_type.as_str(), "byte_length":r.byte_length,"sha256":r.sha256.to_string()})).collect::<Vec<_>>()}).to_string();
        let tenant = "11111111-1111-4111-8111-111111111111".parse().unwrap();
        let store = Source {
            reads: AtomicUsize::new(0),
            bytes: b"data".to_vec(),
        };
        let mut route = route();
        route
            .capabilities
            .remove(&musteragent_identity::DottedKey::new("input.document").expect("capability"));
        let degraded = materialize(&store, tenant, &route, true, &references, text.clone())
            .await
            .expect("unsupported document degrades into notice instead of failing");
        assert!(matches!(
            degraded.as_slice(),
            [
                CanonicalContentPart::Text(_),
                CanonicalContentPart::Image(_),
                CanonicalContentPart::Audio(_),
                CanonicalContentPart::Video(_),
                CanonicalContentPart::Text(_),
            ]
        ));
        let CanonicalContentPart::Text(doc_notice) = &degraded[4] else {
            panic!("unsupported document must degrade to a text notice");
        };
        assert!(
            doc_notice
                .as_str()
                .contains(&references[3].content_id.to_string())
        );
        assert!(doc_notice.as_str().contains("application/pdf"));
        assert!(
            doc_notice
                .as_str()
                .contains("cannot receive this document as original bytes")
        );
        assert_eq!(
            store.reads.load(Ordering::SeqCst),
            3,
            "unsupported document bytes must not be read"
        );
        store.reads.store(0, Ordering::SeqCst);
        route
            .capabilities
            .insert(musteragent_identity::DottedKey::new("input.document").expect("capability"));
        let output = materialize(&store, tenant, &route, true, &references, text.clone())
            .await
            .expect("full multimodal route inlines all supported media");
        assert!(matches!(
            output.as_slice(),
            [
                CanonicalContentPart::Text(_),
                CanonicalContentPart::Image(_),
                CanonicalContentPart::Audio(_),
                CanonicalContentPart::Video(_),
                CanonicalContentPart::File(_)
            ]
        ));
        assert_eq!(store.reads.load(Ordering::SeqCst), 4);
        let bad = Source {
            reads: AtomicUsize::new(0),
            bytes: b"oops".to_vec(),
        };
        assert!(
            materialize(&bad, tenant, &route, true, &references, text)
                .await
                .is_err()
        );
        assert_eq!(bad.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn text_only_route_degrades_file_read_image_with_vision_tool_guidance_when_available() {
        use musteragent_runtime_content::PutContent;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct CountingStore {
            reads: AtomicUsize,
            bytes: Vec<u8>,
        }
        impl ContentStore for CountingStore {
            fn put(
                &self,
                _: PutContent,
            ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
                Box::pin(async { panic!("read only") })
            }
            fn read<'a>(
                &'a self,
                _: musteragent_identity::TenantId,
                _: &'a ContentRef,
            ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
                Box::pin(async move {
                    self.reads.fetch_add(1, Ordering::SeqCst);
                    Ok(self.bytes.clone())
                })
            }
        }
        let image_ref = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("image/png").expect("media type"),
            byte_length: 8,
            sha256: Sha256::digest(b"pngbytes"),
        };
        let text = serde_json::json!({
            "view": {"entry": {"name": "diagram.png"}, "representation": "visual_content"},
            "visual_content": [{
                "content_id": image_ref.content_id.to_string(),
                "media_type": image_ref.media_type.as_str(),
                "byte_length": image_ref.byte_length,
                "sha256": image_ref.sha256.to_string(),
            }]
        })
        .to_string();
        let tenant = "11111111-1111-4111-8111-111111111111"
            .parse()
            .expect("tenant");
        let store = CountingStore {
            reads: AtomicUsize::new(0),
            bytes: b"pngbytes".to_vec(),
        };
        let mut text_route = route();
        text_route.capabilities.clear();

        let with_vision = materialize(
            &store,
            tenant,
            &text_route,
            true,
            std::slice::from_ref(&image_ref),
            text.clone(),
        )
        .await
        .expect("text-only route must not fail on files.read image");
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        let [
            CanonicalContentPart::Text(json_part),
            CanonicalContentPart::Text(notice),
        ] = with_vision.as_slice()
        else {
            panic!("expected JSON receipt plus degraded image notice");
        };
        let id = image_ref.content_id.to_string();
        assert!(json_part.as_str().contains(&id));
        assert!(notice.as_str().contains("vision_tool_required"));
        assert!(notice.as_str().contains(&format!("content_ids=[{id}]")));
        assert!(notice.as_str().contains("vision_analyze"));

        let without_vision = materialize(
            &store,
            tenant,
            &text_route,
            false,
            std::slice::from_ref(&image_ref),
            text.clone(),
        )
        .await
        .expect("text-only route without vision tool must still succeed");
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        let [
            CanonicalContentPart::Text(json_part),
            CanonicalContentPart::Text(notice),
        ] = without_vision.as_slice()
        else {
            panic!("expected JSON receipt plus limitation notice");
        };
        assert!(json_part.as_str().contains(&id));
        assert!(notice.as_str().contains(&format!("content_ids=[{id}]")));
        assert!(!notice.as_str().contains("vision_analyze"));
        assert!(!notice.as_str().contains("vision_tool_required"));
        assert!(
            notice
                .as_str()
                .contains("no visual analysis tool is available")
        );

        let mut visual_route = route();
        visual_route.capabilities = [musteragent_identity::DottedKey::new("vision").expect("cap")]
            .into_iter()
            .collect();
        let inlined = materialize(
            &store,
            tenant,
            &visual_route,
            false,
            std::slice::from_ref(&image_ref),
            text,
        )
        .await
        .expect("visual route keeps inlining image bytes");
        assert_eq!(store.reads.load(Ordering::SeqCst), 1);
        assert!(matches!(
            inlined.as_slice(),
            [
                CanonicalContentPart::Text(_),
                CanonicalContentPart::Image(reference)
            ] if reference == &image_ref
        ));
    }

    #[tokio::test]
    async fn text_only_route_degrades_audio_video_and_document_parts_without_failing() {
        use musteragent_runtime_content::PutContent;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct NeverReadStore {
            reads: AtomicUsize,
        }
        impl ContentStore for NeverReadStore {
            fn put(
                &self,
                _: PutContent,
            ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
                Box::pin(async { panic!("read only") })
            }
            fn read<'a>(
                &'a self,
                _: musteragent_identity::TenantId,
                _: &'a ContentRef,
            ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
                Box::pin(async move {
                    self.reads.fetch_add(1, Ordering::SeqCst);
                    panic!("unsupported parts must never trigger content reads")
                })
            }
        }
        let references = ["audio/wav", "video/mp4", "application/pdf"]
            .into_iter()
            .map(|mime| ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new(mime).expect("media type"),
                byte_length: 12,
                sha256: Sha256::digest(mime.as_bytes()),
            })
            .collect::<Vec<_>>();
        let text = serde_json::json!({
            "source_content": references.iter().map(|r| serde_json::json!({
                "content_id": r.content_id.to_string(),
                "media_type": r.media_type.as_str(),
                "byte_length": r.byte_length,
                "sha256": r.sha256.to_string(),
            })).collect::<Vec<_>>()
        })
        .to_string();
        let tenant = "11111111-1111-4111-8111-111111111111"
            .parse()
            .expect("tenant");
        let store = NeverReadStore {
            reads: AtomicUsize::new(0),
        };
        let mut text_route = route();
        text_route.capabilities.clear();

        let output = materialize(&store, tenant, &text_route, true, &references, text)
            .await
            .expect("unsupported non-image media must degrade into notices");
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        assert_eq!(output.len(), 4);
        let notices: Vec<&str> = output[1..]
            .iter()
            .map(|part| match part {
                CanonicalContentPart::Text(text) => text.as_str(),
                other => panic!("expected text notice, got {other:?}"),
            })
            .collect();
        assert!(notices[0].contains(&references[0].content_id.to_string()));
        assert!(notices[0].contains("cannot hear or watch audio"));
        assert!(notices[1].contains(&references[1].content_id.to_string()));
        assert!(notices[1].contains("cannot hear or watch video"));
        assert!(notices[2].contains(&references[2].content_id.to_string()));
        assert!(notices[2].contains("cannot receive this document as original bytes"));
        assert!(!notices[2].contains("vision_analyze"));
    }

    #[tokio::test]
    async fn files_read_success_content_checks_vision_exposure_and_auxiliary_route() {
        use musteragent_capability::{CapabilityGrantSnapshot, CapabilityLocation};
        use musteragent_discovery::ExposureState;
        use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
        use musteragent_identity::{
            ActivityId, Actor, AgentId, CapabilityProviderId, ConversationId, HomeEpoch,
            ParticipantId, PrincipalId, PrincipalKind, RunId, RuntimeSnapshotId, TenantId,
            VersionedKey,
        };
        use musteragent_model::{AuxiliaryModelRole, CredentialGrantRef};
        use musteragent_policy::{Decision, PermissionMode};
        use musteragent_run::RunContext;
        use musteragent_runtime_capability::{
            DurableToolObservation, InvocationAuthorization, ToolExecutionPortError,
        };
        use musteragent_runtime_contract::ModelClass;
        use musteragent_runtime_snapshot::{
            ModelRouteSnapshot, PinnedAuxiliaryModelRoute, PinnedModelRoute,
            RunExecutionPolicySnapshot, ToolDefinitionSnapshot, authorized_tool_catalog,
        };
        use musteragent_tool::{BuiltinToolCatalog, ToolKey};
        use std::collections::BTreeSet;
        use std::num::NonZeroU32;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct ImageStore {
            reads: AtomicUsize,
            bytes: Vec<u8>,
        }
        impl ContentStore for ImageStore {
            fn put(
                &self,
                _: PutContent,
            ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
                Box::pin(async { panic!("read only") })
            }
            fn read<'a>(
                &'a self,
                _: TenantId,
                _: &'a ContentRef,
            ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
                Box::pin(async move {
                    self.reads.fetch_add(1, Ordering::SeqCst);
                    Ok(self.bytes.clone())
                })
            }
        }
        struct UnusedMaterial;
        impl ToolExecutionMaterialSource for UnusedMaterial {
            fn load<'a>(
                &'a self,
                _: &'a AttemptContext,
                _: &'a WireName,
            ) -> ApplicationFuture<'a, Result<ToolExecutionMaterial, ToolExecutionPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
        }
        struct UnusedJournal;
        impl FencedToolInvocationJournal for UnusedJournal {
            fn load_or_begin(
                &self,
                _: &AttemptContext,
                _: ToolInvocation,
                _: ToolRetryContract,
            ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
            fn load_observation(
                &self,
                _: &AttemptContext,
                _: ToolInvocation,
                _: ToolRetryContract,
            ) -> ApplicationFuture<'_, Result<DurableToolObservation, ToolExecutionPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
            fn request_approval(
                &self,
                _: &AttemptContext,
                _: ApprovalRequest,
            ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
            fn request_input(
                &self,
                _: &AttemptContext,
                _: musteragent_runtime_capability::InputInteractionRequest,
            ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
            fn find_active_authorization_receipt(
                &self,
                _: AuthorizationReceiptQuery,
            ) -> ApplicationFuture<'_, Result<Option<AuthorizationReceipt>, ToolExecutionPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
            fn issue_authorization_receipt(
                &self,
                _: &AttemptContext,
                _: AuthorizationReceipt,
            ) -> ApplicationFuture<'_, Result<AuthorizationReceipt, ToolExecutionPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
            fn start(
                &self,
                _: &AttemptContext,
                _: InvocationId,
                _: ToolStartMode,
                _: ToolExecutionStart,
            ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
            fn complete(
                &self,
                _: &AttemptContext,
                _: InvocationId,
                _: ToolTerminalOutcome,
                _: UtcTimestamp,
            ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
        }
        struct UnusedDispatcher;
        impl CapabilityDispatcher for UnusedDispatcher {
            fn invoke<'a>(
                &'a self,
                _: &'a AttemptContext,
                _: &'a ToolExecutionMaterial,
                _: &'a DurableToolInvocation,
                _: CapabilityInvocation,
            ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
            fn reconcile<'a>(
                &'a self,
                _: &'a AttemptContext,
                _: &'a ToolExecutionMaterial,
                _: &'a DurableToolInvocation,
                _: musteragent_runtime_capability::CapabilityReconciliation,
            ) -> ApplicationFuture<'a, Result<crate::DispatchedReconciliationOutcome, LoopPortError>>
            {
                Box::pin(async { panic!("unused") })
            }
        }
        struct FixedClock;
        impl RuntimeClock for FixedClock {
            fn now(&self) -> UtcTimestamp {
                UtcTimestamp::from_unix_millis(1_790_874_000_000)
            }
        }
        struct UnusedTraces;
        impl WorkerTraceContextSource for UnusedTraces {
            fn trace_context<'a>(
                &'a self,
                _: &'a AttemptContext,
                _: &'a ToolCallId,
            ) -> ApplicationFuture<'a, Result<TraceContext, LoopPortError>> {
                Box::pin(async { panic!("unused") })
            }
        }

        let catalog = BuiltinToolCatalog::load_cloud().expect("cloud catalog");
        let files_read_def = catalog
            .by_key(&ToolKey::from_versioned_key(
                VersionedKey::new("files.read@6").expect("files.read@6"),
            ))
            .expect("files.read@6 definition")
            .clone();
        let vision_def = catalog
            .by_key(&ToolKey::from_versioned_key(
                VersionedKey::new("vision.analyze@4").expect("vision.analyze@4"),
            ))
            .expect("vision.analyze@4 definition")
            .clone();
        let provider_id = CapabilityProviderId::generate();
        let snapshot_of = |def: &musteragent_tool::BuiltinToolDefinition| ToolDefinitionSnapshot {
            manifest: def.pin(provider_id),
            execution_target: def.target.clone(),
            concurrency_class: def.concurrency,
            idempotency: def.idempotency,
            file_operation: def.file_operation.clone(),
            title: def.title.clone(),
            description: def.description.clone(),
            tags: Vec::new(),
            when_to_use: None,
            input_schema_json: def.input_schema_json.clone(),
            output_schema_json: def.output_schema_json.clone(),
        };
        let grant_of = |tool: &ToolDefinitionSnapshot| CapabilityGrantSnapshot {
            key: musteragent_capability::CapabilityKey::from_versioned_key(
                tool.manifest.capability_key.clone(),
            ),
            provider_id,
            location: CapabilityLocation::Cloud,
            effect: tool.manifest.effect,
            allowed_scopes: tool.manifest.required_scopes.clone(),
            policy_decision: Decision::Allow,
        };
        let files_read_tool = snapshot_of(&files_read_def);
        let vision_tool = snapshot_of(&vision_def);
        let files_read_grant = grant_of(&files_read_tool);
        let vision_grant = grant_of(&vision_tool);
        let authorized_tools = authorized_tool_catalog(
            &[files_read_tool.clone(), vision_tool.clone()],
            &[files_read_grant.clone(), vision_grant],
        )
        .expect("authorized catalog");

        let tenant_id = TenantId::generate();
        let build_material =
            |active_route: musteragent_model::ModelRoute,
             expose_vision: bool,
             auxiliary_models: Vec<PinnedAuxiliaryModelRoute>| {
                let exposed_keys = if expose_vision {
                    vec![
                        files_read_tool.manifest.key.clone(),
                        vision_tool.manifest.key.clone(),
                    ]
                } else {
                    vec![files_read_tool.manifest.key.clone()]
                };
                let exposure_state = ExposureState::from_initial(&authorized_tools, &exposed_keys)
                    .expect("exposure");
                ToolExecutionMaterial {
                    application_capabilities: vec![],
                    product_release: None,
                    run_context: RunContext {
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
                        home_epoch: HomeEpoch::new(1).expect("epoch"),
                        runtime_snapshot_id: RuntimeSnapshotId::generate(),
                    },
                    recovery_policy: NonZeroU32::new(4).expect("recovery").into(),
                    actor: Actor {
                        tenant_id,
                        principal_id: PrincipalId::generate(),
                        principal_kind: PrincipalKind::User,
                    },
                    agent_id: AgentId::generate(),
                    model: ModelRouteSnapshot {
                        requested_model_class: ModelClass::new("chat.balanced").expect("class"),
                        route: active_route,
                        credential_grant_ref: CredentialGrantRef::new("grant_primary")
                            .expect("grant"),
                        fallback_candidates: vec![],
                    },
                    active_model_route_ordinal: 0,
                    auxiliary_models,
                    model_stream_budget: RunExecutionPolicySnapshot::production_defaults()
                        .model_stream,
                    model_liveness: None,
                    tool: files_read_tool.clone(),
                    authorized_tools: authorized_tools.clone(),
                    discovery: None,
                    exposure_state,
                    capability_grant: files_read_grant.clone(),
                    authorization: InvocationAuthorization {
                        actor_scopes: files_read_tool.manifest.required_scopes.clone(),
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
                }
            };

        let store = Arc::new(ImageStore {
            reads: AtomicUsize::new(0),
            bytes: b"png-fixture".to_vec(),
        });
        let executor = DurableToolExecutor::new(
            Arc::new(UnusedMaterial),
            Arc::new(UnusedJournal),
            store.clone(),
            Arc::new(UnusedDispatcher),
            Arc::new(FixedClock),
            Arc::new(UnusedTraces),
        );
        let image_ref = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("image/png").expect("media type"),
            byte_length: 11,
            sha256: Sha256::digest(b"png-fixture"),
        };
        let result_json = serde_json::json!({
            "view": {"entry": {"name": "chart.png"}, "representation": "visual_content"},
            "visual_content": [{
                "content_id": image_ref.content_id.to_string(),
                "media_type": "image/png",
                "byte_length": image_ref.byte_length,
                "sha256": image_ref.sha256.to_string(),
            }]
        })
        .to_string();

        let mut text_route = route();
        text_route.capabilities.clear();
        let mut visual_route = route();
        visual_route.capabilities = [musteragent_identity::DottedKey::new("vision").expect("cap")]
            .into_iter()
            .collect();
        let aux_vision = PinnedAuxiliaryModelRoute {
            role: AuxiliaryModelRole::Vision,
            model: PinnedModelRoute {
                route: visual_route.clone(),
                credential_grant_ref: CredentialGrantRef::new("grant_vision").expect("grant"),
            },
        };
        let aux_compression = PinnedAuxiliaryModelRoute {
            role: AuxiliaryModelRole::Compression,
            model: PinnedModelRoute {
                route: visual_route.clone(),
                credential_grant_ref: CredentialGrantRef::new("grant_comp").expect("grant"),
            },
        };

        // 1. Text-only route + vision_analyze exposed + auxiliary vision model configured.
        let material = build_material(text_route.clone(), true, vec![aux_vision.clone()]);
        let content = executor
            .success_content(
                &material,
                std::slice::from_ref(&image_ref),
                result_json.clone(),
            )
            .await
            .expect("text-only files.read with vision_analyze succeeds");
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        let CanonicalContentPart::Text(notice) = &content[1] else {
            panic!("expected text notice");
        };
        assert!(notice.as_str().contains("vision_tool_required"));
        assert!(notice.as_str().contains("vision_analyze"));
        assert!(notice.as_str().contains(&image_ref.content_id.to_string()));

        // 2. Text-only route + vision_analyze NOT exposed (even with auxiliary vision route).
        let material = build_material(text_route.clone(), false, vec![aux_vision]);
        let content = executor
            .success_content(
                &material,
                std::slice::from_ref(&image_ref),
                result_json.clone(),
            )
            .await
            .expect("text-only files.read without exposed vision_analyze succeeds");
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        let CanonicalContentPart::Text(notice) = &content[1] else {
            panic!("expected text notice");
        };
        assert!(notice.as_str().contains(&image_ref.content_id.to_string()));
        assert!(!notice.as_str().contains("vision_analyze"));
        assert!(!notice.as_str().contains("vision_tool_required"));

        // 3. Text-only route + vision_analyze exposed, but no auxiliary vision route.
        let material = build_material(text_route, true, vec![aux_compression]);
        let content = executor
            .success_content(
                &material,
                std::slice::from_ref(&image_ref),
                result_json.clone(),
            )
            .await
            .expect("text-only files.read without auxiliary vision route succeeds");
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        let CanonicalContentPart::Text(notice) = &content[1] else {
            panic!("expected text notice");
        };
        assert!(notice.as_str().contains(&image_ref.content_id.to_string()));
        assert!(!notice.as_str().contains("vision_analyze"));
        assert!(!notice.as_str().contains("vision_tool_required"));

        // 4. Visual route keeps inlining the verified image part.
        let material = build_material(visual_route, false, vec![]);
        let content = executor
            .success_content(&material, std::slice::from_ref(&image_ref), result_json)
            .await
            .expect("visual route inlines image");
        assert_eq!(store.reads.load(Ordering::SeqCst), 1);
        assert!(matches!(
            content.as_slice(),
            [
                CanonicalContentPart::Text(_),
                CanonicalContentPart::Image(reference)
            ] if reference == &image_ref
        ));
    }
}
