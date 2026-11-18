use super::*;
use musteragent_identity::ProductId;
use musteragent_runtime_snapshot::ToolDefinitionSnapshot;
use musteragent_tool::{BuiltinToolCatalog, ProductExecutionTarget, compiled_product_release};

struct Marker;
impl CapabilityDispatcher for Marker {
    fn invoke<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a ToolExecutionMaterial,
        _: &'a DurableToolInvocation,
        _: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async { Err(unavailable()) })
    }
    fn reconcile<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a ToolExecutionMaterial,
        _: &'a DurableToolInvocation,
        _: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(unavailable()) })
    }
}

#[test]
fn published_memory_uses_frozen_source_without_shadowing_other_kernel_contracts() {
    assert_memory_source(1);
}

#[test]
fn published_memory_ranking_reuses_exact_original_read_contracts() {
    assert_memory_source(2);
}

#[test]
fn published_memory_read_profile_reuses_all_eight_original_contracts() {
    assert_memory_source(3);
}

fn assert_memory_source(profile: u8) {
    use musteragent_content_ref::Sha256;
    use musteragent_tool::workspace::{WorkspaceToolSelection, parse_release};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let bytes = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../domains/tool/tests/fixtures/memory-program-published-source.json"
    ));
    let mut document: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    if profile > 1 {
        let pin = document["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|tool| {
                tool.get("implementation")
                    .map(|value| value["component"].clone())
            })
            .unwrap();
        for tool in document["tools"].as_array_mut().unwrap() {
            if profile == 3 {
                let schema = if tool["implementation"].is_object() {
                    musteragent_tool::ApplicationImplementation::MEMORY_V2_SCHEMA
                } else {
                    musteragent_tool::ApplicationImplementation::MEMORY_READ_SCHEMA
                };
                tool["implementation"] =
                    serde_json::json!({"schema_version":schema,"component":pin});
            } else if matches!(
                tool["runtime_definition"]["tool_key"].as_str(),
                Some("memory.search@1" | "memory.list@1" | "memory.get@1")
            ) {
                tool["implementation"] = serde_json::json!({
                    "schema_version":musteragent_tool::ApplicationImplementation::MEMORY_RANKING_SCHEMA,
                    "component":pin,
                });
            }
        }
    }
    let kernel_json: serde_json::Value =
        serde_json::from_str(include_str!("../../../../../contracts/kernel-tools.json")).unwrap();
    for operation in document["tools"].as_array_mut().unwrap() {
        if let Some(original) = kernel_json["products"]["musteragent"]["cloud"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["tool_key"] == operation["runtime_definition"]["tool_key"])
        {
            operation["runtime_definition"] = original.clone();
        }
    }
    let bytes = serde_json::to_vec(&document).unwrap();
    let source = parse_release(&bytes, &Sha256::digest(&bytes).into_bytes()).unwrap();
    let release = compiled_product_release(ProductId::KERNEL);
    let kernel =
        BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let mut registry = ProductCapabilityDispatcher::default();
    registry
        .register(
            release.clone(),
            kernel.definitions().to_vec(),
            Arc::new(Marker),
        )
        .unwrap();
    registry
        .register_published_factory(
            &release,
            Arc::new(move |_: &ProductReleaseRef, d: &BuiltinToolDefinition| {
                calls.fetch_add(1, Ordering::SeqCst);
                assert!(d.application.as_ref().unwrap().authority_witness.is_some());
                Ok((
                    vec![d.clone()],
                    Arc::new(Marker) as Arc<dyn CapabilityDispatcher>,
                ))
            }),
        )
        .unwrap();
    for operation in &source.tools {
        let mut definition = WorkspaceToolSelection {
            plugin_id: source.plugin_id.clone(),
            version: source.version.clone(),
            release_sha256: source.release_sha256.clone(),
            operation_ref: operation.key.clone(),
            selection_sha256: "1".repeat(64),
        }
        .definition_from_release(&source)
        .unwrap();
        definition.application.as_mut().unwrap().authority_witness = Some("{}".into());
        let tool = frozen(&definition);
        let result = registry.bind_published(Some(&release), &tool);
        assert!(result.unwrap().is_some(), "{}", definition.key.as_str());
        // Same key and Source do not permit a different frozen schema.
        let mut changed = tool.clone();
        changed.input_schema_json = b"{}".to_vec();
        assert!(registry.bind_published(Some(&release), &changed).is_err());
        let mut changed = tool.clone();
        changed
            .manifest
            .application
            .as_mut()
            .unwrap()
            .authority_witness = None;
        assert!(registry.bind_published(Some(&release), &changed).is_err());
    }
    assert_eq!(count.load(Ordering::SeqCst), source.tools.len());
}

fn frozen(definition: &BuiltinToolDefinition) -> ToolDefinitionSnapshot {
    ToolDefinitionSnapshot {
        manifest: definition.pin(musteragent_tool::BUILTIN_RUST_PROVIDER_ID.parse().unwrap()),
        execution_target: definition.target.clone(),
        concurrency_class: definition.concurrency,
        idempotency: definition.idempotency,
        file_operation: definition.file_operation.clone(),
        title: definition.title.clone(),
        description: definition.description.clone(),
        input_schema_json: definition.input_schema_json.clone(),
        output_schema_json: definition.output_schema_json.clone(),
    }
}

#[test]
fn signed_release_and_legacy_exact_contract_both_survive_registration_order() {
    let mut registrations = Vec::new();
    for ordinal in 0..2 {
        let mut release = compiled_product_release(ProductId::KERNEL);
        let catalog =
            BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud)
                .unwrap();
        release.digest[0] ^= ordinal;
        // Explicit competing same-key contracts exercise dispatch identity;
        // no business definition is assumed to remain in the current kernel.
        let mut definition = catalog
            .definitions()
            .iter()
            .find(|d| d.key.logical_key() == "system.time.current")
            .unwrap()
            .clone();
        let mut input: serde_json::Value =
            serde_json::from_slice(&definition.input_schema_json).unwrap();
        input["title"] = serde_json::json!(format!("frozen fixture contract {ordinal}"));
        definition.input_schema_json = serde_json::to_vec(&input).unwrap();
        definition.input_schema.schema_hash =
            musteragent_content_ref::Sha256::digest(&definition.input_schema_json);
        let tool = frozen(&definition);
        let marker: Arc<dyn CapabilityDispatcher> = Arc::new(Marker);
        registrations.push((release, vec![definition], tool, marker));
    }
    assert_ne!(
        registrations[0].2.input_schema_json,
        registrations[1].2.input_schema_json
    );
    for reverse in [false, true] {
        let mut registry = ProductCapabilityDispatcher::default();
        let order = if reverse { [1, 0] } else { [0, 1] };
        for index in order {
            let (release, definitions, _, marker) = &registrations[index];
            // Inject an authenticated historical release fixture to test the
            // resolver independently of the single compiled release registry.
            registry.releases.insert(
                release.clone(),
                Registration {
                    contracts: definitions.clone(),
                    dispatcher: marker.clone(),
                    published: None,
                },
            );
        }
        assert!(
            registry
                .resolve_contract(Some(&registrations[0].0), &registrations[1].2)
                .is_err()
        );
        assert!(
            registry
                .resolve_contract(Some(&registrations[1].0), &registrations[0].2)
                .is_err()
        );
        for (release, _, tool, marker) in &registrations {
            assert!(std::ptr::eq(
                registry.resolve_contract(Some(release), tool).unwrap(),
                marker.as_ref()
            ));
            assert!(std::ptr::eq(
                registry.resolve_contract(None, tool).unwrap(),
                marker.as_ref()
            ));
            let mut corrupted = release.clone();
            corrupted.digest[0] ^= 1;
            assert!(registry.resolve_contract(Some(&corrupted), tool).is_err());
            let mut corrupt_tool = tool.clone();
            corrupt_tool.output_schema_json.push(b' ');
            assert!(registry.resolve_contract(None, &corrupt_tool).is_err());
        }
    }
}

#[test]
fn missing_release_does_not_fall_back_to_another_product_with_the_same_tool_key() {
    let mut registry = ProductCapabilityDispatcher::default();
    let work = compiled_product_release(ProductId::KERNEL);
    let mut other = compiled_product_release(ProductId::KERNEL);
    other.digest[0] ^= 1;
    let catalog =
        BuiltinToolCatalog::for_product_release(&work, ProductExecutionTarget::Cloud).unwrap();
    let tool = frozen(&catalog.definitions()[0]);
    registry
        .register(
            work.clone(),
            catalog.definitions().to_vec(),
            Arc::new(Marker),
        )
        .unwrap();
    assert!(registry.resolve_contract(Some(&other), &tool).is_err());
    assert!(
        registry
            .register(work, catalog.definitions().to_vec(), Arc::new(Marker))
            .is_err()
    );
}

fn published_fixture() -> (
    ProductReleaseRef,
    Vec<BuiltinToolDefinition>,
    ToolDefinitionSnapshot,
) {
    published_fixture_with_key("published_fixture.tasks.list@1")
}

fn published_fixture_with_key(
    tool_key: &str,
) -> (
    ProductReleaseRef,
    Vec<BuiltinToolDefinition>,
    ToolDefinitionSnapshot,
) {
    let release = compiled_product_release(ProductId::KERNEL);
    let catalog =
        BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud).unwrap();
    let source: serde_json::Value =
        serde_json::from_str(include_str!("../../../../../contracts/kernel-tools.json")).unwrap();
    let mut raw = source["products"]["musteragent"]["cloud"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["tool_key"] == "web.fetch@1")
        .unwrap()
        .clone();
    raw["tool_key"] = serde_json::json!(tool_key);
    raw["timeout_ms"] = serde_json::json!(30_000);
    let mut definition = BuiltinToolDefinition::from_released_document(raw.clone()).unwrap();
    let digest = "a".repeat(64);
    definition.application = Some(musteragent_tool::ApplicationToolBinding {
        selection: musteragent_tool::workspace::WorkspaceToolSelection {
            plugin_id: "published-app".into(),
            version: "1".into(),
            release_sha256: digest.clone(),
            operation_ref: format!("operation.h_{digest}@1"),
            selection_sha256: digest.clone(),
        },
        operation_key: "operation".into(),
        operation_sha256: digest,
        input_material: None,
        provides: None,
        authority_witness: Some("{}".into()),
        authority_role: None,
        implementation: None,
        runtime_definition: Some(raw),
    });
    (release, catalog.definitions().to_vec(), frozen(&definition))
}

#[test]
fn kernel_tool_rejects_published_application_shadowing() {
    let (_, _, tool) = published_fixture_with_key("web.fetch@1");
    let release = compiled_product_release(ProductId::KERNEL);
    let catalog =
        BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud).unwrap();
    assert!(
        catalog
            .definitions()
            .iter()
            .any(|d| d.key == tool.manifest.key)
    );
    let mut registry = ProductCapabilityDispatcher::default();
    registry
        .register(
            release.clone(),
            catalog.definitions().to_vec(),
            Arc::new(Marker),
        )
        .unwrap();
    registry
        .register_published_factory(
            &release,
            Arc::new(|_: &ProductReleaseRef, _: &BuiltinToolDefinition| {
                panic!("a market application cannot replace the restored kernel contract")
            }),
        )
        .unwrap();
    assert!(registry.bind_published(Some(&release), &tool).is_err());
}

#[test]
fn published_physical_preparation_is_cached_by_full_frozen_contract_and_product() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (release, kernel, tool) = published_fixture();
    let mut registry = ProductCapabilityDispatcher::default();
    registry
        .register(release.clone(), kernel, Arc::new(Marker))
        .unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    registry
        .register_published_factory(
            &release,
            Arc::new(move |_: &ProductReleaseRef, d: &BuiltinToolDefinition| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok((
                    vec![d.clone()],
                    Arc::new(Marker) as Arc<dyn CapabilityDispatcher>,
                ))
            }),
        )
        .unwrap();
    let first = registry
        .bind_published(Some(&release), &tool)
        .unwrap()
        .unwrap();
    let second = registry
        .bind_published(Some(&release), &tool)
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let mut changed = tool.clone();
    changed
        .manifest
        .application
        .as_mut()
        .unwrap()
        .authority_witness = Some("{\"revision\":2}".into());
    let next = registry
        .bind_published(Some(&release), &changed)
        .unwrap()
        .unwrap();
    assert!(!Arc::ptr_eq(&first, &next));
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert!(registry.bind_published(None, &tool).is_err());
    assert!(
        registry
            .bind_published(
                Some(&{
                    let mut missing = release.clone();
                    missing.digest[0] ^= 1;
                    missing
                }),
                &tool
            )
            .is_err()
    );
    let mut corrupt = tool;
    corrupt.output_schema_json.push(b' ');
    assert!(registry.bind_published(Some(&release), &corrupt).is_err());
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[test]
fn published_factory_cannot_replace_frozen_effects_or_shadow_a_kernel_tool() {
    let (release, kernel, mut tool) = published_fixture();
    let kernel_key = kernel[0].key.clone();
    let mut registry = ProductCapabilityDispatcher::default();
    registry
        .register(release.clone(), kernel, Arc::new(Marker))
        .unwrap();
    registry
        .register_published_factory(
            &release,
            Arc::new(|_: &ProductReleaseRef, d: &BuiltinToolDefinition| {
                let mut changed = d.clone();
                changed.output_schema_json.push(b' ');
                Ok((
                    vec![changed],
                    Arc::new(Marker) as Arc<dyn CapabilityDispatcher>,
                ))
            }),
        )
        .unwrap();
    assert!(registry.bind_published(Some(&release), &tool).is_err());
    tool.manifest.key = kernel_key;
    assert!(registry.bind_published(Some(&release), &tool).is_err());
}

#[test]
fn published_contract_retains_provider_budget_separate_from_normalized_liveness() {
    let (release, kernel, tool) = published_fixture();
    // Provider request budget remains 30s after normalization to a 60s
    // activity idle window; rehydration must preserve both.
    assert_eq!(
        tool.manifest
            .execution_policy
            .liveness()
            .unwrap()
            .windows()
            .idle,
        Duration::from_secs(60)
    );
    let mut registry = ProductCapabilityDispatcher::default();
    registry
        .register(release.clone(), kernel, Arc::new(Marker))
        .unwrap();
    registry
        .register_published_factory(
            &release,
            Arc::new(|_: &ProductReleaseRef, d: &BuiltinToolDefinition| {
                assert_eq!(d.key.as_str(), "published_fixture.tasks.list@1");
                assert_eq!(d.timeout, Duration::from_secs(30));
                Ok((
                    vec![d.clone()],
                    Arc::new(Marker) as Arc<dyn CapabilityDispatcher>,
                ))
            }),
        )
        .unwrap();
    assert!(
        registry
            .bind_published(Some(&release), &tool)
            .unwrap()
            .is_some()
    );
    let mut legacy = tool.clone();
    legacy
        .manifest
        .application
        .as_mut()
        .unwrap()
        .runtime_definition = None;
    let encoded = legacy.manifest.application.as_ref().unwrap().encode();
    assert!(
        !String::from_utf8(encoded.clone())
            .unwrap()
            .contains("runtime_definition")
    );
    assert_eq!(
        musteragent_tool::ApplicationToolBinding::decode(&encoded).unwrap(),
        legacy.manifest.application
    );
    assert!(registry.bind_published(Some(&release), &legacy).is_err());
    let mut corrupt = tool;
    corrupt
        .manifest
        .application
        .as_mut()
        .unwrap()
        .runtime_definition
        .as_mut()
        .unwrap()["required_scopes"] = serde_json::json!(["sample_queue.other"]);
    assert!(registry.bind_published(Some(&release), &corrupt).is_err());
}

#[test]
fn authority_role_rehydrates_original_workspace_contract_through_existing_factory() {
    use musteragent_tool::workspace::{WorkspaceReleaseCatalog, WorkspaceToolSelection};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (product, kernel, _) = published_fixture();
    let catalog = WorkspaceReleaseCatalog::from_json(include_bytes!(
        "../../../../../applications/reference-api/examples/workspace-releases.json"
    ))
    .unwrap();
    let release = catalog
        .resolve("workspace.example-clock", "1.0.0", &"3".repeat(64))
        .unwrap()
        .release();
    let mut definition = WorkspaceToolSelection {
        plugin_id: release.plugin_id.clone(),
        version: release.version.clone(),
        release_sha256: release.release_sha256.clone(),
        operation_ref: release.tools[0].key.clone(),
        selection_sha256: "c".repeat(64),
    }
    .definition_from_release(release)
    .unwrap();
    definition.application.as_mut().unwrap().authority_witness = Some("{}".into());
    let tool = frozen(&definition);
    let count = Arc::new(AtomicUsize::new(0));
    let prepared = count.clone();
    let mut registry = ProductCapabilityDispatcher::default();
    registry
        .register(product.clone(), kernel, Arc::new(Marker))
        .unwrap();
    registry
        .register_published_factory(
            &product,
            Arc::new(
                move |_: &ProductReleaseRef, exact: &BuiltinToolDefinition| {
                    assert_eq!(exact.capability_key.as_str(), "core.time.read@1");
                    assert_eq!(exact.application.as_ref().unwrap().runtime_definition, None);
                    prepared.fetch_add(1, Ordering::SeqCst);
                    Ok((
                        vec![exact.clone()],
                        Arc::new(Marker) as Arc<dyn CapabilityDispatcher>,
                    ))
                },
            ),
        )
        .unwrap();
    for _ in 0..2 {
        assert!(
            registry
                .bind_published(Some(&product), &tool)
                .unwrap()
                .is_some()
        );
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let mut drift = tool.clone();
    drift.manifest.application.as_mut().unwrap().authority_role = None;
    assert!(registry.bind_published(Some(&product), &drift).is_err());
    let mut drift = tool;
    drift.manifest.required_scopes.clear();
    assert!(registry.bind_published(Some(&product), &drift).is_err());
}

#[test]
fn generic_workspace_dispatch_uses_only_the_frozen_selected_contract() {
    use musteragent_tool::workspace::{WorkspaceReleaseCatalog, WorkspaceToolSelection};
    let (product, kernel, _) = published_fixture();
    let catalog = WorkspaceReleaseCatalog::from_json(include_bytes!(
        "../../../../../applications/reference-api/examples/workspace-releases.json"
    ))
    .unwrap();
    let release = catalog
        .resolve("mcp.example-search", "1.0.0", &"2".repeat(64))
        .unwrap()
        .release();
    let mut definition = WorkspaceToolSelection {
        plugin_id: release.plugin_id.clone(),
        version: release.version.clone(),
        release_sha256: release.release_sha256.clone(),
        operation_ref: release.tools[0].key.clone(),
        selection_sha256: "f".repeat(64),
    }
    .definition_from_release(release)
    .unwrap();
    definition.application.as_mut().unwrap().authority_witness = Some("{}".into());
    let tool = frozen(&definition);
    drop(catalog);
    let validator = crate::schema_validation::ToolSchemaValidatorCache::new();
    let validate = |arguments| {
        validator.validate(
            tool.manifest.input_schema.schema_hash,
            &tool.input_schema_json,
            &arguments,
        )
    };
    assert!(validate(serde_json::json!({"query":"example"})).is_ok());
    assert!(matches!(
        validate(serde_json::json!({"query":42})),
        Err(crate::schema_validation::SchemaValidationError::Arguments { .. })
    ));
    let mut registry = ProductCapabilityDispatcher::default();
    registry
        .register(product.clone(), kernel, Arc::new(Marker))
        .unwrap();
    assert!(
        matches!(registry.bind_published(Some(&product), &tool), Err(e) if e.code == ErrorCode::CapabilityUnavailable)
    );
    registry
        .register_published_factory(
            &product,
            Arc::new(
                move |_: &ProductReleaseRef, exact: &BuiltinToolDefinition| {
                    assert_eq!(*exact, definition);
                    Ok((
                        vec![exact.clone()],
                        Arc::new(Marker) as Arc<dyn CapabilityDispatcher>,
                    ))
                },
            ),
        )
        .unwrap();
    assert!(
        registry
            .bind_published(Some(&product), &tool)
            .unwrap()
            .is_some()
    );
    let mut foreign = tool;
    foreign
        .manifest
        .application
        .as_mut()
        .unwrap()
        .selection
        .selection_sha256 = "0".repeat(64);
    assert!(registry.bind_published(Some(&product), &foreign).is_err());
}

#[test]
fn backend_registration_retains_only_the_declared_subset_and_method() {
    use musteragent_tool::CapabilityExecutionTarget;
    let release = compiled_product_release(ProductId::KERNEL);
    let catalog =
        BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud).unwrap();
    let fetch = catalog.by_wire_name("web_fetch").unwrap();
    let search = catalog.by_wire_name("web_search").unwrap();
    let mut registry = ProductCapabilityDispatcher::default();
    let mut substituted = fetch.clone();
    substituted.target = CapabilityExecutionTarget::parse("Records.Lookup", None).unwrap();
    assert_eq!(
        registry.register(release.clone(), vec![substituted.clone()], Arc::new(Marker)),
        Err(ErrorCode::RuntimeSnapshotInvalid)
    );
    registry
        .register(release.clone(), vec![fetch.clone()], Arc::new(Marker))
        .unwrap();
    assert!(
        registry
            .resolve_contract(Some(&release), &frozen(fetch))
            .is_ok()
    );
    for denied in [search, &substituted] {
        assert!(
            registry
                .resolve_contract(Some(&release), &frozen(denied))
                .is_err()
        );
        assert!(registry.resolve_contract(None, &frozen(denied)).is_err());
    }
}
