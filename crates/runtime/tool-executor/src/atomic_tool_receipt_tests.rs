use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, InvocationId, VersionedKey};
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_contract::{ErrorInfo, RetryDisposition};

fn result() -> CapabilityResult {
    let hash = Sha256::digest(b"{}");
    CapabilityResult {
        result_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").unwrap(),
            byte_length: 2,
            sha256: hash,
        },
        result_hash: hash,
        result_schema: SchemaRef {
            schema_key: VersionedKey::new("test.resource.result@1").unwrap(),
            schema_hash: Sha256::digest(b"schema"),
        },
        provider_request_ref: None,
    }
}
fn failure() -> ToolTerminalOutcome {
    let code = ErrorCode::ResourceRevisionConflict;
    ToolTerminalOutcome::Failed {
        error: ErrorInfo {
            code,
            category: code.category(),
            safe_message: SafeDisplayText::new("The original version changed").unwrap(),
            retryable: false,
            dependency_ref: None,
            diagnostic_ref: None,
        },
        retry_disposition: RetryDisposition::DoNotRetry,
        diagnostic_result: None,
    }
}
#[test]
fn exact_original_success_is_adopted_without_second_completion() {
    let result = result();
    let outcome = ToolTerminalOutcome::SucceededWithAuthorizedContent {
        result: result.clone(),
        authorized_content: vec![result.result_ref.clone()],
    };
    assert_eq!(
        retained(Some(outcome.clone())),
        RetainedToolResult::Committed(result.clone())
    );
    assert_eq!(
        reconciled(
            Some(outcome),
            original_tool_receipt_identity(InvocationId::generate())
        ),
        DispatchedReconciliationOutcome::Committed(result)
    );
}
#[test]
fn durable_logical_failure_stays_the_original_failure() {
    let expected = failure();
    let ToolTerminalOutcome::Failed {
        error,
        retry_disposition,
        diagnostic_result,
    } = expected.clone()
    else {
        unreachable!()
    };
    assert_eq!(
        retained(Some(expected.clone())),
        RetainedToolResult::Complete(Box::new(CapabilityOutcome::Failed {
            error: error.clone(),
            retry_disposition,
            diagnostic_result: diagnostic_result.clone(),
        }))
    );
    assert_eq!(
        reconciled(
            Some(expected),
            original_tool_receipt_identity(InvocationId::generate())
        ),
        DispatchedReconciliationOutcome::Provider(ReconciliationOutcome::ProvenFailed {
            error,
            retry_disposition,
            diagnostic_result,
        })
    );
}
#[test]
fn missing_receipt_and_unknown_effect_never_authorize_retry() {
    let identity = original_tool_receipt_identity(InvocationId::generate());
    assert_eq!(retained(None), RetainedToolResult::Incomplete);
    assert!(
        matches!(reconciled(None, identity.clone()), DispatchedReconciliationOutcome::Provider(ReconciliationOutcome::StillIndeterminate { reconciliation_ref, .. }) if reconciliation_ref == identity)
    );
    let unknown = ToolTerminalOutcome::Indeterminate {
        reason: SafeDisplayText::new("Original commit has no receipt").unwrap(),
        reconciliation_ref: identity.clone(),
        observed_error: None,
        diagnostic_result: None,
    };
    assert_eq!(
        retained(Some(unknown.clone())),
        RetainedToolResult::Indeterminate
    );
    assert!(matches!(
        reconciled(Some(unknown), identity),
        DispatchedReconciliationOutcome::Provider(ReconciliationOutcome::StillIndeterminate { .. })
    ));
}
struct Clock;
impl RuntimeClock for Clock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(100)
    }
}
#[tokio::test]
async fn bounded_lookup_rejects_expired_deadline_without_polling_storage() {
    let schema = result().result_schema;
    let outcome = lookup(
        &Clock,
        UtcTimestamp::from_unix_millis(100),
        &schema,
        async {
            panic!("expired query must not reach storage");
            #[allow(unreachable_code)]
            Ok(None)
        },
    )
    .await;
    assert!(matches!(
        outcome,
        Err(LoopPortError {
            code: ErrorCode::DependencyTimeout,
            ..
        })
    ));
}
#[tokio::test]
async fn retained_result_must_match_the_original_result_schema() {
    let result = result();
    let mut schema = result.result_schema.clone();
    schema.schema_hash = Sha256::digest(b"different schema");
    let outcome = lookup(
        &Clock,
        UtcTimestamp::from_unix_millis(200),
        &schema,
        async { Ok(Some(ToolTerminalOutcome::Succeeded(result))) },
    )
    .await;
    assert!(matches!(
        outcome,
        Err(LoopPortError {
            code: ErrorCode::RuntimeSnapshotInvalid,
            ..
        })
    ));
}

#[test]
fn only_forward_write_keys_enable_original_receipt_reconciliation() {
    assert!(!enabled("agents.self.configure@1"));
    assert!(!enabled("agents.self.inspect@1"));
    for key in [
        "schedules.create@1",
        "schedules.update@1",
        "schedules.control@1",
        "agents.self.configure@2",
        "resource.file.create@4",
        "resource.file.edit.begin@5",
        "resource.file.edit.append@4",
        "resource.file.edit.patch@6",
        "resource.file.edit.finalize@4",
        "resource.file.edit.abort@4",
        "resource.folder.create@5",
        "resource.node.rename@5",
        "resource.node.move@4",
        "resource.node.trash@4",
        "resource.node.restore@4",
        "resource.change-set.commit@6",
    ] {
        assert!(enabled(key));
    }
    for key in [
        "resource.file.create@3",
        "resource.file.edit.begin@4",
        "resource.file.edit.append@3",
        "resource.file.edit.patch@5",
        "resource.file.edit.finalize@3",
        "resource.file.edit.abort@3",
        "resource.folder.create@4",
        "resource.node.rename@4",
        "resource.node.move@3",
        "resource.node.trash@3",
        "resource.node.restore@3",
        "resource.change-set.commit@5",
        "resource.file.read@5",
        "resource.file.edit.resume@5",
        "resource.search@4",
        "resource.tree.list@4",
        "files.list@4",
        "files.read@5",
        "resource.file.create@5",
    ] {
        assert!(!enabled(key));
    }
}

/// A file-producing Tool whose contract requires query-before-retry needs a
/// durable receipt identity before it can start. A missing opt-in used to end
/// the whole Run on first use; check every current product catalog instead.
#[test]
fn every_receipt_contract_file_tool_opts_into_atomic_receipts() {
    use musteragent_tool::{
        BuiltinToolCatalog, CapabilityAssemblyPlan, CapabilityHandlerKind, ProductExecutionTarget,
        compiled_product_release,
    };
    let mut missing = Vec::new();
    for product in [
        musteragent_identity::ProductId::KERNEL,
        musteragent_identity::ProductId::KERNEL,
    ] {
        let release = compiled_product_release(product);
        for target in [
            ProductExecutionTarget::Cloud,
            ProductExecutionTarget::Device,
        ] {
            let catalog = BuiltinToolCatalog::for_product_release(&release, target).unwrap();
            let plan = CapabilityAssemblyPlan::from_catalog(&catalog).unwrap();
            for kind in [
                CapabilityHandlerKind::FileArtifact,
                CapabilityHandlerKind::DeviceFileArtifact,
                CapabilityHandlerKind::AudioNarration,
                CapabilityHandlerKind::VideoTemplate,
            ] {
                for definition in plan.definitions(kind) {
                    let contract = definition.idempotency.retry_contract();
                    if matches!(
                        contract,
                        ToolRetryContract::QueryBeforeRetry | ToolRetryContract::NotReplayable
                    ) && !super::enabled(definition.key.as_str())
                    {
                        missing.push(format!(
                            "{product:?}/{target:?}/{}",
                            definition.key.as_str()
                        ));
                    }
                }
            }
        }
    }
    assert!(
        missing.is_empty(),
        "add these to atomic_tool_receipt::enabled: {missing:?}"
    );
}
