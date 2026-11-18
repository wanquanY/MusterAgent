use super::*;
use musteragent_runtime_contract::OpaqueDiagnosticRef;

#[tokio::test]
async fn terminal_failure_preserves_answer_provenance_and_independent_child_status() {
    let tenant = TenantId::generate();
    let conversation = ConversationId::generate();
    let epoch = HomeEpoch::new(1).expect("home epoch");
    let parent = RunId::generate();
    let child = RunId::generate();
    let content: Arc<dyn ProjectionContentStore> = Arc::new(MemoryContent::default());
    let mut reducer =
        ProductProjectionReducer::new(tenant, conversation, epoch, None, BTreeMap::new())
            .expect("initialize parent-child reducer");
    for (sequence, run_id, parent_run_id) in [(1, parent, None), (2, child, Some(parent))] {
        let mut admitted = record(
            tenant,
            conversation,
            epoch,
            sequence,
            Some(run_id),
            None,
            DomainEvent::Run(RunEvent::Admitted {
                runtime_snapshot_id: RuntimeSnapshotId::generate(),
                execution_home: RuntimeHome::Device {
                    device_id: musteragent_identity::DeviceId::generate(),
                },
                parent_run_id,
                delegation_id: parent_run_id.map(|_| DelegationId::generate()),
                retry_source_message_id: None,
                retry_source_attempt_number: None,
            }),
        );
        admitted.metadata.activity_id = Some(ActivityId::generate());
        reducer
            .apply(admitted, Arc::clone(&content))
            .await
            .expect("project Run admission");
    }
    apply_output(
        &mut reducer,
        Arc::clone(&content),
        tenant,
        conversation,
        epoch,
        parent,
        MessageId::generate(),
        3,
        OutputId::generate(),
        OutputChannel::AssistantText,
        "Tests passed; waiting for child reports.",
    )
    .await;
    let error = ErrorInfo {
        code: ErrorCode::DependencyTimeout,
        category: ErrorCode::DependencyTimeout.category(),
        safe_message: SafeDisplayText::new("Provider recovery attempts exhausted")
            .expect("safe provider failure"),
        retryable: false,
        dependency_ref: Some(
            OpaqueDiagnosticRef::new("provider:test").expect("provider reference"),
        ),
        diagnostic_ref: Some(
            OpaqueDiagnosticRef::new("trace:recovery").expect("recovery diagnostic"),
        ),
    };
    let failed = record(
        tenant,
        conversation,
        epoch,
        4,
        Some(parent),
        None,
        DomainEvent::Run(RunEvent::Failed {
            error: error.clone(),
            last_attempt_id: None,
        }),
    );
    let failure_record_id = failed.metadata.record_id;
    let correlation_id = failed.metadata.correlation_id;
    reducer
        .apply(failed, Arc::clone(&content))
        .await
        .expect("project parent RunFailed");
    // Completion of a delegated child cannot erase the parent's failure or its answer.
    reducer
        .apply(
            record(
                tenant,
                conversation,
                epoch,
                5,
                Some(child),
                None,
                DomainEvent::Run(RunEvent::Completed {
                    assistant_message_id: None,
                    result_refs: vec![],
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("project child completion");
    let snapshot = reducer
        .finish(
            Arc::clone(&content),
            LedgerOffset::new(5),
            UtcTimestamp::from_unix_millis(6_000),
        )
        .await
        .expect("finish parent-child snapshot");
    let parent_run = snapshot
        .runs
        .iter()
        .find(|run| run.run_id == parent)
        .expect("find projected parent Run");
    assert_eq!(parent_run.status, RunStatus::Failed);
    let failure = parent_run
        .terminal_failure
        .as_ref()
        .expect("parent has terminal failure");
    assert_eq!(failure.error, error);
    assert_eq!(failure.ledger_record_id, failure_record_id);
    assert_eq!(failure.correlation_id, correlation_id);
    assert_eq!(
        failure.conversation_sequence,
        ConversationSeq::new(4).expect("failure sequence")
    );
    let child_run = snapshot
        .runs
        .iter()
        .find(|run| run.run_id == child)
        .expect("find projected child Run");
    assert_eq!(child_run.status, RunStatus::Completed);
    assert_eq!(child_run.terminal_failure, None);
    let answer = snapshot
        .stream_outputs
        .iter()
        .find(|output| output.run_id == parent)
        .expect("find retained assistant answer");
    assert_eq!(
        content
            .read(tenant, &answer.content_ref)
            .await
            .expect("read retained assistant answer"),
        b"Tests passed; waiting for child reports."
    );

    // A restored snapshot retains the same authoritative error without replaying the ledger.
    let restored =
        ProductProjectionReducer::new(tenant, conversation, epoch, Some(snapshot), BTreeMap::new())
            .expect("restore snapshot reducer")
            .finish(
                content,
                LedgerOffset::new(5),
                UtcTimestamp::from_unix_millis(7_000),
            )
            .await
            .expect("finish restored snapshot");
    assert_eq!(
        restored
            .runs
            .iter()
            .find(|run| run.run_id == parent)
            .expect("find restored parent Run")
            .terminal_failure
            .as_ref()
            .expect("preserve restored terminal failure")
            .error,
        error
    );
}
