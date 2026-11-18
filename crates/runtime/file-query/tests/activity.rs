mod support;
use musteragent_content_ref::Sha256;
use musteragent_execution_liveness::{ExecutionActivitySink, ExecutionOperation, LivenessError};
use musteragent_identity::{AttemptId, FencingToken, InvocationId, RunId};
use musteragent_runtime_capability::CooperativeExecution;
use musteragent_runtime_contract::{
    ApplicationFuture,
    execution_liveness::{ActivitySource, ExecutionBinding, ExecutionScope},
};
use musteragent_runtime_file_query::*;
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex},
};
use support::Fixture;

struct Authority(FileQueryContext);
impl FileQueryAuthority for Authority {
    fn resolve<'a>(
        &'a self,
        access: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>> {
        Box::pin(async move {
            assert_eq!(access.actor, self.0.actor);
            Ok(self.0.clone())
        })
    }
}
struct Sink {
    binding: ExecutionBinding,
    cursors: Mutex<Vec<(ActivitySource, u64)>>,
    reject_after: usize,
}
impl ExecutionActivitySink for Sink {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn accepted(
        &self,
    ) -> Result<musteragent_execution_liveness::ExecutionAcceptance, LivenessError> {
        Ok(musteragent_execution_liveness::ExecutionAcceptance::Observed)
    }
    fn wait_for_acceptance(
        &self,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<(), musteragent_execution_liveness::LivenessError>,
    > {
        Box::pin(async { Ok(()) })
    }
    fn ended(&self) -> Result<(), LivenessError> {
        Ok(())
    }
    fn advanced(&self, source: ActivitySource, cursor: NonZeroU64) -> Result<(), LivenessError> {
        let mut cursors = self.cursors.lock().expect("cursors");
        if cursors.len() == self.reject_after {
            return Err(LivenessError::InvalidPhase);
        }
        cursors.push((source, cursor.get()));
        Ok(())
    }
}

async fn query(
    reject_after: usize,
) -> (
    Result<(serde_json::Value, Vec<musteragent_content_ref::ContentRef>), FileQueryError>,
    usize,
    Arc<Sink>,
) {
    let f = Fixture::new();
    for index in 0..12 {
        f.attachment(
            &format!("{index}.txt"),
            "text/plain",
            b"native file content",
        );
    }
    let context = f.context.clone();
    let bytes = f.bytes.clone();
    let service = FileQueryService::new(
        Arc::new(Authority(context.clone())),
        Arc::new(f.application),
    );
    let binding = ExecutionBinding {
        tenant_id: context.actor.tenant_id,
        run_id: RunId::generate(),
        scope: ExecutionScope::Invocation(InvocationId::generate()),
        owner_attempt_id: AttemptId::generate(),
        owner_fence: FencingToken::new(1).expect("fence"),
        producer_generation: NonZeroU64::MIN,
        operation_identity: Sha256::digest(b"file-query-operation"),
    };
    let sink = Arc::new(Sink {
        binding,
        cursors: Mutex::default(),
        reject_after,
    });
    let operation = CooperativeExecution::new(
        binding,
        || Err(FileQueryError::Unavailable),
        move |activity| {
            Box::pin(async move {
                let access = FileQueryAccess {
                    actor: context.actor,
                    conversation_id: context.conversation_id,
                    workspace_id: None,
                    run: None,
                };
                service
                    .execute_tool_activity(
                        &access,
                        "files.search@2",
                        br#"{"scope":"conversation","query":"absent"}"#,
                        Some(&activity),
                    )
                    .await
            })
        },
    );
    let result = Box::new(operation).execute(sink.clone()).await;
    let reads = *bytes.reads.lock().expect("reads");
    (result, reads, sink)
}

#[tokio::test]
async fn activity_file_search_reports_real_reads_across_catalog_pages() {
    let (result, reads, sink) = query(usize::MAX).await;
    assert_eq!(result.expect("search").0["complete"], true);
    assert_eq!(reads, 12);
    let cursors = sink.cursors.lock().expect("cursors");
    assert!(cursors.len() >= reads);
    assert!(
        cursors
            .iter()
            .all(|(source, _)| *source == ActivitySource::ExecutorWork)
    );
    assert!(cursors.windows(2).all(|pair| pair[1].1 > pair[0].1));
}

#[tokio::test]
async fn activity_file_search_stops_between_batches_when_home_rejects_activity() {
    let (result, reads, _) = query(3).await;
    assert_eq!(
        result.expect_err("expired observation"),
        FileQueryError::Unavailable
    );
    assert!(
        reads > 0 && reads < 12,
        "no unobserved continuation after expiry"
    );
}
