use super::*;
use crate::{MemoryProgramIdentity, MemoryWriteOperation, MemoryWritePlanner};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Package {
    identity: MemoryProgramIdentity,
    calls: AtomicUsize,
    purpose: &'static str,
    fail: bool,
}

impl Package {
    fn new(release: u128, purpose: &'static str) -> Self {
        Self {
            identity: MemoryProgramIdentity::published(
                Sha256::digest(format!("source-{release}").as_bytes()),
                Uuid::from_u128(release),
                Sha256::digest(format!("bundle-{release}").as_bytes()),
                Sha256::digest(format!("archive-{release}").as_bytes()),
            )
            .unwrap(),
            calls: AtomicUsize::new(0),
            purpose,
            fail: false,
        }
    }
}

impl MemoryWritePlanner for Package {
    fn identity(&self) -> MemoryProgramIdentity {
        self.identity.clone()
    }

    fn prepare<'a>(
        &'a self,
        operation: MemoryWriteOperation,
        request: &'a [u8],
    ) -> ApplicationFuture<'a, Result<Vec<u8>, MemoryToolError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(!self.fail, "recovery must not compute again");
            let mut decision = WritePreparation::prepare(operation.tool_key(), request).unwrap();
            if let WritePlan::ProposeV3(input) = &mut decision.plan {
                input.outcome = self.purpose.into();
            }
            decision
                .canonical_bytes()
                .map_err(|_| MemoryToolError::InvalidResult)
        })
    }
}

#[tokio::test]
async fn exact_release_recovery_preserves_package_decision_without_recomputation() {
    let (mut context, request) = fixture();
    let store = Store::default();
    store.state.lock().unwrap().lose_ack = true;
    let mut old = Package::new(1, "A package-owned purpose");
    let first = proposal_decision(&store, &old, &context, &request, None)
        .await
        .unwrap();
    assert_eq!(first.input.outcome, old.purpose);
    assert_eq!(old.calls.load(Ordering::SeqCst), 1);
    context.now = UtcTimestamp::from_unix_millis(1000);
    old.fail = true;
    let replay = proposal_decision(&store, &old, &context, &request, None)
        .await
        .unwrap();
    assert_eq!(replay.input, first.input);
    assert_eq!(replay.prepared_at, first.prepared_at);
    assert_eq!(old.calls.load(Ordering::SeqCst), 1);
    let next = Package::new(2, "A changed package purpose");
    assert!(matches!(
        proposal_decision(&store, &next, &context, &request, None).await,
        Err(MemoryToolError::InvalidResult)
    ));
    assert_eq!(next.calls.load(Ordering::SeqCst), 0);
    assert!(
        proposal_decision(
            &store,
            &CompiledMemoryWritePlanner,
            &context,
            &request,
            None
        )
        .await
        .is_err()
    );
    assert_eq!(store.state.lock().unwrap().writes, 1);
    // A genuinely new invocation binds the new release and may produce a new
    // purpose without rebuilding the Host validator or replacing old Content.
    let (new_context, new_request) = fixture();
    let result = proposal_decision(&store, &next, &new_context, &new_request, None)
        .await
        .unwrap();
    assert_eq!(result.input.outcome, next.purpose);
    assert_eq!(next.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_package_output_is_never_frozen() {
    let (context, request) = fixture();
    let store = Store::default();
    let invalid = Package::new(1, "");
    assert!(matches!(
        proposal_decision(&store, &invalid, &context, &request, None).await,
        Err(MemoryToolError::InvalidResult)
    ));
    assert_eq!(store.state.lock().unwrap().writes, 0);
}
