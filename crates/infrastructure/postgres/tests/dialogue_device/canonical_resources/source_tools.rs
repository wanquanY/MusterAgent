//! The Native custody transport uses the same source owner; producer and
//! provider behavior remain separately tested at their original boundaries.
use super::*;
use musteragent_goal::event_subscription::EventSubscriptionConfiguration;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::owner_event_sources::*;
struct Selection {
    actor: Actor,
    agent: AgentId,
}
impl OwnerGoalSourceSelection for Selection {
    fn catalog(
        &self,
        subject: OwnerGoalSourceSubject,
    ) -> ApplicationFuture<'_, Result<Vec<u8>, EventSourceStoreError>> {
        assert_eq!(subject.actor, self.actor);
        assert_eq!(subject.agent_id, self.agent);
        Box::pin(async {
            Ok(br#"{"schema_version":"musteragent.goal-source-catalog.v1","sources":[]}"#.to_vec())
        })
    }
    fn resources(
        &self,
        _: OwnerGoalSourceSubject,
        _: OwnerGoalSourceSelector,
        _: Option<String>,
    ) -> ApplicationFuture<'_, Result<Vec<u8>, EventSourceStoreError>> {
        Box::pin(async { Err(EventSourceStoreError::Forbidden) })
    }
    fn qualify(
        &self,
        _: OwnerGoalSourceSubject,
        _: EventSubscriptionConfiguration,
    ) -> ApplicationFuture<'_, Result<(), EventSourceStoreError>> {
        Box::pin(async { Err(EventSourceStoreError::Forbidden) })
    }
}
fn output(receipt: &DeviceResourceReceipt) -> Value {
    serde_json::from_slice(
        &receipt
            .success()
            .expect("Native source tool succeeded")
            .result_bytes,
    )
    .unwrap()
}
#[tokio::test]
async fn native_goal_sources_use_original_custody_and_reject_unqualified_effects() {
    let mut f = Canonical::new().await;
    f.store = f.store.clone().with_source_selection(Arc::new(Selection {
        actor: f.f.actor,
        agent: f.f.custody.source.scope.agent_id,
    }));
    let catalog = f.request("goal.sources.catalog@1", json!({})).await;
    let result = f.execute(&catalog).await;
    assert_eq!(
        output(&result)["schema_version"],
        "musteragent.goal-source-catalog.v1"
    );
    assert_eq!(
        f.execute(&catalog).await,
        result,
        "lost Native response reuses the signed original command receipt"
    );
    let injected = f
        .request(
            "goal.sources.catalog@1",
            json!({"actor":"not-model-authority"}),
        )
        .await;
    assert!(matches!(
        f.execute(&injected).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));
    let create=f.request("goal.create@3",json!({"action_key":"source-tool-test","title":"Chosen source test","instructions":"Watch only a chosen authorized source.","constraints":[],"success_criteria":["A real change is observed"],"evidence_requirements":["Original source"],"failure_conditions":[],"high_risk":false})).await;
    let goal = output(&f.execute(&create).await)["goal_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let list = f
        .request("goal.sources.list@1", json!({"goal_id":goal}))
        .await;
    assert_eq!(output(&f.execute(&list).await)["sources"], json!([]));
    let foreign = f
        .request(
            "goal.sources.list@1",
            json!({"goal_id":GoalId::generate().to_string()}),
        )
        .await;
    assert!(matches!(
        f.execute(&foreign).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));
    let rejected=f.request("goal.sources.configure@1",json!({"action":"prepare","goal_id":goal,"expected_goal_revision":"1","action_key":"unqualified-source",
        "configuration":{"application_binding_id":OwnerApplicationBindingId::generate().to_string(),"application_revision":"1","application_authority_epoch":"1","capability_ref":"fixture.mail.changes@1","capability_sha256":Sha256::digest(b"not-a-qualified-source").to_string(),"selection_json":"{}","event_kinds":["fixture.changed@1"]},"required":true})).await;
    assert!(matches!(
        f.execute(&rejected).await.outcome,
        DeviceResourceOutcome::Rejected(_)
    ));
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM agent.owner_event_subscriptions),(SELECT count(*) FROM agent.owner_command_receipts WHERE command_kind='owner.event_source.control.v1'),(SELECT count(*) FROM runtime.goal_pursuits)").fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(
        counts,
        (0, 0, 0),
        "failed qualification never creates a source, control receipt or active task"
    );
}
