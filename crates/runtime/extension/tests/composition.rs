mod support;
use support::*;

#[tokio::test]
async fn real_loop_and_durable_tool_executor_smoke() {
    let fixture = Fixture::new(
        ExtensionRegistry::default(),
        vec![
            tool_events("one", "probe", serde_json::json!({})),
            assistant(),
        ],
        Default::default(),
        "Hello",
    );
    let report = fixture.run().await;
    assert!(
        matches!(report.exit, AgentLoopExit::Completed { .. }),
        "{:?}",
        report.exit
    );
    assert_eq!(lock(&fixture.tools.executions).len(), 1);
    assert_eq!(lock(&fixture.journal.results).len(), 1);
}
#[path = "composition/hooks.rs"]
mod hooks;
#[path = "composition/lifecycle.rs"]
mod lifecycle;
#[path = "composition/loops.rs"]
mod loops;
#[path = "composition/registration.rs"]
mod registration;
#[path = "composition/slim.rs"]
mod slim;
