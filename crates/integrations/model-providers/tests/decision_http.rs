mod support;
use musteragent_identity::{DottedKey, UtcTimestamp};
use musteragent_model::{ApiFormat, AuxiliaryModelRole, DecisionEvaluation, ModelParameter};
use musteragent_runtime_model::{
    DecisionModelProvider, DecisionModelRequest, ModelInvocationRouteRole, ModelRequestAuthority,
};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../contracts/fixtures/decision-systemone.json"
    ))
    .expect("valid decision fixture")
}
fn request(format: ApiFormat) -> DecisionModelRequest {
    let canonical = support::request(format);
    let mut authority = ModelRequestAuthority::from(&canonical);
    authority.invocation.timing =
        musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed {
            deadline: UtcTimestamp::from_unix_millis(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock after epoch")
                    .as_millis() as i64
                    + 5000,
            ),
        };
    authority.route_role = ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Decision);
    authority.route.capabilities = ["decision.choice", "decision.score", "decision.noul"]
        .map(|s| DottedKey::new(s).expect("valid fixture capability"))
        .into();
    authority.route.context_window =
        std::num::NonZeroU64::new(65536).expect("positive context window");
    authority.route.parameters = [
        ("max_state_question_tokens", 32768),
        ("max_questions", 128),
        ("max_choice_options", 255),
        ("max_score_levels", 10),
        ("max_request_bytes", 262144),
    ]
    .into_iter()
    .map(|(k, v)| {
        (
            DottedKey::new(format!("decision.{k}")).expect("valid decision limit key"),
            ModelParameter::Int(v),
        )
    })
    .collect();
    DecisionModelRequest {
        authority,
        evaluation: serde_json::from_value(fixture()["request"].clone())
            .expect("valid decision request"),
    }
}
async fn server(status: u16, body: Value) -> (String, tokio::task::JoinHandle<Value>) {
    paced_server(status, body, std::time::Duration::ZERO).await
}
async fn paced_server(
    status: u16,
    body: Value,
    delay: std::time::Duration,
) -> (String, tokio::task::JoinHandle<Value>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let address = listener.local_addr().expect("fixture server address");
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept provider connection");
        let mut bytes = Vec::new();
        let (start, length) = loop {
            let mut buf = [0_u8; 4096];
            let n = socket.read(&mut buf).await.expect("read request headers");
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
            if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..index]);
                assert!(
                    headers
                        .to_ascii_lowercase()
                        .contains("authorization: bearer ")
                );
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|v| v.parse::<usize>().ok())
                    })
                    .expect("request content length");
                break (index + 4, length);
            }
        };
        while bytes.len() < start + length {
            let mut buf = [0_u8; 4096];
            let n = socket.read(&mut buf).await.expect("read request body");
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
        }
        let response = body.to_string();
        socket.write_all(format!("HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",response.len()).as_bytes()).await.expect("write response headers");
        tokio::time::sleep(delay).await;
        // Cancellation fixtures deliberately close this exact connection.
        let _ = socket.write_all(response.as_bytes()).await;
        serde_json::from_slice(&bytes[start..start + length]).expect("decode native request")
    });
    (format!("http://{address}/decisions"), task)
}
#[tokio::test]
async fn both_decision_protocols_send_native_json_and_preserve_fractional_scores() {
    for format in [ApiFormat::TypeSafeSystemOne, ApiFormat::OpenRouterDecisions] {
        let (url, capture) = server(200, fixture()["response"].clone()).await;
        let response = support::provider(&url, format)
            .evaluate(request(format))
            .await
            .expect("evaluate decision request");
        assert_eq!(response.usage.output_tokens, 5);
        let wire = capture.await.expect("join capture task");
        assert_eq!(wire["state"], fixture()["request"]["state"]);
        assert_eq!(wire["questions"], fixture()["request"]["questions"]);
        assert_eq!(wire.as_object().expect("request object").len(), 3);
        assert_eq!(wire["model"], support::REQUESTED_MODEL);
    }
}
#[tokio::test]
async fn invalid_paid_answer_preserves_usage_on_failure() {
    let mut body = fixture()["response"].clone();
    body["answers"]["quality"]["score"] = json!(99);
    let (url, capture) = server(200, body).await;
    let error = support::provider(&url, ApiFormat::TypeSafeSystemOne)
        .evaluate(request(ApiFormat::TypeSafeSystemOne))
        .await
        .expect_err("invalid paid answer must fail validation");
    assert_eq!(
        error
            .usage
            .expect("failed response retains usage")
            .input_tokens,
        90
    );
    capture.await.expect("join capture task");
}
#[tokio::test]
async fn chat_authority_and_bad_input_are_rejected_before_network() {
    let provider = support::provider("http://127.0.0.1:1/decisions", ApiFormat::TypeSafeSystemOne);
    let mut req = request(ApiFormat::TypeSafeSystemOne);
    req.authority.route_role = ModelInvocationRouteRole::Chat;
    assert!(provider.evaluate(req).await.is_err());
    let mut req = request(ApiFormat::TypeSafeSystemOne);
    req.evaluation = DecisionEvaluation {
        state: json!("x"),
        questions: Default::default(),
    };
    assert!(provider.evaluate(req).await.is_err());
}

#[tokio::test]
async fn rounded_live_responses_survive_http_transport_without_rewriting() {
    let cases: Vec<Value> = serde_json::from_str(include_str!(
        "../../../../contracts/fixtures/decision-rounded-responses.json"
    ))
    .expect("valid rounded response fixtures");
    for case in cases.iter().filter(|case| case["valid"] == true) {
        let format = ApiFormat::OpenRouterDecisions;
        let (url, capture) = server(200, case["response"].clone()).await;
        let mut invocation = request(format);
        invocation.evaluation =
            serde_json::from_value(case["request"].clone()).expect("valid rounded request");
        let response = support::provider(&url, format)
            .evaluate(invocation)
            .await
            .expect("evaluate rounded request");
        let expected: musteragent_model::DecisionResponse =
            serde_json::from_value(case["response"].clone()).expect("valid expected response");
        assert_eq!(response.answers, expected.answers, "{}", case["name"]);
        capture.await.expect("join capture task");
    }
}

#[path = "decision_http/activity.rs"]
mod activity;
#[tokio::test]
async fn activity_decision_has_no_legacy_total_or_read_lifetime() {
    activity::no_hidden_lifetime().await;
}
#[tokio::test]
async fn cancelled_decision_retains_remote_stop_uncertainty() {
    activity::in_flight_cancel_keeps_unknown().await;
}
#[tokio::test]
async fn stopped_prepared_decision_cannot_dispatch_later() {
    activity::rejected_prepared_request_never_dispatches().await;
}
