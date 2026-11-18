mod support;
use axum::{Json, Router, extract::State, routing::post};
use futures_util::StreamExt as _;
use musteragent_identity::DottedKey;
use musteragent_model::ApiFormat;
use musteragent_runtime_model::{ModelProvider, ModelStreamEvent, ModelStreamState};
use serde_json::{Value, json};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Wire {
    requests: Arc<Mutex<Vec<Value>>>,
    format: ApiFormat,
    same_identity: bool,
}
async fn generate(
    State(state): State<Wire>,
    Json(body): Json<Value>,
) -> ([(String, String); 1], String) {
    let hop = {
        let mut calls = state.requests.lock().expect("valid test fixture");
        calls.push(body);
        calls.len()
    };
    let identity = if state.same_identity {
        "same-hop".to_owned()
    } else {
        format!("hop-{hop}")
    };
    let value = if state.format == ApiFormat::GoogleGenerateContent {
        let mut candidate = json!({"content":{"role":"model","parts":[{"text":if hop==1 {"first"} else {"last"},"thoughtSignature":"private-signature"}]},"finishReason":if hop==1 {"CONTINUATION"} else {"STOP"}});
        if hop == 1 {
            candidate["continuationToken"] = json!("hop-proof");
        }
        json!({"responseId":identity,"candidates":[candidate],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":2,"thoughtsTokenCount":1,"totalTokenCount":13}})
    } else {
        let mut interaction = json!({"id":identity,"status":if hop==1 {"requires_continuation"} else {"completed"},"usage":{"total_input_tokens":10,"total_output_tokens":2,"total_thought_tokens":1,"total_tokens":13}});
        if hop == 1 {
            interaction["continuation_token"] = json!("hop-proof");
        }
        return (
            [("content-type".into(), "text/event-stream".into())],
            format!(
                "data: {}\n\ndata: {}\n\ndata: {}\n\ndata: {}\n\n",
                json!({"event_type":"interaction.created","interaction":{"id":identity,"status":"in_progress"}}),
                json!({"event_type":"step.start","index":0,"step":{"type":"model_output"}}),
                json!({"event_type":"step.delta","index":0,"delta":{"type":"text","text":if hop==1 {"first"} else {"last"}}}),
                json!({"event_type":"interaction.completed","interaction":interaction})
            ),
        );
    };
    (
        [("content-type".into(), "text/event-stream".into())],
        format!("data: {value}\n\n"),
    )
}

#[tokio::test]
async fn native_google_long_decode_is_one_canonical_stream_with_exact_followup_and_summed_usage() {
    for (format, same_identity) in [
        (ApiFormat::GoogleGenerateContent, false),
        (ApiFormat::GoogleGenerateContent, true),
        (ApiFormat::GoogleInteractions, false),
        (ApiFormat::GoogleInteractions, true),
    ] {
        let state = Wire {
            requests: Arc::new(Mutex::new(Vec::new())),
            format,
            same_identity,
        };
        let router = Router::new()
            .route("/generate", post(generate))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("valid test fixture");
        let address = listener.local_addr().expect("valid test fixture");
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("valid test fixture");
        });
        let provider = support::provider(&format!("http://{address}/generate"), format);
        let mut request = support::request(format);
        request.route.context_window = NonZeroU64::new(1_048_576).expect("valid test fixture");
        request.route.max_output_tokens = NonZeroU64::new(262144).expect("valid test fixture");
        request.max_output_tokens = request.route.max_output_tokens;
        request
            .route
            .capabilities
            .insert(DottedKey::new("usage_aggregates_requests").expect("valid test fixture"));
        let events = provider.stream(request).collect::<Vec<_>>().await;
        server.abort();
        let events = events
            .into_iter()
            .map(|event| event.unwrap_or_else(|error| panic!("{format:?}: {error:?}")))
            .collect::<Vec<_>>();
        let mut canonical = ModelStreamState::default();
        for event in &events {
            canonical.accept(event).expect("valid test fixture");
        }
        assert!(canonical.is_terminal());
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ModelStreamEvent::ResponseStarted { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ModelStreamEvent::ResponseCompleted { .. }))
                .count(),
            1
        );
        let text = events
            .iter()
            .filter_map(|event| {
                if let ModelStreamEvent::TextDelta { delta, .. } = event {
                    Some(delta.as_str())
                } else {
                    None
                }
            })
            .collect::<String>();
        assert_eq!(text, "firstlast");
        let usage = events
            .iter()
            .filter_map(|event| {
                if let ModelStreamEvent::Usage(usage) = event {
                    Some(usage)
                } else {
                    None
                }
            })
            .next_back()
            .expect("valid test fixture");
        assert_eq!(usage.input_tokens_uncached, 20);
        assert_eq!(usage.output_tokens, 4);
        let calls = state.requests.lock().expect("valid test fixture");
        assert_eq!(calls.len(), 2);
        let mut expected = calls[0].clone();
        expected[if format == ApiFormat::GoogleGenerateContent {
            "continuationToken"
        } else {
            "continuation_token"
        }] = json!("hop-proof");
        assert_eq!(calls[1], expected);
    }
}

#[tokio::test]
async fn provider_continuation_proof_cannot_expand_an_ordinary_admitted_request() {
    for format in [
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
    ] {
        let state = Wire {
            requests: Arc::new(Mutex::new(Vec::new())),
            format,
            same_identity: false,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("valid test fixture");
        let address = listener.local_addr().expect("valid test fixture");
        let router = Router::new()
            .route("/generate", post(generate))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("valid test fixture");
        });
        let provider = support::provider(&format!("http://{address}/generate"), format);
        let events = provider
            .stream(support::request(format))
            .collect::<Vec<_>>()
            .await;
        server.abort();
        assert_eq!(
            state.requests.lock().expect("valid test fixture").len(),
            1,
            "ordinary request must not create a new paid hop"
        );
        assert!(events.iter().any(Result::is_err));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Ok(ModelStreamEvent::ResponseCompleted { .. })))
        );
    }
}
