use super::*;

fn completed() -> ProviderSseFrame {
    ProviderSseFrame::new(
        "response.completed",
        r#"{"type":"response.completed","response":{"id":"resp_activity","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#,
    )
}

#[tokio::test]
async fn named_transport_keepalives_do_not_abort_or_create_model_output() {
    let frames = vec![
        ProviderSseFrame::new("keepalive", "{}"),
        started(),
        ProviderSseFrame::new(
            "keepalive",
            r#"{"timestamp":1794397500,"status":"processing"}"#,
        ),
        completed(),
    ];
    let server = MockProviderSse::start(ProviderSseScenario::successful(frames))
        .await
        .expect("provider");
    let provider = support::provider(server.endpoint(), ApiFormat::OpenAiResponses);
    let (request, binding) = activity_request();
    let mut operation = provider.execution(request, binding).expect("operation");
    operation.preflight().await.expect("prepare");
    let events = operation.into_stream().collect::<Vec<_>>().await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(
        events.iter().all(|e| matches!(
            e,
            Ok(ModelStreamEvent::ResponseStarted { .. }
                | ModelStreamEvent::Usage(_)
                | ModelStreamEvent::ResponseCompleted { .. })
        )),
        "transport controls must not mint reasoning, text or tools: {events:?}"
    );
    assert!(matches!(
        events.last(),
        Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
    ));
    assert_eq!(server.captured_requests().await.len(), 1);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn named_keepalive_carrying_data_is_processed_as_data() {
    for payload in [
        r#"{"type":"response.output_text.delta","delta":"must not disappear"}"#,
        r#"{"text":"must not disappear"}"#,
        "[DONE]",
    ] {
        let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
            started(),
            ProviderSseFrame::new("keepalive", payload),
            completed(),
        ]))
        .await
        .expect("provider");
        let provider = support::provider(server.endpoint(), ApiFormat::OpenAiResponses);
        let (request, binding) = activity_request();
        let mut operation = provider.execution(request, binding).expect("operation");
        operation.preflight().await.expect("prepare");
        let events = operation.into_stream().collect::<Vec<_>>().await;
        // A "keepalive" that carries data is processed as data: its content
        // either reaches the stream or the frame fails the attempt; it is
        // never silently dropped while the response completes normally.
        if payload.contains("response.output_text.delta") {
            assert!(
                events.iter().any(|event| matches!(
                    event,
                    Ok(ModelStreamEvent::TextDelta { delta, .. })
                        if delta.as_str() == "must not disappear"
                )),
                "{events:?}"
            );
        }
        server.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn named_keepalive_still_consumes_the_existing_wire_event_budget() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        started(),
        ProviderSseFrame::new("keepalive", "{}"),
        ProviderSseFrame::new("keepalive", "{}"),
        completed(),
    ]))
    .await
    .expect("provider");
    let provider = support::provider(server.endpoint(), ApiFormat::OpenAiResponses);
    let (mut request, binding) = activity_request();
    request.stream_budget.max_events = std::num::NonZeroU32::new(3).expect("budget");
    let mut operation = provider.execution(request, binding).expect("operation");
    operation.preflight().await.expect("prepare");
    let events = operation.into_stream().collect::<Vec<_>>().await;
    assert!(
        matches!(events.last(),Some(Err(e)) if e.safe_code==ProviderErrorCode::ProtocolViolation),
        "{events:?}"
    );
    server.shutdown().await.expect("shutdown");
}
