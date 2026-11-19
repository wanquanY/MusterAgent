mod support;

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use futures_util::StreamExt as _;
use musteragent_model::ApiFormat;
use musteragent_runtime_model::{
    ModelProvider, ModelStreamEvent, ProviderErrorCode, ProviderErrorKind,
};
use serde_json::Value;

use support::{
    FIXTURE_AWS_ACCESS_KEY, FIXTURE_AWS_SESSION_TOKEN, FIXTURE_SECRET, REQUESTED_MODEL,
    SourceFrame, compression_request, content_type, provider, provider_with_read_timeout,
    wire_chunks,
};

const APPROVED_FORMATS: [ApiFormat; 7] = [
    ApiFormat::OpenAiResponses,
    ApiFormat::OpenAiChatCompletions,
    ApiFormat::AnthropicMessages,
    ApiFormat::GoogleGenerateContent,
    ApiFormat::GoogleInteractions,
    ApiFormat::BedrockConverse,
    ApiFormat::BedrockInvokeModel,
];

#[tokio::test]
async fn context_compaction_approved_api_format_matrix() {
    for format in APPROVED_FORMATS {
        let target = invocation_target(format);
        let server = ControlledWire::start(
            &target,
            content_type(format),
            wire_chunks(format, &successful_frames(format)),
            false,
        );
        let direct = provider(server.endpoint(), format);
        let events = direct
            .stream(compression_request(format))
            .map(|result| result.unwrap_or_else(|error| panic!("{format:?}: {error:?}")))
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(
                events.last(),
                Some(ModelStreamEvent::ResponseCompleted { .. })
            ),
            "{format:?}: {events:?}",
        );

        let captured = server.captured();
        assert_eq!(captured.method, "POST", "{format:?}");
        assert_eq!(captured.target, target, "{format:?}");
        assert_auth(format, &captured.headers);
        assert_eq!(
            captured.headers.get("content-type").map(String::as_str),
            Some("application/json"),
            "{format:?}",
        );
        let expected_accept = if matches!(
            format,
            ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel
        ) {
            "application/vnd.amazon.eventstream"
        } else {
            "text/event-stream"
        };
        assert_eq!(
            captured.headers.get("accept").map(String::as_str),
            Some(expected_accept),
            "{format:?}",
        );
        let body: Value = serde_json::from_slice(&captured.body)
            .unwrap_or_else(|error| panic!("{format:?}: invalid JSON body: {error}"));
        assert_compression_body(format, &captured.target, &body);
        server.shutdown();
    }
}

#[tokio::test]
async fn every_approved_compression_direct_wire_types_timeout_and_malformed_failures() {
    for format in APPROVED_FORMATS {
        let target = invocation_target(format);
        let timeout_server = ControlledWire::start(&target, content_type(format), Vec::new(), true);
        let timeout_provider = provider_with_read_timeout(
            timeout_server.endpoint(),
            format,
            Duration::from_millis(40),
        );
        let timeout = timeout_provider
            .stream(compression_request(format))
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(
                timeout.last(),
                Some(Err(error))
                    if error.kind == ProviderErrorKind::Timeout
                        && error.safe_code == ProviderErrorCode::DeadlineExceeded
                        && error.retryable
            ),
            "{format:?}: {timeout:?}",
        );
        assert_eq!(timeout_server.captured().target, target, "{format:?}");
        timeout_server.shutdown();

        let malformed = [malformed_frame(format)];
        let malformed_server = ControlledWire::start(
            &target,
            content_type(format),
            wire_chunks(format, &malformed),
            false,
        );
        let malformed_provider = provider(malformed_server.endpoint(), format);
        let malformed = malformed_provider
            .stream(compression_request(format))
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(
                malformed.last(),
                // A malformed frame never completes the response and leaves
                // the attempt retryable within the Run's retry budget.
                Some(Err(error)) if error.retryable
            ),
            "{format:?}: {malformed:?}",
        );
        assert_eq!(malformed_server.captured().target, target, "{format:?}");
        malformed_server.shutdown();
    }
}

fn assert_auth(format: ApiFormat, headers: &BTreeMap<String, String>) {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiResponses | ApiFormat::OpenAiChatCompletions => {
            assert_eq!(
                headers
                    .get("authorization")
                    .unwrap_or_else(|| panic!("{format:?}: missing Authorization")),
                &format!("Bearer {FIXTURE_SECRET}"),
            );
        }
        ApiFormat::AnthropicMessages => {
            assert_eq!(
                headers.get("x-api-key").map(String::as_str),
                Some(FIXTURE_SECRET),
            );
            assert_eq!(
                headers.get("anthropic-version").map(String::as_str),
                Some("2023-06-01"),
            );
        }
        ApiFormat::GoogleGenerateContent | ApiFormat::GoogleInteractions => assert_eq!(
            headers.get("x-goog-api-key").map(String::as_str),
            Some(FIXTURE_SECRET),
            "{format:?}",
        ),
        ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel => {
            let authorization = headers
                .get("authorization")
                .unwrap_or_else(|| panic!("{format:?}: missing SigV4 Authorization"));
            assert!(
                authorization.starts_with("AWS4-HMAC-SHA256 ")
                    && authorization.contains(&format!("Credential={FIXTURE_AWS_ACCESS_KEY}/"))
                    && authorization.contains("/us-east-1/bedrock/aws4_request")
                    && authorization.contains("SignedHeaders=")
                    && authorization.contains("Signature="),
                "{format:?}: {authorization}",
            );
            assert_eq!(
                headers.get("x-amz-security-token").map(String::as_str),
                Some(FIXTURE_AWS_SESSION_TOKEN),
            );
            assert!(headers.contains_key("x-amz-date"));
        }
    }
}

fn assert_compression_body(format: ApiFormat, target: &str, body: &Value) {
    let mut fields = Vec::new();
    collect_fields(body, &mut fields);
    for forbidden in [
        "tools",
        "toolConfig",
        "thinking",
        "reasoning",
        "reasoning_effort",
        "include",
        "previous_response_id",
        "previous_interaction_id",
        "cache",
        "cache_control",
        "cached_content",
        "cachedContent",
        "prompt_cache_key",
        "prompt_cache_retention",
    ] {
        assert!(
            !fields.contains(&forbidden),
            "{format:?}: Compression leaked {forbidden}: {body}",
        );
    }
    for store in find_values(body, "store") {
        assert_eq!(store, &Value::Bool(false), "{format:?}: {body}");
    }
    let model_in_body = ["model", "modelId"]
        .into_iter()
        .flat_map(|key| find_values(body, key))
        .any(|value| value.as_str() == Some(REQUESTED_MODEL));
    assert!(
        model_in_body || target.contains(REQUESTED_MODEL),
        "{format:?}: admitted model missing from endpoint and body: {target} {body}",
    );
    assert!(
        body.to_string().contains("Hello"),
        "{format:?}: Compression history missing from body: {body}",
    );
}

fn collect_fields<'a>(value: &'a Value, output: &mut Vec<&'a str>) {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                output.push(key);
                collect_fields(value, output);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_fields(value, output);
            }
        }
        _ => {}
    }
}

fn find_values<'a>(value: &'a Value, key: &str) -> Vec<&'a Value> {
    let mut values = Vec::new();
    match value {
        Value::Object(object) => {
            for (field, value) in object {
                if field == key {
                    values.push(value);
                }
                values.extend(find_values(value, key));
            }
        }
        Value::Array(array) => {
            for value in array {
                values.extend(find_values(value, key));
            }
        }
        _ => {}
    }
    values
}

fn invocation_target(format: ApiFormat) -> String {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiResponses => "/v1/responses".to_owned(),
        ApiFormat::OpenAiChatCompletions => "/v1/chat/completions".to_owned(),
        ApiFormat::AnthropicMessages => "/v1/messages".to_owned(),
        ApiFormat::GoogleGenerateContent => {
            format!("/v1beta/models/{REQUESTED_MODEL}:streamGenerateContent?alt=sse")
        }
        ApiFormat::GoogleInteractions => "/v1beta/interactions".to_owned(),
        ApiFormat::BedrockConverse => {
            format!("/model/{REQUESTED_MODEL}/converse-stream")
        }
        ApiFormat::BedrockInvokeModel => {
            format!("/model/{REQUESTED_MODEL}/invoke-with-response-stream")
        }
    }
}

fn malformed_frame(format: ApiFormat) -> SourceFrame {
    SourceFrame {
        event: match format {
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
                unreachable!("decision protocols do not use chat fixtures")
            }
            ApiFormat::OpenAiResponses => "response.created",
            ApiFormat::OpenAiChatCompletions | ApiFormat::GoogleGenerateContent => "message",
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => "message_start",
            ApiFormat::GoogleInteractions => "interaction.created",
            ApiFormat::BedrockConverse => "messageStart",
        },
        data: "{malformed-json",
    }
}

fn successful_frames(format: ApiFormat) -> Vec<SourceFrame> {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiResponses => vec![
            SourceFrame {
                event: "response.created",
                data: r#"{"type":"response.created","response":{"id":"resp_cap010","model":"provider-reported-model"}}"#,
            },
            SourceFrame {
                event: "response.output_text.delta",
                data: r#"{"type":"response.output_text.delta","output_index":0,"delta":"compressed"}"#,
            },
            SourceFrame {
                event: "response.completed",
                data: r#"{"type":"response.completed","response":{"id":"resp_cap010","model":"provider-reported-model","usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}"#,
            },
        ],
        ApiFormat::OpenAiChatCompletions => vec![SourceFrame {
            event: "message",
            data: r#"{"id":"chat_cap010","model":"provider-reported-model","choices":[{"index":0,"delta":{"content":"compressed"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}"#,
        }],
        ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => vec![
            SourceFrame {
                event: "message_start",
                data: r#"{"type":"message_start","message":{"id":"msg_cap010","model":"provider-reported-model","usage":{"input_tokens":10,"output_tokens":0}}}"#,
            },
            SourceFrame {
                event: "content_block_delta",
                data: r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"compressed"}}"#,
            },
            SourceFrame {
                event: "message_delta",
                data: r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#,
            },
            SourceFrame {
                event: "message_stop",
                data: r#"{"type":"message_stop"}"#,
            },
        ],
        ApiFormat::GoogleGenerateContent => vec![SourceFrame {
            event: "message",
            data: r#"{"responseId":"google_cap010","modelVersion":"provider-reported-model","candidates":[{"content":{"parts":[{"text":"compressed"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":2,"totalTokenCount":12}}"#,
        }],
        ApiFormat::GoogleInteractions => vec![
            SourceFrame {
                event: "interaction.created",
                data: r#"{"event_type":"interaction.created","interaction":{"id":"interaction_cap010","model":"provider-reported-model","status":"in_progress"}}"#,
            },
            SourceFrame {
                event: "step.start",
                data: r#"{"event_type":"step.start","index":0,"step":{"type":"model_output"}}"#,
            },
            SourceFrame {
                event: "step.delta",
                data: r#"{"event_type":"step.delta","index":0,"delta":{"type":"text","text":"compressed"}}"#,
            },
            SourceFrame {
                event: "interaction.completed",
                data: r#"{"event_type":"interaction.completed","interaction":{"id":"interaction_cap010","model":"provider-reported-model","status":"completed"},"metadata":{"total_usage":{"total_input_tokens":10,"total_output_tokens":2,"total_tokens":12}}}"#,
            },
        ],
        ApiFormat::BedrockConverse => vec![
            SourceFrame {
                event: "messageStart",
                data: r#"{"role":"assistant"}"#,
            },
            SourceFrame {
                event: "contentBlockDelta",
                data: r#"{"contentBlockIndex":0,"delta":{"text":"compressed"}}"#,
            },
            SourceFrame {
                event: "metadata",
                data: r#"{"usage":{"inputTokens":10,"outputTokens":2,"totalTokens":12}}"#,
            },
            SourceFrame {
                event: "messageStop",
                data: r#"{"stopReason":"end_turn"}"#,
            },
        ],
    }
}

#[derive(Clone)]
struct CapturedRequest {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

struct ControlledWire {
    endpoint: String,
    captured: Arc<Mutex<Option<CapturedRequest>>>,
    shutdown: mpsc::Sender<()>,
    thread: thread::JoinHandle<()>,
}

impl ControlledWire {
    fn start(target: &str, content_type: &str, chunks: Vec<Vec<u8>>, hang: bool) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind controlled Direct wire");
        listener
            .set_nonblocking(true)
            .expect("set controlled wire nonblocking");
        let address = listener.local_addr().expect("controlled wire address");
        let captured = Arc::new(Mutex::new(None));
        let captured_for_thread = Arc::clone(&captured);
        let (shutdown, shutdown_receiver) = mpsc::channel();
        let content_type = content_type.to_owned();
        let thread = thread::spawn(move || {
            let started = Instant::now();
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            started.elapsed() < Duration::from_secs(5),
                            "Direct Provider did not connect to controlled wire",
                        );
                        if shutdown_receiver.try_recv().is_ok() {
                            return;
                        }
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("accept controlled Direct wire: {error}"),
                }
            };
            stream
                .set_nonblocking(false)
                .expect("restore blocking Direct wire stream");
            let request = read_request(&mut stream);
            *captured_for_thread.lock().expect("capture Direct request") = Some(request);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            )
            .expect("write controlled Direct headers");
            for chunk in chunks {
                write!(stream, "{:X}\r\n", chunk.len()).expect("write chunk length");
                stream.write_all(&chunk).expect("write Direct chunk");
                stream.write_all(b"\r\n").expect("finish Direct chunk");
                stream.flush().expect("flush Direct chunk");
            }
            if hang {
                let _ = shutdown_receiver.recv_timeout(Duration::from_secs(5));
            } else {
                stream
                    .write_all(b"0\r\n\r\n")
                    .expect("finish controlled Direct response");
                stream.flush().expect("flush Direct response");
            }
        });
        Self {
            endpoint: format!("http://{address}{target}"),
            captured,
            shutdown,
            thread,
        }
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn captured(&self) -> CapturedRequest {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(captured) = self
                .captured
                .lock()
                .expect("lock controlled capture")
                .clone()
            {
                return captured;
            }
            assert!(
                Instant::now() < deadline,
                "controlled Direct wire did not capture a request",
            );
            thread::sleep(Duration::from_millis(2));
        }
    }

    fn shutdown(self) {
        let _ = self.shutdown.send(());
        self.thread.join().expect("join controlled Direct wire");
    }
}

fn read_request(stream: &mut TcpStream) -> CapturedRequest {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set Direct request timeout");
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).expect("read Direct request");
        assert!(read > 0, "Direct request closed before headers");
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(offset) = find_subslice(&bytes, b"\r\n\r\n") {
            break offset + 4;
        }
    };
    let headers_text = std::str::from_utf8(&bytes[..header_end]).expect("UTF-8 Direct headers");
    let mut lines = headers_text.split("\r\n");
    let request_line = lines.next().expect("Direct request line");
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().expect("Direct method").to_owned();
    let target = request_parts.next().expect("Direct target").to_owned();
    let mut headers = BTreeMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').expect("Direct header");
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }
    let content_length = headers
        .get("content-length")
        .expect("Direct Content-Length")
        .parse::<usize>()
        .expect("numeric Direct Content-Length");
    while bytes.len().saturating_sub(header_end) < content_length {
        let read = stream.read(&mut buffer).expect("read Direct body");
        assert!(read > 0, "Direct request closed before body");
        bytes.extend_from_slice(&buffer[..read]);
    }
    CapturedRequest {
        method,
        target,
        headers,
        body: bytes[header_end..header_end + content_length].to_vec(),
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
