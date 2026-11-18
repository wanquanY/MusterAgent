//! Streamable HTTP transport against an in-process hyper MCP server bound
//! to 127.0.0.1. No other network access.

use std::collections::HashSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use musteragent_identity::TenantId;
use musteragent_mcp::{McpAuthorizationKind, McpServerName, McpTransport as McpTransportConfig};
use musteragent_mcp_client::test_support::{MemoryContentStore, StaticCredentials};
use musteragent_mcp_client::{
    McpClientError, McpClientInvoker, McpClientLimits, McpClientOptions, McpContentKind,
    McpCredentialRequest, McpHttpSettings, McpServerBinding, McpSession, McpToolInvocation,
    McpToolInvoker, StreamableHttpConfig, StreamableHttpTransport, StructuredValidation,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::time::Instant;

const TOKEN: &str = "test-token";

#[derive(Default)]
struct ServerState {
    next_session: u32,
    sessions: HashSet<String>,
    deleted: Vec<String>,
    protocol_versions: Vec<String>,
    notifications: Vec<String>,
}

type Shared = Arc<Mutex<ServerState>>;

fn json_response(status: StatusCode, body: Value) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert("content-type", "application/json".parse().expect("header"));
    response
}

fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = status;
    response
}

fn sse(events: &[Value]) -> Response<Full<Bytes>> {
    let mut body = String::from(": stream opened\n\nid: prime\ndata:\n\n");
    for event in events {
        body.push_str(&format!("event: message\ndata: {event}\n\n"));
    }
    let mut response = Response::new(Full::new(Bytes::from(body)));
    response.headers_mut().insert(
        "content-type",
        "text/event-stream; charset=utf-8".parse().expect("header"),
    );
    response
}

fn tools() -> Value {
    json!([
        {"name": "echo", "description": "Echo", "inputSchema": {"type": "object"}},
        {"name": "add", "inputSchema": {"type": "object"},
         "outputSchema": {"type": "object", "properties": {"sum": {"type": "number"}}, "required": ["sum"]}},
        {"name": "huge", "inputSchema": {"type": "object"}},
        {"name": "expire", "inputSchema": {"type": "object"}}
    ])
}

async fn handle(
    request: Request<Incoming>,
    state: Shared,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let header = |name: &str| {
        request
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    if header("authorization").as_deref() != Some(&format!("Bearer {TOKEN}")) {
        return Ok(empty(StatusCode::UNAUTHORIZED));
    }
    let session = header("mcp-session-id");
    if request.method() == Method::DELETE {
        let mut state = state.lock().expect("state");
        if let Some(session) = session {
            state.sessions.remove(&session);
            state.deleted.push(session);
        }
        return Ok(empty(StatusCode::OK));
    }
    if request.method() != Method::POST {
        return Ok(empty(StatusCode::METHOD_NOT_ALLOWED));
    }
    let accept = header("accept").unwrap_or_default();
    if !accept.contains("application/json") || !accept.contains("text/event-stream") {
        return Ok(empty(StatusCode::NOT_ACCEPTABLE));
    }
    let protocol = header("mcp-protocol-version");
    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => return Ok(empty(StatusCode::BAD_REQUEST)),
    };
    let Ok(message) = serde_json::from_slice::<Value>(&body) else {
        return Ok(empty(StatusCode::BAD_REQUEST));
    };
    let method = message["method"].as_str().unwrap_or_default().to_owned();
    let id = message.get("id").cloned();

    if method == "initialize" {
        let mut state = state.lock().expect("state");
        state.next_session += 1;
        let session = format!("session-{}", state.next_session);
        state.sessions.insert(session.clone());
        let mut response = json_response(
            StatusCode::OK,
            json!({"jsonrpc": "2.0", "id": id, "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "http-test", "version": "1"}
            }}),
        );
        response
            .headers_mut()
            .insert("mcp-session-id", session.parse().expect("header"));
        return Ok(response);
    }
    {
        let mut state = state.lock().expect("state");
        match &session {
            Some(session) if state.sessions.contains(session) => {}
            Some(_) => return Ok(empty(StatusCode::NOT_FOUND)),
            None => return Ok(empty(StatusCode::BAD_REQUEST)),
        }
        state
            .protocol_versions
            .push(protocol.clone().unwrap_or_default());
        if id.is_none() || message.get("method").is_none() {
            state.notifications.push(method.clone());
            return Ok(empty(StatusCode::ACCEPTED));
        }
    }
    let response = match method.as_str() {
        "ping" => sse(&[json!({"jsonrpc": "2.0", "id": id, "result": {}})]),
        "tools/list" => json_response(
            StatusCode::OK,
            json!({"jsonrpc": "2.0", "id": id, "result": {"tools": tools()}}),
        ),
        "tools/call" => {
            let arguments = &message["params"]["arguments"];
            match message["params"]["name"].as_str().unwrap_or_default() {
                "echo" => sse(&[
                    json!({"jsonrpc": "2.0", "method": "notifications/progress",
                           "params": {"progressToken": 1, "progress": 1}}),
                    json!({"jsonrpc": "2.0", "id": id, "result": {"content": [
                        {"type": "text", "text": arguments["text"]}]}}),
                ]),
                "add" => {
                    let sum = arguments["a"].as_f64().unwrap_or(0.0)
                        + arguments["b"].as_f64().unwrap_or(0.0);
                    json_response(
                        StatusCode::OK,
                        json!({"jsonrpc": "2.0", "id": id, "result": {
                            "content": [], "structuredContent": {"sum": sum}}}),
                    )
                }
                "huge" => json_response(
                    StatusCode::OK,
                    json!({"jsonrpc": "2.0", "id": id, "result": {"content": [
                        {"type": "text", "text": "y".repeat(64 * 1024)}]}}),
                ),
                "expire" => {
                    if let Some(session) = &session {
                        state.lock().expect("state").sessions.remove(session);
                    }
                    json_response(
                        StatusCode::OK,
                        json!({"jsonrpc": "2.0", "id": id, "result": {"content": []}}),
                    )
                }
                _ => json_response(
                    StatusCode::OK,
                    json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32602, "message": "unknown"}}),
                ),
            }
        }
        _ => json_response(
            StatusCode::OK,
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "nope"}}),
        ),
    };
    Ok(response)
}

async fn serve() -> (SocketAddr, Shared) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let state = Shared::default();
    let server_state = Arc::clone(&state);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let state = Arc::clone(&server_state);
            tokio::spawn(async move {
                let service = service_fn(move |request| handle(request, Arc::clone(&state)));
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (address, state)
}

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

fn binding(address: SocketAddr, tenant_id: TenantId) -> McpServerBinding {
    McpServerBinding {
        tenant_id,
        server_name: McpServerName::new("http-test").expect("name"),
        transport: McpTransportConfig::StreamableHttp {
            url: format!("http://{address}/mcp"),
        },
        authorization: McpAuthorizationKind::Bearer,
    }
}

fn invoker(
    store: Arc<MemoryContentStore>,
    token: &str,
    limits: McpClientLimits,
) -> McpClientInvoker {
    McpClientInvoker::new(
        store,
        Arc::new(StaticCredentials {
            bearer_token: Some(token.to_owned()),
            environment: Vec::new(),
        }),
        None,
    )
    .with_http_settings(McpHttpSettings {
        connect_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(5),
        ..McpHttpSettings::default()
    })
    .with_options(McpClientOptions {
        limits,
        ..McpClientOptions::default()
    })
}

fn call(address: SocketAddr, tenant: TenantId, tool: &str, arguments: Value) -> McpToolInvocation {
    McpToolInvocation {
        binding: binding(address, tenant),
        tool_name: tool.to_owned(),
        arguments,
        deadline: soon(),
        cancellation: None,
    }
}

#[tokio::test]
async fn json_and_sse_responses_session_and_protocol_headers() {
    let (address, state) = serve().await;
    let store = Arc::new(MemoryContentStore::default());
    let invoker = invoker(store.clone(), TOKEN, McpClientLimits::default());
    let tenant = TenantId::generate();

    let echo = invoker
        .invoke(call(address, tenant, "echo", json!({"text": "over sse"})))
        .await
        .expect("echo over SSE");
    assert_eq!(echo.items[0].kind, McpContentKind::Text);
    assert_eq!(store.bytes(&echo.items[0].content), b"over sse");

    let add = invoker
        .invoke(call(address, tenant, "add", json!({"a": 1, "b": 2})))
        .await
        .expect("add over JSON");
    let structured = add.structured.expect("structured");
    assert_eq!(structured.validation, StructuredValidation::Validated);
    assert_eq!(store.bytes(&structured.content), br#"{"sum":3.0}"#);

    invoker.shutdown().await;
    let state = state.lock().expect("state");
    assert_eq!(state.deleted, vec!["session-1".to_owned()]);
    assert_eq!(
        state.notifications,
        vec!["notifications/initialized".to_owned()]
    );
    assert!(
        state
            .protocol_versions
            .iter()
            .all(|version| version == "2025-06-18")
    );
}

#[tokio::test]
async fn session_api_over_http_supports_ping_and_listing() {
    let (address, _state) = serve().await;
    let config = StreamableHttpConfig::new(
        format!("http://{address}/mcp").parse().expect("url"),
        McpCredentialRequest {
            tenant_id: TenantId::generate(),
            server_name: McpServerName::new("http-test").expect("name"),
            authorization: McpAuthorizationKind::Bearer,
        },
    );
    let transport = Arc::new(
        StreamableHttpTransport::new(
            config,
            Arc::new(StaticCredentials {
                bearer_token: Some(TOKEN.into()),
                environment: Vec::new(),
            }),
        )
        .expect("transport"),
    );
    let session = McpSession::connect(transport.clone(), McpClientOptions::default(), soon())
        .await
        .expect("connect");
    assert!(transport.has_session());
    assert_eq!(session.handshake().protocol_version, "2025-06-18");
    session.ping(soon()).await.expect("ping over SSE");
    assert_eq!(session.list_tools(soon()).await.expect("list").len(), 4);
    session.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn wrong_credentials_are_reported_as_authorization_required() {
    let (address, _state) = serve().await;
    let invoker = invoker(
        Arc::new(MemoryContentStore::default()),
        "wrong-token",
        McpClientLimits::default(),
    );
    assert_eq!(
        invoker
            .invoke(call(
                address,
                TenantId::generate(),
                "echo",
                json!({"text": "x"})
            ))
            .await,
        Err(McpClientError::AuthorizationRequired)
    );
}

#[tokio::test]
async fn oversized_response_is_rejected_not_truncated() {
    let (address, _state) = serve().await;
    let limits = McpClientLimits {
        max_message_bytes: 16 * 1024,
        max_http_response_bytes: 32 * 1024,
        ..McpClientLimits::default()
    };
    let store = Arc::new(MemoryContentStore::default());
    let invoker = invoker(store.clone(), TOKEN, limits);
    let tenant = TenantId::generate();
    assert_eq!(
        invoker
            .invoke(call(address, tenant, "huge", json!({})))
            .await,
        Err(McpClientError::MessageTooLarge { limit: 16 * 1024 })
    );
    assert!(store.is_empty());
    // The poisoned session is replaced on the next invocation.
    invoker
        .invoke(call(address, tenant, "echo", json!({"text": "after"})))
        .await
        .expect("fresh session");
}

#[tokio::test]
async fn expired_session_is_reported_and_reconnected() {
    let (address, state) = serve().await;
    let invoker = invoker(
        Arc::new(MemoryContentStore::default()),
        TOKEN,
        McpClientLimits::default(),
    );
    let tenant = TenantId::generate();
    invoker
        .invoke(call(address, tenant, "expire", json!({})))
        .await
        .expect("expire");
    assert_eq!(
        invoker
            .invoke(call(address, tenant, "echo", json!({"text": "x"})))
            .await,
        Err(McpClientError::SessionExpired)
    );
    invoker
        .invoke(call(address, tenant, "echo", json!({"text": "again"})))
        .await
        .expect("reconnected");
    assert_eq!(state.lock().expect("state").next_session, 2);
}
