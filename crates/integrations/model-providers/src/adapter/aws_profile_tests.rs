use super::*;
use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_types::event_stream::{Header, HeaderValue as EventHeader, Message};
use secrecy::SecretString;

fn gateway(format: ApiFormat) -> ProviderEndpoint {
    ProviderEndpoint {
        model_provider_id: "019fa6de-38fd-7000-8000-000000000741"
            .parse()
            .expect("provider"),
        model_entry_id: "019fa6de-38fd-7000-8000-000000000742"
            .parse()
            .expect("model"),
        api_format: format,
        invocation_url: url::Url::parse("https://gateway.example/v1/bedrock/invoke").expect("URL"),
        connect_timeout: Duration::from_secs(1),
        read_timeout: Duration::from_secs(2),
        total_timeout: Duration::from_secs(3),
        anthropic_version: None,
        aws_region: Some("proxy".to_owned()),
        origin: crate::ProviderEndpointOrigin::RouteGateway,
    }
}

#[test]
fn bedrock_gateway_keeps_scoped_bearer_and_registry_identity_without_direct_signing() {
    for format in [ApiFormat::BedrockConverse, ApiFormat::BedrockInvokeModel] {
        let endpoint = gateway(format);
        let request = build_request(
            &reqwest::Client::new(),
            &endpoint,
            CredentialMaterial::ScopedBearer {
                secret: SecretString::from("fixture-gateway-token"),
                expires_at: musteragent_identity::UtcTimestamp::from_unix_millis(9_000),
            },
            bytes::Bytes::from_static(b"{}"),
            "application/vnd.amazon.eventstream",
            "application/json",
        )
        .expect("gateway does not require upstream signing");
        assert_eq!(
            request.headers()["authorization"],
            "Bearer fixture-gateway-token"
        );
        assert_eq!(
            request.headers()["x-musteragent-model-entry-id"],
            endpoint.model_entry_id.to_string()
        );
        assert!(!request.headers().contains_key("x-amz-date"));
        assert!(!request.headers().contains_key("x-amz-security-token"));
    }
}

#[test]
fn bedrock_wire_frames_remain_decodable_in_the_gateway_only_profile() {
    let converse = serde_json::json!({"delta":{"text":"fixture response"}});
    let invoke = serde_json::json!({"type":"content_block_delta", "delta":{"type":"text_delta", "text":"fixture response"}});
    for (format, event, payload, expected_event, expected) in [
        (
            ApiFormat::BedrockConverse,
            "contentBlockDelta",
            converse.clone(),
            "contentBlockDelta",
            converse,
        ),
        (
            ApiFormat::BedrockInvokeModel,
            "chunk",
            serde_json::json!({"bytes":BASE64_STANDARD.encode(invoke.to_string())}),
            "content_block_delta",
            invoke,
        ),
    ] {
        let message = Message::new(payload.to_string().into_bytes())
            .add_header(Header::new(
                ":event-type",
                EventHeader::String(event.into()),
            ))
            .add_header(Header::new(
                ":content-type",
                EventHeader::String("application/json".into()),
            ))
            .add_header(Header::new(
                ":message-type",
                EventHeader::String("event".into()),
            ));
        let mut wire = Vec::new();
        write_message_to(&message, &mut wire).expect("encode framed fixture");
        let mut bytes = BytesMut::from(wire.as_slice());
        let DecodedFrame::Complete(frame) = MessageFrameDecoder::new()
            .decode_frame(&mut bytes)
            .expect("decode frame")
        else {
            panic!("complete bounded fixture");
        };
        let (name, value) = decode_aws_message(&frame, format, 4_096).expect("normalize payload");
        assert_eq!(name, expected_event);
        assert_eq!(value, expected);
        assert!(bytes.is_empty());
        assert!(
            decode_aws_message(&frame, format, 1).is_err(),
            "bounds still enforced"
        );
    }
}

#[cfg(not(feature = "direct-aws-signing"))]
#[test]
fn disabled_direct_signing_fails_before_a_request_can_be_sent() {
    let result = build_request(
        &reqwest::Client::new(),
        &gateway(ApiFormat::BedrockConverse),
        CredentialMaterial::AwsSigV4(crate::AwsCredentialMaterial {
            access_key_id: SecretString::from("fixture-access-key"),
            secret_access_key: SecretString::from("fixture-secret-key"),
            session_token: None,
        }),
        bytes::Bytes::from_static(b"{}"),
        "application/vnd.amazon.eventstream",
        "application/json",
    );
    let error = result.expect_err("no unsigned or proxy-token fallback for direct credentials");
    assert_eq!(error.kind, ProviderErrorKind::Authorization);
    assert_eq!(
        error.safe_code,
        ProviderErrorCode::RouteConfigurationInvalid
    );
    assert!(!error.retryable);
}
