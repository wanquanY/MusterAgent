//! Reference containers must not add executable user turns on any provider.
use super::*;
use serde_json::Value;

const FORMATS: [ApiFormat; 7] = [
    ApiFormat::OpenAiChatCompletions,
    ApiFormat::OpenAiResponses,
    ApiFormat::AnthropicMessages,
    ApiFormat::GoogleGenerateContent,
    ApiFormat::GoogleInteractions,
    ApiFormat::BedrockConverse,
    ApiFormat::BedrockInvokeModel,
];

fn users(body: &Value, format: ApiFormat) -> Vec<&Value> {
    let key = match format {
        ApiFormat::OpenAiResponses | ApiFormat::GoogleInteractions => "input",
        ApiFormat::GoogleGenerateContent => "contents",
        _ => "messages",
    };
    body[key]
        .as_array()
        .expect("valid test fixture")
        .iter()
        .filter(|message| message["role"] == "user" || message["type"] == "user_input")
        .collect()
}

fn text(message: &Value, format: ApiFormat) -> String {
    let content = &message[if format == ApiFormat::GoogleGenerateContent {
        "parts"
    } else {
        "content"
    }];
    if let Some(text) = content.as_str() {
        return text.to_owned();
    }
    content
        .as_array()
        .expect("valid test fixture")
        .iter()
        .filter_map(|part| part["text"].as_str())
        .collect()
}

#[test]
fn repeated_images_remain_at_their_original_turn_across_protocols() {
    for format in FORMATS {
        let (mut request, contents) = image_request(format);
        let photo = request.input[0].content[1].clone();
        request.input.clear();
        for turn in 1..=10 {
            let mut user = message(
                CanonicalRole::User,
                &format!("Question {turn} with quoted assistant: and source_id"),
            );
            if turn == 3 || turn == 6 {
                user.content.push(photo.clone());
            }
            request.input.push(user);
            if turn < 10 {
                request
                    .input
                    .push(message(CanonicalRole::Assistant, &format!("Answer {turn}")));
            }
        }
        let body = map_request(&request, &contents)
            .expect("valid test fixture")
            .body;
        let users = users(&body, format);
        assert_eq!(users.len(), 10, "{format:?}");
        for (index, user) in users.iter().enumerate() {
            assert!(text(user, format).contains(&format!("Question {} ", index + 1)));
            assert_eq!(
                user.to_string().contains("dmVyaWZpZWQtaW1hZ2Utc291cmNl"),
                index == 2 || index == 5,
                "{format:?} {index}"
            );
        }
    }
}

#[test]
fn retired_tool_remains_a_native_historical_exchange_on_each_wire() {
    for format in FORMATS {
        let mut history = request(format);
        history.tools.clear();
        let mapped =
            map_request(&history, &ResolvedContents::default()).expect("historical tool pairing");
        let body = &mapped.body;
        for key in ["tools", "toolConfig"] {
            if let Some(definitions) = body.get(key) {
                assert!(
                    !definitions.to_string().contains("lookup"),
                    "retired definition on {format:?}"
                );
            }
        }
        let native_key = match format {
            ApiFormat::OpenAiChatCompletions => "tool_calls",
            ApiFormat::OpenAiResponses => "function_call",
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => "tool_use",
            ApiFormat::GoogleGenerateContent => "functionCall",
            ApiFormat::GoogleInteractions => "function_call",
            ApiFormat::BedrockConverse => "toolUse",
            _ => unreachable!(),
        };
        if format == ApiFormat::GoogleGenerateContent {
            let calls = body["contents"]
                .as_array()
                .expect("valid test fixture")
                .iter()
                .flat_map(|m| m["parts"].as_array().expect("valid test fixture"))
                .filter(|part| part.get("functionCall").is_some())
                .collect::<Vec<_>>();
            assert_eq!(
                calls[0]["thoughtSignature"],
                "skip_thought_signature_validator"
            );
        }
        let wire = body.to_string();
        assert!(
            wire.contains(native_key) && wire.contains("lookup"),
            "{format:?}"
        );
    }
}
