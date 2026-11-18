use super::{StructuredInteractionError, StructuredInteractionKind, StructuredInteractionRequest};
use serde_json::{Value, json};

fn parse(value: &Value) -> Result<StructuredInteractionRequest, StructuredInteractionError> {
    StructuredInteractionRequest::parse_tool_arguments(
        "interaction.question@1",
        &serde_json::to_vec(value).unwrap(),
    )
}

#[test]
fn question_normalization_and_response_contract_match_the_existing_product_language() {
    for (request, valid_response, invalid_response) in [
        (
            json!({"kind":"text_input","question":"两份工作的职位和地点分别是什么？"}),
            json!({"text":"研究，上海；开发，北京"}),
            json!({"confirmed":true}),
        ),
        (
            json!({"kind":"single_select","question":"哪项优先？","choices":["成长","收入"],"allow_other":false}),
            json!({"value":"成长"}),
            json!({"value":"未提供的选项"}),
        ),
        (
            json!({"kind":"multi_select","question":"关注哪些条件？","choices":["成长","收入"],"min_selections":2,"max_selections":2,"allow_other":true}),
            json!({"values":["成长","地点"]}),
            json!({"values":["成长","成长"]}),
        ),
    ] {
        let bytes = serde_json::to_vec(&request).unwrap();
        let question = parse(&request).unwrap();
        let previous = StructuredInteractionRequest::parse_v2(&bytes).unwrap();
        assert_ne!(question.kind(), StructuredInteractionKind::Confirm);
        assert_eq!(
            question.canonical_prompt_bytes().unwrap(),
            previous.canonical_prompt_bytes().unwrap()
        );
        assert_eq!(
            question.response_schema().unwrap(),
            previous.response_schema().unwrap()
        );
        question.validate_response(&valid_response).unwrap();
        assert!(question.validate_response(&invalid_response).is_err());
        let persisted =
            StructuredInteractionRequest::parse(&question.canonical_prompt_bytes().unwrap())
                .unwrap();
        persisted.validate_response(&valid_response).unwrap();
    }
}

#[test]
fn question_rejects_confirm_invalid_shape_and_unachievable_selection_constraints() {
    for request in [
        json!({"kind":"confirm","question":"请确认收到？"}),
        json!({"kind":"single_select","question":"?","choices":["a"]}),
        json!({"kind":"single_select","question":"?","choices":["a","a"]}),
        json!({"kind":"single_select","question":"?","choices":["a","b"],"min_selections":1}),
        json!({"kind":"text_input","question":"?","choices":["a","b"]}),
        json!({"kind":"text_input","question":"?","decision_required":true}),
        json!({"kind":"text_input","title":"legacy payload"}),
        json!({"kind":"multi_select","question":"?","choices":["a","b"],"min_selections":0}),
        json!({"kind":"multi_select","question":"?","choices":["a","b"],"min_selections":3,"allow_other":false}),
        json!({"kind":"multi_select","question":"?","choices":["a","b"],"max_selections":4}),
    ] {
        assert!(
            matches!(
                parse(&request),
                Err(StructuredInteractionError::InvalidRequest)
            ),
            "{request}"
        );
    }
}

#[test]
fn question_bounds_are_unicode_character_bounds_and_preserve_optional_answers() {
    let mut request = json!({"kind":"text_input","question":"题".repeat(200),"description":"文".repeat(2000),"placeholder":"答".repeat(500),"required":false});
    let question = parse(&request).unwrap();
    question.validate_response(&json!({"text":""})).unwrap();
    for (field, length) in [
        ("question", 201),
        ("description", 2001),
        ("placeholder", 501),
    ] {
        let mut excessive = request.clone();
        excessive[field] = json!("文".repeat(length));
        assert!(parse(&excessive).is_err(), "{field}");
    }
    request["required"] = json!(true);
    assert!(
        parse(&request)
            .unwrap()
            .validate_response(&json!({"text":""}))
            .is_err()
    );
    let choices = (0..8)
        .map(|i| format!("{i}{}", "选".repeat(199)))
        .collect::<Vec<_>>();
    let valid = json!({"kind":"multi_select","question":"请选择","choices":choices,"min_selections":9,"max_selections":9});
    assert!(parse(&valid).is_ok());
    let mut too_many = valid;
    too_many["choices"]
        .as_array_mut()
        .unwrap()
        .push(json!("额外"));
    assert!(parse(&too_many).is_err());
}

#[test]
fn exact_version_dispatch_preserves_legacy_confirm_without_unknown_version_fallback() {
    for (key, request) in [
        (
            "interaction.request@1",
            json!({"kind":"confirm","title":"Continue?"}),
        ),
        (
            "interaction.request@2",
            json!({"kind":"confirm","question":"Continue?"}),
        ),
    ] {
        let parsed = StructuredInteractionRequest::parse_tool_arguments(
            key,
            &serde_json::to_vec(&request).unwrap(),
        )
        .unwrap();
        assert_eq!(parsed.kind(), StructuredInteractionKind::Confirm);
        assert_eq!(
            parsed.response_schema().unwrap().schema_key.as_str(),
            "interaction.confirm.response@1"
        );
    }
    for key in [
        "interaction.request@3",
        "interaction.question@3",
        "interaction.other@1",
    ] {
        assert!(matches!(
            StructuredInteractionRequest::parse_tool_arguments(
                key,
                br#"{"kind":"text_input","title":"legacy"}"#
            ),
            Err(StructuredInteractionError::ContractMismatch)
        ));
    }
}
