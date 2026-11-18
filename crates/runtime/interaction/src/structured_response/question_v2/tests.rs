use super::*;
use serde_json::{Value, json};

fn decode(value: Value) -> Result<StructuredInteractionRequest, StructuredInteractionError> {
    StructuredInteractionRequest::parse_tool_arguments(
        "interaction.question@2",
        &serde_json::to_vec(&value).unwrap(),
    )
}

#[test]
fn minimal_inputs_and_omitted_descriptions_share_the_original_canonical_defaults() {
    for (legacy, candidate) in [
        (
            json!({"kind":"text_input","question":"何时开始？"}),
            json!({"kind":"text_input","question":"何时开始？"}),
        ),
        (
            json!({"kind":"single_select","question":"哪个城市？","choices":["上海","北京"]}),
            json!({"kind":"single_select","question":"哪个城市？","choices":[{"label":"上海"},{"label":"北京"}]}),
        ),
    ] {
        let old =
            StructuredInteractionRequest::parse_question_v1(&serde_json::to_vec(&legacy).unwrap())
                .unwrap();
        let current = decode(candidate.clone()).unwrap();
        assert_eq!(
            old.canonical_prompt_bytes().unwrap(),
            current.canonical_prompt_bytes().unwrap()
        );
        assert_eq!(
            old.response_schema().unwrap(),
            current.response_schema().unwrap()
        );
        let mut explicit_empty = candidate;
        explicit_empty["description"] = json!("");
        if let Some(choices) = explicit_empty
            .get_mut("choices")
            .and_then(Value::as_array_mut)
        {
            for choice in choices {
                choice["description"] = json!("");
            }
        }
        assert_eq!(
            current.canonical_prompt_bytes().unwrap(),
            decode(explicit_empty)
                .unwrap()
                .canonical_prompt_bytes()
                .unwrap()
        );
    }
}

#[test]
fn choice_explanations_round_trip_in_original_prompt_and_answer_protocol() {
    let question = decode(json!({
        "kind":"single_select", "question":"选择分享主题", "allow_other":false,
        "description":"面向产品同事的 Personal AI 线上分享，选择后起草通知。",
        "choices":[
            {"label":"可靠性","description":"讨论断网、回执未知与恢复时的体验。"},
            {"label":"常驻伙伴与临时助手","description":"比较长期上下文与临时分工，说明适用场景。"}
        ]
    }))
    .unwrap();
    let bytes = question.canonical_prompt_bytes().unwrap();
    let prompt: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(prompt["options"][1]["value"], "常驻伙伴与临时助手");
    assert_eq!(prompt["options"][1]["label"], "常驻伙伴与临时助手");
    assert_eq!(
        prompt["options"][1]["description"],
        "比较长期上下文与临时分工，说明适用场景。"
    );
    let persisted = StructuredInteractionRequest::parse(&bytes).unwrap();
    persisted
        .validate_response(&json!({"value":"常驻伙伴与临时助手"}))
        .unwrap();
    for invalid in [
        json!({"value":"not_an_option"}),
        json!({"confirmed":true}),
        json!({"value":"可靠性","description":"injected"}),
    ] {
        assert!(persisted.validate_response(&invalid).is_err());
    }
    assert_eq!(
        persisted.response_schema().unwrap().schema_key.as_str(),
        "interaction.single-select.response@1"
    );
}

#[test]
fn selection_card_explanations_do_not_change_multi_answer_cardinality_or_custom_choice() {
    let question = decode(json!({
        "kind":"multi_select","question":"需要哪些内容？","min_selections":2,"max_selections":2,
        "choices":[{"label":"可靠性","description":"中断与恢复"},{"label":"体验","description":"用户如何发现待办"}]
    })).unwrap();
    question
        .validate_response(&json!({"values":["可靠性","其他想法"]}))
        .unwrap();
    for invalid in [
        json!({"values":["可靠性"]}),
        json!({"values":["可靠性","可靠性"]}),
        json!({"values":["自定义一","自定义二"]}),
    ] {
        assert!(question.validate_response(&invalid).is_err());
    }
}

#[test]
fn exact_v2_shape_rejects_confirm_legacy_choices_unknown_fields_and_duplicate_labels() {
    for value in [
        json!({"kind":"confirm","question":"Ready?"}),
        json!({"kind":"single_select","question":"?","choices":["A","B"]}),
        json!({"kind":"single_select","question":"?","choices":[{"label":"A"}]}),
        json!({"kind":"single_select","question":"?","choices":[{"label":"A","description":"one"},{"label":"A","description":"two"}]}),
        json!({"kind":"single_select","question":"?","choices":[{"label":"A","value":"custom"},{"label":"B"}]}),
        json!({"kind":"single_select","question":"?","choices":[{"label":"A","description":null},{"label":"B"}]}),
        json!({"kind":"text_input","question":"?","choices":[{"label":"A"},{"label":"B"}]}),
        json!({"kind":"text_input","question":"?","necessary":true}),
        json!({"kind":"single_select","question":"?","choices":[{"label":"A"},{"label":"B"}],"min_selections":1}),
        json!({"kind":"multi_select","question":"?","choices":[{"label":"A"},{"label":"B"}],"min_selections":3,"allow_other":false}),
    ] {
        assert!(decode(value.clone()).is_err(), "{value}");
    }
    for bytes in [
        br#"{"kind":"text_input","question":"one","question":"two"}"#.as_slice(),
        br#"{"kind":"single_select","question":"?","choices":[{"label":"A","label":"C"},{"label":"B"}]}"#.as_slice(),
        br#"{"kind":"single_select","question":"?","choices":[{"label":"A","description":"one","description":"two"},{"label":"B"}]}"#.as_slice(),
    ] { assert!(parse(bytes).is_err()); }
}

#[test]
fn unicode_bounds_apply_to_all_choice_content_without_loosening_original_limits() {
    let value = json!({"kind":"multi_select","question":"题".repeat(200),"description":"文".repeat(2000),"choices":(0..8).map(|i|json!({"label":format!("{i}{}","选".repeat(199)),"description":"述".repeat(500)})).collect::<Vec<_>>()});
    decode(value.clone()).unwrap();
    for field in ["question", "description"] {
        let mut excessive = value.clone();
        excessive[field] = json!(format!("{}额", excessive[field].as_str().unwrap()));
        assert!(decode(excessive).is_err());
    }
    for field in ["label", "description"] {
        let mut excessive = value.clone();
        excessive["choices"][0][field] = json!(format!(
            "{}额",
            excessive["choices"][0][field].as_str().unwrap()
        ));
        assert!(decode(excessive).is_err());
    }
    let mut excessive = value;
    excessive["choices"]
        .as_array_mut()
        .unwrap()
        .push(json!({"label":"第九项"}));
    assert!(decode(excessive).is_err());
}

#[test]
fn v1_is_not_reinterpreted_and_future_versions_do_not_fall_back() {
    let candidate =
        json!({"kind":"single_select","question":"?","choices":[{"label":"A"},{"label":"B"}]});
    let bytes = serde_json::to_vec(&candidate).unwrap();
    for old in ["interaction.question@1", "interaction.request@2"] {
        assert!(StructuredInteractionRequest::parse_tool_arguments(old, &bytes).is_err());
    }
    assert!(matches!(
        StructuredInteractionRequest::parse_tool_arguments("interaction.question@3", &bytes),
        Err(StructuredInteractionError::ContractMismatch)
    ));
    let optional_text =
        decode(json!({"kind":"text_input","question":"还有其他要求吗？","required":false}))
            .unwrap();
    optional_text
        .validate_response(&json!({"text":""}))
        .unwrap();
    assert!(
        optional_text
            .validate_response(&json!({"confirmed":true}))
            .is_err()
    );
}
