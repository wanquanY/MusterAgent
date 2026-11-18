use super::*;

#[test]
fn canonical_input_retains_the_original_screen_action_and_rejects_ambiguous_encoding() {
    let value =
        json!({"tool":"click","arguments":{"scope":"desktop","x":-10.5,"y":125,"button":"left"}});
    let input = ScreenInput::from_json(&value).expect("screen coordinates");
    assert!(!input.is_observation());
    let bytes = input.encode().expect("canonical");
    assert_eq!(
        ScreenInput::decode(&bytes).expect("decode").to_json(),
        value
    );
    let mut padded = bytes.clone();
    padded.push(b' ');
    assert!(ScreenInput::decode(&padded).is_err());
    assert!(
        ScreenInput::decode(br#"{"arguments":{},"tool":"list_apps","tool":"get_desktop_state"}"#)
            .is_err()
    );
}

#[test]
fn screen_contract_excludes_native_paths_private_sessions_and_arbitrary_provider_calls() {
    for value in [
        json!({"tool":"get_desktop_state","arguments":{"screenshot_out_file":"/tmp/leak.png"}}),
        json!({"tool":"click","arguments":{"debug_image_out":"/tmp/leak.png"}}),
        json!({"tool":"click","arguments":{"session":"private-conversation"}}),
        json!({"tool":"move_cursor","arguments":{"cursor_id":"other-run","x":1,"y":2}}),
        json!({"tool":"launch_app","arguments":{"urls":["file:///private/path"]}}),
        json!({"tool":"replay_trajectory","arguments":{"dir":"/private/path"}}),
        json!({"tool":"start_recording","arguments":{"output_dir":"/tmp/leak"}}),
        json!({"tool":"end_session","arguments":{"session":"other-run"}}),
        json!({"tool":"unknown","arguments":{}}),
        json!({"tool":"get_window_state","arguments":{"pid":10,"window_id":11,"capture_mode":"ax"}}),
    ] {
        assert!(ScreenInput::from_json(&value).is_err(), "rejected {value}");
    }
}

#[test]
fn bounds_and_observation_classification_apply_before_any_physical_adapter() {
    ScreenInput::from_json(&json!({"tool":"press_key","arguments":{"pid":10,"key":"ENTER"}}))
        .expect("ordinary key does not inherit the AX action enum");
    ScreenInput::from_json(
        &json!({"tool":"click","arguments":{"element_token":"original-snapshot-token"}}),
    )
    .expect("opaque snapshot token");
    let image = ScreenInput::from_json(&json!({"tool":"get_window_state","arguments":{"pid":10,"window_id":11,"max_elements":2000}})).expect("bounded observation");
    assert!(image.is_observation());
    for arguments in [
        json!({"pid":0,"text":"test"}),
        json!({"pid":1,"text":"\u{0}"}),
        json!({"pid":1,"text":"a".repeat(16385)}),
        json!({"pid":1,"text":"test","delay_ms":5001}),
    ] {
        assert!(
            ScreenInput::from_json(&json!({"tool":"type_text","arguments":arguments})).is_err()
        );
    }
    assert!(ScreenInput::from_json(&json!({"tool":"get_window_state","arguments":{"pid":10,"window_id":11,"max_elements":10001}})).is_err());
}
