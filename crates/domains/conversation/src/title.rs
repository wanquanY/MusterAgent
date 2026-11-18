use musteragent_runtime_contract::{DisplayText, InputContent};

/// Product title policy for a newly admitted direct Conversation.
///
/// The policy is deterministic and deliberately model-free: the first user
/// turn becomes the canonical title in the same transaction that accepts the
/// message. Whitespace is collapsed so multiline prompts remain useful in the
/// compact desktop catalog.
pub const FIRST_USER_MESSAGE_TITLE_MAX_CHARACTERS: usize = 32;

#[must_use]
pub fn first_user_message_title(content: &InputContent) -> Option<DisplayText> {
    let InputContent::InlineUtf8(content) = content else {
        return None;
    };
    first_user_message_title_from_text(content.as_str())
}

/// Derives the same canonical title from authorized UTF-8 content loaded by a
/// durable migration or repair workflow.
#[must_use]
pub fn first_user_message_title_from_text(content: &str) -> Option<DisplayText> {
    let normalized = content.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return None;
    }
    let title = normalized
        .chars()
        .take(FIRST_USER_MESSAGE_TITLE_MAX_CHARACTERS)
        .collect::<String>();
    DisplayText::new(title).ok()
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::ContentId;
    use musteragent_runtime_contract::{InlineInput, InputContent};

    use super::{FIRST_USER_MESSAGE_TITLE_MAX_CHARACTERS, first_user_message_title};

    #[test]
    fn derives_a_compact_title_from_the_first_user_text() {
        let input = InputContent::InlineUtf8(
            InlineInput::new("  帮我分析\n\n这个项目的架构  ").expect("input"),
        );

        assert_eq!(
            first_user_message_title(&input).expect("title").as_str(),
            "帮我分析 这个项目的架构"
        );
    }

    #[test]
    fn truncates_on_utf8_character_boundaries() {
        let source = "会".repeat(FIRST_USER_MESSAGE_TITLE_MAX_CHARACTERS + 8);
        let input = InputContent::InlineUtf8(InlineInput::new(source).expect("input"));

        assert_eq!(
            first_user_message_title(&input)
                .expect("title")
                .as_str()
                .chars()
                .count(),
            FIRST_USER_MESSAGE_TITLE_MAX_CHARACTERS
        );
    }

    #[test]
    fn does_not_guess_a_title_from_an_opaque_content_reference() {
        let input = InputContent::ContentRef(ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
            byte_length: 1,
            sha256: Sha256::digest(b"x"),
        });

        assert!(first_user_message_title(&input).is_none());
    }
}
