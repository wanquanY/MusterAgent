/// Returns the model-visible Markdown body of one Skill instruction source.
///
/// Skill frontmatter is control-plane metadata. It can contain capability
/// requirements and package provenance, neither of which is an executable
/// Tool identity. Keeping that metadata out of model context prevents a model
/// from passing capability keys to Tool activation, whose public contract
/// accepts only exact `tool_key` values returned by Tool discovery.
pub fn model_instruction_body(source: &str) -> Result<&str, SkillInstructionProjectionError> {
    let Some(rest) = source
        .strip_prefix("---\n")
        .or_else(|| source.strip_prefix("---\r\n"))
    else {
        // Historical built-in material predating portable Skill packages did
        // not have frontmatter. It remains valid model instruction content.
        return Ok(source);
    };

    let body = rest
        .split_once("\n---\n")
        .map(|(_, body)| body)
        .or_else(|| rest.split_once("\r\n---\r\n").map(|(_, body)| body))
        .ok_or(SkillInstructionProjectionError::MalformedFrontmatter)?;
    let body = body.trim_start_matches(['\r', '\n']);
    if body.trim().is_empty() {
        return Err(SkillInstructionProjectionError::EmptyBody);
    }
    Ok(body)
}

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
pub enum SkillInstructionProjectionError {
    #[error("Skill instruction frontmatter is malformed")]
    MalformedFrontmatter,
    #[error("Skill instruction Markdown body is empty")]
    EmptyBody,
}

#[cfg(test)]
mod tests {
    use super::{SkillInstructionProjectionError, model_instruction_body};

    #[test]
    fn projects_only_markdown_body_from_lf_or_crlf_frontmatter() {
        for source in [
            "---\nname: research\nrequired_tools:\n  - web.fetch@2\n---\n# Research\nUse Tool discovery.\n",
            "---\r\nname: research\r\nrequired_capabilities:\r\n  - web.fetch@2\r\n---\r\n# Research\r\nUse Tool discovery.\r\n",
        ] {
            let projected = model_instruction_body(source).expect("Skill body");
            assert!(projected.starts_with("# Research"));
            assert!(!projected.contains("required_tools"));
            assert!(!projected.contains("required_capabilities"));
            assert!(!projected.contains("web.fetch@2"));
        }
    }

    #[test]
    fn preserves_historical_plain_instructions_and_rejects_broken_packages() {
        assert_eq!(
            model_instruction_body("Advance the current goal.").expect("legacy instructions"),
            "Advance the current goal."
        );
        assert_eq!(
            model_instruction_body("---\nname: broken\n# Missing delimiter"),
            Err(SkillInstructionProjectionError::MalformedFrontmatter)
        );
        assert_eq!(
            model_instruction_body("---\nname: empty\n---\n\n"),
            Err(SkillInstructionProjectionError::EmptyBody)
        );
    }
}
