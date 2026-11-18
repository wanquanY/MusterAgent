//! Resource selection errors belong to the package interface, not generic discovery.
use musteragent_discovery::AuthorizedSkillDescriptor;
use musteragent_loop::LoopPortError;
use musteragent_runtime_capability::CapabilityInvocation;
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_skill::{
    SkillResourceReadError, SkillResourceReadOutput, read_pinned_skill_resources,
};

use super::{SkillDiscoveryCapabilityHandler, failed_with_message, port};
use crate::DispatchedCapabilityOutcome;

impl SkillDiscoveryCapabilityHandler {
    pub(super) async fn read_resource(
        &self,
        invocation: CapabilityInvocation,
        descriptor: &AuthorizedSkillDescriptor,
        path: Option<&str>,
        skill_name: Option<&str>,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        match read_pinned_skill_resources(
            self.content.as_ref(),
            invocation.tenant_id,
            descriptor,
            path,
        )
        .await
        {
            Ok(output) => match skill_name {
                Some(skill_name) => {
                    self.success_json(invocation, &NamedResourceOutput { skill_name, output })
                        .await
                }
                None => self.success_json(invocation, &output).await,
            },
            Err(error) => {
                let failure = resource_error(&error);
                tracing::warn!(
                    tenant_id = %invocation.tenant_id,
                    run_id = %invocation.run_id,
                    invocation_id = %invocation.invocation_id,
                    skill_ref = descriptor.skill_ref.as_str(),
                    code = failure.code.as_str(),
                    retryable = failure.retryable,
                    reason = %error,
                    "skill resource read failed"
                );
                if !matches!(
                    error,
                    SkillResourceReadError::NotFound | SkillResourceReadError::NotText
                ) {
                    return Err(failure);
                }
                // The authorized, immutable descriptor is the only source for
                // recovery paths. Never consult another skill or the host filesystem.
                let index = read_pinned_skill_resources(
                    self.content.as_ref(),
                    invocation.tenant_id,
                    descriptor,
                    None,
                )
                .await
                .map_err(|error| resource_error(&error))?;
                let message = selection_message(&error, path, &index, skill_name);
                Ok(DispatchedCapabilityOutcome::Provider(failed_with_message(
                    ErrorCode::InvalidCommand,
                    &message,
                )?))
            }
        }
    }
}

#[derive(serde::Serialize)]
struct NamedResourceOutput<'a> {
    skill_name: &'a str,
    #[serde(flatten)]
    output: SkillResourceReadOutput,
}

fn resource_error(error: &SkillResourceReadError) -> LoopPortError {
    match error {
        SkillResourceReadError::NotFound | SkillResourceReadError::NotText => {
            port(ErrorCode::InvalidCommand, false)
        }
        SkillResourceReadError::Integrity => port(ErrorCode::ContentIntegrityFailed, false),
        SkillResourceReadError::Content(error) => port(error.runtime_code(), error.retryable()),
    }
}

fn selection_message(
    error: &SkillResourceReadError,
    path: Option<&str>,
    index: &SkillResourceReadOutput,
    skill_name: Option<&str>,
) -> String {
    let reason = if matches!(error, SkillResourceReadError::NotText) {
        "The requested resource is not UTF-8 text within the 256 KiB read limit."
    } else if path.is_some_and(|path| path.ends_with('/')) {
        "A directory is not a readable skill file."
    } else {
        "The requested path is invalid or is not a file in this skill package."
    };
    let request = match skill_name {
        Some(name) => serde_json::json!({"skill_name":name}),
        None => serde_json::json!({"skill_ref":index.skill_ref}),
    };
    let mut message = format!(
        "{reason} To list exact file paths, call skills.resource.read with {request} and omit path. To read, set path to an exact file from that list."
    );
    // SafeDisplayText is bounded in bytes. Include useful real examples while
    // retaining the complete index request even for large/private packages.
    for file in index
        .resources
        .iter()
        .filter(|file| file.path != "SKILL.md")
    {
        let example = format!(" File: {}.", serde_json::json!(file.path));
        if message.len() + example.len() <= 1_024 {
            message.push_str(&example);
        }
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resource_recovery_uses_the_selected_interface_without_changing_paths() {
        let index = SkillResourceReadOutput {
            skill_ref: "research@8".into(),
            skill_version_id: "internal-id".into(),
            resources: vec![],
            path: None,
            text: None,
        };
        let current = selection_message(
            &SkillResourceReadError::NotFound,
            Some("references/"),
            &index,
            Some("research"),
        );
        assert!(current.contains("\"skill_name\":\"research\""));
        assert!(!current.contains("skill_ref"));
        assert!(!current.contains('@'));
        let legacy = selection_message(&SkillResourceReadError::NotFound, None, &index, None);
        assert!(legacy.contains("\"skill_ref\":\"research@8\""));
        assert!(legacy.contains("omit path"));
    }
}
