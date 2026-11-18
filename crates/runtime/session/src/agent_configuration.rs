//! Independent profile, instructions and permission owners use separate CAS revisions.
use crate::{DialogueStoreError, OwnedAgentApplicationError};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_definition::owned_agent::{OwnedAgentConfiguration, OwnedAgentProfile};
use musteragent_execution_context::{DialogueInputOrigin, DialogueInputSurface};
use musteragent_identity::{Actor, AgentId, CommandId, UtcTimestamp};
use musteragent_policy::PermissionMode;
use std::num::NonZeroU64;
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AgentConfigurationChange {
    /// Renames the current Agent without reconstructing other profile fields.
    DisplayName {
        display_name: String,
    },
    /// Changes only user-editable text; avatar remains owned by the profile transaction.
    ProfileDetails {
        display_name: String,
        description: String,
    },
    Profile {
        display_name: String,
        description: String,
        avatar_ref: Option<ContentRef>,
    },
    /// Changes only the avatar; text identity is retained inside the profile CAS.
    Avatar {
        content_ref: Option<ContentRef>,
    },
    Instructions {
        content_ref: Option<ContentRef>,
    },
    Permission {
        mode: PermissionMode,
    },
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfigureOwnedAgent {
    pub actor: Actor,
    pub command_id: CommandId,
    pub agent_id: AgentId,
    pub expected_revision: NonZeroU64,
    pub change: AgentConfigurationChange,
    pub origin: DialogueInputOrigin,
    pub changed_at: UtcTimestamp,
}
impl ConfigureOwnedAgent {
    pub fn validate(&self) -> Result<(), OwnedAgentApplicationError> {
        if self.actor.principal_kind != musteragent_identity::PrincipalKind::User {
            return Err(OwnedAgentApplicationError::Forbidden);
        }
        self.origin
            .validate()
            .map_err(|_| OwnedAgentApplicationError::Forbidden)?;
        if matches!(self.change, AgentConfigurationChange::Permission { .. })
            && self.origin.surface != DialogueInputSurface::Desktop
        {
            return Err(OwnedAgentApplicationError::Forbidden);
        }
        if let AgentConfigurationChange::Avatar {
            content_ref: Some(reference),
        }
        | AgentConfigurationChange::Profile {
            avatar_ref: Some(reference),
            ..
        } = &self.change
            && (reference.byte_length == 0
                || reference.byte_length > 256 * 1024
                || !matches!(reference.media_type.as_str(), "image/png" | "image/jpeg"))
        {
            return Err(OwnedAgentApplicationError::Forbidden);
        }
        let base = OwnedAgentConfiguration::initial(
            self.agent_id,
            "validation".into(),
            "".into(),
            None,
            None,
        )?;
        self.apply_at_revision(&base, NonZeroU64::MIN)?;
        Ok(())
    }
    pub fn content(&self) -> Option<&ContentRef> {
        match &self.change {
            AgentConfigurationChange::Profile { avatar_ref, .. } => avatar_ref.as_ref(),
            AgentConfigurationChange::Instructions { content_ref }
            | AgentConfigurationChange::Avatar { content_ref } => content_ref.as_ref(),
            AgentConfigurationChange::Permission { .. }
            | AgentConfigurationChange::DisplayName { .. }
            | AgentConfigurationChange::ProfileDetails { .. } => None,
        }
    }
    fn apply_at_revision(
        &self,
        current: &OwnedAgentConfiguration,
        expected: NonZeroU64,
    ) -> Result<OwnedAgentConfiguration, OwnedAgentApplicationError> {
        Ok(match &self.change {
            AgentConfigurationChange::DisplayName { display_name } => current.update_profile(
                expected,
                OwnedAgentProfile {
                    revision: expected,
                    display_name: display_name.clone(),
                    description: current.profile.description.clone(),
                    avatar_ref: current.profile.avatar_ref.clone(),
                },
            )?,
            AgentConfigurationChange::ProfileDetails {
                display_name,
                description,
            } => current.update_profile(
                expected,
                OwnedAgentProfile {
                    revision: expected,
                    display_name: display_name.clone(),
                    description: description.clone(),
                    avatar_ref: current.profile.avatar_ref.clone(),
                },
            )?,
            AgentConfigurationChange::Profile {
                display_name,
                description,
                avatar_ref,
            } => current.update_profile(
                expected,
                OwnedAgentProfile {
                    revision: expected,
                    display_name: display_name.clone(),
                    description: description.clone(),
                    avatar_ref: avatar_ref.clone(),
                },
            )?,
            AgentConfigurationChange::Avatar { content_ref } => current.update_profile(
                expected,
                OwnedAgentProfile {
                    revision: expected,
                    display_name: current.profile.display_name.clone(),
                    description: current.profile.description.clone(),
                    avatar_ref: content_ref.clone(),
                },
            )?,
            AgentConfigurationChange::Instructions { content_ref } => {
                current.update_instructions(expected, content_ref.clone())?
            }
            AgentConfigurationChange::Permission { mode } => {
                current.update_permission(expected, *mode)?
            }
        })
    }
    pub fn apply(
        &self,
        current: &OwnedAgentConfiguration,
    ) -> Result<OwnedAgentConfiguration, DialogueStoreError> {
        if current.agent_id != self.agent_id {
            return Err(DialogueStoreError::Unavailable);
        }
        self.apply_at_revision(current, self.expected_revision)
            .map_err(|error| match error {
                OwnedAgentApplicationError::Domain(
                    musteragent_definition::owned_agent::OwnedAgentError::RevisionConflict,
                ) => DialogueStoreError::RevisionConflict,
                _ => DialogueStoreError::Corrupt,
            })
    }
    pub fn fingerprint(&self) -> Sha256 {
        let mut bytes = b"musteragent.agent-configuration.v1\0".to_vec();
        let mut fields = vec![
            self.actor.tenant_id.to_string(),
            self.actor.principal_id.to_string(),
            self.agent_id.to_string(),
            self.expected_revision.to_string(),
            self.origin.surface.as_str().to_owned(),
            self.origin
                .installation_id
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ];
        match &self.change {
            AgentConfigurationChange::DisplayName { display_name } => {
                fields.extend(["display-name".into(), display_name.clone()])
            }
            AgentConfigurationChange::ProfileDetails {
                display_name,
                description,
            } => fields.extend([
                "profile-details".into(),
                display_name.clone(),
                description.clone(),
            ]),
            AgentConfigurationChange::Profile {
                display_name,
                description,
                ..
            } => fields.extend(["profile".into(), display_name.clone(), description.clone()]),
            AgentConfigurationChange::Avatar { .. } => fields.push("avatar".into()),
            AgentConfigurationChange::Instructions { .. } => fields.push("instructions".into()),
            AgentConfigurationChange::Permission { mode } => {
                fields.extend(["permission".into(), (*mode as u8).to_string()])
            }
        }
        match self.content() {
            Some(c) => fields.extend([
                "content".into(),
                c.content_id.to_string(),
                c.media_type.as_str().to_owned(),
                c.byte_length.to_string(),
                c.sha256.to_string(),
            ]),
            None => fields.push("no-content".into()),
        }
        for f in fields {
            bytes.extend_from_slice(&(f.len() as u64).to_be_bytes());
            bytes.extend_from_slice(f.as_bytes())
        }
        Sha256::digest(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::MediaType;
    use musteragent_identity::{ContentId, PrincipalId, PrincipalKind, TenantId};
    #[test]
    fn avatar_change_preserves_text_and_other_components_and_rejects_stale_revision() {
        let id = AgentId::generate();
        let current = OwnedAgentConfiguration::initial(
            id,
            "Muster".into(),
            "Responsibility".into(),
            None,
            None,
        )
        .expect("valid test fixture");
        let mut command = ConfigureOwnedAgent {
            actor: Actor {
                tenant_id: TenantId::generate(),
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            command_id: CommandId::generate(),
            agent_id: id,
            expected_revision: NonZeroU64::MIN,
            change: AgentConfigurationChange::Avatar {
                content_ref: Some(ContentRef {
                    content_id: ContentId::generate(),
                    media_type: MediaType::new("image/png").expect("valid test fixture"),
                    byte_length: 4,
                    sha256: Sha256::digest("data"),
                }),
            },
            origin: DialogueInputOrigin {
                surface: DialogueInputSurface::Web,
                installation_id: None,
            },
            changed_at: UtcTimestamp::from_unix_millis(1),
        };
        command.validate().expect("valid test fixture");
        let changed = command.apply(&current).expect("valid test fixture");
        assert_eq!(changed.profile.display_name, current.profile.display_name);
        assert_eq!(changed.profile.description, current.profile.description);
        assert_eq!(changed.instructions_revision, current.instructions_revision);
        assert_eq!(changed.permission_revision, current.permission_revision);
        assert!(changed.profile.avatar_ref.is_some());
        assert!(command.apply(&changed).is_err());
        let original_fingerprint = command.fingerprint();
        command.change = AgentConfigurationChange::Avatar { content_ref: None };
        assert_ne!(original_fingerprint, command.fingerprint());
        command.expected_revision = changed.profile.revision;
        let reset = command.apply(&changed).expect("valid test fixture");
        assert!(reset.profile.avatar_ref.is_none());
        assert_eq!(reset.profile.display_name, "Muster");
    }
    #[test]
    fn profile_details_preserve_avatar_in_transaction_and_keep_stable_fingerprint() {
        let id = AgentId::generate();
        let avatar = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("image/png").expect("valid test fixture"),
            byte_length: 4,
            sha256: Sha256::digest("data"),
        };
        let current = OwnedAgentConfiguration::initial(
            id,
            "Muster".into(),
            "".into(),
            Some(avatar.clone()),
            None,
        )
        .expect("valid test fixture");
        let command = ConfigureOwnedAgent {
            actor: Actor {
                tenant_id: TenantId::generate(),
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            command_id: CommandId::generate(),
            agent_id: id,
            expected_revision: NonZeroU64::MIN,
            change: AgentConfigurationChange::ProfileDetails {
                display_name: "青禾".into(),
                description: "研究伙伴".into(),
            },
            origin: DialogueInputOrigin {
                surface: DialogueInputSurface::Web,
                installation_id: None,
            },
            changed_at: UtcTimestamp::from_unix_millis(1),
        };
        command.validate().expect("valid test fixture");
        let before = command.fingerprint();
        let updated = command.apply(&current).expect("valid test fixture");
        assert_eq!(updated.profile.avatar_ref, Some(avatar));
        assert_eq!(updated.profile.display_name, "青禾");
        assert_eq!(before, command.fingerprint());
        assert_eq!(updated.instructions_revision, current.instructions_revision);
        assert!(command.apply(&updated).is_err());
        let mut portrait = updated
            .profile
            .avatar_ref
            .clone()
            .expect("valid test fixture");
        portrait.content_id = ContentId::generate();
        let mut joint = command;
        joint.change = AgentConfigurationChange::Profile {
            display_name: "星芽".into(),
            description: "一起完成事情".into(),
            avatar_ref: Some(portrait.clone()),
        };
        joint.validate().expect("valid test fixture");
        let saved = joint.apply(&current).expect("valid test fixture");
        assert_eq!(saved.profile.display_name, "星芽");
        assert_eq!(saved.profile.avatar_ref, Some(portrait.clone()));
        assert_eq!(
            saved.profile.revision.get(),
            current.profile.revision.get() + 1
        );
        assert!(joint.apply(&saved).is_err());
        portrait.media_type = MediaType::new("text/plain").expect("valid test fixture");
        joint.change = AgentConfigurationChange::Profile {
            display_name: "星芽".into(),
            description: "".into(),
            avatar_ref: Some(portrait),
        };
        assert!(joint.validate().is_err());
    }
    #[test]
    fn rename_uses_profile_cas_without_overwriting_avatar_description_or_other_settings() {
        let avatar = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("image/png").expect("valid test fixture"),
            byte_length: 1,
            sha256: Sha256::digest(b"a"),
        };
        let current = OwnedAgentConfiguration::initial(
            AgentId::generate(),
            "Muster".into(),
            "Research".into(),
            Some(avatar.clone()),
            None,
        )
        .expect("valid test fixture");
        let command = ConfigureOwnedAgent {
            actor: Actor {
                tenant_id: TenantId::generate(),
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            command_id: CommandId::generate(),
            agent_id: current.agent_id,
            expected_revision: current.profile.revision,
            change: AgentConfigurationChange::DisplayName {
                display_name: "Remy".into(),
            },
            origin: DialogueInputOrigin {
                surface: DialogueInputSurface::Web,
                installation_id: None,
            },
            changed_at: UtcTimestamp::from_unix_millis(100),
        };
        command.validate().expect("valid test fixture");
        let fingerprint = command.fingerprint();
        let changed = command.apply(&current).expect("valid test fixture");
        assert_eq!(changed.profile.display_name, "Remy");
        assert_eq!(changed.profile.description, "Research");
        assert_eq!(changed.profile.avatar_ref, Some(avatar));
        assert_eq!(changed.instructions_ref, current.instructions_ref);
        assert_eq!(changed.permission_revision, current.permission_revision);
        assert!(matches!(
            command.apply(&changed),
            Err(DialogueStoreError::RevisionConflict)
        ));
        assert_eq!(command.fingerprint(), fingerprint);
    }
}
