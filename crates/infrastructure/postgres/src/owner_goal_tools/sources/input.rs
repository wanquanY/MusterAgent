//! Strict model input; account, Agent, source facts and command identities are
//! deliberately absent. Original intent custody supplies those values.
use super::*;
use musteragent_goal::source_binding::GoalSourceBindingGuard;
use serde::Deserialize;
use std::{collections::BTreeSet, num::NonZeroU64};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Empty {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Goal {
    pub goal_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Resources {
    pub application_binding_id: String,
    pub application_revision: String,
    pub application_authority_epoch: String,
    pub capability_ref: String,
    pub capability_sha256: String,
    pub cursor: Option<String>,
}
impl Resources {
    pub fn selector(&self) -> Result<OwnerGoalSourceSelector, Error> {
        selector(
            &self.application_binding_id,
            &self.application_revision,
            &self.application_authority_epoch,
            &self.capability_ref,
            &self.capability_sha256,
        )
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Configuration {
    application_binding_id: String,
    application_revision: String,
    application_authority_epoch: String,
    capability_ref: String,
    capability_sha256: String,
    selection_json: String,
    event_kinds: Vec<String>,
}
impl Configuration {
    pub fn decode(self) -> Result<EventSubscriptionConfiguration, Error> {
        let selector = selector(
            &self.application_binding_id,
            &self.application_revision,
            &self.application_authority_epoch,
            &self.capability_ref,
            &self.capability_sha256,
        )?;
        let count = self.event_kinds.len();
        let event_kinds = self
            .event_kinds
            .into_iter()
            .map(|v| VersionedKey::new(v).map_err(|_| Error::InvalidArguments))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if count != event_kinds.len() {
            return Err(Error::InvalidArguments);
        }
        let result = EventSubscriptionConfiguration {
            application_binding_id: selector.application_binding_id,
            application_guard: selector.application_guard,
            capability_ref: selector.capability_ref,
            capability_digest: selector.capability_digest,
            selection: EventSourceSelection::from_json(self.selection_json.as_bytes())
                .map_err(source_error)?,
            event_kinds,
        };
        result.validate().map_err(source_error)?;
        Ok(result)
    }
}
// Internally tagged variants deny unknown fields without serde(flatten), so an
// actor/producer field or mixed action cannot be silently ignored.
#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Configure {
    Prepare {
        goal_id: String,
        expected_goal_revision: String,
        action_key: String,
        configuration: Configuration,
        required: bool,
    },
    Change {
        goal_id: String,
        expected_goal_revision: String,
        action_key: String,
        subscription_id: String,
        expected_subscription_revision: String,
        expected_activation_epoch: String,
        desired: Desired,
        configuration: Option<Configuration>,
    },
    SetRequired {
        goal_id: String,
        expected_goal_revision: String,
        action_key: String,
        subscription_id: String,
        expected_subscription_revision: String,
        expected_activation_epoch: String,
        required: bool,
    },
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Desired {
    Watching,
    Paused,
    Ended,
}
impl Configure {
    pub fn command(
        self,
        scope: &SourceScope,
        at: UtcTimestamp,
    ) -> Result<ControlOwnerEventSubscription, Error> {
        let (goal, revision, key, subscription, control) = match self {
            Self::Prepare {
                goal_id,
                expected_goal_revision,
                action_key,
                configuration,
                required,
            } => (
                goal_id,
                expected_goal_revision,
                action_key,
                None,
                EventSubscriptionControl::Prepare {
                    configuration: configuration.decode()?,
                    required,
                },
            ),
            Self::Change {
                goal_id,
                expected_goal_revision,
                action_key,
                subscription_id,
                expected_subscription_revision,
                expected_activation_epoch,
                desired,
                configuration,
            } => (
                goal_id,
                expected_goal_revision,
                action_key,
                Some(subscription_id),
                EventSubscriptionControl::Change {
                    expected: guard(&expected_subscription_revision, &expected_activation_epoch)?,
                    desired: match desired {
                        Desired::Watching => EventSubscriptionIntent::Watching,
                        Desired::Paused => EventSubscriptionIntent::Paused,
                        Desired::Ended => EventSubscriptionIntent::Ended,
                    },
                    configuration: configuration.map(Configuration::decode).transpose()?,
                },
            ),
            Self::SetRequired {
                goal_id,
                expected_goal_revision,
                action_key,
                subscription_id,
                expected_subscription_revision,
                expected_activation_epoch,
                required,
            } => (
                goal_id,
                expected_goal_revision,
                action_key,
                Some(subscription_id),
                EventSubscriptionControl::SetRequired {
                    expected: guard(&expected_subscription_revision, &expected_activation_epoch)?,
                    required,
                },
            ),
        };
        if key.trim().is_empty() || key.len() > 200 || key.contains('\0') {
            return Err(Error::InvalidArguments);
        }
        let goal_id = GoalId::from_uuid(id(&goal)?).map_err(|_| Error::InvalidArguments)?;
        let command = uuid::Uuid::new_v5(
            &scope.intent_id.into_uuid(),
            format!("owner.goal.sources.configure@1:{goal}:{key}").as_bytes(),
        );
        let subscription = subscription
            .map(|v| id(&v))
            .transpose()?
            .unwrap_or_else(|| uuid::Uuid::new_v5(&command, b"subscription"));
        Ok(ControlOwnerEventSubscription {
            identity: GoalSourceCommandIdentity {
                actor: scope.subject.actor,
                agent_id: scope.subject.agent_id,
                command_id: CommandId::from_uuid(command).map_err(|_| Error::Corrupt)?,
                changed_at: at,
            },
            goal_id,
            expected_goal_revision: counter(&revision)?,
            subscription_id: OwnerEventSubscriptionId::from_uuid(subscription)
                .map_err(|_| Error::InvalidArguments)?,
            control,
        })
    }
}
pub(super) fn id(value: &str) -> Result<uuid::Uuid, Error> {
    let id = uuid::Uuid::parse_str(value).map_err(|_| Error::InvalidArguments)?;
    if id.is_nil() || id.to_string() != value {
        return Err(Error::InvalidArguments);
    }
    Ok(id)
}
pub(super) fn cursor(value: Option<&str>) -> Result<(), Error> {
    if value.is_some_and(|v| v.is_empty() || v.len() > 8192 || v.chars().any(char::is_control)) {
        return Err(Error::InvalidArguments);
    }
    Ok(())
}
pub(super) fn counter(value: &str) -> Result<NonZeroU64, Error> {
    let n = value
        .parse::<u64>()
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(Error::InvalidArguments)?;
    if n.get() > i64::MAX as u64 || n.to_string() != value {
        return Err(Error::InvalidArguments);
    }
    Ok(n)
}
fn guard(revision: &str, epoch: &str) -> Result<EventSubscriptionGuard, Error> {
    let value = EventSubscriptionGuard {
        revision: counter(revision)?,
        activation_epoch: counter(epoch)?,
    };
    if value.activation_epoch > value.revision {
        return Err(Error::InvalidArguments);
    }
    Ok(value)
}
fn selector(
    id_value: &str,
    revision: &str,
    epoch: &str,
    key: &str,
    digest: &str,
) -> Result<OwnerGoalSourceSelector, Error> {
    let revision = counter(revision)?;
    let authority_epoch = counter(epoch)?;
    if authority_epoch > revision
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::InvalidArguments);
    }
    let bytes = (0..64)
        .step_by(2)
        .map(|i| u8::from_str_radix(&digest[i..i + 2], 16).map_err(|_| Error::InvalidArguments))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(OwnerGoalSourceSelector {
        application_binding_id: OwnerApplicationBindingId::from_uuid(id(id_value)?)
            .map_err(|_| Error::InvalidArguments)?,
        application_guard: GoalSourceBindingGuard {
            revision,
            authority_epoch,
        },
        capability_ref: VersionedKey::new(key).map_err(|_| Error::InvalidArguments)?,
        capability_digest: Sha256::from_bytes(bytes).map_err(|_| Error::InvalidArguments)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn scope() -> SourceScope {
        SourceScope {
            subject: OwnerGoalSourceSubject {
                actor: Actor {
                    tenant_id: TenantId::generate(),
                    principal_id: PrincipalId::generate(),
                    principal_kind: PrincipalKind::User,
                },
                agent_id: AgentId::generate(),
                surface: DialogueInputSurface::Desktop,
            },
            intent_id: UserIntentId::generate(),
        }
    }
    fn request(goal: GoalId) -> Value {
        json!({"action":"prepare","goal_id":goal.to_string(),"expected_goal_revision":"1","action_key":"monitor chosen inbox",
        "configuration":{"application_binding_id":OwnerApplicationBindingId::generate().to_string(),"application_revision":"2","application_authority_epoch":"1","capability_ref":"mail.changes@1","capability_sha256":Sha256::digest(b"published source").to_string(),"selection_json":"{\"mailbox\":\"inbox\"}","event_kinds":["mail.changed@1"]},"required":true})
    }
    #[test]
    fn source_resource_cursor_keeps_owner_byte_budget_and_opaque_value() {
        assert!(cursor(None).is_ok());
        for size in [4097, 8192] {
            assert!(cursor(Some(&"x".repeat(size))).is_ok());
        }
        assert!(cursor(Some(&"x".repeat(8193))).is_err());
        assert!(cursor(Some(&"界".repeat(2731))).is_err());
        for invalid in [
            "",
            "a\0b",
            "a\nb",
            "a\rb",
            "a\tb",
            "a\u{007f}b",
            "a\u{0085}b",
        ] {
            assert!(cursor(Some(invalid)).is_err());
        }
        assert!(cursor(Some("opaque+cursor/with==padding")).is_ok());
    }
    #[test]
    fn source_command_is_stable_only_within_original_intent_and_goal() {
        let scope = scope();
        let args = request(GoalId::generate());
        let parse = |scope: &SourceScope, at| {
            serde_json::from_value::<Configure>(args.clone())
                .unwrap()
                .command(scope, UtcTimestamp::from_unix_millis(at))
                .unwrap()
        };
        let first = parse(&scope, 1);
        let replay = parse(&scope, 2);
        assert_eq!(first.identity.command_id, replay.identity.command_id);
        assert_eq!(first.subscription_id, replay.subscription_id);
        let other = SourceScope {
            intent_id: UserIntentId::generate(),
            ..scope.clone()
        };
        assert_ne!(
            first.identity.command_id,
            parse(&other, 1).identity.command_id
        );
        let mut changed = args;
        changed["goal_id"] = json!(GoalId::generate().to_string());
        let other = serde_json::from_value::<Configure>(changed)
            .unwrap()
            .command(&scope, UtcTimestamp::from_unix_millis(1))
            .unwrap();
        assert_ne!(first.identity.command_id, other.identity.command_id);
    }
    #[test]
    fn authority_fields_mixed_actions_duplicate_kinds_and_noncanonical_counters_fail() {
        let scope = scope();
        let args = request(GoalId::generate());
        for field in ["actor", "agent_id", "source_status", "subscription_id"] {
            let mut bad = args.clone();
            bad[field] = json!("injected");
            assert!(serde_json::from_value::<Configure>(bad).is_err());
        }
        for value in ["0", "01", "+1", "9223372036854775808", "1.0"] {
            let mut bad = args.clone();
            bad["expected_goal_revision"] = json!(value);
            assert!(
                serde_json::from_value::<Configure>(bad)
                    .unwrap()
                    .command(&scope, UtcTimestamp::from_unix_millis(1))
                    .is_err()
            );
        }
        let mut bad = args.clone();
        bad["configuration"]["event_kinds"] = json!(["mail.changed@1", "mail.changed@1"]);
        assert!(
            serde_json::from_value::<Configure>(bad)
                .unwrap()
                .command(&scope, UtcTimestamp::from_unix_millis(1))
                .is_err()
        );
        let mut bad = args;
        bad["configuration"]["application_authority_epoch"] = json!("3");
        assert!(
            serde_json::from_value::<Configure>(bad)
                .unwrap()
                .command(&scope, UtcTimestamp::from_unix_millis(1))
                .is_err()
        );
        assert!(serde_json::from_str::<Empty>(r#"{"actor":"x"}"#).is_err());
    }
}
