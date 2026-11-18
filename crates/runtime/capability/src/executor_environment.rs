//! Read-only execution planning. A directory observation never grants a Run.
use crate::DialogueStoreError;
use musteragent_identity::{Actor, AgentId, DeviceId, DeviceInstallationId};
use musteragent_runtime_contract::ApplicationFuture;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutorEnvironmentRef {
    Cloud {},
    Native {
        membership_id: uuid::Uuid,
        membership_epoch: u64,
    },
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ExecutorEnvironmentQuery {
    List {
        after: Option<uuid::Uuid>,
        limit: u32,
    },
    Inspect {
        reference: ExecutorEnvironmentRef,
        view: ExecutorEnvironmentView,
    },
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorEnvironmentView {
    #[default]
    Summary,
    Capabilities,
    Eligibility,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListArguments {
    after_membership_id: Option<uuid::Uuid>,
    #[serde(default = "page_size")]
    limit: u32,
}
fn page_size() -> u32 {
    20
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectArguments {
    environment_ref: ExecutorEnvironmentRef,
    #[serde(default)]
    view: ExecutorEnvironmentView,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorEnvironmentOutputVersion {
    V1,
    V2,
}

pub fn parse_executor_environment_query(
    key: &str,
    bytes: &[u8],
) -> Result<(ExecutorEnvironmentQuery, ExecutorEnvironmentOutputVersion), DialogueStoreError> {
    if bytes.len() > 4096 {
        return Err(DialogueStoreError::Corrupt);
    }
    let (name, revision) = key.split_once('@').ok_or(DialogueStoreError::Corrupt)?;
    let version = match revision {
        "1" => ExecutorEnvironmentOutputVersion::V1,
        "2" => ExecutorEnvironmentOutputVersion::V2,
        _ => return Err(DialogueStoreError::Corrupt),
    };
    let query = match name {
        "environments.list" => {
            let a: ListArguments =
                serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
            if !(1..=50).contains(&a.limit) || a.after_membership_id.is_some_and(|id| id.is_nil()) {
                return Err(DialogueStoreError::Corrupt);
            }
            ExecutorEnvironmentQuery::List {
                after: a.after_membership_id,
                limit: a.limit,
            }
        }
        "environments.inspect" => {
            let a: InspectArguments =
                serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
            if let ExecutorEnvironmentRef::Native {
                membership_id,
                membership_epoch,
            } = &a.environment_ref
                && (membership_id.is_nil()
                    || *membership_epoch == 0
                    || *membership_epoch > i64::MAX as u64)
            {
                return Err(DialogueStoreError::Corrupt);
            }
            ExecutorEnvironmentQuery::Inspect {
                reference: a.environment_ref,
                view: a.view,
            }
        }
        _ => return Err(DialogueStoreError::Corrupt),
    };
    Ok((query, version))
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorEnvironmentConsent {
    Unknown,
    NotGranted,
    Active,
    Expired,
    Changed,
    Unavailable,
}
impl ExecutorEnvironmentConsent {
    // A fresh validation belongs to task admission. Only the consent owner can
    // say whether a user decision is needed; an observation cannot grant it.
    fn next_step(self) -> &'static str {
        match self {
            Self::Unknown => "inspect_environment",
            Self::NotGranted | Self::Expired => "obtain_explicit_consent",
            Self::Active => "none",
            Self::Changed => "review_changed_consent",
            Self::Unavailable => "retry_status_check",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ExecutorEnvironmentCapability {
    pub key: String,
    pub policy_decision: String,
    pub allowed_scopes: Vec<String>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeEnvironment {
    pub membership_id: uuid::Uuid,
    pub membership_epoch: u64,
    pub device_id: DeviceId,
    pub installation_id: DeviceInstallationId,
    pub surface: Option<String>,
    pub display_name: Option<String>,
    pub platform: Option<String>,
    pub connection_observation: Option<(i64, i64)>,
    pub last_authenticated_request_at_ms: Option<i64>,
    pub consent: ExecutorEnvironmentConsent,
    pub capabilities: Vec<ExecutorEnvironmentCapability>,
    pub capabilities_complete: bool,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeEnvironmentPage {
    pub environments: Vec<NativeEnvironment>,
    pub next_membership_id: Option<uuid::Uuid>,
    pub observed_at_ms: i64,
    pub permission_revision: u64,
}
/// Account and Agent are injected from sealed Tool material, never arguments.
/// Implementations consult the original membership/configuration/consent owners.
pub trait ExecutorEnvironmentDirectory: Send + Sync {
    fn read(
        &self,
        actor: Actor,
        agent: AgentId,
        query: ExecutorEnvironmentQuery,
    ) -> ApplicationFuture<'_, Result<NativeEnvironmentPage, DialogueStoreError>>;
}

impl NativeEnvironment {
    pub fn projection(
        &self,
        observed_at_ms: i64,
        permission_revision: u64,
        version: ExecutorEnvironmentOutputVersion,
    ) -> serde_json::Value {
        let mut native = serde_json::json!({
            "environment_ref": ExecutorEnvironmentRef::Native { membership_id: self.membership_id, membership_epoch: self.membership_epoch },
            "host_kind": self.surface,
            "device_id": self.device_id.to_string(),
            "installation_id": self.installation_id.to_string(),
            "connection": {
                "state": "unknown",
                "last_authenticated_request_at_ms": self.last_authenticated_request_at_ms
            },
            "capacity": { "state": "unknown" },
            "execution": { "consent": self.consent, "dispatch_requires_fresh_validation": true },
            "capabilities": self.capabilities,
            "capabilities_complete": self.capabilities_complete,
            "observed_at_ms": observed_at_ms,
            "permission_revision": permission_revision.to_string(),
        });
        if version == ExecutorEnvironmentOutputVersion::V2 {
            native["display_name"] = serde_json::json!(self.display_name);
            native["platform"] = serde_json::json!(self.platform);
            let (state, start, end, source) = match self.connection_observation {
                Some((start, end)) => (
                    if end > observed_at_ms {
                        "reachable"
                    } else {
                        "unreachable"
                    },
                    Some(start),
                    Some(end),
                    Some("native_discovery"),
                ),
                None => ("unknown", None, None, None),
            };
            native["connection"] = serde_json::json!({
                "state": state, "observed_at_ms": start, "expires_at_ms": end, "source": source,
                "last_authenticated_request_at_ms": self.last_authenticated_request_at_ms,
            });
            native["execution"]["consent_next_step"] = serde_json::json!(self.consent.next_step());
        }
        native
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn discovery_cannot_choose_an_actor_or_supply_authority() {
        for bytes in [
            r#"{"actor":"other"}"#,
            r#"{"agent_id":"other"}"#,
            r#"{"limit":0}"#,
            r#"{"limit":51}"#,
            r#"{"consent":true}"#,
        ] {
            assert!(
                parse_executor_environment_query("environments.list@1", bytes.as_bytes()).is_err()
            );
        }
        assert_eq!(
            parse_executor_environment_query("environments.list@1", b"{}"),
            Ok((
                ExecutorEnvironmentQuery::List {
                    after: None,
                    limit: 20
                },
                ExecutorEnvironmentOutputVersion::V1
            ))
        );
    }
    #[test]
    fn native_reference_requires_an_observed_nonzero_membership_epoch() {
        let id = uuid::Uuid::from_u128(1);
        for epoch in [0_u64, u64::MAX] {
            let bytes = serde_json::to_vec(&serde_json::json!({"environment_ref":{"kind":"native","membership_id":id,"membership_epoch":epoch}})).unwrap();
            assert!(parse_executor_environment_query("environments.inspect@1", &bytes).is_err());
        }
        assert!(
            parse_executor_environment_query(
                "environments.inspect@1",
                br#"{"environment_ref":{"kind":"cloud","consent":true}}"#
            )
            .is_err()
        );
    }
    #[test]
    fn approved_consent_and_recent_proof_never_mean_online_or_free_capacity() {
        let value = NativeEnvironment {
            membership_id: uuid::Uuid::from_u128(1),
            membership_epoch: 1,
            device_id: DeviceId::generate(),
            installation_id: DeviceInstallationId::generate(),
            surface: Some("desktop".into()),
            display_name: None,
            platform: None,
            connection_observation: None,
            last_authenticated_request_at_ms: Some(100),
            consent: ExecutorEnvironmentConsent::Active,
            capabilities: vec![],
            capabilities_complete: true,
        }
        .projection(101, 3, ExecutorEnvironmentOutputVersion::V2);
        assert_eq!(value["connection"]["state"], "unknown");
        assert_eq!(value["capacity"]["state"], "unknown");
        assert_eq!(value["execution"]["consent"], "active");
        assert_eq!(
            value["execution"]["dispatch_requires_fresh_validation"],
            true
        );
    }
}
