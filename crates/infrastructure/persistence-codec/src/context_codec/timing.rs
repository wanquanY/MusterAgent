//! Closed invocation timing in the original canonical model-request document.
//! Legacy documents retain their exact `deadline_ms` field and bytes.
use musteragent_content_ref::Sha256;
use musteragent_context::ContextPlanError;
use musteragent_identity::{UtcTimestamp, VersionedKey};
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, BusinessDeadline, InvocationExecutionTiming, LivenessPolicyRef,
};
use serde_json::{Map, Value, json};

pub(super) fn encode(fields: &mut Map<String, Value>, timing: &InvocationExecutionTiming) {
    match timing {
        InvocationExecutionTiming::LegacyFixed { deadline } => {
            fields.insert("deadline_ms".to_owned(), json!(deadline.unix_millis()));
        }
        InvocationExecutionTiming::ActivityV1(value) => {
            fields.insert(
                "activity_timing_v1".to_owned(),
                json!({
                    "policy_key": value.policy.key.as_str(),
                    "policy_digest": value.policy.digest.to_string(),
                    "business_deadline": value.business_deadline.as_ref().map(|deadline| json!({
                        "at_ms": deadline.at.unix_millis(),
                        "source_policy_key": deadline.source.as_str(),
                    })),
                }),
            );
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivityTiming {
    policy_key: String,
    policy_digest: String,
    business_deadline: Option<Deadline>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Deadline {
    at_ms: i64,
    source_policy_key: String,
}

pub(super) fn decode(
    fields: &Map<String, Value>,
) -> Result<InvocationExecutionTiming, ContextPlanError> {
    let invalid = |_| ContextPlanError::invariant();
    match (fields.get("deadline_ms"), fields.get("activity_timing_v1")) {
        (Some(deadline), None) => Ok(InvocationExecutionTiming::LegacyFixed {
            deadline: UtcTimestamp::from_unix_millis(
                deadline.as_i64().ok_or_else(ContextPlanError::invariant)?,
            ),
        }),
        (None, Some(activity)) => {
            let value: ActivityTiming =
                serde_json::from_value(activity.clone()).map_err(invalid)?;
            let business_deadline = value
                .business_deadline
                .map(|deadline| {
                    Ok::<_, ContextPlanError>(BusinessDeadline {
                        at: UtcTimestamp::from_unix_millis(deadline.at_ms),
                        source: VersionedKey::new(deadline.source_policy_key)
                            .map_err(|_| ContextPlanError::invariant())?,
                    })
                })
                .transpose()?;
            Ok(InvocationExecutionTiming::ActivityV1(
                ActivityExecutionTiming {
                    policy: LivenessPolicyRef {
                        key: VersionedKey::new(value.policy_key)
                            .map_err(|_| ContextPlanError::invariant())?,
                        digest: Sha256::from_hex(&value.policy_digest)
                            .map_err(|_| ContextPlanError::invariant())?,
                    },
                    business_deadline,
                },
            ))
        }
        _ => Err(ContextPlanError::invariant()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invocation_timing_preserves_legacy_bytes_and_closes_activity_shape() {
        let mut fields = Map::new();
        let legacy = InvocationExecutionTiming::LegacyFixed {
            deadline: UtcTimestamp::from_unix_millis(123),
        };
        encode(&mut fields, &legacy);
        assert_eq!(
            serde_json::to_string(&fields).expect("encode"),
            r#"{"deadline_ms":123}"#
        );
        assert_eq!(decode(&fields).expect("legacy"), legacy);
        let activity = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: LivenessPolicyRef {
                key: VersionedKey::new("execution.activity@1").expect("key"),
                digest: Sha256::digest(b"policy"),
            },
            business_deadline: None,
        });
        fields.clear();
        encode(&mut fields, &activity);
        assert!(!fields.contains_key("deadline_ms"));
        assert_eq!(decode(&fields).expect("activity"), activity);
        fields.insert("deadline_ms".to_owned(), Value::Null);
        assert!(decode(&fields).is_err());
        fields.remove("deadline_ms");
        fields.get_mut("activity_timing_v1").expect("activity")["idle_ms"] = json!(1000);
        assert!(decode(&fields).is_err());
        assert!(decode(&Map::new()).is_err());
    }

    #[test]
    fn invocation_business_deadline_requires_original_provenance() {
        let timing = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: LivenessPolicyRef {
                key: VersionedKey::new("execution.activity@1").expect("key"),
                digest: Sha256::digest(b"policy"),
            },
            business_deadline: Some(BusinessDeadline {
                at: UtcTimestamp::from_unix_millis(5000),
                source: VersionedKey::new("job.deadline@1").expect("source"),
            }),
        });
        let mut fields = Map::new();
        encode(&mut fields, &timing);
        assert_eq!(decode(&fields).expect("activity"), timing);
        fields.get_mut("activity_timing_v1").expect("activity")["business_deadline"] =
            json!({"at_ms":5000});
        assert!(decode(&fields).is_err());
    }
}
