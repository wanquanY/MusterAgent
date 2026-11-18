use opentelemetry::KeyValue;
use serde_json::{Map, Value};

use crate::TelemetryConfig;

/// Low-cardinality deployment identity shared by every telemetry signal.
///
/// Request, tenant, conversation and run identities never belong here: adding
/// any of them to a Resource would create unbounded metric series.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct TelemetryResourceIdentity {
    service: String,
    version: String,
    environment: String,
    cluster: String,
    deployment: String,
    build: String,
}

impl TelemetryResourceIdentity {
    pub(crate) fn new(
        service: String,
        version: String,
        environment: String,
        cluster: String,
        deployment: String,
        build: String,
    ) -> Self {
        Self {
            service,
            version,
            environment,
            cluster,
            deployment,
            build,
        }
    }

    pub(crate) fn from_config(config: &TelemetryConfig) -> Self {
        Self::new(
            config.service_name.clone(),
            config.service_version.clone(),
            config.deployment_environment.clone(),
            config.cluster_identity.clone(),
            config.deployment_identity.clone(),
            config.build_identity.clone(),
        )
    }

    pub(crate) fn otel_attributes(&self) -> [KeyValue; 8] {
        [
            KeyValue::new("service.name", self.service.clone()),
            KeyValue::new("service.namespace", "musteragent.agent-runtime"),
            KeyValue::new("service.version", self.version.clone()),
            KeyValue::new("deployment.environment.name", self.environment.clone()),
            KeyValue::new("kernel.cluster.id", self.cluster.clone()),
            KeyValue::new("musteragent.deployment.id", self.deployment.clone()),
            KeyValue::new("kernel.build.id", self.build.clone()),
            KeyValue::new(
                "kernel.telemetry.schema",
                musteragent_telemetry::TELEMETRY_SCHEMA_VERSION,
            ),
        ]
    }

    pub(crate) fn insert_log_fields(&self, envelope: &mut Map<String, Value>) {
        for (key, value) in [
            ("service", &self.service),
            ("version", &self.version),
            ("environment", &self.environment),
            ("cluster", &self.cluster),
            ("deployment", &self.deployment),
            ("build", &self.build),
        ] {
            envelope.insert(key.to_owned(), Value::String(value.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_identity_is_shared_without_request_cardinality() {
        let identity = TelemetryResourceIdentity::new(
            "runtime-api".to_owned(),
            "1.2.3".to_owned(),
            "production".to_owned(),
            "primary".to_owned(),
            "release-42".to_owned(),
            "abc123".to_owned(),
        );
        let attributes = identity.otel_attributes();
        let keys = attributes
            .iter()
            .map(|attribute| attribute.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            [
                "service.name",
                "service.namespace",
                "service.version",
                "deployment.environment.name",
                "kernel.cluster.id",
                "musteragent.deployment.id",
                "kernel.build.id",
                "kernel.telemetry.schema",
            ]
        );
        assert!(keys.iter().all(|key| !matches!(
            *key,
            "tenant_id" | "conversation_id" | "run_id" | "trace_id"
        )));

        let mut log = Map::new();
        identity.insert_log_fields(&mut log);
        assert_eq!(log["environment"], "production");
        assert_eq!(log["deployment"], "release-42");
        assert_eq!(log["build"], "abc123");
    }
}
