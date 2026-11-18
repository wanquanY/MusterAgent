use std::sync::Arc;
mod physical;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use musteragent_capability::{CapabilityLocation, CapabilityManifest};
use musteragent_policy::Effect;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use serde::{Deserialize, Serialize};

use crate::{
    CapabilityInvocation, CapabilityOutcome, CapabilityProvider, CapabilityReconciliation,
    CapabilityResult, ReconciliationOutcome, RuntimeClock,
};

const MAX_TIMEZONE_CHARS: usize = 100;

pub struct BuiltinTimeCapability {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    clock: Arc<dyn RuntimeClock>,
}

impl BuiltinTimeCapability {
    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Result<Self, musteragent_capability::CapabilityContractError> {
        manifest.validate()?;
        Ok(Self {
            manifest,
            content,
            clock,
        })
    }

    async fn invoke_inner(
        &self,
        invocation: CapabilityInvocation,
        progress: Option<&physical::TimeProgress<'_>>,
    ) -> CapabilityOutcome {
        if !matches!(
            self.manifest.location,
            CapabilityLocation::Cloud | CapabilityLocation::Device
        ) || self.manifest.effect != Effect::Read
            || self.manifest.key.logical_key() != "core.time.read"
            || invocation.actor.tenant_id != invocation.tenant_id
            || invocation.capability_key != self.manifest.key
            || invocation.capability_provider_id != self.manifest.provider_id
            || invocation.tool_key.logical_key() != "system.time.current"
        {
            return failed(
                ErrorCode::PolicyDenied,
                "Time Capability binding differs from the Runtime Snapshot",
            );
        }
        let bytes = match self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
        {
            Ok(bytes) => bytes,
            Err(_) => {
                return failed(
                    ErrorCode::ContentIntegrityFailed,
                    "Time Capability input could not be resolved",
                );
            }
        };
        if musteragent_runtime_content::verify_content(&invocation.request_ref, &bytes).is_err() {
            return failed(
                ErrorCode::ContentIntegrityFailed,
                "Time Capability input differs from its original content reference",
            );
        }
        let input: TimeInput = match serde_json::from_slice(&bytes) {
            Ok(input) => input,
            Err(_) => {
                return failed(
                    ErrorCode::InvalidCommand,
                    "Time Capability input is invalid",
                );
            }
        };
        if input.timezone.is_empty() || input.timezone.chars().count() > MAX_TIMEZONE_CHARS {
            return failed(
                ErrorCode::InvalidCommand,
                "Time Capability timezone is invalid",
            );
        }
        let timezone: Tz = match input.timezone.parse() {
            Ok(timezone) => timezone,
            Err(_) => {
                return failed(
                    ErrorCode::InvalidCommand,
                    "Time Capability timezone is unknown",
                );
            }
        };
        if let Some(progress) = progress
            && let Err(code) =
                progress.checkpoint(std::num::NonZeroU64::new(1).expect("constant cursor"))
        {
            return failed(
                code,
                "Time execution stopped or its work evidence was rejected",
            );
        }
        let now = self.clock.now();
        let Some(utc) = DateTime::<Utc>::from_timestamp_millis(now.unix_millis()) else {
            return failed(
                ErrorCode::InternalInvariantViolated,
                "Runtime time is outside the supported range",
            );
        };
        let output = TimeOutput {
            timezone: input.timezone,
            iso: utc.with_timezone(&timezone).to_rfc3339(),
            unix_timestamp: utc.timestamp(),
        };
        let bytes = match serde_json::to_vec(&output) {
            Ok(bytes) => bytes,
            Err(_) => {
                return failed(
                    ErrorCode::InternalInvariantViolated,
                    "Time Capability output could not be encoded",
                );
            }
        };
        if let Some(progress) = progress
            && let Err(code) =
                progress.checkpoint(std::num::NonZeroU64::new(2).expect("constant cursor"))
        {
            return failed(
                code,
                "Time execution stopped or its work evidence was rejected",
            );
        }
        let result_ref = match self
            .content
            .put(PutContent {
                tenant_id: invocation.tenant_id,
                media_type: musteragent_content_ref::MediaType::new("application/json")
                    .expect("static media type is valid"),
                bytes,
            })
            .await
        {
            Ok(result_ref) => result_ref,
            Err(_) => {
                return failed(
                    ErrorCode::DependencyUnavailable,
                    "Time Capability output could not be stored",
                );
            }
        };
        CapabilityOutcome::Succeeded(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: invocation.result_schema,
            provider_request_ref: None,
        })
    }
}

impl CapabilityProvider for BuiltinTimeCapability {
    fn execution(
        self: Arc<Self>,
        owner: musteragent_run::AttemptContext,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<crate::CapabilityExecution, crate::CapabilityRuntimeError> {
        physical::execution(self, owner, invocation, binding)
    }

    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }

    fn invoke(&self, invocation: CapabilityInvocation) -> ApplicationFuture<'_, CapabilityOutcome> {
        Box::pin(async move { self.invoke_inner(invocation, None).await })
    }

    fn reconcile(
        &self,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'_, ReconciliationOutcome> {
        Box::pin(async move {
            ReconciliationOutcome::StillIndeterminate {
                reason: safe_text("Read-only Time Capability never requires reconciliation"),
                reconciliation_ref: request.reconciliation_ref,
            }
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimeInput {
    #[serde(default = "default_timezone")]
    timezone: String,
}

#[derive(Debug, Serialize)]
struct TimeOutput {
    timezone: String,
    iso: String,
    unix_timestamp: i64,
}

fn default_timezone() -> String {
    "Asia/Shanghai".to_owned()
}

fn failed(code: ErrorCode, message: &str) -> CapabilityOutcome {
    CapabilityOutcome::Failed {
        error: ErrorInfo {
            code,
            category: code.category(),
            safe_message: safe_text(message),
            retryable: code == ErrorCode::DependencyUnavailable,
            dependency_ref: None,
            diagnostic_ref: None,
        },
        retry_disposition: if code == ErrorCode::DependencyUnavailable {
            RetryDisposition::RetryNewAttempt
        } else {
            RetryDisposition::DoNotRetry
        },
        diagnostic_result: None,
    }
}

fn safe_text(value: &str) -> SafeDisplayText {
    SafeDisplayText::new(value).expect("static safe display text is valid")
}
