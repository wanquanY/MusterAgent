use super::*;
use musteragent_execution_liveness::StopReason;

#[derive(Clone, Copy)]
pub(super) enum ObservationFailure {
    Query(ErrorCode),
    Execution(StopReason),
    StoppedOutcomeUnknown(StopReason),
}
impl ObservationFailure {
    fn is_execution_window_expiration(self) -> bool {
        matches!(
            self,
            Self::Execution(
                StopReason::StartupExpired
                    | StopReason::IdleExpired
                    | StopReason::BusinessDeadlineExpired
                    | StopReason::PostExitDrainExpired
            ) | Self::StoppedOutcomeUnknown(
                StopReason::StartupExpired
                    | StopReason::IdleExpired
                    | StopReason::BusinessDeadlineExpired
                    | StopReason::PostExitDrainExpired
            )
        )
    }
    fn code(self) -> ErrorCode {
        match self {
            Self::Query(code) => code,
            Self::StoppedOutcomeUnknown(reason) => Self::Execution(reason).code(),
            Self::Execution(
                StopReason::RequestTimedOut
                | StopReason::StartupExpired
                | StopReason::IdleExpired
                | StopReason::BusinessDeadlineExpired
                | StopReason::PostExitDrainExpired,
            ) => ErrorCode::DependencyTimeout,
            Self::Execution(_) => ErrorCode::DependencyUnavailable,
        }
    }
    fn message(self) -> &'static str {
        match self {
            Self::StoppedOutcomeUnknown(_) => {
                "Tool execution ended but its result could not be confirmed"
            }
            Self::Query(ErrorCode::DependencyTimeout) => {
                "Tool status query timed out; the external result could not be confirmed"
            }
            Self::Query(_) => {
                "Tool status query failed; the external result could not be confirmed"
            }
            Self::Execution(StopReason::RequestTimedOut) => {
                "Tool dependency request timed out; physical stop could not be confirmed"
            }
            Self::Execution(StopReason::StartupExpired) => {
                "Tool startup timed out; physical stop could not be confirmed"
            }
            Self::Execution(StopReason::IdleExpired) => {
                "Tool became unresponsive; physical stop could not be confirmed"
            }
            Self::Execution(StopReason::BusinessDeadlineExpired) => {
                "Tool business deadline expired; physical stop could not be confirmed"
            }
            Self::Execution(StopReason::PostExitDrainExpired) => {
                "Tool result cleanup timed out; physical stop could not be confirmed"
            }
            Self::Execution(_) => {
                "Tool execution stopped being observed; its external outcome could not be confirmed"
            }
        }
    }
}

fn failed_feedback(error: &musteragent_runtime_contract::ErrorInfo) -> serde_json::Value {
    let mut feedback = serde_json::json!({
        "ok": false,
        "outcome": "failed",
        "error": {
            "code": error.code.as_str(),
            "message": error.safe_message.as_str(),
            "retryable": error.retryable,
        }
    });
    if !error.retryable {
        feedback["error"]["next_step"] = serde_json::Value::String(
            "Do not repeat this identical call. Use the message to change the arguments, \
             or address the reported cause before continuing."
                .to_owned(),
        );
    }
    feedback
}

// The supervisor's persisted window expiration is the stop cause. A native
// failure observed during cleanup remains evidence, but cannot rename that
// cause. RequestTimedOut/ExecutionFailed originate at the adapter and retain
// its more specific error. Receipt-query failures are a separate observation.
fn unknown_error_feedback(
    reason: &SafeDisplayText,
    observed: Option<&musteragent_runtime_contract::ErrorInfo>,
    failure: Option<ObservationFailure>,
) -> serde_json::Value {
    let (code, message) =
        if let Some(failure) = failure.filter(|failure| failure.is_execution_window_expiration()) {
            (failure.code().as_str(), failure.message())
        } else if let Some(error) = observed {
            (error.code.as_str(), error.safe_message.as_str())
        } else {
            (
                failure.map_or("tool_outcome_unknown", |failure| failure.code().as_str()),
                reason.as_str(),
            )
        };
    serde_json::json!({ "code": code, "message": message, "retryable": false })
}

// Cancellation records this invocation's terminal cause. It is not a current
// observation of effects committed independently before or during the wait.
fn cancelled_feedback(reason: CancelReason) -> serde_json::Value {
    let mut guidance = String::from(
        "This operation was cancelled. Cancellation does not prove that earlier completed actions were rolled back or that the environment remained unchanged while the operation was pending. Before describing the current state, inspect it through its original owner rather than treating earlier observations as current. Use the cancellation reason and the original request scope to decide whether any follow-up is appropriate.",
    );
    if reason == CancelReason::UserRequested {
        guidance.push_str(
            " The user cancelled this action. Do not repeat it or request the same approval again unless a new user request authorizes it.",
        );
    }
    serde_json::json!({
        "ok": false,
        "cancelled": format!("{reason:?}"),
        "guidance": guidance,
    })
}

impl DurableToolExecutor {
    pub(super) async fn reconciliation_feedback(
        &self,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
    ) -> Result<ToolExecutionOutcome, LoopPortError> {
        let outcome = durable
            .recorded_outcome
            .as_ref()
            .filter(|outcome| matches!(outcome, ToolTerminalOutcome::Indeterminate { .. }))
            .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false))?;
        self.message_for_outcome(call, material, durable, outcome)
            .await
    }

    pub(super) async fn materialize_outcome(
        &self,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        outcome: &ToolTerminalOutcome,
    ) -> Result<ToolExecutionOutcome, PhysicalToolError> {
        self.message_for_outcome(call, material, durable, outcome)
            .await
            .map_err(PhysicalToolError::from)
    }

    pub(super) async fn message_for_outcome(
        &self,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        outcome: &ToolTerminalOutcome,
    ) -> Result<ToolExecutionOutcome, LoopPortError> {
        self.message_for_outcome_with_query_error(call, material, durable, outcome, None)
            .await
    }

    pub(super) async fn message_for_outcome_with_query_error(
        &self,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        outcome: &ToolTerminalOutcome,
        query_error: Option<ErrorCode>,
    ) -> Result<ToolExecutionOutcome, LoopPortError> {
        self.message_for_outcome_with_failure(
            call,
            material,
            durable,
            outcome,
            query_error.map(ObservationFailure::Query),
        )
        .await
    }

    pub(super) async fn message_for_outcome_with_failure(
        &self,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        outcome: &ToolTerminalOutcome,
        failure: Option<ObservationFailure>,
    ) -> Result<ToolExecutionOutcome, LoopPortError> {
        if durable.recorded_outcome.as_ref() != Some(outcome) {
            return Err(port(ErrorCode::InternalInvariantViolated, false));
        }
        let authorized = match outcome {
            ToolTerminalOutcome::SucceededWithAuthorizedContent {
                authorized_content, ..
            } => authorized_content.as_slice(),
            _ => &[],
        };
        let content = match outcome {
            ToolTerminalOutcome::Succeeded(result)
            | ToolTerminalOutcome::SucceededWithAuthorizedContent { result, .. } => {
                if result.result_schema != material.tool.manifest.output_schema {
                    return Err(port(ErrorCode::ContentIntegrityFailed, false));
                }
                let bytes = self
                    .content
                    .read(material.run_context.tenant_id, &result.result_ref)
                    .await
                    .map_err(port_from_content_read)?;
                if bytes.len() <= MAX_INLINE_TOOL_RESULT_BYTES {
                    let inline = String::from_utf8(bytes)
                        .ok()
                        .map(escape_unsupported_model_controls)
                        .filter(|text| text.len() <= MAX_INLINE_TOOL_RESULT_BYTES);
                    if let Some(text) = inline {
                        self.success_content(material, authorized, text).await?
                    } else {
                        text_content(content_reference_json(result.result_ref.clone()))?
                    }
                } else {
                    text_content(content_reference_json(result.result_ref.clone()))?
                }
            }
            ToolTerminalOutcome::Failed { error, diagnostic_result, .. } => {
                self.failure_feedback(
                    material,
                    failed_feedback(error),
                    diagnostic_result.as_ref(),
                ).await?
            }
            ToolTerminalOutcome::Cancelled(reason) =>
                text_content(cancelled_feedback(*reason).to_string())?,
            ToolTerminalOutcome::Indeterminate { reason, observed_error, diagnostic_result, .. } => self.failure_feedback(
                material,
                serde_json::json!({
                    "ok": false,
                    "error": unknown_error_feedback(reason, observed_error.as_ref(), failure),
                    "observed_error": observed_error.as_ref().map(|error| serde_json::json!({
                        "code": error.code.as_str(),
                        "message": error.safe_message.as_str(),
                        "retryable": false,
                    })),
                    "outcome": "unknown",
                    "tool_invocation_id": durable.invocation.invocation_id.to_string(),
                    "may_have_been_applied": durable.invocation.effect != musteragent_policy::Effect::Read,
                    "uncertainty_reason": reason.as_str(),
                    "observation_failure": failure.map(ObservationFailure::message),
                    "stop_confirmed": matches!(failure, Some(ObservationFailure::StoppedOutcomeUnknown(_))),
                    "guidance": "The external result could not be confirmed. Inspect the operation or its target before repeating an action that could have side effects; choose the next step using this observation."
                }),
                diagnostic_result.as_ref(),
            ).await?,
        };
        let message = CanonicalMessage {
            role: CanonicalRole::Tool,
            content,
            tool_calls: Vec::new(),
            tool_call_id: Some(call.call_id.clone()),
            tool_name: Some(call.name.clone()),
            reasoning: None,
        };
        message
            .validate()
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
        let terminal = match outcome {
            ToolTerminalOutcome::Succeeded(_)
            | ToolTerminalOutcome::SucceededWithAuthorizedContent { .. } => {
                DurableToolCompletion::Succeeded
            }
            ToolTerminalOutcome::Failed { error, .. } => {
                DurableToolCompletion::Failed { code: error.code }
            }
            ToolTerminalOutcome::Cancelled(_) => DurableToolCompletion::Cancelled,
            ToolTerminalOutcome::Indeterminate { .. } => DurableToolCompletion::Indeterminate,
        };
        Ok(ToolExecutionOutcome::Completed {
            message: Box::new(message),
            terminal,
        })
    }

    pub(super) async fn success_content(
        &self,
        material: &ToolExecutionMaterial,
        authorized: &[ContentRef],
        text: String,
    ) -> Result<Vec<CanonicalContentPart>, LoopPortError> {
        if matches!(
            material.tool.manifest.key.as_str(),
            "files.read@5" | "files.read@6"
        ) {
            return self.file_input_content(material, authorized, text).await;
        }
        // The durable completion grants image access. A growing platform list
        // of native tool names cannot decide which plugins may return images.
        let authorized_images = material.tool.manifest.wire_name.as_str() != "vision_analyze";
        let images = if authorized_images {
            result_images::from_tool_result(&text, authorized)?
        } else {
            let output: VisionToolOutput = serde_json::from_str(&text)
                .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
            if output.mode != "native_primary" {
                return text_content(text);
            }
            output
                .image_refs
                .into_iter()
                .map(VisionToolContentRef::decode)
                .collect::<Result<Vec<_>, _>>()?
        };
        if images.is_empty() {
            return text_content(text);
        }
        let active_model =
            active_pinned_model(&material.model, material.active_model_route_ordinal)
                .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        if !pinned_model_supports_vision(&active_model) {
            // Text-only routes keep the explicitly authorized image references
            // available to the normal vision-analysis capability.
            if authorized_images {
                return text_content(text);
            }
            return Err(port(ErrorCode::ContentIntegrityFailed, false));
        }
        let mut content = text_content(text)?;
        for image_ref in images {
            if !image_ref
                .media_type
                .as_str()
                .split(';')
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .starts_with("image/")
            {
                return Err(port(ErrorCode::ContentIntegrityFailed, false));
            }
            let bytes = self
                .content
                .read(material.run_context.tenant_id, &image_ref)
                .await
                .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
            verify_content(&image_ref, &bytes)
                .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
            content.push(CanonicalContentPart::Image(image_ref));
        }
        Ok(content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_feedback_preserves_all_original_causes_and_observation_limits() {
        for (reason, expected) in [
            (CancelReason::UserRequested, "UserRequested"),
            (CancelReason::ConversationArchived, "ConversationArchived"),
            (CancelReason::ParentCancelled, "ParentCancelled"),
            (CancelReason::PolicyRevoked, "PolicyRevoked"),
            (CancelReason::SystemShutdown, "SystemShutdown"),
            (
                CancelReason::CoordinationInterrupted,
                "CoordinationInterrupted",
            ),
        ] {
            let feedback = cancelled_feedback(reason);
            assert_eq!(feedback["ok"], false);
            assert_eq!(feedback["cancelled"], expected);
            let guidance = feedback["guidance"].as_str().unwrap();
            assert!(guidance.contains("Cancellation does not prove"));
            assert!(guidance.contains("earlier completed actions were rolled back"));
            assert!(guidance.contains("environment remained unchanged"));
            assert!(guidance.contains("inspect it through its original owner"));
            assert!(feedback.get("ready").is_none());
            assert!(feedback.get("retryable").is_none());
        }
    }

    #[test]
    fn only_explicit_user_cancellation_carries_the_user_request_boundary() {
        let user = cancelled_feedback(CancelReason::UserRequested);
        let guidance = user["guidance"].as_str().unwrap();
        assert!(guidance.contains("The user cancelled this action."));
        assert!(guidance.contains("unless a new user request authorizes it"));
        for reason in [
            CancelReason::ConversationArchived,
            CancelReason::ParentCancelled,
            CancelReason::PolicyRevoked,
            CancelReason::SystemShutdown,
            CancelReason::CoordinationInterrupted,
        ] {
            let feedback = cancelled_feedback(reason);
            let guidance = feedback["guidance"].as_str().unwrap();
            assert!(!guidance.contains("The user cancelled"));
            assert!(!guidance.contains("a new user request"));
            assert!(guidance.contains("Use the cancellation reason"));
        }
    }

    #[test]
    fn stop_cause_and_native_failure_remain_distinct_for_feedback_and_replay() {
        let reason = SafeDisplayText::new("result not confirmed").unwrap();
        let native = musteragent_runtime_contract::ErrorInfo {
            code: ErrorCode::CapabilityUnavailable,
            category: ErrorCode::CapabilityUnavailable.category(),
            retryable: false,
            safe_message: SafeDisplayText::new("native HTTP 404 during cleanup").unwrap(),
            dependency_ref: None,
            diagnostic_ref: None,
        };
        for stop in [
            StopReason::StartupExpired,
            StopReason::IdleExpired,
            StopReason::BusinessDeadlineExpired,
            StopReason::PostExitDrainExpired,
        ] {
            for failure in [
                ObservationFailure::Execution(stop),
                ObservationFailure::StoppedOutcomeUnknown(stop),
            ] {
                let feedback = unknown_error_feedback(&reason, Some(&native), Some(failure));
                assert_eq!(feedback["code"], "dependency_timeout");
                assert_eq!(feedback["retryable"], false);
                assert_ne!(feedback["message"], native.safe_message.as_str());
            }
        }
        for failure in [
            ObservationFailure::Execution(StopReason::ExecutionFailed),
            ObservationFailure::Query(ErrorCode::DependencyTimeout),
        ] {
            let feedback = unknown_error_feedback(&reason, Some(&native), Some(failure));
            assert_eq!(feedback["code"], "capability_unavailable");
            assert_eq!(feedback["message"], native.safe_message.as_str());
        }
        assert_eq!(native.code, ErrorCode::CapabilityUnavailable);
        assert_eq!(
            native.safe_message.as_str(),
            "native HTTP 404 during cleanup"
        );
    }

    #[test]
    fn unavailable_result_storage_is_recoverable_without_fabricating_tool_failure() {
        use super::super::support::port_from_content_read;
        use musteragent_runtime_content::ContentStoreError;
        let error = port_from_content_read(ContentStoreError::Unavailable);
        assert_eq!(error.code, ErrorCode::ContentUnavailable);
        assert!(error.retryable);
        for cause in [
            ContentStoreError::NotFound,
            ContentStoreError::HashMismatch,
            ContentStoreError::LengthMismatch,
        ] {
            let error = port_from_content_read(cause);
            assert_eq!(error.code, ErrorCode::ContentIntegrityFailed);
            assert!(!error.retryable);
        }
    }
}
