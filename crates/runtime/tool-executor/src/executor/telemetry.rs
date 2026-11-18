use super::*;

pub(super) fn durable_tool_telemetry_observation(
    result: &Result<ToolExecutionOutcome, LoopPortError>,
    durable: Result<&DurableToolObservation, &ToolExecutionPortError>,
) -> ToolTelemetryObservation {
    let receipt = match durable {
        Ok(observation) => match durable_tool_receipt_from_state(
            observation.durable().invocation.status,
            observation.durable().recorded_outcome.as_ref(),
        ) {
            Ok(receipt) => ToolObservationReceipt::Durable {
                receipt,
                // The durable receipt describes the external/authoritative
                // Tool transition. Any error returned after that receipt is a
                // distinct failure to materialize the terminal result for the
                // Agent Loop, regardless of its error class.
                materialization_error: result.as_ref().err().map(|error| error.code),
            },
            Err(invariant) => ToolObservationReceipt::ExecutionError(
                result.as_ref().err().map_or(invariant, |error| error.code),
            ),
        },
        Err(read_error) => ToolObservationReceipt::ExecutionError(
            result
                .as_ref()
                .err()
                .map_or(read_error.code, |error| error.code),
        ),
    };
    tool_telemetry_observation(receipt)
}

pub(super) fn durable_tool_receipt_from_state(
    status: ToolInvocationStatus,
    outcome: Option<&ToolTerminalOutcome>,
) -> Result<DurableToolReceipt, ErrorCode> {
    match (status, outcome) {
        (ToolInvocationStatus::WaitingInteraction, None) => Ok(DurableToolReceipt::Waiting),
        (
            ToolInvocationStatus::Succeeded,
            Some(
                ToolTerminalOutcome::Succeeded(_)
                | ToolTerminalOutcome::SucceededWithAuthorizedContent { .. },
            ),
        ) => Ok(DurableToolReceipt::Succeeded),
        (ToolInvocationStatus::Failed, Some(ToolTerminalOutcome::Failed { error, .. })) => {
            Ok(DurableToolReceipt::Failed { code: error.code })
        }
        (ToolInvocationStatus::Cancelled, Some(ToolTerminalOutcome::Cancelled(_))) => {
            Ok(DurableToolReceipt::Cancelled)
        }
        (ToolInvocationStatus::Indeterminate, Some(ToolTerminalOutcome::Indeterminate { .. })) => {
            Ok(DurableToolReceipt::Indeterminate)
        }
        _ => Err(ErrorCode::InternalInvariantViolated),
    }
}

pub(super) fn tool_telemetry_observation(
    receipt: ToolObservationReceipt,
) -> ToolTelemetryObservation {
    match receipt {
        ToolObservationReceipt::Durable {
            receipt: DurableToolReceipt::Succeeded,
            materialization_error,
        } => ToolTelemetryObservation {
            outcome: TelemetryOutcome::Success,
            error: None,
            materialization_error,
        },
        ToolObservationReceipt::Durable {
            receipt: DurableToolReceipt::Failed { code },
            materialization_error,
        } => ToolTelemetryObservation {
            outcome: TelemetryOutcome::Error,
            error: Some(code),
            materialization_error,
        },
        ToolObservationReceipt::Durable {
            receipt: DurableToolReceipt::Cancelled,
            materialization_error,
        } => ToolTelemetryObservation {
            outcome: TelemetryOutcome::Cancelled,
            error: None,
            materialization_error,
        },
        ToolObservationReceipt::Durable {
            receipt: DurableToolReceipt::Waiting,
            materialization_error,
        } => ToolTelemetryObservation {
            outcome: TelemetryOutcome::Waiting,
            error: None,
            materialization_error,
        },
        ToolObservationReceipt::Durable {
            receipt: DurableToolReceipt::Indeterminate,
            materialization_error,
        } => ToolTelemetryObservation {
            outcome: TelemetryOutcome::ExternalOutcomeUnknown,
            error: None,
            materialization_error,
        },
        ToolObservationReceipt::ExecutionError(code) => ToolTelemetryObservation {
            outcome: TelemetryOutcome::Error,
            error: Some(code),
            materialization_error: None,
        },
    }
}
