//! Closed user-impact descriptions for authoritative, non-recovering endings.
//! No diagnostic text, provider response, Tool log, or arbitrary status is used
//! to compose a public message. The store must establish terminal proof first.
use musteragent_runtime_contract::ErrorCode;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DialogueTerminalNotice {
    IncompleteResult,
    ToolUseFailed,
    ServiceUnavailable,
    AccountLimit,
    AuthorizationRequired,
    InputUnavailable,
    ExecutionStopped,
    OutcomeUnconfirmed,
}
impl DialogueTerminalNotice {
    pub const fn from_final_failure(code: ErrorCode) -> Self {
        match code {
            ErrorCode::ModelResponseInvalid
            | ErrorCode::ModelContextExceeded
            | ErrorCode::PublicDeliveryMissing => Self::IncompleteResult,
            // The model kept calling a tool that was not available or with
            // arguments the tool rejects, after its corrective turns.
            ErrorCode::ToolNotExposed | ErrorCode::InvalidCommand => Self::ToolUseFailed,
            ErrorCode::ModelRouteUnavailable
            | ErrorCode::ModelRequestRejected
            | ErrorCode::DependencyUnavailable
            | ErrorCode::DependencyTimeout => Self::ServiceUnavailable,
            ErrorCode::QuotaExceeded
            | ErrorCode::CreditsExhausted
            | ErrorCode::ModelPaymentRequired => Self::AccountLimit,
            ErrorCode::Unauthenticated
            | ErrorCode::CredentialRefreshRequired
            | ErrorCode::ParticipantNotAuthorized
            | ErrorCode::PolicyDenied
            | ErrorCode::ModelNotEntitled => Self::AuthorizationRequired,
            ErrorCode::ContentUnavailable
            | ErrorCode::ContentIntegrityFailed
            | ErrorCode::ResourceFileUnavailable => Self::InputUnavailable,
            _ => Self::ExecutionStopped,
        }
    }
    /// Stable safe fallback; hosts may localize presentation outside the kernel.
    pub const fn message(self) -> &'static str {
        match self {
            Self::IncompleteResult => "Execution ended without a complete result.",
            Self::ToolUseFailed => "The required tool could not complete successfully.",
            Self::ServiceUnavailable => "An execution dependency is unavailable.",
            Self::AccountLimit => "The execution quota is exhausted.",
            Self::AuthorizationRequired => "Execution authorization is missing or expired.",
            Self::InputUnavailable => "Required input is unavailable or failed integrity checks.",
            Self::ExecutionStopped => "Execution stopped before completion.",
            Self::OutcomeUnconfirmed => "An operation outcome remains unconfirmed.",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PublicTerminalDeliveryOutcome {
    Delivered,
    Suppressed,
    AlreadyResolved,
}
