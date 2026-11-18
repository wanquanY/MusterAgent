use thiserror::Error;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum NormalizeError {
    #[error("Provider event JSON object is required")]
    ObjectRequired,
    #[error("Provider event JSON array is required")]
    ArrayRequired,
    #[error("Provider event is missing required field {0}")]
    MissingField(&'static str),
    #[error("Provider event requires non-empty text")]
    TextRequired,
    #[error("Provider event requires an unsigned integer")]
    UnsignedIntegerRequired,
    #[error("Provider output index exceeds u32")]
    IndexOverflow,
    #[error("Provider event has no type")]
    MissingEventType,
    #[error("Provider emitted an unknown event type")]
    UnknownEventType,
    #[error("Provider emitted an unknown content block")]
    UnknownContentBlock,
    #[error("Provider emitted an unknown delta type")]
    UnknownDeltaType,
    #[error("Provider emitted an unknown stop reason")]
    UnknownStopReason,
    #[error("Provider response stream closed before start")]
    ClosedBeforeStart,
    #[error("Provider response stream closed before a protocol terminal marker")]
    ClosedBeforeTerminal,
    #[error("Provider emitted output after terminal")]
    EventAfterTerminal,
    #[error("Provider response identity changed within one invocation")]
    ResponseIdentityChanged,
    #[error("Provider response reference is invalid")]
    InvalidProviderReference,
    #[error("Provider model identity is invalid")]
    InvalidModelIdentity,
    #[error("Provider replay metadata is invalid")]
    InvalidProviderReplay,
    #[error("Provider reasoning replay metadata is invalid")]
    InvalidReasoningReplay,
    #[error("Provider emitted a duplicate reasoning block")]
    DuplicateReasoningBlock,
    #[error("Provider referenced an unknown reasoning block")]
    UnknownReasoningBlock,
    #[error("Provider reasoning replay block is incomplete")]
    IncompleteReasoningReplay,
    #[error("Provider Tool identity is missing")]
    MissingToolIdentity,
    #[error("Provider Tool identity is invalid")]
    InvalidToolIdentity,
    #[error("Provider Tool identity changed during streaming")]
    ToolIdentityChanged,
    #[error("Provider referenced an unknown Tool call")]
    UnknownTool,
    #[error("Provider completed a Tool call twice")]
    DuplicateToolCompletion,
    #[error("Provider Tool arguments are malformed JSON")]
    MalformedToolJson,
    #[error("Provider emitted both snapshot and incremental Tool arguments")]
    ConflictingToolArguments,
    #[error("Provider Tool arguments must be a JSON object")]
    ToolArgumentsNotObject,
    #[error("Provider JSON value is invalid")]
    InvalidJson,
    #[error("Provider text violates canonical bounds")]
    InvalidText,
    #[error("Provider usage violates canonical accounting")]
    InvalidUsage,
    #[error("Provider stream budget was exceeded")]
    StreamBudgetExceeded,
    #[error("native compaction requires a configured content store")]
    NativeCompactionStoreRequired,
}

impl NormalizeError {
    pub(crate) const fn telemetry_kind(&self) -> musteragent_telemetry::ProviderProtocolError {
        use musteragent_telemetry::ProviderProtocolError as Kind;
        match self {
            Self::ObjectRequired => Kind::ObjectRequired,
            Self::ArrayRequired => Kind::ArrayRequired,
            Self::MissingField(_) => Kind::MissingField,
            Self::TextRequired => Kind::TextRequired,
            Self::UnsignedIntegerRequired => Kind::UnsignedIntegerRequired,
            Self::IndexOverflow => Kind::IndexOverflow,
            Self::MissingEventType => Kind::MissingEventType,
            Self::UnknownEventType => Kind::UnknownEventType,
            Self::UnknownContentBlock => Kind::UnknownContentBlock,
            Self::UnknownDeltaType => Kind::UnknownDeltaType,
            Self::UnknownStopReason => Kind::UnknownStopReason,
            Self::ClosedBeforeStart => Kind::ClosedBeforeStart,
            Self::ClosedBeforeTerminal => Kind::ClosedBeforeTerminal,
            Self::EventAfterTerminal => Kind::EventAfterTerminal,
            Self::ResponseIdentityChanged => Kind::ResponseIdentityChanged,
            Self::InvalidProviderReference => Kind::InvalidProviderReference,
            Self::InvalidModelIdentity => Kind::InvalidModelIdentity,
            Self::InvalidProviderReplay => Kind::InvalidProviderReplay,
            Self::InvalidReasoningReplay => Kind::InvalidReasoningReplay,
            Self::DuplicateReasoningBlock => Kind::DuplicateReasoningBlock,
            Self::UnknownReasoningBlock => Kind::UnknownReasoningBlock,
            Self::IncompleteReasoningReplay => Kind::IncompleteReasoningReplay,
            Self::MissingToolIdentity => Kind::MissingToolIdentity,
            Self::InvalidToolIdentity => Kind::InvalidToolIdentity,
            Self::ToolIdentityChanged => Kind::ToolIdentityChanged,
            Self::UnknownTool => Kind::UnknownTool,
            Self::DuplicateToolCompletion => Kind::DuplicateToolCompletion,
            Self::MalformedToolJson => Kind::MalformedToolJson,
            Self::ConflictingToolArguments => Kind::ConflictingToolArguments,
            Self::ToolArgumentsNotObject => Kind::ToolArgumentsNotObject,
            Self::InvalidJson => Kind::InvalidJson,
            Self::InvalidText => Kind::InvalidText,
            Self::InvalidUsage => Kind::InvalidUsage,
            Self::StreamBudgetExceeded => Kind::StreamBudgetExceeded,
            Self::NativeCompactionStoreRequired => Kind::NativeCompactionStoreRequired,
        }
    }
}
