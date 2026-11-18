//! Strong identities and monotonic scalar types shared by every bounded context.

#![forbid(unsafe_code)]

mod product_release;
pub use product_release::{ProductId, ProductReleaseIdentityError, ProductReleaseRef};

use std::fmt::{Display, Formatter};
use std::num::{NonZeroU32, NonZeroU64};
use std::str::FromStr;

use thiserror::Error;
use uuid::Uuid;

/// An invalid typed identifier.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum IdentityError {
    /// Nil UUIDs are never valid domain identities.
    #[error("{type_name} cannot be nil")]
    Nil { type_name: &'static str },
    /// The wire value was not a UUID.
    #[error("{type_name} is not a valid UUID: {source}")]
    InvalidUuid {
        type_name: &'static str,
        source: uuid::Error,
    },
}

macro_rules! uuid_id {
    ($name:ident) => {
        #[doc = concat!("Strong UUID identity for `", stringify!($name), "`.")]
        #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a time-ordered UUIDv7 for a new Runtime-owned entity.
            #[must_use]
            pub fn generate() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wraps an authoritative or imported UUID without rewriting it.
            pub fn from_uuid(value: Uuid) -> Result<Self, IdentityError> {
                if value.is_nil() {
                    return Err(IdentityError::Nil {
                        type_name: stringify!($name),
                    });
                }
                Ok(Self(value))
            }

            /// Returns the underlying UUID.
            #[must_use]
            pub const fn into_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                Display::fmt(&self.0, formatter)
            }
        }

        impl FromStr for $name {
            type Err = IdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let parsed =
                    Uuid::parse_str(value).map_err(|source| IdentityError::InvalidUuid {
                        type_name: stringify!($name),
                        source,
                    })?;
                Self::from_uuid(parsed)
            }
        }
    };
}

uuid_id!(TenantId);
uuid_id!(PrincipalId);
uuid_id!(ConversationId);
uuid_id!(ParticipantId);
uuid_id!(MessageId);
uuid_id!(WorkspaceId);
uuid_id!(ActivityId);
uuid_id!(RunId);
uuid_id!(AttemptId);
uuid_id!(InteractionId);
uuid_id!(InvocationId);
uuid_id!(AuthorizationReceiptId);
uuid_id!(DeviceResourceId);
uuid_id!(DeviceResourceGrantId);
uuid_id!(DeviceResourceLeaseId);
uuid_id!(DelegationId);
uuid_id!(DelegationDeliveryId);
uuid_id!(DelegationGroupId);
uuid_id!(AuditRecordId);
uuid_id!(ErasureJobId);
uuid_id!(ErasureReceiptId);
uuid_id!(LedgerRecordId);
uuid_id!(FrameId);
uuid_id!(OutputId);
uuid_id!(RuntimeSnapshotId);
uuid_id!(ContextSnapshotId);
uuid_id!(ContextBranchId);
uuid_id!(ExecutionWorkspaceId);
uuid_id!(ProjectionSnapshotId);
uuid_id!(RuntimeDataTicketId);
uuid_id!(AgentId);
uuid_id!(OwnerSpaceId);
uuid_id!(AgentDialogueId);
uuid_id!(DialogueContextCheckpointId);
uuid_id!(OwnerApplicationBindingId);
uuid_id!(SourceCaptureId);
uuid_id!(SourceEventId);
uuid_id!(SourceBoundaryId);
uuid_id!(DialogueEntryId);
uuid_id!(UserIntentId);
uuid_id!(AgentVersionId);
uuid_id!(MemoryCollectionId);
uuid_id!(SkillId);
uuid_id!(SkillVersionId);
uuid_id!(SkillPackagePreviewId);
uuid_id!(ArtifactId);
uuid_id!(ArtifactVersionId);
uuid_id!(ArtifactProposalId);
uuid_id!(ResourceNodeId);
uuid_id!(ResourceEditSessionId);
uuid_id!(ResourceEditOperationId);
uuid_id!(ResourceEditTerminalCommandId);
uuid_id!(ResourceChangeSetId);
uuid_id!(NoteAuthoringSessionId);
uuid_id!(NoteSegmentId);
uuid_id!(NoteBlockId);
uuid_id!(MemoryId);
uuid_id!(MemoryVersionId);
uuid_id!(MemoryDraftId);
uuid_id!(MemoryDraftBatchId);
uuid_id!(CapabilityId);
uuid_id!(CapabilityProviderId);
uuid_id!(ToolDefinitionId);
uuid_id!(LearningSignalId);
uuid_id!(OwnerSourceGrantId);
uuid_id!(OwnerEventSubscriptionId);
uuid_id!(EvolutionProposalId);
uuid_id!(EvolutionEvaluationId);
uuid_id!(EvolutionPromotionId);
uuid_id!(DreamJobId);
uuid_id!(InstinctId);
uuid_id!(StudioRevisionDraftId);
uuid_id!(GoalId);
uuid_id!(GoalPursuitId);
uuid_id!(GoalEvaluationId);
uuid_id!(ProjectId);
uuid_id!(LoginSessionId);
uuid_id!(HomeBindingId);
uuid_id!(ClientSessionId);
uuid_id!(ProjectMembershipId);
uuid_id!(RunAuthorizationId);
uuid_id!(ExecutionAssignmentId);
uuid_id!(ExecutionRecoveryId);
uuid_id!(ExecutorInstanceId);
uuid_id!(ExecutorConsentId);
uuid_id!(AuthorityRequestId);
uuid_id!(HostClockEpochId);
uuid_id!(WorkItemId);
uuid_id!(TaskActionId);
uuid_id!(AssignmentId);
uuid_id!(InterruptRequestId);
uuid_id!(InterSessionMessageId);
uuid_id!(ScheduleOccurrenceId);
uuid_id!(OutboxMessageId);
uuid_id!(ContentId);
uuid_id!(CommandId);
uuid_id!(CorrelationId);
uuid_id!(DeviceId);
// Local installation identity never substitutes for an enrolled account device.
uuid_id!(DeviceInstallationId);
uuid_id!(LeaseId);
uuid_id!(BrowserLeaseId);
uuid_id!(BrowserGatewayTicketId);
uuid_id!(BrowserSessionId);
uuid_id!(PageInstanceId);
uuid_id!(BrowserJobId);

/// Authentication principal category. It is not a Conversation participant role.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum PrincipalKind {
    User,
    Service,
    System,
}

/// Authenticated command actor. Tenant and principal remain authoritative IDs.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Actor {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub principal_kind: PrincipalKind,
}

/// A non-zero scalar violated its contract.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("{type_name} must be greater than zero")]
pub struct NonZeroScalarError {
    type_name: &'static str,
}

macro_rules! non_zero_u64 {
    ($name:ident) => {
        #[doc = concat!("Non-zero monotonic scalar for `", stringify!($name), "`.")]
        #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(NonZeroU64);

        impl $name {
            /// Validates and creates the scalar.
            pub fn new(value: u64) -> Result<Self, NonZeroScalarError> {
                NonZeroU64::new(value).map(Self).ok_or(NonZeroScalarError {
                    type_name: stringify!($name),
                })
            }

            /// Returns the primitive value.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }
    };
}

non_zero_u64!(ConversationSeq);
non_zero_u64!(RunSeq);
non_zero_u64!(HomeEpoch);
non_zero_u64!(PeerPathGeneration);
non_zero_u64!(RunAttemptEpoch);
non_zero_u64!(FencingToken);
non_zero_u64!(VersionNumber);
non_zero_u64!(ProjectRevision);
non_zero_u64!(AssignmentEpoch);
non_zero_u64!(BrowserLeaseEpoch);

/// Monotonic Project/Conversation resource-tree revision.
///
/// Zero is the authoritative empty-tree revision, so this cannot use the
/// non-zero scalar macro shared by Artifact versions and fencing tokens.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ResourceRevision(u64);

impl ResourceRevision {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Result<Self, SequenceOverflowError> {
        self.0.checked_add(1).map(Self).ok_or(SequenceOverflowError)
    }
}

/// A zero-based position after the last consumed Conversation record.
///
/// Unlike `ConversationSeq`, zero is meaningful and means "before the first
/// record". Keeping this type separate prevents an impossible initial cursor.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LedgerOffset(u64);

impl LedgerOffset {
    pub const ZERO: Self = Self(0);

    /// Creates an offset from its wire value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wire value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Allocates the next non-zero Conversation sequence.
    pub fn next_sequence(self) -> Result<ConversationSeq, SequenceOverflowError> {
        self.0
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(ConversationSeq)
            .ok_or(SequenceOverflowError)
    }
}

impl From<ConversationSeq> for LedgerOffset {
    fn from(value: ConversationSeq) -> Self {
        Self(value.get())
    }
}

/// A monotonic sequence exhausted the `u64` namespace.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("conversation sequence overflow")]
pub struct SequenceOverflowError;

/// UTC wall-clock time represented as Unix epoch milliseconds.
///
/// Time is observational and must never be used as Ledger ordering.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UtcTimestamp(i64);

impl UtcTimestamp {
    /// Creates a timestamp from Unix epoch milliseconds.
    #[must_use]
    pub const fn from_unix_millis(value: i64) -> Self {
        Self(value)
    }

    /// Returns Unix epoch milliseconds.
    #[must_use]
    pub const fn unix_millis(self) -> i64 {
        self.0
    }

    /// Adds a non-negative duration without exposing a platform clock type.
    #[must_use]
    pub const fn checked_add_millis(self, millis: i64) -> Option<Self> {
        if millis < 0 {
            return None;
        }
        match self.0.checked_add(millis) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Subtracts a non-negative duration without exposing a platform clock type.
    #[must_use]
    pub const fn checked_sub_millis(self, millis: i64) -> Option<Self> {
        if millis < 0 {
            return None;
        }
        match self.0.checked_sub(millis) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// A one-based attempt number.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AttemptNumber(NonZeroU32);

impl AttemptNumber {
    /// Validates and creates an attempt number.
    pub fn new(value: u32) -> Result<Self, NonZeroScalarError> {
        NonZeroU32::new(value).map(Self).ok_or(NonZeroScalarError {
            type_name: "AttemptNumber",
        })
    }

    /// Returns the primitive value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// An invalid stable logical key.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum LogicalKeyError {
    /// The key was empty.
    #[error("logical key cannot be empty")]
    Empty,
    /// The key did not use the closed lowercase ASCII grammar.
    #[error(
        "logical key must start with a lowercase letter and contain only lowercase ASCII, digits, '-' or '_'"
    )]
    InvalidGrammar,
    /// A versioned key did not contain a numeric `@version` suffix.
    #[error("versioned key must end with a positive numeric '@version' suffix")]
    InvalidVersion,
    /// The key exceeded its protocol bound.
    #[error("logical key exceeds its maximum length")]
    TooLong,
}

/// A stable Agent logical key, distinct from an Agent version.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AgentKey(String);

impl AgentKey {
    /// Validates a stable logical key.
    pub fn new(value: impl Into<String>) -> Result<Self, LogicalKeyError> {
        let value = value.into();
        validate_key_body(&value)?;
        Ok(Self(value))
    }

    /// Returns the wire representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A stable unversioned dotted key such as `workspace.file.read`.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DottedKey(String);

impl DottedKey {
    /// Validates an unversioned dotted key.
    pub fn new(value: impl Into<String>) -> Result<Self, LogicalKeyError> {
        let value = value.into();
        if value.len() > 160 {
            return Err(LogicalKeyError::TooLong);
        }
        validate_dotted_key_body(&value)?;
        Ok(Self(value))
    }

    /// Returns the wire representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A logical key with an explicit positive `@version` suffix.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VersionedKey(String);

impl VersionedKey {
    /// Validates a versioned key such as `web.search@2`.
    pub fn new(value: impl Into<String>) -> Result<Self, LogicalKeyError> {
        let value = value.into();
        if value.len() > 192 {
            return Err(LogicalKeyError::TooLong);
        }
        let (body, version) = value
            .rsplit_once('@')
            .ok_or(LogicalKeyError::InvalidVersion)?;
        validate_dotted_key_body(body)?;
        if version.is_empty() || !version.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(LogicalKeyError::InvalidVersion);
        }
        let version = version
            .parse::<u32>()
            .map_err(|_| LogicalKeyError::InvalidVersion)?;
        if version == 0 {
            return Err(LogicalKeyError::InvalidVersion);
        }
        Ok(Self(value))
    }

    /// Returns the wire representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the stable logical identity without its immutable release
    /// suffix. Runtime implementations bind to this identity (or a Tool wire
    /// name); persisted Snapshots continue to pin the complete versioned key.
    #[must_use]
    pub fn logical_key(&self) -> &str {
        self.0
            .rsplit_once('@')
            .expect("VersionedKey construction proves an @version suffix")
            .0
    }

    /// Returns the positive immutable release number.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.0
            .rsplit_once('@')
            .expect("VersionedKey construction proves an @version suffix")
            .1
            .parse()
            .expect("VersionedKey construction proves a u32 version")
    }
}

/// A bounded opaque idempotency key supplied by a command producer.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Accepts visible ASCII without whitespace, bounded for indexes and logs.
    pub fn new(value: impl Into<String>) -> Result<Self, IdempotencyKeyError> {
        let value = value.into();
        if value.is_empty() {
            return Err(IdempotencyKeyError::Empty);
        }
        if value.len() > 200 {
            return Err(IdempotencyKeyError::TooLong);
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !byte.is_ascii_whitespace())
        {
            return Err(IdempotencyKeyError::InvalidCharacter);
        }
        Ok(Self(value))
    }

    /// Returns the opaque wire value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An idempotency key violated its bounded wire contract.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum IdempotencyKeyError {
    #[error("idempotency key cannot be empty")]
    Empty,
    #[error("idempotency key exceeds 200 bytes")]
    TooLong,
    #[error("idempotency key must contain visible ASCII without whitespace")]
    InvalidCharacter,
}

/// Opaque model/provider Tool call correlation ID.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ToolCallId(String);

impl ToolCallId {
    /// Accepts a bounded visible-ASCII provider identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, ToolCallIdError> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 {
            return Err(ToolCallIdError::InvalidLength);
        }
        if !value.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(ToolCallIdError::InvalidCharacter);
        }
        Ok(Self(value))
    }

    /// Returns the opaque wire value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A Tool call ID violated its provider-safe bound.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ToolCallIdError {
    #[error("Tool call ID must contain 1..=256 bytes")]
    InvalidLength,
    #[error("Tool call ID must contain visible ASCII")]
    InvalidCharacter,
}

fn validate_key_body(value: &str) -> Result<(), LogicalKeyError> {
    if value.is_empty() {
        return Err(LogicalKeyError::Empty);
    }
    let mut characters = value.chars();
    if !characters
        .next()
        .is_some_and(|character| character.is_ascii_lowercase())
    {
        return Err(LogicalKeyError::InvalidGrammar);
    }
    if characters.all(|character| {
        character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || character == '-'
            || character == '_'
    }) {
        Ok(())
    } else {
        Err(LogicalKeyError::InvalidGrammar)
    }
}

fn validate_dotted_key_body(value: &str) -> Result<(), LogicalKeyError> {
    if value
        .split('.')
        .all(|segment| validate_key_body(segment).is_ok())
    {
        Ok(())
    } else {
        Err(LogicalKeyError::InvalidGrammar)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AgentKey, ConversationId, ConversationSeq, DottedKey, IdempotencyKey, LedgerOffset,
        UtcTimestamp, VersionedKey,
    };
    use uuid::Version;

    #[test]
    fn runtime_ids_are_uuid_v7() {
        assert_eq!(
            ConversationId::generate().into_uuid().get_version(),
            Some(Version::SortRand)
        );
    }

    #[test]
    fn logical_keys_are_closed() {
        assert!(AgentKey::new("assistant").is_ok());
        assert!(AgentKey::new("Assistant").is_err());
        assert!(DottedKey::new("workspace.file.read").is_ok());
        let versioned = VersionedKey::new("web.search@2").expect("versioned key");
        assert_eq!(versioned.logical_key(), "web.search");
        assert_eq!(versioned.version(), 2);
        assert!(VersionedKey::new("web.search").is_err());
        assert!(VersionedKey::new("web.search@+2").is_err());
        assert!(IdempotencyKey::new("submit:018f-123").is_ok());
        assert!(IdempotencyKey::new("contains whitespace").is_err());
    }

    #[test]
    fn zero_offset_allocates_the_first_sequence() {
        let first = LedgerOffset::ZERO.next_sequence().expect("first sequence");
        assert_eq!(first.get(), 1);
        assert_eq!(LedgerOffset::from(first), LedgerOffset::new(1));
        assert!(ConversationSeq::new(0).is_err());
    }

    #[test]
    fn timestamp_checked_arithmetic_rejects_negative_durations_and_overflow() {
        let timestamp = UtcTimestamp::from_unix_millis(10_000);
        assert_eq!(
            timestamp.checked_add_millis(250),
            Some(UtcTimestamp::from_unix_millis(10_250))
        );
        assert_eq!(
            timestamp.checked_sub_millis(250),
            Some(UtcTimestamp::from_unix_millis(9_750))
        );
        assert_eq!(timestamp.checked_add_millis(-1), None);
        assert_eq!(timestamp.checked_sub_millis(-1), None);
        assert_eq!(
            UtcTimestamp::from_unix_millis(i64::MIN).checked_sub_millis(1),
            None
        );
    }
}
