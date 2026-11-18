//! Walk trusted IDL before Prost allocates untrusted repeated messages.
//! Content stays borrowed throughout this preflight. Profiles are explicit so
//! adding a richer port cannot silently broaden an existing wire contract.
use prost_reflect::{DescriptorPool, Kind, MessageDescriptor};
use std::{num::NonZeroUsize, sync::OnceLock};

#[derive(Debug, Clone, Copy)]
pub enum BudgetError {
    Bounds,
    Invalid,
    Schema,
}
type Result<T> = std::result::Result<T, BudgetError>;
static SCHEMA: OnceLock<Result<DescriptorPool>> = OnceLock::new();

#[derive(Clone, Copy)]
pub enum Profile {
    Erasure,
    Journal,
    Context,
    LedgerReferences,
    SnapshotReferences,
    SnapshotComponent(crate::RuntimeSnapshotComponentKind),
}
impl Profile {
    fn fields(self) -> usize {
        match self {
            Self::Journal => 16_384,
            Self::Erasure
            | Self::Context
            | Self::SnapshotReferences
            | Self::SnapshotComponent(_) => 65_536,
            // A field consumes at least one payload byte. Keep the original
            // Ledger byte contract instead of imposing a new smaller limit.
            Self::LedgerReferences => musteragent_ledger::MAX_RECORD_PAYLOAD_BYTES,
        }
    }
    fn depth(self) -> usize {
        match self {
            Self::Journal | Self::Erasure => 16,
            Self::Context
            | Self::LedgerReferences
            | Self::SnapshotReferences
            | Self::SnapshotComponent(_) => 32,
        }
    }
    fn packed(self) -> bool {
        matches!(
            self,
            Self::Erasure
                | Self::Context
                | Self::LedgerReferences
                | Self::SnapshotReferences
                | Self::SnapshotComponent(_)
        )
    }
}
pub fn check(bytes: &[u8], maximum: NonZeroUsize, message: &str, profile: Profile) -> Result<()> {
    visit_messages(bytes, maximum, message, profile, &mut |_, _| Ok(()))
}

/// Visits borrowed messages after validating their fields. A visitor failure
/// invalidates the entire result; callers must not publish partial projections.
pub(crate) fn visit_messages(
    bytes: &[u8],
    maximum: NonZeroUsize,
    message: &str,
    profile: Profile,
    visitor: &mut impl FnMut(&str, &[u8]) -> Result<()>,
) -> Result<()> {
    if (bytes.is_empty() && !matches!(profile, Profile::SnapshotComponent(_)))
        || bytes.len() > maximum.get()
    {
        return Err(BudgetError::Bounds);
    }
    let pool = SCHEMA.get_or_init(|| {
        DescriptorPool::decode(crate::FILE_DESCRIPTOR_SET).map_err(|_| BudgetError::Schema)
    });
    let descriptor = pool
        .as_ref()
        .map_err(|e| *e)?
        .get_message_by_name(message)
        .ok_or(BudgetError::Schema)?;
    walk(
        bytes,
        descriptor,
        profile,
        0,
        &mut profile.fields(),
        visitor,
    )
}
fn walk(
    mut bytes: &[u8],
    descriptor: MessageDescriptor,
    profile: Profile,
    depth: usize,
    remaining: &mut usize,
    visitor: &mut impl FnMut(&str, &[u8]) -> Result<()>,
) -> Result<()> {
    let original = bytes;
    if depth > profile.depth() {
        return Err(BudgetError::Bounds);
    }
    let mut last = 0_u32;
    let mut oneofs = 0_u64;
    while !bytes.is_empty() {
        consume(remaining)?;
        let tag = varint(&mut bytes)?;
        let number = u32::try_from(tag >> 3).map_err(|_| BudgetError::Invalid)?;
        let field = descriptor.get_field(number).ok_or(BudgetError::Invalid)?;
        if depth == 0
            && let Profile::SnapshotComponent(kind) = profile
            && !kind.contains_field(number)
        {
            return Err(BudgetError::Invalid);
        }
        // Prost places a oneof at its lowest declared tag, even when the
        // selected variant has a later tag. Match that canonical encoder
        // position without permitting arbitrary order or multiple variants.
        // Otherwise additive variants (for example checkpoint Draining=18 in
        // the phase oneof starting at 5) reject their own original wire bytes.
        let order = field
            .containing_oneof()
            .map(|oneof| oneof.fields().map(|member| member.number()).min())
            .unwrap_or(Some(number))
            .ok_or(BudgetError::Schema)?;
        if field.is_map() || order < last || (order == last && !field.is_list()) {
            return Err(BudgetError::Invalid);
        }
        last = order;
        if let Some(index) = field.field_descriptor_proto().oneof_index {
            let index = u32::try_from(index).map_err(|_| BudgetError::Schema)?;
            let bit = 1_u64.checked_shl(index).ok_or(BudgetError::Schema)?;
            if oneofs & bit != 0 {
                return Err(BudgetError::Invalid);
            }
            oneofs |= bit;
        }
        match (tag & 7, field.kind()) {
            (0, kind) if is_varint(&kind) => {
                varint(&mut bytes)?;
            }
            (1, Kind::Double | Kind::Fixed64 | Kind::Sfixed64) => {
                take(&mut bytes, 8)?;
            }
            (5, Kind::Float | Kind::Fixed32 | Kind::Sfixed32) => {
                take(&mut bytes, 4)?;
            }
            (2, kind) => {
                let length =
                    usize::try_from(varint(&mut bytes)?).map_err(|_| BudgetError::Bounds)?;
                let value = take(&mut bytes, length)?;
                match kind {
                    Kind::Message(child) => {
                        walk(value, child, profile, depth + 1, remaining, visitor)?;
                    }
                    Kind::String => {
                        std::str::from_utf8(value).map_err(|_| BudgetError::Invalid)?;
                    }
                    Kind::Bytes => {}
                    kind if (profile.packed()
                        || matches!(profile, Profile::Journal)
                            && descriptor.full_name()
                                == "musteragent.v1.ExecutionLivenessPolicyV1")
                        && field.is_list()
                        && field.is_packed() =>
                    {
                        packed(value, kind, remaining)?;
                    }
                    _ => return Err(BudgetError::Invalid),
                }
            }
            _ => return Err(BudgetError::Invalid),
        }
    }
    visitor(descriptor.full_name(), original)
}
fn packed(mut bytes: &[u8], kind: Kind, remaining: &mut usize) -> Result<()> {
    // Count every element, not only its containing length-delimited field.
    // The typed codec must also re-encode to reject noncanonical segments.
    while !bytes.is_empty() {
        consume(remaining)?;
        match &kind {
            kind if is_varint(kind) => {
                varint(&mut bytes)?;
            }
            Kind::Double | Kind::Fixed64 | Kind::Sfixed64 => {
                take(&mut bytes, 8)?;
            }
            Kind::Float | Kind::Fixed32 | Kind::Sfixed32 => {
                take(&mut bytes, 4)?;
            }
            _ => return Err(BudgetError::Invalid),
        }
    }
    Ok(())
}
fn is_varint(kind: &Kind) -> bool {
    matches!(
        kind,
        Kind::Bool
            | Kind::Enum(_)
            | Kind::Int32
            | Kind::Int64
            | Kind::Uint32
            | Kind::Uint64
            | Kind::Sint32
            | Kind::Sint64
    )
}
fn consume(remaining: &mut usize) -> Result<()> {
    *remaining = remaining.checked_sub(1).ok_or(BudgetError::Bounds)?;
    Ok(())
}
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    let head = bytes.get(..n).ok_or(BudgetError::Invalid)?;
    *bytes = &bytes[n..];
    Ok(head)
}
fn varint(bytes: &mut &[u8]) -> Result<u64> {
    let mut result = 0_u64;
    for i in 0..10_u32 {
        let byte = *take(bytes, 1)?.first().ok_or(BudgetError::Invalid)?;
        if i == 9 && byte > 1 {
            return Err(BudgetError::Invalid);
        }
        result |= u64::from(byte & 0x7f) << (i * 7);
        if byte & 0x80 == 0 {
            if i > 0 && byte == 0 {
                return Err(BudgetError::Invalid);
            }
            return Ok(result);
        }
    }
    Err(BudgetError::Invalid)
}

#[cfg(test)]
mod tests;

/// Validate an RPC message against its immutable descriptor pool using the same budgets as persistence.
pub fn check_descriptor(
    bytes: &[u8],
    maximum: NonZeroUsize,
    message: &str,
    profile: Profile,
    descriptors: &'static [u8],
    cache: &OnceLock<std::result::Result<DescriptorPool, BudgetError>>,
) -> Result<()> {
    if bytes.is_empty() || bytes.len() > maximum.get() {
        return Err(BudgetError::Bounds);
    }
    let pool =
        cache.get_or_init(|| DescriptorPool::decode(descriptors).map_err(|_| BudgetError::Schema));
    let descriptor = pool
        .as_ref()
        .map_err(|e| *e)?
        .get_message_by_name(message)
        .ok_or(BudgetError::Schema)?;
    walk(
        bytes,
        descriptor,
        profile,
        0,
        &mut profile.fields(),
        &mut |_, _| Ok(()),
    )
}
