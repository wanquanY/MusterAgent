//! The unpublished kernel accepts only its complete current wire contract.
use super::*;
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum SnapshotWireSchema {
    Current,
}
impl SnapshotWireSchema {
    pub(crate) fn parse(value: &str) -> Result<Self, SnapshotCodecError> {
        if value == SNAPSHOT_SCHEMA_VERSION {
            Ok(Self::Current)
        } else {
            Err(SnapshotCodecError::UnsupportedSchema)
        }
    }
}
impl RuntimeSnapshotDecoder for ProtobufSnapshotCodec {
    fn decode_runtime_snapshot(
        &self,
        bytes: &[u8],
    ) -> Result<RuntimeSnapshotContent, ContextPlanError> {
        self.decode_content(bytes)
            .map_err(|_| ContextPlanError::invariant())
    }
}
