//! Admission-sealed publication protocol. Legacy response schemas remain frozen;
//! native tool publication has no response-body parser or fallback schema.
use crate::PublicDeliveryContract;
use musteragent_content_ref::{ContentRef, Sha256};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueOutputContract {
    Response(PublicDeliveryContract),
    NativeToolsV1,
    NativeToolsV2,
    /// V2 plus Runtime-published progress narration from preamble text.
    NativeToolsV3,
}

impl DialogueOutputContract {
    // New admissions use registered delivery tools. Recovery continues to use
    // the exact protocol sealed in its original snapshot, including V1–V7.
    pub const CURRENT: Self = Self::NativeToolsV3;

    /// A native turn that repeatedly stops without invoking a Tool cannot spin
    /// forever. Continuation segments do not count as completed responses.
    pub const NATIVE_UNPUBLISHED_RESPONSE_LIMIT: i32 = 3;

    pub const fn key(self) -> &'static str {
        match self {
            Self::Response(contract) => contract.key(),
            Self::NativeToolsV1 => "musteragent.tool-delivery@1",
            Self::NativeToolsV2 => "musteragent.tool-delivery@2",
            Self::NativeToolsV3 => "musteragent.tool-delivery@3",
        }
    }

    pub fn prompt(self) -> String {
        match self {
            Self::Response(contract) => contract.prompt(),
            Self::NativeToolsV1 => include_str!("tool_delivery_instruction_v1.txt").to_owned(),
            Self::NativeToolsV2 => include_str!("tool_delivery_instruction_v2.txt").to_owned(),
            Self::NativeToolsV3 => include_str!("tool_delivery_instruction_v3.txt").to_owned(),
        }
    }

    pub const fn response_contract(self) -> Option<PublicDeliveryContract> {
        match self {
            Self::Response(contract) => Some(contract),
            Self::NativeToolsV1 | Self::NativeToolsV2 | Self::NativeToolsV3 => None,
        }
    }

    pub const fn uses_tool_result_delivery_references(self) -> bool {
        match self {
            Self::Response(contract) => contract.uses_tool_result_delivery_references(),
            Self::NativeToolsV1 | Self::NativeToolsV2 | Self::NativeToolsV3 => true,
        }
    }

    /// Both instruction versions use the same native publication Tool wire
    /// protocol. They differ only in the immutable behavior sealed at admission.
    pub const fn is_native(self) -> bool {
        matches!(
            self,
            Self::NativeToolsV1 | Self::NativeToolsV2 | Self::NativeToolsV3
        )
    }

    /// Text written alongside Tool calls is published as progress narration.
    pub const fn publishes_preamble_progress(self) -> bool {
        matches!(self, Self::NativeToolsV3)
    }

    pub fn native_from_key(key: &str) -> Option<Self> {
        [
            Self::NativeToolsV1,
            Self::NativeToolsV2,
            Self::NativeToolsV3,
        ]
        .into_iter()
        .find(|v| v.key() == key)
    }

    pub fn from_prompt_ref(reference: &ContentRef) -> Option<Self> {
        [
            Self::NativeToolsV1,
            Self::NativeToolsV2,
            Self::NativeToolsV3,
        ]
        .into_iter()
        .find(|v| {
            let prompt = v.prompt();
            reference.sha256 == Sha256::digest(prompt.as_bytes())
                && reference.byte_length == prompt.len() as u64
        })
        .or_else(|| PublicDeliveryContract::from_prompt_ref(reference).map(Self::Response))
    }
}

impl From<PublicDeliveryContract> for DialogueOutputContract {
    fn from(value: PublicDeliveryContract) -> Self {
        Self::Response(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_protocol_has_no_final_response_schema() {
        assert_eq!(
            DialogueOutputContract::NativeToolsV1.response_contract(),
            None
        );
        assert_ne!(
            DialogueOutputContract::NativeToolsV1.key(),
            PublicDeliveryContract::V7.key()
        );
    }

    #[test]
    fn sealed_native_versions_recover_independently() {
        use musteragent_content_ref::MediaType;
        use musteragent_identity::ContentId;
        assert_eq!(
            DialogueOutputContract::CURRENT,
            DialogueOutputContract::NativeToolsV3
        );
        assert_ne!(
            DialogueOutputContract::NativeToolsV1.prompt(),
            DialogueOutputContract::CURRENT.prompt()
        );
        for version in [
            DialogueOutputContract::NativeToolsV1,
            DialogueOutputContract::NativeToolsV2,
            DialogueOutputContract::NativeToolsV3,
        ] {
            let prompt = version.prompt();
            let mut reference = ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain").unwrap(),
                byte_length: prompt.len() as u64,
                sha256: Sha256::digest(prompt.as_bytes()),
            };
            assert_eq!(
                DialogueOutputContract::from_prompt_ref(&reference),
                Some(version)
            );
            assert_eq!(
                DialogueOutputContract::native_from_key(version.key()),
                Some(version)
            );
            assert!(version.is_native());
            assert!(version.response_contract().is_none());
            reference.byte_length += 1;
            assert!(DialogueOutputContract::from_prompt_ref(&reference).is_none());
        }
        assert!(DialogueOutputContract::native_from_key("musteragent.tool-delivery@4").is_none());
        assert!(DialogueOutputContract::NativeToolsV3.publishes_preamble_progress());
        assert!(!DialogueOutputContract::NativeToolsV2.publishes_preamble_progress());
    }

    #[test]
    fn legacy_contract_bytes_are_preserved() {
        for contract in [
            PublicDeliveryContract::V1,
            PublicDeliveryContract::V2,
            PublicDeliveryContract::V3,
            PublicDeliveryContract::V4,
            PublicDeliveryContract::V5,
            PublicDeliveryContract::V6,
            PublicDeliveryContract::V7,
        ] {
            let output = DialogueOutputContract::from(contract);
            assert_eq!(output.prompt(), contract.prompt());
            assert_eq!(output.response_contract(), Some(contract));
            assert_eq!(output.key(), contract.key());
        }
    }
}
