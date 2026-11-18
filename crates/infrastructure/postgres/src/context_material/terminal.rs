use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{ContextBranchId, ConversationSeq, LedgerRecordId};

pub(super) struct ToolTerminalSource {
    pub(super) sequence: ConversationSeq,
    pub(super) record_id: LedgerRecordId,
    pub(super) outcome: ToolTerminalSourceKind,
    pub(super) branch_id: Option<ContextBranchId>,
}

pub(super) enum ToolTerminalSourceKind {
    Succeeded {
        result_ref: ContentRef,
    },
    Failed {
        diagnostic_result_ref: Option<ContentRef>,
    },
    Cancelled,
    Indeterminate {
        diagnostic_result_ref: Option<ContentRef>,
        reason: musteragent_runtime_contract::SafeDisplayText,
        observed_error: Option<musteragent_runtime_contract::ErrorInfo>,
    },
}

pub(super) fn terminal_result_matches(
    terminal: &ToolTerminalSourceKind,
    persisted_status: i16,
    capability_result_ref: Option<&ContentRef>,
    capability_result_hash: Option<Sha256>,
) -> bool {
    match (
        terminal,
        persisted_status,
        capability_result_ref,
        capability_result_hash,
    ) {
        (
            ToolTerminalSourceKind::Succeeded { result_ref },
            4,
            Some(capability_ref),
            Some(capability_hash),
        ) => result_ref == capability_ref && capability_hash == capability_ref.sha256,
        (
            ToolTerminalSourceKind::Failed {
                diagnostic_result_ref: Some(diagnostic_ref),
            },
            5,
            Some(capability_ref),
            Some(capability_hash),
        ) => diagnostic_ref == capability_ref && capability_hash == capability_ref.sha256,
        (
            ToolTerminalSourceKind::Failed {
                diagnostic_result_ref: None,
            },
            5,
            None,
            None,
        )
        | (ToolTerminalSourceKind::Cancelled, 7, None, None) => true,
        (
            ToolTerminalSourceKind::Indeterminate {
                diagnostic_result_ref,
                ..
            },
            6,
            reference,
            hash,
        ) => {
            diagnostic_result_ref.as_ref() == reference
                && reference.map(|value| value.sha256) == hash
        }
        _ => false,
    }
}
