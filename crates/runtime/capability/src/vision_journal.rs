//! Closed original Tool-owned auxiliary vision persistence over the Home Journal.
//! These proposals contain data; the original Native source and Home transaction
//! independently authorize the parent Tool, exact model route and content.
use musteragent_identity::InvocationId;
use musteragent_runtime_model::{AdmittedModelInvocation, CompleteModelInvocation};

#[derive(Debug, Clone, PartialEq)]
pub enum VisionJournalCommand {
    BeginSupervised {
        parent_invocation_id: InvocationId,
        request: Box<musteragent_runtime_model::SupervisedModelAdmission>,
    },
    CompleteSupervised {
        parent_invocation_id: InvocationId,
        request: Box<musteragent_runtime_model::SupervisedModelCompletion>,
    },
    Cooldown {
        parent_invocation_id: InvocationId,
        admission: Box<AdmittedModelInvocation>,
    },
    Begin {
        parent_invocation_id: InvocationId,
        admission: Box<AdmittedModelInvocation>,
    },
    Complete {
        parent_invocation_id: InvocationId,
        completion: Box<CompleteModelInvocation>,
    },
}
impl VisionJournalCommand {
    pub fn parent_invocation_id(&self) -> InvocationId {
        match self {
            Self::BeginSupervised {
                parent_invocation_id,
                ..
            }
            | Self::CompleteSupervised {
                parent_invocation_id,
                ..
            }
            | Self::Cooldown {
                parent_invocation_id,
                ..
            }
            | Self::Begin {
                parent_invocation_id,
                ..
            }
            | Self::Complete {
                parent_invocation_id,
                ..
            } => *parent_invocation_id,
        }
    }
}
