//! The exact dialogue.output-contract bytes identify the kind of model turn.
//! Only the Dialogue branch is a contract accepted by direct publication.
use crate::{DialogueOutputContract, PublicDeliveryContract};
use musteragent_content_ref::{ContentRef, Sha256};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemResultStep {
    SubmitV1,
    ReturnToRequester,
    VerifyV1,
}

impl WorkItemResultStep {
    pub const fn instruction(self) -> &'static str {
        match self {
            Self::SubmitV1 => {
                "Prepare the exact requested answer or files privately. Submit it once with tasks_submit_result and wait for internal verification. Do not publish a preview or expose a current artifact head."
            }
            Self::ReturnToRequester => {
                "Return the requested answer or files privately with tasks_submit_result. When the requester sends feedback, use the previous result and the specific changes to revise your work."
            }
            Self::VerifyV1 => {
                "Read the exact returned report and its private evidence. Use tasks_verify on the actual submitted result, assessing each original criterion. Acceptance delivers those checked items once. A failed Run or missing result is not evidence of success; arrange permitted repair or identify the blocker. Do not publish a second result."
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RunOutputContract {
    Dialogue(DialogueOutputContract),
    TaskResult(WorkItemResultStep),
}

impl RunOutputContract {
    pub fn from_prompt_ref(reference: &ContentRef) -> Option<Self> {
        for step in [
            WorkItemResultStep::SubmitV1,
            WorkItemResultStep::ReturnToRequester,
            WorkItemResultStep::VerifyV1,
        ] {
            let bytes = step.instruction().as_bytes();
            if reference.byte_length == bytes.len() as u64
                && reference.sha256 == Sha256::digest(bytes)
            {
                return Some(Self::TaskResult(step));
            }
        }
        DialogueOutputContract::from_prompt_ref(reference).map(Self::Dialogue)
    }

    pub fn prompt(self) -> String {
        match self {
            Self::Dialogue(contract) => contract.prompt(),
            Self::TaskResult(step) => step.instruction().to_owned(),
        }
    }

    pub const fn response_contract(self) -> Option<PublicDeliveryContract> {
        match self {
            Self::Dialogue(contract) => contract.response_contract(),
            Self::TaskResult(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::MediaType;
    use musteragent_identity::ContentId;

    #[test]
    fn private_steps_recover_their_exact_bytes_and_never_authorize_direct_publication() {
        for step in [
            WorkItemResultStep::SubmitV1,
            WorkItemResultStep::ReturnToRequester,
            WorkItemResultStep::VerifyV1,
        ] {
            let bytes = step.instruction().as_bytes();
            let reference = ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain;charset=utf-8").unwrap(),
                byte_length: bytes.len() as u64,
                sha256: Sha256::digest(bytes),
            };
            let sealed = RunOutputContract::from_prompt_ref(&reference).unwrap();
            assert_eq!(sealed, RunOutputContract::TaskResult(step));
            assert_eq!(sealed.prompt().as_bytes(), bytes);
            assert_eq!(sealed.response_contract(), None);
            assert!(DialogueOutputContract::from_prompt_ref(&reference).is_none());
            for changed in [
                ContentRef {
                    byte_length: reference.byte_length + 1,
                    ..reference.clone()
                },
                ContentRef {
                    sha256: Sha256::digest(b"unsealed role instruction"),
                    ..reference.clone()
                },
            ] {
                assert!(RunOutputContract::from_prompt_ref(&changed).is_none());
            }
        }
    }
}
