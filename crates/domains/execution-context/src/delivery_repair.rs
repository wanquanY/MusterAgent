//! Private repair is part of the sealed Dialogue delivery contract, not a new
//! Run owner or a configurable Work execution policy.
use crate::{
    PublicDeliveryContract, PublicDeliveryDiagnostic, PublicDeliveryDiagnosticKind,
    PublicDeliveryRepairError, public_delivery_diagnostics_hash,
};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{ConversationSeq, InvocationId, MessageId, OutputId, VersionedKey};

pub const PUBLIC_DELIVERY_REPAIR_REF: &str = "musteragent.private-delivery-repair@1";
pub const PUBLIC_DELIVERY_REPAIR_SPEC: &str = include_str!("delivery_repair_v1.json");
pub fn public_delivery_repair_hash() -> Sha256 {
    Sha256::digest(PUBLIC_DELIVERY_REPAIR_SPEC.as_bytes())
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PublicDeliveryRepairDisposition {
    FormatOnly,
    PrerequisiteRecovery,
}
impl PublicDeliveryRepairDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FormatOnly => "format_only",
            Self::PrerequisiteRecovery => "prerequisite_recovery",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "format_only" => Some(Self::FormatOnly),
            "prerequisite_recovery" => Some(Self::PrerequisiteRecovery),
            _ => None,
        }
    }
    pub fn for_diagnostics(
        issues: &[PublicDeliveryDiagnostic],
    ) -> Result<Self, PublicDeliveryRepairError> {
        public_delivery_diagnostics_hash(issues)?;
        Ok(
            if issues.iter().any(|issue| {
                matches!(
                    issue.kind(),
                    PublicDeliveryDiagnosticKind::ReferenceSyntax
                        | PublicDeliveryDiagnosticKind::ReferenceUnavailable
                )
            }) {
                Self::PrerequisiteRecovery
            } else {
                Self::FormatOnly
            },
        )
    }
}

/// Only authoritative public progress resets the correction episode. A later
/// consumed control invalidates this binding, but does not reset that budget.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PublicDeliveryRepairCut {
    pub progress_sequence: Option<ConversationSeq>,
    pub control_sequence: Option<ConversationSeq>,
}
impl PublicDeliveryRepairCut {
    pub fn validate_at(self, cutoff: ConversationSeq) -> Result<(), PublicDeliveryRepairError> {
        if self.progress_sequence.is_some_and(|seq| seq > cutoff)
            || self.control_sequence.is_some_and(|seq| seq > cutoff)
        {
            return Err(PublicDeliveryRepairError);
        }
        Ok(())
    }
}

/// Complete assistant output assembled and verified by the original Journal.
/// The relation to the Run, sealed frames and cut is checked by that Owner;
/// a ContentRef alone is never authority to read or replay another output.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicDeliveryCandidateProof {
    pub invocation_id: InvocationId,
    pub assistant_message_id: MessageId,
    pub assistant_output_id: OutputId,
    pub ledger_cutoff: ConversationSeq,
    pub content_ref: ContentRef,
}
impl PublicDeliveryCandidateProof {
    pub fn validate(&self) -> Result<(), PublicDeliveryRepairError> {
        if self.content_ref.byte_length == 0
            || self.content_ref.byte_length > crate::MAX_PUBLIC_DELIVERY_BYTES as u64
            || self.content_ref.media_type.as_str() != "text/plain;charset=utf-8"
        {
            return Err(PublicDeliveryRepairError);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicDeliveryRejectionV1 {
    pub candidate: PublicDeliveryCandidateProof,
    pub diagnostics: Vec<PublicDeliveryDiagnostic>,
    pub disposition: PublicDeliveryRepairDisposition,
    pub cut: PublicDeliveryRepairCut,
}
impl PublicDeliveryRejectionV1 {
    pub fn validate(&self) -> Result<(), PublicDeliveryRepairError> {
        self.candidate.validate()?;
        self.cut.validate_at(self.candidate.ledger_cutoff)?;
        if self.disposition != PublicDeliveryRepairDisposition::for_diagnostics(&self.diagnostics)?
        {
            return Err(PublicDeliveryRepairError);
        }
        Ok(())
    }
    pub fn binding(&self) -> Result<PublicDeliveryRepairBinding, PublicDeliveryRepairError> {
        self.binding_for(PublicDeliveryContract::V6)
    }
    pub fn validate_for(
        &self,
        contract: PublicDeliveryContract,
    ) -> Result<(), PublicDeliveryRepairError> {
        self.validate()?;
        match contract {
            PublicDeliveryContract::V6 => Ok(()),
            PublicDeliveryContract::V7
                if self.disposition == PublicDeliveryRepairDisposition::PrerequisiteRecovery
                    && self.diagnostics.iter().all(|d| {
                        d.kind() == PublicDeliveryDiagnosticKind::ReferenceUnavailable
                    }) =>
            {
                Ok(())
            }
            _ => Err(PublicDeliveryRepairError),
        }
    }
    pub fn binding_for(
        &self,
        contract: PublicDeliveryContract,
    ) -> Result<PublicDeliveryRepairBinding, PublicDeliveryRepairError> {
        self.validate_for(contract)?;
        let (spec_ref, spec_hash) = delivery_recovery_policy(contract)?;
        Ok(PublicDeliveryRepairBinding {
            contract_ref: VersionedKey::new(contract.key())
                .map_err(|_| PublicDeliveryRepairError)?,
            contract_prompt_sha256: Sha256::digest(contract.prompt().as_bytes()),
            repair_spec_ref: VersionedKey::new(spec_ref).map_err(|_| PublicDeliveryRepairError)?,
            repair_spec_sha256: spec_hash,
            rejection_invocation_id: self.candidate.invocation_id,
            candidate: self.candidate.clone(),
            diagnostic_hash: public_delivery_diagnostics_hash(&self.diagnostics)?,
            disposition: self.disposition,
            cut: self.cut,
        })
    }
}

/// This binding is committed with the original ContextPlan and checked again
/// before model begin. It does not add any Tool or publication capability.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicDeliveryRepairBinding {
    pub contract_ref: VersionedKey,
    pub contract_prompt_sha256: Sha256,
    pub repair_spec_ref: VersionedKey,
    pub repair_spec_sha256: Sha256,
    pub rejection_invocation_id: InvocationId,
    pub candidate: PublicDeliveryCandidateProof,
    pub diagnostic_hash: Sha256,
    pub disposition: PublicDeliveryRepairDisposition,
    pub cut: PublicDeliveryRepairCut,
}
impl PublicDeliveryRepairBinding {
    pub fn validate(&self) -> Result<(), PublicDeliveryRepairError> {
        self.candidate.validate()?;
        self.cut.validate_at(self.candidate.ledger_cutoff)?;
        let contract = PublicDeliveryContract::from_key(self.contract_ref.as_str())
            .ok_or(PublicDeliveryRepairError)?;
        let (spec_ref, spec_hash) = delivery_recovery_policy(contract)?;
        if (contract == PublicDeliveryContract::V7
            && self.disposition != PublicDeliveryRepairDisposition::PrerequisiteRecovery)
            || self.contract_ref.as_str() != contract.key()
            || self.contract_prompt_sha256 != Sha256::digest(contract.prompt().as_bytes())
            || self.repair_spec_ref.as_str() != spec_ref
            || self.repair_spec_sha256 != spec_hash
            || self.rejection_invocation_id != self.candidate.invocation_id
        {
            return Err(PublicDeliveryRepairError);
        }
        Ok(())
    }
}

/// No replayable candidate is manufactured when the original output exceeds
/// the contract bound. The original sealed frames remain the evidence owner.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicDeliveryUnreplayableOutput {
    pub invocation_id: InvocationId,
    pub assistant_message_id: MessageId,
    pub assistant_output_id: OutputId,
    pub ledger_cutoff: ConversationSeq,
    pub byte_length: u64,
    pub sha256: Sha256,
    pub cut: PublicDeliveryRepairCut,
}
impl PublicDeliveryUnreplayableOutput {
    pub fn validate(&self) -> Result<(), PublicDeliveryRepairError> {
        self.cut.validate_at(self.ledger_cutoff)?;
        if self.byte_length <= crate::MAX_PUBLIC_DELIVERY_BYTES as u64 {
            return Err(PublicDeliveryRepairError);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PublicDeliveryRepairViolation {
    ToolCallDuringRepair,
    EmptyCompletion,
    IncompleteCompletion,
}
impl PublicDeliveryRepairViolation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ToolCallDuringRepair => "tool_call_during_repair",
            Self::EmptyCompletion => "empty_completion",
            Self::IncompleteCompletion => "incomplete_completion",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "tool_call_during_repair" => Some(Self::ToolCallDuringRepair),
            "empty_completion" => Some(Self::EmptyCompletion),
            "incomplete_completion" => Some(Self::IncompleteCompletion),
            _ => None,
        }
    }
}

pub const PUBLIC_DELIVERY_RECOVERY_V7_REF: &str = "musteragent.private-delivery-recovery@2";
pub const PUBLIC_DELIVERY_RECOVERY_V7_SPEC: &str = include_str!("delivery_recovery_v7.json");
pub fn public_delivery_recovery_v7_hash() -> Sha256 {
    Sha256::digest(PUBLIC_DELIVERY_RECOVERY_V7_SPEC.as_bytes())
}
fn delivery_recovery_policy(
    contract: PublicDeliveryContract,
) -> Result<(&'static str, Sha256), PublicDeliveryRepairError> {
    match contract {
        PublicDeliveryContract::V6 => {
            Ok((PUBLIC_DELIVERY_REPAIR_REF, public_delivery_repair_hash()))
        }
        PublicDeliveryContract::V7 => Ok((
            PUBLIC_DELIVERY_RECOVERY_V7_REF,
            public_delivery_recovery_v7_hash(),
        )),
        _ => Err(PublicDeliveryRepairError),
    }
}

/// A rejected complete output retained as evidence, never a request to rewrite.
/// Diagnostic truncation cannot turn this terminal fact into recovery authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicDeliveryFormatRejection {
    pub candidate: PublicDeliveryCandidateProof,
    pub diagnostics: Vec<PublicDeliveryDiagnostic>,
    pub cut: PublicDeliveryRepairCut,
}
impl PublicDeliveryFormatRejection {
    pub fn validate(&self) -> Result<(), PublicDeliveryRepairError> {
        self.candidate.validate()?;
        self.cut.validate_at(self.candidate.ledger_cutoff)?;
        public_delivery_diagnostics_hash(&self.diagnostics)?;
        if self
            .diagnostics
            .iter()
            .any(|d| matches!(d.kind(), PublicDeliveryDiagnosticKind::ReferenceUnavailable))
        {
            return Err(PublicDeliveryRepairError);
        }
        Ok(())
    }
}
