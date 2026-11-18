//! Acknowledgement of a private Task submission in the original signed Resource
//! command lane. Its ids identify private result items, never dialogue entries.
use super::*;

const DOMAIN: &[u8] = b"musteragent.work-item-device-result-cut.v1\0";
const MAX_BYTES: usize = 2048;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkItemDeviceSubmittedResult {
    pub invocation_id: InvocationId,
    pub command_commitment: Sha256,
    pub receipt_commitment: Sha256,
    pub submission: WorkItemResultSubmissionReceipt,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkItemDeviceResultCut {
    pub confirmed: Option<WorkItemDeviceSubmittedResult>,
}

impl WorkItemDeviceSubmittedResult {
    pub fn from_owner_receipt(
        command: &crate::DeviceResourceCommand,
        receipt: &crate::DeviceResourceReceipt,
    ) -> Result<Self, DialogueStoreError> {
        let bad = DialogueStoreError::Corrupt;
        let invocation = command.invocation();
        let task = command.admission().work_item.ok_or(bad)?;
        if command.operation() != crate::DeviceResourceOperation::TaskSubmitResult
            || invocation.tool_key.as_str() != WORK_ITEM_RESULT_SUBMIT_TOOL
        {
            return Err(bad);
        }
        receipt.validate_for(command).map_err(|_| bad)?;
        let success = receipt.success().ok_or(bad)?;
        let input = WorkItemResultSubmissionInput::parse(command.request_bytes())?;
        let submission = WorkItemResultSubmissionReceipt::decode(&success.result_bytes)?;
        if input.task_id != task.work_item_id
            || input.goal_revision != task.goal_revision
            || input.control_revision != task.control_revision
            || input.assignment_epoch != task.assignment_epoch
            || submission.work_item_id != task.work_item_id
            || submission.producing_run_id != command.admission().run_id
            || submission.result_id != work_item_result_id(invocation.invocation_id)
            || success.result_ref.content_id
                != work_item_result_submit_receipt_id(invocation.invocation_id)
            || !success.produced_versions.is_empty()
            || !success.authorized_content.is_empty()
        {
            return Err(bad);
        }
        Ok(Self {
            invocation_id: invocation.invocation_id,
            command_commitment: command.commitment(),
            receipt_commitment: receipt.commitment(command).map_err(|_| bad)?,
            submission,
        })
    }
}

impl WorkItemDeviceResultCut {
    pub fn validate_for(
        &self,
        admission: &WorkItemDeviceAdmissionReceipt,
        outcome: WorkItemDeviceRunOutcome,
    ) -> Result<(), DialogueStoreError> {
        match &self.confirmed {
            Some(r)
                if r.submission.work_item_id == admission.key.work_item_id
                    && r.submission.producing_run_id == admission.run_id =>
            {
                Ok(())
            }
            None if outcome != WorkItemDeviceRunOutcome::Succeeded => Ok(()),
            _ => Err(DialogueStoreError::Corrupt),
        }
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        let mut bytes = DOMAIN.to_vec();
        bytes.push(u8::from(self.confirmed.is_some()));
        if let Some(r) = &self.confirmed {
            let receipt = r.submission.canonical_bytes();
            if [r.command_commitment, r.receipt_commitment]
                .iter()
                .any(|h| h.into_bytes() == [0; 32])
                || r.submission.result_id != work_item_result_id(r.invocation_id)
                || WorkItemResultSubmissionReceipt::decode(&receipt)? != r.submission
            {
                return Err(DialogueStoreError::Corrupt);
            }
            bytes.extend_from_slice(r.invocation_id.into_uuid().as_bytes());
            bytes.extend_from_slice(&r.command_commitment.into_bytes());
            bytes.extend_from_slice(&r.receipt_commitment.into_bytes());
            bytes.extend_from_slice(&(receipt.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&receipt);
        }
        if bytes.len() > MAX_BYTES {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        if bytes.len() > MAX_BYTES {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut r = Reader(
            bytes
                .strip_prefix(DOMAIN)
                .ok_or(DialogueStoreError::Corrupt)?,
        );
        let confirmed = match r.take::<1>()?[0] {
            0 => None,
            1 => Some(WorkItemDeviceSubmittedResult {
                invocation_id: r.id()?,
                command_commitment: r.hash()?,
                receipt_commitment: r.hash()?,
                submission: WorkItemResultSubmissionReceipt::decode(r.frame(MAX_BYTES)?)?,
            }),
            _ => return Err(DialogueStoreError::Corrupt),
        };
        let result = Self { confirmed };
        if !r.0.is_empty() || result.canonical_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(result)
    }
    pub fn commitment(&self) -> Result<Sha256, DialogueStoreError> {
        Ok(Sha256::digest(self.canonical_bytes()?))
    }
}
