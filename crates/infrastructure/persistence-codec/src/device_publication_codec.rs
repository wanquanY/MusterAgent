//! Bounded canonical transport for the original Native admission/publication
//! owners. Parsing proves data shape; it never authenticates a Native subject.
use crate::{dialogue_codec::*, encode_content_ref, wire};
use musteragent_content_ref::Sha256;
use musteragent_runtime_session::{
    DeviceDeliveryBinding, DeviceToolDelivery, DeviceToolDeliveryReceipt,
    WorkItemDevicePublicationSubject,
};
use prost::Message;
type Error = DialogueCodecError;
const BAD: Error = Error::Corrupt;
pub const DELIVERY_LIMIT: usize = 288 * 1024;
pub const RECEIPT_LIMIT: usize = 16 * 1024;

pub fn binding(v: &DeviceDeliveryBinding) -> wire::DeviceDeliveryBinding {
    wire::DeviceDeliveryBinding {
        work_item: v.work_item.map(|v| wire::WorkItemDevicePublicationSubject {
            work_item_id: Some(wire::WorkItemId {
                value: v.work_item_id.to_string(),
            }),
            goal_revision: v.goal_revision,
            control_revision: v.control_revision,
            assignment_epoch: v.assignment_epoch,
        }),
        intent_id: Some(wire::UserIntentId {
            value: v.intent_id.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: v.run_id.to_string(),
        }),
        device_id: Some(wire::DeviceId {
            value: v.device_id.to_string(),
        }),
        installation_id: Some(wire::DeviceInstallationId {
            value: v.installation_id.to_string(),
        }),
        source_digest: v.source_digest.into_bytes().to_vec(),
        snapshot_hash: v.snapshot_hash.into_bytes().to_vec(),
        grant_proof_hash: v.grant_proof_hash.into_bytes().to_vec(),
    }
}
fn hash(bytes: Vec<u8>) -> Result<Sha256, Error> {
    let v = Sha256::from_bytes(bytes).map_err(|_| BAD)?;
    if v.into_bytes() == [0; 32] {
        return Err(BAD);
    }
    Ok(v)
}
pub fn decode_binding(
    v: Option<wire::DeviceDeliveryBinding>,
) -> Result<DeviceDeliveryBinding, Error> {
    let v = required(v)?;
    if v.encoded_len() > 4096 {
        return Err(BAD);
    }
    let binding = DeviceDeliveryBinding {
        work_item: v
            .work_item
            .map(|v| -> Result<_, Error> {
                Ok(WorkItemDevicePublicationSubject {
                    work_item_id: id(v.work_item_id.map(|v| v.value))?,
                    goal_revision: v.goal_revision,
                    control_revision: v.control_revision,
                    assignment_epoch: v.assignment_epoch,
                })
            })
            .transpose()?,
        intent_id: id(v.intent_id.map(|v| v.value))?,
        run_id: id(v.run_id.map(|v| v.value))?,
        device_id: id(v.device_id.map(|v| v.value))?,
        installation_id: id(v.installation_id.map(|v| v.value))?,
        source_digest: hash(v.source_digest)?,
        snapshot_hash: hash(v.snapshot_hash)?,
        grant_proof_hash: hash(v.grant_proof_hash)?,
    };
    binding.validate_subject().map_err(|_| BAD)?;
    Ok(binding)
}
pub fn delivery(v: &DeviceToolDelivery) -> wire::DeviceToolDelivery {
    wire::DeviceToolDelivery {
        admission: Some(binding(v.binding())),
        invocation_id: Some(wire::InvocationId {
            value: v.invocation_id().to_string(),
        }),
        tool_key: v.tool_key().to_owned(),
        request_ref: Some(encode_content_ref(v.request_ref())),
        request_content: v.request_bytes().to_vec(),
    }
}
pub fn decode_delivery(v: Option<wire::DeviceToolDelivery>) -> Result<DeviceToolDelivery, Error> {
    let v = required(v)?;
    if v.encoded_len() > DELIVERY_LIMIT {
        return Err(BAD);
    }
    DeviceToolDelivery::new(
        decode_binding(v.admission)?,
        id(v.invocation_id.map(|v| v.value))?,
        v.tool_key,
        content(required(v.request_ref)?)?,
        v.request_content,
    )
    .map_err(|_| BAD)
}
pub fn receipt(v: &DeviceToolDeliveryReceipt) -> wire::DeviceToolDeliveryReceipt {
    wire::DeviceToolDeliveryReceipt {
        invocation_id: Some(wire::InvocationId {
            value: v.invocation_id.to_string(),
        }),
        delivery_fingerprint: v.fingerprint.into_bytes().to_vec(),
        entry_ids: v
            .entry_ids
            .iter()
            .map(|v| wire::DialogueEntryId {
                value: v.to_string(),
            })
            .collect(),
        result_ref: Some(encode_content_ref(&v.result_ref)),
        result_content: v.result_bytes.clone(),
    }
}
pub fn decode_receipt(
    v: Option<wire::DeviceToolDeliveryReceipt>,
    expected: &DeviceToolDelivery,
) -> Result<DeviceToolDeliveryReceipt, Error> {
    let v = required(v)?;
    if v.encoded_len() > RECEIPT_LIMIT || v.entry_ids.len() > 33 {
        return Err(BAD);
    }
    let r = DeviceToolDeliveryReceipt {
        invocation_id: id(v.invocation_id.map(|v| v.value))?,
        fingerprint: hash(v.delivery_fingerprint)?,
        entry_ids: v
            .entry_ids
            .into_iter()
            .map(|v| id(Some(v.value)))
            .collect::<Result<_, _>>()?,
        result_ref: content(required(v.result_ref)?)?,
        result_bytes: v.result_content,
    };
    r.validate_for(expected).map_err(|_| BAD)?;
    Ok(r)
}
pub fn decode_admission_ack(bytes: &[u8], expected: &DeviceDeliveryBinding) -> Result<(), Error> {
    if bytes.is_empty() || bytes.len() > 4096 {
        return Err(BAD);
    }
    let response = wire::AcknowledgeDeviceAdmissionResponse::decode(bytes).map_err(|_| BAD)?;
    if response.encode_to_vec() != bytes || decode_binding(response.admission)? != *expected {
        return Err(BAD);
    }
    Ok(())
}
pub fn decode_publication_receipt(
    bytes: &[u8],
    expected: &DeviceToolDelivery,
) -> Result<DeviceToolDeliveryReceipt, Error> {
    if bytes.is_empty() || bytes.len() > RECEIPT_LIMIT {
        return Err(BAD);
    }
    let response = wire::PublishDeviceToolDeliveryResponse::decode(bytes).map_err(|_| BAD)?;
    if response.encode_to_vec() != bytes {
        return Err(BAD);
    }
    decode_receipt(response.receipt, expected)
}
