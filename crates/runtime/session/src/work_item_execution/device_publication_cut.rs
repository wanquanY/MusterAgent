//! Immutable confirmed publication observations. Successful terminal receipts
//! require the complete set. Unsuccessful local attempts can retain an ACK
//! subset; the Cloud publication owner separately freezes its complete set.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDevicePublishedDelivery {
    pub invocation_id: InvocationId,
    pub fingerprint: Sha256,
    pub entry_ids: Vec<DialogueEntryId>,
    pub result_ref: ContentRef,
}
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct WorkItemDevicePublicationCut {
    pub deliveries: Vec<WorkItemDevicePublishedDelivery>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDelivery {
    invocation_id: String,
    fingerprint: String,
    entry_ids: Vec<String>,
    content_id: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
}
impl WorkItemDevicePublicationCut {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.deliveries.len() > 1024 {
            return Err(DialogueStoreError::Corrupt);
        }
        let mut prior = None;
        let mut entries = std::collections::BTreeSet::new();
        for d in &self.deliveries {
            if prior.is_some_and(|id| id >= d.invocation_id)
                || d.entry_ids.is_empty()
                || d.entry_ids.len() > 16
                || [d.fingerprint, d.result_ref.sha256]
                    .iter()
                    .any(|h| h.into_bytes() == [0; 32])
                || d.result_ref.media_type.as_str() != "application/json"
                || d.result_ref.byte_length == 0
                || d.result_ref.byte_length > 65536
                || d.result_ref.content_id.into_uuid()
                    != uuid::Uuid::new_v5(&d.invocation_id.into_uuid(), b"public-tool-receipt@1")
                || d.entry_ids.iter().any(|id| !entries.insert(*id))
            {
                return Err(DialogueStoreError::Corrupt);
            }
            prior = Some(d.invocation_id);
        }
        Ok(())
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, DialogueStoreError> {
        self.validate()?;
        let values = self
            .deliveries
            .iter()
            .map(|d| StoredDelivery {
                invocation_id: d.invocation_id.to_string(),
                fingerprint: d.fingerprint.to_string(),
                entry_ids: d.entry_ids.iter().map(ToString::to_string).collect(),
                content_id: d.result_ref.content_id.to_string(),
                media_type: d.result_ref.media_type.as_str().to_owned(),
                byte_length: d.result_ref.byte_length,
                sha256: d.result_ref.sha256.to_string(),
            })
            .collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&values).map_err(|_| DialogueStoreError::Corrupt)?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(DialogueStoreError::Corrupt);
        }
        let values: Vec<StoredDelivery> =
            serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        let deliveries = values
            .into_iter()
            .map(|d| {
                Ok(WorkItemDevicePublishedDelivery {
                    invocation_id: d
                        .invocation_id
                        .parse()
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    fingerprint: Sha256::from_hex(&d.fingerprint)
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    entry_ids: d
                        .entry_ids
                        .into_iter()
                        .map(|id| id.parse().map_err(|_| DialogueStoreError::Corrupt))
                        .collect::<Result<Vec<_>, _>>()?,
                    result_ref: ContentRef {
                        content_id: d
                            .content_id
                            .parse()
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        media_type: MediaType::new(d.media_type)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        byte_length: d.byte_length,
                        sha256: Sha256::from_hex(&d.sha256)
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                    },
                })
            })
            .collect::<Result<Vec<_>, DialogueStoreError>>()?;
        let result = Self { deliveries };
        if result.canonical_bytes()? != bytes {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(result)
    }
    pub fn commitment(&self) -> Result<Sha256, DialogueStoreError> {
        let mut b = b"musteragent.work-item-device-publication-cut.v1\0".to_vec();
        b.extend_from_slice(&self.canonical_bytes()?);
        Ok(Sha256::digest(b))
    }
}
