//! Publication of an existing canonical version, never a local Artifact export.
use super::*;
use crate::{
    DeviceArtifactExportRole, DeviceArtifactProducer, DeviceToolDelivery, DeviceToolDeliveryReceipt,
};
use musteragent_execution_context::{AgentArtifactLocator, MAX_PUBLIC_DELIVERIES};

pub const MAX_DEVICE_RESOURCE_DELIVERY_BYTES: usize = 4 * 1024 * 1024;

/// This is a reference to an immutable owner receipt plus a current successful
/// local producer. PG independently resolves both commitments and verifies
/// the exact version; these fields alone cannot establish authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceResourceDeliveryReference {
    /// Only the existing Delivery/Evidence distinction is shared with export.
    pub role: DeviceArtifactExportRole,
    pub admission: DeviceDeliveryBinding,
    pub command_commitment: Sha256,
    pub receipt_commitment: Sha256,
    pub operation: DeviceResourceOperation,
    pub version: DeviceCanonicalFileVersion,
    pub producer: DeviceArtifactProducer,
}
impl DeviceResourceDeliveryReference {
    pub fn new(
        role: DeviceArtifactExportRole,
        command: &DeviceResourceCommand,
        receipt: &DeviceResourceReceipt,
        version: DeviceCanonicalFileVersion,
        producer: DeviceArtifactProducer,
    ) -> Result<Self, Error> {
        let value = Self {
            role,
            admission: command.admission().clone(),
            command_commitment: command.commitment(),
            receipt_commitment: receipt.commitment(command)?,
            operation: command.operation(),
            version,
            producer,
        };
        value.validate_against(command, receipt)?;
        Ok(value)
    }
    /// Required on both local preparation and remote owner resolution. Matching
    /// a FileNode ID or a successful read result is never production proof.
    pub fn validate_against(
        &self,
        command: &DeviceResourceCommand,
        receipt: &DeviceResourceReceipt,
    ) -> Result<(), Error> {
        self.validate()?;
        receipt.validate_for(command)?;
        let success = receipt.success().ok_or(Error::Corrupt)?;
        if self.admission != *command.admission()
            || self.command_commitment != command.commitment()
            || self.receipt_commitment != receipt.commitment(command)?
            || self.operation != command.operation()
            || self.producer.invocation_id != command.invocation().invocation_id
            || self.producer.tool_call_id != command.invocation().tool_call_id
            || self.producer.tool_key.as_str() != command.invocation().tool_key.as_str()
            || self.producer.result_ref != success.result_ref
            || !success.produced_versions.contains(&self.version)
        {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
    fn validate(&self) -> Result<(), Error> {
        validate_admission(&self.admission)?;
        self.version.validate()?;
        for hash in [
            self.command_commitment,
            self.receipt_commitment,
            self.producer.terminal_record_hash,
            self.producer.result_ref.sha256,
        ] {
            nonzero(hash)?;
        }
        if !self.operation.can_produce_versions()
            || DeviceResourceOperation::from_tool_key(self.producer.tool_key.as_str())?
                != self.operation
            || self.producer.result_ref.media_type.as_str() != "application/json"
            || self.producer.result_ref.byte_length > MAX_DEVICE_RESOURCE_TOOL_BYTES as u64
            || self.producer.terminal_at.unix_millis() <= 0
        {
            return Err(Error::InvalidArguments);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceResourceDeliveryManifest {
    pub request_fingerprint: Sha256,
    /// Original request order: deliveries first, then evidence selections.
    pub entries: Vec<DeviceResourceDeliveryReference>,
}
impl DeviceResourceDeliveryManifest {
    pub fn validate_for(&self, delivery: &DeviceToolDelivery) -> Result<(), Error> {
        if self.request_fingerprint != delivery.fingerprint()
            || self.entries.is_empty()
            || self.entries.len() > MAX_PUBLIC_DELIVERIES
        {
            return Err(Error::InvalidArguments);
        }
        let requested = delivery
            .publication()
            .artifacts
            .iter()
            .map(|r| (DeviceArtifactExportRole::Delivery, r))
            .chain(
                delivery
                    .publication()
                    .evidence
                    .iter()
                    .map(|r| (DeviceArtifactExportRole::Evidence, r)),
            )
            .collect::<Vec<_>>();
        if requested.len() != self.entries.len() {
            return Err(Error::InvalidArguments);
        }
        let mut deliveries = BTreeSet::new();
        let mut identities = Vec::new();
        let mut nodes = BTreeMap::new();
        let mut versions = BTreeMap::new();
        for ((role, requested), entry) in requested.iter().zip(&self.entries) {
            entry.validate()?;
            if *role != entry.role
                || entry.admission != *delivery.binding()
                || entry.producer.invocation_id == delivery.invocation_id()
                || entry.producer.tool_call_id != requested.tool_call_id
                || requested.artifact != AgentArtifactLocator::FileNode(entry.version.node_id)
                || (*role == DeviceArtifactExportRole::Delivery
                    && !deliveries.insert(entry.version.artifact_id))
                || nodes
                    .insert(entry.version.node_id, entry.version.artifact_id)
                    .is_some_and(|old| old != entry.version.artifact_id)
                || versions
                    .insert(entry.version.artifact_version_id, &entry.version)
                    .is_some_and(|old| old != &entry.version)
            {
                return Err(Error::InvalidArguments);
            }
            identities.push(entry.version.content_ref.clone());
            identities.extend(entry.version.dependency_refs.iter().cloned());
            identities.push(entry.producer.result_ref.clone());
        }
        validate_ref_identities(&identities)
    }
    pub fn encode(&self, delivery: &DeviceToolDelivery) -> Result<Vec<u8>, Error> {
        self.validate_for(delivery)?;
        let bytes = codec::encode_manifest(self)?;
        if bytes.len() > MAX_DEVICE_RESOURCE_DELIVERY_BYTES {
            return Err(Error::InvalidArguments);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8], delivery: &DeviceToolDelivery) -> Result<Self, Error> {
        if bytes.len() > MAX_DEVICE_RESOURCE_DELIVERY_BYTES {
            return Err(Error::InvalidArguments);
        }
        let value = codec::decode_manifest(bytes)?;
        require_canonical(bytes, &value.encode(delivery)?)?;
        Ok(value)
    }
    pub fn commitment(&self, delivery: &DeviceToolDelivery) -> Result<Sha256, Error> {
        Ok(digest(
            b"musteragent.device-resource-delivery.v1\0",
            &self.encode(delivery)?,
        ))
    }
}

/// Typed ACK binds the canonical proof in addition to the original publication
/// request. An ordinary request-only receipt cannot acknowledge this branch.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceResourceDeliveryReceipt {
    pub publication: DeviceToolDeliveryReceipt,
    pub resource_commitment: Sha256,
}
impl DeviceResourceDeliveryReceipt {
    pub fn validate_for(
        &self,
        delivery: &DeviceToolDelivery,
        manifest: &DeviceResourceDeliveryManifest,
    ) -> Result<(), Error> {
        self.publication
            .validate_for(delivery)
            .map_err(|_| Error::Corrupt)?;
        if self.resource_commitment != manifest.commitment(delivery)? {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
}

pub trait DeviceResourceDeliveryStore: Send + Sync {
    fn publish_device_resources<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        delivery: &'a DeviceToolDelivery,
        manifest: &'a DeviceResourceDeliveryManifest,
    ) -> ApplicationFuture<'a, Result<DeviceResourceDeliveryReceipt, Error>>;
    fn lookup_device_resource_delivery<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        delivery: &'a DeviceToolDelivery,
        manifest: &'a DeviceResourceDeliveryManifest,
    ) -> ApplicationFuture<'a, Result<Option<DeviceResourceDeliveryReceipt>, Error>>;
}
pub trait DeviceResourceDeliveryTransport: Send + Sync {
    fn deliver_resources<'a>(
        &'a self,
        delivery: &'a DeviceToolDelivery,
        manifest: &'a DeviceResourceDeliveryManifest,
    ) -> ApplicationFuture<'a, Result<DeviceResourceDeliveryReceipt, Error>>;
    fn lookup_resources<'a>(
        &'a self,
        delivery: &'a DeviceToolDelivery,
        manifest: &'a DeviceResourceDeliveryManifest,
    ) -> ApplicationFuture<'a, Result<Option<DeviceResourceDeliveryReceipt>, Error>>;
}
