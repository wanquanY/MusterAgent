use musteragent_ledger::DomainEvent;
use musteragent_scheduler::{DispatchError, SchedulerEventEncoder};
use prost::Message;

/// Canonical Protobuf encoding injected into the durable Scheduler.
#[derive(Debug, Copy, Clone, Default)]
pub struct ProtobufSchedulerEventEncoder;

impl SchedulerEventEncoder for ProtobufSchedulerEventEncoder {
    fn encode_domain_event(&self, event: DomainEvent) -> Result<Vec<u8>, DispatchError> {
        Ok(crate::outbound::domain_event(event).encode_to_vec())
    }
}
