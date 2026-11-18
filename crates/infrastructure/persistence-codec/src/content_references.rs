//! One typed reference collector for durable protobuf owners. It never scans
//! strings/JSON for identifiers and never publishes a partial reference set.
use crate::bounded_protobuf::{self, BudgetError, Profile};
use crate::{wire, wire_common::decode_content_ref};
use musteragent_content_ref::ContentRef;
use musteragent_identity::ContentId;
use prost::Message as _;
use std::{collections::BTreeMap, num::NonZeroUsize};

#[derive(Default)]
pub(crate) struct ContentReferences(BTreeMap<ContentId, ContentRef>);

impl ContentReferences {
    pub(crate) fn visit(
        &mut self,
        bytes: &[u8],
        maximum: NonZeroUsize,
        name: &str,
        profile: Profile,
    ) -> Result<(), BudgetError> {
        bounded_protobuf::visit_messages(bytes, maximum, name, profile, &mut |message, bytes| {
            if message == "musteragent.v1.ContentRef" {
                let value = wire::ContentRef::decode(bytes).map_err(|_| BudgetError::Invalid)?;
                let reference = decode_content_ref(value).map_err(|_| BudgetError::Invalid)?;
                if self
                    .0
                    .insert(reference.content_id, reference.clone())
                    .is_some_and(|prior| prior != reference)
                {
                    return Err(BudgetError::Invalid);
                }
            }
            Ok(())
        })
    }

    pub(crate) fn finish(self) -> Vec<ContentRef> {
        self.0.into_values().collect()
    }
}
