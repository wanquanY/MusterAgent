//! Durable management commands are original MCP configuration operations, not
//! model-facing execution or an Agent application grant. Receipts bind exact
//! bytes and survive removal/recreation of a named server.
use super::*;
use musteragent_identity::CommandId;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum McpConfigurationOperation {
    /// The account UI may only create custom, initially denied connections.
    /// Credential writes remain with the original Secret Store flow.
    Create(McpServerDraft),
    SetEnabled {
        name: McpServerName,
        instance_id: String,
        expected_revision: u64,
        enabled: bool,
    },
    Remove {
        name: McpServerName,
        instance_id: String,
        expected_revision: u64,
    },
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpConfigurationCommand {
    pub command_id: CommandId,
    pub operation: McpConfigurationOperation,
}
impl McpConfigurationCommand {
    pub fn validate(&self) -> Result<(), McpContractError> {
        match &self.operation {
            McpConfigurationOperation::Create(draft) => {
                draft.validate()?;
                if draft.authorization != McpAuthorizationKind::None
                    || draft.owner.origin != McpServerOrigin::Custom
                    || !draft.owner.owner_type.is_empty()
                    || !draft.owner.owner_id.is_empty()
                    || !draft.owner.owner_version.is_empty()
                    || draft.enabled_tools.as_ref().is_none_or(|v| !v.is_empty())
                {
                    return Err(McpContractError::InvalidOwner);
                }
                let size = match &draft.transport {
                    McpTransport::Stdio { command, arguments } => {
                        command.len() + arguments.iter().map(String::len).sum::<usize>()
                    }
                    McpTransport::StreamableHttp { url } => url.len(),
                };
                if size > 256 * 1024 {
                    return Err(McpContractError::InvalidTransport);
                }
            }
            McpConfigurationOperation::SetEnabled {
                instance_id,
                expected_revision,
                ..
            }
            | McpConfigurationOperation::Remove {
                instance_id,
                expected_revision,
                ..
            } => {
                if instance_id.len() != 32
                    || !instance_id
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    || *expected_revision == 0
                    || *expected_revision == u64::MAX
                {
                    return Err(McpContractError::InvalidOwner);
                }
            }
        }
        Ok(())
    }
    #[must_use]
    pub fn name(&self) -> &McpServerName {
        match &self.operation {
            McpConfigurationOperation::Create(draft) => &draft.name,
            McpConfigurationOperation::SetEnabled { name, .. }
            | McpConfigurationOperation::Remove { name, .. } => name,
        }
    }
    /// v1 canonical framing: explicit purpose/tag, UTF-8 fields with u64 BE byte
    /// lengths, numbers as u64 BE and booleans as one byte. No JSON ordering.
    pub fn fingerprint(&self) -> Result<[u8; 32], McpContractError> {
        self.validate()?;
        let mut hash = Sha256::new();
        hash.update(b"musteragent.mcp.configuration-command.v1\0");
        hash.update(self.command_id.into_uuid().as_bytes());
        field(&mut hash, self.name().as_str());
        match &self.operation {
            McpConfigurationOperation::Create(draft) => {
                hash.update([1]);
                match &draft.transport {
                    McpTransport::Stdio { command, arguments } => {
                        hash.update([1]);
                        field(&mut hash, command);
                        hash.update((arguments.len() as u64).to_be_bytes());
                        for arg in arguments {
                            field(&mut hash, arg);
                        }
                    }
                    McpTransport::StreamableHttp { url } => {
                        hash.update([2]);
                        field(&mut hash, url);
                    }
                }
                hash.update([u8::from(draft.enabled)]);
                // All omitted owner/auth/filter fields are validated fixed above.
            }
            McpConfigurationOperation::SetEnabled {
                instance_id,
                expected_revision,
                enabled,
                ..
            } => {
                hash.update([2]);
                field(&mut hash, instance_id);
                hash.update(expected_revision.to_be_bytes());
                hash.update([u8::from(*enabled)]);
            }
            McpConfigurationOperation::Remove {
                instance_id,
                expected_revision,
                ..
            } => {
                hash.update([3]);
                field(&mut hash, instance_id);
                hash.update(expected_revision.to_be_bytes());
            }
        }
        Ok(hash.finalize().into())
    }
}
fn field(hash: &mut Sha256, value: &str) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value.as_bytes());
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum McpConfigurationOutcome {
    Applied {
        instance_id: String,
        revision: u64,
        removed: bool,
    },
    Conflict,
    NotFound,
    OwnerManaged,
    /// Original Writer rejected a previously uncommitted command after its dispatch deadline.
    DispatchExpired,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct McpConfigurationReceipt {
    pub command_id: CommandId,
    pub request_fingerprint: [u8; 32],
    pub outcome: McpConfigurationOutcome,
    pub applied_at: UtcTimestamp,
}
