//! Discovery carries exact schema identity without eagerly reading schema bytes.
use crate::DiscoveryContractError;
use musteragent_content_ref::Sha256;
use musteragent_tool::workspace::indexed::{ApplicationSchemaReference, FrozenApplicationCatalog};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ToolSchemaMaterial {
    Inline(Vec<u8>),
    Indexed(ApplicationSchemaReference),
}
impl ToolSchemaMaterial {
    pub fn inline_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Inline(bytes) => Some(bytes),
            Self::Indexed(_) => None,
        }
    }
    pub fn validate(&self, expected: Sha256) -> Result<(), DiscoveryContractError> {
        match self {
            Self::Inline(bytes) if !bytes.is_empty() && Sha256::digest(bytes) == expected => Ok(()),
            Self::Indexed(reference) => {
                reference
                    .validate()
                    .map_err(|_| DiscoveryContractError::InvalidToolDescriptor)?;
                if reference
                    .content
                    .digest()
                    .map_err(|_| DiscoveryContractError::InvalidToolDescriptor)?
                    != expected
                {
                    return Err(DiscoveryContractError::InvalidToolDescriptor);
                }
                Ok(())
            }
            _ => Err(DiscoveryContractError::InvalidToolDescriptor),
        }
    }
}
impl From<Vec<u8>> for ToolSchemaMaterial {
    fn from(value: Vec<u8>) -> Self {
        Self::Inline(value)
    }
}

/// Catalog identity depends on sealed selection, never cache hydration order.
/// Inline-only historical identities remain byte-for-byte unchanged.
pub fn indexed_catalog_hash(
    inline_hash: Sha256,
    selections: &[FrozenApplicationCatalog],
) -> Result<Sha256, DiscoveryContractError> {
    if selections.is_empty() {
        return Ok(inline_hash);
    }
    if selections.len() > 200
        || selections
            .windows(2)
            .any(|p| p[0].reference() >= p[1].reference())
    {
        return Err(DiscoveryContractError::InvalidToolDescriptor);
    }
    let mut bytes = b"musteragent.authorized-indexed-tool-catalog.v1\0".to_vec();
    bytes.extend(inline_hash.into_bytes());
    bytes.extend((selections.len() as u32).to_be_bytes());
    for selection in selections {
        selection
            .validate()
            .map_err(|_| DiscoveryContractError::InvalidToolDescriptor)?;
        bytes.extend(selection.selection_sha256().into_bytes());
    }
    Ok(Sha256::digest(bytes))
}
