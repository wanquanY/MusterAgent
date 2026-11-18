//! Wire representation of exact frozen application selections. No current
//! publication or account state participates in decoding a sealed selection.
use crate::wire;
use musteragent_content_ref::Sha256;
use musteragent_tool::workspace::{WorkspaceContractError as Error, indexed::*};
use prost::Message;

pub fn reference(value: &ApplicationCatalogReference) -> wire::ApplicationCatalogReference {
    wire::ApplicationCatalogReference {
        family: value.family.clone(),
        source_id: value.source_id.clone(),
        release_sha256: value.release_sha256.into_bytes().to_vec(),
        index_sha256: value.index_sha256.into_bytes().to_vec(),
    }
}
pub fn decode_reference(
    value: Option<wire::ApplicationCatalogReference>,
) -> Result<ApplicationCatalogReference, Error> {
    let value = value.ok_or(Error)?;
    let result = ApplicationCatalogReference {
        family: value.family,
        source_id: value.source_id,
        release_sha256: Sha256::from_bytes(value.release_sha256).map_err(|_| Error)?,
        index_sha256: Sha256::from_bytes(value.index_sha256).map_err(|_| Error)?,
    };
    result.validate()?;
    Ok(result)
}
pub fn frozen(value: &FrozenApplicationCatalog) -> wire::FrozenAccountApplicationCatalog {
    wire::FrozenAccountApplicationCatalog {
        reference: Some(reference(value.reference())),
        authority_template_json: value.authority_template_json().to_vec(),
        dialogue_witness_template: value.dialogue_witness_template().to_vec(),
        scope_groups: value
            .scope_groups()
            .iter()
            .map(|g| wire::ApplicationCatalogScopeGroup {
                scopes: g.scopes.clone(),
                operation_ordinals_le: g.ordinals.iter().flat_map(|v| v.to_le_bytes()).collect(),
            })
            .collect(),
        selection_sha256: value.selection_sha256().into_bytes().to_vec(),
    }
}
pub fn decode_frozen(
    value: wire::FrozenAccountApplicationCatalog,
) -> Result<FrozenApplicationCatalog, Error> {
    if value.encoded_len() > selection::MAX_SELECTION_BYTES {
        return Err(Error);
    }
    let encoded = value.encode_to_vec();
    let groups = value
        .scope_groups
        .into_iter()
        .map(|g| {
            if g.operation_ordinals_le.len() % 2 != 0 {
                return Err(Error);
            }
            Ok(ApplicationCatalogScopeGroup {
                scopes: g.scopes,
                ordinals: g
                    .operation_ordinals_le
                    .chunks_exact(2)
                    .map(|v| u16::from_le_bytes([v[0], v[1]]))
                    .collect(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let result = FrozenApplicationCatalog::new(
        decode_reference(value.reference)?,
        value.authority_template_json,
        value.dialogue_witness_template,
        groups,
        Sha256::from_bytes(value.selection_sha256).map_err(|_| Error)?,
    )?;
    if result.canonical_encoding(true) != encoded {
        return Err(Error);
    }
    validate_dialogue_template(&result)?;
    Ok(result)
}

fn validate_dialogue_template(value: &FrozenApplicationCatalog) -> Result<(), Error> {
    // Account-application consumer witnesses belong to the host integration.
    if !value.dialogue_witness_template().is_empty() {
        return Err(Error);
    }
    Ok(())
}
