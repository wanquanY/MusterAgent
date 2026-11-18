//! Closed product publication identity, shared by signed grants and snapshots.
use crate::wire;
use musteragent_identity::{ProductId, ProductReleaseRef, VersionedKey};

pub fn encode(value: &ProductReleaseRef) -> wire::ProductReleaseRef {
    wire::ProductReleaseRef {
        schema_version: "musteragent.runtime.product-release.v1".into(),
        product_id: value.product.as_str().into(),
        release_id: value.release_id.as_str().into(),
        sha256: value.digest.to_vec(),
    }
}

pub fn decode(
    value: wire::ProductReleaseRef,
) -> Result<ProductReleaseRef, musteragent_identity::ProductReleaseIdentityError> {
    let invalid = || musteragent_identity::ProductReleaseIdentityError;
    if value.schema_version != "musteragent.runtime.product-release.v1" {
        return Err(invalid());
    }
    let product = ProductId::parse(&value.product_id)?;
    ProductReleaseRef::new(
        product,
        VersionedKey::new(value.release_id).map_err(|_| invalid())?,
        value.sha256.try_into().map_err(|_| invalid())?,
    )
}
