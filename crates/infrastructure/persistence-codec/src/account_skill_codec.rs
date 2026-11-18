//! Native admission projection of original immutable Skill lifecycle records.
//! This transports material; it never creates a local lifecycle pointer.
use crate::{decode_content_ref, encode_content_ref, wire};
use musteragent_content_ref::Sha256;
use musteragent_identity::{TenantId, UtcTimestamp};
use musteragent_skill::*;

#[derive(Debug, thiserror::Error)]
#[error("invalid account Skill catalog material")]
pub struct AccountSkillCodecError;
type Error = AccountSkillCodecError;

pub fn encode(entry: &SkillCatalogEntry) -> Result<wire::AccountSkillCatalogEntry, Error> {
    encode_inner(entry, false)
}

pub fn encode_retained(entry: &SkillCatalogEntry) -> Result<wire::AccountSkillCatalogEntry, Error> {
    encode_inner(entry, true)
}

fn encode_inner(
    entry: &SkillCatalogEntry,
    retained: bool,
) -> Result<wire::AccountSkillCatalogEntry, Error> {
    entry.validate().map_err(|_| AccountSkillCodecError)?;
    let bundle_ref = match &entry.authority.identity.provenance {
        SkillProvenance::Bundled { bundle_ref }
            if retained
                && entry.authority.identity.trust == SkillTrust::Official
                && entry.authority.install_scope == SkillInstallScope::Global =>
        {
            Some(bundle_ref.as_str().to_owned())
        }
        SkillProvenance::Bundled { .. } => return Err(AccountSkillCodecError),
        _ if retained => return Err(AccountSkillCodecError),
        _ => None,
    };
    let (origin, source_ref, publisher_ref, created_by) = match &entry.authority.identity.provenance
    {
        SkillProvenance::Custom { created_by } => (
            wire::LocalSkillOrigin::Local,
            String::new(),
            String::new(),
            Some(wire::PrincipalId {
                value: created_by.to_string(),
            }),
        ),
        SkillProvenance::Installed {
            source,
            source_ref,
            publisher_ref,
        } => (
            match source {
                InstalledSkillSource::Marketplace => wire::LocalSkillOrigin::Marketplace,
                InstalledSkillSource::Hub => wire::LocalSkillOrigin::Hub,
                InstalledSkillSource::GitHub => wire::LocalSkillOrigin::Github,
                InstalledSkillSource::Url => wire::LocalSkillOrigin::Url,
            },
            source_ref.clone(),
            publisher_ref.clone(),
            None,
        ),
        SkillProvenance::Bundled { .. } => (
            wire::LocalSkillOrigin::BuiltIn,
            String::new(),
            String::new(),
            None,
        ),
    };
    Ok(wire::AccountSkillCatalogEntry {
        definition: Some(crate::snapshot_codec::skill_definition(&entry.definition)),
        skill_key: entry.authority.identity.skill_key.as_str().into(),
        identity_created_at_ms: entry.authority.identity.created_at.unix_millis(),
        origin: origin.into(),
        trust: match entry.authority.identity.trust {
            SkillTrust::Community => wire::LocalSkillTrust::Community,
            SkillTrust::Verified => wire::LocalSkillTrust::Verified,
            SkillTrust::Local => wire::LocalSkillTrust::Local,
            SkillTrust::Official if retained => wire::LocalSkillTrust::Official,
            SkillTrust::Official => return Err(AccountSkillCodecError),
        }
        .into(),
        source_ref,
        publisher_ref,
        created_by,
        bundle_ref,
        load_default: false,
        when_to_use: entry.authority.when_to_use.clone(),
        spec_hash: Some(wire::Sha256 {
            value: entry.authority.spec_hash.into_bytes().to_vec(),
        }),
        references: entry
            .authority
            .references
            .iter()
            .map(|r| wire::SkillCatalogReferenceSnapshot {
                kind: match r.kind {
                    SkillReferenceKind::Instructions => {
                        wire::SkillCatalogReferenceKind::Instructions
                    }
                    SkillReferenceKind::Manifest => wire::SkillCatalogReferenceKind::Manifest,
                    SkillReferenceKind::Package => wire::SkillCatalogReferenceKind::Package,
                }
                .into(),
                content_ref: Some(encode_content_ref(&r.content_ref)),
            })
            .collect(),
    })
}

pub fn decode(
    value: wire::AccountSkillCatalogEntry,
    tenant: TenantId,
) -> Result<SkillCatalogEntry, Error> {
    decode_inner(value, Some(tenant), false)
}

pub fn decode_retained(value: wire::AccountSkillCatalogEntry) -> Result<SkillCatalogEntry, Error> {
    // A retained catalog must decode to Global below; the tenant is not authority.
    decode_inner(value, None, true)
}

fn decode_inner(
    value: wire::AccountSkillCatalogEntry,
    tenant: Option<TenantId>,
    retained: bool,
) -> Result<SkillCatalogEntry, Error> {
    let definition =
        crate::snapshot_codec::decode_skill(value.definition.ok_or(AccountSkillCodecError)?)
            .map_err(|_| AccountSkillCodecError)?;
    let origin =
        wire::LocalSkillOrigin::try_from(value.origin).map_err(|_| AccountSkillCodecError)?;
    if !retained && value.bundle_ref.is_some() {
        return Err(AccountSkillCodecError);
    }
    let provenance = if retained {
        if value.load_default
            || origin != wire::LocalSkillOrigin::BuiltIn
            || value.trust != wire::LocalSkillTrust::Official as i32
            || !value.source_ref.is_empty()
            || !value.publisher_ref.is_empty()
            || value.created_by.is_some()
        {
            return Err(AccountSkillCodecError);
        }
        SkillProvenance::Bundled {
            bundle_ref: musteragent_identity::VersionedKey::new(
                value.bundle_ref.ok_or(AccountSkillCodecError)?,
            )
            .map_err(|_| AccountSkillCodecError)?,
        }
    } else if origin == wire::LocalSkillOrigin::Local
        && value.source_ref.is_empty()
        && value.publisher_ref.is_empty()
    {
        SkillProvenance::Custom {
            created_by: value
                .created_by
                .ok_or(AccountSkillCodecError)?
                .value
                .parse()
                .map_err(|_| AccountSkillCodecError)?,
        }
    } else {
        if value.created_by.is_some() {
            return Err(AccountSkillCodecError);
        }
        SkillProvenance::Installed {
            source: match origin {
                wire::LocalSkillOrigin::Marketplace => InstalledSkillSource::Marketplace,
                wire::LocalSkillOrigin::Hub => InstalledSkillSource::Hub,
                wire::LocalSkillOrigin::Github => InstalledSkillSource::GitHub,
                wire::LocalSkillOrigin::Url => InstalledSkillSource::Url,
                _ => return Err(AccountSkillCodecError),
            },
            source_ref: value.source_ref,
            publisher_ref: value.publisher_ref,
        }
    };
    let entry = SkillCatalogEntry {
        authority: SkillCatalogAuthority {
            identity: SkillIdentity {
                skill_id: definition.skill_id,
                skill_key: SkillKey::new(value.skill_key).map_err(|_| AccountSkillCodecError)?,
                created_at: UtcTimestamp::from_unix_millis(value.identity_created_at_ms),
                provenance,
                trust: match wire::LocalSkillTrust::try_from(value.trust)
                    .map_err(|_| AccountSkillCodecError)?
                {
                    wire::LocalSkillTrust::Community => SkillTrust::Community,
                    wire::LocalSkillTrust::Verified => SkillTrust::Verified,
                    wire::LocalSkillTrust::Local => SkillTrust::Local,
                    wire::LocalSkillTrust::Official if retained => SkillTrust::Official,
                    _ => return Err(AccountSkillCodecError),
                },
            },
            when_to_use: value.when_to_use,
            install_scope: if retained {
                SkillInstallScope::Global
            } else {
                SkillInstallScope::Tenant(tenant.ok_or(AccountSkillCodecError)?)
            },
            spec_hash: Sha256::from_bytes(value.spec_hash.ok_or(AccountSkillCodecError)?.value)
                .map_err(|_| AccountSkillCodecError)?,
            references: value
                .references
                .into_iter()
                .map(|r| {
                    Ok(SkillCatalogReference {
                        kind: match wire::SkillCatalogReferenceKind::try_from(r.kind)
                            .map_err(|_| AccountSkillCodecError)?
                        {
                            wire::SkillCatalogReferenceKind::Instructions => {
                                SkillReferenceKind::Instructions
                            }
                            wire::SkillCatalogReferenceKind::Manifest => {
                                SkillReferenceKind::Manifest
                            }
                            wire::SkillCatalogReferenceKind::Package => SkillReferenceKind::Package,
                            _ => return Err(AccountSkillCodecError),
                        },
                        content_ref: decode_content_ref(r.content_ref, "skill reference")
                            .map_err(|_| AccountSkillCodecError)?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?,
        },
        definition,
    };
    entry.validate().map_err(|_| AccountSkillCodecError)?;
    Ok(entry)
}

pub fn decode_retained_catalog(
    product: musteragent_identity::ProductId,
    base: musteragent_identity::VersionedKey,
    digest: Sha256,
    bytes: Vec<u8>,
) -> Result<
    musteragent_runtime_skill::VerifiedRetainedSkillCatalog,
    musteragent_runtime_skill::SkillLifecycleStoreError,
> {
    use musteragent_runtime_skill::{SkillLifecycleStoreError as E, VerifiedRetainedSkillCatalog};
    use prost::Message;
    VerifiedRetainedSkillCatalog::verify_encoded(product, base, digest, bytes, |bytes| {
        let wire = wire::SelectAccountSkillsResponse::decode(bytes).map_err(|_| E::Corrupt)?;
        if wire.retained_catalog_sha256.is_some() {
            return Err(E::Corrupt);
        }
        wire.entries
            .into_iter()
            .map(|entry| decode_retained(entry).map_err(|_| E::Corrupt))
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::{ContentRef, MediaType};
    use musteragent_identity::{ContentId, PrincipalId};
    #[test]
    fn account_roundtrip_keeps_exact_identity_and_rejects_forged_origin_or_activation() {
        let tenant = TenantId::generate();
        let mut entry = builtin_skill_catalog_entries().unwrap().remove(0);
        entry.authority.identity.provenance = SkillProvenance::Custom {
            created_by: PrincipalId::generate(),
        };
        entry.authority.identity.trust = SkillTrust::Local;
        entry.authority.install_scope = SkillInstallScope::Tenant(tenant);
        entry.definition.activation_policy = ActivationPolicy::ExplicitOrResolved;
        let manifest = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").unwrap(),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        };
        entry.authority.spec_hash = manifest.sha256;
        entry
            .authority
            .references
            .retain(|r| r.kind == SkillReferenceKind::Instructions);
        entry.authority.references.push(SkillCatalogReference {
            kind: SkillReferenceKind::Manifest,
            content_ref: manifest,
        });
        let encoded = encode(&entry).unwrap();
        assert_eq!(decode(encoded.clone(), tenant).unwrap(), entry);
        let mut altered = encoded.clone();
        altered.origin = wire::LocalSkillOrigin::BuiltIn.into();
        assert!(decode(altered, tenant).is_err());
        let mut altered = encoded.clone();
        altered.trust = wire::LocalSkillTrust::Official.into();
        assert!(decode(altered, tenant).is_err());
        let mut altered = encoded;
        altered.definition.as_mut().unwrap().activation_policy =
            wire::SkillActivationPolicy::Base.into();
        assert!(decode(altered, tenant).is_err());
    }
    #[test]
    fn retained_publication_verifies_bytes_base_and_origin_before_admission() {
        use musteragent_identity::{ProductId, VersionedKey};
        use prost::Message;
        let product = ProductId::KERNEL;
        let entries = musteragent_skill::ProductSkillRelease::for_product(product)
            .entries()
            .unwrap();
        let base = entries
            .iter()
            .find(|e| e.definition.activation_policy == ActivationPolicy::Base)
            .unwrap()
            .definition
            .skill_ref
            .clone();
        let message = wire::SelectAccountSkillsResponse {
            entries: entries
                .iter()
                .map(|e| encode_retained(e).unwrap())
                .collect(),
            retained_catalog_sha256: None,
        };
        let bytes = message.encode_to_vec();
        let digest = Sha256::digest(&bytes);
        let verified =
            decode_retained_catalog(product, base.clone(), digest, bytes.clone()).unwrap();
        assert_eq!(verified.entries(), entries.as_slice());
        let mut tampered = bytes.clone();
        tampered.push(0);
        assert!(decode_retained_catalog(product, base.clone(), digest, tampered).is_err());
        assert!(
            decode_retained_catalog(
                product,
                VersionedKey::new("assistant-base@999").unwrap(),
                digest,
                bytes
            )
            .is_err()
        );
        let mut policy_in_publication = message.clone();
        policy_in_publication.entries[0].load_default = true;
        let bytes = policy_in_publication.encode_to_vec();
        assert!(
            decode_retained_catalog(product, base.clone(), Sha256::digest(&bytes), bytes).is_err()
        );
        let mut impostor = message;
        impostor.entries[0].origin = wire::LocalSkillOrigin::Marketplace.into();
        let bytes = impostor.encode_to_vec();
        assert!(decode_retained_catalog(product, base, Sha256::digest(&bytes), bytes).is_err());
    }
}
