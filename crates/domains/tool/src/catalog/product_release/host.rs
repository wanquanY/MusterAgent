//! Host publications are installed once, before admission or worker startup.
//! There is no mutable active pointer: every lookup requires the exact digest.
use super::*;
use serde::Deserialize;
use std::sync::OnceLock;

/// A validated, immutable publication owned by a downstream host.
#[derive(Clone)]
pub struct HostProductRelease {
    reference: ProductReleaseRef,
    skill_release: ProductReleaseRef,
    pub(super) profile_id: String,
    pub(super) cloud_head: String,
    pub(super) device_head: String,
    pub(super) skill_schema: String,
    pub(super) cloud: BuiltinToolCatalog,
    pub(super) device: BuiltinToolCatalog,
}

/// Publication errors never select another release.
#[derive(Debug, thiserror::Error)]
pub enum HostPublicationError {
    #[error("invalid host publication document")]
    InvalidDocument,
    #[error("invalid host tool contract: {0}")]
    InvalidContract(#[from] ToolCatalogError),
    #[error("host publications already installed with different identities")]
    AlreadyInstalled,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema_version: String,
    skill_release: SkillSource,
    product: String,
    release_id: String,
    profile_id: String,
    cloud_head: String,
    device_head: String,
    skill_schema: String,
    cloud: Vec<super::super::released::ReleasedTool>,
    device: Vec<super::super::released::ReleasedTool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillSource {
    product: String,
    release_id: String,
    digest: [u8; 32],
}

impl HostProductRelease {
    /// Decode a trusted host publication; its identity hashes the exact bytes.
    /// Hosts distribute the same bytes to admission and every worker generation.
    pub fn from_json(bytes: &[u8]) -> Result<Self, HostPublicationError> {
        use HostPublicationError::InvalidDocument;
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(InvalidDocument);
        }
        let d: Document = serde_json::from_slice(bytes).map_err(|_| InvalidDocument)?;
        let product = ProductId::parse(&d.product).map_err(|_| InvalidDocument)?;
        if d.schema_version != "musteragent.host-tool-release.v1"
            || product == ProductId::KERNEL
            || [
                &d.profile_id,
                &d.cloud_head,
                &d.device_head,
                &d.skill_schema,
            ]
            .iter()
            .any(|v| v.is_empty() || v.len() > 256)
            || d.cloud.is_empty()
            || d.device.is_empty()
            || d.cloud.len() > 4096
            || d.device.len() > 4096
        {
            return Err(InvalidDocument);
        }
        let reference = ProductReleaseRef::new(
            product,
            VersionedKey::new(d.release_id).map_err(|_| InvalidDocument)?,
            Sha256::digest(bytes).into_bytes(),
        )
        .map_err(|_| InvalidDocument)?;
        let skill_release = ProductReleaseRef::new(
            ProductId::parse(&d.skill_release.product).map_err(|_| InvalidDocument)?,
            VersionedKey::new(d.skill_release.release_id).map_err(|_| InvalidDocument)?,
            d.skill_release.digest,
        )
        .map_err(|_| InvalidDocument)?;
        // This version explicitly composes a retained kernel Skill publication.
        // No process default, active pointer or recursive host alias is allowed.
        let skills = resolve(&skill_release)?;
        if skills.skill_schema != d.skill_schema {
            return Err(InvalidDocument);
        }
        let cloud_count = d.cloud.len();
        let device_count = d.device.len();
        let cloud = BuiltinToolCatalog::from_raw(
            d.cloud
                .into_iter()
                .map(|v| v.into_raw())
                .collect::<Result<_, _>>()?,
            cloud_count,
        )?;
        let device = BuiltinToolCatalog::from_raw(
            d.device
                .into_iter()
                .map(|v| v.into_raw())
                .collect::<Result<_, _>>()?,
            device_count,
        )?;
        Ok(Self {
            reference,
            skill_release,
            profile_id: d.profile_id,
            cloud_head: d.cloud_head,
            device_head: d.device_head,
            skill_schema: d.skill_schema,
            cloud,
            device,
        })
    }
    /// Full publication identity, including its byte digest.
    pub fn reference(&self) -> &ProductReleaseRef {
        &self.reference
    }
}

static HOST_RELEASES: OnceLock<BTreeMap<ProductReleaseRef, HostProductRelease>> = OnceLock::new();

/// Freeze host publications once. Repeating an identical installation is safe;
/// adding or replacing a release requires a new process generation.
pub fn install_host_product_releases(
    releases: Vec<HostProductRelease>,
) -> Result<(), HostPublicationError> {
    if releases.len() > 128 {
        return Err(HostPublicationError::InvalidDocument);
    }
    let mut entries = BTreeMap::new();
    for release in releases {
        if entries.insert(release.reference.clone(), release).is_some() {
            return Err(HostPublicationError::InvalidDocument);
        }
    }
    if let Err(entries) = HOST_RELEASES.set(entries)
        && HOST_RELEASES
            .get()
            .is_none_or(|installed| !installed.keys().eq(entries.keys()))
    {
        return Err(HostPublicationError::AlreadyInstalled);
    }
    Ok(())
}

pub(super) fn host_release(reference: &ProductReleaseRef) -> Option<&'static HostProductRelease> {
    HOST_RELEASES.get()?.get(reference)
}

/// Exact Skill source explicitly selected in a trusted host publication. Kernel
/// publications resolve to themselves; unknown identities are rejected.
pub fn product_skill_source(
    reference: &ProductReleaseRef,
) -> Result<&ProductReleaseRef, ToolCatalogError> {
    if let Some(host) = host_release(reference) {
        Ok(&host.skill_release)
    } else {
        resolve(reference)?;
        Ok(reference)
    }
}
