//! Immutable publications owned by one host. The catalog is consulted at admission.
use super::{WorkspaceContractError as Error, WorkspaceToolRelease, parse_release};
use musteragent_content_ref::Sha256;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document<'a> {
    schema_version: String,
    #[serde(borrow)]
    releases: Vec<&'a serde_json::value::RawValue>,
}

/// Verified release plus the exact JSON bytes used for durable publication.
#[derive(Debug)]
pub struct WorkspaceReleasePublication {
    release: WorkspaceToolRelease,
    bytes: Vec<u8>,
    sha256: Sha256,
}

impl WorkspaceReleasePublication {
    pub fn release(&self) -> &WorkspaceToolRelease {
        &self.release
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn sha256(&self) -> Sha256 {
        self.sha256
    }
}

#[derive(Debug)]
pub struct WorkspaceReleaseCatalog {
    releases: BTreeMap<String, BTreeMap<String, WorkspaceReleasePublication>>,
}

impl WorkspaceReleaseCatalog {
    pub const MAX_BYTES: usize = 32 * 1024 * 1024;

    pub fn from_json(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > Self::MAX_BYTES {
            return Err(Error);
        }
        let document: Document<'_> = serde_json::from_slice(bytes).map_err(|_| Error)?;
        if document.schema_version != "musteragent.workspace-tool-releases.v1"
            || document.releases.is_empty()
        {
            return Err(Error);
        }
        let mut releases = BTreeMap::<_, BTreeMap<_, _>>::new();
        for value in document.releases {
            let bytes = value.get().as_bytes();
            let sha256 = Sha256::digest(bytes);
            let release = parse_release(bytes, &sha256.into_bytes())?;
            let versions = releases.entry(release.plugin_id.clone()).or_default();
            if versions
                .insert(
                    release.version.clone(),
                    WorkspaceReleasePublication {
                        release,
                        bytes: bytes.to_vec(),
                        sha256,
                    },
                )
                .is_some()
            {
                return Err(Error);
            }
        }
        let catalog = Self { releases };
        let mut contracts = BTreeMap::new();
        for publication in catalog.publications() {
            for tool in &publication.release.tools {
                if let Some(previous) = contracts.insert(&tool.key, tool)
                    && previous.definition()? != tool.definition()?
                {
                    return Err(Error);
                }
            }
        }
        Ok(catalog)
    }

    pub fn publications(&self) -> impl Iterator<Item = &WorkspaceReleasePublication> {
        self.releases.values().flat_map(BTreeMap::values)
    }

    pub fn resolve(
        &self,
        plugin: &str,
        version: &str,
        digest: &str,
    ) -> Result<&WorkspaceReleasePublication, Error> {
        self.releases
            .get(plugin)
            .and_then(|versions| versions.get(version))
            .filter(|publication| publication.release.release_sha256 == digest)
            .ok_or(Error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const BUNDLE: &[u8] = include_bytes!(
        "../../../../../applications/reference-api/examples/workspace-releases.json"
    );

    #[test]
    fn exact_lookup_is_scoped_to_the_host_publication() {
        let catalog = WorkspaceReleaseCatalog::from_json(BUNDLE).unwrap();
        let publication = catalog
            .resolve("mcp.example-search", "1.0.0", &"2".repeat(64))
            .unwrap();
        assert_eq!(Sha256::digest(publication.bytes()), publication.sha256());
        for (name, version, digest) in [
            ("unknown", "1.0.0", "2".repeat(64)),
            ("mcp.example-search", "2.0.0", "2".repeat(64)),
            ("mcp.example-search", "1.0.0", "1".repeat(64)),
        ] {
            assert_eq!(catalog.resolve(name, version, &digest).unwrap_err(), Error);
        }
    }

    #[test]
    fn bundle_rejects_ambiguous_releases_or_conflicting_shared_operations() {
        let mut doc: serde_json::Value = serde_json::from_slice(BUNDLE).unwrap();
        let releases = doc["releases"].as_array_mut().unwrap();
        releases.push(releases[0].clone());
        let parse = |d: &serde_json::Value| {
            WorkspaceReleaseCatalog::from_json(&serde_json::to_vec(d).unwrap())
        };
        assert_eq!(parse(&doc).unwrap_err(), Error);
        doc["releases"][3]["version"] = "2".into();
        assert!(parse(&doc).is_ok());
        doc["releases"][3]["tools"][0]["input_schema"]["properties"]["extra"] =
            serde_json::json!({"type":"integer"});
        assert_eq!(parse(&doc).unwrap_err(), Error);
        doc["releases"][0]["tools"][0]["input_schema"]["type"] = "string".into();
        doc["releases"].as_array_mut().unwrap().truncate(1);
        assert_eq!(parse(&doc).unwrap_err(), Error);
        doc["releases"] = serde_json::json!([]);
        assert_eq!(parse(&doc).unwrap_err(), Error);
    }
}
