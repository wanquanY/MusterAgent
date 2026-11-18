//! Frozen account authority compacted without changing the original per-tool
//! witness. Canonical encoding matches the public protobuf contract exactly.
use super::{ApplicationCatalogReference, Error, MAX_CATALOG_TOOLS};
use musteragent_content_ref::Sha256;
use std::{collections::BTreeSet, sync::Arc};

/// Same exact per-source envelope budget as the publication projection Owner.
pub const MAX_SELECTION_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApplicationCatalogScopeGroup {
    pub scopes: Vec<String>,
    pub ordinals: Vec<u16>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FrozenApplicationCatalog {
    reference: ApplicationCatalogReference,
    authority_template_json: Vec<u8>,
    dialogue_witness_template: Vec<u8>,
    scope_groups: Vec<ApplicationCatalogScopeGroup>,
    selection_sha256: Sha256,
    ordinal_groups: Arc<[u16]>,
}
impl FrozenApplicationCatalog {
    pub fn new(
        reference: ApplicationCatalogReference,
        authority_template_json: Vec<u8>,
        dialogue_witness_template: Vec<u8>,
        scope_groups: Vec<ApplicationCatalogScopeGroup>,
        selection_sha256: Sha256,
    ) -> Result<Self, Error> {
        let ordinal_groups = ordinal_index(&scope_groups)?;
        let value = Self {
            reference,
            authority_template_json,
            dialogue_witness_template,
            scope_groups,
            selection_sha256,
            ordinal_groups,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn reference(&self) -> &ApplicationCatalogReference {
        &self.reference
    }
    pub fn authority_template_json(&self) -> &[u8] {
        &self.authority_template_json
    }
    pub fn dialogue_witness_template(&self) -> &[u8] {
        &self.dialogue_witness_template
    }
    pub fn scope_groups(&self) -> &[ApplicationCatalogScopeGroup] {
        &self.scope_groups
    }
    pub fn selection_sha256(&self) -> Sha256 {
        self.selection_sha256
    }
    pub fn scopes_for(&self, ordinal: u16) -> Option<&[String]> {
        self.ordinal_groups
            .get(usize::from(ordinal))
            .and_then(|group| self.scope_groups.get(usize::from(*group)))
            .map(|group| group.scopes.as_slice())
    }
    pub fn selected_count(&self) -> usize {
        self.scope_groups.iter().map(|g| g.ordinals.len()).sum()
    }
    /// Admission may narrow the authenticated selection, never add an ordinal
    /// or alter a scope tuple. This keeps the original per-tool witness exact.
    pub fn retain_ordinals(&self, allowed: &BTreeSet<u16>) -> Result<Option<Self>, Error> {
        self.validate()?;
        if allowed.iter().any(|o| self.scopes_for(*o).is_none()) {
            return Err(Error);
        }
        if allowed.is_empty() {
            return Ok(None);
        }
        let mut narrowed = self.clone();
        narrowed.scope_groups = self
            .scope_groups
            .iter()
            .filter_map(|g| {
                let ordinals = g
                    .ordinals
                    .iter()
                    .filter(|o| allowed.contains(o))
                    .copied()
                    .collect::<Vec<_>>();
                (!ordinals.is_empty()).then(|| ApplicationCatalogScopeGroup {
                    scopes: g.scopes.clone(),
                    ordinals,
                })
            })
            .collect();
        narrowed.ordinal_groups = ordinal_index(&narrowed.scope_groups)?;
        let mut bytes = b"musteragent.frozen-application-catalog.v1\0".to_vec();
        bytes.extend(narrowed.canonical_encoding(false));
        narrowed.selection_sha256 = Sha256::digest(bytes);
        narrowed.validate()?;
        Ok(Some(narrowed))
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.encoded_len()? > MAX_SELECTION_BYTES {
            return Err(Error);
        }
        self.reference.validate()?;
        if self.authority_template_json.is_empty()
            || self.authority_template_json.len() > 16 * 1024
            || self.dialogue_witness_template.len() > 32 * 1024
            || self.scope_groups.is_empty()
            || self.scope_groups.len() > MAX_CATALOG_TOOLS
        {
            return Err(Error);
        }
        let value: serde_json::Value =
            serde_json::from_slice(&self.authority_template_json).map_err(|_| Error)?;
        let object = value.as_object().ok_or(Error)?;
        let format = object.get("format").and_then(|v| v.as_str()).ok_or(Error)?;
        let mut expected = BTreeSet::from([
            "format",
            "tenant_id",
            "owner_id",
            "product",
            "surface",
            "entry_id",
            "release_id",
            "installation_revision",
            "connection_id",
            "connection_revision",
            "connector_release_id",
        ]);
        if format == "account-application-consumer-witness/v2" {
            expected.extend([
                "installation_authority_id",
                "installation_authorization_epoch",
                "connection_authorization_epoch",
            ]);
        } else if format != "account-application-consumer-witness/v1" {
            return Err(Error);
        }
        if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
            || canonical_witness_json(&value)? != self.authority_template_json
            || object.get("connector_release_id").and_then(|v| v.as_str())
                != Some(self.reference.source_id.as_str())
        {
            return Err(Error);
        }
        let product = object
            .get("product")
            .and_then(|v| v.as_str())
            .ok_or(Error)?;
        musteragent_identity::ProductId::parse(product).map_err(|_| Error)?;
        if !matches!(
            object.get("surface").and_then(|v| v.as_str()),
            Some("web" | "desktop" | "mobile")
        ) {
            return Err(Error);
        }
        let mut previous: Option<&[String]> = None;
        let mut ordinals = BTreeSet::new();
        for group in &self.scope_groups {
            if previous.is_some_and(|p| p >= group.scopes.as_slice())
                || group.scopes.len() > 100
                || group.scopes.iter().any(|s| s.is_empty() || s.len() > 2048)
                || group.scopes.iter().collect::<BTreeSet<_>>().len() != group.scopes.len()
                || group.ordinals.is_empty()
                || group.ordinals.windows(2).any(|p| p[0] >= p[1])
            {
                return Err(Error);
            }
            for ordinal in &group.ordinals {
                if usize::from(*ordinal) >= MAX_CATALOG_TOOLS || !ordinals.insert(*ordinal) {
                    return Err(Error);
                }
            }
            previous = Some(&group.scopes);
        }
        let mut bytes = b"musteragent.frozen-application-catalog.v1\0".to_vec();
        bytes.extend(self.canonical_encoding(false));
        if Sha256::digest(&bytes) != self.selection_sha256 {
            return Err(Error);
        }
        Ok(())
    }
    pub fn encoded_len(&self) -> Result<usize, Error> {
        let reference = [
            self.reference.family.len(),
            self.reference.source_id.len(),
            32,
            32,
        ]
        .into_iter()
        .map(field_len)
        .sum::<usize>();
        let mut length = field_len(reference)
            + field_len(self.authority_template_json.len())
            + field_len(self.dialogue_witness_template.len())
            + field_len(32);
        if length > MAX_SELECTION_BYTES {
            return Err(Error);
        }
        for group in &self.scope_groups {
            let mut group_len = field_len(group.ordinals.len().checked_mul(2).ok_or(Error)?);
            for scope in &group.scopes {
                group_len = group_len.checked_add(field_len(scope.len())).ok_or(Error)?;
            }
            length = length.checked_add(field_len(group_len)).ok_or(Error)?;
            if length > MAX_SELECTION_BYTES {
                return Err(Error);
            }
        }
        Ok(length)
    }
    /// Deterministic wire bytes, with ascending fields and no unknown fields.
    /// This encoder owns only the small frozen selection, not Dialogue witness
    /// protobufs; those are validated and re-encoded by their existing codec.
    pub fn canonical_encoding(&self, include_hash: bool) -> Vec<u8> {
        let mut reference = vec![];
        field(&mut reference, 1, self.reference.family.as_bytes());
        field(&mut reference, 2, self.reference.source_id.as_bytes());
        field(
            &mut reference,
            3,
            &self.reference.release_sha256.into_bytes(),
        );
        field(&mut reference, 4, &self.reference.index_sha256.into_bytes());
        let mut encoded = vec![];
        field(&mut encoded, 1, &reference);
        field(&mut encoded, 2, &self.authority_template_json);
        field(&mut encoded, 3, &self.dialogue_witness_template);
        for group in &self.scope_groups {
            let mut bytes = vec![];
            for scope in &group.scopes {
                field(&mut bytes, 1, scope.as_bytes());
            }
            let ordinals = group
                .ordinals
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>();
            field(&mut bytes, 2, &ordinals);
            field(&mut encoded, 4, &bytes);
        }
        if include_hash {
            field(&mut encoded, 5, &self.selection_sha256.into_bytes());
        }
        encoded
    }
}
fn field_len(length: usize) -> usize {
    if length == 0 {
        return 0;
    }
    let mut count = 1;
    let mut remaining = length;
    while remaining >= 128 {
        count += 1;
        remaining >>= 7;
    }
    1 + count + length
}
fn ordinal_index(groups: &[ApplicationCatalogScopeGroup]) -> Result<Arc<[u16]>, Error> {
    if groups.len() > MAX_CATALOG_TOOLS {
        return Err(Error);
    }
    let mut indexed = vec![u16::MAX; MAX_CATALOG_TOOLS];
    for (group, value) in groups.iter().enumerate() {
        for ordinal in &value.ordinals {
            let slot = indexed.get_mut(usize::from(*ordinal)).ok_or(Error)?;
            if *slot != u16::MAX {
                return Err(Error);
            }
            *slot = u16::try_from(group).map_err(|_| Error)?;
        }
    }
    while indexed.last() == Some(&u16::MAX) {
        indexed.pop();
    }
    Ok(indexed.into())
}
fn field(out: &mut Vec<u8>, number: u8, value: &[u8]) {
    if value.is_empty() {
        return;
    }
    out.push((number << 3) | 2);
    let mut length = value.len();
    while length >= 128 {
        out.push((length as u8 & 127) | 128);
        length >>= 7;
    }
    out.push(length as u8);
    out.extend(value);
}

/// Python's original selection identity uses sorted JSON with ensure_ascii;
/// source objects use UTF-8. Keep these deliberately distinct contracts.
pub fn canonical_witness_json(value: &serde_json::Value) -> Result<Vec<u8>, Error> {
    let json = serde_json::to_string(value).map_err(|_| Error)?;
    let mut ascii = String::with_capacity(json.len());
    use std::fmt::Write;
    for c in json.chars() {
        if c.is_ascii() {
            ascii.push(c);
        } else {
            let mut units = [0; 2];
            for unit in c.encode_utf16(&mut units) {
                write!(&mut ascii, "\\u{unit:04x}").map_err(|_| Error)?;
            }
        }
    }
    Ok(ascii.into_bytes())
}
