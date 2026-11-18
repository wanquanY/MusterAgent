use crate::{FileEntry, FileQueryContext, FileQueryError, ListFiles};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use musteragent_content_ref::Sha256;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    query: String,
    revision: String,
    offset: usize,
}

pub(crate) fn fingerprint(
    context: &FileQueryContext,
    query: &ListFiles,
) -> Result<String, FileQueryError> {
    if let Some(dialogue) = &context.dialogue_source {
        let bytes = serde_json::to_vec(&(
            crate::AGENT_FILE_REFERENCE_SCHEMA,
            context.actor.tenant_id.to_string(),
            context.actor.principal_id.to_string(),
            dialogue.agent_id.to_string(),
            &context.authority_revision,
            query.scope,
            &query.parent,
            &query.name_query,
        ))
        .map_err(|_| FileQueryError::Integrity)?;
        // Retain old cursor bytes for unfiltered queries. A producer selector
        // receives its own identity and cannot reuse another task's cursor.
        return Ok(match &query.produced_by {
            Some(filter) => Sha256::digest(
                serde_json::to_vec(&(bytes, filter)).map_err(|_| FileQueryError::Integrity)?,
            )
            .to_string(),
            None => Sha256::digest(bytes).to_string(),
        });
    }
    let bytes = serde_json::to_vec(&(
        context.actor.tenant_id.to_string(),
        context.actor.principal_id.to_string(),
        context.conversation_id.map(|id| id.to_string()),
        &context.authority_revision,
        context.project_workspace_id.map(|id| id.to_string()),
        context.content_allowlist.as_ref().map(|refs| {
            let mut identities = refs
                .iter()
                .map(|r| {
                    (
                        r.content_id.to_string(),
                        r.sha256.to_string(),
                        r.media_type.as_str(),
                        r.byte_length,
                    )
                })
                .collect::<Vec<_>>();
            identities.sort();
            identities.dedup();
            identities
        }),
        query.scope,
        &query.parent,
        &query.name_query,
    ))
    .map_err(|_| FileQueryError::Integrity)?;
    Ok(Sha256::digest(bytes).to_string())
}

pub(crate) fn revision(entries: &[FileEntry]) -> Result<String, FileQueryError> {
    Ok(
        Sha256::digest(serde_json::to_vec(entries).map_err(|_| FileQueryError::Integrity)?)
            .to_string(),
    )
}

pub(crate) fn offset(
    cursor: Option<&str>,
    fingerprint: &str,
    revision: &str,
) -> Result<usize, FileQueryError> {
    let (saved_revision, offset) = position(cursor, fingerprint)?;
    if saved_revision
        .as_deref()
        .is_some_and(|saved| saved != revision)
    {
        return Err(FileQueryError::Changed);
    }
    Ok(offset)
}

pub(crate) fn position(
    cursor: Option<&str>,
    fingerprint: &str,
) -> Result<(Option<String>, usize), FileQueryError> {
    let Some(cursor) = cursor else {
        return Ok((None, 0));
    };
    if cursor.len() > 4096 {
        return Err(FileQueryError::InvalidQuery);
    }
    let c: Cursor = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(cursor)
            .map_err(|_| FileQueryError::InvalidQuery)?,
    )
    .map_err(|_| FileQueryError::InvalidQuery)?;
    if c.version != 1 || c.query != fingerprint {
        return Err(FileQueryError::InvalidQuery);
    }
    Ok((Some(c.revision), c.offset))
}

pub(crate) fn next(
    fingerprint: &str,
    revision: &str,
    offset: usize,
) -> Result<String, FileQueryError> {
    let c = Cursor {
        version: 1,
        query: fingerprint.to_owned(),
        revision: revision.to_owned(),
        offset,
    };
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&c).map_err(|_| FileQueryError::Integrity)?))
}

/// Shared pagination for bounded metadata-only catalogs. The fingerprint pins
/// the authenticated view; any changed source version invalidates continuation.
pub fn paginate_file_entries(
    context: &FileQueryContext,
    query: &ListFiles,
    entries: Vec<FileEntry>,
    coverage: crate::FileCoverage,
) -> Result<crate::FilePage, FileQueryError> {
    query.validate(context)?;
    let revision = revision(&entries)?;
    let fingerprint = fingerprint(context, query)?;
    let offset = offset(query.cursor.as_deref(), &fingerprint, &revision)?;
    if offset > entries.len() {
        return Err(FileQueryError::InvalidQuery);
    }
    let end = offset.saturating_add(query.limit).min(entries.len());
    Ok(crate::FilePage {
        retraction_status: None,
        entries: entries[offset..end].to_vec(),
        next_cursor: if end < entries.len() {
            Some(next(&fingerprint, &revision, end)?)
        } else {
            None
        },
        revision,
        coverage,
    })
}

/// Page a provider-owned immutable snapshot without constructing/serializing
/// every FileEntry. The provider's revision must identify the complete ordered
/// snapshot; authority and query parameters remain bound by this shared cursor.
pub fn paginate_file_snapshot<T>(
    context: &FileQueryContext,
    query: &ListFiles,
    revision: String,
    mut sources: impl Iterator<Item = T>,
    mut resolve: impl FnMut(T) -> Result<FileEntry, FileQueryError>,
) -> Result<crate::FilePage, FileQueryError> {
    query.validate(context)?;
    if revision.is_empty() || revision.len() > 512 {
        return Err(FileQueryError::Integrity);
    }
    let fingerprint = fingerprint(context, query)?;
    let offset = offset(query.cursor.as_deref(), &fingerprint, &revision)?;
    for _ in 0..offset {
        sources.next().ok_or(FileQueryError::InvalidQuery)?;
    }
    let entries = sources
        .by_ref()
        .take(query.limit)
        .map(&mut resolve)
        .collect::<Result<Vec<_>, _>>()?;
    let more = sources.next().is_some();
    let end = offset
        .checked_add(entries.len())
        .ok_or(FileQueryError::InvalidQuery)?;
    Ok(crate::FilePage {
        retraction_status: None,
        entries,
        next_cursor: more
            .then(|| next(&fingerprint, &revision, end))
            .transpose()?,
        revision,
        coverage: crate::FileCoverage::ProjectChildren,
    })
}

/// Async source owners resolve aliases/receipts only for the selected page,
/// after validating the COMPLETE bounded snapshot revision. Skipped entries do
/// not cause repository writes; cursor authority is identical to the sync path.
pub async fn paginate_file_snapshot_async<T, F, Fut>(
    context: &FileQueryContext,
    query: &ListFiles,
    revision: String,
    mut sources: impl Iterator<Item = T>,
    mut resolve: F,
) -> Result<crate::FilePage, FileQueryError>
where
    F: FnMut(T) -> Fut,
    Fut: std::future::Future<Output = Result<FileEntry, FileQueryError>>,
{
    query.validate(context)?;
    if revision.is_empty() || revision.len() > 512 {
        return Err(FileQueryError::Integrity);
    }
    let fingerprint = fingerprint(context, query)?;
    let offset = offset(query.cursor.as_deref(), &fingerprint, &revision)?;
    for _ in 0..offset {
        sources.next().ok_or(FileQueryError::InvalidQuery)?;
    }
    let mut entries = Vec::with_capacity(query.limit);
    for source in sources.by_ref().take(query.limit) {
        entries.push(resolve(source).await?);
    }
    let more = sources.next().is_some();
    let end = offset
        .checked_add(entries.len())
        .ok_or(FileQueryError::InvalidQuery)?;
    Ok(crate::FilePage {
        retraction_status: None,
        entries,
        next_cursor: more
            .then(|| next(&fingerprint, &revision, end))
            .transpose()?,
        revision,
        coverage: crate::FileCoverage::ProjectChildren,
    })
}
