use super::*;

pub(crate) struct SharingWindow {
    pub(crate) prepared: i64,
    pub(crate) expires: i64,
}

pub(super) fn request_hash(request: &Prepare) -> Sha256 {
    let mut bytes = b"musteragent.dialogue-publication-prepare.v1\0".to_vec();
    for id in [
        request.actor.tenant_id.into_uuid(),
        request.actor.principal_id.into_uuid(),
        request.dialogue_id.into_uuid(),
        request.entry_id.into_uuid(),
        request.expected_head.into_uuid(),
        request.command_id.into_uuid(),
        request.audience.share_id,
        request.audience.publication_revision_id,
    ] {
        bytes.extend_from_slice(id.as_bytes());
    }
    Sha256::digest(bytes)
}
pub(super) fn source_hash(
    request: Sha256,
    binding: Uuid,
    agent: AgentId,
    artifact: ArtifactId,
    title: &str,
    references: &[ContentRef],
    SharingWindow { prepared, expires }: SharingWindow,
) -> Sha256 {
    let mut bytes = b"musteragent.dialogue-publication-source.v1\0".to_vec();
    bytes.extend_from_slice(&request.into_bytes());
    for id in [binding, agent.into_uuid(), artifact.into_uuid()] {
        bytes.extend_from_slice(id.as_bytes());
    }
    for at in [prepared, expires] {
        bytes.extend_from_slice(&at.to_be_bytes());
    }
    field(&mut bytes, title.as_bytes());
    bytes.extend_from_slice(&(references.len() as u32).to_be_bytes());
    for reference in references {
        bytes.extend_from_slice(reference.content_id.into_uuid().as_bytes());
        field(&mut bytes, reference.media_type.as_str().as_bytes());
        bytes.extend_from_slice(&reference.byte_length.to_be_bytes());
        bytes.extend_from_slice(&reference.sha256.into_bytes());
    }
    Sha256::digest(bytes)
}
fn field(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}
pub(super) fn source(row: &PgRow) -> Result<ArtifactPublicationSourceRef, Error> {
    Ok(ArtifactPublicationSourceRef {
        binding_id: row.try_get("binding_id").map_err(sql)?,
        audience: ArtifactPublicationAudience {
            share_id: row.try_get("share_id").map_err(sql)?,
            publication_revision_id: row.try_get("publication_revision_id").map_err(sql)?,
        },
        source_sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("source_sha256").map_err(sql)?)
            .map_err(|_| Error::Invariant)?,
    })
}
pub(super) fn state(row: &PgRow) -> Result<State, Error> {
    match row.try_get::<i16, _>("state").map_err(sql)? {
        1 => Ok(State::Prepared),
        2 => Ok(State::Retained),
        3 => Ok(State::Revoked),
        4 => Ok(State::Expired),
        _ => Err(Error::Invariant),
    }
}
pub(super) fn reference(row: &PgRow) -> Result<ContentRef, Error> {
    Ok(ContentRef {
        content_id: ContentId::from_uuid(row.try_get("content_id").map_err(sql)?)
            .map_err(|_| Error::Invariant)?,
        media_type: MediaType::new(
            row.try_get::<String, _>("content_media_type")
                .map_err(sql)?,
        )
        .map_err(|_| Error::Invariant)?,
        byte_length: row
            .try_get::<String, _>("content_byte_length")
            .map_err(sql)?
            .parse()
            .map_err(|_| Error::Invariant)?,
        sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash").map_err(sql)?)
            .map_err(|_| Error::Invariant)?,
    })
}
