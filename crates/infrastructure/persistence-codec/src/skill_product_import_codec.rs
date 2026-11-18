//! Closed original Skill import protocol. Public product gateways never select
//! arbitrary provenance, trust, URLs or execution authority.
use crate::{wire, wire_common};
use musteragent_identity::{Actor, CommandId};
use musteragent_runtime_skill::{
    ImportProductSkill, ProductSkillImportDisposition as Disposition,
    ProductSkillImportError as Error, ProductSkillImportOrigin as Origin, ProductSkillImportRecord,
    ProductSkillImportStatus as Status, ProductSkillImportTarget as Target,
    ProductSkillMarketplaceSource,
};
use musteragent_skill::Surface;

pub fn actor(value: Option<wire::Actor>) -> Result<Actor, Error> {
    let input = value.as_ref().ok_or(Error::InvalidRequest)?;
    canonical::<uuid::Uuid>(input.tenant_id.as_ref().map(|id| id.value.as_str()))?;
    canonical::<uuid::Uuid>(input.principal_id.as_ref().map(|id| id.value.as_str()))?;
    let actor = wire_common::decode_actor(value).map_err(|_| Error::InvalidRequest)?;
    musteragent_runtime_skill::validate_actor(actor)?;
    Ok(actor)
}
pub fn command(value: Option<wire::CommandId>) -> Result<CommandId, Error> {
    canonical(value.as_ref().map(|id| id.value.as_str()))
}
fn canonical<T: std::str::FromStr + std::fmt::Display>(value: Option<&str>) -> Result<T, Error> {
    let value = value.ok_or(Error::InvalidRequest)?;
    let parsed: T = value.parse().map_err(|_| Error::InvalidRequest)?;
    if parsed.to_string() != value || value == "00000000-0000-0000-0000-000000000000" {
        return Err(Error::InvalidRequest);
    }
    Ok(parsed)
}
pub fn request(value: wire::ImportProductSkillRequest) -> Result<ImportProductSkill, Error> {
    let archive = value.archive.ok_or(Error::InvalidRequest)?;
    canonical::<uuid::Uuid>(archive.content_id.as_ref().map(|id| id.value.as_str()))?;
    let request = ImportProductSkill {
        account_installation: value
            .account_installation
            .map(decode_account_installation)
            .transpose()?,
        actor: actor(value.actor)?,
        command_id: command(value.command_id)?,
        origin: decode_origin(
            value.uploaded_archive,
            &value.market_skill_id,
            &value.market_version_id,
            &value.market_publisher_id,
        )?,
        surface: match wire::SkillSurface::try_from(value.surface) {
            Ok(wire::SkillSurface::Web) => Surface::Web,
            Ok(wire::SkillSurface::Desktop) => Surface::Desktop,
            Ok(wire::SkillSurface::Mobile) => Surface::Mobile,
            _ => return Err(Error::InvalidRequest),
        },
        disposition: match wire::ProductSkillImportDisposition::try_from(value.disposition) {
            Ok(wire::ProductSkillImportDisposition::SaveOnly) => Disposition::SaveOnly,
            Ok(wire::ProductSkillImportDisposition::Install) => Disposition::Install,
            Ok(wire::ProductSkillImportDisposition::Materialize) => Disposition::Materialize,
            _ => return Err(Error::InvalidRequest),
        },
        archive: wire_common::decode_content_ref(archive).map_err(|_| Error::InvalidRequest)?,
    };
    request.validate()?;
    Ok(request)
}
pub fn encode_request(value: &ImportProductSkill) -> wire::ImportProductSkillRequest {
    wire::ImportProductSkillRequest {
        actor: Some(wire_common::actor(value.actor)),
        command_id: Some(wire::CommandId {
            value: value.command_id.to_string(),
        }),
        market_skill_id: value
            .origin
            .marketplace()
            .map(|s| s.market_skill_id.to_string())
            .unwrap_or_default(),
        market_version_id: value
            .origin
            .marketplace()
            .map(|s| s.market_version_id.to_string())
            .unwrap_or_default(),
        market_publisher_id: value
            .origin
            .marketplace()
            .map(|s| s.market_publisher_id.to_string())
            .unwrap_or_default(),
        surface: encode_surface(value.surface),
        disposition: encode_disposition(value.disposition),
        archive: Some(crate::encode_content_ref(&value.archive)),
        uploaded_archive: matches!(value.origin, Origin::UploadedArchive),
        account_installation: value.account_installation.map(encode_account_installation),
    }
}
fn encode_surface(value: Surface) -> i32 {
    match value {
        Surface::Web => wire::SkillSurface::Web,
        Surface::Desktop => wire::SkillSurface::Desktop,
        Surface::Mobile => wire::SkillSurface::Mobile,
        _ => wire::SkillSurface::Unspecified,
    }
    .into()
}
fn encode_disposition(value: Disposition) -> i32 {
    match value {
        Disposition::SaveOnly => wire::ProductSkillImportDisposition::SaveOnly,
        Disposition::Install => wire::ProductSkillImportDisposition::Install,
        Disposition::Materialize => wire::ProductSkillImportDisposition::Materialize,
    }
    .into()
}
pub fn response(
    value: ProductSkillImportRecord,
) -> Result<wire::ProductSkillImportResponse, Error> {
    let prepared = &value.prepared;
    prepared.request.validate()?;
    let receipt = match value.status {
        Status::Accepted if value.completed_at.is_none() && value.pointer_version.is_none() => None,
        Status::Conflict if value.completed_at.is_some() && value.pointer_version.is_none() => None,
        Status::Completed
            if value.completed_at.is_some() && value.pointer_version.is_some_and(|v| v > 0) =>
        {
            Some(wire::ProductSkillImportReceipt {
                skill_id: Some(wire::SkillId {
                    value: prepared.identity.skill_id.to_string(),
                }),
                skill_version_id: Some(wire::SkillVersionId {
                    value: prepared.skill_version_id.to_string(),
                }),
                pointer_version: value.pointer_version.ok_or(Error::Corrupt)?,
                created_version: prepared.created_version,
            })
        }
        _ => return Err(Error::Corrupt),
    };
    if value
        .completed_at
        .is_some_and(|at| at < prepared.accepted_at)
    {
        return Err(Error::Corrupt);
    }
    let request = &prepared.request;
    Ok(wire::ProductSkillImportResponse {
        operation: Some(wire::ProductSkillImportOperation {
            account_installation: request
                .account_installation
                .map(encode_account_installation),
            command_id: Some(wire::CommandId {
                value: request.command_id.to_string(),
            }),
            market_skill_id: request
                .origin
                .marketplace()
                .map(|s| s.market_skill_id.to_string())
                .unwrap_or_default(),
            market_version_id: request
                .origin
                .marketplace()
                .map(|s| s.market_version_id.to_string())
                .unwrap_or_default(),
            market_publisher_id: request
                .origin
                .marketplace()
                .map(|s| s.market_publisher_id.to_string())
                .unwrap_or_default(),
            surface: encode_surface(request.surface),
            disposition: encode_disposition(request.disposition),
            request_digest: Some(wire_common::sha256(request.digest())),
            status: match value.status {
                Status::Accepted => wire::ProductSkillImportStatus::Accepted,
                Status::Completed => wire::ProductSkillImportStatus::Completed,
                Status::Conflict => wire::ProductSkillImportStatus::Conflict,
            }
            .into(),
            accepted_at_unix_ms: prepared.accepted_at.unix_millis(),
            completed_at_unix_ms: value.completed_at.map(|at| at.unix_millis()),
            receipt,
            uploaded_archive: matches!(request.origin, Origin::UploadedArchive),
            uploaded_archive_sha256: matches!(request.origin, Origin::UploadedArchive)
                .then(|| wire_common::sha256(request.archive.sha256)),
        }),
    })
}

pub fn query(
    value: wire::ListProductSkillImportsRequest,
) -> Result<musteragent_runtime_skill::ProductSkillImportQuery, Error> {
    let query = musteragent_runtime_skill::ProductSkillImportQuery {
        actor: actor(value.actor)?,
        target: if value.uploaded_archives {
            if !value.market_skill_id.is_empty() || value.market_version_id.is_some() {
                return Err(Error::InvalidRequest);
            }
            Target::UploadedArchives
        } else {
            Target::Marketplace {
                skill_id: canonical(Some(&value.market_skill_id))?,
                version_id: value
                    .market_version_id
                    .as_deref()
                    .map(|v| canonical(Some(v)))
                    .transpose()?,
            }
        },
        surface: match wire::SkillSurface::try_from(value.surface) {
            Ok(wire::SkillSurface::Web) => Surface::Web,
            Ok(wire::SkillSurface::Desktop) => Surface::Desktop,
            Ok(wire::SkillSurface::Mobile) => Surface::Mobile,
            _ => return Err(Error::InvalidRequest),
        },
        limit: value.limit,
        cursor: value.cursor,
    };
    query.validate()?;
    Ok(query)
}
pub fn page(
    value: musteragent_runtime_skill::ProductSkillImportPage,
) -> Result<wire::ListProductSkillImportsResponse, Error> {
    Ok(wire::ListProductSkillImportsResponse {
        operations: value
            .records
            .into_iter()
            .map(|r| response(r)?.operation.ok_or(Error::Corrupt))
            .collect::<Result<_, _>>()?,
        next_cursor: value.next_cursor,
    })
}

fn decode_origin(
    uploaded: bool,
    skill: &str,
    version: &str,
    publisher: &str,
) -> Result<Origin, Error> {
    if uploaded {
        if !skill.is_empty() || !version.is_empty() || !publisher.is_empty() {
            return Err(Error::InvalidRequest);
        }
        Ok(Origin::UploadedArchive)
    } else {
        Ok(Origin::Marketplace(ProductSkillMarketplaceSource {
            market_skill_id: canonical(Some(skill))?,
            market_version_id: canonical(Some(version))?,
            market_publisher_id: canonical(Some(publisher))?,
        }))
    }
}

pub fn decode_account_installation(
    value: wire::ProductSkillAccountInstallation,
) -> Result<musteragent_runtime_skill::SkillAccountInstallation, Error> {
    let result = musteragent_runtime_skill::SkillAccountInstallation {
        product: musteragent_runtime_skill::SkillAccountProduct::parse(&value.product)
            .map_err(|_| Error::InvalidRequest)?,
        revision: value.revision,
    };
    result.validate()?;
    Ok(result)
}
pub fn encode_account_installation(
    value: musteragent_runtime_skill::SkillAccountInstallation,
) -> wire::ProductSkillAccountInstallation {
    wire::ProductSkillAccountInstallation {
        product: value.product.as_str().into(),
        revision: value.revision,
    }
}
pub fn withdrawal(
    value: wire::WithdrawProductSkillRequest,
) -> Result<musteragent_runtime_skill::WithdrawProductSkill, Error> {
    let result = musteragent_runtime_skill::WithdrawProductSkill {
        actor: actor(value.actor)?,
        market_skill_id: canonical(Some(&value.market_skill_id))?,
        installation: decode_account_installation(
            value.installation.ok_or(Error::InvalidRequest)?,
        )?,
    };
    result.validate()?;
    Ok(result)
}
