//! Original Skill write owner. Content/audit CPU work happens before this
//! transaction; lifecycle writes and the command receipt commit together.
use super::persistence as sql;
use super::*;
use musteragent_identity::CommandId;
use musteragent_persistence_codec::{skill_product_import_codec as codec, wire};
use musteragent_runtime_skill::ActiveSkillVersionGuard;
use musteragent_runtime_skill::{
    ImportProductSkill, PreparedProductSkillImport, ProductSkillImportDisposition,
    ProductSkillImportError as Error, ProductSkillImportMaterial, ProductSkillImportOrigin,
    ProductSkillImportPage, ProductSkillImportQuery, ProductSkillImportRecord,
    ProductSkillImportStatus as Status, ProductSkillImportStore, ProductSkillMarketplaceSource,
    validate_actor,
};
use prost::Message;
use std::collections::BTreeSet;

impl ProductSkillImportStore for PostgresSkillLifecycleStore {
    fn withdraw(
        &self,
        request: musteragent_runtime_skill::WithdrawProductSkill,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<musteragent_runtime_skill::ProductSkillAccountState, Error>>
    {
        Box::pin(self.withdraw_account_installation(request, now))
    }
    fn list(
        &self,
        query: ProductSkillImportQuery,
    ) -> ApplicationFuture<'_, Result<ProductSkillImportPage, Error>> {
        Box::pin(async move {
            query.validate()?;
            let after = query.after()?;
            let rows=sqlx::query("SELECT * FROM skill.product_import_commands WHERE tenant_id=$1 AND principal_id=$2 AND source_kind=$9 AND ($3::uuid IS NULL OR market_skill_id=$3) AND ($4::uuid IS NULL OR market_version_id=$4) AND surface=$5 AND ($6::bigint IS NULL OR (accepted_at_ms,command_id)<($6,$7)) ORDER BY accepted_at_ms DESC,command_id DESC LIMIT $8")
                .bind(query.actor.tenant_id.into_uuid()).bind(query.actor.principal_id.into_uuid()).bind(query.target.market_ids().0).bind(query.target.market_ids().1).bind(query.surface.as_str())
                .bind(after.map(|(at,_)|at.unix_millis())).bind(after.map(|(_,id)|id.into_uuid())).bind(i64::from(query.limit)+1).bind(query.target.source_kind())
                .fetch_all(&self.pool).await.map_err(unavailable)?;
            let mut records = rows
                .iter()
                .map(|row| {
                    decode(
                        row,
                        query.actor,
                        id(row.try_get("command_id").map_err(unavailable)?).map_err(store_error)?,
                    )
                })
                .collect::<Result<Vec<_>, Error>>()?;
            let has_more = records.len() > query.limit as usize;
            records.truncate(query.limit as usize);
            let next_cursor = has_more
                .then(|| records.last().map(|r| query.cursor_for(r)))
                .flatten();
            Ok(ProductSkillImportPage {
                records,
                next_cursor,
            })
        })
    }
    fn marketplace_sources(
        &self,
        tenant: TenantId,
        versions: Vec<SkillVersionId>,
    ) -> ApplicationFuture<
        '_,
        Result<std::collections::BTreeMap<SkillVersionId, ProductSkillMarketplaceSource>, Error>,
    > {
        Box::pin(async move {
            if versions.len() > 200 {
                return Err(Error::InvalidRequest);
            }
            let mut connection = self.pool.acquire().await.map_err(unavailable)?;
            marketplace_sources(&mut connection, tenant, &versions).await
        })
    }
    fn observe(
        &self,
        actor: Actor,
        command: CommandId,
    ) -> ApplicationFuture<'_, Result<Option<ProductSkillImportRecord>, Error>> {
        Box::pin(async move {
            validate_actor(actor)?;
            let mut connection = self.pool.acquire().await.map_err(unavailable)?;
            observe(&mut connection, actor, command, false).await
        })
    }
    fn prepare(
        &self,
        request: ImportProductSkill,
        material: ProductSkillImportMaterial,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<ProductSkillImportRecord, Error>> {
        Box::pin(self.prepare_import(request, material, now))
    }
    fn commit(
        &self,
        prepared: PreparedProductSkillImport,
        package: Option<VerifiedSkillPackage>,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<ProductSkillImportRecord, Error>> {
        Box::pin(self.commit_import(prepared, package, now))
    }
}

impl PostgresSkillLifecycleStore {
    async fn prepare_import(
        &self,
        request: ImportProductSkill,
        material: ProductSkillImportMaterial,
        now: UtcTimestamp,
    ) -> Result<ProductSkillImportRecord, Error> {
        request.validate()?;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Skill)
            .await
            .map_err(unavailable)?;
        if let Some(existing) = observe(&mut tx, request.actor, request.command_id, false).await? {
            return same_request(existing, &request, &material);
        }
        if !content_ref_is_ready(
            &mut tx,
            request.actor.tenant_id.into_uuid(),
            &request.archive,
        )
        .await
        .map_err(store_error)?
        {
            return Err(Error::Corrupt);
        }
        let tenant = request.actor.tenant_id;
        let identity = sql::load_identity_inner(
            &mut tx,
            tenant,
            IdentityPredicate::Key(&material.identity_key),
        )
        .await
        .map_err(store_error)?;
        let (identity, parent) = match identity {
            Some(identity) => {
                validate_source(&request, &identity)?;
                let parent = sql::load_latest_package_inner(&mut tx, tenant, identity.skill_id)
                    .await
                    .map_err(store_error)?
                    .ok_or(Error::Corrupt)?;
                (identity, Some(parent))
            }
            None => (
                SkillIdentity {
                    skill_id: SkillId::generate(),
                    skill_key: material.identity_key.clone(),
                    provenance: provenance(&request),
                    trust: request.origin.trust(),
                    created_at: now,
                },
                None,
            ),
        };
        let active = sql::load_active_inner(&mut tx, tenant, identity.skill_id)
            .await
            .map_err(store_error)?;
        // A committed package without an active pointer is an incomplete legacy
        // import, not a new parent we may silently invent an activation for.
        if parent.is_some() && active.is_none() {
            return Err(Error::IdentityConflict);
        }
        let mut compatible_surfaces = BTreeSet::from([
            Surface::Web,
            Surface::Studio,
            Surface::Api,
            Surface::Desktop,
            Surface::Channel,
            Surface::Mobile,
        ]);
        if let Some(allowed) = &material.declared_surfaces {
            compatible_surfaces.retain(|surface| allowed.contains(surface));
            if !compatible_surfaces.contains(&request.surface) {
                return Err(Error::InvalidRequest);
            }
        } else if let Some(parent) = &parent {
            compatible_surfaces.extend(parent.version.compatible_surfaces.iter().copied());
        }
        let provenance_matches = match (&request.origin, &parent) {
            (ProductSkillImportOrigin::UploadedArchive, Some(_)) => true,
            (ProductSkillImportOrigin::Marketplace(source), Some(parent)) => {
                marketplace_sources(&mut tx, tenant, &[parent.version.skill_version_id])
                    .await?
                    .get(&parent.version.skill_version_id)
                    == Some(source)
            }
            (_, None) => false,
        };
        let reused = parent.as_ref().filter(|parent| {
            provenance_matches
                && parent.version.payload_hash == material.payload_hash
                && parent.version.compatible_surfaces == compatible_surfaces
        });
        let version = if let Some(parent) = reused {
            parent.version.version
        } else {
            VersionNumber::new(
                parent
                    .as_ref()
                    .map_or(Some(1), |p| p.version.version.get().checked_add(1))
                    .ok_or(Error::Corrupt)?,
            )
            .map_err(|_| Error::Corrupt)?
        };
        let prepared = PreparedProductSkillImport {
            request,
            identity,
            skill_version_id: reused
                .map_or_else(SkillVersionId::generate, |p| p.version.skill_version_id),
            version,
            parent_version_id: parent.as_ref().map(|p| p.version.skill_version_id),
            expected_pointer: active.map(|p| ActiveSkillVersionGuard {
                skill_version_id: p.skill_version_id,
                pointer_version: p.pointer_version,
            }),
            compatible_surfaces,
            payload_hash: material.payload_hash,
            created_version: reused.is_none(),
            accepted_at: now,
        };
        insert_prepared(&mut tx, &prepared).await?;
        let record = observe(
            &mut tx,
            prepared.request.actor,
            prepared.request.command_id,
            false,
        )
        .await?
        .ok_or(Error::Corrupt)?;
        let record = same_request(record, &prepared.request, &material)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(record)
    }

    async fn commit_import(
        &self,
        prepared: PreparedProductSkillImport,
        package: Option<VerifiedSkillPackage>,
        now: UtcTimestamp,
    ) -> Result<ProductSkillImportRecord, Error> {
        prepared.request.validate()?;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Skill)
            .await
            .map_err(unavailable)?;
        let record = observe(
            &mut tx,
            prepared.request.actor,
            prepared.request.command_id,
            true,
        )
        .await?
        .ok_or(Error::NotFound)?;
        if record.prepared != prepared {
            return Err(Error::CommandConflict);
        }
        if record.status != Status::Accepted {
            return Ok(record);
        }
        if now < prepared.accepted_at {
            return Err(Error::InvalidRequest);
        }
        // Savepoint keeps conflict receipts durable without any partial identity,
        // audit, immutable version or pointer writes from the attempted command.
        sqlx::query("SAVEPOINT product_import_apply")
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        let pointer = apply(&mut tx, &prepared, package, now).await?;
        if pointer.is_none() {
            sqlx::query("ROLLBACK TO SAVEPOINT product_import_apply")
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
        }
        finish(&mut tx, &prepared, pointer, now).await?;
        let result = observe(
            &mut tx,
            prepared.request.actor,
            prepared.request.command_id,
            false,
        )
        .await?
        .ok_or(Error::Corrupt)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }
}

fn provenance(request: &ImportProductSkill) -> SkillProvenance {
    request.origin.provenance(request.actor)
}

fn validate_source(request: &ImportProductSkill, identity: &SkillIdentity) -> Result<(), Error> {
    if identity.provenance != provenance(request) || identity.trust != request.origin.trust() {
        return Err(Error::IdentityConflict);
    }
    Ok(())
}
fn same_request(
    record: ProductSkillImportRecord,
    request: &ImportProductSkill,
    material: &ProductSkillImportMaterial,
) -> Result<ProductSkillImportRecord, Error> {
    if record.prepared.request != *request
        || record.prepared.identity.skill_key != material.identity_key
        || record.prepared.payload_hash != material.payload_hash
    {
        return Err(Error::CommandConflict);
    }
    Ok(record)
}

async fn apply(
    connection: &mut sqlx::PgConnection,
    prepared: &PreparedProductSkillImport,
    package: Option<VerifiedSkillPackage>,
    now: UtcTimestamp,
) -> Result<Option<u64>, Error> {
    let tenant = prepared.request.actor.tenant_id;
    if !super::account_installation::admit(connection, prepared).await? {
        return Ok(None);
    }
    let identity = &prepared.identity;
    validate_source(&prepared.request, identity)?;
    match sql::insert_identity_inner(&mut *connection, tenant, identity.clone())
        .await
        .map_err(store_error)?
    {
        SkillIdentityInsertOutcome::Existing(existing) if existing != *identity => return Ok(None),
        _ => {}
    }
    // Original normal version writes and pointer CAS use this same row lock.
    sqlx::query("SELECT skill_id FROM skill.skills WHERE tenant_id=$1 AND skill_id=$2 FOR UPDATE")
        .bind(tenant.into_uuid())
        .bind(identity.skill_id.into_uuid())
        .fetch_one(&mut *connection)
        .await
        .map_err(unavailable)?;
    let latest = sql::load_latest_package_inner(&mut *connection, tenant, identity.skill_id)
        .await
        .map_err(store_error)?;
    if latest.as_ref().map(|p| p.version.skill_version_id) != prepared.parent_version_id {
        return Ok(None);
    }
    let current = sql::load_active_inner(&mut *connection, tenant, identity.skill_id)
        .await
        .map_err(store_error)?;
    if current.map(|p| ActiveSkillVersionGuard {
        skill_version_id: p.skill_version_id,
        pointer_version: p.pointer_version,
    }) != prepared.expected_pointer
    {
        return Ok(None);
    }
    let package_record = if prepared.created_version {
        let package = package.ok_or(Error::Corrupt)?;
        package.validate().map_err(|_| Error::Corrupt)?;
        let v = &package.version;
        if package.identity != *identity
            || v.skill_version_id != prepared.skill_version_id
            || v.version != prepared.version
            || v.supersedes != prepared.parent_version_id
            || v.payload_hash != prepared.payload_hash
            || v.compatible_surfaces != prepared.compatible_surfaces
            || v.created_at != prepared.accepted_at
        {
            return Err(Error::Corrupt);
        }
        match sql::insert_version_inner(&mut *connection, tenant, package)
            .await
            .map_err(store_error)?
        {
            SkillVersionInsertOutcome::Inserted => {}
            SkillVersionInsertOutcome::Existing(_) => return Ok(None),
        }
        sql::load_version_inner(&mut *connection, tenant, prepared.skill_version_id)
            .await
            .map_err(store_error)?
            .ok_or(Error::Corrupt)?
    } else {
        if package.is_some() {
            return Err(Error::Corrupt);
        }
        let latest = latest.ok_or(Error::Corrupt)?;
        if latest.version.skill_version_id != prepared.skill_version_id
            || latest.version.version != prepared.version
            || latest.version.payload_hash != prepared.payload_hash
            || latest.version.compatible_surfaces != prepared.compatible_surfaces
        {
            return Err(Error::Corrupt);
        }
        latest
    };
    super::account_installation::activate(connection, prepared, now).await?;
    let install = prepared.request.disposition == ProductSkillImportDisposition::Install;
    if prepared.request.disposition == ProductSkillImportDisposition::Materialize
        && let Some(pointer) = current
    {
        // Owned Agents pin immutable versions independently of Work's
        // active library. Material delivery cannot enable, disable or upgrade
        // that separate product's pointer.
        return Ok(Some(pointer.pointer_version));
    }
    let installed = install;
    let enabled = install;
    if let Some(current) = current
        && current.skill_version_id == prepared.skill_version_id
        && current.saved
        && (!install || (current.installed && current.enabled))
    {
        return Ok(Some(current.pointer_version));
    }
    let replacement = ActiveSkillVersionPointer {
        skill_id: identity.skill_id,
        skill_version_id: prepared.skill_version_id,
        pointer_version: current
            .map_or(Some(1), |p| p.pointer_version.checked_add(1))
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or(Error::Corrupt)?,
        saved: true,
        installed,
        enabled,
        activated_at: now,
        activated_by: prepared.request.actor.principal_id,
    };
    replacement
        .validate(&package_record.identity, &package_record.version)
        .map_err(|_| Error::Corrupt)?;
    match sql::cas_inner(
        &mut *connection,
        tenant,
        identity.skill_id,
        current,
        replacement,
    )
    .await
    .map_err(store_error)?
    {
        ActivePointerCasOutcome::Swapped(pointer) => Ok(Some(pointer.pointer_version)),
        ActivePointerCasOutcome::Conflict(_) => Ok(None),
    }
}

async fn insert_prepared(
    connection: &mut sqlx::PgConnection,
    p: &PreparedProductSkillImport,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO skill.product_import_commands (
        tenant_id,principal_id,command_id,market_skill_id,market_version_id,surface,request_bytes,request_digest,archive_content_id,
        skill_id,skill_key,identity_created_at_ms,skill_version_id,version,parent_skill_version_id,expected_skill_version_id,expected_pointer_version,
        compatible_surfaces,payload_hash,created_version,accepted_at_ms,source_kind)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22) ON CONFLICT DO NOTHING")
        .bind(p.request.actor.tenant_id.into_uuid()).bind(p.request.actor.principal_id.into_uuid()).bind(p.request.command_id.into_uuid())
        .bind(p.request.origin.marketplace().map(|s| s.market_skill_id)).bind(p.request.origin.marketplace().map(|s| s.market_version_id)).bind(p.request.surface.as_str())
        .bind(codec::encode_request(&p.request).encode_to_vec()).bind(p.request.digest().into_bytes().to_vec()).bind(p.request.archive.content_id.into_uuid())
        .bind(p.identity.skill_id.into_uuid()).bind(p.identity.skill_key.as_str()).bind(p.identity.created_at.unix_millis())
        .bind(p.skill_version_id.into_uuid()).bind(i64::try_from(p.version.get()).map_err(|_|Error::Corrupt)?)
        .bind(p.parent_version_id.map(|v|v.into_uuid())).bind(p.expected_pointer.map(|v|v.skill_version_id.into_uuid()))
        .bind(p.expected_pointer.map(|v|i64::try_from(v.pointer_version)).transpose().map_err(|_|Error::Corrupt)?)
        .bind(p.compatible_surfaces.iter().copied().map(surface).collect::<Vec<_>>()).bind(p.payload_hash.into_bytes().to_vec())
        .bind(p.created_version).bind(p.accepted_at.unix_millis()).bind(p.request.origin.source_kind()).execute(connection).await.map_err(unavailable)?;
    Ok(())
}
async fn finish(
    connection: &mut sqlx::PgConnection,
    p: &PreparedProductSkillImport,
    pointer: Option<u64>,
    now: UtcTimestamp,
) -> Result<(), Error> {
    let changed=sqlx::query("UPDATE skill.product_import_commands SET status=$4,completed_at_ms=$5,receipt_pointer_version=$6,archive_content_id=NULL,retained_version_id=$7 WHERE tenant_id=$1 AND principal_id=$2 AND command_id=$3 AND status=1")
        .bind(p.request.actor.tenant_id.into_uuid()).bind(p.request.actor.principal_id.into_uuid()).bind(p.request.command_id.into_uuid())
        .bind(if pointer.is_some(){2i16}else{3i16}).bind(now.unix_millis())
        .bind(pointer.map(i64::try_from).transpose().map_err(|_|Error::Corrupt)?)
        .bind(pointer.map(|_|p.skill_version_id.into_uuid())).execute(connection).await.map_err(unavailable)?;
    if changed.rows_affected() != 1 {
        return Err(Error::Corrupt);
    }
    Ok(())
}
async fn observe(
    connection: &mut sqlx::PgConnection,
    actor: Actor,
    command: CommandId,
    lock: bool,
) -> Result<Option<ProductSkillImportRecord>, Error> {
    let sql = if lock {
        "SELECT * FROM skill.product_import_commands WHERE tenant_id=$1 AND principal_id=$2 AND command_id=$3 FOR UPDATE"
    } else {
        "SELECT * FROM skill.product_import_commands WHERE tenant_id=$1 AND principal_id=$2 AND command_id=$3"
    };
    let row = sqlx::query(sql)
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(command.into_uuid())
        .fetch_optional(connection)
        .await
        .map_err(unavailable)?;
    row.map(|row| decode(&row, actor, command)).transpose()
}
fn decode(
    row: &sqlx::postgres::PgRow,
    actor: Actor,
    command: CommandId,
) -> Result<ProductSkillImportRecord, Error> {
    let bytes: Vec<u8> = row.try_get("request_bytes").map_err(unavailable)?;
    let request = codec::request(
        wire::ImportProductSkillRequest::decode(bytes.as_slice()).map_err(|_| Error::Corrupt)?,
    )
    .map_err(|_| Error::Corrupt)?;
    let digest: Vec<u8> = row.try_get("request_digest").map_err(unavailable)?;
    if request.actor != actor
        || request.command_id != command
        || request.digest().into_bytes().as_slice() != digest.as_slice()
        || codec::encode_request(&request).encode_to_vec() != bytes
        || row.try_get::<i16, _>("source_kind").map_err(unavailable)?
            != request.origin.source_kind()
        || row
            .try_get::<Option<uuid::Uuid>, _>("market_skill_id")
            .map_err(unavailable)?
            != request.origin.marketplace().map(|s| s.market_skill_id)
        || row
            .try_get::<Option<uuid::Uuid>, _>("market_version_id")
            .map_err(unavailable)?
            != request.origin.marketplace().map(|s| s.market_version_id)
    {
        return Err(Error::Corrupt);
    }
    let expected_id: Option<uuid::Uuid> = row
        .try_get("expected_skill_version_id")
        .map_err(unavailable)?;
    let expected_version: Option<i64> = row
        .try_get("expected_pointer_version")
        .map_err(unavailable)?;
    let expected_pointer = match (expected_id, expected_version) {
        (Some(id), Some(v)) if v > 0 => Some(ActiveSkillVersionGuard {
            skill_version_id: super::id(id).map_err(store_error)?,
            pointer_version: v as u64,
        }),
        (None, None) => None,
        _ => return Err(Error::Corrupt),
    };
    let identity = SkillIdentity {
        skill_id: id(row.try_get("skill_id").map_err(unavailable)?).map_err(store_error)?,
        skill_key: SkillKey::new(row.try_get::<String, _>("skill_key").map_err(unavailable)?)
            .map_err(|_| Error::Corrupt)?,
        provenance: provenance(&request),
        trust: request.origin.trust(),
        created_at: UtcTimestamp::from_unix_millis(
            row.try_get("identity_created_at_ms").map_err(unavailable)?,
        ),
    };
    let surfaces: Vec<i16> = row.try_get("compatible_surfaces").map_err(unavailable)?;
    let compatible_surfaces = decode_surfaces(surfaces).map_err(store_error)?;
    let version: i64 = row.try_get("version").map_err(unavailable)?;
    let prepared = PreparedProductSkillImport {
        request,
        identity,
        skill_version_id: id(row.try_get("skill_version_id").map_err(unavailable)?)
            .map_err(store_error)?,
        version: VersionNumber::new(u64::try_from(version).map_err(|_| Error::Corrupt)?)
            .map_err(|_| Error::Corrupt)?,
        parent_version_id: row
            .try_get::<Option<uuid::Uuid>, _>("parent_skill_version_id")
            .map_err(unavailable)?
            .map(id)
            .transpose()
            .map_err(store_error)?,
        expected_pointer,
        compatible_surfaces,
        payload_hash: Sha256::from_bytes(
            row.try_get::<Vec<u8>, _>("payload_hash")
                .map_err(unavailable)?,
        )
        .map_err(|_| Error::Corrupt)?,
        created_version: row.try_get("created_version").map_err(unavailable)?,
        accepted_at: UtcTimestamp::from_unix_millis(
            row.try_get("accepted_at_ms").map_err(unavailable)?,
        ),
    };
    let status = match row.try_get::<i16, _>("status").map_err(unavailable)? {
        1 => Status::Accepted,
        2 => Status::Completed,
        3 => Status::Conflict,
        _ => return Err(Error::Corrupt),
    };
    let record = ProductSkillImportRecord {
        prepared,
        status,
        pointer_version: row
            .try_get::<Option<i64>, _>("receipt_pointer_version")
            .map_err(unavailable)?
            .map(u64::try_from)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
        completed_at: row
            .try_get::<Option<i64>, _>("completed_at_ms")
            .map_err(unavailable)?
            .map(UtcTimestamp::from_unix_millis),
    };
    codec::response(record.clone()).map_err(|_| Error::Corrupt)?;
    Ok(record)
}
fn unavailable<T>(_: T) -> Error {
    Error::Unavailable
}
fn store_error(error: SkillLifecycleAdapterError) -> Error {
    match error {
        SkillLifecycleAdapterError::Corrupt => Error::Corrupt,
        _ => Error::Unavailable,
    }
}

async fn marketplace_sources(
    connection: &mut sqlx::PgConnection,
    tenant: TenantId,
    versions: &[SkillVersionId],
) -> Result<std::collections::BTreeMap<SkillVersionId, ProductSkillMarketplaceSource>, Error> {
    // DISTINCT ON bounds repeated command receipts to one row per immutable
    // version. Runtime admission only reuses a version with this same origin.
    let rows=sqlx::query("SELECT DISTINCT ON (skill_version_id) * FROM skill.product_import_commands WHERE tenant_id=$1 AND source_kind=1 AND status=2 AND skill_version_id=ANY($2) ORDER BY skill_version_id,accepted_at_ms,command_id")
        .bind(tenant.into_uuid()).bind(versions.iter().map(|v|v.into_uuid()).collect::<Vec<_>>()).fetch_all(connection).await.map_err(unavailable)?;
    let mut result = std::collections::BTreeMap::new();
    for row in rows {
        let actor = Actor {
            tenant_id: tenant,
            principal_id: id(row.try_get("principal_id").map_err(unavailable)?)
                .map_err(store_error)?,
            principal_kind: PrincipalKind::User,
        };
        let record = decode(
            &row,
            actor,
            id(row.try_get("command_id").map_err(unavailable)?).map_err(store_error)?,
        )?;
        result.insert(
            record.prepared.skill_version_id,
            record
                .prepared
                .request
                .origin
                .marketplace()
                .ok_or(Error::Corrupt)?
                .clone(),
        );
    }
    Ok(result)
}
