pub(crate) mod account_installation;
mod import_commands;
mod persistence;
mod retained_catalog;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    Actor, PrincipalKind, SkillId, SkillVersionId, TenantId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_skill::{
    ActivePointerCasOutcome, ActiveSkillCatalogSource, SkillIdentityInsertOutcome,
    SkillLifecycleStore, SkillLifecycleStoreError, SkillVersionInsertOutcome,
};
use musteragent_runtime_skill::{
    SkillInstallationRecord, SkillProductStore, SkillProductStoreError,
};
use musteragent_skill::{
    ActiveSkillVersionPointer, InstalledSkillSource, PackageAuditReport, QuarantineDisposition,
    SkillCatalogEntry, SkillCompatibilityFinding, SkillCompatibilityFindingCode, SkillIdentity,
    SkillKey, SkillPackageRecord, SkillProvenance, SkillResourceDescriptor, SkillResourcePath,
    SkillResourceRole, SkillSourceFormat, SkillTrust, SkillVersion, Surface, VerifiedSkillPackage,
};
use sqlx::{PgPool, Row};

#[derive(Clone)]
pub struct PostgresSkillLifecycleStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
}

impl PostgresSkillLifecycleStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self { pool, database }
    }

    async fn load_identity_inner(
        &self,
        tenant_id: TenantId,
        predicate: IdentityPredicate<'_>,
    ) -> Result<Option<SkillIdentity>, SkillLifecycleAdapterError> {
        let mut connection = self.pool.acquire().await?;
        persistence::load_identity_inner(&mut connection, tenant_id, predicate).await
    }

    async fn load_version_inner(
        &self,
        tenant_id: TenantId,
        skill_version_id: SkillVersionId,
    ) -> Result<Option<SkillPackageRecord>, SkillLifecycleAdapterError> {
        let mut connection = self.pool.acquire().await?;
        persistence::load_version_inner(&mut connection, tenant_id, skill_version_id).await
    }

    async fn insert_identity_inner(
        &self,
        tenant_id: TenantId,
        identity: SkillIdentity,
    ) -> Result<SkillIdentityInsertOutcome, SkillLifecycleAdapterError> {
        let mut connection = self.pool.acquire().await?;
        persistence::insert_identity_inner(&mut connection, tenant_id, identity).await
    }

    async fn insert_version_inner(
        &self,
        tenant_id: TenantId,
        package: VerifiedSkillPackage,
    ) -> Result<SkillVersionInsertOutcome, SkillLifecycleAdapterError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Skill)
            .await?;
        let result =
            persistence::insert_version_inner(&mut transaction, tenant_id, package).await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn load_active_inner(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> Result<Option<ActiveSkillVersionPointer>, SkillLifecycleAdapterError> {
        let mut connection = self.pool.acquire().await?;
        persistence::load_active_inner(&mut connection, tenant_id, skill_id).await
    }

    async fn cas_inner(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
        expected: Option<ActiveSkillVersionPointer>,
        replacement: ActiveSkillVersionPointer,
    ) -> Result<ActivePointerCasOutcome, SkillLifecycleAdapterError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Skill)
            .await?;
        if (replacement.installed || replacement.enabled)
            && !account_installation::admit_activation(
                &mut transaction,
                Actor {
                    tenant_id,
                    principal_id: replacement.activated_by,
                    principal_kind: PrincipalKind::User,
                },
                musteragent_runtime_skill::SkillAccountProduct::KERNEL,
                skill_id,
                replacement.skill_version_id,
            )
            .await
            .map_err(|_| SkillLifecycleAdapterError::Corrupt)?
        {
            return Ok(ActivePointerCasOutcome::Conflict(
                persistence::load_active_inner(&mut transaction, tenant_id, skill_id).await?,
            ));
        }
        let result =
            persistence::cas_inner(&mut transaction, tenant_id, skill_id, expected, replacement)
                .await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn active_entries_inner(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<SkillCatalogEntry>, SkillLifecycleAdapterError> {
        let rows = sqlx::query(ACTIVE_VERSION_SELECT)
            .bind(tenant_id.into_uuid())
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                decode_package(&row).and_then(|package| {
                    package
                        .version
                        .as_active_catalog_entry(&package.identity, tenant_id)
                        .map_err(|_| SkillLifecycleAdapterError::Corrupt)
                })
            })
            .collect()
    }

    async fn list_installations_inner(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<SkillInstallationRecord>, SkillLifecycleAdapterError> {
        let rows = sqlx::query(
            "SELECT skill_id, skill_version_id, pointer_version::text AS pointer_version,
                    saved, installed, enabled, activated_at_ms, activated_by
             FROM skill.active_version_pointers
             WHERE tenant_id = $1 ORDER BY skill_id",
        )
        .bind(tenant_id.into_uuid())
        .fetch_all(&self.pool)
        .await?;
        let mut installations = Vec::with_capacity(rows.len());
        for row in rows {
            let pointer = decode_pointer(&row)?;
            let package = self
                .load_version_inner(tenant_id, pointer.skill_version_id)
                .await?
                .ok_or(SkillLifecycleAdapterError::Corrupt)?;
            pointer
                .validate(&package.identity, &package.version)
                .map_err(|_| SkillLifecycleAdapterError::Corrupt)?;
            installations.push(SkillInstallationRecord { package, pointer });
        }
        Ok(installations)
    }

    async fn load_latest_package_inner(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> Result<Option<SkillPackageRecord>, SkillLifecycleAdapterError> {
        let mut connection = self.pool.acquire().await?;
        persistence::load_latest_package_inner(&mut connection, tenant_id, skill_id).await
    }
}

impl SkillLifecycleStore for PostgresSkillLifecycleStore {
    fn load_identity(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> ApplicationFuture<'_, Result<Option<SkillIdentity>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            self.load_identity_inner(tenant_id, IdentityPredicate::Id(skill_id))
                .await
                .map_err(SkillLifecycleAdapterError::port)
        })
    }

    fn load_identity_by_key<'a>(
        &'a self,
        tenant_id: TenantId,
        skill_key: &'a SkillKey,
    ) -> ApplicationFuture<'a, Result<Option<SkillIdentity>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            self.load_identity_inner(tenant_id, IdentityPredicate::Key(skill_key))
                .await
                .map_err(SkillLifecycleAdapterError::port)
        })
    }

    fn insert_identity(
        &self,
        tenant_id: TenantId,
        identity: SkillIdentity,
    ) -> ApplicationFuture<'_, Result<SkillIdentityInsertOutcome, SkillLifecycleStoreError>> {
        Box::pin(async move {
            self.insert_identity_inner(tenant_id, identity)
                .await
                .map_err(SkillLifecycleAdapterError::port)
        })
    }

    fn load_version(
        &self,
        tenant_id: TenantId,
        skill_version_id: SkillVersionId,
    ) -> ApplicationFuture<'_, Result<Option<SkillPackageRecord>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            self.load_version_inner(tenant_id, skill_version_id)
                .await
                .map_err(SkillLifecycleAdapterError::port)
        })
    }

    fn insert_version(
        &self,
        tenant_id: TenantId,
        package: VerifiedSkillPackage,
    ) -> ApplicationFuture<'_, Result<SkillVersionInsertOutcome, SkillLifecycleStoreError>> {
        Box::pin(async move {
            self.insert_version_inner(tenant_id, package)
                .await
                .map_err(SkillLifecycleAdapterError::port)
        })
    }

    fn load_active(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> ApplicationFuture<'_, Result<Option<ActiveSkillVersionPointer>, SkillLifecycleStoreError>>
    {
        Box::pin(async move {
            self.load_active_inner(tenant_id, skill_id)
                .await
                .map_err(SkillLifecycleAdapterError::port)
        })
    }

    fn compare_and_swap_active(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
        expected: Option<ActiveSkillVersionPointer>,
        replacement: ActiveSkillVersionPointer,
    ) -> ApplicationFuture<'_, Result<ActivePointerCasOutcome, SkillLifecycleStoreError>> {
        Box::pin(async move {
            self.cas_inner(tenant_id, skill_id, expected, replacement)
                .await
                .map_err(SkillLifecycleAdapterError::port)
        })
    }
}

impl ActiveSkillCatalogSource for PostgresSkillLifecycleStore {
    fn load_retained_entries<'a>(
        &'a self,
        _actor: Actor,
        product: musteragent_identity::ProductId,
        bases: &'a [VersionedKey],
    ) -> ApplicationFuture<
        'a,
        Result<musteragent_runtime_skill::VerifiedRetainedSkillCatalog, SkillLifecycleStoreError>,
    > {
        Box::pin(async move { self.retained_entries(product, bases).await })
    }

    fn load_account_entries(
        &self,
        actor: Actor,
        product: musteragent_identity::ProductId,
    ) -> ApplicationFuture<'_, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            if actor.principal_kind != PrincipalKind::User {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            let product = product.as_str();
            let ids: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT skill_version_id FROM skill.product_account_installations WHERE tenant_id=$1 AND principal_id=$2 AND product=$3 AND installed=true ORDER BY market_skill_id LIMIT 201")
                .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(product)
                .fetch_all(&self.pool).await.map_err(|_| SkillLifecycleStoreError::Unavailable)?;
            if ids.len() > 200 {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            let rows = sqlx::query(VERSIONS_SELECT)
                .bind(actor.tenant_id.into_uuid())
                .bind(&ids)
                .fetch_all(&self.pool)
                .await
                .map_err(|_| SkillLifecycleStoreError::Unavailable)?;
            if rows.len() != ids.len() {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            rows.into_iter()
                .map(|row| {
                    let package = decode_package(&row).map_err(SkillLifecycleAdapterError::port)?;
                    package
                        .version
                        .as_active_catalog_entry(&package.identity, actor.tenant_id)
                        .map_err(|_| SkillLifecycleStoreError::Corrupt)
                })
                .collect()
        })
    }

    fn load_active_entries(
        &self,
        tenant_id: TenantId,
    ) -> ApplicationFuture<'_, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            self.active_entries_inner(tenant_id)
                .await
                .map_err(SkillLifecycleAdapterError::port)
        })
    }
    fn load_pinned_entries<'a>(
        &'a self,
        actor: Actor,
        pins: &'a [musteragent_runtime_skill::PinnedSkill],
    ) -> ApplicationFuture<'a, Result<Vec<SkillCatalogEntry>, SkillLifecycleStoreError>> {
        Box::pin(async move {
            if actor.principal_kind != PrincipalKind::User || pins.len() > 64 * 128 {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            let ids = pins
                .iter()
                .map(|pin| pin.skill_version_id.into_uuid())
                .collect::<Vec<_>>();
            let rows = sqlx::query(VERSIONS_SELECT)
                .bind(actor.tenant_id.into_uuid())
                .bind(ids)
                .fetch_all(&self.pool)
                .await
                .map_err(|_| SkillLifecycleStoreError::Unavailable)?;
            if rows.len() != pins.len() {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            let by_id = pins
                .iter()
                .map(|pin| (pin.skill_version_id, pin))
                .collect::<std::collections::BTreeMap<_, _>>();
            if by_id.len() != pins.len() {
                return Err(SkillLifecycleStoreError::Corrupt);
            }
            let mut entries = Vec::with_capacity(rows.len());
            for row in rows {
                let package = decode_package(&row).map_err(SkillLifecycleAdapterError::port)?;
                let pin = by_id
                    .get(&package.version.skill_version_id)
                    .ok_or(SkillLifecycleStoreError::Corrupt)?;
                if package.identity.skill_id != pin.skill_id
                    || package.version.skill_ref != pin.skill_ref
                    || package.version.manifest_hash != pin.manifest_hash
                    || package.version.payload_hash != pin.payload_hash
                    || matches!(package.identity.provenance, SkillProvenance::Bundled { .. })
                    || matches!(package.identity.provenance, SkillProvenance::Custom { created_by } if created_by != actor.principal_id)
                {
                    return Err(SkillLifecycleStoreError::Corrupt);
                }
                entries.push(
                    package
                        .version
                        .as_active_catalog_entry(&package.identity, actor.tenant_id)
                        .map_err(|_| SkillLifecycleStoreError::Corrupt)?,
                );
            }
            entries.sort_by(|a, b| a.definition.skill_ref.cmp(&b.definition.skill_ref));
            Ok(entries)
        })
    }
}

impl SkillProductStore for PostgresSkillLifecycleStore {
    fn list_installations(
        &self,
        tenant_id: TenantId,
    ) -> ApplicationFuture<'_, Result<Vec<SkillInstallationRecord>, SkillProductStoreError>> {
        Box::pin(async move {
            self.list_installations_inner(tenant_id)
                .await
                .map_err(SkillLifecycleAdapterError::product_port)
        })
    }

    fn load_latest_package(
        &self,
        tenant_id: TenantId,
        skill_id: SkillId,
    ) -> ApplicationFuture<'_, Result<Option<SkillPackageRecord>, SkillProductStoreError>> {
        Box::pin(async move {
            self.load_latest_package_inner(tenant_id, skill_id)
                .await
                .map_err(SkillLifecycleAdapterError::product_port)
        })
    }
}

const VERSIONS_SELECT: &str = "
SELECT definition.skill_id, definition.skill_key, definition.provenance_kind,
       definition.source_kind, definition.source_ref, definition.publisher_ref,
       definition.created_by, definition.trust_level, definition.created_at_ms AS identity_created_at_ms,
       version.skill_version_id, version.version::text AS version_number,
       version.manifest_hash, version.skill_ref, version.supersedes,
       version.title, version.description, version.when_to_use,
       version.declared_version, version.source_format,
       version.instructions_content_id, version.instructions_media_type,
       version.instructions_byte_length::text AS instructions_byte_length,
       version.instructions_hash, version.manifest_content_id, version.manifest_media_type,
       version.manifest_byte_length::text AS manifest_byte_length,
       version.package_content_id, version.package_media_type,
       version.package_byte_length::text AS package_byte_length,
       version.package_hash, version.package_dependencies,
       version.resources, version.compatibility_findings,
       version.required_tools, version.optional_tools, version.compatible_agent_contracts,
       version.compatible_surfaces, version.file_set_hash AS version_file_set_hash,
       version.audit_digest, version.created_at_ms,
       audit.manifest_hash AS audit_manifest_hash, audit.file_set_hash,
       audit.audit_digest AS audit_audit_digest,
       audit.signature_verified, audit.manifest_verified,
       audit.file_hashes_verified, audit.static_audit_passed, audit.scripts_executed,
       audit.quarantine_disposition
FROM skill.skill_versions AS version
JOIN skill.skills AS definition USING (skill_id)
JOIN skill.package_audits AS audit
  ON audit.skill_version_id = version.skill_version_id
 AND audit.tenant_id = definition.tenant_id
JOIN artifact.content_refs AS instructions
  ON instructions.tenant_id = definition.tenant_id
 AND instructions.content_id = version.instructions_content_id
 AND instructions.media_type = version.instructions_media_type
 AND instructions.byte_length = version.instructions_byte_length
 AND instructions.content_hash = version.instructions_hash
 AND instructions.state = 2
JOIN artifact.content_refs AS manifest
  ON manifest.tenant_id = definition.tenant_id
 AND manifest.content_id = version.manifest_content_id
 AND manifest.media_type = version.manifest_media_type
 AND manifest.byte_length = version.manifest_byte_length
 AND manifest.content_hash = version.manifest_hash
 AND manifest.state = 2
JOIN artifact.content_refs AS package
  ON package.tenant_id = definition.tenant_id
 AND package.content_id = version.package_content_id
 AND package.media_type = version.package_media_type
 AND package.byte_length = version.package_byte_length
 AND package.content_hash = version.package_hash
 AND package.state = 2
WHERE definition.tenant_id = $1
  AND version.skill_version_id = ANY($2)";

const ACTIVE_VERSION_SELECT: &str = "
SELECT definition.skill_id, definition.skill_key, definition.provenance_kind,
       definition.source_kind, definition.source_ref, definition.publisher_ref,
       definition.created_by, definition.trust_level, definition.created_at_ms AS identity_created_at_ms,
       version.skill_version_id, version.version::text AS version_number,
       version.manifest_hash, version.skill_ref, version.supersedes,
       version.title, version.description, version.when_to_use,
       version.declared_version, version.source_format,
       version.instructions_content_id, version.instructions_media_type,
       version.instructions_byte_length::text AS instructions_byte_length,
       version.instructions_hash, version.manifest_content_id, version.manifest_media_type,
       version.manifest_byte_length::text AS manifest_byte_length,
       version.package_content_id, version.package_media_type,
       version.package_byte_length::text AS package_byte_length,
       version.package_hash, version.package_dependencies,
       version.resources, version.compatibility_findings,
       version.required_tools, version.optional_tools, version.compatible_agent_contracts,
       version.compatible_surfaces, version.file_set_hash AS version_file_set_hash,
       version.audit_digest, version.created_at_ms,
       audit.manifest_hash AS audit_manifest_hash, audit.file_set_hash,
       audit.audit_digest AS audit_audit_digest,
       audit.signature_verified, audit.manifest_verified,
       audit.file_hashes_verified, audit.static_audit_passed, audit.scripts_executed,
       audit.quarantine_disposition
FROM skill.active_version_pointers AS pointer
JOIN skill.skills AS definition USING (skill_id)
JOIN skill.skill_versions AS version ON version.skill_version_id = pointer.skill_version_id
JOIN skill.package_audits AS audit
  ON audit.skill_version_id = version.skill_version_id
 AND audit.tenant_id = definition.tenant_id
JOIN artifact.content_refs AS instructions
  ON instructions.tenant_id = definition.tenant_id
 AND instructions.content_id = version.instructions_content_id
 AND instructions.media_type = version.instructions_media_type
 AND instructions.byte_length = version.instructions_byte_length
 AND instructions.content_hash = version.instructions_hash
 AND instructions.state = 2
JOIN artifact.content_refs AS manifest
  ON manifest.tenant_id = definition.tenant_id
 AND manifest.content_id = version.manifest_content_id
 AND manifest.media_type = version.manifest_media_type
 AND manifest.byte_length = version.manifest_byte_length
 AND manifest.content_hash = version.manifest_hash
 AND manifest.state = 2
JOIN artifact.content_refs AS package
  ON package.tenant_id = definition.tenant_id
 AND package.content_id = version.package_content_id
 AND package.media_type = version.package_media_type
 AND package.byte_length = version.package_byte_length
 AND package.content_hash = version.package_hash
 AND package.state = 2
WHERE pointer.tenant_id = $1
  AND pointer.installed
  AND pointer.enabled
  AND definition.tenant_id = $1
ORDER BY version.skill_ref";

async fn content_ref_is_ready(
    transaction: &mut sqlx::PgConnection,
    tenant_id: uuid::Uuid,
    reference: &ContentRef,
) -> Result<bool, SkillLifecycleAdapterError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE tenant_id = $1 AND content_id = $2
               AND media_type = $3 AND byte_length = $4::bigint
               AND content_hash = $5 AND state = 2
         )",
    )
    .bind(tenant_id)
    .bind(reference.content_id.into_uuid())
    .bind(reference.media_type.as_str())
    .bind(reference.byte_length.to_string())
    .bind(reference.sha256.into_bytes().as_slice())
    .fetch_one(&mut *transaction)
    .await?)
}

async fn insert_audit(
    transaction: &mut sqlx::PgConnection,
    tenant_id: uuid::Uuid,
    package: &VerifiedSkillPackage,
) -> Result<(), SkillLifecycleAdapterError> {
    let audit = &package.audit;
    let changed = sqlx::query(
        "INSERT INTO skill.package_audits (
             tenant_id, skill_version_id, manifest_hash, file_set_hash,
             signature_verified, manifest_verified, file_hashes_verified,
             static_audit_passed, scripts_executed, quarantine_disposition, audit_digest
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(tenant_id)
    .bind(package.version.skill_version_id.into_uuid())
    .bind(audit.manifest_hash.into_bytes().as_slice())
    .bind(audit.file_set_hash.into_bytes().as_slice())
    .bind(audit.signature_verified)
    .bind(audit.manifest_verified)
    .bind(audit.file_hashes_verified)
    .bind(audit.static_audit_passed)
    .bind(audit.scripts_executed)
    .bind(quarantine(audit.disposition))
    .bind(audit.audit_digest.into_bytes().as_slice())
    .execute(&mut *transaction)
    .await?;
    if changed.rows_affected() == 1 {
        Ok(())
    } else {
        Err(SkillLifecycleAdapterError::Corrupt)
    }
}

fn decode_package(
    row: &sqlx::postgres::PgRow,
) -> Result<SkillPackageRecord, SkillLifecycleAdapterError> {
    let identity = decode_identity_with_created_at(row, "identity_created_at_ms")?;
    let manifest_hash = hash(row, "manifest_hash")?;
    let audit_digest = hash(row, "audit_digest")?;
    let file_set_hash = hash(row, "file_set_hash")?;
    if hash(row, "audit_manifest_hash")? != manifest_hash
        || hash(row, "audit_audit_digest")? != audit_digest
        || hash(row, "version_file_set_hash")? != file_set_hash
    {
        return Err(SkillLifecycleAdapterError::Corrupt);
    }
    let version = SkillVersion {
        skill_version_id: id(row.try_get("skill_version_id")?)?,
        skill_id: identity.skill_id,
        version: VersionNumber::new(parse_u64(row.try_get("version_number")?)?)
            .map_err(|_| SkillLifecycleAdapterError::Corrupt)?,
        skill_ref: VersionedKey::new(row.try_get::<String, _>("skill_ref")?)
            .map_err(|_| SkillLifecycleAdapterError::Corrupt)?,
        title: row.try_get("title")?,
        description: row.try_get("description")?,
        when_to_use: row.try_get("when_to_use")?,
        declared_version: row.try_get("declared_version")?,
        source_format: decode_source_format(row.try_get("source_format")?)?,
        instructions_ref: content_ref(row, "instructions")?,
        instructions_hash: hash(row, "instructions_hash")?,
        package_ref: content_ref(row, "package")?,
        package_hash: hash(row, "package_hash")?,
        manifest_ref: content_ref(row, "manifest")?,
        manifest_hash,
        payload_hash: file_set_hash,
        package_dependencies: decode_keys(row.try_get("package_dependencies")?)?,
        required_tools: decode_keys(row.try_get("required_tools")?)?,
        optional_tools: decode_keys(row.try_get("optional_tools")?)?,
        compatible_agent_contracts: decode_keys(row.try_get("compatible_agent_contracts")?)?,
        compatible_surfaces: decode_surfaces(row.try_get("compatible_surfaces")?)?,
        audit_digest,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
        supersedes: optional_id(row.try_get("supersedes")?)?,
    };
    let package = SkillPackageRecord {
        identity,
        version,
        resources: decode_resources(row.try_get("resources")?)?,
        compatibility_findings: decode_compatibility_findings(
            row.try_get("compatibility_findings")?,
        )?,
        audit: PackageAuditReport {
            manifest_hash,
            file_set_hash,
            signature_verified: row.try_get("signature_verified")?,
            manifest_verified: row.try_get("manifest_verified")?,
            file_hashes_verified: row.try_get("file_hashes_verified")?,
            static_audit_passed: row.try_get("static_audit_passed")?,
            scripts_executed: row.try_get("scripts_executed")?,
            disposition: match row.try_get::<i16, _>("quarantine_disposition")? {
                1 => QuarantineDisposition::Cleared,
                2 => QuarantineDisposition::Rejected,
                _ => return Err(SkillLifecycleAdapterError::Corrupt),
            },
            audit_digest,
        },
    };
    package
        .validate()
        .map_err(|_| SkillLifecycleAdapterError::Corrupt)?;
    Ok(package)
}

fn decode_identity(
    row: &sqlx::postgres::PgRow,
) -> Result<SkillIdentity, SkillLifecycleAdapterError> {
    decode_identity_with_created_at(row, "created_at_ms")
}

fn decode_identity_with_created_at(
    row: &sqlx::postgres::PgRow,
    created_at_column: &str,
) -> Result<SkillIdentity, SkillLifecycleAdapterError> {
    let provenance_kind: i16 = row.try_get("provenance_kind")?;
    let provenance = match provenance_kind {
        1 => return Err(SkillLifecycleAdapterError::Corrupt),
        2 => SkillProvenance::Custom {
            created_by: id(required_uuid(row, "created_by")?)?,
        },
        3 => SkillProvenance::Installed {
            source: match row.try_get::<Option<i16>, _>("source_kind")? {
                Some(1) => InstalledSkillSource::Marketplace,
                Some(2) => InstalledSkillSource::Hub,
                Some(3) => InstalledSkillSource::GitHub,
                Some(4) => InstalledSkillSource::Url,
                _ => return Err(SkillLifecycleAdapterError::Corrupt),
            },
            source_ref: required_string(row, "source_ref")?,
            publisher_ref: required_string(row, "publisher_ref")?,
        },
        _ => return Err(SkillLifecycleAdapterError::Corrupt),
    };
    let identity = SkillIdentity {
        skill_id: id(row.try_get("skill_id")?)?,
        skill_key: SkillKey::new(row.try_get::<String, _>("skill_key")?)
            .map_err(|_| SkillLifecycleAdapterError::Corrupt)?,
        provenance,
        trust: match row.try_get::<i16, _>("trust_level")? {
            1 => SkillTrust::Official,
            2 => SkillTrust::Verified,
            3 => SkillTrust::Community,
            4 => SkillTrust::Local,
            _ => return Err(SkillLifecycleAdapterError::Corrupt),
        },
        created_at: UtcTimestamp::from_unix_millis(row.try_get(created_at_column)?),
    };
    identity
        .validate()
        .map_err(|_| SkillLifecycleAdapterError::Corrupt)?;
    Ok(identity)
}

fn decode_pointer(
    row: &sqlx::postgres::PgRow,
) -> Result<ActiveSkillVersionPointer, SkillLifecycleAdapterError> {
    Ok(ActiveSkillVersionPointer {
        skill_id: id(row.try_get("skill_id")?)?,
        skill_version_id: id(row.try_get("skill_version_id")?)?,
        pointer_version: parse_u64(row.try_get("pointer_version")?)?,
        saved: row.try_get("saved")?,
        installed: row.try_get("installed")?,
        enabled: row.try_get("enabled")?,
        activated_at: UtcTimestamp::from_unix_millis(row.try_get("activated_at_ms")?),
        activated_by: id(row.try_get("activated_by")?)?,
    })
}

struct EncodedProvenance {
    kind: i16,
    source_kind: Option<i16>,
    source_ref: Option<String>,
    publisher_ref: Option<String>,
    created_by: Option<uuid::Uuid>,
}

fn encode_provenance(
    value: &SkillProvenance,
) -> Result<EncodedProvenance, SkillLifecycleAdapterError> {
    Ok(match value {
        SkillProvenance::Bundled { .. } => return Err(SkillLifecycleAdapterError::Corrupt),
        SkillProvenance::Custom { created_by } => EncodedProvenance {
            kind: 2,
            source_kind: None,
            source_ref: None,
            publisher_ref: None,
            created_by: Some(created_by.into_uuid()),
        },
        SkillProvenance::Installed {
            source,
            source_ref,
            publisher_ref,
        } => EncodedProvenance {
            kind: 3,
            source_kind: Some(match source {
                InstalledSkillSource::Marketplace => 1,
                InstalledSkillSource::Hub => 2,
                InstalledSkillSource::GitHub => 3,
                InstalledSkillSource::Url => 4,
            }),
            source_ref: Some(source_ref.clone()),
            publisher_ref: Some(publisher_ref.clone()),
            created_by: None,
        },
    })
}

fn json_keys(values: &std::collections::BTreeSet<VersionedKey>) -> serde_json::Value {
    serde_json::Value::Array(
        values
            .iter()
            .map(|value| serde_json::Value::String(value.as_str().to_owned()))
            .collect(),
    )
}

fn decode_keys(
    value: serde_json::Value,
) -> Result<std::collections::BTreeSet<VersionedKey>, SkillLifecycleAdapterError> {
    let values = value
        .as_array()
        .ok_or(SkillLifecycleAdapterError::Corrupt)?;
    let parsed = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or(SkillLifecycleAdapterError::Corrupt)
                .and_then(|value| {
                    VersionedKey::new(value).map_err(|_| SkillLifecycleAdapterError::Corrupt)
                })
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    if parsed.len() != values.len() {
        return Err(SkillLifecycleAdapterError::Corrupt);
    }
    Ok(parsed)
}

const fn source_format(value: SkillSourceFormat) -> i16 {
    match value {
        SkillSourceFormat::KernelSourceV1 => 1,
        SkillSourceFormat::CodexFolderV1 => 2,
        SkillSourceFormat::ExtendedFolderV1 => 3,
        SkillSourceFormat::PluginProvidedV1 => 4,
        SkillSourceFormat::GenericSkillFolderV1 => 5,
    }
}

fn decode_source_format(value: i16) -> Result<SkillSourceFormat, SkillLifecycleAdapterError> {
    match value {
        1 => Ok(SkillSourceFormat::KernelSourceV1),
        2 => Ok(SkillSourceFormat::CodexFolderV1),
        3 => Ok(SkillSourceFormat::ExtendedFolderV1),
        4 => Ok(SkillSourceFormat::PluginProvidedV1),
        5 => Ok(SkillSourceFormat::GenericSkillFolderV1),
        _ => Err(SkillLifecycleAdapterError::Corrupt),
    }
}

fn resource_role(value: SkillResourceRole) -> u8 {
    match value {
        SkillResourceRole::Instructions => 1,
        SkillResourceRole::Reference => 2,
        SkillResourceRole::Script => 3,
        SkillResourceRole::Asset => 4,
        SkillResourceRole::UiMetadata => 5,
        SkillResourceRole::Workflow => 6,
        SkillResourceRole::Prompt => 7,
        SkillResourceRole::Example => 8,
        SkillResourceRole::License => 9,
        SkillResourceRole::Other => 10,
    }
}

fn decode_resource_role(value: u64) -> Result<SkillResourceRole, SkillLifecycleAdapterError> {
    match value {
        1 => Ok(SkillResourceRole::Instructions),
        2 => Ok(SkillResourceRole::Reference),
        3 => Ok(SkillResourceRole::Script),
        4 => Ok(SkillResourceRole::Asset),
        5 => Ok(SkillResourceRole::UiMetadata),
        6 => Ok(SkillResourceRole::Workflow),
        7 => Ok(SkillResourceRole::Prompt),
        8 => Ok(SkillResourceRole::Example),
        9 => Ok(SkillResourceRole::License),
        10 => Ok(SkillResourceRole::Other),
        _ => Err(SkillLifecycleAdapterError::Corrupt),
    }
}

fn compatibility_code(value: SkillCompatibilityFindingCode) -> u8 {
    match value {
        SkillCompatibilityFindingCode::InlineShell => 1,
        SkillCompatibilityFindingCode::ExternalPathVariable => 2,
        SkillCompatibilityFindingCode::HostPathReference => 3,
        SkillCompatibilityFindingCode::PackageDependencyRequired => 4,
    }
}

fn decode_compatibility_code(
    value: u64,
) -> Result<SkillCompatibilityFindingCode, SkillLifecycleAdapterError> {
    match value {
        1 => Ok(SkillCompatibilityFindingCode::InlineShell),
        2 => Ok(SkillCompatibilityFindingCode::ExternalPathVariable),
        3 => Ok(SkillCompatibilityFindingCode::HostPathReference),
        4 => Ok(SkillCompatibilityFindingCode::PackageDependencyRequired),
        _ => Err(SkillLifecycleAdapterError::Corrupt),
    }
}

fn encode_resources(values: &[SkillResourceDescriptor]) -> serde_json::Value {
    serde_json::Value::Array(
        values
            .iter()
            .map(|value| {
                serde_json::json!({
                    "path": value.path.as_str(),
                    "media_type": value.media_type.as_str(),
                    "byte_length": value.byte_length.to_string(),
                    "sha256": value.sha256.to_string(),
                    "role": resource_role(value.role),
                })
            })
            .collect(),
    )
}

fn decode_resources(
    value: serde_json::Value,
) -> Result<Vec<SkillResourceDescriptor>, SkillLifecycleAdapterError> {
    value
        .as_array()
        .ok_or(SkillLifecycleAdapterError::Corrupt)?
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or(SkillLifecycleAdapterError::Corrupt)?;
            let hash = decode_hex_hash(required_json_string(object, "sha256")?)?;
            Ok(SkillResourceDescriptor {
                path: SkillResourcePath::new(required_json_string(object, "path")?)
                    .map_err(|_| SkillLifecycleAdapterError::Corrupt)?,
                media_type: MediaType::new(required_json_string(object, "media_type")?)
                    .map_err(|_| SkillLifecycleAdapterError::Corrupt)?,
                byte_length: required_json_string(object, "byte_length")?
                    .parse()
                    .map_err(|_| SkillLifecycleAdapterError::Corrupt)?,
                sha256: hash,
                role: decode_resource_role(
                    object
                        .get("role")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or(SkillLifecycleAdapterError::Corrupt)?,
                )?,
            })
        })
        .collect()
}

fn encode_compatibility_findings(values: &[SkillCompatibilityFinding]) -> serde_json::Value {
    serde_json::Value::Array(
        values
            .iter()
            .map(|value| {
                serde_json::json!({
                    "code": compatibility_code(value.code),
                    "resource_path": value.resource_path.as_str(),
                })
            })
            .collect(),
    )
}

fn decode_compatibility_findings(
    value: serde_json::Value,
) -> Result<Vec<SkillCompatibilityFinding>, SkillLifecycleAdapterError> {
    value
        .as_array()
        .ok_or(SkillLifecycleAdapterError::Corrupt)?
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or(SkillLifecycleAdapterError::Corrupt)?;
            Ok(SkillCompatibilityFinding {
                code: decode_compatibility_code(
                    object
                        .get("code")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or(SkillLifecycleAdapterError::Corrupt)?,
                )?,
                resource_path: SkillResourcePath::new(required_json_string(
                    object,
                    "resource_path",
                )?)
                .map_err(|_| SkillLifecycleAdapterError::Corrupt)?,
            })
        })
        .collect()
}

fn required_json_string<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<&'a str, SkillLifecycleAdapterError> {
    object
        .get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or(SkillLifecycleAdapterError::Corrupt)
}

fn decode_hex_hash(value: &str) -> Result<Sha256, SkillLifecycleAdapterError> {
    if value.len() != Sha256::LENGTH * 2 {
        return Err(SkillLifecycleAdapterError::Corrupt);
    }
    let mut bytes = [0_u8; Sha256::LENGTH];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).map_err(|_| SkillLifecycleAdapterError::Corrupt)?;
        bytes[index] =
            u8::from_str_radix(pair, 16).map_err(|_| SkillLifecycleAdapterError::Corrupt)?;
    }
    Sha256::from_bytes(bytes).map_err(|_| SkillLifecycleAdapterError::Corrupt)
}

fn decode_surfaces(
    values: Vec<i16>,
) -> Result<std::collections::BTreeSet<Surface>, SkillLifecycleAdapterError> {
    let parsed = values
        .iter()
        .map(|value| match value {
            1 => Ok(Surface::Web),
            2 => Ok(Surface::Studio),
            3 => Ok(Surface::Api),
            4 => Ok(Surface::Desktop),
            5 => Ok(Surface::Channel),
            6 => Ok(Surface::Mobile),
            _ => Err(SkillLifecycleAdapterError::Corrupt),
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    if parsed.len() != values.len() {
        return Err(SkillLifecycleAdapterError::Corrupt);
    }
    Ok(parsed)
}

fn content_ref(
    row: &sqlx::postgres::PgRow,
    prefix: &str,
) -> Result<ContentRef, SkillLifecycleAdapterError> {
    Ok(ContentRef {
        content_id: id(row.try_get::<uuid::Uuid, _>(format!("{prefix}_content_id").as_str())?)?,
        media_type: MediaType::new(
            row.try_get::<String, _>(format!("{prefix}_media_type").as_str())?,
        )
        .map_err(|_| SkillLifecycleAdapterError::Corrupt)?,
        byte_length: parse_u64(row.try_get(format!("{prefix}_byte_length").as_str())?)?,
        sha256: hash(row, format!("{prefix}_hash").as_str())?,
    })
}

fn hash(row: &sqlx::postgres::PgRow, column: &str) -> Result<Sha256, SkillLifecycleAdapterError> {
    Sha256::from_bytes(row.try_get::<Vec<u8>, _>(column)?)
        .map_err(|_| SkillLifecycleAdapterError::Corrupt)
}

fn required_string(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<String, SkillLifecycleAdapterError> {
    row.try_get::<Option<String>, _>(column)?
        .ok_or(SkillLifecycleAdapterError::Corrupt)
}

fn required_uuid(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<uuid::Uuid, SkillLifecycleAdapterError> {
    row.try_get::<Option<uuid::Uuid>, _>(column)?
        .ok_or(SkillLifecycleAdapterError::Corrupt)
}

fn id<T>(value: uuid::Uuid) -> Result<T, SkillLifecycleAdapterError>
where
    T: std::str::FromStr,
{
    value
        .to_string()
        .parse()
        .map_err(|_| SkillLifecycleAdapterError::Corrupt)
}

fn optional_id<T>(value: Option<uuid::Uuid>) -> Result<Option<T>, SkillLifecycleAdapterError>
where
    T: std::str::FromStr,
{
    value.map(id).transpose()
}

fn parse_u64(value: String) -> Result<u64, SkillLifecycleAdapterError> {
    value
        .parse()
        .map_err(|_| SkillLifecycleAdapterError::Corrupt)
}

const fn trust(value: SkillTrust) -> i16 {
    match value {
        SkillTrust::Official => 1,
        SkillTrust::Verified => 2,
        SkillTrust::Community => 3,
        SkillTrust::Local => 4,
    }
}

const fn surface(value: Surface) -> i16 {
    value.canonical_code() as i16
}

const fn quarantine(value: QuarantineDisposition) -> i16 {
    match value {
        QuarantineDisposition::Cleared => 1,
        QuarantineDisposition::Rejected => 2,
    }
}

enum IdentityPredicate<'a> {
    Id(SkillId),
    Key(&'a SkillKey),
}

enum IdentityValue<'a> {
    Id(SkillId),
    Key(&'a SkillKey),
}

#[derive(Debug, thiserror::Error)]
enum SkillLifecycleAdapterError {
    #[error("Skill lifecycle database state is corrupt")]
    Corrupt,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Skill Lifecycle Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
}

impl SkillLifecycleAdapterError {
    fn observe_failure(&self) {
        match self {
            Self::Database(sqlx::Error::Database(error)) => tracing::warn!(
                store = "skill_lifecycle",
                sqlstate = error.code().as_deref().unwrap_or("unknown"),
                constraint = error.constraint().unwrap_or("none"),
                "Skill persistence rejected an operation"
            ),
            Self::Database(_) => tracing::warn!(
                store = "skill_lifecycle",
                failure = "database_transport_or_codec",
                "Skill persistence operation failed"
            ),
            Self::Transaction(_) => tracing::warn!(
                store = "skill_lifecycle",
                failure = "transaction",
                "Skill persistence transaction failed"
            ),
            Self::Corrupt => {}
        }
    }
    fn port(self) -> SkillLifecycleStoreError {
        self.observe_failure();
        match self {
            Self::Database(_) | Self::Transaction(_) => SkillLifecycleStoreError::Unavailable,
            Self::Corrupt => SkillLifecycleStoreError::Corrupt,
        }
    }

    fn product_port(self) -> SkillProductStoreError {
        self.observe_failure();
        match self {
            Self::Database(_) | Self::Transaction(_) => SkillProductStoreError::Unavailable,
            Self::Corrupt => SkillProductStoreError::Corrupt,
        }
    }
}
