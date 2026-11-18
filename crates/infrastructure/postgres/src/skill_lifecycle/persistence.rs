//! Original lifecycle SQL shared by normal writes and durable import transactions.
use super::*;

pub(super) async fn load_identity_inner(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    predicate: IdentityPredicate<'_>,
) -> Result<Option<SkillIdentity>, SkillLifecycleAdapterError> {
    let (sql, value) = match predicate {
        IdentityPredicate::Id(id) => (
            "SELECT skill_id, skill_key, provenance_kind, source_kind, source_ref,
                        publisher_ref, created_by, trust_level, created_at_ms
                 FROM skill.skills
                 WHERE tenant_id = $1 AND skill_id = $2",
            IdentityValue::Id(id),
        ),
        IdentityPredicate::Key(key) => (
            "SELECT skill_id, skill_key, provenance_kind, source_kind, source_ref,
                        publisher_ref, created_by, trust_level, created_at_ms
                 FROM skill.skills
                 WHERE tenant_id = $1 AND skill_key = $2",
            IdentityValue::Key(key),
        ),
    };
    let query = sqlx::query(sql).bind(tenant_id.into_uuid());
    let row = match value {
        IdentityValue::Id(id) => {
            query
                .bind(id.into_uuid())
                .fetch_optional(&mut *connection)
                .await?
        }
        IdentityValue::Key(key) => {
            query
                .bind(key.as_str())
                .fetch_optional(&mut *connection)
                .await?
        }
    };
    row.map(|row| decode_identity(&row)).transpose()
}

pub(super) async fn load_version_inner(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    skill_version_id: SkillVersionId,
) -> Result<Option<SkillPackageRecord>, SkillLifecycleAdapterError> {
    let row = sqlx::query(VERSIONS_SELECT)
        .bind(tenant_id.into_uuid())
        .bind(vec![skill_version_id.into_uuid()])
        .fetch_optional(&mut *connection)
        .await?;
    row.map(|row| decode_package(&row)).transpose()
}

pub(super) async fn insert_identity_inner(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    identity: SkillIdentity,
) -> Result<SkillIdentityInsertOutcome, SkillLifecycleAdapterError> {
    identity
        .validate()
        .map_err(|_| SkillLifecycleAdapterError::Corrupt)?;
    let provenance = encode_provenance(&identity.provenance)?;
    let owner_tenant_id = tenant_id.into_uuid();
    let changed = sqlx::query(
        "INSERT INTO skill.skills (
                 skill_id, skill_key, lifecycle, created_at_ms, tenant_id,
                 provenance_kind, source_kind, source_ref, publisher_ref, created_by, trust_level
             ) VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT DO NOTHING",
    )
    .bind(identity.skill_id.into_uuid())
    .bind(identity.skill_key.as_str())
    .bind(identity.created_at.unix_millis())
    .bind(owner_tenant_id)
    .bind(provenance.kind)
    .bind(provenance.source_kind)
    .bind(provenance.source_ref)
    .bind(provenance.publisher_ref)
    .bind(provenance.created_by)
    .bind(trust(identity.trust))
    .execute(&mut *connection)
    .await?;
    if changed.rows_affected() == 1 {
        Ok(SkillIdentityInsertOutcome::Inserted)
    } else {
        let existing = load_identity_inner(
            &mut *connection,
            tenant_id,
            IdentityPredicate::Id(identity.skill_id),
        )
        .await?;
        existing
            .or(load_identity_inner(
                &mut *connection,
                tenant_id,
                IdentityPredicate::Key(&identity.skill_key),
            )
            .await?)
            .map(SkillIdentityInsertOutcome::Existing)
            .ok_or(SkillLifecycleAdapterError::Corrupt)
    }
}

pub(super) async fn insert_version_inner(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    package: VerifiedSkillPackage,
) -> Result<SkillVersionInsertOutcome, SkillLifecycleAdapterError> {
    package
        .validate()
        .map_err(|_| SkillLifecycleAdapterError::Corrupt)?;
    let identity = sqlx::query(
        "SELECT skill_id, skill_key, provenance_kind, source_kind, source_ref,
                    publisher_ref, created_by, trust_level, created_at_ms
             FROM skill.skills
             WHERE tenant_id = $1 AND skill_id = $2 FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(package.identity.skill_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await?;
    if identity
        .map(|row| decode_identity(&row))
        .transpose()?
        .as_ref()
        != Some(&package.identity)
    {
        return Err(SkillLifecycleAdapterError::Corrupt);
    }
    let version = &package.version;
    let owner_tenant_id = tenant_id.into_uuid();
    if !content_ref_is_ready(&mut *connection, owner_tenant_id, &version.instructions_ref).await?
        || !content_ref_is_ready(&mut *connection, owner_tenant_id, &version.manifest_ref).await?
        || !content_ref_is_ready(&mut *connection, owner_tenant_id, &version.package_ref).await?
    {
        return Err(SkillLifecycleAdapterError::Corrupt);
    }
    let inserted = sqlx::query(
        "INSERT INTO skill.skill_versions (
                 skill_version_id, skill_id, version, manifest_hash,
                 skill_ref, supersedes, title, description, when_to_use,
                 declared_version, source_format,
                 instructions_content_id,
                 instructions_media_type, instructions_byte_length, instructions_hash,
                 manifest_content_id, manifest_media_type, manifest_byte_length,
                 package_content_id, package_media_type, package_byte_length,
                 package_hash, package_dependencies, resources,
                 compatibility_findings,
                 required_tools, optional_tools, compatible_agent_contracts,
                 compatible_surfaces, file_set_hash, audit_digest, created_at_ms
             ) VALUES (
                 $1, $2, $3::bigint, $4, $5, $6, $7, $8, $9,
                 $10, $11, $12, $13, $14::bigint, $15, $16, $17, $18::bigint,
                 $19, $20, $21::bigint, $22, $23, $24, $25, $26,
                 $27, $28, $29, $30, $31, $32
             ) ON CONFLICT DO NOTHING",
    )
    .bind(version.skill_version_id.into_uuid())
    .bind(version.skill_id.into_uuid())
    .bind(version.version.get().to_string())
    .bind(version.manifest_hash.into_bytes().as_slice())
    .bind(version.skill_ref.as_str())
    .bind(version.supersedes.map(SkillVersionId::into_uuid))
    .bind(&version.title)
    .bind(&version.description)
    .bind(&version.when_to_use)
    .bind(&version.declared_version)
    .bind(source_format(version.source_format))
    .bind(version.instructions_ref.content_id.into_uuid())
    .bind(version.instructions_ref.media_type.as_str())
    .bind(version.instructions_ref.byte_length.to_string())
    .bind(version.instructions_hash.into_bytes().as_slice())
    .bind(version.manifest_ref.content_id.into_uuid())
    .bind(version.manifest_ref.media_type.as_str())
    .bind(version.manifest_ref.byte_length.to_string())
    .bind(version.package_ref.content_id.into_uuid())
    .bind(version.package_ref.media_type.as_str())
    .bind(version.package_ref.byte_length.to_string())
    .bind(version.package_hash.into_bytes().as_slice())
    .bind(json_keys(&version.package_dependencies))
    .bind(encode_resources(&package.resources))
    .bind(encode_compatibility_findings(
        &package.compatibility_findings,
    ))
    .bind(json_keys(&version.required_tools))
    .bind(json_keys(&version.optional_tools))
    .bind(json_keys(&version.compatible_agent_contracts))
    .bind(
        version
            .compatible_surfaces
            .iter()
            .copied()
            .map(surface)
            .collect::<Vec<_>>(),
    )
    .bind(version.payload_hash.into_bytes().as_slice())
    .bind(version.audit_digest.into_bytes().as_slice())
    .bind(version.created_at.unix_millis())
    .execute(&mut *connection)
    .await?;
    if inserted.rows_affected() == 0 {
        let existing =
            load_version_inner(&mut *connection, tenant_id, version.skill_version_id).await?;
        return existing
            .or(load_conflicting_version(
                &mut *connection,
                tenant_id,
                version.skill_id,
                version.version,
                &version.skill_ref,
            )
            .await?)
            .map(|value| SkillVersionInsertOutcome::Existing(Box::new(value)))
            .ok_or(SkillLifecycleAdapterError::Corrupt);
    }
    insert_audit(&mut *connection, owner_tenant_id, &package).await?;
    Ok(SkillVersionInsertOutcome::Inserted)
}

pub(super) async fn load_active_inner(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    skill_id: SkillId,
) -> Result<Option<ActiveSkillVersionPointer>, SkillLifecycleAdapterError> {
    let row = sqlx::query(
        "SELECT skill_id, skill_version_id, pointer_version::text AS pointer_version,
                    saved, installed, enabled, activated_at_ms, activated_by
             FROM skill.active_version_pointers
             WHERE tenant_id = $1 AND skill_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(skill_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await?;
    row.map(|row| decode_pointer(&row)).transpose()
}

pub(super) async fn cas_inner(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    skill_id: SkillId,
    expected: Option<ActiveSkillVersionPointer>,
    replacement: ActiveSkillVersionPointer,
) -> Result<ActivePointerCasOutcome, SkillLifecycleAdapterError> {
    if replacement.skill_id != skill_id
        || expected.is_some_and(|pointer| pointer.skill_id != skill_id)
    {
        return Err(SkillLifecycleAdapterError::Corrupt);
    }
    // Serialize all original pointer/version writers on their Skill identity.
    sqlx::query(
        "SELECT skill_id FROM skill.skills WHERE tenant_id = $1 AND skill_id = $2 FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(skill_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await?;
    let changed = if let Some(expected) = expected {
        sqlx::query(
            "UPDATE skill.active_version_pointers AS pointer
                 SET skill_version_id = $4, pointer_version = $5::bigint,
                     saved = $9, installed = $10, enabled = $11,
                     activated_at_ms = $6, activated_by = $7
                 WHERE pointer.tenant_id = $1 AND pointer.skill_id = $2
                   AND pointer.skill_version_id = $3
                   AND pointer.pointer_version = $8::bigint
                   AND $5::bigint = pointer.pointer_version + 1
                   AND EXISTS (
                       SELECT 1 FROM skill.skill_versions AS version
                       JOIN skill.package_audits AS audit
                         ON audit.skill_version_id = version.skill_version_id
                       JOIN skill.skills AS definition
                         ON definition.skill_id = version.skill_id
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
                       WHERE version.skill_version_id = $4 AND version.skill_id = $2
                         AND version.created_at_ms <= $6
                         AND definition.tenant_id = $1
                         AND audit.tenant_id = definition.tenant_id
                         AND audit.manifest_hash = version.manifest_hash
                         AND audit.file_set_hash = version.file_set_hash
                         AND audit.audit_digest = version.audit_digest
                   )",
        )
        .bind(tenant_id.into_uuid())
        .bind(skill_id.into_uuid())
        .bind(expected.skill_version_id.into_uuid())
        .bind(replacement.skill_version_id.into_uuid())
        .bind(replacement.pointer_version.to_string())
        .bind(replacement.activated_at.unix_millis())
        .bind(replacement.activated_by.into_uuid())
        .bind(expected.pointer_version.to_string())
        .bind(replacement.saved)
        .bind(replacement.installed)
        .bind(replacement.enabled)
        .execute(&mut *connection)
        .await?
    } else {
        sqlx::query(
            "INSERT INTO skill.active_version_pointers (
                     tenant_id, skill_id, skill_version_id, pointer_version,
                     saved, installed, enabled, activated_at_ms, activated_by
                 ) SELECT $1, $2, $3, $4::bigint, $7, $8, $9, $5, $6
                   FROM skill.skill_versions AS version
                   JOIN skill.package_audits AS audit USING (skill_version_id)
                   JOIN skill.skills AS definition USING (skill_id)
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
                  WHERE version.skill_version_id = $3 AND version.skill_id = $2
                    AND version.version = 1 AND version.supersedes IS NULL
                    AND $4::bigint = 1 AND version.created_at_ms <= $5
                    AND definition.tenant_id = $1
                    AND audit.tenant_id = definition.tenant_id
                    AND audit.manifest_hash = version.manifest_hash
                    AND audit.file_set_hash = version.file_set_hash
                    AND audit.audit_digest = version.audit_digest
                 ON CONFLICT DO NOTHING",
        )
        .bind(tenant_id.into_uuid())
        .bind(skill_id.into_uuid())
        .bind(replacement.skill_version_id.into_uuid())
        .bind(replacement.pointer_version.to_string())
        .bind(replacement.activated_at.unix_millis())
        .bind(replacement.activated_by.into_uuid())
        .bind(replacement.saved)
        .bind(replacement.installed)
        .bind(replacement.enabled)
        .execute(&mut *connection)
        .await?
    };
    if changed.rows_affected() == 1 {
        Ok(ActivePointerCasOutcome::Swapped(replacement))
    } else {
        Ok(ActivePointerCasOutcome::Conflict(
            load_active_inner(&mut *connection, tenant_id, skill_id).await?,
        ))
    }
}

pub(super) async fn load_latest_package_inner(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    skill_id: SkillId,
) -> Result<Option<SkillPackageRecord>, SkillLifecycleAdapterError> {
    let version_id = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT version.skill_version_id
             FROM skill.skill_versions AS version
             JOIN skill.skills AS definition USING (skill_id)
             WHERE definition.tenant_id = $1
               AND version.skill_id = $2
             ORDER BY version.version DESC LIMIT 1",
    )
    .bind(tenant_id.into_uuid())
    .bind(skill_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await?;
    match version_id {
        Some(value) => load_version_inner(&mut *connection, tenant_id, id(value)?).await,
        None => Ok(None),
    }
}

pub(super) async fn load_conflicting_version(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    skill_id: SkillId,
    version: VersionNumber,
    skill_ref: &VersionedKey,
) -> Result<Option<SkillPackageRecord>, SkillLifecycleAdapterError> {
    let ids = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT version.skill_version_id
             FROM skill.skill_versions AS version
             JOIN skill.skills AS definition USING (skill_id)
             WHERE definition.tenant_id = $1
               AND ((version.skill_id = $2 AND version.version = $3::bigint)
                    OR version.skill_ref = $4)",
    )
    .bind(tenant_id.into_uuid())
    .bind(skill_id.into_uuid())
    .bind(version.get().to_string())
    .bind(skill_ref.as_str())
    .fetch_all(&mut *connection)
    .await?;
    if ids.len() > 1 {
        return Err(SkillLifecycleAdapterError::Corrupt);
    }
    let Some(skill_version_id) = ids.into_iter().next() else {
        return Ok(None);
    };
    load_version_inner(&mut *connection, tenant_id, id(skill_version_id)?).await
}
