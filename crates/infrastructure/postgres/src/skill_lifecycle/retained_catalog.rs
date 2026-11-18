//! Read immutable archived publication material, independent of active pointers.
use super::*;
use musteragent_identity::ProductId;

impl PostgresSkillLifecycleStore {
    pub(super) async fn retained_entries(
        &self,
        product: ProductId,
        bases: &[VersionedKey],
    ) -> Result<musteragent_runtime_skill::VerifiedRetainedSkillCatalog, SkillLifecycleStoreError>
    {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| SkillLifecycleStoreError::Unavailable)?;
        Self::retained_entries_in_transaction(&mut connection, product, bases).await
    }

    pub(crate) async fn retained_entries_in_transaction(
        connection: &mut sqlx::PgConnection,
        product: ProductId,
        bases: &[VersionedKey],
    ) -> Result<musteragent_runtime_skill::VerifiedRetainedSkillCatalog, SkillLifecycleStoreError>
    {
        let [base] = bases else {
            return Err(SkillLifecycleStoreError::Corrupt);
        };
        let row: Option<(Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "SELECT catalog_sha256,encoded_catalog FROM skill.retained_admission_catalogs WHERE product=$1 AND base_skill_ref=$2"
        ).bind(product.as_str()).bind(base.as_str()).fetch_optional(connection).await
            .map_err(|_| SkillLifecycleStoreError::Unavailable)?;
        let (digest, bytes) = row.ok_or(SkillLifecycleStoreError::Unavailable)?;
        let digest = Sha256::from_bytes(digest).map_err(|_| SkillLifecycleStoreError::Corrupt)?;
        musteragent_persistence_codec::account_skill_codec::decode_retained_catalog(
            product,
            base.clone(),
            digest,
            bytes,
        )
    }
}
