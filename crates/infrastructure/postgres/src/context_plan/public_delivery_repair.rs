//! The original ContextPlan transaction validates both the private candidate
//! authority and the exact canonical request persisted by the planner.
use super::*;
use musteragent_runtime_content::verify_content;
use std::num::NonZeroUsize;

impl PostgresContextPlanStore {
    pub(super) async fn validate_delivery_repair(
        &self,
        conn: &mut sqlx::PgConnection,
        tenant: TenantId,
        plan: &ContextPlanCommit,
        current_offset: LedgerOffset,
    ) -> Result<(), PostgresContextPlanError> {
        let current_cutoff = musteragent_identity::ConversationSeq::new(current_offset.get())
            .map_err(|_| PostgresContextPlanError::CorruptState)?;
        if plan.source_fence.ledger_cutoff > current_cutoff {
            return Err(PostgresContextPlanError::SourceChanged);
        }
        let expected = crate::public_delivery_repair::current_binding(
            conn,
            tenant,
            plan.snapshot.run_id,
            current_cutoff,
        )
        .await
        .map_err(map_error)?;
        if plan.snapshot.delivery_repair != expected {
            return Err(PostgresContextPlanError::SourceChanged);
        }
        let Some(binding) = plan.snapshot.delivery_repair.as_ref() else {
            return Ok(());
        };
        crate::public_delivery_repair::validate_binding(
            conn,
            tenant,
            plan.snapshot.run_id,
            binding,
            current_cutoff,
        )
        .await
        .map_err(map_error)?;
        if !plan
            .source_fence
            .content_refs
            .contains(&binding.candidate.content_ref)
            || plan.snapshot.source_seq_to > plan.source_fence.ledger_cutoff
        {
            return Err(PostgresContextPlanError::IdentityMismatch);
        }
        let content = self
            .content
            .as_ref()
            .ok_or(PostgresContextPlanError::IdentityMismatch)?;
        let reference = &plan.snapshot.model_request_ref;
        let bytes = content
            .read(tenant, reference)
            .await
            .map_err(PostgresContextPlanError::Content)?;
        verify_content(reference, &bytes)
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        let request = musteragent_persistence_codec::CanonicalJsonContextCodec
            .decode_model_request(
                &bytes,
                NonZeroUsize::new(bytes.len()).ok_or(PostgresContextPlanError::IdentityMismatch)?,
            )
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        if request.invocation.invocation_id != plan.invocation_id
            || request.invocation.run_context.tenant_id != tenant
            || request.invocation.run_context.run_id != plan.snapshot.run_id
            || request.invocation.attempt_context != plan.attempt
        {
            return Err(PostgresContextPlanError::IdentityMismatch);
        }
        let (_, repair) = crate::public_delivery_repair::load_candidate(
            conn,
            tenant,
            plan.snapshot.run_id,
            binding.rejection_invocation_id,
        )
        .await
        .map_err(map_error)?;
        let candidate = content
            .read(tenant, &binding.candidate.content_ref)
            .await
            .map_err(PostgresContextPlanError::Content)?;
        verify_content(&binding.candidate.content_ref, &candidate)
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        let material = crate::public_delivery_repair::authoritative_material(
            self.ledger.database().clone(),
            content.clone(),
            self.repair_decoder
                .clone()
                .ok_or(PostgresContextPlanError::IdentityMismatch)?,
            &request,
            plan.snapshot.iteration,
            self.application_catalogs.clone(),
        )
        .await
        .map_err(material_error)?;
        musteragent_context::validate_public_delivery_repair_request(
            &repair, &candidate, &material, &request,
        )
        .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        Ok(())
    }
}
fn map_error(error: crate::public_delivery_repair::RepairStoreError) -> PostgresContextPlanError {
    match error {
        crate::public_delivery_repair::RepairStoreError::Database(error) => error.into(),
        crate::public_delivery_repair::RepairStoreError::Changed => {
            PostgresContextPlanError::SourceChanged
        }
        crate::public_delivery_repair::RepairStoreError::Corrupt => {
            PostgresContextPlanError::CorruptState
        }
    }
}

fn material_error(error: crate::PostgresContextMaterialError) -> PostgresContextPlanError {
    match error {
        crate::PostgresContextMaterialError::Database(error) => error.into(),
        crate::PostgresContextMaterialError::Transaction(error) => error.into(),
        crate::PostgresContextMaterialError::Content(error) => {
            PostgresContextPlanError::Content(error)
        }
        crate::PostgresContextMaterialError::StaleFence => PostgresContextPlanError::SourceChanged,
        _ => PostgresContextPlanError::CorruptState,
    }
}
