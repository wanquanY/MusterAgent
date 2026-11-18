//! Context material source ports; preparation and physical reads stay distinct.
use super::*;

impl ContextMaterialSource for PostgresContextMaterialSource {
    fn can_preview_initial_dialogue_pages(&self) -> bool {
        true
    }

    fn dialogue_source_preview<'a>(
        &'a self,
        input: &'a ContextPlanInput,
        window: &'a musteragent_context::DialogueContextWindow,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<Option<Vec<musteragent_context::DialogueSourcePreview>>, ContextPlanError>,
    > {
        Box::pin(async move {
            self.dialogue_source_preview_inner(input, window)
                .await
                .map_err(|e| e.plan_error())
        })
    }
    fn preparation_view(&self, input: &ContextPlanInput) -> Option<Arc<dyn ContextMaterialSource>> {
        let mut source = self.clone();
        source.preparation = Some(Arc::new(preparation::SnapshotPreparation::new(
            input.clone(),
        )));
        Some(Arc::new(source))
    }

    fn dialogue_window(
        &self,
        input: &ContextPlanInput,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<musteragent_context::DialogueContextWindow>, ContextPlanError>,
    > {
        let input = input.clone();
        Box::pin(async move {
            self.dialogue_window_inner(&input)
                .await
                .map_err(|error| error.plan_error())
        })
    }

    fn dialogue_capacity_delta<'a>(
        &'a self,
        input: &'a ContextPlanInput,
        basis: &'a musteragent_context::OwnerCapacityBasis,
        window: &'a musteragent_context::DialogueContextWindow,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<Option<musteragent_context::DialogueCapacityDelta>, ContextPlanError>,
    > {
        Box::pin(async move {
            self.dialogue_capacity_delta_inner(input, basis, window)
                .await
                .map_err(|error| error.plan_error())
        })
    }

    fn load(
        &self,
        input: &ContextPlanInput,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ContextMaterial, ContextPlanError>,
    > {
        let input = input.clone();
        Box::pin(async move {
            self.load_inner(&input).await.map_err(|error| {
                if let Some(sqlstate) = error.database_sqlstate() {
                    tracing::warn!(
                        run_id = %input.attempt.run_id,
                        attempt_id = %input.attempt.attempt_id,
                        attempt_number = input.attempt.attempt_number.get(),
                        material.error = error.kind(),
                        material.stage = error.stage().unwrap_or("unspecified"),
                        database.sqlstate = sqlstate.as_str(),
                        "context material load failed"
                    );
                } else {
                    tracing::warn!(
                        run_id = %input.attempt.run_id,
                        attempt_id = %input.attempt.attempt_id,
                        attempt_number = input.attempt.attempt_number.get(),
                        material.error = error.kind(),
                        material.stage = error.stage().unwrap_or("unspecified"),
                        "context material load failed"
                    );
                }
                error.plan_error()
            })
        })
    }
}
