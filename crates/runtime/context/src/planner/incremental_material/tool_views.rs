//! Cache the existing Tool-result visibility owner's pure projection. Policy
//! selection from the current Snapshot remains in tool_result_visibility.
use super::*;
use crate::tool_result_visibility::{ResultView, project_message};
use musteragent_identity::TenantId;

struct ProjectedMessage(CanonicalMessage);
impl RetainedContentMaterial for ProjectedMessage {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(message_identity::message_allocations(&self.0))
    }
}

impl MaterialReuse {
    pub(in crate::planner) fn project_tool_view(
        &self,
        tenant_id: TenantId,
        message: &mut CanonicalMessage,
        view: ResultView,
    ) -> Result<(), ContextPlanError> {
        let projected = self.memo(
            tenant_id,
            "context.tool-result-view.v1",
            || {
                let mut key = Fingerprint::new(b"musteragent.context.tool-result-view.v1\0");
                key.field(view.cache_identity());
                key.message(message);
                Ok(key.finish())
            },
            || {
                let mut projected = message.clone();
                project_message(&mut projected, view)?;
                Ok(ProjectedMessage(projected))
            },
        )?;
        *message = projected.0.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn visibility_view_changes_are_reprojected_without_restoring_hidden_fields() {
        let cache = Arc::new(VerifiedContentCache::default());
        let reuse = MaterialReuse(Some((
            cache.clone(),
            cache.begin_generation().expect("generation"),
        )));
        let tenant = TenantId::generate();
        let original = crate::planner::tests::entry(1, None, r#"{"sha256":"private-digest","result_hash":"private-result","stdout":"full user content"}"#).message;
        for view in [
            ResultView::File,
            ResultView::File,
            ResultView::Python,
            ResultView::Python,
            ResultView::Search,
            ResultView::Application,
        ] {
            let mut full = original.clone();
            project_message(&mut full, view).expect("full");
            let mut incremental = original.clone();
            reuse
                .project_tool_view(tenant, &mut incremental, view)
                .expect("incremental");
            assert_eq!(full, incremental);
        }
        assert_eq!(cache.stats().expect("stats").material_hits, 2);
        assert_eq!(cache.stats().expect("stats").material_misses, 4);
    }
}
