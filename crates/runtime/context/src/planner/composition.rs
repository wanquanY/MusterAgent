//! Composition of the original planner and its original persistence ports.
use super::*;
impl DurableContextPlanner {
    #[must_use]
    pub fn new(
        source: Arc<dyn ContextMaterialSource>,
        content: Arc<dyn ContentStore>,
        plans: Arc<dyn ContextPlanStore>,
        codec: Arc<dyn ContextCodec>,
        compactor: Arc<dyn ContextCompactor>,
    ) -> Self {
        let material_cache = content.material_cache();
        let mut planner = Self::from_ports(
            source,
            Arc::new(crate::content::StoredContextPlanContent(content)),
            plans,
            codec,
            compactor,
        );
        planner.material_cache = material_cache;
        planner
    }

    /// Composes the original planner with purpose-aware persistence. Shared
    /// execution installs its Native writer here; private composition keeps
    /// its existing ContentStore adapter through `new`.
    #[must_use]
    pub fn from_ports(
        source: Arc<dyn ContextMaterialSource>,
        content: Arc<dyn ContextPlanContentWriter>,
        plans: Arc<dyn ContextPlanStore>,
        codec: Arc<dyn ContextCodec>,
        compactor: Arc<dyn ContextCompactor>,
    ) -> Self {
        Self {
            projection: None,
            source,
            content,
            material_cache: None,
            plans,
            codec,
            compactor,
            progress: Arc::new(NoopRunProgressCoordinator),
        }
    }

    #[must_use]
    pub fn with_run_progress_coordinator(
        mut self,
        progress: Arc<dyn RunProgressCoordinator>,
    ) -> Self {
        self.progress = progress;
        self
    }
}
