//! File search uses the same operations-managed embedding route as Memory;
//! file indexing and source/version authorization remain Resource-owned.
use musteragent_postgres::{
    PostgresDatabaseRuntime, PostgresResourceEmbeddings, ResourceEmbeddingModel,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_memory::{MemoryEmbeddingDeadline, MemoryEmbeddingProvider};
use musteragent_runtime_resource::{
    ResourceSearchHit, ResourceSemanticSearch, ResourceStoreError, SearchResourceFiles,
};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct ResourceEmbeddings {
    store: PostgresResourceEmbeddings,
    provider: Arc<dyn MemoryEmbeddingProvider>,
}
impl ResourceEmbeddings {
    pub fn new(
        database: &PostgresDatabaseRuntime,
        provider: Arc<dyn MemoryEmbeddingProvider>,
    ) -> Self {
        Self {
            store: PostgresResourceEmbeddings::new(database.clone()),
            provider,
        }
    }
    async fn model(
        &self,
        deadline: MemoryEmbeddingDeadline,
    ) -> Result<ResourceEmbeddingModel, ResourceStoreError> {
        let configured = self
            .provider
            .configured_model(deadline)
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        configured
            .validate()
            .map_err(|_| ResourceStoreError::Corrupt)?;
        Ok(ResourceEmbeddingModel {
            key: configured.model_key,
            revision: configured.model_revision,
            max_input_bytes: configured.max_input_tokens.get() as usize,
        })
    }
    async fn vector(
        &self,
        model: &ResourceEmbeddingModel,
        text: &str,
        deadline: MemoryEmbeddingDeadline,
    ) -> Result<Vec<f32>, ResourceStoreError> {
        let inputs = split_text(text, model.max_input_bytes)?;
        let batch = self
            .provider
            .embed(inputs.clone(), deadline)
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        // A live host route change cannot mix vectors from two models.
        if batch.model_key != model.key
            || batch.model_revision != model.revision
            || batch.vectors.len() != inputs.len()
        {
            return Err(ResourceStoreError::DependencyUnavailable);
        }
        mean_unit_vector(&batch.vectors, batch.dimensions.get() as usize)
    }
    pub async fn run_once(&self, now: i64) -> Result<bool, ResourceStoreError> {
        if !self.store.available().await? {
            return Err(ResourceStoreError::DependencyUnavailable);
        }
        let deadline = MemoryEmbeddingDeadline::after(Duration::from_secs(120));
        let model = tokio::time::timeout(Duration::from_secs(10), self.model(deadline))
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)??;
        let Some(claim) = self.store.claim(&model, now, 150_000).await? else {
            return Ok(false);
        };
        let remaining = deadline
            .remaining()
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        let result =
            tokio::time::timeout(remaining, self.vector(&model, &claim.text, deadline)).await;
        let settled_at = musteragent_runtime_capability::RuntimeClock::now(
            &crate::composition::SystemRuntimeClock,
        )
        .unix_millis();
        match result {
            Ok(Ok(vector)) => {
                self.store
                    .complete(&model, &claim, &vector, settled_at)
                    .await?
            }
            _ => {
                self.store.fail(&claim, settled_at).await?;
                // A provider outage also pauses this worker, instead of rapidly
                // sending every queued chunk to the same unavailable endpoint.
                return Err(ResourceStoreError::DependencyUnavailable);
            }
        }
        Ok(true)
    }
}
impl ResourceSemanticSearch for ResourceEmbeddings {
    fn search<'a>(
        &'a self,
        query: &'a SearchResourceFiles,
    ) -> ApplicationFuture<'a, Result<Option<Vec<ResourceSearchHit>>, ResourceStoreError>> {
        Box::pin(async move {
            if !self.store.available().await? {
                return Err(ResourceStoreError::DependencyUnavailable);
            }
            let deadline = MemoryEmbeddingDeadline::retrieval();
            let work = async {
                let model = self.model(deadline).await?;
                if !self.store.covered(&model, query).await? {
                    return Ok(None);
                }
                let vector = self.vector(&model, &query.query, deadline).await?;
                self.store.search(&model, query, &vector).await
            };
            match tokio::time::timeout(
                deadline
                    .remaining()
                    .map_err(|_| ResourceStoreError::DependencyUnavailable)?,
                work,
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(ResourceStoreError::DependencyUnavailable),
            }
        })
    }
}
fn split_text(text: &str, maximum: usize) -> Result<Vec<String>, ResourceStoreError> {
    if text.trim().is_empty() || maximum < 4 {
        return Err(ResourceStoreError::Corrupt);
    }
    let mut result = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let mut end = rest.len().min(maximum);
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        let part = &rest[..end];
        if !part.trim().is_empty() {
            result.push(part.to_owned());
        }
        rest = &rest[end..];
        if result.len() > 32 {
            return Err(ResourceStoreError::Corrupt);
        }
    }
    Ok(result)
}
fn mean_unit_vector(
    vectors: &[Vec<f32>],
    dimensions: usize,
) -> Result<Vec<f32>, ResourceStoreError> {
    if vectors.is_empty() || dimensions == 0 || dimensions > 16000 {
        return Err(ResourceStoreError::Corrupt);
    }
    let mut mean = vec![0_f64; dimensions];
    for vector in vectors {
        if vector.len() != dimensions || vector.iter().any(|v| !v.is_finite()) {
            return Err(ResourceStoreError::Corrupt);
        }
        let norm = vector
            .iter()
            .map(|v| f64::from(*v).powi(2))
            .sum::<f64>()
            .sqrt();
        if norm == 0. {
            return Err(ResourceStoreError::Corrupt);
        }
        for (sum, value) in mean.iter_mut().zip(vector) {
            *sum += f64::from(*value) / norm;
        }
    }
    let norm = mean.iter().map(|v| v * v).sum::<f64>().sqrt();
    if norm == 0. || !norm.is_finite() {
        return Err(ResourceStoreError::Corrupt);
    }
    Ok(mean.into_iter().map(|v| (v / norm) as f32).collect())
}
/// Owned indexing task; cancellation leaves an unfinished claim recoverable by lease.
pub struct ResourceEmbeddingWorker(ResourceEmbeddings);
impl ResourceEmbeddingWorker {
    pub fn new(
        database: &PostgresDatabaseRuntime,
        provider: Arc<dyn MemoryEmbeddingProvider>,
    ) -> Self {
        Self(ResourceEmbeddings::new(database, provider))
    }
    pub async fn run(self, stop: tokio_util::sync::CancellationToken) {
        loop {
            let now = musteragent_runtime_capability::RuntimeClock::now(&crate::SystemRuntimeClock)
                .unix_millis();
            tokio::select! {
                _ = stop.cancelled() => return,
                result = self.0.run_once(now) => { if let Err(error) = result { tracing::warn!(?error, "resource embedding projection failed; claim remains recoverable"); } }
            }
            tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn multilingual_inputs_obey_provider_budget_and_vectors_are_normalized() {
        let text = "你好世界 welcome";
        let parts = split_text(text, 7).expect("bounded multilingual input");
        assert_eq!(parts.concat(), text);
        assert!(parts.iter().all(|part| part.len() <= 7));
        let vector =
            mean_unit_vector(&[vec![3., 0.], vec![0., 4.]], 2).expect("finite source vectors");
        assert!((vector[0] - 0.70710677).abs() < 0.00001);
        assert!(mean_unit_vector(&[vec![f32::NAN]], 1).is_err());
        assert!(mean_unit_vector(&[vec![0.]], 1).is_err());
    }
}
