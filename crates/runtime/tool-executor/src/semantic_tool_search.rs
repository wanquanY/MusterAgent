//! Rebuildable multilingual ranking, bounded and shared across Runs. The frozen
//! authorized catalogue remains the sole source of identities and schemas.
use futures_util::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream};
use musteragent_content_ref::Sha256;
use musteragent_discovery::AuthorizedToolCatalog;
use musteragent_runtime_memory::{
    MemoryEmbeddingDeadline, MemoryEmbeddingError, MemoryEmbeddingModel, MemoryEmbeddingProvider,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{OnceCell, Semaphore};

type Scores = BTreeMap<String, u32>;
type IndexCell = Arc<OnceCell<Index>>;
const MAX_CATALOGUES: usize = 4;
const MAX_VECTOR_BYTES: usize = 32 * 1024 * 1024;

pub struct SemanticToolSearch {
    provider: Arc<dyn MemoryEmbeddingProvider>,
    indexes: Mutex<VecDeque<(String, IndexCell)>>,
    builders: Semaphore,
}
struct Index {
    keys: Vec<String>,
    vectors: Vec<Vec<f32>>,
    queries: Mutex<VecDeque<(String, Arc<Scores>)>>,
}
impl SemanticToolSearch {
    pub fn new(provider: Arc<dyn MemoryEmbeddingProvider>) -> Self {
        Self {
            provider,
            indexes: Mutex::new(VecDeque::new()),
            builders: Semaphore::new(2),
        }
    }

    pub async fn scores(
        &self,
        catalog: &AuthorizedToolCatalog,
        query: &str,
    ) -> Result<Arc<Scores>, MemoryEmbeddingError> {
        // A cold catalogue is a bounded background-sized batch, not a single
        // memory lookup. Reuse its completed index and query scores across Runs.
        let deadline = MemoryEmbeddingDeadline::after(Duration::from_secs(30));
        deadline
            .run(async {
                let model = self.provider.configured_model(deadline.catalog()).await?;
                model.validate()?;
                if catalog.definitions().len() > 2048
                    || query.len() > model.max_input_tokens.get() as usize
                {
                    return Err(MemoryEmbeddingError::InvalidResponse);
                }
                let key = Sha256::digest(format!(
                    "{}\n{}\n{}",
                    catalog.catalog_hash(),
                    model.model_key,
                    model.model_revision
                ))
                .to_string();
                let cell = {
                    let mut indexes = self
                        .indexes
                        .lock()
                        .map_err(|_| MemoryEmbeddingError::InvalidResponse)?;
                    if let Some(position) =
                        indexes.iter().position(|(candidate, _)| candidate == &key)
                    {
                        let entry = indexes
                            .remove(position)
                            .ok_or(MemoryEmbeddingError::InvalidResponse)?;
                        let cell = entry.1.clone();
                        indexes.push_back(entry);
                        cell
                    } else {
                        let cell = Arc::new(OnceCell::new());
                        if indexes.len() >= MAX_CATALOGUES {
                            indexes.pop_front();
                        }
                        indexes.push_back((key, cell.clone()));
                        cell
                    }
                };
                if let Some(index) = cell.get() {
                    let queries = index
                        .queries
                        .lock()
                        .map_err(|_| MemoryEmbeddingError::InvalidResponse)?;
                    if let Some((_, scores)) = queries.iter().find(|(key, _)| key == query) {
                        return Ok(scores.clone());
                    }
                }
                let (index, mut vectors) = tokio::try_join!(
                    cell.get_or_try_init(|| self.build(catalog, &model, deadline)),
                    self.embed(&model, vec![query.to_owned()], deadline),
                )?;
                let vector = vectors.pop().ok_or(MemoryEmbeddingError::InvalidResponse)?;
                let mut scores = BTreeMap::new();
                for (key, candidate) in index.keys.iter().zip(&index.vectors) {
                    if candidate.len() != vector.len() {
                        return Err(MemoryEmbeddingError::InvalidResponse);
                    }
                    let similarity: f32 = vector.iter().zip(candidate).map(|(a, b)| a * b).sum();
                    if similarity > 0.35 {
                        scores.insert(key.clone(), (similarity.clamp(0.0, 1.0) * 1000.0) as u32);
                    }
                }
                let scores = Arc::new(scores);
                let mut queries = index
                    .queries
                    .lock()
                    .map_err(|_| MemoryEmbeddingError::InvalidResponse)?;
                if queries.len() >= 16 {
                    queries.pop_front();
                }
                queries.push_back((query.to_owned(), scores.clone()));
                Ok(scores)
            })
            .await
    }

    async fn build(
        &self,
        catalog: &AuthorizedToolCatalog,
        model: &MemoryEmbeddingModel,
        deadline: MemoryEmbeddingDeadline,
    ) -> Result<Index, MemoryEmbeddingError> {
        let _permit = self
            .builders
            .acquire()
            .await
            .map_err(|_| MemoryEmbeddingError::InvalidResponse)?;
        let mut vectors = Vec::new();
        let mut bytes = 0;
        let mut dimensions = None;
        // Preserve catalogue order while overlapping bounded provider batches;
        // cold discovery must not serialize one network round trip per batch.
        let inputs: Vec<Vec<String>> = catalog
            .definitions()
            .chunks(32)
            .map(|tools| {
                tools
                    .iter()
                    .map(|tool| {
                        let mut text = format!(
                            "{}\n{}\n{}",
                            tool.wire_name.as_str(),
                            tool.title,
                            tool.description
                        );
                        let mut end = text
                            .len()
                            .min(1536)
                            .min(model.max_input_tokens.get() as usize);
                        while !text.is_char_boundary(end) {
                            end -= 1;
                        }
                        text.truncate(end);
                        text
                    })
                    .collect()
            })
            .collect();
        let mut batches = stream::iter(inputs)
            .map(|inputs| self.embed(model, inputs, deadline.request()).boxed())
            .buffered(4);
        while let Some(batch) = batches.try_next().await? {
            for vector in batch {
                if dimensions.is_some_and(|value| value != vector.len()) {
                    return Err(MemoryEmbeddingError::InvalidResponse);
                }
                dimensions = Some(vector.len());
                bytes += vector.len() * size_of::<f32>();
                if bytes > MAX_VECTOR_BYTES {
                    return Err(MemoryEmbeddingError::InvalidResponse);
                }
                vectors.push(vector);
            }
        }
        Ok(Index {
            keys: catalog
                .definitions()
                .iter()
                .map(|tool| tool.tool_ref.as_str().to_owned())
                .collect(),
            vectors,
            queries: Mutex::new(VecDeque::new()),
        })
    }

    async fn embed(
        &self,
        model: &MemoryEmbeddingModel,
        inputs: Vec<String>,
        deadline: MemoryEmbeddingDeadline,
    ) -> Result<Vec<Vec<f32>>, MemoryEmbeddingError> {
        let count = inputs.len();
        let batch = self.provider.embed(inputs, deadline).await?;
        if batch.model_key != model.model_key
            || batch.model_revision != model.model_revision
            || batch.vectors.len() != count
            || batch.dimensions.get() > 16000
        {
            return Err(MemoryEmbeddingError::InvalidResponse);
        }
        batch
            .vectors
            .into_iter()
            .map(|mut vector| {
                if vector.len() != batch.dimensions.get() as usize
                    || vector.iter().any(|value| !value.is_finite())
                {
                    return Err(MemoryEmbeddingError::InvalidResponse);
                }
                let norm = vector
                    .iter()
                    .map(|v| f64::from(*v).powi(2))
                    .sum::<f64>()
                    .sqrt();
                if !norm.is_finite() || norm <= f64::EPSILON {
                    return Err(MemoryEmbeddingError::InvalidResponse);
                }
                for value in &mut vector {
                    *value = (f64::from(*value) / norm) as f32;
                }
                Ok(vector)
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "semantic_tool_search_tests.rs"]
mod tests;
