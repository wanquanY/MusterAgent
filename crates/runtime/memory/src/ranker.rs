//! Private computation over bounded, authorized facts. The package may rank
//! candidates; only the Host can associate positions with original identities.
use std::sync::Arc;

pub use musteragent_memory_business::ranking::{RankedSelection, RankingCandidate};
use musteragent_runtime_capability::{CapabilityInvocation, ToolExecutionMaterial};
use musteragent_runtime_contract::ApplicationFuture;

use crate::{MemoryRetrievalError, MemoryToolError};

pub trait MemoryRanker: Send + Sync {
    fn rank<'a>(
        &'a self,
        query: Option<&'a str>,
        candidates: &'a [RankingCandidate],
        limit: u16,
    ) -> ApplicationFuture<'a, Result<Vec<RankedSelection>, MemoryRetrievalError>>;
}

pub trait MemoryRankerFactory: Send + Sync {
    fn bind(
        &self,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<Arc<dyn MemoryRanker>, MemoryToolError>;
}

pub(crate) struct CompiledMemoryRanker;

impl MemoryRanker for CompiledMemoryRanker {
    fn rank<'a>(
        &'a self,
        query: Option<&'a str>,
        candidates: &'a [RankingCandidate],
        limit: u16,
    ) -> ApplicationFuture<'a, Result<Vec<RankedSelection>, MemoryRetrievalError>> {
        Box::pin(async move {
            musteragent_memory_business::ranking::rank(query, candidates, limit)
                .map_err(|_| MemoryRetrievalError::InvalidSearchDocument)
        })
    }
}
