//! Privacy-erasure facts and consumer-owned storage ports.
//!
//! This crate owns request admission, immutable plan snapshots, append-only
//! state transitions and terminal receipts. Data owners implement erasure
//! providers; this crate never stores provider locators, keys or plaintext.

#![forbid(unsafe_code)]

mod agent_memory;
mod completion;
mod contract;
mod executor;
pub use completion::CompleteErasureExecution;
mod fingerprint;
mod port;
mod recovery;
mod source_evidence;
mod work_item;
mod work_item_execution;
pub use recovery::*;
pub use source_evidence::*;
pub use work_item::*;
pub use work_item_execution::*;

pub use agent_memory::{
    AGENT_MEMORY_BACKUP_RETENTION_POLICY, AGENT_MEMORY_ERASURE_SCHEMA,
    AGENT_MEMORY_IMMEDIATE_ERASURE_DATA_CLASSES, AGENT_MEMORY_OBJECT_HISTORY_RETENTION_POLICY,
    AGENT_MEMORY_RETENTION_POLICY, AgentMemoryErasureAdmission, AgentMemoryErasureAdmissionStore,
    AgentMemoryErasureJobPage, AgentMemoryErasureLineage, AgentMemoryErasureQueryStore,
    ListAgentMemoryErasureJobs, ManagedAgentMemoryErasureJob, RequestAgentMemoryErasure,
    build_agent_memory_erasure_facts,
};

pub use contract::{
    ErasureContractError, ErasureDataClass, ErasureJob, ErasureJobProjection, ErasureJobStatus,
    ErasurePlanSnapshot, ErasurePlanVersion, ErasureReceipt, ErasureReceiptOutcome, ErasureRequest,
    ErasureRetentionException, ErasureTransition, ErasureTransitionSequence,
};
pub use executor::{
    AgentMemoryErasureExecutionError, AgentMemoryErasureExecutor, ErasureExecutor,
    erasure_proof_digest,
};
pub use port::{
    ErasureAppendDisposition, ErasureJobCursor, ErasureJobPage, ErasureJobQuery, ErasureJobStore,
    ErasureMutationReceipt, ErasureQueryLimit, ErasureQueryLimitError, MAX_ERASURE_QUERY_PAGE_SIZE,
};
