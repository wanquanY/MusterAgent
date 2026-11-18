use musteragent_runtime_memory::MemoryApplicationError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PostgresMemoryEvolutionError {
    #[error("original dialogue input custody is temporarily unavailable")]
    SourceUnavailable,
    #[error("Memory management requires the owning user")]
    MemoryManagementUnauthorized,
    #[error(transparent)]
    Application(#[from] MemoryApplicationError),
    #[error(transparent)]
    MemoryContract(#[from] musteragent_memory::MemoryContractError),
    #[error(transparent)]
    EvolutionContract(#[from] musteragent_evolution::EvolutionContractError),
    #[error(transparent)]
    InstinctContract(#[from] musteragent_evolution::InstinctContractError),
    #[error("Memory content reference is not durable for this tenant")]
    ContentNotFound,
    #[error("Memory content reference differs from the durable Content Store")]
    ContentIntegrityMismatch,
    #[error("Memory identity or idempotency key conflicts")]
    MemoryConflict,
    #[error("Memory version is not the next immutable version")]
    MemoryVersionConflict,
    #[error("Memory is not accessible to this user")]
    MemoryNotFound,
    #[error("Memory Draft was not found")]
    DraftNotFound,
    #[error("Memory Draft baseline conflicts with the current published head")]
    DraftConflict,
    #[error("Memory Draft idempotency identity conflicts with durable state")]
    DraftIdempotencyConflict,
    #[error("Evolution identity conflicts with another proposal")]
    EvolutionConflict,
    #[error("Evolution proposal was not found")]
    EvolutionNotFound,
    #[error("Evolution proposal is not verified")]
    ProposalNotVerified,
    #[error("Evolution evaluation is absent or did not pass")]
    EvaluationNotPassing,
    #[error("Evolution approval is absent, denied or does not bind this Evaluation")]
    ApprovalNotSatisfied,
    #[error("numeric value exceeds its domain or PostgreSQL range")]
    NumericRange,
    #[error("Runtime-owned Memory/Evolution rows are corrupt")]
    CorruptDatabase,
    #[error("Memory search projection is fenced by a completed privacy erasure")]
    SearchProjectionErased,
    #[error("Memory search backfill claim is invalid")]
    InvalidBackfillClaim,
    #[error("Memory search backfill lease is stale")]
    StaleBackfillLease,
    #[error("Memory/Evolution publication payload encoding failed")]
    Json(#[from] serde_json::Error),
    #[error("PostgreSQL Memory/Evolution persistence failed")]
    Sql(#[from] sqlx::Error),
    #[error("governed PostgreSQL Memory/Evolution Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
}
