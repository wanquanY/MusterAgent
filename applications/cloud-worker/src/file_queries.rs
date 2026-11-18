use musteragent_identity::CorrelationId;
use musteragent_postgres::{
    PostgresAgentFileQueryAuthority, PostgresAgentInputFileSource, PostgresAgentOutputFileSource,
    PostgresArtifactStore, PostgresConversationProjection, PostgresDatabaseRuntime,
    PostgresResourceContextAuthority, PostgresResourceStore, PostgresRuntimeApplicationStore,
};
use musteragent_projection::{SubscriptionError, SubscriptionErrorMapper};
use musteragent_runtime_capability::RuntimeClock;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::{ErrorCode, RuntimeError};
use musteragent_runtime_file_query::*;
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::{ProtobufConversationProjectionCodec, ProtobufSnapshotCodec};
use std::sync::Arc;

pub(crate) fn compose(
    database: &PostgresDatabaseRuntime,
    ledger: Arc<ProtobufLedgerPayloadCodec>,
    content: Arc<dyn ContentStore>,
    artifacts: Arc<PostgresArtifactStore>,
    clock: Arc<dyn RuntimeClock>,
) -> Arc<FileQueryService> {
    let projections = Arc::new(PostgresConversationProjection::new(
        database.clone(),
        ledger,
        Arc::new(ProtobufConversationProjectionCodec),
        content.clone(),
        Arc::new(Errors),
    ));
    Arc::new(FileQueryService::new(
        Arc::new(PostgresAgentFileQueryAuthority::new(
            database.pool().clone(),
            Arc::new(CurrentFileQueryAuthority::from_source(
                Arc::new(PostgresRuntimeApplicationStore::new(
                    database.clone(),
                    Arc::new(ProtobufSnapshotCodec),
                )),
                clock.clone(),
                musteragent_event_envelope::RuntimeHome::Cloud,
            )),
            clock,
        )),
        Arc::new(
            FileQueryApplication::new(
                Arc::new(
                    ConversationFileCatalog::new(projections)
                        .with_independent_artifacts(artifacts.clone())
                        .with_placements(Arc::new(PostgresResourceStore::new(database.clone()))),
                ),
                Arc::new(ManagedResourceFileCatalog::managed_project(
                    Arc::new(PostgresResourceContextAuthority::new(
                        database.pool().clone(),
                    )),
                    Arc::new(PostgresResourceStore::new(database.clone())),
                )),
                Arc::new(ContentFileReader::new(content.clone()).with_artifacts(artifacts)),
            )
            .with_dialogue_catalog(Arc::new(
                AgentFileCatalog::new(
                    Arc::new(ManagedResourceFileCatalog::for_owned_agent(
                        Arc::new(PostgresResourceContextAuthority::new(
                            database.pool().clone(),
                        )),
                        Arc::new(PostgresResourceStore::new(database.clone())),
                    )),
                    Arc::new(PostgresAgentInputFileSource::new(database.pool().clone())),
                )
                .with_outputs(Arc::new(PostgresAgentOutputFileSource::new(
                    database.clone(),
                ))),
            ))
            .with_work_catalog(Arc::new(ManagedResourceFileCatalog::execution_workspace(
                Arc::new(PostgresResourceContextAuthority::new(
                    database.pool().clone(),
                )),
                Arc::new(PostgresResourceStore::new(database.clone())),
            )))
            .with_content_store(content),
        ),
    ))
}
struct Errors;
impl SubscriptionErrorMapper for Errors {
    fn map(&self, error: SubscriptionError) -> RuntimeError {
        let code = match error {
            SubscriptionError::Unauthorized => ErrorCode::PolicyDenied,
            SubscriptionError::CorruptRecord => ErrorCode::ContentIntegrityFailed,
            SubscriptionError::InvalidQuery => ErrorCode::InvalidCommand,
            _ => ErrorCode::DependencyUnavailable,
        };
        musteragent_runtime_session::runtime_error(
            code,
            CorrelationId::generate(),
            false,
            "File source projection could not be read",
        )
    }
}
