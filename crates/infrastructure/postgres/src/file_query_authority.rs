use crate::PostgresRuntimeApplicationStore;
use musteragent_conversation::{ConversationView, GetConversation};
use musteragent_run::{GetRun, RunView};
use musteragent_runtime_contract::{ApplicationFuture, RuntimeError};
use musteragent_runtime_file_query::FileQueryStateSource;
use musteragent_runtime_session::RuntimeApplicationStore;

impl FileQueryStateSource for PostgresRuntimeApplicationStore {
    fn conversation(
        &self,
        query: GetConversation,
    ) -> ApplicationFuture<'_, Result<ConversationView, RuntimeError>> {
        RuntimeApplicationStore::get_conversation(self, query.actor, query.conversation_id)
    }
    fn run(&self, query: GetRun) -> ApplicationFuture<'_, Result<RunView, RuntimeError>> {
        RuntimeApplicationStore::get_run(self, query.actor, query.run_id)
    }
}
