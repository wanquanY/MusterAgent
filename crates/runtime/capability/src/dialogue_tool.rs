//! Trusted invocation identity shared by account-owned tool adapters. This is
//! assembled from sealed Runtime material, never from model JSON arguments.
use musteragent_content_ref::{ContentRef, SchemaRef};
use musteragent_identity::{Actor, AgentId, InvocationId, ToolCallId, UtcTimestamp};
use musteragent_run::RunContext;

#[derive(Debug, Clone)]
pub struct DialogueToolResult {
    pub result_ref: ContentRef,
    pub result_schema: SchemaRef,
    /// Exact references resolved by the account owner, committed through the
    /// original Tool outcome. Returned JSON alone never authorizes Content.
    pub authorized_content: Vec<ContentRef>,
}
#[derive(Clone)]
pub struct DialogueToolInvocation {
    pub actor: Actor,
    pub agent_id: AgentId,
    pub run_context: RunContext,
    pub invocation_id: InvocationId,
    pub tool_call_id: ToolCallId,
    pub tool_key: String,
    pub request_ref: ContentRef,
    pub result_schema: SchemaRef,
    pub requested_at: UtcTimestamp,
}
