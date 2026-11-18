//! Queries carry only structural task facts and original Content references.
//! Legacy Task queries retain Content references. The separate execution
//! directory returns bounded source excerpts under current dialogue authority.
use super::*;
use crate::{
    DialogueExecutionDirectoryContract, DialogueExecutionDirectoryPage,
    DialogueExecutionDirectoryQuery, dialogue_execution_directory_value,
};
use crate::{WorkItemDirectoryCursor, WorkItemDirectoryPage};
use serde::Deserialize;

#[derive(Debug, Clone, Copy)]
pub enum WorkItemReadQuery {
    Executions {
        query: DialogueExecutionDirectoryQuery,
        contract: DialogueExecutionDirectoryContract,
    },
    List {
        cursor: Option<WorkItemDirectoryCursor>,
        limit: u16,
    },
    Inspect {
        work_item_id: WorkItemId,
    },
}
#[derive(Debug, Clone)]
pub enum WorkItemReadResult {
    Executions(DialogueExecutionDirectoryPage),
    List(WorkItemDirectoryPage),
    Inspect(WorkItemAuthorityRecord),
}

pub fn parse_work_item_query(
    tool: &str,
    bytes: &[u8],
) -> Result<WorkItemReadQuery, DialogueStoreError> {
    if bytes.len() > 4096 {
        return Err(DialogueStoreError::Corrupt);
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Cursor {
        through: u64,
        after: u64,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct List {
        cursor: Option<Cursor>,
        #[serde(default = "default_limit")]
        limit: u16,
    }
    fn default_limit() -> u16 {
        25
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Inspect {
        task_id: uuid::Uuid,
    }
    match tool {
        "tasks.executions.list@1" | "tasks.executions.list@2" => {
            Ok(WorkItemReadQuery::Executions {
                query: DialogueExecutionDirectoryQuery::parse(bytes)?,
                contract: DialogueExecutionDirectoryContract::from_tool_key(tool)
                    .ok_or(DialogueStoreError::Unavailable)?,
            })
        }
        "tasks.list@1" => {
            let request: List =
                serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
            if request.limit == 0 || request.limit > 100 {
                return Err(DialogueStoreError::Corrupt);
            }
            Ok(WorkItemReadQuery::List {
                cursor: request.cursor.map(|c| WorkItemDirectoryCursor {
                    through: c.through,
                    after: c.after,
                }),
                limit: request.limit,
            })
        }
        "tasks.inspect@1" => {
            let request: Inspect =
                serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
            Ok(WorkItemReadQuery::Inspect {
                work_item_id: WorkItemId::from_uuid(request.task_id)
                    .map_err(|_| DialogueStoreError::Corrupt)?,
            })
        }
        _ => Err(DialogueStoreError::Unavailable),
    }
}
impl WorkItemReadQuery {
    pub fn tool_key(&self) -> &'static str {
        match self {
            Self::Executions { contract, .. } => contract.tool_key(),
            Self::List { .. } => "tasks.list@1",
            Self::Inspect { .. } => "tasks.inspect@1",
        }
    }
}
pub fn work_item_query_result_bytes(result: &WorkItemReadResult) -> Vec<u8> {
    use serde_json::json;
    let record = |r: &WorkItemAuthorityRecord| {
        use musteragent_work_item::WorkItemStatus::*;
        let status = match r.core.status {
            Queued => "queued",
            Ready => "ready",
            Active => "active",
            Blocked => "blocked",
            Verifying => "verifying",
            Completed => "completed",
            Superseded => "superseded",
            Cancelled => "cancelled",
        };
        let goal = &r.goal_content_ref;
        json!({"task_id":r.core.work_item_id.to_string(),"status":status,"goal_revision":r.core.revisions.goal.get(),"control_revision":r.core.revisions.control.get(),"state_revision":r.core.revisions.state.get(),"assignment_epoch":r.core.assignment_epoch.map(|value|value.get()),"dependencies":r.core.spec.dependencies.iter().map(ToString::to_string).collect::<Vec<_>>(),"goal_ref":{"content_id":goal.content_id.to_string(),"media_type":goal.media_type.as_str(),"byte_length":goal.byte_length,"sha256":goal.sha256.to_string()},"updated_at_ms":r.updated_at.unix_millis()})
    };
    match result {
        WorkItemReadResult::Executions(page) => dialogue_execution_directory_value(page),
        WorkItemReadResult::Inspect(value)=>json!({"task":record(value)}),
        WorkItemReadResult::List(page)=>json!({"tasks":page.records.iter().map(record).collect::<Vec<_>>(),"total_count":page.total_count,"through":page.through,"next_cursor":page.next.map(|cursor|json!({"through":cursor.through,"after":cursor.after}))}),
    }.to_string().into_bytes()
}
