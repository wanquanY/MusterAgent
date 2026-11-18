use super::DeviceResourceError as Error;
use crate::DialogueExecutionDirectoryContract;

/// Only original, registered Tool contracts. No HTTP method, SQL command or
/// model-selected Resource operation can enter this enum.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DeviceResourceOperation {
    ExecutionsList(DialogueExecutionDirectoryContract),
    TaskStart,
    TaskList,
    TaskInspect,
    TaskControl,
    TaskAmend,
    TaskVerify,
    TaskVerifyResult,
    TaskSubmitResult,
    TaskHandoff,
    TaskFollowup,
    GoalCreate,
    GoalDraftSave,
    GoalGet,
    GoalList,
    GoalUpdate,
    GoalControl,
    GoalSourcesCatalog,
    GoalSourcesResources,
    GoalSourcesConfigure,
    GoalSourcesList,

    MemoryRemember,
    MemoryPropose,
    MemoryPublish,
    MemorySearch,
    MemoryList,
    MemoryGet,
    ExperienceSearch,
    ExperienceInspect,
    FileCreate,
    WorkspaceFileImport,
    FilesList,
    FilesSearch,
    FilesInspect,
    FilesRead,
    FileRead,
    EditBegin,
    EditAppend,
    EditPatch,
    EditResume,
    EditFinalize,
    EditAbort,
    Search,
    TreeList,
    FolderCreate,
    NodeRename,
    NodeMove,
    NodeTrash,
    NodeRestore,
    ChangeSetCommit,
}
impl DeviceResourceOperation {
    pub fn from_tool_key(key: &str) -> Result<Self, Error> {
        Ok(match key {
            "tasks.start@3" | "tasks.start@4" => Self::TaskStart,
            "tasks.list@1" => Self::TaskList,
            "tasks.inspect@1" => Self::TaskInspect,
            "tasks.control@1" => Self::TaskControl,
            "tasks.amend@2" => Self::TaskAmend,
            "tasks.verify@2" => Self::TaskVerify,
            "tasks.verify@3" => Self::TaskVerifyResult,
            "tasks.submit_result@1" => Self::TaskSubmitResult,
            "tasks.handoff@1" => Self::TaskHandoff,
            "tasks.followup@1" => Self::TaskFollowup,
            "goal.create@3" => Self::GoalCreate,
            "goal.draft.save@1" => Self::GoalDraftSave,
            "goal.get@3" => Self::GoalGet,
            "goal.list@2" => Self::GoalList,
            "goal.update@3" => Self::GoalUpdate,
            "goal.change_status@2" => Self::GoalControl,
            "goal.sources.catalog@1" => Self::GoalSourcesCatalog,
            "goal.sources.resources@1" => Self::GoalSourcesResources,
            "goal.sources.configure@1" => Self::GoalSourcesConfigure,
            "goal.sources.list@1" => Self::GoalSourcesList,

            "user_memory.remember@2" => Self::MemoryRemember,
            "experience.propose@2" | "experience.propose@3" => Self::MemoryPropose,
            "experience.publish@1" => Self::MemoryPublish,
            "memory.search@1" => Self::MemorySearch,
            "memory.list@1" => Self::MemoryList,
            "memory.get@1" => Self::MemoryGet,
            "experience.search@1" => Self::ExperienceSearch,
            "experience.inspect@1" => Self::ExperienceInspect,
            "tasks.executions.list@1" | "tasks.executions.list@2" => Self::ExecutionsList(
                DialogueExecutionDirectoryContract::from_tool_key(key).ok_or(Error::Forbidden)?,
            ),
            "device.workspace.file.import@1" => Self::WorkspaceFileImport,
            "resource.file.create@3" | "resource.file.create@4" => Self::FileCreate,
            "files.list@4" | "files.list@5" | "files.list@6" => Self::FilesList,
            "files.search@4" | "files.search@5" => Self::FilesSearch,
            "files.inspect@4" | "files.inspect@5" | "files.inspect@6" => Self::FilesInspect,
            "files.read@5" | "files.read@6" => Self::FilesRead,
            "resource.file.read@4" | "resource.file.read@5" => Self::FileRead,
            "resource.file.edit.begin@4" | "resource.file.edit.begin@5" => Self::EditBegin,
            "resource.file.edit.append@3" | "resource.file.edit.append@4" => Self::EditAppend,
            "resource.file.edit.patch@5" | "resource.file.edit.patch@6" => Self::EditPatch,
            "resource.file.edit.resume@4" | "resource.file.edit.resume@5" => Self::EditResume,
            "resource.file.edit.finalize@3" | "resource.file.edit.finalize@4" => Self::EditFinalize,
            "resource.file.edit.abort@3" | "resource.file.edit.abort@4" => Self::EditAbort,
            "resource.search@3" | "resource.search@4" => Self::Search,
            "resource.tree.list@3" | "resource.tree.list@4" => Self::TreeList,
            "resource.folder.create@4" | "resource.folder.create@5" => Self::FolderCreate,
            "resource.node.rename@4" | "resource.node.rename@5" => Self::NodeRename,
            "resource.node.move@3" | "resource.node.move@4" => Self::NodeMove,
            "resource.node.trash@3" | "resource.node.trash@4" => Self::NodeTrash,
            "resource.node.restore@3" | "resource.node.restore@4" => Self::NodeRestore,
            "resource.change-set.commit@5" | "resource.change-set.commit@6" => {
                Self::ChangeSetCommit
            }
            _ => return Err(Error::InvalidArguments),
        })
    }
    /// Stable wire discriminator for the operation, not the actual released
    /// Tool version. Authorization must use the immutable invocation Tool key.
    pub const fn tool_key(self) -> &'static str {
        match self {
            Self::TaskStart => "tasks.start@3",
            Self::TaskList => "tasks.list@1",
            Self::TaskInspect => "tasks.inspect@1",
            Self::TaskControl => "tasks.control@1",
            Self::TaskAmend => "tasks.amend@2",
            Self::TaskVerify => "tasks.verify@2",
            Self::TaskVerifyResult => "tasks.verify@3",
            Self::TaskSubmitResult => "tasks.submit_result@1",
            Self::TaskHandoff => "tasks.handoff@1",
            Self::TaskFollowup => "tasks.followup@1",
            Self::GoalCreate => "goal.create@3",
            Self::GoalDraftSave => "goal.draft.save@1",
            Self::GoalGet => "goal.get@3",
            Self::GoalList => "goal.list@2",
            Self::GoalUpdate => "goal.update@3",
            Self::GoalControl => "goal.change_status@2",
            Self::GoalSourcesCatalog => "goal.sources.catalog@1",
            Self::GoalSourcesResources => "goal.sources.resources@1",
            Self::GoalSourcesConfigure => "goal.sources.configure@1",
            Self::GoalSourcesList => "goal.sources.list@1",

            Self::MemoryRemember => "user_memory.remember@2",
            Self::MemoryPropose => "experience.propose@2",
            Self::MemoryPublish => "experience.publish@1",
            Self::MemorySearch => "memory.search@1",
            Self::MemoryList => "memory.list@1",
            Self::MemoryGet => "memory.get@1",
            Self::ExperienceSearch => "experience.search@1",
            Self::ExperienceInspect => "experience.inspect@1",
            Self::ExecutionsList(contract) => contract.tool_key(),
            Self::WorkspaceFileImport => "device.workspace.file.import@1",
            Self::FileCreate => "resource.file.create@3",
            Self::FilesList => "files.list@4",
            Self::FilesSearch => "files.search@4",
            Self::FilesInspect => "files.inspect@4",
            Self::FilesRead => "files.read@5",
            Self::FileRead => "resource.file.read@4",
            Self::EditBegin => "resource.file.edit.begin@4",
            Self::EditAppend => "resource.file.edit.append@3",
            Self::EditPatch => "resource.file.edit.patch@5",
            Self::EditResume => "resource.file.edit.resume@4",
            Self::EditFinalize => "resource.file.edit.finalize@3",
            Self::EditAbort => "resource.file.edit.abort@3",
            Self::Search => "resource.search@3",
            Self::TreeList => "resource.tree.list@3",
            Self::FolderCreate => "resource.folder.create@4",
            Self::NodeRename => "resource.node.rename@4",
            Self::NodeMove => "resource.node.move@3",
            Self::NodeTrash => "resource.node.trash@3",
            Self::NodeRestore => "resource.node.restore@3",
            Self::ChangeSetCommit => "resource.change-set.commit@5",
        }
    }
    pub const fn is_read_only(self) -> bool {
        matches!(
            self,
            Self::TaskList
                | Self::TaskInspect
                | Self::GoalGet
                | Self::GoalList
                | Self::GoalSourcesCatalog
                | Self::GoalSourcesResources
                | Self::GoalSourcesList
                | Self::MemorySearch
                | Self::MemoryList
                | Self::MemoryGet
                | Self::ExperienceSearch
                | Self::ExperienceInspect
                | Self::ExecutionsList(_)
                | Self::FilesList
                | Self::FilesSearch
                | Self::FilesInspect
                | Self::FilesRead
                | Self::FileRead
                | Self::EditResume
                | Self::Search
                | Self::TreeList
        )
    }
    /// Task controls still execute in the original WorkItem owner. This only
    /// selects the authenticated Native transport family, never authority.
    pub const fn is_task(self) -> bool {
        matches!(
            self,
            Self::TaskStart
                | Self::TaskList
                | Self::TaskInspect
                | Self::TaskControl
                | Self::TaskAmend
                | Self::TaskVerify
                | Self::TaskVerifyResult
                | Self::TaskSubmitResult
                | Self::TaskHandoff
                | Self::TaskFollowup
        )
    }
    pub const fn is_goal_source(self) -> bool {
        matches!(
            self,
            Self::GoalSourcesCatalog
                | Self::GoalSourcesResources
                | Self::GoalSourcesConfigure
                | Self::GoalSourcesList
        )
    }
    pub const fn is_goal(self) -> bool {
        matches!(
            self,
            Self::GoalCreate
                | Self::GoalDraftSave
                | Self::GoalGet
                | Self::GoalList
                | Self::GoalUpdate
                | Self::GoalControl
        ) || self.is_goal_source()
    }
    pub const fn is_memory(self) -> bool {
        matches!(
            self,
            Self::MemoryRemember
                | Self::MemoryPropose
                | Self::MemoryPublish
                | Self::MemorySearch
                | Self::MemoryList
                | Self::MemoryGet
                | Self::ExperienceSearch
                | Self::ExperienceInspect
        )
    }
    pub const fn has_memory_approval(self) -> bool {
        matches!(self, Self::MemoryRemember | Self::MemoryPublish)
    }
    pub const fn can_produce_versions(self) -> bool {
        matches!(
            self,
            Self::FileCreate | Self::EditFinalize | Self::ChangeSetCommit
        )
    }
}
