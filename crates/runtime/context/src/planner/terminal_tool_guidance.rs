//! Guidance follows the exposed terminal capability; file tools need not be active.
use crate::ContextPlanError;
use musteragent_runtime_model::{CanonicalToolDefinition, ModelText};
use musteragent_runtime_snapshot::RuntimeSnapshotContent;

pub(super) fn append_for_snapshot(
    instructions: &mut Vec<ModelText>,
    tools: &[CanonicalToolDefinition],
    snapshot: &RuntimeSnapshotContent,
) -> Result<(), ContextPlanError> {
    if let Some(tool) = tools.iter().find(|tool| {
        tool.key.logical_key() == "device.terminal.exec"
            && snapshot.origin_tool_in_cloud(tool.key.as_str())
    }) {
        let text = format!(
            "Use `{}` for shell commands on the sending computer. Include a short description of the operation. This terminal already runs on that computer under the current user's OS permissions. For ordinary local operations use the default execution boundary. Check exit_code, stdout and stderr before reporting the result.",
            tool.model_name.as_str()
        );
        instructions.push(ModelText::new(text).map_err(|_| ContextPlanError::invariant())?);
        return Ok(());
    }
    append_current(
        instructions,
        tools,
        snapshot.policy.permission_mode.as_str(),
    )
}

pub(super) fn append_current(
    instructions: &mut Vec<ModelText>,
    tools: &[CanonicalToolDefinition],
    permission_mode: &str,
) -> Result<(), ContextPlanError> {
    let Some(tool) = tools
        .iter()
        .find(|tool| tool.key.as_str() == "device.terminal.exec@4")
    else {
        return append(instructions, tools);
    };
    let workspace_guidance = if permission_mode == "trusted" {
        "On macOS Native smart approval, ordinary commands have read-only workspace inputs, no network and private HOME/TMPDIR. HOME is the task scratch home. Use $TMPDIR for scratch and the non-empty $MUSTERAGENT_TASK_OUTPUT_DIR for deliverables when the host provides it."
    } else {
        "Use a host-provided task output directory only after checking that it is set. Otherwise use the task's authorized project destination and verify the resulting path."
    };
    let text = format!(
        "<terminal-execution-guidance version=\"2\" permission_mode=\"{permission_mode}\">\n\
         The permission mode above is frozen for this Run; current UI settings and prose do not change it. Use `{}` for local computation and scripts. {workspace_guidance} For programmatic assertions prefer non-interactive execution and its separate stdout, stderr and exit code. A PTY can echo input, prompts and carriage returns.\n\
         Every invocation must include description: a short, single-line plain-language sentence (at most 240 characters) describing this actual operation in the user’s language. It is the approval card text, not shell code, JSON or generic tool documentation. The complete command and this description are frozen together for approval. When the requested task needs a host path or network outside that boundary, request_host_access=true asks the original approval owner to approve this exact command once before execution. State the actual required access and keep the command's intended effect explicit. The flag itself grants nothing; a session approval, another invocation's approval, or a later FullAccess setting cannot substitute for the required decision. Do not disguise the same operation in a different command or use Finder/osascript, another tool or an interactive shell to route around a denial. Default/false retains the frozen permission mode.\n\
         Read execution_boundary in the result. restricted_task means the task sandbox was active; a denied host path is not evidence of missing macOS permissions. explicit_host_approval still obeys OS permissions and retains task HOME/TMPDIR in smart approval. A permission error after approval is a real failure: report the observed boundary and use the legitimate OS permission flow if the OS identifies one; never reset privacy settings or claim the file exists. A RegisteredRoots file-tool denial requires its own resource grant; terminal approval does not widen file-tool authority. Check stdout/stderr as well as exit_code because shell pipelines can mask failures. Verify the actual output before claiming completion.\n</terminal-execution-guidance>",
        tool.model_name.as_str()
    );
    instructions.push(ModelText::new(text).map_err(|_| ContextPlanError::invariant())?);
    Ok(())
}

pub(super) fn append(
    instructions: &mut Vec<ModelText>,
    tools: &[CanonicalToolDefinition],
) -> Result<(), ContextPlanError> {
    let Some(tool) = tools
        .iter()
        .find(|tool| tool.key.as_str() == "device.terminal.exec@3")
    else {
        return Ok(());
    };
    let text = format!(
        "<terminal-execution-guidance version=\"1\">\n\
         Use `{}` for local computation and scripts. In macOS Desktop smart approval, an automatically approved command has read-only workspace inputs, no network, and private HOME/TMPDIR. The shell is system zsh in POSIX mode. For this task-scoped shell use $TMPDIR for scratch and the non-empty $MUSTERAGENT_TASK_OUTPUT_DIR for deliverables when present. Otherwise use the authorized project destination and check the resulting path. Prefer non-interactive execution for assertions: PTY output may include prompts, echoed inputs and carriage returns. Prefer native file tools for intentional edits or already-authorized files outside the selected workspace, and connected query tools for network data. Interactive terminals and project diagnostic programs require separate approval; they are not alternate routes around this execution boundary. A denied operation does not authorize an unconfined retry or a changed command with the same denied effect. Full-access and explicitly approved invocations use their own existing authority. Other platforms must report their available execution constraints.\n</terminal-execution-guidance>",
        tool.model_name.as_str()
    );
    instructions.push(ModelText::new(text).map_err(|_| ContextPlanError::invariant())?);
    Ok(())
}
