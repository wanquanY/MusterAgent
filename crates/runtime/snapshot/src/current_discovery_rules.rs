//! Current role ceilings, independent of the mutable or compiled tool catalog.
//! Exact key checks preserve version boundaries even for absent catalog entries.
use super::Role;

pub(super) fn allows(role: Role, key: &str, ceiling: bool) -> bool {
    let Some((logical, version)) = key.rsplit_once('@') else {
        return false;
    };
    if role == Role::Evaluation {
        return false;
    }
    let dialogue_family = role != Role::Generic;
    let interactive = matches!(role, Role::Dialogue | Role::Device);
    let device = matches!(role, Role::Device | Role::DeviceTask);
    let report = role == Role::Report;
    let task = matches!(role, Role::Task | Role::DeviceTask);

    if logical == "device.workspace.video.render" || device && super::video::retired(logical) {
        return false;
    }
    // A report may retain signed delegated authority that its model cannot use.
    // Apply this execution restriction before the shared capability ceiling.
    if report && !ceiling && !report_execution_allowed(logical, key) {
        return false;
    }
    match logical {
        "device.workspace.file.import" | "device.workspace.file.publish" => {
            role == Role::Device && version == "1"
        }
        "device.host.desktop.list" => device && version == "1",
        "audio.narration.create"
        | "audio.music.create"
        | "audio.effect.create"
        | "video.template.search"
        | "video.template.use"
        | "video.template.package" => dialogue_family && version == "1" && (interactive || ceiling),
        "video.scene.preview" | "video.scene.render" | "video.narrate" => {
            interactive || role == Role::Generic || ceiling || version != "1"
        }
        "artifacts.deliver" => dialogue_family && version == "2" && (!task || ceiling),
        "dialogue.publish" => dialogue_family && version == "1" && (!task || ceiling),
        "capability.request" => interactive || role == Role::Generic,
        "context.search" => role == Role::Generic || interactive && version == "1",
        "data.python.run" => !dialogue_family || version == "12",
        "files.inspect" | "files.list" | "files.read" => !dialogue_family || version == "6",
        "files.search" => !dialogue_family || version == "5",
        "interaction.request" => !dialogue_family,
        "interaction.question" => !dialogue_family || version == "2",
        "agents.create" => !dialogue_family || interactive && version == "4",
        "agents.self.inspect" => interactive && version == "1",
        "agents.self.configure" => interactive && version == "2",
        "environments.inspect" | "environments.list" => dialogue_family && version == "2",
        "goal.create" | "goal.update" => {
            if dialogue_family {
                version != "2" && (interactive || version != "3")
            } else {
                version != "3"
            }
        }
        "goal.change_status" => {
            if dialogue_family {
                version != "1" && (interactive || version != "2")
            } else {
                version != "2"
            }
        }
        "goal.get" => version != if dialogue_family { "2" } else { "3" },
        "goal.list" => version != if dialogue_family { "1" } else { "2" },
        "goal.draft.save"
        | "goal.sources.catalog"
        | "goal.sources.configure"
        | "goal.sources.list"
        | "goal.sources.resources" => interactive && version == "1",
        "agenda.list" | "agenda.changes.apply" => {
            dialogue_family && matches!(version, "1" | "2" | "3")
        }
        "agenda.get"
        | "agenda.entry.create"
        | "agenda.entry.update"
        | "agenda.entry.control"
        | "agenda.todo.create"
        | "agenda.todo.update"
        | "agenda.todo.control" => dialogue_family && matches!(version, "1" | "2"),
        "schedules.control" | "schedules.create" | "schedules.list" | "schedules.update" => {
            interactive && version == "1"
        }
        "sources.list"
        | "sources.read"
        | "sources.result"
        | "sources.search"
        | "sources.attachments.import"
        | "sources.attachments.list"
        | "owner.sources.list"
        | "owner.sources.read"
        | "owner.sources.result"
        | "owner.sources.search"
        | "owner.sources.attachments.import"
        | "owner.sources.attachments.list" => dialogue_family && version == "1",
        "tasks.amend" | "tasks.executions.list" => dialogue_family && version == "2",
        "tasks.control" | "tasks.inspect" | "tasks.list" => dialogue_family && version == "1",
        "tasks.feedback" => dialogue_family && version == "1" && (report || ceiling),
        "tasks.followup" => interactive && version == "1",
        "tasks.handoff" => dialogue_family && version == "1",
        "tasks.start" => dialogue_family && version == "3",
        "tasks.submit_result" => dialogue_family && version == "1" && (task || ceiling),
        _ if closed_family(logical) => false,
        // Discovery only narrows signed authority. Unknown host tools still need
        // their own signed entitlement, manifest and runtime admission checks.
        _ => true,
    }
}

fn closed_family(logical: &str) -> bool {
    [
        "agenda.",
        "tasks.",
        "schedules.",
        "goal.sources.",
        "goal.draft.",
        "agents.self.",
        "environments.",
        "owner.sources.",
        "sources.",
        "device.host.desktop.",
    ]
    .iter()
    .any(|prefix| logical.starts_with(prefix))
}

fn report_execution_allowed(logical: &str, key: &str) -> bool {
    if logical.starts_with("delegation.") || logical.starts_with("environments.") {
        return false;
    }
    if logical.starts_with("experience.") {
        return matches!(key, "experience.inspect@1" | "experience.search@1");
    }
    if logical.starts_with("memory.") {
        return matches!(key, "memory.get@1" | "memory.list@1" | "memory.search@1");
    }
    if logical.starts_with("agenda.") {
        return matches!(
            key,
            "agenda.list@1" | "agenda.list@2" | "agenda.list@3" | "agenda.get@1" | "agenda.get@2"
        );
    }
    logical != "user_memory.remember"
        && !matches!(
            key,
            "goal.get@3"
                | "goal.list@2"
                | "sources.attachments.import@1"
                | "tasks.handoff@1"
                | "tasks.start@3"
        )
}
