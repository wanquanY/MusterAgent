//! Versioned operations implemented by the native workspace executor.
pub fn supports_workspace_tool(logical_key: &str, version: u32) -> bool {
    matches!(
        (logical_key, version),
        (
            "device.workspace.file.find"
                | "device.workspace.file.list"
                | "device.workspace.file.search",
            1..=3
        ) | ("device.workspace.code.diagnostics", 1..=2)
            | ("device.terminal.exec", 3..=4)
            | (
                "device.workspace.file.read" | "device.workspace.file.edit",
                1..=5
            )
            | (
                "device.workspace.file.write"
                    | "device.workspace.file.patch"
                    | "device.workspace.file.diff",
                1..=4
            )
    )
}

/// Operations a Cloud-coordinated Run may route to its signing Desktop. Beyond
/// the workspace executor, the Desktop can publish a finished local file: the
/// Device locates it, the Desktop uploads it, the Cloud captures the result.
/// It can also import an exact Cloud source the Desktop downloads.
/// Device-homed and hosted runs keep their own publication owner.
pub fn supports_origin_tool(logical_key: &str, version: u32) -> bool {
    supports_workspace_tool(logical_key, version)
        || matches!(
            (logical_key, version),
            (
                "device.workspace.file.publish" | "device.workspace.file.import",
                1
            )
        )
}
