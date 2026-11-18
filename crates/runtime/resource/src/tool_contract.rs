//! Closed wire-format compatibility for forward Agent resource contracts.
//! Storage ownership is resolved separately; this never changes a pinned schema.
#[must_use]
pub fn resource_tool_format_contract(key: &str) -> &str {
    match key {
        "resource.search@3" | "resource.search@4" => "resource.search@2",
        "resource.tree.list@3" | "resource.tree.list@4" => "resource.tree.list@2",
        "resource.node.move@3" | "resource.node.move@4" => "resource.node.move@2",
        "resource.node.trash@3" | "resource.node.trash@4" => "resource.node.trash@2",
        "resource.node.restore@3" | "resource.node.restore@4" => "resource.node.restore@2",
        "resource.file.create@3" | "resource.file.create@4" => "resource.file.create@2",
        "resource.file.read@4" | "resource.file.read@5" => "resource.file.read@3",
        "resource.file.edit.begin@4" | "resource.file.edit.begin@5" => "resource.file.edit.begin@3",
        "resource.file.edit.append@3" | "resource.file.edit.append@4" => {
            "resource.file.edit.append@2"
        }
        "resource.file.edit.finalize@3" | "resource.file.edit.finalize@4" => {
            "resource.file.edit.finalize@2"
        }
        "resource.file.edit.abort@3" | "resource.file.edit.abort@4" => "resource.file.edit.abort@2",
        "resource.file.edit.resume@4" | "resource.file.edit.resume@5" => {
            "resource.file.edit.resume@3"
        }
        "resource.folder.create@4" | "resource.folder.create@5" => "resource.folder.create@3",
        "resource.change-set.commit@5" | "resource.change-set.commit@6" => {
            "resource.change-set.commit@4"
        }
        "resource.file.edit.patch@5" | "resource.file.edit.patch@6" => "resource.file.edit.patch@4",
        "resource.node.rename@4" | "resource.node.rename@5" => "resource.node.rename@3",
        _ => key,
    }
}
#[must_use]
pub fn supports_agent_resource_tool(key: &str) -> bool {
    matches!(
        key,
        "resource.search@3"
            | "resource.search@4"
            | "resource.tree.list@3"
            | "resource.tree.list@4"
            | "resource.node.move@3"
            | "resource.node.move@4"
            | "resource.node.trash@3"
            | "resource.node.trash@4"
            | "resource.node.restore@3"
            | "resource.node.restore@4"
            | "resource.file.create@3"
            | "resource.file.create@4"
            | "resource.file.read@4"
            | "resource.file.read@5"
            | "resource.file.edit.begin@4"
            | "resource.file.edit.begin@5"
            | "resource.file.edit.append@3"
            | "resource.file.edit.append@4"
            | "resource.file.edit.finalize@3"
            | "resource.file.edit.finalize@4"
            | "resource.file.edit.abort@3"
            | "resource.file.edit.abort@4"
            | "resource.file.edit.resume@4"
            | "resource.file.edit.resume@5"
            | "resource.folder.create@4"
            | "resource.folder.create@5"
            | "resource.change-set.commit@5"
            | "resource.change-set.commit@6"
            | "resource.file.edit.patch@5"
            | "resource.file.edit.patch@6"
            | "resource.node.rename@4"
            | "resource.node.rename@5"
    )
}

#[cfg(test)]
mod device_receipt_release_tests {
    use super::*;
    #[test]
    fn forward_receipt_contracts_preserve_original_argument_owners() {
        for (original, forward) in [
            ("resource.file.read@4", "resource.file.read@5"),
            ("resource.file.edit.resume@4", "resource.file.edit.resume@5"),
            ("resource.search@3", "resource.search@4"),
            ("resource.tree.list@3", "resource.tree.list@4"),
            ("resource.file.create@3", "resource.file.create@4"),
            ("resource.file.edit.begin@4", "resource.file.edit.begin@5"),
            ("resource.file.edit.append@3", "resource.file.edit.append@4"),
            ("resource.file.edit.patch@5", "resource.file.edit.patch@6"),
            (
                "resource.file.edit.finalize@3",
                "resource.file.edit.finalize@4",
            ),
            ("resource.file.edit.abort@3", "resource.file.edit.abort@4"),
            ("resource.folder.create@4", "resource.folder.create@5"),
            ("resource.node.rename@4", "resource.node.rename@5"),
            ("resource.node.move@3", "resource.node.move@4"),
            ("resource.node.trash@3", "resource.node.trash@4"),
            ("resource.node.restore@3", "resource.node.restore@4"),
            (
                "resource.change-set.commit@5",
                "resource.change-set.commit@6",
            ),
        ] {
            assert_eq!(
                resource_tool_format_contract(original),
                resource_tool_format_contract(forward)
            );
            assert!(supports_agent_resource_tool(original));
            assert!(supports_agent_resource_tool(forward));
        }
        assert!(!supports_agent_resource_tool("resource.file.create@99"));
        assert_eq!(
            resource_tool_format_contract("resource.file.create@99"),
            "resource.file.create@99"
        );
    }
}
