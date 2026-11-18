//! Deterministic model protocol fixture: decisions depend on actual tool
//! results and delivered child reports, never database access or hidden state.
use super::*;
pub(super) fn response(request: &Value) -> Option<Vec<String>> {
    let messages = request["messages"].as_array()?;
    let users = messages
        .iter()
        .filter(|m| m["role"] == "user")
        .map(|m| flatten_text(&m["content"]))
        .collect::<Vec<_>>()
        .join("\n");
    let parent = users.contains("[e2e:parent]");
    let child = users.contains("[e2e:child]");
    let limit_parent = users.contains("[e2e:limit-parent]");
    let limit_child = users.contains("[e2e:limit-child]");
    let tree_parent = users.contains("[e2e:tree-parent]");
    let tree_child = users.contains("[e2e:tree-child]");
    let failed_parent = users.contains("[e2e:failure-parent]");
    if !parent
        && !child
        && !limit_parent
        && !limit_child
        && !tree_parent
        && !tree_child
        && !failed_parent
    {
        return None;
    }
    let called = |name: &str| {
        messages
            .iter()
            .flat_map(|m| m["tool_calls"].as_array().into_iter().flatten())
            .filter(|c| c["function"]["name"] == name)
            .count()
    };
    let tool_text = messages
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| flatten_text(&m["content"]))
        .collect::<Vec<_>>()
        .join("\n");
    let choose = |name: &str, args: Value| {
        if offered_tool(request, name) {
            tool_response(name, args)
        } else {
            tool_response("tool_catalog_activate", json!({"names":[name]}))
        }
    };
    if tree_parent || tree_child {
        if called("delegation_spawn") == 0 {
            return Some(choose(
                "delegation_spawn",
                json!({"task":if tree_parent {"[e2e:tree-child] Spawn a grandchild"}else{"[e2e:tree-grandchild] Wait for cancellation"}}),
            ));
        }
        return Some(text_response("Waiting for descendant"));
    }
    if failed_parent {
        if called("delegation_spawn") == 0 {
            return Some(choose(
                "delegation_spawn",
                json!({"task":"[e2e:failure-child] Exercise a terminal provider failure"}),
            ));
        }
        return Some(if users.contains("\"status\":\"failed\"") {
            text_response("Parent observed child failure and reported it")
        } else {
            text_response("Waiting for child outcome")
        });
    }
    if limit_parent {
        if called("delegation_spawn") < 2 {
            return Some(choose(
                "delegation_spawn",
                json!({"task":"[e2e:limit-child] Try to spawn a grandchild and report the typed depth denial."}),
            ));
        }
        if !tool_text.contains("delegation.concurrency_limit") {
            return Some(text_response("Expected typed concurrency denial missing"));
        }
        return Some(if users.contains("child_depth_limit_observed") {
            text_response(
                "Parent observed delegation.concurrency_limit and child delegation.depth_limit",
            )
        } else {
            text_response("Waiting for depth report")
        });
    }
    if limit_child {
        if called("delegation_spawn") == 0 {
            return Some(choose(
                "delegation_spawn",
                json!({"task":"Grandchild must be denied by the depth ceiling"}),
            ));
        }
        return Some(if tool_text.contains("delegation.depth_limit") {
            text_response("child_depth_limit_observed")
        } else {
            text_response("Expected typed depth denial missing")
        });
    }
    if parent {
        if called("delegation_spawn") == 0 {
            return Some(choose(
                "delegation_spawn",
                json!({"task":"[e2e:child] Read the time, receive the parent message, acknowledge it, and return result 42."}),
            ));
        }
        if called("delegation_send") == 0 {
            let handle = messages
                .iter()
                .filter(|m| m["role"] == "tool")
                .filter_map(|m| serde_json::from_str::<Value>(&flatten_text(&m["content"])).ok())
                .find_map(|v| v["handle"].as_str().map(str::to_owned));
            return Some(match handle {
                Some(handle) => choose(
                    "delegation_send",
                    json!({"handle":handle,"message":"parent_hello"}),
                ),
                None => text_response("Missing real spawn receipt"),
            });
        }
        if users.contains("child_result_42") {
            if called("delegation_list") == 0 {
                return Some(choose("delegation_list", json!({})));
            }
            if called("delegate_inspect") == 0 {
                let handle = messages
                    .iter()
                    .filter(|m| m["role"] == "tool")
                    .filter_map(|m| {
                        serde_json::from_str::<Value>(&flatten_text(&m["content"])).ok()
                    })
                    .find_map(|v| v["handle"].as_str().map(str::to_owned));
                return Some(match handle {
                    Some(handle) => choose(
                        "delegate_inspect",
                        json!({"view":"timeline","handle":handle}),
                    ),
                    None => text_response("Missing spawn identity for inspect"),
                });
            }
            if called("delegation_receive") == 0 {
                return Some(choose("delegation_receive", json!({})));
            }
            return Some(if tool_text.contains("child_ack_parent_hello") {
                text_response("Parent used child result: 42")
            } else {
                text_response("Missing child acknowledgement")
            });
        }
        return Some(text_response("Waiting for child result."));
    }
    if called("get_current_time") == 0 {
        return Some(choose("get_current_time", json!({"timezone":"UTC"})));
    }
    if called("delegation_receive") == 0 {
        return Some(choose("delegation_receive", json!({})));
    }
    if !tool_text.contains("parent_hello") {
        return Some(text_response("Missing parent message"));
    }
    if called("delegation_send") == 0 {
        return Some(choose(
            "delegation_send",
            json!({"handle":"parent","message":"child_ack_parent_hello"}),
        ));
    }
    Some(text_response("child_result_42"))
}
