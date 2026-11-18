//! Scripted schedule management using only model-visible tool receipts.
use super::*;
pub(super) fn response(request: &Value) -> Option<Vec<String>> {
    let messages = request["messages"].as_array()?;
    if !messages
        .iter()
        .any(|m| m["role"] == "user" && flatten_text(&m["content"]).contains("[e2e:schedule-crud]"))
    {
        return None;
    }
    let calls: Vec<&str> = messages
        .iter()
        .flat_map(|m| m["tool_calls"].as_array().into_iter().flatten())
        .filter_map(|c| c["function"]["name"].as_str())
        .collect();
    let choose = |name: &str, args: Value| {
        if offered_tool(request, name) {
            tool_response(name, args)
        } else {
            tool_response("tool_catalog_activate", json!({"names":[name]}))
        }
    };
    let mut definition = json!({"title":"Managed schedule","instructions":"[e2e:time] Read the current time.","timing":{"Interval":{"every_millis":86400000,"anchor":0}},"timezone":"UTC","concurrency":"Skip","lateness":"CatchUp","lateness_window_millis":1000,"retry":{"max_attempts":2,"base_backoff_millis":100,"factor":2,"max_backoff_millis":500}});
    if !calls.contains(&"schedules_create") {
        return Some(choose(
            "schedules_create",
            json!({"expected_revision":0,"action":{"Create":{"definition":definition,"owner":"Cloud"}}}),
        ));
    }
    let schedule = messages
        .iter()
        .rev()
        .filter(|m| m["role"] == "tool")
        .filter_map(|m| serde_json::from_str::<Value>(&flatten_text(&m["content"])).ok())
        .find_map(|v| v.get("schedule").cloned())?;
    let id = &schedule["schedule_id"];
    let revision = &schedule["revision"];
    if !calls.contains(&"schedules_update") {
        definition["title"] = json!("Updated through tool");
        return Some(choose(
            "schedules_update",
            json!({"schedule_id":id,"expected_revision":revision,"action":{"Update":{"definition":definition}}}),
        ));
    }
    if !calls.contains(&"schedules_control") {
        return Some(choose(
            "schedules_control",
            json!({"schedule_id":id,"expected_revision":revision,"action":"RunNow"}),
        ));
    }
    if !calls.contains(&"schedules_list") {
        return Some(choose("schedules_list", json!({})));
    }
    if !calls.contains(&"schedules_history") {
        return Some(choose(
            "schedules_history",
            json!({"schedule_id":id,"limit":50}),
        ));
    }
    if calls.iter().filter(|&&c| c == "schedules_control").count() == 1 {
        return Some(choose(
            "schedules_control",
            json!({"schedule_id":id,"expected_revision":revision,"action":"Delete"}),
        ));
    }
    Some(text_response(
        "Created, updated, ran, listed, inspected history and deleted the schedule.",
    ))
}
