//! Explicit SSE transport controls. Never normalize a heartbeat as model output
//! or count it as canonical progress. Payload/event budgets still apply.
use serde_json::Value;

pub(super) fn classify(event: &str, data: &str) -> Result<bool, ()> {
    if event != "keepalive" {
        return Ok(false);
    }
    let data = data.trim();
    if data.is_empty() || data == "keepalive" {
        return Ok(true);
    }
    let value: Value = serde_json::from_str(data).map_err(|_| ())?;
    if value.is_null() {
        return Ok(true);
    }
    let fields = value.as_object().ok_or(())?;
    for (key, value) in fields {
        let valid = match key.as_str() {
            "type" | "event" => value.as_str() == Some("keepalive"),
            "timestamp" | "ts" | "time" => {
                value.is_number()
                    || value.as_str().is_some_and(|v| {
                        v.len() <= 64
                            && v.bytes()
                                .all(|b| b.is_ascii_digit() || b"-:.+TZ ".contains(&b))
                    })
            }
            "status" => value
                .as_str()
                .is_some_and(|v| matches!(v, "waiting" | "processing" | "alive" | "ok")),
            "keepalive" | "heartbeat" | "ping" => value.as_bool() == Some(true),
            _ => false,
        };
        if !valid {
            return Err(());
        }
    }
    Ok(true)
}
