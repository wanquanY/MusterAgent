use serde_json::Value;

#[derive(Debug)]
pub struct MappedRequest {
    pub body: Value,
    pub accept: &'static str,
    pub content_type: &'static str,
}

impl MappedRequest {
    pub const fn json_sse(body: Value) -> Self {
        Self {
            body,
            accept: "text/event-stream",
            content_type: "application/json",
        }
    }

    pub const fn aws_event_stream(body: Value) -> Self {
        Self {
            body,
            accept: "application/vnd.amazon.eventstream",
            content_type: "application/json",
        }
    }
}
