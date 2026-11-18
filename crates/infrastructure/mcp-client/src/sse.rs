//! Incremental, bounded `text/event-stream` parser (WHATWG SSE grammar).

use crate::McpClientError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SseEvent {
    /// `None` means the default `message` event type.
    pub event: Option<String>,
    pub data: String,
    pub id: Option<String>,
}

impl SseEvent {
    /// MCP carries JSON-RPC messages only in default/`message` events.
    #[must_use]
    pub fn is_message(&self) -> bool {
        self.event.as_deref().is_none_or(|kind| kind == "message")
    }
}

/// Feed arbitrary chunks; complete events are returned as soon as their
/// terminating blank line arrives. Any pending line or event exceeding
/// `max_event_bytes` fails the stream: it is never silently cut.
pub struct SseParser {
    max_event_bytes: usize,
    line: Vec<u8>,
    data: Vec<u8>,
    has_data: bool,
    event: Option<String>,
    id: Option<String>,
    started: bool,
    pending_cr: bool,
}

impl SseParser {
    #[must_use]
    pub fn new(max_event_bytes: usize) -> Self {
        Self {
            max_event_bytes,
            line: Vec::new(),
            data: Vec::new(),
            has_data: false,
            event: None,
            id: None,
            started: false,
            pending_cr: false,
        }
    }

    pub fn push(&mut self, mut chunk: &[u8]) -> Result<Vec<SseEvent>, McpClientError> {
        let mut events = Vec::new();
        if !self.started && !chunk.is_empty() {
            // A UTF-8 BOM may only appear at the very start of the stream.
            let bom = [0xef_u8, 0xbb, 0xbf];
            let probe = self.line.len() + chunk.len();
            if probe < bom.len() && bom.starts_with(&[&self.line[..], chunk].concat()) {
                self.line.extend_from_slice(chunk);
                return Ok(events);
            }
            let mut joined = std::mem::take(&mut self.line);
            joined.extend_from_slice(chunk);
            self.started = true;
            let start = if joined.starts_with(&bom) {
                bom.len()
            } else {
                0
            };
            let rest = joined[start..].to_vec();
            return self.push(&rest);
        }
        while !chunk.is_empty() {
            if self.pending_cr {
                self.pending_cr = false;
                if chunk[0] == b'\n' {
                    chunk = &chunk[1..];
                    continue;
                }
            }
            match chunk
                .iter()
                .position(|byte| *byte == b'\n' || *byte == b'\r')
            {
                Some(end) => {
                    self.append_line(&chunk[..end])?;
                    if chunk[end] == b'\r' {
                        self.pending_cr = true;
                    }
                    chunk = &chunk[end + 1..];
                    if let Some(event) = self.end_line()? {
                        events.push(event);
                    }
                }
                None => {
                    self.append_line(chunk)?;
                    chunk = &[];
                }
            }
        }
        Ok(events)
    }

    /// End of stream: an unterminated final event is discarded per the SSE
    /// specification, but a stream that ends mid-event is reported so the
    /// caller can surface the truncation.
    pub fn finish(&mut self) -> Result<(), McpClientError> {
        if !self.line.is_empty() || self.has_data {
            return Err(McpClientError::ProtocolViolation(
                "event stream ended inside an event",
            ));
        }
        Ok(())
    }

    fn append_line(&mut self, bytes: &[u8]) -> Result<(), McpClientError> {
        if self.line.len() + self.data.len() + bytes.len() > self.max_event_bytes {
            return Err(McpClientError::MessageTooLarge {
                limit: self.max_event_bytes,
            });
        }
        self.line.extend_from_slice(bytes);
        Ok(())
    }

    fn end_line(&mut self) -> Result<Option<SseEvent>, McpClientError> {
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            return self.dispatch();
        }
        if line[0] == b':' {
            return Ok(None);
        }
        let (field, value) = match line.iter().position(|byte| *byte == b':') {
            Some(colon) => {
                let value = &line[colon + 1..];
                (&line[..colon], value.strip_prefix(b" ").unwrap_or(value))
            }
            None => (&line[..], &b""[..]),
        };
        match field {
            b"data" => {
                if self.has_data {
                    self.data.push(b'\n');
                }
                self.data.extend_from_slice(value);
                self.has_data = true;
                if self.data.len() > self.max_event_bytes {
                    return Err(McpClientError::MessageTooLarge {
                        limit: self.max_event_bytes,
                    });
                }
            }
            b"event" => self.event = Some(utf8(value)?),
            b"id" if !value.contains(&0) => self.id = Some(utf8(value)?),
            _ => {}
        }
        Ok(None)
    }

    fn dispatch(&mut self) -> Result<Option<SseEvent>, McpClientError> {
        let event = self.event.take();
        if !self.has_data {
            return Ok(None);
        }
        self.has_data = false;
        let data = utf8(&std::mem::take(&mut self.data))?;
        Ok(Some(SseEvent {
            event: event.filter(|kind| !kind.is_empty()),
            data,
            id: self.id.clone(),
        }))
    }
}

fn utf8(bytes: &[u8]) -> Result<String, McpClientError> {
    String::from_utf8(bytes.to_vec())
        .map_err(|_| McpClientError::ProtocolViolation("event stream is not UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(chunks: &[&[u8]], limit: usize) -> Result<Vec<SseEvent>, McpClientError> {
        let mut parser = SseParser::new(limit);
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(parser.push(chunk)?);
        }
        parser.finish()?;
        Ok(events)
    }

    #[test]
    fn parses_events_across_arbitrary_chunk_boundaries() {
        let stream = b"\xef\xbb\xbf: keepalive\r\nid: 1\r\nevent: message\r\ndata: {\"a\":\r\ndata: 1}\r\n\r\nid:2\ndata:x\n\nevent: other\ndata: y\n\n";
        let whole = parse_all(&[stream], 1024).expect("whole");
        for split in 0..stream.len() {
            let (left, right) = stream.split_at(split);
            assert_eq!(parse_all(&[left, right], 1024).expect("split"), whole);
        }
        let bytewise: Vec<&[u8]> = stream.chunks(1).collect();
        assert_eq!(parse_all(&bytewise, 1024).expect("bytewise"), whole);
        assert_eq!(whole.len(), 3);
        assert_eq!(whole[0].data, "{\"a\":\n1}");
        assert_eq!(whole[0].id.as_deref(), Some("1"));
        assert!(whole[0].is_message());
        assert_eq!(whole[1].data, "x");
        assert_eq!(whole[1].id.as_deref(), Some("2"));
        assert!(!whole[2].is_message());
    }

    #[test]
    fn bare_carriage_return_terminates_lines() {
        let events = parse_all(&[b"data: a\r\rdata: b\r\r"], 64).expect("events");
        assert_eq!(
            events
                .iter()
                .map(|event| event.data.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn only_events_with_a_data_field_are_dispatched() {
        let events = parse_all(&[b"id: prime\ndata:\n\n", b"id: x\n\n"], 64).expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "");
    }

    #[test]
    fn oversized_events_fail_instead_of_truncating() {
        let mut parser = SseParser::new(8);
        assert_eq!(
            parser.push(b"data: 0123456789\n\n"),
            Err(McpClientError::MessageTooLarge { limit: 8 })
        );
        // The bound covers the pending line plus accumulated event data.
        let mut parser = SseParser::new(12);
        assert!(parser.push(b"data: 0123\n").is_ok());
        assert_eq!(
            parser.push(b"data: 45678\n"),
            Err(McpClientError::MessageTooLarge { limit: 12 })
        );
    }

    #[test]
    fn stream_ending_inside_an_event_is_reported() {
        let mut parser = SseParser::new(64);
        assert!(parser.push(b"data: {\"partial\"").expect("push").is_empty());
        assert!(matches!(
            parser.finish(),
            Err(McpClientError::ProtocolViolation(_))
        ));
        assert!(matches!(
            SseParser::new(64).push(b"data: \xff\n\n"),
            Err(McpClientError::ProtocolViolation(_))
        ));
    }
}
