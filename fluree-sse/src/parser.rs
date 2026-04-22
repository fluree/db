//! SSE event stream parser
//!
//! Parses the Server-Sent Events protocol format:
//! - `event:` lines specify the event type
//! - `data:` lines contain the payload (may span multiple lines)
//! - `id:` lines contain the event ID
//! - Empty lines delimit events
//! - Lines starting with `:` are comments (used for keepalive)

/// A parsed SSE event
#[derive(Debug, Clone)]
pub struct SseEvent {
    /// Event type (from `event:` field)
    pub event_type: Option<String>,
    /// Event data (from `data:` field(s), joined with newlines)
    pub data: String,
    /// Event ID (from `id:` field)
    pub id: Option<String>,
}

/// Streaming SSE parser that accumulates bytes and yields complete events
pub struct SseParser {
    /// Buffer for incomplete data
    buffer: String,
    /// Current event being built
    current_event_type: Option<String>,
    current_data: Vec<String>,
    current_id: Option<String>,
}

impl SseParser {
    /// Create a new parser
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            current_event_type: None,
            current_data: Vec::new(),
            current_id: None,
        }
    }

    /// Feed bytes into the parser and return any complete events
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<SseEvent> {
        // Append new data to buffer
        if let Ok(s) = std::str::from_utf8(bytes) {
            self.buffer.push_str(s);
        } else {
            // Invalid UTF-8, skip this chunk
            tracing::warn!("Received invalid UTF-8 in SSE stream");
            return Vec::new();
        }

        let mut events = Vec::new();

        // Process complete lines
        while let Some(newline_pos) = self.buffer.find('\n') {
            let line = self.buffer[..newline_pos].to_string();
            self.buffer = self.buffer[newline_pos + 1..].to_string();

            // Remove trailing \r if present (CRLF line endings)
            let line = line.trim_end_matches('\r');

            if line.is_empty() {
                // Empty line = end of event
                if !self.current_data.is_empty() || self.current_event_type.is_some() {
                    events.push(SseEvent {
                        event_type: self.current_event_type.take(),
                        data: self.current_data.join("\n"),
                        id: self.current_id.take(),
                    });
                    self.current_data.clear();
                }
            } else if let Some(value) = line.strip_prefix("event:") {
                self.current_event_type = Some(value.trim().to_string());
            } else if let Some(value) = line.strip_prefix("data:") {
                // Handle both "data: value" and "data:value"
                let value = value.strip_prefix(' ').unwrap_or(value);
                self.current_data.push(value.to_string());
            } else if let Some(value) = line.strip_prefix("id:") {
                self.current_id = Some(value.trim().to_string());
            } else if line.starts_with(':') {
                // Comment line (keepalive), ignore
            }
            // Other lines are ignored per SSE spec
        }

        events
    }

    /// Reset the parser state (e.g., on reconnect)
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.current_event_type = None;
        self.current_data.clear();
        self.current_id = None;
    }
}

impl Default for SseParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_event() {
        let mut parser = SseParser::new();
        let events = parser.feed(b"event: ns-record\ndata: {\"test\": true}\n\n");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, Some("ns-record".to_string()));
        assert_eq!(events[0].data, r#"{"test": true}"#);
    }

    #[test]
    fn test_parse_multiline_data() {
        let mut parser = SseParser::new();
        let events = parser.feed(b"event: test\ndata: line1\ndata: line2\ndata: line3\n\n");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "line1\nline2\nline3");
    }

    #[test]
    fn test_parse_with_id() {
        let mut parser = SseParser::new();
        let events = parser.feed(b"id: 123\nevent: test\ndata: hello\n\n");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, Some("123".to_string()));
        assert_eq!(events[0].event_type, Some("test".to_string()));
    }

    #[test]
    fn test_parse_comment_ignored() {
        let mut parser = SseParser::new();
        let events = parser.feed(b": this is a comment\nevent: test\ndata: hello\n\n");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, Some("test".to_string()));
    }

    #[test]
    fn test_parse_chunked_input() {
        let mut parser = SseParser::new();

        // First chunk - incomplete
        let events = parser.feed(b"event: test\nda");
        assert_eq!(events.len(), 0);

        // Second chunk - completes the event
        let events = parser.feed(b"ta: hello\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "hello");
    }

    #[test]
    fn test_parse_multiple_events() {
        let mut parser = SseParser::new();
        let events = parser.feed(b"event: first\ndata: 1\n\nevent: second\ndata: 2\n\n");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, Some("first".to_string()));
        assert_eq!(events[1].event_type, Some("second".to_string()));
    }

    #[test]
    fn test_parse_crlf_line_endings() {
        let mut parser = SseParser::new();
        let events = parser.feed(b"event: test\r\ndata: hello\r\n\r\n");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, Some("test".to_string()));
        assert_eq!(events[0].data, "hello");
    }

    #[test]
    fn test_parse_data_without_space() {
        let mut parser = SseParser::new();
        // Some servers send "data:value" without space after colon
        let events = parser.feed(b"data:no-space\n\n");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "no-space");
    }

    #[test]
    fn test_reset() {
        let mut parser = SseParser::new();
        parser.feed(b"event: test\ndata: partial");

        parser.reset();

        let events = parser.feed(b"event: new\ndata: fresh\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, Some("new".to_string()));
    }
}
