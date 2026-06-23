//! NDJSON streaming-query consumer for `fluree query --format ndjson`.
//!
//! Both producers — the local in-process [`Fluree::run_stream_query`] and the
//! remote `POST /v1/fluree/stream/query/<ledger>` endpoint — emit the same
//! newline-delimited record protocol (see
//! [`fluree_db_api::format::ndjson_stream`]): a `head` record, then one `row`
//! record per result row, optional `heartbeat`s, and exactly one terminal
//! (`end` on success, `error` on failure).
//!
//! This module drives that byte stream to stdout in one of two shapes:
//! - **bare** (default): emit each row's inner binding object, one per line.
//!   `head`/`heartbeat` are consumed; the terminal drives the exit status.
//! - **envelope** (`--envelope`): pass every record through verbatim.
//!
//! In bare mode the terminal record is not printed, so the truncation signal it
//! carries is recovered by failing (non-zero exit) when the stream ends without
//! a terminal record or carries an `error`. A downstream `BrokenPipe` (e.g.
//! `| head`) ends the stream cleanly with exit 0.

use std::io::{self, Write};

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;

use crate::error::{CliError, CliResult};

/// The exact byte prefix of a `row` record. Rows are hand-serialized by
/// `stream_ndjson_rows` as `{"type":"row","row":<obj>}\n`, so the inner binding
/// object can be sliced out without parsing the whole line — the hot path.
const ROW_OPEN: &[u8] = b"{\"type\":\"row\",\"row\":";

/// Summary captured from the stream's terminal record, used by the caller to
/// build the stderr footer and (via [`NdjsonConsumer::finish`]) the exit status.
#[derive(Default, Debug)]
pub struct StreamOutcome {
    pub rows: u64,
    pub fuel: Option<f64>,
    pub time: Option<String>,
    /// True when stdout closed mid-stream (e.g. piped into `head`). The caller
    /// suppresses the footer and exits 0.
    pub broken_pipe: bool,
}

/// Incremental NDJSON record consumer. Feed it raw bytes with
/// [`push_bytes`](Self::push_bytes) (chunk boundaries need not align to lines);
/// call [`finish`](Self::finish) at end-of-stream.
pub struct NdjsonConsumer<W: Write> {
    envelope: bool,
    out: W,
    /// Holds bytes not yet terminated by a newline (records may span chunks).
    buf: Vec<u8>,
    saw_terminal: bool,
    error: Option<(String, String)>,
    outcome: StreamOutcome,
}

impl<W: Write> NdjsonConsumer<W> {
    pub fn new(out: W, envelope: bool) -> Self {
        Self {
            envelope,
            out,
            buf: Vec::with_capacity(8 * 1024),
            saw_terminal: false,
            error: None,
            outcome: StreamOutcome::default(),
        }
    }

    /// Append a chunk and dispatch every complete (newline-terminated) record.
    pub fn push_bytes(&mut self, chunk: &[u8]) -> io::Result<()> {
        if self.outcome.broken_pipe {
            return Ok(());
        }
        self.buf.extend_from_slice(chunk);
        let mut start = 0;
        while let Some(nl) = self.buf[start..].iter().position(|&b| b == b'\n') {
            let end = start + nl;
            // Borrow the line region without holding a borrow on `self` across
            // the dispatch (which mutates `self`): copy the slice indices.
            let line_range = start..end;
            start = end + 1;
            self.dispatch(line_range)?;
            if self.outcome.broken_pipe {
                break;
            }
        }
        if start > 0 {
            self.buf.drain(..start);
        }
        Ok(())
    }

    fn dispatch(&mut self, range: std::ops::Range<usize>) -> io::Result<()> {
        // Trim a trailing CR so CRLF-framed streams parse cleanly.
        let mut end = range.end;
        if end > range.start && self.buf[end - 1] == b'\r' {
            end -= 1;
        }
        let line = &self.buf[range.start..end];
        if line.is_empty() {
            return Ok(());
        }

        // Hot path: a `row` record. Slice out the inner binding object instead
        // of parsing the full line.
        if line.starts_with(ROW_OPEN) && line.last() == Some(&b'}') {
            if self.envelope {
                let line = line.to_vec();
                return self.write_record(&line);
            }
            // inner = line without the `{"type":"row","row":` prefix and the
            // closing `}` of the outer record.
            let inner = line[ROW_OPEN.len()..line.len() - 1].to_vec();
            return self.write_record(&inner);
        }

        // Cold path: head / heartbeat / terminal / unknown. Parse to classify.
        match serde_json::from_slice::<serde_json::Value>(line) {
            Ok(value) => {
                let kind = value.get("type").and_then(|t| t.as_str()).unwrap_or("");
                match kind {
                    "end" => {
                        self.saw_terminal = true;
                        self.outcome.rows = value
                            .get("rows")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(0);
                        self.outcome.fuel = value.get("fuel").and_then(serde_json::Value::as_f64);
                        self.outcome.time = value
                            .get("time")
                            .and_then(serde_json::Value::as_str)
                            .map(String::from);
                    }
                    "error" => {
                        self.saw_terminal = true;
                        self.outcome.rows = value
                            .get("rows")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(0);
                        let code = value
                            .pointer("/error/code")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("error")
                            .to_string();
                        let message = value
                            .pointer("/error/message")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("streaming query failed")
                            .to_string();
                        self.error = Some((code, message));
                    }
                    _ => {}
                }
                if self.envelope {
                    let line = line.to_vec();
                    self.write_record(&line)?;
                }
            }
            Err(_) => {
                // Not valid JSON. Pass through verbatim in envelope mode; in
                // bare mode there is nothing meaningful to emit.
                if self.envelope {
                    let line = line.to_vec();
                    self.write_record(&line)?;
                }
            }
        }
        Ok(())
    }

    /// Write a single record line plus a newline, treating a closed stdout
    /// (BrokenPipe) as a clean early stop rather than an error.
    fn write_record(&mut self, line: &[u8]) -> io::Result<()> {
        match self
            .out
            .write_all(line)
            .and_then(|()| self.out.write_all(b"\n"))
        {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::BrokenPipe => {
                self.outcome.broken_pipe = true;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Flush, validate the terminal, and return the captured outcome.
    ///
    /// Errors (non-zero exit) when the stream carried an `error` terminal or
    /// ended without any terminal record (truncated / dropped connection).
    pub fn finish(mut self) -> CliResult<StreamOutcome> {
        // A trailing record without a final newline still needs dispatching.
        if !self.buf.is_empty() && !self.outcome.broken_pipe {
            let range = 0..self.buf.len();
            self.dispatch(range)
                .map_err(|e| CliError::Remote(format!("write error: {e}")))?;
        }
        match self.out.flush() {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::BrokenPipe => {
                self.outcome.broken_pipe = true;
            }
            Err(e) => return Err(CliError::Remote(format!("write error: {e}"))),
        }

        if self.outcome.broken_pipe {
            return Ok(self.outcome);
        }
        if let Some((code, message)) = self.error {
            return Err(CliError::Remote(format!(
                "streaming query failed [{code}]: {message}"
            )));
        }
        if !self.saw_terminal {
            return Err(CliError::Remote(
                "streaming query ended without a terminal record (stream truncated or connection dropped)"
                    .to_string(),
            ));
        }
        Ok(self.outcome)
    }
}

/// Drain the local producer's channel into the consumer.
pub async fn drive_channel<W: Write>(
    mut rx: mpsc::Receiver<Bytes>,
    consumer: &mut NdjsonConsumer<W>,
) -> CliResult<()> {
    while let Some(chunk) = rx.recv().await {
        consumer
            .push_bytes(&chunk)
            .map_err(|e| CliError::Remote(format!("write error: {e}")))?;
        if consumer.outcome.broken_pipe {
            // Stdout is gone; drain the channel so the producer's `send` calls
            // resolve and it can wind down, but stop writing.
            rx.close();
            while rx.recv().await.is_some() {}
            break;
        }
    }
    Ok(())
}

/// Drain a remote HTTP NDJSON response body into the consumer.
pub async fn drive_response<W: Write>(
    response: reqwest::Response,
    consumer: &mut NdjsonConsumer<W>,
) -> CliResult<()> {
    let mut stream = response.bytes_stream();
    while let Some(item) = stream.next().await {
        let chunk = item.map_err(|e| CliError::Remote(format!("stream read error: {e}")))?;
        consumer
            .push_bytes(&chunk)
            .map_err(|e| CliError::Remote(format!("write error: {e}")))?;
        if consumer.outcome.broken_pipe {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(records: &[&str], envelope: bool) -> (String, CliResult<StreamOutcome>) {
        let mut out: Vec<u8> = Vec::new();
        let result = {
            let mut consumer = NdjsonConsumer::new(&mut out, envelope);
            for r in records {
                consumer.push_bytes(r.as_bytes()).unwrap();
            }
            consumer.finish()
        };
        (String::from_utf8(out).unwrap(), result)
    }

    #[test]
    fn bare_emits_inner_row_objects_only() {
        let (out, res) = run(
            &[
                "{\"type\":\"head\",\"vars\":[\"name\"]}\n",
                "{\"type\":\"row\",\"row\":{\"name\":{\"type\":\"literal\",\"value\":\"Alice\"}}}\n",
                "{\"type\":\"row\",\"row\":{\"name\":{\"type\":\"literal\",\"value\":\"Bob\"}}}\n",
                "{\"type\":\"end\",\"rows\":2}\n",
            ],
            false,
        );
        let outcome = res.unwrap();
        assert_eq!(outcome.rows, 2);
        assert_eq!(
            out,
            "{\"name\":{\"type\":\"literal\",\"value\":\"Alice\"}}\n\
             {\"name\":{\"type\":\"literal\",\"value\":\"Bob\"}}\n"
        );
    }

    #[test]
    fn envelope_passes_every_record_verbatim() {
        let records = [
            "{\"type\":\"head\",\"vars\":[\"name\"]}\n",
            "{\"type\":\"row\",\"row\":{\"name\":{\"type\":\"literal\",\"value\":\"Alice\"}}}\n",
            "{\"type\":\"end\",\"rows\":1}\n",
        ];
        let (out, res) = run(&records, true);
        assert!(res.is_ok());
        assert_eq!(out, records.concat());
    }

    #[test]
    fn record_split_across_chunks_is_reassembled() {
        let mut out: Vec<u8> = Vec::new();
        let res = {
            let mut consumer = NdjsonConsumer::new(&mut out, false);
            consumer
                .push_bytes(b"{\"type\":\"row\",\"row\":{\"a\":")
                .unwrap();
            consumer
                .push_bytes(
                    b"{\"type\":\"literal\",\"value\":\"x\"}}}\n{\"type\":\"end\",\"rows\":1}\n",
                )
                .unwrap();
            consumer.finish()
        };
        assert_eq!(res.unwrap().rows, 1);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"a\":{\"type\":\"literal\",\"value\":\"x\"}}\n"
        );
    }

    #[test]
    fn error_terminal_fails() {
        let (_out, res) = run(
            &[
                "{\"type\":\"row\",\"row\":{\"a\":{\"type\":\"literal\",\"value\":\"x\"}}}\n",
                "{\"type\":\"error\",\"error\":{\"code\":\"fuel_exhausted\",\"message\":\"over\"},\"rows\":1}\n",
            ],
            false,
        );
        let err = res.unwrap_err();
        assert!(err.to_string().contains("fuel_exhausted"));
    }

    #[test]
    fn missing_terminal_is_truncation_error() {
        let (_out, res) = run(
            &["{\"type\":\"row\",\"row\":{\"a\":{\"type\":\"literal\",\"value\":\"x\"}}}\n"],
            false,
        );
        assert!(res.unwrap_err().to_string().contains("truncated"));
    }

    #[test]
    fn end_record_carries_fuel_and_time() {
        let (_out, res) = run(
            &[
                "{\"type\":\"row\",\"row\":{\"a\":{\"type\":\"literal\",\"value\":\"x\"}}}\n",
                "{\"type\":\"end\",\"rows\":1,\"fuel\":12.5,\"time\":\"3.2ms\"}\n",
            ],
            false,
        );
        let outcome = res.unwrap();
        assert_eq!(outcome.fuel, Some(12.5));
        assert_eq!(outcome.time.as_deref(), Some("3.2ms"));
    }
}
