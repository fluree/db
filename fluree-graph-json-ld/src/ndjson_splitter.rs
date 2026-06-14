//! Newline-delimited JSON-LD (ndjson / jsonl) splitter for bulk import.
//!
//! Where [`crate::splitter`] handles structured JSON-LD documents
//! (`{ "@context": …, "@graph": […] }` and top-level arrays) with a
//! depth-tracking byte scanner, this module handles the much simpler
//! **newline-delimited** shape, where each line is one complete JSON-LD node:
//!
//! ```text
//! {"@context": {"ex": "http://example.org/"}}   ← optional leading context
//! {"@id": "ex:1", "ex:name": "Alice"}
//! {"@id": "ex:2", "ex:name": "Bob"}
//! ```
//!
//! Because line boundaries are value boundaries, no structural scanning is
//! needed: the reader groups whole lines into chunks of ~`chunk_size_bytes` and
//! wraps each chunk as a standalone `{ "@context": …, "@graph": [ … ] }`
//! document — the exact shape [`crate::splitter`] emits, so downstream chunk
//! parsing is identical (the chunks reuse [`crate::splitter::ChunkPayload`]).
//!
//! # Context lines
//!
//! The first non-blank line MAY be a lone `{"@context": …}` map shared by all
//! following nodes, or it may already be the first node.
//! [`FirstLineContextPolicy`] controls the interpretation; the default
//! ([`FirstLineContextPolicy::Auto`]) treats line 1 as a context only when it
//! is an object whose single key is `@context`.
//!
//! A lone context on a LATER line **replaces** the shared context for the
//! lines that follow (logged at `warn`), so concatenated ndjson files
//! (`cat a.jsonl b.jsonl`) compose naturally — each segment's records resolve
//! against its own context, exactly as if the files were imported separately.
//! Under [`FirstLineContextPolicy::Entity`] lone context lines are data nodes
//! everywhere, never switches. Nodes carrying an inline `@context` alongside
//! other keys remain ordinary nodes anywhere.
//!
//! # Streaming
//!
//! The reader is driven by any [`BufRead`] (not a `Path`), so it streams local
//! files and remote object byte-streams alike in bounded memory — there is no
//! `Seek` requirement. A background thread emits chunks through a bounded
//! channel, mirroring [`crate::splitter::StreamingJsonLdReader`].
//!
//! # Malformed input
//!
//! True ndjson has exactly one JSON object per line. Each line is validated as
//! a single complete JSON object (cheaply, via `IgnoredAny` — no value tree is
//! built); a line that isn't (e.g. pretty-printed JSON split across lines, or a
//! stray scalar/array line) fails loudly with the offending line number rather
//! than producing a silently-broken chunk. A malformed first line surfaces from
//! the constructor; a malformed later line surfaces from
//! [`NdjsonReader::join`]. A leading UTF-8 BOM is tolerated.

use std::io::BufRead;
use std::sync::mpsc;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};

use serde::de::IgnoredAny;
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::splitter::{ChunkPayload, JsonLdPrelude, SplitError};

/// Policy for interpreting the first non-blank line of an ndjson/jsonl source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FirstLineContextPolicy {
    /// Treat line 1 as a shared `@context` only if it is an object whose single
    /// key is `@context`; otherwise treat it as the first node. (Default.)
    #[default]
    Auto,
    /// Force line 1 to be the shared context. If the line is `{"@context": …}`
    /// its `@context` value is used; otherwise the whole object is used as the
    /// context map. Errors if line 1 is not a JSON object.
    Context,
    /// Lone `{"@context": …}` lines are always data nodes — line 1 is the
    /// first node, and mid-stream lone contexts do NOT replace the shared
    /// context. The escape hatch for data whose nodes can look like contexts.
    Entity,
}

/// Streams a newline-delimited JSON-LD source as standalone JSON-LD chunks.
pub struct NdjsonReader {
    prelude: JsonLdPrelude,
    rx: Mutex<mpsc::Receiver<ChunkPayload>>,
    reader_handle: Option<JoinHandle<Result<usize, SplitError>>>,
}

impl NdjsonReader {
    /// Create a reader over any byte stream (local file, S3 object range
    /// stream, decompressor, …). This is the streaming entry point: no `Seek`
    /// is required and memory stays bounded by `channel_capacity` × chunk size.
    pub fn new_from_reader(
        mut reader: Box<dyn BufRead + Send>,
        chunk_size_bytes: u64,
        channel_capacity: usize,
        policy: FirstLineContextPolicy,
    ) -> Result<Self, SplitError> {
        // Resolve the shared context (and possibly the first node) up front:
        // the context must be known before any chunk can be assembled, and a
        // malformed first line should surface from the constructor.
        let head = read_prelude(&mut reader, policy)?;
        let context = head.context;
        let prelude = JsonLdPrelude {
            context: context.clone(),
        };

        let (tx, rx) = mpsc::sync_channel(channel_capacity.max(1));
        let first_node = head.first_node;
        let lines_consumed = head.lines_consumed;
        let handle = thread::spawn(move || {
            reader_thread(
                reader,
                context,
                first_node,
                lines_consumed,
                chunk_size_bytes,
                policy,
                tx,
            )
        });

        Ok(Self {
            prelude,
            rx: Mutex::new(rx),
            reader_handle: Some(handle),
        })
    }

    /// Receive the next chunk, blocking until available. `Ok(None)` once all
    /// chunks have been emitted (call [`join`](Self::join) afterwards to
    /// observe any reader-thread error).
    pub fn recv_chunk(&self) -> Result<Option<ChunkPayload>, SplitError> {
        let rx = self.rx.lock().unwrap();
        match rx.recv() {
            Ok(payload) => Ok(Some(payload)),
            Err(_) => Ok(None),
        }
    }

    /// The shared `@context`, if the source carried a leading context line.
    pub fn prelude(&self) -> &JsonLdPrelude {
        &self.prelude
    }

    /// Wait for the background reader thread and return the chunk count. This
    /// is where a malformed later line surfaces, so callers that need to detect
    /// such errors MUST call `join` after draining the channel.
    pub fn join(&mut self) -> Result<usize, SplitError> {
        if let Some(handle) = self.reader_handle.take() {
            handle
                .join()
                .map_err(|_| SplitError::InvalidJson("ndjson reader thread panicked".into()))?
        } else {
            Ok(0)
        }
    }
}

/// Outcome of inspecting the first non-blank line.
struct Head {
    context: Option<JsonValue>,
    /// The first node line, when the first non-blank line was a node rather
    /// than a consumed context.
    first_node: Option<Vec<u8>>,
    /// Source lines consumed (the first node, when present, sits on the last
    /// of them). Seeds the reader thread's line counter so later error
    /// messages report true file line numbers.
    lines_consumed: usize,
}

/// UTF-8 byte-order mark, tolerated at the very start of the stream (common in
/// Windows-exported files).
const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

/// Read and interpret the first non-blank line per `policy`.
fn read_prelude(
    reader: &mut Box<dyn BufRead + Send>,
    policy: FirstLineContextPolicy,
) -> Result<Head, SplitError> {
    let mut line_buf = Vec::new();
    let mut line_no = 0usize;
    loop {
        if !read_line(reader, &mut line_buf)? {
            return Ok(Head {
                context: None,
                first_node: None,
                lines_consumed: line_no,
            });
        }
        line_no += 1;
        let line: &[u8] = if line_no == 1 {
            line_buf.strip_prefix(UTF8_BOM).unwrap_or(&line_buf)
        } else {
            &line_buf
        };
        let trimmed = line.trim_ascii();
        if trimmed.is_empty() {
            continue;
        }

        if policy == FirstLineContextPolicy::Entity {
            validate_json_line(trimmed, line_no)?;
            return Ok(Head {
                context: None,
                first_node: Some(trimmed.to_vec()),
                lines_consumed: line_no,
            });
        }

        // Auto / Context both need to inspect the parsed first line.
        let value: JsonValue = serde_json::from_slice(trimmed).map_err(|e| {
            SplitError::InvalidJson(format!("line {line_no}: first line is not valid JSON: {e}"))
        })?;

        return match policy {
            FirstLineContextPolicy::Context => {
                let ctx = context_from_value(&value).ok_or_else(|| {
                    SplitError::InvalidJson(format!(
                        "line {line_no}: first line must be a context object (policy=Context)"
                    ))
                })?;
                Ok(Head {
                    context: Some(ctx),
                    first_node: None,
                    lines_consumed: line_no,
                })
            }
            FirstLineContextPolicy::Auto => {
                if let Some(ctx) = lone_context(&value) {
                    Ok(Head {
                        context: Some(ctx),
                        first_node: None,
                        lines_consumed: line_no,
                    })
                } else if !value.is_object() {
                    Err(SplitError::InvalidJson(format!(
                        "line {line_no}: ndjson requires one JSON object per line"
                    )))
                } else {
                    Ok(Head {
                        context: None,
                        first_node: Some(trimmed.to_vec()),
                        lines_consumed: line_no,
                    })
                }
            }
            FirstLineContextPolicy::Entity => unreachable!("handled above"),
        };
    }
}

/// If `value` is an object whose only key is `@context`, return that context.
fn lone_context(value: &JsonValue) -> Option<JsonValue> {
    match value {
        JsonValue::Object(map) if map.len() == 1 => map.get("@context").cloned(),
        _ => None,
    }
}

/// If `line` (already validated as one JSON object) is a lone
/// `{"@context":…}` object, return its context value. The byte pre-filter
/// keeps the cost near zero for ordinary node lines: a lone-context object
/// necessarily opens with the `"@context"` key, so only lines that do are
/// fully parsed (and a node that merely *starts* with an inline `@context`
/// is rejected by the single-key check).
fn lone_context_line(line: &[u8]) -> Option<JsonValue> {
    let inner = line[1..].trim_ascii_start();
    if !inner.starts_with(b"\"@context\"") {
        return None;
    }
    serde_json::from_slice::<JsonValue>(line)
        .ok()
        .and_then(|v| lone_context(&v))
}

/// Context to use for a forced (policy=Context) first line: the `@context`
/// value if present, otherwise the whole object treated as the context map.
fn context_from_value(value: &JsonValue) -> Option<JsonValue> {
    match value {
        JsonValue::Object(map) => Some(
            map.get("@context")
                .cloned()
                .unwrap_or_else(|| value.clone()),
        ),
        _ => None,
    }
}

/// Background thread: group node lines into `{"@context":…,"@graph":[…]}`
/// chunks of ~`chunk_size` bytes.
fn reader_thread(
    mut reader: Box<dyn BufRead + Send>,
    context: Option<JsonValue>,
    first_node: Option<Vec<u8>>,
    lines_consumed: usize,
    chunk_size: u64,
    policy: FirstLineContextPolicy,
    tx: mpsc::SyncSender<ChunkPayload>,
) -> Result<usize, SplitError> {
    let mut prefix = build_prefix(&context)?;
    let suffix: &[u8] = b"]}";

    // The buffer always starts with the prefix; `emit` seals it with the
    // suffix and moves it out whole, so the accumulated bytes are never
    // re-copied. (The size check below therefore includes the prefix length —
    // negligible against chunk_size.)
    let mut chunk_buf = seeded_buf(&prefix, chunk_size);
    let mut element_count: usize = 0;
    let mut chunk_idx: usize = 0;
    // Continue the prelude's numbering so errors report true file lines.
    let mut line_no: usize = lines_consumed;
    let mut line_buf: Vec<u8> = Vec::new();

    // The first node (already read + validated during prelude detection, where
    // it was counted) starts the stream.
    if let Some(node) = first_node {
        push_node(&mut chunk_buf, &mut element_count, &node);
    }

    while read_line(&mut reader, &mut line_buf)? {
        line_no += 1;
        let trimmed = line_buf.trim_ascii();
        if trimmed.is_empty() {
            continue;
        }
        validate_json_line(trimmed, line_no)?;
        // A lone `{"@context":…}` line mid-stream REPLACES the shared context
        // for the lines that follow, so concatenated ndjson files
        // (`cat a.jsonl b.jsonl`) compose naturally — each segment keeps its
        // own context, mirroring the per-file behavior of directory imports.
        // The current chunk is sealed under the outgoing context first.
        // Under `Entity` policy (the "my lines are never contexts" escape
        // hatch) lone contexts remain data nodes, as on line 1.
        if policy != FirstLineContextPolicy::Entity {
            if let Some(new_ctx) = lone_context_line(trimmed) {
                if element_count > 0 {
                    emit(&tx, &mut chunk_idx, &mut chunk_buf, suffix)?;
                }
                tracing::warn!(
                    line = line_no,
                    "ndjson: lone {{\"@context\":…}} line replaces the shared \
                     context for subsequent lines (concatenated sources?)"
                );
                prefix = build_prefix(&Some(new_ctx))?;
                chunk_buf = seeded_buf(&prefix, chunk_size);
                element_count = 0;
                continue;
            }
        }
        push_node(&mut chunk_buf, &mut element_count, trimmed);

        if chunk_buf.len() as u64 >= chunk_size {
            emit(&tx, &mut chunk_idx, &mut chunk_buf, suffix)?;
            chunk_buf = seeded_buf(&prefix, chunk_size);
            element_count = 0;
        }
    }

    if element_count > 0 {
        emit(&tx, &mut chunk_idx, &mut chunk_buf, suffix)?;
    }

    Ok(chunk_idx)
}

/// A fresh chunk buffer pre-seeded with the document prefix.
fn seeded_buf(prefix: &[u8], chunk_size: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(prefix.len() + chunk_size.max(1) as usize + 16);
    buf.extend_from_slice(prefix);
    buf
}

fn push_node(chunk_buf: &mut Vec<u8>, element_count: &mut usize, node: &[u8]) {
    if *element_count > 0 {
        chunk_buf.extend_from_slice(b",\n");
    }
    chunk_buf.extend_from_slice(node);
    *element_count += 1;
}

/// Seal `chunk_buf` with the suffix and send it, leaving the buffer empty.
fn emit(
    tx: &mpsc::SyncSender<ChunkPayload>,
    chunk_idx: &mut usize,
    chunk_buf: &mut Vec<u8>,
    suffix: &[u8],
) -> Result<(), SplitError> {
    chunk_buf.extend_from_slice(suffix);
    let doc = std::mem::take(chunk_buf);
    tx.send((*chunk_idx, doc))
        .map_err(|_| SplitError::ChannelClosed)?;
    *chunk_idx += 1;
    Ok(())
}

/// Build the chunk prefix: `{"@context":<ctx>,"@graph":[` or `{"@graph":[`.
fn build_prefix(context: &Option<JsonValue>) -> Result<Vec<u8>, SplitError> {
    let mut prefix = Vec::with_capacity(64);
    match context {
        Some(ctx) => {
            prefix.extend_from_slice(b"{\"@context\":");
            prefix.extend_from_slice(&serde_json::to_vec(ctx)?);
            prefix.extend_from_slice(b",\"@graph\":[");
        }
        None => prefix.extend_from_slice(b"{\"@graph\":["),
    }
    Ok(prefix)
}

/// Validate that `line` is exactly one complete JSON object (no value tree is
/// built, and trailing data is rejected).
fn validate_json_line(line: &[u8], line_no: usize) -> Result<(), SplitError> {
    if line.first() != Some(&b'{') {
        return Err(SplitError::InvalidJson(format!(
            "line {line_no}: ndjson requires one JSON object per line"
        )));
    }
    let mut de = serde_json::Deserializer::from_slice(line);
    IgnoredAny::deserialize(&mut de).map_err(|e| {
        SplitError::InvalidJson(format!(
            "line {line_no}: not a single complete JSON value ({e}); \
             ndjson requires one JSON object per line"
        ))
    })?;
    de.end().map_err(|e| {
        SplitError::InvalidJson(format!(
            "line {line_no}: trailing data after JSON value ({e}); \
             ndjson requires one JSON object per line"
        ))
    })?;
    Ok(())
}

/// Read one line (terminator included) into `buf`, clearing it first; the
/// buffer is reused across lines to avoid a per-line allocation. `false` at
/// EOF.
fn read_line(reader: &mut Box<dyn BufRead + Send>, buf: &mut Vec<u8>) -> Result<bool, SplitError> {
    buf.clear();
    Ok(reader.read_until(b'\n', buf)? != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn reader_for(input: &str) -> Box<dyn BufRead + Send> {
        Box::new(Cursor::new(input.as_bytes().to_vec()))
    }

    /// Drive a reader to completion, returning the prelude context, the parsed
    /// chunk documents, and propagating any reader-thread error via `join`.
    fn read_all(
        input: &str,
        chunk_size: u64,
        policy: FirstLineContextPolicy,
    ) -> Result<(Option<JsonValue>, Vec<JsonValue>), SplitError> {
        let mut reader = NdjsonReader::new_from_reader(reader_for(input), chunk_size, 4, policy)?;
        let ctx = reader.prelude().context.clone();
        let mut chunks = Vec::new();
        while let Some((_idx, bytes)) = reader.recv_chunk()? {
            chunks.push(serde_json::from_slice::<JsonValue>(&bytes).unwrap());
        }
        reader.join()?;
        Ok((ctx, chunks))
    }

    /// Total `@graph` nodes across all chunks.
    fn total_nodes(chunks: &[JsonValue]) -> usize {
        chunks
            .iter()
            .map(|c| c["@graph"].as_array().unwrap().len())
            .sum()
    }

    #[test]
    fn auto_captures_leading_lone_context() {
        let input = "{\"@context\":{\"ex\":\"http://example.org/\"}}\n\
                     {\"@id\":\"ex:1\"}\n\
                     {\"@id\":\"ex:2\"}\n";
        let (ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert_eq!(ctx.unwrap()["ex"], "http://example.org/");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0]["@context"]["ex"], "http://example.org/");
        assert_eq!(total_nodes(&chunks), 2);
    }

    #[test]
    fn auto_no_context_when_absent() {
        let input = "{\"@id\":\"http://example.org/1\"}\n{\"@id\":\"http://example.org/2\"}\n";
        let (ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert!(ctx.is_none());
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].get("@context").is_none());
        assert_eq!(total_nodes(&chunks), 2);
    }

    #[test]
    fn auto_first_line_with_context_and_other_keys_is_a_node() {
        // Not a *lone* @context (it also has @id) → it is the first node, and
        // no shared context is captured.
        let input =
            "{\"@context\":{\"ex\":\"http://example.org/\"},\"@id\":\"ex:1\"}\n{\"@id\":\"ex:2\"}\n";
        let (ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert!(ctx.is_none());
        assert_eq!(total_nodes(&chunks), 2);
    }

    #[test]
    fn context_policy_accepts_bare_context_map() {
        let input = "{\"ex\":\"http://example.org/\"}\n{\"@id\":\"ex:1\"}\n";
        let (ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Context).unwrap();
        assert_eq!(ctx.unwrap()["ex"], "http://example.org/");
        assert_eq!(total_nodes(&chunks), 1);
    }

    #[test]
    fn context_policy_unwraps_at_context_object() {
        let input = "{\"@context\":\"http://schema.org/\"}\n{\"@id\":\"ex:1\"}\n";
        let (ctx, _chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Context).unwrap();
        assert_eq!(ctx.unwrap(), "http://schema.org/");
    }

    #[test]
    fn context_policy_rejects_non_object_first_line() {
        let input = "[1,2,3]\n{\"@id\":\"ex:1\"}\n";
        let err = read_all(input, 1_000_000, FirstLineContextPolicy::Context).unwrap_err();
        assert!(matches!(err, SplitError::InvalidJson(_)));
    }

    #[test]
    fn entity_policy_keeps_context_looking_first_line_as_node() {
        let input = "{\"@context\":{\"ex\":\"http://example.org/\"}}\n{\"@id\":\"ex:1\"}\n";
        let (ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Entity).unwrap();
        assert!(ctx.is_none());
        // Both lines are nodes.
        assert_eq!(total_nodes(&chunks), 2);
    }

    #[test]
    fn splits_into_multiple_chunks_and_preserves_all_nodes() {
        let mut input = String::from("{\"@context\":{\"ex\":\"http://example.org/\"}}\n");
        for i in 0..50 {
            input.push_str(&format!(
                "{{\"@id\":\"ex:{i}\",\"ex:name\":\"node {i}\"}}\n"
            ));
        }
        // Small chunk size forces splitting at line boundaries.
        let (_ctx, chunks) = read_all(&input, 64, FirstLineContextPolicy::Auto).unwrap();
        assert!(
            chunks.len() > 1,
            "expected multiple chunks, got {}",
            chunks.len()
        );
        // Every chunk is a standalone doc carrying the shared context.
        for c in &chunks {
            assert_eq!(c["@context"]["ex"], "http://example.org/");
            assert!(!c["@graph"].as_array().unwrap().is_empty());
        }
        assert_eq!(total_nodes(&chunks), 50);
    }

    #[test]
    fn handles_crlf_blank_lines_and_missing_trailing_newline() {
        // CRLF terminators, interspersed blank lines, and no final newline.
        let input = "{\"@context\":{\"ex\":\"http://example.org/\"}}\r\n\
                     \r\n\
                     {\"@id\":\"ex:1\"}\r\n\
                     \r\n\
                     {\"@id\":\"ex:2\"}";
        let (ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert!(ctx.is_some());
        assert_eq!(total_nodes(&chunks), 2);
    }

    #[test]
    fn empty_input_yields_no_chunks() {
        let (ctx, chunks) = read_all("\n  \n\n", 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert!(ctx.is_none());
        assert!(chunks.is_empty());
    }

    #[test]
    fn only_a_context_line_yields_no_chunks() {
        let input = "{\"@context\":{\"ex\":\"http://example.org/\"}}\n";
        let (ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert!(ctx.is_some());
        assert!(chunks.is_empty());
    }

    #[test]
    fn malformed_first_line_fails_from_constructor() {
        // A single `{` is incomplete JSON — surfaces immediately.
        let err = NdjsonReader::new_from_reader(
            reader_for("{\n\"@id\":\"x\"\n}\n"),
            1_000_000,
            4,
            FirstLineContextPolicy::Auto,
        )
        .err()
        .expect("malformed first line must error");
        assert!(matches!(err, SplitError::InvalidJson(_)));
    }

    #[test]
    fn malformed_later_line_fails_loudly_from_join() {
        // First line valid; a later line is two values (not single-value ndjson).
        let input = "{\"@id\":\"ex:1\"}\n{\"@id\":\"ex:2\"} {\"@id\":\"ex:3\"}\n";
        let err = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap_err();
        assert!(matches!(err, SplitError::InvalidJson(_)));
    }

    #[test]
    fn error_line_numbers_account_for_prelude_lines() {
        // Context on line 1, blank line 2, valid node line 3, malformed line 4:
        // the error must say "line 4", not restart counting after the prelude.
        let input = "{\"@context\":{\"ex\":\"http://example.org/\"}}\n\
                     \n\
                     {\"@id\":\"ex:1\"}\n\
                     {\"@id\":\"ex:2\"} trailing\n";
        let err = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line 4"), "wrong line number in: {msg}");
    }

    #[test]
    fn strips_leading_utf8_bom() {
        let mut input = Vec::from(*b"\xEF\xBB\xBF");
        input.extend_from_slice(
            b"{\"@context\":{\"ex\":\"http://example.org/\"}}\n{\"@id\":\"ex:1\"}\n",
        );
        let input = String::from_utf8(input).unwrap();
        let (ctx, chunks) = read_all(&input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert_eq!(ctx.unwrap()["ex"], "http://example.org/");
        assert_eq!(total_nodes(&chunks), 1);
    }

    #[test]
    fn non_object_line_fails_with_line_number() {
        // Scalars/arrays are valid JSON values but not ndjson node lines; they
        // must fail here with a line number, not surface later as a confusing
        // downstream JSON-LD expansion error.
        let input = "{\"@id\":\"ex:1\"}\n42\n";
        let err = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("line 2") && msg.contains("JSON object per line"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn mid_stream_lone_context_replaces_shared_context() {
        // Concatenated-files shape: b.jsonl's context lands mid-stream and
        // replaces a.jsonl's for the lines that follow. The chunk in flight
        // is sealed under the outgoing context.
        let input = "{\"@context\":{\"a\":\"http://a.example/\"}}\n\
                     {\"@id\":\"a:1\"}\n\
                     {\"@context\":{\"b\":\"http://b.example/\"}}\n\
                     {\"@id\":\"b:1\"}\n";
        let (ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        // Prelude still reports the FIRST context.
        assert_eq!(ctx.unwrap()["a"], "http://a.example/");
        assert_eq!(chunks.len(), 2, "switch must seal the in-flight chunk");
        assert_eq!(chunks[0]["@context"]["a"], "http://a.example/");
        assert_eq!(chunks[0]["@graph"][0]["@id"], "a:1");
        assert_eq!(chunks[1]["@context"]["b"], "http://b.example/");
        assert_eq!(chunks[1]["@graph"][0]["@id"], "b:1");
        assert_eq!(total_nodes(&chunks), 2);
    }

    #[test]
    fn consecutive_context_lines_last_one_wins() {
        // Two switches with no nodes between them: no empty chunk is emitted
        // and the following nodes resolve against the last context.
        let input = "{\"@context\":{\"a\":\"http://a.example/\"}}\n\
                     {\"@context\":{\"b\":\"http://b.example/\"}}\n\
                     {\"@id\":\"b:1\"}\n";
        let (_ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0]["@context"]["b"], "http://b.example/");
        assert_eq!(total_nodes(&chunks), 1);
    }

    #[test]
    fn trailing_context_line_emits_no_empty_chunk() {
        let input = "{\"@id\":\"http://example.org/1\"}\n\
                     {\"@context\":{\"x\":\"http://x.example/\"}}\n";
        let (_ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(total_nodes(&chunks), 1);
    }

    #[test]
    fn entity_policy_keeps_mid_stream_lone_context_as_node() {
        let input = "{\"@id\":\"http://example.org/1\"}\n\
                     {\"@context\":{\"x\":\"http://x.example/\"}}\n\
                     {\"@id\":\"http://example.org/2\"}\n";
        let (ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Entity).unwrap();
        assert!(ctx.is_none());
        // No switch: all three lines are nodes in one no-context chunk.
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].get("@context").is_none());
        assert_eq!(total_nodes(&chunks), 3);
    }

    #[test]
    fn inline_context_on_node_lines_is_still_valid() {
        // A node carrying @context alongside other keys is legal JSON-LD and
        // must not trip the lone-context rejection.
        let input = "{\"@id\":\"http://example.org/1\",\"http://schema.org/name\":\"One\"}\n\
                     {\"@context\":{\"s\":\"http://schema.org/\"},\"@id\":\"http://example.org/2\",\"s:name\":\"Two\"}\n";
        let (_ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert_eq!(total_nodes(&chunks), 2);
    }

    #[test]
    fn non_object_first_line_fails_from_constructor() {
        let err = NdjsonReader::new_from_reader(
            reader_for("[{\"@id\":\"ex:1\"}]\n"),
            1_000_000,
            4,
            FirstLineContextPolicy::Auto,
        )
        .err()
        .expect("non-object first line must error");
        assert!(matches!(err, SplitError::InvalidJson(_)));
    }

    #[test]
    fn each_emitted_chunk_is_valid_jsonld() {
        let input = "{\"@context\":{\"ex\":\"http://example.org/\"}}\n\
                     {\"@id\":\"ex:1\",\"ex:desc\":\"has {braces} and ,commas, inside\"}\n\
                     {\"@id\":\"ex:2\"}\n";
        let (_ctx, chunks) = read_all(input, 1_000_000, FirstLineContextPolicy::Auto).unwrap();
        assert_eq!(chunks.len(), 1);
        let graph = chunks[0]["@graph"].as_array().unwrap();
        assert_eq!(graph[0]["ex:desc"], "has {braces} and ,commas, inside");
    }
}
