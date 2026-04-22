//! JSON-LD file splitter for bulk import.
//!
//! Splits large JSON-LD files into independently-parseable chunks for parallel
//! processing. Each emitted chunk is a **complete, valid JSON-LD document** that
//! can be expanded and converted to RDF triples independently.
//!
//! # Supported document shapes
//!
//! | Shape | Structure | Split strategy |
//! |-------|-----------|----------------|
//! | Default graph | `{ "@context": …, "@graph": [entities…] }` | Split within `@graph` at object boundaries |
//! | Named graphs | `{ "@context": …, "@graph": [{ "@id": …, "@graph": […] }, …] }` | Split at named-graph boundaries; large graphs split internally |
//! | Top-level array | `[{ "@context": …, … }, …]` | Split at array element boundaries |
//! | Single object | `{ "@context": …, "@id": …, … }` | No splitting (emitted as-is) |
//!
//! # Core mechanism: depth-tracking byte scanner
//!
//! The splitter uses a byte-level JSON scanner with three states (`Normal`,
//! `InString`, `InEscape`) and a **`u32` depth counter** that tracks brace /
//! bracket nesting. The depth counter is the fundamental correctness mechanism:
//! it determines which `{` / `}` delimit splittable elements vs. nested
//! structure. For example, in a default-graph document the outer `@graph` array
//! sits at depth 2, so element objects open at depth 3. The scanner finds
//! `ObjectEnd(3)` events to identify element boundaries without parsing any
//! values.
//!
//! Because the scanner operates on raw bytes, strings containing structural
//! characters (`{`, `}`, `[`, `]`) are correctly skipped via the `InString` /
//! `InEscape` states — depth only changes in `Normal` state.
//!
//! # Streaming-only design
//!
//! Unlike the Turtle splitter (which offers both a pre-scan `TurtleChunkReader`
//! and a streaming `StreamingTurtleReader`), this module provides only the
//! streaming [`StreamingJsonLdReader`]. A simpler synchronous API is available
//! via [`split_jsonld`], which collects all chunks into a `Vec` without
//! spawning a background thread.
//!
//! The rationale: JSON-LD splitting requires structural scanning to find
//! element boundaries in all cases, so there is no "cheap pre-scan" shortcut
//! analogous to scanning for Turtle statement terminators.
//!
//! # Chunk completeness
//!
//! Every chunk is wrapped with the file's **prefix** (everything from file
//! start through the target array's `[`) and a reconstructed **suffix** (the
//! closing brackets), so each chunk is a standalone JSON-LD document. This
//! differs from the Turtle splitter, where the caller prepends the shared
//! prefix; here, chunks are self-contained to simplify downstream consumers.
//!
//! # Progress reporting
//!
//! Use [`StreamingJsonLdReader::with_progress`] to receive periodic callbacks
//! with `(bytes_scanned, total_file_bytes)` as the file is processed.

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use serde_json::Value as JsonValue;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Errors from JSON-LD splitting.
#[derive(Debug, thiserror::Error)]
pub enum SplitError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Invalid JSON structure: {0}")]
    InvalidJson(String),

    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("File contains no processable data")]
    EmptyData,

    #[error("Channel closed unexpectedly")]
    ChannelClosed,
}

/// Document shape detected during header scanning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentShape {
    /// `{ "@context": …, "@graph": [entity, …] }`
    DefaultGraph,
    /// `{ "@context": …, "@graph": [{ "@id": …, "@graph": […] }, …] }`
    NamedGraphs,
    /// `[obj, …]`
    TopLevelArray,
    /// Single JSON object — no splitting possible.
    SingleObject,
}

/// Extracted context from the JSON-LD header for downstream expansion.
#[derive(Debug, Clone)]
pub struct JsonLdPrelude {
    /// The `@context` value, if present. Feed to `expand_with_context()`.
    pub context: Option<JsonValue>,
}

/// Chunk payload: `(chunk_index, complete_json_ld_document_bytes)`.
///
/// Each `Vec<u8>` is a valid JSON-LD document ready for
/// `serde_json::from_slice` → `expand` → `to_graph_events`.
pub type ChunkPayload = (usize, Vec<u8>);

/// Optional progress callback: `(bytes_scanned, total_file_bytes)`.
pub type ScanProgressFn = Box<dyn Fn(u64, u64) + Send>;

// ---------------------------------------------------------------------------
// StreamingJsonLdReader — public API
// ---------------------------------------------------------------------------

/// Streams a large JSON-LD file as independently-parseable chunks.
///
/// A background reader thread scans the file sequentially, detects object
/// boundaries at the appropriate nesting depth, and emits wrapped chunks
/// through a bounded channel.
pub struct StreamingJsonLdReader {
    prelude: JsonLdPrelude,
    shape: DocumentShape,
    file_size: u64,
    estimated_chunks: usize,
    rx: Arc<Mutex<mpsc::Receiver<ChunkPayload>>>,
    reader_handle: Option<JoinHandle<Result<usize, SplitError>>>,
}

impl StreamingJsonLdReader {
    /// Create a new streaming reader for the given JSON-LD file.
    ///
    /// Scans the file header to detect the document shape and extract
    /// `@context`, then spawns a background thread that emits chunks.
    ///
    /// # Arguments
    /// * `path` — Path to the JSON-LD file
    /// * `chunk_size_bytes` — Target chunk size in bytes (splits at object boundaries ≥ this)
    /// * `channel_capacity` — Bounded channel capacity (controls in-flight memory)
    pub fn new(
        path: &Path,
        chunk_size_bytes: u64,
        channel_capacity: usize,
    ) -> Result<Self, SplitError> {
        Self::with_progress(path, chunk_size_bytes, channel_capacity, None)
    }

    /// Like [`new`](Self::new) but with an optional progress callback.
    pub fn with_progress(
        path: &Path,
        chunk_size_bytes: u64,
        channel_capacity: usize,
        progress: Option<ScanProgressFn>,
    ) -> Result<Self, SplitError> {
        let header = scan_header(path)?;
        let file_size = header.file_size;
        let shape = header.shape;

        let prelude = JsonLdPrelude {
            context: header.context.clone(),
        };

        let estimated_chunks = if file_size == 0 || chunk_size_bytes == 0 {
            1
        } else {
            std::cmp::max(1, (file_size / chunk_size_bytes) as usize)
        };

        // For SingleObject, emit the whole file as one chunk without a
        // background thread.
        if shape == DocumentShape::SingleObject {
            let (tx, rx) = mpsc::sync_channel(1);
            let mut buf = Vec::new();
            File::open(path)?.read_to_end(&mut buf)?;
            let _ = tx.send((0, buf));
            drop(tx);
            return Ok(Self {
                prelude,
                shape,
                file_size,
                estimated_chunks: 1,
                rx: Arc::new(Mutex::new(rx)),
                reader_handle: None,
            });
        }

        let (tx, rx) = mpsc::sync_channel(channel_capacity);
        let owned_path = path.to_path_buf();

        let handle = thread::spawn(move || {
            reader_thread(owned_path, header, chunk_size_bytes, tx, progress)
        });

        Ok(Self {
            prelude,
            shape,
            file_size,
            estimated_chunks,
            rx: Arc::new(Mutex::new(rx)),
            reader_handle: Some(handle),
        })
    }

    /// Receive the next chunk, blocking until available.
    ///
    /// Returns `Ok(None)` when all chunks have been emitted.
    pub fn recv_chunk(&self) -> Result<Option<ChunkPayload>, SplitError> {
        let rx = self.rx.lock().unwrap();
        match rx.recv() {
            Ok(payload) => Ok(Some(payload)),
            Err(_) => Ok(None),
        }
    }

    /// Get a shared receiver for distributing chunks to worker threads.
    pub fn shared_receiver(&self) -> Arc<Mutex<mpsc::Receiver<ChunkPayload>>> {
        Arc::clone(&self.rx)
    }

    /// The extracted `@context` and metadata from the file header.
    pub fn prelude(&self) -> &JsonLdPrelude {
        &self.prelude
    }

    /// The detected document shape.
    pub fn shape(&self) -> DocumentShape {
        self.shape
    }

    /// Total file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Estimated number of chunks (based on file size / chunk size).
    pub fn estimated_chunk_count(&self) -> usize {
        self.estimated_chunks
    }

    /// Wait for the background reader thread to finish.
    ///
    /// Returns the actual number of chunks emitted. Can be called after
    /// draining all chunks via [`recv_chunk`](Self::recv_chunk) to confirm
    /// the thread exited cleanly.
    pub fn join(&mut self) -> Result<usize, SplitError> {
        if let Some(handle) = self.reader_handle.take() {
            handle
                .join()
                .map_err(|_| SplitError::InvalidJson("reader thread panicked".into()))?
        } else {
            // SingleObject path — exactly one chunk.
            Ok(1)
        }
    }
}

// ---------------------------------------------------------------------------
// Synchronous (non-streaming) API
// ---------------------------------------------------------------------------

/// Result of a synchronous split operation.
pub struct SplitResult {
    /// The detected document shape.
    pub shape: DocumentShape,
    /// Extracted `@context` value, if present.
    pub prelude: JsonLdPrelude,
    /// Total file size in bytes.
    pub file_size: u64,
    /// The chunks, each a complete JSON-LD document as raw bytes.
    pub chunks: Vec<Vec<u8>>,
}

/// Split a JSON-LD file into chunks synchronously.
///
/// This is a convenience wrapper around the same scanning logic used by
/// [`StreamingJsonLdReader`], but collects all chunks into a `Vec` on the
/// calling thread without spawning a background thread or channel.
///
/// Use this when you need all chunks in memory at once (e.g., for a
/// non-streaming consumer) or when the file is moderate-sized.
///
/// # Arguments
/// * `path` — Path to the JSON-LD file
/// * `chunk_size_bytes` — Target chunk size in bytes (splits at object boundaries ≥ this)
pub fn split_jsonld(path: &Path, chunk_size_bytes: u64) -> Result<SplitResult, SplitError> {
    let header = scan_header(path)?;
    let file_size = header.file_size;
    let shape = header.shape;
    let prelude = JsonLdPrelude {
        context: header.context.clone(),
    };

    if shape == DocumentShape::SingleObject {
        let mut buf = Vec::new();
        File::open(path)?.read_to_end(&mut buf)?;
        return Ok(SplitResult {
            shape,
            prelude,
            file_size,
            chunks: vec![buf],
        });
    }

    // Use an unbounded-ish channel to collect everything synchronously.
    // We run the reader logic directly on the current thread.
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    reader.seek(SeekFrom::Start(header.array_body_start))?;

    let (tx, rx) = mpsc::sync_channel(64);

    let count = match shape {
        DocumentShape::DefaultGraph | DocumentShape::TopLevelArray => {
            emit_flat_elements(&mut reader, &header, chunk_size_bytes, &tx, &None)?
        }
        DocumentShape::NamedGraphs => {
            emit_named_graphs(&mut reader, &header, chunk_size_bytes, &tx, &None)?
        }
        DocumentShape::SingleObject => unreachable!(),
    };
    drop(tx);

    let mut chunks = Vec::with_capacity(count);
    while let Ok((_idx, bytes)) = rx.recv() {
        chunks.push(bytes);
    }

    Ok(SplitResult {
        shape,
        prelude,
        file_size,
        chunks,
    })
}

// ---------------------------------------------------------------------------
// JSON byte-level scanner
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
enum ScanState {
    Normal,
    InString,
    InEscape,
}

/// Events emitted by the scanner, used internally.
#[derive(Debug, Clone, Copy)]
enum ScanEvent {
    /// Nothing interesting.
    None,
    /// Entered a `"` in Normal state (beginning of a string).
    StringStart,
    /// Exited a `"` in InString state (end of a string).
    StringEnd,
    /// A byte inside a string (for key matching).
    StringByte(u8),
    /// `{` — depth is the new depth after opening.
    ObjectStart(u32),
    /// `}` — depth is the depth before closing (i.e., the depth of this object).
    ObjectEnd(u32),
    /// `[` — depth is the new depth after opening.
    ArrayStart(u32),
    /// `]` — depth is the depth before closing.
    ArrayEnd(u32),
    /// `:` in Normal state.
    Colon,
    /// `,` at the given depth.
    Comma(u32),
}

struct JsonScanner {
    state: ScanState,
    depth: u32,
}

impl JsonScanner {
    fn new() -> Self {
        Self {
            state: ScanState::Normal,
            depth: 0,
        }
    }

    fn new_at_depth(depth: u32) -> Self {
        Self {
            state: ScanState::Normal,
            depth,
        }
    }

    fn advance(&mut self, b: u8) -> ScanEvent {
        match self.state {
            ScanState::InEscape => {
                self.state = ScanState::InString;
                ScanEvent::None
            }
            ScanState::InString => match b {
                b'\\' => {
                    self.state = ScanState::InEscape;
                    ScanEvent::None
                }
                b'"' => {
                    self.state = ScanState::Normal;
                    ScanEvent::StringEnd
                }
                _ => ScanEvent::StringByte(b),
            },
            ScanState::Normal => match b {
                b'"' => {
                    self.state = ScanState::InString;
                    ScanEvent::StringStart
                }
                b'{' => {
                    self.depth += 1;
                    ScanEvent::ObjectStart(self.depth)
                }
                b'}' => {
                    let d = self.depth;
                    self.depth = self.depth.saturating_sub(1);
                    ScanEvent::ObjectEnd(d)
                }
                b'[' => {
                    self.depth += 1;
                    ScanEvent::ArrayStart(self.depth)
                }
                b']' => {
                    let d = self.depth;
                    self.depth = self.depth.saturating_sub(1);
                    ScanEvent::ArrayEnd(d)
                }
                b':' => ScanEvent::Colon,
                b',' => ScanEvent::Comma(self.depth),
                _ => ScanEvent::None,
            },
        }
    }

    fn depth(&self) -> u32 {
        self.depth
    }
}

// ---------------------------------------------------------------------------
// Header scanning
// ---------------------------------------------------------------------------

/// Internal result of scanning the file header.
struct HeaderInfo {
    shape: DocumentShape,
    /// Parsed `@context` value, if present.
    context: Option<JsonValue>,
    /// Raw file bytes from start through the target array's opening `[`.
    prefix_bytes: Vec<u8>,
    /// Reconstructed suffix bytes (closing brackets).
    suffix_bytes: Vec<u8>,
    /// Byte offset in the file where the first array element begins
    /// (just past the `[`).
    array_body_start: u64,
    /// Scanner depth at `array_body_start`.
    scanner_depth_at_body: u32,
    /// For named graphs: the depth at which named-graph `{` objects sit.
    /// For default-graph / top-level array: the depth at which entity `{` sit.
    element_depth: u32,
    file_size: u64,
}

/// A small helper for accumulating a JSON key during byte scanning.
struct KeyAccumulator {
    buf: Vec<u8>,
    active: bool,
}

impl KeyAccumulator {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(32),
            active: false,
        }
    }

    fn start(&mut self) {
        self.buf.clear();
        self.active = true;
    }

    fn push(&mut self, b: u8) {
        if self.active {
            self.buf.push(b);
        }
    }

    fn finish(&mut self) -> Option<Vec<u8>> {
        if self.active {
            self.active = false;
            Some(self.buf.clone())
        } else {
            None
        }
    }

    fn matches(&self, target: &[u8]) -> bool {
        self.buf == target
    }
}

/// Scan the beginning of a JSON-LD file to determine its shape, extract the
/// `@context`, and locate the target array for splitting.
fn scan_header(path: &Path) -> Result<HeaderInfo, SplitError> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();
    if file_size == 0 {
        return Err(SplitError::EmptyData);
    }

    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut scanner = JsonScanner::new();
    let mut key_acc = KeyAccumulator::new();
    let mut prefix_buf: Vec<u8> = Vec::with_capacity(64 * 1024);

    // We track state as we scan through the top-level structure.
    let mut found_document_start = false;
    let mut document_is_array = false;
    let mut awaiting_graph_value = false;
    let mut found_graph_array = false;

    // Context extraction
    let mut awaiting_context_value = false;
    let mut in_context_value = false;
    let mut context_value_depth: u32 = 0;
    let mut context_is_string = false; // true when @context value is a bare string
    let mut context_bytes: Vec<u8> = Vec::new();

    // Track the key string at depth 1 (top-level object keys)
    let mut in_top_key = false;

    // For named-graph detection: peek at first element of @graph array
    let mut peek_first_element = false;
    let mut first_element_depth: u32 = 0;
    let mut first_element_has_id = false;
    let mut first_element_has_graph = false;
    let mut in_first_element_key = false;
    let mut peeking_done = false;

    let mut read_buf = [0u8; 64 * 1024];
    let mut file_pos: u64 = 0;

    // We need to know the position right after the target array's `[`
    let mut array_body_start: u64 = 0;
    let mut scanner_depth_at_body: u32 = 0;

    'outer: loop {
        let n = reader.read(&mut read_buf)?;
        if n == 0 {
            break;
        }

        for (i, &b) in read_buf.iter().enumerate().take(n) {
            let pos = file_pos + i as u64;

            // Capture context value bytes if we're in that mode
            if in_context_value {
                context_bytes.push(b);
            }

            let event = scanner.advance(b);

            // Also feed to key accumulator
            match event {
                ScanEvent::StringByte(sb) => {
                    key_acc.push(sb);
                }
                ScanEvent::StringStart => {
                    if !found_document_start || in_context_value {
                        // Not yet relevant
                    } else if document_is_array {
                        // Top-level array shape — no keys to track
                    } else if scanner.depth() == 1 && !found_graph_array {
                        // We're in the top-level object, this might be a key
                        in_top_key = true;
                        key_acc.start();
                    } else if peek_first_element
                        && scanner.depth() == first_element_depth
                        && !peeking_done
                    {
                        in_first_element_key = true;
                        key_acc.start();
                    }
                }
                ScanEvent::StringEnd => {
                    if in_top_key {
                        in_top_key = false;
                        let key = key_acc.finish();
                        if let Some(k) = &key {
                            if k == b"@graph" {
                                awaiting_graph_value = true;
                            } else if k == b"@context" {
                                awaiting_context_value = true;
                            }
                        }
                    } else if in_first_element_key {
                        in_first_element_key = false;
                        if key_acc.matches(b"@id") {
                            first_element_has_id = true;
                        } else if key_acc.matches(b"@graph") {
                            first_element_has_graph = true;
                        }
                        key_acc.finish();
                        // If we've found both, we can stop peeking
                        if first_element_has_id && first_element_has_graph {
                            peeking_done = true;
                        }
                    } else {
                        key_acc.finish();
                    }
                }
                _ => {}
            }

            // Handle context value tracking
            if awaiting_context_value && !in_context_value {
                match event {
                    ScanEvent::ObjectStart(_)
                    | ScanEvent::ArrayStart(_)
                    | ScanEvent::StringStart => {
                        in_context_value = true;
                        context_value_depth = scanner.depth();
                        context_is_string = matches!(event, ScanEvent::StringStart);
                        context_bytes.clear();
                        context_bytes.push(b);
                        awaiting_context_value = false;
                        // Push to prefix before continue so it's not lost
                        if !found_graph_array {
                            prefix_buf.push(b);
                        }
                        continue;
                    }
                    ScanEvent::Colon => {
                        if !found_graph_array {
                            prefix_buf.push(b);
                        }
                        continue;
                    }
                    ScanEvent::None => {
                        if !found_graph_array {
                            prefix_buf.push(b);
                        }
                        continue;
                    }
                    _ => {}
                }
            }

            // Check if context value is complete
            if in_context_value {
                let ctx_done = match event {
                    ScanEvent::ObjectEnd(d) if d > 0 && scanner.depth() < context_value_depth => {
                        true
                    }
                    ScanEvent::ArrayEnd(d) if d > 0 && scanner.depth() < context_value_depth => {
                        true
                    }
                    ScanEvent::StringEnd
                        if context_is_string && context_value_depth == scanner.depth() =>
                    {
                        true
                    }
                    ScanEvent::Comma(d) if d == 1 && context_value_depth == 0 => {
                        // Simple value (number, boolean, null) ended by comma
                        // Remove the trailing comma from context_bytes
                        context_bytes.pop();
                        true
                    }
                    _ => false,
                };
                if ctx_done {
                    in_context_value = false;
                }
            }

            // Document start detection
            if !found_document_start {
                match event {
                    ScanEvent::ArrayStart(1) => {
                        document_is_array = true;
                        // Top-level array: this is our target array
                        prefix_buf.push(b);
                        array_body_start = pos + 1;
                        scanner_depth_at_body = scanner.depth();
                        found_graph_array = true; // reuse flag to indicate we found target
                        break 'outer;
                    }
                    ScanEvent::ObjectStart(1) => {
                        found_document_start = true;
                        prefix_buf.push(b);
                    }
                    ScanEvent::None => {
                        // Skip BOM / whitespace before document start
                        prefix_buf.push(b);
                    }
                    _ => {
                        return Err(SplitError::InvalidJson(
                            "Expected '{' or '[' at document start".into(),
                        ));
                    }
                }
                continue;
            }

            // Build prefix_buf until we find the target array
            if !found_graph_array {
                prefix_buf.push(b);
            }

            // Look for the @graph array value
            if awaiting_graph_value && !found_graph_array {
                match event {
                    ScanEvent::ArrayStart(2) => {
                        // Found @graph: [
                        found_graph_array = true;
                        awaiting_graph_value = false;
                        array_body_start = pos + 1;
                        scanner_depth_at_body = scanner.depth();

                        // Now peek at first element to detect named graphs
                        peek_first_element = true;
                        first_element_depth = scanner.depth() + 1;
                    }
                    ScanEvent::StringEnd => {
                        // Closing quote of the "@graph" key itself —
                        // awaiting_graph_value was just set on this byte.
                    }
                    ScanEvent::Colon => {
                        // Skip colon after @graph key
                    }
                    ScanEvent::None => {
                        // Skip whitespace
                    }
                    _ => {
                        // @graph value is not an array
                        return Err(SplitError::InvalidJson(
                            "@graph value must be an array".into(),
                        ));
                    }
                }
                continue;
            }

            // Peek at first element for named-graph detection
            if peek_first_element && !peeking_done {
                match event {
                    ScanEvent::ObjectEnd(d) if d == first_element_depth => {
                        // First element closed — we've seen all its keys
                        peeking_done = true;
                        break 'outer;
                    }
                    ScanEvent::ArrayEnd(d) if d == scanner_depth_at_body => {
                        // Empty @graph array
                        peeking_done = true;
                        break 'outer;
                    }
                    _ => {}
                }

                // We need to read through the ENTIRE first element to check
                // for @id and @graph keys. But we only need top-level keys of
                // the first element (at first_element_depth), so skip nested
                // content.
            }
        }

        file_pos += n as u64;
    }

    // Determine shape
    let shape = if document_is_array {
        DocumentShape::TopLevelArray
    } else if !found_graph_array {
        // No @graph array found — treat as single object
        DocumentShape::SingleObject
    } else if peeking_done && first_element_has_id && first_element_has_graph {
        DocumentShape::NamedGraphs
    } else {
        DocumentShape::DefaultGraph
    };

    // Parse context from extracted bytes
    let context = if !context_bytes.is_empty() {
        Some(serde_json::from_slice(&context_bytes)?)
    } else {
        None
    };

    // Build suffix
    let suffix_bytes = match shape {
        DocumentShape::DefaultGraph | DocumentShape::NamedGraphs => b"\n]\n}".to_vec(),
        DocumentShape::TopLevelArray => b"\n]".to_vec(),
        DocumentShape::SingleObject => Vec::new(),
    };

    // Element depth: the depth at which `{` of splittable elements appear
    let element_depth = match shape {
        DocumentShape::TopLevelArray => scanner_depth_at_body + 1, // depth 2
        DocumentShape::DefaultGraph => scanner_depth_at_body + 1,  // depth 3
        DocumentShape::NamedGraphs => scanner_depth_at_body + 1,   // depth 3 (outer)
        DocumentShape::SingleObject => 0,
    };

    Ok(HeaderInfo {
        shape,
        context,
        prefix_bytes: prefix_buf,
        suffix_bytes,
        array_body_start,
        scanner_depth_at_body,
        element_depth,
        file_size,
    })
}

// ---------------------------------------------------------------------------
// Reader thread
// ---------------------------------------------------------------------------

/// Background reader that scans the file and emits wrapped chunks.
fn reader_thread(
    path: PathBuf,
    header: HeaderInfo,
    chunk_size: u64,
    tx: mpsc::SyncSender<ChunkPayload>,
    progress: Option<ScanProgressFn>,
) -> Result<usize, SplitError> {
    let file = File::open(&path)?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    reader.seek(SeekFrom::Start(header.array_body_start))?;

    match header.shape {
        DocumentShape::DefaultGraph | DocumentShape::TopLevelArray => {
            emit_flat_elements(&mut reader, &header, chunk_size, &tx, &progress)
        }
        DocumentShape::NamedGraphs => {
            emit_named_graphs(&mut reader, &header, chunk_size, &tx, &progress)
        }
        DocumentShape::SingleObject => {
            // Should not reach here — handled in StreamingJsonLdReader::new
            Ok(1)
        }
    }
}

/// Assemble a complete JSON-LD document from prefix + element bytes + suffix.
fn assemble_doc(prefix: &[u8], elements: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut doc = Vec::with_capacity(prefix.len() + elements.len() + suffix.len() + 2);
    doc.extend_from_slice(prefix);
    doc.push(b'\n');
    doc.extend_from_slice(elements);
    doc.extend_from_slice(suffix);
    doc
}

/// Emit chunks for Shape A (default graph) and Shape C (top-level array).
///
/// Scans the target array for complete `{…}` objects at `element_depth` and
/// groups them into chunks of approximately `chunk_size` bytes.
fn emit_flat_elements(
    reader: &mut BufReader<File>,
    header: &HeaderInfo,
    chunk_size: u64,
    tx: &mpsc::SyncSender<ChunkPayload>,
    progress: &Option<ScanProgressFn>,
) -> Result<usize, SplitError> {
    let mut scanner = JsonScanner::new_at_depth(header.scanner_depth_at_body);
    let target_depth = header.element_depth;
    let array_depth = header.scanner_depth_at_body;

    let mut chunk_buf: Vec<u8> = Vec::with_capacity(chunk_size as usize);
    let mut chunk_idx: usize = 0;
    let mut element_count: usize = 0;
    let mut in_element = false;

    let mut read_buf = vec![0u8; 256 * 1024];
    let mut bytes_scanned: u64 = header.array_body_start;
    let mut last_progress: u64 = 0;

    loop {
        let n = reader.read(&mut read_buf)?;
        if n == 0 {
            break;
        }

        for &b in &read_buf[..n] {
            let event = scanner.advance(b);

            match event {
                ScanEvent::ObjectStart(d) if d == target_depth && !in_element => {
                    if element_count > 0 {
                        chunk_buf.extend_from_slice(b",\n");
                    }
                    chunk_buf.push(b);
                    in_element = true;
                }
                ScanEvent::ObjectEnd(d) if d == target_depth && in_element => {
                    chunk_buf.push(b);
                    in_element = false;
                    element_count += 1;

                    if chunk_buf.len() as u64 >= chunk_size {
                        let doc =
                            assemble_doc(&header.prefix_bytes, &chunk_buf, &header.suffix_bytes);
                        tx.send((chunk_idx, doc))
                            .map_err(|_| SplitError::ChannelClosed)?;
                        chunk_idx += 1;
                        chunk_buf.clear();
                        element_count = 0;
                    }
                }
                ScanEvent::ArrayEnd(d) if d == array_depth && !in_element => {
                    // Target array closed — emit remaining elements
                    if !chunk_buf.is_empty() {
                        let doc =
                            assemble_doc(&header.prefix_bytes, &chunk_buf, &header.suffix_bytes);
                        tx.send((chunk_idx, doc))
                            .map_err(|_| SplitError::ChannelClosed)?;
                        chunk_idx += 1;
                    }
                    return Ok(chunk_idx);
                }
                _ if in_element => {
                    chunk_buf.push(b);
                }
                _ => {
                    // Inter-element whitespace / commas — skip
                }
            }
        }

        bytes_scanned += n as u64;
        if let Some(ref cb) = progress {
            if bytes_scanned - last_progress > 64 * 1024 * 1024 {
                cb(bytes_scanned, header.file_size);
                last_progress = bytes_scanned;
            }
        }
    }

    // If we get here without seeing ArrayEnd, emit whatever's left
    if !chunk_buf.is_empty() {
        let doc = assemble_doc(&header.prefix_bytes, &chunk_buf, &header.suffix_bytes);
        tx.send((chunk_idx, doc))
            .map_err(|_| SplitError::ChannelClosed)?;
        chunk_idx += 1;
    }

    Ok(chunk_idx)
}

/// Emit chunks for Shape B (named graphs).
///
/// Iterates named-graph objects in the outer `@graph` array. Each named graph
/// always starts a new chunk. If a named graph's inner `@graph` array exceeds
/// `chunk_size`, it is further split at entity boundaries within that inner
/// array.
fn emit_named_graphs(
    reader: &mut BufReader<File>,
    header: &HeaderInfo,
    chunk_size: u64,
    tx: &mpsc::SyncSender<ChunkPayload>,
    progress: &Option<ScanProgressFn>,
) -> Result<usize, SplitError> {
    let mut scanner = JsonScanner::new_at_depth(header.scanner_depth_at_body);
    let ng_object_depth = header.element_depth; // depth of named-graph `{`
    let outer_array_depth = header.scanner_depth_at_body; // depth of outer `[`

    let mut chunk_idx: usize = 0;

    // Buffer for the current named-graph object
    let mut ng_buf: Vec<u8> = Vec::with_capacity(chunk_size as usize);
    let mut in_ng_object = false;
    let mut skip_ng_remainder = false; // after inner splitting, skip to `}`

    // Tracking for inner @graph array splitting
    let mut awaiting_inner_graph_value = false;
    let mut in_inner_array = false;
    let mut inner_array_depth: u32 = 0;
    let mut inner_element_depth: u32 = 0;

    // Key detection within named-graph header
    let mut in_ng_key_string = false;
    let mut ng_key_buf: Vec<u8> = Vec::with_capacity(16);

    // Inner element accumulation (for splitting large named graphs)
    let mut ng_prefix: Vec<u8> = Vec::new(); // outer_prefix + ng header through inner `[`
    let mut ng_suffix: Vec<u8> = Vec::new(); // `]}` + outer_suffix
    let mut inner_chunk_buf: Vec<u8> = Vec::new();
    let mut inner_element_count: usize = 0;
    let mut in_inner_element = false;

    let mut read_buf = vec![0u8; 256 * 1024];
    let mut bytes_scanned: u64 = header.array_body_start;
    let mut last_progress: u64 = 0;

    loop {
        let n = reader.read(&mut read_buf)?;
        if n == 0 {
            break;
        }

        for &b in &read_buf[..n] {
            let event = scanner.advance(b);

            // --- Phase: inside an inner @graph array (splitting a large named graph) ---
            if in_inner_array {
                match event {
                    ScanEvent::ObjectStart(d) if d == inner_element_depth && !in_inner_element => {
                        if inner_element_count > 0 {
                            inner_chunk_buf.extend_from_slice(b",\n");
                        }
                        inner_chunk_buf.push(b);
                        in_inner_element = true;
                    }
                    ScanEvent::ObjectEnd(d) if d == inner_element_depth && in_inner_element => {
                        inner_chunk_buf.push(b);
                        in_inner_element = false;
                        inner_element_count += 1;

                        if inner_chunk_buf.len() as u64 >= chunk_size {
                            let doc = assemble_doc(&ng_prefix, &inner_chunk_buf, &ng_suffix);
                            tx.send((chunk_idx, doc))
                                .map_err(|_| SplitError::ChannelClosed)?;
                            chunk_idx += 1;
                            inner_chunk_buf.clear();
                            inner_element_count = 0;
                        }
                    }
                    ScanEvent::ArrayEnd(d) if d == inner_array_depth && !in_inner_element => {
                        // Inner array closed — emit remaining
                        if !inner_chunk_buf.is_empty() {
                            let doc = assemble_doc(&ng_prefix, &inner_chunk_buf, &ng_suffix);
                            tx.send((chunk_idx, doc))
                                .map_err(|_| SplitError::ChannelClosed)?;
                            chunk_idx += 1;
                            inner_chunk_buf.clear();
                            inner_element_count = 0;
                        }
                        in_inner_array = false;
                        skip_ng_remainder = true;
                    }
                    _ if in_inner_element => {
                        inner_chunk_buf.push(b);
                    }
                    _ => {
                        // Inter-element whitespace/commas in inner array — skip
                    }
                }
                continue;
            }

            // --- Phase: inside a named-graph object header (looking for inner @graph) ---
            if in_ng_object && !in_inner_array {
                // After inner array splitting, consume bytes until the
                // named-graph object's closing `}`.
                if skip_ng_remainder {
                    match event {
                        ScanEvent::ObjectEnd(d) if d == ng_object_depth => {
                            in_ng_object = false;
                            skip_ng_remainder = false;
                            ng_buf.clear();
                        }
                        _ => {}
                    }
                    continue;
                }

                ng_buf.push(b);

                // Key detection at the named-graph object depth
                match event {
                    ScanEvent::StringStart if scanner.depth() == ng_object_depth => {
                        in_ng_key_string = true;
                        ng_key_buf.clear();
                    }
                    ScanEvent::StringByte(sb) if in_ng_key_string => {
                        ng_key_buf.push(sb);
                    }
                    ScanEvent::StringEnd if in_ng_key_string => {
                        in_ng_key_string = false;
                        if ng_key_buf == b"@graph" {
                            awaiting_inner_graph_value = true;
                        }
                    }
                    _ => {}
                }

                // Detect inner @graph array
                if awaiting_inner_graph_value {
                    match event {
                        ScanEvent::ArrayStart(d) if d == ng_object_depth + 1 => {
                            awaiting_inner_graph_value = false;

                            // Check: is this named graph large enough to split?
                            // We decide based on ng_buf accumulated so far.
                            // If ng_buf is already close to chunk_size, this NG is
                            // likely large and worth splitting.
                            //
                            // But we can't know the total size yet. Strategy: always
                            // prepare for splitting (record ng_prefix), then either:
                            // - If the inner array is small, emit the whole NG as one chunk
                            // - If large, emit inner chunks as we go

                            // Build ng_prefix: outer_prefix + ng_header (includes inner `[`)
                            ng_prefix.clear();
                            ng_prefix.extend_from_slice(&header.prefix_bytes);
                            ng_prefix.push(b'\n');
                            ng_prefix.extend_from_slice(&ng_buf);

                            // Build ng_suffix: `\n]}\n` + outer_suffix
                            ng_suffix.clear();
                            ng_suffix.extend_from_slice(b"\n]}");
                            ng_suffix.extend_from_slice(&header.suffix_bytes);

                            in_inner_array = true;
                            inner_array_depth = d;
                            inner_element_depth = d + 1;
                            inner_chunk_buf.clear();
                            inner_element_count = 0;
                            in_inner_element = false;
                        }
                        ScanEvent::StringEnd | ScanEvent::Colon | ScanEvent::None => {
                            // StringEnd: closing quote of "@graph" key
                            // Colon/None: separator/whitespace after key
                        }
                        _ => {
                            // @graph value is not an array — treat this NG
                            // as an opaque object (no inner splitting)
                            awaiting_inner_graph_value = false;
                        }
                    }
                }

                // If the named-graph object closes without inner @graph splitting
                if !in_inner_array {
                    match event {
                        ScanEvent::ObjectEnd(d) if d == ng_object_depth => {
                            // Entire NG object in ng_buf — emit as one chunk
                            let doc =
                                assemble_doc(&header.prefix_bytes, &ng_buf, &header.suffix_bytes);
                            tx.send((chunk_idx, doc))
                                .map_err(|_| SplitError::ChannelClosed)?;
                            chunk_idx += 1;

                            // Reset for next named graph
                            ng_buf.clear();
                            in_ng_object = false;
                            awaiting_inner_graph_value = false;
                            in_ng_key_string = false;
                        }
                        _ => {}
                    }
                } else {
                    // We've transitioned to inner array mode — ng_buf is no longer
                    // needed since ng_prefix captured the header. Clear it.
                    ng_buf.clear();
                }

                continue;
            }

            // --- Phase: scanning outer @graph array for named-graph objects ---
            match event {
                ScanEvent::ObjectStart(d) if d == ng_object_depth && !in_ng_object => {
                    ng_buf.clear();
                    ng_buf.push(b);
                    in_ng_object = true;
                    awaiting_inner_graph_value = false;
                    in_ng_key_string = false;
                }
                ScanEvent::ArrayEnd(d) if d == outer_array_depth => {
                    // Outer @graph array closed — done
                    return Ok(chunk_idx);
                }
                _ => {
                    // Whitespace/commas between named-graph objects — skip
                }
            }
        }

        bytes_scanned += n as u64;
        if let Some(ref cb) = progress {
            if bytes_scanned - last_progress > 64 * 1024 * 1024 {
                cb(bytes_scanned, header.file_size);
                last_progress = bytes_scanned;
            }
        }
    }

    // File ended — emit anything remaining
    if !inner_chunk_buf.is_empty() {
        let doc = assemble_doc(&ng_prefix, &inner_chunk_buf, &ng_suffix);
        tx.send((chunk_idx, doc))
            .map_err(|_| SplitError::ChannelClosed)?;
        chunk_idx += 1;
    } else if !ng_buf.is_empty() {
        let doc = assemble_doc(&header.prefix_bytes, &ng_buf, &header.suffix_bytes);
        tx.send((chunk_idx, doc))
            .map_err(|_| SplitError::ChannelClosed)?;
        chunk_idx += 1;
    }

    Ok(chunk_idx)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// RAII temp file that deletes on drop.
    struct TempJsonFile {
        path: PathBuf,
    }

    impl TempJsonFile {
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempJsonFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    /// Helper: write JSON to a temp file and return a guard that cleans up.
    ///
    /// Includes PID in the filename so that nextest (which runs each test in
    /// a separate process, each starting COUNTER at 0) doesn't collide.
    fn write_temp_json(json: &str) -> TempJsonFile {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("fluree_jsonld_split_test_{pid}_{id}.json"));
        let mut f = File::create(&path).unwrap();
        f.write_all(json.as_bytes()).unwrap();
        f.flush().unwrap();
        TempJsonFile { path }
    }

    /// Helper: collect all chunks from a reader.
    fn collect_chunks(reader: StreamingJsonLdReader) -> Vec<(usize, serde_json::Value)> {
        let mut chunks = Vec::new();
        while let Ok(Some((idx, bytes))) = reader.recv_chunk() {
            let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            chunks.push((idx, val));
        }
        chunks
    }

    // ---- Shape detection ----

    #[test]
    fn detect_default_graph_shape() {
        let json = r#"{"@context":{"ex":"http://example.org/"},"@graph":[{"@id":"ex:1"}]}"#;
        let f = write_temp_json(json);
        let header = scan_header(f.path()).unwrap();
        assert_eq!(header.shape, DocumentShape::DefaultGraph);
        assert!(header.context.is_some());
    }

    #[test]
    fn detect_named_graphs_shape() {
        let json = r#"{
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:g1", "@graph": [{"@id": "ex:1"}]},
                {"@id": "ex:g2", "@graph": [{"@id": "ex:2"}]}
            ]
        }"#;
        let f = write_temp_json(json);
        let header = scan_header(f.path()).unwrap();
        assert_eq!(header.shape, DocumentShape::NamedGraphs);
    }

    #[test]
    fn detect_top_level_array_shape() {
        let json = r#"[{"@id":"ex:1","@type":"ex:Thing"},{"@id":"ex:2"}]"#;
        let f = write_temp_json(json);
        let header = scan_header(f.path()).unwrap();
        assert_eq!(header.shape, DocumentShape::TopLevelArray);
        assert!(header.context.is_none());
    }

    #[test]
    fn detect_single_object_shape() {
        let json = r#"{"@context":{"ex":"http://example.org/"},"@id":"ex:1","ex:name":"Alice"}"#;
        let f = write_temp_json(json);
        let header = scan_header(f.path()).unwrap();
        assert_eq!(header.shape, DocumentShape::SingleObject);
    }

    // ---- Default graph splitting ----

    #[test]
    fn default_graph_no_split_when_small() {
        let json = r#"{
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:1", "ex:name": "Alice"},
                {"@id": "ex:2", "ex:name": "Bob"}
            ]
        }"#;
        let f = write_temp_json(json);
        // chunk_size larger than file → one chunk
        let reader = StreamingJsonLdReader::new(f.path(), 1_000_000, 4).unwrap();
        assert_eq!(reader.shape(), DocumentShape::DefaultGraph);
        let chunks = collect_chunks(reader);
        assert_eq!(chunks.len(), 1);

        // Verify it's valid JSON-LD with both entities
        let graph = chunks[0].1["@graph"].as_array().unwrap();
        assert_eq!(graph.len(), 2);
    }

    #[test]
    fn default_graph_splits_at_object_boundaries() {
        // Create a document with several entities, use a tiny chunk size
        let json = r#"{
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:1", "ex:name": "Alice"},
                {"@id": "ex:2", "ex:name": "Bob"},
                {"@id": "ex:3", "ex:name": "Carol"},
                {"@id": "ex:4", "ex:name": "Dave"}
            ]
        }"#;
        let f = write_temp_json(json);
        // Very small chunk size to force splitting
        let reader = StreamingJsonLdReader::new(f.path(), 40, 4).unwrap();
        let chunks = collect_chunks(reader);

        // Should have multiple chunks
        assert!(
            chunks.len() > 1,
            "Expected multiple chunks, got {}",
            chunks.len()
        );

        // Each chunk should be valid JSON-LD with @context and @graph
        let mut total_entities = 0;
        for (idx, val) in &chunks {
            assert!(val.get("@graph").is_some(), "Chunk {idx} missing @graph");
            let graph = val["@graph"].as_array().unwrap();
            assert!(!graph.is_empty(), "Chunk {idx} has empty @graph");
            total_entities += graph.len();
        }
        assert_eq!(total_entities, 4);
    }

    #[test]
    fn default_graph_preserves_top_level_metadata() {
        let json = r#"{
            "@context": {"ex": "http://example.org/"},
            "@id": "http://example.org/dataset",
            "@graph": [
                {"@id": "ex:1", "ex:name": "Alice"},
                {"@id": "ex:2", "ex:name": "Bob"}
            ]
        }"#;
        let f = write_temp_json(json);
        let reader = StreamingJsonLdReader::new(f.path(), 40, 4).unwrap();
        let chunks = collect_chunks(reader);

        // Every chunk should preserve the @id and @context
        for (idx, val) in &chunks {
            assert!(
                val.get("@context").is_some(),
                "Chunk {idx} missing @context"
            );
        }
    }

    // ---- Named graph splitting ----

    #[test]
    fn named_graphs_split_at_graph_boundaries() {
        let json = r#"{
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {
                    "@id": "ex:graph1",
                    "ex:label": "Graph 1",
                    "@graph": [{"@id": "ex:1", "ex:name": "Alice"}]
                },
                {
                    "@id": "ex:graph2",
                    "ex:label": "Graph 2",
                    "@graph": [{"@id": "ex:2", "ex:name": "Bob"}]
                }
            ]
        }"#;
        let f = write_temp_json(json);
        let reader = StreamingJsonLdReader::new(f.path(), 1_000_000, 4).unwrap();
        assert_eq!(reader.shape(), DocumentShape::NamedGraphs);
        let chunks = collect_chunks(reader);

        // Each named graph should be its own chunk
        assert_eq!(chunks.len(), 2);

        // Each chunk should have @context and one named graph
        for (_, val) in &chunks {
            let graph = val["@graph"].as_array().unwrap();
            assert_eq!(graph.len(), 1);
            assert!(graph[0].get("@id").is_some());
            assert!(graph[0].get("@graph").is_some());
        }
    }

    #[test]
    fn named_graph_preserves_metadata() {
        let json = r#"{
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {
                    "@id": "ex:graph1",
                    "ex:created": "2024-01-01",
                    "ex:author": "Alice",
                    "@graph": [{"@id": "ex:1"}]
                }
            ]
        }"#;
        let f = write_temp_json(json);
        let reader = StreamingJsonLdReader::new(f.path(), 1_000_000, 4).unwrap();
        let chunks = collect_chunks(reader);
        assert_eq!(chunks.len(), 1);

        let ng = &chunks[0].1["@graph"].as_array().unwrap()[0];
        assert_eq!(ng["@id"], "ex:graph1");
        // Metadata keys should be preserved
        assert!(ng.get("ex:created").is_some() || ng.get("ex:author").is_some());
    }

    #[test]
    fn large_named_graph_splits_internally() {
        // One named graph with many entities
        let mut entities = Vec::new();
        for i in 0..20 {
            entities.push(format!(
                r#"{{"@id": "ex:entity{}", "ex:name": "Entity {}", "ex:value": "{}"}}"#,
                i,
                i,
                "x".repeat(50)
            ));
        }
        let json = format!(
            r#"{{
                "@context": {{"ex": "http://example.org/"}},
                "@graph": [{{
                    "@id": "ex:bigGraph",
                    "ex:label": "Big Graph",
                    "@graph": [{}]
                }}]
            }}"#,
            entities.join(",\n")
        );
        let f = write_temp_json(&json);

        // Small chunk size to force splitting within the named graph
        let reader = StreamingJsonLdReader::new(f.path(), 200, 4).unwrap();
        assert_eq!(reader.shape(), DocumentShape::NamedGraphs);
        let chunks = collect_chunks(reader);

        // Should have multiple chunks from the single named graph
        assert!(
            chunks.len() > 1,
            "Expected multiple chunks from large named graph, got {}",
            chunks.len()
        );

        // Each chunk should wrap the named graph structure
        let mut total_entities = 0;
        for (idx, val) in &chunks {
            let outer_graph = val["@graph"].as_array().unwrap();
            assert_eq!(outer_graph.len(), 1, "Chunk {idx} outer @graph len");
            let ng = &outer_graph[0];
            assert_eq!(ng["@id"], "ex:bigGraph");
            let inner = ng["@graph"].as_array().unwrap();
            assert!(!inner.is_empty(), "Chunk {idx} inner @graph empty");
            total_entities += inner.len();
        }
        assert_eq!(total_entities, 20);
    }

    // ---- Top-level array ----

    #[test]
    fn top_level_array_splits() {
        let json = r#"[
            {"@id": "ex:1", "http://example.org/name": "Alice"},
            {"@id": "ex:2", "http://example.org/name": "Bob"},
            {"@id": "ex:3", "http://example.org/name": "Carol"}
        ]"#;
        let f = write_temp_json(json);
        let reader = StreamingJsonLdReader::new(f.path(), 60, 4).unwrap();
        assert_eq!(reader.shape(), DocumentShape::TopLevelArray);
        let chunks = collect_chunks(reader);

        assert!(chunks.len() > 1);
        let mut total = 0;
        for (_, val) in &chunks {
            let arr = val.as_array().unwrap();
            total += arr.len();
        }
        assert_eq!(total, 3);
    }

    // ---- Single object ----

    #[test]
    fn single_object_emits_one_chunk() {
        let json = r#"{"@context":{"ex":"http://example.org/"},"@id":"ex:1","ex:name":"Alice"}"#;
        let f = write_temp_json(json);
        let reader = StreamingJsonLdReader::new(f.path(), 10, 4).unwrap();
        assert_eq!(reader.shape(), DocumentShape::SingleObject);
        let chunks = collect_chunks(reader);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].1["@id"], "ex:1");
    }

    // ---- Context extraction ----

    #[test]
    fn extracts_object_context() {
        let json = r#"{"@context":{"ex":"http://example.org/","name":"ex:name"},"@graph":[{"@id":"ex:1"}]}"#;
        let f = write_temp_json(json);
        let header = scan_header(f.path()).unwrap();
        let ctx = header.context.unwrap();
        assert_eq!(ctx["ex"], "http://example.org/");
        assert_eq!(ctx["name"], "ex:name");
    }

    #[test]
    fn extracts_string_context() {
        let json = r#"{"@context":"http://schema.org/","@graph":[{"@id":"ex:1"}]}"#;
        let f = write_temp_json(json);
        let header = scan_header(f.path()).unwrap();
        let ctx = header.context.unwrap();
        assert_eq!(ctx, "http://schema.org/");
    }

    #[test]
    fn extracts_array_context() {
        let json = r#"{"@context":["http://schema.org/",{"ex":"http://example.org/"}],"@graph":[{"@id":"ex:1"}]}"#;
        let f = write_temp_json(json);
        let header = scan_header(f.path()).unwrap();
        let ctx = header.context.unwrap();
        assert!(ctx.is_array());
        assert_eq!(ctx.as_array().unwrap().len(), 2);
    }

    #[test]
    fn no_context_is_ok() {
        let json = r#"[{"@id":"http://example.org/1"}]"#;
        let f = write_temp_json(json);
        let header = scan_header(f.path()).unwrap();
        assert!(header.context.is_none());
    }

    // ---- Edge cases ----

    #[test]
    fn handles_strings_with_braces() {
        let json = r#"{
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:1", "ex:desc": "has {braces} inside"},
                {"@id": "ex:2", "ex:desc": "also [brackets] and \"quotes\""}
            ]
        }"#;
        let f = write_temp_json(json);
        let reader = StreamingJsonLdReader::new(f.path(), 1_000_000, 4).unwrap();
        let chunks = collect_chunks(reader);
        assert_eq!(chunks.len(), 1);
        let graph = chunks[0].1["@graph"].as_array().unwrap();
        assert_eq!(graph.len(), 2);
        assert_eq!(graph[0]["ex:desc"], "has {braces} inside");
    }

    #[test]
    fn handles_escaped_quotes_in_strings() {
        let json =
            r#"{"@context":{},"@graph":[{"@id":"ex:1","ex:v":"line1\"line2"},{"@id":"ex:2"}]}"#;
        let f = write_temp_json(json);
        let reader = StreamingJsonLdReader::new(f.path(), 40, 4).unwrap();
        let chunks = collect_chunks(reader);
        let mut total = 0;
        for (_, val) in &chunks {
            total += val["@graph"].as_array().unwrap().len();
        }
        assert_eq!(total, 2);
    }

    #[test]
    fn handles_unicode_escapes() {
        let json = r#"{"@context":{},"@graph":[{"@id":"ex:1","ex:v":"\u0022escaped\u0022"},{"@id":"ex:2"}]}"#;
        let f = write_temp_json(json);
        let reader = StreamingJsonLdReader::new(f.path(), 1_000_000, 4).unwrap();
        let chunks = collect_chunks(reader);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn empty_graph_array() {
        let json = r#"{"@context":{"ex":"http://example.org/"},"@graph":[]}"#;
        let f = write_temp_json(json);
        let reader = StreamingJsonLdReader::new(f.path(), 1_000_000, 4).unwrap();
        let chunks = collect_chunks(reader);
        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn join_returns_chunk_count() {
        let json = r#"{"@context":{},"@graph":[{"@id":"ex:1"},{"@id":"ex:2"},{"@id":"ex:3"}]}"#;
        let f = write_temp_json(json);
        let mut reader = StreamingJsonLdReader::new(f.path(), 20, 4).unwrap();
        // Drain the channel first
        while reader.recv_chunk().unwrap().is_some() {}
        let count = reader.join().unwrap();
        assert!(count > 0);
    }

    // ---- Scanner unit tests ----

    #[test]
    fn scanner_tracks_depth() {
        let mut s = JsonScanner::new();
        assert_eq!(s.depth(), 0);

        s.advance(b'{');
        assert_eq!(s.depth(), 1);

        s.advance(b'[');
        assert_eq!(s.depth(), 2);

        s.advance(b']');
        assert_eq!(s.depth(), 1);

        s.advance(b'}');
        assert_eq!(s.depth(), 0);
    }

    #[test]
    fn scanner_ignores_braces_in_strings() {
        let mut s = JsonScanner::new();
        s.advance(b'{'); // depth 1
        s.advance(b'"'); // enter string
        s.advance(b'{'); // inside string — ignored
        s.advance(b'}'); // inside string — ignored
        assert_eq!(s.depth(), 1); // still 1
        s.advance(b'"'); // exit string
        s.advance(b'}'); // depth 0
        assert_eq!(s.depth(), 0);
    }

    #[test]
    fn scanner_handles_escapes() {
        let mut s = JsonScanner::new();
        s.advance(b'"'); // enter string
        s.advance(b'\\'); // escape
        s.advance(b'"'); // escaped quote — NOT end of string
        assert_eq!(s.state, ScanState::InString);
        s.advance(b'"'); // actual end of string
        assert_eq!(s.state, ScanState::Normal);
    }

    // ---- Synchronous split_jsonld API ----

    #[test]
    fn split_jsonld_default_graph() {
        let json = r#"{
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:1", "ex:name": "Alice"},
                {"@id": "ex:2", "ex:name": "Bob"},
                {"@id": "ex:3", "ex:name": "Carol"}
            ]
        }"#;
        let f = write_temp_json(json);
        let result = split_jsonld(f.path(), 40).unwrap();
        assert_eq!(result.shape, DocumentShape::DefaultGraph);
        assert!(result.chunks.len() > 1);

        let mut total = 0;
        for bytes in &result.chunks {
            let val: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            total += val["@graph"].as_array().unwrap().len();
        }
        assert_eq!(total, 3);
    }

    #[test]
    fn split_jsonld_single_object() {
        let json = r#"{"@context":{},"@id":"ex:1","ex:name":"Alice"}"#;
        let f = write_temp_json(json);
        let result = split_jsonld(f.path(), 10).unwrap();
        assert_eq!(result.shape, DocumentShape::SingleObject);
        assert_eq!(result.chunks.len(), 1);
    }

    #[test]
    fn split_jsonld_no_split_when_large_threshold() {
        let json = r#"{"@context":{},"@graph":[{"@id":"ex:1"},{"@id":"ex:2"}]}"#;
        let f = write_temp_json(json);
        let result = split_jsonld(f.path(), 1_000_000).unwrap();
        assert_eq!(result.chunks.len(), 1);
    }
}
