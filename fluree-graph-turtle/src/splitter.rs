//! Turtle file splitter for large-file import.
//!
//! Splits a single large `.ttl` file into chunk byte ranges, each representing
//! a set of complete Turtle statements. Every chunk is independently parseable
//! because the prefix block from the file header is prepended on read.
//!
//! ## Design
//!
//! 1. **Prefix extraction** — tokenize the first 1 MB with `fluree_graph_turtle::tokenize()`
//!    to locate all `@prefix` / `@base` / `PREFIX` / `BASE` directives. The raw source text
//!    of these directives (including interleaved comments) is captured verbatim.
//!
//! 2. **Pre-scan** — a single sequential pass through the file with a lightweight state
//!    machine that tracks string literal, IRI, and comment context. Statement boundaries
//!    (`.` followed by whitespace, `#`, or EOF in `Normal` state) are recorded at byte
//!    offsets. The first boundary after each `chunk_size` multiple becomes a chunk split.
//!
//! 3. **Chunk reading** — each chunk opens its own file handle, seeks to the byte range,
//!    reads the raw bytes, and prepends the prefix block. The result is a valid Turtle
//!    document.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{parse, tokenize, TokenKind};
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for splitting a large Turtle file.
#[derive(Debug, Clone)]
pub struct TurtleSplitConfig {
    /// Target chunk size in bytes. Actual chunks may be slightly larger because
    /// splits only occur at statement boundaries.
    pub chunk_size_bytes: u64,
}

// ============================================================================
// Errors
// ============================================================================

/// Errors specific to Turtle file splitting.
#[derive(Debug, thiserror::Error)]
pub enum SplitError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Turtle tokenization error: {0}")]
    Tokenize(String),

    #[error("prefix/base directive found after data at byte offset {offset}; all directives must appear in the file header")]
    PrefixAfterData { offset: u64 },

    #[error("no statement boundary found within 64 MB of target at byte offset {offset}; possible unterminated statement or corrupt Turtle")]
    NoBoundary { offset: u64 },

    #[error("file is empty or contains only prefix directives")]
    EmptyData,
}

// ============================================================================
// ScanState — lightweight state machine for boundary detection
// ============================================================================

/// Parser context for the byte-level pre-scan.
///
/// Tracks whether we are inside a string literal, IRI, or comment so that
/// `.` characters in those contexts are correctly ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanState {
    /// Outside any literal, IRI, or comment.
    Normal,
    /// Inside `"..."` (short double-quoted string).
    InShortDoubleString,
    /// Inside `"""..."""` (long double-quoted string).
    InLongDoubleString,
    /// Inside `'...'` (short single-quoted string).
    InShortSingleString,
    /// Inside `'''...'''` (long single-quoted string).
    InLongSingleString,
    /// After `\` inside a string. `return_to` encoded as discriminant.
    InStringEscape { return_to: EscapeReturn },
    /// Inside `<...>` (IRI reference).
    InIri,
    /// After `#` until end of line.
    InComment,
}

/// Which string state to return to after processing an escape character.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EscapeReturn {
    ShortDouble,
    LongDouble,
    ShortSingle,
    LongSingle,
}

impl EscapeReturn {
    fn to_state(self) -> ScanState {
        match self {
            EscapeReturn::ShortDouble => ScanState::InShortDoubleString,
            EscapeReturn::LongDouble => ScanState::InLongDoubleString,
            EscapeReturn::ShortSingle => ScanState::InShortSingleString,
            EscapeReturn::LongSingle => ScanState::InLongSingleString,
        }
    }
}

// ============================================================================
// Prefix block extraction
// ============================================================================

/// Size of the header region to tokenize for prefix extraction.
const PREFIX_SCAN_SIZE: usize = 1024 * 1024; // 1 MB

/// Extract the prefix/base directive block from the beginning of a Turtle file.
///
/// Returns `(prefix_text, data_start_byte)` where `prefix_text` is the verbatim
/// source text of all leading directives (including interleaved comments and
/// whitespace) and `data_start_byte` is the byte offset where actual triple
/// data begins.
pub fn extract_prefix_block(path: &Path) -> Result<(String, u64), SplitError> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let scan_size = std::cmp::min(PREFIX_SCAN_SIZE as u64, file_size) as usize;

    let mut buf = vec![0u8; scan_size];
    let mut reader = BufReader::new(file);
    reader.read_exact(&mut buf)?;
    extract_prefix_block_from_bytes(&buf)
}

/// Variant of [`extract_prefix_block`] that reads from any `Read` source.
///
/// Used for compressed inputs (gzip, zstd) where the path-based variant can't
/// `metadata()` for the uncompressed size and can't seek back. Reads up to
/// `PREFIX_SCAN_SIZE` bytes from `reader` (consuming them) and returns the
/// `(prefix_text, data_start_byte)` pair.
pub fn extract_prefix_block_from_reader<R: Read>(
    reader: &mut R,
) -> Result<(String, u64), SplitError> {
    let mut buf = vec![0u8; PREFIX_SCAN_SIZE];
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    buf.truncate(filled);
    extract_prefix_block_from_bytes(&buf)
}

/// Shared scan logic — extract the prefix/base directive block from a buffer
/// of leading bytes. Returns `(prefix_text, data_start_byte_within_buf)`.
fn extract_prefix_block_from_bytes(buf: &[u8]) -> Result<(String, u64), SplitError> {
    // Tokenize the header region. If it ends mid-token, tokenize() may error;
    // we try to find a safe truncation point by scanning backwards for a newline.
    let header_str = find_safe_header(buf);

    let tokens = tokenize(header_str).map_err(|e| SplitError::Tokenize(e.to_string()))?;

    // Walk tokens: collect prefix/base directives. Stop at the first token that
    // is not a directive, whitespace filler, or the terminating dot of a directive.
    //
    // Turtle-style directives (`@prefix`, `@base`) require a trailing `.`.
    // SPARQL-style directives (`PREFIX`, `BASE`) do NOT require a dot — the
    // directive ends after the IRI token.
    let mut last_directive_end: u32 = 0;
    let mut in_directive = false;
    let mut sparql_directive = false;
    let mut saw_any_directive = false;

    for tok in &tokens {
        match tok.kind {
            TokenKind::KwPrefix | TokenKind::KwBase => {
                in_directive = true;
                sparql_directive = false;
                saw_any_directive = true;
            }
            TokenKind::KwSparqlPrefix | TokenKind::KwSparqlBase => {
                in_directive = true;
                sparql_directive = true;
                saw_any_directive = true;
            }
            TokenKind::Dot if in_directive => {
                // Turtle-style directive terminator (also valid after SPARQL-style).
                in_directive = false;
                last_directive_end = tok.end;
            }
            TokenKind::Iri if in_directive && sparql_directive => {
                // SPARQL-style directives end after the IRI (no dot required).
                // `PREFIX ns: <iri>` and `BASE <iri>` both terminate here.
                in_directive = false;
                last_directive_end = tok.end;
            }
            TokenKind::Eof => break,
            _ if in_directive => {
                // Part of the directive (PrefixedNameNs, Iri for Turtle-style, etc.)
            }
            _ => {
                // First non-directive token — this is where data starts.
                break;
            }
        }
    }

    if !saw_any_directive {
        // No prefix block — data starts at byte 0.
        return Ok((String::new(), 0));
    }

    let prefix_end = last_directive_end as usize;

    // Include any trailing whitespace/newlines after the last directive dot
    // so that the prefix block ends cleanly.
    let mut data_start = prefix_end;
    let header_bytes = header_str.as_bytes();
    while data_start < header_bytes.len() && header_bytes[data_start].is_ascii_whitespace() {
        data_start += 1;
    }

    let prefix_text = header_str[..prefix_end].to_string();
    // Ensure prefix block ends with a newline for clean concatenation.
    let prefix_text = if prefix_text.ends_with('\n') {
        prefix_text
    } else {
        format!("{prefix_text}\n")
    };

    tracing::info!(
        prefix_len = prefix_text.len(),
        data_start,
        prefix_first_500 = &prefix_text[..prefix_text.len().min(500)],
        "prefix block extracted"
    );

    Ok((prefix_text, data_start as u64))
}

/// Find a safe truncation point in the buffer for tokenization.
///
/// Even when the buffer is valid UTF-8, the read boundary may land in the
/// middle of a Turtle token (e.g. a long IRI or string literal). We always
/// truncate at the last newline to ensure every line is complete, preventing
/// the tokenizer from seeing unterminated IRIs or strings.
fn find_safe_header(buf: &[u8]) -> &str {
    // Get valid UTF-8 prefix.
    let valid = match std::str::from_utf8(buf) {
        Ok(s) => s.as_bytes(),
        Err(e) => &buf[..e.valid_up_to()],
    };

    // Always truncate at the last newline to avoid cutting mid-token.
    if let Some(nl_pos) = valid.iter().rposition(|&b| b == b'\n') {
        // Safety: valid[..nl_pos+1] is valid UTF-8 (subset of a valid prefix).
        std::str::from_utf8(&valid[..=nl_pos]).unwrap_or("")
    } else {
        // No newline at all — use whatever we have.
        std::str::from_utf8(valid).unwrap_or("")
    }
}

// ============================================================================
// Chunk boundary computation
// ============================================================================

/// Read buffer size for the pre-scan pass.
const SCAN_BUF_SIZE: usize = 64 * 1024; // 64 KB

/// Maximum distance (in bytes) past a chunk target before we error.
const MAX_BOUNDARY_SEARCH: u64 = 64 * 1024 * 1024; // 64 MB

/// Pending lookahead state carried across buffer boundaries.
#[derive(Debug, Default)]
struct Lookahead {
    /// A `.` was the last byte processed; waiting to see if next byte is
    /// whitespace/`#`/EOF to confirm a statement boundary.
    pending_dot: Option<u64>,
    /// Whether the byte immediately preceding the pending dot was a PN_CHARS-like
    /// character (ASCII approximation). Used to avoid treating `.` inside a
    /// prefixed name local part (e.g. `ex:foo.bar`) as a boundary.
    pending_dot_prev_is_pnchar: bool,
    /// Count of consecutive `"` or `'` seen at the end of a buffer (1 or 2),
    /// for detecting triple-quote openings.
    pending_quotes: u8,
    /// Which quote character is pending (`"` or `'`).
    pending_quote_char: u8,
    /// `\r` was the last byte — check for `\n` to form CRLF.
    pending_cr: bool,
}

/// Find the first statement boundary at or after `from`.
///
/// The resync primitive for `--continue-on-error`: a parse fails somewhere,
/// and the driver needs the next point at which a fresh parse can start on a
/// statement rather than in the middle of one. Returns the byte offset just
/// past the terminator, or `None` when the rest of the document contains no
/// complete statement.
///
/// Scanning starts at `from` rather than at zero, so a `.` inside a string
/// that OPENED before `from` will be miscounted as a terminator. That is
/// unavoidable without rescanning the document from the start on every error,
/// and it is safe in the direction that matters: the worst case is resuming
/// mid-statement, which fails again and resyncs again, rather than swallowing
/// input. Resync always advances, so a document with many errors terminates.
pub fn next_statement_boundary(text: &str, from: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let from = from.min(bytes.len());
    let mut scanner = BoundaryScanner::new();
    for (offset, &b) in bytes.iter().enumerate().skip(from) {
        // A mid-file directive is not an error during resync — the caller is
        // already recovering, and refusing here would abort the recovery.
        match scanner.feed(b, offset as u64) {
            Ok(Some(boundary)) => return Some(boundary as usize),
            Ok(None) => {}
            Err(_) => {}
        }
    }
    scanner.finish().map(|b| (b as usize).min(text.len()))
}

/// Split an in-memory Turtle document into independently-parseable pieces.
///
/// Returns `(prefix_block, chunk_ranges)`. Each range is a byte range into
/// `text` holding whole statements; prepending `prefix_block` to any of them
/// produces a valid Turtle document, which is what lets chunks be parsed
/// concurrently.
///
/// The same [`BoundaryScanner`] the file paths use, so the three answers agree
/// — including the refusal: a mid-file `@prefix`/`@base` yields
/// [`SplitError::PrefixAfterData`], because only the first chunk would carry
/// the redefinition and every later one would resolve the prefix against the
/// header binding instead.
///
/// Ranges are cut at the first statement boundary at or past each
/// `target_bytes` multiple, so an actual chunk is a little larger than the
/// target and a document with one enormous statement yields one chunk.
pub fn chunk_in_memory(
    text: &str,
    target_bytes: u64,
) -> Result<(String, Vec<std::ops::Range<usize>>), SplitError> {
    let (prefix_block, data_start) = extract_prefix_block_from_bytes(text.as_bytes())?;
    let data_start = data_start as usize;
    if data_start >= text.len() {
        return Err(SplitError::EmptyData);
    }

    let bytes = text.as_bytes();
    let mut scanner = BoundaryScanner::new();
    let mut ranges = Vec::new();
    let mut start = data_start;

    for (offset, &b) in bytes.iter().enumerate().skip(data_start) {
        if let Some(boundary) = scanner.feed(b, offset as u64)? {
            let boundary = boundary as usize;
            if (boundary - start) as u64 >= target_bytes {
                ranges.push(start..boundary);
                start = boundary;
            }
        }
    }
    // Everything after the last cut, terminator or not — the parser is the
    // authority on whether a trailing fragment is valid, not the chunker.
    // A tail that is only whitespace and comments is absorbed into the
    // previous chunk rather than becoming a chunk of its own: it carries no
    // statement, and a worker spun up for it would do nothing.
    if start < text.len() {
        let tail_is_noise = is_noise_only_chunk(&bytes[start..]);
        match ranges.last_mut() {
            Some(last) if tail_is_noise => last.end = text.len(),
            _ => ranges.push(start..text.len()),
        }
    }
    if ranges.is_empty() {
        return Err(SplitError::EmptyData);
    }
    Ok((prefix_block, ranges))
}

/// Compute chunk byte ranges by scanning the file for statement boundaries.
///
/// Returns a `Vec<(start, end)>` of byte ranges. Each range represents a chunk
/// that contains one or more complete Turtle statements.
///
/// # Errors
///
/// - `PrefixAfterData` if a `@prefix`/`@base`/`PREFIX`/`BASE` directive is
///   detected after data has started.
/// - `NoBoundary` if no statement boundary is found within 64 MB of a target.
pub fn compute_chunk_boundaries(
    path: &Path,
    data_start: u64,
    chunk_size: u64,
) -> Result<Vec<(u64, u64)>, SplitError> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();

    if data_start >= file_size {
        return Err(SplitError::EmptyData);
    }

    let mut reader = BufReader::with_capacity(SCAN_BUF_SIZE, file);
    reader.seek(SeekFrom::Start(data_start))?;

    let mut scanner = BoundaryScanner::new();
    let mut boundaries: Vec<u64> = Vec::new();
    let mut next_target = data_start + chunk_size;
    let mut byte_pos = data_start;
    let mut buf = vec![0u8; SCAN_BUF_SIZE];

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            // EOF — if we have a pending dot, it's a boundary (dot at EOF).
            if let Some(boundary_pos) = scanner.finish() {
                if boundary_pos >= next_target {
                    boundaries.push(boundary_pos);
                }
            }
            break;
        }

        let chunk = &buf[..n];

        for (i, &b) in chunk.iter().enumerate() {
            let abs_pos = byte_pos + i as u64;

            if let Some(boundary_pos) = scanner.feed(b, abs_pos)? {
                if boundary_pos >= next_target {
                    boundaries.push(boundary_pos);
                    next_target = boundary_pos + chunk_size;
                }
            }
        }

        byte_pos += n as u64;

        // Check for overshoot: no boundary found within 64 MB past target.
        if byte_pos > next_target + MAX_BOUNDARY_SEARCH {
            return Err(SplitError::NoBoundary {
                offset: next_target,
            });
        }
    }

    // If no boundaries were recorded (file smaller than chunk_size), use the
    // whole data region as a single chunk.
    if boundaries.is_empty() {
        return Ok(vec![(data_start, file_size)]);
    }

    // Convert boundary positions to (start, end) ranges.
    // The last chunk always extends to EOF (absorbing any trailing whitespace
    // after the final statement boundary).
    let mut ranges = Vec::with_capacity(boundaries.len());
    let mut start = data_start;
    for &end in &boundaries {
        if end > start {
            ranges.push((start, end));
        }
        start = end;
    }
    // Extend the last chunk to EOF instead of creating a tiny trailing range.
    if start < file_size {
        if let Some(last) = ranges.last_mut() {
            last.1 = file_size;
        } else {
            ranges.push((start, file_size));
        }
    }

    Ok(ranges)
}

/// Returns true if `b` can follow a `.` to confirm a statement boundary.
fn is_boundary_follower(b: u8) -> bool {
    // Whitespace/comments always confirm a terminator dot.
    matches!(b, b' ' | b'\t' | b'\r' | b'\n' | b'#')
}

#[inline]
fn is_pnchar_ascii(b: u8) -> bool {
    // Approximation of Turtle PN_CHARS for ASCII-heavy datasets.
    // Used only to avoid splitting inside `ex:foo.bar`-style names.
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')
}

/// True if `buf` contains no Turtle data — only whitespace and `#...` comments.
///
/// Used to avoid emitting an extra "empty" trailing chunk when the file ends
/// with whitespace/comments after the final statement terminator.
fn is_noise_only_chunk(buf: &[u8]) -> bool {
    let mut i = 0usize;
    while i < buf.len() {
        match buf[i] {
            b' ' | b'\t' | b'\r' | b'\n' => {
                i += 1;
            }
            b'#' => {
                // Skip to end of line (or EOF).
                i += 1;
                while i < buf.len() && buf[i] != b'\n' {
                    i += 1;
                }
            }
            _ => return false,
        }
    }
    true
}

/// The byte-level statement-boundary scanner, driven one byte at a time.
///
/// Both chunking paths run this and nothing else. That is the point: the
/// pre-scan path always did, while the streaming path carried a line-based
/// approximation — split at a line ending in `.`, with an ad-hoc toggle for
/// triple-quoted strings — and the two disagreed in three ways that all
/// produced silently wrong output rather than an error:
///
/// - A line-final comment ending in `.` looked like a statement terminator,
///   so a chunk was cut mid-statement and neither half parsed.
/// - `'''` inside a *short* string toggled the long-string flag. An odd number
///   of them left it stuck on, and every subsequent boundary was refused —
///   the rest of the file became one chunk however large it grew.
/// - Mid-file `@prefix`/`@base` directives were not detected at all. Only
///   chunk 0 carries them, so every later chunk resolved the redefined prefix
///   against the header binding and produced wrong IRIs with no error.
///
/// One scanner, one set of answers. `feed` returns the position just past a
/// confirmed statement terminator, which is where a chunk may be cut.
struct BoundaryScanner {
    state: ScanState,
    lookahead: Lookahead,
    prefix_check: PrefixCheck,
    prev_byte: Option<u8>,
}

impl BoundaryScanner {
    fn new() -> Self {
        Self {
            state: ScanState::Normal,
            lookahead: Lookahead::default(),
            prefix_check: PrefixCheck::new(),
            prev_byte: None,
        }
    }

    /// Feed one byte at `abs_pos`.
    ///
    /// Returns `Some(pos)` when the byte *confirms* the statement terminator
    /// that preceded it — `pos` is one past that terminator, so a chunk cut
    /// there contains whole statements. Confirmation is one byte late by
    /// construction: `.` only ends a statement when what follows it is not a
    /// name character, which is what keeps `ex:foo.bar` intact.
    fn feed(&mut self, b: u8, abs_pos: u64) -> Result<Option<u64>, SplitError> {
        let mut boundary = None;

        if let Some(dot_pos) = self.lookahead.pending_dot.take() {
            let next_is_pnchar = is_pnchar_ascii(b);
            if is_boundary_follower(b)
                || !(self.lookahead.pending_dot_prev_is_pnchar && next_is_pnchar)
            {
                boundary = Some(dot_pos + 1);
            }
            // Not a boundary: fall through and process `b` normally.
        }

        if self.lookahead.pending_cr {
            self.lookahead.pending_cr = false;
            // \r\n is one newline; the \r already exited any comment.
        }

        // Triple-quote resolution, which needs the current state to know
        // whether a run of three opens a long string or closes one.
        if self.lookahead.pending_quotes > 0 {
            let pq_char = self.lookahead.pending_quote_char;
            let pq_count = self.lookahead.pending_quotes;

            if b == pq_char {
                let total = pq_count + 1;
                if total >= 3 {
                    self.lookahead.pending_quotes = 0;
                    self.state = match self.state {
                        ScanState::InLongDoubleString | ScanState::InLongSingleString => {
                            ScanState::Normal
                        }
                        _ => {
                            if pq_char == b'"' {
                                ScanState::InLongDoubleString
                            } else {
                                ScanState::InLongSingleString
                            }
                        }
                    };
                    self.prev_byte = Some(b);
                    return Ok(boundary);
                }
                self.lookahead.pending_quotes = total;
                self.prev_byte = Some(b);
                return Ok(boundary);
            }

            self.lookahead.pending_quotes = 0;
            match self.state {
                ScanState::InLongDoubleString | ScanState::InLongSingleString => {
                    // One or two quotes inside a long string: still inside it.
                }
                _ => {
                    self.state = if pq_char == b'"' {
                        ScanState::InShortDoubleString
                    } else {
                        ScanState::InShortSingleString
                    };
                }
            }
        }

        self.state = advance_state(
            self.state,
            b,
            abs_pos,
            self.prev_byte,
            &mut self.lookahead,
            &mut self.prefix_check,
        )?;
        self.prev_byte = Some(b);
        Ok(boundary)
    }

    /// Flush at EOF: a final `.` with nothing after it is still a terminator.
    fn finish(&mut self) -> Option<u64> {
        self.lookahead.pending_dot.take().map(|dot| dot + 1)
    }
}

/// Advance the scan state machine by one byte.
fn advance_state(
    state: ScanState,
    b: u8,
    abs_pos: u64,
    prev_byte: Option<u8>,
    lookahead: &mut Lookahead,
    prefix_check: &mut PrefixCheck,
) -> Result<ScanState, SplitError> {
    match state {
        ScanState::Normal => {
            // Check for prefix/base directives appearing after data.
            prefix_check.feed(b, abs_pos)?;

            match b {
                b'"' => {
                    // Could be start of """ or just "
                    lookahead.pending_quotes = 1;
                    lookahead.pending_quote_char = b'"';
                    Ok(ScanState::Normal) // resolved on next byte
                }
                b'\'' => {
                    lookahead.pending_quotes = 1;
                    lookahead.pending_quote_char = b'\'';
                    Ok(ScanState::Normal)
                }
                b'<' => Ok(ScanState::InIri),
                b'#' => Ok(ScanState::InComment),
                b'.' => {
                    lookahead.pending_dot = Some(abs_pos);
                    lookahead.pending_dot_prev_is_pnchar = prev_byte.is_some_and(is_pnchar_ascii);
                    Ok(ScanState::Normal)
                }
                _ => Ok(ScanState::Normal),
            }
        }

        ScanState::InShortDoubleString => match b {
            b'\\' => Ok(ScanState::InStringEscape {
                return_to: EscapeReturn::ShortDouble,
            }),
            b'"' => Ok(ScanState::Normal),
            b'\n' | b'\r' => {
                // Short strings can't span lines in valid Turtle, but we
                // just return to Normal to be resilient.
                Ok(ScanState::Normal)
            }
            _ => Ok(ScanState::InShortDoubleString),
        },

        ScanState::InLongDoubleString => match b {
            b'\\' => Ok(ScanState::InStringEscape {
                return_to: EscapeReturn::LongDouble,
            }),
            b'"' => {
                // Could be end of long string (""") — we need lookahead.
                // For simplicity, we track consecutive quotes.
                lookahead.pending_quotes = 1;
                lookahead.pending_quote_char = b'"';
                // Stay in long string state; the lookahead handler will
                // transition back to Normal when it sees 3 quotes.
                // Actually, we need special handling here: if we see """,
                // we exit. Let's use a dedicated approach.
                Ok(ScanState::InLongDoubleString)
            }
            _ => Ok(ScanState::InLongDoubleString),
        },

        ScanState::InShortSingleString => match b {
            b'\\' => Ok(ScanState::InStringEscape {
                return_to: EscapeReturn::ShortSingle,
            }),
            b'\'' => Ok(ScanState::Normal),
            b'\n' | b'\r' => Ok(ScanState::Normal),
            _ => Ok(ScanState::InShortSingleString),
        },

        ScanState::InLongSingleString => match b {
            b'\\' => Ok(ScanState::InStringEscape {
                return_to: EscapeReturn::LongSingle,
            }),
            b'\'' => {
                lookahead.pending_quotes = 1;
                lookahead.pending_quote_char = b'\'';
                Ok(ScanState::InLongSingleString)
            }
            _ => Ok(ScanState::InLongSingleString),
        },

        ScanState::InStringEscape { return_to } => {
            // Consume one escaped character and return.
            Ok(return_to.to_state())
        }

        ScanState::InIri => match b {
            b'>' => Ok(ScanState::Normal),
            _ => Ok(ScanState::InIri),
        },

        ScanState::InComment => match b {
            b'\n' => Ok(ScanState::Normal),
            b'\r' => {
                lookahead.pending_cr = true;
                Ok(ScanState::Normal)
            }
            _ => Ok(ScanState::InComment),
        },
    }
}

// ============================================================================
// Prefix-after-data check
// ============================================================================

/// Lightweight detector for `@prefix`, `@base`, `PREFIX`, `BASE` appearing
/// at line start after data has begun.
///
/// We track whether we're at column 0 (or leading whitespace) and match
/// against the keyword prefixes byte-by-byte.
struct PrefixCheck {
    /// Whether any non-whitespace data byte has been seen.
    data_started: bool,
    /// Whether we're at the start of a line (or only whitespace so far on this line).
    at_line_start: bool,
    /// Current match position into one of the keywords.
    match_pos: usize,
    /// Which keyword we're trying to match (index into KEYWORDS).
    match_keyword: usize,
    /// All keyword bytes matched; waiting for a delimiter to confirm.
    awaiting_delimiter: bool,
}

/// Keywords to detect (case-sensitive).
const KEYWORDS: &[&[u8]] = &[b"@prefix", b"@base", b"PREFIX", b"BASE"];

impl PrefixCheck {
    fn new() -> Self {
        Self {
            data_started: false,
            at_line_start: true,
            match_pos: 0,
            match_keyword: usize::MAX,
            awaiting_delimiter: false,
        }
    }

    /// Returns true if `b` is a valid delimiter after a directive keyword.
    /// Directive keywords must be followed by whitespace or `:` (@prefix) or `<` (BASE).
    fn is_keyword_delimiter(b: u8) -> bool {
        matches!(b, b' ' | b'\t' | b':' | b'<')
    }

    fn feed(&mut self, b: u8, abs_pos: u64) -> Result<(), SplitError> {
        // If we matched all keyword bytes, check if the next byte is a delimiter.
        if self.awaiting_delimiter {
            self.awaiting_delimiter = false;
            if Self::is_keyword_delimiter(b) && self.data_started {
                let kw = KEYWORDS[self.match_keyword];
                let kw_start = abs_pos - kw.len() as u64;
                return Err(SplitError::PrefixAfterData { offset: kw_start });
            }
            // Not a delimiter — false alarm (e.g. "BASELINE"), reset.
            self.match_keyword = usize::MAX;
            self.match_pos = 0;
            self.at_line_start = false;
            self.data_started = true;
            return Ok(());
        }

        match b {
            b'\n' | b'\r' => {
                self.at_line_start = true;
                self.match_pos = 0;
                self.match_keyword = usize::MAX;
            }
            b' ' | b'\t' if self.at_line_start => {
                // Still at line start, leading whitespace.
            }
            _ => {
                if self.at_line_start && self.match_pos == 0 {
                    // Try to start matching a keyword.
                    for (i, kw) in KEYWORDS.iter().enumerate() {
                        if b == kw[0] {
                            self.match_keyword = i;
                            self.match_pos = 1;
                            if self.match_pos >= kw.len() {
                                // Single-byte keyword (shouldn't happen with current set,
                                // but handle for correctness).
                                self.awaiting_delimiter = true;
                            }
                            self.at_line_start = false;
                            self.data_started = true;
                            return Ok(());
                        }
                    }
                    // No keyword match — just data.
                    self.at_line_start = false;
                    self.data_started = true;
                } else if self.match_keyword < KEYWORDS.len() {
                    let kw = KEYWORDS[self.match_keyword];
                    if self.match_pos < kw.len() && b == kw[self.match_pos] {
                        self.match_pos += 1;
                        if self.match_pos >= kw.len() {
                            // All keyword bytes matched — need delimiter confirmation.
                            self.awaiting_delimiter = true;
                            return Ok(());
                        }
                    } else {
                        // Mismatch — reset.
                        self.match_keyword = usize::MAX;
                        self.match_pos = 0;
                    }
                    self.at_line_start = false;
                    self.data_started = true;
                } else {
                    self.at_line_start = false;
                    self.data_started = true;
                }
            }
        }
        Ok(())
    }
}

// ============================================================================
// TurtleChunkReader
// ============================================================================

/// Reader for chunks of a large Turtle file.
///
/// Created by [`TurtleChunkReader::new`], which performs the prefix extraction
/// and pre-scan in the constructor. Chunk reads are thread-safe (each opens
/// its own file handle).
pub struct TurtleChunkReader {
    path: PathBuf,
    prefix_block: String,
    /// Byte ranges: `(start, end)` for each chunk.
    ranges: Vec<(u64, u64)>,
}

impl TurtleChunkReader {
    /// Create a new reader by scanning the file for chunk boundaries.
    ///
    /// This performs:
    /// 1. Prefix block extraction (tokenize first 1 MB)
    /// 2. Pre-scan for statement boundaries
    ///
    /// Logs progress for large files.
    pub fn new(path: &Path, config: &TurtleSplitConfig) -> Result<Self, SplitError> {
        let file_size = std::fs::metadata(path)?.len();
        tracing::info!(
            path = %path.display(),
            file_size_mb = file_size / (1024 * 1024),
            chunk_size_mb = config.chunk_size_bytes / (1024 * 1024),
            "scanning large file for chunk boundaries..."
        );

        let (prefix_block, data_start) = extract_prefix_block(path)?;
        tracing::debug!(
            prefix_bytes = prefix_block.len(),
            data_start,
            "prefix block extracted"
        );

        let ranges = compute_chunk_boundaries(path, data_start, config.chunk_size_bytes)?;
        tracing::info!(chunk_count = ranges.len(), "chunk boundaries computed");

        Ok(Self {
            path: path.to_path_buf(),
            prefix_block,
            ranges,
        })
    }

    /// Number of chunks.
    pub fn chunk_count(&self) -> usize {
        self.ranges.len()
    }

    /// Read chunk `index`, prepending the prefix block.
    ///
    /// Returns `None` if `index` is out of range.
    ///
    /// Each call opens its own file handle and seeks to the byte range,
    /// making this safe to call from multiple threads.
    pub fn read_chunk(&self, index: usize) -> io::Result<Option<String>> {
        let Some(&(start, end)) = self.ranges.get(index) else {
            return Ok(None);
        };

        let len = (end - start) as usize;
        let mut buf = vec![0u8; len];

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut buf)?;

        let data = String::from_utf8(buf).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("chunk {index} (bytes {start}..{end}) contains invalid UTF-8: {e}"),
            )
        })?;

        let mut result = String::with_capacity(self.prefix_block.len() + data.len());
        result.push_str(&self.prefix_block);
        result.push_str(&data);

        Ok(Some(result))
    }

    /// The prefix block text (all directives from the file header).
    pub fn prefix_block(&self) -> &str {
        &self.prefix_block
    }

    /// The byte ranges for each chunk (for diagnostics).
    pub fn ranges(&self) -> &[(u64, u64)] {
        &self.ranges
    }
}

// ============================================================================
// StreamingTurtleReader — no pre-scan, emits chunks via channel
// ============================================================================

/// Callback for reporting reader thread progress (bytes_read, total_bytes).
pub type ScanProgressFn = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// A chunk payload: `(index, raw_bytes)`. The raw bytes do NOT include the prefix
/// block — workers prepend it before parsing.
pub type ChunkPayload = (usize, Vec<u8>);

/// Parsed header directives extracted from a Turtle file's prefix block.
///
/// - `prefixes`: short prefix → namespace IRI (already resolved against any `@base`)
/// - `base`: base IRI, **as RESOLVED** — not as the document spelled it.
///   A relative `@base` is resolved against the base in scope before it
///   reaches `on_base`, so what lands here is always absolute.
///
/// That is load-bearing rather than incidental: this prelude is fed straight
/// back into `parse_with_prefixes_base` for every chunk of a parallel import,
/// and that entry point REFUSES a non-absolute base. A relative value here
/// would fail the import loudly (never silently mis-resolve), but the reason
/// it cannot happen is that resolution occurs upstream, at the directive.
#[derive(Debug, Clone, Default)]
pub struct TurtlePrelude {
    pub prefixes: Vec<(String, String)>,
    pub base: Option<String>,
}

/// Collects `@prefix` / `@base` directives from a Turtle snippet.
///
/// The snippet should contain directives only (no triples). If triples are
/// present, this sink will ignore them, but will still allocate term IDs.
#[derive(Default)]
struct PreludeSink {
    prelude: TurtlePrelude,
}

impl GraphSink for PreludeSink {
    fn on_base(&mut self, base_iri: &str) {
        self.prelude.base = Some(base_iri.to_string());
    }

    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        self.prelude
            .prefixes
            .push((prefix.to_string(), namespace_iri.to_string()));
    }

    fn term_iri(&mut self, _iri: &str) -> TermId {
        TermId::new(0)
    }

    fn term_blank(&mut self, _label: Option<&str>) -> TermId {
        TermId::new(0)
    }

    fn term_literal(
        &mut self,
        _value: &str,
        _datatype: Datatype,
        _language: Option<&str>,
    ) -> TermId {
        TermId::new(0)
    }

    fn term_literal_value(&mut self, _value: LiteralValue, _datatype: Datatype) -> TermId {
        TermId::new(0)
    }

    fn emit_triple(&mut self, _subject: TermId, _predicate: TermId, _object: TermId) -> SinkResult {
        Ok(())
    }

    fn emit_list_item(
        &mut self,
        _subject: TermId,
        _predicate: TermId,
        _object: TermId,
        _index: i32,
    ) -> SinkResult {
        Ok(())
    }
}

fn parse_prelude(prefix_block: &str) -> Result<TurtlePrelude, SplitError> {
    if prefix_block.is_empty() {
        return Ok(TurtlePrelude::default());
    }
    let mut sink = PreludeSink::default();
    parse(prefix_block, &mut sink).map_err(|e| SplitError::Tokenize(e.to_string()))?;
    Ok(sink.prelude)
}

/// Streaming reader for a large Turtle file.
///
/// Unlike [`TurtleChunkReader`], this reader does NOT pre-scan the entire file.
/// A background reader thread reads sequentially, identifies statement boundaries,
/// and sends chunk data through a bounded channel as it goes.
///
/// **Single I/O pass**: the reader thread is the only entity reading from disk.
/// Workers receive data from the channel and do CPU-only work (parse, etc.).
/// This avoids double I/O that would occur if workers re-read from the file.
pub struct StreamingTurtleReader {
    prefix_block: String,
    prelude: TurtlePrelude,
    file_size: u64,
    estimated_chunks: usize,
    ns_preflight: std::sync::Arc<std::sync::OnceLock<NamespacePreflight>>,
    /// Shared receiver — multiple parse workers can receive chunks concurrently.
    rx: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<ChunkPayload>>>,
    reader_handle: Option<std::thread::JoinHandle<Result<usize, SplitError>>>,
}

/// Namespace preflight results computed during streaming read.
///
/// Intended to help upstream import code detect “namespace explosion” datasets
/// without any extra I/O pass.
#[derive(Debug, Clone)]
pub struct NamespacePreflight {
    /// Distinct namespace prefixes observed in the sampled windows (using last '/' or '#').
    pub distinct_prefixes: usize,
    /// Distinct http(s) namespace prefixes under `scheme://host/`.
    pub http_host_prefixes: usize,
    /// Distinct http(s) namespace prefixes under `scheme://host/<seg1>/`.
    pub http_host_seg1_prefixes: usize,
    /// True when `distinct_prefixes` exceeded the budget.
    pub exceeded_budget: bool,
    /// Recommended mitigation strategy for namespace allocation.
    pub suggestion: NamespaceSuggestion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceSuggestion {
    /// No special handling recommended.
    None,
    /// Enable coarse namespace fallback heuristic (import-side).
    CoarseHeuristic,
}

impl StreamingTurtleReader {
    /// Create a streaming reader that immediately starts reading the file in
    /// a background thread.
    ///
    /// - `path`: Turtle file to read.
    /// - `chunk_size_bytes`: Target chunk size. Actual chunks may be slightly
    ///   larger (splits only at statement boundaries).
    /// - `channel_capacity`: Bounded channel size (controls max in-flight chunks).
    ///   Each in-flight item is ~`chunk_size_bytes` of heap data. A capacity of
    ///   2–3 is recommended to limit memory while allowing pipelining.
    /// - `progress`: Optional callback `(bytes_read, total_bytes)` invoked
    ///   periodically from the reader thread.
    ///
    /// Returns immediately after extracting the prefix block and spawning the
    /// reader thread. Call [`recv_chunk`](Self::recv_chunk) to pull chunk data.
    pub fn new(
        path: &Path,
        chunk_size_bytes: u64,
        channel_capacity: usize,
        progress: Option<ScanProgressFn>,
    ) -> Result<Self, SplitError> {
        let file_size = std::fs::metadata(path)?.len();

        let (prefix_block, data_start) = extract_prefix_block(path)?;
        let prelude = parse_prelude(&prefix_block)?;
        tracing::info!(
            path = %path.display(),
            file_size_mb = file_size / (1024 * 1024),
            chunk_size_mb = chunk_size_bytes / (1024 * 1024),
            prefix_bytes = prefix_block.len(),
            data_start,
            prefix_first_200 = &prefix_block[..prefix_block.len().min(200)],
            "streaming reader: prefix extracted, spawning reader thread"
        );

        if data_start >= file_size {
            return Err(SplitError::EmptyData);
        }

        let data_len = file_size - data_start;
        let estimated_chunks = data_len.div_ceil(chunk_size_bytes) as usize;

        let (tx, rx) = std::sync::mpsc::sync_channel(channel_capacity);

        let reader_path = path.to_path_buf();
        let ns_preflight: std::sync::Arc<std::sync::OnceLock<NamespacePreflight>> =
            std::sync::Arc::new(std::sync::OnceLock::new());
        let ns_preflight_thread = std::sync::Arc::clone(&ns_preflight);
        let reader_handle = std::thread::Builder::new()
            .name("ttl-reader".into())
            .spawn(move || {
                reader_thread(
                    &reader_path,
                    data_start,
                    file_size,
                    chunk_size_bytes,
                    tx,
                    progress,
                    ns_preflight_thread,
                )
            })
            .map_err(|e| {
                SplitError::Io(io::Error::other(format!(
                    "failed to spawn reader thread: {e}"
                )))
            })?;

        Ok(Self {
            prefix_block,
            prelude,
            file_size,
            estimated_chunks,
            ns_preflight,
            rx: Arc::new(std::sync::Mutex::new(rx)),
            reader_handle: Some(reader_handle),
        })
    }

    /// Create a streaming reader from a non-seekable source (e.g. a streaming
    /// decompressor over a `.ttl.gz` / `.ttl.zst` file).
    ///
    /// `open_reader` is called twice: once on the calling thread to extract the
    /// prelude, and once on the spawned reader thread to stream the body. Each
    /// call must return a freshly-positioned reader at byte 0. This avoids any
    /// seek requirement on the underlying source.
    ///
    /// `total_bytes_hint` is the best-effort *uncompressed* size used only for
    /// progress denominators. Pass `0` if unknown (gzip's footer is only
    /// reliable for files under 4 GiB uncompressed); in that case
    /// `estimated_chunks` is computed from the hint and may be 0.
    ///
    /// All other parameters match [`new`](Self::new).
    pub fn new_from_factory<F>(
        open_reader: F,
        total_bytes_hint: u64,
        chunk_size_bytes: u64,
        channel_capacity: usize,
        progress: Option<ScanProgressFn>,
    ) -> Result<Self, SplitError>
    where
        F: Fn() -> io::Result<Box<dyn BufRead + Send>> + Send + Sync + 'static,
    {
        // Pass 1: open a reader, extract prelude (consumes the prelude bytes).
        let mut prelude_reader = open_reader()?;
        let (prefix_block, data_start) = extract_prefix_block_from_reader(&mut prelude_reader)?;
        drop(prelude_reader);
        let prelude = parse_prelude(&prefix_block)?;

        // `file_size` here is the uncompressed total. For compressed sources
        // the caller may not know it cheaply — in that case 0 means "unknown"
        // and progress callbacks will receive 0 as the denominator.
        let file_size = total_bytes_hint;
        let data_len = file_size.saturating_sub(data_start);
        let estimated_chunks = if data_len == 0 || chunk_size_bytes == 0 {
            0
        } else {
            data_len.div_ceil(chunk_size_bytes) as usize
        };

        tracing::info!(
            file_size_mb = file_size / (1024 * 1024),
            chunk_size_mb = chunk_size_bytes / (1024 * 1024),
            prefix_bytes = prefix_block.len(),
            data_start,
            estimated_chunks,
            "streaming reader (factory): prefix extracted, spawning reader thread"
        );

        let (tx, rx) = std::sync::mpsc::sync_channel(channel_capacity);
        let ns_preflight: std::sync::Arc<std::sync::OnceLock<NamespacePreflight>> =
            std::sync::Arc::new(std::sync::OnceLock::new());
        let ns_preflight_thread = std::sync::Arc::clone(&ns_preflight);
        let reader_handle = std::thread::Builder::new()
            .name("ttl-reader".into())
            .spawn(move || -> Result<usize, SplitError> {
                // Pass 2: open a fresh reader, advance past the prelude, stream.
                let body_reader = open_reader()?;
                reader_thread_from_source(
                    body_reader,
                    data_start,
                    file_size,
                    chunk_size_bytes,
                    tx,
                    progress,
                    ns_preflight_thread,
                )
            })
            .map_err(|e| {
                SplitError::Io(io::Error::other(format!(
                    "failed to spawn reader thread: {e}"
                )))
            })?;

        Ok(Self {
            prefix_block,
            prelude,
            file_size,
            estimated_chunks,
            ns_preflight,
            rx: Arc::new(std::sync::Mutex::new(rx)),
            reader_handle: Some(reader_handle),
        })
    }

    /// Receive the next chunk from the reader thread.
    ///
    /// Returns `Ok(Some((index, raw_bytes)))` for each chunk, or `Ok(None)`
    /// when the reader has finished. The raw bytes do NOT include the prefix
    /// block — use [`prefix_block()`](Self::prefix_block) to prepend it.
    pub fn recv_chunk(&self) -> Result<Option<ChunkPayload>, SplitError> {
        let rx = self.rx.lock().unwrap();
        match rx.recv() {
            Ok(payload) => Ok(Some(payload)),
            Err(std::sync::mpsc::RecvError) => Ok(None),
        }
    }

    /// Get a shared reference to the receiver for distributing across parse workers.
    ///
    /// Each worker locks the mutex to receive the next available chunk.
    /// This provides natural load balancing — faster workers process more chunks.
    pub fn shared_receiver(
        &self,
    ) -> Arc<std::sync::Mutex<std::sync::mpsc::Receiver<ChunkPayload>>> {
        Arc::clone(&self.rx)
    }

    /// Estimated number of chunks (based on file size / chunk size).
    pub fn estimated_chunk_count(&self) -> usize {
        self.estimated_chunks
    }

    /// The prefix block text extracted from the file header.
    pub fn prefix_block(&self) -> &str {
        &self.prefix_block
    }

    /// Parsed header directives (prefixes + base) extracted from the file header.
    pub fn prelude(&self) -> &TurtlePrelude {
        &self.prelude
    }

    /// Namespace preflight results (if computed).
    ///
    /// For streaming reads this is populated by the reader thread before chunk 0 is emitted.
    pub fn namespace_preflight(&self) -> Option<&NamespacePreflight> {
        self.ns_preflight.get()
    }

    /// Clone the internal preflight cell (allows waiting/polling without borrowing `self`).
    pub fn namespace_preflight_cell(
        &self,
    ) -> std::sync::Arc<std::sync::OnceLock<NamespacePreflight>> {
        std::sync::Arc::clone(&self.ns_preflight)
    }

    /// Total file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Wait for the reader thread to finish and return the actual chunk count.
    pub fn join(&mut self) -> Result<usize, SplitError> {
        if let Some(handle) = self.reader_handle.take() {
            match handle.join() {
                Ok(result) => result,
                Err(_) => Err(SplitError::Io(io::Error::other("reader thread panicked"))),
            }
        } else {
            Ok(0)
        }
    }
}

/// Background reader thread: reads file in bulk, finds statement boundaries
/// using the byte-level scan state machine, and sends chunk data through the channel.
///
/// **Single I/O pass** — this is the only entity reading from disk. Workers
/// receive data and do CPU-only processing (UTF-8 validation, prefix prepend,
/// Turtle parsing).
///
/// We intentionally use the same boundary logic as `compute_chunk_boundaries()`
/// (dot-followed-by-whitespace/comment in Normal state) so streaming splitting
/// works for Turtle files that do not contain frequent newlines (e.g. very long
/// lines or statements separated by spaces).
fn reader_thread(
    path: &Path,
    data_start: u64,
    file_size: u64,
    chunk_size_bytes: u64,
    tx: std::sync::mpsc::SyncSender<ChunkPayload>,
    progress: Option<ScanProgressFn>,
    ns_preflight: std::sync::Arc<std::sync::OnceLock<NamespacePreflight>>,
) -> Result<usize, SplitError> {
    let file = File::open(path)?;
    let reader: Box<dyn BufRead + Send> = Box::new(BufReader::with_capacity(SCAN_BUF_SIZE, file));
    reader_thread_from_source(
        reader,
        data_start,
        file_size,
        chunk_size_bytes,
        tx,
        progress,
        ns_preflight,
    )
}

/// Reader-thread variant that takes an already-opened `BufRead` source.
///
/// Used by [`StreamingTurtleReader::new_from_factory`] for compressed inputs
/// (gzip, zstd) and other non-seekable sources. Advances the reader past
/// `data_start` bytes via read-and-discard, then runs the shared chunk
/// emission loop. `file_size` is the best-effort uncompressed total
/// (0 means unknown — used only as the denominator in `progress` callbacks).
fn reader_thread_from_source(
    mut reader: Box<dyn BufRead + Send>,
    data_start: u64,
    file_size: u64,
    chunk_size_bytes: u64,
    tx: std::sync::mpsc::SyncSender<ChunkPayload>,
    progress: Option<ScanProgressFn>,
    ns_preflight: std::sync::Arc<std::sync::OnceLock<NamespacePreflight>>,
) -> Result<usize, SplitError> {
    // Advance past the prelude bytes without a seek (works for any reader).
    // For seekable File readers the cost is one buffered read of the prelude,
    // which is small (< 256 KiB by convention) — negligible vs the chunk loop.
    if data_start > 0 {
        io::copy(&mut reader.by_ref().take(data_start), &mut io::sink())?;
    }

    let mut buf = vec![0u8; SCAN_BUF_SIZE];

    // Accumulate raw bytes for the current chunk.
    let mut chunk_buf: Vec<u8> = Vec::with_capacity(chunk_size_bytes as usize + SCAN_BUF_SIZE);
    let mut abs_pos = data_start;
    let mut chunk_idx: usize = 0;

    // The same byte-level scanner the pre-scan path uses, for the same
    // answers. See `BoundaryScanner` for the three ways the line-based
    // approximation this replaces disagreed with it, all of them silent.
    let mut scanner = BoundaryScanner::new();
    // How far into `chunk_buf` the scanner has consumed, and the absolute
    // position of `chunk_buf[0]` so boundaries can be converted back.
    let mut scan_pos: usize = 0;
    let mut chunk_base = data_start;

    // Progress reporting throttle (every ~64 MB).
    let progress_interval = 64 * 1024 * 1024u64;
    let mut next_progress = data_start + progress_interval;

    // Namespace preflight (bounded windows within chunk 0).
    let mut ns_detector = NamespacePreflightDetector::new(data_start);

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            // EOF — emit final chunk if there's accumulated data.
            if !chunk_buf.is_empty() && !is_noise_only_chunk(&chunk_buf) {
                if tx
                    .send((chunk_idx, std::mem::take(&mut chunk_buf)))
                    .is_err()
                {
                    break;
                }
                chunk_idx += 1;
            }
            break;
        }

        // Bulk append.
        chunk_buf.extend_from_slice(&buf[..n]);
        abs_pos += n as u64;

        // Feed namespace detector only while chunk 0 is being built.
        if chunk_idx == 0 && ns_detector.is_active() {
            let read_start = abs_pos - n as u64;
            ns_detector.feed_range(read_start, &buf[..n]);
            if ns_detector.maybe_finish(&ns_preflight) {
                // Finished early (exceeded budget or all windows processed).
            }
        }

        // Report progress periodically.
        //
        // `file_size` may be 0 when the caller has no cheap way to know the
        // uncompressed total (e.g. a gzip source). Use `saturating_sub` so the
        // denominator stays 0 (the UI's contract for "unknown total") instead
        // of underflowing u64 to a huge bogus value.
        if abs_pos >= next_progress {
            if let Some(ref cb) = progress {
                cb(
                    abs_pos.saturating_sub(data_start),
                    file_size.saturating_sub(data_start),
                );
            }
            next_progress = abs_pos + progress_interval;
        }

        // Feed newly appended bytes to the scanner, cutting a chunk at the
        // first confirmed statement boundary at or past the target size.
        while scan_pos < chunk_buf.len() {
            let b = chunk_buf[scan_pos];
            let abs_pos = chunk_base + scan_pos as u64;
            scan_pos += 1;

            let Some(boundary_abs) = scanner.feed(b, abs_pos)? else {
                continue;
            };
            // `boundary_abs` is one past the terminator, in absolute terms.
            let boundary = (boundary_abs - chunk_base) as usize;
            if (boundary as u64) < chunk_size_bytes {
                continue;
            }

            // Publish namespace preflight before chunk 0 leaves.
            if chunk_idx == 0 && ns_preflight.get().is_none() {
                ns_detector.finish(&ns_preflight);
            }

            let remainder = chunk_buf[boundary..].to_vec();
            chunk_buf.truncate(boundary);

            tracing::debug!(
                chunk = chunk_idx,
                size_mb = chunk_buf.len() as f64 / (1024.0 * 1024.0),
                remainder_bytes = remainder.len(),
                "chunk emitted"
            );

            if tx
                .send((chunk_idx, std::mem::take(&mut chunk_buf)))
                .is_err()
            {
                return Ok(chunk_idx);
            }
            chunk_idx += 1;

            // The remainder becomes the next chunk; the scanner keeps its
            // state (it is mid-document, not mid-statement) and only the
            // buffer-relative offsets rebase.
            chunk_base = boundary_abs;
            chunk_buf = remainder;
            scan_pos = 0;
        }

        // Overshoot protection if we can’t find a boundary line for a long time.
        if (chunk_buf.len() as u64) > chunk_size_bytes + MAX_BOUNDARY_SEARCH {
            return Err(SplitError::NoBoundary {
                offset: data_start + chunk_size_bytes,
            });
        }
    }

    // Final progress report. Skip when `file_size` is 0 (unknown total) —
    // emitting (0, 0) would tell the UI "0 of 0 read" which it can't
    // distinguish from a reset. The CLI handles unknown-total finalization
    // from the next `Committing` phase event instead.
    if let Some(ref cb) = progress {
        if file_size > 0 {
            cb(file_size - data_start, file_size - data_start);
        }
    }

    // If we never emitted chunk 0 (small file) or finished scanning windows late, publish now.
    if ns_preflight.get().is_none() {
        ns_detector.finish(&ns_preflight);
    }

    tracing::info!(total_chunks = chunk_idx, "reader thread finished");

    Ok(chunk_idx)
}

// ============================================================================
// NamespacePreflightDetector (streaming, bounded windows)
// ============================================================================

const NS_PREFLIGHT_BUDGET: usize = 255;
const NS_PREFLIGHT_WINDOW_SIZE: u64 = 8 * 1024 * 1024;
const NS_PREFLIGHT_OFFSETS: &[u64] = &[
    0,
    32 * 1024 * 1024,
    128 * 1024 * 1024,
    320 * 1024 * 1024,
    640 * 1024 * 1024,
];

struct NamespacePreflightDetector {
    windows: std::collections::VecDeque<(u64, u64, NsWindowScanner)>,
    distinct_prefixes: rustc_hash::FxHashSet<Vec<u8>>,
    http_hosts: rustc_hash::FxHashSet<Vec<u8>>,
    http_host_seg1: rustc_hash::FxHashSet<Vec<u8>>,
    exceeded: bool,
}

impl NamespacePreflightDetector {
    fn new(data_start: u64) -> Self {
        let mut windows = std::collections::VecDeque::new();
        for &off in NS_PREFLIGHT_OFFSETS {
            let start = data_start + off;
            let end = start + NS_PREFLIGHT_WINDOW_SIZE;
            windows.push_back((start, end, NsWindowScanner::default()));
        }
        Self {
            windows,
            distinct_prefixes: rustc_hash::FxHashSet::default(),
            http_hosts: rustc_hash::FxHashSet::default(),
            http_host_seg1: rustc_hash::FxHashSet::default(),
            exceeded: false,
        }
    }

    fn is_active(&self) -> bool {
        !self.exceeded
            && !self.windows.is_empty()
            && self.distinct_prefixes.len() <= NS_PREFLIGHT_BUDGET
    }

    fn feed_range(&mut self, abs_start: u64, bytes: &[u8]) {
        if self.exceeded || self.windows.is_empty() {
            return;
        }
        let abs_end = abs_start + bytes.len() as u64;
        // Feed any windows that overlap this read.
        for (w_start, w_end, scanner) in &mut self.windows {
            if *w_end <= abs_start || *w_start >= abs_end {
                continue;
            }
            let s = (*w_start).max(abs_start);
            let e = (*w_end).min(abs_end);
            let rel_s = (s - abs_start) as usize;
            let rel_e = (e - abs_start) as usize;
            scanner.feed(
                &bytes[rel_s..rel_e],
                &mut self.distinct_prefixes,
                &mut self.http_hosts,
                &mut self.http_host_seg1,
                &mut self.exceeded,
            );
            if self.exceeded {
                break;
            }
        }

        // Drop any windows that are fully behind abs_end (we've fed all their bytes).
        while let Some((_w_start, w_end, _)) = self.windows.front() {
            if *w_end <= abs_end {
                self.windows.pop_front();
            } else {
                break;
            }
        }
    }

    fn maybe_finish(&mut self, out: &std::sync::OnceLock<NamespacePreflight>) -> bool {
        if out.get().is_some() {
            return true;
        }
        if self.exceeded || self.windows.is_empty() {
            self.finish(out);
            return true;
        }
        false
    }

    fn finish(&mut self, out: &std::sync::OnceLock<NamespacePreflight>) {
        let exceeded = self.exceeded || self.distinct_prefixes.len() > NS_PREFLIGHT_BUDGET;
        let http_host_prefixes = self.http_hosts.len();
        let http_host_seg1_prefixes = self.http_host_seg1.len();

        let suggestion = if exceeded {
            NamespaceSuggestion::CoarseHeuristic
        } else {
            NamespaceSuggestion::None
        };
        tracing::info!(
            distinct_prefixes = self.distinct_prefixes.len(),
            http_host_prefixes,
            http_host_seg1_prefixes,
            exceeded_budget = exceeded,
            ?suggestion,
            "namespace preflight complete"
        );
        let _ = out.set(NamespacePreflight {
            distinct_prefixes: self.distinct_prefixes.len(),
            http_host_prefixes,
            http_host_seg1_prefixes,
            exceeded_budget: exceeded,
            suggestion,
        });
    }
}

#[derive(Default)]
struct NsWindowScanner {
    in_comment: bool,
    in_iri: bool,
    iri_buf: Vec<u8>,
    in_string: Option<(u8, bool)>, // (quote, triple)
    quote_run: u8,
    escape: bool,
}

impl NsWindowScanner {
    fn feed(
        &mut self,
        bytes: &[u8],
        distinct: &mut rustc_hash::FxHashSet<Vec<u8>>,
        http_hosts: &mut rustc_hash::FxHashSet<Vec<u8>>,
        http_host_seg1: &mut rustc_hash::FxHashSet<Vec<u8>>,
        exceeded: &mut bool,
    ) {
        if *exceeded {
            return;
        }
        let mut i = 0usize;
        while i < bytes.len() {
            let b = bytes[i];

            if self.in_comment {
                if b == b'\n' {
                    self.in_comment = false;
                }
                i += 1;
                continue;
            }

            if self.in_iri {
                if self.escape {
                    self.escape = false;
                    // Keep escaped char as-is (best-effort).
                    self.iri_buf.push(b);
                    i += 1;
                    continue;
                }
                if b == b'\\' {
                    self.escape = true;
                    i += 1;
                    continue;
                }
                if b == b'>' {
                    self.in_iri = false;
                    // Compute prefix = last '/' or '#', inclusive; else empty prefix.
                    let mut split: Option<usize> = None;
                    for (idx, &c) in self.iri_buf.iter().enumerate() {
                        if c == b'/' || c == b'#' {
                            split = Some(idx);
                        }
                    }
                    let prefix = match split {
                        Some(pos) => self.iri_buf[..=pos].to_vec(),
                        None => Vec::new(),
                    };
                    distinct.insert(prefix);

                    // Also track http(s) host and host+seg1 prefixes for strategy selection.
                    if let Some((host_prefix, host_seg1_prefix)) = http_prefixes(&self.iri_buf) {
                        http_hosts.insert(host_prefix);
                        if let Some(seg1) = host_seg1_prefix {
                            http_host_seg1.insert(seg1);
                        }
                    }

                    self.iri_buf.clear();
                    if distinct.len() > NS_PREFLIGHT_BUDGET {
                        *exceeded = true;
                        return;
                    }
                    i += 1;
                    continue;
                }
                self.iri_buf.push(b);
                i += 1;
                continue;
            }

            if let Some((quote, triple)) = self.in_string {
                if self.escape {
                    self.escape = false;
                    i += 1;
                    continue;
                }
                if b == b'\\' {
                    self.escape = true;
                    i += 1;
                    continue;
                }
                if !triple {
                    // Short string ends at matching quote or newline.
                    if b == quote || b == b'\n' || b == b'\r' {
                        self.in_string = None;
                    }
                    i += 1;
                    continue;
                }
                // Triple-quoted: track consecutive quote run.
                if b == quote {
                    self.quote_run = self.quote_run.saturating_add(1);
                    if self.quote_run >= 3 {
                        self.in_string = None;
                        self.quote_run = 0;
                    }
                    i += 1;
                    continue;
                }
                self.quote_run = 0;
                i += 1;
                continue;
            }

            // Normal state
            match b {
                b'#' => {
                    self.in_comment = true;
                    i += 1;
                }
                b'<' => {
                    self.in_iri = true;
                    self.escape = false;
                    self.iri_buf.clear();
                    i += 1;
                }
                b'"' | b'\'' => {
                    // Triple-quote check.
                    if i + 2 < bytes.len() && bytes[i + 1] == b && bytes[i + 2] == b {
                        self.in_string = Some((b, true));
                        self.quote_run = 0;
                        i += 3;
                    } else {
                        self.in_string = Some((b, false));
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }
    }
}

/// Extract `scheme://host/` and optionally `scheme://host/<seg1>/` from a raw IRI buffer.
///
/// Returns `None` if the IRI is not http(s) or doesn't contain a host.
fn http_prefixes(iri: &[u8]) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
    let scheme_len = if iri.starts_with(b"http://") {
        7
    } else if iri.starts_with(b"https://") {
        8
    } else {
        return None;
    };

    // Find end of host.
    let mut host_end = scheme_len;
    while host_end < iri.len() && iri[host_end] != b'/' {
        host_end += 1;
    }
    if host_end >= iri.len() || iri[host_end] != b'/' {
        return None;
    }
    // host-only prefix includes trailing '/'
    let host_prefix = iri[..=host_end].to_vec();

    // seg1 (first path segment)
    let mut seg1_end = host_end + 1;
    while seg1_end < iri.len() && iri[seg1_end] != b'/' {
        seg1_end += 1;
    }
    if seg1_end < iri.len() && iri[seg1_end] == b'/' && seg1_end > host_end + 1 {
        let seg1_prefix = iri[..=seg1_end].to_vec(); // include trailing '/'
        Some((host_prefix, Some(seg1_prefix)))
    } else {
        Some((host_prefix, None))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Write content to a temp file and return the path.
    fn write_temp(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    // ---- extract_prefix_block tests ----

    #[test]
    fn test_prefix_extraction_basic() {
        let ttl = "\
@prefix ex: <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:name \"Alice\" .
";
        let f = write_temp(ttl);
        let (prefix, data_start) = extract_prefix_block(f.path()).unwrap();
        assert!(prefix.contains("@prefix ex:"));
        assert!(prefix.contains("@prefix foaf:"));
        // data_start should be at or after the second directive's dot
        let data_region = &ttl[data_start as usize..];
        assert!(
            data_region.starts_with("ex:alice") || data_region.trim_start().starts_with("ex:alice"),
            "data_region starts with: {:?}",
            &data_region[..40.min(data_region.len())]
        );
    }

    #[test]
    fn test_prefix_extraction_sparql_style() {
        let ttl = "\
PREFIX ex: <http://example.org/>
BASE <http://example.org/>

ex:alice ex:name \"Alice\" .
";
        let f = write_temp(ttl);
        let (prefix, data_start) = extract_prefix_block(f.path()).unwrap();
        assert!(prefix.contains("PREFIX ex:"));
        assert!(prefix.contains("BASE"));
        // data_start must point at or before "ex:alice", not at byte 0
        // and not past the data.
        let data_region = &ttl[data_start as usize..];
        assert!(
            data_region.starts_with("ex:alice") || data_region.trim_start().starts_with("ex:alice"),
            "expected data to start at ex:alice, got: {:?}",
            &data_region[..40.min(data_region.len())]
        );
        // Prefix block must NOT contain data triples.
        assert!(
            !prefix.contains("ex:alice"),
            "prefix block should not contain data triples"
        );
    }

    #[test]
    fn test_prefix_extraction_sparql_no_dot_then_turtle() {
        // Mix: SPARQL-style (no dot) followed by Turtle-style (with dot).
        let ttl = "\
PREFIX ex: <http://example.org/>
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:name \"Alice\" .
";
        let f = write_temp(ttl);
        let (prefix, data_start) = extract_prefix_block(f.path()).unwrap();
        assert!(prefix.contains("PREFIX ex:"));
        assert!(prefix.contains("@prefix foaf:"));
        let data_region = &ttl[data_start as usize..];
        assert!(
            data_region.trim_start().starts_with("ex:alice"),
            "expected data to start at ex:alice, got: {:?}",
            &data_region[..40.min(data_region.len())]
        );
    }

    #[test]
    fn test_prefix_extraction_no_prefixes() {
        let ttl = "<http://example.org/alice> <http://example.org/name> \"Alice\" .\n";
        let f = write_temp(ttl);
        let (prefix, data_start) = extract_prefix_block(f.path()).unwrap();
        assert!(prefix.is_empty());
        assert_eq!(data_start, 0);
    }

    #[test]
    fn test_prefix_extraction_with_comments() {
        let ttl = "\
# This is a Turtle file
@prefix ex: <http://example.org/> .
# Another comment
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:name \"Alice\" .
";
        let f = write_temp(ttl);
        let (prefix, _) = extract_prefix_block(f.path()).unwrap();
        // The prefix block is the raw source up to the end of the last
        // directive's `.` — interleaved comments ARE included because we
        // capture the verbatim text range, not individual tokens.
        assert!(prefix.contains("@prefix ex:"));
        assert!(prefix.contains("@prefix foaf:"));
        assert!(
            prefix.contains("# Another comment"),
            "interleaved comments should be preserved in prefix block"
        );
    }

    // ---- ScanState boundary detection tests ----

    #[test]
    fn test_dot_in_short_string_not_boundary() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice. B.\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        // With a small chunk size, boundaries should only be at real statement dots.
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        // Each statement should be its own chunk (chunk_size=1 forces split at every boundary).
        assert_eq!(ranges.len(), 2, "expected 2 chunks for 2 statements");
    }

    #[test]
    fn test_dot_in_long_string_not_boundary() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:desc \"\"\"This is a long.
Multi-line. Description.\"\"\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2, "expected 2 chunks for 2 statements");
    }

    #[test]
    fn test_dot_in_iri_not_boundary() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:homepage <http://alice.example.org/home.html> .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2, "expected 2 chunks for 2 statements");
    }

    #[test]
    fn test_dot_in_comment_not_boundary() {
        let ttl = "\
@prefix ex: <http://example.org/> .

# This is a comment. With dots.
ex:alice ex:name \"Alice\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_escape_in_string() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Ali\\\"ce.\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_single_chunk_small_file() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        // chunk_size larger than file → single chunk.
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1024 * 1024).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].0, data_start);
    }

    #[test]
    fn test_roundtrip_chunks_preserve_all_triples() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice\" ; ex:age 30 .
ex:bob ex:name \"Bob\" ; ex:age 25 .
ex:carol ex:name \"Carol\" ; ex:age 28 .
ex:dave ex:name \"Dave\" ; ex:age 35 .
";
        let f = write_temp(ttl);
        let config = TurtleSplitConfig {
            chunk_size_bytes: 40, // force multiple chunks
        };
        let reader = TurtleChunkReader::new(f.path(), &config).unwrap();

        // Parse each chunk individually and collect all subjects.
        let mut all_subjects = Vec::new();
        for i in 0..reader.chunk_count() {
            let chunk_text = reader.read_chunk(i).unwrap().unwrap();
            // Parse with fluree_graph_turtle to verify it's valid Turtle.
            let json = crate::parse_to_json(&chunk_text).unwrap();
            let arr = json.as_array().unwrap();
            for node in arr {
                all_subjects.push(node["@id"].as_str().unwrap().to_string());
            }
        }

        all_subjects.sort();
        assert_eq!(
            all_subjects,
            vec![
                "http://example.org/alice",
                "http://example.org/bob",
                "http://example.org/carol",
                "http://example.org/dave",
            ]
        );
    }

    #[test]
    fn test_crlf_line_endings() {
        let ttl = "@prefix ex: <http://example.org/> .\r\n\r\nex:alice ex:name \"Alice\" .\r\nex:bob ex:name \"Bob\" .\r\n";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_single_quote_strings() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name 'Alice. B.' .
ex:bob ex:desc '''Long.
Multi. line.''' .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_dot_followed_by_comment() {
        // `. #comment` should be a boundary.
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice\" .#comment
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_prefix_check_requires_delimiter() {
        // "BASELINE" starts with "BASE" but is not a directive keyword.
        // PrefixCheck must require a delimiter (space, tab, :, <) after the
        // keyword to confirm. This tests the PrefixCheck directly since
        // bare "BASELINE" isn't valid Turtle syntax.
        let mut check = PrefixCheck::new();
        // Feed data so data_started becomes true.
        for (i, &b) in b"ex:alice ex:name \"Alice\" .\n".iter().enumerate() {
            check.feed(b, i as u64).unwrap();
        }
        let offset = 27u64;
        // "BASELINE" at line start should NOT trigger PrefixAfterData.
        for (i, &b) in b"BASELINE stuff\n".iter().enumerate() {
            check
                .feed(b, offset + i as u64)
                .expect("BASELINE should not trigger PrefixAfterData");
        }
        // But "BASE <" (with delimiter) SHOULD trigger.
        let offset2 = offset + 15;
        let mut check2 = PrefixCheck::new();
        for (i, &b) in b"ex:alice ex:name \"Alice\" .\n".iter().enumerate() {
            check2.feed(b, i as u64).unwrap();
        }
        let result = (|| {
            for (i, &b) in b"BASE <http://example.org/>\n".iter().enumerate() {
                check2.feed(b, offset2 + i as u64)?;
            }
            Ok::<(), SplitError>(())
        })();
        assert!(
            matches!(result, Err(SplitError::PrefixAfterData { .. })),
            "BASE followed by delimiter should trigger PrefixAfterData"
        );
    }

    // ---- StreamingTurtleReader tests ----

    /// Helper: receive a chunk from the reader, prepend prefix, return full TTL text.
    fn recv_as_text(reader: &StreamingTurtleReader) -> Option<(usize, String)> {
        let (idx, raw) = reader.recv_chunk().unwrap()?;
        let data = String::from_utf8(raw).unwrap();
        let mut text = String::with_capacity(reader.prefix_block().len() + data.len());
        text.push_str(reader.prefix_block());
        text.push_str(&data);
        Some((idx, text))
    }

    #[test]
    fn test_streaming_roundtrip_preserves_all_triples() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice\" ; ex:age 30 .
ex:bob ex:name \"Bob\" ; ex:age 25 .
ex:carol ex:name \"Carol\" ; ex:age 28 .
ex:dave ex:name \"Dave\" ; ex:age 35 .
";
        let f = write_temp(ttl);
        let mut reader = StreamingTurtleReader::new(
            f.path(),
            40, // small chunk size → multiple chunks
            2,  // channel capacity
            None,
        )
        .unwrap();

        let mut all_subjects = Vec::new();
        while let Some((_idx, chunk_text)) = recv_as_text(&reader) {
            let json = crate::parse_to_json(&chunk_text).unwrap();
            let arr = json.as_array().unwrap();
            for node in arr {
                all_subjects.push(node["@id"].as_str().unwrap().to_string());
            }
        }

        let actual_count = reader.join().unwrap();
        assert!(
            actual_count >= 2,
            "expected at least 2 chunks, got {actual_count}"
        );

        all_subjects.sort();
        assert_eq!(
            all_subjects,
            vec![
                "http://example.org/alice",
                "http://example.org/bob",
                "http://example.org/carol",
                "http://example.org/dave",
            ]
        );
    }

    #[test]
    fn test_streaming_single_chunk_small_file() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice\" .
";
        let f = write_temp(ttl);
        let mut reader = StreamingTurtleReader::new(
            f.path(),
            1024 * 1024, // larger than file
            2,
            None,
        )
        .unwrap();

        let mut chunks = Vec::new();
        while let Some((idx, text)) = recv_as_text(&reader) {
            chunks.push((idx, text));
        }

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0, 0);
        assert!(chunks[0].1.contains("@prefix ex:"));
        assert!(chunks[0].1.contains("ex:alice"));

        let actual = reader.join().unwrap();
        assert_eq!(actual, 1);
    }

    #[test]
    fn test_streaming_prefix_prepended_to_all_chunks() {
        let ttl = "\
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:alice ex:name \"Alice\" .
ex:bob ex:name \"Bob\" .
ex:carol ex:name \"Carol\" .
";
        let f = write_temp(ttl);
        let mut reader = StreamingTurtleReader::new(
            f.path(),
            40, // force multiple chunks
            2,
            None,
        )
        .unwrap();

        while let Some((_idx, text)) = recv_as_text(&reader) {
            assert!(
                text.contains("@prefix ex:"),
                "every chunk must start with prefix block"
            );
            assert!(
                text.contains("@prefix xsd:"),
                "every chunk must include xsd prefix"
            );
            // Verify each chunk is valid Turtle.
            crate::parse_to_json(&text).expect("chunk should be valid Turtle");
        }

        reader.join().unwrap();
    }

    #[test]
    fn test_streaming_dot_in_string_not_boundary() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice. B.\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let mut reader = StreamingTurtleReader::new(
            f.path(),
            1, // tiny chunk size
            2,
            None,
        )
        .unwrap();

        let mut count = 0;
        while let Some((_idx, text)) = recv_as_text(&reader) {
            crate::parse_to_json(&text).expect("chunk should be valid Turtle");
            count += 1;
        }

        assert_eq!(count, 2, "expected 2 chunks for 2 statements");
        reader.join().unwrap();
    }

    #[test]
    fn test_streaming_dot_in_long_string_not_boundary() {
        // Regression: a '.' at end of line inside a triple-quoted string must
        // NOT be treated as a statement boundary by the streaming splitter.
        let ttl = r#"@prefix ex: <http://example.org/> .

ex:alice ex:desc """This sentence ends with a period.
""" .
ex:bob ex:name "Bob" .
"#;
        let f = write_temp(ttl);
        let mut reader = StreamingTurtleReader::new(
            f.path(),
            1, // tiny chunk size to force boundary search
            2,
            None,
        )
        .unwrap();

        let mut all_text = String::new();
        let mut count = 0;
        while let Some((_idx, text)) = recv_as_text(&reader) {
            all_text.push_str(&text);
            count += 1;
        }
        reader.join().unwrap();

        // The multiline string must not be split — both statements should parse.
        assert_eq!(count, 2, "expected 2 chunks for 2 statements");
        assert!(
            all_text.contains("This sentence ends with a period."),
            "multiline string should be preserved intact"
        );
    }

    #[test]
    fn test_streaming_splits_without_newlines() {
        // Splits on long lines with no blank lines between statements.
        //
        // The comment this replaces said newlines were REQUIRED, because the
        // streaming path used to find boundaries by looking for them. It now
        // runs the byte-level scanner, so a newline is incidental; see
        // `streaming_splits_a_document_with_no_newlines_at_all`.
        let ttl = "\
@prefix ex: <http://example.org/> .\n\
ex:alice ex:name \"Alice\" .\n\
ex:bob ex:name \"Bob\" .\n\
ex:carol ex:name \"Carol\" .\n";
        let f = write_temp(ttl);
        let mut reader = StreamingTurtleReader::new(f.path(), 30, 2, None).unwrap();

        let mut count = 0usize;
        while let Some((_idx, text)) = recv_as_text(&reader) {
            crate::parse_to_json(&text).expect("chunk should be valid Turtle");
            count += 1;
        }

        let actual = reader.join().unwrap();
        assert_eq!(count, actual);
        assert!(count >= 2, "expected multiple chunks, got {count}");
    }

    // ---- Streaming boundary defects (plan §1.4) ----
    //
    // These three are written against the line-based scanner and FAIL on it.
    // Each is a way the streaming path splits a chunk somewhere the byte-level
    // pre-scan would not, and each produces a chunk that is not valid Turtle
    // or silently wrong IRIs.

    /// Collect every streaming chunk as text, or the error the reader raised.
    fn stream_chunks(ttl: &str, chunk_size: u64) -> Result<Vec<String>, SplitError> {
        let f = write_temp(ttl);
        let mut reader = StreamingTurtleReader::new(f.path(), chunk_size, 4, None)?;
        let mut out = Vec::new();
        while let Some((_idx, text)) = recv_as_text(&reader) {
            out.push(text);
        }
        reader.join()?;
        Ok(out)
    }

    #[test]
    fn streaming_does_not_split_on_a_period_inside_a_line_final_comment() {
        // The line ends with '.' — but the '.' is inside a comment, and the
        // statement is still open. A line-based check sees "ends with '.'" and
        // splits mid-statement, producing two unparseable halves.
        let ttl = "@prefix ex: <http://example.org/> .\n\
                   ex:alice ex:name \"Alice\" ; # see RFC 3986 sec. 5.1.1.\n\
                   ex:age 30 .\n\
                   ex:bob ex:name \"Bob\" .\n";

        let chunks = stream_chunks(ttl, 40).expect("reader must not fail");
        for (i, text) in chunks.iter().enumerate() {
            crate::parse_to_json(text)
                .unwrap_or_else(|e| panic!("chunk {i} is not valid Turtle: {e}\n{text}"));
        }
    }

    #[test]
    fn streaming_does_not_mistake_quotes_inside_a_short_string_for_a_long_one() {
        // An ODD number of `'''` inside a short double-quoted literal. The
        // line scanner toggles a long-string flag on each occurrence, so an
        // odd count leaves it stuck on and every boundary after this point is
        // refused — the rest of the file becomes one chunk however large it
        // grows. (An even count self-cancels, which is why the parity matters
        // and why this fixture spells it out.)
        let ttl = "@prefix ex: <http://example.org/> .\n\
                   ex:a ex:quote \"three apostrophes: ''' and done\" .\n\
                   ex:b ex:name \"B\" .\n\
                   ex:c ex:name \"C\" .\n\
                   ex:d ex:name \"D\" .\n";

        let chunks = stream_chunks(ttl, 40).expect("reader must not fail");
        assert!(
            chunks.len() >= 2,
            "the flag stuck: got {} chunk(s)",
            chunks.len()
        );
        for (i, text) in chunks.iter().enumerate() {
            crate::parse_to_json(text)
                .unwrap_or_else(|e| panic!("chunk {i} is not valid Turtle: {e}\n{text}"));
        }
    }

    #[test]
    fn streaming_refuses_a_mid_file_directive_instead_of_producing_wrong_iris() {
        // A mid-file @prefix redefinition. Only chunk 0 carries it; every later
        // chunk gets the HEADER's binding prepended, so `ex:x` silently resolves
        // to the wrong namespace. The pre-scan path raises PrefixAfterData; the
        // streaming path did not look.
        let ttl = "@prefix ex: <http://first.example/> .\n\
                   ex:a ex:p \"1\" .\n\
                   ex:b ex:p \"2\" .\n\
                   @prefix ex: <http://second.example/> .\n\
                   ex:c ex:p \"3\" .\n\
                   ex:d ex:p \"4\" .\n";

        match stream_chunks(ttl, 40) {
            Err(SplitError::PrefixAfterData { .. }) => {}
            Err(other) => panic!("wrong error: {other}"),
            Ok(chunks) => panic!(
                "a mid-file directive was accepted; later chunks resolve ex: to the \
                 header binding and are silently wrong. Chunks:\n{}",
                chunks.join("\n---\n")
            ),
        }
    }

    #[test]
    fn streaming_splits_a_document_with_no_newlines_at_all() {
        // Turtle does not require newlines, and a generated dump often has
        // none. The line-based scanner could not split such a file at all: it
        // searched for '\n' and, finding none, accumulated the whole document
        // into one chunk. The byte-level scanner splits on the terminator.
        let mut ttl = String::from("@prefix ex: <http://example.org/> . ");
        for i in 0..60 {
            ttl.push_str(&format!("ex:s{i} ex:p \"v{i}\" . "));
        }
        assert_eq!(
            ttl.matches('\n').count(),
            0,
            "fixture must have no newlines"
        );

        let chunks = stream_chunks(&ttl, 100).expect("reader must not fail");
        assert!(
            chunks.len() >= 2,
            "no newline meant no split: got {} chunk(s)",
            chunks.len()
        );

        let mut total = 0usize;
        for (i, text) in chunks.iter().enumerate() {
            let json = crate::parse_to_json(text)
                .unwrap_or_else(|e| panic!("chunk {i} is not valid Turtle: {e}\n{text}"));
            total += json.as_array().map_or(0, Vec::len);
        }
        assert_eq!(total, 60, "every statement must survive the split");
    }

    // ---- in-memory chunking ----

    #[test]
    fn in_memory_chunks_are_independently_parseable_and_lose_nothing() {
        let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
        for i in 0..200 {
            ttl.push_str(&format!("ex:s{i} ex:p \"v{i}\" .\n"));
        }

        let (prefix, ranges) = chunk_in_memory(&ttl, 300).expect("splits");
        assert!(
            ranges.len() >= 3,
            "expected several chunks, got {}",
            ranges.len()
        );

        // Ranges tile the data region with no gap and no overlap.
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].start, "ranges must tile");
        }
        assert_eq!(ranges.last().unwrap().end, ttl.len());

        let mut total = 0usize;
        for (i, r) in ranges.iter().enumerate() {
            let doc = format!("{prefix}{}", &ttl[r.clone()]);
            let json = crate::parse_to_json(&doc)
                .unwrap_or_else(|e| panic!("chunk {i} is not valid Turtle: {e}\n{doc}"));
            total += json.as_array().map_or(0, Vec::len);
        }
        assert_eq!(total, 200, "every statement survives chunking");
    }

    #[test]
    fn in_memory_chunking_refuses_a_mid_file_directive() {
        // Same refusal as the file paths, for the same reason: only the first
        // chunk would carry the redefinition.
        let ttl = "@prefix ex: <http://first.example/> .\n\
                   ex:a ex:p \"1\" .\n\
                   @prefix ex: <http://second.example/> .\n\
                   ex:b ex:p \"2\" .\n";
        assert!(matches!(
            chunk_in_memory(ttl, 10),
            Err(SplitError::PrefixAfterData { .. })
        ));
    }

    #[test]
    fn in_memory_chunking_keeps_one_huge_statement_whole() {
        // A target smaller than a single statement cannot cut it: boundaries
        // exist only at terminators.
        let mut ttl = String::from("@prefix ex: <http://example.org/> .\nex:s ex:p ");
        ttl.push_str(&format!("\"{}\"", "x".repeat(5000)));
        ttl.push_str(" .\n");

        let (prefix, ranges) = chunk_in_memory(&ttl, 100).expect("splits");
        assert_eq!(ranges.len(), 1, "one statement is one chunk");
        crate::parse_to_json(&format!("{prefix}{}", &ttl[ranges[0].clone()])).unwrap();
    }

    #[test]
    fn next_statement_boundary_finds_the_resume_point() {
        let ttl = "@prefix ex: <http://e/> .\nex:a ex:p \"1\" .\nex:b ex:p \"2\" .\n";

        // From the very start: the end of the directive.
        let first = next_statement_boundary(ttl, 0).unwrap();
        assert_eq!(&ttl[..first], "@prefix ex: <http://e/> .");

        // From inside the second statement: the end of it, not of the third.
        let inside = ttl.find("ex:a").unwrap() + 2;
        let next = next_statement_boundary(ttl, inside).unwrap();
        assert!(ttl[..next].ends_with("\"1\" ."), "{:?}", &ttl[..next]);

        // Past every terminator: nothing left to resume at.
        assert_eq!(next_statement_boundary(ttl, ttl.len()), None);
    }

    #[test]
    fn next_statement_boundary_always_advances() {
        // The property that makes a resync loop terminate: the answer is
        // strictly greater than where the scan began.
        let ttl = "ex:a ex:p \"1\" . ex:b ex:p \"2\" . ex:c ex:p \"3\" .";
        let mut at = 0usize;
        let mut steps = 0;
        while let Some(next) = next_statement_boundary(ttl, at) {
            assert!(next > at, "resync did not advance: {at} -> {next}");
            at = next;
            steps += 1;
            assert!(steps < 10, "resync looped");
        }
        assert_eq!(steps, 3, "three statements, three boundaries");
    }

    #[test]
    fn next_statement_boundary_ignores_a_dot_inside_a_literal() {
        let ttl = "ex:a ex:p \"one. two. three\" .\nex:b ex:p \"x\" .\n";
        let first = next_statement_boundary(ttl, 0).unwrap();
        assert!(
            ttl[..first].ends_with("three\" ."),
            "resumed inside a literal: {:?}",
            &ttl[..first]
        );
    }

    #[test]
    fn test_streaming_progress_callback() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = Arc::clone(&called);
        let progress: ScanProgressFn = Arc::new(move |_bytes, _total| {
            called_clone.store(true, std::sync::atomic::Ordering::Relaxed);
        });

        let mut reader = StreamingTurtleReader::new(
            f.path(),
            1, // tiny chunks
            2,
            Some(progress),
        )
        .unwrap();

        while reader.recv_chunk().unwrap().is_some() {}
        reader.join().unwrap();
        // Progress may or may not be called for small files (depends on
        // whether we cross the 64 MB threshold). Just verify it doesn't panic.
    }
}
