//! Source span utilities for precise diagnostic locations.
//!
//! Every AST node carries a `SourceSpan` indicating its position in the source.
//! This enables precise error messages with line/column information.

use serde::{Deserialize, Serialize};

/// A span in the source text, identified by byte offsets.
///
/// Spans are inclusive of start and exclusive of end: `[start, end)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceSpan {
    /// Byte offset of the start (inclusive)
    pub start: usize,
    /// Byte offset of the end (exclusive)
    pub end: usize,
}

impl SourceSpan {
    /// Create a new span from start to end byte offsets.
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Create an empty span at a single position.
    pub const fn point(offset: usize) -> Self {
        Self {
            start: offset,
            end: offset,
        }
    }

    /// The length of this span in bytes.
    pub const fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    /// Whether this span is empty.
    pub const fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// Create a span that covers both this span and another.
    pub fn union(self, other: Self) -> Self {
        Self {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

    /// Extract the substring covered by this span from the source.
    ///
    /// Returns an empty string if the span is out of range or invalid.
    /// Both start and end are clamped to the source length.
    pub fn slice<'a>(&self, source: &'a str) -> &'a str {
        let len = source.len();
        let start = self.start.min(len);
        let end = self.end.min(len);
        if start <= end {
            &source[start..end]
        } else {
            ""
        }
    }
}

impl From<std::ops::Range<usize>> for SourceSpan {
    fn from(range: std::ops::Range<usize>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<SourceSpan> for std::ops::Range<usize> {
    fn from(span: SourceSpan) -> Self {
        span.start..span.end
    }
}

/// Mapping from byte offsets to line/column positions.
///
/// This is computed lazily when needed for diagnostics rendering.
#[derive(Debug)]
pub struct LineIndex {
    /// Byte offsets of line starts (including offset 0 for line 1)
    line_starts: Vec<usize>,
}

impl LineIndex {
    /// Build a line index from source text.
    pub fn new(source: &str) -> Self {
        let mut line_starts = vec![0];
        for (i, c) in source.char_indices() {
            if c == '\n' {
                line_starts.push(i + 1);
            }
        }
        Self { line_starts }
    }

    /// Convert a byte offset to a line/column position.
    ///
    /// Lines and columns are 1-indexed.
    pub fn line_col(&self, offset: usize) -> LineCol {
        let line = self
            .line_starts
            .partition_point(|&start| start <= offset)
            .saturating_sub(1);
        let line_start = self.line_starts.get(line).copied().unwrap_or(0);
        LineCol {
            line: line as u32 + 1,
            col: (offset - line_start) as u32 + 1,
        }
    }

    /// Get the byte offset of a line start.
    pub fn line_start(&self, line: u32) -> Option<usize> {
        self.line_starts
            .get(line.saturating_sub(1) as usize)
            .copied()
    }

    /// Get the byte offset of a line end (exclusive).
    pub fn line_end(&self, line: u32, source: &str) -> usize {
        self.line_starts
            .get(line as usize)
            .copied()
            .unwrap_or(source.len())
    }

    /// Number of lines in the source.
    pub fn line_count(&self) -> usize {
        self.line_starts.len()
    }
}

/// A line/column position in source text (1-indexed).
///
/// **Note**: Column numbers are byte-based, not character or grapheme-based.
/// For non-ASCII queries, visual alignment in diagnostic rendering may be off.
/// This is a known limitation acceptable for SPARQL (mostly ASCII).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LineCol {
    /// Line number (1-indexed)
    pub line: u32,
    /// Column number (1-indexed, in bytes, not characters)
    pub col: u32,
}

impl LineCol {
    /// Create a new line/column position.
    pub const fn new(line: u32, col: u32) -> Self {
        Self { line, col }
    }
}

impl std::fmt::Display for LineCol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.line, self.col)
    }
}

/// A span with resolved line/column information.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSpan {
    /// Start position (line/col)
    pub start: LineCol,
    /// End position (line/col)
    pub end: LineCol,
    /// Original byte span
    #[serde(flatten)]
    pub span: SourceSpan,
}

impl ResolvedSpan {
    /// Resolve a span using a line index.
    pub fn resolve(span: SourceSpan, index: &LineIndex) -> Self {
        Self {
            start: index.line_col(span.start),
            end: index.line_col(span.end),
            span,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_basics() {
        let span = SourceSpan::new(5, 10);
        assert_eq!(span.len(), 5);
        assert!(!span.is_empty());

        let empty = SourceSpan::point(5);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
    }

    #[test]
    fn test_span_union() {
        let a = SourceSpan::new(5, 10);
        let b = SourceSpan::new(8, 15);
        let union = a.union(b);
        assert_eq!(union, SourceSpan::new(5, 15));
    }

    #[test]
    fn test_span_slice() {
        let source = "hello world";
        let span = SourceSpan::new(6, 11);
        assert_eq!(span.slice(source), "world");
    }

    #[test]
    fn test_span_slice_out_of_range() {
        let source = "hello";

        // Start past end of source (EOF error span)
        let eof_span = SourceSpan::new(100, 105);
        assert_eq!(eof_span.slice(source), "");

        // End past source length
        let past_end = SourceSpan::new(3, 100);
        assert_eq!(past_end.slice(source), "lo");

        // Inverted span (start > end)
        let inverted = SourceSpan::new(10, 5);
        assert_eq!(inverted.slice(source), "");

        // Empty source
        let empty_source = "";
        let any_span = SourceSpan::new(0, 10);
        assert_eq!(any_span.slice(empty_source), "");
    }

    #[test]
    fn test_line_index() {
        let source = "line1\nline2\nline3";
        let index = LineIndex::new(source);

        assert_eq!(index.line_count(), 3);

        // Beginning of file
        assert_eq!(index.line_col(0), LineCol::new(1, 1));

        // Middle of line 1
        assert_eq!(index.line_col(3), LineCol::new(1, 4));

        // Beginning of line 2
        assert_eq!(index.line_col(6), LineCol::new(2, 1));

        // Middle of line 3
        assert_eq!(index.line_col(14), LineCol::new(3, 3));
    }

    #[test]
    fn test_resolved_span() {
        let source = "SELECT ?x\nWHERE { }";
        let index = LineIndex::new(source);
        let span = SourceSpan::new(10, 15); // "WHERE"

        let resolved = ResolvedSpan::resolve(span, &index);
        assert_eq!(resolved.start, LineCol::new(2, 1));
        assert_eq!(resolved.end, LineCol::new(2, 6));
    }
}
