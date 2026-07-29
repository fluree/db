//! Byte offset → line/column, built only when something needs reporting
//!
//! Parsers here carry byte offsets, not line/column pairs: a `u32` span is
//! what the lexer stores per token and what an error can report for free.
//! Turning an offset into a human position needs a scan of the input, and
//! paying for that on every parse — including the overwhelming majority that
//! never report anything — is exactly the cost a converter cannot afford.
//!
//! [`LineIndex`] resolves that by building its table on the *first* lookup.
//! Constructing one is free, so a driver can hold one for the whole parse and
//! still pay nothing on a clean document; with `--continue-on-error`, the
//! thousandth diagnostic reuses the table the first one built.

use std::cell::OnceCell;

/// A lazily-built table of line starts over a source string.
///
/// # Line and column semantics
///
/// - Lines are delimited by `\n` and numbered from 1. A `\r` immediately
///   before the newline belongs to the terminator and is not part of the
///   line's text, matching [`str::lines`].
/// - Columns are 1-based **character** counts, not byte offsets — the same
///   convention the Turtle lexer's caret diagnostics already use, and the
///   one that makes a caret line up under the character it blames.
/// - An offset past the end of the input resolves to the end, rather than
///   panicking: an "unexpected end of input" error points there routinely.
///
/// The table is built on first use through a [`OnceCell`], so this type is
/// deliberately not `Sync`; give each thread its own.
///
/// # Example
///
/// ```
/// use fluree_graph_ir::LineIndex;
///
/// let index = LineIndex::new("ex:s ex:p\nex:o .");
/// assert_eq!(index.line_col(0), (1, 1));
/// assert_eq!(index.line_col(10), (2, 1));
/// assert_eq!(index.line_text(2), "ex:o .");
/// ```
#[derive(Clone, Debug)]
pub struct LineIndex<'a> {
    source: &'a str,
    /// Byte offset of each line's first character. Always starts with 0, so
    /// it is never empty once built.
    line_starts: OnceCell<Vec<usize>>,
}

impl<'a> LineIndex<'a> {
    /// Create an index over `source`. Builds nothing yet.
    pub fn new(source: &'a str) -> Self {
        Self {
            source,
            line_starts: OnceCell::new(),
        }
    }

    /// The indexed source
    pub fn source(&self) -> &'a str {
        self.source
    }

    /// Resolve a byte offset to a 1-based (line, column) pair.
    ///
    /// Offsets past the end of the input clamp to the end. An offset that
    /// falls inside a multi-byte character resolves to that character's
    /// column rather than panicking.
    pub fn line_col(&self, offset: usize) -> (u32, u32) {
        let offset = offset.min(self.source.len());
        let starts = self.starts();
        // `starts` is sorted and begins at 0, so this never underflows.
        let line_idx = starts.partition_point(|&start| start <= offset) - 1;
        let line_start = starts[line_idx];

        let col = self.source[line_start..]
            .char_indices()
            .take_while(|(i, _)| line_start + i < offset)
            .count()
            + 1;

        (saturating_u32(line_idx + 1), saturating_u32(col))
    }

    /// The text of a 1-based line, without its terminator.
    ///
    /// Returns `""` for line 0 or for a line past the end — including the
    /// empty final line a source ending in `\n` has.
    pub fn line_text(&self, line: u32) -> &'a str {
        let starts = self.starts();
        let Some(index) = (line as usize).checked_sub(1) else {
            return "";
        };
        let Some(&start) = starts.get(index) else {
            return "";
        };
        // The next line starts one past its `\n`, so this line's text ends
        // just before it.
        let end = starts
            .get(index + 1)
            .map_or(self.source.len(), |&next| next - 1);
        let text = &self.source[start..end];
        text.strip_suffix('\r').unwrap_or(text)
    }

    /// How many lines the index distinguishes.
    ///
    /// A source ending in `\n` has an empty final line, so this is one more
    /// than [`str::lines`] would yield for such an input. That final line is
    /// a real position: it is where an "unexpected end of input" points.
    pub fn line_count(&self) -> u32 {
        saturating_u32(self.starts().len())
    }

    /// Render the source-context block that goes under a diagnostic's
    /// headline:
    ///
    /// ```text
    ///   |
    /// 3 | ex:s ex:p ex:o
    ///   |           ^
    /// ```
    ///
    /// No leading or trailing newline, so a caller writes
    /// `format!("{headline}\n{block}")`. This is the single definition of
    /// that shape: both the Turtle lexer's own error messages and
    /// [`Diagnostic::render`](crate::Diagnostic::render) go through it, which
    /// is what keeps the two renderings from drifting.
    pub fn caret_block(&self, line: u32, col: u32) -> String {
        let text = self.line_text(line);
        let pointer = " ".repeat(col.saturating_sub(1) as usize);
        format!("  |\n{line} | {text}\n  | {pointer}^")
    }

    /// The line-start table, building it on first use.
    fn starts(&self) -> &[usize] {
        self.line_starts.get_or_init(|| {
            let mut starts = vec![0];
            starts.extend(self.source.match_indices('\n').map(|(i, _)| i + 1));
            starts
        })
    }
}

/// Positions are reported as `u32` (the width of a token span); an input big
/// enough to overflow that is already past the parser's own input limit, so
/// saturate rather than wrap.
fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The line/column algorithm the Turtle lexer used before it shared this
    /// one. Kept as an oracle so the shared implementation is pinned to the
    /// behavior every existing caret diagnostic was written against.
    fn legacy_line_col(source: &str, position: usize) -> (usize, usize) {
        let mut line = 1;
        let mut col = 1;
        for (i, c) in source.char_indices() {
            if i >= position {
                break;
            }
            if c == '\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
        }
        (line, col)
    }

    fn assert_matches_legacy(source: &str) {
        let index = LineIndex::new(source);
        for offset in 0..=source.len() {
            let (line, col) = index.line_col(offset);
            let (want_line, want_col) = legacy_line_col(source, offset);
            assert_eq!(
                (line as usize, col as usize),
                (want_line, want_col),
                "offset {offset} of {source:?}"
            );
            assert_eq!(
                index.line_text(line),
                source.lines().nth(line as usize - 1).unwrap_or(""),
                "line {line} text of {source:?}"
            );
        }
    }

    #[test]
    fn empty_input_has_one_position() {
        let index = LineIndex::new("");
        assert_eq!(index.line_col(0), (1, 1));
        assert_eq!(index.line_count(), 1);
        assert_eq!(index.line_text(1), "");
        // Line 0 does not exist — lines are 1-based.
        assert_eq!(index.line_text(0), "");
        assert_eq!(index.line_text(2), "");
    }

    #[test]
    fn newline_terminated_lines() {
        let index = LineIndex::new("one\ntwo\nthree");
        assert_eq!(index.line_col(0), (1, 1));
        assert_eq!(index.line_col(3), (1, 4)); // the '\n' itself
        assert_eq!(index.line_col(4), (2, 1));
        assert_eq!(index.line_col(8), (3, 1));
        assert_eq!(index.line_text(1), "one");
        assert_eq!(index.line_text(2), "two");
        assert_eq!(index.line_text(3), "three");
        assert_eq!(index.line_count(), 3);
        assert_matches_legacy("one\ntwo\nthree");
    }

    #[test]
    fn crlf_terminators_are_not_part_of_the_line() {
        let index = LineIndex::new("one\r\ntwo\r\n");
        assert_eq!(
            index.line_text(1),
            "one",
            "the \\r belongs to the terminator"
        );
        assert_eq!(index.line_text(2), "two");
        assert_eq!(index.line_col(5), (2, 1), "'t' of two");
        // A source ending in a newline has an empty final line, and that is
        // where an at-EOF diagnostic points.
        assert_eq!(index.line_count(), 3);
        assert_eq!(index.line_text(3), "");
        assert_eq!(index.line_col(10), (3, 1));
        assert_matches_legacy("one\r\ntwo\r\n");
    }

    #[test]
    fn multi_byte_characters_count_as_one_column() {
        // "héllo" — 'é' is two bytes, so byte offsets and columns diverge.
        let source = "héllo\nx";
        let index = LineIndex::new(source);
        assert_eq!(index.line_col(0), (1, 1), "h");
        assert_eq!(index.line_col(1), (1, 2), "é starts here");
        assert_eq!(index.line_col(3), (1, 3), "l — byte 3, column 3");
        assert_eq!(index.line_col(7), (2, 1), "x on the next line");
        assert_eq!(index.line_text(1), "héllo");
        assert_matches_legacy(source);
    }

    #[test]
    fn an_offset_inside_a_character_resolves_to_that_character() {
        // Slicing at byte 2 would panic; the index must not.
        let index = LineIndex::new("é");
        assert_eq!(index.line_col(1), (1, 2));
    }

    #[test]
    fn offsets_at_and_past_the_end_clamp() {
        let source = "abc";
        let index = LineIndex::new(source);
        assert_eq!(
            index.line_col(source.len()),
            (1, 4),
            "one past the last char"
        );
        assert_eq!(
            index.line_col(9_999),
            index.line_col(source.len()),
            "past the end clamps to the end"
        );
    }

    #[test]
    fn consecutive_newlines_make_empty_lines() {
        let source = "a\n\nb";
        let index = LineIndex::new(source);
        assert_eq!(index.line_col(2), (2, 1));
        assert_eq!(index.line_text(2), "");
        assert_eq!(index.line_col(3), (3, 1));
        assert_eq!(index.line_text(3), "b");
        assert_matches_legacy(source);
    }

    #[test]
    fn the_caret_block_points_under_the_offending_column() {
        let index = LineIndex::new("one\ntwo\n");
        let (line, col) = index.line_col(5);
        assert_eq!((line, col), (2, 2));
        assert_eq!(index.caret_block(line, col), "  |\n2 | two\n  |  ^");
    }

    #[test]
    fn the_caret_block_counts_characters_not_bytes() {
        // Byte-based padding would put the caret one cell too far right.
        let source = "é@";
        let index = LineIndex::new(source);
        let (line, col) = index.line_col(2);
        assert_eq!((line, col), (1, 2));
        assert_eq!(index.caret_block(line, col), "  |\n1 | é@\n  |  ^");
    }
}
