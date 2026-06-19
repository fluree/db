//! Source span utilities for precise diagnostic locations.

use serde::{Deserialize, Serialize};

/// A span in the source text, identified by byte offsets.
///
/// Spans are inclusive of start and exclusive of end: `[start, end)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceSpan {
    pub start: usize,
    pub end: usize,
}

impl SourceSpan {
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub const fn point(offset: usize) -> Self {
        Self {
            start: offset,
            end: offset,
        }
    }

    pub const fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    pub const fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    pub fn union(self, other: Self) -> Self {
        Self {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_basics() {
        let span = SourceSpan::new(5, 10);
        assert_eq!(span.len(), 5);
        assert!(!span.is_empty());
    }

    #[test]
    fn test_span_union() {
        let a = SourceSpan::new(5, 10);
        let b = SourceSpan::new(8, 15);
        assert_eq!(a.union(b), SourceSpan::new(5, 15));
    }
}
