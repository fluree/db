//! The TriG locator: where a document's graph blocks are, read from tokens.
//!
//! It never interprets a term, so it cannot disagree with the parser about
//! what the text means. It finds each block's label (as written) and extent,
//! hands the parser each segment's tokens (the document is lexed once, here),
//! and names the construct it refuses: a nested block, a directive inside a
//! block, a blank-node label, an unclosed block, a stray `}`.

use crate::error::{Result, TransactError};
use crate::parse::trig_meta::might_contain_graph_block;
use fluree_graph_turtle::{tokenize, Token, TokenKind, TurtleError};
use std::ops::Range;

/// A stretch of the document the parser reads in one call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum SegmentKind {
    /// Default-graph statements and directives, as written.
    Default,
    /// `{ … }`: an anonymous block, which TriG puts in the default graph.
    DefaultBlock,
    /// A labeled block. `label` is the label as written (`<g>`, `ex:g`),
    /// resolved by the driver; `at` is its byte offset.
    Named { label: String, at: usize },
}

#[derive(Clone, Debug)]
pub(super) struct Segment {
    pub(super) kind: SegmentKind,
    /// The segment's bytes. A statement the segment cuts short is reported
    /// at its end.
    pub(super) range: Range<usize>,
    /// The segment's tokens, in [`Located::tokens`].
    pub(super) tokens: Range<usize>,
}

/// A located document.
#[derive(Debug)]
pub(super) struct Located<'a> {
    /// The document.
    pub(super) text: &'a str,
    /// The document's tokens. A block's header (`GRAPH <g> {`) and its `}`
    /// are in no segment, except that the `}` of a block whose last statement
    /// omits its `.` is that `.`.
    pub(super) tokens: Vec<Token>,
    pub(super) segments: Vec<Segment>,
}

fn locate_error(position: usize, message: impl Into<String>) -> TransactError {
    TransactError::Turtle(TurtleError::parse(position, message))
}

/// Nesting delta of a token inside a statement.
fn nesting(kind: &TokenKind) -> i32 {
    match kind {
        TokenKind::LBracket
        | TokenKind::LParen
        | TokenKind::ReifiedTripleStart
        | TokenKind::TripleTermStart
        | TokenKind::AnnotationOpen => 1,
        TokenKind::RBracket
        | TokenKind::RParen
        | TokenKind::ReifiedTripleEnd
        | TokenKind::TripleTermEnd
        | TokenKind::AnnotationClose => -1,
        _ => 0,
    }
}

fn is_label(kind: &TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::Iri
            | TokenKind::IriEscaped(_)
            | TokenKind::PrefixedName
            | TokenKind::PrefixedNameNs
    )
}

fn is_directive(kind: &TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::KwPrefix
            | TokenKind::KwBase
            | TokenKind::KwSparqlPrefix
            | TokenKind::KwSparqlBase
            | TokenKind::KwVersion
            | TokenKind::KwSparqlVersion
    )
}

/// Find a TriG document's segments by reading tokens only.
pub(super) fn locate(input: &str) -> Result<Located<'_>> {
    let mut tokens = tokenize(input)?;
    let kind_at = |i: usize| tokens.get(i).map(|t: &Token| &t.kind);
    let mut segments = Vec::new();
    // The `}` of each block whose last statement omits its `.`.
    let mut dots: Vec<usize> = Vec::new();
    let mut default_start = 0usize;
    let mut default_tokens = 0usize;
    let mut i = 0usize;

    while i < tokens.len() {
        let tok = &tokens[i];
        let start = tok.start as usize;
        match &tok.kind {
            TokenKind::Eof => break,
            TokenKind::KwSparqlPrefix => {
                // `PREFIX p: <iri>` has no terminating dot.
                i += 3;
            }
            TokenKind::KwSparqlBase | TokenKind::KwSparqlVersion => {
                i += 2;
            }
            TokenKind::RBrace => {
                return Err(locate_error(start, "'}' with no graph block to close"));
            }
            TokenKind::BlankNodeLabel | TokenKind::Anon
                if matches!(kind_at(i + 1), Some(TokenKind::LBrace)) =>
            {
                return Err(locate_error(
                    start,
                    "blank-node graph label: a graph block needs an IRI label, e.g. `<iri> { ... }`",
                ));
            }
            kind => {
                // A block: `GRAPH label {`, `label {`, or `{`.
                let label_at = match kind {
                    TokenKind::KwGraph => {
                        let Some(next) = kind_at(i + 1) else {
                            return Err(locate_error(start, "expected a graph label after GRAPH"));
                        };
                        if matches!(next, TokenKind::BlankNodeLabel | TokenKind::Anon) {
                            return Err(locate_error(
                                tokens[i + 1].start as usize,
                                "blank-node graph label: a graph block needs an IRI label, e.g. \
                                 `GRAPH <iri> { ... }`",
                            ));
                        }
                        if !is_label(next) || !matches!(kind_at(i + 2), Some(TokenKind::LBrace)) {
                            return Err(locate_error(start, "expected `GRAPH <iri> { ... }`"));
                        }
                        Some(i + 1)
                    }
                    k if is_label(k) && matches!(kind_at(i + 1), Some(TokenKind::LBrace)) => {
                        Some(i)
                    }
                    TokenKind::LBrace => None,
                    _ => {
                        // A statement: skip to its terminating dot.
                        i = skip_statement(&tokens, i);
                        continue;
                    }
                };
                let brace = match label_at {
                    Some(l) => l + 1,
                    None => i,
                };
                if start > default_start {
                    segments.push(Segment {
                        kind: SegmentKind::Default,
                        range: default_start..start,
                        tokens: default_tokens..i,
                    });
                }
                let content_start = tokens[brace].end as usize;
                let (close_at, needs_dot) = scan_block(&tokens, brace + 1)?;
                let close = tokens[close_at].start as usize;
                let mut block_tokens = brace + 1..close_at;
                if needs_dot {
                    dots.push(close_at);
                    block_tokens.end += 1;
                }
                let kind = match label_at {
                    Some(l) => SegmentKind::Named {
                        label: input[tokens[l].start as usize..tokens[l].end as usize].to_string(),
                        at: tokens[l].start as usize,
                    },
                    None => SegmentKind::DefaultBlock,
                };
                segments.push(Segment {
                    kind,
                    range: content_start..close + 1,
                    tokens: block_tokens,
                });
                default_start = close + 1;
                default_tokens = close_at + 1;
                i = close_at + 1;
            }
        }
    }
    if default_start < input.len() {
        let end = tokens
            .iter()
            .rposition(|t| !matches!(t.kind, TokenKind::Eof))
            .map_or(0, |last| last + 1);
        segments.push(Segment {
            kind: SegmentKind::Default,
            range: default_start..input.len(),
            tokens: default_tokens..end.max(default_tokens),
        });
    }
    for at in dots {
        let start = tokens[at].start;
        tokens[at] = Token::new(TokenKind::Dot, start, start + 1);
    }
    Ok(Located {
        text: input,
        tokens,
        segments,
    })
}

/// Skip a top-level statement starting at `i`; returns the index after its
/// terminating dot (or of the next graph-block brace, where the parser will
/// report the missing dot).
fn skip_statement(tokens: &[Token], mut i: usize) -> usize {
    let mut depth = 0i32;
    while i < tokens.len() {
        match &tokens[i].kind {
            TokenKind::Eof => return i,
            TokenKind::Dot if depth == 0 => return i + 1,
            TokenKind::LBrace | TokenKind::RBrace if depth == 0 => return i,
            kind => depth = (depth + nesting(kind)).max(0),
        }
        i += 1;
    }
    i
}

/// Scan a block's contents from `i` (just past its `{`). Returns the index
/// of its `}` and whether its last statement omits the terminating dot.
fn scan_block(tokens: &[Token], mut i: usize) -> Result<(usize, bool)> {
    let mut depth = 0i32;
    let mut last: Option<&TokenKind> = None;
    while i < tokens.len() {
        let tok = &tokens[i];
        match &tok.kind {
            TokenKind::Eof => break,
            TokenKind::RBrace if depth == 0 => {
                let needs_dot = last.is_some_and(|k| !matches!(k, TokenKind::Dot));
                return Ok((i, needs_dot));
            }
            TokenKind::LBrace | TokenKind::KwGraph => {
                return Err(locate_error(
                    tok.start as usize,
                    "nested graph block: TriG graph blocks cannot contain graph blocks",
                ));
            }
            kind if is_directive(kind) => {
                return Err(locate_error(
                    tok.start as usize,
                    "a prefix, base or version directive cannot appear inside a graph block",
                ));
            }
            kind => depth = (depth + nesting(kind)).max(0),
        }
        last = Some(&tok.kind);
        i += 1;
    }
    let at = tokens.last().map_or(0, |t| t.end as usize);
    Err(locate_error(at, "unclosed graph block: expected '}'"))
}

/// Whether `text` holds at least one graph block, read by the locator: a
/// token pass, no parse. For callers that must tell TriG from Turtle before
/// choosing a lane.
pub fn has_graph_blocks(text: &str) -> Result<bool> {
    if !might_contain_graph_block(text) {
        return Ok(false);
    }
    Ok(locate(text)?.has_blocks())
}

impl Located<'_> {
    /// Whether the document holds a graph block (`<#txn-meta>` and
    /// anonymous `{ … }` blocks included).
    pub(super) fn has_blocks(&self) -> bool {
        self.segments
            .iter()
            .any(|s| !matches!(s.kind, SegmentKind::Default))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EX: &str = "@prefix ex: <http://example.org/> .\n";

    fn kinds(text: &str) -> Vec<SegmentKind> {
        locate(text)
            .unwrap()
            .segments
            .into_iter()
            .map(|s| s.kind)
            .collect()
    }

    #[test]
    fn the_locator_finds_blocks_in_every_spelling() {
        let doc = format!(
            "{EX}ex:a ex:p 1 .\nGRAPH <http://g/1> {{ ex:b ex:p 2 }}\nex:g2 {{ ex:c ex:p 3 . }}\n\
             {{ ex:d ex:p 4 }}\nex:e ex:p \"has {{ a brace\" .\n"
        );
        assert_eq!(
            kinds(&doc),
            vec![
                SegmentKind::Default,
                SegmentKind::Named {
                    label: "<http://g/1>".to_string(),
                    at: doc.find("<http://g/1>").unwrap()
                },
                SegmentKind::Default,
                SegmentKind::Named {
                    label: "ex:g2".to_string(),
                    at: doc.find("ex:g2").unwrap()
                },
                SegmentKind::Default,
                SegmentKind::DefaultBlock,
                SegmentKind::Default,
            ]
        );
        // A brace or the word "graph" inside a literal is not a block.
        assert_eq!(
            kinds(&format!("{EX}ex:a ex:p \"{{ graph }}\" .\n")),
            vec![SegmentKind::Default]
        );
    }

    #[test]
    fn the_locator_names_the_construct_it_refuses() {
        let refused = [
            (
                "GRAPH <http://g> { GRAPH <http://h> { } }",
                "nested graph block",
            ),
            ("<http://g> { { } }", "nested graph block"),
            ("_:b { }", "blank-node graph label"),
            ("GRAPH [] { }", "blank-node graph label"),
            (
                "GRAPH <http://g> { @prefix ex: <http://x/> . }",
                "directive",
            ),
            (
                "GRAPH <http://g> { <http://s> <http://p> 1 .",
                "unclosed graph block",
            ),
            (
                "GRAPH <http://g> { } <http://s> <http://p> 1 . }",
                "no graph block to close",
            ),
        ];
        for (doc, needle) in refused {
            let e = locate(doc).expect_err("should be refused").to_string();
            assert!(e.contains(needle), "{doc}: {e}");
        }
    }
}
