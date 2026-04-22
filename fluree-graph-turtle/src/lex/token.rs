//! Turtle Token types.
//!
//! Tokens are the output of lexical analysis, ready for parsing.
//!
//! Most token variants are **zero-copy span tokens** — they carry no data.
//! The token's `start`/`end` fields are byte offsets into the original input,
//! and the parser extracts content via `&input[start..end]`.
//!
//! A few rare variants carry pre-processed content (e.g., strings with escape
//! sequences) in an `Arc<str>`.

use std::sync::Arc;

/// A token with its source span.
#[derive(Clone, Debug, PartialEq)]
pub struct Token {
    /// The token kind
    pub kind: TokenKind,
    /// Source location (start byte offset)
    pub start: u32,
    /// Source location (end byte offset)
    pub end: u32,
}

impl Token {
    /// Create a new token.
    pub fn new(kind: TokenKind, start: u32, end: u32) -> Self {
        Self { kind, start, end }
    }

    /// Check if this is an EOF token.
    pub fn is_eof(&self) -> bool {
        matches!(self.kind, TokenKind::Eof)
    }
}

/// Token kinds for Turtle.
///
/// Most variants store no data — content is recovered from the source input
/// using the token's byte span (`start..end`). The parser uses kind-specific
/// offset adjustments to strip delimiters (e.g., `<>` for IRIs, `""` for
/// strings).
///
/// Variants with `Escaped` suffix carry pre-processed content for the rare
/// case where escape sequences altered the text.
#[derive(Clone, Debug, PartialEq)]
pub enum TokenKind {
    // =========================================================================
    // IRIs
    // =========================================================================
    /// Full IRI: `<http://example.org/>`
    /// Span covers the entire token including `<>`.
    /// Content: `&input[(start+1)..(end-1)]`
    Iri,

    /// Full IRI with unicode escapes (rare).
    /// Content is pre-processed and stored inline.
    IriEscaped(Arc<str>),

    /// Prefixed name namespace: `prefix:` (just the prefix with trailing colon).
    /// Span covers `prefix:`.
    /// Prefix: `&input[start..(end-1)]`
    PrefixedNameNs,

    /// Prefixed name with local: `prefix:local`
    /// Span covers `prefix:local`.
    /// Split on first `:` to get `(prefix, local)`.
    PrefixedName,

    // =========================================================================
    // Blank Nodes
    // =========================================================================
    /// Labeled blank node: `_:name`
    /// Span covers `_:name`.
    /// Label: `&input[(start+2)..end]`
    BlankNodeLabel,

    /// Anonymous blank node: `[]`
    Anon,

    /// NIL (empty list): `()`
    Nil,

    // =========================================================================
    // Literals
    // =========================================================================
    /// Short string literal (no escapes): `"..."` or `'...'`
    /// Span covers the entire token including quotes.
    /// Content: `&input[(start+1)..(end-1)]`
    String,

    /// Long string literal (no escapes): `"""..."""` or `'''...'''`
    /// Span covers the entire token including triple quotes.
    /// Content: `&input[(start+3)..(end-3)]`
    LongString,

    /// String literal with escape sequences (rare).
    /// Content is pre-processed and stored inline.
    StringEscaped(Arc<str>),

    /// Integer literal (parsed inline).
    Integer(i64),

    /// Decimal literal.
    /// Span covers the numeric text.
    /// Text: `&input[start..end]`
    Decimal,

    /// Double literal (parsed inline).
    Double(f64),

    /// Language tag (e.g., `@en`, `@en-US`).
    /// Span covers `@tag`.
    /// Tag: `&input[(start+1)..end]`
    LangTag,

    // =========================================================================
    // Keywords / Directives
    // =========================================================================
    /// `@prefix` directive
    KwPrefix,

    /// `@base` directive
    KwBase,

    /// SPARQL-style `PREFIX` (without @)
    KwSparqlPrefix,

    /// SPARQL-style `BASE` (without @)
    KwSparqlBase,

    /// `a` keyword (shorthand for rdf:type)
    KwA,

    /// `true` boolean literal
    KwTrue,

    /// `false` boolean literal
    KwFalse,

    /// TriG `GRAPH` keyword
    KwGraph,

    // =========================================================================
    // Punctuation
    // =========================================================================
    /// `.`
    Dot,
    /// `,`
    Comma,
    /// `;`
    Semicolon,
    /// `^^` (datatype marker)
    DoubleCaret,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `{` (TriG graph block open)
    LBrace,
    /// `}` (TriG graph block close)
    RBrace,

    // =========================================================================
    // Special
    // =========================================================================
    /// End of input
    Eof,
}

impl std::fmt::Display for TokenKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenKind::Iri => write!(f, "<IRI>"),
            TokenKind::IriEscaped(s) => write!(f, "<{s}>"),
            TokenKind::PrefixedNameNs => write!(f, "prefixedNs:"),
            TokenKind::PrefixedName => write!(f, "prefixed:name"),
            TokenKind::BlankNodeLabel => write!(f, "_:blank"),
            TokenKind::Anon => write!(f, "[]"),
            TokenKind::Nil => write!(f, "()"),
            TokenKind::String => write!(f, "\"string\""),
            TokenKind::LongString => write!(f, "\"\"\"string\"\"\""),
            TokenKind::StringEscaped(s) => write!(f, "\"{s}\""),
            TokenKind::Integer(n) => write!(f, "{n}"),
            TokenKind::Decimal => write!(f, "decimal"),
            TokenKind::Double(n) => write!(f, "{n:e}"),
            TokenKind::LangTag => write!(f, "@lang"),
            TokenKind::KwPrefix => write!(f, "@prefix"),
            TokenKind::KwBase => write!(f, "@base"),
            TokenKind::KwSparqlPrefix => write!(f, "PREFIX"),
            TokenKind::KwSparqlBase => write!(f, "BASE"),
            TokenKind::KwA => write!(f, "a"),
            TokenKind::KwTrue => write!(f, "true"),
            TokenKind::KwFalse => write!(f, "false"),
            TokenKind::KwGraph => write!(f, "GRAPH"),
            TokenKind::Dot => write!(f, "."),
            TokenKind::Comma => write!(f, ","),
            TokenKind::Semicolon => write!(f, ";"),
            TokenKind::DoubleCaret => write!(f, "^^"),
            TokenKind::LBracket => write!(f, "["),
            TokenKind::RBracket => write!(f, "]"),
            TokenKind::LParen => write!(f, "("),
            TokenKind::RParen => write!(f, ")"),
            TokenKind::LBrace => write!(f, "{{"),
            TokenKind::RBrace => write!(f, "}}"),
            TokenKind::Eof => write!(f, "EOF"),
        }
    }
}
