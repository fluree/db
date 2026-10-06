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

    /// Integer literal that overflows i64 (xsd:integer is unbounded).
    /// Span covers the numeric text; promoted to BigInt downstream.
    /// Text: `&input[start..end]`
    IntegerOverflow,

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

    /// RDF 1.2 `@version` directive (terminated by `.`)
    KwVersion,

    /// RDF 1.2 SPARQL-style `VERSION` directive (no `.`)
    KwSparqlVersion,

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
    // RDF 1.2 (Turtle-star)
    // =========================================================================
    /// `<<` — reified-triple open (RDF 1.2 asserting form)
    ReifiedTripleStart,
    /// `>>` — reified-triple close
    ReifiedTripleEnd,
    /// `<<(` — triple-term open (RDF 1.2 triple terms as values).
    /// Accepted by the parser only as the object of `rdf:reifies`; any
    /// other position is rejected with a specific deferred error.
    TripleTermStart,
    /// `)>>` — triple-term close
    TripleTermEnd,
    /// `{|` — annotation block open
    AnnotationOpen,
    /// `|}` — annotation block close
    AnnotationClose,
    /// `~` — reifier marker
    Tilde,

    // =========================================================================
    // Special
    // =========================================================================
    /// End of input
    Eof,
}

/// User-facing description for parse errors: punctuation and keywords are
/// quoted as written, value tokens are named by kind.
impl std::fmt::Display for TokenKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            TokenKind::Iri | TokenKind::IriEscaped(_) => return f.write_str("an IRI"),
            TokenKind::PrefixedNameNs | TokenKind::PrefixedName => {
                return f.write_str("a prefixed name")
            }
            TokenKind::BlankNodeLabel => return f.write_str("a blank node label"),
            TokenKind::String | TokenKind::StringEscaped(_) => {
                return f.write_str("a string literal")
            }
            TokenKind::LongString => return f.write_str("a long string literal"),
            TokenKind::Integer(_) | TokenKind::IntegerOverflow => return f.write_str("an integer"),
            TokenKind::Decimal => return f.write_str("a decimal"),
            TokenKind::Double(_) => return f.write_str("a double"),
            TokenKind::LangTag => return f.write_str("a language tag"),
            TokenKind::Eof => return f.write_str("end of input"),
            TokenKind::Anon => "[]",
            TokenKind::Nil => "()",
            TokenKind::KwPrefix => "@prefix",
            TokenKind::KwBase => "@base",
            TokenKind::KwSparqlPrefix => "PREFIX",
            TokenKind::KwSparqlBase => "BASE",
            TokenKind::KwVersion => "@version",
            TokenKind::KwSparqlVersion => "VERSION",
            TokenKind::KwA => "a",
            TokenKind::KwTrue => "true",
            TokenKind::KwFalse => "false",
            TokenKind::KwGraph => "GRAPH",
            TokenKind::Dot => ".",
            TokenKind::Comma => ",",
            TokenKind::Semicolon => ";",
            TokenKind::DoubleCaret => "^^",
            TokenKind::LBracket => "[",
            TokenKind::RBracket => "]",
            TokenKind::LParen => "(",
            TokenKind::RParen => ")",
            TokenKind::LBrace => "{",
            TokenKind::RBrace => "}",
            TokenKind::ReifiedTripleStart => "<<",
            TokenKind::ReifiedTripleEnd => ">>",
            TokenKind::TripleTermStart => "<<(",
            TokenKind::TripleTermEnd => ")>>",
            TokenKind::AnnotationOpen => "{|",
            TokenKind::AnnotationClose => "|}",
            TokenKind::Tilde => "~",
        };
        write!(f, "'{text}'")
    }
}
