//! SPARQL Token types.
//!
//! Tokens are the output of lexical analysis, ready for parsing.
//! Each token carries its source span for precise diagnostics.

use crate::span::SourceSpan;
use std::sync::Arc;

/// A token with its source span.
#[derive(Clone, Debug, PartialEq)]
pub struct Token {
    /// The token kind
    pub kind: TokenKind,
    /// Source location
    pub span: SourceSpan,
}

impl Token {
    /// Create a new token.
    pub fn new(kind: TokenKind, span: SourceSpan) -> Self {
        Self { kind, span }
    }

    /// Create a token from a range.
    pub fn from_range(kind: TokenKind, start: usize, end: usize) -> Self {
        Self {
            kind,
            span: SourceSpan::new(start, end),
        }
    }

    /// Check if this token is of a specific kind.
    pub fn is(&self, kind: TokenKind) -> bool {
        std::mem::discriminant(&self.kind) == std::mem::discriminant(&kind)
    }

    /// Check if this is an EOF token.
    pub fn is_eof(&self) -> bool {
        matches!(self.kind, TokenKind::Eof)
    }
}

/// Token kinds for SPARQL.
///
/// Based on SPARQL 1.1 grammar terminals.
#[derive(Clone, Debug, PartialEq)]
pub enum TokenKind {
    // =========================================================================
    // IRIs
    // =========================================================================
    /// Full IRI: `<http://example.org/>`
    Iri(Arc<str>),

    /// Prefixed name namespace: `prefix:` (includes the colon)
    PrefixedNameNs(Arc<str>),

    /// Prefixed name with local: `prefix:local`
    PrefixedName {
        /// Namespace prefix (without colon)
        prefix: Arc<str>,
        /// Local name
        local: Arc<str>,
    },

    // =========================================================================
    // Variables
    // =========================================================================
    /// Variable: `?name` or `$name` (stored without the sigil)
    Var(Arc<str>),

    // =========================================================================
    // Literals
    // =========================================================================
    /// String literal (unescaped content)
    String(Arc<str>),

    /// Integer literal
    Integer(i64),

    /// Decimal literal (stored as string to preserve precision)
    Decimal(Arc<str>),

    /// Double literal (floating point)
    Double(f64),

    /// Language tag (e.g., `@en`, `@en-US`)
    ///
    /// Stored without the `@` prefix.
    LangTag(Arc<str>),

    // Note: Boolean literals are lexed as keywords (KwTrue, KwFalse)
    // to avoid ambiguity with prefixed names. The parser can convert
    // them to boolean values if needed.

    // =========================================================================
    // Blank Nodes
    // =========================================================================
    /// Labeled blank node: `_:name`
    BlankNodeLabel(Arc<str>),

    /// Anonymous blank node: `[]`
    Anon,

    /// NIL (empty list): `()`
    Nil,

    // =========================================================================
    // Keywords (case-insensitive in SPARQL)
    // =========================================================================
    // Query forms
    KwSelect,
    KwConstruct,
    KwDescribe,
    KwAsk,

    // Dataset clauses
    KwFrom,
    KwNamed,

    // Where clause
    KwWhere,

    // Graph patterns
    KwOptional,
    KwGraph,
    KwService,
    KwSilent,
    KwBind,
    KwAs,
    KwValues,
    KwMinus,
    KwUnion,
    KwFilter,

    // Solution modifiers
    KwGroupBy,
    KwHaving,
    KwOrderBy,
    KwAsc,
    KwDesc,
    KwLimit,
    KwOffset,
    KwDistinct,
    KwReduced,

    // Aggregates
    KwCount,
    KwSum,
    KwMin,
    KwMax,
    KwAvg,
    KwSample,
    KwGroupConcat,
    KwSeparator,

    // Boolean operators
    KwNot,
    KwIn,
    KwExists,

    // Built-in functions
    KwBound,
    KwIf,
    KwCoalesce,

    // Type checking functions
    KwIsIri,
    KwIsUri,
    KwIsBlank,
    KwIsLiteral,
    KwIsNumeric,

    // Accessor functions
    KwStr,
    KwLang,
    KwDatatype,

    // Constructor functions
    KwIri,
    KwUri,
    KwBNode,

    // String functions
    KwStrlen,
    KwSubstr,
    KwUcase,
    KwLcase,
    KwStrStarts,
    KwStrEnds,
    KwContains,
    KwStrBefore,
    KwStrAfter,
    KwEncodeForUri,
    KwConcat,
    KwLangMatches,
    KwRegex,
    KwReplace,
    KwStrDt,
    KwStrLang,

    // Numeric functions
    KwAbs,
    KwRound,
    KwCeil,
    KwFloor,
    KwRand,

    // Date/time functions
    KwNow,
    KwYear,
    KwMonth,
    KwDay,
    KwHours,
    KwMinutes,
    KwSeconds,
    KwTimezone,
    KwTz,

    // Hash functions
    KwMd5,
    KwSha1,
    KwSha256,
    KwSha384,
    KwSha512,

    // Comparison functions
    KwSameTerm,

    // UUID functions
    KwUuid,
    KwStrUuid,

    // Vector similarity functions (Fluree extensions)
    KwDotProduct,
    KwCosineSimilarity,
    KwEuclideanDistance,

    // Prologue
    KwBase,
    KwPrefix,

    // Update
    KwInsert,
    KwDelete,
    KwData,
    KwWith,
    KwUsing,
    KwDefault,
    KwAll,
    KwLoad,
    KwInto,
    KwClear,
    KwDrop,
    KwCreate,
    KwAdd,
    KwMove,
    KwCopy,
    KwTo,

    // Type/predicate shorthand
    /// `a` keyword (shorthand for rdf:type)
    KwA,

    // Boolean literals (also keywords)
    KwTrue,
    KwFalse,

    // Special
    KwUndef,
    /// `BY` (used with GROUP BY, ORDER BY)
    KwBy,

    // =========================================================================
    // Punctuation / Operators
    // =========================================================================
    /// `{`
    LBrace,
    /// `}`
    RBrace,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `[`
    LBracket,
    /// `]`
    RBracket,

    // RDF-star quoted triple delimiters
    /// `<<` (quoted triple start)
    TripleStart,
    /// `>>` (quoted triple end)
    TripleEnd,

    /// `.`
    Dot,
    /// `,`
    Comma,
    /// `;`
    Semicolon,

    /// `^^` (datatype marker)
    DoubleCaret,
    /// `@` (language tag marker)
    At,

    /// `||`
    Or,
    /// `&&`
    And,

    /// `=`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `>`
    Gt,
    /// `<=`
    Le,
    /// `>=`
    Ge,

    /// `+`
    Plus,
    /// `-`
    Minus,
    /// `*`
    Star,
    /// `/`
    Slash,

    /// `!`
    Bang,
    /// `?` (in property paths)
    Question,
    /// `|` (in property paths)
    Pipe,
    /// `^` (inverse in property paths)
    Caret,

    // =========================================================================
    // Special
    // =========================================================================
    /// End of input
    Eof,

    /// Lexer error (includes error message)
    Error(Arc<str>),
}

impl TokenKind {
    /// Check if this token is a keyword.
    pub fn is_keyword(&self) -> bool {
        matches!(
            self,
            TokenKind::KwSelect
                | TokenKind::KwConstruct
                | TokenKind::KwDescribe
                | TokenKind::KwAsk
                | TokenKind::KwFrom
                | TokenKind::KwNamed
                | TokenKind::KwWhere
                | TokenKind::KwOptional
                | TokenKind::KwGraph
                | TokenKind::KwService
                | TokenKind::KwSilent
                | TokenKind::KwBind
                | TokenKind::KwAs
                | TokenKind::KwValues
                | TokenKind::KwMinus
                | TokenKind::KwUnion
                | TokenKind::KwFilter
                | TokenKind::KwGroupBy
                | TokenKind::KwHaving
                | TokenKind::KwOrderBy
                | TokenKind::KwAsc
                | TokenKind::KwDesc
                | TokenKind::KwLimit
                | TokenKind::KwOffset
                | TokenKind::KwDistinct
                | TokenKind::KwReduced
                | TokenKind::KwCount
                | TokenKind::KwSum
                | TokenKind::KwMin
                | TokenKind::KwMax
                | TokenKind::KwAvg
                | TokenKind::KwSample
                | TokenKind::KwGroupConcat
                | TokenKind::KwSeparator
                | TokenKind::KwNot
                | TokenKind::KwIn
                | TokenKind::KwExists
                | TokenKind::KwBound
                | TokenKind::KwIf
                | TokenKind::KwCoalesce
                | TokenKind::KwIsIri
                | TokenKind::KwIsUri
                | TokenKind::KwIsBlank
                | TokenKind::KwIsLiteral
                | TokenKind::KwIsNumeric
                | TokenKind::KwStr
                | TokenKind::KwLang
                | TokenKind::KwDatatype
                | TokenKind::KwIri
                | TokenKind::KwUri
                | TokenKind::KwBNode
                | TokenKind::KwStrlen
                | TokenKind::KwSubstr
                | TokenKind::KwUcase
                | TokenKind::KwLcase
                | TokenKind::KwStrStarts
                | TokenKind::KwStrEnds
                | TokenKind::KwContains
                | TokenKind::KwStrBefore
                | TokenKind::KwStrAfter
                | TokenKind::KwEncodeForUri
                | TokenKind::KwConcat
                | TokenKind::KwLangMatches
                | TokenKind::KwRegex
                | TokenKind::KwReplace
                | TokenKind::KwStrDt
                | TokenKind::KwStrLang
                | TokenKind::KwAbs
                | TokenKind::KwRound
                | TokenKind::KwCeil
                | TokenKind::KwFloor
                | TokenKind::KwRand
                | TokenKind::KwNow
                | TokenKind::KwYear
                | TokenKind::KwMonth
                | TokenKind::KwDay
                | TokenKind::KwHours
                | TokenKind::KwMinutes
                | TokenKind::KwSeconds
                | TokenKind::KwTimezone
                | TokenKind::KwTz
                | TokenKind::KwMd5
                | TokenKind::KwSha1
                | TokenKind::KwSha256
                | TokenKind::KwSha384
                | TokenKind::KwSha512
                | TokenKind::KwSameTerm
                | TokenKind::KwUuid
                | TokenKind::KwStrUuid
                | TokenKind::KwBase
                | TokenKind::KwPrefix
                | TokenKind::KwInsert
                | TokenKind::KwDelete
                | TokenKind::KwData
                | TokenKind::KwWith
                | TokenKind::KwUsing
                | TokenKind::KwDefault
                | TokenKind::KwAll
                | TokenKind::KwLoad
                | TokenKind::KwInto
                | TokenKind::KwClear
                | TokenKind::KwDrop
                | TokenKind::KwCreate
                | TokenKind::KwAdd
                | TokenKind::KwMove
                | TokenKind::KwCopy
                | TokenKind::KwTo
                | TokenKind::KwA
                | TokenKind::KwTrue
                | TokenKind::KwFalse
                | TokenKind::KwUndef
                | TokenKind::KwBy
                | TokenKind::KwDotProduct
                | TokenKind::KwCosineSimilarity
                | TokenKind::KwEuclideanDistance
        )
    }

    /// Check if this is a literal token.
    ///
    /// Note: Boolean literals (true/false) are lexed as keywords,
    /// not as literals, so they are not included here.
    pub fn is_literal(&self) -> bool {
        matches!(
            self,
            TokenKind::String(_)
                | TokenKind::Integer(_)
                | TokenKind::Decimal(_)
                | TokenKind::Double(_)
        )
    }

    /// Get the keyword string for error messages (if this is a keyword).
    pub fn keyword_str(&self) -> Option<&'static str> {
        match self {
            TokenKind::KwSelect => Some("SELECT"),
            TokenKind::KwConstruct => Some("CONSTRUCT"),
            TokenKind::KwDescribe => Some("DESCRIBE"),
            TokenKind::KwAsk => Some("ASK"),
            TokenKind::KwFrom => Some("FROM"),
            TokenKind::KwNamed => Some("NAMED"),
            TokenKind::KwWhere => Some("WHERE"),
            TokenKind::KwOptional => Some("OPTIONAL"),
            TokenKind::KwGraph => Some("GRAPH"),
            TokenKind::KwService => Some("SERVICE"),
            TokenKind::KwSilent => Some("SILENT"),
            TokenKind::KwBind => Some("BIND"),
            TokenKind::KwAs => Some("AS"),
            TokenKind::KwValues => Some("VALUES"),
            TokenKind::KwMinus => Some("MINUS"),
            TokenKind::KwUnion => Some("UNION"),
            TokenKind::KwFilter => Some("FILTER"),
            TokenKind::KwGroupBy => Some("GROUP BY"),
            TokenKind::KwHaving => Some("HAVING"),
            TokenKind::KwOrderBy => Some("ORDER BY"),
            TokenKind::KwAsc => Some("ASC"),
            TokenKind::KwDesc => Some("DESC"),
            TokenKind::KwLimit => Some("LIMIT"),
            TokenKind::KwOffset => Some("OFFSET"),
            TokenKind::KwDistinct => Some("DISTINCT"),
            TokenKind::KwReduced => Some("REDUCED"),
            TokenKind::KwCount => Some("COUNT"),
            TokenKind::KwSum => Some("SUM"),
            TokenKind::KwMin => Some("MIN"),
            TokenKind::KwMax => Some("MAX"),
            TokenKind::KwAvg => Some("AVG"),
            TokenKind::KwSample => Some("SAMPLE"),
            TokenKind::KwGroupConcat => Some("GROUP_CONCAT"),
            TokenKind::KwSeparator => Some("SEPARATOR"),
            TokenKind::KwNot => Some("NOT"),
            TokenKind::KwIn => Some("IN"),
            TokenKind::KwExists => Some("EXISTS"),
            TokenKind::KwBound => Some("BOUND"),
            TokenKind::KwIf => Some("IF"),
            TokenKind::KwCoalesce => Some("COALESCE"),
            TokenKind::KwIsIri => Some("ISIRI"),
            TokenKind::KwIsUri => Some("ISURI"),
            TokenKind::KwIsBlank => Some("ISBLANK"),
            TokenKind::KwIsLiteral => Some("ISLITERAL"),
            TokenKind::KwIsNumeric => Some("ISNUMERIC"),
            TokenKind::KwStr => Some("STR"),
            TokenKind::KwLang => Some("LANG"),
            TokenKind::KwDatatype => Some("DATATYPE"),
            TokenKind::KwIri => Some("IRI"),
            TokenKind::KwUri => Some("URI"),
            TokenKind::KwBNode => Some("BNODE"),
            TokenKind::KwStrlen => Some("STRLEN"),
            TokenKind::KwSubstr => Some("SUBSTR"),
            TokenKind::KwUcase => Some("UCASE"),
            TokenKind::KwLcase => Some("LCASE"),
            TokenKind::KwStrStarts => Some("STRSTARTS"),
            TokenKind::KwStrEnds => Some("STRENDS"),
            TokenKind::KwContains => Some("CONTAINS"),
            TokenKind::KwStrBefore => Some("STRBEFORE"),
            TokenKind::KwStrAfter => Some("STRAFTER"),
            TokenKind::KwEncodeForUri => Some("ENCODE_FOR_URI"),
            TokenKind::KwConcat => Some("CONCAT"),
            TokenKind::KwLangMatches => Some("LANGMATCHES"),
            TokenKind::KwRegex => Some("REGEX"),
            TokenKind::KwReplace => Some("REPLACE"),
            TokenKind::KwStrDt => Some("STRDT"),
            TokenKind::KwStrLang => Some("STRLANG"),
            TokenKind::KwAbs => Some("ABS"),
            TokenKind::KwRound => Some("ROUND"),
            TokenKind::KwCeil => Some("CEIL"),
            TokenKind::KwFloor => Some("FLOOR"),
            TokenKind::KwRand => Some("RAND"),
            TokenKind::KwNow => Some("NOW"),
            TokenKind::KwYear => Some("YEAR"),
            TokenKind::KwMonth => Some("MONTH"),
            TokenKind::KwDay => Some("DAY"),
            TokenKind::KwHours => Some("HOURS"),
            TokenKind::KwMinutes => Some("MINUTES"),
            TokenKind::KwSeconds => Some("SECONDS"),
            TokenKind::KwTimezone => Some("TIMEZONE"),
            TokenKind::KwTz => Some("TZ"),
            TokenKind::KwMd5 => Some("MD5"),
            TokenKind::KwSha1 => Some("SHA1"),
            TokenKind::KwSha256 => Some("SHA256"),
            TokenKind::KwSha384 => Some("SHA384"),
            TokenKind::KwSha512 => Some("SHA512"),
            TokenKind::KwSameTerm => Some("SAMETERM"),
            TokenKind::KwUuid => Some("UUID"),
            TokenKind::KwStrUuid => Some("STRUUID"),
            TokenKind::KwBase => Some("BASE"),
            TokenKind::KwPrefix => Some("PREFIX"),
            TokenKind::KwInsert => Some("INSERT"),
            TokenKind::KwDelete => Some("DELETE"),
            TokenKind::KwData => Some("DATA"),
            TokenKind::KwWith => Some("WITH"),
            TokenKind::KwUsing => Some("USING"),
            TokenKind::KwDefault => Some("DEFAULT"),
            TokenKind::KwAll => Some("ALL"),
            TokenKind::KwLoad => Some("LOAD"),
            TokenKind::KwInto => Some("INTO"),
            TokenKind::KwClear => Some("CLEAR"),
            TokenKind::KwDrop => Some("DROP"),
            TokenKind::KwCreate => Some("CREATE"),
            TokenKind::KwAdd => Some("ADD"),
            TokenKind::KwMove => Some("MOVE"),
            TokenKind::KwCopy => Some("COPY"),
            TokenKind::KwTo => Some("TO"),
            TokenKind::KwA => Some("a"),
            TokenKind::KwTrue => Some("true"),
            TokenKind::KwFalse => Some("false"),
            TokenKind::KwUndef => Some("UNDEF"),
            TokenKind::KwBy => Some("BY"),
            TokenKind::KwDotProduct => Some("dotProduct"),
            TokenKind::KwCosineSimilarity => Some("cosineSimilarity"),
            TokenKind::KwEuclideanDistance => Some("euclideanDistance"),
            _ => None,
        }
    }
}

impl std::fmt::Display for TokenKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenKind::Iri(s) => write!(f, "<{s}>"),
            TokenKind::PrefixedNameNs(s) => write!(f, "{s}:"),
            TokenKind::PrefixedName { prefix, local } => write!(f, "{prefix}:{local}"),
            TokenKind::Var(s) => write!(f, "?{s}"),
            TokenKind::String(s) => write!(f, "\"{s}\""),
            TokenKind::Integer(n) => write!(f, "{n}"),
            TokenKind::Decimal(s) => write!(f, "{s}"),
            TokenKind::Double(n) => write!(f, "{n:e}"),
            TokenKind::LangTag(s) => write!(f, "@{s}"),
            TokenKind::BlankNodeLabel(s) => write!(f, "_:{s}"),
            TokenKind::Anon => write!(f, "[]"),
            TokenKind::Nil => write!(f, "()"),
            TokenKind::LBrace => write!(f, "{{"),
            TokenKind::RBrace => write!(f, "}}"),
            TokenKind::LParen => write!(f, "("),
            TokenKind::RParen => write!(f, ")"),
            TokenKind::LBracket => write!(f, "["),
            TokenKind::RBracket => write!(f, "]"),
            TokenKind::TripleStart => write!(f, "<<"),
            TokenKind::TripleEnd => write!(f, ">>"),
            TokenKind::Dot => write!(f, "."),
            TokenKind::Comma => write!(f, ","),
            TokenKind::Semicolon => write!(f, ";"),
            TokenKind::DoubleCaret => write!(f, "^^"),
            TokenKind::At => write!(f, "@"),
            TokenKind::Or => write!(f, "||"),
            TokenKind::And => write!(f, "&&"),
            TokenKind::Eq => write!(f, "="),
            TokenKind::Ne => write!(f, "!="),
            TokenKind::Lt => write!(f, "<"),
            TokenKind::Gt => write!(f, ">"),
            TokenKind::Le => write!(f, "<="),
            TokenKind::Ge => write!(f, ">="),
            TokenKind::Plus => write!(f, "+"),
            TokenKind::Minus => write!(f, "-"),
            TokenKind::Star => write!(f, "*"),
            TokenKind::Slash => write!(f, "/"),
            TokenKind::Bang => write!(f, "!"),
            TokenKind::Question => write!(f, "?"),
            TokenKind::Pipe => write!(f, "|"),
            TokenKind::Caret => write!(f, "^"),
            TokenKind::Eof => write!(f, "EOF"),
            TokenKind::Error(s) => write!(f, "error: {s}"),
            k if k.is_keyword() => write!(f, "{}", k.keyword_str().unwrap_or("KEYWORD")),
            _ => write!(f, "?"),
        }
    }
}

/// Map a string to its keyword token kind (case-insensitive).
pub fn keyword_from_str(s: &str) -> Option<TokenKind> {
    // Note: SPARQL keywords are case-insensitive
    match s.to_ascii_uppercase().as_str() {
        "SELECT" => Some(TokenKind::KwSelect),
        "CONSTRUCT" => Some(TokenKind::KwConstruct),
        "DESCRIBE" => Some(TokenKind::KwDescribe),
        "ASK" => Some(TokenKind::KwAsk),
        "FROM" => Some(TokenKind::KwFrom),
        "NAMED" => Some(TokenKind::KwNamed),
        "WHERE" => Some(TokenKind::KwWhere),
        "OPTIONAL" => Some(TokenKind::KwOptional),
        "GRAPH" => Some(TokenKind::KwGraph),
        "SERVICE" => Some(TokenKind::KwService),
        "SILENT" => Some(TokenKind::KwSilent),
        "BIND" => Some(TokenKind::KwBind),
        "AS" => Some(TokenKind::KwAs),
        "VALUES" => Some(TokenKind::KwValues),
        "MINUS" => Some(TokenKind::KwMinus),
        "UNION" => Some(TokenKind::KwUnion),
        "FILTER" => Some(TokenKind::KwFilter),
        "GROUP" => Some(TokenKind::KwGroupBy), // "GROUP" alone, parser handles "BY"
        "HAVING" => Some(TokenKind::KwHaving),
        "ORDER" => Some(TokenKind::KwOrderBy), // "ORDER" alone, parser handles "BY"
        "ASC" => Some(TokenKind::KwAsc),
        "DESC" => Some(TokenKind::KwDesc),
        "LIMIT" => Some(TokenKind::KwLimit),
        "OFFSET" => Some(TokenKind::KwOffset),
        "DISTINCT" => Some(TokenKind::KwDistinct),
        "REDUCED" => Some(TokenKind::KwReduced),
        "COUNT" => Some(TokenKind::KwCount),
        "SUM" => Some(TokenKind::KwSum),
        "MIN" => Some(TokenKind::KwMin),
        "MAX" => Some(TokenKind::KwMax),
        "AVG" => Some(TokenKind::KwAvg),
        "SAMPLE" => Some(TokenKind::KwSample),
        "GROUP_CONCAT" => Some(TokenKind::KwGroupConcat),
        "SEPARATOR" => Some(TokenKind::KwSeparator),
        "NOT" => Some(TokenKind::KwNot),
        "IN" => Some(TokenKind::KwIn),
        "EXISTS" => Some(TokenKind::KwExists),
        "BOUND" => Some(TokenKind::KwBound),
        "IF" => Some(TokenKind::KwIf),
        "COALESCE" => Some(TokenKind::KwCoalesce),
        "ISIRI" => Some(TokenKind::KwIsIri),
        "ISURI" => Some(TokenKind::KwIsUri),
        "ISBLANK" => Some(TokenKind::KwIsBlank),
        "ISLITERAL" => Some(TokenKind::KwIsLiteral),
        "ISNUMERIC" => Some(TokenKind::KwIsNumeric),
        "STR" => Some(TokenKind::KwStr),
        "LANG" => Some(TokenKind::KwLang),
        "DATATYPE" => Some(TokenKind::KwDatatype),
        "IRI" => Some(TokenKind::KwIri),
        "URI" => Some(TokenKind::KwUri),
        "BNODE" => Some(TokenKind::KwBNode),
        "STRLEN" => Some(TokenKind::KwStrlen),
        "SUBSTR" => Some(TokenKind::KwSubstr),
        "UCASE" => Some(TokenKind::KwUcase),
        "LCASE" => Some(TokenKind::KwLcase),
        "STRSTARTS" => Some(TokenKind::KwStrStarts),
        "STRENDS" => Some(TokenKind::KwStrEnds),
        "CONTAINS" => Some(TokenKind::KwContains),
        "STRBEFORE" => Some(TokenKind::KwStrBefore),
        "STRAFTER" => Some(TokenKind::KwStrAfter),
        "ENCODE_FOR_URI" => Some(TokenKind::KwEncodeForUri),
        "CONCAT" => Some(TokenKind::KwConcat),
        "LANGMATCHES" => Some(TokenKind::KwLangMatches),
        "REGEX" => Some(TokenKind::KwRegex),
        "REPLACE" => Some(TokenKind::KwReplace),
        "STRDT" => Some(TokenKind::KwStrDt),
        "STRLANG" => Some(TokenKind::KwStrLang),
        "ABS" => Some(TokenKind::KwAbs),
        "ROUND" => Some(TokenKind::KwRound),
        "CEIL" => Some(TokenKind::KwCeil),
        "FLOOR" => Some(TokenKind::KwFloor),
        "RAND" => Some(TokenKind::KwRand),
        "NOW" => Some(TokenKind::KwNow),
        "YEAR" => Some(TokenKind::KwYear),
        "MONTH" => Some(TokenKind::KwMonth),
        "DAY" => Some(TokenKind::KwDay),
        "HOURS" => Some(TokenKind::KwHours),
        "MINUTES" => Some(TokenKind::KwMinutes),
        "SECONDS" => Some(TokenKind::KwSeconds),
        "TIMEZONE" => Some(TokenKind::KwTimezone),
        "TZ" => Some(TokenKind::KwTz),
        "MD5" => Some(TokenKind::KwMd5),
        "SHA1" => Some(TokenKind::KwSha1),
        "SHA256" => Some(TokenKind::KwSha256),
        "SHA384" => Some(TokenKind::KwSha384),
        "SHA512" => Some(TokenKind::KwSha512),
        "SAMETERM" => Some(TokenKind::KwSameTerm),
        "UUID" => Some(TokenKind::KwUuid),
        "STRUUID" => Some(TokenKind::KwStrUuid),
        // Vector similarity functions (Fluree extensions, case-insensitive with underscore variants)
        "DOTPRODUCT" | "DOT_PRODUCT" => Some(TokenKind::KwDotProduct),
        "COSINESIMILARITY" | "COSINE_SIMILARITY" => Some(TokenKind::KwCosineSimilarity),
        "EUCLIDEANDISTANCE" | "EUCLIDEAN_DISTANCE" | "EUCLIDIANDISTANCE" => {
            Some(TokenKind::KwEuclideanDistance)
        }
        "BASE" => Some(TokenKind::KwBase),
        "PREFIX" => Some(TokenKind::KwPrefix),
        "INSERT" => Some(TokenKind::KwInsert),
        "DELETE" => Some(TokenKind::KwDelete),
        "DATA" => Some(TokenKind::KwData),
        "WITH" => Some(TokenKind::KwWith),
        "USING" => Some(TokenKind::KwUsing),
        "DEFAULT" => Some(TokenKind::KwDefault),
        "ALL" => Some(TokenKind::KwAll),
        "LOAD" => Some(TokenKind::KwLoad),
        "INTO" => Some(TokenKind::KwInto),
        "CLEAR" => Some(TokenKind::KwClear),
        "DROP" => Some(TokenKind::KwDrop),
        "CREATE" => Some(TokenKind::KwCreate),
        "ADD" => Some(TokenKind::KwAdd),
        "MOVE" => Some(TokenKind::KwMove),
        "COPY" => Some(TokenKind::KwCopy),
        "TO" => Some(TokenKind::KwTo),
        "TRUE" => Some(TokenKind::KwTrue),
        "FALSE" => Some(TokenKind::KwFalse),
        "UNDEF" => Some(TokenKind::KwUndef),
        "BY" => Some(TokenKind::KwBy),
        // Special case: 'a' is only a keyword when lowercase
        _ if s == "a" => Some(TokenKind::KwA),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_lookup() {
        assert_eq!(keyword_from_str("SELECT"), Some(TokenKind::KwSelect));
        assert_eq!(keyword_from_str("select"), Some(TokenKind::KwSelect));
        assert_eq!(keyword_from_str("SeLeCt"), Some(TokenKind::KwSelect));
        assert_eq!(keyword_from_str("a"), Some(TokenKind::KwA));
        assert_eq!(keyword_from_str("A"), None); // 'a' is case-sensitive
        assert_eq!(keyword_from_str("notakeyword"), None);
    }

    #[test]
    fn test_token_display() {
        assert_eq!(
            format!("{}", TokenKind::Iri(Arc::from("http://example.org/"))),
            "<http://example.org/>"
        );
        assert_eq!(format!("{}", TokenKind::Var(Arc::from("name"))), "?name");
        assert_eq!(format!("{}", TokenKind::KwSelect), "SELECT");
    }

    #[test]
    fn test_token_is_keyword() {
        assert!(TokenKind::KwSelect.is_keyword());
        assert!(TokenKind::KwA.is_keyword());
        assert!(!TokenKind::Var(Arc::from("x")).is_keyword());
        assert!(!TokenKind::LBrace.is_keyword());
    }
}
