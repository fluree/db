//! Cypher token kinds.

use crate::span::SourceSpan;

#[derive(Clone, Debug, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: SourceSpan,
}

impl Token {
    pub fn new(kind: TokenKind, span: SourceSpan) -> Self {
        Self { kind, span }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TokenKind {
    // ===== Keywords (case-insensitive) =====
    Match,
    Optional,
    Where,
    Return,
    Distinct,
    As,
    And,
    Or,
    Not,
    Xor,
    In,
    Is,
    Null,
    True,
    False,
    Order,
    By,
    Asc,
    Desc,
    Skip,
    Limit,
    Union,
    All,
    With,
    Unwind,
    Create,
    Merge,
    On,
    Set,
    Remove,
    Delete,
    Detach,
    Case,
    When,
    Then,
    Else,
    End,
    Starts,
    Ends,
    Contains,
    Call,
    Yield,
    Exists,
    Count,

    // ===== Literals / identifiers =====
    /// `name`, `\`weird name\``.
    Ident(String),
    /// `42`, `0x1F`.
    Integer(i64),
    /// `3.14`, `1e5`.
    Float(f64),
    /// `"hello"`, `'world'`.
    String(String),
    /// `$name`, `$0`.
    Param(String),

    // ===== Punctuation =====
    LParen,           // (
    RParen,           // )
    LBracket,         // [
    RBracket,         // ]
    LBrace,           // {
    RBrace,           // }
    Comma,            // ,
    Semicolon,        // ;
    Dot,              // .
    DotDot,           // ..
    Colon,            // :
    Eq,               // =
    NotEq,            // <>
    Lt,               // <
    Le,               // <=
    Gt,               // >
    Ge,               // >=
    Plus,             // +
    Minus,            // -
    Star,             // *
    Slash,            // /
    Percent,          // %
    Caret,            // ^
    PlusEq,           // +=
    Pipe,             // |
    DashArrowRight,   // ->
    LArrowDash,       // <-
    Eof,
}

impl TokenKind {
    pub fn keyword_from_str(s: &str) -> Option<TokenKind> {
        // Case-insensitive keyword lookup. Cypher keywords are
        // conventionally uppercase but the spec is case-insensitive.
        let upper = s.to_ascii_uppercase();
        Some(match upper.as_str() {
            "MATCH" => TokenKind::Match,
            "OPTIONAL" => TokenKind::Optional,
            "WHERE" => TokenKind::Where,
            "RETURN" => TokenKind::Return,
            "DISTINCT" => TokenKind::Distinct,
            "AS" => TokenKind::As,
            "AND" => TokenKind::And,
            "OR" => TokenKind::Or,
            "NOT" => TokenKind::Not,
            "XOR" => TokenKind::Xor,
            "IN" => TokenKind::In,
            "IS" => TokenKind::Is,
            "NULL" => TokenKind::Null,
            "TRUE" => TokenKind::True,
            "FALSE" => TokenKind::False,
            "ORDER" => TokenKind::Order,
            "BY" => TokenKind::By,
            "ASC" | "ASCENDING" => TokenKind::Asc,
            "DESC" | "DESCENDING" => TokenKind::Desc,
            "SKIP" => TokenKind::Skip,
            "LIMIT" => TokenKind::Limit,
            "UNION" => TokenKind::Union,
            "ALL" => TokenKind::All,
            "WITH" => TokenKind::With,
            "UNWIND" => TokenKind::Unwind,
            "CREATE" => TokenKind::Create,
            "MERGE" => TokenKind::Merge,
            "ON" => TokenKind::On,
            "SET" => TokenKind::Set,
            "REMOVE" => TokenKind::Remove,
            "DELETE" => TokenKind::Delete,
            "DETACH" => TokenKind::Detach,
            "CASE" => TokenKind::Case,
            "WHEN" => TokenKind::When,
            "THEN" => TokenKind::Then,
            "ELSE" => TokenKind::Else,
            "END" => TokenKind::End,
            "STARTS" => TokenKind::Starts,
            "ENDS" => TokenKind::Ends,
            "CONTAINS" => TokenKind::Contains,
            "CALL" => TokenKind::Call,
            "YIELD" => TokenKind::Yield,
            "EXISTS" => TokenKind::Exists,
            "COUNT" => TokenKind::Count,
            _ => return None,
        })
    }
}

impl std::fmt::Display for TokenKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenKind::Match => write!(f, "MATCH"),
            TokenKind::Optional => write!(f, "OPTIONAL"),
            TokenKind::Where => write!(f, "WHERE"),
            TokenKind::Return => write!(f, "RETURN"),
            TokenKind::Distinct => write!(f, "DISTINCT"),
            TokenKind::As => write!(f, "AS"),
            TokenKind::And => write!(f, "AND"),
            TokenKind::Or => write!(f, "OR"),
            TokenKind::Not => write!(f, "NOT"),
            TokenKind::Xor => write!(f, "XOR"),
            TokenKind::In => write!(f, "IN"),
            TokenKind::Is => write!(f, "IS"),
            TokenKind::Null => write!(f, "NULL"),
            TokenKind::True => write!(f, "TRUE"),
            TokenKind::False => write!(f, "FALSE"),
            TokenKind::Order => write!(f, "ORDER"),
            TokenKind::By => write!(f, "BY"),
            TokenKind::Asc => write!(f, "ASC"),
            TokenKind::Desc => write!(f, "DESC"),
            TokenKind::Skip => write!(f, "SKIP"),
            TokenKind::Limit => write!(f, "LIMIT"),
            TokenKind::Union => write!(f, "UNION"),
            TokenKind::All => write!(f, "ALL"),
            TokenKind::With => write!(f, "WITH"),
            TokenKind::Unwind => write!(f, "UNWIND"),
            TokenKind::Create => write!(f, "CREATE"),
            TokenKind::Merge => write!(f, "MERGE"),
            TokenKind::On => write!(f, "ON"),
            TokenKind::Set => write!(f, "SET"),
            TokenKind::Remove => write!(f, "REMOVE"),
            TokenKind::Delete => write!(f, "DELETE"),
            TokenKind::Detach => write!(f, "DETACH"),
            TokenKind::Case => write!(f, "CASE"),
            TokenKind::When => write!(f, "WHEN"),
            TokenKind::Then => write!(f, "THEN"),
            TokenKind::Else => write!(f, "ELSE"),
            TokenKind::End => write!(f, "END"),
            TokenKind::Starts => write!(f, "STARTS"),
            TokenKind::Ends => write!(f, "ENDS"),
            TokenKind::Contains => write!(f, "CONTAINS"),
            TokenKind::Call => write!(f, "CALL"),
            TokenKind::Yield => write!(f, "YIELD"),
            TokenKind::Exists => write!(f, "EXISTS"),
            TokenKind::Count => write!(f, "COUNT"),
            TokenKind::Ident(s) => write!(f, "{s}"),
            TokenKind::Integer(n) => write!(f, "{n}"),
            TokenKind::Float(x) => write!(f, "{x}"),
            TokenKind::String(s) => write!(f, "{s:?}"),
            TokenKind::Param(name) => write!(f, "${name}"),
            TokenKind::LParen => write!(f, "("),
            TokenKind::RParen => write!(f, ")"),
            TokenKind::LBracket => write!(f, "["),
            TokenKind::RBracket => write!(f, "]"),
            TokenKind::LBrace => write!(f, "{{"),
            TokenKind::RBrace => write!(f, "}}"),
            TokenKind::Comma => write!(f, ","),
            TokenKind::Semicolon => write!(f, ";"),
            TokenKind::Dot => write!(f, "."),
            TokenKind::DotDot => write!(f, ".."),
            TokenKind::Colon => write!(f, ":"),
            TokenKind::Eq => write!(f, "="),
            TokenKind::NotEq => write!(f, "<>"),
            TokenKind::Lt => write!(f, "<"),
            TokenKind::Le => write!(f, "<="),
            TokenKind::Gt => write!(f, ">"),
            TokenKind::Ge => write!(f, ">="),
            TokenKind::Plus => write!(f, "+"),
            TokenKind::Minus => write!(f, "-"),
            TokenKind::Star => write!(f, "*"),
            TokenKind::Slash => write!(f, "/"),
            TokenKind::Percent => write!(f, "%"),
            TokenKind::Caret => write!(f, "^"),
            TokenKind::PlusEq => write!(f, "+="),
            TokenKind::Pipe => write!(f, "|"),
            TokenKind::DashArrowRight => write!(f, "->"),
            TokenKind::LArrowDash => write!(f, "<-"),
            TokenKind::Eof => write!(f, "<eof>"),
        }
    }
}
