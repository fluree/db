//! The restricted `rr:sqlQuery` an Iceberg-backed source accepts: one table,
//! optionally narrowed by a conjunction of column comparisons.
//!
//! ```text
//! SELECT * | <col> [, <col>]...
//! FROM <table>
//! [WHERE <condition> [AND <condition>]...]
//! [;]
//!
//! <condition> := <col> <op> <literal> | <col> IS [NOT] NULL
//! <op>        := = | <> | != | < | <= | > | >=
//! <literal>   := [-]<digits>[.<digits>] | '<text>' | TRUE | FALSE
//! ```
//!
//! A source that reads table files has no SQL engine behind it, so it cannot
//! run an arbitrary query. This subset is what it can evaluate itself, exactly:
//! the table is scanned and every row that does not satisfy the conjunction is
//! dropped before any term map sees it. Anything outside the subset (a join,
//! `OR`, a function, a subquery, an alias, ...) is refused with an error naming
//! the construct, never approximated.
//!
//! Identifiers, bare or double-quoted (`"a ""b"""`), are matched against the
//! table exactly as written, case included, the same way `rr:tableName` and
//! `rr:column` are. Keywords are case-insensitive. A namespaced table
//! (`ns.table`) is joined with `.` and resolves like `rr:tableName`.
//!
//! The select list does not shape the rows: the triples map's own term maps
//! choose the columns a scan reads. It is kept so the source can refuse a
//! column the table does not have. This parser checks only the grammar; the
//! columns, and whether each literal compares exactly with its column's type,
//! are checked by the source against the table's schema.

use std::fmt;

use super::LogicalTable;

/// A parsed restricted `rr:sqlQuery`: see the [module docs](self).
#[derive(Debug, Clone, PartialEq)]
pub struct RestrictedSelect {
    /// The table, normalized like an `rr:tableName` (`ns.table`).
    pub table: String,
    /// The select list, or `None` for `SELECT *`. Advisory: see the module docs.
    pub columns: Option<Vec<String>>,
    /// The `WHERE` conjunction; empty when there is no `WHERE`.
    pub conditions: Vec<RowCondition>,
}

/// One conjunct of the `WHERE` clause.
#[derive(Debug, Clone, PartialEq)]
pub enum RowCondition {
    /// `<col> <op> <literal>`.
    Compare {
        column: String,
        op: RowCmpOp,
        value: SqlLiteral,
    },
    /// `<col> IS NULL`.
    IsNull { column: String },
    /// `<col> IS NOT NULL`.
    IsNotNull { column: String },
}

impl RowCondition {
    /// The column the condition tests.
    pub fn column(&self) -> &str {
        match self {
            RowCondition::Compare { column, .. }
            | RowCondition::IsNull { column }
            | RowCondition::IsNotNull { column } => column,
        }
    }
}

/// A comparison operator of the subset. `!=` parses to [`RowCmpOp::NotEq`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowCmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// A literal of the subset, as written. Typing it against its column is the
/// source's job: `1` may compare with an integer or a decimal column, never a
/// boolean one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlLiteral {
    /// An integer: digits with an optional leading `-`.
    Integer(i128),
    /// A number with a fractional part, as `unscaled × 10^-scale`: `-1.50` is
    /// unscaled `-150`, scale 2. The written scale is kept.
    Decimal { unscaled: i128, scale: u8 },
    /// A single-quoted string, with `''` unescaped to `'`.
    String(String),
    /// `TRUE` or `FALSE`.
    Boolean(bool),
}

/// Why a query is outside the subset. The message names the construct.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct UnsupportedSql(String);

impl UnsupportedSql {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl RestrictedSelect {
    /// Parse `sql`, refusing anything outside the subset.
    pub fn parse(sql: &str) -> Result<Self, UnsupportedSql> {
        let tokens = tokenize(sql)?;
        Parser { tokens, pos: 0 }.select()
    }

    /// Every column the query names: the select list, then the conditions'.
    pub fn named_columns(&self) -> impl Iterator<Item = &str> {
        self.columns
            .iter()
            .flatten()
            .map(String::as_str)
            .chain(self.conditions.iter().map(RowCondition::column))
    }
}

impl fmt::Display for RowCmpOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RowCmpOp::Eq => "=",
            RowCmpOp::NotEq => "<>",
            RowCmpOp::Lt => "<",
            RowCmpOp::LtEq => "<=",
            RowCmpOp::Gt => ">",
            RowCmpOp::GtEq => ">=",
        })
    }
}

impl fmt::Display for SqlLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlLiteral::Integer(n) => write!(f, "{n}"),
            SqlLiteral::Decimal { unscaled, scale } => {
                let digits = unscaled.unsigned_abs().to_string();
                let scale = usize::from(*scale);
                let padded = format!("{digits:0>width$}", width = scale + 1);
                let (int, frac) = padded.split_at(padded.len() - scale);
                let sign = if *unscaled < 0 { "-" } else { "" };
                write!(f, "{sign}{int}.{frac}")
            }
            SqlLiteral::String(s) => write!(f, "'{}'", s.replace('\'', "''")),
            SqlLiteral::Boolean(b) => f.write_str(if *b { "TRUE" } else { "FALSE" }),
        }
    }
}

impl fmt::Display for RowCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowCondition::Compare { column, op, value } => write!(f, "{column} {op} {value}"),
            RowCondition::IsNull { column } => write!(f, "{column} IS NULL"),
            RowCondition::IsNotNull { column } => write!(f, "{column} IS NOT NULL"),
        }
    }
}

// --- tokenizer --------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    /// An identifier; `quoted` when written in double quotes (never a keyword).
    Ident {
        text: String,
        quoted: bool,
    },
    /// A single-quoted string literal, unescaped.
    Str(String),
    /// An unsigned number as written: digits, optionally `.` and digits.
    Number(String),
    /// A comparison operator, normalized (`!=` becomes `<>`).
    Op(RowCmpOp),
    Comma,
    Star,
    Dot,
    Minus,
    Semicolon,
    /// Any other character, kept for the error message.
    Other(char),
}

impl Token {
    /// The keyword this token spells, upper-cased; `None` for a quoted
    /// identifier or any non-identifier.
    fn keyword(&self) -> Option<String> {
        match self {
            Token::Ident {
                text,
                quoted: false,
            } => Some(text.to_ascii_uppercase()),
            _ => None,
        }
    }

    fn describe(&self) -> String {
        match self {
            Token::Ident { text, quoted: true } => format!("\"{text}\""),
            Token::Ident { text, .. } => text.clone(),
            Token::Str(s) => format!("'{s}'"),
            Token::Number(n) => n.clone(),
            Token::Op(op) => op.to_string(),
            Token::Comma => ",".to_string(),
            Token::Star => "*".to_string(),
            Token::Dot => ".".to_string(),
            Token::Minus => "-".to_string(),
            Token::Semicolon => ";".to_string(),
            Token::Other(c) => c.to_string(),
        }
    }
}

fn tokenize(sql: &str) -> Result<Vec<Token>, UnsupportedSql> {
    let mut tokens = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let mut text = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_ascii_alphanumeric() || c == '_' || c == '$' {
                    text.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            tokens.push(Token::Ident {
                text,
                quoted: false,
            });
            continue;
        }
        if c.is_ascii_digit() {
            let mut text = String::new();
            let mut seen_dot = false;
            while let Some(&c) = chars.peek() {
                if c.is_ascii_digit() {
                    text.push(c);
                } else if c == '.' && !seen_dot {
                    seen_dot = true;
                    text.push(c);
                } else if c.is_ascii_alphabetic() || c == '_' {
                    return Err(UnsupportedSql::new(format!(
                        "unsupported numeric literal '{text}{c}…': write integers and decimals \
                         as plain digits (no exponent, no suffix)"
                    )));
                } else {
                    break;
                }
                chars.next();
            }
            if text.ends_with('.') {
                return Err(UnsupportedSql::new(format!(
                    "numeric literal '{text}' needs digits after the decimal point"
                )));
            }
            tokens.push(Token::Number(text));
            continue;
        }
        chars.next();
        let token = match c {
            '\'' => Token::Str(quoted(&mut chars, '\'', "string literal")?),
            '"' => Token::Ident {
                text: quoted(&mut chars, '"', "quoted identifier")?,
                quoted: true,
            },
            ',' => Token::Comma,
            '*' => Token::Star,
            '.' => Token::Dot,
            ';' => Token::Semicolon,
            '=' => Token::Op(RowCmpOp::Eq),
            '<' => match chars.peek() {
                Some('=') => {
                    chars.next();
                    Token::Op(RowCmpOp::LtEq)
                }
                Some('>') => {
                    chars.next();
                    Token::Op(RowCmpOp::NotEq)
                }
                _ => Token::Op(RowCmpOp::Lt),
            },
            '>' => match chars.peek() {
                Some('=') => {
                    chars.next();
                    Token::Op(RowCmpOp::GtEq)
                }
                _ => Token::Op(RowCmpOp::Gt),
            },
            '!' if chars.peek() == Some(&'=') => {
                chars.next();
                Token::Op(RowCmpOp::NotEq)
            }
            '-' if chars.peek() == Some(&'-') => {
                return Err(UnsupportedSql::new("SQL comments are not supported"));
            }
            '/' if chars.peek() == Some(&'*') => {
                return Err(UnsupportedSql::new("SQL comments are not supported"));
            }
            '-' => Token::Minus,
            other => Token::Other(other),
        };
        tokens.push(token);
    }
    Ok(tokens)
}

/// The body of a `quote`-delimited token whose opening quote was consumed, with
/// a doubled quote unescaped to one.
fn quoted(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    quote: char,
    what: &str,
) -> Result<String, UnsupportedSql> {
    let mut text = String::new();
    loop {
        match chars.next() {
            Some(c) if c == quote => {
                if chars.peek() == Some(&quote) {
                    chars.next();
                    text.push(quote);
                } else {
                    return Ok(text);
                }
            }
            Some(c) => text.push(c),
            None => return Err(UnsupportedSql::new(format!("unterminated {what}"))),
        }
    }
}

// --- parser -----------------------------------------------------------------

/// Words that end a clause of the subset. A bare identifier spelling one is a
/// keyword, never a column; quote the column to use such a name.
const KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE",
];

/// Constructs the subset refuses, with how to say what is meant instead. Matched
/// by the upper-cased bare word where the parser expected something else.
fn refusal_for(word: &str) -> Option<&'static str> {
    Some(match word {
        "OR" => "OR is not supported: the WHERE clause must be a conjunction (AND) of conditions",
        "NOT" => "NOT is not supported except in IS NOT NULL; use the inverse comparison",
        "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "NATURAL" | "ON" | "USING" => {
            "joins are not supported: map each table with its own triples map and relate them \
             with rr:parentTriplesMap"
        }
        "GROUP" | "HAVING" => "GROUP BY / HAVING are not supported",
        "ORDER" => "ORDER BY is not supported",
        "LIMIT" | "OFFSET" | "FETCH" | "TOP" => "LIMIT / OFFSET are not supported",
        "UNION" | "INTERSECT" | "EXCEPT" | "MINUS" => "set operations are not supported",
        "DISTINCT" | "ALL" => "SELECT DISTINCT / ALL is not supported",
        "AS" => "aliases (AS) are not supported",
        "IN" => "IN lists are not supported",
        "BETWEEN" => "BETWEEN is not supported; write two comparisons joined by AND",
        "LIKE" | "ILIKE" | "SIMILAR" | "REGEXP" | "RLIKE" => "pattern matching is not supported",
        "WITH" => "WITH (common table expressions) is not supported",
        "CASE" | "CAST" | "EXISTS" => "expressions are not supported, only columns and literals",
        _ => return None,
    })
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn at_keyword(&self, keyword: &str) -> bool {
        self.peek().and_then(Token::keyword).as_deref() == Some(keyword)
    }

    /// An error for the token at the cursor, where `expected` was wanted.
    fn unexpected(&self, expected: &str) -> UnsupportedSql {
        match self.peek() {
            None => UnsupportedSql::new(format!("expected {expected}, found the end of the query")),
            Some(token) => {
                if let Some(refusal) = token.keyword().as_deref().and_then(refusal_for) {
                    return UnsupportedSql::new(refusal);
                }
                match token {
                    Token::Other('(' | ')') => UnsupportedSql::new(
                        "parentheses are not supported: no functions, subqueries or grouped \
                         conditions",
                    ),
                    _ => UnsupportedSql::new(format!(
                        "expected {expected}, found '{}'",
                        token.describe()
                    )),
                }
            }
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), UnsupportedSql> {
        if self.at_keyword(keyword) {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.unexpected(keyword))
        }
    }

    /// An identifier that is not a keyword (or any quoted identifier).
    fn identifier(&mut self, what: &str) -> Result<String, UnsupportedSql> {
        match self.peek() {
            Some(Token::Ident { text, quoted }) => {
                let is_keyword = !quoted && KEYWORDS.contains(&text.to_ascii_uppercase().as_str());
                let is_refused = !quoted && refusal_for(&text.to_ascii_uppercase()).is_some();
                if is_keyword || is_refused {
                    return Err(self.unexpected(what));
                }
                if text.is_empty() {
                    return Err(UnsupportedSql::new(format!(
                        "empty quoted identifier as {what}"
                    )));
                }
                let text = text.clone();
                self.pos += 1;
                Ok(text)
            }
            _ => Err(self.unexpected(what)),
        }
    }

    /// A column reference: one identifier, not qualified by a table.
    fn column(&mut self) -> Result<String, UnsupportedSql> {
        let column = self.identifier("a column name")?;
        if self.peek() == Some(&Token::Dot) {
            return Err(UnsupportedSql::new(format!(
                "qualified column references ('{column}.…') are not supported; name the column \
                 alone"
            )));
        }
        if self.peek() == Some(&Token::Other('(')) {
            return Err(UnsupportedSql::new(format!(
                "function calls ('{column}(…)') are not supported, only columns and literals"
            )));
        }
        Ok(column)
    }

    fn select(mut self) -> Result<RestrictedSelect, UnsupportedSql> {
        self.expect_keyword("SELECT")?;
        let columns = if self.peek() == Some(&Token::Star) {
            self.pos += 1;
            None
        } else {
            let mut columns = vec![self.column()?];
            while self.peek() == Some(&Token::Comma) {
                self.pos += 1;
                columns.push(self.column()?);
            }
            Some(columns)
        };
        self.expect_keyword("FROM")?;
        let table = self.table()?;
        let mut conditions = Vec::new();
        if self.at_keyword("WHERE") {
            self.pos += 1;
            conditions.push(self.condition()?);
            while self.at_keyword("AND") {
                self.pos += 1;
                conditions.push(self.condition()?);
            }
        }
        if self.peek() == Some(&Token::Semicolon) {
            self.pos += 1;
        }
        if self.peek().is_some() {
            let expected = if conditions.is_empty() {
                "WHERE or the end of the query"
            } else {
                "AND or the end of the query"
            };
            return Err(self.unexpected(expected));
        }
        Ok(RestrictedSelect {
            table,
            columns,
            conditions,
        })
    }

    /// `ident (. ident)*`, joined with `.` and normalized like `rr:tableName`.
    fn table(&mut self) -> Result<String, UnsupportedSql> {
        let mut parts = vec![self.table_part()?];
        while self.peek() == Some(&Token::Dot) {
            self.pos += 1;
            parts.push(self.table_part()?);
        }
        if self.peek() == Some(&Token::Comma) {
            return Err(UnsupportedSql::new(
                "more than one table in FROM is not supported: map each table with its own \
                 triples map",
            ));
        }
        if matches!(self.peek(), Some(Token::Ident { quoted, text }) if *quoted || !is_clause_word(text))
        {
            return Err(UnsupportedSql::new(
                "table aliases are not supported; name the table alone",
            ));
        }
        Ok(LogicalTable::normalize_table_name(&parts.join(".")))
    }

    fn table_part(&mut self) -> Result<String, UnsupportedSql> {
        if self.peek() == Some(&Token::Other('(')) {
            return Err(UnsupportedSql::new("subqueries are not supported"));
        }
        let part = self.identifier("a table name")?;
        if part.contains('.') {
            return Err(UnsupportedSql::new(format!(
                "table name part \"{part}\" contains '.', which cannot be told apart from a \
                 namespace separator"
            )));
        }
        Ok(part)
    }

    fn condition(&mut self) -> Result<RowCondition, UnsupportedSql> {
        if matches!(
            self.peek(),
            Some(Token::Str(_) | Token::Number(_) | Token::Minus)
        ) {
            return Err(UnsupportedSql::new(
                "a condition must name the column first: <column> <op> <literal>",
            ));
        }
        let column = self.column()?;
        if self.at_keyword("IS") {
            self.pos += 1;
            let negated = self.at_keyword("NOT");
            if negated {
                self.pos += 1;
            }
            self.expect_keyword("NULL")?;
            return Ok(if negated {
                RowCondition::IsNotNull { column }
            } else {
                RowCondition::IsNull { column }
            });
        }
        let op = match self.peek() {
            Some(Token::Op(op)) => *op,
            _ => return Err(self.unexpected("a comparison operator or IS [NOT] NULL")),
        };
        self.pos += 1;
        let value = self.literal(&column)?;
        Ok(RowCondition::Compare { column, op, value })
    }

    fn literal(&mut self, column: &str) -> Result<SqlLiteral, UnsupportedSql> {
        let negative = self.peek() == Some(&Token::Minus);
        if negative {
            self.pos += 1;
        }
        let token = self.peek().cloned();
        let literal = match token {
            Some(Token::Number(text)) => number(&text, negative)?,
            _ if negative => return Err(self.unexpected("a number after '-'")),
            Some(Token::Str(s)) => SqlLiteral::String(s),
            Some(token) => match token.keyword().as_deref() {
                Some("TRUE") => SqlLiteral::Boolean(true),
                Some("FALSE") => SqlLiteral::Boolean(false),
                Some("NULL") => {
                    return Err(UnsupportedSql::new(format!(
                        "comparing with NULL matches no row; write '{column} IS NULL' or \
                         '{column} IS NOT NULL'"
                    )))
                }
                _ if matches!(token, Token::Ident { .. }) => {
                    return Err(UnsupportedSql::new(format!(
                        "the right side of a comparison on '{column}' must be a literal; \
                         comparing two columns is not supported"
                    )))
                }
                _ => return Err(self.unexpected("a literal")),
            },
            None => return Err(self.unexpected("a literal")),
        };
        self.pos += 1;
        Ok(literal)
    }
}

/// Whether a bare word may follow a table name without being read as an alias.
fn is_clause_word(word: &str) -> bool {
    let upper = word.to_ascii_uppercase();
    KEYWORDS.contains(&upper.as_str()) || refusal_for(&upper).is_some()
}

/// A numeric token as an integer or decimal literal.
fn number(text: &str, negative: bool) -> Result<SqlLiteral, UnsupportedSql> {
    let too_large = || UnsupportedSql::new(format!("numeric literal '{text}' has too many digits"));
    let (int, frac) = text.split_once('.').unwrap_or((text, ""));
    let digits = format!("{int}{frac}");
    let magnitude: i128 = digits.parse().map_err(|_| too_large())?;
    let value = if negative { -magnitude } else { magnitude };
    if frac.is_empty() {
        return Ok(SqlLiteral::Integer(value));
    }
    let scale = u8::try_from(frac.len())
        .ok()
        .filter(|s| *s <= 38)
        .ok_or_else(too_large)?;
    Ok(SqlLiteral::Decimal {
        unscaled: value,
        scale,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> RestrictedSelect {
        RestrictedSelect::parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    fn refusal(sql: &str) -> String {
        match RestrictedSelect::parse(sql) {
            Ok(parsed) => panic!("{sql} parsed as {parsed:?}"),
            Err(e) => e.to_string(),
        }
    }

    fn cmp(column: &str, op: RowCmpOp, value: SqlLiteral) -> RowCondition {
        RowCondition::Compare {
            column: column.to_string(),
            op,
            value,
        }
    }

    #[test]
    fn star_without_where() {
        let parsed = parse("SELECT * FROM customers_history");
        assert_eq!(parsed.table, "customers_history");
        assert_eq!(parsed.columns, None);
        assert!(parsed.conditions.is_empty());
    }

    #[test]
    fn column_list_namespaced_table_and_conjunction() {
        let parsed = parse(
            "select customer_id, \"Name\" from crm.customers_history \
             where is_current = 1 and is_deleted <> 0 AND name != 'x' and region IS NOT NULL \
             AND valid_to is null;",
        );
        assert_eq!(parsed.table, "crm.customers_history");
        assert_eq!(
            parsed.columns,
            Some(vec!["customer_id".to_string(), "Name".to_string()])
        );
        assert_eq!(
            parsed.conditions,
            vec![
                cmp("is_current", RowCmpOp::Eq, SqlLiteral::Integer(1)),
                cmp("is_deleted", RowCmpOp::NotEq, SqlLiteral::Integer(0)),
                cmp("name", RowCmpOp::NotEq, SqlLiteral::String("x".into())),
                RowCondition::IsNotNull {
                    column: "region".into()
                },
                RowCondition::IsNull {
                    column: "valid_to".into()
                },
            ]
        );
    }

    #[test]
    fn every_operator_and_literal_form() {
        let parsed = parse(
            "SELECT * FROM t WHERE a < -5 AND b <= 1.50 AND c > -0.25 AND d >= 0 \
             AND e = 'it''s' AND f = TRUE AND g = false",
        );
        assert_eq!(
            parsed.conditions,
            vec![
                cmp("a", RowCmpOp::Lt, SqlLiteral::Integer(-5)),
                cmp(
                    "b",
                    RowCmpOp::LtEq,
                    SqlLiteral::Decimal {
                        unscaled: 150,
                        scale: 2
                    }
                ),
                cmp(
                    "c",
                    RowCmpOp::Gt,
                    SqlLiteral::Decimal {
                        unscaled: -25,
                        scale: 2
                    }
                ),
                cmp("d", RowCmpOp::GtEq, SqlLiteral::Integer(0)),
                cmp("e", RowCmpOp::Eq, SqlLiteral::String("it's".into())),
                cmp("f", RowCmpOp::Eq, SqlLiteral::Boolean(true)),
                cmp("g", RowCmpOp::Eq, SqlLiteral::Boolean(false)),
            ]
        );
    }

    #[test]
    fn identifiers_keep_their_case_and_quoting() {
        let parsed = parse("SELECT * FROM \"Sales\".\"Order Lines\" WHERE \"Is Current\" = 1");
        assert_eq!(parsed.table, "Sales.Order Lines");
        assert_eq!(parsed.conditions[0].column(), "Is Current");
        // A quoted keyword is an identifier.
        let parsed = parse("SELECT \"from\" FROM t WHERE \"and\" = 'x'");
        assert_eq!(parsed.columns, Some(vec!["from".to_string()]));
        assert_eq!(parsed.conditions[0].column(), "and");
        // A doubled quote inside a quoted identifier is one quote.
        let parsed = parse("SELECT * FROM t WHERE \"a\"\"b\" = 1");
        assert_eq!(parsed.conditions[0].column(), "a\"b");
    }

    #[test]
    fn slash_namespace_normalizes_like_table_name() {
        assert_eq!(parse("SELECT * FROM \"ns/t\"").table, "ns.t");
    }

    #[test]
    fn display_round_trips_the_condition() {
        let parsed = parse("SELECT * FROM t WHERE a <= -0.050 AND b = 'o''k' AND c IS NOT NULL");
        let shown: Vec<String> = parsed.conditions.iter().map(ToString::to_string).collect();
        assert_eq!(shown, vec!["a <= -0.050", "b = 'o''k'", "c IS NOT NULL"]);
    }

    #[test]
    fn refusals_name_the_construct() {
        let cases: &[(&str, &str)] = &[
            (
                "SELECT * FROM t WHERE a = 1 OR b = 2",
                "OR is not supported",
            ),
            ("SELECT * FROM t WHERE NOT a = 1", "NOT is not supported"),
            (
                "SELECT * FROM t JOIN u ON t.id = u.id",
                "joins are not supported",
            ),
            ("SELECT * FROM t, u", "more than one table"),
            ("SELECT * FROM t x", "table aliases are not supported"),
            ("SELECT * FROM t AS x", "aliases (AS) are not supported"),
            ("SELECT a AS b FROM t", "aliases (AS) are not supported"),
            ("SELECT DISTINCT a FROM t", "DISTINCT"),
            ("SELECT * FROM t ORDER BY a", "ORDER BY"),
            ("SELECT * FROM t WHERE a = 1 LIMIT 5", "LIMIT"),
            ("SELECT * FROM t GROUP BY a", "GROUP BY"),
            ("SELECT * FROM t UNION SELECT * FROM u", "set operations"),
            ("SELECT * FROM t WHERE a IN (1, 2)", "IN lists"),
            ("SELECT * FROM t WHERE a BETWEEN 1 AND 2", "BETWEEN"),
            ("SELECT * FROM t WHERE a LIKE 'x%'", "pattern matching"),
            ("SELECT * FROM (SELECT * FROM t)", "subqueries"),
            ("SELECT upper(a) FROM t", "function calls"),
            ("SELECT * FROM t WHERE (a = 1)", "parentheses"),
            ("SELECT * FROM t WHERE a = b", "comparing two columns"),
            ("SELECT * FROM t WHERE 1 = a", "name the column first"),
            ("SELECT * FROM t WHERE a = NULL", "IS NULL"),
            ("SELECT * FROM t WHERE t.a = 1", "qualified column"),
            ("SELECT * FROM t WHERE a = 1e5", "no exponent"),
            (
                "SELECT * FROM t WHERE a = 1.",
                "digits after the decimal point",
            ),
            ("SELECT * FROM t WHERE a = 'open", "unterminated string"),
            ("SELECT * FROM t -- note", "comments"),
            ("SELECT * FROM t WHERE", "end of the query"),
            ("SELECT * FROM t WHERE a = 1 AND", "end of the query"),
            ("SELECT FROM t", "a column name"),
            ("SELECT *, a FROM t", "expected FROM"),
            (
                "WITH x AS (SELECT 1) SELECT * FROM x",
                "WITH (common table expressions)",
            ),
            ("UPDATE t SET a = 1", "expected SELECT"),
            ("SELECT * FROM t WHERE a = 1 a", "expected AND"),
            ("SELECT * FROM t WHERE a = 1; SELECT 1", "expected AND"),
            ("SELECT * FROM t WHERE a = 1 + 1", "expected AND"),
            ("SELECT * FROM t WHERE a = -'x'", "a number after '-'"),
            ("SELECT * FROM \"a.b\"", "contains '.'"),
            ("SELECT * FROM t WHERE \"\" = 1", "empty quoted identifier"),
            (
                "SELECT * FROM t WHERE a = 1234567890123456789012345678901234567890",
                "too many digits",
            ),
        ];
        for (sql, needle) in cases {
            let message = refusal(sql);
            assert!(
                message.contains(needle),
                "{sql}: expected a refusal containing {needle:?}, got {message:?}"
            );
        }
    }
}
