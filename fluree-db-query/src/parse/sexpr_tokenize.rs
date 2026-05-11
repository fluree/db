//! S-expression tokenization and string parsing utilities
//!
//! Provides tokenization for S-expressions used in:
//! - Aggregate function syntax: `(as (count ?x) ?cnt)`
//! - Filter expressions: `(and (> ?age 18) (< ?age 65))`
//!
//! # Example
//!
//! ```
//! use fluree_db_query::parse::sexpr_tokenize::{tokenize_sexpr, SexprToken};
//!
//! let tokens = tokenize_sexpr("(as (count ?x) ?cnt)").unwrap();
//! assert_eq!(tokens.len(), 1);
//! assert!(matches!(&tokens[0], SexprToken::List(_)));
//! ```

use super::error::{ParseError, Result};

/// Token in an S-expression
///
/// S-expressions are tokenized into nested atoms, quoted strings, and lists:
/// - `?x` → `Atom("?x")`
/// - `42` → `Atom("42")`
/// - `"hello world"` → `String("hello world")`
/// - `(count ?x)` → `List([Atom("count"), Atom("?x")])`
///
/// `Atom` and `String` are kept distinct so callers parsing scalar
/// expressions can preserve literal type — e.g. `"false"` stays a string,
/// while bare `false` is a boolean. `as_str` collapses both back to the
/// underlying text for callers that don't care (e.g. the groupconcat
/// separator).
#[derive(Debug, Clone, PartialEq)]
pub enum SexprToken {
    /// Atom: variable, symbol, number, or boolean (unquoted in source).
    Atom(String),
    /// Quoted string literal. Surrounding `"..."` is stripped; the contents
    /// are preserved verbatim.
    String(String),
    /// Nested list (sub-expression)
    List(Vec<SexprToken>),
}

impl SexprToken {
    /// Extract atom value (unquoted), returning error if this is a list or
    /// quoted string. Use this when the value must syntactically be an
    /// identifier — variable, function name, alias, `*`, `as`.
    pub fn as_atom(&self) -> Result<&str> {
        match self {
            SexprToken::Atom(s) => Ok(s),
            SexprToken::String(s) => Err(ParseError::InvalidSelect(format!(
                "expected unquoted atom, got string literal: \"{s}\""
            ))),
            SexprToken::List(_) => Err(ParseError::InvalidSelect(
                "expected atom, got list".to_string(),
            )),
        }
    }

    /// Extract list contents, returning error if this is an atom or string
    pub fn as_list(&self) -> Result<&[SexprToken]> {
        match self {
            SexprToken::List(tokens) => Ok(tokens),
            SexprToken::Atom(_) | SexprToken::String(_) => Err(ParseError::InvalidSelect(
                "expected list, got atom".to_string(),
            )),
        }
    }

    /// Extract atom value with custom error context (rejects strings and lists).
    pub fn expect_atom(&self, context: &str) -> Result<&str> {
        match self {
            SexprToken::Atom(s) => Ok(s),
            SexprToken::String(s) => Err(ParseError::InvalidSelect(format!(
                "{context} must be an unquoted atom, got string literal: \"{s}\""
            ))),
            SexprToken::List(_) => Err(ParseError::InvalidSelect(format!(
                "{context} must be an atom, not a list"
            ))),
        }
    }

    /// Extract list contents with custom error context
    pub fn expect_list(&self, context: &str) -> Result<&[SexprToken]> {
        match self {
            SexprToken::List(tokens) => Ok(tokens),
            SexprToken::Atom(a) => Err(ParseError::InvalidSelect(format!(
                "{context} must be a list, got atom: {a}"
            ))),
            SexprToken::String(s) => Err(ParseError::InvalidSelect(format!(
                "{context} must be a list, got string literal: \"{s}\""
            ))),
        }
    }

    /// Inner text regardless of quoting (atom or string literal). Use for
    /// values where the syntactic form doesn't change semantics — e.g. the
    /// groupconcat separator: `(groupconcat ?x ", ")` and
    /// `(groupconcat ?x ,)` both extract `,`.
    pub fn as_str(&self) -> Result<&str> {
        match self {
            SexprToken::Atom(s) | SexprToken::String(s) => Ok(s),
            SexprToken::List(_) => Err(ParseError::InvalidSelect(
                "expected atom or string, got list".to_string(),
            )),
        }
    }
}

/// Tokenize an S-expression string into nested tokens
///
/// Handles:
/// - Nested parentheses: `(as (count ?x) ?y)` → `List[Atom("as"), List[Atom("count"), Atom("?x")], Atom("?y")]`
/// - Quoted strings: `"hello world"` → `String("hello world")` (distinct from `Atom`)
/// - Symbols and variables: `count`, `?x`, `*`
///
/// # Example
///
/// ```
/// use fluree_db_query::parse::sexpr_tokenize::{tokenize_sexpr, SexprToken};
///
/// let tokens = tokenize_sexpr("(count ?x)").unwrap();
/// assert_eq!(tokens.len(), 1);
/// assert!(matches!(&tokens[0], SexprToken::List(_)));
/// ```
pub fn tokenize_sexpr(s: &str) -> Result<Vec<SexprToken>> {
    let mut chars = s.chars().peekable();
    tokenize_sexpr_inner(&mut chars, false)
}

fn tokenize_sexpr_inner(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    in_list: bool,
) -> Result<Vec<SexprToken>> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    while let Some(&c) = chars.peek() {
        match c {
            '(' => {
                // Push any pending atom
                if !current.is_empty() {
                    tokens.push(SexprToken::Atom(current.clone()));
                    current.clear();
                }
                chars.next(); // consume '('
                              // Recursively parse the nested list
                let nested = tokenize_sexpr_inner(chars, true)?;
                tokens.push(SexprToken::List(nested));
            }
            ')' => {
                // Push any pending atom
                if !current.is_empty() {
                    tokens.push(SexprToken::Atom(current.clone()));
                    current.clear();
                }
                if in_list {
                    chars.next(); // consume ')'
                    return Ok(tokens);
                }
                return Err(ParseError::InvalidSelect(
                    "unexpected ')' in S-expression".to_string(),
                ));
            }
            '"' => {
                // Push any pending atom
                if !current.is_empty() {
                    tokens.push(SexprToken::Atom(current.clone()));
                    current.clear();
                }
                chars.next(); // consume opening quote
                              // Read until closing quote
                let mut string_val = String::new();
                let mut closed = false;
                while let Some(&sc) = chars.peek() {
                    if sc == '"' {
                        chars.next(); // consume closing quote
                        closed = true;
                        break;
                    }
                    string_val.push(sc);
                    chars.next();
                }
                if !closed {
                    return Err(ParseError::InvalidSelect(
                        "unclosed string literal in S-expression (missing closing '\")".to_string(),
                    ));
                }
                tokens.push(SexprToken::String(string_val));
            }
            c if c.is_whitespace() => {
                // Push any pending atom
                if !current.is_empty() {
                    tokens.push(SexprToken::Atom(current.clone()));
                    current.clear();
                }
                chars.next(); // consume whitespace
            }
            _ => {
                current.push(c);
                chars.next();
            }
        }
    }

    // Push any remaining atom
    if !current.is_empty() {
        tokens.push(SexprToken::Atom(current));
    }

    if in_list {
        return Err(ParseError::InvalidSelect(
            "unclosed '(' in S-expression".to_string(),
        ));
    }

    Ok(tokens)
}

/// Split the first token from an S-expression string
///
/// Returns `(first_token, remaining_string)`.
///
/// # Example
///
/// ```
/// use fluree_db_query::parse::sexpr_tokenize::split_first_token;
///
/// let (op, rest) = split_first_token("count ?x").unwrap();
/// assert_eq!(op, "count");
/// assert_eq!(rest, "?x");
/// ```
pub fn split_first_token(s: &str) -> Result<(&str, &str)> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ParseError::InvalidFilter("empty expression".to_string()));
    }

    // Find the end of the first token
    let end = s
        .find(|c: char| c.is_whitespace() || c == '(' || c == ')')
        .unwrap_or(s.len());

    Ok((&s[..end], s[end..].trim()))
}

/// Find the index of the matching close paren for an open paren at position 0
///
/// # Example
///
/// ```
/// use fluree_db_query::parse::sexpr_tokenize::find_matching_paren;
///
/// let s = "(foo (bar baz))";
/// let end = find_matching_paren(s).unwrap();
/// assert_eq!(end, 14); // Position of final ')'
/// ```
pub fn find_matching_paren(s: &str) -> Result<usize> {
    let mut depth = 0;
    let mut in_quote = false;
    for (i, c) in s.chars().enumerate() {
        if in_quote {
            if c == '"' {
                in_quote = false;
            }
            continue;
        }
        match c {
            '"' => in_quote = true,
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
    }
    Err(ParseError::InvalidFilter(
        "unmatched parenthesis".to_string(),
    ))
}

/// Find the index of the matching close bracket for an open bracket at position 0
///
/// # Example
///
/// ```
/// use fluree_db_query::parse::sexpr_tokenize::find_matching_bracket;
///
/// let s = "[1 [2 3]]";
/// let end = find_matching_bracket(s).unwrap();
/// assert_eq!(end, 8); // Position of final ']'
/// ```
pub fn find_matching_bracket(s: &str) -> Result<usize> {
    let mut depth = 0;
    for (i, c) in s.chars().enumerate() {
        match c {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
    }
    Err(ParseError::InvalidFilter("unmatched bracket".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_simple() {
        let tokens = tokenize_sexpr("(count ?x)").unwrap();
        assert_eq!(tokens.len(), 1);
        let list = tokens[0].as_list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].as_atom().unwrap(), "count");
        assert_eq!(list[1].as_atom().unwrap(), "?x");
    }

    #[test]
    fn test_tokenize_nested() {
        let tokens = tokenize_sexpr("(as (count ?x) ?y)").unwrap();
        assert_eq!(tokens.len(), 1);
        let list = tokens[0].as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_atom().unwrap(), "as");

        let inner = list[1].as_list().unwrap();
        assert_eq!(inner.len(), 2);
        assert_eq!(inner[0].as_atom().unwrap(), "count");
        assert_eq!(inner[1].as_atom().unwrap(), "?x");

        assert_eq!(list[2].as_atom().unwrap(), "?y");
    }

    #[test]
    fn test_tokenize_quoted_string_distinct_from_atom() {
        let tokens = tokenize_sexpr(r#"(groupconcat ?x ", ")"#).unwrap();
        assert_eq!(tokens.len(), 1);
        let list = tokens[0].as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_atom().unwrap(), "groupconcat");
        assert_eq!(list[1].as_atom().unwrap(), "?x");
        // Quoted string is a `String` variant, not an `Atom`. Strict
        // accessors reject it; `as_str` normalizes both back to text.
        assert!(matches!(&list[2], SexprToken::String(s) if s == ", "));
        assert!(list[2].as_atom().is_err());
        assert_eq!(list[2].as_str().unwrap(), ", ");
    }

    #[test]
    fn test_tokenize_quoted_keyword_not_a_keyword() {
        // A quoted "false" must remain a string token — it should NOT collapse
        // into the same shape as the bare boolean atom `false`.
        let quoted = tokenize_sexpr(r#"(coalesce ?v "false")"#).unwrap();
        let q_list = quoted[0].as_list().unwrap();
        assert!(matches!(&q_list[2], SexprToken::String(s) if s == "false"));

        let bare = tokenize_sexpr("(coalesce ?v false)").unwrap();
        let b_list = bare[0].as_list().unwrap();
        assert!(matches!(&b_list[2], SexprToken::Atom(s) if s == "false"));
    }

    #[test]
    fn test_tokenize_unclosed_string() {
        let result = tokenize_sexpr(r#"(foo "unclosed)"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_tokenize_unclosed_paren() {
        let result = tokenize_sexpr("(foo (bar)");
        assert!(result.is_err());
    }

    #[test]
    fn test_split_first_token() {
        let (first, rest) = split_first_token("count ?x").unwrap();
        assert_eq!(first, "count");
        assert_eq!(rest, "?x");
    }

    #[test]
    fn test_split_first_token_with_paren() {
        let (first, rest) = split_first_token("as (count ?x) ?y").unwrap();
        assert_eq!(first, "as");
        assert_eq!(rest, "(count ?x) ?y");
    }

    #[test]
    fn test_find_matching_paren() {
        let s = "(foo (bar baz))";
        let end = find_matching_paren(s).unwrap();
        assert_eq!(end, 14);
        assert_eq!(&s[..=end], "(foo (bar baz))");
    }

    #[test]
    fn test_find_matching_bracket() {
        let s = "[1 [2 3]]";
        let end = find_matching_bracket(s).unwrap();
        assert_eq!(end, 8);
        assert_eq!(&s[..=end], "[1 [2 3]]");
    }

    #[test]
    fn test_sexpr_token_as_atom() {
        let token = SexprToken::Atom("foo".to_string());
        assert_eq!(token.as_atom().unwrap(), "foo");

        let list_token = SexprToken::List(vec![]);
        assert!(list_token.as_atom().is_err());
    }

    #[test]
    fn test_sexpr_token_expect_atom() {
        let token = SexprToken::Atom("foo".to_string());
        assert_eq!(token.expect_atom("variable").unwrap(), "foo");

        let list_token = SexprToken::List(vec![]);
        let err = list_token.expect_atom("variable").unwrap_err();
        assert!(err.to_string().contains("variable"));
    }
}
