//! SPARQL Lexer implementation using winnow.
//!
//! Tokenizes SPARQL input into a stream of tokens with source spans.

use std::sync::Arc;

use winnow::ascii::digit1;
use winnow::combinator::{alt, delimited, opt, peek, preceded};
use winnow::error::{ContextError, StrContext};
use winnow::stream::{AsChar, Location, Stream}; // Location trait provides current_token_start()
use winnow::token::{any, one_of, take_till, take_while};
use winnow::{LocatingSlice, ModalResult, Parser};

use super::chars::*;
use super::token::{keyword_from_str, Token, TokenKind};
use crate::span::SourceSpan;

/// Input type for the lexer - tracks position for spans.
pub type Input<'a> = LocatingSlice<&'a str>;

/// Lexer for SPARQL queries.
pub struct Lexer<'a> {
    input: &'a str,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer for the given input.
    pub fn new(input: &'a str) -> Self {
        Self { input }
    }

    /// Tokenize the entire input.
    pub fn tokenize(self) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut input = LocatingSlice::new(self.input);

        loop {
            // Skip whitespace and comments
            skip_ws_and_comments(&mut input);

            if input.is_empty() {
                // Add EOF token
                let pos = input.current_token_start();
                tokens.push(Token::new(TokenKind::Eof, SourceSpan::point(pos)));
                break;
            }

            let start = input.current_token_start();

            match next_token(&mut input) {
                Ok(kind) => {
                    let end = input.current_token_start();
                    tokens.push(Token::new(kind, SourceSpan::new(start, end)));
                }
                Err(_) => {
                    // On error, skip one character and emit an error token
                    let c = any::<_, ContextError>.parse_next(&mut input).unwrap_or('?');
                    let end = input.current_token_start();
                    tokens.push(Token::new(
                        TokenKind::Error(Arc::from(format!("unexpected character: '{c}'"))),
                        SourceSpan::new(start, end),
                    ));
                }
            }
        }

        tokens
    }
}

/// Skip whitespace and comments.
fn skip_ws_and_comments(input: &mut Input<'_>) {
    loop {
        // Skip whitespace
        let _: ModalResult<&str, ContextError> = take_while(0.., is_ws).parse_next(input);

        // Check for comment
        if input.starts_with('#') {
            // Skip until end of line
            let _: ModalResult<&str, ContextError> =
                take_till(0.., |c| c == '\n' || c == '\r').parse_next(input);
            // Skip the newline if present
            let _: ModalResult<Option<char>, ContextError> =
                opt(one_of(['\n', '\r'])).parse_next(input);
        } else {
            break;
        }
    }
}

/// Parse the next token.
fn next_token(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    alt((
        // Multi-char operators (must come before single-char)
        parse_double_caret,
        parse_triple_start, // RDF-star << (must come before single <)
        parse_triple_end,   // RDF-star >> (must come before single >)
        parse_double_pipe,
        parse_double_amp,
        parse_ne,
        parse_le,
        parse_ge,
        // IRIs
        parse_iri_ref,
        // Blank nodes (must come before prefixed names - both can start with '_')
        parse_blank_node_label,
        parse_anon,
        // NIL: () with optional whitespace
        parse_nil,
        // Default prefix (:name or just :)
        parse_default_prefix,
        // Prefixed names and keywords (handles overlap with 'a' keyword)
        parse_prefixed_name_or_keyword,
        // Variables
        parse_variable,
        // Literals
        parse_string_literal,
        parse_number,
        // Language tags (must come before punctuation which handles '@')
        parse_lang_tag,
        // Single-char punctuation (must come after multi-char)
        parse_punctuation,
    ))
    .parse_next(input)
}

// =============================================================================
// IRI Parsing
// =============================================================================

/// Parse an IRI reference: `<...>`
fn parse_iri_ref(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    delimited('<', parse_iri_content, '>')
        .map(|s: String| TokenKind::Iri(Arc::from(s)))
        .context(StrContext::Label("IRI"))
        .parse_next(input)
}

/// Parse the content inside an IRI (validates characters and handles escapes).
fn parse_iri_content(input: &mut Input<'_>) -> ModalResult<String> {
    let mut result = String::new();

    loop {
        // Take valid IRI characters
        let chunk: &str = take_while(0.., is_iri_char).parse_next(input)?;
        result.push_str(chunk);

        if input.is_empty() || input.starts_with('>') {
            break;
        }

        // Check for escape sequence
        if input.starts_with('\\') {
            '\\'.parse_next(input)?;
            // IRI escapes are \uXXXX or \UXXXXXXXX only
            if input.starts_with('u') || input.starts_with('U') {
                if let Some(c) = parse_unicode_escape(input)? {
                    result.push(c);
                } else {
                    // Invalid unicode escape - reject the IRI
                    return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
                }
            } else {
                // Invalid escape in IRI
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }
        } else {
            // Invalid character in IRI - reject
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        }
    }

    if result.is_empty() {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    Ok(result)
}

/// Parse a Unicode escape sequence (\uXXXX or \UXXXXXXXX).
/// The leading backslash should already be consumed.
fn parse_unicode_escape(input: &mut Input<'_>) -> ModalResult<Option<char>> {
    if input.starts_with('u') {
        'u'.parse_next(input)?;
        let hex: &str = take_while(4..=4, AsChar::is_hex_digit).parse_next(input)?;
        let code = u32::from_str_radix(hex, 16).unwrap_or(0xFFFD);
        Ok(char::from_u32(code))
    } else if input.starts_with('U') {
        'U'.parse_next(input)?;
        let hex: &str = take_while(8..=8, AsChar::is_hex_digit).parse_next(input)?;
        let code = u32::from_str_radix(hex, 16).unwrap_or(0xFFFD);
        Ok(char::from_u32(code))
    } else {
        Ok(None)
    }
}

// =============================================================================
// Prefixed Names and Keywords
// =============================================================================

/// Parse a default prefix name (`:local`) or default prefix namespace (`:`).
///
/// In SPARQL, `:name` uses the default (empty) prefix, which is very common.
fn parse_default_prefix(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    // Must start with ':'
    ':'.parse_next(input)?;

    // Try to parse local part
    let local = opt(parse_pn_local).parse_next(input)?;

    match local {
        Some(local) => Ok(TokenKind::PrefixedName {
            prefix: Arc::from(""),
            local: Arc::from(local.as_str()),
        }),
        None => Ok(TokenKind::PrefixedNameNs(Arc::from(""))),
    }
}

/// Parse a prefixed name or keyword.
///
/// This handles the ambiguity between:
/// - Keywords (SELECT, WHERE, etc.)
/// - The 'a' keyword (rdf:type shorthand)
/// - Prefixed names (prefix:local)
/// - Prefix namespace (prefix:)
///
/// Note: PN_PREFIX must start with PN_CHARS_BASE (not '_' or digit).
/// Keywords can be any alphanumeric sequence.
fn parse_prefixed_name_or_keyword(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    let start = input.checkpoint();

    // First character determines if this could be a prefixed name
    let first_char = input
        .chars()
        .next()
        .ok_or_else(|| winnow::error::ErrMode::Backtrack(ContextError::new()))?;

    let is_valid_prefix_start = is_pn_prefix_start(first_char);

    // Parse the word - need to handle dots carefully
    // PN_PREFIX can contain dots in the middle but cannot end with a dot
    let mut word = String::new();

    // First char (already validated above)
    let c: char = any.parse_next(input)?;
    word.push(c);

    // Continue with more chars
    loop {
        // Take PN_CHARS (no dots)
        let chunk: &str = take_while(0.., is_pn_chars).parse_next(input)?;
        word.push_str(chunk);

        if input.is_empty() {
            break;
        }

        // Check for '.' - only consume if followed by PN_CHARS (middle dot)
        if input.starts_with('.') {
            let rest = &input.as_ref()[1..];
            if let Some(next_char) = rest.chars().next() {
                if is_pn_chars(next_char) {
                    // Dot followed by valid char - consume the dot
                    '.'.parse_next(input)?;
                    word.push('.');
                    continue;
                }
            }
            // Dot not followed by valid char - don't consume
            break;
        }
        break;
    }

    // Check if followed by a colon (prefixed name)
    if peek(opt(':')).parse_next(input)?.is_some() {
        // For prefixed names, the prefix must start with PN_CHARS_BASE
        if !is_valid_prefix_start {
            input.reset(&start);
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        }

        // Consume the colon
        ':'.parse_next(input)?;

        // Try to parse local part
        let local = opt(parse_pn_local).parse_next(input)?;

        match local {
            Some(local) => Ok(TokenKind::PrefixedName {
                prefix: Arc::from(word.as_str()),
                local: Arc::from(local.as_str()),
            }),
            None => Ok(TokenKind::PrefixedNameNs(Arc::from(word.as_str()))),
        }
    } else {
        // No colon - check if it's a keyword
        match keyword_from_str(&word) {
            Some(kw) => Ok(kw),
            None => {
                // Not a keyword and no colon - this is an error
                // Reset and fail
                input.reset(&start);
                Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
            }
        }
    }
}

/// Parse a local name (after the colon in a prefixed name).
///
/// Returns a String because we need to handle PLX escapes.
/// Local names can contain '.' but cannot end with it.
fn parse_pn_local(input: &mut Input<'_>) -> ModalResult<String> {
    // PN_LOCAL can start with PN_CHARS_U, ':', digit, or PLX
    let first_char = input
        .chars()
        .next()
        .ok_or_else(|| winnow::error::ErrMode::Backtrack(ContextError::new()))?;

    if !is_pn_local_start(first_char) && first_char != '%' && first_char != '\\' {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    let mut result = String::new();

    loop {
        // Take regular local name characters (except '.', handle separately)
        let chunk: &str =
            take_while(0.., |c: char| is_pn_chars(c) || c == ':').parse_next(input)?;
        result.push_str(chunk);

        if input.is_empty() {
            break;
        }

        // Check for '.' - only consume if followed by more local name chars
        if input.starts_with('.') {
            // Peek at what comes after the dot
            let rest = &input.as_ref()[1..];
            if let Some(next_char) = rest.chars().next() {
                if is_pn_chars(next_char)
                    || next_char == ':'
                    || next_char == '%'
                    || next_char == '\\'
                {
                    // Dot followed by valid local name char - consume the dot
                    '.'.parse_next(input)?;
                    result.push('.');
                    continue;
                }
            }
            // Dot not followed by valid char - don't consume, break
            break;
        }

        // Check for '/' - extension beyond strict SPARQL spec for convenience.
        // Consume '/' if followed by local name chars but NOT if followed by another prefix.
        // This allows `hsc:product/123` while preserving `hsc:parent/ex:child` as a path sequence.
        if input.starts_with('/') {
            let rest = &input.as_ref()[1..];
            if let Some(next_char) = rest.chars().next() {
                // If next char could start a local name AND it's not the start of another prefix,
                // include the slash in this local name.
                // Another prefix would be: letters followed by ':'
                if is_pn_chars(next_char) || next_char.is_ascii_digit() {
                    // Check if this looks like another prefixed name (prefix:local)
                    // by scanning ahead for a pattern like "word:"
                    let looks_like_prefix = rest
                        .find(':')
                        .map(|colon_pos| {
                            // Everything before the colon should be valid prefix chars
                            let potential_prefix = &rest[..colon_pos];
                            !potential_prefix.is_empty()
                                && potential_prefix
                                    .chars()
                                    .all(|c| is_pn_chars_base(c) || c == '.')
                        })
                        .unwrap_or(false);

                    if !looks_like_prefix {
                        // Not another prefix, consume the slash as part of local name
                        '/'.parse_next(input)?;
                        result.push('/');
                        continue;
                    }
                }
            }
            // Slash followed by another prefix or invalid char - don't consume, break
            break;
        }

        // Check for PLX (percent-encoded or escaped character)
        if input.starts_with('%') {
            // Percent-encoded: %HH
            '%'.parse_next(input)?;
            let hex: &str = take_while(2..=2, AsChar::is_hex_digit).parse_next(input)?;
            if hex.len() != 2 {
                // Invalid percent encoding
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }
            result.push('%');
            result.push_str(hex);
        } else if input.starts_with('\\') {
            // Local escape: \ followed by specific chars
            '\\'.parse_next(input)?;
            let escaped: char = any.parse_next(input)?;
            // Valid local escapes: _~.-!$&'()*+,;=/?#@%
            if "_~.-!$&'()*+,;=/?#@%".contains(escaped) {
                result.push(escaped);
            } else {
                // Invalid escape
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }
        } else {
            break;
        }
    }

    if result.is_empty() {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    Ok(result)
}

// =============================================================================
// Variables
// =============================================================================

/// Parse a variable: `?name` or `$name`
fn parse_variable(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    preceded(one_of(['?', '$']), parse_varname)
        .map(|name: &str| TokenKind::Var(Arc::from(name)))
        .context(StrContext::Label("variable"))
        .parse_next(input)
}

/// Parse a variable name (after the sigil).
fn parse_varname<'a>(input: &mut Input<'a>) -> ModalResult<&'a str> {
    (
        take_while(1, is_varname_start),
        take_while(0.., is_varname_char),
    )
        .take()
        .parse_next(input)
}

// =============================================================================
// Blank Nodes
// =============================================================================

/// Parse a blank node label: `_:name`
fn parse_blank_node_label(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    preceded("_:", parse_blank_node_name)
        .map(|name: &str| TokenKind::BlankNodeLabel(Arc::from(name)))
        .context(StrContext::Label("blank node"))
        .parse_next(input)
}

/// Parse a blank node name (after `_:`).
///
/// Rejects blank node labels that end with '.'.
fn parse_blank_node_name<'a>(input: &mut Input<'a>) -> ModalResult<&'a str> {
    // BLANK_NODE_LABEL ::= '_:' (PN_CHARS_U | [0-9]) ((PN_CHARS | '.')* PN_CHARS)?
    let result: &str = (
        take_while(1, |c: char| is_pn_chars_u(c) || c.is_ascii_digit()),
        take_while(0.., |c: char| is_pn_chars(c) || c == '.'),
    )
        .take()
        .parse_next(input)?;

    // Blank node label cannot end with '.' - reject rather than silently fix
    if result.ends_with('.') {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    Ok(result)
}

/// Parse anonymous blank node: `[]`
fn parse_anon(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    // Check for [] with optional whitespace inside
    ('[', take_while(0.., is_ws), ']')
        .map(|_| TokenKind::Anon)
        .parse_next(input)
}

/// Parse NIL (empty list): `()`
fn parse_nil(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    // Check for () with optional whitespace inside
    ('(', take_while(0.., is_ws), ')')
        .map(|_| TokenKind::Nil)
        .parse_next(input)
}

// =============================================================================
// String Literals
// =============================================================================

/// Parse a string literal (single or double quotes, short or long).
fn parse_string_literal(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    alt((
        parse_string_long_double,
        parse_string_long_single,
        parse_string_short_double,
        parse_string_short_single,
    ))
    .parse_next(input)
}

/// Parse a short double-quoted string: `"..."`
fn parse_string_short_double(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    delimited('"', parse_string_content_double, '"')
        .map(|s| TokenKind::String(Arc::from(s)))
        .parse_next(input)
}

/// Parse a short single-quoted string: `'...'`
fn parse_string_short_single(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    delimited('\'', parse_string_content_single, '\'')
        .map(|s| TokenKind::String(Arc::from(s)))
        .parse_next(input)
}

/// Parse a long double-quoted string: `"""..."""`
fn parse_string_long_double(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    delimited("\"\"\"", parse_long_string_content_double, "\"\"\"")
        .map(|s| TokenKind::String(Arc::from(s)))
        .parse_next(input)
}

/// Parse a long single-quoted string: `'''...'''`
fn parse_string_long_single(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    delimited("'''", parse_long_string_content_single, "'''")
        .map(|s| TokenKind::String(Arc::from(s)))
        .parse_next(input)
}

/// Parse content inside a double-quoted string (handling escapes).
fn parse_string_content_double(input: &mut Input<'_>) -> ModalResult<String> {
    let mut result = String::new();

    loop {
        // Take non-special characters
        let chunk: &str = take_while(0.., |c| c != '"' && c != '\\' && c != '\n' && c != '\r')
            .parse_next(input)?;
        result.push_str(chunk);

        // Check what's next
        if input.is_empty() || input.starts_with('"') {
            break;
        }

        if input.starts_with('\\') {
            // Handle escape sequence
            '\\'.parse_next(input)?;
            let escaped = parse_escape_char(input)?;
            result.push(escaped);
        } else {
            // Newline in short string - error
            break;
        }
    }

    Ok(result)
}

/// Parse content inside a single-quoted string (handling escapes).
fn parse_string_content_single(input: &mut Input<'_>) -> ModalResult<String> {
    let mut result = String::new();

    loop {
        let chunk: &str = take_while(0.., |c| c != '\'' && c != '\\' && c != '\n' && c != '\r')
            .parse_next(input)?;
        result.push_str(chunk);

        if input.is_empty() || input.starts_with('\'') {
            break;
        }

        if input.starts_with('\\') {
            '\\'.parse_next(input)?;
            let escaped = parse_escape_char(input)?;
            result.push(escaped);
        } else {
            break;
        }
    }

    Ok(result)
}

/// Parse content inside a long double-quoted string.
fn parse_long_string_content_double(input: &mut Input<'_>) -> ModalResult<String> {
    let mut result = String::new();

    loop {
        // Take characters until we hit a potential end or escape
        let chunk: &str = take_while(0.., |c| c != '"' && c != '\\').parse_next(input)?;
        result.push_str(chunk);

        if input.is_empty() {
            break;
        }

        if input.starts_with("\"\"\"") {
            break;
        }

        if input.starts_with('\\') {
            '\\'.parse_next(input)?;
            let escaped = parse_escape_char(input)?;
            result.push(escaped);
        } else if input.starts_with('"') {
            // Single or double quote, not triple
            let c: char = any.parse_next(input)?;
            result.push(c);
        } else {
            break;
        }
    }

    Ok(result)
}

/// Parse content inside a long single-quoted string.
fn parse_long_string_content_single(input: &mut Input<'_>) -> ModalResult<String> {
    let mut result = String::new();

    loop {
        let chunk: &str = take_while(0.., |c| c != '\'' && c != '\\').parse_next(input)?;
        result.push_str(chunk);

        if input.is_empty() {
            break;
        }

        if input.starts_with("'''") {
            break;
        }

        if input.starts_with('\\') {
            '\\'.parse_next(input)?;
            let escaped = parse_escape_char(input)?;
            result.push(escaped);
        } else if input.starts_with('\'') {
            let c: char = any.parse_next(input)?;
            result.push(c);
        } else {
            break;
        }
    }

    Ok(result)
}

/// Parse an escape character after a backslash.
///
/// Returns an error for invalid escape sequences rather than silently dropping them.
fn parse_escape_char(input: &mut Input<'_>) -> ModalResult<char> {
    let c: char = any.parse_next(input)?;
    match c {
        't' => Ok('\t'),
        'b' => Ok('\x08'),
        'n' => Ok('\n'),
        'r' => Ok('\r'),
        'f' => Ok('\x0C'),
        '"' => Ok('"'),
        '\'' => Ok('\''),
        '\\' => Ok('\\'),
        'u' => {
            // \uXXXX
            let hex: &str = take_while(4..=4, AsChar::is_hex_digit).parse_next(input)?;
            if hex.len() != 4 {
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }
            let code = u32::from_str_radix(hex, 16)
                .map_err(|_| winnow::error::ErrMode::Backtrack(ContextError::new()))?;
            char::from_u32(code)
                .ok_or_else(|| winnow::error::ErrMode::Backtrack(ContextError::new()))
        }
        'U' => {
            // \UXXXXXXXX
            let hex: &str = take_while(8..=8, AsChar::is_hex_digit).parse_next(input)?;
            if hex.len() != 8 {
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }
            let code = u32::from_str_radix(hex, 16)
                .map_err(|_| winnow::error::ErrMode::Backtrack(ContextError::new()))?;
            char::from_u32(code)
                .ok_or_else(|| winnow::error::ErrMode::Backtrack(ContextError::new()))
        }
        // Invalid escape sequence - reject
        _ => Err(winnow::error::ErrMode::Backtrack(ContextError::new())),
    }
}

// =============================================================================
// Numbers
// =============================================================================

/// Parse a numeric literal (integer, decimal, or double).
fn parse_number(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    alt((parse_double, parse_decimal, parse_integer)).parse_next(input)
}

/// Parse an integer literal (unsigned only).
///
/// Per the SPARQL spec, `INTEGER ::= [0-9]+` is unsigned.
/// `INTEGER_POSITIVE` and `INTEGER_NEGATIVE` (signed) are handled at the
/// parser level — `+`/`-` are tokenized as `Plus`/`Minus` operators.
/// This ensures `?o+10` correctly produces `Var, Plus, Integer` rather
/// than `Var, Integer(+10)` which would break expression parsing.
fn parse_integer(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    let digits: &str = digit1.parse_next(input)?;

    // Make sure it's not followed by an exponent (that would be a double)
    if peek(opt(one_of(['e', 'E']))).parse_next(input)?.is_some() {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    // Check for decimal point - only reject if followed by a digit
    // (1. should be Integer + Dot, but 1.5 should fail here and be parsed as Decimal)
    if input.starts_with('.') {
        let rest = &input.as_ref()[1..];
        if rest.chars().next().is_some_and(|c| c.is_ascii_digit()) {
            // Followed by .digit - this should be a decimal, not integer
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        }
        // Followed by . but not a digit - that's fine, 1. becomes Integer(1) + Dot
    }

    let value = digits.parse::<i64>().unwrap_or(0);
    Ok(TokenKind::Integer(value))
}

/// Parse a decimal literal (unsigned only).
///
/// Per the SPARQL spec, `DECIMAL ::= [0-9]* '.' [0-9]+` is unsigned.
/// Signs are handled at the parser level as `Plus`/`Minus` operators.
fn parse_decimal(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    // Either digits.digits or .digits
    let (whole, frac) = alt((
        // digits.digits
        (digit1, preceded('.', digit1)).map(|(w, f): (&str, &str)| (Some(w), f)),
        // .digits
        preceded('.', digit1).map(|f: &str| (None, f)),
    ))
    .parse_next(input)?;

    // Make sure it's not followed by an exponent
    if peek(opt(one_of(['e', 'E']))).parse_next(input)?.is_some() {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    let mut num_str = String::new();
    if let Some(w) = whole {
        num_str.push_str(w);
    }
    num_str.push('.');
    num_str.push_str(frac);

    Ok(TokenKind::Decimal(Arc::from(num_str)))
}

/// Parse a double (floating point) literal (unsigned mantissa).
///
/// Per the SPARQL spec, `DOUBLE` has an unsigned mantissa.
/// The leading sign is handled at the parser level as `Plus`/`Minus`.
/// Note: the exponent sign (`e-5`) IS consumed here as part of the token.
fn parse_double(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    // Mantissa (unsigned)
    let mantissa = alt((
        // digits.digits
        (digit1, '.', opt(digit1)).take(),
        // .digits
        ('.', digit1).take(),
        // digits (exponent required)
        digit1,
    ))
    .parse_next(input)?;

    // Exponent (required for double)
    one_of(['e', 'E']).parse_next(input)?;
    let exp_sign = opt(one_of(['+', '-'])).parse_next(input)?;
    let exp_digits: &str = digit1.parse_next(input)?;

    let mut num_str = String::new();
    num_str.push_str(mantissa);
    num_str.push('e');
    if let Some(s) = exp_sign {
        num_str.push(s);
    }
    num_str.push_str(exp_digits);

    let value = num_str.parse::<f64>().unwrap_or(f64::NAN);
    Ok(TokenKind::Double(value))
}

// =============================================================================
// Language Tags
// =============================================================================

/// Parse a language tag: `@en`, `@en-US`, etc.
///
/// LANGTAG ::= '@' [a-zA-Z]+ ('-' [a-zA-Z0-9]+)*
fn parse_lang_tag(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    // Must start with @
    '@'.parse_next(input)?;

    // First part: [a-zA-Z]+
    let first: &str = take_while(1.., |c: char| c.is_ascii_alphabetic()).parse_next(input)?;

    let mut tag = first.to_string();

    // Subsequent parts: ('-' [a-zA-Z0-9]+)*
    while input.starts_with('-') {
        // Peek ahead to check if there's alphanumeric after the hyphen
        let rest = &input.as_ref()[1..];
        if rest
            .chars()
            .next()
            .is_none_or(|c| !c.is_ascii_alphanumeric())
        {
            break;
        }

        '-'.parse_next(input)?;
        let part: &str = take_while(1.., |c: char| c.is_ascii_alphanumeric()).parse_next(input)?;
        tag.push('-');
        tag.push_str(part);
    }

    Ok(TokenKind::LangTag(Arc::from(tag)))
}

// =============================================================================
// Operators and Punctuation
// =============================================================================

fn parse_double_caret(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    "^^".map(|_| TokenKind::DoubleCaret).parse_next(input)
}

fn parse_triple_start(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    "<<".map(|_| TokenKind::TripleStart).parse_next(input)
}

fn parse_triple_end(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    ">>".map(|_| TokenKind::TripleEnd).parse_next(input)
}

fn parse_double_pipe(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    "||".map(|_| TokenKind::Or).parse_next(input)
}

fn parse_double_amp(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    "&&".map(|_| TokenKind::And).parse_next(input)
}

fn parse_ne(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    "!=".map(|_| TokenKind::Ne).parse_next(input)
}

fn parse_le(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    "<=".map(|_| TokenKind::Le).parse_next(input)
}

fn parse_ge(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    ">=".map(|_| TokenKind::Ge).parse_next(input)
}

/// Parse single-character punctuation.
fn parse_punctuation(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    any.verify_map(|c| match c {
        '{' => Some(TokenKind::LBrace),
        '}' => Some(TokenKind::RBrace),
        '(' => Some(TokenKind::LParen),
        ')' => Some(TokenKind::RParen),
        '[' => Some(TokenKind::LBracket),
        ']' => Some(TokenKind::RBracket),
        '.' => Some(TokenKind::Dot),
        ',' => Some(TokenKind::Comma),
        ';' => Some(TokenKind::Semicolon),
        '@' => Some(TokenKind::At),
        '=' => Some(TokenKind::Eq),
        '<' => Some(TokenKind::Lt),
        '>' => Some(TokenKind::Gt),
        '+' => Some(TokenKind::Plus),
        '-' => Some(TokenKind::Minus),
        '*' => Some(TokenKind::Star),
        '/' => Some(TokenKind::Slash),
        '!' => Some(TokenKind::Bang),
        '?' => Some(TokenKind::Question),
        '|' => Some(TokenKind::Pipe),
        '^' => Some(TokenKind::Caret),
        _ => None,
    })
    .parse_next(input)
}

/// Tokenize a SPARQL query string.
pub fn tokenize(input: &str) -> Vec<Token> {
    Lexer::new(input).tokenize()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(input: &str) -> Vec<TokenKind> {
        tokenize(input)
            .into_iter()
            .map(|t| t.kind)
            .filter(|k| !matches!(k, TokenKind::Eof))
            .collect()
    }

    #[test]
    fn test_iri() {
        assert_eq!(
            tok("<http://example.org/>"),
            vec![TokenKind::Iri(Arc::from("http://example.org/"))]
        );
    }

    #[test]
    fn test_prefixed_name() {
        assert_eq!(
            tok("ex:name"),
            vec![TokenKind::PrefixedName {
                prefix: Arc::from("ex"),
                local: Arc::from("name"),
            }]
        );

        assert_eq!(tok("ex:"), vec![TokenKind::PrefixedNameNs(Arc::from("ex"))]);
    }

    #[test]
    fn test_variable() {
        assert_eq!(tok("?name"), vec![TokenKind::Var(Arc::from("name"))]);
        assert_eq!(tok("$var"), vec![TokenKind::Var(Arc::from("var"))]);
    }

    #[test]
    fn test_keywords() {
        assert_eq!(tok("SELECT"), vec![TokenKind::KwSelect]);
        assert_eq!(tok("select"), vec![TokenKind::KwSelect]);
        assert_eq!(tok("WHERE"), vec![TokenKind::KwWhere]);
        assert_eq!(tok("OPTIONAL"), vec![TokenKind::KwOptional]);
        assert_eq!(tok("a"), vec![TokenKind::KwA]);
    }

    #[test]
    fn test_blank_node() {
        assert_eq!(
            tok("_:b1"),
            vec![TokenKind::BlankNodeLabel(Arc::from("b1"))]
        );
        assert_eq!(tok("[]"), vec![TokenKind::Anon]);
    }

    #[test]
    fn test_string_literal() {
        assert_eq!(
            tok("\"hello\""),
            vec![TokenKind::String(Arc::from("hello"))]
        );
        assert_eq!(tok("'hello'"), vec![TokenKind::String(Arc::from("hello"))]);
        assert_eq!(
            tok("\"hello\\nworld\""),
            vec![TokenKind::String(Arc::from("hello\nworld"))]
        );
    }

    #[test]
    fn test_numbers() {
        assert_eq!(tok("42"), vec![TokenKind::Integer(42)]);
        // Signs are tokenized as separate Plus/Minus operators (SPARQL spec:
        // INTEGER is unsigned; INTEGER_POSITIVE/NEGATIVE are grammar-level)
        assert_eq!(tok("-42"), vec![TokenKind::Minus, TokenKind::Integer(42)]);
        assert_eq!(tok("+10"), vec![TokenKind::Plus, TokenKind::Integer(10)]);
        assert_eq!(tok("3.14"), vec![TokenKind::Decimal(Arc::from("3.14"))]);
        assert_eq!(tok("1e10"), vec![TokenKind::Double(1e10)]);
        // Exponent signs are still consumed as part of the double token
        assert_eq!(tok("1.5e-3"), vec![TokenKind::Double(1.5e-3)]);
    }

    #[test]
    fn test_operators() {
        assert_eq!(tok("^^"), vec![TokenKind::DoubleCaret]);
        assert_eq!(tok("||"), vec![TokenKind::Or]);
        assert_eq!(tok("&&"), vec![TokenKind::And]);
        assert_eq!(tok("!="), vec![TokenKind::Ne]);
        assert_eq!(tok("<="), vec![TokenKind::Le]);
        assert_eq!(tok(">="), vec![TokenKind::Ge]);
    }

    #[test]
    fn test_punctuation() {
        // Note: [] is parsed as Anon (anonymous blank node) in SPARQL
        // Note: () is parsed as Nil (empty list) in SPARQL
        assert_eq!(
            tok("{}.,;"),
            vec![
                TokenKind::LBrace,
                TokenKind::RBrace,
                TokenKind::Dot,
                TokenKind::Comma,
                TokenKind::Semicolon,
            ]
        );
        // Test brackets/parens separately - only become Anon/Nil when empty
        assert_eq!(tok("["), vec![TokenKind::LBracket]);
        assert_eq!(tok("]"), vec![TokenKind::RBracket]);
        assert_eq!(tok("("), vec![TokenKind::LParen]);
        assert_eq!(tok(")"), vec![TokenKind::RParen]);
    }

    #[test]
    fn test_anon_and_nil() {
        // [] is Anon (anonymous blank node)
        assert_eq!(tok("[]"), vec![TokenKind::Anon]);
        assert_eq!(tok("[ ]"), vec![TokenKind::Anon]); // with whitespace

        // () is Nil (empty list)
        assert_eq!(tok("()"), vec![TokenKind::Nil]);
        assert_eq!(tok("( )"), vec![TokenKind::Nil]); // with whitespace
    }

    #[test]
    fn test_comments() {
        assert_eq!(
            tok("SELECT # this is a comment\n?x"),
            vec![TokenKind::KwSelect, TokenKind::Var(Arc::from("x"))]
        );
    }

    #[test]
    fn test_simple_query() {
        let tokens = tok("SELECT ?name WHERE { ?s <http://example.org/name> ?name }");
        assert_eq!(
            tokens,
            vec![
                TokenKind::KwSelect,
                TokenKind::Var(Arc::from("name")),
                TokenKind::KwWhere,
                TokenKind::LBrace,
                TokenKind::Var(Arc::from("s")),
                TokenKind::Iri(Arc::from("http://example.org/name")),
                TokenKind::Var(Arc::from("name")),
                TokenKind::RBrace,
            ]
        );
    }

    #[test]
    fn test_spans() {
        let tokens = tokenize("SELECT ?x");
        assert_eq!(tokens[0].span, SourceSpan::new(0, 6)); // SELECT
        assert_eq!(tokens[1].span, SourceSpan::new(7, 9)); // ?x
    }

    #[test]
    fn test_iri_escapes() {
        // Valid IRI with unicode escapes
        assert_eq!(
            tok(r"<http://example.org/\u00E9>"),
            vec![TokenKind::Iri(Arc::from("http://example.org/é"))]
        );
    }

    #[test]
    fn test_iri_invalid_chars() {
        // IRIs with invalid characters should fail to parse
        // When IRI parsing fails, the < will be lexed as Lt
        // and subsequent chars will be lexed individually
        let tokens = tok("<http://example.org/{bad}>");
        assert!(tokens
            .iter()
            .any(|t| matches!(t, TokenKind::Lt | TokenKind::Error(_))));
    }

    #[test]
    fn test_prefix_must_start_with_letter() {
        // Valid: prefix starts with letter
        assert_eq!(
            tok("ex:name"),
            vec![TokenKind::PrefixedName {
                prefix: Arc::from("ex"),
                local: Arc::from("name"),
            }]
        );

        // Invalid: prefix starts with underscore (should fail, _ prefixes go to blank nodes)
        // _:name is a blank node, not a prefixed name
        assert_eq!(
            tok("_:name"),
            vec![TokenKind::BlankNodeLabel(Arc::from("name"))]
        );
    }

    #[test]
    fn test_no_silent_trimming() {
        // ex:foo. should lex as prefixed name "ex:foo" + Dot
        // (trailing dot is not part of the local name)
        let tokens = tok("ex:foo.");
        assert_eq!(tokens.len(), 2);
        assert_eq!(
            tokens[0],
            TokenKind::PrefixedName {
                prefix: Arc::from("ex"),
                local: Arc::from("foo"),
            }
        );
        assert_eq!(tokens[1], TokenKind::Dot);

        // But dots in the middle are fine
        let tokens = tok("ex:foo.bar");
        assert_eq!(
            tokens[0],
            TokenKind::PrefixedName {
                prefix: Arc::from("ex"),
                local: Arc::from("foo.bar"),
            }
        );
    }

    #[test]
    fn test_invalid_string_escape() {
        // Invalid escape \x should cause an error token
        let tokens = tok(r#""hello\xworld""#);
        // The string parse will fail, resulting in error token(s)
        assert!(tokens.iter().any(|t| matches!(t, TokenKind::Error(_))));
    }

    #[test]
    fn test_default_prefix() {
        // :name (empty prefix) is valid SPARQL - very common pattern
        assert_eq!(
            tok(":name"),
            vec![TokenKind::PrefixedName {
                prefix: Arc::from(""),
                local: Arc::from("name"),
            }]
        );

        // Just : (empty prefix namespace)
        assert_eq!(tok(":"), vec![TokenKind::PrefixedNameNs(Arc::from(""))]);
    }

    #[test]
    fn test_numeric_edge_cases() {
        // .5 is a valid decimal (already handled)
        assert_eq!(tok(".5"), vec![TokenKind::Decimal(Arc::from(".5"))]);

        // 1. should be Integer + Dot (SPARQL spec doesn't allow trailing dot)
        // This matches most SPARQL implementations
        let tokens = tok("1.");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0], TokenKind::Integer(1));
        assert_eq!(tokens[1], TokenKind::Dot);
    }

    #[test]
    fn test_prefix_with_dot() {
        // ex.foo:bar - dot in the middle of prefix is valid SPARQL
        let tokens = tok("ex.foo:bar");
        assert_eq!(
            tokens[0],
            TokenKind::PrefixedName {
                prefix: Arc::from("ex.foo"),
                local: Arc::from("bar"),
            }
        );

        // ex.:name is INVALID - "ex" alone is not a valid token (not a keyword, not followed by :)
        // So this produces errors. This is correct behavior - the input is malformed.
        let tokens = tok("ex.:name");
        // "ex" produces errors (not a keyword), then "." and ":name" are lexed
        assert!(tokens.iter().any(|t| matches!(t, TokenKind::Error(_))));
        // But the :name part should still lex correctly as default prefix
        assert!(tokens
            .iter()
            .any(|t| matches!(t, TokenKind::PrefixedName { prefix, .. } if prefix.as_ref() == "")));
    }
}
