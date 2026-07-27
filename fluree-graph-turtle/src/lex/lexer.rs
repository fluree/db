//! Turtle Lexer implementation using winnow.
//!
//! Tokenizes Turtle input into a stream of tokens with source spans.
//! Fails fast on the first lexical error with a clear, actionable message.
//!
//! Most tokens are **zero-copy span tokens** — the token stores byte offsets
//! into the source input and no heap-allocated content. Only rare tokens with
//! escape sequences allocate an `Arc<str>`.

use std::sync::Arc;

use winnow::ascii::digit1;
use winnow::combinator::{alt, opt, peek, preceded};
use winnow::error::ContextError;
use winnow::stream::{AsChar, Location, Stream};
use winnow::token::{any, one_of, take_till, take_while};
use winnow::{LocatingSlice, ModalResult, Parser};

use super::chars::*;
use super::token::{Token, TokenKind};
use crate::error::{Result, TurtleError};

/// Input type for the lexer - tracks position for spans.
pub type Input<'a> = LocatingSlice<&'a str>;

/// Lexer for Turtle documents.
pub struct Lexer<'a> {
    input: &'a str,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer for the given input.
    pub fn new(input: &'a str) -> Self {
        Self { input }
    }

    /// Tokenize the entire input.
    ///
    /// Returns an error immediately on the first invalid token, providing
    /// a clear error message with line/column and source context.
    pub fn tokenize(self) -> Result<Vec<Token>> {
        crate::error::check_input_len(self.input.len())?;
        let mut tokens = Vec::new();
        let mut input = LocatingSlice::new(self.input);

        loop {
            // Skip whitespace and comments
            skip_ws_and_comments(&mut input);

            if input.is_empty() {
                let pos = input.current_token_start() as u32;
                tokens.push(Token::new(TokenKind::Eof, pos, pos));
                break;
            }

            let start_pos = input.current_token_start();

            match next_token(&mut input) {
                Ok(kind) => {
                    let end_pos = input.current_token_start();
                    tokens.push(Token::new(kind, start_pos as u32, end_pos as u32));
                }
                Err(_) => {
                    // Fail fast with a descriptive error message
                    return Err(make_lex_error(self.input, start_pos, &input));
                }
            }
        }

        Ok(tokens)
    }
}

/// Streaming lexer that produces tokens on demand.
///
/// Unlike [`Lexer::tokenize()`] which materializes all tokens into a `Vec`,
/// this produces one token per [`next_token()`](StreamingLexer::next_token)
/// call — the parser pulls tokens as needed, avoiding the upfront allocation.
pub struct StreamingLexer<'a> {
    source: &'a str,
    input: Input<'a>,
}

impl<'a> StreamingLexer<'a> {
    /// Create a new streaming lexer for the given input.
    pub fn new(source: &'a str) -> Self {
        Self {
            source,
            input: LocatingSlice::new(source),
        }
    }

    /// Get the next token. Returns an EOF token at end of input.
    pub fn next_token(&mut self) -> Result<Token> {
        skip_ws_and_comments(&mut self.input);

        if self.input.is_empty() {
            let pos = self.input.current_token_start() as u32;
            return Ok(Token::new(TokenKind::Eof, pos, pos));
        }

        let start_pos = self.input.current_token_start();

        match next_token(&mut self.input) {
            Ok(kind) => {
                let end_pos = self.input.current_token_start() as u32;
                Ok(Token::new(kind, start_pos as u32, end_pos))
            }
            Err(_) => Err(make_lex_error(self.source, start_pos, &self.input)),
        }
    }
}

/// Create a descriptive error message for an invalid token.
fn make_lex_error(source: &str, position: usize, input: &Input<'_>) -> TurtleError {
    let remaining = input.as_ref();
    let bad_char = remaining.chars().next().unwrap_or('?');
    let (line, col) = lex_line_col(source, position);
    let line_content = lex_get_line(source, line);

    let pointer = " ".repeat(col.saturating_sub(1));
    let message = if bad_char == '"' || bad_char == '\'' {
        format!(
            "unterminated string literal at line {line}, column {col}\n  |\n{line} | {line_content}\n  | {pointer}^"
        )
    } else if bad_char == '<' {
        format!(
            "invalid or unterminated IRI at line {line}, column {col}\n  |\n{line} | {line_content}\n  | {pointer}^"
        )
    } else if !bad_char.is_ascii() && !is_pn_chars_base(bad_char) {
        format!(
            "unexpected character '{}' (U+{:04X}) at line {}, column {}\n  |\n{} | {}\n  | {}^",
            bad_char.escape_unicode(),
            bad_char as u32,
            line,
            col,
            line,
            line_content,
            pointer
        )
    } else {
        format!(
            "unexpected character '{bad_char}' at line {line}, column {col}\n  |\n{line} | {line_content}\n  | {pointer}^"
        )
    };

    TurtleError::Lexer { position, message }
}

fn lex_line_col(source: &str, position: usize) -> (usize, usize) {
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

fn lex_get_line(source: &str, line_num: usize) -> &str {
    source.lines().nth(line_num.saturating_sub(1)).unwrap_or("")
}

/// Skip whitespace and comments.
fn skip_ws_and_comments(input: &mut Input<'_>) {
    loop {
        let _: ModalResult<&str, ContextError> = take_while(0.., is_ws).parse_next(input);

        if input.starts_with('#') {
            let _: ModalResult<&str, ContextError> =
                take_till(0.., |c| c == '\n' || c == '\r').parse_next(input);
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
        // IRIs
        parse_iri_ref,
        // Blank nodes (must come before prefixed names)
        parse_blank_node_label,
        parse_anon,
        // NIL: () with optional whitespace
        parse_nil,
        // Directives (@prefix, @base, @lang)
        parse_at_directive,
        // Default prefix (:name or just :)
        parse_default_prefix,
        // Prefixed names and keywords (a, true, false, PREFIX, BASE)
        parse_prefixed_name_or_keyword,
        // String literals
        parse_string_literal,
        // Numbers
        parse_number,
        // Single-char punctuation
        parse_punctuation,
    ))
    .parse_next(input)
}

// =============================================================================
// IRI Parsing
// =============================================================================

/// Parse an IRI reference (`<...>`) or an RDF 1.2 star opener (`<<` / `<<(`).
///
/// Fast path: scans to `>` without allocating. Returns `TokenKind::Iri`.
/// Slow path (rare): if `\u`/`\U` escapes are found, processes them and
/// returns `TokenKind::IriEscaped(Arc<str>)`.
///
/// Star recognition costs the common IRI path nothing: `<` is not a valid
/// IRI character, so on `<<` the `is_iri_char` scan consumes zero bytes and
/// control reaches what used to be the error fallback — the `<` peek below
/// only runs on inputs that previously failed to lex.
fn parse_iri_ref(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    '<'.parse_next(input)?;

    // Fast path: scan valid IRI chars
    let first_chunk: &str = take_while(0.., is_iri_char).parse_next(input)?;

    if input.starts_with('>') {
        // Common case: no escapes, content is in the span
        '>'.parse_next(input)?;
        return Ok(TokenKind::Iri);
    }

    // RDF 1.2 star openers: `<<` (reified triple) and `<<(` (triple term).
    // Only reachable when the second `<` is adjacent (`first_chunk` empty),
    // i.e. on input that was a hard lex error before star support.
    if first_chunk.is_empty() && input.starts_with('<') {
        '<'.parse_next(input)?;
        if input.starts_with('(') {
            '('.parse_next(input)?;
            return Ok(TokenKind::TripleTermStart);
        }
        return Ok(TokenKind::ReifiedTripleStart);
    }

    // Slow path: has unicode escapes
    if input.starts_with('\\') {
        let mut result = String::from(first_chunk);
        loop {
            '\\'.parse_next(input)?;
            if input.starts_with('u') || input.starts_with('U') {
                if let Some(c) = parse_unicode_escape(input)? {
                    result.push(c);
                } else {
                    return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
                }
            } else {
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }

            let chunk: &str = take_while(0.., is_iri_char).parse_next(input)?;
            result.push_str(chunk);

            if input.starts_with('>') {
                break;
            }
            if !input.starts_with('\\') {
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }
        }
        '>'.parse_next(input)?;
        Ok(TokenKind::IriEscaped(Arc::from(result.as_str())))
    } else if input.is_empty() {
        // Unterminated IRI
        Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
    } else {
        // Invalid character in IRI
        Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
    }
}

/// Parse a Unicode escape sequence (\uXXXX or \UXXXXXXXX).
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
// Directives (@prefix, @base, language tags)
// =============================================================================

/// Parse @ directives and language tags.
///
/// For language tags, returns bare `TokenKind::LangTag` — the tag text is
/// recovered from the span via `&input[(start+1)..end]` (stripping `@`).
fn parse_at_directive(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    '@'.parse_next(input)?;

    // Read the word after @
    let word: &str =
        take_while(1.., |c: char| c.is_ascii_alphanumeric() || c == '-').parse_next(input)?;

    match word.to_lowercase().as_str() {
        "prefix" => Ok(TokenKind::KwPrefix),
        "base" => Ok(TokenKind::KwBase),
        _ => Ok(TokenKind::LangTag),
    }
}

// =============================================================================
// Prefixed Names and Keywords
// =============================================================================

/// Parse a default prefix name (`:local`) or default prefix namespace (`:`).
///
/// Returns bare `TokenKind::PrefixedName` or `TokenKind::PrefixedNameNs`.
/// Span covers the full token (e.g., `:local` or `:`).
fn parse_default_prefix(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    ':'.parse_next(input)?;

    let had_local = opt(parse_pn_local).parse_next(input)?;

    match had_local {
        Some(()) => Ok(TokenKind::PrefixedName),
        None => Ok(TokenKind::PrefixedNameNs),
    }
}

/// Parse a prefixed name or keyword (a, true, false, PREFIX, BASE).
///
/// Returns bare `TokenKind::PrefixedName` or `TokenKind::PrefixedNameNs`.
/// Span covers the full token (e.g., `ex:name` or `ex:`).
/// Parser splits on first `:` to get `(prefix, local)`.
fn parse_prefixed_name_or_keyword(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    let start = input.checkpoint();

    let first_char = input
        .chars()
        .next()
        .ok_or_else(|| winnow::error::ErrMode::Backtrack(ContextError::new()))?;

    let is_valid_prefix_start = is_pn_prefix_start(first_char);

    let mut word = String::new();
    let c: char = any.parse_next(input)?;
    word.push(c);

    loop {
        let chunk: &str = take_while(0.., is_pn_chars).parse_next(input)?;
        word.push_str(chunk);

        if input.is_empty() {
            break;
        }

        if input.starts_with('.') {
            let rest = &input.as_ref()[1..];
            if let Some(next_char) = rest.chars().next() {
                if is_pn_chars(next_char) {
                    '.'.parse_next(input)?;
                    word.push('.');
                    continue;
                }
            }
            break;
        }
        break;
    }

    // Check if followed by a colon (prefixed name)
    if peek(opt(':')).parse_next(input)?.is_some() {
        if !is_valid_prefix_start {
            input.reset(&start);
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        }

        ':'.parse_next(input)?;

        let had_local = opt(parse_pn_local).parse_next(input)?;

        match had_local {
            Some(()) => Ok(TokenKind::PrefixedName),
            None => Ok(TokenKind::PrefixedNameNs),
        }
    } else {
        // Check if it's a keyword
        match word.as_str() {
            "a" => Ok(TokenKind::KwA),
            "true" => Ok(TokenKind::KwTrue),
            "false" => Ok(TokenKind::KwFalse),
            "PREFIX" => Ok(TokenKind::KwSparqlPrefix),
            "BASE" => Ok(TokenKind::KwSparqlBase),
            "GRAPH" => Ok(TokenKind::KwGraph),
            _ => {
                input.reset(&start);
                Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
            }
        }
    }
}

/// Parse a local name (after the colon in a prefixed name).
///
/// Advances past the local name content, validating characters. Does not
/// build a String — the content is recovered from the token's byte span.
fn parse_pn_local(input: &mut Input<'_>) -> ModalResult<()> {
    let first_char = input
        .chars()
        .next()
        .ok_or_else(|| winnow::error::ErrMode::Backtrack(ContextError::new()))?;

    if !is_pn_local_start(first_char) && first_char != '%' && first_char != '\\' {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    loop {
        let _chunk: &str =
            take_while(0.., |c: char| is_pn_chars(c) || c == ':').parse_next(input)?;

        if input.is_empty() {
            break;
        }

        if input.starts_with('.') {
            let rest = &input.as_ref()[1..];
            if let Some(next_char) = rest.chars().next() {
                if is_pn_chars(next_char)
                    || next_char == ':'
                    || next_char == '%'
                    || next_char == '\\'
                {
                    '.'.parse_next(input)?;
                    continue;
                }
            }
            break;
        }

        if input.starts_with('%') {
            '%'.parse_next(input)?;
            let hex: &str = take_while(2..=2, AsChar::is_hex_digit).parse_next(input)?;
            if hex.len() != 2 {
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }
        } else if input.starts_with('\\') {
            '\\'.parse_next(input)?;
            let escaped: char = any.parse_next(input)?;
            if "_~.-!$&'()*+,;=/?#@%".contains(escaped) {
                // Valid local escape — consumed, content is in the span
            } else {
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }
        } else {
            break;
        }
    }

    Ok(())
}

// =============================================================================
// Blank Nodes
// =============================================================================

/// Parse a blank node label: `_:name`
///
/// Returns bare `TokenKind::BlankNodeLabel`. Span covers `_:name`.
/// Label: `&input[(start+2)..end]`.
fn parse_blank_node_label(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    preceded("_:", parse_blank_node_name)
        .map(|_name: &str| TokenKind::BlankNodeLabel)
        .parse_next(input)
}

/// Parse a blank node name (after `_:`).
///
/// Grammar: `BLANK_NODE_LABEL ::= '_:' (PN_CHARS_U | [0-9]) ((PN_CHARS | '.')*
/// PN_CHARS)?` — interior dots (including consecutive ones, `_:a..b`) are
/// label characters, but the label must not END in a dot. The scan stays
/// deliberately greedy over `PN_CHARS | '.'` (one branch-light predicate per
/// char, no lookahead); any trailing dots are then rewound so they lex as the
/// statement terminator instead: `_:o6.` is `BlankNodeLabel("o6")` + `Dot`.
fn parse_blank_node_name<'a>(input: &mut Input<'a>) -> ModalResult<&'a str> {
    let start = input.checkpoint();
    let result: &str = (
        take_while(1, |c: char| is_pn_chars_u(c) || c.is_ascii_digit()),
        take_while(0.., |c: char| is_pn_chars(c) || c == '.'),
    )
        .take()
        .parse_next(input)?;

    let label = result.trim_end_matches('.');
    if label.len() < result.len() {
        // Rewind the trailing dots: reset to the start of the name and
        // re-consume exactly the label bytes, so the token span ends before
        // the first trailing dot. `label.len()` is a char boundary ('.' is
        // ASCII) and non-zero (the first char cannot be '.').
        input.reset(&start);
        return Ok(input.next_slice(label.len()));
    }

    Ok(result)
}

/// Parse anonymous blank node: `[]`
fn parse_anon(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    ('[', take_while(0.., is_ws), ']')
        .map(|_| TokenKind::Anon)
        .parse_next(input)
}

/// Parse NIL (empty list): `()`
fn parse_nil(input: &mut Input<'_>) -> ModalResult<TokenKind> {
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

/// Parse short double-quoted string: `"..."`
///
/// Fast path: scans to closing `"` without allocating → `TokenKind::String`.
/// Slow path: if `\` escapes found, processes them → `TokenKind::StringEscaped(Arc<str>)`.
fn parse_string_short_double(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    '"'.parse_next(input)?;

    // Fast path: scan for closing quote
    let first_chunk: &str =
        take_while(0.., |c| c != '"' && c != '\\' && c != '\n' && c != '\r').parse_next(input)?;

    // Common case: no escapes
    if input.starts_with('"') {
        '"'.parse_next(input)?;
        return Ok(TokenKind::String);
    }

    // Slow path: has escape sequences
    if input.starts_with('\\') {
        let mut result = String::from(first_chunk);
        loop {
            '\\'.parse_next(input)?;
            let escaped = parse_escape_char(input)?;
            result.push(escaped);

            let chunk: &str = take_while(0.., |c| c != '"' && c != '\\' && c != '\n' && c != '\r')
                .parse_next(input)?;
            result.push_str(chunk);

            if input.starts_with('"') || input.is_empty() {
                break;
            }
            if !input.starts_with('\\') {
                break;
            }
        }
        '"'.parse_next(input)?;
        Ok(TokenKind::StringEscaped(Arc::from(result.as_str())))
    } else {
        // Hit \n, \r, or EOF without closing quote
        Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
    }
}

/// Parse short single-quoted string: `'...'`
///
/// Fast path: scans to closing `'` without allocating → `TokenKind::String`.
/// Slow path: if `\` escapes found, processes them → `TokenKind::StringEscaped(Arc<str>)`.
fn parse_string_short_single(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    '\''.parse_next(input)?;

    // Fast path: scan for closing quote
    let first_chunk: &str =
        take_while(0.., |c| c != '\'' && c != '\\' && c != '\n' && c != '\r').parse_next(input)?;

    // Common case: no escapes
    if input.starts_with('\'') {
        '\''.parse_next(input)?;
        return Ok(TokenKind::String);
    }

    // Slow path: has escape sequences
    if input.starts_with('\\') {
        let mut result = String::from(first_chunk);
        loop {
            '\\'.parse_next(input)?;
            let escaped = parse_escape_char(input)?;
            result.push(escaped);

            let chunk: &str = take_while(0.., |c| c != '\'' && c != '\\' && c != '\n' && c != '\r')
                .parse_next(input)?;
            result.push_str(chunk);

            if input.starts_with('\'') || input.is_empty() {
                break;
            }
            if !input.starts_with('\\') {
                break;
            }
        }
        '\''.parse_next(input)?;
        Ok(TokenKind::StringEscaped(Arc::from(result.as_str())))
    } else {
        Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
    }
}

/// Parse long double-quoted string: `"""..."""`
///
/// Returns `TokenKind::LongString` if no escapes (span covers full token
/// including triple-quotes), or `TokenKind::StringEscaped(Arc<str>)` if
/// escape sequences were processed.
fn parse_string_long_double(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    "\"\"\"".parse_next(input)?;

    let mut has_escapes = false;
    let mut result = String::new();

    loop {
        let chunk: &str = take_while(0.., |c| c != '"' && c != '\\').parse_next(input)?;
        result.push_str(chunk);

        if input.is_empty() {
            break;
        }

        if input.starts_with("\"\"\"") {
            break;
        }

        if input.starts_with('\\') {
            has_escapes = true;
            '\\'.parse_next(input)?;
            let escaped = parse_escape_char(input)?;
            result.push(escaped);
        } else if input.starts_with('"') {
            let c: char = any.parse_next(input)?;
            result.push(c);
        } else {
            break;
        }
    }

    "\"\"\"".parse_next(input)?;

    if has_escapes {
        Ok(TokenKind::StringEscaped(Arc::from(result.as_str())))
    } else {
        // No escapes — span is sufficient (content = &input[(start+3)..(end-3)])
        Ok(TokenKind::LongString)
    }
}

/// Parse long single-quoted string: `'''...'''`
///
/// Returns `TokenKind::LongString` if no escapes, or
/// `TokenKind::StringEscaped(Arc<str>)` if escape sequences were processed.
fn parse_string_long_single(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    "'''".parse_next(input)?;

    let mut has_escapes = false;
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
            has_escapes = true;
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

    "'''".parse_next(input)?;

    if has_escapes {
        Ok(TokenKind::StringEscaped(Arc::from(result.as_str())))
    } else {
        Ok(TokenKind::LongString)
    }
}

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
            let hex: &str = take_while(8..=8, AsChar::is_hex_digit).parse_next(input)?;
            if hex.len() != 8 {
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }
            let code = u32::from_str_radix(hex, 16)
                .map_err(|_| winnow::error::ErrMode::Backtrack(ContextError::new()))?;
            char::from_u32(code)
                .ok_or_else(|| winnow::error::ErrMode::Backtrack(ContextError::new()))
        }
        _ => Err(winnow::error::ErrMode::Backtrack(ContextError::new())),
    }
}

// =============================================================================
// Numbers
// =============================================================================

fn parse_number(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    alt((parse_double, parse_decimal, parse_integer)).parse_next(input)
}

fn parse_integer(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    let sign = opt(one_of(['+', '-'])).parse_next(input)?;
    let digits: &str = digit1.parse_next(input)?;

    if peek(opt(one_of(['e', 'E']))).parse_next(input)?.is_some() {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    if input.starts_with('.') {
        let rest = &input.as_ref()[1..];
        if rest.chars().next().is_some_and(|c| c.is_ascii_digit()) {
            return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
        }
    }

    let mut num_str = String::new();
    if let Some(s) = sign {
        num_str.push(s);
    }
    num_str.push_str(digits);

    // xsd:integer is unbounded: values past i64 promote to BigInt downstream
    // (span-based token, like Decimal) instead of silently corrupting.
    match num_str.parse::<i64>() {
        Ok(value) => Ok(TokenKind::Integer(value)),
        Err(_) => Ok(TokenKind::IntegerOverflow),
    }
}

/// Parse a decimal literal.
///
/// Returns bare `TokenKind::Decimal` — the numeric text is recovered from
/// the span via `&input[start..end]`.
fn parse_decimal(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    let _sign = opt(one_of(['+', '-'])).parse_next(input)?;

    let (_whole, _frac) = alt((
        (digit1, preceded('.', digit1)).map(|(w, f): (&str, &str)| (Some(w), f)),
        preceded('.', digit1).map(|f: &str| (None, f)),
    ))
    .parse_next(input)?;

    if peek(opt(one_of(['e', 'E']))).parse_next(input)?.is_some() {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
    }

    Ok(TokenKind::Decimal)
}

fn parse_double(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    let sign = opt(one_of(['+', '-'])).parse_next(input)?;

    let mantissa = alt((
        (digit1, '.', opt(digit1)).take(),
        ('.', digit1).take(),
        digit1,
    ))
    .parse_next(input)?;

    one_of(['e', 'E']).parse_next(input)?;
    let exp_sign = opt(one_of(['+', '-'])).parse_next(input)?;
    let exp_digits: &str = digit1.parse_next(input)?;

    let mut num_str = String::new();
    if let Some(s) = sign {
        num_str.push(s);
    }
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
// Operators and Punctuation
// =============================================================================

fn parse_double_caret(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    "^^".map(|_| TokenKind::DoubleCaret).parse_next(input)
}

fn parse_punctuation(input: &mut Input<'_>) -> ModalResult<TokenKind> {
    let start = input.checkpoint();
    let c: char = any.parse_next(input)?;
    match c {
        '.' => Ok(TokenKind::Dot),
        ',' => Ok(TokenKind::Comma),
        ';' => Ok(TokenKind::Semicolon),
        '[' => Ok(TokenKind::LBracket),
        ']' => Ok(TokenKind::RBracket),
        '(' => Ok(TokenKind::LParen),
        ')' => Ok(TokenKind::RParen),
        // `{|` opens an RDF 1.2 annotation block; bare `{` stays a TriG
        // graph-block brace. One extra byte peek on the `{` branch only.
        '{' => {
            if input.starts_with('|') {
                '|'.parse_next(input)?;
                Ok(TokenKind::AnnotationOpen)
            } else {
                Ok(TokenKind::LBrace)
            }
        }
        '}' => Ok(TokenKind::RBrace),
        // RDF 1.2 closers/markers. `|`, `>` and `~` were hard lex errors
        // before star support, so these arms never fire on pre-star input.
        // Lone `|` / `>` fall to the reset-and-error arm, preserving the
        // pre-star error position.
        '|' if input.starts_with('}') => {
            '}'.parse_next(input)?;
            Ok(TokenKind::AnnotationClose)
        }
        '>' if input.starts_with('>') => {
            '>'.parse_next(input)?;
            Ok(TokenKind::ReifiedTripleEnd)
        }
        '~' => Ok(TokenKind::Tilde),
        _ => {
            input.reset(&start);
            Err(winnow::error::ErrMode::Backtrack(ContextError::new()))
        }
    }
}

/// Tokenize a Turtle document string.
///
/// Returns an error immediately on the first invalid token, with a clear
/// error message including line/column information and source context.
pub fn tokenize(input: &str) -> Result<Vec<Token>> {
    Lexer::new(input).tokenize()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(input: &str) -> Vec<TokenKind> {
        tokenize(input)
            .unwrap()
            .into_iter()
            .map(|t| t.kind)
            .filter(|k| !matches!(k, TokenKind::Eof))
            .collect()
    }

    /// Helper: tokenize and return (kind, span_text) pairs for assertions.
    fn tok_spans(input: &str) -> Vec<(TokenKind, &str)> {
        tokenize(input)
            .unwrap()
            .into_iter()
            .filter(|t| !matches!(t.kind, TokenKind::Eof))
            .map(|t| {
                let text = &input[t.start as usize..t.end as usize];
                (t.kind, text)
            })
            .collect()
    }

    #[test]
    fn test_iri() {
        assert_eq!(tok("<http://example.org/>"), vec![TokenKind::Iri]);
        // Verify span content
        let spans = tok_spans("<http://example.org/>");
        assert_eq!(spans[0].1, "<http://example.org/>");
    }

    #[test]
    fn test_empty_iri() {
        // Empty IRI (relative reference to base)
        assert_eq!(tok("<>"), vec![TokenKind::Iri]);
        let spans = tok_spans("<>");
        assert_eq!(spans[0].1, "<>");
    }

    #[test]
    fn test_iri_with_unicode_escape() {
        // IRI with \u escape → IriEscaped
        let tokens = tok("<http://example.org/\\u0041>");
        assert_eq!(tokens.len(), 1);
        match &tokens[0] {
            TokenKind::IriEscaped(s) => assert_eq!(s.as_ref(), "http://example.org/A"),
            other => panic!("Expected IriEscaped, got {other:?}"),
        }
    }

    #[test]
    fn test_prefixed_name() {
        assert_eq!(tok("ex:name"), vec![TokenKind::PrefixedName]);
        assert_eq!(tok("ex:"), vec![TokenKind::PrefixedNameNs]);

        // Verify span content
        let spans = tok_spans("ex:name");
        assert_eq!(spans[0].1, "ex:name");

        let spans = tok_spans("ex:");
        assert_eq!(spans[0].1, "ex:");
    }

    #[test]
    fn test_default_prefix() {
        assert_eq!(tok(":name"), vec![TokenKind::PrefixedName]);
        assert_eq!(tok(":"), vec![TokenKind::PrefixedNameNs]);

        let spans = tok_spans(":name");
        assert_eq!(spans[0].1, ":name");
    }

    #[test]
    fn test_blank_node() {
        assert_eq!(tok("_:b1"), vec![TokenKind::BlankNodeLabel]);
        assert_eq!(tok("[]"), vec![TokenKind::Anon]);

        let spans = tok_spans("_:b1");
        assert_eq!(spans[0].1, "_:b1");
    }

    #[test]
    fn test_blank_node_trailing_dot() {
        // #1444: a blank-node label must not end in '.', so the dot is the
        // statement terminator, not a lexical error: `_:o6.` → label + Dot.
        assert_eq!(
            tok("_:o6."),
            vec![TokenKind::BlankNodeLabel, TokenKind::Dot]
        );
        let spans = tok_spans("_:o6.");
        assert_eq!(spans[0].1, "_:o6");
        assert_eq!(spans[1].1, ".");

        // The space form is unchanged.
        assert_eq!(
            tok("_:o6 ."),
            vec![TokenKind::BlankNodeLabel, TokenKind::Dot]
        );
        let spans = tok_spans("_:o6 .");
        assert_eq!(spans[0].1, "_:o6");
        assert_eq!(spans[1].1, ".");
    }

    #[test]
    fn test_blank_node_interior_dots_unchanged() {
        // Interior dots — including consecutive ones — are valid label
        // characters and must keep lexing exactly as before the trailing-dot
        // rewind (ROADMAP §1.1-9 byte-identity requirement).
        let spans = tok_spans("_:a.b");
        assert_eq!(spans, vec![(TokenKind::BlankNodeLabel, "_:a.b")]);

        let spans = tok_spans("_:a..b");
        assert_eq!(spans, vec![(TokenKind::BlankNodeLabel, "_:a..b")]);
    }

    #[test]
    fn test_blank_node_trailing_dot_at_eof() {
        // `_:a.` with nothing after the dot: label `a`, then Dot, then Eof.
        assert_eq!(tok("_:a."), vec![TokenKind::BlankNodeLabel, TokenKind::Dot]);
        let spans = tok_spans("_:a.");
        assert_eq!(spans[0].1, "_:a");
        assert_eq!(spans[1].1, ".");
    }

    #[test]
    fn test_blank_node_multiple_trailing_dots() {
        // Every trailing dot is rewound; each lexes as its own Dot token.
        assert_eq!(
            tok("_:a..."),
            vec![
                TokenKind::BlankNodeLabel,
                TokenKind::Dot,
                TokenKind::Dot,
                TokenKind::Dot
            ]
        );
        let spans = tok_spans("_:a...");
        assert_eq!(spans[0].1, "_:a");
    }

    #[test]
    fn test_nil() {
        assert_eq!(tok("()"), vec![TokenKind::Nil]);
        assert_eq!(tok("( )"), vec![TokenKind::Nil]);
    }

    #[test]
    fn test_keywords() {
        assert_eq!(tok("a"), vec![TokenKind::KwA]);
        assert_eq!(tok("true"), vec![TokenKind::KwTrue]);
        assert_eq!(tok("false"), vec![TokenKind::KwFalse]);
        assert_eq!(tok("@prefix"), vec![TokenKind::KwPrefix]);
        assert_eq!(tok("@base"), vec![TokenKind::KwBase]);
        assert_eq!(tok("PREFIX"), vec![TokenKind::KwSparqlPrefix]);
        assert_eq!(tok("BASE"), vec![TokenKind::KwSparqlBase]);
    }

    #[test]
    fn test_lang_tag() {
        assert_eq!(tok("@en"), vec![TokenKind::LangTag]);
        assert_eq!(tok("@en-US"), vec![TokenKind::LangTag]);

        // Verify span captures the full tag
        let spans = tok_spans("@en-US");
        assert_eq!(spans[0].1, "@en-US");
    }

    #[test]
    fn test_string_literal() {
        // No escapes → String (span token)
        assert_eq!(tok("\"hello\""), vec![TokenKind::String]);
        assert_eq!(tok("'hello'"), vec![TokenKind::String]);

        // With escapes → StringEscaped
        let tokens = tok("\"hello\\nworld\"");
        assert_eq!(tokens.len(), 1);
        match &tokens[0] {
            TokenKind::StringEscaped(s) => assert_eq!(s.as_ref(), "hello\nworld"),
            other => panic!("Expected StringEscaped, got {other:?}"),
        }

        // Verify span for non-escaped string
        let spans = tok_spans("\"hello\"");
        assert_eq!(spans[0].1, "\"hello\"");
    }

    #[test]
    fn test_long_string() {
        // No escapes → LongString
        assert_eq!(tok("\"\"\"hello\nworld\"\"\""), vec![TokenKind::LongString]);

        let spans = tok_spans("\"\"\"hello\nworld\"\"\"");
        assert_eq!(spans[0].1, "\"\"\"hello\nworld\"\"\"");
    }

    #[test]
    fn test_long_string_with_escapes() {
        let tokens = tok("\"\"\"hello\\nworld\"\"\"");
        assert_eq!(tokens.len(), 1);
        match &tokens[0] {
            TokenKind::StringEscaped(s) => assert_eq!(s.as_ref(), "hello\nworld"),
            other => panic!("Expected StringEscaped, got {other:?}"),
        }
    }

    #[test]
    fn test_numbers() {
        assert_eq!(tok("42"), vec![TokenKind::Integer(42)]);
        assert_eq!(tok("-42"), vec![TokenKind::Integer(-42)]);
        assert_eq!(tok("3.14"), vec![TokenKind::Decimal]);
        assert_eq!(tok("1e10"), vec![TokenKind::Double(1e10)]);

        // Verify decimal span
        let spans = tok_spans("3.14");
        assert_eq!(spans[0].1, "3.14");
    }

    #[test]
    fn test_punctuation() {
        assert_eq!(
            tok(".;,"),
            vec![TokenKind::Dot, TokenKind::Semicolon, TokenKind::Comma]
        );
        assert_eq!(tok("^^"), vec![TokenKind::DoubleCaret]);
    }

    #[test]
    fn test_comments() {
        assert_eq!(
            tok("ex:name # this is a comment\nex:value"),
            vec![TokenKind::PrefixedName, TokenKind::PrefixedName]
        );
    }

    #[test]
    fn test_simple_turtle() {
        let tokens = tok("<http://example.org/alice> <http://xmlns.com/foaf/0.1/name> \"Alice\" .");
        assert_eq!(tokens.len(), 4);
        assert!(matches!(&tokens[0], TokenKind::Iri));
        assert!(matches!(&tokens[1], TokenKind::Iri));
        assert!(matches!(&tokens[2], TokenKind::String));
        assert!(matches!(&tokens[3], TokenKind::Dot));
    }

    // =========================================================================
    // RDF 1.2 (Turtle-star) tokens
    // =========================================================================

    #[test]
    fn test_star_tokens() {
        assert_eq!(
            tok("<< >>"),
            vec![TokenKind::ReifiedTripleStart, TokenKind::ReifiedTripleEnd]
        );
        assert_eq!(tok("<<("), vec![TokenKind::TripleTermStart]);
        assert_eq!(
            tok("{| |}"),
            vec![TokenKind::AnnotationOpen, TokenKind::AnnotationClose]
        );
        assert_eq!(tok("~"), vec![TokenKind::Tilde]);
    }

    #[test]
    fn test_star_reified_triple_token_stream() {
        // `<<:a :b :c>> :q :z .` — the eval-triple-terms data-1 shape.
        assert_eq!(
            tok("<<:a :b :c>> :q :z ."),
            vec![
                TokenKind::ReifiedTripleStart,
                TokenKind::PrefixedName,
                TokenKind::PrefixedName,
                TokenKind::PrefixedName,
                TokenKind::ReifiedTripleEnd,
                TokenKind::PrefixedName,
                TokenKind::PrefixedName,
                TokenKind::Dot,
            ]
        );
        // IRI object adjacent to the closer: `<u>` then `>>`.
        assert_eq!(
            tok("<< <s> <p> <u>>> ."),
            vec![
                TokenKind::ReifiedTripleStart,
                TokenKind::Iri,
                TokenKind::Iri,
                TokenKind::Iri,
                TokenKind::ReifiedTripleEnd,
                TokenKind::Dot,
            ]
        );
    }

    #[test]
    fn test_star_annotation_and_tilde_stream() {
        assert_eq!(
            tok(":a :b :c ~ :r {| :q :z |} ."),
            vec![
                TokenKind::PrefixedName,
                TokenKind::PrefixedName,
                TokenKind::PrefixedName,
                TokenKind::Tilde,
                TokenKind::PrefixedName,
                TokenKind::AnnotationOpen,
                TokenKind::PrefixedName,
                TokenKind::PrefixedName,
                TokenKind::AnnotationClose,
                TokenKind::Dot,
            ]
        );
    }

    #[test]
    fn test_star_lone_closers_still_error() {
        // `>` and `|` alone were lex errors before star support and must stay so.
        assert!(tokenize(":a > :b").is_err());
        assert!(tokenize(":a | :b").is_err());
        // A space between the angles is NOT a star opener: `< <` keeps the
        // pre-star "invalid or unterminated IRI" behavior.
        let err = tokenize("< <a> .").unwrap_err().to_string();
        assert!(err.contains("invalid or unterminated IRI"), "{err}");
    }

    /// Non-star corpus pin: lexing star-free Turtle is UNCHANGED.
    ///
    /// This corpus exercises every pre-star token kind, including the
    /// dispatch sites the star tokens share a first byte with (`<` for
    /// IRIs, `{`/`}` for TriG braces). If a lexer change alters any span
    /// or kind here, the byte-identical guarantee for existing import
    /// data is broken.
    #[test]
    fn test_non_star_corpus_unchanged() {
        let corpus = "@prefix ex: <http://example.org/> .\n\
             @base <http://example.org/base/> .\n\
             PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n\
             BASE <http://example.org/b2/>\n\
             ex:alice a foaf:Person ;\n\
               foaf:name \"Alice\"@en , 'Alicia' ;\n\
               ex:bio \"\"\"multi\nline\"\"\" ;\n\
               ex:age 30 ;\n\
               ex:height 1.75 ;\n\
               ex:score 1e10 ;\n\
               ex:ok true ;\n\
               ex:no false ;\n\
               ex:when \"2024-01-01\"^^<http://www.w3.org/2001/XMLSchema#date> ;\n\
               ex:friends ( ex:bob _:b1 ) ;\n\
               ex:none () ;\n\
               ex:knows [ foaf:name \"Bob\" ] .\n\
             GRAPH <http://example.org/g> { ex:s ex:p ex:o . }\n";
        let kinds = tok(corpus);
        use TokenKind as T;
        let expected = vec![
            // @prefix ex: <...> .
            T::KwPrefix,
            T::PrefixedNameNs,
            T::Iri,
            T::Dot,
            // @base <...> .
            T::KwBase,
            T::Iri,
            T::Dot,
            // PREFIX foaf: <...>
            T::KwSparqlPrefix,
            T::PrefixedNameNs,
            T::Iri,
            // BASE <...>
            T::KwSparqlBase,
            T::Iri,
            // ex:alice a foaf:Person ;
            T::PrefixedName,
            T::KwA,
            T::PrefixedName,
            T::Semicolon,
            // foaf:name "Alice"@en , 'Alicia' ;
            T::PrefixedName,
            T::String,
            T::LangTag,
            T::Comma,
            T::String,
            T::Semicolon,
            // ex:bio """multi\nline""" ;
            T::PrefixedName,
            T::LongString,
            T::Semicolon,
            // ex:age 30 ;
            T::PrefixedName,
            T::Integer(30),
            T::Semicolon,
            // ex:height 1.75 ;
            T::PrefixedName,
            T::Decimal,
            T::Semicolon,
            // ex:score 1e10 ;
            T::PrefixedName,
            T::Double(1e10),
            T::Semicolon,
            // ex:ok true ;
            T::PrefixedName,
            T::KwTrue,
            T::Semicolon,
            // ex:no false ;
            T::PrefixedName,
            T::KwFalse,
            T::Semicolon,
            // ex:when "2024-01-01"^^<...> ;
            T::PrefixedName,
            T::String,
            T::DoubleCaret,
            T::Iri,
            T::Semicolon,
            // ex:friends ( ex:bob _:b1 ) ;
            T::PrefixedName,
            T::LParen,
            T::PrefixedName,
            T::BlankNodeLabel,
            T::RParen,
            T::Semicolon,
            // ex:none () ;
            T::PrefixedName,
            T::Nil,
            T::Semicolon,
            // ex:knows [ foaf:name "Bob" ] .
            T::PrefixedName,
            T::LBracket,
            T::PrefixedName,
            T::String,
            T::RBracket,
            T::Dot,
            // GRAPH <...> { ex:s ex:p ex:o . }
            T::KwGraph,
            T::Iri,
            T::LBrace,
            T::PrefixedName,
            T::PrefixedName,
            T::PrefixedName,
            T::Dot,
            T::RBrace,
        ];
        assert_eq!(kinds, expected);

        // Span sanity on the shared-first-byte token kinds: `<`-opened
        // tokens are still whole IRIs, and braces are single bytes.
        for (kind, text) in tok_spans(corpus) {
            match kind {
                TokenKind::Iri => assert!(text.starts_with('<') && text.ends_with('>')),
                TokenKind::LBrace => assert_eq!(text, "{"),
                TokenKind::RBrace => assert_eq!(text, "}"),
                _ => {}
            }
        }
    }

    #[test]
    fn test_error_unexpected_char() {
        let result = tokenize("ex:name $ ex:value");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unexpected character"));
        assert!(msg.contains("$"));
        assert!(msg.contains("line 1"));
    }

    #[test]
    fn test_error_unterminated_string() {
        let result = tokenize("ex:name \"unterminated");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line 1"));
    }

    #[test]
    fn test_error_with_line_info() {
        let result = tokenize("ex:name \"ok\" .\nex:other $ .");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line 2"));
        assert!(msg.contains("$"));
    }
}
