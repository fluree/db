//! Cypher lexer.
//!
//! Hand-written byte-oriented scanner. Cypher source is mostly ASCII;
//! string literals may contain UTF-8 which we pass through unchanged.
//! Identifiers accept ASCII letters, digits, and `_`, plus backtick-
//! quoted forms for unusual names.

use crate::span::SourceSpan;

use super::token::{Token, TokenKind};

#[derive(Debug, Clone, thiserror::Error)]
pub enum LexError {
    #[error("unterminated string literal at offset {0}")]
    UnterminatedString(usize),
    #[error("unterminated backtick identifier at offset {0}")]
    UnterminatedBacktick(usize),
    #[error("invalid escape sequence at offset {0}")]
    InvalidEscape(usize),
    #[error("invalid number at offset {0}")]
    InvalidNumber(usize),
    #[error("unexpected character {ch:?} at offset {offset}")]
    UnexpectedChar { ch: char, offset: usize },
}

impl LexError {
    pub fn span(&self) -> SourceSpan {
        let off = match self {
            LexError::UnterminatedString(o)
            | LexError::UnterminatedBacktick(o)
            | LexError::InvalidEscape(o)
            | LexError::InvalidNumber(o) => *o,
            LexError::UnexpectedChar { offset, .. } => *offset,
        };
        SourceSpan::new(off, off + 1)
    }
}

/// Tokenize Cypher source. Returns all tokens including a final `Eof`.
pub fn tokenize(src: &str) -> Result<Vec<Token>, LexError> {
    let bytes = src.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();

    while i < bytes.len() {
        let start = i;
        let b = bytes[i];
        match b {
            // ----- whitespace -----
            b' ' | b'\t' | b'\r' | b'\n' => {
                i += 1;
                continue;
            }
            // ----- comments -----
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                if i + 1 < bytes.len() {
                    i += 2;
                }
                continue;
            }
            // ----- multi-char punctuation -----
            b'<' if peek(bytes, i + 1) == Some(b'>') => {
                push(&mut out, TokenKind::NotEq, start, i + 2);
                i += 2;
            }
            b'<' if peek(bytes, i + 1) == Some(b'=') => {
                push(&mut out, TokenKind::Le, start, i + 2);
                i += 2;
            }
            b'<' if peek(bytes, i + 1) == Some(b'-') => {
                push(&mut out, TokenKind::LArrowDash, start, i + 2);
                i += 2;
            }
            b'>' if peek(bytes, i + 1) == Some(b'=') => {
                push(&mut out, TokenKind::Ge, start, i + 2);
                i += 2;
            }
            b'-' if peek(bytes, i + 1) == Some(b'>') => {
                push(&mut out, TokenKind::DashArrowRight, start, i + 2);
                i += 2;
            }
            b'+' if peek(bytes, i + 1) == Some(b'=') => {
                push(&mut out, TokenKind::PlusEq, start, i + 2);
                i += 2;
            }
            b'.' if peek(bytes, i + 1) == Some(b'.') => {
                push(&mut out, TokenKind::DotDot, start, i + 2);
                i += 2;
            }
            // ----- single-char punctuation -----
            b'(' => {
                push(&mut out, TokenKind::LParen, start, i + 1);
                i += 1;
            }
            b')' => {
                push(&mut out, TokenKind::RParen, start, i + 1);
                i += 1;
            }
            b'[' => {
                push(&mut out, TokenKind::LBracket, start, i + 1);
                i += 1;
            }
            b']' => {
                push(&mut out, TokenKind::RBracket, start, i + 1);
                i += 1;
            }
            b'{' => {
                push(&mut out, TokenKind::LBrace, start, i + 1);
                i += 1;
            }
            b'}' => {
                push(&mut out, TokenKind::RBrace, start, i + 1);
                i += 1;
            }
            b',' => {
                push(&mut out, TokenKind::Comma, start, i + 1);
                i += 1;
            }
            b';' => {
                push(&mut out, TokenKind::Semicolon, start, i + 1);
                i += 1;
            }
            b'.' => {
                push(&mut out, TokenKind::Dot, start, i + 1);
                i += 1;
            }
            b':' => {
                push(&mut out, TokenKind::Colon, start, i + 1);
                i += 1;
            }
            b'=' => {
                push(&mut out, TokenKind::Eq, start, i + 1);
                i += 1;
            }
            b'<' => {
                push(&mut out, TokenKind::Lt, start, i + 1);
                i += 1;
            }
            b'>' => {
                push(&mut out, TokenKind::Gt, start, i + 1);
                i += 1;
            }
            b'+' => {
                push(&mut out, TokenKind::Plus, start, i + 1);
                i += 1;
            }
            b'-' => {
                push(&mut out, TokenKind::Minus, start, i + 1);
                i += 1;
            }
            b'*' => {
                push(&mut out, TokenKind::Star, start, i + 1);
                i += 1;
            }
            b'/' => {
                push(&mut out, TokenKind::Slash, start, i + 1);
                i += 1;
            }
            b'%' => {
                push(&mut out, TokenKind::Percent, start, i + 1);
                i += 1;
            }
            b'^' => {
                push(&mut out, TokenKind::Caret, start, i + 1);
                i += 1;
            }
            b'|' => {
                push(&mut out, TokenKind::Pipe, start, i + 1);
                i += 1;
            }
            // ----- string literals -----
            b'"' | b'\'' => {
                let quote = b;
                let (text, end) = read_string(bytes, i, quote)?;
                push(&mut out, TokenKind::String(text), start, end);
                i = end;
            }
            // ----- parameters -----
            b'$' => {
                i += 1;
                let name_start = i;
                while i < bytes.len() && is_ident_continue(bytes[i]) {
                    i += 1;
                }
                if i == name_start {
                    return Err(LexError::UnexpectedChar {
                        ch: '$',
                        offset: start,
                    });
                }
                let name = std::str::from_utf8(&bytes[name_start..i])
                    .expect("ASCII identifier")
                    .to_string();
                push(&mut out, TokenKind::Param(name), start, i);
            }
            // ----- backtick identifiers -----
            b'`' => {
                let (text, end) = read_backtick(bytes, i)?;
                push(&mut out, TokenKind::Ident(text), start, end);
                i = end;
            }
            // ----- numbers -----
            b'0'..=b'9' => {
                let (kind, end) = read_number(bytes, i)?;
                push(&mut out, kind, start, end);
                i = end;
            }
            // ----- identifiers / keywords -----
            _ if is_ident_start(b) => {
                let name_start = i;
                while i < bytes.len() && is_ident_continue(bytes[i]) {
                    i += 1;
                }
                let raw =
                    std::str::from_utf8(&bytes[name_start..i]).expect("ASCII identifier");
                let kind = TokenKind::keyword_from_str(raw)
                    .unwrap_or_else(|| TokenKind::Ident(raw.to_string()));
                push(&mut out, kind, start, i);
            }
            _ => {
                let ch = src[start..].chars().next().unwrap_or('?');
                return Err(LexError::UnexpectedChar { ch, offset: start });
            }
        }
    }

    out.push(Token::new(
        TokenKind::Eof,
        SourceSpan::new(bytes.len(), bytes.len()),
    ));
    Ok(out)
}

fn push(out: &mut Vec<Token>, kind: TokenKind, start: usize, end: usize) {
    out.push(Token::new(kind, SourceSpan::new(start, end)));
}

fn peek(bytes: &[u8], i: usize) -> Option<u8> {
    bytes.get(i).copied()
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_continue(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn read_string(bytes: &[u8], start: usize, quote: u8) -> Result<(String, usize), LexError> {
    let mut i = start + 1;
    let mut out = String::new();
    while i < bytes.len() {
        let b = bytes[i];
        if b == quote {
            return Ok((out, i + 1));
        }
        if b == b'\\' {
            i += 1;
            if i >= bytes.len() {
                return Err(LexError::InvalidEscape(i - 1));
            }
            let esc = bytes[i];
            match esc {
                b'n' => out.push('\n'),
                b't' => out.push('\t'),
                b'r' => out.push('\r'),
                b'\\' => out.push('\\'),
                b'\'' => out.push('\''),
                b'"' => out.push('"'),
                b'`' => out.push('`'),
                b'0' => out.push('\0'),
                b'b' => out.push('\u{08}'),
                b'f' => out.push('\u{0c}'),
                b'u' => {
                    // \uXXXX
                    let hex_start = i + 1;
                    let hex_end = hex_start + 4;
                    if hex_end > bytes.len() {
                        return Err(LexError::InvalidEscape(i - 1));
                    }
                    let hex = std::str::from_utf8(&bytes[hex_start..hex_end])
                        .map_err(|_| LexError::InvalidEscape(i - 1))?;
                    let cp = u32::from_str_radix(hex, 16)
                        .map_err(|_| LexError::InvalidEscape(i - 1))?;
                    if let Some(c) = char::from_u32(cp) {
                        out.push(c);
                    } else {
                        return Err(LexError::InvalidEscape(i - 1));
                    }
                    i = hex_end - 1;
                }
                _ => return Err(LexError::InvalidEscape(i - 1)),
            }
            i += 1;
            continue;
        }
        // Pass through bytes — for non-ASCII, decode UTF-8 character.
        if b < 0x80 {
            out.push(b as char);
            i += 1;
        } else {
            let s = std::str::from_utf8(&bytes[i..])
                .map_err(|_| LexError::InvalidEscape(i))?;
            let ch = s.chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    Err(LexError::UnterminatedString(start))
}

fn read_backtick(bytes: &[u8], start: usize) -> Result<(String, usize), LexError> {
    let mut i = start + 1;
    let mut out = String::new();
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'`' {
            return Ok((out, i + 1));
        }
        if b < 0x80 {
            out.push(b as char);
            i += 1;
        } else {
            let s = std::str::from_utf8(&bytes[i..])
                .map_err(|_| LexError::UnterminatedBacktick(start))?;
            let ch = s.chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    Err(LexError::UnterminatedBacktick(start))
}

fn read_number(bytes: &[u8], start: usize) -> Result<(TokenKind, usize), LexError> {
    let mut i = start;
    // hex / octal
    if bytes[i] == b'0' && i + 1 < bytes.len() {
        match bytes[i + 1] {
            b'x' | b'X' => {
                i += 2;
                let hex_start = i;
                while i < bytes.len() && bytes[i].is_ascii_hexdigit() {
                    i += 1;
                }
                if i == hex_start {
                    return Err(LexError::InvalidNumber(start));
                }
                let raw = std::str::from_utf8(&bytes[hex_start..i])
                    .map_err(|_| LexError::InvalidNumber(start))?;
                let n = i64::from_str_radix(raw, 16)
                    .map_err(|_| LexError::InvalidNumber(start))?;
                return Ok((TokenKind::Integer(n), i));
            }
            b'o' | b'O' => {
                i += 2;
                let o_start = i;
                while i < bytes.len() && (b'0'..=b'7').contains(&bytes[i]) {
                    i += 1;
                }
                if i == o_start {
                    return Err(LexError::InvalidNumber(start));
                }
                let raw = std::str::from_utf8(&bytes[o_start..i])
                    .map_err(|_| LexError::InvalidNumber(start))?;
                let n = i64::from_str_radix(raw, 8)
                    .map_err(|_| LexError::InvalidNumber(start))?;
                return Ok((TokenKind::Integer(n), i));
            }
            _ => {}
        }
    }

    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    let mut is_float = false;
    // Decimal — but only if followed by digit; otherwise the dot is a
    // separate token (property accessor).
    if i + 1 < bytes.len() && bytes[i] == b'.' && bytes[i + 1].is_ascii_digit() {
        is_float = true;
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    }
    // Exponent
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        is_float = true;
        i += 1;
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        let exp_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_start {
            return Err(LexError::InvalidNumber(start));
        }
    }

    let raw = std::str::from_utf8(&bytes[start..i]).map_err(|_| LexError::InvalidNumber(start))?;
    if is_float {
        let f: f64 = raw.parse().map_err(|_| LexError::InvalidNumber(start))?;
        Ok((TokenKind::Float(f), i))
    } else {
        let n: i64 = raw.parse().map_err(|_| LexError::InvalidNumber(start))?;
        Ok((TokenKind::Integer(n), i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(src: &str) -> Vec<TokenKind> {
        tokenize(src)
            .unwrap()
            .into_iter()
            .map(|t| t.kind)
            .filter(|k| !matches!(k, TokenKind::Eof))
            .collect()
    }

    #[test]
    fn keywords() {
        let toks = kinds("MATCH match Match");
        assert_eq!(toks, vec![TokenKind::Match, TokenKind::Match, TokenKind::Match]);
    }

    #[test]
    fn punctuation_longest_match() {
        let toks = kinds("<> <= >= <- -> += ..");
        assert_eq!(
            toks,
            vec![
                TokenKind::NotEq,
                TokenKind::Le,
                TokenKind::Ge,
                TokenKind::LArrowDash,
                TokenKind::DashArrowRight,
                TokenKind::PlusEq,
                TokenKind::DotDot,
            ]
        );
    }

    #[test]
    fn single_char_punct() {
        let toks = kinds("()[]{},;:.=+-*/^|");
        assert_eq!(
            toks,
            vec![
                TokenKind::LParen,
                TokenKind::RParen,
                TokenKind::LBracket,
                TokenKind::RBracket,
                TokenKind::LBrace,
                TokenKind::RBrace,
                TokenKind::Comma,
                TokenKind::Semicolon,
                TokenKind::Colon,
                TokenKind::Dot,
                TokenKind::Eq,
                TokenKind::Plus,
                TokenKind::Minus,
                TokenKind::Star,
                TokenKind::Slash,
                TokenKind::Caret,
                TokenKind::Pipe,
            ]
        );
    }

    #[test]
    fn identifier_vs_keyword() {
        let toks = kinds("MATCH Person where");
        assert_eq!(
            toks,
            vec![
                TokenKind::Match,
                TokenKind::Ident("Person".to_string()),
                TokenKind::Where,
            ]
        );
    }

    #[test]
    fn backtick_identifier() {
        let toks = kinds("`weird name`");
        assert_eq!(toks, vec![TokenKind::Ident("weird name".to_string())]);
    }

    #[test]
    fn strings() {
        assert_eq!(
            kinds(r#""hello""#),
            vec![TokenKind::String("hello".to_string())]
        );
        assert_eq!(
            kinds(r#"'world'"#),
            vec![TokenKind::String("world".to_string())]
        );
        assert_eq!(
            kinds(r#""line\nbreak""#),
            vec![TokenKind::String("line\nbreak".to_string())]
        );
    }

    #[test]
    fn numbers() {
        assert_eq!(kinds("42"), vec![TokenKind::Integer(42)]);
        assert_eq!(kinds("3.14"), vec![TokenKind::Float(3.14)]);
        assert_eq!(kinds("1e3"), vec![TokenKind::Float(1000.0)]);
        assert_eq!(kinds("0x1F"), vec![TokenKind::Integer(31)]);
    }

    #[test]
    fn number_dot_is_separate() {
        // n.prop must NOT lex as floats
        let toks = kinds("n.prop");
        assert_eq!(
            toks,
            vec![
                TokenKind::Ident("n".to_string()),
                TokenKind::Dot,
                TokenKind::Ident("prop".to_string()),
            ]
        );
    }

    #[test]
    fn params() {
        assert_eq!(
            kinds("$name $0"),
            vec![
                TokenKind::Param("name".to_string()),
                TokenKind::Param("0".to_string()),
            ]
        );
    }

    #[test]
    fn line_comment() {
        let toks = kinds("MATCH // ignore\n n");
        assert_eq!(
            toks,
            vec![TokenKind::Match, TokenKind::Ident("n".to_string())]
        );
    }

    #[test]
    fn block_comment() {
        let toks = kinds("MATCH /* big\nmess */ n");
        assert_eq!(
            toks,
            vec![TokenKind::Match, TokenKind::Ident("n".to_string())]
        );
    }

    #[test]
    fn rel_arrows() {
        let toks = kinds("(a)-[r]->(b)");
        assert_eq!(
            toks,
            vec![
                TokenKind::LParen,
                TokenKind::Ident("a".to_string()),
                TokenKind::RParen,
                TokenKind::Minus,
                TokenKind::LBracket,
                TokenKind::Ident("r".to_string()),
                TokenKind::RBracket,
                TokenKind::DashArrowRight,
                TokenKind::LParen,
                TokenKind::Ident("b".to_string()),
                TokenKind::RParen,
            ]
        );
    }

    #[test]
    fn left_arrow() {
        let toks = kinds("<-[r]-");
        assert_eq!(
            toks,
            vec![
                TokenKind::LArrowDash,
                TokenKind::LBracket,
                TokenKind::Ident("r".to_string()),
                TokenKind::RBracket,
                TokenKind::Minus,
            ]
        );
    }
}
