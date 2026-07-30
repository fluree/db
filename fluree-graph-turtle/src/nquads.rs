//! Strict N-Triples / N-Quads reader.
//!
//! Deliberately NOT the Turtle parser in a narrower mood. N-Triples and
//! N-Quads are line-oriented grammars whose whole character is what they
//! *refuse*: no directives, no prefixed names, no relative IRIs, no `,`/`;`
//! lists, no `'''`/`"""` strings, no bare numeric or boolean literals, no
//! collections. Every one of those is valid Turtle, so a reader built by
//! restricting the Turtle parser cannot reject them — which is exactly why
//! the W3C negative-syntax tests for these formats were unenforceable while
//! the suites ran through it (burn-down cause E).
//!
//! So this is its own scanner, ~one statement per line, sharing only the
//! character-class predicates. It is also the shape the parallel pipeline
//! wants: statements never span lines, so a chunk boundary is any newline.
//!
//! # What the two formats share
//!
//! N-Triples is N-Quads without the optional fourth term. One scanner
//! handles both, with [`LineDialect`] deciding whether a graph label is
//! allowed — parameterizing is honest here because the grammars genuinely
//! differ by that one production, unlike Turtle-vs-N-Triples which differ
//! nearly everywhere.

use fluree_graph_ir::chars::{is_pn_chars, is_pn_chars_u, simple_escape, unicode_escape_value};
use fluree_graph_ir::{Datatype, GraphSink, TermId};
use fluree_vocab::iri::{iri_violation, IriViolation};
use fluree_vocab::lang::language_tag_violation;

use crate::error::{Result, TurtleError};

/// Which line-oriented grammar to accept.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum LineDialect {
    /// N-Triples: exactly three terms per statement.
    #[default]
    NTriples,
    /// N-Quads: three terms plus an optional graph label.
    NQuads,
}

impl LineDialect {
    fn name(self) -> &'static str {
        match self {
            LineDialect::NTriples => "N-Triples",
            LineDialect::NQuads => "N-Quads",
        }
    }
}

/// Parse an N-Triples document.
///
/// # Errors
///
/// Anything that is not N-Triples, including the Turtle constructs a
/// Turtle-based reader would have accepted.
pub fn parse_ntriples<S: GraphSink>(input: &str, sink: &mut S) -> Result<()> {
    Reader::new(input, sink, LineDialect::NTriples).run()
}

/// Parse an N-Quads document.
///
/// A statement carrying a graph label needs a quad-capable sink; against a
/// triple-only one the reader refuses rather than dropping the label.
///
/// # Errors
///
/// Anything that is not N-Quads.
pub fn parse_nquads<S: GraphSink>(input: &str, sink: &mut S) -> Result<()> {
    Reader::new(input, sink, LineDialect::NQuads).run()
}

struct Reader<'a, 'i, S> {
    input: &'i str,
    bytes: &'i [u8],
    pos: usize,
    sink: &'a mut S,
    dialect: LineDialect,
}

impl<'a, 'i, S: GraphSink> Reader<'a, 'i, S> {
    fn new(input: &'i str, sink: &'a mut S, dialect: LineDialect) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
            sink,
            dialect,
        }
    }

    fn err(&self, message: impl Into<String>) -> TurtleError {
        TurtleError::parse(self.pos, message.into())
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    /// Whitespace WITHIN a statement: spaces and tabs only. A newline ends a
    /// statement, so it is never skipped here — that is the property that
    /// makes the format chunkable at any line boundary.
    fn skip_inline_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t')) {
            self.pos += 1;
        }
    }

    /// Whitespace BETWEEN statements, including newlines and comments.
    fn skip_between_statements(&mut self) {
        loop {
            match self.peek() {
                Some(b' ' | b'\t' | b'\r' | b'\n') => self.pos += 1,
                Some(b'#') => {
                    while !matches!(self.peek(), None | Some(b'\n')) {
                        self.pos += 1;
                    }
                }
                _ => return,
            }
        }
    }

    fn run(mut self) -> Result<()> {
        loop {
            self.skip_between_statements();
            if self.peek().is_none() {
                return Ok(());
            }
            self.statement()?;
        }
    }

    fn statement(&mut self) -> Result<()> {
        let subject = self.subject()?;
        self.skip_inline_ws();
        let predicate = self.predicate()?;
        self.skip_inline_ws();
        let object = self.object()?;
        self.skip_inline_ws();

        let graph = if self.dialect == LineDialect::NQuads && self.peek() != Some(b'.') {
            let g = self.graph_label()?;
            self.skip_inline_ws();
            Some(g)
        } else {
            None
        };

        if self.peek() != Some(b'.') {
            return Err(self.err(format!(
                "expected `.` to end the {} statement",
                self.dialect.name()
            )));
        }
        self.pos += 1;

        // Trailing content on the line is an error; only whitespace and a
        // comment may follow the terminator.
        self.skip_inline_ws();
        if let Some(c) = self.peek() {
            if c != b'\n' && c != b'\r' && c != b'#' {
                return Err(self.err("unexpected content after the statement terminator"));
            }
        }

        match graph {
            Some(g) => {
                if !self.sink.supports_quads() {
                    return Err(self.err(
                        "this document has named graphs but the output cannot represent \
                         them; a triple-only sink would have to drop the graph names",
                    ));
                }
                self.sink.emit_quad(subject, predicate, object, g)?;
            }
            None => self.sink.emit_triple(subject, predicate, object)?,
        }
        self.sink.end_statement();
        Ok(())
    }

    /// `subject ::= IRIREF | BLANK_NODE_LABEL`
    fn subject(&mut self) -> Result<TermId> {
        match self.peek() {
            Some(b'<') => self.iri_term(),
            Some(b'_') => self.blank_term(),
            _ => Err(self.err("expected an IRI or blank node as subject")),
        }
    }

    /// `predicate ::= IRIREF` — never a blank node, never `a`.
    fn predicate(&mut self) -> Result<TermId> {
        match self.peek() {
            Some(b'<') => self.iri_term(),
            _ => Err(self.err(
                "expected an IRI as predicate (a blank node or the `a` keyword is Turtle, not \
                 a line format)",
            )),
        }
    }

    /// `object ::= IRIREF | BLANK_NODE_LABEL | literal`
    fn object(&mut self) -> Result<TermId> {
        match self.peek() {
            Some(b'<') => self.iri_term(),
            Some(b'_') => self.blank_term(),
            Some(b'"') => self.literal_term(),
            Some(b'\'') => {
                Err(self.err("single-quoted strings are Turtle; a line format uses \" only"))
            }
            _ => Err(self.err(
                "expected an IRI, blank node, or quoted literal as object (bare numbers and \
                 booleans are Turtle, not a line format)",
            )),
        }
    }

    /// `graphLabel ::= IRIREF | BLANK_NODE_LABEL`
    fn graph_label(&mut self) -> Result<TermId> {
        match self.peek() {
            Some(b'<') => self.iri_term(),
            Some(b'_') => self.blank_term(),
            _ => Err(self.err("expected an IRI or blank node as the graph label")),
        }
    }

    /// Scan `<...>`, returning the resolved (absolute) IRI text.
    fn iri_text(&mut self) -> Result<String> {
        debug_assert_eq!(self.peek(), Some(b'<'));
        self.pos += 1;
        let start = self.pos;
        let mut out = String::new();
        let mut had_escape = false;

        loop {
            let Some(c) = self.peek() else {
                return Err(self.err("unterminated IRI"));
            };
            match c {
                b'>' => {
                    if !had_escape {
                        out.push_str(&self.input[start..self.pos]);
                    }
                    self.pos += 1;
                    break;
                }
                b'\\' => {
                    if !had_escape {
                        out.push_str(&self.input[start..self.pos]);
                        had_escape = true;
                    }
                    self.pos += 1;
                    out.push(self.unicode_escape()?);
                }
                _ => {
                    let ch = self.current_char()?;
                    // The IRIREF production excludes controls, space, and the
                    // delimiters — unescaped. This is what makes
                    // `<http://a b>` an error rather than a weird IRI.
                    if (ch as u32) <= 0x20
                        || matches!(ch, '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`')
                    {
                        return Err(
                            self.err(format!("character {ch:?} must be escaped inside an IRI"))
                        );
                    }
                    if had_escape {
                        out.push(ch);
                    }
                    self.pos += ch.len_utf8();
                }
            }
        }

        // One rule, two readers. The Turtle parser runs `iri_violation` over
        // every resolved IRI under `ParserOptions::validate`; this reader runs
        // the same predicate unconditionally, because the line grammars have
        // no base and no ingest fast path to protect — an N-Triples document
        // is either a set of RDF terms or it is not one.
        //
        // It has to happen HERE, after the loop, rather than in the loop's
        // character check above: ` ` is legal source that expands to a
        // space, so the byte scan cannot see it and only the expanded string
        // can be judged. That is the gap this closes.
        if let Some(violation) = iri_violation(&out) {
            return Err(self.err(match violation {
                IriViolation::NotAbsolute => format!(
                    "`{out}` is a relative IRI; {} has no base, so every IRI must be absolute",
                    self.dialect.name()
                ),
                other => format!("`{out}` is not an IRI: {other}"),
            }));
        }
        Ok(out)
    }

    fn iri_term(&mut self) -> Result<TermId> {
        let iri = self.iri_text()?;
        Ok(self.sink.term_iri(&iri))
    }

    fn current_char(&self) -> Result<char> {
        self.input[self.pos..]
            .chars()
            .next()
            .ok_or_else(|| self.err("unexpected end of input"))
    }

    /// `\uXXXX` / `\UXXXXXXXX`, positioned on the `u`/`U`.
    fn unicode_escape(&mut self) -> Result<char> {
        let width = match self.peek() {
            Some(b'u') => 4,
            Some(b'U') => 8,
            _ => return Err(self.err("only \\u and \\U escapes are allowed inside an IRI")),
        };
        self.pos += 1;
        let start = self.pos;
        let end = start + width;

        // Validate the window as BYTES before slicing it as a string. Hex
        // digits are ASCII, so proving all `width` bytes are hex digits also
        // proves `end` is a char boundary — which is what stops a multi-byte
        // character straddling the window from panicking the process.
        // Checking only the LENGTH, as this did, is not enough: `"\u0ee"`
        // with two-byte `e`s has the bytes and splits one of them in half.
        if end > self.bytes.len() || !self.bytes[start..end].iter().all(u8::is_ascii_hexdigit) {
            return Err(self.err(format!(
                "a \\{} escape needs exactly {width} hex digits",
                if width == 4 { 'u' } else { 'U' }
            )));
        }

        let hex = &self.input[start..end];
        let ch = unicode_escape_value(hex)
            .ok_or_else(|| self.err(format!("\\u{hex} is not a Unicode scalar value")))?;
        self.pos = end;
        Ok(ch)
    }

    /// `BLANK_NODE_LABEL ::= '_:' (PN_CHARS_U | [0-9]) ((PN_CHARS | '.')* PN_CHARS)?`
    fn blank_term(&mut self) -> Result<TermId> {
        if !self.input[self.pos..].starts_with("_:") {
            return Err(self.err("expected a blank node label (`_:`)"));
        }
        self.pos += 2;

        let start = self.pos;
        let first = self.current_char()?;
        if !(is_pn_chars_u(first) || first.is_ascii_digit()) {
            return Err(self.err("a blank node label must start with a letter, `_`, or a digit"));
        }
        self.pos += first.len_utf8();

        while let Ok(ch) = self.current_char() {
            if is_pn_chars(ch) || ch == '.' {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }

        // A label may not END in a dot: that dot is the statement terminator.
        let mut end = self.pos;
        while end > start && self.bytes[end - 1] == b'.' {
            end -= 1;
        }
        self.pos = end;

        let label = &self.input[start..end];
        if label.is_empty() {
            return Err(self.err("empty blank node label"));
        }
        Ok(self.sink.term_blank(Some(label)))
    }

    /// `literal ::= STRING_LITERAL_QUOTE ('^^' IRIREF | LANGTAG)?`
    fn literal_term(&mut self) -> Result<TermId> {
        let value = self.quoted_string()?;

        if self.peek() == Some(b'@') {
            self.pos += 1;
            let tag = self.langtag()?;
            return Ok(self
                .sink
                .term_literal(&value, Datatype::rdf_lang_string(), Some(&tag)));
        }

        if self.input[self.pos..].starts_with("^^") {
            self.pos += 2;
            if self.peek() != Some(b'<') {
                return Err(self.err("a datatype must be an absolute IRI in `<>`"));
            }
            let dt = self.iri_text()?;
            return Ok(self.sink.term_literal(&value, Datatype::from_iri(dt), None));
        }

        Ok(self.sink.term_literal(&value, Datatype::xsd_string(), None))
    }

    /// A double-quoted string on ONE line; the line formats have no long
    /// string form, and a raw newline inside quotes is an error.
    fn quoted_string(&mut self) -> Result<String> {
        debug_assert_eq!(self.peek(), Some(b'"'));
        if self.input[self.pos..].starts_with("\"\"\"") {
            return Err(self.err("triple-quoted strings are Turtle; a line format uses \" only"));
        }
        self.pos += 1;

        let mut out = String::new();
        loop {
            let Some(c) = self.peek() else {
                return Err(self.err("unterminated string"));
            };
            match c {
                b'"' => {
                    self.pos += 1;
                    return Ok(out);
                }
                b'\n' | b'\r' => {
                    return Err(self.err("a raw newline inside a string is not allowed"));
                }
                b'\\' => {
                    self.pos += 1;
                    let Some(esc) = self.peek() else {
                        return Err(self.err("truncated escape"));
                    };
                    if matches!(esc, b'u' | b'U') {
                        let ch = self.unicode_escape()?;
                        out.push(ch);
                        continue;
                    }
                    // ECHAR is one grammar row shared by every RDF text
                    // syntax; the table lives in fluree-graph-ir so this
                    // reader and the Turtle lexer cannot drift apart.
                    let Some(ch) = simple_escape(esc as char) else {
                        return Err(
                            self.err(format!("`\\{}` is not a valid string escape", esc as char))
                        );
                    };
                    self.pos += 1;
                    out.push(ch);
                }
                _ => {
                    let ch = self.current_char()?;
                    out.push(ch);
                    self.pos += ch.len_utf8();
                }
            }
        }
    }

    /// `LANGTAG ::= '@' [a-zA-Z]+ ('-' [a-zA-Z0-9]+)*`
    ///
    /// The scanner DELIMITS and the shared predicate DECIDES. Taking a maximal
    /// run of the tag alphabet and then judging it with
    /// `language_tag_violation` is what keeps this reader and the Turtle
    /// parser on one rule — a hand-rolled acceptance loop here was a second
    /// implementation of the same production, and two implementations of one
    /// grammar drift.
    ///
    /// The run is a superset of `LANGTAG`, so every ill-formed tag reaches the
    /// predicate rather than being cut short by the scan and reported as a
    /// different error. `-` and the alphanumerics are the whole alphabet, and
    /// none of `.`, `<`, `"` or whitespace is in it, so the delimiter is
    /// exactly where the tag ends.
    fn langtag(&mut self) -> Result<String> {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_alphanumeric() || c == b'-') {
            self.pos += 1;
        }
        let tag = &self.input[start..self.pos];
        if let Some(violation) = language_tag_violation(tag) {
            return Err(self.err(format!("`{tag}` is not a language tag: {violation}")));
        }
        Ok(tag.to_string())
    }
}
