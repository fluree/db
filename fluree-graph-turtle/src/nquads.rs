//! Strict N-Triples and N-Quads reader.
//!
//! Not the Turtle parser in a narrower mood: the line formats are defined by
//! what they refuse — directives, prefixed names, relative IRIs, `,`/`;`
//! lists, long and single-quoted strings, bare numbers and booleans,
//! collections, reified triples and annotations — and every one of those is
//! valid Turtle, so a reader built on the Turtle grammar accepts them. This
//! is its own scanner, one statement per line, sharing the Turtle lexer's
//! character classes and the language-tag rule.
//!
//! N-Triples is N-Quads without the optional graph label, so one scanner
//! reads both. RDF 1.2 adds triple terms (`<<( s p o )>>`, nestable in
//! object position) and base directions (`@en--ltr`).

use fluree_graph_ir::syntax::is_lang_tag_body;
use fluree_graph_ir::{Datatype, GraphSink, TermId};
use fluree_vocab::iri::is_absolute_iri;
use fluree_vocab::rdf;

use crate::error::{Result, TurtleError};
use crate::lex::chars::{is_pn_chars, is_pn_chars_u};

/// Which line format to read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LineFormat {
    NTriples,
    NQuads,
}

impl LineFormat {
    fn name(self) -> &'static str {
        match self {
            LineFormat::NTriples => "N-Triples",
            LineFormat::NQuads => "N-Quads",
        }
    }
}

/// Parse an N-Triples document into GraphSink events. Literals keep their
/// lexical form.
pub fn parse_ntriples<S: GraphSink>(input: &str, sink: &mut S) -> Result<()> {
    Reader::new(input, sink, LineFormat::NTriples)?.run()
}

/// Parse an N-Quads document into GraphSink events. A statement with a
/// graph label needs a sink that
/// [supports quads](GraphSink::supports_quads); against one that does not,
/// the parse fails rather than drop the label.
pub fn parse_nquads<S: GraphSink>(input: &str, sink: &mut S) -> Result<()> {
    Reader::new(input, sink, LineFormat::NQuads)?.run()
}

struct Reader<'a, 'i, S> {
    input: &'i str,
    bytes: &'i [u8],
    pos: usize,
    sink: &'a mut S,
    format: LineFormat,
}

impl<'a, 'i, S: GraphSink> Reader<'a, 'i, S> {
    fn new(input: &'i str, sink: &'a mut S, format: LineFormat) -> Result<Self> {
        crate::error::check_input_len(input.len())?;
        Ok(Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
            sink,
            format,
        })
    }

    fn err(&self, message: impl Into<String>) -> TurtleError {
        TurtleError::parse(self.pos, message)
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn rest(&self) -> &'i str {
        &self.input[self.pos..]
    }

    fn current_char(&self) -> Result<char> {
        self.rest()
            .chars()
            .next()
            .ok_or_else(|| self.err("unexpected end of input"))
    }

    /// Spaces and tabs within a statement. A newline ends a statement, so it
    /// is never skipped here.
    fn skip_inline_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t')) {
            self.pos += 1;
        }
    }

    /// Whitespace, newlines and comments between statements.
    fn skip_between_statements(&mut self) {
        loop {
            match self.peek() {
                Some(b' ' | b'\t' | b'\r' | b'\n') => self.pos += 1,
                Some(b'#') => {
                    while !matches!(self.peek(), None | Some(b'\n' | b'\r')) {
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

    /// `subject predicate object graphLabel? '.'`, then only whitespace or a
    /// comment to the end of the line.
    fn statement(&mut self) -> Result<()> {
        let subject = self.node("subject")?;
        self.skip_inline_ws();
        let predicate = self.predicate()?;
        self.skip_inline_ws();
        let object = self.object()?;
        self.skip_inline_ws();
        let graph = if self.format == LineFormat::NQuads && self.peek() != Some(b'.') {
            let graph = self.node("graph label")?;
            self.skip_inline_ws();
            Some(graph)
        } else {
            None
        };
        if self.peek() != Some(b'.') {
            return Err(self.err(format!(
                "expected '.' to end the {} statement",
                self.format.name()
            )));
        }
        self.pos += 1;
        self.skip_inline_ws();
        if !matches!(self.peek(), None | Some(b'\n' | b'\r' | b'#')) {
            return Err(self.err(format!(
                "a {} statement ends its line; only a comment may follow the '.'",
                self.format.name()
            )));
        }
        match graph {
            None => self.sink.emit_triple(subject, predicate, object)?,
            Some(_) if !self.sink.supports_quads() => {
                return Err(self.err(
                    "the document has a named graph, which this parse's output cannot \
                     represent without dropping the graph's name",
                ))
            }
            Some(graph) => self.sink.emit_quad(subject, predicate, object, graph)?,
        }
        self.sink.end_statement();
        Ok(())
    }

    /// An IRI or a blank node: a subject, a triple term's subject, or a
    /// graph label.
    fn node(&mut self, position: &str) -> Result<TermId> {
        match self.peek() {
            Some(b'<') if self.rest().starts_with("<<") => Err(self.err(format!(
                "a {position} is an IRI or a blank node; triple terms appear only as \
                 objects, and reified triples (`<< … >>`) are Turtle"
            ))),
            Some(b'<') => self.iri_term(),
            Some(b'_') => self.blank_term(),
            _ => Err(self.err(format!("expected an IRI or a blank node as the {position}"))),
        }
    }

    /// An IRI: never a blank node, a literal, or Turtle's `a`.
    fn predicate(&mut self) -> Result<TermId> {
        match self.peek() {
            Some(b'<') if !self.rest().starts_with("<<") => self.iri_term(),
            _ => Err(self.err("expected an IRI as the predicate")),
        }
    }

    /// An IRI, a blank node, a literal, or a triple term.
    fn object(&mut self) -> Result<TermId> {
        match self.peek() {
            Some(b'<') if self.rest().starts_with("<<(") => self.triple_term(),
            Some(b'<') if self.rest().starts_with("<<") => Err(self.err(
                "reified triples (`<< … >>`) are Turtle; a line format writes \
                              a triple term as `<<( s p o )>>`",
            )),
            Some(b'<') => self.iri_term(),
            Some(b'_') => self.blank_term(),
            Some(b'"') => self.literal_term(),
            _ => Err(self.err(
                "expected an IRI, a blank node, a quoted literal or a triple term as the \
                 object (bare numbers, booleans and single quotes are Turtle)",
            )),
        }
    }

    /// `<<( subject predicate object )>>`, its object possibly another one.
    fn triple_term(&mut self) -> Result<TermId> {
        if !self.sink.supports_triple_terms() {
            return Err(self.err("this parse's output cannot represent triple terms"));
        }
        self.pos += 3; // `<<(`
        self.skip_inline_ws();
        let subject = self.node("triple term's subject")?;
        self.skip_inline_ws();
        let predicate = self.predicate()?;
        self.skip_inline_ws();
        let object = self.object()?;
        self.skip_inline_ws();
        if !self.rest().starts_with(")>>") {
            return Err(self.err("expected ')>>' to close the triple term"));
        }
        self.pos += 3;
        Ok(self.sink.term_triple(subject, predicate, object)?)
    }

    fn iri_term(&mut self) -> Result<TermId> {
        let iri = self.iri_text()?;
        Ok(self.sink.term_iri(&iri))
    }

    /// `<…>`: an absolute IRI, whose only escapes are `\u` and `\U`, and in
    /// which neither the source nor an escape may produce a character the
    /// IRIREF production excludes.
    fn iri_text(&mut self) -> Result<String> {
        let start = self.pos;
        self.pos += 1; // `<`
        let mut iri = String::new();
        loop {
            let at = self.pos;
            let ch = match self.peek() {
                None => return Err(self.err("unterminated IRI")),
                Some(b'>') => {
                    self.pos += 1;
                    break;
                }
                Some(b'\\') => {
                    self.pos += 1;
                    if !matches!(self.peek(), Some(b'u' | b'U')) {
                        return Err(self.err("only \\u and \\U escapes are allowed in an IRI"));
                    }
                    self.unicode_escape()?
                }
                Some(_) => {
                    let ch = self.current_char()?;
                    self.pos += ch.len_utf8();
                    ch
                }
            };
            if ch <= ' ' || matches!(ch, '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\') {
                return Err(TurtleError::parse(
                    at,
                    format!("{ch:?} cannot appear in an IRI"),
                ));
            }
            iri.push(ch);
        }
        if !is_absolute_iri(&iri) {
            return Err(TurtleError::parse(
                start,
                format!(
                    "<{iri}> is a relative IRI; {} has no base, so every IRI is absolute",
                    self.format.name()
                ),
            ));
        }
        Ok(iri)
    }

    /// `\uXXXX` or `\UXXXXXXXX`, positioned on the `u` or `U`.
    fn unicode_escape(&mut self) -> Result<char> {
        let width = if self.peek() == Some(b'u') { 4 } else { 8 };
        let start = self.pos + 1;
        let end = start + width;
        // Checked as bytes before slicing: hex digits are ASCII, so this also
        // proves `end` falls on a character boundary.
        let digits = self.bytes.get(start..end);
        if !digits.is_some_and(|d| d.iter().all(u8::is_ascii_hexdigit)) {
            return Err(self.err(format!("a \\u escape needs {width} hex digits")));
        }
        let code = u32::from_str_radix(&self.input[start..end], 16)
            .map_err(|_| self.err("invalid \\u escape"))?;
        let ch = char::from_u32(code)
            .ok_or_else(|| self.err(format!("\\u{code:X} is not a Unicode scalar value")))?;
        self.pos = end;
        Ok(ch)
    }

    /// `_:label`. A label may contain `.` but not end with one: a trailing
    /// dot is the statement's terminator.
    fn blank_term(&mut self) -> Result<TermId> {
        if !self.rest().starts_with("_:") {
            return Err(self.err("expected a blank node label (`_:…`)"));
        }
        self.pos += 2;
        let start = self.pos;
        let first = self.current_char()?;
        if !(is_pn_chars_u(first) || first.is_ascii_digit()) {
            return Err(self.err("a blank node label starts with a letter, a digit or '_'"));
        }
        self.pos += first.len_utf8();
        while let Ok(ch) = self.current_char() {
            if !(is_pn_chars(ch) || ch == '.') {
                break;
            }
            self.pos += ch.len_utf8();
        }
        while self.pos > start && self.bytes[self.pos - 1] == b'.' {
            self.pos -= 1;
        }
        let label = &self.input[start..self.pos];
        Ok(self.sink.term_blank(Some(label)))
    }

    /// `"…"`, then `^^<datatype>` or `@tag` (with an optional `--ltr` or
    /// `--rtl` base direction).
    fn literal_term(&mut self) -> Result<TermId> {
        let value = self.quoted_string()?;
        // Whitespace may separate the string from its tag or datatype, as it
        // may any two terminals.
        let after_string = self.pos;
        self.skip_inline_ws();
        if self.peek() == Some(b'@') {
            self.pos += 1;
            let tag = self.language_tag()?;
            return Ok(self
                .sink
                .term_literal(&value, Datatype::rdf_lang_string(), Some(tag)));
        }
        if self.rest().starts_with("^^") {
            self.pos += 2;
            self.skip_inline_ws();
            if self.peek() != Some(b'<') {
                return Err(self.err("a datatype is an IRI in '<…>'"));
            }
            let datatype = self.iri_text()?;
            if datatype == rdf::LANG_STRING || datatype == rdf::DIR_LANG_STRING {
                return Err(self.err(format!(
                    "<{datatype}> is not written as a datatype; a language-tagged string \
                     takes its tag with '@'"
                )));
            }
            return Ok(self
                .sink
                .term_literal(&value, Datatype::from_iri(&datatype), None));
        }
        self.pos = after_string;
        Ok(self.sink.term_literal(&value, Datatype::xsd_string(), None))
    }

    /// A double-quoted string on one line, with `ECHAR` and `UCHAR` escapes.
    fn quoted_string(&mut self) -> Result<String> {
        if self.rest().starts_with("\"\"\"") {
            return Err(self.err("long strings (`\"\"\"…\"\"\"`) are Turtle"));
        }
        self.pos += 1; // `"`
        let mut value = String::new();
        loop {
            match self.peek() {
                None => return Err(self.err("unterminated string")),
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(value);
                }
                Some(b'\n' | b'\r') => {
                    return Err(self.err("a string cannot contain a raw line break"))
                }
                Some(b'\\') => {
                    self.pos += 1;
                    let escaped = match self.peek() {
                        Some(b'u' | b'U') => self.unicode_escape()?,
                        Some(c) => {
                            let ch = match c {
                                b't' => '\t',
                                b'b' => '\u{8}',
                                b'n' => '\n',
                                b'r' => '\r',
                                b'f' => '\u{c}',
                                b'"' => '"',
                                b'\'' => '\'',
                                b'\\' => '\\',
                                _ => {
                                    return Err(
                                        self.err(format!("\\{} is not a string escape", c as char))
                                    )
                                }
                            };
                            self.pos += 1;
                            ch
                        }
                        None => return Err(self.err("unterminated string")),
                    };
                    value.push(escaped);
                }
                Some(_) => {
                    let ch = self.current_char()?;
                    self.pos += ch.len_utf8();
                    value.push(ch);
                }
            }
        }
    }

    /// `[a-zA-Z]+ ('-' [a-zA-Z0-9]+)*`, every subtag at most 8 characters as
    /// BCP 47 requires, then an optional `--ltr` or `--rtl`.
    fn language_tag(&mut self) -> Result<&'i str> {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_alphanumeric() || c == b'-') {
            self.pos += 1;
        }
        let word = &self.input[start..self.pos];
        let (tag, direction) = match word.split_once("--") {
            Some((tag, direction)) => (tag, Some(direction)),
            None => (word, None),
        };
        if !is_lang_tag_body(tag) || tag.split('-').any(|subtag| subtag.len() > 8) {
            return Err(self.err(format!(
                "`@{word}` is not a language tag: subtags of at most 8 letters and digits, \
                 the first all letters (for example `@en` or `@en-GB`)"
            )));
        }
        if direction.is_some_and(|d| d != "ltr" && d != "rtl") {
            return Err(self.err(format!(
                "`@{word}` has an invalid base direction: it is `--ltr` or `--rtl`"
            )));
        }
        Ok(word)
    }
}
