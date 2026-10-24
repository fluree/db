//! N-Triples, N-Quads, Turtle and TriG writers.
//!
//! Both write a whole [`Graph`] into one `String`, reserving an estimate up
//! front and writing each term straight into it: no per-term allocation, and
//! numbers are formatted in place. Pass a sorted graph (`Graph::canonicalize`
//! also removes duplicates): N-Triples output is then deterministic, and Turtle
//! can group each subject's triples into one block.
//!
//! Blank nodes are written with their own labels, so a stored `_:fdb-…` node
//! keeps the identifier that writes resolve back to it. An IRI term whose text
//! starts with `_:` is a blank node too: that is how Fluree hands back stored
//! blank nodes. A label that is not a valid `BLANK_NODE_LABEL` (ids minted by
//! old imports contain `/` and `:`) is hex-encoded behind an `x` so the
//! document still parses.
//!
//! A language tag has no escape form, so a graph holding one that is not a
//! valid `LANGTAG` is refused rather than written: `"hi"@en . <s> <p> <o>`
//! would read back as a second triple.
//!
//! RDF 1.2 reifier attachments ([`Graph::reifications`]) are written as
//! `o ~ r` in Turtle and TriG, and as `r rdf:reifies <<( s p o )>>` lines in
//! N-Triples and N-Quads (the forms export uses).
//!
//! Turtle and TriG write a well-formed RDF collection as `( … )`; see
//! [`Collections`].

use crate::collections::{blank_nodes_in_several_graphs, Collections};
use crate::PrefixMap;
use fluree_graph_ir::datatype::iri as dt_iri;
use fluree_graph_ir::{
    push_canonical_xsd_double, syntax, Dataset, Datatype, Graph, LiteralValue, Term, Triple,
};
use std::collections::HashSet;
use std::fmt::Write as _;

/// A literal's language tag is not a valid `LANGTAG`, so it cannot be written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidLangTag(pub String);

impl std::fmt::Display for InvalidLangTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "language tag {:?} cannot be written as RDF text", self.0)
    }
}

impl std::error::Error for InvalidLangTag {}

fn check_lang_tags(graph: &Graph) -> Result<(), InvalidLangTag> {
    let reified = graph.reifications().iter().map(|r| &r.triple);
    for t in graph.iter().chain(reified) {
        if let Term::Literal {
            language: Some(lang),
            ..
        } = t.object()
        {
            if !syntax::is_lang_tag(lang) {
                return Err(InvalidLangTag(lang.to_string()));
            }
        }
    }
    Ok(())
}

/// Rough bytes per triple, for the up-front reservation.
const BYTES_PER_TRIPLE: usize = 96;

/// Write `graph` as N-Triples: one `s p o .` line per triple, full IRIs, then
/// one `r rdf:reifies <<( s p o )>> .` line per reifier attachment.
pub fn format_ntriples(graph: &Graph) -> Result<String, InvalidLangTag> {
    check_lang_tags(graph)?;
    let mut out = String::with_capacity(graph.len() * BYTES_PER_TRIPLE);
    push_nt_graph(&mut out, graph, None);
    Ok(out)
}

/// Write `dataset` as N-Quads: N-Triples lines, with the graph name as a
/// fourth term on each named graph's lines.
pub fn format_nquads(dataset: &Dataset) -> Result<String, InvalidLangTag> {
    for (_, graph) in dataset.graphs() {
        check_lang_tags(graph)?;
    }
    let mut out = String::with_capacity(dataset.len() * BYTES_PER_TRIPLE);
    for (name, graph) in dataset.graphs() {
        push_nt_graph(&mut out, graph, name);
    }
    Ok(out)
}

fn push_nt_graph(out: &mut String, graph: &Graph, name: Option<&Term>) {
    let end_line = |out: &mut String| {
        if let Some(name) = name {
            out.push(' ');
            push_nt_term(out, name);
        }
        out.push_str(" .\n");
    };
    for t in graph.iter() {
        push_nt_triple(out, t);
        end_line(out);
    }
    for r in graph.reifications() {
        push_nt_term(out, &r.reifier);
        out.push(' ');
        syntax::push_iri_ref(out, dt_iri::RDF_REIFIES);
        out.push_str(" <<( ");
        push_nt_triple(out, &r.triple);
        out.push_str(" )>>");
        end_line(out);
    }
}

fn push_nt_triple(out: &mut String, t: &Triple) {
    push_nt_term(out, t.subject());
    out.push(' ');
    push_nt_term(out, t.predicate());
    out.push(' ');
    push_nt_term(out, t.object());
}

/// Write `graph` as Turtle: `@prefix` declarations from `prefixes`, then one
/// block per subject, with `;` between predicates, `,` between the objects of
/// one predicate, `a` for `rdf:type` (listed first), prefixed names and
/// numeric/boolean shorthands wherever they read back to the same term, and
/// an object's reifiers as `~ r` after it.
pub fn format_turtle(graph: &Graph, prefixes: &PrefixMap) -> Result<String, InvalidLangTag> {
    check_lang_tags(graph)?;
    let mut out = String::with_capacity(graph.len() * BYTES_PER_TRIPLE / 2);
    prefixes.push_declarations(&mut out);
    push_turtle_graph(&mut out, graph, prefixes, &HashSet::new(), "");
    Ok(out)
}

/// Write `dataset` as TriG: the default graph as top-level Turtle, then a
/// `GRAPH <name> { ... }` block per named graph.
pub fn format_trig(dataset: &Dataset, prefixes: &PrefixMap) -> Result<String, InvalidLangTag> {
    for (_, graph) in dataset.graphs() {
        check_lang_tags(graph)?;
    }
    let mut out = String::with_capacity(dataset.len() * BYTES_PER_TRIPLE / 2);
    prefixes.push_declarations(&mut out);
    let shared = blank_nodes_in_several_graphs(dataset);
    push_turtle_graph(&mut out, &dataset.default, prefixes, &shared, "");
    for (name, graph) in &dataset.named {
        if !out.is_empty() && !out.ends_with("\n\n") {
            out.push('\n');
        }
        out.push_str("GRAPH ");
        push_turtle_term(&mut out, name, prefixes);
        out.push_str(" {\n");
        push_turtle_graph(&mut out, graph, prefixes, &shared, "    ");
        out.push_str("}\n");
    }
    Ok(out)
}

impl Collections<'_> {
    /// Write `term`, spelling a collection head as `( … )`.
    fn push_term(&self, out: &mut String, term: &Term, prefixes: &PrefixMap) {
        match self.heads.get(term) {
            Some(items) => {
                out.push('(');
                for item in items {
                    out.push(' ');
                    self.push_term(out, item, prefixes);
                }
                out.push_str(" )");
            }
            None => push_turtle_term(out, term, prefixes),
        }
    }
}

fn push_turtle_graph<'g>(
    out: &mut String,
    graph: &'g Graph,
    prefixes: &PrefixMap,
    shared: &HashSet<&'g Term>,
    indent: &str,
) {
    // Reifiers by the triple they reify; the ones whose triple the graph
    // does not hold are written as `r rdf:reifies <<( s p o )>>` at the end.
    let mut reifiers = graph.reifiers_by_triple();
    let collections = Collections::of(graph, &reifiers, shared);
    let triples = graph.triples();
    let mut start = 0;
    let mut written = 0;
    while start < triples.len() {
        let subject = triples[start].subject();
        let end = start
            + triples[start..]
                .iter()
                .position(|t| t.subject() != subject)
                .unwrap_or(triples.len() - start);
        let block = &triples[start..end];
        if collections.cells.contains(subject) {
            start = end;
            continue;
        }
        if written > 0 {
            out.push('\n');
        }
        written += 1;
        out.push_str(indent);
        push_turtle_term(out, subject, prefixes);

        let is_type = |t: &&Triple| t.predicate().as_iri() == Some(dt_iri::RDF_TYPE);
        let types = block.iter().filter(is_type);
        let rest = block.iter().filter(|t| !is_type(t));
        let mut predicate: Option<&Term> = None;
        for t in types.chain(rest) {
            if predicate == Some(t.predicate()) {
                out.push_str(", ");
            } else {
                if predicate.is_some() {
                    out.push_str(" ;\n");
                    out.push_str(indent);
                    out.push_str("    ");
                } else {
                    out.push(' ');
                }
                if t.is_rdf_type() {
                    out.push('a');
                } else {
                    push_turtle_term(out, t.predicate(), prefixes);
                }
                out.push(' ');
                predicate = Some(t.predicate());
            }
            collections.push_term(out, t.object(), prefixes);
            // `remove` hashes even on an empty map; most graphs have no reifiers.
            let rs = if reifiers.is_empty() {
                None
            } else {
                reifiers.remove(t)
            };
            if let Some(rs) = rs {
                for r in rs {
                    out.push_str(" ~ ");
                    push_turtle_term(out, r, prefixes);
                }
            }
        }
        out.push_str(" .\n");
        start = end;
    }
    let mut unasserted: Vec<(&Triple, &Term)> = reifiers
        .into_iter()
        .flat_map(|(t, rs)| rs.into_iter().map(move |r| (t, r)))
        .collect();
    unasserted.sort_unstable();
    for (t, r) in unasserted {
        if !triples.is_empty() || !out.is_empty() {
            out.push('\n');
        }
        out.push_str(indent);
        push_turtle_term(out, r, prefixes);
        out.push(' ');
        prefixes.push_iri(out, dt_iri::RDF_REIFIES);
        out.push_str(" <<( ");
        push_turtle_term(out, t.subject(), prefixes);
        out.push(' ');
        push_turtle_term(out, t.predicate(), prefixes);
        out.push(' ');
        push_turtle_term(out, t.object(), prefixes);
        out.push_str(" )>> .\n");
    }
}

/// Append one term in N-Triples syntax. The caller has checked its language
/// tag ([`check_lang_tags`]).
fn push_nt_term(out: &mut String, term: &Term) {
    match term {
        Term::Iri(iri) => match iri.strip_prefix("_:") {
            Some(label) => push_blank(out, label),
            None => syntax::push_iri_ref(out, iri),
        },
        Term::BlankNode(id) => push_blank(out, id.as_str()),
        Term::Literal {
            value,
            datatype,
            language,
        } => {
            push_quoted(out, value);
            // Tags are case-insensitive; the canonical form is lowercase.
            let language = language.as_deref().map(str::to_ascii_lowercase);
            push_literal_suffix(out, datatype, language.as_deref(), |out, iri| {
                syntax::push_iri_ref(out, iri);
            });
        }
        Term::TripleTerm(t) => {
            out.push_str("<<( ");
            for term in t.iter() {
                push_nt_term(out, term);
                out.push(' ');
            }
            out.push_str(")>>");
        }
    }
}

fn push_turtle_term(out: &mut String, term: &Term, prefixes: &PrefixMap) {
    match term {
        Term::Iri(iri) => match iri.strip_prefix("_:") {
            Some(label) => push_blank(out, label),
            None => prefixes.push_iri(out, iri),
        },
        Term::BlankNode(id) => push_blank(out, id.as_str()),
        Term::Literal {
            value,
            datatype,
            language,
        } => {
            if language.is_none() && push_shorthand(out, value, datatype) {
                return;
            }
            push_quoted(out, value);
            push_literal_suffix(out, datatype, language.as_deref(), |out, iri| {
                prefixes.push_iri(out, iri);
            });
        }
        Term::TripleTerm(t) => {
            out.push_str("<<( ");
            for term in t.iter() {
                push_turtle_term(out, term, prefixes);
                out.push(' ');
            }
            out.push_str(")>>");
        }
    }
}

/// Write a literal bare when Turtle's `INTEGER`, `DECIMAL`, `DOUBLE` or
/// `BOOLEAN` form reads back as exactly this value and datatype.
fn push_shorthand(out: &mut String, value: &LiteralValue, datatype: &Datatype) -> bool {
    let len = out.len();
    match (datatype.as_iri(), value) {
        (dt_iri::XSD_INTEGER, LiteralValue::Integer(i)) => {
            let _ = write!(out, "{i}");
            true
        }
        (dt_iri::XSD_BOOLEAN, LiteralValue::Boolean(b)) => {
            out.push_str(if *b { "true" } else { "false" });
            true
        }
        (dt_iri::XSD_DOUBLE, LiteralValue::Double(d)) => {
            push_canonical_xsd_double(out, *d);
            // NaN and INF have no bare form.
            if syntax::is_turtle_double(&out[len..]) {
                true
            } else {
                out.truncate(len);
                false
            }
        }
        (dt_iri::XSD_INTEGER, LiteralValue::String(s)) if syntax::is_turtle_integer(s) => {
            out.push_str(s);
            true
        }
        (dt_iri::XSD_DECIMAL, LiteralValue::String(s)) if syntax::is_turtle_decimal(s) => {
            out.push_str(s);
            true
        }
        (dt_iri::XSD_BOOLEAN, LiteralValue::String(s)) if matches!(&**s, "true" | "false") => {
            out.push_str(s);
            true
        }
        _ => false,
    }
}

fn push_quoted(out: &mut String, value: &LiteralValue) {
    out.push('"');
    match value {
        LiteralValue::String(s) | LiteralValue::Json(s) => syntax::push_string(out, s),
        LiteralValue::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        LiteralValue::Integer(i) => {
            let _ = write!(out, "{i}");
        }
        LiteralValue::Double(d) => push_canonical_xsd_double(out, *d),
    }
    out.push('"');
}

fn push_literal_suffix(
    out: &mut String,
    datatype: &Datatype,
    language: Option<&str>,
    push_datatype: impl FnOnce(&mut String, &str),
) {
    if let Some(lang) = language {
        out.push('@');
        out.push_str(lang);
    } else if !datatype.is_xsd_string() {
        out.push_str("^^");
        push_datatype(out, datatype.as_iri());
    }
}

fn push_blank(out: &mut String, label: &str) {
    out.push_str("_:");
    syntax::push_blank_label(out, label);
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_ir::Datatype;
    use fluree_vocab::rdf;
    use serde_json::json;

    const EX: &str = "http://example.org/";

    fn ex(local: &str) -> Term {
        Term::iri(format!("{EX}{local}"))
    }

    fn sample() -> Graph {
        let mut g = Graph::new();
        g.add_triple(ex("alice"), ex("name"), Term::string("Alice \"A\"\n"));
        g.add_triple(ex("alice"), Term::iri(dt_iri::RDF_TYPE), ex("Person"));
        g.add_triple(ex("alice"), ex("knows"), ex("bob"));
        g.add_triple(ex("alice"), ex("knows"), Term::blank("b1"));
        g.add_triple(ex("alice"), ex("age"), Term::integer(42));
        g.add_triple(ex("alice"), ex("score"), Term::double(1.5));
        g.add_triple(ex("alice"), ex("ratio"), Term::double(f64::NAN));
        g.add_triple(
            ex("alice"),
            ex("big"),
            Term::typed("12.50", Datatype::xsd_decimal()),
        );
        g.add_triple(ex("alice"), ex("n"), Term::long(7));
        g.add_triple(ex("alice"), ex("label"), Term::lang_string("salut", "fr"));
        g.add_triple(Term::iri("_:fdb-1"), ex("p"), Term::boolean(true));
        g.add_triple(
            Term::blank("old/ledger:1"),
            ex("p"),
            Term::json(r#"{"a":1}"#),
        );
        g.canonicalize();
        g
    }

    /// A tag has no escape form: a crafted one would end the literal and read
    /// back as extra triples, so the writers refuse it.
    #[test]
    fn invalid_language_tag_is_refused_not_written() {
        let tag = "en . <urn:injected> <urn:p> \"pwned\" . #";
        let tagged = Term::Literal {
            value: LiteralValue::string("hi"),
            datatype: Datatype::rdf_lang_string(),
            language: Some(tag.into()),
        };
        let refused = Err(InvalidLangTag(tag.to_string()));
        let mut g = Graph::new();
        g.add_triple(ex("alice"), ex("label"), tagged.clone());
        assert_eq!(format_ntriples(&g), refused);
        assert_eq!(format_turtle(&g, &PrefixMap::default()), refused);

        // In a named graph, and in a reified triple the graph does not hold.
        let mut d = Dataset::new();
        d.graph_mut(Some(&ex("g1")))
            .add_triple(ex("alice"), ex("label"), tagged.clone());
        assert_eq!(format_nquads(&d), refused);
        assert_eq!(format_trig(&d, &PrefixMap::default()), refused);
        let mut reified = Graph::new();
        reified.add_reification(ex("alice"), ex("label"), tagged, ex("claim"));
        assert_eq!(format_ntriples(&reified), refused);
        assert_eq!(format_turtle(&reified, &PrefixMap::default()), refused);
        // `Display` cannot fail, so it %-encodes: never parseable as a triple.
        let shown = g.iter().next().unwrap().to_string();
        assert!(!shown.contains("<urn:injected>"), "{shown}");
    }

    #[test]
    fn ntriples_lines() {
        let nt = format_ntriples(&sample()).unwrap();
        let x = "http://www.w3.org/2001/XMLSchema#";
        let expected = [
            format!("<{EX}alice> <{EX}knows> _:b1 ."),
            format!("_:fdb-1 <{EX}p> \"true\"^^<{x}boolean> ."),
            format!(
                "_:x6f6c642f6c65646765723a31 <{EX}p> \"{{\\\"a\\\":1}}\"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON> ."
            ),
            format!("<{EX}alice> <{EX}age> \"42\"^^<{x}integer> ."),
            format!("<{EX}alice> <{EX}name> \"Alice \\\"A\\\"\\n\" ."),
            format!("<{EX}alice> <{EX}label> \"salut\"@fr ."),
            format!("<{EX}alice> <{EX}ratio> \"NaN\"^^<{x}double> ."),
        ];
        for line in expected {
            assert!(nt.lines().any(|l| l == line), "missing {line}\nin:\n{nt}");
        }
        assert_eq!(nt.lines().count(), 12, "{nt}");
        assert!(nt.ends_with(" .\n"));
    }

    #[test]
    fn turtle_groups_subjects_and_uses_shorthands() {
        let prefixes = PrefixMap::from_context(&json!({
            "ex": EX,
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        }));
        let ttl = format_turtle(&sample(), &prefixes).unwrap();
        let alice = ttl
            .split("\n\n")
            .find(|block| block.starts_with("ex:alice"))
            .unwrap_or_else(|| panic!("no alice block in:\n{ttl}"));
        assert!(alice.starts_with("ex:alice a ex:Person ;\n    "), "{alice}");
        for part in [
            "ex:age 42",
            "ex:big 12.50",
            "ex:knows _:b1, ex:bob",
            "ex:label \"salut\"@fr",
            "ex:n \"7\"^^xsd:long",
            "ex:name \"Alice \\\"A\\\"\\n\"",
            "ex:ratio \"NaN\"^^xsd:double",
            "ex:score 1.5E0",
        ] {
            assert!(alice.contains(part), "missing {part:?} in:\n{alice}");
        }
        assert!(alice.trim_end().ends_with(" ."), "{alice}");
        assert!(ttl.starts_with("@prefix ex: <http://example.org/> .\n@prefix xsd:"));
        assert!(ttl.contains("_:fdb-1 ex:p true .\n"), "{ttl}");
    }

    /// Both writers' output parses back to the same triples, escapes and
    /// shorthands included.
    #[test]
    fn output_round_trips_through_the_turtle_parser() {
        let mut g = sample();
        g.add_triple(
            Term::iri("http://example.org/a b<c>"),
            ex("text"),
            Term::string("tab\t bell\u{7} quote\" back\\ é 😀 \u{85}"),
        );
        g.add_triple(
            ex("alice"),
            ex("custom"),
            Term::typed("x", Datatype::from_iri(format!("{EX}dt"))),
        );
        g.canonicalize();
        let prefixes = PrefixMap::from_context(&json!({ "ex": EX }));

        for (format, text) in [
            ("N-Triples", format_ntriples(&g).unwrap()),
            ("Turtle", format_turtle(&g, &prefixes).unwrap()),
        ] {
            let mut sink = fluree_graph_ir::GraphCollectorSink::new();
            fluree_graph_turtle::parse(&text, &mut sink)
                .unwrap_or_else(|e| panic!("{format} must parse: {e}\n{text}"));
            // Stored blank nodes come back as blank-node terms, which sort
            // apart from the `_:` IRIs they were written from: compare lines.
            let lines = |g: &Graph| {
                let mut lines: Vec<String> = format_ntriples(g)
                    .unwrap()
                    .lines()
                    .map(String::from)
                    .collect();
                lines.sort();
                lines
            };
            assert_eq!(
                lines(&sink.into_graph()),
                lines(&g),
                "{format} did not round-trip:\n{text}"
            );
        }
    }

    /// A graph with two reified edges (IRI and blank-node reifiers, one edge
    /// with both) and one reification whose triple the graph does not hold.
    fn reified() -> Graph {
        let mut g = Graph::new();
        g.add_triple(ex("alice"), ex("knows"), ex("bob"));
        g.add_triple(ex("alice"), ex("age"), Term::integer(42));
        g.add_triple(ex("claim"), ex("confidence"), Term::double(0.9));
        g.add_reification(ex("alice"), ex("knows"), ex("bob"), ex("claim"));
        g.add_reification(ex("alice"), ex("knows"), ex("bob"), Term::blank("r1"));
        g.add_reification(ex("alice"), ex("age"), Term::integer(42), ex("claim2"));
        g.add_reification(ex("carol"), ex("knows"), ex("dave"), ex("claim3"));
        g.canonicalize();
        g
    }

    #[test]
    fn turtle_writes_reifiers_after_their_object() {
        let prefixes = PrefixMap::from_context(&json!({ "ex": EX }));
        let ttl = format_turtle(&reified(), &prefixes).unwrap();
        assert!(ttl.contains("ex:knows ex:bob ~ _:r1 ~ ex:claim"), "{ttl}");
        assert!(ttl.contains("ex:age 42 ~ ex:claim2"), "{ttl}");
        // No triple to hang it on: the reification is written out whole.
        assert!(
            ttl.contains(
                "ex:claim3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> \
                 <<( ex:carol ex:knows ex:dave )>> ."
            ),
            "{ttl}"
        );
    }

    #[test]
    fn ntriples_write_reifies_lines() {
        let nt = format_ntriples(&reified()).unwrap();
        let reifies = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies>";
        for line in [
            format!("<{EX}claim> {reifies} <<( <{EX}alice> <{EX}knows> <{EX}bob> )>> ."),
            format!("_:r1 {reifies} <<( <{EX}alice> <{EX}knows> <{EX}bob> )>> ."),
            format!("<{EX}claim3> {reifies} <<( <{EX}carol> <{EX}knows> <{EX}dave> )>> ."),
        ] {
            assert!(nt.lines().any(|l| l == line), "missing {line}\nin:\n{nt}");
        }
        assert_eq!(nt.lines().count(), 3 + 4, "{nt}");
    }

    /// Both writers' reifications read back as the same attachments.
    #[test]
    fn reifications_round_trip_through_the_turtle_parser() {
        let g = reified();
        let prefixes = PrefixMap::from_context(&json!({ "ex": EX }));
        let key = |g: &Graph| {
            let mut rs: Vec<String> = g
                .reifications()
                .iter()
                .map(|r| format!("{} {}", r.reifier, r.triple))
                .collect();
            rs.sort();
            rs
        };
        for (format, text) in [
            ("N-Triples", format_ntriples(&g).unwrap()),
            ("Turtle", format_turtle(&g, &prefixes).unwrap()),
        ] {
            let mut sink = fluree_graph_ir::GraphCollectorSink::new();
            fluree_graph_turtle::parse(&text, &mut sink)
                .unwrap_or_else(|e| panic!("{format} must parse: {e}\n{text}"));
            let parsed = sink.into_graph();
            assert_eq!(key(&parsed), key(&g), "{format}:\n{text}");
        }
    }

    #[test]
    fn datasets_as_trig_and_nquads() {
        let mut d = Dataset::new();
        d.graph_mut(None)
            .add_triple(ex("a"), ex("p"), Term::string("default"));
        let g = ex("g1");
        d.graph_mut(Some(&g)).add_triple(ex("b"), ex("p"), ex("c"));
        d.graph_mut(Some(&g))
            .add_reification(ex("b"), ex("p"), ex("c"), ex("r"));
        d.canonicalize();

        let nq = format_nquads(&d).unwrap();
        assert_eq!(
            nq,
            format!(
                "<{EX}a> <{EX}p> \"default\" .\n\
                 <{EX}b> <{EX}p> <{EX}c> <{EX}g1> .\n\
                 <{EX}r> <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> \
                 <<( <{EX}b> <{EX}p> <{EX}c> )>> <{EX}g1> .\n"
            )
        );

        let prefixes = PrefixMap::from_context(&json!({ "ex": EX }));
        let trig = format_trig(&d, &prefixes).unwrap();
        assert_eq!(
            trig,
            "@prefix ex: <http://example.org/> .\n\n\
             ex:a ex:p \"default\" .\n\n\
             GRAPH ex:g1 {\n    ex:b ex:p ex:c ~ ex:r .\n}\n"
        );
        // A dataset with only a default graph is plain Turtle / N-Triples.
        let only_default = Dataset::from(reified());
        assert_eq!(
            format_trig(&only_default, &prefixes),
            format_turtle(&reified(), &prefixes)
        );
        assert_eq!(format_nquads(&only_default), format_ntriples(&reified()));
    }

    /// `holder p ( items )` as its `rdf:first`/`rdf:rest` triples, with
    /// the list's nodes labeled `{label}0`, `{label}1`, …
    fn add_list(g: &mut Graph, holder: Term, p: Term, items: &[Term], label: &str) -> Term {
        let node = |i: usize| Term::blank(format!("{label}{i}"));
        g.add_triple(holder, p, node(0));
        for (i, item) in items.iter().enumerate() {
            g.add_triple(node(i), Term::iri(rdf::FIRST), item.clone());
            let rest = if i + 1 == items.len() {
                Term::iri(rdf::NIL)
            } else {
                node(i + 1)
            };
            g.add_triple(node(i), Term::iri(rdf::REST), rest);
        }
        node(0)
    }

    /// The items of the list `holder p` holds, read back from a parsed
    /// graph's `rdf:first`/`rdf:rest` triples.
    fn read_list(g: &Graph, holder: &Term, p: &Term) -> Vec<Term> {
        let object = |s: &Term, p: &Term| {
            g.iter()
                .find(|t| t.subject() == s && t.predicate() == p)
                .map(|t| t.object().clone())
                .unwrap_or_else(|| panic!("no {p} on {s}"))
        };
        let (first, rest) = (Term::iri(rdf::FIRST), Term::iri(rdf::REST));
        let mut items = Vec::new();
        let mut node = object(holder, p);
        while node.as_iri() != Some(rdf::NIL) {
            items.push(object(&node, &first));
            node = object(&node, &rest);
        }
        items
    }

    fn parse_spine(text: &str) -> Graph {
        let mut sink = fluree_graph_ir::GraphCollectorSink::new();
        let options = fluree_graph_turtle::ParserOptions::new()
            .with_collections(fluree_graph_turtle::CollectionStyle::Spine);
        fluree_graph_turtle::parse_with_options(text, &mut sink, options)
            .unwrap_or_else(|e| panic!("must parse: {e}\n{text}"));
        sink.into_graph()
    }

    /// A well-formed collection is written as `( … )`, nested ones too, and
    /// reads back as the same list.
    #[test]
    fn turtle_writes_a_collection_as_a_list() {
        let mut g = Graph::new();
        add_list(
            &mut g,
            ex("s"),
            ex("list"),
            &[Term::integer(1), Term::integer(2)],
            "a",
        );
        let inner = Term::blank("inner");
        g.add_triple(Term::blank("outer0"), Term::iri(rdf::FIRST), inner.clone());
        g.add_triple(
            Term::blank("outer0"),
            Term::iri(rdf::REST),
            Term::blank("outer1"),
        );
        g.add_triple(
            Term::blank("outer1"),
            Term::iri(rdf::FIRST),
            Term::integer(3),
        );
        g.add_triple(
            Term::blank("outer1"),
            Term::iri(rdf::REST),
            Term::iri(rdf::NIL),
        );
        g.add_triple(ex("s"), ex("nested"), Term::blank("outer0"));
        g.add_triple(inner.clone(), Term::iri(rdf::FIRST), ex("x"));
        g.add_triple(inner, Term::iri(rdf::REST), Term::iri(rdf::NIL));
        g.canonicalize();

        let prefixes = PrefixMap::from_context(&json!({ "ex": EX }));
        let ttl = format_turtle(&g, &prefixes).unwrap();
        assert!(ttl.contains("ex:list ( 1 2 )"), "{ttl}");
        assert!(ttl.contains("ex:nested ( ( ex:x ) 3 )"), "{ttl}");
        assert!(!ttl.contains("rdf-syntax-ns#first"), "{ttl}");

        let parsed = parse_spine(&ttl);
        assert_eq!(parsed.len(), g.len(), "{ttl}");
        assert_eq!(
            read_list(&parsed, &ex("s"), &ex("list")),
            [Term::integer(1), Term::integer(2)]
        );
        let nested = read_list(&parsed, &ex("s"), &ex("nested"));
        assert_eq!(nested[1], Term::integer(3));
        let inner_head = parsed
            .iter()
            .find(|t| t.object() == &ex("x"))
            .map(|t| t.subject().clone())
            .unwrap();
        assert_eq!(nested[0], inner_head);
    }

    /// A chain that is not a well-formed list keeps its triples: `( … )`
    /// would lose what else is said about a node, or the node's label.
    #[test]
    fn turtle_keeps_the_triples_of_a_chain_that_is_not_a_list() {
        let one_two = [Term::integer(1), Term::integer(2)];
        type Change = fn(&mut Graph);
        let cases: [(&str, Change); 3] = [
            ("a node with another property", |g| {
                g.add_triple(Term::blank("a1"), ex("note"), Term::string("x"));
            }),
            ("a node used twice", |g| {
                g.add_triple(ex("t"), ex("p"), Term::blank("a1"));
            }),
            ("a chain that does not end in rdf:nil", |g| {
                let last = |t: &Triple| {
                    t.subject() == &Term::blank("a1") && t.predicate() == &Term::iri(rdf::REST)
                };
                let kept: Vec<Triple> = g.iter().filter(|t| !last(t)).cloned().collect();
                *g = Graph::new();
                for t in kept {
                    g.add_triple(
                        t.subject().clone(),
                        t.predicate().clone(),
                        t.object().clone(),
                    );
                }
                g.add_triple(Term::blank("a1"), Term::iri(rdf::REST), ex("elsewhere"));
            }),
        ];
        let prefixes = PrefixMap::from_context(&json!({ "ex": EX }));
        for (case, change) in cases {
            let mut g = Graph::new();
            add_list(&mut g, ex("s"), ex("list"), &one_two, "a");
            change(&mut g);
            g.canonicalize();
            let ttl = format_turtle(&g, &prefixes).unwrap();
            assert!(!ttl.contains('('), "{case}:\n{ttl}");
        }

        // A reified cell keeps its triples, and its annotation; the
        // well-formed tail after it still folds, as JSON-LD's list
        // conversion folds it.
        let mut g = Graph::new();
        add_list(&mut g, ex("s"), ex("list"), &one_two, "a");
        g.add_reification(
            Term::blank("a0"),
            Term::iri(rdf::FIRST),
            Term::integer(1),
            ex("r"),
        );
        g.canonicalize();
        let ttl = format_turtle(&g, &prefixes).unwrap();
        assert!(ttl.contains("ex:list _:a0"), "{ttl}");
        assert!(ttl.contains("1 ~ ex:r"), "{ttl}");

        // A stored blank node (`_:fdb-…`, an IRI term) keeps its label.
        let mut g = Graph::new();
        g.add_triple(ex("s"), ex("list"), Term::iri("_:fdb-1"));
        g.add_triple(
            Term::iri("_:fdb-1"),
            Term::iri(rdf::FIRST),
            Term::integer(1),
        );
        g.add_triple(
            Term::iri("_:fdb-1"),
            Term::iri(rdf::REST),
            Term::iri(rdf::NIL),
        );
        g.canonicalize();
        let ttl = format_turtle(&g, &prefixes).unwrap();
        assert!(ttl.contains("ex:list _:fdb-1"), "{ttl}");
    }

    /// TriG writes a list inside its graph's block, but not one whose node
    /// another graph also uses, since a blank node's label spans the document.
    #[test]
    fn trig_writes_lists_within_one_graph() {
        let one = [Term::integer(1)];
        let mut d = Dataset::new();
        add_list(d.graph_mut(Some(&ex("g1"))), ex("s"), ex("list"), &one, "a");
        add_list(
            d.graph_mut(Some(&ex("g1"))),
            ex("s"),
            ex("shared"),
            &one,
            "b",
        );
        d.graph_mut(Some(&ex("g2")))
            .add_triple(ex("t"), ex("p"), Term::blank("b0"));
        d.canonicalize();
        let prefixes = PrefixMap::from_context(&json!({ "ex": EX }));
        let trig = format_trig(&d, &prefixes).unwrap();
        assert!(trig.contains("ex:list ( 1 )"), "{trig}");
        assert!(trig.contains("ex:shared _:b0"), "{trig}");
        assert!(trig.contains("ex:p _:b0"), "{trig}");
    }

    /// `ex:s ex:items` a two-cell list whose cells are labelled `cell`.
    fn list(g: &mut Graph, cell: impl Fn(usize) -> Term) {
        let rest = |k: usize| {
            if k == 1 {
                Term::iri(dt_iri::RDF_NIL)
            } else {
                cell(k + 1)
            }
        };
        g.add_triple(ex("s"), ex("items"), cell(0));
        for k in 0..2 {
            g.add_triple(
                cell(k),
                Term::iri(dt_iri::RDF_FIRST),
                Term::integer(k as i64 + 1),
            );
            g.add_triple(cell(k), Term::iri(dt_iri::RDF_REST), rest(k));
        }
    }

    #[test]
    fn turtle_writes_a_chain_of_unnamed_cells_as_a_collection() {
        let prefixes = PrefixMap::from_context(&json!({ "ex": EX }));
        let mut g = Graph::new();
        list(&mut g, |k| Term::iri(format!("_:l0-{k}")));
        g.canonicalize();
        let ttl = format_turtle(&g, &prefixes).unwrap();
        assert_eq!(
            ttl,
            "@prefix ex: <http://example.org/> .\n\nex:s ex:items ( 1 2 ) .\n"
        );

        // The parser reads a collection back as list members.
        let mut sink = fluree_graph_ir::GraphCollectorSink::new();
        fluree_graph_turtle::parse(&ttl, &mut sink).unwrap();
        let mut members: Vec<_> = sink.into_graph().iter().map(Triple::list_index).collect();
        members.sort();
        assert_eq!(members, [Some(0), Some(1)], "{ttl}");
    }

    /// A cell a second triple names, or a stored cell's stable label, is
    /// written as triples.
    #[test]
    fn turtle_keeps_cells_something_else_names() {
        let prefixes = PrefixMap::from_context(&json!({ "ex": EX }));
        let mut named_twice = Graph::new();
        list(&mut named_twice, |k| Term::blank(format!("c{k}")));
        named_twice.add_triple(ex("t"), ex("tail"), Term::blank("c1"));
        named_twice.canonicalize();
        let ttl = format_turtle(&named_twice, &prefixes).unwrap();
        assert!(!ttl.contains('('), "{ttl}");
        assert!(ttl.contains("ex:t ex:tail _:c1 ."), "{ttl}");

        let mut stored = Graph::new();
        list(&mut stored, |k| Term::iri(format!("_:fdb-7-c{k}")));
        stored.canonicalize();
        let ttl = format_turtle(&stored, &prefixes).unwrap();
        assert!(!ttl.contains('('), "{ttl}");

        // TriG labels span graphs: a cell another graph names stays.
        let mut d = Dataset::new();
        list(d.graph_mut(None), |k| Term::blank(format!("c{k}")));
        d.graph_mut(Some(&ex("g1")))
            .add_triple(ex("t"), ex("tail"), Term::blank("c1"));
        d.canonicalize();
        let trig = format_trig(&d, &prefixes).unwrap();
        assert!(!trig.contains('('), "{trig}");
        let mut alone = Dataset::new();
        list(alone.graph_mut(None), |k| Term::blank(format!("c{k}")));
        alone
            .graph_mut(Some(&ex("g1")))
            .add_triple(ex("t"), ex("p"), ex("o"));
        alone.canonicalize();
        let trig = format_trig(&alone, &prefixes).unwrap();
        assert!(trig.contains("ex:s ex:items ( 1 2 ) ."), "{trig}");
    }

    #[test]
    fn empty_graph() {
        assert_eq!(format_ntriples(&Graph::new()).unwrap(), "");
        assert_eq!(
            format_turtle(&Graph::new(), &PrefixMap::default()).unwrap(),
            ""
        );
    }
}
