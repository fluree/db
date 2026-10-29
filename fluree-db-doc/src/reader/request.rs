//! The request an HTTP reader takes: one RDF dataset per call, whose named
//! graphs give each input its role.
//!
//! - `xt:document`: the document node (`doc:SourceDocument`, its full text as
//!   `nif:isString`) and the structure graph's elements, each with
//!   `nif:beginIndex` / `nif:endIndex` into that text and
//!   `doc:sourceDocument` naming the document;
//! - `xt:ontology`: the classes and properties to read for;
//! - `xt:instances`: the catalog, records to link mentions to;
//! - `xt:request`: optional controls, here the windows a long text is read in.
//!
//! A reader takes a bounded text per call and a bounded prompt per window,
//! so a long document is cut into [`Part`]s, each one call, and a part whose
//! text would not fit one window beside the ontology and catalog is tiled
//! into windows. No cut falls inside an element that carries offsets, so
//! every element a call names lies wholly inside its text; a container too
//! long for any single call is the one exception, and goes clipped.

use crate::vocab::{DOC_NS, NIF_NS, PO_NS, RDFS_NS, RDF_NS};
use crate::{DocError, Result};
use fluree_graph_ir::{Graph, GraphCollectorSink, Term};
use std::collections::{BTreeMap, BTreeSet};

/// The namespace the dataset's role graphs are named in.
pub const ROLE_NS: &str = "https://ns.flur.ee/extract#";

/// What one call may carry. The defaults are the reader contract's limits.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Characters of document text in one call.
    pub request_chars: usize,
    /// Windows in one call.
    pub windows: usize,
    /// Prompt tokens one window may take, ontology and catalog included.
    pub window_tokens: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            request_chars: 48_000,
            windows: 16,
            window_tokens: 200_000,
        }
    }
}

/// The reader counts tokens with its own tokenizer; this is a deliberately
/// high guess at that count, used only to decide whether a part needs
/// windows. The reader enforces the real limit, before doing any work.
///
/// A window's prompt holds the ontology, every catalog record and the
/// window's text twice (as context and as the text being read).
#[derive(Debug, Clone, Copy, Default)]
pub struct PromptSize {
    pub ontology_terms: usize,
    pub catalog_records: usize,
}

impl PromptSize {
    const PER_TERM: usize = 48;
    const PER_RECORD: usize = 64;
    const FIXED: usize = 1_024;

    fn prefix_tokens(&self) -> usize {
        Self::FIXED + self.ontology_terms * Self::PER_TERM + self.catalog_records * Self::PER_RECORD
    }

    /// The longest window text that leaves the prompt under `limit` (the
    /// text read twice, at a pessimistic three characters a token), or
    /// `None` when the ontology and catalog alone are past it (no cut of
    /// the text helps then; the reader says so).
    fn window_chars(&self, limit: usize) -> Option<usize> {
        let room = (limit * 9 / 10).checked_sub(self.prefix_tokens())?;
        let chars = room * 3 / 2;
        (chars >= 1_000).then_some(chars)
    }
}

/// One call's share of a document: characters `[begin, end)` of its text,
/// read in `windows` (offsets relative to `begin`; empty means one window).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    pub begin: usize,
    pub end: usize,
    pub windows: Vec<(usize, usize)>,
}

/// A document as the parser left it: its IRI, file name, text projection
/// and structure graph (JSON-LD, as [`crate::ParsedDocument::doco`] holds it).
pub struct Document<'a> {
    pub iri: &'a str,
    pub name: &'a str,
    pub text: &'a str,
    pub doco: &'a str,
}

/// An element with offsets: where it sits, and whether it holds other
/// elements with offsets (a leaf does not).
#[derive(Debug, Clone, Copy)]
struct Span {
    begin: usize,
    end: usize,
    leaf: bool,
}

impl Span {
    /// Whether the element belongs to `part`: it shares text with it, or,
    /// being empty, sits inside it (an empty element at a cut goes with
    /// the part that starts there).
    fn overlaps(&self, part: &Part) -> bool {
        if self.begin == self.end {
            return self.begin >= part.begin && (self.begin < part.end || part.end == part.begin);
        }
        self.begin < part.end && self.end > part.begin
    }
}

/// The document's structure graph read for cutting: the triples, and the
/// span of every element that carries offsets.
pub struct Structure {
    graph: Graph,
    spans: BTreeMap<String, Span>,
    chars: usize,
}

impl Structure {
    pub fn new(doc: &Document<'_>) -> Result<Self> {
        let graph =
            parse_jsonld(doc.doco).map_err(|e| DocError::Parse(format!("structure graph: {e}")))?;
        let begin_p = format!("{NIF_NS}beginIndex");
        let end_p = format!("{NIF_NS}endIndex");
        let contains_p = format!("{PO_NS}contains");
        let mut begins = BTreeMap::new();
        let mut ends = BTreeMap::new();
        let mut contains: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for t in graph.iter() {
            let Some(s) = t.subject().as_iri() else {
                continue;
            };
            let p = t.predicate().as_iri().unwrap_or_default();
            if p == begin_p {
                if let Some(n) = integer(t.object()) {
                    begins.insert(s.to_string(), n);
                }
            } else if p == end_p {
                if let Some(n) = integer(t.object()) {
                    ends.insert(s.to_string(), n);
                }
            } else if p == contains_p {
                if let Some(o) = t.object().as_iri() {
                    contains
                        .entry(s.to_string())
                        .or_default()
                        .push(o.to_string());
                }
            }
        }
        let chars = doc.text.chars().count();
        let mut spans = BTreeMap::new();
        for (iri, begin) in &begins {
            let Some(&end) = ends.get(iri) else { continue };
            if begin > &end || end > chars {
                return Err(DocError::Parse(format!(
                    "structure graph: {iri} spans {begin}..{end}, outside the {chars}-character text"
                )));
            }
            spans.insert(
                iri.clone(),
                Span {
                    begin: *begin,
                    end,
                    leaf: true,
                },
            );
        }
        for (parent, children) in &contains {
            if children.iter().any(|c| spans.contains_key(c)) {
                if let Some(span) = spans.get_mut(parent) {
                    span.leaf = false;
                }
            }
        }
        Ok(Self {
            graph,
            spans,
            chars,
        })
    }

    /// The parts a document is read in, and each part's windows.
    pub fn plan(&self, limits: &Limits, size: &PromptSize) -> Result<Vec<Part>> {
        let window = size.window_chars(limits.window_tokens);
        // A part never holds more windows than a call may carry.
        let part_chars = match window {
            Some(w) => limits.request_chars.min(w.saturating_mul(limits.windows)),
            None => limits.request_chars,
        };
        let mut parts = Vec::new();
        for (begin, end) in self.cut(0, self.chars, part_chars, false)? {
            let windows = match window {
                Some(w) if end - begin > w => self
                    .cut(begin, end, w, true)?
                    .into_iter()
                    .map(|(b, e)| (b - begin, e - begin))
                    .collect(),
                _ => Vec::new(),
            };
            parts.push(Part {
                begin,
                end,
                windows,
            });
        }
        Ok(parts)
    }

    /// `[from, to)` cut into runs of at most `max` characters. A cut falls
    /// only where no element is split: with `leaves_only`, no leaf (the
    /// reader's rule for windows); otherwise no element at all, falling back
    /// to leaves only when one container is longer than `max`.
    fn cut(
        &self,
        from: usize,
        to: usize,
        max: usize,
        leaves_only: bool,
    ) -> Result<Vec<(usize, usize)>> {
        let mut runs = Vec::new();
        let mut at = from;
        while to - at > max {
            let limit = at + max;
            let found = self.last_cut(at, limit, leaves_only).or_else(|| {
                (!leaves_only)
                    .then(|| self.last_cut(at, limit, true))
                    .flatten()
            });
            let Some(c) = found else {
                let leaf = self
                    .spans
                    .iter()
                    .find(|(_, s)| s.leaf && s.begin < limit && s.end > limit)
                    .map(|(iri, s)| format!("{iri} ({} characters)", s.end - s.begin))
                    .unwrap_or_else(|| "an element".to_string());
                return Err(DocError::Config(format!(
                    "{leaf} is longer than a reader call takes ({max} characters)"
                )));
            };
            runs.push((at, c));
            at = c;
        }
        runs.push((at, to));
        Ok(runs)
    }

    fn last_cut(&self, after: usize, at_most: usize, leaves_only: bool) -> Option<usize> {
        let splits = |c: usize| {
            self.spans
                .values()
                .any(|s| (!leaves_only || s.leaf) && s.begin < c && c < s.end)
        };
        // Candidates: element boundaries, which is where text changes owner.
        let candidates: BTreeSet<usize> = self
            .spans
            .values()
            .flat_map(|s| [s.begin, s.end])
            .filter(|&c| c > after && c <= at_most)
            .collect();
        candidates.into_iter().rev().find(|&c| !splits(c))
    }

    /// The structure graph as the parser wrote it.
    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    /// The innermost element holding characters `[begin, end)` of the
    /// document's text, for a mention's `doc:sourceElement`.
    pub fn element_at(&self, begin: usize, end: usize) -> Option<&str> {
        self.spans
            .iter()
            .filter(|(_, s)| s.begin <= begin && end <= s.end)
            .min_by_key(|(_, s)| s.end - s.begin)
            .map(|(iri, _)| iri.as_str())
    }

    /// The `xt:document` graph for one part: the elements inside it with
    /// offsets moved to the part's text, the document node carrying that
    /// text, and `doc:sourceDocument` on every element that lacks it.
    pub fn document_graph(&self, doc: &Document<'_>, part: &Part) -> Graph {
        let begin_p = format!("{NIF_NS}beginIndex");
        let end_p = format!("{NIF_NS}endIndex");
        // Text a clipped container no longer matches.
        let text_ps = [
            format!("{NIF_NS}isString"),
            format!("{NIF_NS}anchorOf"),
            format!("{RDFS_NS}label"),
        ];
        let source_p = format!("{DOC_NS}sourceDocument");
        let mut graph = Graph::new();
        let mut sourced = BTreeSet::new();
        for t in self.graph.iter() {
            let subject = t.subject().as_iri();
            let span = subject.and_then(|s| self.spans.get(s));
            if let Some(span) = span {
                if !span.overlaps(part) {
                    continue;
                }
                let clipped = span.begin < part.begin || span.end > part.end;
                let p = t.predicate().as_iri().unwrap_or_default();
                if p == begin_p {
                    let b = span.begin.clamp(part.begin, part.end) - part.begin;
                    graph.add_triple(
                        t.subject().clone(),
                        t.predicate().clone(),
                        Term::integer(b as i64),
                    );
                    continue;
                }
                if p == end_p {
                    let e = span.end.clamp(part.begin, part.end) - part.begin;
                    graph.add_triple(
                        t.subject().clone(),
                        t.predicate().clone(),
                        Term::integer(e as i64),
                    );
                    continue;
                }
                if clipped && text_ps.iter().any(|x| x == p) {
                    continue;
                }
                if p == source_p {
                    sourced.insert(subject.unwrap_or_default().to_string());
                }
            } else if subject == Some(doc.iri) {
                // The document node is restated below with this part's text.
                continue;
            }
            graph.add(t.clone());
        }
        let doc_term = Term::iri(doc.iri);
        for (iri, span) in &self.spans {
            if span.overlaps(part) && iri != doc.iri && !sourced.contains(iri) {
                graph.add_triple(Term::iri(iri), Term::iri(&source_p), doc_term.clone());
            }
        }
        graph.add_triple(
            doc_term.clone(),
            Term::iri(format!("{RDF_NS}type")),
            Term::iri(format!("{DOC_NS}SourceDocument")),
        );
        graph.add_triple(
            doc_term.clone(),
            Term::iri(format!("{NIF_NS}isString")),
            Term::string(char_slice(doc.text, part.begin, part.end)),
        );
        graph.add_triple(
            doc_term,
            Term::iri(format!("{DOC_NS}documentName")),
            Term::string(doc.name),
        );
        graph
    }
}

/// The request body for one part, as TriG.
pub fn body(document: &Graph, ontology: &Graph, catalog: &Graph, part: &Part) -> Result<String> {
    let mut out = String::new();
    for (role, graph) in [
        ("document", document),
        ("ontology", ontology),
        ("instances", catalog),
    ] {
        if graph.is_empty() && role != "document" {
            continue;
        }
        push_graph(&mut out, role, graph)?;
    }
    if !part.windows.is_empty() {
        let mut controls = Graph::new();
        let request = Term::blank("request");
        controls.add_triple(
            request.clone(),
            Term::iri(format!("{RDF_NS}type")),
            Term::iri(format!("{ROLE_NS}Request")),
        );
        for (i, (b, e)) in part.windows.iter().enumerate() {
            let w = Term::blank(format!("window{i}"));
            controls.add_triple(
                request.clone(),
                Term::iri(format!("{ROLE_NS}window")),
                w.clone(),
            );
            controls.add_triple(
                w.clone(),
                Term::iri(format!("{NIF_NS}beginIndex")),
                Term::integer(*b as i64),
            );
            controls.add_triple(
                w,
                Term::iri(format!("{NIF_NS}endIndex")),
                Term::integer(*e as i64),
            );
        }
        push_graph(&mut out, "request", &controls)?;
    }
    Ok(out)
}

/// One role graph as a TriG block. Blank node labels are scoped to the whole
/// TriG document, so each role's are prefixed to keep two inputs' `_:b0`
/// from becoming one node.
fn push_graph(out: &mut String, role: &str, graph: &Graph) -> Result<()> {
    let relabel = |t: &Term| match t.as_blank() {
        Some(b) => Term::blank(format!("{role}_{}", b.as_str())),
        None => t.clone(),
    };
    let mut scoped = Graph::new();
    for t in graph.iter() {
        scoped.add_triple(
            relabel(t.subject()),
            t.predicate().clone(),
            relabel(t.object()),
        );
    }
    let lines = fluree_graph_format::format_ntriples(&scoped)
        .map_err(|e| DocError::Parse(format!("{role} graph: {e}")))?;
    out.push_str(&format!("<{ROLE_NS}{role}> {{\n{lines}}}\n"));
    Ok(())
}

/// A graph as sorted N-Triples, for hashing what a request was built from.
pub fn canonical_ntriples(graph: &Graph) -> String {
    let mut g = graph.clone();
    g.sort();
    g.dedupe();
    fluree_graph_format::format_ntriples(&g).unwrap_or_default()
}

/// A JSON-LD document read into a graph.
///
/// A JSON number or boolean with no `@type` is typed as JSON-LD's RDF
/// conversion types it (`xsd:integer`, `xsd:double`, `xsd:boolean`): the
/// adapter keeps the native value but labels it `xsd:string`, which a
/// reader then refuses (`doc:columnIndex must be an xsd:integer`).
pub fn parse_jsonld(json: &str) -> std::result::Result<Graph, String> {
    use fluree_graph_ir::LiteralValue;
    let value: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let expanded = fluree_graph_json_ld::expand(&value).map_err(|e| e.to_string())?;
    let mut sink = GraphCollectorSink::new();
    fluree_graph_json_ld::adapter::to_graph_events(&expanded, &mut sink)
        .map_err(|e| e.to_string())?;
    let mut graph = sink.into_graph();
    for t in graph.iter_mut() {
        let retyped = match t.object() {
            Term::Literal {
                value,
                datatype,
                language: None,
            } if datatype.is_xsd_string() => match value {
                LiteralValue::Integer(i) => Some(Term::integer(*i)),
                LiteralValue::Double(d) => Some(Term::double(*d)),
                LiteralValue::Boolean(b) => Some(Term::boolean(*b)),
                _ => None,
            },
            _ => None,
        };
        if let Some(object) = retyped {
            *t = fluree_graph_ir::Triple::new(t.subject().clone(), t.predicate().clone(), object);
        }
    }
    Ok(graph)
}

/// Turtle or N-Triples read into a graph.
pub fn parse_turtle(text: &str) -> std::result::Result<Graph, String> {
    let mut sink = GraphCollectorSink::new();
    fluree_graph_turtle::parse(text, &mut sink).map_err(|e| e.to_string())?;
    Ok(sink.into_graph())
}

fn integer(term: &Term) -> Option<usize> {
    let (value, _, _) = term.as_literal()?;
    value
        .as_integer()
        .and_then(|n| usize::try_from(n).ok())
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
}

/// Characters `[begin, end)` of `text`, counted in Unicode scalar values as
/// NIF offsets are.
pub fn char_slice(text: &str, begin: usize, end: usize) -> &str {
    let byte = |n: usize| text.char_indices().nth(n).map_or(text.len(), |(i, _)| i);
    &text[byte(begin)..byte(end)]
}

/// The ontology's size, for [`PromptSize`]: its classes and properties.
pub fn ontology_terms(ontology: &Graph) -> usize {
    let type_p = format!("{RDF_NS}type");
    let kinds = [
        format!("{RDFS_NS}Class"),
        format!("{RDF_NS}Property"),
        "http://www.w3.org/2002/07/owl#Class".to_string(),
        "http://www.w3.org/2002/07/owl#ObjectProperty".to_string(),
        "http://www.w3.org/2002/07/owl#DatatypeProperty".to_string(),
    ];
    ontology
        .iter()
        .filter(|t| t.predicate().as_iri() == Some(type_p.as_str()))
        .filter(|t| {
            t.object()
                .as_iri()
                .is_some_and(|o| kinds.iter().any(|k| k == o))
        })
        .map(|t| t.subject().clone())
        .collect::<BTreeSet<_>>()
        .len()
}

/// The catalog's size, for [`PromptSize`]: its distinct subjects.
pub fn catalog_records(catalog: &Graph) -> usize {
    catalog
        .iter()
        .map(|t| t.subject().clone())
        .collect::<BTreeSet<_>>()
        .len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doco(elements: &[(&str, usize, usize, &[&str])]) -> String {
        let nodes: Vec<_> = elements
            .iter()
            .map(|(id, b, e, children)| {
                let mut node = json!({
                    "@id": format!("urn:d-{id}"),
                    "@type": "doco:Paragraph",
                    "nif:beginIndex": b,
                    "nif:endIndex": e,
                    "nif:isString": "x",
                });
                if !children.is_empty() {
                    node["po:contains"] = json!(children
                        .iter()
                        .map(|c| json!({"@id": format!("urn:d-{c}")}))
                        .collect::<Vec<_>>());
                }
                node
            })
            .collect();
        json!({
            "@context": {
                "doco": "http://purl.org/spar/doco/",
                "nif": NIF_NS,
                "po": PO_NS,
            },
            "@graph": nodes,
        })
        .to_string()
    }

    fn structure(text: &str, elements: &[(&str, usize, usize, &[&str])]) -> Structure {
        let doco = doco(elements);
        Structure::new(&Document {
            iri: "urn:d",
            name: "d.md",
            text,
            doco: &doco,
        })
        .unwrap()
    }

    fn limits(chars: usize) -> Limits {
        Limits {
            request_chars: chars,
            ..Limits::default()
        }
    }

    #[test]
    fn a_short_text_is_one_part_with_no_windows() {
        let s = structure("aaaa bbbb", &[("a", 0, 4, &[]), ("b", 5, 9, &[])]);
        let parts = s.plan(&Limits::default(), &PromptSize::default()).unwrap();
        assert_eq!(
            parts,
            vec![Part {
                begin: 0,
                end: 9,
                windows: vec![]
            }]
        );
    }

    #[test]
    fn a_long_text_is_cut_between_elements_never_inside_one() {
        let text = "aaaa bbbb cccc";
        let s = structure(
            text,
            &[("a", 0, 4, &[]), ("b", 5, 9, &[]), ("c", 10, 14, &[])],
        );
        let parts = s.plan(&limits(8), &PromptSize::default()).unwrap();
        let runs: Vec<_> = parts.iter().map(|p| (p.begin, p.end)).collect();
        // 8 characters would split `b` (5..9); the cut backs off to 5.
        assert_eq!(runs, vec![(0, 5), (5, 10), (10, 14)]);
    }

    #[test]
    fn a_container_too_long_for_one_call_is_cut_between_its_leaves_and_clipped() {
        let text = "aaaa bbbb";
        let s = structure(
            text,
            &[("t", 0, 9, &["a", "b"]), ("a", 0, 4, &[]), ("b", 5, 9, &[])],
        );
        let parts = s.plan(&limits(6), &PromptSize::default()).unwrap();
        assert_eq!(parts.len(), 2);
        let doc = Document {
            iri: "urn:d",
            name: "d.md",
            text,
            doco: "",
        };
        let second = s.document_graph(&doc, &parts[1]);
        let nt = fluree_graph_format::format_ntriples(&second).unwrap();
        // The container keeps its place, clipped to this part, without the
        // text it no longer matches; the leaf moves to the part's offsets.
        assert!(nt.contains("<urn:d-t> <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#beginIndex> \"0\""));
        assert!(!nt.contains(
            "<urn:d-t> <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#isString>"
        ));
        assert!(nt.contains("<urn:d-b> <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#endIndex> \"4\""));
        // The other part's leaf is only named by the container's spine.
        assert!(!nt.contains(
            "<urn:d-a> <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#beginIndex>"
        ));
        assert!(nt.contains("\"bbbb\""));
        assert!(nt.contains("<urn:d-b> <https://ns.flur.ee/doc#sourceDocument> <urn:d>"));
    }

    #[test]
    fn a_span_belongs_to_the_innermost_element_holding_it() {
        let s = structure(
            "aaaa bbbb",
            &[("t", 0, 9, &["a", "b"]), ("a", 0, 4, &[]), ("b", 5, 9, &[])],
        );
        assert_eq!(s.element_at(6, 8), Some("urn:d-b"));
        assert_eq!(s.element_at(3, 6), Some("urn:d-t"));
        assert_eq!(s.element_at(3, 12), None);
    }

    #[test]
    fn a_leaf_longer_than_a_call_is_refused_by_name() {
        let s = structure("aaaaaaaa", &[("a", 0, 8, &[])]);
        let err = s.plan(&limits(4), &PromptSize::default()).unwrap_err();
        assert!(err.to_string().contains("urn:d-a (8 characters)"), "{err}");
    }

    #[test]
    fn a_big_catalog_tiles_the_part_into_windows_at_leaf_boundaries() {
        let text = "a".repeat(30_000);
        let s = structure(
            &text,
            &[
                ("a", 0, 10_000, &[]),
                ("b", 10_000, 20_000, &[]),
                ("c", 20_000, 30_000, &[]),
            ],
        );
        // 2,630 records leave room for about 15,700 characters a window.
        let size = PromptSize {
            ontology_terms: 10,
            catalog_records: 2_630,
        };
        let parts = s.plan(&Limits::default(), &size).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(
            parts[0].windows,
            vec![(0, 10_000), (10_000, 20_000), (20_000, 30_000)]
        );
    }

    #[test]
    fn the_body_names_every_role_and_keeps_blank_nodes_apart() {
        let mut ontology = Graph::new();
        ontology.add_triple(Term::blank("b0"), Term::iri("urn:p"), Term::string("o"));
        let mut catalog = Graph::new();
        catalog.add_triple(Term::blank("b0"), Term::iri("urn:p"), Term::string("c"));
        let document = Graph::new();
        let part = Part {
            begin: 0,
            end: 10,
            windows: vec![(0, 5), (5, 10)],
        };
        let trig = body(&document, &ontology, &catalog, &part).unwrap();
        assert!(trig.contains("<https://ns.flur.ee/extract#ontology> {"));
        assert!(trig.contains("<https://ns.flur.ee/extract#instances> {"));
        assert!(trig.contains("<https://ns.flur.ee/extract#request> {"));
        assert!(trig.contains("_:ontology_b0") && trig.contains("_:instances_b0"));
    }

    #[test]
    fn json_numbers_keep_their_rdf_types() {
        let g = parse_jsonld(r#"{"@context":{"ex":"urn:ex:"},"@graph":[{"@id":"urn:a","ex:i":0,"ex:d":1.5,"ex:b":true,"ex:s":"0"}]}"#).unwrap();
        let nt = fluree_graph_format::format_ntriples(&g).unwrap();
        assert!(
            nt.contains("<urn:ex:i> \"0\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            "{nt}"
        );
        assert!(
            nt.contains("<urn:ex:d> \"1.5E0\"^^<http://www.w3.org/2001/XMLSchema#double>"),
            "{nt}"
        );
        assert!(
            nt.contains("<urn:ex:b> \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>"),
            "{nt}"
        );
        assert!(nt.contains("<urn:ex:s> \"0\" ."), "{nt}");
    }

    #[test]
    fn char_slice_counts_unicode_scalars() {
        assert_eq!(char_slice("né ü x", 1, 4), "é ü");
    }
}
