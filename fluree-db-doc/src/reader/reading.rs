//! A reader's answer: a Fluree DB transaction in the extraction vocabulary
//! (`xt:`, [`XT_NS`]), response layout 2 ([`LAYOUT`]).
//!
//! The answer is one run over one document, ready to transact as it is:
//!
//! - the run (its IRI, the document it read and that text's SHA-256, the
//!   release) is the commit's metadata, the JSON-LD `txn-meta` object;
//! - the facts are the default graph's nodes: the triples of the accepted,
//!   affirmed claims, and each new entity's type and label;
//! - each claim behind a fact is an annotation of that fact (`@annotation`);
//!   any other claim states its triple as an `rdf:Statement`;
//! - the mentions, when asked for, are a named graph (`xt:mentions`) of
//!   occurrences anchored to the text's offsets.
//!
//! The document is not echoed. Nothing here renames the reader's terms: the
//! transaction is kept as the reader wrote it, with one change, when the
//! document was read in several calls: the offsets of a later call are moved
//! from its part of the text into the whole document's. What is read out of
//! it ([`Reading::mentions`], [`Reading::claims`], the counts) is for
//! reporting, and [`trig`] / [`turtle`] print the same transaction as RDF.
//!
//! An answer in another layout (one that echoes the document graph, or
//! carries no commit metadata naming layout 2) is refused by name.

use crate::vocab::{FLUREE_NS, NIF_NS, RDF_NS};
use crate::{DocError, Result};
use fluree_graph_ir::{Dataset, Graph, GraphCollectorSink, LiteralValue, Term};
use fluree_graph_json_ld::ParsedContext;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// The extraction vocabulary.
pub const XT_NS: &str = "https://ns.flur.ee/extract#";
/// The response layout this reads: a Fluree DB transaction, the run in the
/// commit metadata, no document echo.
pub const LAYOUT: &str = "https://ns.flur.ee/extract/response/2";
/// The graph a TriG transaction writes the commit metadata in.
pub const TXN_META_GRAPH: &str = "#txn-meta";
/// The commit metadata's subject, `fluree:commit:this`.
const COMMIT_THIS: &str = "https://ns.flur.ee/db#commit:this";
/// What the contract names a run's new entities under (`<prefix><document
/// key>-<entity key>`): counted as new even when no mention marks them.
const NEW_ENTITY_PREFIX: &str = "urn:fluree:entity:";

const BEGIN: &str = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#beginIndex";
const END: &str = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#endIndex";

/// Where a part's answer sits in the whole document.
#[derive(Debug, Clone, Copy)]
pub struct Frame<'a> {
    pub doc_iri: &'a str,
    /// The part's first character in the document's text.
    pub offset: usize,
}

/// One span of text the reading found (`xt:Occurrence`).
#[derive(Debug, Clone, PartialEq)]
pub struct Mention {
    pub id: String,
    /// The words, `nif:anchorOf`.
    pub text: Option<String>,
    /// Offsets in the document's text, end exclusive.
    pub begin: Option<usize>,
    pub end: Option<usize>,
    /// `xt:occurrenceClass`.
    pub class: Option<String>,
    /// The catalog record it links to (`xt:entity`), or the entity the run
    /// minted for it (`xt:localEntity`, and then `new_entity`).
    pub entity: Option<String>,
    pub new_entity: bool,
    /// `xt:linkConfidence` for a link, else `xt:score`.
    pub confidence: Option<f64>,
    /// The structure element the span sits in (`doc:sourceElement`'s
    /// target), when the document's structure was at hand.
    pub element: Option<String>,
}

/// One claim (`xt:Claim`).
#[derive(Debug, Clone, PartialEq)]
pub struct Claim {
    pub id: String,
    pub subject: Option<String>,
    pub predicate: Option<String>,
    pub object: Option<Term>,
    /// `xt:assertionStatus`: `xt:affirmed`, `xt:negated`, `xt:hypothetical`.
    pub status: Option<String>,
    pub accepted: Option<bool>,
    pub score: Option<f64>,
    /// The claim annotates its fact: the triple is in the facts.
    pub asserted: bool,
}

/// One call's answer, read.
#[derive(Debug, Clone)]
pub struct Reading {
    /// The answer as the reader wrote it (compact JSON-LD, a Fluree DB
    /// transaction), offsets in the document's text.
    pub transaction: Value,
    /// The same as RDF; the commit metadata in the graph [`TXN_META_GRAPH`].
    pub dataset: Dataset,
    /// The run's IRI (`xt:run` in the commit metadata, else the answer's
    /// `X-Fluree-Run`).
    pub run: Option<String>,
    /// The release that answered and the endpoint's ID for the call
    /// (`X-Fluree-Release`, `X-Fluree-Request-ID`), when the answer came
    /// over the wire with them.
    pub release: Option<String>,
    pub request_id: Option<String>,
    /// Whether the reading cache answered instead of the reader.
    pub cached: bool,
    pub mentions: Vec<Mention>,
    pub claims: Vec<Claim>,
    /// Statements in the facts, claims aside.
    pub facts: usize,
    /// Entities the run minted, as against catalog records.
    pub new_entities: BTreeSet<String>,
    /// Catalog records the mentions link to.
    pub linked: BTreeSet<String>,
    /// The answer's prefixes, for printing it.
    prefixes: BTreeMap<String, String>,
}

/// Read one answer. `frame.offset` moves its offsets into the document's
/// text.
pub fn read_answer(body: &str, frame: &Frame<'_>) -> Result<Reading> {
    let mut doc: Value = serde_json::from_str(body)
        .map_err(|e| DocError::Model(format!("the reader's answer is not JSON: {e}")))?;
    if !doc.is_object() {
        return Err(not_layout_2("it is not a JSON-LD object"));
    }
    let raw_context = doc.get("@context").cloned().unwrap_or_else(|| json!({}));
    let ctx = fluree_graph_json_ld::parse_context(&raw_context)
        .map_err(|e| DocError::Model(format!("the reader's answer: @context: {e}")))?;
    check_layout(&doc, &ctx)?;
    if frame.offset > 0 {
        shift_offsets(&mut doc, &ctx, frame.offset as u64);
    }
    let dataset = to_dataset(&doc, &raw_context, &ctx)?;
    let mut reading = Reading {
        transaction: doc,
        dataset,
        run: None,
        release: None,
        request_id: None,
        cached: false,
        mentions: Vec::new(),
        claims: Vec::new(),
        facts: 0,
        new_entities: BTreeSet::new(),
        linked: BTreeSet::new(),
        prefixes: prefixes_of(&raw_context),
    };
    reading.index();
    Ok(reading)
}

fn not_layout_2(why: &str) -> DocError {
    DocError::Model(format!(
        "the reader's answer is not in the layout this reads ({why}): expected a Fluree DB \
         transaction in response layout 2 (<{LAYOUT}>), the run in its `txn-meta` and the \
         facts in its @graph. An answer that echoes the document graph is the earlier layout; \
         a reader serving it needs updating"
    ))
}

/// The key `key` stands for: an IRI, or a keyword.
fn key_iri(key: &str, ctx: &ParsedContext) -> String {
    if key.starts_with('@') || key == ctx.id_key || key == ctx.type_key {
        return key.to_string();
    }
    fluree_graph_json_ld::expand_iri_with_vocab(key, ctx, true)
}

/// A value's IRI: `{"@id": …}`, or a string where the key coerces to `@id`.
fn value_iri(value: &Value, ctx: &ParsedContext) -> Option<String> {
    let s = match value {
        Value::String(s) => s.as_str(),
        Value::Object(o) => o.get("@id").or_else(|| o.get(&ctx.id_key))?.as_str()?,
        _ => return None,
    };
    Some(fluree_graph_json_ld::expand_iri_with_vocab(s, ctx, false))
}

fn check_layout(doc: &Value, ctx: &ParsedContext) -> Result<()> {
    let Some(meta) = doc.get("txn-meta").and_then(Value::as_object) else {
        return Err(not_layout_2("it carries no commit metadata, `txn-meta`"));
    };
    let layout_key = format!("{XT_NS}responseLayout");
    let layouts: Vec<String> = meta
        .iter()
        .filter(|(k, _)| key_iri(k, ctx) == layout_key)
        .flat_map(|(_, v)| match v {
            Value::Array(a) => a.clone(),
            v => vec![v.clone()],
        })
        .filter_map(|v| value_iri(&v, ctx))
        .collect();
    if layouts.iter().any(|l| l == LAYOUT) {
        return Ok(());
    }
    Err(not_layout_2(&match layouts.first() {
        Some(other) => format!("it names layout <{other}>"),
        None => "its commit metadata names no `xt:responseLayout`".to_string(),
    }))
}

/// Add `offset` to every `nif:beginIndex` / `nif:endIndex` in the document,
/// whatever graph or node it sits in.
fn shift_offsets(value: &mut Value, ctx: &ParsedContext, offset: u64) {
    fn shift(v: &mut Value, offset: u64) {
        match v {
            Value::Number(n) => {
                if let Some(n) = n.as_u64() {
                    *v = json!(n + offset);
                }
            }
            Value::Object(o) => match o.get_mut("@value") {
                Some(Value::Number(n)) => {
                    if let Some(n) = n.as_u64() {
                        o.insert("@value".into(), json!(n + offset));
                    }
                }
                Some(Value::String(s)) => {
                    if let Ok(n) = s.parse::<u64>() {
                        *s = (n + offset).to_string();
                    }
                }
                _ => {}
            },
            Value::Array(a) => a.iter_mut().for_each(|v| shift(v, offset)),
            _ => {}
        }
    }
    match value {
        Value::Object(o) => {
            for (k, v) in o.iter_mut() {
                if k == "@context" {
                    continue;
                }
                let iri = key_iri(k, ctx);
                if iri == BEGIN || iri == END {
                    shift(v, offset);
                } else {
                    shift_offsets(v, ctx, offset);
                }
            }
        }
        Value::Array(a) => a.iter_mut().for_each(|v| shift_offsets(v, ctx, offset)),
        _ => {}
    }
}

/// A fact one of the answer's claims annotates.
struct Annotated {
    graph: Option<String>,
    subject: String,
    key: String,
    value: Value,
    reifier: String,
}

/// The answer as RDF: every graph, each claim's annotation as a reifier
/// attachment to its fact, and the commit metadata in [`TXN_META_GRAPH`].
fn to_dataset(doc: &Value, raw_context: &Value, ctx: &ParsedContext) -> Result<Dataset> {
    let bad = |e: String| DocError::Model(format!("the reader's answer: {e}"));
    let mut body = doc.clone();
    let meta = body
        .as_object_mut()
        .and_then(|o| o.remove("txn-meta"))
        .unwrap_or_else(|| json!({}));
    let mut annotated = Vec::new();
    if let Some(items) = body.get_mut("@graph").and_then(Value::as_array_mut) {
        split_graph(items, None, ctx, &mut annotated);
    }

    let expanded = fluree_graph_json_ld::expand(&body).map_err(|e| bad(e.to_string()))?;
    let items = match expanded {
        Value::Array(items) => items,
        Value::Object(o) => match o.get("@graph") {
            Some(Value::Array(g)) if !o.contains_key("@id") => g.clone(),
            _ => vec![Value::Object(o)],
        },
        _ => Vec::new(),
    };
    let mut dataset = Dataset::new();
    let mut default_nodes = Vec::new();
    for item in items {
        match (item.get("@id").and_then(Value::as_str), item.get("@graph")) {
            (Some(name), Some(Value::Array(nodes))) => {
                let graph = to_graph(nodes).map_err(bad)?;
                let target = dataset.graph_mut(Some(&Term::iri(name)));
                merge(target, graph);
            }
            _ => default_nodes.push(item),
        }
    }
    merge(&mut dataset.default, to_graph(&default_nodes).map_err(bad)?);

    for a in annotated {
        let mini = json!({ "@context": raw_context, "@id": a.subject, a.key: a.value });
        let expanded = fluree_graph_json_ld::expand(&mini).map_err(|e| bad(e.to_string()))?;
        let graph = to_graph(&as_array(expanded)).map_err(bad)?;
        let Some(t) = graph.iter().next() else {
            continue;
        };
        let reifier = Term::iri(fluree_graph_json_ld::expand_iri_with_vocab(
            &a.reifier, ctx, false,
        ));
        let name = a.graph.as_deref().map(Term::iri);
        dataset.graph_mut(name.as_ref()).add_reification(
            t.subject().clone(),
            t.predicate().clone(),
            t.object().clone(),
            reifier,
        );
    }

    if let Value::Object(mut meta) = meta {
        meta.insert("@id".into(), json!(COMMIT_THIS));
        meta.insert("@context".into(), raw_context.clone());
        let expanded =
            fluree_graph_json_ld::expand(&Value::Object(meta)).map_err(|e| bad(e.to_string()))?;
        let graph = to_graph(&as_array(expanded)).map_err(bad)?;
        if !graph.is_empty() {
            merge(dataset.graph_mut(Some(&Term::iri(TXN_META_GRAPH))), graph);
        }
    }
    Ok(dataset)
}

fn as_array(v: Value) -> Vec<Value> {
    match v {
        Value::Array(a) => a,
        v => vec![v],
    }
}

fn merge(into: &mut Graph, from: Graph) {
    for r in from.reifications() {
        into.add_reification(
            r.triple.subject().clone(),
            r.triple.predicate().clone(),
            r.triple.object().clone(),
            r.reifier.clone(),
        );
    }
    for t in from.iter() {
        into.add(t.clone());
    }
}

fn to_graph(nodes: &[Value]) -> std::result::Result<Graph, String> {
    let mut sink = GraphCollectorSink::new();
    fluree_graph_json_ld::adapter::to_graph_events(&Value::Array(nodes.to_vec()), &mut sink)
        .map_err(|e| e.to_string())?;
    let mut graph = sink.into_graph();
    super::request::retype_native_literals(&mut graph);
    Ok(graph)
}

/// Take each `@annotation` off the values in `items` (one graph's nodes, or
/// the top level with its named graphs), keeping the annotation as a node
/// of that graph and a record of the fact it annotates.
fn split_graph(
    items: &mut Vec<Value>,
    graph: Option<String>,
    ctx: &ParsedContext,
    out: &mut Vec<Annotated>,
) {
    let mut added = Vec::new();
    for item in items.iter_mut() {
        let Value::Object(node) = item else { continue };
        if node.contains_key("@graph") {
            let name = node
                .get("@id")
                .or_else(|| node.get(&ctx.id_key))
                .and_then(Value::as_str)
                .map(|s| fluree_graph_json_ld::expand_iri_with_vocab(s, ctx, false));
            if let Some(Value::Array(inner)) = node.get_mut("@graph") {
                split_graph(inner, name, ctx, out);
            }
            continue;
        }
        split_node(node, &graph, ctx, out, &mut added);
    }
    items.extend(added);
}

fn split_node(
    node: &mut Map<String, Value>,
    graph: &Option<String>,
    ctx: &ParsedContext,
    out: &mut Vec<Annotated>,
    added: &mut Vec<Value>,
) {
    let subject = node
        .get("@id")
        .or_else(|| node.get(&ctx.id_key))
        .and_then(Value::as_str)
        .map(str::to_string);
    for (key, values) in node.iter_mut() {
        if key.starts_with('@') || *key == ctx.id_key || *key == ctx.type_key {
            continue;
        }
        let list: Vec<&mut Value> = match values {
            Value::Array(a) => a.iter_mut().collect(),
            v => vec![v],
        };
        for value in list {
            let Value::Object(v) = value else { continue };
            if let Some(annotation) = v.remove("@annotation") {
                let reifier = annotation
                    .get("@id")
                    .or_else(|| annotation.get(&ctx.id_key))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                if let (Some(subject), Some(reifier)) = (&subject, reifier) {
                    out.push(Annotated {
                        graph: graph.clone(),
                        subject: subject.clone(),
                        key: key.clone(),
                        value: Value::Object(v.clone()),
                        reifier,
                    });
                }
                if annotation.is_object() {
                    added.push(annotation);
                }
            } else if !v.contains_key("@value") {
                // An embedded node: its own values may be annotated too.
                split_node(v, graph, ctx, out, added);
            }
        }
    }
}

/// The answer's prefixes: its context's namespaces, and `fluree:` for the
/// commit metadata.
fn prefixes_of(raw_context: &Value) -> BTreeMap<String, String> {
    let absolute =
        |s: &str| s.starts_with("http://") || s.starts_with("https://") || s.starts_with("urn:");
    let mut out = BTreeMap::new();
    if let Some(ctx) = raw_context.as_object() {
        for (k, v) in ctx {
            if k.starts_with('@') {
                continue;
            }
            let ns = match v {
                Value::String(s) if absolute(s) => Some(s.clone()),
                Value::Object(o) if o.get("@prefix") == Some(&Value::Bool(true)) => o
                    .get("@id")
                    .and_then(Value::as_str)
                    .filter(|s| absolute(s))
                    .map(str::to_string),
                _ => None,
            };
            if let Some(ns) = ns {
                out.insert(k.clone(), ns);
            }
        }
    }
    out.entry("fluree".into())
        .or_insert_with(|| FLUREE_NS.to_string());
    out
}

impl Reading {
    /// Read the mentions, claims and counts out of the dataset.
    fn index(&mut self) {
        let xt = |local: &str| format!("{XT_NS}{local}");
        let rdf_type = format!("{RDF_NS}type");
        let meta = Term::iri(TXN_META_GRAPH);
        let mut props: HashMap<String, Vec<(String, Term)>> = HashMap::new();
        let mut asserted: HashMap<String, (Term, Term, Term)> = HashMap::new();
        for (name, graph) in self.dataset.graphs() {
            if name == Some(&meta) {
                if self.run.is_none() {
                    self.run = graph
                        .iter()
                        .find(|t| t.predicate().as_iri() == Some(&xt("run")))
                        .and_then(|t| t.object().as_iri())
                        .map(str::to_string);
                }
                continue;
            }
            for t in graph.iter() {
                let Some(s) = subject_key(t.subject()) else {
                    continue;
                };
                let p = t.predicate().as_iri().unwrap_or_default().to_string();
                props.entry(s).or_default().push((p, t.object().clone()));
            }
            for r in graph.reifications() {
                if let Some(id) = subject_key(&r.reifier) {
                    asserted.insert(
                        id,
                        (
                            r.triple.subject().clone(),
                            r.triple.predicate().clone(),
                            r.triple.object().clone(),
                        ),
                    );
                }
            }
        }
        let types = |s: &str| -> Vec<&str> {
            props
                .get(s)
                .into_iter()
                .flatten()
                .filter(|(p, _)| *p == rdf_type)
                .filter_map(|(_, o)| o.as_iri())
                .collect()
        };
        let first = |s: &str, p: &str| -> Option<&Term> {
            props.get(s)?.iter().find(|(q, _)| q == p).map(|(_, o)| o)
        };
        let iri = |s: &str, p: &str| first(s, p).and_then(Term::as_iri).map(str::to_string);
        let number = |s: &str, p: &str| -> Option<f64> {
            let (v, _, _) = first(s, p)?.as_literal()?;
            match v {
                LiteralValue::Double(d) => Some(*d),
                LiteralValue::Integer(i) => Some(*i as f64),
                other => other.as_str().and_then(|s| s.parse().ok()),
            }
        };
        let offset = |s: &str, p: &str| -> Option<usize> {
            let (v, _, _) = first(s, p)?.as_literal()?;
            match v {
                LiteralValue::Integer(i) => usize::try_from(*i).ok(),
                other => other.as_str().and_then(|s| s.parse().ok()),
            }
        };

        let mut subjects: Vec<&String> = props.keys().collect();
        subjects.sort();
        let claim_types = [xt("Claim"), format!("{RDF_NS}Statement")];
        let mut claim_nodes = BTreeSet::new();
        for s in subjects {
            let ts = types(s);
            if ts.contains(&xt("Occurrence").as_str()) {
                let (entity, new_entity) = match iri(s, &xt("entity")) {
                    Some(e) => (Some(e), false),
                    None => (iri(s, &xt("localEntity")), true),
                };
                let entity_is_some = entity.is_some();
                self.mentions.push(Mention {
                    id: s.clone(),
                    text: first(s, &format!("{NIF_NS}anchorOf"))
                        .and_then(Term::as_literal)
                        .map(|(v, _, _)| v.lexical()),
                    begin: offset(s, BEGIN),
                    end: offset(s, END),
                    class: iri(s, &xt("occurrenceClass")),
                    entity,
                    new_entity: new_entity && entity_is_some,
                    confidence: number(s, &xt("linkConfidence"))
                        .or_else(|| number(s, &xt("score"))),
                    element: None,
                });
            }
            if ts.contains(&xt("LocalEntity").as_str()) {
                self.new_entities.insert(s.clone());
            }
            if ts.iter().any(|t| claim_types.iter().any(|c| c == t)) {
                claim_nodes.insert(s.clone());
            }
            if ts.contains(&xt("Claim").as_str()) {
                let triple = asserted.get(s);
                let (subject, predicate, object) = match triple {
                    Some((s, p, o)) => (
                        s.as_iri().map(str::to_string),
                        p.as_iri().map(str::to_string),
                        Some(o.clone()),
                    ),
                    None => (
                        iri(s, &format!("{RDF_NS}subject")),
                        iri(s, &format!("{RDF_NS}predicate")),
                        first(s, &format!("{RDF_NS}object")).cloned(),
                    ),
                };
                self.claims.push(Claim {
                    id: s.clone(),
                    subject,
                    predicate,
                    object,
                    status: iri(s, &xt("assertionStatus")),
                    accepted: first(s, &xt("accepted"))
                        .and_then(Term::as_literal)
                        .and_then(|(v, _, _)| v.as_bool()),
                    score: number(s, &xt("score")),
                    asserted: triple.is_some(),
                });
            }
        }
        self.mentions.sort_by_key(|m| (m.begin, m.end));
        for m in &self.mentions {
            match (&m.entity, m.new_entity) {
                (Some(e), true) => {
                    self.new_entities.insert(e.clone());
                }
                (Some(e), false) => {
                    self.linked.insert(e.clone());
                }
                _ => {}
            }
        }
        for t in self.dataset.default.iter() {
            let Some(s) = subject_key(t.subject()) else {
                continue;
            };
            if s.starts_with(NEW_ENTITY_PREFIX) {
                self.new_entities.insert(s.clone());
            }
            if !claim_nodes.contains(&s) {
                self.facts += 1;
            }
        }
        // A catalog record is not new, whatever a mark says.
        let linked = self.linked.clone();
        self.new_entities.retain(|e| !linked.contains(e));
    }

    /// The prefixes the answer was written with.
    pub fn prefix_map(&self) -> fluree_graph_format::PrefixMap {
        fluree_graph_format::PrefixMap::from_map(self.prefixes.clone())
    }

    /// The transaction with every graph's content in the one named graph
    /// `graph`, its commit metadata kept: what `doc ingest` commits, so a
    /// re-read can replace the document's reading by clearing that graph.
    /// The facts and the claims that annotate them move together, so each
    /// annotation stays on its fact's edge.
    pub fn in_graph(&self, graph: &str) -> Value {
        let mut tx = self.transaction.clone();
        let Some(o) = tx.as_object_mut() else {
            return tx;
        };
        let items = match o.remove("@graph") {
            Some(Value::Array(items)) => items,
            Some(other) => vec![other],
            None => Vec::new(),
        };
        let mut nodes = Vec::new();
        for item in items {
            match item {
                Value::Object(mut named) if named.contains_key("@graph") => {
                    match named.remove("@graph") {
                        Some(Value::Array(inner)) => nodes.extend(inner),
                        Some(other) => nodes.push(other),
                        None => {}
                    }
                }
                other => nodes.push(other),
            }
        }
        o.insert("@graph".into(), json!([{ "@id": graph, "@graph": nodes }]));
        tx
    }
}

fn subject_key(t: &Term) -> Option<String> {
    match t.as_iri() {
        Some(iri) => Some(iri.to_string()),
        None => t.as_blank().map(|b| format!("_:{}", b.as_str())),
    }
}

/// Readings as TriG: each a transaction as it would be inserted, the commit
/// metadata in `<#txn-meta>`, each claim behind a fact as `rdf:reifies`.
/// Several calls' readings follow one another, each its own transaction.
pub fn trig(readings: &[Reading]) -> Result<String> {
    let mut out = String::new();
    for (i, r) in readings.iter().enumerate() {
        if readings.len() > 1 {
            out.push_str(&format!(
                "{}# call {} of {}: its own transaction{}\n",
                if i > 0 { "\n" } else { "" },
                i + 1,
                readings.len(),
                r.run
                    .as_deref()
                    .map(|run| format!(", run <{run}>"))
                    .unwrap_or_default()
            ));
        }
        out.push_str(
            &fluree_graph_format::format_trig(&r.dataset, &r.prefix_map())
                .map_err(|e| DocError::Model(format!("reading: {e}")))?,
        );
    }
    Ok(out)
}

/// Readings as Turtle: every graph's statements together, without the
/// commit metadata (Turtle has no graphs).
pub fn turtle(readings: &[Reading]) -> Result<String> {
    let meta = Term::iri(TXN_META_GRAPH);
    let mut graph = Graph::new();
    let mut prefixes = BTreeMap::new();
    for r in readings {
        for (name, g) in r.dataset.graphs() {
            if name != Some(&meta) {
                merge(&mut graph, g.clone());
            }
        }
        prefixes.extend(r.prefixes.clone());
    }
    graph.canonicalize();
    fluree_graph_format::format_turtle(&graph, &fluree_graph_format::PrefixMap::from_map(prefixes))
        .map_err(|e| DocError::Model(format!("reading: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An answer recorded from the reader's service on 2026-11-10 for a
    /// sample invoice, with `X-Fluree-Include: claims,mentions`: its IRIs
    /// and release renamed to neutral ones, nothing else changed.
    const INVOICE: &str = include_str!("../../tests/fixtures/reader/invoice-answer.jsonld");
    const ENTITY: &str = "urn:fluree:entity:d64n9b-kstx8ha68g50ryhb";
    const RUN: &str = "urn:fluree:run:01M72WJ0KYFWR6FJA8G14VBS1Y";

    fn frame(offset: usize) -> Frame<'static> {
        Frame {
            doc_iri: "urn:example:api:invoice",
            offset,
        }
    }

    #[test]
    fn a_recorded_answer_reads_as_facts_claims_and_mentions() {
        let r = read_answer(INVOICE, &frame(0)).unwrap();
        assert_eq!(r.run.as_deref(), Some(RUN));
        assert_eq!(r.mentions.len(), 6);
        let daxin = r
            .mentions
            .iter()
            .find(|m| m.text.as_deref() == Some("Daxin Scientific Supply Co., Ltd."))
            .unwrap();
        assert_eq!((daxin.begin, daxin.end), (Some(46), Some(79)));
        assert_eq!(daxin.entity.as_deref(), Some("urn:example:check:org/daxin"));
        assert!(!daxin.new_entity);
        assert!(daxin.confidence.unwrap() > 0.99);
        assert_eq!(
            daxin.class.as_deref(),
            Some("urn:example:check:Organization")
        );
        let number = r.mentions.iter().find(|m| m.begin == Some(22)).unwrap();
        assert_eq!(number.entity.as_deref(), Some(ENTITY));
        assert!(number.new_entity);

        assert_eq!(r.claims.len(), 2);
        assert!(r.claims.iter().all(|c| c.asserted
            && c.accepted == Some(true)
            && c.status.as_deref() == Some("https://ns.flur.ee/extract#affirmed")));
        let supplier = r
            .claims
            .iter()
            .find(|c| c.predicate.as_deref() == Some("urn:example:check:supplier"))
            .unwrap();
        assert_eq!(supplier.subject.as_deref(), Some(ENTITY));
        assert_eq!(
            supplier.object.as_ref().and_then(Term::as_iri),
            Some("urn:example:check:org/daxin")
        );
        let total = r
            .claims
            .iter()
            .find(|c| c.predicate.as_deref() == Some("urn:example:check:total"))
            .unwrap();
        assert_eq!(
            total
                .object
                .as_ref()
                .and_then(Term::as_literal)
                .map(|(v, _, _)| v.lexical()),
            Some("5170".to_string())
        );

        // The invoice's type, label, supplier and total.
        assert_eq!(r.facts, 4);
        assert_eq!(r.new_entities, BTreeSet::from([ENTITY.to_string()]));
        assert_eq!(
            r.linked,
            BTreeSet::from(["urn:example:check:org/daxin".to_string()])
        );
    }

    #[test]
    fn a_later_part_moves_every_offset_into_the_documents_text() {
        let r = read_answer(INVOICE, &frame(1000)).unwrap();
        let daxin = r
            .mentions
            .iter()
            .find(|m| m.entity.as_deref() == Some("urn:example:check:org/daxin"))
            .unwrap();
        assert_eq!((daxin.begin, daxin.end), (Some(1046), Some(1079)));
        // In the transaction too, which is what is stored and printed.
        let text = r.transaction.to_string();
        assert!(text.contains("\"beginIndex\":1046"), "{text}");
        assert!(!text.contains("\"beginIndex\":46,"), "{text}");
    }

    #[test]
    fn an_answer_in_another_layout_is_refused_by_name() {
        // The earlier layout: the document graph echoed, no commit metadata.
        let echo = json!([
            { "@id": "urn:d", "@graph": [{ "@id": "urn:d", format!("{NIF_NS}isString"): [{ "@value": "text" }] }] },
            { "@id": "urn:d/reading", "@graph": [] }
        ])
        .to_string();
        let err = read_answer(&echo, &frame(0)).unwrap_err().to_string();
        assert!(err.contains("response layout 2"), "{err}");
        assert!(err.contains("earlier layout"), "{err}");
        let other = INVOICE.replace(LAYOUT, "https://ns.flur.ee/extract/response/1");
        let err = read_answer(&other, &frame(0)).unwrap_err().to_string();
        assert!(
            err.contains("names layout <https://ns.flur.ee/extract/response/1>"),
            "{err}"
        );
        assert!(read_answer("not json", &frame(0)).is_err());
    }

    #[test]
    fn the_transaction_in_one_graph_keeps_its_commit_metadata_and_annotations() {
        let r = read_answer(INVOICE, &frame(0)).unwrap();
        let tx = r.in_graph("urn:example:api:invoice/reading");
        assert_eq!(tx["txn-meta"], r.transaction["txn-meta"]);
        assert_eq!(tx["@context"], r.transaction["@context"]);
        let graphs = tx["@graph"].as_array().unwrap();
        assert_eq!(graphs.len(), 1);
        assert_eq!(graphs[0]["@id"], "urn:example:api:invoice/reading");
        let nodes = graphs[0]["@graph"].as_array().unwrap();
        // The mentions graph's seven nodes and the invoice node.
        assert_eq!(nodes.len(), 8);
        let invoice = nodes
            .iter()
            .find(|n| n["id"] == ENTITY && n.get("label").is_some())
            .unwrap();
        assert_eq!(invoice["t:supplier"]["@annotation"]["@id"], "run:r-0");
        // Read back, it is the same reading in another graph.
        let again = read_answer(&tx.to_string(), &frame(0)).unwrap();
        assert_eq!(again.claims, r.claims);
        assert_eq!(again.mentions.len(), r.mentions.len());
    }

    #[test]
    fn a_reading_prints_as_trig_with_its_commit_metadata_and_as_turtle() {
        let r = read_answer(INVOICE, &frame(0)).unwrap();
        let trig = trig(std::slice::from_ref(&r)).unwrap();
        assert!(
            trig.contains("@prefix xt: <https://ns.flur.ee/extract#>"),
            "{trig}"
        );
        assert!(trig.contains("<#txn-meta>"), "{trig}");
        assert!(trig.contains("fluree:commit:this"), "{trig}");
        assert!(
            trig.contains("rdf:reifies") || trig.contains(" ~ "),
            "{trig}"
        );
        assert!(trig.contains("xt:mentions"), "{trig}");
        assert!(
            trig.contains("\"Daxin Scientific Supply Co., Ltd.\""),
            "{trig}"
        );
        // Native JSON numbers stay numbers.
        assert!(trig.contains("beginIndex 46"), "{trig}");
        let ttl = turtle(std::slice::from_ref(&r)).unwrap();
        assert!(!ttl.contains("txn-meta"), "{ttl}");
        assert!(!ttl.contains("commit:this"), "{ttl}");
        assert!(
            ttl.contains("urn:example:check:org/daxin") || ttl.contains("c:daxin"),
            "{ttl}"
        );
    }
}
