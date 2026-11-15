//! Edge-annotation lowering for the JSON-LD transactor parser.
//!
//! Runs on a clause's **expansion**, after [`refuse_reserved_iris`] has read
//! it, and rewrites every `@annotation` / `@edge` / `@reifies` (and the
//! `@ttl` / `@expiresAt` shorthand) into `f:reifies*` slots naming the
//! annotated edge. JSON-LD has no triple-term syntax, so the slots are how the
//! edge reaches `parse_expanded_triples_with_ctx`; [`fold_slots_into_links`]
//! then turns each annotation's slots into its `rdf:reifies <<( s p o )>>`
//! link, and bulk import's sink does the same. The slots are never stored.
//!
//! Every IRI a slot holds is one the expander resolved: the edge's predicate
//! is the key its value is under, by whatever context (scoped ones included)
//! resolved it, and its object is the value as the parser writes it.
//!
//! The **inline form** (`@annotation` / `@edge` on the value of a property)
//! annotates the edge from the node to that value:
//!    ```json
//!    { "@id": "ex:alice",
//!      "ex:worksFor": {
//!        "@id": "ex:acme",
//!        "@annotation": { "ex:role": "Engineer" }
//!      }
//!    }
//!    ```
//! Each annotation node reifies the edge `(ex:alice, ex:worksFor, ex:acme)`;
//! its subject is its `@id`, or a fresh blank node. A literal value
//! (`{"@value": ...}`) can be annotated too.
//!
//! The **annotation-rooted form** (`@reifies` on a node) makes the node a
//! reifier of each triple named, without asserting it.
//!
//! In an update's `delete`, an annotation retracts: by its `@id`, or, without
//! one, every annotation of the edge whose properties match its body, through
//! a `where` pattern the lowering adds.
//!
//! Deferred shapes are refused (see `docs/concepts/edge-annotations.md`
//! "Current limits"): annotation-of-annotation, an annotation on a `@list`,
//! `@set` or `@reverse` value, and annotation keywords inside a list or a
//! triple term.

use crate::error::{Result, TransactError};
use fluree_graph_json_ld::{
    expand_with_context_policy, expand_with_root_context, parse_context, ParsedContext,
};
use fluree_vocab::{rdf, reifies_iris};
use serde_json::{json, Map, Value};

const ANNOTATION_KEY: &str = "@annotation";
const EDGE_KEY: &str = "@edge";
const REIFIES_KEY: &str = "@reifies";

/// `@ttl` / `@expiresAt`: a value's lifetime, shorthand for a TTL
/// instruction in its annotation (`f:ttl` / `f:expiresAt`).
const TTL_KEY: &str = "@ttl";
const EXPIRES_AT_KEY: &str = "@expiresAt";

/// True when `key` is an annotation/edge keyword on the *object* side.
fn is_annotation_key(key: &str) -> bool {
    key == ANNOTATION_KEY || key == EDGE_KEY
}

/// True when `key` is TTL shorthand on the object side.
fn is_ttl_key(key: &str) -> bool {
    key == TTL_KEY || key == EXPIRES_AT_KEY
}

/// Move a value's `@ttl` / `@expiresAt` into its annotation as the TTL
/// instruction it stands for (`f:ttl` with whole seconds, or
/// `f:expiresAt` with a UTC `xsd:dateTime`), creating the annotation when
/// the value has none.
fn fold_ttl_shorthand(map: &mut Map<String, Value>) -> Result<()> {
    let ttl = map.remove(TTL_KEY);
    let expires = map.remove(EXPIRES_AT_KEY);
    let (instruction, value) = match (ttl, expires) {
        (None, None) => return Ok(()),
        (Some(_), Some(_)) => {
            return Err(TransactError::Parse(
                "a value takes @ttl or @expiresAt, not both".to_string(),
            ))
        }
        (Some(ttl), None) => {
            let seconds = crate::ttl_input::ttl_seconds_from_json(&ttl)
                .map_err(|e| TransactError::Parse(format!("@ttl: {e}")))?;
            (
                fluree_vocab::retention_iris::TTL,
                json!({ "@value": seconds }),
            )
        }
        (None, Some(expires)) => {
            let Value::String(text) = &expires else {
                return Err(TransactError::Parse(
                    "@expiresAt: a deadline is an RFC 3339 timestamp string".to_string(),
                ));
            };
            let instant = crate::ttl_input::expires_at(text)
                .map_err(|e| TransactError::Parse(format!("@expiresAt: {e}")))?;
            (
                fluree_vocab::retention_iris::EXPIRES_AT,
                json!({
                    "@value": crate::ttl_input::expires_at_lexical(&instant),
                    "@type": fluree_vocab::xsd::DATE_TIME,
                }),
            )
        }
    };
    let key = if map.contains_key(EDGE_KEY) {
        EDGE_KEY
    } else {
        ANNOTATION_KEY
    };
    let blocks = map.entry(key).or_insert_with(|| json!([{}]));
    let Some([Value::Object(block)]) = blocks.as_array_mut().map(Vec::as_mut_slice) else {
        return Err(TransactError::Parse(
            "@ttl / @expiresAt need one @annotation, an object".to_string(),
        ));
    };
    for iri in [
        fluree_vocab::retention_iris::TTL,
        fluree_vocab::retention_iris::EXPIRES_AT,
    ] {
        if block.contains_key(iri) {
            return Err(TransactError::Parse(format!(
                "a value takes one TTL instruction: @ttl / @expiresAt or {iri} in its \
                 @annotation, not both"
            )));
        }
    }
    block.insert(instruction.to_string(), json!([value]));
    Ok(())
}

/// Classification of the object position on a reified base edge. Drives
/// every site that produces an `f:reifies*` bundle (an annotation, a
/// retraction, a `@reifies` triple), so that all writers mint the same
/// `EdgeKey` for the same logical edge.
#[derive(Debug, Clone)]
pub(crate) enum ReifiedObjectShape {
    /// Object is an IRI or blank-node reference. The `String` is the
    /// IRI / blank-node identifier (possibly minted).
    Iri(String),
    /// Object is a literal. Fields capture the canonicalized JSON-LD
    /// value object: only `@value` (always), `@type` (when explicit),
    /// and `@language` (when explicit) survive — every other key is
    /// rejected upstream.
    Literal {
        /// Canonical value-object JSON: `{"@value": ..., ?"@type": ...,
        /// ?"@language": ...}`, the value as the parser writes it.
        value: Value,
        /// `@language` payload if explicit. Drives `f:reifiesLang`
        /// emission — required so the link `fold_slots_into_links` builds
        /// names the same `lang` the base flake carries via `flake.m.lang`.
        language: Option<String>,
    },
    /// Object is a triple term: the node naming its triple,
    /// `{"@id": s, p: o}`.
    TripleTerm(Value),
}

/// Reject the deferred JSON-LD wrapper shapes that can't carry an edge
/// annotation in v1: `@list`, `@set`, `@reverse`, `@variable`.
pub(crate) fn reject_deferred_object_wrappers(map: &Map<String, Value>) -> Result<()> {
    for key in ["@list", "@set", "@reverse", "@variable"] {
        if map.contains_key(key) {
            return Err(TransactError::UnsupportedFeature(format!(
                "edge annotations on '{key}' wrappers are deferred (v1)"
            )));
        }
    }
    Ok(())
}

/// Classify an object-position map (with `@annotation`/`@edge` already
/// stripped) into either a reference or a literal value-object.
///
/// Rules:
/// - `{"@id": "..."}` → `Iri`.
/// - `{"@value": "...", "@type": "@id"}` → `Iri` (JSON-LD identifier-
///   typed literal form). The `@value` payload must be a string.
/// - `{"@value": ..., ?"@type": ..., ?"@language": ...}` → `Literal`.
///   Any other key on the map (besides those three) is rejected. A
///   `@language` tag requires `@value` to be a string, and `@language`
///   may not co-occur with an explicit `@type` (per RDF: an annotated
///   string is either typed or language-tagged, never both).
/// - `{"@list": ...}`, `{"@set": ...}`, `{"@reverse": ...}`,
///   `{"@variable": ...}`, `{"@vocab": ...}` → deferred-shape rejection.
///
/// Wrapper rejection is performed up front via
/// [`reject_deferred_object_wrappers`]; do not rely on this function
/// alone for that check when constructing an `Iri` branch outside the
/// classifier.
pub(crate) fn classify_reified_object(map: &Map<String, Value>) -> Result<ReifiedObjectShape> {
    reject_deferred_object_wrappers(map)?;

    // Reject "@language without @value" — a meaningless value-object
    // shape that would otherwise leak through to the Ref branch and
    // leave the @language key on the base object after annotation
    // stripping. Routing through here also keeps callers from having
    // to repeat this guard.
    if map.contains_key("@language") && !map.contains_key("@value") {
        return Err(TransactError::Parse(
            "@language requires a sibling @value on annotated objects".to_string(),
        ));
    }

    if let Some(value_node) = map.get("@value") {
        let ty = map.get("@type").and_then(|t| t.as_str());
        if ty == Some("@id") {
            let Some(iri) = value_node.as_str() else {
                return Err(TransactError::Parse(
                    "@value with @type=@id must be a string IRI".to_string(),
                ));
            };
            if map.contains_key("@language") {
                return Err(TransactError::Parse(
                    "@language is invalid alongside @type=@id".to_string(),
                ));
            }
            // Whitelist: the identifier-typed value-object form may
            // carry only @value and @type. Stray keys (e.g. predicate
            // properties left over from a malformed payload) indicate
            // a deferred/ambiguous shape and are rejected here.
            for k in map.keys() {
                if k != "@value" && k != "@type" {
                    return Err(TransactError::UnsupportedFeature(format!(
                        "unexpected key '{k}' alongside @type=@id; \
                         only @value and @type are supported on identifier-typed value objects"
                    )));
                }
            }
            return Ok(ReifiedObjectShape::Iri(iri.to_string()));
        }
        if ty == Some("@vocab") {
            return Err(TransactError::UnsupportedFeature(
                "@type=@vocab on annotated objects is deferred (v1); \
                 use @id or expand to the resolved IRI"
                    .to_string(),
            ));
        }

        // Literal branch. Whitelist keys.
        for k in map.keys() {
            if k != "@value" && k != "@type" && k != "@language" {
                return Err(TransactError::UnsupportedFeature(format!(
                    "unexpected key '{k}' on annotated literal value object; \
                     only @value, @type, and @language are supported"
                )));
            }
        }
        let datatype = ty.map(String::from);
        let language = match map.get("@language") {
            None => None,
            Some(Value::String(s)) => {
                super::jsonld::check_lang_tag(s)?;
                Some(s.clone())
            }
            Some(other) => {
                return Err(TransactError::Parse(format!(
                    "@language must be a string, got {other}"
                )));
            }
        };

        // Per RDF / JSON-LD: an annotated literal carries either an
        // explicit datatype OR a language tag, never both. (A language-
        // tagged string's datatype is implicitly `rdf:langString`, but
        // it is never written alongside `@language` in JSON-LD.)
        if datatype.is_some() && language.is_some() {
            return Err(TransactError::Parse(
                "@language and @type cannot co-occur on a literal value object; \
                 use @language for language-tagged strings or @type for typed literals"
                    .to_string(),
            ));
        }

        // Language tags only apply to strings — emitting f:reifiesLang
        // for a non-string base flake would produce a bundle that
        // decodes to a different EdgeKey than the base flake's
        // EdgeKey::from_flake (which derives lang from FlakeMeta.lang,
        // populated only on string-shaped flakes).
        if language.is_some() && !value_node.is_string() {
            return Err(TransactError::Parse(format!(
                "@language requires @value to be a string, got {value_node}"
            )));
        }

        // Build a canonical value-object containing only whitelisted keys
        // (rather than cloning the input map). Insertion order keeps the
        // emitted JSON deterministic for snapshot-style tests.
        let mut canon = Map::new();
        canon.insert("@value".to_string(), value_node.clone());
        if let Some(t) = &datatype {
            canon.insert("@type".to_string(), json!(t));
        }
        if let Some(l) = &language {
            canon.insert("@language".to_string(), json!(l));
        }
        return Ok(ReifiedObjectShape::Literal {
            value: Value::Object(canon),
            language,
        });
    }

    if let Some(Value::String(id)) = map.get("@id") {
        return Ok(ReifiedObjectShape::Iri(id.clone()));
    }

    Err(TransactError::Parse(
        "annotated object must carry @id or @value".to_string(),
    ))
}

/// Build the JSON payload for `f:reifiesObject` plus the optional
/// `f:reifiesLang` companion, which [`fold_slots_into_links`] reads back
/// into the link's triple term.
///
/// Returns `(object_payload, lang)`:
/// - `Iri` → `({"@id": "..."}, None)`
/// - `Literal { language: Some(l), ... }` → `(value_object, Some(l))`
/// - `Literal { language: None, ... }` → `(value_object, None)`
pub(crate) fn emit_reifies_object_payload(shape: &ReifiedObjectShape) -> (Value, Option<String>) {
    match shape {
        ReifiedObjectShape::Iri(id) => (json!({ "@id": id }), None),
        ReifiedObjectShape::Literal {
            value, language, ..
        } => (value.clone(), language.clone()),
        ReifiedObjectShape::TripleTerm(node) => (json!({ "@id": node }), None),
    }
}

/// Label prefix of the blank nodes the lowering mints for anonymous
/// annotation subjects and objects. Reserved: the firewall refuses user
/// labels that start with it.
const ANN_BLANK_PREFIX: &str = "_:fluree_ann_";

/// Refuse a statement written under a reserved predicate (`f:reifies*` or
/// `rdf:reifies`), in an expanded document. Every key there is the IRI the
/// parser writes, resolved by the expander's own rules, so the check reads
/// exactly what would be stored. It runs on the document as written, before
/// the lowering adds the statements it synthesizes. A `@json` literal's
/// payload is opaque and is not read. A blank-node label in the prefix the
/// lowering reserves is refused too: that node would merge with an
/// annotation node.
pub(crate) fn refuse_reserved_iris(expanded: &Value) -> Result<()> {
    match expanded {
        Value::Object(map) => {
            let json_literal = map
                .get("@type")
                .and_then(Value::as_str)
                .is_some_and(|t| t == "@json" || t == rdf::JSON);
            for (k, v) in map {
                if k == "@value" && json_literal {
                    continue;
                }
                if k == "@id" {
                    if let Some(label) = v.as_str().filter(|s| s.starts_with(ANN_BLANK_PREFIX)) {
                        return Err(TransactError::Parse(format!(
                            "blank-node label {label:?} uses the prefix {ANN_BLANK_PREFIX:?}, \
                             which is reserved for edge-annotation nodes; choose another label"
                        )));
                    }
                }
                refuse_reserved_predicate(k)?;
                refuse_reserved_iris(v)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                refuse_reserved_iris(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Refuse `iri` as a predicate a user writes: `f:reifies*` and `rdf:reifies`
/// are written only through `@annotation` and `@reifies`.
pub(crate) fn refuse_reserved_predicate(iri: &str) -> Result<()> {
    if iri == rdf::REIFIES || reifies_iris::ALL.contains(&iri) {
        return Err(TransactError::UnsupportedFeature(format!(
            "'{iri}' is a system-controlled predicate; use @annotation or @reifies instead"
        )));
    }
    Ok(())
}

/// Lower `doc`'s annotations as a transaction does: expanded against its own
/// `@context` (each of an update's `insert`, `upsert` and `delete` clauses on
/// its own), checked by [`refuse_reserved_iris`], and lowered, in RDF mode.
/// `doc` becomes that lowered expansion; an update's clauses are replaced in
/// place, and a retraction's `where` patterns are appended to its `where`.
pub fn lower_edge_annotations(doc: &mut Value) -> Result<()> {
    let root = top_level_context(doc)?;
    let mut lowering = Lowering::new(false, 0);
    let Some(wrapper) = doc
        .as_object_mut()
        .filter(|map| is_transaction_wrapper(map))
    else {
        let mut expanded = expand_with_root_context(doc, &root, false)?;
        refuse_reserved_iris(&expanded)?;
        lower_expanded(&mut expanded, Clause::Assert, None, &mut lowering)?;
        *doc = expanded;
        return Ok(());
    };
    for (key, clause) in [
        ("insert", Clause::Assert),
        ("upsert", Clause::Assert),
        ("delete", Clause::Retract),
    ] {
        if let Some(value) = wrapper.get_mut(key) {
            let mut expanded = expand_with_context_policy(value, &root, false)?;
            refuse_reserved_iris(&expanded)?;
            lower_expanded(&mut expanded, clause, None, &mut lowering)?;
            *value = expanded;
        }
    }
    let patterns = lowering.into_where_patterns();
    if !patterns.is_empty() {
        let items = match wrapper.remove("where") {
            None => patterns,
            Some(Value::Array(mut items)) => {
                items.extend(patterns);
                items
            }
            Some(other) => std::iter::once(other).chain(patterns).collect(),
        };
        wrapper.insert("where".to_string(), Value::Array(items));
    }
    Ok(())
}

/// Parse the document's top-level `@context` once.
pub fn top_level_context(doc: &Value) -> Result<ParsedContext> {
    Ok(doc
        .as_object()
        .and_then(|m| m.get("@context"))
        .map(parse_context)
        .transpose()
        .map_err(|e| TransactError::Parse(format!("failed to parse @context: {e}")))?
        .unwrap_or_else(ParsedContext::new))
}

/// Cheap read-only check for whether `doc` contains any `@annotation` /
/// `@edge` / `@reifies` block (or `@ttl` / `@expiresAt` shorthand) for the
/// lowering to rewrite.
///
/// Lets the transactor skip the lowering walk for the common non-annotated
/// transaction. The reserved-predicate check is a separate concern and runs
/// on every document.
///
/// Opaque subtrees ([`opaque_subtree`]) don't count: a keyword that exists
/// only inside a `@json` document is not a block the lowering would rewrite.
/// This runs before the document's `@context` is parsed, so it recognizes
/// only the canonical `@type` spelling — an aliased one falls through and
/// opens the pass, which costs a walk and changes no answer.
pub fn document_has_annotation_keys(doc: &Value) -> bool {
    match doc {
        Value::Object(map) => {
            let opaque = opaque_subtree(map);
            map.iter().any(|(k, v)| {
                if opaque && k == "@value" {
                    return false;
                }
                is_annotation_key(k)
                    || is_ttl_key(k)
                    || k == REIFIES_KEY
                    || document_has_annotation_keys(v)
            })
        }
        Value::Array(items) => items.iter().any(document_has_annotation_keys),
        _ => false,
    }
}

/// True when `map`'s `@value` is a JSON-LD 1.1 `@json` literal written with
/// the canonical `@type` key: a JSON document carried verbatim, in which
/// nothing is a keyword this parser owns.
fn opaque_subtree(map: &Map<String, Value>) -> bool {
    map.contains_key("@value")
        && map
            .get("@type")
            .and_then(Value::as_str)
            .is_some_and(|t| t == "@json" || t == rdf::JSON)
}

/// Internal variable name prefix of a retraction by selector.
const SYNTH_VAR_PREFIX: &str = "?_fluree_del_ann_";

/// The smallest index past every `?_fluree_del_ann_N` the update's clauses
/// (`where`, `delete`, `insert`, `upsert`, `values`) write, so a retraction's
/// variable never joins one of the user's.
pub(crate) fn next_synth_var_index(obj: &Map<String, Value>) -> u32 {
    let mut max_seen: Option<u32> = None;
    for clause in super::CLAUSE_KEYS {
        if let Some(v) = obj.get(*clause) {
            scan_synth_var_usage(v, &mut max_seen);
        }
    }
    max_seen.map(|n| n + 1).unwrap_or(0)
}

fn scan_synth_var_usage(value: &Value, max_seen: &mut Option<u32>) {
    match value {
        Value::String(s) => {
            if let Some(tail) = s.strip_prefix(SYNTH_VAR_PREFIX) {
                if let Ok(n) = tail.parse::<u32>() {
                    *max_seen = Some(max_seen.map_or(n, |cur| cur.max(n)));
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                scan_synth_var_usage(item, max_seen);
            }
        }
        Value::Object(map) => {
            for v in map.values() {
                scan_synth_var_usage(v, max_seen);
            }
        }
        _ => {}
    }
}

/// True when `map` is a transaction wrapper (UPDATE / explicit
/// Insert+opts shape) rather than a node-map. The wrapper carries
/// clause keys ([`super::CLAUSE_KEYS`]) plus the optional `@context` and
/// any transactor-reserved keys ([`super::RESERVED_TXN_KEYS`] — `opts`,
/// `ledger`, `txn-meta`, `from`, `fromNamed`, `from-named`, `graph`), but
/// no predicates of its own. Each clause's value is a document of its own.
fn is_transaction_wrapper(map: &Map<String, Value>) -> bool {
    let has_clause = super::CLAUSE_KEYS.iter().any(|k| map.contains_key(*k));
    if !has_clause {
        return false;
    }
    map.keys().all(|k| {
        k == "@context"
            || super::CLAUSE_KEYS.contains(&k.as_str())
            || super::RESERVED_TXN_KEYS.contains(&k.as_str())
    })
}

/// Which statements a clause's annotations stand for: an update's `delete`
/// retracts them, every other clause asserts them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Clause {
    Assert,
    Retract,
}

/// What one transaction's lowering threads through its clauses: the labels
/// and variables it mints, and the `where` patterns its retractions by
/// selector add.
pub(crate) struct Lowering {
    /// When `true`, an empty `@annotation: {}` mints a property-less
    /// annotation node (LPG mode — a Cypher relationship keeps its identity
    /// without properties). When `false` (RDF default), an empty block is a
    /// no-op and inserts stay idempotent at the `(s, p, o)` level. See
    /// `docs/concepts/edge-annotations.md` "Empty annotation blocks".
    lpg_mode: bool,
    next_blank: usize,
    next_var: u32,
    where_patterns: Vec<Value>,
}

impl Lowering {
    /// `first_var`: the first index a retraction's variable may take
    /// ([`next_synth_var_index`]).
    pub(crate) fn new(lpg_mode: bool, first_var: u32) -> Self {
        Self {
            lpg_mode,
            next_blank: 0,
            next_var: first_var,
            where_patterns: Vec::new(),
        }
    }

    /// The `where` patterns that bind the retractions by selector.
    pub(crate) fn into_where_patterns(self) -> Vec<Value> {
        self.where_patterns
    }

    fn mint_blank(&mut self) -> String {
        let id = format!("{ANN_BLANK_PREFIX}{}", self.next_blank);
        self.next_blank += 1;
        id
    }

    fn mint_var(&mut self) -> String {
        let var = format!("{SYNTH_VAR_PREFIX}{}", self.next_var);
        self.next_var += 1;
        var
    }
}

/// Lower the annotations in `expanded`, a clause's expansion that
/// [`refuse_reserved_iris`] has read. Each annotation on a property's value
/// becomes a node naming the edge in `f:reifies*` slots, included in the node
/// the edge is written from, so it is written in the edge's scope; each
/// `@reifies` names its triple in slots on its own node. `graph` is the graph
/// the clause is written in (a `["graph", <name>, …]` item's name), which a
/// retraction's `where` pattern reads.
pub(crate) fn lower_expanded(
    expanded: &mut Value,
    clause: Clause,
    graph: Option<&str>,
    lowering: &mut Lowering,
) -> Result<()> {
    lower_nodes(expanded, Walk { clause, graph }, lowering)
}

/// Where the walk is: the clause, and the graph a retraction's `where`
/// pattern names (as written, for the `where` to resolve as the templates do).
#[derive(Clone, Copy)]
struct Walk<'g> {
    clause: Clause,
    graph: Option<&'g str>,
}

/// Nodes no property holds: the document, a graph's content, `@included`.
fn lower_nodes(value: &mut Value, walk: Walk<'_>, lowering: &mut Lowering) -> Result<()> {
    match value {
        Value::Array(items) => {
            for item in items {
                lower_nodes(item, walk, lowering)?;
            }
            Ok(())
        }
        Value::Object(map) => lower_node(map, walk, lowering),
        _ => Ok(()),
    }
}

/// Lower one expanded node object: its `@reifies`, the annotations on its
/// properties' values, and the nodes it holds. The annotations a property's
/// value carries are taken off it before the value is walked, so one left on
/// a node is on a node no property holds, and names no edge.
fn lower_node(map: &mut Map<String, Value>, walk: Walk<'_>, lowering: &mut Lowering) -> Result<()> {
    if let Some(key) = [ANNOTATION_KEY, EDGE_KEY]
        .into_iter()
        .find(|k| map.contains_key(*k))
    {
        return Err(TransactError::UnsupportedFeature(format!(
            "{key} as a top-level node property (without an enclosing predicate) \
             is the deferred unasserted-reifier shape (v1)"
        )));
    }
    // A literal: its `@value` is not JSON-LD (a `@json` one is a document of
    // its own).
    if map.contains_key("@value") {
        return Ok(());
    }
    if let Some(items) = map.get("@list") {
        return refuse_annotations_within(items, "a @list");
    }
    if let Some(Value::Object(term)) = map.get("@id") {
        refuse_annotations_within_map(term, "a triple term")?;
    }

    // The graph this node's statements are in: its own selector, else the
    // enclosing node's; a named graph's content is in the graph it names.
    let selector = match map.get("@graph") {
        Some(Value::String(raw)) => Some(raw.clone()),
        _ => None,
    };
    let walk = Walk {
        graph: selector.as_deref().or(walk.graph),
        ..walk
    };
    let named = match (map.get("@graph"), map.get("@id")) {
        (Some(Value::Array(_) | Value::Object(_)), Some(Value::String(id))) => Some(id.clone()),
        _ => None,
    };

    if let Some(reifies) = map.remove(REIFIES_KEY) {
        let reifier = node_id(map, lowering)?;
        lower_reifies_block(map, &reifier, reifies)?;
    }

    if let Some(nodes) = map.get_mut("@included") {
        lower_nodes(nodes, walk, lowering)?;
    }
    if let Some(content) = map.get_mut("@graph").filter(|g| !g.is_string()) {
        let content_walk = Walk {
            graph: named.as_deref().or(walk.graph),
            ..walk
        };
        lower_nodes(content, content_walk, lowering)?;
    }

    let mut included = Vec::new();
    let properties: Vec<String> = map
        .keys()
        .filter(|k| !k.starts_with('@'))
        .cloned()
        .collect();
    for property in properties {
        let end = edge_end(map, &property, walk.clause, lowering)?;
        let Some(Value::Array(values)) = map.get_mut(&property) else {
            continue;
        };
        let values = std::mem::take(values);
        let edge = Edge::Forward {
            subject: end.as_deref(),
            predicate: &property,
        };
        let kept = lower_values(values, edge, walk, lowering, &mut included)?;
        if kept.is_empty() {
            map.remove(&property);
        } else {
            map.insert(property, Value::Array(kept));
        }
    }

    // `@reverse`: properties whose values are the subjects of edges pointing
    // at this node.
    if let Some(Value::Object(reverse)) = map.get_mut("@reverse") {
        let mut reverse = std::mem::take(reverse);
        let annotated = reverse.values().any(values_carry_annotation);
        let end = if annotated {
            end_id(map, walk.clause, lowering)?
        } else {
            None
        };
        let properties: Vec<String> = reverse.keys().cloned().collect();
        for property in properties {
            let Some(Value::Array(values)) = reverse.get_mut(&property) else {
                continue;
            };
            let values = std::mem::take(values);
            let edge = Edge::Reverse {
                object: end.as_deref(),
                predicate: &property,
            };
            let kept = lower_values(values, edge, walk, lowering, &mut included)?;
            if kept.is_empty() {
                reverse.remove(&property);
            } else {
                reverse.insert(property, Value::Array(kept));
            }
        }
        if reverse.is_empty() {
            map.remove("@reverse");
        } else {
            map.insert("@reverse".to_string(), Value::Object(reverse));
        }
    }

    if !included.is_empty() {
        include_nodes(map, included);
    }
    Ok(())
}

/// The node's end of the edges `property` writes, when one of its values
/// carries an annotation: the node's `@id` (minted when an assertion needs
/// one). `None` when no value is annotated.
fn edge_end(
    map: &mut Map<String, Value>,
    property: &str,
    clause: Clause,
    lowering: &mut Lowering,
) -> Result<Option<String>> {
    if map.get(property).is_some_and(values_carry_annotation) {
        end_id(map, clause, lowering)
    } else {
        Ok(None)
    }
}

/// This node's `@id`, as an end of an annotated edge. An assertion mints a
/// blank node for a node without one; a retraction names the node it
/// retracts from, and gets `None` here.
fn end_id(
    map: &mut Map<String, Value>,
    clause: Clause,
    lowering: &mut Lowering,
) -> Result<Option<String>> {
    match clause {
        Clause::Assert => node_id(map, lowering).map(Some),
        Clause::Retract => Ok(map.get("@id").and_then(Value::as_str).map(String::from)),
    }
}

/// `map`'s `@id`, minting a blank node when it has none, so the annotation
/// and the parser name the same node.
fn node_id(map: &mut Map<String, Value>, lowering: &mut Lowering) -> Result<String> {
    match map.get("@id") {
        Some(Value::String(id)) => Ok(id.clone()),
        Some(_) => Err(TransactError::Parse(
            "an annotated edge's subject must be a node named by an IRI or a blank node"
                .to_string(),
        )),
        None => {
            let id = lowering.mint_blank();
            map.insert("@id".to_string(), json!(id));
            Ok(id)
        }
    }
}

/// Whether one of a property's expanded values carries an annotation or the
/// lifetime shorthand.
fn values_carry_annotation(values: &Value) -> bool {
    let carries = |v: &Value| {
        v.as_object()
            .is_some_and(|m| m.keys().any(|k| is_annotation_key(k) || is_ttl_key(k)))
    };
    match values {
        Value::Array(items) => items.iter().any(carries),
        other => carries(other),
    }
}

/// Which end of an annotated edge the walked node is.
#[derive(Clone, Copy)]
enum Edge<'a> {
    /// The node is the subject; each value is an edge's object.
    Forward {
        subject: Option<&'a str>,
        predicate: &'a str,
    },
    /// A reverse property: each value is an edge's subject, and the node is
    /// the object.
    Reverse {
        object: Option<&'a str>,
        predicate: &'a str,
    },
}

/// Lower the annotations on one property's values and walk the nodes among
/// them. Returns the values kept: a retraction takes its edge out, as a
/// retraction by annotation does not retract the edge.
fn lower_values(
    values: Vec<Value>,
    edge: Edge<'_>,
    walk: Walk<'_>,
    lowering: &mut Lowering,
    included: &mut Vec<Value>,
) -> Result<Vec<Value>> {
    let mut kept = Vec::with_capacity(values.len());
    for mut value in values {
        if !lower_value(&mut value, edge, walk, lowering, included)? {
            continue;
        }
        if let Value::Object(node) = &mut value {
            lower_node(node, walk, lowering)?;
        }
        kept.push(value);
    }
    Ok(kept)
}

/// Lower the annotation on `value`, one value of an edge: an assertion
/// includes an annotation node, a retraction a retract template (and, by
/// selector, a `where` pattern). Returns whether the value stays.
fn lower_value(
    value: &mut Value,
    edge: Edge<'_>,
    walk: Walk<'_>,
    lowering: &mut Lowering,
    included: &mut Vec<Value>,
) -> Result<bool> {
    let Value::Object(map) = value else {
        return Ok(true);
    };
    if map.contains_key(ANNOTATION_KEY) && map.contains_key(EDGE_KEY) {
        return Err(TransactError::Parse(
            "edge annotation: cannot specify both @annotation and @edge on the same object"
                .to_string(),
        ));
    }
    match walk.clause {
        Clause::Assert => fold_ttl_shorthand(map)?,
        Clause::Retract if map.keys().any(|k| is_ttl_key(k)) => {
            return Err(TransactError::Parse(
                "@ttl / @expiresAt set the lifetime of an inserted value; they do not \
                 apply in a delete"
                    .to_string(),
            ))
        }
        Clause::Retract => {}
    }
    let Some(blocks) = map.remove(ANNOTATION_KEY).or_else(|| map.remove(EDGE_KEY)) else {
        return Ok(true);
    };
    let (node_end, predicate) = match edge {
        Edge::Forward { subject, predicate } => (subject, predicate),
        Edge::Reverse { object, predicate } => (object, predicate),
    };
    let Some(node_end) = node_end else {
        return Err(TransactError::Parse(
            "delete-clause @annotation requires the enclosing predicate to have a parent @id \
             (the subject of the reified edge)"
                .to_string(),
        ));
    };
    reject_deferred_object_wrappers(map)?;

    // `@language` without `@value` reaches the classifier, which refuses it.
    let literal = map.contains_key("@value") || map.contains_key("@language");
    let (subject, object) = match edge {
        Edge::Forward { .. } => {
            let object = if literal {
                classify_reified_object(map)?
            } else if let Some(term @ Value::Object(_)) = map.get("@id") {
                ReifiedObjectShape::TripleTerm(term.clone())
            } else {
                ReifiedObjectShape::Iri(value_id(map, walk.clause, lowering)?)
            };
            (node_end.to_string(), object)
        }
        Edge::Reverse { .. } => {
            if literal {
                return Err(TransactError::Parse(format!(
                    "a reverse property's value is a node, the subject of the edge; \
                     '{predicate}' has a literal value"
                )));
            }
            let subject = value_id(map, walk.clause, lowering)?;
            (subject, ReifiedObjectShape::Iri(node_end.to_string()))
        }
    };
    let blocks = match blocks {
        Value::Array(blocks) => blocks,
        block => vec![block],
    };
    let edge = (subject.as_str(), predicate, &object);
    match walk.clause {
        Clause::Assert => {
            for block in blocks {
                if let Some(node) = annotation_node(edge, block, lowering)? {
                    included.push(node);
                }
            }
            Ok(true)
        }
        Clause::Retract => {
            for block in blocks {
                included.push(retraction(edge, block, walk.graph, lowering)?);
            }
            Ok(false)
        }
    }
}

/// The `@id` of a value that is an annotated edge's node end. An assertion
/// mints one for a node without it; a retraction must name it.
fn value_id(
    map: &mut Map<String, Value>,
    clause: Clause,
    lowering: &mut Lowering,
) -> Result<String> {
    if clause == Clause::Retract && !map.contains_key("@id") {
        return Err(TransactError::Parse(
            "delete-clause @annotation requires the parent object node to carry an explicit \
             @id (the object of the reified edge)"
                .to_string(),
        ));
    }
    node_id(map, lowering)
}

/// An annotated edge: subject, predicate and object.
type AnnotatedEdge<'a> = (&'a str, &'a str, &'a ReifiedObjectShape);

/// The `f:reifies*` slots naming `edge`, in expanded form.
fn edge_slots((subject, predicate, object): AnnotatedEdge<'_>) -> Map<String, Value> {
    let mut slots = Map::new();
    slots.insert(
        reifies_iris::SUBJECT.to_string(),
        json!([{ "@id": subject }]),
    );
    slots.insert(
        reifies_iris::PREDICATE.to_string(),
        json!([{ "@id": predicate }]),
    );
    let (object, lang) = emit_reifies_object_payload(object);
    slots.insert(reifies_iris::OBJECT.to_string(), json!([object]));
    // `fold_slots_into_links` takes the term's language from this slot.
    if let Some(lang) = lang {
        slots.insert(reifies_iris::LANG.to_string(), json!([{ "@value": lang }]));
    }
    slots
}

/// The node for one annotation of `edge`: its `@id` (the reifier, minted
/// when the annotation has none), the edge's slots, and the annotation's
/// properties. `None` for an empty annotation in RDF mode.
fn annotation_node(
    edge: AnnotatedEdge<'_>,
    block: Value,
    lowering: &mut Lowering,
) -> Result<Option<Value>> {
    let mut node = annotation_body(block)?;
    if node.is_empty() && !lowering.lpg_mode {
        return Ok(None);
    }
    refuse_annotations_in_body(&node)?;
    let id = match node.get("@id") {
        Some(Value::String(id)) => id.clone(),
        Some(_) => {
            return Err(TransactError::Parse(
                "an annotation's @id must name a node".to_string(),
            ))
        }
        None => lowering.mint_blank(),
    };
    node.insert("@id".to_string(), json!(id));
    node.extend(edge_slots(edge));
    Ok(Some(Value::Object(node)))
}

/// The retract template for one annotation of `edge` in a `delete`: by the
/// annotation's `@id`, or, without one, by a variable that a `where` pattern
/// binds to each annotation of the edge whose properties match the body's.
fn retraction(
    edge: AnnotatedEdge<'_>,
    block: Value,
    graph: Option<&str>,
    lowering: &mut Lowering,
) -> Result<Value> {
    let mut body = annotation_body(block)?;
    let id = match body.remove("@id") {
        Some(Value::String(id)) => {
            // A client's blank-node label is minted fresh at insert time and
            // names no stored node; a stable `_:fdb-…` id from a query does.
            if id.starts_with("_:") && crate::namespace::stable_blank_node_sid(&id).is_none() {
                return Err(TransactError::UnsupportedFeature(
                    "delete by anonymous annotation @id is not supported — client blank-node \
                     labels are minted fresh at insert time and not user-addressable. Use the \
                     stable `_:fdb-...` id returned by a query, a selector form (no @id, body \
                     properties only), or attach an explicit @id at insert."
                        .to_string(),
                ));
            }
            id
        }
        Some(_) => {
            return Err(TransactError::Parse(
                "an annotation's @id must name a node".to_string(),
            ))
        }
        None => {
            refuse_annotations_in_body(&body)?;
            let var = lowering.mint_var();
            let pattern = selector_pattern(&var, &body, edge, graph);
            lowering.where_patterns.push(pattern);
            var
        }
    };
    let mut template = edge_slots(edge);
    template.insert("@id".to_string(), json!(id));
    Ok(Value::Object(template))
}

/// The `where` node a retraction by selector binds `var` with: the body's
/// properties, and the query-side `@reifies` naming the edge, in the edge's
/// graph.
fn selector_pattern(
    var: &str,
    body: &Map<String, Value>,
    (subject, predicate, object): AnnotatedEdge<'_>,
    graph: Option<&str>,
) -> Value {
    let mut node: Map<String, Value> = body
        .iter()
        .map(|(k, v)| (k.clone(), where_value(v)))
        .collect();
    node.insert("@id".to_string(), json!(var));
    let (object, _) = emit_reifies_object_payload(object);
    node.insert(
        REIFIES_KEY.to_string(),
        json!({ "@id": subject, predicate: object }),
    );
    if let Some(graph) = graph {
        node.insert("@graph".to_string(), json!(graph));
    }
    Value::Object(node)
}

/// An expanded value as a `where` reads it: a property's one value bare (the
/// `where` takes no array of values), and a plain literal as its scalar,
/// which the `where` reads as the document wrote it (a `?var` included).
fn where_value(value: &Value) -> Value {
    match value {
        Value::Array(items) if items.len() == 1 => where_value(&items[0]),
        Value::Array(items) => Value::Array(items.iter().map(where_value).collect()),
        Value::Object(map) => {
            if let (1, Some(scalar)) = (map.len(), map.get("@value")) {
                if !scalar.is_object() && !scalar.is_array() {
                    return scalar.clone();
                }
            }
            if map.contains_key("@value") || map.contains_key("@list") {
                return value.clone();
            }
            Value::Object(
                map.iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            if k == "@id" {
                                v.clone()
                            } else {
                                where_value(v)
                            },
                        )
                    })
                    .collect(),
            )
        }
        other => other.clone(),
    }
}

/// An `@annotation` value, which describes a node.
fn annotation_body(block: Value) -> Result<Map<String, Value>> {
    match block {
        Value::Object(map) if !map.contains_key("@value") => Ok(map),
        _ => Err(TransactError::Parse(
            "@annotation value must be a JSON object describing the annotation subject".to_string(),
        )),
    }
}

/// Refuse an annotation keyword in an annotation's body: annotation of an
/// annotation, and a triple term nested in one, are deferred (v1).
fn refuse_annotations_in_body(body: &Map<String, Value>) -> Result<()> {
    find_annotation_keyword(body).map_or(Ok(()), |k| {
        Err(TransactError::UnsupportedFeature(if k == REIFIES_KEY {
            "@reifies nested inside an @annotation body is the deferred nested-triple-term \
             shape (v1)"
                .to_string()
        } else {
            format!(
                "{k} nested inside an @annotation body is the deferred \
                 annotation-of-annotation shape (v1)"
            )
        }))
    })
}

/// Refuse an annotation keyword anywhere in `value`, which is `place` (a
/// list, a triple term): no statement in it can be annotated in v1.
fn refuse_annotations_within(value: &Value, place: &str) -> Result<()> {
    match value {
        Value::Object(map) => refuse_annotations_within_map(map, place),
        Value::Array(items) => items
            .iter()
            .try_for_each(|item| refuse_annotations_within(item, place)),
        _ => Ok(()),
    }
}

fn refuse_annotations_within_map(map: &Map<String, Value>, place: &str) -> Result<()> {
    find_annotation_keyword(map).map_or(Ok(()), |k| {
        Err(TransactError::UnsupportedFeature(format!(
            "{k} inside {place} is deferred (v1): edge annotations name statements of a node"
        )))
    })
}

/// The first annotation keyword (`@annotation`, `@edge`, `@reifies`) in the
/// expanded `map` or the nodes it holds. A literal's `@value` is not read.
fn find_annotation_keyword(map: &Map<String, Value>) -> Option<&'static str> {
    for (k, v) in map {
        if let Some(keyword) = [ANNOTATION_KEY, EDGE_KEY, REIFIES_KEY]
            .into_iter()
            .find(|keyword| k == keyword)
        {
            return Some(keyword);
        }
        if k == "@value" {
            continue;
        }
        let found = match v {
            Value::Object(inner) => find_annotation_keyword(inner),
            Value::Array(items) => items.iter().find_map(|item| match item {
                Value::Object(inner) => find_annotation_keyword(inner),
                _ => None,
            }),
            _ => None,
        };
        if found.is_some() {
            return found;
        }
    }
    None
}

/// Add `nodes` to `map`'s `@included`, keeping any nodes already there.
fn include_nodes(map: &mut Map<String, Value>, nodes: Vec<Value>) {
    match map.get_mut("@included") {
        Some(Value::Array(existing)) => existing.extend(nodes),
        Some(single) => {
            let node = std::mem::take(single);
            *single = Value::Array(std::iter::once(node).chain(nodes).collect());
        }
        None => {
            map.insert("@included".to_string(), Value::Array(nodes));
        }
    }
}

/// Lower a `@reifies` value on the node `reifier`: the node is a reifier,
/// and each triple named becomes its `rdf:reifies` link without being
/// asserted (RDF 1.2). The first triple's slots ride on the node, each other
/// one's on a node with the same `@id` included in it, so in its scope.
fn lower_reifies_block(map: &mut Map<String, Value>, reifier: &str, reifies: Value) -> Result<()> {
    let blocks = match reifies {
        Value::Array(blocks) if !blocks.is_empty() => blocks,
        Value::Array(_) => {
            return Err(TransactError::Parse(
                "@reifies must describe at least one triple".to_string(),
            ))
        }
        block => vec![block],
    };
    let mut included = Vec::new();
    for (i, block) in blocks.iter().enumerate() {
        let slots = reifies_slots(block)?;
        if i == 0 {
            map.extend(slots);
        } else {
            let mut node = slots;
            node.insert("@id".to_string(), json!(reifier));
            included.push(Value::Object(node));
        }
    }
    if !included.is_empty() {
        include_nodes(map, included);
    }
    Ok(())
}

/// The `f:reifies*` slots naming the one triple an expanded `@reifies` value
/// describes: `{"@id": s, p: [o]}`.
fn reifies_slots(block: &Value) -> Result<Map<String, Value>> {
    let Value::Object(block) = block else {
        return Err(TransactError::Parse(
            "@reifies value must be a JSON object describing a triple".to_string(),
        ));
    };
    if let Some(k) = find_annotation_keyword(block) {
        return Err(TransactError::UnsupportedFeature(format!(
            "{k} inside @reifies is the deferred nested-triple-term shape"
        )));
    }
    let Some(Value::String(subject)) = block.get("@id") else {
        return Err(TransactError::Parse(
            "@reifies must include an @id naming the triple's subject".to_string(),
        ));
    };
    let pairs: Vec<(&String, &Value)> = block.iter().filter(|(k, _)| !k.starts_with('@')).collect();
    let [(predicate, objects)] = pairs.as_slice() else {
        return Err(TransactError::Parse(format!(
            "@reifies must describe exactly one triple (got {} predicates); \
             use an array to reify several",
            pairs.len()
        )));
    };
    let object = match objects {
        Value::Array(items) if items.len() == 1 => &items[0],
        Value::Array(_) => {
            return Err(TransactError::Parse(
                "@reifies must describe exactly one triple; use an array of @reifies \
                 blocks to reify several"
                    .to_string(),
            ))
        }
        object => object,
    };
    let shape = match object {
        Value::Object(object)
            if object.contains_key("@value") || object.contains_key("@language") =>
        {
            classify_reified_object(object)?
        }
        Value::Object(object) if object.len() == 1 => match object.get("@id") {
            Some(Value::String(id)) => ReifiedObjectShape::Iri(id.clone()),
            Some(term @ Value::Object(_)) => ReifiedObjectShape::TripleTerm(term.clone()),
            _ => {
                return Err(TransactError::Parse(
                    "@reifies object must be an @id reference or a value".to_string(),
                ))
            }
        },
        _ => {
            return Err(TransactError::Parse(
                "@reifies object must be an @id reference or a value".to_string(),
            ))
        }
    };
    Ok(edge_slots((subject, predicate, &shape)))
}

/// Replace each annotation's lowered `f:reifies*` slot templates with its
/// `rdf:reifies` link. The slots are this module's intermediate form: they
/// carry the edge through JSON-LD expansion, which has no triple-term syntax.
/// Each annotation sets each slot once, in its own node, so a slot set again
/// for the same reifier starts that reifier's next link.
pub(crate) fn fold_slots_into_links(templates: &mut Vec<crate::ir::TripleTemplate>) -> Result<()> {
    use crate::ir::{TemplateGraph, TemplateTerm, TemplateTripleTerm, TripleTemplate};
    use fluree_db_core::{DatatypeConstraint, FlakeValue};
    use fluree_vocab::db;

    #[derive(Default)]
    struct Slots {
        s: Option<TemplateTerm>,
        p: Option<TemplateTerm>,
        o: Option<(TemplateTerm, Option<DatatypeConstraint>)>,
        lang: Option<String>,
    }
    impl Slots {
        fn has(&self, slot: &str) -> bool {
            match slot {
                db::REIFIES_SUBJECT => self.s.is_some(),
                db::REIFIES_PREDICATE => self.p.is_some(),
                db::REIFIES_OBJECT => self.o.is_some(),
                db::REIFIES_LANG => self.lang.is_some(),
                _ => false,
            }
        }
    }
    let key = |t: &TripleTemplate| -> Option<(TemplateGraph, String)> {
        let subject = match &t.subject {
            TemplateTerm::Var(v) => format!("?{v:?}"),
            TemplateTerm::Sid(sid) => format!("<{sid}>"),
            TemplateTerm::BlankNode(label) => label.clone(),
            _ => return None,
        };
        Some((t.graph.clone(), subject))
    };

    let mut reifiers: Vec<((TemplateGraph, String), TripleTemplate, Slots)> = Vec::new();
    let mut kept = Vec::with_capacity(templates.len());
    for t in templates.drain(..) {
        let slot = match &t.predicate {
            TemplateTerm::Sid(p) if fluree_db_core::is_reserved_reifies_predicate(p) => {
                p.name.to_string()
            }
            _ => {
                kept.push(t);
                continue;
            }
        };
        let k = key(&t).ok_or_else(|| {
            TransactError::Parse("an annotation's reifier must be a node".to_string())
        })?;
        let open = reifiers
            .iter()
            .rposition(|(rk, _, slots)| *rk == k && !slots.has(&slot));
        let at = match open {
            Some(at) => at,
            None => {
                reifiers.push((k, t.clone(), Slots::default()));
                reifiers.len() - 1
            }
        };
        let slots = &mut reifiers[at].2;
        match slot.as_str() {
            db::REIFIES_SUBJECT => slots.s = Some(t.object),
            db::REIFIES_PREDICATE => slots.p = Some(t.object),
            db::REIFIES_OBJECT => slots.o = Some((t.object, t.dtc)),
            db::REIFIES_LANG => {
                if let TemplateTerm::Value(FlakeValue::String(lang)) = t.object {
                    slots.lang = Some(lang);
                }
            }
            // The link lives in its template's graph and names the object's
            // datatype itself.
            db::REIFIES_GRAPH | db::REIFIES_DATATYPE => {}
            other => {
                return Err(TransactError::UnsupportedFeature(format!(
                    "f:{other} is not supported on an annotation"
                )))
            }
        }
    }

    for (_, first, slots) in reifiers {
        let (Some(s), Some(p), Some((o, dtc))) = (slots.s, slots.p, slots.o) else {
            return Err(TransactError::Parse(
                "an annotation must name its triple's subject, predicate and object".to_string(),
            ));
        };
        let dtc = match slots.lang {
            Some(lang) => Some(DatatypeConstraint::LangTag(std::sync::Arc::from(lang))),
            None => dtc,
        };
        kept.push(TripleTemplate {
            predicate: TemplateTerm::Sid(fluree_db_core::rdf_reifies_sid().clone()),
            object: TemplateTerm::TripleTerm(Box::new(TemplateTripleTerm { s, p, o, dtc })),
            dtc: None,
            list_index: None,
            ..first
        });
    }
    *templates = kept;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Lower `doc` as a transaction's data: its lowered expansion.
    fn lower(mut doc: Value) -> Result<Value> {
        lower_edge_annotations(&mut doc)?;
        Ok(doc)
    }

    /// The one value of `node`'s expanded `key`.
    fn one<'a>(node: &'a Value, key: &str) -> &'a Value {
        match node.get(key).and_then(Value::as_array).map(Vec::as_slice) {
            Some([value]) => value,
            _ => panic!("one value of {key} in {node:#}"),
        }
    }

    /// The annotation nodes the lowering included in `node` (the edge's
    /// subject).
    fn included(node: &Value) -> &Vec<Value> {
        node.get("@included")
            .and_then(Value::as_array)
            .expect("annotation nodes are included in the edge's subject")
    }

    // ---- Reserved-key sync guard ----------------------------------------

    #[test]
    fn every_reserved_key_tolerated_by_envelope_and_wrapper() {
        // Pins the invariant that the structural detectors stay in lock-step
        // with the canonical reserved-key set. A reserved key riding alongside
        // `@graph` (envelope) or a clause (wrapper) must NOT demote the doc to
        // a node-map, or annotation lowering would skip the data and silently
        // drop reifications.
        for key in super::super::RESERVED_TXN_KEYS {
            let mut envelope = obj_map(json!({ "@graph": [] }));
            envelope.insert((*key).to_string(), json!("x"));
            assert_eq!(
                fluree_graph_json_ld::doc_shape(&envelope).unwrap(),
                fluree_graph_json_ld::DocShape::Envelope,
                "an envelope must tolerate reserved key {key:?}"
            );

            // Use a clause key the reserved key never collides with.
            let mut wrapper = obj_map(json!({ "insert": {"@id": "ex:a", "ex:p": "v"} }));
            if *key != "insert" {
                wrapper.insert((*key).to_string(), json!("x"));
            }
            assert!(
                is_transaction_wrapper(&wrapper),
                "is_transaction_wrapper must tolerate reserved key {key:?}"
            );
        }
    }

    // ---- ReifiedObjectShape classifier ----------------------------------

    fn obj_map(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => panic!("expected JSON object"),
        }
    }

    #[test]
    fn classify_iri_via_at_id() {
        let m = obj_map(json!({"@id": "ex:acme"}));
        let shape = classify_reified_object(&m).unwrap();
        assert!(matches!(shape, ReifiedObjectShape::Iri(ref s) if s == "ex:acme"));
    }

    #[test]
    fn classify_iri_via_value_typed_id() {
        let m = obj_map(json!({"@value": "ex:acme", "@type": "@id"}));
        let shape = classify_reified_object(&m).unwrap();
        assert!(matches!(shape, ReifiedObjectShape::Iri(ref s) if s == "ex:acme"));
    }

    #[test]
    fn classify_iri_typed_id_rejects_non_string_value() {
        let m = obj_map(json!({"@value": 42, "@type": "@id"}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(err.to_string().contains("@value with @type=@id"));
    }

    #[test]
    fn classify_iri_typed_id_rejects_with_language() {
        let m = obj_map(json!({"@value": "ex:acme", "@type": "@id", "@language": "en"}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(err
            .to_string()
            .contains("@language is invalid alongside @type=@id"));
    }

    #[test]
    fn classify_plain_string_literal() {
        let m = obj_map(json!({"@value": "Alice"}));
        let shape = classify_reified_object(&m).unwrap();
        match shape {
            ReifiedObjectShape::Literal { value, language } => {
                assert_eq!(value, json!({"@value": "Alice"}));
                assert!(language.is_none());
            }
            _ => panic!("expected Literal"),
        }
    }

    #[test]
    fn classify_typed_literal() {
        let m = obj_map(json!({"@value": "2024-01-01", "@type": "xsd:date"}));
        let shape = classify_reified_object(&m).unwrap();
        match shape {
            ReifiedObjectShape::Literal { value, language } => {
                assert_eq!(value, json!({"@value": "2024-01-01", "@type": "xsd:date"}));
                assert!(language.is_none());
                // Datatype is derivable from value when needed:
                assert_eq!(
                    value.get("@type").and_then(|v| v.as_str()),
                    Some("xsd:date")
                );
            }
            _ => panic!("expected Literal"),
        }
    }

    #[test]
    fn classify_language_tagged_literal() {
        let m = obj_map(json!({"@value": "chat", "@language": "fr"}));
        let shape = classify_reified_object(&m).unwrap();
        match shape {
            ReifiedObjectShape::Literal { value, language } => {
                assert_eq!(value, json!({"@value": "chat", "@language": "fr"}));
                assert_eq!(language.as_deref(), Some("fr"));
            }
            _ => panic!("expected Literal"),
        }
    }

    #[test]
    fn classify_rejects_language_with_non_string_value() {
        // RDF: language tags only apply to strings. Emitting f:reifiesLang
        // for a numeric base flake would break the EdgeKey round-trip.
        let m = obj_map(json!({"@value": 42, "@language": "en"}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(
            err.to_string()
                .contains("@language requires @value to be a string"),
            "got: {err}"
        );
    }

    #[test]
    fn classify_rejects_language_plus_type_combo() {
        // JSON-LD: @language and @type cannot co-occur — a literal is
        // either typed or language-tagged.
        let m = obj_map(json!({
            "@value": "Alice",
            "@type": "xsd:string",
            "@language": "en"
        }));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(
            err.to_string()
                .contains("@language and @type cannot co-occur"),
            "got: {err}"
        );
    }

    #[test]
    fn classify_rejects_non_string_language_tag() {
        let m = obj_map(json!({"@value": "x", "@language": 7}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(err.to_string().contains("@language must be a string"));
    }

    #[test]
    fn classify_rejects_language_without_value() {
        // A meaningless value-object shape: `@language` requires a
        // sibling `@value`. Without this guard, the caller's Ref
        // dispatch would accept the map and silently drop @language.
        let m = obj_map(json!({"@id": "ex:acme", "@language": "en"}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(
            err.to_string()
                .contains("@language requires a sibling @value"),
            "got: {err}"
        );
    }

    #[test]
    fn classify_typed_id_rejects_stray_key() {
        // Identifier-typed value-object form may only carry @value and
        // @type. Extra predicate-shaped keys indicate an ambiguous /
        // deferred shape.
        let m = obj_map(json!({
            "@value": "ex:acme",
            "@type": "@id",
            "ex:bogus": "x"
        }));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(
            err.to_string()
                .contains("unexpected key 'ex:bogus' alongside @type=@id"),
            "got: {err}"
        );
    }

    /// An annotated value object is taken as written, as an unannotated one
    /// is: neither its term's datatype or language nor the default language
    /// reaches it, so the reified bundle names the same literal as the edge.
    #[test]
    fn annotated_literal_is_taken_as_written() {
        let contexts = [
            json!({"ex": "http://example.org/", "label": {"@id": "ex:label", "@type": "http://www.w3.org/2001/XMLSchema#date"}}),
            json!({"ex": "http://example.org/", "@language": "fr", "label": "ex:label"}),
            json!({"ex": "http://example.org/", "@language": "fr", "label": {"@id": "ex:label", "@language": null}}),
            json!({"ex": "http://example.org/", "@language": "fr", "label": {"@id": "ex:label", "@language": "de"}}),
            json!({"ex": "http://example.org/", "@language": "fr", "label": {"@id": "ex:label", "@type": "@id"}}),
            json!({"ex": "http://example.org/", "@language": "fr", "label": {"@id": "ex:label", "@type": "@vocab"}}),
        ];
        for ctx in contexts {
            let doc = json!({
                "@context": ctx,
                "@id": "ex:alice",
                "label": {"@value": "2024-01-01", "@annotation": {"ex:source": "hr"}}
            });
            let result = lower(doc).unwrap_or_else(|e| panic!("{ctx}: {e}"));
            let sibling = &included(&result)[0];
            assert_eq!(
                one(sibling, reifies_iris::OBJECT),
                &json!({"@value": "2024-01-01"}),
                "{ctx}"
            );
            assert!(sibling.get(reifies_iris::LANG).is_none(), "{ctx}");
        }
    }

    #[test]
    fn an_annotated_literal_keeps_its_own_datatype() {
        let doc = json!({
            "@context": {
                "ex": "http://example.org/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "joinedAt": { "@id": "ex:joinedAt", "@type": "xsd:date" }
            },
            "@id": "ex:alice",
            "joinedAt": {
                "@value": "2024-01-01",
                "@type": "xsd:date",
                "@annotation": { "ex:source": "hr" }
            }
        });
        let result = lower(doc).unwrap();
        let sibling = &included(&result)[0];
        assert_eq!(
            one(sibling, reifies_iris::OBJECT),
            &json!({"@value": "2024-01-01", "@type": "http://www.w3.org/2001/XMLSchema#date"})
        );
    }

    /// A `@json` term makes its whole value one JSON literal, as JSON-LD
    /// expands it: an `@annotation` written in that value is part of the
    /// document, not an annotation.
    #[test]
    fn a_json_term_holds_its_whole_value_as_one_literal() {
        let doc = json!({
            "@context": {
                "ex": "http://example.org/",
                "doc": { "@id": "ex:doc", "@type": "@json" }
            },
            "@id": "ex:alice",
            "doc": {"@value": "x", "@annotation": { "ex:source": "hr" }}
        });
        let result = lower(doc).unwrap();
        assert!(result.get("@included").is_none(), "{result:#}");
        assert_eq!(
            one(&result, "http://example.org/doc"),
            &json!({
                "@value": {"@value": "x", "@annotation": {"ex:source": "hr"}},
                "@type": "@json"
            })
        );
    }

    #[test]
    fn a_node_local_id_alias_names_the_subject() {
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@graph": [{
                "@context": { "id": "@id" },
                "id": "ex:alice",
                "ex:worksFor": {
                    "id": "ex:acme",
                    "@annotation": { "ex:role": "Engineer" }
                }
            }]
        });
        let result = lower(doc).unwrap();
        let sibling = &included(&result[0])[0];
        assert_eq!(
            one(sibling, reifies_iris::SUBJECT),
            &json!({"@id": "http://example.org/alice"})
        );
        assert_eq!(
            one(sibling, reifies_iris::OBJECT),
            &json!({"@id": "http://example.org/acme"})
        );
    }

    #[test]
    fn accepts_value_form_annotation_under_id_typed_term() {
        // `@type: "@id"` term coercion applies to strings only, so the
        // value object stays a literal on the base edge, matching the
        // literal f:reifiesObject the lowering writes.
        let doc = json!({
            "@context": {
                "ex": "http://example.org/",
                "ref": { "@id": "ex:ref", "@type": "@id" }
            },
            "@id": "ex:alice",
            "ref": {
                "@value": "ex:bob",
                "@annotation": { "ex:source": "x" }
            }
        });
        let result = lower(doc).expect("a value object under an @id term is a literal");
        let sibling = &included(&result)[0];
        assert_eq!(
            one(sibling, reifies_iris::OBJECT),
            &json!({"@value": "ex:bob"})
        );
    }

    #[test]
    fn intercept_rejects_language_without_value_on_object() {
        // An object carrying `@language` without `@value` must not be read
        // as a reference with a minted id.
        let doc = json!({
            "@id": "ex:alice",
            "ex:label": {
                "@id": "ex:acme",
                "@language": "en",
                "@annotation": { "ex:source": "hr" }
            }
        });
        let err = lower(doc).unwrap_err();
        assert!(
            err.to_string()
                .contains("@language requires a sibling @value"),
            "got: {err}"
        );
    }

    #[test]
    fn classify_rejects_list_wrapper() {
        let m = obj_map(json!({"@list": [1, 2, 3], "@annotation": {"ex:k": "v"}}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(err.to_string().contains("'@list'"));
    }

    #[test]
    fn classify_rejects_set_wrapper() {
        let m = obj_map(json!({"@set": [1, 2]}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(err.to_string().contains("'@set'"));
    }

    #[test]
    fn classify_rejects_reverse_wrapper() {
        let m = obj_map(json!({"@reverse": {"ex:p": "x"}}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(err.to_string().contains("'@reverse'"));
    }

    #[test]
    fn classify_rejects_variable_wrapper() {
        let m = obj_map(json!({"@variable": "v"}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(err.to_string().contains("'@variable'"));
    }

    #[test]
    fn classify_rejects_vocab_typed_value() {
        let m = obj_map(json!({"@value": "term", "@type": "@vocab"}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(err.to_string().contains("@type=@vocab"));
    }

    #[test]
    fn classify_rejects_stray_key_on_literal() {
        let m = obj_map(json!({"@value": "Alice", "ex:bogus": "x"}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(err.to_string().contains("unexpected key 'ex:bogus'"));
    }

    #[test]
    fn classify_rejects_empty_object() {
        let m = obj_map(json!({}));
        let err = classify_reified_object(&m).unwrap_err();
        assert!(err.to_string().contains("@id or @value"));
    }

    #[test]
    fn emit_payload_iri() {
        let (payload, lang) =
            emit_reifies_object_payload(&ReifiedObjectShape::Iri("ex:acme".to_string()));
        assert_eq!(payload, json!({"@id": "ex:acme"}));
        assert!(lang.is_none());
    }

    #[test]
    fn emit_payload_plain_literal() {
        let shape = classify_reified_object(&obj_map(json!({"@value": "Alice"}))).unwrap();
        let (payload, lang) = emit_reifies_object_payload(&shape);
        assert_eq!(payload, json!({"@value": "Alice"}));
        assert!(lang.is_none());
    }

    #[test]
    fn emit_payload_typed_literal() {
        let shape = classify_reified_object(&obj_map(
            json!({"@value": "2024-01-01", "@type": "xsd:date"}),
        ))
        .unwrap();
        let (payload, lang) = emit_reifies_object_payload(&shape);
        assert_eq!(
            payload,
            json!({"@value": "2024-01-01", "@type": "xsd:date"})
        );
        assert!(lang.is_none());
    }

    #[test]
    fn emit_payload_lang_tagged_literal() {
        let shape = classify_reified_object(&obj_map(json!({"@value": "chat", "@language": "fr"})))
            .unwrap();
        let (payload, lang) = emit_reifies_object_payload(&shape);
        assert_eq!(payload, json!({"@value": "chat", "@language": "fr"}));
        assert_eq!(lang.as_deref(), Some("fr"));
    }

    #[test]
    fn classify_canonicalizes_value_object_key_order_typed() {
        // Canonical form preserves insertion order @value → @type
        // regardless of input order. (The lang variant gets its own
        // assertion below; the two forms can't co-occur.)
        let shape = classify_reified_object(&obj_map(json!({
            "@type": "xsd:date",
            "@value": "2024-01-01",
        })))
        .unwrap();
        match shape {
            ReifiedObjectShape::Literal { value, .. } => {
                let m = value.as_object().unwrap();
                let keys: Vec<&str> = m.keys().map(String::as_str).collect();
                assert_eq!(keys, vec!["@value", "@type"]);
            }
            _ => panic!("expected Literal"),
        }
    }

    #[test]
    fn classify_canonicalizes_value_object_key_order_lang() {
        let shape = classify_reified_object(&obj_map(json!({
            "@language": "fr",
            "@value": "chat",
        })))
        .unwrap();
        match shape {
            ReifiedObjectShape::Literal { value, .. } => {
                let m = value.as_object().unwrap();
                let keys: Vec<&str> = m.keys().map(String::as_str).collect();
                assert_eq!(keys, vec!["@value", "@language"]);
            }
            _ => panic!("expected Literal"),
        }
    }

    // ---- Lowering ----------------------------------------------------------

    #[test]
    fn lowers_inline_annotation_to_an_included_node_naming_the_edge() {
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "ex:role": "Engineer" }
            }
        });
        let result = lower(doc).unwrap();
        assert_eq!(included(&result).len(), 1, "one annotation node");
        assert_eq!(
            one(&result, "http://example.org/worksFor"),
            &json!({"@id": "http://example.org/acme"}),
            "the edge stays, without its annotation"
        );
        let sibling = &included(&result)[0];
        assert_eq!(
            one(sibling, reifies_iris::SUBJECT),
            &json!({"@id": "http://example.org/alice"})
        );
        assert_eq!(
            one(sibling, reifies_iris::PREDICATE),
            &json!({"@id": "http://example.org/worksFor"})
        );
        assert_eq!(
            one(sibling, reifies_iris::OBJECT),
            &json!({"@id": "http://example.org/acme"})
        );
        assert_eq!(
            one(sibling, "http://example.org/role"),
            &json!({"@value": "Engineer"})
        );
    }

    /// The edge's predicate is the key its value is under, as the expander
    /// resolved it: by the subject's type-scoped context, which does not
    /// reach the nodes the subject holds (the annotation among them), and by
    /// a property-scoped context.
    #[test]
    fn the_predicate_is_the_key_as_its_context_resolves_it() {
        let annotated = json!({"@id": "http://example.org/o", "@annotation": {"http://example.org/label": "a"}});
        for (doc, path) in [
            (
                json!({
                    "@context": {
                        "@vocab": "http://outer.example/",
                        "Emp": {"@id": "http://example.org/Emp", "@context": {"p": "http://inner.example/p"}}
                    },
                    "@id": "http://example.org/s",
                    "@type": "Emp",
                    "p": annotated.clone()
                }),
                vec![],
            ),
            (
                json!({
                    "@context": {
                        "@vocab": "http://outer.example/",
                        "child": {"@id": "http://example.org/child", "@context": {"p": "http://inner.example/p"}}
                    },
                    "@id": "http://example.org/parent",
                    "child": {"@id": "http://example.org/s", "p": annotated.clone()}
                }),
                vec!["http://example.org/child"],
            ),
        ] {
            let result = lower(doc.clone()).unwrap();
            let subject = path.iter().fold(&result, |node, key| one(node, key));
            assert_eq!(
                one(&included(subject)[0], reifies_iris::PREDICATE),
                &json!({"@id": "http://inner.example/p"}),
                "{doc}"
            );
        }
    }

    #[test]
    fn anonymous_annotation_mints_blank_node_id() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "ex:role": "Engineer" }
            }
        });
        let result = lower(doc).unwrap();
        let sibling = &included(&result)[0];
        let id = sibling.get("@id").and_then(|v| v.as_str()).unwrap();
        assert!(
            id.starts_with("_:fluree_ann_"),
            "anonymous annotation should mint a blank node id, got {id}"
        );
    }

    #[test]
    fn explicit_annotation_id_is_preserved() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": {
                    "@id": "ex:employment-1",
                    "ex:role": "Engineer"
                }
            }
        });
        let result = lower(doc).unwrap();
        let sibling = &included(&result)[0];
        assert_eq!(sibling.get("@id").unwrap(), &json!("ex:employment-1"));
    }

    #[test]
    fn each_annotation_in_an_array_is_its_own_reifier() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": [{"@id": "ex:a1"}, {"@id": "ex:a2"}]
            }
        });
        let result = lower(doc).unwrap();
        let ids: Vec<&Value> = included(&result).iter().map(|n| &n["@id"]).collect();
        assert_eq!(ids, [&json!("ex:a1"), &json!("ex:a2")]);
    }

    #[test]
    fn annotation_resolves_context_aliased_id_on_ref_object() {
        let doc = json!({
            "@context": { "id": "@id", "ex": "http://example.org/" },
            "id": "ex:alice",
            "ex:worksFor": {
                "id": "ex:acme",
                "@annotation": { "ex:role": "Engineer" }
            }
        });
        let result = lower(doc).unwrap();
        assert_eq!(included(&result).len(), 1, "one annotation node");
        assert_eq!(
            one(&included(&result)[0], reifies_iris::OBJECT),
            &json!({ "@id": "http://example.org/acme" })
        );
    }

    #[test]
    fn edge_alias_normalizes_to_annotation() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@edge": { "ex:role": "Engineer" }
            }
        });
        let result = lower(doc).unwrap();
        assert_eq!(included(&result).len(), 1);
    }

    #[test]
    fn lowers_typed_literal_with_annotation_to_value_object_sibling() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:joinedAt": {
                "@value": "2024-01-01",
                "@type": "xsd:date",
                "@annotation": { "ex:source": "ex:hr-system" }
            }
        });
        let result = lower(doc).unwrap();
        assert_eq!(
            one(&result, "ex:joinedAt"),
            &json!({"@value": "2024-01-01", "@type": "xsd:date"}),
            "the literal stays, without its annotation"
        );
        let sibling = &included(&result)[0];
        assert_eq!(
            one(sibling, reifies_iris::OBJECT),
            &json!({"@value": "2024-01-01", "@type": "xsd:date"})
        );
        assert!(sibling.get(reifies_iris::LANG).is_none());
    }

    #[test]
    fn lowers_lang_tagged_literal_emits_reifies_lang() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:label": {
                "@value": "chat",
                "@language": "fr",
                "@annotation": { "ex:source": "ex:lexicon" }
            }
        });
        let result = lower(doc).unwrap();
        let sibling = &included(&result)[0];
        assert_eq!(
            one(sibling, reifies_iris::OBJECT),
            &json!({"@value": "chat", "@language": "fr"})
        );
        // The folded link takes its language from this slot.
        assert_eq!(one(sibling, reifies_iris::LANG), &json!({"@value": "fr"}));
    }

    #[test]
    fn lowers_plain_string_literal_with_annotation() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:name": {
                "@value": "Alice",
                "@annotation": { "ex:source": "hr" }
            }
        });
        let result = lower(doc).unwrap();
        let sibling = &included(&result)[0];
        assert_eq!(
            one(sibling, reifies_iris::OBJECT),
            &json!({"@value": "Alice"})
        );
        assert!(sibling.get(reifies_iris::LANG).is_none());
    }

    #[test]
    fn ttl_shorthand_becomes_an_instruction_in_the_annotation() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:status": {"@value": "on", "@ttl": 60}
        });
        let result = lower(doc).unwrap();
        assert_eq!(one(&result, "ex:status"), &json!({"@value": "on"}));
        assert_eq!(
            one(&included(&result)[0], fluree_vocab::retention_iris::TTL),
            &json!({"@value": 60})
        );
    }

    // ---- Retractions (an update's delete) ------------------------------------

    /// Lower `delete` as an update's delete clause; returns the lowered
    /// clause and the update's `where`.
    fn lower_delete(delete: Value) -> Result<(Value, Vec<Value>)> {
        let mut doc = json!({"delete": delete});
        lower_edge_annotations(&mut doc)?;
        let wheres = match doc.get("where") {
            Some(Value::Array(items)) => items.clone(),
            _ => Vec::new(),
        };
        Ok((doc["delete"].clone(), wheres))
    }

    /// The retract templates the lowering included in `node`.
    fn retractions(node: &Value) -> &Vec<Value> {
        included(node)
    }

    #[test]
    fn delete_by_id_on_plain_string_literal_object_emits_value_object_template() {
        let (delete, wheres) = lower_delete(json!({
            "@id": "ex:alice",
            "ex:name": {
                "@value": "Alice",
                "@annotation": { "@id": "ex:ann-1" }
            }
        }))
        .unwrap();
        let templates = retractions(&delete);
        assert_eq!(templates.len(), 1);
        let t = &templates[0];
        assert_eq!(t.get("@id").unwrap(), &json!("ex:ann-1"));
        assert_eq!(one(t, reifies_iris::SUBJECT), &json!({"@id": "ex:alice"}));
        assert_eq!(one(t, reifies_iris::PREDICATE), &json!({"@id": "ex:name"}));
        assert_eq!(one(t, reifies_iris::OBJECT), &json!({"@value": "Alice"}));
        assert!(t.get(reifies_iris::LANG).is_none());
        // A retraction by annotation does not retract the edge.
        assert!(delete.get("ex:name").is_none(), "{delete:#}");
        // No WHERE for by-id delete.
        assert!(wheres.is_empty());
    }

    #[test]
    fn delete_by_id_on_typed_literal_object() {
        let (delete, _) = lower_delete(json!({
            "@id": "ex:alice",
            "ex:joinedAt": {
                "@value": "2024-01-01",
                "@type": "xsd:date",
                "@annotation": { "@id": "ex:ann-2" }
            }
        }))
        .unwrap();
        let t = &retractions(&delete)[0];
        assert_eq!(
            one(t, reifies_iris::OBJECT),
            &json!({"@value": "2024-01-01", "@type": "xsd:date"})
        );
        assert!(t.get(reifies_iris::LANG).is_none());
    }

    #[test]
    fn delete_by_id_on_lang_tagged_literal_object_emits_reifies_lang() {
        let (delete, _) = lower_delete(json!({
            "@id": "ex:alice",
            "ex:label": {
                "@value": "chat",
                "@language": "fr",
                "@annotation": { "@id": "ex:ann-3" }
            }
        }))
        .unwrap();
        let t = &retractions(&delete)[0];
        assert_eq!(
            one(t, reifies_iris::OBJECT),
            &json!({"@value": "chat", "@language": "fr"})
        );
        // Without the language, the retraction would name another edge.
        assert_eq!(one(t, reifies_iris::LANG), &json!({"@value": "fr"}));
    }

    #[test]
    fn delete_by_selector_on_plain_string_literal_object() {
        let (delete, wheres) = lower_delete(json!({
            "@id": "ex:alice",
            "ex:name": {
                "@value": "Alice",
                "@annotation": { "ex:source": "hr" }
            }
        }))
        .unwrap();
        let wn = &wheres[0];
        assert_eq!(
            wn.get(REIFIES_KEY).unwrap(),
            &json!({"@id": "ex:alice", "ex:name": {"@value": "Alice"}})
        );
        let t = &retractions(&delete)[0];
        assert_eq!(t["@id"], wn["@id"], "the template is bound by the where");
        assert_eq!(one(t, reifies_iris::OBJECT), &json!({"@value": "Alice"}));
        // The selector's body is the where node's: a plain literal as written.
        assert_eq!(wn.get("ex:source").unwrap(), &json!("hr"));
    }

    #[test]
    fn delete_by_selector_on_lang_tagged_literal_keeps_lang_in_where_and_template() {
        let (delete, wheres) = lower_delete(json!({
            "@id": "ex:alice",
            "ex:label": {
                "@value": "chat",
                "@language": "fr",
                "@annotation": { "ex:source": "lexicon" }
            }
        }))
        .unwrap();
        // The WHERE selector's `@reifies` keeps the language tag, so the
        // same lexical string in another language does not bind.
        assert_eq!(
            wheres[0].get(REIFIES_KEY).unwrap(),
            &json!({"@id": "ex:alice", "ex:label": {"@value": "chat", "@language": "fr"}})
        );
        let t = &retractions(&delete)[0];
        assert_eq!(one(t, reifies_iris::LANG), &json!({"@value": "fr"}));
    }

    /// A retraction is included in the node it is written from, so it is in
    /// that node's graph; a selector's `where` names the graph.
    #[test]
    fn a_retraction_in_a_named_graph_is_in_its_edges_graph() {
        let (delete, wheres) = lower_delete(json!([{
            "@graph": "ex:hr-graph",
            "@id": "ex:alice",
            "ex:name": {"@value": "Alice", "@annotation": { "@id": "ex:ann-g" }}
        }, {
            "@graph": "ex:hr-graph",
            "@id": "ex:bob",
            "ex:name": {"@value": "Bob", "@annotation": { "ex:source": "hr" }}
        }]))
        .unwrap();
        for node in delete.as_array().unwrap() {
            assert_eq!(node["@graph"], "ex:hr-graph");
            let t = &retractions(node)[0];
            // The parser anchors it (`f:reifiesGraph`) from that scope.
            assert!(t.get(reifies_iris::GRAPH).is_none(), "{t:?}");
            assert!(t.get("@graph").is_none(), "{t:?}");
        }
        assert_eq!(wheres.len(), 1);
        assert_eq!(wheres[0]["@graph"], "ex:hr-graph");
    }

    #[test]
    fn a_retraction_needs_both_ends_of_its_edge() {
        let err = lower_delete(json!({
            "ex:name": {"@value": "Alice", "@annotation": {"@id": "ex:ann"}}
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("parent @id"), "{err}");
        let err = lower_delete(json!({
            "@id": "ex:alice",
            "ex:knows": {"ex:name": "Bob", "@annotation": {"@id": "ex:ann"}}
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("explicit @id"), "{err}");
    }

    // ---- Refusals ------------------------------------------------------------

    #[test]
    fn rejects_both_annotation_and_edge() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "ex:role": "A" },
                "@edge":       { "ex:role": "B" }
            }
        });
        let err = lower(doc).unwrap_err();
        assert!(err.to_string().contains("@annotation") && err.to_string().contains("@edge"));
    }

    #[test]
    fn rejects_nested_annotation_in_body() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": {
                    "ex:source": {
                        "@id": "ex:hr",
                        "@annotation": { "ex:meta": "x" }
                    }
                }
            }
        });
        let msg = lower(doc).unwrap_err().to_string();
        assert!(
            msg.contains("annotation-of-annotation") && msg.contains("deferred"),
            "{msg}"
        );
    }

    #[test]
    fn rejects_an_annotation_on_a_node_no_property_holds() {
        let msg = lower(json!({"@id": "ex:a", "@annotation": {"ex:c": 1}}))
            .unwrap_err()
            .to_string();
        assert!(msg.contains("unasserted-reifier"), "{msg}");
    }

    #[test]
    fn rejects_user_authored_reifies_iri() {
        let doc = json!({
            "@id": "ex:alice",
            reifies_iris::SUBJECT: { "@id": "ex:bob" }
        });
        let err = lower(doc).unwrap_err();
        assert!(err.to_string().contains("system-controlled"));
    }

    /// W9: the lowering's anonymous annotation nodes are `_:fluree_ann_N`; a
    /// user node labelled that way would merge with one of them.
    #[test]
    fn rejects_user_label_in_the_annotation_prefix() {
        for doc in [
            json!({"@id": "_:fluree_ann_0", "ex:p": 1}),
            json!({"@id": "ex:a", "ex:knows": {"@id": "_:fluree_ann_3"}}),
            json!({"@context": {"id": "@id"}, "id": "_:fluree_ann_1", "ex:p": 1}),
        ] {
            let err = lower(doc.clone()).unwrap_err().to_string();
            assert!(
                err.starts_with("Parse error: ") && err.contains("_:fluree_ann_"),
                "{doc}: {err}"
            );
        }
        // Other labels are untouched.
        lower(json!({"@id": "_:fluree_annotation", "ex:p": 1})).unwrap();
    }

    #[test]
    fn reifies_on_insert_names_its_triple_without_asserting_it() {
        let doc = json!({
            "@id": "ex:employment-1",
            "ex:role": "Engineer",
            "@reifies": [
                { "@id": "ex:alice", "ex:worksFor": { "@id": "ex:acme" } },
                { "@id": "ex:alice", "ex:age": 42 }
            ]
        });
        let lowered = lower(doc).expect("lower");
        assert_eq!(one(&lowered, "ex:role"), &json!({"@value": "Engineer"}));
        assert_eq!(
            one(&lowered, reifies_iris::SUBJECT),
            &json!({"@id": "ex:alice"})
        );
        assert_eq!(
            one(&lowered, reifies_iris::PREDICATE),
            &json!({"@id": "ex:worksFor"})
        );
        assert_eq!(
            one(&lowered, reifies_iris::OBJECT),
            &json!({"@id": "ex:acme"})
        );
        // The second triple rides on a node with the reifier's `@id`,
        // included in it, so written in its scope.
        let included = included(&lowered);
        assert_eq!(included.len(), 1, "{lowered:#}");
        assert_eq!(included[0]["@id"], "ex:employment-1");
        assert_eq!(
            one(&included[0], reifies_iris::PREDICATE),
            &json!({"@id": "ex:age"})
        );
        assert_eq!(
            one(&included[0], reifies_iris::OBJECT),
            &json!({"@value": 42})
        );
        assert!(
            lowered.get("ex:worksFor").is_none()
                && included.iter().all(|n| n.get("ex:worksFor").is_none()),
            "the triple is not asserted"
        );
    }

    #[test]
    fn rejects_compact_reifies_iri_via_context() {
        let doc = json!({
            "@context": { "f": "https://ns.flur.ee/db#" },
            "@id": "ex:alice",
            "f:reifiesSubject": { "@id": "ex:bob" }
        });
        let msg = lower(doc).unwrap_err().to_string();
        assert!(
            msg.contains("system-controlled") && msg.contains("reifiesSubject"),
            "expected reserved-predicate firewall error, got: {msg}"
        );
    }

    #[test]
    fn rejects_annotation_nested_inside_list_item() {
        // List-occurrence annotations are deferred (v1).
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice",
            "ex:colleagues": {
                "@list": [
                    { "@id": "ex:bob",
                      "@annotation": { "ex:role": "buddy" } }
                ]
            }
        });
        let msg = lower(doc).unwrap_err().to_string();
        assert!(
            msg.contains("annotation") && msg.contains("deferred"),
            "list-nested @annotation must be rejected with a deferred message: {msg}"
        );
    }

    #[test]
    fn rejects_reifies_nested_inside_list_item() {
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:ann1",
            "ex:colleagues": {
                "@list": [
                    { "@id": "ex:bob",
                      "@reifies": { "@id": "ex:alice", "ex:knows": { "@id": "ex:bob" } } }
                ]
            }
        });
        let err = lower(doc).unwrap_err();
        assert!(err.to_string().contains("@reifies"));
    }

    #[test]
    fn rejects_annotation_nested_inside_variable_wrapper() {
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice",
            "ex:status": {
                "@variable": "?status",
                "@annotation": { "ex:meta": "x" }
            }
        });
        let err = lower(doc).unwrap_err();
        assert!(err.to_string().contains("annotation"));
    }

    // ---- What the lowering leaves alone ------------------------------------

    /// The expansion of `doc` as the lowering reads it.
    fn expanded(doc: &Value) -> Value {
        expand_with_root_context(doc, &top_level_context(doc).unwrap(), false).unwrap()
    }

    #[test]
    fn a_document_without_annotations_is_its_expansion() {
        for doc in [
            json!({
                "@context": { "ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#" },
                "@id": "ex:alice",
                "ex:joinedAt": { "@value": "2024-01-01", "@type": "xsd:date" }
            }),
            json!({
                "@context": { "ex": "http://example.org/" },
                "@id": "ex:alice",
                "ex:nicknames": { "@list": ["A", "B", "C"] }
            }),
            json!({
                "@context": { "ex": "http://example.org/" },
                "@id": "ex:alice",
                "ex:status": { "@variable": "?status" }
            }),
            json!({
                "@context": { "ex": "http://example.org/" },
                "@id": "ex:alice",
                "ex:name": "Alice"
            }),
        ] {
            assert_eq!(lower(doc.clone()).unwrap(), expanded(&doc), "{doc}");
        }
    }

    #[test]
    fn annotation_in_named_graph_is_included_in_its_edges_subject() {
        // The annotation node is included in the edge's subject, so the
        // parser writes it in the subject's (the edge's) graph. It carries no
        // graph of its own, and the parser anchors it (`f:reifiesGraph`) from
        // that scope.
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice",
            "@graph": "ex:hr-graph",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "ex:role": "Engineer" }
            }
        });
        let result = lower(doc).unwrap();
        assert_eq!(result.get("@graph"), Some(&json!("ex:hr-graph")));
        let sibling = &included(&result)[0];
        assert!(sibling.get("@graph").is_none(), "{sibling}");
        assert!(sibling.get(reifies_iris::GRAPH).is_none(), "{sibling}");
    }
}
