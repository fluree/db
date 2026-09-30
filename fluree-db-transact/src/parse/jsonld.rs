//! JSON-LD transaction parser
//!
//! This module parses JSON-LD transaction documents into the Transaction IR.
//! Supports parsing of insert, upsert, and update transactions with proper
//! JSON-LD context expansion.
//!
//! # Architecture
//!
//! This parser reuses the query parser for WHERE clauses to ensure consistent
//! semantics (OPTIONAL, UNION, FILTER, etc.) between queries and transactions.
//! Only INSERT/DELETE templates are parsed here, which generate flakes rather
//! than match patterns.

use super::txn_meta::extract_txn_meta;
use crate::error::{Result, TransactError};
use crate::ir::{
    GraphName, GraphScope, GraphSel, InlineValues, TemplateGraph, TemplateTerm, TripleTemplate,
    Txn, TxnOpts, TxnType, WriteGraphs,
};
use crate::namespace::NamespaceRegistry;
use fluree_db_core::dataset_ref::GraphIri;
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::FlakeValue;
use fluree_db_query::parse::{
    parse_where_with_counters, JsonLdParseCtx, JsonLdParsePolicy, PathAliasMap, UnresolvedQuery,
};
use fluree_db_query::VarRegistry;
use fluree_graph_json_ld::{
    classify_graph_value, expand_with_context_policy, parse_context, GraphValue, ParsedContext,
};
use fluree_vocab::{
    rdf::{self, TYPE},
    rdf_names,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Parse a JSON-LD transaction into the Transaction IR
///
/// The transaction format depends on the transaction type:
///
/// ## Insert
/// ```json
/// {
///   "@context": {"ex": "http://example.org/"},
///   "@id": "ex:alice",
///   "ex:name": "Alice",
///   "ex:age": 30
/// }
/// ```
///
/// ## Upsert
/// Same as insert, but existing values for provided predicates are deleted first.
///
/// ## Update (SPARQL-style)
/// ```json
/// {
///   "@context": {"ex": "http://example.org/"},
///   "where": { "@id": "?s", "ex:name": "?name" },
///   "delete": { "@id": "?s", "ex:name": "?name" },
///   "insert": { "@id": "?s", "ex:name": "New Name" }
/// }
/// ```
///
/// `ledger_id` is the ledger the transaction targets (`name:branch`); graph
/// names resolve against it (the `config` keyword and the ledger's own
/// reserved graph IRIs).
pub fn parse_transaction(
    json: &Value,
    txn_type: TxnType,
    opts: TxnOpts,
    ns_registry: &mut NamespaceRegistry,
    ledger_id: &str,
) -> Result<Txn> {
    parse_rooted(json, txn_type, opts, ns_registry, ledger_id, None)
}

/// [`parse_transaction`], with the request's own graph as the fixed root
/// scope when `request_graph` is `Some` (graph insert / sync).
fn parse_rooted(
    json: &Value,
    txn_type: TxnType,
    mut opts: TxnOpts,
    ns_registry: &mut NamespaceRegistry,
    ledger_id: &str,
    request_graph: Option<&GraphSel>,
) -> Result<Txn> {
    // Pull `lpgEdgeLifecycle` from the transaction's `opts` block when
    // the programmatic `TxnOpts::lpg_edge_lifecycle` is unset. This
    // mirrors how `strictCompactIri` is read from the JSON when the
    // builder doesn't override it.
    //
    // Resolved BEFORE the lowering passes so they can branch on it —
    // an empty `@annotation: {}` is a no-op in RDF mode but mints a
    // fresh property-less annotation subject in LPG mode (a Cypher
    // relationship retains identity even without properties).
    if opts.lpg_edge_lifecycle.is_none() {
        opts.lpg_edge_lifecycle = json
            .as_object()
            .and_then(|m| m.get("opts"))
            .and_then(|v| v.as_object())
            .and_then(|o| o.get("lpgEdgeLifecycle"))
            .and_then(Value::as_bool);
    }
    let lpg_mode = opts.lpg_edge_lifecycle.unwrap_or(false);

    // Requested SHACL validation mode ("warn" / "reject"), same JSON-over-
    // programmatic precedence as the opts above. Whether the request is
    // honored is decided later against the ledger config's override control
    // (see TxnOpts::validation_mode); an unrecognized string is ignored.
    if opts.validation_mode.is_none() {
        opts.validation_mode = json
            .as_object()
            .and_then(|m| m.get("opts"))
            .and_then(|v| v.as_object())
            .and_then(|o| o.get("validationMode"))
            .and_then(Value::as_str)
            .and_then(fluree_db_core::ledger_config::ValidationMode::parse_opt);
    }

    // The request's own constraints (`opts.shapes`, `opts.uniqueProperties`),
    // same precedence. Read here, where every surface parses a JSON
    // transaction (the server, the embedded API, the CLI), so none drops them
    // and leaves the caller believing its data was checked. Whether they
    // apply is decided at staging (override control, policy scope).
    read_body_constraints(json, &mut opts)?;

    // M1: lower `@annotation` / `@edge` / `@reifies` into the seven-fact
    // `f:reifies*` system encoding before JSON-LD expansion, rejecting
    // user-authored `f:reifies*` IRIs and every deferred shape (literal-
    // valued annotations, multi-triple reifiers, annotation-of-annotation).
    //
    // The firewall always runs on the *original* document (read-only) — it
    // is the security guard against user-authored system IRIs, so it must
    // not be gated. Only the mutating lowering passes need the payload
    // clone, and only when an `@annotation` / `@edge` / `@reifies` block is
    // actually present: a large non-annotated transaction then skips the
    // clone and both rewrite walks entirely.
    let top_ctx = super::edge_annotations::top_level_context(json)?;
    super::edge_annotations::run_user_authored_reifies_firewall(json, &top_ctx)?;

    // Two-pass lowering for UPDATE transactions:
    //
    // 1. The delete-clause pre-pass rewrites `@annotation` blocks inside
    //    `delete:` into explicit `f:reifies*` retract templates so the
    //    assertion-shaped lowering below doesn't synthesize spurious
    //    sibling nodes. Insert / Upsert paths skip it — their docs carry no
    //    `delete` key, so it would be a structural no-op anyway.
    // 2. The standard assertion-shaped lowering rewrites the rest. Both
    //    passes synthesize `f:reifies*` IRIs internally, which is why the
    //    firewall ran first on the un-rewritten input.
    let lowered = if super::edge_annotations::document_has_annotation_keys(json) {
        let mut lowered = json.clone();
        if matches!(txn_type, TxnType::Update) {
            super::edge_annotations::lower_delete_annotation_blocks(&mut lowered)?;
        }
        super::edge_annotations::lower_edge_annotations_after_firewall(
            &mut lowered,
            &top_ctx,
            lpg_mode,
        )?;
        std::borrow::Cow::Owned(lowered)
    } else {
        std::borrow::Cow::Borrowed(json)
    };

    let annotated = matches!(lowered, std::borrow::Cow::Owned(_));
    let txn = match txn_type {
        TxnType::Insert | TxnType::Upsert => parse_data(
            &lowered,
            txn_type,
            opts,
            ns_registry,
            ledger_id,
            request_graph,
        )?,
        TxnType::Update => parse_update(&lowered, opts, ns_registry, ledger_id)?,
    };
    if annotated {
        refuse_annotations_under_a_variable_graph(&txn.insert_templates)?;
        refuse_annotations_under_a_variable_graph(&txn.delete_templates)?;
        check_reifiers_match_edges(&txn.insert_templates)?;
    }
    Ok(txn)
}

/// Fill `opts.shapes` and `opts.unique_properties` from the body's `opts`
/// block where the caller set neither: `shapes` a JSON-LD object or an array
/// of them, `uniqueProperties` an array of property IRI strings (an empty one
/// means none). A malformed value is refused rather than ignored.
fn read_body_constraints(json: &Value, opts: &mut TxnOpts) -> Result<()> {
    let Some(body_opts) = json
        .as_object()
        .and_then(|m| m.get("opts"))
        .and_then(Value::as_object)
    else {
        return Ok(());
    };
    if opts.shapes.is_none() {
        if let Some(shapes) = body_opts.get("shapes") {
            let well_formed = match shapes {
                Value::Object(_) => true,
                Value::Array(items) => items.iter().all(Value::is_object),
                _ => false,
            };
            if !well_formed {
                return Err(TransactError::Parse(
                    "opts.shapes must be a JSON-LD object or an array of JSON-LD objects"
                        .to_string(),
                ));
            }
            opts.shapes = Some(shapes.clone());
        }
    }
    if opts.unique_properties.is_none() {
        if let Some(raw) = body_opts.get("uniqueProperties") {
            let iris = raw
                .as_array()
                .and_then(|items| {
                    items
                        .iter()
                        .map(|v| v.as_str().map(str::to_string))
                        .collect::<Option<Vec<String>>>()
                })
                .ok_or_else(|| {
                    TransactError::Parse(
                        "opts.uniqueProperties must be an array of property IRI strings"
                            .to_string(),
                    )
                })?;
            if !iris.is_empty() {
                opts.unique_properties = Some(iris);
            }
        }
    }
    Ok(())
}

/// An edge annotation under a variable graph (`["graph", "?g", …]` or
/// `"@graph": "?g"`) would anchor its reifier to the graph the WHERE binds,
/// which flake generation cannot write as an object. Refused here, with a
/// message saying why, instead of failing there.
fn refuse_annotations_under_a_variable_graph(templates: &[TripleTemplate]) -> Result<()> {
    let annotated_under_a_variable = templates.iter().any(|t| {
        matches!(t.graph, TemplateGraph::Var(_))
            && matches!(&t.predicate, TemplateTerm::Sid(p) if crate::ir::is_reifies_subject(p))
    });
    if annotated_under_a_variable {
        return Err(TransactError::Parse(
            "edge annotations are not supported under a variable graph; name the graph \
             (an IRI, a compact IRI or a keyword) where an annotated edge is written"
                .to_string(),
        ));
    }
    Ok(())
}

/// Every reifier bundle the annotation lowering added must reify an edge this
/// document asserts, in the bundle's own graph: a bundle written to graph `g`
/// with `f:reifiesSubject s` and `f:reifiesPredicate p` needs an asserted
/// `(s, p)` template in `g` (JSON-LD `@annotation` always asserts the edge it
/// annotates). The lowering places each bundle beside its edge; if its graph
/// scoping ever disagreed with the parser's, the annotation would commit in a
/// graph that does not hold its edge. That is refused here instead.
///
/// A document whose templates all write the default graph has no scoping to
/// disagree about, so the check runs once any template names a graph.
fn check_reifiers_match_edges(templates: &[TripleTemplate]) -> Result<()> {
    use rustc_hash::{FxHashMap, FxHashSet};

    if templates.iter().all(|t| t.graph == TemplateGraph::Default) {
        return Ok(());
    }

    /// A template term by reference: the check allocates nothing per
    /// template.
    #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
    enum Key<'a> {
        Sid(u16, &'a str),
        Var(fluree_db_query::VarId),
        Blank(&'a str),
    }
    fn key(term: &TemplateTerm) -> Option<Key<'_>> {
        match term {
            TemplateTerm::Sid(sid) => Some(Key::Sid(sid.namespace_code, &sid.name)),
            TemplateTerm::Var(var) => Some(Key::Var(*var)),
            TemplateTerm::BlankNode(label) => Some(Key::Blank(label)),
            TemplateTerm::Value(_) => None,
        }
    }

    let mut subjects: FxHashMap<(Key<'_>, &TemplateGraph), Key<'_>> = FxHashMap::default();
    let mut predicates: FxHashMap<(Key<'_>, &TemplateGraph), Key<'_>> = FxHashMap::default();
    let mut edges: FxHashSet<(Key<'_>, Key<'_>, &TemplateGraph)> =
        FxHashSet::with_capacity_and_hasher(templates.len(), Default::default());
    for t in templates {
        let Some(subject) = key(&t.subject) else {
            continue;
        };
        match (&t.predicate, key(&t.object)) {
            (TemplateTerm::Sid(p), Some(object)) if crate::ir::is_reifies_subject(p) => {
                subjects.insert((subject, &t.graph), object);
            }
            (TemplateTerm::Sid(p), Some(object)) if crate::ir::is_reifies_predicate(p) => {
                predicates.insert((subject, &t.graph), object);
            }
            (predicate, _) => {
                if let Some(p) = key(predicate) {
                    edges.insert((subject, p, &t.graph));
                }
            }
        }
    }
    for (&(reifier, graph), &s) in &subjects {
        let asserted = predicates
            .get(&(reifier, graph))
            .is_some_and(|&p| edges.contains(&(s, p, graph)));
        if !asserted {
            return Err(TransactError::Parse(format!(
                "an edge annotation would be written to {} without the edge it annotates; \
                 an annotation and its edge must be in the same graph",
                match graph {
                    TemplateGraph::Default => "the default graph".to_string(),
                    TemplateGraph::Iri(iri) => format!("graph <{iri}>"),
                    TemplateGraph::Var(_) => "a variable graph".to_string(),
                }
            )));
        }
    }
    Ok(())
}

/// Parse a graph-sync transaction (see [`Txn::sync_graph`]).
///
/// The payload is an ordinary insert-shaped JSON-LD document describing the
/// target graph's DESIRED full contents, parsed as [`parse_graph_insert`]
/// does, with the sync directive stamped on the transaction. An explicitly
/// empty document (`"@graph": []`) means "the graph's desired contents are
/// empty"; the API layer gates that behind an explicit opt-in before it
/// becomes a whole-graph clear.
pub fn parse_sync_transaction(
    json: &Value,
    graph: &GraphSel,
    opts: TxnOpts,
    ns_registry: &mut NamespaceRegistry,
    ledger_id: &str,
) -> Result<Txn> {
    // A malformed target is refused under the label staging's own check of
    // it uses.
    if let GraphSel::Graph(iri) = graph {
        GraphIri::parse(iri).map_err(|e| TransactError::Parse(format!("sync target: {e}")))?;
    }
    let mut txn = parse_graph_insert(json, graph, opts, ns_registry, ledger_id)?;
    txn.sync_graph = Some(graph.clone());
    Ok(txn)
}

/// Parse an insert whose triples all land in `graph`, the default graph or
/// one named graph, named by the caller rather than by the payload.
///
/// Parsing is exactly insert parsing (same context handling, annotation
/// lowering, txn-meta extraction) with `graph` as the root scope. Differences
/// from insert:
/// - an explicitly empty document (`"@graph": []`) parses to an empty
///   transaction instead of an error;
/// - a payload that addresses any other graph itself (a graph selector naming
///   another graph, or a named-graph object) is rejected: the scope is
///   exactly one graph.
pub fn parse_graph_insert(
    json: &Value,
    graph: &GraphSel,
    opts: TxnOpts,
    ns_registry: &mut NamespaceRegistry,
    ledger_id: &str,
) -> Result<Txn> {
    // `{"@graph": []}`: an envelope with no nodes. (With an `@id` the same
    // value is an empty JSON-LD named graph, which a graph write refuses.)
    let explicitly_empty = json.as_object().is_some_and(|obj| {
        matches!(
            fluree_graph_json_ld::doc_shape(obj),
            Ok(fluree_graph_json_ld::DocShape::Envelope)
        ) && obj
            .get("@graph")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
    });
    if explicitly_empty {
        let mut txn = Txn::insert().with_opts(opts);
        if let GraphName::Iri(iri) = request_graph_name(graph)? {
            txn.write_graphs.insert(iri.into_string());
        }
        return Ok(txn);
    }
    // Reifier bundles land in the request graph's root scope, which anchors
    // them to it (`GraphScope::emit`).
    parse_rooted(
        json,
        TxnType::Insert,
        opts,
        ns_registry,
        ledger_id,
        Some(graph),
    )
}

/// The graph a graph insert / sync request names, as a write-side name.
fn request_graph_name(graph: &GraphSel) -> Result<GraphName> {
    match graph {
        GraphSel::Default => Ok(GraphName::Default),
        GraphSel::Graph(iri) => GraphIri::parse(iri)
            .map(GraphName::Iri)
            .map_err(|e| TransactError::Parse(format!("target graph: {e}"))),
    }
}

/// The strict compact-IRI policy: the programmatic option, else the
/// document's `opts.strictCompactIri`, else strict.
fn strict_compact_iri(opts: &TxnOpts, json: &Value) -> bool {
    opts.strict_compact_iri
        .or_else(|| {
            use fluree_db_query::parse::policy::parse_strict_compact_iri_opt;
            json.as_object().and_then(parse_strict_compact_iri_opt)
        })
        .unwrap_or(true)
}

/// Parse an insert or upsert document.
///
/// Upsert parses exactly as insert; the upsert logic (query existing values,
/// retract them) happens in staging.
///
/// The root scope is the default graph, or the request's graph for a graph
/// insert / sync (`request_graph`), which the payload may not leave.
fn parse_data(
    json: &Value,
    txn_type: TxnType,
    opts: TxnOpts,
    ns_registry: &mut NamespaceRegistry,
    ledger_id: &str,
    request_graph: Option<&GraphSel>,
) -> Result<Txn> {
    let context = extract_context(json)?;
    let strict = strict_compact_iri(&opts, json);

    // Extract transaction metadata (only from envelope-form documents with @graph)
    let txn_meta = extract_txn_meta(json, &context, ns_registry, strict)?;

    // Strip top-level `opts` so it is not expanded as data (single-object form)
    let json_for_expand = strip_opts_for_expansion(json, &context)?;
    let expanded = expand_with_context_policy(&json_for_expand, &context, strict)?;

    let no_aliases = HashMap::new();
    let mut blanks = BlankIssuer::default();
    let (templates, vars, write_graphs) = loop {
        let mut vars = VarRegistry::new();
        let mut write_graphs = WriteGraphs::new();
        let root = match request_graph {
            Some(graph) => write_graphs.scope(request_graph_name(graph)?),
            None => GraphScope::default_graph(),
        };
        let mut ctx = TemplateParseCtx::new(
            &context,
            &mut vars,
            ns_registry,
            false,
            strict,
            &mut write_graphs,
            &no_aliases,
            ledger_id,
            GraphRole::Data,
            &mut blanks,
        );
        if request_graph.is_some() {
            ctx.fixed_root = Some(root.clone());
        }
        let templates = parse_expanded_triples_with_ctx(&expanded, &root, &mut ctx)?;
        if !blanks.needs_reparse()? {
            break (templates, vars, write_graphs);
        }
    };
    if templates.is_empty() {
        let (verb, noun) = match txn_type {
            TxnType::Upsert => ("Upsert", "upsert"),
            _ => ("Insert", "insert"),
        };
        return Err(TransactError::Parse(format!(
            "{verb} must contain at least one predicate or @type (an object with only @id is not a valid {noun})"
        )));
    }

    let txn = match txn_type {
        TxnType::Upsert => Txn::upsert(),
        _ => Txn::insert(),
    };
    let mut txn = txn
        .with_inserts(templates)
        .with_vars(vars)
        .with_opts(opts)
        .with_txn_meta(txn_meta);
    txn.write_graphs = write_graphs.into_iris();
    Ok(txn)
}

/// Parse an update transaction (SPARQL-style with WHERE/DELETE/INSERT)
///
/// This function reuses the query parser for WHERE clauses, ensuring consistent
/// semantics (OPTIONAL, UNION, FILTER, etc.) between queries and transactions.
///
/// The WHERE clause is parsed to `Vec<UnresolvedPattern>` (keeping IRIs as strings).
/// These patterns are lowered to `Pattern` during staging, when we have access to
/// the ledger's database for IRI encoding.
fn parse_update(
    json: &Value,
    opts: TxnOpts,
    ns_registry: &mut NamespaceRegistry,
    ledger_id: &str,
) -> Result<Txn> {
    let obj = json
        .as_object()
        .ok_or_else(|| TransactError::Parse("Update transaction must be an object".to_string()))?;

    // An update with neither clause can never change data. Rejecting it here
    // (mirroring the empty-insert/upsert guards) prevents a mistargeted body —
    // e.g. a Cypher envelope posted as JSON-LD — from committing an empty
    // transaction and reporting success.
    if !obj.contains_key("insert") && !obj.contains_key("delete") {
        return Err(TransactError::Parse(
            "update transaction must contain an \"insert\" or \"delete\" clause".to_string(),
        ));
    }

    // Parse context from the outer document
    let context = extract_context(json)?;

    let strict = strict_compact_iri(&opts, json);

    // Extract transaction metadata (only from envelope-form documents with @graph)
    let txn_meta = extract_txn_meta(json, &context, ns_registry, strict)?;

    // Optional WHERE dataset scoping using query-style dataset keys.
    // - `from.graph` (or `"from": "<graph IRI>"`, or `"from": ["<g1>", "<g2>"]`) scopes WHERE
    //   default graph(s) (USING equivalent; multiple graphs are merged for default-graph patterns)
    // - `fromNamed` (or legacy `from-named`) restricts visible named graphs for WHERE (USING NAMED equivalent)
    let where_named_graphs = parse_update_where_named_graphs(
        obj.get("fromNamed").or_else(|| obj.get("from-named")),
        &context,
        strict,
    )?;
    let from_named_aliases: HashMap<String, String> = where_named_graphs
        .as_ref()
        .map(|v| {
            v.iter()
                .filter_map(|g| g.alias.as_ref().map(|a| (a.clone(), g.iri.clone())))
                .collect()
        })
        .unwrap_or_default();

    let has_where = obj.get("where").is_some();
    let has_values = obj.get("values").is_some();
    let allow_object_vars = has_where || has_values;
    let object_var_parsing = allow_object_vars && opts.object_var_parsing.unwrap_or(true);

    // Parse WHERE clause using the query parser
    // This reuses full pattern support (OPTIONAL, UNION, FILTER, etc.)
    // Variables remain as strings in UnresolvedPattern; they'll be assigned VarIds
    // during lowering in stage.rs using the same VarRegistry as INSERT/DELETE.
    let where_patterns = if let Some(where_val) = obj.get("where") {
        let mut query = UnresolvedQuery::new(context.clone());
        let mut subject_counter: u32 = 0;
        let mut nested_counter: u32 = 0;
        let parse_policy = JsonLdParsePolicy {
            strict_compact_iri: strict,
        };
        // Graph names in the WHERE resolve as in the templates: this ledger's
        // keywords and the same `fromNamed` aliases.
        let ctx = JsonLdParseCtx::new(context.clone(), PathAliasMap::new(), parse_policy)
            .with_graph_names(fluree_db_query::parse::GraphNameEnv {
                ledger_id: Some(ledger_id.to_string()),
                aliases: from_named_aliases.clone(),
            });
        parse_where_with_counters(
            where_val,
            &ctx,
            &mut query,
            &mut subject_counter,
            &mut nested_counter,
            object_var_parsing,
        )
        .map_err(|e| TransactError::Parse(format!("WHERE clause: {e}")))?;

        query.patterns
    } else {
        Vec::new()
    };

    // The template clauses and VALUES share one blank-node issuer; they are
    // parsed a second time only when a user `_:bN` label collided with an
    // anonymous node's (see `BlankIssuer`).
    let mut blanks = BlankIssuer::default();
    let clauses = loop {
        let clauses = parse_update_clauses(
            obj,
            &context,
            strict,
            object_var_parsing,
            &from_named_aliases,
            ns_registry,
            ledger_id,
            &mut blanks,
        )?;
        if !blanks.needs_reparse()? {
            break clauses;
        }
    };

    let where_default_graph_iris = parse_update_where_default_graph_iris(
        obj.get("from"),
        &context,
        &from_named_aliases,
        strict,
    )?
    .unwrap_or_else(|| clauses.template_root_iri.iter().cloned().collect());

    let mut txn = Txn::update()
        .with_wheres(where_patterns)
        .with_deletes(clauses.delete)
        .with_inserts(clauses.insert)
        .with_vars(clauses.vars)
        .with_opts(opts)
        .with_txn_meta(txn_meta);
    txn.write_graphs = clauses.write_graphs.into_iris();
    txn.template_default_graph = clauses.template_root_iri;
    txn.update_where_default_graph_iris = Some(where_default_graph_iris);
    txn.update_where_named_graphs = where_named_graphs;
    if let Some(values) = clauses.values {
        txn = txn.with_values(values);
    }
    Ok(txn)
}

/// The parts of an update that name blank nodes: the `graph` key's scope,
/// the delete and insert templates, and VALUES.
struct UpdateClauses {
    vars: VarRegistry,
    write_graphs: WriteGraphs,
    /// The `graph` key's IRI, the WHERE's default graph unless `from` names
    /// one (SPARQL `WITH`).
    template_root_iri: Option<String>,
    delete: Vec<TripleTemplate>,
    insert: Vec<TripleTemplate>,
    values: Option<InlineValues>,
}

#[allow(clippy::too_many_arguments)]
fn parse_update_clauses(
    obj: &serde_json::Map<String, Value>,
    context: &ParsedContext,
    strict: bool,
    object_var_parsing: bool,
    from_named_aliases: &HashMap<String, String>,
    ns_registry: &mut NamespaceRegistry,
    ledger_id: &str,
    blanks: &mut BlankIssuer,
) -> Result<UpdateClauses> {
    let mut vars = VarRegistry::new();
    let mut write_graphs = WriteGraphs::new();

    // Optional transaction-level default graph (SPARQL `WITH`): the root scope
    // of both template clauses, and the WHERE's default graph unless `from`
    // names one.
    let template_root = match obj.get("graph") {
        None => GraphScope::default_graph(),
        Some(graph_val) => {
            let GraphValue::Selector(raw) = classify_graph_value(graph_val) else {
                return Err(TransactError::Parse(
                    "graph must be a graph IRI (string, or {\"@id\": ...})".to_string(),
                ));
            };
            let name = TemplateParseCtx::new(
                context,
                &mut vars,
                ns_registry,
                false,
                strict,
                &mut write_graphs,
                from_named_aliases,
                ledger_id,
                GraphRole::UpdateTemplate,
                blanks,
            )
            .resolve_graph_name(raw, GraphRole::UpdateDefault)?;
            // The template default graph: staging writes it to the ledger's
            // default graph when it names the ledger's own address.
            write_graphs.scope(name).as_template_default()
        }
    };
    let template_root_iri = match template_root.graph() {
        TemplateGraph::Iri(iri) => Some(iri.to_string()),
        _ => None,
    };

    // Parse DELETE clause
    let delete = if let Some(delete_val) = obj.get("delete") {
        validate_type_fields(delete_val)?;
        let mut ctx = TemplateParseCtx::new(
            context,
            &mut vars,
            ns_registry,
            object_var_parsing,
            strict,
            &mut write_graphs,
            from_named_aliases,
            ledger_id,
            GraphRole::UpdateTemplate,
            blanks,
        );
        let templates = parse_update_templates_with_ctx(delete_val, &template_root, &mut ctx)?;
        // Blank nodes are not allowed in delete templates (mirrors SPARQL 1.1
        // Update §19.8 note 8 on the JSON-LD surface): a blank node denotes a
        // fresh node, so the retraction would skolemize a brand-new SID and
        // silently match nothing. Stable `_:fdb-` ids already resolved to
        // constant SIDs above, so they pass. Nested objects without `@id`
        // also mint blank nodes and are rejected the same way.
        if let Some(label) = first_blank_node_in_templates(&templates) {
            return Err(TransactError::Parse(format!(
                "blank node {label} is not allowed in delete: it denotes a fresh node and can \
                 never match existing data. Use a variable bound by \"where\", a concrete @id, \
                 or a Fluree stable _:fdb- id."
            )));
        }
        if templates.is_empty() {
            // An explicit empty delete (e.g. `"delete": []`) is a no-op.
            // Still reject structurally-empty deletes like `{ "@id": "ex:foo" }`.
            if matches!(delete_val, Value::Array(arr) if arr.is_empty()) {
                Vec::new()
            } else {
                return Err(TransactError::Parse(
                    "delete must contain at least one predicate or @type".to_string(),
                ));
            }
        } else {
            templates
        }
    } else {
        Vec::new()
    };

    // Parse INSERT clause
    let insert = if let Some(insert_val) = obj.get("insert") {
        validate_type_fields(insert_val)?;
        let mut ctx = TemplateParseCtx::new(
            context,
            &mut vars,
            ns_registry,
            object_var_parsing,
            strict,
            &mut write_graphs,
            from_named_aliases,
            ledger_id,
            GraphRole::UpdateTemplate,
            blanks,
        );
        let templates = parse_update_templates_with_ctx(insert_val, &template_root, &mut ctx)?;
        if templates.is_empty() {
            return Err(TransactError::Parse(
                "insert must contain at least one predicate or @type (an object with only @id is not a valid insert)"
                    .to_string(),
            ));
        }
        templates
    } else {
        Vec::new()
    };

    let values = match obj.get("values") {
        Some(values_val) => Some(parse_inline_values(
            values_val,
            context,
            &mut vars,
            ns_registry,
            strict,
            blanks,
        )?),
        None => None,
    };

    Ok(UpdateClauses {
        vars,
        write_graphs,
        template_root_iri,
        delete,
        insert,
        values,
    })
}

fn parse_update_where_default_graph_iris(
    from_val: Option<&Value>,
    context: &ParsedContext,
    from_named_aliases: &HashMap<String, String>,
    strict: bool,
) -> Result<Option<Vec<String>>> {
    let Some(v) = from_val else {
        return Ok(None);
    };

    // Normalize a single graph selector value after alias resolution.
    // Returns Ok(None) for "default" (skip), Ok(Some(iri)) for a real graph,
    // or Err for "txn-meta".
    let resolve_single = |item: &Value| -> Result<Option<String>> {
        let resolved = resolve_graph_selector_value_for_update(item, from_named_aliases);
        match &resolved {
            Value::String(s) if s == "default" => Ok(None),
            Value::String(s) if s == "txn-meta" => Err(TransactError::Parse(
                "from: \"txn-meta\" is not currently supported as a default graph selector in updates"
                    .to_string(),
            )),
            _ => Ok(Some(expand_update_graph_iri(&resolved, context, strict)?)),
        }
    };

    match v {
        // String shorthand
        Value::String(_) => match resolve_single(v)? {
            Some(iri) => Ok(Some(vec![iri])),
            None => Ok(Some(Vec::new())),
        },
        // Array form: multiple default graphs (merged for default-graph patterns).
        Value::Array(arr) => {
            let mut out: Vec<String> = Vec::new();
            for item in arr {
                if let Some(iri) = resolve_single(item)? {
                    out.push(iri);
                }
            }
            Ok(Some(out))
        }
        // Object form: allow {"graph": ...} and ignore other dataset fields.
        Value::Object(obj) => {
            if let Some(graph) = obj.get("graph") {
                parse_update_where_default_graph_iris(Some(graph), context, from_named_aliases, strict)
            } else {
                Ok(None)
            }
        }
        _ => Err(TransactError::Parse(
            "from must be a string graph selector, an array of graph selectors, or an object with a 'graph' field"
                .to_string(),
        )),
    }
}

fn resolve_graph_selector_value_for_update(
    v: &Value,
    from_named_aliases: &HashMap<String, String>,
) -> Value {
    match v {
        Value::String(s) => from_named_aliases
            .get(s)
            .map(|iri| Value::String(iri.clone()))
            .unwrap_or_else(|| Value::String(s.clone())),
        _ => v.clone(),
    }
}

fn parse_update_where_named_graphs(
    from_named_val: Option<&Value>,
    context: &ParsedContext,
    strict: bool,
) -> Result<Option<Vec<crate::ir::UpdateNamedGraph>>> {
    let Some(v) = from_named_val else {
        return Ok(None);
    };

    let mut out: Vec<crate::ir::UpdateNamedGraph> = Vec::new();

    let items: Vec<Value> = match v {
        Value::Array(arr) => arr.clone(),
        _ => vec![v.clone()],
    };

    for item in items {
        match item {
            Value::String(_) | Value::Object(_) | Value::Array(_) => {
                if let Value::Object(obj) = &item {
                    // Accept query-style graph source objects: { "@id": "...", "graph": "<iri>", "alias": "x" }
                    let explicit_alias = obj
                        .get("alias")
                        .and_then(|a| a.as_str())
                        .map(std::string::ToString::to_string);
                    let graph_val = obj.get("graph").ok_or_else(|| {
                        TransactError::Parse(
                            "fromNamed objects must include a 'graph' field".to_string(),
                        )
                    })?;
                    // If no alias provided, use the raw graph selector string as an implicit alias.
                    // This makes `fromNamed: ["ex:g2"]` usable as `["graph", "ex:g2", ...]`
                    // in WHERE patterns even though GRAPH names are not expanded via @context.
                    let implicit_alias = graph_val.as_str().map(std::string::ToString::to_string);
                    let iri = expand_update_graph_iri(graph_val, context, strict)?;
                    out.push(crate::ir::UpdateNamedGraph {
                        iri,
                        alias: explicit_alias.or(implicit_alias),
                    });
                } else {
                    // String shorthand (or other selector shape): treat as graph IRI
                    let implicit_alias = item.as_str().map(std::string::ToString::to_string);
                    let iri = expand_update_graph_iri(&item, context, strict)?;
                    out.push(crate::ir::UpdateNamedGraph {
                        iri,
                        alias: implicit_alias,
                    });
                }
            }
            _ => {
                return Err(TransactError::Parse(
                    "fromNamed must be a string, an object, or an array of those".to_string(),
                ))
            }
        }
    }

    Ok(Some(out))
}

fn expand_update_graph_iri(v: &Value, context: &ParsedContext, strict: bool) -> Result<String> {
    let selector = match v {
        Value::String(s) => Value::Object({
            let mut m = serde_json::Map::new();
            m.insert("@id".to_string(), Value::String(s.clone()));
            m
        }),
        Value::Object(obj) => Value::Object(obj.clone()),
        Value::Array(arr) => {
            // JSON-LD expansion often represents a single node as a one-element array.
            // For graph selectors, multi-element arrays are ambiguous, so reject them
            // instead of silently truncating.
            if arr.len() != 1 {
                return Err(TransactError::Parse(
                    "graph selector array must contain exactly one element".to_string(),
                ));
            }
            let first = &arr[0];
            match first {
                Value::String(s) => Value::Object({
                    let mut m = serde_json::Map::new();
                    m.insert("@id".to_string(), Value::String(s.clone()));
                    m
                }),
                Value::Object(obj) => Value::Object(obj.clone()),
                _ => {
                    return Err(TransactError::Parse(
                        "graph selector must be a string IRI (or {\"@id\": ...})".to_string(),
                    ))
                }
            }
        }
        _ => {
            return Err(TransactError::Parse(
                "graph selector must be a string IRI (or {\"@id\": ...})".to_string(),
            ))
        }
    };

    let expanded = expand_with_context_policy(&selector, context, strict)?;
    let iri = match &expanded {
        Value::Array(arr) => arr
            .first()
            .and_then(|x| x.as_object())
            .and_then(|o| o.get("@id"))
            .and_then(|id| id.as_str())
            .map(std::string::ToString::to_string),
        Value::Object(o) => o
            .get("@id")
            .and_then(|id| id.as_str())
            .map(std::string::ToString::to_string),
        _ => None,
    }
    .ok_or_else(|| TransactError::Parse("graph selector must expand to an @id IRI".to_string()))?;

    Ok(iri)
}

/// Labels for one document's anonymous nodes: `_:b0`, `_:b1`, … in document
/// order, one sequence for the whole transaction body (every clause, every
/// `["graph", …]` item). It is never reset mid-document: two anonymous nodes
/// never share a label.
///
/// The positional labels are part of a stored blank node's identity (upsert
/// and sync skolemize under a payload-scoped id), so their spelling is kept.
/// A user-written label of the same form (`_:b3`) would merge with the
/// anonymous node given that number; the parse notes every such label, and
/// when one was also issued the document is parsed once more with the
/// issuer skipping those numbers. Only documents that were being corrupted
/// see different labels.
#[derive(Debug, Default)]
struct BlankIssuer {
    next: usize,
    /// Numbers of user `_:bN` labels to skip (the re-parse).
    skip: std::collections::HashSet<usize>,
    /// Numbers of the user `_:bN` labels seen in this parse.
    user: std::collections::HashSet<usize>,
    reparsed: bool,
}

impl BlankIssuer {
    fn issue(&mut self) -> TemplateTerm {
        while self.skip.contains(&self.next) {
            self.next += 1;
        }
        let label = format!("_:b{}", self.next);
        self.next += 1;
        TemplateTerm::BlankNode(label)
    }

    /// Record a user-written blank-node label.
    fn note_user_label(&mut self, label: &str) {
        let Some(digits) = label.strip_prefix("_:b") else {
            return;
        };
        // Only the canonical spelling `issue` produces can collide.
        let canonical = !digits.is_empty()
            && digits.bytes().all(|b| b.is_ascii_digit())
            && (digits == "0" || !digits.starts_with('0'));
        if let (true, Ok(n)) = (canonical, digits.parse::<usize>()) {
            self.user.insert(n);
        }
    }

    /// Whether a user label collided with an issued one. If so, reset for the
    /// one re-parse that skips every user number; a collision after the
    /// re-parse cannot happen and is refused rather than committed.
    fn needs_reparse(&mut self) -> Result<bool> {
        let collided = self
            .user
            .iter()
            .any(|n| *n < self.next && !self.skip.contains(n));
        if !collided {
            return Ok(false);
        }
        if self.reparsed {
            return Err(TransactError::Parse(
                "blank-node labels still collide after re-labelling anonymous nodes".to_string(),
            ));
        }
        *self = BlankIssuer {
            skip: std::mem::take(&mut self.user),
            reparsed: true,
            ..BlankIssuer::default()
        };
        Ok(true)
    }
}

/// Which graph names a selector may use, by where it was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphRole {
    /// Insert / upsert / graph-insert data: there is no WHERE, so a graph
    /// cannot be a variable.
    Data,
    /// An update's insert/delete templates: `?g` denotes the WHERE binding.
    UpdateTemplate,
    /// The update's transaction-level `graph` key (SPARQL `WITH`): a graph,
    /// never a variable.
    UpdateDefault,
}

struct TemplateParseCtx<'a> {
    context: &'a ParsedContext,
    vars: &'a mut VarRegistry,
    ns_registry: &'a mut NamespaceRegistry,
    object_var_parsing: bool,
    strict_compact_iri: bool,
    write_graphs: &'a mut WriteGraphs,
    from_named_aliases: &'a HashMap<String, String>,
    /// The ledger the transaction targets, for the `config` keyword.
    ledger_id: &'a str,
    /// Where graph selectors in this clause were written.
    role: GraphRole,
    /// Graph insert / sync: the request names the one graph the payload
    /// writes; every scope must be this one.
    fixed_root: Option<GraphScope>,
    /// The document's anonymous-node labels.
    blanks: &'a mut BlankIssuer,
}

impl<'a> TemplateParseCtx<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        context: &'a ParsedContext,
        vars: &'a mut VarRegistry,
        ns_registry: &'a mut NamespaceRegistry,
        object_var_parsing: bool,
        strict_compact_iri: bool,
        write_graphs: &'a mut WriteGraphs,
        from_named_aliases: &'a HashMap<String, String>,
        ledger_id: &'a str,
        role: GraphRole,
        blanks: &'a mut BlankIssuer,
    ) -> Self {
        Self {
            context,
            vars,
            ns_registry,
            object_var_parsing,
            strict_compact_iri,
            write_graphs,
            from_named_aliases,
            ledger_id,
            role,
            fixed_root: None,
            blanks,
        }
    }

    /// Expand a JSON-LD document, respecting strict policy.
    fn expand_document(&self, json: &Value) -> std::result::Result<Value, TransactError> {
        Ok(expand_with_context_policy(
            json,
            self.context,
            self.strict_compact_iri,
        )?)
    }

    /// Resolve a graph name written in this document. Every position (a node
    /// selector, a `["graph", g, …]` item, the update `graph` key, and a
    /// node-level `@graph` in `where`) classifies the name by the one order
    /// in [`classify_written_graph_name`]; here that means:
    ///
    /// 1. `?name` is a WHERE variable, in update templates only.
    /// 2. A `fromNamed` alias names its IRI (checked before the keywords, so
    ///    an alias named `config` keeps meaning its IRI).
    /// 3. A keyword: `default` is the default graph, `config` this ledger's
    ///    config graph; `txn-meta` is not a write target.
    /// 4. Anything else expands as a node identifier (`@id`-style,
    ///    `@base`-relative) and must be an absolute IRI.
    fn resolve_graph_name(&mut self, raw: &str, role: GraphRole) -> Result<GraphName> {
        use fluree_db_query::parse::WrittenGraphName as W;
        match fluree_db_query::parse::classify_written_graph_name(
            raw,
            self.from_named_aliases,
            self.context,
            self.strict_compact_iri,
        )? {
            W::Var(var) => match role {
                GraphRole::UpdateTemplate => Ok(GraphName::Var(self.vars.get_or_insert(var))),
                GraphRole::Data => Err(TransactError::Parse(format!(
                    "graph {raw:?} is a variable; a variable graph names the graph a \"where\" \
                     binds, so it is only valid in an update's insert or delete"
                ))),
                GraphRole::UpdateDefault => Err(TransactError::Parse(format!(
                    "the update \"graph\" key names the transaction's default graph and takes \
                     a graph IRI, not the variable {raw:?}"
                ))),
            },
            W::Alias { iri, .. } => named_graph(raw, iri),
            W::Default => Ok(GraphName::Default),
            W::Config => named_graph(
                raw,
                &fluree_db_core::graph_registry::config_graph_iri(self.ledger_id),
            ),
            W::TxnMeta => Err(TransactError::Parse(format!(
                "graph \"txn-meta\" names the reserved system graph <{}>, which is not a write \
                 target; transaction metadata goes in the \"txn-meta\" sidecar",
                fluree_db_core::graph_registry::txn_meta_graph_iri(self.ledger_id)
            ))),
            W::Expanded(iri) => named_graph(raw, &iri),
        }
    }

    /// The scope of a JSON-LD named graph's content: the graph the owning
    /// node's (expanded) `@id` names. The name must be an IRI: Fluree has no
    /// blank-node graph names, a variable cannot name one, and a graph object
    /// without an `@id` is only meaningful as the top-level envelope. The
    /// name is checked even for empty content; `None` then, so an empty graph
    /// is not registered as a write target.
    fn named_graph_scope(&mut self, id: Option<&Value>, empty: bool) -> Result<Option<GraphScope>> {
        let Some(Value::String(iri)) = id else {
            return Err(TransactError::Parse(
                "a node object with `@graph` content is a JSON-LD named graph and needs an `@id` \
                 naming the graph; only the top-level envelope may omit it"
                    .to_string(),
            ));
        };
        if iri.starts_with("_:") || iri.starts_with('?') {
            return Err(TransactError::Parse(format!(
                "a JSON-LD named graph must be named by an IRI, not {iri:?}"
            )));
        }
        if self.fixed_root.is_some() {
            return Err(TransactError::Parse(
                "payload must not address named graphs; the target graph is given by the request"
                    .to_string(),
            ));
        }
        let name = named_graph(iri, iri)?;
        Ok((!empty).then(|| self.write_graphs.scope(name)))
    }

    /// The scope a graph selector written in this clause opens.
    fn selector_scope(&mut self, raw: &str) -> Result<GraphScope> {
        let name = self.resolve_graph_name(raw, self.role)?;
        let scope = self.write_graphs.scope(name);
        match &self.fixed_root {
            Some(root) if *root != scope => Err(TransactError::Parse(
                "payload must not address a graph other than the target; the target graph is \
                 given by the request"
                    .to_string(),
            )),
            _ => Ok(scope),
        }
    }
}

/// A graph named by `raw`, resolved to `iri`, which must be absolute.
fn named_graph(raw: &str, iri: &str) -> Result<GraphName> {
    GraphIri::parse(iri).map(GraphName::Iri).map_err(|e| {
        TransactError::Parse(format!(
            "graph {raw:?} does not name an absolute IRI ({e}); use a full IRI, a compact IRI \
             whose prefix the @context defines, \"default\", or \"config\""
        ))
    })
}

/// First blank-node label appearing in any template term, if one exists.
///
/// Used to reject blank nodes in delete templates (they can never match
/// stored data). Stable `_:fdb-` ids never appear here — the term parsers
/// resolve them to constant `TemplateTerm::Sid`s.
fn first_blank_node_in_templates(templates: &[TripleTemplate]) -> Option<&str> {
    templates.iter().find_map(|t| {
        [&t.subject, &t.predicate, &t.object]
            .into_iter()
            .find_map(|term| match term {
                TemplateTerm::BlankNode(label) => Some(label.as_str()),
                _ => None,
            })
    })
}

/// The `["graph", <name>, <pattern>]` template item, as `(name, pattern)`.
fn graph_sugar(item: &Value) -> Option<(&Value, &Value)> {
    match item {
        Value::Array(arr) if arr.len() == 3 && arr[0].as_str() == Some("graph") => {
            Some((&arr[1], &arr[2]))
        }
        _ => None,
    }
}

/// Parse an update clause (`insert` / `delete`) in `root`, the scope the
/// update's `graph` key opens (the default graph without one). A
/// `["graph", <name>, <pattern>]` item scopes its pattern to that graph.
fn parse_update_templates_with_ctx(
    val: &Value,
    root: &GraphScope,
    ctx: &mut TemplateParseCtx<'_>,
) -> Result<Vec<TripleTemplate>> {
    let mut out: Vec<TripleTemplate> = Vec::new();
    if let Value::Array(items) = val {
        let mut plain_items: Vec<Value> = Vec::new();
        for item in items {
            if let Some((name, pattern)) = graph_sugar(item) {
                let GraphValue::Selector(raw) = classify_graph_value(name) else {
                    return Err(TransactError::Parse(
                        "a [\"graph\", <name>, <pattern>] item needs a graph IRI (string) as its \
                         name"
                            .to_string(),
                    ));
                };
                let scope = ctx.selector_scope(raw)?;
                let expanded = ctx.expand_document(pattern)?;
                parse_expanded_nodes(&expanded, &scope, ctx, &mut out)?;
                continue;
            }
            plain_items.push(item.clone());
        }
        if !plain_items.is_empty() {
            let expanded = ctx.expand_document(&Value::Array(plain_items))?;
            parse_expanded_nodes(&expanded, root, ctx, &mut out)?;
        }
        return Ok(out);
    }

    let expanded = ctx.expand_document(val)?;
    parse_expanded_nodes(&expanded, root, ctx, &mut out)?;
    Ok(out)
}

/// Extract and parse the @context from a JSON-LD document
fn extract_context(json: &Value) -> Result<ParsedContext> {
    if let Some(ctx_val) = json.get("@context") {
        Ok(parse_context(&normalize_context_value(ctx_val))?)
    } else {
        Ok(ParsedContext::new())
    }
}

/// Strip top-level transactor-reserved keys so the JSON-LD expander never
/// treats them as data predicates.
///
/// The reserved set is [`super::RESERVED_TXN_KEYS`] — control keys (`opts`,
/// `txn-meta`), HTTP routing (`ledger`), and dataset selectors (`from`,
/// `fromNamed`, `graph`, …). On the INSERT / UPSERT paths this strip runs on,
/// the routing / dataset keys are pure noise: `opts` / `txn-meta` are
/// extracted upstream by [`extract_txn_meta`], and `ledger` / `from*` are
/// resolved by the HTTP layer for routing only. (UPDATE consumes `from*` /
/// `graph` semantically and never calls this strip.)
///
/// In envelope form (with `@graph`), the expander already ignores extra
/// top-level keys — but the single-object form would otherwise leak each as
/// a stray literal predicate (e.g. a dangling `<subject> ledger "x:main"`
/// triple), so the strip is what keeps body-ledger writes clean.
///
/// Collision guard: in single-object form, if the document's own `@context`
/// defines a reserved name as a term, the user almost certainly means it as a
/// real predicate — silently stripping it would drop genuine triples. Since
/// the name is reserved (routing consumes `ledger` / `from*` before parse,
/// `opts` / `txn-meta` are control sidecars), we reject the collision with an
/// actionable error rather than guess. Envelope form is unaffected: there the
/// reserved key is top-level metadata, never a data predicate.
///
/// Returns `Cow::Borrowed` when no stripping is needed, `Cow::Owned` otherwise.
fn strip_opts_for_expansion<'a>(
    json: &'a Value,
    context: &ParsedContext,
) -> Result<std::borrow::Cow<'a, Value>> {
    use super::RESERVED_TXN_KEYS;
    match json.as_object() {
        Some(obj) if RESERVED_TXN_KEYS.iter().any(|k| obj.contains_key(*k)) => {
            // Only an envelope's top-level keys are metadata; a single node
            // (graph selector or not) and a named graph hold data there.
            let is_envelope = matches!(
                fluree_graph_json_ld::doc_shape(obj),
                Ok(fluree_graph_json_ld::DocShape::Envelope)
            );
            if !is_envelope {
                if let Some(k) = RESERVED_TXN_KEYS
                    .iter()
                    .find(|k| obj.contains_key(**k) && context.contains(k))
                {
                    return Err(TransactError::Parse(format!(
                        "@context term {k:?} collides with a reserved transactor key \
                         and would be silently dropped; rename the term \
                         (reserved keys: {RESERVED_TXN_KEYS:?})"
                    )));
                }
            }
            let mut cloned = obj.clone();
            for k in RESERVED_TXN_KEYS {
                cloned.remove(*k);
            }
            Ok(std::borrow::Cow::Owned(Value::Object(cloned)))
        }
        _ => Ok(std::borrow::Cow::Borrowed(json)),
    }
}

pub(crate) fn expand_datatype_iri(
    type_iri: &str,
    context: &ParsedContext,
    strict: bool,
) -> std::result::Result<String, fluree_graph_json_ld::JsonLdError> {
    expand_datatype_iri_with_policy(type_iri, context, strict)
}

fn expand_datatype_iri_with_policy(
    type_iri: &str,
    context: &ParsedContext,
    strict: bool,
) -> std::result::Result<String, fluree_graph_json_ld::JsonLdError> {
    // Try context resolution first (unchecked — we have builtin fallbacks below)
    let (expanded, entry) = fluree_graph_json_ld::details(type_iri, context);
    if entry.is_some() {
        return Ok(expanded);
    }

    // Builtin xsd: fallback (common in transactions without explicit xsd context)
    if let Some(local) = type_iri.strip_prefix("xsd:") {
        if let Some(full) = expand_builtin_xsd_datatype(local) {
            return Ok(full.to_string());
        }
    }

    // Builtin rdf: fallback
    if let Some(local) = type_iri.strip_prefix("rdf:") {
        let full = match local {
            rdf_names::JSON => Some(rdf::JSON),
            rdf_names::LANG_STRING => Some(rdf::LANG_STRING),
            _ => None,
        };
        if let Some(full) = full {
            return Ok(full.to_string());
        }
    }

    // No resolution path succeeded — apply strict guard
    fluree_graph_json_ld::details_with_policy(type_iri, context, strict)?;
    Ok(expanded)
}

fn expand_builtin_xsd_datatype(local: &str) -> Option<&'static str> {
    fluree_vocab::datatype::KnownDatatype::from_xsd_local(local).map(|dt| dt.canonical_form())
}

fn normalize_context_value(context_val: &Value) -> Value {
    if let Value::Object(map) = context_val {
        if let Some(base) = map.get("@base") {
            if !map.contains_key("@vocab") {
                let mut out = map.clone();
                out.insert("@vocab".to_string(), base.clone());
                return Value::Object(out);
            }
        }
    }
    context_val.clone()
}

/// Validate that any `@type` fields are strings (IRI) or arrays of strings.
///
/// JSON-LD allows `@type` values only as strings (or arrays). If an object/literal is used
/// (e.g., `{"@value": ...}`), some JSON-LD expansion implementations may silently drop it.
/// We enforce this early for better API errors.
fn validate_type_fields(v: &Value) -> Result<()> {
    match v {
        Value::Array(arr) => {
            for item in arr {
                validate_type_fields(item)?;
            }
        }
        Value::Object(obj) => {
            if let Some(t) = obj.get("@type") {
                let valid = match t {
                    Value::String(_) => true,
                    Value::Array(a) => a.iter().all(|x| matches!(x, Value::String(_))),
                    _ => false,
                };
                if !valid {
                    return Err(TransactError::Parse(format!(
                        "@type must be a string or array of strings, got: {t:?}"
                    )));
                }
            }
            for (_k, child) in obj {
                validate_type_fields(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

// WHERE clause parsing has been removed - we now use the query parser's
// parse_where_with_counters function for full pattern support (OPTIONAL, UNION, etc.)

fn parse_inline_values(
    value: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    strict: bool,
    blanks: &mut BlankIssuer,
) -> Result<InlineValues> {
    let arr = value.as_array().ok_or_else(|| {
        TransactError::Parse("values must be a 2-element array: [vars, rows]".to_string())
    })?;
    if arr.len() != 2 {
        return Err(TransactError::Parse(
            "values must be a 2-element array: [vars, rows]".to_string(),
        ));
    }

    let vars_val = &arr[0];
    let var_names: Vec<&str> = match vars_val {
        Value::String(s) => vec![s.as_str()],
        Value::Array(vs) => vs
            .iter()
            .map(|v| {
                v.as_str()
                    .ok_or_else(|| TransactError::Parse("values vars must be strings".to_string()))
            })
            .collect::<Result<Vec<_>>>()?,
        _ => {
            return Err(TransactError::Parse(
                "values vars must be a string or array of strings".to_string(),
            ))
        }
    };

    let mut var_ids = Vec::with_capacity(var_names.len());
    for name in var_names {
        if !name.starts_with('?') {
            return Err(TransactError::Parse(
                "values vars must start with '?'".to_string(),
            ));
        }
        var_ids.push(vars.get_or_insert(name));
    }

    let rows_val = arr[1]
        .as_array()
        .ok_or_else(|| TransactError::Parse("values rows must be an array".to_string()))?;
    let var_count = var_ids.len();

    let mut rows: Vec<Vec<TemplateTerm>> = Vec::with_capacity(rows_val.len());
    for row_val in rows_val {
        let cells: Vec<&Value> = match row_val {
            Value::Array(cells) => cells.iter().collect(),
            _ if var_count == 1 => vec![row_val],
            _ => {
                return Err(TransactError::Parse(
                    "values row must be an array (or scalar when one var)".to_string(),
                ))
            }
        };

        if cells.len() != var_count {
            return Err(TransactError::Parse(format!(
                "Invalid value binding: number of variables and values don't match (vars={}, row={})",
                var_count,
                cells.len()
            )));
        }

        let mut out_row = Vec::with_capacity(var_count);
        for cell in cells {
            out_row.push(parse_values_cell(
                cell,
                context,
                ns_registry,
                strict,
                blanks,
            )?);
        }
        rows.push(out_row);
    }

    Ok(InlineValues::new(var_ids, rows))
}

fn parse_values_cell(
    cell: &Value,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
    strict: bool,
    blanks: &mut BlankIssuer,
) -> Result<TemplateTerm> {
    match cell {
        Value::Null => Err(TransactError::Parse(
            "values cell cannot be null".to_string(),
        )),
        Value::Bool(b) => Ok(TemplateTerm::Value(FlakeValue::Boolean(*b))),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(TemplateTerm::Value(FlakeValue::Long(i)))
            } else if let Some(f) = n.as_f64() {
                Ok(TemplateTerm::Value(FlakeValue::Double(f)))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number type in values: {n}"
                )))
            }
        }
        Value::String(s) => Ok(TemplateTerm::Value(FlakeValue::String(s.clone()))),
        Value::Object(map) => {
            if let Some(id_val) = map.get("@id") {
                let id_str = id_val.as_str().ok_or_else(|| {
                    TransactError::Parse("@id in values must be a string".to_string())
                })?;
                let (expanded, _) =
                    fluree_graph_json_ld::details_with_policy(id_str, context, strict)?;
                if expanded.starts_with("_:") {
                    // Stable Fluree blank-node ids address the existing node;
                    // other labels keep fresh-mint skolemization semantics.
                    if let Some(sid) = crate::namespace::stable_blank_node_sid(&expanded) {
                        return Ok(TemplateTerm::Sid(sid));
                    }
                    blanks.note_user_label(&expanded);
                    return Ok(TemplateTerm::BlankNode(expanded.to_string()));
                }
                return Ok(TemplateTerm::Sid(ns_registry.sid_for_iri(&expanded)));
            }

            let value_val = map.get("@value").ok_or_else(|| {
                TransactError::Parse("values object must contain @id or @value".to_string())
            })?;

            if let Some(type_val) = map.get("@type").and_then(|v| v.as_str()) {
                if type_val == "@id" {
                    let id_str = value_val.as_str().ok_or_else(|| {
                        TransactError::Parse(
                            "@value must be a string when @type is @id".to_string(),
                        )
                    })?;
                    let (expanded, _) =
                        fluree_graph_json_ld::details_with_policy(id_str, context, strict)?;
                    return Ok(TemplateTerm::Sid(ns_registry.sid_for_iri(&expanded)));
                }

                let expanded_type = expand_datatype_iri(type_val, context, strict)?;
                let parsed = coerce_value_with_datatype(value_val, &expanded_type, ns_registry)?;
                return Ok(parsed.term);
            }

            match value_val {
                Value::String(s) => Ok(TemplateTerm::Value(FlakeValue::String(s.clone()))),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Ok(TemplateTerm::Value(FlakeValue::Long(i)))
                    } else if let Some(f) = n.as_f64() {
                        Ok(TemplateTerm::Value(FlakeValue::Double(f)))
                    } else {
                        Err(TransactError::Parse(format!(
                            "Unsupported number type in values: {n}"
                        )))
                    }
                }
                Value::Bool(b) => Ok(TemplateTerm::Value(FlakeValue::Boolean(*b))),
                _ => Err(TransactError::Parse(format!(
                    "Unsupported @value type in values: {value_val:?}"
                ))),
            }
        }
        _ => Err(TransactError::Parse(format!(
            "Unsupported values cell: {cell:?}"
        ))),
    }
}

/// Parse expanded JSON-LD (one node object or an array of them) in `scope`.
fn parse_expanded_triples_with_ctx(
    expanded: &Value,
    scope: &GraphScope,
    ctx: &mut TemplateParseCtx<'_>,
) -> Result<Vec<TripleTemplate>> {
    let mut out = Vec::new();
    parse_expanded_nodes(expanded, scope, ctx, &mut out)?;
    Ok(out)
}

/// [`parse_expanded_triples_with_ctx`], appending to `out`.
fn parse_expanded_nodes(
    expanded: &Value,
    scope: &GraphScope,
    ctx: &mut TemplateParseCtx<'_>,
    out: &mut Vec<TripleTemplate>,
) -> Result<()> {
    match expanded {
        Value::Array(arr) => {
            for item in arr {
                parse_expanded_object_with_ctx(item, scope, ctx, out)?;
            }
            Ok(())
        }
        Value::Object(_) => {
            parse_expanded_object_with_ctx(expanded, scope, ctx, out)?;
            Ok(())
        }
        _ => Err(TransactError::Parse(
            "Expected expanded object or array of objects".to_string(),
        )),
    }
}

/// Parse a single expanded JSON-LD node object into triple templates,
/// appended to `out`.
///
/// The node's statements, and those of every node nested in it, are written
/// to the node's scope: its own graph selector
/// (`{ "@id": "...", "@graph": "<graph iri>", ... }`, a Fluree extension) if
/// it has one, else `scope`, the enclosing node's. Nested node templates are
/// appended before the statement that references the nested node.
///
/// Returns the subject term assigned to this node (IRI, variable, or blank
/// node); callers that reference this node (e.g. as the object of a parent
/// triple) use it directly.
fn parse_expanded_object_with_ctx(
    expanded: &Value,
    scope: &GraphScope,
    ctx: &mut TemplateParseCtx<'_>,
    out: &mut Vec<TripleTemplate>,
) -> Result<TemplateTerm> {
    let obj = expanded
        .as_object()
        .ok_or_else(|| TransactError::Parse("Expected expanded object".to_string()))?;

    // A JSON-LD 1.1 named graph (`{"@id": G, "@graph": [nodes]}`): the node's
    // own properties stay in the enclosing scope, and the content nodes are
    // written to the graph its `@id` names (after the properties, below).
    let mut named_graph_content: Option<&[Value]> = None;
    let own_scope: GraphScope;
    let scope: &GraphScope = match obj.get("@graph").map(classify_graph_value) {
        None => scope,
        Some(GraphValue::Selector(raw)) => {
            own_scope = ctx.selector_scope(raw)?;
            &own_scope
        }
        Some(GraphValue::Content(nodes)) => {
            named_graph_content = Some(nodes);
            scope
        }
        Some(GraphValue::Invalid(why)) => return Err(TransactError::Parse(why.to_string())),
    };

    // Get subject from @id (already expanded IRI or variable); a node
    // without one is a fresh blank node.
    let subject = match obj.get("@id") {
        Some(id) => parse_expanded_id_with_ctx(id, ctx)?,
        None => ctx.blanks.issue(),
    };

    // Parse each predicate-object pair
    for (key, value) in obj {
        match key.as_str() {
            // Handled above, or (`@index`) carries no statement.
            "@id" | "@context" | "@graph" | "@index" => continue,
            // JSON-LD 1.1 §4.7: nodes described alongside this one, with no
            // statement linking them; they share its scope.
            "@included" => {
                for node in node_objects(value, "@included")? {
                    parse_expanded_object_with_ctx(node, scope, ctx, out)?;
                }
                continue;
            }
            // JSON-LD 1.1 §4.8: statements whose object is this node.
            "@reverse" => {
                parse_reverse_properties(value, &subject, scope, ctx, out)?;
                continue;
            }
            "@type" => {}
            k if is_keyword_form(k) => {
                return Err(TransactError::Parse(format!(
                    "JSON-LD keyword {k:?} is not supported on a node object"
                )));
            }
            _ => {}
        }

        if key == "@type" {
            // @type becomes rdf:type triples
            let predicate = TemplateTerm::Sid(ctx.ns_registry.sid_for_iri(TYPE));

            let types = match value {
                Value::Array(arr) => arr.iter().collect::<Vec<_>>(),
                _ => vec![value],
            };

            for type_val in types {
                if let Some(type_iri) = type_val.as_str() {
                    let object = if type_iri.starts_with('?') {
                        let var_id = ctx.vars.get_or_insert(type_iri);
                        TemplateTerm::Var(var_id)
                    } else {
                        TemplateTerm::Sid(ctx.ns_registry.sid_for_iri(type_iri))
                    };
                    scope.emit(
                        out,
                        ctx.ns_registry,
                        subject.clone(),
                        predicate.clone(),
                        object,
                        None,
                        None,
                    );
                } else {
                    return Err(TransactError::Parse(format!(
                        "Invalid @type value: expected IRI string, got: {type_val:?}"
                    )));
                }
            }
            continue;
        }

        if key == TYPE {
            return Err(TransactError::Parse(format!(
                "\"{TYPE}\" is not a valid predicate IRI. Please use the JSON-LD \"@type\" keyword instead."
            )));
        }

        // Regular predicate (expanded IRI)
        let predicate = if key.starts_with('?') {
            let var_id = ctx.vars.get_or_insert(key);
            TemplateTerm::Var(var_id)
        } else {
            TemplateTerm::Sid(ctx.ns_registry.sid_for_iri(key))
        };

        for parsed_value in parse_expanded_objects_with_ctx(value, scope, ctx, out)? {
            scope.emit(
                out,
                ctx.ns_registry,
                subject.clone(),
                predicate.clone(),
                parsed_value.term,
                parsed_value.dtc,
                parsed_value.list_index,
            );
        }
    }

    if let Some(nodes) = named_graph_content {
        if let Some(graph) = ctx.named_graph_scope(obj.get("@id"), nodes.is_empty())? {
            for node in nodes {
                parse_expanded_object_with_ctx(node, &graph, ctx, out)?;
            }
        }
    }

    Ok(subject)
}

/// A key spelled like a JSON-LD keyword (`@` then letters). Keys that
/// merely start with `@` (`@odata.etag`) are ordinary property names.
fn is_keyword_form(key: &str) -> bool {
    key.strip_prefix('@')
        .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_alphabetic()))
}

/// The node objects of an `@included` value (one node or an array of them).
fn node_objects<'v>(value: &'v Value, keyword: &str) -> Result<Vec<&'v Value>> {
    let items: Vec<&Value> = match value {
        Value::Array(items) => items.iter().collect(),
        other => vec![other],
    };
    for item in &items {
        let is_node = item
            .as_object()
            .is_some_and(|m| !m.contains_key("@value") && !m.contains_key("@list"));
        if !is_node {
            return Err(TransactError::Parse(format!(
                "{keyword} must hold node objects"
            )));
        }
    }
    Ok(items)
}

/// Emit a node's reverse properties (the expanded `@reverse` map): for each
/// property `p` and each value `v`, the statement `(v, p, owner)` in the
/// owner's scope. A value that is a node object is parsed in that scope
/// first.
fn parse_reverse_properties(
    value: &Value,
    owner: &TemplateTerm,
    scope: &GraphScope,
    ctx: &mut TemplateParseCtx<'_>,
    out: &mut Vec<TripleTemplate>,
) -> Result<()> {
    let maps: Vec<&serde_json::Map<String, Value>> = match value {
        Value::Object(map) => vec![map],
        Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_object().ok_or_else(|| {
                    TransactError::Parse("@reverse must be a map of properties".to_string())
                })
            })
            .collect::<Result<_>>()?,
        _ => {
            return Err(TransactError::Parse(
                "@reverse must be a map of properties".to_string(),
            ))
        }
    };
    for map in maps {
        for (property, values) in map {
            if property.starts_with('@') || property.starts_with('?') {
                return Err(TransactError::Parse(format!(
                    "@reverse may hold only property IRIs, not {property:?}"
                )));
            }
            let predicate = TemplateTerm::Sid(ctx.ns_registry.sid_for_iri(property));
            for node in node_objects(values, "a reverse property's value")? {
                let subject = match node.get("@id") {
                    Some(id) if node.as_object().is_some_and(|m| m.len() == 1) => {
                        parse_expanded_id_with_ctx(id, ctx)?
                    }
                    _ => parse_expanded_object_with_ctx(node, scope, ctx, out)?,
                };
                scope.emit(
                    out,
                    ctx.ns_registry,
                    subject,
                    predicate.clone(),
                    owner.clone(),
                    None,
                    None,
                );
            }
        }
    }
    Ok(())
}

/// Parse an expanded @id value
fn parse_expanded_id_with_ctx(
    value: &Value,
    ctx: &mut TemplateParseCtx<'_>,
) -> Result<TemplateTerm> {
    match value {
        Value::String(s) => {
            if s.starts_with('?') {
                // Variable
                let var_id = ctx.vars.get_or_insert(s);
                Ok(TemplateTerm::Var(var_id))
            } else if s.starts_with("_:") {
                // Stable Fluree blank-node ids resolve to the existing node;
                // other labels stay BlankNode for fresh-mint skolemization.
                if let Some(sid) = crate::namespace::stable_blank_node_sid(s) {
                    return Ok(TemplateTerm::Sid(sid));
                }
                ctx.blanks.note_user_label(s);
                Ok(TemplateTerm::BlankNode(s.clone()))
            } else {
                // Expanded IRI - encode as SID
                Ok(TemplateTerm::Sid(ctx.ns_registry.sid_for_iri(s)))
            }
        }
        _ => Err(TransactError::Parse(format!(
            "Expected string for @id, got: {value:?}"
        ))),
    }
}

/// The ledger unit-test transactions target.
#[cfg(test)]
const TEST_LEDGER: &str = "test:main";

/// Compatibility wrapper used by unit tests (parses an expanded `@id`).
#[cfg(test)]
fn parse_expanded_id(
    value: &Value,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
) -> Result<TemplateTerm> {
    let context = ParsedContext::new();
    let mut write_graphs = WriteGraphs::new();
    let empty_aliases: HashMap<String, String> = HashMap::new();
    let mut blanks = BlankIssuer::default();
    let mut ctx = TemplateParseCtx::new(
        &context,
        vars,
        ns_registry,
        true,
        true,
        &mut write_graphs,
        &empty_aliases,
        TEST_LEDGER,
        GraphRole::UpdateTemplate,
        &mut blanks,
    );
    parse_expanded_id_with_ctx(value, &mut ctx)
}

/// Parsed value with optional datatype constraint and list index
struct ParsedValue {
    term: TemplateTerm,
    dtc: Option<DatatypeConstraint>,
    list_index: Option<i32>,
}

impl ParsedValue {
    fn new(term: TemplateTerm) -> Self {
        Self {
            term,
            dtc: None,
            list_index: None,
        }
    }

    fn with_dtc(mut self, dtc: DatatypeConstraint) -> Self {
        self.dtc = Some(dtc);
        self
    }

    #[allow(dead_code)]
    fn with_list_index(mut self, index: i32) -> Self {
        self.list_index = Some(index);
        self
    }
}

/// Parse expanded object value(s)
///
/// In expanded JSON-LD, values are wrapped in arrays and may have @value/@type/@language.
/// Handles @list specially by expanding list elements into multiple ParsedValues with
/// list_index set.
fn parse_expanded_objects_with_ctx(
    value: &Value,
    scope: &GraphScope,
    ctx: &mut TemplateParseCtx<'_>,
    out: &mut Vec<TripleTemplate>,
) -> Result<Vec<ParsedValue>> {
    match value {
        Value::Array(arr) => {
            let mut results = Vec::new();
            for v in arr {
                // Check if this is a @list object
                if let Value::Object(obj) = v {
                    if let Some(list_val) = obj.get("@list") {
                        // Parse list and add all elements with their indices
                        let list_items = parse_list_values_with_ctx(list_val, scope, ctx, out)?;
                        results.extend(list_items);
                        continue;
                    }
                }
                // Not a @list, parse normally
                results.push(parse_expanded_value_with_ctx(v, scope, ctx, out)?);
            }
            Ok(results)
        }
        _ => Ok(vec![parse_expanded_value_with_ctx(value, scope, ctx, out)?]),
    }
}

/// Parse a single expanded value
///
/// Handles:
/// - `{"@id": "..."}` - reference (with optional nested property materialization)
/// - `{"@value": "...", "@type": "..."}` - typed literal
/// - `{"@value": "...", "@language": "..."}` - language-tagged string
/// - `{"@value": "..."}` - plain literal
/// - `{"@list": [...]}` - list
/// - `{"@variable": "..."}` - Fluree variable extension
/// - `{...}` - nested blank node (no @id/@value/@list/@variable)
fn parse_expanded_value_with_ctx(
    value: &Value,
    scope: &GraphScope,
    ctx: &mut TemplateParseCtx<'_>,
    out: &mut Vec<TripleTemplate>,
) -> Result<ParsedValue> {
    match value {
        Value::Object(obj) => {
            // Check for @id (reference)
            if let Some(id) = obj.get("@id") {
                // If the object has additional keys, materialize it as a nested node.
                let has_nested_props = obj
                    .keys()
                    .any(|k| k.as_str() != "@id" && k.as_str() != "@context");
                if has_nested_props {
                    parse_expanded_object_with_ctx(value, scope, ctx, out)?;
                }
                return Ok(ParsedValue::new(parse_expanded_id_with_ctx(id, ctx)?));
            }

            // Check for @value (literal)
            if let Some(val) = obj.get("@value") {
                return parse_literal_value_with_meta(
                    val,
                    obj,
                    ctx.context,
                    ctx.vars,
                    ctx.ns_registry,
                    ctx.object_var_parsing,
                    ctx.strict_compact_iri,
                );
            }

            // Check for @list (ordered collection)
            if let Some(list_val) = obj.get("@list") {
                return parse_list_value_with_ctx(list_val, scope, ctx, out);
            }

            if let Some(var_val) = obj.get("@variable") {
                let var = match var_val {
                    Value::String(s) => s.as_str(),
                    Value::Object(map) => {
                        map.get("@value").and_then(|v| v.as_str()).ok_or_else(|| {
                            TransactError::Parse("@variable must be a string".to_string())
                        })?
                    }
                    Value::Array(items) => items
                        .first()
                        .and_then(|item| match item {
                            Value::String(s) => Some(s.as_str()),
                            Value::Object(map) => map.get("@value").and_then(|v| v.as_str()),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            TransactError::Parse("@variable must be a string".to_string())
                        })?,
                    _ => {
                        return Err(TransactError::Parse(
                            "@variable must be a string".to_string(),
                        ))
                    }
                };
                if !var.starts_with('?') {
                    return Err(TransactError::Parse(
                        "@variable value must start with '?'".to_string(),
                    ));
                }
                let var_id = ctx.vars.get_or_insert(var);
                return Ok(ParsedValue::new(TemplateTerm::Var(var_id)));
            }

            // Nested node object without @id — treat as a blank node.
            // Any object that reaches this point has properties but none of the
            // JSON-LD value keywords (@id, @value, @list, @variable), so it must
            // be a node object. Per the JSON-LD spec, a node without @id is a
            // blank node — regardless of whether it has @type or not.
            let subject = parse_expanded_object_with_ctx(value, scope, ctx, out)?;
            Ok(ParsedValue::new(subject))
        }
        // Direct values (shouldn't happen in properly expanded JSON-LD, but handle for robustness).
        // String values are literals — only `{"@id": "..."}` or a context-declared
        // `@type: "@id"` (which expansion rewrites to `{"@id": ...}`) produces an IRI reference.
        Value::String(s) => {
            if s.starts_with('?') && ctx.object_var_parsing {
                let var_id = ctx.vars.get_or_insert(s);
                Ok(ParsedValue::new(TemplateTerm::Var(var_id)))
            } else {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::String(
                    s.clone(),
                ))))
            }
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Long(i))))
            } else if let Some(f) = n.as_f64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Double(f))))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number format: {n}"
                )))
            }
        }
        Value::Bool(b) => Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Boolean(
            *b,
        )))),
        _ => Err(TransactError::Parse(format!(
            "Unsupported value: {value:?}"
        ))),
    }
}

// Compatibility wrapper used by unit tests.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn parse_expanded_value(
    value: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    templates: &mut Vec<TripleTemplate>,
    object_var_parsing: bool,
) -> Result<ParsedValue> {
    let mut write_graphs = WriteGraphs::new();
    let no_aliases = HashMap::new();
    let mut blanks = BlankIssuer::default();
    let mut ctx = TemplateParseCtx::new(
        context,
        vars,
        ns_registry,
        object_var_parsing,
        true,
        &mut write_graphs,
        &no_aliases,
        TEST_LEDGER,
        GraphRole::UpdateTemplate,
        &mut blanks,
    );
    parse_expanded_value_with_ctx(value, &GraphScope::default_graph(), &mut ctx, templates)
}

/// Refuse a language tag that is not a `LANGTAG` (optionally with an RDF 1.2
/// base direction). Turtle and N-Triples have no escape for a tag, so an
/// invalid one would end the literal in every text serialization of it.
pub(crate) fn check_lang_tag(lang: &str) -> Result<()> {
    if fluree_graph_ir::syntax::is_lang_tag(lang) {
        Ok(())
    } else {
        Err(TransactError::Parse(format!(
            "invalid language tag {lang:?}: a language tag starts with a letter and \
             continues as `-`-separated alphanumeric subtags (for example \"en\" or \"en-GB\")"
        )))
    }
}

/// Parse a literal value with optional @type or @language, returning full metadata
#[allow(clippy::too_many_arguments)]
fn parse_literal_value_with_meta(
    val: &Value,
    obj: &serde_json::Map<String, Value>,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    object_var_parsing: bool,
    strict: bool,
) -> Result<ParsedValue> {
    // Check for @type first - always route through typed coercion when present
    if let Some(type_val) = obj.get("@type") {
        if let Some(type_iri) = type_val.as_str() {
            let expanded_type = expand_datatype_iri(type_iri, context, strict)?;

            // Handle @json specially
            if type_iri == "@json" || expanded_type == rdf::JSON {
                // Canonicalizing here gives one term per value, regardless
                // of how the writer serialized it (#1781). A string `@value`
                // holds an already serialized document, so it is
                // canonicalized in place when it parses and stored as written
                // when it does not.
                let json_string = match val {
                    Value::String(s) => {
                        fluree_graph_ir::canonicalize_json(s).unwrap_or_else(|_| s.clone())
                    }
                    _ => fluree_graph_ir::canonicalize_json_value(val),
                };
                let datatype_sid = ns_registry.sid_for_iri(rdf::JSON);
                return Ok(
                    ParsedValue::new(TemplateTerm::Value(FlakeValue::Json(json_string)))
                        .with_dtc(DatatypeConstraint::Explicit(datatype_sid)),
                );
            }

            // Handle @vector shorthand: "@vector" or full IRI both route
            // through the standard vector coercion path.
            let resolved_type = if type_iri == "@vector" {
                fluree_vocab::fluree::EMBEDDING_VECTOR
            } else if type_iri == "@fulltext" {
                fluree_vocab::fluree::FULL_TEXT
            } else {
                expanded_type.as_str()
            };

            // Route all @value types through typed coercion
            return coerce_value_with_datatype(val, resolved_type, ns_registry);
        }
    }

    // No explicit @type - handle based on JSON value type
    match val {
        Value::String(s) => {
            // Check if it's a variable - allow in @value for transaction WHERE patterns
            if s.starts_with('?') && object_var_parsing {
                let var_id = vars.get_or_insert(s);
                return Ok(ParsedValue::new(TemplateTerm::Var(var_id)));
            }

            // Check for @language
            if let Some(lang_val) = obj.get("@language") {
                if let Some(lang) = lang_val.as_str() {
                    check_lang_tag(lang)?;
                    return Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::String(
                        s.clone(),
                    )))
                    .with_dtc(DatatypeConstraint::LangTag(Arc::from(lang))));
                }
            }

            // `@value` with no `@type` is always a literal — never coerce to an IRI.
            // To produce an IRI reference, callers must use `{"@id": "..."}` or declare
            // `@type: "@id"` on the property in `@context`.
            Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::String(
                s.clone(),
            ))))
        }
        Value::Number(n) => {
            // No explicit type - infer from JSON number
            if let Some(i) = n.as_i64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Long(i))))
            } else if let Some(f) = n.as_f64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Double(f))))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number in @value: {n}"
                )))
            }
        }
        Value::Bool(b) => Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Boolean(
            *b,
        )))),
        _ => Err(TransactError::Parse(format!(
            "Unsupported @value type: {val:?}"
        ))),
    }
}

/// Coerce a JSON value to the appropriate FlakeValue based on the explicit datatype IRI.
///
/// This is a thin wrapper around `fluree_db_core::coerce::coerce_json_value` that:
/// 1. Delegates coercion to the core module (which enforces type compatibility and range validation)
/// 2. Wraps the result in `ParsedValue` with the datatype SID
///
/// # Type Compatibility Rules (enforced by core)
/// - String @value can be coerced to any type
/// - Numeric @value + xsd:string → ERROR
/// - Boolean @value + xsd:string → ERROR
/// - Numeric @value + xsd:boolean → ERROR
/// - Integer subtypes enforce range bounds (e.g., xsd:byte must be -128 to 127)
fn coerce_value_with_datatype(
    val: &Value,
    type_iri: &str,
    ns_registry: &mut NamespaceRegistry,
) -> Result<ParsedValue> {
    let datatype_sid = ns_registry.sid_for_iri(type_iri);

    // Delegate to core coercion module
    let flake_value = fluree_db_core::coerce::coerce_json_value(val, type_iri)
        .map_err(|e| TransactError::Parse(e.message))?;

    Ok(ParsedValue::new(TemplateTerm::Value(flake_value))
        .with_dtc(DatatypeConstraint::Explicit(datatype_sid)))
}

/// Convert a string value to the appropriate FlakeValue based on XSD datatype,
/// returning the explicit datatype SID for preservation in the flake.
///
/// This is a thin wrapper around the core coercion module that:
/// 1. Creates a JSON string value for coercion
/// 2. Delegates to `fluree_db_core::coerce::coerce_json_value`
/// 3. Wraps the result in `ParsedValue` with the datatype SID
///
/// # Coercion Policy (enforced by core)
/// - xsd:integer family: Try i64 first, fall back to BigInt; validates range bounds
/// - xsd:decimal: Parse as BigDecimal (preserves precision from string literals)
/// - xsd:double/float: Parse as f64
/// - xsd:dateTime/date/time: Parse into temporal FlakeValue variants
/// - xsd:boolean: Parse "true"/"false"/"1"/"0"
/// - Other types: Store as string with explicit datatype
#[cfg(test)]
fn convert_typed_value_with_meta(
    raw: &str,
    type_iri: &str,
    ns_registry: &mut NamespaceRegistry,
) -> Result<ParsedValue> {
    let datatype_sid = ns_registry.sid_for_iri(type_iri);

    // Create a JSON string value and delegate to core coercion
    let json_value = Value::String(raw.to_string());
    let flake_value = fluree_db_core::coerce::coerce_json_value(&json_value, type_iri)
        .map_err(|e| TransactError::Parse(e.message))?;

    Ok(ParsedValue::new(TemplateTerm::Value(flake_value))
        .with_dtc(DatatypeConstraint::Explicit(datatype_sid)))
}

/// Parse a @list value into a ParsedValue representing the first list element
///
/// Note: This function is called from `parse_expanded_value` which expects a single
/// ParsedValue. For proper @list support, the caller (`parse_expanded_objects`) detects
/// @list objects and uses `parse_list_values` instead to get all elements with indices.
///
/// This function only handles the fallback case and returns the first element.
/// An empty @list denotes the IRI rdf:nil and returns that single term, in
/// both this fallback and the `parse_list_values` path (issue #1694 twin).
fn parse_list_value_with_ctx(
    list_val: &Value,
    scope: &GraphScope,
    ctx: &mut TemplateParseCtx<'_>,
    out: &mut Vec<TripleTemplate>,
) -> Result<ParsedValue> {
    // @list should contain an array
    let items = match list_val {
        Value::Array(arr) => arr,
        _ => {
            return Err(TransactError::Parse(
                "@list must contain an array".to_string(),
            ))
        }
    };

    // An empty @list denotes the IRI rdf:nil (JSON-LD 1.1 § List to RDF
    // Conversion) — same as `parse_list_values_with_ctx`. This position used
    // to error ("Empty @list in unexpected position").
    if items.is_empty() {
        return Ok(ParsedValue::new(TemplateTerm::Sid(
            ctx.ns_registry.sid_for_iri(rdf::NIL),
        )));
    }

    // Parse the first element with index 0
    let first = &items[0];
    let mut parsed = parse_single_list_item_with_ctx(first, scope, ctx, out)?;
    parsed.list_index = Some(0);
    Ok(parsed)
}

/// Parse list items from a @list value, returning all elements with their indices
fn parse_list_values_with_ctx(
    list_val: &Value,
    scope: &GraphScope,
    ctx: &mut TemplateParseCtx<'_>,
    out: &mut Vec<TripleTemplate>,
) -> Result<Vec<ParsedValue>> {
    // @list should contain an array
    let items = match list_val {
        Value::Array(arr) => arr,
        _ => {
            return Err(TransactError::Parse(
                "@list must contain an array".to_string(),
            ))
        }
    };

    // An empty @list denotes the IRI rdf:nil (JSON-LD 1.1 § List to RDF
    // Conversion): store the one triple, as an ordinary IRI object with no
    // list index — the twin of the Turtle `()` fix (issue #1694). It used
    // to produce zero templates, silently losing the statement.
    if items.is_empty() {
        return Ok(vec![ParsedValue::new(TemplateTerm::Sid(
            ctx.ns_registry.sid_for_iri(rdf::NIL),
        ))]);
    }

    // Parse each item with its index
    let mut results = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let mut parsed = parse_single_list_item_with_ctx(item, scope, ctx, out)?;
        parsed.list_index = Some(index as i32);
        results.push(parsed);
    }

    Ok(results)
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn parse_list_values(
    list_val: &Value,
    context: &ParsedContext,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
    object_var_parsing: bool,
    templates: &mut Vec<TripleTemplate>,
) -> Result<Vec<ParsedValue>> {
    let mut write_graphs = WriteGraphs::new();
    let no_aliases = HashMap::new();
    let mut blanks = BlankIssuer::default();
    let mut ctx = TemplateParseCtx::new(
        context,
        vars,
        ns_registry,
        object_var_parsing,
        true,
        &mut write_graphs,
        &no_aliases,
        TEST_LEDGER,
        GraphRole::UpdateTemplate,
        &mut blanks,
    );
    parse_list_values_with_ctx(list_val, &GraphScope::default_graph(), &mut ctx, templates)
}

/// Parse a single item from a @list array
///
/// For `Value::Object` items, delegates to `parse_expanded_value` which already
/// handles all object shapes: `@id` refs, `@value` literals, `@list`, `@variable`,
/// and blank node objects (nested objects without JSON-LD keywords).
fn parse_single_list_item_with_ctx(
    item: &Value,
    scope: &GraphScope,
    ctx: &mut TemplateParseCtx<'_>,
    out: &mut Vec<TripleTemplate>,
) -> Result<ParsedValue> {
    match item {
        Value::Object(obj) => {
            // Nested @list inside a list item is not supported (would silently
            // lose data because parse_list_value only returns the first element).
            if obj.contains_key("@list") {
                return Err(TransactError::Parse(
                    "Nested @list not supported".to_string(),
                ));
            }
            parse_expanded_value_with_ctx(item, scope, ctx, out)
        }
        // Direct values — string list items are literals, not IRI references.
        // Wrap in `{"@id": "..."}` to produce an IRI.
        Value::String(s) => {
            if s.starts_with('?') {
                let var_id = ctx.vars.get_or_insert(s);
                Ok(ParsedValue::new(TemplateTerm::Var(var_id)))
            } else {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::String(
                    s.clone(),
                ))))
            }
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Long(i))))
            } else if let Some(f) = n.as_f64() {
                Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Double(f))))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number in list: {n}"
                )))
            }
        }
        Value::Bool(b) => Ok(ParsedValue::new(TemplateTerm::Value(FlakeValue::Boolean(
            *b,
        )))),
        _ => Err(TransactError::Parse(format!(
            "Unsupported list item type: {item:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_registry() -> NamespaceRegistry {
        NamespaceRegistry::new()
    }

    #[test]
    fn strip_removes_every_reserved_key_but_keeps_data() {
        // Single-object form: every reserved key must be stripped before
        // expansion (else it leaks as a stray literal predicate, e.g. a
        // dangling `<subject> ledger "x:main"` triple), while genuine data
        // predicates survive untouched.
        for key in super::super::RESERVED_TXN_KEYS {
            let mut obj = serde_json::Map::new();
            obj.insert("ex:name".to_string(), json!("Alice"));
            obj.insert((*key).to_string(), json!("x:main"));
            let doc = Value::Object(obj);
            let stripped = strip_opts_for_expansion(&doc, &ParsedContext::new()).unwrap();
            assert!(
                stripped.get(key).is_none(),
                "reserved key {key:?} must be stripped before expansion"
            );
            assert!(
                stripped.get("ex:name").is_some(),
                "data predicate must survive stripping of {key:?}"
            );
        }
    }

    #[test]
    fn strip_errors_when_reserved_key_is_a_context_term() {
        // Single-object form: a reserved name defined as a real @context term
        // is almost certainly intended as data. Silently stripping it would
        // drop genuine triples, so we reject the collision instead of guessing.
        for key in super::super::RESERVED_TXN_KEYS {
            let mut ctx = ParsedContext::new();
            ctx.terms.insert(
                (*key).to_string(),
                fluree_graph_json_ld::ContextEntry {
                    id: Some(format!("http://example.org/{key}")),
                    ..Default::default()
                },
            );
            let mut obj = serde_json::Map::new();
            obj.insert("ex:name".to_string(), json!("Alice"));
            obj.insert((*key).to_string(), json!("data value"));
            let doc = Value::Object(obj);
            let err = strip_opts_for_expansion(&doc, &ctx).unwrap_err();
            assert!(
                matches!(err, TransactError::Parse(ref m) if m.contains(key)),
                "collision on {key:?} should error, got {err:?}"
            );
        }
    }

    #[test]
    fn strip_allows_reserved_context_term_in_envelope_form() {
        // Envelope form (with @graph) never treats top-level keys as data
        // predicates, so a context-term collision is harmless and must not
        // error — the reserved key is just stripped as routing/control noise.
        let mut ctx = ParsedContext::new();
        ctx.terms.insert(
            "graph".to_string(),
            fluree_graph_json_ld::ContextEntry {
                id: Some("http://example.org/graph".to_string()),
                ..Default::default()
            },
        );
        let doc = json!({
            "graph": "x:main",
            "@graph": [{"ex:name": "Alice"}]
        });
        let stripped = strip_opts_for_expansion(&doc, &ctx).unwrap();
        assert!(stripped.get("graph").is_none());
        assert!(stripped.get("@graph").is_some());
    }

    #[test]
    fn test_parse_insert_with_context() {
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:alice",
            "ex:name": "Alice",
            "ex:age": 30
        });

        let txn = parse_transaction(
            &json,
            TxnType::Insert,
            TxnOpts::default(),
            &mut ns_registry,
            TEST_LEDGER,
        )
        .unwrap();

        assert_eq!(txn.txn_type, TxnType::Insert);
        assert_eq!(txn.insert_templates.len(), 2);
        assert!(txn.where_patterns.is_empty());
        assert!(txn.delete_templates.is_empty());

        // Check that http://example.org/ was registered
        assert!(ns_registry.has_prefix("http://example.org/"));
    }

    #[test]
    fn test_parse_update_with_context() {
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/"},
            "where": { "@id": "?s", "ex:name": "?name" },
            "delete": { "@id": "?s", "ex:name": "?name" },
            "insert": { "@id": "?s", "ex:name": "New Name" }
        });

        let txn = parse_update(&json, TxnOpts::default(), &mut ns_registry, TEST_LEDGER).unwrap();

        assert_eq!(txn.txn_type, TxnType::Update);
        assert_eq!(txn.where_patterns.len(), 1);
        assert_eq!(txn.delete_templates.len(), 1);
        assert_eq!(txn.insert_templates.len(), 1);
    }

    #[test]
    fn test_parse_update_without_insert_or_delete_rejected() {
        let mut ns_registry = test_registry();

        // A Cypher envelope mistakenly posted as a JSON-LD update: no
        // insert/delete clause means the transaction could never change data.
        let cypher_envelope = json!({
            "cypher": "MERGE (n:Person {name: 'Alice'})",
            "params": {}
        });
        let err = parse_update(
            &cypher_envelope,
            TxnOpts::default(),
            &mut ns_registry,
            TEST_LEDGER,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("\"insert\" or \"delete\""),
            "unexpected error: {err}"
        );

        // where-only updates are equally inert and rejected.
        let where_only = json!({
            "@context": {"ex": "http://example.org/"},
            "where": { "@id": "?s", "ex:name": "?name" }
        });
        let err = parse_update(
            &where_only,
            TxnOpts::default(),
            &mut ns_registry,
            TEST_LEDGER,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("\"insert\" or \"delete\""),
            "unexpected error: {err}"
        );

        // An explicit empty delete keeps its documented no-op semantics.
        let explicit_empty_delete = json!({
            "@context": {"ex": "http://example.org/"},
            "delete": []
        });
        let txn = parse_update(
            &explicit_empty_delete,
            TxnOpts::default(),
            &mut ns_registry,
            TEST_LEDGER,
        )
        .unwrap();
        assert!(txn.delete_templates.is_empty());
        assert!(txn.insert_templates.is_empty());
    }

    #[test]
    fn test_parse_update_from_named_sets_where_named_graphs() {
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/"},
            "fromNamed": [
                { "alias": "g2", "graph": "http://example.org/g2" }
            ],
            "where": [
                ["graph", "g2", { "@id": "ex:s", "ex:p": "?o" }]
            ],
            "insert": [
                ["graph", "http://example.org/g2", { "@id": "ex:s", "ex:q": "?o" }]
            ]
        });

        let txn = parse_update(&json, TxnOpts::default(), &mut ns_registry, TEST_LEDGER).unwrap();
        let named = txn
            .update_where_named_graphs
            .as_ref()
            .expect("expected fromNamed to populate txn.update_where_named_graphs");
        assert_eq!(named.len(), 1);
        assert_eq!(named[0].iri, "http://example.org/g2");
        assert_eq!(named[0].alias.as_deref(), Some("g2"));
    }

    #[test]
    fn test_parse_update_from_named_string_is_implicit_alias() {
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "fromNamed": ["ex:g2"],
            "where": [
                ["graph", "ex:g2", { "@id": "ex:s", "ex:p": "?o" }]
            ],
            "insert": [
                ["graph", "ex:g2", { "@id": "ex:s", "ex:q": "touched" }]
            ]
        });

        let txn = parse_update(&json, TxnOpts::default(), &mut ns_registry, TEST_LEDGER).unwrap();
        let named = txn
            .update_where_named_graphs
            .as_ref()
            .expect("expected fromNamed to populate txn.update_where_named_graphs");
        assert_eq!(named.len(), 1);
        assert_eq!(named[0].iri, "http://example.org/ns/g2");
        assert_eq!(named[0].alias.as_deref(), Some("ex:g2"));
    }

    #[test]
    fn test_parse_update_allows_from_named_alias_in_template_graph_selector() {
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/"},
            "fromNamed": [
                { "alias": "g2", "graph": "http://example.org/g2" }
            ],
            "values": ["?x", [1]],
            "insert": [
                ["graph", "g2", { "@id": "ex:s", "ex:p": "v" }]
            ]
        });

        let txn = parse_update(&json, TxnOpts::default(), &mut ns_registry, TEST_LEDGER).unwrap();
        assert!(
            txn.write_graphs.contains("http://example.org/g2"),
            "expected write_graphs to contain resolved graph IRI for alias g2"
        );
        assert!(
            txn.insert_templates
                .iter()
                .any(|t| t.graph == TemplateGraph::Iri("http://example.org/g2".into())),
            "expected insert templates to target the resolved graph IRI"
        );
    }

    #[test]
    fn test_parse_variable() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let term = parse_expanded_id(&json!("?x"), &mut vars, &mut ns_registry).unwrap();

        match term {
            TemplateTerm::Var(id) => assert_eq!(id, vars.get_or_insert("?x")),
            _ => panic!("Expected variable"),
        }
    }

    #[test]
    fn test_parse_blank_node() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let term = parse_expanded_id(&json!("_:b1"), &mut vars, &mut ns_registry).unwrap();

        match term {
            TemplateTerm::BlankNode(label) => assert_eq!(label, "_:b1"),
            _ => panic!("Expected blank node"),
        }
    }

    #[test]
    fn test_parse_typed_literal() {
        let mut ns_registry = test_registry();

        // Integer - should preserve xsd:integer datatype
        let result = convert_typed_value_with_meta(
            "42",
            "http://www.w3.org/2001/XMLSchema#integer",
            &mut ns_registry,
        )
        .unwrap();
        assert!(matches!(
            result.term,
            TemplateTerm::Value(FlakeValue::Long(42))
        ));
        // Verify datatype is preserved
        let dtc = result.dtc.as_ref().expect("should have dtc");
        assert!(dtc.datatype().name.as_ref().contains("integer"));

        // Double - should preserve xsd:double datatype
        let result = convert_typed_value_with_meta(
            "3.13",
            "http://www.w3.org/2001/XMLSchema#double",
            &mut ns_registry,
        )
        .unwrap();
        if let TemplateTerm::Value(FlakeValue::Double(f)) = result.term {
            assert!((f - 3.13).abs() < 0.001);
        } else {
            panic!("Expected double");
        }
        assert!(result.dtc.is_some());

        // Boolean - should preserve xsd:boolean datatype
        let result = convert_typed_value_with_meta(
            "true",
            "http://www.w3.org/2001/XMLSchema#boolean",
            &mut ns_registry,
        )
        .unwrap();
        assert!(matches!(
            result.term,
            TemplateTerm::Value(FlakeValue::Boolean(true))
        ));
        assert!(result.dtc.is_some());
    }

    #[test]
    fn test_parse_rdf_type() {
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/", "Person": "ex:Person"},
            "@id": "ex:alice",
            "@type": "Person"
        });

        let txn = parse_transaction(
            &json,
            TxnType::Insert,
            TxnOpts::default(),
            &mut ns_registry,
            TEST_LEDGER,
        )
        .unwrap();

        // Should have one triple: ex:alice rdf:type ex:Person
        assert_eq!(txn.insert_templates.len(), 1);

        let template = &txn.insert_templates[0];
        // Predicate should be rdf:type
        if let TemplateTerm::Sid(sid) = &template.predicate {
            assert_eq!(sid.namespace_code, 3); // NS_RDF
            assert_eq!(sid.name.as_ref(), "type");
        } else {
            panic!("Expected Sid for predicate");
        }
    }

    #[test]
    fn test_parse_value_object() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let mut templates: Vec<TripleTemplate> = Vec::new();
        let ctx = ParsedContext::new();

        // @value with @type - should preserve datatype
        let val = json!({"@value": "42", "@type": "http://www.w3.org/2001/XMLSchema#integer"});
        let result = parse_expanded_value(
            &val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            &mut templates,
            true,
        )
        .unwrap();
        assert!(matches!(
            result.term,
            TemplateTerm::Value(FlakeValue::Long(42))
        ));
        let dtc = result.dtc.as_ref().expect("should have dtc");
        assert!(dtc.datatype().name.as_ref().contains("integer"));
    }

    #[test]
    fn test_parse_value_object_builtin_xsd_curie_without_context() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let mut templates: Vec<TripleTemplate> = Vec::new();
        let ctx = ParsedContext::new();

        let val = json!({"@value": "before", "@type": "xsd:string"});
        let result = parse_expanded_value(
            &val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            &mut templates,
            true,
        )
        .unwrap();

        assert!(matches!(
            result.term,
            TemplateTerm::Value(FlakeValue::String(ref s)) if s == "before"
        ));
        let dtc = result.dtc.as_ref().expect("should have dtc");
        assert_eq!(dtc.datatype().namespace_code, 2);
        assert_eq!(dtc.datatype().name.as_ref(), "string");
    }

    /// An `@language` that is not a `LANGTAG` is refused at ingest: no text
    /// serialization could write it without it ending the literal.
    #[test]
    fn test_parse_invalid_language_tag_is_rejected() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let mut templates: Vec<TripleTemplate> = Vec::new();
        let ctx = ParsedContext::new();
        let mut parse = |lang: &str| {
            parse_expanded_value(
                &json!({"@value": "hi", "@language": lang}),
                &ctx,
                &mut vars,
                &mut ns_registry,
                &mut templates,
                true,
            )
        };
        let err = parse("en . <urn:injected> <urn:p> \"pwned\" . #")
            .err()
            .expect("invalid tag must be refused");
        assert!(err.to_string().contains("invalid language tag"), "{err}");
        assert!(parse("en--ltr").is_ok());
    }

    #[test]
    fn test_parse_language_tagged_string() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let mut templates: Vec<TripleTemplate> = Vec::new();
        let ctx = ParsedContext::new();

        // @value with @language
        let val = json!({"@value": "Hello", "@language": "en"});
        let result = parse_expanded_value(
            &val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            &mut templates,
            true,
        )
        .unwrap();
        assert!(matches!(
            result.term,
            TemplateTerm::Value(FlakeValue::String(_))
        ));
        assert_eq!(
            result
                .dtc
                .as_ref()
                .and_then(|d: &DatatypeConstraint| d.lang_tag()),
            Some("en")
        );
    }

    #[test]
    fn test_parse_list_values() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();

        // Parse a @list with three string items
        let list_val = json!(["a", "b", "c"]);
        let mut templates = Vec::new();
        let results = parse_list_values(
            &list_val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            true,
            &mut templates,
        )
        .unwrap();

        assert_eq!(results.len(), 3);

        // Check each item has correct list_index
        assert_eq!(results[0].list_index, Some(0));
        assert_eq!(results[1].list_index, Some(1));
        assert_eq!(results[2].list_index, Some(2));

        // Check values
        assert!(matches!(
            &results[0].term,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "a"
        ));
        assert!(matches!(
            &results[1].term,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "b"
        ));
        assert!(matches!(
            &results[2].term,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "c"
        ));
    }

    #[test]
    fn test_parse_empty_list() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();

        // An empty @list denotes rdf:nil: ONE ParsedValue, an ordinary IRI
        // object with no list index (issue #1694 twin — it used to produce
        // zero values, silently losing the statement).
        let list_val = json!([]);
        let mut templates = Vec::new();
        let results = parse_list_values(
            &list_val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            true,
            &mut templates,
        )
        .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].list_index.is_none());
        let nil_sid = ns_registry.sid_for_iri(rdf::NIL);
        assert!(
            matches!(&results[0].term, TemplateTerm::Sid(sid) if *sid == nil_sid),
            "empty @list must parse to the rdf:nil IRI"
        );
        assert!(templates.is_empty(), "no side-emitted templates");
    }

    /// The singular fallback (`parse_expanded_value` reaching a `@list`
    /// object outside the array position) must agree with the plural path:
    /// an empty `@list` is the single term rdf:nil. It used to error with
    /// "Empty @list in unexpected position".
    #[test]
    fn test_parse_empty_list_single_value_position() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let mut templates: Vec<TripleTemplate> = Vec::new();
        let ctx = ParsedContext::new();

        let val = json!({"@list": []});
        let result = parse_expanded_value(
            &val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            &mut templates,
            true,
        )
        .unwrap();
        assert!(result.list_index.is_none());
        let nil_sid = ns_registry.sid_for_iri(rdf::NIL);
        assert!(
            matches!(&result.term, TemplateTerm::Sid(sid) if *sid == nil_sid),
            "empty @list must parse to the rdf:nil IRI"
        );
    }

    #[test]
    fn test_parse_list_in_insert() {
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();

        // Insert with @list - expanded JSON-LD form
        let json = json!([{
            "@id": "http://example.org/alice",
            "http://example.org/colors": [{"@list": [
                {"@value": "red"},
                {"@value": "green"},
                {"@value": "blue"}
            ]}]
        }]);

        let mut vars = VarRegistry::new();
        let mut write_graphs = WriteGraphs::new();
        let empty_aliases = HashMap::new();
        let mut blanks = BlankIssuer::default();
        let mut parse_ctx = TemplateParseCtx::new(
            &ctx,
            &mut vars,
            &mut ns_registry,
            true,
            true,
            &mut write_graphs,
            &empty_aliases,
            TEST_LEDGER,
            GraphRole::UpdateTemplate,
            &mut blanks,
        );
        let templates =
            parse_expanded_triples_with_ctx(&json, &GraphScope::default_graph(), &mut parse_ctx)
                .unwrap();

        // Should have 3 templates, one for each list item
        assert_eq!(templates.len(), 3);

        // Check list indices
        assert_eq!(templates[0].list_index, Some(0));
        assert_eq!(templates[1].list_index, Some(1));
        assert_eq!(templates[2].list_index, Some(2));

        // Check values
        assert!(matches!(
            &templates[0].object,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "red"
        ));
        assert!(matches!(
            &templates[1].object,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "green"
        ));
        assert!(matches!(
            &templates[2].object,
            TemplateTerm::Value(FlakeValue::String(s)) if s == "blue"
        ));
    }

    #[test]
    fn test_parse_nested_blank_node() {
        // A property value that is a node object without @id should be treated as a blank node.
        // Input is in expanded JSON-LD form (arrays around values, @type is array of strings).
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();
        let mut vars = VarRegistry::new();
        let mut write_graphs = WriteGraphs::new();

        let expanded = json!([{
            "@id": "http://example.org/thing/1",
            "http://example.org/relatedTo": [{
                "@type": ["http://example.org/Widget"],
                "http://example.org/name": [{"@value": "nested-widget"}]
            }]
        }]);

        let empty_aliases = HashMap::new();
        let mut blanks = BlankIssuer::default();
        let mut parse_ctx = TemplateParseCtx::new(
            &ctx,
            &mut vars,
            &mut ns_registry,
            false,
            true,
            &mut write_graphs,
            &empty_aliases,
            TEST_LEDGER,
            GraphRole::UpdateTemplate,
            &mut blanks,
        );
        let templates = parse_expanded_triples_with_ctx(
            &expanded,
            &GraphScope::default_graph(),
            &mut parse_ctx,
        )
        .unwrap();

        // Should have 3 triples (order: nested triples first, then parent reference):
        //   _:b0    rdf:type  Widget           (nested, materialized first)
        //   _:b0    name      "nested-widget"  (nested, materialized first)
        //   thing/1 relatedTo _:b0             (parent reference, added last)
        assert_eq!(templates.len(), 3);

        // Find the parent→blank node reference triple (parent subject is a Sid)
        let ref_triple = templates
            .iter()
            .find(|t| matches!(&t.subject, TemplateTerm::Sid(_)))
            .expect("Expected a triple with parent Sid subject");
        assert!(matches!(&ref_triple.object, TemplateTerm::BlankNode(_)));

        // Extract the blank node label from the reference
        let bnode_label = match &ref_triple.object {
            TemplateTerm::BlankNode(label) => label.clone(),
            _ => panic!("Expected BlankNode"),
        };

        // The other 2 triples should use the same blank node as subject
        let bnode_triples: Vec<_> = templates
            .iter()
            .filter(|t| matches!(&t.subject, TemplateTerm::BlankNode(_)))
            .collect();
        assert_eq!(bnode_triples.len(), 2);
        for t in &bnode_triples {
            let label = match &t.subject {
                TemplateTerm::BlankNode(l) => l.as_str(),
                _ => unreachable!(),
            };
            assert_eq!(label, bnode_label);
        }
    }

    #[test]
    fn test_parse_doubly_nested_blank_nodes() {
        // Two levels of nesting, both without @id — must get distinct blank node IDs.
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();
        let mut vars = VarRegistry::new();
        let mut write_graphs = WriteGraphs::new();

        let expanded = json!([{
            "@id": "http://example.org/root",
            "http://example.org/outer": [{
                "@type": ["http://example.org/Outer"],
                "http://example.org/inner": [{
                    "@type": ["http://example.org/Inner"],
                    "http://example.org/value": [{"@value": "deep"}]
                }]
            }]
        }]);

        let empty_aliases = HashMap::new();
        let mut blanks = BlankIssuer::default();
        let mut parse_ctx = TemplateParseCtx::new(
            &ctx,
            &mut vars,
            &mut ns_registry,
            false,
            true,
            &mut write_graphs,
            &empty_aliases,
            TEST_LEDGER,
            GraphRole::UpdateTemplate,
            &mut blanks,
        );
        let templates = parse_expanded_triples_with_ctx(
            &expanded,
            &GraphScope::default_graph(),
            &mut parse_ctx,
        )
        .unwrap();

        // Collect all blank node labels used as subjects
        let bnode_subjects: Vec<&str> = templates
            .iter()
            .filter_map(|t| match &t.subject {
                TemplateTerm::BlankNode(label) => Some(label.as_str()),
                _ => None,
            })
            .collect();

        // There should be at least 2 distinct blank node labels (outer + inner)
        let mut unique: Vec<&str> = bnode_subjects.clone();
        unique.sort();
        unique.dedup();
        assert!(
            unique.len() >= 2,
            "Expected at least 2 distinct blank node labels, got: {unique:?}"
        );
    }

    #[test]
    fn test_parse_sibling_nested_blank_nodes() {
        // Two sibling nested objects without @id under different properties — distinct blank nodes.
        let mut ns_registry = test_registry();
        let ctx = ParsedContext::new();
        let mut vars = VarRegistry::new();
        let mut write_graphs = WriteGraphs::new();

        let expanded = json!([{
            "@id": "http://example.org/parent",
            "http://example.org/left": [{
                "@type": ["http://example.org/Left"],
                "http://example.org/label": [{"@value": "L"}]
            }],
            "http://example.org/right": [{
                "@type": ["http://example.org/Right"],
                "http://example.org/label": [{"@value": "R"}]
            }]
        }]);

        let empty_aliases = HashMap::new();
        let mut blanks = BlankIssuer::default();
        let mut parse_ctx = TemplateParseCtx::new(
            &ctx,
            &mut vars,
            &mut ns_registry,
            false,
            true,
            &mut write_graphs,
            &empty_aliases,
            TEST_LEDGER,
            GraphRole::UpdateTemplate,
            &mut blanks,
        );
        let templates = parse_expanded_triples_with_ctx(
            &expanded,
            &GraphScope::default_graph(),
            &mut parse_ctx,
        )
        .unwrap();

        // Collect blank node labels used as objects of the parent (the references)
        let bnode_refs: Vec<&str> = templates
            .iter()
            .filter(|t| matches!(&t.subject, TemplateTerm::Sid(_)))
            .filter_map(|t| match &t.object {
                TemplateTerm::BlankNode(label) => Some(label.as_str()),
                _ => None,
            })
            .collect();

        assert_eq!(
            bnode_refs.len(),
            2,
            "Expected 2 blank node references from parent"
        );
        assert_ne!(
            bnode_refs[0], bnode_refs[1],
            "Sibling blank nodes must have distinct labels"
        );
    }

    #[test]
    fn test_parse_nested_blank_node_insert() {
        // End-to-end: parse_insert with compact JSON-LD containing nested blank nodes.
        // This mirrors the real-world scenario from the bug report.
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {
                "ex": "http://example.org/",
                "prov": "http://www.w3.org/ns/prov#"
            },
            "@id": "ex:calendar/1",
            "@type": "ex:Calendar",
            "prov:wasGeneratedBy": {
                "@type": "prov:Generation",
                "prov:atTime": "2026-02-14T18:58:49Z",
                "prov:hadActivity": {
                    "@type": "prov:Activity",
                    "prov:atLocation": "row:1"
                }
            }
        });

        let txn = parse_transaction(
            &json,
            TxnType::Insert,
            TxnOpts::default(),
            &mut ns_registry,
            TEST_LEDGER,
        )
        .unwrap();

        // Should succeed and produce triples for:
        //   calendar/1  rdf:type       Calendar
        //   calendar/1  wasGeneratedBy _:b0
        //   _:b0        rdf:type       Generation
        //   _:b0        atTime         "2026-02-14T18:58:49Z"
        //   _:b0        hadActivity    _:b1
        //   _:b1        rdf:type       Activity
        //   _:b1        atLocation     "row:1"
        assert!(
            txn.insert_templates.len() >= 7,
            "Expected at least 7 triples, got {}",
            txn.insert_templates.len()
        );

        // Verify at least 2 distinct blank node subjects exist
        let bnode_subjects: std::collections::HashSet<_> = txn
            .insert_templates
            .iter()
            .filter_map(|t| match &t.subject {
                TemplateTerm::BlankNode(label) => Some(label.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            bnode_subjects.len() >= 2,
            "Expected at least 2 distinct blank node subjects, got: {bnode_subjects:?}"
        );
    }

    #[test]
    fn test_parse_nested_blank_node_without_type() {
        // Nested blank nodes do NOT require @type. Any object with properties
        // but no @id is a blank node per the JSON-LD spec.
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:andrew",
            "ex:name": "andrew",
            "ex:friend": {
                "ex:name": "ben",
                "ex:friend": {
                    "ex:name": "jake"
                }
            }
        });

        let txn = parse_transaction(
            &json,
            TxnType::Insert,
            TxnOpts::default(),
            &mut ns_registry,
            TEST_LEDGER,
        )
        .unwrap();

        // Should produce:
        //   andrew  name    "andrew"
        //   andrew  friend  _:b0
        //   _:b0    name    "ben"
        //   _:b0    friend  _:b1
        //   _:b1    name    "jake"
        assert_eq!(
            txn.insert_templates.len(),
            5,
            "Expected 5 triples, got {}",
            txn.insert_templates.len()
        );

        // Verify 2 distinct blank node subjects (ben and jake)
        let bnode_subjects: std::collections::HashSet<_> = txn
            .insert_templates
            .iter()
            .filter_map(|t| match &t.subject {
                TemplateTerm::BlankNode(label) => Some(label.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            bnode_subjects.len(),
            2,
            "Expected 2 distinct blank node subjects, got: {bnode_subjects:?}"
        );
    }

    // ---------------------------------------------------------------------
    // Graph scope (B1): a template's graph is its innermost enclosing scope.
    // ---------------------------------------------------------------------

    const G: &str = "http://example.org/g";
    const G2: &str = "http://example.org/g2";

    fn parse_doc(json: &Value, txn_type: TxnType) -> Result<Txn> {
        let mut ns = test_registry();
        parse_transaction(json, txn_type, TxnOpts::default(), &mut ns, TEST_LEDGER)
    }

    fn in_graph(iri: &str) -> TemplateGraph {
        TemplateGraph::Iri(iri.into())
    }

    /// Every insert template's graph, as `(subject kind, graph)` pairs are
    /// awkward to assert on; the graphs alone say where each statement lands.
    fn graphs(templates: &[TripleTemplate]) -> Vec<TemplateGraph> {
        templates.iter().map(|t| t.graph.clone()).collect()
    }

    #[test]
    fn scope_inherits_to_nested_and_anonymous_nodes() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{
                "@id": "ex:root",
                "@graph": "ex:g",
                "ex:named": {"@id": "ex:n", "ex:q": 1},
                "ex:anon": {"ex:r": 2, "ex:deeper": {"ex:s": 3}},
                "ex:items": {"@list": [{"ex:t": 4}, {"@id": "ex:li", "ex:u": 5}]}
            }]
        });
        for txn_type in [TxnType::Insert, TxnType::Upsert] {
            let txn = parse_doc(&doc, txn_type).unwrap();
            assert!(txn.insert_templates.len() >= 10, "{txn_type:?}");
            assert!(
                graphs(&txn.insert_templates)
                    .iter()
                    .all(|g| *g == in_graph(G)),
                "{txn_type:?}: every template must land in the selector's graph: {:?}",
                graphs(&txn.insert_templates)
            );
            assert_eq!(txn.write_graphs.iter().collect::<Vec<_>>(), vec![G]);
        }
    }

    #[test]
    fn nested_own_selector_wins_and_siblings_keep_parent_scope() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:root",
            "@graph": "ex:g",
            "ex:a": {"@id": "ex:x", "@graph": "ex:g2", "ex:p": 1, "ex:in": {"ex:q": 2}},
            "ex:b": {"@id": "ex:y", "ex:p": 3}
        });
        let txn = parse_doc(&doc, TxnType::Insert).unwrap();
        let mut ns = test_registry();
        let p = ns.sid_for_iri("http://example.org/p");
        let q = ns.sid_for_iri("http://example.org/q");
        let graph_of = |pred: &fluree_db_core::Sid, obj: i64| {
            txn.insert_templates
                .iter()
                .find(|t| {
                    matches!(&t.predicate, TemplateTerm::Sid(s) if s == pred)
                        && matches!(&t.object, TemplateTerm::Value(FlakeValue::Long(n)) if *n == obj)
                })
                .map(|t| t.graph.clone())
                .expect("template present")
        };
        // ex:x carries its own selector: it and everything under it are in g2.
        assert_eq!(graph_of(&p, 1), in_graph(G2));
        assert_eq!(graph_of(&q, 2), in_graph(G2));
        // Its sibling ex:y has none, so it inherits the parent's g.
        assert_eq!(graph_of(&p, 3), in_graph(G));
    }

    /// P7c: the update's `graph` key is the transaction default; a node
    /// selector overrides it for the node's whole subtree.
    #[test]
    fn update_node_selector_beats_txn_graph_for_subtree() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "graph": "ex:g",
            "insert": {
                "@id": "ex:s",
                "@graph": "ex:g2",
                "ex:child": {"@id": "ex:c", "ex:v": 1}
            }
        });
        let txn = parse_doc(&doc, TxnType::Update).unwrap();
        assert!(
            graphs(&txn.insert_templates)
                .iter()
                .all(|g| *g == in_graph(G2)),
            "{:?}",
            graphs(&txn.insert_templates)
        );
        // WITH semantics: the WHERE default graph is still the `graph` key.
        assert_eq!(
            txn.update_where_default_graph_iris,
            Some(vec![G.to_string()])
        );
    }

    /// P7e: a delete of a nested `@id` node under a selector retracts in the
    /// selector's graph, not the default graph.
    #[test]
    fn update_delete_nested_node_retracts_in_node_graph() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "delete": {
                "@id": "ex:s",
                "@graph": "ex:g",
                "ex:child": {"@id": "ex:c", "ex:v": 1}
            }
        });
        let txn = parse_doc(&doc, TxnType::Update).unwrap();
        assert_eq!(txn.delete_templates.len(), 2);
        assert!(graphs(&txn.delete_templates)
            .iter()
            .all(|g| *g == in_graph(G)));
    }

    /// Graphs in one document scope independently: a node with no selector
    /// after a scoped one lands back in the enclosing graph.
    #[test]
    fn sugar_and_update_key_scope_subtrees() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "graph": "ex:g",
            "insert": [
                ["graph", "ex:g2", {"@id": "ex:a", "ex:p": {"ex:q": 1}}],
                {"@id": "ex:b", "ex:p": {"ex:q": 2}}
            ]
        });
        let txn = parse_doc(&doc, TxnType::Update).unwrap();
        let (in_g2, in_g): (Vec<_>, Vec<_>) = txn
            .insert_templates
            .iter()
            .partition(|t| t.graph == in_graph(G2));
        assert_eq!(in_g2.len(), 2, "ex:a's link and its anonymous child");
        assert_eq!(in_g.len(), 2);
        assert!(in_g.iter().all(|t| t.graph == in_graph(G)));
    }

    /// D-B4: a variable graph in update templates denotes the WHERE's
    /// `GRAPH ?g` binding, never a graph literally named `?g`.
    #[test]
    fn variable_graph_in_update_templates_is_var() {
        for insert in [
            json!([["graph", "?g", {"@id": "?s", "ex:seen": true}]]),
            json!({"@id": "?s", "@graph": "?g", "ex:seen": true}),
        ] {
            let doc = json!({
                "@context": {"ex": "http://example.org/"},
                "where": [["graph", "?g", {"@id": "?s", "ex:p": "?o"}]],
                "insert": insert
            });
            let txn = parse_doc(&doc, TxnType::Update).unwrap();
            let g = txn.vars.get("?g").expect("?g registered with its ? prefix");
            assert!(txn
                .insert_templates
                .iter()
                .all(|t| t.graph == TemplateGraph::Var(g)));
            assert!(
                txn.write_graphs.is_empty(),
                "no graph named ?g: {:?}",
                txn.write_graphs
            );
        }
    }

    #[test]
    fn variable_graph_outside_update_templates_rejected() {
        let insert = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:s",
            "@graph": "?g",
            "ex:p": 1
        });
        let err = parse_doc(&insert, TxnType::Insert).unwrap_err().to_string();
        assert!(
            err.starts_with("Parse error: ") && err.contains("variable"),
            "{err}"
        );

        let key = json!({
            "@context": {"ex": "http://example.org/"},
            "graph": "?g",
            "insert": {"@id": "ex:s", "ex:p": 1}
        });
        let err = parse_doc(&key, TxnType::Update).unwrap_err().to_string();
        assert!(
            err.starts_with("Parse error: ") && err.contains("?g"),
            "{err}"
        );
    }

    #[test]
    fn default_keyword_is_default_graph_everywhere() {
        let docs = [
            (
                TxnType::Insert,
                json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:s", "@graph": "default", "ex:p": 1}),
            ),
            (
                TxnType::Update,
                json!({"@context": {"ex": "http://example.org/"}, "graph": "default", "insert": {"@id": "ex:s", "ex:p": 1}}),
            ),
            (
                TxnType::Update,
                json!({"@context": {"ex": "http://example.org/"}, "insert": [["graph", "default", {"@id": "ex:s", "ex:p": 1}]]}),
            ),
        ];
        for (txn_type, doc) in docs {
            let txn = parse_doc(&doc, txn_type).unwrap();
            assert!(
                txn.insert_templates
                    .iter()
                    .all(|t| t.graph == TemplateGraph::Default),
                "{doc}"
            );
            assert!(txn.write_graphs.is_empty(), "{doc}");
        }
    }

    /// L-X4: `config` names the ledger's config graph in every position.
    #[test]
    fn config_keyword_names_the_ledger_config_graph() {
        let config_iri = fluree_db_core::graph_registry::config_graph_iri(TEST_LEDGER);
        let docs = [
            (
                TxnType::Insert,
                json!({"@context": {"f": "https://ns.flur.ee/db#"}, "@id": "urn:cfg", "@graph": "config", "@type": "f:LedgerConfig"}),
            ),
            (
                TxnType::Update,
                json!({"@context": {"f": "https://ns.flur.ee/db#"}, "graph": "config", "insert": {"@id": "urn:cfg", "@type": "f:LedgerConfig"}}),
            ),
        ];
        for (txn_type, doc) in docs {
            let txn = parse_doc(&doc, txn_type).unwrap();
            assert!(txn
                .insert_templates
                .iter()
                .all(|t| t.graph == in_graph(&config_iri)));
            assert!(txn.write_graphs.contains(&config_iri));
        }
    }

    #[test]
    fn txn_meta_keyword_is_not_a_write_target() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:s",
            "@graph": "txn-meta",
            "ex:p": 1
        });
        let err = parse_doc(&doc, TxnType::Insert).unwrap_err().to_string();
        assert!(
            err.starts_with("Parse error: ")
                && err.contains("reserved system graph")
                && err.contains("#txn-meta"),
            "{err}"
        );
    }

    /// L-X4: a graph name that is not an absolute IRI is refused instead of
    /// minting a relative-IRI graph.
    #[test]
    fn relative_graph_name_refused() {
        let doc = json!({"@id": "http://example.org/s", "@graph": "g1", "http://example.org/p": 1});
        let err = parse_doc(&doc, TxnType::Insert).unwrap_err().to_string();
        assert!(
            err.starts_with("Parse error: ") && err.contains("\"g1\""),
            "{err}"
        );

        // With @base the same name is absolute.
        let based = json!({
            "@context": {"@base": "http://example.org/"},
            "@id": "s",
            "@graph": "g1",
            "http://example.org/p": 1
        });
        let txn = parse_doc(&based, TxnType::Insert).unwrap();
        assert!(txn.write_graphs.contains("http://example.org/g1"));
    }

    #[test]
    fn invalid_graph_value_refused() {
        let doc = json!({"@id": "http://example.org/s", "@graph": 3, "http://example.org/p": 1});
        let err = parse_doc(&doc, TxnType::Insert).unwrap_err().to_string();
        assert!(
            err.starts_with("Parse error: ") || err.starts_with("JSON-LD error: "),
            "{err}"
        );
    }

    /// Graph insert / sync: the request graph is the root scope. A selector
    /// naming it is accepted; any other graph is refused.
    #[test]
    fn graph_insert_root_scope_refuses_other_scopes() {
        let target = GraphSel::Graph(G.to_string());
        let mut ns = test_registry();
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:s",
            "ex:child": {"ex:v": 1}
        });
        let txn =
            parse_graph_insert(&doc, &target, TxnOpts::default(), &mut ns, TEST_LEDGER).unwrap();
        assert!(txn.insert_templates.iter().all(|t| t.graph == in_graph(G)));
        assert!(txn.write_graphs.contains(G));

        let same = json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:s", "@graph": "ex:g", "ex:p": 1});
        parse_graph_insert(&same, &target, TxnOpts::default(), &mut ns, TEST_LEDGER).unwrap();

        // Another named graph, or the default graph, is not the target.
        for selector in ["ex:g2", "default"] {
            let other = json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:s", "ex:c": {"@id": "ex:c", "@graph": selector, "ex:p": 1}});
            let err = parse_graph_insert(&other, &target, TxnOpts::default(), &mut ns, TEST_LEDGER)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("must not address a graph other than the target"),
                "{selector}: {err}"
            );
        }
    }

    // ---------------------------------------------------------------------
    // JSON-LD 1.1 named-graph objects: `{"@id": G, "@graph": [nodes]}`.
    // ---------------------------------------------------------------------

    const NG: &str = "http://example.org/NG";

    /// The graph of the template whose predicate ends with `local`.
    fn graph_of_pred(txn: &Txn, local: &str) -> TemplateGraph {
        let hits: Vec<&TripleTemplate> = txn
            .insert_templates
            .iter()
            .filter(|t| matches!(&t.predicate, TemplateTerm::Sid(s) if &*s.name == local))
            .collect();
        assert_eq!(hits.len(), 1, "one ex:{local} template: {hits:?}");
        hits[0].graph.clone()
    }

    /// P6a: the owner's own properties stay in the enclosing graph; the
    /// content goes to the graph the owner names.
    #[test]
    fn named_graph_object_single() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:NG",
            "ex:label": "w",
            "@graph": [{"@id": "ex:a", "ex:p": 1}, {"ex:q": 2}]
        });
        let txn = parse_doc(&doc, TxnType::Insert).unwrap();
        assert_eq!(graph_of_pred(&txn, "label"), TemplateGraph::Default);
        assert_eq!(graph_of_pred(&txn, "p"), in_graph(NG));
        assert_eq!(graph_of_pred(&txn, "q"), in_graph(NG));
        assert!(txn.txn_meta.is_empty(), "a named graph's keys are data");
        assert_eq!(txn.write_graphs.iter().collect::<Vec<_>>(), vec![NG]);
    }

    /// P6b: a named graph inside an envelope. Its content used to be read as
    /// the owner's graph *selector* (the first content node's `@id`) and
    /// dropped.
    #[test]
    fn named_graph_object_in_envelope() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:plain", "ex:r": 0},
                {"@id": "ex:NG", "@graph": [{"@id": "ex:a", "ex:p": 1}]}
            ]
        });
        let txn = parse_doc(&doc, TxnType::Insert).unwrap();
        assert_eq!(graph_of_pred(&txn, "r"), TemplateGraph::Default);
        assert_eq!(graph_of_pred(&txn, "p"), in_graph(NG));
    }

    /// Named graphs nest: each node's content goes to the innermost graph
    /// that names it, and the dataset stays flat.
    #[test]
    fn named_graph_object_nested() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{
                "@id": "ex:NG",
                "@graph": [
                    {"@id": "ex:NG2", "ex:label": "inner", "@graph": [{"@id": "ex:b", "ex:q": 2}]},
                    {"@id": "ex:a", "ex:p": 1}
                ]
            }]
        });
        let txn = parse_doc(&doc, TxnType::Insert).unwrap();
        assert_eq!(graph_of_pred(&txn, "p"), in_graph(NG));
        assert_eq!(
            graph_of_pred(&txn, "label"),
            in_graph(NG),
            "NG2's own data is in NG"
        );
        assert_eq!(graph_of_pred(&txn, "q"), in_graph("http://example.org/NG2"));
    }

    /// A named graph as a property value: the link is in the current scope,
    /// the content in the named graph.
    #[test]
    fn named_graph_object_as_property_value() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:s",
            "ex:source": {"@id": "ex:NG", "@graph": [{"@id": "ex:a", "ex:p": 1}]}
        });
        let txn = parse_doc(&doc, TxnType::Insert).unwrap();
        assert_eq!(graph_of_pred(&txn, "source"), TemplateGraph::Default);
        assert_eq!(graph_of_pred(&txn, "p"), in_graph(NG));
    }

    /// The object form with properties is content too: `ex:g9`'s statement
    /// is in the graph `ex:o` names, and `ex:o`'s own statement is in the
    /// enclosing graph. It used to write `ex:o ex:q 2` into `ex:g9` and drop
    /// `ex:g9 ex:p 1`.
    #[test]
    fn object_form_graph_with_properties_is_content() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{"@id": "ex:o", "@graph": {"@id": "ex:g9", "ex:p": 1}, "ex:q": 2}]
        });
        let txn = parse_doc(&doc, TxnType::Insert).unwrap();
        assert_eq!(graph_of_pred(&txn, "q"), TemplateGraph::Default);
        assert_eq!(graph_of_pred(&txn, "p"), in_graph("http://example.org/o"));
    }

    #[test]
    fn named_graph_object_blank_or_anonymous_name_rejected() {
        for doc in [
            // blank-node graph name
            json!({"@id": "_:g", "@graph": [{"@id": "http://example.org/a", "http://example.org/p": 1}]}),
            // a graph object without `@id` below the top level
            json!({"@id": "http://example.org/s", "http://example.org/p": {"@graph": [{"@id": "http://example.org/a", "http://example.org/q": 1}]}}),
        ] {
            let err = parse_doc(&doc, TxnType::Insert).unwrap_err().to_string();
            assert!(err.starts_with("Parse error: "), "{doc}: {err}");
        }
        // A variable cannot name one either, even in update templates.
        let update = json!({
            "where": [{"@id": "?s", "http://example.org/p": "?o"}],
            "insert": {"@id": "?g", "@graph": [{"@id": "?s", "http://example.org/q": 1}]}
        });
        let err = parse_doc(&update, TxnType::Update).unwrap_err().to_string();
        assert!(err.starts_with("Parse error: "), "{err}");
    }

    /// Graph insert / sync: the request names the graph; a named-graph object
    /// in the payload is refused, even one naming the request's own graph.
    #[test]
    fn graph_insert_refuses_named_graph_objects() {
        let target = GraphSel::Graph(NG.to_string());
        let mut ns = test_registry();
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:NG",
            "@graph": [{"@id": "ex:a", "ex:p": 1}]
        });
        let err = parse_graph_insert(&doc, &target, TxnOpts::default(), &mut ns, TEST_LEDGER)
            .unwrap_err()
            .to_string();
        assert!(err.contains("must not address named graphs"), "{err}");

        // `{"@id": G, "@graph": []}` is an empty named graph, not the
        // explicitly empty envelope a sync may clear a graph with.
        let empty_named = json!({"@id": NG, "@graph": []});
        assert!(parse_graph_insert(
            &empty_named,
            &target,
            TxnOpts::default(),
            &mut ns,
            TEST_LEDGER
        )
        .is_err());
        let empty_envelope = json!({"@graph": []});
        let txn = parse_graph_insert(
            &empty_envelope,
            &target,
            TxnOpts::default(),
            &mut ns,
            TEST_LEDGER,
        )
        .unwrap();
        assert!(txn.insert_templates.is_empty());
    }

    // ---------------------------------------------------------------------
    // Document-scoped blank-node labels (D-B5).
    // ---------------------------------------------------------------------

    fn blank_labels(templates: &[TripleTemplate]) -> std::collections::BTreeSet<String> {
        templates
            .iter()
            .flat_map(|t| [&t.subject, &t.object])
            .filter_map(|term| match term {
                TemplateTerm::BlankNode(label) => Some(label.clone()),
                _ => None,
            })
            .collect()
    }

    /// N4: anonymous nodes in different `["graph", …]` items are different
    /// nodes. The label counter used to restart for every item, so all of
    /// them became `_:b0` and merged.
    #[test]
    fn blank_issuer_is_document_scoped_across_sugar_items() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "insert": [
                ["graph", "ex:g", {"@id": "ex:a", "ex:p": {"ex:v": 1}}],
                ["graph", "ex:g", {"@id": "ex:b", "ex:p": {"ex:v": 2}}],
                {"@id": "ex:c", "ex:p": {"ex:v": 3}}
            ]
        });
        let txn = parse_doc(&doc, TxnType::Update).unwrap();
        assert_eq!(
            blank_labels(&txn.insert_templates).len(),
            3,
            "three anonymous nodes, three labels: {:?}",
            blank_labels(&txn.insert_templates)
        );
    }

    /// A user label of the issuer's own form stays that one node, and an
    /// anonymous node never shares it, whichever comes first.
    #[test]
    fn user_b_label_never_collides_with_anonymous() {
        let user_first = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "_:b0", "ex:p": 1},
                {"@id": "ex:s", "ex:q": {"ex:v": 2}}
            ]
        });
        let anon_first = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:s", "ex:q": {"ex:v": 2}},
                {"@id": "_:b0", "ex:p": 1},
                {"@id": "_:b1", "ex:p": 3}
            ]
        });
        for doc in [user_first, anon_first] {
            let txn = parse_doc(&doc, TxnType::Insert).unwrap();
            let labels = blank_labels(&txn.insert_templates);
            let users = doc["@graph"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|n| n["@id"].as_str().is_some_and(|id| id.starts_with("_:")))
                .count();
            assert_eq!(labels.len(), users + 1, "{doc}: {labels:?}");
            assert!(
                labels.contains("_:b0"),
                "the user's label is kept: {labels:?}"
            );
        }
        // Without a collision nothing is re-labelled.
        let plain = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{"@id": "_:mine", "ex:p": 1}, {"@id": "ex:s", "ex:q": {"ex:v": 2}}]
        });
        let labels = blank_labels(&parse_doc(&plain, TxnType::Insert).unwrap().insert_templates);
        assert!(
            labels.contains("_:b0") && labels.contains("_:mine"),
            "{labels:?}"
        );
    }

    // ---------------------------------------------------------------------
    // Keywords on node objects (D-B3, L-B13, L-B16).
    // ---------------------------------------------------------------------

    fn pred_local(t: &TripleTemplate) -> String {
        match &t.predicate {
            TemplateTerm::Sid(s) => s.name.to_string(),
            other => format!("{other:?}"),
        }
    }

    fn sid_local(term: &TemplateTerm) -> String {
        match term {
            TemplateTerm::Sid(s) => s.name.to_string(),
            other => format!("{other:?}"),
        }
    }

    /// `@reverse` (keyword and a context term defined with `@reverse`) emits
    /// the inverse statement. The keyword used to become a predicate named
    /// `@reverse`, and the term form was written forward.
    #[test]
    fn reverse_properties_emit_the_inverse_statement() {
        let doc = json!({
            "@context": {
                "ex": "http://example.org/",
                "parent": {"@reverse": "ex:child", "@type": "@id"}
            },
            "@id": "ex:kid",
            "parent": "ex:mom",
            "@reverse": {"ex:child": {"@id": "ex:dad", "ex:name": "Dad"}}
        });
        let txn = parse_doc(&doc, TxnType::Insert).unwrap();
        let mut child: Vec<(String, String)> = txn
            .insert_templates
            .iter()
            .filter(|t| pred_local(t) == "child")
            .map(|t| (sid_local(&t.subject), sid_local(&t.object)))
            .collect();
        child.sort();
        assert_eq!(
            child,
            [
                ("dad".to_string(), "kid".to_string()),
                ("mom".to_string(), "kid".to_string())
            ]
        );
        assert!(
            txn.insert_templates.iter().any(|t| pred_local(t) == "name"),
            "the reverse value node's own statements are written"
        );
    }

    /// `@included` nodes are written in the owner's scope with no linking
    /// statement; `@index` carries none.
    #[test]
    fn included_nodes_share_the_owner_scope_and_index_is_dropped() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{
                "@id": "ex:a",
                "@graph": "ex:g",
                "@index": "ignored",
                "ex:p": 1,
                "@included": [{"@id": "ex:b", "ex:q": {"ex:r": 2}}]
            }]
        });
        let txn = parse_doc(&doc, TxnType::Insert).unwrap();
        let preds: Vec<String> = txn.insert_templates.iter().map(pred_local).collect();
        assert_eq!(txn.insert_templates.len(), 3, "{preds:?}");
        assert!(txn.insert_templates.iter().all(|t| t.graph == in_graph(G)));
        assert!(!preds.iter().any(|p| p.starts_with('@')), "{preds:?}");
    }

    /// L-B16: `@nest` merges into the enclosing node through the whole
    /// transaction path (expansion, then templates).
    #[test]
    fn nest_properties_belong_to_the_enclosing_node() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:s",
            "@nest": {"ex:a": 1, "@nest": {"ex:b": 2}}
        });
        let txn = parse_doc(&doc, TxnType::Insert).unwrap();
        assert_eq!(txn.insert_templates.len(), 2);
        assert!(txn
            .insert_templates
            .iter()
            .all(|t| sid_local(&t.subject) == "s"));
    }

    /// L-B13: keys that merely start with `@` are ordinary property names,
    /// as before; keyword-form keys that are not supported on a node object
    /// are refused rather than written as a predicate named after the key.
    #[test]
    fn keyword_form_keys_refused_other_at_keys_unchanged() {
        let odata = json!({
            "@id": "http://example.org/s",
            "@odata.etag": "W/\"1\"",
            "http://example.org/p": 1
        });
        let txn = parse_doc(&odata, TxnType::Insert).unwrap();
        // Exactly the predicate the key always produced: the key, as an IRI.
        let odata_pred = test_registry().sid_for_iri("@odata.etag");
        assert_eq!(txn.insert_templates.len(), 2);
        assert!(
            txn.insert_templates
                .iter()
                .any(|t| matches!(&t.predicate, TemplateTerm::Sid(s) if *s == odata_pred)),
            "{:?}",
            txn.insert_templates
        );

        for key in ["@foo", "@language", "@direction", "@value"] {
            let mut doc = serde_json::Map::new();
            doc.insert("@id".into(), json!("http://example.org/s"));
            doc.insert("http://example.org/p".into(), json!(1));
            doc.insert(key.into(), json!("x"));
            let err = parse_doc(&Value::Object(doc), TxnType::Insert);
            match err {
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        msg.starts_with("Parse error: ") || msg.starts_with("JSON-LD error: "),
                        "{key}: {msg}"
                    );
                }
                Ok(txn) => panic!("{key} must be refused, got {:?}", txn.insert_templates),
            }
        }
    }

    /// A transaction body's `opts.shapes` and `opts.uniqueProperties` reach
    /// the transaction on every surface that parses it (the HTTP server used
    /// to be the only one reading them), for inserts and updates alike; a
    /// caller's own options win, and a malformed value is refused.
    #[test]
    fn body_constraints_are_read_from_opts() {
        let shapes = json!({"@id": "http://example.org/Shape", "@type": "http://www.w3.org/ns/shacl#NodeShape"});
        let body = |extra: Value| {
            let mut doc = json!({
                "opts": {"shapes": shapes, "uniqueProperties": ["http://example.org/email"]},
                "@id": "http://example.org/a",
                "http://example.org/email": "a@x"
            });
            if let Value::Object(map) = extra {
                doc.as_object_mut().unwrap().extend(map);
            }
            doc
        };
        let txn = parse_doc(&body(json!({})), TxnType::Insert).unwrap();
        assert_eq!(txn.opts.shapes, Some(shapes.clone()));
        assert_eq!(
            txn.opts.unique_properties,
            Some(vec!["http://example.org/email".to_string()])
        );

        let update = json!({
            "opts": {"shapes": [shapes]},
            "insert": {"@id": "http://example.org/a", "http://example.org/p": 1}
        });
        let txn = parse_doc(&update, TxnType::Update).unwrap();
        assert_eq!(txn.opts.shapes, Some(json!([shapes])));

        // The caller's own options win over the body's.
        let mut ns = test_registry();
        let mine = json!({"@id": "http://example.org/Mine"});
        let txn = parse_transaction(
            &body(json!({})),
            TxnType::Insert,
            TxnOpts {
                shapes: Some(mine.clone()),
                ..TxnOpts::default()
            },
            &mut ns,
            TEST_LEDGER,
        )
        .unwrap();
        assert_eq!(txn.opts.shapes, Some(mine));

        for (opts, what) in [
            (json!({"shapes": "not a document"}), "shapes"),
            (json!({"shapes": [1]}), "shapes"),
            (
                json!({"uniqueProperties": "http://example.org/email"}),
                "uniqueProperties",
            ),
            (json!({"uniqueProperties": [1]}), "uniqueProperties"),
        ] {
            let doc =
                json!({"opts": opts, "@id": "http://example.org/a", "http://example.org/p": 1});
            let err = parse_doc(&doc, TxnType::Insert).unwrap_err().to_string();
            assert!(
                err.starts_with("Parse error: ") && err.contains(what),
                "{opts}: {err}"
            );
        }
    }

    /// L-B14/L-B16: every refusal this change introduces renders with a
    /// "Parse error: " or "JSON-LD error: " prefix, the classes clients map
    /// to a 400.
    #[test]
    fn new_refusals_render_as_parse_or_json_ld_errors() {
        let ctx = json!({"ex": "http://example.org/"});
        let cases: Vec<(&str, TxnType, Value)> = vec![
            (
                "invalid @graph value",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "ex:s", "@graph": true, "ex:p": 1}),
            ),
            (
                "variable graph outside update templates",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "ex:s", "@graph": "?g", "ex:p": 1}),
            ),
            (
                "relative graph name",
                TxnType::Insert,
                json!({"@id": "http://example.org/s", "@graph": "g1", "http://example.org/p": 1}),
            ),
            (
                "txn-meta keyword",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "ex:s", "@graph": "txn-meta", "ex:p": 1}),
            ),
            (
                "blank named-graph name",
                TxnType::Insert,
                json!({"@id": "_:g", "@graph": [{"@id": "http://example.org/a", "http://example.org/p": 1}]}),
            ),
            (
                "graph object without @id below the top level",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "ex:s", "ex:p": {"@graph": [{"@id": "ex:a", "ex:q": 1}]}}),
            ),
            (
                "unknown keyword",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "ex:s", "@foo": 1, "ex:p": 1}),
            ),
            (
                "node-level @language",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "ex:s", "@language": "en", "ex:p": 1}),
            ),
            (
                "@nest with @id",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "ex:s", "@nest": {"@id": "ex:o", "ex:p": 1}}),
            ),
            (
                "@reverse that is not a map",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "ex:s", "@reverse": "ex:o"}),
            ),
            (
                "annotation inside @reverse",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "ex:s", "@reverse": {"ex:p": {"@id": "ex:o", "@annotation": {"ex:r": 1}}}}),
            ),
            (
                "reserved annotation label",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "_:fluree_ann_0", "ex:p": 1}),
            ),
            (
                "variable update graph key",
                TxnType::Update,
                json!({"@context": ctx, "graph": "?g", "insert": {"@id": "ex:s", "ex:p": 1}}),
            ),
            (
                "graph-item name that is not a graph IRI",
                TxnType::Update,
                json!({"@context": ctx, "insert": [["graph", 7, {"@id": "ex:s", "ex:p": 1}]]}),
            ),
        ];
        for (what, txn_type, doc) in cases {
            let msg = parse_doc(&doc, txn_type)
                .map(|t| format!("accepted: {:?}", t.insert_templates))
                .unwrap_or_else(|e| e.to_string());
            assert!(
                msg.starts_with("Parse error: ") || msg.starts_with("JSON-LD error: "),
                "{what}: {msg}"
            );
        }
        let mut ns = test_registry();
        let msg = parse_graph_insert(
            &json!({"@context": ctx, "@id": "ex:s", "@graph": "ex:other", "ex:p": 1}),
            &GraphSel::Graph(G.to_string()),
            TxnOpts::default(),
            &mut ns,
            TEST_LEDGER,
        )
        .unwrap_err()
        .to_string();
        assert!(
            msg.starts_with("Parse error: "),
            "graph insert addressing a graph: {msg}"
        );
    }

    // ---------------------------------------------------------------------
    // Edge annotations follow the scope (B1c); one place writes the anchor.
    // ---------------------------------------------------------------------

    fn is_pred(t: &TripleTemplate, local: &str) -> bool {
        matches!(&t.predicate, TemplateTerm::Sid(s) if &*s.name == local)
    }

    /// The graphs of the edge `ex:worksFor`, of every `f:reifies*` statement,
    /// and the `f:reifiesGraph` objects, from one parsed document.
    fn annotation_graphs(txn: &Txn) -> (Vec<TemplateGraph>, Vec<TemplateGraph>, Vec<String>) {
        let edge: Vec<TemplateGraph> = txn
            .insert_templates
            .iter()
            .filter(|t| is_pred(t, "worksFor"))
            .map(|t| t.graph.clone())
            .collect();
        let bundle: Vec<TemplateGraph> = txn
            .insert_templates
            .iter()
            .filter(
                |t| matches!(&t.predicate, TemplateTerm::Sid(s) if s.name.starts_with("reifies")),
            )
            .map(|t| t.graph.clone())
            .collect();
        let anchors: Vec<String> = txn
            .insert_templates
            .iter()
            .filter(|t| is_pred(t, "reifiesGraph"))
            .map(|t| sid_local(&t.object))
            .collect();
        (edge, bundle, anchors)
    }

    fn annotated_edge() -> Value {
        json!({"@id": "ex:acme", "@annotation": {"ex:role": "Engineer"}})
    }

    /// A1, A2, A3, a node selector, and named-graph content: the edge, its
    /// bundle, and the bundle's `f:reifiesGraph` all name the same graph. A1
    /// used to be refused (the bundle carried no anchor); A2 and A3 wrote the
    /// bundle to the default graph without one.
    #[test]
    fn annotation_bundle_lands_in_and_names_the_edge_graph() {
        let ctx = json!({"ex": "http://example.org/"});
        let cases: Vec<(&str, TxnType, Value)> = vec![
            (
                "node selector",
                TxnType::Insert,
                json!({"@context": ctx, "@graph": [{"@id": "ex:alice", "@graph": "ex:g", "ex:worksFor": annotated_edge()}]}),
            ),
            (
                "A1: update graph key",
                TxnType::Update,
                json!({"@context": ctx, "graph": "ex:g", "insert": {"@id": "ex:alice", "ex:worksFor": annotated_edge()}}),
            ),
            (
                "A2: graph item",
                TxnType::Update,
                json!({"@context": ctx, "insert": [["graph", "ex:g", {"@id": "ex:alice", "ex:worksFor": annotated_edge()}]]}),
            ),
            (
                "A3: array selector",
                TxnType::Insert,
                json!({"@context": ctx, "@graph": [{"@id": "ex:alice", "@graph": ["ex:g"], "ex:worksFor": annotated_edge()}]}),
            ),
            (
                "named-graph content",
                TxnType::Insert,
                json!({"@context": ctx, "@id": "ex:g", "@graph": [{"@id": "ex:alice", "ex:worksFor": annotated_edge()}]}),
            ),
            (
                "P8a: nested node under a selector",
                TxnType::Insert,
                json!({"@context": ctx, "@graph": [{
                    "@id": "ex:carol", "@graph": "ex:g",
                    "ex:knows": {"@id": "ex:alice", "ex:worksFor": annotated_edge()}
                }]}),
            ),
        ];
        for (what, txn_type, doc) in cases {
            let txn = parse_doc(&doc, txn_type).unwrap_or_else(|e| panic!("{what}: {e}"));
            let (edge, bundle, anchors) = annotation_graphs(&txn);
            assert_eq!(edge, [in_graph(G)], "{what}: the edge");
            assert!(
                !bundle.is_empty() && bundle.iter().all(|g| *g == in_graph(G)),
                "{what}: the bundle {bundle:?}"
            );
            assert_eq!(
                anchors,
                ["g"],
                "{what}: exactly one anchor, naming the graph"
            );
        }
    }

    /// At an insert document's root a bare `graph` key is the transaction's
    /// routing key, which the parser strips, not a graph selector: the
    /// annotation lowering must not scope the edge's bundle by it either, or
    /// the bundle would land in a graph its edge is not in.
    #[test]
    fn root_graph_routing_key_does_not_scope_annotations() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "graph": G,
            "@id": "ex:alice",
            "ex:worksFor": annotated_edge()
        });
        let txn = parse_doc(&doc, TxnType::Insert).expect("parse");
        let (edge, bundle, anchors) = annotation_graphs(&txn);
        assert_eq!(edge, [TemplateGraph::Default], "the edge");
        assert!(
            !bundle.is_empty() && bundle.iter().all(|g| *g == TemplateGraph::Default),
            "the bundle {bundle:?}"
        );
        assert!(anchors.is_empty(), "no anchor: {anchors:?}");
    }

    /// A default-graph edge's bundle has no anchor (absence means default).
    #[test]
    fn default_graph_annotation_has_no_anchor() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:alice",
            "ex:worksFor": annotated_edge()
        });
        let (edge, bundle, anchors) = annotation_graphs(&parse_doc(&doc, TxnType::Insert).unwrap());
        assert_eq!(edge, [TemplateGraph::Default]);
        assert!(bundle.iter().all(|g| *g == TemplateGraph::Default));
        assert!(anchors.is_empty());
    }

    /// W8: an annotation on a named graph's own property stays with the
    /// property in the enclosing graph; the sibling is not appended into the
    /// named graph's content.
    #[test]
    fn annotation_on_a_named_graph_node_property_stays_in_the_enclosing_graph() {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:NG",
            "ex:worksFor": annotated_edge(),
            "@graph": [{"@id": "ex:a", "ex:p": 1}]
        });
        let (edge, bundle, anchors) = annotation_graphs(&parse_doc(&doc, TxnType::Insert).unwrap());
        assert_eq!(edge, [TemplateGraph::Default]);
        assert!(
            bundle.iter().all(|g| *g == TemplateGraph::Default),
            "{bundle:?}"
        );
        assert!(anchors.is_empty());
    }

    /// Graph insert: the bundle lands in the request graph and names it.
    #[test]
    fn graph_insert_anchors_bundles_to_the_request_graph() {
        let mut ns = test_registry();
        let doc = json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "ex:worksFor": annotated_edge()});
        let txn = parse_graph_insert(
            &doc,
            &GraphSel::Graph(G.to_string()),
            TxnOpts::default(),
            &mut ns,
            TEST_LEDGER,
        )
        .unwrap();
        let (edge, bundle, anchors) = annotation_graphs(&txn);
        assert_eq!(edge, [in_graph(G)]);
        assert!(bundle.iter().all(|g| *g == in_graph(G)));
        assert_eq!(anchors, ["g"]);
    }

    /// An edge annotation under a variable graph is refused while parsing,
    /// with a message that says so; flake generation used to fail on the
    /// reifier's `f:reifiesGraph ?g` anchor instead.
    #[test]
    fn annotations_under_a_variable_graph_are_refused() {
        let annotated = json!({"@id": "ex:o", "@annotation": {"ex:note": "x"}});
        for insert in [
            json!([["graph", "?g", {"@id": "?s", "ex:q": annotated}]]),
            json!({"@id": "?s", "@graph": "?g", "ex:q": annotated}),
        ] {
            let doc = json!({
                "@context": {"ex": "http://example.org/"},
                "where": [["graph", "?g", {"@id": "?s", "ex:p": "?o"}]],
                "insert": insert
            });
            let err = parse_doc(&doc, TxnType::Update).unwrap_err().to_string();
            assert!(
                err.starts_with("Parse error: ") && err.contains("variable graph"),
                "{doc}: {err}"
            );
        }
    }

    /// The fail-closed cross-check: a reifier bundle whose edge is not
    /// asserted in the bundle's graph is refused.
    #[test]
    fn reifier_without_its_edge_in_the_same_graph_is_refused() {
        let mut ns = test_registry();
        let s = TemplateTerm::Sid(ns.sid_for_iri("http://example.org/alice"));
        let p = TemplateTerm::Sid(ns.sid_for_iri("http://example.org/worksFor"));
        let o = TemplateTerm::Sid(ns.sid_for_iri("http://example.org/acme"));
        let ann = TemplateTerm::BlankNode("_:fluree_ann_0".to_string());
        let reifies = |local: &str| {
            TemplateTerm::Sid(fluree_db_core::Sid::new(
                fluree_vocab::namespaces::FLUREE_DB,
                local,
            ))
        };
        let edge = TripleTemplate::new(s.clone(), p.clone(), o.clone());
        let bundle = |graph: TemplateGraph| {
            [
                (reifies(fluree_vocab::db::REIFIES_SUBJECT), s.clone()),
                (reifies(fluree_vocab::db::REIFIES_PREDICATE), p.clone()),
            ]
            .into_iter()
            .map(|(pred, obj)| {
                let mut t = TripleTemplate::new(ann.clone(), pred, obj);
                t.graph = graph.clone();
                t
            })
            .collect::<Vec<_>>()
        };
        let mut same = vec![edge.clone()];
        same.extend(bundle(TemplateGraph::Default));
        check_reifiers_match_edges(&same).unwrap();

        let mut edge_in_g = edge.clone();
        edge_in_g.graph = in_graph(G);
        let mut same_named = vec![edge_in_g];
        same_named.extend(bundle(in_graph(G)));
        check_reifiers_match_edges(&same_named).unwrap();

        let mut split = vec![edge];
        split.extend(bundle(in_graph(G)));
        let err = check_reifiers_match_edges(&split).unwrap_err().to_string();
        assert!(err.starts_with("Parse error: ") && err.contains(G), "{err}");
    }
}
