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
use crate::ir::{InlineValues, TemplateTerm, TripleTemplate, Txn, TxnOpts, TxnType};
use crate::namespace::NamespaceRegistry;
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::FlakeValue;
use fluree_db_query::parse::{
    parse_where_with_counters, JsonLdParseCtx, JsonLdParsePolicy, PathAliasMap, UnresolvedQuery,
};
use fluree_db_query::VarRegistry;
use fluree_graph_json_ld::{expand_with_context_policy, parse_context, ParsedContext};
use fluree_vocab::{
    rdf::{self, TYPE},
    rdf_names,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Assigns per-transaction graph IDs for JSON-LD `@graph` selectors.
///
/// These IDs are scoped to the transaction envelope via `Txn.graph_delta`.
/// They do not need to be globally stable across commits, as long as the commit
/// carries the mapping used to encode flakes.
struct GraphIdAssigner {
    iri_to_id: HashMap<String, u16>,
    next_id: u16, // 2+ reserved for user graphs
}

impl GraphIdAssigner {
    fn new() -> Self {
        Self {
            iri_to_id: HashMap::new(),
            next_id: 2,
        }
    }

    fn get_or_assign(&mut self, iri: &str) -> u16 {
        if let Some(&id) = self.iri_to_id.get(iri) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.iri_to_id.insert(iri.to_string(), id);
        id
    }

    fn delta(&self) -> rustc_hash::FxHashMap<u16, String> {
        self.iri_to_id
            .iter()
            .map(|(iri, &g_id)| (g_id, iri.clone()))
            .collect()
    }
}

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
pub fn parse_transaction(
    json: &Value,
    txn_type: TxnType,
    opts: TxnOpts,
    ns_registry: &mut NamespaceRegistry,
) -> Result<Txn> {
    match txn_type {
        TxnType::Insert => parse_insert(json, opts, ns_registry),
        TxnType::Upsert => parse_upsert(json, opts, ns_registry),
        TxnType::Update => parse_update(json, opts, ns_registry),
    }
}

/// Parse an insert transaction
fn parse_insert(json: &Value, opts: TxnOpts, ns_registry: &mut NamespaceRegistry) -> Result<Txn> {
    let mut vars = VarRegistry::new();
    let mut graph_ids = GraphIdAssigner::new();

    // Parse and merge context
    let context = extract_context(json)?;

    // Resolve strict compact-IRI policy
    let strict = opts
        .strict_compact_iri
        .or_else(|| {
            use fluree_db_query::parse::policy::parse_strict_compact_iri_opt;
            json.as_object().and_then(parse_strict_compact_iri_opt)
        })
        .unwrap_or(true);

    // Extract transaction metadata (only from envelope-form documents with @graph)
    let txn_meta = extract_txn_meta(json, &context, ns_registry, strict)?;

    // Strip top-level `opts` so it is not expanded as data (single-object form)
    let json_for_expand = strip_opts_for_expansion(json);
    // Expand the document
    let expanded = expand_with_context_policy(&json_for_expand, &context, strict)?;

    let empty_aliases = HashMap::new();
    let mut ctx = TemplateParseCtx::new(
        &context,
        &mut vars,
        ns_registry,
        false,
        strict,
        &mut graph_ids,
        None,
        &empty_aliases,
    );
    let templates = parse_expanded_triples_with_ctx(&expanded, &mut ctx)?;
    if templates.is_empty() {
        return Err(TransactError::Parse(
            "Insert must contain at least one predicate or @type (an object with only @id is not a valid insert)"
                .to_string(),
        ));
    }

    let mut txn = Txn::insert()
        .with_inserts(templates)
        .with_vars(vars)
        .with_opts(opts)
        .with_txn_meta(txn_meta);
    txn.graph_delta = graph_ids.delta();
    Ok(txn)
}

/// Parse an upsert transaction
///
/// Upsert is similar to insert, but generates WHERE and DELETE clauses
/// to remove existing values for provided predicates.
fn parse_upsert(json: &Value, opts: TxnOpts, ns_registry: &mut NamespaceRegistry) -> Result<Txn> {
    // For now, upsert is handled the same as insert at parse time
    // The actual upsert logic (query existing, delete old) happens in stage
    let mut vars = VarRegistry::new();
    let mut graph_ids = GraphIdAssigner::new();

    let context = extract_context(json)?;

    // Resolve strict compact-IRI policy
    let strict = opts
        .strict_compact_iri
        .or_else(|| {
            use fluree_db_query::parse::policy::parse_strict_compact_iri_opt;
            json.as_object().and_then(parse_strict_compact_iri_opt)
        })
        .unwrap_or(true);

    // Extract transaction metadata (only from envelope-form documents with @graph)
    let txn_meta = extract_txn_meta(json, &context, ns_registry, strict)?;

    // Strip top-level `opts` so it is not expanded as data (single-object form)
    let json_for_expand = strip_opts_for_expansion(json);
    let expanded = expand_with_context_policy(&json_for_expand, &context, strict)?;

    let empty_aliases = HashMap::new();
    let mut ctx = TemplateParseCtx::new(
        &context,
        &mut vars,
        ns_registry,
        false,
        strict,
        &mut graph_ids,
        None,
        &empty_aliases,
    );
    let templates = parse_expanded_triples_with_ctx(&expanded, &mut ctx)?;
    if templates.is_empty() {
        return Err(TransactError::Parse(
            "Upsert must contain at least one predicate or @type (an object with only @id is not a valid upsert)"
                .to_string(),
        ));
    }

    let mut txn = Txn::upsert()
        .with_inserts(templates)
        .with_vars(vars)
        .with_opts(opts)
        .with_txn_meta(txn_meta);
    txn.graph_delta = graph_ids.delta();
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
fn parse_update(json: &Value, opts: TxnOpts, ns_registry: &mut NamespaceRegistry) -> Result<Txn> {
    let obj = json
        .as_object()
        .ok_or_else(|| TransactError::Parse("Update transaction must be an object".to_string()))?;

    let mut vars = VarRegistry::new();
    let mut graph_ids = GraphIdAssigner::new();

    // Parse context from the outer document
    let context = extract_context(json)?;

    // Resolve strict compact-IRI policy
    let strict = opts
        .strict_compact_iri
        .or_else(|| {
            use fluree_db_query::parse::policy::parse_strict_compact_iri_opt;
            parse_strict_compact_iri_opt(obj)
        })
        .unwrap_or(true);

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

    // Optional transaction-level default graph.
    // This applies to:
    // - WHERE patterns (scopes default-graph patterns to the named graph)
    // - DELETE/INSERT templates that do not specify per-node @graph
    let template_default_graph = parse_update_template_default_graph(
        obj.get("graph"),
        &context,
        &from_named_aliases,
        &mut graph_ids,
        strict,
    )?;

    let where_default_graph_iris = parse_update_where_default_graph_iris(
        obj.get("from"),
        &context,
        &from_named_aliases,
        strict,
    )?
    .unwrap_or_else(|| {
        template_default_graph
            .as_ref()
            .map(|(_, iri)| vec![iri.clone()])
            .unwrap_or_default()
    });

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
        let ctx = JsonLdParseCtx::new(context.clone(), PathAliasMap::new(), parse_policy);
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

    // Parse DELETE clause
    let delete_templates = if let Some(delete_val) = obj.get("delete") {
        validate_type_fields(delete_val)?;
        let mut ctx = TemplateParseCtx::new(
            &context,
            &mut vars,
            ns_registry,
            object_var_parsing,
            strict,
            &mut graph_ids,
            template_default_graph.as_ref().map(|(g_id, _)| *g_id),
            &from_named_aliases,
        );
        let templates = parse_update_templates_with_ctx(delete_val, &mut ctx)?;
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
    let insert_templates = if let Some(insert_val) = obj.get("insert") {
        validate_type_fields(insert_val)?;
        let mut ctx = TemplateParseCtx::new(
            &context,
            &mut vars,
            ns_registry,
            object_var_parsing,
            strict,
            &mut graph_ids,
            template_default_graph.as_ref().map(|(g_id, _)| *g_id),
            &from_named_aliases,
        );
        let templates = parse_update_templates_with_ctx(insert_val, &mut ctx)?;
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

    let mut txn = Txn::update()
        .with_wheres(where_patterns)
        .with_deletes(delete_templates)
        .with_inserts(insert_templates)
        .with_vars(vars)
        .with_opts(opts)
        .with_txn_meta(txn_meta);
    txn.graph_delta = graph_ids.delta();
    txn.update_where_default_graph_iris = Some(where_default_graph_iris);
    txn.update_where_named_graphs = where_named_graphs;

    if let Some(values_val) = obj.get("values") {
        let values = parse_inline_values(values_val, &context, &mut txn.vars, ns_registry, strict)?;
        txn = txn.with_values(values);
    }

    Ok(txn)
}

fn parse_update_template_default_graph(
    graph_val: Option<&Value>,
    context: &ParsedContext,
    from_named_aliases: &HashMap<String, String>,
    graph_ids: &mut GraphIdAssigner,
    strict: bool,
) -> Result<Option<(u16, String)>> {
    let Some(v) = graph_val else {
        return Ok(None);
    };

    // Allow `"graph": "default"` as a no-op.
    if matches!(v, Value::String(s) if s == "default") {
        return Ok(None);
    }

    // Disallow txn-meta as a write target.
    if matches!(v, Value::String(s) if s == "txn-meta") {
        return Err(TransactError::Parse(
            "graph: \"txn-meta\" is not a valid update write target".to_string(),
        ));
    }

    let resolved = resolve_graph_selector_value_for_update(v, from_named_aliases);
    parse_update_default_graph(Some(&resolved), context, graph_ids, strict)
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

fn parse_update_default_graph(
    graph_val: Option<&Value>,
    context: &ParsedContext,
    graph_ids: &mut GraphIdAssigner,
    strict: bool,
) -> Result<Option<(u16, String)>> {
    let Some(v) = graph_val else {
        return Ok(None);
    };

    let selector = match v {
        Value::String(s) => Value::Object({
            let mut m = serde_json::Map::new();
            m.insert("@id".to_string(), Value::String(s.clone()));
            m
        }),
        Value::Object(obj) => Value::Object(obj.clone()),
        Value::Array(arr) => {
            let Some(first) = arr.first() else {
                return Ok(None);
            };
            match first {
                Value::String(s) => Value::Object({
                    let mut m = serde_json::Map::new();
                    m.insert("@id".to_string(), Value::String(s.clone()));
                    m
                }),
                Value::Object(obj) => Value::Object(obj.clone()),
                _ => {
                    return Err(TransactError::Parse(
                        "graph must be a string IRI (or {\"@id\": ...})".to_string(),
                    ))
                }
            }
        }
        _ => {
            return Err(TransactError::Parse(
                "graph must be a string IRI (or {\"@id\": ...})".to_string(),
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
    .ok_or_else(|| TransactError::Parse("graph must expand to an @id IRI".to_string()))?;

    let g_id = graph_ids.get_or_assign(&iri);
    Ok(Some((g_id, iri)))
}

struct TemplateParseCtx<'a> {
    context: &'a ParsedContext,
    vars: &'a mut VarRegistry,
    ns_registry: &'a mut NamespaceRegistry,
    object_var_parsing: bool,
    strict_compact_iri: bool,
    graph_ids: &'a mut GraphIdAssigner,
    default_graph_id: Option<u16>,
    from_named_aliases: &'a HashMap<String, String>,
    blank_counter: usize,
}

impl<'a> TemplateParseCtx<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        context: &'a ParsedContext,
        vars: &'a mut VarRegistry,
        ns_registry: &'a mut NamespaceRegistry,
        object_var_parsing: bool,
        strict_compact_iri: bool,
        graph_ids: &'a mut GraphIdAssigner,
        default_graph_id: Option<u16>,
        from_named_aliases: &'a HashMap<String, String>,
    ) -> Self {
        Self {
            context,
            vars,
            ns_registry,
            object_var_parsing,
            strict_compact_iri,
            graph_ids,
            default_graph_id,
            from_named_aliases,
            blank_counter: 0,
        }
    }

    /// Expand a predicate or @type value (uses @vocab), respecting strict policy.
    fn expand_vocab(
        &self,
        s: &str,
    ) -> std::result::Result<(String, Option<fluree_graph_json_ld::ContextEntry>), TransactError>
    {
        Ok(fluree_graph_json_ld::details_with_policy(
            s,
            self.context,
            self.strict_compact_iri,
        )?)
    }

    /// Expand a subject @id (uses @base), respecting strict policy.
    // Kept for: pre-expansion @id validation (not yet wired in template parsing).
    // Use when: template subjects need strict compact-IRI checking before expansion.
    #[expect(dead_code)]
    fn expand_id(
        &self,
        s: &str,
    ) -> std::result::Result<(String, Option<fluree_graph_json_ld::ContextEntry>), TransactError>
    {
        Ok(fluree_graph_json_ld::details_with_vocab_policy(
            s,
            self.context,
            false,
            self.strict_compact_iri,
        )?)
    }

    /// Expand a JSON-LD document, respecting strict policy.
    fn expand_document(&self, json: &Value) -> std::result::Result<Value, TransactError> {
        Ok(expand_with_context_policy(
            json,
            self.context,
            self.strict_compact_iri,
        )?)
    }
}

fn parse_update_templates_with_ctx(
    val: &Value,
    ctx: &mut TemplateParseCtx<'_>,
) -> Result<Vec<TripleTemplate>> {
    // Template sugar: allow arrays of the form ["graph", <graph-iri>, <pattern>]
    if let Value::Array(items) = val {
        let mut out: Vec<TripleTemplate> = Vec::new();
        let mut plain_items: Vec<Value> = Vec::new();

        for item in items {
            if let Value::Array(arr) = item {
                if arr.len() == 3 && arr[0].as_str() == Some("graph") {
                    let resolved_graph =
                        resolve_graph_selector_value_for_update(&arr[1], ctx.from_named_aliases);
                    let graph = parse_update_default_graph(
                        Some(&resolved_graph),
                        ctx.context,
                        ctx.graph_ids,
                        ctx.strict_compact_iri,
                    )?
                    .ok_or_else(|| {
                        TransactError::Parse("graph wrapper requires a graph IRI".to_string())
                    })?;
                    let expanded = ctx.expand_document(&arr[2])?;
                    let prev_default = ctx.default_graph_id;
                    ctx.default_graph_id = Some(graph.0);
                    let templates = parse_expanded_triples_with_ctx(&expanded, ctx)?;
                    ctx.default_graph_id = prev_default;
                    out.extend(templates);
                    continue;
                }
            }
            plain_items.push(item.clone());
        }

        if !plain_items.is_empty() {
            let expanded = ctx.expand_document(&Value::Array(plain_items))?;
            let templates = parse_expanded_triples_with_ctx(&expanded, ctx)?;
            out.extend(templates);
        }

        return Ok(out);
    }

    // Non-array templates: parse normally.
    let expanded = ctx.expand_document(val)?;
    parse_expanded_triples_with_ctx(&expanded, ctx)
}

/// Extract and parse the @context from a JSON-LD document
fn extract_context(json: &Value) -> Result<ParsedContext> {
    if let Some(ctx_val) = json.get("@context") {
        Ok(parse_context(&normalize_context_value(ctx_val))?)
    } else {
        Ok(ParsedContext::new())
    }
}

/// Strip top-level keys that must not be interpreted as data by the JSON-LD
/// expander.
///
/// - `opts`: reserved for parse-time options (e.g. `opts.strictCompactIri`).
/// - `txn-meta`: the txn-meta sidecar; entries are extracted separately by
///   [`extract_txn_meta`] and must not appear as transaction data.
///
/// In envelope form (with `@graph`), the expander already ignores extra
/// top-level keys — only the single-object form leaks these as properties.
///
/// Returns `Cow::Borrowed` when no stripping is needed, `Cow::Owned` otherwise.
fn strip_opts_for_expansion(json: &Value) -> std::borrow::Cow<'_, Value> {
    const STRIP_KEYS: &[&str] = &["opts", "txn-meta"];
    match json.as_object() {
        Some(obj) if STRIP_KEYS.iter().any(|k| obj.contains_key(*k)) => {
            let mut cloned = obj.clone();
            for k in STRIP_KEYS {
                cloned.remove(*k);
            }
            std::borrow::Cow::Owned(Value::Object(cloned))
        }
        _ => std::borrow::Cow::Borrowed(json),
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
            out_row.push(parse_values_cell(cell, context, ns_registry, strict)?);
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

/// Parse expanded JSON-LD into triple templates
///
/// Handles both single objects and arrays of objects.
fn parse_expanded_triples_with_ctx(
    expanded: &Value,
    ctx: &mut TemplateParseCtx<'_>,
) -> Result<Vec<TripleTemplate>> {
    ctx.blank_counter = 0;
    match expanded {
        Value::Array(arr) => arr.iter().try_fold(Vec::new(), |mut templates, item| {
            let (_subject, item_templates) = parse_expanded_object_with_ctx(item, ctx)?;
            templates.extend(item_templates);
            Ok(templates)
        }),
        Value::Object(_) => {
            let (_subject, templates) = parse_expanded_object_with_ctx(expanded, ctx)?;
            Ok(templates)
        }
        _ => Err(TransactError::Parse(
            "Expected expanded object or array of objects".to_string(),
        )),
    }
}

/// Parse a single expanded JSON-LD object into triple templates.
///
/// Returns the subject term assigned to this node (IRI, variable, or blank node)
/// along with the generated triples. Callers that need to reference this node
/// (e.g., as the object of a parent triple) use the returned subject directly.
fn parse_expanded_object_with_ctx(
    expanded: &Value,
    ctx: &mut TemplateParseCtx<'_>,
) -> Result<(TemplateTerm, Vec<TripleTemplate>)> {
    let obj = expanded
        .as_object()
        .ok_or_else(|| TransactError::Parse("Expected expanded object".to_string()))?;

    let mut templates = Vec::new();

    // Optional named-graph selector for this node.
    //
    // Transaction JSON-LD supports a non-standard but convenient form:
    // `{ "@id": "...", "@graph": "<graph iri>", ... }`
    //
    // This is distinct from *envelope form* (top-level `@graph: [...]`) used
    // for txn-meta extraction.
    let graph_id = obj
        .get("@graph")
        .and_then(|v| match v {
            Value::String(s) => Some(s.as_str()),
            Value::Object(map) => map.get("@id").and_then(|id| id.as_str()),
            Value::Array(arr) => arr.first().and_then(|x| match x {
                Value::String(s) => Some(s.as_str()),
                Value::Object(map) => map.get("@id").and_then(|id| id.as_str()),
                _ => None,
            }),
            _ => None,
        })
        .map(|raw| -> Result<u16> {
            let resolved = resolve_graph_selector_str_for_templates(raw, ctx)?;
            Ok(ctx.graph_ids.get_or_assign(&resolved))
        })
        .transpose()?;
    let graph_id = graph_id.or(ctx.default_graph_id);

    // Get subject from @id (already expanded IRI or variable)
    let subject = if let Some(id) = obj.get("@id") {
        parse_expanded_id_with_ctx(id, ctx)?
    } else {
        // Generate a blank node if no @id
        let n = ctx.blank_counter;
        ctx.blank_counter += 1;
        TemplateTerm::BlankNode(format!("_:b{n}"))
    };

    // Parse each predicate-object pair
    for (key, value) in obj {
        // Skip JSON-LD keywords except @type which becomes rdf:type
        if key == "@id" || key == "@context" || key == "@graph" {
            continue;
        }

        if key == "@type" {
            // @type becomes rdf:type triples
            let rdf_type_iri = TYPE;
            let predicate = TemplateTerm::Sid(ctx.ns_registry.sid_for_iri(rdf_type_iri));

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
                    let mut t = TripleTemplate::new(subject.clone(), predicate.clone(), object);
                    if let Some(g_id) = graph_id {
                        t = t.with_graph_id(g_id);
                    }
                    templates.push(t);
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

        let parsed_values = parse_expanded_objects_with_ctx(value, ctx, &mut templates)?;

        for parsed_value in parsed_values {
            let mut template =
                TripleTemplate::new(subject.clone(), predicate.clone(), parsed_value.term);
            if let Some(g_id) = graph_id {
                template = template.with_graph_id(g_id);
            }
            if let Some(dtc) = parsed_value.dtc {
                template = template.with_dtc(dtc);
            }
            if let Some(idx) = parsed_value.list_index {
                template = template.with_list_index(idx);
            }
            templates.push(template);
        }
    }

    Ok((subject, templates))
}

fn resolve_graph_selector_str_for_templates(
    raw: &str,
    ctx: &TemplateParseCtx<'_>,
) -> Result<String> {
    if let Some(iri) = ctx.from_named_aliases.get(raw) {
        return Ok(iri.clone());
    }
    let (expanded, _) = ctx.expand_vocab(raw)?;
    Ok(expanded)
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
                // Blank node
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

/// Compatibility wrapper used by unit tests (parses an expanded `@id`).
#[cfg(test)]
fn parse_expanded_id(
    value: &Value,
    vars: &mut VarRegistry,
    ns_registry: &mut NamespaceRegistry,
) -> Result<TemplateTerm> {
    let context = ParsedContext::new();
    let mut graph_ids = GraphIdAssigner::new();
    let empty_aliases: HashMap<String, String> = HashMap::new();
    let mut ctx = TemplateParseCtx::new(
        &context,
        vars,
        ns_registry,
        true,
        true,
        &mut graph_ids,
        None,
        &empty_aliases,
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
#[allow(clippy::too_many_arguments)]
fn parse_expanded_objects_with_ctx(
    value: &Value,
    ctx: &mut TemplateParseCtx<'_>,
    templates: &mut Vec<TripleTemplate>,
) -> Result<Vec<ParsedValue>> {
    match value {
        Value::Array(arr) => {
            let mut results = Vec::new();
            for v in arr {
                // Check if this is a @list object
                if let Value::Object(obj) = v {
                    if let Some(list_val) = obj.get("@list") {
                        // Parse list and add all elements with their indices
                        let list_items = parse_list_values_with_ctx(list_val, ctx, templates)?;
                        results.extend(list_items);
                        continue;
                    }
                }
                // Not a @list, parse normally
                results.push(parse_expanded_value_with_ctx(v, ctx, templates)?);
            }
            Ok(results)
        }
        _ => Ok(vec![parse_expanded_value_with_ctx(value, ctx, templates)?]),
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
#[allow(clippy::too_many_arguments)]
fn parse_expanded_value_with_ctx(
    value: &Value,
    ctx: &mut TemplateParseCtx<'_>,
    templates: &mut Vec<TripleTemplate>,
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
                    let (_subject, nested_templates) = parse_expanded_object_with_ctx(value, ctx)?;
                    templates.extend(nested_templates);
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
                return parse_list_value_with_ctx(list_val, ctx, templates);
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
            let (subject, nested_templates) = parse_expanded_object_with_ctx(value, ctx)?;
            templates.extend(nested_templates);
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
    graph_ids: &mut GraphIdAssigner,
    default_graph_id: Option<u16>,
    from_named_aliases: &HashMap<String, String>,
    blank_counter: &mut usize,
) -> Result<ParsedValue> {
    let mut ctx = TemplateParseCtx::new(
        context,
        vars,
        ns_registry,
        object_var_parsing,
        true,
        graph_ids,
        default_graph_id,
        from_named_aliases,
    );
    ctx.blank_counter = *blank_counter;
    let out = parse_expanded_value_with_ctx(value, &mut ctx, templates);
    *blank_counter = ctx.blank_counter;
    out
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
                // If @value is already a string, use it directly (avoid double-serialization)
                // Only serialize if it's an object, array, or other non-string JSON value
                let json_string = match val {
                    Value::String(s) => s.clone(),
                    _ => serde_json::to_string(val).map_err(|e| {
                        TransactError::Parse(format!("Failed to serialize @json value: {e}"))
                    })?,
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
/// Empty lists produce an error here since we can't return "no value" - the proper
/// empty list handling happens in `parse_expanded_objects` via `parse_list_values`.
#[allow(clippy::too_many_arguments)]
fn parse_list_value_with_ctx(
    list_val: &Value,
    ctx: &mut TemplateParseCtx<'_>,
    templates: &mut Vec<TripleTemplate>,
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

    // Empty list: we can't return "no value" from this function.
    // The proper path for empty lists is through parse_expanded_objects which
    // uses parse_list_values and filters empty results. If we get here with an
    // empty list, it's an edge case where @list wasn't detected at the array level.
    if items.is_empty() {
        return Err(TransactError::Parse(
            "Empty @list in unexpected position (should be handled by parse_expanded_objects)"
                .to_string(),
        ));
    }

    // Parse the first element with index 0
    let first = &items[0];
    let mut parsed = parse_single_list_item_with_ctx(first, ctx, templates)?;
    parsed.list_index = Some(0);
    Ok(parsed)
}

/// Parse list items from a @list value, returning all elements with their indices
#[allow(clippy::too_many_arguments)]
fn parse_list_values_with_ctx(
    list_val: &Value,
    ctx: &mut TemplateParseCtx<'_>,
    templates: &mut Vec<TripleTemplate>,
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

    // Empty list produces zero triples
    if items.is_empty() {
        return Ok(Vec::new());
    }

    // Parse each item with its index
    let mut results = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let mut parsed = parse_single_list_item_with_ctx(item, ctx, templates)?;
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
    graph_ids: &mut GraphIdAssigner,
    default_graph_id: Option<u16>,
    from_named_aliases: &HashMap<String, String>,
    blank_counter: &mut usize,
) -> Result<Vec<ParsedValue>> {
    let mut ctx = TemplateParseCtx::new(
        context,
        vars,
        ns_registry,
        object_var_parsing,
        true,
        graph_ids,
        default_graph_id,
        from_named_aliases,
    );
    ctx.blank_counter = *blank_counter;
    let out = parse_list_values_with_ctx(list_val, &mut ctx, templates);
    *blank_counter = ctx.blank_counter;
    out
}

/// Parse a single item from a @list array
///
/// For `Value::Object` items, delegates to `parse_expanded_value` which already
/// handles all object shapes: `@id` refs, `@value` literals, `@list`, `@variable`,
/// and blank node objects (nested objects without JSON-LD keywords).
#[allow(clippy::too_many_arguments)]
fn parse_single_list_item_with_ctx(
    item: &Value,
    ctx: &mut TemplateParseCtx<'_>,
    templates: &mut Vec<TripleTemplate>,
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
            parse_expanded_value_with_ctx(item, ctx, templates)
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
    fn test_parse_insert_with_context() {
        let mut ns_registry = test_registry();
        let json = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:alice",
            "ex:name": "Alice",
            "ex:age": 30
        });

        let txn = parse_insert(&json, TxnOpts::default(), &mut ns_registry).unwrap();

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

        let txn = parse_update(&json, TxnOpts::default(), &mut ns_registry).unwrap();

        assert_eq!(txn.txn_type, TxnType::Update);
        assert_eq!(txn.where_patterns.len(), 1);
        assert_eq!(txn.delete_templates.len(), 1);
        assert_eq!(txn.insert_templates.len(), 1);
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

        let txn = parse_update(&json, TxnOpts::default(), &mut ns_registry).unwrap();
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

        let txn = parse_update(&json, TxnOpts::default(), &mut ns_registry).unwrap();
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

        let txn = parse_update(&json, TxnOpts::default(), &mut ns_registry).unwrap();
        assert!(
            txn.graph_delta
                .values()
                .any(|iri| iri == "http://example.org/g2"),
            "expected graph_delta to contain resolved graph IRI for alias g2"
        );
        assert!(
            txn.insert_templates.iter().any(|t| t.graph_id.is_some()),
            "expected insert templates to be tagged with a graph_id"
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

        let txn = parse_insert(&json, TxnOpts::default(), &mut ns_registry).unwrap();

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
        let mut graph_ids = GraphIdAssigner::new();
        let mut blank_counter: usize = 0;

        // @value with @type - should preserve datatype
        let val = json!({"@value": "42", "@type": "http://www.w3.org/2001/XMLSchema#integer"});
        let result = parse_expanded_value(
            &val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            &mut templates,
            true,
            &mut graph_ids,
            None,
            &HashMap::new(),
            &mut blank_counter,
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
        let mut graph_ids = GraphIdAssigner::new();
        let mut blank_counter: usize = 0;

        let val = json!({"@value": "before", "@type": "xsd:string"});
        let result = parse_expanded_value(
            &val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            &mut templates,
            true,
            &mut graph_ids,
            None,
            &HashMap::new(),
            &mut blank_counter,
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

    #[test]
    fn test_parse_language_tagged_string() {
        let mut vars = VarRegistry::new();
        let mut ns_registry = test_registry();
        let mut templates: Vec<TripleTemplate> = Vec::new();
        let ctx = ParsedContext::new();
        let mut graph_ids = GraphIdAssigner::new();
        let mut blank_counter: usize = 0;

        // @value with @language
        let val = json!({"@value": "Hello", "@language": "en"});
        let result = parse_expanded_value(
            &val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            &mut templates,
            true,
            &mut graph_ids,
            None,
            &HashMap::new(),
            &mut blank_counter,
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
        let mut graph_ids = GraphIdAssigner::new();
        let mut blank_counter = 0usize;
        let results = parse_list_values(
            &list_val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            true,
            &mut templates,
            &mut graph_ids,
            None,
            &HashMap::new(),
            &mut blank_counter,
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

        // Empty @list produces zero ParsedValues
        let list_val = json!([]);
        let mut templates = Vec::new();
        let mut graph_ids = GraphIdAssigner::new();
        let mut blank_counter = 0usize;
        let results = parse_list_values(
            &list_val,
            &ctx,
            &mut vars,
            &mut ns_registry,
            true,
            &mut templates,
            &mut graph_ids,
            None,
            &HashMap::new(),
            &mut blank_counter,
        )
        .unwrap();
        assert!(results.is_empty());
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
        let mut graph_ids = GraphIdAssigner::new();
        let empty_aliases = HashMap::new();
        let mut parse_ctx = TemplateParseCtx::new(
            &ctx,
            &mut vars,
            &mut ns_registry,
            true,
            true,
            &mut graph_ids,
            None,
            &empty_aliases,
        );
        let templates = parse_expanded_triples_with_ctx(&json, &mut parse_ctx).unwrap();

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
        let mut graph_ids = GraphIdAssigner::new();

        let expanded = json!([{
            "@id": "http://example.org/thing/1",
            "http://example.org/relatedTo": [{
                "@type": ["http://example.org/Widget"],
                "http://example.org/name": [{"@value": "nested-widget"}]
            }]
        }]);

        let empty_aliases = HashMap::new();
        let mut parse_ctx = TemplateParseCtx::new(
            &ctx,
            &mut vars,
            &mut ns_registry,
            false,
            true,
            &mut graph_ids,
            None,
            &empty_aliases,
        );
        let templates = parse_expanded_triples_with_ctx(&expanded, &mut parse_ctx).unwrap();

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
        let mut graph_ids = GraphIdAssigner::new();

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
        let mut parse_ctx = TemplateParseCtx::new(
            &ctx,
            &mut vars,
            &mut ns_registry,
            false,
            true,
            &mut graph_ids,
            None,
            &empty_aliases,
        );
        let templates = parse_expanded_triples_with_ctx(&expanded, &mut parse_ctx).unwrap();

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
        let mut graph_ids = GraphIdAssigner::new();

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
        let mut parse_ctx = TemplateParseCtx::new(
            &ctx,
            &mut vars,
            &mut ns_registry,
            false,
            true,
            &mut graph_ids,
            None,
            &empty_aliases,
        );
        let templates = parse_expanded_triples_with_ctx(&expanded, &mut parse_ctx).unwrap();

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

        let txn = parse_insert(&json, TxnOpts::default(), &mut ns_registry).unwrap();

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

        let txn = parse_insert(&json, TxnOpts::default(), &mut ns_registry).unwrap();

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
}
