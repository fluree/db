//! Edge-annotation pre-expansion lowering for the JSON-LD transactor
//! parser.
//!
//! Walks the raw transaction document **before** JSON-LD expansion and
//! rewrites every `@annotation` / `@edge` / `@reifies` block into the
//! seven-fact `f:reifies*` system encoding. The output is a document
//! that contains only ordinary IRIs (no `@`-keyword extensions), so
//! the rest of the parsing pipeline (`expand_with_context_policy`,
//! `parse_expanded_triples_with_ctx`) processes it unchanged.
//!
//! Two source shapes are accepted:
//!
//! 1. **Inline form** (`@annotation` / `@edge` on the *object* node of
//!    a predicate):
//!    ```json
//!    { "@id": "ex:alice",
//!      "ex:worksFor": {
//!        "@id": "ex:acme",
//!        "@annotation": { "ex:role": "Engineer" }
//!      }
//!    }
//!    ```
//!    The annotation reifies the edge `(ex:alice, ex:worksFor, ex:acme)`.
//!    The annotation subject is the `@id` inside the `@annotation` block,
//!    or a freshly-minted blank node when absent.
//!
//! 2. **Annotation-rooted form** (`@reifies` on an enclosing node):
//!    ```json
//!    { "@id": "ex:employment-1",
//!      "ex:role": "Engineer",
//!      "@reifies": {
//!        "@id": "ex:alice",
//!        "ex:worksFor": { "@id": "ex:acme" }
//!      }
//!    }
//!    ```
//!    The enclosing node IS the annotation; `@reifies` names the base
//!    edge it reifies. We emit the base edge as a *sibling* top-level
//!    node so the standard parser asserts it.
//!
//! Strict deferred-shape rejection (per
//! `EDGE_ANNOTATIONS_IMPL_PLAN.md` decisions section):
//!
//! - Literal-valued annotations (`@value` + `@annotation`) → error.
//! - Multi-triple `@reifies` (more than one predicate-object pair) →
//!   error.
//! - Annotation-of-annotation (nested `@annotation` inside an
//!   annotation body) → error.
//! - Reifiers attached to triple-term values, list elements, etc. →
//!   error.
//!
//! After lowering, the document additionally goes through a write-
//! surface firewall pass that rejects any user-authored `f:reifies*`
//! IRI (full or compact). This pre-lowering scan is run before the
//! lowering itself, so the firewall doesn't block the IRIs that this
//! module emits.

use crate::error::{Result, TransactError};
use fluree_graph_json_ld::{expand_iri, parse_context, ParsedContext};
use fluree_vocab::reifies_iris;
use serde_json::{json, Map, Value};

const ANNOTATION_KEY: &str = "@annotation";
const EDGE_KEY: &str = "@edge";
const REIFIES_KEY: &str = "@reifies";

/// True when `key` is an annotation/edge keyword on the *object* side.
fn is_annotation_key(key: &str) -> bool {
    key == ANNOTATION_KEY || key == EDGE_KEY
}

/// Mutable counter used to mint unique blank-node IDs for anonymous
/// annotation subjects. Threaded through the recursion so siblings
/// don't collide.
pub(crate) struct LowerCtx {
    /// Counter for `_:fluree_ann_N` blank-node IDs.
    next_anon_id: usize,
    /// Sibling top-level nodes synthesized during lowering. Each entry
    /// is a complete node-map ready for the standard parser to ingest.
    siblings: Vec<Value>,
}

impl LowerCtx {
    fn new() -> Self {
        Self {
            next_anon_id: 0,
            siblings: Vec::new(),
        }
    }

    fn mint_blank(&mut self) -> String {
        let id = format!("_:fluree_ann_{}", self.next_anon_id);
        self.next_anon_id += 1;
        id
    }
}

/// Reject any user-authored `f:reifies*` IRI before lowering runs, so
/// the firewall doesn't fire on the IRIs that this module
/// synthesizes.
///
/// Resolves compact-IRI forms (e.g. `f:reifiesSubject` with
/// `"f": "https://ns.flur.ee/db#"` in `@context`) through the
/// document's `@context` before checking. Without this, a user could
/// bypass the firewall by writing the compact form and having
/// expansion silently introduce system facts.
///
/// The walker maintains a stack of merged contexts so per-node
/// `@context` overrides are honored. When a key has no `@context`
/// resolution and isn't already a full IRI, it's left untouched —
/// downstream JSON-LD expansion will fail or treat it as a plain
/// string, neither of which can produce a reserved-predicate flake.
fn scan_user_authored_reifies_iris(value: &Value, context: &ParsedContext) -> Result<()> {
    match value {
        Value::Object(map) => {
            // Merge any per-node `@context` into the inherited one.
            let merged: Option<ParsedContext> = if let Some(local_ctx) = map.get("@context") {
                Some(
                    fluree_graph_json_ld::parse_context_with_base(context, local_ctx).map_err(
                        |e| {
                            TransactError::Parse(format!(
                                "failed to parse nested @context during firewall scan: {e}"
                            ))
                        },
                    )?,
                )
            } else {
                None
            };
            let effective = merged.as_ref().unwrap_or(context);

            for (k, v) in map {
                // `@context` is structural, not a predicate.
                if k == "@context" {
                    continue;
                }
                let expanded_key = if k.starts_with('@') {
                    k.clone()
                } else {
                    expand_iri(k, effective)
                };
                if reifies_iris::ALL.iter().any(|iri| *iri == expanded_key) {
                    return Err(TransactError::UnsupportedFeature(format!(
                        "'{k}' resolves to a system-controlled predicate '{expanded_key}'; \
                         use @annotation or @reifies instead"
                    )));
                }
                scan_user_authored_reifies_iris(v, effective)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                scan_user_authored_reifies_iris(item, context)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Top-level entry point. Walks `doc` and rewrites all
/// `@annotation` / `@edge` / `@reifies` blocks into the equivalent
/// `f:reifies*` triples.
///
/// The transformation is in-place on the structure of `doc` plus
/// possibly appended sibling nodes (returned alongside). Callers
/// should re-wrap accordingly:
///
/// - Single-node form `{ ... }` → `{"@graph": [<original>, <siblings>...]}`
///   when siblings exist, otherwise unchanged.
/// - Array form `[...]` → original array with siblings appended.
/// - Envelope form `{"@graph": [...]}` → siblings appended to the
///   existing `@graph` array.
pub fn lower_edge_annotations(doc: &mut Value) -> Result<()> {
    let top_ctx = top_level_context(doc)?;
    scan_user_authored_reifies_iris(doc, &top_ctx)?;
    lower_edge_annotations_after_firewall(doc, &top_ctx)
}

/// Same as [`lower_edge_annotations`] minus the firewall scan.
///
/// Use this when the caller has already run the firewall on the
/// original (pre-rewritten) document — e.g. when chaining the
/// `delete`-clause pre-pass before the standard lowering. Both
/// passes synthesize `f:reifies*` IRIs internally; running the
/// firewall a second time would falsely flag those.
pub fn lower_edge_annotations_after_firewall(
    doc: &mut Value,
    top_ctx: &ParsedContext,
) -> Result<()> {
    let mut ctx = LowerCtx::new();
    let walk_ctx = WalkCtx {
        json_ld: top_ctx,
        graph: None,
    };
    lower_value_with_subject(doc, None, &walk_ctx, &mut ctx)?;

    if !ctx.siblings.is_empty() {
        attach_siblings(doc, ctx.siblings);
    }
    Ok(())
}

/// Parse the document's top-level `@context` once. Used by both the
/// firewall scan and the lowering walker so they share IRI
/// resolution for compact forms.
pub fn top_level_context(doc: &Value) -> Result<ParsedContext> {
    Ok(doc
        .as_object()
        .and_then(|m| m.get("@context"))
        .map(parse_context)
        .transpose()
        .map_err(|e| TransactError::Parse(format!("failed to parse @context: {e}")))?
        .unwrap_or_else(ParsedContext::new))
}

/// Run the user-authored `f:reifies*` firewall against the ORIGINAL
/// document, before any lowering pass runs. Exposed for the
/// transactor's two-pass dispatch (delete-clause pre-pass + standard
/// lowering) so the firewall fires once on the user's input.
pub fn run_user_authored_reifies_firewall(doc: &Value, top_ctx: &ParsedContext) -> Result<()> {
    scan_user_authored_reifies_iris(doc, top_ctx)
}

/// Pre-pass for UPDATE transactions: rewrite `@annotation` blocks
/// inside the `delete` clause into explicit `f:reifies*` retract
/// templates **before** the main `lower_edge_annotations` walker runs.
///
/// The default lowering treats `@annotation` as an assertion — it
/// synthesizes a sibling node with `f:reifiesSubject` etc. and a
/// freshly-minted blank-node SID. That works for inserts but is
/// wrong for deletes: the synthesized SID never matches existing
/// data, so the delete becomes a silent no-op. This pre-pass
/// rewrites delete-clause `@annotation` blocks into delete-template
/// shape directly.
///
/// **Supported shape — by annotation id:**
///
/// ```json
/// {"delete": {
///     "@id": "ex:alice",
///     "ex:worksFor": {
///         "@id": "ex:acme",
///         "@annotation": { "@id": "ex:emp/A" }
///     }
/// }}
/// ```
///
/// becomes (after this pass):
///
/// ```json
/// {"delete": [
///     {"@id": "ex:emp/A",
///      "f:reifiesSubject":   {"@id": "ex:alice"},
///      "f:reifiesPredicate": {"@id": "ex:worksFor"},
///      "f:reifiesObject":    {"@id": "ex:acme"}}
/// ]}
/// ```
///
/// The base-edge selector (the surrounding `{@id alice ex:worksFor:
/// {@id acme}}`) is *not* lowered into a base-edge retract — per the
/// design contract, this shape retracts exactly the targeted
/// annotation occurrence, not the edge it reifies. Users who want
/// both the annotation and the edge gone should issue two delete
/// statements.
///
/// **Selector shape:** an `@annotation` block without an explicit
/// `@id` (e.g. `{ ex:role: "Engineer" }`) requires runtime
/// resolution against live data. The pre-pass mints a fresh
/// variable `?_fluree_del_ann_<n>`, synthesizes a `@reifies`-rooted
/// WHERE pattern that binds the variable to every live annotation
/// matching the selector body and reifying the named edge, and
/// emits a by-variable delete template for the bundle. The standard
/// SPARQL UPDATE machinery then runs the WHERE, instantiates the
/// template per binding, and the resulting bundle retracts trigger
/// the LPG body cleanup pass at stage time.
pub fn lower_delete_annotation_blocks(doc: &mut Value) -> Result<()> {
    // Read the @context BEFORE we borrow the doc mutably for the
    // delete-clause walk — both views are needed and serde_json's
    // immutable / mutable accessors don't compose otherwise.
    let top_ctx = top_level_context(doc)?;
    let Some(obj) = doc.as_object_mut() else {
        return Ok(());
    };
    if !obj.contains_key("delete") {
        return Ok(());
    }
    // Take ownership of `delete` (and the `where` clause if any) so
    // we can splice both. Re-insert at the end. `obj.get_mut`
    // borrows can't compose with subsequent inserts, so the
    // remove/walk/insert dance keeps the borrow checker happy.
    let mut delete_val = obj.remove("delete").expect("checked above");
    let where_val = obj.remove("where");

    let mut new_templates: Vec<Value> = Vec::new();
    let mut new_where_patterns: Vec<Value> = Vec::new();
    let mut next_var: u32 = 0;
    // Top-level graph context: the transactor's UPDATE doc has no
    // envelope `@graph` (that's the insert/upsert envelope form),
    // so we start from `None` and let per-node `@graph: "<iri>"`
    // selectors inside the delete clause set the inherited graph as
    // we recurse. The `@graph` value gets threaded into the
    // synthesized retract template so the retract flake's
    // `g = Some(graph_sid)` matches the original assertion's flake
    // identity.
    walk_delete_for_annotations(
        &mut delete_val,
        &top_ctx,
        None,
        &mut new_templates,
        &mut new_where_patterns,
        &mut next_var,
    )?;

    // Splice the new templates into the delete clause. Convert
    // single-object form to an array, then append.
    if !new_templates.is_empty() {
        let mut items: Vec<Value> = match delete_val {
            Value::Array(arr) => arr.into_iter().filter(|v| !is_empty_node(v)).collect(),
            Value::Null => Vec::new(),
            other => {
                if is_empty_node(&other) {
                    Vec::new()
                } else {
                    vec![other]
                }
            }
        };
        items.extend(new_templates);
        delete_val = Value::Array(items);
    }
    obj.insert("delete".to_string(), delete_val);

    // Splice the new WHERE patterns into the existing where clause
    // (or create one). Selector-form retracts depend on these
    // patterns to bind their variables to live annotations. The
    // parser accepts an array or a single object — normalize to
    // array when we need to merge.
    if !new_where_patterns.is_empty() {
        let merged = match where_val {
            None => Value::Array(new_where_patterns),
            Some(Value::Array(mut arr)) => {
                arr.extend(new_where_patterns);
                Value::Array(arr)
            }
            Some(other) => {
                let mut items = vec![other];
                items.extend(new_where_patterns);
                Value::Array(items)
            }
        };
        obj.insert("where".to_string(), merged);
    } else if let Some(w) = where_val {
        obj.insert("where".to_string(), w);
    }

    Ok(())
}

/// True iff `v` is `{"@id": ...}` or `{}` with no other keys — a
/// node-map that contributes no triples and would error out as a
/// "structurally empty" delete template. We strip these after
/// removing an `@annotation` block.
fn is_empty_node(v: &Value) -> bool {
    let Some(obj) = v.as_object() else {
        return false;
    };
    if obj.is_empty() {
        return true;
    }
    obj.iter().all(|(k, _)| k == "@id" || k == "@context")
}

/// Walk the delete clause looking for `@annotation` blocks. For each
/// one with an explicit `@id`, build a retract template, then strip
/// the `<predicate>: { ... @annotation: ... }` pair from its parent
/// so the base edge isn't accidentally retracted alongside.
///
/// `inherited_graph` is the named-graph IRI in scope for this
/// subtree. Per-node `@graph: "<iri>"` selectors override it for
/// their subtree. The graph IRI is threaded into the synthesized
/// retract template so the retract flake matches the original
/// assertion's flake identity (which carries `g = Some(graph_sid)`
/// for named-graph annotations).
fn walk_delete_for_annotations(
    val: &mut Value,
    ctx: &ParsedContext,
    inherited_graph: Option<&str>,
    out_templates: &mut Vec<Value>,
    out_where: &mut Vec<Value>,
    next_var: &mut u32,
) -> Result<()> {
    match val {
        Value::Array(items) => {
            for item in items {
                walk_delete_for_annotations(
                    item,
                    ctx,
                    inherited_graph,
                    out_templates,
                    out_where,
                    next_var,
                )?;
            }
            Ok(())
        }
        Value::Object(map) => {
            // Capture the parent's @id for `f:reifiesSubject`. Honor
            // context-aliased `@id` keys (e.g. `id: ...` when context
            // says `"id": "@id"`).
            let parent_id = map
                .get("@id")
                .and_then(Value::as_str)
                .or_else(|| {
                    let alias = ctx.id_key.as_str();
                    if alias != "@id" {
                        map.get(alias).and_then(Value::as_str)
                    } else {
                        None
                    }
                })
                .map(String::from);

            // Per-node `@graph: "<iri>"` overrides the inherited
            // graph for this subtree. Mirrors
            // `extract_node_graph_selector` from the assertion-side
            // walker so the two paths agree on graph scoping.
            let node_graph: Option<String> = match map.get("@graph") {
                Some(Value::String(s)) => Some(s.clone()),
                Some(Value::Object(g)) => g.get("@id").and_then(Value::as_str).map(String::from),
                _ => None,
            };
            let node_graph_ref = node_graph.as_deref();
            let effective_graph: Option<&str> = node_graph_ref.or(inherited_graph);

            // First pass: find predicate keys whose value carries an
            // `@annotation`, lift them into retract templates, and
            // remove from the parent. We collect mutations first to
            // avoid borrowing twice.
            let predicate_keys: Vec<String> = map
                .keys()
                .filter(|k| !k.starts_with('@') && *k != ctx.id_key.as_str())
                .cloned()
                .collect();

            let mut keys_to_remove: Vec<String> = Vec::new();
            for key in predicate_keys {
                let value = map.get_mut(&key).expect("collected key");
                lift_annotations_under_predicate(
                    parent_id.as_deref(),
                    &key,
                    value,
                    ctx,
                    effective_graph,
                    out_templates,
                    out_where,
                    next_var,
                    &mut |strip_predicate| {
                        if strip_predicate {
                            keys_to_remove.push(key.clone());
                        }
                    },
                )?;
            }
            for k in keys_to_remove {
                map.remove(&k);
            }

            // Second pass: recurse into remaining (non-stripped)
            // child values to handle nested delete shapes.
            let remaining: Vec<String> = map.keys().cloned().collect();
            for key in remaining {
                if let Some(v) = map.get_mut(&key) {
                    walk_delete_for_annotations(
                        v,
                        ctx,
                        effective_graph,
                        out_templates,
                        out_where,
                        next_var,
                    )?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Process the value of a single predicate inside a delete clause.
/// When the value is an object containing an `@annotation` block,
/// lift it into a retract template (appended to `out`) and signal
/// the caller to strip the predicate from the parent.
///
/// `inherited_graph` is the named-graph in scope at the predicate-
/// value's parent; per-object `@graph` overrides apply on the inner
/// node and propagate via the recursion in `walk_delete_for_annotations`.
fn lift_annotations_under_predicate(
    parent_subject: Option<&str>,
    predicate: &str,
    value: &mut Value,
    ctx: &ParsedContext,
    inherited_graph: Option<&str>,
    out_templates: &mut Vec<Value>,
    out_where: &mut Vec<Value>,
    next_var: &mut u32,
    strip_callback: &mut dyn FnMut(bool),
) -> Result<()> {
    match value {
        Value::Array(items) => {
            // Each array item gets the same parent context. We
            // process and remove the items that carry annotations,
            // leaving the rest in place. If every item is stripped,
            // the parent's predicate becomes redundant.
            let mut survivors: Vec<Value> = Vec::with_capacity(items.len());
            for mut item in std::mem::take(items) {
                let mut item_stripped = false;
                lift_annotations_under_predicate(
                    parent_subject,
                    predicate,
                    &mut item,
                    ctx,
                    inherited_graph,
                    out_templates,
                    out_where,
                    next_var,
                    &mut |s| item_stripped = s,
                )?;
                if !item_stripped {
                    survivors.push(item);
                }
            }
            if survivors.is_empty() {
                strip_callback(true);
            } else {
                *items = survivors;
            }
            Ok(())
        }
        Value::Object(map) => {
            if !map.contains_key(ANNOTATION_KEY) && !map.contains_key(EDGE_KEY) {
                return Ok(());
            }
            let parent_subject = parent_subject.ok_or_else(|| {
                TransactError::Parse(
                    "delete-clause @annotation requires the enclosing predicate to have a \
                     parent @id (the subject of the reified edge)"
                        .to_string(),
                )
            })?;
            let object_id = map
                .get("@id")
                .and_then(Value::as_str)
                .map(String::from)
                .ok_or_else(|| {
                    TransactError::Parse(
                        "delete-clause @annotation requires the parent object node to carry \
                         an explicit @id (the object of the reified edge)"
                            .to_string(),
                    )
                })?;
            // Per-object `@graph` selector on the *base edge object*
            // overrides the inherited graph. Mirrors the
            // assertion-side walker's per-node graph extraction.
            let object_graph: Option<String> = match map.get("@graph") {
                Some(Value::String(s)) => Some(s.clone()),
                Some(Value::Object(g)) => g.get("@id").and_then(Value::as_str).map(String::from),
                _ => None,
            };
            let effective_graph: Option<&str> = object_graph.as_deref().or(inherited_graph);

            let ann_block = map.remove(ANNOTATION_KEY).or_else(|| map.remove(EDGE_KEY));
            let Some(ann_block) = ann_block else {
                return Ok(());
            };

            build_annotation_delete(
                parent_subject,
                predicate,
                &object_id,
                ann_block,
                ctx,
                effective_graph,
                out_templates,
                out_where,
                next_var,
            )?;

            // After lifting, the parent's predicate-value is just
            // `{@id: <object>}`. That alone is a structurally empty
            // node — but stripping the *entire* predicate from the
            // parent matches the design contract: by-id retracts
            // do not delete the base edge.
            strip_callback(true);
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Build the retract delete template(s) for one `@annotation` block,
/// dispatching on shape:
///
/// - **By-id** (block has explicit `@id`): emit a single delete
///   template with the same seven-fact `f:reifies*` shape used by the
///   assertion-side lowering. The standard
///   `parse_update_templates_with_ctx` path turns it into per-flake
///   retracts. Pushed to `out_templates` only — no WHERE pattern
///   needed.
///
/// - **By-selector** (no `@id`, body holds selector properties): mint
///   a fresh variable `?_fluree_del_ann_<n>`, push a `@reifies`-rooted
///   WHERE pattern that binds the variable to every live annotation
///   matching the selector body and reifying the named edge, and push
///   a delete template keyed by the variable. The SPARQL UPDATE
///   machinery instantiates the template per WHERE binding.
fn build_annotation_delete(
    parent_subject: &str,
    predicate: &str,
    object_id: &str,
    ann_block: Value,
    ctx: &ParsedContext,
    graph_iri: Option<&str>,
    out_templates: &mut Vec<Value>,
    out_where: &mut Vec<Value>,
    next_var: &mut u32,
) -> Result<()> {
    let Value::Object(mut ann_map) = ann_block else {
        return Err(TransactError::Parse(
            "@annotation value must be a JSON object".to_string(),
        ));
    };
    let id_alias = ctx.id_key.as_str();
    let explicit_id: Option<String> = ann_map
        .get("@id")
        .and_then(Value::as_str)
        .or_else(|| {
            if id_alias != "@id" {
                ann_map.get(id_alias).and_then(Value::as_str)
            } else {
                None
            }
        })
        .map(String::from);

    let template_id: String = if let Some(ann_id) = explicit_id {
        // Reject blank-node @ids: a user can't legitimately pin a
        // specific anonymous annotation by its blank-node id (those
        // are minted server-side at insert time). If they really
        // want to retract an anonymous annotation, they should use
        // the selector form (no @id) and let the WHERE clause
        // resolve it.
        if ann_id.starts_with("_:") {
            return Err(TransactError::UnsupportedFeature(
                "delete by anonymous annotation @id is not supported — anonymous SIDs \
                 are minted at insert time and not user-addressable. Use a selector \
                 form (no @id, body properties only) or attach an explicit @id at insert."
                    .to_string(),
            ));
        }
        ann_id
    } else {
        // Selector form. Mint a unique variable, build a WHERE
        // pattern that constrains it to live annotations matching
        // the selector body and reifying the named edge, and use
        // the variable as the delete-template @id.
        let var = format!("?_fluree_del_ann_{}", *next_var);
        *next_var += 1;

        // Strip the alias so we don't carry duplicate @id keys into
        // the WHERE pattern (we'll insert our own).
        ann_map.remove("@id");
        if id_alias != "@id" {
            ann_map.remove(id_alias);
        }

        // Reject nested annotations / @reifies inside the selector
        // body — same deferral rule as inserts.
        scan_nested_annotation_keywords(&Value::Object(ann_map.clone()))?;

        // Build the WHERE pattern as a flat triple-pattern node. The
        // body properties (remaining in `ann_map`) act as selector
        // predicates; the `f:reifies*` triples pin the annotation to
        // the (parent_subject, predicate, object_id) edge. We emit
        // the system predicates directly rather than the higher-level
        // `@reifies` shape because the standard lowering walker
        // rejects `@reifies` outside its query-side context, while
        // `f:reifies*` IRIs are accepted as ordinary IRIs (the
        // user-authored-reifies firewall has already run against the
        // original doc, so our synthesized ones aren't re-scanned).
        // The JSON-LD-Q query parser still resolves `f:reifies*`
        // triple patterns into the same indexed lookups as
        // `@reifies` would.
        let mut where_node = ann_map.clone();
        where_node.insert("@id".to_string(), Value::String(var.clone()));
        where_node.insert(
            reifies_iris::SUBJECT.to_string(),
            json!({"@id": parent_subject}),
        );
        where_node.insert(
            reifies_iris::PREDICATE.to_string(),
            json!({"@id": predicate}),
        );
        where_node.insert(reifies_iris::OBJECT.to_string(), json!({"@id": object_id}));
        if let Some(graph) = graph_iri {
            where_node.insert(reifies_iris::GRAPH.to_string(), json!({"@id": graph}));
            // Named-graph case: wrap the node in the JLDQ s-expression
            // graph form `["graph", "<iri>", { ...patterns... }]` so
            // the WHERE evaluation scopes its triple matches to the
            // named graph. Without the wrapper a default-graph WHERE
            // would not see flakes that live only inside the named
            // graph, and the variable would never bind.
            //
            // The graph name is stored unchanged through to execution
            // (`GraphName::Iri(Arc::from(name))` in `parse/lower.rs`),
            // so we expand compact IRIs here against the document's
            // top-level `@context` rather than leaving them for a
            // resolution pass that doesn't run for this position.
            let expanded_graph = expand_iri(graph, ctx);
            out_where.push(json!(["graph", expanded_graph, Value::Object(where_node),]));
        } else {
            out_where.push(Value::Object(where_node));
        }

        var
    };

    let mut template = Map::new();
    template.insert("@id".to_string(), Value::String(template_id));
    template.insert(
        reifies_iris::SUBJECT.to_string(),
        json!({"@id": parent_subject}),
    );
    template.insert(
        reifies_iris::PREDICATE.to_string(),
        json!({"@id": predicate}),
    );
    template.insert(reifies_iris::OBJECT.to_string(), json!({"@id": object_id}));
    // For named-graph annotations, both the f:reifies* flakes and
    // the synthetic annotation node live in the reified edge's
    // graph. Without this, the parser would emit default-graph
    // retract flakes (`g = None`) which can't cancel named-graph
    // assertions — flake identity includes `g`. Mirrors the
    // assertion-side `build_annotation_sibling` graph emission so
    // assert and retract round-trip exactly.
    if let Some(graph) = graph_iri {
        template.insert(reifies_iris::GRAPH.to_string(), json!({"@id": graph}));
        template.insert("@graph".to_string(), Value::String(graph.to_string()));
    }
    out_templates.push(Value::Object(template));
    Ok(())
}

/// Inherited context for the lowering walker.
///
/// `json_ld` is used for compact-IRI resolution checks (the same
/// context the firewall scan resolves against). `graph` is the
/// in-effect named-graph IRI for the current node — propagated from
/// envelope-level `@graph` selectors and per-node `@graph: "<iri>"`
/// keys, so synthetic annotation siblings can carry `f:reifiesGraph`
/// when the reified edge lives in a named graph.
#[derive(Clone, Copy)]
pub(crate) struct WalkCtx<'a> {
    json_ld: &'a ParsedContext,
    graph: Option<&'a str>,
}

/// Recursively reject `@annotation` / `@edge` / `@reifies` anywhere
/// inside `value`. Used to enforce the annotation-of-annotation
/// deferral on annotation bodies.
fn scan_nested_annotation_keywords(value: &Value) -> Result<()> {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                if is_annotation_key(k) {
                    return Err(TransactError::UnsupportedFeature(format!(
                        "{k} nested inside an @annotation body is the deferred \
                         annotation-of-annotation shape (v1)"
                    )));
                }
                if k == REIFIES_KEY {
                    return Err(TransactError::UnsupportedFeature(
                        "@reifies nested inside an @annotation body is the \
                         deferred nested-triple-term shape (v1)"
                            .to_string(),
                    ));
                }
                scan_nested_annotation_keywords(v)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                scan_nested_annotation_keywords(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Read or mint the `@id` of `map`, ensuring future references resolve
/// to the same identifier. When the node has no `@id`, we add
/// `"@id": "_:fluree_ann_N"` so the subsequent annotation lowering and
/// the standard parser see the same blank node.
///
/// Honors `walk.json_ld.id_key` so context-aliased ids (e.g.
/// `"@context": {"id": "@id"}` with node `{"id": "foo", ...}`) are
/// recognized and the synthetic blank is not minted on top of them.
/// Inserting a literal `@id` alongside an aliased one would leave the
/// JSON-LD expander with two `@id` candidates and silently drop the
/// surrounding predicates.
fn ensure_subject_id(
    map: &mut Map<String, Value>,
    walk: &WalkCtx<'_>,
    ctx: &mut LowerCtx,
) -> String {
    if let Some(Value::String(s)) = map.get("@id") {
        return s.clone();
    }
    let id_alias = walk.json_ld.id_key.as_str();
    if id_alias != "@id" {
        if let Some(Value::String(s)) = map.get(id_alias) {
            return s.clone();
        }
    }
    let new_id = ctx.mint_blank();
    map.insert("@id".to_string(), json!(new_id.clone()));
    new_id
}

/// Construct the synthetic top-level node for an annotation. Returns a
/// node-map containing `@id` (the annotation subject) plus the seven
/// `f:reifies*` predicates and the body properties.
///
/// `base_subject_id` is `None` when we cannot determine the surrounding
/// node's `@id` from the local context (e.g. the parent had not yet
/// been finalized when we entered this call). In that case we pass the
/// resolution responsibility to the caller via a deferred-id scheme:
/// the synthetic node carries `f:reifiesSubject` as a *placeholder*
/// blank-node reference that the second lowering pass binds.
///
/// In v1, the surrounding `lower_object` already runs before this
/// helper for top-level objects, so `base_subject_id` is always
/// populated by the call chain that reaches the inline form. The
/// `None` arm exists for future deeper nesting and is rejected at
/// runtime as a deferred shape.
fn build_annotation_sibling(
    base_subject_id: Option<&str>,
    predicate: &str,
    object_id: &str,
    ann_block: Value,
    base_graph: Option<&str>,
    ctx: &mut LowerCtx,
) -> Result<Value> {
    let base_subject_id = base_subject_id.ok_or_else(|| {
        TransactError::UnsupportedFeature(
            "edge annotations on deeply-nested predicate paths are not supported in v1; \
             attach @annotation to a single hop from a top-level @id"
                .to_string(),
        )
    })?;

    let Value::Object(mut ann_map) = ann_block else {
        return Err(TransactError::Parse(
            "@annotation value must be a JSON object describing the annotation subject".to_string(),
        ));
    };

    // Reject nested @annotation / @edge / @reifies anywhere in the body
    // — annotation-of-annotation is the deferred shape (v1).
    scan_nested_annotation_keywords(&Value::Object(ann_map.clone()))?;

    // Annotation subject: explicit @id or a fresh blank node.
    let ann_id = if let Some(Value::String(s)) = ann_map.get("@id") {
        s.clone()
    } else {
        ctx.mint_blank()
    };
    ann_map.insert("@id".to_string(), json!(ann_id.clone()));

    // f:reifies* predicates pinning the base edge.
    ann_map.insert(
        reifies_iris::SUBJECT.to_string(),
        json!({"@id": base_subject_id}),
    );
    ann_map.insert(
        reifies_iris::PREDICATE.to_string(),
        json!({"@id": predicate}),
    );
    ann_map.insert(reifies_iris::OBJECT.to_string(), json!({"@id": object_id}));

    // f:reifiesGraph — emitted iff the reified edge lives in a named
    // graph. Default-graph edges omit it (absence = default), which
    // matches the encoding in `EdgeKey::to_reifies_facts` and the
    // bundle validator's "at most one" rule for `f:reifiesGraph`.
    //
    // The synthetic annotation node *also* lives in the same named
    // graph as the edge it reifies, so we set its own `@graph`
    // selector to the same IRI. Otherwise the annotation flakes
    // would land in the default graph while the edge is in a named
    // graph — a partition that breaks both visibility and cascade.
    if let Some(graph_iri) = base_graph {
        ann_map.insert(reifies_iris::GRAPH.to_string(), json!({"@id": graph_iri}));
        ann_map.insert("@graph".to_string(), json!(graph_iri));
    }

    // f:reifiesDatatype is intentionally omitted at lowering time —
    // we don't know the object's datatype before JSON-LD expansion.
    // The decoder treats it as optional and derives the canonical
    // datatype from the flake-level `dt` of `f:reifiesObject`. The
    // in-Rust `EdgeKey::to_reifies_facts` builder still emits both
    // for diagnostic clarity.

    Ok(Value::Object(ann_map))
}

/// Lower a `@reifies` block on the enclosing node. The enclosing node
/// IS the annotation; `@reifies` names the base edge.
fn lower_reifies_block(
    map: &mut Map<String, Value>,
    reifies_val: Value,
    _ctx: &mut LowerCtx,
) -> Result<()> {
    let Value::Object(reifies_map) = reifies_val else {
        return Err(TransactError::Parse(
            "@reifies value must be a JSON object describing the base triple".to_string(),
        ));
    };

    // Reject nested annotations inside @reifies (v1 deferral).
    for (k, _) in &reifies_map {
        if is_annotation_key(k) || k == REIFIES_KEY {
            return Err(TransactError::UnsupportedFeature(format!(
                "{k} inside @reifies is the deferred nested-triple-term shape (v1)"
            )));
        }
    }

    // Subject of the base edge: @id of the @reifies node-map.
    let Some(Value::String(base_subject)) = reifies_map.get("@id") else {
        return Err(TransactError::Parse(
            "@reifies must include an @id naming the base subject".to_string(),
        ));
    };

    // Find the single predicate-object pair (non-`@`-keyword key).
    let pred_obj_pairs: Vec<(&String, &Value)> = reifies_map
        .iter()
        .filter(|(k, _)| !k.starts_with('@'))
        .collect();
    if pred_obj_pairs.len() != 1 {
        return Err(TransactError::UnsupportedFeature(format!(
            "@reifies must describe exactly one base triple (got {} predicates); \
             multi-triple reifiers are deferred to v2",
            pred_obj_pairs.len()
        )));
    }
    let (predicate, object_val) = pred_obj_pairs[0];

    // Resolve the object: must be an IRI string, `{"@id": "..."}`, or a
    // blank node; literal-valued reifiers are deferred.
    let object_id = match object_val {
        Value::String(s) => s.clone(),
        Value::Object(ov) => match ov.get("@id") {
            Some(Value::String(s)) => s.clone(),
            _ => {
                return Err(TransactError::UnsupportedFeature(
                    "@reifies object position: literal-valued or multi-property objects are deferred (v1); \
                     reify only IRI-typed (or @id-shaped) objects"
                        .to_string(),
                ));
            }
        },
        _ => {
            return Err(TransactError::Parse(
                "@reifies object must be an IRI string, @id reference, or variable".to_string(),
            ));
        }
    };

    // Inject f:reifies* predicates onto the enclosing map.
    map.insert(
        reifies_iris::SUBJECT.to_string(),
        json!({"@id": base_subject}),
    );
    map.insert(
        reifies_iris::PREDICATE.to_string(),
        json!({"@id": predicate}),
    );
    map.insert(reifies_iris::OBJECT.to_string(), json!({"@id": object_id}));

    // The base edge is asserted by the user including @reifies, so
    // we don't synthesize a sibling for it: presence of f:reifiesSubject /
    // f:reifiesPredicate / f:reifiesObject IS the assertion intent at
    // the system level; the actual base flake is asserted via the
    // `f:reifies*` mechanism plus the AttachmentNovelty observer in
    // M1's runtime path. M2 layers an arena on top.
    //
    // Wait — actually no. `@reifies` is *only* a query-side construct
    // in v1 per the design doc. On the insert path, `@reifies` is
    // currently rejected as the deferred unasserted-reifier shape.
    Err(TransactError::UnsupportedFeature(
        "@reifies on inserts is deferred (v1); use @annotation on the inline form instead, \
         or split the insert into the base edge plus a separate annotation node"
            .to_string(),
    ))
}

/// Append synthetic sibling nodes to the document so the standard
/// parser ingests them alongside the original payload.
fn attach_siblings(doc: &mut Value, siblings: Vec<Value>) {
    if siblings.is_empty() {
        return;
    }
    match doc {
        Value::Object(map) => {
            // Envelope form: append to the existing @graph.
            if let Some(Value::Array(arr)) = map.get_mut("@graph") {
                arr.extend(siblings);
                return;
            }
            // Single-node form: rewrap as `{"@graph": [original, siblings...]}`,
            // preserving the @context at the top level.
            let original = std::mem::replace(map, Map::new());
            // Move @context out (if any) to keep it at the envelope level.
            let mut envelope = Map::new();
            let mut original_node = original;
            if let Some(ctx) = original_node.remove("@context") {
                envelope.insert("@context".to_string(), ctx);
            }
            let mut graph_items = Vec::with_capacity(1 + siblings.len());
            graph_items.push(Value::Object(original_node));
            graph_items.extend(siblings);
            envelope.insert("@graph".to_string(), Value::Array(graph_items));
            *map = envelope;
        }
        Value::Array(arr) => {
            arr.extend(siblings);
        }
        _ => {
            // Top-level scalar shouldn't occur for a transaction
            // document; the rest of the parser will reject it.
        }
    }
}

// ---------------------------------------------------------------------------
// Two-pass lowering: the first pass needs the *parent's* `@id` to set
// `f:reifiesSubject`, but the parent's id may be assigned (via a fresh
// blank node) only when it's processed. We resolve this with an
// explicit two-pass approach implemented by `lower_with_subject`.
// ---------------------------------------------------------------------------

/// Recursive variant that knows the subject and graph context of the
/// enclosing node.
///
/// Used internally to thread the parent's `@id` (resolved via
/// `ensure_subject_id`) and the inherited graph selector into the
/// inline-annotation lowering.
pub(crate) fn lower_value_with_subject(
    value: &mut Value,
    parent_subject: Option<&str>,
    walk: &WalkCtx<'_>,
    ctx: &mut LowerCtx,
) -> Result<()> {
    match value {
        Value::Array(items) => {
            for item in items {
                lower_value_with_subject(item, parent_subject, walk, ctx)?;
            }
            Ok(())
        }
        Value::Object(map) => lower_object_with_subject(map, parent_subject, walk, ctx),
        _ => Ok(()),
    }
}

/// True when `map` is an envelope wrapper rather than a node-map. The
/// transactor's envelope form is `{"@context": ..., "@graph": [...]}`
/// or just `{"@graph": [...]}` — the wrapper holds no predicates of
/// its own and must not be treated as a node (e.g. by minting an @id
/// for it, which would then become a stray subject).
///
/// `opts` is a known top-level transactor key (stripped before
/// JSON-LD expansion by `strip_opts_for_expansion`); allowing it here
/// keeps documents like `{"@context": ..., "opts": ..., "@graph": [...]}`
/// classified as envelopes so we don't mint a synthetic subject for the
/// outer wrapper.
fn is_envelope(map: &Map<String, Value>) -> bool {
    if !map.contains_key("@graph") {
        return false;
    }
    let graph_is_array = matches!(map.get("@graph"), Some(Value::Array(_)));
    if !graph_is_array {
        return false;
    }
    map.keys()
        .all(|k| matches!(k.as_str(), "@context" | "@graph" | "opts"))
}

/// True when `map` is a JSON-LD value/list/variable wrapper rather
/// than a node-map. These objects describe a literal value, a list,
/// or a variable reference — they must not be treated as nodes (we
/// must not mint `@id` for them or walk their structural keys as
/// predicates).
///
/// Detection mirrors the transactor's expanded-value parser: presence
/// of `@value`, `@language`, `@list`, or `@variable` makes this a
/// value-class object.
fn is_jsonld_value_object(map: &Map<String, Value>) -> bool {
    map.contains_key("@value")
        || map.contains_key("@language")
        || map.contains_key("@list")
        || map.contains_key("@variable")
}

/// Extract a per-node graph selector. Returns the raw IRI / variable
/// string when present, `None` otherwise. Per-node `@graph` differs
/// from envelope `@graph` (which is an array of nodes) — this only
/// fires on the per-node form.
fn extract_node_graph_selector(map: &Map<String, Value>) -> Option<String> {
    let val = map.get("@graph")?;
    match val {
        Value::String(s) => Some(s.clone()),
        Value::Object(g) => g.get("@id").and_then(|x| x.as_str()).map(String::from),
        _ => None,
    }
}

fn lower_object_with_subject(
    map: &mut Map<String, Value>,
    _parent_subject: Option<&str>,
    walk: &WalkCtx<'_>,
    ctx: &mut LowerCtx,
) -> Result<()> {
    // Value/list/variable objects must not be lowered as nodes —
    // they describe a literal, an ordered collection, or a variable
    // reference, none of which are subjects in their own right.
    //
    // The early-return must NOT skip the annotation-keyword scan
    // inside the wrapper's contents: e.g. `@list` items are full
    // node-maps and could carry `@annotation` (the deferred list-
    // occurrence shape), and `@variable` wrappers could be misused
    // to embed a deferred shape. We scan the wrapper once and
    // reject any deferred mention before returning.
    if is_jsonld_value_object(map) {
        scan_nested_annotation_keywords(&Value::Object(map.clone()))?;
        return Ok(());
    }

    // Envelope form: recurse into `@graph` only. Don't mint an @id
    // for the wrapper (it isn't a node). The envelope's `@graph`
    // is the default-graph wrapper, so child nodes inherit the same
    // graph context as the envelope.
    if is_envelope(map) {
        if let Some(graph_val) = map.get_mut("@graph") {
            lower_value_with_subject(graph_val, None, walk, ctx)?;
        }
        return Ok(());
    }

    // Compute the in-effect graph selector for this node and its
    // children. Per-node `@graph: "<iri>"` overrides; otherwise we
    // inherit from the walker.
    let node_graph = extract_node_graph_selector(map);
    let node_graph_ref = node_graph.as_deref();
    let effective_graph = node_graph_ref.or(walk.graph);
    let child_walk = WalkCtx {
        json_ld: walk.json_ld,
        graph: effective_graph,
    };

    // 1. Honor `@reifies` on this node (rejected in v1 — see above).
    if map.contains_key(REIFIES_KEY) {
        let val = map.remove(REIFIES_KEY).unwrap();
        lower_reifies_block(map, val, ctx)?;
    }

    // 2. Mint or read the @id so children can reference us.
    let my_id = ensure_subject_id(map, walk, ctx);

    // 3. Walk predicate-value pairs. Skip JSON-LD keywords plus their
    //    context aliases (e.g. `"id": "@id"`) so we don't treat the
    //    aliased subject reference as a regular predicate.
    let id_alias = walk.json_ld.id_key.as_str();
    let type_alias = walk.json_ld.type_key.as_str();
    let keys: Vec<String> = map.keys().cloned().collect();
    for key in keys {
        if key == "@id" || key == "@context" || key == "@graph" || key == "@type" {
            continue;
        }
        if key == id_alias || key == type_alias {
            continue;
        }
        if is_annotation_key(&key) {
            return Err(TransactError::UnsupportedFeature(format!(
                "{key} as a top-level node property (without an enclosing predicate) \
                 is the deferred unasserted-reifier shape (v1)"
            )));
        }

        let predicate = key.clone();
        let value = map.get_mut(&key).expect("key collected from this map");
        intercept_annotations_for_predicate(&my_id, &predicate, value, &child_walk, ctx)?;

        // Recurse into the rewritten value to catch nested forms.
        if let Some(v) = map.get_mut(&key) {
            lower_value_with_subject(v, Some(&my_id), &child_walk, ctx)?;
        }
    }
    Ok(())
}

/// Variant of `intercept_annotations_in_value` that has the parent
/// subject available, so `f:reifiesSubject` can be set correctly.
fn intercept_annotations_for_predicate(
    parent_subject: &str,
    predicate: &str,
    value: &mut Value,
    walk: &WalkCtx<'_>,
    ctx: &mut LowerCtx,
) -> Result<()> {
    match value {
        Value::Array(items) => {
            for item in items {
                intercept_annotations_for_predicate(parent_subject, predicate, item, walk, ctx)?;
            }
            Ok(())
        }
        Value::Object(map) => {
            // Detect literal-value-with-annotation deferred shape.
            let has_value = map.contains_key("@value") || map.contains_key("@language");
            let has_ann = map.contains_key(ANNOTATION_KEY) || map.contains_key(EDGE_KEY);
            if has_value && has_ann {
                return Err(TransactError::UnsupportedFeature(
                    "literal-valued edge annotations are deferred to v2; \
                     attach annotations only to object IRIs (with @id), not to typed literals"
                        .to_string(),
                ));
            }
            if map.contains_key(ANNOTATION_KEY) && map.contains_key(EDGE_KEY) {
                return Err(TransactError::Parse(
                    "edge annotation: cannot specify both @annotation and @edge on the same object"
                        .to_string(),
                ));
            }
            let ann_block = map.remove(ANNOTATION_KEY).or_else(|| map.remove(EDGE_KEY));
            let Some(ann_block) = ann_block else {
                return Ok(());
            };

            // Skip lowering for value/list/variable objects. (The
            // literal-value rejection above already caught the
            // `@annotation` + `@value` combination; this is a
            // belt-and-braces guard.)
            if is_jsonld_value_object(map) {
                return Err(TransactError::UnsupportedFeature(
                    "edge annotations are not supported on @list / @variable wrappers in v1"
                        .to_string(),
                ));
            }

            let object_id = ensure_subject_id(map, walk, ctx);
            if reifies_iris::ALL.contains(&predicate) {
                return Err(TransactError::UnsupportedFeature(format!(
                    "'{predicate}' is a system-controlled predicate; use @annotation instead"
                )));
            }
            let synth = build_annotation_sibling(
                Some(parent_subject),
                predicate,
                &object_id,
                ann_block,
                walk.graph,
                ctx,
            )?;
            ctx.siblings.push(synth);
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Run the full pipeline (scan + two-pass lowering) on `doc` and
    /// return the rewritten document plus any minted blank-node count.
    fn lower(mut doc: Value) -> Result<Value> {
        lower_edge_annotations(&mut doc)?;
        Ok(doc)
    }

    #[test]
    fn lowers_inline_annotation_to_sibling_with_reifies_predicates() {
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "ex:role": "Engineer" }
            }
        });
        let result = lower(doc).unwrap();
        // Must wrap in @graph since we added a sibling.
        let graph = result
            .get("@graph")
            .and_then(|g| g.as_array())
            .expect("envelope @graph form");
        assert_eq!(graph.len(), 2, "original + 1 annotation sibling");

        // Original node: no @annotation key remains.
        let original = &graph[0];
        let nested = original.get("ex:worksFor").unwrap();
        assert!(
            !nested.as_object().unwrap().contains_key("@annotation"),
            "@annotation must be removed from the original payload"
        );

        // Sibling: has f:reifiesSubject/Predicate/Object pointing at the edge.
        let sibling = &graph[1];
        assert_eq!(
            sibling.get(reifies_iris::SUBJECT).unwrap(),
            &json!({"@id": "ex:alice"})
        );
        assert_eq!(
            sibling.get(reifies_iris::PREDICATE).unwrap(),
            &json!({"@id": "ex:worksFor"})
        );
        assert_eq!(
            sibling.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@id": "ex:acme"})
        );
        // Body fact preserved.
        assert_eq!(sibling.get("ex:role").unwrap(), &json!("Engineer"));
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
        let sibling = &result.get("@graph").unwrap().as_array().unwrap()[1];
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
        let sibling = &result.get("@graph").unwrap().as_array().unwrap()[1];
        assert_eq!(
            sibling.get("@id").unwrap(),
            &json!("ex:employment-1"),
            "explicit @id on the annotation must be preserved"
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
        // Same shape as the @annotation case.
        assert_eq!(result.get("@graph").unwrap().as_array().unwrap().len(), 2);
    }

    #[test]
    fn rejects_literal_value_with_annotation() {
        let doc = json!({
            "@id": "ex:alice",
            "ex:joinedAt": {
                "@value": "2024-01-01",
                "@type": "xsd:date",
                "@annotation": { "ex:source": "ex:hr-system" }
            }
        });
        let err = lower(doc).unwrap_err();
        assert!(err.to_string().contains("literal-valued"));
    }

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
        let err = lower(doc).unwrap_err();
        // The deeper @annotation hit the body-keys check or the
        // recursion's own check.
        let msg = err.to_string();
        assert!(
            msg.contains("annotation") && msg.contains("deferred"),
            "expected deferred annotation-of-annotation message, got: {msg}"
        );
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

    #[test]
    fn rejects_reifies_on_insert() {
        let doc = json!({
            "@id": "ex:employment-1",
            "ex:role": "Engineer",
            "@reifies": {
                "@id": "ex:alice",
                "ex:worksFor": { "@id": "ex:acme" }
            }
        });
        let err = lower(doc).unwrap_err();
        assert!(
            err.to_string().contains("@reifies on inserts"),
            "expected @reifies-on-insert deferral message, got: {err}"
        );
    }

    #[test]
    fn rejects_compact_reifies_iri_via_context() {
        // Compact form via `@context` must be rejected with the same
        // firewall message as the full IRI form. Without context-aware
        // resolution this would slip through and produce user-authored
        // system facts.
        let doc = json!({
            "@context": { "f": "https://ns.flur.ee/db#" },
            "@id": "ex:alice",
            "f:reifiesSubject": { "@id": "ex:bob" }
        });
        let err = lower(doc).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("system-controlled") && msg.contains("reifiesSubject"),
            "expected reserved-predicate firewall error, got: {msg}"
        );
    }

    #[test]
    fn typed_literal_value_object_passes_through_untouched() {
        // Regression: previously the lowering walker minted an @id for
        // `{"@value": "...", "@type": "..."}` and treated `@value` as a
        // predicate. Verify a typed-literal value-object is preserved
        // exactly, with no @id minted and no annotation siblings.
        let doc = json!({
            "@context": { "ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#" },
            "@id": "ex:alice",
            "ex:joinedAt": { "@value": "2024-01-01", "@type": "xsd:date" }
        });
        let original = doc.clone();
        let result = lower(doc).unwrap();
        assert_eq!(
            result, original,
            "value object must not be lowered as a node"
        );
    }

    #[test]
    fn rejects_annotation_nested_inside_list_item() {
        // List-occurrence annotations are deferred (v1). A `@list`
        // wrapper containing an `@annotation` inside one of its items
        // must error rather than silently slip past the early-return
        // for value-class objects.
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
        let err = lower(doc).unwrap_err();
        let msg = err.to_string();
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
        // @variable should be a leaf reference — embedding a deferred
        // shape inside it must error.
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

    #[test]
    fn list_object_passes_through_untouched() {
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice",
            "ex:nicknames": { "@list": ["A", "B", "C"] }
        });
        let original = doc.clone();
        let result = lower(doc).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn variable_object_passes_through_untouched() {
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice",
            "ex:status": { "@variable": "?status" }
        });
        let original = doc.clone();
        let result = lower(doc).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn annotation_in_named_graph_emits_reifies_graph() {
        // When the parent node is in a named graph (per-node @graph
        // selector), the synthetic annotation sibling must carry both
        // `f:reifiesGraph` (so the decoder pins the right graph) and
        // its own `@graph` (so the annotation flakes land in the same
        // named graph as the reified edge).
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
        let graph = result
            .get("@graph")
            .and_then(|g| g.as_array())
            .expect("envelope @graph form");
        let sibling = &graph[1];

        assert_eq!(
            sibling.get(reifies_iris::GRAPH).unwrap(),
            &json!({"@id": "ex:hr-graph"}),
            "f:reifiesGraph should pin the reified edge's named graph"
        );
        assert_eq!(
            sibling.get("@graph").unwrap(),
            &json!("ex:hr-graph"),
            "annotation sibling should land in the same named graph as its edge"
        );
    }

    #[test]
    fn annotation_in_default_graph_omits_reifies_graph() {
        // Default-graph edges encode "default" as the *absence* of
        // `f:reifiesGraph` — matching the bundle validator's
        // "at most one" rule and `EdgeKey::from_reifies_facts`'s
        // None-means-default semantics.
        let doc = json!({
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "ex:role": "Engineer" }
            }
        });
        let result = lower(doc).unwrap();
        let sibling = &result.get("@graph").unwrap().as_array().unwrap()[1];
        assert!(
            sibling.get(reifies_iris::GRAPH).is_none(),
            "f:reifiesGraph must be absent for default-graph edges"
        );
        assert!(
            sibling.get("@graph").is_none(),
            "annotation sibling must not pin a named graph for default-graph edges"
        );
    }

    #[test]
    fn no_annotation_doc_passes_through_unchanged() {
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice",
            "ex:name": "Alice"
        });
        let original = doc.clone();
        let result = lower(doc).unwrap();
        // Lowering only mints a blank node @id when needed; this doc
        // has explicit @id everywhere, so it should be untouched.
        assert_eq!(result, original);
    }
}
