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
//! The accepted insert shape is the **inline form** (`@annotation` /
//! `@edge` on the *object* node of a predicate):
//!    ```json
//!    { "@id": "ex:alice",
//!      "ex:worksFor": {
//!        "@id": "ex:acme",
//!        "@annotation": { "ex:role": "Engineer" }
//!      }
//!    }
//!    ```
//! The annotation reifies the edge `(ex:alice, ex:worksFor, ex:acme)`.
//! The annotation subject is the `@id` inside the `@annotation` block,
//! or a freshly-minted blank node when absent. Annotated literal objects
//! (plain, typed, and language-tagged) are supported, provided they
//! carry an explicit `@type` / `@language` — `@context` coercion of an
//! annotated literal is rejected (see
//! [`reject_context_coercion_on_annotated_literal`]) so the reified
//! `f:reifiesObject` bundle can't silently diverge from the base flake.
//!
//! The **annotation-rooted form** (`@reifies` on an enclosing node) is
//! *not* an insert surface in v1: user-authored `@reifies` on a write is
//! rejected as deferred by [`lower_reifies_block`]. `@reifies` is a
//! query-side construct; the only writer that produces the
//! `@reifies`-rooted shape is the internal delete-by-id rewrite.
//!
//! Strict deferred-shape rejection (per the contract in
//! `docs/concepts/edge-annotations.md` "Current limits"):
//!
//! - User-authored `@reifies` on the insert side → error.
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
use fluree_graph_json_ld::{expand_iri, parse_context, ParsedContext, TypeValue};
use fluree_vocab::reifies_iris;
use serde_json::{json, Map, Value};

const ANNOTATION_KEY: &str = "@annotation";
const EDGE_KEY: &str = "@edge";
const REIFIES_KEY: &str = "@reifies";

/// True when `key` is an annotation/edge keyword on the *object* side.
fn is_annotation_key(key: &str) -> bool {
    key == ANNOTATION_KEY || key == EDGE_KEY
}

/// Classification of the object position on a reified base edge. Drives
/// every site that produces an `f:reifies*` bundle (insert sibling,
/// delete template, delete-by-selector WHERE), so that all writers mint
/// the same `EdgeKey` for the same logical edge.
///
/// Constructed via [`classify_reified_object`] after `@annotation` /
/// `@edge` have been removed from the object map.
#[derive(Debug, Clone)]
pub(crate) enum ReifiedObjectShape {
    /// Object is an IRI or blank-node reference. The `String` is the
    /// IRI / blank-node identifier (possibly minted upstream).
    Iri(String),
    /// Object is a literal. Fields capture the canonicalized JSON-LD
    /// value object: only `@value` (always), `@type` (when explicit),
    /// and `@language` (when explicit) survive — every other key is
    /// rejected upstream.
    Literal {
        /// Canonical value-object JSON: `{"@value": ..., ?"@type": ...,
        /// ?"@language": ...}`. Suitable for direct insertion under
        /// `f:reifiesObject`. Callers needing the datatype string for
        /// delete-WHERE generation can extract it via
        /// `value.get("@type")`.
        value: Value,
        /// `@language` payload if explicit. Drives `f:reifiesLang`
        /// emission — required so `EdgeKey::from_reifies_facts` decodes
        /// to the same `lang` the base flake carries via `flake.m.lang`.
        language: Option<String>,
    },
}

/// Reject the deferred JSON-LD wrapper shapes that can't carry an edge
/// annotation in v1: `@list`, `@set`, `@reverse`, `@variable`. Called
/// before either classifier branch so wrapper rejection is uniform.
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

/// Reject annotated literal value-objects whose effective datatype or
/// language is determined by JSON-LD context coercion rather than by
/// explicit `@type` / `@language` on the value object.
///
/// Why: the JSON-LD expander applies a predicate's term-coerced
/// `@type` and the active default `@language` to bare `{"@value": ...}`
/// objects, so the base flake's `EdgeKey` ends up with the coerced
/// datatype / language. The reified bundle synthesized by this module,
/// however, only copies *explicit* `@type` / `@language` from the
/// value object. Without rejection, the two EdgeKeys would silently
/// diverge — exactly the failure mode the gate tests in
/// `it_edge_annotations.rs` (`edgekey_roundtrip_*`) guard against.
///
/// Resolution path for users: write `@type` and/or `@language`
/// explicitly on the annotated value object. The non-annotated form
/// of the same triple continues to use context coercion normally; the
/// stricter rule applies only when `@annotation` / `@edge` is present.
///
/// Implementation: this helper is called *after* `@annotation` is
/// stripped from the map but *before* `classify_reified_object`. It
/// has no effect on the Ref / `@id` branch.
pub(crate) fn reject_context_coercion_on_annotated_literal(
    map: &Map<String, Value>,
    predicate: &str,
    context: &ParsedContext,
) -> Result<()> {
    let entry = context.get(predicate);

    // Datatype coercion: any predicate-level `@type` (including `@id`,
    // `@vocab`, `@json`) without an explicit `@type` on the value
    // object means the expander applies the coercion, while the
    // synthesized `f:reifiesObject` would carry no such datatype.
    if let Some(e) = entry {
        if let Some(type_) = e.type_.as_ref() {
            if !map.contains_key("@type") {
                let coerced = match type_ {
                    TypeValue::Iri(t) => t.clone(),
                    TypeValue::Id => "@id".to_string(),
                    TypeValue::Vocab => "@vocab".to_string(),
                    TypeValue::Json => "@json".to_string(),
                };
                return Err(TransactError::Parse(format!(
                    "annotated literal on '{predicate}' relies on @context term coercion \
                     (@type='{coerced}'); annotated literals must carry an explicit @type so the \
                     reified f:reifiesObject bundle matches the base edge's EdgeKey. \
                     Add @type to the value object."
                )));
            }
        }
    }

    // Language coercion: the JSON-LD value-object expander in
    // `parse_value_object` reads `context.language` (the document
    // default) regardless of any per-term `@language` setting, then
    // applies it whenever no `@type` is present on the value object.
    // So the guard mirrors that behavior — checking only the document
    // default — even if a per-term entry technically declares (or
    // clears) a language. Without this, a value-object expander that
    // tags the base flake with the default language would diverge
    // from a reified bundle that thinks the per-term clear suppressed
    // the tag.
    //
    // (Bare-scalar strings under a per-term language entry DO get
    // term-language tags at expansion time, but the value-object
    // form is the only shape edge annotations accept.)
    let value_is_string = matches!(map.get("@value"), Some(Value::String(_)));
    if value_is_string && !map.contains_key("@language") {
        if let Some(lang) = context.language.as_deref() {
            return Err(TransactError::Parse(format!(
                "annotated string literal on '{predicate}' relies on @context language coercion \
                 (@language='{lang}'); annotated literals must carry an explicit @language so \
                 the reified f:reifiesLang flake matches the base edge's EdgeKey. \
                 Add @language to the value object."
            )));
        }
    }

    Ok(())
}

/// Classify an object-position map (with `@annotation`/`@edge` already
/// stripped) into either a reference or a literal value-object.
///
/// Rules:
/// - `{"@id": "..."}` → `Iri`. (Aliased `@id` keys, e.g. `"id"` under a
///   custom JSON-LD context, are NOT resolved here — the caller must
///   resolve aliasing via `ensure_subject_id` before invoking this
///   function for the Ref path.)
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
            Some(Value::String(s)) => Some(s.clone()),
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
/// `f:reifiesLang` companion. Mirrors [`EdgeKey::to_reifies_facts`]
/// so writers and the binary decoder agree on bundle shape.
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
    }
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
    /// When `true`, an empty `@annotation: {}` block mints a fresh
    /// property-less annotation subject (LPG mode — Cypher
    /// relationships retain identity without properties). When
    /// `false` (RDF default), an empty block is a no-op: no
    /// annotation subject is minted, no attachment row is written,
    /// and inserts remain idempotent at the `(s, p, o)` level.
    /// See `docs/concepts/edge-annotations.md` "Empty annotation blocks".
    lpg_mode: bool,
}

impl LowerCtx {
    fn new(lpg_mode: bool) -> Self {
        Self {
            next_anon_id: 0,
            siblings: Vec::new(),
            lpg_mode,
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
    // Test / external callers that don't carry transaction options
    // default to RDF mode (empty `@annotation: {}` is a no-op).
    lower_edge_annotations_after_firewall(doc, &top_ctx, false)
}

/// Same as [`lower_edge_annotations`] minus the firewall scan.
///
/// Use this when the caller has already run the firewall on the
/// original (pre-rewritten) document — e.g. when chaining the
/// `delete`-clause pre-pass before the standard lowering. Both
/// passes synthesize `f:reifies*` IRIs internally; running the
/// firewall a second time would falsely flag those.
///
/// `lpg_mode` controls the empty-block contract: `false` (RDF
/// default) makes `@annotation: {}` a no-op; `true` mints a fresh
/// property-less annotation subject (Cypher relationship identity).
pub fn lower_edge_annotations_after_firewall(
    doc: &mut Value,
    top_ctx: &ParsedContext,
    lpg_mode: bool,
) -> Result<()> {
    let mut ctx = LowerCtx::new(lpg_mode);
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

/// Cheap read-only check for whether `doc` contains any `@annotation` /
/// `@edge` / `@reifies` block that the lowering passes would rewrite.
///
/// Lets the transactor skip the payload clone and the mutating lowering
/// walks for the common non-annotated transaction. The firewall is a
/// separate concern and must still run unconditionally — it guards against
/// user-authored `f:reifies*` IRIs, which this scan does not look for.
pub fn document_has_annotation_keys(doc: &Value) -> bool {
    match doc {
        Value::Object(map) => map.iter().any(|(k, v)| {
            is_annotation_key(k) || k == REIFIES_KEY || document_has_annotation_keys(v)
        }),
        Value::Array(items) => items.iter().any(document_has_annotation_keys),
        _ => false,
    }
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
    // Seed the variable counter past any user-authored
    // `?_fluree_del_ann_N` occurrences in the original `where`,
    // `delete`, `insert`, `upsert`, and `values` clauses. A naive
    // `next_var = 0` would collide with a user-provided variable of
    // the same name and silently mis-join the selector retract
    // against the wrong bindings. This scan MUST run before the
    // `obj.remove` calls below — otherwise it sees an empty doc and
    // returns 0 regardless of user state.
    let mut next_var: u32 = next_synth_var_index(obj);

    // Take ownership of `delete` (and the `where` clause if any) so
    // we can splice both. Re-insert at the end. `obj.get_mut`
    // borrows can't compose with subsequent inserts, so the
    // remove/walk/insert dance keeps the borrow checker happy.
    let mut delete_val = obj.remove("delete").expect("checked above");
    let where_val = obj.remove("where");

    let mut new_templates: Vec<Value> = Vec::new();
    let mut new_where_patterns: Vec<Value> = Vec::new();
    // Top-level graph context: the transactor's UPDATE doc has no
    // envelope `@graph` (that's the insert/upsert envelope form),
    // so we start from `None` and let per-node `@graph: "<iri>"`
    // selectors inside the delete clause set the inherited graph as
    // we recurse. The `@graph` value gets threaded into the
    // synthesized retract template so the retract flake's
    // `g = Some(graph_sid)` matches the original assertion's flake
    // identity.
    let mut sink = DeleteAnnotationSink {
        out_templates: &mut new_templates,
        out_where: &mut new_where_patterns,
        next_var: &mut next_var,
    };
    walk_delete_for_annotations(&mut delete_val, &top_ctx, None, &mut sink)?;

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

/// Internal variable name prefix used by the selector-form retract
/// rewrite. Kept in one place so the scanner and the minter agree.
const SYNTH_VAR_PREFIX: &str = "?_fluree_del_ann_";

/// Walk the UPDATE document's user-visible clauses (`where`, `delete`,
/// `insert`, `upsert`, `values`) and return the smallest counter
/// value that will produce a fresh `?_fluree_del_ann_N` name. A
/// user-authored pattern that happens to reference our internal
/// prefix would otherwise collide with the minter and mis-bind the
/// selector retract against the user's variable.
fn next_synth_var_index(obj: &Map<String, Value>) -> u32 {
    let mut max_seen: Option<u32> = None;
    for clause in ["where", "delete", "insert", "upsert", "values"] {
        if let Some(v) = obj.get(clause) {
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

/// Accumulator state threaded through the delete-clause annotation
/// walker, predicate-value lifter, and template builder. Bundles the
/// three accumulating mutables (template list, WHERE pattern list,
/// fresh-var counter) so the recursion can pass a single `&mut`
/// borrow instead of three positional args.
///
/// `ParsedContext` and the per-subtree `inherited_graph` stay as
/// recursion-local args — they're swapped per-node when a child
/// brings its own `@context` or `@graph` selector, so packing them
/// into the sink would force every recursive call to construct a
/// new sink just to override one field.
struct DeleteAnnotationSink<'a> {
    out_templates: &'a mut Vec<Value>,
    out_where: &'a mut Vec<Value>,
    next_var: &'a mut u32,
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
    sink: &mut DeleteAnnotationSink<'_>,
) -> Result<()> {
    match val {
        Value::Array(items) => {
            for item in items {
                walk_delete_for_annotations(item, ctx, inherited_graph, sink)?;
            }
            Ok(())
        }
        Value::Object(map) => {
            // Merge any per-node `@context` into the parent context.
            // Symmetric with the insert walker — without this, a
            // node-local term coercion would apply at expansion time
            // but be invisible to
            // `reject_context_coercion_on_annotated_literal`, masking
            // an EdgeKey mismatch.
            let local_merged: Option<ParsedContext> = if let Some(local_ctx) = map.get("@context") {
                Some(
                    fluree_graph_json_ld::parse_context_with_base(ctx, local_ctx).map_err(|e| {
                        TransactError::Parse(format!(
                            "failed to parse nested @context during delete-clause \
                                     annotation lowering: {e}"
                        ))
                    })?,
                )
            } else {
                None
            };
            let effective_ctx: &ParsedContext = local_merged.as_ref().unwrap_or(ctx);

            // Capture the parent's @id for `f:reifiesSubject`. Honor
            // context-aliased `@id` keys (e.g. `id: ...` when context
            // says `"id": "@id"`).
            let parent_id = map
                .get("@id")
                .and_then(Value::as_str)
                .or_else(|| {
                    let alias = effective_ctx.id_key.as_str();
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
                .filter(|k| !k.starts_with('@') && *k != effective_ctx.id_key.as_str())
                .cloned()
                .collect();

            let mut keys_to_remove: Vec<String> = Vec::new();
            for key in predicate_keys {
                let value = map.get_mut(&key).expect("collected key");
                lift_annotations_under_predicate(
                    parent_id.as_deref(),
                    &key,
                    value,
                    effective_ctx,
                    effective_graph,
                    sink,
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
                    walk_delete_for_annotations(v, effective_ctx, effective_graph, sink)?;
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
    sink: &mut DeleteAnnotationSink<'_>,
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
                    sink,
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

            // Per-object `@graph` selector on the *base edge object*
            // overrides the inherited graph. Mirrors the
            // assertion-side walker's per-node graph extraction.
            // Captured BEFORE shape classification so a Ref object's
            // graph selector is preserved (literal value-objects
            // never carry `@graph` — the classifier whitelist
            // rejects it as a stray key).
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

            // Wrapper rejection runs in both branches.
            reject_deferred_object_wrappers(map)?;

            // Classify the post-strip base-edge object. The value-
            // object form drives a literal-shaped `f:reifiesObject`
            // payload (plus `f:reifiesLang` when language-tagged); the
            // node-map form resolves the @id (with context aliasing)
            // and produces a Ref shape. Strip the per-node @graph
            // selector before classifying so it doesn't trip the
            // identifier-typed value-object stray-key check (it lives
            // on the parent node, not on a value-object).
            map.remove("@graph");
            let object_shape = if map.contains_key("@value") || map.contains_key("@language") {
                // Mirror the insert-path guard: annotated literals
                // must NOT rely on @context coercion, since the
                // synthesized f:reifies* bundle would otherwise decode
                // to an EdgeKey that doesn't match the original
                // assertion's. A by-id delete that doesn't match
                // becomes a silent no-op.
                reject_context_coercion_on_annotated_literal(map, predicate, ctx)?;
                classify_reified_object(map)?
            } else {
                let id_alias = ctx.id_key.as_str();
                let object_id = map
                    .get("@id")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        if id_alias != "@id" {
                            map.get(id_alias).and_then(Value::as_str)
                        } else {
                            None
                        }
                    })
                    .map(String::from)
                    .ok_or_else(|| {
                        TransactError::Parse(
                            "delete-clause @annotation requires the parent object node to carry \
                             an explicit @id (the object of the reified edge)"
                                .to_string(),
                        )
                    })?;
                ReifiedObjectShape::Iri(object_id)
            };

            build_annotation_delete(
                parent_subject,
                predicate,
                &object_shape,
                ann_block,
                ctx,
                effective_graph,
                sink,
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
    object: &ReifiedObjectShape,
    ann_block: Value,
    ctx: &ParsedContext,
    graph_iri: Option<&str>,
    sink: &mut DeleteAnnotationSink<'_>,
) -> Result<()> {
    let Value::Object(mut ann_map) = ann_block else {
        return Err(TransactError::Parse(
            "@annotation value must be a JSON object".to_string(),
        ));
    };

    // Compute the `f:reifiesObject` JSON payload plus optional
    // `f:reifiesLang` companion once. Both the selector WHERE pattern
    // and the delete template emit identical shape so the synthesized
    // retract bundle decodes to the same EdgeKey as the original
    // assertion (the contract the gate tests pin in
    // `it_edge_annotations.rs`).
    let (object_payload, lang_payload) = emit_reifies_object_payload(object);
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
        let var = format!("{SYNTH_VAR_PREFIX}{}", *sink.next_var);
        *sink.next_var += 1;

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
        where_node.insert(reifies_iris::OBJECT.to_string(), object_payload.clone());
        if let Some(lang) = &lang_payload {
            // Emit `f:reifiesLang` as an additional WHERE constraint
            // so the selector form binds only annotations whose
            // language tag matches — same lexical string across
            // different languages must not collide.
            where_node.insert(reifies_iris::LANG.to_string(), json!(lang));
        }
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
            sink.out_where
                .push(json!(["graph", expanded_graph, Value::Object(where_node),]));
        } else {
            sink.out_where.push(Value::Object(where_node));
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
    template.insert(reifies_iris::OBJECT.to_string(), object_payload);
    if let Some(lang) = lang_payload {
        template.insert(reifies_iris::LANG.to_string(), json!(lang));
    }
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
    sink.out_templates.push(Value::Object(template));
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
    object: &ReifiedObjectShape,
    ann_block: Value,
    base_graph: Option<&str>,
    ctx: &mut LowerCtx,
) -> Result<Option<Value>> {
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

    // Empty-body contract (per `docs/concepts/edge-annotations.md`
    // "Empty annotation blocks"). `@context` is purely structural and
    // doesn't count as a property; everything else does — including
    // an explicit `@id`, which signals the user wants annotation
    // identity. So the no-op case is exactly `{}` (or `{"@context": ...}`).
    //
    // - RDF mode (default): no annotation subject is minted, no
    //   attachment row is written. Inserts remain idempotent at the
    //   `(s, p, o)` level.
    // - LPG mode (`opts.lpgEdgeLifecycle: true`): mint a fresh
    //   property-less annotation subject so the relationship retains
    //   identity (matches Cypher).
    let body_is_empty = ann_map.iter().all(|(k, _)| k == "@context");
    if body_is_empty && !ctx.lpg_mode {
        return Ok(None);
    }

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

    // f:reifiesObject and (optional) f:reifiesLang. For a literal
    // object, the value payload is the canonical value-object built by
    // the classifier; `EdgeKey::from_reifies_facts` derives `lang` from
    // a separate `f:reifiesLang` flake, so language-tagged literals
    // MUST emit it explicitly — otherwise the decoded EdgeKey would
    // not match the writer's base-edge EdgeKey.
    let (object_payload, lang_payload) = emit_reifies_object_payload(object);
    ann_map.insert(reifies_iris::OBJECT.to_string(), object_payload);
    if let Some(lang) = lang_payload {
        ann_map.insert(reifies_iris::LANG.to_string(), json!(lang));
    }

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

    Ok(Some(Value::Object(ann_map)))
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

/// True when `map` is a transaction wrapper (UPDATE / explicit
/// Insert+opts shape) rather than a node-map. The wrapper carries
/// clause keys (`where`, `delete`, `insert`, `upsert`, `values`,
/// `opts`, `ledger`) plus the optional `@context`, but no predicates
/// of its own — so we must not mint an `@id` for it. Each clause's
/// value is itself a fresh top-level payload that we recurse into.
///
/// The check fires when at least one clause key is present and
/// every key is either a clause key, `@context`, or `@id`/`@type`
/// (rare in transaction docs but tolerated). The `@id`/`@type`
/// tolerance lets through documents that have already been minted
/// by an earlier pass.
fn is_transaction_wrapper(map: &Map<String, Value>) -> bool {
    const CLAUSE_KEYS: &[&str] = &["where", "delete", "insert", "upsert", "values"];
    const WRAPPER_KEYS: &[&str] = &[
        "where",
        "delete",
        "insert",
        "upsert",
        "values",
        "opts",
        "ledger",
        "@context",
        "from",
        "fromNamed",
    ];
    let has_clause = CLAUSE_KEYS.iter().any(|k| map.contains_key(*k));
    if !has_clause {
        return false;
    }
    map.keys().all(|k| WRAPPER_KEYS.contains(&k.as_str()))
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

    // Transaction-wrapper form: an UPDATE-style document
    // `{"where": ..., "delete": ..., "insert": ..., "upsert": ...,
    // "values": ..., "opts": ..., "ledger": ..., "@context": ...}`.
    // The wrapper itself is not a node — it carries no predicates of
    // its own — so we must not mint an `@id` for it. Recurse into the
    // value of each clause that can carry annotations as if it were
    // a fresh top-level document. `where`, `values`, `opts`, and
    // `ledger` are query/control clauses (no annotations to lower at
    // this layer) so we skip them.
    if is_transaction_wrapper(map) {
        for clause in ["insert", "delete", "upsert"] {
            if let Some(clause_val) = map.get_mut(clause) {
                lower_value_with_subject(clause_val, None, walk, ctx)?;
            }
        }
        return Ok(());
    }

    // Compute the in-effect graph selector for this node and its
    // children. Per-node `@graph: "<iri>"` overrides; otherwise we
    // inherit from the walker.
    let node_graph = extract_node_graph_selector(map);
    let node_graph_ref = node_graph.as_deref();
    let effective_graph = node_graph_ref.or(walk.graph);

    // Merge any per-node `@context` into the walker's parent context.
    // Without this, a node-local term definition (e.g. `"joinedAt":
    // { "@id": "...", "@type": "xsd:date" }`) would apply at JSON-LD
    // expansion time but be invisible to
    // `reject_context_coercion_on_annotated_literal`, allowing the
    // base flake's `EdgeKey` to diverge from the synthesized
    // `f:reifies*` bundle. Mirrors the firewall scanner's context
    // merge logic.
    let local_merged: Option<ParsedContext> = if let Some(local_ctx) = map.get("@context") {
        Some(
            fluree_graph_json_ld::parse_context_with_base(walk.json_ld, local_ctx).map_err(
                |e| {
                    TransactError::Parse(format!(
                        "failed to parse nested @context during edge-annotation lowering: {e}"
                    ))
                },
            )?,
        )
    } else {
        None
    };
    let effective_json_ld: &ParsedContext = local_merged.as_ref().unwrap_or(walk.json_ld);

    let child_walk = WalkCtx {
        json_ld: effective_json_ld,
        graph: effective_graph,
    };

    // 1. Honor `@reifies` on this node (rejected in v1 — see above).
    //    Subject minting must use the merged context so a node-local
    //    `@id` alias is recognized.
    if map.contains_key(REIFIES_KEY) {
        let val = map.remove(REIFIES_KEY).unwrap();
        // `@reifies` is one of the cases that requires a subject id; the
        // lower function reads `map`'s `@id` directly, but the mint must
        // run first so the value is present.
        let _ = ensure_subject_id(map, &child_walk, ctx);
        lower_reifies_block(map, val, ctx)?;
    }

    // 2. Walk predicate-value pairs. Skip JSON-LD keywords plus their
    //    context aliases (e.g. `"id": "@id"`) so we don't treat the
    //    aliased subject reference as a regular predicate. Use the
    //    merged context for alias resolution — a node-local `"id":
    //    "@id"` override must be honored or the surrounding mints /
    //    intercepts would treat the aliased id as a regular predicate.
    //
    // **Lazy @id minting.** Only mint `@id` for this node when an
    // annotation is actually being intercepted on one of its
    // predicates. Minting unconditionally would:
    //   (a) corrupt non-node objects that flow through this path
    //       (e.g. txn-meta values like `ex:invalid: {"foo": "bar"}`),
    //       making them look like valid JSON-LD nodes and bypassing
    //       downstream validation; and
    //   (b) introduce stray subjects that the rest of the parser
    //       would expand into spurious flakes.
    //
    // The original eager-mint behavior caused regressions in
    // `it_select_star_novelty_retract::expansion_applies_novelty_retractions`,
    // `it_transact::object_var_test`, and the `it_txn_meta::*`
    // negative-validation tests when the lowering pass ran on
    // ordinary (non-annotation) transactions.
    let id_alias = effective_json_ld.id_key.as_str();
    let type_alias = effective_json_ld.type_key.as_str();
    let keys: Vec<String> = map.keys().cloned().collect();
    let mut minted_id: Option<String> = read_existing_subject_id(map, &child_walk);
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

        // Mint the parent @id lazily — only if this predicate's value
        // actually carries an annotation we'll need to anchor. Use an
        // immutable peek first so the borrow of `map` is released
        // before we hand it back mutably to `ensure_subject_id`.
        if minted_id.is_none() {
            let needs_mint = map
                .get(&key)
                .map(value_subtree_carries_annotation)
                .unwrap_or(false);
            if needs_mint {
                minted_id = Some(ensure_subject_id(map, &child_walk, ctx));
            }
        }

        let value = map.get_mut(&key).expect("key collected from this map");

        // `intercept_annotations_for_predicate` only consults
        // `parent_subject` when it finds an annotation block; passing a
        // synthetic placeholder when no annotation is present is safe
        // because the function returns early without using it.
        let placeholder = String::new();
        let parent = minted_id.as_deref().unwrap_or(&placeholder);
        intercept_annotations_for_predicate(parent, &predicate, value, &child_walk, ctx)?;

        // Recurse into the rewritten value to catch nested forms.
        // Reborrow because intercept_* may have mutated the map.
        if let Some(v) = map.get_mut(&key) {
            let parent_ref = minted_id.as_deref();
            lower_value_with_subject(v, parent_ref, &child_walk, ctx)?;
        }
    }
    Ok(())
}

/// Read an already-present subject id from a node-map, honoring the
/// JSON-LD `@id` alias declared in the active context. Returns `None`
/// when neither key is present; in that case the caller decides whether
/// minting is required (see lazy-mint logic above).
fn read_existing_subject_id(map: &Map<String, Value>, walk: &WalkCtx<'_>) -> Option<String> {
    if let Some(Value::String(s)) = map.get("@id") {
        return Some(s.clone());
    }
    let id_alias = walk.json_ld.id_key.as_str();
    if id_alias != "@id" {
        if let Some(Value::String(s)) = map.get(id_alias) {
            return Some(s.clone());
        }
    }
    None
}

/// True iff `value`'s structural subtree contains an `@annotation`,
/// `@edge`, or `@reifies` key. Used to gate the lazy `@id` mint so
/// we only stamp an id on parent nodes that actually need to anchor
/// a `f:reifiesSubject` pointer.
///
/// Stops at value/list/variable wrappers — those are NOT node-maps
/// and the deferred-shape rejection inside
/// `intercept_annotations_for_predicate` handles any annotation key
/// they might carry.
fn value_subtree_carries_annotation(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if map.contains_key(ANNOTATION_KEY)
                || map.contains_key(EDGE_KEY)
                || map.contains_key(REIFIES_KEY)
            {
                return true;
            }
            // Stop at value/list/variable wrappers — they can't host
            // an annotation in v1, and recursing further would
            // false-positive on inner keys that happen to match.
            if is_jsonld_value_object(map) {
                return false;
            }
            map.values().any(value_subtree_carries_annotation)
        }
        Value::Array(items) => items.iter().any(value_subtree_carries_annotation),
        _ => false,
    }
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

            // Wrapper rejection runs in both branches.
            reject_deferred_object_wrappers(map)?;

            // Route maps carrying either `@value` or `@language`
            // through the classifier — both are JSON-LD value-object
            // signals. The classifier rejects `@language` without a
            // sibling `@value`, so the Ref branch only receives
            // object-position node-maps that are unambiguously
            // identifier references.
            //
            // For the Ref branch, we route through `ensure_subject_id`
            // so context-aliased `@id` keys (e.g. `"id"` under a
            // custom JSON-LD context) and blank-node minting both
            // work — building `ReifiedObjectShape::Iri` directly from
            // the returned id.
            let is_value_object_shape = map.contains_key("@value") || map.contains_key("@language");
            let shape = if is_value_object_shape {
                // Reject @context-coerced literals before classifying
                // so the synthesized f:reifies* bundle's EdgeKey
                // cannot silently diverge from the base flake's.
                reject_context_coercion_on_annotated_literal(map, predicate, walk.json_ld)?;
                classify_reified_object(map)?
            } else {
                let object_id = ensure_subject_id(map, walk, ctx);
                ReifiedObjectShape::Iri(object_id)
            };

            if reifies_iris::ALL.contains(&predicate) {
                return Err(TransactError::UnsupportedFeature(format!(
                    "'{predicate}' is a system-controlled predicate; use @annotation instead"
                )));
            }
            let synth = build_annotation_sibling(
                Some(parent_subject),
                predicate,
                &shape,
                ann_block,
                walk.graph,
                ctx,
            )?;
            if let Some(node) = synth {
                ctx.siblings.push(node);
            }
            // None = empty body in RDF mode = no-op (no attachment row
            // written, base edge stays a plain triple).
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

    #[test]
    fn rejects_term_coerced_datatype_on_annotated_literal() {
        // `joinedAt` term coerces `@type` to xsd:date via @context.
        // Without the rejection, the synthesized f:reifiesObject would
        // be `{"@value": "2024-01-01"}` (xsd:string) while the base
        // flake would be xsd:date — EdgeKey mismatch.
        let doc = json!({
            "@context": {
                "ex": "http://example.org/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "joinedAt": { "@id": "ex:joinedAt", "@type": "xsd:date" }
            },
            "@id": "ex:alice",
            "joinedAt": {
                "@value": "2024-01-01",
                "@annotation": { "ex:source": "hr" }
            }
        });
        let err = lower(doc).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("@context term coercion") && msg.contains("XMLSchema#date"),
            "got: {msg}"
        );
        assert!(msg.contains("Add @type"));
    }

    #[test]
    fn accepts_term_coerced_datatype_when_value_object_is_explicit() {
        // Same shape as above but with explicit @type on the value
        // object → no mismatch, no rejection.
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
        let result = lower(doc).expect("explicit @type must lower cleanly");
        let graph = result.get("@graph").and_then(|g| g.as_array()).unwrap();
        let sibling = &graph[1];
        assert_eq!(
            sibling.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "2024-01-01", "@type": "xsd:date"})
        );
    }

    #[test]
    fn rejects_default_language_on_annotated_string() {
        let doc = json!({
            "@context": { "@language": "fr", "ex": "http://example.org/" },
            "@id": "ex:alice",
            "ex:label": {
                "@value": "chat",
                "@annotation": { "ex:source": "lexicon" }
            }
        });
        let err = lower(doc).unwrap_err();
        assert!(
            err.to_string()
                .contains("@context language coercion (@language='fr')"),
            "got: {err}"
        );
        assert!(err.to_string().contains("Add @language"));
    }

    #[test]
    fn per_term_language_does_not_suppress_default_language_rejection() {
        // The value-object expander in fluree-graph-json-ld reads
        // `context.language` directly and ignores per-term language
        // overrides (including `Some(None)` clears). The guard mirrors
        // that behavior: a default `@language` triggers rejection
        // even when the predicate's term entry would conceptually
        // override it. Otherwise the guard would pass but the
        // expander would still tag the base flake with `fr`, breaking
        // the EdgeKey round-trip.
        let doc = json!({
            "@context": {
                "@language": "fr",
                "ex": "http://example.org/",
                "code": { "@id": "ex:code", "@language": null }
            },
            "@id": "ex:alice",
            "code": {
                "@value": "ALPHA",
                "@annotation": { "ex:source": "internal" }
            }
        });
        let err = lower(doc).unwrap_err();
        assert!(
            err.to_string()
                .contains("@context language coercion (@language='fr')"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_node_local_context_term_coercion_on_annotated_literal() {
        // Envelope @context holds prefixes; the inner @graph node
        // introduces its own @context defining `joinedAt` with
        // `@type: xsd:date`. The lowering walker must merge the inner
        // context into the parent's so the coercion guard fires.
        // (Two separate `@context` keys on the same JSON object would
        // collapse via JSON parsing, so the envelope form is the only
        // way to express true per-node context merging in input JSON.)
        let doc = json!({
            "@context": {
                "ex": "http://example.org/",
                "xsd": "http://www.w3.org/2001/XMLSchema#"
            },
            "@graph": [{
                "@context": {
                    "joinedAt": { "@id": "ex:joinedAt", "@type": "xsd:date" }
                },
                "@id": "ex:alice",
                "joinedAt": {
                    "@value": "2024-01-01",
                    "@annotation": { "ex:source": "hr" }
                }
            }]
        });
        let err = lower(doc).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("@context term coercion")
                && (msg.contains("XMLSchema#date") || msg.contains("xsd:date")),
            "got: {msg}"
        );
    }

    #[test]
    fn rejects_node_local_default_language_on_annotated_string() {
        let doc = json!({
            "@context": { "ex": "http://example.org/" },
            "@graph": [{
                "@context": { "@language": "de" },
                "@id": "ex:alice",
                "ex:label": {
                    "@value": "Buch",
                    "@annotation": { "ex:source": "lex" }
                }
            }]
        });
        let err = lower(doc).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("@context language coercion") && msg.contains("'de'"),
            "got: {msg}"
        );
    }

    #[test]
    fn rejects_node_local_context_term_coercion_on_delete_clause() {
        // Symmetric coverage on the delete pre-pass: a per-node
        // `@context` inside a delete-clause node must merge into the
        // effective context before the coercion guard runs.
        let mut doc = json!({
            "@context": {
                "ex": "http://example.org/",
                "xsd": "http://www.w3.org/2001/XMLSchema#"
            },
            "delete": [{
                "@context": {
                    "joinedAt": { "@id": "ex:joinedAt", "@type": "xsd:date" }
                },
                "@id": "ex:alice",
                "joinedAt": {
                    "@value": "2024-01-01",
                    "@annotation": { "@id": "ex:ann-stale" }
                }
            }]
        });
        let err = lower_delete_annotation_blocks(&mut doc).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("@context term coercion")
                && (msg.contains("XMLSchema#date") || msg.contains("xsd:date")),
            "got: {msg}"
        );
    }

    #[test]
    fn node_local_context_provides_id_alias_for_subject_resolution() {
        // Regression: alias lookups inside the insert walker used to
        // read the *parent* walker's id_key, so a node-local
        // `"id": "@id"` alias would be invisible. The subject's id
        // would fall through to blank-node minting and the
        // synthesized f:reifiesSubject would point at the minted
        // blank rather than the user's IRI.
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
        let result = lower(doc).expect("node-local id alias must lower cleanly");
        // The lowered doc wraps the original + sibling in @graph (the
        // top-level @graph envelope already exists, so the sibling is
        // appended in-place).
        let graph = result
            .get("@graph")
            .and_then(|g| g.as_array())
            .expect("envelope @graph form");
        let sibling = graph
            .iter()
            .find(|v| {
                v.as_object()
                    .map(|m| m.contains_key(reifies_iris::SUBJECT))
                    .unwrap_or(false)
            })
            .expect("annotation sibling must be appended");
        // f:reifiesSubject must point at the user's IRI, not a minted
        // blank — confirming the alias was resolved from the merged
        // (node-local) context.
        assert_eq!(
            sibling.get(reifies_iris::SUBJECT).unwrap(),
            &json!({"@id": "ex:alice"})
        );
        assert_eq!(
            sibling.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@id": "ex:acme"})
        );
    }

    #[test]
    fn rejects_term_coerced_id_typing_on_value_form_annotation() {
        // `@type: "@id"` term coercion would make the base edge a Ref,
        // not a literal. Annotated value-object form without explicit
        // @type=@id would generate a literal f:reifiesObject while the
        // base flake is a Ref. Reject.
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
        let err = lower(doc).unwrap_err();
        assert!(
            err.to_string()
                .contains("@context term coercion (@type='@id')"),
            "got: {err}"
        );
    }

    #[test]
    fn intercept_rejects_language_without_value_on_object() {
        // End-to-end regression for the routing fix: an object
        // carrying `@language` without `@value` must NOT slip into the
        // Ref branch via `ensure_subject_id` minting a blank.
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

    // ---- Existing lowering tests ----------------------------------------

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
    fn annotation_resolves_context_aliased_id_on_ref_object() {
        // Regression: prior to using ensure_subject_id's return value
        // directly in the Ref branch, an object whose @id was aliased
        // under a JSON-LD context (e.g. "id" instead of "@id") errored
        // with "must carry @id or @value" because the classifier only
        // checked literal "@id".
        let doc = json!({
            "@context": { "id": "@id", "ex": "http://example.org/" },
            "id": "ex:alice",
            "ex:worksFor": {
                "id": "ex:acme",
                "@annotation": { "ex:role": "Engineer" }
            }
        });
        let result = lower(doc).expect("aliased @id on annotated ref must lower cleanly");
        let graph = result.get("@graph").and_then(|g| g.as_array()).unwrap();
        assert_eq!(graph.len(), 2, "original + sibling");
        let sibling = &graph[1];
        assert_eq!(
            sibling.get(reifies_iris::OBJECT).unwrap(),
            &json!({ "@id": "ex:acme" }),
            "f:reifiesObject must carry the resolved IRI from the aliased id key"
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
        let graph = result
            .get("@graph")
            .and_then(|g| g.as_array())
            .expect("envelope @graph form");
        assert_eq!(graph.len(), 2, "original + 1 annotation sibling");

        // Original payload retains the literal value object minus
        // @annotation.
        let original = &graph[0];
        let nested = original.get("ex:joinedAt").unwrap().as_object().unwrap();
        assert_eq!(nested.get("@value"), Some(&json!("2024-01-01")));
        assert_eq!(nested.get("@type"), Some(&json!("xsd:date")));
        assert!(!nested.contains_key("@annotation"));

        // Sibling carries f:reifiesObject as the canonicalized value-
        // object — not an @id reference. f:reifiesLang must be absent
        // (no @language).
        let sibling = &graph[1];
        assert_eq!(
            sibling.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "2024-01-01", "@type": "xsd:date"})
        );
        assert!(
            sibling.get(reifies_iris::LANG).is_none(),
            "no @language → no f:reifiesLang"
        );
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
        let graph = result.get("@graph").and_then(|g| g.as_array()).unwrap();
        let sibling = &graph[1];
        assert_eq!(
            sibling.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "chat", "@language": "fr"})
        );
        // f:reifiesLang explicit — required for EdgeKey::from_reifies_facts
        // to decode the same `lang` the base flake carries via flake.m.lang.
        assert_eq!(sibling.get(reifies_iris::LANG).unwrap(), &json!("fr"));
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
        let graph = result.get("@graph").and_then(|g| g.as_array()).unwrap();
        let sibling = &graph[1];
        assert_eq!(
            sibling.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "Alice"})
        );
        assert!(sibling.get(reifies_iris::LANG).is_none());
    }

    // ---- Delete-pre-pass: literal-object support ------------------------

    /// Run the delete-clause pre-pass on `doc` (assumes it has a
    /// `delete` clause) and return the rewritten doc.
    fn lower_delete(mut doc: Value) -> Result<Value> {
        lower_delete_annotation_blocks(&mut doc)?;
        Ok(doc)
    }

    fn templates(doc: &Value) -> &Vec<Value> {
        match doc.get("delete").unwrap() {
            Value::Array(a) => a,
            _ => panic!("expected delete array"),
        }
    }

    fn wheres(doc: &Value) -> &Vec<Value> {
        match doc.get("where").unwrap() {
            Value::Array(a) => a,
            _ => panic!("expected where array"),
        }
    }

    #[test]
    fn delete_by_id_on_plain_string_literal_object_emits_value_object_template() {
        let doc = json!({
            "delete": {
                "@id": "ex:alice",
                "ex:name": {
                    "@value": "Alice",
                    "@annotation": { "@id": "ex:ann-1" }
                }
            }
        });
        let lowered = lower_delete(doc).unwrap();
        let templates = templates(&lowered);
        assert_eq!(templates.len(), 1);
        let t = &templates[0];
        assert_eq!(t.get("@id").unwrap(), &json!("ex:ann-1"));
        assert_eq!(
            t.get(reifies_iris::SUBJECT).unwrap(),
            &json!({"@id": "ex:alice"})
        );
        assert_eq!(
            t.get(reifies_iris::PREDICATE).unwrap(),
            &json!({"@id": "ex:name"})
        );
        assert_eq!(
            t.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "Alice"})
        );
        assert!(t.get(reifies_iris::LANG).is_none());
        // No WHERE for by-id delete.
        assert!(lowered.get("where").is_none());
    }

    #[test]
    fn delete_by_id_on_typed_literal_object() {
        let doc = json!({
            "delete": {
                "@id": "ex:alice",
                "ex:joinedAt": {
                    "@value": "2024-01-01",
                    "@type": "xsd:date",
                    "@annotation": { "@id": "ex:ann-2" }
                }
            }
        });
        let lowered = lower_delete(doc).unwrap();
        let t = &templates(&lowered)[0];
        assert_eq!(
            t.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "2024-01-01", "@type": "xsd:date"})
        );
        assert!(t.get(reifies_iris::LANG).is_none());
    }

    #[test]
    fn delete_by_id_on_lang_tagged_literal_object_emits_reifies_lang() {
        let doc = json!({
            "delete": {
                "@id": "ex:alice",
                "ex:label": {
                    "@value": "chat",
                    "@language": "fr",
                    "@annotation": { "@id": "ex:ann-3" }
                }
            }
        });
        let lowered = lower_delete(doc).unwrap();
        let t = &templates(&lowered)[0];
        assert_eq!(
            t.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "chat", "@language": "fr"})
        );
        // Explicit f:reifiesLang on the delete template — without it,
        // the retract's decoded EdgeKey would have lang=None while the
        // original assertion's EdgeKey has lang=Some("fr"), and the
        // retract flake would not cancel the assertion.
        assert_eq!(t.get(reifies_iris::LANG).unwrap(), &json!("fr"));
    }

    #[test]
    fn delete_by_selector_on_plain_string_literal_object() {
        let doc = json!({
            "delete": {
                "@id": "ex:alice",
                "ex:name": {
                    "@value": "Alice",
                    "@annotation": { "ex:source": "hr" }
                }
            }
        });
        let lowered = lower_delete(doc).unwrap();
        let wn = &wheres(&lowered)[0];
        assert_eq!(
            wn.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "Alice"})
        );
        let t = &templates(&lowered)[0];
        assert_eq!(
            t.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "Alice"})
        );
        // Selector body property must appear on the WHERE node.
        assert_eq!(wn.get("ex:source").unwrap(), &json!("hr"));
    }

    #[test]
    fn delete_by_selector_on_lang_tagged_literal_emits_lang_in_where_and_template() {
        let doc = json!({
            "delete": {
                "@id": "ex:alice",
                "ex:label": {
                    "@value": "chat",
                    "@language": "fr",
                    "@annotation": { "ex:source": "lexicon" }
                }
            }
        });
        let lowered = lower_delete(doc).unwrap();
        let wn = &wheres(&lowered)[0];
        assert_eq!(
            wn.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "chat", "@language": "fr"})
        );
        // f:reifiesLang on the WHERE selector pins the join to the
        // right language tag — same lexical string in another
        // language must not bind.
        assert_eq!(wn.get(reifies_iris::LANG).unwrap(), &json!("fr"));
        let t = &templates(&lowered)[0];
        assert_eq!(t.get(reifies_iris::LANG).unwrap(), &json!("fr"));
    }

    /// Find the appended retract template — the first delete-clause
    /// item that carries an `f:reifiesSubject` key. Used by tests
    /// where the original delete node carries a non-`@id` keyword
    /// (e.g. `@graph`) and therefore survives the stripping pass.
    fn find_retract_template(doc: &Value) -> &Value {
        templates(doc)
            .iter()
            .find(|v| {
                v.as_object()
                    .map(|m| m.contains_key(reifies_iris::SUBJECT))
                    .unwrap_or(false)
            })
            .expect("a retract template must be appended")
    }

    #[test]
    fn delete_by_id_on_literal_in_named_graph_keeps_graph_selector() {
        let doc = json!({
            "delete": {
                "@graph": "ex:hr-graph",
                "@id": "ex:alice",
                "ex:name": {
                    "@value": "Alice",
                    "@annotation": { "@id": "ex:ann-g" }
                }
            }
        });
        let lowered = lower_delete(doc).unwrap();
        let t = find_retract_template(&lowered);
        assert_eq!(
            t.get(reifies_iris::GRAPH).unwrap(),
            &json!({"@id": "ex:hr-graph"})
        );
        assert_eq!(t.get("@graph").unwrap(), &json!("ex:hr-graph"));
        assert_eq!(
            t.get(reifies_iris::OBJECT).unwrap(),
            &json!({"@value": "Alice"})
        );
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
