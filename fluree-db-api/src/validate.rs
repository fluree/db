//! SHACL validation reports over a ledger view.
//!
//! The shared core behind `fluree validate` (CLI) and the HTTP validate
//! endpoint: validate the *current state* of a ledger (or one of its named
//! graphs) against SHACL shapes and produce a W3C-shaped validation report,
//! instead of rejecting a transaction the way staging-time enforcement does.
//!
//! Shapes can come from four places (see [`ShapesSource`]): the ledger's own
//! attached shapes (honoring `f:shapesSource`), a same-ledger named graph, or
//! an ad-hoc JSON-LD / Turtle document. Ad-hoc documents ride the same
//! non-persisting inline-shapes overlay used by per-transaction `opts.shapes`
//! — nothing is written to the ledger. By default an ad-hoc source *replaces*
//! the attached shapes ("does this data conform to THESE rules?"); set
//! [`ValidateOptions::include_attached`] to union them instead.
//!
//! Validation always runs over the query-visible view — indexed snapshot plus
//! novelty overlay — so recently committed data is never silently skipped.

use crate::error::{ApiError, Result};
use crate::ledger_view::LedgerView;
use fluree_db_core::{FlakeValue, GraphDbRef, GraphId, LedgerSnapshot, NoOverlay, Sid};
use fluree_db_shacl::compile::ShapeCompiler;
use fluree_db_shacl::{Severity, ShaclCache, ShaclCacheKey, ShaclEngine};
use fluree_db_transact::namespace::NamespaceRegistry;
use fluree_db_transact::TransactError;
use fluree_vocab::config_iris;
use fluree_vocab::shacl as sh_vocab;
use serde_json::{json, Value as JsonValue};

/// Where the shapes used for validation come from.
#[derive(Debug, Clone)]
pub enum ShapesSource {
    /// The ledger's own attached shapes: the default graph, or the graph(s)
    /// configured via `f:shapesSource`. Mirrors what transactions enforce.
    Attached,
    /// A named graph of the ledger being validated, addressed by IRI.
    Graph(String),
    /// An ad-hoc JSON-LD shapes document (non-persisting overlay).
    InlineJsonLd(JsonValue),
    /// An ad-hoc Turtle shapes document (non-persisting overlay).
    InlineTurtle(String),
}

/// Options for [`validate_view`] / [`crate::Fluree::validate_ledger`].
#[derive(Debug, Clone)]
pub struct ValidateOptions {
    /// IRI of the data graph to validate. `None` = the default graph.
    pub graph: Option<String>,
    /// Where the shapes come from.
    pub shapes: ShapesSource,
    /// When the shapes source is not [`ShapesSource::Attached`], also union
    /// in the ledger's attached shapes instead of replacing them.
    pub include_attached: bool,
    /// Micro-fuel ceiling for the whole validation pass. `None` / `Some(0)`
    /// = unbounded.
    ///
    /// Matters because of `sh:sparql`: every other SHACL constraint can only
    /// read what is reachable from the focus node, but a `sh:sparql` body may
    /// walk anywhere in the graph, once per focus node. Fuel is what makes
    /// that reading bounded.
    pub max_fuel: Option<u64>,
    /// Request-scoped cooperative cancellation: the deadline, and the carrier
    /// for the per-query memory ceiling (which is installed only when a
    /// cancellation is present). Server callers should always pass one.
    pub cancellation: Option<fluree_db_core::QueryCancellation>,
}

impl Default for ValidateOptions {
    fn default() -> Self {
        Self {
            graph: None,
            shapes: ShapesSource::Attached,
            include_attached: false,
            max_fuel: None,
            cancellation: None,
        }
    }
}

/// One validation result with all identifiers resolved to IRIs.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReportResult {
    /// The node that failed validation (`sh:focusNode`): a JSON string for
    /// IRIs / blank-node labels, or a JSON-LD value object (or native
    /// scalar) for literal `sh:targetNode` targets.
    pub focus_node: JsonValue,
    /// The property path, when it is a single predicate (`sh:resultPath`).
    /// Complex paths are omitted rather than misrepresented.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_path: Option<String>,
    /// The node shape that produced this result.
    pub source_shape: String,
    /// The property shape that produced this result, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_constraint: Option<String>,
    /// The constraint component IRI (`sh:sourceConstraintComponent`).
    pub constraint_component: String,
    /// Severity IRI: `sh:Violation`, `sh:Warning`, or `sh:Info`.
    pub severity: String,
    /// Human-readable message (`sh:resultMessage`).
    pub message: String,
    /// The offending value, when applicable (`sh:value`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<JsonValue>,
}

/// A resolved validation report, ready for serialization.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ValidateReport {
    /// `true` when no result carries `sh:Violation` severity.
    pub conforms: bool,
    /// Individual results, sorted by (focus node, component, message).
    pub results: Vec<ReportResult>,
    /// Number of compiled shapes that were checked. `0` means the shapes
    /// source produced no shapes — the report is vacuously conforming.
    pub shape_count: usize,
}

impl ValidateReport {
    /// Count results with `sh:Violation` severity.
    pub fn violation_count(&self) -> usize {
        self.count_severity(sh_vocab::VIOLATION)
    }

    /// Count results with `sh:Warning` severity.
    pub fn warning_count(&self) -> usize {
        self.count_severity(sh_vocab::WARNING)
    }

    /// Count results with `sh:Info` severity.
    pub fn info_count(&self) -> usize {
        self.count_severity(sh_vocab::INFO)
    }

    fn count_severity(&self, severity: &str) -> usize {
        self.results
            .iter()
            .filter(|r| r.severity == severity)
            .count()
    }

    /// Serialize as a W3C `sh:ValidationReport` JSON-LD document.
    pub fn to_jsonld(&self) -> JsonValue {
        let results: Vec<JsonValue> = self
            .results
            .iter()
            .map(|r| {
                let mut obj = serde_json::Map::new();
                obj.insert("@type".into(), json!("sh:ValidationResult"));
                let focus = match r.focus_node.as_str() {
                    Some(iri) => json!({"@id": iri}),
                    None => r.focus_node.clone(),
                };
                obj.insert("sh:focusNode".into(), focus);
                if let Some(path) = &r.result_path {
                    obj.insert("sh:resultPath".into(), json!({"@id": path}));
                }
                obj.insert(
                    "sh:resultSeverity".into(),
                    json!({"@id": compact_sh(&r.severity)}),
                );
                // W3C sh:sourceShape is the shape that declares the failed
                // constraint — the property shape when there is one.
                let source_shape = r.source_constraint.as_ref().unwrap_or(&r.source_shape);
                obj.insert("sh:sourceShape".into(), json!({"@id": source_shape}));
                obj.insert(
                    "sh:sourceConstraintComponent".into(),
                    json!({"@id": compact_sh(&r.constraint_component)}),
                );
                obj.insert("sh:resultMessage".into(), json!(r.message));
                if let Some(value) = &r.value {
                    obj.insert("sh:value".into(), value.clone());
                }
                JsonValue::Object(obj)
            })
            .collect();

        json!({
            "@context": {"sh": "http://www.w3.org/ns/shacl#"},
            "@type": "sh:ValidationReport",
            "sh:conforms": self.conforms,
            "sh:result": results,
        })
    }

    /// Serialize as a W3C `sh:ValidationReport` Turtle document.
    pub fn to_turtle(&self) -> String {
        let mut out = String::from("@prefix sh: <http://www.w3.org/ns/shacl#> .\n\n");
        out.push_str("[] a sh:ValidationReport ;\n");
        out.push_str(&format!("    sh:conforms {}", self.conforms));
        for r in &self.results {
            out.push_str(" ;\n    sh:result [\n        a sh:ValidationResult ;\n");
            let focus_term = match r.focus_node.as_str() {
                Some(iri) => turtle_term(iri),
                None => turtle_value_term(&r.focus_node),
            };
            out.push_str(&format!("        sh:focusNode {focus_term} ;\n"));
            if let Some(path) = &r.result_path {
                out.push_str(&format!("        sh:resultPath {} ;\n", turtle_term(path)));
            }
            out.push_str(&format!(
                "        sh:resultSeverity {} ;\n",
                turtle_sh_term(&r.severity)
            ));
            let source_shape = r.source_constraint.as_ref().unwrap_or(&r.source_shape);
            out.push_str(&format!(
                "        sh:sourceShape {} ;\n",
                turtle_term(source_shape)
            ));
            out.push_str(&format!(
                "        sh:sourceConstraintComponent {} ;\n",
                turtle_sh_term(&r.constraint_component)
            ));
            if let Some(value) = &r.value {
                out.push_str(&format!(
                    "        sh:value {} ;\n",
                    turtle_value_term(value)
                ));
            }
            out.push_str(&format!(
                "        sh:resultMessage {}\n    ]",
                turtle_string(&r.message)
            ));
        }
        out.push_str(" .\n");
        out
    }
}

/// Render an IRI or blank-node label as a Turtle term.
///
/// A blank-node label may carry characters that are invalid in a Turtle
/// BLANK_NODE_LABEL — map them to `-` so the emitted report always parses.
///
/// Two label shapes still need it. Ledgers imported by Fluree 4.1.4 or earlier
/// hold ids that embedded the ledger id, so they contain `/` and `:`
/// (`fdb-lubm:main-1-genid10`); current import ids are `[0-9a-z-]` only (see
/// `fluree_db_core::skolem`) and pass through unchanged. Client-authored
/// labels can also carry characters JSON-LD accepts and Turtle does not.
///
/// The rewrite is **lossy**: two labels differing only in sanitized characters
/// collapse onto one. That is acceptable here — a validation report is
/// human-facing output, not a round-trippable serialization of the graph — and
/// it is why the same rewrite is deliberately NOT applied on the export path
/// (`crate::export`), where identity has to survive.
fn turtle_term(iri_or_bnode: &str) -> String {
    match iri_or_bnode.strip_prefix("_:") {
        Some(label) => {
            let clean: String = label
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                        c
                    } else {
                        '-'
                    }
                })
                .collect();
            format!("_:{clean}")
        }
        None => turtle_iri(iri_or_bnode),
    }
}

/// Render an IRI as a Turtle IRIREF, `\uXXXX`-escaping the characters the Turtle
/// grammar forbids inside `<…>` (controls, space, and ``<>"{}|^`\``). The parser
/// unescapes these back to the original codepoint, so the IRI round-trips —
/// unlike percent-encoding, which would change the IRI's identity.
fn turtle_iri(iri: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(iri.len() + 2);
    out.push('<');
    for c in iri.chars() {
        match c {
            '\u{00}'..='\u{20}' | '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\' => {
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            _ => out.push(c),
        }
    }
    out.push('>');
    out
}

/// Render an IRI as `sh:Name` when it lives in the SHACL namespace.
fn turtle_sh_term(iri: &str) -> String {
    match iri.strip_prefix("http://www.w3.org/ns/shacl#") {
        Some(local) => format!("sh:{local}"),
        None => turtle_term(iri),
    }
}

/// Render a report value (as produced by `value_json`) as a Turtle term.
fn turtle_value_term(value: &JsonValue) -> String {
    match value {
        JsonValue::Object(obj) => {
            if let Some(iri) = obj.get("@id").and_then(|v| v.as_str()) {
                return turtle_term(iri);
            }
            if let Some(lex) = obj.get("@value").and_then(|v| v.as_str()) {
                if let Some(lang) = obj.get("@language").and_then(|v| v.as_str()) {
                    return format!("{}@{lang}", turtle_string(lex));
                }
                if let Some(dt) = obj.get("@type").and_then(|v| v.as_str()) {
                    return format!("{}^^{}", turtle_string(lex), turtle_iri(dt));
                }
                return turtle_string(lex);
            }
            turtle_string(&value.to_string())
        }
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => turtle_string(s),
        other => turtle_string(&other.to_string()),
    }
}

/// Quote and escape a Turtle string literal.
fn turtle_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Compact an IRI in the SHACL namespace to its `sh:` form for readability.
fn compact_sh(iri: &str) -> String {
    match iri.strip_prefix("http://www.w3.org/ns/shacl#") {
        Some(local) => format!("sh:{local}"),
        None => iri.to_string(),
    }
}

impl crate::Fluree {
    /// Validate the current state of a ledger against SHACL shapes and
    /// return a resolved validation report. See [`ValidateOptions`].
    ///
    /// This is the entry point that supports a cross-ledger `f:shapesSource`
    /// (`f:ledger` set): the model ledger's shapes are resolved through the
    /// governance cache (one nameservice head lookup when M is unchanged)
    /// and validated against exactly like the transaction path.
    pub async fn validate_ledger(
        &self,
        ledger_id: &str,
        options: &ValidateOptions,
    ) -> Result<ValidateReport> {
        let handle = self.ledger_cached(ledger_id).await?;
        let view = handle.snapshot().await;

        let use_attached =
            matches!(options.shapes, ShapesSource::Attached) || options.include_attached;
        let (config, cross) = if use_attached {
            let config = resolve_config_for_validate(&view).await;
            let is_cross_ledger = config
                .as_ref()
                .and_then(|c| c.shacl.as_ref())
                .and_then(|s| s.shapes_source.as_ref())
                .is_some_and(|s| s.ledger.is_some());
            let cross = if is_cross_ledger {
                let mut resolve_ctx = crate::cross_ledger::ResolveCtx::new(ledger_id, self);
                crate::tx::open_cross_ledger_shapes_model(config.as_ref(), &mut resolve_ctx)
                    .await
                    .map_err(ApiError::from)?
            } else {
                None
            };
            (config, cross)
        } else {
            (None, None)
        };

        validate_view_inner(&view, ledger_id, options, config, cross).await
    }
}

/// Resolve the ledger config for validation. Mirrors the transaction path's
/// degrade semantics: a broken config graph read falls back to defaults
/// (default-graph shapes) rather than failing validation.
async fn resolve_config_for_validate(
    view: &LedgerView,
) -> Option<fluree_db_core::ledger_config::LedgerConfig> {
    match crate::config_resolver::resolve_ledger_config(
        view.snapshot.as_ref(),
        view.novelty.as_ref(),
        view.t,
    )
    .await
    {
        Ok(config) => config,
        Err(e) => {
            tracing::debug!(error = %e, "Config graph read failed during validate — using defaults");
            None
        }
    }
}

/// Validate a ledger view against SHACL shapes.
///
/// Runs over the query-visible composition (snapshot + novelty overlay) of
/// `view`, scoped to the data graph selected by `options.graph`.
///
/// This view-scoped entry point has no connection handle, so a cross-ledger
/// `f:shapesSource` cannot be resolved here and returns an error — use
/// [`crate::Fluree::validate_ledger`] for ledgers configured that way.
pub async fn validate_view(
    view: &LedgerView,
    ledger_id: &str,
    options: &ValidateOptions,
) -> Result<ValidateReport> {
    let use_attached = matches!(options.shapes, ShapesSource::Attached) || options.include_attached;
    let config = if use_attached {
        resolve_config_for_validate(view).await
    } else {
        None
    };
    validate_view_inner(view, ledger_id, options, config, None).await
}

/// Fuel tracker for one validation pass. `None` / `Some(0)` = unbounded,
/// matching `tracker_for_limits` on the query and transact paths.
fn validate_tracker(max_fuel: Option<u64>) -> fluree_db_core::tracking::Tracker {
    use fluree_db_core::tracking::{Tracker, TrackingOptions};
    match max_fuel.filter(|limit| *limit > 0) {
        Some(limit) => Tracker::new(TrackingOptions {
            track_time: false,
            track_fuel: true,
            track_policy: false,
            max_fuel: Some(limit),
        }),
        None => Tracker::disabled(),
    }
}

async fn validate_view_inner(
    view: &LedgerView,
    ledger_id: &str,
    options: &ValidateOptions,
    config: Option<fluree_db_core::ledger_config::LedgerConfig>,
    cross: Option<crate::tx::CrossLedgerShapesModel>,
) -> Result<ValidateReport> {
    let snapshot = view.snapshot.as_ref();
    let novelty = view.novelty.as_ref();
    let to_t = view.t;

    let data_g_id = match options.graph.as_deref() {
        None => 0u16,
        Some(iri) => resolve_graph_iri(snapshot, iri)?,
    };

    // Holders declared before `shape_dbs` so the borrows they hand out
    // outlive the GraphDbRefs (drop order: shape_dbs first).
    let mut inline_registry: Option<NamespaceRegistry> = None;
    #[allow(unused_assignments)]
    let mut inline_snapshot: Option<LedgerSnapshot> = None;
    #[allow(unused_assignments)]
    let mut inline_overlay = None;
    let mut inline_membership: Option<fluree_db_shacl::CrossLedgerMembership<'_>> = None;
    #[allow(unused_assignments)]
    let mut cross_overlay = None;
    let mut cross_membership: Option<fluree_db_shacl::CrossLedgerMembership<'_>> = None;

    let mut shape_dbs: Vec<GraphDbRef<'_>> = Vec::new();
    let mut membership: Vec<GraphId> = Vec::new();

    let use_attached = matches!(options.shapes, ShapesSource::Attached) || options.include_attached;
    if use_attached {
        if let Some(model) = cross.as_ref() {
            // Cross-ledger `f:shapesSource`: compile from M's wire artifact,
            // translated against D's committed namespace registry (validate
            // has no in-flight transaction, so the snapshot registry is
            // complete). Mirrors the transaction path's shape source; D's
            // own default-graph shapes compose via the bundle overlay's
            // novelty base exactly as they do at staging time.
            let wire = model.wire().ok_or_else(|| {
                ApiError::from(TransactError::Parse(
                    "cross-ledger shapes resolution returned a non-shapes artifact".into(),
                ))
            })?;
            let registry = NamespaceRegistry::from_db(snapshot);
            let bundle = wire
                .translate_to_schema_bundle_flakes(&registry)
                .map_err(|e| {
                    ApiError::from(TransactError::Parse(format!(
                        "cross-ledger shapes wire translation failed: {e}"
                    )))
                })?;
            cross_overlay = Some(fluree_db_query::schema_bundle::SchemaBundleOverlay::new(
                novelty, bundle,
            ));
            shape_dbs.push(GraphDbRef::new(
                snapshot,
                0u16,
                cross_overlay.as_ref().expect("just set above"),
                to_t,
            ));
            membership.push(0);
            // `sh:class` value-sets living alongside M's shapes: probe M's
            // shapes graph after a local membership miss, translating D-term
            // Sids through the snapshot namespace map.
            cross_membership = Some(fluree_db_shacl::CrossLedgerMembership {
                model_db: GraphDbRef::new(
                    &model.model_db.snapshot,
                    model.model_g_id,
                    model.model_db.overlay.as_ref(),
                    model.model_db.t,
                ),
                data_ns_map: snapshot.namespaces(),
                same_term_space: false,
            });
        } else {
            let shapes_g_ids = crate::tx::resolve_shapes_source_g_ids(config.as_ref(), snapshot)?;
            for g_id in &shapes_g_ids {
                shape_dbs.push(GraphDbRef::new(snapshot, *g_id, novelty, to_t));
            }
            membership.extend(shapes_g_ids);
        }
    }

    match &options.shapes {
        ShapesSource::Attached => {}
        ShapesSource::Graph(iri) => {
            let g_id = resolve_graph_iri(snapshot, iri)?;
            if !membership.contains(&g_id) {
                shape_dbs.push(GraphDbRef::new(snapshot, g_id, novelty, to_t));
                membership.push(g_id);
            }
        }
        ShapesSource::InlineJsonLd(_) | ShapesSource::InlineTurtle(_) => {
            let mut registry = NamespaceRegistry::from_db(snapshot);
            let bundle = match &options.shapes {
                ShapesSource::InlineJsonLd(doc) => {
                    crate::inline_shapes::parse_inline_shapes_to_bundle(
                        doc,
                        &mut registry,
                        to_t,
                        ledger_id,
                    )?
                }
                ShapesSource::InlineTurtle(turtle) => {
                    crate::inline_shapes::parse_inline_shapes_turtle_to_bundle(
                        turtle,
                        &mut registry,
                        to_t,
                        ledger_id,
                    )?
                }
                _ => unreachable!("outer match arm covers only inline sources"),
            }
            .ok_or_else(empty_shapes_doc)?;
            inline_registry = Some(registry);
            // Compile from the bundle alone: an empty genesis snapshot with a
            // no-op base overlay keeps the ledger's own graph-0 shapes
            // (indexed or in novelty) out of the compile scan. When
            // `include_attached` is set, the attached shape dbs pushed above
            // contribute the union — never a double scan of graph 0.
            static NO_OVERLAY: NoOverlay = NoOverlay;
            inline_snapshot = Some(LedgerSnapshot::genesis(ledger_id));
            inline_overlay = Some(fluree_db_query::schema_bundle::SchemaBundleOverlay::new(
                &NO_OVERLAY,
                bundle,
            ));
            let bundle_db = GraphDbRef::new(
                inline_snapshot.as_ref().expect("just set above"),
                0u16,
                inline_overlay.as_ref().expect("just set above"),
                to_t,
            );
            shape_dbs.push(bundle_db);
            // The bundle may carry value-set facts alongside the shapes
            // (e.g. `ex:CA rdf:type ex:State` for a `sh:class ex:State`
            // constraint) — matching the documented f:shapesSource
            // semantics where vocabulary lives with the shapes. It shares
            // the data ledger's term space, so membership probes use the
            // data-side Sids directly.
            inline_membership = Some(fluree_db_shacl::CrossLedgerMembership {
                model_db: bundle_db,
                data_ns_map: snapshot.namespaces(),
                same_term_space: true,
            });
        }
    }

    let shapes = ShapeCompiler::compile_from_dbs(&shape_dbs)
        .await
        .map_err(TransactError::from)?;
    // Hierarchy comes from the ledger's shared cache (novelty-aware) even
    // when shapes compile from a detached inline bundle — subclass expansion
    // must reflect the data, including unindexed schema commits.
    let hierarchy = view
        .schema_hierarchy_cache
        .current(snapshot, novelty, to_t, view.novelty.schema_epoch)
        .await
        .map_err(TransactError::from)?;
    let cache = ShaclCache::new(
        ShaclCacheKey::new(ledger_id, to_t as u64),
        shapes,
        hierarchy.as_ref(),
    );
    let mut engine = match hierarchy {
        Some(h) => ShaclEngine::new_with_hierarchy(cache, h),
        None => ShaclEngine::new(cache),
    }
    .with_membership_graphs(membership);
    if let Some(cancellation) = options.cancellation.clone() {
        engine = engine.with_cancellation(cancellation);
    }

    let shape_count = engine.shape_count();
    if engine.is_empty() {
        return Ok(ValidateReport {
            conforms: true,
            results: Vec::new(),
            shape_count,
        });
    }

    // Fuel bounds how much the pass may READ. `sh:sparql` runs one query per
    // focus node whose body is not limited to the focus node's neighbourhood,
    // so without a tracker a single request can walk the whole ledger
    // repeatedly. `with_tracker` is a no-op on a disabled tracker, so the
    // unbounded default costs nothing.
    let tracker = validate_tracker(options.max_fuel);
    let data_db = GraphDbRef::new(snapshot, data_g_id, novelty, to_t).with_tracker(&tracker);
    // The engine takes one external membership source. Inline shapes carry
    // their own value-set facts (same term space), so an explicit inline
    // document keeps precedence; the cross-ledger probe serves the attached
    // cross-ledger shapes otherwise.
    let raw = engine
        .validate_all_with_membership(data_db, inline_membership.or(cross_membership))
        .await
        .map_err(TransactError::from)?;

    let resolve = |sid: &Sid| resolve_sid(snapshot, inline_registry.as_ref(), sid);
    let mut results: Vec<ReportResult> = raw
        .results
        .iter()
        .map(|r| ReportResult {
            focus_node: match &r.focus_node {
                fluree_db_shacl::FocusNode::Node(sid) => JsonValue::String(resolve(sid)),
                fluree_db_shacl::FocusNode::Literal(lit) => {
                    match value_json(
                        &lit.value,
                        Some(&lit.datatype),
                        lit.lang.as_deref(),
                        &resolve,
                    ) {
                        // A bare string would be ambiguous with an IRI focus —
                        // wrap plain string literals as a value object.
                        JsonValue::String(s) => json!({"@value": s}),
                        other => other,
                    }
                }
            },
            result_path: r.result_path.as_ref().map(&resolve),
            source_shape: resolve(&r.source_shape),
            source_constraint: r.source_constraint.as_ref().map(&resolve),
            constraint_component: r.constraint_component.to_string(),
            severity: severity_iri(r.severity).to_string(),
            message: r.message.clone(),
            value: r.value.as_ref().map(|v| {
                value_json(
                    v,
                    r.value_datatype.as_ref(),
                    r.value_lang.as_deref(),
                    &resolve,
                )
            }),
        })
        .collect();
    results.sort_by(|a, b| {
        (
            a.focus_node.to_string(),
            &a.constraint_component,
            &a.message,
        )
            .cmp(&(
                b.focus_node.to_string(),
                &b.constraint_component,
                &b.message,
            ))
    });

    Ok(ValidateReport {
        conforms: raw.conforms,
        results,
        shape_count,
    })
}

fn empty_shapes_doc() -> ApiError {
    ApiError::Transact(TransactError::Parse(
        "shapes document contains no triples".into(),
    ))
}

fn resolve_graph_iri(snapshot: &LedgerSnapshot, iri: &str) -> Result<GraphId> {
    if iri == config_iris::DEFAULT_GRAPH {
        return Ok(0);
    }
    snapshot
        .graph_registry
        .graph_id_for_iri(iri)
        .ok_or_else(|| {
            ApiError::NotFound(format!(
                "graph '{iri}' not found in this ledger's graph registry"
            ))
        })
}

/// Resolve a SID to an IRI: the snapshot's namespace table first, then the
/// inline-shapes registry (ad-hoc shape vocabulary gets ephemeral codes the
/// snapshot has never seen), then the raw name as a last resort.
fn resolve_sid(
    snapshot: &LedgerSnapshot,
    inline_registry: Option<&NamespaceRegistry>,
    sid: &Sid,
) -> String {
    snapshot
        .decode_sid(sid)
        .or_else(|| {
            inline_registry.and_then(|registry| {
                registry
                    .get_prefix(sid.namespace_code)
                    .map(|prefix| format!("{prefix}{}", sid.name))
            })
        })
        .unwrap_or_else(|| sid.name.to_string())
}

fn severity_iri(severity: Severity) -> &'static str {
    match severity {
        Severity::Violation => sh_vocab::VIOLATION,
        Severity::Warning => sh_vocab::WARNING,
        Severity::Info => sh_vocab::INFO,
    }
}

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// Render an `sh:value` term as JSON-LD, preserving RDF term fidelity:
/// language-tagged literals become `{"@value", "@language"}`, non-native
/// datatypes become `{"@value", "@type"}` with the lexical form, and only
/// the JSON-native XSD types (string / boolean / integer / double) render
/// as bare JSON scalars.
fn value_json(
    value: &FlakeValue,
    datatype: Option<&Sid>,
    lang: Option<&str>,
    resolve: &impl Fn(&Sid) -> String,
) -> JsonValue {
    if let FlakeValue::Ref(sid) = value {
        return json!({"@id": resolve(sid)});
    }
    if let Some(lang) = lang {
        return json!({"@value": lexical_form(value), "@language": lang});
    }
    if let Some(dt) = datatype {
        let dt_iri = resolve(dt);
        // A String carrying the `@id` datatype is a stringified IRI
        // (STR() semantics in the string facets) — report the IRI node.
        // Match the JSON-LD `@id` datatype by namespace, not the bare local
        // name, so a real datatype whose local name is `id` (e.g.
        // `<http://example.org/id>`) is not misreported as an IRI node.
        let is_id_datatype = dt_iri == "@id"
            || (dt.namespace_code == fluree_vocab::namespaces::JSON_LD
                && &*dt.name == fluree_vocab::jsonld_names::ID);
        if is_id_datatype {
            if let FlakeValue::String(s) = value {
                return json!({"@id": s});
            }
        }
        return match (value, dt_iri.strip_prefix(XSD)) {
            (FlakeValue::String(s), Some("string")) => json!(s),
            (FlakeValue::Boolean(b), Some("boolean")) => json!(b),
            (FlakeValue::Long(n), Some("integer")) => json!(n),
            (FlakeValue::Double(d), Some("double")) => json!(d),
            _ => json!({"@value": lexical_form(value), "@type": dt_iri}),
        };
    }
    // No datatype threaded: JSON natives stay native; self-describing
    // temporal / numeric variants carry their XSD type.
    match value {
        FlakeValue::Boolean(b) => json!(b),
        FlakeValue::Long(n) => json!(n),
        FlakeValue::Double(d) => json!(d),
        FlakeValue::String(s) => json!(s),
        FlakeValue::Json(s) => serde_json::from_str(s).unwrap_or_else(|_| json!(s)),
        FlakeValue::Null => JsonValue::Null,
        other => match inferred_xsd_type(other) {
            Some(local) => json!({
                "@value": lexical_form(other),
                "@type": format!("{XSD}{local}"),
            }),
            None => json!(other.to_string()),
        },
    }
}

/// Lexical form of a literal for `@value`. `Display` is the canonical form
/// for the temporal / numeric variants, but wraps strings in quotes.
fn lexical_form(value: &FlakeValue) -> String {
    match value {
        FlakeValue::String(s) | FlakeValue::Json(s) => s.clone(),
        other => other.to_string(),
    }
}

/// XSD local name for FlakeValue variants that self-describe their datatype.
fn inferred_xsd_type(value: &FlakeValue) -> Option<&'static str> {
    Some(match value {
        FlakeValue::Decimal(_) => "decimal",
        FlakeValue::BigInt(_) => "integer",
        FlakeValue::DateTime(_) => "dateTime",
        FlakeValue::Date(_) => "date",
        FlakeValue::Time(_) => "time",
        FlakeValue::GYear(_) => "gYear",
        FlakeValue::GYearMonth(_) => "gYearMonth",
        FlakeValue::GMonth(_) => "gMonth",
        FlakeValue::GDay(_) => "gDay",
        FlakeValue::GMonthDay(_) => "gMonthDay",
        FlakeValue::YearMonthDuration(_) => "yearMonthDuration",
        FlakeValue::DayTimeDuration(_) => "dayTimeDuration",
        FlakeValue::Duration(_) => "duration",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_report() -> ValidateReport {
        ValidateReport {
            conforms: false,
            results: vec![ReportResult {
                focus_node: "http://example.org/ns/nameless".into(),
                result_path: Some("http://schema.org/name".into()),
                source_shape: "http://example.org/ns/UserShape".into(),
                source_constraint: Some("http://example.org/ns/name-ps".into()),
                constraint_component: "http://www.w3.org/ns/shacl#MinCountConstraintComponent"
                    .into(),
                severity: "http://www.w3.org/ns/shacl#Violation".into(),
                message: "Expected at least 1 value(s) but found 0".into(),
                value: None,
            }],
            shape_count: 1,
        }
    }

    #[test]
    fn turtle_report_round_trips_through_parser() {
        let turtle = sample_report().to_turtle();
        let mut sink = fluree_graph_ir::GraphCollectorSink::new();
        fluree_graph_turtle::parse(&turtle, &mut sink).expect("report Turtle must parse");
        let graph = sink.into_graph();
        // report node (type + conforms + result) and the result node's fields
        assert!(graph.len() >= 8, "expected full report triples: {turtle}");
        assert!(turtle.contains("sh:MinCountConstraintComponent"));
        assert!(turtle.contains("<http://example.org/ns/nameless>"));
    }

    #[test]
    fn turtle_term_sanitizes_blank_node_labels() {
        assert_eq!(
            turtle_term("_:fdb-inline-shapes-validate/scratch:main-2-b1"),
            "_:fdb-inline-shapes-validate-scratch-main-2-b1"
        );
        assert_eq!(turtle_term("http://ex.org/a"), "<http://ex.org/a>");
    }

    #[test]
    fn turtle_string_escapes_specials() {
        assert_eq!(turtle_string("a\"b\\c\nd"), "\"a\\\"b\\\\c\\nd\"");
    }

    #[test]
    fn turtle_iri_escapes_illegal_chars() {
        // Well-formed IRIs pass through unchanged.
        assert_eq!(turtle_iri("http://ex.org/a"), "<http://ex.org/a>");
        // Grammar-illegal chars become \uXXXX and the result still parses.
        assert_eq!(
            turtle_iri("http://ex.org/a b<c>"),
            "<http://ex.org/a\\u0020b\\u003Cc\\u003E>"
        );
        let turtle = format!(
            "<http://ex.org/s> <http://ex.org/p> {} .\n",
            turtle_iri("http://ex.org/a b|c")
        );
        let mut sink = fluree_graph_ir::GraphCollectorSink::new();
        fluree_graph_turtle::parse(&turtle, &mut sink).expect("escaped IRI must parse");
    }
}
