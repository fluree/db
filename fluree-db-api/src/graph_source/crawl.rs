//! Graph-source (R2RML/Iceberg) subgraph-crawl hydration.
//!
//! FQL's subgraph / "crawl" projection (`{"select": {"?s": ["*"]}}`) is normally
//! satisfied by the async hydration formatter, which fetches each bound subject's
//! flakes from the native binary index. An R2RML graph source has **no**
//! binary-index flakes — its data lives in Iceberg and is only reachable through
//! the R2RML operator — so native hydration resolves every subject to `null` and
//! the crawl returns an empty array (`[]`), which is what makes the Solo
//! virtual-dataset "View Instances" screen come back empty.
//!
//! This module expands a wildcard crawl over a graph source through the R2RML
//! operator instead: it rewrites the crawl into a SINGLE flat wildcard scan
//! (`?s ?p ?o`, which the operator binds via `predicate_var` — see
//! `fluree_db_query::r2rml` — and on which it also emits each subject's
//! `rr:class`-derived `rdf:type` rows), executes it via the same R2RML query
//! path the rest of the engine uses, and regroups the flat
//! `(subject, predicate, object)` rows into per-subject JSON-LD documents,
//! folding `rdf:type` rows into `@type`.
//!
//! Scope: only a **wildcard** (`["*"]`) single-column crawl is expanded here.
//! Explicit-predicate selections, nested ref-crawls, and multi-column projections
//! fall back to the normal path (returning `Ok(None)`), so this never changes the
//! behavior of any query it does not fully handle.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde_json::{json, Map, Value as JsonValue};

use fluree_db_query::ir::projection::Column;
use fluree_db_query::var_registry::VarId;
use fluree_db_query::Binding;

use crate::format::{format_node_object_binding, FormatterConfig, IriCompactor};
use crate::view::{GraphDb, QueryInput};
use crate::{Fluree, QueryExecutionOptions, Result};

/// Fresh variable names for the injected wildcard scan. The leading `?__` keeps
/// them from colliding with any user variable.
const CRAWL_PRED: &str = "?__crawl_p";
const CRAWL_OBJ: &str = "?__crawl_o";
const CRAWL_TYPE: &str = "?__crawl_type";

/// Per-subject triple budget used to translate the crawl's **subject** LIMIT into
/// the flat query's **triple** LIMIT. The flat scan fetches `(limit + 1) × BUDGET`
/// triples — enough that the first `limit` subjects are fully materialized (each
/// with up to `BUDGET` predicate/object/type triples) while still bounding the
/// scan so it early-terminates instead of walking the whole table. A subject with
/// more than `BUDGET` triples may be truncated (acceptable for the tabular
/// dimension/fact tables R2RML maps; rows are wide in columns, not in triples).
const TRIPLES_PER_SUBJECT_BUDGET: usize = 64;

/// Maximum subject OFFSET a crawl may request. OFFSET paging re-scans and then
/// discards the first `offset` subjects on every page (the flat triple budget
/// covers `offset + limit` subjects), so a deep offset silently becomes a large
/// remote scan; past this depth the crawl errors with guidance instead. Browse
/// paging (the surface this exists for) stays far below it.
const MAX_CRAWL_OFFSET_SUBJECTS: usize = 10_000;

/// Distinct object-var name for the i-th explicit predicate of a predicate-list
/// crawl. A DISTINCT var per predicate is REQUIRED: a shared object var makes
/// the star members a self-join constraint rather than a star, defeating the
/// single-scan star collapse (see `r2rml::rewrite`).
fn crawl_obj_var(i: usize) -> String {
    format!("?__crawl_o{i}")
}

/// The projection shape of a recognized subgraph crawl.
#[derive(Debug, Clone)]
enum CrawlProjection {
    /// `["*"]` — every predicate/object of the subject plus its declared classes.
    Wildcard,
    /// `["@id"]` — subject IRIs only; no property or type scan (the cheapest
    /// shape: it never materializes a predicate-object map).
    IdOnly,
    /// An explicit forward-predicate list (`["v:p1", "v:p2", ...]`). `want_type`
    /// records an explicit `"@type"` in the list; `want_id` records an explicit
    /// `"@id"`. For a variable-subject crawl `@id` is always emitted (it is the
    /// node's identity); for a constant-IRI select-map (Cluster B) `@id` is
    /// emitted ONLY when `want_id` (native omits `@id` from an explicit projection
    /// that does not request it — the single subject is already known). Each
    /// predicate scans with a DISTINCT object var so the members star-collapse
    /// into ONE scan (and inherit class fusion when the WHERE binds a class).
    Predicates {
        predicates: Vec<String>,
        want_type: bool,
        want_id: bool,
    },
}

/// The subject root of a recognized crawl.
enum CrawlRoot<'a> {
    /// A variable subject (`"?s"`): the crawl's WHERE binds and filters many
    /// subjects, which the expansion groups into per-subject documents.
    Var(&'a str),
    /// A constant-IRI subject (`"<iri>"`): a bound-subject select-map (Cluster B,
    /// D4). The one subject is already known, so the expansion runs the proven
    /// bound-subject wildcard scan (pruned to the subject's own table) and shapes
    /// the single result document, returning the native-parity `{"@id": <iri>}`
    /// stub when the subject is absent / unreversible.
    Iri(&'a str),
}

/// A recognized crawl decomposed into the parts the flat-query builder needs.
struct DetectedCrawl<'a> {
    /// The subject root: a variable, or a constant IRI (a bound-subject
    /// select-map, Cluster B).
    root: CrawlRoot<'a>,
    /// The original WHERE clause (binds/filters a variable subject). Always
    /// present for a [`CrawlRoot::Var`]; typically `None` for a
    /// [`CrawlRoot::Iri`] (the subject is already bound by the constant).
    where_clause: Option<&'a JsonValue>,
    /// The query's `@context`, if any (carried onto the flat query).
    context: Option<&'a JsonValue>,
    /// The crawl's subject LIMIT, if any.
    limit: Option<usize>,
    /// The crawl's subject OFFSET (0 when absent). Applied to the grouped
    /// SUBJECTS (not the flat triples), so the flat scan must fetch enough
    /// triples to cover `offset + limit` subjects and grouping then skips the
    /// first `offset`.
    offset: usize,
    /// Which projection shape this crawl requests.
    projection: CrawlProjection,
}

/// Accumulates one subject's properties in first-seen order.
struct SubjectAcc {
    /// Distinct class IRIs (`@type`), in first-seen order.
    types: Vec<String>,
    /// `(predicate IRI, values)` pairs, in first-seen order; values de-duplicated.
    props: Vec<(String, Vec<JsonValue>)>,
}

impl SubjectAcc {
    fn empty() -> Self {
        Self {
            types: Vec::new(),
            props: Vec::new(),
        }
    }

    fn add_type(&mut self, type_iri: String) {
        if !self.types.contains(&type_iri) {
            self.types.push(type_iri);
        }
    }

    fn add_value(&mut self, pred: String, value: JsonValue) {
        match self.props.iter_mut().find(|(k, _)| *k == pred) {
            Some((_, vals)) => {
                if !vals.contains(&value) {
                    vals.push(value);
                }
            }
            None => self.props.push((pred, vec![value])),
        }
    }
}

/// Expand a wildcard subgraph crawl over an R2RML graph source, returning the
/// per-subject JSON-LD documents. Returns `Ok(None)` when `input` is not a
/// wildcard crawl this path handles, so the caller falls back to normal
/// formatting.
pub(crate) async fn expand_wildcard_crawl(
    fluree: &Fluree,
    view: &GraphDb,
    input: &JsonValue,
    provider: &dyn fluree_db_query::r2rml::R2rmlProvider,
    table_provider: &dyn fluree_db_query::r2rml::R2rmlTableProvider,
    execution: QueryExecutionOptions,
    format_config: &FormatterConfig,
) -> Result<Option<JsonValue>> {
    let Some(DetectedCrawl {
        root,
        where_clause,
        context,
        limit,
        offset,
        projection,
    }) = detect_wildcard_crawl(input)
    else {
        return Ok(None);
    };

    // C1: a top-level `values` clause is NOT consumed by crawl planning
    // (`detect_wildcard_crawl` / `build_flat_query` never read it), so it would
    // be silently DROPPED — the crawl would return the ENTIRE class instead of
    // the VALUES-constrained subjects (a wrong-answer defect, not merely a slow
    // one). Refuse loudly with the typed R2RML envelope until VALUES is honored
    // on the crawl path (the loud-or-correct precedent). A flat-select VALUES
    // query is not a crawl select-map, so it never reaches here (detected `None`
    // above) and keeps working on the normal path.
    if input.get("values").is_some_and(|v| !v.is_null()) {
        return Err(crate::ApiError::Query(
            fluree_db_query::QueryError::r2rml_unsupported_pattern(
                "a top-level `values` clause on a virtual-dataset (graph-source) subgraph \
                 crawl is not yet supported and would be silently dropped, returning the \
                 entire class. Put the subject constraint in the WHERE clause instead — \
                 e.g. `{\"select\": [\"?p\", \"?o\"], \"where\": {\"@id\": \"<subject-iri>\", \
                 \"?p\": \"?o\"}}` for a single subject.",
            ),
        ));
    }

    // Cluster B (D4): a constant-IRI select-map root routes onto the proven
    // bound-subject scan machinery (a dedicated single-subject expansion), not the
    // multi-subject var-crawl grouping below. Absent/unreversible subject →
    // native-parity `{"@id": <iri>}` stub.
    let subject_var = match root {
        CrawlRoot::Var(v) => v,
        CrawlRoot::Iri(iri) => {
            return expand_bound_subject_select_map(
                fluree,
                view,
                iri,
                where_clause,
                context,
                &projection,
                provider,
                table_provider,
                execution,
                format_config,
            )
            .await;
        }
    };
    // A variable-subject crawl always carries a WHERE (the `detect` gate requires
    // it) — it is what binds `?s`.
    let where_clause = where_clause.expect("a variable-subject crawl always has a WHERE");

    if offset > MAX_CRAWL_OFFSET_SUBJECTS {
        return Err(crate::ApiError::query(format!(
            "crawl offset {offset} exceeds the virtual-dataset paging ceiling \
             ({MAX_CRAWL_OFFSET_SUBJECTS}); narrow the query with a WHERE filter \
             instead of paging this deep"
        )));
    }

    // Rewrite the crawl into a flat scan: keep the original WHERE (it binds and
    // filters `?s`), then project the columns the projection needs. The flat scan
    // is LIMITed to bound work (an unbounded multi-scan join over a remote table
    // does not early-terminate); the subject LIMIT/OFFSET are re-applied exactly
    // after grouping. The flat triple budget must cover `offset + limit` subjects
    // (the OFFSET is on grouped subjects, not flat triples), so paging forward
    // fetches — and then skips — the earlier subjects.
    let flat_limit = limit.map(|n| {
        offset
            .saturating_add(n)
            .saturating_add(1)
            .saturating_mul(TRIPLES_PER_SUBJECT_BUDGET)
    });
    let flat_query = build_flat_query(subject_var, where_clause, context, flat_limit, &projection);

    // This is a browse ("View Instances") crawl: render R2RML RefObjectMap objects
    // by templating the parent IRI from the child row's own FK columns instead of
    // scanning every FK-parent table to verify referential integrity. The operator
    // applies this ONLY to the injected true-wildcard scan (`?s ?p ?o`), never to a
    // predicate-filtered ref used as a subject filter, so a `Predicates` crawl or a
    // ref-binding WHERE keeps the scan + dangling-FK semantics and its subject set.
    let execution = execution.with_trust_fk_refs(true);

    let result = fluree
        .query_view_with_r2rml_options(
            view,
            QueryInput::JsonLd(&flat_query),
            provider,
            table_provider,
            execution,
        )
        .await?;

    let Some(cols) = result.output.columns() else {
        return Ok(None);
    };
    let var_at = |i: usize| -> Option<VarId> {
        match cols.get(i) {
            Some(Column::Var(v)) => Some(*v),
            _ => None,
        }
    };
    // Column 0 is the subject in every crawl's select.
    let Some(s_var) = var_at(0) else {
        return Ok(None);
    };

    let compactor = IriCompactor::new(view.snapshot.shared_namespaces(), &result.context);

    // Group flat rows by subject IRI, preserving first-seen subject order. Type
    // IRIs and property keys are stored already-compacted; `@id` is compacted at
    // assembly time from the raw subject key.
    let mut order: Vec<String> = Vec::new();
    let mut subjects: HashMap<String, SubjectAcc> = HashMap::new();

    match &projection {
        CrawlProjection::IdOnly => {
            for batch in &result.batches {
                for row in 0..batch.len() {
                    let Some(subject_iri) = batch.get(row, s_var).and_then(Binding::get_iri) else {
                        continue;
                    };
                    let key = subject_iri.to_string();
                    subjects.entry(key.clone()).or_insert_with(|| {
                        order.push(key);
                        SubjectAcc::empty()
                    });
                }
            }
        }
        CrawlProjection::Wildcard => {
            // Columns: [?s, ?p, ?o].
            let (Some(p_var), Some(o_var)) = (var_at(1), var_at(2)) else {
                return Ok(None);
            };
            for batch in &result.batches {
                for row in 0..batch.len() {
                    let Some(subject_iri) = batch.get(row, s_var).and_then(Binding::get_iri) else {
                        continue;
                    };
                    let key = subject_iri.to_string();
                    let acc = subjects.entry(key.clone()).or_insert_with(|| {
                        order.push(key);
                        SubjectAcc::empty()
                    });
                    if let Some(pred_iri) = batch.get(row, p_var).and_then(Binding::get_iri) {
                        // The wildcard scan emits the subject's classes as
                        // `rr:class`-derived `rdf:type` rows (native parity);
                        // fold them into `@type` rather than rendering the
                        // class as an "rdf:type" property.
                        if &**pred_iri == fluree_vocab::rdf::TYPE {
                            if let Some(class_iri) =
                                batch.get(row, o_var).and_then(Binding::get_iri)
                            {
                                acc.add_type(compactor.compact_vocab_iri(class_iri));
                            }
                        } else if let Some(obj_binding) = batch.get(row, o_var) {
                            let value = format_node_object_binding(
                                &result,
                                obj_binding,
                                &compactor,
                                format_config,
                            )?;
                            acc.add_value(compactor.compact_vocab_iri(pred_iri), value);
                        }
                    }
                }
            }
        }
        CrawlProjection::Predicates {
            predicates,
            want_type,
            .. // want_id: a variable-subject crawl always emits @id (node identity).
        } => {
            // Columns: [?s, ?__crawl_o0, .., ?__crawl_o{n-1}, (?__crawl_type)?].
            let obj_vars: Vec<Option<VarId>> =
                (0..predicates.len()).map(|i| var_at(i + 1)).collect();
            let type_var = if *want_type {
                var_at(predicates.len() + 1)
            } else {
                None
            };
            for batch in &result.batches {
                for row in 0..batch.len() {
                    let Some(subject_iri) = batch.get(row, s_var).and_then(Binding::get_iri) else {
                        continue;
                    };
                    let key = subject_iri.to_string();
                    let acc = subjects.entry(key.clone()).or_insert_with(|| {
                        order.push(key);
                        SubjectAcc::empty()
                    });
                    for (i, ovar) in obj_vars.iter().enumerate() {
                        let Some(ovar) = ovar else { continue };
                        if let Some(obj_binding) = batch.get(row, *ovar) {
                            if !matches!(obj_binding, Binding::Unbound) {
                                let value = format_node_object_binding(
                                    &result,
                                    obj_binding,
                                    &compactor,
                                    format_config,
                                )?;
                                acc.add_value(predicates[i].clone(), value);
                            }
                        }
                    }
                    if let Some(tv) = type_var {
                        if let Some(type_iri) = batch.get(row, tv).and_then(Binding::get_iri) {
                            acc.add_type(compactor.compact_vocab_iri(type_iri));
                        }
                    }
                }
            }
        }
    }

    // Assemble per-subject JSON-LD documents, honoring the crawl's subject
    // OFFSET then LIMIT. Paging is BEST-EFFORT: subject order follows the scan's
    // first-seen order, which is deterministic for a stable table but is not
    // enforced across separately-executed requests (per-file reads run
    // concurrently, and a table can compact/append between pages), so a page
    // boundary can skip or repeat a subject when the underlying scan order
    // shifts. Fine for shallow browse paging; a real pagination surface wants
    // keyset/cursor paging instead of OFFSET.
    let normalize = format_config.normalize_arrays;
    let take = limit.unwrap_or(usize::MAX);
    let mut docs: Vec<JsonValue> = Vec::new();
    for key in order.into_iter().skip(offset).take(take) {
        let acc = subjects.remove(&key).expect("accumulated subject");
        let mut doc = Map::new();
        doc.insert("@id".to_string(), json!(compactor.compact_id_iri(&key)));
        if !acc.types.is_empty() {
            let types: Vec<JsonValue> = acc.types.into_iter().map(JsonValue::String).collect();
            doc.insert("@type".to_string(), collapse(types, normalize));
        }
        for (pred, values) in acc.props {
            doc.insert(pred, collapse(values, normalize));
        }
        docs.push(JsonValue::Object(doc));
    }

    Ok(Some(JsonValue::Array(docs)))
}

/// Cluster B (D4): expand a constant-IRI select-map root (`{"select": {"<iri>":
/// [...]}}`) over a graph source into the ONE subject's JSON-LD document.
///
/// Routes onto the proven bound-subject wildcard scan (`{"@id": <iri>, ?p: ?o}`),
/// which the R2RML operator prunes to the subject's OWN table via subject-template
/// reversal (`operator.rs` `subject_constant` prune) — the exact machinery the
/// where-clause bound-subject inspect uses. It runs that ONE pruned scan for every
/// projection shape (`["*"]`, `["@type"]`, a forward-predicate list) and applies
/// the requested projection to the assembled document, because for a single
/// subject the extra materialized columns are free AND this avoids the inner-join
/// drop a per-predicate scan would suffer when a requested predicate is absent (a
/// cross-join of independent bound-subject scans yields ZERO rows if any predicate
/// has no value). An absent / unreversible subject scans zero rows and returns the
/// native-parity `[{"@id": <iri>}]` stub — exactly what native hydration returns
/// for a genuinely-missing subject, so present and absent subjects both match
/// native. `@id` is always seeded (native's `format_subject` seeds it
/// unconditionally).
#[allow(clippy::too_many_arguments)]
async fn expand_bound_subject_select_map(
    fluree: &Fluree,
    view: &GraphDb,
    subject_iri: &str,
    where_clause: Option<&JsonValue>,
    context: Option<&JsonValue>,
    projection: &CrawlProjection,
    provider: &dyn fluree_db_query::r2rml::R2rmlProvider,
    table_provider: &dyn fluree_db_query::r2rml::R2rmlTableProvider,
    execution: QueryExecutionOptions,
    format_config: &FormatterConfig,
) -> Result<Option<JsonValue>> {
    // The single bound-subject wildcard scan. `@id` is a constant, so this lowers
    // to `subject_constant = Some` and hits the table + file prune (not a
    // fan-out). A caller WHERE (rare for a select-map) is preserved as an extra
    // constraint.
    let mut where_patterns: Vec<JsonValue> = where_clause.map(where_as_array).unwrap_or_default();
    where_patterns.push(json!({ "@id": subject_iri, CRAWL_PRED: CRAWL_OBJ }));
    let mut flat = Map::new();
    if let Some(ctx) = context {
        flat.insert("@context".to_string(), ctx.clone());
    }
    flat.insert("select".to_string(), json!([CRAWL_PRED, CRAWL_OBJ]));
    flat.insert("where".to_string(), JsonValue::Array(where_patterns));
    let flat_query = JsonValue::Object(flat);

    // Browse detail: template RefObjectMap parent IRIs from the child row's own FK
    // columns instead of scanning every FK-parent table (same as the var crawl).
    let execution = execution.with_trust_fk_refs(true);

    let result = fluree
        .query_view_with_r2rml_options(
            view,
            QueryInput::JsonLd(&flat_query),
            provider,
            table_provider,
            execution,
        )
        .await?;

    let Some(cols) = result.output.columns() else {
        return Ok(None);
    };
    let var_at = |i: usize| -> Option<VarId> {
        match cols.get(i) {
            Some(Column::Var(v)) => Some(*v),
            _ => None,
        }
    };
    // Columns: [?__crawl_p, ?__crawl_o] — NO subject column (the subject is the
    // constant `subject_iri`; every scanned row belongs to it).
    let (Some(p_var), Some(o_var)) = (var_at(0), var_at(1)) else {
        return Ok(None);
    };

    let compactor = IriCompactor::new(view.snapshot.shared_namespaces(), &result.context);

    // Accumulate the ONE subject's rows. Types are compacted here (emitted as-is);
    // property keys are kept as their RAW full IRI so a forward-predicate-list
    // projection can match a requested term against either the full IRI or its
    // compacted form (below) — the compactor only compacts, so full-IRI keys keep
    // matching unambiguous.
    let mut acc = SubjectAcc::empty();
    for batch in &result.batches {
        for row in 0..batch.len() {
            let Some(pred_iri) = batch.get(row, p_var).and_then(Binding::get_iri) else {
                continue;
            };
            if &**pred_iri == fluree_vocab::rdf::TYPE {
                if let Some(class_iri) = batch.get(row, o_var).and_then(Binding::get_iri) {
                    acc.add_type(compactor.compact_vocab_iri(class_iri));
                }
            } else if let Some(obj_binding) = batch.get(row, o_var) {
                let value =
                    format_node_object_binding(&result, obj_binding, &compactor, format_config)?;
                acc.add_value(pred_iri.to_string(), value);
            }
        }
    }

    // Assemble the single document, applying the requested projection. Nothing
    // accumulated (absent subject) → the `{"@id": <iri>}` stub.
    let normalize = format_config.normalize_arrays;
    let mut doc = Map::new();
    // `@id` is emitted for the wildcard and id-only forms, and for a
    // forward-predicate list ONLY when `@id` was explicitly requested — native
    // omits `@id` from an explicit projection that does not ask for it (the single
    // subject is already known), e.g. `["@type"]` → `{"@type": …}` (no `@id`).
    let include_id = !matches!(
        projection,
        CrawlProjection::Predicates { want_id: false, .. }
    );
    if include_id {
        doc.insert(
            "@id".to_string(),
            json!(compactor.compact_id_iri(subject_iri)),
        );
    }
    let keep_types = matches!(
        projection,
        CrawlProjection::Wildcard
            | CrawlProjection::Predicates {
                want_type: true,
                ..
            }
    );
    if keep_types && !acc.types.is_empty() {
        let types: Vec<JsonValue> = acc.types.into_iter().map(JsonValue::String).collect();
        doc.insert("@type".to_string(), collapse(types, normalize));
    }
    match projection {
        // `["*"]`: every property, keyed by its compacted (`@vocab`/context) form.
        CrawlProjection::Wildcard => {
            for (full_iri, values) in acc.props {
                doc.insert(
                    compactor.compact_vocab_iri(&full_iri),
                    collapse(values, normalize),
                );
            }
        }
        // `["@id"]` / `["@type"]`: no forward properties.
        CrawlProjection::IdOnly => {}
        // Forward-predicate list: emit ONLY the requested predicates, in requested
        // order, under the user's own term (native keys the projection this way).
        // Match a requested term against a property's full IRI OR its compacted
        // form, so both `["<full-iri>"]` and `["prefix:name"]` requests resolve.
        CrawlProjection::Predicates { predicates, .. } => {
            for pred in predicates {
                if let Some((_, values)) = acc.props.iter().find(|(full_iri, _)| {
                    full_iri == pred || &compactor.compact_vocab_iri(full_iri) == pred
                }) {
                    doc.insert(pred.clone(), collapse(values.clone(), normalize));
                }
            }
        }
    }
    Ok(Some(JsonValue::Array(vec![JsonValue::Object(doc)])))
}

/// Master kill-switch for expanding a subgraph crawl over a graph source through
/// the R2RML operator. Default **on**. Set `FLUREE_R2RML_CRAWL_EXPAND=0` (or
/// `false`/`off`) to restore native binary-index hydration — which returns `[]`
/// for a virtual dataset (the pre-fix behavior), so this is a safety escape
/// hatch, not a normal setting.
fn crawl_expand_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        // Master switch (default on).
        let expand = env_flag_enabled("FLUREE_R2RML_CRAWL_EXPAND");
        // Coupling: expand-on + class-fusion-off routes a browse through the
        // UNFUSED crawl (a full TriplesMap fan-out + shared-catalog 429 storm —
        // strictly worse than the pre-fix fast empty result). So when the
        // rewriter's class fusion (`FLUREE_R2RML_CRAWL_CLASS_FUSION`) is
        // explicitly disabled, force expansion off too, falling back to native
        // hydration (`[]` for a virtual dataset).
        let class_fusion = env_flag_enabled("FLUREE_R2RML_CRAWL_CLASS_FUSION");
        expand && class_fusion
    })
}

/// Read an on/off environment flag that defaults to **on**. Only `0`, `false`,
/// `off`, or `no` (case-insensitive) disable it.
fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(true)
}

/// Cluster B (D4): whether a constant-IRI select-map root (`{"select": {"<iri>":
/// [...]}}`) over a graph source is routed onto the bound-subject crawl machinery
/// (template-reversal → keyed pruned scan → node-document assembly) instead of
/// native binary-index hydration — which has no flakes for a virtual dataset and
/// returns the bare `{"@id": <iri>}` stub for an EXISTING subject (the deployed
/// "subject detail is empty" bug). Default **on**. Set
/// `FLUREE_R2RML_SELECT_MAP_ROUTING=0` (or `false`/`off`/`no`) to restore the
/// pre-fix stub behavior — an insurance switch, since this re-routes an existing
/// (shipping-but-broken) path rather than adding a new one. Read once, cached.
fn select_map_routing_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| env_flag_enabled("FLUREE_R2RML_SELECT_MAP_ROUTING"))
}

/// Cheap check: does `input` look like a subgraph crawl this module expands?
/// Used by the query terminals to skip the (single-ledger) crawl-routing work
/// for ordinary queries. Equivalent to `detect_wildcard_crawl(input).is_some()`.
pub(crate) fn is_wildcard_crawl(input: &JsonValue) -> bool {
    detect_wildcard_crawl(input).is_some()
}

/// Interception entry point used by **every** formatting terminal (the
/// graph-source alias path *and* the ledger-scoped / dataset / connection paths).
///
/// If `json` is a subgraph "crawl" projection over a graph-source-backed `view`,
/// expand it through the R2RML operator and return the per-subject JSON-LD
/// documents. Returns `Ok(None)` — so the caller falls back to its normal
/// (native) formatting — when: the kill-switch is off, there is no R2RML
/// provider, the input is not JSON-LD, the view is not graph-source-backed
/// (`graph_source_id` is `None`, i.e. a genuinely native ledger), or the crawl
/// shape is not one this module handles. This is what makes the Solo virtual
/// dataset "View Instances" screen return data instead of `[]`.
pub(crate) async fn maybe_expand_crawl(
    fluree: &Fluree,
    view: &GraphDb,
    json: Option<&JsonValue>,
    r2rml: Option<(
        &dyn fluree_db_query::r2rml::R2rmlProvider,
        &dyn fluree_db_query::r2rml::R2rmlTableProvider,
    )>,
    execution: QueryExecutionOptions,
    format_config: &FormatterConfig,
) -> Result<Option<JsonValue>> {
    if !crawl_expand_enabled() {
        return Ok(None);
    }
    // Native ledgers (no graph source) hydrate against their binary index as
    // before — this gate is the load-bearing guard that keeps native crawls,
    // and any non-graph-source view, on their existing path.
    if view.graph_source_id.is_none() {
        return Ok(None);
    }
    let (Some((provider, table_provider)), Some(json)) = (r2rml, json) else {
        return Ok(None);
    };
    expand_wildcard_crawl(
        fluree,
        view,
        json,
        provider,
        table_provider,
        execution,
        format_config,
    )
    .await
}

/// Recognize a single-column subgraph crawl `{"select": {"?s": [...]}, ...}` in
/// one of the handled projection shapes (`["*"]`, `["@id"]`, or an explicit
/// predicate list). Returns `None` for any other shape (which then falls back to
/// the normal formatter).
fn detect_wildcard_crawl(input: &JsonValue) -> Option<DetectedCrawl<'_>> {
    detect_wildcard_crawl_inner(input, select_map_routing_enabled())
}

/// Inner detection with the Cluster-B select-map switch passed explicitly, so
/// tests can exercise both the ON and OFF branch without racing the process-wide
/// `OnceLock` env cache.
fn detect_wildcard_crawl_inner(
    input: &JsonValue,
    select_map_routing: bool,
) -> Option<DetectedCrawl<'_>> {
    let obj = input.as_object()?;
    let select = obj.get("select")?.as_object()?;
    if select.len() != 1 {
        return None;
    }
    let (root_key, spec) = select.iter().next()?;
    let projection = classify_projection(spec.as_array()?)?;
    let limit = obj
        .get("limit")
        .and_then(JsonValue::as_u64)
        .map(|n| n as usize);
    let offset = obj
        .get("offset")
        .and_then(JsonValue::as_u64)
        .map(|n| n as usize)
        .unwrap_or(0);
    let context = obj.get("@context");

    if root_key.starts_with('?') {
        // Variable-subject crawl: the WHERE binds and filters `?s` — REQUIRED
        // (a bare `{"select": {"?s": ["*"]}}` has nothing to bind `?s`).
        Some(DetectedCrawl {
            root: CrawlRoot::Var(root_key.as_str()),
            where_clause: Some(obj.get("where")?),
            context,
            limit,
            offset,
            projection,
        })
    } else if select_map_routing {
        // Cluster B (D4): a constant-IRI root select-map (`{"<iri>": [...]}`). The
        // subject is already bound by the constant, so a WHERE is OPTIONAL
        // (typically absent). Behind `FLUREE_R2RML_SELECT_MAP_ROUTING` (default
        // on); OFF ⇒ `None` here, so the query falls through to native hydration
        // (today's `{"@id": <iri>}` stub) unchanged.
        Some(DetectedCrawl {
            root: CrawlRoot::Iri(root_key.as_str()),
            where_clause: obj.get("where"),
            context,
            limit,
            offset,
            projection,
        })
    } else {
        None
    }
}

/// Classify a crawl's selection array into a [`CrawlProjection`]. Returns `None`
/// for shapes this module does not expand (empty, a nested ref-crawl object, or
/// an unsupported JSON-LD keyword), so the caller falls back to normal
/// formatting.
fn classify_projection(spec: &[JsonValue]) -> Option<CrawlProjection> {
    if spec.is_empty() {
        return None;
    }
    // Any `"*"` entry means the full wildcard shape.
    if spec.iter().any(|v| v.as_str() == Some("*")) {
        return Some(CrawlProjection::Wildcard);
    }
    let mut predicates: Vec<String> = Vec::new();
    let mut want_type = false;
    let mut want_id = false;
    for entry in spec {
        // Only string terms are handled; a nested ref-crawl (object) falls back.
        let key = entry.as_str()?;
        match key {
            "@id" => want_id = true, // explicit @id request; needs no scan.
            "@type" => want_type = true,
            // Any other JSON-LD keyword (`@graph`, ...) isn't a forward
            // predicate — fall back rather than mis-scan.
            _ if key.starts_with('@') => return None,
            _ => predicates.push(key.to_string()),
        }
    }
    if predicates.is_empty() && !want_type {
        // The selection was exactly `["@id"]` (id-only, cheapest).
        Some(CrawlProjection::IdOnly)
    } else {
        Some(CrawlProjection::Predicates {
            predicates,
            want_type,
            want_id,
        })
    }
}

/// Normalize a WHERE clause into a pattern vector (a single-object WHERE is
/// wrapped) so injected scan patterns can be appended.
fn where_as_array(where_clause: &JsonValue) -> Vec<JsonValue> {
    match where_clause {
        JsonValue::Array(patterns) => patterns.clone(),
        other => vec![other.clone()],
    }
}

/// Build the flat scan query for a crawl: the original WHERE (binds/filters
/// `?s`) plus the scan patterns the projection needs, and a matching select.
///
/// - [`CrawlProjection::Wildcard`]: `?s ?p ?o` (every predicate/object) + `?s a
///   ?type` (declared classes).
/// - [`CrawlProjection::IdOnly`]: no injected scan — just project `?s`, so the
///   WHERE's own class/predicate scan binds the subject and nothing else runs.
/// - [`CrawlProjection::Predicates`]: one `?s <p_i> ?__crawl_o{i}` per predicate
///   with a DISTINCT object var (so they star-collapse into one scan), plus an
///   optional `?s a ?__crawl_type`.
fn build_flat_query(
    subject_var: &str,
    where_clause: &JsonValue,
    context: Option<&JsonValue>,
    flat_limit: Option<usize>,
    projection: &CrawlProjection,
) -> JsonValue {
    let mut where_patterns = where_as_array(where_clause);
    let select: Vec<JsonValue> = match projection {
        CrawlProjection::Wildcard => {
            // `?s ?__crawl_p ?__crawl_o` — every (predicate, object) of `?s`.
            // The subject's declared class(es) arrive on this SAME scan as
            // `rr:class`-derived `rdf:type` rows (the operator emits them for a
            // true wildcard; the regroup folds `?p == rdf:type` into `@type`).
            // No separate `?s a ?type` pattern is injected: a standalone
            // type-var scan was a REQUIRED join (inner-joining out subjects
            // whose TriplesMap declares no `rr:class`) and, when not fused,
            // the topmost budgeted scan (starving the wildcard of the LIMIT
            // budget). One scan does it all.
            where_patterns.push(json!({ "@id": subject_var, CRAWL_PRED: CRAWL_OBJ }));
            vec![json!(subject_var), json!(CRAWL_PRED), json!(CRAWL_OBJ)]
        }
        CrawlProjection::IdOnly => vec![json!(subject_var)],
        CrawlProjection::Predicates {
            predicates,
            want_type,
            .. // want_id: a variable-subject crawl always emits @id (node identity).
        } => {
            let mut select = vec![json!(subject_var)];
            for (i, pred) in predicates.iter().enumerate() {
                let obj_var = crawl_obj_var(i);
                // Build `{"@id": ?s, "<pred>": "?__crawl_o{i}"}` with the
                // predicate as a dynamic key (json! needs literal keys).
                let mut pat = Map::new();
                pat.insert("@id".to_string(), json!(subject_var));
                pat.insert(pred.clone(), json!(obj_var));
                where_patterns.push(JsonValue::Object(pat));
                select.push(json!(obj_var));
            }
            if *want_type {
                where_patterns.push(json!({ "@id": subject_var, "@type": CRAWL_TYPE }));
                select.push(json!(CRAWL_TYPE));
            }
            select
        }
    };

    let mut query = Map::new();
    if let Some(ctx) = context {
        query.insert("@context".to_string(), ctx.clone());
    }
    query.insert("select".to_string(), JsonValue::Array(select));
    query.insert("where".to_string(), JsonValue::Array(where_patterns));
    if let Some(n) = flat_limit {
        query.insert("limit".to_string(), json!(n));
    }
    JsonValue::Object(query)
}

/// A single value renders bare (unless array-normalization is on); multiple
/// values always render as a JSON array.
fn collapse(mut values: Vec<JsonValue>, normalize: bool) -> JsonValue {
    if !normalize && values.len() == 1 {
        values.pop().expect("len == 1")
    } else {
        JsonValue::Array(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_wildcard_crawl_and_extracts_parts() {
        let input = json!({
            "@context": {"v": "http://example/org/ns"},
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "v:Geography"},
            "limit": 3,
            "offset": 6
        });
        let DetectedCrawl {
            root,
            where_clause,
            context,
            limit,
            offset,
            projection,
        } = detect_wildcard_crawl(&input).expect("wildcard crawl");
        assert!(matches!(root, CrawlRoot::Var("?s")));
        assert_eq!(limit, Some(3));
        assert_eq!(offset, 6);
        assert!(context.is_some());
        assert_eq!(
            where_clause,
            Some(&json!({"@id": "?s", "@type": "v:Geography"}))
        );
        assert!(matches!(projection, CrawlProjection::Wildcard));
        assert!(is_wildcard_crawl(&input));
    }

    #[test]
    fn detects_constant_iri_select_map_root() {
        // Cluster B (D4): a constant-IRI root (`{"select": {"<iri>": [...]}}`, NO
        // where) is recognized as a bound-subject select-map when routing is ON.
        let detail = json!({"select": {"http://example.org/customer/1": ["*"]}});
        let d = detect_wildcard_crawl_inner(&detail, true).expect("constant-IRI root recognized");
        assert!(matches!(
            d.root,
            CrawlRoot::Iri("http://example.org/customer/1")
        ));
        assert!(
            d.where_clause.is_none(),
            "a constant-IRI root needs no WHERE"
        );
        assert!(matches!(d.projection, CrawlProjection::Wildcard));
        assert!(is_wildcard_crawl(&detail));
        // `["@type"]` and a forward-predicate list are also recognized.
        assert!(matches!(
            detect_wildcard_crawl_inner(&json!({"select": {"ex:s": ["@type"]}}), true)
                .map(|d| d.projection),
            Some(CrawlProjection::Predicates {
                want_type: true,
                ..
            })
        ));
        assert!(matches!(
            detect_wildcard_crawl_inner(&json!({"select": {"ex:s": ["ex:name", "ex:age"]}}), true)
                .map(|d| d.projection),
            Some(CrawlProjection::Predicates {
                want_type: false,
                ..
            })
        ));
        // Switch OFF → NOT recognized: the query falls through to native
        // hydration (today's `{"@id": <iri>}` stub) unchanged.
        assert!(detect_wildcard_crawl_inner(&detail, false).is_none());
    }

    #[test]
    fn detects_id_only_and_predicate_crawls() {
        // `["@id"]` — id-only (cheapest).
        let id_only = json!({"select": {"?s": ["@id"]}, "where": {"@id": "?s", "@type": "v:C"}});
        let projection = detect_wildcard_crawl(&id_only)
            .expect("id-only crawl")
            .projection;
        assert!(matches!(projection, CrawlProjection::IdOnly));
        assert!(is_wildcard_crawl(&id_only));

        // Explicit forward-predicate list — now a recognized crawl (FIX 4).
        let preds = json!({
            "select": {"?s": ["@id", "v:name", "v:age"]},
            "where": {"@id": "?s", "@type": "v:C"}
        });
        let projection = detect_wildcard_crawl(&preds)
            .expect("predicate crawl")
            .projection;
        match projection {
            CrawlProjection::Predicates {
                predicates,
                want_type,
                ..
            } => {
                assert_eq!(predicates, vec!["v:name".to_string(), "v:age".to_string()]);
                assert!(!want_type);
            }
            other => panic!("expected Predicates, got {other:?}"),
        }

        // A predicate list that also asks for `@type`.
        let with_type = json!({"select": {"?s": ["v:name", "@type"]}, "where": {"@id": "?s"}});
        let projection = detect_wildcard_crawl(&with_type)
            .expect("predicate+type crawl")
            .projection;
        assert!(matches!(projection, CrawlProjection::Predicates { want_type, .. } if want_type));
    }

    #[test]
    fn falls_back_for_non_crawl_shapes() {
        // Flat select (select is an array, not a subject→projection map).
        assert!(detect_wildcard_crawl(&json!({"select": ["?s"], "where": {}})).is_none());
        // Multi-column projection.
        assert!(
            detect_wildcard_crawl(&json!({"select": {"?s": ["*"], "?x": ["*"]}, "where": {}}))
                .is_none()
        );
        // Missing where on a VARIABLE-subject crawl (nothing binds `?s`).
        assert!(detect_wildcard_crawl(&json!({"select": {"?s": ["*"]}})).is_none());
        // Constant-IRI root falls back when select-map routing is OFF (the query
        // then hits native hydration and returns the `{"@id": <iri>}` stub). The
        // ON case is covered by `detects_constant_iri_select_map_root`.
        assert!(detect_wildcard_crawl_inner(&json!({"select": {"ex:s": ["*"]}}), false).is_none());
        // Empty projection list.
        assert!(detect_wildcard_crawl(&json!({"select": {"?s": []}, "where": {}})).is_none());
        // Unsupported JSON-LD keyword in the list.
        assert!(
            detect_wildcard_crawl(&json!({"select": {"?s": ["@graph"]}, "where": {}})).is_none()
        );
    }

    #[test]
    fn flat_query_injects_single_wildcard_scan() {
        let context = json!({"v": "http://example/org/ns"});
        let where_clause = json!({"@id": "?s", "@type": "v:Geography"});
        let flat = build_flat_query(
            "?s",
            &where_clause,
            Some(&context),
            Some(256),
            &CrawlProjection::Wildcard,
        );

        assert_eq!(flat["select"], json!(["?s", CRAWL_PRED, CRAWL_OBJ]));
        assert_eq!(flat["@context"], context);
        assert_eq!(flat["limit"], json!(256));
        // where = [ original, wildcard(?s ?p ?o) ] — ONE injected scan. No
        // separate `?s a ?type` pattern: the class arrives on the wildcard as
        // `rdf:type` rows (a standalone type-var scan was a REQUIRED join that
        // dropped class-less subjects and, unfused, starved the LIMIT budget).
        let patterns = flat["where"].as_array().expect("where array");
        assert_eq!(patterns.len(), 2);
        assert_eq!(patterns[0], where_clause);
        assert_eq!(patterns[1], json!({"@id": "?s", CRAWL_PRED: CRAWL_OBJ}));
    }

    #[test]
    fn flat_query_wraps_array_where() {
        let where_clause = json!([{"@id": "?s", "v:country": "?c"}]);
        let flat = build_flat_query("?s", &where_clause, None, None, &CrawlProjection::Wildcard);
        let patterns = flat["where"].as_array().expect("where array");
        // Original single pattern + the one injected wildcard scan.
        assert_eq!(patterns.len(), 2);
        assert_eq!(patterns[0], json!({"@id": "?s", "v:country": "?c"}));
        assert!(flat.get("@context").is_none());
        assert!(flat.get("limit").is_none());
    }

    #[test]
    fn flat_query_id_only_projects_subject_alone() {
        let where_clause = json!({"@id": "?s", "@type": "v:C"});
        let flat = build_flat_query("?s", &where_clause, None, None, &CrawlProjection::IdOnly);
        // Select is just the subject; no scan patterns injected beyond the WHERE.
        assert_eq!(flat["select"], json!(["?s"]));
        let patterns = flat["where"].as_array().expect("where array");
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0], where_clause);
    }

    #[test]
    fn flat_query_predicates_uses_distinct_object_vars() {
        let where_clause = json!({"@id": "?s", "@type": "v:C"});
        let projection = CrawlProjection::Predicates {
            predicates: vec!["v:name".to_string(), "v:age".to_string()],
            want_type: true,
            want_id: false,
        };
        let flat = build_flat_query("?s", &where_clause, None, None, &projection);
        // Select: subject, one distinct object var per predicate, then type.
        assert_eq!(
            flat["select"],
            json!(["?s", "?__crawl_o0", "?__crawl_o1", CRAWL_TYPE])
        );
        let patterns = flat["where"].as_array().expect("where array");
        // original + p0 + p1 + type
        assert_eq!(patterns.len(), 4);
        assert_eq!(patterns[1], json!({"@id": "?s", "v:name": "?__crawl_o0"}));
        assert_eq!(patterns[2], json!({"@id": "?s", "v:age": "?__crawl_o1"}));
        assert_eq!(patterns[3], json!({"@id": "?s", "@type": CRAWL_TYPE}));
    }

    #[test]
    fn collapse_unwraps_single_unless_normalized() {
        assert_eq!(collapse(vec![json!("x")], false), json!("x"));
        assert_eq!(collapse(vec![json!("x")], true), json!(["x"]));
        assert_eq!(
            collapse(vec![json!("x"), json!("y")], false),
            json!(["x", "y"])
        );
    }
}

/// End-to-end crawl tests driving the FULL crawl (build flat query → R2RML
/// operator + rewrite fusion → group → JSON-LD docs) against an in-crate mock
/// R2RML provider — no live catalog. Exercises FIX 1/2/4 together: routing,
/// class-fusion scan pruning, the vertical-partition guard, and the id-only /
/// limit / multi-class shapes.
#[cfg(test)]
mod e2e {
    use super::*;
    use async_trait::async_trait;
    use fluree_db_iceberg::io::batch::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
    use fluree_db_query::error::Result as QueryResult;
    use fluree_db_query::r2rml::{
        ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanFilter,
    };
    use fluree_db_r2rml::mapping::{
        CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap, TriplesMap,
    };
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::{FlureeBuilder, LedgerState, Novelty};
    use fluree_db_core::LedgerSnapshot;

    /// Mock provider: one compiled mapping + per-table batches, recording every
    /// scanned table name so tests can assert TriplesMap fan-out was pruned.
    #[derive(Debug)]
    struct MockCrawlProvider {
        mapping: Arc<CompiledR2rmlMapping>,
        tables: HashMap<String, Vec<ColumnBatch>>,
        scanned: Mutex<Vec<String>>,
    }

    impl MockCrawlProvider {
        fn new(mapping: CompiledR2rmlMapping, tables: HashMap<String, Vec<ColumnBatch>>) -> Self {
            Self {
                mapping: Arc::new(mapping),
                tables,
                scanned: Mutex::new(Vec::new()),
            }
        }
        fn scanned_tables(&self) -> Vec<String> {
            let mut v = self.scanned.lock().unwrap().clone();
            v.sort();
            v.dedup();
            v
        }
    }

    #[async_trait]
    impl R2rmlProvider for MockCrawlProvider {
        async fn has_r2rml_mapping(&self, _gs: &str) -> bool {
            true
        }
        async fn compiled_mapping(
            &self,
            _gs: &str,
            _as_of_t: Option<i64>,
        ) -> QueryResult<Arc<CompiledR2rmlMapping>> {
            Ok(Arc::clone(&self.mapping))
        }
    }

    #[async_trait]
    impl R2rmlTableProvider for MockCrawlProvider {
        async fn scan_table(
            &self,
            _gs: &str,
            table: &str,
            _projection: &[String],
            _filters: &[ScanFilter],
            _topk: Option<&fluree_db_query::r2rml::ScanTopK>,
            _as_of_t: Option<i64>,
        ) -> QueryResult<ColumnBatchStream> {
            self.scanned.lock().unwrap().push(table.to_string());
            let batches = self.tables.get(table).cloned().unwrap_or_default();
            use futures::StreamExt;
            Ok(Box::pin(futures::stream::iter(batches).map(Ok)))
        }
    }

    /// A `TriplesMap`: table + subject template + one class + one string POM.
    fn tm(
        iri: &str,
        table: &str,
        template: &str,
        class: &str,
        pred: &str,
        col: &str,
    ) -> TriplesMap {
        TriplesMap::new(iri, table)
            .with_subject_template(template)
            .with_class(class)
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant(pred),
                object_map: ObjectMap::column(col),
            })
    }

    /// One batch with an `id` (Int64) column and one nullable String column.
    fn id_str_batch(col: &str, ids: &[i64], vals: &[&str]) -> ColumnBatch {
        let schema = BatchSchema::new(vec![
            FieldInfo {
                name: "id".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                field_id: 1,
            },
            FieldInfo {
                name: col.to_string(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 2,
            },
        ]);
        ColumnBatch::new(
            Arc::new(schema),
            vec![
                Column::Int64(ids.iter().map(|i| Some(*i)).collect()),
                Column::String(vals.iter().map(|s| Some((*s).to_string())).collect()),
            ],
        )
        .unwrap()
    }

    /// A genesis graph-source view with the `example.org` namespace registered.
    /// Returns the backing ledger too so its snapshot Arc stays alive.
    fn genesis_view() -> (LedgerState, GraphDb) {
        let snapshot = LedgerSnapshot::genesis("crawl-e2e:main");
        let ledger = LedgerState::new(snapshot, Novelty::new(0));
        let mut view = GraphDb::from_ledger_state(&ledger);
        Arc::make_mut(&mut view.snapshot)
            .insert_namespace_code(9_999, "http://example.org/".to_string())
            .unwrap();
        view.graph_source_id = Some("crawl-e2e:main".into());
        (ledger, view)
    }

    async fn run_crawl(
        provider: &MockCrawlProvider,
        view: &GraphDb,
        crawl: &JsonValue,
    ) -> Vec<JsonValue> {
        run_crawl_cfg(provider, view, crawl, &FormatterConfig::default()).await
    }

    /// Like [`run_crawl`] but with an explicit formatter config, so tests can
    /// exercise the crawl object formatter across output formats (typed-json).
    async fn run_crawl_cfg(
        provider: &MockCrawlProvider,
        view: &GraphDb,
        crawl: &JsonValue,
        config: &FormatterConfig,
    ) -> Vec<JsonValue> {
        let fluree = FlureeBuilder::memory().build_memory();
        expand_wildcard_crawl(
            &fluree,
            view,
            crawl,
            provider,
            provider,
            QueryExecutionOptions::new(),
            config,
        )
        .await
        .expect("crawl expansion succeeds")
        .expect("crawl shape is handled")
        .as_array()
        .expect("crawl returns a JSON array")
        .clone()
    }

    fn two_table_provider() -> MockCrawlProvider {
        let mapping = CompiledR2rmlMapping::new(vec![
            tm(
                "#People",
                "people",
                "http://example.org/person/{id}",
                "http://example.org/Person",
                "http://example.org/name",
                "name",
            ),
            tm(
                "#Orders",
                "orders",
                "http://example.org/order/{id}",
                "http://example.org/Order",
                "http://example.org/label",
                "label",
            ),
        ]);
        let mut tables = HashMap::new();
        tables.insert(
            "people".to_string(),
            vec![id_str_batch("name", &[1, 2], &["Alice", "Bob"])],
        );
        tables.insert(
            "orders".to_string(),
            vec![id_str_batch("label", &[10, 11], &["O-10", "O-11"])],
        );
        MockCrawlProvider::new(mapping, tables)
    }

    fn person_crawl(projection: JsonValue, limit: Option<u64>) -> JsonValue {
        let mut q = serde_json::Map::new();
        q.insert("@context".into(), json!({"v": "http://example.org/"}));
        q.insert("select".into(), json!({"?s": projection}));
        q.insert("where".into(), json!({"@id": "?s", "@type": "v:Person"}));
        if let Some(n) = limit {
            q.insert("limit".into(), json!(n));
        }
        JsonValue::Object(q)
    }

    fn ids(docs: &[JsonValue]) -> std::collections::BTreeSet<String> {
        docs.iter()
            .filter_map(|d| d.get("@id").and_then(|v| v.as_str()).map(str::to_string))
            .collect()
    }

    // (a) A wildcard `["*"]` crawl returns the SAME subjects as an `["@id"]` crawl.
    #[tokio::test]
    async fn crawl_wildcard_subjects_match_id_only() {
        let provider = two_table_provider();
        let (_ledger, view) = genesis_view();
        let wildcard = run_crawl(&provider, &view, &person_crawl(json!(["*"]), None)).await;
        let id_only = run_crawl(&provider, &view, &person_crawl(json!(["@id"]), None)).await;
        assert_eq!(wildcard.len(), 2, "two Person instances");
        assert_eq!(ids(&wildcard), ids(&id_only), "same subject set both ways");
        assert!(
            ids(&wildcard).iter().all(|s| !s.contains("order")),
            "only Person (people) subjects, never Order subjects"
        );
    }

    // (b) `["@id"]` returns ids (each doc is exactly `{"@id": ...}`), not `[]`.
    #[tokio::test]
    async fn crawl_id_only_returns_ids_not_empty() {
        let provider = two_table_provider();
        let (_ledger, view) = genesis_view();
        let docs = run_crawl(&provider, &view, &person_crawl(json!(["@id"]), None)).await;
        assert!(!docs.is_empty(), "id-only crawl must return ids, not []");
        for d in &docs {
            let obj = d.as_object().expect("doc is an object");
            assert!(obj.contains_key("@id"));
            assert_eq!(obj.len(), 1, "id-only doc carries @id only: {obj:?}");
        }
    }

    // (c) A one-class `["*"]` crawl over a multi-TriplesMap mapping scans ONLY the
    //     queried class's table (fusion prunes the fan-out).
    #[tokio::test]
    async fn crawl_wildcard_scans_only_class_table() {
        let provider = two_table_provider();
        let (_ledger, view) = genesis_view();
        let _ = run_crawl(&provider, &view, &person_crawl(json!(["*"]), None)).await;
        assert_eq!(
            provider.scanned_tables(),
            vec!["people".to_string()],
            "class fusion must prune the scan to the Person table only"
        );
    }

    // (d) A 2nd TriplesMap sharing the subject template but lacking the class
    //     forces the guard to REFUSE fusion, so the wildcard still returns that
    //     map's triples (no silent under-fetch).
    #[tokio::test]
    async fn crawl_wildcard_vertical_partition_returns_second_map() {
        let mapping = CompiledR2rmlMapping::new(vec![
            tm(
                "#PersonClass",
                "people",
                "http://example.org/person/{id}",
                "http://example.org/Person",
                "http://example.org/name",
                "name",
            ),
            // Same subject template, NO class, a distinct predicate/table.
            TriplesMap::new("#PersonEmail", "people_email")
                .with_subject_template("http://example.org/person/{id}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://example.org/email"),
                    object_map: ObjectMap::column("email"),
                }),
        ]);
        let mut tables = HashMap::new();
        tables.insert(
            "people".to_string(),
            vec![id_str_batch("name", &[1], &["Alice"])],
        );
        tables.insert(
            "people_email".to_string(),
            vec![id_str_batch("email", &[1], &["alice@example.org"])],
        );
        let provider = MockCrawlProvider::new(mapping, tables);
        let (_ledger, view) = genesis_view();
        let docs = run_crawl(&provider, &view, &person_crawl(json!(["*"]), None)).await;
        assert_eq!(docs.len(), 1);
        let serialized = serde_json::to_string(&docs).unwrap();
        assert!(
            serialized.contains("alice@example.org"),
            "vertical-partition guard must keep the classless map's email triple: {serialized}"
        );
        assert!(
            provider
                .scanned_tables()
                .contains(&"people_email".to_string()),
            "the classless second table must still be scanned"
        );
    }

    // (e) A multi-class subject's `@type` includes ALL declared classes.
    #[tokio::test]
    async fn crawl_wildcard_multi_class_type_complete() {
        let mapping = CompiledR2rmlMapping::new(vec![TriplesMap::new("#PA", "people")
            .with_subject_template("http://example.org/person/{id}")
            .with_class("http://example.org/Person")
            .with_class("http://example.org/Agent")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://example.org/name"),
                object_map: ObjectMap::column("name"),
            })]);
        let mut tables = HashMap::new();
        tables.insert(
            "people".to_string(),
            vec![id_str_batch("name", &[1], &["Alice"])],
        );
        let provider = MockCrawlProvider::new(mapping, tables);
        let (_ledger, view) = genesis_view();
        let docs = run_crawl(&provider, &view, &person_crawl(json!(["*"]), None)).await;
        assert_eq!(docs.len(), 1);
        let types = docs[0].get("@type").expect("has @type");
        let type_list = types
            .as_array()
            .cloned()
            .unwrap_or_else(|| vec![types.clone()]);
        assert_eq!(
            type_list.len(),
            2,
            "class-constrained type-var must bind BOTH declared classes: {type_list:?}"
        );
    }

    // (f) A LIMIT k crawl returns exactly k subjects.
    #[tokio::test]
    async fn crawl_wildcard_limit_returns_exactly_k() {
        let mapping = CompiledR2rmlMapping::new(vec![tm(
            "#People",
            "people",
            "http://example.org/person/{id}",
            "http://example.org/Person",
            "http://example.org/name",
            "name",
        )]);
        let mut tables = HashMap::new();
        tables.insert(
            "people".to_string(),
            vec![id_str_batch("name", &[1, 2, 3], &["Alice", "Bob", "Cara"])],
        );
        let provider = MockCrawlProvider::new(mapping, tables);
        let (_ledger, view) = genesis_view();
        let docs = run_crawl(&provider, &view, &person_crawl(json!(["*"]), Some(2))).await;
        assert_eq!(docs.len(), 2, "LIMIT 2 must return exactly 2 subjects");
    }

    // (g) LIMIT + OFFSET paginates over subjects: page 2 is the NEXT subjects, not
    //     page 1 again, and an offset past the end returns [].
    #[tokio::test]
    async fn crawl_wildcard_offset_paginates_subjects() {
        let mapping = CompiledR2rmlMapping::new(vec![tm(
            "#People",
            "people",
            "http://example.org/person/{id}",
            "http://example.org/Person",
            "http://example.org/name",
            "name",
        )]);
        let mut tables = HashMap::new();
        tables.insert(
            "people".to_string(),
            vec![id_str_batch("name", &[1, 2, 3], &["Alice", "Bob", "Cara"])],
        );
        let provider = MockCrawlProvider::new(mapping, tables);
        let (_ledger, view) = genesis_view();

        let paged = |limit: u64, offset: u64| {
            json!({
                "@context": {"v": "http://example.org/"},
                "select": {"?s": ["*"]},
                "where": {"@id": "?s", "@type": "v:Person"},
                "limit": limit,
                "offset": offset
            })
        };
        let page1 = run_crawl(&provider, &view, &paged(2, 0)).await;
        let page2 = run_crawl(&provider, &view, &paged(2, 2)).await;
        assert_eq!(page1.len(), 2, "page 1 = first 2 subjects");
        assert_eq!(page2.len(), 1, "page 2 = the remaining 1 subject (3 total)");
        // The two pages must not overlap (the OFFSET bug returned page 1 again).
        let disjoint = ids(&page1).is_disjoint(&ids(&page2));
        assert!(
            disjoint,
            "pages must not overlap: {:?} vs {:?}",
            ids(&page1),
            ids(&page2)
        );
        // Offset past the end -> empty.
        let past = run_crawl(&provider, &view, &paged(2, 10)).await;
        assert!(past.is_empty(), "offset past the end returns no subjects");
    }

    // (g2) An offset past the paging ceiling errors with guidance instead of
    //      silently launching an O(offset) remote scan.
    #[tokio::test]
    async fn crawl_wildcard_offset_past_ceiling_errors() {
        let mapping = CompiledR2rmlMapping::new(vec![tm(
            "#People",
            "people",
            "http://example.org/person/{id}",
            "http://example.org/Person",
            "http://example.org/name",
            "name",
        )]);
        let mut tables = HashMap::new();
        tables.insert(
            "people".to_string(),
            vec![id_str_batch("name", &[1], &["Alice"])],
        );
        let provider = MockCrawlProvider::new(mapping, tables);
        let (_ledger, view) = genesis_view();
        let fluree = FlureeBuilder::memory().build_memory();
        let crawl = json!({
            "@context": {"v": "http://example.org/"},
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "v:Person"},
            "limit": 2,
            "offset": MAX_CRAWL_OFFSET_SUBJECTS + 1
        });
        let err = expand_wildcard_crawl(
            &fluree,
            &view,
            &crawl,
            &provider,
            &provider,
            QueryExecutionOptions::new(),
            &FormatterConfig::default(),
        )
        .await
        .expect_err("an offset past the ceiling must error, not scan");
        assert!(
            err.to_string().contains("paging ceiling"),
            "error must explain the ceiling, got: {err}"
        );
    }

    /// A batch with `id` (Int64), one nullable String column, and one FK column
    /// (`fk_col`, Int64) — a child row that carries its own FK value.
    fn id_str_fk_batch(str_col: &str, fk_col: &str, rows: &[(i64, &str, i64)]) -> ColumnBatch {
        let schema = BatchSchema::new(vec![
            FieldInfo {
                name: "id".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                field_id: 1,
            },
            FieldInfo {
                name: str_col.to_string(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 2,
            },
            FieldInfo {
                name: fk_col.to_string(),
                field_type: FieldType::Int64,
                nullable: true,
                field_id: 3,
            },
        ]);
        ColumnBatch::new(
            Arc::new(schema),
            vec![
                Column::Int64(rows.iter().map(|(i, _, _)| Some(*i)).collect()),
                Column::String(
                    rows.iter()
                        .map(|(_, s, _)| Some((*s).to_string()))
                        .collect(),
                ),
                Column::Int64(rows.iter().map(|(_, _, f)| Some(*f)).collect()),
            ],
        )
        .unwrap()
    }

    /// A batch with `id` (Int64) and two FK columns (Int64) — an edge/self-ref
    /// child with two FKs to the SAME parent (the collision regression).
    fn id_two_fk_batch(fk1: &str, fk2: &str, rows: &[(i64, i64, i64)]) -> ColumnBatch {
        let schema = BatchSchema::new(vec![
            FieldInfo {
                name: "id".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                field_id: 1,
            },
            FieldInfo {
                name: fk1.to_string(),
                field_type: FieldType::Int64,
                nullable: true,
                field_id: 2,
            },
            FieldInfo {
                name: fk2.to_string(),
                field_type: FieldType::Int64,
                nullable: true,
                field_id: 3,
            },
        ]);
        ColumnBatch::new(
            Arc::new(schema),
            vec![
                Column::Int64(rows.iter().map(|(i, _, _)| Some(*i)).collect()),
                Column::Int64(rows.iter().map(|(_, a, _)| Some(*a)).collect()),
                Column::Int64(rows.iter().map(|(_, _, b)| Some(*b)).collect()),
            ],
        )
        .unwrap()
    }

    // A trusted `["*"]` browse crawl renders a RefObjectMap object as a templated
    // parent IRI built from the CHILD's own FK column, WITHOUT scanning the parent
    // table — for both a present FK and a dangling one (the browse relaxation).
    #[tokio::test]
    async fn crawl_ref_templated_from_child_without_parent_scan() {
        let mapping = CompiledR2rmlMapping::new(vec![
            // Child: Customer with a scalar `name` and a ref to Account.
            TriplesMap::new("#Customer", "customers")
                .with_subject_template("http://example.org/customer/{id}")
                .with_class("http://example.org/Customer")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://example.org/name"),
                    object_map: ObjectMap::column("name"),
                })
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://example.org/account"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Account",
                        "account_id",
                        "id",
                    )),
                }),
            // Parent: Account, subject templated on its PK `id`.
            tm(
                "#Account",
                "accounts",
                "http://example.org/account/{id}",
                "http://example.org/Account",
                "http://example.org/label",
                "label",
            ),
        ]);
        let mut tables = HashMap::new();
        // Customer 1 → account 10 (present); Customer 2 → account 99 (DANGLING).
        tables.insert(
            "customers".to_string(),
            vec![id_str_fk_batch(
                "name",
                "account_id",
                &[(1, "Alice", 10), (2, "Bob", 99)],
            )],
        );
        tables.insert(
            "accounts".to_string(),
            vec![id_str_batch("label", &[10], &["Acct-10"])],
        );
        let provider = MockCrawlProvider::new(mapping, tables);
        let (_ledger, view) = genesis_view();
        let crawl = json!({
            "@context": {"v": "http://example.org/"},
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "v:Customer"}
        });
        let docs = run_crawl(&provider, &view, &crawl).await;
        assert_eq!(docs.len(), 2, "two Customer instances");
        // Each Customer's `account` ref MUST serialize as a JSON-LD node
        // reference `{"@id": …}`, NOT a bare string (defect D1). Assert the
        // parsed SHAPE, so a bare-string regression can't slip past a substring
        // match. The one object-valued forward property is the account ref.
        let account_ids: std::collections::BTreeSet<String> = docs
            .iter()
            .map(|doc| {
                let obj = doc.as_object().expect("crawl doc is an object");
                let refs: Vec<&JsonValue> = obj
                    .iter()
                    .filter(|(k, _)| !k.starts_with('@'))
                    .filter(|(_, v)| !v.is_string())
                    .map(|(_, v)| v)
                    .collect();
                assert_eq!(
                    refs.len(),
                    1,
                    "exactly one non-literal forward property (the account ref): {obj:?}"
                );
                refs[0]
                    .as_object()
                    .and_then(|o| o.get("@id"))
                    .and_then(JsonValue::as_str)
                    .unwrap_or_else(|| {
                        panic!("account ref must be {{\"@id\": …}}, got {}", refs[0])
                    })
                    .to_string()
            })
            .collect();
        // Present FK (account 10) and dangling FK (account 99, the browse
        // relaxation) both render as templated `@id` node references.
        assert!(
            account_ids.iter().any(|id| id.ends_with("account/10")),
            "present FK must be an @id node ref to account/10: {account_ids:?}"
        );
        assert!(
            account_ids.iter().any(|id| id.ends_with("account/99")),
            "dangling FK must be an @id node ref to account/99 (browse relaxation): {account_ids:?}"
        );
        // The parent (Account) table is NEVER scanned; the child IS.
        assert!(
            !provider.scanned_tables().contains(&"accounts".to_string()),
            "parent Account table must not be scanned: {:?}",
            provider.scanned_tables()
        );
        assert!(
            provider.scanned_tables().contains(&"customers".to_string()),
            "child Customer table must be scanned"
        );
    }

    // Two FKs from one child to the SAME parent each resolve their OWN FK column
    // (regression for the child-agnostic shortcut: a child-specific placeholder
    // keyed by the shared parent `LookupCacheKey` would render BOTH as the origin).
    #[tokio::test]
    async fn crawl_two_fks_to_same_parent_render_distinct() {
        let mapping = CompiledR2rmlMapping::new(vec![
            TriplesMap::new("#Flight", "flights")
                .with_subject_template("http://example.org/flight/{id}")
                .with_class("http://example.org/Flight")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://example.org/origin"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Airport",
                        "origin_id",
                        "id",
                    )),
                })
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://example.org/destination"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Airport", "dest_id", "id",
                    )),
                }),
            tm(
                "#Airport",
                "airports",
                "http://example.org/airport/{id}",
                "http://example.org/Airport",
                "http://example.org/code",
                "code",
            ),
        ]);
        let mut tables = HashMap::new();
        tables.insert(
            "flights".to_string(),
            vec![id_two_fk_batch("origin_id", "dest_id", &[(1, 100, 200)])],
        );
        tables.insert(
            "airports".to_string(),
            vec![id_str_batch("code", &[100, 200], &["AAA", "BBB"])],
        );
        let provider = MockCrawlProvider::new(mapping, tables);
        let (_ledger, view) = genesis_view();
        let crawl = json!({
            "@context": {"v": "http://example.org/"},
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "v:Flight"}
        });
        let docs = run_crawl(&provider, &view, &crawl).await;
        assert_eq!(docs.len(), 1);
        let serialized = serde_json::to_string(&docs).unwrap();
        assert!(
            serialized.contains("airport/100"),
            "origin must render airport/100: {serialized}"
        );
        assert!(
            serialized.contains("airport/200"),
            "destination must render airport/200 (distinct from origin): {serialized}"
        );
        assert!(
            !provider.scanned_tables().contains(&"airports".to_string()),
            "parent Airport table must not be scanned: {:?}",
            provider.scanned_tables()
        );
    }

    // A directly-bound-subject var-predicate inspect (`<iri> ?p ?o`) scans ONLY
    // the TriplesMap whose subject template can produce the IRI — not every table.
    // (This is the exact lowered shape a single-subject detail view should send:
    // a constant `@id`, which lowers to `subject_constant=Some` and hits the
    // prune. A VALUES-bound `?s` stays a variable and does NOT — the detail view
    // must send the constant-subject form.)
    #[tokio::test]
    async fn bound_subject_inspect_prunes_to_matching_table() {
        let provider = two_table_provider(); // People person/{id} + Orders order/{id}
        let (_ledger, view) = genesis_view();
        let fluree = FlureeBuilder::memory().build_memory();
        let query = json!({
            "@context": {"v": "http://example.org/"},
            "select": ["?p", "?o"],
            "where": {"@id": "http://example.org/person/1", "?p": "?o"}
        });
        fluree
            .query_view_with_r2rml_options(
                &view,
                QueryInput::JsonLd(&query),
                &provider,
                &provider,
                QueryExecutionOptions::new(),
            )
            .await
            .expect("bound-subject inspect query succeeds");
        assert_eq!(
            provider.scanned_tables(),
            vec!["people".to_string()],
            "bound subject person/1 must prune to the People table only (not Orders)"
        );
    }

    /// One Customer (Alice) with a scalar `name` and a ref to Account (present
    /// FK), plus the Account parent map. Shared by the typed-json and
    /// `/info`⇔crawl serialization tests.
    fn customer_account_provider() -> MockCrawlProvider {
        let mapping = CompiledR2rmlMapping::new(vec![
            TriplesMap::new("#Customer", "customers")
                .with_subject_template("http://example.org/customer/{id}")
                .with_class("http://example.org/Customer")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://example.org/name"),
                    object_map: ObjectMap::column("name"),
                })
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("http://example.org/account"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Account",
                        "account_id",
                        "id",
                    )),
                }),
            tm(
                "#Account",
                "accounts",
                "http://example.org/account/{id}",
                "http://example.org/Account",
                "http://example.org/label",
                "label",
            ),
        ]);
        let mut tables = HashMap::new();
        tables.insert(
            "customers".to_string(),
            vec![id_str_fk_batch("name", "account_id", &[(1, "Alice", 10)])],
        );
        tables.insert(
            "accounts".to_string(),
            vec![id_str_batch("label", &[10], &["Acct-10"])],
        );
        MockCrawlProvider::new(mapping, tables)
    }

    // A typed-json crawl shapes refs as {"@id":…} (format-independent) and
    // literals as {"@value","@type"} value-objects — the crawl previously
    // ignored `format` and always emitted default JSON-LD (defect D2).
    #[tokio::test]
    async fn crawl_typed_json_shapes_refs_and_literals() {
        let provider = customer_account_provider();
        let (_ledger, view) = genesis_view();
        let crawl = json!({
            "@context": {"v": "http://example.org/"},
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "v:Customer"}
        });
        let docs = run_crawl_cfg(&provider, &view, &crawl, &FormatterConfig::typed_json()).await;
        assert_eq!(docs.len(), 1);
        let doc = docs[0].as_object().expect("doc is an object");
        let mut saw_typed_literal = false;
        let mut saw_id_ref = false;
        for (k, v) in doc {
            if k.starts_with('@') {
                continue;
            }
            if v.get("@value").is_some() {
                saw_typed_literal = true;
                assert!(
                    v.get("@type").is_some(),
                    "typed-json literal must carry @type: {v}"
                );
            } else if v.get("@id").is_some() {
                saw_id_ref = true;
            } else {
                panic!("typed-json property {k} is neither a value-object nor an @id ref: {v}");
            }
        }
        assert!(
            saw_typed_literal,
            "a typed literal value-object must appear: {doc:?}"
        );
        assert!(
            saw_id_ref,
            "the account ref must be an @id node reference: {doc:?}"
        );
    }

    // A boolean object renders as a real JSON boolean (`true`), not the string
    // "true" the R2RML operator leaves it as (defect D3) — in BOTH the default
    // and typed-json formats.
    #[tokio::test]
    async fn crawl_boolean_literal_renders_as_bool() {
        let mapping = CompiledR2rmlMapping::new(vec![TriplesMap::new("#Flag", "flags")
            .with_subject_template("http://example.org/flag/{id}")
            .with_class("http://example.org/Flag")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://example.org/active"),
                object_map: ObjectMap::column_typed(
                    "active",
                    "http://www.w3.org/2001/XMLSchema#boolean",
                ),
            })]);
        let mut tables = HashMap::new();
        // The operator reads the column value as the string "true" tagged
        // xsd:boolean — the exact shape D3 mis-rendered as a JSON string.
        tables.insert(
            "flags".to_string(),
            vec![id_str_batch("active", &[1], &["true"])],
        );
        let provider = MockCrawlProvider::new(mapping, tables);
        let (_ledger, view) = genesis_view();
        let crawl = json!({
            "@context": {"v": "http://example.org/"},
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "v:Flag"}
        });
        // Default format: bare JSON boolean.
        let docs = run_crawl(&provider, &view, &crawl).await;
        assert_eq!(docs.len(), 1);
        let (_, active) = docs[0]
            .as_object()
            .unwrap()
            .iter()
            .find(|(k, _)| !k.starts_with('@'))
            .expect("the active property");
        assert!(
            active.is_boolean() && active == &json!(true),
            "boolean must be a JSON bool, not the string \"true\": {active}"
        );
        // Typed-json: {"@value": true, "@type": …} with a real boolean @value.
        let typed = run_crawl_cfg(&provider, &view, &crawl, &FormatterConfig::typed_json()).await;
        let (_, active_t) = typed[0]
            .as_object()
            .unwrap()
            .iter()
            .find(|(k, _)| !k.starts_with('@'))
            .expect("the active property (typed)");
        assert!(
            active_t["@value"].is_boolean() && active_t["@value"] == json!(true),
            "typed-json @value must be a JSON bool: {active_t}"
        );
    }

    // Cross-path invariant (P1): every property `/info`
    // (`build_virtual_ledger_info`) reports with the `@id` datatype MUST
    // serialize in the crawl as a node reference `{"@id": …}` — the exact
    // two-path disagreement about `account` that defect D1 fixed. The crawl is
    // issued with NO @context + full IRIs so its property keys are full IRIs
    // that match `/info`'s full-IRI property keys.
    #[tokio::test]
    async fn crawl_ref_serialization_matches_info_id_datatype() {
        use crate::ledger_info::{build_virtual_ledger_info, VirtualSourceMeta};
        use fluree_db_nameservice::{GraphSourceRecord, GraphSourceType};

        let provider = customer_account_provider();

        // Which forward properties does /info type as `@id`?
        let record = GraphSourceRecord {
            graph_source_id: "crawl-e2e:main".to_string(),
            name: "crawl-e2e".to_string(),
            branch: "main".to_string(),
            source_type: GraphSourceType::Iceberg,
            config: "{}".to_string(),
            dependencies: vec![],
            index_id: None,
            index_t: 0,
            retracted: false,
        };
        let meta = VirtualSourceMeta {
            source_type: "Iceberg".to_string(),
            catalog_type: None,
            catalog_uri: None,
            table_location: None,
            warehouse: None,
            tables: vec!["customers".to_string(), "accounts".to_string()],
            snapshot_id: Some(1),
            mor_approximate_tables: Vec::new(),
        };
        let mut counts = HashMap::new();
        counts.insert("customers".to_string(), 1);
        counts.insert("accounts".to_string(), 1);
        let info = serde_json::to_value(build_virtual_ledger_info(
            &record,
            Some(provider.mapping.as_ref()),
            &meta,
            &counts,
        ))
        .unwrap();
        let id_props: std::collections::BTreeSet<String> = info["stats"]["properties"]
            .as_object()
            .expect("info carries stats.properties")
            .iter()
            .filter(|(_, v)| v["datatypes"].get("@id").is_some())
            .map(|(k, _)| k.clone())
            .collect();
        assert!(
            id_props.contains("http://example.org/account"),
            "/info must type the account ref as @id: {id_props:?}"
        );

        let (_ledger, view) = genesis_view();
        let crawl = json!({
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "http://example.org/Customer"}
        });
        let docs = run_crawl(&provider, &view, &crawl).await;
        assert_eq!(docs.len(), 1);
        for doc in &docs {
            for (key, val) in doc.as_object().expect("doc is an object") {
                if !id_props.contains(key) {
                    continue;
                }
                let values: Vec<&JsonValue> = match val {
                    JsonValue::Array(a) => a.iter().collect(),
                    other => vec![other],
                };
                for v in values {
                    assert!(
                        v.as_object().and_then(|o| o.get("@id")).is_some(),
                        "property {key} is @id-typed by /info but the crawl serialized it as {v}"
                    );
                }
            }
        }
    }

    // C1: a top-level VALUES clause on a graph-source crawl is DROPPED by the
    // crawl planner (it would silently return the whole class). Refuse loudly
    // with the typed R2RML unsupported-pattern envelope instead of over-returning.
    #[tokio::test]
    async fn crawl_with_top_level_values_refuses_loudly() {
        let provider = two_table_provider();
        let (_ledger, view) = genesis_view();
        let fluree = FlureeBuilder::memory().build_memory();
        let crawl = json!({
            "@context": {"v": "http://example.org/"},
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "v:Person"},
            "values": ["?s", [{"@id": "http://example.org/person/1"}]]
        });
        let err = expand_wildcard_crawl(
            &fluree,
            &view,
            &crawl,
            &provider,
            &provider,
            QueryExecutionOptions::new(),
            &FormatterConfig::default(),
        )
        .await
        .expect_err("a crawl carrying a dropped VALUES clause must refuse, not over-return");
        assert!(
            matches!(
                err,
                crate::ApiError::Query(fluree_db_query::QueryError::R2rmlUnsupportedPattern { .. })
            ),
            "must be the typed R2RML unsupported-pattern refusal, got: {err:?}"
        );
        assert!(
            err.to_string().to_lowercase().contains("values"),
            "refusal must name the VALUES clause: {err}"
        );
        // The refusal fires BEFORE any scan (no whole-class over-fetch).
        assert!(
            provider.scanned_tables().is_empty(),
            "the VALUES refusal must not scan the class table: {:?}",
            provider.scanned_tables()
        );
    }

    // Regression guard: a FLAT-select VALUES query is not a crawl select-map, so
    // the crawl expander declines it (Ok(None)) and it flows to the normal path
    // where VALUES works — the C1 guard must not touch it.
    #[tokio::test]
    async fn flat_select_values_is_not_intercepted_by_crawl() {
        let provider = two_table_provider();
        let (_ledger, view) = genesis_view();
        let fluree = FlureeBuilder::memory().build_memory();
        let flat = json!({
            "@context": {"v": "http://example.org/"},
            "select": ["?p", "?o"],
            "where": {"@id": "?s", "?p": "?o"},
            "values": ["?s", [{"@id": "http://example.org/person/1"}]]
        });
        let out = expand_wildcard_crawl(
            &fluree,
            &view,
            &flat,
            &provider,
            &provider,
            QueryExecutionOptions::new(),
            &FormatterConfig::default(),
        )
        .await
        .expect("a flat-select VALUES query must not error in the crawl expander");
        assert!(
            out.is_none(),
            "flat-select VALUES must fall through to the normal path (Ok(None))"
        );
    }

    // E2 / D9: a crawl over a class with ZERO matching TriplesMaps
    // short-circuits to an empty result WITHOUT scanning any table — instead of
    // fanning out over every TriplesMap (the wildcard fan-out DNF).
    #[tokio::test]
    async fn crawl_unmapped_class_returns_empty_without_scans() {
        let provider = two_table_provider(); // declares only Person + Order
        let (_ledger, view) = genesis_view();
        let crawl = json!({
            "@context": {"v": "http://example.org/"},
            "select": {"?s": ["*"]},
            // No TriplesMap declares `Commit`.
            "where": {"@id": "?s", "@type": "v:Commit"}
        });
        let docs = run_crawl(&provider, &view, &crawl).await;
        assert!(
            docs.is_empty(),
            "an unmapped class must return no subjects: {docs:?}"
        );
        assert!(
            provider.scanned_tables().is_empty(),
            "an unmapped class must scan zero tables (no fan-out): {:?}",
            provider.scanned_tables()
        );
    }

    // Control for E2: a MAPPED class is unaffected — it still scans exactly its
    // own class table (the short-circuit fires only on a zero-TriplesMap class).
    #[tokio::test]
    async fn crawl_mapped_class_still_scans_its_table() {
        let provider = two_table_provider();
        let (_ledger, view) = genesis_view();
        let docs = run_crawl(&provider, &view, &person_crawl(json!(["*"]), None)).await;
        assert_eq!(
            docs.len(),
            2,
            "the mapped Person class still returns its subjects"
        );
        assert_eq!(
            provider.scanned_tables(),
            vec!["people".to_string()],
            "a mapped class scans exactly its class table"
        );
    }

    // ---- Cluster B (D4): constant-IRI select-map routing ----

    // `{"select": {"<iri>": ["*"]}}` (NO where) hydrates the ONE existing subject
    // into a FULL native-parity node document — FK refs as {"@id":…} node
    // references, not the empty native-hydration stub the select-map used to
    // return. Prunes to the subject's own table (bound-subject prune).
    #[tokio::test]
    async fn bound_subject_select_map_wildcard_hydrates_node_doc() {
        let provider = customer_account_provider();
        let (_ledger, view) = genesis_view();
        let crawl = json!({"select": {"http://example.org/customer/1": ["*"]}});
        let docs = run_crawl(&provider, &view, &crawl).await;
        assert_eq!(docs.len(), 1, "exactly one subject document");
        let doc = docs[0].as_object().expect("doc object");
        assert_eq!(doc["@id"], json!("http://example.org/customer/1"));
        assert_eq!(
            doc.get("@type"),
            Some(&json!("http://example.org/Customer"))
        );
        assert_eq!(doc.get("http://example.org/name"), Some(&json!("Alice")));
        // FK ref → {"@id":…} (the A-cluster node-reference shape), NOT a bare string.
        assert_eq!(
            doc.get("http://example.org/account"),
            Some(&json!({"@id": "http://example.org/account/10"})),
            "FK ref must be an @id node reference: {doc:?}"
        );
        // Bound-subject prune + trust_fk_refs: only the customers table is scanned
        // (not accounts, and no FK-parent verification scan).
        assert_eq!(provider.scanned_tables(), vec!["customers".to_string()]);
    }

    // `{"select": {"<iri>": ["@type"]}}` returns @id + @type ONLY (no forward
    // properties).
    #[tokio::test]
    async fn bound_subject_select_map_type_only() {
        let provider = customer_account_provider();
        let (_ledger, view) = genesis_view();
        let crawl = json!({"select": {"http://example.org/customer/1": ["@type"]}});
        let docs = run_crawl(&provider, &view, &crawl).await;
        assert_eq!(docs.len(), 1);
        let doc = docs[0].as_object().unwrap();
        // Native omits @id from an explicit ["@type"] projection (the single
        // subject is already known): {"@type": …}, no @id.
        assert!(
            doc.get("@id").is_none(),
            "no @id for an explicit [\"@type\"] projection: {doc:?}"
        );
        assert_eq!(
            doc.get("@type"),
            Some(&json!("http://example.org/Customer"))
        );
        assert_eq!(doc.len(), 1, "only @type: {doc:?}");
    }

    // A forward-predicate list projects ONLY the requested predicates (here:
    // name), omitting the account ref and @type — and matches a requested term
    // against either the full IRI or its compacted form.
    #[tokio::test]
    async fn bound_subject_select_map_predicate_list_projects_requested_only() {
        let provider = customer_account_provider();
        let (_ledger, view) = genesis_view();
        // Full-IRI request.
        let crawl =
            json!({"select": {"http://example.org/customer/1": ["http://example.org/name"]}});
        let doc = run_crawl(&provider, &view, &crawl).await[0]
            .as_object()
            .unwrap()
            .clone();
        // Native omits @id from an explicit predicate list (not requested here).
        assert!(
            doc.get("@id").is_none(),
            "no @id when not requested: {doc:?}"
        );
        assert_eq!(doc.get("http://example.org/name"), Some(&json!("Alice")));
        assert!(doc.get("http://example.org/account").is_none());
        assert!(doc.get("@type").is_none());
        // An explicit "@id" in the list IS emitted (want_id).
        let with_id = json!({"select": {"http://example.org/customer/1": ["@id", "http://example.org/name"]}});
        let doc_id = run_crawl(&provider, &view, &with_id).await[0]
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(doc_id["@id"], json!("http://example.org/customer/1"));
        assert_eq!(doc_id.get("http://example.org/name"), Some(&json!("Alice")));
        // Compact-term request (via @context) resolves to the same property.
        let crawl2 = json!({
            "@context": {"v": "http://example.org/"},
            "select": {"http://example.org/customer/1": ["v:name"]}
        });
        let doc2 = run_crawl(&provider, &view, &crawl2).await[0]
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(
            doc2.get("v:name"),
            Some(&json!("Alice")),
            "compact-term request projects under the requested term: {doc2:?}"
        );
    }

    // An ABSENT / unreversible subject scans zero matching rows and returns the
    // native-parity `[{"@id": <iri>}]` stub (native returns the same for a
    // genuinely-missing subject).
    #[tokio::test]
    async fn bound_subject_select_map_absent_subject_returns_stub() {
        let provider = two_table_provider();
        let (_ledger, view) = genesis_view();
        let crawl = json!({"select": {"http://example.org/person/999": ["*"]}});
        let docs = run_crawl(&provider, &view, &crawl).await;
        assert_eq!(docs.len(), 1, "the stub is a single-element array");
        let doc = docs[0].as_object().unwrap();
        assert_eq!(doc["@id"], json!("http://example.org/person/999"));
        assert_eq!(
            doc.len(),
            1,
            "absent subject → bare @id stub (native parity): {doc:?}"
        );
    }

    // Native parity for an ABSENT subject on the NON-wildcard forms. Native
    // returns `[{}]` (one empty node) for both `["@type"]` and a forward-predicate
    // list on a missing subject: an explicit projection does NOT seed `@id`, and
    // no requested property/type is present, so the node is empty (but a node IS
    // emitted — the select-map projects a specific requested root). VERIFIED
    // against native-sf01 (a missing customer): both forms → `[{}]`, so Cluster B
    // is at parity. (The wildcard form separately returns the `[{"@id"}]` stub
    // above, because `["*"]` DOES seed `@id`.)
    #[tokio::test]
    async fn bound_subject_select_map_absent_non_wildcard_returns_empty_node() {
        let provider = two_table_provider();
        let (_ledger, view) = genesis_view();
        // `["@type"]` on a missing subject → one empty node.
        let type_docs = run_crawl(
            &provider,
            &view,
            &json!({"select": {"http://example.org/person/999": ["@type"]}}),
        )
        .await;
        assert_eq!(type_docs.len(), 1, "one node (the requested subject)");
        assert!(
            type_docs[0].as_object().unwrap().is_empty(),
            "absent [\"@type\"] → [{{}}] (native parity): {type_docs:?}"
        );
        // A forward-predicate list on a missing subject → one empty node.
        let pred_docs = run_crawl(
            &provider,
            &view,
            &json!({
                "@context": {"v": "http://example.org/"},
                "select": {"http://example.org/person/999": ["v:name"]}
            }),
        )
        .await;
        assert_eq!(pred_docs.len(), 1);
        assert!(
            pred_docs[0].as_object().unwrap().is_empty(),
            "absent predicate-list → [{{}}] (native parity): {pred_docs:?}"
        );
    }

    // Regression guard: a constant-IRI select-map over a NATIVE ledger (no graph
    // source) is UNCHANGED — `maybe_expand_crawl` returns None at the
    // native-hydration gate, so it never touches the R2RML routing.
    #[tokio::test]
    async fn native_select_map_is_not_rerouted() {
        let snapshot = LedgerSnapshot::genesis("native:main");
        let ledger = LedgerState::new(snapshot, Novelty::new(0));
        let view = GraphDb::from_ledger_state(&ledger); // graph_source_id = None
        let fluree = FlureeBuilder::memory().build_memory();
        let crawl = json!({"select": {"http://example.org/customer/1": ["*"]}});
        let out = maybe_expand_crawl(
            &fluree,
            &view,
            Some(&crawl),
            None,
            QueryExecutionOptions::new(),
            &FormatterConfig::default(),
        )
        .await
        .expect("no error");
        assert!(
            out.is_none(),
            "a native ledger select-map must fall through to native hydration"
        );
    }
}
