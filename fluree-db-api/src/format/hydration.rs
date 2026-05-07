//! Hydration formatter — materializes a Sid into a nested JSON-LD object by
//! recursively fetching its properties.
//!
//! Each hydration column carries its own root, level, and depth budget. The
//! formatter dispatches per column: a variable root reads the row's binding
//! and expands that subject; an IRI-constant root expands the named subject
//! directly; non-hydration (`Var`) columns are formatted by the regular
//! flat-binding formatters.
//!
//! # Supported Syntax
//!
//! ```json
//! // Variable-bound hydration
//! {"select": {"?person": ["*", {"ex:friend": ["*"]}]},
//!  "where": {"@id": "?person", "type": "ex:User"}}
//!
//! // With depth parameter for auto-expansion
//! {"select": {"?s": ["*"]}, "depth": 3, "where": ...}
//!
//! // IRI constant root (no WHERE needed)
//! {"select": {"ex:alice": ["*"]}}
//!
//! // Multiple hydration columns: each row is [expanded_a, expanded_b].
//! {"select": [{"?person": ["*"]}, {"?org": ["*"]}],
//!  "where": {"@id": "?person", "ex:worksFor": "?org"}}
//!
//! // Hydration columns mixed with flat variables.
//! {"select": ["?age", {"?person": ["*"]}],
//!  "where": {"@id": "?person", "ex:age": "?age"}}
//! ```
//!
//! # Output cardinality
//!
//! Solution rows generally map 1:1 to output rows, except when no column
//! depends on any solution binding (every column is an IRI-constant
//! hydration). In that case the output is independent of the row, so the
//! formatter emits a single row regardless of how many solutions the WHERE
//! produced. An empty solution set still produces no rows.
//!
//! A variable root that's unbound for a given solution renders as `null` in
//! that cell rather than dropping the entire row.

use super::config::{FormatterConfig, OutputFormat};
use super::datatype::is_inferable_datatype;
use super::iri::IriCompactor;
use super::{FormatError, Result};
use crate::QueryResult;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::range::{RangeMatch, RangeTest};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{Flake, GraphDbRef, Sid, Tracker};
use fluree_db_policy::{is_schema_flake, PolicyContext};
use fluree_db_query::binding::Binding;
use fluree_db_query::ir::{Column, ForwardItem, HydrationSpec, NestedSelectSpec, Root};
use fluree_vocab::namespaces::JSON_LD;
use fluree_vocab::rdf::{self, TYPE as RDF_TYPE_IRI};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::{json, Value as JsonValue};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Cache key: (Sid, local_spec_hash, depth_remaining)
///
/// The local_spec_hash is computed from the current `NestedSelectSpec`,
/// NOT any top-level spec. This serves two purposes:
/// - Different nested hydrations of the same Sid produce different entries.
/// - Multiple top-level hydration columns share entries when they land on
///   the same Sid with structurally identical levels, and stay separated
///   when their levels differ.
type CacheKey = (Sid, u64, usize);

/// Depth bookkeeping for one hydration call.
///
/// Each hydration column carries its own budget; threading the pair as a
/// single value (instead of two parallel `usize` parameters) keeps the
/// recursive signatures small and puts the descent / expansion-gate logic
/// on the type itself.
#[derive(Copy, Clone)]
struct DepthBudget {
    /// Current recursion depth (0 at a column's root).
    current: usize,
    /// Auto-expansion budget for this column.
    max: usize,
}

impl DepthBudget {
    /// Budget at a column's root: depth 0, with the column's `max`.
    fn root(max: usize) -> Self {
        Self { current: 0, max }
    }

    /// Budget for one level of recursive descent.
    fn descend(self) -> Self {
        Self {
            current: self.current + 1,
            max: self.max,
        }
    }

    /// Levels of auto-expansion still permitted (for cache keying).
    fn remaining(self) -> usize {
        self.max.saturating_sub(self.current)
    }

    /// Whether auto-expansion (no explicit sub-spec) is still permitted at
    /// this level — i.e. there's at least one descent left in the budget.
    fn can_expand(self) -> bool {
        self.current < self.max
    }
}

/// Context for formatting a specific predicate's values.
struct PredicateContext<'a> {
    /// The predicate SID being formatted.
    pred: &'a Sid,
    /// The flakes to format.
    flakes: &'a [&'a Flake],
    /// Explicit nested selection spec for this predicate (if any).
    explicit_sub_spec: Option<&'a NestedSelectSpec>,
}

/// Hash a `NestedSelectSpec` to a u64 cache key. We can't derive `Hash`
/// because the nested HashMaps don't implement it; iterate keys in sorted
/// order for determinism.
fn compute_level_hash(level: &NestedSelectSpec) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hash_level(level, &mut hasher);
    hasher.finish()
}

fn hash_level<H: std::hash::Hasher>(level: &NestedSelectSpec, hasher: &mut H) {
    use std::hash::Hash;
    match level {
        NestedSelectSpec::Wildcard {
            refinements,
            reverse,
        } => {
            0u8.hash(hasher);
            let mut keys: Vec<_> = refinements.keys().collect();
            keys.sort();
            keys.len().hash(hasher);
            for key in keys {
                key.hash(hasher);
                hash_level(refinements.get(key).unwrap(), hasher);
            }
            hash_reverse(reverse, hasher);
        }
        NestedSelectSpec::Explicit { forward, reverse } => {
            1u8.hash(hasher);
            forward.len().hash(hasher);
            for item in forward {
                hash_forward_item(item, hasher);
            }
            hash_reverse(reverse, hasher);
        }
    }
}

fn hash_forward_item<H: std::hash::Hasher>(item: &ForwardItem, hasher: &mut H) {
    use std::hash::Hash;
    match item {
        ForwardItem::Id => 0u8.hash(hasher),
        ForwardItem::Property {
            predicate,
            sub_spec,
        } => {
            1u8.hash(hasher);
            predicate.hash(hasher);
            match sub_spec {
                None => 0u8.hash(hasher),
                Some(boxed) => {
                    1u8.hash(hasher);
                    hash_level(boxed, hasher);
                }
            }
        }
    }
}

fn hash_reverse<H: std::hash::Hasher>(
    reverse: &HashMap<Sid, Option<Box<NestedSelectSpec>>>,
    hasher: &mut H,
) {
    use std::hash::Hash;
    let mut keys: Vec<_> = reverse.keys().collect();
    keys.sort();
    keys.len().hash(hasher);
    for key in keys {
        key.hash(hasher);
        match reverse.get(key).unwrap() {
            None => 0u8.hash(hasher),
            Some(inner) => {
                1u8.hash(hasher);
                hash_level(inner, hasher);
            }
        }
    }
}

/// Resolve a hydration root variable's binding into a `Sid` for a single row.
///
/// Returns `Ok(None)` when the binding is unbound, poisoned, missing, or not
/// subject-shaped (literals, IRIs that didn't match a known subject, etc.).
/// Such columns render as `null` rather than skipping the row entirely.
fn resolve_root_sid_from_binding(
    result: &QueryResult,
    binding: Option<&Binding>,
) -> Result<Option<Sid>> {
    match binding {
        Some(b) if b.is_encoded() => {
            let materialized = super::materialize::materialize_binding(result, b)?;
            Ok(match materialized {
                Binding::Sid { sid, .. } => Some(sid),
                Binding::IriMatch { primary_sid, .. } => Some(primary_sid),
                _ => None,
            })
        }
        Some(Binding::Sid { sid, .. }) => Ok(Some(sid.clone())),
        Some(Binding::IriMatch { primary_sid, .. }) => Ok(Some(primary_sid.clone())),
        Some(Binding::Unbound | Binding::Poisoned) | None => Ok(None),
        Some(
            Binding::Lit { .. }
            | Binding::Grouped(_)
            | Binding::Iri(_)
            | Binding::EncodedLit { .. }
            | Binding::EncodedSid { .. }
            | Binding::EncodedPid { .. },
        ) => Ok(None),
    }
}

/// Format one hydration column for one solution row.
///
/// Resolves the column's root (variable or IRI constant) into a `Sid` and
/// expands it via [`HydrationFormatter::format_subject`] using the column's
/// own level and depth budget. A variable root that's unbound for this row
/// renders as `null` rather than skipping the row entirely.
async fn format_hydration_column(
    formatter: &HydrationFormatter<'_>,
    spec: &HydrationSpec,
    result: &QueryResult,
    batch: &fluree_db_query::Batch,
    row_idx: usize,
    cache: &mut HashMap<CacheKey, JsonValue>,
) -> Result<JsonValue> {
    let root_sid: Sid = match &spec.root {
        Root::Sid(sid) => sid.clone(),
        Root::Var(var_id) => {
            let Some(sid) = resolve_root_sid_from_binding(result, batch.get(row_idx, *var_id))?
            else {
                return Ok(JsonValue::Null);
            };
            sid
        }
    };

    let mut visited = HashSet::new();
    formatter
        .format_subject(
            &root_sid,
            &spec.level,
            DepthBudget::root(spec.depth),
            &mut visited,
            cache,
        )
        .await
}

/// Async hydration formatter entry point.
///
/// This is the full hydration implementation with async database access
/// for property fetching, depth expansion, and reverse property expansion.
///
/// # Policy Support
///
/// When `policy` is `Some`, flakes are filtered according to view policies.
/// When `policy` is `None`, no filtering is applied (zero overhead).
pub async fn format_async(
    result: &QueryResult,
    db: GraphDbRef<'_>,
    compactor: &IriCompactor,
    config: &FormatterConfig,
    policy: Option<&PolicyContext>,
    tracker: Option<&Tracker>,
) -> Result<JsonValue> {
    if !result.output.has_hydration() {
        return Err(FormatError::InvalidBinding(
            "Hydration format called without any hydration columns".into(),
        ));
    }
    let columns = result.output.columns().ok_or_else(|| {
        FormatError::InvalidBinding("Hydration format called on non-Select output".into())
    })?;

    // Attach the tracker to the GraphDbRef so db.range calls inside the
    // formatter charge per-leaflet + per-dict-touch fuel through the
    // BinaryGraphView/BinaryCursor wiring (not just the per-flake baseline).
    let db = match tracker {
        Some(t) => db.with_tracker(t),
        None => db,
    };

    let formatter = HydrationFormatter::new(db, compactor, config, policy, tracker);

    // Shared cache across all rows and all hydration columns. The cache key
    // includes a hash of the current `NestedSelectSpec`, so columns with
    // structurally identical levels share entries; columns with different
    // levels do not collide.
    let mut cache: HashMap<CacheKey, JsonValue> = HashMap::new();
    let mut rows: Vec<JsonValue> = Vec::new();

    // If the underlying query produced no solutions, expansion produces no
    // rows — even when a column root is a constant Sid.
    if result.row_count() == 0 {
        return Ok(JsonValue::Array(rows));
    }

    // Single-column projections (e.g. `select: {"?x": ["*"]}`) emit a bare
    // object per row; multi-column projections emit array rows.
    let single_column = columns.len() == 1;

    // A projection is "row-independent" when no column reads any solution
    // binding — every column is a `Root::Sid` hydration. Such a projection
    // produces the same output for every solution row, so we emit a single
    // row regardless of how many solutions the WHERE clause yielded (the
    // WHERE still gates *whether* a row is emitted via row_count == 0
    // above).
    let row_dependent = columns.iter().any(|c| match c {
        Column::Var(_) => true,
        Column::Hydration(spec) => matches!(spec.root, Root::Var(_)),
    });

    'rows: for batch in &result.batches {
        for row_idx in 0..batch.len() {
            let mut row_values: Vec<JsonValue> = Vec::with_capacity(columns.len());

            for column in columns {
                let value = match column {
                    Column::Var(v) => match batch.get(row_idx, *v) {
                        Some(binding) if formatter.typed => {
                            super::typed::format_binding_with_result(result, binding, compactor)?
                        }
                        Some(binding) => {
                            super::jsonld::format_binding_with_result(result, binding, compactor)?
                        }
                        None => JsonValue::Null,
                    },
                    Column::Hydration(spec) => {
                        format_hydration_column(
                            &formatter, spec, result, batch, row_idx, &mut cache,
                        )
                        .await?
                    }
                };
                row_values.push(value);
            }

            if single_column {
                rows.push(row_values.into_iter().next().unwrap());
            } else {
                rows.push(JsonValue::Array(row_values));
            }

            if !row_dependent {
                break 'rows;
            }
        }
    }

    Ok(JsonValue::Array(rows))
}

/// Hydration formatter with async DB access.
///
/// The formatter is spec-agnostic: per-call the caller passes the
/// `NestedSelectSpec` level and `max_depth` budget. This lets a single
/// formatter serve multiple hydration columns in the same query.
struct HydrationFormatter<'a> {
    db: GraphDbRef<'a>,
    compactor: &'a IriCompactor,
    /// Whether to emit typed JSON (`{"@value": ..., "@type": ...}`) for all literals.
    typed: bool,
    /// Whether to always wrap property values in arrays (even single-valued).
    normalize_arrays: bool,
    /// Optional policy context for access control filtering.
    /// When None, no policy filtering is applied (zero overhead).
    policy: Option<&'a PolicyContext>,
    /// Optional execution tracker for fuel/policy tracking.
    tracker: Option<&'a Tracker>,
}

impl<'a> HydrationFormatter<'a> {
    fn new(
        db: GraphDbRef<'a>,
        compactor: &'a IriCompactor,
        config: &FormatterConfig,
        policy: Option<&'a PolicyContext>,
        tracker: Option<&'a Tracker>,
    ) -> Self {
        Self {
            db,
            compactor,
            typed: config.format == OutputFormat::TypedJson,
            normalize_arrays: config.normalize_arrays,
            policy,
            tracker,
        }
    }

    /// Format a subject to a JSON-LD object
    ///
    /// # Arguments
    /// - `sid`: Subject to expand
    /// - `level`: Selection specification (what properties to include)
    /// - `depth`: Current depth and per-column max — see [`DepthBudget`].
    /// - `visited`: Cycle detection set (per-path)
    /// - `cache`: Result cache (shared across all subjects)
    ///
    /// Note: Returns BoxFuture to support recursive async calls
    fn format_subject<'b>(
        &'b self,
        sid: &'b Sid,
        level: &'b NestedSelectSpec,
        depth: DepthBudget,
        visited: &'b mut HashSet<Sid>,
        cache: &'b mut HashMap<CacheKey, JsonValue>,
    ) -> BoxFuture<'b, Result<JsonValue>> {
        async move {
            let cache_key = (sid.clone(), compute_level_hash(level), depth.remaining());

            // Check cache first (same Sid + spec + depth = same result)
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.clone());
            }

            // Cycle detection - if already in current path, return just @id
            if !visited.insert(sid.clone()) {
                return Ok(json!({ "@id": self.compactor.compact_sid(sid)? }));
            }

            // Build object with sorted keys for determinism (BTreeMap)
            let mut obj: BTreeMap<String, JsonValue> = BTreeMap::new();

            // @id inclusion:
            // - Always include for nested hydrations (identity of an expanded ref)
            // - Otherwise include when wildcard or explicit @id selection
            if depth.current > 0 || level.includes_id() {
                obj.insert("@id".to_string(), json!(self.compactor.compact_sid(sid)?));
            }

            // Fetch forward properties
            let flakes = self.fetch_subject_properties(sid).await?;

            // Group flakes by predicate
            let mut by_pred: HashMap<Sid, Vec<&Flake>> = HashMap::new();
            for flake in &flakes {
                by_pred.entry(flake.p.clone()).or_default().push(flake);
            }

            // Format each predicate
            for (pred, mut pred_flakes) in by_pred {
                // `select_predicate` returns `None` when the level is Explicit
                // and the predicate isn't listed; otherwise it returns
                // `Some(sub_spec)` (which may itself be `None` for "select but
                // don't recurse").
                let Some(explicit_sub_spec) = level.select_predicate(&pred) else {
                    continue;
                };

                // If these flakes represent an ordered list (`@list`), preserve transaction order
                // by sorting by the list index stored in FlakeMeta.i.
                //
                // NOTE: Even when the caller did not explicitly define @container @list in context,
                // list assertions carry list indices. Sorting by meta index is the correct behavior
                // for ordered lists.
                if pred_flakes
                    .iter()
                    .any(|f| f.m.as_ref().and_then(|m| m.i).is_some())
                {
                    pred_flakes.sort_by_key(|f| f.m.as_ref().and_then(|m| m.i).unwrap_or(i32::MAX));
                }

                let pred_ctx = PredicateContext {
                    pred: &pred,
                    flakes: &pred_flakes,
                    explicit_sub_spec,
                };
                let values = self
                    .format_predicate_values(pred_ctx, level, depth, visited, cache)
                    .await?;

                if !values.is_empty() {
                    let key = self.format_predicate_key(&pred)?;
                    if values.len() == 1
                        && !self.normalize_arrays
                        && !self.force_array_for_key(&key)
                    {
                        obj.insert(key, values.into_iter().next().unwrap());
                    } else {
                        obj.insert(key, JsonValue::Array(values));
                    }
                }
            }

            // Format reverse properties
            for (rev_pred, rev_nested_opt) in level.reverse() {
                let rev_flakes = self.fetch_reverse_properties(sid, rev_pred).await?;
                if !rev_flakes.is_empty() {
                    let values = self
                        .format_reverse_values(
                            &rev_flakes,
                            rev_nested_opt.as_deref(),
                            level,
                            depth,
                            visited,
                            cache,
                        )
                        .await?;
                    if !values.is_empty() {
                        let key = self.compactor.compact_reverse_sid(rev_pred)?;
                        if values.len() == 1
                            && !self.normalize_arrays
                            && !self.force_array_for_key(&key)
                        {
                            obj.insert(key, values.into_iter().next().unwrap());
                        } else {
                            obj.insert(key, JsonValue::Array(values));
                        }
                    }
                }
            }

            // Remove from visited (allow revisiting via different paths)
            visited.remove(sid);

            // Convert BTreeMap to JsonValue::Object
            let result = JsonValue::Object(obj.into_iter().collect());

            // Cache the result
            cache.insert(cache_key, result.clone());

            Ok(result)
        }
        .boxed()
    }

    /// Whether this compact key should always be represented as an array.
    ///
    /// JSON-LD `@container` semantics:
    /// - `@set`: always array (even for 1 value)
    /// - `@list`: always array (ordered list)
    fn force_array_for_key(&self, compact_key: &str) -> bool {
        let Some(entry) = self.compactor.context().get(compact_key) else {
            return false;
        };
        let Some(containers) = &entry.container else {
            return false;
        };
        containers.iter().any(|c| {
            matches!(
                c,
                fluree_graph_json_ld::Container::Set | fluree_graph_json_ld::Container::List
            )
        })
    }

    /// Format predicate key with @type special-casing
    fn format_predicate_key(&self, pred: &Sid) -> Result<String> {
        let expanded = self.compactor.decode_sid(pred)?;
        if expanded == RDF_TYPE_IRI {
            Ok("@type".to_string())
        } else {
            self.compactor.compact_sid(pred)
        }
    }

    /// Format values for a predicate
    async fn format_predicate_values<'b>(
        &'b self,
        pred_ctx: PredicateContext<'b>,
        parent_level: &'b NestedSelectSpec,
        depth: DepthBudget,
        visited: &'b mut HashSet<Sid>,
        cache: &'b mut HashMap<CacheKey, JsonValue>,
    ) -> Result<Vec<JsonValue>> {
        let expanded = self.compactor.decode_sid(pred_ctx.pred)?;
        let is_rdf_type = expanded == RDF_TYPE_IRI;

        let mut values = Vec::new();
        for flake in pred_ctx.flakes {
            match &flake.o {
                FlakeValue::Ref(ref_sid) => {
                    if is_rdf_type {
                        // @type special case: compact IRI string, not {"@id": ...}
                        values.push(json!(self.compactor.compact_sid(ref_sid)?));
                    } else {
                        // Hydration decision:
                        // 1. If explicit sub-selection exists → expand with that spec
                        // 2. Else if budget permits → auto-expand with FULL parent level
                        // 3. Else → just return {"@id": ...}
                        if let Some(nested) = pred_ctx.explicit_sub_spec {
                            values.push(
                                self.format_subject(
                                    ref_sid,
                                    nested,
                                    depth.descend(),
                                    visited,
                                    cache,
                                )
                                .await?,
                            );
                        } else if depth.can_expand() {
                            values.push(
                                self.format_subject(
                                    ref_sid,
                                    parent_level,
                                    depth.descend(),
                                    visited,
                                    cache,
                                )
                                .await?,
                            );
                        } else {
                            // Max depth reached, just @id
                            values.push(json!({ "@id": self.compactor.compact_sid(ref_sid)? }));
                        }
                    }
                }
                _ => {
                    // Literal value
                    if let Some(nested) = pred_ctx.explicit_sub_spec {
                        values.push(self.format_literal_virtual(flake, nested)?);
                    } else if self.typed {
                        values.push(self.format_typed_literal_value(flake)?);
                    } else {
                        values.push(self.format_literal_value(flake)?);
                    }
                }
            }
        }
        Ok(values)
    }

    /// Format reverse property values
    async fn format_reverse_values<'b>(
        &'b self,
        flakes: &[Flake],
        nested_spec: Option<&'b NestedSelectSpec>,
        parent_level: &'b NestedSelectSpec,
        depth: DepthBudget,
        visited: &'b mut HashSet<Sid>,
        cache: &'b mut HashMap<CacheKey, JsonValue>,
    ) -> Result<Vec<JsonValue>> {
        let mut values = Vec::new();
        for flake in flakes {
            // For reverse lookup, subject is the entity that points to our object
            let subject_sid = &flake.s;

            if let Some(nested) = nested_spec {
                values.push(
                    self.format_subject(subject_sid, nested, depth.descend(), visited, cache)
                        .await?,
                );
            } else if depth.can_expand() {
                // Auto-expand reverse refs with FULL parent level
                values.push(
                    self.format_subject(subject_sid, parent_level, depth.descend(), visited, cache)
                        .await?,
                );
            } else {
                // No expansion - just @id
                values.push(json!({ "@id": self.compactor.compact_sid(subject_sid)? }));
            }
        }
        Ok(values)
    }

    /// Format a literal flake value
    fn format_literal_value(&self, flake: &Flake) -> Result<JsonValue> {
        let dt_full = self.compactor.decode_sid(&flake.dt)?;
        let dt_compact = self.compactor.compact_sid(&flake.dt)?;

        // Special handling for @json datatype: deserialize the JSON string.
        // Accept both Json and String variants (see jsonld.rs for rationale).
        if dt_full == rdf::JSON || dt_compact == "@json" {
            return match &flake.o {
                FlakeValue::Json(json_str) | FlakeValue::String(json_str) => {
                    serde_json::from_str(json_str).map_err(|e| {
                        FormatError::InvalidBinding(format!("Invalid JSON in @json value: {e}"))
                    })
                }
                _ => Err(FormatError::InvalidBinding(
                    "@json datatype must have FlakeValue::Json".to_string(),
                )),
            };
        }

        // Check for language tag
        if let Some(ref meta) = flake.m {
            if let Some(ref lang) = meta.lang {
                return match &flake.o {
                    FlakeValue::String(s) => Ok(json!({
                        "@value": s,
                        "@language": lang
                    })),
                    FlakeValue::Null => Ok(JsonValue::Null),
                    _ => Err(FormatError::InvalidBinding(
                        "Language-tagged literals must be strings".to_string(),
                    )),
                };
            }
        }

        // Inferable datatypes can omit @type
        if is_inferable_datatype(&dt_full) {
            return match &flake.o {
                FlakeValue::String(s) => Ok(JsonValue::String(s.clone())),
                FlakeValue::Long(n) => Ok(json!(n)),
                FlakeValue::Double(d) => {
                    if d.is_nan() {
                        Ok(JsonValue::String("NaN".to_string()))
                    } else if d.is_infinite() {
                        if d.is_sign_positive() {
                            Ok(JsonValue::String("INF".to_string()))
                        } else {
                            Ok(JsonValue::String("-INF".to_string()))
                        }
                    } else {
                        Ok(json!(d))
                    }
                }
                FlakeValue::Boolean(b) => Ok(json!(b)),
                FlakeValue::Vector(v) => Ok(JsonValue::Array(v.iter().map(|f| json!(f)).collect())),
                FlakeValue::Json(_) => Err(FormatError::InvalidBinding(
                    "@json should have been handled above".to_string(),
                )),
                FlakeValue::Null => Ok(JsonValue::Null),
                FlakeValue::Ref(sid) => {
                    // This shouldn't happen for literals, but handle gracefully
                    Ok(json!({ "@id": self.compactor.compact_sid(sid)? }))
                }
                // Extended numeric types - serialize as string
                FlakeValue::BigInt(n) => Ok(JsonValue::String(n.to_string())),
                FlakeValue::Decimal(d) => Ok(JsonValue::String(d.to_string())),
                // Temporal types - serialize as original string
                FlakeValue::DateTime(dt) => Ok(JsonValue::String(dt.to_string())),
                FlakeValue::Date(d) => Ok(JsonValue::String(d.to_string())),
                FlakeValue::Time(t) => Ok(JsonValue::String(t.to_string())),
                // Additional temporal types - serialize as original string
                FlakeValue::GYear(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::GYearMonth(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::GMonth(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::GDay(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::GMonthDay(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::YearMonthDuration(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::DayTimeDuration(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::Duration(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::GeoPoint(v) => Ok(JsonValue::String(v.to_string())),
            };
        }

        // Non-inferable datatypes include @type
        let value_json = match &flake.o {
            FlakeValue::String(s) => JsonValue::String(s.clone()),
            FlakeValue::Long(n) => json!(n),
            FlakeValue::Double(d) => {
                if d.is_nan() {
                    JsonValue::String("NaN".to_string())
                } else if d.is_infinite() {
                    if d.is_sign_positive() {
                        JsonValue::String("INF".to_string())
                    } else {
                        JsonValue::String("-INF".to_string())
                    }
                } else {
                    json!(d)
                }
            }
            FlakeValue::Boolean(b) => json!(b),
            FlakeValue::Vector(v) => JsonValue::Array(v.iter().map(|f| json!(f)).collect()),
            FlakeValue::Json(json_str) => JsonValue::String(json_str.clone()), // Fallback for non-@json context
            FlakeValue::Null => return Ok(JsonValue::Null),
            FlakeValue::Ref(sid) => {
                return Ok(json!({ "@id": self.compactor.compact_sid(sid)? }));
            }
            // Extended numeric types - serialize as string with @type
            FlakeValue::BigInt(n) => JsonValue::String(n.to_string()),
            FlakeValue::Decimal(d) => JsonValue::String(d.to_string()),
            // Temporal types - serialize as original string with @type
            FlakeValue::DateTime(dt) => JsonValue::String(dt.to_string()),
            FlakeValue::Date(d) => JsonValue::String(d.to_string()),
            FlakeValue::Time(t) => JsonValue::String(t.to_string()),
            // Additional temporal types - serialize as original string with @type
            FlakeValue::GYear(v) => JsonValue::String(v.to_string()),
            FlakeValue::GYearMonth(v) => JsonValue::String(v.to_string()),
            FlakeValue::GMonth(v) => JsonValue::String(v.to_string()),
            FlakeValue::GDay(v) => JsonValue::String(v.to_string()),
            FlakeValue::GMonthDay(v) => JsonValue::String(v.to_string()),
            FlakeValue::YearMonthDuration(v) => JsonValue::String(v.to_string()),
            FlakeValue::DayTimeDuration(v) => JsonValue::String(v.to_string()),
            FlakeValue::Duration(v) => JsonValue::String(v.to_string()),
            FlakeValue::GeoPoint(v) => JsonValue::String(v.to_string()),
        };

        Ok(json!({
            "@value": value_json,
            "@type": dt_compact
        }))
    }

    /// Format a literal flake value with explicit type annotations (TypedJson mode).
    ///
    /// Always emits `{"@value": ..., "@type": "..."}` for every literal, including
    /// types that would normally be inferred (xsd:string, xsd:long, etc.).
    /// Language-tagged strings use `{"@value": ..., "@language": "..."}` (no @type).
    /// `@json` values use `{"@value": <parsed>, "@type": "@json"}`.
    fn format_typed_literal_value(&self, flake: &Flake) -> Result<JsonValue> {
        let dt_full = self.compactor.decode_sid(&flake.dt)?;
        let dt_compact = self.compactor.compact_sid(&flake.dt)?;

        // @json datatype: deserialize and wrap with @type
        if dt_full == rdf::JSON || dt_compact == "@json" {
            return match &flake.o {
                FlakeValue::Json(json_str) | FlakeValue::String(json_str) => {
                    let json_val: JsonValue = serde_json::from_str(json_str).map_err(|e| {
                        FormatError::InvalidBinding(format!("Invalid JSON in @json value: {e}"))
                    })?;
                    Ok(json!({
                        "@value": json_val,
                        "@type": "@json"
                    }))
                }
                _ => Err(FormatError::InvalidBinding(
                    "@json datatype must have FlakeValue::Json".to_string(),
                )),
            };
        }

        // Language-tagged string: use @language instead of @type
        if let Some(ref meta) = flake.m {
            if let Some(ref lang) = meta.lang {
                return match &flake.o {
                    FlakeValue::String(s) => Ok(json!({
                        "@value": s,
                        "@language": lang
                    })),
                    FlakeValue::Null => Ok(JsonValue::Null),
                    _ => Err(FormatError::InvalidBinding(
                        "Language-tagged literals must be strings".to_string(),
                    )),
                };
            }
        }

        // All other types: always include @type
        let value_json = match &flake.o {
            FlakeValue::String(s) => json!(s),
            FlakeValue::Long(n) => json!(n),
            FlakeValue::Double(d) => {
                if d.is_nan() {
                    json!("NaN")
                } else if d.is_infinite() {
                    if d.is_sign_positive() {
                        json!("INF")
                    } else {
                        json!("-INF")
                    }
                } else {
                    json!(d)
                }
            }
            FlakeValue::Boolean(b) => json!(b),
            FlakeValue::Vector(v) => json!(v),
            // JSON values should usually be handled by the `@json` branch above,
            // but we can still encounter `FlakeValue::Json` here (e.g., custom
            // datatypes or typed-json formatting paths). Never panic in a server
            // formatter: fall back to parsing the JSON payload, or emit the raw
            // string if it isn't valid JSON.
            FlakeValue::Json(s) => {
                serde_json::from_str::<JsonValue>(s).unwrap_or_else(|_| json!(s))
            }
            FlakeValue::Null => return Ok(JsonValue::Null),
            FlakeValue::Ref(sid) => return Ok(json!({ "@id": self.compactor.compact_sid(sid)? })),
            FlakeValue::BigInt(n) => json!(n.to_string()),
            FlakeValue::Decimal(d) => json!(d.to_string()),
            FlakeValue::DateTime(dt) => json!(dt.to_string()),
            FlakeValue::Date(d) => json!(d.to_string()),
            FlakeValue::Time(t) => json!(t.to_string()),
            FlakeValue::GYear(v) => json!(v.to_string()),
            FlakeValue::GYearMonth(v) => json!(v.to_string()),
            FlakeValue::GMonth(v) => json!(v.to_string()),
            FlakeValue::GDay(v) => json!(v.to_string()),
            FlakeValue::GMonthDay(v) => json!(v.to_string()),
            FlakeValue::YearMonthDuration(v) => json!(v.to_string()),
            FlakeValue::DayTimeDuration(v) => json!(v.to_string()),
            FlakeValue::Duration(v) => json!(v.to_string()),
            FlakeValue::GeoPoint(v) => json!(v.to_string()),
        };

        Ok(json!({
            "@value": value_json,
            "@type": dt_compact
        }))
    }

    /// Format a literal as a virtual node when explicitly selected.
    ///
    /// Supports `@value`, `@type`, and `@language` selection semantics.
    fn format_literal_virtual(&self, flake: &Flake, spec: &NestedSelectSpec) -> Result<JsonValue> {
        let mut want_value = false;
        let mut want_type = false;
        let mut want_language = false;

        match spec {
            NestedSelectSpec::Wildcard { .. } => {
                want_value = true;
                want_type = true;
                want_language = flake.m.as_ref().and_then(|m| m.lang.as_ref()).is_some();
            }
            NestedSelectSpec::Explicit { forward, .. } => {
                for item in forward {
                    if let ForwardItem::Property { predicate, .. } = item {
                        if predicate.namespace_code == JSON_LD {
                            match predicate.name.as_ref() {
                                "value" => want_value = true,
                                "type" => want_type = true,
                                "language" => want_language = true,
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        let mut obj = serde_json::Map::new();

        if want_value {
            obj.insert(
                "@value".to_string(),
                self.format_literal_plain_value(flake)?,
            );
        }

        if want_type {
            let dt_compact = self.compactor.compact_sid(&flake.dt)?;
            obj.insert("@type".to_string(), json!(dt_compact));
        }

        if want_language {
            if let Some(lang) = flake.m.as_ref().and_then(|m| m.lang.as_ref()) {
                obj.insert("@language".to_string(), json!(lang));
            }
        }

        Ok(JsonValue::Object(obj))
    }

    fn format_literal_plain_value(&self, flake: &Flake) -> Result<JsonValue> {
        let dt_full = self.compactor.decode_sid(&flake.dt)?;
        let dt_compact = self.compactor.compact_sid(&flake.dt)?;

        if dt_full == rdf::JSON || dt_compact == "@json" {
            return match &flake.o {
                FlakeValue::Json(json_str) | FlakeValue::String(json_str) => {
                    serde_json::from_str(json_str).map_err(|e| {
                        FormatError::InvalidBinding(format!("Invalid JSON in @json value: {e}"))
                    })
                }
                _ => Err(FormatError::InvalidBinding(
                    "@json datatype must have FlakeValue::Json".to_string(),
                )),
            };
        }

        match &flake.o {
            FlakeValue::String(s) => Ok(JsonValue::String(s.clone())),
            FlakeValue::Long(n) => Ok(json!(n)),
            FlakeValue::Double(d) => {
                if d.is_nan() {
                    Ok(JsonValue::String("NaN".to_string()))
                } else if d.is_infinite() {
                    if d.is_sign_positive() {
                        Ok(JsonValue::String("INF".to_string()))
                    } else {
                        Ok(JsonValue::String("-INF".to_string()))
                    }
                } else {
                    Ok(json!(d))
                }
            }
            FlakeValue::Boolean(b) => Ok(json!(b)),
            FlakeValue::Vector(v) => Ok(JsonValue::Array(v.iter().map(|f| json!(f)).collect())),
            FlakeValue::Json(_) => Err(FormatError::InvalidBinding(
                "@json should have been handled above".to_string(),
            )),
            FlakeValue::Null => Ok(JsonValue::Null),
            FlakeValue::Ref(sid) => Ok(json!({ "@id": self.compactor.compact_sid(sid)? })),
            FlakeValue::BigInt(n) => Ok(JsonValue::String(n.to_string())),
            FlakeValue::Decimal(d) => Ok(JsonValue::String(d.to_string())),
            FlakeValue::DateTime(dt) => Ok(JsonValue::String(dt.to_string())),
            FlakeValue::Date(d) => Ok(JsonValue::String(d.to_string())),
            FlakeValue::Time(t) => Ok(JsonValue::String(t.to_string())),
            // Additional temporal types - serialize as original string
            FlakeValue::GYear(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::GYearMonth(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::GMonth(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::GDay(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::GMonthDay(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::YearMonthDuration(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::DayTimeDuration(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::Duration(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::GeoPoint(v) => Ok(JsonValue::String(v.to_string())),
        }
    }

    /// Fetch all properties for a subject using SPOT index
    ///
    /// When policy is set, filters flakes according to view policies.
    /// When policy is None, returns all flakes (zero overhead).
    async fn fetch_subject_properties(&self, sid: &Sid) -> Result<Vec<Flake>> {
        let flakes = self
            .db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(sid.clone()),
            )
            .await
            .map_err(|e| {
                FormatError::InvalidBinding(format!("Failed to fetch subject properties: {e}"))
            })?;

        // Policy filtering: only when policy is Some and not root.
        // Per-flake / per-leaflet / per-dict-touch fuel charges happen inside
        // db.range via the GraphDbRef tracker — no extra charge needed here.
        if let Some(policy_ctx) = self.policy {
            if !policy_ctx.wrapper().is_root() {
                return Ok(self.filter_flakes_by_policy(flakes, policy_ctx));
            }
        }

        Ok(flakes)
    }

    /// Fetch subjects that have this object via predicate (reverse lookup using POST index)
    ///
    /// When policy is set, filters flakes according to view policies.
    /// When policy is None, returns all flakes (zero overhead).
    async fn fetch_reverse_properties(&self, object_sid: &Sid, pred: &Sid) -> Result<Vec<Flake>> {
        let flakes = self
            .db
            .range(
                IndexType::Post,
                RangeTest::Eq,
                RangeMatch::predicate_object(pred.clone(), FlakeValue::Ref(object_sid.clone())),
            )
            .await
            .map_err(|e| {
                FormatError::InvalidBinding(format!("Failed to fetch reverse properties: {e}"))
            })?;

        // Policy filtering: only when policy is Some and not root
        // Zero overhead when policy is None (common case).
        // Per-flake / per-leaflet / per-dict-touch fuel charges happen inside
        // db.range via the GraphDbRef tracker — no extra charge needed here.
        if let Some(policy_ctx) = self.policy {
            if !policy_ctx.wrapper().is_root() {
                return Ok(self.filter_flakes_by_policy(flakes, policy_ctx));
            }
        }

        Ok(flakes)
    }

    /// Filter flakes according to view policies
    ///
    /// Schema flakes are always allowed. Other flakes are checked against
    /// the policy context.
    fn filter_flakes_by_policy(
        &self,
        flakes: Vec<Flake>,
        policy_ctx: &PolicyContext,
    ) -> Vec<Flake> {
        flakes
            .into_iter()
            .filter(|flake| {
                // Schema flakes always allowed (needed for query planning/formatting)
                if is_schema_flake(&flake.p, &flake.o) {
                    return true;
                }

                // Get subject classes from cache (empty if not cached)
                // Note: Hydration doesn't pre-populate class cache like BinaryScanOperator.
                // For hydration with class policies, the cache will be empty and
                // class policies may not work correctly. This is a known limitation
                // that can be addressed by pre-populating in format_async if needed.
                let subject_classes = policy_ctx
                    .get_cached_subject_classes(&flake.s)
                    .unwrap_or_default();

                let allowed = match self.tracker {
                    Some(tracker) => policy_ctx.allow_view_flake_tracked(
                        &flake.s,
                        &flake.p,
                        &flake.o,
                        &subject_classes,
                        tracker,
                    ),
                    None => {
                        policy_ctx.allow_view_flake(&flake.s, &flake.p, &flake.o, &subject_classes)
                    }
                };

                allowed.unwrap_or_default() // On error, conservatively deny
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_different_depths() {
        let sid = Sid::new(100, "alice");
        let spec_hash = 12345u64;

        let key1: CacheKey = (sid.clone(), spec_hash, 0);
        let key2: CacheKey = (sid.clone(), spec_hash, 1);
        let key3: CacheKey = (sid, spec_hash, 0);

        // Different depths should produce different keys
        assert_ne!(key1, key2);
        // Same Sid + spec + depth should be equal
        assert_eq!(key1, key3);
    }

    fn explicit(forward: Vec<ForwardItem>) -> NestedSelectSpec {
        NestedSelectSpec::Explicit {
            forward,
            reverse: HashMap::new(),
        }
    }

    fn wildcard() -> NestedSelectSpec {
        NestedSelectSpec::Wildcard {
            refinements: HashMap::new(),
            reverse: HashMap::new(),
        }
    }

    #[test]
    fn level_hash_distinguishes_explicit_from_wildcard() {
        let hash_empty = compute_level_hash(&explicit(vec![]));
        let hash_wildcard = compute_level_hash(&wildcard());
        assert_ne!(hash_empty, hash_wildcard);
        // Same level produces same hash.
        assert_eq!(compute_level_hash(&wildcard()), hash_wildcard);
    }

    #[test]
    fn level_hash_distinguishes_properties() {
        let pred1 = Sid::new(1, "name");
        let pred2 = Sid::new(2, "age");

        let hash1 = compute_level_hash(&explicit(vec![ForwardItem::Property {
            predicate: pred1.clone(),
            sub_spec: None,
        }]));
        let hash2 = compute_level_hash(&explicit(vec![ForwardItem::Property {
            predicate: pred2.clone(),
            sub_spec: None,
        }]));
        assert_ne!(hash1, hash2);

        // Sub-spec presence changes the hash.
        let hash3 = compute_level_hash(&explicit(vec![ForwardItem::Property {
            predicate: pred1.clone(),
            sub_spec: Some(Box::new(wildcard())),
        }]));
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn level_hash_distinguishes_reverse() {
        let rev_pred = Sid::new(10, "friendOf");

        let hash_no_reverse = compute_level_hash(&wildcard());

        let mut reverse1 = HashMap::new();
        reverse1.insert(rev_pred.clone(), None);
        let hash_with_reverse = compute_level_hash(&NestedSelectSpec::Wildcard {
            refinements: HashMap::new(),
            reverse: reverse1,
        });
        assert_ne!(hash_no_reverse, hash_with_reverse);

        let mut reverse2 = HashMap::new();
        reverse2.insert(rev_pred, Some(Box::new(wildcard())));
        let hash_nested = compute_level_hash(&NestedSelectSpec::Wildcard {
            refinements: HashMap::new(),
            reverse: reverse2,
        });
        assert_ne!(hash_with_reverse, hash_nested);
    }
}
