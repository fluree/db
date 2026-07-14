//! R2RML Scan Operator
//!
//! This operator executes R2RML scans against Iceberg tables and emits
//! RDF term bindings according to the mapping specification.
//!
//! # Design
//!
//! The operator is correlated: it consumes a child stream (often an EmptyOperator seed)
//! and for each input row, scans the appropriate Iceberg table(s) and materializes
//! RDF terms according to the TriplesMap definition.
//!
//! # Execution Flow
//!
//! 1. `open()`: Load the compiled R2RML mapping from the provider
//! 2. `next_batch()`: For each input row:
//!    - Scan the logical table from the TriplesMap
//!    - For RefObjectMap joins, scan parent tables and build lookup indexes
//!    - Materialize subject/predicate/object terms
//!    - Emit bindings for query variables
//! 3. `close()`: Release resources
//!
//! # RefObjectMap Join Execution
//!
//! When a PredicateObjectMap contains a RefObjectMap (referencing a parent TriplesMap),
//! the operator:
//!
//! 1. Scans the parent TriplesMap's table
//! 2. Builds a hash lookup: parent join key → parent subject IRI
//! 3. For each child row, extracts child join key values
//! 4. Looks up the parent subject IRI from the hash map
//! 5. Emits the parent IRI as the object binding

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::eval::PreparedBoolExpression;
use crate::filter::filter_batch;
use crate::group_aggregate::{binding_to_group_key_normalized, GroupKeyOwned};
use crate::ir::R2rmlPattern;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::r2rml::ColumnBatchStream;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_r2rml::mapping::{
    extract_template_columns, CompiledR2rmlMapping, ObjectMap, PredicateObjectMap, RefObjectMap,
    TriplesMap,
};
use fluree_db_r2rml::materialize::{
    expand_template, get_join_key_from_batch, materialize_object_from_batch,
    materialize_predicate_from_batch, materialize_subject_from_batch, reverse_subject_template,
    RdfTerm,
};
use fluree_db_tabular::{Column, ColumnBatch};
use fluree_vocab::xsd;
use futures::StreamExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Lookup table for RefObjectMap joins.
///
/// Maps parent join key (as `Vec<String>`) to materialized parent subject IRI.
/// The key is a composite key of all parent columns specified in join conditions.
pub type ParentLookup = HashMap<Vec<String>, RdfTerm>;

/// Composite key for caching a parent lookup: `(parent_tm_iri, sorted_parent_join_cols)`.
type LookupCacheKey = (String, Vec<String>);

/// Query-scoped key for the cross-operator-rebuild parent-lookup memo (PR-8b):
/// `(graph_source_id, parent_tm_iri, sorted_parent_join_cols, as_of_t)`. Unlike
/// the per-operator [`LookupCacheKey`], this memo is shared across ALL R2RML
/// operators in a query, so the key MUST carry `graph_source_id` (two sources can
/// hold a same-named table — no cross-source pollution) and `as_of_t` (cheap
/// insurance: the per-query snapshot pin should keep it constant, but a
/// query-wide share must not alias two snapshots).
pub type R2rmlParentMemoKey = (String, String, Vec<String>, Option<i64>);

/// The query-scoped parent-lookup memo (PR-8b). Extends PR-4's per-operator
/// [`LookupCacheKey`] cache to a lifetime that survives the operator REBUILD an
/// inner join with an interposed non-pushable FILTER + LIMIT performs per driving
/// batch — which resets the per-operator cache (the q031 seam). Sharing is valid:
/// a lookup's content is fixed by its key at a stable `as_of_t`.
///
/// `total_rows` bounds accumulation query-wide: a query-scoped cache can hold many
/// parents where a single operator's cache held at most its own, so an insert that
/// would exceed the total cap is refused — the caller falls through to a per-batch
/// rebuild for that key — keeping memory bounded (per-entry is already ≤ one
/// window via the q015 fact-as-parent guard).
#[derive(Default)]
pub struct R2rmlParentMemoInner {
    map: HashMap<R2rmlParentMemoKey, Arc<ParentLookup>>,
    total_rows: usize,
}

impl R2rmlParentMemoInner {
    fn get(&self, key: &R2rmlParentMemoKey) -> Option<Arc<ParentLookup>> {
        self.map.get(key).cloned()
    }

    /// Cache `lookup` under `key` unless it would push the memo past `total_cap`
    /// rows. Returns whether the entry is now cached. A key already present is
    /// treated as cached (idempotent).
    fn try_insert(
        &mut self,
        key: R2rmlParentMemoKey,
        lookup: &Arc<ParentLookup>,
        total_cap: usize,
    ) -> bool {
        if self.map.contains_key(&key) {
            return true;
        }
        let rows = lookup.len();
        if self.total_rows.saturating_add(rows) > total_cap {
            return false;
        }
        self.total_rows += rows;
        self.map.insert(key, Arc::clone(lookup));
        true
    }
}

/// Shared, query-scoped parent-lookup memo — see [`R2rmlParentMemoInner`].
pub type R2rmlParentMemo = std::sync::Arc<std::sync::Mutex<R2rmlParentMemoInner>>;

/// Query-scoped parent-memo total-rows cap, as a multiple of the materialize
/// window. Per-entry is already ≤ one window (the q015 fact-as-parent guard); this
/// bounds the SUM across a query's parents. Default 2×; env
/// `FLUREE_R2RML_PARENT_MEMO_TOTAL_WINDOWS`.
fn parent_memo_total_cap_rows() -> usize {
    let mult = std::env::var("FLUREE_R2RML_PARENT_MEMO_TOTAL_WINDOWS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(2);
    mult.saturating_mul(materialize_window_rows())
}

/// A child-templated `RefObjectMap` shortcut: render the parent subject IRI
/// directly from the child row's own FK columns via the parent's subject
/// template, with NO scan of the parent table.
///
/// Valid only when the parent subject is a pure IRI template whose placeholder
/// columns are all FK join columns (so every placeholder is resolvable from the
/// child's own FK value) and the FK is single-column. Used ONLY on the trusted
/// browse-crawl path ([`ExecutionContext::trust_fk_refs`]) for the injected
/// true-wildcard scan: it skips the parent-table existence check, so a
/// present-but-dangling FK renders a templated IRI instead of yielding no triple.
/// A matched (non-dangling) FK renders a byte-identical IRI to the parent-scan
/// path — same template, same `iri_escape`, and the join guarantees the child FK
/// value equals the parent key. Child-agnostic (holds only the parent template),
/// so two FKs from one child to the SAME parent share a [`LookupCacheKey`]
/// without colliding: each resolves its own FK value at materialize time.
struct RefShortcut {
    subject_template: String,
}

/// Build a [`RefShortcut`] for a `RefObjectMap`, or `None` (keep the parent scan)
/// when the parent subject is not provably child-templatable:
/// - not a pure IRI template (uses `rr:column` / `rr:constant`, or a non-IRI
///   term type such as a blank node), or
/// - a template placeholder is not one of the FK join columns (the child row does
///   not carry its value — e.g. a subject keyed on a non-FK column), or
/// - the FK is composite (`join_conditions.len() != 1`): the parent lookup keys
///   on SORTED parent columns while the child key is built in declared order, so
///   a composite render could transpose columns. Refused for the MVP (auto-
///   generated Iceberg mappings are single-column FK → PK); composite falls back
///   to the scan, which stays authoritative.
fn build_ref_shortcut(parent_tm: &TriplesMap, rom: &RefObjectMap) -> Option<RefShortcut> {
    let sm = &parent_tm.subject_map;
    if sm.column.is_some() || sm.constant.is_some() || !sm.generates_iri() {
        return None;
    }
    let template = sm.template.as_ref()?;
    if rom.join_conditions.len() != 1 {
        return None;
    }
    // Extract the template's placeholder columns from the template string itself
    // rather than trusting `subject_map.template_columns` to be populated (some
    // builders set the template without extracting its columns). Every placeholder
    // must be an FK join column so it is resolvable from the child's own FK value;
    // a placeholder-less (constant) template can't vary per FK, so fall back to the
    // scan.
    let template_cols = extract_template_columns(template);
    if template_cols.is_empty() {
        return None;
    }
    let parent_cols = rom.parent_columns();
    if !template_cols
        .iter()
        .all(|c| parent_cols.contains(&c.as_str()))
    {
        return None;
    }
    Some(RefShortcut {
        subject_template: template.clone(),
    })
}

/// Target number of table rows to materialize into bindings per parallel window.
///
/// Materialization explodes the compact columnar form into fat `Binding` rows
/// (the memory wall: a full-table materialize of a 6M-row scan is ~14 GB). By
/// materializing one bounded window at a time — still in parallel on rayon —
/// the resident binding footprint is capped at roughly this many rows while the
/// scan streams to the downstream operator. Override with
/// `FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS`.
const DEFAULT_MATERIALIZE_WINDOW_ROWS: usize = 512 * 1024;

fn materialize_window_rows() -> usize {
    std::env::var("FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MATERIALIZE_WINDOW_ROWS)
}

/// Whether LIMIT early-termination (row-budget) pushdown into the scan is
/// enabled. Read once from `FLUREE_R2RML_LIMIT_PUSHDOWN` (family falsy
/// spellings, [`super::env_switch_enabled`]); disabling restores full-window
/// materialization under a LIMIT.
fn limit_pushdown_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_R2RML_LIMIT_PUSHDOWN"))
}

/// How a window of produced rows is combined with the buffered child rows.
///
/// The join is *flipped* relative to a naive per-child probe: the (small,
/// already-buffered) child side is indexed once, and the (large) produced side
/// is streamed window-by-window and probed against it. This is what lets the
/// scan avoid materializing the whole table's bindings at once.
enum JoinPlan {
    /// No shared variables: every produced row pairs with every child row
    /// (typically the single seed row).
    Cross,
    /// Shared join variables: child rows indexed by their join-key values.
    Hash {
        join_vars: Vec<VarId>,
        /// Fully-bound child rows: join key → child row indices.
        full_index: HashMap<Vec<GroupKeyOwned>, Vec<usize>>,
        /// Child rows with an unbound join var (a wildcard position, matched
        /// linearly): `(child_row_idx, key with None for the unbound vars)`.
        partial_rows: Vec<(usize, Vec<Option<GroupKeyOwned>>)>,
    },
}

/// One TriplesMap's streaming scan state: the live batch stream from the table
/// scan, the parent lookups for its RefObjectMap joins, and the precomputed join
/// plan against the buffered child. Batches are pulled in bounded windows and
/// dropped after materializing, so the whole table is never resident.
struct TmStream {
    tm_iri: String,
    stream: ColumnBatchStream,
    exhausted: bool,
    /// Parent (dimension) lookups for this TriplesMap's RefObjectMap POMs, keyed
    /// by `(parent TriplesMap IRI, join_cols)` ([`LookupCacheKey`]). `Arc`-shared
    /// so a lookup memoized on the operator's `parent_lookup_cache` (PR-4) is
    /// reused across child batches without a re-scan or a clone.
    parent_lookups: HashMap<LookupCacheKey, Arc<ParentLookup>>,
    /// Child-templated ref shortcuts for RefObjectMap POMs whose parent scan was
    /// skipped (trusted browse crawl only). Keyed identically to `parent_lookups`;
    /// a given `LookupCacheKey` populates exactly one of the two maps.
    ref_shortcuts: HashMap<LookupCacheKey, RefShortcut>,
    join: JoinPlan,
}

/// In-flight streaming scan for one buffered child batch. The child is held
/// resident (it is the small side and is indexed by the join plans); each
/// TriplesMap's produced batches are materialized in bounded windows and emitted
/// incrementally, so the operator never holds the whole table's bindings.
struct ScanProgress {
    child_batch: Batch,
    child_schema: Vec<VarId>,
    tms: Vec<TmStream>,
    tm_idx: usize,
    window_rows: usize,
}

/// Whether PR-3 star TriplesMap-set pruning is enabled. Read once from
/// `FLUREE_R2RML_STAR_TM_PRUNE` (family falsy spellings,
/// [`super::env_switch_enabled`]). When on, a same-subject star resolves only
/// TriplesMaps that supply EVERY star predicate — a provably-empty prune of the
/// shared-base-predicate fan-out (a map missing a member produces no complete
/// star row). Off ⇒ today's base-predicate-only resolution.
fn star_tm_prune_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_R2RML_STAR_TM_PRUNE"))
}

/// PR-3 star resolution prune: whether a TriplesMap can contribute to a star,
/// combining fix (a) and fix (b'). Both are provably-empty prunes.
///
/// - **(a)** every predicate in `star_required_preds` must have a PredicateObjectMap
///   here — a same-subject star needs every member bound, and a map missing one
///   produces no complete star row (materialization is per-map, no cross-map join
///   for members). This prune preserves the pre-existing star-*formation* gap
///   (F10 in `04-findings-register.md`): required members split across
///   template-sharing maps already produce zero star rows, so pruning every map
///   is result-identical; the future fix lives in the rewrite (refuse to fuse
///   when no single map covers all members).
/// - **(b')** when `prune_class` is set (only when template-disjoint; see
///   [`R2rmlPattern::class_prune_hint`]), the map must declare that class.
///
/// Empty `star_required_preds` + `None` `prune_class` (switch off / not a star) ⇒
/// always `true`, i.e. today's behavior.
fn tm_passes_star_prune(
    tm: &TriplesMap,
    star_required_preds: &[String],
    prune_class: Option<&str>,
) -> bool {
    let has_all_preds = star_required_preds.iter().all(|p| {
        tm.predicate_object_maps
            .iter()
            .any(|pom| pom.predicate_map.as_constant() == Some(p.as_str()))
    });
    if !has_all_preds {
        return false;
    }
    if let Some(class) = prune_class {
        if !tm.classes().iter().any(|c| c.as_str() == class) {
            return false;
        }
    }
    true
}

/// R2RML scan operator for `Pattern::R2rml`.
///
/// Scans an Iceberg table through an R2RML mapping and produces RDF term bindings.
pub struct R2rmlScanOperator {
    /// Child operator providing input solutions (may be EmptyOperator seed)
    child: BoxedOperator,
    /// R2RML pattern from the query IR
    pattern: R2rmlPattern,
    /// Output schema (child schema + new vars from R2RML scan)
    schema: Arc<[VarId]>,
    /// Mapping from variables to output column positions
    out_pos: HashMap<VarId, usize>,
    /// Cached compiled mapping (loaded once in open)
    mapping: Option<Arc<CompiledR2rmlMapping>>,
    /// Pending output rows that overflowed the current output batch.
    pending: VecDeque<Vec<Binding>>,
    /// In-flight streaming scan for the current buffered child batch, advanced
    /// one window per `next_batch` so the whole table is never materialized.
    progress: Option<ScanProgress>,
    /// Inner-table scans cached across child batches, keyed by
    /// `(table_name, projection)`. A correlated join re-invokes `build_progress`
    /// once per child batch; without this the (dimension-sized) inner table is
    /// re-scanned every batch. Only inners up to one materialize window are
    /// cached, so a cached inner never exceeds the resident footprint a single
    /// scan window already holds; larger inners fall back to per-batch streaming.
    /// Only UNFILTERED scans are cached — a filtered scan may return a pruned
    /// subset, which the filter-agnostic key must never replay for another scan.
    scan_cache: HashMap<(String, Vec<String>), Arc<Vec<ColumnBatch>>>,
    /// PR-4: cross-child-batch parent-lookup memoization. A correlated join
    /// (OPTIONAL / ref) re-enters `build_progress` per child batch; without this
    /// the (dimension) parent tables are re-scanned every batch (q008: 123+
    /// parent scans, DNF → 8 scans with the memo). Keyed like `parent_lookups`:
    /// `(parent TriplesMap IRI, join_cols)` — the TM IRI, NOT the parent table
    /// name. Two parent TMs over one table can render different subject IRIs
    /// from the same join key (different subject templates), so a table-name
    /// key would replay the wrong lookup; don't "simplify" the key. The key
    /// deterministically fixes the parent projection at a stable `as_of_t`, so
    /// a memoized lookup is valid for every later batch. Bounded: a lookup
    /// larger than one materialize window is NOT retained (a fact-as-parent
    /// falls through to today's per-batch build) so the cache can't OOM. Gated
    /// by `FLUREE_R2RML_PARENT_MEMO`. (q050 is NOT fixed by this: its
    /// correlated OPTIONAL rebuilds the whole operator per row, resetting any
    /// operator-scoped cache — that's PR-4b.)
    parent_lookup_cache: HashMap<LookupCacheKey, Arc<ParentLookup>>,
    /// Whether cross-batch parent memoization is on for this operator. Read once
    /// from `parent_memo_enabled()` at construction (per-operator, not a global
    /// `OnceLock`), so a test can drive both regimes deterministically in one
    /// process and production still honors `FLUREE_R2RML_PARENT_MEMO`.
    parent_memo: bool,
    /// LIMIT early-termination budget: the max output rows a downstream `LIMIT`
    /// needs from this operator. `None` = unbounded. Set only when this is the
    /// topmost row-preserving scan (a scan feeding a join/FILTER never receives
    /// one), so once `emitted` reaches it the scan can stop without changing
    /// results. Also caps the materialize window so a `LIMIT n` does not
    /// materialize a full window before the first row.
    row_budget: Option<usize>,
    /// Output rows emitted so far, counted against `row_budget`.
    emitted: usize,
    /// A scan-local FILTER the planner folded into this scan (see
    /// [`R2rmlPattern::consumed_filter`]). Applied to each output batch with the
    /// same evaluator the dropped `FilterOperator` would use, so results are
    /// unchanged — but now the LIMIT budget and the filter live in one operator,
    /// so a `FILTER + LIMIT` scan can stop after enough *matching* rows.
    consumed_filter: Option<PreparedBoolExpression>,
    /// State
    state: OperatorState,
}

impl R2rmlScanOperator {
    /// Create a new R2RML scan operator.
    pub fn new(child: BoxedOperator, pattern: R2rmlPattern) -> Self {
        let child_schema = child.schema();

        // Build output schema: start with child vars, then add R2RML pattern vars
        let mut schema_vars: Vec<VarId> = child_schema.to_vec();
        let mut seen: HashSet<VarId> = schema_vars.iter().copied().collect();

        // Add subject variable if new (constant subjects bind no variable)
        if let Some(subject_var) = pattern.subject_var {
            if seen.insert(subject_var) {
                schema_vars.push(subject_var);
            }
        }

        // Add object variable if present and new
        if let Some(obj_var) = pattern.object_var {
            if seen.insert(obj_var) {
                schema_vars.push(obj_var);
            }
        }

        // Add predicate variable (`?s ?p ?o` / `<iri> ?p ?o`) if present and new
        if let Some(pred_var) = pattern.predicate_var {
            if seen.insert(pred_var) {
                schema_vars.push(pred_var);
            }
        }

        // Add type variable (`?s rdf:type ?type`) if present and new
        if let Some(type_var) = pattern.type_var {
            if seen.insert(type_var) {
                schema_vars.push(type_var);
            }
        }

        // Add same-subject star object variables if new
        for (_, var) in &pattern.star_bindings {
            if seen.insert(*var) {
                schema_vars.push(*var);
            }
        }

        // Build output position map
        let out_pos: HashMap<VarId, usize> = schema_vars
            .iter()
            .enumerate()
            .map(|(i, &v)| (v, i))
            .collect();

        let schema = Arc::from(schema_vars);

        let consumed_filter = pattern
            .consumed_filter
            .clone()
            .map(PreparedBoolExpression::new);

        Self {
            child,
            pattern,
            schema,
            out_pos,
            mapping: None,
            pending: VecDeque::new(),
            progress: None,
            scan_cache: HashMap::new(),
            parent_lookup_cache: HashMap::new(),
            parent_memo: parent_memo_enabled(),
            row_budget: None,
            emitted: 0,
            consumed_filter,
            state: OperatorState::Created,
        }
    }

    /// Build the output batch from accumulated `columns`, applying the consumed
    /// scan-local filter when present. Returns `None` when nothing survives (an
    /// empty window, or every row filtered out), so the caller keeps pulling.
    fn finalize_batch(
        &self,
        columns: Vec<Vec<Binding>>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<Batch>> {
        let batch = Batch::new(Arc::clone(&self.schema), columns)?;
        if batch.is_empty() {
            return Ok(None);
        }
        match &self.consumed_filter {
            Some(prepared) => filter_batch(&batch, prepared, &self.schema, ctx),
            None => Ok(Some(batch)),
        }
    }

    /// All predicate IRIs this pattern materializes: the base `predicate_filter`,
    /// same-subject star members (`star_bindings`), and fused constant-object
    /// constraints (`star_constraints`). Used for projection and parent-lookup
    /// building so a star scan reads every needed column in one pass — omitting
    /// the constraint predicates would leave the real (column-pruning) reader
    /// without the constraint's column, dropping every row.
    fn pattern_predicates(&self) -> Vec<&str> {
        let mut preds: Vec<&str> = Vec::new();
        if let Some(p) = self.pattern.predicate_filter.as_deref() {
            preds.push(p);
        }
        for (pred, _) in &self.pattern.star_bindings {
            preds.push(pred.as_str());
        }
        for (pred, _) in &self.pattern.star_constraints {
            preds.push(pred.as_str());
        }
        preds
    }

    /// Whether this pattern fuses multiple same-subject predicates into one scan
    /// (extra var members or constant-object constraints beyond the base). Such a
    /// scan must project/parent-lookup the union of all star predicates, not just
    /// the base `predicate_filter`.
    fn has_star_members(&self) -> bool {
        !self.pattern.star_bindings.is_empty() || !self.pattern.star_constraints.is_empty()
    }

    /// True for a pure `rdf:type`/subject-only pattern: no object var, no
    /// predicate filter, no star members. Such a pattern derives only the subject
    /// (its `rr:class` constraint is enforced by TriplesMap selection), so it
    /// needs only the subject columns and scans no RefObjectMap parents. A true
    /// wildcard `?s ?p ?o` is excluded — it has `object_var = Some` and must
    /// still materialize every POM/parent.
    fn is_subject_only_pattern(&self) -> bool {
        self.pattern.object_var.is_none()
            && self.pattern.predicate_filter.is_none()
            && self.pattern.star_bindings.is_empty()
    }

    /// Resolve this pattern's pushdown predicates (keyed by query variable) to
    /// table columns for the given TriplesMap, producing scan filters. A
    /// variable maps to a column via its predicate IRI; only plain `rr:column`
    /// scalar object maps are pushable (see [`value_pushdown_column`]).
    fn build_scan_filters(&self, triples_map: &TriplesMap) -> Vec<crate::r2rml::ScanFilter> {
        let mut out = Vec::new();
        for pd in &self.pattern.scan_filters {
            let pred_iri = if Some(pd.var) == self.pattern.object_var {
                self.pattern.predicate_filter.as_deref()
            } else {
                self.pattern
                    .star_bindings
                    .iter()
                    .find(|(_, v)| *v == pd.var)
                    .map(|(p, _)| p.as_str())
            };
            let Some(pred_iri) = pred_iri else { continue };

            // The predicate's values come from EVERY matching object map, so a
            // file-level prune is only sound when the predicate maps to exactly
            // one scalar object map backed by exactly one column. Otherwise a row
            // could match via a column we didn't prune on — skip the pushdown and
            // let the in-engine FILTER handle it.
            let mut matching = triples_map
                .predicate_object_maps
                .iter()
                .filter(|p| p.predicate_map.as_constant() == Some(pred_iri));
            let (Some(pom), None) = (matching.next(), matching.next()) else {
                continue;
            };
            let Some(col) = value_pushdown_column(&pom.object_map) else {
                continue;
            };
            out.push(crate::r2rml::ScanFilter {
                column: col.to_string(),
                op: pd.op,
                value: pd.value.clone(),
            });
        }

        // A scalar constant-object equality pushes as a scan filter too
        // (optimization; the operator enforces correctness). IRI constants are
        // operator-enforced only — a FK-key pushdown needs template reversal.
        if let (Some(crate::r2rml::ObjectConstant::Scalar(value)), Some(pred_iri)) = (
            &self.pattern.object_constant,
            self.pattern.predicate_filter.as_deref(),
        ) {
            let mut matching = triples_map
                .predicate_object_maps
                .iter()
                .filter(|p| p.predicate_map.as_constant() == Some(pred_iri));
            if let (Some(pom), None) = (matching.next(), matching.next()) {
                if let Some(col) = value_pushdown_column(&pom.object_map) {
                    out.push(crate::r2rml::ScanFilter {
                        column: col.to_string(),
                        op: crate::r2rml::ScanCmpOp::Eq,
                        value: value.clone(),
                    });
                }
            }
        }

        // Bound-subject key pushdown: reverse the subject template against the
        // constant IRI to recover each key column's raw value, and push it as an
        // equality so Iceberg can prune to the matching rows. Emitted
        // unconditionally, like the object-constant filters above; whether it is
        // *applied* is governed by the same reader-level pushdown kill-switch
        // (`FLUREE_ICEBERG_PREDICATE_PUSHDOWN`). Only unambiguously-reversible
        // template shapes yield filters (see `reverse_subject_template`); the
        // physical type is resolved later against the Iceberg schema, and
        // unsupported types are skipped. The operator still enforces the subject
        // equality, so a skipped or partial push is a perf choice, never a
        // correctness one.
        if let (Some(subject_iri), Some(template)) = (
            self.pattern.subject_constant.as_deref(),
            triples_map.subject_map.template.as_deref(),
        ) {
            if let Some(keys) = reverse_subject_template(template, subject_iri) {
                for (column, raw) in keys {
                    out.push(crate::r2rml::ScanFilter {
                        column,
                        op: crate::r2rml::ScanCmpOp::Eq,
                        value: crate::r2rml::ScanValue::TemplateKey(raw),
                    });
                }
            }
        }
        out
    }

    /// Materialize one window of a TriplesMap's produced column batches into
    /// variable assignments. Datatype Sids are resolved once into a
    /// `LiteralEncoder` (not per cell) and the window's batches are materialized
    /// in parallel on the rayon pool. The window (not the whole table) bounds the
    /// resident binding footprint.
    fn materialize_window(
        &self,
        triples_map: &TriplesMap,
        batches: &[ColumnBatch],
        parent_lookups: &HashMap<LookupCacheKey, Arc<ParentLookup>>,
        ref_shortcuts: &HashMap<LookupCacheKey, RefShortcut>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Vec<(VarId, Binding)>>> {
        use rayon::prelude::*;
        let encoder = LiteralEncoder::build(triples_map, ctx.active_snapshot);
        let pattern = &self.pattern;
        let per_batch: Vec<Vec<Vec<(VarId, Binding)>>> = batches
            .par_iter()
            .map(|batch| {
                materialize_batch(
                    pattern,
                    triples_map,
                    batch,
                    parent_lookups,
                    ref_shortcuts,
                    &encoder,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(per_batch.into_iter().flatten().collect())
    }

    /// Index the buffered child rows by their join-key values, so the streamed
    /// produced side can be probed against them. Mirrors the per-child key logic
    /// of the previous probe (poisoned → drop, unbound → wildcard) with the roles
    /// flipped (child indexed, produced streamed).
    fn build_join_plan(
        &self,
        join_vars: &[VarId],
        child_schema: &[VarId],
        child_batch: &Batch,
        ctx: &ExecutionContext<'_>,
    ) -> JoinPlan {
        if join_vars.is_empty() {
            return JoinPlan::Cross;
        }
        let store = ctx.binary_store.as_deref();
        let gv = ctx.graph_view();
        let gv = gv.as_ref();

        let mut full_index: HashMap<Vec<GroupKeyOwned>, Vec<usize>> = HashMap::new();
        let mut partial_rows: Vec<(usize, Vec<Option<GroupKeyOwned>>)> = Vec::new();
        for row_idx in 0..child_batch.len() {
            let mut key: Vec<Option<GroupKeyOwned>> = Vec::with_capacity(join_vars.len());
            let mut all_bound = true;
            let mut poisoned = false;
            for &jv in join_vars {
                let pos = child_schema.iter().position(|&v| v == jv).unwrap();
                let b = &child_batch.column_by_idx(pos).unwrap()[row_idx];
                if b.is_poisoned() {
                    poisoned = true;
                    break;
                }
                if b.is_bound() {
                    key.push(Some(binding_to_group_key_normalized(b, store, gv)));
                } else {
                    all_bound = false;
                    key.push(None);
                }
            }
            if poisoned {
                // A poisoned binding can never match — drop the row.
                continue;
            }
            if all_bound {
                let full: Vec<GroupKeyOwned> = key.into_iter().map(Option::unwrap).collect();
                full_index.entry(full).or_default().push(row_idx);
            } else {
                partial_rows.push((row_idx, key));
            }
        }
        JoinPlan::Hash {
            join_vars: join_vars.to_vec(),
            full_index,
            partial_rows,
        }
    }

    /// Buffer one child batch and set up its streaming scan: resolve the matching
    /// TriplesMap(s), scan each table (and any RefObjectMap parent tables), and
    /// build the per-TriplesMap join plan against the child. Returns `None` when
    /// no TriplesMap matches this pattern (the caller pulls the next child).
    async fn build_progress(
        &mut self,
        ctx: &ExecutionContext<'_>,
        child_batch: Batch,
    ) -> Result<Option<ScanProgress>> {
        let mapping = self
            .mapping
            .as_ref()
            .ok_or_else(|| QueryError::Internal("R2RML mapping not loaded".to_string()))?
            .clone();
        let child_schema = self.child.schema().to_vec();

        // PR-3 fix (a): a same-subject star requires every member predicate bound,
        // so only TriplesMaps supplying ALL star predicates can contribute — prune
        // the shared-base-predicate fan-out (a map missing a member produces no
        // complete star row, so this is a provably-empty prune, result-preserving).
        // Computed before the resolution closure, which borrows `self.pattern`.
        let star_prune_on = star_tm_prune_enabled();
        let star_required_preds: Vec<String> = if star_prune_on && self.has_star_members() {
            self.pattern_predicates()
                .iter()
                .map(|s| (*s).to_string())
                .collect()
        } else {
            Vec::new()
        };
        // PR-3 fix (b'): resolution-only class prune (template-disjoint; see
        // `R2rmlPattern::class_prune_hint`). Gated by the same switch as fix (a).
        let prune_class: Option<String> = if star_prune_on {
            self.pattern.class_prune_hint.clone()
        } else {
            None
        };

        // Resolve the TriplesMap(s) for this pattern (same for every child row).
        let triples_maps: Vec<&TriplesMap> = if let Some(ref tm_iri) = self.pattern.triples_map_iri
        {
            let tm = mapping.get(tm_iri).ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "TriplesMap '{tm_iri}' not found in R2RML mapping"
                ))
            })?;
            vec![tm]
        } else {
            mapping
                .triples_maps
                .values()
                .filter(|tm| {
                    // class_filter: only maps that produce this class.
                    if let Some(ref class_filter) = self.pattern.class_filter {
                        if !tm.classes().contains(class_filter) {
                            return false;
                        }
                    }
                    // predicate_filter: only maps that have this predicate.
                    if let Some(ref pred_filter) = self.pattern.predicate_filter {
                        let has_pred = tm.predicate_object_maps.iter().any(|pom| {
                            pom.predicate_map.as_constant() == Some(pred_filter.as_str())
                        });
                        if !has_pred {
                            return false;
                        }
                    }
                    // PR-3 fix (a) all-members-intersection + fix (b') class prune
                    // (both provably-empty; see `tm_passes_star_prune`). Inputs are
                    // empty/None when the switch is off or this is not a star.
                    if !tm_passes_star_prune(tm, &star_required_preds, prune_class.as_deref()) {
                        return false;
                    }
                    // subject_constant prune: a bound subject IRI can only come
                    // from a TriplesMap whose template subject can PRODUCE it, and
                    // every IRI a template yields starts with the template's
                    // constant prefix (the text before the first `{`, emitted
                    // verbatim by `expand_template`). So a subject IRI that does
                    // not start with this TM's constant prefix provably cannot be
                    // produced here — skip it. This turns a bound-subject inspect
                    // (`<iri> ?p ?o`) from a fan-out over every table into a scan
                    // of just the subject's own table (the per-row match at
                    // `subject_term_matches_iri` already enforces equality, so
                    // this is a necessary-condition prune: it can only over-keep,
                    // never drop a real match). Only a template subject is
                    // prunable; a column/constant subject (no template) is kept.
                    if let Some(ref subject_iri) = self.pattern.subject_constant {
                        if let Some(template) = tm.subject_map.template.as_deref() {
                            if !subject_iri.starts_with(super::rewrite::constant_prefix(template)) {
                                return false;
                            }
                        }
                    }
                    true
                })
                .collect()
        };

        if triples_maps.is_empty() {
            return Ok(None);
        }

        let table_provider = ctx.r2rml_table_provider.ok_or_else(|| {
            QueryError::InvalidQuery("R2RML table provider not configured".to_string())
        })?;

        // PR-8 slice 1: warm every TriplesMap table's catalog context CONCURRENTLY
        // before the serial per-map scan loop below, so a multi-table pattern's
        // per-table `loadTable` GETs overlap instead of summing. Best-effort (see
        // `prefetch_tables`); a single-table pattern is a no-op inside it.
        if super::parallel_catalog_resolution_enabled() {
            let mut tm_tables: Vec<String> = Vec::with_capacity(triples_maps.len());
            for tm in &triples_maps {
                if let Some(t) = tm.table_name() {
                    tm_tables.push(t.to_string());
                }
            }
            table_provider
                .prefetch_tables(&self.pattern.graph_source_id, &tm_tables)
                .await;
        }

        // Join vars (pattern-produced vars the child already binds) are the same
        // for every TriplesMap of this pattern.
        let join_vars: Vec<VarId> = self
            .pattern
            .produced_vars()
            .into_iter()
            .filter(|v| child_schema.contains(v))
            .collect();

        let mut tms: Vec<TmStream> = Vec::with_capacity(triples_maps.len());
        let mut seen: HashSet<String> = HashSet::new();

        for triples_map in &triples_maps {
            // Cancellation checkpoint before each TriplesMap's table scan: a
            // multi-TriplesMap pattern issues one `scan_table` (catalog loadTable
            // + plan) per map here without returning to `next_batch`.
            ctx.check_cancelled()?;
            if !seen.insert(triples_map.iri.clone()) {
                continue;
            }

            let table_name = triples_map.table_name().ok_or_else(|| {
                QueryError::InvalidQuery("TriplesMap has no logical table".to_string())
            })?;

            // Determine projection columns. For a same-subject star, project the
            // union of columns needed for every star predicate so the whole star
            // is satisfied by one scan.
            let projection: Vec<String> = if !self.has_star_members() {
                if self.is_subject_only_pattern() {
                    // rdf:type / subject-only pattern: only the subject columns are
                    // load-bearing. Projecting every POM column (the
                    // `columns_for_predicate(None)` fallback) reads FK/value columns
                    // that subject-only materialization never consults.
                    triples_map
                        .subject_columns()
                        .into_iter()
                        .map(std::string::ToString::to_string)
                        .collect()
                } else {
                    triples_map
                        .columns_for_predicate(self.pattern.predicate_filter.as_deref())
                        .into_iter()
                        .map(std::string::ToString::to_string)
                        .collect()
                }
            } else {
                let mut cols: Vec<String> = Vec::new();
                for pred in self.pattern_predicates() {
                    cols.extend(
                        triples_map
                            .columns_for_predicate(Some(pred))
                            .into_iter()
                            .map(std::string::ToString::to_string),
                    );
                }
                cols.sort();
                cols.dedup();
                cols
            };

            // Scan the table, pushing resolved FILTER predicates for file pruning
            // (column resolution needs the mapping, so it happens here).
            let scan_filters = self.build_scan_filters(triples_map);
            let as_of_t = if ctx.dataset.is_some() {
                None
            } else {
                Some(ctx.to_t)
            };
            // Reuse an already-materialized inner scan across child batches: a
            // correlated join calls `build_progress` once per child batch, so
            // without this the (dimension-sized) inner table is re-scanned every
            // batch. The first scan of a `(table, projection)` is collected (up to
            // one window) and replayed for later batches; a larger inner streams
            // fresh each batch as before.
            //
            // Only unfiltered scans are cached. A pushdown `scan_filter` can prune
            // files, so a filtered scan may yield a row SUBSET; the cache key is
            // `(table, projection)` and does not carry the filter, so replaying a
            // pruned subset for a differently-filtered (or unfiltered) scan of the
            // same table/projection would drop rows. Filtered scans therefore
            // bypass the cache entirely (both read and write).
            //
            // A budgeted scan (under a LIMIT) also bypasses the cache: caching
            // collects a full window before the operator can stop early, which
            // would defeat the LIMIT. A budgeted scan is the topmost
            // row-preserving scan, so it stops after ~a batch and gains little
            // from cross-batch reuse anyway.
            let cacheable =
                scan_cache_enabled() && scan_filters.is_empty() && self.row_budget.is_none();
            let cache_key = (table_name.to_string(), projection.clone());
            let stream: ColumnBatchStream = if !cacheable {
                table_provider
                    .scan_table(
                        &self.pattern.graph_source_id,
                        table_name,
                        &projection,
                        &scan_filters,
                        as_of_t,
                    )
                    .await?
            } else if let Some(cached) = self.scan_cache.get(&cache_key) {
                replay_stream(Arc::clone(cached))
            } else {
                let fresh = table_provider
                    .scan_table(
                        &self.pattern.graph_source_id,
                        table_name,
                        &projection,
                        &scan_filters,
                        as_of_t,
                    )
                    .await?;
                match collect_scan_capped(fresh, materialize_window_rows()).await? {
                    CollectedScan::Complete(batches) => {
                        let arc = Arc::new(batches);
                        self.scan_cache.insert(cache_key, Arc::clone(&arc));
                        replay_stream(arc)
                    }
                    CollectedScan::Overflow(prefix, remainder) => {
                        Box::pin(futures::stream::iter(prefix.into_iter().map(Ok)).chain(remainder))
                    }
                }
            };

            // Build parent lookup tables for RefObjectMap POMs that pass the
            // predicate filter. Parent (dimension) tables are small and consumed
            // whole into the lookup, so they are not streamed.
            let parent_memo = self.parent_memo;
            let mut parent_lookups: HashMap<LookupCacheKey, Arc<ParentLookup>> = HashMap::new();
            let mut ref_shortcuts: HashMap<LookupCacheKey, RefShortcut> = HashMap::new();
            // Trusted browse crawl (`trust_fk_refs`) over the injected TRUE-wildcard
            // scan (`?s ?p ?o`): render RefObjectMap objects by templating the
            // parent IRI from the child's own FK columns and SKIP the parent-table
            // scan. Gated on the exact true-wildcard shape (the `else => true`
            // branch of `filtered_poms` below) so a predicate-filtered ref (a WHERE
            // `?s <ref> ?o` join) or a star/predicate-list crawl keeps the scan +
            // dangling-FK semantics and its subject set — a ref used as a subject
            // filter must not be relaxed to a match.
            let ref_template_shortcut = ctx.trust_fk_refs
                && !self.has_star_members()
                && self.pattern.predicate_filter.is_none()
                && self.pattern.object_var.is_some();
            // Scoped so `star_preds` (which borrows `self`) is released before the
            // parent-lookup loop mutates `self.parent_lookup_cache` (PR-4).
            let filtered_poms: Vec<_> = {
                let star_preds = self.pattern_predicates();
                triples_map
                    .predicate_object_maps
                    .iter()
                    .filter(|pom| {
                        if self.has_star_members() {
                            pom.predicate_map
                                .as_constant()
                                .is_some_and(|p| star_preds.contains(&p))
                        } else if let Some(ref pred_filter) = self.pattern.predicate_filter {
                            pom.predicate_map.as_constant() == Some(pred_filter.as_str())
                        } else if self.pattern.object_var.is_none() {
                            // rdf:type / subject-only pattern: no POM is load-bearing
                            // (the parent scans it would trigger are pure dead work,
                            // as subject-only materialization never reads
                            // object/parent values). The all-POMs branch below is for
                            // a TRUE wildcard `?s ?p ?o`, where `?p`/`?o` range over
                            // every predicate.
                            false
                        } else {
                            true
                        }
                    })
                    .collect()
            };

            for pom in &filtered_poms {
                // Cancellation checkpoint before each FK-parent scan: a subject
                // with many RefObjectMap POMs scans one parent dimension per POM
                // here. (Known residual: `collect_stream` then drains one parent
                // fully before the next poll — acceptable, parents are small dims.)
                ctx.check_cancelled()?;
                if let ObjectMap::RefObjectMap(ref rom) = pom.object_map {
                    let mut parent_join_cols: Vec<String> = rom
                        .parent_columns()
                        .into_iter()
                        .map(std::string::ToString::to_string)
                        .collect();
                    parent_join_cols.sort();
                    let lookup_key: LookupCacheKey =
                        (rom.parent_triples_map.clone(), parent_join_cols.clone());

                    // A given key populates exactly one of the two maps; skip if
                    // either already holds it (else the 2nd POM sharing a parent
                    // re-introduces the parent scan we removed).
                    if parent_lookups.contains_key(&lookup_key)
                        || ref_shortcuts.contains_key(&lookup_key)
                    {
                        continue;
                    }

                    // PR-4: reuse a parent lookup memoized in an earlier child batch
                    // (skips the parent-table re-scan — the q008 fix). The key
                    // `(parent_tm, join_cols)` fixes the projection and the lookup
                    // content at a stable `as_of_t`, so a prior batch's lookup is
                    // valid here. The ref-shortcut path below never populates the
                    // cache, and the shortcut-vs-scan choice is query-stable, so a
                    // cached scan-lookup is never mixed with a shortcut key.
                    if parent_memo {
                        if let Some(cached) = self.parent_lookup_cache.get(&lookup_key) {
                            parent_lookups.insert(lookup_key, Arc::clone(cached));
                            continue;
                        }
                    }

                    // PR-8b: reuse a parent lookup memoized by an EARLIER OPERATOR
                    // INSTANCE this query. An inner join with an interposed
                    // non-pushable FILTER + LIMIT rebuilds this R2RML operator per
                    // driving batch, resetting `self.parent_lookup_cache` above (the
                    // q031 seam); this query-scoped memo — keyed with
                    // `graph_source_id` + `as_of_t`, so a cross-operator/cross-source
                    // share can't alias — survives the rebuild. On a hit, seed the
                    // per-operator cache too so later batches of THIS instance take
                    // the fast local path.
                    if parent_memo {
                        let as_of_t = if ctx.dataset.is_some() {
                            None
                        } else {
                            Some(ctx.to_t)
                        };
                        let ctx_key: R2rmlParentMemoKey = (
                            self.pattern.graph_source_id.clone(),
                            rom.parent_triples_map.clone(),
                            parent_join_cols.clone(),
                            as_of_t,
                        );
                        let hit = ctx.r2rml_parent_memo.lock().unwrap().get(&ctx_key);
                        if let Some(cached) = hit {
                            self.parent_lookup_cache
                                .insert(lookup_key.clone(), Arc::clone(&cached));
                            parent_lookups.insert(lookup_key, cached);
                            continue;
                        }
                    }

                    let parent_tm = match mapping.get(&rom.parent_triples_map) {
                        Some(tm) => tm,
                        None => {
                            tracing::warn!(
                                parent = %rom.parent_triples_map,
                                "Parent TriplesMap not found for RefObjectMap, skipping"
                            );
                            continue;
                        }
                    };

                    // Trusted browse crawl: if the parent subject is provably
                    // templatable from the child's FK columns, store a shortcut and
                    // skip the parent scan (the ref IRI is rendered from the child
                    // at materialize time; dangling-FK relaxed). Falls through to
                    // the scan when not provably safe (composite FK, non-template /
                    // column / constant / blank-node parent subject, non-FK
                    // template column) — the scan then stays authoritative.
                    if ref_template_shortcut {
                        if let Some(shortcut) = build_ref_shortcut(parent_tm, rom) {
                            ref_shortcuts.insert(lookup_key, shortcut);
                            continue;
                        }
                    }

                    let parent_table = match parent_tm.table_name() {
                        Some(name) => name,
                        None => {
                            tracing::warn!(
                                parent = %rom.parent_triples_map,
                                "Parent TriplesMap has no logical table, skipping"
                            );
                            continue;
                        }
                    };

                    // Columns needed from the parent: join columns + subject
                    // template columns (+ rr:column if the subject uses one).
                    let mut parent_projection: Vec<String> = parent_join_cols.clone();
                    parent_projection
                        .extend(parent_tm.subject_map.template_columns.iter().cloned());
                    if let Some(ref col) = parent_tm.subject_map.column {
                        parent_projection.push(col.clone());
                    }
                    parent_projection.sort();
                    parent_projection.dedup();

                    let as_of_t = if ctx.dataset.is_some() {
                        None
                    } else {
                        Some(ctx.to_t)
                    };
                    // Parent (dimension) tables are small; collect the stream
                    // fully into the lookup rather than streaming it.
                    let parent_stream = table_provider
                        .scan_table(
                            &self.pattern.graph_source_id,
                            parent_table,
                            &parent_projection,
                            &[],
                            as_of_t,
                        )
                        .await?;
                    let parent_batches = collect_stream(parent_stream).await?;

                    let lookup = Arc::new(build_parent_lookup(
                        parent_tm,
                        &parent_join_cols,
                        parent_batches,
                    )?);
                    // Memoize across child batches unless the lookup exceeds one
                    // materialize window — a fact-as-parent (q015) is used for this
                    // batch but NOT retained, falling through to today's per-batch
                    // rebuild so the cache can't grow unbounded. The window is
                    // env-tunable (`FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS`), so
                    // raising it also raises what each memo key may retain —
                    // intentional (both bound the same working-set notion), but a
                    // window bump knowingly buys a bigger cache.
                    if parent_memo && lookup.len() <= materialize_window_rows() {
                        self.parent_lookup_cache
                            .insert(lookup_key.clone(), Arc::clone(&lookup));
                        // PR-8b: also publish to the query-scoped memo so a later
                        // operator rebuild reuses it. Refused (silently falling
                        // through to a per-batch rebuild) if it would push the memo
                        // past its total-rows cap — bounding cross-parent
                        // accumulation the per-operator cache never had.
                        let as_of_t = if ctx.dataset.is_some() {
                            None
                        } else {
                            Some(ctx.to_t)
                        };
                        let ctx_key: R2rmlParentMemoKey = (
                            self.pattern.graph_source_id.clone(),
                            rom.parent_triples_map.clone(),
                            parent_join_cols.clone(),
                            as_of_t,
                        );
                        ctx.r2rml_parent_memo.lock().unwrap().try_insert(
                            ctx_key,
                            &lookup,
                            parent_memo_total_cap_rows(),
                        );
                    }
                    parent_lookups.insert(lookup_key, lookup);
                }
            }

            let join = self.build_join_plan(&join_vars, &child_schema, &child_batch, ctx);

            tms.push(TmStream {
                tm_iri: triples_map.iri.clone(),
                stream,
                exhausted: false,
                parent_lookups,
                ref_shortcuts,
                join,
            });
        }

        // Under a LIMIT, cap the materialize window at the remaining budget so a
        // `LIMIT n` does not explode a full 512K-row window into bindings before
        // the first output row.
        // Cap the window to the remaining LIMIT budget. This holds for a
        // consumed filter too: the budget counts *matching* rows while a window
        // materializes unfiltered rows, but the `next_batch` loop re-checks the
        // post-filter `emitted` and keeps pulling more windows until the budget
        // is met, so a bounded window can never under-return — it only avoids
        // materializing a full window before the filter runs.
        let window_rows = match self.row_budget {
            Some(b) => materialize_window_rows().min(b.saturating_sub(self.emitted).max(1)),
            None => materialize_window_rows(),
        };

        Ok(Some(ScanProgress {
            child_batch,
            child_schema,
            tms,
            tm_idx: 0,
            window_rows,
        }))
    }

    /// Pull and materialize the next window from the in-flight scan and emit its
    /// rows. Each call pulls one bounded window of batches from the current
    /// TriplesMap's stream (so only O(window + in-flight files) is resident),
    /// materializes them in parallel, emits, and drops the window. Returns `true`
    /// while batches remain, `false` once the scan for this child batch is fully
    /// consumed.
    async fn advance_one_window(
        &mut self,
        ctx: &ExecutionContext<'_>,
        progress: &mut ScanProgress,
        num_cols: usize,
        columns: &mut [Vec<Binding>],
    ) -> Result<bool> {
        let mapping = self
            .mapping
            .as_ref()
            .ok_or_else(|| QueryError::Internal("R2RML mapping not loaded".to_string()))?
            .clone();

        while progress.tm_idx < progress.tms.len() {
            let i = progress.tm_idx;
            if progress.tms[i].exhausted {
                progress.tm_idx += 1;
                continue;
            }

            // Pull a window of batches from the stream: at least one, then up to
            // the row budget. The stream itself bounds in-flight file decodes.
            let mut window: Vec<ColumnBatch> = Vec::new();
            let mut rows = 0usize;
            while rows < progress.window_rows {
                // Cancellation checkpoint at row-group granularity: stop before
                // pulling (and decoding) the next Parquet batch. This is the
                // load-bearing poll for a runaway streaming scan of a large fact
                // table — a pure relaxed-atomic flag read, unmeasurable here.
                ctx.check_cancelled()?;
                match progress.tms[i].stream.next().await {
                    Some(batch) => {
                        let batch = batch?;
                        rows += batch.num_rows;
                        window.push(batch);
                    }
                    None => {
                        progress.tms[i].exhausted = true;
                        break;
                    }
                }
            }

            if window.is_empty() {
                // Stream ended with nothing left for this TriplesMap.
                progress.tm_idx += 1;
                continue;
            }

            let triples_map = mapping.get(&progress.tms[i].tm_iri).ok_or_else(|| {
                QueryError::Internal(format!(
                    "TriplesMap '{}' missing from mapping mid-scan",
                    progress.tms[i].tm_iri
                ))
            })?;
            let produced = self.materialize_window(
                triples_map,
                &window,
                &progress.tms[i].parent_lookups,
                &progress.tms[i].ref_shortcuts,
                ctx,
            )?;

            if !produced.is_empty() {
                emit_produced_window(
                    &self.out_pos,
                    &progress.child_schema,
                    &progress.child_batch,
                    &progress.tms[i].join,
                    &produced,
                    num_cols,
                    ctx.batch_size,
                    columns,
                    &mut self.pending,
                    ctx,
                )?;
            }
            // Geometric window growth. A budgeted (LIMIT) scan starts with a small
            // window (~the remaining budget) so a selective query does not explode
            // a full window into bindings before the first output row. But when the
            // produced rows feed an internal join that filters most of them out,
            // the output budget is never met, and a fixed tiny window would re-scan
            // the whole table in many small passes (slower than the un-budgeted
            // full-window path — fluree/db#1406 review). Growing the window each
            // pass ramps it up to the full materialize size after a handful of
            // low-yield passes, so the pathological case self-corrects while a
            // genuinely selective LIMIT still stops after its cheap first window.
            // The un-budgeted window already starts at the full size, so `.min`
            // makes this a no-op there.
            progress.window_rows = progress
                .window_rows
                .saturating_mul(4)
                .min(materialize_window_rows());
            // `window` is dropped here, freeing the batches before the next pull.
            return Ok(true);
        }
        Ok(false)
    }
}

/// Drain a [`ColumnBatchStream`] fully into a vector. Used for small dimension
/// (parent) tables whose entire contents become a lookup.
async fn collect_stream(mut stream: ColumnBatchStream) -> Result<Vec<ColumnBatch>> {
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.push(batch?);
    }
    Ok(out)
}

/// Whether the correlated inner-scan cache is enabled. Read once from
/// `FLUREE_R2RML_SCAN_CACHE` (family falsy spellings,
/// [`super::env_switch_enabled`]); disabling restores the per-child-batch
/// re-scan behavior.
fn scan_cache_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_R2RML_SCAN_CACHE"))
}

/// Whether PR-4 cross-batch parent-lookup memoization is enabled. Read once from
/// `FLUREE_R2RML_PARENT_MEMO` (family falsy spellings,
/// [`super::env_switch_enabled`]). Off ⇒ today's per-child-batch parent-lookup
/// rebuild.
fn parent_memo_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_R2RML_PARENT_MEMO"))
}

/// Outcome of trying to fully collect an inner scan for caching.
enum CollectedScan {
    /// The whole inner fit within the cap — safe to cache and replay.
    Complete(Vec<ColumnBatch>),
    /// The inner exceeded the cap — too large to cache. The prefix already
    /// pulled plus the still-open remainder serve this one batch.
    Overflow(Vec<ColumnBatch>, ColumnBatchStream),
}

/// Collect `stream` until it ends (→ `Complete`, cacheable) or its row count
/// reaches `cap` with more remaining (→ `Overflow`, too large to cache). The cap
/// equals one materialize window, so a cached inner never exceeds the resident
/// footprint a single scan window already materializes.
async fn collect_scan_capped(mut stream: ColumnBatchStream, cap: usize) -> Result<CollectedScan> {
    let mut collected = Vec::new();
    let mut rows = 0usize;
    while rows < cap {
        match stream.next().await {
            Some(batch) => {
                let batch = batch?;
                rows += batch.num_rows;
                collected.push(batch);
            }
            None => return Ok(CollectedScan::Complete(collected)),
        }
    }
    Ok(CollectedScan::Overflow(collected, stream))
}

/// A [`ColumnBatchStream`] that replays cached batches. `ColumnBatch` clones are
/// cheap (its columns are `Arc`-backed), so replay does not re-copy the data.
fn replay_stream(batches: Arc<Vec<ColumnBatch>>) -> ColumnBatchStream {
    Box::pin(futures::stream::iter(
        (0..batches.len()).map(move |i| Ok(batches[i].clone())),
    ))
}

/// Emit one combined output row: the child row's bindings overlaid with a
/// produced assignment, into `columns` (or `pending` once the batch is full).
///
/// The common (not-yet-full) path writes straight into the columnar buffers
/// instead of allocating a per-row `Vec<Binding>` — that per-row allocation was
/// the single largest heap-allocation site for analytical R2RML scans.
#[allow(clippy::too_many_arguments)]
fn emit_combined_row(
    out_pos: &HashMap<VarId, usize>,
    child_schema: &[VarId],
    child_batch: &Batch,
    child_row_idx: usize,
    prod: &[(VarId, Binding)],
    num_cols: usize,
    batch_size: usize,
    columns: &mut [Vec<Binding>],
    pending: &mut VecDeque<Vec<Binding>>,
) {
    if columns[0].len() < batch_size {
        // Push an `Unbound` placeholder to every column, then overwrite the
        // bound positions in place — no per-row temporary vector.
        for col in columns.iter_mut() {
            col.push(Binding::Unbound);
        }
        for (col_idx, &var) in child_schema.iter().enumerate() {
            let out_idx = *out_pos.get(&var).unwrap();
            *columns[out_idx].last_mut().unwrap() =
                child_batch.column_by_idx(col_idx).unwrap()[child_row_idx].clone();
        }
        for (var, binding) in prod {
            *columns[*out_pos.get(var).unwrap()].last_mut().unwrap() = binding.clone();
        }
    } else {
        // Overflow path: the batch is full, so stage a complete row for `pending`.
        let mut out_row: Vec<Binding> = vec![Binding::Unbound; num_cols];
        for (col_idx, &var) in child_schema.iter().enumerate() {
            out_row[*out_pos.get(&var).unwrap()] =
                child_batch.column_by_idx(col_idx).unwrap()[child_row_idx].clone();
        }
        for (var, binding) in prod {
            out_row[*out_pos.get(var).unwrap()] = binding.clone();
        }
        pending.push_back(out_row);
    }
}

/// Combine a window of produced rows with the buffered child rows per the join
/// plan, emitting into `columns`/`pending`. The produced side is the streamed
/// (large) side; the child index was built once in `build_progress`.
#[allow(clippy::too_many_arguments)]
fn emit_produced_window(
    out_pos: &HashMap<VarId, usize>,
    child_schema: &[VarId],
    child_batch: &Batch,
    join: &JoinPlan,
    produced: &[Vec<(VarId, Binding)>],
    num_cols: usize,
    batch_size: usize,
    columns: &mut [Vec<Binding>],
    pending: &mut VecDeque<Vec<Binding>>,
    ctx: &ExecutionContext<'_>,
) -> Result<()> {
    let mut emit = |child_row_idx: usize, prod: &[(VarId, Binding)]| -> Result<()> {
        ctx.tracker.consume_fuel(1)?;
        emit_combined_row(
            out_pos,
            child_schema,
            child_batch,
            child_row_idx,
            prod,
            num_cols,
            batch_size,
            columns,
            pending,
        );
        Ok(())
    };

    match join {
        JoinPlan::Cross => {
            // No shared vars: every produced row pairs with every child row
            // (child is usually the single seed row).
            for prod in produced {
                for child_row_idx in 0..child_batch.len() {
                    emit(child_row_idx, prod)?;
                }
            }
        }
        JoinPlan::Hash {
            join_vars,
            full_index,
            partial_rows,
        } => {
            let store = ctx.binary_store.as_deref();
            let gv = ctx.graph_view();
            let gv = gv.as_ref();
            for prod in produced {
                // A produced row always binds every pattern var, so its join key
                // is complete.
                let pkey: Vec<GroupKeyOwned> = join_vars
                    .iter()
                    .filter_map(|jv| {
                        prod.iter()
                            .find(|(v, _)| v == jv)
                            .map(|(_, b)| binding_to_group_key_normalized(b, store, gv))
                    })
                    .collect();
                if pkey.len() != join_vars.len() {
                    continue;
                }
                // Fully-bound child rows: exact hash probe.
                if let Some(rows) = full_index.get(&pkey) {
                    for &child_row_idx in rows {
                        emit(child_row_idx, prod)?;
                    }
                }
                // Child rows with an unbound (wildcard) join var: match those that
                // agree on every bound position.
                for (child_row_idx, partial) in partial_rows {
                    let agrees = partial
                        .iter()
                        .zip(pkey.iter())
                        .all(|(c, p)| c.as_ref().is_none_or(|c| c == p));
                    if agrees {
                        emit(*child_row_idx, prod)?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Datatype Sids resolved once per scan instead of per literal cell — the
/// per-cell `encode_iri` was a large share of materialization cost. Shared with
/// the fused-aggregate operator so its filter/expression eval-var bindings are
/// encoded identically to the normal materialization path.
pub(crate) struct LiteralEncoder {
    dt_sids: HashMap<String, fluree_db_core::Sid>,
    xsd_string: fluree_db_core::Sid,
}

impl LiteralEncoder {
    pub(crate) fn build(
        triples_map: &TriplesMap,
        snapshot: &fluree_db_core::LedgerSnapshot,
    ) -> Self {
        let fallback = fluree_db_core::Sid::new(2, "string");
        let mut dt_sids: HashMap<String, fluree_db_core::Sid> = HashMap::new();
        for pom in &triples_map.predicate_object_maps {
            if let Some(dt) = object_map_datatype(&pom.object_map) {
                dt_sids
                    .entry(dt.to_string())
                    .or_insert_with(|| snapshot.encode_iri(dt).unwrap_or_else(|| fallback.clone()));
            }
        }
        let xsd_string = snapshot.encode_iri(xsd::STRING).unwrap_or(fallback);
        Self {
            dt_sids,
            xsd_string,
        }
    }

    /// Convert an RdfTerm to a Binding without touching the snapshot (datatype
    /// Sids are pre-resolved). IRIs are kept as raw strings — graph source IRIs
    /// are independent of any Fluree namespace table.
    pub(crate) fn encode(&self, term: &RdfTerm) -> Binding {
        use fluree_db_core::FlakeValue;
        use fluree_vocab::UnresolvedDatatypeConstraint as Udc;
        match term {
            RdfTerm::Iri(iri) => Binding::iri(iri.as_str()),
            RdfTerm::BlankNode(id) => Binding::iri(format!("_:{id}")),
            RdfTerm::Literal { value, dtc } => match dtc {
                Some(Udc::LangTag(lang)) => {
                    Binding::lit_lang(FlakeValue::String(value.clone()), lang.as_ref())
                }
                Some(Udc::Explicit(dt_iri)) => {
                    let dt_sid = self
                        .dt_sids
                        .get(dt_iri.as_ref())
                        .cloned()
                        .unwrap_or_else(|| self.xsd_string.clone());
                    // Coerce numeric XSD literals from string to typed FlakeValue
                    // (arithmetic reads the value, not the datatype Sid);
                    // non-numeric datatypes keep their string form.
                    let val = match fluree_db_core::coerce_value(
                        FlakeValue::String(value.clone()),
                        dt_iri.as_ref(),
                    ) {
                        Ok(
                            c @ (FlakeValue::Long(_)
                            | FlakeValue::Double(_)
                            | FlakeValue::BigInt(_)
                            | FlakeValue::Decimal(_)),
                        ) => c,
                        _ => FlakeValue::String(value.clone()),
                    };
                    Binding::lit(val, dt_sid)
                }
                _ => Binding::lit(FlakeValue::String(value.clone()), self.xsd_string.clone()),
            },
        }
    }
}

/// The backing column of an object map whose materialized literal value is the
/// raw column value *verbatim*, so a column-level scan filter comparing that
/// value cannot drop a row the operator would keep. Only plain `rr:column`
/// qualifies: `Template` transforms the value (`"PREFIX-{code}"` ≠ `code`),
/// `Constant` ignores the row, and `RefObjectMap` is an IRI join. Pushing a
/// filter for those compares the raw column against a transformed constant and
/// silently prunes matching rows, violating the pushdown-is-only-an-optimization
/// invariant.
fn value_pushdown_column(om: &ObjectMap) -> Option<&str> {
    match om {
        ObjectMap::Column { column, .. } => Some(column.as_str()),
        _ => None,
    }
}

/// Datatype IRI declared by an ObjectMap, if any (column/template/constant).
fn object_map_datatype(om: &ObjectMap) -> Option<&str> {
    use fluree_db_r2rml::mapping::ConstantValue;
    match om {
        ObjectMap::Column { datatype, .. } | ObjectMap::Template { datatype, .. } => {
            datatype.as_deref()
        }
        ObjectMap::Constant {
            value: ConstantValue::Literal { datatype, .. },
        } => datatype.as_deref(),
        _ => None,
    }
}

/// Materialize the object term for one POM at a table row, resolving a
/// RefObjectMap through the pre-built parent lookup. Free fn so it runs off the
/// operator inside a rayon worker.
fn materialize_pom_object(
    pom: &PredicateObjectMap,
    iceberg_batch: &ColumnBatch,
    table_row_idx: usize,
    parent_lookups: &HashMap<(String, Vec<String>), Arc<ParentLookup>>,
    ref_shortcuts: &HashMap<LookupCacheKey, RefShortcut>,
) -> Result<Option<RdfTerm>> {
    if let ObjectMap::RefObjectMap(ref rom) = pom.object_map {
        let child_columns: Vec<String> = rom
            .child_columns()
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect();
        // A null value in any join column means no FK reference at all → no triple
        // (both the scan and the shortcut agree; the shortcut relaxes only a
        // present-but-dangling FK, never a null one).
        let child_key = match get_join_key_from_batch(&child_columns, iceberg_batch, table_row_idx)
        {
            Some(k) => k,
            None => return Ok(None),
        };
        let mut parent_join_cols: Vec<String> = rom
            .parent_columns()
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect();
        parent_join_cols.sort();
        let lookup_key = (rom.parent_triples_map.clone(), parent_join_cols);
        // Trusted browse crawl: render the parent IRI from the child's own FK
        // columns via the parent subject template — no parent scan. `child_key`
        // is in `child_columns()` (declared) order, positionally aligned with
        // `parent_columns()`; `build_ref_shortcut` only fires for a single-column
        // FK, so this is one pair. Byte-identical to the scan path for a matched
        // row (same template + `iri_escape`; the join guarantees the child FK
        // value equals the parent key), differing only for a dangling FK (a
        // templated IRI instead of no triple — the intended browse relaxation).
        if let Some(sc) = ref_shortcuts.get(&lookup_key) {
            let values: HashMap<String, Option<String>> = rom
                .parent_columns()
                .iter()
                .zip(&child_key)
                .map(|(pc, cv)| ((*pc).to_string(), Some(cv.clone())))
                .collect();
            return Ok(expand_template(&sc.subject_template, &values)
                .ok()
                .map(RdfTerm::iri));
        }
        Ok(parent_lookups
            .get(&lookup_key)
            .and_then(|l| l.get(&child_key))
            .cloned())
    } else {
        Ok(materialize_object_from_batch(
            &pom.object_map,
            iceberg_batch,
            table_row_idx,
        )?)
    }
}

/// Whether a materialized object term equals a constant-object constraint.
/// IRI constants match exactly; literal (scalar) constants are loose-matched
/// (gated in `convert_triple_to_r2rml`), comparing the value and ignoring the
/// materialized term's datatype/language.
///
/// Integer comparison never uses `f64` (which would admit false positives across
/// adjacent large integers). It matches an exact integer lexical form always,
/// and a decimal lexical form (`"100.00"`) only when `numeric_column` — i.e. the
/// object's backing column is Decimal/Float. The Arrow scan filter casts the
/// pushed integer literal into the column's own type, so it keeps `100.00` for a
/// numeric column but drops a text `"100.00"` cell; mirroring that here keeps the
/// operator match a superset of the scan filter (pushdown never drops a kept row)
/// while still answering `?s :amount 100` against a `DECIMAL(10,2)` column.
///
/// For a `Decimal` object, `decimal_canonical` may carry the constant's
/// precomputed canonical string (see [`decimal_canonical_of`]); an exact lexical
/// match (the common same-scale case) then skips the per-row `BigDecimal` parse,
/// while a scale variant (`"9.990"` vs `"9.99"`) falls back to the numeric compare.
fn rdf_term_eq_object_constant_cached(
    term: &RdfTerm,
    constant: &crate::r2rml::ObjectConstant,
    numeric_column: bool,
    decimal_canonical: Option<&str>,
) -> bool {
    use crate::r2rml::{ObjectConstant, ScanValue};
    match constant {
        // Bound IRI / ref object: exact IRI match.
        ObjectConstant::Iri(iri) => matches!(term, RdfTerm::Iri(v) if v == iri),
        // Decimal / big-integer object: numeric (scale-insensitive) match, so a
        // query `9.99` matches a column materialized as `9.990`.
        ObjectConstant::Decimal(d) => {
            let RdfTerm::Literal { value: v, .. } = term else {
                return false;
            };
            if decimal_canonical == Some(v.as_str()) {
                return true;
            }
            v.parse::<bigdecimal::BigDecimal>().is_ok_and(|x| &x == d)
        }
        // Double object: exact f64 value match.
        ObjectConstant::Double(f) => {
            let RdfTerm::Literal { value: v, .. } = term else {
                return false;
            };
            v.parse::<f64>().is_ok_and(|x| x == *f)
        }
        // Literal object: loose value match, ignoring datatype/language.
        ObjectConstant::Scalar(value) => {
            let RdfTerm::Literal { value: v, .. } = term else {
                return false;
            };
            match value {
                ScanValue::Str(s) => v == s,
                ScanValue::Int(n) => {
                    v.parse::<i64>().is_ok_and(|x| x == *n)
                        || (numeric_column && decimal_lexical_eq_int(v, *n))
                }
                ScanValue::Bool(b) => match v.as_str() {
                    "true" | "1" => *b,
                    "false" | "0" => !*b,
                    _ => false,
                },
                // The subject/object date column materializes as ISO 8601; parse
                // it back to days-since-epoch and compare to the constant.
                ScanValue::Date(days) => {
                    fluree_db_core::Date::parse(v).is_ok_and(|d| d.days_since_epoch() == *days)
                }
                // A TemplateKey is only ever a reversed subject-key filter, never
                // an object constant, so it never matches an object term.
                ScanValue::TemplateKey(_) => false,
            }
        }
    }
}

/// Whether a materialized subject term equals a constant (bound) subject IRI.
/// Subject maps always produce IRIs, so a non-IRI term never matches.
fn subject_term_matches_iri(term: &RdfTerm, want: &str) -> bool {
    matches!(term, RdfTerm::Iri(v) if v == want)
}

/// The constant's precomputed `BigDecimal::to_string()`, for an
/// `ObjectConstant::Decimal` — computed once per scan so the hot per-row match
/// can skip re-parsing. `None` for any other constant.
fn decimal_canonical_of(constant: &crate::r2rml::ObjectConstant) -> Option<String> {
    match constant {
        crate::r2rml::ObjectConstant::Decimal(d) => Some(d.to_string()),
        _ => None,
    }
}

/// Uncached convenience wrapper — used only by tests; the hot per-row paths call
/// [`rdf_term_eq_object_constant_cached`] directly with a precomputed canonical
/// string.
#[cfg(test)]
fn rdf_term_eq_object_constant(
    term: &RdfTerm,
    constant: &crate::r2rml::ObjectConstant,
    numeric_column: bool,
) -> bool {
    rdf_term_eq_object_constant_cached(term, constant, numeric_column, None)
}

/// Whether a decimal lexical form (`"100.00"`, `"-100.0"`) equals integer `n`
/// exactly: the fractional digits are all zero and the integer part parses to
/// `n`. Uses no `f64`, so it stays exact for integers beyond `2^53`.
fn decimal_lexical_eq_int(v: &str, n: i64) -> bool {
    let (int_part, frac_part) = v.split_once('.').unwrap_or((v, ""));
    frac_part.bytes().all(|b| b == b'0') && int_part.parse::<i64>().is_ok_and(|x| x == n)
}

/// Whether the object's backing column is a numeric (Decimal/Float) physical
/// type, so an integer constant may match a decimal lexical form. Only a plain
/// `rr:column` object qualifies — anything else does not push a scan filter (see
/// [`value_pushdown_column`]), so the strict lexical match already suffices.
fn object_column_is_numeric(pom: &PredicateObjectMap, batch: &ColumnBatch) -> bool {
    let ObjectMap::Column { column, .. } = &pom.object_map else {
        return false;
    };
    matches!(
        batch.column_by_name(column),
        Some(Column::Decimal { .. } | Column::Float32(_) | Column::Float64(_))
    )
}

/// Materialize one column batch into produced variable assignments (subject +
/// object vars) — the per-batch unit of the parallel scan. Mirrors the previous
/// per-row logic (star cross product, subject-only, single-object).
fn materialize_batch(
    pattern: &R2rmlPattern,
    triples_map: &TriplesMap,
    iceberg_batch: &ColumnBatch,
    parent_lookups: &HashMap<(String, Vec<String>), Arc<ParentLookup>>,
    ref_shortcuts: &HashMap<LookupCacheKey, RefShortcut>,
    encoder: &LiteralEncoder,
) -> Result<Vec<Vec<(VarId, Binding)>>> {
    // Precompute each decimal constant's canonical string once (not per row), so
    // the per-row match can skip the `BigDecimal` parse on an exact lexical hit.
    let object_constant_canon: Option<String> = pattern
        .object_constant
        .as_ref()
        .and_then(decimal_canonical_of);
    let star_constraint_canon: Vec<Option<String>> = pattern
        .star_constraints
        .iter()
        .map(|(_, c)| decimal_canonical_of(c))
        .collect();

    let mut produced: Vec<Vec<(VarId, Binding)>> = Vec::new();
    for table_row_idx in 0..iceberg_batch.num_rows {
        let subject_term = match materialize_subject_from_batch(
            &triples_map.subject_map,
            iceberg_batch,
            table_row_idx,
        )? {
            Some(t) => t,
            None => continue,
        };

        // Bound-subject filter (`<store/5> <pred> ?o`): keep only rows whose
        // subject IRI equals the constant. This is the pattern's semantics,
        // enforced regardless of any scan pushdown.
        if let Some(want) = pattern.subject_constant.as_deref() {
            if !subject_term_matches_iri(&subject_term, want) {
                continue;
            }
        }
        let subject_binding = encoder.encode(&subject_term);

        if !pattern.star_bindings.is_empty() || !pattern.star_constraints.is_empty() {
            let mut members: Vec<(VarId, &str)> = Vec::new();
            if let (Some(ov), Some(pf)) = (pattern.object_var, pattern.predicate_filter.as_deref())
            {
                members.push((ov, pf));
            }
            for (pred, var) in &pattern.star_bindings {
                members.push((*var, pred.as_str()));
            }

            let mut binding_lists: Vec<(VarId, Vec<Binding>)> = Vec::with_capacity(members.len());
            let mut row_ok = true;
            for (var, pred) in &members {
                let mut vals: Vec<Binding> = Vec::new();
                for pom in triples_map
                    .predicate_object_maps
                    .iter()
                    .filter(|p| p.predicate_map.as_constant() == Some(*pred))
                {
                    if let Some(t) = materialize_pom_object(
                        pom,
                        iceberg_batch,
                        table_row_idx,
                        parent_lookups,
                        ref_shortcuts,
                    )? {
                        vals.push(encoder.encode(&t));
                    }
                }
                if vals.is_empty() {
                    row_ok = false;
                    break;
                }
                binding_lists.push((*var, vals));
            }

            // Fused constant-object constraints: the row survives only when each
            // predicate produces at least one object equal to its constant. This
            // is an existence filter (produces no var), enforced by the operator.
            if row_ok {
                for ((pred, required), canon) in
                    pattern.star_constraints.iter().zip(&star_constraint_canon)
                {
                    let mut matched = false;
                    for pom in triples_map
                        .predicate_object_maps
                        .iter()
                        .filter(|p| p.predicate_map.as_constant() == Some(pred.as_str()))
                    {
                        if let Some(t) = materialize_pom_object(
                            pom,
                            iceberg_batch,
                            table_row_idx,
                            parent_lookups,
                            ref_shortcuts,
                        )? {
                            let numeric = object_column_is_numeric(pom, iceberg_batch);
                            if rdf_term_eq_object_constant_cached(
                                &t,
                                required,
                                numeric,
                                canon.as_deref(),
                            ) {
                                matched = true;
                                break;
                            }
                        }
                    }
                    if !matched {
                        row_ok = false;
                        break;
                    }
                }
            }
            if !row_ok {
                continue;
            }

            // Seed row: the subject binding, or empty for a constant subject.
            // The cross-product below clones this row per extra object, so a clone
            // (not a move) of the subject binding is required here.
            let seed = match pattern.subject_var {
                Some(sv) => vec![(sv, subject_binding.clone())],
                None => Vec::new(),
            };
            let mut rows: Vec<Vec<(VarId, Binding)>> = vec![seed];
            for (var, vals) in &binding_lists {
                if vals.len() == 1 {
                    for r in &mut rows {
                        r.push((*var, vals[0].clone()));
                    }
                } else {
                    let mut next = Vec::with_capacity(rows.len() * vals.len());
                    for r in &rows {
                        for v in vals {
                            let mut nr = r.clone();
                            nr.push((*var, v.clone()));
                            next.push(nr);
                        }
                    }
                    rows = next;
                }
            }
            produced.extend(rows);
            continue;
        }

        let Some(obj_var) = pattern.object_var else {
            // Constant-object (`?s <pred> "value"`): keep the subject only when
            // this predicate has an object equal to the required constant. The
            // equality is the pattern's semantics, enforced here regardless of
            // scan pushdown; the pushed ScanFilter is only an optimization. Exactly
            // one row per surviving subject, so the subject binding is moved (not
            // cloned) into it.
            if let Some(required) = &pattern.object_constant {
                let mut matched = false;
                for pom in triples_map.predicate_object_maps.iter().filter(|pom| {
                    pattern
                        .predicate_filter
                        .as_deref()
                        .is_some_and(|pf| pom.predicate_map.as_constant() == Some(pf))
                }) {
                    if let Some(t) = materialize_pom_object(
                        pom,
                        iceberg_batch,
                        table_row_idx,
                        parent_lookups,
                        ref_shortcuts,
                    )? {
                        let numeric = object_column_is_numeric(pom, iceberg_batch);
                        if rdf_term_eq_object_constant_cached(
                            &t,
                            required,
                            numeric,
                            object_constant_canon.as_deref(),
                        ) {
                            matched = true;
                            break;
                        }
                    }
                }
                if matched {
                    produced.push(match pattern.subject_var {
                        Some(sv) => vec![(sv, subject_binding)],
                        None => Vec::new(),
                    });
                }
                continue;
            }
            // Pure subject-only pattern. A plain `?s a ex:Class` scan (constrained
            // by `class_filter`) emits the subject alone. A projected `?s a ?type`
            // scan (`type_var`) instead emits one row per class the map declares,
            // binding `?type` to that class IRI — the same subjects a bound-class
            // scan visits, with the class projected rather than filtered. A map
            // that declares no class produces no row for a `type_var` pattern (its
            // subjects have no rdf:type triple).
            match pattern.type_var {
                Some(tv) => {
                    for class_iri in triples_map.classes() {
                        let mut row = Vec::with_capacity(2);
                        if let Some(sv) = pattern.subject_var {
                            row.push((sv, subject_binding.clone()));
                        }
                        row.push((tv, Binding::iri(class_iri.as_str())));
                        produced.push(row);
                    }
                }
                None => {
                    produced.push(match pattern.subject_var {
                        Some(sv) => vec![(sv, subject_binding)],
                        None => Vec::new(),
                    });
                }
            }
            continue;
        };

        for pom in triples_map.predicate_object_maps.iter().filter(|pom| {
            pattern
                .predicate_filter
                .as_deref()
                .is_none_or(|pf| pom.predicate_map.as_constant() == Some(pf))
        }) {
            if let Some(t) = materialize_pom_object(
                pom,
                iceberg_batch,
                table_row_idx,
                parent_lookups,
                ref_shortcuts,
            )? {
                let object_binding = encoder.encode(&t);
                // Build the (subject?, predicate?, object) prefix once. Capacity
                // accounts for the extra type slot (a fused browse crawl also
                // projects `?type`) so no push reallocates on this hottest scan
                // path.
                let cap = if pattern.type_var.is_some() { 4 } else { 3 };
                let mut base = Vec::with_capacity(cap);
                if let Some(sv) = pattern.subject_var {
                    base.push((sv, subject_binding.clone()));
                }
                // Variable-predicate wildcard (`?s ?p ?o` / `<iri> ?p ?o`): bind
                // `?p` to this POM's predicate IRI. A templated/column (non-
                // constant) predicate — rare but representable — is materialized
                // from the row; when it expands to nothing (NULL column) the
                // triple does not exist for this row, so the POM is SKIPPED
                // rather than emitting a solution with a bound object and an
                // unbound predicate.
                if let Some(pv) = pattern.predicate_var {
                    match pom.predicate_map.as_constant() {
                        Some(pred_iri) => base.push((pv, Binding::iri(pred_iri))),
                        None => match materialize_predicate_from_batch(
                            &pom.predicate_map,
                            iceberg_batch,
                            table_row_idx,
                        )? {
                            Some(pred_iri) => base.push((pv, Binding::iri(pred_iri))),
                            None => continue,
                        },
                    }
                }
                base.push((obj_var, object_binding));
                // When the pattern ALSO projects a type-var (a browse crawl fused
                // its `?s a ?type` into this wildcard), emit each `(predicate,
                // object)` row once per declared class — the per-`(p,o)` × class
                // cartesian, identical to the two-scan `wildcard ⋈ type-var` inner
                // join. Without a type-var the behavior is byte-identical to before.
                match pattern.type_var {
                    None => produced.push(base),
                    Some(tv) => {
                        let classes = triples_map.classes();
                        match classes.len() {
                            // A classless scanned map keeps its `(p,o)` triple with
                            // `?type` unbound — never drop the wildcard binding.
                            // (Unreachable while fused, since `class_filter` prunes
                            // to classed maps; kept for two-scan parity and the
                            // future non-`["*"]` crawl shapes.)
                            0 => produced.push(base),
                            // Common case: exactly one class — bind it, no clone.
                            1 => {
                                base.push((tv, Binding::iri(classes[0].as_str())));
                                produced.push(base);
                            }
                            // Multi-class: clone the `(p,o)` prefix once per class.
                            _ => {
                                for class_iri in classes {
                                    let mut row = base.clone();
                                    row.push((tv, Binding::iri(class_iri.as_str())));
                                    produced.push(row);
                                }
                            }
                        }
                    }
                }
            }
        }

        // A TRUE-wildcard scan (`?s ?p ?o` / `<iri> ?p ?o`: variable predicate,
        // no predicate filter) must ALSO emit each subject's `rr:class`-derived
        // `rdf:type` triple — the POM loop above materializes only the data
        // predicates, while a native wildcard returns the type triple too
        // (without this, the subject inspector shows no `@type` on a virtual
        // dataset). Excluded when a type-var is projected (a fused browse crawl
        // already carries the class on every row — no double emission) and when
        // a predicate filter pins `?p` to one data predicate.
        if pattern.predicate_filter.is_none() && pattern.type_var.is_none() {
            if let Some(pv) = pattern.predicate_var {
                for class_iri in triples_map.classes() {
                    let mut row = Vec::with_capacity(3);
                    if let Some(sv) = pattern.subject_var {
                        row.push((sv, subject_binding.clone()));
                    }
                    row.push((pv, Binding::iri(fluree_vocab::rdf::TYPE)));
                    row.push((obj_var, Binding::iri(class_iri.as_str())));
                    produced.push(row);
                }
            }
        }
    }
    Ok(produced)
}

/// Build a parent lookup table for RefObjectMap joins.
///
/// Scans the parent TriplesMap's table and builds a HashMap mapping
/// parent join key → parent subject IRI.
///
/// # Arguments
///
/// * `parent_tm` - The parent TriplesMap
/// * `parent_columns` - Column names used in join conditions (from parent side)
/// * `batches` - Column batches from scanning the parent table
///
/// # Returns
///
/// HashMap mapping join key (as `Vec<String>`) to parent subject `RdfTerm`.
fn build_parent_lookup(
    parent_tm: &TriplesMap,
    parent_columns: &[String],
    batches: Vec<ColumnBatch>,
) -> Result<ParentLookup> {
    let mut lookup = ParentLookup::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows {
            // Materialize parent subject
            let subject_term =
                match materialize_subject_from_batch(&parent_tm.subject_map, &batch, row_idx) {
                    Ok(Some(term)) => term,
                    Ok(None) => continue, // Null subject - skip
                    Err(e) => {
                        tracing::warn!(
                            parent_tm = %parent_tm.iri,
                            row_idx,
                            error = %e,
                            "Failed to materialize parent subject, skipping row"
                        );
                        continue;
                    }
                };

            // Extract join key from parent row
            let key = match get_join_key_from_batch(parent_columns, &batch, row_idx) {
                Some(k) => k,
                None => continue, // Null in join key - skip
            };

            // Insert into lookup (last wins for duplicate keys)
            lookup.insert(key, subject_term);
        }
    }

    tracing::debug!(
        parent_tm = %parent_tm.iri,
        lookup_size = lookup.len(),
        "Built parent lookup table for RefObjectMap join"
    );

    Ok(lookup)
}

#[async_trait]
impl Operator for R2rmlScanOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    fn set_row_budget(&mut self, budget: usize) {
        // Record the budget but do NOT forward it to the child: the child feeds
        // this operator's correlated scan/join, which is not row-preserving, so
        // an inner scan must still produce every row the join needs. Only the
        // topmost row-preserving scan is budgeted — `LimitOperator` forwards a
        // budget solely through row-preserving operators, so if this operator
        // received one, its output flows 1:1 to the LIMIT.
        if limit_pushdown_enabled() {
            self.row_budget = Some(budget);
        }
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        // Open child first
        self.child.open(ctx).await?;
        self.emitted = 0;

        // Load the compiled mapping from the provider
        let provider = ctx
            .r2rml_provider
            .ok_or_else(|| QueryError::InvalidQuery("R2RML provider not configured".to_string()))?;

        // IMPORTANT: In dataset mode, there is no meaningful dataset-level `to_t`.
        // Passing `None` avoids inventing a cross-ledger time and lets the provider
        // select the latest snapshot (or apply its own semantics).
        let as_of_t = if ctx.dataset.is_some() {
            None
        } else {
            Some(ctx.to_t)
        };
        let mapping = provider
            .compiled_mapping(&self.pattern.graph_source_id, as_of_t)
            .await?;

        self.mapping = Some(mapping);
        self.state = OperatorState::Open;

        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state == OperatorState::Exhausted {
            return Ok(None);
        }

        let num_cols = self.schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(ctx.batch_size))
            .collect();

        loop {
            // Cancellation checkpoint at the top of the internal loop: this loop
            // can pull many windows / files / child batches before returning a
            // full output batch, so the runner's between-`next_batch` check would
            // otherwise never run for a whole-table scan. Covers the loop's
            // non-advancing branches (overflow drain, child pull) that site 2
            // does not.
            ctx.check_cancelled()?;
            // 1. Drain overflow from a prior window before doing more work.
            while !self.pending.is_empty() && columns[0].len() < ctx.batch_size {
                let row = self.pending.pop_front().unwrap();
                for (col_idx, binding) in row.into_iter().enumerate() {
                    columns[col_idx].push(binding);
                }
            }
            // Emit once a full batch is accumulated, or once the LIMIT budget is
            // (optimistically) met. With a consumed filter the pre-filter count
            // over-estimates matches, so this only triggers an emit *attempt* —
            // `finalize_batch` filters and `emitted` counts the survivors; if
            // that leaves the budget unmet the loop keeps pulling. Enabling it
            // for the consumed case is what stops the scan after ~one window
            // instead of accumulating a full `batch_size` before filtering.
            let budget_met = self
                .row_budget
                .is_some_and(|b| self.emitted + columns[0].len() >= b);
            if columns[0].len() >= ctx.batch_size || (budget_met && !columns[0].is_empty()) {
                // Fast path (no consumed filter): emit the accumulated columns
                // directly, exactly as before — no extra allocation.
                if self.consumed_filter.is_none() {
                    self.emitted += columns[0].len();
                    if self.row_budget.is_some_and(|b| self.emitted >= b) {
                        self.state = OperatorState::Exhausted;
                    }
                    return Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?));
                }
                // Consumed-filter path: filter this window, count matching rows,
                // and keep pulling if the whole window is filtered out.
                let taken = std::mem::replace(
                    &mut columns,
                    (0..num_cols)
                        .map(|_| Vec::with_capacity(ctx.batch_size))
                        .collect(),
                );
                if let Some(out) = self.finalize_batch(taken, ctx)? {
                    self.emitted += out.len();
                    if self.row_budget.is_some_and(|b| self.emitted >= b) {
                        self.state = OperatorState::Exhausted;
                    }
                    return Ok(Some(out));
                }
                continue;
            }

            // 2. Advance an in-flight scan by one materialization window. The
            //    window's rows fill `columns` (overflow spills to `pending`), so
            //    the whole table is never materialized at once.
            if let Some(mut progress) = self.progress.take() {
                let more = self
                    .advance_one_window(ctx, &mut progress, num_cols, &mut columns)
                    .await?;
                if more {
                    self.progress = Some(progress);
                }
                continue;
            }

            // 3. No scan in flight: pull the next child batch and start one.
            match self.child.next_batch(ctx).await? {
                Some(child_batch) => {
                    if let Some(progress) = self.build_progress(ctx, child_batch).await? {
                        self.progress = Some(progress);
                    }
                    continue;
                }
                None => {
                    if columns[0].is_empty() {
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                    // Fast path (no consumed filter): emit directly, unchanged.
                    if self.consumed_filter.is_none() {
                        self.emitted += columns[0].len();
                        return Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?));
                    }
                    // The child is exhausted, so this is the terminal batch
                    // whether or not any row survives the consumed filter.
                    self.state = OperatorState::Exhausted;
                    let taken = std::mem::take(&mut columns);
                    if let Some(out) = self.finalize_batch(taken, ctx)? {
                        self.emitted += out.len();
                        return Ok(Some(out));
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.mapping = None;
        self.pending.clear();
        self.progress = None;
        self.scan_cache.clear();
        self.parent_lookup_cache.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Could use Iceberg table statistics in the future
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::r2rml::{ObjectConstant, ScanValue};
    use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};
    use fluree_db_r2rml::materialize::RdfTerm;

    fn pom(pred: &str, col: &str) -> PredicateObjectMap {
        PredicateObjectMap {
            predicate_map: PredicateMap::constant(pred),
            object_map: ObjectMap::column(col),
        }
    }

    // PR-3 fix (a): a star resolves only TriplesMaps that supply EVERY star
    // predicate. A map with a distinguishing member is kept; one missing it is
    // pruned (case a); a map legitimately supplying all members is kept (case c).
    #[test]
    fn star_prune_requires_all_member_predicates() {
        let store = TriplesMap::new("#Store", "dim_store")
            .with_subject_template("http://ex/store/{k}")
            .with_predicate_object(pom("http://ex/name", "store_name"))
            .with_predicate_object(pom("http://ex/channel", "channel"));
        let customer = TriplesMap::new("#Customer", "dim_customer")
            .with_subject_template("http://ex/customer/{k}")
            .with_predicate_object(pom("http://ex/name", "full_name"));
        let required = vec![
            "http://ex/name".to_string(),
            "http://ex/channel".to_string(),
        ];
        // case (c): DIM_STORE supplies name AND channel -> kept.
        assert!(tm_passes_star_prune(&store, &required, None));
        // case (a): DIM_CUSTOMER has name but not channel -> pruned (dead work).
        assert!(!tm_passes_star_prune(&customer, &required, None));
    }

    // PR-3 fix (b'): with a (template-disjoint) class prune, only class-declaring
    // maps survive resolution; the class's own scan enforces membership.
    #[test]
    fn star_prune_class_keeps_only_declaring_maps() {
        let store = TriplesMap::new("#Store", "dim_store")
            .with_subject_template("http://ex/store/{k}")
            .with_class("http://ex/Store")
            .with_predicate_object(pom("http://ex/name", "store_name"));
        let customer = TriplesMap::new("#Customer", "dim_customer")
            .with_subject_template("http://ex/customer/{k}")
            .with_predicate_object(pom("http://ex/name", "full_name"));
        let name = vec!["http://ex/name".to_string()];
        assert!(tm_passes_star_prune(&store, &name, Some("http://ex/Store")));
        assert!(!tm_passes_star_prune(
            &customer,
            &name,
            Some("http://ex/Store")
        ));
    }

    // PR-3 fix (d): switch off (empty inputs) reproduces today's fan-out — every
    // map passes the prune regardless of its predicates or class.
    #[test]
    fn star_prune_noop_when_inputs_empty() {
        let customer = TriplesMap::new("#Customer", "dim_customer")
            .with_subject_template("http://ex/customer/{k}")
            .with_predicate_object(pom("http://ex/name", "full_name"));
        assert!(tm_passes_star_prune(&customer, &[], None));
    }

    #[test]
    fn build_ref_shortcut_accepts_only_child_templatable_single_col() {
        use fluree_db_r2rml::mapping::SubjectMap;
        // Parent subject templated on its PK `id`; single-column FK account_id → id.
        let parent =
            TriplesMap::new("#Account", "accounts").with_subject_template("http://ex/account/{id}");
        let rom = RefObjectMap::new("#Account", "account_id", "id");
        let sc = build_ref_shortcut(&parent, &rom).expect("single-col PK-template is templatable");
        assert_eq!(sc.subject_template, "http://ex/account/{id}");

        // Reject: composite FK (the parent lookup keys on sorted cols vs the child's
        // declared order — refuse rather than risk a transposed IRI).
        let mut composite = RefObjectMap::new("#Account", "a", "x");
        composite.add_condition("b", "y");
        assert!(
            build_ref_shortcut(&parent, &composite).is_none(),
            "composite FK must be refused"
        );

        // Reject: template placeholder is not an FK join column (subject keyed on
        // `name`, FK joins on `id`) — the child row does not carry `name`.
        let parent_nonfk = TriplesMap::new("#Account", "accounts")
            .with_subject_template("http://ex/account/{name}");
        assert!(
            build_ref_shortcut(&parent_nonfk, &rom).is_none(),
            "non-FK template column must be refused"
        );

        // Reject: column-subject parent (rr:column) — not a template.
        let mut parent_col = TriplesMap::new("#Account", "accounts");
        parent_col.subject_map = SubjectMap::column("uri");
        assert!(
            build_ref_shortcut(&parent_col, &rom).is_none(),
            "column subject must be refused"
        );

        // Reject: constant-subject parent (rr:constant).
        let mut parent_const = TriplesMap::new("#Account", "accounts");
        parent_const.subject_map = SubjectMap::constant("http://ex/the-account");
        assert!(
            build_ref_shortcut(&parent_const, &rom).is_none(),
            "constant subject must be refused"
        );

        // Reject: blank-node parent subject (term type is not IRI).
        let mut parent_bnode = TriplesMap::new("#Account", "accounts");
        parent_bnode.subject_map = SubjectMap::template("http://ex/account/{id}").as_blank_node();
        assert!(
            build_ref_shortcut(&parent_bnode, &rom).is_none(),
            "blank-node subject must be refused"
        );
    }

    #[test]
    fn object_constant_matching() {
        // IRI constant: exact IRI match only.
        let iri = ObjectConstant::Iri("http://ex/geo/1".to_string());
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::iri("http://ex/geo/1"),
            &iri,
            false
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::iri("http://ex/geo/2"),
            &iri,
            false
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("http://ex/geo/1"),
            &iri,
            false
        ));

        // String constant: loose lexical match, datatype/language-agnostic — a
        // plain-string query object matches a lang-tagged materialized literal.
        let s = ObjectConstant::Scalar(ScanValue::Str("chat".to_string()));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("chat"),
            &s,
            false
        ));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::lang_string("chat", "fr"),
            &s,
            false
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("dog"),
            &s,
            false
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::iri("chat"),
            &s,
            false
        ));

        // Integer constant against a NON-numeric (text) column: EXACT — "2024"
        // matches; a decimal lexical does not (the scan filter casts the int to
        // text and drops "2024.0", so the operator must too).
        let n = ObjectConstant::Scalar(ScanValue::Int(2024));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("2024"),
            &n,
            false
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("2024.0"),
            &n,
            false
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("2025"),
            &n,
            false
        ));

        // Integer constant against a numeric (Decimal/Float) column: a
        // zero-fraction decimal lexical matches (`?s :amount 100` vs 100.00),
        // a non-zero fraction does not.
        let amount = ObjectConstant::Scalar(ScanValue::Int(100));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("100.00"),
            &amount,
            true
        ));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("100"),
            &amount,
            true
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("100.50"),
            &amount,
            true
        ));
        // Exactness holds beyond 2^53 (no f64): trailing-zero decimal matches,
        // the adjacent integer does not.
        let big = ObjectConstant::Scalar(ScanValue::Int(9_007_199_254_740_993));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("9007199254740993"),
            &big,
            false
        ));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("9007199254740993.00"),
            &big,
            true
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("9007199254740992"),
            &big,
            false
        ));

        // Boolean constant: true/1 vs false/0.
        let b = ObjectConstant::Scalar(ScanValue::Bool(true));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("true"),
            &b,
            false
        ));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("1"),
            &b,
            false
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("false"),
            &b,
            false
        ));
    }

    #[test]
    fn only_plain_columns_are_value_pushable() {
        use fluree_db_r2rml::mapping::{ObjectMap, RefObjectMap};

        // Plain rr:column → materialized value is the raw column, so a
        // column-level scan filter is a sound optimization.
        assert_eq!(
            value_pushdown_column(&ObjectMap::column("code")),
            Some("code")
        );
        assert_eq!(
            value_pushdown_column(&ObjectMap::column_typed(
                "year",
                "http://www.w3.org/2001/XMLSchema#integer"
            )),
            Some("year")
        );

        // A single-column template transforms the value ("PREFIX-{code}" ≠ code):
        // pushing Eq(code, "PREFIX-A") would drop every row the operator keeps.
        assert_eq!(
            value_pushdown_column(&ObjectMap::template(
                "PREFIX-{code}",
                vec!["code".to_string()]
            )),
            None
        );
        // Constant ignores the row; RefObjectMap is an IRI join — neither pushable.
        assert_eq!(
            value_pushdown_column(&ObjectMap::constant_literal("x")),
            None
        );
        assert_eq!(
            value_pushdown_column(&ObjectMap::RefObjectMap(RefObjectMap::new(
                "gs:other", "fk", "id"
            ))),
            None
        );
    }

    #[test]
    fn numeric_and_date_object_matching() {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        // Decimal: scale-insensitive numeric match (`9.99` == `9.990`).
        // (numeric_column is irrelevant to the Decimal arm; pass false.)
        let d = ObjectConstant::Decimal(BigDecimal::from_str("9.99").unwrap());
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("9.99"),
            &d,
            false
        ));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("9.990"),
            &d,
            false
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("9.98"),
            &d,
            false
        ));
        // An IRI term never matches a literal constant.
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::iri("9.99"),
            &d,
            false
        ));

        // Cached fast-path (fed the constant's own canonical string, as the hot
        // loop does): identical results to the uncached path — an exact lexical
        // hit short-circuits, a scale variant falls back to the numeric compare.
        let canon = decimal_canonical_of(&d);
        assert_eq!(canon.as_deref(), Some("9.99"));
        let c = canon.as_deref();
        assert!(rdf_term_eq_object_constant_cached(
            &RdfTerm::string("9.99"),
            &d,
            false,
            c
        )); // fast hit
        assert!(rdf_term_eq_object_constant_cached(
            &RdfTerm::string("9.990"),
            &d,
            false,
            c
        )); // fallback
        assert!(!rdf_term_eq_object_constant_cached(
            &RdfTerm::string("9.98"),
            &d,
            false,
            c
        ));
        // With no cached string, still correct via the numeric compare.
        assert!(rdf_term_eq_object_constant_cached(
            &RdfTerm::string("9.990"),
            &d,
            false,
            None
        ));

        // Double: exact f64 value match, insensitive to trailing zeros.
        let f = ObjectConstant::Double(1.5);
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("1.5"),
            &f,
            false
        ));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("1.50"),
            &f,
            false
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("1.6"),
            &f,
            false
        ));

        // Date: ISO 8601 materialized lexical parsed back to days-since-epoch.
        let days = fluree_db_core::Date::parse("2024-01-15")
            .unwrap()
            .days_since_epoch();
        let dt = ObjectConstant::Scalar(ScanValue::Date(days));
        assert!(rdf_term_eq_object_constant(
            &RdfTerm::string("2024-01-15"),
            &dt,
            false
        ));
        assert!(!rdf_term_eq_object_constant(
            &RdfTerm::string("2024-01-16"),
            &dt,
            false
        ));
    }

    #[test]
    fn bound_subject_matching() {
        // Subject maps always yield IRIs: exact IRI match, never a literal.
        assert!(subject_term_matches_iri(
            &RdfTerm::iri("http://ex/store/5"),
            "http://ex/store/5"
        ));
        assert!(!subject_term_matches_iri(
            &RdfTerm::iri("http://ex/store/50"),
            "http://ex/store/5"
        ));
        assert!(!subject_term_matches_iri(
            &RdfTerm::string("http://ex/store/5"),
            "http://ex/store/5"
        ));
    }

    // ---- true-wildcard rdf:type emission + non-constant predicate binding ----

    use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};

    fn iri_of(b: &Binding) -> String {
        match b {
            Binding::Iri(s) => s.to_string(),
            other => panic!("expected an IRI binding, got {other:?}"),
        }
    }

    fn find(row: &[(VarId, Binding)], v: VarId) -> Option<&Binding> {
        row.iter().find(|(rv, _)| *rv == v).map(|(_, b)| b)
    }

    fn single_col_batch(name: &str, values: Vec<Option<i64>>) -> ColumnBatch {
        let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
            name: name.to_string(),
            field_type: FieldType::Int64,
            nullable: false,
            field_id: 1,
        }]));
        ColumnBatch::new(schema, vec![Column::Int64(values)]).unwrap()
    }

    /// A bound-subject true wildcard (`<iri> ?p ?o`) emits the `rr:class`-derived
    /// `rdf:type` triple that the POM loop alone omits — without it the subject
    /// inspector shows no `@type` on a virtual dataset (native returns it).
    #[test]
    fn bound_subject_wildcard_emits_rdf_type() {
        // DIM_STORE keyed on STORE_KEY, class ex:Store, no data POMs — so the
        // ONLY output is the class-derived rdf:type row.
        let tm = TriplesMap::new("#Store", "DIM_STORE")
            .with_subject_template("http://ex/store/{STORE_KEY}")
            .with_class("http://ex/Store");
        let batch = single_col_batch("STORE_KEY", vec![Some(1)]);

        // `<http://ex/store/1> ?p ?o` (?p = VarId 1, ?o = VarId 2).
        let pattern =
            R2rmlPattern::new_bound_subject("gs:main", "http://ex/store/1", Some(VarId(2)))
                .with_predicate_var(VarId(1));

        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let encoder = LiteralEncoder::build(&tm, &snapshot);
        let lookups: HashMap<(String, Vec<String>), Arc<ParentLookup>> = HashMap::new();
        let shortcuts: HashMap<LookupCacheKey, RefShortcut> = HashMap::new();

        let rows = materialize_batch(&pattern, &tm, &batch, &lookups, &shortcuts, &encoder)
            .expect("materialize");

        assert_eq!(rows.len(), 1, "exactly the one rdf:type row: {rows:?}");
        let row = &rows[0];
        assert_eq!(
            find(row, VarId(1)).map(iri_of).as_deref(),
            Some(fluree_vocab::rdf::TYPE),
            "?p = rdf:type"
        );
        assert_eq!(
            find(row, VarId(2)).map(iri_of).as_deref(),
            Some("http://ex/Store"),
            "?o = the class IRI"
        );
    }

    /// A var-subject true wildcard (`?s ?p ?o`) emits each row's data POM rows
    /// PLUS one class-derived rdf:type row binding the subject — matching the
    /// native wildcard, which returns the type triple alongside the data.
    #[test]
    fn var_subject_wildcard_emits_rdf_type_rows() {
        let tm = TriplesMap::new("#Store", "DIM_STORE")
            .with_subject_template("http://ex/store/{STORE_KEY}")
            .with_class("http://ex/Store")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: fluree_db_r2rml::mapping::PredicateMap::constant(
                    "http://ex/storeKey",
                ),
                object_map: ObjectMap::column("STORE_KEY"),
            });
        let batch = single_col_batch("STORE_KEY", vec![Some(7)]);

        // `?s ?p ?o` (?s = VarId 0, ?p = VarId 1, ?o = VarId 2).
        let pattern =
            R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2))).with_predicate_var(VarId(1));

        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let encoder = LiteralEncoder::build(&tm, &snapshot);
        let lookups: HashMap<(String, Vec<String>), Arc<ParentLookup>> = HashMap::new();
        let shortcuts: HashMap<LookupCacheKey, RefShortcut> = HashMap::new();

        let rows = materialize_batch(&pattern, &tm, &batch, &lookups, &shortcuts, &encoder)
            .expect("materialize");

        assert_eq!(rows.len(), 2, "one data row + one type row: {rows:?}");
        let type_row = rows
            .iter()
            .find(|r| {
                find(r, VarId(1))
                    .map(|b| matches!(b, Binding::Iri(s) if &**s == fluree_vocab::rdf::TYPE))
                    .unwrap_or(false)
            })
            .expect("a type row must be emitted");
        assert!(
            find(type_row, VarId(0)).is_some(),
            "type row must bind the subject var"
        );
        assert_eq!(
            find(type_row, VarId(2)).map(iri_of).as_deref(),
            Some("http://ex/Store"),
            "type row object = class IRI"
        );
        // The data row still carries its constant predicate.
        assert!(
            rows.iter().any(|r| {
                find(r, VarId(1))
                    .map(|b| matches!(b, Binding::Iri(s) if &**s == "http://ex/storeKey"))
                    .unwrap_or(false)
            }),
            "the data POM row must survive alongside the type row: {rows:?}"
        );
    }

    /// A templated (non-constant) predicate binds `?p` from the row when the
    /// template expands, and SKIPS the POM when a referenced column is NULL —
    /// never emitting a solution with a bound object and an unbound predicate.
    #[test]
    fn templated_predicate_binds_or_skips_never_half_bound() {
        use fluree_db_r2rml::mapping::PredicateMap;

        // Classless map (isolates POM behavior): predicate templated over
        // ATTR_NAME, object from ATTR_VAL.
        let tm = TriplesMap::new("#Attr", "ATTRS")
            .with_subject_template("http://ex/attr-row/{ID}")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::template(
                    "http://ex/attr/{ATTR_NAME}",
                    vec!["ATTR_NAME".to_string()],
                ),
                object_map: ObjectMap::column("ATTR_VAL"),
            });

        let schema = Arc::new(BatchSchema::new(vec![
            FieldInfo {
                name: "ID".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                field_id: 1,
            },
            FieldInfo {
                name: "ATTR_NAME".to_string(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 2,
            },
            FieldInfo {
                name: "ATTR_VAL".to_string(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 3,
            },
        ]));
        let batch = ColumnBatch::new(
            schema,
            vec![
                Column::Int64(vec![Some(1), Some(2)]),
                Column::String(vec![Some("color".to_string()), None]), // row 2: NULL name
                Column::String(vec![Some("red".to_string()), Some("blue".to_string())]),
            ],
        )
        .unwrap();

        let pattern =
            R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2))).with_predicate_var(VarId(1));

        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let encoder = LiteralEncoder::build(&tm, &snapshot);
        let lookups: HashMap<(String, Vec<String>), Arc<ParentLookup>> = HashMap::new();
        let shortcuts: HashMap<LookupCacheKey, RefShortcut> = HashMap::new();

        let rows = materialize_batch(&pattern, &tm, &batch, &lookups, &shortcuts, &encoder)
            .expect("materialize");

        // Row 1 materializes with ?p expanded from the template; row 2's POM is
        // skipped (NULL template column ⇒ the triple does not exist), so no row
        // may carry a bound object with an unbound predicate.
        assert_eq!(rows.len(), 1, "only the expandable row survives: {rows:?}");
        assert_eq!(
            find(&rows[0], VarId(1)).map(iri_of).as_deref(),
            Some("http://ex/attr/color"),
            "?p = the row-expanded templated predicate"
        );
        for row in &rows {
            assert!(
                !(find(row, VarId(2)).is_some() && find(row, VarId(1)).is_none()),
                "no solution may bind ?o while leaving ?p unbound: {row:?}"
            );
        }
    }

    // PR-4: the cross-child-batch parent-lookup memoization, CI-enforced. Drives
    // `build_progress` N times (as a correlated join / a multi-batch child stream
    // does) against a scan-COUNTING provider: with the memo ON the DIM parent is
    // scanned exactly once; with it OFF the parent is re-scanned every batch
    // (today's fan-out — the q008 DNF class). A future refactor that drops the
    // cache (e.g. reintroduces the parent-scan-per-batch bypass) trips this.
    #[test]
    fn parent_lookup_memoized_across_child_batches() {
        use crate::context::ExecutionContext;
        use crate::r2rml::R2rmlTableProvider;
        use crate::seed::EmptyOperator;
        use crate::var_registry::VarRegistry;
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap,
            TriplesMap,
        };
        use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};
        use std::sync::Mutex;

        #[derive(Debug, Default)]
        struct CountingProvider {
            scans: Mutex<HashMap<String, usize>>,
        }
        fn one_row(col: &str, field_id: i32) -> ColumnBatch {
            let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
                name: col.to_string(),
                field_type: FieldType::Int64,
                nullable: true,
                field_id,
            }]));
            ColumnBatch::new(schema, vec![Column::Int64(vec![Some(1)])]).unwrap()
        }
        #[async_trait::async_trait]
        impl R2rmlTableProvider for CountingProvider {
            async fn scan_table(
                &self,
                _graph_source_id: &str,
                table_name: &str,
                _projection: &[String],
                _filters: &[crate::r2rml::ScanFilter],
                _as_of_t: Option<i64>,
            ) -> Result<ColumnBatchStream> {
                *self
                    .scans
                    .lock()
                    .unwrap()
                    .entry(table_name.to_string())
                    .or_default() += 1;
                // The parent ("customers") is consumed to build the lookup, so it
                // must carry its join column ID; the main ("orders") stream is only
                // stored by build_progress, so its content is irrelevant here.
                let batch = if table_name == "customers" {
                    one_row("ID", 1)
                } else {
                    one_row("CUST_ID", 2)
                };
                Ok(Box::pin(futures::stream::once(async move { Ok(batch) })))
            }
        }

        // orders --edw:customer (RefObjectMap)--> customers.
        let orders = TriplesMap::new("#Order", "orders")
            .with_subject_template("http://ex/order/{ORDER_KEY}")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("edw:customer"),
                object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                    "#Customer",
                    "CUST_ID",
                    "ID",
                )),
            });
        let customers = TriplesMap::new("#Customer", "customers")
            .with_subject_template("http://ex/customer/{ID}");
        let mapping = Arc::new(CompiledR2rmlMapping::new(vec![orders, customers]));
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();

        // Returns (parent "customers" scans, child "orders" scans).
        let table_scans = |memo: bool| -> (usize, usize) {
            let provider = CountingProvider::default();
            {
                let mut ctx = ExecutionContext::new(&snapshot, &vars);
                ctx.r2rml_table_provider = Some(&provider);

                let mut pattern = R2rmlPattern::new("gs:main", VarId(0), Some(VarId(1)));
                pattern.predicate_filter = Some("edw:customer".to_string());
                let mut op = R2rmlScanOperator::new(Box::new(EmptyOperator::new()), pattern);
                op.mapping = Some(Arc::clone(&mapping));
                op.parent_memo = memo;

                for _ in 0..5 {
                    futures::executor::block_on(op.build_progress(&ctx, Batch::single_empty()))
                        .expect("build_progress");
                }
            } // ctx (and its borrow of `provider`) dropped before reading the tally.
            let scans = provider.scans.lock().unwrap();
            let count = |table: &str| scans.get(table).copied().unwrap_or(0);
            (count("customers"), count("orders"))
        };

        let (parent_on, child_on) = table_scans(true);
        let (parent_off, child_off) = table_scans(false);
        assert_eq!(
            parent_on, 1,
            "memo ON: DIM parent scanned exactly once across 5 child batches"
        );
        assert_eq!(
            parent_off, 5,
            "memo OFF: DIM parent re-scanned every batch (today's fan-out)"
        );
        // The memo must change PARENT behavior only. The child ("orders") scan
        // is already deduped across batches by the pre-existing `scan_cache`
        // (unfiltered inner scans, `FLUREE_R2RML_SCAN_CACHE`), so it is 1 in
        // BOTH regimes — the parent memo neither helps nor hurts the child path.
        assert_eq!(
            child_on, 1,
            "memo ON: child scanned once total (scan_cache, not the parent memo)"
        );
        assert_eq!(
            child_off, child_on,
            "memo OFF: identical child scan count — the memo affects parents only"
        );
    }

    // PR-8b shared test rig: a scan-counting provider + an orders→customers
    // RefObjectMap mapping. `customers` (the DIM parent) carries its join column;
    // `orders` (the child) is content-irrelevant (only stored, never lookup-built).
    #[cfg(test)]
    mod pr8b {
        use super::*;
        use crate::context::ExecutionContext;
        use crate::r2rml::R2rmlTableProvider;
        use crate::seed::EmptyOperator;
        use crate::var_registry::VarRegistry;
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap,
            TriplesMap,
        };
        use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};
        use std::sync::Mutex;

        #[derive(Debug, Default)]
        struct CountingProvider {
            scans: Mutex<HashMap<String, usize>>,
        }
        fn one_row(col: &str, field_id: i32) -> ColumnBatch {
            let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
                name: col.to_string(),
                field_type: FieldType::Int64,
                nullable: true,
                field_id,
            }]));
            ColumnBatch::new(schema, vec![Column::Int64(vec![Some(1)])]).unwrap()
        }
        #[async_trait::async_trait]
        impl R2rmlTableProvider for CountingProvider {
            async fn scan_table(
                &self,
                _graph_source_id: &str,
                table_name: &str,
                _projection: &[String],
                _filters: &[crate::r2rml::ScanFilter],
                _as_of_t: Option<i64>,
            ) -> Result<ColumnBatchStream> {
                *self
                    .scans
                    .lock()
                    .unwrap()
                    .entry(table_name.to_string())
                    .or_default() += 1;
                let batch = if table_name == "customers" {
                    one_row("ID", 1)
                } else {
                    one_row("CUST_ID", 2)
                };
                Ok(Box::pin(futures::stream::once(async move { Ok(batch) })))
            }
        }
        fn mapping() -> Arc<CompiledR2rmlMapping> {
            let orders = TriplesMap::new("#Order", "orders")
                .with_subject_template("http://ex/order/{ORDER_KEY}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("edw:customer"),
                    object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                        "#Customer",
                        "CUST_ID",
                        "ID",
                    )),
                });
            let customers = TriplesMap::new("#Customer", "customers")
                .with_subject_template("http://ex/customer/{ID}");
            Arc::new(CompiledR2rmlMapping::new(vec![orders, customers]))
        }
        fn build_once(ctx: &ExecutionContext<'_>, mapping: &Arc<CompiledR2rmlMapping>, gs: &str) {
            let mut pattern = R2rmlPattern::new(gs, VarId(0), Some(VarId(1)));
            pattern.predicate_filter = Some("edw:customer".to_string());
            let mut op = R2rmlScanOperator::new(Box::new(EmptyOperator::new()), pattern);
            op.mapping = Some(Arc::clone(mapping));
            op.parent_memo = true;
            futures::executor::block_on(op.build_progress(ctx, Batch::single_empty()))
                .expect("build_progress");
            // `op` dropped here → its per-operator `parent_lookup_cache` is gone; only
            // the query-scoped ctx memo persists across the next `build_once`.
        }

        // The q031 seam: an inner join with an interposed FILTER + LIMIT rebuilds the
        // R2RML operator per driving batch. PR-4's per-operator cache is reset by
        // that rebuild; the query-scoped ctx memo (PR-8b) must survive — a FRESH
        // operator each iteration against ONE ctx ⇒ the DIM parent is scanned once,
        // not once per rebuild. (Distinct from `parent_lookup_memoized_across_child_batches`,
        // which reuses ONE operator across batches.)
        #[test]
        fn parent_lookup_survives_operator_rebuild() {
            let mapping = mapping();
            let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
            let vars = VarRegistry::new();
            let provider = CountingProvider::default();
            {
                let mut ctx = ExecutionContext::new(&snapshot, &vars);
                ctx.r2rml_table_provider = Some(&provider);
                for _ in 0..5 {
                    build_once(&ctx, &mapping, "gs:main");
                }
            }
            assert_eq!(
                provider
                    .scans
                    .lock()
                    .unwrap()
                    .get("customers")
                    .copied()
                    .unwrap_or(0),
                1,
                "ctx memo survives the operator rebuild: parent scanned once across 5 rebuilds"
            );
        }

        // A query-scoped memo shared across R2RML operators must NOT cross-pollute
        // two graph sources holding a same-named parent table: the key carries
        // `graph_source_id`, so `gs:A` and `gs:B` each scan `customers` once.
        #[test]
        fn parent_memo_isolated_by_graph_source() {
            let mapping = mapping();
            let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
            let vars = VarRegistry::new();
            let provider = CountingProvider::default();
            {
                let mut ctx = ExecutionContext::new(&snapshot, &vars);
                ctx.r2rml_table_provider = Some(&provider);
                build_once(&ctx, &mapping, "gs:A");
                build_once(&ctx, &mapping, "gs:B");
            }
            assert_eq!(
                provider.scans.lock().unwrap().get("customers").copied().unwrap_or(0),
                2,
                "two graph sources ⇒ two parent scans (graph_source_id in the key, no cross-source pollution)"
            );
        }
    }
}
