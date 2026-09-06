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
use crate::r2rml::policy::R2rmlPolicyGate;
use crate::r2rml::ColumnBatchStream;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_r2rml::mapping::{
    extract_template_columns, CompiledR2rmlMapping, ObjectMap, PredicateObjectMap, RefObjectMap,
    TriplesMap,
};
use fluree_db_r2rml::materialize::{
    canonical_join, expand_template, get_join_key_from_batch, materialize_object_from_batch,
    materialize_predicate_from_batch, materialize_subject_from_batch, parent_key_insert_keep_min,
    reverse_subject_template, RdfTerm,
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
/// query-wide share must not alias two snapshots). This is also what makes the F19
/// `with_graph_ref` memo-share sound: a referenced graph shares the one memo Arc,
/// but its parent lookups stay keyed under its own `graph_source_id` — so removing
/// that component would silently merge two stores' lookups (see `context.rs`).
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

/// Whether a single-column DESC `ORDER BY … LIMIT` may push a scan-side top-k
/// directive into the R2RML scan (PR-5), so it reads only the files that can hold
/// the top-k. Read once from `FLUREE_R2RML_TOPK_PUSHDOWN` (family falsy
/// spellings); off restores the full-materialize top-k (scan streams every row,
/// the `SortOperator` keeps k). Gating in `set_topk` means off ⇒ no directive ⇒
/// the scan takes its normal path (a full revert).
fn topk_pushdown_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_R2RML_TOPK_PUSHDOWN"))
}

/// F-AUD-3 kill switch: whether the non-aggregate scan/crawl path records its
/// materialized windows and fact-parent lookup builds against the query memory
/// budget. Default ON (`FLUREE_SCAN_MEM_ACCOUNTING`, family falsy spellings via
/// [`super::env_switch_enabled`]). ON makes a wide crawl trip a typed
/// `MemoryBudgetExceeded` (507) instead of OOMing — closing the blind spot the
/// audit's specimen 071cd59f (a point-lookup crawl that hard-OOM'd at 10 GB) fell
/// into, which the hash-join / group-aggregate `record_alloc`s never covered.
/// OFF is a clean revert to the prior behavior (scan path invisible to the budget;
/// the `checkpoint()` polls degrade to pure cancellation checks because the scan
/// records nothing). Each materialized window is charged then RELEASED once emitted
/// (it has a provable drop point), so a long streaming scan accounts only its
/// resident window rather than the all-time sum of every freed window — that sum
/// otherwise false-aborts a bounded-memory long scan (q038). The retained parent-map
/// build (A2) and the per-file buffers (V2 site B, still excluded) are the remaining
/// non-released allocations; A2 genuinely persists so its cumulative charge is
/// correct, and site B needs its own release pairing.
fn scan_mem_accounting_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| super::env_switch_enabled("FLUREE_SCAN_MEM_ACCOUNTING"))
}

/// F-AUD-3: conservative per-entry byte estimate for a `build_parent_lookup`
/// entry (a join key vec + a materialized subject `RdfTerm`, typically an IRI
/// string) used to account the fact-as-parent build against the budget. ~200 B is
/// the V2 estimate; like [`crate::context::BINDING_EST_BYTES`] it is a floor
/// (ignores per-entry heap beyond the term), safe because over-counting only trips
/// a build already near OOM.
const PARENT_ENTRY_EST_BYTES: usize = 200;

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
    /// Scan-side top-k directive (PR-5; ASC added in item 8): `(primary sort var,
    /// LIMIT+OFFSET, ascending)`, set by a `SortOperator` for an `ORDER BY
    /// <scan col> LIMIT k` directly above this scan. Resolved to the sort column
    /// against the mapping at scan time ([`Self::resolve_topk_directive`]); `None`
    /// = no pushdown (full scan + the authoritative sort above). Only ever
    /// consulted for the main table scan, never a parent/dimension lookup.
    topk: Option<(VarId, usize, bool)>,
    /// A scan-local FILTER the planner folded into this scan (see
    /// [`R2rmlPattern::consumed_filter`]). Applied to each output batch with the
    /// same evaluator the dropped `FilterOperator` would use, so results are
    /// unchanged — but now the LIMIT budget and the filter live in one operator,
    /// so a `FILTER + LIMIT` scan can stop after enough *matching* rows.
    consumed_filter: Option<PreparedBoolExpression>,
    /// View-policy gate, built at open when a non-root policy is active. `None`
    /// = unfiltered scan (see `r2rml::policy`).
    policy_gate: Option<R2rmlPolicyGate>,
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
            topk: None,
            consumed_filter,
            policy_gate: None,
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

    /// F-16: whether a pattern is a FOLDED crawl wildcard (W4-1b: `?s ?p ?o`
    /// plus same-subject const-object constraints) whose every constraint is a
    /// scalar. Only such a pattern may keep the `trust_fk_refs` FK-template ref
    /// shortcut: a scalar constraint checks a scalar column and never relaxes a
    /// ref, while an IRI (or any non-scalar) constraint on a ref predicate would
    /// be satisfied by the templated render of a DANGLING FK — the shortcut
    /// skips exactly the parent scan that would have dropped that subject, so
    /// re-enabling it there would over-match (catch #11). Var star members
    /// (`star_bindings`) also disqualify: they are fixed-predicate star shapes,
    /// not the crawl fold.
    fn folded_wildcard_all_scalar(pattern: &R2rmlPattern) -> bool {
        pattern.predicate_var.is_some()
            && pattern.star_bindings.is_empty()
            && !pattern.star_constraints.is_empty()
            && pattern
                .star_constraints
                .iter()
                .all(|(_, c)| matches!(c, crate::r2rml::ObjectConstant::Scalar(_)))
    }

    /// The `trust_fk_refs` FK-template ref-shortcut admission (the seam the
    /// parent-lookup build consults; see the comment at the call site). A
    /// pattern qualifies when trust is on, it is a plain OR scalar-folded true
    /// wildcard, and no predicate filter narrows it. Extracted so the composed
    /// predicate is pinned by `f16_ref_template_shortcut_fires_for_scalar_folded`
    /// — the production build calls THIS fn, so the test exercises the real
    /// admission, not a copy.
    fn ref_template_shortcut_enabled(trust_fk_refs: bool, pattern: &R2rmlPattern) -> bool {
        let star_free = pattern.star_bindings.is_empty() && pattern.star_constraints.is_empty();
        trust_fk_refs
            && (star_free || Self::folded_wildcard_all_scalar(pattern))
            && pattern.predicate_filter.is_none()
            && pattern.object_var.is_some()
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
    /// Resolve the stored top-k directive (`(sort_var, k)`) to a [`crate::r2rml::ScanTopK`]
    /// for THIS table scan, or `None` (→ full scan) when the sort var doesn't map
    /// to exactly one scalar pushdown column of `triples_map` — the same soundness
    /// gate `build_scan_filters` uses. The scan-side prune uses only this primary
    /// column; the `SortOperator` above still applies the exact compound order +
    /// LIMIT, so a `None` here is only a missed optimization, never wrong.
    fn resolve_topk_directive(&self, triples_map: &TriplesMap) -> Option<crate::r2rml::ScanTopK> {
        let (sort_var, k, ascending) = self.topk?;
        // SOUNDNESS (heap feed): decline when a residual filter the operator
        // enforces after the scan is present — the heap would see pre-filter rows.
        if topk_residual_filter_present(&self.pattern) {
            return None;
        }
        // The view-policy gate is a residual filter too: it drops rows AFTER the
        // scan emits, so a denied row can set the k-th bound and prune files whose
        // VISIBLE rows belong in the true top-k.
        if self.policy_gate.is_some() {
            return None;
        }
        let pred_iri = if Some(sort_var) == self.pattern.object_var {
            self.pattern.predicate_filter.as_deref()
        } else {
            self.pattern
                .star_bindings
                .iter()
                .find(|(_, v)| *v == sort_var)
                .map(|(p, _)| p.as_str())
        }?;
        let mut matching = triples_map
            .predicate_object_maps
            .iter()
            .filter(|p| p.predicate_map.as_constant() == Some(pred_iri));
        let (Some(pom), None) = (matching.next(), matching.next()) else {
            // Decline observably (PR-7's decline-breadcrumb convention, #1495
            // review): a mapping with duplicate predicates silently loses the
            // scan-side top-k otherwise, with nothing in the logs.
            tracing::debug!(
                predicate = %pred_iri,
                "r2rml topk declined: sort predicate does not map to exactly one POM"
            );
            return None;
        };
        let col = value_pushdown_column(&pom.object_map)?;
        Some(crate::r2rml::ScanTopK {
            sort_column: col.to_string(),
            k,
            ascending,
        })
    }

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
            push_scalar_eq_filter(&mut out, triples_map, pred_iri, value);
        }

        // W4-1 PRIMARY: a scalar constant-object member of a same-subject star
        // (`?ol ex:orderLineKey "1"; ?ol ex:order ?ord`) lands in `star_constraints`,
        // not the base `object_constant`, so without this it was enforced ONLY
        // residually — the whole FACT was read and filtered post-scan (the round-3b
        // point-lookup fanned into a full 120 M-row scan). Push each scalar star
        // constraint as a scan filter under the same soundness gate, so a constant
        // key equality prunes the scan even alongside other predicates. IRI/decimal/
        // double constraints stay operator-enforced only (not pushable here).
        for (pred_iri, constant) in &self.pattern.star_constraints {
            if let crate::r2rml::ObjectConstant::Scalar(value) = constant {
                push_scalar_eq_filter(&mut out, triples_map, pred_iri, value);
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
    ) -> Result<MaterializedRows> {
        use rayon::prelude::*;
        let encoder = LiteralEncoder::build(triples_map, ctx.active_snapshot);
        let pattern = &self.pattern;
        let derive_row_classes = self
            .policy_gate
            .as_ref()
            .is_some_and(|g| g.needs_row_classes(triples_map));
        let per_batch: Vec<MaterializedRows> = batches
            .par_iter()
            .map(|batch| {
                materialize_batch_rows(
                    pattern,
                    triples_map,
                    batch,
                    parent_lookups,
                    ref_shortcuts,
                    &encoder,
                    derive_row_classes,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let mut rows = Vec::new();
        let mut classes = derive_row_classes.then(Vec::new);
        for m in per_batch {
            rows.extend(m.rows);
            if let (Some(all), Some(part)) = (classes.as_mut(), m.classes) {
                all.extend(part);
            }
        }
        Ok(MaterializedRows { rows, classes })
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
        // F-16/catch-#10: the all-preds prune argument holds only for FIXED-predicate
        // stars (a map missing a member yields no complete star row). A folded crawl
        // WILDCARD emits per-(p,o) across co-subject maps, so a map lacking the
        // folded constraint predicate can still contribute rows — pruning it would
        // drop those rows for vertically-partitioned subjects (F10-class mappings).
        let star_required_preds: Vec<String> =
            if star_prune_on && self.pattern.predicate_var.is_none() && self.has_star_members() {
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

        // View policy: a map whose required predicates are all hidden from the
        // identity can produce no row — skip its table scan entirely.
        let triples_maps: Vec<&TriplesMap> = match self.policy_gate.as_mut() {
            Some(gate) => {
                let mut kept = Vec::with_capacity(triples_maps.len());
                for tm in triples_maps {
                    if gate.tm_can_yield(ctx, &self.pattern, tm).await? {
                        kept.push(tm);
                    } else {
                        tracing::debug!(
                            graph_source = %self.pattern.graph_source_id,
                            triples_map = %tm.iri,
                            "R2RML scan: TriplesMap skipped, its required predicates are hidden by view policy"
                        );
                    }
                }
                kept
            }
            None => triples_maps,
        };

        // Two maps minting this pattern's triples alike — the same rows,
        // subject and object maps, and the same classes where the pattern
        // projects the type — yield the same triples, which the graph holds
        // once: keep one of them. A variable predicate reads every map's
        // own predicates, so it never dedups.
        let triples_maps: Vec<&TriplesMap> = if self.pattern.predicate_var.is_some() {
            triples_maps
        } else {
            let preds = self.pattern_predicates();
            let class = self.pattern.class_filter.as_deref();
            let type_var = self.pattern.type_var.is_some();
            if preds.is_empty() && class.is_none() {
                triples_maps
            } else {
                let mut kept: Vec<&TriplesMap> = Vec::with_capacity(triples_maps.len());
                for tm in triples_maps {
                    let alike = kept.iter().any(|k| {
                        k.same_source_row(tm)
                            && preds.iter().all(|p| k.mints_alike(tm, p))
                            && class.is_none_or(|c| {
                                k.classes().iter().any(|x| x == c)
                                    && tm.classes().iter().any(|x| x == c)
                            })
                            && (!type_var || k.classes() == tm.classes())
                    });
                    if !alike {
                        kept.push(tm);
                    }
                }
                kept
            }
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
            let projection: Vec<String> = if self.pattern.predicate_var.is_some() {
                // Variable-predicate wildcard (`?s ?p ?o`): materialize EVERY POM, so
                // project ALL columns. A W4-1b-folded crawl wildcard ALSO carries
                // star_constraints, but those are a subject-level existence filter,
                // not a projection restriction — `columns_for_predicate(None)` already
                // covers the constraint predicates' columns too. (Without this branch a
                // folded wildcard fell into the `has_star_members` else below and
                // projected ONLY the constraint column, dropping every other POM's
                // value so `?p`/`?o` never materialized.)
                triples_map
                    .columns_for_predicate(None)
                    .into_iter()
                    .map(std::string::ToString::to_string)
                    .collect()
            } else if !self.has_star_members() {
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

            // View policy with column-derived rdf:type: project the type columns
            // too so each row's classes can be materialized for class policies.
            let projection: Vec<String> = match self.policy_gate.as_ref() {
                Some(gate) if gate.needs_row_classes(triples_map) => {
                    let mut cols = projection;
                    cols.extend(R2rmlPolicyGate::row_class_columns(triples_map));
                    cols.sort();
                    cols.dedup();
                    cols
                }
                _ => projection,
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
            //
            // A top-k scan (PR-5) ALSO bypasses the cache: it returns a pruned
            // file SUBSET, and the cache key `(table, projection)` does not carry
            // the directive — replaying that subset for a later FULL scan of the
            // same table+projection would silently drop rows (the exact silent-
            // wrong class the differential's second-scan case guards).
            //
            // The bypass keys on the STORED directive (`self.topk`), NOT the
            // resolved one: when `resolve_topk_directive` DECLINES (a residual
            // filter, below), `self.topk` is still `Some` so `cacheable` is false,
            // but `main_scan_topk` is `None` so the scan runs FULL. That full scan
            // then merely skips the cache — a missed optimization, never wrong. The
            // load-bearing invariant is one-directional: NO path that could return
            // a pruned subset (`self.topk.is_some()`) is ever cacheable, so a
            // pruned result can never poison the `(table, projection)` cache.
            let main_scan_topk = self.resolve_topk_directive(triples_map);
            let cacheable = scan_cache_enabled()
                && scan_filters.is_empty()
                && self.row_budget.is_none()
                && self.topk.is_none();
            let cache_key = (table_name.to_string(), projection.clone());
            let stream: ColumnBatchStream = if !cacheable {
                table_provider
                    .scan_table(
                        &self.pattern.graph_source_id,
                        table_name,
                        &projection,
                        &scan_filters,
                        main_scan_topk.as_ref(),
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
                        main_scan_topk.as_ref(),
                        as_of_t,
                    )
                    .await?;
                match collect_scan_capped(fresh, materialize_window_rows(), ctx).await? {
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
            //
            // F-16/catch-#10+#11 carve-out: a FOLDED crawl wildcard (W4-1b — the
            // same true-wildcard shape plus same-subject const-object constraints)
            // keeps the shortcut ONLY when every folded constraint is a scalar: a
            // scalar constraint checks a scalar column and never touches a ref,
            // while an IRI constraint on a ref predicate would be satisfied by the
            // templated render of a DANGLING FK (the shortcut skips the parent
            // scan that would have dropped it) — an over-match. Non-scalar
            // constraints keep the shortcut off and take the sound parent-scan
            // path.
            let ref_template_shortcut =
                Self::ref_template_shortcut_enabled(ctx.trust_fk_refs, &self.pattern);
            // Scoped so `star_preds` (which borrows `self`) is released before the
            // parent-lookup loop mutates `self.parent_lookup_cache` (PR-4).
            let filtered_poms: Vec<_> = {
                let star_preds = self.pattern_predicates();
                triples_map
                    .predicate_object_maps
                    .iter()
                    .filter(|pom| {
                        if self.pattern.predicate_var.is_none() && self.has_star_members() {
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
                    // MAJOR-1 (#1529 review): key the parent lookup through the SHARED
                    // `canonical_join` (parent columns ordered deterministically, child
                    // columns positionally aligned) so the index built here and the
                    // child-side probe in `materialize_pom_object` agree on column
                    // order. The old independent `parent_columns().sort()` matched the
                    // probe only when the child-declared order happened to agree with
                    // the parent-sorted order — a multi-column FK where they disagree
                    // transposed the probe key and silently dropped every edge.
                    let (parent_join_cols, _child_join_cols) = canonical_join(rom)?;
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
                            None,
                            as_of_t,
                        )
                        .await?;
                    let parent_batches = collect_stream(parent_stream, ctx).await?;

                    let lookup = Arc::new(build_parent_lookup(
                        ctx,
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
            let MaterializedRows {
                rows: produced,
                classes: row_classes,
            } = self.materialize_window(
                triples_map,
                &window,
                &progress.tms[i].parent_lookups,
                &progress.tms[i].ref_shortcuts,
                ctx,
            )?;
            // View policy: drop rows whose read triples the identity cannot see.
            let produced = match self.policy_gate.as_mut() {
                Some(gate) => {
                    gate.filter_rows(ctx, &self.pattern, triples_map, produced, row_classes)
                        .await?
                }
                None => produced,
            };

            // F-AUD-3 site A1: account the materialized window against the query
            // memory budget so a wide non-aggregate crawl (the previously-blind scan
            // path) trips a typed `MemoryBudgetExceeded` instead of OOMing. Charge the
            // resident window and `checkpoint()` BEFORE doing more work, so an oversized
            // window (or this window on top of a retained A2 fact-parent build / an
            // upstream fold) aborts typed. The window is then RELEASED once emitted
            // (below) — it has a provable drop point, so a streaming scan of N
            // sequentially-freed windows accounts only the resident one instead of
            // their all-time sum. Without the release, ~70 freed 512K-row windows of a
            // long un-fused-COUNT scan (q038) SUM past the budget and false-abort a
            // query whose per-window resident memory is bounded and fine.
            let window_est = if scan_mem_accounting_enabled() {
                let est = produced
                    .len()
                    .saturating_mul(num_cols)
                    .saturating_mul(crate::context::BINDING_EST_BYTES);
                ctx.record_alloc(est);
                ctx.checkpoint()?;
                est
            } else {
                0
            };

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

            // The window has been handed off (its rows copied into `columns` / the
            // bounded `self.pending` overflow) and `produced` drops at the end of this
            // iteration — release its charge so only the in-flight window is counted.
            // The retained overflow is bounded (≤ one window, drained before the next
            // pull) and intentionally left untracked (minimal per V2 site B). The A2
            // fact-parent build charge is NOT released — that map genuinely persists.
            if window_est != 0 {
                ctx.release(window_est);
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
async fn collect_stream(
    mut stream: ColumnBatchStream,
    ctx: &ExecutionContext<'_>,
) -> Result<Vec<ColumnBatch>> {
    let mut out = Vec::new();
    loop {
        // T3.1a: cancellation checkpoint before decoding the next batch, so a
        // deadline/abort stops a parent-dimension drain mid-sweep instead of
        // materializing the whole table into the lookup first. (The main
        // streaming scan already polls per-batch in `next_batch`; this closes the
        // collect-into-lookup helpers that drained a scan without a checkpoint.)
        ctx.check_cancelled()?;
        match stream.next().await {
            Some(batch) => out.push(batch?),
            None => break,
        }
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
async fn collect_scan_capped(
    mut stream: ColumnBatchStream,
    cap: usize,
    ctx: &ExecutionContext<'_>,
) -> Result<CollectedScan> {
    let mut collected = Vec::new();
    let mut rows = 0usize;
    while rows < cap {
        // T3.1a: cancellation checkpoint before decoding the next cached-inner
        // batch (bounded by the materialize window, but still a large drain).
        ctx.check_cancelled()?;
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
                    // Coerce numeric and temporal XSD literals from string to
                    // the typed FlakeValue: arithmetic reads the value, and
                    // `=` is a type error between a string-backed literal and
                    // a dateTime (ordering coerces, equality does not). Other
                    // datatypes keep their string form.
                    let val = match fluree_db_core::coerce_value(
                        FlakeValue::String(value.clone()),
                        dt_iri.as_ref(),
                    ) {
                        Ok(
                            c @ (FlakeValue::Long(_)
                            | FlakeValue::Double(_)
                            | FlakeValue::BigInt(_)
                            | FlakeValue::Decimal(_)
                            | FlakeValue::DateTime(_)
                            | FlakeValue::Date(_)
                            | FlakeValue::Time(_)),
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

/// Whether a pattern carries a RESIDUAL filter — one the operator enforces per
/// row AFTER the scan emits (a folded `consumed_filter`, a constant object or
/// subject, or a same-subject star existence constraint). The scan-side top-k
/// heap is fed the scan's EMITTED rows; a residual filter means those include
/// rows that don't survive to the result, so the k-th bound would ride too high
/// and prune files whose qualifying rows belong in the true top-k. When true, the
/// top-k pushdown MUST be declined (→ full sort). Pushed `scan_filters` are NOT
/// residual — the reader applies them, so the emitted batch is already filtered.
fn topk_residual_filter_present(pattern: &R2rmlPattern) -> bool {
    pattern.consumed_filter.is_some()
        || pattern.object_constant.is_some()
        || pattern.subject_constant.is_some()
        || !pattern.star_constraints.is_empty()
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

/// The xsd integer-family datatypes: types whose canonical lexical form of an
/// integer value carries no decimal point, so a decimal-free integer string is the
/// column's exact rendering. Only these admit the string→`Int` scan-filter coercion
/// (`coerce_scalar_for_pushdown`); `xsd:decimal`/`double` render with a fractional
/// part, so their lexical↔numeric relationship is scale-dependent and not coerced.
fn is_xsd_integer_datatype(iri: &str) -> bool {
    use fluree_vocab::xsd;
    matches!(
        iri,
        xsd::INTEGER
            | xsd::LONG
            | xsd::INT
            | xsd::SHORT
            | xsd::BYTE
            | xsd::UNSIGNED_LONG
            | xsd::UNSIGNED_INT
            | xsd::UNSIGNED_SHORT
            | xsd::UNSIGNED_BYTE
            | xsd::NON_NEGATIVE_INTEGER
            | xsd::POSITIVE_INTEGER
    )
}

/// Coerce a scalar constant to the [`ScanValue`] that soundly prunes `om`'s column,
/// or `None` to DECLINE the push (leaving the operator's residual check the sole
/// authority — a declined push is never wrong, only unpruned).
///
/// SOUNDNESS (the W4-1 hard requirement): the residual
/// [`rdf_term_eq_object_constant`] compares a `Scalar(Str)` constant LEXICALLY
/// against the column's materialized rendering, so a pushed file-prune filter is
/// admissible only when its match-set PROVABLY covers that residual match-set. The
/// one coercion performed — a string literal against a declared xsd-integer column —
/// fires ONLY when the string round-trips canonically (`n.to_string() == s`): then
/// lexical-eq ⟺ integer-eq and an `Int(n)` equality prunes exactly the residual's
/// rows. Non-canonical forms (`"01"`, `"1.0"`, a non-numeric string) are ambiguous
/// and DECLINED. A value already carrying a pushable, residual-matched type
/// (`Int`/`Bool`/`Date`) pushes as-is; a string against any non-integer column
/// pushes as-is too — a string column prunes lexicographically, and the reader
/// safely ignores a string filter on a numeric physical column (pre-existing).
fn coerce_scalar_for_pushdown(
    value: &crate::r2rml::ScanValue,
    om: &ObjectMap,
) -> Option<crate::r2rml::ScanValue> {
    use crate::r2rml::ScanValue;
    match value {
        ScanValue::Str(s) => {
            if object_map_datatype(om).is_some_and(is_xsd_integer_datatype) {
                match s.parse::<i64>() {
                    // Canonical round-trip ⇒ lexical-eq ⟺ integer-eq; otherwise the
                    // coercion is ambiguous and could over-prune vs the residual.
                    Ok(n) if n.to_string() == *s => Some(ScanValue::Int(n)),
                    _ => None,
                }
            } else {
                Some(ScanValue::Str(s.clone()))
            }
        }
        // Already a pushable, type-matched value; the residual uses the same
        // semantics for these variants.
        ScanValue::Int(_) | ScanValue::Bool(_) | ScanValue::Date(_) => Some(value.clone()),
        // Double/Decimal/TemplateKey/Timestamp never wrap a Scalar object constant
        // (a numeric/temporal object routes elsewhere, operator-enforced only), so
        // these are unreachable here; push as-is defensively — never wrong.
        ScanValue::Double(_)
        | ScanValue::Decimal { .. }
        | ScanValue::TemplateKey(_)
        | ScanValue::Timestamp { .. } => Some(value.clone()),
        // A `Set` only ever arrives via the FILTER-IN / VALUES set-pushdown path,
        // never wrapped in a `Scalar` object constant — decline to coerce it.
        ScanValue::Set(_) => None,
    }
}

/// Push an `Eq` scan filter for a scalar constant-object equality on `pred_iri`,
/// applying the [`coerce_scalar_for_pushdown`] soundness gate. Shared by the base
/// `object_constant` (single-predicate `?s pred const`) and the `star_constraints`
/// (a constant-object member of a same-subject star, e.g. `?ol …key "1"; ?ol ?p ?o`)
/// so both classes of constant key-equality prune the scan identically. A file-level
/// prune is only sound when the predicate maps to EXACTLY ONE scalar object map
/// backed by one column (else a row could match via an unpruned column); otherwise
/// no filter is pushed and the operator's residual check remains the authority.
fn push_scalar_eq_filter(
    out: &mut Vec<crate::r2rml::ScanFilter>,
    triples_map: &TriplesMap,
    pred_iri: &str,
    value: &crate::r2rml::ScanValue,
) {
    let mut matching = triples_map
        .predicate_object_maps
        .iter()
        .filter(|p| p.predicate_map.as_constant() == Some(pred_iri));
    let (Some(pom), None) = (matching.next(), matching.next()) else {
        return;
    };
    let Some(col) = value_pushdown_column(&pom.object_map) else {
        return;
    };
    if let Some(v) = coerce_scalar_for_pushdown(value, &pom.object_map) {
        out.push(crate::r2rml::ScanFilter {
            column: col.to_string(),
            op: crate::r2rml::ScanCmpOp::Eq,
            value: v,
        });
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
        // MAJOR-1 (#1529 review): resolve the join through the SHARED `canonical_join`
        // so the child probe key is built in the SAME column order the parent index
        // was keyed on (parent columns ordered deterministically, child columns
        // positionally aligned). Reading `child_columns()` in declared order
        // transposed the key whenever it disagreed with the parent-sorted order,
        // silently dropping the FK edge as if it were dangling.
        let (parent_join_cols, child_columns) = canonical_join(rom)?;
        // A null value in any join column means no FK reference at all → no triple
        // (both the scan and the shortcut agree; the shortcut relaxes only a
        // present-but-dangling FK, never a null one).
        let child_key = match get_join_key_from_batch(&child_columns, iceberg_batch, table_row_idx)
        {
            Some(k) => k,
            None => return Ok(None),
        };
        let lookup_key = (rom.parent_triples_map.clone(), parent_join_cols.clone());
        // Trusted browse crawl: render the parent IRI from the child's own FK
        // columns via the parent subject template — no parent scan. `child_key` is in
        // canonical (parent-aligned) order, so zip it with the SAME canonical parent
        // columns; `build_ref_shortcut` only fires for a single-column FK, so this is
        // one pair. Byte-identical to the scan path for a matched row (same template
        // + `iri_escape`; the join guarantees the child FK value equals the parent
        // key), differing only for a dangling FK (a templated IRI instead of no
        // triple — the intended browse relaxation).
        if let Some(sc) = ref_shortcuts.get(&lookup_key) {
            let values: HashMap<String, Option<String>> = parent_join_cols
                .iter()
                .zip(&child_key)
                .map(|(pc, cv)| (pc.clone(), Some(cv.clone())))
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
pub(crate) fn rdf_term_eq_object_constant_cached(
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
                // Double / Decimal `ScanValue`s only ever reach the scan as
                // FILTER pushdowns, never wrapped in a `Scalar` object constant
                // (a numeric object routes to `ObjectConstant::Double`/`Decimal`
                // above). These arms exist for match exhaustiveness; should one be
                // reached, match loosely by value so it can never be WRONG.
                ScanValue::Double(f) => v.parse::<f64>().is_ok_and(|x| x == *f),
                ScanValue::Decimal {
                    unscaled, scale, ..
                } => {
                    let d = bigdecimal::BigDecimal::new(
                        num_bigint::BigInt::from(*unscaled),
                        i64::from(*scale),
                    );
                    v.parse::<bigdecimal::BigDecimal>().is_ok_and(|x| x == d)
                }
                // A TemplateKey is only ever a reversed subject-key filter, never
                // an object constant, so it never matches an object term.
                ScanValue::TemplateKey(_) => false,
                // A Set is only ever a FILTER-IN / VALUES scan filter, never an
                // object constant, so it never matches an object term.
                ScanValue::Set(_) => false,
                // A Timestamp is only ever a FILTER pushdown value (dateTime object
                // constants are not lowered), never an object constant here.
                ScanValue::Timestamp { .. } => false,
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
pub(crate) fn decimal_canonical_of(constant: &crate::r2rml::ObjectConstant) -> Option<String> {
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
pub(crate) fn object_column_is_numeric(pom: &PredicateObjectMap, batch: &ColumnBatch) -> bool {
    let ObjectMap::Column { column, .. } = &pom.object_map else {
        return false;
    };
    matches!(
        batch.column_by_name(column),
        Some(Column::Decimal { .. } | Column::Float32(_) | Column::Float64(_))
    )
}

/// Whether the current row's subject satisfies EVERY folded `star_constraint` — each
/// constraint predicate must yield at least one object equal to its constant. The
/// E2 / W4-1b constant-object existence filter, shared by the fixed-predicate star
/// branch and the folded-wildcard subject pre-check. `star_constraint_canon` is the
/// per-constraint precomputed decimal canonical (parallel to `star_constraints`).
#[allow(clippy::too_many_arguments)]
fn row_passes_star_constraints(
    pattern: &R2rmlPattern,
    triples_map: &TriplesMap,
    iceberg_batch: &ColumnBatch,
    table_row_idx: usize,
    parent_lookups: &HashMap<(String, Vec<String>), Arc<ParentLookup>>,
    ref_shortcuts: &HashMap<LookupCacheKey, RefShortcut>,
    star_constraint_canon: &[Option<String>],
) -> Result<bool> {
    for ((pred, required), canon) in pattern.star_constraints.iter().zip(star_constraint_canon) {
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
                if rdf_term_eq_object_constant_cached(&t, required, numeric, canon.as_deref()) {
                    matched = true;
                    break;
                }
            }
        }
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Materialize one column batch into produced variable assignments (subject +
/// object vars) — the per-batch unit of the parallel scan. Mirrors the previous
/// per-row logic (star cross product, subject-only, single-object).
/// Produced rows of one column batch, optionally paired with each row's
/// column-derived classes (see [`R2rmlPolicyGate::needs_row_classes`]).
struct MaterializedRows {
    rows: Vec<Vec<(VarId, Binding)>>,
    /// Parallel to `rows` when requested.
    classes: Option<Vec<Vec<String>>>,
}

/// Test/convenience form of [`materialize_batch_rows`] without row classes.
#[cfg(test)]
fn materialize_batch(
    pattern: &R2rmlPattern,
    triples_map: &TriplesMap,
    iceberg_batch: &ColumnBatch,
    parent_lookups: &HashMap<(String, Vec<String>), Arc<ParentLookup>>,
    ref_shortcuts: &HashMap<LookupCacheKey, RefShortcut>,
    encoder: &LiteralEncoder,
) -> Result<Vec<Vec<(VarId, Binding)>>> {
    materialize_batch_rows(
        pattern,
        triples_map,
        iceberg_batch,
        parent_lookups,
        ref_shortcuts,
        encoder,
        false,
    )
    .map(|m| m.rows)
}

#[allow(clippy::too_many_arguments)]
fn materialize_batch_rows(
    pattern: &R2rmlPattern,
    triples_map: &TriplesMap,
    iceberg_batch: &ColumnBatch,
    parent_lookups: &HashMap<(String, Vec<String>), Arc<ParentLookup>>,
    ref_shortcuts: &HashMap<LookupCacheKey, RefShortcut>,
    encoder: &LiteralEncoder,
    derive_row_classes: bool,
) -> Result<MaterializedRows> {
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
    let mut row_classes: Option<Vec<Vec<String>>> = derive_row_classes.then(Vec::new);
    for table_row_idx in 0..iceberg_batch.num_rows {
        let before = produced.len();
        materialize_row(
            pattern,
            triples_map,
            iceberg_batch,
            table_row_idx,
            parent_lookups,
            ref_shortcuts,
            encoder,
            object_constant_canon.as_deref(),
            &star_constraint_canon,
            &mut produced,
        )?;
        if let Some(rc) = row_classes.as_mut() {
            if produced.len() > before {
                let classes = derived_row_classes(
                    triples_map,
                    iceberg_batch,
                    table_row_idx,
                    parent_lookups,
                    ref_shortcuts,
                )?;
                rc.extend(std::iter::repeat_n(classes, produced.len() - before));
            }
        }
    }
    Ok(MaterializedRows {
        rows: produced,
        classes: row_classes,
    })
}

/// A row's column-derived classes: every IRI a non-constant `rdf:type` object
/// map materializes for it (constant classes are static per map and handled by
/// the policy gate directly).
fn derived_row_classes(
    triples_map: &TriplesMap,
    iceberg_batch: &ColumnBatch,
    table_row_idx: usize,
    parent_lookups: &HashMap<(String, Vec<String>), Arc<ParentLookup>>,
    ref_shortcuts: &HashMap<LookupCacheKey, RefShortcut>,
) -> Result<Vec<String>> {
    let mut classes = Vec::new();
    for pom in &triples_map.predicate_object_maps {
        if pom.predicate_map.as_constant() != Some(fluree_vocab::rdf::TYPE)
            || matches!(pom.object_map, ObjectMap::Constant { .. })
        {
            continue;
        }
        if let Some(RdfTerm::Iri(iri)) = materialize_pom_object(
            pom,
            iceberg_batch,
            table_row_idx,
            parent_lookups,
            ref_shortcuts,
        )? {
            classes.push(iri);
        }
    }
    Ok(classes)
}

/// Materialize one table row of `triples_map` into produced assignments,
/// appending to `produced`. Split out of the batch loop so callers can attribute
/// the rows a table row produced (e.g. to pair them with that row's classes).
#[allow(clippy::too_many_arguments)]
fn materialize_row(
    pattern: &R2rmlPattern,
    triples_map: &TriplesMap,
    iceberg_batch: &ColumnBatch,
    table_row_idx: usize,
    parent_lookups: &HashMap<(String, Vec<String>), Arc<ParentLookup>>,
    ref_shortcuts: &HashMap<LookupCacheKey, RefShortcut>,
    encoder: &LiteralEncoder,
    object_constant_canon: Option<&str>,
    star_constraint_canon: &[Option<String>],
    produced: &mut Vec<Vec<(VarId, Binding)>>,
) -> Result<()> {
    let subject_term = match materialize_subject_from_batch(
        &triples_map.subject_map,
        iceberg_batch,
        table_row_idx,
    )? {
        Some(t) => t,
        None => return Ok(()),
    };

    // Bound-subject filter (`<store/5> <pred> ?o`): keep only rows whose
    // subject IRI equals the constant. This is the pattern's semantics,
    // enforced regardless of any scan pushdown.
    if let Some(want) = pattern.subject_constant.as_deref() {
        if !subject_term_matches_iri(&subject_term, want) {
            return Ok(());
        }
    }
    let subject_binding = encoder.encode(&subject_term);

    // A fixed-predicate same-subject star (extra star_bindings and/or folded
    // const-object star_constraints). A variable-predicate WILDCARD is excluded
    // here even when it carries folded star_constraints (W4-1b): it must reach
    // the wildcard POM loop below to bind ?p/?o, so its star_constraints are
    // applied as a subject-level existence pre-check there — this fixed-predicate
    // path never binds a predicate var and would emit a subject-only row.
    if pattern.predicate_var.is_none()
        && (!pattern.star_bindings.is_empty() || !pattern.star_constraints.is_empty())
    {
        let mut members: Vec<(VarId, &str)> = Vec::new();
        if let (Some(ov), Some(pf)) = (pattern.object_var, pattern.predicate_filter.as_deref()) {
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
        if row_ok
            && !row_passes_star_constraints(
                pattern,
                triples_map,
                iceberg_batch,
                table_row_idx,
                parent_lookups,
                ref_shortcuts,
                star_constraint_canon,
            )?
        {
            row_ok = false;
        }
        if !row_ok {
            return Ok(());
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
        return Ok(());
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
                        object_constant_canon,
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
            return Ok(());
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
        return Ok(());
    };

    // W4-1b: a folded crawl wildcard carries const-object members as
    // `star_constraints`. Apply them as a SUBJECT existence pre-check before
    // emitting any (p,o) row: keep this subject's wildcard rows only when every
    // constraint predicate yields its constant — the same existence filter the
    // standalone joined key scan enforced. Empty (a no-op) for a plain wildcard;
    // a fixed-predicate star with constraints took the star branch above, so only
    // a folded wildcard reaches here with a non-empty set.
    if !pattern.star_constraints.is_empty()
        && !row_passes_star_constraints(
            pattern,
            triples_map,
            iceberg_batch,
            table_row_idx,
            parent_lookups,
            ref_shortcuts,
            star_constraint_canon,
        )?
    {
        return Ok(());
    }

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
    Ok(())
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
    ctx: &ExecutionContext<'_>,
    parent_tm: &TriplesMap,
    parent_columns: &[String],
    batches: Vec<ColumnBatch>,
) -> Result<ParentLookup> {
    let mut lookup = ParentLookup::new();
    let mut dup_key_collisions = 0u64;

    for batch in batches {
        // F-AUD-3 site A2: the fact-as-parent hazard (V2) — a RefObjectMap whose
        // parent is a FACT table transiently builds a full parent-sized map (tens of
        // millions of entries) here, unbounded by the memo cap, which only refuses to
        // RETAIN an over-window lookup AFTER it is fully built. Account each batch's
        // worst-case contribution and checkpoint inside the build loop so a
        // budget-exceeding build aborts typed (`MemoryBudgetExceeded`) BEFORE the
        // whole map is resident, instead of OOMing. `num_rows` over-counts skipped
        // (null-subject / null-key) rows — deliberately conservative. Unlike the A1
        // scan window (released on hand-off), this charge is NOT released: the lookup
        // is genuinely retained for the whole join, so the cumulative charge correctly
        // equals its resident footprint.
        if scan_mem_accounting_enabled() {
            ctx.record_alloc(batch.num_rows.saturating_mul(PARENT_ENTRY_EST_BYTES));
            ctx.checkpoint()?;
        }
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

            // Deterministic keep-min on a duplicate join key (shared with the
            // materialize twin builder via `parent_key_insert_keep_min`), so the
            // virtual path and the twin resolve the SAME parent and both are
            // reproducible regardless of scan / IO-completion order — NOT the old
            // scan-order-dependent last-wins. The true R2RML fan-out (one edge per
            // matching parent) is a tracked shared-path follow-up.
            if parent_key_insert_keep_min(&mut lookup, key, subject_term) {
                dup_key_collisions += 1;
            }
        }
    }

    tracing::debug!(
        parent_tm = %parent_tm.iri,
        lookup_size = lookup.len(),
        dup_key_collisions,
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

    fn set_topk(&mut self, ordering: &[crate::sort::SortSpec], k: usize) {
        // Record the top-k directive; it is resolved to a scan column against the
        // mapping at scan time and honored only for the main table scan. Like
        // `row_budget`, do NOT forward to the child — an inner correlated scan must
        // still produce every row the join needs; only a topmost scan is eligible.
        // Only the primary key matters here: the scan skips files that cannot
        // hold the top-k by that key, a superset under ties, so a compound
        // ORDER BY is the sort's business.
        // An ASC directive is admitted only when the sort column is REQUIRED (the
        // provider re-checks nullability at scan time), and only under
        // `FLUREE_R2RML_TOPK_ASC` (default on): off is byte-identical to the
        // pre-item-8 DESC-only scan. The planner offers ASC unconditionally so
        // the SQL pushdown lane, whose ORDER BY columns are required by
        // construction, is not tied to this scan's switch.
        let Some(primary) = ordering.first() else {
            return;
        };
        let sort_var = primary.var;
        let ascending = matches!(primary.direction, crate::sort::SortDirection::Ascending);
        if ascending && !crate::r2rml::topk_asc_enabled() {
            return;
        }
        if topk_pushdown_enabled() {
            self.topk = Some((sort_var, k, ascending));
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

        self.policy_gate = R2rmlPolicyGate::build(ctx, &mapping, &self.pattern.graph_source_id);
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
            // Cancellation + memory checkpoint at the top of the internal loop: this
            // loop can pull many windows / files / child batches before returning a
            // full output batch, so the runner's between-`next_batch` check would
            // otherwise never run for a whole-table scan. Covers the loop's
            // non-advancing branches (overflow drain, child pull) that site 2 does
            // not. Upgraded from `check_cancelled()` to `checkpoint()` (F-AUD-3): it
            // also enforces the query memory budget against the window bytes recorded
            // by site A1 (a no-op for the budget when scan accounting is off, since
            // nothing on this path records then).
            ctx.checkpoint()?;
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
        self.policy_gate = None;
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

    /// T3.1a: a cancelled query must stop a parent-dimension drain up front
    /// (return `Cancelled`) instead of materializing the whole table into the
    /// lookup. Without the checkpoint the empty stream would drain to `Ok`.
    #[tokio::test]
    async fn collect_stream_bails_on_cancelled_query() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{LedgerSnapshot, QueryCancellation};
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let cancellation = QueryCancellation::new();
        cancellation.cancel();
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancellation);
        let stream: ColumnBatchStream = Box::pin(futures::stream::empty());
        assert!(matches!(
            collect_stream(stream, &ctx).await,
            Err(QueryError::Cancelled { .. })
        ));
    }

    /// T3.1a: same checkpoint on the capped cached-inner drain.
    #[tokio::test]
    async fn collect_scan_capped_bails_on_cancelled_query() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{LedgerSnapshot, QueryCancellation};
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let cancellation = QueryCancellation::new();
        cancellation.cancel();
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancellation);
        let stream: ColumnBatchStream = Box::pin(futures::stream::empty());
        assert!(matches!(
            collect_scan_capped(stream, 1000, &ctx).await,
            Err(QueryError::Cancelled { .. })
        ));
    }

    /// C3 (F-AUD-20): the drain loop must actually STOP MID-SWEEP — not merely bail
    /// up front on an already-cancelled empty stream (the two tests above). Here a
    /// 5-batch stream cancels the query *while producing its first batch*; the loop
    /// must consume exactly that one batch and then return `Cancelled` at the next
    /// checkpoint, leaving the remaining four unread. Without the per-iteration
    /// `check_cancelled()` the loop would drain all five (poll count 5).
    #[tokio::test]
    async fn collect_stream_stops_a_drain_loop_mid_sweep() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{LedgerSnapshot, QueryCancellation};
        use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let cancellation = QueryCancellation::new();
        let polls = Arc::new(AtomicUsize::new(0));

        let cancel_from_stream = cancellation.clone();
        let polls_in = Arc::clone(&polls);
        let stream: ColumnBatchStream = Box::pin(futures::stream::unfold(0usize, move |i| {
            let cancel_from_stream = cancel_from_stream.clone();
            let polls_in = Arc::clone(&polls_in);
            async move {
                if i >= 5 {
                    return None;
                }
                polls_in.fetch_add(1, Ordering::SeqCst);
                // Cancel as the FIRST batch is produced; the loop's next-iteration
                // checkpoint must catch it before pulling batch 2.
                if i == 0 {
                    cancel_from_stream.cancel();
                }
                let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
                    name: "K".to_string(),
                    field_type: FieldType::Int64,
                    nullable: true,
                    field_id: 1,
                }]));
                let batch = ColumnBatch::new(schema, vec![Column::Int64(vec![Some(1)])]).unwrap();
                Some((Ok(batch), i + 1))
            }
        }));

        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancellation);
        let result = collect_stream(stream, &ctx).await;
        assert!(
            matches!(result, Err(QueryError::Cancelled { .. })),
            "a mid-sweep cancel must stop the drain, got {result:?}"
        );
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "the loop consumed only the first batch, not the whole 5-batch stream"
        );
    }

    /// F-AUD-3 site A1 — specimen 071cd59f regression. A wide non-aggregate crawl
    /// (`?s ?p ?o`) materializes a window of bindings that the pre-fix scan path
    /// never recorded against the memory budget, so a runaway crawl OOM'd instead of
    /// aborting typed. With scan accounting on (the default), the materialized
    /// window is recorded and a tiny 1-byte ceiling makes the window checkpoint
    /// abort TYPED (`MemoryBudgetExceeded`, a 507) rather than completing/OOMing.
    #[tokio::test]
    async fn r3b_scan_window_budget_aborts_typed() {
        use crate::r2rml::R2rmlTableProvider;
        use crate::seed::EmptyOperator;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::QueryCancellation;
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, TriplesMap,
        };
        use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};

        #[derive(Debug)]
        struct StoreProvider;
        #[async_trait::async_trait]
        impl R2rmlTableProvider for StoreProvider {
            async fn scan_table(
                &self,
                _graph_source_id: &str,
                _table_name: &str,
                _projection: &[String],
                _filters: &[crate::r2rml::ScanFilter],
                _topk: Option<&crate::r2rml::ScanTopK>,
                _as_of_t: Option<i64>,
            ) -> Result<ColumnBatchStream> {
                let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
                    name: "STORE_KEY".to_string(),
                    field_type: FieldType::Int64,
                    nullable: true,
                    field_id: 1,
                }]));
                let batch =
                    ColumnBatch::new(schema, vec![Column::Int64(vec![Some(1), Some(2), Some(3)])])
                        .unwrap();
                Ok(Box::pin(futures::stream::once(async move { Ok(batch) })))
            }
        }

        let tm = TriplesMap::new("#Store", "DIM_STORE")
            .with_subject_template("http://ex/store/{STORE_KEY}")
            .with_class("http://ex/Store")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/storeKey"),
                object_map: ObjectMap::column("STORE_KEY"),
            });
        let mapping = Arc::new(CompiledR2rmlMapping::new(vec![tm]));
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let provider = StoreProvider;

        let cancel = QueryCancellation::new();
        cancel.set_memory_limit(1); // 1-byte ceiling → the first window's record crosses it.
        let mut ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancel);
        ctx.r2rml_table_provider = Some(&provider);

        // `?s ?p ?o` — the true-wildcard crawl the blind spot lived on.
        let pattern =
            R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2))).with_predicate_var(VarId(1));
        let mut op = R2rmlScanOperator::new(Box::new(EmptyOperator::new()), pattern);
        op.mapping = Some(Arc::clone(&mapping));

        let mut progress = op
            .build_progress(&ctx, Batch::single_empty())
            .await
            .expect("build_progress")
            .expect("wildcard crawl resolves the one TriplesMap");
        let num_cols = op.schema().len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();
        let err = op
            .advance_one_window(&ctx, &mut progress, num_cols, &mut columns)
            .await
            .expect_err("the materialized window must trip the 1-byte budget");
        assert!(
            matches!(err, QueryError::MemoryBudgetExceeded { .. }),
            "wide crawl window must abort typed, got {err:?}"
        );
    }

    /// F-AUD-3 site A1 — q038 regression (the false-abort the live re-bless caught).
    /// A long non-aggregate scan streams many windows that are each materialized then
    /// FREED; the per-window budget charge is released on hand-off, so the windows do
    /// not SUM to a false over-budget. Here 64 one-row windows (charge ~528 B each)
    /// run under an 8000 B ceiling: each resident window fits and the scan COMPLETES —
    /// whereas the pre-fix cumulative counter crossed the ceiling within ~16 windows
    /// and false-aborted a bounded-memory scan. The single-window abort test above
    /// (an oversized window on a 1-byte ceiling) still passes: the checkpoint fires
    /// while the window is charged, before it is released.
    #[tokio::test]
    async fn r3b_scan_windows_release_no_false_abort() {
        use crate::r2rml::R2rmlTableProvider;
        use crate::seed::EmptyOperator;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::QueryCancellation;
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, TriplesMap,
        };
        use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};

        const N_WINDOWS: usize = 64;

        #[derive(Debug)]
        struct ManyRowsProvider;
        #[async_trait::async_trait]
        impl R2rmlTableProvider for ManyRowsProvider {
            async fn scan_table(
                &self,
                _graph_source_id: &str,
                _table_name: &str,
                _projection: &[String],
                _filters: &[crate::r2rml::ScanFilter],
                _topk: Option<&crate::r2rml::ScanTopK>,
                _as_of_t: Option<i64>,
            ) -> Result<ColumnBatchStream> {
                let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
                    name: "STORE_KEY".to_string(),
                    field_type: FieldType::Int64,
                    nullable: true,
                    field_id: 1,
                }]));
                // N single-row batches → with window_rows pinned to 1, one window each.
                let batches: Vec<Result<ColumnBatch>> = (0..N_WINDOWS as i64)
                    .map(|k| {
                        Ok(ColumnBatch::new(
                            Arc::clone(&schema),
                            vec![Column::Int64(vec![Some(k + 1)])],
                        )
                        .unwrap())
                    })
                    .collect();
                Ok(Box::pin(futures::stream::iter(batches)))
            }
        }

        let tm = TriplesMap::new("#Store", "DIM_STORE")
            .with_subject_template("http://ex/store/{STORE_KEY}")
            .with_class("http://ex/Store")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/storeKey"),
                object_map: ObjectMap::column("STORE_KEY"),
            });
        let mapping = Arc::new(CompiledR2rmlMapping::new(vec![tm]));
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let provider = ManyRowsProvider;

        let cancel = QueryCancellation::new();
        // Between one window (~528 B) and the naive cumulative (64×528 ≈ 34 KB): the
        // pre-fix counter crosses this within ~16 windows; released windows never do.
        cancel.set_memory_limit(8000);
        let mut ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancel);
        ctx.r2rml_table_provider = Some(&provider);

        let pattern =
            R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2))).with_predicate_var(VarId(1));
        let mut op = R2rmlScanOperator::new(Box::new(EmptyOperator::new()), pattern);
        op.mapping = Some(Arc::clone(&mapping));

        let mut progress = op
            .build_progress(&ctx, Batch::single_empty())
            .await
            .expect("build_progress")
            .expect("wildcard crawl resolves the one TriplesMap");
        let num_cols = op.schema().len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();

        let mut windows = 0usize;
        loop {
            progress.window_rows = 1; // pin one row per window (bypass geometric growth) — env-free.
            let more = op
                .advance_one_window(&ctx, &mut progress, num_cols, &mut columns)
                .await
                .expect("a bounded-resident streaming scan must NOT false-abort on the budget");
            // Simulate the consumer draining the emitted batch so nothing accumulates
            // outside the released window charge.
            for c in &mut columns {
                c.clear();
            }
            op.pending.clear();
            // The window charge is released on hand-off, so live accounted memory
            // never approaches the naive cumulative — it stays within one window.
            assert!(
                ctx.mem_used() < 8000,
                "released window charge must keep live memory under budget, got {}",
                ctx.mem_used()
            );
            if !more {
                break;
            }
            windows += 1;
            assert!(windows < 1000, "safety bound");
        }
        assert!(
            windows >= N_WINDOWS,
            "the whole streaming scan must complete; got {windows} windows"
        );
    }

    /// F-AUD-3 site A2 — the fact-as-parent build. `build_parent_lookup` transiently
    /// materializes a full parent-sized map; with a RefObjectMap whose parent is a
    /// FACT table this is tens of millions of entries, unbounded by the memo cap.
    /// The per-batch accounting + checkpoint makes a budget-exceeding build abort
    /// TYPED before the whole map is resident (a 1-byte ceiling trips on the first
    /// batch here) instead of OOMing.
    #[test]
    fn r3b_parent_build_budget_aborts_typed() {
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{LedgerSnapshot, QueryCancellation};
        use fluree_db_r2rml::mapping::TriplesMap;

        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let cancel = QueryCancellation::new();
        cancel.set_memory_limit(1); // 1-byte ceiling.
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancel);

        let parent_tm = TriplesMap::new("#Customer", "customers")
            .with_subject_template("http://ex/customer/{ID}");
        // One batch of rows is enough — the per-batch record_alloc crosses the ceiling
        // before any row is inserted, so the build aborts on the first batch.
        let batch = single_col_batch("ID", vec![Some(1), Some(2), Some(3)]);
        let err = build_parent_lookup(&ctx, &parent_tm, &["ID".to_string()], vec![batch])
            .expect_err("the fact-parent build must trip the 1-byte budget");
        assert!(
            matches!(err, QueryError::MemoryBudgetExceeded { .. }),
            "fact-parent build must abort typed, got {err:?}"
        );
    }

    /// A two-column Int64 batch, for composite-FK tests.
    fn two_col_i64_batch(
        (n0, v0): (&str, Vec<Option<i64>>),
        (n1, v1): (&str, Vec<Option<i64>>),
    ) -> ColumnBatch {
        let schema = Arc::new(BatchSchema::new(vec![
            FieldInfo {
                name: n0.to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                field_id: 1,
            },
            FieldInfo {
                name: n1.to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                field_id: 2,
            },
        ]));
        ColumnBatch::new(schema, vec![Column::Int64(v0), Column::Int64(v1)]).unwrap()
    }

    #[test]
    fn composite_fk_disagreeing_name_order_resolves_via_canonical_join() {
        // MAJOR-1 (#1529 review): a composite FK whose child-declared column order
        // disagrees with its parent-sorted order. Before the fix the operator keyed
        // the parent index in parent-sorted order but probed it with the child key in
        // DECLARED order — transposed, so the edge was silently dropped as dangling.
        // With both sides routed through `canonical_join`, the edge resolves.
        use fluree_db_r2rml::mapping::{
            JoinCondition, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap,
        };

        // Conditions ordered so that sorting by PARENT column reorders the pairs:
        // (child c_hi -> parent z_zone), (child c_lo -> parent a_area).
        let rom = RefObjectMap::with_conditions(
            "#Zone",
            vec![
                JoinCondition::new("c_hi", "z_zone"),
                JoinCondition::new("c_lo", "a_area"),
            ],
        );
        // Canonical (pairs sorted by parent): parent [a_area, z_zone], child [c_lo, c_hi].
        let (parent_cols, child_cols) = canonical_join(&rom).unwrap();
        assert_eq!(
            parent_cols,
            vec!["a_area".to_string(), "z_zone".to_string()]
        );
        assert_eq!(child_cols, vec!["c_lo".to_string(), "c_hi".to_string()]);

        let parent_tm = TriplesMap::new("#Zone", "zones")
            .with_subject_template("http://ex/zone/{z_zone}_{a_area}");
        // Parent row a_area=10, z_zone=99 -> subject http://ex/zone/99_10.
        let parent_batch =
            two_col_i64_batch(("a_area", vec![Some(10)]), ("z_zone", vec![Some(99)]));

        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = crate::var_registry::VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let lookup = Arc::new(
            build_parent_lookup(&ctx, &parent_tm, &parent_cols, vec![parent_batch]).unwrap(),
        );
        let mut lookups: HashMap<(String, Vec<String>), Arc<ParentLookup>> = HashMap::new();
        lookups.insert(("#Zone".to_string(), parent_cols.clone()), lookup);
        let shortcuts: HashMap<LookupCacheKey, RefShortcut> = HashMap::new();

        // Child row c_hi=99 (-> z_zone), c_lo=10 (-> a_area); child-DECLARED order is
        // [c_hi, c_lo], the transposition the old probe fell into.
        let child_batch = two_col_i64_batch(("c_hi", vec![Some(99)]), ("c_lo", vec![Some(10)]));
        let pom = PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/inZone"),
            object_map: ObjectMap::RefObjectMap(rom),
        };
        let obj =
            materialize_pom_object(&pom, &child_batch, 0, &lookups, &shortcuts).expect("no error");
        assert_eq!(
            obj,
            Some(RdfTerm::iri("http://ex/zone/99_10")),
            "composite FK with disagreeing name order must resolve via canonical_join"
        );
    }

    #[test]
    fn build_parent_lookup_keeps_min_subject_on_duplicate_join_key() {
        // MAJOR-5 (#1529 review): build_parent_lookup's last-wins -> keep-min swap
        // changes which parent a LIVE virtual query binds on a duplicate join key,
        // yet reverting it left the whole suite green. Two parent rows share join key
        // 1 but mint distinct subjects; keep-min must bind the lexicographically
        // smaller IRI regardless of row order (rows are a-then-b, so last-wins would
        // pick 'b' — this pins the keep-min semantics on the live query path).
        let parent_tm = TriplesMap::new("#Customer", "customers")
            .with_subject_template("http://ex/customer/{SID}");
        let schema = Arc::new(BatchSchema::new(vec![
            FieldInfo {
                name: "JK".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                field_id: 1,
            },
            FieldInfo {
                name: "SID".to_string(),
                field_type: FieldType::String,
                nullable: false,
                field_id: 2,
            },
        ]));
        let batch = ColumnBatch::new(
            schema,
            vec![
                Column::Int64(vec![Some(1), Some(1)]),
                Column::String(vec![Some("a".to_string()), Some("b".to_string())]),
            ],
        )
        .unwrap();

        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = crate::var_registry::VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let lookup =
            build_parent_lookup(&ctx, &parent_tm, &["JK".to_string()], vec![batch]).unwrap();
        assert_eq!(
            lookup.get(&["1".to_string()][..]),
            Some(&RdfTerm::iri("http://ex/customer/a")),
            "keep-min must bind the lexicographically smaller subject on a dup join key"
        );
    }

    /// PR-5 soundness: the top-k pushdown must be declined whenever the scan
    /// carries a residual filter the operator enforces after emitting rows —
    /// otherwise the heap is fed pre-filter values and could prune files whose
    /// qualifying rows belong in the true top-k (silently dropping rows). A pure
    /// value scan (q046 shape) is eligible; a folded FILTER, a constant
    /// object/subject, or a same-subject star existence constraint declines.
    #[test]
    fn topk_declines_on_residual_filter() {
        let base = R2rmlPattern::new("gs:main", VarId(1), Some(VarId(2)))
            .with_predicate("http://ex/orderTotal");
        assert!(
            !topk_residual_filter_present(&base),
            "pure value scan is eligible"
        );

        let mut folded_filter = base.clone();
        folded_filter.consumed_filter = Some(crate::ir::Expression::Var(VarId(7)));
        assert!(topk_residual_filter_present(&folded_filter));

        let mut const_object = base.clone();
        const_object.object_constant = Some(ObjectConstant::Iri("http://ex/x".to_string()));
        assert!(topk_residual_filter_present(&const_object));

        let mut bound_subject = base.clone();
        bound_subject.subject_constant = Some("http://ex/o/1".to_string());
        assert!(topk_residual_filter_present(&bound_subject));

        let mut star_constraint = base.clone();
        star_constraint.star_constraints = vec![(
            "http://ex/isCurrent".to_string(),
            ObjectConstant::Scalar(ScanValue::Bool(true)),
        )];
        assert!(topk_residual_filter_present(&star_constraint));
    }

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

    #[test]
    fn parent_memo_refuses_insert_past_total_cap() {
        // PR-8b review: the total-rows cap must REFUSE an insert that would exceed it
        // (the caller then falls back to a per-batch rebuild for that key), while an
        // already-present key stays idempotent. The cap is passed directly to keep
        // the test env-hermetic (no FLUREE_R2RML_PARENT_MEMO_TOTAL_WINDOWS).
        let mut memo = R2rmlParentMemoInner::default();
        let lookup = |n: usize| -> std::sync::Arc<ParentLookup> {
            let mut m = ParentLookup::new();
            for i in 0..n {
                m.insert(vec![i.to_string()], RdfTerm::iri(format!("http://ex/{i}")));
            }
            std::sync::Arc::new(m)
        };
        let key = |s: &str| -> R2rmlParentMemoKey {
            (
                "gs".to_string(),
                s.to_string(),
                vec!["id".to_string()],
                None,
            )
        };
        assert!(memo.try_insert(key("A"), &lookup(2), 3)); // 2 <= 3
        assert!(memo.try_insert(key("A"), &lookup(2), 3)); // same key, idempotent
        assert!(!memo.try_insert(key("B"), &lookup(2), 3)); // 2 + 2 = 4 > 3 -> refused
        assert!(memo.try_insert(key("C"), &lookup(1), 3)); // 2 + 1 = 3 fits exactly
        assert!(!memo.try_insert(key("D"), &lookup(1), 3)); // at cap -> refused
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

    // W4-1 SECONDARY (the hard requirement): a pushed filter's match-set must
    // PROVABLY cover the residual's. The residual (`rdf_term_eq_object_constant`,
    // `Scalar(Str)` arm) compares LEXICALLY, so a string literal coerces to `Int`
    // ONLY when it round-trips canonically (lexical-eq ⟺ integer-eq); every
    // ambiguous form declines the push so the residual — never wrong — stays sole
    // authority. These are the refutation cases (residual-matches-but-would-mismatch
    // and non-numeric-vs-numeric are never over-pruned).
    #[test]
    fn w4_1_coerce_string_to_int_only_when_canonical() {
        use crate::r2rml::ScanValue;
        use fluree_db_r2rml::mapping::ObjectMap;

        let int_col = ObjectMap::column_typed("K", fluree_vocab::xsd::INTEGER);
        let long_col = ObjectMap::column_typed("K", fluree_vocab::xsd::LONG);
        let str_untyped = ObjectMap::column("NAME"); // no rr:datatype → xsd:string
        let str_typed = ObjectMap::column_typed("NAME", fluree_vocab::xsd::STRING);
        let dec_col = ObjectMap::column_typed("AMT", fluree_vocab::xsd::DECIMAL);

        // Canonical integer string against an integer column → coerced (prunes).
        assert_eq!(
            coerce_scalar_for_pushdown(&ScanValue::Str("1".into()), &int_col),
            Some(ScanValue::Int(1))
        );
        assert_eq!(
            coerce_scalar_for_pushdown(&ScanValue::Str("-5".into()), &long_col),
            Some(ScanValue::Int(-5))
        );

        // REFUTATION — non-canonical / non-integer against an integer column DECLINE
        // (never over-prune vs the lexical residual): "01" (leading zero), "1.0"
        // (fraction), "+1" (sign form), " 1" (space), "abc" (non-numeric), "-0"
        // (parses to 0 whose canonical render "0" ≠ "-0", so the pushed Int would
        // prune rows the lexical residual keeps), and a >i64 value (parse overflow).
        for s in [
            "01",
            "1.0",
            "+1",
            " 1",
            "abc",
            "",
            "-0",
            "99999999999999999999",
        ] {
            assert_eq!(
                coerce_scalar_for_pushdown(&ScanValue::Str(s.into()), &int_col),
                None,
                "non-canonical {s:?} against an integer column must DECLINE the push"
            );
        }

        // A string against a string column pushes AS-IS (lexicographic prune matches
        // the residual's lexical compare) — no coercion, incl. non-canonical forms.
        assert_eq!(
            coerce_scalar_for_pushdown(&ScanValue::Str("1".into()), &str_untyped),
            Some(ScanValue::Str("1".into()))
        );
        assert_eq!(
            coerce_scalar_for_pushdown(&ScanValue::Str("01".into()), &str_typed),
            Some(ScanValue::Str("01".into()))
        );
        // A string against a DECIMAL column is not coerced (scale-dependent lexical
        // relationship) → pushed as-is; the reader safely ignores a string filter on
        // a numeric physical column (no prune, never wrong).
        assert_eq!(
            coerce_scalar_for_pushdown(&ScanValue::Str("1".into()), &dec_col),
            Some(ScanValue::Str("1".into()))
        );

        // Already-typed, residual-matched values push as-is.
        assert_eq!(
            coerce_scalar_for_pushdown(&ScanValue::Int(7), &int_col),
            Some(ScanValue::Int(7))
        );
        assert_eq!(
            coerce_scalar_for_pushdown(&ScanValue::Bool(true), &str_untyped),
            Some(ScanValue::Bool(true))
        );
    }

    // W4-1 PRIMARY dispatch: the shared push helper resolves a scalar key equality
    // to a single scalar pushdown column and applies the coercion gate, declining
    // on a duplicate-predicate (unsound file-prune) or a non-column object map.
    #[test]
    fn w4_1_push_scalar_eq_filter_resolution_and_gate() {
        use crate::r2rml::{ScanCmpOp, ScanValue};
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};

        let tm = TriplesMap::new("#OrderLine", "FACT_ORDER_LINE")
            .with_subject_template("http://ex/ol/{ORDER_LINE_KEY}")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/orderLineKey"),
                object_map: ObjectMap::column_typed("ORDER_LINE_KEY", fluree_vocab::xsd::INTEGER),
            });

        // Canonical key equality → one coerced Int filter on the mapped column.
        let mut out = Vec::new();
        push_scalar_eq_filter(
            &mut out,
            &tm,
            "http://ex/orderLineKey",
            &ScanValue::Str("1".into()),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].column, "ORDER_LINE_KEY");
        assert!(matches!(out[0].op, ScanCmpOp::Eq));
        assert_eq!(out[0].value, ScanValue::Int(1));

        // Non-canonical → declined (coercion gate).
        let mut out = Vec::new();
        push_scalar_eq_filter(
            &mut out,
            &tm,
            "http://ex/orderLineKey",
            &ScanValue::Str("01".into()),
        );
        assert!(out.is_empty());

        // Duplicate predicate (two POMs) → not a sound single-column prune → declined.
        let tm_dup = tm.clone().with_predicate_object(PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/orderLineKey"),
            object_map: ObjectMap::column("ORDER_LINE_KEY_ALT"),
        });
        let mut out = Vec::new();
        push_scalar_eq_filter(
            &mut out,
            &tm_dup,
            "http://ex/orderLineKey",
            &ScanValue::Str("1".into()),
        );
        assert!(out.is_empty());

        // A value-transforming (template) object map is not pushable → declined.
        let tm_tmpl = TriplesMap::new("#X", "T").with_predicate_object(PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/p"),
            object_map: ObjectMap::template("PRE-{C}", vec!["C".to_string()]),
        });
        let mut out = Vec::new();
        push_scalar_eq_filter(
            &mut out,
            &tm_tmpl,
            "http://ex/p",
            &ScanValue::Str("1".into()),
        );
        assert!(out.is_empty());
    }

    // W4-1 PRIMARY end-to-end: a scalar constant-object member of a same-subject
    // star (`?ol …key "1"; ?ol ex:order ?ord`) lands in `star_constraints` and now
    // produces a scan filter, so the multi-predicate point-lookup prunes the FACT
    // scan (was residual-only → full read). The residual guard stays intact.
    #[test]
    fn w4_1_star_constraint_pushes_scan_filter() {
        use crate::r2rml::{ObjectConstant, ScanCmpOp, ScanValue};
        use crate::seed::EmptyOperator;
        use fluree_db_r2rml::mapping::{ObjectMap, PredicateMap, PredicateObjectMap};

        let tm = TriplesMap::new("#OrderLine", "FACT_ORDER_LINE")
            .with_subject_template("http://ex/ol/{ORDER_LINE_KEY}")
            .with_predicate_object(PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/orderLineKey"),
                object_map: ObjectMap::column_typed("ORDER_LINE_KEY", fluree_vocab::xsd::INTEGER),
            })
            .with_predicate_object(pom("http://ex/order", "ORDER_KEY"));

        // ?ol ex:order ?ord (the star base) + ?ol ex:orderLineKey "1" (folded const).
        let mut pattern = R2rmlPattern::new("gs:main", VarId(0), Some(VarId(1)));
        pattern.predicate_filter = Some("http://ex/order".to_string());
        pattern.star_constraints = vec![(
            "http://ex/orderLineKey".to_string(),
            ObjectConstant::Scalar(ScanValue::Str("1".to_string())),
        )];
        // The star constraint is a residual filter regardless of the push (the
        // operator still enforces it post-scan) — the push is an optimization on top.
        assert!(topk_residual_filter_present(&pattern));

        let op = R2rmlScanOperator::new(Box::new(EmptyOperator::new()), pattern);
        let filters = op.build_scan_filters(&tm);
        let key = filters
            .iter()
            .find(|f| f.column == "ORDER_LINE_KEY")
            .expect("the folded key equality must now push a scan filter");
        assert!(matches!(key.op, ScanCmpOp::Eq));
        assert_eq!(key.value, ScanValue::Int(1));
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

    // W4-1b SHIP-BLOCKER regression (lambda-audit-2): a FOLDED crawl wildcard carries
    // const-object members as star_constraints. Before the execution fix,
    // has_star_members()==true routed it into the fixed-predicate star materialize,
    // which emitted a subject-only row and NEVER bound ?p/?o. It must instead behave
    // as a wildcard whose SUBJECT is filtered by the constraint: satisfied → all
    // (p,o) rows; violated → the subject drops entirely.
    #[test]
    fn w4_1b_folded_wildcard_emits_po_rows_under_star_constraint() {
        use crate::r2rml::{ObjectConstant, ScanValue};
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
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let encoder = LiteralEncoder::build(&tm, &snapshot);
        let lookups: HashMap<(String, Vec<String>), Arc<ParentLookup>> = HashMap::new();
        let shortcuts: HashMap<LookupCacheKey, RefShortcut> = HashMap::new();

        // `?s ?p ?o` with a folded const-object constraint storeKey==7 (SATISFIED):
        // behaves exactly like the plain wildcard — data POM row + rdf:type row, each
        // binding ?p AND ?o (this is what the ship-blocker broke).
        let mut pass =
            R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2))).with_predicate_var(VarId(1));
        pass.star_constraints = vec![(
            "http://ex/storeKey".to_string(),
            ObjectConstant::Scalar(ScanValue::Int(7)),
        )];
        let rows =
            materialize_batch(&pass, &tm, &batch, &lookups, &shortcuts, &encoder).expect("mat");
        assert_eq!(
            rows.len(),
            2,
            "satisfied → wildcard emits (p,o): data POM + type row, NOT a subject-only row: {rows:?}"
        );
        // The folded wildcard must emit the SAME (p,o) triples the UNFOLDED wildcard
        // does when the subject satisfies the constraint (lambda-audit-2: assert the
        // bindings, not just the count — a count alone could pass on two subject-only
        // rows from some other path). Mirrors var_subject_wildcard_emits_rdf_type_rows.
        assert!(
            rows.iter().any(|r| {
                find(r, VarId(1))
                    .map(|b| matches!(b, Binding::Iri(s) if &**s == "http://ex/storeKey"))
                    .unwrap_or(false)
                    && find(r, VarId(2)).is_some()
            }),
            "data POM row must bind ?p=storeKey AND ?o: {rows:?}"
        );
        assert!(
            rows.iter().any(|r| {
                find(r, VarId(1))
                    .map(|b| matches!(b, Binding::Iri(s) if &**s == fluree_vocab::rdf::TYPE))
                    .unwrap_or(false)
                    && find(r, VarId(2)).map(iri_of).as_deref() == Some("http://ex/Store")
            }),
            "rdf:type row must bind ?p=rdf:type AND ?o=Store: {rows:?}"
        );

        // Same wildcard, constraint storeKey==8 (VIOLATED) → the subject drops.
        let mut fail =
            R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2))).with_predicate_var(VarId(1));
        fail.star_constraints = vec![(
            "http://ex/storeKey".to_string(),
            ObjectConstant::Scalar(ScanValue::Int(8)),
        )];
        let rows =
            materialize_batch(&fail, &tm, &batch, &lookups, &shortcuts, &encoder).expect("mat");
        assert!(
            rows.is_empty(),
            "violated → no rows (subject filtered by the star_constraint): {rows:?}"
        );
    }

    /// F-16/catch-#11: the `trust_fk_refs` shortcut re-enable for a folded crawl
    /// wildcard is SCALAR-GATED. A scalar-constrained fold qualifies; an IRI
    /// constraint (which a dangling-FK template render would falsely satisfy),
    /// a mixed set, a fixed-predicate star (`star_bindings`), a plain wildcard
    /// (no constraints — governed by `!has_star_members()`, not this helper),
    /// and a non-wildcard all DISQUALIFY.
    #[test]
    fn f16_folded_wildcard_all_scalar_gate() {
        use crate::r2rml::{ObjectConstant, ScanValue};
        let scalar = |p: &str| (p.to_string(), ObjectConstant::Scalar(ScanValue::Int(7)));
        let iri = |p: &str| {
            (
                p.to_string(),
                ObjectConstant::Iri("http://ex/order/5".to_string()),
            )
        };

        // Folded wildcard, all-scalar → qualifies.
        let mut pass =
            R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2))).with_predicate_var(VarId(1));
        pass.star_constraints = vec![scalar("http://ex/lineNumber")];
        assert!(R2rmlScanOperator::folded_wildcard_all_scalar(&pass));

        // IRI constraint → disqualified (dangling-FK over-match hazard).
        let mut ref_c = pass.clone();
        ref_c.star_constraints = vec![iri("http://ex/order")];
        assert!(!R2rmlScanOperator::folded_wildcard_all_scalar(&ref_c));

        // Mixed scalar + IRI → disqualified (every constraint must be scalar).
        let mut mixed = pass.clone();
        mixed.star_constraints = vec![scalar("http://ex/lineNumber"), iri("http://ex/order")];
        assert!(!R2rmlScanOperator::folded_wildcard_all_scalar(&mixed));

        // Fixed-predicate star member present → not the crawl fold.
        let mut star = pass.clone();
        star.star_bindings = vec![("http://ex/qty".to_string(), VarId(3))];
        assert!(!R2rmlScanOperator::folded_wildcard_all_scalar(&star));

        // Plain wildcard (no constraints) → not this helper's business.
        let plain =
            R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2))).with_predicate_var(VarId(1));
        assert!(!R2rmlScanOperator::folded_wildcard_all_scalar(&plain));

        // Fixed-predicate pattern (no predicate_var) → disqualified.
        let mut fixed = R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2)));
        fixed.star_constraints = vec![scalar("http://ex/lineNumber")];
        assert!(!R2rmlScanOperator::folded_wildcard_all_scalar(&fixed));
    }

    /// Task #17 — the F-16 closure evidence: the SHORTCUT FIRES (not merely the
    /// flag) for a scalar-folded crawl wildcard under `trust_fk_refs`. The
    /// production parent-lookup build consults `ref_template_shortcut_enabled`
    /// (this exact fn) and, when true, inserts the `build_ref_shortcut` result
    /// instead of scanning the parent — so admission==true AND
    /// build_ref_shortcut==Some together pin the fire condition for the folded
    /// shape. The corpus cannot exercise this seam: SPARQL queries run via
    /// `query_from` where `trust_fk_refs` is always false (the shortcut is a
    /// browse-crawl-API feature), so a regression here is perf-silent to q068.
    #[test]
    fn f16_ref_template_shortcut_fires_for_scalar_folded() {
        use crate::r2rml::{ObjectConstant, ScanValue};
        // The ref side: single-column templated FK — shortcut-eligible (the
        // deployed `edw:order` → FactOrder shape).
        let parent = TriplesMap::new("#Order", "FACT_ORDER")
            .with_subject_template("http://ex/order/{ORDER_KEY}");
        let rom = RefObjectMap::new("#Order", "ORDER_KEY", "ORDER_KEY");
        assert!(
            build_ref_shortcut(&parent, &rom).is_some(),
            "single-col templated FK must be shortcut-eligible"
        );

        // Scalar-folded crawl wildcard + trust ON → admission true ⇒ with the
        // eligible ref above, the build inserts the shortcut (no parent scan).
        let mut folded =
            R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2))).with_predicate_var(VarId(1));
        folded.star_constraints = vec![(
            "http://ex/lineNumber".to_string(),
            ObjectConstant::Scalar(ScanValue::Int(1)),
        )];
        assert!(R2rmlScanOperator::ref_template_shortcut_enabled(
            true, &folded
        ));

        // Iri-folded → admission false (sound parent-scan path; catch #11).
        let mut iri_folded = folded.clone();
        iri_folded.star_constraints = vec![(
            "http://ex/order".to_string(),
            ObjectConstant::Iri("http://ex/order/5".to_string()),
        )];
        assert!(!R2rmlScanOperator::ref_template_shortcut_enabled(
            true,
            &iri_folded
        ));

        // Trust OFF (the chat/`query_from` path) → admission false regardless.
        assert!(!R2rmlScanOperator::ref_template_shortcut_enabled(
            false, &folded
        ));

        // Plain wildcard + trust ON → admission true (pre-existing behavior,
        // unchanged by the F-16 amendment).
        let plain =
            R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2))).with_predicate_var(VarId(1));
        assert!(R2rmlScanOperator::ref_template_shortcut_enabled(
            true, &plain
        ));

        // Fixed-predicate star + trust ON → admission false (star shapes keep
        // parent-scan + dangling-FK semantics; unchanged).
        let mut star = R2rmlPattern::new("gs:main", VarId(0), Some(VarId(2)));
        star.star_constraints = vec![(
            "http://ex/lineNumber".to_string(),
            ObjectConstant::Scalar(ScanValue::Int(1)),
        )];
        assert!(!R2rmlScanOperator::ref_template_shortcut_enabled(
            true, &star
        ));
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
                _topk: Option<&crate::r2rml::ScanTopK>,
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
                _topk: Option<&crate::r2rml::ScanTopK>,
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

        // F19 residual fix: a correlated inner join rebuilt across a `with_graph_ref`
        // boundary (SERVICE / multi-source default R2RML) must ALSO keep the memo.
        // `with_graph_ref` switches store, so it re-creates `const_sid_cache` fresh —
        // but the parent-memo key is store-disambiguated (graph_source_id + as_of_t),
        // so it now CLONES the memo Arc. Deriving the ctx per rebuild via
        // `with_graph_ref` therefore scans the DIM parent ONCE, not once per rebuild.
        // (Before the fix, `with_graph_ref` used `R2rmlParentMemo::default()` and this
        // asserted 5 — the `with_active_graph`-only coverage missed this path.)
        #[test]
        fn parent_memo_survives_with_graph_ref_rebuild() {
            use crate::dataset::GraphRef;
            let mapping = mapping();
            let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
            let vars = VarRegistry::new();
            let no_overlay = fluree_db_core::NoOverlay;
            let graph = GraphRef::new(&snapshot, 0, &no_overlay, snapshot.t, "test/main");
            let provider = CountingProvider::default();
            {
                let mut base = ExecutionContext::new(&snapshot, &vars);
                base.r2rml_table_provider = Some(&provider);
                for _ in 0..5 {
                    let gctx = base.with_graph_ref(&graph);
                    build_once(&gctx, &mapping, "gs:main");
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
                "with_graph_ref shares the parent memo: parent scanned once across 5 rebuilds"
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

        // PR-5 cache-poison guard: a top-k scan returns a PRUNED file subset, so it
        // must never populate/replay the per-operator scan_cache — a later full scan
        // of the same (table, projection) must see FULL results, not the subset.
        // Proven by scan COUNT across repeated batches: WITHOUT topk the main table
        // is cached (scanned once); WITH topk it is re-scanned each batch (the
        // `cacheable = … && self.topk.is_none()` bypass held). This is the test that
        // keeps the bypass true across future refactors.
        #[test]
        fn topk_scan_bypasses_scan_cache() {
            let orders = TriplesMap::new("#Order", "orders")
                .with_subject_template("http://ex/order/{ORDER_KEY}")
                .with_predicate_object(PredicateObjectMap {
                    predicate_map: PredicateMap::constant("edw:orderTotal"),
                    object_map: ObjectMap::column("ORDER_TOTAL"),
                });
            let mapping = Arc::new(CompiledR2rmlMapping::new(vec![orders]));

            fn orders_scans(topk: bool, mapping: &Arc<CompiledR2rmlMapping>) -> usize {
                let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
                let vars = VarRegistry::new();
                let provider = CountingProvider::default();
                {
                    let mut ctx = ExecutionContext::new(&snapshot, &vars);
                    ctx.r2rml_table_provider = Some(&provider);
                    let mut pattern = R2rmlPattern::new("gs:main", VarId(0), Some(VarId(1)));
                    pattern.predicate_filter = Some("edw:orderTotal".to_string());
                    let mut op = R2rmlScanOperator::new(Box::new(EmptyOperator::new()), pattern);
                    op.mapping = Some(Arc::clone(mapping));
                    if topk {
                        op.topk = Some((VarId(1), 10, false));
                    }
                    for _ in 0..3 {
                        futures::executor::block_on(op.build_progress(&ctx, Batch::single_empty()))
                            .expect("build_progress");
                    }
                }
                let count = provider
                    .scans
                    .lock()
                    .unwrap()
                    .get("orders")
                    .copied()
                    .unwrap_or(0);
                count
            }

            assert_eq!(
                orders_scans(false, &mapping),
                1,
                "no topk: the main scan is cached and replayed across batches"
            );
            assert_eq!(
                orders_scans(true, &mapping),
                3,
                "topk: the main scan bypasses the cache each batch — a pruned subset is never cached"
            );
        }
    }
}
