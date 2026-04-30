//! BM25 Search Operator (Pattern::IndexSearch)
//!
//! This operator executes BM25 full-text search against a loaded BM25 graph source index
//! and emits bindings for:
//! - f:resultId      -> `Binding::IriMatch` (canonical IRI with ledger provenance for cross-ledger joins)
//!   or `Binding::Iri` (if IRI cannot be encoded to SID)
//! - f:resultScore   -> `Binding::Lit` (xsd:double)
//! - f:resultLedger  -> `Binding::Lit` (xsd:string; ledger alias) [optional]
//!
//! # Multi-Ledger Support
//!
//! BM25 search works in both single-ledger and multi-ledger (dataset) contexts. The operator
//! doesn't scan graphs directly - it consults the BM25 index provider by graph source alias.
//! Results are emitted as `IriMatch` bindings with ledger provenance, enabling correct
//! cross-ledger joins (same pattern as VectorSearchOperator).
//!
//! # Notes
//!
//! - The BM25 index stores canonical IRIs with ledger provenance; we encode them using
//!   `ctx.encode_iri_in_ledger(iri, ledger_alias)` for correct multi-ledger semantics.
//! - If an IRI cannot be encoded, we fall back to `Binding::Iri` which allows IRI-based
//!   comparisons but won't constrain scans.
//! - The operator is correlated: it consumes a child stream (often an EmptyOperator seed)
//!   and may read the query target from an input variable.

use crate::binding::{Batch, Binding, RowAccess};
use crate::bm25::{Analyzer, Bm25Index, Bm25Scorer};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::ir::{IndexSearchPattern, IndexSearchTarget};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::FlakeValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// Re-export SearchHit from protocol for unified hit type across all layers
pub use fluree_search_protocol::SearchHit;

/// Provider for BM25 indexes keyed by graph source alias.
///
/// This is the lower-level provider that returns the raw index for local scoring.
/// Higher layers (API/connection) implement this by consulting nameservice graph source records
/// and loading the snapshot from storage, possibly with sync-to-t semantics.
///
/// For remote search scenarios, use [`Bm25SearchProvider`] instead, which returns
/// search results directly without requiring a local index.
#[async_trait]
pub trait Bm25IndexProvider: std::fmt::Debug + Send + Sync {
    /// Return the BM25 index for a graph source alias.
    ///
    /// # `as_of_t`
    ///
    /// - In single-ledger mode, this is typically `Some(ctx.to_t)`.
    /// - In dataset (multi-ledger) mode, there is no meaningful "dataset t".
    ///   Callers should pass `None` (meaning: provider selects latest) unless
    ///   the query itself provides an unambiguous as-of anchor.
    async fn bm25_index(
        &self,
        graph_source_id: &str,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> Result<Arc<Bm25Index>>;
}

/// Result of a BM25 search operation.
///
/// Contains the index watermark and search hits. This is the return type for
/// [`Bm25SearchProvider::search_bm25`].
#[derive(Debug, Clone)]
pub struct Bm25SearchResult {
    /// Transaction time watermark of the index that was searched.
    ///
    /// Clients can use this to measure staleness vs ledger head.
    pub index_t: i64,

    /// Search hits in descending score order.
    ///
    /// Uses the unified [`SearchHit`] type from `fluree-search-protocol`.
    pub hits: Vec<SearchHit>,
}

impl Bm25SearchResult {
    /// Create a new search result.
    pub fn new(index_t: i64, hits: Vec<SearchHit>) -> Self {
        Self { index_t, hits }
    }

    /// Create an empty search result.
    pub fn empty(index_t: i64) -> Self {
        Self {
            index_t,
            hits: Vec::new(),
        }
    }
}

/// Provider that returns BM25 search results directly.
///
/// This is the preferred provider interface for the search service protocol.
/// It supports both embedded and remote search backends:
///
/// - **Embedded**: The `EmbeddedBm25SearchProvider` adapter (in `fluree-db-api`) wraps
///   a [`Bm25IndexProvider`] and performs local scoring.
/// - **Remote**: The `RemoteBm25SearchProvider` (in `fluree-db-api`) implements this
///   trait by making HTTP calls to the search service endpoint.
///
/// The operator can use either this provider (when available) or fall back to
/// [`Bm25IndexProvider`] for backward compatibility.
///
/// # Semantics
///
/// - **`as_of_t`**: If `Some(t)`, search the newest snapshot with watermark <= t.
///   If `None`, search the latest available snapshot.
/// - **`sync`**: If `true`, wait for the latest snapshot head before searching.
///   If `false`, search whatever snapshot is already available (fast path).
/// - **`timeout_ms`**: Maximum time to wait for sync + search.
#[async_trait]
pub trait Bm25SearchProvider: std::fmt::Debug + Send + Sync {
    /// Execute a BM25 search and return results directly.
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - Graph source alias (e.g., "products-search:main")
    /// * `query_text` - The search query text
    /// * `limit` - Maximum number of hits to return
    /// * `as_of_t` - Target transaction time for time-travel queries (None = latest)
    /// * `sync` - Whether to sync to latest index head before searching
    /// * `timeout_ms` - Timeout for the entire operation
    async fn search_bm25(
        &self,
        graph_source_id: &str,
        query_text: &str,
        limit: usize,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> Result<Bm25SearchResult>;
}

/// BM25 search operator for `Pattern::IndexSearch`.
///
/// This operator supports two execution modes:
/// - **Search provider mode** (preferred): Uses `Bm25SearchProvider` to get search results
///   directly, supporting both embedded and remote backends.
/// - **Index provider mode** (legacy): Uses `Bm25IndexProvider` to load the raw index
///   and performs local scoring.
///
/// The operator checks for `bm25_search_provider` first; if not available, it falls back
/// to `bm25_provider` for backward compatibility.
pub struct Bm25SearchOperator {
    /// Child operator providing input solutions (may be EmptyOperator seed)
    child: BoxedOperator,
    /// Search pattern
    pattern: IndexSearchPattern,
    /// Output schema (child schema + any new vars from the search result)
    in_schema: Arc<[VarId]>,
    /// Mapping from variables to output column positions
    out_pos: HashMap<VarId, usize>,
    /// Cached BM25 index (loaded once in open) - used in legacy index provider mode
    index: Option<Arc<Bm25Index>>,
    /// Cached search results for constant targets (search provider mode only)
    cached_search_result: Option<Bm25SearchResult>,
    /// Whether we're using search provider mode (true) or index provider mode (false)
    use_search_provider: bool,
    /// Analyzer used for query analysis - only used in legacy mode
    analyzer: Analyzer,
    /// Datatypes for typed literal bindings
    datatypes: WellKnownDatatypes,
    /// State
    state: OperatorState,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl Bm25SearchOperator {
    pub fn new(child: BoxedOperator, pattern: IndexSearchPattern) -> Self {
        let child_schema = child.schema();

        // Build output schema: start with child vars, then add id/score/ledger vars if missing.
        let mut schema_vars: Vec<VarId> = child_schema.to_vec();
        let mut seen: HashSet<VarId> = schema_vars.iter().copied().collect();

        // id var is required
        if seen.insert(pattern.id_var) {
            schema_vars.push(pattern.id_var);
        }
        if let Some(v) = pattern.score_var {
            if seen.insert(v) {
                schema_vars.push(v);
            }
        }
        if let Some(v) = pattern.ledger_var {
            if seen.insert(v) {
                schema_vars.push(v);
            }
        }

        let schema: Arc<[VarId]> = Arc::from(schema_vars.into_boxed_slice());
        let out_pos: HashMap<VarId, usize> =
            schema.iter().enumerate().map(|(i, v)| (*v, i)).collect();

        Self {
            child,
            pattern,
            in_schema: schema,
            out_pos,
            index: None,
            cached_search_result: None,
            use_search_provider: false,
            analyzer: Analyzer::english_default(),
            datatypes: WellKnownDatatypes::new(),
            state: OperatorState::Created,
            out_schema: None,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    fn resolve_target_from_row(
        &self,
        ctx: &ExecutionContext<'_>,
        row: &crate::binding::RowView<'_>,
    ) -> Result<Option<String>> {
        match &self.pattern.target {
            IndexSearchTarget::Const(s) => Ok(Some(s.clone())),
            IndexSearchTarget::Var(v) => match row.get(*v) {
                None | Some(Binding::Unbound) => Ok(None),
                Some(Binding::Poisoned) => Ok(None),
                Some(Binding::Lit { val, .. }) => match val {
                    FlakeValue::String(s) => Ok(Some(s.clone())),
                    // Allow non-string literals to stringify (parity-ish; keeps things permissive)
                    other => Ok(Some(other.to_string())),
                },
                Some(Binding::EncodedLit {
                    o_kind,
                    o_key,
                    p_id,
                    dt_id,
                    lang_id,
                    ..
                }) => {
                    // Late materialization: decode only when BM25 needs the target string.
                    if let Some(gv) = ctx.graph_view() {
                        let val = gv
                            .decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
                            .map_err(|e| {
                                tracing::debug!(
                                    o_kind,
                                    o_key,
                                    p_id,
                                    dt_id,
                                    lang_id,
                                    error = %e,
                                    "BM25 failed to decode encoded literal target"
                                );
                                crate::error::QueryError::dictionary_lookup(format!(
                                    "BM25 target decode: o_kind={o_kind}, o_key={o_key}, p_id={p_id}, dt_id={dt_id}, lang_id={lang_id}: {e}"
                                ))
                            })?;
                        Ok(Some(val.to_string()))
                    } else {
                        Ok(None)
                    }
                }
                Some(Binding::Sid { sid, .. }) => {
                    // If user bound f:searchText to an IRI, treat its decoded IRI as the search string.
                    // (Not typical, but keeps query robust.)
                    Ok(ctx.decode_sid(sid))
                }
                Some(Binding::IriMatch { iri, .. }) => {
                    // IriMatch: use canonical IRI as search string
                    Ok(Some(iri.to_string()))
                }
                Some(Binding::Iri(iri)) => {
                    // Raw IRI from graph source - use as search string
                    Ok(Some(iri.to_string()))
                }
                Some(Binding::Grouped(_)) => Ok(None),
                // EncodedSid/EncodedPid: decode to IRI string if store available
                Some(Binding::EncodedSid { s_id, .. }) => {
                    // Novelty-aware: use graph_view() for subject resolution.
                    match ctx.resolve_subject_iri(*s_id) {
                        Some(Ok(iri)) => Ok(Some(iri)),
                        Some(Err(e)) => {
                            tracing::debug!(
                                s_id,
                                error = %e,
                                "BM25 failed to resolve encoded subject target"
                            );
                            Err(crate::error::QueryError::dictionary_lookup(format!(
                                "BM25 target subject lookup: s_id={s_id}: {e}"
                            )))
                        }
                        None => Ok(None),
                    }
                }
                Some(Binding::EncodedPid { p_id }) => {
                    if let Some(store) = ctx.binary_store.as_deref() {
                        match store.resolve_predicate_iri(*p_id) {
                            Some(iri) => Ok(Some(iri.to_string())),
                            None => Ok(None),
                        }
                    } else {
                        Ok(None)
                    }
                }
            },
        }
    }

    fn bind_or_check(existing: Option<&Binding>, candidate: &Binding) -> bool {
        match existing {
            None => true,
            Some(Binding::Unbound) => true,
            Some(Binding::Poisoned) => false,
            Some(other) => other == candidate,
        }
    }
}

#[async_trait]
impl Operator for Bm25SearchOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;

        // BM25 search works in both single-ledger and multi-ledger (dataset) contexts.
        // Unlike graph scanning operators, BM25 doesn't iterate active graphs - it consults
        // the BM25 index/search provider directly by graph source alias. Results are emitted as
        // IriMatch bindings with ledger provenance for correct cross-ledger joins.

        // Prefer bm25_search_provider (new unified path) over bm25_provider (legacy)
        if let Some(search_provider) = ctx.bm25_search_provider {
            self.use_search_provider = true;

            // IMPORTANT: In dataset mode, there is no meaningful dataset-level `to_t`.
            // Passing `None` avoids inventing a cross-ledger time and lets the provider
            // select the latest snapshot (or apply its own semantics).
            let as_of_t = if ctx.dataset.is_some() {
                None
            } else {
                Some(ctx.to_t)
            };

            // If target is constant, we can pre-fetch results in open()
            if let IndexSearchTarget::Const(query_text) = &self.pattern.target {
                let limit = self.pattern.limit.unwrap_or(usize::MAX);
                let result = search_provider
                    .search_bm25(
                        &self.pattern.graph_source_id,
                        query_text,
                        limit,
                        as_of_t,
                        self.pattern.sync,
                        self.pattern.timeout,
                    )
                    .await?;
                self.cached_search_result = Some(result);
            }
            // For variable targets, we'll call search_bm25 per row in next_batch
        } else if let Some(index_provider) = ctx.bm25_provider {
            // Legacy path: load index for local scoring
            self.use_search_provider = false;

            // IMPORTANT: In dataset mode, there is no meaningful dataset-level `to_t`.
            let as_of_t = if ctx.dataset.is_some() {
                None
            } else {
                Some(ctx.to_t)
            };

            let idx = index_provider
                .bm25_index(
                    &self.pattern.graph_source_id,
                    as_of_t,
                    self.pattern.sync,
                    self.pattern.timeout,
                )
                .await?;
            self.index = Some(idx);
        } else {
            return Err(QueryError::InvalidQuery(
                "BM25 IndexSearch requires ExecutionContext.bm25_search_provider or bm25_provider (not configured)"
                    .to_string(),
            ));
        }

        // If target is a variable, ensure it's available from the child schema.
        if let IndexSearchTarget::Var(v) = &self.pattern.target {
            if !self.child.schema().iter().any(|vv| vv == v) {
                return Err(QueryError::InvalidQuery(format!(
                    "IndexSearch target variable {v:?} is not bound by previous patterns"
                )));
            }
        }

        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // Pull one child batch; expand each row by BM25 results.
        let input_batch = match self.child.next_batch(ctx).await? {
            Some(b) => b,
            None => {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
        };

        if input_batch.is_empty() {
            return Ok(Some(Batch::empty(self.in_schema.clone())?));
        }

        // Output columns
        let num_cols = self.in_schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(input_batch.len()))
            .collect();

        let child_schema = self.child.schema();
        let child_cols: Vec<&[Binding]> = (0..child_schema.len())
            .map(|i| {
                input_batch
                    .column_by_idx(i)
                    .expect("child batch schema mismatch")
            })
            .collect();

        let limit = self.pattern.limit.unwrap_or(usize::MAX);

        #[allow(clippy::needless_range_loop)]
        for row_idx in 0..input_batch.len() {
            let row_view = input_batch.row_view(row_idx).unwrap();

            // Get search hits - either from cache, search provider, or local scoring
            let hits: Vec<(Arc<str>, Arc<str>, f64)> = if self.use_search_provider {
                // Search provider mode (preferred path)
                if let Some(ref cached) = self.cached_search_result {
                    // Use cached results for constant target
                    cached
                        .hits
                        .iter()
                        .map(|h| {
                            (
                                Arc::from(h.iri.as_str()),
                                Arc::from(h.ledger_alias.as_str()),
                                h.score,
                            )
                        })
                        .collect()
                } else {
                    // Variable target - resolve and call search provider
                    let Some(target) = self.resolve_target_from_row(ctx, &row_view)? else {
                        continue;
                    };
                    if target.trim().is_empty() {
                        continue;
                    }

                    let search_provider = ctx.bm25_search_provider.ok_or_else(|| {
                        QueryError::InvalidQuery("BM25 search provider not available".to_string())
                    })?;

                    // IMPORTANT: In dataset mode, there is no meaningful dataset-level `to_t`.
                    let as_of_t = if ctx.dataset.is_some() {
                        None
                    } else {
                        Some(ctx.to_t)
                    };

                    let result = search_provider
                        .search_bm25(
                            &self.pattern.graph_source_id,
                            &target,
                            limit,
                            as_of_t,
                            self.pattern.sync,
                            self.pattern.timeout,
                        )
                        .await?;

                    result
                        .hits
                        .into_iter()
                        .map(|h| {
                            (
                                Arc::from(h.iri.as_str()),
                                Arc::from(h.ledger_alias.as_str()),
                                h.score,
                            )
                        })
                        .collect()
                }
            } else {
                // Legacy index provider mode - local scoring
                let index = self
                    .index
                    .as_ref()
                    .ok_or_else(|| QueryError::InvalidQuery("BM25 index not loaded".to_string()))?;

                let Some(target) = self.resolve_target_from_row(ctx, &row_view)? else {
                    continue;
                };
                if target.trim().is_empty() {
                    continue;
                }

                // Analyze query terms and score locally
                let terms = self.analyzer.analyze_to_strings(&target);
                if terms.is_empty() {
                    continue;
                }
                let term_refs: Vec<&str> = terms.iter().map(std::string::String::as_str).collect();
                let scorer = Bm25Scorer::new(index, &term_refs);
                let results = scorer.top_k(limit);

                results
                    .into_iter()
                    .map(|(doc_key, score)| {
                        (
                            doc_key.subject_iri.clone(),
                            doc_key.ledger_alias.clone(),
                            score,
                        )
                    })
                    .collect()
            };

            if hits.is_empty() {
                continue;
            }

            // For each BM25 result row, merge with the child row.
            for (subject_iri, ledger_alias, score) in hits {
                // Create IriMatch binding for correct cross-ledger joins.
                // IMPORTANT: Encode SID using the hit's source ledger (not primary db)
                // so that primary_sid is consistent with ledger_alias.
                let id_binding = if let Some(sid) =
                    ctx.encode_iri_in_ledger(subject_iri.as_ref(), ledger_alias.as_ref())
                {
                    // Have a valid SID in the hit's source ledger - use IriMatch with full provenance
                    Binding::iri_match(subject_iri.clone(), sid, ledger_alias.clone())
                } else {
                    // Can't encode to SID (IRI not in hit ledger's namespace table) - use raw IRI
                    // This allows the result to participate in IRI-based comparisons
                    // even if it can't be looked up directly. Note: Binding::Iri won't constrain
                    // scans in join substitution (only Sid and IriMatch do), but that's correct
                    // since "cannot encode ⇒ cannot scan anyway".
                    Binding::Iri(subject_iri.clone())
                };

                let score_binding =
                    Binding::lit(FlakeValue::Double(score), self.datatypes.xsd_double.clone());
                let ledger_binding = Binding::lit(
                    FlakeValue::String(ledger_alias.to_string()),
                    self.datatypes.xsd_string.clone(),
                );

                // Compatibility checks for overlapping vars:
                let existing_id = row_view.get(self.pattern.id_var);
                if !Self::bind_or_check(existing_id, &id_binding) {
                    continue;
                }
                if let Some(v) = self.pattern.score_var {
                    let existing = row_view.get(v);
                    if !Self::bind_or_check(existing, &score_binding) {
                        continue;
                    }
                }
                if let Some(v) = self.pattern.ledger_var {
                    let existing = row_view.get(v);
                    if !Self::bind_or_check(existing, &ledger_binding) {
                        continue;
                    }
                }

                // Emit output row (columnar): start with child columns.
                for (col_idx, &var) in child_schema.iter().enumerate() {
                    let out_idx = *self
                        .out_pos
                        .get(&var)
                        .expect("output schema missing child var");
                    columns[out_idx].push(child_cols[col_idx][row_idx].clone());
                }

                // Add/override id/score/ledger vars (if not present in child, these are new cols)
                let id_pos = *self.out_pos.get(&self.pattern.id_var).unwrap();
                columns[id_pos].push(id_binding);

                if let Some(v) = self.pattern.score_var {
                    let pos = *self.out_pos.get(&v).unwrap();
                    columns[pos].push(score_binding);
                }
                if let Some(v) = self.pattern.ledger_var {
                    let pos = *self.out_pos.get(&v).unwrap();
                    columns[pos].push(ledger_binding);
                }
            }
        }

        if columns.first().map(std::vec::Vec::is_empty).unwrap_or(true) {
            return Ok(Some(Batch::empty(self.in_schema.clone())?));
        }

        let batch = Batch::new(self.in_schema.clone(), columns)?;
        Ok(trim_batch(&self.out_schema, batch))
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Conservative: child rows * limit (if set)
        let child = self.child.estimated_rows();
        let lim = self.pattern.limit;
        match (child, lim) {
            (Some(c), Some(l)) => Some(c.saturating_mul(l)),
            (Some(c), None) => Some(c),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bm25::DocKey;
    use crate::execute::build_where_operators_seeded;
    use crate::ir::{IndexSearchTarget, Pattern};
    use crate::seed::EmptyOperator;
    use crate::var_registry::VarRegistry;
    use fluree_db_core::LedgerSnapshot;

    #[derive(Debug, Default)]
    struct TestProvider {
        map: std::collections::HashMap<String, Arc<Bm25Index>>,
    }

    #[async_trait]
    impl Bm25IndexProvider for TestProvider {
        async fn bm25_index(
            &self,
            graph_source_id: &str,
            _as_of_t: Option<i64>,
            _sync: bool,
            _timeout_ms: Option<u64>,
        ) -> Result<Arc<Bm25Index>> {
            self.map.get(graph_source_id).cloned().ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "No BM25 index for graph source alias {graph_source_id}"
                ))
            })
        }
    }

    fn make_test_snapshot() -> LedgerSnapshot {
        let mut snapshot = LedgerSnapshot::genesis("test/main");
        // Ensure example IRIs used by BM25 tests are encodable to SIDs.
        snapshot
            .insert_namespace_code(100, "http://example.org/".to_string())
            .unwrap();
        snapshot
    }

    #[tokio::test]
    async fn test_bm25_operator_seeded_const_target() {
        let snapshot = make_test_snapshot();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");
        let score = vars.get_or_insert("?score");

        let mut idx = Bm25Index::new();
        idx.upsert_document(
            DocKey::new("ledger:main", "http://example.org/doc1"),
            [("hello", 1), ("world", 1)].into_iter().collect(),
        );
        idx.upsert_document(
            DocKey::new("ledger:main", "http://example.org/doc2"),
            [("hello", 1), ("rust", 2)].into_iter().collect(),
        );
        let idx = Arc::new(idx);

        let mut provider = TestProvider::default();
        provider.map.insert("search:main".to_string(), idx);

        let isp = IndexSearchPattern::new(
            "search:main",
            IndexSearchTarget::Const("rust".to_string()),
            id,
        )
        .with_score_var(score)
        .with_limit(10);

        let patterns = vec![Pattern::IndexSearch(isp)];

        // Build operator with explicit seed (EmptyOperator) to mimic runner behavior.
        let empty = EmptyOperator::new();
        let seed: BoxedOperator = Box::new(empty);
        let mut op = build_where_operators_seeded(
            Some(seed),
            &patterns,
            None,
            None,
            &crate::temporal_mode::PlanningContext::current(),
        )
        .expect("build operators");

        let mut ctx = ExecutionContext::new(&snapshot, &vars);
        ctx.bm25_provider = Some(&provider);

        op.open(&ctx).await.unwrap();
        let batch = op.next_batch(&ctx).await.unwrap().unwrap();
        assert!(!batch.is_empty());

        // Should include id and score vars in schema.
        assert!(batch.schema().contains(&id));
        assert!(batch.schema().contains(&score));
    }

    #[tokio::test]
    async fn test_bm25_operator_dedup_terms_and_blank_target() {
        let snapshot = make_test_snapshot();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");

        let mut idx = Bm25Index::new();
        idx.upsert_document(
            DocKey::new("ledger:main", "http://example.org/doc1"),
            [("rust", 1)].into_iter().collect(),
        );
        let idx = Arc::new(idx);

        let mut provider = TestProvider::default();
        provider.map.insert("search:main".to_string(), idx);

        // Blank target should return empty batch.
        let isp = IndexSearchPattern::new(
            "search:main",
            IndexSearchTarget::Const("   ".to_string()),
            id,
        );
        let patterns = vec![Pattern::IndexSearch(isp)];

        let empty = EmptyOperator::new();
        let seed: BoxedOperator = Box::new(empty);
        let mut op = build_where_operators_seeded(
            Some(seed),
            &patterns,
            None,
            None,
            &crate::temporal_mode::PlanningContext::current(),
        )
        .expect("build operators");

        let mut ctx = ExecutionContext::new(&snapshot, &vars);
        ctx.bm25_provider = Some(&provider);

        op.open(&ctx).await.unwrap();
        let batch = op.next_batch(&ctx).await.unwrap().unwrap();
        assert!(batch.is_empty());
    }
}
