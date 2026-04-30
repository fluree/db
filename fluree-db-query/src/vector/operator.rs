//! Vector Search Operator (Pattern::VectorSearch)
//!
//! This operator executes vector similarity search against a vector index provider
//! and emits bindings for:
//! - f:resultId      -> `Binding::IriMatch` (canonical IRI with ledger provenance for cross-ledger joins)
//!   or `Binding::Iri` (if IRI cannot be encoded to SID)
//! - f:resultScore   -> `Binding::Lit` (xsd:double, similarity score)
//! - f:resultLedger  -> `Binding::Lit` (xsd:string; ledger alias) [optional]
//!
//! # Provider Abstraction
//!
//! The `VectorIndexProvider` trait abstracts the vector search backend.
//! Implementations can connect to:
//! - Embedded in-process indexes (requires `vector` feature)
//! - External services (future)
//!
//! # Example Query
//!
//! ```json
//! {
//!   "where": [{
//!     "f:graphSource": "embeddings:main",
//!     "f:queryVector": [0.1, 0.2, 0.3],
//!     "f:distanceMetric": "cosine",
//!     "f:searchLimit": 10,
//!     "f:searchResult": {"f:resultId": "?doc", "f:resultScore": "?score"}
//!   }],
//!   "select": ["?doc", "?score"]
//! }
//! ```

use crate::binding::{Batch, Binding, RowAccess};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::ir::{VectorSearchPattern, VectorSearchTarget};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::FlakeValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::VectorSearchParams;

/// A single hit from vector search
#[derive(Debug, Clone)]
pub struct VectorSearchHit {
    /// Document IRI
    pub iri: Arc<str>,
    /// Source ledger alias
    pub ledger_alias: Arc<str>,
    /// Similarity score (interpretation depends on metric)
    pub score: f64,
}

impl VectorSearchHit {
    /// Create a new search hit
    pub fn new(iri: impl Into<Arc<str>>, ledger_alias: impl Into<Arc<str>>, score: f64) -> Self {
        Self {
            iri: iri.into(),
            ledger_alias: ledger_alias.into(),
            score,
        }
    }
}

/// Provider for vector similarity search.
///
/// This trait abstracts the vector search backend, allowing different
/// implementations for embedded indexes or external services.
///
/// # Implementors
///
/// - `MockVectorProvider` - for testing
#[async_trait]
pub trait VectorIndexProvider: std::fmt::Debug + Send + Sync {
    /// Search for similar vectors.
    ///
    /// # Arguments
    ///
    /// * `graph_source_id` - Graph source alias (e.g., "embeddings:main")
    /// * `params` - Search parameters (query vector, metric, limit, etc.)
    ///
    /// # Returns
    ///
    /// Vector of search hits, ordered by similarity (best first).
    async fn search(
        &self,
        graph_source_id: &str,
        params: VectorSearchParams<'_>,
    ) -> Result<Vec<VectorSearchHit>>;

    /// Check if a collection exists for the given graph source alias
    async fn collection_exists(&self, graph_source_id: &str) -> Result<bool>;
}

/// Vector search operator for `Pattern::VectorSearch`.
pub struct VectorSearchOperator {
    /// Child operator providing input solutions (may be EmptyOperator seed)
    child: BoxedOperator,
    /// Search pattern
    pattern: VectorSearchPattern,
    /// Output schema (child schema + any new vars from the search result)
    in_schema: Arc<[VarId]>,
    /// Mapping from variables to output column positions
    out_pos: HashMap<VarId, usize>,
    /// Datatypes for typed literal bindings
    datatypes: WellKnownDatatypes,
    /// State
    state: OperatorState,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl VectorSearchOperator {
    pub fn new(child: BoxedOperator, pattern: VectorSearchPattern) -> Self {
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

    /// Resolve the query vector from the pattern (constant or variable)
    fn resolve_vector_from_row(
        &self,
        _ctx: &ExecutionContext<'_>,
        row: &crate::binding::RowView<'_>,
    ) -> Result<Option<Vec<f32>>> {
        match &self.pattern.target {
            VectorSearchTarget::Const(vec) => Ok(Some(vec.clone())),
            VectorSearchTarget::Var(v) => match row.get(*v) {
                None | Some(Binding::Unbound) => Ok(None),
                Some(Binding::Poisoned) => Ok(None),
                Some(Binding::Lit { val, .. }) => match val {
                    FlakeValue::Vector(v) => {
                        // Convert f64 to f32
                        Ok(Some(v.iter().map(|x| *x as f32).collect()))
                    }
                    _ => Ok(None),
                },
                Some(Binding::EncodedLit { .. }) => Ok(None),
                Some(
                    Binding::Sid { .. }
                    | Binding::IriMatch { .. }
                    | Binding::Iri(_)
                    | Binding::Grouped(_)
                    | Binding::EncodedSid { .. }
                    | Binding::EncodedPid { .. },
                ) => Ok(None),
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
impl Operator for VectorSearchOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;

        let _provider = ctx.vector_provider.ok_or_else(|| {
            QueryError::InvalidQuery(
                "VectorSearch requires ExecutionContext.vector_provider (not configured)"
                    .to_string(),
            )
        })?;

        // If target is a variable, ensure it's available from the child schema.
        if let VectorSearchTarget::Var(v) = &self.pattern.target {
            if !self.child.schema().iter().any(|vv| vv == v) {
                return Err(QueryError::InvalidQuery(format!(
                    "VectorSearch target variable {v:?} is not bound by previous patterns"
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

        let provider = ctx.vector_provider.ok_or_else(|| {
            QueryError::InvalidQuery("Vector provider not configured".to_string())
        })?;

        // Pull one child batch; expand each row by vector search results.
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

        let limit = self.pattern.limit.unwrap_or(10);

        #[allow(clippy::needless_range_loop)]
        for row_idx in 0..input_batch.len() {
            let row_view = input_batch.row_view(row_idx).unwrap();
            let Some(query_vector) = self.resolve_vector_from_row(ctx, &row_view)? else {
                continue;
            };

            // Empty vectors produce no results
            if query_vector.is_empty() {
                continue;
            }

            // Execute vector search
            let params = VectorSearchParams::new(&query_vector, self.pattern.metric, limit)
                .with_as_of_t(if ctx.dataset.is_some() {
                    None
                } else {
                    Some(ctx.to_t)
                })
                .with_sync(self.pattern.sync)
                .with_timeout_ms(self.pattern.timeout);

            let results = provider
                .search(&self.pattern.graph_source_id, params)
                .await?;

            // For each search result, merge with the child row.
            for hit in results {
                // Create IriMatch binding for correct cross-ledger joins.
                // The hit already contains the canonical IRI and ledger alias.
                // IMPORTANT: Encode SID using the hit's source ledger (not primary db)
                // so that primary_sid is consistent with ledger_alias.
                let id_binding = if let Some(sid) =
                    ctx.encode_iri_in_ledger(hit.iri.as_ref(), hit.ledger_alias.as_ref())
                {
                    // Have a valid SID in the hit's source ledger - use IriMatch with full provenance
                    Binding::iri_match(hit.iri.clone(), sid, hit.ledger_alias.clone())
                } else {
                    // Can't encode to SID (IRI not in hit ledger's namespace table) - use raw IRI
                    // This allows the result to participate in IRI-based comparisons
                    // even if it can't be looked up directly. Note: Binding::Iri won't constrain
                    // scans in join substitution (only Sid and IriMatch do), but that's correct
                    // since "cannot encode ⇒ cannot scan anyway".
                    Binding::Iri(hit.iri.clone())
                };

                let score_binding = Binding::lit(
                    FlakeValue::Double(hit.score),
                    self.datatypes.xsd_double.clone(),
                );
                let ledger_binding = Binding::lit(
                    FlakeValue::String(hit.ledger_alias.to_string()),
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

                // Add/override id/score/ledger vars
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
    use crate::execute::build_where_operators_seeded;
    use crate::ir::{Pattern, VectorSearchTarget};
    use crate::seed::EmptyOperator;
    use crate::var_registry::VarRegistry;
    use crate::vector::DistanceMetric;
    use fluree_db_core::LedgerSnapshot;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct MockVectorProvider {
        results: Vec<VectorSearchHit>,
        /// Track calls to verify search was invoked correctly
        search_calls: Mutex<Vec<SearchCall>>,
    }

    #[derive(Debug, Clone)]
    struct SearchCall {
        graph_source_id: String,
        query_vector: Vec<f32>,
        metric: DistanceMetric,
        limit: usize,
    }

    impl Default for MockVectorProvider {
        fn default() -> Self {
            Self {
                results: Vec::new(),
                search_calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl MockVectorProvider {
        fn with_results(results: Vec<VectorSearchHit>) -> Self {
            Self {
                results,
                search_calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl VectorIndexProvider for MockVectorProvider {
        async fn search(
            &self,
            graph_source_id: &str,
            params: VectorSearchParams<'_>,
        ) -> Result<Vec<VectorSearchHit>> {
            // Record the call
            self.search_calls.lock().unwrap().push(SearchCall {
                graph_source_id: graph_source_id.to_string(),
                query_vector: params.query_vector.to_vec(),
                metric: params.metric,
                limit: params.limit,
            });
            Ok(self.results.iter().take(params.limit).cloned().collect())
        }

        async fn collection_exists(&self, _graph_source_id: &str) -> Result<bool> {
            Ok(true)
        }
    }

    fn make_test_snapshot() -> LedgerSnapshot {
        let mut snapshot = LedgerSnapshot::genesis("test/main");
        // Ensure example IRIs used by tests are encodable to SIDs.
        snapshot
            .insert_namespace_code(100, "http://example.org/".to_string())
            .unwrap();
        snapshot
    }

    #[test]
    fn test_vector_search_hit() {
        let hit = VectorSearchHit::new("http://example.org/doc1", "ledger:main", 0.95);
        assert_eq!(hit.iri.as_ref(), "http://example.org/doc1");
        assert_eq!(hit.ledger_alias.as_ref(), "ledger:main");
        assert!((hit.score - 0.95).abs() < 0.001);
    }

    #[test]
    fn test_distance_metric_default() {
        assert_eq!(DistanceMetric::default(), DistanceMetric::Cosine);
    }

    #[tokio::test]
    async fn test_vector_operator_constant_target() {
        let snapshot = make_test_snapshot();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");
        let score = vars.get_or_insert("?score");

        // Create mock provider with some results
        let provider = MockVectorProvider::with_results(vec![
            VectorSearchHit::new("http://example.org/doc1", "ledger:main", 0.95),
            VectorSearchHit::new("http://example.org/doc2", "ledger:main", 0.85),
        ]);

        // Create vector search pattern with constant vector
        let vsp = VectorSearchPattern::new(
            "embeddings:main",
            VectorSearchTarget::Const(vec![0.1, 0.2, 0.3]),
            id,
        )
        .with_metric(DistanceMetric::Cosine)
        .with_score_var(score)
        .with_limit(10);

        let patterns = vec![Pattern::VectorSearch(vsp)];

        // Build operator with explicit seed
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
        ctx.vector_provider = Some(&provider);

        op.open(&ctx).await.unwrap();
        let batch = op.next_batch(&ctx).await.unwrap().unwrap();

        // Should have results
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 2);

        // Schema should include id and score
        assert!(batch.schema().contains(&id));
        assert!(batch.schema().contains(&score));

        // Verify search was called correctly
        let calls = provider.search_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].graph_source_id, "embeddings:main");
        assert_eq!(calls[0].query_vector, vec![0.1, 0.2, 0.3]);
        assert_eq!(calls[0].metric, DistanceMetric::Cosine);
        assert_eq!(calls[0].limit, 10);
    }

    #[tokio::test]
    async fn test_vector_operator_empty_results() {
        let snapshot = make_test_snapshot();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");

        // Empty provider
        let provider = MockVectorProvider::default();

        let vsp = VectorSearchPattern::new(
            "embeddings:main",
            VectorSearchTarget::Const(vec![0.1, 0.2]),
            id,
        );

        let patterns = vec![Pattern::VectorSearch(vsp)];

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
        ctx.vector_provider = Some(&provider);

        op.open(&ctx).await.unwrap();
        let batch = op.next_batch(&ctx).await.unwrap().unwrap();

        // Should be empty
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn test_vector_operator_respects_limit() {
        let snapshot = make_test_snapshot();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");

        // Provider with many results
        let provider = MockVectorProvider::with_results(vec![
            VectorSearchHit::new("http://example.org/doc1", "ledger:main", 0.95),
            VectorSearchHit::new("http://example.org/doc2", "ledger:main", 0.90),
            VectorSearchHit::new("http://example.org/doc3", "ledger:main", 0.85),
            VectorSearchHit::new("http://example.org/doc4", "ledger:main", 0.80),
            VectorSearchHit::new("http://example.org/doc5", "ledger:main", 0.75),
        ]);

        // Limit to 2 results
        let vsp = VectorSearchPattern::new(
            "embeddings:main",
            VectorSearchTarget::Const(vec![0.5, 0.5]),
            id,
        )
        .with_limit(2);

        let patterns = vec![Pattern::VectorSearch(vsp)];

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
        ctx.vector_provider = Some(&provider);

        op.open(&ctx).await.unwrap();
        let batch = op.next_batch(&ctx).await.unwrap().unwrap();

        // Should only have 2 results
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn test_vector_operator_missing_provider_error() {
        let snapshot = make_test_snapshot();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");

        let vsp = VectorSearchPattern::new(
            "embeddings:main",
            VectorSearchTarget::Const(vec![0.1, 0.2]),
            id,
        );

        let patterns = vec![Pattern::VectorSearch(vsp)];

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

        // No vector_provider set
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let err = op.open(&ctx).await.unwrap_err();
        assert!(err.to_string().contains("vector_provider"));
    }

    #[tokio::test]
    async fn test_vector_operator_with_ledger_var() {
        let snapshot = make_test_snapshot();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");
        let ledger = vars.get_or_insert("?source");

        let provider = MockVectorProvider::with_results(vec![VectorSearchHit::new(
            "http://example.org/doc1",
            "docs:main",
            0.9,
        )]);

        let vsp =
            VectorSearchPattern::new("embeddings:main", VectorSearchTarget::Const(vec![0.5]), id)
                .with_ledger_var(ledger);

        let patterns = vec![Pattern::VectorSearch(vsp)];

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
        ctx.vector_provider = Some(&provider);

        op.open(&ctx).await.unwrap();
        let batch = op.next_batch(&ctx).await.unwrap().unwrap();

        // Should have ledger var in schema
        assert!(batch.schema().contains(&ledger));
    }
}
