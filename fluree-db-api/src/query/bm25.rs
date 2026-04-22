use serde_json::Value as JsonValue;

use crate::query::helpers::{parse_dataset_spec, tracker_for_limits};
use crate::{
    ApiError, DataSetDb, ExecutableQuery, Fluree, FlureeIndexProvider, QueryResult, Result,
    VarRegistry,
};

use fluree_db_query::parse::parse_query;

impl Fluree {
    /// Execute a query against a loaded dataset with BM25 and vector index provider support.
    ///
    /// This enables both `f:searchText` (BM25) and `f:queryVector` (similarity search) patterns
    /// in queries against graph sources.
    pub async fn query_dataset_with_bm25(
        &self,
        dataset: &DataSetDb,
        query_json: &JsonValue,
    ) -> Result<QueryResult> {
        // Get the primary graph for parsing/encoding
        let primary = dataset
            .primary()
            .ok_or_else(|| ApiError::query("Dataset has no graphs for query execution"))?;
        // Parse the query using the primary ledger's DB for IRI encoding
        let mut vars = VarRegistry::new();
        let parsed = parse_query(query_json, primary.snapshot.as_ref(), &mut vars, None)?;

        // Build the runtime dataset
        let runtime_dataset = dataset.as_runtime_dataset();

        // Build executable query
        let executable = ExecutableQuery::simple(parsed.clone());

        // Create index provider for graph source support (implements both BM25 and Vector)
        let provider = FlureeIndexProvider::new(self);

        // Execute with dataset and BM25 provider.
        //
        // Vector provider support is feature-gated. When disabled,
        // f:queryVector patterns are not available and we run the BM25-only path.
        let tracker = tracker_for_limits(query_json);
        let db = primary.as_graph_db_ref();
        let tracker_ref = if tracker.is_enabled() {
            Some(&tracker)
        } else {
            None
        };
        let batches = {
            #[cfg(feature = "vector")]
            {
                crate::execute_with_dataset_and_providers(
                    db,
                    &vars,
                    &executable,
                    &runtime_dataset,
                    &provider,
                    &provider,
                    tracker_ref,
                )
                .await?
            }
            #[cfg(not(feature = "vector"))]
            {
                crate::execute_with_dataset_and_bm25(
                    db,
                    &vars,
                    &executable,
                    &runtime_dataset,
                    &provider,
                    tracker_ref,
                )
                .await?
            }
        };

        // Dataset graph crawl formatting may need to see flakes from multiple ledgers (union),
        // and each ledger may have a different `t`. We therefore:
        // - use a composite overlay (union of novelty overlays)
        // - omit result `t` unless the dataset resolves to one meaningful ledger/time
        let novelty = dataset.composite_overlay();

        Ok(super::helpers::build_query_result(
            vars,
            parsed,
            batches,
            dataset.result_t(),
            novelty,
            None,
        ))
    }

    /// Execute a connection query with index provider support (BM25 + Vector).
    ///
    /// This method enables both `f:searchText` (BM25 full-text search) and `f:queryVector`
    /// (similarity search) patterns in queries. Despite the name, it supports all
    /// graph source index types.
    ///
    /// For queries that don't use graph source patterns, prefer `query_connection()`
    /// as it may take faster code paths for simple single-ledger queries.
    pub async fn query_connection_with_bm25(&self, query_json: &JsonValue) -> Result<QueryResult> {
        let (spec, qc_opts) = parse_dataset_spec(query_json)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing ledger specification in connection query",
            ));
        }

        // NOTE: Unlike query_connection(), we do NOT take the single-ledger fast path here.
        // BM25 queries with f:searchText patterns require the full dataset execution path with
        // FlureeIndexProvider wired into the execution context. Always use dataset path.

        let dataset = if qc_opts.has_any_policy_inputs() {
            self.build_dataset_view_with_policy(&spec, &qc_opts).await?
        } else {
            self.build_dataset_view(&spec).await?
        };

        self.query_dataset_with_bm25(&dataset, query_json).await
    }
}
