//! Builder for DataSetDb from DatasetSpec
//!
//! Provides utilities to construct `DataSetDb` from query dataset
//! specifications, applying time travel, policy, and reasoning wrappers.

use crate::view::{DataSetDb, GraphDb};
use crate::{dataset, time_resolve, ApiError, DatasetSpec, Fluree, QueryConnectionOptions, Result};
use chrono::DateTime;

macro_rules! build_dataset_view_from_spec {
    (
        $self:expr,
        $spec:expr,
        history_transform = $history_transform:expr,
        load_view = $load_view:expr,
        apply_policy = $apply_policy:expr $(,)?
    ) => {{
        let spec = $spec;

        // History/changes queries are a Fluree dataset extension.
        // In this mode, the "from" array specifies a (from,to) range on ONE ledger,
        // not two distinct default graphs.
        if let Some(range) = spec.history_range() {
            let ledger = $self.ledger(&range.identifier).await?;
            let latest_t = ledger.t();

            let from_t = resolve_history_endpoint_t(&ledger, &range.from, latest_t).await?;
            let to_t = resolve_history_endpoint_t(&ledger, &range.to, latest_t).await?;

            let view = GraphDb::from_ledger_state(&ledger);
            let view = ($history_transform)(view).await?;
            Ok(DataSetDb::single(view).with_history_range(from_t, to_t))
        } else {
            let mut dataset_db = DataSetDb::new();

            // Load default graphs, applying per-source policy and config reasoning
            for source in &spec.default_graphs {
                let view = ($load_view)(source).await?;
                let view = ($apply_policy)(view, source).await?;
                let view = $self.apply_config_defaults(view, None);
                // If this is a graph source, also register as a named graph
                // so GRAPH <gs_id> patterns can resolve it during execution.
                if let Some(ref gs_id) = view.graph_source_id {
                    dataset_db = dataset_db.with_named(gs_id.as_ref(), view.clone());
                }
                dataset_db = dataset_db.with_default(view);
            }

            // Load named graphs, applying per-source policy and config reasoning
            for source in &spec.named_graphs {
                let view = ($load_view)(source).await?;
                let view = ($apply_policy)(view, source).await?;
                let view = $self.apply_config_defaults(view, None);
                // Add by identifier (primary key)
                dataset_db = dataset_db.with_named(source.identifier.as_str(), view.clone());
                // Also add by alias if present (enables ["graph", "<alias>", ...] lookup)
                if let Some(alias) = &source.source_alias {
                    dataset_db = dataset_db.with_named(alias.as_str(), view);
                }
            }

            Ok(dataset_db)
        }
    }};
}

macro_rules! try_single_view_from_spec {
    (
        $spec:expr,
        load_view = $load_view:expr $(,)?
    ) => {{
        let spec = $spec;
        // Single default graph, no named graphs, no history range = single-ledger
        if spec.default_graphs.len() == 1
            && spec.named_graphs.is_empty()
            && spec.history_range.is_none()
        {
            let source = &spec.default_graphs[0];
            let view = ($load_view)(source).await?;
            Ok(Some(view))
        } else {
            Ok(None)
        }
    }};
}

// ============================================================================
// Dataset View Builder
// ============================================================================

impl Fluree {
    /// Build a `DataSetDb` from a `DatasetSpec`.
    ///
    /// This loads views for all graphs in the spec, applying time travel
    /// specifications and per-source policy overrides where present.
    ///
    /// # Per-Source Policy
    ///
    /// If a `GraphSource` has a `policy_override` set, that policy is applied
    /// to that source's view. This enables fine-grained access control where
    /// different graphs in the same query can have different policies.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (spec, opts) = DatasetSpec::from_query_json(&query)?;
    /// let dataset = fluree.build_dataset_view(&spec).await?;
    /// let result = fluree.query_dataset(&dataset, &query).await?;
    /// ```
    pub async fn build_dataset_view(&self, spec: &DatasetSpec) -> Result<DataSetDb> {
        build_dataset_view_from_spec!(
            self,
            spec,
            history_transform = |view| async { Ok::<GraphDb, ApiError>(view) },
            load_view = |source| self.load_view_from_source(source),
            apply_policy = |view, source| self.maybe_apply_source_policy(view, source),
        )
    }

    /// Build a `DataSetDb` with policy applied to all views.
    ///
    /// Policy is built from `QueryConnectionOptions` and applied uniformly
    /// to all views in the dataset, unless a source has a per-source policy
    /// override which takes precedence.
    ///
    /// # Policy Precedence
    ///
    /// Per-source `policy_override` takes precedence over global `opts`:
    /// - If source has `policy_override` with any fields set → use per-source policy
    /// - Otherwise → use global `opts` policy
    pub async fn build_dataset_view_with_policy(
        &self,
        spec: &DatasetSpec,
        opts: &QueryConnectionOptions,
    ) -> Result<DataSetDb> {
        build_dataset_view_from_spec!(
            self,
            spec,
            history_transform = |view| async {
                let view = self.wrap_policy(view, opts, None).await?;
                Ok::<GraphDb, ApiError>(self.apply_config_defaults(view, None))
            },
            load_view = |source| self.load_view_from_source(source),
            apply_policy = |view, source| self.apply_policy_with_override(view, source, opts),
        )
    }

    /// Apply per-source policy if present, otherwise no policy.
    ///
    /// This is used by `build_dataset_view` when no global policy is provided.
    async fn maybe_apply_source_policy(
        &self,
        view: GraphDb,
        source: &dataset::GraphSource,
    ) -> Result<GraphDb> {
        if let Some(policy_override) = &source.policy_override {
            if policy_override.has_policy() {
                let opts = policy_override.to_query_connection_options();
                return self.wrap_policy(view, &opts, None).await;
            }
        }
        Ok(view)
    }

    /// Apply policy with per-source override taking precedence over global.
    ///
    /// This is used by `build_dataset_view_with_policy` to allow per-source
    /// policy to override the global policy from `QueryConnectionOptions`.
    async fn apply_policy_with_override(
        &self,
        view: GraphDb,
        source: &dataset::GraphSource,
        global_opts: &QueryConnectionOptions,
    ) -> Result<GraphDb> {
        // Per-source policy override takes precedence
        if let Some(policy_override) = &source.policy_override {
            if policy_override.has_policy() {
                let opts = policy_override.to_query_connection_options();
                return self.wrap_policy(view, &opts, None).await;
            }
        }
        // Fall back to global policy
        self.wrap_policy(view, global_opts, None).await
    }

    /// Build a single `GraphDb` from a `GraphSource`.
    ///
    /// Tries to resolve as a ledger first. If not found, checks if the
    /// identifier is a graph source (Iceberg/R2RML) and creates a minimal
    /// genesis context tagged with the graph source ID.
    ///
    /// For sources with a time spec, time travel on graph sources is
    /// explicitly rejected with a clear error.
    ///
    /// If `graph_selector` is set, it is applied after resolution
    /// (the parser rejects the ambiguous case where both fragment and
    /// graph_selector are present).
    pub(crate) async fn load_view_from_source(
        &self,
        source: &dataset::GraphSource,
    ) -> Result<GraphDb> {
        let view = match &source.time_spec {
            None => {
                let result = self.db(&source.identifier).await;
                match result {
                    Ok(v) => v,
                    Err(ref e) if e.is_not_found() => {
                        self.resolve_as_graph_source(&source.identifier).await?
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Some(time_spec) => {
                let ts = convert_time_spec(time_spec)?;
                match self.db_at(&source.identifier, ts).await {
                    Ok(v) => v,
                    Err(ref e) if e.is_not_found() => {
                        // Check if it's a graph source — reject time travel explicitly
                        let gs_id = fluree_db_core::normalize_ledger_id(&source.identifier)
                            .unwrap_or_else(|_| source.identifier.clone());

                        if self
                            .nameservice()
                            .lookup_graph_source(&gs_id)
                            .await
                            .map_err(|e| ApiError::internal(e.to_string()))?
                            .is_some()
                        {
                            return Err(ApiError::query(
                                "Time travel is not supported for graph sources. \
                                 Remove the time specification to query at latest.",
                            ));
                        }
                        return Err(ApiError::NotFound(source.identifier.clone()));
                    }
                    Err(e) => return Err(e),
                }
            }
        };

        // Apply explicit graph selector if set.
        // Note: If the identifier contained a fragment like #txn-meta, that was
        // already applied by view()/view_at(). The parser rejects the ambiguous
        // case where both fragment and graph_selector are present.
        //
        // After re-selecting the graph, re-resolve config for the new graph
        // target so per-graph overrides match the actual graph being queried.
        match &source.graph_selector {
            Some(selector) => {
                let view = Self::apply_graph_selector(view, selector)?;
                self.resolve_and_attach_config(view).await
            }
            None => Ok(view),
        }
    }

    /// Check if a DatasetSpec represents a single-ledger query.
    ///
    /// Returns the single view if it's a single-ledger fast-path candidate.
    pub async fn try_single_view_from_spec(&self, spec: &DatasetSpec) -> Result<Option<GraphDb>> {
        try_single_view_from_spec!(
            spec,
            load_view = |source| self.load_view_from_source(source),
        )
    }

    /// Check if spec qualifies for single-ledger fast path (no time override).
    ///
    /// This is used to decide whether to take the optimized single-ledger path
    /// in query_connection.
    pub fn is_single_ledger_fast_path(spec: &DatasetSpec) -> bool {
        spec.default_graphs.len() == 1
            && spec.named_graphs.is_empty()
            && spec.default_graphs[0].time_spec.is_none()
    }

    /// Resolve an identifier as a graph source, creating a minimal genesis context.
    async fn resolve_as_graph_source(&self, identifier: &str) -> Result<GraphDb> {
        let gs_id = fluree_db_core::normalize_ledger_id(identifier)
            .unwrap_or_else(|_| identifier.to_string());
        let record = self
            .nameservice()
            .lookup_graph_source(&gs_id)
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        if record.is_none() {
            return Err(ApiError::NotFound(identifier.to_string()));
        }

        let snapshot = fluree_db_core::LedgerSnapshot::genesis(&gs_id);
        let state =
            fluree_db_ledger::LedgerState::new(snapshot, fluree_db_novelty::Novelty::new(0));
        let mut db = GraphDb::from_ledger_state(&state);
        db.graph_source_id = Some(gs_id.into());
        Ok(db)
    }
}

async fn resolve_history_endpoint_t(
    ledger: &fluree_db_ledger::LedgerState,
    spec: &dataset::TimeSpec,
    latest_t: i64,
) -> Result<i64> {
    match spec {
        dataset::TimeSpec::AtT(t) => Ok(*t),
        dataset::TimeSpec::Latest => Ok(latest_t),
        dataset::TimeSpec::AtTime(iso) => {
            let dt = DateTime::parse_from_rfc3339(iso).map_err(|e| {
                ApiError::internal(format!(
                    "Invalid ISO-8601 timestamp for time travel: {iso} ({e})"
                ))
            })?;
            // See `Fluree::load_view_at` for rationale: `ledger#time` is epoch-ms and we
            // ceiling sub-ms ISO inputs to avoid truncation off-by-one.
            let mut target_epoch_ms = dt.timestamp_millis();
            if dt.timestamp_subsec_nanos() % 1_000_000 != 0 {
                target_epoch_ms += 1;
            }

            time_resolve::datetime_to_t(
                &ledger.snapshot,
                Some(ledger.novelty.as_ref()),
                target_epoch_ms,
                latest_t,
            )
            .await
        }
        dataset::TimeSpec::AtCommit(commit_prefix) => {
            time_resolve::commit_to_t(
                &ledger.snapshot,
                Some(ledger.novelty.as_ref()),
                commit_prefix,
                latest_t,
            )
            .await
        }
    }
}

/// Convert dataset::TimeSpec to crate::TimeSpec
fn convert_time_spec(ts: &dataset::TimeSpec) -> Result<crate::TimeSpec> {
    match ts {
        dataset::TimeSpec::AtT(t) => Ok(crate::TimeSpec::AtT(*t)),
        dataset::TimeSpec::AtTime(iso) => Ok(crate::TimeSpec::AtTime(iso.clone())),
        dataset::TimeSpec::AtCommit(sha) => Ok(crate::TimeSpec::AtCommit(sha.clone())),
        dataset::TimeSpec::Latest => Ok(crate::TimeSpec::Latest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::GraphSource;
    use crate::FlureeBuilder;

    #[tokio::test]
    async fn test_build_dataset_view_single() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let spec = DatasetSpec::new().with_default(GraphSource::new("testdb:main"));
        let dataset = fluree.build_dataset_view(&spec).await.unwrap();

        assert!(dataset.is_single_ledger());
        assert!(dataset.primary().is_some());
    }

    #[tokio::test]
    async fn test_build_dataset_view_multiple() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger1 = fluree.create_ledger("db1").await.unwrap();
        let _ledger2 = fluree.create_ledger("db2").await.unwrap();

        let spec = DatasetSpec::new()
            .with_default(GraphSource::new("db1:main"))
            .with_named(GraphSource::new("db2:main"));

        let dataset = fluree.build_dataset_view(&spec).await.unwrap();

        assert!(!dataset.is_single_ledger());
        assert_eq!(dataset.len(), 2);
    }

    #[tokio::test]
    async fn test_try_single_view_from_spec() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        // Single default, no time spec - should return Some
        let spec = DatasetSpec::new().with_default(GraphSource::new("testdb:main"));
        let result = fluree.try_single_view_from_spec(&spec).await.unwrap();
        assert!(result.is_some());

        // Single default with time spec - should still return Some (single ledger)
        let spec = DatasetSpec::new()
            .with_default(GraphSource::new("testdb:main").with_time(dataset::TimeSpec::AtT(0)));
        let result = fluree.try_single_view_from_spec(&spec).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_is_single_ledger_fast_path() {
        // No time spec - fast path
        let spec = DatasetSpec::new().with_default(GraphSource::new("testdb:main"));
        assert!(Fluree::is_single_ledger_fast_path(&spec));

        // With time spec - not fast path (needs time resolution)
        let spec = DatasetSpec::new()
            .with_default(GraphSource::new("testdb:main").with_time(dataset::TimeSpec::AtT(5)));
        assert!(!Fluree::is_single_ledger_fast_path(&spec));

        // Multiple graphs - not fast path
        let spec = DatasetSpec::new()
            .with_default(GraphSource::new("db1:main"))
            .with_default(GraphSource::new("db2:main"));
        assert!(!Fluree::is_single_ledger_fast_path(&spec));
    }
}
