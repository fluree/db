//! Builder for DataSetDb from DatasetSpec
//!
//! Provides utilities to construct `DataSetDb` from query dataset
//! specifications, applying time travel and policy wrappers. Config-graph
//! defaults are applied later, at the query-preparation choke point
//! (`Fluree::complete_config_defaults`).

use crate::view::{DataSetDb, GraphDb};
use crate::{dataset, time_resolve, ApiError, DatasetSpec, Fluree, GovernanceOptions, Result};
use fluree_db_core::VerifiedIdentity;

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
            let ledger = $self.ledger(&range.ledger).await?;

            let from_t = time_resolve::resolve_time_spec(&ledger, &range.from).await?;
            let to_t = time_resolve::resolve_time_spec(&ledger, &range.to).await?;

            let view = GraphDb::from_ledger_state(&ledger);
            let view = ($history_transform)(view).await?;
            Ok(DataSetDb::single(view).with_history_range(from_t, to_t))
        } else {
            let mut dataset_db = DataSetDb::new();

            // Load default graphs, applying per-source policy
            for source in &spec.default_graphs {
                let view = ($load_view)(source).await?;
                let view = ($apply_policy)(view, source).await?;
                // If this is a graph source, also register as a named graph
                // so GRAPH <gs_id> patterns can resolve it during execution.
                if let Some(ref gs_id) = view.graph_source_id {
                    dataset_db = dataset_db.with_named(gs_id.as_ref(), view.clone());
                }
                dataset_db = dataset_db.with_default(view);
            }

            // Load named graphs, applying per-source policy
            for source in &spec.named_graphs {
                let view = ($load_view)(source).await?;
                let view = ($apply_policy)(view, source).await?;
                // Register under exactly ONE enumerable key: the name the user
                // gave the member (its alias, else its text as written, pin
                // included). A second key onto the same view would make
                // `GRAPH ?g` bind every solution twice (azure-chat#50).
                dataset_db = dataset_db.with_named(source.name(), view);
            }

            // A pinned member written `L@t:2` was once known as `L`; that name
            // still answers `GRAPH <L>` as a non-enumerated alias, when exactly
            // one member claims it and no member is named it outright.
            let mut claims: std::collections::HashMap<String, Vec<&str>> =
                std::collections::HashMap::new();
            for source in spec.named_graphs.iter().filter(|s| s.alias().is_none()) {
                if let Some(unpinned) = source.unpinned_name() {
                    claims.entry(unpinned).or_default().push(source.name());
                }
            }
            for (alias, names) in claims {
                if let [name] = names.as_slice() {
                    if !dataset_db.named.contains_key(alias.as_str()) {
                        dataset_db = dataset_db.with_named_alias(alias, *name);
                    }
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
        self.build_dataset_view_as(spec, None).await
    }

    /// [`Fluree::build_dataset_view`] on behalf of an auth-layer-verified
    /// caller.
    ///
    /// No request-level policy is applied, but a per-source `policy_override`
    /// still consults the ledger's `f:overrideControl`, and that check gates on
    /// `server_identity`. Pass the same verified identity a server route would
    /// place in `GovernanceOptions::server_identity`; `None` means anonymous,
    /// which `f:IdentityRestricted` denies.
    pub async fn build_dataset_view_as(
        &self,
        spec: &DatasetSpec,
        server_identity: Option<&VerifiedIdentity>,
    ) -> Result<DataSetDb> {
        build_dataset_view_from_spec!(
            self,
            spec,
            history_transform = |view| self.wrap_policy_defaults(view),
            load_view = |source| self.load_view_from_source(source),
            apply_policy =
                |view, source| self.maybe_apply_source_policy(view, source, server_identity),
        )
    }

    /// Build a `DataSetDb` with policy applied to all views.
    ///
    /// Policy is built from `GovernanceOptions` and applied uniformly
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
        opts: &GovernanceOptions,
    ) -> Result<DataSetDb> {
        build_dataset_view_from_spec!(
            self,
            spec,
            history_transform = |view| async { self.wrap_policy(view, opts).await },
            load_view = |source| self.load_view_from_source(source),
            apply_policy = |view, source| self.apply_policy_with_override(view, source, opts),
        )
    }

    /// Apply per-source policy if present, otherwise configured defaults.
    ///
    /// This is used by `build_dataset_view` when no global policy is provided.
    async fn maybe_apply_source_policy(
        &self,
        view: GraphDb,
        source: &dataset::GraphSource,
        server_identity: Option<&VerifiedIdentity>,
    ) -> Result<GraphDb> {
        if let Some(policy_override) = source.policy_override() {
            if policy_override.has_policy() {
                let mut opts = policy_override.to_query_connection_options();
                // The override comes from the request body; the verified
                // identity that gates config overrides is request-level.
                opts.server_identity = server_identity.cloned();
                return self.wrap_policy(view, &opts).await;
            }
        }
        self.wrap_policy_defaults(view).await
    }

    /// Apply policy with per-source override taking precedence over global.
    ///
    /// This is used by `build_dataset_view_with_policy` to allow per-source
    /// policy to override the global policy from `GovernanceOptions`.
    async fn apply_policy_with_override(
        &self,
        view: GraphDb,
        source: &dataset::GraphSource,
        global_opts: &GovernanceOptions,
    ) -> Result<GraphDb> {
        // Per-source policy override takes precedence
        if let Some(policy_override) = source.policy_override() {
            if policy_override.has_policy() {
                let mut opts = policy_override.to_query_connection_options();
                // The override comes from the request body; the verified
                // identity that gates config overrides is request-level.
                opts.server_identity = global_opts.server_identity.clone();
                return self.wrap_policy(view, &opts).await;
            }
        }
        // Fall back to global policy
        self.wrap_policy(view, global_opts).await
    }

    /// Build a single `GraphDb` from a `GraphSource`, on a connection surface:
    /// the member must name a ledger (or graph source) by address.
    ///
    /// The address is loaded as a ledger first; if no ledger has that id, as a
    /// graph source (Iceberg/R2RML), which yields a minimal genesis context
    /// tagged with the graph source id. Then the address's graph is selected,
    /// and config is resolved for that graph so per-graph overrides match the
    /// graph actually queried.
    ///
    /// For sources with a time spec, a graph source reads the pinned table
    /// state (`@time:` / `@recorded:` / `@snapshot:`); `@t:` and `@commit:`
    /// are rejected with a clear error.
    ///
    /// A bare graph IRI or a graph keyword names a graph of *some* ledger, and
    /// a connection surface has no ledger to find it in: that is a 400 naming
    /// the fix, never a nameservice lookup of the IRI as if it were a ledger.
    pub(crate) async fn load_view_from_source(
        &self,
        source: &dataset::GraphSource,
    ) -> Result<GraphDb> {
        use fluree_db_core::{DatasetRef, MemberRef};
        let address = match source.reference() {
            MemberRef::Dataset(
                DatasetRef::Address(address) | DatasetRef::Ambiguous { address, .. },
            ) => address,
            MemberRef::Dataset(DatasetRef::GraphIri(iri)) => {
                return Err(ApiError::invalid_query(format!(
                    "'{iri}' is a graph IRI, not a ledger, and this query has no target \
                     ledger to find it in. Name the ledger too ('<ledger>#{iri}'), or \
                     query that ledger's own endpoint"
                )));
            }
            MemberRef::Keyword(sel) => {
                return Err(ApiError::invalid_query(format!(
                    "'{sel}' names a graph of a ledger, and this query has no target \
                     ledger. Name the ledger too ('<ledger>#{sel}'), or query that \
                     ledger's own endpoint"
                )));
            }
        };
        let time_spec = source.time_spec();

        // Box the ledger-load future: the load chain (get_or_load → load →
        // load_novelty → bulk_apply_commits) is deep, and in debug builds its
        // inline future would balloon this frame — and every dispatcher frame
        // above it that materializes this future before awaiting — pushing the
        // plain `select *` connection query past the default ~2 MB worker stack
        // (fluree/db#1408). Boxing keeps the load future's state on the heap so
        // it costs O(1) stack here and in the callers above.
        let loaded = match time_spec {
            None => Box::pin(self.load_graph_db(address.id())).await,
            Some(spec) => Box::pin(self.load_graph_db_at(address.id(), spec.clone())).await,
        };
        match loaded {
            Ok(view) => {
                let view = Self::apply_graph_selector(view, address.graph())?;
                self.resolve_and_attach_config(view).await
            }
            Err(ref e) if e.is_not_found() => {
                // A graph source reads the pinned table state; the pin rides the
                // view to the R2RML provider, or is refused.
                let spec = time_spec.cloned().unwrap_or(dataset::TimeSpec::Latest);
                Box::pin(self.resolve_graph_source_address(address, &spec))
                    .await?
                    .ok_or_else(|| ApiError::NotFound(address.id().to_string()))
            }
            Err(e) => Err(e),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::GraphSource;
    use crate::FlureeBuilder;

    fn source(s: &str) -> GraphSource {
        GraphSource::parse(s).unwrap()
    }

    #[tokio::test]
    async fn test_build_dataset_view_single() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let spec = DatasetSpec::new().with_default(source("testdb:main"));
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
            .with_default(source("db1:main"))
            .with_named(source("db2:main"));

        let dataset = fluree.build_dataset_view(&spec).await.unwrap();

        assert!(!dataset.is_single_ledger());
        assert_eq!(dataset.len(), 2);
    }

    #[tokio::test]
    async fn test_try_single_view_from_spec() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        // Single default, no time spec - should return Some
        let spec = DatasetSpec::new().with_default(source("testdb:main"));
        let result = fluree.try_single_view_from_spec(&spec).await.unwrap();
        assert!(result.is_some());

        // Single default with time spec - should still return Some (single ledger)
        let spec = DatasetSpec::new()
            .with_default(source("testdb:main").with_time(dataset::TimeSpec::AtT(0)));
        let result = fluree.try_single_view_from_spec(&spec).await.unwrap();
        assert!(result.is_some());
    }

    /// A bare graph IRI names no ledger: on a connection surface it is refused
    /// as a caller mistake, not looked up in the nameservice as a ledger.
    #[tokio::test]
    async fn a_bare_graph_iri_is_refused_without_a_target_ledger() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();
        for iri in ["http://ex.org/g", "urn:ex:doc:1", "txn-meta"] {
            let spec = DatasetSpec::new()
                .with_default(source("testdb:main"))
                .with_named(source(iri));
            let err = fluree.build_dataset_view(&spec).await.unwrap_err();
            assert_eq!(err.status_code(), 400, "{iri}: {err}");
            assert!(err.to_string().contains("no target"), "{iri}: {err}");
        }
    }
}
