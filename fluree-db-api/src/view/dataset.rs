//! Dataset view for multi-ledger queries
//!
//! Provides `DataSetDb`, a collection of `GraphDb`s representing
//! a SPARQL-style dataset with default and named graphs.

use std::collections::HashMap;
use std::sync::Arc;

use crate::view::GraphDb;
use crate::OverlayProvider;

/// A dataset view composed of multiple `GraphDb`s.
///
/// This mirrors SPARQL dataset semantics:
/// - `default`: graphs merged for the default graph (FROM clauses)
/// - `named`: graphs available as named graphs (FROM NAMED clauses)
///
/// # Usage
///
/// ```ignore
/// // Build dataset from multiple views
/// let view1 = fluree.db("ledger1:main").await?;
/// let view2 = fluree.db("ledger2:main").await?;
///
/// let dataset = DataSetDb::new()
///     .with_default(view1)
///     .with_named("http://example.org/graph2", view2);
///
/// // Query across the dataset
/// let result = fluree.query_dataset(&dataset, sparql_query).await?;
/// ```
#[derive(Clone)]
pub struct DataSetDb {
    /// Default graph views (merged for queries without GRAPH clause)
    pub default: Vec<GraphDb>,
    /// Named graph views (accessed via GRAPH clause)
    pub named: HashMap<Arc<str>, GraphDb>,
    /// Deterministic named graph insertion order (for primary selection).
    ///
    /// We keep this because `HashMap` does not preserve insertion order, but
    /// connection queries need stable "primary" selection for parsing/formatting.
    pub named_order: Vec<Arc<str>>,
    /// Optional history range (from_t, to_t) for history/changes queries.
    ///
    /// When set, query execution runs in history mode (`@op` support) and applies
    /// the time range bounds. This is a Fluree extension detected from dataset
    /// specs like: `from: ["ledger@t:1", "ledger@t:latest"]`.
    pub history_range: Option<(i64, i64)>,
}

impl DataSetDb {
    /// Create an empty dataset view.
    pub fn new() -> Self {
        Self {
            default: Vec::new(),
            named: HashMap::new(),
            named_order: Vec::new(),
            history_range: None,
        }
    }

    /// Create a dataset view with a single default graph.
    pub fn single(view: GraphDb) -> Self {
        Self {
            default: vec![view],
            named: HashMap::new(),
            named_order: Vec::new(),
            history_range: None,
        }
    }

    /// Add a view to the default graph.
    pub fn with_default(mut self, view: GraphDb) -> Self {
        self.default.push(view);
        self
    }

    /// Add a view as a named graph.
    pub fn with_named(mut self, name: impl Into<Arc<str>>, view: GraphDb) -> Self {
        let key: Arc<str> = name.into();
        if !self.named.contains_key(&key) {
            self.named_order.push(Arc::clone(&key));
        }
        self.named.insert(key, view);
        self
    }

    /// Attach a history time range (from_t, to_t) for history/changes queries.
    pub fn with_history_range(mut self, from_t: i64, to_t: i64) -> Self {
        self.history_range = Some((from_t, to_t));
        self
    }

    /// Returns true if this dataset represents a history/changes query.
    pub fn is_history_mode(&self) -> bool {
        self.history_range.is_some()
    }

    /// Get the history time range, if present.
    pub fn history_time_range(&self) -> Option<(i64, i64)> {
        self.history_range
    }

    /// Add multiple views to the default graph.
    pub fn with_defaults(mut self, views: impl IntoIterator<Item = GraphDb>) -> Self {
        self.default.extend(views);
        self
    }

    /// Check if the dataset has any graphs.
    pub fn is_empty(&self) -> bool {
        self.default.is_empty() && self.named.is_empty()
    }

    /// Get a "primary" graph view for parsing/formatting.
    ///
    /// Primary selection behavior:
    /// - first default if present
    /// - else first named (in insertion order)
    /// - else None
    pub fn primary(&self) -> Option<&GraphDb> {
        if let Some(v) = self.default.first() {
            return Some(v);
        }
        let iri = self.named_order.first()?;
        self.named.get(iri)
    }

    /// Get the "primary" graph view, mutably.
    pub fn primary_mut(&mut self) -> Option<&mut GraphDb> {
        if let Some(v) = self.default.first_mut() {
            return Some(v);
        }
        let iri = self.named_order.first()?.clone();
        self.named.get_mut(&iri)
    }

    /// Get a named graph by IRI.
    pub fn get_named(&self, name: &str) -> Option<&GraphDb> {
        self.named.get(name)
    }

    /// Get the maximum `t` across all views in the dataset.
    ///
    /// This is only a safe upper bound for internal operations that need a bound
    /// across overlays. It is NOT meaningful user-facing result metadata for
    /// multi-ledger datasets because `t` values are ledger-local.
    pub fn max_t(&self) -> i64 {
        let default_max = self.default.iter().map(|v| v.t).max().unwrap_or(0);
        let named_max = self.named.values().map(|v| v.t).max().unwrap_or(0);
        default_max.max(named_max)
    }

    /// Get a meaningful result `t` if the dataset is effectively one ledger/time.
    ///
    /// Returns `Some(t)` only when every view in the dataset points at the same
    /// ledger and the same transaction boundary. Returns `None` for multi-ledger
    /// or mixed-time datasets.
    pub fn result_t(&self) -> Option<i64> {
        let mut views = self.default.iter().chain(self.named.values());
        let first = views.next()?;
        let ledger_id = first.ledger_id.as_ref();
        let t = first.t;

        views
            .all(|view| view.ledger_id.as_ref() == ledger_id && view.t == t)
            .then_some(t)
    }

    /// Build a runtime `fluree-db-query` dataset from this view.
    ///
    /// This is the internal bridge to the query engine. Each graph keeps its own
    /// `t` (per-view), and policy enforcement is carried via `GraphRef::policy_enforcer`.
    pub(crate) fn as_runtime_dataset(&self) -> fluree_db_query::DataSet<'_> {
        let mut ds = fluree_db_query::DataSet::new();

        for view in &self.default {
            let mut graph = fluree_db_query::GraphRef::new(
                view.snapshot.as_ref(),
                view.graph_id,
                view.overlay.as_ref(),
                view.t,
                Arc::clone(&view.ledger_id),
            );
            graph.policy_enforcer = view.policy_enforcer().cloned();
            ds = ds.with_default_graph(graph);
        }

        for iri in &self.named_order {
            let view = self
                .named
                .get(iri)
                .expect("named_order key must exist in named map");
            let mut graph = fluree_db_query::GraphRef::new(
                view.snapshot.as_ref(),
                view.graph_id,
                view.overlay.as_ref(),
                view.t,
                Arc::clone(&view.ledger_id),
            );
            graph.policy_enforcer = view.policy_enforcer().cloned();
            ds = ds.with_named_graph(Arc::clone(iri), graph);
        }

        ds
    }

    /// Build a composite overlay across all graphs (for graph crawl formatting).
    pub(crate) fn composite_overlay(&self) -> Option<Arc<dyn OverlayProvider>> {
        let mut overlays: Vec<Arc<dyn OverlayProvider>> = Vec::new();

        for v in &self.default {
            overlays.push(Arc::clone(&v.overlay));
        }
        for iri in &self.named_order {
            if let Some(v) = self.named.get(iri) {
                overlays.push(Arc::clone(&v.overlay));
            }
        }

        if overlays.is_empty() {
            return None;
        }

        let composite = crate::overlay::CompositeOverlay::new(overlays);
        Some(Arc::new(composite))
    }

    /// Get the number of views in the dataset.
    pub fn len(&self) -> usize {
        self.default.len() + self.named.len()
    }

    /// Check if this is a single-ledger dataset.
    ///
    /// Returns `true` if there's exactly one default graph and no named graphs.
    pub fn is_single_ledger(&self) -> bool {
        self.default.len() == 1 && self.named.is_empty() && self.history_range.is_none()
    }

    /// Unwrap a single-ledger dataset into its view.
    ///
    /// Returns `None` if this is a multi-ledger dataset.
    pub fn into_single(mut self) -> Option<GraphDb> {
        if self.is_single_ledger() {
            self.default.pop()
        } else {
            None
        }
    }
}

impl Default for DataSetDb {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for DataSetDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataSetDb")
            .field("default_count", &self.default.len())
            .field("named_graphs", &self.named_order)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlureeBuilder;

    #[tokio::test]
    async fn test_dataset_view_single() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let view = fluree.db("testdb:main").await.unwrap();
        let dataset = DataSetDb::single(view);

        assert!(dataset.is_single_ledger());
        assert_eq!(dataset.len(), 1);
        assert!(dataset.primary().is_some());
    }

    #[tokio::test]
    async fn test_dataset_view_builder() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger1 = fluree.create_ledger("db1").await.unwrap();
        let _ledger2 = fluree.create_ledger("db2").await.unwrap();

        let view1 = fluree.db("db1:main").await.unwrap();
        let view2 = fluree.db("db2:main").await.unwrap();

        let dataset = DataSetDb::new()
            .with_default(view1)
            .with_named("http://example.org/graph2", view2);

        assert!(!dataset.is_single_ledger());
        assert_eq!(dataset.len(), 2);
        assert!(dataset.get_named("http://example.org/graph2").is_some());
    }

    #[tokio::test]
    async fn test_dataset_view_into_single() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let view = fluree.db("testdb:main").await.unwrap();
        let expected_ledger_id = view.ledger_id.clone();
        let dataset = DataSetDb::single(view);

        let unwrapped = dataset.into_single();
        assert!(unwrapped.is_some());
        assert_eq!(unwrapped.unwrap().ledger_id, expected_ledger_id);
    }

    #[tokio::test]
    async fn test_dataset_view_max_t() {
        use serde_json::json;

        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("dataset_max_t_test").await.unwrap();

        // Transact to get t=1 (use same pattern as working test)
        let txn = json!({ "@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:a", "ex:name": "Alice"}] });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        // Use explicit t values to test max_t logic
        // (view_at_t uses historical loading which correctly sets t)
        let view_t1 = fluree.db_at_t("dataset_max_t_test:main", 1).await.unwrap();
        let view_t0 = fluree.db_at_t("dataset_max_t_test:main", 0).await.unwrap();

        assert_eq!(view_t1.t, 1);
        assert_eq!(view_t0.t, 0);

        let dataset = DataSetDb::new()
            .with_default(view_t1)
            .with_named("http://example.org/old", view_t0);

        assert_eq!(dataset.max_t(), 1);
        assert_eq!(dataset.result_t(), None);
    }

    #[tokio::test]
    async fn test_dataset_view_result_t_single_ledger_same_time() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("dataset_result_t_test").await.unwrap();

        let view1 = fluree.db("dataset_result_t_test:main").await.unwrap();
        let view2 = fluree.db("dataset_result_t_test:main").await.unwrap();

        let dataset = DataSetDb::new()
            .with_default(view1.clone())
            .with_named("http://example.org/same-ledger", view2);

        assert_eq!(dataset.result_t(), Some(view1.t));
    }
}
