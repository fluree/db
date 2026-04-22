//! Materialized graph snapshot bound to an executor.
//!
//! [`GraphSnapshot`] holds both a [`GraphDb`] and a reference to [`Fluree`],
//! so queries no longer need `&fluree` passed separately.

use crate::graph_query_builder::GraphSnapshotQueryBuilder;
use crate::view::GraphDb;
use crate::Fluree;

/// A materialized, queryable graph snapshot.
///
/// Holds both the immutable snapshot and a reference to the executor,
/// so queries can be run without passing `&fluree` at each call site.
///
/// # Examples
///
/// ```ignore
/// let snapshot = fluree.graph("mydb:main").load().await?;
///
/// // Query multiple times — no re-loading
/// let r1 = snapshot.query().sparql("SELECT ...").execute().await?;
/// let r2 = snapshot.query().jsonld(&q).execute().await?;
///
/// // Access the underlying view if needed
/// let view = snapshot.db();
/// ```
pub struct GraphSnapshot<'a> {
    pub(crate) fluree: &'a Fluree,
    pub(crate) view: GraphDb,
}

impl<'a> GraphSnapshot<'a> {
    /// Create a new snapshot (called internally by `Graph::load()`).
    pub(crate) fn new(fluree: &'a Fluree, view: GraphDb) -> Self {
        Self { fluree, view }
    }

    /// Create a query builder for this snapshot.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = fluree.graph("mydb:main").load().await?;
    /// let result = snapshot.query().jsonld(&q).execute().await?;
    /// ```
    pub fn query(&self) -> GraphSnapshotQueryBuilder<'a, '_> {
        GraphSnapshotQueryBuilder::new_from_parts(self.fluree, &self.view)
    }

    /// Access the underlying [`GraphDb`] snapshot.
    pub fn db(&self) -> &GraphDb {
        &self.view
    }

    /// Unwrap into the underlying [`GraphDb`] snapshot.
    pub fn into_db(self) -> GraphDb {
        self.view
    }
}
