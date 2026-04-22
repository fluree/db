//! Lazy graph handle: zero-cost alias + time spec + executor reference.
//!
//! [`Graph`] is returned by [`Fluree::graph()`] and [`Fluree::graph_at()`].
//! No I/O occurs until a terminal method is called (`.load()`, `.query().execute()`,
//! `.transact().commit()`).

use crate::dataset::TimeSpec;
use crate::graph_commit_builder::CommitBuilder;
use crate::graph_query_builder::GraphQueryBuilder;
use crate::graph_snapshot::GraphSnapshot;
use crate::graph_transact_builder::GraphTransactBuilder;
use crate::{Fluree, Result};
use fluree_db_core::ContentId;

/// A lazy, zero-cost handle to a ledger graph.
///
/// No I/O occurs until a terminal method is called (`.load()`, `.query().execute()`,
/// `.transact().commit()`, `.transact().stage()`).
///
/// # Examples
///
/// ```ignore
/// // Lazy query — no intermediate .await?
/// let result = fluree
///     .graph("mydb:main")
///     .query()
///     .sparql("SELECT ?s WHERE { ?s ?p ?o }")
///     .execute()
///     .await?;
///
/// // Lazy transact + commit
/// let out = fluree
///     .graph("mydb:main")
///     .transact()
///     .insert(&data)
///     .commit()
///     .await?;
///
/// // Materialize for reuse
/// let snapshot = fluree.graph("mydb:main").load().await?;
/// let r1 = snapshot.query().sparql("SELECT ...").execute().await?;
/// let r2 = snapshot.query().jsonld(&q).execute().await?;
/// ```
pub struct Graph<'a> {
    pub(crate) fluree: &'a Fluree,
    pub(crate) ledger_id: String,
    pub(crate) time_spec: TimeSpec,
}

impl<'a> Graph<'a> {
    /// Create a new lazy graph handle.
    pub(crate) fn new(fluree: &'a Fluree, ledger_id: String, time_spec: TimeSpec) -> Self {
        Self {
            fluree,
            ledger_id,
            time_spec,
        }
    }

    /// Materialize the snapshot, producing a [`GraphSnapshot`] that can be queried
    /// multiple times without re-loading.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = fluree.graph("mydb:main").load().await?;
    /// let r1 = snapshot.query().sparql("...").execute().await?;
    /// let r2 = snapshot.query().jsonld(&q).execute().await?;
    /// ```
    pub async fn load(&self) -> Result<GraphSnapshot<'a>> {
        let view = self
            .fluree
            .load_graph_db_at(&self.ledger_id, self.time_spec.clone())
            .await?;
        Ok(GraphSnapshot::new(self.fluree, view))
    }

    /// Create a query builder. No I/O occurs until `.execute().await?`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree
    ///     .graph("mydb:main")
    ///     .query()
    ///     .sparql("SELECT ?s WHERE { ?s ?p ?o }")
    ///     .execute()
    ///     .await?;
    /// ```
    /// Create a query builder.
    ///
    /// When the `iceberg` feature is compiled, R2RML/Iceberg graph source
    /// support is automatically enabled — graph sources resolve transparently.
    pub fn query(&self) -> GraphQueryBuilder<'a, '_> {
        let builder = GraphQueryBuilder::new(self);
        #[cfg(feature = "iceberg")]
        let builder = builder.with_r2rml();
        builder
    }

    /// Create a transaction builder. No I/O occurs until `.commit().await?`
    /// or `.stage().await?`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let out = fluree
    ///     .graph("mydb:main")
    ///     .transact()
    ///     .insert(&data)
    ///     .commit()
    ///     .await?;
    /// ```
    pub fn transact(&self) -> GraphTransactBuilder<'a, '_> {
        GraphTransactBuilder::new(self)
    }

    /// Fetch and decode a single commit by CID.
    ///
    /// Returns a [`CommitDetail`](crate::graph_commit_builder::CommitDetail) with
    /// all flakes resolved to compact IRIs. Optionally supply a custom `@context`
    /// via `.context()` for IRI compaction.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let detail = fluree
    ///     .graph("mydb:main")
    ///     .commit(&commit_id)
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn commit(&self, id: &ContentId) -> CommitBuilder<'a, '_> {
        CommitBuilder::new(self, id.clone())
    }

    /// Fetch and decode a single commit by hex-digest prefix.
    ///
    /// Accepts abbreviated commit hashes (minimum 6 chars) as shown by
    /// `fluree log`, or full CID strings. If the string parses as a valid CID,
    /// it's used directly; otherwise it's treated as a hex prefix.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let detail = fluree
    ///     .graph("mydb:main")
    ///     .commit_prefix("bagaybq")
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn commit_prefix(&self, prefix: &str) -> CommitBuilder<'a, '_> {
        // Try parsing as a full CID first
        if let Ok(cid) = prefix.parse::<ContentId>() {
            CommitBuilder::new(self, cid)
        } else {
            CommitBuilder::from_prefix(self, prefix.to_string())
        }
    }

    /// Fetch and decode a single commit by transaction number (`t`).
    ///
    /// Resolves the `t` value to a commit CID via the txn-meta index, then
    /// decodes and returns the full [`CommitDetail`](crate::graph_commit_builder::CommitDetail).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let detail = fluree
    ///     .graph("mydb:main")
    ///     .commit_t(5)
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn commit_t(&self, t: i64) -> CommitBuilder<'a, '_> {
        CommitBuilder::from_t(self, t)
    }
}
