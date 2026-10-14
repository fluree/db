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
use crate::{ApiError, Fluree, Result};
use fluree_db_core::{ContentId, LedgerId, LedgerRef, RefError};

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
    /// The address as the caller wrote it, for messages.
    pub(crate) ledger_id: String,
    /// The address, parsed once: `[urn:fluree:]name[:branch][@pin][#graph]`.
    /// Building a handle does no I/O and cannot fail, so a malformed address
    /// is reported by the first terminal call.
    address: std::result::Result<LedgerRef, RefError>,
    /// The time `graph_at` was given; `Latest` for `graph`.
    time_spec: TimeSpec,
}

impl<'a> Graph<'a> {
    /// Create a new lazy graph handle.
    pub(crate) fn new(fluree: &'a Fluree, ledger_id: String, time_spec: TimeSpec) -> Self {
        let address = LedgerRef::parse(&ledger_id);
        Self {
            fluree,
            ledger_id,
            address,
            time_spec,
        }
    }

    /// The parsed address.
    pub(crate) fn address(&self) -> Result<&LedgerRef> {
        self.address
            .as_ref()
            .map_err(|e| ApiError::InvalidLedgerId(e.clone()))
    }

    /// The ledger (or graph source) the handle names.
    pub(crate) fn id(&self) -> Result<&LedgerId> {
        self.address().map(LedgerRef::id)
    }

    /// The time the handle reads: a pin written in the address, or the one
    /// `graph_at` was given. Both at once must agree.
    pub(crate) fn time_spec(&self) -> Result<TimeSpec> {
        match (self.address()?.at(), &self.time_spec) {
            (None, spec) => Ok(spec.clone()),
            (Some(own), TimeSpec::Latest) => Ok(own.clone()),
            (Some(own), spec) if own == spec => Ok(own.clone()),
            (Some(_), _) => Err(ApiError::invalid_query(format!(
                "'{}' pins a time and graph_at names a different one; drop one of them",
                self.ledger_id
            ))),
        }
    }

    /// Materialize the snapshot, producing a [`GraphSnapshot`] that can be queried
    /// multiple times without re-loading.
    ///
    /// The snapshot carries the ledger's configured policy defaults, so a bare
    /// read of a governed ledger is filtered here exactly as it is through
    /// [`Graph::query`]. An unconfigured ledger comes back untouched.
    ///
    /// A query's own `opts` cannot be applied here: the snapshot is materialized
    /// before any query is attached, and the same snapshot serves many queries.
    /// Those still reach only [`Graph::query`] and `query_from`.
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
            .load_address_at(self.address()?, self.time_spec()?)
            .await?;
        let view = self.fluree.wrap_policy_defaults(view).await?;
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
    /// Accepts a hex digest prefix (minimum 6 characters) as printed by
    /// `fluree log`, or a full CID string. If the string parses as a valid CID
    /// it is used directly; otherwise it is treated as a hex prefix.
    ///
    /// Hex, not base32: the indexed commit subject is minted from
    /// `ContentId::digest_hex()`, so the prefix scan is keyed on hex. An
    /// abbreviated CID cannot be resolved — the first twelve characters of
    /// every commit CID are a constant header — and is rejected saying so.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let detail = fluree
    ///     .graph("mydb:main")
    ///     .commit_prefix("ddca65d84c08")
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
