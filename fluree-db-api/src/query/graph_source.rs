use serde_json::Value as JsonValue;

use crate::query::helpers::{build_query_result, parse_jsonld_query, parse_sparql_to_ir};
use crate::query::nameservice_builder::NameserviceQueryBuilder;
use crate::{ExecutableQuery, Fluree, LedgerState, QueryResult, Result};

impl Fluree {
    /// Create a builder for querying nameservice metadata.
    ///
    /// Returns a [`NameserviceQueryBuilder`] for fluent query construction
    /// against all ledger and graph source records in the nameservice.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find all ledgers on main branch
    /// let query = json!({
    ///     "@context": {"f": "https://ns.flur.ee/db#"},
    ///     "select": ["?ledger"],
    ///     "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:branch": "main"}]
    /// });
    ///
    /// let results = fluree.nameservice_query()
    ///     .jsonld(&query)
    ///     .execute_formatted()
    ///     .await?;
    /// ```
    ///
    /// # Available Properties
    ///
    /// Ledger records (`@type: "f:LedgerSource"`):
    /// - `f:ledger` - Ledger name
    /// - `f:branch` - Branch name
    /// - `f:t` - Transaction number
    /// - `f:status` - Status ("ready" or "retracted")
    /// - `f:ledgerCommit` - Commit address
    /// - `f:ledgerIndex` - Index info
    ///
    /// Graph source records (`@type: "f:IndexSource"` or `"f:MappedSource"`):
    /// - `f:name` - Graph source name
    /// - `f:branch` - Branch name
    /// - `f:graphSourceConfig` - Configuration
    /// - `f:graphSourceDependencies` - Source ledgers
    pub fn nameservice_query(&self) -> NameserviceQueryBuilder<'_> {
        NameserviceQueryBuilder::new(self)
    }

    /// Execute a query against all nameservice records (convenience method).
    ///
    /// This is a shorthand for:
    /// ```ignore
    /// fluree.nameservice_query()
    ///     .jsonld(&query)
    ///     .execute_formatted()
    ///     .await
    /// ```
    ///
    /// For more control over formatting, use [`nameservice_query()`](Self::nameservice_query).
    pub async fn query_nameservice(&self, query_json: &JsonValue) -> Result<JsonValue> {
        crate::nameservice_query::query_nameservice(&self.nameservice_mode, query_json).await
    }

    /// Execute a JSON-LD query with R2RML graph source support.
    pub async fn query_graph_source(
        &self,
        ledger: &LedgerState,
        query_json: &JsonValue,
    ) -> Result<QueryResult> {
        // No default-context auto-injection on this internal R2RML path —
        // callers that want it should pre-merge `@context` into `query_json`
        // (or wrap via `Fluree::db_with_default_context` if/when this method
        // is refactored to take a `&GraphDb`).
        let (vars, parsed) = parse_jsonld_query(query_json, &ledger.snapshot, None, None)?;
        let executable = ExecutableQuery::simple(parsed.clone());

        let r2rml_provider = crate::r2rml_provider!(self);
        let tracker = crate::Tracker::disabled();
        let db = ledger.as_graph_db_ref(0);
        let batches = crate::execute_with_r2rml(
            db,
            &vars,
            &executable,
            &tracker,
            &r2rml_provider,
            &r2rml_provider,
        )
        .await?;

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            Some(ledger.t()),
            Some(ledger.novelty.clone()),
            None,
        ))
    }

    /// Execute a SPARQL query with R2RML graph source support.
    pub async fn sparql_graph_source(
        &self,
        ledger: &LedgerState,
        sparql: &str,
    ) -> Result<QueryResult> {
        // See `query_graph_source` above — no default-context injection here.
        let (vars, parsed) = parse_sparql_to_ir(sparql, &ledger.snapshot, None)?;
        let executable = ExecutableQuery::simple(parsed.clone());

        let r2rml_provider = crate::r2rml_provider!(self);
        let tracker = crate::Tracker::disabled();
        let db = ledger.as_graph_db_ref(0);
        let batches = crate::execute_with_r2rml(
            db,
            &vars,
            &executable,
            &tracker,
            &r2rml_provider,
            &r2rml_provider,
        )
        .await?;

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            Some(ledger.t()),
            Some(ledger.novelty.clone()),
            None,
        ))
    }
}
