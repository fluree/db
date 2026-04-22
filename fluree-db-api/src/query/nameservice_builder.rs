//! Nameservice query builder: fluent API for querying nameservice metadata.
//!
//! The nameservice stores metadata about all ledgers and graph sources.
//! This builder provides a consistent API matching other query builders
//! ([`ViewQueryBuilder`], [`DatasetQueryBuilder`], [`FromQueryBuilder`]).
//!
//! # Example
//!
//! ```ignore
//! // Find all ledgers on main branch
//! let query = json!({
//!     "@context": {"f": "https://ns.flur.ee/db#"},
//!     "select": ["?ledger"],
//!     "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:branch": "main"}]
//! });
//!
//! let results = fluree.nameservice_query()
//!     .jsonld(&query)
//!     .execute_formatted()
//!     .await?;
//! ```
//!
//! # How It Works
//!
//! 1. Fetches all `NsRecord` and `GraphSourceRecord` from the nameservice
//! 2. Converts them to JSON-LD format
//! 3. Creates a temporary in-memory Fluree instance
//! 4. Inserts all records as a `@graph` transaction
//! 5. Executes the provided query against the temp ledger
//! 6. Returns results (temp instance is automatically cleaned up)

use serde_json::Value as JsonValue;

use crate::error::{BuilderError, BuilderErrors};
use crate::format::FormatterConfig;
use crate::ledger_info::{gs_record_to_jsonld, ns_record_to_jsonld};
use crate::query::builder::QueryCore;
use crate::view::QueryInput;
use crate::{ApiError, Fluree, GraphDb, Result};
use fluree_db_ledger::IndexConfig;
use fluree_db_transact::{CommitOpts, TxnOpts, TxnType};
use serde_json::json;

// ============================================================================
// NameserviceQueryBuilder
// ============================================================================

/// Builder for queries against nameservice metadata.
///
/// Created via [`Fluree::nameservice_query()`].
///
/// Queries all ledger and graph source records stored in the nameservice.
/// Uses standard JSON-LD/SPARQL syntax against an ephemeral in-memory database
/// populated with nameservice records.
///
/// # Example
///
/// ```ignore
/// // Query all ledgers with their t values
/// let query = json!({
///     "@context": {"f": "https://ns.flur.ee/db#"},
///     "select": ["?ledger", "?t"],
///     "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:t": "?t"}]
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
/// - `f:ledger` - Ledger name (without branch)
/// - `f:branch` - Branch name (e.g., "main")
/// - `f:t` - Current transaction number
/// - `f:status` - Status ("ready" or "retracted")
/// - `f:ledgerCommit` - Commit address reference
/// - `f:ledgerIndex` - Index info with `@id` and `f:t`
///
/// Graph source records (`@type: "f:IndexSource"` or `"f:MappedSource"`):
/// - `f:name` - Graph source name
/// - `f:branch` - Branch name
/// - `f:status` - Status
/// - `f:graphSourceConfig` - Configuration JSON
/// - `f:graphSourceDependencies` - Source ledger dependencies
/// - `f:graphSourceIndex` - Index address
/// - `f:graphSourceIndexT` - Index t value
pub struct NameserviceQueryBuilder<'a> {
    fluree: &'a Fluree,
    core: QueryCore<'a>,
}

impl<'a> NameserviceQueryBuilder<'a> {
    /// Create a new builder (called by `Fluree::nameservice_query()`).
    pub(crate) fn new(fluree: &'a Fluree) -> Self {
        Self {
            fluree,
            core: QueryCore::new(),
        }
    }

    // --- Shared setters (delegated to QueryCore) ---

    /// Set the query input as JSON-LD.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let query = json!({
    ///     "@context": {"f": "https://ns.flur.ee/db#"},
    ///     "select": ["?ledger"],
    ///     "where": [{"@id": "?ns", "f:ledger": "?ledger"}]
    /// });
    ///
    /// let results = fluree.nameservice_query()
    ///     .jsonld(&query)
    ///     .execute_formatted()
    ///     .await?;
    /// ```
    pub fn jsonld(mut self, json: &'a JsonValue) -> Self {
        self.core.set_jsonld(json);
        self
    }

    /// Set the query input as SPARQL.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = fluree.nameservice_query()
    ///     .sparql("SELECT ?ledger WHERE { ?ns <https://ns.flur.ee/db#ledger> ?ledger }")
    ///     .execute_formatted()
    ///     .await?;
    /// ```
    pub fn sparql(mut self, sparql: &'a str) -> Self {
        self.core.set_sparql(sparql);
        self
    }

    /// Set format configuration (used by `.execute_formatted()`).
    ///
    /// Defaults to JSON-LD for `.jsonld()` input, SPARQL JSON for `.sparql()` input.
    pub fn format(mut self, config: FormatterConfig) -> Self {
        self.core.set_format(config);
        self
    }

    // --- Terminal operations ---

    /// Validate builder configuration without executing.
    ///
    /// Returns all accumulated errors at once.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        let errs = self.validate_core();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(BuilderErrors(errs))
        }
    }

    /// Internal validation (doesn't check graph_sources since we don't support them here)
    fn validate_core(&self) -> Vec<BuilderError> {
        let mut errs = Vec::new();
        if self.core.input.is_none() {
            errs.push(BuilderError::Missing {
                field: "input",
                hint: "call .jsonld(&query) or .sparql(\"SELECT ...\")",
            });
        }
        errs
    }

    /// Execute the query and return formatted JSON output.
    ///
    /// Creates a temporary in-memory database, populates it with all
    /// nameservice records, executes the query, and returns formatted results.
    ///
    /// Uses `.format()` config if set, otherwise defaults based on input type
    /// (JSON-LD for `.jsonld()`, SPARQL JSON for `.sparql()`).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = fluree.nameservice_query()
    ///     .jsonld(&query)
    ///     .execute_formatted()
    ///     .await?;
    ///
    /// // Results are JSON array, e.g.: [["ledger-one"], ["ledger-two"]]
    /// ```
    pub async fn execute_formatted(mut self) -> Result<JsonValue> {
        let errs = self.validate_core();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        // Get default format BEFORE taking input (default_format checks input type)
        let format_config = self
            .core
            .format
            .take()
            .unwrap_or_else(|| self.core.default_format());
        let input = self.core.input.take().unwrap(); // safe: validated

        // Execute the query against nameservice records
        self.execute_query(input, Some(format_config)).await
    }

    /// Execute the query and return raw JSON output.
    ///
    /// Like `execute_formatted()` but uses default formatting based on input type.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = fluree.nameservice_query()
    ///     .jsonld(&query)
    ///     .execute()
    ///     .await?;
    /// ```
    pub async fn execute(mut self) -> Result<JsonValue> {
        let errs = self.validate_core();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let input = self.core.input.take().unwrap();
        self.execute_query(input, None).await
    }

    /// Internal: execute query against nameservice records
    async fn execute_query(
        &self,
        input: QueryInput<'a>,
        format_config: Option<FormatterConfig>,
    ) -> Result<JsonValue> {
        // 1. Get all ledger records
        let ledger_records = self.fluree.nameservice().all_records().await?;

        // 2. Get all graph source records
        let gs_records = self.fluree.nameservice().all_graph_source_records().await?;

        // 3. Convert to JSON-LD
        let mut all_records: Vec<JsonValue> =
            ledger_records.iter().map(ns_record_to_jsonld).collect();
        all_records.extend(gs_records.iter().map(gs_record_to_jsonld));

        // 4. If no records, return empty result immediately
        if all_records.is_empty() {
            return Ok(json!([]));
        }

        // 5. Create temporary in-memory Fluree instance
        let temp_fluree = crate::FlureeBuilder::memory().build_memory();

        // 6. Create temporary ledger
        let ledger = temp_fluree
            .create_ledger("ns-query")
            .await
            .map_err(|e| ApiError::internal(format!("Failed to create temp ledger: {e}")))?;

        // 7. Insert all records as JSON-LD transaction
        let txn_json = json!({ "@graph": all_records });
        let index_config = IndexConfig::default();

        let result = temp_fluree
            .transact(
                ledger,
                TxnType::Insert,
                &txn_json,
                TxnOpts::default(),
                CommitOpts::default(),
                &index_config,
            )
            .await
            .map_err(|e| ApiError::internal(format!("Failed to insert NS records: {e}")))?;

        // 8. Execute query based on input type
        let db = GraphDb::from_ledger_state(&result.ledger);
        match input {
            QueryInput::JsonLd(query_json) => {
                if let Some(config) = format_config {
                    // Use formatted query path
                    let query_result = temp_fluree
                        .query(&db, query_json)
                        .await
                        .map_err(|e| ApiError::query(format!("Nameservice query failed: {e}")))?;
                    Ok(query_result
                        .format_async(db.as_graph_db_ref(), &config)
                        .await?)
                } else {
                    // Use default JSON-LD formatting
                    let query_result = temp_fluree
                        .query(&db, query_json)
                        .await
                        .map_err(|e| ApiError::query(format!("Nameservice query failed: {e}")))?;
                    query_result
                        .to_jsonld_async(db.as_graph_db_ref())
                        .await
                        .map_err(|e| ApiError::query(format!("Nameservice query failed: {e}")))
                }
            }
            QueryInput::Sparql(sparql) => {
                let query_result = temp_fluree.query(&db, sparql).await.map_err(|e| {
                    ApiError::query(format!("Nameservice SPARQL query failed: {e}"))
                })?;

                let config = format_config.unwrap_or_else(FormatterConfig::sparql_json);
                Ok(query_result
                    .format_async(db.as_graph_db_ref(), &config)
                    .await?)
            }
        }

        // temp_fluree is dropped here - automatic cleanup
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlureeBuilder;
    use fluree_db_core::{ContentId, ContentKind};
    use fluree_db_nameservice::{GraphSourcePublisher, GraphSourceType, Publisher};

    async fn setup_ns_with_records() -> Fluree {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create some ledger records
        let cid1 = ContentId::new(ContentKind::Commit, b"commit-1");
        let cid2 = ContentId::new(ContentKind::Commit, b"commit-2");
        let cid3 = ContentId::new(ContentKind::Commit, b"commit-3");
        fluree
            .nameservice_mode
            .publish_commit("db1:main", 10, &cid1)
            .await
            .unwrap();
        fluree
            .nameservice_mode
            .publish_commit("db1:dev", 5, &cid2)
            .await
            .unwrap();
        fluree
            .nameservice_mode
            .publish_commit("db2:main", 20, &cid3)
            .await
            .unwrap();

        // Create a graph source record
        fluree
            .nameservice_mode
            .publish_graph_source(
                "my-search",
                "main",
                GraphSourceType::Bm25,
                r#"{"k1":1.2}"#,
                &["db1:main".to_string()],
            )
            .await
            .unwrap();

        fluree
    }

    #[test]
    fn test_builder_validate_missing_input() {
        let fluree = FlureeBuilder::memory().build_memory();
        let builder = NameserviceQueryBuilder::new(&fluree);
        let result = builder.validate();
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert_eq!(errs.0.len(), 1);
        assert!(matches!(
            &errs.0[0],
            BuilderError::Missing { field: "input", .. }
        ));
    }

    #[test]
    fn test_builder_validate_with_jsonld() {
        let fluree = FlureeBuilder::memory().build_memory();
        let query = json!({
            "select": ["?ledger"],
            "where": [{"@id": "?ns", "f:ledger": "?ledger"}]
        });
        let builder = fluree.nameservice_query().jsonld(&query);
        let result = builder.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_builder_validate_with_sparql() {
        let fluree = FlureeBuilder::memory().build_memory();
        let builder = fluree
            .nameservice_query()
            .sparql("SELECT ?ledger WHERE { ?ns <https://ns.flur.ee/db#ledger> ?ledger }");
        let result = builder.validate();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_builder_execute_all_ledgers() {
        let fluree = setup_ns_with_records().await;

        let query = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?ledger"],
            "where": [{"@id": "?ns", "@type": "f:LedgerSource", "f:ledger": "?ledger"}]
        });

        let result = fluree
            .nameservice_query()
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap();

        let arr = result.as_array().expect("Expected array result");
        assert_eq!(arr.len(), 3, "Should have 3 ledger records");
    }

    #[tokio::test]
    async fn test_builder_execute_by_branch() {
        let fluree = setup_ns_with_records().await;

        let query = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?ledger"],
            "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:branch": "main"}]
        });

        let result = fluree
            .nameservice_query()
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap();

        let arr = result.as_array().expect("Expected array result");
        assert_eq!(arr.len(), 2, "Should have 2 ledgers on main branch");
    }

    #[tokio::test]
    async fn test_builder_execute_graph_sources() {
        let fluree = setup_ns_with_records().await;

        let query = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?name"],
            "where": [{"@id": "?gs", "@type": "f:IndexSource", "f:name": "?name"}]
        });

        let result = fluree
            .nameservice_query()
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap();

        let arr = result.as_array().expect("Expected array result");
        assert_eq!(arr.len(), 1, "Should have 1 graph source record");
    }

    #[tokio::test]
    async fn test_builder_empty_nameservice() {
        let fluree = FlureeBuilder::memory().build_memory();

        let query = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?ledger"],
            "where": [{"@id": "?ns", "f:ledger": "?ledger"}]
        });

        let result = fluree
            .nameservice_query()
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap();

        assert_eq!(result, json!([]));
    }

    #[tokio::test]
    async fn test_builder_with_custom_format() {
        let fluree = setup_ns_with_records().await;

        let query = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?ledger", "?t"],
            "where": [{"@id": "?ns", "@type": "f:LedgerSource", "f:ledger": "?ledger", "f:t": "?t"}]
        });

        let result = fluree
            .nameservice_query()
            .jsonld(&query)
            .format(FormatterConfig::agent_json())
            .execute_formatted()
            .await
            .unwrap();

        // AgentJson returns an envelope with schema, rows, rowCount, etc.
        let obj = result.as_object().expect("Expected object envelope");
        assert!(obj.contains_key("schema"), "Should have schema");
        assert!(obj.contains_key("rows"), "Should have rows");
        let rows = obj["rows"].as_array().expect("rows should be array");
        assert_eq!(rows.len(), 3, "Should have 3 results");
        assert!(rows[0].is_object(), "Rows should be objects");
    }

    #[tokio::test]
    async fn test_builder_sparql_query() {
        let fluree = setup_ns_with_records().await;

        let result = fluree
            .nameservice_query()
            .sparql(
                "PREFIX f: <https://ns.flur.ee/db#>
                 SELECT ?ledger WHERE { ?ns a f:LedgerSource ; f:ledger ?ledger }",
            )
            .execute_formatted()
            .await
            .unwrap();

        // SPARQL returns { head: { vars: [...] }, results: { bindings: [...] } }
        assert!(
            result.get("head").is_some(),
            "SPARQL result should have 'head'"
        );
        assert!(
            result.get("results").is_some(),
            "SPARQL result should have 'results'"
        );

        let bindings = result["results"]["bindings"].as_array().unwrap();
        assert_eq!(bindings.len(), 3, "Should have 3 ledger results");
    }
}
