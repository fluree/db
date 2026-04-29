//! Nameservice Query API
//!
//! This module provides the `query_nameservice` function that creates a temporary
//! in-memory database from all nameservice records (ledgers and graph sources)
//! and executes a query against it.
//!
//! Nameservice query API implementation.
//!
//! ## Example Queries
//!
//! Find all ledgers:
//! ```json
//! {"select": ["?ledger"], "where": [{"@id": "?ns", "f:ledger": "?ledger"}]}
//! ```
//!
//! Find ledgers on main branch:
//! ```json
//! {"select": ["?ledger"], "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:branch": "main"}]}
//! ```
//!
//! Find ledgers with specific t values:
//! ```json
//! {"select": ["?ledger", "?t"], "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:t": "?t"}]}
//! ```

use crate::ledger_info::{gs_record_to_jsonld, ns_record_to_jsonld};
use crate::{ApiError, FlureeBuilder, GraphDb, Result};
use fluree_db_nameservice::NameService;
use fluree_db_transact::{CommitOpts, TxnOpts, TxnType};
use serde_json::{json, Value as JsonValue};

/// Execute a query against all nameservice records.
///
/// Creates a temporary in-memory database from all nameservice records
/// (both ledgers and graph sources) and executes the query against it.
///
/// This is useful for ledger discovery, finding branches, or querying
/// metadata across all managed databases.
///
/// # Arguments
///
/// * `nameservice` - The nameservice to query
/// * `query_json` - JSON-LD query to execute
///
/// # Returns
///
/// Query results as formatted JSON-LD.
///
/// # Example
///
/// ```ignore
/// // Find all ledgers
/// let query = json!({
///     "select": ["?ledger"],
///     "where": [{"@id": "?ns", "f:ledger": "?ledger"}]
/// });
/// let results = query_nameservice(&nameservice, &query).await?;
/// ```
pub async fn query_nameservice<N>(nameservice: &N, query_json: &JsonValue) -> Result<JsonValue>
where
    N: NameService,
{
    // 1. Get all ledger records
    let ledger_records = nameservice.all_records().await?;

    // 2. Get all graph source records
    let gs_records = nameservice.all_graph_source_records().await?;

    // 3. Convert to JSON-LD
    let mut all_records: Vec<JsonValue> = ledger_records.iter().map(ns_record_to_jsonld).collect();

    all_records.extend(gs_records.iter().map(gs_record_to_jsonld));

    // 4. If no records, return empty result immediately
    if all_records.is_empty() {
        // Return empty array for empty nameservice
        return Ok(json!([]));
    }

    // 5. Create temporary in-memory Fluree instance
    let temp_fluree = FlureeBuilder::memory().build_memory();

    // 6. Create temporary ledger
    let ledger = temp_fluree
        .create_ledger("ns-query")
        .await
        .map_err(|e| ApiError::internal(format!("Failed to create temp ledger: {e}")))?;

    // 7. Insert all records as JSON-LD transaction
    let txn_json = json!({ "@graph": all_records });
    let index_config = crate::server_defaults::default_index_config();

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

    // 8. Execute query against the populated ledger via normal GraphDb query path
    let db = GraphDb::from_ledger_state(&result.ledger);
    let query_result = temp_fluree
        .query(&db, query_json)
        .await
        .map_err(|e| ApiError::query(format!("Nameservice query failed: {e}")))?;

    query_result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .map_err(|e| ApiError::query(format!("Nameservice query failed: {e}")))

    // temp_fluree is dropped here - automatic cleanup
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{ContentId, ContentKind};
    use fluree_db_nameservice::{
        memory::MemoryNameService, GraphSourcePublisher, GraphSourceType, Publisher,
    };

    async fn setup_ns_with_records() -> MemoryNameService {
        let ns = MemoryNameService::new();

        // Create some ledger records
        let cid1 = ContentId::new(ContentKind::Commit, b"commit-1");
        let cid2 = ContentId::new(ContentKind::Commit, b"commit-2");
        let cid3 = ContentId::new(ContentKind::Commit, b"commit-3");
        ns.publish_commit("db1:main", 10, &cid1).await.unwrap();
        ns.publish_commit("db1:dev", 5, &cid2).await.unwrap();
        ns.publish_commit("db2:main", 20, &cid3).await.unwrap();

        // Create a graph source record
        ns.publish_graph_source(
            "my-search",
            "main",
            GraphSourceType::Bm25,
            r#"{"k1":1.2}"#,
            &["db1:main".to_string()],
        )
        .await
        .unwrap();

        ns
    }

    #[tokio::test]
    async fn test_query_all_ledgers() {
        let ns = setup_ns_with_records().await;

        let query = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?ledger"],
            "where": [{"@id": "?ns", "@type": "f:LedgerSource", "f:ledger": "?ledger"}]
        });

        let result = query_nameservice(&ns, &query).await.unwrap();
        let arr = result.as_array().expect("Expected array result");

        // Should have 3 ledger records (db1:main, db1:dev, db2:main)
        assert_eq!(arr.len(), 3);
    }

    #[tokio::test]
    async fn test_query_ledgers_by_branch() {
        let ns = setup_ns_with_records().await;

        let query = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?ledger"],
            "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:branch": "main"}]
        });

        let result = query_nameservice(&ns, &query).await.unwrap();
        let arr = result.as_array().expect("Expected array result");

        // Should have 2 ledgers on main branch (db1 and db2)
        assert_eq!(arr.len(), 2);
    }

    #[tokio::test]
    async fn test_query_graph_sources() {
        let ns = setup_ns_with_records().await;

        let query = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?name"],
            "where": [{"@id": "?gs", "@type": "f:IndexSource", "f:name": "?name"}]
        });

        let result = query_nameservice(&ns, &query).await.unwrap();
        let arr = result.as_array().expect("Expected array result");

        // Should have 1 graph source (my-search)
        assert_eq!(arr.len(), 1);
    }

    #[tokio::test]
    async fn test_query_empty_nameservice() {
        let ns = MemoryNameService::new();

        let query = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?ledger"],
            "where": [{"@id": "?ns", "f:ledger": "?ledger"}]
        });

        let result = query_nameservice(&ns, &query).await.unwrap();
        assert_eq!(result, json!([]));
    }

    #[tokio::test]
    async fn test_query_ledger_with_t_values() {
        let ns = setup_ns_with_records().await;

        let query = json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "select": ["?ledger", "?t"],
            "where": [{"@id": "?ns", "@type": "f:LedgerSource", "f:ledger": "?ledger", "f:t": "?t"}],
            "orderBy": [{"var": "?t", "desc": true}]
        });

        let result = query_nameservice(&ns, &query).await.unwrap();
        let arr = result.as_array().expect("Expected array result");

        // Should have 3 results ordered by t descending
        assert_eq!(arr.len(), 3);

        // First result should be db2:main with t=20
        // (The exact format depends on the query output format)
    }
}
