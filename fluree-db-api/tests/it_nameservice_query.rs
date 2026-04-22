//! Nameservice query integration tests
//!

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use std::collections::HashSet;
use support::assert_index_defaults;

fn extract_first_string(v: &serde_json::Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        return Some(s.to_string());
    }
    if let Some(arr) = v.as_array() {
        return arr
            .first()
            .and_then(|x| x.as_str())
            .map(std::string::ToString::to_string);
    }
    if let Some(obj) = v.as_object() {
        return obj
            .get("@id")
            .and_then(|x| x.as_str())
            .map(std::string::ToString::to_string);
    }
    None
}

async fn create_and_insert(fluree: &fluree_db_api::Fluree, ledger: &str, name: &str) {
    let ledger_state = fluree.create_ledger(ledger).await.expect("create_ledger");
    let tx = json!({
        "@context": {"test":"http://example.org/test#"},
        "@graph": [{"@id": format!("test:{name}"), "@type":"Person", "name": name}]
    });
    let _ = fluree.insert(ledger_state, &tx).await.expect("insert");
}

#[tokio::test]
async fn nameservice_query_memory_parity() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create 3 ledgers, with ledger-three having 2 commits.
    create_and_insert(&fluree, "ledger-one", "Alice").await;
    create_and_insert(&fluree, "ledger-two", "Bob").await;

    let ledger_three = fluree
        .create_ledger("ledger-three")
        .await
        .expect("create ledger-three");
    let tx1 = json!({
        "@context": {"test":"http://example.org/test#"},
        "@graph": [{"@id":"test:person3","@type":"Person","name":"Charlie"}]
    });
    let tx2 = json!({
        "@context": {"test":"http://example.org/test#"},
        "@graph": [{"@id":"test:person4","@type":"Person","name":"David"}]
    });
    let ledger_three = fluree
        .insert(ledger_three, &tx1)
        .await
        .expect("insert 1")
        .ledger;
    let _ledger_three = fluree
        .insert(ledger_three, &tx2)
        .await
        .expect("insert 2")
        .ledger;

    // Query for database records ("Query for specific ledger information")
    let db_query = json!({
        "@context": {"f":"https://ns.flur.ee/db#"},
        "select": {"?ns": ["f:ledger", "f:branch", "f:t"]},
        "where": [{"@id":"?ns","@type":"f:LedgerSource"}]
    });
    let db_result = fluree
        .query_nameservice(&db_query)
        .await
        .expect("query_nameservice");
    let db_arr = db_result.as_array().expect("array");
    assert!(db_arr.len() >= 3, "expected >= 3 database records");

    // Query for ledgers on main branch
    let branch_query = json!({
        "@context": {"f":"https://ns.flur.ee/db#"},
        "select": ["?ledger"],
        "where": [{"@id":"?ns","@type":"f:LedgerSource","f:ledger":"?ledger","f:branch":"main"}]
    });
    let branch_result = fluree
        .query_nameservice(&branch_query)
        .await
        .expect("query_nameservice");
    let branch_arr = branch_result.as_array().expect("array");
    assert!(branch_arr.len() >= 3, "expected >= 3 ledgers on main");

    let ledger_names: HashSet<String> =
        branch_arr.iter().filter_map(extract_first_string).collect();
    assert_eq!(
        ledger_names,
        HashSet::from([
            "ledger-one".to_string(),
            "ledger-two".to_string(),
            "ledger-three".to_string()
        ])
    );

    // Query for ledger t values; ensure ledger-three has t >= 2
    let t_query = json!({
        "@context": {"f":"https://ns.flur.ee/db#"},
        "select": ["?ledger","?t"],
        "where": [{"@id":"?ns","f:ledger":"?ledger","f:t":"?t"}]
    });
    let t_result = fluree
        .query_nameservice(&t_query)
        .await
        .expect("query_nameservice");
    let t_arr = t_result.as_array().expect("array");
    assert!(t_arr.len() >= 3, "expected >= 3 ledger t rows");
    let ledger_three_t = t_arr
        .iter()
        .filter_map(|row| row.as_array())
        .find(|row| row.first().and_then(|v| v.as_str()) == Some("ledger-three"))
        .and_then(|row| row.get(1))
        .and_then(serde_json::Value::as_i64)
        .expect("ledger-three t");
    assert!(
        ledger_three_t >= 2,
        "expected ledger-three t >= 2, got {ledger_three_t}"
    );

    // Query with no results
    let none_query = json!({
        "@context": {"f":"https://ns.flur.ee/db#"},
        "select": ["?ledger"],
        "where": [{"@id":"?ns","f:ledger":"?ledger","f:branch":"nonexistent-branch"}]
    });
    let none_result = fluree
        .query_nameservice(&none_query)
        .await
        .expect("query_nameservice");
    assert_eq!(none_result, json!([]));
}

#[tokio::test]
async fn nameservice_query_file_storage_parity() {
    assert_index_defaults();
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build file fluree");

    let ledger = fluree
        .create_ledger("file-ledger")
        .await
        .expect("create_ledger");
    let tx = json!({
        "@context": {"test":"http://example.org/test#"},
        "@graph": [{"@id":"test:file-person","@type":"Person","name":"File User"}]
    });
    let _ = fluree.insert(ledger, &tx).await.expect("insert");

    let query = json!({
        "@context": {"f":"https://ns.flur.ee/db#"},
        "select": ["?ledger","?t"],
        "where": [{"@id":"?ns","f:ledger":"?ledger","f:t":"?t"}]
    });
    let result = fluree
        .query_nameservice(&query)
        .await
        .expect("query_nameservice");
    let arr = result.as_array().expect("array");
    assert!(!arr.is_empty(), "expected at least 1 ledger");
    let file_ledger_rows = arr
        .iter()
        .filter_map(|row| row.as_array())
        .filter(|row| row.first().and_then(|v| v.as_str()) == Some("file-ledger"))
        .count();
    assert_eq!(file_ledger_rows, 1, "expected exactly 1 file-ledger row");
}

#[tokio::test]
async fn nameservice_slash_ledger_names_parity() {
    assert_index_defaults();
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build file fluree");

    // Create ledgers with '/' in their names and insert data.
    let l1 = fluree
        .create_ledger("tenant1/customers")
        .await
        .expect("create");
    let l2 = fluree
        .create_ledger("tenant1/products")
        .await
        .expect("create");
    let l3 = fluree
        .create_ledger("tenant2/orders")
        .await
        .expect("create");

    let _ = fluree
        .insert(
            l1,
            &json!({"@context":{"test":"http://example.org/test#"},"@graph":[{"@id":"test:customer1","@type":"Customer","name":"ACME Corp"}]}),
        )
        .await
        .expect("insert");
    let _ = fluree
        .insert(
            l2,
            &json!({"@context":{"test":"http://example.org/test#"},"@graph":[{"@id":"test:product1","@type":"Product","name":"Widget"}]}),
        )
        .await
        .expect("insert");
    let _ = fluree
        .insert(
            l3,
            &json!({"@context":{"test":"http://example.org/test#"},"@graph":[{"@id":"test:order1","@type":"Order","total":100}]}),
        )
        .await
        .expect("insert");

    // Query all ledger names
    let query = json!({
        "@context": {"f":"https://ns.flur.ee/db#"},
        "select": ["?ledger"],
        "where": [{"@id":"?ns","@type":"f:LedgerSource","f:ledger":"?ledger"}]
    });
    let result = fluree
        .query_nameservice(&query)
        .await
        .expect("query_nameservice");
    let arr = result.as_array().expect("array");
    assert!(
        arr.len() >= 3,
        "expected >= 3 ledgers, got {}: {result}",
        arr.len()
    );

    let names: HashSet<String> = arr.iter().filter_map(extract_first_string).collect();
    assert!(names.contains("tenant1/customers"));
    assert!(names.contains("tenant1/products"));
    assert!(names.contains("tenant2/orders"));

    // Verify filesystem layout: ns@v2/{ledger-name}/{branch}.json
    let ns_dir = tmp.path().join("ns@v2");
    assert!(ns_dir.exists(), "ns@v2 directory should exist");
    assert!(
        ns_dir.join("tenant1").exists(),
        "tenant1 subdirectory should exist"
    );
    assert!(
        ns_dir.join("tenant2").exists(),
        "tenant2 subdirectory should exist"
    );

    assert!(ns_dir.join("tenant1/customers/main.json").exists());
    assert!(ns_dir.join("tenant1/products/main.json").exists());
    assert!(ns_dir.join("tenant2/orders/main.json").exists());
}
