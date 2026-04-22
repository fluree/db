//! Transact list container serialization integration tests
//!
//! Tests RDF @list container serialization and persistence.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn list_container_serialization_test() {
    // Create a temporary directory for file-backed storage
    let temp_dir = TempDir::new().unwrap();
    let test_dir_str = temp_dir.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(test_dir_str)
        .build()
        .expect("build file fluree");
    let ledger_id = "crm/test:main";

    // Create ledger
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    // Transaction with @list container (reproducing the issue)
    let txn = json!({
        "@context": {
            "crm": "https://data.flur.ee/SampleUnifiedCRMModel/",
            "crm:companyIds": {"@container": "@list"}
        },
        "insert": [{
            "@id": "crm:contact/contact-final",
            "@type": ["crm:Contact"],
            "crm:companyIds": ["company-final"]
        }]
    });

    // Stage and commit the transaction
    let _ledger1 = fluree.update(ledger0, &txn).await.unwrap().ledger;

    // Create second connection to test loading from disk
    let fluree2 = FlureeBuilder::file(test_dir_str)
        .build()
        .expect("build file fluree2");

    // Try to load the database - this should not fail
    let loaded_ledger = fluree2.ledger(ledger_id).await.unwrap();

    // Query to verify data was loaded correctly
    let query = json!({
        "@context": {"crm": "https://data.flur.ee/SampleUnifiedCRMModel/"},
        "select": {"crm:contact/contact-final": ["*"]}
    });

    let result = support::query_jsonld(&fluree2, &loaded_ledger, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(loaded_ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // Verify the data was correctly saved and loaded
    assert!(
        !jsonld.is_null(),
        "Database should load successfully from disk"
    );
    assert_eq!(
        jsonld.as_array().unwrap().len(),
        1,
        "Should have one contact record"
    );

    // When querying a single-value list, Fluree returns the value directly, not as a list
    let contact = &jsonld.as_array().unwrap()[0];
    assert_eq!(
        contact["crm:companyIds"], "company-final",
        "Single list value should be returned directly"
    );
}

#[tokio::test]
async fn list_container_multiple_values_test() {
    // Create a temporary directory for file-backed storage
    let temp_dir = TempDir::new().unwrap();
    let test_dir_str = temp_dir.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(test_dir_str)
        .build()
        .expect("build file fluree");
    let ledger_id = "test/lists:main";

    // Create ledger
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    // Transaction with @list containing multiple values
    let txn = json!({
        "@context": [
            support::default_context(),
            {
                "ex": "http://example.org/ns/",
                "ex:orderedItems": {"@container": "@list"}
            }
        ],
        "insert": {
            "@id": "ex:thing1",
            "ex:orderedItems": ["first", "second", "third"]
        }
    });

    let _ledger1 = fluree.update(ledger0, &txn).await.unwrap().ledger;

    // Load with new connection
    let fluree2 = FlureeBuilder::file(test_dir_str)
        .build()
        .expect("build file fluree2");
    let loaded_ledger = fluree2.ledger(ledger_id).await.unwrap();

    let query = json!({
        "@context": [
            support::default_context(),
            {"ex": "http://example.org/ns/"}
        ],
        "select": {"ex:thing1": ["*"]}
    });

    let result = support::query_jsonld(&fluree2, &loaded_ledger, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(loaded_ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    assert!(!jsonld.is_null(), "Database should load successfully");
    let thing = &jsonld.as_array().unwrap()[0];
    assert_eq!(
        thing["ex:orderedItems"],
        json!(["first", "second", "third"]),
        "Ordered list values should be preserved"
    );
}

#[tokio::test]
async fn list_container_with_objects_test() {
    // Create a temporary directory for file-backed storage
    let temp_dir = TempDir::new().unwrap();
    let test_dir_str = temp_dir.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(test_dir_str)
        .build()
        .expect("build file fluree");
    let ledger_id = "test/list-objects:main";

    // Create ledger
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    // Transaction with @list containing object references
    let txn = json!({
        "@context": [
            support::default_context(),
            {
                "ex": "http://example.org/ns/",
                "ex:orderedFriends": {"@container": "@list"}
            }
        ],
        "insert": [
            {"@id": "ex:alice", "schema:name": "Alice"},
            {"@id": "ex:bob", "schema:name": "Bob"},
            {
                "@id": "ex:charlie",
                "schema:name": "Charlie",
                "ex:orderedFriends": [{"@id": "ex:alice"}, {"@id": "ex:bob"}]
            }
        ]
    });

    let _ledger1 = fluree.update(ledger0, &txn).await.unwrap().ledger;

    // Load with new connection
    let fluree2 = FlureeBuilder::file(test_dir_str)
        .build()
        .expect("build file fluree2");
    let loaded_ledger = fluree2.ledger(ledger_id).await.unwrap();

    let query = json!({
        "@context": [
            support::default_context(),
            {"ex": "http://example.org/ns/"}
        ],
        "select": {"ex:charlie": ["*", {"ex:orderedFriends": ["*"]}]}
    });

    let result = support::query_jsonld(&fluree2, &loaded_ledger, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(loaded_ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    assert!(!jsonld.is_null(), "Database should load successfully");

    let charlie = &jsonld.as_array().unwrap()[0];
    let friends = charlie["ex:orderedFriends"].as_array().unwrap();

    assert_eq!(friends.len(), 2, "Should have two ordered friends");
    assert_eq!(
        friends[0]["schema:name"], "Alice",
        "First friend should be Alice"
    );
    assert_eq!(
        friends[1]["schema:name"], "Bob",
        "Second friend should be Bob"
    );
}

/// Tests that @list can contain inline blank node objects (not just @id refs or scalars).
/// This exercises the transaction parser's ability to parse nested objects within @list.
#[tokio::test]
async fn list_container_with_blank_node_objects_test() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/list-blank-nodes:main";

    let ledger = fluree.create_ledger(ledger_id).await.unwrap();

    // Transaction with @list containing inline blank node objects (no @id)
    let txn = json!({
        "@context": [
            support::default_context(),
            {
                "ex": "http://example.org/ns/",
                "ex:steps": {"@container": "@list"}
            }
        ],
        "insert": {
            "@id": "ex:recipe1",
            "schema:name": "Pasta",
            "ex:steps": [
                {"schema:name": "Boil water", "ex:duration": 10},
                {"schema:name": "Add pasta", "ex:duration": 8},
                {"schema:name": "Drain", "ex:duration": 1}
            ]
        }
    });

    let ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

    // Query to verify the blank node objects were persisted with their properties
    let query = json!({
        "@context": [
            support::default_context(),
            {"ex": "http://example.org/ns/"}
        ],
        "select": {"ex:recipe1": ["*", {"ex:steps": ["*"]}]}
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    let recipe = &jsonld.as_array().unwrap()[0];
    let steps = recipe["ex:steps"].as_array().unwrap();

    assert_eq!(steps.len(), 3, "Should have three steps");
    assert_eq!(steps[0]["schema:name"], "Boil water", "First step name");
    assert_eq!(steps[1]["schema:name"], "Add pasta", "Second step name");
    assert_eq!(steps[2]["schema:name"], "Drain", "Third step name");
}
