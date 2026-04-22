//! End-to-end tutorial integration test
//!
//! Exercises the workflow from docs/getting-started/tutorial-end-to-end.md
//! to ensure all documented examples produce correct results.
//!
//! Covers: insert (JSON-LD + Turtle), SPARQL queries, JSON-LD queries,
//! fulltext search, time travel, branching, merge, and update transactions.

mod support;

use fluree_db_api::{ConflictStrategy, FlureeBuilder};
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/",
        "f": "https://ns.flur.ee/db#"
    })
}

fn extract_sorted_strings(rows: &JsonValue) -> Vec<String> {
    let mut names: Vec<String> = rows
        .as_array()
        .expect("expected array")
        .iter()
        .map(|r| {
            r.as_str()
                .map(std::string::ToString::to_string)
                .or_else(|| {
                    r.as_array().and_then(|a| {
                        a.first()
                            .and_then(|v| v.as_str())
                            .map(std::string::ToString::to_string)
                    })
                })
                .expect("each row should contain a string")
        })
        .collect();
    names.sort();
    names
}

// =========================================================================
// Step 1: Create ledger and insert data (JSON-LD)
// =========================================================================

#[tokio::test]
async fn tutorial_step1_insert_and_query() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("knowledge-base").await.unwrap();

    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "schema:Person",
                "schema:name": "Alice Chen",
                "ex:role": "engineer",
                "ex:team": "platform"
            },
            {
                "@id": "ex:bob",
                "@type": "schema:Person",
                "schema:name": "Bob Martinez",
                "ex:role": "engineer",
                "ex:team": "platform"
            },
            {
                "@id": "ex:carol",
                "@type": "schema:Person",
                "schema:name": "Carol White",
                "ex:role": "manager",
                "ex:team": "platform"
            },
            {
                "@id": "ex:doc1",
                "@type": "ex:Article",
                "schema:name": "Deployment Runbook",
                "schema:author": {"@id": "ex:alice"},
                "ex:team": "platform",
                "ex:visibility": "internal",
                "ex:content": {
                    "@value": "Step 1: Check the monitoring dashboard. Step 2: Run the database migration script. Step 3: Deploy the new container image using the CI pipeline.",
                    "@type": "@fulltext"
                }
            },
            {
                "@id": "ex:doc2",
                "@type": "ex:Article",
                "schema:name": "Onboarding Guide",
                "schema:author": {"@id": "ex:bob"},
                "ex:team": "platform",
                "ex:visibility": "public",
                "ex:content": {
                    "@value": "Welcome to the platform team. This guide covers setting up your development environment, accessing the database, and deploying your first service.",
                    "@type": "@fulltext"
                }
            },
            {
                "@id": "ex:doc3",
                "@type": "ex:Article",
                "schema:name": "Incident Response Playbook",
                "schema:author": {"@id": "ex:carol"},
                "ex:team": "platform",
                "ex:visibility": "confidential",
                "ex:content": {
                    "@value": "During a production incident, the on-call engineer should check database health, review recent deployments, and escalate if the service is not recovering within 15 minutes.",
                    "@type": "@fulltext"
                }
            }
        ]
    });

    let result = fluree.insert(ledger, &txn).await.expect("initial insert");
    assert_eq!(result.receipt.t, 1, "first transaction should be t=1");
    let ledger = result.ledger;

    // Verify: query all articles with SPARQL
    let sparql = r"
        PREFIX schema: <http://schema.org/>
        PREFIX ex: <http://example.org/>

        SELECT ?title ?visibility
        WHERE {
            ?doc a ex:Article ;
                 schema:name ?title ;
                 ex:visibility ?visibility .
        }
        ORDER BY ?title
    ";

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("sparql query");
    let rows = result.to_jsonld(&ledger.snapshot).expect("format");

    let titles: Vec<String> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r[0].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        titles,
        vec![
            "Deployment Runbook",
            "Incident Response Playbook",
            "Onboarding Guide"
        ]
    );

    // Verify: query articles with JSON-LD
    let query = json!({
        "@context": ctx(),
        "select": ["?title"],
        "where": [
            {"@id": "?doc", "@type": "ex:Article", "schema:name": "?title"}
        ],
        "orderBy": ["?title"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("jsonld query");
    let rows = result.to_jsonld(&ledger.snapshot).expect("format");
    let jsonld_titles = extract_sorted_strings(&rows);
    assert_eq!(
        jsonld_titles,
        vec![
            "Deployment Runbook",
            "Incident Response Playbook",
            "Onboarding Guide"
        ]
    );

    // Verify: query with join (articles + authors)
    let join_query = json!({
        "@context": ctx(),
        "select": ["?title", "?author_name"],
        "where": [
            {
                "@id": "?doc", "@type": "ex:Article",
                "schema:name": "?title",
                "schema:author": "?author"
            },
            {"@id": "?author", "schema:name": "?author_name"}
        ],
        "orderBy": ["?title"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &join_query)
        .await
        .expect("join query");
    let rows = result.to_jsonld(&ledger.snapshot).expect("format");
    let arr = rows.as_array().unwrap();
    assert_eq!(arr.len(), 3, "should have 3 articles with authors");
    assert_eq!(arr[0][0], "Deployment Runbook");
    assert_eq!(arr[0][1], "Alice Chen");
}

// =========================================================================
// Step 2: Fulltext search
// =========================================================================

#[tokio::test]
async fn tutorial_step2_fulltext_search() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("kb-search").await.unwrap();

    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:doc1", "@type": "ex:Article",
                "schema:name": "Deployment Runbook",
                "ex:visibility": "internal",
                "ex:content": {
                    "@value": "Step 1: Check the monitoring dashboard. Step 2: Run the database migration script. Step 3: Deploy the new container image.",
                    "@type": "@fulltext"
                }
            },
            {
                "@id": "ex:doc2", "@type": "ex:Article",
                "schema:name": "Onboarding Guide",
                "ex:visibility": "public",
                "ex:content": {
                    "@value": "Welcome to the platform team. This guide covers setting up your development environment, accessing the database, and deploying your first service.",
                    "@type": "@fulltext"
                }
            },
            {
                "@id": "ex:doc3", "@type": "ex:Article",
                "schema:name": "Incident Response Playbook",
                "ex:visibility": "confidential",
                "ex:content": {
                    "@value": "During a production incident, the on-call engineer should check database health, review recent deployments, and escalate.",
                    "@type": "@fulltext"
                }
            }
        ]
    });

    let ledger = fluree.insert(ledger, &txn).await.expect("insert").ledger;

    // Search for "database deployment"
    let search_query = json!({
        "@context": ctx(),
        "select": ["?title", "?score"],
        "where": [
            {"@id": "?doc", "@type": "ex:Article", "ex:content": "?content", "schema:name": "?title"},
            ["bind", "?score", "(fulltext ?content \"database deployment\")"],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]],
        "limit": 10
    });

    let result = support::query_jsonld(&fluree, &ledger, &search_query)
        .await
        .expect("fulltext query");
    let rows = result.to_jsonld(&ledger.snapshot).expect("format");
    let arr = rows.as_array().unwrap();

    assert!(
        !arr.is_empty(),
        "should find at least one article matching 'database deployment'"
    );

    // All scores should be positive
    for row in arr {
        let score = row[1].as_f64().expect("score should be numeric");
        assert!(score > 0.0, "scores should be positive");
    }

    // Search with filter: only public articles
    let filtered_query = json!({
        "@context": ctx(),
        "select": ["?title", "?score"],
        "where": [
            {
                "@id": "?doc", "@type": "ex:Article",
                "ex:content": "?content",
                "schema:name": "?title",
                "ex:visibility": "public"
            },
            ["bind", "?score", "(fulltext ?content \"database deployment\")"],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]]
    });

    let result = support::query_jsonld(&fluree, &ledger, &filtered_query)
        .await
        .expect("filtered fulltext query");
    let rows = result.to_jsonld(&ledger.snapshot).expect("format");
    let arr = rows.as_array().unwrap();

    // Only the onboarding guide is public
    for row in arr {
        assert_eq!(
            extract_title(row),
            "Onboarding Guide",
            "only the public article should match"
        );
    }
}

fn extract_title(row: &JsonValue) -> &str {
    row[0].as_str().unwrap_or_default()
}

/// Extract text from a value that may be a plain string or a typed literal
/// object ({"@value": "...", "@type": "..."}).
fn extract_text(val: &JsonValue) -> &str {
    val.as_str().unwrap_or_else(|| {
        val.get("@value")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
    })
}

// =========================================================================
// Step 3: Update data and time travel
// =========================================================================

#[tokio::test]
async fn tutorial_step3_update_and_time_travel() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("kb-timetravel").await.unwrap();

    // t=1: insert original content
    let txn = json!({
        "@context": ctx(),
        "@graph": [{
            "@id": "ex:doc1", "@type": "ex:Article",
            "schema:name": "Deployment Runbook",
            "ex:content": {
                "@value": "Step 1: Check the dashboard. Step 2: Run migration. Step 3: Deploy.",
                "@type": "@fulltext"
            }
        }]
    });
    let r1 = fluree.insert(ledger, &txn).await.expect("t=1 insert");
    assert_eq!(r1.receipt.t, 1);
    let ledger_t1 = r1.ledger;

    // t=2: update the content
    let update_txn = json!({
        "@context": ctx(),
        "where": {"@id": "ex:doc1", "ex:content": "?old"},
        "delete": {"@id": "ex:doc1", "ex:content": "?old"},
        "insert": {
            "@id": "ex:doc1",
            "ex:content": {
                "@value": "Step 1: Check dashboard and verify health. Step 2: Dry-run migration. Step 3: Deploy. Step 4: Verify in staging.",
                "@type": "@fulltext"
            }
        }
    });
    let r2 = fluree
        .update(ledger_t1, &update_txn)
        .await
        .expect("t=2 update");
    assert_eq!(r2.receipt.t, 2);
    let ledger_t2 = r2.ledger;

    // Query current state: should see updated content
    let query = json!({
        "@context": ctx(),
        "select": ["?content"],
        "where": [{"@id": "ex:doc1", "ex:content": "?content"}]
    });

    let result = support::query_jsonld(&fluree, &ledger_t2, &query)
        .await
        .expect("query current");
    let rows = result.to_jsonld(&ledger_t2.snapshot).expect("format");
    let current_content = extract_text(&rows[0]);
    assert!(
        current_content.contains("Dry-run migration"),
        "current content should be the updated version, got: {current_content}"
    );

    // Query historical state (t=1): should see original content
    let time_travel_query = json!({
        "@context": ctx(),
        "from": ["kb-timetravel:main@t:1"],
        "select": ["?content"],
        "where": [{"@id": "ex:doc1", "ex:content": "?content"}]
    });

    let result = fluree
        .query_connection(&time_travel_query)
        .await
        .expect("time travel query");
    let db = support::graphdb_from_ledger(&ledger_t2);
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format");
    let historical_content = extract_text(&rows[0]);
    assert!(
        !historical_content.contains("Dry-run migration"),
        "historical content should be the original version"
    );
    assert!(
        historical_content.contains("Run migration"),
        "historical content should contain original text"
    );
}

// =========================================================================
// Step 4: Branching
// =========================================================================

#[tokio::test]
async fn tutorial_step4_branch_and_merge() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("kb-branch").await.unwrap();

    // Seed data on main
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:doc1", "@type": "ex:Article",
                "schema:name": "Deployment Runbook",
                "ex:visibility": "internal"
            },
            {
                "@id": "ex:doc2", "@type": "ex:Article",
                "schema:name": "Onboarding Guide",
                "ex:visibility": "public"
            },
            {
                "@id": "ex:doc3", "@type": "ex:Article",
                "schema:name": "Incident Response Playbook",
                "ex:visibility": "confidential"
            }
        ]
    });
    fluree.insert(ledger, &txn).await.expect("seed main");

    // Create branch
    let branch_record = fluree
        .create_branch("kb-branch", "reorganize", None)
        .await
        .expect("create branch");
    assert_eq!(branch_record.branch, "reorganize");
    assert_eq!(branch_record.ledger_id, "kb-branch:reorganize");

    // Verify branch list
    let branches = fluree.list_branches("kb-branch").await.unwrap();
    let mut branch_names: Vec<&str> = branches.iter().map(|b| b.branch.as_str()).collect();
    branch_names.sort();
    assert_eq!(branch_names, vec!["main", "reorganize"]);

    // Transact on branch: add categories
    let branch_ledger = fluree.ledger("kb-branch:reorganize").await.unwrap();
    let branch_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:doc1", "ex:category": "operations"},
            {"@id": "ex:doc2", "ex:category": "onboarding"},
            {"@id": "ex:doc3", "ex:category": "operations"}
        ]
    });
    fluree
        .insert(branch_ledger, &branch_txn)
        .await
        .expect("branch insert");

    // Update visibility on branch
    let branch_ledger = fluree.ledger("kb-branch:reorganize").await.unwrap();
    let branch_update = json!({
        "@context": ctx(),
        "where": {"@id": "ex:doc3", "ex:visibility": "confidential"},
        "delete": {"@id": "ex:doc3", "ex:visibility": "confidential"},
        "insert": {"@id": "ex:doc3", "ex:visibility": "internal"}
    });
    fluree
        .update(branch_ledger, &branch_update)
        .await
        .expect("branch update");

    // Verify: branch has categories
    let branch_ledger = fluree.ledger("kb-branch:reorganize").await.unwrap();
    let cat_query = json!({
        "@context": ctx(),
        "select": ["?title", "?category"],
        "where": [
            {"@id": "?doc", "@type": "ex:Article", "schema:name": "?title", "ex:category": "?category"}
        ],
        "orderBy": ["?title"]
    });
    let result = support::query_jsonld(&fluree, &branch_ledger, &cat_query)
        .await
        .expect("branch category query");
    let rows = result.to_jsonld(&branch_ledger.snapshot).unwrap();
    assert_eq!(
        rows.as_array().unwrap().len(),
        3,
        "branch should have 3 categorized articles"
    );

    // Verify: main does NOT have categories
    let main_ledger = fluree.ledger("kb-branch:main").await.unwrap();
    let result = support::query_jsonld(&fluree, &main_ledger, &cat_query).await;
    match result {
        Ok(res) => {
            let rows = res.to_jsonld(&main_ledger.snapshot).unwrap();
            assert_eq!(
                rows.as_array().unwrap().len(),
                0,
                "main should have no categorized articles"
            );
        }
        Err(_) => {
            // No results is fine — main doesn't have categories
        }
    }

    // Verify: branch changed doc3's visibility
    let branch_ledger = fluree.ledger("kb-branch:reorganize").await.unwrap();
    let vis_query = json!({
        "@context": ctx(),
        "select": ["?vis"],
        "where": [{"@id": "ex:doc3", "ex:visibility": "?vis"}]
    });
    let result = support::query_jsonld(&fluree, &branch_ledger, &vis_query)
        .await
        .expect("branch visibility query");
    let rows = result.to_jsonld(&branch_ledger.snapshot).unwrap();
    assert_eq!(rows[0], "internal", "branch should have updated visibility");

    // Verify: main still has original visibility
    let main_ledger = fluree.ledger("kb-branch:main").await.unwrap();
    let result = support::query_jsonld(&fluree, &main_ledger, &vis_query)
        .await
        .expect("main visibility query");
    let rows = result.to_jsonld(&main_ledger.snapshot).unwrap();
    assert_eq!(
        rows[0], "confidential",
        "main should retain original visibility"
    );

    // Merge branch back to main
    let report = fluree
        .merge_branch("kb-branch", "reorganize", None, ConflictStrategy::default())
        .await
        .expect("merge");
    assert!(report.fast_forward, "should be fast-forward merge");
    assert!(report.commits_copied > 0, "should have copied commits");

    // Verify: main now has categories and updated visibility
    let main_ledger = fluree.ledger("kb-branch:main").await.unwrap();
    let result = support::query_jsonld(&fluree, &main_ledger, &cat_query)
        .await
        .expect("post-merge category query");
    let rows = result.to_jsonld(&main_ledger.snapshot).unwrap();
    assert_eq!(
        rows.as_array().unwrap().len(),
        3,
        "main should have categories after merge"
    );

    let result = support::query_jsonld(&fluree, &main_ledger, &vis_query)
        .await
        .expect("post-merge visibility query");
    let rows = result.to_jsonld(&main_ledger.snapshot).unwrap();
    assert_eq!(
        rows[0], "internal",
        "main should have updated visibility after merge"
    );

    // Drop the branch
    fluree
        .drop_branch("kb-branch", "reorganize")
        .await
        .expect("drop branch");
    let branches = fluree.list_branches("kb-branch").await.unwrap();
    assert_eq!(branches.len(), 1, "only main should remain after drop");
    assert_eq!(branches[0].branch, "main");
}

// =========================================================================
// Step 5: Combined workflow — insert, search, update, time travel, branch
// =========================================================================

#[tokio::test]
async fn tutorial_step5_combined_workflow() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("kb-combined").await.unwrap();

    // t=1: Insert articles with fulltext content
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:doc1", "@type": "ex:Article",
                "schema:name": "Database Migration Guide",
                "ex:content": {
                    "@value": "This guide covers database migration best practices including schema versioning and rollback procedures.",
                    "@type": "@fulltext"
                }
            },
            {
                "@id": "ex:doc2", "@type": "ex:Article",
                "schema:name": "API Design Patterns",
                "ex:content": {
                    "@value": "REST API design patterns for scalable web services including pagination, filtering, and error handling.",
                    "@type": "@fulltext"
                }
            }
        ]
    });
    let r1 = fluree.insert(ledger, &txn).await.expect("t=1");
    let ledger = r1.ledger;

    // Search: find database-related articles
    let search_q = json!({
        "@context": ctx(),
        "select": ["?title", "?score"],
        "where": [
            {"@id": "?doc", "@type": "ex:Article", "ex:content": "?content", "schema:name": "?title"},
            ["bind", "?score", "(fulltext ?content \"database migration\")"],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]]
    });
    let result = support::query_jsonld(&fluree, &ledger, &search_q)
        .await
        .expect("search");
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();
    assert!(!arr.is_empty(), "should find database migration article");
    assert_eq!(
        arr[0][0].as_str().unwrap(),
        "Database Migration Guide",
        "migration guide should rank highest"
    );

    // t=2: Update article
    let update = json!({
        "@context": ctx(),
        "where": {"@id": "ex:doc1", "ex:content": "?old"},
        "delete": {"@id": "ex:doc1", "ex:content": "?old"},
        "insert": {
            "@id": "ex:doc1",
            "ex:content": {
                "@value": "Updated: database migration guide with zero-downtime deployment strategies and blue-green migration patterns.",
                "@type": "@fulltext"
            }
        }
    });
    let r2 = fluree.update(ledger, &update).await.expect("t=2 update");
    let ledger = r2.ledger;

    // Time travel: query at t=1 should have original content
    let tt_query = json!({
        "@context": ctx(),
        "from": ["kb-combined:main@t:1"],
        "select": ["?content"],
        "where": [{"@id": "ex:doc1", "ex:content": "?content"}]
    });
    let result = fluree
        .query_connection(&tt_query)
        .await
        .expect("time travel");
    let db = support::graphdb_from_ledger(&ledger);
    let rows = result.to_jsonld_async(db.as_graph_db_ref()).await.unwrap();
    let old = extract_text(&rows[0]);
    assert!(
        old.contains("rollback procedures"),
        "t=1 should have original text, got: {old}"
    );
    assert!(
        !old.contains("zero-downtime"),
        "t=1 should not have updated text"
    );

    // Branch: create experiment
    fluree
        .create_branch("kb-combined", "experiment", None)
        .await
        .expect("create branch");

    // Add data on branch
    let exp_ledger = fluree.ledger("kb-combined:experiment").await.unwrap();
    let exp_txn = json!({
        "@context": ctx(),
        "@graph": [{"@id": "ex:doc3", "@type": "ex:Article", "schema:name": "Experimental Feature"}]
    });
    fluree
        .insert(exp_ledger, &exp_txn)
        .await
        .expect("branch insert");

    // Main shouldn't see the experiment
    let main_ledger = fluree.ledger("kb-combined:main").await.unwrap();
    let count_q = json!({
        "@context": ctx(),
        "select": ["?title"],
        "where": [{"@id": "?doc", "@type": "ex:Article", "schema:name": "?title"}],
        "orderBy": ["?title"]
    });
    let result = support::query_jsonld(&fluree, &main_ledger, &count_q)
        .await
        .expect("main count");
    let rows = result.to_jsonld(&main_ledger.snapshot).unwrap();
    assert_eq!(
        rows.as_array().unwrap().len(),
        2,
        "main should have 2 articles (experiment not visible)"
    );

    // Branch should see 3
    let exp_ledger = fluree.ledger("kb-combined:experiment").await.unwrap();
    let result = support::query_jsonld(&fluree, &exp_ledger, &count_q)
        .await
        .expect("branch count");
    let rows = result.to_jsonld(&exp_ledger.snapshot).unwrap();
    assert_eq!(
        rows.as_array().unwrap().len(),
        3,
        "branch should have 3 articles"
    );

    // Merge experiment into main
    let report = fluree
        .merge_branch(
            "kb-combined",
            "experiment",
            None,
            ConflictStrategy::default(),
        )
        .await
        .expect("merge");
    assert!(report.fast_forward);

    // After merge, main should see all 3
    let main_ledger = fluree.ledger("kb-combined:main").await.unwrap();
    let result = support::query_jsonld(&fluree, &main_ledger, &count_q)
        .await
        .expect("post-merge count");
    let rows = result.to_jsonld(&main_ledger.snapshot).unwrap();
    assert_eq!(
        rows.as_array().unwrap().len(),
        3,
        "main should have 3 articles after merge"
    );
}

// =========================================================================
// Turtle insert
// =========================================================================

#[tokio::test]
async fn tutorial_turtle_insert() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("kb-turtle").await.unwrap();

    let turtle = r#"
        @prefix schema: <http://schema.org/> .
        @prefix ex:     <http://example.org/> .
        @prefix f:      <https://ns.flur.ee/db#> .

        ex:alice a schema:Person ;
            schema:name "Alice Chen" ;
            ex:role     "engineer" .

        ex:doc1 a ex:Article ;
            schema:name   "Deployment Runbook" ;
            schema:author ex:alice ;
            ex:content    "Check monitoring, run migration, deploy container."^^f:fullText .
    "#;

    let result = fluree
        .stage_owned(ledger)
        .insert_turtle(turtle)
        .execute()
        .await
        .expect("turtle insert");
    let ledger = result.ledger;

    // Verify the turtle data was inserted
    let query = json!({
        "@context": {
            "schema": "http://schema.org/",
            "ex": "http://example.org/"
        },
        "select": ["?name"],
        "where": [{"@id": "ex:alice", "schema:name": "?name"}]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query turtle data");
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    assert_eq!(rows[0], "Alice Chen");

    // Verify fulltext was indexed
    let search_q = json!({
        "@context": {
            "schema": "http://schema.org/",
            "ex": "http://example.org/"
        },
        "select": ["?title", "?score"],
        "where": [
            {"@id": "?doc", "@type": "ex:Article", "ex:content": "?content", "schema:name": "?title"},
            ["bind", "?score", "(fulltext ?content \"monitoring deployment\")"],
            ["filter", "(> ?score 0)"]
        ]
    });
    let result = support::query_jsonld(&fluree, &ledger, &search_q)
        .await
        .expect("fulltext on turtle data");
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    assert!(
        !rows.as_array().unwrap().is_empty(),
        "should find turtle-inserted article via fulltext"
    );
}

// =========================================================================
// SPARQL query equivalence
// =========================================================================

#[tokio::test]
async fn tutorial_sparql_jsonld_equivalence() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("kb-equiv").await.unwrap();

    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "schema:Person", "schema:name": "Alice", "ex:team": "platform"},
            {"@id": "ex:bob", "@type": "schema:Person", "schema:name": "Bob", "ex:team": "platform"},
            {"@id": "ex:carol", "@type": "schema:Person", "schema:name": "Carol", "ex:team": "marketing"}
        ]
    });
    let ledger = fluree.insert(ledger, &txn).await.expect("insert").ledger;

    // SPARQL query
    let sparql = r#"
        PREFIX schema: <http://schema.org/>
        PREFIX ex: <http://example.org/>

        SELECT ?name
        WHERE {
            ?person a schema:Person ;
                    schema:name ?name ;
                    ex:team "platform" .
        }
        ORDER BY ?name
    "#;
    let sparql_result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("sparql");
    let sparql_rows = sparql_result.to_jsonld(&ledger.snapshot).unwrap();

    // JSON-LD equivalent
    let jsonld_q = json!({
        "@context": ctx(),
        "select": ["?name"],
        "where": [
            {"@id": "?person", "@type": "schema:Person", "schema:name": "?name", "ex:team": "platform"}
        ],
        "orderBy": ["?name"]
    });
    let jsonld_result = support::query_jsonld(&fluree, &ledger, &jsonld_q)
        .await
        .expect("jsonld");
    let jsonld_rows = jsonld_result.to_jsonld(&ledger.snapshot).unwrap();

    // Both should return Alice and Bob
    let sparql_names = extract_sorted_strings(&sparql_rows);
    let jsonld_names = extract_sorted_strings(&jsonld_rows);
    assert_eq!(sparql_names, vec!["Alice", "Bob"]);
    assert_eq!(jsonld_names, vec!["Alice", "Bob"]);
    assert_eq!(
        sparql_names, jsonld_names,
        "SPARQL and JSON-LD should return identical results"
    );
}
