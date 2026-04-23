//! Datalog rule integration tests
//!
//! These tests validate user-defined datalog rules (f:rule predicate).
//! Rules use `where`/`insert` patterns to derive new facts during query execution.
//!
//! Test coverage:
//! - Basic grandparent rule (2-hop traversal)
//! - Rule with multiple where patterns
//! - Fixpoint iteration (rules triggering other rules)

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows};

// =============================================================================
// Basic Datalog Rule Tests
// =============================================================================

#[tokio::test]
async fn datalog_grandparent_rule() {
    // Test: Define a grandparent rule that derives grandparent relationships
    // from parent-of-parent chains.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/grandparent");

    // First, insert the rule definition
    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:grandparentRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": {"@id": "?person", "ex:parent": {"ex:parent": "?grandparent"}},
                        "insert": {"@id": "?person", "ex:grandparent": {"@id": "?grandparent"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    // Verify the rule was stored correctly by querying for it
    let rule_check = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "select": ["?rule", "?ruleValue"],
        "where": {"@id": "?rule", "f:rule": "?ruleValue"}
    });
    let rule_rows = support::query_jsonld(&fluree, &ledger, &rule_check)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let rule_results = normalize_rows(&rule_rows);
    eprintln!("Found rules: {rule_results:?}");
    assert!(
        !rule_results.is_empty(),
        "Should have found the rule definition"
    );

    // Insert family data
    let family_data = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    let ledger = fluree.insert(ledger, &family_data).await.unwrap().ledger;

    // Verify the family data was stored correctly
    let data_check = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?parent",
        "where": {"@id": "ex:alice", "ex:parent": "?parent"}
    });
    let data_rows = support::query_jsonld(&fluree, &ledger, &data_check)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let data_results = normalize_rows(&data_rows);
    eprintln!("Alice's parents: {data_results:?}");
    assert!(
        data_results.contains(&json!("ex:bob")),
        "Alice should have parent bob"
    );

    // Query for Alice's grandparent with datalog reasoning enabled
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?grandparent",
        "where": {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Alice's grandparent should be Charlie (via bob)
    assert!(
        results.contains(&json!("ex:charlie")),
        "Alice should have grandparent Charlie via datalog rule, got {results:?}"
    );
}

#[tokio::test]
async fn datalog_sibling_rule() {
    // Test: Define a sibling rule that derives sibling relationships
    // from shared parent relationships.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/sibling");

    // First, insert the rule definition
    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:siblingRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?x", "ex:parent": "?parent"},
                            {"@id": "?y", "ex:parent": "?parent"}
                        ],
                        "insert": {"@id": "?x", "ex:sibling": {"@id": "?y"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    // Insert family data with siblings
    let family_data = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:carol"}},
            {"@id": "ex:bob", "ex:parent": {"@id": "ex:carol"}}
        ]
    });
    let ledger = fluree.insert(ledger, &family_data).await.unwrap().ledger;

    // Query for Alice's siblings with datalog reasoning enabled
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?sibling",
        "where": {"@id": "ex:alice", "ex:sibling": "?sibling"},
        "reasoning": "datalog"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Alice should have Bob as a sibling
    assert!(
        results.contains(&json!("ex:bob")),
        "Alice should have sibling Bob via datalog rule, got {results:?}"
    );
}

#[tokio::test]
async fn datalog_no_rules_returns_empty() {
    // Test: When no rules are defined, datalog reasoning returns no derived facts
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/no-rules");

    // Insert some data without any rules
    let data = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query for grandparent (should be empty - no rule defined)
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?grandparent",
        "where": {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Should be empty - no grandparent rule defined
    assert!(
        results.is_empty(),
        "Should return empty without grandparent rule, got {results:?}"
    );
}

#[tokio::test]
async fn datalog_combined_with_owl2rl() {
    // Test: Both OWL2-RL and datalog rules can be enabled together
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/combined");

    // Insert OWL symmetric property + datalog rule
    let schema_and_rule = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            {
                "@id": "ex:knows",
                "@type": "owl:SymmetricProperty"
            },
            {
                "@id": "ex:friendOfFriendRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": {"@id": "?x", "ex:knows": {"ex:knows": "?z"}},
                        "insert": {"@id": "?x", "ex:friendOfFriend": {"@id": "?z"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree
        .insert(ledger0, &schema_and_rule)
        .await
        .unwrap()
        .ledger;

    // Insert relationship data
    let data = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "@graph": [
            {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:knows": {"@id": "ex:charlie"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    // Query with both reasoning modes
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?fof",
        "where": {"@id": "ex:alice", "ex:friendOfFriend": "?fof"},
        "reasoning": ["owl2rl", "datalog"]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Alice's friend-of-friend should include Charlie (via bob)
    assert!(
        results.contains(&json!("ex:charlie")),
        "Alice should have friend-of-friend Charlie, got {results:?}"
    );
}

#[tokio::test]
async fn datalog_recursive_ancestor_rule() {
    // Test: Recursive rule that derives ancestors transitively
    // This tests that the fixpoint iteration incorporates derived facts
    // from previous iterations for recursive rules to work correctly.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/recursive-ancestor");

    // Define recursive ancestor rule:
    // - Base case: parent is an ancestor
    // - Recursive case: ancestor of ancestor is ancestor
    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:ancestorBaseRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": {"@id": "?x", "ex:parent": "?y"},
                        "insert": {"@id": "?x", "ex:ancestor": {"@id": "?y"}}
                    }
                }
            },
            {
                "@id": "ex:ancestorRecursiveRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": {"@id": "?x", "ex:ancestor": {"ex:ancestor": "?z"}},
                        "insert": {"@id": "?x", "ex:ancestor": {"@id": "?z"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    // Insert a 4-generation family tree: alice -> bob -> charlie -> dave
    let family_data = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:parent": {"@id": "ex:charlie"}},
            {"@id": "ex:charlie", "ex:parent": {"@id": "ex:dave"}}
        ]
    });
    let ledger = fluree.insert(ledger, &family_data).await.unwrap().ledger;

    // Query for Alice's ancestors with datalog reasoning
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?ancestor",
        "where": {"@id": "ex:alice", "ex:ancestor": "?ancestor"},
        "reasoning": "datalog"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Alice should have ALL ancestors: bob, charlie, dave
    // This requires the recursive rule to fire multiple times:
    // Iteration 1: derive alice->ancestor->bob (base), bob->ancestor->charlie (base), charlie->ancestor->dave (base)
    // Iteration 2: derive alice->ancestor->charlie (recursive from alice->bob->charlie)
    // Iteration 3: derive alice->ancestor->dave (recursive from alice->charlie->dave)
    assert!(
        results.contains(&json!("ex:bob")),
        "Alice should have ancestor Bob, got {results:?}"
    );
    assert!(
        results.contains(&json!("ex:charlie")),
        "Alice should have ancestor Charlie (requires recursive rule), got {results:?}"
    );
    assert!(
        results.contains(&json!("ex:dave")),
        "Alice should have ancestor Dave (requires 2 recursive iterations), got {results:?}"
    );
}

#[tokio::test]
async fn datalog_chains_off_owl_entailments() {
    // Test: Datalog rules can see and chain off OWL2-RL derived facts.
    // This tests that when both owl2rl and datalog are enabled, datalog rules
    // can match against facts that were derived by OWL2-RL reasoning.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/chains-owl");

    // Setup:
    // 1. OWL symmetric property on ex:knows (OWL derives bob->knows->alice from alice->knows->bob)
    // 2. Datalog rule: if ?x knows ?y and ?y has ?interest, then ?x knows about ?interest
    let schema_and_rule = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {
                "@id": "ex:knows",
                "@type": "owl:SymmetricProperty"
            },
            {
                "@id": "ex:interestDiscoveryRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": {"@id": "?x", "ex:knows": {"ex:interest": "?interest"}},
                        "insert": {"@id": "?x", "ex:knowsAbout": {"@id": "?interest"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree
        .insert(ledger0, &schema_and_rule)
        .await
        .unwrap()
        .ledger;

    // Insert data:
    // - alice knows bob (explicit)
    // - bob has interest ex:music
    //
    // With OWL: bob->knows->alice is derived (symmetric)
    // With datalog: alice->knowsAbout->music (from alice->knows->bob->interest->music)
    //               bob->knowsAbout->music (from bob->knows->alice, but alice has no interest)
    //
    // The key test: Can alice discover music through the OWL-derived bob->knows->alice?
    // Actually no - the rule is "?x knows ?y and ?y has interest"
    // So for alice: alice->knows->bob, bob->interest->music => alice->knowsAbout->music
    // For bob (via OWL symmetry): bob->knows->alice (OWL derived), alice->interest->??? (none)
    //
    // Let's flip it: bob knows alice explicitly, alice has the interest
    // OWL derives alice->knows->bob
    // Datalog: bob->knows->alice->interest->music => bob->knowsAbout->music
    // Datalog (via OWL): alice->knows->bob (OWL derived), bob->interest->??? (none)
    //
    // Actually let's do a clearer test:
    // bob knows alice, alice knows charlie, charlie has interest music
    // OWL: alice->knows->bob (symmetric)
    // Datalog: alice->knows->charlie->interest->music => alice->knowsAbout->music
    // Datalog via OWL: bob->knows->alice (OWL), alice->knows->charlie (explicit)
    //   Then we need bob to discover music through alice
    //
    // Simpler test: bob->knows->alice, alice->interest->music
    // OWL derives: alice->knows->bob
    // Datalog rule: ?x knows ?y where ?y has interest => ?x knowsAbout interest
    // For bob: bob->knows->alice (explicit), alice->interest->music => bob->knowsAbout->music
    // For alice (via OWL): alice->knows->bob (OWL derived), bob->interest->??? (none)
    // So bob discovers music through alice.
    //
    // But wait - to test that datalog sees OWL facts, we need a case where
    // the datalog rule REQUIRES the OWL-derived fact.
    //
    // Let's try: alice->knows->bob, bob->interest->music
    // OWL: bob->knows->alice (symmetric)
    // Datalog: for bob: bob->knows->alice (OWL derived!), alice->interest->??? (none)
    //          for alice: alice->knows->bob (explicit), bob->interest->music => alice->knowsAbout->music
    // So alice discovers music - but that doesn't require OWL.
    //
    // To truly test OWL+Datalog chaining:
    // charlie->knows->bob (explicit), bob->interest->music
    // OWL: bob->knows->charlie (symmetric - this is the OWL-derived fact)
    // Datalog rule: ?x knows someone who knows someone with interest => ?x knowsAbout
    //
    // Actually simpler - let's just have the rule use the symmetric path:
    // Data: bob->knows->alice
    // OWL: alice->knows->bob (symmetric)
    // Rule: if alice knows bob and bob has interest, alice knows about interest
    // But bob has no interest in this setup...
    //
    // Let me make it simpler:
    // Data: alice->knows->bob, bob->interest->music
    // OWL symmetric: bob->knows->alice
    // Rule: ?x knows ?y and ?y interest ?z => ?x knowsAbout ?z
    // Result: alice->knowsAbout->music (from alice->knows->bob, bob->interest->music)
    //         bob->knowsAbout->??? (from bob->knows->alice via OWL, alice->interest->??? none)
    // This doesn't test chaining off OWL.
    //
    // OK, different approach:
    // Data: bob->friend->alice
    // OWL inverse: ex:friendOf is inverse of ex:friend, so alice->friendOf->bob
    // Datalog: if ?x friendOf ?y and ?y interest ?z => ?x learnsAbout ?z
    // So we need: alice->friendOf->bob (OWL derived), bob->interest->music
    // Result: alice->learnsAbout->music
    // This REQUIRES the OWL-derived alice->friendOf->bob fact!

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {
                "@id": "ex:friend",
                "owl:inverseOf": {"@id": "ex:friendOf"}
            },
            {"@id": "ex:bob", "ex:friend": {"@id": "ex:alice"}, "ex:interest": {"@id": "ex:music"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    // Add the rule that uses friendOf (which only exists via OWL inverse)
    let rule_using_inverse = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:learnsAboutRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": {"@id": "?x", "ex:friendOf": {"ex:interest": "?interest"}},
                        "insert": {"@id": "?x", "ex:learnsAbout": {"@id": "?interest"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree
        .insert(ledger, &rule_using_inverse)
        .await
        .unwrap()
        .ledger;

    // Query for what alice learns about - this REQUIRES:
    // 1. OWL inverse to derive alice->friendOf->bob
    // 2. Datalog to see that OWL fact and derive alice->learnsAbout->music
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?interest",
        "where": {"@id": "ex:alice", "ex:learnsAbout": "?interest"},
        "reasoning": ["owl2rl", "datalog"]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Alice should learn about music through the OWL-derived friendOf relationship
    assert!(
        results.contains(&json!("ex:music")),
        "Alice should learn about music via OWL+Datalog chaining. \
        OWL derives alice->friendOf->bob, Datalog uses that to derive alice->learnsAbout->music. \
        Got: {results:?}"
    );
}

#[tokio::test]
async fn datalog_filter_expression() {
    // Test: Filter expressions in rule bodies filter bindings based on conditions.
    // This tests the ["filter", "(op ?var value)"] syntax.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/filter");

    // Define a rule that marks people as senior citizens if age >= 62
    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {
                "@id": "ex:seniorRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?person", "ex:age": "?age"},
                            ["filter", "(>= ?age 62)"]
                        ],
                        "insert": {"@id": "?person", "ex:status": "senior"}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    // Insert people with various ages
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id": "ex:alice", "ex:age": {"@value": 65, "@type": "xsd:integer"}},
            {"@id": "ex:bob", "ex:age": {"@value": 45, "@type": "xsd:integer"}},
            {"@id": "ex:charlie", "ex:age": {"@value": 70, "@type": "xsd:integer"}},
            {"@id": "ex:dave", "ex:age": {"@value": 62, "@type": "xsd:integer"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    // Query for senior citizens with datalog reasoning
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?person",
        "where": {"@id": "?person", "ex:status": "senior"},
        "reasoning": "datalog"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Alice (65), Charlie (70), and Dave (62) should be seniors
    // Bob (45) should NOT be a senior
    assert!(
        results.contains(&json!("ex:alice")),
        "Alice (age 65) should be a senior, got {results:?}"
    );
    assert!(
        results.contains(&json!("ex:charlie")),
        "Charlie (age 70) should be a senior, got {results:?}"
    );
    assert!(
        results.contains(&json!("ex:dave")),
        "Dave (age 62) should be a senior (boundary case), got {results:?}"
    );
    assert!(
        !results.contains(&json!("ex:bob")),
        "Bob (age 45) should NOT be a senior, got {results:?}"
    );
}

#[tokio::test]
async fn datalog_filter_less_than() {
    // Test: Filter with less-than comparison
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/filter-lt");

    // Define a rule that marks items as "affordable" if price < 100
    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:affordableRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?item", "ex:price": "?price"},
                            ["filter", "(< ?price 100)"]
                        ],
                        "insert": {"@id": "?item", "ex:affordable": true}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    // Insert items with various prices
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id": "ex:widget", "ex:price": {"@value": 50, "@type": "xsd:integer"}},
            {"@id": "ex:gadget", "ex:price": {"@value": 150, "@type": "xsd:integer"}},
            {"@id": "ex:gizmo", "ex:price": {"@value": 99, "@type": "xsd:integer"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    // Query for affordable items
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?item",
        "where": {"@id": "?item", "ex:affordable": true},
        "reasoning": "datalog"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Widget (50) and Gizmo (99) should be affordable
    // Gadget (150) should NOT be affordable
    assert!(
        results.contains(&json!("ex:widget")),
        "Widget (price 50) should be affordable, got {results:?}"
    );
    assert!(
        results.contains(&json!("ex:gizmo")),
        "Gizmo (price 99) should be affordable, got {results:?}"
    );
    assert!(
        !results.contains(&json!("ex:gadget")),
        "Gadget (price 150) should NOT be affordable, got {results:?}"
    );
}

// =============================================================================
// Query-Time Rules Tests
// =============================================================================

#[tokio::test]
async fn datalog_query_time_rules() {
    // Test: Rules provided at query time via the "rules" field.
    // No rules are stored in the database; they're passed with the query.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/query-time-rules");

    // Insert family data (no rules in the database)
    let family_data = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &family_data).await.unwrap().ledger;

    // Query for Alice's grandparent with a query-time rule
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?grandparent",
        "where": {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog",
        "rules": [{
            "@context": {"ex": "http://example.org/"},
            "where": {"@id": "?person", "ex:parent": {"ex:parent": "?grandparent"}},
            "insert": {"@id": "?person", "ex:grandparent": {"@id": "?grandparent"}}
        }]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Alice's grandparent should be Charlie (via bob)
    assert!(
        results.contains(&json!("ex:charlie")),
        "Alice should have grandparent Charlie via query-time rule, got {results:?}"
    );
}

#[tokio::test]
async fn datalog_query_time_rules_with_id() {
    // Test: Query-time rules with explicit @id
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/query-time-rules-with-id");

    // Insert family data
    let family_data = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &family_data).await.unwrap().ledger;

    // Query with named rule
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?grandparent",
        "where": {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog",
        "rules": [{
            "@id": "ex:myGrandparentRule",
            "@context": {"ex": "http://example.org/"},
            "where": {"@id": "?person", "ex:parent": {"ex:parent": "?grandparent"}},
            "insert": {"@id": "?person", "ex:grandparent": {"@id": "?grandparent"}}
        }]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:charlie")),
        "Named query-time rule should work, got {results:?}"
    );
}

#[tokio::test]
async fn datalog_query_time_rules_multiple() {
    // Test: Multiple query-time rules that chain together
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/query-time-rules-multiple");

    // Insert family data with uncle relationship
    let family_data = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "@graph": [
            {"@id": "ex:brian", "ex:parent": {"@id": "ex:carol"}},
            {"@id": "ex:carol", "ex:brother": {"@id": "ex:mike"}},
            {"@id": "ex:mike", "ex:spouse": {"@id": "ex:holly"}},
            {"@id": "ex:holly", "ex:gender": {"@id": "ex:Female"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &family_data).await.unwrap().ledger;

    // Query with two rules: uncle rule and aunt rule (chained)
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?aunt",
        "where": {"@id": "ex:brian", "ex:aunt": "?aunt"},
        "reasoning": "datalog",
        "rules": [
            {
                "@context": {"ex": "http://example.org/"},
                "where": {"@id": "?person", "ex:parent": {"ex:brother": "?uncle"}},
                "insert": {"@id": "?person", "ex:uncle": {"@id": "?uncle"}}
            },
            {
                "@context": {"ex": "http://example.org/"},
                "where": {"@id": "?person", "ex:uncle": {"ex:spouse": {"@id": "?aunt", "ex:gender": {"@id": "ex:Female"}}}},
                "insert": {"@id": "?person", "ex:aunt": {"@id": "?aunt"}}
            }
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Brian's aunt should be Holly (via mike, carol's brother, whose spouse is holly)
    assert!(
        results.contains(&json!("ex:holly")),
        "Brian should have aunt Holly via chained query-time rules, got {results:?}"
    );
}

#[tokio::test]
async fn datalog_query_time_rules_with_filter() {
    // Test: Query-time rules with filter expressions
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/query-time-rules-filter");

    // Insert people with ages
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id": "ex:alice", "ex:age": {"@value": 65, "@type": "xsd:integer"}},
            {"@id": "ex:bob", "ex:age": {"@value": 45, "@type": "xsd:integer"}},
            {"@id": "ex:charlie", "ex:age": {"@value": 70, "@type": "xsd:integer"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query with a query-time rule that has a filter
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?person",
        "where": {"@id": "?person", "ex:status": "senior"},
        "reasoning": "datalog",
        "rules": [{
            "@context": {"ex": "http://example.org/"},
            "where": [
                {"@id": "?person", "ex:age": "?age"},
                ["filter", "(>= ?age 62)"]
            ],
            "insert": {"@id": "?person", "ex:status": "senior"}
        }]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:alice")),
        "Alice (65) should be a senior, got {results:?}"
    );
    assert!(
        results.contains(&json!("ex:charlie")),
        "Charlie (70) should be a senior, got {results:?}"
    );
    assert!(
        !results.contains(&json!("ex:bob")),
        "Bob (45) should NOT be a senior, got {results:?}"
    );
}

#[tokio::test]
async fn datalog_query_time_rules_merged_with_db_rules() {
    // Test: Query-time rules are merged with rules stored in the database
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/query-time-rules-merged");

    // Store an uncle rule in the database
    let db_rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:uncleRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": {"@id": "?person", "ex:parent": {"ex:brother": "?uncle"}},
                        "insert": {"@id": "?person", "ex:uncle": {"@id": "?uncle"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &db_rule_data).await.unwrap().ledger;

    // Insert family data
    let family_data = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "@graph": [
            {"@id": "ex:brian", "ex:parent": {"@id": "ex:carol"}},
            {"@id": "ex:carol", "ex:brother": {"@id": "ex:mike"}},
            {"@id": "ex:mike", "ex:spouse": {"@id": "ex:holly"}},
            {"@id": "ex:holly", "ex:gender": {"@id": "ex:Female"}}
        ]
    });
    let ledger = fluree.insert(ledger, &family_data).await.unwrap().ledger;

    // Query with an aunt rule at query time - this should chain with the DB uncle rule
    let q = json!({
        "@context": {
            "ex": "http://example.org/"
        },
        "select": "?aunt",
        "where": {"@id": "ex:brian", "ex:aunt": "?aunt"},
        "reasoning": "datalog",
        "rules": [{
            "@context": {"ex": "http://example.org/"},
            "where": {"@id": "?person", "ex:uncle": {"ex:spouse": {"@id": "?aunt", "ex:gender": {"@id": "ex:Female"}}}},
            "insert": {"@id": "?person", "ex:aunt": {"@id": "?aunt"}}
        }]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // The aunt rule (query-time) chains off the uncle rule (DB-stored)
    // brian -> uncle -> mike (from DB rule), mike -> spouse -> holly -> aunt (from query-time rule)
    assert!(
        results.contains(&json!("ex:holly")),
        "Query-time aunt rule should chain with DB uncle rule. Got: {results:?}"
    );
}
