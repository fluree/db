//! Datalog rule integration tests
//!
//! These tests validate user-defined datalog rules (f:rule predicate).
//! Rules use `where`/`insert` patterns to derive new facts during query execution.
//!
//! Test coverage:
//! - Basic grandparent rule (2-hop traversal)
//! - Rule with multiple where patterns
//! - Fixpoint iteration (rules triggering other rules)

use crate::support;
use crate::support::{genesis_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

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

/// Security: query-time datalog rule injection is admin-only. Under a non-root
/// view policy, caller-supplied `rules` are stripped — a rule with a viewable
/// head could otherwise launder hidden data the policy author never anticipated
/// (the derived flake is filtered only by its own (s,p,o), not provenance, and
/// a caller-invented predicate can't be pre-denied). DB-stored rules and OWL
/// reasoning are admin-controlled and unaffected.
#[tokio::test]
async fn datalog_query_time_rules_stripped_under_non_root_policy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "datalog/query-time-rules-policy";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let family = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob",   "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    fluree.insert(ledger0, &family).await.unwrap();

    let rules = json!([{
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "?person", "ex:parent": {"ex:parent": "?grandparent"}},
        "insert": {"@id": "?person", "ex:grandparent": {"@id": "?grandparent"}}
    }]);

    // Control (root / no policy): the query-time rule fires → grandparent derived.
    let base_q = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": "?grandparent",
        "where": {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog",
        "rules": rules.clone()
    });
    let base = fluree.query_connection(&base_q).await.expect("base query");
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let base_rows = normalize_rows(&base.to_jsonld(&ledger.snapshot).unwrap());
    assert!(
        base_rows.contains(&json!("ex:charlie")),
        "control: query-time rule should fire without a policy, got {base_rows:?}"
    );

    // Under a permissive but NON-ROOT view policy (allow-all, so base data is
    // fully visible), the rule is stripped → no derived grandparent. The only
    // reason it can't fire is the strip, not data filtering.
    let policy_q = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "opts": {
            "policy": [{"f:action": "f:view", "f:allow": true}],
            "default-allow": true
        },
        "select": "?grandparent",
        "where": {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog",
        "rules": rules
    });
    let policed = fluree
        .query_connection(&policy_q)
        .await
        .expect("policed query");
    let policed_rows = normalize_rows(&policed.to_jsonld(&ledger.snapshot).unwrap());
    assert!(
        !policed_rows.contains(&json!("ex:charlie")),
        "query-time datalog rules must be stripped under a non-root view policy, got {policed_rows:?}"
    );
}

// =============================================================================
// Property-Position Variables (issue #1531)
// =============================================================================

/// A variable in property position of an `insert` pattern must be replaced
/// with its bound value, not committed as the literal property name (#1531).
///
/// The rule binds `?rel` to a predicate IRI via an object reference in the
/// where clause, then uses it as the property of the derived fact:
/// `{"@id": "?s", "?rel": "?o"}` with `?rel` = `ex:knows` must derive
/// `ex:alice ex:knows ex:bob`.
#[tokio::test]
async fn datalog_rule_insert_property_variable_substituted() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/insert-prop-var");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:relateRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "ex:relates": {"@id": "?o"}},
                            {"@id": "?s", "ex:relType": {"@id": "?rel"}}
                        ],
                        "insert": {"@id": "?s", "?rel": {"@id": "?o"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice",
             "ex:relates": {"@id": "ex:bob"},
             "ex:relType": {"@id": "ex:knows"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?who",
        "where": {"@id": "ex:alice", "ex:knows": "?who"},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);
    assert!(
        results.contains(&json!("ex:bob")),
        "insert-position property var ?rel must resolve to ex:knows, got {results:?}"
    );
}

/// A variable in property position of a `where` pattern must match any
/// predicate and bind it, so the insert clause can re-use it (#1531).
///
/// The rule copies every property of a `ex:sameAs` target onto the subject:
/// `where {?s ex:sameAs ?other . ?other ?prop ?val}` /
/// `insert {?s ?prop ?val}`.
#[tokio::test]
async fn datalog_rule_where_property_variable_binds_and_substitutes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/where-prop-var");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:copyRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "ex:sameAs": {"@id": "?other"}},
                            {"@id": "?other", "?prop": "?val"}
                        ],
                        "insert": {"@id": "?s", "?prop": "?val"}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:a", "ex:sameAs": {"@id": "ex:b"}},
            {"@id": "ex:b", "ex:color": "blue", "ex:size": 5}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?color", "?size"],
        "where": {"@id": "ex:a", "ex:color": "?color", "ex:size": "?size"},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);
    assert!(
        results.contains(&json!(["blue", 5])),
        "where-position property var ?prop must match and carry into insert, got {results:?}"
    );
}

/// A rule whose predicate variable ends up bound to a literal (not an IRI)
/// matches nothing for that row — it must not abort rule execution and drop
/// every other rule's derived facts.
#[tokio::test]
async fn datalog_rule_literal_bound_property_variable_does_not_abort_other_rules() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/literal-prop-var");

    // Rule 1 (sound): derives grandparent. Rule 2 (unsatisfiable): binds ?p to
    // the string value of ex:tag, then reuses ?p in predicate position, where
    // a literal can never match.
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
            },
            {
                "@id": "ex:literalPredicateRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?x", "ex:tag": "?p"},
                            {"@id": "?x", "?p": "?v"}
                        ],
                        "insert": {"@id": "?x", "ex:derived": "?v"}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}, "ex:tag": "blue"},
            {"@id": "ex:bob", "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
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
    assert!(
        results.contains(&json!("ex:charlie")),
        "sound rule's derivations must survive a sibling rule's literal-bound \
         predicate var, got {results:?}"
    );
}

/// A literal *constant* in subject position (a plausible typo for a prefixed
/// IRI, e.g. `{"@id": "Alice"}` instead of `{"@id": "ex:Alice"}`) can never
/// match a flake, so that rule derives nothing — but it must not abort the
/// fixpoint and drop every other rule's derivations.
#[tokio::test]
async fn datalog_rule_literal_subject_constant_does_not_abort_other_rules() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/literal-subject");

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
            },
            {
                // `"Alice"` is colon-less and non-`?`, so it parses as a string
                // literal in subject position — a rule that can never match.
                "@id": "ex:literalSubjectRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": {"@id": "Alice", "ex:parent": "?p"},
                        "insert": {"@id": "Alice", "ex:derived": "?p"}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
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
    assert!(
        results.contains(&json!("ex:charlie")),
        "sound rule's derivations must survive a sibling rule's literal subject \
         constant, got {results:?}"
    );
}

/// A copy-properties rule whose all-unbound pattern `{?other ?prop ?val}` is
/// written FIRST (the full-scan-leading order) must still derive correctly —
/// the matcher reorders patterns most-constrained-first, hoisting the
/// grounding `ex:sameAs` pattern ahead of it. Mirrors
/// `datalog_rule_where_property_variable_binds_and_substitutes` with the
/// where patterns reversed.
#[tokio::test]
async fn datalog_rule_all_unbound_leading_pattern_still_derives_correctly() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/reorder-leading-unbound");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:copyRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?other", "?prop": "?val"},
                            {"@id": "?s", "ex:sameAs": {"@id": "?other"}}
                        ],
                        "insert": {"@id": "?s", "?prop": "?val"}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:a", "ex:sameAs": {"@id": "ex:b"}},
            {"@id": "ex:b", "ex:color": "blue", "ex:size": 5}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?color", "?size"],
        "where": {"@id": "ex:a", "ex:color": "?color", "ex:size": "?size"},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);
    assert!(
        results.contains(&json!(["blue", 5])),
        "reordered rule with all-unbound leading pattern must still derive, got {results:?}"
    );
}

// =============================================================================
// IRI comparison in rule FILTERs (issue #1556)
//
// A filter operand that names an IRI must compare against a bound IRI as an
// IRI. Before the fix all three sites disagreed — the operand parsed as the
// *string* `"ex:ssn"`, a bound Sid resolved to its bare local name `"ssn"`,
// and `compare_values` string-compared the two — so `=` was always false and
// `!=` was always true. The `!=` direction is the dangerous one: an exclusion
// filter silently excluded nothing and the rule derived the very fact it was
// written to withhold.
// =============================================================================

/// The headline of #1556: a copy-properties rule that excludes a sensitive
/// predicate with `(!= ?prop ex:ssn)` must actually exclude it.
///
/// Before the fix this FAILED OPEN — `ex:ssn` was copied onto `ex:alice`
/// anyway, with no error and no warning.
#[tokio::test]
async fn datalog_filter_iri_exclusion_actually_excludes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/filter-iri-exclusion");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:copyPropsRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "ex:sameAs": {"@id": "?other"}},
                            {"@id": "?other", "?prop": "?val"},
                            ["filter", "(!= ?prop ex:ssn)"]
                        ],
                        "insert": {"@id": "?s", "?prop": "?val"}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:sameAs": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:name": "Bob", "ex:ssn": "123-45-6789"}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?prop", "?val"],
        "where": {"@id": "ex:alice", "?prop": "?val"},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!(["ex:name", "Bob"])),
        "the non-excluded property must still be copied, got {results:?}"
    );
    let rendered = serde_json::to_string(&results).unwrap();
    assert!(
        !rendered.contains("ssn") && !rendered.contains("123-45-6789"),
        "(!= ?prop ex:ssn) must EXCLUDE ex:ssn — an exclusion filter that fails \
         open copies the sensitive value, got {results:?}"
    );
}

/// The other direction of #1556: `(= ?p ex:knows)` was always false, so the
/// rule derived nothing at all.
#[tokio::test]
async fn datalog_filter_iri_equality_matches() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/filter-iri-equality");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:connectedRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "?p": {"@id": "?o"}},
                            ["filter", "(= ?p ex:knows)"]
                        ],
                        "insert": {"@id": "?s", "ex:connected": {"@id": "?o"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?who",
        "where": {"@id": "ex:alice", "ex:connected": "?who"},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:bob")),
        "(= ?p ex:knows) must match a ?p bound to ex:knows, got {results:?}"
    );
}

/// IRI comparison must be namespace-aware: `(= ?p ex:knows)` must not match
/// `foaf:knows`. The only operand that matched before the fix was the bare
/// local name `knows`, which matched every `knows` in every namespace.
#[tokio::test]
async fn datalog_filter_iri_equality_is_namespace_aware() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/filter-iri-namespace");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:connectedRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "?p": {"@id": "?o"}},
                            ["filter", "(= ?p ex:knows)"]
                        ],
                        "insert": {"@id": "?s", "ex:connected": {"@id": "?o"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "foaf": "http://xmlns.com/foaf/0.1/"
        },
        "@graph": [
            {"@id": "ex:alice",
             "ex:knows": {"@id": "ex:bob"},
             "foaf:knows": {"@id": "ex:carol"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?who",
        "where": {"@id": "ex:alice", "ex:connected": "?who"},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:bob")),
        "ex:knows must match, got {results:?}"
    );
    assert!(
        !results.contains(&json!("ex:carol")),
        "foaf:knows must NOT match the ex:knows filter operand — IRI comparison \
         must be namespace-aware, not local-name-blind, got {results:?}"
    );
}

/// #1556 is not specific to predicate position: `flake_value_to_binding` maps
/// a `Ref` object to a `Sid` binding too, so an object-position IRI filter was
/// broken identically — and in the `!=` direction, identically fail-open.
#[tokio::test]
async fn datalog_filter_iri_object_position_exclusion() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/filter-iri-object");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:knowsOtherRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "ex:knows": {"@id": "?o"}},
                            ["filter", "(!= ?o ex:bob)"]
                        ],
                        "insert": {"@id": "?s", "ex:knowsOther": {"@id": "?o"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:knows": [{"@id": "ex:bob"}, {"@id": "ex:carol"}]}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?who",
        "where": {"@id": "ex:alice", "ex:knowsOther": "?who"},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:carol")),
        "the non-excluded object must still derive, got {results:?}"
    );
    assert!(
        !results.contains(&json!("ex:bob")),
        "(!= ?o ex:bob) must exclude ex:bob in object position, got {results:?}"
    );
}

/// Fail CLOSED, not open. A filter operand that is shaped like a compact IRI
/// but whose prefix the rule's `@context` never defines cannot be compared as
/// an IRI. Demoting it to a string — what the engine used to do — is exactly
/// the fail-open exclusion of #1556, so the rule is rejected at parse time
/// instead: it derives nothing rather than deriving the excluded fact, and the
/// author gets a named error.
#[tokio::test(flavor = "current_thread")]
async fn datalog_filter_unresolvable_iri_operand_fails_closed() {
    let (store, _guard) = support::span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/filter-iri-unresolvable");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                // Sound rule: must keep deriving even though its sibling is rejected.
                "@id": "ex:grandparentRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": {"@id": "?person", "ex:parent": {"ex:parent": "?grandparent"}},
                        "insert": {"@id": "?person", "ex:grandparent": {"@id": "?grandparent"}}
                    }
                }
            },
            {
                // `foo:` is never defined in this rule's @context, so the
                // operand cannot be resolved to an IRI.
                "@id": "ex:copyPropsRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "ex:sameAs": {"@id": "?other"}},
                            {"@id": "?other", "?prop": "?val"},
                            ["filter", "(!= ?prop foo:ssn)"]
                        ],
                        "insert": {"@id": "?s", "?prop": "?val"}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:sameAs": {"@id": "ex:bob"}, "ex:parent": {"@id": "ex:dan"}},
            {"@id": "ex:dan", "ex:parent": {"@id": "ex:erin"}},
            {"@id": "ex:bob", "ex:name": "Bob", "ex:ssn": "123-45-6789"}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?prop", "?val"],
        "where": {"@id": "ex:alice", "?prop": "?val"},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    let rendered = serde_json::to_string(&results).unwrap();
    assert!(
        !rendered.contains("123-45-6789"),
        "an unresolvable IRI filter operand must fail CLOSED — the rule must not \
         run and copy the value its filter was meant to exclude, got {results:?}"
    );
    assert!(
        results.contains(&json!(["ex:grandparent", "ex:erin"])),
        "the sound sibling rule must keep deriving, got {results:?}"
    );

    let diagnostics: Vec<String> = store
        .all_events()
        .into_iter()
        .filter(|e| e.level == tracing::Level::WARN)
        .flat_map(|e| {
            let mut parts: Vec<String> = e.fields.values().cloned().collect();
            parts.push(e.message().to_string());
            parts
        })
        .collect();
    assert!(
        diagnostics.iter().any(|d| d.contains("foo:ssn")),
        "the rejected rule must produce a diagnostic naming the operand, got {diagnostics:?}"
    );
}

// =============================================================================
// Unbound insert-pattern variables (issue #1560)
// =============================================================================

/// #1560: a rule whose `insert` references a variable the `where` clause never
/// binds derives nothing for every binding row — `instantiate_pattern` returns
/// `None` and the row is dropped. That is correct semantics but it used to be
/// completely silent. It must now produce a diagnostic naming the variable,
/// without disturbing any other rule's derivations.
#[tokio::test(flavor = "current_thread")]
async fn datalog_unbound_insert_variable_reports_named_diagnostic() {
    let (store, _guard) = support::span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/unbound-insert-var");

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
            },
            {
                // The where clause binds ?relation; the insert says ?rel.
                "@id": "ex:typoRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": {"@id": "?s", "ex:relType": {"@id": "?relation"}},
                        "insert": {"@id": "?s", "?rel": {"@id": "?s"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:relType": {"@id": "ex:friendOf"}, "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
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

    assert!(
        results.contains(&json!("ex:charlie")),
        "the sound rule must keep deriving alongside the broken one, got {results:?}"
    );

    let diagnostics: Vec<String> = store
        .all_events()
        .into_iter()
        .filter(|e| e.level == tracing::Level::WARN)
        .flat_map(|e| {
            let mut parts: Vec<String> = e.fields.values().cloned().collect();
            parts.push(e.message().to_string());
            parts
        })
        .collect();
    assert!(
        diagnostics.iter().any(|d| d.contains("?rel")),
        "a rule whose insert references an unbindable variable must produce a \
         diagnostic naming ?rel, got {diagnostics:?}"
    );
}

/// The other half of #1560's acceptance criteria: no diagnostic noise for a
/// rule that legitimately derives nothing because its where clause matched
/// nothing. Silence is correct there; only "matched but could not instantiate"
/// is an authoring bug.
#[tokio::test(flavor = "current_thread")]
async fn datalog_rule_matching_nothing_is_not_flagged() {
    let (store, _guard) = support::span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/no-match-no-warning");

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

    // Only a one-hop chain: the two-hop where clause matches nothing.
    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [{"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}}]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?grandparent",
        "where": {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog"
    });
    let _ = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    let noisy: Vec<String> = store
        .all_events()
        .into_iter()
        .filter(|e| e.level == tracing::Level::WARN)
        .map(|e| e.message().to_string())
        .filter(|m| m.contains("derived no facts"))
        .collect();
    assert!(
        noisy.is_empty(),
        "a rule whose where clause matched nothing must not be flagged, got {noisy:?}"
    );
}

// =============================================================================
// Review follow-ups on the #1556 fix
// =============================================================================

/// Making every Sid-bound variable resolve as an IRI must not start dropping
/// IRI-valued rows from a filter that tests a *literal*.
///
/// SPARQL 1.1 §17.4.1.7 makes RDFterm-equal a type error only when both
/// operands are literals; an IRI against a literal is simply not the same RDF
/// term, so `!=` is true and the row is kept. Treating that pairing as a type
/// error would be fail-closed but wrong — silent under-derivation.
#[tokio::test]
async fn datalog_filter_literal_does_not_drop_iri_valued_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/filter-literal-vs-iri");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:copyExceptBobRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "ex:sameAs": {"@id": "?other"}},
                            {"@id": "?other", "?prop": "?val"},
                            ["filter", "(!= ?val \"Bob\")"]
                        ],
                        "insert": {"@id": "?s", "?prop": "?val"}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:sameAs": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:name": "Bob", "ex:friend": {"@id": "ex:carol"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?prop", "?val"],
        "where": {"@id": "ex:alice", "?prop": "?val"},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!(["ex:friend", "ex:carol"])),
        "an IRI-valued property must survive a filter that excludes a literal — \
         an IRI is not the literal \"Bob\", so (!= ?val \"Bob\") holds, got {results:?}"
    );
    assert!(
        !results.contains(&json!(["ex:name", "Bob"])),
        "the literal the filter names must still be excluded, got {results:?}"
    );
}

/// A filter that quotes an operand containing whitespace must survive
/// tokenization — the parse error for an unresolvable IRI operand recommends
/// quoting, so the hatch has to actually work.
#[tokio::test]
async fn datalog_filter_quoted_operand_with_whitespace_works() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/filter-quoted-whitespace");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:notJohnRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "ex:name": "?name"},
                            ["filter", "(!= ?name \"John Smith\")"]
                        ],
                        "insert": {"@id": "?s", "ex:screened": true}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:a", "ex:name": "John Smith"},
            {"@id": "ex:b", "ex:name": "Jane Doe"}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "ex:screened": true},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:b")),
        "the non-excluded row must derive, got {results:?}"
    );
    assert!(
        !results.contains(&json!("ex:a")),
        "a quoted operand containing a space must still exclude its match, \
         got {results:?}"
    );
}

/// A malformed filter must not vanish. If it does, `rule.filters` is empty,
/// filtering is skipped entirely, and the rule derives everything it was
/// written to restrict — the #1556 failure mode reached by another route.
///
/// All three shapes were silently dropped before: a wrong-length array, a
/// non-string expression, and a filter keyword whose case does not match.
#[tokio::test(flavor = "current_thread")]
async fn datalog_malformed_filter_does_not_silently_vanish() {
    for (label, filter_element) in [
        (
            "wrong arity",
            json!(["filter", "(!= ?prop ex:ssn)", "oops"]),
        ),
        ("wrong case", json!(["FILTER", "(!= ?prop ex:ssn)"])),
        (
            "non-string expression",
            json!(["filter", {"expr": "(!= ?prop ex:ssn)"}]),
        ),
    ] {
        let (store, _guard) = support::span_capture::init_test_tracing();

        let fluree = FlureeBuilder::memory().build_memory();
        let ledger0 = genesis_ledger(&fluree, "datalog/malformed-filter");

        let rule_data = json!({
            "@context": {
                "ex": "http://example.org/",
                "f": "https://ns.flur.ee/db#"
            },
            "@graph": [
                {
                    "@id": "ex:copyPropsRule",
                    "f:rule": {
                        "@type": "@json",
                        "@value": {
                            "@context": {"ex": "http://example.org/"},
                            "where": [
                                {"@id": "?s", "ex:sameAs": {"@id": "?other"}},
                                {"@id": "?other", "?prop": "?val"},
                                filter_element
                            ],
                            "insert": {"@id": "?s", "?prop": "?val"}
                        }
                    }
                }
            ]
        });
        let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

        let data = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [
                {"@id": "ex:alice", "ex:sameAs": {"@id": "ex:bob"}},
                {"@id": "ex:bob", "ex:name": "Bob", "ex:ssn": "123-45-6789"}
            ]
        });
        let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

        let q = json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?prop", "?val"],
            "where": {"@id": "ex:alice", "?prop": "?val"},
            "reasoning": "datalog"
        });
        let rows = support::query_jsonld(&fluree, &ledger, &q)
            .await
            .unwrap()
            .to_jsonld(&ledger.snapshot)
            .unwrap();
        let results = normalize_rows(&rows);

        let rendered = serde_json::to_string(&results).unwrap();
        assert!(
            !rendered.contains("123-45-6789"),
            "[{label}] a malformed filter must not be dropped on the floor — the \
             rule ran unfiltered and copied the value the filter was meant to \
             exclude, got {results:?}"
        );

        let warned = store
            .all_events()
            .into_iter()
            .any(|e| e.level == tracing::Level::WARN);
        assert!(
            warned,
            "[{label}] rejecting a malformed filter must produce a diagnostic"
        );
    }
}

/// The bare-local-name filter form — `(= ?p knows)` — was the only operand
/// shape that ever matched a bound IRI before #1556 was fixed, so a rule
/// written as a workaround uses exactly it. It now correctly derives nothing,
/// because a bare name is a string literal and a string is never an IRI.
///
/// The risk is that it does so in silence: RDFterm-equal makes IRI-vs-literal
/// a clean `False` rather than an `Error`, so the "could not compare its
/// operands" path cannot see it. A stale rule must still say something.
#[tokio::test(flavor = "current_thread")]
async fn datalog_bare_local_name_filter_warns_instead_of_going_quiet() {
    let (store, _guard) = support::span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/bare-local-name-filter");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:staleWorkaroundRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "?p": {"@id": "?o"}},
                            ["filter", "(= ?p knows)"]
                        ],
                        "insert": {"@id": "?s", "ex:derivedKnows": {"@id": "?o"}}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [{"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}}]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?who",
        "where": {"@id": "ex:alice", "ex:derivedKnows": "?who"},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.is_empty(),
        "a bare local name is a string literal and must not match an IRI \
         namespace-blindly, got {results:?}"
    );

    let diagnostics: Vec<String> = store
        .all_events()
        .into_iter()
        .filter(|e| e.level == tracing::Level::WARN)
        .map(|e| e.message().to_string())
        .collect();
    assert!(
        diagnostics
            .iter()
            .any(|d| d.contains("compared an IRI against a literal")),
        "a stale bare-local-name filter must not fail silently — it needs a \
         diagnostic pointing at the operand form, got {diagnostics:?}"
    );
}

/// The converse guard: a filter that legitimately compares an IRI against a
/// literal and keeps rows must NOT trip the bare-local-name warning. The
/// signal is only meaningful if it stays quiet on correct usage.
#[tokio::test(flavor = "current_thread")]
async fn datalog_iri_versus_literal_filter_that_keeps_rows_is_not_flagged() {
    let (store, _guard) = support::span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/iri-vs-literal-no-warn");

    let rule_data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:copyExceptBobRule",
                "f:rule": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            {"@id": "?s", "ex:sameAs": {"@id": "?other"}},
                            {"@id": "?other", "?prop": "?val"},
                            ["filter", "(!= ?val \"Bob\")"]
                        ],
                        "insert": {"@id": "?s", "?prop": "?val"}
                    }
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:sameAs": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:name": "Bob", "ex:friend": {"@id": "ex:carol"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?prop", "?val"],
        "where": {"@id": "ex:alice", "?prop": "?val"},
        "reasoning": "datalog"
    });
    let _ = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    let noisy: Vec<String> = store
        .all_events()
        .into_iter()
        .filter(|e| e.level == tracing::Level::WARN)
        .map(|e| e.message().to_string())
        .filter(|m| m.contains("compared an IRI against a literal"))
        .collect();
    assert!(
        noisy.is_empty(),
        "an IRI-vs-literal filter that keeps rows is correct usage and must not \
         be flagged, got {noisy:?}"
    );
}
