//! View policy over derived facts.
//!
//! The documented contract (docs/query/reasoning.md, "Reasoning under access
//! policy"): materialized reasoning computes one shared closure, and a derived
//! fact is filtered by its own subject, predicate and object, never by the
//! premises it was derived from. Query rewriting (`rdfs`, `owl2ql`) instead
//! runs under the caller's identity, so a hidden premise cannot contribute.
//!
//! Policy is enforced per row-emitting operator, so the denied-head checks
//! reach a derived predicate through a scan, a join, an aggregate and a
//! property path, on both novelty-only and indexed storage. Every check runs
//! the root query first, so the restricted reader is served from a closure
//! that is already warm.
//!
//! Stored `f:rule` definitions are used throughout: query-time rules are
//! stripped under any non-root policy, which would make a denied-head check
//! pass for the wrong reason.

use crate::support::{
    genesis_ledger, instances_sharing_storage, normalize_rows, rebuild_and_publish_index,
    MemoryFluree,
};
use serde_json::{json, Value};

#[derive(Clone, Copy, Debug)]
enum Storage {
    NoveltyOnly,
    Indexed,
}

const STORAGE: [Storage; 2] = [Storage::NoveltyOnly, Storage::Indexed];

fn context() -> Value {
    json!({
        "ex": "http://example.org/",
        "f": "https://ns.flur.ee/db#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
    })
}

fn stored_rule(id: &str, rule: Value) -> Value {
    json!({"@id": id, "f:rule": {"@type": "@json", "@value": rule}})
}

fn family_with_rules() -> Value {
    let rule_ctx = json!({"ex": "http://example.org/"});
    json!({
        "@context": context(),
        "@graph": [
            stored_rule("ex:ancestorBase", json!({
                "@context": rule_ctx,
                "where": {"@id": "?x", "ex:parent": "?y"},
                "insert": {"@id": "?x", "ex:ancestor": {"@id": "?y"}}
            })),
            stored_rule("ex:ancestorStep", json!({
                "@context": rule_ctx,
                "where": {"@id": "?x", "ex:ancestor": {"ex:ancestor": "?z"}},
                "insert": {"@id": "?x", "ex:ancestor": {"@id": "?z"}}
            })),
            stored_rule("ex:highEarnerRule", json!({
                "@context": rule_ctx,
                "where": [
                    {"@id": "?p", "ex:salary": "?s"},
                    ["filter", "(> ?s 200000)"]
                ],
                "insert": {"@id": "?p", "@type": "ex:HighEarner"}
            })),
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}, "ex:salary": 250_000},
            {"@id": "ex:bob", "ex:name": "Bob", "ex:parent": {"@id": "ex:carol"}, "ex:salary": 90000},
            {"@id": "ex:carol", "ex:name": "Carol", "ex:parent": {"@id": "ex:dave"}},
            {"@id": "ex:dave", "ex:name": "Dave"}
        ]
    })
}

/// Commit `data` through a writer and return a separate reader over the same
/// storage, so every query loads the ledger as published rather than reusing
/// the writer's in-memory state.
async fn seeded_reader(ledger_id: &str, data: &Value, storage: Storage) -> MemoryFluree {
    let (writer, reader) = instances_sharing_storage();
    let ledger = genesis_ledger(&writer, ledger_id);
    let committed = writer.insert(ledger, data).await.expect("seed").ledger;
    if let Storage::Indexed = storage {
        rebuild_and_publish_index(&writer, ledger_id).await;
    }
    let loaded = reader.ledger(ledger_id).await.expect("reader load");
    match storage {
        Storage::NoveltyOnly => assert_eq!(loaded.index_t(), 0, "expected no index"),
        Storage::Indexed => assert_eq!(
            loaded.index_t(),
            committed.t(),
            "expected the reader to load the published index"
        ),
    }
    reader
}

fn denying(property: &str) -> Value {
    json!({
        "policy": [{
            "@id": "ex:denyProperty",
            "f:required": true,
            "f:onProperty": [{"@id": property}],
            "f:action": "f:view",
            "f:allow": false
        }],
        "default-allow": true
    })
}

fn restricted(query: &Value, property: &str) -> Value {
    let mut query = query.clone();
    query["opts"] = denying(property);
    query
}

async fn rows(fluree: &MemoryFluree, query: &Value) -> Vec<Value> {
    let ledger_id = query["from"].as_str().expect("query names its ledger");
    let result = fluree
        .query_connection(query)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{query:#}"));
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    normalize_rows(&result.to_jsonld(&ledger.snapshot).expect("to_jsonld"))
}

fn query(ledger_id: &str, select: Value, where_: Value, reasoning: &str) -> Value {
    json!({
        "@context": context(),
        "from": ledger_id,
        "select": select,
        "where": where_,
        "reasoning": reasoning
    })
}

const ANCESTOR: &str = "http://example.org/ancestor";
const UNRELATED: &str = "http://example.org/unrelated";

#[tokio::test]
async fn denied_derived_predicate_is_hidden_from_every_row_shape() {
    for storage in STORAGE {
        let ledger_id = format!("reasoning-policy/denied-head-{storage:?}:main");
        let fluree = seeded_reader(&ledger_id, &family_with_rules(), storage).await;

        let scan = query(
            &ledger_id,
            json!(["?a"]),
            json!({"@id": "ex:alice", "ex:ancestor": "?a"}),
            "datalog",
        );
        let join = query(
            &ledger_id,
            json!(["?a", "?name"]),
            json!([
                {"@id": "ex:alice", "ex:ancestor": "?a"},
                {"@id": "?a", "ex:name": "?name"}
            ]),
            "datalog",
        );
        let count = query(
            &ledger_id,
            json!(["(count ?a)"]),
            json!({"@id": "?x", "ex:ancestor": "?a"}),
            "datalog",
        );
        let mut path = query(
            &ledger_id,
            json!(["?a"]),
            json!({"@id": "ex:alice", "ancestorPlus": "?a"}),
            "datalog",
        );
        path["@context"]["ancestorPlus"] = json!({"@path": "ex:ancestor+"});

        let ancestors = vec![json!(["ex:bob"]), json!(["ex:carol"]), json!(["ex:dave"])];
        let expectations = [
            ("scan", &scan, ancestors.clone()),
            (
                "join",
                &join,
                vec![
                    json!(["ex:bob", "Bob"]),
                    json!(["ex:carol", "Carol"]),
                    json!(["ex:dave", "Dave"]),
                ],
            ),
            ("count", &count, vec![json!([6])]),
            ("path", &path, ancestors),
        ];

        for (shape, q, expected_root) in expectations {
            let root_rows = rows(&fluree, q).await;
            assert_eq!(
                root_rows, expected_root,
                "{storage:?}/{shape}: root should see every derived ancestor"
            );
            assert_eq!(
                rows(&fluree, &restricted(q, UNRELATED)).await,
                expected_root,
                "{storage:?}/{shape}: a policy that does not name ex:ancestor leaves the \
                 derived rows visible"
            );
            let hidden = rows(&fluree, &restricted(q, ANCESTOR)).await;
            let leaked = match shape {
                "count" => hidden != vec![json!([0])] && !hidden.is_empty(),
                _ => !hidden.is_empty(),
            };
            assert!(
                !leaked,
                "{storage:?}/{shape}: a reader denied ex:ancestor must not see derived \
                 ancestor rows, got {hidden:?}"
            );
        }

        // The same reader still sees the premises: the policy hides the
        // derived predicate only.
        let parent = query(
            &ledger_id,
            json!(["?p"]),
            json!({"@id": "ex:alice", "ex:parent": "?p"}),
            "datalog",
        );
        assert_eq!(
            rows(&fluree, &restricted(&parent, ANCESTOR)).await,
            vec![json!(["ex:bob"])],
            "{storage:?}: the asserted premise stays visible"
        );
    }
}

#[tokio::test]
async fn restricted_first_caller_does_not_narrow_the_shared_closure() {
    for storage in STORAGE {
        let ledger_id = format!("reasoning-policy/restricted-first-{storage:?}:main");
        let fluree = seeded_reader(&ledger_id, &family_with_rules(), storage).await;
        let scan = query(
            &ledger_id,
            json!(["?a"]),
            json!({"@id": "ex:alice", "ex:ancestor": "?a"}),
            "datalog",
        );

        assert!(
            rows(&fluree, &restricted(&scan, ANCESTOR)).await.is_empty(),
            "{storage:?}: the restricted reader is filtered"
        );
        assert_eq!(
            rows(&fluree, &scan).await,
            vec![json!(["ex:bob"]), json!(["ex:carol"]), json!(["ex:dave"])],
            "{storage:?}: a closure first built for a restricted reader must still \
             serve root the complete derivation"
        );
    }
}

#[tokio::test]
async fn allowed_head_derived_from_hidden_premise_stays_visible() {
    const SALARY: &str = "http://example.org/salary";
    for storage in STORAGE {
        let ledger_id = format!("reasoning-policy/hidden-premise-{storage:?}:main");
        let fluree = seeded_reader(&ledger_id, &family_with_rules(), storage).await;

        let salary = query(
            &ledger_id,
            json!(["?s"]),
            json!({"@id": "ex:alice", "ex:salary": "?s"}),
            "datalog",
        );
        let high_earners = query(
            &ledger_id,
            json!(["?p"]),
            json!({"@id": "?p", "@type": "ex:HighEarner"}),
            "datalog",
        );

        assert_eq!(rows(&fluree, &salary).await, vec![json!([250_000])]);
        assert_eq!(
            rows(&fluree, &high_earners).await,
            vec![json!(["ex:alice"])]
        );

        assert!(
            rows(&fluree, &restricted(&salary, SALARY)).await.is_empty(),
            "{storage:?}: the premise itself is hidden"
        );
        assert_eq!(
            rows(&fluree, &restricted(&high_earners, SALARY)).await,
            vec![json!(["ex:alice"])],
            "{storage:?}: a derived fact is filtered by its own predicate, not its \
             premises, so a viewable head re-expresses the hidden salary"
        );
    }
}

/// The two reasoning lanes disagree under the same policy, by design. With
/// `ex:ssn rdfs:subPropertyOf ex:identifier` and `ex:ssn` hidden, RDFS
/// rewrites `ex:identifier` into a scan of `ex:ssn` under the caller's
/// identity, which the policy filters. OWL 2 RL materializes an
/// `ex:identifier` fact, which the policy allows.
#[tokio::test]
async fn rewriting_and_materialization_lanes_differ_under_policy() {
    const SSN: &str = "http://example.org/ssn";
    let data = json!({
        "@context": context(),
        "@graph": [
            {"@id": "ex:ssn", "rdfs:subPropertyOf": {"@id": "ex:identifier"}},
            {"@id": "ex:alice", "ex:ssn": "111-22-3333"}
        ]
    });
    for storage in STORAGE {
        let ledger_id = format!("reasoning-policy/lanes-{storage:?}:main");
        let fluree = seeded_reader(&ledger_id, &data, storage).await;
        let identifier = |reasoning| {
            query(
                &ledger_id,
                json!(["?s", "?id"]),
                json!({"@id": "?s", "ex:identifier": "?id"}),
                reasoning,
            )
        };
        let alice = vec![json!(["ex:alice", "111-22-3333"])];

        for lane in ["rdfs", "owl2rl"] {
            assert_eq!(
                rows(&fluree, &identifier(lane)).await,
                alice,
                "{storage:?}/{lane}: root sees the entailed identifier"
            );
        }
        assert!(
            rows(&fluree, &restricted(&identifier("rdfs"), SSN))
                .await
                .is_empty(),
            "{storage:?}/rdfs: rewriting scans the hidden ex:ssn under the caller's policy"
        );
        assert_eq!(
            rows(&fluree, &restricted(&identifier("owl2rl"), SSN)).await,
            alice,
            "{storage:?}/owl2rl: the materialized ex:identifier fact is judged by its \
             own predicate"
        );
    }
}

/// Invoices with three stored rules: a negation (`ex:unpaid` when no
/// payment exists), an aggregate (`ex:invoiceCount` per customer) and a head
/// that derives into the `ex:audit` named graph (`ex:flagged` over 1,000).
fn invoices_with_rules() -> Value {
    let rule_ctx = json!({"ex": "http://example.org/"});
    json!({
        "@context": context(),
        "@graph": [
            stored_rule("ex:unpaidRule", json!({
                "@context": rule_ctx,
                "where": [
                    {"@id": "?i", "@type": "ex:Invoice"},
                    ["not-exists", {"@id": "?i", "ex:paidBy": "?p"}]
                ],
                "insert": {"@id": "?i", "ex:unpaid": true}
            })),
            stored_rule("ex:invoiceCountRule", json!({
                "@context": rule_ctx,
                "where": [["query", {
                    "@context": rule_ctx,
                    "select": ["?c", "(as (count ?i) ?n)"],
                    "where": {"@id": "?i", "ex:customer": "?c"},
                    "groupBy": ["?c"]
                }]],
                "insert": {"@id": "?c", "ex:invoiceCount": "?n"}
            })),
            stored_rule("ex:flagRule", json!({
                "@context": rule_ctx,
                "where": [{"@id": "?i", "ex:amount": "?a"}, ["filter", "(> ?a 1000)"]],
                "insert": {"@id": "?i", "@graph": AUDIT, "ex:flagged": true}
            })),
            {"@id": "ex:auditLog", "@graph": AUDIT, "ex:note": "audit trail"},
            {"@id": "ex:inv1", "@type": "ex:Invoice", "ex:customer": {"@id": "ex:acme"},
             "ex:paidBy": {"@id": "ex:pay1"}, "ex:amount": 5000},
            {"@id": "ex:inv2", "@type": "ex:Invoice", "ex:customer": {"@id": "ex:acme"},
             "ex:amount": 200},
            {"@id": "ex:inv3", "@type": "ex:Invoice", "ex:customer": {"@id": "ex:globex"},
             "ex:amount": 1500}
        ]
    })
}

const AUDIT: &str = "http://example.org/audit";

async fn sorted_rows(fluree: &MemoryFluree, query: &Value) -> Vec<Value> {
    let mut rows = rows(fluree, query).await;
    rows.sort_by_key(ToString::to_string);
    rows
}

/// For a derived predicate computed by negation, aggregation or into a named
/// graph: a reader who cannot see a premise still gets the conclusions
/// computed from all the data, and a reader denied the derived predicate gets
/// nothing. The restricted reader queries first, so it is the one that
/// materializes; a closure computed from its view would call a paid invoice
/// unpaid and undercount.
#[tokio::test]
async fn negation_aggregation_and_named_graph_heads_follow_the_contract() {
    for storage in STORAGE {
        let cases = [
            (
                "negation",
                json!(["?i"]),
                json!({"@id": "?i", "ex:unpaid": true}),
                "http://example.org/paidBy",
                "http://example.org/unpaid",
                vec![json!(["ex:inv2"]), json!(["ex:inv3"])],
            ),
            (
                "aggregate",
                json!(["?c", "?n"]),
                json!({"@id": "?c", "ex:invoiceCount": "?n"}),
                "http://example.org/customer",
                "http://example.org/invoiceCount",
                vec![json!(["ex:acme", 2]), json!(["ex:globex", 1])],
            ),
            (
                "named-graph",
                json!(["?i"]),
                json!([["graph", AUDIT, {"@id": "?i", "ex:flagged": true}]]),
                "http://example.org/amount",
                "http://example.org/flagged",
                vec![json!(["ex:inv1"]), json!(["ex:inv3"])],
            ),
        ];
        for (shape, select, where_, premise, head, expected) in cases {
            let ledger_id = format!("reasoning-policy/{shape}-{storage:?}:main").to_lowercase();
            let fluree = seeded_reader(&ledger_id, &invoices_with_rules(), storage).await;
            let q = query(&ledger_id, select, where_, "datalog");
            let premise_rows = query(
                &ledger_id,
                json!(["?s", "?o"]),
                json!({"@id": "?s", premise: "?o"}),
                "datalog",
            );
            assert!(
                sorted_rows(&fluree, &restricted(&premise_rows, premise))
                    .await
                    .is_empty(),
                "{storage:?}/{shape}: the reader cannot see the premise itself"
            );
            assert_eq!(
                sorted_rows(&fluree, &restricted(&q, premise)).await,
                expected,
                "{storage:?}/{shape}: hiding a premise from the reader must not change \
                 what the shared closure concludes"
            );
            assert_eq!(
                sorted_rows(&fluree, &q).await,
                expected,
                "{storage:?}/{shape}: root"
            );
            assert!(
                sorted_rows(&fluree, &restricted(&q, head)).await.is_empty(),
                "{storage:?}/{shape}: a reader denied the derived predicate sees none of it"
            );
        }
    }
}
