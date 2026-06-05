//! Explain API integration tests
//!
//! The native/statistics-backed tests live in `it_query_explain_native.rs`.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use std::sync::{Mutex, MutexGuard, OnceLock};
use support::{genesis_ledger, graphdb_from_ledger};

/// Serializes `FLUREE_HASH_JOIN` access across this file's tests and restores the
/// prior value on drop. `HashJoinPlanner` reads this var at plan time, so a test
/// that forces the hash join must not run while another builds a plan, and must not
/// leak the override on panic. Every plan-building test holds one for its duration;
/// `acquire()` starts each test in the default (AUTO) mode.
struct HashJoinEnv {
    _guard: MutexGuard<'static, ()>,
    prev: Option<String>,
}

impl HashJoinEnv {
    fn acquire() -> Self {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let guard = LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = std::env::var("FLUREE_HASH_JOIN").ok();
        std::env::remove_var("FLUREE_HASH_JOIN");
        Self {
            _guard: guard,
            prev,
        }
    }
    fn force_on(&self) {
        std::env::set_var("FLUREE_HASH_JOIN", "1");
    }
}

impl Drop for HashJoinEnv {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var("FLUREE_HASH_JOIN", v),
            None => std::env::remove_var("FLUREE_HASH_JOIN"),
        }
    }
}

#[tokio::test]
async fn explain_no_stats_reports_none_and_reason() {
    let _env = HashJoinEnv::acquire();
    // Scenario: explain-no-stats-test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "no-stats:main");

    // Ensure the `ex` namespace is allocated (so query parsing can encode IRIs),
    // but do NOT run indexing so stats remain unavailable.
    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:alice",
                "ex:name":"Alice"
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let q = json!({
        "@context": {"ex":"http://example.org/"},
        "select": ["?person"],
        "where": [{"@id":"?person","ex:name":"?name"}]
    });

    let db = graphdb_from_ledger(&ledger);
    let resp = fluree.explain(&db, &q).await.expect("explain");
    assert_eq!(resp["plan"]["optimization"], "none");
    assert_eq!(resp["plan"]["reason"], "No statistics available");
    assert!(resp.get("query").is_some());
    assert!(resp["plan"].get("where-clause").is_some());
}

#[tokio::test]
async fn explain_sparql_no_stats_reports_none_and_reason() {
    let _env = HashJoinEnv::acquire();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "no-stats-sparql:main");

    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:alice",
                "ex:name":"Alice"
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?person WHERE { ?person ex:name ?name }";

    let db = graphdb_from_ledger(&ledger);
    let resp = fluree
        .explain_sparql(&db, sparql)
        .await
        .expect("explain_sparql");
    assert_eq!(resp["plan"]["optimization"], "none");
    assert_eq!(resp["plan"]["reason"], "No statistics available");
    assert!(resp.get("query").is_some());
    // SPARQL explain does not include where-clause (that's a JSON-LD concept)
    assert!(resp["plan"].get("where-clause").is_none());
}

#[tokio::test]
async fn explain_physical_plan_present_and_concretely_named() {
    let _env = HashJoinEnv::acquire();
    // plan.physical is built from the REAL operator tree (build-only, no exec).
    // Validate it is present and operators resolve to concrete names (the
    // default op_name() must see through dyn dispatch, not report "dyn Operator").
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "physical-names:main");

    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:alice",
                "ex:name":"Alice"
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let sparql =
        "PREFIX ex: <http://example.org/>\nSELECT ?person ?name WHERE { ?person ex:name ?name }";

    let db = graphdb_from_ledger(&ledger);
    let resp = fluree
        .explain_sparql(&db, sparql)
        .await
        .expect("explain_sparql");

    let physical = &resp["plan"]["physical"];
    eprintln!(
        "PHYSICAL = {}",
        serde_json::to_string_pretty(physical).unwrap()
    );
    assert!(
        physical.get("error").is_none(),
        "physical build errored: {physical}"
    );
    let op = physical["op"].as_str().expect("physical.op is a string");
    assert!(!op.is_empty());
    assert!(
        !op.contains("dyn Operator"),
        "operator name should be concrete, got {op:?}"
    );
    // The tree is connected (not truncated at the root): the scan leaf is reachable.
    assert!(
        physical_contains_op(physical, "DatasetOperator"),
        "expected a DatasetOperator scan leaf in the physical tree: {physical}"
    );
}

/// Recursively search a `plan.physical` node (and its `children[].node`) for an
/// operator whose `op` equals `name`.
fn physical_contains_op(node: &serde_json::Value, name: &str) -> bool {
    physical_find_op(node, name).is_some()
}

/// Recursively find the first `plan.physical` node whose `op` equals `name`.
fn physical_find_op<'a>(node: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    if node["op"] == name {
        return Some(node);
    }
    node["children"]
        .as_array()
        .and_then(|cs| cs.iter().find_map(|e| physical_find_op(&e["node"], name)))
}

#[tokio::test]
async fn explain_physical_object_subject_join_shows_hash_join_decision() {
    // 2-pattern object→subject join: `?b ex:knows ?x` with `?x` bound from
    // `?a ex:knows ?x`. This is the exact shape the object→subject hash join
    // targets. Default (no stats): the planner keeps the NestedLoop and records
    // the rejected hash-join reason. FLUREE_HASH_JOIN=1: the hash join is chosen.
    let env = HashJoinEnv::acquire(); // AUTO mode; restored (and serialized) via guard
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "physical-join:main");
    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:alice",
                "ex:knows": {"@id":"ex:bob"}
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let sparql =
        "PREFIX ex: <http://example.org/>\nSELECT ?a ?b WHERE { ?a ex:knows ?x . ?b ex:knows ?x }";
    let db = graphdb_from_ledger(&ledger);

    // Default (AUTO): nested-loop join, hash join evaluated and rejected.
    let resp = fluree
        .explain_sparql(&db, sparql)
        .await
        .expect("explain_sparql");
    let physical = &resp["plan"]["physical"];
    eprintln!(
        "PHYSICAL(nl) = {}",
        serde_json::to_string_pretty(physical).unwrap()
    );
    let nl = physical_find_op(physical, "NestedLoopJoinOperator")
        .expect("expected a NestedLoopJoinOperator in physical plan");
    assert_eq!(
        nl["details"]["hash-join-chosen"], false,
        "nested loop should record the rejected hash join"
    );
    assert!(
        nl["details"]["hash-join-reason"].is_string(),
        "nested loop should carry a hash-join reason"
    );

    // Forced: the object→subject hash join is selected.
    env.force_on();
    let resp_forced = fluree
        .explain_sparql(&db, sparql)
        .await
        .expect("explain_sparql");
    let physical_forced = &resp_forced["plan"]["physical"];
    eprintln!(
        "PHYSICAL(hash) = {}",
        serde_json::to_string_pretty(physical_forced).unwrap()
    );
    let hj = physical_find_op(physical_forced, "HashJoinOperator")
        .expect("expected a HashJoinOperator under FLUREE_HASH_JOIN=1");
    assert_eq!(hj["details"]["hash-join-chosen"], true);
    assert_eq!(hj["details"]["hash-join-reason"], "forced-on");
}

#[tokio::test]
async fn explain_logical_estimates_are_bound_var_aware() {
    let _env = HashJoinEnv::acquire();
    // `?s ex:p ?o . ?o ex:q ?x`: the second triple's subject ?o is bound by the
    // first, so its logical estimate must be the (selective) bound-subject estimate,
    // not a full predicate scan as if no earlier variable were bound. Even without
    // stats the defaults differ (bound-subject vs property-scan), so this catches a
    // node rendered with an empty bound set.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "logical-bound:main");
    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:a",
                "ex:p": {"@id":"ex:b", "ex:q": {"@id":"ex:c"}}
            }),
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&ledger);
    let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?s ?x WHERE { ?s ex:p ?o . ?o ex:q ?x }";
    let resp = fluree
        .explain_sparql(&db, sparql)
        .await
        .expect("explain_sparql");
    let logical = resp["plan"]["logical"]
        .as_array()
        .expect("logical plan array");
    assert_eq!(logical.len(), 2, "two triples expected: {logical:?}");
    let row_count = |n: &serde_json::Value| n["estimate"]["row-count"].as_i64();
    let first = row_count(&logical[0]).expect("first row-count");
    let second = row_count(&logical[1]).expect("second row-count");
    assert!(
        second < first,
        "bound-subject second triple must estimate below the first full scan: {first} then {second}"
    );
}

#[tokio::test]
async fn explain_physical_plan_surfaces_fast_path() {
    let _env = HashJoinEnv::acquire();
    // A COUNT(*) over a predicate is answered by a metadata fast path. The
    // planner selects it at build time, so plan.physical names the fast-path
    // operator (label-tagged) — the signal the pattern-level views cannot give.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "physical-fastpath:main");

    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:alice",
                "ex:name":"Alice"
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let sparql =
        "PREFIX ex: <http://example.org/>\nSELECT (COUNT(?o) AS ?c) WHERE { ?s ex:name ?o }";

    let db = graphdb_from_ledger(&ledger);
    let resp = fluree
        .explain_sparql(&db, sparql)
        .await
        .expect("explain_sparql");

    let physical = &resp["plan"]["physical"];
    eprintln!(
        "PHYSICAL(count) = {}",
        serde_json::to_string_pretty(physical).unwrap()
    );
    let s = serde_json::to_string(physical).unwrap();
    assert!(
        s.contains("FastPath"),
        "expected a fast-path operator in physical plan: {s}"
    );
}

#[tokio::test]
async fn explain_physical_expands_subquery_inner_plan() {
    let _env = HashJoinEnv::acquire();
    // BSBM-BI queries are subquery-based; the inner joins are where the time is.
    // The SubqueryOperator builds its subplan lazily, so explain rebuilds it
    // (build-only) and exposes it under a `SubqueryBody` node.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "physical-subquery:main");
    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:alice",
                "ex:name":"Alice",
                "ex:knows": {"@id":"ex:bob"}
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let sparql = "PREFIX ex: <http://example.org/>\n\
        SELECT ?a ?c WHERE { \
          ?a ex:name ?n . \
          { SELECT ?a (COUNT(?x) AS ?c) WHERE { ?a ex:knows ?x } GROUP BY ?a } \
        }";
    let db = graphdb_from_ledger(&ledger);
    let resp = fluree
        .explain_sparql(&db, sparql)
        .await
        .expect("explain_sparql");
    let physical = &resp["plan"]["physical"];
    eprintln!(
        "PHYSICAL(subquery) = {}",
        serde_json::to_string_pretty(physical).unwrap()
    );

    let sub = physical_find_op(physical, "SubqueryOperator")
        .expect("expected a SubqueryOperator in physical plan");
    // The inner plan is expanded under a SubqueryBody node, not opaque.
    let body = physical_find_op(sub, "SubqueryBody")
        .expect("subquery inner plan should be expanded under SubqueryBody");
    // The inner operator tree is visible (a real operator under the body).
    assert!(
        body["children"].as_array().is_some_and(|cs| !cs.is_empty()),
        "SubqueryBody should contain the inner operator tree"
    );
    // The inner aggregation (the GROUP BY / COUNT) is no longer opaque.
    assert!(
        physical_find_op(body, "GroupAggregateOperator").is_some(),
        "inner subquery aggregation should be visible: {body}"
    );
}

#[tokio::test]
async fn explain_logical_plan_preserves_compound_structure() {
    let _env = HashJoinEnv::acquire();
    // The `logical` plan view is the compound-aware reorder_patterns order,
    // available even without stats. Verify a triple + OPTIONAL render as a
    // `triple` node and an `optional` node containing its inner triple.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "logical-compound:main");

    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:alice",
                "ex:name":"Alice",
                "ex:email":"alice@example.org"
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?person ?email WHERE { ?person ex:name ?name . OPTIONAL { ?person ex:email ?email } }";

    let db = graphdb_from_ledger(&ledger);
    let resp = fluree
        .explain_sparql(&db, sparql)
        .await
        .expect("explain_sparql");

    let logical = resp["plan"]["logical"]
        .as_array()
        .expect("plan.logical is an array");
    assert!(!logical.is_empty(), "logical plan should not be empty");

    // Every node carries a kind + category.
    assert!(logical
        .iter()
        .all(|n| n.get("kind").is_some() && n.get("category").is_some()));

    // The OPTIONAL renders as an expander node holding its inner triple,
    // not flattened away.
    let optional = logical
        .iter()
        .find(|n| n["kind"] == "optional")
        .expect("optional node present in logical plan");
    assert_eq!(optional["category"], "expander");
    let inner = optional["patterns"]
        .as_array()
        .expect("optional has inner patterns");
    assert!(inner.iter().any(|n| n["kind"] == "triple"));

    // The required triple is a source.
    let triple = logical
        .iter()
        .find(|n| n["kind"] == "triple")
        .expect("required triple present");
    assert_eq!(triple["category"], "source");
}
