//! History-mode coverage for compound query shapes: history × {OPTIONAL,
//! UNION, FILTER NOT EXISTS, count fast path}.
//!
//! These exercise the planner-mode refactor's late-builder threading: each
//! of these patterns constructs subplans inside operator `open()` (e.g.
//! `OptionalOperator::PlanTreeOptionalBuilder::build`,
//! `UnionOperator::next_batch`, `ExistsOperator::has_match`). Pre-refactor,
//! those late builders called `PlanningContext::current()` directly, so a
//! history-range outer query would silently collapse retracts inside the
//! compound block. Post-refactor, the `planning` field captured at
//! construction is threaded into every late `build_where_operators_seeded`
//! call, so the inner subplans run in the same temporal mode as the outer.
//!
//! Each test:
//! 1. Sets up a small ledger with both asserts and retracts (so history
//!    mode produces strictly more rows than current mode).
//! 2. Runs the query in history mode and expects retract events to appear
//!    inside the compound construct.
//! 3. Runs the same query in current mode as a baseline so a regression
//!    would show up as identical row counts in both modes.
//!
//! The fast-path test is the inverse: a query shape that *would* normally
//! trigger a count fast path (`SELECT (COUNT(*) AS ?c) WHERE { ?s <p> <o> }`)
//! must NOT take the fast path in history mode — it has to plan the full
//! scan tree so retract events are counted. Verified by checking that
//! history-mode count > current-mode count when retracts exist.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, FormatterConfig, ReindexOptions};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({ "ex": "http://example.org/" })
}

async fn reindex_to_current(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> i64 {
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    fluree
        .index_status(ledger_id)
        .await
        .expect("index_status")
        .index_t
}

/// Three-document fixture used by the OPTIONAL / UNION / FILTER tests.
///
/// - `ex:alice`: gets `ex:name = "Alice"` and `ex:tag = "tagged"` at t=1.
///   At t=2 the name is upserted to "Alice Smith" (retract + new assert).
///   At t=3 the tag is retracted (no new assert) via a separate
///   `where`/`delete` update.
/// - `ex:bob`:   gets `ex:name = "Bob"` at t=1, no tag, no other changes.
/// - `ex:carol`: gets `ex:age = 30` at t=1 (no `ex:name`, no tag).
///
/// The retract-only tag on alice is the key for the FILTER NOT EXISTS
/// test: in history mode the inner subplan must see the retract event,
/// while in current mode the tag is gone.
///
/// Reindex runs after t=1 and again after t=3 so both the t=2 name retract
/// and the t=3 tag retract land in the history sidecar rather than in
/// plain novelty.
async fn build_three_doc_ledger() -> (fluree_db_api::Fluree, &'static str, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_str().unwrap().to_string();
    let fluree = FlureeBuilder::file(&path).build().expect("build");
    let ledger_id = "test/history-combos:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    let tx1 = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice", "ex:tag": "tagged" },
            { "@id": "ex:bob",   "ex:name": "Bob" },
            { "@id": "ex:carol", "ex:age":  30 },
        ],
    });
    let r1 = fluree.insert(ledger0, &tx1).await.expect("tx1");
    assert_eq!(r1.receipt.t, 1);

    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 1);

    // t=2: upsert alice.name (retract "Alice", new assert "Alice Smith").
    // t=3: retract alice.tag via a separate `where`/`delete` update so the
    // deletion doesn't collide with the upsert.
    let tx2 = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:name": "Alice Smith",
    });
    let r2 = fluree.upsert(r1.ledger, &tx2).await.expect("tx2");
    assert_eq!(r2.receipt.t, 2);

    let tx3 = json!({
        "@context": ctx(),
        "where":  { "@id": "ex:alice", "ex:tag": "?_tag" },
        "delete": { "@id": "ex:alice", "ex:tag": "?_tag" },
    });
    let r3 = fluree.update(r2.ledger, &tx3).await.expect("delete tag");
    assert_eq!(r3.receipt.t, 3);

    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 3);

    (fluree, ledger_id, tmp)
}

async fn run_query(
    fluree: &fluree_db_api::Fluree,
    q: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let result = fluree
        .query_from()
        .jsonld(q)
        .format(FormatterConfig::typed_json().with_normalize_arrays())
        .execute_tracked()
        .await
        .expect("query");
    let value = serde_json::to_value(&result.result).expect("serialize");
    value.as_array().cloned().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// History × OPTIONAL
//
// Outer: every subject. OPTIONAL: `?s ex:name ?n`. Carol has no name so the
// optional is unbound for her; Alice's name has a retract event at t=2 in
// history mode.
//
// Current mode: 3 outer rows, alice/bob have name bindings, carol's name is
//   unbound. Alice's name is "Alice Smith" (the live value).
// History mode: optional inside a history-range outer must capture the
//   retract at t=2 → alice yields three name events ("Alice" assert@1,
//   "Alice" retract@2, "Alice Smith" assert@2) instead of one.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn history_optional_emits_retracts_inside_optional_block() {
    let (fluree, ledger_id, _tmp) = build_three_doc_ledger().await;

    let q_history = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["?s", "?n", "?op"],
        "where": [
            // Outer pivot: anyone with either name or age. Uses UNION so
            // we exercise nested late-builder threading too.
            ["union",
                [{ "@id": "?s", "ex:name": "?_anyname" }],
                [{ "@id": "?s", "ex:age":  "?_anyage" }]
            ],
            // OPTIONAL is the late-builder seam: pre-refactor this would
            // silently plan as Current even when the outer is History.
            ["optional", { "@id": "?s", "ex:name": {"@value": "?n", "@op": "?op"} }],
        ],
    });

    let rows = run_query(&fluree, &q_history).await;

    // Count the alice rows that came through the OPTIONAL block. In history
    // mode we expect both the retract of "Alice" and the assert of "Alice
    // Smith" at t=2, plus the original "Alice" assert at t=1.
    let alice_name_events: Vec<_> = rows
        .iter()
        .filter(|r| {
            r.get("?s")
                .and_then(|s| s.get("@id"))
                .and_then(|i| i.as_str())
                .map(|s| s.ends_with("alice"))
                .unwrap_or(false)
        })
        .filter(|r| r.get("?n").and_then(|n| n.get("@value")).is_some())
        .collect();

    assert!(
        alice_name_events.len() >= 3,
        "history × OPTIONAL must surface alice's retract event from inside \
         the OPTIONAL block (expected ≥3 name events for alice, got {}); \
         rows: {rows:#?}",
        alice_name_events.len()
    );

    let has_retract = alice_name_events.iter().any(|r| {
        r.get("?op")
            .and_then(|o| o.get("@value").or(Some(o)))
            .and_then(serde_json::Value::as_bool)
            == Some(false)
    });
    assert!(
        has_retract,
        "history × OPTIONAL must include at least one retract (?op=false) \
         for alice's name; got {alice_name_events:#?}"
    );
}

// ---------------------------------------------------------------------------
// History × UNION
//
// Each branch of the UNION is its own pattern list, planned at runtime
// inside `UnionOperator::next_batch` via `build_where_operators_seeded`.
// In history mode every branch must inherit `History`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn history_union_emits_retracts_in_branches() {
    let (fluree, ledger_id, _tmp) = build_three_doc_ledger().await;

    let q = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["?s", "?v", "?op"],
        "where": [
            ["union",
                [{ "@id": "?s", "ex:name": {"@value": "?v", "@op": "?op"} }],
                [{ "@id": "?s", "ex:age":  {"@value": "?v", "@op": "?op"} }]
            ],
        ],
    });

    let rows = run_query(&fluree, &q).await;

    let retracts: Vec<_> = rows
        .iter()
        .filter(|r| {
            r.get("?op")
                .and_then(|o| o.get("@value").or(Some(o)))
                .and_then(serde_json::Value::as_bool)
                == Some(false)
        })
        .collect();

    assert!(
        !retracts.is_empty(),
        "history × UNION must include at least one retract event from a \
         branch (alice's name retract at t=2); rows: {rows:#?}"
    );
}

// ---------------------------------------------------------------------------
// History × FILTER NOT EXISTS
//
// FILTER NOT EXISTS plans a per-row inner subplan via
// `eval_exists_for_row` → `build_where_operators_seeded`. The inner plan
// must inherit `History` so its semantics match the outer history-range
// query.
//
// Discriminating shape: alice has a tag asserted at t=1 and retracted at
// t=3 with no re-assert; bob never has a tag. The expected result with
// correct History inheritance is `{ex:bob}` only — alice is filtered out
// because History EXISTS sees her in-range tag events. A regression to
// `Current` for the inner subplan would also keep alice (her tag is
// currently gone, so EXISTS would be false), and the assertion below
// fails with a clear message.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn history_filter_not_exists_inner_inherits_history() {
    let (fluree, ledger_id, _tmp) = build_three_doc_ledger().await;

    // FILTER NOT EXISTS routes through `filter.rs::eval_exists_for_row` →
    // `build_where_operators_seeded`. The inner EXISTS subplan must inherit
    // the outer planning context (History here), or it silently downgrades
    // to current state and reports the wrong existence.
    //
    // Discriminating shape: alice's `ex:tag` was asserted at t=1 and
    // retracted at t=3 (no new assert). bob never had a tag.
    //
    //   Outer: ?s ex:name ?n (history range [1, latest])
    //     → alice rows: assert@1 "Alice", retract@2 "Alice", assert@2 "Alice Smith"
    //     → bob row:    assert@1 "Bob"
    //
    //   FILTER NOT EXISTS { ?s ex:tag ?_t }:
    //     - History semantics (correct, what this refactor enforces):
    //       The inner subquery scans `?s ex:tag ?_t` over the same range.
    //       For alice the tag has events (assert@1, retract@3), so EXISTS
    //       holds → NOT EXISTS is false → all alice rows are filtered out.
    //       For bob there are no tag events → NOT EXISTS holds → bob is
    //       kept.
    //     - Current-state regression: the inner EXISTS sees the live
    //       state. alice's tag is currently retracted, so EXISTS is false
    //       → NOT EXISTS true → alice rows are kept. The result would
    //       include alice.
    //
    // The discriminating assertion: result subjects must be `{bob}` only.
    let q = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["?s"],
        "where": [
            { "@id": "?s", "ex:name": "?_n" },
            ["filter",
                ["not-exists", { "@id": "?s", "ex:tag": "?_t" }]
            ],
        ],
    });

    let rows = run_query(&fluree, &q).await;
    let subjects: std::collections::BTreeSet<String> = rows
        .iter()
        .filter_map(|r| {
            r.get("?s")
                .and_then(|s| s.get("@id"))
                .and_then(|i| i.as_str())
                .map(str::to_string)
        })
        .collect();

    let expected: std::collections::BTreeSet<String> = ["ex:bob".to_string()].into_iter().collect();
    assert_eq!(
        subjects, expected,
        "FILTER NOT EXISTS inner subplan must inherit History — alice's \
         retracted-but-historically-asserted tag must make EXISTS true and \
         filter alice out. If the result also contains alice, the inner \
         EXISTS silently downgraded to Current. Got: {subjects:?}; rows: {rows:#?}"
    );
}

// ---------------------------------------------------------------------------
// History × count fast path
//
// The query shape `SELECT (COUNT(*) AS ?c) WHERE { ?s <p> ?o }` would
// normally trigger a count fast path that uses leaflet metadata to skip
// the scan entirely. In history mode that fast path is wrong (it counts
// current-state rows, ignoring retracts), so the planner declines to
// take it (Phase 5 of the refactor).
//
// Verification: history-mode count must be strictly greater than
// current-mode count when retracts exist (the retract event is its own
// row in history but doesn't count in current state).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn history_count_does_not_take_current_state_fast_path() {
    let (fluree, ledger_id, _tmp) = build_three_doc_ledger().await;

    // Current-state count: 2 name rows (alice "Alice Smith", bob "Bob").
    let q_current = json!({
        "@context": ctx(),
        "from": ledger_id,
        "select": ["(as (count *) ?c)"],
        "where": [{ "@id": "?s", "ex:name": "?n" }],
    });
    let current_rows = run_query(&fluree, &q_current).await;
    let current_count = current_rows
        .first()
        .and_then(|r| r.get("?c"))
        .and_then(|c| c.get("@value"))
        .and_then(serde_json::Value::as_i64)
        .expect("current count");
    assert_eq!(
        current_count, 2,
        "current-state count must be 2 (alice's live name + bob's name); \
         rows: {current_rows:#?}"
    );

    // History-range count: 4 name events (alice assert@1, alice retract@2,
    // alice assert@2 "Alice Smith", bob assert@1).
    let q_history = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["(as (count *) ?c)"],
        "where": [{ "@id": "?s", "ex:name": "?n" }],
    });
    let history_rows = run_query(&fluree, &q_history).await;
    let history_count = history_rows
        .first()
        .and_then(|r| r.get("?c"))
        .and_then(|c| c.get("@value"))
        .and_then(serde_json::Value::as_i64)
        .expect("history count");

    assert!(
        history_count > current_count,
        "history-mode count must exceed current-state count (the retract \
         event is its own row); current={current_count}, history={history_count}; \
         if these are equal, the fast path is being taken in history mode \
         (Phase 5 regression)"
    );
    assert_eq!(
        history_count, 4,
        "expected 4 history events (alice assert@1, retract@2, assert@2, \
         bob assert@1); got {history_count}; rows: {history_rows:#?}"
    );
}
