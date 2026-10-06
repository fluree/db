//! Event-time commits + recorded-axis time travel integration tests
//!
//! Covers:
//! - Caller-supplied event times (`CommitOpts::timestamp`) driving `@iso:`
//!   time travel over backdated history
//! - Event-time monotonicity + future-bound rejection
//! - Sticky dual-stamp mode (`db:receivedAt`) and `@recorded:` resolution
//! - Plain ledgers staying free of receivedAt metadata, with `@recorded:`
//!   falling back to the event axis

#![cfg(feature = "native")]

use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use chrono::{SecondsFormat, Utc};
use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_core::{range_with_overlay, IndexType, RangeMatch, RangeTest};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::{json, Value as JsonValue};

fn ctx_test() -> JsonValue {
    json!({
        "test": "http://example.org/test#",
        "name": "test:name",
        "Person": "test:Person"
    })
}

fn person_tx(id: &str, name: &str) -> JsonValue {
    json!({
        "@context": ctx_test(),
        "@graph": [{"@id": format!("test:{id}"), "@type": "Person", "name": name}]
    })
}

fn test_index_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    }
}

async fn insert_at(
    fluree: &MemoryFluree,
    ledger: MemoryLedger,
    tx: &JsonValue,
    opts: CommitOpts,
) -> Result<MemoryLedger, fluree_db_api::ApiError> {
    fluree
        .insert_with_opts(ledger, tx, TxnOpts::default(), opts, &test_index_config())
        .await
        .map(|r| r.ledger)
}

async fn query_names_at(
    fluree: &MemoryFluree,
    db_for_formatting: fluree_db_core::GraphDbRef<'_>,
    from_spec: &str,
) -> Result<Vec<JsonValue>, fluree_db_api::ApiError> {
    let q = json!({
        "@context": ctx_test(),
        "from": [from_spec],
        "select": ["?name"],
        "where": [{"@id":"?s","name":"?name"}],
        "orderBy": ["?name"]
    });
    let result = fluree.query_connection(&q).await?;
    let jsonld = result.to_jsonld_async(db_for_formatting).await?;
    Ok(normalize_rows(&jsonld))
}

/// Count `db:receivedAt` flakes in the txn-meta graph, optionally at one t.
async fn received_at_flakes(
    ledger: &MemoryLedger,
    at_t: Option<i64>,
) -> Vec<fluree_db_core::Flake> {
    let pred = fluree_db_core::Sid::new(
        fluree_vocab::namespaces::FLUREE_DB,
        fluree_vocab::db::RECEIVED_AT,
    );
    let flakes = range_with_overlay(
        &ledger.snapshot,
        fluree_db_core::TXN_META_GRAPH_ID,
        ledger.novelty.as_ref(),
        IndexType::Post,
        RangeTest::Eq,
        RangeMatch::predicate(pred),
        fluree_db_core::RangeOptions::default().with_to_t(ledger.t()),
    )
    .await
    .expect("receivedAt range scan");
    match at_t {
        Some(t) => flakes.into_iter().filter(|f| f.t == t).collect(),
        None => flakes,
    }
}

// =============================================================================
// Backdated event times + @iso: time travel
// =============================================================================

#[tokio::test]
async fn backdated_event_times_drive_iso_time_travel() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/event-time-backdated:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Three commits with historical event times, years before "now".
    let ledger1 = insert_at(
        &fluree,
        ledger0,
        &person_tx("p1", "Alice"),
        CommitOpts::default().with_timestamp("2020-01-01T00:00:00Z"),
    )
    .await
    .expect("backdated commit t=1");
    let ledger2 = insert_at(
        &fluree,
        ledger1,
        &person_tx("p2", "Bob"),
        CommitOpts::default().with_timestamp("2021-01-01T00:00:00Z"),
    )
    .await
    .expect("backdated commit t=2");
    let ledger3 = insert_at(
        &fluree,
        ledger2,
        &person_tx("p3", "Carol"),
        CommitOpts::default().with_timestamp("2022-01-01T00:00:00Z"),
    )
    .await
    .expect("backdated commit t=3");

    let db = ledger3.as_graph_db_ref(0);

    // Time travel by event time lands between the historical commits.
    let names = query_names_at(
        &fluree,
        db,
        &format!("{ledger_id}@iso:2020-06-01T00:00:00Z"),
    )
    .await
    .expect("@iso mid-2020");
    assert_eq!(names, vec![json!(["Alice"])]);

    let names = query_names_at(
        &fluree,
        db,
        &format!("{ledger_id}@iso:2021-06-01T00:00:00Z"),
    )
    .await
    .expect("@iso mid-2021");
    assert_eq!(names, vec![json!(["Alice"]), json!(["Bob"])]);

    // At/after the last event time: full state.
    let names = query_names_at(
        &fluree,
        db,
        &format!("{ledger_id}@iso:2025-01-01T00:00:00Z"),
    )
    .await
    .expect("@iso 2025");
    assert_eq!(
        names,
        vec![json!(["Alice"]), json!(["Bob"]), json!(["Carol"])]
    );

    // Before the earliest event time: error.
    let err = query_names_at(
        &fluree,
        db,
        &format!("{ledger_id}@iso:2019-01-01T00:00:00Z"),
    )
    .await
    .expect_err("@iso 2019 should error");
    assert!(
        err.to_string().contains("no data as of"),
        "unexpected error: {err}"
    );
}

// =============================================================================
// Guard rejections
// =============================================================================

#[tokio::test]
async fn event_time_earlier_than_head_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/event-time-monotonic:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let ledger1 = insert_at(
        &fluree,
        ledger0,
        &person_tx("p1", "Alice"),
        CommitOpts::default().with_timestamp("2022-01-01T00:00:00Z"),
    )
    .await
    .expect("commit at 2022");

    let err = insert_at(
        &fluree,
        ledger1,
        &person_tx("p2", "Bob"),
        CommitOpts::default().with_timestamp("2021-01-01T00:00:00Z"),
    )
    .await
    .expect_err("earlier event time must be rejected");
    assert!(
        err.to_string().contains("monotonically non-decreasing"),
        "unexpected error: {err}"
    );

    // Equal event time is allowed (non-decreasing, not strictly increasing).
    let ledger1b = genesis_ledger(&fluree, "it/event-time-equal:main");
    let ledger2b = insert_at(
        &fluree,
        ledger1b,
        &person_tx("p1", "Alice"),
        CommitOpts::default().with_timestamp("2022-01-01T00:00:00Z"),
    )
    .await
    .expect("commit at 2022");
    insert_at(
        &fluree,
        ledger2b,
        &person_tx("p2", "Bob"),
        CommitOpts::default().with_timestamp("2022-01-01T00:00:00Z"),
    )
    .await
    .expect("equal event time is allowed");
}

#[tokio::test]
async fn future_event_time_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/event-time-future:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let tomorrow =
        (Utc::now() + chrono::Duration::days(1)).to_rfc3339_opts(SecondsFormat::Millis, true);
    let err = insert_at(
        &fluree,
        ledger0,
        &person_tx("p1", "Alice"),
        CommitOpts::default().with_timestamp(tomorrow),
    )
    .await
    .expect_err("future event time must be rejected");
    assert!(
        err.to_string().contains("future"),
        "unexpected error: {err}"
    );

    let ledger0 = genesis_ledger(&fluree, "it/event-time-garbage:main");
    let err = insert_at(
        &fluree,
        ledger0,
        &person_tx("p1", "Alice"),
        CommitOpts::default().with_timestamp("not-a-timestamp"),
    )
    .await
    .expect_err("unparseable event time must be rejected");
    assert!(
        err.to_string().contains("RFC 3339"),
        "unexpected error: {err}"
    );
}

// =============================================================================
// Dual-stamp mode + @recorded:
// =============================================================================

#[tokio::test]
async fn dual_stamp_recorded_axis_resolution() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/event-time-recorded:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Fully backdated ledger: event times 2020/2021/2022, recorded times
    // pinned to distinct 2026 instants (deterministic).
    let ledger1 = insert_at(
        &fluree,
        ledger0,
        &person_tx("p1", "Alice"),
        CommitOpts::default()
            .with_timestamp("2020-01-01T00:00:00Z")
            .with_received_at("2026-01-01T00:00:00Z"),
    )
    .await
    .expect("t=1");
    let ledger2 = insert_at(
        &fluree,
        ledger1,
        &person_tx("p2", "Bob"),
        CommitOpts::default()
            .with_timestamp("2021-01-01T00:00:00Z")
            .with_received_at("2026-01-02T00:00:00Z"),
    )
    .await
    .expect("t=2");
    let ledger3 = insert_at(
        &fluree,
        ledger2,
        &person_tx("p3", "Carol"),
        CommitOpts::default()
            .with_timestamp("2022-01-01T00:00:00Z")
            .with_received_at("2026-01-03T00:00:00Z"),
    )
    .await
    .expect("t=3");

    let db = ledger3.as_graph_db_ref(0);

    // Event axis: mid-2021 sees Alice + Bob.
    let names = query_names_at(
        &fluree,
        db,
        &format!("{ledger_id}@iso:2021-06-01T00:00:00Z"),
    )
    .await
    .expect("@iso mid-2021");
    assert_eq!(names, vec![json!(["Alice"]), json!(["Bob"])]);

    // Recorded axis: mid-recorded-day-2 sees Alice + Bob (t=2 recorded
    // 2026-01-02, t=3 not until 01-03) even though all EVENT times are past.
    let names = query_names_at(
        &fluree,
        db,
        &format!("{ledger_id}@recorded:2026-01-02T12:00:00Z"),
    )
    .await
    .expect("@recorded day 2");
    assert_eq!(names, vec![json!(["Alice"]), json!(["Bob"])]);

    // Recorded axis after everything: full state.
    let names = query_names_at(
        &fluree,
        db,
        &format!("{ledger_id}@recorded:2026-02-01T00:00:00Z"),
    )
    .await
    .expect("@recorded after all");
    assert_eq!(
        names,
        vec![json!(["Alice"]), json!(["Bob"]), json!(["Carol"])]
    );

    // Recorded axis before anything was recorded: error, even though the
    // EVENT axis has data for 2025 (backdated commits must not leak).
    let err = query_names_at(
        &fluree,
        db,
        &format!("{ledger_id}@recorded:2025-06-01T00:00:00Z"),
    )
    .await
    .expect_err("@recorded before first recording should error");
    assert!(
        err.to_string().contains("no data recorded as of"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn dual_stamp_is_sticky_after_first_event_time() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/event-time-sticky:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // t=1: plain commit — no receivedAt.
    let ledger1 = insert_at(
        &fluree,
        ledger0,
        &person_tx("p1", "Alice"),
        CommitOpts::default(),
    )
    .await
    .expect("t=1 plain");
    assert!(
        received_at_flakes(&ledger1, None).await.is_empty(),
        "plain ledger must not carry receivedAt metadata"
    );

    // t=2: explicit received_at flips the ledger into dual-stamp mode.
    // (Event time defaults to now, which is >= t=1's stamp.)
    let ledger2 = insert_at(
        &fluree,
        ledger1,
        &person_tx("p2", "Bob"),
        CommitOpts::default().with_received_at(Utc::now().to_rfc3339()),
    )
    .await
    .expect("t=2 flip");
    assert_eq!(
        received_at_flakes(&ledger2, Some(2)).await.len(),
        1,
        "flip commit must carry receivedAt"
    );

    // t=3: plain commit opts — sticky mode must auto-stamp receivedAt.
    let ledger3 = insert_at(
        &fluree,
        ledger2,
        &person_tx("p3", "Carol"),
        CommitOpts::default(),
    )
    .await
    .expect("t=3 plain after flip");
    assert_eq!(
        received_at_flakes(&ledger3, Some(3)).await.len(),
        1,
        "post-flip commit must auto-stamp receivedAt (sticky dual-stamp mode)"
    );
}

#[tokio::test]
async fn plain_ledger_recorded_equals_iso() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/event-time-plain:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let ledger1 = insert_at(
        &fluree,
        ledger0,
        &person_tx("p1", "Alice"),
        CommitOpts::default(),
    )
    .await
    .expect("t=1");
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let mid = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let ledger2 = insert_at(
        &fluree,
        ledger1,
        &person_tx("p2", "Bob"),
        CommitOpts::default(),
    )
    .await
    .expect("t=2");

    assert!(
        received_at_flakes(&ledger2, None).await.is_empty(),
        "plain ledger must not carry receivedAt metadata"
    );

    let db = ledger2.as_graph_db_ref(0);
    let via_iso = query_names_at(&fluree, db, &format!("{ledger_id}@iso:{mid}"))
        .await
        .expect("@iso mid");
    let via_recorded = query_names_at(&fluree, db, &format!("{ledger_id}@recorded:{mid}"))
        .await
        .expect("@recorded mid");
    assert_eq!(via_iso, via_recorded, "axes must coincide on plain ledgers");
    assert_eq!(via_iso, vec![json!(["Alice"])]);
}
