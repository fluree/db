//! Tests for the reverse-POST `ORDER BY DESC(?o) LIMIT k` fast path
//! (`fluree-db-query/src/fast_post_order_limit.rs`).
//!
//! These run against the **binary index** (data is indexed before querying) so
//! the fast path is actually exercised: the base lane (no overlay) for the
//! `with_indexed_view` tests, and the overlay lane (indexed base + uncommitted
//! novelty) for `overlay_*`. Each assertion compares the **ordered** result
//! array (NOT `normalize_rows`, which sorts and would mask ordering bugs).

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, LedgerManagerConfig, QueryInput};
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger_for_fluree, rebuild_and_publish_index};

type MemoryFluree = fluree_db_api::Fluree;

fn seed_json() -> JsonValue {
    json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id": "ex:c1", "@type": "ex:Conversation",
             "ex:dateModified": {"@value": "2024-01-01T00:00:00Z", "@type": "xsd:dateTime"},
             "ex:score": 10, "ex:name": "alpha"},
            {"@id": "ex:c2", "@type": "ex:Conversation",
             "ex:dateModified": {"@value": "2024-02-01T00:00:00Z", "@type": "xsd:dateTime"},
             "ex:score": 20, "ex:name": "bravo"},
            {"@id": "ex:c3", "@type": "ex:Conversation",
             "ex:dateModified": {"@value": "2024-03-01T00:00:00Z", "@type": "xsd:dateTime"},
             "ex:score": 30, "ex:name": "charlie"},
            {"@id": "ex:c4", "@type": "ex:Conversation",
             "ex:dateModified": {"@value": "2024-04-01T00:00:00Z", "@type": "xsd:dateTime"},
             "ex:score": 40, "ex:name": "delta"},
            {"@id": "ex:c5", "@type": "ex:Conversation",
             "ex:dateModified": {"@value": "2024-05-01T00:00:00Z", "@type": "xsd:dateTime"},
             "ex:score": 50, "ex:name": "echo"},
            // A non-Conversation with the LATEST dateModified — must be excluded
            // by the class filter, included when there is no class constraint.
            {"@id": "ex:note1", "@type": "ex:Note",
             "ex:dateModified": {"@value": "2024-12-01T00:00:00Z", "@type": "xsd:dateTime"},
             "ex:score": 99, "ex:name": "zulu"}
        ]
    })
}

/// Extract the first-column value of each (ordered) result row as a string.
fn col0(jsonld: &JsonValue) -> Vec<String> {
    jsonld
        .as_array()
        .expect("result is an array of rows")
        .iter()
        .map(|row| {
            row.as_array().expect("row is an array")[0]
                .as_str()
                .expect("col0 is a string")
                .to_string()
        })
        .collect()
}

/// Seed `seed_json`, build + publish the index **synchronously** (no background
/// indexer, so the indexed view is deterministically ready — important for the
/// emit-span assertion), then run `body` with the indexed view.
async fn with_indexed_view<F, Fut>(ledger_id: &str, body: F)
where
    F: FnOnce(MemoryFluree, fluree_db_api::GraphDb) -> Fut + 'static,
    Fut: std::future::Future<Output = ()>,
{
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
    fluree.insert(ledger, &seed_json()).await.expect("insert");
    rebuild_and_publish_index(&fluree, ledger_id).await;
    let view = fluree.db(ledger_id).await.expect("load indexed view");
    body(fluree, view).await;
}

#[tokio::test]
async fn indexed_post_order_desc_datetime_class_and_offset() {
    with_indexed_view("it/post-order-dt:main", |fluree, view| async move {
        // dateTime is an embedded order-preserving scalar — the exact case that
        // must materialize via `decode_value` (it was the case that silently
        // bailed before the materialization fix). The synchronous index build in
        // `with_indexed_view` makes the fast path deterministically reachable;
        // detection is covered by the `detect_post_order_desc_limit` unit test.
        // (Span-based "did it emit" assertions are unreliable here: tracing's
        // thread-local subscriber loses spans when query work migrates across the
        // shared worker pool under concurrent `#[tokio::test]`s.)

        // JSON-LD: top-3 Conversations by dateModified DESC. note1 (latest, but
        // a Note) must be excluded by the class constraint.
        let q = json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?c", "?d"],
            "where": [{"@id": "?c", "@type": "ex:Conversation", "ex:dateModified": "?d"}],
            "orderBy": "(desc ?d)",
            "limit": 3
        });
        let res = fluree
            .query(&view, QueryInput::JsonLd(&q))
            .await
            .expect("jsonld query");
        let jsonld = res.to_jsonld(&view.snapshot).expect("to_jsonld");
        assert_eq!(
            col0(&jsonld),
            vec!["ex:c5", "ex:c4", "ex:c3"],
            "DESC dateModified + class Conversation, LIMIT 3"
        );

        // SPARQL parity: identical subjects, identical order.
        let sparql = r"
            PREFIX ex: <http://example.org/>
            SELECT ?c ?d
            WHERE { ?c a ex:Conversation ; ex:dateModified ?d . }
            ORDER BY DESC(?d) LIMIT 3
        ";
        let res = fluree
            .query(&view, QueryInput::Sparql(sparql))
            .await
            .expect("sparql query");
        let jsonld = res.to_jsonld(&view.snapshot).expect("to_jsonld");
        assert_eq!(
            col0(&jsonld),
            vec!["ex:c5", "ex:c4", "ex:c3"],
            "SPARQL parity for DESC dateModified + class"
        );

        // OFFSET 1 LIMIT 2 → skip c5, take c4, c3.
        let q = json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?c", "?d"],
            "where": [{"@id": "?c", "@type": "ex:Conversation", "ex:dateModified": "?d"}],
            "orderBy": "(desc ?d)",
            "offset": 1,
            "limit": 2
        });
        let res = fluree
            .query(&view, QueryInput::JsonLd(&q))
            .await
            .expect("jsonld offset query");
        let jsonld = res.to_jsonld(&view.snapshot).expect("to_jsonld");
        assert_eq!(
            col0(&jsonld),
            vec!["ex:c4", "ex:c3"],
            "DESC dateModified + class, OFFSET 1 LIMIT 2"
        );

        // No class constraint: note1 (latest) is now included.
        let q = json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?c", "?d"],
            "where": [{"@id": "?c", "ex:dateModified": "?d"}],
            "orderBy": "(desc ?d)",
            "limit": 3
        });
        let res = fluree
            .query(&view, QueryInput::JsonLd(&q))
            .await
            .expect("jsonld no-class query");
        let jsonld = res.to_jsonld(&view.snapshot).expect("to_jsonld");
        assert_eq!(
            col0(&jsonld),
            vec!["ex:note1", "ex:c5", "ex:c4"],
            "DESC dateModified, no class, LIMIT 3 includes the Note"
        );
    })
    .await;
}

#[tokio::test]
async fn indexed_post_order_desc_integer() {
    with_indexed_view("it/post-order-int:main", |fluree, view| async move {
        // Integer ordering predicate (order-preserving o_type). Result is correct
        // whether served by the fast path or its fallback; detection itself is
        // covered deterministically by the `detect_post_order_desc_limit` unit
        // test in `operator_tree.rs`.
        let sparql = r"
            PREFIX ex: <http://example.org/>
            SELECT ?c ?s
            WHERE { ?c a ex:Conversation ; ex:score ?s . }
            ORDER BY DESC(?s) LIMIT 2
        ";
        let res = fluree
            .query(&view, QueryInput::Sparql(sparql))
            .await
            .expect("sparql query");
        let jsonld = res.to_jsonld(&view.snapshot).expect("to_jsonld");
        assert_eq!(
            col0(&jsonld),
            vec!["ex:c5", "ex:c4"],
            "DESC integer score + class, LIMIT 2"
        );
    })
    .await;
}

#[tokio::test]
async fn indexed_post_order_desc_string_falls_back_correctly() {
    with_indexed_view("it/post-order-str:main", |fluree, view| async move {
        // String ordering predicate: the fast path detects the shape but the
        // operator bails at runtime (LEX_ID o_type is not order-preserving),
        // deferring to the generic top-k. Result must still be correct
        // (lexicographic DESC): echo > delta > charlie.
        let sparql = r"
            PREFIX ex: <http://example.org/>
            SELECT ?c ?n
            WHERE { ?c a ex:Conversation ; ex:name ?n . }
            ORDER BY DESC(?n) LIMIT 3
        ";
        let res = fluree
            .query(&view, QueryInput::Sparql(sparql))
            .await
            .expect("sparql query");
        let jsonld = res.to_jsonld(&view.snapshot).expect("to_jsonld");
        assert_eq!(
            col0(&jsonld),
            vec!["ex:c5", "ex:c4", "ex:c3"],
            "DESC string name falls back to generic top-k, still correct"
        );
    })
    .await;
}

#[tokio::test]
async fn overlay_post_order_desc_reflects_novelty() {
    // Index the baseline (c1..c5 Conversations, dateModified 2024-01..05; note1 a
    // Note at 2024-12), then exercise the OVERLAY lane with uncommitted novelty:
    // an assert (new high value + novelty rdf:type), a retract, and a value
    // supersession (retract old + assert new — the dedup/net-0 case).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/post-order-overlay:main";
    let ctx = json!({"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"});

    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = fluree
        .insert(ledger0, &seed_json())
        .await
        .expect("seed")
        .ledger;
    rebuild_and_publish_index(&fluree, ledger_id).await;

    // Novelty t2: a NEW Conversation c6 at 2024-06-01 (assert, incl. rdf:type).
    let ledger2 = fluree
        .insert(
            ledger1,
            &json!({
                "@context": ctx,
                "@graph": [{"@id": "ex:c6", "@type": "ex:Conversation",
                    "ex:dateModified": {"@value": "2024-06-01T00:00:00Z", "@type": "xsd:dateTime"}}]
            }),
        )
        .await
        .expect("assert c6")
        .ledger;
    let t2 = ledger2.t();

    // Novelty t3: retract c5's dateModified (c5 drops out of the ordering).
    let ledger3 = fluree
        .update(
            ledger2,
            &json!({
                "@context": ctx,
                "delete": [{"@id": "ex:c5",
                    "ex:dateModified": {"@value": "2024-05-01T00:00:00Z", "@type": "xsd:dateTime"}}]
            }),
        )
        .await
        .expect("retract c5 date")
        .ledger;
    let t3 = ledger3.t();

    // Novelty t4: supersede c2's dateModified 2024-02 → 2024-08 (retract+assert).
    let ledger4 = fluree
        .update(
            ledger3,
            &json!({
                "@context": ctx,
                "delete": [{"@id": "ex:c2",
                    "ex:dateModified": {"@value": "2024-02-01T00:00:00Z", "@type": "xsd:dateTime"}}],
                "insert": [{"@id": "ex:c2",
                    "ex:dateModified": {"@value": "2024-08-01T00:00:00Z", "@type": "xsd:dateTime"}}]
            }),
        )
        .await
        .expect("supersede c2 date")
        .ledger;
    let t4 = ledger4.t();

    let sparql = r"
        PREFIX ex: <http://example.org/>
        SELECT ?c ?d
        WHERE { ?c a ex:Conversation ; ex:dateModified ?d . }
        ORDER BY DESC(?d) LIMIT 3
    ";
    let run = |t| {
        let fluree = &fluree;
        async move {
            let view = fluree.db_at_t(ledger_id, t).await.expect("view");
            let res = fluree
                .query(&view, QueryInput::Sparql(sparql))
                .await
                .expect("query");
            let jsonld = res.to_jsonld(&view.snapshot).expect("to_jsonld");
            col0(&jsonld)
        }
    };

    // t2: overlay assert. c6 (06) is a novelty Conversation and must rank top.
    assert_eq!(
        run(t2).await,
        vec!["ex:c6", "ex:c5", "ex:c4"],
        "overlay assert: novelty Conversation c6 ranks first (06 > 05 > 04)"
    );
    // t3: overlay retract. c5 loses its dateModified and drops out.
    assert_eq!(
        run(t3).await,
        vec!["ex:c6", "ex:c4", "ex:c3"],
        "overlay retract: c5's dateModified gone (06, 04, 03)"
    );
    // t4: supersession. c2 jumps to 08 (retract old 02 + assert 08), no dup.
    assert_eq!(
        run(t4).await,
        vec!["ex:c2", "ex:c6", "ex:c4"],
        "overlay supersession: c2 now 08 (08, 06, 04)"
    );
}

/// Regression for the overlay leaf-walk window bound (CRITICAL-1).
///
/// Leaves are NOT predicate-homogeneous — a leaf flushes on row count, not on a
/// `p_id` change — so a single leaf can hold multiple predicates. The overlay
/// lane previously bounded each leaf's assert window with `leaf.first_key.o_key`,
/// the leaf's GLOBAL minimum, which belongs to the lowest-`p_id` predicate in the
/// leaf — not necessarily the queried one. When that foreign predicate's `o_key`
/// is larger than the queried predicate's novelty asserts, those asserts were
/// excluded from the window and emitted last (or dropped past LIMIT) — wrong
/// `ORDER BY DESC(?o) LIMIT k` order and a wrong top-k set.
///
/// Construction makes this deterministic: predicate `p_id`s are assigned in
/// UTF-8 byte-lex order of the IRI (`chunk_dict::sort_and_write_sorted_vocab`),
/// so `ex:aaa` < `ex:score` ⇒ `aaa` gets the lower `p_id`. `ex:aaa` carries a
/// huge integer (`1_000_000`), so the shared leaf's `first_key` is that row and
/// `leaf.first_key.o_key` ≫ every `ex:score` encoding. The novelty assert
/// `ex:score = 35` then falls below the (buggy) window bound. With the fix the
/// window is bounded by THIS predicate's own minimum `o_key` in the leaf, so 35
/// merges into the correct rank.
#[tokio::test]
async fn overlay_post_order_desc_multi_predicate_leaf_window() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/post-order-overlay-shared-leaf:main";
    let ctx = json!({"ex": "http://example.org/"});

    // Base: a foreign predicate `ex:aaa` (lower p_id, huge value) shares one leaf
    // with `ex:score` (10..50). Default leaf size keeps it all in a single leaf.
    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx,
                "@graph": [
                    {"@id": "ex:anchor", "ex:aaa": 1_000_000},
                    {"@id": "ex:s1", "ex:score": 10},
                    {"@id": "ex:s2", "ex:score": 20},
                    {"@id": "ex:s3", "ex:score": 30},
                    {"@id": "ex:s4", "ex:score": 40},
                    {"@id": "ex:s5", "ex:score": 50}
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    rebuild_and_publish_index(&fluree, ledger_id).await;

    // Novelty: ex:s6 score 35 — must rank between 40 and 30.
    let ledger2 = fluree
        .insert(
            ledger1,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:s6", "ex:score": 35}]}),
        )
        .await
        .expect("assert s6")
        .ledger;
    let t2 = ledger2.t();

    let sparql = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s ?v
        WHERE { ?s ex:score ?v . }
        ORDER BY DESC(?v) LIMIT 3
    ";
    let view = fluree.db_at_t(ledger_id, t2).await.expect("view");
    let res = fluree
        .query(&view, QueryInput::Sparql(sparql))
        .await
        .expect("query");
    let jsonld = res.to_jsonld(&view.snapshot).expect("to_jsonld");
    assert_eq!(
        col0(&jsonld),
        vec!["ex:s5", "ex:s4", "ex:s6"],
        "novelty score 35 must rank 3rd (50, 40, 35) despite a foreign \
         lower-p_id predicate with a larger o_key sharing the leaf"
    );
}
