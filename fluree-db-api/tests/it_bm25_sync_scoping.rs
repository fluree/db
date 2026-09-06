//! The guard on the incremental-sync query narrowing.
//!
//! Everything else about that optimisation is invisible from outside. A scoped
//! sync and an unscoped one are *required* to produce identical indexes, so no
//! correctness assertion can tell them apart, and the fuel comparison in
//! `it_graph_source_bm25.rs` measures the query engine rather than this code
//! path — it passes with the whole change reverted. That left the optimisation
//! unguarded: forcing `scoped = None` in `sync_bm25_index` so the full query
//! always ran left every unit and integration test green.
//!
//! So assert on the one thing the sync path does differently: it reports which
//! branch it took. `sync_bm25_index` is driven end to end and the scoped-path
//! event has to appear while the decline event does not. Forcing the fallback,
//! deleting the call site, or making the guard reject a shape it should accept
//! all fail here.
//!
//! Kept as the ONLY test in its own `[[test]]` binary, the same convention as
//! `it_minmax_fast_path_fired` and `it_cyclic_bgp_probe`, and for the same two
//! reasons: tracing's callsite-interest cache is process-global, so a callsite
//! hit while another dispatcher registers can briefly see stale interest and
//! drop the event; and `set_default` capture is thread-local, so the assertion
//! is sound only while the asserted event is emitted on this thread. A
//! standalone binary removes both under bare `cargo test`, which is what a
//! contributor reaches for first. nextest already isolates per process.

mod support;

use fluree_db_api::{Bm25CreateConfig, FlureeBuilder};
use serde_json::json;

/// Message emitted by `sync_bm25_index` when it narrows the indexing query.
const SCOPED: &str = "Scoped indexing query to affected subjects";
/// Message emitted when it declines and falls back to the full scan.
const DECLINED: &str = "Indexing query not narrowed; falling back to the full scan";

#[tokio::test(flavor = "current_thread")]
async fn incremental_sync_actually_scopes_the_indexing_query() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger_id = "bm25/scoping:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let seed = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc1", "@type":"ex:Doc", "ex:title":"alpha beta" },
            { "@id":"ex:doc2", "@type":"ex:Doc", "ex:title":"beta gamma" },
            { "@id":"ex:doc3", "@type":"ex:Doc", "ex:title":"gamma delta" }
        ]
    });
    let ledger1 = fluree.insert(ledger0, &seed).await.unwrap().ledger;

    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });
    let created = fluree
        .create_full_text_index(Bm25CreateConfig::new("scoping", ledger_id, query))
        .await
        .expect("index creation");

    // One document of three changes, so the affected set is a strict subset and
    // the narrowing is load bearing rather than incidentally equal to
    // "everything".
    let delta = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [{ "@id":"ex:doc2", "@type":"ex:Doc", "ex:title":"beta gamma omega" }]
    });
    let _ledger2 = fluree.insert(ledger1, &delta).await.unwrap().ledger;

    // Installed only around the sync, so nothing from setup is captured.
    let (store, guard) = support::span_capture::init_test_tracing();
    let sync = fluree
        .sync_bm25_index(&created.graph_source_id)
        .await
        .expect("incremental sync");
    drop(guard);

    assert!(
        !sync.was_full_resync,
        "this test is meaningless if the sync fell back to a full resync"
    );

    let seen = || {
        store
            .all_events()
            .iter()
            .map(|e| e.message().to_string())
            .collect::<Vec<_>>()
            .join("\n  ")
    };
    assert!(
        store.has_event(SCOPED),
        "the sync did not narrow its indexing query — the optimisation is not \
         wired into `sync_bm25_index`. Events seen:\n  {}",
        seen()
    );
    assert!(
        !store.has_event(DECLINED),
        "the sync declined to narrow a query shape it is supposed to accept. \
         Events seen:\n  {}",
        seen()
    );

    // The affected set is what gets bound, so an off-by-everything (binding the
    // whole corpus) would still emit the event above. Pin the count too.
    let scoped_event = store
        .find_events(SCOPED)
        .into_iter()
        .next()
        .expect("the scoped event was asserted present above");
    assert_eq!(
        scoped_event
            .fields
            .get("affected_count")
            .map(String::as_str),
        Some("1"),
        "one document changed, so exactly one subject should have been bound: {:?}",
        scoped_event.fields
    );
}
