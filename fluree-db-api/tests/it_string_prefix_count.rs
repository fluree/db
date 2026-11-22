//! Prefix counts use directory rows for covered blocks and reconcile novelty
//! by full fact identity. Compare with the general pipeline and assert the
//! blocks actually read, so a fallback cannot hide a broken optimization.
#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, GraphDb, ReindexOptions};
use fluree_db_core::{Flake, FlakeValue};
use fluree_db_novelty::Novelty;
use serde_json::json;
use std::sync::Arc;

const LEDGER: &str = "it/prefix-blocks:main";
const EX: &str = "http://example.org/";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[derive(Debug)]
struct Reads {
    metadata: u64,
    scanned: u64,
    ops: u64,
}

/// All three folded forms must agree with the pipeline and the expected
/// answer; return the block counts from STRSTARTS for access assertions.
async fn check(
    fluree: &Fluree,
    view: &GraphDb,
    predicate: &str,
    prefix: &str,
    expected: u64,
    block_path: bool,
) -> Option<Reads> {
    let pattern = format!("?s <{EX}{predicate}> ?o");
    let mut reads = None;
    for query in [
        format!("SELECT (COUNT(*) AS ?n) WHERE {{ {pattern} FILTER STRSTARTS(?o, \"{prefix}\") }}"),
        format!("SELECT (COUNT(*) AS ?n) WHERE {{ {pattern} FILTER REGEX(?o, \"^{prefix}\") }}"),
        format!(
            "SELECT (SUM(<{XSD}integer>(STRSTARTS(?o, \"{prefix}\"))) AS ?n) WHERE {{ {pattern} }}"
        ),
    ] {
        let (spans, guard) = span_capture::init_test_tracing();
        let fast = fluree
            .query(view, query.as_str())
            .await
            .expect(&query)
            .to_jsonld(&view.snapshot)
            .unwrap();
        assert_eq!(fast, json!([[expected]]), "{query}");
        let events = spans.find_events("string-prefix block count");
        assert_eq!(
            !events.is_empty(),
            block_path,
            "block routing: {query}, t={}, index_t={:?}, overlay_empty={}, events={:?}",
            view.t,
            view.binary_store().map(|s| s.max_t()),
            view.overlay.is_effectively_empty(),
            spans
                .all_events()
                .into_iter()
                .filter(|e| e.message().contains("fast-path") || e.fields.contains_key("error"))
                .collect::<Vec<_>>()
        );
        if predicate == "newName" {
            assert!(
                spans.find_events("fast-path outcome").iter().any(|event| {
                    event.fields.get("outcome").map(String::as_str) == Some("proceed")
                        && event
                            .fields
                            .get("site")
                            .is_some_and(|site| site.starts_with("string-prefix"))
                }),
                "novelty-only predicate must use the fast path: {query}"
            );
        }
        if let Some(event) = events.first() {
            reads = Some(Reads {
                metadata: event.fields["metadata_leaflets"].parse().unwrap(),
                scanned: event.fields["scanned_leaflets"].parse().unwrap(),
                ops: event.fields["novelty_ops"].parse().unwrap(),
            });
        }
        drop(guard);
        set_fast_paths_disabled(true);
        let generic = fluree
            .query(view, query.as_str())
            .await
            .expect(&query)
            .to_jsonld(&view.snapshot)
            .unwrap();
        set_fast_paths_disabled(false);
        assert_eq!(fast, generic, "pipeline comparison: {query}");
    }
    reads
}

async fn staged_view(fluree: &Fluree) -> GraphDb {
    let handle = fluree.ledger_cached(LEDGER).await.unwrap();
    GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state())
}

#[tokio::test]
async fn prefix_blocks_indexed_and_with_novelty() {
    let _reset = Reset;
    let db_dir = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    // Repeated objects span leaflet boundaries. Six earlier values make the
    // narrower prefixes start/end inside 5-row blocks instead of at edges.
    let mut ttl = String::new();
    for i in 0..120 {
        ttl.push_str(&format!("<{EX}s{i:03}> <{EX}name> \"Com{:03}\" .\n", i / 4));
    }
    for i in 0..6 {
        ttl.push_str(&format!("<{EX}a{i}> <{EX}name> \"Alpha{i}\" .\n"));
        ttl.push_str(&format!("<{EX}z{i}> <{EX}name> \"Zulu{i}\" .\n"));
    }
    ttl.push_str(&format!(
        "<{EX}en> <{EX}name> \"Com language\"@en .\n\
         <{EX}fr> <{EX}name> \"Com language\"@fr .\n\
         <{EX}other> <{EX}other> \"Com other predicate\" .\n"
    ));
    std::fs::write(src.path().join("names.ttl"), ttl).unwrap();
    fluree
        .create(LEDGER)
        .import(src.path())
        .threads(2)
        .memory_budget_mb(256)
        .leaflet_rows(5)
        .leaf_rows(15)
        .execute()
        .await
        .unwrap();
    let base = fluree.db(LEDGER).await.unwrap();
    assert!(base.binary_store().unwrap().lex_sorted_string_ids());
    assert!(
        base.binary_store()
            .unwrap()
            .dedicated_branch(
                0,
                fluree_db_binary_index::format::run_record::RunSortOrder::Post
            )
            .unwrap()
            .leaves
            .len()
            > 3,
        "exercise ranges spanning several leaves"
    );

    let reads = check(&fluree, &base, "name", "Com", 122, true)
        .await
        .unwrap();
    assert!(reads.metadata > 20, "{reads:?}");
    assert!(
        reads.scanned <= 4,
        "only string/language boundary blocks: {reads:?}"
    );
    assert_eq!(reads.ops, 0);
    let reads = check(&fluree, &base, "other", "Com", 1, true)
        .await
        .unwrap();
    assert_eq!(reads.metadata, 1);
    assert_eq!(reads.scanned, 0, "fully covered predicate needs no columns");
    let reads = check(&fluree, &base, "name", "Com01", 40, true)
        .await
        .unwrap();
    assert!(reads.metadata > 0 && reads.scanned > 0, "{reads:?}");
    assert!(
        reads.scanned <= 4,
        "only string/language range boundaries: {reads:?}"
    );
    check(&fluree, &base, "name", "Com012", 4, true).await;
    check(&fluree, &base, "name", "Absent", 0, true).await;
    let mut before_index = base.clone();
    before_index.t -= 1;
    check(&fluree, &before_index, "name", "Com", 0, false).await;
    let reads = check(&fluree, &base, "name", "Alpha", 6, true)
        .await
        .unwrap();
    assert!(reads.metadata > 0);

    // Inject already-dictionary-backed ops directly: transaction validation
    // usually removes duplicate assertions and missing retractions, but a
    // query overlay (including reasoning) need not. Neither is a +/-1 delta.
    let mut novelty = Novelty::new(base.t);
    let op = |subject: &str, value: &str, t: i64, assert: bool| {
        Flake::new(
            base.snapshot.encode_iri(&format!("{EX}{subject}")).unwrap(),
            base.snapshot.encode_iri(&format!("{EX}name")).unwrap(),
            FlakeValue::String(value.into()),
            base.snapshot.encode_iri(&format!("{XSD}string")).unwrap(),
            t,
            assert,
            None,
        )
    };
    let t = base.t + 1;
    novelty
        .apply_commit(
            vec![
                op("s004", "Com001", t, true),  // already present
                op("s005", "Com001", t, false), // delete
                op("s001", "Com001", t, false), // absent fact, existing subject/object
                op("s000", "Com001", t, true),  // another subject on the same object
                op("s006", "Com001", t, false),
            ],
            t,
            &Default::default(),
        )
        .unwrap();
    novelty
        .apply_commit(
            vec![op("s006", "Com001", t + 1, true)],
            t + 1,
            &Default::default(),
        )
        .unwrap();
    novelty
        .apply_commit(
            vec![
                op("s006", "Com001", t + 2, false),
                op("s007", "Com001", t + 2, false),
                op("s007", "Com001", t + 2, true), // assertion wins the same-t tie
            ],
            t + 2,
            &Default::default(),
        )
        .unwrap();
    let mut synthetic = base.clone();
    synthetic.overlay = Arc::new(novelty);
    for (as_of, expected) in [(t, 121), (t + 1, 122), (t + 2, 121)] {
        synthetic.t = as_of;
        let reads = check(&fluree, &synthetic, "name", "Com", expected, true)
            .await
            .unwrap();
        assert!(reads.metadata > 20, "{reads:?}");
        assert!(
            reads.scanned <= 6,
            "only boundary/touched blocks: {reads:?}"
        );
        assert!(reads.ops >= 5, "{reads:?}");
    }

    // Real transactions allocate new string IDs out of lexical order and
    // add a language string and a new predicate. The base dictionary has no
    // "Novel" prefix at all; novelty must still contribute it.
    let handle = fluree.ledger_cached(LEDGER).await.unwrap();
    fluree.stage(&handle).update(&json!({
        "@context": {"ex": EX},
        "delete": [
            {"@id": "ex:s008", "ex:name": "Com002"},
            {"@id": "ex:fr", "ex:name": {"@value": "Com language", "@language": "fr"}}
        ],
        "insert": [
            {"@id": "ex:new", "ex:name": "Com new"},
            {"@id": "ex:new-lang", "ex:name": {"@value": "Com novel language", "@language": "en"}},
            {"@id": "ex:new-existing", "ex:name": "Com001"},
            {"@id": "ex:new-low", "ex:name": "A new string"},
            {"@id": "ex:new-high", "ex:name": "Z new string"},
            {"@id": "ex:new-prefix", "ex:name": "Novel only"},
            {"@id": "ex:new-p", "ex:newName": "Com new"}
        ]
    })).execute().await.unwrap();
    let with_novelty = staged_view(&fluree).await;
    assert!(!with_novelty.overlay.is_effectively_empty());
    let reads = check(&fluree, &with_novelty, "name", "Com", 123, true)
        .await
        .unwrap();
    assert!(
        reads.metadata > 20,
        "untouched blocks remain metadata-only: {reads:?}"
    );
    assert!(reads.scanned <= 6, "{reads:?}");
    check(&fluree, &with_novelty, "name", "Novel", 1, true).await;
    check(&fluree, &with_novelty, "newName", "Com", 1, false).await;
    check(&fluree, &with_novelty, "name", "Absent", 0, true).await;

    // A subsequent transaction cancels a novelty assertion and restores a
    // base fact. Older snapshots retain their answers.
    fluree
        .stage(&handle)
        .update(&json!({
            "@context": {"ex": EX},
            "delete": [{"@id": "ex:new", "ex:name": "Com new"}],
            "insert": [{"@id": "ex:s008", "ex:name": "Com002"}]
        }))
        .execute()
        .await
        .unwrap();
    let later = staged_view(&fluree).await;
    check(&fluree, &later, "name", "Com", 123, true).await;
    check(&fluree, &with_novelty, "name", "Com new", 1, true).await;
    check(&fluree, &later, "name", "Com new", 0, true).await;
    check(&fluree, &base, "name", "Com", 122, true).await;

    // Query surfaces read stored list members as values. Repeated members
    // are separate facts by list position, including when both are novel.
    fluree
        .stage(&handle)
        .insert(&json!({
            "@context": {"ex": EX}, "@id": "ex:list",
            "ex:name": {"@list": ["Com list", "Com list"]}
        }))
        .execute()
        .await
        .unwrap();
    let lists = staged_view(&fluree).await;
    check(&fluree, &lists, "name", "Com", 125, true).await;
    // The explicit RDF-list view instead sees the list head on ex:name.
    // It must retain the planner's list-view fallback.
    for disabled in [false, true] {
        set_fast_paths_disabled(disabled);
        let (spans, _guard) = span_capture::init_test_tracing();
        let rows = lists.query(&fluree)
            .sparql(&format!("SELECT (COUNT(*) AS ?n) WHERE {{ ?s <{EX}name> ?o FILTER STRSTARTS(?o, \"Com\") }}"))
            .execution_options(fluree_db_api::QueryExecutionOptions::new().with_rdf_list_view(true))
            .execute().await.unwrap().to_jsonld(&lists.snapshot).unwrap();
        assert_eq!(rows, json!([[123]]));
        assert!(!spans.has_event("string-prefix block count"));
    }
    set_fast_paths_disabled(false);

    // A language tag absent from the base dictionary currently cannot be
    // translated by the shared overlay reader. Preserve its raw-flake
    // fallback, including the new string, rather than counting only base.
    fluree
        .stage(&handle)
        .insert(&json!({
            "@context": {"ex": EX}, "@id": "ex:new-tag",
            "ex:name": {"@value": "Com new tag", "@language": "zh"}
        }))
        .execute()
        .await
        .unwrap();
    let new_tag = staged_view(&fluree).await;
    check(&fluree, &new_tag, "name", "Com", 126, false).await;

    // A rebuild may produce non-lexical IDs. That representation must keep
    // its general scan; the metadata lane must not assume sorted IDs.
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .unwrap();
    fluree.disconnect_ledger(LEDGER).await;
    let rebuilt = fluree.db(LEDGER).await.unwrap();
    assert!(!rebuilt.binary_store().unwrap().lex_sorted_string_ids());
    check(&fluree, &rebuilt, "name", "Com", 126, false).await;

    // No index at all: the same query forms continue to read pure novelty.
    let mem = FlureeBuilder::memory().build_memory();
    let ledger = mem.create_ledger(LEDGER).await.unwrap();
    let ledger = mem
        .insert(
            ledger,
            &json!({
                "@context": {"ex": EX},
                "@graph": [{"@id": "ex:one", "ex:name": "Com one"},
                           {"@id": "ex:two", "ex:name": "Com two"},
                           {"@id": "ex:three", "ex:name": "Other"}]
            }),
        )
        .await
        .unwrap()
        .ledger;
    let unindexed = GraphDb::from_ledger_state(&ledger);
    assert!(unindexed.binary_store().is_none());
    check(&mem, &unindexed, "name", "Com", 2, false).await;
}
