//! Filtered-DELETE list-meta hydration against the `has_list_meta` flag.
//!
//! A `WHERE { ?s p ?o } DELETE { ?s p ?o }` retraction carries no list
//! position, so staging copies `m.i` from the currently-asserted flake
//! before the accumulator dedups. That lookup used to cost a full overlay
//! walk per retracted (subject, predicate) — quadratic in retractions ×
//! novelty (a 40k-retraction delete over a novelty-heavy ledger never
//! finished). Now:
//!
//! 1. ledgers whose index root AND novelty report no `@list` rows skip the
//!    lookup entirely (`IndexRoot.has_list_meta == Some(false)`), and
//! 2. ledgers with lists resolve every group from one overlay walk plus a
//!    base-only seek per group, merged with the same lifecycle rule
//!    `range_with_overlay` applies.
//!
//! These pin the flag's three states through a real index build and the
//! list-retraction semantics on the merged path.
//!
//! The last case sits one layer down, on the read side: when no bound
//! component of the pattern resolves to a persisted id, the range provider
//! serves the pattern from the overlay alone, and that overlay is a log —
//! it must be lifecycle-resolved, not filtered to asserts.

#![cfg(feature = "native")]

mod support;
use crate::support::{
    query_jsonld_formatted, start_background_indexer_local, trigger_index_and_wait_outcome,
};
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/ns/"})
}

/// Novelty never indexed: `reindex_min_bytes` is unreachable so the
/// background worker only runs on explicit triggers.
fn index_cfg() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1 << 40,
        reindex_max_bytes: 1 << 41,
    }
}

async fn count(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    pred: &str,
) -> usize {
    let q = json!({
        "@context": ctx(),
        "select": ["?s", "?o"],
        "where": {"@id": "?s", pred: "?o"}
    });
    query_jsonld_formatted(fluree, ledger, &q)
        .await
        .expect("count query")
        .as_array()
        .map(Vec::len)
        .unwrap_or(0)
}

async fn list_values(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    subject: &str,
) -> Vec<String> {
    list_items(fluree, ledger, subject, "ex:items").await
}

/// Items of `subject`'s `pred` list; language-tagged entries render as
/// `value@lang` so two tags on the same lexical form stay distinguishable.
async fn list_items(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    subject: &str,
    pred: &str,
) -> Vec<String> {
    let q = json!({
        "@context": ctx(),
        "select": {"?s": [pred]},
        "where": {"@id": "?s"},
        "values": ["?s", [{"@id": subject}]]
    });
    let rows = query_jsonld_formatted(fluree, ledger, &q)
        .await
        .expect("list query");
    let render = |v: &serde_json::Value| -> Option<String> {
        match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Object(o) => {
                let val = o.get("@value")?.as_str()?;
                Some(match o.get("@language").and_then(|l| l.as_str()) {
                    Some(lang) => format!("{val}@{lang}"),
                    None => val.to_string(),
                })
            }
            _ => None,
        }
    };
    rows.as_array()
        .and_then(|a| a.first())
        .and_then(|node| node.get(pred))
        .map(|v| match v {
            serde_json::Value::Array(items) => items.iter().filter_map(render).collect(),
            other => render(other).into_iter().collect(),
        })
        .unwrap_or_default()
}

/// No `@list` anywhere: the indexed root records `Some(false)`, novelty
/// stays `false`, and a predicate-wide filtered delete spanning base rows
/// and novelty-only rows still retracts everything (the skip path must not
/// drop retractions).
#[tokio::test]
async fn no_list_ledger_records_flag_and_deletes_cleanly() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/list-meta-none:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();
            assert_eq!(ledger.snapshot.has_list_meta, Some(false), "empty snapshot is exact");

            let batch = |from: usize, to: usize| {
                let graph: Vec<_> = (from..to)
                    .map(|n| json!({"@id": format!("ex:r{n}"), "ex:created": format!("2024-01-{:02}", n % 28 + 1), "ex:name": format!("r{n}")}))
                    .collect();
                json!({"@context": ctx(), "@graph": graph})
            };
            let r = fluree
                .upsert_with_opts(ledger, &batch(0, 300), TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());
            assert_eq!(ledger.snapshot.has_list_meta, Some(false), "full build observed no list rows");
            assert!(!ledger.novelty.has_list_meta);

            // Novelty-only subjects on top of the indexed base.
            let _ = fluree
                .upsert_with_opts(ledger, &batch(300, 600), TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(!ledger.novelty.has_list_meta);
            assert_eq!(count(&fluree, &ledger, "ex:created").await, 600);

            let del = json!({
                "@context": ctx(),
                "where": [{"@id": "?s", "ex:created": "?d"}],
                "delete": [{"@id": "?s", "ex:created": "?d"}]
            });
            let r = fluree
                .update_with_opts(ledger, &del, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 600);
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(count(&fluree, &ledger, "ex:created").await, 0);
            assert_eq!(count(&fluree, &ledger, "ex:name").await, 600, "unrelated predicate untouched");
        })
        .await;
}

/// `@list` rows in the base AND in novelty: the root records `Some(true)`
/// after indexing, novelty flips its bit on the first list commit, and a
/// filtered delete of one value removes exactly one positional entry per
/// WHERE binding — for a base-indexed list, a novelty-only list, and a
/// base list with an entry retracted and re-asserted in novelty.
#[tokio::test]
async fn list_ledger_hydrates_positions_across_base_and_novelty() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/list-meta-some:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Base: two lists, plus filler subjects so the delete's WHERE
            // spans many (s,p) groups.
            let mut graph = vec![
                json!({"@id": "ex:base1", "ex:items": {"@list": ["a", "b", "c"]}}),
                json!({"@id": "ex:base2", "ex:items": {"@list": ["b", "b", "d"]}}),
            ];
            graph.extend((0..200).map(|n| json!({"@id": format!("ex:f{n}"), "ex:items": {"@list": ["b"]}})));
            let r = fluree
                .insert_with_opts(ledger, &json!({"@context": ctx(), "@graph": graph}), TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.novelty.has_list_meta, "novelty saw a list position");
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(ledger.snapshot.has_list_meta, Some(true), "full build observed list rows");

            // Novelty: a new list subject, and a second commit that retracts
            // `ex:base2`'s middle entry so the merged view must drop it.
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:nov1", "ex:items": {"@list": ["b", "e"]}}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();

            // Lifecycle over the base: retract `ex:base2`'s trailing "d"
            // (a novelty retract of a base-indexed list row), then re-assert
            // the whole list so "d" comes back at position 2 as a novelty
            // assert. The merged base+overlay view must resolve that fact
            // key to its newest op (live) when hydrating below.
            let retract_d = json!({
                "@context": ctx(),
                "where": [{"@id": "ex:base2", "ex:items": "d"}],
                "delete": [{"@id": "ex:base2", "ex:items": "d"}]
            });
            let r = fluree
                .update_with_opts(ledger, &retract_d, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 1);
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(list_values(&fluree, &ledger, "ex:base2").await, vec!["b", "b"]);
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:base2", "ex:items": {"@list": ["b", "b", "d"]}}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(list_values(&fluree, &ledger, "ex:base2").await, vec!["b", "b", "d"]);
            assert_eq!(list_values(&fluree, &ledger, "ex:base1").await, vec!["a", "b", "c"]);
            assert_eq!(list_values(&fluree, &ledger, "ex:nov1").await, vec!["b", "e"]);

            // Delete every "b" everywhere. Each (subject, "b") binding must
            // remove exactly one positional entry: base1 loses its b, base2
            // keeps one of its two b's (one binding → one entry), nov1 loses
            // its b, every filler empties.
            let del = json!({
                "@context": ctx(),
                "where": [{"@id": "?s", "ex:items": "b"}],
                "delete": [{"@id": "?s", "ex:items": "b"}]
            });
            let r = fluree
                .update_with_opts(ledger, &del, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 203, "base1 + base2 + nov1 + 200 fillers");
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(list_values(&fluree, &ledger, "ex:base1").await, vec!["a", "c"]);
            assert_eq!(list_values(&fluree, &ledger, "ex:base2").await, vec!["b", "d"]);
            assert_eq!(list_values(&fluree, &ledger, "ex:nov1").await, vec!["e"]);
            assert_eq!(list_values(&fluree, &ledger, "ex:f7").await, Vec::<String>::new());
        })
        .await;
}

/// Language-tagged list entries carry `m = { lang, i }`. A retraction bound
/// from a tagged value arrives with `{ lang, i: None }` and must still get
/// its position hydrated — and from the candidate with the SAME tag, so
/// identical lexical forms under different languages are not confused.
#[tokio::test]
async fn language_tagged_list_entries_hydrate_by_tag() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/list-meta-lang:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            let labels = |id: &str| {
                json!({"@id": id, "ex:labels": {"@list": [
                    {"@value": "b", "@language": "en"},
                    {"@value": "b", "@language": "fr"},
                    {"@value": "c", "@language": "en"}
                ]}})
            };
            // `ex:base` is indexed; `ex:nov` lives only in novelty.
            let r = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@graph": [labels("ex:base")]}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(ledger.snapshot.has_list_meta, Some(true));
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@graph": [labels("ex:nov")]}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            for id in ["ex:base", "ex:nov"] {
                assert_eq!(
                    list_items(&fluree, &ledger, id, "ex:labels").await,
                    vec!["b@en", "b@fr", "c@en"]
                );
            }

            // Delete only the French "b". The English "b" shares its lexical
            // form and datatype; matching on language must pick position 1,
            // not position 0.
            let del = json!({
                "@context": ctx(),
                "where": [{"@id": "?s", "ex:labels": {"@value": "b", "@language": "fr"}}],
                "delete": [{"@id": "?s", "ex:labels": {"@value": "b", "@language": "fr"}}]
            });
            let r = fluree
                .update_with_opts(
                    ledger,
                    &del,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 2, "one entry per subject");
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            for id in ["ex:base", "ex:nov"] {
                assert_eq!(
                    list_items(&fluree, &ledger, id, "ex:labels").await,
                    vec!["b@en", "c@en"],
                    "{id}"
                );
            }
        })
        .await;
}

/// The `!ledger.novelty.has_list_meta` half of the skip guard.
///
/// An indexed root that observed no list rows (`Some(false)`) can still have
/// a `@list` arrive in novelty afterwards. Only the novelty bit distinguishes
/// that from a genuinely list-free ledger, so dropping it from the guard
/// skips hydration and the positional entry silently survives its retraction.
/// The reload leg pins the other half: novelty replayed from the commit chain
/// must carry the bit forward, or the guard is wrong again after a restart.
#[tokio::test]
async fn novelty_list_over_indexed_no_list_root_still_hydrates() {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let mut fluree = FlureeBuilder::file(dir.clone()).build().unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/list-meta-novelty-only:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Base carries `ex:items` as PLAIN values only — no `@list`
            // anywhere, so the full build records `Some(false)`.
            let graph: Vec<_> = (0..50)
                .map(|n| json!({"@id": format!("ex:b{n}"), "ex:items": "b", "ex:name": format!("b{n}")}))
                .collect();
            let r = fluree
                .insert_with_opts(ledger, &json!({"@context": ctx(), "@graph": graph}), TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert!(!fluree.ledger(ledger_id).await.unwrap().novelty.has_list_meta);
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());
            assert_eq!(ledger.snapshot.has_list_meta, Some(false), "full build observed no list rows");

            // Now a `@list` lands in novelty. Root still says `Some(false)`:
            // the novelty bit is the only thing keeping hydration alive.
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:nov1", "ex:items": {"@list": ["a", "b", "c"]}}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(ledger.snapshot.has_list_meta, Some(false), "root unchanged");
            assert!(ledger.novelty.has_list_meta, "novelty saw the list position");
            assert_eq!(list_values(&fluree, &ledger, "ex:nov1").await, vec!["a", "b", "c"]);

            let del = json!({
                "@context": ctx(),
                "where": [{"@id": "?s", "ex:items": "b"}],
                "delete": [{"@id": "?s", "ex:items": "b"}]
            });
            let r = fluree
                .update_with_opts(ledger, &del, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 51, "50 plain base rows + the list entry");
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(
                list_values(&fluree, &ledger, "ex:nov1").await,
                vec!["a", "c"],
                "novelty list entry retracted at its hydrated position"
            );
            assert_eq!(count(&fluree, &ledger, "ex:items").await, 2, "only ex:nov1's a and c remain");
            assert_eq!(count(&fluree, &ledger, "ex:name").await, 50, "base subjects survive");

            // Reload: novelty is replayed from the commit chain on top of the
            // same `Some(false)` root, and must carry the bit — otherwise the
            // guard skips hydration for every write after a restart.
            drop(fluree);
            let reloaded = FlureeBuilder::file(dir).build().unwrap();
            let ledger = reloaded.ledger(ledger_id).await.unwrap();
            assert_eq!(ledger.snapshot.has_list_meta, Some(false));
            assert!(ledger.novelty.has_list_meta, "replayed novelty carries the bit");
            assert_eq!(list_values(&reloaded, &ledger, "ex:nov1").await, vec!["a", "c"]);

            // And it is load-bearing on the reloaded handle, not just present.
            let del_c = json!({
                "@context": ctx(),
                "where": [{"@id": "?s", "ex:items": "c"}],
                "delete": [{"@id": "?s", "ex:items": "c"}]
            });
            let r = reloaded
                .update_with_opts(ledger, &del_c, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 1);
            let ledger = reloaded.ledger(ledger_id).await.unwrap();
            assert_eq!(list_values(&reloaded, &ledger, "ex:nov1").await, vec!["a"]);
        })
        .await;
}

/// A retracted base list row must not out-vote the live novelty one.
///
/// Hydration copies the FIRST list-carrying meta indexed per object value,
/// and the base seek's rows land before the overlay's. Delete `"b"` from an
/// indexed `["b"]`, re-assert as `["x", "b"]`, then delete `"b"` again: the
/// stale base candidate is `b@0` and the live one is `b@1`, so without
/// `resolve_current_flakes` dropping the retracted key the retraction is
/// hydrated to position 0 and the real entry survives.
#[tokio::test]
async fn stale_base_list_candidate_resolves_before_hydration() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/list-meta-stale:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Base: "b" at position 0, indexed.
            let r = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:stale", "ex:items": {"@list": ["b"]}}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(ledger.snapshot.has_list_meta, Some(true));
            assert_eq!(list_values(&fluree, &ledger, "ex:stale").await, vec!["b"]);

            let del_b = json!({
                "@context": ctx(),
                "where": [{"@id": "ex:stale", "ex:items": "b"}],
                "delete": [{"@id": "ex:stale", "ex:items": "b"}]
            });
            let _ = fluree
                .update_with_opts(ledger, &del_b, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(list_values(&fluree, &ledger, "ex:stale").await, Vec::<String>::new());

            // Re-assert with "b" moved to position 1. The base's `b@0` is now
            // a retracted key that still sorts ahead of the live `b@1`.
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:stale", "ex:items": {"@list": ["x", "b"]}}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(list_values(&fluree, &ledger, "ex:stale").await, vec!["x", "b"]);

            let r = fluree
                .update_with_opts(ledger, &del_b, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 1);
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(
                list_values(&fluree, &ledger, "ex:stale").await,
                vec!["x"],
                "hydrated from the live b@1, not the retracted base b@0"
            );
        })
        .await;
}

/// The predicate itself exists ONLY in novelty: the indexed base has never
/// seen `ex:items` in any form (the seed rows carry `ex:name` only), so no
/// bound component of the crawl's `(subject, predicate)` seek resolves to a
/// persisted id and the range provider serves the pattern straight from the
/// overlay. That overlay is a LOG — the assert at `t=2` and its retraction
/// at `t=3` both sit in novelty — so serving it assert-only resurrected the
/// deleted entry: the receipt said one flake retracted and the value stayed
/// readable.
///
/// Nothing about this is list-specific, so the plain-valued leg rides along
/// on a second novelty-only predicate. The reload leg pins that the durable
/// commit chain agrees with the live view (the bug was symmetric: a reload
/// replays the same novelty and read it the same wrong way).
#[tokio::test]
async fn novelty_only_predicate_retraction_takes_effect() {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let mut fluree = FlureeBuilder::file(dir.clone()).build().unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/list-meta-novpred:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Seed rows WITHOUT the predicate, so `ex:items` never reaches
            // the persisted predicate dictionary.
            let graph: Vec<_> = (0..50)
                .map(|n| json!({"@id": format!("ex:r{n}"), "ex:name": format!("r{n}")}))
                .collect();
            let r = fluree
                .upsert_with_opts(ledger, &json!({"@context": ctx(), "@graph": graph}), TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());

            // The list AND its predicate live only in novelty.
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:nov1", "ex:items": {"@list": ["a", "b", "c"]}}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(list_values(&fluree, &ledger, "ex:nov1").await, vec!["a", "b", "c"]);

            let del = json!({
                "@context": ctx(),
                "where": [{"@id": "?s", "ex:items": "b"}],
                "delete": [{"@id": "?s", "ex:items": "b"}]
            });
            let r = fluree
                .update_with_opts(ledger, &del, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 1);
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(
                list_values(&fluree, &ledger, "ex:nov1").await,
                vec!["a", "c"],
                "live handle must observe the retraction"
            );
            // The BGP path resolved the overlay correctly all along; pin it so
            // the two read paths stay in agreement.
            let q_sel = json!({
                "@context": ctx(),
                "select": ["?s", "?o"],
                "where": {"@id": "?s", "ex:items": "?o"}
            });
            assert_eq!(
                query_jsonld_formatted(&fluree, &ledger, &q_sel)
                    .await
                    .unwrap()
                    .as_array()
                    .map(Vec::len),
                Some(2),
                "BGP path agrees with the crawl"
            );

            // Same mechanism, plain values: a second novelty-only predicate
            // with no `@list` anywhere.
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:nov2", "ex:tags": ["p", "q"]}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            let del_p = json!({
                "@context": ctx(),
                "where": [{"@id": "ex:nov2", "ex:tags": "p"}],
                "delete": [{"@id": "ex:nov2", "ex:tags": "p"}]
            });
            let r = fluree
                .update_with_opts(ledger, &del_p, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 1);
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(
                list_items(&fluree, &ledger, "ex:nov2", "ex:tags").await,
                vec!["q"],
                "plain-valued novelty-only predicate retracts too"
            );

            // Reload: a fresh process replays the commit chain and must agree.
            drop(fluree);
            let reloaded = FlureeBuilder::file(dir).build().unwrap();
            let ledger = reloaded.ledger(ledger_id).await.unwrap();
            assert_eq!(
                list_values(&reloaded, &ledger, "ex:nov1").await,
                vec!["a", "c"],
                "replayed commit chain must observe the retraction"
            );
            assert_eq!(
                list_items(&reloaded, &ledger, "ex:nov2", "ex:tags").await,
                vec!["q"]
            );
        })
        .await;
}

/// Two facts that share `(s, p, o, dt)` and differ only in `m` — a `@list`
/// repeating a value, and one lexical form under two language tags.
///
/// These are distinct facts, but every comparator orders `t` before `m`, so
/// the sibling's assert lands between a fact's own assert and its retraction.
/// Resolution that cuts groups on run adjacency over full identity therefore
/// closes early and hands back the retracted entry as live — the same
/// resurrection this file exists to pin, one layer down in the resolver.
///
/// Both legs delete the entry whose metadata sorts FIRST: that is the failing
/// direction. Deleting the last position passes even with the grouping bug,
/// so a pin that only covered it would be vacuous.
#[tokio::test]
async fn novelty_only_predicate_resolves_metadata_siblings() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/list-meta-novpred-siblings:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Indexed base that has never seen either predicate, so both
            // patterns below are served straight from the overlay.
            let graph: Vec<_> = (0..50)
                .map(|n| json!({"@id": format!("ex:r{n}"), "ex:name": format!("r{n}")}))
                .collect();
            let r = fluree
                .upsert_with_opts(ledger, &json!({"@context": ctx(), "@graph": graph}), TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());

            // `["a", "b", "a"]` — "a" at positions 0 and 2.
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:dup", "ex:items": {"@list": ["a", "b", "a"]}}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(
                list_values(&fluree, &ledger, "ex:dup").await,
                vec!["a", "b", "a"]
            );

            // One WHERE binding retracts exactly one positional entry, and
            // hydration copies the FIRST candidate meta — position 0.
            let del_a = json!({
                "@context": ctx(),
                "where": [{"@id": "ex:dup", "ex:items": "a"}],
                "delete": [{"@id": "ex:dup", "ex:items": "a"}]
            });
            let r = fluree
                .update_with_opts(ledger, &del_a, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 1);
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(
                list_values(&fluree, &ledger, "ex:dup").await,
                vec!["b", "a"],
                "the position-0 entry goes; its twin at position 2 stays"
            );

            // Same lexical form under two language tags on a second
            // novelty-only predicate.
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:tagged", "ex:labels": [
                        {"@value": "hello", "@language": "en"},
                        {"@value": "hello", "@language": "fr"}
                    ]}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(
                list_items(&fluree, &ledger, "ex:tagged", "ex:labels").await,
                vec!["hello@en", "hello@fr"]
            );

            let del_en = json!({
                "@context": ctx(),
                "where": [{"@id": "ex:tagged", "ex:labels": {"@value": "hello", "@language": "en"}}],
                "delete": [{"@id": "ex:tagged", "ex:labels": {"@value": "hello", "@language": "en"}}]
            });
            let r = fluree
                .update_with_opts(ledger, &del_en, TxnOpts::default(), CommitOpts::default(), &index_cfg())
                .await
                .unwrap();
            assert_eq!(r.receipt.flake_count, 1);
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(
                list_items(&fluree, &ledger, "ex:tagged", "ex:labels").await,
                vec!["hello@fr"],
                "the @en fact goes; the @fr fact sharing its lexical form stays"
            );
        })
        .await;
}

/// `offset` / `limit` on the overlay-only path apply to the RESOLVED current
/// state, not to the log.
///
/// The fix moved when they apply relative to resolution, and nothing else
/// pins the order — a later refactor could reorder it silently and only fail
/// on a ledger where a retraction happens to sit inside the first page.
/// Here `ex:page` carries ten novelty-only values with the first two
/// retracted: paging that ran before resolution would spend its window on
/// retracted rows and hand back a short or wrong page.
#[tokio::test]
async fn novelty_only_predicate_paging_applies_after_resolution() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/list-meta-novpred-paging:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            let graph: Vec<_> = (0..50)
                .map(|n| json!({"@id": format!("ex:r{n}"), "ex:name": format!("r{n}")}))
                .collect();
            let r = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@graph": graph}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());

            // v0..v9 under a novelty-only predicate.
            let values: Vec<String> = (0..10).map(|n| format!("v{n}")).collect();
            let _ = fluree
                .insert_with_opts(
                    ledger,
                    &json!({"@context": ctx(), "@id": "ex:page", "ex:vals": values}),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg(),
                )
                .await
                .unwrap();

            // Retract the two that sort first.
            for v in ["v0", "v1"] {
                let ledger = fluree.ledger(ledger_id).await.unwrap();
                let del = json!({
                    "@context": ctx(),
                    "where": [{"@id": "ex:page", "ex:vals": v}],
                    "delete": [{"@id": "ex:page", "ex:vals": v}]
                });
                let r = fluree
                    .update_with_opts(
                        ledger,
                        &del,
                        TxnOpts::default(),
                        CommitOpts::default(),
                        &index_cfg(),
                    )
                    .await
                    .unwrap();
                assert_eq!(r.receipt.flake_count, 1, "retracting {v}");
            }

            let ledger = fluree.ledger(ledger_id).await.unwrap();
            let mut live = list_items(&fluree, &ledger, "ex:page", "ex:vals").await;
            live.sort();
            assert_eq!(
                live,
                vec!["v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9"],
                "the two retracted values are gone from the unpaged read"
            );

            // A LIMIT smaller than the retracted prefix: every returned row
            // must be a live value. Resolution-before-paging is what makes
            // that true — paging the log first would surface `v0`/`v1`.
            let paged = json!({
                "@context": ctx(),
                "select": ["?o"],
                "where": {"@id": "ex:page", "ex:vals": "?o"},
                "limit": 3
            });
            let rows = query_jsonld_formatted(&fluree, &ledger, &paged)
                .await
                .unwrap();
            let got: Vec<String> = rows
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|r| {
                    r.as_array()
                        .and_then(|c| c.first())
                        .and_then(|v| v.as_str())
                })
                .map(String::from)
                .collect();
            assert_eq!(got.len(), 3, "a full page of live rows, got {got:?}");
            assert!(
                got.iter().all(|v| v != "v0" && v != "v1"),
                "retracted values must never enter a page, got {got:?}"
            );
        })
        .await;
}
