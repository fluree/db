//! Numeric counts use live block bounds, with exact novelty reconciliation.
//! Differential checks run with fast paths disabled on the same snapshot.
#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, GraphDb, ReindexOptions};
use fluree_db_core::{Flake, FlakeMeta, FlakeValue};
use fluree_db_novelty::Novelty;
use serde_json::json;
use std::sync::Arc;

const LEDGER: &str = "it/numeric-blocks:main";
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

async fn check(
    fluree: &Fluree,
    view: &GraphDb,
    pred: &str,
    cmp: &str,
    rhs: &str,
    expected: u64,
    block: bool,
) -> Option<Reads> {
    let pattern = format!("?s <{EX}{pred}> ?o");
    let mut reads = None;
    for query in [
        format!("SELECT (COUNT(*) AS ?n) WHERE {{ {pattern} FILTER (?o {cmp} {rhs}) }}"),
        format!("SELECT (COUNT(?s) AS ?n) WHERE {{ {pattern} FILTER (?o {cmp} {rhs}) }}"),
        format!("SELECT (SUM(?o {cmp} {rhs}) AS ?n) WHERE {{ {pattern} }}"),
    ] {
        let (spans, guard) = span_capture::init_test_tracing();
        let fast = fluree
            .query(view, query.as_str())
            .await
            .expect(&query)
            .to_jsonld(&view.snapshot)
            .unwrap();
        let events = spans.find_events("numeric range block count");
        assert_eq!(
            !events.is_empty(),
            block,
            "routing t={}, {query}, {:?}",
            view.t,
            spans.find_events("fast-path outcome")
        );
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
        assert_eq!(fast, generic, "pipeline: {query}");
        assert_eq!(fast, json!([[expected]]), "t={}, {query}", view.t);
    }
    reads
}

async fn staged_view(fluree: &Fluree) -> GraphDb {
    let handle = fluree.ledger_cached(LEDGER).await.unwrap();
    GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state())
}

#[tokio::test]
async fn numeric_counts_use_live_bounds_with_novelty() {
    let _reset = Reset;
    let db = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(db.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let mut ttl = format!("@prefix ex: <{EX}> .\n@prefix xsd: <{XSD}> .\n");
    for i in 0..240 {
        let n = i - 120;
        ttl.push_str(&format!("ex:s{i:03} ex:n {n} .\n"));
        let value = match i % 4 {
            0 => n.to_string(),
            1 => format!("{n}e0"),
            2 => format!("\"{n}.25\"^^xsd:decimal"),
            _ => format!("\"{n}\"^^xsd:short"),
        };
        ttl.push_str(&format!("ex:s{i:03} ex:mixed {value} .\n"));
    }
    ttl.push_str("ex:nan ex:mixed \"NaN\"^^xsd:double .\nex:inf ex:mixed \"INF\"^^xsd:double .\nex:neg-inf ex:mixed \"-INF\"^^xsd:double .\nex:text ex:mixed \"not numeric\" .\n");
    for i in 1..=12 {
        ttl.push_str(&format!("ex:h{i} ex:history {i} .\n"));
    }
    ttl.push_str("ex:lo ex:history -999 .\nex:hi ex:history 999 .\nex:big ex:history 99999999999999999999999999999999999 .\nex:junk ex:history \"zzz\" .\n");
    std::fs::write(src.path().join("numbers.ttl"), ttl).unwrap();
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
    for (cmp, expected) in [(">", 119), (">=", 120), ("<", 120), ("<=", 121)] {
        let r = check(&fluree, &base, "n", cmp, "0", expected, true)
            .await
            .unwrap();
        assert!(r.metadata > 15 && r.scanned <= 2, "{r:?}");
        assert_eq!(r.ops, 0);
    }
    // Preserve the old whole-predicate shortcut: these need no block walk.
    check(&fluree, &base, "n", ">", "-200", 240, false).await;
    check(&fluree, &base, "n", ">", "200", 0, false).await;
    for (cmp, expected) in [(">", 120), (">=", 121), ("<", 121), ("<=", 122)] {
        check(&fluree, &base, "mixed", cmp, "0", expected, true).await;
    }
    // Non-encodable thresholds retain exact generic numeric comparisons.
    check(&fluree, &base, "n", ">", "0.5", 119, false).await;
    check(&fluree, &base, "history", ">", "0", 14, false).await;

    let handle = fluree.ledger_cached(LEDGER).await.unwrap();
    fluree
        .stage(&handle)
        .update(&json!({
            "@context":{"ex":EX},
            "delete":[{"@id":"ex:s130","ex:n":10},{"@id":"ex:s010","ex:n":-110}],
            "insert":[{"@id":"ex:new-hi","ex:n":777},{"@id":"ex:new-lo","ex:n":-777},
                      {"@id":"ex:new-old-value","ex:n":10},{"@id":"ex:new-p","ex:onlyNovel":5}]
        }))
        .execute()
        .await
        .unwrap();
    let novelty = staged_view(&fluree).await;
    let r = check(&fluree, &novelty, "n", ">", "0", 120, true)
        .await
        .unwrap();
    assert!(r.metadata > 15 && r.scanned <= 4, "{r:?}");
    assert_eq!(r.ops, 3);
    check(&fluree, &novelty, "n", "<", "0", 120, true).await;
    check(&fluree, &novelty, "n", ">", "500", 1, true).await;
    check(&fluree, &novelty, "onlyNovel", ">", "0", 1, false).await;
    // The writes touched n, not mixed: it still counts base blocks.
    let r = check(&fluree, &novelty, "mixed", ">", "0", 120, true)
        .await
        .unwrap();
    assert_eq!(r.ops, 0);

    fluree
        .stage(&handle)
        .update(&json!({"@context":{"ex":EX},
            "delete":[{"@id":"ex:new-hi","ex:n":777}],
            "insert":[{"@id":"ex:s130","ex:n":10}]
        }))
        .execute()
        .await
        .unwrap();
    let later = staged_view(&fluree).await;
    check(&fluree, &later, "n", ">", "0", 120, true).await;
    check(&fluree, &later, "n", ">", "500", 0, true).await;
    check(&fluree, &novelty, "n", ">", "500", 1, true).await;

    // Raw overlays can contain operations transaction validation elides.
    // Match by subject, datatype and list place, not just by numeric value.
    let t = base.t + 1;
    let op = |s: &str, n: i64, t: i64, asserted: bool| {
        Flake::new(
            base.snapshot.encode_iri(&format!("{EX}{s}")).unwrap(),
            base.snapshot.encode_iri(&format!("{EX}n")).unwrap(),
            FlakeValue::Long(n),
            base.snapshot.encode_iri(&format!("{XSD}integer")).unwrap(),
            t,
            asserted,
            None,
        )
    };
    let mut overlay = Novelty::new(base.t);
    overlay
        .apply_commit(
            vec![
                op("s130", 10, t, true),
                op("s131", 11, t, false),
                op("s132", 11, t, false),
                op("s132", 10, t, true),
            ],
            t,
            &Default::default(),
        )
        .unwrap();
    overlay
        .apply_commit(
            vec![op("s131", 11, t + 1, true)],
            t + 1,
            &Default::default(),
        )
        .unwrap();
    overlay
        .apply_commit(
            vec![op("s130", 10, t + 2, false), op("s130", 10, t + 2, true)],
            t + 2,
            &Default::default(),
        )
        .unwrap();
    let mut raw = base.clone();
    raw.overlay = Arc::new(overlay);
    for (at, expected) in [(t, 119), (t + 1, 120), (t + 2, 120)] {
        raw.t = at;
        check(&fluree, &raw, "n", ">", "0", expected, true).await;
    }

    let mut dense = Novelty::new(base.t);
    dense
        .apply_commit(
            (120..240)
                .map(|i| op(&format!("s{i:03}"), i - 120, t, false))
                .collect(),
            t,
            &Default::default(),
        )
        .unwrap();
    raw.overlay = Arc::new(dense);
    raw.t = t;
    check(&fluree, &raw, "n", ">", "0", 0, true).await;

    let mut unsupported = Novelty::new(base.t);
    let mut big = op("s130", 0, t, true);
    big.o = FlakeValue::BigInt(Box::new(
        "99999999999999999999999999999999999".parse().unwrap(),
    ));
    unsupported
        .apply_commit(vec![big], t, &Default::default())
        .unwrap();
    raw.overlay = Arc::new(unsupported);
    check(&fluree, &raw, "n", ">", "0", 120, false).await;

    // Deleted extremes/types remain in routing history, but not live bounds.
    fluree
        .stage(&handle)
        .update(&json!({"@context":{"ex":EX},
            "where":[{"@id":"?s","ex:history":"?v"},["filter", "(or (< ?v 0) (> ?v 100))"]],
            "delete":[{"@id":"?s","ex:history":"?v"}]
        }))
        .execute()
        .await
        .unwrap();
    fluree
        .stage(&handle)
        .update(&json!({"@context":{"ex":EX},
            "delete":[{"@id":"ex:junk","ex:history":"zzz"}]
        }))
        .execute()
        .await
        .unwrap();
    let opts = ReindexOptions::default()
        .with_indexer_config(fluree_db_indexer::IndexerConfig::small().with_leaflet_rows(2));
    fluree.reindex(LEDGER, opts).await.unwrap();
    fluree.disconnect_ledger(LEDGER).await;
    let rebuilt = fluree.db(LEDGER).await.unwrap();
    let r = check(&fluree, &rebuilt, "history", ">", "0", 12, true)
        .await
        .unwrap();
    assert!(
        r.metadata > 0 && r.scanned == 0,
        "live edges avoid history's extremes: {r:?}"
    );
    let historical = fluree.db_at_t(LEDGER, base.t).await.unwrap();
    check(&fluree, &historical, "n", ">", "0", 119, false).await;
    check_indexed_lists(&fluree).await;
}

async fn check_indexed_lists(fluree: &Fluree) {
    let src = tempfile::tempdir().unwrap();
    std::fs::write(
        src.path().join("lists.jsonld"),
        serde_json::to_vec(&json!({
            "@context":{"ex":EX},
            "@graph": (0..20).map(|i| json!({
                "@id":format!("ex:list{i:03}"), "ex:listN":{"@list":[7,7]}
            })).collect::<Vec<_>>()
        }))
        .unwrap(),
    )
    .unwrap();
    fluree
        .create("it/listn-lists:main")
        .import(src.path())
        .threads(2)
        .memory_budget_mb(256)
        .leaflet_rows(100)
        .execute()
        .await
        .unwrap();
    let base = fluree.db("it/listn-lists:main").await.unwrap();
    let t = base.t + 1;
    // POST orders list position before subject, including for equal values.
    // The subjects deliberately reverse across the two list positions.
    let mut list_overlay = Novelty::new(base.t);
    let list_op = |s: &str, i| {
        Flake::new(
            base.snapshot.encode_iri(&format!("{EX}{s}")).unwrap(),
            base.snapshot.encode_iri(&format!("{EX}listN")).unwrap(),
            FlakeValue::Long(7),
            base.snapshot.encode_iri(&format!("{XSD}integer")).unwrap(),
            t,
            false,
            Some(FlakeMeta::with_index(i)),
        )
    };
    list_overlay
        .apply_commit(
            vec![list_op("list019", 0), list_op("list000", 1)],
            t,
            &Default::default(),
        )
        .unwrap();
    let mut raw = base.clone();
    raw.t = t;
    raw.overlay = Arc::new(list_overlay);
    check(fluree, &raw, "listN", ">", "0", 38, true).await;
}
