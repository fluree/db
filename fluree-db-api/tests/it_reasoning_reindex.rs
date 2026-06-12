//! Regression guard: blank-node defined-class reasoning survives a full
//! reindex from commit history.
//!
//! History: `benchmark-db/benchmarks/lubm/FINDINGS.md` recorded `Chair`→0
//! after `fluree index` — a fresh import reasoned correctly but the
//! rebuilt index lost defined-class entailments, suspected blank-node
//! round-trip breakage in the rebuild write path (`canonical_split` fix
//! not covering it). Verified non-reproducing on `refactor/resoning-2`
//! (June 2026, LUBM-1 import → reindex → full suite identical); this test
//! pins that property on a minimal Chair-shaped ontology so it can't
//! silently regress again. The A2 reasoned-head design additionally
//! depends on it: reindex drops reasoned heads and re-derives against the
//! rebuilt base, so rebuilt TBox blank nodes must reason identically.
//!
//! The shape matters: the restriction and list nodes are *blank*, and the
//! intersection list is written as explicit `rdf:first`/`rdf:rest`
//! triples (LUBM's N-Triples form) — the Turtle `( ... )` collection
//! syntax has a separate known flattening bug and would not exercise the
//! same structure.

mod support;

use fluree_db_api::{FlureeBuilder, ReindexOptions};
use serde_json::json;

const TTL: &str = r"
    @prefix ex: <http://example.org/> .
    @prefix owl: <http://www.w3.org/2002/07/owl#> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

    ex:Chair owl:equivalentClass _:int .
    _:int owl:intersectionOf _:l1 .
    _:l1 rdf:first ex:Professor .
    _:l1 rdf:rest _:l2 .
    _:l2 rdf:first _:r .
    _:l2 rdf:rest rdf:nil .
    _:r rdf:type owl:Restriction .
    _:r owl:onProperty ex:headOf .
    _:r owl:someValuesFrom ex:Department .

    ex:alice rdf:type ex:Professor .
    ex:alice ex:headOf ex:dept1 .
    ex:dept1 rdf:type ex:Department .
    ex:bob rdf:type ex:Professor .
";

fn chair_query() -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?x"],
        "where": {"@id": "?x", "@type": "ex:Chair"},
        "reasoning": "owl2rl"
    })
}

async fn chairs(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> Vec<serde_json::Value> {
    let view = fluree.db(ledger_id).await.expect("load db");
    let rows = fluree
        .query(&view, &chair_query())
        .await
        .expect("query")
        .to_jsonld(&view.snapshot)
        .expect("format");
    support::normalize_rows(&rows)
}

#[tokio::test]
async fn defined_class_closure_survives_full_reindex() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let path = tmp.path().to_str().unwrap();
    let ledger_id = "it/reasoning-reindex:main";

    // Seed and verify with the first handle (closure computed over novelty).
    {
        let fluree = FlureeBuilder::file(path).build().expect("build");
        let ledger = fluree.create_ledger(ledger_id).await.expect("create");
        let _ = fluree
            .stage_owned(ledger)
            .upsert_turtle(TTL)
            .execute()
            .await
            .expect("stage turtle")
            .ledger;

        let before = chairs(&fluree, ledger_id).await;
        assert_eq!(
            before,
            vec![json!(["ex:alice"])],
            "alice is Chair via Professor ⊓ ∃headOf.Department before reindex"
        );

        // Full rebuild from commit history — the regression's code path.
        let result = fluree
            .reindex(ledger_id, ReindexOptions::default())
            .await
            .expect("reindex");
        assert!(
            result.index_t >= 1,
            "reindex covered the commit: {result:?}"
        );
    }

    // Fresh handle: guaranteed to load the rebuilt index root from the
    // nameservice (no novelty remains — reasoning seeds from the index,
    // which is exactly where Chair→0 regressed).
    let fluree = FlureeBuilder::file(path).build().expect("reopen");
    let after = chairs(&fluree, ledger_id).await;
    assert_eq!(
        after,
        vec![json!(["ex:alice"])],
        "defined-class closure must survive full reindex (FINDINGS.md Chair→0 guard)"
    );
}
