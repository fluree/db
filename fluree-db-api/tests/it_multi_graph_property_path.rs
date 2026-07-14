//! Multi-graph property-path traversal over a cross-ledger dataset (INDEXED).
//!
//! Mirrors the server's real conditions: data is pushed into the **binary
//! index** (not just novelty), then a cross-ledger dataset query is run through
//! the connection path. This is what triggers the multi-graph GRAPH + property
//! path failures that the novelty-only path does not.
//!
//! Domain: a **library** ledger (`lib:` books) references a **subject taxonomy**
//! ledger (`subj:` topics linked by `subj:broader`, e.g. jazz ⊂ music ⊂ arts).
//! Most cases use *divergent* namespace codes — a ledger registers its own
//! prefix first and the shared `subj:` prefix only via a ref, so `subj:` gets a
//! different code per ledger (the real cross-ledger condition).
//!
//! See GitHub issue #1405 (property paths + multi-ledger datasets).

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerManagerConfig};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::{json, Value as JsonValue};
use support::{
    genesis_ledger_for_fluree, start_background_indexer_local, trigger_index_and_wait_outcome,
};

type MemoryFluree = fluree_db_api::Fluree;

async fn insert_indexed(
    fluree: &MemoryFluree,
    handle: &fluree_db_indexer::IndexerHandle,
    ledger_id: &str,
    doc: &JsonValue,
) {
    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };
    let ledger = genesis_ledger_for_fluree(fluree, ledger_id);
    let result = fluree
        .insert_with_opts(
            ledger,
            doc,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_cfg,
        )
        .await
        .expect("insert");
    let _ = trigger_index_and_wait_outcome(handle, ledger_id, result.ledger.t()).await;
}

/// Seed a `subject.example` taxonomy (jazz ⊂ music ⊂ arts) and a
/// `library.example` book that references the deepest subject. `subj:` is
/// registered first in the taxonomy but only via a ref in the catalog (whose
/// own `lib:` prefix registers first), so the two ledgers assign `subj:`
/// different namespace codes.
async fn seed(fluree: &MemoryFluree, handle: &fluree_db_indexer::IndexerHandle) {
    insert_indexed(
        fluree,
        handle,
        "taxonomy:main",
        &json!({
            "@context": {"subj": "http://subject.example/",
                         "rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
            "@graph": [
                {"@id": "subj:arts",  "rdfs:label": "Arts"},
                {"@id": "subj:music", "subj:broader": {"@id": "subj:arts"},  "rdfs:label": "Music"},
                {"@id": "subj:jazz",  "subj:broader": {"@id": "subj:music"}, "rdfs:label": "Jazz"}
            ]
        }),
    )
    .await;
    insert_indexed(
        fluree,
        handle,
        "catalog:main",
        &json!({
            "@context": {"lib": "http://library.example/", "subj": "http://subject.example/"},
            "@graph": [ {"@id": "lib:book1", "lib:subject": {"@id": "subj:jazz"}} ]
        }),
    )
    .await;
}

fn fluree_with_indexer() -> (
    MemoryFluree,
    tokio::task::LocalSet,
    fluree_db_indexer::IndexerHandle,
) {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    (fluree, local, handle)
}

/// Q1 (control) — property path over a SINGLE-graph `FROM`, indexed.
#[tokio::test]
async fn q1_single_graph_property_path_works() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            seed(&fluree, &handle).await;
            let sparql = r"
PREFIX subj: <http://subject.example/>
SELECT ?anc FROM <taxonomy:main>
WHERE { subj:jazz subj:broader* ?anc }";
            let result = fluree
                .query_connection_sparql(sparql)
                .await
                .expect("single-graph property path should execute");
            let tax = fluree.ledger("taxonomy:main").await.expect("load");
            let s = result
                .to_jsonld(&tax.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(
                s.contains("subj:jazz") && s.contains("subj:music") && s.contains("subj:arts"),
                "{s}"
            );
        })
        .await;
}

/// Q2 (characterization) — property path over MULTI-graph `FROM` is guarded.
/// This is failure 1 in #1405, intentionally left in place (cross-snapshot BFS
/// is a follow-up); the test asserts the guard still fires.
#[tokio::test]
async fn q2_multi_graph_property_path_is_guarded() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            seed(&fluree, &handle).await;
            let sparql = r"
PREFIX subj: <http://subject.example/>
PREFIX lib: <http://library.example/>
SELECT DISTINCT ?book FROM <catalog:main> FROM <taxonomy:main>
WHERE { ?book lib:subject ?c . ?c subj:broader* subj:arts }";
            let err = fluree
                .query_connection_sparql(sparql)
                .await
                .expect_err("multi-graph property path should be rejected");
            assert!(
                err.to_string()
                    .contains("Property paths over multi-graph datasets are not supported"),
                "unexpected error: {err}"
            );
        })
        .await;
}

/// Q3a — a GRAPH-scoped property path over an INDEXED multi-ledger dataset,
/// joined to a default-graph instance. Pre-fix this hit an internal invariant
/// (`EncodedSid reached stamp_provenance`). Expected: `lib:book1`.
#[tokio::test]
async fn q3a_graph_scoped_path_over_multiledger_should_join() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            seed(&fluree, &handle).await;
            let sparql = r"
PREFIX subj: <http://subject.example/>
PREFIX lib: <http://library.example/>
SELECT DISTINCT ?book FROM <catalog:main> FROM NAMED <taxonomy:main>
WHERE { ?book lib:subject ?c . GRAPH <taxonomy:main> { ?c subj:broader* subj:arts } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(
                result.is_ok(),
                "GRAPH-scoped property path over a multi-ledger dataset should execute, got: {:?}",
                result.err()
            );
            let cat = fluree.ledger("catalog:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&cat.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(s.contains("lib:book1"), "expected lib:book1: {s}");
        })
        .await;
}

/// Q3b — a plain cross-`GRAPH` variable join over an indexed multi-ledger
/// dataset. Expected: `lib:book1`.
#[tokio::test]
async fn q3b_cross_graph_join_should_return_rows() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            seed(&fluree, &handle).await;
            let sparql = r"
PREFIX subj: <http://subject.example/>
PREFIX lib: <http://library.example/>
SELECT DISTINCT ?book FROM NAMED <catalog:main> FROM NAMED <taxonomy:main>
WHERE { GRAPH <catalog:main> { ?book lib:subject ?c }
        GRAPH <taxonomy:main> { ?c subj:broader subj:music } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(
                result.is_ok(),
                "cross-graph join should execute, got: {:?}",
                result.err()
            );
            let cat = fluree.ledger("catalog:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&cat.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(
                s.contains("lib:book1"),
                "cross-graph join returned no rows: {s}"
            );
        })
        .await;
}

/// Q3c — the cross-graph join under explicit **namespace-code divergence**:
/// `subj:` is registered first in `taxonomy` but only via a ref in `catalog2`
/// (whose own `lib:` prefix registers first), so `?c` has a different code in
/// each ledger. Pre-fix this returned []. Expected: `lib:book1`.
#[tokio::test]
async fn q3c_cross_graph_join_divergent_ns_should_return_rows() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            // taxonomy: subject.example registered FIRST (low code).
            insert_indexed(
                &fluree,
                &handle,
                "taxonomy:main",
                &json!({"@context": {"subj": "http://subject.example/"},
                        "@graph": [
                            {"@id": "subj:jazz",  "subj:broader": {"@id": "subj:music"}},
                            {"@id": "subj:music", "subj:broader": {"@id": "subj:arts"}}
                        ]}),
            )
            .await;
            // catalog2: library.example registered first, subject.example only via
            // the ref to subj:jazz → subject.example gets a *different* code here.
            insert_indexed(
                &fluree,
                &handle,
                "catalog2:main",
                &json!({"@context": {"lib": "http://library.example/", "subj": "http://subject.example/"},
                        "@graph": [ {"@id": "lib:book1", "lib:subject": {"@id": "subj:jazz"}} ]}),
            )
            .await;

            let sparql = r"
PREFIX subj: <http://subject.example/>
PREFIX lib: <http://library.example/>
SELECT DISTINCT ?book FROM NAMED <catalog2:main> FROM NAMED <taxonomy:main>
WHERE { GRAPH <catalog2:main> { ?book lib:subject ?c }
        GRAPH <taxonomy:main>  { ?c subj:broader subj:music } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(result.is_ok(), "should execute, got: {:?}", result.err());
            let cat = fluree.ledger("catalog2:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&cat.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(s.contains("lib:book1"), "divergent-ns cross-graph join returned no rows: {s}");
        })
        .await;
}

/// Q3d (mitigation) — same divergent shape as Q3c, but `catalog3` is seeded with
/// a deterministic *vocabulary warm-up*: it touches the shared `subject.example`
/// namespace FIRST (before its own `lib:` prefix), so `subj:` gets the SAME code
/// as in `taxonomy`. Aligning the codes sidesteps the re-encoding gap, so this
/// returns `lib:book1` even before the fix.
#[tokio::test]
async fn q3d_namespace_warmup_aligns_codes_and_join_works() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            insert_indexed(
                &fluree,
                &handle,
                "taxonomy:main",
                &json!({"@context": {"subj": "http://subject.example/"},
                        "@graph": [
                            {"@id": "subj:jazz",  "subj:broader": {"@id": "subj:music"}},
                            {"@id": "subj:music", "subj:broader": {"@id": "subj:arts"}}
                        ]}),
            )
            .await;
            // WARM-UP: register subject.example FIRST via a throwaway node, THEN
            // the library-specific (lib:) data. subject.example now aligns.
            insert_indexed(
                &fluree,
                &handle,
                "catalog3:main",
                &json!({"@context": {"subj": "http://subject.example/", "lib": "http://library.example/"},
                        "@graph": [
                            {"@id": "subj:_vocab", "subj:_seed": "1"},
                            {"@id": "lib:book1", "lib:subject": {"@id": "subj:jazz"}}
                        ]}),
            )
            .await;

            let sparql = r"
PREFIX subj: <http://subject.example/>
PREFIX lib: <http://library.example/>
SELECT DISTINCT ?book FROM NAMED <catalog3:main> FROM NAMED <taxonomy:main>
WHERE { GRAPH <catalog3:main> { ?book lib:subject ?c }
        GRAPH <taxonomy:main>  { ?c subj:broader subj:music } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(result.is_ok(), "should execute, got: {:?}", result.err());
            let cat = fluree.ledger("catalog3:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&cat.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(s.contains("lib:book1"), "warm-up did NOT align codes: {s}");
        })
        .await;
}

// =============================================================================
// Usage-pattern matrix (issue #1405, bugs 2+3). Each new case is INDEXED and
// uses DIVERGENT namespace codes (a ledger registers its own prefix first, the
// shared one only via a ref), unless noted. These pin behaviors the q1–q3d
// repro does not.
// =============================================================================

/// A1 (join independent datasets) — a join key bound as SUBJECT in graph 1 and
/// used as OBJECT in graph 2, under namespace divergence. `lib:book1` is a
/// `lib:Book` on the shelf and appears as the object of `list:includes` in a
/// reading list. Exercises the object-position substitution the subject-position
/// repro (Q3c) does not. Expected: `list:reading1`.
#[tokio::test]
async fn a1_object_position_cross_graph_join_divergent_ns() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            // shelf: library.example registered FIRST; lib:book1 is a subject.
            insert_indexed(
                &fluree,
                &handle,
                "shelf:main",
                &json!({"@context": {"lib": "http://library.example/"},
                        "@graph": [{"@id": "lib:book1", "@type": "lib:Book"}]}),
            )
            .await;
            // lists: list.example registered first; library.example only via the
            // ref → a different code. lib:book1 is the OBJECT of list:includes.
            insert_indexed(
                &fluree,
                &handle,
                "lists:main",
                &json!({"@context": {"list": "http://list.example/", "lib": "http://library.example/"},
                        "@graph": [{"@id": "list:reading1", "list:includes": {"@id": "lib:book1"}}]}),
            )
            .await;

            let sparql = r"
PREFIX lib: <http://library.example/>
PREFIX list: <http://list.example/>
SELECT DISTINCT ?list FROM NAMED <shelf:main> FROM NAMED <lists:main>
WHERE { GRAPH <shelf:main> { ?book a lib:Book }
        GRAPH <lists:main> { ?list list:includes ?book } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(result.is_ok(), "should execute, got: {:?}", result.err());
            let lists = fluree.ledger("lists:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&lists.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(
                s.contains("list:reading1"),
                "object-position divergent-ns join returned no rows: {s}"
            );
        })
        .await;
}

/// A2 (completeness) — a book with TWO subjects under divergence: BOTH must come
/// back (per-value re-encode, nothing dropped). Expected: labels `Jazz` AND
/// `Blues`.
#[tokio::test]
async fn a2_multi_value_cross_graph_join_divergent_ns() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            insert_indexed(
                &fluree,
                &handle,
                "taxonomy:main",
                &json!({"@context": {"subj": "http://subject.example/",
                                     "rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
                        "@graph": [
                            {"@id": "subj:jazz",  "rdfs:label": "Jazz"},
                            {"@id": "subj:blues", "rdfs:label": "Blues"}
                        ]}),
            )
            .await;
            insert_indexed(
                &fluree,
                &handle,
                "catm:main",
                &json!({"@context": {"lib": "http://library.example/", "subj": "http://subject.example/"},
                        "@graph": [{"@id": "lib:book1",
                                    "lib:subject": [{"@id": "subj:jazz"}, {"@id": "subj:blues"}]}]}),
            )
            .await;

            let sparql = r"
PREFIX subj: <http://subject.example/>
PREFIX lib: <http://library.example/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?label FROM NAMED <catm:main> FROM NAMED <taxonomy:main>
WHERE { GRAPH <catm:main> { lib:book1 lib:subject ?c }
        GRAPH <taxonomy:main> { ?c rdfs:label ?label } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(result.is_ok(), "should execute, got: {:?}", result.err());
            let cat = fluree.ledger("catm:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&cat.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(
                s.contains("Jazz") && s.contains("Blues"),
                "multi-value divergent-ns join dropped a value (want both Jazz+Blues): {s}"
            );
        })
        .await;
}

/// A3 (precision) — a book with two subjects (jazz, music); only `subj:music`
/// has `subj:broader subj:arts` (`subj:jazz`'s broader is `subj:music`). The
/// divergent-ns join must return EXACTLY `subj:music` and must NOT falsely match
/// `subj:jazz`. Guards against a re-encode so loose it over-matches.
#[tokio::test]
async fn a3_cross_graph_join_precision_divergent_ns() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            insert_indexed(
                &fluree,
                &handle,
                "taxonomy:main",
                &json!({"@context": {"subj": "http://subject.example/"},
                        "@graph": [
                            {"@id": "subj:music", "subj:broader": {"@id": "subj:arts"}},
                            {"@id": "subj:jazz",  "subj:broader": {"@id": "subj:music"}}
                        ]}),
            )
            .await;
            insert_indexed(
                &fluree,
                &handle,
                "catp:main",
                &json!({"@context": {"lib": "http://library.example/", "subj": "http://subject.example/"},
                        "@graph": [{"@id": "lib:book1",
                                    "lib:subject": [{"@id": "subj:jazz"}, {"@id": "subj:music"}]}]}),
            )
            .await;

            let sparql = r"
PREFIX subj: <http://subject.example/>
PREFIX lib: <http://library.example/>
SELECT DISTINCT ?c FROM NAMED <catp:main> FROM NAMED <taxonomy:main>
WHERE { GRAPH <catp:main> { lib:book1 lib:subject ?c }
        GRAPH <taxonomy:main> { ?c subj:broader subj:arts } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(result.is_ok(), "should execute, got: {:?}", result.err());
            let tax = fluree.ledger("taxonomy:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&tax.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(s.contains("subj:music"), "precision join missed the true match subj:music: {s}");
            assert!(
                !s.contains("subj:jazz"),
                "precision join falsely matched subj:jazz (over-match): {s}"
            );
        })
        .await;
}

/// A4 (taxonomy + instances) — `subj:broader+` (strict "proper ancestors")
/// scoped path joined to a default-graph book, under divergence. Exercises the
/// indexed-GRAPH-path materialization AND the divergent join key together.
/// Expected: `lib:book1`.
#[tokio::test]
async fn a4_strict_path_plus_join_divergent_ns() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            insert_indexed(
                &fluree,
                &handle,
                "taxonomy:main",
                &json!({"@context": {"subj": "http://subject.example/"},
                        "@graph": [
                            {"@id": "subj:jazz",  "subj:broader": {"@id": "subj:music"}},
                            {"@id": "subj:music", "subj:broader": {"@id": "subj:arts"}}
                        ]}),
            )
            .await;
            insert_indexed(
                &fluree,
                &handle,
                "cata:main",
                &json!({"@context": {"lib": "http://library.example/", "subj": "http://subject.example/"},
                        "@graph": [{"@id": "lib:book1", "lib:subject": {"@id": "subj:jazz"}}]}),
            )
            .await;

            let sparql = r"
PREFIX subj: <http://subject.example/>
PREFIX lib: <http://library.example/>
SELECT DISTINCT ?book FROM <cata:main> FROM NAMED <taxonomy:main>
WHERE { ?book lib:subject ?c . GRAPH <taxonomy:main> { ?c subj:broader+ subj:arts } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(result.is_ok(), "should execute, got: {:?}", result.err());
            let cat = fluree.ledger("cata:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&cat.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(
                s.contains("lib:book1"),
                "strict-path + divergent-ns join returned no rows: {s}"
            );
        })
        .await;
}

/// A5 (chained hop) — a join key crossing TWO ledger boundaries
/// (catalog → thesaurus → labels), all with divergent codes on the shared
/// `subj:` namespace. Pins that re-encoding COMPOSES across more than one
/// boundary. Expected: `Music`.
#[tokio::test]
async fn a5_three_ledger_chain_divergent_ns() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            // Each ledger registers its own local prefix FIRST so the shared
            // `subj:` namespace gets a different code in each.
            insert_indexed(
                &fluree,
                &handle,
                "catalog:main",
                &json!({"@context": {"lib": "http://library.example/", "subj": "http://subject.example/"},
                        "@graph": [{"@id": "lib:book1", "lib:subject": {"@id": "subj:jazz"}}]}),
            )
            .await;
            insert_indexed(
                &fluree,
                &handle,
                "thesaurus:main",
                &json!({"@context": {"th": "http://thesaurus.example/", "subj": "http://subject.example/"},
                        "@graph": [
                            {"@id": "th:_seed", "th:_x": "1"},
                            {"@id": "subj:jazz", "subj:broader": {"@id": "subj:music"}}
                        ]}),
            )
            .await;
            insert_indexed(
                &fluree,
                &handle,
                "labels:main",
                &json!({"@context": {"lbl": "http://labels.example/", "subj": "http://subject.example/",
                                     "rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
                        "@graph": [
                            {"@id": "lbl:_seed", "lbl:_x": "1"},
                            {"@id": "subj:music", "rdfs:label": "Music"}
                        ]}),
            )
            .await;

            let sparql = r"
PREFIX subj: <http://subject.example/>
PREFIX lib: <http://library.example/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?l FROM NAMED <catalog:main> FROM NAMED <thesaurus:main> FROM NAMED <labels:main>
WHERE { GRAPH <catalog:main>   { lib:book1 lib:subject ?c }
        GRAPH <thesaurus:main> { ?c subj:broader ?d }
        GRAPH <labels:main>    { ?d rdfs:label ?l } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(result.is_ok(), "should execute, got: {:?}", result.err());
            let labels = fluree.ledger("labels:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&labels.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(
                s.contains("Music"),
                "three-ledger chained divergent-ns join returned no rows: {s}"
            );
        })
        .await;
}

/// A6 (regression) — a SINGLE-ledger GRAPH-scoped path is unaffected by the
/// multi-ledger materialization gating (which fires only when stamping is
/// needed). Green before and after. Expected: `subj:jazz`, `subj:music`,
/// `subj:arts`.
#[tokio::test]
async fn a6_single_ledger_graph_path_unaffected() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            insert_indexed(
                &fluree,
                &handle,
                "taxonomy:main",
                &json!({"@context": {"subj": "http://subject.example/"},
                "@graph": [
                    {"@id": "subj:jazz",  "subj:broader": {"@id": "subj:music"}},
                    {"@id": "subj:music", "subj:broader": {"@id": "subj:arts"}}
                ]}),
            )
            .await;

            let sparql = r"
PREFIX subj: <http://subject.example/>
SELECT DISTINCT ?anc FROM NAMED <taxonomy:main>
WHERE { GRAPH <taxonomy:main> { subj:jazz subj:broader* ?anc } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(
                result.is_ok(),
                "single-ledger GRAPH path should execute, got: {:?}",
                result.err()
            );
            let tax = fluree.ledger("taxonomy:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&tax.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(
                s.contains("subj:jazz") && s.contains("subj:music") && s.contains("subj:arts"),
                "single-ledger GRAPH path did not return the full chain: {s}"
            );
        })
        .await;
}

/// A7 (semi-join) — `FILTER EXISTS` across a GRAPH boundary. This path is
/// `SeedOperator`-based (distinct from the nested-loop join), so it's a separate
/// code path — but the root-cause materialization fix covers it too. Keeps the
/// book whose subject has `subj:broader subj:music`. Expected: `lib:book1`.
#[tokio::test]
async fn a7_filter_exists_cross_graph_divergent_ns() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            insert_indexed(
                &fluree,
                &handle,
                "taxonomy:main",
                &json!({"@context": {"subj": "http://subject.example/"},
                        "@graph": [{"@id": "subj:jazz", "subj:broader": {"@id": "subj:music"}}]}),
            )
            .await;
            insert_indexed(
                &fluree,
                &handle,
                "catx:main",
                &json!({"@context": {"lib": "http://library.example/", "subj": "http://subject.example/"},
                        "@graph": [{"@id": "lib:book1", "lib:subject": {"@id": "subj:jazz"}}]}),
            )
            .await;

            let sparql = r"
PREFIX subj: <http://subject.example/>
PREFIX lib: <http://library.example/>
SELECT DISTINCT ?book FROM NAMED <catx:main> FROM NAMED <taxonomy:main>
WHERE { GRAPH <catx:main> { ?book lib:subject ?c }
        FILTER EXISTS { GRAPH <taxonomy:main> { ?c subj:broader subj:music } } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(result.is_ok(), "should execute, got: {:?}", result.err());
            let cat = fluree.ledger("catx:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&cat.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(
                s.contains("lib:book1"),
                "FILTER EXISTS across a GRAPH boundary (divergent ns) dropped the row: {s}"
            );
        })
        .await;
}

/// A8 (taxonomy crawl) — a BOTH-endpoints-unbound closure (`?s subj:broader+
/// ?o`) inside a GRAPH block, where the query's primary ledger differs from the
/// path's graph so the path predicate `subj:broader` has a divergent code. Pins
/// that the closure/adjacency read (not just the bounded read_step) also
/// re-encodes the traversal predicate. Expected: pairs incl. `subj:arts`.
#[tokio::test]
async fn a8_unbounded_closure_in_graph_divergent_pred() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            // primary (first FROM NAMED): registers lib: first, subj: only via a
            // ref → subj: gets a divergent code vs taxonomy.
            insert_indexed(
                &fluree,
                &handle,
                "prim:main",
                &json!({"@context": {"lib": "http://library.example/", "subj": "http://subject.example/"},
                        "@graph": [{"@id": "lib:book1", "lib:subject": {"@id": "subj:jazz"}}]}),
            )
            .await;
            insert_indexed(
                &fluree,
                &handle,
                "taxonomy:main",
                &json!({"@context": {"subj": "http://subject.example/"},
                        "@graph": [
                            {"@id": "subj:jazz",  "subj:broader": {"@id": "subj:music"}},
                            {"@id": "subj:music", "subj:broader": {"@id": "subj:arts"}}
                        ]}),
            )
            .await;

            let sparql = r"
PREFIX subj: <http://subject.example/>
SELECT DISTINCT ?s ?o FROM NAMED <prim:main> FROM NAMED <taxonomy:main>
WHERE { GRAPH <taxonomy:main> { ?s subj:broader+ ?o } }";
            let result = fluree.query_connection_sparql(sparql).await;
            assert!(result.is_ok(), "should execute, got: {:?}", result.err());
            let tax = fluree.ledger("taxonomy:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&tax.snapshot)
                .expect("to_jsonld")
                .to_string();
            // jazz→music→arts: closure must include the deep pair reaching subj:arts.
            assert!(
                s.contains("subj:arts") && s.contains("subj:jazz"),
                "unbounded closure with a divergent-code predicate found no edges: {s}"
            );
        })
        .await;
}

/// A9 (JSON-LD regression guard) — the JSON-LD analog of Q3a: a default-graph
/// pattern (`catalog`) joined to a `["graph", …]`-scoped pattern in `taxonomy`,
/// over divergent, indexed ledgers. Expected: `lib:book1`.
///
/// NOTE: unlike the SPARQL cases, this passes with *or* without the
/// `DatasetOperator` materialization fix — the JSON-LD `query_connection` path
/// does not late-materialize these bindings to `EncodedSid`, so it never hit the
/// bug the SPARQL path did (q3a/q3c). Kept as a guard that JSON-LD cross-ledger
/// cross-graph joins keep working; it is NOT a red→green demonstration of the
/// fix.
#[tokio::test]
async fn a9_jsonld_cross_graph_join_divergent_ns() {
    let (fluree, local, handle) = fluree_with_indexer();
    local
        .run_until(async move {
            insert_indexed(
                &fluree,
                &handle,
                "taxonomy:main",
                &json!({"@context": {"subj": "http://subject.example/"},
                        "@graph": [
                            {"@id": "subj:jazz",  "subj:broader": {"@id": "subj:music"}},
                            {"@id": "subj:music", "subj:broader": {"@id": "subj:arts"}}
                        ]}),
            )
            .await;
            insert_indexed(
                &fluree,
                &handle,
                "catalog:main",
                &json!({"@context": {"lib": "http://library.example/", "subj": "http://subject.example/"},
                        "@graph": [ {"@id": "lib:book1", "lib:subject": {"@id": "subj:jazz"}} ]}),
            )
            .await;

            let query = json!({
                "@context": {"subj": "http://subject.example/", "lib": "http://library.example/"},
                "from": "catalog:main",
                "fromNamed": ["taxonomy:main"],
                "select": ["?book"],
                "where": [
                    {"@id": "?book", "lib:subject": "?c"},
                    ["graph", "taxonomy:main", {"@id": "?c", "subj:broader": {"@id": "subj:music"}}]
                ]
            });
            let result = fluree.query_connection(&query).await;
            assert!(result.is_ok(), "JSON-LD cross-graph join should execute, got: {:?}", result.err());
            let cat = fluree.ledger("catalog:main").await.expect("load");
            let s = result
                .unwrap()
                .to_jsonld(&cat.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(
                s.contains("lib:book1"),
                "JSON-LD divergent-ns cross-graph join returned no rows: {s}"
            );
        })
        .await;
}
