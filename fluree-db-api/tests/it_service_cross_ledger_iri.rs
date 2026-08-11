//! IRIs crossing a `SERVICE <fluree:ledger:...>` boundary.
//!
//! A SERVICE block that targets a different ledger used to hand its rows back
//! carrying that ledger's SIDs. The parent then decoded them against its own
//! namespace table, so every IRI the block newly bound came out with the wrong
//! prefix — a well-formed absolute IRI naming the wrong thing, on a 200.
//!
//! The fixtures below give the two ledgers prefixes that were allocated the
//! same namespace code, which is what makes a wrong-table decode visible rather
//! than merely possible: `http://beta.example/b1` mis-decodes to
//! `http://alpha.example/b1` instead of failing.
//!
//! Joins are deliberately covered in both orders even though they were already
//! correct (SERVICE seeds its inner tree from the parent row, so cross-boundary
//! identity comes from substitution, never from comparing a foreign SID) —
//! stamping changes the binding representation, and these pin that it did not
//! disturb them.

use crate::support::{build_and_publish_index, genesis_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::{DataSetDb, DatasetSpec, FlureeBuilder, GraphSource};
use serde_json::{json, Value as JsonValue};

const ALPHA: &str = "http://alpha.example/";
const BETA: &str = "http://beta.example/";

async fn seed(fluree: &MemoryFluree, ledger_id: &str, graph: JsonValue) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(ledger0, &json!({ "@graph": graph }))
        .await
        .expect("seed")
        .ledger
}

/// alpha holds one subject, beta two. Each ledger sees exactly one prefix
/// before the shared `tag` predicate, so both `http://alpha.example/` and
/// `http://beta.example/` are allocated the same namespace code.
async fn seed_pair(fluree: &MemoryFluree, suffix: &str) -> (String, String) {
    let alpha = format!("xl-alpha-{suffix}:main");
    let beta = format!("xl-beta-{suffix}:main");
    seed(
        fluree,
        &alpha,
        json!([{"@id": format!("{ALPHA}a1"), format!("{ALPHA}tag"): "shared"}]),
    )
    .await;
    seed(
        fluree,
        &beta,
        json!([
            {"@id": format!("{BETA}b1"), format!("{BETA}tag"): "shared", format!("{BETA}rank"): 2},
            {"@id": format!("{BETA}b2"), format!("{BETA}tag"): "shared", format!("{BETA}rank"): 1}
        ]),
    )
    .await;
    (alpha, beta)
}

async fn dataset_for(fluree: &MemoryFluree, alpha: &str, beta: &str) -> DataSetDb {
    let spec = DatasetSpec::new()
        .with_default(GraphSource::new(alpha))
        .with_named(GraphSource::new(beta));
    fluree
        .build_dataset_view(&spec)
        .await
        .expect("build_dataset_view")
}

async fn rows(fluree: &MemoryFluree, dataset: &DataSetDb, q: &str) -> Vec<JsonValue> {
    let result = fluree
        .query_dataset(dataset, q)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{q}"));
    let jsonld = result
        .to_jsonld(dataset.primary().unwrap().snapshot.as_ref())
        .expect("to_jsonld");
    let mut v = jsonld.as_array().cloned().unwrap_or_default();
    v.sort_by_key(|r| serde_json::to_string(r).unwrap_or_default());
    v
}

/// Every shape that carries a SERVICE-bound IRI out of the block. Each must
/// name the beta subjects with beta's prefix.
#[tokio::test]
async fn service_cross_ledger_iris_keep_their_own_prefix() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (alpha, beta) = seed_pair(&fluree, "shapes").await;
    let dataset = dataset_for(&fluree, &alpha, &beta).await;
    let svc = format!("fluree:ledger:{beta}");

    let select = rows(
        &fluree,
        &dataset,
        &format!(r#"SELECT ?s WHERE {{ SERVICE <{svc}> {{ ?s ?p "shared" }} }}"#),
    )
    .await;
    assert_eq!(
        select,
        vec![json!([format!("{BETA}b1")]), json!([format!("{BETA}b2")])]
    );

    let distinct = rows(
        &fluree,
        &dataset,
        &format!(r#"SELECT DISTINCT ?s WHERE {{ SERVICE <{svc}> {{ ?s ?p "shared" }} }}"#),
    )
    .await;
    assert_eq!(distinct, select, "DISTINCT must not change the terms");

    let grouped = rows(
        &fluree,
        &dataset,
        &format!(
            r#"SELECT ?s (COUNT(*) AS ?n) WHERE {{ SERVICE <{svc}> {{ ?s ?p "shared" }} }} GROUP BY ?s"#
        ),
    )
    .await;
    assert_eq!(
        grouped,
        vec![
            json!([format!("{BETA}b1"), 1]),
            json!([format!("{BETA}b2"), 1])
        ]
    );

    let concat = rows(
        &fluree,
        &dataset,
        &format!(
            r#"SELECT (GROUP_CONCAT(?s; SEPARATOR="|") AS ?g) WHERE {{ SERVICE <{svc}> {{ ?s ?p "shared" }} }}"#
        ),
    )
    .await;
    let g = concat[0][0].as_str().expect("concat is a string");
    let parts: std::collections::HashSet<&str> = g.split('|').collect();
    assert_eq!(
        parts,
        [format!("{BETA}b1"), format!("{BETA}b2")]
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>(),
        "got {g:?}"
    );

    // ORDER BY sorts on a beta literal but projects the beta IRI. Kept to one
    // triple in the body: a multi-pattern SERVICE body returns no rows at all
    // on this build, which is a separate pre-existing defect and would mask
    // what this case is here to check.
    let ordered = fluree
        .query_dataset(
            &dataset,
            &format!(
                "SELECT ?s WHERE {{ SERVICE <{svc}> {{ ?s <{BETA}rank> ?r }} }} ORDER BY ?r"
            ),
        )
        .await
        .expect("ORDER BY over a SERVICE-bound IRI")
        .to_jsonld(dataset.primary().unwrap().snapshot.as_ref())
        .expect("to_jsonld");
    assert_eq!(
        ordered,
        json!([[format!("{BETA}b2")], [format!("{BETA}b1")]]),
        "rank 1 (b2) before rank 2 (b1), both with beta's prefix"
    );
}

/// The same shapes against an INDEXED target. The stamping path cannot decode
/// an `EncodedSid`, so the SERVICE subtree has to fall back off the binary
/// store the way dataset members do — without that this errors rather than
/// mis-decoding.
#[tokio::test]
async fn service_cross_ledger_iris_on_an_indexed_target() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (alpha, beta) = seed_pair(&fluree, "indexed").await;
    build_and_publish_index(&fluree, &beta).await;
    let dataset = dataset_for(&fluree, &alpha, &beta).await;
    let svc = format!("fluree:ledger:{beta}");

    let select = rows(
        &fluree,
        &dataset,
        &format!(r#"SELECT ?s WHERE {{ SERVICE <{svc}> {{ ?s ?p "shared" }} }}"#),
    )
    .await;
    assert_eq!(
        select,
        vec![json!([format!("{BETA}b1")]), json!([format!("{BETA}b2")])]
    );
}

/// A self-referencing SERVICE names the ledger it is already in, so nothing
/// crosses a boundary and the terms are unchanged.
#[tokio::test]
async fn service_same_ledger_is_unaffected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (alpha, beta) = seed_pair(&fluree, "self").await;
    let dataset = dataset_for(&fluree, &alpha, &beta).await;

    let select = rows(
        &fluree,
        &dataset,
        &format!(r#"SELECT ?s WHERE {{ SERVICE <fluree:ledger:{alpha}> {{ ?s ?p "shared" }} }}"#),
    )
    .await;
    assert_eq!(select, vec![json!([format!("{ALPHA}a1")])]);
}

/// Joins across the boundary, in both orders, against the `GRAPH` form that
/// already went through the dataset operator's stamping. Two subjects sharing a
/// local name under different prefixes must not be conflated, and the same
/// absolute IRI under different namespace codes must still match.
#[tokio::test]
async fn service_cross_ledger_join_identity_matches_graph_form() {
    let fluree = FlureeBuilder::memory().build_memory();
    // Same local name on both sides, different prefixes: never the same term.
    seed(
        &fluree,
        "xl-alpha-join:main",
        json!([{"@id": format!("{ALPHA}x"), format!("{ALPHA}tag"): "A"}]),
    )
    .await;
    seed(
        &fluree,
        "xl-beta-join:main",
        json!([{"@id": format!("{BETA}x"), format!("{BETA}tag"): "B"}]),
    )
    .await;
    let dataset = dataset_for(&fluree, "xl-alpha-join:main", "xl-beta-join:main").await;

    for (label, inner) in [
        ("service", "SERVICE <fluree:ledger:xl-beta-join:main>"),
        ("graph", "GRAPH <xl-beta-join:main>"),
    ] {
        let local_first = rows(
            &fluree,
            &dataset,
            &format!(r#"SELECT ?s WHERE {{ ?s <{ALPHA}tag> "A" . {inner} {{ ?s ?p "B" }} }}"#),
        )
        .await;
        assert!(
            local_first.is_empty(),
            "[{label}] different IRIs sharing a local name must not join: {local_first:?}"
        );

        let remote_first = rows(
            &fluree,
            &dataset,
            &format!(r#"SELECT ?s WHERE {{ {inner} {{ ?s ?p "B" }} . ?s <{ALPHA}tag> "A" }}"#),
        )
        .await;
        assert!(
            remote_first.is_empty(),
            "[{label}] same, with the remote pattern written first: {remote_first:?}"
        );
    }
}

/// The same absolute IRI on both sides, deliberately given different namespace
/// codes (beta allocates its own prefix first), must still join.
#[tokio::test]
async fn service_cross_ledger_join_matches_on_shared_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    const SHARED: &str = "http://shared.example/";
    seed(
        &fluree,
        "xl-alpha-shared:main",
        json!([{"@id": format!("{SHARED}thing"), format!("{SHARED}tag"): "A"}]),
    )
    .await;
    seed(
        &fluree,
        "xl-beta-shared:main",
        json!([
            {"@id": format!("{BETA}filler"), format!("{BETA}tag"): "filler"},
            {"@id": format!("{SHARED}thing"), format!("{BETA}tag"): "B"}
        ]),
    )
    .await;
    let dataset = dataset_for(&fluree, "xl-alpha-shared:main", "xl-beta-shared:main").await;

    let joined = rows(
        &fluree,
        &dataset,
        &format!(
            r#"SELECT ?s WHERE {{ ?s <{SHARED}tag> "A" .
               SERVICE <fluree:ledger:xl-beta-shared:main> {{ ?s <{BETA}tag> "B" }} }}"#
        ),
    )
    .await;
    assert_eq!(joined, vec![json!([format!("{SHARED}thing")])]);
}
