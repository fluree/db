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
            &format!("SELECT ?s WHERE {{ SERVICE <{svc}> {{ ?s <{BETA}rank> ?r }} }} ORDER BY ?r"),
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

/// A SERVICE body with two patterns sharing `?s` must return the joined rows,
/// exactly as the same body evaluated via `GRAPH` does (issue #1665).
///
/// The join key crosses between the body's patterns as a raw scan binding, so
/// this is the shape where a target-encoded SID substituted back into a
/// pattern gets decoded against the requester's namespace table — the
/// `seed_pair` fixture's same-code/different-prefix tables make that visible
/// as zero rows rather than a coincidental match.
#[tokio::test]
async fn service_multi_pattern_body_returns_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (alpha, beta) = seed_pair(&fluree, "mp").await;
    let dataset = dataset_for(&fluree, &alpha, &beta).await;
    let svc = format!("fluree:ledger:{beta}");

    let graph_control = rows(
        &fluree,
        &dataset,
        &format!(
            r#"SELECT ?s ?r WHERE {{ GRAPH <{beta}> {{ ?s ?p "shared" . ?s <{BETA}rank> ?r }} }}"#
        ),
    )
    .await;
    assert_eq!(
        graph_control,
        vec![
            json!([format!("{BETA}b1"), 2]),
            json!([format!("{BETA}b2"), 1])
        ],
        "GRAPH control must return the joined rows"
    );

    let service = rows(
        &fluree,
        &dataset,
        &format!(
            r#"SELECT ?s ?r WHERE {{ SERVICE <{svc}> {{ ?s ?p "shared" . ?s <{BETA}rank> ?r }} }}"#
        ),
    )
    .await;
    assert_eq!(service, graph_control, "SERVICE body must join like GRAPH");
}

/// Three patterns chained on the same subject: each hop's binding crosses back
/// into a pattern, so every hop after the first exercises the substitution.
#[tokio::test]
async fn service_three_pattern_body_returns_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (alpha, beta) = seed_pair(&fluree, "mp3").await;
    let dataset = dataset_for(&fluree, &alpha, &beta).await;
    let svc = format!("fluree:ledger:{beta}");

    let body = format!(r#"?s ?p "shared" . ?s <{BETA}rank> ?r . ?s <{BETA}tag> ?t"#);
    let graph_control = rows(
        &fluree,
        &dataset,
        &format!(r"SELECT ?s ?r ?t WHERE {{ GRAPH <{beta}> {{ {body} }} }}"),
    )
    .await;
    assert_eq!(
        graph_control,
        vec![
            json!([format!("{BETA}b1"), 2, "shared"]),
            json!([format!("{BETA}b2"), 1, "shared"])
        ],
        "GRAPH control must return the joined rows"
    );

    let service = rows(
        &fluree,
        &dataset,
        &format!(r"SELECT ?s ?r ?t WHERE {{ SERVICE <{svc}> {{ {body} }} }}"),
    )
    .await;
    assert_eq!(service, graph_control, "SERVICE body must join like GRAPH");
}

/// A shared-variable join with no constants at all in the second pattern —
/// the pure form of the intra-body join, no predicate/object narrowing to
/// hide behind.
#[tokio::test]
async fn service_multi_pattern_all_variable_join() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (alpha, beta) = seed_pair(&fluree, "mpv").await;
    let dataset = dataset_for(&fluree, &alpha, &beta).await;
    let svc = format!("fluree:ledger:{beta}");

    let body = r#"?s ?p "shared" . ?s ?p2 ?o2"#;
    let graph_control = rows(
        &fluree,
        &dataset,
        &format!(r"SELECT ?s ?p2 ?o2 WHERE {{ GRAPH <{beta}> {{ {body} }} }}"),
    )
    .await;
    assert_eq!(
        graph_control.len(),
        4,
        "each beta subject carries two facts: {graph_control:?}"
    );

    let service = rows(
        &fluree,
        &dataset,
        &format!(r"SELECT ?s ?p2 ?o2 WHERE {{ SERVICE <{svc}> {{ {body} }} }}"),
    )
    .await;
    assert_eq!(service, graph_control, "SERVICE body must join like GRAPH");
}

/// Two patterns with NO shared variable (a cross product) worked before this
/// fix — no binding crosses back into a pattern — and must keep working.
#[tokio::test]
async fn service_multi_pattern_cross_product_unaffected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (alpha, beta) = seed_pair(&fluree, "mpx").await;
    let dataset = dataset_for(&fluree, &alpha, &beta).await;
    let svc = format!("fluree:ledger:{beta}");

    let body = format!(r#"?s <{BETA}tag> "shared" . ?x <{BETA}rank> ?r"#);
    let graph_control = rows(
        &fluree,
        &dataset,
        &format!(r"SELECT ?s ?x ?r WHERE {{ GRAPH <{beta}> {{ {body} }} }}"),
    )
    .await;
    assert_eq!(graph_control.len(), 4, "2 x 2 cross product");

    let service = rows(
        &fluree,
        &dataset,
        &format!(r"SELECT ?s ?x ?r WHERE {{ SERVICE <{svc}> {{ {body} }} }}"),
    )
    .await;
    assert_eq!(service, graph_control, "SERVICE body must join like GRAPH");
}

/// Aligned-namespace control: both ledgers allocate their first code to the
/// SAME prefix, so the wrong-table decode coincidentally produces the right
/// IRI and the bug is masked. This pins the passing case so the divergent
/// fixtures above can't silently degrade into it, and so the fix costs the
/// aligned case nothing but the stamp.
#[tokio::test]
async fn service_multi_pattern_aligned_namespaces() {
    let fluree = FlureeBuilder::memory().build_memory();
    const COMMON: &str = "http://common.example/";
    seed(
        &fluree,
        "xl-alpha-aligned:main",
        json!([{"@id": format!("{COMMON}a1"), format!("{COMMON}tag"): "shared"}]),
    )
    .await;
    seed(
        &fluree,
        "xl-beta-aligned:main",
        json!([
            {"@id": format!("{COMMON}b1"), format!("{COMMON}tag"): "shared", format!("{COMMON}rank"): 2},
            {"@id": format!("{COMMON}b2"), format!("{COMMON}tag"): "shared", format!("{COMMON}rank"): 1}
        ]),
    )
    .await;
    let dataset = dataset_for(&fluree, "xl-alpha-aligned:main", "xl-beta-aligned:main").await;

    let service = rows(
        &fluree,
        &dataset,
        &format!(
            r#"SELECT ?s ?r WHERE {{ SERVICE <fluree:ledger:xl-beta-aligned:main>
               {{ ?s ?p "shared" . ?s <{COMMON}rank> ?r }} }}"#
        ),
    )
    .await;
    assert_eq!(
        service,
        vec![
            json!([format!("{COMMON}b1"), 2]),
            json!([format!("{COMMON}b2"), 1])
        ]
    );
}

// ---------------------------------------------------------------------------
// Binding producers inside the body other than a scan. Per-scan stamping makes
// every scan output an `IriMatch`; anything else that puts a reference into a
// body row — a BIND, a VALUES cell, the parent row itself, a property path —
// has to arrive in the same namespace-neutral form, or the fused-BIND clobber
// check, `=`/`!=`, and the boundary decode see a `Sid` against an `IriMatch`.
// ---------------------------------------------------------------------------

async fn seeded(fluree: &MemoryFluree, suffix: &str) -> (String, DataSetDb, String) {
    let (alpha, beta) = seed_pair(fluree, suffix).await;
    let dataset = dataset_for(fluree, &alpha, &beta).await;
    let svc = format!("fluree:ledger:{beta}");
    (beta, dataset, svc)
}

/// The SERVICE form of `body` must return exactly what the GRAPH form does,
/// and the GRAPH form must return `expect_rows` rows (so the control is not
/// vacuous).
async fn assert_matches_graph(
    fluree: &MemoryFluree,
    dataset: &DataSetDb,
    beta: &str,
    svc: &str,
    projection: &str,
    body: &str,
    expect_rows: usize,
) {
    let graph = rows(
        fluree,
        dataset,
        &format!(r"SELECT {projection} WHERE {{ GRAPH <{beta}> {{ {body} }} }}"),
    )
    .await;
    assert_eq!(graph.len(), expect_rows, "GRAPH control for: {body}");
    let service = rows(
        fluree,
        dataset,
        &format!(r"SELECT {projection} WHERE {{ SERVICE <{svc}> {{ {body} }} }}"),
    )
    .await;
    assert_eq!(service, graph, "SERVICE body must match GRAPH for: {body}");
}

/// A BIND that mints a reference the body then joins on: leading, via
/// `IRI()`, and in object position. Each used to return zero rows — the
/// minted `Sid` never unified with the stamped scan output.
#[tokio::test]
async fn service_body_bind_minted_reference_joins() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (beta, ds, svc) = seeded(&fluree, "bind-mint").await;
    for body in [
        format!(r"BIND(<{BETA}b1> AS ?s) ?s <{BETA}rank> ?r"),
        format!(r#"BIND(IRI("{BETA}b1") AS ?s) ?s <{BETA}rank> ?r"#),
        format!(r"?s <{BETA}rank> ?r . BIND(<{BETA}b1> AS ?x) FILTER(?s = ?x)"),
    ] {
        assert_matches_graph(&fluree, &ds, &beta, &svc, "?s ?r", &body, 1).await;
    }
    // Trailing BIND with no dependency and STR() of a stamped term were never
    // affected; pinned so the representation change cannot disturb them.
    assert_matches_graph(
        &fluree,
        &ds,
        &beta,
        &svc,
        "?s ?r ?x",
        &format!(r"?s <{BETA}rank> ?r BIND(<{BETA}b1> AS ?x)"),
        2,
    )
    .await;
    assert_matches_graph(
        &fluree,
        &ds,
        &beta,
        &svc,
        "?s ?str",
        &format!(r"?s <{BETA}rank> ?r BIND(STR(?s) AS ?str)"),
        2,
    )
    .await;
}

/// A BIND-minted reference in OBJECT position, and a ref-valued chain.
#[tokio::test]
async fn service_body_bind_minted_object_joins() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alpha = "xl-alpha-bind-obj:main";
    let beta = "xl-beta-bind-obj:main";
    seed(
        &fluree,
        alpha,
        json!([{"@id": format!("{ALPHA}a1"), format!("{ALPHA}tag"): "shared"}]),
    )
    .await;
    seed(
        &fluree,
        beta,
        json!([
            {"@id": format!("{BETA}b1"), format!("{BETA}tag"): "shared",
             format!("{BETA}knows"): {"@id": format!("{BETA}b2")}},
            {"@id": format!("{BETA}b2"), format!("{BETA}tag"): "shared"}
        ]),
    )
    .await;
    let ds = dataset_for(&fluree, alpha, beta).await;
    let svc = format!("fluree:ledger:{beta}");
    assert_matches_graph(
        &fluree,
        &ds,
        beta,
        &svc,
        "?s",
        &format!(r"BIND(<{BETA}b2> AS ?o) ?s <{BETA}knows> ?o"),
        1,
    )
    .await;
    assert_matches_graph(
        &fluree,
        &ds,
        beta,
        &svc,
        "?s ?o",
        &format!(r"?s <{BETA}knows> ?o . ?o <{BETA}tag> ?t"),
        1,
    )
    .await;
    assert_matches_graph(
        &fluree,
        &ds,
        beta,
        &svc,
        "?s ?o",
        &format!(r"?s <{BETA}knows> ?o FILTER(?o = <{BETA}b2>)"),
        1,
    )
    .await;
}

/// `=` / `!=` / `IN` / `sameTerm` between a stamped variable and an IRI
/// constant, in both fused (single scan) and post-join positions. The
/// post-join predicate case compares an `IriMatch` with a `Sid`-encoded
/// constant and used to be "different resource" — `!=` kept every row.
#[tokio::test]
async fn service_body_reference_equality_against_constants() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (beta, ds, svc) = seeded(&fluree, "ref-eq").await;
    for (body, n) in [
        (format!(r"?s <{BETA}rank> ?r FILTER(?s = <{BETA}b1>)"), 1),
        (format!(r"?s <{BETA}rank> ?r FILTER(?s != <{BETA}b1>)"), 1),
        (format!(r"?s <{BETA}rank> ?r FILTER(?s IN (<{BETA}b1>))"), 1),
        (
            format!(r"?s <{BETA}rank> ?r FILTER(sameTerm(?s, <{BETA}b1>))"),
            1,
        ),
        (
            format!(r"?s <{BETA}rank> ?r . ?s ?p ?o FILTER(isIRI(?s) && ?p = <{BETA}tag>)"),
            2,
        ),
        (
            format!(r"?s <{BETA}rank> ?r . ?s ?p ?o FILTER(?p != <{BETA}tag>)"),
            2,
        ),
        (
            format!(r"?s <{BETA}rank> ?r . ?s ?p ?o FILTER(?p IN (<{BETA}tag>))"),
            2,
        ),
        (
            format!(r"?s <{BETA}rank> ?r . ?s ?p ?o FILTER(sameTerm(?p, <{BETA}tag>))"),
            2,
        ),
    ] {
        assert_matches_graph(&fluree, &ds, &beta, &svc, "?s ?o", &body, n).await;
    }
}

/// Compound operators inside the body all match their GRAPH form.
#[tokio::test]
async fn service_body_compound_operators_match_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (beta, ds, svc) = seeded(&fluree, "compound").await;
    let cases: [(&str, String, usize); 8] = [
        (
            "?s ?r",
            format!(r"VALUES ?s {{ <{BETA}b1> }} ?s <{BETA}rank> ?r"),
            1,
        ),
        (
            "?s ?r ?t",
            format!(
                r#"?s ?p "shared" OPTIONAL {{ ?s <{BETA}rank> ?r }} OPTIONAL {{ ?s <{BETA}tag> ?t }}"#
            ),
            2,
        ),
        (
            "?s",
            format!(r#"?s ?p "shared" MINUS {{ ?s <{BETA}rank> 2 }}"#),
            1,
        ),
        (
            "?s ?r",
            format!(r"{{ ?s <{BETA}rank> ?r }} UNION {{ ?s <{BETA}rank> ?r FILTER(?r = 1) }}"),
            3,
        ),
        (
            "?s",
            format!(r#"?s ?p "shared" FILTER EXISTS {{ ?s <{BETA}rank> 2 }}"#),
            1,
        ),
        (
            "?s",
            format!(r#"?s ?p "shared" FILTER NOT EXISTS {{ ?s <{BETA}rank> 2 }}"#),
            1,
        ),
        (
            "?s ?r",
            format!(r#"{{ SELECT ?s WHERE {{ ?s ?p "shared" }} }} ?s <{BETA}rank> ?r"#),
            2,
        ),
        (
            "?s ?n",
            format!(
                r"{{ SELECT ?s (COUNT(?p) AS ?n) WHERE {{ ?s ?p ?o }} GROUP BY ?s }} ?s <{BETA}rank> ?r"
            ),
            2,
        ),
    ];
    for (projection, body, n) in cases {
        assert_matches_graph(&fluree, &ds, &beta, &svc, projection, &body, n).await;
    }
}

/// Property paths are not wrapped by `DatasetOperator`, so their output is
/// stamped by the operator itself. Covered: the path alone at the boundary,
/// a path consuming a stamped scan binding, and a path whose output is
/// substituted into the next scan (the #1665 shape, one operator over).
/// The last is asserted literally: its GRAPH form is #1770.
#[tokio::test]
async fn service_body_property_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alpha = "xl-alpha-path:main";
    let beta = "xl-beta-path:main";
    seed(
        &fluree,
        alpha,
        json!([{"@id": format!("{ALPHA}a1"), format!("{ALPHA}tag"): "shared"}]),
    )
    .await;
    seed(
        &fluree,
        beta,
        json!([
            {"@id": format!("{BETA}b1"), format!("{BETA}tag"): "one",
             format!("{BETA}knows"): {"@id": format!("{BETA}b2")}},
            {"@id": format!("{BETA}b2"), format!("{BETA}tag"): "two",
             format!("{BETA}knows"): {"@id": format!("{BETA}b3")}},
            {"@id": format!("{BETA}b3"), format!("{BETA}tag"): "three"}
        ]),
    )
    .await;
    let ds = dataset_for(&fluree, alpha, beta).await;
    let svc = format!("fluree:ledger:{beta}");
    let b = |n: &str| json!(format!("{BETA}{n}"));

    assert_matches_graph(
        &fluree,
        &ds,
        beta,
        &svc,
        "?o",
        &format!(r"<{BETA}b1> <{BETA}knows>+ ?o"),
        2,
    )
    .await;
    assert_matches_graph(
        &fluree,
        &ds,
        beta,
        &svc,
        "?s ?o",
        &format!(r#"?o <{BETA}tag> "three" . ?s <{BETA}knows>+ ?o"#),
        2,
    )
    .await;
    let path_then_join = rows(
        &fluree,
        &ds,
        &format!(
            r"SELECT ?o ?t WHERE {{ SERVICE <{svc}> {{ <{BETA}b1> <{BETA}knows>+ ?o . ?o <{BETA}tag> ?t }} }}"
        ),
    )
    .await;
    assert_eq!(
        path_then_join,
        vec![json!([b("b2"), "two"]), json!([b("b3"), "three"])]
    );
}

/// alpha allocates `http://alpha.example/` and a filler prefix before
/// `http://beta.example/`; beta allocates only `http://beta.example/`. The same
/// absolute IRI therefore carries a DIFFERENT namespace code in each ledger,
/// and beta's table has no entry at alpha's code — a requester-encoded `Sid`
/// decoded through beta's table fails outright instead of merely mis-naming.
async fn seed_colliding_codes(fluree: &MemoryFluree, suffix: &str) -> (String, String) {
    let alpha = format!("xl-alpha-coll-{suffix}:main");
    let beta = format!("xl-beta-coll-{suffix}:main");
    seed(
        fluree,
        &alpha,
        json!([
            {"@id": format!("{ALPHA}a1"), format!("{ALPHA}tag"): "x"},
            {"@id": "http://filler.example/f1", "http://filler.example/tag": "y"},
            {"@id": format!("{BETA}b1"), format!("{BETA}tag"): "A"}
        ]),
    )
    .await;
    seed(
        fluree,
        &beta,
        json!([
            {"@id": format!("{BETA}b1"), format!("{BETA}tag"): "B", format!("{BETA}rank"): 2},
            {"@id": format!("{BETA}b2"), format!("{BETA}tag"): "B", format!("{BETA}rank"): 1}
        ]),
    )
    .await;
    (alpha, beta)
}

/// Requester-encoded references that enter the body other than by pattern
/// substitution — a VALUES cell (lowered against the requester), and the
/// parent row copied by `BIND(?parent AS ?x)` — must be stamped in the
/// REQUESTER's ledger before the boundary stamp sees them. Under colliding
/// codes the boundary stamp used to fail with a decode error on both.
///
/// The `GRAPH <ledger>` form of a multi-ledger dataset has the same boundary
/// stamp and the same entry points, so it is held to the same answers.
#[tokio::test]
async fn service_body_requester_encoded_terms_under_colliding_codes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (alpha, beta) = seed_colliding_codes(&fluree, "entry").await;
    let ds = dataset_for(&fluree, &alpha, &beta).await;
    let svc = format!("fluree:ledger:{beta}");
    for blk in [format!("GRAPH <{beta}>"), format!("SERVICE <{svc}>")] {
        let values = rows(
            &fluree,
            &ds,
            &format!(
                r"SELECT ?s ?r WHERE {{ {blk} {{ VALUES ?s {{ <{BETA}b1> }} ?s <{BETA}rank> ?r }} }}"
            ),
        )
        .await;
        assert_eq!(values, vec![json!([format!("{BETA}b1"), 2])], "{blk}");

        let bind_parent = rows(
            &fluree,
            &ds,
            &format!(
                r#"SELECT ?a ?x ?s WHERE {{ ?a <{BETA}tag> "A" .
                   {blk} {{ BIND(?a AS ?x) ?s <{BETA}rank> 2 }} }}"#
            ),
        )
        .await;
        assert_eq!(
            bind_parent,
            vec![json!([
                format!("{BETA}b1"),
                format!("{BETA}b1"),
                format!("{BETA}b1")
            ])],
            "{blk}"
        );

        // A parent term compared (not substituted) inside the body; `!=`
        // keeps the comparison out of the equijoin fold.
        let compared = rows(
            &fluree,
            &ds,
            &format!(
                r#"SELECT ?a ?s WHERE {{ ?a <{BETA}tag> "A" .
                   {blk} {{ ?s <{BETA}tag> "B" FILTER(?s != ?a) }} }}"#
            ),
        )
        .await;
        assert_eq!(
            compared,
            vec![json!([format!("{BETA}b1"), format!("{BETA}b2")])],
            "{blk}"
        );
    }
}

/// A parent term the target ledger does not know must match nothing there —
/// in both the substituted and the compared form, in both lanes.
#[tokio::test]
async fn service_body_foreign_subject_absent_in_target() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (beta, ds, svc) = seeded(&fluree, "foreign").await;
    for blk in [format!("GRAPH <{beta}>"), format!("SERVICE <{svc}>")] {
        for body in [
            format!(r"?a <{BETA}tag> ?t"),
            format!(r"?s <{BETA}tag> ?t FILTER(?s = ?a)"),
        ] {
            let r = rows(
                &fluree,
                &ds,
                &format!(
                    r#"SELECT ?a ?t WHERE {{ ?a <{ALPHA}tag> "shared" . {blk} {{ {body} }} }}"#
                ),
            )
            .await;
            assert!(r.is_empty(), "{blk} {{ {body} }} must be empty, got {r:?}");
        }
    }
}

/// `?p = <iri>` after a join, where the constant DOES encode in the primary
/// (colliding codes: `http://beta.example/` exists in alpha too). The scan
/// side is a stamped `IriMatch`, the constant a `Sid` comparable; without the
/// decode bridge they are "different resources", so `=` drops every row and
/// `!=` keeps every row — in the GRAPH form as much as the SERVICE form. The
/// `isIRI(?s)` conjunct keeps the filter out of the pattern-constant fold, so
/// the comparison actually runs.
#[tokio::test]
async fn stamped_predicate_equality_against_primary_encodable_constant() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (alpha, beta) = seed_colliding_codes(&fluree, "pred-eq").await;
    let ds = dataset_for(&fluree, &alpha, &beta).await;
    let svc = format!("fluree:ledger:{beta}");
    let b = |n: &str| json!(format!("{BETA}{n}"));
    for blk in [format!("GRAPH <{beta}>"), format!("SERVICE <{svc}>")] {
        let eq = rows(
            &fluree,
            &ds,
            &format!(
                r"SELECT ?s ?o WHERE {{ {blk} {{ ?s <{BETA}rank> ?r . ?s ?p ?o FILTER(isIRI(?s) && ?p = <{BETA}tag>) }} }} ORDER BY ?s"
            ),
        )
        .await;
        assert_eq!(
            eq,
            vec![json!([b("b1"), "B"]), json!([b("b2"), "B"])],
            "= in {blk}"
        );
        let ne = rows(
            &fluree,
            &ds,
            &format!(
                r"SELECT ?s ?o WHERE {{ {blk} {{ ?s <{BETA}rank> ?r . ?s ?p ?o FILTER(isIRI(?s) && ?p != <{BETA}tag>) }} }} ORDER BY ?s"
            ),
        )
        .await;
        assert_eq!(
            ne,
            vec![json!([b("b1"), 2]), json!([b("b2"), 1])],
            "!= in {blk}"
        );
    }
}
