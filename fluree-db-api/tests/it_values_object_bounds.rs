//! Regression tests for BUG-values-join-planner.
//!
//! Two bugs, one report:
//!
//! 1. A `VALUES` clause binding an object variable of a fused star was
//!    deferred to a post-join `ValuesOperator`, so it constrained nothing
//!    about the scan: the star drained the whole predicate extent
//!    (materializing every row, including wide payloads) and the tiny VALUES
//!    filtered afterwards — measured 1,600x slower than the equivalent
//!    `FILTER ?v IN (...)` on a 129k-edge two-bound-endpoint join. Such
//!    VALUES now lower to membership filters (see
//!    `convert_star_values_to_membership_filters` in fluree-db-query; the
//!    plan-shape pins live in that crate's unit tests). The tests here pin
//!    the SEMANTICS the conversion must preserve: identical rows to the
//!    filter form, duplicate-row multiplicity, and UNDEF match-any.
//!
//! 2. `FILTER(?v IN (<iri> ...))` matched NOTHING against index-encoded
//!    bindings: `eval_in` compared an `EncodedSid`/`Sid` row value against a
//!    constant `Iri` through `rdf_term_equal`, whose Resource arm compares
//!    representations, not resources. (`NOT IN` dually kept everything.)
//!    `IN`/`NOT IN` now route through the same resource fast path as `=`.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{assert_index_defaults, genesis_ledger, rebuild_and_publish_index};

const LEDGER_ID: &str = "values-object-bounds:main";
const N: usize = 1500;

fn iri(n: usize) -> String {
    format!("http://example.org/item/PRT-{n:06}")
}

fn entity_a() -> String {
    iri(900_001)
}
fn entity_b() -> String {
    iri(900_002)
}
fn entity_c() -> String {
    iri(900_003)
}

/// N match-evidence edges. Edge 0 links (A, B), edge 1 links (A, C); the
/// rest link a disjoint pool. Every edge carries a wide `ns:snap` payload so
/// an unconstrained star scan materializes real weight.
async fn seed(fluree: &fluree_db_api::Fluree) {
    let ledger0 = genesis_ledger(fluree, LEDGER_ID);
    let mut graph = Vec::with_capacity(N);
    for i in 0..N {
        let (a, b) = match i {
            0 => (entity_a(), entity_b()),
            1 => (entity_a(), entity_c()),
            _ => (iri(i * 2), iri(i * 2 + 1)),
        };
        graph.push(json!({
            "@id": format!("http://example.org/edge/{i}"),
            "ns:entity1": { "@id": a },
            "ns:entity2": { "@id": b },
            "ns:snap": format!("snapshot payload {i} {}", "x".repeat(64)),
        }));
    }
    fluree
        .insert(
            ledger0,
            &json!({ "@context": { "ns": "http://example.org/ns#" }, "@graph": graph }),
        )
        .await
        .expect("insert seed data");
    rebuild_and_publish_index(fluree, LEDGER_ID).await;
}

/// Run a SPARQL query, returning its bindings rows.
async fn run(fluree: &fluree_db_api::Fluree, sparql: &str) -> Vec<JsonValue> {
    let result = fluree
        .query_from()
        .sparql(sparql)
        .track_all()
        .execute_tracked()
        .await
        .expect("query should succeed");
    assert_eq!(result.status, 200);
    result.result["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .clone()
}

fn sorted(mut rows: Vec<JsonValue>) -> Vec<JsonValue> {
    rows.sort_by_key(std::string::ToString::to_string);
    rows
}

/// The report's core claim: binding both endpoints of an edge with two
/// VALUES clauses must return exactly what the `FILTER ?b IN (...)`
/// workaround returns.
#[tokio::test]
async fn two_values_and_filter_in_return_identical_rows() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;
    let (a, b, c) = (entity_a(), entity_b(), entity_c());

    let values_shape = format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?a ?b ?snap FROM <{LEDGER_ID}> WHERE {{\n\
           VALUES ?a {{ <{a}> }}\n\
           VALUES ?b {{ <{b}> <{c}> }}\n\
           ?ev ns:entity1 ?a ; ns:entity2 ?b ; ns:snap ?snap\n\
         }}"
    );
    let filter_shape = format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?a ?b ?snap FROM <{LEDGER_ID}> WHERE {{\n\
           VALUES ?a {{ <{a}> }}\n\
           ?ev ns:entity1 ?a ; ns:entity2 ?b ; ns:snap ?snap .\n\
           FILTER(?b IN (<{b}>, <{c}>))\n\
         }}"
    );

    let values_rows = sorted(run(&fluree, &values_shape).await);
    let filter_rows = sorted(run(&fluree, &filter_shape).await);

    assert_eq!(values_rows.len(), 2, "edges 0 and 1 match");
    assert_eq!(
        values_rows, filter_rows,
        "two-VALUES and VALUES+FILTER-IN must return identical rows"
    );
}

/// Duplicate VALUES rows multiply solutions (SPARQL join semantics). The
/// membership-filter conversion must decline this shape, not dedup it.
#[tokio::test]
async fn duplicate_values_rows_multiply_solutions() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;
    let (a, b) = (entity_a(), entity_b());

    let q = format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?a ?b FROM <{LEDGER_ID}> WHERE {{\n\
           VALUES ?a {{ <{a}> }}\n\
           VALUES ?b {{ <{b}> <{b}> }}\n\
           ?ev ns:entity1 ?a ; ns:entity2 ?b ; ns:snap ?snap\n\
         }}"
    );
    let rows = run(&fluree, &q).await;
    assert_eq!(
        rows.len(),
        2,
        "edge 0 joins BOTH duplicate rows: multiplicity 2, not a deduped 1"
    );
    assert_eq!(rows[0], rows[1]);
}

/// An UNDEF cell joins as match-any. The conversion must decline this shape.
#[tokio::test]
async fn undef_values_row_matches_any() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;
    let (a, b) = (entity_a(), entity_b());

    let q = format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?b FROM <{LEDGER_ID}> WHERE {{\n\
           VALUES ?a {{ <{a}> }}\n\
           VALUES ?b {{ <{b}> UNDEF }}\n\
           ?ev ns:entity1 ?a ; ns:entity2 ?b ; ns:snap ?snap\n\
         }}"
    );
    let rows = run(&fluree, &q).await;
    // Edge 0 (b=B) matches the <B> row AND the UNDEF row; edge 1 (b=C)
    // matches only the UNDEF row.
    assert_eq!(
        rows.len(),
        3,
        "UNDEF joins every edge; <B> joins edge 0 again"
    );
}

/// `FILTER(?v IN (<iri>...))` must match index-encoded ref bindings — one
/// element, several elements, and in subject position.
#[tokio::test]
async fn filter_in_matches_encoded_iri_bindings() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;
    let (b, c) = (entity_b(), entity_c());

    let one = format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?ev FROM <{LEDGER_ID}> WHERE {{\n\
           ?ev ns:entity2 ?b . FILTER(?b IN (<{b}>)) }}"
    );
    assert_eq!(run(&fluree, &one).await.len(), 1, "single-element IN");

    let two = format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?ev FROM <{LEDGER_ID}> WHERE {{\n\
           ?ev ns:entity2 ?b . FILTER(?b IN (<{b}>, <{c}>)) }}"
    );
    assert_eq!(run(&fluree, &two).await.len(), 2, "two-element IN");

    let absent = format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?ev FROM <{LEDGER_ID}> WHERE {{\n\
           ?ev ns:entity2 ?b . FILTER(?b IN (<http://example.org/item/none>)) }}"
    );
    assert_eq!(
        run(&fluree, &absent).await.len(),
        0,
        "absent IRI matches nothing"
    );

    let subject = format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?ev FROM <{LEDGER_ID}> WHERE {{\n\
           ?ev ns:entity2 ?b . FILTER(?ev IN (<http://example.org/edge/0>)) }}"
    );
    assert_eq!(run(&fluree, &subject).await.len(), 1, "subject-position IN");
}

/// `NOT IN` is the dual: it must EXCLUDE the listed resources (pre-fix it
/// kept everything, because no element ever compared equal).
#[tokio::test]
async fn filter_not_in_excludes_encoded_iri_bindings() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;
    let (a, b) = (entity_a(), entity_b());

    let q = format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?ev ?b FROM <{LEDGER_ID}> WHERE {{\n\
           VALUES ?a {{ <{a}> }}\n\
           ?ev ns:entity1 ?a ; ns:entity2 ?b .\n\
           FILTER(?b NOT IN (<{b}>))\n\
         }}"
    );
    let rows = run(&fluree, &q).await;
    assert_eq!(
        rows.len(),
        1,
        "edge 0 (b = <B>) must be excluded, edge 1 kept"
    );
    assert_eq!(rows[0]["b"]["value"], entity_c());
}

/// Row-drain work must be visible to fuel accounting. Leaflet touches are
/// thousands of rows coarse, so before scan-emission and VALUES-join charges
/// existed, a query that drained and materialized a whole predicate extent
/// reported floor-level fuel (~1.0) — invisible to `max_fuel` limits.
#[tokio::test]
async fn drained_rows_are_visible_to_fuel() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;
    let b = entity_b();

    let fuel_of = |q: String| {
        let fluree = &fluree;
        async move {
            let result = fluree
                .query_from()
                .sparql(&q)
                .track_all()
                .execute_tracked()
                .await
                .expect("query should succeed");
            assert_eq!(result.status, 200);
            result.fuel.expect("fuel")
        }
    };

    // A star that emits every subject: N rows through the scan lane.
    let star_fuel = fuel_of(format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?a ?b FROM <{LEDGER_ID}> WHERE {{\n\
           ?ev ns:entity1 ?a ; ns:entity2 ?b ; ns:snap ?snap }}"
    ))
    .await;
    assert!(
        star_fuel > 2.0,
        "star drained ~{N} rows but charged only {star_fuel} fuel; \
         scan-emission charges regressed and the lane is invisible to max_fuel"
    );
    assert!(
        star_fuel < 100.0,
        "star fuel ({star_fuel}) blew past the per-row schedule"
    );

    // A declined (UNDEF) VALUES joins the full stream through ValuesOperator:
    // its per-input-row charge stacks on the scan-emission charge.
    let values_join_fuel = fuel_of(format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?b FROM <{LEDGER_ID}> WHERE {{\n\
           VALUES ?b {{ <{b}> UNDEF }}\n\
           ?ev ns:entity1 ?a ; ns:entity2 ?b ; ns:snap ?snap }}"
    ))
    .await;
    assert!(
        values_join_fuel > star_fuel + 1.0,
        "VALUES join over the same stream charged {values_join_fuel} vs bare star \
         {star_fuel}; the ValuesOperator per-input-row charge regressed"
    );
}

/// Coverage for the property-join operator's own row lanes — the batched
/// subject probe and the SPOT star walk — which bypass
/// `BinaryScanOperator` entirely, so the scan-emission charge never sees
/// their rows. An object-anchored star routes through them: this drives
/// 800 rows through the SPOT lane and pins that the query stays inside a
/// sane fuel envelope.
///
/// NOT a pin on the per-row charges themselves. Their contribution here is
/// 0.80 of 6.61 fuel (800 rows x `PER_ROW_MICRO_FUEL`), and the IO touch
/// charges over the same leaflets dominate it — so deleting both charges
/// leaves any end-to-end assertion on this query green. Isolating them
/// needs two shapes with identical IO and different row counts, which this
/// index layout cannot produce. Verified by hand instead: with the charges
/// removed the hub query reports 5.81 fuel, with them 6.61.
#[tokio::test]
async fn property_join_probe_lanes_stay_in_a_sane_fuel_envelope() {
    const HUB_LEDGER: &str = "values-object-bounds-hub:main";
    const HUB_EDGES: usize = 400;

    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // A hub entity on `ns:entity1` with many edges, so the object-anchored
    // star expands real volume through the property join rather than the
    // two rows the shared fixture's hub carries.
    let ledger0 = genesis_ledger(&fluree, HUB_LEDGER);
    let hub = entity_a();
    let mut graph = Vec::with_capacity(HUB_EDGES);
    for i in 0..HUB_EDGES {
        graph.push(json!({
            "@id": format!("http://example.org/hub-edge/{i}"),
            "ns:entity1": { "@id": hub },
            "ns:entity2": { "@id": iri(i) },
            "ns:snap": format!("snapshot payload {i} {}", "x".repeat(64)),
        }));
    }
    fluree
        .insert(
            ledger0,
            &json!({ "@context": { "ns": "http://example.org/ns#" }, "@graph": graph }),
        )
        .await
        .expect("insert hub data");
    rebuild_and_publish_index(&fluree, HUB_LEDGER).await;

    // A singleton VALUES on the object folds into the triple and anchors
    // the star, which is what routes it through the probe/SPOT lanes.
    let sparql = format!(
        "PREFIX ns: <http://example.org/ns#>\n\
         SELECT ?b FROM <{HUB_LEDGER}> WHERE {{\n\
           VALUES ?a {{ <{hub}> }}\n\
           ?ev ns:entity1 ?a ; ns:entity2 ?b ; ns:snap ?snap }}"
    );
    let result = fluree
        .query_from()
        .sparql(&sparql)
        .track_all()
        .execute_tracked()
        .await
        .expect("query should succeed");
    assert_eq!(result.status, 200);
    let rows = result.result["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .len();
    assert_eq!(rows, HUB_EDGES, "the hub star must expand every edge");

    let fuel = result.fuel.expect("fuel");
    assert!(
        fuel > 2.0,
        "expanding a {HUB_EDGES}-edge hub charged only {fuel} fuel; the \
         lane reports floor-level cost and is invisible to max_fuel"
    );
    assert!(
        fuel < 100.0,
        "hub star fuel ({fuel}) blew past the per-row schedule"
    );
}

/// A nested-loop join reads leaflets itself, through `scan_matches` and the
/// object-driven flush, so its probe rows cross none of the scan-operator
/// charging surfaces. Before those lanes were charged, a star whose subject
/// was seeded by `VALUES` expanded its whole probe side for free: 400
/// subjects returned 400 rows and reported 1.001 fuel — the query floor and
/// nothing else, invisible to `max_fuel`.
///
/// Pins the charge by proportionality rather than a magic total: the same
/// query shape over 4 seeded subjects and over 400 must differ by roughly
/// the row count, which only a per-row charge on that lane can supply.
#[tokio::test]
async fn nested_loop_join_probe_rows_are_visible_to_fuel() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;

    let fuel_of = |n: usize| {
        let fluree = &fluree;
        async move {
            let subjects = (0..n)
                .map(|i| format!("<http://example.org/edge/{i}>"))
                .collect::<Vec<_>>()
                .join(" ");
            let result = fluree
                .query_from()
                .sparql(&format!(
                    "PREFIX ns: <http://example.org/ns#>\n\
                     SELECT ?b FROM <{LEDGER_ID}> WHERE {{\n\
                       VALUES ?ev {{ {subjects} }}\n\
                       ?ev ns:entity1 ?a ; ns:entity2 ?b ; ns:snap ?snap }}"
                ))
                .track_all()
                .execute_tracked()
                .await
                .expect("query should succeed");
            assert_eq!(result.status, 200);
            let rows = result.result["results"]["bindings"]
                .as_array()
                .expect("bindings")
                .len();
            assert_eq!(rows, n, "every seeded subject must expand");
            result.fuel.expect("fuel")
        }
    };

    let few = fuel_of(4).await;
    let many = fuel_of(400).await;

    // 400 subjects x 3 predicates = 1200 probe rows at PER_ROW_MICRO_FUEL,
    // so the gap is ~1.2 fuel. Assert well inside that so the pin survives
    // unrelated schedule tuning, but far enough above the 4-subject case
    // that only a per-row charge on this lane can produce it.
    assert!(
        many > few + 0.5,
        "expanding 400 seeded subjects charged {many} fuel vs {few} for 4; \
         the nested-loop-join probe lanes are uncharged again and the whole \
         probe side is invisible to max_fuel"
    );
}

/// The `IN`-over-resources bug was never SPARQL-specific: JSON-LD `filter`
/// expressions lower to the same IR and run through the same `eval_in`, so
/// the encoded-vs-IRI mismatch dropped every row there too. Twin of
/// `filter_in_matches_encoded_iri_bindings`, per the SPARQL/JSON-LD parity
/// rule for shared-IR fixes.
#[tokio::test]
async fn jsonld_filter_in_matches_encoded_iri_bindings() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;
    let (b, c) = (entity_b(), entity_c());

    let db = fluree.db(LEDGER_ID).await.expect("db");
    let rows_for = |expr: String| {
        let fluree = &fluree;
        let db = &db;
        async move {
            let q = json!({
                "@context": { "ns": "http://example.org/ns#" },
                "select": ["?ev"],
                "where": [
                    { "@id": "?ev", "ns:entity2": "?b" },
                    ["filter", expr]
                ]
            });
            fluree
                .query(db, &q)
                .await
                .expect("query")
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .expect("format")
                .as_array()
                .expect("rows array")
                .len()
        }
    };

    assert_eq!(
        rows_for(format!("(in ?b [(iri \"{b}\")])")).await,
        1,
        "single-element IN over an encoded ref binding"
    );
    assert_eq!(
        rows_for(format!("(in ?b [(iri \"{b}\") (iri \"{c}\")])")).await,
        2,
        "two-element IN over encoded ref bindings"
    );
    assert_eq!(
        rows_for("(in ?b [(iri \"http://example.org/item/none\")])".to_string()).await,
        0,
        "absent IRI matches nothing"
    );
}
