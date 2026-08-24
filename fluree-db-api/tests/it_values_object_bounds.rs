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
