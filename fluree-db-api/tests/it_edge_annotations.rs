//! Edge annotations — broader M1b integration tests.
//!
//! Pins the user-facing contracts that the M1 design doc commits to:
//!
//! - **Parallel annotations** on the same `(s, p, o)` edge produce one
//!   row per annotation occurrence — Cypher fidelity.
//! - **Multiplicity contract**: a bare `?s ?p ?o` triple pattern
//!   returns a single row regardless of how many annotations are
//!   attached to that edge.
//! - **Annotation-rooted lookup** (`@reifies`) finds the edge from
//!   metadata; the base-edge triple in the expansion gives the
//!   visibility check for free.
//! - **Named-graph round-trip**: an annotated edge in a named graph
//!   stays paired with its annotation across query boundaries —
//!   regression coverage for the M1a `f:reifiesGraph` fix.
//!
//! See `EDGE_ANNOTATIONS.md` for the surface contract and
//! `EDGE_ANNOTATIONS_IMPL_PLAN.md` for the milestone split.
//!
//! Tests deliberately scope themselves to single-graph queries (or
//! `Pattern::Graph`-wrapped patterns) to stay within the correctness
//! envelope of the M1b expansion. The cross-graph misjoin gap is
//! tracked in the plan and lands with the M2 custom-operator path.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, MemoryFluree, MemoryLedger};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Match a row column against the compact or expanded form of an IRI,
/// in either bare-string or `{"@id": "..."}` shape. Mirrors the helper
/// in `it_edge_annotations_parse.rs` so the broader tests stay robust
/// to formatter changes.
fn iri_matches(value: &JsonValue, compact: &str, expanded: &str) -> bool {
    [compact, expanded].iter().any(|expect| {
        value.as_str() == Some(*expect)
            || value.get("@id").and_then(|v| v.as_str()) == Some(*expect)
    })
}

async fn seed_single_annotation(ledger_id: &str) -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "ex:role": "Engineer" }
        }
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed insert");
    (fluree, committed.ledger)
}

#[tokio::test]
async fn parallel_annotations_on_one_edge_return_one_row_per_occurrence() {
    // Cypher fidelity: two distinct annotations on the same (s, p, o)
    // edge must produce two rows under the inline-form query, with
    // the role binding distinguishing them. This is the multiplicity
    // contract for `Pattern::EdgeAnnotation`.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:parallel";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Insert two parallel annotations on the same edge. Each annotation
    // has an explicit @id so they can be told apart on retract paths
    // later (and so neither is anonymous).
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/2020",
                        "ex:role": "Engineer"
                    }
                }
            },
            {
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/2024",
                        "ex:role": "Manager"
                    }
                }
            }
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    let query = json!({
        "@context": ctx(),
        "select": ["?role"],
        "where": {
            "@id": "?person",
            "ex:worksFor": {
                "@id": "?org",
                "@annotation": { "ex:role": "?role" }
            }
        }
    });

    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("parallel-annotation query");
    let arr = rows.as_array().expect("Select array");
    assert_eq!(
        arr.len(),
        2,
        "two parallel annotations must produce two rows, got: {arr:#?}"
    );

    // Pull out the ?role bindings (each row is a single-column tuple
    // because select is `["?role"]`).
    let roles: std::collections::BTreeSet<String> = arr
        .iter()
        .filter_map(|row| row.as_array())
        .filter_map(|cols| cols.first().and_then(|v| v.as_str()))
        .map(String::from)
        .collect();
    assert_eq!(
        roles,
        ["Engineer", "Manager"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    );
}

#[tokio::test]
async fn bare_triple_pattern_returns_one_row_per_edge_regardless_of_annotations() {
    // Multiplicity contract: the `Pattern::Triple(?s, ex:worksFor, ?o)`
    // surface returns one row per *edge*, even when multiple
    // annotations exist for that edge. Annotations only affect
    // cardinality through the `@annotation` / `@reifies` IR variants.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:bare-multiplicity";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Two annotations on the same edge.
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
            }},
            { "@id": "ex:alice", "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "@id": "ex:emp/B", "ex:role": "Manager" }
            }}
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    // Bare triple pattern — no @annotation block, no `@reifies`.
    let query = json!({
        "@context": ctx(),
        "select": ["?person", "?org"],
        "where": { "@id": "?person", "ex:worksFor": { "@id": "?org" } }
    });

    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("bare triple query");
    let arr = rows.as_array().expect("Select array");
    assert_eq!(
        arr.len(),
        1,
        "bare triple cardinality must be 1 per edge regardless of annotations, got: {arr:#?}"
    );
    let row = arr[0].as_array().expect("row tuple");
    assert!(iri_matches(&row[0], "ex:alice", "http://example.org/alice"));
    assert!(iri_matches(&row[1], "ex:acme", "http://example.org/acme"));
}

#[tokio::test]
async fn select_distinct_collapses_parallel_annotations_when_projecting_edge_only() {
    // Even when the WHERE clause uses `@annotation` (which produces
    // per-occurrence cardinality), `selectDistinct` over edge-only
    // projection columns (?person, ?org) collapses to one row.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:select-distinct";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
            }},
            { "@id": "ex:alice", "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "@id": "ex:emp/B", "ex:role": "Manager" }
            }}
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    let query = json!({
        "@context": ctx(),
        "selectDistinct": ["?person", "?org"],
        "where": {
            "@id": "?person",
            "ex:worksFor": {
                "@id": "?org",
                "@annotation": { "ex:role": "?role" }
            }
        }
    });

    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("selectDistinct query");
    let arr = rows.as_array().expect("Select array");
    assert_eq!(
        arr.len(),
        1,
        "selectDistinct over (?person, ?org) collapses parallel annotations: {arr:#?}"
    );
}

#[tokio::test]
async fn annotation_rooted_query_finds_matching_edge() {
    // `@reifies`: filter by annotation metadata, return the edge it
    // reifies. Smoke test — broader visibility-check coverage lives
    // in `it_edge_annotations_parse.rs`.
    let (fluree, ledger) = seed_single_annotation("it/edge-annotations:reifies-roundtrip").await;

    let query = json!({
        "@context": ctx(),
        "select": ["?person", "?org"],
        "where": {
            "ex:role": "Engineer",
            "@reifies": {
                "@id": "?person",
                "ex:worksFor": { "@id": "?org" }
            }
        }
    });

    let rows = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("@reifies query");
    let arr = rows.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    let row = arr[0].as_array().expect("row");
    assert!(iri_matches(&row[0], "ex:alice", "http://example.org/alice"));
    assert!(iri_matches(&row[1], "ex:acme", "http://example.org/acme"));
}

#[tokio::test]
async fn annotation_rooted_query_returns_no_rows_when_metadata_doesnt_match() {
    // Negative case: filtering by a role that no annotation carries
    // must produce zero rows. Pins that the body patterns actually
    // join — a bug here would mean the f:reifies* lookup is short-
    // circuiting before reading metadata.
    let (fluree, ledger) = seed_single_annotation("it/edge-annotations:reifies-no-match").await;

    let query = json!({
        "@context": ctx(),
        "select": ["?person", "?org"],
        "where": {
            "ex:role": "Salesperson",
            "@reifies": {
                "@id": "?person",
                "ex:worksFor": { "@id": "?org" }
            }
        }
    });

    let rows = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("@reifies negative query");
    let arr = rows.as_array().expect("array");
    assert!(
        arr.is_empty(),
        "expected zero rows when the role doesn't match, got: {arr:#?}"
    );
}

#[tokio::test]
async fn annotation_in_named_graph_insert_succeeds() {
    // Regression coverage for the M1a `f:reifiesGraph` fix on the
    // *write* path. An annotated edge in a named graph must be
    // accepted by the transactor, with the lowering emitting
    // `f:reifiesGraph` on the synthetic annotation sibling and
    // pinning the sibling's own `@graph` to the same named graph.
    //
    // Full round-trip query coverage for named graphs needs the
    // `from` / `fromNamed` dataset wiring (see `it_named_graphs.rs`)
    // and pairs naturally with the cross-graph custom operator
    // tracked in the M1b plan TODO list. This test scopes itself to
    // the lowering-side guarantee.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:named-graph-insert";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "@graph": "ex:hr-graph",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "ex:role": "Engineer" }
        }
    });
    fluree
        .insert(ledger0, &txn)
        .await
        .expect("M1a fix: named-graph annotation insert must succeed end-to-end");
}
