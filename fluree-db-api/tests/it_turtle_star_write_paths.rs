//! Turtle-star on the JSON-LD-converted write paths (upsert, graph sync).
//!
//! `insert_turtle` streams Turtle straight into the transaction sink, which
//! has accepted RDF 1.2 reifiers (`~ r`, `{| … |}`, `<< s p o >>`) since the
//! edge-annotation M1 work. `upsert_turtle`, `sync_named_graph` and the
//! memory importer instead convert Turtle to JSON-LD first
//! (`fluree_graph_turtle::parse_to_json`), and that conversion used to
//! refuse every star construct with "not supported on this ingest path".
//!
//! These tests pin that the converted path now yields the same reifier
//! bundles as a direct insert, that the annotations are queryable on every
//! query surface, and that the upsert semantics (replace the body, keep the
//! edge) hold for annotated edges.

use crate::support::{self, genesis_ledger};
use fluree_db_api::{FlureeBuilder, SyncGraphOpts};
use serde_json::{json, Value as JsonValue};

const PREFIXES: &str = "@prefix ex: <http://example.org/> .\n\
                        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

fn with_prefixes(turtle: &str) -> String {
    format!("{PREFIXES}{turtle}")
}

/// Match a row column against the compact or expanded form of an IRI, in
/// either bare-string or `{"@id": "..."}` shape.
fn iri_matches(value: &JsonValue, compact: &str, expanded: &str) -> bool {
    [compact, expanded].iter().any(|expect| {
        value.as_str() == Some(*expect)
            || value.get("@id").and_then(|v| v.as_str()) == Some(*expect)
    })
}

fn rows(result: &JsonValue) -> &Vec<JsonValue> {
    result
        .as_array()
        .unwrap_or_else(|| panic!("expected a row array, got {result:#}"))
}

/// `SELECT ?conf` over the inline SPARQL 1.2 annotation surface.
async fn confidences(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    graph: Option<&str>,
) -> Vec<String> {
    let pattern = "ex:alice ex:knows ex:bob {| ex:confidence ?conf |}";
    let body = match graph {
        Some(g) => format!("GRAPH <{g}> {{ {pattern} }}"),
        None => pattern.to_string(),
    };
    let sparql = format!(
        "PREFIX ex: <http://example.org/>\n\
         SELECT ?conf WHERE {{ {body} }} ORDER BY ?conf"
    );
    let result = support::query_sparql_formatted(fluree, ledger, &sparql)
        .await
        .expect("annotation query");
    rows(&result)
        .iter()
        .map(|row| {
            let cell = row.as_array().and_then(|r| r.first()).unwrap_or(row);
            match cell {
                JsonValue::String(s) => s.clone(),
                JsonValue::Number(n) => n.to_string(),
                other => other
                    .get("@value")
                    .map(|v| match v {
                        JsonValue::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_else(|| other.to_string()),
            }
        })
        .collect()
}

#[tokio::test]
async fn upsert_turtle_accepts_named_reifier_and_annotation_is_queryable() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/turtle-star-upsert:named");

    let turtle = with_prefixes(
        "ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 ; ex:source \"hr\" |} .\n",
    );
    let committed = fluree
        .upsert_turtle(ledger0, &turtle)
        .await
        .expect("upsert_turtle must accept Turtle-star");
    let ledger = &committed.ledger;

    // Inline JSON-LD surface: the claim id and its body come back together.
    let query = json!({
        "@context": ctx(),
        "select": ["?o", "?claim", "?source"],
        "where": {
            "@id": "ex:alice",
            "ex:knows": {
                "@id": "?o",
                "@annotation": { "@id": "?claim", "ex:source": "?source" }
            }
        }
    });
    let result = support::query_jsonld_formatted(&fluree, ledger, &query)
        .await
        .expect("inline annotation query");
    let arr = rows(&result);
    assert_eq!(arr.len(), 1, "one annotated edge, got: {arr:#?}");
    let row = arr[0].as_array().expect("row");
    assert!(
        iri_matches(&row[0], "ex:bob", "http://example.org/bob"),
        "{row:?}"
    );
    assert!(
        iri_matches(&row[1], "ex:claim1", "http://example.org/claim1"),
        "named reifier must be the annotation subject verbatim: {row:?}"
    );
    assert_eq!(row[2], "hr");

    // Inline SPARQL 1.2 surface.
    assert_eq!(confidences(&fluree, ledger, None).await, ["0.9"]);

    // Claim-first surface: walk from the reifier to the edge it reifies.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                  SELECT ?s ?o WHERE { ex:claim1 rdf:reifies <<( ?s ex:knows ?o )>> }";
    let result = support::query_sparql_formatted(&fluree, ledger, sparql)
        .await
        .expect("claim-first query");
    let arr = rows(&result);
    assert_eq!(arr.len(), 1, "{arr:#?}");
    let row = arr[0].as_array().expect("row");
    assert!(iri_matches(&row[0], "ex:alice", "http://example.org/alice"));
    assert!(iri_matches(&row[1], "ex:bob", "http://example.org/bob"));
}

#[tokio::test]
async fn upsert_turtle_replaces_the_annotation_body_on_re_upsert() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/turtle-star-upsert:replace");

    let first = fluree
        .upsert_turtle(
            ledger0,
            &with_prefixes("ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 |} .\n"),
        )
        .await
        .expect("first upsert");
    assert_eq!(confidences(&fluree, &first.ledger, None).await, ["0.9"]);

    // Upsert semantics on the claim: the body predicate is replaced, not
    // accumulated, and the edge + its reifier attachment survive.
    let second = fluree
        .upsert_turtle(
            first.ledger,
            &with_prefixes("ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.95 |} .\n"),
        )
        .await
        .expect("second upsert");
    assert_eq!(confidences(&fluree, &second.ledger, None).await, ["0.95"]);
}

#[tokio::test]
async fn upsert_turtle_and_insert_turtle_agree_on_the_claim_graph() {
    let turtle = with_prefixes(
        "ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 ; ex:source \"hr\" |} .\n\
         ex:alice ex:knows ex:carol {| ex:source \"linkedin\" |} .\n\
         ex:alice ex:age 42 ~ ex:claim2 .\n",
    );
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                  SELECT ?s ?p ?o ?src WHERE {\n\
                    ?r rdf:reifies <<( ?s ?p ?o )>> .\n\
                    OPTIONAL { ?r ex:source ?src }\n\
                  } ORDER BY ?p ?o ?src";

    let fluree = FlureeBuilder::memory().build_memory();
    let inserted = fluree
        .insert_turtle(
            genesis_ledger(&fluree, "it/turtle-star-parity:insert"),
            &turtle,
        )
        .await
        .expect("insert_turtle");
    let upserted = fluree
        .upsert_turtle(
            genesis_ledger(&fluree, "it/turtle-star-parity:upsert"),
            &turtle,
        )
        .await
        .expect("upsert_turtle");

    let via_insert = support::query_sparql_formatted(&fluree, &inserted.ledger, sparql)
        .await
        .expect("query insert ledger");
    let via_upsert = support::query_sparql_formatted(&fluree, &upserted.ledger, sparql)
        .await
        .expect("query upsert ledger");
    assert_eq!(rows(&via_insert).len(), 3, "{via_insert:#}");
    assert_eq!(
        via_insert, via_upsert,
        "the converted path must reify the same edges with the same bodies as a direct insert"
    );
}

#[tokio::test]
async fn annotated_type_edge_is_accepted_by_insert_and_refused_by_upsert() {
    let fluree = FlureeBuilder::memory().build_memory();
    let turtle = with_prefixes("ex:alice a ex:Person {| ex:source \"hr\" |} ; a ex:Employee .\n");

    // The direct Turtle path reifies the type edge like any other edge.
    let committed = fluree
        .insert_turtle(
            genesis_ledger(&fluree, "it/turtle-star-type-edge:insert"),
            &turtle,
        )
        .await
        .expect("insert_turtle with an annotated rdf:type edge");
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  SELECT ?src WHERE { ex:alice a ex:Person {| ex:source ?src |} }";
    let result = support::query_sparql_formatted(&fluree, &committed.ledger, sparql)
        .await
        .expect("annotated type query");
    let arr = rows(&result);
    assert_eq!(arr.len(), 1, "{arr:#?}");
    let cell = arr[0].as_array().and_then(|r| r.first()).unwrap_or(&arr[0]);
    assert_eq!(cell, "hr");

    // The JSON-LD-converted path has no `@annotation` home on a `@type`
    // value: refuse with a message that names the shape and the way out,
    // instead of dropping the claim or failing deep in the parser.
    let err = fluree
        .upsert_turtle(
            genesis_ledger(&fluree, "it/turtle-star-type-edge:upsert"),
            &turtle,
        )
        .await
        .expect_err("upsert_turtle must refuse an annotated rdf:type edge");
    let msg = err.to_string();
    assert!(msg.contains("rdf:type") && msg.contains("insert"), "{msg}");
}

#[tokio::test]
async fn one_named_reifier_on_two_edges_is_rejected_on_every_turtle_path() {
    // A reifier denotes exactly one edge. Reusing an explicit reifier on
    // two different triples in one document would store a bundle that
    // `EdgeKey::from_reifies_facts` rejects, so every reader silently drops
    // BOTH annotations. Fail loud at write time instead.
    let turtle = with_prefixes(
        "ex:alice ex:knows ex:bob ~ ex:claim1 .\n\
         ex:alice ex:knows ex:carol ~ ex:claim1 .\n",
    );

    let fluree = FlureeBuilder::memory().build_memory();
    let err = fluree
        .insert_turtle(
            genesis_ledger(&fluree, "it/turtle-star-reuse:insert"),
            &turtle,
        )
        .await
        .expect_err("insert_turtle must reject a reifier reused on two edges");
    let msg = err.to_string();
    assert!(msg.contains("claim1"), "must name the reifier: {msg}");

    let err = fluree
        .upsert_turtle(
            genesis_ledger(&fluree, "it/turtle-star-reuse:upsert"),
            &turtle,
        )
        .await
        .expect_err("upsert_turtle must reject a reifier reused on two edges");
    let msg = err.to_string();
    assert!(msg.contains("claim1"), "must name the reifier: {msg}");
}

#[tokio::test]
async fn sync_named_graph_accepts_turtle_star_payload() {
    const GRAPH: &str = "http://example.org/graphs/claims";
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/turtle-star-sync:claims";
    // Sync addresses a ledger by id, so it has to exist: seed one
    // default-graph triple (the way `fluree graph sync` finds a ledger the
    // user created earlier).
    fluree
        .insert_turtle(
            genesis_ledger(&fluree, ledger_id),
            &with_prefixes("ex:alice ex:name \"Alice\" .\n"),
        )
        .await
        .expect("seed ledger");

    // `fluree sync` converts Turtle client-side with exactly this call. Two
    // claims on one edge: the shape a claims file has in practice.
    let payload = fluree_graph_turtle::parse_to_json(&with_prefixes(
        "ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 ; ex:source ex:hr |} .\n\
         ex:alice ex:knows ex:bob ~ ex:claim2 {| ex:confidence 0.7 ; ex:source ex:crm |} .\n\
         ex:bob ex:knows ex:carol ~ ex:claim3 {| ex:confidence 0.5 |} .\n",
    ))
    .expect("Turtle-star converts to a sync payload");

    let report = fluree
        .sync_named_graph(ledger_id, GRAPH, &payload, SyncGraphOpts::default())
        .await
        .expect("sync with an annotated edge");
    assert!(report.committed, "{report:?}");
    assert!(report.asserted > 0, "{report:?}");

    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    assert_eq!(
        confidences(&fluree, &ledger, Some(GRAPH)).await,
        ["0.7", "0.9"]
    );

    // Syncing the same payload again is a no-op: the named reifiers and
    // their bundles are stable across conversions.
    let again = fluree
        .sync_named_graph(ledger_id, GRAPH, &payload, SyncGraphOpts::default())
        .await
        .expect("re-sync");
    if again.committed {
        let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
        let flakes = flakes_at(&ledger, again.t).await;
        panic!(
            "identical payload must not commit: {again:?}\nflakes at t={}: {flakes:#?}",
            again.t
        );
    }
    assert_eq!((again.asserted, again.retracted), (0, 0), "{again:?}");
}

#[tokio::test]
async fn named_graph_annotation_survives_indexing_and_re_sync() {
    // Sync's own contract across an index boundary: an annotated payload
    // syncs, gets indexed, still reads back from the index, and an unchanged
    // re-sync is still a no-op rather than a churn commit.
    //
    // Sync's retraction wave cancels the unchanged reifies facts, so an
    // identical re-sync asserts nothing for the single-target invariant to
    // inspect. The re-point at the end of this test does reach it. The
    // invariant's own indexed-named-graph case is also pinned by
    // `it_edge_annotations_indexed::indexed_named_graph_annotation_stays_writable`,
    // which reaches it through a plain JSON-LD write.
    const GRAPH: &str = "http://example.org/graphs/claims";
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/turtle-star-sync:indexed-named-graph";
    fluree
        .insert_turtle(
            genesis_ledger(&fluree, ledger_id),
            &with_prefixes("ex:alice ex:name \"Alice\" .\n"),
        )
        .await
        .expect("seed ledger");

    let payload = fluree_graph_turtle::parse_to_json(&with_prefixes(
        "ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 |} .\n",
    ))
    .expect("Turtle-star converts");
    let report = fluree
        .sync_named_graph(ledger_id, GRAPH, &payload, SyncGraphOpts::default())
        .await
        .expect("first sync");
    assert!(report.committed, "{report:?}");

    // Move the bundle out of novelty and into the index.
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("reload indexed ledger");
    assert_eq!(
        confidences(&fluree, &ledger, Some(GRAPH)).await,
        ["0.9"],
        "the annotation must still read back from the index"
    );

    // Re-sync the identical payload against the indexed bundle.
    let again = fluree
        .sync_named_graph(ledger_id, GRAPH, &payload, SyncGraphOpts::default())
        .await
        .expect("an unchanged re-sync of an INDEXED named-graph annotation must be accepted");
    assert!(
        !again.committed,
        "identical payload must still be a no-op after indexing: {again:?}"
    );

    // Re-pointing the same reifier at a different edge goes through: sync's
    // delta retracts the old attachment in the same transaction that asserts
    // the new one, which is exactly what the single-target invariant asks for.
    // What the invariant refuses is a reifier on *two* edges at once, pinned
    // by `one_named_reifier_on_two_edges_is_rejected_on_every_turtle_path`.
    let repointed = fluree_graph_turtle::parse_to_json(&with_prefixes(
        "ex:alice ex:knows ex:carol ~ ex:claim1 {| ex:confidence 0.9 |} .\n",
    ))
    .expect("Turtle-star converts");
    let moved = fluree
        .sync_named_graph(ledger_id, GRAPH, &repointed, SyncGraphOpts::default())
        .await
        .expect("re-pointing an indexed reifier through sync");
    assert!(moved.committed, "{moved:?}");

    let ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("reload after re-point");
    let sparql = format!(
        "PREFIX ex: <http://example.org/>\n\
         SELECT ?o WHERE {{ GRAPH <{GRAPH}> {{ ex:alice ex:knows ?o ~ ex:claim1 }} }}"
    );
    let result = support::query_sparql(&fluree, &ledger, &sparql)
        .await
        .expect("re-pointed claim query");
    let json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    let objects: Vec<String> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| b["o"]["value"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        objects,
        ["http://example.org/carol"],
        "the claim must describe exactly the edge the payload gives it"
    );
}

/// Every novelty flake written at exactly `t`, for failure diagnostics.
async fn flakes_at(ledger: &fluree_db_api::LedgerState, t: i64) -> Vec<String> {
    use fluree_db_core::comparator::IndexType;
    ledger
        .novelty
        .iter_flakes(IndexType::Spot)
        .filter(|f| f.t == t)
        .map(|f| {
            format!(
                "g={:?} {} {} {:?} dt={} op={}",
                f.g, f.s, f.p, f.o, f.dt, f.op
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Ordinary edits to annotated data, on the delta-computing write paths.
//
// Sync and upsert both work out an exact delta before staging: sync diffs the
// payload against the graph's current contents, upsert retracts what it is
// about to replace. Both therefore *do* retract a prior attachment in the same
// transaction, which is exactly what the single-target invariant's own error
// message tells a user to do. These pin that ordinary edits — dropping a line,
// changing an edge's object, re-pointing a claim — go through.
// ---------------------------------------------------------------------------

const CLAIMS_GRAPH: &str = "http://example.org/graphs/claims";

/// `SELECT ?s ?o ?conf` over every annotated `ex:knows` edge in the graph.
async fn annotated_knows(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> Vec<(String, String, String)> {
    let sparql = format!(
        "PREFIX ex: <http://example.org/>\n\
         SELECT ?s ?o ?conf WHERE {{ GRAPH <{CLAIMS_GRAPH}> \
         {{ ?s ex:knows ?o {{| ex:confidence ?conf |}} }} }} ORDER BY ?s ?o"
    );
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let result = support::query_sparql(fluree, &ledger, &sparql)
        .await
        .expect("annotated-knows query");
    let json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| {
            let get = |k: &str| {
                b[k]["value"]
                    .as_str()
                    .unwrap_or_default()
                    .rsplit('/')
                    .next()
                    .unwrap_or_default()
                    .to_string()
            };
            (get("s"), get("o"), get("conf"))
        })
        .collect()
}

async fn sync(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    turtle: &str,
) -> std::result::Result<bool, String> {
    let payload =
        fluree_graph_turtle::parse_to_json(&with_prefixes(turtle)).expect("Turtle-star converts");
    fluree
        .sync_named_graph(ledger_id, CLAIMS_GRAPH, &payload, SyncGraphOpts::default())
        .await
        .map(|r| r.committed)
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn re_syncing_without_a_claims_line_drops_just_that_claim() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/turtle-star-sync:drop-a-line";
    fluree
        .insert_turtle(
            genesis_ledger(&fluree, ledger_id),
            &with_prefixes("ex:alice ex:name \"Alice\" .\n"),
        )
        .await
        .expect("seed ledger");

    assert!(sync(
        &fluree,
        ledger_id,
        "ex:alice ex:knows ex:bob {| ex:confidence 0.9 |} .\n\
         ex:bob ex:knows ex:carol {| ex:confidence 0.5 |} .\n",
    )
    .await
    .expect("first sync"));
    assert_eq!(
        annotated_knows(&fluree, ledger_id).await,
        [
            ("alice".into(), "bob".into(), "0.9".into()),
            ("bob".into(), "carol".into(), "0.5".into())
        ]
    );

    // Drop the first line. Sync's delta retracts that edge and its whole
    // bundle, and leaves the second line untouched.
    assert!(sync(
        &fluree,
        ledger_id,
        "ex:bob ex:knows ex:carol {| ex:confidence 0.5 |} .\n",
    )
    .await
    .expect("re-sync without the first line"));
    assert_eq!(
        annotated_knows(&fluree, ledger_id).await,
        [("bob".into(), "carol".into(), "0.5".into())],
        "dropping a line must drop exactly that claim"
    );
}

#[tokio::test]
async fn re_syncing_an_anonymous_annotated_edge_can_change_its_object() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/turtle-star-sync:change-object";
    fluree
        .insert_turtle(
            genesis_ledger(&fluree, ledger_id),
            &with_prefixes("ex:alice ex:name \"Alice\" .\n"),
        )
        .await
        .expect("seed ledger");

    assert!(sync(
        &fluree,
        ledger_id,
        "ex:alice ex:knows ex:bob {| ex:confidence 0.9 |} .\n",
    )
    .await
    .expect("first sync"));

    assert!(sync(
        &fluree,
        ledger_id,
        "ex:alice ex:knows ex:carol {| ex:confidence 0.9 |} .\n",
    )
    .await
    .expect("re-sync with a different object"));
    assert_eq!(
        annotated_knows(&fluree, ledger_id).await,
        [("alice".into(), "carol".into(), "0.9".into())],
        "the edge and its claim must both follow the payload"
    );
}

#[tokio::test]
async fn upserting_a_named_claim_can_change_the_fact_it_describes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/turtle-star-upsert:repoint");

    let first = fluree
        .upsert_turtle(ledger0, &with_prefixes("ex:alice ex:age 42 ~ ex:c1 .\n"))
        .await
        .expect("first upsert");

    // Upsert replaces `ex:age` on `ex:alice`, so the old edge is retracted in
    // the same transaction that asserts the new one. The claim follows it.
    let second = fluree
        .upsert_turtle(
            first.ledger,
            &with_prefixes("ex:alice ex:age 43 ~ ex:c1 .\n"),
        )
        .await
        .expect("re-pointing a claim through upsert");

    let sparql = "PREFIX ex: <http://example.org/>\n\
                  SELECT ?age WHERE { ex:alice ex:age ?age ~ ex:c1 }";
    let result = support::query_sparql(&fluree, &second.ledger, sparql)
        .await
        .expect("claim query");
    let json = result
        .to_sparql_json(&second.ledger.snapshot)
        .expect("sparql json");
    let ages: Vec<String> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| b["age"]["value"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(ages, ["43"], "the claim must describe the new fact");
}

#[tokio::test]
async fn re_upserting_an_anonymous_annotation_replaces_it_rather_than_duplicating() {
    // The named-reifier twin of this is
    // `upsert_turtle_replaces_the_annotation_body_on_re_upsert`. An anonymous
    // `{| … |}` has no author-supplied identity, so "replace the body, keep
    // the edge" has to hold on an identity the write path derives itself.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/turtle-star-upsert:anon-replace");
    let turtle = with_prefixes("ex:alice ex:knows ex:bob {| ex:confidence 0.9 |} .\n");

    let first = fluree
        .upsert_turtle(ledger0, &turtle)
        .await
        .expect("first upsert");
    assert_eq!(confidences(&fluree, &first.ledger, None).await, ["0.9"]);

    let second = fluree
        .upsert_turtle(first.ledger, &turtle)
        .await
        .expect("identical re-upsert");
    assert_eq!(
        confidences(&fluree, &second.ledger, None).await,
        ["0.9"],
        "re-upserting the same anonymous annotation must not accumulate claims"
    );

    // The boundary, stated deliberately. An anonymous claim has no identity
    // its author can refer to, so upsert cannot know that a claim with a new
    // body is meant to *be* the old one. Changing the body therefore adds a
    // second claim about the same edge rather than replacing the first. Name
    // the reifier when replacement is what you want — that case is
    // `upsert_turtle_replaces_the_annotation_body_on_re_upsert`.
    let third = fluree
        .upsert_turtle(
            second.ledger,
            &with_prefixes("ex:alice ex:knows ex:bob {| ex:confidence 0.95 |} .\n"),
        )
        .await
        .expect("re-upsert with a different body");
    let mut got = confidences(&fluree, &third.ledger, None).await;
    got.sort();
    assert_eq!(
        got,
        ["0.9", "0.95"],
        "a differently-bodied anonymous claim is a new claim, not a replacement"
    );
}

#[tokio::test]
async fn re_upserting_parallel_annotations_is_a_no_op() {
    // Two claims about one edge. The Turtle-to-JSON-LD adapter states the base
    // edge once per claim, so the accumulator sees it twice; an identical
    // re-upsert must still net to nothing rather than committing the surplus.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/turtle-star-upsert:parallel-noop");
    let turtle = with_prefixes(
        "ex:alice ex:knows ex:bob ~ ex:c1 {| ex:confidence 0.9 |} \
         ~ ex:c2 {| ex:confidence 0.7 |} .\n",
    );

    let first = fluree
        .upsert_turtle(ledger0, &turtle)
        .await
        .expect("first upsert");
    let t_after_first = first.ledger.t();

    let second = fluree
        .upsert_turtle(first.ledger, &turtle)
        .await
        .expect("identical re-upsert");
    assert_eq!(
        second.ledger.t(),
        t_after_first,
        "an identical payload must not produce a commit"
    );
}
