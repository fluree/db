//! Graph sync from Turtle, N-Triples and TriG text.
//!
//! The RDF payload takes the same delta path as JSON-LD sync: a graph written
//! in one format and re-synced in another must not commit. A TriG body's
//! blocks are unwrapped and read by the Turtle parser, so block contents get
//! the full Turtle grammar (`/upsert`'s block parser rejects `[ … ]`).

#![cfg(feature = "native")]

use crate::support::genesis_ledger;
use fluree_db_api::{FlureeBuilder, SyncGraphOpts, SyncGraphReport, TxnOpts};
use serde_json::{json, Value as JsonValue};

const ONT_IRI: &str = "http://example.org/graphs/ontology";
const OTHER_IRI: &str = "http://example.org/graphs/other";

const TTL_V1: &str = r#"
@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice" ; ex:role "engineer" .
ex:bob ex:name "Bob" .
"#;

/// v1 with alice's role changed, bob dropped and carol added.
const TTL_V2: &str = r#"
@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice" ; ex:role "manager" .
ex:carol ex:name "Carol" .
"#;

const NT_V1: &str = r#"
<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/alice> <http://example.org/role> "engineer" .
<http://example.org/bob> <http://example.org/name> "Bob" .
"#;

/// Literal shapes and labeled and anonymous blank nodes, written the same way
/// inside and outside a TriG block.
const MIXED_TRIPLES: &str = r#"
ex:widget ex:count 42 ;
    ex:ratio 1.50 ;
    ex:weight 2.5e0 ;
    ex:active true ;
    ex:label "Widget"@en ;
    ex:made "2024-01-15"^^xsd:date ;
    ex:sku "0042"^^xsd:string ;
    ex:big "12345678901234567890"^^xsd:integer ;
    ex:odd "opaque"^^ex:customType ;
    ex:partOf ex:assembly ;
    ex:owner _:o1 ;
    ex:spec [ ex:tolerance 0.01 ; ex:unit "mm" ] .
_:o1 ex:name "Owner" .
"#;

const PREFIXES: &str = "@prefix ex: <http://example.org/> .\n\
                        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";

fn mixed_turtle() -> String {
    format!("{PREFIXES}{MIXED_TRIPLES}")
}

fn mixed_trig(graph: &str) -> String {
    format!("{PREFIXES}GRAPH <{graph}> {{\n{MIXED_TRIPLES}}}\n")
}

/// Seed one default-graph triple and one triple in OTHER_IRI.
async fn seed(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> i64 {
    let ledger = genesis_ledger(fluree, ledger_id);
    let trig = format!(
        r#"
        @prefix ex: <http://example.org/> .
        ex:default-subject ex:p "default-graph-value" .
        GRAPH <{OTHER_IRI}> {{
            ex:zed ex:name "Zed" .
        }}
        "#,
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed insert")
        .receipt
        .t
}

async fn sync_rdf(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    text: &str,
    opts: SyncGraphOpts,
) -> fluree_db_api::Result<SyncGraphReport> {
    fluree
        .sync_named_graph_rdf_with(ledger_id, ONT_IRI, text, opts, TxnOpts::default(), None)
        .await
}

async fn rows_in_graph(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    graph_iri: Option<&str>,
) -> Vec<JsonValue> {
    let from = match graph_iri {
        Some(iri) => format!("{ledger_id}#{iri}"),
        None => ledger_id.to_string(),
    };
    let q = json!({
        "from": from,
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });
    let result = fluree.query_connection(&q).await.expect("query connection");
    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    rows.as_array().cloned().unwrap_or_default()
}

async fn count_in_graph(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    graph_iri: Option<&str>,
) -> usize {
    rows_in_graph(fluree, ledger_id, graph_iri).await.len()
}

fn assert_noop(report: &SyncGraphReport, what: &str) {
    assert!(
        !report.committed && report.asserted == 0 && report.retracted == 0,
        "{what} must not commit: {report:?}"
    );
}

#[tokio::test]
async fn turtle_sync_commits_only_the_delta() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-rdf/delta:main";
    let seed_t = seed(&fluree, ledger_id).await;

    let first = sync_rdf(&fluree, ledger_id, TTL_V1, SyncGraphOpts::default())
        .await
        .expect("first sync");
    assert_eq!((first.asserted, first.retracted), (3, 0), "{first:?}");
    assert!(first.committed);
    assert_eq!(first.t, seed_t + 1);

    let again = sync_rdf(&fluree, ledger_id, TTL_V1, SyncGraphOpts::default())
        .await
        .expect("identical resync");
    assert_noop(&again, "an identical Turtle resync");

    // N-Triples is Turtle's subset: the same triples spelled out are a no-op.
    let nt = sync_rdf(&fluree, ledger_id, NT_V1, SyncGraphOpts::default())
        .await
        .expect("N-Triples resync");
    assert_noop(&nt, "the same triples as N-Triples");

    // v1 → v2: role engineer→manager, bob removed, carol added. Alice's
    // unchanged name is not in the commit.
    let delta = sync_rdf(&fluree, ledger_id, TTL_V2, SyncGraphOpts::default())
        .await
        .expect("delta sync");
    assert_eq!((delta.asserted, delta.retracted), (2, 2), "{delta:?}");
    assert_eq!(delta.t, first.t + 1, "one commit for the whole delta");

    assert_eq!(count_in_graph(&fluree, ledger_id, Some(ONT_IRI)).await, 3);
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(OTHER_IRI)).await, 1);
    assert_eq!(count_in_graph(&fluree, ledger_id, None).await, 1);
}

#[tokio::test]
async fn turtle_and_jsonld_payloads_stage_the_same_flakes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-rdf/jsonld-parity:main";
    seed(&fluree, ledger_id).await;

    let jsonld = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice", "ex:role": "engineer" },
            { "@id": "ex:bob", "ex:name": "Bob" }
        ]
    });
    let first = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &jsonld, SyncGraphOpts::default())
        .await
        .expect("JSON-LD sync");
    assert!(first.committed);

    let turtle = sync_rdf(&fluree, ledger_id, TTL_V1, SyncGraphOpts::default())
        .await
        .expect("Turtle resync");
    assert_noop(&turtle, "the JSON-LD graph re-synced as Turtle");
}

#[tokio::test]
async fn trig_graph_block_and_turtle_stage_the_same_flakes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-rdf/trig-parity:main";
    seed(&fluree, ledger_id).await;

    let trig = mixed_trig(ONT_IRI);
    let first = sync_rdf(&fluree, ledger_id, &trig, SyncGraphOpts::default())
        .await
        .expect("TriG sync");
    assert!(first.committed, "{first:?}");
    let written = count_in_graph(&fluree, ledger_id, Some(ONT_IRI)).await;
    assert_eq!(written, 15, "every MIXED_TRIPLES triple lands in the graph");

    // Blank nodes in the block keep their identity across syncs.
    let again = sync_rdf(&fluree, ledger_id, &trig, SyncGraphOpts::default())
        .await
        .expect("TriG resync");
    assert_noop(&again, "an identical TriG resync");

    // The same triples outside a GRAPH block stage the same flakes, blank
    // nodes included.
    let turtle = sync_rdf(
        &fluree,
        ledger_id,
        &mixed_turtle(),
        SyncGraphOpts::default(),
    )
    .await
    .expect("Turtle resync");
    assert_noop(&turtle, "the TriG graph re-synced as Turtle");

    let back = sync_rdf(&fluree, ledger_id, &trig, SyncGraphOpts::default())
        .await
        .expect("TriG resync after Turtle");
    assert_noop(&back, "the Turtle graph re-synced as TriG");
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(OTHER_IRI)).await, 1);
}

#[tokio::test]
async fn trig_blocks_read_as_one_turtle_document() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-rdf/trig-blocks:main";
    seed(&fluree, ledger_id).await;

    // Two blocks for the graph (compact and keyword forms), the last triple
    // of each without its `.`, one blank-node label shared across both, and
    // a txn-meta block that annotates the commit rather than the graph.
    let trig = format!(
        "{PREFIXES}<{ONT_IRI}> {{ ex:alice ex:name \"Alice\" ; ex:knows _:k }}\n\
         GRAPH <#txn-meta> {{ <fluree:commit:this> ex:source \"tool-spec\" . }}\n\
         GRAPH <{ONT_IRI}> {{ _:k ex:name \"Kim\" . ex:alice ex:role \"engineer\" }}\n"
    );
    let first = sync_rdf(&fluree, ledger_id, &trig, SyncGraphOpts::default())
        .await
        .expect("multi-block TriG sync");
    assert_eq!((first.asserted, first.retracted), (4, 0), "{first:?}");

    let turtle = format!(
        "{PREFIXES}ex:alice ex:name \"Alice\" ; ex:knows _:k ; ex:role \"engineer\" .\n\
         _:k ex:name \"Kim\" .\n"
    );
    let same = sync_rdf(&fluree, ledger_id, &turtle, SyncGraphOpts::default())
        .await
        .expect("Turtle resync");
    assert_noop(
        &same,
        "the multi-block graph re-synced as one Turtle document",
    );

    let kim = rows_in_graph(&fluree, ledger_id, Some(ONT_IRI))
        .await
        .into_iter()
        .filter(|row| row[2] == "Kim")
        .count();
    assert_eq!(kim, 1, "the shared label is one node");
}

#[tokio::test]
async fn trig_block_annotations_and_collections_match_turtle() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-rdf/trig-star:main";
    seed(&fluree, ledger_id).await;

    let triples = "ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 |} .\n\
                   ex:alice ex:tags ( \"a\" \"b\" ) .\n";
    let trig = format!("{PREFIXES}GRAPH <{ONT_IRI}> {{\n{triples}}}\n");
    let first = sync_rdf(&fluree, ledger_id, &trig, SyncGraphOpts::default())
        .await
        .expect("TriG-star sync");
    assert!(first.committed, "{first:?}");

    let turtle = format!("{PREFIXES}{triples}");
    let same = sync_rdf(&fluree, ledger_id, &turtle, SyncGraphOpts::default())
        .await
        .expect("Turtle-star resync");
    assert_noop(&same, "the annotated TriG graph re-synced as Turtle");
}

#[tokio::test]
async fn trig_body_is_confined_to_the_target_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-rdf/trig-scope:main";
    seed(&fluree, ledger_id).await;
    let before = sync_rdf(&fluree, ledger_id, TTL_V1, SyncGraphOpts::default())
        .await
        .expect("sync");

    let other_block = mixed_trig(OTHER_IRI);
    let err = sync_rdf(&fluree, ledger_id, &other_block, SyncGraphOpts::default())
        .await
        .expect_err("a GRAPH block for another graph must be refused");
    assert!(err.to_string().contains(OTHER_IRI), "{err}");

    let both = format!(
        "{PREFIXES}ex:stray ex:p \"default\" .\nGRAPH <{ONT_IRI}> {{ ex:a ex:p \"in\" . }}\n"
    );
    let err = sync_rdf(&fluree, ledger_id, &both, SyncGraphOpts::default())
        .await
        .expect_err("default-graph triples beside the target block must be refused");
    assert!(err.to_string().contains("not both"), "{err}");

    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    assert_eq!(ledger.t(), before.t, "a refused sync commits nothing");
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(OTHER_IRI)).await, 1);
}

#[tokio::test]
async fn empty_rdf_payload_requires_allow_empty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-rdf/empty:main";
    seed(&fluree, ledger_id).await;
    sync_rdf(&fluree, ledger_id, TTL_V1, SyncGraphOpts::default())
        .await
        .expect("sync");

    let empty_block = format!("GRAPH <{ONT_IRI}> {{ }}\n");
    for empty in ["", PREFIXES, empty_block.as_str()] {
        let err = sync_rdf(&fluree, ledger_id, empty, SyncGraphOpts::default())
            .await
            .expect_err("an empty RDF payload must not clear the graph unasked");
        assert!(err.to_string().contains("allowEmpty"), "{empty:?}: {err}");
    }
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(ONT_IRI)).await, 3);

    let cleared = sync_rdf(
        &fluree,
        ledger_id,
        PREFIXES,
        SyncGraphOpts {
            allow_empty: true,
            ..Default::default()
        },
    )
    .await
    .expect("allowEmpty clears");
    assert_eq!((cleared.asserted, cleared.retracted), (0, 3), "{cleared:?}");
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(ONT_IRI)).await, 0);
    assert_eq!(count_in_graph(&fluree, ledger_id, Some(OTHER_IRI)).await, 1);
}

#[tokio::test]
async fn rdf_dry_run_reports_without_committing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-rdf/dry-run:main";
    seed(&fluree, ledger_id).await;
    let first = sync_rdf(&fluree, ledger_id, TTL_V1, SyncGraphOpts::default())
        .await
        .expect("sync");

    let dry = sync_rdf(
        &fluree,
        ledger_id,
        TTL_V2,
        SyncGraphOpts {
            dry_run: true,
            ..Default::default()
        },
    )
    .await
    .expect("dry run");
    assert_eq!((dry.asserted, dry.retracted), (2, 2), "{dry:?}");
    assert!(dry.dry_run && !dry.committed);
    assert_eq!(dry.t, first.t);
    assert_eq!(fluree.ledger(ledger_id).await.unwrap().t(), first.t);
}
