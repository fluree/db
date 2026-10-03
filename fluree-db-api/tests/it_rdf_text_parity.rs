//! RDF text is parsed once, by the Turtle parser, on every write lane.
//!
//! Upsert read Turtle and TriG through a JSON-LD round trip: the text was
//! parsed into a graph, re-encoded as JSON-LD and expanded again. The round
//! trip refused IRIs whose scheme the compact-IRI guard took for an
//! undefined prefix (`tag:`, `kb:`; #1977), stored collections as unordered
//! values, and parsed literals more strictly than insert. Upsert now stages
//! from the parse itself, so it stores what insert stores.

use crate::support::{genesis_ledger, rebuild_and_publish_index, MemoryFluree};
use fluree_db_api::policy_builder::build_policy_context_from_opts;
use fluree_db_api::{
    CommitOpts, Fluree, FlureeBuilder, GovernanceOptions, GraphDb, GraphPayload, LedgerState,
    PolicyContext, TrackingOptions,
};
use fluree_db_core::comparator::IndexType;
use fluree_db_core::{
    range_with_overlay, ContentStore as _, Flake, FlakeValue, RangeMatch, RangeOptions, RangeTest,
    Sid,
};
use fluree_db_transact::{GraphSel, TxnOpts};
use serde_json::{json, Value as JsonValue};

const TAG: &str = "tag:example.org,2025:";
const KB: &str = "kb:n#";

fn memory() -> MemoryFluree {
    FlureeBuilder::memory().build_memory()
}

// =============================================================================
// Reading back what a lane stored
// =============================================================================

/// A node as written in the document: its IRI, or `_:label` for a blank
/// node, whatever skolem scope the lane minted it under
/// (`fdb-<scope>-<label>` for a streamed insert, `fdb-<scope>-<n>-<label>`
/// for templates).
fn node(ledger: &LedgerState, sid: &Sid) -> String {
    let iri = ledger
        .snapshot
        .decode_sid(sid)
        .unwrap_or_else(|| format!("?{}", sid.name));
    let Some(at) = iri.find("fdb-") else {
        return iri;
    };
    let rest = &iri[at + 4..];
    let rest = rest.split_once('-').map_or(rest, |(_scope, rest)| rest);
    let label = match rest.split_once('-') {
        Some((n, label)) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => label,
        _ => rest,
    };
    format!("_:{label}")
}

fn render(ledger: &LedgerState, f: &Flake) -> String {
    let o = match &f.o {
        FlakeValue::Ref(sid) => node(ledger, sid),
        other => format!("{other:?}"),
    };
    let dt = ledger.snapshot.decode_sid(&f.dt).unwrap_or_default();
    let lang =
        f.m.as_ref()
            .and_then(|m| m.lang.as_deref())
            .map(|l| format!("@{l}"))
            .unwrap_or_default();
    let i =
        f.m.as_ref()
            .and_then(|m| m.i)
            .map(|i| format!("#{i}"))
            .unwrap_or_default();
    format!(
        "{} {} {o} ^^{dt}{lang}{i}",
        node(ledger, &f.s),
        node(ledger, &f.p)
    )
}

/// Every current fact of `graph` (`None`: the default graph), one line
/// each, sorted. Read at the flake level, so datatypes, language tags and
/// list positions are all visible.
async fn facts(ledger: &LedgerState, graph: Option<&str>) -> Vec<String> {
    let g_id = match graph {
        None => 0,
        Some(iri) => match ledger.snapshot.graph_registry.graph_id_for_iri(iri) {
            Some(g) => g,
            None => return Vec::new(),
        },
    };
    let flakes = range_with_overlay(
        &ledger.snapshot,
        g_id,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::new(),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .expect("range");
    let mut out: Vec<String> = flakes
        .iter()
        .filter(|f| f.op)
        .map(|f| render(ledger, f))
        .collect();
    out.sort();
    out
}

/// Rows of a SPARQL SELECT, each cell its IRI or lexical form, sorted.
async fn select(fluree: &Fluree, ledger: &LedgerState, sparql: &str) -> Vec<Vec<String>> {
    let db = GraphDb::from_ledger_state(ledger);
    let result = fluree.query(&db, sparql).await.expect("query");
    let formatted = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format");
    let cell = |v: &JsonValue| match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Object(o) => o
            .get("@id")
            .or_else(|| o.get("@value"))
            .map(|x| {
                x.as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| x.to_string())
            })
            .unwrap_or_else(|| v.to_string()),
        other => other.to_string(),
    };
    let mut rows: Vec<Vec<String>> = formatted
        .as_array()
        .expect("row array")
        .iter()
        .map(|row| match row.as_array() {
            Some(cells) => cells.iter().map(cell).collect(),
            None => vec![cell(row)],
        })
        .collect();
    rows.sort();
    rows
}

fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(ToString::to_string).collect()
}

// =============================================================================
// IRIs the JSON-LD round trip refused (#1977)
// =============================================================================

/// `tag:` and `kb:` IRIs in every position: subject, predicate, object,
/// class, datatype, and a block's graph label; written in full and as
/// prefixed names.
fn iri_doc() -> String {
    format!(
        "@prefix t: <{TAG}> .\n\
         @prefix k: <{KB}> .\n\
         <{TAG}s> <{TAG}p> <{KB}o> ;\n\
             a <{KB}Class> ;\n\
             <{KB}q> \"v\"^^<{TAG}dt> .\n\
         k:s2 t:p t:o .\n\
         GRAPH <{TAG}g> {{ k:s3 t:p \"w\" . }}\n"
    )
}

/// What [`iri_doc`] stores, as `[graph, s, p, o, datatype]`.
fn iri_rows() -> Vec<Vec<String>> {
    let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let xsd_string = "http://www.w3.org/2001/XMLSchema#string";
    let s = format!("{TAG}s");
    let p = format!("{TAG}p");
    let mut rows = vec![
        row(&["", &s, &p, &format!("{KB}o"), ""]),
        row(&["", &s, rdf_type, &format!("{KB}Class"), ""]),
        row(&["", &s, &format!("{KB}q"), "v", &format!("{TAG}dt")]),
        row(&["", &format!("{KB}s2"), &p, &format!("{TAG}o"), ""]),
        row(&[&format!("{TAG}g"), &format!("{KB}s3"), &p, "w", xsd_string]),
    ];
    rows.sort();
    rows
}

/// Every triple as `[graph, s, p, o, datatype]`; `graph` is empty for the
/// default graph and `datatype` for IRI objects.
async fn iri_quads(fluree: &Fluree, ledger: &LedgerState) -> Vec<Vec<String>> {
    let body = "?s ?p ?o BIND(IF(isLiteral(?o), STR(DATATYPE(?o)), \"\") AS ?dt)";
    let mut rows: Vec<Vec<String>> = select(
        fluree,
        ledger,
        &format!("SELECT ?s ?p ?o ?dt WHERE {{ {body} }}"),
    )
    .await
    .into_iter()
    .map(|r| [vec![String::new()], r].concat())
    .collect();
    rows.extend(
        select(
            fluree,
            ledger,
            &format!("SELECT ?g ?s ?p ?o ?dt WHERE {{ GRAPH ?g {{ {body} }} }}"),
        )
        .await,
    );
    rows.sort();
    rows
}

/// A non-root policy context that allows every write but one, so the
/// policy-gated staging path runs.
async fn allow_most(ledger: &LedgerState) -> PolicyContext {
    let opts = GovernanceOptions {
        policy: Some(json!([{
            "@id": "http://example.org/denySsn",
            "f:required": true,
            "f:onProperty": [{"@id": "http://example.org/ssn"}],
            "f:action": "f:modify",
            "f:allow": false
        }])),
        default_allow: Some(true),
        ..Default::default()
    };
    build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
        &[0],
    )
    .await
    .expect("build policy context")
}

/// Every upsert entry point, and every insert entry point for a document
/// with graph blocks, stores `tag:`/`kb:` IRIs verbatim. The JSON-LD round
/// trip refused each of them with "Unresolved compact IRI".
#[tokio::test]
async fn every_rdf_text_lane_stores_iris_the_json_ld_round_trip_refused() {
    let doc = iri_doc();
    let expected = iri_rows();
    let fluree = memory();
    let mut failures = Vec::new();

    let seed = "@prefix ex: <http://example.org/> .\nex:seed ex:ssn \"0\" .\n";
    for lane in [
        "Fluree::upsert_turtle",
        "Fluree::upsert_turtle_with_opts",
        "owned builder",
        "cached handle",
        "cached handle + policy",
        "graph builder",
        "insert: Fluree::insert_turtle",
        "insert: owned builder",
        "insert: cached handle",
        "insert: cached handle + policy",
        "insert: graph builder",
    ] {
        let id = format!(
            "it/rdf-iri-{}:main",
            lane.replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        );
        fluree.create_ledger(&id).await.expect("create");
        fluree
            .graph(&id)
            .transact()
            .insert_turtle(seed)
            .commit()
            .await
            .expect("seed");
        let ledger = fluree.ledger(&id).await.expect("load");
        let outcome = match lane {
            "Fluree::upsert_turtle" => fluree.upsert_turtle(ledger, &doc).await.map(|_| ()),
            "Fluree::upsert_turtle_with_opts" => fluree
                .upsert_turtle_with_opts(
                    ledger,
                    &doc,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &fluree_db_api::server_defaults::default_index_config(),
                )
                .await
                .map(|_| ()),
            "owned builder" => fluree
                .stage_owned(ledger)
                .upsert_turtle(&doc)
                .execute()
                .await
                .map(|_| ()),
            "cached handle" => {
                let handle = fluree.ledger_cached(&id).await.expect("handle");
                fluree
                    .stage(&handle)
                    .upsert_turtle(&doc)
                    .execute()
                    .await
                    .map(|_| ())
            }
            "cached handle + policy" => {
                let policy = allow_most(&ledger).await;
                let handle = fluree.ledger_cached(&id).await.expect("handle");
                fluree
                    .stage(&handle)
                    .upsert_turtle(&doc)
                    .policy(policy)
                    .execute()
                    .await
                    .map(|_| ())
            }
            "graph builder" => fluree
                .graph(&id)
                .transact()
                .upsert_turtle(&doc)
                .commit()
                .await
                .map(|_| ()),
            "insert: Fluree::insert_turtle" => fluree.insert_turtle(ledger, &doc).await.map(|_| ()),
            "insert: owned builder" => fluree
                .stage_owned(ledger)
                .insert_turtle(&doc)
                .execute()
                .await
                .map(|_| ()),
            "insert: cached handle" => {
                let handle = fluree.ledger_cached(&id).await.expect("handle");
                fluree
                    .stage(&handle)
                    .insert_turtle(&doc)
                    .execute()
                    .await
                    .map(|_| ())
            }
            "insert: cached handle + policy" => {
                let policy = allow_most(&ledger).await;
                let handle = fluree.ledger_cached(&id).await.expect("handle");
                fluree
                    .stage(&handle)
                    .insert_turtle(&doc)
                    .policy(policy)
                    .execute()
                    .await
                    .map(|_| ())
            }
            "insert: graph builder" => fluree
                .graph(&id)
                .transact()
                .insert_turtle(&doc)
                .commit()
                .await
                .map(|_| ()),
            other => unreachable!("{other}"),
        };
        if let Err(e) = outcome {
            failures.push(format!("{lane}: {e}"));
            continue;
        }
        let ledger = fluree.ledger(&id).await.expect("reload");
        let mut got = iri_quads(&fluree, &ledger).await;
        got.retain(|r| !r[1].starts_with("http://example.org/"));
        if got != expected {
            failures.push(format!("{lane}: {got:#?}"));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Graph sync and graph insert of RDF text (the `/sync` and graph store
/// routes) store `tag:`/`kb:` IRIs verbatim, from a plain Turtle body and
/// from a TriG block naming the target. Both converted the body to JSON-LD
/// and refused them.
#[tokio::test]
async fn graph_payload_lanes_store_iris_the_json_ld_round_trip_refused() {
    let fluree = memory();
    let target = format!("{TAG}g");
    let statements = format!(
        "<{TAG}s> <{TAG}p> <{KB}o> ; a <{KB}Class> ; <{KB}q> \"v\"^^<{TAG}dt> .\n\
         <{KB}s2> <{TAG}p> <{TAG}o> .\n"
    );
    let block = format!("GRAPH <{target}> {{\n{statements}}}\n");
    let default_rows: Vec<Vec<String>> =
        iri_rows().into_iter().filter(|r| r[0].is_empty()).collect();
    let named_rows: Vec<Vec<String>> = default_rows
        .iter()
        .map(|r| [vec![target.clone()], r[1..].to_vec()].concat())
        .collect();
    let mut failures = Vec::new();
    for (lane, graph, body, expected) in [
        (
            "sync default graph",
            GraphSel::Default,
            &statements,
            &default_rows,
        ),
        (
            "sync named graph, Turtle body",
            GraphSel::Graph(target.clone()),
            &statements,
            &named_rows,
        ),
        (
            "sync named graph, TriG block",
            GraphSel::Graph(target.clone()),
            &block,
            &named_rows,
        ),
        (
            "graph insert, TriG block",
            GraphSel::Graph(target.clone()),
            &block,
            &named_rows,
        ),
    ] {
        let id = format!(
            "it/rdf-graph-iri-{}:main",
            lane.replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        );
        let builder = fluree.stage_owned(genesis_ledger(&fluree, &id));
        let builder = if lane.starts_with("sync") {
            builder.sync_graph_payload(graph, GraphPayload::Rdf(body), false)
        } else {
            builder.insert_graph_payload(graph, GraphPayload::Rdf(body))
        };
        match builder.execute().await {
            Ok(result) => {
                let got = iri_quads(&fluree, &result.ledger).await;
                if got != *expected {
                    failures.push(format!("{lane}: {got:#?}"));
                }
            }
            Err(e) => failures.push(format!("{lane}: {e}")),
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// A collection synced into a graph keeps its order and repeats: the sync
/// body was converted to JSON-LD, which stored a collection's entries as
/// unordered values.
#[tokio::test]
async fn a_synced_collection_keeps_its_order() {
    let fluree = memory();
    let g = "http://example.org/g";
    let body = "@prefix ex: <http://example.org/> .\nex:s ex:items ( \"b\" \"a\" \"b\" ) .\n";
    let result = fluree
        .stage_owned(genesis_ledger(&fluree, "it/rdf-sync-list:main"))
        .sync_graph_payload(
            GraphSel::Graph(g.to_string()),
            GraphPayload::Rdf(body),
            false,
        )
        .execute()
        .await
        .expect("sync");
    let xsd = "^^http://www.w3.org/2001/XMLSchema#string";
    assert_eq!(
        facts(&result.ledger, Some(g)).await,
        vec![
            format!("http://example.org/s http://example.org/items String(\"a\") {xsd}#1"),
            format!("http://example.org/s http://example.org/items String(\"b\") {xsd}#0"),
            format!("http://example.org/s http://example.org/items String(\"b\") {xsd}#2"),
        ]
    );
}

/// Sync scopes blank nodes to the target graph, and the parse labels them
/// as the JSON-LD conversion did, so a graph last synced through that
/// conversion re-syncs from the same text without a commit: its blank
/// nodes are the same stored nodes.
#[tokio::test]
#[allow(clippy::disallowed_methods)] // the conversion the RDF lane replaced
async fn a_graph_synced_through_json_ld_resyncs_from_the_same_text_as_a_no_op() {
    let fluree = memory();
    let id = "it/rdf-sync-identity:main";
    let g = "http://example.org/g";
    let doc = "@prefix ex: <http://example.org/> .\n\
               @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\
               ex:w ex:owner _:o1 ; ex:spec [ ex:unit \"mm\" ; ex:tol 0.01 ] ;\n\
                    ex:label \"Widget\"@en ; ex:made \"2024-01-15\"^^xsd:date .\n\
               _:o1 ex:name \"Owner\" .\n";
    let nodes = fluree_graph_turtle::parse_to_json(doc).expect("convert");
    let converted = json!({ "@graph": nodes });
    let first = fluree
        .stage_owned(genesis_ledger(&fluree, id))
        .sync_graph_payload(
            GraphSel::Graph(g.to_string()),
            GraphPayload::JsonLd(&converted),
            false,
        )
        .execute()
        .await
        .expect("sync through JSON-LD");
    assert!(first.receipt.flake_count > 0);
    let again = fluree
        .stage_owned(first.ledger)
        .sync_graph_payload(
            GraphSel::Graph(g.to_string()),
            GraphPayload::Rdf(doc),
            false,
        )
        .execute()
        .await
        .expect("sync from text");
    assert_eq!(
        (again.receipt.flake_count, again.receipt.t),
        (0, first.receipt.t),
        "the same graph from the text must not commit"
    );
}

// =============================================================================
// Upsert stores what insert stores
// =============================================================================

/// Collections with repeats, nested collections, `rdf:type` with blank-node
/// and literal objects, an ill-typed literal (kept, as insert keeps it), big
/// and exact numbers, JSON and language-tagged literals, and annotations,
/// on an edge and on `rdf:type`. Special float values are left out of this
/// fixture; they are tracked separately.
const PARITY_DOC: &str = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
ex:s ex:list ( "b" "a" "b" ) ;
     ex:nested ( ( 1 2 ) "x" ) ;
     a _:c , [ ex:k "anon" ] , "a literal type" ;
     ex:bad "abc"^^xsd:integer ;
     ex:int 42 ;
     ex:big 123456789012345678901234567890 ;
     ex:dec 1.50 ;
     ex:dbl 1.5e0 ;
     ex:json "{\"a\":1}"^^rdf:JSON ;
     ex:lang "hello"@en , "bonjour"@fr ;
     ex:bool true ;
     ex:date "2025-01-01"^^xsd:date .
_:c ex:label "c" .
ex:s ex:knows ex:o ~ ex:r {| ex:src "wiki" |} .
ex:s a ex:T {| ex:conf 0.9 |} .
"#;

/// One document, inserted by the streaming parser, upserted, and synced
/// into the default graph of fresh ledgers, stores the same facts: same
/// values, datatypes, language tags, list positions and annotation
/// bundles, blank nodes by label.
#[tokio::test]
async fn upsert_and_sync_store_what_insert_stores() {
    let fluree = memory();
    let inserted = fluree
        .insert_turtle(
            genesis_ledger(&fluree, "it/rdf-parity-insert:main"),
            PARITY_DOC,
        )
        .await
        .expect("insert")
        .ledger;
    let upserted = fluree
        .upsert_turtle(
            genesis_ledger(&fluree, "it/rdf-parity-upsert:main"),
            PARITY_DOC,
        )
        .await
        .expect("upsert")
        .ledger;
    let synced = fluree
        .stage_owned(genesis_ledger(&fluree, "it/rdf-parity-sync:main"))
        .sync_graph_payload(GraphSel::Default, GraphPayload::Rdf(PARITY_DOC), false)
        .execute()
        .await
        .expect("sync")
        .ledger;
    let expected = facts(&inserted, None).await;
    assert!(
        expected
            .iter()
            .any(|f| f.contains("list") && f.ends_with("#2")),
        "the fixture must hold a list: {expected:#?}"
    );
    assert_eq!(facts(&upserted, None).await, expected, "upsert");
    assert_eq!(facts(&synced, None).await, expected, "sync");
}

/// A collection upserted over another replaces it entry by entry, in order.
/// The round trip stored a collection as unordered values, so a Turtle
/// list upsert read back as a set.
#[tokio::test]
async fn a_turtle_list_upsert_replaces_the_list_in_order() {
    let fluree = memory();
    let ledger = fluree
        .insert_turtle(
            genesis_ledger(&fluree, "it/rdf-list-upsert:main"),
            "@prefix ex: <http://example.org/> .\nex:s ex:items ( \"a\" \"b\" ) .\n",
        )
        .await
        .expect("seed")
        .ledger;
    let result = fluree
        .upsert_turtle(
            ledger,
            "@prefix ex: <http://example.org/> .\nex:s ex:items ( \"x\" \"y\" \"x\" ) .\n",
        )
        .await
        .expect("upsert");
    assert_eq!(
        (result.receipt.retract_count, result.receipt.assert_count),
        (2, 3)
    );
    let xsd = "^^http://www.w3.org/2001/XMLSchema#string";
    assert_eq!(
        facts(&result.ledger, None).await,
        vec![
            format!("http://example.org/s http://example.org/items String(\"x\") {xsd}#0"),
            format!("http://example.org/s http://example.org/items String(\"x\") {xsd}#2"),
            format!("http://example.org/s http://example.org/items String(\"y\") {xsd}#1"),
        ]
    );
}

/// The ledger-config recipe (`docs/ledger-config/writing-config.md`)
/// upserts as written: its settings are anonymous blank nodes inside the
/// config graph's block, which the TriG block reader refused (#1511).
#[tokio::test]
async fn the_config_recipe_upserts_as_written() {
    let fluree = memory();
    let id = "it/rdf-config:main";
    let config = format!("urn:fluree:{id}#config");
    let recipe = format!(
        "@prefix f: <https://ns.flur.ee/db#> .\n\
         GRAPH <{config}> {{\n\
           <urn:fluree:{id}:config:ledger> a f:LedgerConfig ;\n\
             f:reasoningDefaults [ f:reasoningModes f:RDFS ] .\n\
         }}\n"
    );
    let result = fluree
        .upsert_turtle(genesis_ledger(&fluree, id), &recipe)
        .await
        .expect("the recipe upserts");
    let stored = facts(&result.ledger, Some(&config)).await;
    assert!(
        stored
            .iter()
            .any(|f| f.contains("reasoningModes https://ns.flur.ee/db#RDFS")),
        "{stored:#?}"
    );
}

/// A ledger id used as an IRI reference inside a GRAPH block, with no `@base`
/// in force, is stored exactly as written on upsert, TriG insert and sync:
/// cross-ledger configuration names its model ledger this way
/// (`f:ledger <org/governance:main>`, a relative reference). A relative block
/// label is kept as written too (a sync target must be an absolute IRI, so
/// that case runs on upsert and insert). The same statement in the default
/// graph still needs a base, and a base in force still resolves both.
#[tokio::test]
async fn a_ledger_id_in_a_graph_block_is_stored_as_written() {
    let fluree = memory();
    let statement = "<urn:cfg:src> <https://ns.flur.ee/db#ledger> <org/governance:main> .";
    let cases: [(&str, &[&str]); 2] = [
        ("http://example.org/cfg", &["upsert", "insert", "sync"]),
        ("cfg/local", &["upsert", "insert"]),
    ];
    for (i, (g, lanes)) in cases.into_iter().enumerate() {
        let block = format!("GRAPH <{g}> {{ {statement} }}\n");
        for &lane in lanes {
            let ledger = genesis_ledger(&fluree, &format!("it/rdf-ledger-id-{lane}-{i}:main"));
            let result = match lane {
                "upsert" => fluree.upsert_turtle(ledger, &block).await.map(|r| r.ledger),
                "insert" => fluree.insert_turtle(ledger, &block).await.map(|r| r.ledger),
                _ => fluree
                    .stage_owned(ledger)
                    .sync_graph_payload(
                        GraphSel::Graph(g.to_string()),
                        GraphPayload::Rdf(&block),
                        false,
                    )
                    .execute()
                    .await
                    .map(|r| r.ledger),
            }
            .unwrap_or_else(|e| panic!("{lane} <{g}>: {e}"));
            let stored = facts(&result, Some(g)).await;
            assert!(
                stored
                    .iter()
                    .any(|f| f.contains("#ledger org/governance:main ")),
                "{lane} <{g}>: {stored:#?}"
            );
        }
    }

    let err = fluree
        .upsert_turtle(
            genesis_ledger(&fluree, "it/rdf-ledger-id-default:main"),
            statement,
        )
        .await
        .map(|_| ())
        .expect_err("a default-graph relative reference needs a base");
    assert!(
        matches!(err, fluree_db_api::ApiError::Turtle(_)) && err.to_string().contains("relative"),
        "{err:?}"
    );

    let based = fluree
        .upsert_turtle(
            genesis_ledger(&fluree, "it/rdf-ledger-id-based:main"),
            &format!("@base <http://b.org/> .\nGRAPH <cfg/local> {{ {statement} }}\n"),
        )
        .await
        .expect("upsert under a base")
        .ledger;
    let stored = facts(&based, Some("http://b.org/cfg/local")).await;
    assert!(
        stored
            .iter()
            .any(|f| f.contains("#ledger http://b.org/org/governance:main ")),
        "{stored:#?}"
    );
}

// =============================================================================
// Identity: the same document addresses the same blank nodes
// =============================================================================

const IDEMPOTENT_DOC: &str = r#"
@prefix ex: <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
ex:A owl:equivalentClass [ owl:unionOf ( ex:B ex:C ex:B ) ] .
ex:A ex:label "A"@en , "Ah"@fr .
ex:A ex:note "plain" .
ex:A ex:rel ex:B ~ ex:r1 {| ex:src "wiki" |} .
GRAPH <http://example.org/g> { ex:A ex:part [ ex:k "v" ] . }
"#;

/// Upserting the same document again commits nothing: its blank nodes
/// (restrictions, collections, annotations, in a block too) resolve to the
/// same stored nodes, and every value it names is already stored as it
/// names it. Reordering statements keeps that; editing one value does not.
#[tokio::test]
async fn upserting_the_same_document_again_commits_nothing() {
    let fluree = memory();
    let first = fluree
        .upsert_turtle(
            genesis_ledger(&fluree, "it/rdf-idempotent:main"),
            IDEMPOTENT_DOC,
        )
        .await
        .expect("first upsert");
    assert!(first.receipt.flake_count > 0);
    let t = first.receipt.t;

    let again = fluree
        .upsert_turtle(first.ledger, IDEMPOTENT_DOC)
        .await
        .expect("second upsert");
    assert_eq!(
        (again.receipt.flake_count, again.receipt.t),
        (0, t),
        "an identical re-upsert must not commit"
    );

    // Plain statements moved: same content, same blank nodes.
    let reordered = IDEMPOTENT_DOC.replace(
        "ex:A ex:label \"A\"@en , \"Ah\"@fr .\nex:A ex:note \"plain\" .\n",
        "ex:A ex:note \"plain\" .\nex:A ex:label \"Ah\"@fr , \"A\"@en .\n",
    );
    assert_ne!(reordered, IDEMPOTENT_DOC);
    let reordered = fluree
        .upsert_turtle(again.ledger, &reordered)
        .await
        .expect("reordered upsert");
    assert_eq!(
        (reordered.receipt.flake_count, reordered.receipt.t),
        (0, t),
        "statement order is not content"
    );

    let edited = IDEMPOTENT_DOC.replace("\"plain\"", "\"edited\"");
    let edited = fluree
        .upsert_turtle(reordered.ledger, &edited)
        .await
        .expect("edited upsert");
    assert!(edited.receipt.t > t, "an edited document commits");
    assert!(facts(&edited.ledger, None)
        .await
        .iter()
        .any(|f| f.contains("note String(\"edited\")")));
}

/// `turtle.md`'s query for blank-node subjects of the default graph that
/// nothing references, in any graph.
const ORPHANS_DEFAULT: &str = "SELECT DISTINCT ?b WHERE {\n  ?b ?p ?o .\n  FILTER(isBlank(?b))\n  \
     FILTER NOT EXISTS { ?s ?q ?b }\n  FILTER NOT EXISTS { GRAPH ?h { ?s2 ?q2 ?b } }\n}";

/// `turtle.md`'s query for the same, graph by graph.
const ORPHANS_PER_GRAPH: &str = "SELECT DISTINCT ?g ?b WHERE {\n  GRAPH ?g {\n    ?b ?p ?o .\n    \
     FILTER(isBlank(?b))\n  }\n  FILTER NOT EXISTS { ?s ?q ?b }\n  \
     FILTER NOT EXISTS { GRAPH ?h { ?s2 ?q2 ?b } }\n}";

/// The queries `turtle.md` documents for blank nodes nothing references
/// find the node an edited document left behind, and not the one that
/// replaced it: in the default graph, and graph by graph.
#[tokio::test]
async fn the_documented_query_lists_the_blank_nodes_an_edit_left_behind() {
    let fluree = memory();
    let v1 = "@prefix ex: <http://example.org/> .\nex:A ex:part [ ex:k \"v1\" ] .\n";
    let first = fluree
        .upsert_turtle(genesis_ledger(&fluree, "it/rdf-orphans:main"), v1)
        .await
        .expect("v1");
    let v2 = v1.replace("\"v1\"", "\"v2\"");
    let second = fluree.upsert_turtle(first.ledger, &v2).await.expect("v2");
    let orphans = select(&fluree, &second.ledger, ORPHANS_DEFAULT).await;
    assert_eq!(orphans.len(), 1, "{orphans:?}");
    let values = select(
        &fluree,
        &second.ledger,
        "SELECT ?v WHERE { ?b <http://example.org/k> ?v FILTER(isBlank(?b)) \
         FILTER NOT EXISTS { ?s ?q ?b } }",
    )
    .await;
    assert_eq!(values, vec![row(&["v1"])], "the orphan is v1's node");

    // The same edit inside a GRAPH block, found by the per-graph query.
    let g = "http://example.org/g";
    let v1 = format!(
        "@prefix ex: <http://example.org/> .\nGRAPH <{g}> {{ ex:A ex:part [ ex:k \"v1\" ] . }}\n"
    );
    let first = fluree
        .upsert_turtle(genesis_ledger(&fluree, "it/rdf-orphans-graph:main"), &v1)
        .await
        .expect("v1 in a graph");
    let v2 = v1.replace("\"v1\"", "\"v2\"");
    let second = fluree
        .upsert_turtle(first.ledger, &v2)
        .await
        .expect("v2 in a graph");
    let orphans = select(&fluree, &second.ledger, ORPHANS_PER_GRAPH).await;
    assert_eq!(orphans.len(), 1, "{orphans:?}");
    assert_eq!(orphans[0][0], g, "{orphans:?}");
}

/// A blank-node label names one node across a TriG document, so a node
/// described in one graph can be referenced from another graph, or from the
/// default graph. The documented queries do not list it; a check within the
/// node's own graph would.
#[tokio::test]
async fn the_documented_query_counts_references_from_other_graphs() {
    let fluree = memory();
    let doc = "@prefix ex: <http://example.org/> .\n\
               _:d ex:k \"described in the default graph\" .\n\
               GRAPH <http://example.org/g1> { _:n ex:k \"described in g1\" . }\n\
               GRAPH <http://example.org/g2> { ex:A ex:part _:n , _:d . }\n";
    let r = fluree
        .upsert_turtle(genesis_ledger(&fluree, "it/rdf-orphans-across:main"), doc)
        .await
        .expect("upsert");
    let default_orphans = select(&fluree, &r.ledger, ORPHANS_DEFAULT).await;
    assert!(default_orphans.is_empty(), "{default_orphans:?}");
    let graph_orphans = select(&fluree, &r.ledger, ORPHANS_PER_GRAPH).await;
    assert!(graph_orphans.is_empty(), "{graph_orphans:?}");

    // Checked within the node's own graph only, both would be listed.
    let own_graph_only = select(
        &fluree,
        &r.ledger,
        "SELECT DISTINCT ?g ?b WHERE { GRAPH ?g { ?b ?p ?o . FILTER(isBlank(?b)) \
         FILTER NOT EXISTS { ?s ?q ?b } } }",
    )
    .await;
    assert_eq!(own_graph_only.len(), 1, "{own_graph_only:?}");
    let default_only = select(
        &fluree,
        &r.ledger,
        "SELECT DISTINCT ?b WHERE { ?b ?p ?o . FILTER(isBlank(?b)) \
         FILTER NOT EXISTS { ?s ?q ?b } }",
    )
    .await;
    assert_eq!(default_only.len(), 1, "{default_only:?}");
}

// =============================================================================
// Fuel and the raw transaction
// =============================================================================

const FUEL_SEED: &str = "@prefix ex: <http://example.org/> .\n\
                         ex:s ex:name \"Alice\" ; ex:label \"a\"@en , \"b\"@fr ;\n\
                              ex:items ( \"x\" \"y\" ) ; ex:ssn \"0\" .\n";
const FUEL_UPSERT: &str = "@prefix ex: <http://example.org/> .\n\
                           ex:s ex:name \"Alicia\" ; ex:label \"c\"@en ;\n\
                                ex:items ( \"z\" ) .\n\
                           GRAPH <http://example.org/g> { ex:s ex:name \"G\" . }\n";

/// A tracked Turtle upsert reports the fuel it staged: the per-transaction
/// baseline (10) plus 0.001 per committed flake, the same charge a JSON-LD
/// upsert of the same flakes pays. Billing reads this tally, so the lane
/// must keep it on the cached-handle builder, with and without a policy,
/// over novelty and over an index.
#[tokio::test]
async fn a_tracked_turtle_upsert_reports_its_fuel() {
    let tracking = TrackingOptions {
        track_fuel: true,
        ..Default::default()
    };
    let mut failures = Vec::new();
    for indexed in [false, true] {
        for with_policy in [false, true] {
            let lane = format!("indexed={indexed} policy={with_policy}");
            let dir = tempfile::tempdir().expect("tempdir");
            let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
                .build()
                .expect("file fluree");
            let id = format!("it/rdf-fuel-{indexed}-{with_policy}:main");
            fluree.create_ledger(&id).await.expect("create");
            fluree
                .graph(&id)
                .transact()
                .insert_turtle(FUEL_SEED)
                .commit()
                .await
                .expect("seed");
            if indexed {
                rebuild_and_publish_index(&fluree, &id).await;
            }
            let handle = fluree.ledger_cached(&id).await.expect("handle");
            let builder = fluree
                .stage(&handle)
                .upsert_turtle(FUEL_UPSERT)
                .tracking(tracking.clone());
            let result = if with_policy {
                let ledger = fluree.ledger(&id).await.expect("load");
                builder.policy(allow_most(&ledger).await).execute().await
            } else {
                builder.execute().await
            }
            .expect("tracked upsert");
            let receipt = &result.receipt;
            // name + both labels + both list entries, and three assertions
            // in the default graph plus one in g.
            if (receipt.retract_count, receipt.assert_count) != (5, 4) {
                failures.push(format!("{lane}: receipt {receipt:?}"));
            }
            let fuel = result.tally.as_ref().and_then(|t| t.fuel);
            let expected = 10.0 + receipt.flake_count as f64 / 1000.0;
            match fuel {
                Some(f) if (f - expected).abs() < 1e-9 => {}
                other => failures.push(format!("{lane}: fuel {other:?}, expected {expected}")),
            }
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// With `store_raw_txn`, the raw transaction of a Turtle upsert is the text
/// the caller sent, as for a Turtle insert, not a JSON-LD rendering of it.
#[tokio::test]
async fn the_raw_transaction_of_a_turtle_upsert_is_its_text() {
    let fluree = memory();
    let id = "it/rdf-raw-txn:main";
    let doc = "@prefix ex: <http://example.org/> .\nex:s ex:p ( 1 2 ) .\n";
    let result = fluree
        .upsert_turtle_with_opts(
            genesis_ledger(&fluree, id),
            doc,
            TxnOpts::default().store_raw_txn(true),
            CommitOpts::default(),
            &fluree_db_api::server_defaults::default_index_config(),
        )
        .await
        .expect("upsert");
    let store = fluree.content_store(id);
    let commit = fluree_db_core::commit::codec::read_commit(
        &store
            .get(&result.receipt.commit_id)
            .await
            .expect("commit blob"),
    )
    .expect("commit decodes");
    let txn = commit.txn.expect("the commit references its raw txn");
    let stored: JsonValue =
        serde_json::from_slice(&store.get(&txn).await.expect("raw txn blob")).expect("json");
    assert_eq!(stored, JsonValue::String(doc.to_string()));
}

// =============================================================================
// Errors
// =============================================================================

/// A malformed document fails as a Turtle parse error, pointing into the
/// document as sent, and names the TriG construct it refuses.
#[tokio::test]
async fn a_malformed_document_fails_as_a_turtle_parse_error() {
    let fluree = memory();
    let doc = "@prefix ex: <http://example.org/> .\n\
               GRAPH <http://example.org/g1> { ex:a ex:p 1 . }\n\
               GRAPH <http://example.org/g2> { ex:b ex:p ; }\n";
    let err = fluree
        .upsert_turtle(genesis_ledger(&fluree, "it/rdf-errors:main"), doc)
        .await
        .map(|_| ())
        .expect_err("malformed");
    assert!(
        matches!(err, fluree_db_api::ApiError::Turtle(_)),
        "a Turtle parse error: {err:?}"
    );
    let message = err.to_string();
    let second = doc.find("GRAPH <http://example.org/g2>").expect("block");
    let position: usize = message
        .split("position ")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no position in {message}"));
    assert!(
        position > second,
        "the position is the document's, inside the second block: {message}"
    );

    let nested = "GRAPH <http://example.org/g> { GRAPH <http://example.org/h> { } }";
    let err = fluree
        .upsert_turtle(genesis_ledger(&fluree, "it/rdf-errors-2:main"), nested)
        .await
        .map(|_| ())
        .expect_err("nested");
    assert!(err.to_string().contains("nested graph block"), "{err}");
}

/// What a block's contents are refused for is the user's mistake, reported
/// as one: an undefined prefix is a Turtle parse error, a hand-written
/// `f:reifies*` statement a transaction error. Neither reads as an engine
/// fault.
#[tokio::test]
async fn refusals_inside_a_block_are_the_user_s_errors() {
    let fluree = memory();
    let undefined = "GRAPH <http://example.org/g1> { nope:a <http://example.org/p> 1 . }";
    let err = fluree
        .upsert_turtle(
            genesis_ledger(&fluree, "it/rdf-block-errors:main"),
            undefined,
        )
        .await
        .map(|_| ())
        .expect_err("undefined prefix");
    assert!(
        matches!(err, fluree_db_api::ApiError::Turtle(_)) && err.to_string().contains("nope"),
        "{err:?}"
    );

    let reserved = "GRAPH <http://example.org/g1> { <http://example.org/claim> \
                    <https://ns.flur.ee/db#reifiesSubject> <http://example.org/evil> . }";
    let err = fluree
        .upsert_turtle(
            genesis_ledger(&fluree, "it/rdf-block-errors-2:main"),
            reserved,
        )
        .await
        .map(|_| ())
        .expect_err("reserved predicate");
    assert!(
        matches!(err, fluree_db_api::ApiError::Transact(_))
            && err.to_string().contains("system-controlled predicate"),
        "{err:?}"
    );
}
