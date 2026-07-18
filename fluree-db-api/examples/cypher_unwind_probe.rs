//! Reproduce the KB Elasticsearch-reindex query shape: a large `UNWIND $uris`
//! feeding a per-URI property-equality match plus one-hop expansion,
//! against an n10s-style ledger (every node typed `INDEXED`, carrying its
//! IRI as a `value` string property, rdf:type modeled as an edge).
//!
//! Reported behavior at ~72k nodes / ~190k edges: ~21k URIs pegs CPU and
//! OOMs a 16 GB server; the same query with `LIMIT 20` still takes ~35 s,
//! while a single point lookup by known value is single-digit ms.
//!
//! ```bash
//! cargo run --release --example cypher_unwind_probe -p fluree-db-api
//! PROBE_NODES=72000 PROBE_URIS=21000 cargo run --release --example cypher_unwind_probe -p fluree-db-api
//! ```

use std::time::Instant;

use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use serde_json::json;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn kb_uri(i: usize) -> String {
    format!("http://kb.example/entity/{i}")
}

/// One KB-style node: label INDEXED, `value` holds the entity IRI as a plain
/// string, plus a scalar prop. ~2.6 outgoing edges per node across a few
/// relationship predicates, scattered over the id space.
fn kb_node(i: usize, nodes: usize, mixed: bool, annotated: bool) -> serde_json::Value {
    let edge_count = if i % 5 < 3 { 3 } else { 2 };
    // PROBE_MIXED: a slice of nodes carry a language-tagged `value` (RDF-KB
    // label idiom). The predicate then has >1 string-compatible datatype, so
    // stats can't pin one and bound-string probes lose the tight seek.
    let value = if mixed && i.is_multiple_of(20) {
        json!({"@value": format!("entité {i}"), "@language": "fr"})
    } else {
        json!(kb_uri(i))
    };
    let mut node = json!({
        "@id": format!("http://kb.example/node/{i}"),
        "@type": "INDEXED",
        "value": value,
        "name": format!("entity {i}"),
    });
    let obj = node.as_object_mut().unwrap();
    for e in 0..edge_count {
        let target = ((i as u64 + e as u64 + 1).wrapping_mul(2_654_435_761)) % nodes as u64;
        let pred = ["order", "related", "refersTo"][e % 3];
        // PROBE_ANNOTATED: reify each edge (what the Cypher bulk import and
        // Cypher CREATE do), so `-[p]->` traverses real relationship nodes.
        let object = if annotated {
            json!({"@id": format!("http://kb.example/node/{target}"), "@annotation": {}})
        } else {
            json!({"@id": format!("http://kb.example/node/{target}")})
        };
        obj.insert(pred.to_string(), object);
    }
    node
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let nodes = env_usize("PROBE_NODES", 72_000);
    let uris = env_usize("PROBE_URIS", 1_000);
    // reindex: insert + reindex (indexer-built root)
    // import:  bulk-import a .jsonl (import-pipeline-built root — the
    //          customer's `fluree create --from` path)
    // novelty: insert only (all data in novelty)
    let mode = std::env::var("PROBE_MODE").unwrap_or_else(|_| "reindex".to_string());
    let mixed = std::env::var("PROBE_MIXED").is_ok();
    let annotated = std::env::var("PROBE_ANNOTATED").is_ok();

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree: Fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("fluree");

    let build_start = Instant::now();
    match mode.as_str() {
        "import" => {
            use std::io::Write;
            let data_path = dir.path().join("kb.jsonl");
            let mut f =
                std::io::BufWriter::new(std::fs::File::create(&data_path).expect("create jsonl"));
            for i in 0..nodes {
                serde_json::to_writer(&mut f, &kb_node(i, nodes, mixed, annotated))
                    .expect("write node");
                f.write_all(b"\n").expect("newline");
            }
            f.into_inner().expect("flush");
            fluree
                .create("probe:kb")
                .import(&data_path)
                .execute()
                .await
                .expect("bulk import");
        }
        _ => {
            let ledger = fluree.create_ledger("probe:kb").await.expect("ledger");
            let graph: Vec<_> = (0..nodes)
                .map(|i| kb_node(i, nodes, mixed, annotated))
                .collect();
            fluree
                .insert(
                    ledger,
                    &json!({"@graph": graph, "opts": {"lpgEdgeLifecycle": annotated}}),
                )
                .await
                .expect("seed");
            if mode != "novelty" {
                fluree
                    .reindex("probe:kb", ReindexOptions::default())
                    .await
                    .expect("reindex");
            }
        }
    }
    eprintln!(
        "seeded {nodes} nodes (~{} edges) via {mode} (mixed={mixed} annotated={annotated}) in {:.1}s",
        (nodes as f64 * 2.6) as u64,
        build_start.elapsed().as_secs_f64()
    );

    let db = fluree.db("probe:kb").await.expect("db");

    // Baseline: point lookup by known value (customer: single-digit ms).
    let point = format!(
        "MATCH (s:INDEXED {{value: \"{}\"}}) RETURN s.value",
        kb_uri(nodes / 2)
    );
    for label in ["point (cold)", "point (warm)"] {
        let t = Instant::now();
        let r = fluree.query_cypher(&db, &point).await.expect("point");
        eprintln!(
            "{label}: {} rows in {:.1}ms",
            r.batches
                .iter()
                .map(fluree_db_api::Batch::len)
                .sum::<usize>(),
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // The reindex query, param-substituted exactly like the HTTP path.
    let uri_list: Vec<serde_json::Value> = (0..uris)
        .map(|k| json!(kb_uri((k * 7919) % nodes)))
        .collect();
    let params: fluree_db_cypher::ParamMap =
        serde_json::from_value(json!({"uris": uri_list})).expect("params");

    let reindex_query = "UNWIND $uris AS uri \
         MATCH (s:INDEXED {value: uri})-[p]->(o:INDEXED) \
         RETURN uri, s.value AS subject, type(p) AS rel, o.value AS object";

    for (label, query) in [
        ("unwind+limit20", format!("{reindex_query} LIMIT 20")),
        ("unwind full", reindex_query.to_string()),
    ] {
        let t = Instant::now();
        match fluree
            .query_cypher_with_params(&db, &query, Some(&params))
            .await
        {
            Ok(r) => eprintln!(
                "{label} ({uris} uris): {} rows in {:.2}s",
                r.batches
                    .iter()
                    .map(fluree_db_api::Batch::len)
                    .sum::<usize>(),
                t.elapsed().as_secs_f64()
            ),
            Err(e) => eprintln!(
                "{label} ({uris} uris): ERROR after {:.2}s: {e}",
                t.elapsed().as_secs_f64()
            ),
        }
    }
}
