//! Reproduce the KB concept-page / ES-reindex Cypher shapes that read
//! relationship properties (`p.p`, `r1.p`, `r2.p`) on an n10s-style reified
//! ledger — the *required* edge-annotation lane (a property-reading rel var
//! lowers to the `f:reifies*` chain in the main BGP, unlike the value-only
//! OPTIONAL lane).
//!
//! Field report (~61k nodes / ~191k reified edges, 59 relationship types):
//! - `getObjectsByUris` (UNWIND + untyped rel + `p.p`): OOM at 21k URIs; the
//!   server log fills with per-row `BinaryScanOperator::open` on
//!   `f:reifiesSubject`.
//! - `getObjectAssociations`: 4.9 s stock, 9.5 s once `r1.p, r2.p` are added.
//!
//! ```bash
//! cargo run --release --example cypher_kb_pageload_probe -p fluree-db-api
//! PROBE_NODES=72000 PROBE_URIS=21000 cargo run --release --example cypher_kb_pageload_probe -p fluree-db-api
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

fn class_uri(c: usize) -> String {
    format!("http://kb.example/class/{c}")
}

const SCHEME_URI: &str = "http://kb.example/scheme/mediatopic/";

fn rel_pred(k: usize) -> String {
    format!("http://kb.example/rel/{k}")
}

/// One KB entity node: labels `INDEXED` + its class, `value` holds the entity
/// IRI as a string (n10s idiom), plus `kind`/`type`/`lang` scalar props. Each
/// node carries ~2.6 reified outgoing edges over `rel_types` relationship
/// predicates; every annotation stores the predicate IRI under `p` (the
/// customer's edge shape). Every 40th node points into the scheme node.
fn kb_node(i: usize, nodes: usize, classes: usize, rel_types: usize) -> serde_json::Value {
    let mut node = json!({
        "@id": format!("http://kb.example/node/{i}"),
        "@type": ["INDEXED", class_uri(i % classes)],
        "value": kb_uri(i),
        "kind": "uri",
    });
    let obj = node.as_object_mut().unwrap();
    if !i.is_multiple_of(3) {
        obj.insert("type".into(), json!("concept"));
    }
    if i.is_multiple_of(2) {
        obj.insert("lang".into(), json!("en"));
    }
    let edge_count = if i % 5 < 3 { 3 } else { 2 };
    for e in 0..edge_count {
        let target = ((i as u64 + e as u64 + 1).wrapping_mul(2_654_435_761)) % nodes as u64;
        let k = (i * 31 + e * 7) % rel_types;
        let pred = rel_pred(k);
        obj.insert(
            pred.clone(),
            json!({
                "@id": format!("http://kb.example/node/{target}"),
                "@annotation": {"w": 1, "p": pred}
            }),
        );
    }
    if i.is_multiple_of(40) {
        let pred = "http://kb.example/rel/inScheme".to_string();
        obj.insert(
            pred.clone(),
            json!({"@id": "http://kb.example/node/scheme", "@annotation": {"w": 1, "p": pred}}),
        );
    }
    node
}

fn class_node(c: usize) -> serde_json::Value {
    json!({
        "@id": class_uri(c),
        "@type": "INDEXED",
        "value": class_uri(c),
        "kind": "uri",
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if std::env::var("RUST_LOG").is_ok() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    }
    let nodes = env_usize("PROBE_NODES", 72_000);
    let classes = env_usize("PROBE_CLASSES", 50);
    let rel_types = env_usize("PROBE_RELTYPES", 50);
    let uris = env_usize("PROBE_URIS", 1_000);
    // import:  bulk-import a .jsonl (import-pipeline root, annotation_index
    //          absent — the customer's `fluree create --from` ledger shape)
    // reindex: insert + reindex (indexer-built root WITH the annotation arena)
    let mode = std::env::var("PROBE_MODE").unwrap_or_else(|_| "import".to_string());

    let dir = tempfile::tempdir().expect("tempdir");
    // Server parity: the ledger cache config enables the leaflet cache —
    // without it every per-row scan re-decodes its leaflet from scratch.
    let fluree: Fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build()
        .expect("fluree");

    let build_start = Instant::now();
    let all_nodes = || {
        let mut graph: Vec<_> = (0..classes).map(class_node).collect();
        graph.push(json!({
            "@id": "http://kb.example/node/scheme",
            "@type": "INDEXED",
            "value": SCHEME_URI,
            "kind": "uri",
        }));
        graph.extend((0..nodes).map(|i| kb_node(i, nodes, classes, rel_types)));
        graph
    };
    match mode.as_str() {
        "import" => {
            use std::io::Write;
            let data_path = dir.path().join("kb.jsonl");
            let mut f =
                std::io::BufWriter::new(std::fs::File::create(&data_path).expect("create jsonl"));
            for node in all_nodes() {
                serde_json::to_writer(&mut f, &node).expect("write node");
                f.write_all(b"\n").expect("newline");
            }
            f.into_inner().expect("flush");
            fluree
                .create("probe:kbpage")
                .import(&data_path)
                .execute()
                .await
                .expect("bulk import");
        }
        _ => {
            let ledger = fluree.create_ledger("probe:kbpage").await.expect("ledger");
            fluree
                .insert(
                    ledger,
                    &json!({"@graph": all_nodes(), "opts": {"lpgEdgeLifecycle": true}}),
                )
                .await
                .expect("seed");
            fluree
                .reindex("probe:kbpage", ReindexOptions::default())
                .await
                .expect("reindex");
        }
    }
    eprintln!(
        "seeded {nodes} nodes / {classes} classes / {rel_types} rel types via {mode} in {:.1}s",
        build_start.elapsed().as_secs_f64()
    );

    let db = fluree.db("probe:kbpage").await.expect("db");
    eprintln!(
        "annotation_index={} content_store={}",
        db.snapshot.annotation_index.is_some(),
        db.snapshot.content_store.is_some()
    );

    // ---- getObjectsByUris: UNWIND + untyped rel + p.p property read ----
    let uri_list: Vec<serde_json::Value> = (0..uris)
        .map(|k| json!(kb_uri((k * 7919) % nodes)))
        .collect();
    let params: fluree_db_cypher::ParamMap =
        serde_json::from_value(json!({"uris": uri_list})).expect("params");

    let by_uris = "UNWIND $uris as uri \
         MATCH (s:INDEXED {value: uri})-[p]->(o:INDEXED) \
         RETURN distinct s.value, s.kind, p.p AS predType, o.value, o.kind, o.type, o.lang";
    // Control: same query without the rel-property read (value-only lane).
    let by_uris_no_p = "UNWIND $uris as uri \
         MATCH (s:INDEXED {value: uri})-[p]->(o:INDEXED) \
         RETURN distinct s.value, s.kind, o.value, o.kind, o.type, o.lang";

    // ---- getObjectAssociations: two untyped rels in one MATCH ----
    let class_list = (0..20.min(classes))
        .map(|c| format!("'{}'", class_uri(c)))
        .collect::<Vec<_>>()
        .join(",");
    let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let assoc_stock = format!(
        "MATCH (t:INDEXED)<-[:`{rdf_type}`]-(a)-[r1]->(p2), \
               (a)-[r2]->(n {{value:'{SCHEME_URI}'}}) \
         WHERE t.value IN [{class_list}] \
         RETURN DISTINCT a.value"
    );
    let assoc_propread = format!(
        "MATCH (t:INDEXED)<-[:`{rdf_type}`]-(a)-[r1]->(p2), \
               (a)-[r2]->(n {{value:'{SCHEME_URI}'}}) \
         WHERE t.value IN [{class_list}] \
         RETURN DISTINCT a.value, r1.p, r2.p"
    );

    if std::env::var("PROBE_EXPLAIN").is_ok() {
        for (label, q) in [
            ("by_uris", by_uris),
            ("by_uris_no_p", by_uris_no_p),
            (
                "bisect_b",
                "UNWIND $uris as uri MATCH (s:INDEXED {value: uri})-[p]->(o:INDEXED) \
                 RETURN distinct s.value, p.p AS predType",
            ),
            (
                "bisect_d",
                "UNWIND $uris as uri MATCH (s {value: uri})-[p]->(o) \
                 RETURN distinct s.value, p.p AS predType",
            ),
            ("assoc_stock", assoc_stock.as_str()),
            ("assoc_propread", assoc_propread.as_str()),
        ] {
            let plan = fluree
                .explain_cypher(&db, q, Some(&params))
                .await
                .expect("explain");
            eprintln!(
                "PLAN {label}: {}",
                serde_json::to_string(&plan["plan"]["physical"]).unwrap_or_default()
            );
            eprintln!(
                "ORDER {label}: {}",
                serde_json::to_string(&plan["plan"]["logical"]).unwrap_or_default()
            );
        }
    }

    // PROBE_BISECT: peel the by_uris p.p plan one stage at a time to
    // attribute the per-uri cost (class checks, DISTINCT, prop reads).
    let mut queries: Vec<(&str, String)> = vec![
        ("by_uris no-p (value lane)", by_uris_no_p.to_string()),
        ("by_uris p.p (required lane)", by_uris.to_string()),
        ("assoc stock", assoc_stock),
        ("assoc r1.p/r2.p", assoc_propread),
    ];
    if std::env::var("PROBE_BISECT").is_ok() {
        queries = vec![
            ("A full p.p", by_uris.to_string()),
            (
                "B minimal RETURN",
                "UNWIND $uris as uri MATCH (s:INDEXED {value: uri})-[p]->(o:INDEXED) \
                 RETURN distinct s.value, p.p AS predType"
                    .to_string(),
            ),
            (
                "C no o-label",
                "UNWIND $uris as uri MATCH (s:INDEXED {value: uri})-[p]->(o) \
                 RETURN distinct s.value, p.p AS predType"
                    .to_string(),
            ),
            (
                "D no s-label",
                "UNWIND $uris as uri MATCH (s {value: uri})-[p]->(o) \
                 RETURN distinct s.value, p.p AS predType"
                    .to_string(),
            ),
            (
                "E no distinct",
                "UNWIND $uris as uri MATCH (s {value: uri})-[p]->(o) \
                 RETURN s.value, p.p AS predType"
                    .to_string(),
            ),
            (
                "F bare chain",
                "UNWIND $uris as uri MATCH (s {value: uri})-[p]->(o) \
                 RETURN p.p AS predType"
                    .to_string(),
            ),
        ];
    }
    for (label, query) in queries {
        let t = Instant::now();
        match fluree
            .query_cypher_with_params(&db, &query, Some(&params))
            .await
        {
            Ok(r) => eprintln!(
                "{label}: {} rows in {:.2}s",
                r.batches
                    .iter()
                    .map(fluree_db_api::Batch::len)
                    .sum::<usize>(),
                t.elapsed().as_secs_f64()
            ),
            Err(e) => eprintln!(
                "{label}: ERROR after {:.2}s: {e}",
                t.elapsed().as_secs_f64()
            ),
        }
    }
}
