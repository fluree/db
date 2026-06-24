//! TPC-H-over-Iceberg benchmark harness (Direct catalog / MinIO).
//!
//! Not a CI test — gated behind `TPCH_BENCH=1` and requires the local MinIO +
//! TPC-H dataset from /Users/bplatz/fluree/iceberg-tpch (see scripts/).
//!
//! Run:
//!   source /Users/bplatz/fluree/iceberg-tpch/scripts/minio.env
//!   TPCH_BENCH=1 cargo test -p fluree-db-api --features iceberg \
//!     --test it_tpch_iceberg_bench -- --ignored --nocapture
#![cfg(feature = "iceberg")]

// Materialization is allocation-heavy and parallel; the system allocator
// contends badly across cores. mimalloc is what the deployed binary should use.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod support;

use std::time::Instant;

use fluree_db_api::{Batch, FlureeBuilder, R2rmlCreateConfig};
use support::genesis_ledger;

const WORK: &str = "/Users/bplatz/fluree/iceberg-tpch";
const ENDPOINT: &str = "http://localhost:9000";

// --- Diagnostic micro-probes (localize where time goes) ---

// P1: single predicate, LIMIT 5 — isolates scan/materialize cost (no join/agg).
const P1: &str = r#"
PREFIX v: <http://tpch/voc/>
SELECT ?ep WHERE { GRAPH <tpch-lineitem:main> { ?l v:l_extendedprice ?ep } } LIMIT 5
"#;

// P2: single predicate, full-scan COUNT — scan + count, no join.
const P2: &str = r#"
PREFIX v: <http://tpch/voc/>
SELECT (COUNT(?ep) AS ?c) WHERE { GRAPH <tpch-lineitem:main> { ?l v:l_extendedprice ?ep } }
"#;

// P3: two predicates on same subject — exercises multi-POM join path.
const P3: &str = r#"
PREFIX v: <http://tpch/voc/>
SELECT (COUNT(?l) AS ?c) WHERE {
  GRAPH <tpch-lineitem:main> { ?l v:l_extendedprice ?ep ; v:l_discount ?d }
}
"#;

// TPC-H Q6: forecasting revenue change — single-table filter + SUM.
const Q6: &str = r#"
PREFIX v: <http://tpch/voc/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT (SUM(?ep * ?disc) AS ?revenue) WHERE {
  GRAPH <tpch-lineitem:main> {
    ?l v:l_extendedprice ?ep ;
       v:l_discount ?disc ;
       v:l_quantity ?qty ;
       v:l_shipdate ?sd .
    FILTER(?sd >= "1994-01-01"^^xsd:date && ?sd < "1995-01-01"^^xsd:date
           && ?disc >= 0.05 && ?disc <= 0.07 && ?qty < 24)
  }
}
"#;

// TPC-H Q1: pricing summary report — filter + GROUP BY + 8 aggregates.
const Q1: &str = r#"
PREFIX v: <http://tpch/voc/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?rf ?ls
  (SUM(?qty) AS ?sum_qty)
  (SUM(?ep) AS ?sum_base_price)
  (SUM(?ep * (1 - ?disc)) AS ?sum_disc_price)
  (SUM(?ep * (1 - ?disc) * (1 + ?tax)) AS ?sum_charge)
  (AVG(?qty) AS ?avg_qty)
  (AVG(?ep) AS ?avg_price)
  (AVG(?disc) AS ?avg_disc)
  (COUNT(?l) AS ?count_order)
WHERE {
  GRAPH <tpch-lineitem:main> {
    ?l v:l_returnflag ?rf ;
       v:l_linestatus ?ls ;
       v:l_quantity ?qty ;
       v:l_extendedprice ?ep ;
       v:l_discount ?disc ;
       v:l_tax ?tax ;
       v:l_shipdate ?sd .
    FILTER(?sd <= "1998-09-02"^^xsd:date)
  }
}
GROUP BY ?rf ?ls
ORDER BY ?rf ?ls
"#;

// --- Cross-source join probes (lineitem ⋈ orders on orderkey) ---

// J1: pure join cardinality — COUNT of lineitem joined to its order.
// Baseline isolates the cross-source join cost (no filter/agg).
const J1: &str = r#"
PREFIX v: <http://tpch/voc/>
SELECT (COUNT(?l) AS ?c) WHERE {
  GRAPH <tpch-lineitem:main> { ?l v:l_orderkey ?ok }
  GRAPH <tpch-orders:main>   { ?o v:o_orderkey ?ok }
}
"#;

// J3: TPC-H Q3-style (orders ⋈ lineitem, no customer) — filter both sides,
// group revenue by order. Exercises join + filter + aggregate together.
const J3: &str = r#"
PREFIX v: <http://tpch/voc/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?ok (SUM(?ep * (1 - ?disc)) AS ?revenue) WHERE {
  GRAPH <tpch-orders:main> {
    ?o v:o_orderkey ?ok ; v:o_orderdate ?od .
    FILTER(?od < "1995-03-15"^^xsd:date)
  }
  GRAPH <tpch-lineitem:main> {
    ?l v:l_orderkey ?ok ; v:l_extendedprice ?ep ; v:l_discount ?disc ; v:l_shipdate ?sd .
    FILTER(?sd > "1995-03-15"^^xsd:date)
  }
}
GROUP BY ?ok
ORDER BY DESC(?revenue)
LIMIT 10
"#;

fn print_result(label: &str, r: &fluree_db_api::QueryResult, elapsed_ms: u128) {
    let rows: usize = r.batches.iter().map(Batch::len).sum();
    eprintln!("\n=== {label}: {rows} rows in {elapsed_ms} ms ===");
    let cols = r.batches.first().map(|b| b.schema().len()).unwrap_or(0);
    for batch in &r.batches {
        for i in 0..batch.len().min(12) {
            let vals: Vec<String> = (0..cols)
                .map(|c| {
                    batch
                        .column_by_idx(c)
                        .and_then(|col| col.get(i))
                        .map(|v| format!("{v:?}"))
                        .unwrap_or_else(|| "·".into())
                })
                .collect();
            eprintln!("  {}", vals.join(" | "));
        }
    }
}

#[tokio::test]
#[ignore = "Requires local MinIO + TPC-H dataset. Set TPCH_BENCH=1 to run."]
async fn tpch_lineitem_q1_q6() {
    if std::env::var("TPCH_BENCH").is_err() {
        eprintln!("Skipping (set TPCH_BENCH=1)");
        return;
    }
    // Ensure ambient AWS creds for the Direct S3 storage (MinIO defaults).
    if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    }
    std::env::set_var("AWS_REGION", "us-east-1");

    let fluree = FlureeBuilder::memory().build_memory();
    let mut ledger = genesis_ledger(&fluree, "tpch:main");

    // Register vocab + subject IRI namespaces so SPARQL predicate IRIs encode to
    // Sids and materialized subjects are encodable (rows are skipped otherwise).
    {
        let snap = std::sync::Arc::make_mut(&mut ledger.snapshot);
        snap.insert_namespace_code(9990, "http://tpch/voc/".into())
            .unwrap();
        snap.insert_namespace_code(9991, "http://tpch/lineitem/".into())
            .unwrap();
        snap.insert_namespace_code(9992, "http://tpch/orders/".into())
            .unwrap();
    }

    // DATASET selects scale: "tpch" (SF1), "tpch_small" (SF0.01), "tpch_tiny" (SF0.001).
    let dataset = std::env::var("DATASET").unwrap_or_else(|_| "tpch".into());
    eprintln!("dataset = {dataset}");

    // Register one Direct graph source per table.
    for (name, table) in [("tpch-lineitem", "lineitem"), ("tpch-orders", "orders")] {
        let mapping = std::fs::read_to_string(format!("{WORK}/mappings/{table}.ttl"))
            .unwrap_or_else(|e| panic!("read {table}.ttl: {e}"));
        let loc = format!("s3://warehouse/iceberg/{dataset}/{table}");
        let config = R2rmlCreateConfig::new_direct(name, loc, mapping)
            .with_s3_endpoint(ENDPOINT)
            .with_s3_region("us-east-1")
            .with_s3_path_style(true)
            .with_mapping_media_type("text/turtle");
        let created = fluree
            .create_r2rml_graph_source(config)
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
        eprintln!(
            "graph source: {} | validated={} | maps={}",
            created.graph_source_id, created.mapping_validated, created.triples_map_count
        );
    }

    // Ad-hoc SPARQL via env (no rebuild needed for new probes).
    if let Ok(adhoc) = std::env::var("SPARQL") {
        let t = Instant::now();
        match fluree.sparql_graph_source(&ledger, &adhoc).await {
            Ok(r) => print_result("ADHOC", &r, t.elapsed().as_millis()),
            Err(e) => panic!("ADHOC failed after {} ms: {e}", t.elapsed().as_millis()),
        }
        return;
    }

    // Pick which queries to run via QUERY env (comma-separated names). Default: probes.
    let registry = [
        ("P1", P1),
        ("P2", P2),
        ("P3", P3),
        ("Q6", Q6),
        ("Q1", Q1),
        ("J1", J1),
        ("J3", J3),
    ];
    let want = std::env::var("QUERY").unwrap_or_else(|_| "P1".into());
    let selected: Vec<&str> = want.split(',').map(str::trim).collect();

    for (label, q) in registry {
        if !selected.contains(&label) {
            continue;
        }
        let t = Instant::now();
        match fluree.sparql_graph_source(&ledger, q).await {
            Ok(r) => print_result(label, &r, t.elapsed().as_millis()),
            Err(e) => panic!("{label} failed after {} ms: {e}", t.elapsed().as_millis()),
        }
    }
}
