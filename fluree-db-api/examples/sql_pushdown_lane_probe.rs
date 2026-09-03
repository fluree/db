//! Times the SQL pushdown lane against the per-scan lane over a live
//! Postgres source of realistic size.
//!
//! Expects a `shop` schema (`shop.customers`, `shop.orders`) reachable through
//! a bridge at `FLUREE_SQL_BRIDGE_POSTGRES_URL`; the shapes are the ones the
//! lane suite pins, at scale. Every shape runs on both lanes and the row sets
//! must agree, so a run is also a differential at scale.
//!
//! ```bash
//! FLUREE_SQL_BRIDGE_POSTGRES_URL=http://127.0.0.1:18081 \
//!   cargo run --release --example sql_pushdown_lane_probe -p fluree-db-api --features "sql native"
//! ```
//!
//! `PROBE_ITERS` (default 3) repeats each shape; `PROBE_SHAPES` is a
//! comma-separated filter on shape names; `PROBE_SKIP_SCAN=1` times the lane
//! alone.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, SqlCreateConfig, SqlDialect};
use serde_json::Value;

const SHOP_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#Customer>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "shop.customers" ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{id}" ; rr:class ex:Customer ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:country ; rr:objectMap [ rr:column "country" ] ] .

    <http://example.org/mapping#Order>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "shop.orders" ] ;
        rr:subjectMap [ rr:template "http://example.org/order/{id}" ; rr:class ex:Order ] ;
        rr:predicateObjectMap [
            rr:predicate ex:total ;
            rr:objectMap [ rr:column "total" ; rr:datatype xsd:decimal ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:placed ;
            rr:objectMap [ rr:column "placed" ; rr:datatype xsd:date ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:shipped ;
            rr:objectMap [ rr:column "shipped" ; rr:datatype xsd:dateTime ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:customer ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Customer> ;
                rr:joinCondition [ rr:child "customer_id" ; rr:parent "id" ]
            ]
        ] .
"#;

const PREFIX: &str =
    "PREFIX ex: <http://example.org/>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

/// How the two lanes' results are compared.
#[derive(Clone, Copy)]
enum Compare {
    Rows,
    /// A single-key top-k among ties: any k tied rows are correct, so only
    /// the sort key's values must agree.
    Values(&'static str),
}

struct Shape {
    name: &'static str,
    sparql: &'static str,
    compare: Compare,
}

const SHAPES: &[Shape] = &[
    Shape {
        name: "count",
        sparql: "SELECT (COUNT(*) AS ?n) FROM <shop:main> WHERE { ?o ex:total ?t }",
        compare: Compare::Rows,
    },
    Shape {
        name: "filter-1pct",
        sparql: "SELECT ?o ?t FROM <shop:main> WHERE { ?o ex:total ?t FILTER(?t > 990) }",
        compare: Compare::Rows,
    },
    Shape {
        name: "topk-desc",
        sparql: "SELECT ?o ?t FROM <shop:main> WHERE { ?o ex:total ?t } ORDER BY DESC(?t) LIMIT 10",
        compare: Compare::Values("t"),
    },
    Shape {
        // The tiebreak is a template IRI the statement cannot order on, so
        // the LIMIT stays in the engine and the lane streams the table.
        name: "topk-tiebreak",
        sparql: "SELECT ?o ?t FROM <shop:main> WHERE { ?o ex:total ?t } ORDER BY DESC(?t) ?o LIMIT 10",
        compare: Compare::Rows,
    },
    Shape {
        name: "topk-two-keys",
        sparql: "SELECT ?o ?t ?p FROM <shop:main> WHERE { ?o ex:total ?t ; ex:placed ?p } ORDER BY DESC(?t) ?p LIMIT 10",
        compare: Compare::Values("t"),
    },
    Shape {
        name: "constant-subject",
        sparql: "SELECT ?t ?p FROM <shop:main> WHERE { <http://example.org/order/777777> ex:total ?t ; ex:placed ?p }",
        compare: Compare::Rows,
    },
    Shape {
        name: "fk-join-5pct",
        sparql: "SELECT ?o ?n ?t FROM <shop:main> WHERE { ?c ex:country \"UK\" ; ex:name ?n . ?o ex:customer ?c ; ex:total ?t }",
        compare: Compare::Rows,
    },
    Shape {
        name: "fk-join-dated",
        sparql: "SELECT ?o ?n ?t FROM <shop:main> WHERE { ?c ex:country \"UK\" ; ex:name ?n . ?o ex:customer ?c ; ex:total ?t ; ex:placed ?p FILTER(?p >= \"2024-12-01\"^^xsd:date) }",
        compare: Compare::Rows,
    },
    Shape {
        name: "group-by-customer",
        sparql: "SELECT ?c (COUNT(?o) AS ?n) (SUM(?t) AS ?s) FROM <shop:main> WHERE { ?o ex:customer ?c ; ex:total ?t } GROUP BY ?c",
        compare: Compare::Rows,
    },
    Shape {
        name: "group-by-country",
        sparql: "SELECT ?k (COUNT(?o) AS ?n) (SUM(?t) AS ?s) FROM <shop:main> WHERE { ?c ex:country ?k . ?o ex:customer ?c ; ex:total ?t } GROUP BY ?k",
        compare: Compare::Rows,
    },
    Shape {
        name: "top-customers",
        sparql: "SELECT ?c (COUNT(?o) AS ?n) FROM <shop:main> WHERE { ?o ex:customer ?c } GROUP BY ?c ORDER BY DESC(?n) LIMIT 10",
        compare: Compare::Values("n"),
    },
    Shape {
        name: "fk-join-full",
        sparql: "SELECT ?o ?n FROM <shop:main> WHERE { ?o ex:customer ?c . ?c ex:name ?n }",
        compare: Compare::Rows,
    },
];

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Sorted `var=value` rows, so the two lanes can be compared as sets.
fn rows_of(v: &Value, compare: Compare) -> BTreeSet<String> {
    let vars: Vec<&str> = match compare {
        Compare::Rows => v["head"]["vars"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default(),
        Compare::Values(var) => vec![var],
    };
    v["results"]["bindings"]
        .as_array()
        .map(|b| {
            b.iter()
                .enumerate()
                .map(|(i, row)| {
                    let cells = vars
                        .iter()
                        .map(|var| format!("{var}={}", row[var]["value"].as_str().unwrap_or("?")))
                        .collect::<Vec<_>>()
                        .join(" ");
                    match compare {
                        Compare::Rows => cells,
                        Compare::Values(_) => format!("{cells} #{i}"),
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

struct Run {
    rows: BTreeSet<String>,
    times: Vec<Duration>,
    sent: Vec<String>,
}

async fn run(fluree: &Fluree, sparql: &str, iters: usize, lane: bool, compare: Compare) -> Run {
    set_fast_paths_disabled(!lane);
    let mut times = Vec::with_capacity(iters);
    let mut rows = BTreeSet::new();
    let mut sent = Vec::new();
    for i in 0..iters {
        let start = Instant::now();
        let tracked = fluree
            .query_from()
            .sparql(sparql)
            .execute_tracked()
            .await
            .unwrap_or_else(|e| panic!("query failed: {}\n{sparql}", e.error));
        times.push(start.elapsed());
        if i == 0 {
            rows = rows_of(&tracked.result, compare);
            sent = tracked
                .sql
                .unwrap_or_default()
                .into_iter()
                .map(|s| s.sql)
                .collect();
        }
    }
    set_fast_paths_disabled(false);
    Run { rows, times, sent }
}

fn median(times: &[Duration]) -> Duration {
    let mut t = times.to_vec();
    t.sort();
    t[t.len() / 2]
}

fn ms(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1000.0)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let url = std::env::var("FLUREE_SQL_BRIDGE_POSTGRES_URL")
        .expect("FLUREE_SQL_BRIDGE_POSTGRES_URL must point at a bridge over the shop schema");
    let iters = env_usize("PROBE_ITERS", 3);
    let skip_scan = std::env::var("PROBE_SKIP_SCAN").is_ok_and(|v| v == "1");
    let only: Vec<String> = std::env::var("PROBE_SHAPES")
        .map(|s| s.split(',').map(str::to_string).collect())
        .unwrap_or_default();

    let fluree = FlureeBuilder::memory().build_memory();
    let mut source = SqlCreateConfig::new("shop", url, SHOP_R2RML);
    source.dialect = SqlDialect::Postgres;
    fluree
        .create_sql_graph_source(source)
        .await
        .expect("create sql source");

    println!(
        "{:<20} {:>10} {:>10} {:>10} {:>9} {:>9}  rows",
        "shape", "lane med", "lane min", "scan med", "scan min", "speedup"
    );
    let mut mismatches = 0;
    for shape in SHAPES {
        if !only.is_empty() && !only.iter().any(|o| o == shape.name) {
            continue;
        }
        let sparql = format!("{PREFIX}{}", shape.sparql);
        let lane = run(&fluree, &sparql, iters, true, shape.compare).await;
        if lane.sent.is_empty() {
            eprintln!("{}: the lane sent nothing (declined?)", shape.name);
        }
        if skip_scan {
            println!(
                "{:<20} {:>10} {:>10} {:>10} {:>9} {:>9}  {}",
                shape.name,
                ms(median(&lane.times)),
                ms(*lane.times.iter().min().unwrap()),
                "-",
                "-",
                "-",
                lane.rows.len()
            );
            continue;
        }
        let scan = run(&fluree, &sparql, iters, false, shape.compare).await;
        let lane_med = median(&lane.times);
        let scan_med = median(&scan.times);
        let same = lane.rows == scan.rows;
        if !same {
            mismatches += 1;
            let only_lane: Vec<_> = lane.rows.difference(&scan.rows).take(3).collect();
            let only_scan: Vec<_> = scan.rows.difference(&lane.rows).take(3).collect();
            eprintln!(
                "{}: ROW MISMATCH lane={} scan={}\n  only lane: {only_lane:?}\n  only scan: {only_scan:?}",
                shape.name,
                lane.rows.len(),
                scan.rows.len()
            );
        }
        println!(
            "{:<20} {:>10} {:>10} {:>10} {:>9} {:>8.1}x  {}{}",
            shape.name,
            ms(lane_med),
            ms(*lane.times.iter().min().unwrap()),
            ms(scan_med),
            ms(*scan.times.iter().min().unwrap()),
            scan_med.as_secs_f64() / lane_med.as_secs_f64(),
            lane.rows.len(),
            if same { "" } else { "  MISMATCH" }
        );
    }
    if std::env::var("PROBE_SHOW_SQL").is_ok_and(|v| v == "1") {
        for shape in SHAPES {
            let sparql = format!("{PREFIX}{}", shape.sparql);
            let lane = run(&fluree, &sparql, 1, true, Compare::Rows).await;
            println!("\n-- {}\n{}", shape.name, lane.sent.join("\n"));
        }
    }
    if mismatches > 0 {
        eprintln!("{mismatches} shape(s) disagreed between the lanes");
        std::process::exit(1);
    }
}
