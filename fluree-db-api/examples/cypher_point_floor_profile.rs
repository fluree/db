//! Point-lookup floor profiler: where do the fixed ~0.4 ms go on trivial
//! MATCH statements against an **indexed** ledger?
//!
//! Trivial point-lookup and short-pattern statements are dominated by a
//! per-query fixed overhead. `query_cypher` already logs per-phase timings at info level
//! (`cypher query phases`: parse_ms / plan_ms / exec_ms); this example
//! runs the floor-shaped statements in a loop with that target enabled
//! and reports medians per phase, plus the typed-format (hydration) and
//! end-to-end cost measured directly.
//!
//! ```bash
//! cargo run --release --example cypher_point_floor_profile -p fluree-db-api
//! ```

use std::sync::{Arc, Mutex};
use std::time::Instant;

use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use serde_json::json;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Captures `cypher query phases` log lines and parses the phase fields.
#[derive(Clone, Default)]
struct PhaseCapture {
    rows: Arc<Mutex<Vec<(f64, f64, f64)>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PhaseCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = PhaseVisitor::default();
        event.record(&mut visitor);
        if let (Some(parse), Some(plan), Some(exec)) =
            (visitor.parse_ms, visitor.plan_ms, visitor.exec_ms)
        {
            self.rows.lock().unwrap().push((parse, plan, exec));
        }
    }
}

#[derive(Default)]
struct PhaseVisitor {
    parse_ms: Option<f64>,
    plan_ms: Option<f64>,
    exec_ms: Option<f64>,
}

impl tracing::field::Visit for PhaseVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let parsed = format!("{value:?}").trim_matches('"').parse::<f64>().ok();
        match field.name() {
            "parse_ms" => self.parse_ms = parsed,
            "plan_ms" => self.plan_ms = parsed,
            "exec_ms" => self.exec_ms = parsed,
            _ => {}
        }
    }
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if values.is_empty() {
        return 0.0;
    }
    values[values.len() / 2]
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let users = env_usize("PROBE_USERS", 20_000);
    let iters = env_usize("PROBE_ITERS", 200);

    let capture = PhaseCapture::default();
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry().with(capture.clone()).init();
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree: Fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("fluree");
    let ledger = fluree.create_ledger("probe:main").await.expect("ledger");

    let graph: Vec<_> = (0..users)
        .map(|i| {
            json!({
                "@id": format!("u{i}"),
                "@type": "User",
                "id": i,
                "name": format!("user{i}"),
                "age": 18 + (i % 60),
                "knows": (1..=10u64).map(|k| {
                    let target = ((i as u64 + k).wrapping_mul(2_654_435_761)) % users as u64;
                    json!({"@id": format!("u{target}")})
                }).collect::<Vec<_>>()
            })
        })
        .collect();
    fluree
        .insert(ledger, &json!({"@graph": graph}))
        .await
        .expect("seed");
    fluree
        .reindex("probe:main", ReindexOptions::default())
        .await
        .expect("reindex");
    let db = fluree.db("probe:main").await.expect("db");

    let statements: &[(&str, &str)] = &[
        ("single_vertex_read", "MATCH (n:User {id: 4112}) RETURN n"),
        (
            "pattern_short",
            "MATCH (a:User {id: 4112})-[:knows]->(b:User) RETURN b.id",
        ),
        (
            "pattern_long",
            "MATCH (a:User {id: 4112})-[:knows]->(b)-[:knows]->(c)-[:knows]->(d) RETURN count(d)",
        ),
        ("scalar_prop", "MATCH (n:User {id: 4112}) RETURN n.name"),
        (
            "shortest_path",
            "MATCH p = shortestPath((a:User {id: 5})-[*..15]->(b:User {id: 10487})) RETURN length(p)",
        ),
    ];

    let only = std::env::var("PROBE_ONLY").ok();

    for (name, text) in statements {
        if let Some(only) = &only {
            if name != only {
                continue;
            }
        }
        // Warmup.
        for _ in 0..20 {
            let r = fluree.query_cypher(&db, text).await.expect("warmup");
            let _ = r.to_cypher_typed_table(&db).await.expect("warmup fmt");
        }
        capture.rows.lock().unwrap().clear();

        let mut e2e = Vec::with_capacity(iters);
        let mut fmt = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t0 = Instant::now();
            let result = fluree.query_cypher(&db, text).await.expect("query");
            let t1 = Instant::now();
            std::hint::black_box(result.to_cypher_typed_table(&db).await.expect("fmt"));
            let t2 = Instant::now();
            e2e.push((t1 - t0).as_secs_f64() * 1000.0);
            fmt.push((t2 - t1).as_secs_f64() * 1000.0);
        }

        let rows = capture.rows.lock().unwrap().clone();
        let mut parse: Vec<f64> = rows.iter().map(|r| r.0).collect();
        let mut plan: Vec<f64> = rows.iter().map(|r| r.1).collect();
        let mut exec: Vec<f64> = rows.iter().map(|r| r.2).collect();
        eprintln!(
            "{name}: query={:.3}ms (parse={:.3} plan={:.3} exec={:.3}) format={:.3}ms  total={:.3}ms",
            median(&mut e2e),
            median(&mut parse),
            median(&mut plan),
            median(&mut exec),
            median(&mut fmt),
            median(&mut e2e) + median(&mut fmt),
        );
    }
}
