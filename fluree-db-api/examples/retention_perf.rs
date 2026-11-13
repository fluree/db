//! Commit latency of untagged writes with retention disabled, with an active
//! retention policy and a running worker but no expirations (enabled-idle),
//! with every write carrying a TTL, and while the worker expires a backlog.
//!
//! ```text
//! cargo run --release -p fluree-db-api --example retention_perf -- [commits] [backlog]
//! ```
//!
//! Each mode runs on its own ledger in one file-backed instance with the
//! background indexer and the retention worker installed, so the disabled
//! mode measures the cost a ledger that never uses retention pays.

use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_api::retention::RetentionOptions;
use fluree_db_api::{Fluree, FlureeBuilder};
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"})
}

fn doc(i: usize, ttl: Option<u64>) -> JsonValue {
    let mut node = json!({
        "@context": ctx(),
        "@id": format!("ex:s{i}"),
        "ex:a": i, "ex:b": format!("b{i}"), "ex:c": i * 2, "ex:d": format!("d{i}"),
        "ex:e": i % 7, "ex:f": format!("f{i}"), "ex:g": i % 13, "ex:h": format!("h{i}"),
        "ex:i": i % 17
    });
    node["ex:status"] = match ttl {
        Some(seconds) => json!({"@value": format!("s{i}"), "@ttl": seconds}),
        None => json!(format!("s{i}")),
    };
    node
}

async fn create(fluree: &Fluree, name: &str) -> String {
    fluree.create_ledger(name).await.expect("create");
    format!("{name}:main")
}

async fn commit(fluree: &Fluree, ledger_id: &str, body: &JsonValue) -> Duration {
    let started = Instant::now();
    fluree
        .graph(ledger_id)
        .transact()
        .insert(body)
        .commit()
        .await
        .expect("commit");
    started.elapsed()
}

fn report(mode: &str, mut latencies: Vec<Duration>, total: Duration) {
    latencies.sort();
    let at = |q: f64| latencies[((latencies.len() as f64 - 1.0) * q).round() as usize];
    let mean = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    println!(
        "{mode:<14} commits={:>6} total={:>9.3}s rate={:>8.1}/s mean={:>8.3}ms p50={:>8.3}ms p95={:>8.3}ms p99={:>8.3}ms",
        latencies.len(),
        total.as_secs_f64(),
        latencies.len() as f64 / total.as_secs_f64(),
        mean.as_secs_f64() * 1e3,
        at(0.50).as_secs_f64() * 1e3,
        at(0.95).as_secs_f64() * 1e3,
        at(0.99).as_secs_f64() * 1e3,
    );
}

async fn run(fluree: &Fluree, ledger_id: &str, commits: usize, ttl: Option<u64>, mode: &str) {
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(commits);
    for i in 0..commits {
        latencies.push(commit(fluree, ledger_id, &doc(i, ttl)).await);
    }
    report(mode, latencies, started.elapsed());
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let commits: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(2_000);
    let backlog: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5_000);
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = Arc::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .with_indexer_catchup_interval(Duration::from_secs(1))
            .build()
            .expect("fluree"),
    );
    // RETENTION_INTERVAL_MS overrides the worker's pass interval.
    let interval_ms = std::env::var("RETENTION_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(250);
    fluree.start_maintenance(
        Default::default(),
        Some(RetentionOptions {
            interval: Duration::from_millis(interval_ms),
            ..Default::default()
        }),
    );

    let disabled = create(&fluree, "perf/disabled").await;
    run(&fluree, &disabled, commits, None, "disabled").await;

    // Control: a config graph that sets nothing retention reads.
    let config_only = create(&fluree, "perf/config-only").await;
    fluree
        .graph(&config_only)
        .transact()
        .upsert_turtle(&format!(
            "@prefix f: <https://ns.flur.ee/db#> .
             GRAPH <urn:fluree:{config_only}#config> {{
               <urn:fluree:{config_only}:config:ledger> a f:LedgerConfig .
             }}"
        ))
        .commit()
        .await
        .expect("config");
    run(&fluree, &config_only, commits, None, "config-only").await;

    let idle = create(&fluree, "perf/enabled-idle").await;
    fluree
        .graph(&idle)
        .transact()
        .upsert_turtle(&format!(
            "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
             GRAPH <urn:fluree:{idle}#config> {{
               <urn:fluree:{idle}:config:ledger> a f:LedgerConfig ;
                 f:retentionDefaults <urn:cfg:r> .
               <urn:cfg:r> f:property <urn:cfg:p> .
               <urn:cfg:p> f:target ex:unused ; f:ttl 86400 .
             }}"
        ))
        .commit()
        .await
        .expect("config");
    run(&fluree, &idle, commits, None, "enabled-idle").await;

    // Control for order: a second disabled ledger, written while the
    // indexer still works through the earlier ledgers' commits.
    let disabled_2 = create(&fluree, "perf/disabled-2").await;
    run(&fluree, &disabled_2, commits, None, "disabled-2").await;

    let tagged = create(&fluree, "perf/tagged").await;
    run(&fluree, &tagged, commits, Some(86_400), "tagged").await;

    // A backlog that comes due at once, expired while untagged writes run.
    let loaded = create(&fluree, "perf/expiry-load").await;
    for chunk in 0..backlog.div_ceil(100) {
        let nodes: Vec<JsonValue> = (0..100)
            .map(|j| {
                let i = chunk * 100 + j;
                json!({"@id": format!("ex:x{i}"), "ex:v": {"@value": i, "@ttl": 1}})
            })
            .collect();
        commit(
            &fluree,
            &loaded,
            &json!({"@context": ctx(), "@graph": nodes}),
        )
        .await;
    }
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let expiry_started = Instant::now();
    run(&fluree, &loaded, commits, None, "expiry-load").await;
    let mut drained = None;
    for _ in 0..1_200 {
        let status = fluree.retention_status(&loaded).await.expect("status");
        let pending = status.registration.as_ref().map_or(0, |r| r.pending);
        let expired = status.registration.as_ref().map_or(0, |r| r.expired_total);
        if pending == 0 && expired as usize >= backlog {
            drained = Some(expiry_started.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let status = fluree.retention_status(&loaded).await.expect("status");
    println!(
        "expiry backlog={backlog} drained_after={:?} registration={}",
        drained,
        serde_json::to_string(&status.registration).unwrap()
    );
}
