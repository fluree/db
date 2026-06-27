//! INDEXED burst profiler — real query engine, real published base index.
//!
//! `novelty_burst_profile` (pure novelty) and `novelty_read_fanout` (real engine,
//! EMPTY base index) both show broad-read latency is dominated by a flat
//! O(novelty) per-query pass, with segment fan-out a ~2x rider on top. The open
//! question they can't answer: **does that per-query O(novelty) cost persist when
//! a large PUBLISHED base index is present?** If the planner reads persisted base
//! stats and only passes over the novelty delta, a burst's read penalty is small
//! and bounded by burst size; if not, only draining (indexing) recovers it and
//! compaction (which preserves flake count) does nothing.
//!
//! This harness builds a real on-disk base index, then measures the SAME queries
//! through the real engine at four stages:
//!   S0  drained baseline  — base indexed, novelty empty (the target)
//!   S1  burst peak         — base + K overlay segments (one per burst commit)
//!   S2  after compact_all  — base + 1 overlay segment (K=1)
//!   S3  after re-drain      — burst folded into a new index, novelty empty again
//!
//! Plus the wall-clock to `compact_all` the overlay (the S1->S2 bridge) and to
//! re-index (the S1->S3 drain).
//!
//! ## Config (env)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `BI_BASE_TXNS` | `20000` | base dataset size (one merged commit, then indexed) |
//! | `BI_BURST_COMMITS` | `1000` | burst = this many small commits (== peak overlay K) |
//! | `BI_BURST_TXNS_PER_COMMIT` | `1` | txns per burst commit (small txn) |
//! | `BI_NPC` | `8` | nodes per txn (~8 flakes/node) |
//! | `BI_REPEATS` | `15` | timed repeats per query (adaptive) |
//! | `BI_DB_DIR` | `target/burst-indexed-db` | storage dir (use /dev/shm on Linux) |
//!
//! ## Run
//! ```bash
//! BI_DB_DIR=/dev/shm/bi cargo run --release \
//!   --example novelty_burst_indexed -p fluree-db-api --features native
//! ```

use std::sync::Arc;
use std::time::Instant;

use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, LedgerState};
use serde_json::{json, Value as JsonValue};

use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_jsonld};

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Merge transactions `[lo, hi)` into one JSON-LD `@graph` so the range commits
/// as ONE transaction (== one novelty segment).
fn merged_insert(lo: usize, hi: usize, npc: usize) -> JsonValue {
    let mut graph: Vec<JsonValue> = Vec::new();
    for t in lo..hi {
        let doc = txn_data_to_jsonld(&generate_txn_data(t, npc));
        if let Some(arr) = doc.get("@graph").and_then(JsonValue::as_array) {
            graph.extend(arr.iter().cloned());
        }
    }
    json!({ "@context": ctx(), "@graph": graph })
}

/// Representative queries; bounded result sets so the timed cost is the engine's
/// per-query overhead + overlay fan-out, not row streaming.
fn queries(txns: usize, npc: usize) -> Vec<(&'static str, JsonValue)> {
    let mid_txn = txns / 2;
    let base = mid_txn * npc;
    let person_id = format!("ex:person-{:06}", base + npc / 2);
    vec![
        (
            "point_crawl",
            json!({ "@context": ctx(), "select": { person_id: ["*"] } }),
        ),
        (
            "narrow_age",
            json!({
                "@context": ctx(),
                "select": ["?s"],
                "where": { "id": "?s", "ex:age": 40 }
            }),
        ),
        (
            "scan_limit",
            json!({
                "@context": ctx(),
                "select": ["?s"],
                "where": { "id": "?s", "ex:age": "?a" },
                "limit": 100
            }),
        ),
        (
            "join_limit",
            json!({
                "@context": ctx(),
                "select": ["?n"],
                "where": [
                    { "id": "?c", "ex:employees": "?p" },
                    { "id": "?p", "ex:name": "?n" }
                ],
                "limit": 100
            }),
        ),
    ]
}

/// Run a query (one warmup + adaptive repeats); return mean micros, or None on error.
async fn time_query(fluree: &Fluree, db: &GraphDb, q: &JsonValue, max_reps: usize) -> Option<u128> {
    let w0 = Instant::now();
    if fluree.query(db, q).await.is_err() {
        return None;
    }
    let warm = w0.elapsed().as_micros();
    let reps = if warm > 2_000_000 {
        1
    } else if warm > 200_000 {
        3
    } else {
        max_reps
    };
    let mut times = Vec::with_capacity(reps);
    for _ in 0..reps {
        let start = Instant::now();
        if fluree.query(db, q).await.is_err() {
            return None;
        }
        times.push(start.elapsed().as_micros());
    }
    times.sort_unstable();
    Some(times[times.len() / 2])
}

/// Replicates `support::rebuild_and_publish_index` (public APIs only): rebuild a
/// binary index for the ledger's current head and publish it to the nameservice.
async fn rebuild_and_publish(fluree: &Fluree, ledger_id: &str) {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("ledger record exists");
    let result = fluree_db_indexer::rebuild_index_from_commits(
        fluree.content_store(ledger_id),
        ledger_id,
        &record,
        fluree_db_indexer::IndexerConfig::default(),
    )
    .await
    .expect("index rebuild");
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_index(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish index");
}

/// Measure all queries over `ledger`, return mean micros aligned with `queries()`.
async fn measure(
    fluree: &Fluree,
    ledger: &LedgerState,
    qs: &[(&'static str, JsonValue)],
    reps: usize,
) -> Vec<Option<u128>> {
    let db = GraphDb::from_ledger_state(ledger);
    let mut out = Vec::with_capacity(qs.len());
    for (_, q) in qs {
        out.push(time_query(fluree, &db, q, reps).await);
    }
    out
}

fn cell(v: Option<u128>) -> String {
    match v {
        Some(us) => format!("{us}"),
        None => "-".to_string(),
    }
}

fn ratio(base: Option<u128>, v: Option<u128>) -> String {
    match (base, v) {
        (Some(b), Some(x)) if b > 0 => format!("{:.1}x", x as f64 / b as f64),
        _ => "-".to_string(),
    }
}

async fn run() {
    let base_txns = env_usize("BI_BASE_TXNS", 20_000);
    let burst_commits = env_usize("BI_BURST_COMMITS", 1000);
    let tpc = env_usize("BI_BURST_TXNS_PER_COMMIT", 1);
    let npc = env_usize("BI_NPC", 8);
    let reps = env_usize("BI_REPEATS", 15);
    let dir = env_str("BI_DB_DIR", "target/burst-indexed-db");

    let _ = std::fs::remove_dir_all(&dir);
    let fluree = FlureeBuilder::file(dir.clone())
        .without_indexing()
        .build()
        .expect("build fluree");
    let ledger_id = "burst/indexed:main";

    println!("======================================================================");
    println!("  INDEXED BURST PROFILE  (real engine, real published base index)");
    println!("======================================================================");
    println!("  base: {base_txns} txns (indexed)   burst: {burst_commits} commits x {tpc} txn  (npc={npc})");

    // ---- Build base + publish index ----
    let t0 = Instant::now();
    let mut ledger = fluree.create_ledger(ledger_id).await.expect("create");
    ledger = fluree
        .insert(ledger, &merged_insert(0, base_txns, npc))
        .await
        .expect("base insert")
        .ledger;
    println!(
        "  base inserted ({} novelty flakes) in {:.1}s; rebuilding index...",
        ledger.novelty().len(),
        t0.elapsed().as_secs_f64()
    );
    let ti = Instant::now();
    rebuild_and_publish(&fluree, ledger_id).await;
    // Reload: attaches BinaryRangeProvider, drains novelty into the base index.
    let base_ledger = fluree.ledger(ledger_id).await.expect("reload indexed");
    println!(
        "  index published + reloaded in {:.1}s; novelty now {} flakes, K={} (drained)\n",
        ti.elapsed().as_secs_f64(),
        base_ledger.novelty().len(),
        base_ledger.novelty().max_segment_count(),
    );

    let qs = queries(base_txns, npc);

    // ---- S0: drained baseline ----
    let s0 = measure(&fluree, &base_ledger, &qs, reps).await;

    // ---- Apply the burst: many small commits => K overlay segments ----
    let tb = Instant::now();
    let mut ledger = base_ledger.clone();
    let mut next = base_txns;
    for _ in 0..burst_commits {
        let lo = next;
        let hi = next + tpc;
        next = hi;
        ledger = fluree
            .insert(ledger, &merged_insert(lo, hi, npc))
            .await
            .expect("burst insert")
            .ledger;
    }
    let burst_k = ledger.novelty().max_segment_count();
    let burst_flakes = ledger.novelty().len();
    println!(
        "  burst applied: {burst_commits} commits in {:.1}s -> overlay K={burst_k}, {burst_flakes} novelty flakes",
        tb.elapsed().as_secs_f64()
    );

    // ---- S1: burst peak K ----
    let s1 = measure(&fluree, &ledger, &qs, reps).await;

    // ---- S1b: cheap read-path TIERED compaction (overlay -> K~tier) ----
    let tier = env_usize("BI_TIER", 16);
    let mut tiered = ledger.clone();
    let tt = Instant::now();
    let mut tmerges = 0usize;
    while tiered.novelty().needs_tier_compaction(tier) {
        tmerges += Arc::make_mut(&mut tiered.novelty).tier_compact(tier);
    }
    let tier_ms = tt.elapsed().as_secs_f64() * 1000.0;
    let tier_k = tiered.novelty().max_segment_count();
    println!(
        "  tier_compact({tier}) overlay: K={burst_k} -> {tier_k} in {tier_ms:.1}ms ({tmerges} merges)"
    );
    let s1b = measure(&fluree, &tiered, &qs, reps).await;

    // ---- S2: after compact_all (overlay -> K=1). Time the bridge. ----
    let mut compacted = ledger.clone();
    let tc = Instant::now();
    Arc::make_mut(&mut compacted.novelty).compact_all();
    let compact_ms = tc.elapsed().as_secs_f64() * 1000.0;
    println!(
        "  compact_all overlay: K={burst_k} -> {} in {:.1}ms ({:.3}us/flake)",
        compacted.novelty().max_segment_count(),
        compact_ms,
        compact_ms * 1000.0 / burst_flakes.max(1) as f64,
    );
    let s2 = measure(&fluree, &compacted, &qs, reps).await;

    // ---- S3: after re-drain (fold burst into a new index). Time the drain. ----
    let td = Instant::now();
    rebuild_and_publish(&fluree, ledger_id).await;
    let redrained = fluree.ledger(ledger_id).await.expect("reload redrained");
    let drain_ms = td.elapsed().as_secs_f64() * 1000.0;
    println!(
        "  re-index (drain) in {:.1}ms; novelty now {} flakes, K={}\n",
        drain_ms,
        redrained.novelty().len(),
        redrained.novelty().max_segment_count(),
    );
    let s3 = measure(&fluree, &redrained, &qs, reps).await;

    // ---- Report ----
    println!("---- query latency (us, median) by stage ----");
    println!(
        "  {:<13}{:>11}{:>11}{:>11}{:>11}{:>11}",
        "query", "S0 drained", "S1 burstK", "S1b tierK", "S2 compK1", "S3 redrain"
    );
    for (qi, (name, _)) in qs.iter().enumerate() {
        println!(
            "  {name:<13}{:>11}{:>11}{:>11}{:>11}{:>11}",
            cell(s0[qi]),
            cell(s1[qi]),
            cell(s1b[qi]),
            cell(s2[qi]),
            cell(s3[qi]),
        );
    }
    println!("\n---- regression vs S0 (drained baseline) ----");
    println!(
        "  {:<13}{:>12}{:>12}{:>12}{:>12}",
        "query", "S1 burstK", "S1b tierK", "S2 compK1", "S3 redrain"
    );
    for (qi, (name, _)) in qs.iter().enumerate() {
        println!(
            "  {name:<13}{:>12}{:>12}{:>12}{:>12}",
            ratio(s0[qi], s1[qi]),
            ratio(s0[qi], s1b[qi]),
            ratio(s0[qi], s2[qi]),
            ratio(s0[qi], s3[qi]),
        );
    }

    println!("\n======================================================================");
    println!("  S1=burst penalty. S1b=cheap tiered (K~{tier}). S2=full compact (K=1).");
    println!("  S3=drained. If S1b ~= S2, the existing cheap tiered pass captures all the");
    println!("  compaction value -> no background compact-to-K=1 machinery needed. If S2/S3");
    println!("  still far above S0, the cost is O(overlay) not O(K): drain / push LIMIT.");
    println!(
        "  bridges: tier({tier})={tier_ms:.0}ms  compact_all={compact_ms:.0}ms  drain={drain_ms:.0}ms."
    );
    println!("======================================================================");

    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let rt = fluree_bench_support::bench_runtime();
    rt.block_on(run());
}
