//! In-process READ fan-out profiler: query latency vs novelty SEGMENT COUNT.
//!
//! Increment 2 made novelty append-only/segmented, which collapsed the *write*
//! slope (per-commit cost is now O(batch), independent of accumulated novelty).
//! The tradeoff lives on the *read* side: a range read k-way merges across all of
//! a graph's segments, doing one binary-search probe per segment to find its
//! sub-range before the merge. So per-read setup cost grows with segment count
//! until compaction bounds it. This harness measures that fan-out directly, to
//! decide whether compaction must be **synchronous**, **tiered**, or just
//! **reindex-triggered**.
//!
//! ## Method
//!
//! The SAME dataset (a fixed pool of `FANOUT_TXNS` person/company transactions)
//! is loaded with a controlled number of segments by grouping those txns into
//! `S` commits (one `insert` == one commit == one segment per touched graph).
//! Query RESULTS are therefore identical across configs; only the segment layout
//! varies. Auto-indexing is disabled so novelty never drains, and with an empty
//! base index every read is served from the novelty overlay — so query latency
//! is dominated by the overlay's segment merge, which is exactly what we want to
//! isolate.
//!
//! Queries use small/limited result sets so the timed quantity is the per-read
//! *fan-out setup* (S binary-search probes + heap build), not result streaming:
//!   - `point_crawl`  — crawl one subject (a handful of flakes): purest signal.
//!   - `narrow_age`   — one predicate+value POST slice (medium, bounded result).
//!   - `scan_limit`   — scan a predicate, LIMIT 100: full fan-out, tiny result.
//!   - `join_limit`   — 2-pattern join, LIMIT 100: fan-out multiplied by probes.
//!
//! ## Config (env vars)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `FANOUT_TXNS` | `40000` | total transactions = dataset size (must be divisible by each segment count) |
//! | `FANOUT_NPC` | `10` | nodes per transaction (~8 flakes/node) |
//! | `FANOUT_SEGMENTS` | `1,10,100,1000,10000,40000` | segment counts to sweep |
//! | `FANOUT_REPEATS` | `25` | timed repeats per query |
//! | `FANOUT_DB_DIR` | `target/fanout-db` | storage dir (use `/dev/shm/...` on Linux) |
//!
//! ## Run
//! ```bash
//! FANOUT_DB_DIR=/dev/shm/fanout cargo run --release \
//!   --example novelty_read_fanout -p fluree-db-api --features native
//! ```

use std::io::Write;
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

/// Merge transactions `[lo, hi)` into a single JSON-LD `@graph` document so the
/// whole range commits as ONE transaction (== one novelty segment).
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

/// Build a fresh ledger holding the `txns` dataset spread across exactly
/// `segments` commits (== segments). Returns the loaded final ledger.
async fn build_segmented(
    dir: &str,
    txns: usize,
    npc: usize,
    segments: usize,
) -> (Fluree, LedgerState) {
    let _ = std::fs::remove_dir_all(dir);
    let fluree = FlureeBuilder::file(dir.to_string())
        .without_indexing()
        .build()
        .expect("build fluree");
    let ledger_id = "fanout/bench:main";
    let mut ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create_ledger");

    let tpc = txns / segments; // transactions per commit (txns divisible by segments)
    for s in 0..segments {
        let lo = s * tpc;
        let hi = if s == segments - 1 { txns } else { lo + tpc };
        let json = merged_insert(lo, hi, npc);
        let res = fluree.insert(ledger, &json).await.expect("insert");
        ledger = res.ledger;
    }
    (fluree, ledger)
}

/// The representative query set. Result sets are tiny/bounded so the timed cost
/// is dominated by the overlay fan-out, not row materialization.
fn queries(txns: usize, npc: usize) -> Vec<(&'static str, JsonValue)> {
    // A person gid guaranteed to exist in the middle of the dataset. With the
    // generator, txn `t` puts companies at gids [base, base+max(1,npc/10)) and
    // persons after, where base = t*npc — so base + npc/2 is always a person.
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

/// Run a query (one warmup, then adaptive repeats) and return
/// `(mean_us, p50_us, n_reps)`. Adaptive: a cell whose single run is already
/// expensive uses fewer repeats so a pathological fan-out cell (e.g. a join over
/// 40k segments) can't blow the run up by 25x. Returns `None` if the query errors.
async fn time_query(
    fluree: &Fluree,
    db: &GraphDb,
    q: &JsonValue,
    max_reps: usize,
) -> Option<(u128, u128, usize)> {
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
    let mean = times.iter().sum::<u128>() / times.len() as u128;
    let p50 = times[times.len() / 2];
    Some((mean, p50, reps))
}

/// Emit a line to both stdout (flushed, so it survives non-TTY pipe buffering)
/// and the results file (flushed, so progress is readable mid-run via `cat`).
fn emit(out: &mut std::fs::File, line: &str) {
    println!("{line}");
    let _ = std::io::stdout().flush();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

async fn run() {
    let txns = env_usize("FANOUT_TXNS", 40_000);
    let npc = env_usize("FANOUT_NPC", 10);
    let repeats = env_usize("FANOUT_REPEATS", 10);
    let dir_base = env_str("FANOUT_DB_DIR", "target/fanout-db");
    let out_path = env_str("FANOUT_OUT", "target/fanout-results.txt");
    let seg_list: Vec<usize> = env_str("FANOUT_SEGMENTS", "1,10,100,1000,10000,40000")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .filter(|&s: &usize| s > 0 && txns.is_multiple_of(s))
        .collect();

    if let Some(parent) = std::path::Path::new(&out_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let mut out = std::fs::File::create(&out_path).expect("create results file");

    emit(
        &mut out,
        &format!(
            "read fan-out: {txns} txns x {npc} nodes, segments {seg_list:?}, up to {repeats} repeats/query (adaptive)"
        ),
    );

    let qs = queries(txns, npc);
    // table[query_idx] -> Vec of Option<mean_us> aligned with seg_list.
    let mut table: Vec<Vec<Option<u128>>> = qs.iter().map(|_| Vec::new()).collect();

    for (ci, &segments) in seg_list.iter().enumerate() {
        let dir = format!("{dir_base}/seg{segments}");
        let t0 = Instant::now();
        let (fluree, ledger) = build_segmented(&dir, txns, npc, segments).await;
        let db = GraphDb::from_ledger_state(&ledger);
        emit(
            &mut out,
            &format!(
                "\n=== {segments} segment(s): built in {:.1}s, novelty {} flakes ===",
                t0.elapsed().as_secs_f64(),
                ledger.novelty().len()
            ),
        );

        for (qi, (name, q)) in qs.iter().enumerate() {
            match time_query(&fluree, &db, q, repeats).await {
                Some((mean, p50, n)) => {
                    emit(
                        &mut out,
                        &format!("  {name:12} mean={mean:>9}us  p50={p50:>9}us  (n={n})"),
                    );
                    table[qi].push(Some(mean));
                }
                None => {
                    if ci == 0 {
                        emit(
                            &mut out,
                            &format!("  {name:12} ERROR (query failed — skipping)"),
                        );
                    }
                    table[qi].push(None);
                }
            }
        }

        drop(db);
        drop(ledger);
        drop(fluree);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Summary table + fan-out ratio (max-seg / min-seg).
    emit(&mut out, "\n================ read fan-out: query latency (us, mean) vs segment count ================");
    {
        let mut header = format!("{:14}", "query");
        for &s in &seg_list {
            header.push_str(&format!("{:>11}", format!("{s}seg")));
        }
        header.push_str(&format!("{:>11}", "ratio"));
        emit(&mut out, &header);
    }
    for (qi, (name, _)) in qs.iter().enumerate() {
        let mut line = format!("{name:14}");
        for cell in &table[qi] {
            match cell {
                Some(us) => line.push_str(&format!("{us:>11}")),
                None => line.push_str(&format!("{:>11}", "-")),
            }
        }
        let row = &table[qi];
        let ratio = match (row.first().and_then(|c| *c), row.last().and_then(|c| *c)) {
            (Some(first), Some(last)) if first > 0 => last as f64 / first as f64,
            _ => 0.0,
        };
        line.push_str(&format!("{ratio:>11.1}"));
        emit(&mut out, &line);
    }
    emit(
        &mut out,
        "========================================================================================",
    );
    emit(
        &mut out,
        &format!(
            "ratio = latency at {} segments / latency at {} segments.",
            seg_list.last().copied().unwrap_or(0),
            seg_list.first().copied().unwrap_or(0)
        ),
    );
    emit(
        &mut out,
        "~1x across the sweep => fan-out cheap (reindex-triggered compaction suffices).",
    );
    emit(
        &mut out,
        "linear in segment count => need active compaction (tiered or synchronous).",
    );
}

fn main() {
    let rt = fluree_bench_support::bench_runtime();
    rt.block_on(run());
}
