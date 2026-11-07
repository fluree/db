//! Native reasoning-index spike over LUBM: build an OWL 2 RL index from a
//! file-backed, indexed ledger and compare it with query-time reasoning.
//!
//! ```bash
//! cargo run --release --example reasoning_index_spike -p fluree-db-api --features native -- \
//!     <lubm-combined.ttl> <queries dir> [expected counts, comma-separated q01..q14]
//! ```
//!
//! Reports, as JSON on stdout:
//! - per query: query-time and index row counts, whether the two complete result
//!   multisets are equal, and the expected count when given;
//! - the index's stats and storage: bytes in the base index, in the index, and in
//!   the shared dictionaries; how many of the index root's dictionary blobs it
//!   shares with the base root, and the bytes of the ones it does not;
//! - the same after one small commit folded in as a logical generation.
//!
//! With `SPIKE_TIMINGS=1` it also reports wall-clock figures: the rebuild and
//! the logical refresh, and per query the first and the median of
//! `SPIKE_RUNS` (default 5) runs, query-time and from the index. Take timings
//! on the benchmark hosts only.
//!
//! `SPIKE_DIR=<path>` keeps the ledger there instead of in a temporary
//! directory. A second process with `SPIKE_REOPEN=1` and the same
//! `SPIKE_DIR` then opens it cold and times each query's first and median
//! run from the index, as after a restart. Each process reports its peak
//! resident set (`peak_rss_kb`) where the platform exposes it.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_core::{ContentId, ContentStore, CODEC_FLUREE_DICT_BLOB};
use serde_json::{json, Value as JsonValue};

const LEDGER: &str = "lubm:main";

/// The rows of a SPARQL result in canonical, sorted form.
fn multiset(result: &JsonValue) -> Vec<String> {
    let rows: Vec<JsonValue> = match result.pointer("/results/bindings") {
        Some(JsonValue::Array(rows)) => rows.clone(),
        _ => match result {
            JsonValue::Array(rows) => rows.clone(),
            other => vec![other.clone()],
        },
    };
    let mut out: Vec<String> = rows.iter().map(ToString::to_string).collect();
    out.sort();
    out
}

fn timings_enabled() -> bool {
    std::env::var("SPIKE_TIMINGS").is_ok_and(|v| v == "1")
}

/// The first run's and the median run's wall-clock milliseconds.
async fn timed(fluree: &Fluree, sparql: &str) -> JsonValue {
    let runs: usize = std::env::var("SPIKE_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
        .max(1);
    let mut ms = Vec::with_capacity(runs);
    for _ in 0..runs {
        let started = std::time::Instant::now();
        let _ = run(fluree, sparql).await;
        ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    let first = ms[0];
    ms.sort_by(f64::total_cmp);
    json!({"first_ms": first, "median_ms": ms[ms.len() / 2]})
}

async fn run(fluree: &Fluree, sparql: &str) -> (Vec<String>, bool) {
    let resp = fluree
        .graph(LEDGER)
        .query()
        .sparql(sparql)
        .track_all()
        .execute_tracked()
        .await
        .unwrap_or_else(|e| panic!("query failed: {e:?}"));
    let served = resp
        .reasoning
        .as_ref()
        .and_then(|r| r.index.as_ref())
        .is_some();
    (multiset(&resp.result), served)
}

fn dir_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                dir_bytes(&p)
            } else {
                e.metadata().map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}

fn find_dir(root: &Path, name: &str) -> Option<PathBuf> {
    for entry in std::fs::read_dir(root).ok()?.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == name) {
                return Some(path);
            }
            if let Some(found) = find_dir(&path, name) {
                return Some(found);
            }
        }
    }
    None
}

async fn dict_refs(store: &dyn ContentStore, root: &ContentId) -> HashSet<ContentId> {
    let bytes = store.get(root).await.expect("root");
    IndexRoot::decode(&bytes)
        .expect("decode root")
        .all_cas_ids()
        .into_iter()
        .filter(|id| id.codec() == CODEC_FLUREE_DICT_BLOB)
        .collect()
}

async fn storage_report(
    fluree: &Fluree,
    dir: &Path,
    index: &fluree_db_core::reasoning_index::ReasoningIndexId,
    index_root: &ContentId,
) -> JsonValue {
    let record = fluree.nameservice().lookup(LEDGER).await.unwrap().unwrap();
    let base_root = record.index_head_id.clone().expect("base index");
    let store = fluree.backend().content_store(&record.storage_namespace());
    let index_store = fluree
        .backend()
        .content_store(&record.storage_namespace().reasoning_index(index.as_str()));
    let base_dicts = dict_refs(store.as_ref(), &base_root).await;
    let index_dicts = dict_refs(index_store.as_ref(), index_root).await;
    let shared = index_dicts.intersection(&base_dicts).count();
    let mut index_only_bytes = 0u64;
    for id in index_dicts.difference(&base_dicts) {
        index_only_bytes += store.get(id).await.map(|b| b.len() as u64).unwrap_or(0);
    }
    let branch = find_dir(dir, "main").expect("branch folder");
    json!({
        "base_index_bytes": dir_bytes(&branch.join("index")),
        "index_bytes": dir_bytes(&branch.join("@reasoning")),
        "shared_dict_bytes": find_dir(dir, "@shared").map(|p| dir_bytes(&p)),
        "index_root_dict_blobs": index_dicts.len(),
        "shared_with_base_root": shared,
        "index_only_dict_blobs": index_dicts.len() - shared,
        "index_only_dict_bytes": index_only_bytes,
    })
}

/// The process's peak resident set in KiB, from `/proc/self/status`.
fn peak_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
}

/// The `.sparql` files of `dir`, by name, each with a budget no LUBM closure
/// reaches, so query-time answers are complete.
fn load_queries(dir: &Path) -> Vec<(String, String)> {
    let mut names: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("queries")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "sparql"))
        .collect();
    names.sort();
    names
        .iter()
        .map(|p| {
            let text = std::fs::read_to_string(p).expect("query");
            let text = format!("# PRAGMA reasoning-max-facts: 100000000\n{text}");
            (p.file_stem().unwrap().to_string_lossy().to_string(), text)
        })
        .collect()
}

/// Open a kept ledger cold and time every query from its reasoning index.
async fn reopen(dir: &Path, queries_dir: &Path) {
    let opened = std::time::Instant::now();
    let fluree: Fluree = FlureeBuilder::file(dir.to_string_lossy().to_string())
        .build()
        .expect("fluree");
    let mut per_query = Vec::new();
    for (name, sparql) in load_queries(queries_dir) {
        let started = std::time::Instant::now();
        let (rows, served) = run(&fluree, &sparql).await;
        let first_ms = started.elapsed().as_secs_f64() * 1000.0;
        let mut entry = timed(&fluree, &sparql).await;
        entry["first_ms"] = json!(first_ms);
        entry["query"] = json!(name);
        entry["rows"] = json!(rows.len());
        entry["served_from_index"] = json!(served);
        per_query.push(entry);
    }
    let report = json!({
        "ledger": LEDGER,
        "reopen": per_query,
        "total_ms": opened.elapsed().as_secs_f64() * 1000.0,
        "peak_rss_kb": peak_rss_kb(),
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let ttl_path = args
        .get(1)
        .expect("usage: <lubm-combined.ttl> <queries dir> [counts]");
    let queries_dir = PathBuf::from(args.get(2).expect("queries dir"));
    let expected: Vec<Option<usize>> = args
        .get(3)
        .map(|s| s.split(',').map(|n| n.trim().parse().ok()).collect())
        .unwrap_or_default();

    let kept = std::env::var("SPIKE_DIR").ok().map(PathBuf::from);
    if std::env::var("SPIKE_REOPEN").is_ok_and(|v| v == "1") {
        let dir = kept.expect("SPIKE_REOPEN needs SPIKE_DIR");
        reopen(&dir, &queries_dir).await;
        return;
    }
    let temp;
    let dir: PathBuf = match kept {
        Some(dir) => {
            std::fs::create_dir_all(&dir).expect("SPIKE_DIR");
            dir
        }
        None => {
            temp = tempfile::tempdir().expect("tempdir");
            temp.path().to_path_buf()
        }
    };
    let fluree: Fluree = FlureeBuilder::file(dir.to_string_lossy().to_string())
        .build()
        .expect("fluree");
    // `SPIKE_IMPORT=1` bulk-imports the file (indexed on arrival); otherwise
    // one transaction, then a reindex.
    let ledger = if std::env::var("SPIKE_IMPORT").is_ok() {
        fluree
            .create(LEDGER)
            .import(ttl_path)
            .execute()
            .await
            .expect("import");
        fluree.ledger(LEDGER).await.expect("load")
    } else {
        let ledger = fluree.create_ledger(LEDGER).await.expect("ledger");
        let ttl = std::fs::read_to_string(ttl_path).expect("read ttl");
        let ledger = fluree
            .insert_turtle(ledger, &ttl)
            .await
            .expect("insert")
            .ledger;
        drop(ttl);
        fluree
            .reindex(LEDGER, ReindexOptions::default())
            .await
            .expect("reindex");
        ledger
    };

    let queries = load_queries(&queries_dir);

    let mut query_time = Vec::new();
    let mut query_time_ms = Vec::new();
    for (_, sparql) in &queries {
        let (rows, served) = run(&fluree, sparql).await;
        assert!(!served, "nothing is registered yet");
        query_time.push(rows);
        if timings_enabled() {
            query_time_ms.push(timed(&fluree, sparql).await);
        }
    }

    let index = fluree
        .register_reasoning_index(LEDGER, &json!({"reasoning": "owl2rl"}))
        .await
        .expect("register");
    let rebuild_started = std::time::Instant::now();
    let rebuild = fluree
        .refresh_reasoning_index(LEDGER, &index)
        .await
        .expect("refresh");
    let rebuild_ms = rebuild_started.elapsed().as_secs_f64() * 1000.0;
    let doc = fluree.reasoning_indexes(LEDGER).await.unwrap();
    let pointer = doc.indexes[&index].head.clone().expect("published");
    let manifest = {
        let record = fluree.nameservice().lookup(LEDGER).await.unwrap().unwrap();
        let store = fluree
            .backend()
            .content_store(&record.storage_namespace().reasoning_index(index.as_str()));
        fluree_db_core::reasoning_index::ReasoningManifest::decode(
            &store.get(&pointer.manifest).await.unwrap(),
        )
        .unwrap()
    };

    let mut per_query = Vec::new();
    let mut all_equal = true;
    for (i, ((name, sparql), expected_rows)) in queries.iter().zip(&query_time).enumerate() {
        let (rows, served) = run(&fluree, sparql).await;
        let equal = &rows == expected_rows;
        all_equal &= equal && served;
        let mut entry = json!({
            "query": name,
            "query_time_rows": expected_rows.len(),
            "index_rows": rows.len(),
            "served_from_index": served,
            "multisets_equal": equal,
            "expected": expected.get(i).copied().flatten(),
        });
        if timings_enabled() {
            entry["query_time"] = query_time_ms[i].clone();
            entry["index"] = timed(&fluree, sparql).await;
        }
        per_query.push(entry);
    }
    let storage = storage_report(&fluree, &dir, &index, &manifest.root).await;

    // One small commit, folded in as a logical generation.
    let _ledger = fluree
        .insert_turtle(
            ledger,
            "@prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> .\n\
             <http://example.org/spike/newStudent> a ub:GraduateStudent ;\n\
               ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0> .\n",
        )
        .await
        .expect("commit")
        .ledger;
    let index_bytes_before = storage["index_bytes"].as_u64().unwrap_or(0);
    let logical_started = std::time::Instant::now();
    let logical = fluree
        .refresh_reasoning_index(LEDGER, &index)
        .await
        .expect("refresh");
    let logical_ms = logical_started.elapsed().as_secs_f64() * 1000.0;
    let doc = fluree.reasoning_indexes(LEDGER).await.unwrap();
    let pointer = doc.indexes[&index].head.clone().expect("published");
    let record = fluree.nameservice().lookup(LEDGER).await.unwrap().unwrap();
    let index_store = fluree
        .backend()
        .content_store(&record.storage_namespace().reasoning_index(index.as_str()));
    let manifest2 = fluree_db_core::reasoning_index::ReasoningManifest::decode(
        &index_store.get(&pointer.manifest).await.unwrap(),
    )
    .unwrap();
    let storage2 = storage_report(&fluree, &dir, &index, &manifest2.root).await;
    let (q1_rows, q1_served) = run(&fluree, &queries[0].1).await;

    let derived_set_bytes = {
        let index = fluree_db_core::reasoning_index::DerivedIndex::decode(
            &index_store.get(&manifest2.derived).await.unwrap(),
        )
        .unwrap();
        let mut total = 0usize;
        for bucket in &index.buckets {
            total += index_store.get(bucket).await.map(|b| b.len()).unwrap_or(0);
        }
        total
    };
    let mut report = json!({
        "ledger": LEDGER,
        "rebuild": serde_json::to_value(&rebuild).unwrap(),
        "parity": {"all_equal_and_served": all_equal, "queries": per_query},
        "storage": storage,
        "logical": {
            "outcome": serde_json::to_value(&logical).unwrap(),
            "storage": storage2,
            "index_bytes_written": storage2["index_bytes"].as_u64().unwrap_or(0).saturating_sub(index_bytes_before),
            "derived_set_bytes": derived_set_bytes,
            "q01_rows_after": q1_rows.len(),
            "q01_served": q1_served,
        }
    });
    if timings_enabled() {
        report["timings"] = json!({"rebuild_ms": rebuild_ms, "logical_refresh_ms": logical_ms});
    }
    report["peak_rss_kb"] = json!(peak_rss_kb());
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
