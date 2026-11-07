//! Native reasoned-head spike over LUBM: build an OWL 2 RL head from a
//! file-backed, indexed ledger and compare it with query-time reasoning.
//!
//! ```bash
//! cargo run --release --example reasoned_head_spike -p fluree-db-api --features native -- \
//!     <lubm-combined.ttl> <queries dir> [expected counts, comma-separated q01..q14]
//! ```
//!
//! Reports, as JSON on stdout:
//! - per query: query-time and head row counts, whether the two complete result
//!   multisets are equal, and the expected count when given;
//! - the head's stats and storage: bytes in the base index, in the head, and in
//!   the shared dictionaries; how many of the head root's dictionary blobs it
//!   shares with the base root, and the bytes of the ones it does not;
//! - the same after one small commit folded in as a logical generation.
//!
//! No timings: measure those on the benchmark hosts.

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
    let mut out: Vec<String> = rows.iter().map(|r| r.to_string()).collect();
    out.sort();
    out
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
        .and_then(|r| r.head.as_ref())
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
    head: &fluree_db_core::reasoned_head::HeadId,
    head_root: &ContentId,
) -> JsonValue {
    let record = fluree.nameservice().lookup(LEDGER).await.unwrap().unwrap();
    let base_root = record.index_head_id.clone().expect("base index");
    let store = fluree.backend().content_store(&record.storage_namespace());
    let head_store = fluree
        .backend()
        .content_store(&record.storage_namespace().reasoned_head(head.as_str()));
    let base_dicts = dict_refs(store.as_ref(), &base_root).await;
    let head_dicts = dict_refs(head_store.as_ref(), head_root).await;
    let shared = head_dicts.intersection(&base_dicts).count();
    let mut head_only_bytes = 0u64;
    for id in head_dicts.difference(&base_dicts) {
        head_only_bytes += store.get(id).await.map(|b| b.len() as u64).unwrap_or(0);
    }
    let branch = find_dir(dir, "main").expect("branch folder");
    json!({
        "base_index_bytes": dir_bytes(&branch.join("index")),
        "head_bytes": dir_bytes(&branch.join("@reasoned")),
        "shared_dict_bytes": find_dir(dir, "@shared").map(|p| dir_bytes(&p)),
        "head_root_dict_blobs": head_dicts.len(),
        "shared_with_base_root": shared,
        "head_only_dict_blobs": head_dicts.len() - shared,
        "head_only_dict_bytes": head_only_bytes,
    })
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

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree: Fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
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

    let mut names: Vec<PathBuf> = std::fs::read_dir(&queries_dir)
        .expect("queries")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "sparql"))
        .collect();
    names.sort();
    let queries: Vec<(String, String)> = names
        .iter()
        .map(|p| {
            let text = std::fs::read_to_string(p).expect("query");
            // A budget no LUBM closure reaches, so query-time answers are complete.
            let text = format!("# PRAGMA reasoning-max-facts: 100000000\n{text}");
            (p.file_stem().unwrap().to_string_lossy().to_string(), text)
        })
        .collect();

    let mut query_time = Vec::new();
    for (_, sparql) in &queries {
        let (rows, served) = run(&fluree, sparql).await;
        assert!(!served, "nothing is registered yet");
        query_time.push(rows);
    }

    let head = fluree
        .register_reasoned_head(LEDGER, &json!({"reasoning": "owl2rl"}))
        .await
        .expect("register");
    let rebuild = fluree
        .refresh_reasoned_head(LEDGER, &head)
        .await
        .expect("refresh");
    let doc = fluree.reasoned_heads(LEDGER).await.unwrap();
    let pointer = doc.heads[&head].published.clone().expect("published");
    let manifest = {
        let record = fluree.nameservice().lookup(LEDGER).await.unwrap().unwrap();
        let store = fluree
            .backend()
            .content_store(&record.storage_namespace().reasoned_head(head.as_str()));
        fluree_db_core::reasoned_head::ReasonedManifest::decode(
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
        per_query.push(json!({
            "query": name,
            "query_time_rows": expected_rows.len(),
            "head_rows": rows.len(),
            "served_from_head": served,
            "multisets_equal": equal,
            "expected": expected.get(i).copied().flatten(),
        }));
    }
    let storage = storage_report(&fluree, dir.path(), &head, &manifest.root).await;

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
    let head_bytes_before = storage["head_bytes"].as_u64().unwrap_or(0);
    let logical = fluree
        .refresh_reasoned_head(LEDGER, &head)
        .await
        .expect("refresh");
    let doc = fluree.reasoned_heads(LEDGER).await.unwrap();
    let pointer = doc.heads[&head].published.clone().expect("published");
    let record = fluree.nameservice().lookup(LEDGER).await.unwrap().unwrap();
    let head_store = fluree
        .backend()
        .content_store(&record.storage_namespace().reasoned_head(head.as_str()));
    let manifest2 = fluree_db_core::reasoned_head::ReasonedManifest::decode(
        &head_store.get(&pointer.manifest).await.unwrap(),
    )
    .unwrap();
    let storage2 = storage_report(&fluree, dir.path(), &head, &manifest2.root).await;
    let (q1_rows, q1_served) = run(&fluree, &queries[0].1).await;

    let derived_set_bytes = {
        let index = fluree_db_core::reasoned_head::DerivedIndex::decode(
            &head_store.get(&manifest2.derived).await.unwrap(),
        )
        .unwrap();
        let mut total = 0usize;
        for bucket in &index.buckets {
            total += head_store.get(bucket).await.map(|b| b.len()).unwrap_or(0);
        }
        total
    };
    let report = json!({
        "ledger": LEDGER,
        "rebuild": serde_json::to_value(&rebuild).unwrap(),
        "parity": {"all_equal_and_served": all_equal, "queries": per_query},
        "storage": storage,
        "logical": {
            "outcome": serde_json::to_value(&logical).unwrap(),
            "storage": storage2,
            "head_bytes_written": storage2["head_bytes"].as_u64().unwrap_or(0).saturating_sub(head_bytes_before),
            "derived_set_bytes": derived_set_bytes,
            "q01_rows_after": q1_rows.len(),
            "q01_served": q1_served,
        }
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
