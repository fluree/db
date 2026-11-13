//! Decimals in a legacy NumBig arena count by value on the object-keyed
//! fast paths.
//!
//! Index builds before decimal normalization gave each scale variant its own
//! arena handle, so `1.5` and `1.50` sit under two handles of one predicate.
//! Such indexes are still around. The fast paths that count or group a
//! predicate's objects by their raw `(o_type, o_key)` would split that value
//! in two, so on a graph whose arena has scale-variant duplicates they hand
//! the query to the general pipeline, which decodes and compares by value.
//!
//! No current build writes such an arena, so the test imports a ledger and
//! then overwrites its decimal arena with the bytes a pre-normalization build
//! wrote for the same three handles. A fresh Fluree over the same directory
//! loads it as an older index would be loaded.

#![cfg(feature = "native")]

use crate::support::span_capture::{init_test_tracing, SpanStore};
use crate::support::FAST_PATH_SWITCH;
use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder, GraphSnapshot};
use serde_json::Value;
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;

/// Three decimals under `ex:d` (three handles) and a string on every subject
/// for the star join's filter predicate.
const FIXTURE: &str = r#"@prefix ex: <http://example.org/> .
ex:a ex:d 1.5 ; ex:t "x" .
ex:b ex:d 2.25 ; ex:t "x" .
ex:c ex:d 19.99 ; ex:t "y" .
"#;

/// The NBA1 bytes of a pre-normalization arena holding `1.50`, `1.5` and
/// `19.99` under handles 0, 1 and 2: two subjects then hold `1.5`, under two
/// handles, and one holds `19.99`, whichever subject the import gave which
/// handle.
fn legacy_arena_bytes() -> Vec<u8> {
    /// One decimal entry as a pre-normalization build wrote it: the unscaled
    /// value's signed little-endian bytes and the scale as given.
    fn bigdec_entry(buf: &mut Vec<u8>, unscaled_le: &[u8], scale: i64) {
        buf.push(1); // decimal repr tag
        let repr_len = 4 + unscaled_le.len() + 8;
        buf.extend_from_slice(&(repr_len as u32).to_le_bytes());
        buf.extend_from_slice(&(unscaled_le.len() as u32).to_le_bytes());
        buf.extend_from_slice(unscaled_le);
        buf.extend_from_slice(&scale.to_le_bytes());
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(b"NBA1");
    buf.extend_from_slice(&3u32.to_le_bytes());
    bigdec_entry(&mut buf, &[0x96, 0x00], 2); // 150 x 10^-2 = 1.50
    bigdec_entry(&mut buf, &[0x0f], 1); // 15 x 10^-1 = 1.5
    bigdec_entry(&mut buf, &[0xcf, 0x07], 2); // 1999 x 10^-2 = 19.99
    buf
}

/// Every file under `dir` that starts with the NumBig arena magic.
fn numbig_arena_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).expect("read dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if std::fs::read(&path).is_ok_and(|b| b.starts_with(b"NBA1")) {
                found.push(path);
            }
        }
    }
    found
}

/// A file-backed directory holding the fixture's ledger, bulk-imported, so
/// every fact is in the index.
async fn imported_ledger(ledger_id: &str) -> (TempDir, TempDir) {
    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");
    std::fs::File::create(data_dir.path().join("00-data.ttl"))
        .expect("create ttl")
        .write_all(FIXTURE.as_bytes())
        .expect("write ttl");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("build file-backed Fluree");
    fluree
        .create(ledger_id)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    (db_dir, data_dir)
}

/// Replace the imported ledger's one decimal arena with
/// [`legacy_arena_bytes`]. A Fluree opened afterwards reads the file as it is.
fn install_legacy_arena(db_dir: &Path) {
    let arenas = numbig_arena_files(db_dir);
    assert_eq!(arenas.len(), 1, "one decimal arena: {arenas:?}");
    std::fs::write(&arenas[0], legacy_arena_bytes()).expect("write legacy arena");
}

/// The rows' `n` (or `c`) counts, sorted: the decimal each group renders is
/// whichever scale variant it met first, so the counts are what's compared.
fn counts(results: &Value, var: &str) -> Vec<i64> {
    let mut n: Vec<i64> = results["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|row| {
            row[var]["value"]
                .as_str()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("count in {row}"))
        })
        .collect();
    n.sort_unstable();
    n
}

async fn sparql(db: &GraphSnapshot<'_>, body: &str) -> Value {
    let query = format!("PREFIX ex: <http://example.org/>\n{body}");
    db.query()
        .sparql(&query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"))
}

/// The fast-path sites that stamped an outcome starting with `outcome` since
/// event `before`.
fn stamped(store: &SpanStore, before: usize, outcome: &str) -> Vec<String> {
    store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| {
            e.fields
                .get("outcome")
                .is_some_and(|o| o.starts_with(outcome))
        })
        .filter_map(|e| e.fields.get("site").cloned())
        .collect()
}

/// One object-keyed lane: its `fast-path outcome` site (`None` for the
/// partitioned GROUP BY, which folds in place rather than declining), its
/// query, the variable holding its counts, and the counts on the legacy
/// arena (`1.5` on two subjects, `19.99` on one) and on the arena as
/// imported (three distinct decimals).
struct Lane {
    site: Option<&'static str>,
    query: &'static str,
    var: &'static str,
    legacy: &'static [i64],
    imported: &'static [i64],
}

const LANES: [Lane; 6] = [
    Lane {
        site: Some("COUNT(DISTINCT)"),
        query: "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ex:d ?o }",
        var: "n",
        legacy: &[2],
        imported: &[3],
    },
    Lane {
        site: Some("group_by_object_count_topk"),
        query: "SELECT ?o (COUNT(?s) AS ?n) WHERE { ?s ex:d ?o } GROUP BY ?o \
                ORDER BY DESC(?n) LIMIT 5",
        var: "n",
        legacy: &[1, 2],
        imported: &[1, 1, 1],
    },
    Lane {
        site: Some("group_by_object_star_topk"),
        query: "SELECT ?o (COUNT(?s) AS ?n) WHERE { ?s ex:d ?o . ?s ex:t ?x } GROUP BY ?o \
                ORDER BY DESC(?n) LIMIT 5",
        var: "n",
        legacy: &[1, 2],
        imported: &[1, 1, 1],
    },
    Lane {
        site: None,
        query: "SELECT ?o (COUNT(?s) AS ?n) WHERE { ?s ex:d ?o } GROUP BY ?o",
        var: "n",
        legacy: &[1, 2],
        imported: &[1, 1, 1],
    },
    // The whole-graph shapes Cypher lowers `MATCH (n) RETURN count(DISTINCT
    // n.d)` and `MATCH (n) RETURN n.d, count(*)` to.
    Lane {
        site: Some("whole-graph scalar aggregates"),
        query: "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { \
                { SELECT DISTINCT ?s WHERE { ?s ?p ?x } } OPTIONAL { ?s ex:d ?o } }",
        var: "n",
        legacy: &[2],
        imported: &[3],
    },
    Lane {
        site: Some("whole-graph scalar aggregates"),
        query: "SELECT ?o (COUNT(*) AS ?n) WHERE { \
                { SELECT DISTINCT ?s WHERE { ?s ?p ?x } } OPTIONAL { ?s ex:d ?o } } GROUP BY ?o",
        var: "n",
        legacy: &[1, 2],
        imported: &[1, 1, 1],
    },
];

/// Whether `site` served the query: it proceeded, and did not decline at
/// open after the plan chose it.
fn served(store: &SpanStore, before: usize, site: &str) -> bool {
    stamped(store, before, "proceed").iter().any(|s| s == site)
        && !stamped(store, before, "fallback").iter().any(|s| s == site)
}

/// On the legacy arena every lane answers by value: the fast paths decline
/// it, and the general pipeline (fast paths off) agrees.
#[tokio::test(flavor = "current_thread")]
async fn legacy_decimal_scale_variants_count_as_one_value() {
    let ledger_id = "numbig-legacy/identity:main";
    let (db_dir, _data_dir) = imported_ledger(ledger_id).await;
    install_legacy_arena(db_dir.path());
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("reopen");
    let db = fluree.graph(ledger_id).load().await.expect("load");
    let (store, _guard) = init_test_tracing();

    let mut failures = Vec::new();
    {
        let _switch = FAST_PATH_SWITCH.read().await;
        for lane in &LANES {
            let before = store.find_events("fast-path outcome").len();
            let got = counts(&sparql(&db, lane.query).await, lane.var);
            if got != lane.legacy {
                failures.push(format!(
                    "fast paths on, {}: {got:?}, want {:?}; stamps {:?}",
                    lane.query,
                    lane.legacy,
                    stamped(&store, before, "")
                ));
            }
            if let Some(site) = lane.site.filter(|site| served(&store, before, site)) {
                failures.push(format!("{site} served the legacy arena: {}", lane.query));
            }
        }
    }
    {
        let _switch = FAST_PATH_SWITCH.write().await;
        set_fast_paths_disabled(true);
        let _restore = FastPathsGuard;
        for lane in &LANES {
            let got = counts(&sparql(&db, lane.query).await, lane.var);
            if got != lane.legacy {
                failures.push(format!(
                    "fast paths off, {}: {got:?}, want {:?}",
                    lane.query, lane.legacy
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The canary: on the arena as a current build writes it, the same lanes
/// serve the same queries.
#[tokio::test(flavor = "current_thread")]
async fn normalized_decimal_arena_keeps_the_object_keyed_lanes() {
    let ledger_id = "numbig-legacy/canary:main";
    let (db_dir, _data_dir) = imported_ledger(ledger_id).await;
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("reopen");
    let db = fluree.graph(ledger_id).load().await.expect("load");
    let _switch = FAST_PATH_SWITCH.read().await;
    let (store, _guard) = init_test_tracing();

    let mut failures = Vec::new();
    for lane in &LANES {
        let before = store.find_events("fast-path outcome").len();
        let got = counts(&sparql(&db, lane.query).await, lane.var);
        if got != lane.imported {
            failures.push(format!("{}: {got:?}, want {:?}", lane.query, lane.imported));
        }
        if let Some(site) = lane.site.filter(|site| !served(&store, before, site)) {
            failures.push(format!(
                "{site} must serve {}: {:?}",
                lane.query,
                stamped(&store, before, "")
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Restores the fast paths when a run with them off ends, panicking or not.
struct FastPathsGuard;

impl Drop for FastPathsGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}
