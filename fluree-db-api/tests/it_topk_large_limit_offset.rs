//! `ORDER BY` with a `LIMIT` or `OFFSET` far beyond the rows there are.
//!
//! A sort keeps its best `LIMIT + OFFSET` rows; these queries answer from the
//! rows that exist, on SPARQL and JSON-LD, SELECT and CONSTRUCT, with fast
//! paths on and off, on an indexed ledger and on one with novelty over its
//! index. The two fast paths that keep their own top-k buffers are stamped:
//! the POST-order `DESC` scan and the grouped-count top-k.
//!
//! Own test binary: the kill switch is process-global, and this file both
//! toggles it and asserts fast-path routing.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder};
use serde_json::{json, Value};
use std::io::Write;
use tempfile::TempDir;

const DATA: &str = r"
@prefix ex: <http://ex/> .

ex:a ex:size 1 .
ex:b ex:size 2 .
ex:c ex:size 3 .
ex:d ex:size 4 .
ex:e ex:size 5 .
";

const PREFIX: &str = "PREFIX ex: <http://ex/>\n";

/// `i64::MAX`, the largest `LIMIT` / `OFFSET` either surface takes, and 2^62.
const LARGE: [u64; 2] = [9_223_372_036_854_775_807, 4_611_686_018_427_387_904];

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

async fn indexed_ledger(dir: &TempDir, name: &str) -> (fluree_db_api::Fluree, String) {
    let data_dir = TempDir::new().expect("data tmpdir");
    let path = data_dir.path().join("00-fixture.ttl");
    let mut f = std::fs::File::create(&path).expect("create ttl");
    f.write_all(DATA.as_bytes()).expect("write ttl");

    // Bulk import builds a fully persisted binary index with no trailing
    // novelty, so the index-backed fast paths can fire.
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let ledger_id = format!("test/{name}:main");
    fluree
        .create(&ledger_id)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    (fluree, ledger_id)
}

/// The `?b` column, local names in row order, or `rows=0`.
fn subjects(results: &Value) -> String {
    let rows = results["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    if rows.is_empty() {
        return "rows=0".to_string();
    }
    rows.iter()
        .map(|row| {
            let iri = row["b"]["value"].as_str().unwrap_or("?");
            iri.rsplit('/').next().unwrap_or(iri).to_string()
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// The number of rows.
fn row_count(results: &Value) -> String {
    let rows = results["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    format!("rows={}", rows.len())
}

/// The number of nodes in a CONSTRUCT result's `@graph`.
fn node_count(graph: &Value) -> String {
    let nodes = graph["@graph"].as_array().map_or(0, Vec::len);
    format!("nodes={nodes}")
}

/// `?z` grouped by count of subjects, most first, as JSON-LD.
fn grouped_count_jsonld(limit: u64, offset: u64) -> Value {
    json!({
        "@context": {"ex": "http://ex/"},
        "select": ["?z", "(as (count ?b) ?c)"],
        "where": {"@id": "?b", "ex:size": "?z"},
        "groupBy": ["?z"],
        "orderBy": "(desc ?c)",
        "limit": limit,
        "offset": offset
    })
}

enum Surface {
    Sparql(String),
    JsonLd(Value),
    Construct(String),
}

struct Case {
    name: String,
    surface: Surface,
    summarize: fn(&Value) -> String,
    expected: String,
    /// A fast-path site that must proceed with fast paths on.
    must_fire: Option<&'static str>,
}

/// Every case, for a ledger whose subjects by `ex:size` ascending are `all`.
fn cases(all: &[&str]) -> Vec<Case> {
    let n = all.len();
    let asc = all.join(",");
    let desc = all.iter().rev().copied().collect::<Vec<_>>().join(",");
    let mut cases = vec![
        Case {
            name: "LIMIT 2 OFFSET 1 (control)".into(),
            surface: Surface::Sparql(
                "SELECT ?b WHERE { ?b ex:size ?z } ORDER BY ?b LIMIT 2 OFFSET 1".into(),
            ),
            summarize: subjects,
            expected: all[1..3].join(","),
            must_fire: None,
        },
        Case {
            name: format!("SPARQL grouped count LIMIT 2 OFFSET {}", n - 1),
            surface: Surface::Sparql(format!(
                "SELECT ?z (COUNT(?b) AS ?c) WHERE {{ ?b ex:size ?z }} \
                 GROUP BY ?z ORDER BY DESC(?c) LIMIT 2 OFFSET {}",
                n - 1
            )),
            summarize: row_count,
            expected: "rows=1".into(),
            must_fire: Some("group_by_object_count_topk"),
        },
        Case {
            name: format!("JSON-LD grouped count limit 2 offset {}", n - 1),
            surface: Surface::JsonLd(grouped_count_jsonld(2, (n - 1) as u64)),
            summarize: row_count,
            expected: "rows=1".into(),
            must_fire: Some("group_by_object_count_topk"),
        },
    ];
    for large in LARGE {
        cases.extend([
            Case {
                name: format!("SPARQL SELECT LIMIT 2 OFFSET {large}"),
                surface: Surface::Sparql(format!(
                    "SELECT ?b WHERE {{ ?b ex:size ?z }} ORDER BY ?b LIMIT 2 OFFSET {large}"
                )),
                summarize: subjects,
                expected: "rows=0".into(),
                must_fire: None,
            },
            Case {
                name: format!("SPARQL SELECT LIMIT {large}"),
                surface: Surface::Sparql(format!(
                    "SELECT ?b WHERE {{ ?b ex:size ?z }} ORDER BY ?b LIMIT {large}"
                )),
                summarize: subjects,
                expected: asc.clone(),
                must_fire: None,
            },
            Case {
                name: format!("SPARQL DESC scan LIMIT 2 OFFSET {large}"),
                surface: Surface::Sparql(format!(
                    "SELECT ?b ?z WHERE {{ ?b ex:size ?z }} ORDER BY DESC(?z) LIMIT 2 OFFSET {large}"
                )),
                summarize: subjects,
                expected: "rows=0".into(),
                must_fire: Some("post-order-desc-limit"),
            },
            Case {
                name: format!("SPARQL DESC scan LIMIT {large}"),
                surface: Surface::Sparql(format!(
                    "SELECT ?b ?z WHERE {{ ?b ex:size ?z }} ORDER BY DESC(?z) LIMIT {large}"
                )),
                summarize: subjects,
                expected: desc.clone(),
                must_fire: Some("post-order-desc-limit"),
            },
            Case {
                name: format!("SPARQL grouped count LIMIT {large}"),
                surface: Surface::Sparql(format!(
                    "SELECT ?z (COUNT(?b) AS ?c) WHERE {{ ?b ex:size ?z }} \
                     GROUP BY ?z ORDER BY DESC(?c) LIMIT {large}"
                )),
                summarize: row_count,
                expected: format!("rows={n}"),
                must_fire: Some("group_by_object_count_topk"),
            },
            Case {
                name: format!("SPARQL grouped count LIMIT 2 OFFSET {large}"),
                surface: Surface::Sparql(format!(
                    "SELECT ?z (COUNT(?b) AS ?c) WHERE {{ ?b ex:size ?z }} \
                     GROUP BY ?z ORDER BY DESC(?c) LIMIT 2 OFFSET {large}"
                )),
                summarize: row_count,
                expected: "rows=0".into(),
                must_fire: Some("group_by_object_count_topk"),
            },
            Case {
                name: format!("JSON-LD grouped count limit 2 offset {large}"),
                surface: Surface::JsonLd(grouped_count_jsonld(2, large)),
                summarize: row_count,
                expected: "rows=0".into(),
                must_fire: Some("group_by_object_count_topk"),
            },
            Case {
                name: format!("SPARQL CONSTRUCT LIMIT 1 OFFSET {large}"),
                surface: Surface::Construct(format!(
                    "CONSTRUCT {{ ?b ex:s ?z }} WHERE {{ ?b ex:size ?z }} \
                     ORDER BY ?b LIMIT 1 OFFSET {large}"
                )),
                summarize: node_count,
                expected: "nodes=0".into(),
                must_fire: None,
            },
            Case {
                name: format!("SPARQL CONSTRUCT LIMIT {large}"),
                surface: Surface::Construct(format!(
                    "CONSTRUCT {{ ?b ex:s ?z }} WHERE {{ ?b ex:size ?z }} ORDER BY ?b LIMIT {large}"
                )),
                summarize: node_count,
                expected: format!("nodes={n}"),
                must_fire: None,
            },
            Case {
                name: format!("JSON-LD select limit 2 offset {large}"),
                surface: Surface::JsonLd(json!({
                    "@context": {"ex": "http://ex/"},
                    "select": ["?b"],
                    "where": {"@id": "?b", "ex:size": "?z"},
                    "orderBy": "?b",
                    "limit": 2,
                    "offset": large
                })),
                summarize: subjects,
                expected: "rows=0".into(),
                must_fire: None,
            },
            Case {
                name: format!("JSON-LD select limit {large}"),
                surface: Surface::JsonLd(json!({
                    "@context": {"ex": "http://ex/"},
                    "select": ["?b"],
                    "where": {"@id": "?b", "ex:size": "?z"},
                    "orderBy": "?b",
                    "limit": large
                })),
                summarize: subjects,
                expected: asc.clone(),
                must_fire: None,
            },
        ]);
    }
    cases
}

#[tokio::test(flavor = "current_thread")]
async fn order_by_answers_from_the_rows_there_are_whatever_the_limit_and_offset() {
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast-path phase of this \
         test would run generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;

    let db_dir = TempDir::new().expect("db tmpdir");
    let (fluree, ledger_id) = indexed_ledger(&db_dir, "topk-bounds").await;
    let indexed = fluree.ledger(&ledger_id).await.expect("load");
    let with_novelty = fluree
        .insert(
            fluree.ledger(&ledger_id).await.expect("load"),
            &json!({"@context": {"ex": "http://ex/"}, "@id": "ex:f", "ex:size": 6}),
        )
        .await
        .expect("insert")
        .ledger;

    let (store, _tracing_guard) = span_capture::init_test_tracing();
    let mut failures: Vec<String> = Vec::new();

    for (state, ledger, all) in [
        ("indexed", &indexed, &["a", "b", "c", "d", "e"][..]),
        (
            "novelty",
            &with_novelty,
            &["a", "b", "c", "d", "e", "f"][..],
        ),
    ] {
        for (lane, disabled) in [("fast", false), ("generic", true)] {
            set_fast_paths_disabled(disabled);
            for case in cases(all) {
                let before = store.find_events("fast-path outcome").len();
                let db = fluree_db_api::GraphDb::from_ledger_state(ledger);
                let got = match &case.surface {
                    Surface::Sparql(q) => {
                        let q = format!("{PREFIX}{q}");
                        match fluree.query(&db, q.as_str()).await {
                            Ok(r) => r
                                .to_sparql_json(&ledger.snapshot)
                                .map(|j| (case.summarize)(&j)),
                            Err(e) => Ok(format!("ERR:{e}")),
                        }
                    }
                    Surface::JsonLd(q) => match fluree.query(&db, q).await {
                        Ok(r) => r
                            .to_sparql_json(&ledger.snapshot)
                            .map(|j| (case.summarize)(&j)),
                        Err(e) => Ok(format!("ERR:{e}")),
                    },
                    Surface::Construct(q) => {
                        let q = format!("{PREFIX}{q}");
                        match fluree.query(&db, q.as_str()).await {
                            Ok(r) => r
                                .to_construct(&ledger.snapshot)
                                .map(|g| (case.summarize)(&g)),
                            Err(e) => Ok(format!("ERR:{e}")),
                        }
                    }
                }
                .unwrap_or_else(|e| format!("FORMAT ERR:{e}"));
                if got != case.expected {
                    failures.push(format!(
                        "[{state}/{lane}] {}: got {got}, expected {}",
                        case.name, case.expected
                    ));
                }
                if let (Some(site), false) = (case.must_fire, disabled) {
                    let proceeded = store.find_events("fast-path outcome")[before..]
                        .iter()
                        .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
                        .any(|e| e.fields.get("site").map(String::as_str) == Some(site));
                    if !proceeded {
                        failures.push(format!(
                            "[{state}/{lane}] {}: expected site `{site}` to proceed",
                            case.name
                        ));
                    }
                }
            }
        }
    }
    set_fast_paths_disabled(false);

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
