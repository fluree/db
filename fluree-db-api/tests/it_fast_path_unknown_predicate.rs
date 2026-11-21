//! Regression pin for #2448: a fast path answering a query that names a
//! predicate the index has never seen returned an internal error ("predicate
//! not found in binary index dict") instead of an empty result, and the
//! `SUM(STRLEN(GROUP_CONCAT))` lane answered it with an unbound sum instead of
//! `0`. The fast paths run only when the index answers alone, so such a
//! predicate has no rows.
//!
//! The ledgers are bulk-imported: only an import assigns string ids in lexical
//! order, which the string-prefix count lane requires, and it leaves no
//! novelty, so every lane here can fire. Each case asserts the hand-pinned
//! answer on the fast lane and on the generic pipeline, and that its fast path
//! still `proceed`s, so the fix cannot pass by declining the lane.
//!
//! Own test binary: toggles the process-global kill switch and asserts
//! fast-path routing via span capture.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder};
use serde_json::{json, Value};
use std::io::Write;
use tempfile::TempDir;

const DATA: &str = r#"
@prefix ex: <http://example.org/> .
ex:p1 a ex:Person ; ex:name "abc one" ; ex:age 30 ; ex:knows ex:p2 .
ex:p2 a ex:Person ; ex:name "abd two" ; ex:age 40 ; ex:knows ex:p1 .
ex:p3 a ex:Person ; ex:name "xyz three" ; ex:age 50 ; ex:knows ex:p1 .
"#;

const STRING_PREFIX_COUNT: &str = "string-prefix COUNT(*)";
const STRING_PREFIX_SUM: &str = "string-prefix SUM(STRSTARTS)";
const LABEL_REGEX_TYPE: &str = "label-regex-type";
const STAR_CONST_ORDER_TOPK: &str = "star-const-order-topk";
const SUM_STRLEN_GROUP_CONCAT: &str = "SUM(STRLEN(GROUP_CONCAT))";

struct Case {
    name: &'static str,
    query: Value,
    /// As `summarize` renders it.
    expected: &'static str,
    site: &'static str,
}

fn sparql(q: &str) -> Value {
    Value::String(format!(
        "PREFIX ex: <http://example.org/>\n\
         PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n{q}"
    ))
}

fn cases() -> Vec<Case> {
    let ctx = json!({"ex": "http://example.org/"});
    vec![
        Case {
            name: "STRSTARTS count, unknown predicate",
            query: sparql(
                r#"SELECT (COUNT(*) AS ?n) WHERE { ?s ex:nope ?o FILTER(STRSTARTS(?o, "abc")) }"#,
            ),
            expected: "n=0",
            site: STRING_PREFIX_COUNT,
        },
        Case {
            name: "STRSTARTS count, unknown predicate (JSON-LD)",
            query: json!({
                "@context": ctx,
                "select": ["(as (count *) ?n)"],
                "where": [{"@id": "?s", "ex:nope": "?o"}, ["filter", "(strStarts ?o \"abc\")"]]
            }),
            expected: "n=0",
            site: STRING_PREFIX_COUNT,
        },
        Case {
            name: "STRSTARTS count, known predicate",
            query: sparql(
                r#"SELECT (COUNT(*) AS ?n) WHERE { ?s ex:name ?o FILTER(STRSTARTS(?o, "ab")) }"#,
            ),
            expected: "n=2",
            site: STRING_PREFIX_COUNT,
        },
        Case {
            name: "SUM(STRSTARTS), unknown predicate",
            query: sparql(
                r#"SELECT (SUM(xsd:integer(STRSTARTS(?o, "abc"))) AS ?n) WHERE { ?s ex:nope ?o }"#,
            ),
            expected: "n=0",
            site: STRING_PREFIX_SUM,
        },
        Case {
            name: "label + type + regex, unknown label predicate",
            query: sparql(
                r#"SELECT ?s ?l WHERE { ?s ex:nope ?l . ?s a ex:Person . FILTER regex(?l, "a") }"#,
            ),
            expected: "rows=0",
            site: LABEL_REGEX_TYPE,
        },
        Case {
            name: "label + type + regex, unknown label predicate (JSON-LD)",
            query: json!({
                "@context": ctx,
                "select": ["?s", "?l"],
                "where": [
                    {"@id": "?s", "@type": "ex:Person", "ex:nope": "?l"},
                    ["filter", "(regex ?l \"a\")"]
                ]
            }),
            expected: "rows=0",
            site: LABEL_REGEX_TYPE,
        },
        Case {
            name: "label + type + regex, known predicates",
            query: sparql(
                r#"SELECT ?s ?l WHERE { ?s ex:name ?l . ?s a ex:Person . FILTER regex(?l, "ab") }"#,
            ),
            expected: "rows=2",
            site: LABEL_REGEX_TYPE,
        },
        Case {
            name: "star top-k, unknown label predicate",
            query: sparql(
                "SELECT DISTINCT ?s ?l WHERE { ?s ex:knows ex:p1 . ?s ex:age ?v \
                 FILTER(?v > 1) ?s ex:nope ?l } ORDER BY ?l LIMIT 5",
            ),
            expected: "rows=0",
            site: STAR_CONST_ORDER_TOPK,
        },
        Case {
            name: "star top-k, unknown numeric predicate",
            query: sparql(
                "SELECT DISTINCT ?s ?l WHERE { ?s ex:knows ex:p1 . ?s ex:nope ?v \
                 FILTER(?v > 1) ?s ex:name ?l } ORDER BY ?l LIMIT 5",
            ),
            expected: "rows=0",
            site: STAR_CONST_ORDER_TOPK,
        },
        Case {
            name: "star top-k, unknown constraint predicate",
            query: sparql(
                "SELECT DISTINCT ?s ?l WHERE { ?s ex:nope ex:p1 . ?s ex:age ?v \
                 FILTER(?v > 1) ?s ex:name ?l } ORDER BY ?l LIMIT 5",
            ),
            expected: "rows=0",
            site: STAR_CONST_ORDER_TOPK,
        },
        Case {
            name: "star top-k, unknown label predicate (JSON-LD)",
            query: json!({
                "@context": ctx,
                "selectDistinct": ["?s", "?l"],
                "where": [
                    {"@id": "?s", "ex:knows": {"@id": "ex:p1"}, "ex:age": "?v", "ex:nope": "?l"},
                    ["filter", "(> ?v 1)"]
                ],
                "orderBy": "?l",
                "limit": 5
            }),
            expected: "rows=0",
            site: STAR_CONST_ORDER_TOPK,
        },
        Case {
            name: "star top-k, known predicates",
            query: sparql(
                "SELECT DISTINCT ?s ?l WHERE { ?s ex:knows ex:p1 . ?s ex:age ?v \
                 FILTER(?v > 1) ?s ex:name ?l } ORDER BY ?l LIMIT 5",
            ),
            expected: "rows=2",
            site: STAR_CONST_ORDER_TOPK,
        },
        // No rows means no groups, so the outer SUM runs over nothing: `0`,
        // not an unbound value. No JSON-LD twin: a JSON-LD subquery's
        // `groupBy` orders by its keys, and the lane declines an ordered
        // subquery.
        Case {
            name: "SUM(STRLEN(GROUP_CONCAT)), unknown predicate",
            query: sparql(
                r#"SELECT (SUM(STRLEN(?cat)) AS ?n) {
                     { SELECT (GROUP_CONCAT(?o; SEPARATOR=" ") AS ?cat)
                       { ?s ex:nope ?o } GROUP BY ?s }
                   }"#,
            ),
            expected: "n=0",
            site: SUM_STRLEN_GROUP_CONCAT,
        },
        Case {
            name: "SUM(STRLEN(GROUP_CONCAT)), known predicate",
            query: sparql(
                r#"SELECT (SUM(STRLEN(?cat)) AS ?n) {
                     { SELECT (GROUP_CONCAT(?o; SEPARATOR=" ") AS ?cat)
                       { ?s ex:name ?o } GROUP BY ?s }
                   }"#,
            ),
            // "abc one" + "abd two" + "xyz three", one value per subject.
            expected: "n=23",
            site: SUM_STRLEN_GROUP_CONCAT,
        },
    ]
}

fn summarize(results: &Value) -> String {
    let bindings = results["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("no results.bindings in {results}"));
    if bindings.len() == 1 {
        if let Some(v) = bindings[0].get("n").and_then(|c| c["value"].as_str()) {
            return format!("n={v}");
        }
    }
    format!("rows={}", bindings.len())
}

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

async fn imported_ledger(dir: &TempDir, name: &str, ttl: &str) -> (fluree_db_api::Fluree, String) {
    let data_dir = TempDir::new().expect("data tmpdir");
    let mut f = std::fs::File::create(data_dir.path().join("00-fixture.ttl")).expect("create ttl");
    f.write_all(ttl.as_bytes()).expect("write ttl");
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

#[tokio::test(flavor = "current_thread")]
async fn fast_paths_answer_an_unknown_predicate_with_no_rows() {
    // The kill switch OR's with this env var; with it set, the fast-path phase
    // below runs generically and pins nothing.
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set; unset it."
    );
    let _guard = FastPathGuard;

    let db_dir = TempDir::new().expect("db tmpdir");
    let (fluree, ledger_id) = imported_ledger(&db_dir, "unknown-pred", DATA).await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let run = |c: &Case| {
        let query = c.query.clone();
        let (fluree, ledger) = (&fluree, &ledger);
        async move {
            let db = fluree_db_api::GraphDb::from_ledger_state(ledger);
            let result = match &query {
                Value::String(q) => fluree.query(&db, q.as_str()).await,
                other => fluree.query(&db, other).await,
            };
            match result {
                Ok(r) => match r.to_sparql_json(&ledger.snapshot) {
                    Ok(json) => summarize(&json),
                    Err(e) => format!("ERR:{e}"),
                },
                Err(e) => format!("ERR:{e}"),
            }
        }
    };
    let cases = cases();

    let (store, tracing_guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);
    let mut fast = Vec::new();
    let mut proceeded: Vec<Vec<String>> = Vec::new();
    for c in &cases {
        let before = store.find_events("fast-path outcome").len();
        fast.push(run(c).await);
        proceeded.push(
            store.find_events("fast-path outcome")[before..]
                .iter()
                .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
                .filter_map(|e| e.fields.get("site").cloned())
                .collect(),
        );
    }
    drop(tracing_guard);

    set_fast_paths_disabled(true);
    let mut generic = Vec::new();
    for c in &cases {
        generic.push(run(c).await);
    }
    set_fast_paths_disabled(false);

    let mut failures = Vec::new();
    for (i, c) in cases.iter().enumerate() {
        if fast[i] != c.expected {
            failures.push(format!(
                "{}: fast lane returned {}, expected {} [proceeded: {:?}]",
                c.name, fast[i], c.expected, proceeded[i]
            ));
        }
        if generic[i] != c.expected {
            failures.push(format!(
                "{}: generic pipeline returned {}, expected {}",
                c.name, generic[i], c.expected
            ));
        }
        if !proceeded[i].iter().any(|s| s == c.site) {
            failures.push(format!(
                "{}: expected `{}` to proceed [proceeded: {:?}]",
                c.name, c.site, proceeded[i]
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
