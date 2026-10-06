//! Fast paths over `xsd:double` special values give the general pipeline's
//! answers, which follow SPARQL's value semantics: `INF` is above and `-INF`
//! below every other number, no comparison holds for NaN (SPARQL 1.1 §17.3,
//! F&O 3.1 §4.3.2), and MIN/MAX follow ORDER BY, which places NaN after `INF`
//! (§18.5.1.5, §18.5.1.6).
//!
//! Own test binary: toggles the process-global fast-path kill switch and
//! asserts routing through span capture.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, LedgerState, ReindexOptions};
use serde_json::{json, Value};

const EX: &str = "http://example.org/";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const PREFIXES: &str =
    "PREFIX ex: <http://example.org/>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

const COUNT_COMPARE: &str = "COUNT rows numeric compare";
const MIN_MAX: &str = "MIN/MAX string";
const TOP_K: &str = "star-const-order-topk";

/// `(subject, xsd:double lexical)`. Each subject also has `ex:kind ex:K`, its
/// own name as `ex:label`, and the integer 1 as `ex:n`.
const FINITE: &[(&str, &str)] = &[("neg", "-1.5"), ("zero", "0"), ("pos", "2.5")];
const SPECIAL: &[(&str, &str)] = &[
    ("ninf", "-INF"),
    ("inf", "INF"),
    ("nan", "NaN"),
    ("nan2", "NaN"),
];

fn turtle(values: &[(&str, &str)]) -> String {
    let mut ttl = format!("@prefix ex: <{EX}> .\n@prefix xsd: <{XSD}> .\n");
    for (s, v) in values {
        ttl.push_str(&format!(
            "ex:{s} ex:v \"{v}\"^^xsd:double ; ex:n 1 ; ex:kind ex:K ; ex:label \"{s}\" .\n"
        ));
    }
    ttl
}

async fn insert(fluree: &Fluree, ledger_id: &str, values: &[(&str, &str)]) {
    fluree
        .graph(ledger_id)
        .transact()
        .insert_turtle(&turtle(values))
        .commit()
        .await
        .expect("insert");
}

async fn reindex(fluree: &Fluree, ledger_id: &str) {
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("full rebuild");
}

/// Every value indexed.
async fn indexed(fluree: &Fluree) -> &'static str {
    let ledger_id = "double-fast-paths:indexed";
    fluree.create_ledger(ledger_id).await.expect("create");
    insert(fluree, ledger_id, FINITE).await;
    insert(fluree, ledger_id, SPECIAL).await;
    reindex(fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    assert!(ledger.snapshot.range_provider.is_some());
    assert_eq!(ledger.snapshot.t, ledger.t(), "every value is indexed");
    ledger_id
}

/// The finite values indexed, the special values in novelty over the index.
async fn overlay(fluree: &Fluree) -> &'static str {
    let ledger_id = "double-fast-paths:overlay";
    fluree.create_ledger(ledger_id).await.expect("create");
    insert(fluree, ledger_id, FINITE).await;
    reindex(fluree, ledger_id).await;
    insert(fluree, ledger_id, SPECIAL).await;
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    assert!(ledger.snapshot.range_provider.is_some());
    assert!(ledger.snapshot.t < ledger.t(), "special values in novelty");
    ledger_id
}

struct Case {
    name: &'static str,
    sparql: &'static str,
    jsonld: Value,
    /// The projected variable whose values are the answer.
    var: &'static str,
    expected: &'static [&'static str],
    /// The fast path that must serve it on the indexed ledger.
    site: &'static str,
}

fn count_where(filter: &str) -> Value {
    json!({
        "select": ["(as (count ?s) ?out)"],
        "where": [{"@id": "?s", "ex:v": "?v"}, ["filter", filter]]
    })
}

fn top_k_where(property: &str, filter: &str) -> Value {
    json!({
        "selectDistinct": ["?s", "?label"],
        "where": [
            {"@id": "?s", "ex:kind": {"@id": "ex:K"}, property: "?v", "ex:label": "?label"},
            ["filter", filter]
        ],
        "orderBy": ["?label"],
        "limit": 10
    })
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "COUNT of > 0",
            sparql: "SELECT (COUNT(?s) AS ?out) WHERE { ?s ex:v ?v FILTER(?v > 0) }",
            jsonld: count_where("(> ?v 0)"),
            var: "out",
            expected: &["2"],
            site: COUNT_COMPARE,
        },
        Case {
            name: "COUNT of < 0",
            sparql: "SELECT (COUNT(?s) AS ?out) WHERE { ?s ex:v ?v FILTER(?v < 0) }",
            jsonld: count_where("(< ?v 0)"),
            var: "out",
            expected: &["2"],
            site: COUNT_COMPARE,
        },
        Case {
            name: "COUNT of >= -INF: every number but NaN",
            sparql: r#"SELECT (COUNT(?s) AS ?out) WHERE { ?s ex:v ?v FILTER(?v >= "-INF"^^xsd:double) }"#,
            jsonld: count_where("(>= ?v -INF)"),
            var: "out",
            expected: &["5"],
            site: COUNT_COMPARE,
        },
        Case {
            name: "COUNT of <= INF: every number but NaN",
            sparql: r#"SELECT (COUNT(?s) AS ?out) WHERE { ?s ex:v ?v FILTER(?v <= "INF"^^xsd:double) }"#,
            jsonld: count_where("(<= ?v INF)"),
            var: "out",
            expected: &["5"],
            site: COUNT_COMPARE,
        },
        Case {
            name: "COUNT of > INF",
            sparql: r#"SELECT (COUNT(?s) AS ?out) WHERE { ?s ex:v ?v FILTER(?v > "INF"^^xsd:double) }"#,
            jsonld: count_where("(> ?v INF)"),
            var: "out",
            expected: &["0"],
            site: COUNT_COMPARE,
        },
        Case {
            name: "COUNT of < NaN: no comparison holds for NaN",
            sparql: r#"SELECT (COUNT(?s) AS ?out) WHERE { ?s ex:v ?v FILTER(?v < "NaN"^^xsd:double) }"#,
            jsonld: count_where("(< ?v NaN)"),
            var: "out",
            expected: &["0"],
            site: COUNT_COMPARE,
        },
        Case {
            name: "MIN",
            sparql: "SELECT (MIN(?v) AS ?out) WHERE { ?s ex:v ?v }",
            jsonld: json!({"select": ["(as (min ?v) ?out)"], "where": {"@id": "?s", "ex:v": "?v"}}),
            var: "out",
            expected: &["-INF"],
            site: MIN_MAX,
        },
        Case {
            name: "MAX: NaN, which ORDER BY places after INF",
            sparql: "SELECT (MAX(?v) AS ?out) WHERE { ?s ex:v ?v }",
            jsonld: json!({"select": ["(as (max ?v) ?out)"], "where": {"@id": "?s", "ex:v": "?v"}}),
            var: "out",
            expected: &["NaN"],
            site: MIN_MAX,
        },
        Case {
            name: "top-k labels of > 0",
            sparql:
                "SELECT DISTINCT ?s ?label WHERE { ?s ex:kind ex:K . ?s ex:v ?v FILTER(?v > 0) \
                     ?s ex:label ?label } ORDER BY ?label LIMIT 10",
            jsonld: top_k_where("ex:v", "(> ?v 0)"),
            var: "label",
            expected: &["inf", "pos"],
            site: TOP_K,
        },
        Case {
            name: "top-k labels of > -INF: NaN is not above it",
            sparql: r#"SELECT DISTINCT ?s ?label WHERE { ?s ex:kind ex:K . ?s ex:v ?v
                       FILTER(?v > "-INF"^^xsd:double) ?s ex:label ?label } ORDER BY ?label LIMIT 10"#,
            jsonld: top_k_where("ex:v", "(> ?v -INF)"),
            var: "label",
            expected: &["inf", "neg", "pos", "zero"],
            site: TOP_K,
        },
        Case {
            name: "top-k labels of > NaN",
            sparql: r#"SELECT DISTINCT ?s ?label WHERE { ?s ex:kind ex:K . ?s ex:v ?v
                       FILTER(?v > "NaN"^^xsd:double) ?s ex:label ?label } ORDER BY ?label LIMIT 10"#,
            jsonld: top_k_where("ex:v", "(> ?v NaN)"),
            var: "label",
            expected: &[],
            site: TOP_K,
        },
        Case {
            name: "top-k labels of integers > NaN",
            sparql: r#"SELECT DISTINCT ?s ?label WHERE { ?s ex:kind ex:K . ?s ex:n ?v
                       FILTER(?v > "NaN"^^xsd:double) ?s ex:label ?label } ORDER BY ?label LIMIT 10"#,
            jsonld: top_k_where("ex:n", "(> ?v NaN)"),
            var: "label",
            expected: &[],
            site: TOP_K,
        },
        Case {
            name: "top-k labels of integers > -INF",
            sparql: r#"SELECT DISTINCT ?s ?label WHERE { ?s ex:kind ex:K . ?s ex:n ?v
                       FILTER(?v > "-INF"^^xsd:double) ?s ex:label ?label } ORDER BY ?label LIMIT 10"#,
            jsonld: top_k_where("ex:n", "(> ?v -INF)"),
            var: "label",
            expected: &["inf", "nan", "nan2", "neg", "ninf", "pos", "zero"],
            site: TOP_K,
        },
    ]
}

/// A double's token (`NaN`, `INF`, `-INF` or its shortest decimal form), or
/// a literal's lexical form.
fn token(lexical: &str, double: bool) -> String {
    if !double || matches!(lexical, "NaN" | "INF" | "-INF") {
        return lexical.to_string();
    }
    let value: f64 = lexical.parse().expect("numeral");
    format!("{value}")
}

async fn sparql_answer(fluree: &Fluree, ledger: &LedgerState, case: &Case) -> Vec<String> {
    let db = fluree_db_api::GraphDb::from_ledger_state(ledger);
    let out = fluree
        .query(&db, format!("{PREFIXES}{}", case.sparql).as_str())
        .await
        .unwrap_or_else(|e| panic!("{}: SPARQL: {e}", case.name))
        .to_sparql_json(&ledger.snapshot)
        .expect("SPARQL JSON");
    let double = format!("{XSD}double");
    out["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|solution| match solution.get(case.var) {
            None => "unbound".to_string(),
            Some(term) => token(
                term["value"].as_str().expect("value"),
                term["datatype"].as_str() == Some(double.as_str()),
            ),
        })
        .collect()
}

async fn jsonld_answer(fluree: &Fluree, ledger: &LedgerState, case: &Case) -> Vec<String> {
    let mut query = case.jsonld.clone();
    query["@context"] = json!({"ex": EX, "xsd": XSD});
    let db = fluree_db_api::GraphDb::from_ledger_state(ledger);
    let out = fluree
        .query(&db, &query)
        .await
        .unwrap_or_else(|e| panic!("{}: JSON-LD: {e}", case.name))
        .to_jsonld(&ledger.snapshot)
        .expect("JSON-LD");
    // The answer is the last selected value of each row.
    out.as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            let cell = match row {
                Value::Array(cells) => cells.last().expect("cell"),
                cell => cell,
            };
            match cell {
                Value::Null => "unbound".to_string(),
                Value::String(s) => s.clone(),
                Value::Number(n) => n
                    .as_i64()
                    .map(|i| i.to_string())
                    .unwrap_or_else(|| format!("{}", n.as_f64().expect("number"))),
                other => panic!("{}: unexpected cell {other}", case.name),
            }
        })
        .collect()
}

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn fast_paths_over_special_doubles_match_the_general_pipeline() {
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set, so the fast-path phase would run \
         generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;
    let dir = tempfile::tempdir().expect("tmpdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("file-backed Fluree");
    let ledgers = [
        ("indexed", indexed(&fluree).await),
        ("novelty over an index", overlay(&fluree).await),
    ];
    let cases = cases();
    let mut failures: Vec<String> = Vec::new();

    for (fast_paths, disabled) in [("fast paths on", false), ("fast paths off", true)] {
        set_fast_paths_disabled(disabled);
        for (lane, ledger_id) in ledgers {
            let ledger = fluree.ledger(ledger_id).await.expect("load");
            for case in &cases {
                let (store, tracing_guard) = span_capture::init_test_tracing();
                let sparql = sparql_answer(&fluree, &ledger, case).await;
                let jsonld = jsonld_answer(&fluree, &ledger, case).await;
                let proceeded: Vec<String> = store
                    .find_events("fast-path outcome")
                    .iter()
                    .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
                    .filter_map(|e| e.fields.get("site").cloned())
                    .collect();
                drop(tracing_guard);

                for (surface, answer) in [("SPARQL", &sparql), ("JSON-LD", &jsonld)] {
                    if answer.as_slice() != case.expected {
                        failures.push(format!(
                            "{fast_paths}, {lane}, {surface}: {}: expected {:?}, got {answer:?} \
                             [proceeded: {proceeded:?}]",
                            case.name, case.expected
                        ));
                    }
                }
                let fired = proceeded.iter().filter(|s| *s == case.site).count();
                if !disabled && lane == "indexed" && fired != 2 {
                    failures.push(format!(
                        "{lane}: {}: `{}` must serve both surfaces; it served {fired} \
                         [proceeded: {proceeded:?}]",
                        case.name, case.site
                    ));
                }
                if disabled && fired != 0 {
                    failures.push(format!(
                        "{lane}: {}: `{}` served a query with fast paths off",
                        case.name, case.site
                    ));
                }
            }
        }
    }
    set_fast_paths_disabled(false);

    assert!(
        failures.is_empty(),
        "{} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
