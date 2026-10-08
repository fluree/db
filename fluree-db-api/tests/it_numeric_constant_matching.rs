//! Numeric constants in triple patterns: a typed literal matches its own
//! datatype, a bare number matches every numeric datatype holding an equal
//! value — the same answer on every lane and on both query surfaces
//! (fluree/db#1737).
//!
//! Before: the JSON-LD parser rewrote a constant's `@type` to `xsd:integer` /
//! `xsd:double`, and matching treated those two as covering their numeric
//! family, so a typed constant was lenient in novelty and hit the wrong
//! o_type once indexed (`{"@value":"25","@type":"xsd:long"}` returned the
//! `xsd:integer` row). A bare number seeked a single o_type once indexed, so
//! `25` lost the `xsd:long`/`xsd:int`/`xsd:double` rows it matched in
//! novelty, and the COUNT fast path counted only `xsd:integer`. Under a
//! policy, the range provider kept only one numeric family's key.
//!
//! Own test binary: toggles the process-global fast-path kill switch and
//! asserts fast-path routing via span capture.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{
    set_fast_paths_disabled, Fluree, FlureeBuilder, GovernanceOptions, GraphDb, QueryInput,
    TimeSpec,
};
use serde_json::{json, Value};

const LEDGER: &str = "numeric-constants:main";
const EX: &str = "http://example.org/ns/";
const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
const COUNT_SITE: &str = "predicate_object_count";
const POLICY_SCAN_SITE: &str = "policy_predicate_scan";
const SEEK_SITE: &str = "bare_number_seek";

fn ctx() -> Value {
    json!({"ex": "http://example.org/ns/", "xsd": "http://www.w3.org/2001/XMLSchema#"})
}

fn typed(value: &str, datatype: &str) -> Value {
    json!({"@value": value, "@type": format!("xsd:{datatype}")})
}

fn data() -> Value {
    json!({"@context": ctx(), "@graph": [
        {"@id": "ex:a", "ex:v": 25},
        {"@id": "ex:b", "ex:v": typed("25", "int")},
        {"@id": "ex:c", "ex:v": typed("25", "long")},
        {"@id": "ex:d", "ex:v": typed("25.0", "double")},
        {"@id": "ex:e", "ex:v": typed("26", "long")},
        {"@id": "ex:j", "ex:v": typed("25", "decimal")},
        // Its NumBig arena handle is the same number as `ex:j`'s.
        {"@id": "ex:k", "ex:dec": typed("7.5", "decimal")},
        {"@id": "ex:f", "ex:w": typed("1.5", "float")},
        {"@id": "ex:g", "ex:w": typed("1.5", "double")},
        {"@id": "ex:h", "ex:only": typed("25", "long")}
    ]})
}

/// Written once the index exists: a numeric datatype `ex:v` has not held
/// before, so only novelty-aware datatype stats can route a seek to it.
fn novelty_over_index_data() -> Value {
    json!({"@context": ctx(), "@id": "ex:i", "ex:v": typed("25", "short")})
}

struct Case {
    name: &'static str,
    sparql: String,
    /// `None` where JSON-LD can't express the shape.
    jsonld: Option<Value>,
    /// A `COUNT`, answered by the `predicate_object_count` fast path once indexed.
    count: bool,
    /// The constant is a bare integer or double on both surfaces, so an indexed
    /// scan must seek its slices rather than walk.
    seeks: bool,
    /// Subjects on the novelty and indexed lanes; `ex:i` joins the bare `ex:v`
    /// matches once it is written over the index.
    expected: &'static [&'static str],
    matches_short: bool,
}

fn case(
    name: &'static str,
    sparql_object: &str,
    jsonld_object: Value,
    predicate: &str,
    expected: &'static [&'static str],
    matches_short: bool,
) -> Case {
    Case {
        name,
        sparql: format!("SELECT ?s WHERE {{ ?s ex:{predicate} {sparql_object} }}"),
        jsonld: Some(json!({
            "@context": ctx(),
            "select": ["?s"],
            "where": {"@id": "?s", format!("ex:{predicate}"): jsonld_object}
        })),
        expected,
        matches_short,
        count: false,
        // SPARQL reads `25.0` as a decimal; `25` and `25.0e0` are bare numbers.
        seeks: !sparql_object.contains("^^")
            && (!sparql_object.contains('.') || sparql_object.contains('e')),
    }
}

fn cases() -> Vec<Case> {
    const BARE_25: &[&str] = &["ex:a", "ex:b", "ex:c", "ex:d", "ex:j"];
    vec![
        case("bare 25", "25", json!(25), "v", BARE_25, true),
        case("bare 25.0", "25.0e0", json!(25.0), "v", BARE_25, true),
        case("bare decimal 25.0", "25.0", json!(25.0), "v", BARE_25, true),
        case(
            "typed integer",
            r#""25"^^xsd:integer"#,
            typed("25", "integer"),
            "v",
            &["ex:a"],
            false,
        ),
        case(
            "typed int",
            r#""25"^^xsd:int"#,
            typed("25", "int"),
            "v",
            &["ex:b"],
            false,
        ),
        case(
            "typed long",
            r#""25"^^xsd:long"#,
            typed("25", "long"),
            "v",
            &["ex:c"],
            false,
        ),
        case(
            "typed double",
            r#""25"^^xsd:double"#,
            typed("25", "double"),
            "v",
            &["ex:d"],
            false,
        ),
        case(
            "typed float",
            r#""1.5"^^xsd:float"#,
            typed("1.5", "float"),
            "w",
            &["ex:f"],
            false,
        ),
        case(
            "bare 1.5",
            "1.5e0",
            json!(1.5),
            "w",
            &["ex:f", "ex:g"],
            false,
        ),
        case(
            "bare 25.0, one datatype",
            "25.0e0",
            json!(25.0),
            "only",
            &["ex:h"],
            false,
        ),
        Case {
            name: "bare 25, any predicate",
            sparql: "SELECT ?s WHERE { ?s ?p 25 }".to_string(),
            // JSON-LD rejects a constant object under a variable predicate.
            jsonld: None,
            expected: &["ex:a", "ex:b", "ex:c", "ex:d", "ex:h", "ex:j"],
            matches_short: true,
            count: false,
            seeks: true,
        },
        Case {
            name: "bare 7.5, any predicate",
            sparql: "SELECT ?s WHERE { ?s ?p 7.5e0 }".to_string(),
            jsonld: None,
            expected: &["ex:k"],
            matches_short: false,
            count: false,
            seeks: true,
        },
        Case {
            name: "COUNT bare 25",
            sparql: "SELECT (COUNT(?s) AS ?n) WHERE { ?s ex:v 25 }".to_string(),
            jsonld: None,
            expected: &["5"],
            matches_short: true,
            count: true,
            seeks: true,
        },
    ]
}

fn expected_for(case: &Case, lane: &str) -> Vec<String> {
    if lane.starts_with("novelty over index") && case.matches_short {
        if case.count {
            return vec!["6".to_string()];
        }
        let mut subjects: Vec<String> = case.expected.iter().map(|s| (*s).to_string()).collect();
        subjects.push("ex:i".to_string());
        subjects.sort();
        return subjects;
    }
    case.expected.iter().map(|s| (*s).to_string()).collect()
}

/// First column of every row, as strings, sorted.
fn first_column(rows: &Value) -> Vec<String> {
    let mut out: Vec<String> = rows
        .as_array()
        .expect("array of rows")
        .iter()
        .map(|row| {
            let cell = if row.is_array() { &row[0] } else { row };
            match cell {
                Value::String(s) => s.replace(EX, "ex:"),
                other => other.to_string(),
            }
        })
        .collect();
    out.sort();
    out
}

/// How a lane reads the ledger.
enum View {
    Latest,
    /// Under a policy that may touch every predicate the cases scan but hides
    /// nothing, so every scan takes the policy-filtered range fallback.
    Policy,
    AtT(i64),
}

async fn open_view(fluree: &Fluree, view: &View) -> GraphDb {
    match view {
        View::Latest => fluree.graph(LEDGER).load().await.expect("load").into_db(),
        View::Policy => {
            let on_property = ["v", "w", "only"].map(|p| json!({"@id": format!("{EX}{p}")}));
            fluree
                .db_with_policy(
                    LEDGER,
                    &GovernanceOptions {
                        policy: Some(json!([{
                            "@id": format!("{EX}allowNumbers"),
                            "f:action": "f:view",
                            "f:onProperty": on_property,
                            "f:allow": true
                        }])),
                        default_allow: Some(true),
                        ..Default::default()
                    },
                )
                .await
                .expect("db_with_policy")
        }
        View::AtT(t) => fluree
            .graph_at(LEDGER, TimeSpec::AtT(*t))
            .load()
            .await
            .expect("load at t")
            .into_db(),
    }
}

async fn run(fluree: &Fluree, db: &GraphDb, query: QueryInput<'_>) -> Vec<String> {
    let label = format!("{query:?}");
    let result = fluree
        .query(db, query)
        .await
        .unwrap_or_else(|e| panic!("{label}: {e}"));
    first_column(&result.to_jsonld(&db.snapshot).expect("jsonld"))
}

/// Outcomes stamped on `site` since `before`.
fn outcomes(store: &span_capture::SpanStore, before: usize, site: &str) -> Vec<String> {
    store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect()
}

/// Every case on both surfaces, with fast paths on and off. Returns failures.
async fn check_lane(
    fluree: &Fluree,
    lane: &str,
    view: View,
    store: &span_capture::SpanStore,
) -> Vec<String> {
    let db = open_view(fluree, &view).await;
    let indexed = lane != "novelty";
    let policed = matches!(view, View::Policy);
    let mut failures = Vec::new();
    for c in cases() {
        let expected = expected_for(&c, lane);
        for fast_paths in [true, false] {
            set_fast_paths_disabled(!fast_paths);
            let lane_name = if fast_paths { "fast" } else { "generic" };
            let sparql = format!("{PREFIX}{}", c.sparql);
            let mut surfaces = vec![("SPARQL", QueryInput::Sparql(&sparql))];
            if let Some(q) = &c.jsonld {
                surfaces.push(("JSON-LD", QueryInput::JsonLd(q)));
            }
            for (surface, query) in surfaces {
                let before = store.find_events("fast-path outcome").len();
                let got = run(fluree, &db, query).await;
                if got != expected {
                    failures.push(format!(
                        "{lane} / {lane_name} / {surface} {}: got {got:?}, expected {expected:?}",
                        c.name
                    ));
                }
                // The COUNT must be served by its fast path wherever that path
                // applies (a fully indexed ledger), or this case pins nothing.
                let count_proceeded =
                    outcomes(store, before, COUNT_SITE).contains(&"proceed".to_string());
                if c.count && fast_paths && lane == "indexed" && !count_proceeded {
                    failures.push(format!(
                        "{lane} / SPARQL {}: `{COUNT_SITE}` did not proceed",
                        c.name
                    ));
                }
                // A scan over the index must seek a bare number, whatever the
                // predicate: a walk returns the same rows, only slower.
                if c.seeks && indexed && !policed && !count_proceeded {
                    let seen = outcomes(store, before, SEEK_SITE);
                    if seen.is_empty() || seen.iter().any(|o| o != "proceed") {
                        failures.push(format!(
                            "{lane} / {lane_name} / {surface} {}: `{SEEK_SITE}` must proceed, saw {seen:?}",
                            c.name
                        ));
                    }
                }
                // Likewise the policy lane must reach the range fallback.
                if policed {
                    let seen = outcomes(store, before, POLICY_SCAN_SITE);
                    if seen.is_empty() || seen.iter().any(|o| o != "fallback:gate_declined") {
                        failures.push(format!(
                            "{lane} / {surface} {}: `{POLICY_SCAN_SITE}` must decline, saw {seen:?}",
                            c.name
                        ));
                    }
                }
            }
        }
        set_fast_paths_disabled(false);
    }
    failures
}

struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn numeric_constants_match_the_same_rows_on_every_lane_and_surface() {
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast-path runs would be generic"
    );
    let _guard = FastPathGuard;
    let (store, _tracing_guard) = span_capture::init_test_tracing();

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    let ledger = fluree.create_ledger(LEDGER).await.expect("create");
    fluree.insert(ledger, &data()).await.expect("insert");

    let mut failures = check_lane(&fluree, "novelty", View::Latest, &store).await;

    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");
    let indexed = fluree.ledger(LEDGER).await.expect("load");
    assert!(
        indexed.snapshot.range_provider.is_some(),
        "indexed lane setup"
    );
    assert_eq!(
        indexed.snapshot.t,
        indexed.t(),
        "indexed lane has no novelty"
    );
    failures.extend(check_lane(&fluree, "indexed", View::Latest, &store).await);
    failures.extend(check_lane(&fluree, "indexed, policy", View::Policy, &store).await);

    fluree
        .insert(indexed, &novelty_over_index_data())
        .await
        .expect("insert over index");
    failures.extend(check_lane(&fluree, "novelty over index", View::Latest, &store).await);
    failures.extend(check_lane(&fluree, "novelty over index, policy", View::Policy, &store).await);

    // A read below the index's max_t can't use NumBig arena handles.
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex over novelty");
    failures.extend(check_lane(&fluree, "time travel", View::AtT(1), &store).await);

    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}
