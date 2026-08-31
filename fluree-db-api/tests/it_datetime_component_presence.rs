//! A calendar field a value does not carry must answer unbound, not filler.
//!
//! `xsd:gYear` has a year and nothing else, so `DAY("2005"^^xsd:gYear)` has no
//! answer in the data. The engine used to promote every temporal value to a
//! whole instant and read the field off the promoted result, so absent fields
//! came back as the promotion's filler — day 1, month 1, and (from the Unix
//! epoch) year 1970 — indistinguishable in the answer from a real value.
//!
//! The filler is close to harmless on a predicate that is entirely year-only
//! (the shape reported as a side observation on #1652: a nonsense query getting
//! a nonsense number). It is not harmless on a predicate mixing `xsd:date` with
//! `xsd:gYear`, which is ordinary in bibliographic data where some records are
//! dated and some are year-only. There the year-only rows contributed a
//! fabricated day 1 each, so `AVG(DAY(?o))` was dragged toward 1,
//! `COUNT(DAY(?o))` counted rows that have no day, and — worst — a
//! `FILTER(DAY(?o) < 15)` selected exactly the rows with no day at all and
//! excluded every row that had one.
//!
//! `xsd:date` keeps its time-of-day fields at the midnight the XSD timeline
//! maps it to: that one is a documented convention rather than invented data,
//! and it is pinned below so the narrowing cannot quietly widen.
//!
//! Every case also runs under the fast-path kill switch, because three separate
//! lanes read these fields — the per-row evaluator, its no-chrono fast
//! extractor, and the fused `SUM(DAY(?o))` scan that folds a homogeneous
//! leaflet with no column IO. They previously each carried their own copy of
//! the defaults; the point of the shared table in `fluree-db-core::temporal` is
//! that they cannot drift, and fast-vs-generic agreement here is what proves it.
//!
//! Own test binary: toggles the process-global fast-path kill switch.

#![cfg(feature = "native")]

use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{
    set_fast_paths_disabled, CommitOpts, Fluree, FlureeBuilder, FormatterConfig, IndexConfig,
    TxnOpts,
};
use serde_json::Value;

mod support;

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

const DATA: &str = r#"
@prefix ex: <http://example.org/ns/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:a ex:gyear "2005"^^xsd:gYear .
ex:b ex:gyearmonth "2005-07"^^xsd:gYearMonth .
ex:c ex:gmonth "--03"^^xsd:gMonth .
ex:d ex:gday "---17"^^xsd:gDay .
ex:e ex:gmonthday "--03-17"^^xsd:gMonthDay .
ex:f ex:date "2005-07-17"^^xsd:date .
ex:g ex:datetime "2005-07-17T13:45:00Z"^^xsd:dateTime .
ex:h ex:time "13:45:00"^^xsd:time .

# Mixed predicate: three dated records, three year-only ones. The dated days
# are 17, 20 and 30 — all >= 15, so a `DAY(?o) < 15` filter must select none.
ex:m1 ex:pubdate "2005-07-17"^^xsd:date .
ex:m2 ex:pubdate "2005-08-20"^^xsd:date .
ex:m3 ex:pubdate "2005-09-30"^^xsd:date .
ex:m4 ex:pubdate "2005"^^xsd:gYear .
ex:m5 ex:pubdate "2006"^^xsd:gYear .
ex:m6 ex:pubdate "2007"^^xsd:gYear .
"#;

/// Routing assertion against the engine's `fast-path outcome` stamps.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Routing {
    /// No routing claim — the case is about the answer, not the lane.
    Any,
    /// This site must `proceed`. Pins that the fused lane still serves the
    /// shape: its fallback computes the same number, so a silent disable is
    /// invisible to the value assertions alone.
    MustFire(&'static str),
}

struct Case {
    name: &'static str,
    sparql: &'static str,
    /// Rows as `normalize` renders them. `null` is an unbound field.
    expected: &'static str,
    routing: Routing,
}

fn cases() -> Vec<Case> {
    use Routing::{Any, MustFire};
    vec![
        // ---- carried fields stay exactly as they were ----------------------
        Case {
            name: "YEAR(gYear) is carried",
            sparql: "SELECT (YEAR(?o) AS ?v) WHERE { ?s ex:gyear ?o }",
            expected: "[[2005]]",
            routing: Any,
        },
        Case {
            name: "YEAR+MONTH(gYearMonth) are carried",
            sparql: "SELECT (YEAR(?o) AS ?y) (MONTH(?o) AS ?m) WHERE { ?s ex:gyearmonth ?o }",
            expected: "[[2005,7]]",
            routing: Any,
        },
        Case {
            name: "MONTH(gMonth) is carried",
            sparql: "SELECT (MONTH(?o) AS ?v) WHERE { ?s ex:gmonth ?o }",
            expected: "[[3]]",
            routing: Any,
        },
        Case {
            name: "DAY(gDay) is carried",
            sparql: "SELECT (DAY(?o) AS ?v) WHERE { ?s ex:gday ?o }",
            expected: "[[17]]",
            routing: Any,
        },
        Case {
            name: "MONTH+DAY(gMonthDay) are carried",
            sparql: "SELECT (MONTH(?o) AS ?m) (DAY(?o) AS ?d) WHERE { ?s ex:gmonthday ?o }",
            expected: "[[3,17]]",
            routing: Any,
        },
        Case {
            name: "YEAR+MONTH+DAY(date) are carried",
            sparql: "SELECT (YEAR(?o) AS ?y) (MONTH(?o) AS ?m) (DAY(?o) AS ?d) WHERE { ?s ex:date ?o }",
            expected: "[[2005,7,17]]",
            routing: Any,
        },
        Case {
            name: "all fields of dateTime are carried",
            sparql: "SELECT (YEAR(?o) AS ?y) (DAY(?o) AS ?d) (HOURS(?o) AS ?h) (MINUTES(?o) AS ?mi) WHERE { ?s ex:datetime ?o }",
            expected: "[[2005,17,13,45]]",
            routing: Any,
        },
        Case {
            name: "HOURS+MINUTES(time) are carried",
            sparql: "SELECT (HOURS(?o) AS ?h) (MINUTES(?o) AS ?m) WHERE { ?s ex:time ?o }",
            expected: "[[13,45]]",
            routing: Any,
        },
        // xsd:date -> midnight is a kept convention, pinned so it can't widen.
        Case {
            name: "HOURS(date) keeps the midnight convention",
            sparql: "SELECT (HOURS(?o) AS ?h) (MINUTES(?o) AS ?m) WHERE { ?s ex:date ?o }",
            expected: "[[0,0]]",
            routing: Any,
        },
        // ---- absent fields are unbound, not filler -------------------------
        Case {
            name: "MONTH/DAY(gYear) are absent",
            sparql: "SELECT (MONTH(?o) AS ?m) (DAY(?o) AS ?d) WHERE { ?s ex:gyear ?o }",
            expected: "[[null,null]]",
            routing: Any,
        },
        Case {
            name: "DAY(gYearMonth) is absent",
            sparql: "SELECT (DAY(?o) AS ?d) WHERE { ?s ex:gyearmonth ?o }",
            expected: "[[null]]",
            routing: Any,
        },
        // The epoch's 1970 was the most clearly invented of the fillers.
        Case {
            name: "YEAR(gMonth) is absent, not 1970",
            sparql: "SELECT (YEAR(?o) AS ?y) (DAY(?o) AS ?d) WHERE { ?s ex:gmonth ?o }",
            expected: "[[null,null]]",
            routing: Any,
        },
        Case {
            name: "YEAR(gDay) is absent, not 1970",
            sparql: "SELECT (YEAR(?o) AS ?y) (MONTH(?o) AS ?m) WHERE { ?s ex:gday ?o }",
            expected: "[[null,null]]",
            routing: Any,
        },
        Case {
            name: "YEAR(gMonthDay) is absent, not 1970",
            sparql: "SELECT (YEAR(?o) AS ?y) WHERE { ?s ex:gmonthday ?o }",
            expected: "[[null]]",
            routing: Any,
        },
        Case {
            name: "date fields of time are absent, not 1970",
            sparql: "SELECT (YEAR(?o) AS ?y) (MONTH(?o) AS ?m) (DAY(?o) AS ?d) WHERE { ?s ex:time ?o }",
            expected: "[[null,null,null]]",
            routing: Any,
        },
        // ---- the mixed predicate: what the filler actually cost ------------
        // Days present: 17, 20, 30. The three year-only rows have none.
        Case {
            name: "MIXED AVG(DAY) ignores rows with no day",
            sparql: "SELECT (AVG(DAY(?o)) AS ?v) WHERE { ?s ex:pubdate ?o }",
            expected: "[[\"22.33333333333333333333333333333333\"]]",
            routing: Any,
        },
        Case {
            name: "MIXED SUM(DAY) ignores rows with no day",
            sparql: "SELECT (SUM(DAY(?o)) AS ?v) WHERE { ?s ex:pubdate ?o }",
            expected: "[[67]]",
            routing: Any,
        },
        Case {
            name: "MIXED COUNT(DAY) counts only rows that have one",
            sparql: "SELECT (COUNT(DAY(?o)) AS ?v) WHERE { ?s ex:pubdate ?o }",
            expected: "[[3]]",
            routing: Any,
        },
        // The sharpest case: every dated row has a day >= 15, so the honest
        // answer is zero. The filler used to make this select all three
        // year-only rows — exactly the rows that cannot satisfy it.
        Case {
            name: "MIXED FILTER(DAY < 15) selects no fabricated rows",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?s ex:pubdate ?o FILTER(DAY(?o) < 15) }",
            expected: "[[0]]",
            routing: Any,
        },
        Case {
            name: "MIXED YEAR is carried by both datatypes",
            sparql: "SELECT (SUM(YEAR(?o)) AS ?v) WHERE { ?s ex:pubdate ?o }",
            expected: "[[12033]]",
            routing: Any,
        },
        // ---- SECONDS: its own evaluator, its own copy of the gate ---------
        // `eval_seconds` returns xsd:decimal and so does not route through
        // `eval_datetime_component`; it repeats the presence check itself.
        // Without these, an inverted condition there passes every pin above.
        // The decimal return is why a carried value renders as a JSON string.
        Case {
            name: "SECONDS(time) is carried",
            sparql: "SELECT (SECONDS(?o) AS ?v) WHERE { ?s ex:time ?o }",
            expected: "[[\"0\"]]",
            routing: Any,
        },
        Case {
            name: "SECONDS(dateTime) is carried",
            sparql: "SELECT (SECONDS(?o) AS ?v) WHERE { ?s ex:datetime ?o }",
            expected: "[[\"0\"]]",
            routing: Any,
        },
        Case {
            name: "SECONDS(gYear) is absent, not zero",
            sparql: "SELECT (SECONDS(?o) AS ?v) WHERE { ?s ex:gyear ?o }",
            expected: "[[null]]",
            routing: Any,
        },
        Case {
            name: "SECONDS(gMonth) is absent, not zero",
            sparql: "SELECT (SECONDS(?o) AS ?v) WHERE { ?s ex:gmonth ?o }",
            expected: "[[null]]",
            routing: Any,
        },
        // The midnight convention for the field the cases above don't cover:
        // a date carries seconds only in the sense that its instant does.
        Case {
            name: "SECONDS(date) keeps the midnight convention",
            sparql: "SELECT (SECONDS(?o) AS ?v) WHERE { ?s ex:date ?o }",
            expected: "[[\"0\"]]",
            routing: Any,
        },
        // ---- the fused SUM lane, which folds a homogeneous leaflet ---------
        // Every row is absent, so the fold contributes nothing and SPARQL's
        // empty-sum identity stands.
        Case {
            name: "fused SUM(DAY) over a year-only predicate sums nothing",
            sparql: "SELECT (SUM(DAY(?o)) AS ?v) WHERE { ?s ex:gyear ?o }",
            expected: "[[0]]",
            routing: MustFire("SUM(DAY)"),
        },
        Case {
            name: "fused SUM(YEAR) over a year-only predicate is carried",
            sparql: "SELECT (SUM(YEAR(?o)) AS ?v) WHERE { ?s ex:gyear ?o }",
            expected: "[[2005]]",
            routing: MustFire("SUM(YEAR)"),
        },
        Case {
            name: "fused SUM(YEAR) over a month-only predicate sums nothing",
            sparql: "SELECT (SUM(YEAR(?o)) AS ?v) WHERE { ?s ex:gmonth ?o }",
            expected: "[[0]]",
            routing: MustFire("SUM(YEAR)"),
        },
    ]
}

async fn setup() -> (tempfile::TempDir, Fluree, String) {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(path).build().expect("build Fluree");
    let alias = "dtpresence:main".to_string();
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            DATA,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
            None,
        )
        .await
        .expect("insert");
    // Indexed: the fused SUM lane and the encoded-literal extractor only run
    // against a persisted binary index.
    fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .expect("reindex");
    (dir, fluree, alias)
}

async fn run_query(fluree: &Fluree, alias: &str, sparql: &str) -> Value {
    let full = format!("{PREFIX}{sparql}");
    let snapshot = fluree.graph(alias).load().await.expect("load");
    snapshot
        .query()
        .sparql(&full)
        .format(FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("query {sparql}: {e}"))
}

fn normalize(rows: &Value) -> String {
    serde_json::to_string(rows).expect("serialize rows")
}

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn absent_calendar_fields_answer_unbound_on_every_lane() {
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast-path phase of this \
         test would run generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;
    let (_dir, fluree, alias) = setup().await;

    // Span capture attributes each fast-lane answer to the site that served
    // it. `FastPathOperator::open` stamps `fast-path outcome` under the
    // operator's label, so the fused scalar-agg lane is observable without any
    // instrumentation of its own.
    let (store, tracing_guard) = support::span_capture::init_test_tracing();

    let mut failures: Vec<String> = Vec::new();
    for case in cases() {
        set_fast_paths_disabled(false);
        let before = store.find_events("fast-path outcome").len();
        let fast = normalize(&run_query(&fluree, &alias, case.sparql).await);
        let proceeded: Vec<String> = store.find_events("fast-path outcome")[before..]
            .iter()
            .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
            .filter_map(|e| e.fields.get("site").cloned())
            .collect();
        set_fast_paths_disabled(true);
        let generic = normalize(&run_query(&fluree, &alias, case.sparql).await);
        set_fast_paths_disabled(false);

        if fast != case.expected {
            failures.push(format!(
                "{}: fast lane returned {fast}, expected {}",
                case.name, case.expected
            ));
        }
        if generic != case.expected {
            failures.push(format!(
                "{}: generic pipeline returned {generic}, expected {} — the \
                 hand-computed answer is wrong or the general pipeline regressed",
                case.name, case.expected
            ));
        }
        if let Routing::MustFire(site) = case.routing {
            if !proceeded.iter().any(|s| s == site) {
                failures.push(format!(
                    "{}: site `{site}` did not proceed — the fused lane stopped \
                     serving this shape and the fallback answered instead \
                     [proceeded: {proceeded:?}]",
                    case.name
                ));
            }
        }
    }
    drop(tracing_guard);

    assert!(
        failures.is_empty(),
        "calendar-field presence found {} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n")
    );
}
