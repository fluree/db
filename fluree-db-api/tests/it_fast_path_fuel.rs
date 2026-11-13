//! Fast-path index reads charge fuel like the scan lanes do: one index touch
//! per leaflet read.
//!
//! Each lane runs over the same facts indexed twice, in 3-row leaflets and in
//! default ones, so its answer, its rows and every other charge match, and
//! the fuel differs by the leaflets alone (about 100 against 1, 0.01 each).
//! A lane that reads the index without charging costs the same on both.
//!
//! Every case also checks its hand-written answer and that its lane served
//! it (its `fast-path outcome` stamp proceeded), so a case cannot pass on the
//! general pipeline.
//!
//! Own test binary: asserts fast-path routing via span capture.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{
    Fluree, FlureeBuilder, GraphDb, QueryExecutionOptions, Tracker, TrackingOptions,
};
use fluree_db_indexer::IndexerConfig;
use serde_json::{json, Value};
use tempfile::TempDir;

const SMALL: &str = "fuel/fast-small:main";
const LARGE: &str = "fuel/fast-large:main";
const CYPHER_SMALL: &str = "fuel/fast-cypher-small:main";
const CYPHER_LARGE: &str = "fuel/fast-cypher-large:main";
const IMPORTED_SMALL: &str = "fuel/fast-imported-small:main";
const IMPORTED_LARGE: &str = "fuel/fast-imported-large:main";
const N: usize = 300;
/// The fuel 100 leaflets cost less one: what a charged lane adds on the
/// small-leaflet ledger, with room for lanes that skip some leaflets.
const LEAFLET_GAP: f64 = 0.3;
/// The same for a lane that reads a third of a predicate's leaflets.
const LEAFLET_GAP_SPARSE: f64 = 0.1;

fn turtle() -> String {
    let mut ttl = String::from(
        "@prefix ex: <http://example.org/> .\n\
         @prefix geo: <http://www.opengis.net/ont/geosparql#> .\n",
    );
    for i in 0..N {
        let lng = 2.0 + i as f64 * 0.0001;
        ttl.push_str(&format!(
            "ex:s{i} a ex:Thing ; ex:name \"name{i:03}\" ; ex:p ex:o{} ; ex:q {i} ; \
             ex:next ex:s{} ; ex:loc \"POINT({lng} 48.0)\"^^geo:wktLiteral .\n",
            i % 3,
            i + 1
        ));
    }
    ttl
}

/// Index `ledger` in 3-row leaflets when `small`, default ones otherwise.
async fn reindex(fluree: &Fluree, ledger: &str, small: bool) {
    let mut opts = ReindexOptions::default();
    if small {
        opts = opts.with_indexer_config(IndexerConfig::default().with_leaflet_rows(3));
    }
    fluree.reindex(ledger, opts).await.expect("index");
    fluree.disconnect_ledger(ledger).await;
}

async fn seed(fluree: &Fluree) {
    let ttl = turtle();
    for (ledger, small) in [(SMALL, true), (LARGE, false)] {
        fluree.create_ledger(ledger).await.expect("create");
        let handle = fluree.ledger_cached(ledger).await.expect("cache");
        fluree
            .stage(&handle)
            .upsert_turtle(&ttl)
            .execute()
            .await
            .expect("seed");
        reindex(fluree, ledger, small).await;
    }
    // Cypher labels and properties are bare names, so its lane gets a
    // ledger of its own written with them.
    let nodes: Vec<Value> = (0..N)
        .map(|i| json!({"@id": format!("n{i}"), "@type": "Thing", "q": i}))
        .collect();
    for (ledger, small) in [(CYPHER_SMALL, true), (CYPHER_LARGE, false)] {
        fluree.create_ledger(ledger).await.expect("create");
        let handle = fluree.ledger_cached(ledger).await.expect("cache");
        let data = json!({ "@graph": nodes });
        fluree
            .stage(&handle)
            .insert(&data)
            .execute()
            .await
            .expect("seed");
        reindex(fluree, ledger, small).await;
    }
}

/// A file-backed instance holding the string facts bulk-imported twice: only
/// an import assigns string ids in lexical order, which the string-prefix
/// lane needs.
async fn seed_imported(db_dir: &TempDir, data_dir: &TempDir) -> Fluree {
    let ttl: String = (0..N)
        .map(|i| format!("<http://example.org/s{i}> <http://example.org/name> \"name{i:03}\" .\n"))
        .collect();
    std::fs::write(data_dir.path().join("names.ttl"), ttl).expect("write ttl");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("file-backed instance");
    for (ledger, small) in [(IMPORTED_SMALL, true), (IMPORTED_LARGE, false)] {
        let mut import = fluree
            .create(ledger)
            .import(data_dir.path())
            .threads(2)
            .memory_budget_mb(256)
            .cleanup(false);
        if small {
            import = import.leaflet_rows(3);
        }
        import.execute().await.expect("import");
    }
    fluree
}

async fn view(fluree: &Fluree, ledger: &str) -> GraphDb {
    let handle = fluree.ledger_cached(ledger).await.expect("load");
    GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state())
}

enum Query {
    Sparql(&'static str),
    JsonLd(Value),
    Cypher(&'static str),
}

/// Which pair of ledgers, 3-row leaflets and default ones, a case reads.
#[derive(Clone, Copy)]
enum Ledgers {
    Committed,
    Cypher,
    Imported,
}

struct Case {
    name: &'static str,
    ledgers: Ledgers,
    query: Query,
    /// The lane's `fast-path outcome` site; `None` for a lane that is the
    /// only one its pattern has.
    site: Option<&'static str>,
    expected: Value,
    fuel: Fuel,
}

enum Fuel {
    /// The small-leaflet ledger costs at least this much more.
    Gap(f64),
    /// Each ledger costs at least this much: the lane reads at least one
    /// leaflet per step, whatever the leaflet size.
    AtLeast(f64),
}

/// The rows of a SPARQL or JSON-LD answer, sorted, as JSON strings. A SPARQL
/// binding becomes `{var: value}` with its variables in name order.
fn rows(result: &Value) -> Vec<String> {
    let rows: Vec<String> = match result["results"]["bindings"].as_array() {
        Some(bindings) => bindings
            .iter()
            .map(|b| {
                let row: std::collections::BTreeMap<&String, &Value> = b
                    .as_object()
                    .expect("binding")
                    .iter()
                    .map(|(var, term)| (var, &term["value"]))
                    .collect();
                serde_json::to_string(&row).expect("row")
            })
            .collect(),
        None => result
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| match row.as_object() {
                Some(row) => {
                    serde_json::to_string(&row.iter().collect::<std::collections::BTreeMap<_, _>>())
                        .expect("row")
                }
                None => row.to_string(),
            })
            .collect(),
    };
    let mut rows = rows;
    rows.sort();
    rows
}

/// Run `case` on `ledger`: its sorted rows, its fuel, and the
/// `site=outcome` stamps it left.
async fn run(
    fluree: &Fluree,
    store: &span_capture::SpanStore,
    ledger: &str,
    case: &Case,
) -> (Vec<String>, f64, Vec<String>) {
    let db = view(fluree, ledger).await;
    let before = store.find_events("fast-path outcome").len();
    let (rows, fuel) = match &case.query {
        Query::Sparql(q) => {
            let r = db
                .query(fluree)
                .sparql(q)
                .execute_tracked()
                .await
                .unwrap_or_else(|e| panic!("{}: {e:?}", case.name));
            (rows(&r.result), r.fuel.expect("fuel"))
        }
        Query::JsonLd(q) => {
            let r = db
                .query(fluree)
                .jsonld(q)
                .execute_tracked()
                .await
                .unwrap_or_else(|e| panic!("{}: {e:?}", case.name));
            (rows(&r.result), r.fuel.expect("fuel"))
        }
        Query::Cypher(q) => {
            let tracker = Tracker::new(TrackingOptions {
                track_fuel: true,
                ..TrackingOptions::default()
            });
            let result = fluree
                .query_cypher_with_tracker(
                    &db,
                    q,
                    None,
                    &QueryExecutionOptions::default(),
                    &tracker,
                )
                .await
                .unwrap_or_else(|e| panic!("{}: {e:?}", case.name));
            let cj = result
                .to_cypher_json_async(db.as_graph_db_ref())
                .await
                .expect("cypher json");
            let data: Vec<Value> = cj["results"][0]["data"]
                .as_array()
                .expect("data")
                .iter()
                .map(|d| d["row"].clone())
                .collect();
            (
                rows(&Value::Array(data)),
                tracker.tally().and_then(|t| t.fuel).expect("fuel"),
            )
        }
    };
    let stamps = store.find_events("fast-path outcome")[before..]
        .iter()
        .map(|e| {
            let field = |k: &str| e.fields.get(k).map_or("", String::as_str).to_string();
            format!("{}={}", field("site"), field("outcome"))
        })
        .collect();
    (rows, fuel, stamps)
}

fn cases() -> Vec<Case> {
    let sparql_rows = |rows: &[Value]| Value::Array(rows.to_vec());
    vec![
        Case {
            name: "count plan (star of two)",
            ledgers: Ledgers::Committed,
            query: Query::Sparql(
                "PREFIX ex: <http://example.org/> \
                 SELECT (COUNT(*) AS ?n) WHERE { ?s ex:q ?q . ?s ex:name ?nm }",
            ),
            site: Some("count-plan"),
            expected: sparql_rows(&[json!({"n": "300"})]),
            fuel: Fuel::Gap(LEAFLET_GAP),
        },
        Case {
            name: "whole-graph aggregate (class-anchored)",
            ledgers: Ledgers::Cypher,
            query: Query::Cypher("MATCH (n:Thing) RETURN avg(n.q) AS a"),
            site: Some("whole-graph scalar aggregates"),
            expected: json!([["149.5"]]),
            fuel: Fuel::Gap(LEAFLET_GAP),
        },
        Case {
            name: "star with constant objects, ordered top-k",
            ledgers: Ledgers::Committed,
            query: Query::Sparql(
                "PREFIX ex: <http://example.org/> \
                 SELECT DISTINCT ?s ?label WHERE { ?s ex:p ex:o1 . ?s ex:q ?v \
                 FILTER(?v > 250) ?s ex:name ?label } ORDER BY ?label LIMIT 3",
            ),
            site: Some("star-const-order-topk"),
            expected: sparql_rows(&[
                json!({"s": "http://example.org/s253", "label": "name253"}),
                json!({"s": "http://example.org/s256", "label": "name256"}),
                json!({"s": "http://example.org/s259", "label": "name259"}),
            ]),
            fuel: Fuel::Gap(LEAFLET_GAP_SPARSE),
        },
        Case {
            // Every object is its own group, so no leaflet is one group the
            // directory alone can count: each is decoded.
            name: "group by object, counted top-k",
            ledgers: Ledgers::Committed,
            query: Query::Sparql(
                "PREFIX ex: <http://example.org/> \
                 SELECT ?o (COUNT(?s) AS ?c) WHERE { ?s ex:next ?o } \
                 GROUP BY ?o ORDER BY DESC(?c) LIMIT 1000",
            ),
            site: Some("group_by_object_count_topk"),
            expected: sparql_rows(
                &(1..=N)
                    .map(|i| json!({"o": format!("http://example.org/s{i}"), "c": "1"}))
                    .collect::<Vec<_>>(),
            ),
            fuel: Fuel::Gap(LEAFLET_GAP),
        },
        Case {
            name: "string-prefix count",
            ledgers: Ledgers::Imported,
            query: Query::Sparql(
                "PREFIX ex: <http://example.org/> \
                 SELECT (COUNT(*) AS ?n) WHERE { ?s ex:name ?nm FILTER(STRSTARTS(?nm, \"name1\")) }",
            ),
            site: Some("string-prefix COUNT(*)"),
            expected: sparql_rows(&[json!({"n": "100"})]),
            fuel: Fuel::Gap(LEAFLET_GAP_SPARSE),
        },
        Case {
            name: "label regex with a type check",
            ledgers: Ledgers::Committed,
            query: Query::Sparql(
                "PREFIX ex: <http://example.org/> \
                 SELECT ?s ?label WHERE { ?s ex:name ?label . ?s a ex:Thing . \
                 FILTER regex(?label, \"^name29\") }",
            ),
            site: Some("label-regex-type"),
            expected: sparql_rows(
                &(290..300)
                    .map(|i| json!({"s": format!("http://example.org/s{i}"), "label": format!("name{i}")}))
                    .collect::<Vec<_>>(),
            ),
            fuel: Fuel::Gap(LEAFLET_GAP),
        },
        Case {
            name: "one-or-more path count",
            ledgers: Ledgers::Committed,
            query: Query::Sparql(
                "PREFIX ex: <http://example.org/> \
                 SELECT (COUNT(*) AS ?c) WHERE { ex:s0 ex:next+ ?o }",
            ),
            site: Some("property-path+ COUNT(*)"),
            expected: sparql_rows(&[json!({"c": "300"})]),
            fuel: Fuel::Gap(LEAFLET_GAP),
        },
        Case {
            name: "one-or-more path, id lane",
            ledgers: Ledgers::Committed,
            query: Query::Sparql(
                "PREFIX ex: <http://example.org/> SELECT ?o WHERE { ex:s0 ex:next+ ?o }",
            ),
            site: None,
            expected: sparql_rows(
                &(1..=N)
                    .map(|i| json!({"o": format!("http://example.org/s{i}")}))
                    .collect::<Vec<_>>(),
            ),
            // One frontier lookup per hop, a leaflet each, on top of the floor.
            fuel: Fuel::AtLeast(1.0 + N as f64 * 0.01),
        },
        Case {
            name: "geo search",
            ledgers: Ledgers::Committed,
            query: Query::JsonLd(json!({
                "@context": {
                    "ex": "http://example.org/",
                    "geof": "http://www.opengis.net/def/function/geosparql/"
                },
                "select": ["?place"],
                "where": [
                    { "@id": "?place", "ex:loc": "?loc" },
                    ["bind", "?dist", "(geof:distance ?loc \"POINT(2.0 48.0)\")"],
                    ["filter", "(<= ?dist 10000)"]
                ]
            })),
            site: None,
            expected: Value::Array(
                (0..N)
                    .map(|i| json!([format!("ex:s{i}")]))
                    .collect::<Vec<_>>(),
            ),
            fuel: Fuel::Gap(LEAFLET_GAP),
        },
    ]
}

#[tokio::test]
async fn fast_path_index_reads_charge_the_leaflets_they_touch() {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    seed(&fluree).await;
    let (db_dir, data_dir) = (TempDir::new().expect("tmp"), TempDir::new().expect("tmp"));
    let imported = seed_imported(&db_dir, &data_dir).await;
    let (store, _tracing) = span_capture::init_test_tracing();

    let mut failures: Vec<String> = Vec::new();
    for case in cases() {
        let (fluree, small_ledger, large_ledger) = match case.ledgers {
            Ledgers::Committed => (&fluree, SMALL, LARGE),
            Ledgers::Cypher => (&fluree, CYPHER_SMALL, CYPHER_LARGE),
            Ledgers::Imported => (&imported, IMPORTED_SMALL, IMPORTED_LARGE),
        };
        let expected = rows(&case.expected);
        let (small_rows, small_fuel, small_sites) = run(fluree, &store, small_ledger, &case).await;
        let (large_rows, large_fuel, large_sites) = run(fluree, &store, large_ledger, &case).await;
        for (leaflets, got, sites) in [
            ("3-row leaflets", &small_rows, &small_sites),
            ("default leaflets", &large_rows, &large_sites),
        ] {
            if *got != expected {
                failures.push(format!(
                    "{} ({leaflets}): {got:?}, want {expected:?}",
                    case.name
                ));
            }
            if let Some(site) = case.site {
                if !sites.iter().any(|s| *s == format!("{site}=proceed")) {
                    failures.push(format!(
                        "{} ({leaflets}): `{site}` did not proceed (stamps: {sites:?})",
                        case.name
                    ));
                }
            }
        }
        eprintln!(
            "{}: {small_fuel} fuel on 3-row leaflets, {large_fuel} on default ones",
            case.name
        );
        match case.fuel {
            Fuel::Gap(gap) if small_fuel - large_fuel < gap => failures.push(format!(
                "{}: 3-row leaflets cost {small_fuel} fuel, default leaflets {large_fuel}; \
                 want a gap of at least {gap}",
                case.name
            )),
            Fuel::AtLeast(min) if small_fuel < min || large_fuel < min => {
                failures.push(format!(
                    "{}: cost {small_fuel} and {large_fuel} fuel, want at least {min}",
                    case.name
                ));
            }
            _ => {}
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A fuel limit stops each lane mid-read as a fuel error: the lanes'
/// cursors and leaflet loads hand the refusal back intact instead of
/// folding it into an internal error. Each case costs at least 1.34 fuel on
/// the 3-row leaflets, so a 1.2 limit trips inside the lane.
#[tokio::test]
async fn a_fuel_limit_stops_fast_path_lanes_as_a_fuel_error() {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    seed(&fluree).await;
    let (db_dir, data_dir) = (TempDir::new().expect("tmp"), TempDir::new().expect("tmp"));
    let imported = seed_imported(&db_dir, &data_dir).await;

    let mut failures: Vec<String> = Vec::new();
    for case in cases() {
        let (fluree, ledger) = match case.ledgers {
            Ledgers::Committed => (&fluree, SMALL),
            Ledgers::Imported => (&imported, IMPORTED_SMALL),
            Ledgers::Cypher => continue,
        };
        let Query::Sparql(query) = case.query else {
            continue;
        };
        if matches!(case.fuel, Fuel::AtLeast(_)) {
            continue;
        }
        let db = view(fluree, ledger).await;
        let query = format!("# PRAGMA max-fuel: 1.2\n{query}");
        match db.query(fluree).sparql(&query).execute_tracked().await {
            Ok(r) => failures.push(format!(
                "{}: ran to {:?} fuel under a 1.2 limit",
                case.name, r.fuel
            )),
            Err(e) if e.status == 400 && e.error.to_lowercase().contains("fuel") => {}
            Err(e) => failures.push(format!(
                "{}: refused with status {}: {}",
                case.name, e.status, e.error
            )),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
