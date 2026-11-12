//! A triples map over an Iceberg table narrowed by a restricted `rr:sqlQuery`
//! (`SELECT * FROM <table> WHERE <col> <op> <literal> [AND ...]`).
//!
//! The committed fixtures `silver.customers_history` and `silver.orders` (see
//! `scripts/local/write_local_iceberg_table.py`) are a history table with
//! repeated customer ids, a soft-deleted current row, a null `is_current`, and
//! a non-current row holding the largest balance, plus orders referencing it.
//! The mapping keeps the current, non-deleted rows:
//!
//! ```sql
//! SELECT * FROM silver.customers_history WHERE is_current = 1 AND is_deleted = FALSE
//! ```
//!
//! Every reader of the map's rows must apply that filter exactly, and every
//! shortcut that answers without reading rows must decline. Each answer is
//! compared with a native ledger holding the rows the filter keeps (the
//! oracle), on SPARQL and on JSON-LD, with fast paths on and off, and the
//! lanes that matter carry routing stamps:
//!
//! - the fused aggregate serves the filtered map (it scans);
//! - the manifest `COUNT` shortcut fires for an unfiltered map over the same
//!   table and declines for the filtered one (no statistic knows which rows a
//!   filter keeps);
//! - the scan-side top-k reads the filtered rows best-first, so the top
//!   balance is a current row's, not the larger non-current one;
//! - the browse crawl's FK shortcut, which renders a reference from the
//!   child's own key (dangling ones included), keeps the parent scan for a
//!   filtered parent, so an order whose customer the filter excludes has no
//!   reference.
//!
//! Own binary: it sets `FLUREE_ICEBERG_LOCAL_ROOTS` and flips the process-wide
//! fast-path switch, so its tests are serialized by one lock.

#![cfg(all(feature = "iceberg", feature = "native"))]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, R2rmlCreateConfig};
use serde_json::{json, Value};
use tokio::sync::Mutex;

/// The tests share the fast-path switch and the fixtures root.
static SERIAL: Mutex<()> = Mutex::const_new(());

const FILTERED: &str = "filtered:main";
const ORACLE: &str = "oracle:main";

/// `{customer_logical_table}` is the customer map's logical table.
const MAPPING: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#Customer> a rr:TriplesMap ;
        rr:logicalTable [ {customer_logical_table} ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{customer_id}" ; rr:class ex:Customer ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:tier ; rr:objectMap [ rr:column "tier" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:balance ;
            rr:objectMap [ rr:column "balance" ; rr:datatype xsd:decimal ] ] .

    <http://example.org/mapping#HighScorer> a rr:TriplesMap ;
        rr:logicalTable [ rr:sqlQuery "SELECT customer_id, valid_from FROM silver.customers_history WHERE score >= 3" ] ;
        rr:subjectMap [ rr:template "http://example.org/history/{customer_id}/{valid_from}" ;
                        rr:class ex:HighScorer ] .

    <http://example.org/mapping#HistoryRow> a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.customers_history" ] ;
        rr:subjectMap [ rr:template "http://example.org/history/{customer_id}/{valid_from}" ;
                        rr:class ex:HistoryRow ] .

    <http://example.org/mapping#Order> a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.orders" ] ;
        rr:subjectMap [ rr:template "http://example.org/order/{order_id}" ; rr:class ex:Order ] ;
        rr:predicateObjectMap [ rr:predicate ex:customer ; rr:objectMap [
            rr:parentTriplesMap <http://example.org/mapping#Customer> ;
            rr:joinCondition [ rr:child "customer_id" ; rr:parent "customer_id" ] ] ] .
"#;

const CURRENT: &str = r#"rr:sqlQuery "SELECT * FROM silver.customers_history WHERE is_current = 1 AND is_deleted = FALSE""#;
const WHOLE_TABLE: &str = r#"rr:tableName "silver.customers_history""#;

fn mapping(customer_logical_table: &str) -> String {
    MAPPING.replace("{customer_logical_table}", customer_logical_table)
}

fn fixtures_dir() -> String {
    format!("{}/tests/fixtures/iceberg", env!("CARGO_MANIFEST_DIR"))
}

/// A Direct source over the `silver` warehouse root: each table resolves to its
/// own directory beneath it.
async fn create_source(fluree: &Fluree, name: &str, mapping: &str) {
    // SAFETY: set before any storage or scan is built; every test in this binary
    // sets the same value.
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", fixtures_dir());
    let location = format!("file://{}/silver", fixtures_dir());
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct(name, &location, mapping)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// The rows the filter keeps, as a native ledger.
async fn create_oracle(fluree: &Fluree) {
    let ledger = fluree.create_ledger(ORACLE).await.expect("oracle ledger");
    let customer = |id: i64| format!("http://example.org/customer/{id}");
    let decimal =
        |v: &str| json!({"@value": v, "@type": "http://www.w3.org/2001/XMLSchema#decimal"});
    let mut graph = vec![
        json!({"@id": customer(1), "@type": ["http://example.org/Customer"],
               "http://example.org/name": "alice", "http://example.org/tier": "gold",
               "http://example.org/balance": decimal("25.50")}),
        json!({"@id": customer(2), "@type": ["http://example.org/Customer"],
               "http://example.org/name": "bob", "http://example.org/tier": "silver",
               "http://example.org/balance": decimal("7.25")}),
        json!({"@id": customer(4), "@type": ["http://example.org/Customer"],
               "http://example.org/name": "dave", "http://example.org/balance": decimal("100.00")}),
    ];
    // `score >= 3` compares as IEEE numbers: the NaN row (customer 2's first
    // version) is not among them.
    for history in ["2/2024-07-01", "5/2024-01-01", "5/2024-08-01"] {
        graph.push(json!({
            "@id": format!("http://example.org/history/{history}"),
            "@type": "http://example.org/HighScorer",
        }));
    }
    for (order, customer_id) in [
        (100, Some(1)),
        (101, Some(2)),
        (102, None),
        (103, None),
        (104, Some(1)),
        (105, None),
    ] {
        let mut node = json!({
            "@id": format!("http://example.org/order/{order}"),
            "@type": "http://example.org/Order",
        });
        if let Some(id) = customer_id {
            node["http://example.org/customer"] = json!({"@id": customer(id)});
        }
        graph.push(node);
    }
    fluree
        .insert(ledger, &json!({"@graph": graph}))
        .await
        .expect("seed oracle");
}

/// One query on both surfaces: SPARQL (with `{FROM}` where the dataset goes)
/// and its JSON-LD twin (without `from`).
struct Case {
    name: &'static str,
    sparql: &'static str,
    jsonld: Value,
    /// The SPARQL projection, in the order each expected row joins it.
    vars: &'static [&'static str],
    /// The expected rows, each `|`-joined, sorted (SPARQL form).
    expected: &'static [&'static str],
    /// A routing site that must proceed (fast paths on).
    must_fire: Option<&'static str>,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "names",
            vars: &["c", "name"],
            sparql: "SELECT ?c ?name {FROM} WHERE { ?c a ex:Customer ; ex:name ?name }",
            jsonld: json!({"select": ["?c", "?name"],
                           "where": {"@id": "?c", "@type": "ex:Customer", "ex:name": "?name"}}),
            expected: &["customer/1|alice", "customer/2|bob", "customer/4|dave"],
            must_fire: None,
        },
        Case {
            name: "count",
            vars: &["n"],
            sparql: "SELECT (COUNT(?c) AS ?n) {FROM} WHERE { ?c a ex:Customer }",
            jsonld: json!({"select": ["(count ?c)"], "where": {"@id": "?c", "@type": "ex:Customer"}}),
            expected: &["3"],
            must_fire: Some("fused_r2rml_aggregate"),
        },
        Case {
            name: "refs",
            vars: &["o", "c"],
            sparql: "SELECT ?o ?c {FROM} WHERE { ?o ex:customer ?c }",
            jsonld: json!({"select": ["?o", "?c"], "where": {"@id": "?o", "ex:customer": "?c"}}),
            expected: &[
                "order/100|customer/1",
                "order/101|customer/2",
                "order/104|customer/1",
            ],
            must_fire: None,
        },
        Case {
            name: "ref-join",
            vars: &["o", "name"],
            sparql: "SELECT ?o ?name {FROM} WHERE { ?o ex:customer ?c . ?c ex:name ?name }",
            jsonld: json!({"select": ["?o", "?name"],
                           "where": [{"@id": "?o", "ex:customer": "?c"}, {"@id": "?c", "ex:name": "?name"}]}),
            expected: &["order/100|alice", "order/101|bob", "order/104|alice"],
            must_fire: None,
        },
        Case {
            name: "top-balance",
            vars: &["c"],
            sparql: "SELECT ?c {FROM} WHERE { ?c ex:balance ?b } ORDER BY DESC(?b) LIMIT 1",
            jsonld: json!({"select": ["?c"], "where": {"@id": "?c", "ex:balance": "?b"},
                           "orderBy": "(desc ?b)", "limit": 1}),
            expected: &["customer/4"],
            must_fire: None,
        },
        Case {
            name: "tiers",
            vars: &["tier", "n"],
            sparql:
                "SELECT ?tier (COUNT(?c) AS ?n) {FROM} WHERE { ?c ex:tier ?tier } GROUP BY ?tier",
            jsonld: json!({"select": ["?tier", "(as (count ?c) ?n)"],
                           "where": {"@id": "?c", "ex:tier": "?tier"}, "groupBy": ["?tier"]}),
            expected: &["gold|1", "silver|1"],
            must_fire: Some("fused_r2rml_aggregate"),
        },
        Case {
            name: "float-filter",
            vars: &["c"],
            sparql: "SELECT ?c {FROM} WHERE { ?c a ex:HighScorer }",
            jsonld: json!({"select": ["?c"], "where": {"@id": "?c", "@type": "ex:HighScorer"}}),
            expected: &[
                "history/2/2024-07-01",
                "history/5/2024-01-01",
                "history/5/2024-08-01",
            ],
            must_fire: None,
        },
    ]
}

/// A term as the rows compare it: an IRI or literal with the
/// `http://example.org/` prefix dropped.
fn term(v: &Value) -> String {
    let s = match v {
        Value::String(s) => s.clone(),
        Value::Object(o) => o
            .get("value")
            .or_else(|| o.get("@value"))
            .or_else(|| o.get("@id"))
            .map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .unwrap_or_else(|| v.to_string()),
        other => other.to_string(),
    };
    s.trim_start_matches("http://example.org/")
        .trim_start_matches("ex:")
        .to_string()
}

async fn sparql_rows(fluree: &Fluree, from: &str, case: &Case) -> Vec<String> {
    let body = case.sparql.replace("{FROM}", &format!("FROM <{from}>"));
    let sparql = format!("PREFIX ex: <http://example.org/> {body}");
    let result = fluree
        .query_from()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{}: {e}\n{sparql}", case.name));
    let vars = case.vars;
    let mut rows: Vec<String> = result["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("{}: not SPARQL JSON: {result}", case.name))
        .iter()
        .map(|b| {
            vars.iter()
                .map(|v| term(&b[*v]))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    rows.sort();
    rows
}

async fn jsonld_rows(fluree: &Fluree, from: &str, case: &Case) -> Vec<String> {
    let mut query = case.jsonld.clone();
    query["@context"] = json!({"ex": "http://example.org/"});
    query["from"] = json!(from);
    let result = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{} (JSON-LD): {e}\n{query}", case.name));
    let mut rows: Vec<String> = result
        .as_array()
        .unwrap_or_else(|| panic!("{} (JSON-LD): not an array: {result}", case.name))
        .iter()
        .map(|row| match row {
            Value::Array(cells) => cells.iter().map(term).collect::<Vec<_>>().join("|"),
            cell => term(cell),
        })
        .collect();
    rows.sort();
    rows
}

/// Sites that proceeded since event `before`.
fn proceeded(store: &span_capture::SpanStore, before: usize) -> Vec<String> {
    store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
        .filter_map(|e| e.fields.get("site").cloned())
        .collect()
}

/// Every case answers the rows the filter keeps, equal to the native oracle,
/// on both surfaces, with fast paths on and off; the routing stamps show the
/// lanes that served them.
#[tokio::test(flavor = "current_thread")]
async fn filtered_map_answers_match_the_native_oracle() {
    let _serial = SERIAL.lock().await;
    let fluree = FlureeBuilder::memory().build_memory();
    create_source(&fluree, "filtered", &mapping(CURRENT)).await;
    create_oracle(&fluree).await;
    let (store, _tracing) = span_capture::init_test_tracing();

    for disabled in [false, true] {
        set_fast_paths_disabled(disabled);
        for case in cases() {
            let before = store.find_events("fast-path outcome").len();
            let virtual_sparql = sparql_rows(&fluree, FILTERED, &case).await;
            if !disabled {
                if let Some(site) = case.must_fire {
                    let sites = proceeded(&store, before);
                    assert!(
                        sites.iter().any(|s| s == site),
                        "{}: `{site}` must proceed [proceeded: {sites:?}]",
                        case.name
                    );
                }
            }
            let expected: Vec<String> = case.expected.iter().map(ToString::to_string).collect();
            assert_eq!(
                virtual_sparql, expected,
                "{} (SPARQL, disabled={disabled})",
                case.name
            );
            let oracle_sparql = sparql_rows(&fluree, ORACLE, &case).await;
            assert_eq!(
                virtual_sparql, oracle_sparql,
                "{} vs oracle (SPARQL)",
                case.name
            );

            let virtual_jsonld = jsonld_rows(&fluree, FILTERED, &case).await;
            let oracle_jsonld = jsonld_rows(&fluree, ORACLE, &case).await;
            assert_eq!(
                virtual_jsonld, oracle_jsonld,
                "{} vs oracle (JSON-LD, disabled={disabled})",
                case.name
            );
            assert_eq!(
                virtual_jsonld.len(),
                expected.len(),
                "{} (JSON-LD): {virtual_jsonld:?}",
                case.name
            );
        }
    }
    set_fast_paths_disabled(false);

    // The manifest COUNT shortcut: fires for the unfiltered map over the same
    // table (8 rows) and declines for the filtered one, whose count is 3.
    let count = |class: &str| {
        format!("PREFIX ex: <http://example.org/> SELECT (COUNT(?s) AS ?n) FROM <{FILTERED}> WHERE {{ ?s a ex:{class} }}")
    };
    let fired = |store: &span_capture::SpanStore, before: usize| -> Vec<String> {
        store.find_spans("r2rml.count_manifest")[before..]
            .iter()
            .filter_map(|s| s.fields.get("fired").cloned())
            .collect()
    };
    for (class, n, expect_fired) in [("HistoryRow", "8", "true"), ("Customer", "3", "false")] {
        let before = store.find_spans("r2rml.count_manifest").len();
        let result = fluree
            .query_from()
            .sparql(&count(class))
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("count {class}: {e}"));
        assert_eq!(
            term(&result["results"]["bindings"][0]["n"]),
            n,
            "count {class}: {result}"
        );
        let outcomes = fired(&store, before);
        assert_eq!(
            outcomes,
            [expect_fired],
            "manifest COUNT for {class}: {outcomes:?}"
        );
    }

    // The scan-side top-k ran for the top balance and stopped early: the file
    // holding the current rows' best balance is read first, and the other file
    // cannot beat it. Its answer (customer/4, 100.00) is above; unfiltered, a
    // non-current row's 500.00 would win.
    let before = store.find_events("scan-side top-k prune").len();
    let top = sparql_rows(&fluree, FILTERED, &cases()[4]).await;
    assert_eq!(top, ["customer/4"]);
    let prunes = &store.find_events("scan-side top-k prune")[before..];
    assert!(
        prunes
            .iter()
            .any(|e| e.fields.get("files_pruned").map(String::as_str) == Some("1")),
        "the top-k must read one file and prune the other: {:?}",
        prunes.iter().map(|e| &e.fields).collect::<Vec<_>>()
    );
}

/// The `{"?s": ["*"]}` browse crawl renders a reference from the child's own
/// key without scanning the parent, dangling keys included. Over the whole
/// history table that shortcut fires (order 105 references the absent
/// customer 99); over the filtered table it must not, or orders whose customer
/// the filter excludes would reference subjects the mapping never emits.
#[tokio::test]
async fn browse_crawl_keeps_the_parent_scan_for_a_filtered_parent() {
    let _serial = SERIAL.lock().await;
    let fluree = FlureeBuilder::memory().build_memory();
    create_source(&fluree, "filtered", &mapping(CURRENT)).await;
    create_source(&fluree, "whole", &mapping(WHOLE_TABLE)).await;

    let crawl = |from: &str| {
        json!({
            "@context": {"ex": "http://example.org/"},
            "from": from,
            "select": {"?o": ["*"]},
            "where": {"@id": "?o", "@type": "ex:Order"},
        })
    };
    let refs = |docs: &Value| -> Vec<String> {
        let mut out: Vec<String> = docs
            .as_array()
            .expect("crawl returns an array")
            .iter()
            .map(|doc| {
                let order = term(&doc["@id"]);
                let customer = doc
                    .get("ex:customer")
                    .or_else(|| doc.get("http://example.org/customer"))
                    .map(term)
                    .unwrap_or_else(|| "-".to_string());
                format!("{order}|{customer}")
            })
            .collect();
        out.sort();
        out
    };
    for (from, expected) in [
        (
            "whole:main",
            [
                "order/100|customer/1",
                "order/101|customer/2",
                "order/102|customer/3",
                "order/103|customer/5",
                "order/104|customer/1",
                "order/105|customer/99",
            ],
        ),
        (
            FILTERED,
            [
                "order/100|customer/1",
                "order/101|customer/2",
                "order/102|-",
                "order/103|-",
                "order/104|customer/1",
                "order/105|-",
            ],
        ),
    ] {
        let docs = fluree
            .query_from()
            .jsonld(&crawl(from))
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("crawl {from}: {e}"));
        assert_eq!(refs(&docs), expected, "crawl {from}: {docs}");
    }
}

/// The bulk build reads every map through the filtered scan; the tracking
/// materializer, which cannot retract a subject whose row leaves the filter,
/// refuses the mapping and points at the bulk build.
#[tokio::test]
async fn bulk_materialize_applies_the_filter_and_tracking_refuses() {
    use fluree_db_r2rml::materialize::NTriplesCollector;
    let _serial = SERIAL.lock().await;
    let fluree = FlureeBuilder::memory().build_memory();
    create_source(&fluree, "filtered", &mapping(CURRENT)).await;

    let provider = fluree_db_api::FlureeR2rmlProvider::new(&fluree);
    let mut collector = NTriplesCollector::default();
    fluree_db_api::materialize::materialize_graph(&provider, FILTERED, &mut collector)
        .await
        .expect("bulk materialize");
    let triples = collector.sorted_unique();
    let names: Vec<&String> = triples
        .iter()
        .filter(|t| t.contains("<http://example.org/name>"))
        .collect();
    assert_eq!(names.len(), 3, "{names:?}");
    assert!(
        !triples.iter().any(|t| t.contains("-v1\"")),
        "a non-current version leaked: {triples:?}"
    );
    let mut refs: Vec<String> = triples
        .iter()
        .filter(|t| t.contains("<http://example.org/customer>"))
        .map(|t| {
            let parts: Vec<&str> = t.split_whitespace().collect();
            let strip = |iri: &str| {
                iri.trim_start_matches("<http://example.org/")
                    .trim_end_matches('>')
                    .to_string()
            };
            format!("{} {}", strip(parts[0]), strip(parts[2]))
        })
        .collect();
    refs.sort();
    assert_eq!(
        refs,
        [
            "order/100 customer/1",
            "order/101 customer/2",
            "order/104 customer/1"
        ],
        "{triples:?}"
    );

    fluree.create_ledger("target:main").await.expect("target");
    let err = fluree
        .materialize_r2rml_graph_source(FILTERED, "target:main", false)
        .await
        .expect_err("a filtered map cannot be tracked");
    let message = err.to_string();
    assert!(
        message.contains("http://example.org/mapping#Customer")
            && message.contains("fluree materialize"),
        "{message}"
    );

    // `/info` lists the table the query reads, never its alias.
    let info = fluree
        .ledger_info(FILTERED)
        .execute()
        .await
        .expect("graph source info");
    assert_eq!(
        info["source"]["tables"],
        json!(["silver.customers_history", "silver.orders"]),
        "{info}"
    );
}

/// A query outside the subset is refused at registration, naming the
/// construct; a column or literal the table's schema cannot decide is refused
/// at the first read, naming the column. Nothing is approximated.
#[tokio::test]
async fn unsupported_queries_are_refused_naming_the_construct() {
    let _serial = SERIAL.lock().await;
    let fluree = FlureeBuilder::memory().build_memory();
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", fixtures_dir());
    let location = format!("file://{}/silver", fixtures_dir());
    let logical = |sql: &str| mapping(&format!("rr:sqlQuery \"{sql}\""));

    for (sql, needle) in [
        (
            "SELECT * FROM silver.customers_history WHERE is_current = 1 OR is_deleted = TRUE",
            "OR is not supported",
        ),
        (
            "SELECT * FROM silver.customers_history c JOIN silver.orders o ON c.customer_id = o.customer_id",
            "aliases",
        ),
        ("SELECT * FROM silver.customers_history WHERE tier LIKE 'g%'", "pattern matching"),
        ("SELECT * FROM silver.customers_history ORDER BY balance", "ORDER BY"),
    ] {
        let err = fluree
            .create_r2rml_graph_source(
                R2rmlCreateConfig::new_direct("refused", &location, logical(sql))
                    .with_mapping_media_type("text/turtle"),
            )
            .await
            .expect_err(sql);
        let message = err.to_string();
        assert!(message.contains(needle), "{sql}: {message}");
        assert!(message.contains("Customer"), "{sql}: names the map: {message}");
    }

    for (i, (sql, needle)) in [
        (
            "SELECT * FROM silver.customers_history WHERE is_current = '1'",
            "compare it with an integer",
        ),
        (
            "SELECT * FROM silver.customers_history WHERE region = 'west'",
            "does not have",
        ),
        (
            "SELECT * FROM silver.customers_history WHERE Tier = 'gold'",
            "did you mean \"tier\"",
        ),
        (
            "SELECT * FROM silver.customers_history WHERE valid_from > '2024-01-01'",
            "column 'valid_from' is date",
        ),
        (
            "SELECT * FROM silver.customers_history WHERE score > 0.5",
            "no exact binary value",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("typed-{i}");
        fluree
            .create_r2rml_graph_source(
                R2rmlCreateConfig::new_direct(&name, &location, logical(sql))
                    .with_mapping_media_type("text/turtle"),
            )
            .await
            .unwrap_or_else(|e| panic!("{sql} parses, so it registers: {e}"));
        for query in [
            format!("SELECT ?c FROM <{name}:main> WHERE {{ ?c a <http://example.org/Customer> }}"),
            format!(
                "SELECT (COUNT(?c) AS ?n) FROM <{name}:main> WHERE {{ ?c a <http://example.org/Customer> }}"
            ),
        ] {
            let err = fluree
                .query_from()
                .sparql(&query)
                .execute_formatted()
                .await
                .expect_err(sql);
            assert!(err.to_string().contains(needle), "{sql}\n{query}\n{err}");
        }
    }
}

/// The provisional-mapping lane (validating a mapping before it is persisted)
/// answers a filtered map as a registered source does. It resolves the
/// query's alias to its `FROM` table, decodes the filter's columns though no
/// term map reads them, and drops every row the filter excludes. It once
/// handed the alias to the catalog as a table name and failed.
#[tokio::test(flavor = "current_thread")]
async fn a_provisional_query_over_a_filtered_map_answers_as_the_registered_source() {
    let _serial = SERIAL.lock().await;
    let fluree = FlureeBuilder::memory().build_memory();
    let ttl = mapping(CURRENT);
    create_source(&fluree, "filtered", &ttl).await;
    let body = "WHERE { ?c a ex:Customer ; ex:name ?n }";
    let rows = |result: Value| -> Vec<String> {
        let mut rows: Vec<String> = result["results"]["bindings"]
            .as_array()
            .unwrap_or_else(|| panic!("not SPARQL JSON: {result}"))
            .iter()
            .map(|b| format!("{}|{}", term(&b["c"]), term(&b["n"])))
            .collect();
        rows.sort();
        rows
    };

    let registered = rows(
        fluree
            .query_from()
            .sparql(&format!(
                "PREFIX ex: <http://example.org/> SELECT ?c ?n FROM <{FILTERED}> {body}"
            ))
            .execute_formatted()
            .await
            .expect("registered source"),
    );
    let tables = ["customers_history", "orders"]
        .into_iter()
        .map(|t| {
            (
                format!("silver.{t}"),
                format!("{}/silver/{t}", fixtures_dir()),
            )
        })
        .collect();
    let provisional = fluree
        .query_provisional_r2rml_over_local_tables_for_test(
            tables,
            ttl,
            format!("PREFIX ex: <http://example.org/> SELECT ?c ?n {body}"),
        )
        .await
        .expect("a provisional query over a filtered map");
    let provisional = rows(
        provisional
            .to_sparql_json(&fluree_db_core::LedgerSnapshot::genesis("provisional:main"))
            .expect("format"),
    );
    assert_eq!(
        registered,
        ["customer/1|alice", "customer/2|bob", "customer/4|dave"],
        "the registered source keeps the current, non-deleted rows"
    );
    assert_eq!(provisional, registered);
}

/// Copy `silver/customers_history` and `silver/orders` under a fresh
/// directory beneath the fixtures root, so deleting their manifests touches no
/// other test and their scan-files cache keys are their own.
fn stage_tables() -> tempfile::TempDir {
    fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).expect("create staged dir");
        for entry in std::fs::read_dir(from).expect("read fixture dir") {
            let entry = entry.expect("fixture entry");
            let target = to.join(entry.file_name());
            if entry.file_type().expect("file type").is_dir() {
                copy_dir(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), &target).expect("copy fixture file");
            }
        }
    }
    let staging = tempfile::TempDir::new_in(fixtures_dir()).expect("staging dir");
    for table in ["customers_history", "orders"] {
        copy_dir(
            &std::path::Path::new(&fixtures_dir())
                .join("silver")
                .join(table),
            &staging.path().join("silver").join(table),
        );
    }
    staging
}

/// Delete every staged table's data manifests (the manifest lists stay), so
/// a scan that plans by reading manifests fails instead of answering.
fn delete_staged_manifests(staging: &tempfile::TempDir) {
    for table in ["customers_history", "orders"] {
        let metadata = staging.path().join("silver").join(table).join("metadata");
        let mut deleted = 0;
        for entry in std::fs::read_dir(&metadata).expect("metadata dir") {
            let path = entry.expect("entry").path();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if name.ends_with(".avro") && !name.starts_with("snap-") {
                std::fs::remove_file(&path).expect("delete a data manifest");
                deleted += 1;
            }
        }
        assert!(deleted > 0, "{table} has data manifests to delete");
    }
}

/// A filtered map planned from a kept file list applies its row filter.
///
/// The list is kept by scans of the unfiltered maps over the same tables
/// (`HistoryRow`, `Order`); then the data manifests are deleted, so every
/// later scan must plan from the kept list. Every case still answers only the
/// rows the filters admit, equal to the native oracle on both surfaces: from
/// the in-memory list, and in a second instance, from the list the first one
/// wrote to the disk catalog cache.
#[tokio::test(flavor = "current_thread")]
async fn a_filtered_map_planned_from_the_kept_file_list_applies_its_filter() {
    let _serial = SERIAL.lock().await;
    set_fast_paths_disabled(false);
    let staging = stage_tables();
    let location = format!("file://{}/silver", staging.path().display());
    let ttl = mapping(CURRENT);
    let create = |fluree: Fluree| {
        let (location, ttl) = (location.clone(), ttl.clone());
        async move {
            std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", fixtures_dir());
            fluree
                .create_r2rml_graph_source(
                    R2rmlCreateConfig::new_direct("kept", &location, &ttl)
                        .with_mapping_media_type("text/turtle"),
                )
                .await
                .expect("create the staged source");
            create_oracle(&fluree).await;
            fluree
        }
    };
    let from = "kept:main";
    let fluree = create(FlureeBuilder::memory().build_memory()).await;

    // Keep both tables' file lists through maps that apply no row filter.
    let whole = Case {
        name: "whole-table",
        vars: &["s"],
        sparql: "SELECT ?s {FROM} WHERE { { ?s a ex:HistoryRow } UNION { ?s a ex:Order } }",
        jsonld: json!(null),
        expected: &[],
        must_fire: None,
    };
    assert_eq!(sparql_rows(&fluree, from, &whole).await.len(), 8 + 6);
    delete_staged_manifests(&staging);

    let check = |fluree: &Fluree, instance: &'static str| {
        let fluree = fluree.clone();
        async move {
            for case in cases() {
                let expected: Vec<String> = case.expected.iter().map(ToString::to_string).collect();
                assert_eq!(
                    sparql_rows(&fluree, from, &case).await,
                    expected,
                    "{} (SPARQL, {instance})",
                    case.name
                );
                assert_eq!(
                    jsonld_rows(&fluree, from, &case).await,
                    jsonld_rows(&fluree, ORACLE, &case).await,
                    "{} vs oracle (JSON-LD, {instance})",
                    case.name
                );
            }
        }
    };
    check(&fluree, "in-memory kept list").await;

    // A second instance has an empty in-memory cache; the first wrote the
    // lists to the disk catalog cache off its query path, so wait for them.
    let second = create(FlureeBuilder::memory().build_memory()).await;
    let mut waited = 0;
    while let Err(e) = second
        .query_from()
        .sparql(&format!(
            "PREFIX ex: <http://example.org/> SELECT ?s FROM <{from}> \
             WHERE {{ {{ ?s a ex:HistoryRow }} UNION {{ ?s a ex:Order }} }}"
        ))
        .execute_formatted()
        .await
    {
        waited += 1;
        assert!(
            waited < 100,
            "the disk catalog cache never held the lists: {e}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    check(&second, "disk kept list").await;
}
