//! An R2RML graph source answers over the set of triples its mapping defines
//! (R2RML §11), not over the bag of rows that mint them. A table whose rows
//! repeat a triple (a history table holding several versions of one customer,
//! an exact duplicate row) must answer as a native ledger holding those triples
//! does: each pattern matches each triple once, a same-subject star over a
//! subject minted by several rows is the cross product of that subject's
//! objects, and a COUNT counts triples.
//!
//! The sources are the committed local Iceberg tables `sets.*`
//! (`scripts/local/write_local_iceberg_table.py`):
//! - `customers_history`: no key covers `customer_id`; customer 1 has three
//!   versions (two alike), customer 2 bob then robert twice, customer 5 two
//!   versions differing in name AND city, customer 4 a NULL city, and one row
//!   a NULL `customer_id`;
//! - `customers_current`: one row per customer, minting the same subjects and
//!   class as the history table, plus customer 6;
//! - `orders`: references the history table on `customer_id`; order 101 is an
//!   exact duplicate row, 104's customer dangles, 105's is NULL.
//! - `order_lines`: in two files, the first holding order 42's amount twice,
//!   above every amount in the second.
//! - `customers_us`, `customers_eu`: each keyed by `id`, minting one
//!   template; customer 7 has a row in each.
//!
//! Three mappings over them:
//! - `sets-hist`: the history table and the orders, no key declared;
//! - `sets-union`: the history and current tables minting one class;
//! - `sets-keyed`: the current table and a per-version map over the history
//!   table, each with an `f:key` that makes its subjects unique, so every
//!   existing fast path keeps serving them.
//!
//! Every shape runs on SPARQL and on its JSON-LD twin, with fast paths on and
//! off, over the graph source and over a native ledger holding the triples the
//! mapping defines. Those triples are computed here from the fixture's rows,
//! independently of the materializer, and most answers are also pinned by
//! count so the oracle itself is checked.
//!
//! Own binary: it flips the process-wide fast-path switch, sets
//! `FLUREE_ICEBERG_LOCAL_ROOTS`, and asserts routing stamps.

#![cfg(all(feature = "iceberg", feature = "native"))]

#[path = "support/span_capture.rs"]
mod span_capture;

use std::collections::{BTreeMap, BTreeSet};

use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, R2rmlCreateConfig};
use serde_json::{json, Value};
use tokio::sync::Mutex;

/// Every test flips the process-wide fast-path switch.
static SERIAL: Mutex<()> = Mutex::const_new(());

const EX: &str = "http://example.org/";
const FUSED: &str = "fused_r2rml_aggregate";
/// The fused fold's stamp when it folds each solution of a map whose rows can
/// repeat one exactly once.
const FOLD_DEDUPE: &str = "fused_r2rml_aggregate_dedupe";
/// The scan's routing stamp: `proceed` when every map it reads is proven to
/// mint each triple once, so it skips deduplication.
const PROVEN: &str = "r2rml_scan_proven_distinct";
/// The scan's routing stamp for an `ORDER BY … LIMIT` it reads best-first,
/// skipping files that cannot hold the top k: `proceed` when it does.
const TOPK: &str = "r2rml_scan_topk";

/// `sets.customers_history`: `(customer_id, name, city)`, as the fixture
/// writer writes it (valid_from and is_current are not mapped here).
const HISTORY: [(Option<i64>, &str, Option<&str>, &str); 11] = [
    (Some(1), "alice", Some("Boston"), "2024-01-01"),
    (Some(2), "bob", Some("Austin"), "2024-01-01"),
    (Some(3), "carol", Some("Chicago"), "2024-02-01"),
    (Some(1), "alice", Some("Denver"), "2024-06-01"),
    (Some(5), "erin", Some("Miami"), "2024-01-01"),
    (Some(2), "robert", Some("Austin"), "2024-05-01"),
    (Some(2), "robert", Some("Austin"), "2024-09-01"),
    (Some(4), "dave", None, "2024-03-01"),
    (Some(1), "alice", Some("Denver"), "2024-11-01"),
    (Some(5), "erin-m", Some("Tampa"), "2024-07-01"),
    (None, "ghost", Some("Nowhere"), "2024-01-01"),
];

/// `sets.customers_current`: `(customer_id, name)`.
const CURRENT: [(i64, &str); 6] = [
    (1, "alice"),
    (2, "robert"),
    (3, "carol"),
    (4, "dave"),
    (5, "erin-m"),
    (6, "frank"),
];

/// `sets.orders`: `(order_id, customer_id)`.
const ORDERS: [(i64, Option<i64>); 8] = [
    (100, Some(1)),
    (101, Some(2)),
    (102, Some(3)),
    (103, Some(5)),
    (101, Some(2)),
    (104, Some(99)),
    (105, None),
    (106, Some(1)),
];

/// `sets.orders`: `(order_id, amount)`, in the same row order.
const AMOUNTS: [(i64, &str); 8] = [
    (100, "10.00"),
    (101, "20.00"),
    (102, "5.50"),
    (103, "7.25"),
    (101, "20.00"),
    (104, "1.00"),
    (105, "3.00"),
    (106, "2.00"),
];

/// `sets.order_lines`: `(line_id, order_id, amount)`, the first two rows in
/// one file and the rest in another.
const LINES: [(i64, i64, i64); 4] = [(1, 42, 100), (2, 42, 100), (3, 43, 50), (4, 44, 40)];

/// One regional table's rows: `(id, name, city)`.
type RegionalRows = [(i64, &'static str, &'static str); 2];

/// `sets.customers_us` and `sets.customers_eu`.
const REGIONAL: [(&str, RegionalRows); 2] = [
    ("us", [(7, "Ada", "Miami"), (8, "Bo", "Austin")]),
    ("eu", [(7, "Ada L.", "Lyon"), (9, "Cy", "Paris")]),
];

const HISTORY_MAP: &str = r#"
    <http://example.org/mapping#History>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_history" ] ;
        rr:subjectMap [
            rr:template "http://example.org/customer/{customer_id}" ;
            rr:class ex:Customer
        ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:city ; rr:objectMap [ rr:column "city" ] ] .
"#;

const ORDER_MAP: &str = r#"
    <http://example.org/mapping#Order>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.orders" ] ;
        rr:subjectMap [
            rr:template "http://example.org/order/{order_id}" ;
            rr:class ex:Order
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:customer ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#History> ;
                rr:joinCondition [ rr:child "customer_id" ; rr:parent "customer_id" ]
            ]
        ] .
"#;

const AMOUNT_MAP: &str = r#"
    <http://example.org/mapping#Amount>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.orders" ] ;
        rr:subjectMap [ rr:template "http://example.org/order/{order_id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:amount ;
            rr:objectMap [ rr:column "amount" ; rr:datatype xsd:decimal ]
        ] .
"#;

/// Each order's line amount, no key declared: order 42's two lines mint one
/// triple.
const ORDER_LINE_MAP: &str = r#"
    <http://example.org/mapping#OrderLine>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.order_lines" ] ;
        rr:subjectMap [ rr:template "http://example.org/order/{order_id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:amount ;
            rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ]
        ] .
"#;

/// Each line's amount, keyed by `line_id`: every row is its own triple.
const LINE_MAP: &str = r#"
    <http://example.org/mapping#Line>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.order_lines" ; f:key ( "line_id" ) ] ;
        rr:subjectMap [ rr:template "http://example.org/line/{line_id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:amount ;
            rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ]
        ] .
"#;

/// The two regional tables, each keyed by `id`, minting one customer
/// template: each map mints a subject from one row, but customer 7 is minted
/// by a row of each.
const REGIONAL_MAPS: &str = r#"
    <http://example.org/mapping#Us>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_us" ; f:key ( "id" ) ] ;
        rr:subjectMap [
            rr:template "http://example.org/customer/{id}" ;
            rr:class ex:Customer
        ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:city ; rr:objectMap [ rr:column "city" ] ] .

    <http://example.org/mapping#Eu>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_eu" ; f:key ( "id" ) ] ;
        rr:subjectMap [
            rr:template "http://example.org/customer/{id}" ;
            rr:class ex:Customer
        ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:city ; rr:objectMap [ rr:column "city" ] ] .
"#;

const CURRENT_MAP: &str = r#"
    <http://example.org/mapping#Current>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_current" ] ;
        rr:subjectMap [
            rr:template "http://example.org/customer/{customer_id}" ;
            rr:class ex:Customer
        ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
"#;

const KEYED_MAPS: &str = r#"
    <http://example.org/mapping#Current>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_current" ; f:key ( "customer_id" ) ] ;
        rr:subjectMap [
            rr:template "http://example.org/customer/{customer_id}" ;
            rr:class ex:Customer
        ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .

    <http://example.org/mapping#Version>
        a rr:TriplesMap ;
        rr:logicalTable [
            rr:tableName "sets.customers_history" ;
            f:key ( "customer_id" "valid_from" )
        ] ;
        rr:subjectMap [
            rr:template "http://example.org/version/{customer_id}/{valid_from}" ;
            rr:class ex:Version
        ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:city ; rr:objectMap [ rr:column "city" ] ] .
"#;

const PREFIXES: &str = r"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix f: <https://ns.flur.ee/db#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
";

fn ex(local: &str) -> String {
    format!("{EX}{local}")
}

fn customer(id: i64) -> String {
    ex(&format!("customer/{id}"))
}

fn order(id: i64) -> String {
    ex(&format!("order/{id}"))
}

fn version(id: i64, valid_from: &str) -> String {
    ex(&format!("version/{id}/{valid_from}"))
}

/// One graph source and the native ledger holding its triples.
struct Source {
    alias: &'static str,
    native: &'static str,
    maps: &'static [&'static str],
}

const HIST: Source = Source {
    alias: "sets-hist",
    native: "sets-hist-native:main",
    maps: &[HISTORY_MAP, ORDER_MAP],
};
const UNION: Source = Source {
    alias: "sets-union",
    native: "sets-union-native:main",
    maps: &[HISTORY_MAP, CURRENT_MAP],
};
const KEYED: Source = Source {
    alias: "sets-keyed",
    native: "sets-keyed-native:main",
    maps: &[KEYED_MAPS],
};
/// The orders' amounts, no key declared, an exact duplicate row.
const AMOUNTS_SRC: Source = Source {
    alias: "sets-amount",
    native: "sets-amount-native:main",
    maps: &[AMOUNT_MAP],
};
/// The two regional tables, each keyed.
const REGIONS: Source = Source {
    alias: "sets-regions",
    native: "sets-regions-native:main",
    maps: &[REGIONAL_MAPS],
};
/// The order lines per order, no key declared.
const LINES_PER_ORDER: Source = Source {
    alias: "sets-order-lines",
    native: "sets-order-lines-native:main",
    maps: &[ORDER_LINE_MAP],
};
/// The order lines per line, keyed.
const LINES_KEYED: Source = Source {
    alias: "sets-lines",
    native: "sets-lines-native:main",
    maps: &[LINE_MAP],
};
/// The current table alone, no key declared: its Iceberg identifier field
/// is the key.
const IDENT: Source = Source {
    alias: "sets-ident",
    native: "sets-ident-native:main",
    maps: &[CURRENT_MAP],
};

impl Source {
    fn from(&self) -> String {
        format!("{}:main", self.alias)
    }

    fn mapping(&self) -> String {
        let mut m = PREFIXES.to_string();
        for map in self.maps {
            m.push_str(map);
        }
        m
    }

    /// The triples the mapping defines over the fixture rows, as JSON-LD
    /// nodes: one node per subject, each predicate's objects a set. A row with
    /// a NULL subject column mints nothing; a NULL object column mints no
    /// triple for that predicate; a referencing map joins a child row to every
    /// parent row whose join column equals the child's.
    fn graph(&self) -> Vec<Value> {
        let mut nodes: BTreeMap<String, BTreeMap<String, BTreeSet<String>>> = BTreeMap::new();
        let mut add = |s: String, p: &str, o: String| {
            nodes
                .entry(s)
                .or_default()
                .entry(p.to_string())
                .or_default()
                .insert(o);
        };
        let iri = |i: String| format!("<{i}>");
        let has = |m: &str| self.maps.contains(&m);
        if has(HISTORY_MAP) {
            for (id, name, city, _) in HISTORY {
                let Some(id) = id else { continue };
                add(customer(id), "@type", iri(ex("Customer")));
                add(customer(id), "name", name.to_string());
                if let Some(c) = city {
                    add(customer(id), "city", c.to_string());
                }
            }
        }
        if has(CURRENT_MAP) {
            for (id, name) in CURRENT {
                add(customer(id), "@type", iri(ex("Customer")));
                add(customer(id), "name", name.to_string());
            }
        }
        if has(ORDER_MAP) {
            for (oid, cid) in ORDERS {
                add(order(oid), "@type", iri(ex("Order")));
                let Some(cid) = cid else { continue };
                if HISTORY.iter().any(|(id, ..)| *id == Some(cid)) {
                    add(order(oid), "customer", iri(customer(cid)));
                }
            }
        }
        if has(AMOUNT_MAP) {
            for (oid, amount) in AMOUNTS {
                add(order(oid), "amount", format!("decimal:{amount}"));
            }
        }
        if has(REGIONAL_MAPS) {
            for (_, rows) in REGIONAL {
                for (id, name, city) in rows {
                    add(customer(id), "@type", iri(ex("Customer")));
                    add(customer(id), "name", name.to_string());
                    add(customer(id), "city", city.to_string());
                }
            }
        }
        if has(ORDER_LINE_MAP) {
            for (_, oid, amount) in LINES {
                add(order(oid), "amount", format!("integer:{amount}"));
            }
        }
        if has(LINE_MAP) {
            for (lid, _, amount) in LINES {
                add(
                    ex(&format!("line/{lid}")),
                    "amount",
                    format!("integer:{amount}"),
                );
            }
        }
        if has(KEYED_MAPS) {
            for (id, name) in CURRENT {
                add(customer(id), "@type", iri(ex("Customer")));
                add(customer(id), "name", name.to_string());
            }
            for (id, name, city, valid_from) in HISTORY {
                let Some(id) = id else { continue };
                let v = version(id, valid_from);
                add(v.clone(), "@type", iri(ex("Version")));
                add(v.clone(), "name", name.to_string());
                if let Some(c) = city {
                    add(v, "city", c.to_string());
                }
            }
        }
        nodes
            .into_iter()
            .map(|(s, preds)| {
                let mut node = json!({ "@id": s });
                for (p, objects) in preds {
                    let key = if p == "@type" { p.clone() } else { ex(&p) };
                    let values: Vec<Value> = objects
                        .into_iter()
                        .map(
                            |o| match o.strip_prefix('<').and_then(|o| o.strip_suffix('>')) {
                                Some(i) if p == "@type" => json!(i),
                                Some(i) => json!({ "@id": i }),
                                None => {
                                    match (o.strip_prefix("decimal:"), o.strip_prefix("integer:")) {
                                        (Some(d), _) => json!({
                                            "@value": d,
                                            "@type": "http://www.w3.org/2001/XMLSchema#decimal"
                                        }),
                                        (_, Some(n)) => json!({
                                            "@value": n.parse::<i64>().expect("an integer"),
                                            "@type": "http://www.w3.org/2001/XMLSchema#integer"
                                        }),
                                        _ => json!(o),
                                    }
                                }
                            },
                        )
                        .collect();
                    node[key] = Value::Array(values);
                }
                node
            })
            .collect()
    }
}

/// Allowlist the fixtures directory, before anything builds Iceberg storage.
fn allow_fixture_roots() {
    // SAFETY: set before any storage or scan is built; every test in this
    // binary sets the same value.
    std::env::set_var(
        "FLUREE_ICEBERG_LOCAL_ROOTS",
        format!("{}/tests/fixtures/iceberg", env!("CARGO_MANIFEST_DIR")),
    );
}

/// Register each source and the native ledger holding its triples.
async fn seed(fluree: &Fluree, sources: &[&Source]) {
    let root = format!(
        "file://{}/tests/fixtures/iceberg/sets",
        env!("CARGO_MANIFEST_DIR")
    );
    for source in sources {
        fluree
            .create_r2rml_graph_source(
                R2rmlCreateConfig::new_direct(source.alias, &root, source.mapping())
                    .with_mapping_media_type("text/turtle"),
            )
            .await
            .unwrap_or_else(|e| panic!("create graph source {}: {e}", source.alias));
        let native = fluree
            .create_ledger(source.native)
            .await
            .expect("create native ledger");
        fluree
            .insert(native, &json!({ "@graph": source.graph() }))
            .await
            .expect("seed native ledger");
    }
}

#[derive(Clone)]
struct Case {
    name: &'static str,
    /// The SPARQL text; `{from}` names the dataset.
    sparql: String,
    /// The JSON-LD twin, without `from`.
    jsonld: Value,
    /// The output variables, in select order.
    vars: &'static [&'static str],
    /// The native answer's row count, where pinning it checks the oracle.
    rows: Option<usize>,
    /// A routing stamp the graph-source run (fast paths on) must (`true`) or
    /// must not (`false`) record as `proceed`.
    stamps: Vec<(&'static str, bool)>,
    /// How many table scans the graph-source run (fast paths on) opens, as an
    /// inclusive range: a fused star reads its table once, its members run
    /// as separate scans read it more than once.
    scans: Option<(usize, usize)>,
}

fn case(name: &'static str, sparql: &str, jsonld: Value, vars: &'static [&'static str]) -> Case {
    Case {
        name,
        sparql: format!("PREFIX ex: <{EX}> {sparql}"),
        jsonld,
        vars,
        rows: None,
        stamps: Vec::new(),
        scans: None,
    }
}

impl Case {
    fn rows(mut self, n: usize) -> Self {
        self.rows = Some(n);
        self
    }

    fn stamp(mut self, site: &'static str, fires: bool) -> Self {
        self.stamps.push((site, fires));
        self
    }

    fn scans(mut self, at_least: usize, at_most: usize) -> Self {
        self.scans = Some((at_least, at_most));
        self
    }
}

/// A term's text: an IRI, or a literal's lexical form.
fn text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Object(o) => o
            .get("value")
            .or_else(|| o.get("@id"))
            .or_else(|| o.get("@value"))
            .map(text)
            .unwrap_or_else(|| panic!("not a term: {v}")),
        Value::Array(_) => panic!("not a term: {v}"),
    }
}

/// A result's rows as the text of `vars`, sorted (duplicates kept).
fn result_rows(result: &Value, vars: &[&str]) -> Vec<String> {
    let mut rows: Vec<String> = if let Some(bindings) = result["results"]["bindings"].as_array() {
        bindings
            .iter()
            .map(|b| {
                vars.iter()
                    .map(|v| text(&b[*v]))
                    .collect::<Vec<_>>()
                    .join(" | ")
            })
            .collect()
    } else {
        result
            .as_array()
            .unwrap_or_else(|| panic!("not a result: {result}"))
            .iter()
            .map(|row| match row {
                Value::Array(cells) => cells.iter().map(text).collect::<Vec<_>>().join(" | "),
                cell => text(cell),
            })
            .collect()
    };
    rows.sort();
    rows
}

/// One run's rows, the stamps recorded as `proceed`, and its table scans.
struct Run {
    rows: Vec<String>,
    proceeded: BTreeSet<String>,
    stamped: BTreeSet<String>,
    scans: usize,
}

async fn run(
    fluree: &Fluree,
    store: &span_capture::SpanStore,
    c: &Case,
    from: &str,
    jsonld: bool,
    fast: bool,
) -> Run {
    set_fast_paths_disabled(!fast);
    let before = store.find_events("fast-path outcome").len();
    let scans_before = store.find_spans("r2rml.scan_table").len();
    let result = if jsonld {
        let mut q = c.jsonld.clone();
        q["from"] = json!(from);
        fluree.query_from().jsonld(&q).execute_formatted().await
    } else {
        fluree
            .query_from()
            .sparql(&c.sparql.replace("{from}", from))
            .execute_formatted()
            .await
    };
    set_fast_paths_disabled(false);
    let rows = match result {
        Ok(v) => result_rows(&v, c.vars),
        Err(e) => vec![format!("ERROR {e}")],
    };
    let events = store.find_events("fast-path outcome");
    let mut proceeded = BTreeSet::new();
    let mut stamped = BTreeSet::new();
    for e in &events[before..] {
        let Some(site) = e.fields.get("site") else {
            continue;
        };
        stamped.insert(site.clone());
        if e.fields.get("outcome").map(String::as_str) == Some("proceed") {
            proceeded.insert(site.clone());
        }
    }
    Run {
        rows,
        proceeded,
        stamped,
        scans: store.find_spans("r2rml.scan_table").len() - scans_before,
    }
}

/// Run every case on both surfaces over `source` (fast and generic) and over
/// its native ledger, and describe every disagreement.
async fn check(fluree: &Fluree, cases: &[Case], source: &Source) -> Vec<String> {
    let (store, _guard) = span_capture::init_test_tracing();
    let from = source.from();
    let mut failures = Vec::new();
    for c in cases {
        let mut problems = Vec::new();
        for (surface, jsonld) in [("SPARQL", false), ("JSON-LD", true)] {
            let native = run(fluree, &store, c, source.native, jsonld, true).await;
            if let Some(n) = c.rows {
                if native.rows.len() != n {
                    problems.push(format!(
                        "{surface}: the native answer has {} rows, not {n}: {:?}",
                        native.rows.len(),
                        native.rows
                    ));
                }
            }
            for fast in [true, false] {
                let lane = if fast { "fast" } else { "generic" };
                let got = run(fluree, &store, c, &from, jsonld, fast).await;
                if got.rows != native.rows {
                    problems.push(format!(
                        "{surface} {lane}: {} rows {:?}, native {} rows {:?}",
                        got.rows.len(),
                        got.rows,
                        native.rows.len(),
                        native.rows
                    ));
                }
                if !fast {
                    // With fast paths off nothing relies on a key: every
                    // scan deduplicates, so the generic lane is the oracle
                    // for the key-dependent decisions too.
                    for (site, _) in &c.stamps {
                        if got.proceeded.contains(*site) {
                            problems.push(format!("{surface} generic: `{site}` proceeded"));
                        }
                    }
                    continue;
                }
                if let Some((lo, hi)) = c.scans {
                    if !(lo..=hi).contains(&got.scans) {
                        problems.push(format!(
                            "{surface}: {} table scans, expected {lo}..={hi}",
                            got.scans
                        ));
                    }
                }
                for (site, fires) in &c.stamps {
                    let proceeded = got.proceeded.contains(*site);
                    if *fires && !proceeded {
                        problems.push(format!(
                            "{surface}: `{site}` must proceed (stamped: {:?})",
                            got.stamped
                        ));
                    }
                    if !*fires && proceeded {
                        problems.push(format!("{surface}: `{site}` must not proceed"));
                    }
                }
            }
        }
        if !problems.is_empty() {
            failures.push(format!("{}:\n  {}", c.name, problems.join("\n  ")));
        }
    }
    failures
}

fn select(vars: &[&str], where_clause: Value) -> Value {
    json!({ "select": vars.iter().map(|v| format!("?{v}")).collect::<Vec<_>>(), "where": where_clause })
}

fn values(var: &str, iris: &[String]) -> Value {
    json!([
        "values",
        [
            format!("?{var}"),
            iris.iter().map(|i| json!({ "@id": i })).collect::<Vec<_>>()
        ]
    ])
}

fn count(name: &'static str, sparql_where: &str, var: &str, jsonld_where: Value) -> Case {
    case(
        name,
        &format!("SELECT (COUNT(?{var}) AS ?k) FROM <{{from}}> WHERE {{ {sparql_where} }}"),
        json!({ "select": [format!("(as (count ?{var}) ?k)")], "where": jsonld_where }),
        &["k"],
    )
    .rows(1)
}

/// Patterns over the history table and the orders: no key is declared, so
/// every map may mint a triple more than once.
fn history_cases() -> Vec<Case> {
    let name = ex("name");
    let city = ex("city");
    let customer_class = ex("Customer");
    vec![
        case(
            "a predicate over versions matches each triple once",
            "SELECT ?s ?n FROM <{from}> WHERE { ?s ex:name ?n }",
            select(&["s", "n"], json!({ "@id": "?s", name.clone(): "?n" })),
            &["s", "n"],
        )
        .rows(7)
        .stamp(PROVEN, false),
        case(
            "a second predicate over versions",
            "SELECT ?s ?c FROM <{from}> WHERE { ?s ex:city ?c }",
            select(&["s", "c"], json!({ "@id": "?s", city.clone(): "?c" })),
            &["s", "c"],
        )
        .rows(6),
        case(
            "a class over versions matches each subject once",
            "SELECT ?s FROM <{from}> WHERE { ?s a ex:Customer }",
            select(&["s"], json!({ "@id": "?s", "@type": customer_class.clone() })),
            &["s"],
        )
        .rows(5),
        case(
            "a star over versions is each subject's cross product",
            "SELECT ?s ?n ?c FROM <{from}> WHERE { ?s ex:name ?n ; ex:city ?c }",
            select(
                &["s", "n", "c"],
                json!({ "@id": "?s", name.clone(): "?n", city.clone(): "?c" }),
            ),
            &["s", "n", "c"],
        )
        .rows(9)
        .scans(2, usize::MAX),
        case(
            "a star's constant matches any version",
            "SELECT ?s ?n FROM <{from}> WHERE { ?s ex:name ?n ; ex:city \"Miami\" }",
            select(&["s", "n"], json!({ "@id": "?s", name.clone(): "?n", city.clone(): "Miami" })),
            &["s", "n"],
        )
        .rows(2)
        .scans(2, usize::MAX),
        case(
            "a class beside a star over versions",
            "SELECT ?s ?n ?c FROM <{from}> WHERE { ?s a ex:Customer ; ex:name ?n ; ex:city ?c }",
            select(
                &["s", "n", "c"],
                json!({ "@id": "?s", "@type": customer_class.clone(), name.clone(): "?n", city.clone(): "?c" }),
            ),
            &["s", "n", "c"],
        )
        .rows(9),
        case(
            "an OPTIONAL over versions",
            "SELECT ?s ?c FROM <{from}> WHERE { ?s a ex:Customer OPTIONAL { ?s ex:city ?c } }",
            select(
                &["s", "c"],
                json!([
                    { "@id": "?s", "@type": customer_class.clone() },
                    ["optional", { "@id": "?s", city.clone(): "?c" }]
                ]),
            ),
            &["s", "c"],
        )
        .rows(7),
        case(
            "a bound subject's wildcard over versions",
            "SELECT ?p ?o FROM <{from}> WHERE { <http://example.org/customer/2> ?p ?o }",
            select(&["p", "o"], json!({ "@id": customer(2), "?p": "?o" })),
            &["p", "o"],
        )
        .rows(4),
        case(
            "a wildcard over versions and a duplicate row",
            "SELECT ?s ?p ?o FROM <{from}> WHERE { ?s ?p ?o }",
            select(&["s", "p", "o"], json!({ "@id": "?s", "?p": "?o" })),
            &["s", "p", "o"],
        )
        .rows(30),
        case(
            "a class over a duplicate row",
            "SELECT ?o FROM <{from}> WHERE { ?o a ex:Order }",
            select(&["o"], json!({ "@id": "?o", "@type": ex("Order") })),
            &["o"],
        )
        .rows(7),
        case(
            "a referencing map over a duplicate row",
            "SELECT ?o ?c FROM <{from}> WHERE { ?o ex:customer ?c }",
            select(&["o", "c"], json!({ "@id": "?o", ex("customer"): "?c" })),
            &["o", "c"],
        )
        .rows(5),
        case(
            "a join through versions",
            "SELECT ?o ?n FROM <{from}> WHERE { ?o ex:customer ?c . ?c ex:name ?n }",
            select(
                &["o", "n"],
                json!([{ "@id": "?o", ex("customer"): "?c" }, { "@id": "?c", name.clone(): "?n" }]),
            ),
            &["o", "n"],
        )
        .rows(7),
        case(
            "a VALUES subject set over versions",
            "SELECT ?s ?c FROM <{from}> WHERE { \
             VALUES ?s { <http://example.org/customer/1> <http://example.org/customer/5> } \
             ?s ex:city ?c }",
            select(
                &["s", "c"],
                json!([values("s", &[customer(1), customer(5)]), { "@id": "?s", city.clone(): "?c" }]),
            ),
            &["s", "c"],
        )
        .rows(4),
        // The scan's input is a bag: a repeated input row joins each triple
        // once per repeat, as on a native ledger. Only the triples are a set.
        case(
            "a repeated VALUES row joins each triple once per repeat",
            "SELECT ?s ?n FROM <{from}> WHERE { \
             VALUES ?s { <http://example.org/customer/2> <http://example.org/customer/2> \
             <http://example.org/customer/5> } ?s ex:name ?n }",
            select(
                &["s", "n"],
                json!([
                    values("s", &[customer(2), customer(2), customer(5)]),
                    { "@id": "?s", name.clone(): "?n" }
                ]),
            ),
            &["s", "n"],
        )
        .rows(6)
        .stamp(PROVEN, false),
        case(
            "a UNION repeating a row joins each triple once per repeat",
            "SELECT ?s ?n FROM <{from}> WHERE { \
             { VALUES ?s { <http://example.org/customer/2> } } UNION \
             { VALUES ?s { <http://example.org/customer/2> } } ?s ex:name ?n }",
            select(
                &["s", "n"],
                json!([
                    ["union", [values("s", &[customer(2)])], [values("s", &[customer(2)])]],
                    { "@id": "?s", name.clone(): "?n" }
                ]),
            ),
            &["s", "n"],
        )
        .rows(4)
        .stamp(PROVEN, false),
        case(
            "repeated join rows reach each version's triples once per repeat",
            "SELECT ?c ?n FROM <{from}> WHERE { ?o ex:customer ?c . ?c ex:name ?n }",
            select(
                &["c", "n"],
                json!([{ "@id": "?o", ex("customer"): "?c" }, { "@id": "?c", name.clone(): "?n" }]),
            ),
            &["c", "n"],
        )
        .rows(7),
        case(
            "DISTINCT over versions",
            "SELECT DISTINCT ?n FROM <{from}> WHERE { ?s ex:name ?n }",
            json!({ "selectDistinct": ["?n"], "where": { "@id": "?s", name.clone(): "?n" } }),
            &["n"],
        )
        .rows(7),
        // A single pattern over one keyless map folds each solution once.
        count(
            "COUNT a predicate over versions",
            "?s ex:name ?n",
            "n",
            json!({ "@id": "?s", name.clone(): "?n" }),
        )
        .stamp(FUSED, true)
        .stamp(FOLD_DEDUPE, true),
        count(
            "COUNT a class over versions",
            "?s a ex:Customer",
            "s",
            json!({ "@id": "?s", "@type": customer_class.clone() }),
        )
        .stamp(FUSED, true)
        .stamp(FOLD_DEDUPE, true),
        count(
            "COUNT a class over a duplicate row",
            "?o a ex:Order",
            "o",
            json!({ "@id": "?o", "@type": ex("Order") }),
        )
        .stamp(FUSED, true)
        .stamp(FOLD_DEDUPE, true),
        count(
            "COUNT a referencing map over a duplicate row",
            "?o ex:customer ?c",
            "c",
            json!({ "@id": "?o", ex("customer"): "?c" }),
        )
        .stamp(FUSED, false),
        case(
            "GROUP BY a column over versions",
            "SELECT ?c (COUNT(?s) AS ?k) FROM <{from}> WHERE { ?s ex:city ?c } GROUP BY ?c",
            json!({
                "select": ["?c", "(as (count ?s) ?k)"],
                "where": { "@id": "?s", city.clone(): "?c" },
                "groupBy": ["?c"],
            }),
            &["c", "k"],
        )
        .rows(6)
        .stamp(FUSED, true)
        .stamp(FOLD_DEDUPE, true),
    ]
}

/// Two maps over different tables minting the same subjects and class.
fn union_cases() -> Vec<Case> {
    let name = ex("name");
    vec![
        case(
            "a class over two maps matches each subject once",
            "SELECT ?s FROM <{from}> WHERE { ?s a ex:Customer }",
            select(&["s"], json!({ "@id": "?s", "@type": ex("Customer") })),
            &["s"],
        )
        .rows(6),
        case(
            "a predicate over two maps matches each triple once",
            "SELECT ?s ?n FROM <{from}> WHERE { ?s ex:name ?n }",
            select(&["s", "n"], json!({ "@id": "?s", name.clone(): "?n" })),
            &["s", "n"],
        )
        .rows(8),
        case(
            "a type variable over two maps",
            "SELECT ?s ?t FROM <{from}> WHERE { ?s a ?t }",
            select(&["s", "t"], json!({ "@id": "?s", "@type": "?t" })),
            &["s", "t"],
        )
        .rows(6),
        case(
            "a wildcard over two maps",
            "SELECT ?s ?p ?o FROM <{from}> WHERE { ?s ?p ?o }",
            select(&["s", "p", "o"], json!({ "@id": "?s", "?p": "?o" })),
            &["s", "p", "o"],
        )
        .rows(20),
        count(
            "COUNT a class over two maps",
            "?s a ex:Customer",
            "s",
            json!({ "@id": "?s", "@type": ex("Customer") }),
        )
        .stamp(FUSED, false),
    ]
}

/// Aggregates over a decimal column with an exact duplicate row: the fused
/// fold folds each solution once.
fn amount_cases() -> Vec<Case> {
    let amount = ex("amount");
    vec![
        case(
            "SUM over a duplicate row",
            "SELECT (SUM(?a) AS ?t) FROM <{from}> WHERE { ?o ex:amount ?a }",
            json!({ "select": ["(as (sum ?a) ?t)"], "where": { "@id": "?o", amount.clone(): "?a" } }),
            &["t"],
        )
        .rows(1)
        .stamp(FUSED, true)
        .stamp(FOLD_DEDUPE, true),
        case(
            "GROUP BY a decimal over a duplicate row",
            "SELECT ?a (COUNT(?o) AS ?k) FROM <{from}> WHERE { ?o ex:amount ?a } GROUP BY ?a",
            json!({
                "select": ["?a", "(as (count ?o) ?k)"],
                "where": { "@id": "?o", amount.clone(): "?a" },
                "groupBy": ["?a"],
            }),
            &["a", "k"],
        )
        // The fused fold does not group by a decimal; the generic lane answers.
        .rows(7),
    ]
}

/// Stars over a subject two keyed maps mint, from a row of each: each map's
/// rows are one per subject, but the subject's objects come from both, and
/// the graph pairs every name with every city across them.
fn regional_cases() -> Vec<Case> {
    let name = ex("name");
    let city = ex("city");
    vec![
        case(
            "a star over a subject two keyed maps mint",
            "SELECT ?s ?n ?c FROM <{from}> WHERE { ?s ex:name ?n ; ex:city ?c }",
            select(
                &["s", "n", "c"],
                json!({ "@id": "?s", name.clone(): "?n", city.clone(): "?c" }),
            ),
            &["s", "n", "c"],
        )
        // Customer 7: two names by two cities; 8 and 9 one each.
        .rows(6),
        case(
            "a star's constant over a subject two keyed maps mint",
            "SELECT ?s ?n FROM <{from}> WHERE { ?s ex:name ?n ; ex:city \"Miami\" }",
            select(
                &["s", "n"],
                json!({ "@id": "?s", name.clone(): "?n", city.clone(): "Miami" }),
            ),
            &["s", "n"],
        )
        .rows(2),
        count(
            "COUNT a star over a subject two keyed maps mint",
            "?s ex:name ?n ; ex:city ?c",
            "c",
            json!({ "@id": "?s", name.clone(): "?n", city.clone(): "?c" }),
        ),
        case(
            "a typed star over a subject two keyed maps mint",
            "SELECT ?s ?n ?c FROM <{from}> WHERE { ?s a ex:Customer ; ex:name ?n ; ex:city ?c }",
            select(
                &["s", "n", "c"],
                json!({
                    "@id": "?s",
                    "@type": ex("Customer"),
                    name.clone(): "?n",
                    city.clone(): "?c"
                }),
            ),
            &["s", "n", "c"],
        )
        .rows(6),
    ]
}

/// `ORDER BY DESC(?a) LIMIT k [OFFSET n]` over the order lines' amounts. The
/// first file holds the two largest amounts; the scan-side top-k reads it
/// first, and its bound tells it whether the second file can hold any of the
/// top k. Per order, the first file's two rows are one triple, so the second
/// file holds the second-largest; per line, they are two.
fn top_k_cases(keyed: bool) -> Vec<Case> {
    let amount = ex("amount");
    let order_by = |name, vars: &'static [&'static str], limit: usize, offset: usize| {
        let projection = vars.iter().map(|v| format!("?{v}")).collect::<Vec<_>>();
        let mut jsonld = json!({
            "select": projection,
            "where": { "@id": "?s", amount.clone(): "?a" },
            "orderBy": "(desc ?a)",
            "limit": limit,
        });
        if offset > 0 {
            jsonld["offset"] = json!(offset);
        }
        let offset_clause = if offset > 0 {
            format!(" OFFSET {offset}")
        } else {
            String::new()
        };
        case(
            name,
            &format!(
                "SELECT {} FROM <{{from}}> WHERE {{ ?s ex:amount ?a }} \
                 ORDER BY DESC(?a) LIMIT {limit}{offset_clause}",
                projection.join(" ")
            ),
            jsonld,
            vars,
        )
        .rows(limit)
        // The scan reads its top k from the first file; deduplicating, it
        // must read both.
        .stamp(TOPK, keyed)
        .stamp(PROVEN, keyed)
    };
    vec![
        order_by("ORDER BY DESC LIMIT 2", &["s", "a"], 2, 0),
        order_by("ORDER BY DESC LIMIT 1 OFFSET 1", &["a"], 1, 1),
        order_by("ORDER BY DESC LIMIT 3", &["s", "a"], 3, 0),
    ]
}

/// Maps whose declared key makes each subject one row's: every existing fast
/// path still serves them.
fn keyed_cases() -> Vec<Case> {
    let name = ex("name");
    let city = ex("city");
    vec![
        case(
            "a keyed predicate skips deduplication",
            "SELECT ?s ?n FROM <{from}> WHERE { ?s a ex:Customer ; ex:name ?n }",
            select(
                &["s", "n"],
                json!({ "@id": "?s", "@type": ex("Customer"), name.clone(): "?n" }),
            ),
            &["s", "n"],
        )
        .rows(6)
        .stamp(PROVEN, true)
        .scans(1, 1),
        case(
            "a keyed star",
            "SELECT ?v ?n ?c FROM <{from}> WHERE { ?v ex:name ?n ; ex:city ?c }",
            select(
                &["v", "n", "c"],
                json!({ "@id": "?v", name.clone(): "?n", city.clone(): "?c" }),
            ),
            &["v", "n", "c"],
        )
        .rows(9)
        .stamp(PROVEN, true)
        .scans(1, 1),
        count(
            "COUNT a keyed class",
            "?v a ex:Version",
            "v",
            json!({ "@id": "?v", "@type": ex("Version") }),
        )
        .stamp(FUSED, true)
        .stamp(FOLD_DEDUPE, false),
        count(
            "COUNT a keyed predicate",
            "?v ex:city ?c",
            "c",
            json!({ "@id": "?v", city.clone(): "?c" }),
        )
        .stamp(FUSED, true)
        .stamp(FOLD_DEDUPE, false),
        case(
            "a keyed star's constant",
            "SELECT ?v ?n FROM <{from}> WHERE { ?v ex:name ?n ; ex:city \"Miami\" }",
            select(
                &["v", "n"],
                json!({ "@id": "?v", name.clone(): "?n", city.clone(): "Miami" }),
            ),
            &["v", "n"],
        )
        .rows(1)
        .stamp(PROVEN, true)
        .scans(1, 1),
    ]
}

/// A map keyed by its table's Iceberg identifier field alone.
fn identifier_cases() -> Vec<Case> {
    vec![
        case(
            "an identifier-keyed predicate skips deduplication",
            "SELECT ?s ?n FROM <{from}> WHERE { ?s <http://example.org/name> ?n }",
            select(&["s", "n"], json!({ "@id": "?s", ex("name"): "?n" })),
            &["s", "n"],
        )
        .rows(6)
        .stamp(PROVEN, true),
        count(
            "COUNT an identifier-keyed class",
            "?s a ex:Customer",
            "s",
            json!({ "@id": "?s", "@type": ex("Customer") }),
        )
        .stamp(FUSED, true)
        .stamp(FOLD_DEDUPE, false),
    ]
}

/// Every pattern over rows that repeat a triple answers as the native ledger
/// holding the triples does, on every lane.
#[tokio::test]
async fn patterns_over_repeated_rows_match_each_triple_once() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, &[&HIST, &UNION, &AMOUNTS_SRC]).await;
    let mut failures = check(&fluree, &history_cases(), &HIST).await;
    failures.extend(check(&fluree, &union_cases(), &UNION).await);
    failures.extend(check(&fluree, &amount_cases(), &AMOUNTS_SRC).await);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A map whose declared key proves each subject one row's keeps every fast
/// path, and answers as the native ledger does.
#[tokio::test]
async fn keyed_maps_keep_their_fast_paths() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, &[&KEYED, &IDENT]).await;
    let mut failures = check(&fluree, &keyed_cases(), &KEYED).await;
    failures.extend(check(&fluree, &identifier_cases(), &IDENT).await);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A same-subject star fuses its members into one scan only where each
/// subject's objects are one row's. Two keyed maps minting one template each
/// hold one row per subject, but a subject in both has a row in each: the
/// star answers the cross product across them, as the native ledger does,
/// on every lane, in a COUNT, and in a crawl folding a constant.
#[tokio::test]
async fn a_star_over_a_subject_two_maps_mint_pairs_their_rows() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, &[&REGIONS]).await;
    let mut failures = check(&fluree, &regional_cases(), &REGIONS).await;
    for where_clause in [
        json!({ "@id": "?s", "ex:city": "Miami" }),
        json!({ "@id": "?s", "@type": "ex:Customer", "ex:city": "Miami" }),
    ] {
        let mut answers = Vec::new();
        for from in [REGIONS.from(), REGIONS.native.to_string()] {
            let query = json!({
                "@context": { "ex": EX },
                "from": from,
                "select": { "?s": ["*"] },
                "where": where_clause.clone(),
                "orderBy": "?s",
            });
            let result = fluree
                .query_from()
                .jsonld(&query)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("crawl on {from}: {e}"));
            answers.push(normalize_docs(&result));
        }
        if answers[0] != answers[1] || answers[1].is_empty() {
            failures.push(format!(
                "crawl {where_clause}: graph source {:#?}\nnative {:#?}",
                answers[0], answers[1]
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A scan-side top-k reads a map's files best-first and stops once no unread
/// file can beat the k-th value it has seen. Over a map whose rows can repeat
/// a triple, the rows it has seen are not k triples, so it declines; over a
/// keyed map it still reads only the first file.
#[tokio::test]
async fn a_top_k_counts_triples_not_rows() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, &[&LINES_PER_ORDER, &LINES_KEYED]).await;
    let mut failures = check(&fluree, &top_k_cases(false), &LINES_PER_ORDER).await;
    failures.extend(check(&fluree, &top_k_cases(true), &LINES_KEYED).await);
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    // The keyed top-k reads the first file and skips the second.
    let (store, _guard) = span_capture::init_test_tracing();
    let before = store.find_events("scan-side top-k prune").len();
    let c = &top_k_cases(true)[0];
    let got = run(&fluree, &store, c, &LINES_KEYED.from(), false, true).await;
    assert_eq!(got.rows.len(), 2, "{:?}", got.rows);
    let prunes = &store.find_events("scan-side top-k prune")[before..];
    assert!(
        prunes
            .iter()
            .any(|e| e.fields.get("files_pruned").map(String::as_str) == Some("1")),
        "the keyed top-k must read one file and prune the other: {prunes:?}"
    );
}

/// A declared key naming a column the table lacks refuses every query over
/// the map, on both surfaces: a scan never trusts a key its table cannot hold.
#[tokio::test]
async fn a_declared_key_the_table_cannot_hold_refuses_the_query() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = format!(
        "file://{}/tests/fixtures/iceberg/sets",
        env!("CARGO_MANIFEST_DIR")
    );
    let mapping = format!(
        "{PREFIXES}{}",
        KEYED_MAPS.replace(
            r#"f:key ( "customer_id" )"#,
            r#"f:key ( "no_such_column" )"#
        )
    );
    assert!(mapping.contains("no_such_column"));
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("sets-badkey", &root, mapping)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create graph source");
    let sparql = format!(
        "PREFIX ex: <{EX}> SELECT ?s ?n FROM <sets-badkey:main> WHERE {{ ?s a ex:Customer ; ex:name ?n }}"
    );
    let jsonld = json!({
        "from": "sets-badkey:main",
        "select": ["?s", "?n"],
        "where": { "@id": "?s", "@type": ex("Customer"), ex("name"): "?n" },
    });
    let errors = [
        fluree
            .query_from()
            .sparql(&sparql)
            .execute_formatted()
            .await,
        fluree
            .query_from()
            .jsonld(&jsonld)
            .execute_formatted()
            .await,
    ];
    for (surface, result) in ["SPARQL", "JSON-LD"].iter().zip(errors) {
        let err = result.expect_err(&format!("{surface}: a bad declared key must refuse"));
        let text = err.to_string();
        assert!(
            text.contains("no_such_column") && text.contains("sets.customers_current"),
            "{surface}: {text}"
        );
    }
}

/// A `LIMIT` over rows that repeat a triple answers distinct rows of the whole
/// answer, as many as the answer has up to the limit.
#[tokio::test]
async fn a_limit_over_repeated_rows_answers_distinct_rows() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, &[&HIST, &UNION]).await;
    let (store, _guard) = span_capture::init_test_tracing();
    let mut wrong = Vec::new();
    for (source, pattern, vars) in [
        (&HIST, "?s <http://example.org/name> ?n", &["s", "n"][..]),
        (&HIST, "?s a <http://example.org/Customer>", &["s"][..]),
        (&UNION, "?s a <http://example.org/Customer>", &["s"][..]),
        (&UNION, "?s ?p ?o", &["s", "p", "o"][..]),
    ] {
        let projection = vars.iter().map(|v| format!("?{v}")).collect::<Vec<_>>();
        let whole = Case {
            name: "whole",
            sparql: format!(
                "SELECT {} FROM <{{from}}> WHERE {{ {pattern} }}",
                projection.join(" ")
            ),
            jsonld: json!({}),
            vars,
            rows: None,
            stamps: Vec::new(),
            scans: None,
        };
        let native = run(&fluree, &store, &whole, source.native, false, true)
            .await
            .rows;
        for k in 1..=native.len() + 1 {
            let limited = Case {
                sparql: format!("{} LIMIT {k}", whole.sparql),
                ..whole.clone()
            };
            for fast in [true, false] {
                let rows = run(&fluree, &store, &limited, &source.from(), false, fast)
                    .await
                    .rows;
                let distinct: BTreeSet<&String> = rows.iter().collect();
                if rows.len() != k.min(native.len())
                    || distinct.len() != rows.len()
                    || !rows.iter().all(|r| native.contains(r))
                {
                    wrong.push(format!(
                        "{} `{pattern}` LIMIT {k} (fast {fast}): {rows:?}",
                        source.alias
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A subgraph crawl over versions lists each value once, as the crawl over the
/// native ledger does.
#[tokio::test]
async fn a_crawl_over_versions_lists_each_value_once() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, &[&HIST, &UNION]).await;
    let mut wrong = Vec::new();
    for (source, where_clause) in [
        (&HIST, json!({ "@id": "?s", "@type": "ex:Customer" })),
        (&UNION, json!({ "@id": "?s", "@type": "ex:Customer" })),
        // A constant the crawl folds onto its wildcard: the subject holds it
        // on one version, and its document lists every version's values.
        (&HIST, json!({ "@id": "?s", "ex:city": "Miami" })),
        (
            &HIST,
            json!({ "@id": "?s", "@type": "ex:Customer", "ex:city": "Miami" }),
        ),
    ] {
        let mut answers = Vec::new();
        for from in [source.from(), source.native.to_string()] {
            let query = json!({
                "@context": { "ex": EX },
                "from": from,
                "select": { "?s": ["*"] },
                "where": where_clause.clone(),
                "orderBy": "?s",
            });
            let result = fluree
                .query_from()
                .jsonld(&query)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("crawl on {from}: {e}"));
            answers.push(normalize_docs(&result));
        }
        if answers[0] != answers[1] || answers[1].is_empty() {
            wrong.push(format!(
                "{} {where_clause}: graph source {:#?}\nnative {:#?}",
                source.alias, answers[0], answers[1]
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Crawl documents with every multi-valued property's values sorted (a crawl
/// lists a property's values in no promised order) and duplicates kept.
fn normalize_docs(result: &Value) -> Vec<BTreeMap<String, Vec<String>>> {
    result
        .as_array()
        .unwrap_or_else(|| panic!("a crawl answers an array: {result}"))
        .iter()
        .map(|doc| {
            doc.as_object()
                .expect("a crawl document is an object")
                .iter()
                .map(|(k, v)| {
                    let mut vals: Vec<String> = match v {
                        Value::Array(items) => items.iter().map(text).collect(),
                        other => vec![text(other)],
                    };
                    vals.sort();
                    (k.clone(), vals)
                })
                .collect()
        })
        .collect()
}

/// `/info` over the local fixtures: a count is exact where the table's
/// metadata proves it and `null` otherwise, with `count-upper-bound` where a
/// bound is provable. A Direct source reads each table's metadata file for
/// its row count (snapshot summary) and schema.
#[tokio::test]
async fn info_counts_are_exact_or_null_with_a_provable_bound() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = format!(
        "file://{}/tests/fixtures/iceberg/sets",
        env!("CARGO_MANIFEST_DIR")
    );
    let mapping = format!(
        r#"{PREFIXES}
        <http://example.org/mapping#Current> a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "sets.customers_current" ] ;
            rr:subjectMap [ rr:template "http://example.org/customer/{{customer_id}}" ;
                            rr:class ex:CurrentCustomer ] ;
            rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
        <http://example.org/mapping#History> a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "sets.customers_history" ] ;
            rr:subjectMap [ rr:template "http://example.org/history/{{customer_id}}" ;
                            rr:class ex:HistoryCustomer ] .
        <http://example.org/mapping#Active> a rr:TriplesMap ;
            rr:logicalTable [ rr:sqlQuery "SELECT * FROM sets.customers_history WHERE is_current = 1" ;
                              f:key ( "customer_id" ) ] ;
            rr:subjectMap [ rr:template "http://example.org/active/{{customer_id}}" ;
                            rr:class ex:Active ] .
        <http://example.org/mapping#Order> a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "sets.orders" ] ;
            rr:subjectMap [ rr:template "http://example.org/order/{{order_id}}" ; rr:class ex:Order ] ;
            rr:predicateObjectMap [ rr:predicate ex:customer ; rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Current> ;
                rr:joinCondition [ rr:child "customer_id" ; rr:parent "customer_id" ] ] ] .
        <http://example.org/mapping#Parent> a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "sets.parents_dup" ] ;
            rr:subjectMap [ rr:template "http://example.org/parent/{{id}}" ; rr:class ex:Parent ] ;
            rr:predicateObjectMap [ rr:predicate ex:sameCode ; rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Parent> ;
                rr:joinCondition [ rr:child "code" ; rr:parent "code" ] ] ] .
        "#
    );
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("sets-info", &root, mapping)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create graph source");
    let info = fluree
        .ledger_info("sets-info:main")
        .execute()
        .await
        .expect("graph source info");
    let counts = |entry: &Value| {
        (
            entry["count"].as_i64(),
            entry.get("count-upper-bound").and_then(Value::as_i64),
        )
    };
    let class = |name: &str| counts(&info["stats"]["classes"][ex(name)]);
    let property = |name: &str| counts(&info["stats"]["properties"][ex(name)]);

    assert_eq!(
        info["source"]["table-row-counts"],
        json!({
            "sets.customers_current": 6,
            "sets.customers_history": 11,
            "sets.orders": 8,
            "sets.parents_dup": 8,
        }),
        "{info}"
    );
    // Keyed by its identifier field, the key column required: exact.
    assert_eq!(class("CurrentCustomer"), (Some(6), None), "{info}");
    // Its `name` column is nullable: a row may mint no name.
    assert_eq!(property("name"), (None, Some(6)), "{info}");
    // No key: a customer has several versions.
    assert_eq!(class("HistoryCustomer"), (None, Some(11)), "{info}");
    // A key holds for the filtered rows, but no metadata counts them.
    assert_eq!(class("Active"), (None, Some(11)), "{info}");
    // Keyless and nullable: bounded by the rows.
    assert_eq!(class("Order"), (None, Some(8)), "{info}");
    // A customer id matches at most one current customer: one reference per
    // order row at most.
    assert_eq!(property("customer"), (None, Some(8)), "{info}");
    let customer_ref = &info["stats"]["classes"][ex("Order")]["properties"][ex("customer")];
    assert_eq!(
        customer_ref["ref-classes"][ex("CurrentCustomer")],
        Value::Null,
        "{customer_ref}"
    );
    assert_eq!(
        customer_ref["ref-classes-upper-bound"][ex("CurrentCustomer")],
        8,
        "{customer_ref}"
    );
    // A code matches several parents (fan-out): no bound.
    assert_eq!(property("sameCode"), (None, None), "{info}");
    let same_code = &info["stats"]["classes"][ex("Parent")]["properties"][ex("sameCode")];
    assert!(
        same_code.get("ref-classes-upper-bound").is_none(),
        "{same_code}"
    );
}
