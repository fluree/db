//! The SQL lane answers over the set of triples a mapping defines.
//!
//! An R2RML graph source is the RDF dataset R2RML §11 defines: a SET of
//! triples. Its tables are bags, and the `sets.*` tables here repeat triples
//! on purpose: `customers_history` holds several versions of a customer (some
//! identical, some differing in one column), `customers_current` mints the
//! same subjects and class from another table, and `orders` references the
//! history table on a key that matches several of its rows, with one order
//! an exact duplicate row. The same rows sit in the committed local Iceberg
//! fixture `sets.*`, so every lane reads the same data.
//!
//! The oracle is a native ledger holding the triples the mapping defines,
//! written out by hand: two lanes agreeing is no evidence of either being
//! right. Each case runs on SPARQL and on its JSON-LD twin and pins the SQL
//! lane's routing stamp: `MustFire` where the lane answers (with the
//! statement it sends), `MustNotFire` where it must decline to the per-scan
//! lane. A source whose keys are proven (a declared `f:key`, or a probed
//! table on a source registered without `allow_duplicate_subjects`) keeps the
//! statement the lane sent before set semantics, byte for byte.
//!
//! Own binary: it flips the process-wide fast-path switch and sets
//! `FLUREE_ICEBERG_LOCAL_ROOTS`.

#![cfg(all(feature = "sql", feature = "native"))]

#[path = "support/fake_sql.rs"]
mod fake_sql;
#[path = "support/span_capture.rs"]
mod span_capture;

use fake_sql::{FakeSql, Table};
use fluree_db_api::{
    set_fast_paths_disabled, Fluree, FlureeBuilder, R2rmlCreateConfig, SqlCreateConfig, SqlDialect,
};
use fluree_db_nameservice::GraphSourceType;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use wiremock::MockServer;

/// Every test flips the process-wide fast-path switch.
static KILL_SWITCH: Mutex<()> = Mutex::const_new(());

const SQL_BLOCK: &str = "sql_block_pushdown";
const SQL_AGG: &str = "sql_aggregate_pushdown";

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n";

/// Customer versions, the orders that reference them, and a reference from
/// a version to every version sharing its city: a join on a column that is
/// not the parent's subject.
const HIST_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#History>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_history" ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{customer_id}" ; rr:class ex:Customer ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:city ; rr:objectMap [ rr:column "city" ] ] .

    <http://example.org/mapping#Neighbor>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_history" ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{customer_id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:neighbor ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#History> ;
                rr:joinCondition [ rr:child "city" ; rr:parent "city" ]
            ]
        ] .

    # A version subject whose template is not injective: `-` separates the
    # two values, and the IRI escape writes `-` inside a value too.
    <http://example.org/mapping#Version>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_history" ] ;
        rr:subjectMap [ rr:template "http://example.org/version/{customer_id}-{name}" ] ;
        rr:predicateObjectMap [ rr:predicate ex:at ; rr:objectMap [ rr:column "city" ] ] .

    <http://example.org/mapping#Order>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.orders" ] ;
        rr:subjectMap [ rr:template "http://example.org/order/{order_id}" ; rr:class ex:Order ] ;
        rr:predicateObjectMap [
            rr:predicate ex:amount ;
            rr:objectMap [ rr:column "amount" ; rr:datatype xsd:decimal ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:customer ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#History> ;
                rr:joinCondition [ rr:child "customer_id" ; rr:parent "customer_id" ]
            ]
        ] .
"#;

/// The history and current tables minting the same subjects, class and
/// `ex:name`.
const BOTH_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#History>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_history" ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{customer_id}" ; rr:class ex:Customer ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:city ; rr:objectMap [ rr:column "city" ] ] .

    <http://example.org/mapping#Current>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_current" ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{customer_id}" ; rr:class ex:Customer ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
"#;

/// The current table alone; `{KEY}` is replaced by an `f:key` declaration
/// or by nothing.
const CURRENT_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix f: <https://ns.flur.ee/db#> .

    <http://example.org/mapping#Current>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.customers_current" {KEY} ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{customer_id}" ; rr:class ex:Customer ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
"#;

fn current_mapping(keyed: bool) -> String {
    CURRENT_R2RML.replace(
        "{KEY}",
        if keyed {
            "; f:key ( \"customer_id\" )"
        } else {
            ""
        },
    )
}

/// The rows of the Iceberg fixture `sets.*`
/// (`scripts/local/write_local_iceberg_table.py`), file boundaries aside.
fn sets_tables() -> FakeSql {
    let history = [
        (Some(1), "alice", Some("Boston"), "2024-01-01", 0),
        (Some(2), "bob", Some("Austin"), "2024-01-01", 0),
        (Some(3), "carol", Some("Chicago"), "2024-02-01", 1),
        (Some(1), "alice", Some("Denver"), "2024-06-01", 0),
        (Some(5), "erin", Some("Miami"), "2024-01-01", 0),
        (Some(2), "robert", Some("Austin"), "2024-05-01", 0),
        (Some(2), "robert", Some("Austin"), "2024-09-01", 1),
        (Some(4), "dave", None, "2024-03-01", 1),
        (Some(1), "alice", Some("Denver"), "2024-11-01", 1),
        (Some(5), "erin-m", Some("Tampa"), "2024-07-01", 1),
        (None, "ghost", Some("Nowhere"), "2024-01-01", 0),
    ];
    let current = [
        (1, "alice"),
        (2, "robert"),
        (3, "carol"),
        (4, "dave"),
        (5, "erin-m"),
        (6, "frank"),
    ];
    let orders = [
        (100, Some(1), "10.00"),
        (101, Some(2), "20.00"),
        (102, Some(3), "5.50"),
        (103, Some(5), "7.25"),
        (101, Some(2), "20.00"),
        (104, Some(99), "1.00"),
        (105, None, "3.00"),
        (106, Some(1), "2.00"),
    ];
    FakeSql::new()
        .table(Table::new(
            "sets.customers_history",
            &[
                ("customer_id", "bigint"),
                ("name", "varchar"),
                ("city", "varchar"),
                ("valid_from", "date"),
                ("is_current", "integer"),
            ],
            history
                .iter()
                .map(|(id, name, city, from, current)| {
                    vec![
                        json!(id),
                        json!(name),
                        json!(city),
                        json!(from),
                        json!(current),
                    ]
                })
                .collect(),
        ))
        .table(Table::new(
            "sets.customers_current",
            &[("customer_id", "bigint"), ("name", "varchar")],
            current
                .iter()
                .map(|(id, name)| vec![json!(id), json!(name)])
                .collect(),
        ))
        .table(Table::new(
            "sets.orders",
            &[
                ("order_id", "bigint"),
                ("customer_id", "bigint"),
                ("amount", "decimal(10,2)"),
            ],
            orders
                .iter()
                .map(|(id, customer, amount)| vec![json!(id), json!(customer), json!(amount)])
                .collect(),
        ))
}

/// The triples `HIST_R2RML` (`with_current: false`) or `BOTH_R2RML`
/// (`true`) define over those rows, written out by hand.
async fn seed_native(fluree: &Fluree, ledger_id: &str, with_current: bool) {
    let ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("native ledger");
    let ex = "http://example.org/";
    let customer = |id: u32, names: &[&str], cities: &[&str]| {
        let mut c = json!({
            "@id": format!("{ex}customer/{id}"),
            "@type": format!("{ex}Customer"),
            format!("{ex}name"): names,
        });
        if !cities.is_empty() {
            c[format!("{ex}city")] = json!(cities);
        }
        c
    };
    let mut graph = vec![
        customer(1, &["alice"], &["Boston", "Denver"]),
        customer(2, &["bob", "robert"], &["Austin"]),
        customer(3, &["carol"], &["Chicago"]),
        customer(4, &["dave"], &[]),
        customer(5, &["erin", "erin-m"], &["Miami", "Tampa"]),
    ];
    if with_current {
        graph.push(customer(6, &["frank"], &[]));
    } else {
        for (version, city) in [
            ("1-alice", "Boston"),
            ("1-alice", "Denver"),
            ("2-bob", "Austin"),
            ("2-robert", "Austin"),
            ("3-carol", "Chicago"),
            ("5-erin", "Miami"),
            ("5-erin-m", "Tampa"),
        ] {
            graph.push(json!({
                "@id": format!("{ex}version/{version}"),
                format!("{ex}at"): city,
            }));
        }
        // Every version shares its city only with versions of its own
        // customer.
        for id in [1, 2, 3, 5] {
            graph.push(json!({
                "@id": format!("{ex}customer/{id}"),
                format!("{ex}neighbor"): {"@id": format!("{ex}customer/{id}")},
            }));
        }
        let order = |id: u32, customer: Option<u32>, amount: &str| {
            let mut o = json!({
                "@id": format!("{ex}order/{id}"),
                "@type": format!("{ex}Order"),
                format!("{ex}amount"): {
                    "@value": amount,
                    "@type": "http://www.w3.org/2001/XMLSchema#decimal"
                },
            });
            if let Some(c) = customer {
                o[format!("{ex}customer")] = json!({"@id": format!("{ex}customer/{c}")});
            }
            o
        };
        graph.extend([
            order(100, Some(1), "10.00"),
            order(101, Some(2), "20.00"),
            order(102, Some(3), "5.50"),
            order(103, Some(5), "7.25"),
            // The referenced key matches no history row: no reference.
            order(104, None, "1.00"),
            order(105, None, "3.00"),
            order(106, Some(1), "2.00"),
        ]);
    }
    fluree
        .insert(ledger, &json!({"@graph": graph}))
        .await
        .expect("seed native ledger");
}

/// A SQL source over the fake endpoint, with or without the duplicate-subject
/// probe.
async fn sql_source(fluree: &Fluree, server: &MockServer, name: &str, mapping: &str, probe: bool) {
    let mut config = SqlCreateConfig::new(name, server.uri(), mapping);
    config.allow_duplicate_subjects = !probe;
    fluree
        .create_sql_graph_source(config)
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// `hist-*` and `both-*` as SQL sources (registered with
/// `allow_duplicate_subjects`: the probe would refuse the history table),
/// Iceberg sources over the local fixture, and native ledgers.
async fn setup() -> (MockServer, Fluree) {
    let fixtures = format!("{}/tests/fixtures/iceberg", env!("CARGO_MANIFEST_DIR"));
    // Set before any storage or scan is built; nothing else in this binary
    // reads the variable.
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", &fixtures);
    let server = sets_tables().mount().await;
    let fluree = FlureeBuilder::memory().build_memory();
    sql_source(&fluree, &server, "hist-sql", HIST_R2RML, false).await;
    sql_source(&fluree, &server, "both-sql", BOTH_R2RML, false).await;
    let root = format!("file://{fixtures}/sets");
    for (name, mapping) in [("hist-ice", HIST_R2RML), ("both-ice", BOTH_R2RML)] {
        fluree
            .create_r2rml_graph_source(
                R2rmlCreateConfig::new_direct(name, &root, mapping)
                    .with_mapping_media_type("text/turtle"),
            )
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    seed_native(&fluree, "hist-native:main", false).await;
    seed_native(&fluree, "both-native:main", true).await;
    (server, fluree)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Routing {
    MustFire,
    MustNotFire,
}

struct Case {
    name: &'static str,
    /// `hist` or `both`: the mapping, and the native ledger answering it.
    src: &'static str,
    /// The query; `<SRC>` stands for the source each lane reads.
    sparql: &'static str,
    /// The JSON-LD twin, without `@context` and `from`.
    jsonld: Value,
    /// The JSON-LD select's variables, in select order.
    vars: &'static [&'static str],
    /// The SQL lane's site for this shape and whether it must serve it.
    site: &'static str,
    routing: Routing,
    /// The statements the SQL lane sends when it serves the shape.
    sql: &'static [&'static str],
    /// The answer, written out: the native ledger must give it too.
    expect: &'static [&'static str],
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "a pattern over a keyless map answers each triple once",
            src: "hist",
            sparql: "SELECT ?s ?n FROM <SRC> WHERE { ?s ex:name ?n }",
            jsonld: json!({"select": ["?s", "?n"], "where": [{"@id": "?s", "ex:name": "?n"}]}),
            vars: &["?s", "?n"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."name" AS "c1" FROM "sets"."customers_history" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
            expect: &[
                "n=alice s=http://example.org/customer/1",
                "n=bob s=http://example.org/customer/2",
                "n=carol s=http://example.org/customer/3",
                "n=dave s=http://example.org/customer/4",
                "n=erin s=http://example.org/customer/5",
                "n=erin-m s=http://example.org/customer/5",
                "n=robert s=http://example.org/customer/2",
            ],
        },
        Case {
            name: "a projection keeps the duplicates the set's solutions project to",
            src: "hist",
            sparql: "SELECT ?s FROM <SRC> WHERE { ?s ex:city ?c }",
            jsonld: json!({"select": ["?s"], "where": [{"@id": "?s", "ex:city": "?c"}]}),
            vars: &["?s"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."city" AS "c1" FROM "sets"."customers_history" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."city" IS NOT NULL"#],
            expect: &[
                "s=http://example.org/customer/1",
                "s=http://example.org/customer/1",
                "s=http://example.org/customer/2",
                "s=http://example.org/customer/3",
                "s=http://example.org/customer/5",
                "s=http://example.org/customer/5",
            ],
        },
        Case {
            name: "a constant subject over a keyless map answers its triples once",
            src: "hist",
            sparql: "SELECT ?n FROM <SRC> WHERE { <http://example.org/customer/1> ex:name ?n }",
            jsonld: json!({"select": ["?n"], "where": [{"@id": "ex:customer/1", "ex:name": "?n"}]}),
            vars: &["?n"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "t0"."name" AS "c0" FROM "sets"."customers_history" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."customer_id" = 1 AND "t0"."name" IS NOT NULL"#],
            expect: &["n=alice"],
        },
        Case {
            name: "a reference into a keyless parent is one edge per child and subject",
            src: "hist",
            sparql: "SELECT ?o ?c FROM <SRC> WHERE { ?o ex:customer ?c }",
            jsonld: json!({"select": ["?o", "?c"], "where": [{"@id": "?o", "ex:customer": "?c"}]}),
            vars: &["?o", "?c"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "t0"."order_id" AS "c0", "t1"."customer_id" AS "c1" FROM "sets"."orders" AS "t0" JOIN "sets"."customers_history" AS "t1" ON "t0"."customer_id" = "t1"."customer_id" WHERE "t0"."order_id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."customer_id" IS NOT NULL"#],
            expect: &[
                "c=http://example.org/customer/1 o=http://example.org/order/100",
                "c=http://example.org/customer/1 o=http://example.org/order/106",
                "c=http://example.org/customer/2 o=http://example.org/order/101",
                "c=http://example.org/customer/3 o=http://example.org/order/102",
                "c=http://example.org/customer/5 o=http://example.org/order/103",
            ],
        },
        Case {
            name: "a reference into a keyless parent entity joins each of its triples once",
            src: "hist",
            sparql: "SELECT ?o ?n FROM <SRC> WHERE { ?o ex:customer ?c . ?c ex:name ?n }",
            jsonld: json!({"select": ["?o", "?n"], "where": [
                {"@id": "?o", "ex:customer": "?c"},
                {"@id": "?c", "ex:name": "?n"}
            ]}),
            vars: &["?o", "?n"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "t0"."order_id" AS "c0", "t1"."customer_id" AS "c1", "t1"."name" AS "c2" FROM "sets"."orders" AS "t0" JOIN "sets"."customers_history" AS "t1" ON "t0"."customer_id" = "t1"."customer_id" WHERE "t0"."order_id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."customer_id" IS NOT NULL AND "t1"."name" IS NOT NULL"#],
            expect: &[
                "n=alice o=http://example.org/order/100",
                "n=alice o=http://example.org/order/106",
                "n=bob o=http://example.org/order/101",
                "n=carol o=http://example.org/order/102",
                "n=erin o=http://example.org/order/103",
                "n=erin-m o=http://example.org/order/103",
                "n=robert o=http://example.org/order/101",
            ],
        },
        Case {
            name: "a reference on a keyless parent's non-subject column is one edge per pair",
            src: "hist",
            sparql: "SELECT ?a ?b FROM <SRC> WHERE { ?a ex:neighbor ?b }",
            jsonld: json!({"select": ["?a", "?b"], "where": [{"@id": "?a", "ex:neighbor": "?b"}]}),
            vars: &["?a", "?b"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "t0"."customer_id" AS "c0", "t1"."customer_id" AS "c1" FROM "sets"."customers_history" AS "t0" JOIN "sets"."customers_history" AS "t1" ON "t0"."city" = "t1"."city" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."city" IS NOT NULL AND "t1"."customer_id" IS NOT NULL"#],
            expect: &[
                "a=http://example.org/customer/1 b=http://example.org/customer/1",
                "a=http://example.org/customer/2 b=http://example.org/customer/2",
                "a=http://example.org/customer/3 b=http://example.org/customer/3",
                "a=http://example.org/customer/5 b=http://example.org/customer/5",
            ],
        },
        Case {
            name: "a reference on a keyless parent's non-subject column reads every row of it",
            src: "hist",
            sparql: "SELECT ?a ?n FROM <SRC> WHERE { ?a ex:neighbor ?b . ?b ex:name ?n }",
            jsonld: json!({"select": ["?a", "?n"], "where": [
                {"@id": "?a", "ex:neighbor": "?b"},
                {"@id": "?b", "ex:name": "?n"}
            ]}),
            vars: &["?a", "?n"],
            site: SQL_BLOCK,
            routing: Routing::MustNotFire,
            sql: &[],
            expect: &[
                "a=http://example.org/customer/1 n=alice",
                "a=http://example.org/customer/2 n=bob",
                "a=http://example.org/customer/2 n=robert",
                "a=http://example.org/customer/3 n=carol",
                "a=http://example.org/customer/5 n=erin",
                "a=http://example.org/customer/5 n=erin-m",
            ],
        },
        Case {
            name: "a subject template that is not injective declines",
            src: "hist",
            sparql: "SELECT ?v ?c FROM <SRC> WHERE { ?v ex:at ?c }",
            jsonld: json!({"select": ["?v", "?c"], "where": [{"@id": "?v", "ex:at": "?c"}]}),
            vars: &["?v", "?c"],
            site: SQL_BLOCK,
            routing: Routing::MustNotFire,
            sql: &[],
            expect: &[
                "c=Austin v=http://example.org/version/2-bob",
                "c=Austin v=http://example.org/version/2-robert",
                "c=Boston v=http://example.org/version/1-alice",
                "c=Chicago v=http://example.org/version/3-carol",
                "c=Denver v=http://example.org/version/1-alice",
                "c=Miami v=http://example.org/version/5-erin",
                "c=Tampa v=http://example.org/version/5-erin-m",
            ],
        },
        Case {
            name: "distinct VALUES rows over a keyless map join each solution once",
            src: "hist",
            sparql: "SELECT ?s ?n FROM <SRC> WHERE { VALUES ?s { <http://example.org/customer/1> <http://example.org/customer/2> } ?s ex:name ?n }",
            jsonld: json!({"select": ["?s", "?n"], "where": [
                ["values", ["?s", [{"@id": "ex:customer/1"}, {"@id": "ex:customer/2"}]]],
                {"@id": "?s", "ex:name": "?n"}
            ]}),
            vars: &["?s", "?n"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."name" AS "c1" FROM "sets"."customers_history" AS "t0" JOIN (VALUES (1), (2)) AS "v0" ("k0") ON "v0"."k0" = "t0"."customer_id" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
            expect: &[
                "n=alice s=http://example.org/customer/1",
                "n=bob s=http://example.org/customer/2",
                "n=robert s=http://example.org/customer/2",
            ],
        },
        Case {
            name: "a VALUES row given twice joins each solution twice",
            src: "hist",
            sparql: "SELECT ?s ?n FROM <SRC> WHERE { VALUES ?s { <http://example.org/customer/1> <http://example.org/customer/1> } ?s ex:name ?n }",
            jsonld: json!({"select": ["?s", "?n"], "where": [
                ["values", ["?s", [{"@id": "ex:customer/1"}, {"@id": "ex:customer/1"}]]],
                {"@id": "?s", "ex:name": "?n"}
            ]}),
            vars: &["?s", "?n"],
            site: SQL_BLOCK,
            routing: Routing::MustNotFire,
            sql: &[],
            expect: &[
                "n=alice s=http://example.org/customer/1",
                "n=alice s=http://example.org/customer/1",
            ],
        },
        Case {
            name: "a DISTINCT above keeps its one DISTINCT",
            src: "hist",
            sparql: "SELECT DISTINCT ?n FROM <SRC> WHERE { ?s ex:name ?n }",
            jsonld: json!({"selectDistinct": ["?n"], "where": [{"@id": "?s", "ex:name": "?n"}]}),
            vars: &["?n"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "t0"."name" AS "c0" FROM "sets"."customers_history" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
            expect: &[
                "n=alice", "n=bob", "n=carol", "n=dave", "n=erin", "n=erin-m", "n=robert",
            ],
        },
        Case {
            name: "a top-k over a keyless map orders the set",
            src: "hist",
            sparql: "SELECT ?s ?n FROM <SRC> WHERE { ?s ex:name ?n } ORDER BY ?n LIMIT 3",
            jsonld: json!({"select": ["?s", "?n"], "where": [{"@id": "?s", "ex:name": "?n"}],
                "orderBy": ["?n"], "limit": 3}),
            vars: &["?s", "?n"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."name" AS "c1" FROM "sets"."customers_history" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL ORDER BY "t0"."name" ASC LIMIT 3"#],
            expect: &[
                "n=alice s=http://example.org/customer/1",
                "n=bob s=http://example.org/customer/2",
                "n=carol s=http://example.org/customer/3",
            ],
        },
        Case {
            name: "UNION branches are each a set and together a bag",
            src: "hist",
            sparql: "SELECT ?s ?v FROM <SRC> WHERE { { ?s ex:name ?v } UNION { ?s ex:city ?v } }",
            jsonld: json!({"select": ["?s", "?v"], "where": [
                ["union", {"@id": "?s", "ex:name": "?v"}, {"@id": "?s", "ex:city": "?v"}]
            ]}),
            vars: &["?s", "?v"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT "u"."c0" AS "c0", "u"."c1" AS "c1", "u"."c2" AS "c2" FROM (SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."name" AS "c1", 0 AS "c2" FROM "sets"."customers_history" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL UNION ALL SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."city" AS "c1", 1 AS "c2" FROM "sets"."customers_history" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."city" IS NOT NULL) AS "u""#],
            expect: &[
                "s=http://example.org/customer/1 v=Boston",
                "s=http://example.org/customer/1 v=Denver",
                "s=http://example.org/customer/1 v=alice",
                "s=http://example.org/customer/2 v=Austin",
                "s=http://example.org/customer/2 v=bob",
                "s=http://example.org/customer/2 v=robert",
                "s=http://example.org/customer/3 v=Chicago",
                "s=http://example.org/customer/3 v=carol",
                "s=http://example.org/customer/4 v=dave",
                "s=http://example.org/customer/5 v=Miami",
                "s=http://example.org/customer/5 v=Tampa",
                "s=http://example.org/customer/5 v=erin",
                "s=http://example.org/customer/5 v=erin-m",
            ],
        },
        Case {
            name: "a star over a keyless subject is the per-subject cross product",
            src: "hist",
            sparql: "SELECT ?s ?n ?c FROM <SRC> WHERE { ?s ex:name ?n ; ex:city ?c }",
            jsonld: json!({"select": ["?s", "?n", "?c"],
                "where": [{"@id": "?s", "ex:name": "?n", "ex:city": "?c"}]}),
            vars: &["?s", "?n", "?c"],
            site: SQL_BLOCK,
            routing: Routing::MustNotFire,
            sql: &[],
            expect: &[
                "c=Austin n=bob s=http://example.org/customer/2",
                "c=Austin n=robert s=http://example.org/customer/2",
                "c=Boston n=alice s=http://example.org/customer/1",
                "c=Chicago n=carol s=http://example.org/customer/3",
                "c=Denver n=alice s=http://example.org/customer/1",
                "c=Miami n=erin s=http://example.org/customer/5",
                "c=Miami n=erin-m s=http://example.org/customer/5",
                "c=Tampa n=erin s=http://example.org/customer/5",
                "c=Tampa n=erin-m s=http://example.org/customer/5",
            ],
        },
        Case {
            name: "an optional member of a keyless subject joins every one of its values",
            src: "hist",
            sparql: "SELECT ?s ?n ?c FROM <SRC> WHERE { ?s ex:name ?n OPTIONAL { ?s ex:city ?c } }",
            jsonld: json!({"select": ["?s", "?n", "?c"], "where": [
                {"@id": "?s", "ex:name": "?n"},
                ["optional", {"@id": "?s", "ex:city": "?c"}]
            ]}),
            vars: &["?s", "?n", "?c"],
            site: SQL_BLOCK,
            routing: Routing::MustNotFire,
            sql: &[],
            expect: &[
                "c= n=dave s=http://example.org/customer/4",
                "c=Austin n=bob s=http://example.org/customer/2",
                "c=Austin n=robert s=http://example.org/customer/2",
                "c=Boston n=alice s=http://example.org/customer/1",
                "c=Chicago n=carol s=http://example.org/customer/3",
                "c=Denver n=alice s=http://example.org/customer/1",
                "c=Miami n=erin s=http://example.org/customer/5",
                "c=Miami n=erin-m s=http://example.org/customer/5",
                "c=Tampa n=erin s=http://example.org/customer/5",
                "c=Tampa n=erin-m s=http://example.org/customer/5",
            ],
        },
        Case {
            name: "a class two maps mint for one subject counts the subject once",
            src: "both",
            sparql: "SELECT ?s FROM <SRC> WHERE { ?s a ex:Customer }",
            jsonld: json!({"select": ["?s"], "where": [{"@id": "?s", "@type": "ex:Customer"}]}),
            vars: &["?s"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "u0"."c1" AS "c0", "u0"."c0" AS "c1" FROM (SELECT "t0"."customer_id" AS "c0", 0 AS "c1" FROM "sets"."customers_current" AS "t0" WHERE "t0"."customer_id" IS NOT NULL UNION ALL SELECT "t1"."customer_id" AS "c0", 0 AS "c1" FROM "sets"."customers_history" AS "t1" WHERE "t1"."customer_id" IS NOT NULL) AS "u0""#],
            expect: &[
                "s=http://example.org/customer/1",
                "s=http://example.org/customer/2",
                "s=http://example.org/customer/3",
                "s=http://example.org/customer/4",
                "s=http://example.org/customer/5",
                "s=http://example.org/customer/6",
            ],
        },
        Case {
            name: "a predicate two maps mint answers each triple once",
            src: "both",
            sparql: "SELECT ?s ?n FROM <SRC> WHERE { ?s ex:name ?n }",
            jsonld: json!({"select": ["?s", "?n"], "where": [{"@id": "?s", "ex:name": "?n"}]}),
            vars: &["?s", "?n"],
            site: SQL_BLOCK,
            routing: Routing::MustFire,
            sql: &[r#"SELECT DISTINCT "u0"."c2" AS "c0", "u0"."c0" AS "c1", "u0"."c1" AS "c2" FROM (SELECT "t0"."customer_id" AS "c0", "t0"."name" AS "c1", 0 AS "c2" FROM "sets"."customers_current" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL UNION ALL SELECT "t1"."customer_id" AS "c0", "t1"."name" AS "c1", 0 AS "c2" FROM "sets"."customers_history" AS "t1" WHERE "t1"."customer_id" IS NOT NULL AND "t1"."name" IS NOT NULL) AS "u0""#],
            expect: &[
                "n=alice s=http://example.org/customer/1",
                "n=bob s=http://example.org/customer/2",
                "n=carol s=http://example.org/customer/3",
                "n=dave s=http://example.org/customer/4",
                "n=erin s=http://example.org/customer/5",
                "n=erin-m s=http://example.org/customer/5",
                "n=frank s=http://example.org/customer/6",
                "n=robert s=http://example.org/customer/2",
            ],
        },
        Case {
            name: "COUNT(*) over a keyless map counts the set's solutions",
            src: "hist",
            sparql: "SELECT (COUNT(*) AS ?k) FROM <SRC> WHERE { ?s ex:name ?n }",
            jsonld: json!({"select": ["(as (count *) ?k)"], "where": [{"@id": "?s", "ex:name": "?n"}]}),
            vars: &["?k"],
            site: SQL_AGG,
            routing: Routing::MustFire,
            sql: &[r#"SELECT COUNT(*) AS "c0" FROM (SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."name" AS "c1" FROM "sets"."customers_history" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL) AS "s""#],
            expect: &["k=7"],
        },
        Case {
            name: "SUM over a duplicated row adds its triple once",
            src: "hist",
            sparql: "SELECT (SUM(?a) AS ?t) FROM <SRC> WHERE { ?o ex:amount ?a }",
            jsonld: json!({"select": ["(as (sum ?a) ?t)"], "where": [{"@id": "?o", "ex:amount": "?a"}]}),
            vars: &["?t"],
            site: SQL_AGG,
            routing: Routing::MustFire,
            sql: &[r#"SELECT SUM("s"."c1") AS "c0", COUNT("s"."c1") AS "c1" FROM (SELECT DISTINCT "t0"."order_id" AS "c0", "t0"."amount" AS "c1" FROM "sets"."orders" AS "t0" WHERE "t0"."order_id" IS NOT NULL AND "t0"."amount" IS NOT NULL) AS "s""#],
            expect: &["t=48.75"],
        },
        Case {
            name: "GROUP BY over a keyless parent counts each child once",
            src: "hist",
            sparql: "SELECT ?c (COUNT(?o) AS ?k) FROM <SRC> WHERE { ?o ex:customer ?c } GROUP BY ?c",
            jsonld: json!({"select": ["?c", "(as (count ?o) ?k)"],
                "where": [{"@id": "?o", "ex:customer": "?c"}], "groupBy": ["?c"]}),
            vars: &["?c", "?k"],
            site: SQL_AGG,
            routing: Routing::MustFire,
            sql: &[r#"SELECT "s"."c1" AS "c0", COUNT("s"."c0") AS "c1" FROM (SELECT DISTINCT "t0"."order_id" AS "c0", "t1"."customer_id" AS "c1" FROM "sets"."orders" AS "t0" JOIN "sets"."customers_history" AS "t1" ON "t0"."customer_id" = "t1"."customer_id" WHERE "t0"."order_id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."customer_id" IS NOT NULL) AS "s" GROUP BY "s"."c1""#],
            expect: &[
                "c=http://example.org/customer/1 k=2",
                "c=http://example.org/customer/2 k=1",
                "c=http://example.org/customer/3 k=1",
                "c=http://example.org/customer/5 k=1",
            ],
        },
        Case {
            name: "COUNT over a keyless star declines with its block",
            src: "hist",
            sparql: "SELECT (COUNT(*) AS ?k) FROM <SRC> WHERE { ?s ex:name ?n ; ex:city ?c }",
            jsonld: json!({"select": ["(as (count *) ?k)"],
                "where": [{"@id": "?s", "ex:name": "?n", "ex:city": "?c"}]}),
            vars: &["?k"],
            site: SQL_AGG,
            routing: Routing::MustNotFire,
            sql: &[],
            expect: &["k=9"],
        },
    ]
}

/// The cases the per-scan lane cannot run (a graph source has no native index
/// for the engine's subquery operator): the SQL lane and the native ledger
/// answer them.
fn subquery_cases() -> Vec<Case> {
    vec![Case {
        name: "a grouped sub-select over a keyless map counts the set's solutions",
        src: "hist",
        sparql: "SELECT ?c ?k FROM <SRC> WHERE { ?c ex:name ?n { SELECT ?c (COUNT(?o) AS ?k) WHERE { ?o ex:customer ?c } GROUP BY ?c } }",
        jsonld: json!({"select": ["?c", "?k"], "where": [
            {"@id": "?c", "ex:name": "?n"},
            ["query", {"@context": {"ex": "http://example.org/"},
                "select": ["?c", "(as (count ?o) ?k)"],
                "where": [{"@id": "?o", "ex:customer": "?c"}], "groupBy": ["?c"]}]
        ]}),
        vars: &["?c", "?k"],
        site: SQL_BLOCK,
        routing: Routing::MustFire,
        sql: &[r#"SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."name" AS "c1", "d0"."c1" AS "c2" FROM "sets"."customers_history" AS "t0" JOIN (SELECT "s"."c1" AS "c0", COUNT("s"."c0") AS "c1" FROM (SELECT DISTINCT "t3"."order_id" AS "c0", "t4"."customer_id" AS "c1" FROM "sets"."orders" AS "t3" JOIN "sets"."customers_history" AS "t4" ON "t3"."customer_id" = "t4"."customer_id" WHERE "t3"."order_id" IS NOT NULL AND "t3"."customer_id" IS NOT NULL AND "t4"."customer_id" IS NOT NULL) AS "s" GROUP BY "s"."c1") AS "d0" ON "t0"."customer_id" = "d0"."c0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
        expect: &[
            "c=http://example.org/customer/1 k=2",
            "c=http://example.org/customer/2 k=1",
            "c=http://example.org/customer/2 k=1",
            "c=http://example.org/customer/3 k=1",
            "c=http://example.org/customer/5 k=1",
            "c=http://example.org/customer/5 k=1",
        ],
    }]
}

/// SPARQL JSON results as sorted rows of `var=value` pairs.
fn sparql_rows(v: &Value) -> Vec<String> {
    let mut vars: Vec<String> = v["head"]["vars"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    vars.sort();
    let mut out: Vec<String> = v["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("not SPARQL JSON: {v}"))
        .iter()
        .map(|b| {
            vars.iter()
                .map(|var| {
                    let val = b[var]["value"].as_str().unwrap_or("").to_string();
                    format!("{var}={}", canon(&val))
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    out.sort();
    out
}

/// JSON-LD select rows rendered like [`sparql_rows`].
fn jsonld_rows(v: &Value, select: &[&str]) -> Vec<String> {
    let mut order: Vec<(usize, String)> = select
        .iter()
        .enumerate()
        .map(|(i, s)| (i, s.trim_start_matches('?').to_string()))
        .collect();
    order.sort_by(|a, b| a.1.cmp(&b.1));
    let mut out: Vec<String> = v
        .as_array()
        .unwrap_or_else(|| panic!("not JSON-LD select results: {v}"))
        .iter()
        .map(|row| {
            // A one-variable select answers bare values, not rows.
            let cell_at = |i: usize| match row {
                Value::Array(cells) => cells.get(i).cloned().unwrap_or(Value::Null),
                other if select.len() == 1 => other.clone(),
                _ => Value::Null,
            };
            order
                .iter()
                .map(|(i, var)| {
                    let val = match cell_at(*i) {
                        Value::Null => String::new(),
                        Value::String(s) => s,
                        Value::Object(o) => o
                            .get("@value")
                            .or_else(|| o.get("@id"))
                            .map(|x| match x {
                                Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .unwrap_or_default(),
                        other => other.to_string(),
                    };
                    format!("{var}={}", canon(&expand(&val)))
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    out.sort();
    out
}

/// A compacted `ex:` IRI, expanded.
fn expand(v: &str) -> String {
    match v.strip_prefix("ex:") {
        Some(local) => format!("http://example.org/{local}"),
        None => v.to_string(),
    }
}

/// A decimal by value: `48.750` and `48.75` are one number.
fn canon(v: &str) -> String {
    let plain = v.strip_prefix('-').unwrap_or(v);
    let Some((int, frac)) = plain.split_once('.') else {
        return v.to_string();
    };
    if int.is_empty()
        || !int.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return v.to_string();
    }
    let frac = frac.trim_end_matches('0');
    let sign = if v.starts_with('-') { "-" } else { "" };
    if frac.is_empty() {
        format!("{sign}{int}")
    } else {
        format!("{sign}{int}.{frac}")
    }
}

/// One answer: rows, the sites that proceeded, and the SQL lane's statements.
struct Answer {
    rows: Vec<String>,
    sites: Vec<String>,
    sql: Vec<String>,
}

async fn answer(
    fluree: &Fluree,
    store: &span_capture::SpanStore,
    c: &Case,
    from: &str,
    jsonld: bool,
) -> Answer {
    let events = store.find_events("fast-path outcome").len();
    let result = if jsonld {
        let mut q = c.jsonld.clone();
        q["@context"] = json!({"ex": "http://example.org/"});
        q["from"] = json!(from);
        fluree
            .query_from()
            .jsonld(&q)
            .execute_tracked()
            .await
            .map(|t| (jsonld_rows(&t.result, c.vars), t.sql))
    } else {
        let sparql = format!(
            "{PREFIX}{}",
            c.sparql.replace("<SRC>", &format!("<{from}>"))
        );
        fluree
            .query_from()
            .sparql(&sparql)
            .execute_tracked()
            .await
            .map(|t| (sparql_rows(&t.result), t.sql))
    };
    let (rows, sql) = match result {
        Ok((rows, sql)) => (
            rows,
            sql.unwrap_or_default().into_iter().map(|s| s.sql).collect(),
        ),
        Err(e) => (vec![format!("ERROR {}", e.error)], Vec::new()),
    };
    Answer {
        rows,
        sites: store.find_events("fast-path outcome")[events..]
            .iter()
            .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
            .filter_map(|e| e.fields.get("site").cloned())
            .collect(),
        sql,
    }
}

/// The native ledger's answer on both surfaces, checked against the case's
/// written-out answer.
async fn native(
    fluree: &Fluree,
    store: &span_capture::SpanStore,
    c: &Case,
    problems: &mut Vec<String>,
) -> [Vec<String>; 2] {
    let from = format!("{}-native:main", c.src);
    let sparql = answer(fluree, store, c, &from, false).await.rows;
    let jsonld = answer(fluree, store, c, &from, true).await.rows;
    let expect: Vec<String> = c.expect.iter().map(|s| (*s).to_string()).collect();
    if sparql != expect {
        problems.push(format!(
            "native sparql {sparql:?} != written-out {expect:?}"
        ));
    }
    if jsonld != expect {
        problems.push(format!(
            "native json-ld {jsonld:?} != written-out {expect:?}"
        ));
    }
    [sparql, jsonld]
}

/// The SQL lane's own answers: every case it serves, on both surfaces,
/// against the native ledger, with its routing stamp and statements.
async fn check_sql_lane(fluree: &Fluree, cases: &[Case]) -> Vec<String> {
    let (store, _guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);
    let mut failures = Vec::new();
    for c in cases {
        let mut problems = Vec::new();
        let oracle = native(fluree, &store, c, &mut problems).await;
        let from = format!("{}-sql:main", c.src);
        for (i, (surface, jsonld)) in [("sparql", false), ("json-ld", true)].iter().enumerate() {
            let a = answer(fluree, &store, c, &from, *jsonld).await;
            let fired = a.sites.iter().any(|s| s == c.site);
            match (c.routing, fired) {
                (Routing::MustFire, false) => problems.push(format!(
                    "{surface}: `{}` must fire [proceeded: {:?}]",
                    c.site, a.sites
                )),
                (Routing::MustNotFire, true) => {
                    problems.push(format!("{surface}: `{}` must not fire", c.site));
                }
                _ => {}
            }
            if c.routing == Routing::MustNotFire && !a.sql.is_empty() {
                problems.push(format!("{surface}: a declined shape sent {:#?}", a.sql));
            }
            if c.routing == Routing::MustFire {
                if a.rows != oracle[i] {
                    problems.push(format!(
                        "{surface}: SQL lane {:?} != native {:?}",
                        a.rows, oracle[i]
                    ));
                }
                if !c.sql.is_empty() && a.sql != c.sql {
                    problems.push(format!(
                        "{surface}: statements {:#?}\nexpected exactly:\n{}",
                        a.sql,
                        c.sql.join("\n")
                    ));
                }
            }
        }
        if !problems.is_empty() {
            failures.push(format!("{}:\n  {}", c.name, problems.join("\n  ")));
        }
    }
    failures
}

#[tokio::test(flavor = "current_thread")]
async fn the_sql_lane_answers_each_triple_once() {
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "unset FLUREE_DISABLE_QUERY_FAST_PATHS: this pins nothing otherwise"
    );
    let _lock = KILL_SWITCH.lock().await;
    let (_server, fluree) = setup().await;
    let mut cases = cases();
    cases.extend(subquery_cases());
    let failures = check_sql_lane(&fluree, &cases).await;
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

/// Every other lane over the same rows: the per-scan lane the SQL lane
/// declines to (and runs with fast paths off), and the Iceberg source's
/// lanes, fast and generic.
#[tokio::test(flavor = "current_thread")]
async fn every_other_lane_answers_the_same_set() {
    let _lock = KILL_SWITCH.lock().await;
    let (_server, fluree) = setup().await;
    let (store, _guard) = span_capture::init_test_tracing();
    let mut failures = Vec::new();
    for c in &cases() {
        let mut problems = Vec::new();
        set_fast_paths_disabled(false);
        let oracle = native(&fluree, &store, c, &mut problems).await;
        for (lane, from, fast) in [
            ("sql declined", format!("{}-sql:main", c.src), true),
            ("sql per-scan", format!("{}-sql:main", c.src), false),
            ("iceberg fast", format!("{}-ice:main", c.src), true),
            ("iceberg generic", format!("{}-ice:main", c.src), false),
        ] {
            // The SQL lane's own answers are the other test's.
            if lane == "sql declined" && c.routing == Routing::MustFire {
                continue;
            }
            set_fast_paths_disabled(!fast);
            for (i, (surface, jsonld)) in [("sparql", false), ("json-ld", true)].iter().enumerate()
            {
                let a = answer(&fluree, &store, c, &from, *jsonld).await;
                if a.rows != oracle[i] {
                    problems.push(format!(
                        "{lane} {surface}: {:?} != native {:?}",
                        a.rows, oracle[i]
                    ));
                }
            }
        }
        set_fast_paths_disabled(false);
        if !problems.is_empty() {
            failures.push(format!("{}:\n  {}", c.name, problems.join("\n  ")));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

/// The statements the lane sends for `sparql` over `from`, with its rows.
async fn statements(fluree: &Fluree, from: &str, sparql: &str) -> (Vec<String>, Vec<String>) {
    let sparql = format!("{PREFIX}{}", sparql.replace("<SRC>", &format!("<{from}>")));
    let tracked = fluree
        .query_from()
        .sparql(&sparql)
        .execute_tracked()
        .await
        .unwrap_or_else(|e| panic!("{from}: {}\n{sparql}", e.error));
    let sent = tracked
        .sql
        .unwrap_or_default()
        .into_iter()
        .map(|s| s.sql)
        .collect();
    (sparql_rows(&tracked.result), sent)
}

/// A map whose subject key is proven keeps the statement the lane sent before
/// set semantics, byte for byte: proven by the registration probe (a source
/// registered without `allow_duplicate_subjects`), or by a declared `f:key`.
/// The same table with no evidence (registered with
/// `allow_duplicate_subjects`, no `f:key`) answers the same rows through a
/// statement that removes duplicates.
#[tokio::test(flavor = "current_thread")]
async fn proven_keys_keep_the_statement_byte_for_byte() {
    let _lock = KILL_SWITCH.lock().await;
    set_fast_paths_disabled(false);
    let server = sets_tables().mount().await;
    let fluree = FlureeBuilder::memory().build_memory();
    sql_source(
        &fluree,
        &server,
        "cur-probed",
        &current_mapping(false),
        true,
    )
    .await;
    sql_source(&fluree, &server, "cur-keyed", &current_mapping(true), false).await;
    sql_source(&fluree, &server, "cur-bare", &current_mapping(false), false).await;

    let shapes: &[(&str, &[&str], &[&str])] = &[
        (
            "SELECT ?s ?n FROM <SRC> WHERE { ?s ex:name ?n }",
            &[
                r#"SELECT "t0"."customer_id" AS "c0", "t0"."name" AS "c1" FROM "sets"."customers_current" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL"#,
            ],
            &[
                r#"SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."name" AS "c1" FROM "sets"."customers_current" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL"#,
            ],
        ),
        (
            "SELECT ?s ?n FROM <SRC> WHERE { ?s a ex:Customer ; ex:name ?n }",
            &[
                r#"SELECT "t0"."customer_id" AS "c0", "t0"."name" AS "c1" FROM "sets"."customers_current" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL"#,
            ],
            &[
                r#"SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."name" AS "c1" FROM "sets"."customers_current" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL"#,
            ],
        ),
        (
            "SELECT (COUNT(*) AS ?k) FROM <SRC> WHERE { ?s ex:name ?n }",
            &[
                r#"SELECT COUNT(*) AS "c0" FROM "sets"."customers_current" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL"#,
            ],
            &[
                r#"SELECT COUNT(*) AS "c0" FROM (SELECT DISTINCT "t0"."customer_id" AS "c0", "t0"."name" AS "c1" FROM "sets"."customers_current" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL) AS "s""#,
            ],
        ),
    ];
    let mut failures = Vec::new();
    for (sparql, proven, bare) in shapes {
        let (rows, _) = statements(&fluree, "cur-probed:main", sparql).await;
        for (from, expect) in [
            ("cur-probed:main", *proven),
            ("cur-keyed:main", *proven),
            ("cur-bare:main", *bare),
        ] {
            let (got_rows, sent) = statements(&fluree, from, sparql).await;
            if sent != expect {
                failures.push(format!(
                    "{from} {sparql}:\n  sent {sent:#?}\n  expected {expect:#?}"
                ));
            }
            if got_rows != rows {
                failures.push(format!(
                    "{from} {sparql}: rows {got_rows:?} != the probed source's {rows:?}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

/// What the duplicate-subject probe verified is what the lane trusts. An
/// endpoint unreachable at registration verifies nothing, so the lane removes
/// repeated solutions until `fluree sql check` records the key; a source
/// registered before the probe recorded its findings keeps the trust it had.
#[tokio::test(flavor = "current_thread")]
async fn the_lane_trusts_only_what_the_probe_verified() {
    let _lock = KILL_SWITCH.lock().await;
    set_fast_paths_disabled(false);
    // Answers nothing yet: the registration's connection test fails.
    let server = MockServer::start().await;
    let fluree = FlureeBuilder::memory().build_memory();
    let created = fluree
        .create_sql_graph_source(SqlCreateConfig::new(
            "cur-late",
            server.uri(),
            current_mapping(false),
        ))
        .await
        .expect("registers despite the unreachable endpoint");
    assert!(!created.connection_tested);
    sets_tables().mount_on(&server).await;

    let sparql = "SELECT ?s ?n FROM <SRC> WHERE { ?s ex:name ?n }";
    let plain = r#"SELECT "t0"."customer_id" AS "c0", "t0"."name" AS "c1" FROM "sets"."customers_current" AS "t0" WHERE "t0"."customer_id" IS NOT NULL AND "t0"."name" IS NOT NULL"#;
    let distinct = format!("SELECT DISTINCT {}", &plain["SELECT ".len()..]);
    let config = || async {
        let record = fluree
            .nameservice()
            .lookup_graph_source("cur-late:main")
            .await
            .expect("lookup")
            .expect("the record");
        serde_json::from_str::<Value>(&record.config).expect("config JSON")
    };

    let (rows, sent) = statements(&fluree, "cur-late:main", sparql).await;
    assert_eq!(sent, [distinct], "an unprobed table proves nothing");
    assert_eq!(config().await["verified_subject_keys"], json!({}));

    // The same record as an older registration wrote it: no field.
    let mut legacy = config().await;
    legacy
        .as_object_mut()
        .expect("config object")
        .remove("verified_subject_keys");
    fluree
        .publisher()
        .expect("publisher")
        .publish_graph_source(
            "cur-late",
            "main",
            GraphSourceType::Sql,
            &legacy.to_string(),
            &[],
        )
        .await
        .expect("republish");
    let (legacy_rows, sent) = statements(&fluree, "cur-late:main", sparql).await;
    assert_eq!(
        sent,
        [plain.to_string()],
        "a legacy record keeps today's trust"
    );
    assert_eq!(legacy_rows, rows);

    // `check` probes the now reachable table and records its key.
    let checked = fluree
        .check_sql_graph_source("cur-late:main")
        .await
        .expect("check");
    assert!(checked.duplicate_subject_tables.is_empty());
    assert_eq!(
        config().await["verified_subject_keys"],
        json!({"sets.customers_current": ["customer_id"]})
    );
    let (checked_rows, sent) = statements(&fluree, "cur-late:main", sparql).await;
    assert_eq!(sent, [plain.to_string()]);
    assert_eq!(checked_rows, rows);
}

/// A key the mapping declares must name columns the SQL table holds, of a
/// type that can identify a row: the query refuses on both surfaces rather
/// than trust a key the table cannot hold.
#[tokio::test(flavor = "current_thread")]
async fn a_declared_key_the_table_cannot_hold_refuses_the_query() {
    let _lock = KILL_SWITCH.lock().await;
    set_fast_paths_disabled(false);
    let server = sets_tables().mount().await;
    let fluree = FlureeBuilder::memory().build_memory();
    let mapping = CURRENT_R2RML.replace("{KEY}", "; f:key ( \"customer_ref\" )");
    sql_source(&fluree, &server, "cur-badkey", &mapping, true).await;
    let sparql = format!("{PREFIX}SELECT ?s ?n FROM <cur-badkey:main> WHERE {{ ?s ex:name ?n }}");
    let sparql_err = fluree
        .query_from()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .expect_err("SPARQL refuses")
        .to_string();
    let jsonld_err = fluree
        .query_from()
        .jsonld(&json!({
            "@context": {"ex": "http://example.org/"},
            "from": "cur-badkey:main",
            "select": ["?s", "?n"],
            "where": [{"@id": "?s", "ex:name": "?n"}],
        }))
        .execute_formatted()
        .await
        .expect_err("JSON-LD refuses")
        .to_string();
    for err in [sparql_err, jsonld_err] {
        assert!(
            err.contains("customer_ref") && err.contains("sets.customers_current"),
            "{err}"
        );
    }
}

/// A timestamp key identifies a term only to the microsecond its values are
/// read to: two values a `timestamp(9)` keeps a nanosecond apart render one
/// term, so a key declared over one refuses the query; one over a
/// `timestamp(6)` holds.
#[tokio::test(flavor = "current_thread")]
async fn a_declared_key_over_a_nanosecond_timestamp_refuses_the_query() {
    let _lock = KILL_SWITCH.lock().await;
    set_fast_paths_disabled(false);
    let server = FakeSql::new()
        .table(Table::new(
            "sets.stamps",
            &[
                ("stamp9", "timestamp(9)"),
                ("stamp6", "timestamp(6)"),
                ("label", "varchar"),
            ],
            vec![vec![Value::Null, Value::Null, json!("a")]],
        ))
        .mount()
        .await;
    let fluree = FlureeBuilder::memory().build_memory();
    let mapping = |key: &str| {
        format!(
            r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .
        @prefix f: <https://ns.flur.ee/db#> .
        <http://example.org/mapping#Stamp>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "sets.stamps" ; f:key ( "{key}" ) ] ;
            rr:subjectMap [ rr:template "http://example.org/stamp/{{label}}" ] ;
            rr:predicateObjectMap [ rr:predicate ex:label ; rr:objectMap [ rr:column "label" ] ] .
        "#
        )
    };
    sql_source(&fluree, &server, "stamp9", &mapping("stamp9"), false).await;
    sql_source(&fluree, &server, "stamp6", &mapping("stamp6"), false).await;
    let sparql =
        |from: &str| format!("{PREFIX}SELECT ?s ?l FROM <{from}:main> WHERE {{ ?s ex:label ?l }}");
    let jsonld = |from: &str| {
        json!({
            "@context": {"ex": "http://example.org/"},
            "from": format!("{from}:main"),
            "select": ["?s", "?l"],
            "where": [{"@id": "?s", "ex:label": "?l"}],
        })
    };
    let sparql_err = fluree
        .query_from()
        .sparql(&sparql("stamp9"))
        .execute_formatted()
        .await
        .expect_err("SPARQL refuses")
        .to_string();
    let jsonld_err = fluree
        .query_from()
        .jsonld(&jsonld("stamp9"))
        .execute_formatted()
        .await
        .expect_err("JSON-LD refuses")
        .to_string();
    for err in [sparql_err, jsonld_err] {
        assert!(
            err.contains("stamp9") && err.contains("microsecond"),
            "{err}"
        );
    }
    let rows = sparql_rows(&query_sparql(&fluree, &sparql("stamp6")).await);
    assert_eq!(rows, ["l=a s=http://example.org/stamp/a"]);
    fluree
        .query_from()
        .jsonld(&jsonld("stamp6"))
        .execute_formatted()
        .await
        .expect("a microsecond timestamp key holds");

    // The probe records a key it found unique only where its columns can
    // identify a term: not over the nanosecond timestamp.
    let subject_over = |column: &str| {
        format!(
            r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .
        <http://example.org/mapping#Stamp>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "sets.stamps" ] ;
            rr:subjectMap [ rr:template "http://example.org/stamp/{{{column}}}" ] ;
            rr:predicateObjectMap [ rr:predicate ex:label ; rr:objectMap [ rr:column "label" ] ] .
        "#
        )
    };
    for (name, column, recorded) in [
        ("probed9", "stamp9", json!({})),
        ("probed6", "stamp6", json!({"sets.stamps": ["stamp6"]})),
    ] {
        let created = fluree
            .create_sql_graph_source(SqlCreateConfig::new(
                name,
                server.uri(),
                subject_over(column),
            ))
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
        let record = fluree
            .nameservice()
            .lookup_graph_source(&format!("{name}:main"))
            .await
            .expect("lookup")
            .expect("the record");
        let config: Value = serde_json::from_str(&record.config).expect("config JSON");
        assert_eq!(config["verified_subject_keys"], recorded, "{name}");
        let warned = created
            .mapping_warnings
            .iter()
            .any(|w| w.contains(column) && w.contains("cannot identify a term"));
        assert_eq!(
            warned,
            column == "stamp9",
            "{name}: {:?}",
            created.mapping_warnings
        );
    }
}

async fn query_sparql(fluree: &Fluree, sparql: &str) -> Value {
    fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{e}\n{sparql}"))
}

// ---------------------------------------------------------------------------
// Live databases behind the bridge
// ---------------------------------------------------------------------------

/// The `sets.*` rows as statements every bridge backend runs, under
/// unqualified table names.
const LIVE_SEED: &[&str] = &[
    "DROP TABLE IF EXISTS customers_history",
    "DROP TABLE IF EXISTS customers_current",
    "DROP TABLE IF EXISTS orders",
    "CREATE TABLE customers_history (customer_id BIGINT, name VARCHAR(64), city VARCHAR(64), valid_from DATE, is_current INTEGER)",
    "CREATE TABLE customers_current (customer_id BIGINT NOT NULL, name VARCHAR(64))",
    "CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, amount DECIMAL(10,2))",
    "INSERT INTO customers_history VALUES (1, 'alice', 'Boston', '2024-01-01', 0), (2, 'bob', 'Austin', '2024-01-01', 0), (3, 'carol', 'Chicago', '2024-02-01', 1), (1, 'alice', 'Denver', '2024-06-01', 0), (5, 'erin', 'Miami', '2024-01-01', 0), (2, 'robert', 'Austin', '2024-05-01', 0), (2, 'robert', 'Austin', '2024-09-01', 1), (4, 'dave', NULL, '2024-03-01', 1), (1, 'alice', 'Denver', '2024-11-01', 1), (5, 'erin-m', 'Tampa', '2024-07-01', 1), (NULL, 'ghost', 'Nowhere', '2024-01-01', 0)",
    "INSERT INTO customers_current VALUES (1, 'alice'), (2, 'robert'), (3, 'carol'), (4, 'dave'), (5, 'erin-m'), (6, 'frank')",
    "INSERT INTO orders VALUES (100, 1, 10.00), (101, 2, 20.00), (102, 3, 5.50), (103, 5, 7.25), (101, 2, 20.00), (104, 99, 1.00), (105, NULL, 3.00), (106, 1, 2.00)",
];

/// The SQL lane over a live database answers what the native ledger does
/// wherever it serves a shape. A shape it declines is the per-scan lane's
/// (see [`every_other_lane_answers_the_same_set`]); the lane may decline a
/// shape it serves over the fake endpoint only where the dialect's `DISTINCT`
/// folds strings, or where `declines` names it (a column reaching the bridge
/// as a type the aggregate lane cannot fold exactly).
async fn live_set_semantics(dialect: SqlDialect, url_var: &str, declines: &[&str]) {
    let Ok(url) = std::env::var(url_var) else {
        eprintln!("{url_var} unset: live {dialect:?} set-semantics differential skipped");
        return;
    };
    let _lock = KILL_SWITCH.lock().await;
    set_fast_paths_disabled(false);
    let mut cfg = fluree_db_sql::SqlGsConfig::new(url.clone());
    cfg.dialect = dialect;
    let cfg = cfg.hydrate(None).await.expect("hydrate");
    let auth = cfg
        .auth
        .create_provider_arc(&fluree_db_iceberg::IdentityContext::default())
        .expect("auth");
    let client = fluree_db_sql::TrinoClient::new(&cfg, auth).expect("client");
    for stmt in LIVE_SEED {
        client
            .execute_collect(stmt)
            .await
            .unwrap_or_else(|e| panic!("seed failed: {e}\n{stmt}"));
    }
    let fluree = FlureeBuilder::memory().build_memory();
    for (name, mapping) in [("hist-sql", HIST_R2RML), ("both-sql", BOTH_R2RML)] {
        let mut config = SqlCreateConfig::new(name, url.clone(), mapping.replace("sets.", ""));
        config.dialect = dialect;
        config.allow_duplicate_subjects = true;
        fluree
            .create_sql_graph_source(config)
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    seed_native(&fluree, "hist-native:main", false).await;
    seed_native(&fluree, "both-native:main", true).await;

    let folds_strings = matches!(dialect, SqlDialect::Mysql);
    let (store, _guard) = span_capture::init_test_tracing();
    let mut failures = Vec::new();
    for c in &cases() {
        let mut problems = Vec::new();
        let oracle = native(&fluree, &store, c, &mut problems).await;
        let a = answer(&fluree, &store, c, &format!("{}-sql:main", c.src), false).await;
        let fired = a.sites.iter().any(|s| s == c.site);
        match (c.routing, fired) {
            (_, true) if a.rows != oracle[0] => problems.push(format!(
                "SQL lane {:?} != native {:?}\n  sent {:#?}",
                a.rows, oracle[0], a.sql
            )),
            (Routing::MustNotFire, true) => problems.push(format!("`{}` must not fire", c.site)),
            (Routing::MustFire, false) if !folds_strings && !declines.contains(&c.name) => {
                problems.push(format!("`{}` must fire [proceeded: {:?}]", c.site, a.sites));
            }
            _ => {}
        }
        if !problems.is_empty() {
            failures.push(format!("{}:\n  {}", c.name, problems.join("\n  ")));
        }
    }
    assert!(
        failures.is_empty(),
        "{dialect:?}:\n{}",
        failures.join("\n\n")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn live_bridge_sqlite_answers_each_triple_once() {
    // SQLite's `NUMERIC` reaches the bridge as text or double, so a SUM over
    // it declines (its datatype is decimal), as in the pushdown lane suite.
    live_set_semantics(
        SqlDialect::Sqlite,
        "FLUREE_SQL_BRIDGE_URL",
        &["SUM over a duplicated row adds its triple once"],
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn live_bridge_postgres_answers_each_triple_once() {
    live_set_semantics(SqlDialect::Postgres, "FLUREE_SQL_BRIDGE_POSTGRES_URL", &[]).await;
}

#[tokio::test(flavor = "current_thread")]
async fn live_bridge_mysql_answers_each_triple_once() {
    live_set_semantics(SqlDialect::Mysql, "FLUREE_SQL_BRIDGE_MYSQL_URL", &[]).await;
}
