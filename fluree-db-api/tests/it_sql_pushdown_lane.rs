//! The SQL pushdown lane: one statement per GRAPH block over a SQL source.
//!
//! Every admitted shape is pinned three ways against a fake endpoint that
//! executes the statement it receives: the exact SQL (the statement a SQL
//! expert would write for the mapping), the rows, and the routing stamp
//! (`MustFire` for admitted shapes, `MustNotFire` for declined ones). Every
//! shape then runs again with fast paths disabled and must return the same
//! rows — the per-scan lane is the oracle.

#![cfg(all(feature = "sql", feature = "native"))]

#[path = "support/fake_sql.rs"]
mod fake_sql;
#[path = "support/span_capture.rs"]
mod span_capture;

use fake_sql::{FakeSql, Table};
use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, SqlCreateConfig};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use wiremock::MockServer;

/// The shop mapping over `shop.customers` / `shop.orders` (the fixture) or,
/// for a live SQLite run, unqualified table names.
fn shop_mapping(prefix: &str) -> String {
    SHOP_R2RML.replace("shop.", prefix)
}

const SHOP_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#Customer>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "shop.customers" ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{id}" ; rr:class ex:Customer ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:country ; rr:objectMap [ rr:column "country" ] ] .

    <http://example.org/mapping#Order>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "shop.orders" ] ;
        rr:subjectMap [ rr:template "http://example.org/order/{id}" ; rr:class ex:Order ] ;
        rr:predicateObjectMap [
            rr:predicate ex:total ;
            rr:objectMap [ rr:column "total" ; rr:datatype xsd:decimal ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:placed ;
            rr:objectMap [ rr:column "placed" ; rr:datatype xsd:date ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:shipped ;
            rr:objectMap [ rr:column "shipped" ; rr:datatype xsd:dateTime ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:updated ;
            rr:objectMap [ rr:column "updated" ; rr:datatype xsd:dateTime ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:customer ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Customer> ;
                rr:joinCondition [ rr:child "customer_id" ; rr:parent "id" ]
            ]
        ] .
"#;

const SITE: &str = "sql_block_pushdown";

/// The global kill switch is process-wide; tests that flip it take this.
static KILL_SWITCH: Mutex<()> = Mutex::const_new(());

async fn shop() -> MockServer {
    FakeSql::new()
        .table(Table::new(
            "shop.customers",
            &[
                ("id", "bigint"),
                ("name", "varchar"),
                ("country", "varchar"),
            ],
            vec![
                vec![json!(1), json!("Ada"), json!("UK")],
                vec![json!(2), json!("Bo"), Value::Null],
                vec![json!(3), json!("Cy"), json!("US")],
            ],
        ))
        .table(Table::new(
            "shop.orders",
            &[
                ("id", "bigint"),
                ("customer_id", "bigint"),
                ("total", "decimal(10,2)"),
                ("placed", "date"),
                // A zoned and a naive timestamp: only the zoned one compares
                // exactly with an xsd:dateTime literal in SQL.
                ("shipped", "timestamp(6) with time zone"),
                ("updated", "timestamp(6)"),
            ],
            vec![
                vec![
                    json!(10),
                    json!(1),
                    json!("99.50"),
                    json!("2024-01-05"),
                    json!("2024-01-06 09:30:00.000000 UTC"),
                    json!("2024-01-06 09:30:00.000000"),
                ],
                vec![
                    json!(11),
                    json!(1),
                    json!("5.00"),
                    json!("2024-02-01"),
                    json!("2024-02-02 18:00:00.000000 UTC"),
                    json!("2024-02-02 18:00:00.000000"),
                ],
                vec![
                    json!(12),
                    json!(2),
                    json!("42.00"),
                    json!("2024-03-01"),
                    Value::Null,
                    Value::Null,
                ],
                vec![
                    json!(13),
                    Value::Null,
                    json!("7.00"),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ],
            ],
        ))
        .mount()
        .await
}

async fn setup() -> (MockServer, Fluree) {
    let server = shop().await;
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_sql_graph_source(SqlCreateConfig::new(
            "shop-sql",
            server.uri(),
            shop_mapping("shop."),
        ))
        .await
        .expect("create sql source");
    (server, fluree)
}

/// SPARQL JSON results as sorted rows of `var=value` pairs (variables in
/// alphabetical order, which is also the order SPARQL JSON reports them).
fn rows_of(v: &Value) -> Vec<String> {
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
                    format!("{var}={val}")
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    out.sort();
    out
}

async fn query(fluree: &Fluree, sparql: &str) -> Value {
    fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{sparql}"))
}

async fn block_statements(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.method == "POST")
        .map(|r| String::from_utf8_lossy(&r.body).to_string())
        // Only the lane aliases its accesses; per-scan statements and the
        // probes never carry `AS "t0"`.
        .filter(|s| s.contains(r#" AS "t0""#))
        .collect()
}

fn proceeded_sites(store: &span_capture::SpanStore, from: usize) -> Vec<String> {
    store.find_events("fast-path outcome")[from..]
        .iter()
        .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
        .filter_map(|e| e.fields.get("site").cloned())
        .collect()
}

enum Routing {
    MustFire,
    MustNotFire,
}

struct Case {
    name: &'static str,
    sparql: &'static str,
    /// The exact statements, in order, when the lane fires.
    sql: &'static [&'static str],
    rows: &'static [&'static str],
    routing: Routing,
    /// The decline reason the lowering must report, for `MustNotFire` shapes.
    declined: Option<&'static str>,
}

const PREFIX: &str =
    "PREFIX ex: <http://example.org/>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "star with an exact numeric filter",
            sparql: "SELECT ?o ?t FROM <shop-sql:main> WHERE { ?o ex:total ?t FILTER(?t > 40) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL AND "t0"."total" > 40"#],
            rows: &[
                "o=http://example.org/order/10 t=99.50",
                "o=http://example.org/order/12 t=42.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "foreign-key join between two entities",
            sparql: "SELECT ?o ?n FROM <shop-sql:main> WHERE { ?o ex:customer ?c . ?c ex:name ?n }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t1"."id" AS "c1", "t1"."name" AS "c2" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL AND "t1"."name" IS NOT NULL"#],
            rows: &[
                "n=Ada o=http://example.org/order/10",
                "n=Ada o=http://example.org/order/11",
                "n=Bo o=http://example.org/order/12",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "foreign-key object alone joins the parent for its IRI",
            sparql: "SELECT ?o ?c FROM <shop-sql:main> WHERE { ?o ex:customer ?c }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t1"."id" AS "c1" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL"#],
            rows: &[
                "c=http://example.org/customer/1 o=http://example.org/order/10",
                "c=http://example.org/customer/1 o=http://example.org/order/11",
                "c=http://example.org/customer/2 o=http://example.org/order/12",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "constant subject reverses through the template",
            sparql: "SELECT ?t FROM <shop-sql:main> WHERE { <http://example.org/order/10> ex:total ?t }",
            sql: &[r#"SELECT "t0"."total" AS "c0" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."id" = 10 AND "t0"."total" IS NOT NULL"#],
            rows: &["t=99.50"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "optional member of the same entity is a nullable column",
            sparql: "SELECT ?n ?k FROM <shop-sql:main> WHERE { ?c ex:name ?n OPTIONAL { ?c ex:country ?k } }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1", "t0"."country" AS "c2" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
            rows: &["k= n=Bo", "k=UK n=Ada", "k=US n=Cy"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "optional entity hanging off a foreign key is a left join",
            sparql: "SELECT ?n ?o FROM <shop-sql:main> WHERE { ?c ex:name ?n OPTIONAL { ?o ex:customer ?c } }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1", "t1"."id" AS "c2" FROM "shop"."customers" AS "t0" LEFT JOIN "shop"."orders" AS "t1" ON "t1"."id" IS NOT NULL AND "t1"."customer_id" IS NOT NULL AND "t1"."customer_id" = "t0"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
            rows: &[
                "n=Ada o=http://example.org/order/10",
                "n=Ada o=http://example.org/order/11",
                "n=Bo o=http://example.org/order/12",
                "n=Cy o=",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "unpushable filter stays in the engine as a residual",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { ?c ex:name ?n FILTER(STRLEN(?n) > 2) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
            rows: &["n=Ada"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "typed date filter pushes as a DATE literal",
            sparql: "SELECT ?o FROM <shop-sql:main> WHERE { ?o ex:placed ?p FILTER(?p >= \"2024-02-01\"^^xsd:date) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."placed" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."placed" IS NOT NULL AND "t0"."placed" >= DATE '2024-02-01'"#],
            rows: &["o=http://example.org/order/11", "o=http://example.org/order/12"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "IN list pushes as a set",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { ?c ex:name ?n FILTER(?n IN (\"Ada\", \"Cy\")) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL AND "t0"."name" IN ('Ada', 'Cy')"#],
            rows: &["n=Ada", "n=Cy"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "VALUES in the block is a static key set",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { VALUES ?c { <http://example.org/customer/2> } ?c ex:name ?n }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" JOIN (VALUES (2)) AS "v0" ("k0") ON "v0"."k0" = "t0"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
            rows: &["n=Bo"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "LIMIT without ORDER BY pushes a LIMIT",
            sparql: "SELECT ?o FROM <shop-sql:main> WHERE { ?o ex:total ?t } LIMIT 2",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL LIMIT 2"#],
            rows: &["o=http://example.org/order/10", "o=http://example.org/order/11"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "dateTime filter pushes as a zoned TIMESTAMP against a zoned column",
            sparql: "SELECT ?o FROM <shop-sql:main> WHERE { ?o ex:shipped ?s FILTER(?s > \"2024-01-10T00:00:00Z\"^^xsd:dateTime) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."shipped" AT TIME ZONE 'UTC' AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."shipped" IS NOT NULL AND "t0"."shipped" > TIMESTAMP '2024-01-10 00:00:00.000000 UTC'"#],
            rows: &["o=http://example.org/order/11"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "dateTime filter on a naive timestamp column stays in the engine",
            sparql: "SELECT ?o FROM <shop-sql:main> WHERE { ?o ex:updated ?u FILTER(?u > \"2024-01-10T00:00:00Z\"^^xsd:dateTime) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."updated" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."updated" IS NOT NULL"#],
            rows: &["o=http://example.org/order/11"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "ORDER BY DESC LIMIT pushes a top-k",
            sparql: "SELECT ?o ?t FROM <shop-sql:main> WHERE { ?o ex:total ?t } ORDER BY DESC(?t) LIMIT 2",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL ORDER BY "t0"."total" DESC LIMIT 2"#],
            rows: &[
                "o=http://example.org/order/10 t=99.50",
                "o=http://example.org/order/12 t=42.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "OFFSET widens the pushed top-k",
            sparql: "SELECT ?o ?t FROM <shop-sql:main> WHERE { ?o ex:total ?t } ORDER BY DESC(?t) OFFSET 1 LIMIT 1",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL ORDER BY "t0"."total" DESC LIMIT 2"#],
            rows: &["o=http://example.org/order/12 t=42.00"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a residual filter keeps LIMIT in the engine",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { ?c ex:name ?n FILTER(STRLEN(?n) > 1) } LIMIT 1",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
            rows: &["n=Ada"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "constant subject whose key cannot be the column's type is empty without a round trip",
            sparql: "SELECT ?t FROM <shop-sql:main> WHERE { <http://example.org/order/abc> ex:total ?t }",
            sql: &[],
            rows: &[],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "predicates of two triples maps on one subject are empty without a round trip",
            sparql: "SELECT ?n ?t FROM <shop-sql:main> WHERE { ?x ex:name ?n . ?x ex:total ?t }",
            sql: &[],
            rows: &[],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a variable shared by two value classes declines",
            sparql: "SELECT ?c ?o FROM <shop-sql:main> WHERE { ?c ex:name ?v . ?o ex:total ?v }",
            sql: &[],
            rows: &[],
            routing: Routing::MustNotFire,
            declined: Some("repeated variable joins two value classes"),
        },
        Case {
            name: "an optional hanging off an optional entity declines",
            sparql: "SELECT ?n ?t FROM <shop-sql:main> WHERE { ?c ex:name ?n OPTIONAL { ?o ex:customer ?c } OPTIONAL { ?o ex:total ?t } }",
            sql: &[],
            rows: &["n=Ada t=5.00", "n=Ada t=99.50", "n=Bo t=42.00", "n=Cy t="],
            routing: Routing::MustNotFire,
            declined: Some("optional chained on an optional entity"),
        },
        Case {
            name: "an inexact filter inside OPTIONAL declines",
            sparql: "SELECT ?n ?k FROM <shop-sql:main> WHERE { ?c ex:name ?n OPTIONAL { ?c ex:country ?k FILTER(STRLEN(?k) > 1) } }",
            sql: &[],
            rows: &["k= n=Bo", "k=UK n=Ada", "k=US n=Cy"],
            routing: Routing::MustNotFire,
            declined: Some("filter inside a folded optional"),
        },
        Case {
            name: "VALUES over an optional variable declines",
            sparql: "SELECT ?n ?k FROM <shop-sql:main> WHERE { ?c ex:name ?n OPTIONAL { ?c ex:country ?k } VALUES ?k { \"UK\" } }",
            sql: &[],
            // An unbound ?k is compatible with the VALUES row, so Bo keeps it;
            // a WHERE on the column would have dropped that row.
            rows: &["k=UK n=Ada", "k=UK n=Bo"],
            routing: Routing::MustNotFire,
            declined: Some("VALUES over an optional variable"),
        },
        Case {
            name: "variable predicate is not admitted",
            sparql: "SELECT ?p ?v FROM <shop-sql:main> WHERE { <http://example.org/customer/1> ?p ?v }",
            sql: &[],
            rows: &[
                "p=http://example.org/country v=UK",
                "p=http://example.org/name v=Ada",
                "p=http://www.w3.org/1999/02/22-rdf-syntax-ns#type v=http://example.org/Customer",
            ],
            routing: Routing::MustNotFire,
            declined: None,
        },
        Case {
            name: "ORDER BY ASC LIMIT pushes a top-k",
            sparql: "SELECT ?o ?t FROM <shop-sql:main> WHERE { ?o ex:total ?t } ORDER BY ?t LIMIT 2",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL ORDER BY "t0"."total" ASC LIMIT 2"#],
            rows: &[
                "o=http://example.org/order/11 t=5.00",
                "o=http://example.org/order/13 t=7.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "SELECT DISTINCT is DISTINCT over the projected columns",
            sparql: "SELECT DISTINCT ?n FROM <shop-sql:main> WHERE { ?o ex:customer ?c . ?c ex:name ?n }",
            sql: &[r#"SELECT DISTINCT "t1"."name" AS "c0" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL AND "t1"."name" IS NOT NULL"#],
            rows: &["n=Ada", "n=Bo"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "DISTINCT keeps the columns a residual filter reads",
            sparql: "SELECT DISTINCT ?c FROM <shop-sql:main> WHERE { ?o ex:customer ?c . ?c ex:name ?n FILTER(STRLEN(?n) > 2) }",
            sql: &[r#"SELECT DISTINCT "t1"."id" AS "c0", "t1"."name" AS "c1" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL AND "t1"."name" IS NOT NULL"#],
            rows: &["c=http://example.org/customer/1"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "UNION runs one statement per branch",
            sparql: "SELECT ?o ?v FROM <shop-sql:main> WHERE { { ?o ex:total ?v } UNION { ?o ex:placed ?v } }",
            sql: &[
                r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL"#,
                r#"SELECT "t0"."id" AS "c0", "t0"."placed" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."placed" IS NOT NULL"#,
            ],
            rows: &[
                "o=http://example.org/order/10 v=2024-01-05",
                "o=http://example.org/order/10 v=99.50",
                "o=http://example.org/order/11 v=2024-02-01",
                "o=http://example.org/order/11 v=5.00",
                "o=http://example.org/order/12 v=2024-03-01",
                "o=http://example.org/order/12 v=42.00",
                "o=http://example.org/order/13 v=7.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "UNION branches carry the block's other triples and their own filters",
            sparql: "SELECT ?o ?n FROM <shop-sql:main> WHERE { ?o ex:customer ?c . ?c ex:name ?n . { ?c ex:country \"UK\" } UNION { ?o ex:total ?t FILTER(?t > 40) } }",
            sql: &[
                r#"SELECT "t0"."id" AS "c0", "t1"."id" AS "c1", "t1"."name" AS "c2" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL AND "t1"."name" IS NOT NULL AND "t1"."country" IS NOT NULL AND "t1"."country" = 'UK'"#,
                r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1", "t1"."id" AS "c2", "t1"."name" AS "c3" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t0"."total" IS NOT NULL AND "t0"."total" > 40 AND "t1"."id" IS NOT NULL AND "t1"."name" IS NOT NULL"#,
            ],
            rows: &[
                "n=Ada o=http://example.org/order/10",
                "n=Ada o=http://example.org/order/10",
                "n=Ada o=http://example.org/order/11",
                "n=Bo o=http://example.org/order/12",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a UNION branch that can yield nothing sends no statement",
            sparql: "SELECT ?o ?t FROM <shop-sql:main> WHERE { { ?o ex:total ?t } UNION { ?o ex:total ?t . ?o ex:name ?x } }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL"#],
            rows: &[
                "o=http://example.org/order/10 t=99.50",
                "o=http://example.org/order/11 t=5.00",
                "o=http://example.org/order/12 t=42.00",
                "o=http://example.org/order/13 t=7.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a UNION branch with a nested OPTIONAL is not admitted",
            sparql: "SELECT ?o ?v FROM <shop-sql:main> WHERE { { ?o ex:total ?v } UNION { ?o ex:placed ?v OPTIONAL { ?o ex:customer ?c OPTIONAL { ?c ex:name ?n } } } }",
            sql: &[],
            rows: &[
                "o=http://example.org/order/10 v=2024-01-05",
                "o=http://example.org/order/10 v=99.50",
                "o=http://example.org/order/11 v=2024-02-01",
                "o=http://example.org/order/11 v=5.00",
                "o=http://example.org/order/12 v=2024-03-01",
                "o=http://example.org/order/12 v=42.00",
                "o=http://example.org/order/13 v=7.00",
            ],
            routing: Routing::MustNotFire,
            declined: None,
        },
        Case {
            name: "disconnected entities decline (no cartesian product)",
            sparql: "SELECT ?n ?t FROM <shop-sql:main> WHERE { ?c ex:name ?n . ?o ex:total ?t FILTER(?t > 90) }",
            sql: &[],
            rows: &["n=Ada t=99.50", "n=Bo t=99.50", "n=Cy t=99.50"],
            routing: Routing::MustNotFire,
            declined: Some("disconnected entities (cartesian product)"),
        },
    ]
}

#[tokio::test]
async fn admitted_shapes_send_the_expert_statement_and_match_the_scan_lane() {
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "unset FLUREE_DISABLE_QUERY_FAST_PATHS: the lane phase would pin nothing"
    );
    let _lock = KILL_SWITCH.lock().await;
    let (server, fluree) = setup().await;
    let cases = cases();

    let (store, tracing_guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);
    let mut failures: Vec<String> = Vec::new();
    let mut lane_rows: Vec<Vec<String>> = Vec::new();
    for c in &cases {
        let before_events = store.find_events("fast-path outcome").len();
        let before_declines = store.find_events("sql pushdown declined").len();
        let before_stmts = block_statements(&server).await.len();
        let rows = rows_of(&query(&fluree, &format!("{PREFIX}{}", c.sparql)).await);
        let proceeded = proceeded_sites(&store, before_events);
        let declined: Vec<String> = store.find_events("sql pushdown declined")[before_declines..]
            .iter()
            .filter_map(|e| e.fields.get("why").cloned())
            .collect();
        let sent: Vec<String> = block_statements(&server).await[before_stmts..].to_vec();
        let expected: Vec<String> = c.rows.iter().map(|s| (*s).to_string()).collect();
        if rows != expected {
            failures.push(format!(
                "{}: lane rows {rows:?}, expected {expected:?}",
                c.name
            ));
        }
        match c.routing {
            Routing::MustFire => {
                if !proceeded.iter().any(|s| s == SITE) {
                    failures.push(format!(
                        "{}: expected `{SITE}` to proceed [proceeded: {proceeded:?}, declined: {declined:?}]",
                        c.name
                    ));
                }
            }
            Routing::MustNotFire => {
                if proceeded.iter().any(|s| s == SITE) {
                    failures.push(format!(
                        "{}: `{SITE}` proceeded on a declined shape",
                        c.name
                    ));
                }
                if let Some(why) = c.declined {
                    if !declined.iter().any(|d| d == why) {
                        failures.push(format!(
                            "{}: expected decline `{why}`, got {declined:?}",
                            c.name
                        ));
                    }
                }
            }
        }
        if !c.sql.is_empty() {
            if sent != c.sql {
                failures.push(format!(
                    "{}: statements sent {sent:#?}\nexpected exactly:\n{}",
                    c.name,
                    c.sql.join("\n")
                ));
            }
        } else if !sent.is_empty() {
            failures.push(format!("{}: unexpected block statements {sent:#?}", c.name));
        }
        lane_rows.push(rows);
    }
    drop(tracing_guard);

    set_fast_paths_disabled(true);
    for (i, c) in cases.iter().enumerate() {
        let rows = rows_of(&query(&fluree, &format!("{PREFIX}{}", c.sparql)).await);
        if rows != lane_rows[i] {
            failures.push(format!(
                "{}: scan lane rows {rows:?} differ from lane rows {:?}",
                c.name, lane_rows[i]
            ));
        }
    }
    set_fast_paths_disabled(false);

    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

/// A static view policy prunes the mapping before the statement is built:
/// a hidden required predicate empties the block without a round trip, a
/// hidden column is never selected, and a subject-targeted policy (not
/// static) declines to the per-scan lane, which enforces it row by row.
#[tokio::test]
async fn static_policy_prunes_the_statement() {
    let _lock = KILL_SWITCH.lock().await;
    let (server, fluree) = setup().await;
    let context = json!({"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"});
    let deny_country = json!([{
        "@id": "http://example.org/noCountry", "@type": "f:AccessPolicy", "f:action": "f:view",
        "f:allow": false, "f:onProperty": [{"@id": "http://example.org/country"}]
    }]);

    let run = |policy: Value, r#where: Value, select: Value| {
        let fluree = &fluree;
        let context = context.clone();
        async move {
            let q = json!({
                "@context": context,
                "from": "shop-sql:main",
                "opts": {"policy": policy, "default-allow": true},
                "select": select,
                "where": r#where,
            });
            fluree
                .query_from()
                .jsonld(&q)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("policy query failed: {e}"))
        }
    };

    let (store, tracing_guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);

    // A required hidden predicate: nothing to send.
    let before = block_statements(&server).await.len();
    let rows = run(
        deny_country.clone(),
        json!({"@id": "?c", "ex:name": "?n", "ex:country": "?k"}),
        json!(["?n", "?k"]),
    )
    .await;
    assert_eq!(rows.as_array().map(Vec::len), Some(0), "{rows}");
    assert_eq!(
        block_statements(&server).await.len(),
        before,
        "no statement for an empty block"
    );

    // Only the visible column is selected.
    let rows = run(
        deny_country.clone(),
        json!({"@id": "?c", "ex:name": "?n"}),
        json!(["?n"]),
    )
    .await;
    assert_eq!(rows, json!([["Ada"], ["Bo"], ["Cy"]]), "{rows}");
    let sent = block_statements(&server).await;
    assert_eq!(
        sent.last().map(String::as_str),
        Some(
            r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL"#
        )
    );

    // A hidden optional member: the variable stays unbound, no column.
    let rows = run(
        deny_country,
        json!([{"@id": "?c", "ex:name": "?n"}, ["optional", {"@id": "?c", "ex:country": "?k"}]]),
        json!(["?n", "?k"]),
    )
    .await;
    assert_eq!(
        rows,
        json!([["Ada", null], ["Bo", null], ["Cy", null]]),
        "{rows}"
    );

    // Subject targeting is not static: the lane declines, the scan lane hides
    // the one subject.
    let before = block_statements(&server).await.len();
    let before_events = store.find_events("fast-path outcome").len();
    let rows = run(
        json!([{
            "@id": "http://example.org/noAda", "@type": "f:AccessPolicy", "f:action": "f:view",
            "f:allow": false, "f:onSubject": [{"@id": "http://example.org/customer/1"}]
        }]),
        json!({"@id": "?c", "ex:name": "?n"}),
        json!(["?n"]),
    )
    .await;
    assert_eq!(rows, json!([["Bo"], ["Cy"]]), "{rows}");
    assert_eq!(
        block_statements(&server).await.len(),
        before,
        "declined: no block statement"
    );
    let proceeded = proceeded_sites(&store, before_events);
    assert!(!proceeded.iter().any(|s| s == SITE), "{proceeded:?}");
    drop(tracing_guard);
}

/// Bindings the outer query already holds are sent into the statement as a
/// key set, so the source does the semi-join instead of the engine rescanning
/// it per outer row. A ledger pattern joins the same way; its placement
/// relative to the block is the planner's, so the pinned statement uses an
/// outer VALUES seed.
#[tokio::test]
async fn outer_bindings_become_a_key_set() {
    let _lock = KILL_SWITCH.lock().await;
    let (server, fluree) = setup().await;
    let ledger = fluree.create_ledger("crm:main").await.expect("ledger");
    fluree
        .insert_turtle_with_opts(
            ledger,
            "@prefix ex: <http://example.org/> .\n\
             <http://example.org/customer/1> ex:tier \"gold\" .\n\
             <http://example.org/customer/3> ex:tier \"silver\" .\n\
             <http://example.org/customer/9> ex:tier \"none\" .",
            fluree_db_api::TxnOpts::default(),
            fluree_db_api::CommitOpts::default(),
            &fluree_db_api::IndexConfig {
                reindex_min_bytes: 5_000_000_000,
                reindex_max_bytes: 5_000_000_000,
            },
            None,
        )
        .await
        .expect("insert");

    let sparql = "
        PREFIX ex: <http://example.org/>
        SELECT ?c ?n FROM <crm:main> FROM NAMED <shop-sql:main>
        WHERE {
            VALUES ?c { <http://example.org/customer/1> <http://example.org/customer/3> <http://example.org/customer/9> }
            GRAPH <shop-sql:main> { ?c ex:name ?n }
        }
    ";
    let rows = rows_of(&query(&fluree, sparql).await);
    assert_eq!(
        rows,
        vec![
            "c=http://example.org/customer/1 n=Ada",
            "c=http://example.org/customer/3 n=Cy"
        ]
    );
    let sent = block_statements(&server).await;
    assert_eq!(
        sent,
        vec![r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" JOIN (VALUES (1), (3), (9)) AS "k" ("k0") ON "k"."k0" = "t0"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL"#.to_string()]
    );

    // The ledger-driven join returns the same rows whichever side the planner
    // drives from.
    let sparql = "
        PREFIX ex: <http://example.org/>
        SELECT ?tier ?n FROM <crm:main> FROM NAMED <shop-sql:main>
        WHERE { ?c ex:tier ?tier . GRAPH <shop-sql:main> { ?c ex:name ?n } }
    ";
    let rows = rows_of(&query(&fluree, sparql).await);
    assert_eq!(rows, vec!["n=Ada tier=gold", "n=Cy tier=silver"]);
}

const AGG_SITE: &str = "sql_aggregate_pushdown";

fn aggregate_cases() -> Vec<Case> {
    vec![
        Case {
            name: "COUNT(*) over a star is one counting statement",
            sparql: "SELECT (COUNT(*) AS ?n) FROM <shop-sql:main> WHERE { ?o ex:total ?t }",
            sql: &[r#"SELECT COUNT(*) AS "c0" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL"#],
            rows: &["n=4"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "GROUP BY a foreign-key object with COUNT and SUM",
            sparql: "SELECT ?c (COUNT(?o) AS ?n) (SUM(?t) AS ?s) FROM <shop-sql:main> WHERE { ?o ex:customer ?c . ?o ex:total ?t } GROUP BY ?c",
            sql: &[r#"SELECT "t1"."id" AS "c0", COUNT("t0"."id") AS "c1", SUM("t0"."total") AS "c2", COUNT("t0"."total") AS "c3" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t0"."total" IS NOT NULL AND "t1"."id" IS NOT NULL GROUP BY "t1"."id""#],
            rows: &[
                "c=http://example.org/customer/1 n=2 s=104.50",
                "c=http://example.org/customer/2 n=1 s=42.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "AVG pushes SUM and COUNT and divides in the engine",
            sparql: "SELECT ?c (AVG(?t) AS ?a) FROM <shop-sql:main> WHERE { ?o ex:customer ?c . ?o ex:total ?t } GROUP BY ?c",
            sql: &[r#"SELECT "t1"."id" AS "c0", SUM("t0"."total") AS "c1", COUNT("t0"."total") AS "c2" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t0"."total" IS NOT NULL AND "t1"."id" IS NOT NULL GROUP BY "t1"."id""#],
            rows: &[
                "a=42 c=http://example.org/customer/2",
                "a=52.25 c=http://example.org/customer/1",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "MIN and MAX come back as terms of the mapping's datatype",
            sparql: "SELECT (MIN(?t) AS ?lo) (MAX(?p) AS ?last) FROM <shop-sql:main> WHERE { ?o ex:total ?t . ?o ex:placed ?p }",
            sql: &[r#"SELECT MIN("t0"."total") AS "c0", MAX("t0"."placed") AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL AND "t0"."placed" IS NOT NULL"#],
            rows: &["last=2024-03-01 lo=5.00"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "COUNT DISTINCT of a foreign-key object",
            sparql: "SELECT (COUNT(DISTINCT ?c) AS ?n) FROM <shop-sql:main> WHERE { ?o ex:customer ?c }",
            sql: &[r#"SELECT COUNT(DISTINCT "t1"."id") AS "c0" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL"#],
            rows: &["n=2"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "GROUP BY without aggregates is SELECT DISTINCT",
            sparql: "SELECT ?k FROM <shop-sql:main> WHERE { ?c ex:country ?k } GROUP BY ?k",
            sql: &[r#"SELECT DISTINCT "t0"."country" AS "c0" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."country" IS NOT NULL"#],
            rows: &["k=UK", "k=US"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "ORDER BY an aggregate with LIMIT pushes a top-k on the output",
            sparql: "SELECT ?c (COUNT(?o) AS ?n) FROM <shop-sql:main> WHERE { ?o ex:customer ?c } GROUP BY ?c ORDER BY DESC(?n) LIMIT 1",
            sql: &[r#"SELECT "t1"."id" AS "c0", COUNT("t0"."id") AS "c1" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL GROUP BY "t1"."id" ORDER BY "c1" DESC LIMIT 1"#],
            rows: &["c=http://example.org/customer/1 n=2"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "HAVING keeps LIMIT in the engine and filters the groups",
            sparql: "SELECT ?c (COUNT(?o) AS ?n) FROM <shop-sql:main> WHERE { ?o ex:customer ?c } GROUP BY ?c HAVING (COUNT(?o) > 1) LIMIT 5",
            sql: &[r#"SELECT "t1"."id" AS "c0", COUNT("t0"."id") AS "c1" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL GROUP BY "t1"."id""#],
            rows: &["c=http://example.org/customer/1 n=2"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "an optional member counts only bound values",
            sparql: "SELECT (COUNT(?k) AS ?n) (COUNT(*) AS ?all) FROM <shop-sql:main> WHERE { ?c ex:name ?x OPTIONAL { ?c ex:country ?k } }",
            sql: &[r#"SELECT COUNT("t0"."country") AS "c0", COUNT(*) AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
            rows: &["all=3 n=2"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a residual filter under an aggregate declines",
            sparql: "SELECT (COUNT(*) AS ?n) FROM <shop-sql:main> WHERE { ?c ex:name ?x FILTER(STRLEN(?x) > 2) }",
            sql: &[],
            rows: &["n=1"],
            routing: Routing::MustNotFire,
            declined: Some("residual filter under an aggregate"),
        },
    ]
}

/// Grouped queries over a SQL block are one grouped statement; each shape
/// is pinned like the join layer's, and replayed against the per-scan lane.
#[tokio::test]
async fn grouped_queries_send_one_grouped_statement() {
    let _lock = KILL_SWITCH.lock().await;
    let (server, fluree) = setup().await;
    let cases = aggregate_cases();
    let (store, tracing_guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);
    let mut failures: Vec<String> = Vec::new();
    let mut lane_rows: Vec<Vec<String>> = Vec::new();
    for c in &cases {
        let before_events = store.find_events("fast-path outcome").len();
        let before_declines = store.find_events("sql aggregate pushdown declined").len();
        let before_stmts = block_statements(&server).await.len();
        let rows = rows_of(&query(&fluree, &format!("{PREFIX}{}", c.sparql)).await);
        let proceeded = proceeded_sites(&store, before_events);
        let declined: Vec<String> = store.find_events("sql aggregate pushdown declined")
            [before_declines..]
            .iter()
            .filter_map(|e| e.fields.get("why").cloned())
            .collect();
        let sent: Vec<String> = block_statements(&server).await[before_stmts..].to_vec();
        let expected: Vec<String> = c.rows.iter().map(|s| (*s).to_string()).collect();
        if rows != expected {
            failures.push(format!(
                "{}: lane rows {rows:?}, expected {expected:?}",
                c.name
            ));
        }
        match c.routing {
            Routing::MustFire => {
                if !proceeded.iter().any(|s| s == AGG_SITE) {
                    failures.push(format!(
                        "{}: expected `{AGG_SITE}` to proceed [proceeded: {proceeded:?}, declined: {declined:?}]",
                        c.name
                    ));
                }
            }
            Routing::MustNotFire => {
                if proceeded.iter().any(|s| s == AGG_SITE) {
                    failures.push(format!(
                        "{}: `{AGG_SITE}` proceeded on a declined shape",
                        c.name
                    ));
                }
                if let Some(why) = c.declined {
                    if !declined.iter().any(|d| d == why) {
                        failures.push(format!(
                            "{}: expected decline `{why}`, got {declined:?}",
                            c.name
                        ));
                    }
                }
            }
        }
        if !c.sql.is_empty() {
            if sent != c.sql {
                failures.push(format!(
                    "{}: statements sent {sent:#?}\nexpected exactly:\n{}",
                    c.name,
                    c.sql.join("\n")
                ));
            }
        } else if sent
            .iter()
            .any(|s| s.contains("GROUP BY") || s.contains("COUNT("))
        {
            failures.push(format!(
                "{}: unexpected grouped statement {sent:#?}",
                c.name
            ));
        }
        lane_rows.push(rows);
    }
    drop(tracing_guard);

    set_fast_paths_disabled(true);
    for (i, c) in cases.iter().enumerate() {
        let rows = rows_of(&query(&fluree, &format!("{PREFIX}{}", c.sparql)).await);
        if rows != lane_rows[i] {
            failures.push(format!(
                "{}: scan lane rows {rows:?} differ from lane rows {:?}",
                c.name, lane_rows[i]
            ));
        }
    }
    set_fast_paths_disabled(false);
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

const TEXT_AMOUNT_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#Entry>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "shop.entries" ] ;
        rr:subjectMap [ rr:template "http://example.org/entry/{id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:amount ;
            rr:objectMap [ rr:column "amount" ; rr:datatype xsd:decimal ]
        ] .
"#;

/// A numeric datatype over a text column (a SQLite `NUMERIC` the bridge
/// reports as varchar): the lane declines the SUM, and every path below it
/// must still parse the text as the generic pipeline does.
#[tokio::test]
async fn text_typed_numeric_columns_sum_the_same_on_every_path() {
    let _lock = KILL_SWITCH.lock().await;
    let server = FakeSql::new()
        .table(Table::new(
            "shop.entries",
            &[("id", "bigint"), ("amount", "varchar")],
            vec![
                vec![json!(1), json!("99.50")],
                vec![json!(2), json!("5")],
                vec![json!(3), Value::Null],
            ],
        ))
        .mount()
        .await;
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_sql_graph_source(SqlCreateConfig::new(
            "entries-sql",
            server.uri(),
            TEXT_AMOUNT_R2RML,
        ))
        .await
        .expect("create");
    let sparql = "PREFIX ex: <http://example.org/>\nSELECT (SUM(?a) AS ?s) (COUNT(?a) AS ?n) FROM <entries-sql:main> WHERE { ?e ex:amount ?a }";
    set_fast_paths_disabled(true);
    let scan = rows_of(&query(&fluree, sparql).await);
    set_fast_paths_disabled(false);
    let fast = rows_of(&query(&fluree, sparql).await);
    assert_eq!(scan, vec!["n=2 s=104.50"]);
    assert_eq!(fast, scan, "fast paths disagree with the scan lane");
}

/// A tracked query reports every statement the lane sent, in order, so a
/// caller can see what ran remotely without reading the server log.
#[tokio::test]
async fn tracked_queries_report_the_statements_sent() {
    let _lock = KILL_SWITCH.lock().await;
    let (_server, fluree) = setup().await;
    set_fast_paths_disabled(false);
    let response = fluree
        .query_from()
        .sparql(&format!(
            "{PREFIX}SELECT ?o ?n FROM <shop-sql:main> WHERE {{ ?o ex:customer ?c . ?c ex:name ?n }}"
        ))
        .execute_tracked()
        .await
        .unwrap_or_else(|e| panic!("tracked query failed: {}", e.error));
    let sent: Vec<(String, String)> = response
        .sql
        .unwrap_or_default()
        .into_iter()
        .map(|s| (s.source, s.sql))
        .collect();
    assert_eq!(
        sent,
        vec![(
            "shop-sql:main".to_string(),
            r#"SELECT "t0"."id" AS "c0", "t1"."id" AS "c1", "t1"."name" AS "c2" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL AND "t1"."name" IS NOT NULL"#.to_string()
        )]
    );

    // A query the lane never ran reports nothing.
    let response = fluree
        .query_from()
        .sparql(&format!(
            "{PREFIX}SELECT ?p ?v FROM <shop-sql:main> WHERE {{ <http://example.org/customer/1> ?p ?v }}"
        ))
        .execute_tracked()
        .await
        .unwrap_or_else(|e| panic!("tracked query failed: {}", e.error));
    assert!(response.sql.is_none(), "{:?}", response.sql);
}

/// Grouped cases whose SUM/AVG column is `decimal` in the mapping but text
/// or double in SQLite, so the aggregate lane declines there.
const SQLITE_DECLINES: &[&str] = &[
    "GROUP BY a foreign-key object with COUNT and SUM",
    "AVG pushes SUM and COUNT and divides in the engine",
];

/// Every case replayed against a real database: SQLite behind
/// `fluree-sql-bridge`, seeded through the bridge itself. The fake endpoint
/// pins the statements; this pins that a database agrees with the per-scan
/// lane on the rows. Gated on `FLUREE_SQL_BRIDGE_URL` (a bridge over an
/// otherwise unused SQLite file); CI's bridge job sets it.
#[tokio::test]
async fn live_bridge_agrees_with_the_scan_lane() {
    let Ok(url) = std::env::var("FLUREE_SQL_BRIDGE_URL") else {
        eprintln!("FLUREE_SQL_BRIDGE_URL unset: live bridge differential skipped");
        return;
    };
    let _lock = KILL_SWITCH.lock().await;

    let mut cfg = fluree_db_sql::SqlGsConfig::new(url.clone());
    cfg.dialect = fluree_db_api::SqlDialect::Sqlite;
    let cfg = cfg.hydrate(None).await.expect("hydrate");
    let auth = cfg.auth.create_provider_arc().expect("auth");
    let client = fluree_db_sql::TrinoClient::new(&cfg, auth).expect("client");
    for stmt in [
        "DROP TABLE IF EXISTS customers",
        "DROP TABLE IF EXISTS orders",
        "CREATE TABLE customers (id INTEGER, name TEXT, country TEXT)",
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, total NUMERIC, placed DATE, shipped TIMESTAMP, updated TIMESTAMP)",
        "INSERT INTO customers VALUES (1, 'Ada', 'UK'), (2, 'Bo', NULL), (3, 'Cy', 'US')",
        "INSERT INTO orders VALUES \
            (10, 1, 99.50, '2024-01-05', '2024-01-06 09:30:00', '2024-01-06 09:30:00'), \
            (11, 1, 5.00, '2024-02-01', '2024-02-02 18:00:00', '2024-02-02 18:00:00'), \
            (12, 2, 42.00, '2024-03-01', NULL, NULL), \
            (13, NULL, 7.00, NULL, NULL, NULL)",
    ] {
        client
            .execute_collect(stmt)
            .await
            .unwrap_or_else(|e| panic!("seed failed: {e}\n{stmt}"));
    }

    let fluree = FlureeBuilder::memory().build_memory();
    let mut source = SqlCreateConfig::new("shop-live", url, shop_mapping(""));
    source.dialect = fluree_db_api::SqlDialect::Sqlite;
    fluree
        .create_sql_graph_source(source)
        .await
        .expect("create live sql source");

    let mut failures: Vec<String> = Vec::new();
    for c in cases().into_iter().chain(aggregate_cases()) {
        let sparql = format!("{PREFIX}{}", c.sparql).replace("shop-sql:main", "shop-live:main");
        set_fast_paths_disabled(false);
        let tracked = fluree
            .query_from()
            .sparql(&sparql)
            .execute_tracked()
            .await
            .unwrap_or_else(|e| panic!("{}: lane query failed: {}\n{sparql}", c.name, e.error));
        let lane_rows = rows_of(&tracked.result);
        let sent = tracked.sql.unwrap_or_default();
        // SQLite's `NUMERIC` reaches the bridge as text or double, so a
        // SUM/AVG over it declines (its datatype is decimal); the rows must
        // still agree, and the decline itself is pinned.
        let declines_on_sqlite = SQLITE_DECLINES.contains(&c.name);
        match (&c.routing, c.sql.is_empty()) {
            (Routing::MustFire, false) if declines_on_sqlite && !sent.is_empty() => {
                failures.push(format!(
                    "{}: expected a decline on SQLite, sent {sent:?}",
                    c.name
                ));
            }
            (Routing::MustFire, false) if !declines_on_sqlite && sent.is_empty() => {
                failures.push(format!("{}: the lane sent no statement", c.name));
            }
            // A declined grouped shape may still run its block through the
            // join layer under the engine's grouping; only a grouped
            // statement would be wrong.
            (Routing::MustNotFire, _)
                if sent
                    .iter()
                    .any(|s| s.sql.contains("GROUP BY") || s.sql.contains("COUNT(")) =>
            {
                failures.push(format!("{}: a declined shape sent {sent:?}", c.name));
            }
            _ => {}
        }
        set_fast_paths_disabled(true);
        let scan_rows = rows_of(&query(&fluree, &sparql).await);
        if lane_rows != scan_rows {
            failures.push(format!(
                "{}: lane rows {lane_rows:?} differ from scan lane rows {scan_rows:?} [sent: {sent:?}]",
                c.name
            ));
        }
    }
    set_fast_paths_disabled(false);
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

const DUP_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#Tag>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "shop.item_tags" ] ;
        rr:subjectMap [ rr:template "http://example.org/item/{item_id}" ] ;
        rr:predicateObjectMap [ rr:predicate ex:tag ; rr:objectMap [ rr:column "tag" ] ] .
"#;

/// A subject minted from a non-unique column: registration reports it, the
/// lane refuses a statement over the table, and `--allow-duplicate-subjects`
/// lets the query run with the duplicates it implies.
#[tokio::test]
async fn duplicate_subject_keys_are_detected_and_refused() {
    let _lock = KILL_SWITCH.lock().await;
    let server = FakeSql::new()
        .table(Table::new(
            "shop.item_tags",
            &[("item_id", "bigint"), ("tag", "varchar")],
            vec![
                vec![json!(1), json!("red")],
                vec![json!(1), json!("sale")],
                vec![json!(2), json!("blue")],
            ],
        ))
        .mount()
        .await;
    let fluree = FlureeBuilder::memory().build_memory();

    let created = fluree
        .create_sql_graph_source(SqlCreateConfig::new("tags-sql", server.uri(), DUP_R2RML))
        .await
        .expect("create");
    assert!(
        created
            .mapping_warnings
            .iter()
            .any(|w| w.contains("shop.item_tags") && w.contains("not unique")),
        "{:?}",
        created.mapping_warnings
    );
    let probes: Vec<String> = server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .map(|r| String::from_utf8_lossy(&r.body).to_string())
        .filter(|s| s.contains("HAVING"))
        .collect();
    assert_eq!(
        probes,
        vec![r#"SELECT 1 FROM "shop"."item_tags" WHERE "item_id" IS NOT NULL GROUP BY "item_id" HAVING COUNT(*) > 1 LIMIT 1"#.to_string()]
    );

    let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?i ?t FROM <tags-sql:main> WHERE { ?i ex:tag ?t }";
    let err = fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect_err("the lane refuses a flagged table");
    assert!(
        err.to_string().contains("shop.item_tags") && err.to_string().contains("non-unique"),
        "{err}"
    );

    // Re-check reports the same finding and keeps the flag.
    let checked = fluree
        .check_sql_graph_source("tags-sql:main")
        .await
        .expect("check");
    assert_eq!(
        checked.duplicate_subject_tables,
        vec!["shop.item_tags".to_string()]
    );
    assert!(!checked.allow_duplicate_subjects);

    // Accepting duplicates: the same query runs, with one row per table row.
    let mut config = SqlCreateConfig::new("tags-ok", server.uri(), DUP_R2RML);
    config.allow_duplicate_subjects = true;
    fluree
        .create_sql_graph_source(config)
        .await
        .expect("create with duplicates accepted");
    let rows = rows_of(
        &query(
            &fluree,
            "PREFIX ex: <http://example.org/>\nSELECT ?i ?t FROM <tags-ok:main> WHERE { ?i ex:tag ?t }",
        )
        .await,
    );
    assert_eq!(
        rows,
        vec![
            "i=http://example.org/item/1 t=red",
            "i=http://example.org/item/1 t=sale",
            "i=http://example.org/item/2 t=blue",
        ]
    );
}
