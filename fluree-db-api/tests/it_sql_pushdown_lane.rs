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
use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, SqlCreateConfig, SqlDialect};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use wiremock::MockServer;

/// The shop mapping over `shop.customers` / `shop.orders` (the fixture) or,
/// for a live run, unqualified table names.
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

/// The customer entity split across three triples maps sharing its subject
/// template: two over `customers` (vertical partitioning by column) and one
/// over `profiles` (by table). `ex:label` is on both `customers` maps.
fn vp_mapping(prefix: &str) -> String {
    VP_R2RML.replace("shop.", prefix)
}

const VP_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#Customer>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "shop.customers" ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{id}" ; rr:class ex:Customer ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:label ; rr:objectMap [ rr:column "name" ] ] .

    <http://example.org/mapping#CustomerCountry>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "shop.customers" ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{id}" ] ;
        rr:predicateObjectMap [ rr:predicate ex:country ; rr:objectMap [ rr:column "country" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:label ; rr:objectMap [ rr:column "name" ] ] .

    <http://example.org/mapping#CustomerProfile>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "shop.profiles" ] ;
        rr:subjectMap [ rr:template "http://example.org/customer/{id}" ] ;
        rr:predicateObjectMap [ rr:predicate ex:email ; rr:objectMap [ rr:column "email" ] ] .

    <http://example.org/mapping#Order>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "shop.orders" ] ;
        rr:subjectMap [ rr:template "http://example.org/order/{id}" ; rr:class ex:Order ] ;
        rr:predicateObjectMap [
            rr:predicate ex:total ;
            rr:objectMap [ rr:column "total" ; rr:datatype xsd:decimal ]
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
            "shop.profiles",
            &[("id", "bigint"), ("email", "varchar")],
            vec![
                vec![json!(1), json!("ada@example.org")],
                vec![json!(3), json!("cy@example.org")],
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
    fluree
        .create_sql_graph_source(SqlCreateConfig::new(
            "shop-vp",
            server.uri(),
            vp_mapping("shop."),
        ))
        .await
        .expect("create partitioned sql source");
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
            name: "a compound ORDER BY on required columns pushes a multi-key top-k",
            sparql: "SELECT ?o ?p ?t FROM <shop-sql:main> WHERE { ?o ex:total ?t ; ex:placed ?p } ORDER BY DESC(?p) ?t LIMIT 2",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1", "t0"."placed" AS "c2" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL AND "t0"."placed" IS NOT NULL ORDER BY "t0"."placed" DESC, "t0"."total" ASC LIMIT 2"#],
            rows: &[
                "o=http://example.org/order/11 p=2024-02-01 t=5.00",
                "o=http://example.org/order/12 p=2024-03-01 t=42.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a secondary ORDER BY key on the subject keeps LIMIT in the engine",
            sparql: "SELECT ?o ?t FROM <shop-sql:main> WHERE { ?o ex:total ?t } ORDER BY DESC(?t) ?o LIMIT 2",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL"#],
            rows: &[
                "o=http://example.org/order/10 t=99.50",
                "o=http://example.org/order/12 t=42.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            // Ada has two orders: a LIMIT pushed with only the primary key
            // would let the database pick either of them.
            name: "a secondary ORDER BY key the statement cannot order on keeps LIMIT in the engine",
            sparql: "SELECT ?o ?n FROM <shop-sql:main> WHERE { ?o ex:customer ?c . ?c ex:name ?n } ORDER BY ?n DESC(?o) LIMIT 1",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t1"."id" AS "c1", "t1"."name" AS "c2" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL AND "t1"."name" IS NOT NULL"#],
            rows: &["n=Ada o=http://example.org/order/11"],
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
            name: "a BIND over block columns is computed in the engine",
            sparql: "SELECT ?o ?d FROM <shop-sql:main> WHERE { ?o ex:total ?t BIND(?t * 2 AS ?d) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL"#],
            rows: &[
                "d=10.00 o=http://example.org/order/11",
                "d=14.00 o=http://example.org/order/13",
                "d=199.00 o=http://example.org/order/10",
                "d=84.00 o=http://example.org/order/12",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a filter over a BIND variable stays in the engine after the BIND",
            sparql: "SELECT ?o FROM <shop-sql:main> WHERE { ?o ex:total ?t BIND(?t * 2 AS ?d) FILTER(?d > 50) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL"#],
            rows: &["o=http://example.org/order/10", "o=http://example.org/order/12"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "DISTINCT over a BIND keeps the columns the BIND reads",
            sparql: "SELECT DISTINCT ?d FROM <shop-sql:main> WHERE { ?o ex:total ?t BIND(?t * 2 AS ?d) }",
            sql: &[r#"SELECT DISTINCT "t0"."total" AS "c0" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL"#],
            rows: &["d=10.00", "d=14.00", "d=199.00", "d=84.00"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a BIND after a UNION reads the union's variable",
            sparql: "SELECT ?o ?s FROM <shop-sql:main> WHERE { { ?o ex:total ?v } UNION { ?o ex:placed ?v } BIND(STR(?v) AS ?s) }",
            sql: &[
                r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL"#,
                r#"SELECT "t0"."id" AS "c0", "t0"."placed" AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."placed" IS NOT NULL"#,
            ],
            rows: &[
                "o=http://example.org/order/10 s=2024-01-05",
                "o=http://example.org/order/10 s=99.50",
                "o=http://example.org/order/11 s=2024-02-01",
                "o=http://example.org/order/11 s=5.00",
                "o=http://example.org/order/12 s=2024-03-01",
                "o=http://example.org/order/12 s=42.00",
                "o=http://example.org/order/13 s=7.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a BIND reading a variable bound after it is not admitted",
            sparql: "SELECT ?o ?d FROM <shop-sql:main> WHERE { BIND(?t * 2 AS ?d) ?o ex:total ?t }",
            sql: &[],
            // The engine binds ?t before the BIND runs; the lane leaves the
            // shape to it rather than assume that order.
            rows: &[
                "d=10.00 o=http://example.org/order/11",
                "d=14.00 o=http://example.org/order/13",
                "d=199.00 o=http://example.org/order/10",
                "d=84.00 o=http://example.org/order/12",
            ],
            routing: Routing::MustNotFire,
            declined: None,
        },
        Case {
            name: "a BIND variable joined by a triple is not admitted",
            sparql: "SELECT ?o ?d FROM <shop-sql:main> WHERE { ?o ex:total ?t BIND(?t AS ?d) ?x ex:total ?d }",
            sql: &[],
            rows: &[
                "d=42.00 o=http://example.org/order/12",
                "d=5.00 o=http://example.org/order/11",
                "d=7.00 o=http://example.org/order/13",
                "d=99.50 o=http://example.org/order/10",
            ],
            routing: Routing::MustNotFire,
            declined: None,
        },
        Case {
            name: "a BIND inside a UNION branch is not admitted",
            sparql: "SELECT ?o ?s FROM <shop-sql:main> WHERE { ?o ex:total ?v { BIND(STR(?v) AS ?s) } UNION { ?o ex:placed ?p } }",
            sql: &[],
            rows: &[
                "o=http://example.org/order/10 s=",
                "o=http://example.org/order/10 s=99.50",
                "o=http://example.org/order/11 s=",
                "o=http://example.org/order/11 s=5.00",
                "o=http://example.org/order/12 s=",
                "o=http://example.org/order/12 s=42.00",
                "o=http://example.org/order/13 s=7.00",
            ],
            routing: Routing::MustNotFire,
            declined: None,
        },
        Case {
            name: "STRSTARTS pushes a LIKE prefix and keeps the exact filter in the engine",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { ?c ex:name ?n FILTER(STRSTARTS(?n, \"A\")) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL AND "t0"."name" LIKE 'A%' ESCAPE '!'"#],
            rows: &["n=Ada"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "CONTAINS and STRENDS push LIKE patterns",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { ?c ex:name ?n FILTER(CONTAINS(?n, \"d\") || STRENDS(?n, \"o\")) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL AND (("t0"."name" LIKE '%d%' ESCAPE '!') OR ("t0"."name" LIKE '%o' ESCAPE '!'))"#],
            rows: &["n=Ada", "n=Bo"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "LIKE wildcards in the needle are escaped",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { ?c ex:name ?n FILTER(CONTAINS(?n, \"%\") || CONTAINS(?n, \"_\") || CONTAINS(?n, \"!\")) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL AND ((("t0"."name" LIKE '%!%%' ESCAPE '!') OR ("t0"."name" LIKE '%!_%' ESCAPE '!')) OR ("t0"."name" LIKE '%!!%' ESCAPE '!'))"#],
            rows: &[],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "an anchored literal REGEX pushes a LIKE prefix",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { ?c ex:name ?n FILTER(REGEX(?n, \"^B\")) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL AND "t0"."name" LIKE 'B%' ESCAPE '!'"#],
            rows: &["n=Bo"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a REGEX with flags or metacharacters stays a plain residual",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { ?c ex:name ?n FILTER(REGEX(?n, \"^b\", \"i\") || REGEX(?n, \"^C.\")) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL"#],
            rows: &["n=Bo", "n=Cy"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a widened conjunction drops what it cannot widen and keeps the exact part",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { ?c ex:name ?n ; ex:country ?k FILTER(STRSTARTS(?n, \"A\") && STRLEN(?k) = 2 && ?k = \"UK\") }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1", "t0"."country" AS "c2" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL AND "t0"."country" IS NOT NULL AND "t0"."name" LIKE 'A%' ESCAPE '!' AND "t0"."country" = 'UK'"#],
            rows: &["n=Ada"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a widened filter keeps LIMIT in the engine",
            sparql: "SELECT ?n FROM <shop-sql:main> WHERE { ?c ex:name ?n FILTER(STRSTARTS(?n, \"A\")) } LIMIT 1",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL AND "t0"."name" LIKE 'A%' ESCAPE '!'"#],
            rows: &["n=Ada"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a widened filter over an optional variable is a top-level WHERE",
            sparql: "SELECT ?n ?k FROM <shop-sql:main> WHERE { ?c ex:name ?n OPTIONAL { ?c ex:country ?k } FILTER(STRSTARTS(?k, \"U\")) }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1", "t0"."country" AS "c2" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL AND "t0"."country" LIKE 'U%' ESCAPE '!'"#],
            rows: &["k=UK n=Ada", "k=US n=Cy"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "members split across two maps over one table share one access",
            sparql: "SELECT ?n ?k FROM <shop-vp:main> WHERE { ?c ex:name ?n ; ex:country ?k }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1", "t0"."country" AS "c2" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL AND "t0"."country" IS NOT NULL"#],
            rows: &["k=UK n=Ada", "k=US n=Cy"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a class from one map and a member from another share one access",
            sparql: "SELECT ?k FROM <shop-vp:main> WHERE { ?c a ex:Customer ; ex:country ?k }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."country" AS "c1" FROM "shop"."customers" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."country" IS NOT NULL"#],
            rows: &["k=UK", "k=US"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "members split across two tables join on the subject's key",
            sparql: "SELECT ?n ?e FROM <shop-vp:main> WHERE { ?c ex:name ?n ; ex:email ?e }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1", "t1"."email" AS "c2" FROM "shop"."customers" AS "t0" JOIN "shop"."profiles" AS "t1" ON "t0"."id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL AND "t1"."id" IS NOT NULL AND "t1"."email" IS NOT NULL"#],
            rows: &["e=ada@example.org n=Ada", "e=cy@example.org n=Cy"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a foreign key joins a partitioned entity on its shared subject",
            sparql: "SELECT ?o ?k FROM <shop-vp:main> WHERE { ?o ex:customer ?c . ?c ex:country ?k }",
            sql: &[r#"SELECT "t0"."id" AS "c0", "t1"."id" AS "c1", "t1"."country" AS "c2" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL AND "t1"."country" IS NOT NULL"#],
            rows: &["k=UK o=http://example.org/order/10", "k=UK o=http://example.org/order/11"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a predicate two maps provide, with no map providing the entity, declines",
            sparql: "SELECT ?l ?e FROM <shop-vp:main> WHERE { ?c ex:label ?l ; ex:email ?e }",
            sql: &[],
            // The per-scan lane answers once per map minting the triple.
            rows: &[
                "e=ada@example.org l=Ada",
                "e=ada@example.org l=Ada",
                "e=cy@example.org l=Cy",
                "e=cy@example.org l=Cy",
            ],
            routing: Routing::MustNotFire,
            declined: Some("predicate provided by several triples maps"),
        },
        Case {
            name: "UNION branch combinations above the cap decline",
            sparql: "SELECT ?a ?b ?c ?d FROM <shop-sql:main> WHERE { { <http://example.org/order/13> ex:total ?a } UNION { <http://example.org/order/13> ex:placed ?a } { <http://example.org/order/13> ex:total ?b } UNION { <http://example.org/order/13> ex:placed ?b } { <http://example.org/order/13> ex:total ?c } UNION { <http://example.org/order/13> ex:placed ?c } { <http://example.org/order/13> ex:total ?d } UNION { <http://example.org/order/13> ex:placed ?d } }",
            sql: &[],
            rows: &["a=7.00 b=7.00 c=7.00 d=7.00"],
            routing: Routing::MustNotFire,
            declined: Some("too many UNION branch combinations"),
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

/// Outer bindings above the provider's key-set row cap go out as several
/// statements; a `VALUES` or `IN` list inside the block above the cap is
/// not pushed at all (the block still runs on the lane, the list in the
/// engine), since the block's own key set is not chunked.
#[tokio::test]
async fn key_sets_above_the_cap_chunk_or_stay_in_the_engine() {
    let _lock = KILL_SWITCH.lock().await;
    let (server, fluree) = setup().await;
    // 2001 distinct keys: 1..=3 exist, the rest match nothing.
    let iris: Vec<String> = (1..=2001)
        .map(|i| format!("<http://example.org/customer/{i}>"))
        .collect();
    let values = iris.join(" ");

    let sparql = format!(
        "{PREFIX}SELECT ?c ?n FROM NAMED <shop-sql:main> WHERE {{ VALUES ?c {{ {values} }} GRAPH <shop-sql:main> {{ ?c ex:name ?n }} }}"
    );
    let before = block_statements(&server).await.len();
    let rows = rows_of(&query(&fluree, &sparql).await);
    assert_eq!(
        rows,
        vec![
            "c=http://example.org/customer/1 n=Ada",
            "c=http://example.org/customer/2 n=Bo",
            "c=http://example.org/customer/3 n=Cy",
        ]
    );
    let sent = block_statements(&server).await[before..].to_vec();
    assert_eq!(sent.len(), 2, "2001 keys chunk into 2000 + 1: {sent:?}");
    assert!(sent[0].contains("(2000)") && !sent[0].contains("(2001)"));
    assert!(sent[1].contains("(VALUES (2001)) AS \"k\""), "{}", sent[1]);
    set_fast_paths_disabled(true);
    let scan = rows_of(&query(&fluree, &sparql).await);
    set_fast_paths_disabled(false);
    assert_eq!(scan, rows, "scan lane disagrees");

    let sparql = format!(
        "{PREFIX}SELECT ?c ?n FROM <shop-sql:main> WHERE {{ ?c ex:name ?n VALUES ?c {{ {values} }} }}"
    );
    let before = block_statements(&server).await.len();
    let rows = rows_of(&query(&fluree, &sparql).await);
    assert_eq!(rows.len(), 3);
    let sent = block_statements(&server).await[before..].to_vec();
    assert!(
        sent.is_empty(),
        "an oversized VALUES in the block declines the lane: {sent:?}"
    );

    let names: Vec<String> = (1..=2001).map(|i| format!("\"n{i}\"")).collect();
    let sparql = format!(
        "{PREFIX}SELECT ?n FROM <shop-sql:main> WHERE {{ ?c ex:name ?n FILTER(?n IN (\"Ada\", {})) }}",
        names.join(", ")
    );
    let before = block_statements(&server).await.len();
    let rows = rows_of(&query(&fluree, &sparql).await);
    assert_eq!(rows, vec!["n=Ada"]);
    let sent = block_statements(&server).await[before..].to_vec();
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert!(
        !sent[0].contains(" IN ("),
        "an oversized IN list stays a residual: {}",
        sent[0]
    );
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
            name: "MIN and MAX of the same variable are two outputs of one column",
            sparql: "SELECT (MIN(?t) AS ?lo) (MAX(?t) AS ?hi) FROM <shop-sql:main> WHERE { ?o ex:total ?t }",
            sql: &[r#"SELECT MIN("t0"."total") AS "c0", MAX("t0"."total") AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL"#],
            rows: &["hi=99.50 lo=5.00"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "MIN and MAX of the same variable per group",
            sparql: "SELECT ?c (MIN(?t) AS ?lo) (MAX(?t) AS ?hi) FROM <shop-sql:main> WHERE { ?o ex:customer ?c . ?o ex:total ?t } GROUP BY ?c",
            sql: &[r#"SELECT "t1"."id" AS "c0", MIN("t0"."total") AS "c1", MAX("t0"."total") AS "c2" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t0"."total" IS NOT NULL AND "t1"."id" IS NOT NULL GROUP BY "t1"."id""#],
            rows: &[
                "c=http://example.org/customer/1 hi=99.50 lo=5.00",
                "c=http://example.org/customer/2 hi=42.00 lo=42.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a BIND the aggregates do not read leaves the grouped statement alone",
            sparql: "SELECT ?c (SUM(?t) AS ?s) FROM <shop-sql:main> WHERE { ?o ex:customer ?c . ?o ex:total ?t BIND(?t * 2 AS ?d) } GROUP BY ?c",
            sql: &[r#"SELECT "t1"."id" AS "c0", SUM("t0"."total") AS "c1", COUNT("t0"."total") AS "c2" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t0"."total" IS NOT NULL AND "t1"."id" IS NOT NULL GROUP BY "t1"."id""#],
            rows: &[
                "c=http://example.org/customer/1 s=104.50",
                "c=http://example.org/customer/2 s=42.00",
            ],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "an aggregate over a BIND variable declines",
            sparql: "SELECT ?c (SUM(?d) AS ?s) FROM <shop-sql:main> WHERE { ?o ex:customer ?c . ?o ex:total ?t BIND(?t * 2 AS ?d) } GROUP BY ?c",
            sql: &[],
            rows: &[
                "c=http://example.org/customer/1 s=209.00",
                "c=http://example.org/customer/2 s=84.00",
            ],
            routing: Routing::MustNotFire,
            declined: Some("aggregate over a variable without columns"),
        },
        Case {
            name: "a widened filter under an aggregate declines",
            sparql: "SELECT (COUNT(*) AS ?n) FROM <shop-sql:main> WHERE { ?c ex:name ?nm FILTER(STRSTARTS(?nm, \"A\")) }",
            sql: &[],
            rows: &["n=1"],
            routing: Routing::MustNotFire,
            declined: Some("residual filter under an aggregate"),
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
            // Every total is its own group, so the counts all tie.
            name: "a compound ORDER BY over an aggregate and a group key pushes a multi-key top-k",
            sparql: "SELECT ?t (COUNT(?o) AS ?n) FROM <shop-sql:main> WHERE { ?o ex:total ?t } GROUP BY ?t ORDER BY ?n ?t LIMIT 1",
            sql: &[r#"SELECT "t0"."total" AS "c0", COUNT("t0"."id") AS "c1" FROM "shop"."orders" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL GROUP BY "t0"."total" ORDER BY "c1" ASC, "t0"."total" ASC LIMIT 1"#],
            rows: &["n=1 t=5.00"],
            routing: Routing::MustFire,
            declined: None,
        },
        Case {
            name: "a secondary ORDER BY key over a template subject keeps LIMIT in the engine",
            sparql: "SELECT ?c (COUNT(?o) AS ?n) FROM <shop-sql:main> WHERE { ?o ex:customer ?c } GROUP BY ?c ORDER BY DESC(?n) ?c LIMIT 1",
            sql: &[r#"SELECT "t1"."id" AS "c0", COUNT("t0"."id") AS "c1" FROM "shop"."orders" AS "t0" JOIN "shop"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id" WHERE "t0"."id" IS NOT NULL AND "t0"."customer_id" IS NOT NULL AND "t1"."id" IS NOT NULL GROUP BY "t1"."id""#],
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

// ---------------------------------------------------------------------------
// Live differential: every case replayed against real databases
// ---------------------------------------------------------------------------
//
// The fake endpoint pins the statements; only a database can confirm those
// statements return the rows the per-scan lane returns *and* the rows the
// fake pinned. Each backend below runs behind `fluree-sql-bridge`, seeded
// through the bridge itself. The base tables carry exactly the fake's rows,
// so the fake's expected rows hold here too; `words`, `tags` and `events`
// carry the values on which dialects disagree (collation, zones) for the
// dialect cases in `live_cases`. Gated on the backend's URL variable; CI's
// bridge job sets all three.

/// A database behind the bridge, with the fixture in its own types.
struct LiveBackend {
    dialect: SqlDialect,
    /// Environment variable carrying the bridge URL.
    url_var: &'static str,
    seed: &'static [&'static str],
    /// Grouped cases the aggregate lane must decline here because a fixture
    /// column reaches the bridge as a type it cannot fold exactly.
    declines: &'static [&'static str],
}

const CUSTOMER_ROWS: &str =
    "INSERT INTO customers VALUES (1, 'Ada', 'UK'), (2, 'Bo', NULL), (3, 'Cy', 'US')";
/// `words` holds strings a case-folding collation equates (`Ada`/`ada`), a
/// padding one equates (`Bo`/`Bo `), and one a locale sorts among the ASCII
/// letters (`Émile`, which code-point order puts last).
const PROFILE_ROWS: &str =
    "INSERT INTO profiles VALUES (1, 'ada@example.org'), (3, 'cy@example.org')";
const WORD_ROWS: &str = "INSERT INTO words VALUES (1, 'Ada', 3), (2, 'ada', 3), (3, 'Bo', 2), (4, 'Bo ', 3), (5, 'Émile', 5)";
/// `tags` is keyed by a string column, so its subjects are minted from
/// values a collation would merge.
const TAG_ROWS: &str = "INSERT INTO tags VALUES ('Ada', 1), ('ada', 2), ('Bo ', 3)";

const SQLITE: LiveBackend = LiveBackend {
    dialect: SqlDialect::Sqlite,
    url_var: "FLUREE_SQL_BRIDGE_URL",
    seed: &[
        "DROP TABLE IF EXISTS customers",
        "DROP TABLE IF EXISTS orders",
        "DROP TABLE IF EXISTS words",
        "DROP TABLE IF EXISTS tags",
        "DROP TABLE IF EXISTS events",
        "DROP TABLE IF EXISTS profiles",
        "CREATE TABLE customers (id INTEGER, name TEXT, country TEXT)",
        "CREATE TABLE profiles (id INTEGER, email TEXT)",
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, total NUMERIC, placed DATE, shipped TIMESTAMP, updated TIMESTAMP)",
        "CREATE TABLE words (id INTEGER, word TEXT, len INTEGER)",
        "CREATE TABLE tags (tag TEXT, n INTEGER)",
        "CREATE TABLE events (id INTEGER, at_tz TIMESTAMP, at_local TIMESTAMP)",
        CUSTOMER_ROWS,
        PROFILE_ROWS,
        "INSERT INTO orders VALUES \
            (10, 1, 99.50, '2024-01-05', '2024-01-06 09:30:00', '2024-01-06 09:30:00'), \
            (11, 1, 5.00, '2024-02-01', '2024-02-02 18:00:00', '2024-02-02 18:00:00'), \
            (12, 2, 42.00, '2024-03-01', NULL, NULL), \
            (13, NULL, 7.00, NULL, NULL, NULL)",
        WORD_ROWS,
        TAG_ROWS,
        "INSERT INTO events VALUES (1, '2024-01-10 03:00:00', '2024-01-10 03:00:00'), (2, '2024-01-09 21:00:00', '2024-01-09 21:00:00')",
    ],
    // SQLite's `NUMERIC` reaches the bridge as text or double, so a SUM/AVG
    // over it declines (its datatype is decimal).
    declines: &[
        "GROUP BY a foreign-key object with COUNT and SUM",
        "AVG pushes SUM and COUNT and divides in the engine",
    ],
};

/// Zoned values are inserted with an explicit offset so the server's own
/// zone (deliberately not UTC in CI) cannot leak into the fixture.
const POSTGRES: LiveBackend = LiveBackend {
    dialect: SqlDialect::Postgres,
    url_var: "FLUREE_SQL_BRIDGE_POSTGRES_URL",
    seed: &[
        "DROP TABLE IF EXISTS customers",
        "DROP TABLE IF EXISTS orders",
        "DROP TABLE IF EXISTS words",
        "DROP TABLE IF EXISTS tags",
        "DROP TABLE IF EXISTS events",
        "DROP TABLE IF EXISTS profiles",
        "CREATE TABLE customers (id BIGINT, name TEXT, country TEXT)",
        "CREATE TABLE profiles (id BIGINT, email TEXT)",
        "CREATE TABLE orders (id BIGINT, customer_id BIGINT, total NUMERIC(10,2), placed DATE, shipped TIMESTAMPTZ, updated TIMESTAMP)",
        "CREATE TABLE words (id BIGINT, word TEXT, len INTEGER)",
        "CREATE TABLE tags (tag TEXT, n INTEGER)",
        "CREATE TABLE events (id BIGINT, at_tz TIMESTAMPTZ, at_local TIMESTAMP)",
        CUSTOMER_ROWS,
        PROFILE_ROWS,
        "INSERT INTO orders VALUES \
            (10, 1, 99.50, '2024-01-05', '2024-01-06 09:30:00+00:00', '2024-01-06 09:30:00'), \
            (11, 1, 5.00, '2024-02-01', '2024-02-02 18:00:00+00:00', '2024-02-02 18:00:00'), \
            (12, 2, 42.00, '2024-03-01', NULL, NULL), \
            (13, NULL, 7.00, NULL, NULL, NULL)",
        WORD_ROWS,
        TAG_ROWS,
        "INSERT INTO events VALUES (1, '2024-01-10 03:00:00+00:00', '2024-01-10 03:00:00'), (2, '2024-01-09 21:00:00+00:00', '2024-01-09 21:00:00')",
    ],
    declines: &[],
};

const MYSQL: LiveBackend = LiveBackend {
    dialect: SqlDialect::Mysql,
    url_var: "FLUREE_SQL_BRIDGE_MYSQL_URL",
    seed: &[
        "DROP TABLE IF EXISTS customers",
        "DROP TABLE IF EXISTS orders",
        "DROP TABLE IF EXISTS words",
        "DROP TABLE IF EXISTS tags",
        "DROP TABLE IF EXISTS events",
        "DROP TABLE IF EXISTS profiles",
        "CREATE TABLE customers (id BIGINT, name VARCHAR(64), country VARCHAR(64))",
        "CREATE TABLE profiles (id BIGINT, email VARCHAR(64))",
        "CREATE TABLE orders (id BIGINT, customer_id BIGINT, total DECIMAL(10,2), placed DATE, shipped TIMESTAMP NULL, updated DATETIME)",
        "CREATE TABLE words (id BIGINT, word VARCHAR(64), len INT)",
        "CREATE TABLE tags (tag VARCHAR(64), n INT)",
        "CREATE TABLE events (id BIGINT, at_tz TIMESTAMP NULL, at_local DATETIME)",
        CUSTOMER_ROWS,
        PROFILE_ROWS,
        "INSERT INTO orders VALUES \
            (10, 1, 99.50, '2024-01-05', '2024-01-06 09:30:00+00:00', '2024-01-06 09:30:00'), \
            (11, 1, 5.00, '2024-02-01', '2024-02-02 18:00:00+00:00', '2024-02-02 18:00:00'), \
            (12, 2, 42.00, '2024-03-01', NULL, NULL), \
            (13, NULL, 7.00, NULL, NULL, NULL)",
        WORD_ROWS,
        TAG_ROWS,
        "INSERT INTO events VALUES (1, '2024-01-10 03:00:00+00:00', '2024-01-10 03:00:00'), (2, '2024-01-09 21:00:00+00:00', '2024-01-09 21:00:00')",
    ],
    declines: &[],
};

/// Maps over the dialect tables, appended to the shop mapping for live runs.
const LIVE_R2RML: &str = r#"
    <http://example.org/mapping#Word>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "words" ] ;
        rr:subjectMap [ rr:template "http://example.org/word/{id}" ; rr:class ex:Word ] ;
        rr:predicateObjectMap [ rr:predicate ex:word ; rr:objectMap [ rr:column "word" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:len ; rr:objectMap [ rr:column "len" ] ] .

    <http://example.org/mapping#Tag>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "tags" ] ;
        rr:subjectMap [ rr:template "http://example.org/tag/{tag}" ; rr:class ex:Tag ] ;
        rr:predicateObjectMap [ rr:predicate ex:n ; rr:objectMap [ rr:column "n" ] ] ;
        rr:predicateObjectMap [
            rr:predicate ex:wordOf ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Word> ;
                rr:joinCondition [ rr:child "tag" ; rr:parent "word" ]
            ]
        ] .

    <http://example.org/mapping#Event>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "events" ] ;
        rr:subjectMap [ rr:template "http://example.org/event/{id}" ; rr:class ex:Event ] ;
        rr:predicateObjectMap [
            rr:predicate ex:at ;
            rr:objectMap [ rr:column "at_tz" ; rr:datatype xsd:dateTime ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:atLocal ;
            rr:objectMap [ rr:column "at_local" ; rr:datatype xsd:dateTime ]
        ] .
"#;

/// What the statements the lane sent must look like on one backend.
enum Sent {
    /// Some statement carries this fragment.
    Contains(&'static str),
    /// No statement carries this fragment (the shape ran in the engine).
    Lacks(&'static str),
    /// The lane sent nothing: the whole query ran through the per-scan lane.
    Nothing,
}

/// A case whose answer depends on the database's string, time or numeric
/// semantics. The lane must fire on every backend unless `sent` says
/// `Nothing`; `sent` pins *how* per backend, and `rows` pins the SPARQL
/// answer (bytes, code points, instants, lexical forms) on all of them.
struct LiveCase {
    name: &'static str,
    sparql: &'static str,
    rows: &'static [&'static str],
    /// Backends the case runs on; empty = all.
    only: &'static [SqlDialect],
    sent: &'static [(SqlDialect, Sent)],
}

fn live_cases() -> Vec<LiveCase> {
    use SqlDialect::{Mysql, Postgres, Sqlite};
    vec![
        LiveCase {
            name: "string equality compares bytes",
            sparql: "SELECT ?w FROM <shop-live:main> WHERE { ?x ex:word ?w FILTER(?w = \"ada\") }",
            rows: &["w=ada"],
            only: &[],
            sent: &[(Mysql, Sent::Contains("= BINARY 'ada'"))],
        },
        LiveCase {
            name: "string equality does not pad",
            sparql: "SELECT ?w FROM <shop-live:main> WHERE { ?x ex:word ?w FILTER(?w = \"Bo\") }",
            rows: &["w=Bo"],
            only: &[],
            sent: &[(Mysql, Sent::Contains("= BINARY 'Bo'"))],
        },
        LiveCase {
            name: "IN list compares bytes",
            sparql: "SELECT ?w FROM <shop-live:main> WHERE { ?x ex:word ?w FILTER(?w IN (\"ada\", \"Bo\")) }",
            rows: &["w=Bo", "w=ada"],
            only: &[],
            sent: &[(Mysql, Sent::Contains("IN (BINARY 'ada', BINARY 'Bo')"))],
        },
        LiveCase {
            name: "a constant subject on a string key compares bytes",
            sparql: "SELECT ?n FROM <shop-live:main> WHERE { <http://example.org/tag/ada> ex:n ?n }",
            rows: &["n=2"],
            only: &[],
            sent: &[(Mysql, Sent::Contains("= BINARY 'ada'"))],
        },
        LiveCase {
            name: "a key set on a string key compares bytes",
            sparql: "SELECT ?t ?n FROM <shop-live:main> WHERE { VALUES ?t { <http://example.org/tag/ada> <http://example.org/tag/Bo%20> } ?t ex:n ?n }",
            rows: &[
                "n=2 t=http://example.org/tag/ada",
                "n=3 t=http://example.org/tag/Bo%20",
            ],
            only: &[],
            sent: &[(Mysql, Sent::Contains("BINARY 'ada'"))],
        },
        LiveCase {
            name: "a string join compares bytes",
            sparql: "SELECT ?t ?l FROM <shop-live:main> WHERE { ?t ex:wordOf ?w . ?w ex:len ?l }",
            rows: &[
                "l=3 t=http://example.org/tag/Ada",
                "l=3 t=http://example.org/tag/Bo%20",
                "l=3 t=http://example.org/tag/ada",
            ],
            only: &[],
            sent: &[(Mysql, Sent::Contains("= BINARY `t"))],
        },
        LiveCase {
            name: "DISTINCT over strings runs in the engine where the database folds case",
            sparql: "SELECT DISTINCT ?w FROM <shop-live:main> WHERE { ?x ex:word ?w }",
            rows: &["w=Ada", "w=Bo", "w=Bo ", "w=ada", "w=Émile"],
            only: &[],
            sent: &[
                (Sqlite, Sent::Contains("SELECT DISTINCT")),
                (Postgres, Sent::Contains("SELECT DISTINCT")),
                (Mysql, Sent::Lacks("DISTINCT")),
            ],
        },
        LiveCase {
            name: "GROUP BY a string runs in the engine where the database folds case",
            sparql: "SELECT ?w (COUNT(*) AS ?c) FROM <shop-live:main> WHERE { ?x ex:word ?w } GROUP BY ?w",
            rows: &[
                "c=1 w=Ada",
                "c=1 w=Bo",
                "c=1 w=Bo ",
                "c=1 w=ada",
                "c=1 w=Émile",
            ],
            only: &[],
            sent: &[
                (Sqlite, Sent::Contains("GROUP BY")),
                (Postgres, Sent::Contains("GROUP BY")),
                (Mysql, Sent::Nothing),
            ],
        },
        LiveCase {
            name: "COUNT DISTINCT of strings runs in the engine where the database folds case",
            sparql: "SELECT (COUNT(DISTINCT ?w) AS ?c) FROM <shop-live:main> WHERE { ?x ex:word ?w }",
            rows: &["c=5"],
            only: &[],
            sent: &[
                (Sqlite, Sent::Contains("COUNT(DISTINCT")),
                (Postgres, Sent::Contains("COUNT(DISTINCT")),
                (Mysql, Sent::Lacks("COUNT(DISTINCT")),
            ],
        },
        LiveCase {
            name: "string ORDER BY LIMIT pushes a top-k only under code-point order",
            sparql: "SELECT ?w FROM <shop-live:main> WHERE { ?x ex:word ?w } ORDER BY ?w LIMIT 2",
            rows: &["w=Ada", "w=Bo"],
            only: &[],
            sent: &[
                (Sqlite, Sent::Contains("ORDER BY")),
                (Postgres, Sent::Lacks("ORDER BY")),
                (Mysql, Sent::Lacks("ORDER BY")),
            ],
        },
        LiveCase {
            name: "string ORDER BY DESC LIMIT pushes a top-k only under code-point order",
            sparql: "SELECT ?w FROM <shop-live:main> WHERE { ?x ex:word ?w } ORDER BY DESC(?w) LIMIT 2",
            rows: &["w=ada", "w=Émile"],
            only: &[],
            sent: &[
                (Sqlite, Sent::Contains("ORDER BY")),
                (Postgres, Sent::Lacks("ORDER BY")),
                (Mysql, Sent::Lacks("ORDER BY")),
            ],
        },
        LiveCase {
            name: "MIN of strings runs in the engine where order is a collation",
            sparql: "SELECT (MIN(?w) AS ?lo) FROM <shop-live:main> WHERE { ?x ex:word ?w }",
            rows: &["lo=Ada"],
            only: &[],
            sent: &[
                (Sqlite, Sent::Contains("MIN(")),
                (Postgres, Sent::Lacks("MIN(")),
                (Mysql, Sent::Lacks("MIN(")),
            ],
        },
        LiveCase {
            name: "MAX of strings runs in the engine where order is a collation",
            sparql: "SELECT (MAX(?w) AS ?hi) FROM <shop-live:main> WHERE { ?x ex:word ?w }",
            rows: &["hi=Émile"],
            only: &[],
            sent: &[
                (Sqlite, Sent::Contains("MAX(")),
                (Postgres, Sent::Lacks("MAX(")),
                (Mysql, Sent::Lacks("MAX(")),
            ],
        },
        // The servers run in a zone five hours behind UTC in CI. A literal
        // that lost its zone would move by that much, which is why the
        // fixture's instants sit within five hours of the boundary.
        LiveCase {
            name: "a zoned filter compares the instant whatever the server's zone",
            sparql: "SELECT ?e FROM <shop-live:main> WHERE { ?e ex:at ?t FILTER(?t > \"2024-01-10T00:00:00Z\"^^xsd:dateTime) }",
            rows: &["e=http://example.org/event/1"],
            only: &[Postgres, Mysql],
            sent: &[
                (
                    Postgres,
                    Sent::Contains("> TIMESTAMP WITH TIME ZONE '2024-01-10 00:00:00.000000 UTC'"),
                ),
                (Mysql, Sent::Contains("> TIMESTAMP '2024-01-10 00:00:00.000000+00:00'")),
            ],
        },
        LiveCase {
            name: "a zoned value reads back as the instant stored",
            sparql: "SELECT ?t FROM <shop-live:main> WHERE { <http://example.org/event/1> ex:at ?t }",
            rows: &["t=2024-01-10T03:00:00Z"],
            only: &[Postgres, Mysql],
            sent: &[],
        },
        // The engine's xsd:dateTime carries no offset, so a naive timestamp
        // is taken as UTC and renders with `Z`, on every backend.
        LiveCase {
            name: "a naive value reads back as UTC",
            sparql: "SELECT ?t FROM <shop-live:main> WHERE { <http://example.org/event/1> ex:atLocal ?t }",
            rows: &["t=2024-01-10T03:00:00Z"],
            only: &[],
            sent: &[],
        },
        // A decimal's lexical form follows the scale the endpoint reports for
        // the column: the bridge reports NUMERIC/DECIMAL as `decimal(38, 6)`
        // unless started with `--decimal-scale`, and SQLite's NUMERIC is a
        // double. Same value everywhere; pinned so a change is deliberate.
        LiveCase {
            name: "a decimal value reads back at the reported scale",
            sparql: "SELECT ?t FROM <shop-live:main> WHERE { <http://example.org/order/10> ex:total ?t }",
            rows: &["t=99.500000"],
            only: &[Postgres, Mysql],
            sent: &[],
        },
        LiveCase {
            name: "a decimal value reads back as a double on SQLite",
            sparql: "SELECT ?t FROM <shop-live:main> WHERE { <http://example.org/order/10> ex:total ?t }",
            rows: &["t=99.5"],
            only: &[Sqlite],
            sent: &[],
        },
        LiveCase {
            name: "SUM and AVG of a decimal column fold in the database",
            sparql: "SELECT (SUM(?t) AS ?s) (AVG(?t) AS ?a) FROM <shop-live:main> WHERE { ?o ex:total ?t }",
            rows: &["a=38.375 s=153.500000"],
            only: &[Postgres, Mysql],
            sent: &[(Postgres, Sent::Contains("SUM(")), (Mysql, Sent::Contains("SUM("))],
        },
        LiveCase {
            name: "a BIND and a filter over it run in the engine",
            sparql: "SELECT ?o ?d FROM <shop-live:main> WHERE { ?o ex:total ?t BIND(?t * 2 AS ?d) FILTER(?d > 50) }",
            rows: &[
                "d=199.000000 o=http://example.org/order/10",
                "d=84.000000 o=http://example.org/order/12",
            ],
            only: &[Postgres, Mysql],
            sent: &[(Postgres, Sent::Lacks("* 2")), (Mysql, Sent::Lacks("* 2"))],
        },
        LiveCase {
            name: "a BIND and a filter over it run in the engine on SQLite",
            sparql: "SELECT ?o ?d FROM <shop-live:main> WHERE { ?o ex:total ?t BIND(?t * 2 AS ?d) FILTER(?d > 50) }",
            rows: &["d=199.0 o=http://example.org/order/10", "d=84 o=http://example.org/order/12"],
            only: &[Sqlite],
            sent: &[(Sqlite, Sent::Lacks("* 2"))],
        },
        LiveCase {
            name: "STRSTARTS widens to a LIKE the engine narrows back",
            sparql: "SELECT ?w FROM <shop-live:main> WHERE { ?x ex:word ?w FILTER(STRSTARTS(?w, \"A\")) }",
            rows: &["w=Ada"],
            only: &[],
            sent: &[
                (Sqlite, Sent::Contains("LIKE 'A%' ESCAPE '!'")),
                (Postgres, Sent::Contains("LIKE 'A%' ESCAPE '!'")),
                (Mysql, Sent::Contains("LIKE 'A%' ESCAPE '!'")),
            ],
        },
        LiveCase {
            name: "CONTAINS widens to a LIKE on a non-ASCII word",
            sparql: "SELECT ?w FROM <shop-live:main> WHERE { ?x ex:word ?w FILTER(CONTAINS(?w, \"mile\")) }",
            rows: &["w=Émile"],
            only: &[],
            sent: &[
                (Sqlite, Sent::Contains("LIKE '%mile%' ESCAPE '!'")),
                (Postgres, Sent::Contains("LIKE '%mile%' ESCAPE '!'")),
                (Mysql, Sent::Contains("LIKE '%mile%' ESCAPE '!'")),
            ],
        },
        LiveCase {
            name: "MIN and MAX of one decimal column fold in the database",
            sparql: "SELECT (MIN(?t) AS ?lo) (MAX(?t) AS ?hi) FROM <shop-live:main> WHERE { ?o ex:total ?t }",
            rows: &["hi=99.500000 lo=5.000000"],
            only: &[Postgres, Mysql],
            sent: &[
                (Postgres, Sent::Contains("MIN(")),
                (Postgres, Sent::Contains("MAX(")),
                (Mysql, Sent::Contains("MIN(")),
                (Mysql, Sent::Contains("MAX(")),
            ],
        },
        LiveCase {
            name: "MIN and MAX of one decimal column fold in the database on SQLite",
            sparql: "SELECT (MIN(?t) AS ?lo) (MAX(?t) AS ?hi) FROM <shop-live:main> WHERE { ?o ex:total ?t }",
            rows: &["hi=99.5 lo=5"],
            only: &[Sqlite],
            sent: &[(Sqlite, Sent::Contains("MIN(")), (Sqlite, Sent::Contains("MAX("))],
        },
    ]
}

/// Rows with every plain decimal lexical reduced to its shortest form, so a
/// fixture pinned against the fake's `decimal(10,2)` (`99.50`) compares by
/// value with a backend that reports the column at another scale (`99.5` as
/// a SQLite double, `99.500000` through the bridge's default scale).
fn by_value(rows: &[String]) -> Vec<String> {
    fn canon(v: &str) -> String {
        let plain = v.strip_prefix('-').unwrap_or(v);
        let (int, frac) = match plain.split_once('.') {
            Some(parts) => parts,
            None => return v.to_string(),
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
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            row.split(' ')
                .map(|tok| match tok.split_once('=') {
                    Some((var, val)) => format!("{var}={}", canon(val)),
                    None => tok.to_string(),
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    out.sort();
    out
}

/// The lane's rows and the statements it sent for `sparql`.
async fn lane_run(fluree: &Fluree, sparql: &str, name: &str) -> (Vec<String>, Vec<String>) {
    set_fast_paths_disabled(false);
    let tracked = fluree
        .query_from()
        .sparql(sparql)
        .execute_tracked()
        .await
        .unwrap_or_else(|e| panic!("{name}: lane query failed: {}\n{sparql}", e.error));
    let sent = tracked
        .sql
        .unwrap_or_default()
        .into_iter()
        .map(|s| s.sql)
        .collect();
    (rows_of(&tracked.result), sent)
}

/// The per-scan lane's rows for `sparql`.
async fn scan_run(fluree: &Fluree, sparql: &str) -> Vec<String> {
    set_fast_paths_disabled(true);
    let rows = rows_of(&query(fluree, sparql).await);
    set_fast_paths_disabled(false);
    rows
}

async fn live_differential(backend: &LiveBackend) {
    let Ok(url) = std::env::var(backend.url_var) else {
        eprintln!(
            "{} unset: live {:?} differential skipped",
            backend.url_var, backend.dialect
        );
        return;
    };
    let _lock = KILL_SWITCH.lock().await;

    let mut cfg = fluree_db_sql::SqlGsConfig::new(url.clone());
    cfg.dialect = backend.dialect;
    let cfg = cfg.hydrate(None).await.expect("hydrate");
    let auth = cfg.auth.create_provider_arc().expect("auth");
    let client = fluree_db_sql::TrinoClient::new(&cfg, auth).expect("client");
    for stmt in backend.seed {
        client
            .execute_collect(stmt)
            .await
            .unwrap_or_else(|e| panic!("seed failed: {e}\n{stmt}"));
    }

    let fluree = FlureeBuilder::memory().build_memory();
    let mut source = SqlCreateConfig::new(
        "shop-live",
        url.clone(),
        format!("{}{LIVE_R2RML}", shop_mapping("")),
    );
    source.dialect = backend.dialect;
    fluree
        .create_sql_graph_source(source)
        .await
        .expect("create live sql source");
    let mut partitioned = SqlCreateConfig::new("shop-vp-live", url, vp_mapping(""));
    partitioned.dialect = backend.dialect;
    fluree
        .create_sql_graph_source(partitioned)
        .await
        .expect("create live partitioned sql source");

    let mut failures: Vec<String> = Vec::new();
    for c in cases().into_iter().chain(aggregate_cases()) {
        let sparql = format!("{PREFIX}{}", c.sparql)
            .replace("shop-sql:main", "shop-live:main")
            .replace("shop-vp:main", "shop-vp-live:main");
        let (lane_rows, sent) = lane_run(&fluree, &sparql, c.name).await;
        let declines = backend.declines.contains(&c.name);
        match (&c.routing, c.sql.is_empty()) {
            (Routing::MustFire, false) if declines && !sent.is_empty() => {
                failures.push(format!(
                    "{}: expected a decline on {:?}, sent {sent:?}",
                    c.name, backend.dialect
                ));
            }
            (Routing::MustFire, false) if !declines && sent.is_empty() => {
                failures.push(format!("{}: the lane sent no statement", c.name));
            }
            // A declined grouped shape may still run its block through the
            // join layer under the engine's grouping; only a grouped
            // statement would be wrong.
            (Routing::MustNotFire, _)
                if sent
                    .iter()
                    .any(|s| s.contains("GROUP BY") || s.contains("COUNT(")) =>
            {
                failures.push(format!("{}: a declined shape sent {sent:?}", c.name));
            }
            _ => {}
        }
        let pinned: Vec<String> = c.rows.iter().map(ToString::to_string).collect();
        if by_value(&lane_rows) != by_value(&pinned) {
            failures.push(format!(
                "{}: lane rows {lane_rows:?} differ from the pinned rows {:?} [sent: {sent:?}]",
                c.name, c.rows
            ));
        }
        let scan_rows = scan_run(&fluree, &sparql).await;
        if lane_rows != scan_rows {
            failures.push(format!(
                "{}: lane rows {lane_rows:?} differ from scan lane rows {scan_rows:?} [sent: {sent:?}]",
                c.name
            ));
        }
    }

    for c in live_cases() {
        if !c.only.is_empty() && !c.only.contains(&backend.dialect) {
            continue;
        }
        let sparql = format!("{PREFIX}{}", c.sparql);
        let (lane_rows, sent) = lane_run(&fluree, &sparql, c.name).await;
        let expects_nothing = c
            .sent
            .iter()
            .any(|(d, e)| *d == backend.dialect && matches!(e, Sent::Nothing));
        if sent.is_empty() != expects_nothing {
            failures.push(format!(
                "{}: the lane sent {sent:?}, expected {}",
                c.name,
                if expects_nothing {
                    "nothing"
                } else {
                    "a statement"
                }
            ));
        }
        for (dialect, expect) in c.sent {
            if *dialect != backend.dialect {
                continue;
            }
            match expect {
                Sent::Contains(frag) if !sent.iter().any(|s| s.contains(frag)) => {
                    failures.push(format!(
                        "{}: no statement contains {frag:?}; sent {sent:?}",
                        c.name
                    ));
                }
                Sent::Lacks(frag) if sent.iter().any(|s| s.contains(frag)) => {
                    failures.push(format!(
                        "{}: a statement contains {frag:?}; sent {sent:?}",
                        c.name
                    ));
                }
                _ => {}
            }
        }
        if lane_rows != c.rows {
            failures.push(format!(
                "{}: lane rows {lane_rows:?} differ from the pinned rows {:?} [sent: {sent:?}]",
                c.name, c.rows
            ));
        }
        let scan_rows = scan_run(&fluree, &sparql).await;
        if lane_rows != scan_rows {
            failures.push(format!(
                "{}: lane rows {lane_rows:?} differ from scan lane rows {scan_rows:?} [sent: {sent:?}]",
                c.name
            ));
        }
    }
    set_fast_paths_disabled(false);
    assert!(
        failures.is_empty(),
        "{:?}:\n{}",
        backend.dialect,
        failures.join("\n\n")
    );
}

#[tokio::test]
async fn live_bridge_sqlite_agrees_with_the_scan_lane() {
    live_differential(&SQLITE).await;
}

#[tokio::test]
async fn live_bridge_postgres_agrees_with_the_scan_lane() {
    live_differential(&POSTGRES).await;
}

#[tokio::test]
async fn live_bridge_mysql_agrees_with_the_scan_lane() {
    live_differential(&MYSQL).await;
}

/// A skipped differential is not a passing one: CI must supply every backend.
#[test]
fn live_bridge_backends_are_configured_in_ci() {
    if std::env::var("CI").is_err() {
        eprintln!("SKIPPED: not CI");
        return;
    }
    for b in [&SQLITE, &POSTGRES, &MYSQL] {
        assert!(
            std::env::var(b.url_var).is_ok_and(|v| !v.is_empty()),
            "{} must be set in CI, or the {:?} differential passes by doing nothing",
            b.url_var,
            b.dialect
        );
    }
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
