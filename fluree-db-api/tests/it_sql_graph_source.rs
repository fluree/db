//! End-to-end over a SQL graph source: registration, the R2RML query path,
//! typed filter pushdown and the exact COUNT shortcut — against a fake
//! Trino-protocol endpoint, so the SQL the engine actually sends is asserted.

#![cfg(all(feature = "sql", feature = "native"))]

use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, SqlCreateConfig, TxnOpts};
use serde_json::{json, Value};
use wiremock::MockServer;

const PEOPLE_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#People>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sales.people" ] ;
        rr:subjectMap [
            rr:template "http://example.org/person/{id}" ;
            rr:class ex:Person
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:name ;
            rr:objectMap [ rr:column "name" ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:score ;
            rr:objectMap [ rr:column "score" ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:born ;
            rr:objectMap [ rr:column "born" ]
        ] .
"#;

#[path = "support/fake_sql.rs"]
mod fake_sql;
use fake_sql::{FakeSql, Table};

/// The fake endpoint: an in-memory `sales.people` the statements run against.
async fn fake_trino() -> MockServer {
    FakeSql::new()
        .table(Table::new(
            "sales.people",
            &[
                ("id", "bigint"),
                ("name", "varchar"),
                ("score", "double"),
                ("born", "date"),
            ],
            vec![
                vec![json!(1), json!("alice"), json!(9.25), json!("1985-01-02")],
                vec![json!(2), json!("bob"), json!(7.5), json!("1990-05-04")],
                vec![json!(3), Value::Null, Value::Null, Value::Null],
            ],
        ))
        .mount()
        .await
}

/// SPARQL JSON results → the binding rows.
fn bindings(v: &Value) -> Vec<Value> {
    v.pointer("/results/bindings")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| panic!("not SPARQL JSON results: {v}"))
}

async fn statements(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.method == "POST")
        .map(|r| String::from_utf8_lossy(&r.body).to_string())
        .collect()
}

#[tokio::test]
async fn sql_graph_source_end_to_end() {
    let server = fake_trino().await;
    let fluree = FlureeBuilder::memory().build_memory();

    // 1. Register.
    let mut config = SqlCreateConfig::new("people-sql", server.uri(), PEOPLE_R2RML);
    config.catalog = Some("pg".into());
    let created = fluree
        .create_sql_graph_source(config)
        .await
        .expect("create sql graph source");
    assert_eq!(created.graph_source_id, "people-sql:main");
    assert!(created.connection_tested, "SELECT 1 probe succeeded");
    assert!(created.mapping_validated);
    assert_eq!(created.table_names, vec!["sales.people".to_string()]);
    assert_eq!(created.triples_map_count, 1);

    let info = fluree
        .nameservice()
        .lookup_graph_source("people-sql:main")
        .await
        .expect("lookup")
        .expect("record");
    assert_eq!(
        info.source_type,
        fluree_db_nameservice::GraphSourceType::Sql
    );

    // 2. A plain scan.
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": "people-sql:main",
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
    });
    let rows = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("query sql source");
    let names: Vec<String> = rows
        .as_array()
        .expect("array")
        .iter()
        .map(std::string::ToString::to_string)
        .collect();
    assert_eq!(
        names.len(),
        2,
        "the null-name row yields no ex:name triple: {names:?}"
    );
    assert!(names.iter().any(|n| n.contains("alice")) && names.iter().any(|n| n.contains("bob")));

    let sent = statements(&server).await;
    let probe = sent
        .iter()
        .find(|s| s.contains("LIMIT 0"))
        .expect("schema probe was issued");
    assert_eq!(probe, r#"SELECT * FROM "sales"."people" LIMIT 0"#);
    let scan = sent
        .iter()
        .find(|s| s.starts_with(r#"SELECT "t0""#))
        .unwrap_or_else(|| panic!("block statement; sent: {sent:#?}"));
    assert_eq!(
        scan,
        r#"SELECT "t0"."id" AS "c0", "t0"."name" AS "c1" FROM "sales"."people" AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."name" IS NOT NULL"#,
        "one statement, only the mapped and needed columns"
    );

    // 3. A constant object is pushed as a typed WHERE.
    let sparql = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s ?score FROM <people-sql:main>
        WHERE { ?s ex:name "bob" ; ex:score ?score }
    "#;
    let rows = fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("filtered query");
    let rows = bindings(&rows);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert!(rows[0].to_string().contains("person/2"), "{rows:?}");
    let sent = statements(&server).await;
    assert!(
        sent.iter().any(|s| s.contains(r#""t0"."name" = 'bob'"#)),
        "equality pushed to SQL: {sent:?}"
    );

    // 4. Typed decoding: a date column round-trips as xsd:date.
    let sparql = "
        PREFIX ex: <http://example.org/>
        SELECT ?born FROM <people-sql:main>
        WHERE { <http://example.org/person/1> ex:born ?born }
    ";
    let rows = fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("date query");
    assert!(rows.to_string().contains("1985-01-02"), "{rows}");

    // 5. COUNT over the class answers 3 whether the exact shortcut fired or
    //    the scan counted (the fake is consistent); record which.
    let sparql = "
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(?s) AS ?n) FROM <people-sql:main>
        WHERE { ?s a ex:Person }
    ";
    let rows = fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("count query");
    assert!(rows.to_string().contains('3'), "{rows}");
    let sent = statements(&server).await;
    eprintln!(
        "COUNT(*) shortcut fired: {}",
        sent.iter().any(|s| s.contains("COUNT(*)"))
    );
}

const ORDERS_SQLQUERY_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#OpenOrders>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:sqlQuery "SELECT id, total FROM sales.orders WHERE status = 'open'" ] ;
        rr:subjectMap [
            rr:template "http://example.org/order/{id}" ;
            rr:class ex:Order
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:total ;
            rr:objectMap [ rr:column "total" ]
        ] .
"#;

/// An `rr:sqlQuery` logical table is scanned as a derived table, with the
/// projection and pushed filters applied on top of the mapping's query.
#[tokio::test]
async fn sql_query_logical_table_is_scanned_as_a_derived_table() {
    let server = FakeSql::new()
        .table(Table::new(
            "sales.orders_open",
            &[("id", "bigint"), ("total", "decimal(10,2)")],
            vec![
                vec![json!(10), json!("99.50")],
                vec![json!(11), json!("5.00")],
            ],
        ))
        .query(
            "SELECT id, total FROM sales.orders WHERE status = 'open'",
            "sales.orders_open",
        )
        .mount()
        .await;

    let fluree = FlureeBuilder::memory().build_memory();
    let created = fluree
        .create_sql_graph_source(SqlCreateConfig::new(
            "orders-sql",
            server.uri(),
            ORDERS_SQLQUERY_R2RML,
        ))
        .await
        .expect("create");
    assert_eq!(created.table_count, 1);
    assert!(
        created.table_names[0].starts_with("sqlQuery:"),
        "{:?}",
        created.table_names
    );

    let sparql = "
        PREFIX ex: <http://example.org/>
        SELECT ?o ?total FROM <orders-sql:main>
        WHERE { ?o ex:total ?total } ORDER BY ?o
    ";
    let rows = fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("query over rr:sqlQuery");
    let rows = bindings(&rows);
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(
        rows[0].to_string().contains("order/10") && rows[0].to_string().contains("99.50"),
        "{rows:?}"
    );

    let sent = statements(&server).await;
    let scan = sent
        .iter()
        .find(|s| s.starts_with(r#"SELECT "t0""#))
        .expect("derived-table statement");
    assert_eq!(
        scan,
        r#"SELECT "t0"."id" AS "c0", "t0"."total" AS "c1" FROM (SELECT id, total FROM sales.orders WHERE status = 'open') AS "t0" WHERE "t0"."id" IS NOT NULL AND "t0"."total" IS NOT NULL"#
    );
}

/// The Iceberg-backed registration path refuses `rr:sqlQuery` up front.
#[tokio::test]
async fn iceberg_sources_refuse_sql_query_mappings() {
    let fluree = FlureeBuilder::memory().build_memory();
    let config = fluree_db_api::R2rmlCreateConfig::new(
        "ice",
        "https://polaris.example.invalid",
        "default.default",
        ORDERS_SQLQUERY_R2RML,
    );
    let err = fluree
        .create_r2rml_graph_source(config)
        .await
        .expect_err("rr:sqlQuery is not for Iceberg");
    assert!(err.to_string().contains("rr:sqlQuery"), "{err}");
}

#[tokio::test]
async fn registration_survives_an_unreachable_endpoint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let config = SqlCreateConfig::new("dead-sql", "http://127.0.0.1:9", PEOPLE_R2RML);
    let created = fluree
        .create_sql_graph_source(config)
        .await
        .expect("registration does not require a live endpoint");
    assert!(!created.connection_tested);
    assert!(created.mapping_validated);

    // Querying it surfaces the transport error rather than empty results.
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": "dead-sql:main",
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
    });
    let err = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect_err("unreachable endpoint fails the query");
    assert!(err.to_string().contains("dead-sql:main"), "{err}");
}

/// Against a live `fluree-sql-bridge` (or Trino) — run with
/// `FLUREE_SQL_BRIDGE_URL=http://127.0.0.1:8080` and
/// `FLUREE_SQL_BRIDGE_DIALECT=sqlite|postgres|mysql|trino` (default `sqlite`,
/// which is what CI's bridge job supplies); `FLUREE_SQL_BRIDGE_CATALOG` /
/// `FLUREE_SQL_BRIDGE_SCHEMA` qualify the table. Behind a bridge the test
/// seeds its own `bridge_people(id, name, score, born)` table through the
/// bridge (its own name, so the pushdown lane's live suite over the same
/// bridge cannot leave a `people` of another shape behind); a Trino endpoint
/// must already serve it. Skips (loudly) when unset, so CI without a bridge
/// does not silently pass it.
#[tokio::test]
async fn live_bridge_round_trip() {
    use fluree_db_api::SqlDialect;
    let Ok(endpoint) = std::env::var("FLUREE_SQL_BRIDGE_URL") else {
        eprintln!("SKIPPED live_bridge_round_trip: FLUREE_SQL_BRIDGE_URL not set");
        return;
    };
    let dialect = match std::env::var("FLUREE_SQL_BRIDGE_DIALECT").as_deref() {
        Ok("postgres") => SqlDialect::Postgres,
        Ok("mysql") => SqlDialect::Mysql,
        Ok("trino") => SqlDialect::Trino,
        _ => SqlDialect::Sqlite,
    };
    let ddl = match dialect {
        SqlDialect::Sqlite => {
            Some("CREATE TABLE bridge_people (id INTEGER, name TEXT, score REAL, born DATE)")
        }
        SqlDialect::Postgres => Some(
            "CREATE TABLE bridge_people (id BIGINT, name TEXT, score DOUBLE PRECISION, born DATE)",
        ),
        SqlDialect::Mysql => Some(
            "CREATE TABLE bridge_people (id BIGINT, name VARCHAR(64), score DOUBLE, born DATE)",
        ),
        SqlDialect::Trino => None,
    };
    if let Some(ddl) = ddl {
        let mut cfg = fluree_db_sql::SqlGsConfig::new(endpoint.clone());
        cfg.dialect = dialect;
        let cfg = cfg.hydrate(None).await.expect("hydrate");
        let auth = cfg.auth.create_provider_arc().expect("auth");
        let client = fluree_db_sql::TrinoClient::new(&cfg, auth).expect("client");
        for stmt in [
            "DROP TABLE IF EXISTS bridge_people",
            ddl,
            "INSERT INTO bridge_people VALUES (1, 'alice', 9.25, '1985-01-02'), (2, 'bob', NULL, NULL), (3, NULL, 7.5, '1990-05-06')",
        ] {
            client
                .execute_collect(stmt)
                .await
                .unwrap_or_else(|e| panic!("seed failed: {e}\n{stmt}"));
        }
    }
    let mapping = PEOPLE_R2RML.replace("sales.people", "bridge_people");
    let fluree = FlureeBuilder::memory().build_memory();
    let mut config = SqlCreateConfig::new("live-sql", endpoint, mapping);
    config.dialect = dialect;
    config.catalog = std::env::var("FLUREE_SQL_BRIDGE_CATALOG").ok();
    config.schema = std::env::var("FLUREE_SQL_BRIDGE_SCHEMA").ok();
    let created = fluree
        .create_sql_graph_source(config)
        .await
        .expect("create");
    assert!(
        created.connection_tested,
        "SELECT 1 against the live endpoint"
    );

    let sparql = "
        PREFIX ex: <http://example.org/>
        SELECT ?s ?name ?score ?born FROM <live-sql:main>
        WHERE { ?s ex:name ?name . OPTIONAL { ?s ex:score ?score } OPTIONAL { ?s ex:born ?born } }
        ORDER BY ?s
    ";
    let rows = fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("live query");
    let rows = bindings(&rows);
    assert_eq!(rows.len(), 2, "{rows:?}");
    let text = rows[0].to_string();
    assert!(
        text.contains("alice") && text.contains("9.25") && text.contains("1985-01-02"),
        "{text}"
    );

    let sparql = "
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(?s) AS ?n) FROM <live-sql:main> WHERE { ?s a ex:Person }
    ";
    let rows = fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("live count");
    assert!(rows.to_string().contains('3'), "{rows}");

    let sparql = "
        PREFIX ex: <http://example.org/>
        SELECT ?s FROM <live-sql:main> WHERE { ?s ex:name \"bob\" }
    ";
    let rows = fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("live pushed filter");
    let rows = bindings(&rows);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert!(rows[0].to_string().contains("person/2"), "{rows:?}");
}

/// A ledger joined with a SQL source in one query: the ledger holds facts
/// about the same subjects the SQL rows mint, and the join happens in-engine.
#[tokio::test]
async fn ledger_and_sql_source_join_in_one_dataset() {
    let server = fake_trino().await;
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_sql_graph_source(SqlCreateConfig::new(
            "people-sql",
            server.uri(),
            PEOPLE_R2RML,
        ))
        .await
        .expect("create sql source");

    let ledger = fluree.create_ledger("teams:main").await.expect("ledger");
    fluree
        .insert_turtle_with_opts(
            ledger,
            "@prefix ex: <http://example.org/> .\n\
             <http://example.org/person/1> ex:team \"red\" .\n\
             <http://example.org/person/2> ex:team \"blue\" .",
            TxnOpts::default(),
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 5_000_000_000,
                reindex_max_bytes: 5_000_000_000,
            },
            None,
        )
        .await
        .expect("insert");

    // Joining a ledger with a mapped source: the ledger is the default graph,
    // the source is a GRAPH block — and on the dataset path it must be listed
    // with FROM NAMED, exactly as for an Iceberg source (without it the GRAPH
    // block resolves to nothing and the join is empty).
    let sparql = "
        PREFIX ex: <http://example.org/>
        SELECT ?name ?team FROM <teams:main> FROM NAMED <people-sql:main>
        WHERE { ?p ex:team ?team . GRAPH <people-sql:main> { ?p ex:name ?name } }
        ORDER BY ?name
    ";
    let rows = fluree
        .query_from()
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("join query");
    let rows = bindings(&rows);
    assert_eq!(rows.len(), 2, "{rows:?}");
    let row = |i: usize, var: &str| rows[i][var]["value"].as_str().unwrap_or("").to_string();
    assert_eq!(
        (row(0, "name"), row(0, "team")),
        ("alice".into(), "red".into()),
        "{rows:?}"
    );
    assert_eq!(
        (row(1, "name"), row(1, "team")),
        ("bob".into(), "blue".into()),
        "{rows:?}"
    );

    // The same join in JSON-LD: the graph pattern is the ARRAY form
    // `["graph", <alias>, pattern]` (an object with a "graph" key is not a
    // graph pattern), and the source enters the dataset via `fromNamed`.
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": "teams:main",
        "from-named": ["people-sql:main"],
        "select": ["?name", "?team"],
        "where": [
            {"@id": "?p", "ex:team": "?team"},
            ["graph", "people-sql:main", {"@id": "?p", "ex:name": "?name"}]
        ],
        "orderBy": "?name"
    });
    let rows = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("jsonld join query");
    let rows = rows.as_array().expect("array").clone();
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0], json!(["alice", "red"]), "{rows:?}");
    assert_eq!(rows[1], json!(["bob", "blue"]), "{rows:?}");
}
