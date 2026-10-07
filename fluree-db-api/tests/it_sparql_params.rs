//! SPARQL parameters: a variable bound to a value runs as if the value were
//! written in its place.

use fluree_db_api::{
    Fluree, FlureeBuilder, FormatterConfig, GraphDb, QueryExecutionOptions, SparqlParamMap,
    TxnOperation,
};
use serde_json::{json, Value as JsonValue};

const LEDGER: &str = "it/sparql-params:main";
const PREFIX: &str = "PREFIX ex: <http://example.org/> ";

async fn seeded() -> Fluree {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger(LEDGER).await.expect("create");
    fluree
        .graph(LEDGER)
        .transact()
        .insert(&json!({
            "@context": { "ex": "http://example.org/" },
            "@graph": [
                { "@id": "ex:alice", "ex:name": "Alice", "ex:age": 30 },
                { "@id": "ex:bob", "ex:name": "Bob", "ex:age": 17 },
                { "@id": "ex:carol", "ex:name": "Carol" },
            ],
        }))
        .commit()
        .await
        .expect("seed");
    fluree
}

fn params(value: JsonValue) -> SparqlParamMap {
    value.as_object().expect("an object").clone()
}

async fn rows(
    fluree: &Fluree,
    db: &GraphDb,
    sparql: &str,
    params: Option<SparqlParamMap>,
) -> fluree_db_api::Result<Vec<JsonValue>> {
    let options = match params {
        Some(params) => QueryExecutionOptions::new().with_params(params),
        None => QueryExecutionOptions::new(),
    };
    let result = fluree
        .query_with_options(db, format!("{PREFIX}{sparql}").as_str(), options)
        .await?;
    let mut rows = match result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format")
    {
        JsonValue::Array(rows) => rows,
        other => vec![other],
    };
    rows.sort_by_key(ToString::to_string);
    Ok(rows)
}

async fn head(fluree: &Fluree) -> GraphDb {
    fluree.db(LEDGER).await.expect("db")
}

#[tokio::test]
async fn parameters_run_as_the_inline_query() {
    let fluree = seeded().await;
    let db = head(&fluree).await;
    let parameterized =
        "SELECT ?s ?age WHERE { ?s ex:name $name ; ex:age ?age FILTER(?age > $min) }";
    let inline = "SELECT ?s ?age WHERE { ?s ex:name \"Alice\" ; ex:age ?age FILTER(?age > 21) }";
    let values = params(json!({ "name": "Alice", "min": 21 }));

    let with_params = rows(&fluree, &db, parameterized, Some(values.clone()))
        .await
        .unwrap();
    assert_eq!(with_params, vec![json!(["ex:alice", 30])]);
    assert_eq!(with_params, rows(&fluree, &db, inline, None).await.unwrap());

    let explained = fluree
        .explain_sparql_with_params(&db, &format!("{PREFIX}{parameterized}"), Some(&values))
        .await
        .unwrap();
    let explained_inline = fluree
        .explain_sparql(&db, &format!("{PREFIX}{inline}"))
        .await
        .unwrap();
    assert_eq!(explained["plan"], explained_inline["plan"]);
}

/// A trailing `VALUES` join would leave `$min` unbound inside the OPTIONAL,
/// so its filter would drop every age.
#[tokio::test]
async fn a_filter_inside_optional_sees_the_value() {
    let fluree = seeded().await;
    let db = head(&fluree).await;
    let rows = rows(
        &fluree,
        &db,
        "SELECT ?name ?age WHERE { ?s ex:name ?name OPTIONAL { ?s ex:age ?age FILTER(?age >= $min) } }",
        Some(params(json!({ "min": 18 }))),
    )
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            json!(["Alice", 30]),
            json!(["Bob", null]),
            json!(["Carol", null])
        ]
    );
}

#[tokio::test]
async fn a_projected_parameter_is_a_column() {
    let fluree = seeded().await;
    let db = head(&fluree).await;
    let projected = rows(
        &fluree,
        &db,
        "SELECT $name ?age WHERE { ?s ex:name $name ; ex:age ?age }",
        Some(params(json!({ "name": "Bob" }))),
    )
    .await
    .unwrap();
    assert_eq!(projected, vec![json!(["Bob", 17])]);

    let grouped = rows(
        &fluree,
        &db,
        "SELECT ?name (COUNT(?s) AS ?n) WHERE { ?s ex:name ?name } GROUP BY ?name",
        Some(params(json!({ "name": "Bob" }))),
    )
    .await
    .unwrap();
    assert_eq!(grouped, vec![json!(["Bob", 1])]);
}

#[tokio::test]
async fn a_misspelt_parameter_or_a_json_ld_query_is_refused() {
    let fluree = seeded().await;
    let db = head(&fluree).await;
    let err = rows(
        &fluree,
        &db,
        "SELECT ?s WHERE { ?s ex:name $name }",
        Some(params(json!({ "nmae": "Alice" }))),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("not a variable"), "{err}");

    let jsonld =
        json!({ "select": ["?s"], "where": { "@id": "?s", "http://example.org/name": "?n" } });
    let Err(err) = fluree
        .query_with_options(
            &db,
            &jsonld,
            QueryExecutionOptions::new().with_params(params(json!({ "n": "Alice" }))),
        )
        .await
    else {
        panic!("a JSON-LD query takes no parameters");
    };
    assert!(err.to_string().contains("SPARQL"), "{err}");
}

#[test]
fn empty_parameters_are_no_parameters() {
    let options = QueryExecutionOptions::new().with_params(SparqlParamMap::new());
    assert!(format!("{options:?}").contains("params: None"));
}

#[tokio::test]
async fn updates_take_parameters() {
    let fluree = seeded().await;
    let birthday = format!(
        "{PREFIX}DELETE {{ ?s ex:age ?old }} INSERT {{ ?s ex:age $age }} \
         WHERE {{ ?s ex:name $name ; ex:age ?old }}"
    );
    let bob = params(json!({ "name": "Bob", "age": 18 }));
    fluree
        .graph(LEDGER)
        .transact()
        .sparql_update_with_params(&birthday, &bob)
        .commit()
        .await
        .unwrap();

    let mut txn = fluree
        .begin_transaction(LEDGER, fluree_db_api::TransactionOptions::default())
        .await
        .unwrap();
    txn.stage(TxnOperation::SparqlUpdate(
        birthday.clone(),
        Some(params(json!({ "name": "Alice", "age": 31 }))),
    ))
    .await
    .unwrap();
    txn.commit(Default::default()).await.unwrap();

    let db = head(&fluree).await;
    let ages = rows(
        &fluree,
        &db,
        "SELECT ?name ?age WHERE { ?s ex:name ?name ; ex:age ?age }",
        None,
    )
    .await
    .unwrap();
    assert_eq!(ages, vec![json!(["Alice", 31]), json!(["Bob", 18])]);

    let Err(err) = fluree
        .graph(LEDGER)
        .transact()
        .sparql_update_with_params(&birthday, &params(json!({ "nmae": "Bob", "age": 1 })))
        .commit()
        .await
    else {
        panic!("a misspelt parameter is refused");
    };
    assert!(err.to_string().contains("not a variable"), "{err}");
}

#[tokio::test]
async fn a_connection_query_takes_parameters() {
    let fluree = seeded().await;
    let result = fluree
        .query_from()
        .sparql(&format!(
            "{PREFIX}SELECT ?age FROM <{LEDGER}> WHERE {{ ?s ex:name $name ; ex:age ?age }}"
        ))
        .params(params(json!({ "name": "Alice" })))
        .format(FormatterConfig::sparql_json())
        .execute_formatted()
        .await
        .unwrap();
    let bindings = result["results"]["bindings"].as_array().unwrap();
    assert_eq!(bindings.len(), 1, "{result}");
    assert_eq!(bindings[0]["age"]["value"], "30");
}

#[tokio::test]
async fn a_blank_node_parameter_is_one_stored_node() {
    let fluree = seeded().await;
    fluree
        .graph(LEDGER)
        .transact()
        .insert(&json!({ "http://example.org/name": "Dan" }))
        .commit()
        .await
        .unwrap();
    let db = head(&fluree).await;
    let dan = rows(&fluree, &db, "SELECT ?s WHERE { ?s ex:name \"Dan\" }", None)
        .await
        .unwrap();
    let dan = dan[0][0].as_str().expect("a blank node id").to_string();
    assert!(dan.starts_with("_:fdb-"), "{dan}");

    // A label that isn't a stored id would lower to a variable and match every node.
    for label in [
        json!({ "@id": "_:x" }),
        json!({ "@value": "_:x", "@type": "@id" }),
    ] {
        let err = rows(
            &fluree,
            &db,
            "SELECT ?n WHERE { $s ex:name ?n }",
            Some(params(json!({ "s": label }))),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("blank node label"), "{err}");
    }
    let Err(err) = fluree
        .graph(LEDGER)
        .transact()
        .sparql_update_with_params(
            &format!("{PREFIX}DELETE WHERE {{ $s ?p ?o }}"),
            &params(json!({ "s": { "@id": "_:x" } })),
        )
        .commit()
        .await
    else {
        panic!("a blank node label is refused");
    };
    assert!(err.to_string().contains("blank node label"), "{err}");

    fluree
        .graph(LEDGER)
        .transact()
        .sparql_update_with_params(
            &format!("{PREFIX}DELETE WHERE {{ $s ?p ?o }}"),
            &params(json!({ "s": { "@id": dan } })),
        )
        .commit()
        .await
        .unwrap();
    let db = head(&fluree).await;
    let names = rows(&fluree, &db, "SELECT ?n WHERE { ?s ex:name ?n }", None)
        .await
        .unwrap();
    assert_eq!(
        names,
        vec![json!(["Alice"]), json!(["Bob"]), json!(["Carol"])]
    );
}

#[tokio::test]
async fn an_id_typed_value_is_an_iri() {
    let fluree = seeded().await;
    let db = head(&fluree).await;
    let sparql = "SELECT ?n WHERE { $s ex:name ?n }";
    let typed = params(json!({ "s": { "@value": "http://example.org/alice", "@type": "@id" } }));
    let id = params(json!({ "s": { "@id": "http://example.org/alice" } }));
    let names = rows(&fluree, &db, sparql, Some(typed)).await.unwrap();
    assert_eq!(names, vec![json!(["Alice"])]);
    assert_eq!(names, rows(&fluree, &db, sparql, Some(id)).await.unwrap());
}
