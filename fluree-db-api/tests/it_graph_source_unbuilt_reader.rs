//! A build without the `iceberg` feature has no reader for a mapped graph
//! source (Iceberg / R2RML, and SQL / Delta, which stack on it). A record of
//! one, written by a build that had the reader into a shared nameservice, is
//! refused by every query route with what the build lacks, never read as an
//! empty graph or reported as a missing ledger.
//!
//! Compiled only without `iceberg` (`cargo test -p fluree-db-api --test
//! grp_graphsource`, whose default features leave it off). CI's test run is
//! `--all-features`, so it never builds this module; the reader decision itself
//! is tested in every build by `graph_source::tests` in the library.

use fluree_db_api::{ApiError, FlureeBuilder};

/// Store a mapped record, as a build with the reader would have registered it.
/// No query here parses the config: the refusal is by type.
async fn publish(
    fluree: &fluree_db_api::Fluree,
    name: &str,
    source_type: fluree_db_nameservice::GraphSourceType,
) -> String {
    let config = r#"{"catalog":{"type":"direct","table_location":"s3://bucket/ns/t"}}"#;
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_graph_source(name, "main", source_type, config, &[])
        .await
        .expect("publish mapped record");
    format!("{name}:main")
}

fn assert_unsupported<T: std::fmt::Debug>(
    result: Result<T, ApiError>,
    gs: &str,
    feature: &str,
    route: &str,
) {
    let err = match result {
        Ok(answer) => panic!("{route}: answered instead of refusing: {answer:?}"),
        Err(err) => err,
    };
    assert_eq!(
        err.error_type(),
        "err:db/GraphSourceUnsupported",
        "{route}: {err}"
    );
    assert_eq!(err.status_code(), 501, "{route}: {err}");
    let message = err.to_string();
    assert!(
        message.contains(&format!("graph source '{gs}'"))
            && message.contains(&format!("compiled without the '{feature}' feature")),
        "{route}: {message}"
    );
}

#[tokio::test]
async fn a_mapped_source_this_build_cannot_read_is_refused_on_every_route() {
    let fluree = FlureeBuilder::memory().build_memory();
    let gs = publish(
        &fluree,
        "unbuilt-iceberg",
        fluree_db_nameservice::GraphSourceType::Iceberg,
    )
    .await;

    let sparql = "SELECT ?s WHERE { ?s ?p ?o }";
    let from = format!("SELECT ?s FROM <{gs}> WHERE {{ ?s ?p ?o }}");
    assert_unsupported(
        fluree.query_from().sparql(&from).execute_formatted().await,
        &gs,
        "iceberg",
        "SPARQL FROM",
    );
    let jsonld = serde_json::json!({
        "from": gs,
        "select": ["?s"],
        "where": {"@id": "?s", "?p": "?o"},
    });
    assert_unsupported(
        fluree
            .query_from()
            .jsonld(&jsonld)
            .execute_formatted()
            .await,
        &gs,
        "iceberg",
        "JSON-LD from",
    );

    // The direct target once answered "Ledger not found".
    let target = fluree.graph(&gs);
    assert_unsupported(
        target.query().sparql(sparql).execute_formatted().await,
        &gs,
        "iceberg",
        "SPARQL target",
    );
    let target_jsonld = serde_json::json!({
        "select": ["?s"],
        "where": {"@id": "?s", "?p": "?o"},
    });
    assert_unsupported(
        target
            .query()
            .jsonld(&target_jsonld)
            .execute_formatted()
            .await,
        &gs,
        "iceberg",
        "JSON-LD target",
    );

    // SQL stacks on the iceberg reader, so this build lacks its own as well.
    let sql = publish(
        &fluree,
        "unbuilt-sql",
        fluree_db_nameservice::GraphSourceType::Sql,
    )
    .await;
    let sql_from = format!("SELECT ?s FROM <{sql}> WHERE {{ ?s ?p ?o }}");
    assert_unsupported(
        fluree
            .query_from()
            .sparql(&sql_from)
            .execute_formatted()
            .await,
        &sql,
        "sql",
        "SPARQL FROM a SQL source",
    );
}
