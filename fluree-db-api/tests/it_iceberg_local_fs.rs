//! End-to-end test of catalog-less LOCAL Iceberg tables (`file://` Direct mode).
//!
//! Drives the REAL read stack — direct catalog resolution (with the
//! version-hint-absent listing fallback), metadata/manifest parsing, Parquet
//! reads, the R2RML query path, and the snapshot-pinned/incremental scan
//! surface — against an actual Iceberg table on the local filesystem.
//!
//! Runs in CI against the COMMITTED fixture `tests/fixtures/iceberg/silver/
//! people` — a real pyiceberg-written two-snapshot table (5 rows:
//! alice..erin; snapshot 1 = 3 rows, snapshot 2 = +2). The fixture's metadata
//! carries the ABSOLUTE paths it was written under (`file:///tmp/...`), so
//! reading it from a checkout also proves the relocated-table location remap:
//! the provider infers `metadata.location → configured table_location` and
//! rewrites every manifest file reference.
//!
//! Regenerate the fixture (needs `pip install "pyiceberg[sql-sqlite,pyarrow]"`):
//!
//! ```bash
//! python3 scripts/local/write_local_iceberg_table.py /tmp/fluree-local-iceberg
//! cp -r /tmp/fluree-local-iceberg/silver/people fluree-db-api/tests/fixtures/iceberg/silver/people
//! ```
//!
//! `FLUREE_LOCAL_ICEBERG_TABLE=file:///path/to/table` overrides the fixture to
//! run against any table with the same shape.
//!
//! Local tables are fail-closed behind `FLUREE_ICEBERG_LOCAL_ROOTS` (see
//! `fluree_db_iceberg::local_guard`), so the test sets that allowlist to the
//! table's own directory before touching the stack — which also keeps it
//! honest: a read that escaped the table directory would be refused here just
//! as it would in a deployment.

#![cfg(all(feature = "iceberg", feature = "native"))]

use fluree_db_api::{FlureeBuilder, FlureeR2rmlProvider, R2rmlCreateConfig};
use futures::TryStreamExt;

const PEOPLE_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#PeopleMapping>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people" ] ;
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
        ] .
"#;

fn table_location() -> String {
    if let Ok(loc) = std::env::var("FLUREE_LOCAL_ICEBERG_TABLE") {
        if !loc.trim().is_empty() {
            return loc;
        }
    }
    // The committed fixture, resolved from the crate dir so the test runs from
    // any checkout location — which is exactly what exercises the remap (the
    // fixture's metadata references the /tmp path it was written under).
    format!(
        "file://{}/tests/fixtures/iceberg/silver/people",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// Allowlist the table's own directory, as a deployment would. Must run before
/// anything builds Iceberg storage — the guard captures the roots on first use.
fn allow_table_root(location: &str) {
    let root = location.strip_prefix("file://").unwrap_or(location);
    // SAFETY: set at the top of the test, before any storage or scan is built.
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", root);
}

#[tokio::test]
async fn local_table_end_to_end() {
    let location = table_location();
    allow_table_root(&location);
    let fluree = FlureeBuilder::memory().build_memory();

    // 1. Register the graph source: Direct mode, file:// location, inline
    //    mapping. No catalog service, no object store, no credentials.
    let config = R2rmlCreateConfig::new_direct("local-people", &location, PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    let created = fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create local-file graph source");
    eprintln!(
        "graph source {} created (connection_tested={}, mapping_validated={})",
        created.graph_source_id, created.connection_tested, created.mapping_validated
    );

    // 2. Query it through the R2RML query path — the full stack: direct
    //    catalog (listing fallback: pyiceberg writes no version-hint.text),
    //    metadata + Avro manifests, Parquet decode, term materialization.
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": "local-people:main",
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
    });
    let rows = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("query local iceberg table");
    let names = rows.as_array().expect("array result");
    eprintln!("query returned {} rows: {names:?}", names.len());
    assert_eq!(names.len(), 5, "the fixture's five rows come back");
    let all = names
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<String>();
    assert!(all.contains("alice") && all.contains("erin"), "got: {all}");

    // 3. The snapshot surface: pinned + incremental reads over the same table.
    let provider = FlureeR2rmlProvider::new(&fluree);
    let gs = "local-people:main";

    // Current snapshot resolves.
    let current = provider
        .current_snapshot_id(gs, "silver.people")
        .await
        .expect("current snapshot")
        .expect("table has snapshots");

    // Full unpinned streaming read: all 5 rows.
    let scan = provider
        .scan_for_materialize_stream(gs, "silver.people", &[], None, None, None)
        .await
        .expect("full scan");
    assert_eq!(scan.to_snapshot_id, Some(current));
    let batches: Vec<_> = scan.stream.try_collect().await.expect("stream batches");
    let full_rows: usize = batches.iter().map(|b| b.num_rows).sum();
    assert_eq!(full_rows, 5, "full read sees both snapshots' rows");

    // Find the FIRST snapshot id (parent of current) by loading the table
    // metadata straight through the iceberg crate — which also exercises the
    // direct-catalog listing fallback at its own level (pyiceberg writes no
    // version-hint.text).
    let first = {
        use fluree_db_iceberg::catalog::{SendCatalogClient, TableIdentifier};
        use fluree_db_iceberg::io::FileIcebergStorage;
        use fluree_db_iceberg::metadata::TableMetadata;
        use fluree_db_iceberg::{SendDirectCatalogClient, SendIcebergStorage};

        let storage = std::sync::Arc::new(FileIcebergStorage::new());
        let client =
            SendDirectCatalogClient::new(location.clone(), std::sync::Arc::clone(&storage));
        let resp = client
            .load_table(&TableIdentifier::new("silver", "people"), false)
            .await
            .expect("direct load_table via listing fallback");
        let bytes = SendIcebergStorage::read(storage.as_ref(), &resp.metadata_location)
            .await
            .expect("read metadata json");
        let meta = TableMetadata::from_json_str(std::str::from_utf8(&bytes).expect("utf8"))
            .expect("parse metadata");
        meta.snapshot(current)
            .and_then(|s| s.parent_snapshot_id)
            .expect("two snapshots in fixture")
    };

    let scan = provider
        .scan_for_materialize_stream(gs, "silver.people", &[], Some(first), None, None)
        .await
        .expect("incremental scan");
    assert!(scan.incremental, "append-only window scans incrementally");
    let batches: Vec<_> = scan.stream.try_collect().await.expect("stream batches");
    let inc_rows: usize = batches.iter().map(|b| b.num_rows).sum();
    assert_eq!(
        inc_rows, 2,
        "incremental window sees only the second append"
    );

    // PINNED read: to = the FIRST snapshot → only the first append's rows,
    // and the resolved watermark is the pin, not current.
    let scan = provider
        .scan_for_materialize_stream(gs, "silver.people", &[], None, Some(first), None)
        .await
        .expect("pinned scan");
    assert_eq!(scan.to_snapshot_id, Some(first), "pin is honored");
    let batches: Vec<_> = scan.stream.try_collect().await.expect("stream batches");
    let pinned_rows: usize = batches.iter().map(|b| b.num_rows).sum();
    assert_eq!(
        pinned_rows, 3,
        "pinned read sees the table as of snapshot 1"
    );

    // An expired/unknown pin is the typed error, never a fall-forward.
    // (`MaterializeScan` has no Debug — a stream field — so match manually.)
    match provider
        .scan_for_materialize_stream(gs, "silver.people", &[], None, Some(999), None)
        .await
    {
        Ok(_) => panic!("unknown pin must fail, not fall forward"),
        Err(fluree_db_query::error::QueryError::SnapshotNotFound {
            snapshot_id: 999, ..
        }) => {}
        Err(other) => panic!("expected typed SnapshotNotFound, got: {other}"),
    }

    // A local location OUTSIDE the allowlist is refused when the graph source is
    // CREATED — the operator is told which switch governs it, rather than the
    // path being read and its directory listing surfacing in a later error.
    // Asserted in this test rather than its own so the allowlist is already
    // installed and no second process/env write can race it.
    let outside = R2rmlCreateConfig::new_direct("etc-probe", "/etc", PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    let err = fluree
        .create_r2rml_graph_source(outside)
        .await
        .expect_err("a local location outside the allowlist must be refused")
        .to_string();
    assert!(
        err.contains("FLUREE_ICEBERG_LOCAL_ROOTS"),
        "refusal must name the switch that governs local tables: {err}"
    );

    eprintln!("local iceberg end-to-end: all assertions passed");
}
