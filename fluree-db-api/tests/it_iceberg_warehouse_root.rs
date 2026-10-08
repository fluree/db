//! Catalog-less WAREHOUSE-ROOT Direct locations through `prepare_iceberg_scan`.
//!
//! A `table_location` may name a multi-table warehouse root rather than one
//! table's directory — the shape a bucket copy of a catalog-managed database
//! takes. Resolving the table's own directory beneath that root has to happen at
//! *every* entry point into Direct metadata resolution. It did not: the query
//! path (`load_table_context`) resolved, while `prepare_iceberg_scan` — which
//! serves `current_snapshot_id` and `scan_for_materialize_stream` — read
//! `{root}/metadata/` and hard-failed on `version-hint.text`.
//!
//! That is a *divergence* bug: one resolution step, several call sites, one of
//! them not calling it. Unit tests over `match_warehouse_table_dir` cover the
//! string matching but not the call sites, so nothing caught it and nothing
//! would catch a third entry point repeating it. This test drives the real
//! provider entry points against a warehouse root, so it fails if any of them
//! stops resolving.
//!
//! The fixture is the committed pyiceberg table at
//! `tests/fixtures/iceberg/silver/people`; its PARENT (`.../silver`) is the
//! warehouse root. With `rr:tableName "silver.people"`, the root's leaf name
//! (`silver`) mismatches the requested table (`people`), which is exactly the
//! warehouse-root branch.
//!
//! Standalone test binary: it sets `FLUREE_ICEBERG_LOCAL_ROOTS` (local tables
//! are fail-closed, see `fluree_db_iceberg::local_guard`), and process-env
//! writes must not race other tests.

#![cfg(all(feature = "iceberg", feature = "native"))]

use fluree_db_api::{FlureeBuilder, FlureeR2rmlProvider, R2rmlCreateConfig};
use futures::TryStreamExt;

/// `rr:tableName "silver.people"` — namespace `silver`, table `people`. Against
/// a `.../silver` location this is the warehouse-root case.
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
        ] .
"#;

/// The warehouse ROOT — the fixture table's parent, holding `people/` and no
/// `metadata/` of its own.
fn warehouse_root() -> String {
    format!(
        "file://{}/tests/fixtures/iceberg/silver",
        env!("CARGO_MANIFEST_DIR")
    )
}

#[tokio::test]
async fn warehouse_root_resolves_at_every_direct_entry_point() {
    let root = warehouse_root();
    // SAFETY: set at the top of the only test in this binary, before any
    // storage is built. Allowlisting the ROOT also covers the table beneath it.
    std::env::set_var(
        "FLUREE_ICEBERG_LOCAL_ROOTS",
        root.strip_prefix("file://").unwrap_or(&root),
    );

    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct("wh-people", &root, PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create warehouse-root graph source");

    // The query path resolved warehouse roots before this fix — assert it still
    // does, so a change that "fixes" the scan path by moving resolution cannot
    // pass by breaking this one.
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": "wh-people:main",
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
    });
    let rows = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("query through a warehouse root");
    let names = rows.as_array().expect("array result");
    assert_eq!(names.len(), 5, "all five fixture rows resolve: {names:?}");

    let provider = FlureeR2rmlProvider::new(&fluree);
    let gs = "wh-people:main";

    // THE REGRESSION: both of these enter `prepare_iceberg_scan`. Before the
    // fix each failed with "Failed to read version-hint.text at
    // {root}/metadata/version-hint.text" — the root has no metadata/ of its own.
    let current = provider
        .current_snapshot_id(gs, "silver.people")
        .await
        .expect("current_snapshot_id must resolve the table under the root")
        .expect("fixture table has snapshots");

    let scan = provider
        .scan_for_materialize_stream(gs, "silver.people", &[], None, None, None)
        .await
        .expect("scan_for_materialize_stream must resolve the table under the root");
    assert_eq!(
        scan.to_snapshot_id,
        Some(current),
        "the scan and the pin agree on the resolved table's snapshot"
    );
    let batches: Vec<_> = scan.stream.try_collect().await.expect("stream batches");
    let full_rows: usize = batches.iter().map(|b| b.num_rows).sum();
    assert_eq!(full_rows, 5, "full scan reads the resolved table's rows");

    // A pinned scan takes the same resolution path with a caller-supplied `to`.
    let scan = provider
        .scan_for_materialize_stream(gs, "silver.people", &[], None, Some(current), None)
        .await
        .expect("pinned scan under a warehouse root");
    assert_eq!(scan.to_snapshot_id, Some(current), "pin is honored");
    let batches: Vec<_> = scan.stream.try_collect().await.expect("stream batches");
    let pinned_rows: usize = batches.iter().map(|b| b.num_rows).sum();
    assert_eq!(pinned_rows, 5, "pinning current reads the whole table");

    // A table that does not exist under the root fails loudly rather than
    // falling back to reading the root as if it were a table.
    let missing = provider.current_snapshot_id(gs, "silver.nonesuch").await;
    assert!(
        missing.is_err(),
        "an unmatched table under a warehouse root must fail, got: {missing:?}"
    );

    eprintln!("warehouse-root resolution: all entry points resolved");
}
