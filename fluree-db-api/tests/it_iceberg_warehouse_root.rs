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
//! A root may also be laid out by namespace (`<root>/<namespace>/<table>`); the
//! second test stages one holding a same-named table in two namespaces.
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

fn fixtures_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/iceberg")
}

/// Allowlist the fixtures directory: the committed warehouse root and the staged
/// one both live under it. The guard captures the roots on first use, so every
/// test in this binary installs the SAME value before building any storage.
fn allow_fixture_roots() {
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", fixtures_dir());
}

fn copy_dir_all(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).expect("create staged dir");
    for entry in std::fs::read_dir(src).expect("read fixture dir") {
        let entry = entry.expect("dir entry");
        let to = dst.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir_all(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).expect("copy fixture file");
        }
    }
}

/// One mapping over the SAME table name in two namespaces.
const TWO_SITES_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#NorthPeople>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "north.people" ] ;
        rr:subjectMap [
            rr:template "http://example.org/north/person/{id}" ;
            rr:class ex:NorthPerson
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:name ;
            rr:objectMap [ rr:column "name" ]
        ] .

    <http://example.org/mapping#SouthPeople>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "south.people" ] ;
        rr:subjectMap [
            rr:template "http://example.org/south/person/{id}" ;
            rr:class ex:SouthPerson
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:name ;
            rr:objectMap [ rr:column "name" ]
        ] .

    <http://example.org/mapping#EuWestPeople>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "eu.west.people" ] ;
        rr:subjectMap [
            rr:template "http://example.org/eu-west/person/{id}" ;
            rr:class ex:EuWestPerson
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:name ;
            rr:objectMap [ rr:column "name" ]
        ] .
"#;

/// A root laid out `<root>/<namespace>/<table>` holds a same-named table in
/// several namespaces, one of them two levels deep (`eu.west` → `eu/west/`).
/// Each `rr:tableName` must read its OWN namespace's directory; matching on
/// the table name alone cannot tell them apart.
#[tokio::test]
async fn same_table_name_in_two_namespaces_resolves_by_namespace() {
    allow_fixture_roots();

    // `north/people` is the 5-row fixture; `south/people` is a different table
    // (the multi-append one) staged under the same name.
    let staging =
        tempfile::TempDir::new_in(fixtures_dir()).expect("staging dir under the fixtures root");
    copy_dir_all(
        &fixtures_dir().join("silver/people"),
        &staging.path().join("north/people"),
    );
    copy_dir_all(
        &fixtures_dir().join("silver/people_window"),
        &staging.path().join("south/people"),
    );
    copy_dir_all(
        &fixtures_dir().join("silver/people_backlog"),
        &staging.path().join("eu/west/people"),
    );
    let root = format!("file://{}", staging.path().display());

    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct("two-sites", &root, TWO_SITES_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create namespaced warehouse-root graph source");

    let count = |class: &'static str| {
        let fluree = &fluree;
        async move {
            let query = serde_json::json!({
                "@context": {"ex": "http://example.org/"},
                "from": "two-sites:main",
                "select": ["?name"],
                "where": {"@id": "?s", "@type": class, "ex:name": "?name"},
            });
            fluree
                .query_from()
                .jsonld(&query)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("query {class}: {e}"))
                .as_array()
                .expect("array result")
                .len()
        }
    };
    assert_eq!(count("ex:NorthPerson").await, 5, "north.people rows");
    assert_eq!(count("ex:SouthPerson").await, 3000, "south.people rows");
    // Same row count as `south`; the snapshot pins below tell the two apart.
    assert_eq!(count("ex:EuWestPerson").await, 3000, "eu.west.people rows");

    // The scan/pin entry point resolves by namespace too.
    let provider = FlureeR2rmlProvider::new(&fluree);
    let gs = "two-sites:main";
    let north = provider
        .current_snapshot_id(gs, "north.people")
        .await
        .expect("north.people pin");
    let south = provider
        .current_snapshot_id(gs, "south.people")
        .await
        .expect("south.people pin");
    assert_ne!(north, south, "each namespace pins its own table");
    let eu_west = provider
        .current_snapshot_id(gs, "eu.west.people")
        .await
        .expect("eu.west.people pin");
    assert!(
        eu_west != north && eu_west != south,
        "a two-level namespace pins its own table"
    );

    // A namespace directory that lacks the table is a miss, not a match on
    // another namespace's same-named table.
    std::fs::create_dir_all(staging.path().join("east")).expect("empty namespace dir");
    let fluree = FlureeBuilder::memory().build_memory();
    let mapping = TWO_SITES_R2RML.replace("south.people", "east.people");
    let config = R2rmlCreateConfig::new_direct("east-site", &root, &mapping)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create graph source");
    let missing = FlureeR2rmlProvider::new(&fluree)
        .current_snapshot_id("east-site:main", "east.people")
        .await;
    assert!(
        missing.is_err(),
        "east.people must not resolve to another namespace's table, got: {missing:?}"
    );
}

#[tokio::test]
async fn warehouse_root_resolves_at_every_direct_entry_point() {
    let root = warehouse_root();
    allow_fixture_roots();

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
