//! End-to-end test of catalog-less LOCAL Iceberg tables (`file://` Direct mode).
//!
//! Drives the REAL read stack — direct catalog resolution (with the
//! version-hint-absent listing fallback), metadata/manifest parsing, Parquet
//! reads, the R2RML query path, and the snapshot-pinned/incremental scan
//! surface — against an actual Iceberg table on the local filesystem.
//!
//! Runs in CI against COMMITTED fixtures under `tests/fixtures/iceberg/silver/`,
//! real pyiceberg-written tables:
//!
//! - `people` — two snapshots, 5 rows (alice..erin; snapshot 1 = 3 rows,
//!   snapshot 2 = +2).
//! - `people_backlog` — five 600-row appends whose first THREE snapshots have
//!   been expired, leaving only the two newest. That is the shape a materialize
//!   source is in once its watermark has fallen out of the source table's
//!   snapshot retention: a full read is forced, and no retained snapshot names a
//!   checkpoint anywhere near the start of the backlog.
//! - `people_window` — the same five appends with every snapshot RETAINED: a
//!   healthy source that got ahead of its job, so the window resolves and is
//!   read incrementally, and is too large to commit in one pass.
//! - `amounts` — decimal columns for FILTER pushdown: `ratio` is `decimal(38,9)`
//!   (BigQuery's `NUMERIC`) with values one grid step either side of 0.05 and
//!   1.05; `price` is `decimal(5,2)`, too narrow to hold a literal like 1000.
//! - `literals` — string columns a mapping reads as `xsd:integer` (`ntext`:
//!   "999", "1000", …) and `xsd:boolean` (`btext`: "1", "true", …), and a
//!   double column holding -0.0 and 0.0.
//!
//! The fixtures' metadata carries the ABSOLUTE paths they were written under
//! (`file:///tmp/...`), so reading them from a checkout also proves the
//! relocated-table location remap: the provider infers `metadata.location →
//! configured table_location` and rewrites every manifest file reference.
//!
//! Regenerate the fixtures (needs `pip install "pyiceberg[sql-sqlite,pyarrow]"`):
//!
//! ```bash
//! python3 scripts/local/write_local_iceberg_table.py /tmp/fluree-local-iceberg
//! cp -r /tmp/fluree-local-iceberg/silver/people fluree-db-api/tests/fixtures/iceberg/silver/people
//! cp -r /tmp/fluree-local-iceberg/silver/people_backlog fluree-db-api/tests/fixtures/iceberg/silver/people_backlog
//! cp -r /tmp/fluree-local-iceberg/silver/people_window fluree-db-api/tests/fixtures/iceberg/silver/people_window
//! cp -r /tmp/fluree-local-iceberg/silver/amounts fluree-db-api/tests/fixtures/iceberg/silver/amounts
//! cp -r /tmp/fluree-local-iceberg/silver/literals fluree-db-api/tests/fixtures/iceberg/silver/literals
//! ```
//!
//! `FLUREE_LOCAL_ICEBERG_TABLE=file:///path/to/table` overrides the `people`
//! fixture to run those tests against any table with the same shape.
//!
//! Local tables are fail-closed behind `FLUREE_ICEBERG_LOCAL_ROOTS` (see
//! `fluree_db_iceberg::local_guard`), so each test allowlists the fixtures
//! directory before touching the stack — which also keeps it honest: a read
//! that escaped the fixtures directory would be refused here just as it would
//! in a deployment.

#![cfg(all(feature = "iceberg", feature = "native"))]

use fluree_db_api::{ApiError, FlureeBuilder, FlureeR2rmlProvider, R2rmlCreateConfig};
use futures::TryStreamExt;

#[path = "support/span_capture.rs"]
mod span_capture;

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

/// The committed expired-history fixture — see the module doc.
fn backlog_table_location() -> String {
    format!(
        "file://{}/tests/fixtures/iceberg/silver/people_backlog",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// The directory every committed fixture lives under — the one root the guard
/// allowlists, so a copy staged beneath it is readable without a second root.
fn fixtures_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/iceberg")
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

/// Allowlist the fixtures directory (every table this file reads lives under it),
/// plus the override table's directory when one is set. Must run before anything
/// builds Iceberg storage — the guard captures the roots on first use — and every
/// test in this binary must install the SAME value, because the first to touch the
/// guard decides for all of them.
fn allow_fixture_roots() {
    let mut roots = vec![format!(
        "{}/tests/fixtures/iceberg",
        env!("CARGO_MANIFEST_DIR")
    )];
    if let Ok(loc) = std::env::var("FLUREE_LOCAL_ICEBERG_TABLE") {
        let loc = loc.trim();
        if !loc.is_empty() {
            roots.push(loc.strip_prefix("file://").unwrap_or(loc).to_string());
        }
    }
    // SAFETY: set at the top of the test, before any storage or scan is built.
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", roots.join(":"));
}

#[tokio::test]
async fn local_table_end_to_end() {
    let location = table_location();
    allow_fixture_roots();
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

/// Time-pinned reads of a graph source. The fixture has two snapshots (3 rows,
/// then +2), so a pin that were silently dropped would still return all five.
/// Both public routes are covered — the `from`-driven dataset builder and the
/// lazy `graph_at()` handle, whose graph-source fallback used to drop the pin —
/// and a COUNT is checked against its scan so the manifest shortcut cannot
/// answer from a different snapshot than the rows.
/// A SPARQL connection query carrying governance options — what every server
/// SPARQL request is, even with no policy headers — reads a graph source named
/// in `FROM`. That branch once ran without the graph-source providers and
/// answered with no rows and no error.
#[tokio::test]
async fn a_sparql_connection_query_with_governance_options_reads_a_graph_source() {
    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct("governed-people", &location, PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create local-file graph source");

    let sparql = "SELECT ?name FROM <governed-people:main> \
                  WHERE { ?s <http://example.org/name> ?name }";
    let ask = || {
        fluree
            .query_from()
            .sparql(sparql)
            .connection_opts(fluree_db_api::GovernanceOptions::default())
    };

    let result = ask().execute().await.expect("execute");
    assert_eq!(result.row_count(), 5, "execute");

    let formatted = ask().execute_formatted().await.expect("execute_formatted");
    assert_eq!(
        formatted["results"]["bindings"].as_array().map(Vec::len),
        Some(5),
        "execute_formatted: {formatted}"
    );

    let text = ask()
        .execute_formatted_string()
        .await
        .expect("execute_formatted_string");
    assert_eq!(text.matches("\"name\"").count(), 1 + 5, "{text}");

    let tracked = ask()
        .execute_tracked()
        .await
        .unwrap_or_else(|e| panic!("execute_tracked: {}", e.error));
    assert_eq!(
        tracked.result["results"]["bindings"]
            .as_array()
            .map(Vec::len),
        Some(5),
        "execute_tracked: {}",
        tracked.result
    );
}

#[tokio::test]
async fn pinned_query_on_a_graph_source_reads_that_snapshot() {
    use fluree_db_api::TimeSpec;

    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct("local-people-pinned", &location, PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create local-file graph source");
    let alias = "local-people-pinned:main";

    // The two snapshots' ids and commit times, straight from the table metadata.
    let (first, first_ms, current_ms) = {
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
            .expect("direct load_table");
        let bytes = SendIcebergStorage::read(storage.as_ref(), &resp.metadata_location)
            .await
            .expect("read metadata json");
        let meta = TableMetadata::from_json_str(std::str::from_utf8(&bytes).expect("utf8"))
            .expect("parse metadata");
        let current = meta.current_snapshot().expect("current snapshot");
        let first = meta
            .snapshot(
                current
                    .parent_snapshot_id
                    .expect("two snapshots in fixture"),
            )
            .expect("first snapshot");
        (first.snapshot_id, first.timestamp_ms, current.timestamp_ms)
    };
    assert!(
        first_ms < current_ms,
        "fixture snapshots are distinct in time"
    );
    let iso = |ms: i64| {
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
            .expect("valid ms")
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    };
    // An instant strictly between the two commits selects the first snapshot;
    // the instant of the first commit itself does too (at-or-before).
    let between = iso(i64::midpoint(first_ms, current_ms));
    let at_first = iso(first_ms);
    let before_first = iso(first_ms - 1);

    let names = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
    });
    let count = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(count ?s)"],
        "where": {"@id": "?s", "ex:name": "?name"},
    });
    let rows_of = |v: &serde_json::Value| v.as_array().map(Vec::len);
    let count_of = |v: &serde_json::Value| v[0][0].as_u64().or_else(|| v[0].as_u64());
    // SPARQL twins (CLAUDE.md: the two surfaces share an IR but not an entry
    // point — `FROM <alias@…>` lexes the pin out of an IRI).
    let sparql_names = |from: &str| {
        format!("SELECT ?name FROM <{from}> WHERE {{ ?s <http://example.org/name> ?name }}")
    };
    let sparql_count = |from: &str| {
        format!(
            "SELECT (COUNT(?s) AS ?n) FROM <{from}> WHERE {{ ?s <http://example.org/name> ?name }}"
        )
    };
    let bindings_of = |v: &serde_json::Value| v["results"]["bindings"].as_array().map(Vec::len);
    let binding_count_of = |v: &serde_json::Value| {
        v["results"]["bindings"][0]["n"]["value"]
            .as_str()
            .map(str::to_string)
    };

    // Unpinned control: both routes see the current snapshot's five rows.
    let latest = fluree
        .graph(alias)
        .query()
        .jsonld(&names)
        .execute_formatted()
        .await
        .expect("latest query");
    assert_eq!(rows_of(&latest), Some(5), "got: {latest}");
    // ...and advertise no `t`: the genesis view's 0 is not a state a client
    // could pin the next page to.
    let raw = fluree
        .graph(alias)
        .query()
        .jsonld(&names)
        .execute()
        .await
        .expect("latest raw query");
    assert_eq!(raw.t, None, "a virtual source has no t to pin on");

    // --- Honored pins: the first snapshot's three rows, on both routes, with
    //     the COUNT agreeing with the scan.
    let honored: Vec<(String, TimeSpec)> = vec![
        (format!("snapshot:{first}"), TimeSpec::AtSnapshot(first)),
        (format!("time:{between}"), TimeSpec::AtTime(between.clone())),
        (format!("iso:{between}"), TimeSpec::AtTime(between.clone())),
        (
            format!("time:{at_first}"),
            TimeSpec::AtTime(at_first.clone()),
        ),
        (
            format!("recorded:{between}"),
            TimeSpec::AtRecorded(between.clone()),
        ),
    ];
    for (suffix, spec) in &honored {
        let mut from_names = names.clone();
        from_names["from"] = serde_json::Value::String(format!("{alias}@{suffix}"));
        let rows = fluree
            .query_from()
            .jsonld(&from_names)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("from @{suffix}: {e}"));
        assert_eq!(rows_of(&rows), Some(3), "from @{suffix}: {rows}");

        let mut from_count = count.clone();
        from_count["from"] = serde_json::Value::String(format!("{alias}@{suffix}"));
        let n = fluree
            .query_from()
            .jsonld(&from_count)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("count from @{suffix}: {e}"));
        assert_eq!(count_of(&n), Some(3), "count from @{suffix}: {n}");

        let rows = fluree
            .graph_at(alias, spec.clone())
            .query()
            .jsonld(&names)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("graph_at({spec:?}): {e}"));
        assert_eq!(rows_of(&rows), Some(3), "graph_at({spec:?}): {rows}");

        // The tracked cores push the pin separately from the plain ones; a
        // request with tracking headers runs through them.
        let tracked = fluree
            .query_from()
            .jsonld(&from_names)
            .track_all()
            .execute_tracked()
            .await
            .unwrap_or_else(|e| panic!("tracked from @{suffix}: {e:?}"));
        assert_eq!(
            rows_of(&tracked.result),
            Some(3),
            "tracked from @{suffix}: {tracked:?}"
        );
        let tracked = fluree
            .graph_at(alias, spec.clone())
            .query()
            .jsonld(&names)
            .track_all()
            .execute_tracked()
            .await
            .unwrap_or_else(|e| panic!("tracked graph_at({spec:?}): {e:?}"));
        assert_eq!(
            rows_of(&tracked.result),
            Some(3),
            "tracked graph_at({spec:?}): {tracked:?}"
        );
        // A second (named) view makes it a dataset, which has its own core.
        let mut dataset_names = from_names.clone();
        dataset_names["fromNamed"] = serde_json::json!([format!("{alias}@{suffix}")]);
        let tracked = fluree
            .query_from()
            .jsonld(&dataset_names)
            .track_all()
            .execute_tracked()
            .await
            .unwrap_or_else(|e| panic!("tracked dataset @{suffix}: {e:?}"));
        assert_eq!(
            rows_of(&tracked.result),
            Some(3),
            "tracked dataset @{suffix}: {tracked:?}"
        );

        let from = format!("{alias}@{suffix}");
        let rows = fluree
            .query_from()
            .sparql(&sparql_names(&from))
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("SPARQL FROM <{from}>: {e}"));
        assert_eq!(bindings_of(&rows), Some(3), "SPARQL FROM <{from}>: {rows}");
        let n = fluree
            .query_from()
            .sparql(&sparql_count(&from))
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("SPARQL count FROM <{from}>: {e}"));
        assert_eq!(
            binding_count_of(&n).as_deref(),
            Some("3"),
            "SPARQL count FROM <{from}>: {n}"
        );
    }

    // The pin applies when the query addresses the source itself in an
    // explicit GRAPH block (which skips the auto-wrapping the bare form gets).
    let mut got = Vec::new();
    for from in [alias.to_string(), format!("{alias}@snapshot:{first}")] {
        let rows = fluree
            .query_from()
            .sparql(&format!(
                "SELECT ?name FROM <{from}> WHERE {{ GRAPH <{alias}> {{ ?s <http://example.org/name> ?name }} }}"
            ))
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("GRAPH <{alias}> FROM <{from}>: {e}"));
        got.push(bindings_of(&rows));
    }
    assert_eq!(
        got,
        [Some(5), Some(3)],
        "explicit GRAPH block: latest, pinned"
    );

    // --- Refused pins. Each must be an error on both routes — never the
    //     current snapshot, never the oldest one — and a 400: the caller asked
    //     for something the source cannot answer.
    let expect_refused =
        |result: Result<serde_json::Value, ApiError>, route: &str, needle: &str| {
            let err = match result {
                Ok(rows) => panic!("{route}: pinned query returned {rows} instead of an error"),
                Err(err) => err,
            };
            assert!(
                err.to_string().contains(needle),
                "{route}: unexpected error: {err}"
            );
            assert_eq!(err.status_code(), 400, "{route}: {err}");
        };
    let refused: Vec<(String, TimeSpec, &str)> = vec![
        // Before the oldest retained snapshot: typed, names the oldest.
        (
            format!("iso:{before_first}"),
            TimeSpec::AtTime(before_first.clone()),
            "no snapshot of table 'silver.people' at or before",
        ),
        // An id no snapshot has.
        (
            "snapshot:999".to_string(),
            TimeSpec::AtSnapshot(999),
            "snapshot 999 not found",
        ),
        // Ledger-only selectors have no meaning against a table.
        (
            "t:1".to_string(),
            TimeSpec::AtT(1),
            "Graph sources have no transaction numbers or commit hashes",
        ),
        (
            "commit:abcdef".to_string(),
            TimeSpec::AtCommit("abcdef".to_string()),
            "Graph sources have no transaction numbers or commit hashes",
        ),
    ];
    for (suffix, spec, needle) in &refused {
        let mut from_names = names.clone();
        from_names["from"] = serde_json::Value::String(format!("{alias}@{suffix}"));
        let result = fluree
            .query_from()
            .jsonld(&from_names)
            .execute_formatted()
            .await;
        expect_refused(result, &format!("from @{suffix}"), needle);

        let mut from_count = count.clone();
        from_count["from"] = serde_json::Value::String(format!("{alias}@{suffix}"));
        let result = fluree
            .query_from()
            .jsonld(&from_count)
            .execute_formatted()
            .await;
        expect_refused(result, &format!("count from @{suffix}"), needle);

        let result = fluree
            .graph_at(alias, spec.clone())
            .query()
            .jsonld(&names)
            .execute_formatted()
            .await;
        expect_refused(result, &format!("graph_at({spec:?})"), needle);

        let from = format!("{alias}@{suffix}");
        let result = fluree
            .query_from()
            .sparql(&sparql_names(&from))
            .execute_formatted()
            .await;
        expect_refused(result, &format!("SPARQL FROM <{from}>"), needle);
    }

    // One source pinned to two states in one query is a conflict, not a race.
    let two_states = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": format!("{alias}@snapshot:{first}"),
        "fromNamed": [format!("{alias}@iso:{}", iso(current_ms))],
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
    });
    let result = fluree
        .query_from()
        .jsonld(&two_states)
        .execute_formatted()
        .await;
    expect_refused(result, "two pins", "two different states");

    // ...and so is one reference pinned and another unpinned: the pin is per
    // source for the whole query, so the unpinned view must not silently follow it.
    let pinned_and_latest = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": [format!("{alias}@snapshot:{first}"), alias],
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
    });
    let result = fluree
        .query_from()
        .jsonld(&pinned_and_latest)
        .execute_formatted()
        .await;
    expect_refused(result, "pinned + latest", "two different states");

    // A graph source that is not an Iceberg-backed table (here a BM25 index)
    // reads its current state only: a pin on it is refused, not ignored.
    let ledger = fluree.create_ledger("bm25-src").await.expect("ledger");
    fluree
        .insert(
            ledger,
            &serde_json::json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [{"@id": "ex:doc1", "@type": "ex:Doc", "ex:title": "Hello world"}]
            }),
        )
        .await
        .expect("seed bm25 ledger");
    fluree
        .create_full_text_index(fluree_db_api::Bm25CreateConfig::new(
            "bm25-pinned",
            "bm25-src:main",
            serde_json::json!({
                "@context": {"ex": "http://example.org/"},
                "where": [{"@id": "?x", "@type": "ex:Doc", "ex:title": "?title"}],
                "select": {"?x": ["@id", "ex:title"]}
            }),
        ))
        .await
        .expect("create bm25 index");
    for suffix in [format!("time:{between}"), "snapshot:1".to_string()] {
        let mut on_bm25 = names.clone();
        on_bm25["from"] = serde_json::Value::String(format!("bm25-pinned:main@{suffix}"));
        let result = fluree
            .query_from()
            .jsonld(&on_bm25)
            .execute_formatted()
            .await;
        expect_refused(
            result,
            &format!("bm25 @{suffix}"),
            "does not support time-pinned reads",
        );
    }

    // A table with NO snapshots yet (staged as the fixture's pre-first-commit
    // metadata alone): unpinned reads are empty, and every pin is an error that
    // says so — there is no oldest snapshot to name, and nothing to fall back to.
    let staging =
        tempfile::TempDir::new_in(fixtures_dir()).expect("staging dir under the fixtures root");
    let empty_dir = staging.path().join("people_empty");
    let empty_meta = empty_dir.join("metadata");
    std::fs::create_dir_all(&empty_meta).expect("empty table metadata dir");
    let first_meta = std::fs::read_dir(fixtures_dir().join("silver/people/metadata"))
        .expect("people metadata dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
        .min()
        .expect("a 00000 metadata file");
    std::fs::copy(
        &first_meta,
        empty_meta.join(first_meta.file_name().expect("name")),
    )
    .expect("stage the empty-table metadata");
    let empty_alias = "local-people-empty:main";
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct(
                "local-people-empty",
                format!("file://{}", empty_dir.display()),
                PEOPLE_R2RML,
            )
            .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create empty-table graph source");
    let rows = fluree
        .graph(empty_alias)
        .query()
        .jsonld(&names)
        .execute_formatted()
        .await
        .expect("unpinned read of an empty table");
    assert_eq!(rows_of(&rows), Some(0), "got: {rows}");
    for (suffix, spec, needle) in [
        (
            format!("iso:{between}"),
            TimeSpec::AtTime(between.clone()),
            "the table has no snapshots",
        ),
        (
            format!("snapshot:{first}"),
            TimeSpec::AtSnapshot(first),
            "not found for table",
        ),
    ] {
        let mut from_names = names.clone();
        from_names["from"] = serde_json::Value::String(format!("{empty_alias}@{suffix}"));
        let result = fluree
            .query_from()
            .jsonld(&from_names)
            .execute_formatted()
            .await;
        expect_refused(result, &format!("empty table, from @{suffix}"), needle);
        let result = fluree
            .graph_at(empty_alias, spec.clone())
            .query()
            .jsonld(&names)
            .execute_formatted()
            .await;
        expect_refused(result, &format!("empty table, graph_at({spec:?})"), needle);
    }

    // `@snapshot:` on a native ledger is refused; it is not a ledger selector.
    fluree.create_ledger("plain-ledger").await.expect("ledger");
    let mut on_ledger = names.clone();
    on_ledger["from"] = serde_json::Value::String("plain-ledger:main@snapshot:1".to_string());
    let result = fluree
        .query_from()
        .jsonld(&on_ledger)
        .execute_formatted()
        .await;
    expect_refused(
        result,
        "ledger @snapshot",
        "selects a graph source's table snapshot",
    );
    on_ledger["from"] = serde_json::Value::String("plain-ledger:main@iso:not-a-time".to_string());
    let result = fluree
        .query_from()
        .jsonld(&on_ledger)
        .execute_formatted()
        .await;
    expect_refused(
        result,
        "ledger @iso malformed",
        "Invalid ISO-8601 timestamp for time travel",
    );
}

/// The lake face of the stats kernel: the same table, profiled through the
/// scan the virtual graph reads, pinned to its current snapshot.
#[tokio::test]
async fn local_table_profiles_through_the_scan() {
    use fluree_db_api::profile::ProfileRequest;
    use fluree_db_stats::ValueKind;

    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct("local-people-profile", &location, PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create local-file graph source");
    let gs = "local-people-profile:main";

    // Every column, no grouping.
    let all = fluree
        .profile_table(
            gs,
            "silver.people",
            &ProfileRequest::columns(Vec::<String>::new()),
        )
        .await
        .expect("profile all columns");
    assert!(all.snapshot_id.is_some(), "pinned to a snapshot");
    assert!(all.skipped.is_empty(), "{:?}", all.skipped);
    let names: Vec<&str> = all.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "score", "active"]);

    let id = &all.columns[0].summary;
    assert_eq!(id.count, 5);
    assert_eq!(id.distinct, 5);
    assert!(id.distinct_is_exact);
    assert!(id.key_candidate, "{id:?}");
    assert_eq!(id.kinds.get(&ValueKind::Int), Some(&5));

    let name = &all.columns[1].summary;
    let text = name.text.as_ref().expect("text summary");
    assert_eq!(text.min.as_deref(), Some("alice"));
    assert!(name.top_values.iter().any(|v| v.value == "erin"));

    let score = &all.columns[2].summary;
    let num = score.numeric.as_ref().expect("numeric summary");
    assert_eq!(num.count, score.count - score.null_count);
    assert!(num.p50.is_some());

    let active = &all.columns[3].summary;
    assert!(active.distinct <= 2);
    assert!(active.top_values_exact);

    // One column grouped by another, plus a column that does not exist.
    let grouped = fluree
        .profile_table(
            gs,
            "silver.people",
            &ProfileRequest::columns(["score", "nope"]).group_by(["active"]),
        )
        .await
        .expect("grouped profile");
    assert_eq!(grouped.skipped.len(), 1);
    assert_eq!(grouped.skipped[0].name, "nope");
    assert_eq!(grouped.columns.len(), 1);
    let g = grouped.columns[0].grouped.as_ref().expect("grouped");
    assert!(g.group_count >= 1 && g.group_count <= 2, "{g:?}");
    assert_eq!(g.total.count, 5);
    assert_eq!(
        g.groups.iter().map(|x| x.summary.count).sum::<u64>() + g.ungrouped,
        5
    );

    // Every named column unknown: an empty projection reads the whole
    // table, so the scan is skipped rather than run for nothing. The
    // report still says what was asked for and what was not there.
    let none = fluree
        .profile_table(
            gs,
            "silver.people",
            &ProfileRequest::columns(["nope", "also-nope"]),
        )
        .await
        .expect("profile with no known columns");
    assert!(none.columns.is_empty(), "{:?}", none.columns);
    assert_eq!(none.skipped.len(), 2);
    assert!(none.snapshot_id.is_some(), "still pinned to a snapshot");
}

/// The expired-history backlog, drained through the real state ledger in bounded
/// passes.
///
/// This is the composition no unit test crosses, and both production surprises in
/// this feature's history were composition failures: a checkpoint walk that needed
/// intact history on precisely the tables that had none, and a budget whose
/// arithmetic was correct and too large to ever engage. The loop below crosses every
/// seam at once. The ceiling the builder configured sizes the budget; the first pass
/// cuts, finds no retained snapshot to checkpoint at, and records a cursor instead;
/// later passes resume above it; the complete pass advances the snapshot and retires
/// the cursor; and the poll after that is an ordinary empty incremental scan.
///
/// Assumes `FLUREE_MATERIALIZE_MAX_ROWS_PER_PASS` and
/// `FLUREE_MATERIALIZE_FLAKE_BYTES_PER_ROW` are unset, as they are in CI.
#[tokio::test]
async fn an_expired_history_table_drains_in_bounded_passes() {
    allow_fixture_roots();
    let location = backlog_table_location();

    // File-backed so the indexer drains novelty between passes, with a ceiling
    // chosen so the derived budget lands between one and two 600-row commits: the
    // 3,000-row table then drains in three passes of 1200, 1200 and 600 rows.
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .with_indexing_thresholds(64 * 1024, 500 * 1024)
        .build()
        .expect("file-backed fluree with a background indexer");
    let indexer = fluree
        .indexer_handle()
        .expect("file builder starts background indexing")
        .clone();

    let mapping = PEOPLE_R2RML.replace("silver.people", "silver.people_backlog");
    let config = R2rmlCreateConfig::new_direct("backlog", &location, &mapping)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create backlog graph source");
    let (gs, table, target) = (
        "backlog:main",
        "silver.people_backlog",
        "backlog_native:main",
    );
    const STATE: &str = "fluree_materialize_state:main";

    let provider = FlureeR2rmlProvider::new(&fluree);
    let budget = provider.pass_row_budget();
    assert!(
        (601..=1200).contains(&budget),
        "the budget derived from the configured ceiling must cut inside the second \
         commit, or the pass layout below is wrong: budget={budget}"
    );
    let head = provider
        .current_snapshot_id(gs, table)
        .await
        .expect("current snapshot")
        .expect("fixture has snapshots");

    // (rows this pass, cursor after it, snapshot watermark after it). Sequence
    // numbers are the fixture's: commits 1..=5, so the cuts land at 2 and 4.
    let expected = [
        (1200, Some(2), None),
        (1200, Some(4), None),
        (600, None, Some(head)),
    ];
    for (pass, (rows, cursor, watermark)) in expected.into_iter().enumerate() {
        // One poll as the tracking worker runs it: a target the novelty ceiling
        // deferred is retried once the indexer has drained what deferred it. The
        // point of this test is that each RETRY resumes where the last pass got to.
        let mut attempts = 0;
        let result = loop {
            attempts += 1;
            match fluree
                .materialize_r2rml_graph_source(gs, target, false)
                .await
            {
                Ok(r) => break r,
                Err(ApiError::MaterializePartial { detail, .. }) if attempts < 50 => {
                    eprintln!("pass {}: deferred ({detail}); draining novelty", pass + 1);
                    indexer
                        .wait_for_idle(&fluree_db_api::LedgerId::parse(target).unwrap())
                        .await;
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                Err(e) => panic!("pass {} failed: {e}", pass + 1),
            }
        };
        eprintln!(
            "pass {}: rows_read={} incremental={} to={:?}",
            pass + 1,
            result.rows_read,
            result.incremental,
            result.to_snapshot_id
        );
        assert!(
            !result.incremental,
            "an expired watermark forces a full read"
        );
        assert_eq!(
            result.rows_read,
            rows,
            "pass {} read the wrong prefix",
            pass + 1
        );
        assert_eq!(
            fluree
                .materialize_sequence_cursor(STATE, gs, target, table)
                .await
                .expect("read cursor"),
            cursor,
            "cursor after pass {}",
            pass + 1
        );
        assert_eq!(
            fluree
                .materialize_watermark(STATE, gs, target, table)
                .await
                .expect("read watermark"),
            watermark,
            "snapshot watermark after pass {} — it must not move until the drain completes",
            pass + 1
        );
        // Let the indexer drain the target before the next pass so the next commit
        // has headroom; the retry loop above covers the case where it has not.
        indexer
            .wait_for_idle(&fluree_db_api::LedgerId::parse(target).unwrap())
            .await;
    }

    // Steady state: the watermark resolves, the window is empty, nothing commits.
    let idle = fluree
        .materialize_r2rml_graph_source(gs, target, false)
        .await
        .expect("steady-state poll");
    assert!(
        idle.incremental,
        "with the watermark at the head the poll is incremental"
    );
    assert_eq!(idle.rows_read, 0);
    assert!(!idle.committed, "an empty fresh window commits nothing");

    // Every row of every commit landed exactly once, including the rows either
    // side of each cut — the seams are where a resume that repeats or skips a
    // commit would show.
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": target,
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
        "limit": 10_000,
    });
    let rows = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("query the materialized target");
    let names: std::collections::BTreeSet<String> = rows
        .as_array()
        .expect("array result")
        .iter()
        .map(std::string::ToString::to_string)
        .collect();
    assert_eq!(
        names.len(),
        3000,
        "3,000 distinct people across three passes"
    );
    for edge in [
        "person-0001",
        "person-1200",
        "person-1201",
        "person-2400",
        "person-2401",
        "person-3000",
    ] {
        assert!(names.iter().any(|n| n.contains(edge)), "missing {edge}");
    }
}

/// A healthy source's backlog, drained INCREMENTALLY in bounded passes.
///
/// The sibling of the expired-history drain above, for the other half of the
/// same failure. A window is committed whole or not at all, so an incremental
/// window over the novelty ceiling is deferred on every poll, advances nothing,
/// and grows by a poll each time — until the watermark falls out of retention
/// and the job pays for a full read of the entire table to recover a backlog
/// that was a few snapshots wide. The cap stops short of the head instead, and
/// this test crosses every seam it touches: the budget the builder's ceiling
/// derives, the window sized from snapshot summaries, the incremental plan of
/// the capped window, the watermark landing at the capped snapshot rather than
/// the head, and the next poll resuming from it.
///
/// The source has to ADVANCE between passes for the window to exist at all, so
/// the fixture is staged as a private copy whose later metadata files are held
/// back for the first poll and restored afterwards. Clearing the metadata cache
/// between the two stands in for the TTL a real worker would wait out.
///
/// Assumes `FLUREE_MATERIALIZE_MAX_ROWS_PER_PASS` and
/// `FLUREE_MATERIALIZE_FLAKE_BYTES_PER_ROW` are unset, as they are in CI.
#[tokio::test]
async fn an_incremental_backlog_drains_in_bounded_passes() {
    allow_fixture_roots();

    // Stage the table, then hold back every metadata file after the first commit
    // so the job's first poll sees a one-commit table.
    let staging =
        tempfile::TempDir::new_in(fixtures_dir()).expect("staging dir under the fixtures root");
    let table_dir = staging.path().join("people_window");
    copy_dir_all(&fixtures_dir().join("silver/people_window"), &table_dir);
    let metadata_dir = table_dir.join("metadata");
    let held_dir = staging.path().join("held");
    std::fs::create_dir_all(&held_dir).expect("held dir");
    let mut metadata_files: Vec<std::path::PathBuf> = std::fs::read_dir(&metadata_dir)
        .expect("metadata dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
        .collect();
    metadata_files.sort();
    // `00000` is the empty table and `00001` the first commit; everything after
    // is the backlog the source builds up while the job is away.
    assert!(
        metadata_files.len() >= 4,
        "fixture has {} metadata files",
        metadata_files.len()
    );
    let held: Vec<(std::path::PathBuf, std::path::PathBuf)> = metadata_files
        .iter()
        .skip(2)
        .map(|p| (p.clone(), held_dir.join(p.file_name().expect("file name"))))
        .collect();
    for (from, to) in &held {
        std::fs::rename(from, to).expect("hold back a metadata file");
    }
    // The chain the source will end up at, oldest first, read from its final
    // metadata rather than hard-coded — pyiceberg assigns the ids.
    let final_meta: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&held.last().expect("held files").1).expect("read final metadata"),
    )
    .expect("parse final metadata");
    let mut chain: Vec<(i64, i64)> = final_meta["snapshots"]
        .as_array()
        .expect("snapshots")
        .iter()
        .map(|s| {
            (
                s["sequence-number"].as_i64().expect("sequence"),
                s["snapshot-id"].as_i64().expect("snapshot id"),
            )
        })
        .collect();
    chain.sort();
    let chain: Vec<i64> = chain.into_iter().map(|(_, id)| id).collect();
    assert_eq!(chain.len(), 5, "fixture is five commits");

    let location = format!("file://{}", table_dir.display());
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .with_indexing_thresholds(64 * 1024, 500 * 1024)
        .build()
        .expect("file-backed fluree with a background indexer");
    let indexer = fluree
        .indexer_handle()
        .expect("file builder starts background indexing")
        .clone();

    let mapping = PEOPLE_R2RML.replace("silver.people", "silver.people_window");
    let config = R2rmlCreateConfig::new_direct("window", &location, &mapping)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create window graph source");
    let (gs, table, target) = ("window:main", "silver.people_window", "window_native:main");
    const STATE: &str = "fluree_materialize_state:main";

    // The same ceiling as the full-read drain, for the same reason: the derived
    // budget must land between one and two 600-row commits for the pass layout
    // below to be the one the cap produces.
    let budget = FlureeR2rmlProvider::new(&fluree).pass_row_budget();
    assert!(
        (601..=1200).contains(&budget),
        "the budget derived from the configured ceiling must cut inside the second \
         commit, or the pass layout below is wrong: budget={budget}"
    );

    // One poll as the tracking worker runs it, retried once the indexer has
    // drained whatever deferred it.
    let poll = |pass: usize| {
        let fluree = &fluree;
        let indexer = &indexer;
        async move {
            let mut attempts = 0;
            loop {
                attempts += 1;
                match fluree
                    .materialize_r2rml_graph_source(gs, target, false)
                    .await
                {
                    Ok(r) => break r,
                    Err(ApiError::MaterializePartial { detail, .. }) if attempts < 50 => {
                        eprintln!("pass {pass}: deferred ({detail}); draining novelty");
                        indexer
                            .wait_for_idle(&fluree_db_api::LedgerId::parse(target).unwrap())
                            .await;
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                    Err(e) => panic!("pass {pass} failed: {e}"),
                }
            }
        }
    };
    let watermark = || async {
        fluree
            .materialize_watermark(STATE, gs, target, table)
            .await
            .expect("read watermark")
    };

    // Pass 1: a first run against a one-commit table is an ordinary initial full
    // read, well under budget, and it leaves the watermark at that commit.
    let first = poll(1).await;
    assert!(!first.incremental, "a first run is a full read");
    assert_eq!(first.rows_read, 600);
    assert_eq!(watermark().await, Some(chain[0]));
    indexer
        .wait_for_idle(&fluree_db_api::LedgerId::parse(target).unwrap())
        .await;

    // The source commits four more times while the job is away.
    for (from, to) in &held {
        std::fs::rename(to, from).expect("restore a metadata file");
    }
    fluree.r2rml_cache().clear().await;

    // (rows this pass, snapshot watermark after it). The window is (1, 5] =
    // 2,400 rows against a budget between 601 and 1,200: the budget runs out
    // inside commit 3, which is kept whole; then inside the head, so the pass
    // stops at the boundary below it; then the rest fits.
    let expected = [(1200, chain[2]), (600, chain[3]), (600, chain[4])];
    for (i, (rows, mark)) in expected.into_iter().enumerate() {
        let pass = i + 2;
        let result = poll(pass).await;
        eprintln!(
            "pass {pass}: rows_read={} incremental={} to={:?}",
            result.rows_read, result.incremental, result.to_snapshot_id
        );
        assert!(
            result.incremental,
            "pass {pass}: a resolvable watermark over an append-only window is read \
             incrementally — a full read here means the cap broke the incremental path"
        );
        assert_eq!(result.rows_read, rows, "pass {pass} read the wrong prefix");
        assert_eq!(
            result.to_snapshot_id,
            Some(mark),
            "pass {pass} reported the wrong window end"
        );
        assert_eq!(
            watermark().await,
            Some(mark),
            "snapshot watermark after pass {pass} — it must land at the capped snapshot, \
             not the head"
        );
        // A capped incremental pass is COMPLETE for its window: no cursor.
        assert_eq!(
            fluree
                .materialize_sequence_cursor(STATE, gs, target, table)
                .await
                .expect("read cursor"),
            None,
            "cursor after pass {pass}"
        );
        indexer
            .wait_for_idle(&fluree_db_api::LedgerId::parse(target).unwrap())
            .await;
    }

    // Steady state: the watermark is at the head, the window is empty, nothing
    // commits.
    let idle = poll(5).await;
    assert!(idle.incremental);
    assert_eq!(idle.rows_read, 0);
    assert!(!idle.committed, "an empty fresh window commits nothing");

    // Every row of every commit landed exactly once — the seams between passes
    // are where a cut that skipped or repeated a snapshot would show.
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": target,
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
        "limit": 10_000,
    });
    let rows = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("query the target");
    // A one-variable select comes back as bare values or one-element rows,
    // depending on the formatter; accept either.
    let names: Vec<&str> = rows
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| {
            r.as_str()
                .or_else(|| {
                    r.as_array()
                        .and_then(|a| a.first())
                        .and_then(|v| v.as_str())
                })
                .expect("name")
        })
        .collect();
    assert_eq!(names.len(), 3000, "every row of every commit, exactly once");
    let distinct: std::collections::BTreeSet<&str> = names.iter().copied().collect();
    assert_eq!(distinct.len(), 3000);
    for i in 1..=3000 {
        let name = format!("person-{i:04}");
        assert!(distinct.contains(name.as_str()), "{name} missing");
    }
}

/// A full read pinned to a snapshot returns that snapshot whole, whatever the
/// per-pass budget. The budget paces the tracking worker's unpinned passes; a
/// consumer that pins a snapshot and gets a prefix back has silently lost rows.
///
/// Both routes a cut can take are crossed. On `people_window` every snapshot is
/// retained, so a cut checkpoints at an earlier snapshot and reports it as the
/// read's `to` with no sequence — nothing a caller can tell from a whole read.
/// On `people_backlog` nothing near the start is retained, so a cut records a
/// sequence cursor instead. A pin resuming from a cursor reads everything above
/// it, which crosses the cursor path's own cut.
///
/// Assumes `FLUREE_MATERIALIZE_MAX_ROWS_PER_PASS` and
/// `FLUREE_MATERIALIZE_FLAKE_BYTES_PER_ROW` are unset, as they are in CI.
#[tokio::test]
async fn a_pinned_full_read_is_never_cut_by_the_row_budget() {
    allow_fixture_roots();
    // The ceiling the drain tests use, so the budget cuts inside the second
    // 600-row commit of a 3,000-row table.
    let fluree = FlureeBuilder::memory()
        .with_indexing_thresholds(64 * 1024, 500 * 1024)
        .build_memory();
    let provider = FlureeR2rmlProvider::new(&fluree);
    let budget = provider.pass_row_budget();
    assert!(
        (601..=1200).contains(&budget),
        "an unpinned read of these tables must be cut for this test to mean \
         anything: budget={budget}"
    );

    async fn read_all(stream: fluree_db_query::r2rml::ColumnBatchStream) -> usize {
        let batches: Vec<_> = stream.try_collect().await.expect("stream batches");
        batches.iter().map(|b| b.num_rows).sum()
    }

    for fixture in ["people_window", "people_backlog"] {
        let location = format!(
            "file://{}/tests/fixtures/iceberg/silver/{fixture}",
            env!("CARGO_MANIFEST_DIR")
        );
        let table = format!("silver.{fixture}");
        let mapping = PEOPLE_R2RML.replace("silver.people", &table);
        let config = R2rmlCreateConfig::new_direct(fixture, &location, &mapping)
            .with_mapping_media_type("text/turtle");
        fluree
            .create_r2rml_graph_source(config)
            .await
            .expect("create graph source");
        let gs = format!("{fixture}:main");
        let head = provider
            .current_snapshot_id(&gs, &table)
            .await
            .expect("current snapshot")
            .expect("fixture has snapshots");

        let scan = provider
            .scan_for_materialize_stream(&gs, &table, &[], None, Some(head), None)
            .await
            .expect("pinned full read");
        assert!(!scan.incremental, "{fixture}: no window, so a full read");
        assert_eq!(
            scan.to_snapshot_id,
            Some(head),
            "{fixture}: reports its pin"
        );
        assert_eq!(scan.to_sequence, None, "{fixture}: not a prefix");
        assert_eq!(
            read_all(scan.stream).await,
            3000,
            "{fixture}: every row of the pin"
        );
    }

    // Commits 3..=5 lie above a cursor at 2, and all of them are read.
    let scan = provider
        .scan_for_materialize_stream(
            "people_backlog:main",
            "silver.people_backlog",
            &[],
            None,
            Some(
                provider
                    .current_snapshot_id("people_backlog:main", "silver.people_backlog")
                    .await
                    .expect("current snapshot")
                    .expect("fixture has snapshots"),
            ),
            Some(2),
        )
        .await
        .expect("pinned read resumed from a cursor");
    assert_eq!(scan.to_sequence, None, "not a prefix");
    assert_eq!(
        read_all(scan.stream).await,
        1800,
        "every commit above the cursor"
    );
}

/// A graph source sharing the default graph with a ledger is refused, in
/// SPARQL and JSON-LD alike — the default graph would not union it in, and a
/// query that silently read only some of its sources would be wrong. Named
/// and addressed with GRAPH, the same join works.
#[tokio::test]
async fn a_graph_source_joins_a_ledger_as_a_named_graph_not_in_the_default_graph() {
    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct("joined-people", &location, PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create graph source");
    let ledger = fluree.create_ledger("crm:main").await.expect("ledger");
    fluree
        .insert(
            ledger,
            &serde_json::json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "http://example.org/person/1", "ex:tier": "gold"
            }),
        )
        .await
        .expect("insert");

    let mixed = "PREFIX ex: <http://example.org/> SELECT ?name ?tier \
                 FROM <crm:main> FROM <joined-people:main> \
                 WHERE { ?p ex:name ?name ; ex:tier ?tier }";
    let err = fluree
        .query_from()
        .sparql(mixed)
        .execute()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("FROM NAMED"), "{err}");
    let mixed_jsonld = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": ["joined-people:main", "crm:main"],
        "select": ["?name", "?tier"],
        "where": {"@id": "?p", "ex:name": "?name", "ex:tier": "?tier"}
    });
    let err = fluree
        .query_from()
        .jsonld(&mixed_jsonld)
        .execute()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("FROM NAMED"), "{err}");

    let named = "PREFIX ex: <http://example.org/> SELECT ?name ?tier \
                 FROM <crm:main> FROM NAMED <joined-people:main> \
                 WHERE { ?p ex:tier ?tier GRAPH <joined-people:main> { ?p ex:name ?name } }";
    let rows = fluree
        .query_from()
        .sparql(named)
        .execute_formatted()
        .await
        .expect("named join");
    assert_eq!(
        rows["results"]["bindings"].as_array().map(Vec::len),
        Some(1),
        "{rows}"
    );
}

/// A dropped graph source is gone: its record stays as a tombstone, which
/// must not still resolve for queries in the same process.
#[tokio::test]
async fn a_dropped_graph_source_no_longer_answers() {
    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct("dropped-people", &location, PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create graph source");
    let sparql =
        "SELECT ?name FROM <dropped-people:main> WHERE { ?s <http://example.org/name> ?name }";
    fluree
        .query_from()
        .sparql(sparql)
        .execute()
        .await
        .expect("query before drop");

    fluree
        .drop_graph_source("dropped-people", None, fluree_db_api::DropMode::Hard)
        .await
        .expect("drop");

    let err = fluree
        .query_from()
        .sparql(sparql)
        .execute()
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "connection query after drop: {err}");
    let graph = fluree.graph("dropped-people:main");
    let err = graph
        .query()
        .with_r2rml()
        .sparql("SELECT ?name WHERE { ?s <http://example.org/name> ?name }")
        .execute()
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "graph query after drop: {err}");
}

/// `ORDER BY ... LIMIT k` hands `k = LIMIT + OFFSET` to the scan-side top-k,
/// whose heap starts at a capped capacity: a `LIMIT` far beyond the table's
/// rows answers from the rows there are, on SPARQL and JSON-LD.
#[tokio::test]
async fn order_by_with_a_limit_beyond_the_table_answers_its_rows() {
    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct("topk-people", &location, PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create graph source");

    const LIMIT: u64 = 9_223_372_036_854_775_807;
    // erin, person/5, holds the top score; an IRI may come back compacted.
    let is_top = |v: &serde_json::Value| v.as_str().is_some_and(|s| s.ends_with("person/5"));
    for (offset, rows) in [(0u64, 5usize), (1, 4)] {
        let sparql = format!(
            "SELECT ?s ?score FROM <topk-people:main> \
             WHERE {{ ?s <http://example.org/score> ?score }} \
             ORDER BY DESC(?score) LIMIT {LIMIT} OFFSET {offset}"
        );
        let result = fluree
            .query_from()
            .sparql(&sparql)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("SPARQL OFFSET {offset}: {e}"));
        let bindings = result["results"]["bindings"]
            .as_array()
            .expect("bindings array");
        assert_eq!(bindings.len(), rows, "SPARQL OFFSET {offset}: {result}");
        assert_eq!(
            is_top(&bindings[0]["s"]["value"]),
            offset == 0,
            "SPARQL OFFSET {offset}: {result}"
        );

        let jsonld = serde_json::json!({
            "@context": {"ex": "http://example.org/"},
            "from": "topk-people:main",
            "select": ["?s", "?score"],
            "where": {"@id": "?s", "ex:score": "?score"},
            "orderBy": "(desc ?score)",
            "limit": LIMIT,
            "offset": offset,
        });
        let result = fluree
            .query_from()
            .jsonld(&jsonld)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("JSON-LD OFFSET {offset}: {e}"));
        let rows_out = result.as_array().expect("array result");
        assert_eq!(rows_out.len(), rows, "JSON-LD OFFSET {offset}: {result}");
        assert_eq!(
            is_top(&rows_out[0][0]),
            offset == 0,
            "JSON-LD OFFSET {offset}: {result}"
        );
    }
}

const AMOUNTS_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#AmountsMapping>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.amounts" ] ;
        rr:subjectMap [ rr:template "http://example.org/amount/{id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:id ;
            rr:objectMap [ rr:column "id" ; rr:datatype xsd:long ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:ratio ;
            rr:objectMap [ rr:column "ratio" ; rr:datatype xsd:decimal ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:price ;
            rr:objectMap [ rr:column "price" ; rr:datatype xsd:decimal ]
        ] .
"#;

/// The ids (sorted) a SPARQL `FILTER(?v <filter>)` over `ex:<column>` returns.
async fn sparql_amount_ids(fluree: &fluree_db_api::Fluree, column: &str, filter: &str) -> Vec<i64> {
    let sparql = format!(
        "PREFIX ex: <http://example.org/> \
         SELECT ?id FROM <amounts:main> \
         WHERE {{ ?s ex:id ?id ; ex:{column} ?v FILTER(?v {filter}) }}"
    );
    let result = fluree
        .query_from()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"));
    let mut ids: Vec<i64> = result["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("bindings for {sparql}: {result}"))
        .iter()
        .map(|b| b["id"]["value"].as_str().unwrap().parse().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}

/// A decimal FILTER over a `decimal(38,9)` / `decimal(5,2)` column answers
/// exactly what the in-engine FILTER would. The pushdown evaluates the
/// literal against each decoded batch before the engine sees it, so a literal
/// the column cannot hold exactly must widen that filter to every row, never
/// narrow it: a row it drops is lost. Each case below failed on main: the
/// first two panicked the reader ("scale 2 is greater than precision 1"), the
/// next two lost a row to Arrow's rounding cast, and the last lost every row
/// to a cast that overflowed to NULL.
#[tokio::test]
async fn decimal_filters_answer_exactly() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let location = format!(
        "file://{}/tests/fixtures/iceberg/silver/amounts",
        env!("CARGO_MANIFEST_DIR")
    );
    let config = R2rmlCreateConfig::new_direct("amounts", &location, AMOUNTS_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create decimal graph source");

    // ratio: 1=0.049999999 2=0.050000000 3=0.050000001 4=1.050000000 5=-0.05
    // price: 1=999.99 2=1.00 3=12.34 4=0.05 5=-999.99
    let cases: [(&str, &str, &[i64]); 6] = [
        ("ratio", ">= 0.05", &[2, 3, 4]),
        ("price", ">= 0.05", &[1, 2, 3, 4]),
        ("ratio", "< 1.0500000001", &[1, 2, 3, 4, 5]),
        ("ratio", "> 1.0499999999", &[4]),
        ("price", "< 1000", &[1, 2, 3, 4, 5]),
        ("ratio", "= 0.05", &[2]),
    ];
    for (column, filter, want) in cases {
        assert_eq!(
            sparql_amount_ids(&fluree, column, filter).await,
            want,
            "FILTER(?{column} {filter})"
        );
    }

    // JSON-LD has no decimal FILTER literal (a fractional number is a double,
    // which is not pushed against a decimal column), so its route into the
    // same row filter is an integer literal against a decimal column: the
    // overflow case above.
    let jsonld = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": "amounts:main",
        "select": ["?id"],
        "where": [
            {"@id": "?s", "ex:id": "?id", "ex:price": "?p"},
            ["filter", "(< ?p 1000)"]
        ],
    });
    let rows = fluree
        .query_from()
        .jsonld(&jsonld)
        .execute_formatted()
        .await
        .expect("JSON-LD decimal filter");
    let mut ids: Vec<i64> = rows
        .as_array()
        .unwrap_or_else(|| panic!("array result: {rows}"))
        .iter()
        .map(|row| row[0].as_i64().unwrap_or_else(|| panic!("id in {rows}")))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, [1, 2, 3, 4, 5], "JSON-LD (< ?p 1000): {rows}");
}

const LITERALS_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#LiteralsMapping>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.literals" ] ;
        rr:subjectMap [ rr:template "http://example.org/literal/{id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:id ;
            rr:objectMap [ rr:column "id" ; rr:datatype xsd:long ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:n ;
            rr:objectMap [ rr:column "ntext" ; rr:datatype xsd:integer ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:b ;
            rr:objectMap [ rr:column "btext" ; rr:datatype xsd:boolean ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:d ;
            rr:objectMap [ rr:column "f64" ; rr:datatype xsd:double ]
        ] .
"#;

/// The ids (sorted) that `FILTER(?v <op> <lit>)` over `ex:<pred>` returns, on
/// SPARQL (`sparql_lit`) and on its JSON-LD twin (`jsonld_lit`).
async fn literal_ids(
    fluree: &fluree_db_api::Fluree,
    pred: &str,
    op: &str,
    sparql_lit: &str,
    jsonld_lit: &str,
) -> (Vec<i64>, Vec<i64>) {
    let sparql = format!(
        "PREFIX ex: <http://example.org/> \
         SELECT ?id FROM <literals:main> \
         WHERE {{ ?s ex:id ?id ; ex:{pred} ?v FILTER(?v {op} {sparql_lit}) }}"
    );
    let result = fluree
        .query_from()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"));
    let mut ids: Vec<i64> = result["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("bindings for {sparql}: {result}"))
        .iter()
        .map(|b| b["id"]["value"].as_str().unwrap().parse().unwrap())
        .collect();
    ids.sort_unstable();

    let jsonld = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": "literals:main",
        "select": ["?id"],
        "where": [
            {"@id": "?s", "ex:id": "?id", format!("ex:{pred}"): "?v"},
            ["filter", format!("({op} ?v {jsonld_lit})")]
        ],
    });
    let rows = fluree
        .query_from()
        .jsonld(&jsonld)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{jsonld}: {e}"));
    let mut jsonld_ids: Vec<i64> = rows
        .as_array()
        .unwrap_or_else(|| panic!("array result: {rows}"))
        .iter()
        .map(|row| {
            row.as_i64()
                .or_else(|| row[0].as_i64())
                .unwrap_or_else(|| panic!("id in {rows}"))
        })
        .collect();
    jsonld_ids.sort_unstable();
    (ids, jsonld_ids)
}

/// A pushed comparison must not compare in an order the engine does not. An
/// `xsd:integer` or `xsd:boolean` map over a string column orders by value in
/// the engine, where the column's own comparison is by text ("999" < "1000"
/// is false; "1" is not "true"). And the engine treats -0.0 as equal to 0,
/// where the Arrow kernels order -0.0 below 0.0. Each case below lost a true
/// match to the row filter before it was fixed.
#[tokio::test]
async fn literal_filters_compare_as_the_engine_does() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let location = format!(
        "file://{}/tests/fixtures/iceberg/silver/literals",
        env!("CARGO_MANIFEST_DIR")
    );
    let config = R2rmlCreateConfig::new_direct("literals", &location, LITERALS_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create literals graph source");

    // ntext: 1="999" 2="1000" 3="50" 4="-5" 5="7"
    // btext: 1="1" 2="true" 3="0" 4="false" 5=null
    // f64:   1=-0.0 2=0.0 3=1.5 4=-1.5 5=null
    let cases: [(&str, &str, &str, &str, &[i64]); 13] = [
        ("n", "<", "1000", "1000", &[1, 3, 4, 5]),
        ("n", ">=", "50", "50", &[1, 2, 3]),
        ("n", "=", "1000", "1000", &[2]),
        ("b", "=", "true", "true", &[1, 2]),
        ("b", "=", "false", "false", &[3, 4]),
        ("d", ">=", "0", "0", &[1, 2, 3]),
        ("d", "<=", "0", "0", &[1, 2, 4]),
        ("d", ">", "0", "0", &[3]),
        ("d", "<", "0", "0", &[4]),
        ("d", "=", "0", "0", &[1, 2]),
        ("d", ">=", "0.0e0", "0.0", &[1, 2, 3]),
        ("d", "<=", "-0.0e0", "-0.0", &[1, 2, 4]),
        ("d", ">", "-1.5e0", "-1.5", &[1, 2, 3]),
    ];
    let mut failures = Vec::new();
    for (pred, op, sparql_lit, jsonld_lit, want) in cases {
        let (sparql, jsonld) = literal_ids(&fluree, pred, op, sparql_lit, jsonld_lit).await;
        if sparql != want {
            failures.push(format!(
                "SPARQL FILTER(?{pred} {op} {sparql_lit}): {sparql:?} != {want:?}"
            ));
        }
        if jsonld != want {
            failures.push(format!(
                "JSON-LD ({op} ?{pred} {jsonld_lit}): {jsonld:?} != {want:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// BigQuery writes a snapshot summary of only `{"operation": "overwrite"}`, so
/// the summary has no delete counters for the zero-I/O guard to read. A
/// merge-on-read delete manifest in such a snapshot's manifest list must still
/// refuse every read that would return rows (the scan, a `COUNT(*)`, a
/// materialize read) with the merge-on-read refusal, never return the rows the
/// delete files remove.
///
/// The staged table is the `people` fixture with its current snapshot's summary
/// replaced by BigQuery's and its manifest list replaced by the same list plus
/// one delete manifest (`content = 1`), which does not exist: refusing must not
/// need to read it. The control is the same summary over the original list.
#[tokio::test]
async fn a_delete_manifest_a_bigquery_summary_does_not_report_refuses_every_read() {
    use apache_avro::types::Value as AvroValue;

    allow_fixture_roots();
    let staging =
        tempfile::TempDir::new_in(fixtures_dir()).expect("staging dir under the fixtures root");
    let stage = |name: &str, with_delete_manifest: bool| -> String {
        let table_dir = staging.path().join(name);
        copy_dir_all(&fixtures_dir().join("silver/people"), &table_dir);
        let metadata_dir = table_dir.join("metadata");
        let mut metadata_files: Vec<std::path::PathBuf> = std::fs::read_dir(&metadata_dir)
            .expect("metadata dir")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
            .collect();
        metadata_files.sort();
        let newest = metadata_files.last().expect("a metadata file").clone();
        let mut meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&newest).expect("read metadata"))
                .expect("parse metadata");
        let current = meta["current-snapshot-id"].clone();
        let snapshot = meta["snapshots"]
            .as_array_mut()
            .expect("snapshots")
            .iter_mut()
            .find(|s| s["snapshot-id"] == current)
            .expect("current snapshot");
        snapshot["summary"] = serde_json::json!({"operation": "overwrite"});
        if with_delete_manifest {
            // The list, as written: the fixture's `file:///tmp/...` path, which
            // the provider remaps onto the staged location.
            let list_ref = snapshot["manifest-list"]
                .as_str()
                .expect("list")
                .to_string();
            let list_name = list_ref.rsplit('/').next().expect("list file name");
            let original = std::fs::read(metadata_dir.join(list_name)).expect("read list");
            let reader = apache_avro::Reader::new(&original[..]).expect("avro list");
            let schema = reader.writer_schema().clone();
            let records: Vec<AvroValue> = reader.map(|r| r.expect("record")).collect();
            let AvroValue::Record(data) = records[0].clone() else {
                panic!("a manifest_file record");
            };
            let delete: Vec<(String, AvroValue)> = data
                .into_iter()
                .map(|(k, v)| match k.as_str() {
                    "manifest_path" => {
                        let AvroValue::String(p) = v else {
                            panic!("manifest_path")
                        };
                        (k, AvroValue::String(p.replace(".avro", "-deletes.avro")))
                    }
                    "content" => (k, AvroValue::Int(1)),
                    _ => (k, v),
                })
                .collect();
            let mut writer = apache_avro::Writer::new(&schema, Vec::new());
            for record in records {
                writer.append(record).expect("copy a record");
            }
            writer
                .append(AvroValue::Record(delete))
                .expect("add the delete manifest");
            let with_deletes = "snap-with-delete-manifest.avro";
            std::fs::write(
                metadata_dir.join(with_deletes),
                writer.into_inner().expect("avro bytes"),
            )
            .expect("write list");
            snapshot["manifest-list"] =
                serde_json::json!(list_ref.replace(list_name, with_deletes));
        }
        std::fs::write(&newest, serde_json::to_vec(&meta).expect("metadata json"))
            .expect("write metadata");
        format!("file://{}", table_dir.display())
    };

    let fluree = FlureeBuilder::memory().build_memory();
    let query = |gs: &str| {
        serde_json::json!({
            "@context": {"ex": "http://example.org/"},
            "from": gs,
            "select": ["?name"],
            "where": {"@id": "?s", "ex:name": "?name"},
        })
    };
    let count = |gs: &str| {
        serde_json::json!({
            "@context": {"ex": "http://example.org/"},
            "from": gs,
            "select": ["(count ?s)"],
            "where": {"@id": "?s", "@type": "ex:Person"},
        })
    };
    let refused = |result: Result<serde_json::Value, ApiError>, what: &str| {
        let err = result.expect_err(what);
        assert!(
            err.to_string()
                .contains("Merge-on-read deletes not applied"),
            "{what}: {err}"
        );
    };

    // Control: BigQuery's summary over a list with no delete manifest reads.
    let clean = stage("clean", false);
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("bq-clean", &clean, PEOPLE_R2RML)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("register the clean table");
    let rows = fluree
        .query_from()
        .jsonld(&query("bq-clean:main"))
        .execute_formatted()
        .await
        .expect("the clean table reads");
    assert_eq!(rows.as_array().expect("rows").len(), 5);

    // The delete manifest refuses each read, though the summary says nothing.
    let mor = stage("mor", true);
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("bq-mor", &mor, PEOPLE_R2RML)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("register the table (registration reads no data)");
    refused(
        fluree
            .query_from()
            .jsonld(&query("bq-mor:main"))
            .execute_formatted()
            .await,
        "the scan",
    );
    refused(
        fluree
            .query_from()
            .jsonld(&count("bq-mor:main"))
            .execute_formatted()
            .await,
        "COUNT(*)",
    );
    let provider = FlureeR2rmlProvider::new(&fluree);
    match provider
        .scan_for_materialize_stream("bq-mor:main", "silver.people", &[], None, None, None)
        .await
    {
        Ok(_) => panic!("a materialize read must be refused"),
        Err(err) => assert!(
            err.to_string()
                .contains("Merge-on-read deletes not applied"),
            "materialize: {err}"
        ),
    }
}

/// A subgraph crawl over a graph source answers the subjects its ORDER BY puts
/// on the page, in that order, as the same crawl over a native ledger does. The
/// crawl runs as one flat scan regrouped by subject, and that scan used to drop
/// the ORDER BY: the page came back in scan order and, under an OFFSET, held the
/// wrong subjects. The native twin carries the fixture's IRIs and scores, so it
/// is the oracle; every projection shape the expansion handles is covered,
/// because each one builds its own flat query.
#[tokio::test]
async fn a_graph_source_crawl_answers_in_order_by_order() {
    use serde_json::{json, Value};

    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("ordered-people", &location, PEOPLE_R2RML)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create graph source");
    let twin = fluree
        .create_ledger("ordered-twin:main")
        .await
        .expect("twin ledger");
    let people = [
        (1, "alice", 91.5),
        (2, "bob", 82.0),
        (3, "carol", 77.25),
        (4, "dave", 64.0),
        (5, "erin", 99.9),
    ];
    let graph: Vec<Value> = people
        .iter()
        .map(|(id, name, score)| {
            json!({
                "@id": format!("http://example.org/person/{id}"),
                "@type": "http://example.org/Person",
                "http://example.org/name": name,
                "http://example.org/score": score,
            })
        })
        .collect();
    fluree
        .insert(twin, &json!({ "@graph": graph }))
        .await
        .expect("seed native twin");

    let optional_sort = json!([
        {"@id": "?s", "@type": "ex:Person"},
        ["optional", {"@id": "?s", "ex:score": "?sort"}]
    ]);
    let one_map = json!({"@id": "?s", "@type": "ex:Person", "ex:score": "?sort"});
    // Scores: 1 → 91.5, 2 → 82, 3 → 77.25, 4 → 64, 5 → 99.9.
    let cases = [
        // The live R2-3 shape: the sort key comes from an OPTIONAL.
        (
            "[*] desc",
            json!(["*"]),
            optional_sort.clone(),
            json!([["desc", "?sort"]]),
            5,
            0,
            vec![5, 1, 2, 3, 4],
        ),
        // OFFSET and LIMIT cut the ordered subjects, not the scan's.
        (
            "[*] asc page",
            json!(["*"]),
            one_map.clone(),
            json!("?sort"),
            3,
            1,
            vec![3, 2, 1],
        ),
        (
            "[@id] desc page",
            json!(["@id"]),
            one_map.clone(),
            json!("(desc ?sort)"),
            2,
            1,
            vec![1, 2],
        ),
        (
            "predicates desc",
            json!(["@id", "ex:name"]),
            optional_sort,
            json!([["desc", "?sort"]]),
            4,
            0,
            vec![5, 1, 2, 3],
        ),
    ];
    for (label, projection, where_clause, order_by, limit, offset, expected) in cases {
        let expected: Vec<String> = expected.iter().map(|n| format!("ex:person/{n}")).collect();
        for from in ["ordered-people:main", "ordered-twin:main"] {
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": from,
                "select": {"?s": projection},
                "where": where_clause,
                "orderBy": order_by,
                "limit": limit,
                "offset": offset,
            });
            let result = fluree
                .query_from()
                .jsonld(&query)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{label} on {from}: {e}"));
            let ids: Vec<&str> = result
                .as_array()
                .expect("a crawl answers an array")
                .iter()
                .map(|doc| doc["@id"].as_str().expect("each document has an @id"))
                .collect();
            assert_eq!(ids, expected, "{label} on {from}: {result}");
        }
    }
}

/// The crawl's flat scan carries no GROUP BY / HAVING, so a crawl that asks for
/// them is refused, as a top-level `values` is, rather than answered without
/// them.
#[tokio::test]
async fn a_graph_source_crawl_refuses_group_by_and_having() {
    use serde_json::json;

    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("grouped-people", &location, PEOPLE_R2RML)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create graph source");
    for (key, value) in [
        ("groupBy", json!(["?s"])),
        ("having", json!("(> (count ?n) 1)")),
    ] {
        let mut query = json!({
            "@context": {"ex": "http://example.org/"},
            "from": "grouped-people:main",
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "ex:name": "?n"},
        });
        query[key] = value;
        let err = fluree
            .query_from()
            .jsonld(&query)
            .execute_formatted()
            .await
            .expect_err("a crawl cannot honor {key}");
        assert!(
            matches!(
                err,
                ApiError::Query(fluree_db_query::QueryError::R2rmlUnsupportedPattern { .. })
            ),
            "{key}: {err}"
        );
    }
}

/// A subgraph crawl over a graph source keeps the request's options: its
/// fuel limit, from the body's `opts` or a tracked request's override, on the
/// connection and the graph terminals alike, and its reasoning keys. The crawl runs
/// a rewritten flat query, which used to carry only the context, `select`,
/// `where`, `orderBy` and `limit`, so a crawl ran without its fuel limit and
/// without reasoning. SPARQL has no subgraph crawl, so there is no twin.
#[tokio::test]
async fn crawl_over_a_graph_source_keeps_its_query_options() {
    use fluree_db_api::{TrackedQueryResponse, TrackingOptions};
    use serde_json::{json, Value};

    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("optioned-people", &location, PEOPLE_R2RML)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create graph source");

    let crawl = |extra: Value| {
        let mut q = json!({
            "@context": {"ex": "http://example.org/"},
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "ex:Person"},
        });
        for (k, v) in extra.as_object().unwrap() {
            q[k] = v.clone();
        }
        q
    };
    let rows = |v: &Value| v.as_array().map_or(0, Vec::len);
    let fuel_exceeded = |err: &ApiError| {
        matches!(
            err,
            ApiError::Query(fluree_db_query::QueryError::FuelLimitExceeded(_))
        )
    };
    let tracked_fuel_exceeded =
        |status: u16, error: &str| status == 400 && error.to_lowercase().contains("fuel");
    // Five people with a name and a score cost more than 1.001 fuel: the
    // 1.000 floor and one thousandth per row.
    let tight = json!({"opts": {"max-fuel": 1.001}});
    let generous = json!({"opts": {"max-fuel": 1_000_000}});
    let with_from = |mut q: Value| {
        q["from"] = json!("optioned-people:main");
        q
    };

    // Connection terminal, body limit.
    let ok = fluree
        .query_from()
        .jsonld(&with_from(crawl(generous.clone())))
        .execute_formatted()
        .await
        .expect("a crawl under its limit succeeds");
    assert_eq!(rows(&ok), 5, "{ok}");
    let err = fluree
        .query_from()
        .jsonld(&with_from(crawl(tight.clone())))
        .execute_formatted()
        .await
        .expect_err("a crawl past its body fuel limit fails");
    assert!(fuel_exceeded(&err), "connection, body limit: {err:?}");

    // Graph terminal, body limit.
    let graph = fluree.graph("optioned-people:main");
    let err = graph
        .query()
        .with_r2rml()
        .jsonld(&crawl(tight.clone()))
        .execute_formatted()
        .await
        .expect_err("a crawl past its body fuel limit fails");
    assert!(fuel_exceeded(&err), "graph, body limit: {err:?}");

    // Tracked terminals: the override's limit holds, and replaces the body's.
    let override_limit = TrackingOptions {
        track_fuel: true,
        max_fuel: Some(1_001),
        ..TrackingOptions::default()
    };
    let check_tracked = |label: &str,
                         limited: Result<TrackedQueryResponse, _>,
                         lifted: Result<TrackedQueryResponse, _>| {
        let limited: fluree_db_api::TrackedErrorResponse =
            limited.expect_err(&format!("{label}: the override's limit holds"));
        assert!(
            tracked_fuel_exceeded(limited.status, &limited.error),
            "{label}: {} {}",
            limited.status,
            limited.error
        );
        let lifted = lifted.unwrap_or_else(|e| {
            panic!("{label}: an override with no limit lifts the body's: {e:?}")
        });
        assert_eq!(rows(&lifted.result), 5, "{label}: {:?}", lifted.result);
        // The crawl reports what it cost, as a tracked flat query does.
        assert!(
            lifted.fuel.is_some_and(|fuel| fuel > 1.0),
            "{label}: fuel {:?}",
            lifted.fuel
        );
        assert!(lifted.time.is_some(), "{label}: no time reported");
        assert!(
            limited.fuel.is_some(),
            "{label}: a crawl past its limit reports the fuel it spent"
        );
    };
    check_tracked(
        "connection",
        fluree
            .query_from()
            .jsonld(&with_from(crawl(json!({}))))
            .tracking(override_limit.clone())
            .execute_tracked()
            .await,
        fluree
            .query_from()
            .jsonld(&with_from(crawl(tight.clone())))
            .track_all()
            .execute_tracked()
            .await,
    );
    check_tracked(
        "graph",
        graph
            .query()
            .with_r2rml()
            .jsonld(&crawl(json!({})))
            .tracking(override_limit.clone())
            .execute_tracked()
            .await,
        graph
            .query()
            .with_r2rml()
            .jsonld(&crawl(tight.clone()))
            .track_all()
            .execute_tracked()
            .await,
    );

    // Reasoning reaches the flat query's parser: an unknown mode is refused,
    // as on a flat query, where the crawl used to answer as if none were named.
    // (`crawl::e2e::crawl_carries_reasoning` shows an RDFS crawl answering the
    // subclass's instances.)
    for (label, query) in [
        (
            "crawl",
            with_from(crawl(json!({"reasoning": "no-such-mode"}))),
        ),
        (
            "flat",
            with_from(json!({
                "@context": {"ex": "http://example.org/"},
                "select": ["?s"],
                "where": {"@id": "?s", "@type": "ex:Person"},
                "reasoning": "no-such-mode",
            })),
        ),
    ] {
        let err = fluree
            .query_from()
            .jsonld(&query)
            .execute_formatted()
            .await
            .expect_err("an unknown reasoning mode is refused");
        assert!(err.to_string().contains("reasoning"), "{label}: {err}");
    }
}

/// SPARQL twin of the crawl ordering. SPARQL has no subgraph crawl: the
/// shape that picks a page of subjects by ORDER BY and returns their triples is
/// a CONSTRUCT, which runs on the ordinary R2RML path and keeps only the
/// ordered page's triples. DESCRIBE lowers to a subquery, which a graph source
/// refuses rather than answering from its empty native index.
#[tokio::test]
async fn sparql_construct_over_a_graph_source_keeps_the_ordered_page() {
    fn strings<'v>(v: &'v serde_json::Value, out: &mut Vec<&'v str>) {
        match v {
            serde_json::Value::String(s) => out.push(s),
            serde_json::Value::Array(a) => a.iter().for_each(|v| strings(v, out)),
            serde_json::Value::Object(o) => o.values().for_each(|v| strings(v, out)),
            _ => {}
        }
    }

    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("constructed-people", &location, PEOPLE_R2RML)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create graph source");
    let construct = "PREFIX ex: <http://example.org/> \
        CONSTRUCT { ?s ex:name ?name } FROM <constructed-people:main> \
        WHERE { ?s a ex:Person ; ex:name ?name OPTIONAL { ?s ex:score ?sort } } \
        ORDER BY DESC(?sort) LIMIT 2 OFFSET 1";
    let result = fluree
        .query_from()
        .sparql(construct)
        .execute_formatted()
        .await
        .expect("CONSTRUCT");
    let mut found = Vec::new();
    strings(&result, &mut found);
    let mut names: Vec<&str> = found
        .into_iter()
        .filter(|s| ["alice", "bob", "carol", "dave", "erin"].contains(s))
        .collect();
    names.sort_unstable();
    // DESC(score): erin, alice, bob, carol, dave; OFFSET 1 LIMIT 2 → alice, bob.
    assert_eq!(names, ["alice", "bob"], "CONSTRUCT: {result}");

    let describe = "PREFIX ex: <http://example.org/> \
        DESCRIBE ?s FROM <constructed-people:main> \
        WHERE { ?s a ex:Person } ORDER BY ?s LIMIT 2";
    let err = fluree
        .query_from()
        .sparql(describe)
        .execute_formatted()
        .await
        .expect_err("DESCRIBE over a graph source is refused");
    assert!(err.to_string().contains("subquery"), "DESCRIBE: {err}");
}

/// The people table mapped twice over disjoint subject templates: persons
/// carry a name, agents a score. So a person never has a score, and a class
/// or constraint on persons fuses into the scan beside it (the templates prove
/// the fusion drops nothing).
const SPLIT_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#Person> a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people" ] ;
        rr:subjectMap [ rr:template "http://example.org/person/{id}" ; rr:class ex:Person ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .

    <http://example.org/mapping#Agent> a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people" ] ;
        rr:subjectMap [ rr:template "http://example.org/agent/{id}" ; rr:class ex:Agent ] ;
        rr:predicateObjectMap [ rr:predicate ex:score ; rr:objectMap [ rr:column "score" ] ] .
"#;

/// A pattern written above an OPTIONAL is part of its left side. The rewrite
/// fuses a class (or a constant-object constraint) into the scan written below
/// the OPTIONAL; that fused scan must stay above it. Below, the OPTIONAL runs
/// over an empty seed, binds `?s` to the agents it matches, and every person,
/// none of whom has a score, is lost. On a lake the trigger is a null column
/// under an OPTIONAL.
#[tokio::test]
async fn an_optional_keeps_the_class_written_above_it() {
    use serde_json::{json, Value};

    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("split-people", &location, SPLIT_R2RML)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create graph source");
    let jsonld = |query: Value| {
        let fluree = &fluree;
        async move {
            let mut query = query;
            query["@context"] = json!({"ex": "http://example.org/"});
            query["from"] = json!("split-people:main");
            fluree
                .query_from()
                .jsonld(&query)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"))
        }
    };
    let rows = |v: &Value| v.as_array().map_or(0, Vec::len);
    let no_score = json!(["optional", {"@id": "?s", "ex:score": "?x"}]);

    // The browse crawl: the class fuses into the injected wildcard.
    let crawl = jsonld(json!({
        "select": {"?s": ["*"]},
        "where": [{"@id": "?s", "@type": "ex:Person"}, no_score],
    }))
    .await;
    assert_eq!(rows(&crawl), 5, "crawl: {crawl}");
    assert!(
        crawl
            .as_array()
            .unwrap()
            .iter()
            .all(|d| d["ex:name"].is_string()),
        "each person keeps its name: {crawl}"
    );

    // A constant-object constraint folded onto the crawl wildcard (W4-1b).
    let alice = jsonld(json!({
        "select": {"?s": ["*"]},
        "where": [{"@id": "?s", "ex:name": "alice"}, no_score],
    }))
    .await;
    assert_eq!(rows(&alice), 1, "constraint crawl: {alice}");

    // The class fused into a star written below the OPTIONAL.
    let star = jsonld(json!({
        "select": ["?s", "?n", "?x"],
        "where": [{"@id": "?s", "@type": "ex:Person"}, no_score, {"@id": "?s", "ex:name": "?n"}],
    }))
    .await;
    assert_eq!(rows(&star), 5, "class into star: {star}");

    // A flat wildcard, and its SPARQL twin: a type and a name per person.
    let flat = jsonld(json!({
        "select": ["?s", "?p", "?o"],
        "where": [{"@id": "?s", "@type": "ex:Person"}, no_score, {"@id": "?s", "?p": "?o"}],
    }))
    .await;
    assert_eq!(rows(&flat), 10, "flat wildcard: {flat}");
    let sparql = fluree
        .query_from()
        .sparql(
            "PREFIX ex: <http://example.org/> SELECT ?s ?p ?o FROM <split-people:main> \
             WHERE { ?s a ex:Person OPTIONAL { ?s ex:score ?x } ?s ?p ?o }",
        )
        .execute_formatted()
        .await
        .expect("SPARQL");
    assert_eq!(
        rows(&sparql["results"]["bindings"]),
        10,
        "SPARQL wildcard: {sparql}"
    );
}

/// A MINUS is positional like an OPTIONAL: what is written before it is its
/// left side, so a scan the rewrite fuses or groups must not cross it. Every
/// shape runs over `SPLIT_R2RML` and over a native twin holding the same
/// triples, on SPARQL and JSON-LD, and must give the count Jena ARQ and
/// Oxigraph both give. The fused shapes lifted the wildcard or the star above a
/// MINUS that reads its variables; the scopes without an OPTIONAL were never
/// restored to written order, so the grouped scans ran below the MINUS.
#[tokio::test]
async fn a_minus_keeps_the_scans_written_above_it() {
    use serde_json::{json, Value};

    let location = table_location();
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("minus-people", &location, SPLIT_R2RML)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create graph source");
    let twin = fluree
        .create_ledger("minus-twin:main")
        .await
        .expect("twin ledger");
    let people = [
        (1, "alice", "91.5"),
        (2, "bob", "82.0"),
        (3, "carol", "77.25"),
        (4, "dave", "64.0"),
        (5, "erin", "99.9"),
    ];
    let mut graph: Vec<Value> = Vec::new();
    for (id, name, score) in people {
        graph.push(json!({
            "@id": format!("http://example.org/person/{id}"),
            "@type": "http://example.org/Person",
            "http://example.org/name": name,
        }));
        graph.push(json!({
            "@id": format!("http://example.org/agent/{id}"),
            "@type": "http://example.org/Agent",
            "http://example.org/score": score,
        }));
    }
    fluree
        .insert(twin, &json!({ "@graph": graph }))
        .await
        .expect("seed native twin");

    let score = json!(["optional", {"@id": "?s", "ex:score": "?x"}]);
    let person = json!({"@id": "?s", "@type": "ex:Person"});
    let alice = json!({"@id": "?s", "ex:name": "alice"});
    let wildcard = json!({"@id": "?s", "?p": "?o"});
    // (label, SPARQL query, JSON-LD select, JSON-LD where, rows per ARQ and Oxigraph)
    let cases = [
        (
            "class into wildcard, MINUS then OPTIONAL",
            "SELECT * WHERE { ?s a ex:Person . MINUS { ?s ex:name ?o } OPTIONAL { ?s ex:score ?x } ?s ?p ?o }",
            json!(["?s", "?p", "?o"]),
            json!([person, ["minus", {"@id": "?s", "ex:name": "?o"}], score, wildcard]),
            0,
        ),
        (
            "class into star, MINUS then OPTIONAL",
            "SELECT * WHERE { ?s a ex:Person . MINUS { ?s a ?n } OPTIONAL { ?s ex:score ?x } ?s ex:name ?n }",
            json!(["?s", "?n"]),
            json!([person, ["minus", {"@id": "?s", "@type": "?n"}], score, {"@id": "?s", "ex:name": "?n"}]),
            0,
        ),
        (
            "constraint into wildcard, MINUS then OPTIONAL",
            "SELECT * WHERE { ?s ex:name \"alice\" . MINUS { ?s a ?o } OPTIONAL { ?s ex:score ?x } ?s ?p ?o }",
            json!(["?s", "?p", "?o"]),
            json!([alice, ["minus", {"@id": "?s", "@type": "?o"}], score, wildcard]),
            0,
        ),
        (
            "class into wildcard, MINUS only",
            "SELECT * WHERE { ?s a ex:Person . MINUS { ?s ex:name ?o } ?s ?p ?o }",
            json!(["?s", "?p", "?o"]),
            json!([person, ["minus", {"@id": "?s", "ex:name": "?o"}], wildcard]),
            0,
        ),
        (
            "class into star, MINUS only",
            "SELECT * WHERE { ?s a ex:Person . MINUS { ?s a ?n } ?s ex:name ?n }",
            json!(["?s", "?n"]),
            json!([person, ["minus", {"@id": "?s", "@type": "?n"}], {"@id": "?s", "ex:name": "?n"}]),
            0,
        ),
        (
            "constraint into wildcard, MINUS only",
            "SELECT * WHERE { ?s ex:name \"alice\" . MINUS { ?s a ?o } ?s ?p ?o }",
            json!(["?s", "?p", "?o"]),
            json!([alice, ["minus", {"@id": "?s", "@type": "?o"}], wildcard]),
            0,
        ),
        // The review's P3 and P3b, verbatim. The class triple is written
        // first, so `?s` is bound when the MINUS runs (Join(Minus(Extend(
        // BGP(?s a Person), ?k), BGP(?s name "alice")), BGP(?s name ?n)),
        // §18.2.2) and alice is removed: 4. With no OPTIONAL the scope was
        // never restored to written order, so the graph source ran the class
        // below the MINUS, which then removed nothing: 5.
        (
            "P3: class, BIND, then MINUS",
            "SELECT ?s ?n WHERE { ?s a ex:Person . BIND(\"k\" AS ?k) MINUS { ?s ex:name \"alice\" } ?s ex:name ?n }",
            json!(["?s", "?n"]),
            json!([person, ["bind", "?k", ["expr", ["str", "k"]]], ["minus", alice], {"@id": "?s", "ex:name": "?n"}]),
            4,
        ),
        (
            "P3b: class, BIND, MINUS, then OPTIONAL",
            "SELECT ?s ?n WHERE { ?s a ex:Person . BIND(\"k\" AS ?k) MINUS { ?s ex:name \"alice\" } ?s ex:name ?n OPTIONAL { ?s ex:score ?x } }",
            json!(["?s", "?n"]),
            json!([person, ["bind", "?k", ["expr", ["str", "k"]]], ["minus", alice], {"@id": "?s", "ex:name": "?n"}, score]),
            4,
        ),
        // With the BIND written first, the MINUS's left side is the BIND alone,
        // whose domain shares nothing with the MINUS, so it removes nothing and
        // the triple written after it joins every person: 5.
        (
            "BIND first, then MINUS",
            "SELECT * WHERE { BIND(1 AS ?k) MINUS { ?s ex:name \"alice\" } ?s ex:name ?n }",
            json!(["?s", "?n", "?k"]),
            json!([["bind", "?k", "(+ 0 1)"], ["minus", alice], {"@id": "?s", "ex:name": "?n"}]),
            5,
        ),
    ];
    let count = |v: &Value| {
        v["results"]["bindings"]
            .as_array()
            .or_else(|| v.as_array())
            .map_or(usize::MAX, Vec::len)
    };
    let mut wrong = Vec::new();
    for (label, query, select, where_clause, expected) in &cases {
        for from in ["minus-people:main", "minus-twin:main"] {
            let sparql = format!(
                "PREFIX ex: <http://example.org/> {}",
                query.replacen(" WHERE", &format!(" FROM <{from}> WHERE"), 1)
            );
            let result = fluree
                .query_from()
                .sparql(&sparql)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{label} SPARQL on {from}: {e}"));
            if count(&result) != *expected {
                wrong.push(format!("{label} SPARQL on {from}: {} rows", count(&result)));
            }
            let jsonld = json!({
                "@context": {"ex": "http://example.org/"},
                "from": from,
                "select": select,
                "where": where_clause,
            });
            let result = fluree
                .query_from()
                .jsonld(&jsonld)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{label} JSON-LD on {from}: {e}"));
            if count(&result) != *expected {
                wrong.push(format!(
                    "{label} JSON-LD on {from}: {} rows",
                    count(&result)
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "rows differ from ARQ/Oxigraph:\n{}",
        wrong.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Scan planning from the snapshot's kept file list. `people_window` is five
// appends of 600 rows with disjoint ids (1..=600, 601..=1200, ...), one data
// file each, so an id range selects whole files.
// ---------------------------------------------------------------------------

const WINDOW_IDS_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#WindowIds>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people_window" ] ;
        rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:id ;
            rr:objectMap [ rr:column "id" ; rr:datatype xsd:long ]
        ] .
"#;

/// A private copy of `people_window` (so deleting its manifests touches no
/// other test, and its cache keys are its own) behind a graph source `alias`.
async fn staged_window(alias: &str) -> (fluree_db_api::Fluree, tempfile::TempDir) {
    allow_fixture_roots();
    let staging =
        tempfile::TempDir::new_in(fixtures_dir()).expect("staging dir under the fixtures root");
    let table_dir = staging.path().join("people_window");
    copy_dir_all(&fixtures_dir().join("silver/people_window"), &table_dir);
    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct(
        alias,
        format!("file://{}", table_dir.display()),
        WINDOW_IDS_R2RML,
    )
    .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create window graph source");
    (fluree, staging)
}

/// Delete the staged table's data manifests (the manifest lists stay), so any
/// later scan that reads a manifest fails.
fn delete_data_manifests(staging: &tempfile::TempDir) -> usize {
    let metadata = staging.path().join("people_window/metadata");
    let mut deleted = 0;
    for entry in std::fs::read_dir(&metadata).expect("metadata dir") {
        let path = entry.expect("entry").path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with(".avro") && !name.starts_with("snap-") {
            std::fs::remove_file(&path).expect("delete a data manifest");
            deleted += 1;
        }
    }
    deleted
}

/// The ids (sorted) a SPARQL query over `from` returns, with `filter` as the
/// body of a `FILTER(...)` over `?id` (none when empty).
async fn sparql_window_ids(fluree: &fluree_db_api::Fluree, from: &str, filter: &str) -> Vec<i64> {
    let filter = if filter.is_empty() {
        String::new()
    } else {
        format!("FILTER({filter})")
    };
    let sparql = format!(
        "PREFIX ex: <http://example.org/> SELECT ?id FROM <{from}> WHERE {{ ?s ex:id ?id {filter} }}"
    );
    let result = fluree
        .query_from()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"));
    let mut ids: Vec<i64> = result["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("bindings for {sparql}: {result}"))
        .iter()
        .map(|b| b["id"]["value"].as_str().unwrap().parse().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}

/// The JSON-LD twin of [`sparql_window_ids`]; `filter` is a JSON-LD filter
/// expression over `?id`.
async fn jsonld_window_ids(fluree: &fluree_db_api::Fluree, from: &str, filter: &str) -> Vec<i64> {
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": from,
        "select": ["?id"],
        "where": [{"@id": "?s", "ex:id": "?id"}, ["filter", filter]],
    });
    let result = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut ids: Vec<i64> = result
        .as_array()
        .unwrap_or_else(|| panic!("rows for {query}: {result}"))
        .iter()
        .map(|row| {
            let v = if row.is_array() { &row[0] } else { row };
            v.as_i64().unwrap_or_else(|| panic!("an id in {result}"))
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn id_range(lo: i64, hi: i64) -> Vec<i64> {
    (lo..=hi).collect()
}

/// Once a snapshot's file list is kept, a filtered scan plans from it and
/// reads no manifest: the data manifests are deleted after the first
/// (unfiltered) scan, and filtered scans on both surfaces still answer
/// exactly. Before, a filtered scan re-read every manifest on every query.
#[tokio::test]
async fn a_filtered_scan_reads_no_manifest_once_the_file_list_is_kept() {
    let (fluree, staging) = staged_window("kept-list").await;
    let from = "kept-list:main";
    assert_eq!(sparql_window_ids(&fluree, from, "").await.len(), 3000);
    assert_eq!(
        delete_data_manifests(&staging),
        5,
        "one manifest per append"
    );

    assert_eq!(
        sparql_window_ids(&fluree, from, "?id >= 700 && ?id < 1300").await,
        id_range(700, 1299)
    );
    assert_eq!(
        sparql_window_ids(&fluree, from, "?id IN (5, 2500, 9999)").await,
        vec![5, 2500]
    );
    assert_eq!(
        sparql_window_ids(&fluree, from, "?id > 2995").await,
        id_range(2996, 3000)
    );
    assert_eq!(
        jsonld_window_ids(&fluree, from, "(and (>= ?id 700) (< ?id 1300))").await,
        id_range(700, 1299)
    );
    assert_eq!(
        jsonld_window_ids(&fluree, from, "(= ?id 2500)").await,
        vec![2500]
    );
    assert_eq!(sparql_window_ids(&fluree, from, "").await.len(), 3000);
}

/// A filtered first scan keeps the WHOLE file list, not its filtered
/// selection: after it, with the manifests gone, a different filter and an
/// unfiltered scan both answer from the kept list.
#[tokio::test]
async fn a_filtered_first_scan_keeps_the_whole_file_list() {
    let (fluree, staging) = staged_window("first-filtered").await;
    let from = "first-filtered:main";
    assert_eq!(
        sparql_window_ids(&fluree, from, "?id >= 2950").await,
        id_range(2950, 3000)
    );
    assert_eq!(delete_data_manifests(&staging), 5);

    assert_eq!(
        sparql_window_ids(&fluree, from, "?id < 3").await,
        vec![1, 2]
    );
    assert_eq!(
        jsonld_window_ids(&fluree, from, "(< ?id 3)").await,
        vec![1, 2]
    );
    assert_eq!(
        sparql_window_ids(&fluree, from, "").await,
        id_range(1, 3000)
    );
}

/// Every scan records its file selection on `iceberg.scan_plan`, a scan
/// planned from the kept list included: a warm unfiltered scan reports all
/// five files (it used to record nothing), and a warm filtered one the files
/// its filter kept and pruned.
#[tokio::test(flavor = "current_thread")]
async fn a_scan_from_the_kept_list_records_its_file_counts() {
    let (fluree, _staging) = staged_window("kept-counts").await;
    let from = "kept-counts:main";
    let (store, _tracing_guard) = span_capture::init_test_tracing();
    let counts = |seen: usize| {
        store.find_spans("iceberg.scan_plan")[seen..]
            .iter()
            .map(|s| {
                (
                    s.fields.get("files_selected").cloned().unwrap_or_default(),
                    s.fields.get("files_pruned").cloned().unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>()
    };
    let pair = |selected: &str, pruned: &str| vec![(selected.to_string(), pruned.to_string())];

    // The first scan reads the manifests and keeps the list.
    assert_eq!(sparql_window_ids(&fluree, from, "").await.len(), 3000);
    assert_eq!(counts(0), pair("5", "0"));

    for (filter, want, (selected, pruned)) in [
        ("", 3000, ("5", "0")),
        ("?id >= 700 && ?id < 1300", 600, ("2", "3")),
        ("?id IN (5, 2500)", 2, ("2", "3")),
        ("?id > 99999", 0, ("0", "5")),
    ] {
        let seen = store.find_spans("iceberg.scan_plan").len();
        assert_eq!(
            sparql_window_ids(&fluree, from, filter).await.len(),
            want,
            "{filter}"
        );
        assert_eq!(counts(seen), pair(selected, pruned), "{filter:?}");
    }
}

/// A pinned scan keeps and plans from its own snapshot's list, apart from the
/// current snapshot's, filtered or not, in either order.
#[tokio::test]
async fn a_pinned_filtered_scan_plans_from_its_own_snapshots_list() {
    let (fluree, staging) = staged_window("pinned-kept").await;
    // The second commit (ids 1..=1200), read from the final metadata.
    let latest = std::fs::read_dir(staging.path().join("people_window/metadata"))
        .expect("metadata dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
        .max()
        .expect("a metadata file");
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(latest).expect("read metadata")).unwrap();
    let second = meta["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["sequence-number"] == 2)
        .expect("a second commit")["snapshot-id"]
        .as_i64()
        .unwrap();
    let current = "pinned-kept:main";
    let pinned = format!("pinned-kept:main@snapshot:{second}");
    let window = "?id >= 1100 && ?id < 1300";

    for _ in 0..2 {
        assert_eq!(
            sparql_window_ids(&fluree, current, window).await,
            id_range(1100, 1299)
        );
        assert_eq!(
            sparql_window_ids(&fluree, &pinned, window).await,
            id_range(1100, 1200)
        );
        assert_eq!(
            jsonld_window_ids(&fluree, &pinned, "(>= ?id 1100)").await,
            id_range(1100, 1200)
        );
        assert_eq!(sparql_window_ids(&fluree, &pinned, "").await.len(), 1200);
        assert_eq!(sparql_window_ids(&fluree, current, "").await.len(), 3000);
    }
}

const RENAMED_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#Renamed>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.renamed" ] ;
        rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:id ;
            rr:objectMap [ rr:column "id" ; rr:datatype xsd:long ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:name ;
            rr:objectMap [ rr:column "full_name" ]
        ] .
"#;

/// A private copy of `silver.renamed` behind graph source `alias`, and the id
/// of its first snapshot (written before the rename).
async fn staged_renamed(alias: &str) -> (fluree_db_api::Fluree, tempfile::TempDir, i64) {
    allow_fixture_roots();
    let staging =
        tempfile::TempDir::new_in(fixtures_dir()).expect("staging dir under the fixtures root");
    let table_dir = staging.path().join("renamed");
    copy_dir_all(&fixtures_dir().join("silver/renamed"), &table_dir);
    let latest = std::fs::read_dir(table_dir.join("metadata"))
        .expect("metadata dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
        .max()
        .expect("a metadata file");
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(latest).expect("read metadata")).unwrap();
    let first = meta["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["sequence-number"] == 1)
        .expect("a first commit")["snapshot-id"]
        .as_i64()
        .unwrap();
    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct(
        alias,
        format!("file://{}", table_dir.display()),
        RENAMED_R2RML,
    )
    .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create renamed graph source");
    (fluree, staging, first)
}

/// `(id, name)` pairs (sorted) a SPARQL query over `from` returns.
async fn sparql_names(
    fluree: &fluree_db_api::Fluree,
    from: &str,
    filter: &str,
) -> Vec<(i64, String)> {
    let filter = if filter.is_empty() {
        String::new()
    } else {
        format!("FILTER({filter})")
    };
    let sparql = format!(
        "PREFIX ex: <http://example.org/> SELECT ?id ?name FROM <{from}> \
         WHERE {{ ?s ex:id ?id ; ex:name ?name {filter} }}"
    );
    let result = fluree
        .query_from()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"));
    let mut rows: Vec<(i64, String)> = result["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("bindings for {sparql}: {result}"))
        .iter()
        .map(|b| {
            (
                b["id"]["value"].as_str().unwrap().parse().unwrap(),
                b["name"]["value"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    rows.sort();
    rows
}

/// The JSON-LD twin of [`sparql_names`] (a filter is required).
async fn jsonld_names(
    fluree: &fluree_db_api::Fluree,
    from: &str,
    filter: &str,
) -> Vec<(i64, String)> {
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": from,
        "select": ["?id", "?name"],
        "where": [{"@id": "?s", "ex:id": "?id", "ex:name": "?name"}, ["filter", filter]],
    });
    let result = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut rows: Vec<(i64, String)> = result
        .as_array()
        .unwrap_or_else(|| panic!("rows for {query}: {result}"))
        .iter()
        .map(|row| {
            (
                row[0].as_i64().unwrap(),
                row[1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    rows.sort();
    rows
}

fn people(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
    rows.iter().map(|(id, n)| (*id, n.to_string())).collect()
}

/// A read pinned to a snapshot older than a column rename finds the column
/// under its current name, whichever way the scan is planned: from the
/// manifests (the first scan, filtered or not) or from the kept file list
/// (later scans, filtered or not). Each plan's tasks name their columns by
/// the schema they carry, and the mapping names columns as they are now.
#[tokio::test]
async fn a_pinned_read_finds_a_renamed_column_under_its_current_name() {
    let early = people(&[(1, "alice"), (2, "bob"), (3, "carol")]);
    let all = people(&[
        (1, "alice"),
        (2, "bob"),
        (3, "carol"),
        (4, "dave"),
        (5, "erin"),
    ]);

    // Unfiltered first: planned from the manifests, then from the kept list.
    let (fluree, _staging, first) = staged_renamed("renamed-a").await;
    let pinned = format!("renamed-a:main@snapshot:{first}");
    assert_eq!(
        sparql_names(&fluree, &pinned, "").await,
        early,
        "pinned, cold"
    );
    assert_eq!(
        sparql_names(&fluree, &pinned, "").await,
        early,
        "pinned, warm"
    );
    assert_eq!(
        sparql_names(&fluree, &pinned, "?id >= 2").await,
        early[1..],
        "pinned, filtered, warm"
    );
    assert_eq!(
        jsonld_names(&fluree, &pinned, "(>= ?id 2)").await,
        early[1..],
        "pinned, filtered, warm (JSON-LD)"
    );
    assert_eq!(sparql_names(&fluree, "renamed-a:main", "").await, all);

    // Filtered first: the filtered plan is the one read from the manifests.
    let (fluree, _staging, first) = staged_renamed("renamed-b").await;
    let pinned = format!("renamed-b:main@snapshot:{first}");
    assert_eq!(
        jsonld_names(&fluree, &pinned, "(<= ?id 2)").await,
        early[..2],
        "pinned, filtered, cold (JSON-LD)"
    );
    assert_eq!(
        sparql_names(&fluree, &pinned, "?id <= 2").await,
        early[..2],
        "pinned, filtered, warm"
    );
    assert_eq!(
        sparql_names(&fluree, "renamed-b:main", "?id > 3").await,
        all[3..],
        "current, filtered"
    );
}

/// A materialize read whose window ends at a snapshot older than a column
/// rename keeps the column: the projection is resolved, and the batches are
/// named, under current names, so a twin built from it carries every row's
/// value. Covers the incremental read and the streaming read pinned to that
/// snapshot.
#[tokio::test]
async fn a_materialize_read_before_a_rename_keeps_the_renamed_column() {
    let (fluree, _staging, first) = staged_renamed("renamed-m").await;
    let provider = FlureeR2rmlProvider::new(&fluree);
    let projection = vec!["id".to_string(), "full_name".to_string()];
    let names_of = |batches: &[fluree_db_iceberg::ColumnBatch]| {
        let mut rows: Vec<(i64, String)> = batches
            .iter()
            .flat_map(|b| {
                let ids = b.column_by_name("id").expect("an id column");
                let names = b
                    .column_by_name("full_name")
                    .unwrap_or_else(|| panic!("a full_name column in {:?}", b.schema));
                (0..b.num_rows)
                    .map(|i| {
                        (
                            ids.get_i64(i).expect("an id"),
                            names.get_string(i).expect("a name").to_string(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        rows.sort();
        rows
    };
    let early = people(&[(1, "alice"), (2, "bob"), (3, "carol")]);

    let batches = provider
        .scan_table_incremental("renamed-m:main", "silver.renamed", &projection, None, first)
        .await
        .expect("incremental read to the first snapshot");
    assert_eq!(names_of(&batches), early, "incremental");

    let scan = provider
        .scan_for_materialize_stream(
            "renamed-m:main",
            "silver.renamed",
            &projection,
            None,
            Some(first),
            None,
        )
        .await
        .expect("pinned materialize read");
    let batches: Vec<_> = scan.stream.try_collect().await.expect("stream batches");
    assert_eq!(names_of(&batches), early, "pinned stream");
}

/// `people_window` read as five files with disjoint `id` ranges (1..=600,
/// 601..=1200, …), so a scan-side top-k by `id` can prune files.
const WINDOW_PEOPLE_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#WindowMapping>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people_window" ] ;
        rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:id ;
            rr:objectMap [ rr:column "id" ; rr:datatype xsd:integer ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:name ;
            rr:objectMap [ rr:column "name" ]
        ] .
"#;

/// The ids (in answer order) of the first column named `id` in a SPARQL or
/// JSON-LD result.
fn answer_ids(result: &serde_json::Value) -> Vec<i64> {
    let parse = |v: &serde_json::Value| -> i64 {
        match v {
            serde_json::Value::Number(n) => n.as_i64().unwrap(),
            serde_json::Value::String(s) => s.parse().unwrap(),
            serde_json::Value::Object(o) => match &o["value"] {
                serde_json::Value::String(s) => s.parse().unwrap(),
                other => other.as_i64().unwrap(),
            },
            other => panic!("not an id: {other}"),
        }
    };
    if let Some(bindings) = result["results"]["bindings"].as_array() {
        return bindings.iter().map(|b| parse(&b["id"])).collect();
    }
    result
        .as_array()
        .unwrap_or_else(|| panic!("not a result: {result}"))
        .iter()
        .map(|row| parse(&row[0]))
        .collect()
}

/// `people_window.id` references `cells.i32`; persons 9 and 10 reference
/// cells 2 and 5 (each key sits on two cells, and the smaller wins), and most
/// persons' references dangle.
const PERSON_CELL_TOPK_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#Person>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people_window" ] ;
        rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:id ;
            rr:objectMap [ rr:column "id" ; rr:datatype xsd:integer ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:cell ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Cell> ;
                rr:joinCondition [ rr:child "id" ; rr:parent "i32" ]
            ]
        ] .

    <http://example.org/mapping#Cell>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.cells" ] ;
        rr:subjectMap [ rr:template "http://example.org/cell/{id}" ] .
"#;

/// A `VALUES` binding the object of a reference: `ORDER BY DESC(?id) LIMIT 3`
/// answers the persons that reference those cells (ids 10 and 9), not the
/// table's top three ids, which the join with the `VALUES` then drops.
#[tokio::test]
async fn order_by_limit_over_a_scan_its_input_binds_answers_the_bound_rows() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = format!(
        "file://{}/tests/fixtures/iceberg/silver",
        env!("CARGO_MANIFEST_DIR")
    );
    let config = R2rmlCreateConfig::new_direct("topk-cell", &root, PERSON_CELL_TOPK_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create graph source");

    let cells = [
        "http://example.org/cell/2".to_string(),
        "http://example.org/cell/5".to_string(),
    ];
    let mut wrong = Vec::new();
    let sparql = format!(
        "SELECT ?id FROM <topk-cell:main> WHERE {{ VALUES ?c {{ {} }} \
         ?p <http://example.org/cell> ?c ; <http://example.org/id> ?id }} \
         ORDER BY DESC(?id) LIMIT 3",
        cells
            .iter()
            .map(|c| format!("<{c}>"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    let result = fluree
        .query_from()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"));
    if answer_ids(&result) != [10, 9] {
        wrong.push(format!("SPARQL: {result}"));
    }
    let ids: Vec<serde_json::Value> = cells
        .iter()
        .map(|c| serde_json::json!({"@id": c}))
        .collect();
    let jsonld = serde_json::json!({
        "from": "topk-cell:main",
        "select": ["?id"],
        "where": [
            ["values", ["?c", ids]],
            {"@id": "?p", "http://example.org/cell": "?c", "http://example.org/id": "?id"}
        ],
        "orderBy": "(desc ?id)",
        "limit": 3,
    });
    let result = fluree
        .query_from()
        .jsonld(&jsonld)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("JSON-LD: {e}"));
    if answer_ids(&result) != [10, 9] {
        wrong.push(format!("JSON-LD: {result}"));
    }
    assert!(
        wrong.is_empty(),
        "expected ids [10, 9]:\n{}",
        wrong.join("\n")
    );
}

/// `ORDER BY … LIMIT k` over a scan whose rows are joined to its input (a
/// `VALUES` block, here) answers the top k of the JOINED rows. The scan-side
/// top-k keeps the k best rows the reader returns and skips every file that
/// cannot beat them, which is sound only when the reader returns no row the
/// operator later drops. The join with the `VALUES` drops rows, and a pushed
/// `IN` may be applied only partly by the reader: fed those rows, the heap
/// fills with ids 2401..=3000 from the last file, stops before the first, and
/// loses id 5.
#[tokio::test]
async fn order_by_limit_over_a_joined_scan_answers_the_joined_top_k() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let location = format!(
        "file://{}/tests/fixtures/iceberg/silver/people_window",
        env!("CARGO_MANIFEST_DIR")
    );
    let config = R2rmlCreateConfig::new_direct("topk-window", &location, WINDOW_PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create graph source");

    let cases = [
        // A single-var VALUES also pushes an `IN` on the id column.
        (
            "VALUES ?id { 5 2500 }",
            serde_json::json!(["values", ["?id", [5, 2500]]]),
        ),
        // A two-var VALUES pushes nothing; the join alone drops rows.
        (
            "VALUES (?s ?id) { (<http://example.org/person/5> 5) (<http://example.org/person/2500> 2500) }",
            serde_json::json!(["values", [["?s", "?id"], [
                [{"@id": "http://example.org/person/5"}, 5],
                [{"@id": "http://example.org/person/2500"}, 2500]
            ]]]),
        ),
    ];
    let mut wrong = Vec::new();
    for (values, jsonld_values) in cases {
        let sparql = format!(
            "SELECT ?id FROM <topk-window:main> \
             WHERE {{ {values} ?s <http://example.org/id> ?id }} \
             ORDER BY DESC(?id) LIMIT 2"
        );
        let result = fluree
            .query_from()
            .sparql(&sparql)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("{sparql}: {e}"));
        if answer_ids(&result) != [2500, 5] {
            wrong.push(format!("SPARQL {values}: {result}"));
        }

        let jsonld = serde_json::json!({
            "@context": {"ex": "http://example.org/"},
            "from": "topk-window:main",
            "select": ["?id"],
            "where": [jsonld_values, {"@id": "?s", "ex:id": "?id"}],
            "orderBy": "(desc ?id)",
            "limit": 2,
        });
        let result = fluree
            .query_from()
            .jsonld(&jsonld)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("JSON-LD {values}: {e}"));
        if answer_ids(&result) != [2500, 5] {
            wrong.push(format!("JSON-LD {values}: {result}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "expected ids [2500, 5]:\n{}",
        wrong.join("\n")
    );
}

/// The rows of a SPARQL (bindings) or JSON-LD (array of rows) result, each as
/// the text of its `vars` (an IRI, or a literal's lexical form), sorted.
fn answer_rows(result: &serde_json::Value, vars: &[&str]) -> Vec<Vec<String>> {
    let text = |v: &serde_json::Value| -> String {
        match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Object(o) => o
                .get("value")
                .or_else(|| o.get("@id"))
                .or_else(|| o.get("@value"))
                .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
                .unwrap_or_else(|| panic!("not a term: {v}")),
            other => panic!("not a term: {other}"),
        }
    };
    let mut rows: Vec<Vec<String>> =
        if let Some(bindings) = result["results"]["bindings"].as_array() {
            bindings
                .iter()
                .map(|b| vars.iter().map(|v| text(&b[*v])).collect())
                .collect()
        } else {
            result
                .as_array()
                .unwrap_or_else(|| panic!("not a result: {result}"))
                .iter()
                .map(|row| match row {
                    serde_json::Value::Array(cells) => cells.iter().map(text).collect(),
                    cell => vec![text(cell)],
                })
                .collect()
        };
    rows.sort();
    rows
}

fn person(id: u32) -> String {
    format!("http://example.org/person/{id}")
}

/// A `VALUES` block of subjects narrows the scan to their template keys (on
/// five files with disjoint `id` ranges, the two subjects' files). Subjects the
/// template cannot produce, or that no row holds, contribute nothing.
#[tokio::test]
async fn a_values_subject_set_answers_its_subjects() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let location = format!(
        "file://{}/tests/fixtures/iceberg/silver/people_window",
        env!("CARGO_MANIFEST_DIR")
    );
    let config = R2rmlCreateConfig::new_direct("keys-window", &location, WINDOW_PEOPLE_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create graph source");

    let subjects = [
        person(5),
        person(2500),
        person(99_999),
        "http://other.example/person/7".to_string(),
    ];
    let expected = vec![
        vec![person(2500), "person-2500".to_string()],
        vec![person(5), "person-0005".to_string()],
    ];
    let sparql = format!(
        "SELECT ?s ?n FROM <keys-window:main> WHERE {{ VALUES ?s {{ {} }} \
         ?s <http://example.org/name> ?n }}",
        subjects
            .iter()
            .map(|s| format!("<{s}>"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    let result = fluree
        .query_from()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"));
    assert_eq!(
        answer_rows(&result, &["s", "n"]),
        expected,
        "SPARQL: {result}"
    );

    let ids: Vec<serde_json::Value> = subjects
        .iter()
        .map(|s| serde_json::json!({"@id": s}))
        .collect();
    let jsonld = serde_json::json!({
        "from": "keys-window:main",
        "select": ["?s", "?n"],
        "where": [
            ["values", ["?s", ids]],
            {"@id": "?s", "http://example.org/name": "?n"}
        ],
    });
    let result = fluree
        .query_from()
        .jsonld(&jsonld)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("JSON-LD: {e}"));
    assert_eq!(
        answer_rows(&result, &["s", "n"]),
        expected,
        "JSON-LD: {result}"
    );
}

/// `people_window.id` references `cells.i32`, whose values 9 and 10 each sit
/// on two rows (cells 2 and 4, cells 5 and 7) and whose 2 and 100 sit on one;
/// every other person's reference dangles. The join keeps the smaller cell
/// for a duplicated key and drops a dangling reference, whether the parent is
/// read whole (no LIMIT: the first window holds 3,000 keys) or by the keys of
/// a `LIMIT`'s first rows or a `VALUES` block's subjects.
const PERSON_CELL_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#Person>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people_window" ] ;
        rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:cell ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Cell> ;
                rr:joinCondition [ rr:child "id" ; rr:parent "i32" ]
            ]
        ] .

    <http://example.org/mapping#Cell>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.cells" ] ;
        rr:subjectMap [ rr:template "http://example.org/cell/{id}" ] .
"#;

#[tokio::test]
async fn a_referencing_map_keeps_its_answers_when_the_parent_is_read_by_key() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = format!(
        "file://{}/tests/fixtures/iceberg/silver",
        env!("CARGO_MANIFEST_DIR")
    );
    let config = R2rmlCreateConfig::new_direct("person-cell", &root, PERSON_CELL_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create graph source");

    let cell = |id: u32| format!("http://example.org/cell/{id}");
    let all = vec![
        vec![person(10), cell(5)],
        vec![person(100), cell(6)],
        vec![person(2), cell(1)],
        vec![person(9), cell(2)],
    ];
    let values = [person(9), person(10), person(11)];
    let values_answer = vec![vec![person(10), cell(5)], vec![person(9), cell(2)]];

    let mut wrong = Vec::new();
    for (label, sparql_where, jsonld_values, limit) in [
        ("all", String::new(), None, None),
        ("LIMIT 2", String::new(), None, Some(2)),
        (
            "VALUES",
            format!(
                "VALUES ?p {{ {} }}",
                values
                    .iter()
                    .map(|s| format!("<{s}>"))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            Some(
                values
                    .iter()
                    .map(|s| serde_json::json!({"@id": s}))
                    .collect::<Vec<_>>(),
            ),
            None,
        ),
    ] {
        let sparql = format!(
            "SELECT ?p ?c FROM <person-cell:main> WHERE {{ {sparql_where} \
             ?p <http://example.org/cell> ?c }}{}",
            limit.map_or(String::new(), |k| format!(" LIMIT {k}"))
        );
        let sparql_result = fluree
            .query_from()
            .sparql(&sparql)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("{sparql}: {e}"));

        let mut where_clause = Vec::new();
        if let Some(ids) = jsonld_values {
            where_clause.push(serde_json::json!(["values", ["?p", ids]]));
        }
        where_clause.push(serde_json::json!({"@id": "?p", "http://example.org/cell": "?c"}));
        let mut jsonld = serde_json::json!({
            "from": "person-cell:main",
            "select": ["?p", "?c"],
            "where": where_clause,
        });
        if let Some(k) = limit {
            jsonld["limit"] = serde_json::json!(k);
        }
        let jsonld_result = fluree
            .query_from()
            .jsonld(&jsonld)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("JSON-LD {label}: {e}"));

        for (surface, result) in [("SPARQL", &sparql_result), ("JSON-LD", &jsonld_result)] {
            let rows = answer_rows(result, &["p", "c"]);
            let ok = match label {
                "all" => rows == all,
                "LIMIT 2" => rows.len() == 2 && rows.iter().all(|r| all.contains(r)),
                _ => rows == values_answer,
            };
            if !ok {
                wrong.push(format!("{surface} {label}: {rows:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "wrong answers:\n{}", wrong.join("\n"));
}

/// Each `people_window` row references its own twin through a referencing
/// object map over the same table.
const WINDOW_TWIN_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#Person>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people_window" ] ;
        rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
        rr:predicateObjectMap [
            rr:predicate ex:twin ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Twin> ;
                rr:joinCondition [ rr:child "id" ; rr:parent "id" ]
            ]
        ] .

    <http://example.org/mapping#Twin>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people_window" ] ;
        rr:subjectMap [ rr:template "http://example.org/twin/{id}" ] .
"#;

/// A parent read by key decides whether to probe from the keyed read's file
/// counts, planned from the snapshot's kept file list like any scan: once the
/// list is kept, the data manifests can go and a `LIMIT` (probing its first
/// rows' keys) and a `VALUES` (pushing its subjects, then probing) still answer.
/// A file-count plan that re-read the manifests would fail here.
#[tokio::test]
async fn a_parent_probe_plans_its_file_counts_from_the_kept_list() {
    allow_fixture_roots();
    let staging =
        tempfile::TempDir::new_in(fixtures_dir()).expect("staging dir under the fixtures root");
    let table_dir = staging.path().join("people_window");
    copy_dir_all(&fixtures_dir().join("silver/people_window"), &table_dir);
    let fluree = FlureeBuilder::memory().build_memory();
    let config = R2rmlCreateConfig::new_direct(
        "window-twin",
        format!("file://{}", table_dir.display()),
        WINDOW_TWIN_R2RML,
    )
    .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create window graph source");

    let twin = |id: u32| format!("http://example.org/twin/{id}");
    let values = format!("VALUES ?p {{ <{}> <{}> }}", person(5), person(2500));
    let ask = |body: String, limit: &'static str| {
        let fluree = &fluree;
        async move {
            let sparql = format!(
                "SELECT ?p ?q FROM <window-twin:main> WHERE {{ {body} \
                 ?p <http://example.org/twin> ?q }}{limit}"
            );
            let result = fluree
                .query_from()
                .sparql(&sparql)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{sparql}: {e}"));
            answer_rows(&result, &["p", "q"])
        }
    };
    let ask_jsonld = || {
        let fluree = &fluree;
        async move {
            let jsonld = serde_json::json!({
                "from": "window-twin:main",
                "select": ["?p", "?q"],
                "where": [
                    ["values", ["?p", [{"@id": person(5)}, {"@id": person(2500)}]]],
                    {"@id": "?p", "http://example.org/twin": "?q"}
                ],
            });
            let result = fluree
                .query_from()
                .jsonld(&jsonld)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("JSON-LD: {e}"));
            answer_rows(&result, &["p", "q"])
        }
    };
    let bound = vec![vec![person(2500), twin(2500)], vec![person(5), twin(5)]];
    let twins = |rows: &[Vec<String>]| {
        rows.len() == 2
            && rows
                .iter()
                .all(|r| r[1] == r[0].replace("/person/", "/twin/"))
    };

    // The first queries read the manifests and keep the file list.
    assert_eq!(ask(values.clone(), "").await, bound);
    assert!(twins(&ask(String::new(), " LIMIT 2").await));
    assert_eq!(
        delete_data_manifests(&staging),
        5,
        "one manifest per append"
    );

    // Every plan now comes from the kept list, the probes' file counts too.
    assert_eq!(ask(values, "").await, bound, "SPARQL VALUES");
    assert_eq!(ask_jsonld().await, bound, "JSON-LD VALUES");
    let limited = ask(String::new(), " LIMIT 2").await;
    assert!(twins(&limited), "SPARQL LIMIT 2: {limited:?}");
}

// ---------------------------------------------------------------------------
// Table keys over the `sets` fixtures: what an Iceberg source reports, and a
// declared `f:key` checked against the pinned snapshot's schema.
// ---------------------------------------------------------------------------

/// `sets.customers_current` (customer_id an Iceberg identifier field) and
/// `sets.customers_history` (no identifier field) minting the same subjects;
/// `history_table_extra` follows the history table's name on its logical
/// table (an `f:key`, or nothing).
fn sets_customers_r2rml(history_table_extra: &str) -> String {
    format!(
        r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix ex: <http://example.org/> .

        <http://example.org/mapping#Current>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "sets.customers_current" ] ;
            rr:subjectMap [
                rr:template "http://example.org/customer/{{customer_id}}" ;
                rr:class ex:Customer
            ] ;
            rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .

        <http://example.org/mapping#History>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "sets.customers_history" {history_table_extra} ] ;
            rr:subjectMap [
                rr:template "http://example.org/customer/{{customer_id}}" ;
                rr:class ex:Customer
            ] ;
            rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
    "#
    )
}

/// A graph source `alias` over the committed `sets` tables (a warehouse root:
/// each table name resolves to its directory), mapped by `mapping`.
async fn sets_source(alias: &str, mapping: &str) -> fluree_db_api::Fluree {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = format!(
        "file://{}/tests/fixtures/iceberg/sets",
        env!("CARGO_MANIFEST_DIR")
    );
    let config =
        R2rmlCreateConfig::new_direct(alias, &root, mapping).with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create sets graph source");
    fluree
}

#[tokio::test]
async fn an_iceberg_identifier_field_is_a_table_key() {
    use fluree_db_query::r2rml::{effective_table_keys, R2rmlProvider, R2rmlTableProvider};
    let fluree = sets_source("sets-keys", &sets_customers_r2rml("")).await;
    let provider = FlureeR2rmlProvider::new(&fluree);
    let gs = "sets-keys:main";

    assert_eq!(
        provider
            .table_keys(gs, "sets.customers_current", None)
            .await
            .expect("keys of customers_current"),
        Some(vec![vec!["customer_id".to_string()]])
    );
    assert_eq!(
        provider
            .table_keys(gs, "sets.customers_history", None)
            .await
            .expect("keys of customers_history"),
        None
    );

    // Through the one way a lane obtains keys: the current table's subjects
    // are unique, the history table's are not.
    let mapping = provider.compiled_mapping(gs, None).await.expect("mapping");
    for (table, unique) in [
        ("sets.customers_current", true),
        ("sets.customers_history", false),
    ] {
        let keys = effective_table_keys(&provider, &mapping, gs, table, None)
            .await
            .expect("effective keys");
        let tm = mapping.find_maps_for_table(table)[0];
        assert_eq!(tm.subject_unique_under(&keys), unique, "{table}: {keys:?}");
    }
}

#[tokio::test]
async fn a_declared_key_unites_with_what_the_table_reports() {
    use fluree_db_query::r2rml::{effective_table_keys, R2rmlProvider};
    // The history table's natural key carries the version column, so it does
    // not make the per-customer subject unique.
    let fluree = sets_source(
        "sets-declared",
        &sets_customers_r2rml(r#"; f:key ( "customer_id" "valid_from" )"#),
    )
    .await;
    let provider = FlureeR2rmlProvider::new(&fluree);
    let gs = "sets-declared:main";
    let mapping = provider.compiled_mapping(gs, None).await.expect("mapping");
    let keys = effective_table_keys(&provider, &mapping, gs, "sets.customers_history", None)
        .await
        .expect("a usable declared key");
    assert_eq!(
        keys,
        fluree_db_r2rml::TableKeys::new([["customer_id", "valid_from"]])
    );
    let history = mapping.find_maps_for_table("sets.customers_history")[0];
    assert!(!history.subject_unique_under(&keys));
}

#[tokio::test]
async fn a_declared_key_the_table_cannot_hold_is_refused() {
    use fluree_db_query::r2rml::{effective_table_keys, R2rmlProvider, R2rmlTableProvider};
    // A column the snapshot lacks (a misspelling, or other casing).
    for (key, says) in [
        (
            r#"( "customer_idx" )"#,
            r#"column "customer_idx", which the table does not have"#,
        ),
        (
            r#"( "Customer_ID" )"#,
            r#"column "Customer_ID", which the table does not have"#,
        ),
    ] {
        let alias = format!("sets-bad-{}", key.len());
        let fluree = sets_source(&alias, &sets_customers_r2rml(&format!("; f:key {key}"))).await;
        let provider = FlureeR2rmlProvider::new(&fluree);
        let gs = format!("{alias}:main");
        let err = provider
            .table_keys(&gs, "sets.customers_history", None)
            .await
            .expect_err("a key the table cannot hold refuses");
        let msg = err.to_string();
        assert!(
            msg.contains(says) && msg.contains("sets.customers_history"),
            "{key}: {msg}"
        );
        // The choke point every lane goes through refuses too, rather than
        // falling back to the declaration.
        let mapping = provider.compiled_mapping(&gs, None).await.expect("mapping");
        assert!(
            effective_table_keys(&provider, &mapping, &gs, "sets.customers_history", None)
                .await
                .is_err()
        );
        // A table that declares nothing is unaffected.
        assert!(provider
            .table_keys(&gs, "sets.customers_current", None)
            .await
            .is_ok());
    }

    // A floating-point column, on silver.people's `score`.
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let mapping = PEOPLE_R2RML.replace(
        r#"rr:logicalTable [ rr:tableName "silver.people" ]"#,
        r#"rr:logicalTable [ rr:tableName "silver.people" ; <https://ns.flur.ee/db#key> ( "score" ) ]"#,
    );
    assert_ne!(mapping, PEOPLE_R2RML, "the key was spliced in");
    let config = R2rmlCreateConfig::new_direct("people-float-key", &table_location(), &mapping)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("registration compiles the mapping; the schema is checked at query time");
    let provider = FlureeR2rmlProvider::new(&fluree);
    let msg = provider
        .table_keys("people-float-key:main", "silver.people", None)
        .await
        .expect_err("a floating-point key refuses")
        .to_string();
    assert!(
        msg.contains(r#"column "score" of type Float64"#) && msg.contains("floating-point"),
        "{msg}"
    );
}
