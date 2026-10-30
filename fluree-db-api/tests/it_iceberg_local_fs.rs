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

    // A BM25 index is not a graph: named as the dataset, pinned or not, it is
    // refused as one (a pin on a SQL source, which is one, is refused in
    // it_sql_graph_source).
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
            "is a BM25 index, not a graph",
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

/// Store an Iceberg graph-source record with no R2RML mapping, as registration
/// once allowed: no surface can register one now, but records stored then
/// remain. Its location is a real table, so only the missing mapping can be
/// why a query of it fails.
async fn publish_unmapped_source(fluree: &fluree_db_api::Fluree, name: &str) -> String {
    let config = fluree_db_api::IcebergCreateConfig::new_direct(name, table_location())
        .to_iceberg_gs_config()
        .to_json()
        .expect("serialize config");
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_graph_source(
            name,
            "main",
            fluree_db_nameservice::GraphSourceType::Iceberg,
            &config,
            &[],
        )
        .await
        .expect("publish unmapped record");
    format!("{name}:main")
}

/// The refusal every route gives a query that names a graph source with no
/// mapping: the typed code and status, naming the source and the cause.
fn assert_unmapped<T: std::fmt::Debug>(result: Result<T, ApiError>, gs: &str, route: &str) {
    let err = match result {
        Ok(answer) => panic!("{route}: answered instead of refusing: {answer:?}"),
        Err(err) => err,
    };
    assert_eq!(
        err.error_type(),
        "err:db/GraphSourceUnmapped",
        "{route}: {err}"
    );
    assert_eq!(err.status_code(), 409, "{route}: {err}");
    let message = err.to_string();
    assert!(
        message.contains(&format!("graph source '{gs}' has no R2RML mapping")),
        "{route}: {message}"
    );
    assert!(!message.contains("not found"), "{route}: {message}");
}

const NAME_SPARQL: &str = "SELECT ?name WHERE { ?s <http://example.org/name> ?name }";

fn name_jsonld() -> serde_json::Value {
    serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
    })
}

/// A graph source with no mapping queried as the target itself refuses with
/// its cause. Read as "not a mapped source", the target fell through to the
/// ledger lookup and answered "Ledger not found".
#[tokio::test]
async fn an_unmapped_graph_source_refuses_a_query_that_targets_it() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let gs = publish_unmapped_source(&fluree, "unmapped-target").await;

    let graph = fluree.graph(&gs);
    assert_unmapped(
        graph.query().sparql(NAME_SPARQL).execute_formatted().await,
        &gs,
        "SPARQL target",
    );
    let jsonld = name_jsonld();
    assert_unmapped(
        graph.query().jsonld(&jsonld).execute_formatted().await,
        &gs,
        "JSON-LD target",
    );

    // Materializing it reads the same mapping, and refuses the same way.
    assert_unmapped(
        fluree
            .materialize_r2rml_graph_source(&gs, "unmapped-target-copy:main", false)
            .await
            .map(|_| ()),
        &gs,
        "materialize",
    );

    // The remedy the refusal names: registering it again under the same name,
    // with a mapping, replaces the record, and the source answers.
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("unmapped-target", table_location(), PEOPLE_R2RML)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("register again with a mapping");
    let rows = fluree
        .graph(&gs)
        .query()
        .sparql(NAME_SPARQL)
        .execute()
        .await
        .expect("query the remapped source");
    assert_eq!(rows.row_count(), 5, "the remapped source answers");
}

/// A graph source with no mapping named in a dataset (`FROM`, `FROM NAMED`)
/// refuses with its cause. It used to resolve to an empty genesis graph and
/// answer no rows without an error.
#[tokio::test]
async fn an_unmapped_graph_source_refuses_a_query_that_names_it_in_its_dataset() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let gs = publish_unmapped_source(&fluree, "unmapped-dataset").await;
    fluree.create_ledger("crm:main").await.expect("ledger");

    let from = format!("SELECT ?name FROM <{gs}> WHERE {{ ?s <http://example.org/name> ?name }}");
    assert_unmapped(
        fluree.query_from().sparql(&from).execute_formatted().await,
        &gs,
        "SPARQL FROM",
    );
    let mut from_jsonld = name_jsonld();
    from_jsonld["from"] = gs.clone().into();
    assert_unmapped(
        fluree
            .query_from()
            .jsonld(&from_jsonld)
            .execute_formatted()
            .await,
        &gs,
        "JSON-LD from",
    );

    // Every server SPARQL request carries governance options; that branch once
    // ran without the graph-source providers, so the refusal must not depend
    // on them.
    assert_unmapped(
        fluree
            .query_from()
            .sparql(&from)
            .connection_opts(fluree_db_api::GovernanceOptions::default())
            .execute_formatted()
            .await,
        &gs,
        "SPARQL FROM with governance options",
    );
    // `query_dataset` runs a dataset built up front without the graph-source
    // providers, so their probe never sees the source: resolving it into the
    // dataset is what has to refuse.
    let spec = fluree_db_api::DatasetSpec::new()
        .with_default(fluree_db_api::GraphSource::parse(&gs).expect("parse source"));
    assert_unmapped(
        fluree.build_dataset_view(&spec).await.map(|_| ()),
        &gs,
        "build_dataset_view",
    );

    // A tracked query reports only a status and a message.
    match fluree.query_from().sparql(&from).execute_tracked().await {
        Ok(answer) => panic!("tracked SPARQL FROM: answered {}", answer.result),
        Err(err) => {
            assert_eq!(err.status, 409, "tracked SPARQL FROM: {}", err.error);
            assert!(
                err.error.contains("has no R2RML mapping"),
                "tracked SPARQL FROM: {}",
                err.error
            );
        }
    }

    let named = format!(
        "SELECT ?name FROM <crm:main> FROM NAMED <{gs}> \
         WHERE {{ GRAPH <{gs}> {{ ?s <http://example.org/name> ?name }} }}"
    );
    assert_unmapped(
        fluree.query_from().sparql(&named).execute_formatted().await,
        &gs,
        "SPARQL FROM NAMED + GRAPH",
    );
    let named_jsonld = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": "crm:main",
        "fromNamed": [gs],
        "select": ["?name"],
        "where": [["graph", gs, {"@id": "?s", "ex:name": "?name"}]],
    });
    assert_unmapped(
        fluree
            .query_from()
            .jsonld(&named_jsonld)
            .execute_formatted()
            .await,
        &gs,
        "JSON-LD fromNamed + graph",
    );
}

/// `GRAPH <source>` inside a query of a ledger, with no dataset naming the
/// source, asks the R2RML probe directly rather than resolving a dataset. A
/// source with no mapping refuses there too, instead of matching nothing.
#[tokio::test]
async fn an_unmapped_graph_source_refuses_a_graph_pattern_in_a_ledger_query() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let gs = publish_unmapped_source(&fluree, "unmapped-graph").await;
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

    let graph = fluree.graph("crm:main");
    let sparql =
        format!("SELECT ?name WHERE {{ GRAPH <{gs}> {{ ?s <http://example.org/name> ?name }} }}");
    assert_unmapped(
        graph.query().sparql(&sparql).execute_formatted().await,
        &gs,
        "SPARQL GRAPH in a ledger query",
    );
    let jsonld = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?name"],
        "where": [["graph", gs, {"@id": "?s", "ex:name": "?name"}]],
    });
    assert_unmapped(
        graph.query().jsonld(&jsonld).execute_formatted().await,
        &gs,
        "JSON-LD graph in a ledger query",
    );
}

/// Registering a Direct source reaches its tables the way its queries do (a
/// warehouse root's per-table directory included) and reports the outcome as
/// `connection_tested`, as registering a REST source does. A table that cannot
/// be read still registers, and then fails when queried.
#[tokio::test]
async fn a_direct_registration_reports_whether_its_tables_resolve() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let register = |name: &'static str, location: String| {
        let fluree = &fluree;
        async move {
            fluree
                .create_r2rml_graph_source(
                    R2rmlCreateConfig::new_direct(name, &location, PEOPLE_R2RML)
                        .with_mapping_media_type("text/turtle"),
                )
                .await
                .unwrap_or_else(|e| panic!("register {name}: {e}"))
        }
    };

    let table = register("probe-table", table_location()).await;
    assert!(table.connection_tested, "a readable table is verified");

    // The mapping's `silver.people` resolves to its own directory beneath the
    // root, as it does when the source is queried.
    let root = format!("file://{}/silver", fixtures_dir().display());
    let rooted = register("probe-root", root).await;
    assert!(
        rooted.connection_tested,
        "a warehouse root resolves the mapping's table"
    );
    let rows = fluree
        .query_from()
        .sparql(
            "SELECT ?name FROM <probe-root:main> \
             WHERE { ?s <http://example.org/name> ?name }",
        )
        .execute()
        .await
        .expect("query the warehouse-root source");
    assert_eq!(rows.row_count(), 5, "the probe and the query agree");

    let missing = format!("file://{}/silver/no_such_table", fixtures_dir().display());
    let unread = register("probe-missing", missing).await;
    assert!(
        !unread.connection_tested,
        "a table that cannot be read is not verified"
    );
    assert!(
        matches!(
            &unread.connection_check,
            fluree_db_api::ConnectionCheck::TableFailed { table, .. } if table == "silver.people"
        ),
        "the outcome names the table: {:?}",
        unread.connection_check
    );
    assert_eq!(
        table.connection_check,
        fluree_db_api::ConnectionCheck::Verified
    );
    assert!(
        fluree
            .nameservice()
            .lookup_graph_source("probe-missing:main")
            .await
            .expect("lookup")
            .is_some(),
        "and is still registered"
    );
    let err = fluree
        .query_from()
        .sparql(
            "SELECT ?name FROM <probe-missing:main> \
             WHERE { ?s <http://example.org/name> ?name }",
        )
        .execute()
        .await
        .expect_err("a source whose table cannot be read fails when queried");
    assert!(
        err.to_string().contains("no_such_table"),
        "the failure names the location: {err}"
    );
}

/// A REST catalog that serves the committed `people` fixture's current
/// metadata inline at `silver.people` and refuses `silver.refused` (403).
async fn people_catalog() -> wiremock::MockServer {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let metadata_dir = fixtures_dir().join("silver/people/metadata");
    let current = std::fs::read_dir(&metadata_dir)
        .expect("people metadata dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
        .max()
        .expect("a metadata file");
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&current).expect("read metadata"))
            .expect("parse metadata");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/silver/tables/people"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": format!("file://{}", current.display()),
            "metadata": metadata,
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/silver/tables/refused"))
        .respond_with(ResponseTemplate::new(403).set_body_string("not yours"))
        .mount(&server)
        .await;
    server
}

/// Registering a REST source loads each table its mapping names through the
/// catalog, as its queries will, and reports it as `connection_tested`. A
/// source whose tables come from its mapping has no table of its own; the
/// check once loaded the placeholder standing in for one, so it reported
/// `false` for every such source. A table the catalog refuses still
/// registers, reported as not verified.
#[tokio::test]
async fn a_rest_registration_loads_each_mapped_table_through_the_catalog() {
    allow_fixture_roots();
    let catalog = people_catalog().await;
    let fluree = FlureeBuilder::memory().build_memory();
    let register = |name: &'static str, mapping: String| {
        let fluree = &fluree;
        let config = R2rmlCreateConfig::new(
            name,
            catalog.uri(),
            fluree_db_iceberg::config::MAPPING_DEFINED_TABLE,
            mapping,
        )
        .with_mapping_media_type("text/turtle")
        .with_vended_credentials(false)
        .with_s3_region("us-east-1");
        async move {
            fluree
                .create_r2rml_graph_source(config)
                .await
                .unwrap_or_else(|e| panic!("register {name}: {e}"))
        }
    };

    let mapped = register("rest-people", PEOPLE_R2RML.to_string()).await;
    assert!(
        mapped.connection_tested,
        "a table the catalog serves is verified"
    );
    let asked: Vec<String> = catalog
        .received_requests()
        .await
        .expect("request recording")
        .iter()
        .map(|r| r.url.path().to_string())
        .collect();
    assert!(
        asked
            .iter()
            .any(|p| p == "/v1/namespaces/silver/tables/people"),
        "the mapped table was loaded: {asked:?}"
    );
    assert!(
        !asked.iter().any(|p| p.contains("default")),
        "the placeholder is never loaded: {asked:?}"
    );

    let refused = register(
        "rest-refused",
        PEOPLE_R2RML.replace("silver.people", "silver.refused"),
    )
    .await;
    assert!(
        !refused.connection_tested,
        "a table the catalog refuses is not verified"
    );
    match &refused.connection_check {
        fluree_db_api::ConnectionCheck::TableFailed { table, error } => {
            assert_eq!(table, "silver.refused");
            assert!(error.contains("not yours"), "the catalog's reason: {error}");
        }
        other => panic!("expected TableFailed, got {other:?}"),
    }
    assert_eq!(
        mapped.connection_check,
        fluree_db_api::ConnectionCheck::Verified
    );

    // A table the catalog answers it does not have, and a catalog that does
    // not answer, are both a table that did not load; the reason tells them
    // apart.
    let missing = register(
        "rest-missing",
        PEOPLE_R2RML.replace("silver.people", "silver.nope"),
    )
    .await;
    match &missing.connection_check {
        fluree_db_api::ConnectionCheck::TableFailed { table, error } => {
            assert_eq!(table, "silver.nope");
            assert!(error.contains("not found"), "the catalog's answer: {error}");
        }
        other => panic!("expected TableFailed, got {other:?}"),
    }
    let unreachable = fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new(
                "rest-unreachable",
                "http://127.0.0.1:9/catalog",
                fluree_db_iceberg::config::MAPPING_DEFINED_TABLE,
                PEOPLE_R2RML.to_string(),
            )
            .with_mapping_media_type("text/turtle")
            .with_vended_credentials(false)
            .with_s3_region("us-east-1"),
        )
        .await
        .expect("an unreachable catalog still registers");
    match &unreachable.connection_check {
        fluree_db_api::ConnectionCheck::TableFailed { table, error } => {
            assert_eq!(table, "silver.people");
            assert!(!error.contains("not found"), "no catalog answered: {error}");
        }
        other => panic!("expected TableFailed, got {other:?}"),
    }

    // A catalog client that cannot be built loads no table, and is not
    // reported as a table: its catalog credential names a secret that no
    // resolver holds.
    let unbuilt = fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new(
                "rest-unbuilt",
                catalog.uri(),
                fluree_db_iceberg::config::MAPPING_DEFINED_TABLE,
                PEOPLE_R2RML.to_string(),
            )
            .with_mapping_media_type("text/turtle")
            .with_vended_credentials(false)
            .with_s3_region("us-east-1")
            .with_auth_bearer_token_ref("catalog/no-such-token"),
        )
        .await
        .expect("a source whose client cannot be built still registers");
    assert!(!unbuilt.connection_tested);
    assert!(
        matches!(
            unbuilt.connection_check,
            fluree_db_api::ConnectionCheck::ClientFailed { .. }
        ),
        "{:?}",
        unbuilt.connection_check
    );
    assert!(
        fluree
            .nameservice()
            .lookup_graph_source("rest-refused:main")
            .await
            .expect("lookup")
            .is_some(),
        "and is still registered"
    );

    // Verified means every mapped table: one refused among served ones is not.
    let both = format!(
        "{PEOPLE_R2RML}\n{}",
        PEOPLE_R2RML
            .replace("silver.people", "silver.refused")
            .replace("mapping#PeopleMapping", "mapping#RefusedMapping")
    );
    let partial = register("rest-partial", both).await;
    assert_eq!(partial.table_count, 2, "{:?}", partial.table_names);
    assert!(
        !partial.connection_tested,
        "one refused table leaves the source unverified"
    );
}

/// Mappings that parse but define no `rr:TriplesMap`: the `rr:` prefix without
/// its trailing `#` turns every `rr:` term into another IRI, and a misspelled
/// class types no map. Every query of a source with one would answer no rows.
fn mappings_that_define_nothing() -> [(&'static str, String); 2] {
    [
        ("no-hash", PEOPLE_R2RML.replace("r2rml#>", "r2rml>")),
        (
            "misspelled",
            PEOPLE_R2RML.replace("a rr:TriplesMap", "a rr:TripleMap"),
        ),
    ]
}

#[tokio::test]
async fn a_mapping_that_defines_no_triples_map_is_refused_at_registration() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    for (name, mapping) in mappings_that_define_nothing() {
        let err = fluree
            .create_r2rml_graph_source(
                R2rmlCreateConfig::new_direct(name, table_location(), mapping)
                    .with_mapping_media_type("text/turtle"),
            )
            .await
            .expect_err(name);
        assert_eq!(err.status_code(), 400, "{name}: {err}");
        assert!(
            err.to_string().contains(
                "the R2RML mapping defines no rr:TriplesMap; check that the rr: prefix is \
                 http://www.w3.org/ns/r2rml#"
            ),
            "{name}: {err}"
        );
        assert!(
            fluree
                .nameservice()
                .lookup_graph_source(&format!("{name}:main"))
                .await
                .expect("lookup")
                .is_none(),
            "{name}: a refused registration stores no record"
        );
    }
}

/// Store a source whose mapping defines no `rr:TriplesMap`, as registration
/// once allowed: the mapping is in the content store, as a registration would
/// have put it, and the record points at it.
async fn publish_empty_mapping_source(fluree: &fluree_db_api::Fluree, name: &str) -> String {
    use fluree_db_core::ContentStore;
    let (_, empty) = mappings_that_define_nothing().into_iter().next().unwrap();
    let gs = format!("{name}:main");
    let cid = fluree
        .content_store(&gs)
        .put(
            fluree_db_core::ContentKind::GraphSourceMapping,
            empty.as_bytes(),
        )
        .await
        .expect("store the mapping");
    let config = R2rmlCreateConfig::new_direct(name, table_location(), empty)
        .with_mapping_media_type("text/turtle")
        .to_iceberg_gs_config(&cid.to_string())
        .to_json()
        .expect("serialize config");
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_graph_source(
            name,
            "main",
            fluree_db_nameservice::GraphSourceType::Iceberg,
            &config,
            &[],
        )
        .await
        .expect("publish");
    gs
}

/// A source stored with a mapping that defines nothing (registered before
/// registration refused one) refuses every query with the cause, rather than
/// answering no rows.
#[tokio::test]
async fn a_stored_mapping_that_defines_no_triples_map_refuses_every_query() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let gs = publish_empty_mapping_source(&fluree, "empty-mapping").await;
    let gs = gs.as_str();
    fluree.create_ledger("crm:main").await.expect("ledger");

    let refused = |result: Result<serde_json::Value, ApiError>, route: &str| {
        let err = match result {
            Ok(answer) => panic!("{route}: answered instead of refusing: {answer}"),
            Err(err) => err,
        };
        assert_eq!(
            err.error_type(),
            "err:db/GraphSourceUnmapped",
            "{route}: {err}"
        );
        assert_eq!(err.status_code(), 409, "{route}: {err}");
        assert!(
            err.to_string().contains(&format!(
                "graph source '{gs}' has an R2RML mapping that defines no rr:TriplesMap"
            )),
            "{route}: {err}"
        );
    };
    let from = format!("SELECT ?name FROM <{gs}> WHERE {{ ?s <http://example.org/name> ?name }}");
    refused(
        fluree.query_from().sparql(&from).execute_formatted().await,
        "SPARQL FROM",
    );
    let mut from_jsonld = name_jsonld();
    from_jsonld["from"] = gs.into();
    refused(
        fluree
            .query_from()
            .jsonld(&from_jsonld)
            .execute_formatted()
            .await,
        "JSON-LD from",
    );
    let target = fluree.graph(gs);
    refused(
        target.query().sparql(NAME_SPARQL).execute_formatted().await,
        "SPARQL target",
    );
    let jsonld = name_jsonld();
    refused(
        target.query().jsonld(&jsonld).execute_formatted().await,
        "JSON-LD target",
    );
    let in_ledger =
        format!("SELECT ?name WHERE {{ GRAPH <{gs}> {{ ?s <http://example.org/name> ?name }} }}");
    refused(
        fluree
            .graph("crm:main")
            .query()
            .sparql(&in_ledger)
            .execute_formatted()
            .await,
        "SPARQL GRAPH in a ledger query",
    );
}

/// A mapping that cannot be read names no table, and the catalog source's own
/// `table` is a placeholder: registration loads nothing, so the source is not
/// verified, and the placeholder never reaches the catalog.
#[tokio::test]
async fn a_registration_whose_mapping_cannot_be_read_loads_no_table() {
    allow_fixture_roots();
    let catalog = people_catalog().await;
    let fluree = FlureeBuilder::memory().build_memory();
    let created = fluree
        .create_r2rml_graph_source(R2rmlCreateConfig {
            iceberg: fluree_db_api::IcebergCreateConfig::new(
                "unread-mapping",
                catalog.uri(),
                fluree_db_iceberg::config::MAPPING_DEFINED_TABLE,
            )
            .with_vended_credentials(false)
            .with_s3_region("us-east-1"),
            mapping: fluree_db_api::R2rmlMappingInput::Address("no/such/mapping.ttl".into()),
            mapping_media_type: Some("text/turtle".into()),
        })
        .await
        .expect("an unreadable mapping address registers unvalidated");
    assert!(!created.mapping_validated);
    assert!(!created.connection_tested, "nothing was loaded");
    assert_eq!(
        created.connection_check,
        fluree_db_api::ConnectionCheck::NoTables
    );
    let asked: Vec<String> = catalog
        .received_requests()
        .await
        .expect("request recording")
        .iter()
        .map(|r| r.url.path().to_string())
        .collect();
    assert!(
        !asked.iter().any(|p| p.contains("/tables/")),
        "no table, placeholder or otherwise, was loaded: {asked:?}"
    );
}

/// Registration loads a mapping's tables together, through one catalog client
/// built before the loads: the client cache does not coalesce concurrent
/// misses, so loads that each found it cold would each build one, resolving
/// the catalog's token reference once per table.
#[tokio::test]
async fn a_registration_builds_one_catalog_client_for_all_its_tables() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct CountingResolver(AtomicUsize);

    #[async_trait::async_trait]
    impl fluree_db_iceberg::SecretResolver for CountingResolver {
        async fn resolve_secret(
            &self,
            _secret_ref: &str,
        ) -> Result<String, fluree_db_iceberg::SecretResolveError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            // Hold the build open, as a real resolver's round trip does, so
            // loads issued together would overlap it.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Ok("catalog-token".to_string())
        }
    }

    allow_fixture_roots();
    let catalog = people_catalog().await;
    {
        use wiremock::matchers::{method, path_regex};
        use wiremock::{Mock, ResponseTemplate};
        let metadata_dir = fixtures_dir().join("silver/people/metadata");
        let current = std::fs::read_dir(&metadata_dir)
            .expect("people metadata dir")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
            .max()
            .expect("a metadata file");
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&current).expect("read")).expect("parse");
        Mock::given(method("GET"))
            .and(path_regex(r"^/v1/namespaces/silver/tables/people_[0-9]+$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "metadata-location": format!("file://{}", current.display()),
                "metadata": metadata,
            })))
            .mount(&catalog)
            .await;
    }
    let resolver = std::sync::Arc::new(CountingResolver::default());
    let fluree = FlureeBuilder::memory()
        .with_secret_resolver(resolver.clone())
        .build_memory();
    let mapping = (1..=4)
        .map(|i| {
            PEOPLE_R2RML
                .replace("silver.people", &format!("silver.people_{i}"))
                .replace("mapping#PeopleMapping", &format!("mapping#People{i}"))
        })
        .collect::<Vec<_>>()
        .join("\n");
    let created = fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new(
                "wide",
                catalog.uri(),
                fluree_db_iceberg::config::MAPPING_DEFINED_TABLE,
                mapping,
            )
            .with_mapping_media_type("text/turtle")
            .with_vended_credentials(false)
            .with_s3_region("us-east-1")
            .with_auth_bearer_token_ref("catalog/token"),
        )
        .await
        .expect("register");
    assert_eq!(created.table_count, 4, "{:?}", created.table_names);
    assert!(created.connection_tested, "every table loaded");
    assert_eq!(
        resolver.0.load(Ordering::SeqCst),
        1,
        "one client, so the token reference was resolved once"
    );
}

/// `GRAPH ?g` with `?g` bound from data before the block runs. A binding that
/// names a source no query reads as a graph (no mapping, an empty mapping, a
/// BM25 index) is a graph outside the dataset: it yields no rows, SPARQL's
/// answer, and the query goes on, answering for the other bindings. A
/// constant `GRAPH <source>` naming the same source is refused.
#[tokio::test]
async fn a_graph_variable_bound_from_data_to_an_unreadable_source_yields_no_rows() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let unmapped = publish_unmapped_source(&fluree, "bound-unmapped").await;
    let empty = publish_empty_mapping_source(&fluree, "bound-empty").await;
    fluree.create_ledger("crm:main").await.expect("ledger");
    fluree
        .graph("crm:main")
        .transact()
        .sparql_update(&format!(
            "PREFIX ex: <http://example.org/> \
             INSERT DATA {{ \
               <urn:a> ex:in <{unmapped}> . <urn:b> ex:in <{empty}> . \
               <urn:c> ex:in <bound-search:main> . <urn:d> ex:in <urn:g1> . \
               <urn:doc> a ex:Doc ; ex:title \"hello\" . \
               GRAPH <urn:g1> {{ <urn:x> ex:p \"in g1\" }} }}"
        ))
        .commit()
        .await
        .expect("seed");
    fluree
        .create_full_text_index(fluree_db_api::Bm25CreateConfig::new(
            "bound-search",
            "crm:main",
            serde_json::json!({
                "@context": {"ex": "http://example.org/"},
                "where": [{"@id": "?x", "@type": "ex:Doc", "ex:title": "?t"}],
                "select": {"?x": ["@id", "ex:title"]}
            }),
        ))
        .await
        .expect("index");

    // `?g` is bound before the GRAPH block runs only where the block is
    // correlated with the rows binding it: after a subquery, or inside
    // OPTIONAL. (A plain join evaluates `GRAPH ?g` over the ledger's own named
    // graphs and never probes a source.)
    let ledger = fluree.graph("crm:main");
    let one_row = |rows: &serde_json::Value, route: &str| {
        let bindings = rows["results"]["bindings"]
            .as_array()
            .unwrap_or_else(|| panic!("{route}: {rows}"));
        assert_eq!(
            bindings.len(),
            1,
            "{route}: only urn:g1 is a graph here: {rows}"
        );
        assert_eq!(bindings[0]["g"]["value"], "urn:g1", "{route}: {rows}");
        assert_eq!(bindings[0]["o"]["value"], "in g1", "{route}: {rows}");
    };
    let subquery = "PREFIX ex: <http://example.org/> SELECT ?g ?o WHERE { \
                    { SELECT ?g WHERE { ?s ex:in ?g } } GRAPH ?g { ?x ex:p ?o } }";
    let rows = ledger
        .query()
        .sparql(subquery)
        .execute_formatted()
        .await
        .expect("SPARQL subquery: a data-bound GRAPH variable does not fail the query");
    one_row(&rows, "SPARQL subquery");
    let optional = "PREFIX ex: <http://example.org/> SELECT ?g ?o WHERE { \
                    ?s ex:in ?g OPTIONAL { GRAPH ?g { ?x ex:p ?o } } }";
    let rows = ledger
        .query()
        .sparql(optional)
        .execute_formatted()
        .await
        .expect("SPARQL OPTIONAL: a data-bound GRAPH variable does not fail the query");
    let bindings = rows["results"]["bindings"].as_array().expect("bindings");
    assert_eq!(bindings.len(), 4, "every ?g row is kept: {rows}");
    let matched: Vec<_> = bindings.iter().filter(|b| b.get("o").is_some()).collect();
    assert_eq!(matched.len(), 1, "only urn:g1 matches inside GRAPH: {rows}");
    assert_eq!(matched[0]["g"]["value"], "urn:g1", "{rows}");

    let jsonld_subquery = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?g", "?o"],
        "where": [
            ["query", {"select": ["?g"], "where": {"@id": "?s", "http://example.org/in": "?g"}}],
            ["graph", "?g", {"@id": "?x", "ex:p": "?o"}]
        ]
    });
    let rows = ledger
        .query()
        .jsonld(&jsonld_subquery)
        .execute_formatted()
        .await
        .expect("JSON-LD subquery: a data-bound graph variable does not fail the query");
    let rows = rows.as_array().expect("rows");
    assert_eq!(rows.len(), 1, "only urn:g1 is a graph here: {rows:?}");
    assert!(rows[0].to_string().contains("in g1"), "{rows:?}");
    let jsonld_optional = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?g", "?o"],
        "where": [
            {"@id": "?s", "ex:in": "?g"},
            ["optional", ["graph", "?g", {"@id": "?x", "ex:p": "?o"}]]
        ]
    });
    let rows = ledger
        .query()
        .jsonld(&jsonld_optional)
        .execute_formatted()
        .await
        .expect("JSON-LD optional: a data-bound graph variable does not fail the query");
    let rows = rows.as_array().expect("rows");
    assert_eq!(rows.len(), 4, "every ?g row is kept: {rows:?}");
    assert_eq!(
        rows.iter()
            .filter(|r| r.to_string().contains("in g1"))
            .count(),
        1,
        "only urn:g1 matches inside graph: {rows:?}"
    );

    // The same sources named as constants are refused.
    for gs in [unmapped.as_str(), empty.as_str(), "bound-search:main"] {
        let constant = format!(
            "PREFIX ex: <http://example.org/> SELECT ?o WHERE {{ GRAPH <{gs}> {{ ?x ex:p ?o }} }}"
        );
        let err = ledger
            .query()
            .sparql(&constant)
            .execute_formatted()
            .await
            .expect_err(gs);
        assert!(
            matches!(
                err.error_type(),
                "err:db/GraphSourceUnmapped" | "err:db/GraphSourceNotAGraph"
            ),
            "{gs}: {err}"
        );
    }
}
