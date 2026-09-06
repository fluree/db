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
/// Assumes `FLUREE_MATERIALIZE_MAX_ROWS_PER_FULL_PASS` and
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
    let budget = provider.full_pass_row_budget();
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
                    indexer.wait_for_idle(target).await;
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
        indexer.wait_for_idle(target).await;
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
