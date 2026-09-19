//! Reader correctness against committed fixtures.
//!
//! `dim_store` / `fact_order` are written by delta-rs (`scripts/delta-spike/
//! fixture.py`); the rest by Delta Spark (`spark_fixture.py`), so deletion
//! vectors, column mapping and in-commit timestamps come from the reference
//! writer. Expected rows are restated here from the generators' inputs, never
//! read back through the reader under test.

use std::path::{Path, PathBuf};

use fluree_db_delta::{DeltaError, DeltaIoConfig, DeltaSnapshot, DeltaTable, VersionSelector};
use fluree_db_tabular::{Column, ColumnBatch};
use futures::StreamExt;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Every test must install the same allowlist: the guard reads it once.
fn allow_roots() {
    let roots = [
        fixtures().display().to_string(),
        std::env::temp_dir().display().to_string(),
    ];
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", roots.join(":"));
}

fn open(table: &str) -> DeltaTable {
    allow_roots();
    let location = fixtures().join(table);
    DeltaTable::open(table, location.to_str().unwrap(), &DeltaIoConfig::default())
        .expect("open table")
}

#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Null,
    Int(i64),
    Str(String),
}

fn cell(column: &Column, row: usize) -> Cell {
    if column.is_null(row) {
        return Cell::Null;
    }
    match column {
        Column::Int64(_) => Cell::Int(column.get_i64(row).unwrap()),
        Column::Int32(_) => Cell::Int(i64::from(column.get_i32(row).unwrap())),
        Column::String(_) => Cell::Str(column.get_string(row).unwrap().to_string()),
        other => panic!("unexpected column type {:?}", other.field_type()),
    }
}

/// Rows of `columns`, sorted by the first column.
async fn rows(snapshot: &DeltaSnapshot, columns: &[&str]) -> Vec<Vec<Cell>> {
    let projection: Vec<String> = columns.iter().map(ToString::to_string).collect();
    let batches: Vec<ColumnBatch> = snapshot
        .scan(&projection)
        .expect("plan scan")
        .map(|b| b.expect("batch"))
        .collect()
        .await;
    let mut out = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows {
            out.push(
                columns
                    .iter()
                    .map(|c| cell(batch.column_by_name(c).expect("projected column"), row))
                    .collect::<Vec<_>>(),
            );
        }
    }
    out.sort_by_key(|r| match &r[0] {
        Cell::Int(i) => *i,
        other => panic!("sort key {other:?}"),
    });
    out
}

fn int(v: i64) -> Cell {
    Cell::Int(v)
}

fn s(v: &str) -> Cell {
    Cell::Str(v.to_string())
}

#[tokio::test]
async fn every_version_of_a_rewritten_table_reads_its_own_rows() {
    let table = open("fact_order");
    let cols = ["order_id", "store_id", "amount", "region"];
    let v0 = vec![
        vec![int(1), int(1), int(100), s("east")],
        vec![int(2), int(2), int(200), s("west")],
        vec![int(3), int(1), Cell::Null, s("east")],
        vec![int(4), int(3), int(400), Cell::Null],
    ];
    let mut v1 = v0.clone();
    v1.push(vec![int(5), int(2), int(500), s("west")]);
    v1.push(vec![int(6), int(1), int(600), s("east")]);
    let mut v2 = v1.clone();
    v2[1][2] = int(250);
    let v3: Vec<_> = v2[1..].to_vec();

    for (version, expected) in [(0, &v0), (1, &v1), (2, &v2), (3, &v3)] {
        let snapshot = table
            .snapshot(VersionSelector::Version(version))
            .await
            .unwrap();
        assert_eq!(snapshot.version(), version);
        assert_eq!(snapshot.column_names(), cols);
        assert_eq!(&rows(&snapshot, &cols).await, expected, "version {version}");
    }

    // v4 adds a nullable column; the latest read is v4 and sees it.
    let latest = table.snapshot(VersionSelector::Latest).await.unwrap();
    assert_eq!(latest.version(), 4);
    assert_eq!(
        latest.column_names(),
        ["order_id", "store_id", "amount", "region", "note"]
    );
    let noted = rows(&latest, &["order_id", "note"]).await;
    assert_eq!(noted.len(), 6);
    assert_eq!(noted[5], vec![int(7), s("new column")]);
    assert!(noted[..5].iter().all(|r| r[1] == Cell::Null));
}

#[tokio::test]
async fn projection_is_by_name_and_keeps_full_schema_field_ids() {
    let table = open("fact_order");
    let snapshot = table.snapshot(VersionSelector::Version(0)).await.unwrap();
    // Reordered, and matched case-insensitively when no exact name exists.
    let schema = snapshot
        .batch_schema(&["REGION".to_string(), "order_id".to_string()])
        .unwrap();
    let fields: Vec<_> = schema
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f.field_id))
        .collect();
    assert_eq!(fields, [("region", 4), ("order_id", 1)]);

    let err = snapshot.batch_schema(&["note".to_string()]).unwrap_err();
    assert!(
        matches!(&err, DeltaError::ColumnNotFound { column, version: 0, .. } if column == "note"),
        "{err}"
    );
}

#[tokio::test]
async fn unknown_versions_are_errors_not_the_latest_table() {
    let table = open("fact_order");
    let err = table
        .snapshot(VersionSelector::Version(5))
        .await
        .err()
        .expect("version 5 does not exist");
    assert!(
        matches!(err, DeltaError::VersionNotFound { version: 5, .. }),
        "{err}"
    );
}

#[tokio::test]
async fn deletion_vectors_remove_rows_without_rewriting_files() {
    let table = open("deletion_vectors");
    let removed = [0, 1, 1023, 1024, 2047, 2048, 3071, 4094, 4095];
    let expect = |skip: &[i64]| -> Vec<Vec<Cell>> {
        (0..4096)
            .filter(|i| !skip.contains(i))
            .map(|i| vec![int(i), int(i * 10), s(&(i % 2).to_string())])
            .collect()
    };
    let cols = ["id", "amount", "region"];
    let v0 = table.snapshot(VersionSelector::Version(0)).await.unwrap();
    assert_eq!(rows(&v0, &cols).await, expect(&[]));
    let v1 = table.snapshot(VersionSelector::Latest).await.unwrap();
    assert_eq!(v1.version(), 1);
    assert_eq!(rows(&v1, &cols).await, expect(&removed));
}

#[tokio::test]
async fn column_mapping_follows_rename_drop_and_re_add() {
    let table = open("column_mapping");
    let at = |v| table.snapshot(VersionSelector::Version(v));

    let v0 = at(0).await.unwrap();
    assert_eq!(
        rows(&v0, &["id", "amount"]).await,
        [vec![int(1), int(100)], vec![int(2), int(30000)]]
    );
    let v1 = at(1).await.unwrap();
    assert_eq!(v1.column_names(), ["id", "renamed_amount"]);
    assert_eq!(
        rows(&v1, &["id", "renamed_amount"]).await,
        [vec![int(1), int(100)], vec![int(2), int(30000)]]
    );
    // The mapping id survives the rename.
    let id_of = |snapshot: &DeltaSnapshot, name: &str| {
        snapshot.batch_schema(&[name.to_string()]).unwrap().fields[0].field_id
    };
    assert_eq!(id_of(&v0, "amount"), id_of(&v1, "renamed_amount"));

    let v2 = at(2).await.unwrap();
    assert_eq!(v2.column_names(), ["id"]);
    // A re-added `amount` is a new column: it must not resurrect the dropped
    // column's values from the unchanged Parquet file.
    let v3 = at(3).await.unwrap();
    assert_eq!(
        rows(&v3, &["id", "amount"]).await,
        [vec![int(1), Cell::Null], vec![int(2), Cell::Null]]
    );
    assert_ne!(id_of(&v0, "amount"), id_of(&v3, "amount"));
}

#[tokio::test]
async fn a_version_whose_data_was_removed_fails_at_scan() {
    let table = open("history_loss");
    let latest = table.snapshot(VersionSelector::Latest).await.unwrap();
    assert_eq!(
        rows(&latest, &["id", "amount"]).await,
        [vec![int(3), int(50000)]]
    );

    // The log still replays to v0; its only data file is gone.
    let v0 = table.snapshot(VersionSelector::Version(0)).await.unwrap();
    let results: Vec<_> = v0.scan(&[]).unwrap().collect().await;
    let err = results
        .into_iter()
        .find_map(Result::err)
        .expect("scan of vacuumed version must fail");
    assert!(err.is_missing_file(), "{err:?}");
}

#[tokio::test]
async fn reads_outside_the_local_allowlist_are_refused() {
    allow_roots();
    let err = DeltaTable::open("passwd", "/etc", &DeltaIoConfig::default()).unwrap_err();
    assert!(matches!(err, DeltaError::Config(_)), "{err}");
}

/// Commit times Delta Spark recorded in the `in_commit_time` fixture's log.
const ICT: [i64; 3] = [1_789_829_202_524, 1_789_829_202_991, 1_789_829_203_546];

#[tokio::test]
async fn an_instant_selects_the_latest_version_committed_at_or_before_it() {
    let table = open("in_commit_time");
    for (instant, version) in [
        (ICT[0], 0),
        (ICT[1] - 1, 0),
        (ICT[1], 1),
        (ICT[2] - 1, 1),
        (ICT[2], 2),
        // After the latest commit: the latest state, not an error.
        (ICT[2] + 86_400_000, 2),
    ] {
        let snapshot = table
            .snapshot(VersionSelector::AsOfTimestampMs(instant))
            .await
            .unwrap_or_else(|e| panic!("instant {instant}: {e}"));
        assert_eq!(snapshot.version(), version, "instant {instant}");
        assert_eq!(
            snapshot.timestamp_ms().await.unwrap(),
            ICT[version as usize]
        );
    }
    let v1 = table
        .snapshot(VersionSelector::AsOfTimestampMs(ICT[1]))
        .await
        .unwrap();
    assert_eq!(
        rows(&v1, &["id", "amount"]).await,
        [vec![int(1), int(100)], vec![int(2), int(30000)]]
    );

    let err = table
        .snapshot(VersionSelector::AsOfTimestampMs(ICT[0] - 1))
        .await
        .err()
        .expect("nothing was committed before the first version");
    assert!(
        matches!(
            &err,
            DeltaError::NoVersionAtTime { requested_ms, oldest_ms: Some(oldest), .. }
                if *requested_ms == ICT[0] - 1 && *oldest == ICT[0]
        ),
        "{err:?}"
    );
}

/// A table without in-commit timestamps resolves instants against its log
/// files' modification times.
#[tokio::test]
async fn without_in_commit_timestamps_an_instant_resolves_by_log_file_mtime() {
    allow_roots();
    let staged = tempfile::tempdir().unwrap();
    let dir = staged.path().join("fact_order");
    copy_dir_all(&fixtures().join("fact_order"), &dir);
    let base_ms: i64 = 1_700_000_000_000;
    for version in 0..5_i64 {
        let log = dir.join(format!("_delta_log/{version:020}.json"));
        let at = std::time::UNIX_EPOCH
            + std::time::Duration::from_millis((base_ms + version * 60_000) as u64);
        std::fs::File::options()
            .write(true)
            .open(&log)
            .unwrap()
            .set_modified(at)
            .unwrap();
    }
    let table = DeltaTable::open(
        "fact_order",
        dir.to_str().unwrap(),
        &DeltaIoConfig::default(),
    )
    .unwrap();
    for (instant, version) in [
        (base_ms, 0),
        (base_ms + 60_000 - 1, 0),
        (base_ms + 3 * 60_000, 3),
        (base_ms + 10 * 60_000, 4),
    ] {
        let snapshot = table
            .snapshot(VersionSelector::AsOfTimestampMs(instant))
            .await
            .unwrap_or_else(|e| panic!("instant {instant}: {e}"));
        assert_eq!(snapshot.version(), version, "instant {instant}");
    }
    let err = table
        .snapshot(VersionSelector::AsOfTimestampMs(base_ms - 1))
        .await
        .err()
        .expect("before the first commit");
    assert!(
        matches!(&err, DeltaError::NoVersionAtTime { oldest_ms: Some(o), .. } if *o == base_ms),
        "{err:?}"
    );
}

fn copy_dir_all(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_all(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

/// A Delta log may name data files by absolute URI. One outside the allowlist
/// must be refused by the store, not opened and handed to the Parquet reader.
#[tokio::test]
async fn an_absolute_data_path_outside_the_allowlist_is_refused_at_read() {
    allow_roots();
    let staged = tempfile::tempdir().unwrap();
    let dir = staged.path().join("dim_store");
    copy_dir_all(&fixtures().join("dim_store"), &dir);
    let outside = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let log = dir.join("_delta_log/00000000000000000000.json");
    let rewritten: Vec<String> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|line| {
            let mut action: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(add) = action.get_mut("add") {
                add["path"] = format!("file://{}", outside.display()).into();
            }
            action.to_string()
        })
        .collect();
    std::fs::write(&log, rewritten.join("\n")).unwrap();

    let table = DeltaTable::open(
        "dim_store",
        dir.to_str().unwrap(),
        &DeltaIoConfig::default(),
    )
    .unwrap();
    let snapshot = table.snapshot(VersionSelector::Latest).await.unwrap();
    let results: Vec<_> = snapshot.scan(&[]).unwrap().collect().await;
    let err = results
        .into_iter()
        .find_map(Result::err)
        .expect("read outside the allowlist must fail");
    let chain = format!("{err:?}");
    assert!(chain.contains("PermissionDenied"), "{chain}");
}
