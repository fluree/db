//! Reader correctness against committed fixtures.
//!
//! `dim_store` / `fact_order` are written by delta-rs (`scripts/delta-spike/
//! fixture.py`); the rest by Delta Spark (`spark_fixture.py`), so deletion
//! vectors, column mapping and in-commit timestamps come from the reference
//! writer. Expected rows are restated here from the generators' inputs, never
//! read back through the reader under test.

use std::path::{Path, PathBuf};

use fluree_db_delta::{
    ColumnFilter, DeltaError, DeltaIoConfig, DeltaSnapshot, DeltaTable, FilterOp, FilterValue,
    VersionSelector,
};
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
        .scan(&projection, &[])
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
    let results: Vec<_> = v0.scan(&[], &[]).unwrap().collect().await;
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
    let results: Vec<_> = snapshot.scan(&[], &[]).unwrap().collect().await;
    let err = results
        .into_iter()
        .find_map(Result::err)
        .expect("read outside the allowlist must fail");
    let chain = format!("{err:?}");
    assert!(chain.contains("PermissionDenied"), "{chain}");
}

/// Building an Azure store is offline (tokens are fetched on first read), so
/// this pins the configuration contract without an Azure account.
#[tokio::test]
async fn a_checkpointed_log_reads_the_same_with_or_without_its_early_commits() {
    let expected = |through: i64| -> Vec<Vec<Cell>> {
        (0..=through).map(|i| vec![int(i), int(i * 1000)]).collect()
    };
    for name in ["checkpointed", "log_cleaned"] {
        let table = open(name);
        let latest = table.snapshot(VersionSelector::Latest).await.unwrap();
        assert_eq!(latest.version(), 12, "{name}");
        assert_eq!(
            rows(&latest, &["id", "amount"]).await,
            expected(12),
            "{name}"
        );
        // The checkpoint itself, and a commit replayed on top of it.
        for version in [10, 11] {
            let pinned = table
                .snapshot(VersionSelector::Version(version))
                .await
                .unwrap();
            assert_eq!(
                rows(&pinned, &["id", "amount"]).await,
                expected(version as i64),
                "{name} v{version}"
            );
        }
    }

    // Before the checkpoint: replayable only while the commits survive.
    let v3 = open("checkpointed")
        .snapshot(VersionSelector::Version(3))
        .await
        .unwrap();
    assert_eq!(rows(&v3, &["id", "amount"]).await, expected(3));
    let gone = open("log_cleaned")
        .snapshot(VersionSelector::Version(3))
        .await;
    assert!(
        matches!(gone, Err(DeltaError::VersionNotFound { version: 3, .. })),
        "{:?}",
        gone.err()
    );
}

#[tokio::test]
async fn an_instant_before_the_oldest_retained_commit_is_refused_on_a_cleaned_log() {
    // The table has no in-commit timestamps and a checkout does not preserve
    // mtimes, so stage a copy whose log files carry known ones.
    allow_roots();
    let staged = tempfile::tempdir().unwrap();
    let dir = staged.path().join("log_cleaned");
    copy_dir_all(&fixtures().join("log_cleaned"), &dir);
    let base_ms: i64 = 1_700_000_000_000;
    let stamp = |name: String, ms: i64| {
        let at = std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms as u64);
        std::fs::File::options()
            .write(true)
            .open(dir.join("_delta_log").join(name))
            .unwrap()
            .set_modified(at)
            .unwrap();
    };
    for version in 10..13_i64 {
        stamp(
            format!("{version:020}.json"),
            base_ms + (version - 10) * 60_000,
        );
    }
    stamp(format!("{:020}.checkpoint.parquet", 10), base_ms);
    let table = DeltaTable::open(
        "log_cleaned",
        dir.to_str().unwrap(),
        &DeltaIoConfig::default(),
    )
    .unwrap();

    let at = |ms: i64| table.snapshot(VersionSelector::AsOfTimestampMs(ms));
    assert_eq!(at(base_ms).await.unwrap().version(), 10);
    assert_eq!(at(base_ms + 60_000).await.unwrap().version(), 11);
    assert_eq!(at(base_ms + 59_999).await.unwrap().version(), 10);
    // Versions 0–9 were committed before this, but nothing of them remains.
    match at(base_ms - 1).await {
        Err(DeltaError::NoVersionAtTime { oldest_ms, .. }) => {
            assert_eq!(oldest_ms, Some(base_ms));
        }
        other => panic!(
            "expected NoVersionAtTime, got {:?}",
            other.map(|s| s.version())
        ),
    }
}

#[tokio::test]
async fn every_scalar_type_written_by_spark_arrives_typed() {
    let snapshot = open("types")
        .snapshot(VersionSelector::Latest)
        .await
        .unwrap();
    let batches: Vec<ColumnBatch> = snapshot
        .scan(&[], &[])
        .unwrap()
        .map(|b| b.expect("batch"))
        .collect()
        .await;
    assert_eq!(batches.iter().map(|b| b.num_rows).sum::<usize>(), 3);
    // Spark spread the rows over several files; pick each out by id.
    let at = |id: i64| {
        batches
            .iter()
            .find_map(|b| {
                let ids = b.column_by_name("id").expect("id");
                (0..b.num_rows)
                    .find(|&r| ids.get_i64(r) == Some(id))
                    .map(|r| (b, r))
            })
            .expect("id present")
    };
    let ((b1, one), (b2, two), (b3, nulls)) = (at(1), at(2), at(3));
    let col1 = |name: &str| b1.column_by_name(name).expect("column");
    let col2 = |name: &str| b2.column_by_name(name).expect("column");

    assert_eq!(col1("flag").get_bool(one), Some(true));
    assert_eq!(col2("tiny").get_i32(two), Some(-7));
    assert_eq!(col1("small").get_i32(one), Some(300));
    assert_eq!(col2("num").get_i32(two), Some(-70_000));
    assert_eq!(col2("ratio").get_f64(two), Some(-0.25));
    assert_eq!(col1("approx").get_f32(one), Some(2.5));
    assert_eq!(col2("label").get_string(two), Some(""));
    assert_eq!(col1("blob").get_bytes(one), Some(&[0x00, 0xFF, 0x10][..]));
    assert_eq!(col2("blob").get_bytes(two), Some(&[][..]));
    // 2024-02-29 and 1969-12-31 as days from the epoch.
    assert_eq!(col1("day").get_date(one), Some(19_782));
    assert_eq!(col2("day").get_date(two), Some(-1));
    let decimal = |column: &Column, row: usize| match column {
        Column::Decimal {
            values,
            precision,
            scale,
        } => (values[row], *precision, *scale),
        other => panic!("decimal column, got {:?}", other.field_type()),
    };
    assert_eq!(decimal(col1("price"), one), (Some(1999), 10, 2));
    assert_eq!(decimal(col2("price"), two), (Some(-1), 10, 2));
    assert_eq!(
        decimal(col1("big"), one),
        (Some(123_456_789_012_345_678_900_123_456_789), 38, 10)
    );
    assert_eq!(decimal(col2("big"), two), (Some(-10_000_000_001), 38, 10));
    // A zoned instant and a wall-clock one keep their frames apart.
    let micros = |column: &Column, zoned: bool, row: usize| match (column, zoned) {
        (Column::TimestampTz(v), true) | (Column::Timestamp(v), false) => v[row],
        (other, _) => panic!("zoned={zoned}, got {:?}", other.field_type()),
    };
    assert_eq!(micros(col1("at"), true, one), Some(1_709_210_096_789_000));
    assert_eq!(micros(col2("at"), true, two), Some(-500_000));
    assert_eq!(
        micros(col1("wall"), false, one),
        Some(1_709_168_523_000_000)
    );
    assert_eq!(micros(col2("wall"), false, two), Some(-1_000_000));
    for name in snapshot.column_names().iter().filter(|n| *n != "id") {
        assert!(
            b3.column_by_name(name).expect("column").is_null(nulls),
            "{name} of the all-null row"
        );
    }
}

fn filter(column: &str, op: FilterOp, value: FilterValue) -> ColumnFilter {
    ColumnFilter {
        column: column.to_string(),
        op,
        value,
    }
}

/// `partitioned` is six files of four rows: regions east/north/west twice over,
/// ids 0.., 100.., 200.. then 300.., 400.., 500...
#[tokio::test]
async fn filters_skip_files_by_partition_value_and_by_statistics() {
    let snapshot = open("partitioned")
        .snapshot(VersionSelector::Latest)
        .await
        .unwrap();
    let files = |filters: Vec<ColumnFilter>| {
        let snapshot = snapshot.clone();
        async move { snapshot.file_count(&filters).await.unwrap() }
    };
    let str_ = |v: &str| FilterValue::Str(v.to_string());

    assert_eq!(files(vec![]).await, 6);
    assert_eq!(
        files(vec![filter("region", FilterOp::Eq, str_("north"))]).await,
        2
    );
    assert_eq!(
        files(vec![filter("id", FilterOp::GtEq, FilterValue::Int(500))]).await,
        1
    );
    assert_eq!(
        files(vec![filter("id", FilterOp::Lt, FilterValue::Int(0))]).await,
        0
    );
    // 203 is inside a file's range; 250 is between files.
    assert_eq!(
        files(vec![filter("id", FilterOp::Eq, FilterValue::Int(203))]).await,
        1
    );
    assert_eq!(
        files(vec![filter("id", FilterOp::Eq, FilterValue::Int(250))]).await,
        0
    );
    let members = FilterValue::Set(vec![FilterValue::Int(1), FilterValue::Int(402)]);
    assert_eq!(files(vec![filter("id", FilterOp::In, members)]).await, 2);
    assert_eq!(
        files(vec![
            filter("region", FilterOp::Eq, str_("west")),
            filter("amount", FilterOp::Gt, FilterValue::Int(3000)),
        ])
        .await,
        1
    );
    // Not expressible against the column: nothing is skipped.
    assert_eq!(
        files(vec![filter("id", FilterOp::Eq, str_("203"))]).await,
        6
    );
}

/// Ids a scan of `columns` under `filters` returns, sorted.
async fn ids(snapshot: &DeltaSnapshot, columns: &[&str], filters: Vec<ColumnFilter>) -> Vec<i64> {
    let projection: Vec<String> = columns.iter().map(ToString::to_string).collect();
    let mut out: Vec<i64> = snapshot
        .scan(&projection, &filters)
        .unwrap()
        .map(|b| b.expect("batch"))
        .collect::<Vec<ColumnBatch>>()
        .await
        .iter()
        .flat_map(|b| {
            let id = b.column_by_name("id").expect("id");
            (0..b.num_rows)
                .map(|r| id.get_i64(r).unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    out.sort_unstable();
    out
}

#[tokio::test]
async fn rows_a_filter_rejects_do_not_leave_the_reader() {
    let str_ = |v: &str| FilterValue::Str(v.to_string());
    let sales = open("partitioned")
        .snapshot(VersionSelector::Latest)
        .await
        .unwrap();
    let all = ["id", "amount", "region"];

    assert_eq!(
        ids(
            &sales,
            &all,
            vec![filter("id", FilterOp::Eq, FilterValue::Int(203))]
        )
        .await,
        [203]
    );
    assert_eq!(
        ids(
            &sales,
            &all,
            vec![filter("id", FilterOp::NotEq, FilterValue::Int(203))]
        )
        .await
        .len(),
        23
    );
    assert_eq!(
        ids(
            &sales,
            &all,
            vec![filter("amount", FilterOp::GtEq, FilterValue::Int(5010))]
        )
        .await,
        [501, 502, 503]
    );
    assert_eq!(
        ids(
            &sales,
            &all,
            vec![filter("amount", FilterOp::Lt, FilterValue::Int(20))]
        )
        .await,
        [0, 1]
    );
    let members = FilterValue::Set(vec![
        FilterValue::Int(1),
        FilterValue::Int(250),
        FilterValue::Int(402),
    ]);
    assert_eq!(
        ids(&sales, &all, vec![filter("id", FilterOp::In, members)]).await,
        [1, 402]
    );
    // A partition column is filtered like any other, and terms are conjoined.
    assert_eq!(
        ids(
            &sales,
            &all,
            vec![
                filter("region", FilterOp::Eq, str_("north")),
                filter("id", FilterOp::LtEq, FilterValue::Int(101)),
            ]
        )
        .await,
        [100, 101]
    );
    assert_eq!(
        ids(
            &sales,
            &all,
            vec![filter("region", FilterOp::Gt, str_("north"))]
        )
        .await
        .len(),
        8
    );
    // The filtered column is not projected: files are skipped, rows are not.
    assert_eq!(
        ids(
            &sales,
            &["id"],
            vec![filter("amount", FilterOp::Eq, FilterValue::Int(2030))]
        )
        .await,
        [200, 201, 202, 203]
    );
    // A value of another type than the column filters nothing.
    assert_eq!(
        ids(&sales, &all, vec![filter("id", FilterOp::Eq, str_("203"))])
            .await
            .len(),
        24
    );

    // `types`: ids 1 and 2 carry values, id 3 is null in every other column.
    let types = open("types")
        .snapshot(VersionSelector::Latest)
        .await
        .unwrap();
    let every: Vec<String> = types.column_names();
    let every: Vec<&str> = every.iter().map(String::as_str).collect();
    let pick = |f: ColumnFilter| ids(&types, &every, vec![f]);
    assert_eq!(
        pick(filter("flag", FilterOp::Eq, FilterValue::Bool(false))).await,
        [2]
    );
    // A null is no match for `!=` either: the row has no such triple.
    assert_eq!(
        pick(filter("flag", FilterOp::NotEq, FilterValue::Bool(false))).await,
        [1]
    );
    assert_eq!(
        pick(filter("tiny", FilterOp::Lt, FilterValue::Int(0))).await,
        [2]
    );
    assert_eq!(
        pick(filter("small", FilterOp::Eq, FilterValue::Int(300))).await,
        [1]
    );
    assert_eq!(
        pick(filter("num", FilterOp::GtEq, FilterValue::Int(-70_000))).await,
        [1, 2]
    );
    assert_eq!(
        pick(filter("ratio", FilterOp::Gt, FilterValue::Double(1.0))).await,
        [1]
    );
    assert_eq!(pick(filter("label", FilterOp::Eq, str_(""))).await, [2]);
    assert_eq!(
        pick(filter("day", FilterOp::Lt, FilterValue::Date(0))).await,
        [2]
    );
    let zoned = |micros| FilterValue::Timestamp { micros, tz: true };
    let wall = |micros| FilterValue::Timestamp { micros, tz: false };
    assert_eq!(pick(filter("at", FilterOp::Gt, zoned(0))).await, [1]);
    assert_eq!(
        pick(filter("wall", FilterOp::LtEq, wall(-1_000_000))).await,
        [2]
    );
    assert_eq!(
        pick(filter("id", FilterOp::Eq, FilterValue::Raw("2".into()))).await,
        [2]
    );

    // Left to the caller: a frame mismatch, a narrowed float, a zero double
    // (Arrow orders -0.0 below 0.0), a decimal, a key not in canonical form.
    for unusable in [
        filter("at", FilterOp::Gt, wall(0)),
        filter("wall", FilterOp::Gt, zoned(0)),
        filter("approx", FilterOp::Gt, FilterValue::Double(0.5)),
        filter("ratio", FilterOp::Gt, FilterValue::Double(0.0)),
        filter("price", FilterOp::Gt, FilterValue::Int(1)),
        filter("id", FilterOp::Eq, FilterValue::Raw("02".into())),
    ] {
        let kept = pick(unusable.clone()).await;
        assert_eq!(kept, [1, 2, 3], "{unusable:?}");
    }
}

/// Rewrite every `add` action's statistics in a copied table's log.
fn rewrite_stats(table: &Path, edit: impl Fn(&mut serde_json::Value)) {
    for entry in std::fs::read_dir(table.join("_delta_log")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let rewritten: Vec<String> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| {
                let mut action: serde_json::Value = serde_json::from_str(line).unwrap();
                if let Some(add) = action.get_mut("add") {
                    edit(add);
                }
                action.to_string()
            })
            .collect();
        std::fs::write(&path, rewritten.join("\n") + "\n").unwrap();
    }
}

#[tokio::test]
async fn the_log_answers_a_row_count_only_when_it_provably_matches_a_scan() {
    let latest = |name: &str| {
        let table = open(name);
        async move { table.snapshot(VersionSelector::Latest).await.unwrap() }
    };
    let cols = |names: &[&str]| names.iter().map(ToString::to_string).collect::<Vec<_>>();

    let partitioned = latest("partitioned").await;
    assert_eq!(partitioned.exact_row_count(&[]).await.unwrap(), Some(24));
    assert_eq!(
        partitioned
            .exact_row_count(&cols(&["id", "region"]))
            .await
            .unwrap(),
        Some(24)
    );
    assert_eq!(
        partitioned.exact_row_count(&cols(&["nope"])).await.unwrap(),
        None
    );

    // A null in a counted column: the row would not be counted by a scan.
    let types = latest("types").await;
    assert_eq!(
        types.exact_row_count(&cols(&["id"])).await.unwrap(),
        Some(3)
    );
    assert_eq!(
        types
            .exact_row_count(&cols(&["id", "label"]))
            .await
            .unwrap(),
        None
    );

    // Deletion vectors make the recorded counts an over-count.
    let vectors = latest("deletion_vectors").await;
    assert_eq!(vectors.exact_row_count(&[]).await.unwrap(), None);
    assert_eq!(rows(&vectors, &["id"]).await.len(), 4096 - 9);

    // Statistics are optional. Without a null count for a counted column, or
    // without any statistics, the log proves nothing.
    allow_roots();
    let open_copy = |dir: &Path| {
        DeltaTable::open("copy", dir.to_str().unwrap(), &DeltaIoConfig::default()).unwrap()
    };
    let no_null_count = tempfile::tempdir().unwrap();
    copy_dir_all(&fixtures().join("partitioned"), no_null_count.path());
    rewrite_stats(no_null_count.path(), |add| {
        let mut stats: serde_json::Value =
            serde_json::from_str(add["stats"].as_str().unwrap()).unwrap();
        stats["nullCount"].as_object_mut().unwrap().remove("amount");
        add["stats"] = stats.to_string().into();
    });
    let snapshot = open_copy(no_null_count.path())
        .snapshot(VersionSelector::Latest)
        .await
        .unwrap();
    assert_eq!(
        snapshot.exact_row_count(&cols(&["id"])).await.unwrap(),
        Some(24)
    );
    assert_eq!(
        snapshot.exact_row_count(&cols(&["amount"])).await.unwrap(),
        None
    );

    let no_stats = tempfile::tempdir().unwrap();
    copy_dir_all(&fixtures().join("partitioned"), no_stats.path());
    rewrite_stats(no_stats.path(), |add| {
        add.as_object_mut().unwrap().remove("stats");
    });
    let snapshot = open_copy(no_stats.path())
        .snapshot(VersionSelector::Latest)
        .await
        .unwrap();
    assert_eq!(snapshot.exact_row_count(&[]).await.unwrap(), None);
    assert_eq!(rows(&snapshot, &["id"]).await.len(), 24);
}

#[tokio::test]
async fn azure_locations_open_with_a_hydrated_service_principal_only() {
    use fluree_db_delta::config::AzureAuth;
    use fluree_db_iceberg::ConfigValue;

    let location = "abfss://lake@acct.dfs.core.windows.net/Tables/orders";
    let with_secret = |client_secret: ConfigValue| DeltaIoConfig {
        azure: Some(AzureAuth::ClientSecret {
            tenant_id: "tenant".to_string(),
            client_id: "app".to_string(),
            client_secret,
        }),
        ..Default::default()
    };

    DeltaTable::open(
        "orders",
        location,
        &with_secret(ConfigValue::literal("s3cret")),
    )
    .expect("service principal store builds offline");

    // A secret reference must be hydrated first; opening with it unresolved
    // fails closed rather than falling back to ambient credentials.
    let by_ref = with_secret(ConfigValue::SecretRef {
        secret_ref: "vault://delta/azure".to_string(),
    });
    let err = DeltaTable::open("orders", location, &by_ref).unwrap_err();
    assert!(matches!(err, DeltaError::Config(_)), "{err}");
    let err = by_ref.hydrate(None).await.unwrap_err();
    assert!(matches!(err, DeltaError::Config(_)), "{err}");

    let err = DeltaTable::open(
        "orders",
        "abfss://lake@evil.example.com/Tables/orders",
        &DeltaIoConfig::default(),
    )
    .unwrap_err();
    assert!(matches!(err, DeltaError::Config(_)), "{err}");
}

/// Network check, no account needed: a bogus service principal must get as far
/// as Microsoft Entra *rejecting the credentials*. That proves the store's HTTP
/// stack (its own reqwest/rustls, separate from the workspace's) completes a
/// TLS handshake with this platform's trust roots; a certificate or provider
/// problem would surface as a transport error instead.
///
///   cargo test -p fluree-db-delta --test reader -- --ignored live_
#[tokio::test]
#[ignore = "needs outbound HTTPS"]
async fn live_azure_token_request_reaches_entra() {
    use fluree_db_delta::config::AzureAuth;
    use fluree_db_iceberg::ConfigValue;

    let io = DeltaIoConfig {
        azure: Some(AzureAuth::ClientSecret {
            tenant_id: "00000000-0000-0000-0000-000000000000".to_string(),
            client_id: "00000000-0000-0000-0000-000000000000".to_string(),
            client_secret: ConfigValue::literal("not-a-secret"),
        }),
        ..Default::default()
    };
    let table = DeltaTable::open(
        "probe",
        "abfss://probe@azureopendatastorage.dfs.core.windows.net/none",
        &io,
    )
    .unwrap();
    let err = format!(
        "{:?}",
        table
            .snapshot(VersionSelector::Latest)
            .await
            .err()
            .expect("bogus credentials cannot read")
    );
    println!("{err}");
    let lowered = err.to_lowercase();
    assert!(
        !lowered.contains("certificate") && !lowered.contains("cryptoprovider"),
        "TLS failure: {err}"
    );
    // Entra's answer to an unknown tenant.
    assert!(
        err.contains("AADSTS") || lowered.contains("tenant"),
        "did not reach Entra: {err}"
    );
}
