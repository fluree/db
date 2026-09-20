//! Pruning inside a data file, and footer reuse. The rows a filter returns are
//! the same with or without pruning, so each case also reads the process-wide
//! count of decoded rows — which is why this is its own test binary, with one
//! test.

use std::path::{Path, PathBuf};

use fluree_db_delta::{
    rows_decoded, ColumnFilter, DeltaIoConfig, DeltaSnapshot, DeltaTable, FilterOp, FilterValue,
    VersionSelector,
};
use futures::StreamExt;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn filter(column: &str, op: FilterOp, value: FilterValue) -> ColumnFilter {
    ColumnFilter {
        column: column.to_string(),
        op,
        value,
    }
}

/// The `id`s a scan returns, sorted, and how many rows it decoded to find them.
async fn scan(
    snapshot: &DeltaSnapshot,
    projection: &[&str],
    filters: Vec<ColumnFilter>,
) -> (Vec<i64>, u64) {
    let before = rows_decoded();
    let projection: Vec<String> = projection.iter().map(ToString::to_string).collect();
    let batches: Vec<_> = snapshot
        .scan(&projection, &filters)
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect()
        .await;
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name("id").expect("id");
            (0..batch.num_rows).map(move |row| column.get_i64(row).expect("id"))
        })
        .collect();
    ids.sort_unstable();
    (ids, rows_decoded() - before)
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

#[tokio::test]
async fn row_groups_and_pages_no_filter_can_match_are_not_decoded() {
    let staged = tempfile::tempdir().unwrap();
    std::env::set_var(
        "FLUREE_ICEBERG_LOCAL_ROOTS",
        format!(
            "{}:{}",
            fixtures().display(),
            staged.path().canonicalize().unwrap().display()
        ),
    );
    let int = FilterValue::Int;

    // 4096 rows, `amount = id * 10`, two files of five row groups each. Version
    // 0 has no deletion vector. Both files' log statistics admit every filter
    // here, so what is left out is left out inside the files.
    let table = DeltaTable::open(
        "deletion_vectors",
        fixtures().join("deletion_vectors").to_str().unwrap(),
        &DeltaIoConfig::default(),
    )
    .unwrap();
    let plain = table.snapshot(VersionSelector::Version(0)).await.unwrap();
    let all = ["id", "amount"];

    let (ids, decoded) = scan(&plain, &all, vec![]).await;
    assert_eq!((ids.len(), decoded), (4096, 4096));

    let (ids, decoded) = scan(&plain, &all, vec![filter("id", FilterOp::Eq, int(1500))]).await;
    assert_eq!(ids, [1500]);
    // One 200-row page of each file.
    assert_eq!(decoded, 400);

    let (ids, decoded) = scan(&plain, &all, vec![filter("id", FilterOp::GtEq, int(4000))]).await;
    assert_eq!(ids, (4000..4096).collect::<Vec<_>>());
    assert_eq!(decoded, 266);

    let (ids, decoded) = scan(
        &plain,
        &all,
        vec![filter(
            "amount",
            FilterOp::In,
            FilterValue::Set(vec![int(10), int(40_950)]),
        )],
    )
    .await;
    assert_eq!(ids, [1, 4095]);
    assert_eq!(decoded, 428);

    // Two filters together.
    let (ids, decoded) = scan(
        &plain,
        &all,
        vec![
            filter("id", FilterOp::GtEq, int(1300)),
            filter("amount", FilterOp::LtEq, int(15_000)),
        ],
    )
    .await;
    assert_eq!(ids, (1300..=1500).collect::<Vec<_>>());
    assert_eq!(decoded, 800);

    // A filter on a column the scan does not project still prunes; with no
    // row filter to finish the job, the rows sharing its pages come back too.
    let (ids, decoded) = scan(
        &plain,
        &["id"],
        vec![filter("amount", FilterOp::Eq, int(15_000))],
    )
    .await;
    assert!(ids.contains(&1500));
    assert_eq!(ids.len(), 400);
    assert_eq!(decoded, 400);

    // `!=` rules nothing out.
    let (ids, decoded) = scan(&plain, &all, vec![filter("id", FilterOp::NotEq, int(7))]).await;
    assert_eq!((ids.len(), decoded), (4095, 4096));

    // A deletion vector addresses rows by position in the file, so a file that
    // carries one is decoded whole: 1023 is deleted, 1025 is not.
    let vectored = table.snapshot(VersionSelector::Latest).await.unwrap();
    let (ids, decoded) = scan(
        &vectored,
        &all,
        vec![filter(
            "id",
            FilterOp::In,
            FilterValue::Set(vec![int(1023), int(1025)]),
        )],
    )
    .await;
    assert_eq!(ids, [1025]);
    assert_eq!(decoded, 4096);

    // Each comparable type, on Spark's own encodings. With the log's statistics
    // removed every file is opened; row 1 sits alone in one file, rows 2 and 3
    // (3 is all null) in another, and the footers tell them apart.
    let types = staged.path().canonicalize().unwrap().join("types");
    copy_dir_all(&fixtures().join("types"), &types);
    for entry in std::fs::read_dir(types.join("_delta_log")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let rewritten: Vec<String> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| {
                let mut action: serde_json::Value = serde_json::from_str(line).unwrap();
                if let Some(add) = action.get_mut("add").and_then(|a| a.as_object_mut()) {
                    add.remove("stats");
                }
                action.to_string()
            })
            .collect();
        std::fs::write(&path, rewritten.join("\n") + "\n").unwrap();
    }
    let types = DeltaTable::open("types", types.to_str().unwrap(), &DeltaIoConfig::default())
        .unwrap()
        .snapshot(VersionSelector::Latest)
        .await
        .unwrap();
    let timestamp = |micros, tz| FilterValue::Timestamp { micros, tz };
    for (column, op, value, decoded_rows) in [
        ("small", FilterOp::Eq, int(300), 1),
        ("ratio", FilterOp::Gt, FilterValue::Double(1.0), 1),
        (
            "label",
            FilterOp::Eq,
            FilterValue::Str("alpha".to_string()),
            1,
        ),
        ("day", FilterOp::GtEq, FilterValue::Date(19_782), 1),
        (
            "wall",
            FilterOp::Eq,
            timestamp(1_709_168_523_000_000, false),
            1,
        ),
        // Spark writes a zoned timestamp as INT96, which carries no usable
        // bounds, and a boolean's bounds are not compared: nothing is left out.
        (
            "at",
            FilterOp::Eq,
            timestamp(1_709_210_096_789_000, true),
            3,
        ),
        ("flag", FilterOp::Eq, FilterValue::Bool(true), 3),
    ] {
        let (ids, decoded) = scan(&types, &[], vec![filter(column, op, value)]).await;
        assert_eq!(ids, [1], "{column}");
        assert_eq!(decoded, decoded_rows, "{column}");
    }

    // A data file never changes, so its footer is read once per process. With
    // the file's closing magic overwritten a footer cannot be parsed again, so
    // a second read that succeeds used the one it kept.
    let break_footers = |table: &Path| {
        for entry in std::fs::read_dir(table).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                let mut bytes = std::fs::read(&path).unwrap();
                let end = bytes.len();
                bytes[end - 4..].copy_from_slice(b"XXXX");
                std::fs::write(&path, bytes).unwrap();
            }
        }
    };
    for (name, budget, survives) in [("kept", "64", true), ("unkept", "0", false)] {
        std::env::set_var("FLUREE_DELTA_FOOTER_CACHE_MB", budget);
        let root = staged.path().canonicalize().unwrap().join(name);
        copy_dir_all(&fixtures().join("dim_store"), &root);
        let snapshot = DeltaTable::open(name, root.to_str().unwrap(), &DeltaIoConfig::default())
            .unwrap()
            .snapshot(VersionSelector::Latest)
            .await
            .unwrap();
        let read = || {
            let snapshot = snapshot.clone();
            async move {
                let batches: Vec<_> = snapshot.scan(&[], &[]).unwrap().collect().await;
                batches
                    .into_iter()
                    .map(|batch| batch.map(|b| b.num_rows))
                    .sum::<Result<usize, _>>()
            }
        };
        assert_eq!(read().await.unwrap(), 3);
        break_footers(&root);
        assert_eq!(read().await.ok(), survives.then_some(3), "{name}");
    }
}
