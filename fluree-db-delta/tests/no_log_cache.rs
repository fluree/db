//! `FLUREE_DELTA_LOG_CACHE_MB=0`: nothing is remembered between reads and every
//! scan plans from the log. Its own test binary, because the setting is
//! process-wide.

use std::path::Path;

use fluree_db_delta::{
    ColumnFilter, DeltaIoConfig, DeltaTable, FilterOp, FilterValue, VersionSelector,
};
use futures::StreamExt;

#[tokio::test]
async fn with_no_budget_every_read_replays_the_log() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/partitioned");
    let staged = tempfile::tempdir().unwrap();
    let root = staged.path().canonicalize().unwrap().join("partitioned");
    copy_dir_all(&fixture, &root);
    std::env::set_var(
        "FLUREE_ICEBERG_LOCAL_ROOTS",
        staged.path().canonicalize().unwrap(),
    );
    std::env::set_var("FLUREE_DELTA_LOG_CACHE_MB", "0");

    let table = DeltaTable::open(
        "partitioned",
        root.to_str().unwrap(),
        &DeltaIoConfig::default(),
    )
    .unwrap();
    let latest = table.snapshot(VersionSelector::Latest).await.unwrap();
    let north = ColumnFilter {
        column: "region".to_string(),
        op: FilterOp::Eq,
        value: FilterValue::Str("north".to_string()),
    };
    assert_eq!(latest.file_count(&[]).await.unwrap(), 6);
    assert_eq!(
        latest
            .file_count(std::slice::from_ref(&north))
            .await
            .unwrap(),
        2
    );
    assert_eq!(latest.exact_row_count(&[]).await.unwrap(), Some(24));
    let rows: usize = latest
        .scan(&["id".to_string()], &[north])
        .unwrap()
        .map(|batch| batch.unwrap().num_rows)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .sum();
    assert_eq!(rows, 8);

    // Nothing was kept: without its log the table cannot be read again.
    let log = root.join("_delta_log");
    for entry in std::fs::read_dir(&log).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() && entry.file_name() != *format!("{:020}.json", 5) {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    assert!(table.snapshot(VersionSelector::Latest).await.is_err());
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
