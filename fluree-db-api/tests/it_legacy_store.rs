//! A store written by Fluree 4.2.1, before name bindings, keeps working. On
//! open it moves to `ns@v3/`; its ledgers are bound where their data already
//! is, and the ledger it had soft-dropped is in the dropped list.
//!
//! The fixture was written by the released 4.2.1 CLI and server: `legacydb`
//! (Alice, indexed, then Bob; branch `dev` then Carol) and `gonedb` (Dave),
//! soft-dropped through `POST /drop` with `"hard": false`.

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_core::{LedgerName, StorageRoot};
use serde_json::json;
use std::path::Path;

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn legacy_store() -> tempfile::TempDir {
    let tmp = tempfile::TempDir::new().unwrap();
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-4.2.1-store");
    copy_dir(&fixture, tmp.path());
    tmp
}

/// The `ex:name` values `id` holds, sorted.
async fn names(fluree: &Fluree, id: &str) -> Vec<String> {
    let ledger = fluree.ledger(id).await.expect("load");
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?n",
        "where": {"@id": "?s", "ex:name": "?n"}
    });
    let result = support::query_jsonld(fluree, &ledger, &query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("format");
    let mut names: Vec<String> = serde_json::from_value(result).unwrap();
    names.sort();
    names
}

#[tokio::test]
async fn a_store_from_before_name_bindings_opens_migrated() {
    let tmp = legacy_store();
    let legacy_record = std::fs::read(tmp.path().join("ns@v2/legacydb/main.json")).unwrap();
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    assert_eq!(names(&fluree, "legacydb").await, ["Alice", "Bob"]);
    assert_eq!(
        names(&fluree, "legacydb:dev").await,
        ["Alice", "Bob", "Carol"]
    );

    // Its data stays where 4.2.1 put it, and it takes writes again.
    let ledger = fluree.ledger("legacydb").await.expect("load");
    assert_eq!(
        ledger
            .ns_record
            .as_ref()
            .and_then(|r| r.storage_root.clone()),
        Some(StorageRoot::legacy(&LedgerName::parse("legacydb").unwrap()))
    );
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [{"@id": "ex:eve", "ex:name": "Eve"}]
            }),
        )
        .await
        .expect("insert after migration");

    assert_eq!(names(&fluree, "legacydb").await, ["Alice", "Bob", "Eve"]);

    // The soft-dropped ledger freed its name and restores.
    let dropped = fluree.list_dropped().await.expect("list dropped");
    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].name, "gonedb");
    assert!(fluree.ledger("gonedb").await.is_err());
    fluree
        .restore_dropped(dropped[0].instance.as_str())
        .await
        .expect("restore");

    assert_eq!(names(&fluree, "gonedb").await, ["Dave"]);

    // 4.2.1's address is left as it was, for a rollback.
    assert_eq!(
        std::fs::read(tmp.path().join("ns@v2/legacydb/main.json")).unwrap(),
        legacy_record
    );

    // A second open finds the store current.
    drop(fluree);
    let reopened = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("reopen");
    assert_eq!(names(&reopened, "legacydb").await, ["Alice", "Bob", "Eve"]);
}
