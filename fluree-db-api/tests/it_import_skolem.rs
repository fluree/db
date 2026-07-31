//! Identity of blank nodes minted by bulk import.
//!
//! Import skolemizes every blank node in its source into the reserved
//! `_:fdb-...` label space. These tests pin what that id *is* — not its exact
//! bytes, which are an implementation detail, but the four properties callers
//! actually depend on:
//!
//! - it can be written back in SPARQL, so an imported blank-node structure is
//!   editable in place the same way a transacted one is
//!   ([`it_stable_blank_nodes`](super::it_stable_blank_nodes));
//! - it depends on the document, not on the shape of the import — adding a file
//!   to the directory does not renumber anything;
//! - it depends on the ledger, so two ledgers loaded from one tree do not look
//!   like they share nodes;
//! - unless the caller says otherwise, in which case they do.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use serde_json::json;

/// A document with one labeled blank node (referenced twice, so a merge failure
/// is visible as two subjects) and one anonymous node.
const DOC: &str = r#"@prefix ex: <http://example.org/> .
@prefix schema: <http://schema.org/> .

ex:alice ex:knows _:shared .
_:shared schema:name "Shared" .
ex:bob ex:knows _:shared .
ex:carol ex:owns [ schema:name "Anonymous" ] .
"#;

fn write(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).expect("write fixture");
    path
}

/// Every `_:fdb-` id in the ledger that carries `schema:name` `name`.
async fn blank_ids_named(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    name: &str,
) -> Vec<String> {
    let qr = support::query_jsonld(
        fluree,
        ledger,
        &json!({
            "select": ["?s"],
            "where": {"@id": "?s", "http://schema.org/name": name}
        }),
    )
    .await
    .expect("query");
    let json = qr.to_jsonld(&ledger.snapshot).expect("jsonld");
    let mut ids: Vec<String> = json
        .as_array()
        .expect("array result")
        .iter()
        .filter_map(|row| match row {
            serde_json::Value::Array(cols) => cols.first().and_then(|v| v.as_str()),
            other => other.as_str(),
        })
        .filter(|s| s.starts_with("_:fdb-"))
        .map(str::to_string)
        .collect();
    ids.sort();
    ids
}

/// The sole `_:fdb-` id carrying `schema:name` `name`.
async fn sole_blank_id(fluree: &fluree_db_api::Fluree, ledger: &LedgerState, name: &str) -> String {
    let ids = blank_ids_named(fluree, ledger, name).await;
    assert_eq!(
        ids.len(),
        1,
        "expected exactly one node named {name}: {ids:?}"
    );
    ids.into_iter().next().unwrap()
}

/// Import `DOC` (plus any extra files) into a fresh ledger and hand back the
/// live handle.
async fn import_doc(
    db_dir: &std::path::Path,
    data_dir: &std::path::Path,
    alias: &str,
    namespace: Option<&str>,
) -> (fluree_db_api::Fluree, LedgerState) {
    let fluree = FlureeBuilder::file(db_dir.to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let mut builder = fluree
        .create(alias)
        .import(data_dir)
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false);
    if let Some(ns) = namespace {
        builder = builder.skolem_namespace(ns);
    }
    builder.execute().await.expect("import should succeed");
    let ledger = fluree.ledger(alias).await.expect("load ledger");
    (fluree, ledger)
}

// ============================================================================
// SPARQL writability — the reason the format changed
// ============================================================================

/// The headline property. Import used to build its skolem key out of the ledger
/// id, so a minted id looked like `_:fdb-test/skolem-sparql:main-d0-shared`.
/// SPARQL's `BLANK_NODE_LABEL` production admits neither `/` nor `:`, so that
/// id could be read out of a query result and never written back — a
/// bulk-imported blank-node structure was, uniquely, uneditable from SPARQL.
///
/// Fails before the hashed format: `parse_sparql` rejects the DELETE template.
#[tokio::test]
async fn imported_blank_node_id_is_writable_in_sparql() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    write(data_dir.path(), "doc.ttl", DOC);

    // A ledger id with both offenders in it: `/` in the name, `:` before the
    // branch.
    let (fluree, ledger) = import_doc(
        db_dir.path(),
        data_dir.path(),
        "test/skolem-sparql:main",
        None,
    )
    .await;
    let bnode = sole_blank_id(&fluree, &ledger, "Shared").await;

    let sparql = format!(
        "PREFIX schema: <http://schema.org/>\n\
         DELETE {{ {bnode} schema:name ?old }}\n\
         INSERT {{ {bnode} schema:name \"Renamed\" }}\n\
         WHERE  {{ {bnode} schema:name ?old }}"
    );
    let parsed = fluree_db_sparql::parse_sparql(&sparql);
    assert!(
        !parsed.has_errors(),
        "a minted import id must lex as a SPARQL BLANK_NODE_LABEL; \
         id was {bnode}, diagnostics: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE");
    let ledger = fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("stage SPARQL UPDATE")
        .ledger;

    // The edit landed on the stored node, rather than minting a fresh one.
    assert!(
        blank_ids_named(&fluree, &ledger, "Shared").await.is_empty(),
        "the old name should be retracted"
    );
    assert_eq!(
        sole_blank_id(&fluree, &ledger, "Renamed").await,
        bnode,
        "the SPARQL update must address the stored node, not mint a new one"
    );
}

// ============================================================================
// What the id depends on
// ============================================================================

/// Blank-node ids must not depend on what else happens to be in the import.
///
/// Before the hashed format the document scope was the file's *position* in the
/// sorted directory listing, and anonymous nodes carried the global chunk
/// ordinal on top of that — so dropping one more file into the directory
/// renumbered every id behind it. Nothing about `doc.ttl`'s content changed;
/// its ids did.
///
/// Fails before: `doc.ttl` is file 0 in the first import and file 1 in the
/// second, so both the labeled and the anonymous id come out different.
#[tokio::test]
async fn minted_ids_do_not_depend_on_the_rest_of_the_directory() {
    let alone_db = tempfile::tempdir().expect("db tmpdir");
    let alone_data = tempfile::tempdir().expect("data tmpdir");
    write(alone_data.path(), "02-doc.ttl", DOC);

    // Same file, but now preceded by another document in listing order.
    let together_db = tempfile::tempdir().expect("db tmpdir");
    let together_data = tempfile::tempdir().expect("data tmpdir");
    write(
        together_data.path(),
        "01-other.ttl",
        "<http://example.org/x> <http://example.org/p> <http://example.org/y> .\n",
    );
    write(together_data.path(), "02-doc.ttl", DOC);

    // Same ledger id on both sides, in separate stores, so the ONLY thing that
    // differs between the two imports is the extra file.
    let (f1, l1) = import_doc(
        alone_db.path(),
        alone_data.path(),
        "test/skolem-shape:main",
        None,
    )
    .await;
    let (f2, l2) = import_doc(
        together_db.path(),
        together_data.path(),
        "test/skolem-shape:main",
        None,
    )
    .await;

    assert_eq!(
        sole_blank_id(&f1, &l1, "Shared").await,
        sole_blank_id(&f2, &l2, "Shared").await,
        "a labeled blank node's id depends on its document, not on the import"
    );
    assert_eq!(
        sole_blank_id(&f1, &l1, "Anonymous").await,
        sole_blank_id(&f2, &l2, "Anonymous").await,
        "an anonymous node's id depends on its document, not on the import"
    );
}

/// Two ledgers loaded from one tree hold *different* blank nodes. Blank nodes
/// are local to the graph that contains them; nothing should make two ledgers
/// look like they share one by accident.
#[tokio::test]
async fn different_ledgers_mint_different_ids() {
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    write(data_dir.path(), "doc.ttl", DOC);
    let db_a = tempfile::tempdir().expect("db tmpdir");
    let db_b = tempfile::tempdir().expect("db tmpdir");

    let (fa, la) = import_doc(db_a.path(), data_dir.path(), "test/skolem-one:main", None).await;
    let (fb, lb) = import_doc(db_b.path(), data_dir.path(), "test/skolem-two:main", None).await;

    assert_ne!(
        sole_blank_id(&fa, &la, "Shared").await,
        sole_blank_id(&fb, &lb, "Shared").await,
        "the ledger id salts the mint"
    );
}

/// …but the two spellings of ONE ledger id are one ledger, so they must salt
/// identically. `create my/ledger --import` and `create my/ledger:main --import`
/// both land on `my/ledger:main`.
#[tokio::test]
async fn ledger_id_spelling_does_not_change_minted_ids() {
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    write(data_dir.path(), "doc.ttl", DOC);
    let db_bare = tempfile::tempdir().expect("db tmpdir");
    let db_branch = tempfile::tempdir().expect("db tmpdir");

    let (f1, l1) = import_doc(db_bare.path(), data_dir.path(), "test/skolem-spell", None).await;
    let (f2, l2) = import_doc(
        db_branch.path(),
        data_dir.path(),
        "test/skolem-spell:main",
        None,
    )
    .await;

    assert_eq!(
        sole_blank_id(&f1, &l1, "Shared").await,
        sole_blank_id(&f2, &l2, "Shared").await,
        "'name' and 'name:main' are the same ledger and must salt the same"
    );
}

/// `--skolem-namespace` makes the cross-ledger sharing deliberate: two ledgers
/// imported from one tree under one namespace mint identical ids, so a blank
/// node can be matched across them.
#[tokio::test]
async fn explicit_skolem_namespace_shares_ids_across_ledgers() {
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    write(data_dir.path(), "doc.ttl", DOC);
    let db_a = tempfile::tempdir().expect("db tmpdir");
    let db_b = tempfile::tempdir().expect("db tmpdir");

    let (fa, la) = import_doc(
        db_a.path(),
        data_dir.path(),
        "test/skolem-ns-a:main",
        Some("shared-corpus"),
    )
    .await;
    let (fb, lb) = import_doc(
        db_b.path(),
        data_dir.path(),
        "test/skolem-ns-b:main",
        Some("shared-corpus"),
    )
    .await;

    assert_eq!(
        sole_blank_id(&fa, &la, "Shared").await,
        sole_blank_id(&fb, &lb, "Shared").await,
        "an explicit namespace overrides the ledger id on both sides"
    );
}

/// Two files sitting at the same relative path in *different* trees are two
/// documents of one import only if their keys differ — here they do not clash,
/// but their labels must still stay apart, which is the invariant a shared
/// scope would break.
#[tokio::test]
async fn labels_stay_distinct_between_documents() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    write(data_dir.path(), "a.ttl", DOC);
    write(
        data_dir.path(),
        "b.ttl",
        "@prefix schema: <http://schema.org/> .\n\
         _:shared schema:name \"Other document\" .\n",
    );

    let (fluree, ledger) = import_doc(
        db_dir.path(),
        data_dir.path(),
        "test/skolem-distinct:main",
        None,
    )
    .await;

    assert_ne!(
        sole_blank_id(&fluree, &ledger, "Shared").await,
        sole_blank_id(&fluree, &ledger, "Other document").await,
        "`_:shared` in two documents is two nodes"
    );
}

// ============================================================================
// ndjson: one source is one document
// ============================================================================

/// `NdjsonReader` cuts one `.jsonl` source into many JSON-LD chunks, applying
/// the source's leading `@context` to each. The document scope has to follow
/// the source, not the cut: before this change the scope was the chunk, so
/// `_:shared` near the top of a file and `_:shared` near the bottom became two
/// subjects the moment the file grew past one chunk — and the boundary that
/// decided it was `chunk_size_mb`, not anything in the data.
///
/// Fails before: the reference and the named node land in different chunks and
/// resolve to different subjects.
#[tokio::test]
async fn ndjson_blank_label_is_scoped_to_its_source_not_its_chunk() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let mut lines = String::from(
        "{\"@context\":{\"ex\":\"http://example.org/ns/\",\"schema\":\"http://schema.org/\"}}\n\
         {\"@id\":\"_:shared\",\"schema:name\":\"Shared\"}\n",
    );
    // Pad past one 1 MB chunk so the last line is cut into a different chunk.
    for i in 0..12_000 {
        lines.push_str(&format!(
            "{{\"@id\":\"ex:filler{i}\",\"schema:description\":\"padding that pushes the reference into a later import chunk\"}}\n"
        ));
    }
    lines.push_str("{\"@id\":\"ex:alice\",\"ex:knows\":{\"@id\":\"_:shared\"}}\n");
    assert!(lines.len() > 1024 * 1024, "fixture must exceed one chunk");
    write(data_dir.path(), "people.jsonl", &lines);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let result = fluree
        .create("test/skolem-ndjson:main")
        .import(data_dir.path())
        .threads(1)
        .chunk_size_mb(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("ndjson import should succeed");
    assert!(
        result.t > 1,
        "test is vacuous unless the source was split: {} chunk(s)",
        result.t
    );
    let ledger = fluree
        .ledger("test/skolem-ndjson:main")
        .await
        .expect("load ledger");

    let named = sole_blank_id(&fluree, &ledger, "Shared").await;

    let qr = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({
            "select": ["?o"],
            "where": {"@id": "http://example.org/ns/alice", "http://example.org/ns/knows": "?o"}
        }),
    )
    .await
    .expect("query");
    let json = qr.to_jsonld(&ledger.snapshot).expect("jsonld");
    let referenced = json
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| match row {
            serde_json::Value::Array(cols) => cols.first().and_then(|v| v.as_str()),
            other => other.as_str(),
        })
        .expect("alice must reference a node")
        .to_string();

    assert_eq!(
        referenced, named,
        "`_:shared` unifies across one ndjson source however the chunker cuts it"
    );
}

// ============================================================================
// Duplicate document keys
// ============================================================================

/// The local arms cannot produce a duplicate key — `read_dir` yields each entry
/// once — but `RemoteSource::OrderedObjects` takes a caller-supplied list, and
/// nothing stops that list naming one address twice. Two documents sharing a
/// scope silently merge their `_:x` nodes, so the import refuses to start.
#[tokio::test]
async fn duplicate_remote_addresses_abort_the_import() {
    use fluree_db_core::{MemoryStorage, StorageRead, StorageWrite};
    use std::sync::Arc;

    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let storage = Arc::new(MemoryStorage::new());
    storage
        .write_bytes("remote/doc.ttl", DOC.as_bytes())
        .await
        .expect("seed remote object");
    let object = fluree_db_api::RemoteObject {
        address: "remote/doc.ttl".to_string(),
        size_bytes: DOC.len() as u64,
    };

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let storage_dyn: Arc<dyn StorageRead> = storage.clone();
    let err = fluree
        .create("test/skolem-dup:main")
        .import_from_storage(
            storage_dyn,
            fluree_db_api::RemoteSource::OrderedObjects(vec![object.clone(), object]),
        )
        .threads(1)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect_err("an import naming one object twice must not run");

    let msg = err.to_string();
    assert!(
        msg.contains("share the blank-node scope") && msg.contains("remote/doc.ttl"),
        "the error must name the colliding documents, got: {msg}"
    );
}

// ============================================================================
// Import manifest
// ============================================================================

/// A minted id is a hash, so on its own it says nothing about where it came
/// from. The import records the mapping in the `txn-meta` graph: one
/// `db:importSource` triple per document, subject = the blank-node scope every
/// id from that document shares. Together with `skolem::split_doc_scope`, that
/// turns an opaque `_:fdb-d…-label` back into a file name.
#[tokio::test]
async fn import_manifest_maps_blank_node_scopes_back_to_documents() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    write(data_dir.path(), "a.ttl", DOC);
    write(
        data_dir.path(),
        "b.ttl",
        "@prefix schema: <http://schema.org/> .\n\
         _:shared schema:name \"Other document\" .\n",
    );

    let (fluree, ledger) = import_doc(
        db_dir.path(),
        data_dir.path(),
        "test/skolem-manifest:main",
        None,
    )
    .await;

    let manifest = fluree
        .query_connection(&json!({
            "from": "test/skolem-manifest:main#txn-meta",
            "select": ["?doc", "?source"],
            "where": {"@id": "?doc", "https://ns.flur.ee/db#importSource": "?source"}
        }))
        .await
        .expect("txn-meta query");
    let rows = manifest.to_jsonld(&ledger.snapshot).expect("jsonld");
    let entries: Vec<(String, String)> = rows
        .as_array()
        .expect("array result")
        .iter()
        .filter_map(|row| {
            let cols = row.as_array()?;
            Some((
                cols.first()?.as_str()?.into(),
                cols.get(1)?.as_str()?.into(),
            ))
        })
        .collect();

    let sources: std::collections::BTreeSet<&str> =
        entries.iter().map(|(_, src)| src.as_str()).collect();
    assert_eq!(
        sources,
        ["a.ttl", "b.ttl"].into_iter().collect(),
        "every source document must appear, keyed relative to the import root"
    );

    // The round trip a reader actually makes: minted id → scope → document.
    let bnode = sole_blank_id(&fluree, &ledger, "Shared").await;
    let local = bnode.strip_prefix("_:").expect("blank-node id");
    let (scope, label) =
        fluree_db_core::skolem::split_doc_scope(local).expect("minted id must carry a doc scope");
    assert_eq!(label, "shared", "the source label survives the mint");
    let source = entries
        .iter()
        .find(|(doc, _)| doc.strip_prefix("_:fdb-") == Some(scope))
        .map(|(_, src)| src.as_str());
    assert_eq!(
        source,
        Some("a.ttl"),
        "the manifest must resolve {bnode} back to its document; entries: {entries:?}"
    );
}
