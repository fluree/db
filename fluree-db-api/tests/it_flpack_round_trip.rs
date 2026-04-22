//! Round-trip test for native ledger export/import via `.flpack` files.
//!
//! Creates a file-backed ledger, transacts data, exports it as a `.flpack`
//! pack stream, then imports into a second (separate) file-backed instance
//! under a different name. Verifies that query results match.
//!
//! Run with:
//!   cargo test -p fluree-db-api --test it_flpack_round_trip --features native

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::pack::{compute_missing_commits, compute_missing_index_artifacts};
use fluree_db_api::FlureeBuilder;
use fluree_db_core::commit::codec::envelope::decode_envelope;
use fluree_db_core::commit::codec::format::{CommitHeader, HEADER_LEN};
use fluree_db_core::pack::{
    decode_frame, encode_data_frame, encode_end_frame, encode_header_frame, encode_manifest_frame,
    read_stream_preamble, write_stream_preamble, PackFrame, PackHeader, DEFAULT_MAX_PAYLOAD,
};
use fluree_db_core::{ContentKind, ContentStore};
use fluree_db_nameservice_sync::ingest_pack_frame;
use serde_json::json;
use std::collections::HashSet;

/// Export a ledger to an in-memory `.flpack` byte buffer.
async fn export_ledger_to_bytes(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> Vec<u8> {
    let ns_record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("ledger should exist in ns");

    let commit_head_id = ns_record
        .commit_head_id
        .as_ref()
        .expect("ledger should have commits");

    let content_store = fluree.content_store(ledger_id);

    let missing_commits = compute_missing_commits(
        &content_store,
        std::slice::from_ref(commit_head_id),
        &HashSet::new(),
    )
    .await
    .expect("walk commit chain");

    let index_artifacts = if let Some(ref index_id) = ns_record.index_head_id {
        Some(
            compute_missing_index_artifacts(&content_store, index_id, None)
                .await
                .expect("enumerate index artifacts"),
        )
    } else {
        None
    };

    let artifact_count = index_artifacts.as_ref().map_or(0, std::vec::Vec::len);

    let mut out = Vec::new();
    write_stream_preamble(&mut out);

    let header = if artifact_count > 0 {
        PackHeader::with_indexes(
            Some(missing_commits.len() as u32),
            Some(artifact_count as u32),
            0,
            true,
        )
    } else {
        PackHeader::commits_only(Some(missing_commits.len() as u32), true)
    };
    encode_header_frame(&header, &mut out);

    let mut buf = Vec::new();
    let mut txn_cids_sent = HashSet::new();

    for commit_cid in &missing_commits {
        let raw_bytes = content_store.get(commit_cid).await.expect("read commit");

        buf.clear();
        encode_data_frame(commit_cid, &raw_bytes, &mut buf);
        out.extend_from_slice(&buf);

        // Decode envelope to find txn blob CID.
        let hdr = CommitHeader::read_from(&raw_bytes).expect("commit header");
        let envelope_start = HEADER_LEN;
        let envelope_end = envelope_start + hdr.envelope_len as usize;
        if envelope_end <= raw_bytes.len() {
            if let Ok(env) = decode_envelope(&raw_bytes[envelope_start..envelope_end]) {
                if let Some(ref txn_cid) = env.txn {
                    if txn_cids_sent.insert(txn_cid.clone()) {
                        let txn_bytes = content_store.get(txn_cid).await.expect("read txn blob");
                        buf.clear();
                        encode_data_frame(txn_cid, &txn_bytes, &mut buf);
                        out.extend_from_slice(&buf);
                    }
                }
            }
        }
    }

    // Index artifacts.
    if let Some(ref artifacts) = index_artifacts {
        let index_root_id = ns_record.index_head_id.as_ref().unwrap();
        buf.clear();
        let manifest = json!({
            "phase": "indexes",
            "root_id": index_root_id.to_string(),
            "artifact_count": artifacts.len(),
        });
        encode_manifest_frame(&manifest, &mut buf);
        out.extend_from_slice(&buf);

        for artifact_cid in artifacts {
            let artifact_bytes = content_store
                .get(artifact_cid)
                .await
                .expect("read artifact");
            buf.clear();
            encode_data_frame(artifact_cid, &artifact_bytes, &mut buf);
            out.extend_from_slice(&buf);
        }
    }

    // Nameservice manifest.
    buf.clear();
    let ns_manifest = json!({
        "phase": "nameservice",
        "ledger_id": ns_record.ledger_id,
        "name": ns_record.name,
        "branch": ns_record.branch,
        "commit_head_id": commit_head_id.to_string(),
        "commit_t": ns_record.commit_t,
        "index_head_id": ns_record.index_head_id.as_ref().map(ToString::to_string),
        "index_t": ns_record.index_t,
    });
    encode_manifest_frame(&ns_manifest, &mut buf);
    out.extend_from_slice(&buf);

    // End frame.
    buf.clear();
    encode_end_frame(&mut buf);
    out.extend_from_slice(&buf);

    out
}

/// Import a `.flpack` byte buffer into a Fluree instance under the given ledger name.
async fn import_ledger_from_bytes(fluree: &fluree_db_api::Fluree, ledger_id: &str, data: &[u8]) {
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger for import");

    let mut pos = read_stream_preamble(data).expect("valid preamble");
    let admin_storage = fluree
        .backend()
        .admin_storage_cloned()
        .expect("managed backend");

    let mut saw_header = false;
    let mut ns_manifest: Option<serde_json::Value> = None;
    let mut objects = 0usize;

    loop {
        assert!(pos < data.len(), "unexpected end of pack stream");

        let (frame, consumed) =
            decode_frame(&data[pos..], DEFAULT_MAX_PAYLOAD).expect("decode frame");
        pos += consumed;

        match frame {
            PackFrame::Header(_) => {
                saw_header = true;
            }
            PackFrame::Data { cid, payload } => {
                assert!(saw_header, "data frame before header");
                ingest_pack_frame(&cid, &payload, &admin_storage, ledger_id)
                    .await
                    .unwrap_or_else(|e| panic!("ingest failed for {cid}: {e}"));
                objects += 1;
            }
            PackFrame::Manifest(json) => {
                if json.get("phase").and_then(|v| v.as_str()) == Some("nameservice") {
                    ns_manifest = Some(json);
                }
            }
            PackFrame::Error(msg) => panic!("error frame in pack: {msg}"),
            PackFrame::End => break,
        }
    }

    assert!(
        objects > 0,
        "pack stream should contain at least one object"
    );

    let manifest = ns_manifest.expect("pack stream should contain nameservice manifest");

    let handle = fluree.ledger_cached(ledger_id).await.expect("load handle");

    // Set commit head.
    let commit_cid_str = manifest
        .get("commit_head_id")
        .and_then(|v| v.as_str())
        .expect("manifest should have commit_head_id");
    let commit_cid: fluree_db_core::ContentId = commit_cid_str.parse().expect("parse commit CID");
    let commit_t = manifest
        .get("commit_t")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    fluree
        .set_commit_head(&handle, &commit_cid, commit_t)
        .await
        .expect("set commit head");

    // Set index head (if present).
    if let Some(index_cid_str) = manifest.get("index_head_id").and_then(|v| v.as_str()) {
        let index_cid: fluree_db_core::ContentId = index_cid_str.parse().expect("parse index CID");
        let index_t = manifest
            .get("index_t")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        fluree
            .set_index_head(&handle, &index_cid, index_t)
            .await
            .expect("set index head");
    }
}

/// Full round-trip: create → transact → export → import (different name) → query.
#[tokio::test]
async fn flpack_export_import_round_trip() {
    let src_dir = tempfile::TempDir::new().expect("src tempdir");
    let dst_dir = tempfile::TempDir::new().expect("dst tempdir");

    let src_ledger = "flpack-test/source:main";
    let dst_ledger = "flpack-test/imported:main";

    // ── Source: create and populate ──────────────────────────────────
    let src_fluree = FlureeBuilder::file(src_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build source");

    let src_db = fluree_db_core::LedgerSnapshot::genesis(src_ledger);
    let src_state = fluree_db_api::LedgerState::new(src_db, fluree_db_api::Novelty::new(0));

    let insert_data = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:age": 42
            },
            {
                "@id": "ex:bob",
                "@type": "ex:User",
                "schema:name": "Bob",
                "schema:age": 22
            },
            {
                "@id": "ex:carol",
                "@type": "ex:User",
                "schema:name": "Carol",
                "schema:age": 33
            }
        ]
    });

    let committed = src_fluree
        .insert(src_state, &insert_data)
        .await
        .expect("insert");
    assert_eq!(committed.receipt.t, 1);

    // Run a query on the source to get expected results.
    let query = json!({
        "select": ["?name"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        },
        "orderBy": "?name",
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        }
    });

    let src_db = fluree_db_api::GraphDb::from_ledger_state(&committed.ledger);
    let src_result = src_fluree.query(&src_db, &query).await.expect("src query");
    let src_json = src_result
        .to_jsonld(&committed.ledger.snapshot)
        .expect("src to_jsonld");

    // ── Export ───────────────────────────────────────────────────────
    let pack_bytes = export_ledger_to_bytes(&src_fluree, src_ledger).await;
    assert!(
        pack_bytes.len() > 100,
        "pack should contain substantial data"
    );

    // ── Destination: import under a different name ───────────────────
    let dst_fluree = FlureeBuilder::file(dst_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build destination");

    import_ledger_from_bytes(&dst_fluree, dst_ledger, &pack_bytes).await;

    // ── Query the imported ledger ───────────────────────────────────
    let dst_handle = dst_fluree
        .ledger(dst_ledger)
        .await
        .expect("load imported ledger");

    let dst_db = fluree_db_api::GraphDb::from_ledger_state(&dst_handle);
    let dst_result = dst_fluree.query(&dst_db, &query).await.expect("dst query");
    let dst_json = dst_result
        .to_jsonld(&dst_handle.snapshot)
        .expect("dst to_jsonld");

    // Results should match.
    assert_eq!(
        src_json, dst_json,
        "query results should match after flpack round-trip"
    );
    // Verify non-empty.
    let arr = dst_json.as_array().expect("result should be array");
    assert_eq!(arr.len(), 3, "should have 3 user names");
}

/// Round-trip with binary indexing: transact → index → export (with index artifacts) → import → query.
#[tokio::test]
async fn flpack_export_import_round_trip_with_index() {
    use support::{start_background_indexer_local, trigger_index_and_wait};

    let src_dir = tempfile::TempDir::new().expect("src tempdir");
    let dst_dir = tempfile::TempDir::new().expect("dst tempdir");

    let src_ledger = "flpack-test/indexed-source:main";
    let dst_ledger = "flpack-test/indexed-imported:main";

    // ── Source: create, populate, and index ─────────────────────────
    let mut src_fluree = FlureeBuilder::file(src_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build source");

    let (local, handle) = start_background_indexer_local(
        src_fluree.backend().clone(),
        Arc::new(src_fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    src_fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async {
            let src_db = fluree_db_core::LedgerSnapshot::genesis(src_ledger);
            let src_state = fluree_db_api::LedgerState::new(src_db, fluree_db_api::Novelty::new(0));

            let insert_data = json!({
                "@context": {
                    "ex": "http://example.org/ns/",
                    "schema": "http://schema.org/"
                },
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "@type": "ex:User",
                        "schema:name": "Alice",
                        "schema:age": 42
                    },
                    {
                        "@id": "ex:bob",
                        "@type": "ex:User",
                        "schema:name": "Bob",
                        "schema:age": 22
                    },
                    {
                        "@id": "ex:carol",
                        "@type": "ex:User",
                        "schema:name": "Carol",
                        "schema:age": 33
                    }
                ]
            });

            let committed = src_fluree
                .insert(src_state, &insert_data)
                .await
                .expect("insert");
            assert_eq!(committed.receipt.t, 1);

            // Trigger indexing and wait for completion.
            trigger_index_and_wait(&handle, src_ledger, committed.receipt.t).await;

            // Verify index head is set.
            let ns_record = src_fluree
                .nameservice()
                .lookup(src_ledger)
                .await
                .expect("ns lookup")
                .expect("ledger should exist");
            assert!(
                ns_record.index_head_id.is_some(),
                "index_head_id should be set after indexing"
            );

            // Query source for expected results.
            let query = json!({
                "select": ["?name"],
                "where": {
                    "@id": "?s",
                    "@type": "ex:User",
                    "schema:name": "?name"
                },
                "orderBy": "?name",
                "@context": {
                    "ex": "http://example.org/ns/",
                    "schema": "http://schema.org/"
                }
            });

            let src_db = fluree_db_api::GraphDb::from_ledger_state(&committed.ledger);
            let src_result = src_fluree.query(&src_db, &query).await.expect("src query");
            let src_json = src_result
                .to_jsonld(&committed.ledger.snapshot)
                .expect("src to_jsonld");

            // ── Export (should include index artifacts) ─────────────
            let pack_bytes = export_ledger_to_bytes(&src_fluree, src_ledger).await;

            // Verify the pack contains index artifact frames.
            let mut pos = read_stream_preamble(&pack_bytes).expect("preamble");
            let mut index_artifact_count = 0usize;
            let mut has_index_manifest = false;
            loop {
                let (frame, consumed) =
                    decode_frame(&pack_bytes[pos..], DEFAULT_MAX_PAYLOAD).expect("decode");
                pos += consumed;
                match frame {
                    PackFrame::Data { cid, .. }
                        if cid.content_kind() != Some(ContentKind::Commit)
                            && cid.content_kind() != Some(ContentKind::Txn) =>
                    {
                        index_artifact_count += 1;
                    }
                    PackFrame::Manifest(ref json)
                        if json.get("phase").and_then(|v| v.as_str()) == Some("indexes") =>
                    {
                        has_index_manifest = true;
                    }
                    PackFrame::End => break,
                    _ => {}
                }
            }
            assert!(
                has_index_manifest,
                "pack should contain an indexes manifest"
            );
            assert!(
                index_artifact_count > 0,
                "pack should contain index artifact data frames"
            );

            // ── Destination: import ────────────────────────────────
            let dst_fluree = FlureeBuilder::file(dst_dir.path().to_string_lossy().to_string())
                .build()
                .expect("build destination");

            import_ledger_from_bytes(&dst_fluree, dst_ledger, &pack_bytes).await;

            // ── Query the imported ledger ───────────────────────────
            let dst_handle = dst_fluree
                .ledger(dst_ledger)
                .await
                .expect("load imported ledger");

            let dst_db = fluree_db_api::GraphDb::from_ledger_state(&dst_handle);
            let dst_result = dst_fluree.query(&dst_db, &query).await.expect("dst query");
            let dst_json = dst_result
                .to_jsonld(&dst_handle.snapshot)
                .expect("dst to_jsonld");

            assert_eq!(
                src_json, dst_json,
                "query results should match after indexed flpack round-trip"
            );
            let arr = dst_json.as_array().expect("result should be array");
            assert_eq!(arr.len(), 3, "should have 3 user names");
        })
        .await;
}

/// Verify that the pack stream can be decoded frame-by-frame.
#[tokio::test]
async fn flpack_stream_structure_is_valid() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let ledger_id = "flpack-test/structure:main";

    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    let db = fluree_db_core::LedgerSnapshot::genesis(ledger_id);
    let state = fluree_db_api::LedgerState::new(db, fluree_db_api::Novelty::new(0));

    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:test",
        "ex:value": "hello"
    });

    let committed = fluree.insert(state, &insert).await.expect("insert");
    assert_eq!(committed.receipt.t, 1);

    let pack_bytes = export_ledger_to_bytes(&fluree, ledger_id).await;

    // Walk the frame structure.
    let mut pos = read_stream_preamble(&pack_bytes).expect("preamble");
    let mut frame_types: Vec<String> = Vec::new();
    let mut commit_count = 0usize;
    let mut has_ns_manifest = false;

    loop {
        let (frame, consumed) =
            decode_frame(&pack_bytes[pos..], DEFAULT_MAX_PAYLOAD).expect("decode");
        pos += consumed;

        match frame {
            PackFrame::Header(h) => {
                frame_types.push("Header".to_string());
                assert!(
                    h.commit_count.unwrap_or(0) > 0,
                    "header should report commits"
                );
            }
            PackFrame::Data { cid, .. } => {
                frame_types.push(format!("Data({:?})", cid.content_kind()));
                if cid.content_kind() == Some(ContentKind::Commit) {
                    commit_count += 1;
                }
            }
            PackFrame::Manifest(ref json) => {
                let phase = json
                    .get("phase")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                frame_types.push(format!("Manifest({phase})"));
                if phase == "nameservice" {
                    has_ns_manifest = true;
                    assert!(
                        json.get("commit_head_id").is_some(),
                        "ns manifest should have commit_head_id"
                    );
                    assert!(
                        json.get("commit_t").is_some(),
                        "ns manifest should have commit_t"
                    );
                }
            }
            PackFrame::Error(msg) => panic!("unexpected error frame: {msg}"),
            PackFrame::End => {
                frame_types.push("End".to_string());
                break;
            }
        }
    }

    assert!(commit_count >= 1, "should have at least one commit frame");
    assert!(has_ns_manifest, "should have a nameservice manifest");
    assert_eq!(
        frame_types.first().map(std::string::String::as_str),
        Some("Header"),
        "first frame should be Header"
    );
    assert_eq!(
        frame_types.last().map(std::string::String::as_str),
        Some("End"),
        "last frame should be End"
    );
}
