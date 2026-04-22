use fluree_db_binary_index::dict::branch::DictBranch;
use fluree_db_binary_index::dict::incremental::{
    build_incremental_string_packs, build_incremental_subject_packs_for_ns, update_reverse_tree,
};
use fluree_db_binary_index::dict::reverse_leaf::ReverseEntry;
use fluree_db_binary_index::dict::DictTreeReader;
use fluree_db_binary_index::format::branch::{
    build_branch_bytes, read_branch_from_bytes, BranchManifest, LeafEntry,
};
use fluree_db_binary_index::format::leaf::{decode_leaf_header_v3, LeafWriter};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::format::wire_helpers::PackBranchEntry;
use fluree_db_core::content_kind::{CODEC_FLUREE_DICT_BLOB, CODEC_FLUREE_INDEX_BRANCH};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::ContentId;
use fluree_db_indexer::run_index::incremental_branch::{update_branch, BranchUpdateConfig};
use std::collections::HashMap;

fn int_record_v2(g_id: u16, s_id: u64, p_id: u32, v: i64, t: u32) -> RunRecordV2 {
    use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;
    use fluree_db_core::value_id::ObjKey;
    RunRecordV2 {
        s_id: SubjectId::from_u64(s_id),
        o_key: ObjKey::encode_i64(v).as_u64(),
        p_id,
        t,
        o_i: LIST_INDEX_NONE,
        o_type: OType::XSD_INTEGER.as_u16(),
        g_id,
    }
}

#[test]
fn incremental_branch_only_fetches_and_rewrites_touched_leaves() {
    // Force many small leaves: 5 rows per leaflet, 25 rows per leaf.
    let mut writer = LeafWriter::new(RunSortOrder::Spot, 5, 25, 1);

    // Base data: enough subjects to span multiple leaf blobs.
    // LeafWriter here targets 25 rows per leaf, so use 60 rows to ensure 2+ leaves.
    let base: Vec<RunRecordV2> = (0..60)
        .map(|s| int_record_v2(0, s, 1, s as i64, 1))
        .collect();
    for r in &base {
        writer.push_record(*r).expect("push record");
    }
    let infos = writer.finish().expect("finish");
    assert!(
        infos.len() >= 2,
        "expected multiple leaf blobs (got {})",
        infos.len()
    );

    let mut leaf_bytes: HashMap<ContentId, Vec<u8>> = HashMap::new();
    let mut leaves: Vec<LeafEntry> = Vec::with_capacity(infos.len());
    for info in infos {
        leaf_bytes.insert(info.leaf_cid.clone(), info.leaf_bytes);
        leaves.push(LeafEntry {
            first_key: info.first_key,
            last_key: info.last_key,
            row_count: info.total_rows,
            leaf_cid: info.leaf_cid,
            sidecar_cid: None,
        });
    }

    // Encode+decode a branch manifest to ensure the routing bytes are valid.
    let branch_bytes = build_branch_bytes(RunSortOrder::Spot, 0, &leaves);
    let existing_branch = read_branch_from_bytes(&branch_bytes).expect("decode branch");

    // Choose a novelty record that falls squarely inside a middle leaf by subject id.
    let novelty = vec![int_record_v2(0, 30, 2, 999, 2)];
    let novelty_ops = vec![1u8]; // assert

    let config = BranchUpdateConfig {
        order: RunSortOrder::Spot,
        g_id: 0,
        zstd_level: 1,
        leaflet_target_rows: 50, // avoid splits in this test
        leaf_target_rows: 200,   // irrelevant unless split
    };

    let fetched: std::cell::RefCell<Vec<ContentId>> = std::cell::RefCell::new(Vec::new());
    let fetch_leaf = |cid: &ContentId| -> Result<Vec<u8>, std::io::Error> {
        fetched.borrow_mut().push(cid.clone());
        Ok(leaf_bytes.get(cid).expect("leaf cid present").clone())
    };
    let fetch_sidecar = |_cid: &ContentId| -> Result<Option<Vec<u8>>, std::io::Error> { Ok(None) };

    let update = update_branch(
        &branch_bytes,
        &novelty,
        &novelty_ops,
        &config,
        &fetch_leaf,
        &fetch_sidecar,
    )
    .expect("update_branch");

    // Invariant (a): only touched leaves are fetched from CAS.
    assert_eq!(
        fetched.borrow().len(),
        1,
        "expected only 1 leaf fetch for localized novelty"
    );
    assert_eq!(
        update.replaced_leaf_cids.len(),
        1,
        "expected exactly 1 replaced leaf CID"
    );

    // Invariant (a): untouched leaves keep their CIDs.
    let updated_branch: BranchManifest =
        read_branch_from_bytes(&update.branch_bytes).expect("decode updated branch");
    assert_eq!(updated_branch.leaves.len(), existing_branch.leaves.len());

    let replaced = &update.replaced_leaf_cids[0];
    for (i, (old, new)) in existing_branch
        .leaves
        .iter()
        .zip(updated_branch.leaves.iter())
        .enumerate()
    {
        if &old.leaf_cid == replaced {
            assert_ne!(old.leaf_cid, new.leaf_cid, "leaf {i} should be rewritten");
        } else {
            assert_eq!(
                old.leaf_cid, new.leaf_cid,
                "leaf {i} should be reused by CID"
            );
        }
    }

    // Basic correctness: the rewritten leaf bytes decode and include the novelty predicate (p=2).
    let replaced_idx = existing_branch
        .leaves
        .iter()
        .position(|e| &e.leaf_cid == replaced)
        .expect("replaced leaf cid should exist in original branch");
    let rewritten_leaf_cid = updated_branch.leaves[replaced_idx].leaf_cid.clone();
    let new_leaf = update
        .new_leaf_blobs
        .iter()
        .find(|b| b.info.leaf_cid == rewritten_leaf_cid)
        .unwrap_or_else(|| &update.new_leaf_blobs[0]);
    let hdr = decode_leaf_header_v3(&new_leaf.info.leaf_bytes).expect("read leaf header");
    assert!(hdr.total_rows >= 6, "expected row count to increase");

    // Sanity: branch CID matches the encoded bytes.
    let expected_branch_cid = ContentId::from_hex_digest(
        CODEC_FLUREE_INDEX_BRANCH,
        &fluree_db_core::sha256_hex(&update.branch_bytes),
    )
    .expect("valid sha");
    assert_eq!(update.branch_cid, expected_branch_cid);
}

#[test]
fn forward_dict_incremental_appends_new_packs_and_reuses_existing_refs() {
    // Existing routing table with two packs (fake CIDs are fine for this invariant).
    let existing = vec![
        PackBranchEntry {
            first_id: 0,
            last_id: 9,
            pack_cid: ContentId::from_hex_digest(
                CODEC_FLUREE_DICT_BLOB,
                &fluree_db_core::sha256_hex(b"pack0"),
            )
            .unwrap(),
        },
        PackBranchEntry {
            first_id: 10,
            last_id: 19,
            pack_cid: ContentId::from_hex_digest(
                CODEC_FLUREE_DICT_BLOB,
                &fluree_db_core::sha256_hex(b"pack1"),
            )
            .unwrap(),
        },
    ];

    // New entries above the watermark (append-only).
    let new_owned: Vec<(u32, Vec<u8>)> = (20..30)
        .map(|id| (id, format!("v{id}").into_bytes()))
        .collect();
    let new_refs: Vec<(u32, &[u8])> = new_owned
        .iter()
        .map(|(id, v)| (*id, v.as_slice()))
        .collect();

    let out =
        build_incremental_string_packs(&existing, &new_refs).expect("incremental string packs");

    assert_eq!(
        &out.all_pack_refs[..existing.len()],
        existing.as_slice(),
        "existing pack refs should be preserved unchanged"
    );
    assert!(
        out.all_pack_refs.len() > existing.len(),
        "expected new pack refs to be appended"
    );
    assert!(
        !out.new_packs.is_empty(),
        "expected at least one new pack artifact"
    );

    // Subject packs (single namespace) follow the same reuse+append invariant.
    let ns_code = 3u16;
    let subj_existing = existing.clone();
    let subj_new_owned: Vec<(u64, Vec<u8>)> = (20u64..30u64)
        .map(|id| (id, format!("s{id}").into_bytes()))
        .collect();
    let subj_new_refs: Vec<(u64, &[u8])> = subj_new_owned
        .iter()
        .map(|(id, v)| (*id, v.as_slice()))
        .collect();
    let subj_out = build_incremental_subject_packs_for_ns(ns_code, &subj_existing, &subj_new_refs)
        .expect("incremental subject packs");
    assert_eq!(
        &subj_out.all_pack_refs[..subj_existing.len()],
        subj_existing.as_slice(),
        "existing subject pack refs should be preserved unchanged"
    );
    assert!(
        subj_out.all_pack_refs.len() > subj_existing.len(),
        "expected new subject pack refs appended"
    );
}

#[test]
fn reverse_dict_incremental_only_rewrites_affected_leaf_and_lookups_still_work() {
    // Build a small reverse tree with many leaves by using a tiny target leaf size.
    let target_leaf_bytes = 200usize;

    let mut entries: Vec<ReverseEntry> = (0..200u64)
        .map(|i| ReverseEntry {
            key: format!("k{i:04}").into_bytes(),
            id: i,
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));

    let built = fluree_db_binary_index::dict::builder::build_reverse_tree(
        entries.clone(),
        target_leaf_bytes,
    )
    .expect("build_reverse_tree");
    let existing_branch: DictBranch = built.branch;

    // Leaf bytes by index (aligned with branch.leaves order).
    assert_eq!(
        built.leaves.len(),
        existing_branch.leaves.len(),
        "expected one artifact per branch leaf"
    );
    let existing_leaf_bytes_by_idx: Vec<Vec<u8>> =
        built.leaves.iter().map(|a| a.bytes.clone()).collect();

    // Choose a new key that routes to exactly one existing leaf.
    let new_key = b"k0100x".to_vec();
    let leaf_idx = existing_branch
        .find_leaf(&new_key)
        .expect("expected key to route to a leaf");

    let new_entries = vec![ReverseEntry {
        key: new_key.clone(),
        id: 9_999,
    }];

    let mut fetched: Vec<usize> = Vec::new();
    let mut fetch_leaf = |idx: usize| -> Result<Vec<u8>, std::io::Error> {
        fetched.push(idx);
        Ok(existing_leaf_bytes_by_idx[idx].clone())
    };

    let out = update_reverse_tree(
        &existing_branch,
        &new_entries,
        target_leaf_bytes,
        &mut fetch_leaf,
    )
    .expect("update_reverse_tree");

    // Invariant (c): only the affected leaf is fetched/rewritten.
    assert_eq!(fetched, vec![leaf_idx], "expected only one leaf fetch");
    assert_eq!(
        out.replaced_leaf_indices,
        vec![leaf_idx],
        "expected exactly the routed leaf to be replaced"
    );

    // Unchanged leaves keep their addresses; the replaced leaf gets a pending address.
    for (i, (old, new)) in existing_branch
        .leaves
        .iter()
        .zip(out.branch.leaves.iter())
        .enumerate()
    {
        if i == leaf_idx {
            assert_ne!(old.address, new.address);
            assert!(
                new.address.starts_with("pending:"),
                "expected pending address for new leaf"
            );
        } else {
            assert_eq!(old.address, new.address, "leaf {i} should be reused");
        }
    }

    // Query correctness: old + new keys resolve via DictTreeReader from memory.
    let mut leaves_by_address: HashMap<String, Vec<u8>> = HashMap::new();
    for (i, bl) in existing_branch.leaves.iter().enumerate() {
        leaves_by_address.insert(bl.address.clone(), existing_leaf_bytes_by_idx[i].clone());
    }
    for leaf_art in &out.new_leaves {
        let addr = format!("pending:{}", leaf_art.hash);
        leaves_by_address.insert(addr, leaf_art.bytes.clone());
    }

    let reader = DictTreeReader::from_memory(out.branch, leaves_by_address);
    // Existing key still works.
    assert_eq!(reader.reverse_lookup(b"k0000").unwrap(), Some(0));
    // New key works.
    assert_eq!(reader.reverse_lookup(&new_key).unwrap(), Some(9_999));
}
