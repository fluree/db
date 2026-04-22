//! Dictionary flat-file upload to CAS.
//!
//! `upload_dicts_from_disk` reads persisted flat dict files (subjects, strings,
//! numbig arenas, vector arenas) and uploads them to CAS as forward packs +
//! reverse trees.

use fluree_db_binary_index::{
    DictPackRefs, DictRefs, DictTreeRefs, PackBranchEntry, VectorDictRef,
};
use fluree_db_core::{ContentId, ContentStore};

use crate::error::{IndexerError, Result};
use crate::run_index;

use super::types::UploadedDicts;
use super::upload::upload_dict_file;

/// Output of a flushed reverse-index leaf: `(leaf_bytes, first_key, last_key, entry_count)`.
type FlushedLeaf = (Vec<u8>, Vec<u8>, Vec<u8>, u32);

fn flush_reverse_leaf<F>(
    leaf_offsets: &mut Vec<u32>,
    leaf_data: &mut Vec<u8>,
    first_key: &mut Option<Vec<u8>>,
    chunk_bytes: &mut usize,
    mut last_key: F,
) -> Option<FlushedLeaf>
where
    F: FnMut() -> Vec<u8>,
{
    if leaf_offsets.is_empty() {
        return None;
    }
    let entry_count = leaf_offsets.len() as u32;
    let header_size = 8;
    let offset_table_size = leaf_offsets.len() * 4;
    let total = header_size + offset_table_size + leaf_data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&fluree_db_binary_index::dict::reverse_leaf::REVERSE_LEAF_MAGIC);
    buf.extend_from_slice(&entry_count.to_le_bytes());
    for off in leaf_offsets.iter() {
        buf.extend_from_slice(&off.to_le_bytes());
    }
    buf.extend_from_slice(leaf_data);
    debug_assert_eq!(buf.len(), total);

    let fk = first_key.take().unwrap_or_default();
    let lk = last_key();

    leaf_offsets.clear();
    leaf_data.clear();
    *chunk_bytes = 0;

    Some((buf, fk, lk, entry_count))
}

fn build_forward_pack_artifact(
    entries: &[(u64, &[u8])],
    kind: u8,
    ns_code: u16,
    target_page_bytes: usize,
) -> std::io::Result<(Vec<u8>, u64, u64)> {
    if entries.is_empty() {
        return Err(std::io::Error::other("cannot build empty forward pack"));
    }
    let bytes = fluree_db_binary_index::dict::forward_pack::encode_forward_pack(
        entries,
        kind,
        ns_code,
        target_page_bytes,
    )?;
    Ok((bytes, entries[0].0, entries.last().expect("non-empty").0))
}

///
/// Reads flat files written by `GlobalDicts::persist()` and builds CoW trees
/// for subject/string dicts. Does NOT require `GlobalDicts` in memory.
///
/// Required files in `run_dir`:
///   - `subjects.fwd`, `subjects.idx`, `subjects.sids`
///   - `strings.fwd`, `strings.idx`
///   - `graphs.dict`, `datatypes.dict`, `languages.dict`
///   - `numbig/p_*.nba` (zero or more)
///
/// Watermark derivation from `subjects.sids`:
///   - Decode each sid64 via `SubjectId::from_u64` → `(ns_code, local_id)`
///   - `subject_watermarks[ns_code]` = max local_id for that ns_code
///   - Overflow ns_code (0xFFFF): always wide, watermark = 0
///   - `needs_wide` = any local_id exceeds `u16::MAX`
///   - `string_watermark` = string entry count − 1 (IDs are 0..=N contiguous)
#[allow(clippy::type_complexity)]
pub async fn upload_dicts_from_disk(
    content_store: &dyn ContentStore,
    run_dir: &std::path::Path,
    namespace_codes: &std::collections::HashMap<u16, String>,
    trust_sorted_order_invariants: bool,
) -> Result<UploadedDicts> {
    use fluree_db_binary_index::dict::branch::{BranchLeafEntry, DictBranch};
    use fluree_db_binary_index::dict::builder;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::{ContentKind, DictKind, SubjectIdEncoding};
    use std::collections::BTreeMap;

    // ---- 1. Read small dicts for v4 root inlining ----
    tracing::info!("reading small dictionaries for v4 root (graphs, datatypes, languages)");

    let graphs_path = run_dir.join("graphs.dict");
    let graphs_dict = run_index::dict_io::read_predicate_dict(&graphs_path)
        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", graphs_path.display(), e)))?;
    let graph_iris: Vec<String> = (0..graphs_dict.len())
        .filter_map(|id| {
            graphs_dict
                .resolve(id)
                .map(std::string::ToString::to_string)
        })
        .collect();

    let datatypes_path = run_dir.join("datatypes.dict");
    let datatypes_dict = run_index::dict_io::read_predicate_dict(&datatypes_path).map_err(|e| {
        IndexerError::StorageRead(format!("read {}: {}", datatypes_path.display(), e))
    })?;
    let datatype_iris: Vec<String> = (0..datatypes_dict.len())
        .filter_map(|id| {
            datatypes_dict
                .resolve(id)
                .map(std::string::ToString::to_string)
        })
        .collect();

    let languages_path = run_dir.join("languages.dict");
    let language_tags = if languages_path.exists() {
        let lang_dict = run_index::dict_io::read_language_dict(&languages_path).map_err(|e| {
            IndexerError::StorageRead(format!("read {}: {}", languages_path.display(), e))
        })?;
        let mut tags: Vec<(u16, String)> = lang_dict
            .iter()
            .map(|(id, tag)| (id, tag.to_string()))
            .collect();
        tags.sort_unstable_by_key(|(id, _)| *id);
        tags.into_iter().map(|(_, tag)| tag).collect()
    } else {
        Vec::new()
    };

    // ---- 2. Read sids (needed for both subject trees and watermark computation) ----
    let sids_path = run_dir.join("subjects.sids");
    let sids: Vec<u64> = run_index::dict_io::read_subject_sid_map(&sids_path)
        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", sids_path.display(), e)))?;

    // ---- 3. Upload subject trees, string trees, numbig, vectors in parallel ----
    tracing::info!(
        "uploading subject trees, string trees, numbig arenas, and vector arenas in parallel"
    );
    if trust_sorted_order_invariants {
        tracing::info!(
            "trusting bulk-import sorted-order invariants for subject/string dict reverse trees"
        );
    }

    let (subject_trees, string_result, numbig, vectors) = tokio::try_join!(
        // Task A: Subject forward + reverse trees
        async {
            let subj_idx_path = run_dir.join("subjects.idx");
            let (subj_offsets, subj_lens) = run_index::dict_io::read_forward_index(&subj_idx_path)
                .map_err(|e| {
                    IndexerError::StorageRead(format!("read {}: {}", subj_idx_path.display(), e))
                })?;
            let subj_fwd_path = run_dir.join("subjects.fwd");
            let subj_fwd_file = std::fs::File::open(&subj_fwd_path).map_err(|e| {
                IndexerError::StorageRead(format!("open {}: {}", subj_fwd_path.display(), e))
            })?;
            // SAFETY: The file is opened read-only and is not concurrently modified.
            // The forward-dict file is an immutable index artifact written before this point.
            let subj_fwd_data = unsafe { memmap2::Mmap::map(&subj_fwd_file) }.map_err(|e| {
                IndexerError::StorageRead(format!("mmap {}: {}", subj_fwd_path.display(), e))
            })?;
            tracing::info!(
                subjects = sids.len(),
                fwd_bytes = subj_fwd_data.len(),
                "subject dict files loaded"
            );

            // Precompute suffix ranges so we can sort/build trees without allocating per-entry Vecs.
            let mut ns_codes: Vec<u16> = Vec::with_capacity(sids.len());
            let mut suf_offs: Vec<u64> = Vec::with_capacity(sids.len());
            let mut suf_lens: Vec<u32> = Vec::with_capacity(sids.len());
            for (&sid, (&off, &len)) in sids.iter().zip(subj_offsets.iter().zip(subj_lens.iter())) {
                let ns_code = SubjectId::from_u64(sid).ns_code();
                let iri = &subj_fwd_data[off as usize..(off as usize + len as usize)];
                let prefix_bytes = namespace_codes
                    .get(&ns_code)
                    .map(std::string::String::as_bytes)
                    .unwrap_or(b"");
                if !prefix_bytes.is_empty() && iri.starts_with(prefix_bytes) {
                    ns_codes.push(ns_code);
                    suf_offs.push(off + prefix_bytes.len() as u64);
                    suf_lens.push(len.saturating_sub(prefix_bytes.len() as u32));
                } else {
                    ns_codes.push(ns_code);
                    suf_offs.push(off);
                    suf_lens.push(len);
                }
            }

            // Pass 1: forward packs (FPK1 format, one stream per namespace)
            tracing::info!("building subject forward packs");
            let sids_sorted = if trust_sorted_order_invariants {
                true
            } else {
                sids.windows(2).all(|w| w[0] <= w[1])
            };

            let id_order: Option<Vec<usize>> = if sids_sorted {
                None
            } else {
                let mut v: Vec<usize> = (0..sids.len()).collect();
                v.sort_unstable_by_key(|&i| sids[i]);
                Some(v)
            };
            let subject_fwd_pack_refs = {
                use fluree_db_binary_index::dict::forward_pack::KIND_SUBJECT_FWD;
                use fluree_db_binary_index::dict::pack_builder::{
                    DEFAULT_TARGET_PACK_BYTES, DEFAULT_TARGET_PAGE_BYTES,
                };

                let kind = ContentKind::DictBlob {
                    dict: DictKind::SubjectForward,
                };
                let iter_fn = |i: usize| {
                    let sid = sids[i];
                    let off = suf_offs[i] as usize;
                    let len = suf_lens[i] as usize;
                    let sid = SubjectId::from_u64(sid);
                    let ns_code = sid.ns_code();
                    let local_id = sid.local_id();
                    let suffix = &subj_fwd_data[off..off + len];
                    (ns_code, local_id, suffix)
                };

                let mut subject_fwd_ns_packs: Vec<(u16, Vec<PackBranchEntry>)> = Vec::new();
                let mut current_ns: Option<u16> = None;
                let mut current_pack_refs: Vec<PackBranchEntry> = Vec::new();
                let mut current_entries: Vec<(u64, &[u8])> = Vec::new();
                let mut current_pack_est = 0usize;

                match &id_order {
                    None => {
                        for i in 0..sids.len() {
                            let (ns_code, local_id, suffix) = iter_fn(i);

                            if current_ns != Some(ns_code) {
                                if let Some(prev_ns) = current_ns {
                                    if !current_entries.is_empty() {
                                        let (bytes, first_id, last_id) =
                                            build_forward_pack_artifact(
                                                &current_entries,
                                                KIND_SUBJECT_FWD,
                                                prev_ns,
                                                DEFAULT_TARGET_PAGE_BYTES,
                                            )
                                            .map_err(
                                                |e| {
                                                    IndexerError::StorageWrite(format!(
                                                        "subject pack build: {e}"
                                                    ))
                                                },
                                            )?;
                                        let cas_result =
                                            content_store.put(kind, &bytes).await.map_err(|e| {
                                                IndexerError::StorageWrite(e.to_string())
                                            })?;
                                        current_pack_refs.push(PackBranchEntry {
                                            first_id,
                                            last_id,
                                            pack_cid: cas_result,
                                        });
                                        current_entries.clear();
                                        current_pack_est = 0;
                                    }
                                    subject_fwd_ns_packs
                                        .push((prev_ns, std::mem::take(&mut current_pack_refs)));
                                }
                                current_ns = Some(ns_code);
                            }

                            current_pack_est += suffix.len() + 4;
                            current_entries.push((local_id, suffix));

                            if current_pack_est >= DEFAULT_TARGET_PACK_BYTES {
                                let (bytes, first_id, last_id) = build_forward_pack_artifact(
                                    &current_entries,
                                    KIND_SUBJECT_FWD,
                                    ns_code,
                                    DEFAULT_TARGET_PAGE_BYTES,
                                )
                                .map_err(|e| {
                                    IndexerError::StorageWrite(format!("subject pack build: {e}"))
                                })?;
                                let cas_result = content_store
                                    .put(kind, &bytes)
                                    .await
                                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                                current_pack_refs.push(PackBranchEntry {
                                    first_id,
                                    last_id,
                                    pack_cid: cas_result,
                                });
                                current_entries.clear();
                                current_pack_est = 0;
                            }
                        }
                    }
                    Some(order) => {
                        for &i in order {
                            let (ns_code, local_id, suffix) = iter_fn(i);

                            if current_ns != Some(ns_code) {
                                if let Some(prev_ns) = current_ns {
                                    if !current_entries.is_empty() {
                                        let (bytes, first_id, last_id) =
                                            build_forward_pack_artifact(
                                                &current_entries,
                                                KIND_SUBJECT_FWD,
                                                prev_ns,
                                                DEFAULT_TARGET_PAGE_BYTES,
                                            )
                                            .map_err(
                                                |e| {
                                                    IndexerError::StorageWrite(format!(
                                                        "subject pack build: {e}"
                                                    ))
                                                },
                                            )?;
                                        let cas_result =
                                            content_store.put(kind, &bytes).await.map_err(|e| {
                                                IndexerError::StorageWrite(e.to_string())
                                            })?;
                                        current_pack_refs.push(PackBranchEntry {
                                            first_id,
                                            last_id,
                                            pack_cid: cas_result,
                                        });
                                        current_entries.clear();
                                        current_pack_est = 0;
                                    }
                                    subject_fwd_ns_packs
                                        .push((prev_ns, std::mem::take(&mut current_pack_refs)));
                                }
                                current_ns = Some(ns_code);
                            }

                            current_pack_est += suffix.len() + 4;
                            current_entries.push((local_id, suffix));

                            if current_pack_est >= DEFAULT_TARGET_PACK_BYTES {
                                let (bytes, first_id, last_id) = build_forward_pack_artifact(
                                    &current_entries,
                                    KIND_SUBJECT_FWD,
                                    ns_code,
                                    DEFAULT_TARGET_PAGE_BYTES,
                                )
                                .map_err(|e| {
                                    IndexerError::StorageWrite(format!("subject pack build: {e}"))
                                })?;
                                let cas_result = content_store
                                    .put(kind, &bytes)
                                    .await
                                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                                current_pack_refs.push(PackBranchEntry {
                                    first_id,
                                    last_id,
                                    pack_cid: cas_result,
                                });
                                current_entries.clear();
                                current_pack_est = 0;
                            }
                        }
                    }
                }

                if let Some(ns_code) = current_ns {
                    if !current_entries.is_empty() {
                        let (bytes, first_id, last_id) = build_forward_pack_artifact(
                            &current_entries,
                            KIND_SUBJECT_FWD,
                            ns_code,
                            DEFAULT_TARGET_PAGE_BYTES,
                        )
                        .map_err(|e| {
                            IndexerError::StorageWrite(format!("subject pack build: {e}"))
                        })?;
                        let cas_result = content_store
                            .put(kind, &bytes)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        current_pack_refs.push(PackBranchEntry {
                            first_id,
                            last_id,
                            pack_cid: cas_result,
                        });
                    }
                    subject_fwd_ns_packs.push((ns_code, current_pack_refs));
                }

                subject_fwd_ns_packs
            };
            // Pass 2: reverse tree
            // Fast path: when subjects were produced by vocab-merge, the file order is already
            // sorted by `(ns_code, suffix)` (which matches the reverse-tree key ordering).
            // Avoid sorting indices by comparing huge byte slices.
            let keys_sorted = if trust_sorted_order_invariants {
                true
            } else {
                let mut ok = true;
                for i in 1..sids.len() {
                    let prev_ns = ns_codes[i - 1];
                    let curr_ns = ns_codes[i];
                    if prev_ns < curr_ns {
                        continue;
                    }
                    if prev_ns > curr_ns {
                        ok = false;
                        break;
                    }
                    let a = &subj_fwd_data[suf_offs[i - 1] as usize
                        ..(suf_offs[i - 1] as usize + suf_lens[i - 1] as usize)];
                    let b = &subj_fwd_data
                        [suf_offs[i] as usize..(suf_offs[i] as usize + suf_lens[i] as usize)];
                    if a > b {
                        ok = false;
                        break;
                    }
                }
                ok
            };
            let rev_order: Option<Vec<usize>> = if keys_sorted {
                None
            } else {
                tracing::info!("building subject reverse tree (fallback index-sort)");
                let mut v: Vec<usize> = (0..sids.len()).collect();
                v.sort_unstable_by(|&a, &b| {
                    let na = ns_codes[a];
                    let nb = ns_codes[b];
                    match na.cmp(&nb) {
                        std::cmp::Ordering::Equal => {
                            let sa = &subj_fwd_data[suf_offs[a] as usize
                                ..(suf_offs[a] as usize + suf_lens[a] as usize)];
                            let sb = &subj_fwd_data[suf_offs[b] as usize
                                ..(suf_offs[b] as usize + suf_lens[b] as usize)];
                            sa.cmp(sb)
                        }
                        other => other,
                    }
                });
                Some(v)
            };
            let subject_reverse = {
                let kind = ContentKind::DictBlob {
                    dict: DictKind::SubjectReverse,
                };
                let mut leaf_cids: Vec<ContentId> = Vec::new();
                let mut branch_entries: Vec<BranchLeafEntry> = Vec::new();

                let mut leaf_offsets: Vec<u32> = Vec::new();
                let mut leaf_data: Vec<u8> = Vec::new();
                let mut chunk_bytes: usize = 0;
                let mut first_key: Option<Vec<u8>> = None;
                // Track last key parts for branch boundary (avoid per-entry allocation).
                let mut last_ns: u16 = 0;
                let mut last_off: u64 = 0;
                let mut last_len: u32 = 0;

                match &rev_order {
                    None => {
                        for i in 0..sids.len() {
                            let ns = ns_codes[i];
                            let suffix = &subj_fwd_data[suf_offs[i] as usize
                                ..(suf_offs[i] as usize + suf_lens[i] as usize)];
                            let sid = sids[i];

                            // Key = [ns_code BE][suffix]
                            let key_len = 2 + suffix.len();
                            let entry_size = 12 + key_len;
                            leaf_offsets.push(chunk_bytes as u32);
                            chunk_bytes += entry_size;

                            leaf_data.extend_from_slice(&(key_len as u32).to_le_bytes());
                            leaf_data.extend_from_slice(&ns.to_be_bytes());
                            leaf_data.extend_from_slice(suffix);
                            leaf_data.extend_from_slice(&sid.to_le_bytes());

                            if first_key.is_none() {
                                let mut k = Vec::with_capacity(key_len);
                                k.extend_from_slice(&ns.to_be_bytes());
                                k.extend_from_slice(suffix);
                                first_key = Some(k);
                            }
                            last_ns = ns;
                            last_off = suf_offs[i];
                            last_len = suf_lens[i];

                            if chunk_bytes >= builder::DEFAULT_TARGET_LEAF_BYTES {
                                if let Some((leaf_bytes, fk, lk, entry_count)) = flush_reverse_leaf(
                                    &mut leaf_offsets,
                                    &mut leaf_data,
                                    &mut first_key,
                                    &mut chunk_bytes,
                                    || {
                                        let suffix = &subj_fwd_data[last_off as usize
                                            ..(last_off as usize + last_len as usize)];
                                        let mut lk = Vec::with_capacity(2 + suffix.len());
                                        lk.extend_from_slice(&last_ns.to_be_bytes());
                                        lk.extend_from_slice(suffix);
                                        lk
                                    },
                                ) {
                                    let cas_result = content_store
                                        .put(kind, &leaf_bytes)
                                        .await
                                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                                    let address = cas_result.to_string();
                                    leaf_cids.push(cas_result);
                                    branch_entries.push(BranchLeafEntry {
                                        first_key: fk,
                                        last_key: lk,
                                        entry_count,
                                        address,
                                    });
                                }
                            }
                        }
                    }
                    Some(order) => {
                        for &i in order {
                            let ns = ns_codes[i];
                            let suffix = &subj_fwd_data[suf_offs[i] as usize
                                ..(suf_offs[i] as usize + suf_lens[i] as usize)];
                            let sid = sids[i];

                            // Key = [ns_code BE][suffix]
                            let key_len = 2 + suffix.len();
                            let entry_size = 12 + key_len;
                            leaf_offsets.push(chunk_bytes as u32);
                            chunk_bytes += entry_size;

                            leaf_data.extend_from_slice(&(key_len as u32).to_le_bytes());
                            leaf_data.extend_from_slice(&ns.to_be_bytes());
                            leaf_data.extend_from_slice(suffix);
                            leaf_data.extend_from_slice(&sid.to_le_bytes());

                            if first_key.is_none() {
                                let mut k = Vec::with_capacity(key_len);
                                k.extend_from_slice(&ns.to_be_bytes());
                                k.extend_from_slice(suffix);
                                first_key = Some(k);
                            }
                            last_ns = ns;
                            last_off = suf_offs[i];
                            last_len = suf_lens[i];

                            if chunk_bytes >= builder::DEFAULT_TARGET_LEAF_BYTES {
                                if let Some((leaf_bytes, fk, lk, entry_count)) = flush_reverse_leaf(
                                    &mut leaf_offsets,
                                    &mut leaf_data,
                                    &mut first_key,
                                    &mut chunk_bytes,
                                    || {
                                        let suffix = &subj_fwd_data[last_off as usize
                                            ..(last_off as usize + last_len as usize)];
                                        let mut lk = Vec::with_capacity(2 + suffix.len());
                                        lk.extend_from_slice(&last_ns.to_be_bytes());
                                        lk.extend_from_slice(suffix);
                                        lk
                                    },
                                ) {
                                    let cas_result = content_store
                                        .put(kind, &leaf_bytes)
                                        .await
                                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                                    let address = cas_result.to_string();
                                    leaf_cids.push(cas_result);
                                    branch_entries.push(BranchLeafEntry {
                                        first_key: fk,
                                        last_key: lk,
                                        entry_count,
                                        address,
                                    });
                                }
                            }
                        }
                    }
                }

                if let Some((leaf_bytes, fk, lk, entry_count)) = flush_reverse_leaf(
                    &mut leaf_offsets,
                    &mut leaf_data,
                    &mut first_key,
                    &mut chunk_bytes,
                    || {
                        let suffix = &subj_fwd_data
                            [last_off as usize..(last_off as usize + last_len as usize)];
                        let mut lk = Vec::with_capacity(2 + suffix.len());
                        lk.extend_from_slice(&last_ns.to_be_bytes());
                        lk.extend_from_slice(suffix);
                        lk
                    },
                ) {
                    let cas_result = content_store
                        .put(kind, &leaf_bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    let address = cas_result.to_string();
                    leaf_cids.push(cas_result);
                    branch_entries.push(BranchLeafEntry {
                        first_key: fk,
                        last_key: lk,
                        entry_count,
                        address,
                    });
                }

                let branch = DictBranch {
                    leaves: branch_entries,
                };
                let branch_bytes = branch.encode();
                let branch_result = content_store
                    .put(kind, &branch_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                Ok::<_, IndexerError>(DictTreeRefs {
                    branch: branch_result,
                    leaves: leaf_cids,
                })?
            };

            tracing::info!("subject dict artifacts uploaded");
            Ok::<_, IndexerError>((subject_fwd_pack_refs, subject_reverse))
        },
        // Task B: String forward + reverse trees
        async {
            let str_idx_path = run_dir.join("strings.idx");
            let str_fwd_path = run_dir.join("strings.fwd");
            if str_idx_path.exists() && str_fwd_path.exists() {
                let (str_offsets, str_lens) = run_index::dict_io::read_forward_index(&str_idx_path)
                    .map_err(|e| {
                        IndexerError::StorageRead(format!("read {}: {}", str_idx_path.display(), e))
                    })?;
                let str_fwd_file = std::fs::File::open(&str_fwd_path).map_err(|e| {
                    IndexerError::StorageRead(format!("open {}: {}", str_fwd_path.display(), e))
                })?;
                // SAFETY: The file is opened read-only and is not concurrently modified.
                // The forward-dict file is an immutable index artifact written before this point.
                let str_fwd_data = unsafe { memmap2::Mmap::map(&str_fwd_file) }.map_err(|e| {
                    IndexerError::StorageRead(format!("mmap {}: {}", str_fwd_path.display(), e))
                })?;
                let count = str_offsets.len();
                tracing::info!(
                    strings = count,
                    fwd_bytes = str_fwd_data.len(),
                    "string dict files loaded"
                );

                // Pass 1: forward packs (FPK1 format)
                tracing::info!("building string forward packs");
                // IDs are 0..count contiguous and already in order of the forward file.
                let string_fwd_pack_refs = {
                    use fluree_db_binary_index::dict::forward_pack::KIND_STRING_FWD;
                    use fluree_db_binary_index::dict::pack_builder::{
                        DEFAULT_TARGET_PACK_BYTES, DEFAULT_TARGET_PAGE_BYTES,
                    };

                    let kind = ContentKind::DictBlob {
                        dict: DictKind::StringForward,
                    };
                    let mut pack_refs = Vec::new();
                    let mut entries: Vec<(u64, &[u8])> = Vec::new();
                    let mut pack_est = 0usize;

                    for i in 0..count {
                        let off = str_offsets[i] as usize;
                        let len = str_lens[i] as usize;
                        pack_est += len + 4;
                        entries.push((i as u64, &str_fwd_data[off..off + len]));

                        if pack_est >= DEFAULT_TARGET_PACK_BYTES {
                            let (bytes, first_id, last_id) = build_forward_pack_artifact(
                                &entries,
                                KIND_STRING_FWD,
                                0,
                                DEFAULT_TARGET_PAGE_BYTES,
                            )
                            .map_err(|e| {
                                IndexerError::StorageWrite(format!("string pack build: {e}"))
                            })?;
                            let cas_result = content_store
                                .put(kind, &bytes)
                                .await
                                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                            pack_refs.push(PackBranchEntry {
                                first_id,
                                last_id,
                                pack_cid: cas_result,
                            });
                            entries.clear();
                            pack_est = 0;
                        }
                    }

                    if !entries.is_empty() {
                        let (bytes, first_id, last_id) = build_forward_pack_artifact(
                            &entries,
                            KIND_STRING_FWD,
                            0,
                            DEFAULT_TARGET_PAGE_BYTES,
                        )
                        .map_err(|e| {
                            IndexerError::StorageWrite(format!("string pack build: {e}"))
                        })?;
                        let cas_result = content_store
                            .put(kind, &bytes)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        pack_refs.push(PackBranchEntry {
                            first_id,
                            last_id,
                            pack_cid: cas_result,
                        });
                    }

                    pack_refs
                };
                // Pass 2: reverse tree
                // Fast path: when strings were produced by vocab-merge, IDs are assigned
                // in lexicographic key order, so the forward file order is already the
                // correct reverse-tree key order. Avoid sorting indices (O(n log n)).
                let strings_sorted = if trust_sorted_order_invariants {
                    true
                } else {
                    let mut ok = true;
                    for i in 1..count {
                        let oa = str_offsets[i - 1] as usize;
                        let la = str_lens[i - 1] as usize;
                        let ob = str_offsets[i] as usize;
                        let lb = str_lens[i] as usize;
                        if str_fwd_data[oa..oa + la] > str_fwd_data[ob..ob + lb] {
                            ok = false;
                            break;
                        }
                    }
                    ok
                };
                let rev_order: Option<Vec<usize>> = if strings_sorted {
                    None
                } else {
                    tracing::info!("building string reverse tree (fallback index-sort)");
                    let mut v: Vec<usize> = (0..count).collect();
                    v.sort_unstable_by(|&a, &b| {
                        let oa = str_offsets[a] as usize;
                        let la = str_lens[a] as usize;
                        let ob = str_offsets[b] as usize;
                        let lb = str_lens[b] as usize;
                        str_fwd_data[oa..oa + la].cmp(&str_fwd_data[ob..ob + lb])
                    });
                    Some(v)
                };

                let sr = {
                    let kind = ContentKind::DictBlob {
                        dict: DictKind::StringReverse,
                    };
                    let mut leaf_cids: Vec<ContentId> = Vec::new();
                    let mut branch_entries: Vec<BranchLeafEntry> = Vec::new();

                    let mut leaf_offsets: Vec<u32> = Vec::new();
                    let mut leaf_data: Vec<u8> = Vec::new();
                    let mut chunk_bytes: usize = 0;
                    let mut first_key: Option<Vec<u8>> = None;
                    // Track last key slice for boundary without cloning per entry.
                    let mut last_off: usize = 0;
                    let mut last_len: usize = 0;

                    match &rev_order {
                        None => {
                            for i in 0..count {
                                let off = str_offsets[i] as usize;
                                let len = str_lens[i] as usize;
                                let key = &str_fwd_data[off..off + len];
                                let id = i as u64;

                                let entry_size = 12 + key.len();
                                leaf_offsets.push(chunk_bytes as u32);
                                chunk_bytes += entry_size;

                                leaf_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                                leaf_data.extend_from_slice(key);
                                leaf_data.extend_from_slice(&id.to_le_bytes());

                                if first_key.is_none() {
                                    first_key = Some(key.to_vec());
                                }
                                last_off = off;
                                last_len = len;

                                if chunk_bytes >= builder::DEFAULT_TARGET_LEAF_BYTES {
                                    if let Some((leaf_bytes, fk, lk, entry_count)) =
                                        flush_reverse_leaf(
                                            &mut leaf_offsets,
                                            &mut leaf_data,
                                            &mut first_key,
                                            &mut chunk_bytes,
                                            || {
                                                str_fwd_data[last_off..(last_off + last_len)]
                                                    .to_vec()
                                            },
                                        )
                                    {
                                        let cas_result =
                                            content_store.put(kind, &leaf_bytes).await.map_err(
                                                |e| IndexerError::StorageWrite(e.to_string()),
                                            )?;
                                        let address = cas_result.to_string();
                                        leaf_cids.push(cas_result);
                                        branch_entries.push(BranchLeafEntry {
                                            first_key: fk,
                                            last_key: lk,
                                            entry_count,
                                            address,
                                        });
                                    }
                                }
                            }
                        }
                        Some(order) => {
                            for &i in order {
                                let off = str_offsets[i] as usize;
                                let len = str_lens[i] as usize;
                                let key = &str_fwd_data[off..off + len];
                                let id = i as u64;

                                let entry_size = 12 + key.len();
                                leaf_offsets.push(chunk_bytes as u32);
                                chunk_bytes += entry_size;

                                leaf_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                                leaf_data.extend_from_slice(key);
                                leaf_data.extend_from_slice(&id.to_le_bytes());

                                if first_key.is_none() {
                                    first_key = Some(key.to_vec());
                                }
                                last_off = off;
                                last_len = len;

                                if chunk_bytes >= builder::DEFAULT_TARGET_LEAF_BYTES {
                                    if let Some((leaf_bytes, fk, lk, entry_count)) =
                                        flush_reverse_leaf(
                                            &mut leaf_offsets,
                                            &mut leaf_data,
                                            &mut first_key,
                                            &mut chunk_bytes,
                                            || {
                                                str_fwd_data[last_off..(last_off + last_len)]
                                                    .to_vec()
                                            },
                                        )
                                    {
                                        let cas_result =
                                            content_store.put(kind, &leaf_bytes).await.map_err(
                                                |e| IndexerError::StorageWrite(e.to_string()),
                                            )?;
                                        let address = cas_result.to_string();
                                        leaf_cids.push(cas_result);
                                        branch_entries.push(BranchLeafEntry {
                                            first_key: fk,
                                            last_key: lk,
                                            entry_count,
                                            address,
                                        });
                                    }
                                }
                            }
                        }
                    }

                    if let Some((leaf_bytes, fk, lk, entry_count)) = flush_reverse_leaf(
                        &mut leaf_offsets,
                        &mut leaf_data,
                        &mut first_key,
                        &mut chunk_bytes,
                        || str_fwd_data[last_off..(last_off + last_len)].to_vec(),
                    ) {
                        let cas_result = content_store
                            .put(kind, &leaf_bytes)
                            .await
                            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                        let address = cas_result.to_string();
                        leaf_cids.push(cas_result);
                        branch_entries.push(BranchLeafEntry {
                            first_key: fk,
                            last_key: lk,
                            entry_count,
                            address,
                        });
                    }

                    let branch = DictBranch {
                        leaves: branch_entries,
                    };
                    let branch_bytes = branch.encode();
                    let branch_result = content_store
                        .put(kind, &branch_bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

                    Ok::<_, IndexerError>(DictTreeRefs {
                        branch: branch_result,
                        leaves: leaf_cids,
                    })?
                };

                tracing::info!("string dict artifacts uploaded");
                Ok::<_, IndexerError>((count, string_fwd_pack_refs, sr))
            } else {
                // No strings persisted — empty packs + empty reverse tree
                let kind_rev = ContentKind::DictBlob {
                    dict: DictKind::StringReverse,
                };
                let empty_branch = DictBranch { leaves: vec![] };
                let empty_bytes = empty_branch.encode();
                let wr_rev = content_store
                    .put(kind_rev, &empty_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                Ok((
                    0,
                    vec![], // no forward packs
                    DictTreeRefs {
                        branch: wr_rev,
                        leaves: vec![],
                    },
                ))
            }
        },
        // Task C: Numbig arenas (per-graph subdirectories)
        async {
            let mut numbig: BTreeMap<String, BTreeMap<String, ContentId>> = BTreeMap::new();
            // Scan for g_{id}/numbig/ subdirectories
            for dir_entry in std::fs::read_dir(run_dir)
                .map_err(|e| IndexerError::StorageRead(format!("read run_dir: {e}")))?
            {
                let dir_entry = dir_entry
                    .map_err(|e| IndexerError::StorageRead(format!("read run_dir entry: {e}")))?;
                let dir_name = dir_entry.file_name();
                let dir_name_str = dir_name.to_string_lossy();
                if let Some(g_id_str) = dir_name_str.strip_prefix("g_") {
                    let nb_dir = dir_entry.path().join("numbig");
                    if nb_dir.exists() {
                        let mut per_pred = BTreeMap::new();
                        for entry in std::fs::read_dir(&nb_dir).map_err(|e| {
                            IndexerError::StorageRead(format!("read numbig dir: {e}"))
                        })? {
                            let entry = entry.map_err(|e| {
                                IndexerError::StorageRead(format!("read numbig entry: {e}"))
                            })?;
                            let name = entry.file_name();
                            let name_str = name.to_string_lossy();
                            if let Some(rest) = name_str.strip_prefix("p_") {
                                if let Some(id_str) = rest.strip_suffix(".nba") {
                                    if let Ok(p_id) = id_str.parse::<u32>() {
                                        let cid = upload_dict_file(
                                            content_store,
                                            &entry.path(),
                                            DictKind::NumBig { p_id },
                                            "dict artifact uploaded to CAS (from disk)",
                                        )
                                        .await?;
                                        per_pred.insert(p_id.to_string(), cid);
                                    }
                                }
                            }
                        }
                        if !per_pred.is_empty() {
                            numbig.insert(g_id_str.to_string(), per_pred);
                        }
                    }
                }
            }
            Ok::<_, IndexerError>(numbig)
        },
        // Task D: Vector arenas (per-graph subdirectories)
        async {
            let mut vectors: BTreeMap<String, BTreeMap<String, VectorDictRef>> = BTreeMap::new();
            // Scan for g_{id}/vectors/ subdirectories
            for dir_entry in std::fs::read_dir(run_dir)
                .map_err(|e| IndexerError::StorageRead(format!("read run_dir: {e}")))?
            {
                let dir_entry = dir_entry
                    .map_err(|e| IndexerError::StorageRead(format!("read run_dir entry: {e}")))?;
                let dir_name = dir_entry.file_name();
                let dir_name_str = dir_name.to_string_lossy();
                if let Some(g_id_str) = dir_name_str.strip_prefix("g_") {
                    let vec_dir = dir_entry.path().join("vectors");
                    if vec_dir.exists() {
                        let mut per_pred = BTreeMap::new();
                        for entry in std::fs::read_dir(&vec_dir).map_err(|e| {
                            IndexerError::StorageRead(format!("read vectors dir: {e}"))
                        })? {
                            let entry = entry.map_err(|e| {
                                IndexerError::StorageRead(format!("read vectors entry: {e}"))
                            })?;
                            let name = entry.file_name();
                            let name_str = name.to_string_lossy();
                            if let Some(rest) = name_str.strip_prefix("p_") {
                                if let Some(id_str) = rest.strip_suffix(".vam") {
                                    if let Ok(p_id) = id_str.parse::<u32>() {
                                        let manifest_bytes =
                                            tokio::fs::read(entry.path()).await.map_err(|e| {
                                                IndexerError::StorageRead(format!(
                                                    "read vector manifest: {e}"
                                                ))
                                            })?;
                                        let manifest =
                                            fluree_db_binary_index::arena::vector::read_vector_manifest(
                                                &manifest_bytes,
                                            )
                                            .map_err(
                                                |e| {
                                                    IndexerError::StorageRead(format!(
                                                        "parse vector manifest: {e}"
                                                    ))
                                                },
                                            )?;

                                        let mut shard_cids =
                                            Vec::with_capacity(manifest.shards.len());
                                        let mut shard_infos =
                                            Vec::with_capacity(manifest.shards.len());
                                        for (shard_idx, shard_info) in
                                            manifest.shards.iter().enumerate()
                                        {
                                            let shard_path =
                                                vec_dir.join(format!("p_{p_id}_s_{shard_idx}.vas"));
                                            let shard_cid = upload_dict_file(
                                                content_store,
                                                &shard_path,
                                                DictKind::VectorShard { p_id },
                                                "dict artifact uploaded to CAS (from disk)",
                                            )
                                            .await?;
                                            shard_infos.push(
                                                fluree_db_binary_index::arena::vector::ShardInfo {
                                                    cas: shard_cid.to_string(),
                                                    count: shard_info.count,
                                                },
                                            );
                                            shard_cids.push(shard_cid);
                                        }

                                        let final_manifest =
                                            fluree_db_binary_index::arena::vector::VectorManifest {
                                                shards: shard_infos,
                                                ..manifest
                                            };
                                        let manifest_json = serde_json::to_vec_pretty(
                                            &final_manifest,
                                        )
                                        .map_err(|e| {
                                            IndexerError::StorageWrite(format!(
                                                "serialize vector manifest: {e}"
                                            ))
                                        })?;
                                        let final_manifest_path =
                                            vec_dir.join(format!("p_{p_id}_final.vam"));
                                        std::fs::write(&final_manifest_path, &manifest_json)
                                            .map_err(|e| {
                                                IndexerError::StorageWrite(format!(
                                                    "write final vector manifest: {e}"
                                                ))
                                            })?;
                                        let manifest_cid = upload_dict_file(
                                            content_store,
                                            &final_manifest_path,
                                            DictKind::VectorManifest { p_id },
                                            "dict artifact uploaded to CAS (from disk)",
                                        )
                                        .await?;

                                        per_pred.insert(
                                            p_id.to_string(),
                                            VectorDictRef {
                                                p_id,
                                                manifest: manifest_cid,
                                                shards: shard_cids,
                                            },
                                        );
                                    }
                                }
                            }
                        }
                        if !per_pred.is_empty() {
                            vectors.insert(g_id_str.to_string(), per_pred);
                        }
                    }
                }
            }
            Ok::<_, IndexerError>(vectors)
        },
    )?;

    let (subject_fwd_ns_packs, subject_reverse) = subject_trees;
    let (string_count, string_fwd_packs, string_reverse) = string_result;

    // ---- 4. Compute subject_id_encoding + watermarks from sids ----
    let overflow_ns: u16 = 0xFFFF;
    let mut needs_wide = false;
    let mut max_ns_code: u16 = 0;
    let mut watermark_map: BTreeMap<u16, u64> = BTreeMap::new();

    for &sid in &sids {
        let subject_id = SubjectId::from_u64(sid);
        let ns_code = subject_id.ns_code();
        let local_id = subject_id.local_id();

        if ns_code == overflow_ns {
            needs_wide = true;
            continue;
        }

        if local_id > u16::MAX as u64 {
            needs_wide = true;
        }

        if ns_code > max_ns_code {
            max_ns_code = ns_code;
        }

        let entry = watermark_map.entry(ns_code).or_insert(0);
        if local_id > *entry {
            *entry = local_id;
        }
    }

    let subject_id_encoding = if needs_wide {
        SubjectIdEncoding::Wide
    } else {
        SubjectIdEncoding::Narrow
    };

    let watermark_len = if watermark_map.is_empty() {
        0
    } else {
        max_ns_code as usize + 1
    };
    let mut subject_watermarks: Vec<u64> = vec![0; watermark_len];
    for (&ns_code, &max_local) in &watermark_map {
        subject_watermarks[ns_code as usize] = max_local;
    }

    let string_watermark = if string_count > 0 {
        (string_count - 1) as u32
    } else {
        0
    };

    tracing::info!(
        subjects = sids.len(),
        strings = string_count,
        numbig_graphs = numbig.len(),
        vector_graphs = vectors.len(),
        ?subject_id_encoding,
        watermarks = subject_watermarks.len(),
        string_watermark,
        "dictionary trees built and uploaded to CAS (from disk)"
    );
    Ok(UploadedDicts {
        dict_refs: DictRefs {
            forward_packs: DictPackRefs {
                string_fwd_packs,
                subject_fwd_ns_packs,
            },
            subject_reverse,
            string_reverse,
        },
        subject_id_encoding,
        subject_watermarks,
        string_watermark,
        graph_iris,
        datatype_iris,
        language_tags,
        numbig,
        vectors,
    })
}
