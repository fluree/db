//! Language tag reconciliation across chunk vocab files.
//!
//! Each chunk has its own per-chunk `lang_id` mapping. Before merging,
//! we read all lang dicts from vocab files and build a unified
//! `LanguageTagDict` with per-chunk remap tables.

use super::global_dict::LanguageTagDict;
use crate::run_index::runs::run_file::deserialize_lang_dict;
use std::io;
use std::path::PathBuf;

/// Build a unified language dictionary and per-chunk remap tables from
/// per-chunk language vocab files (written during Phase A).
///
/// Each vocab file uses the `serialize_lang_dict` format.
/// Returns `(unified_dict, per_chunk_remaps)` where
/// `per_chunk_remaps[i]` maps chunk-local lang_id → global lang_id.
///
/// If a chunk has no language tags (empty or missing vocab file), the remap
/// is just `[0]` (sentinel only).
pub fn build_lang_remap_from_vocabs(
    vocab_paths: &[PathBuf],
) -> io::Result<(LanguageTagDict, Vec<Vec<u16>>)> {
    let mut unified = LanguageTagDict::new();
    let mut remaps = Vec::with_capacity(vocab_paths.len());

    for path in vocab_paths {
        let local_dict = if path.exists() {
            let data = std::fs::read(path)?;
            if data.is_empty() {
                LanguageTagDict::new()
            } else {
                deserialize_lang_dict(&data)?
            }
        } else {
            LanguageTagDict::new()
        };

        // Build remap: local_id → global_id
        // remap[0] = 0 always (sentinel for "no lang tag")
        let max_local_id = local_dict.len();
        let mut remap = vec![0u16; (max_local_id as usize) + 1];

        for (local_id, tag) in local_dict.iter() {
            let global_id = unified.get_or_insert(Some(tag));
            remap[local_id as usize] = global_id;
        }

        remaps.push(remap);
    }

    Ok((unified, remaps))
}
