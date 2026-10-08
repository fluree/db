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
    let mut local_dicts = Vec::with_capacity(vocab_paths.len());
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
        local_dicts.push(local_dict);
    }

    // Assign global IDs in lexical tag order, matching the lexical per-chunk
    // assignment in `sort_remap_and_write_sorted_commit`. Both sides being
    // lexical makes every local→global remap monotone, so remapping a
    // SPOT-sorted chunk stream cannot reorder langString records (lang_id is
    // the langString `o_type` payload, which participates in the sort key).
    let mut all_tags: Vec<&str> = local_dicts
        .iter()
        .flat_map(|dict| dict.iter().map(|(_, tag)| tag))
        .collect();
    all_tags.sort_unstable();
    all_tags.dedup();

    let mut unified = LanguageTagDict::new();
    for tag in all_tags {
        unified.get_or_insert(Some(tag));
    }

    // Build remaps: local_id → global_id. remap[0] = 0 always (sentinel for
    // "no lang tag").
    let mut remaps = Vec::with_capacity(local_dicts.len());
    for local_dict in &local_dicts {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::runs::run_file::serialize_lang_dict;

    fn write_vocab(dir: &std::path::Path, name: &str, tags: &[&str]) -> PathBuf {
        let mut dict = LanguageTagDict::new();
        for tag in tags {
            dict.get_or_insert(Some(tag));
        }
        let path = dir.join(name);
        std::fs::write(&path, serialize_lang_dict(&dict)).unwrap();
        path
    }

    // The k-way merges consume chunk streams remapped local→global on the
    // fly and rely on them staying sorted; since lang_id is the langString
    // o_type payload, every per-chunk remap must be monotone over the ids
    // the chunk uses. First-seen global assignment broke this whenever a
    // later chunk introduced a lexically-smaller tag.
    #[test]
    fn remaps_are_monotone_for_adversarial_first_seen_order() {
        let dir = std::env::temp_dir().join("fluree_test_lang_remap_monotone");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Chunk vocabs are written lexically (see the chunk sort), but chunk
        // 1 introduces "de" — lexically before chunk 0's "en" — which
        // first-seen global assignment would number after it.
        let v0 = write_vocab(&dir, "c0.voc", &["en"]);
        let v1 = write_vocab(&dir, "c1.voc", &["de", "en"]);

        let (unified, remaps) = build_lang_remap_from_vocabs(&[v0, v1]).unwrap();

        assert_eq!(unified.resolve(1), Some("de"));
        assert_eq!(unified.resolve(2), Some("en"));

        for remap in &remaps {
            let used: Vec<u16> = remap[1..].to_vec();
            let mut sorted = used.clone();
            sorted.sort_unstable();
            assert_eq!(used, sorted, "local→global lang remap must be monotone");
        }
        assert_eq!(remaps[0], vec![0, 2]); // en → 2
        assert_eq!(remaps[1], vec![0, 1, 2]); // de → 1, en → 2

        let _ = std::fs::remove_dir_all(&dir);
    }
}
