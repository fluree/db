//! Build forward dictionary packs from sorted entry iterators.
//!
//! Produces `PackArtifact` values ready for CAS upload. The authoritative
//! CID comes from the `content_write_bytes` result, not from pre-computation.

use std::io;

use super::forward_pack::{encode_forward_pack, KIND_STRING_FWD, KIND_SUBJECT_FWD};

/// Default target page size (bytes). Each page is the smallest unit of
/// random access within a pack. Smaller pages reduce the first-touch
/// working set on cold runs (fewer bytes faulted per page hit).
pub const DEFAULT_TARGET_PAGE_BYTES: usize = 512 * 1024;

/// Default target pack size (bytes). Packs are large immutable CAS objects.
pub const DEFAULT_TARGET_PACK_BYTES: usize = 256 * 1024 * 1024;

/// A single pack artifact produced by the builder, ready for CAS upload.
#[derive(Debug)]
pub struct PackArtifact {
    /// Complete pack bytes (`FPK1` format).
    pub bytes: Vec<u8>,
    /// First ID covered by this pack (inclusive).
    pub first_id: u64,
    /// Last ID covered by this pack (inclusive).
    pub last_id: u64,
}

/// Result of building packs for one forward dictionary stream.
#[derive(Debug)]
pub struct PackBuildResult {
    pub packs: Vec<PackArtifact>,
}

/// Build string forward packs from sorted, contiguous `(str_id, value)` entries.
///
/// String IDs are globally contiguous (0..N). The iterator must yield entries
/// in ascending `str_id` order.
pub fn build_string_forward_packs(
    entries: &[(u32, &[u8])],
    target_page_bytes: usize,
    target_pack_bytes: usize,
) -> io::Result<PackBuildResult> {
    if entries.is_empty() {
        return Ok(PackBuildResult { packs: Vec::new() });
    }

    // Convert u32 IDs to u64 for the pack encoder.
    let entries_u64: Vec<(u64, &[u8])> = entries.iter().map(|&(id, v)| (id as u64, v)).collect();

    build_packs_from_contiguous(
        &entries_u64,
        KIND_STRING_FWD,
        0, // ns_code = 0 for strings
        target_page_bytes,
        target_pack_bytes,
    )
}

/// Build subject forward packs for a single namespace.
///
/// `entries` must be sorted by `local_id` in ascending order and contiguous.
/// Values are suffix bytes (namespace prefix stripped).
pub fn build_subject_forward_packs_for_ns(
    ns_code: u16,
    entries: &[(u64, &[u8])],
    target_page_bytes: usize,
    target_pack_bytes: usize,
) -> io::Result<PackBuildResult> {
    if entries.is_empty() {
        return Ok(PackBuildResult { packs: Vec::new() });
    }

    build_packs_from_contiguous(
        entries,
        KIND_SUBJECT_FWD,
        ns_code,
        target_page_bytes,
        target_pack_bytes,
    )
}

/// Internal: partition contiguous entries into packs.
fn build_packs_from_contiguous(
    entries: &[(u64, &[u8])],
    kind: u8,
    ns_code: u16,
    target_page_bytes: usize,
    target_pack_bytes: usize,
) -> io::Result<PackBuildResult> {
    let mut packs = Vec::new();
    let mut pack_start = 0usize;
    let mut pack_data_est = 0usize;

    for (i, &(_, value)) in entries.iter().enumerate() {
        // Rough estimate of per-entry contribution to pack size.
        pack_data_est += value.len() + 4; // value bytes + offset entry

        let is_last = i == entries.len() - 1;
        if pack_data_est >= target_pack_bytes || is_last {
            let pack_entries = &entries[pack_start..=i];
            let pack_bytes = encode_forward_pack(pack_entries, kind, ns_code, target_page_bytes)?;
            let first_id = pack_entries[0].0;
            let last_id = pack_entries.last().unwrap().0;

            packs.push(PackArtifact {
                bytes: pack_bytes,
                first_id,
                last_id,
            });

            pack_start = i + 1;
            pack_data_est = 0;
        }
    }

    Ok(PackBuildResult { packs })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict::forward_pack::ForwardPack;

    #[test]
    fn test_build_string_packs_small() {
        let entries: Vec<(u32, Vec<u8>)> = (0..100)
            .map(|i| (i, format!("string_{i}").into_bytes()))
            .collect();
        let refs: Vec<(u32, &[u8])> = entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();

        let result =
            build_string_forward_packs(&refs, DEFAULT_TARGET_PAGE_BYTES, DEFAULT_TARGET_PACK_BYTES)
                .unwrap();

        // Small dataset → one pack
        assert_eq!(result.packs.len(), 1);
        assert_eq!(result.packs[0].first_id, 0);
        assert_eq!(result.packs[0].last_id, 99);

        // Verify content
        let pack = ForwardPack::from_bytes(&result.packs[0].bytes).unwrap();
        assert_eq!(pack.lookup_str(50).unwrap(), Some("string_50".to_string()));
    }

    #[test]
    fn test_build_string_packs_multi_page() {
        let entries: Vec<(u32, Vec<u8>)> = (0..10_000)
            .map(|i| (i, format!("http://example.org/entity/{i}").into_bytes()))
            .collect();
        let refs: Vec<(u32, &[u8])> = entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();

        // Small page target to force multiple pages.
        let result = build_string_forward_packs(&refs, 4096, DEFAULT_TARGET_PACK_BYTES).unwrap();

        assert_eq!(result.packs.len(), 1); // Single pack (data well under 256MB)
        let pack = ForwardPack::from_bytes(&result.packs[0].bytes).unwrap();
        assert!(
            pack.page_count() > 1,
            "expected multiple pages, got {}",
            pack.page_count()
        );

        // Spot-check
        assert_eq!(
            pack.lookup_str(9999).unwrap(),
            Some("http://example.org/entity/9999".to_string())
        );
    }

    #[test]
    fn test_build_string_packs_multi_pack() {
        let entries: Vec<(u32, Vec<u8>)> = (0..1000)
            .map(|i| (i, format!("val_{i}").into_bytes()))
            .collect();
        let refs: Vec<(u32, &[u8])> = entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();

        // Very small pack target to force multiple packs.
        let result = build_string_forward_packs(&refs, 512, 2048).unwrap();
        assert!(
            result.packs.len() > 1,
            "expected multiple packs, got {}",
            result.packs.len()
        );

        // Verify all entries are reachable across packs.
        for pack_artifact in &result.packs {
            let pack = ForwardPack::from_bytes(&pack_artifact.bytes).unwrap();
            for id in pack_artifact.first_id..=pack_artifact.last_id {
                assert!(
                    pack.lookup(id).is_some(),
                    "missing id {} in pack [{}, {}]",
                    id,
                    pack_artifact.first_id,
                    pack_artifact.last_id
                );
            }
        }
    }

    #[test]
    fn test_build_subject_packs() {
        let entries: Vec<(u64, Vec<u8>)> = (0..50)
            .map(|i| (i as u64, format!("suffix/{i}").into_bytes()))
            .collect();
        let refs: Vec<(u64, &[u8])> = entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();

        let result = build_subject_forward_packs_for_ns(
            7,
            &refs,
            DEFAULT_TARGET_PAGE_BYTES,
            DEFAULT_TARGET_PACK_BYTES,
        )
        .unwrap();

        assert_eq!(result.packs.len(), 1);
        let pack = ForwardPack::from_bytes(&result.packs[0].bytes).unwrap();
        assert_eq!(
            pack.header().kind,
            super::super::forward_pack::KIND_SUBJECT_FWD
        );
        assert_eq!(pack.header().ns_code, 7);
        assert_eq!(pack.lookup_str(25).unwrap(), Some("suffix/25".to_string()));
    }

    #[test]
    fn test_build_empty() {
        let result =
            build_string_forward_packs(&[], DEFAULT_TARGET_PAGE_BYTES, DEFAULT_TARGET_PACK_BYTES)
                .unwrap();
        assert!(result.packs.is_empty());
    }
}
