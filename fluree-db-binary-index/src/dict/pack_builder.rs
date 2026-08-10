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
///
/// 16 MiB balances object count against the cost of a single cold cache fill:
/// S3 has no special latency tier here, and a 16 MiB fetch breaks even with
/// roughly six sequential 512 KiB page misses. It also bounds the work of one
/// compaction merge, which rewrites at most this many bytes.
///
/// The trade runs both ways, and the direction that pushes this constant back
/// DOWN is the sparse cold lookup. The read path whole-fetches a pack
/// (`pack_reader.rs` gets the CID, caches it, then mmaps) rather than reading a
/// byte range, so a first cold touch of any single ID costs the whole pack. A
/// reader resolving a handful of scattered IDs against a cold cache therefore
/// moves strictly more bytes after a run of small packs is merged than before.
/// Anything doing real dictionary work wins — total bytes are conserved and
/// round trips drop sharply — but a deployment whose cache does not survive
/// between invocations (per-container caches on Lambda, discarded on every cold
/// start) sits on the losing side and is the workload to measure before raising
/// this.
pub const DEFAULT_TARGET_PACK_BYTES: usize = 16 * 1024 * 1024;

/// Pack count a freshly built stream aims to stay under.
///
/// A quarter of the `u16` routing-table cap, so a rebuild leaves room for the
/// incremental packs that accumulate before the next one.
pub const SAFE_PACK_COUNT: u64 = 16_384;

/// Target pack size for a stream holding `total_dict_bytes`, doubling from
/// [`DEFAULT_TARGET_PACK_BYTES`] only once a fixed target could no longer keep
/// the routing table under [`SAFE_PACK_COUNT`].
///
/// The routing table is `u16`-counted, so a stream cut at a fixed 16 MiB hits a
/// hard wall around a terabyte of dictionary — packs stop fitting and the ledger
/// cannot publish. Scaling the target removes the wall: pack count is bounded by
/// construction at any data size, and the ledgers that pay for larger packs are
/// exactly the ones where per-pack overhead amortizes best.
///
/// **The floor is the point.** This returns exactly `DEFAULT_TARGET_PACK_BYTES`
/// until a stream passes `SAFE_PACK_COUNT × 16 MiB` (256 GiB), so an ordinary
/// ledger's packs are cut identically to before — no behaviour change, nothing
/// to roll out. Doubling (rather than tracking size continuously) keeps the
/// target stable as a dictionary grows: it changes once per doubling instead of
/// drifting every build.
///
/// Compaction deliberately does NOT follow this target; it stays at
/// `COMPACTION_MAX_OUTPUT_BYTES`. Raising the merge ceiling would make every
/// already-compacted pack eligible to merge with its neighbours again — eight
/// adjacent 16 MiB packs are size-comparable and would qualify under a larger
/// ceiling — so every byte of every dictionary in the fleet would be rewritten
/// `log_8(new / old)` times for no benefit to any ledger that is not near the
/// wall. Packs larger than the merge ceiling are simply inert to compaction
/// (there is no split operation), and a stream that does drift back over the cap
/// aborts to a rebuild, which re-cuts it at this target. Rebuild is the
/// pressure valve; compaction is left alone.
pub fn pack_target_bytes(total_dict_bytes: u64) -> usize {
    let mut target = DEFAULT_TARGET_PACK_BYTES;
    // Exponential, so this runs a handful of times even for absurd inputs; the
    // guard is against overflow rather than against looping.
    while total_dict_bytes / (target as u64) > SAFE_PACK_COUNT {
        match target.checked_mul(2) {
            Some(doubled) => target = doubled,
            None => break,
        }
    }
    target
}

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
mod target_tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    #[test]
    fn the_floor_holds_for_every_ordinary_dictionary() {
        // This is the property that makes the rule safe to ship: an existing
        // ledger's packs must be cut exactly as before, so no fleet-wide
        // re-cutting or re-merging is triggered by deploying it.
        for bytes in [
            0,
            1,
            MIB,
            100 * MIB,
            GIB,
            100 * GIB,
            // Right at the threshold: 16 MiB x SAFE_PACK_COUNT = 256 GiB.
            SAFE_PACK_COUNT * DEFAULT_TARGET_PACK_BYTES as u64,
        ] {
            assert_eq!(
                pack_target_bytes(bytes),
                DEFAULT_TARGET_PACK_BYTES,
                "{bytes} bytes must still cut at the default target"
            );
        }
    }

    #[test]
    fn the_target_doubles_only_past_the_threshold() {
        // The step lands where a whole extra pack would be needed, not at the
        // exact byte: the rule compares `total / target` against the safe count,
        // so the last partial pack does not push a stream into the next tier.
        let threshold = SAFE_PACK_COUNT * DEFAULT_TARGET_PACK_BYTES as u64;
        assert_eq!(
            pack_target_bytes(threshold + DEFAULT_TARGET_PACK_BYTES as u64 - 1),
            DEFAULT_TARGET_PACK_BYTES,
            "a partial pack past the threshold stays in the default tier"
        );
        assert_eq!(
            pack_target_bytes(threshold + DEFAULT_TARGET_PACK_BYTES as u64),
            DEFAULT_TARGET_PACK_BYTES * 2,
            "one whole pack past the threshold steps up exactly one tier"
        );
        assert_eq!(
            pack_target_bytes(2 * threshold),
            DEFAULT_TARGET_PACK_BYTES * 2,
            "a doubled dictionary sits in the doubled tier, not a third one"
        );
        assert_eq!(
            pack_target_bytes(4 * threshold),
            DEFAULT_TARGET_PACK_BYTES * 4
        );
    }

    #[test]
    fn the_pack_count_stays_bounded_at_any_size() {
        // The whole purpose: a fixed target walls out around a terabyte, and
        // this must not, however large the dictionary gets.
        for bytes in [256 * GIB, 1024 * GIB, 64 * 1024 * GIB, u64::MAX / 2] {
            let target = pack_target_bytes(bytes) as u64;
            let packs = bytes / target;
            assert!(
                packs <= SAFE_PACK_COUNT,
                "{bytes} bytes at target {target} yields {packs} packs, over the safe count"
            );
            assert!(
                packs < u16::MAX as u64,
                "and must stay under the u16 routing-table cap"
            );
        }
    }

    #[test]
    fn the_target_never_shrinks_as_a_dictionary_grows() {
        // Monotonicity is what keeps a growing ledger from oscillating between
        // tiers and re-cutting its packs on every rebuild.
        let mut previous = 0usize;
        let mut bytes = MIB;
        while bytes < u64::MAX / 4 {
            let target = pack_target_bytes(bytes);
            assert!(target >= previous, "target shrank at {bytes} bytes");
            assert!(target >= DEFAULT_TARGET_PACK_BYTES);
            previous = target;
            bytes *= 2;
        }
    }
}

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

        assert_eq!(result.packs.len(), 1); // Single pack (data well under the target)
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
