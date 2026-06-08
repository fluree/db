//! V3 index build from sorted commit artifacts.
//!
//! Orchestrates the full V3 pipeline: remap sorted commits → V2 run files →
//! k-way merge → FLI3/FBR3 artifacts. Operates synchronously within a
//! `spawn_blocking` context.
//!
//! Bulk import now writes V2-native sorted-commit artifacts directly, so this
//! module consumes those artifacts without a bulk-import-only V1 → V2 pass.

use crate::run_index::build::index_build::{
    build_all_indexes, BuildAllConfig, IndexBuildResult, PersistingLeafWriter,
};
use crate::run_index::build::merge::KWayMerge;
use crate::run_index::runs::run_writer::{
    MultiOrderConfig, MultiOrderRunWriter, MultiOrderRunWriterWithOp,
};
use crate::run_index::runs::spool::{
    link_chunk_run_files_to_flat, remap_commit_to_runs_with_op, remap_sorted_commit_v2_to_runs,
    MmapStringRemap, MmapSubjectRemap, SortedCommitMergeReaderV2, SubjectRemap,
};
use crate::stats::{stats_record_from_v2, SpotClassStats, DT_REF_ID};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::cmp_v2_spot;
use fluree_db_core::o_type::OType;
use fluree_db_core::o_type_registry::OTypeRegistry;
use rustc_hash::{FxHashMap, FxHashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub const BUILD_STAGE_REMAP: u8 = 1;
pub const BUILD_STAGE_LINK_RUNS: u8 = 2;
pub const BUILD_STAGE_MERGE: u8 = 3;
const PROGRESS_BATCH_SIZE: u64 = 4096;

#[derive(Debug, Clone, Copy)]
struct ProcessMemorySnapshot {
    vm_rss_mb: u64,
    rss_anon_mb: u64,
    rss_file_mb: u64,
    vm_swap_mb: u64,
}

#[cfg(target_os = "linux")]
fn process_memory_snapshot() -> Option<ProcessMemorySnapshot> {
    fn kb_for(status: &str, key: &str) -> u64 {
        status
            .lines()
            .find_map(|line| {
                let rest = line.strip_prefix(key)?;
                rest.split_whitespace().next()?.parse::<u64>().ok()
            })
            .unwrap_or(0)
    }

    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    Some(ProcessMemorySnapshot {
        vm_rss_mb: kb_for(&status, "VmRSS:") / 1024,
        rss_anon_mb: kb_for(&status, "RssAnon:") / 1024,
        rss_file_mb: kb_for(&status, "RssFile:") / 1024,
        vm_swap_mb: kb_for(&status, "VmSwap:") / 1024,
    })
}

#[cfg(not(target_os = "linux"))]
fn process_memory_snapshot() -> Option<ProcessMemorySnapshot> {
    None
}

fn log_index_memory(stage: &str) {
    if let Some(mem) = process_memory_snapshot() {
        tracing::info!(
            stage,
            vm_rss_mb = mem.vm_rss_mb,
            rss_anon_mb = mem.rss_anon_mb,
            rss_file_mb = mem.rss_file_mb,
            vm_swap_mb = mem.vm_swap_mb,
            "index build memory snapshot"
        );
    }
}

/// Input for a single sorted commit chunk.
#[derive(Clone)]
pub struct CommitInput {
    /// Path to the V2-native sorted commit file.
    pub commit_path: PathBuf,
    /// Number of records in the commit file.
    pub record_count: u64,
    /// Path to the mmap'd subject remap file.
    pub subject_remap_path: PathBuf,
    /// Path to the mmap'd string remap file.
    pub string_remap_path: PathBuf,
    /// Chunk-local → global language ID remap.
    pub lang_remap: Vec<u16>,
    /// Optional rdf:type sidecar used to rebuild the subject→class bitset table.
    pub types_map_path: Option<PathBuf>,
}

/// Configuration for the V3 build-from-commits pipeline.
#[derive(Clone)]
pub struct BuildConfig {
    /// Base directory for temporary run files.
    pub run_dir: PathBuf,
    /// Output directory for per-graph index artifacts.
    pub index_dir: PathBuf,
    /// Graph ID (builds are graph-scoped).
    pub g_id: u16,
    /// Target rows per leaflet.
    pub leaflet_target_rows: usize,
    /// Target rows per leaf.
    pub leaf_target_rows: usize,
    /// Zstd compression level.
    pub zstd_level: i32,
    /// Memory budget for run writers (bytes).
    pub run_budget_bytes: usize,
    /// Worker count for chunk-parallel secondary-order generation.
    pub worker_count: usize,
    /// Remap progress counter (optional).
    pub remap_progress: Option<Arc<AtomicU64>>,
    /// Merge/build progress counter (optional).
    pub build_progress: Option<Arc<AtomicU64>>,
    /// Shared stage marker for external progress reporting.
    pub stage_marker: Option<Arc<AtomicU8>>,
}

/// Result of the V3 build pipeline.
pub struct BuildResult {
    /// Per-order build results (each contains per-graph artifacts).
    pub order_results: Vec<(RunSortOrder, IndexBuildResult)>,
    /// Total rows across all orders (from the POST result for canonical count).
    pub total_rows: u64,
    /// Total records remapped across all chunks.
    pub total_remapped: u64,
    /// Time spent in the remap phase.
    pub remap_elapsed: std::time::Duration,
    /// Time spent in the build phase.
    pub build_elapsed: std::time::Duration,
}

/// Dense 64-class subject→class-mask table: one `u64` bitmask per subject
/// (bit *i* set ⇒ the subject is a member of `bit_to_class[i]`). Compact and
/// cache-friendly, but limited to 64 distinct classes — used as the fast path
/// in [`ClassMembership`] when a ledger has ≤ 64 classes.
pub struct ClassBitsetTable {
    pub bit_to_class: Vec<u64>,
    graph_bitsets: FxHashMap<u16, FxHashMap<u16, Vec<u64>>>,
}

impl ClassBitsetTable {
    pub fn get(&self, g_id: u16, sid: u64) -> u64 {
        let ns_code = (sid >> 48) as u16;
        let local_id = (sid & 0x0000_FFFF_FFFF_FFFF) as usize;
        self.graph_bitsets
            .get(&g_id)
            .and_then(|ns| ns.get(&ns_code))
            .and_then(|v| v.as_slice().get(local_id).copied())
            .unwrap_or(0)
    }
}

/// Sparse subject→class membership: an explicit (sorted, deduped) class list
/// per subject. Memory is flat in the number of distinct classes (~one entry
/// per `(subject, class)` type assertion), so it scales to arbitrarily large
/// ontologies where the dense [`ClassBitsetTable`] would not fit. Used as the
/// fallback in [`ClassMembership`] once a ledger exceeds 64 distinct classes.
pub struct SparseClassMembership {
    /// `(g_id, subject_sid64)` → sorted, deduped global class sids.
    membership: FxHashMap<(u16, u64), Box<[u64]>>,
    /// Number of distinct classes (diagnostics only).
    distinct_classes: usize,
}

impl SparseClassMembership {
    #[inline]
    fn classes_of(&self, g_id: u16, sid: u64) -> &[u64] {
        self.membership
            .get(&(g_id, sid))
            .map(|b| &b[..])
            .unwrap_or(&[])
    }
}

const CLASS_MEMBERSHIP_BUCKETS: usize = 256;
const CLASS_MEMBERSHIP_ENTRY_BYTES: usize = 18;
const CLASS_MEMBERSHIP_INDEX_BYTES: usize = 32;

/// Disk-backed subject→class membership used by the import path at large scale.
///
/// The in-memory sparse representation is too large for Wikidata-scale imports:
/// a `HashMap<(graph, subject), Box<[class]>>` grows with every typed subject.
/// This representation keeps compact sorted bucket indexes mmapped from disk and
/// binary-searches the relevant bucket for target-class lookups.
pub struct DiskClassMembership {
    buckets: Vec<Option<DiskClassMembershipBucket>>,
    distinct_classes: usize,
    subjects: usize,
}

pub struct DiskClassMembershipBucket {
    index: memmap2::Mmap,
    data: memmap2::Mmap,
    entries: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TypeEntry {
    g_id: u16,
    subject: u64,
    class: u64,
}

impl Ord for TypeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.g_id, self.subject, self.class).cmp(&(other.g_id, other.subject, other.class))
    }
}

impl PartialOrd for TypeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn class_membership_bucket(g_id: u16, sid: u64) -> usize {
    let h = sid.wrapping_mul(11_400_714_819_323_198_485).rotate_left(17) ^ (g_id as u64);
    (h as usize) & (CLASS_MEMBERSHIP_BUCKETS - 1)
}

fn write_type_entry<W: std::io::Write>(writer: &mut W, entry: TypeEntry) -> io::Result<()> {
    writer.write_all(&entry.g_id.to_le_bytes())?;
    writer.write_all(&entry.subject.to_le_bytes())?;
    writer.write_all(&entry.class.to_le_bytes())?;
    Ok(())
}

fn read_type_entries(path: &Path) -> io::Result<Vec<TypeEntry>> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len() as usize;
    if len % CLASS_MEMBERSHIP_ENTRY_BYTES != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("class membership bucket has invalid byte length: {len}"),
        ));
    }

    let mut bytes = vec![0u8; len];
    file.read_exact(&mut bytes)?;
    let mut entries = Vec::with_capacity(len / CLASS_MEMBERSHIP_ENTRY_BYTES);
    for chunk in bytes.chunks_exact(CLASS_MEMBERSHIP_ENTRY_BYTES) {
        entries.push(TypeEntry {
            g_id: u16::from_le_bytes(chunk[0..2].try_into().unwrap()),
            subject: u64::from_le_bytes(chunk[2..10].try_into().unwrap()),
            class: u64::from_le_bytes(chunk[10..18].try_into().unwrap()),
        });
    }
    Ok(entries)
}

fn write_membership_index_entry<W: std::io::Write>(
    writer: &mut W,
    g_id: u16,
    subject: u64,
    offset: u64,
    count: u32,
) -> io::Result<()> {
    let mut buf = [0u8; CLASS_MEMBERSHIP_INDEX_BYTES];
    buf[0..2].copy_from_slice(&g_id.to_le_bytes());
    buf[8..16].copy_from_slice(&subject.to_le_bytes());
    buf[16..24].copy_from_slice(&offset.to_le_bytes());
    buf[24..28].copy_from_slice(&count.to_le_bytes());
    writer.write_all(&buf)
}

fn membership_index_key(index: &[u8], pos: usize) -> (u16, u64) {
    let start = pos * CLASS_MEMBERSHIP_INDEX_BYTES;
    (
        u16::from_le_bytes(index[start..start + 2].try_into().unwrap()),
        u64::from_le_bytes(index[start + 8..start + 16].try_into().unwrap()),
    )
}

fn mmap_readonly(path: &Path) -> io::Result<memmap2::Mmap> {
    let file = std::fs::File::open(path)?;
    // SAFETY: class-membership files are fully written before they are mapped,
    // then treated as immutable index-build artifacts.
    unsafe { memmap2::Mmap::map(&file) }
}

impl DiskClassMembership {
    fn classes_of(&self, g_id: u16, sid: u64, out: &mut Vec<u64>) {
        out.clear();
        let bucket_idx = class_membership_bucket(g_id, sid);
        let Some(bucket) = self.buckets.get(bucket_idx).and_then(Option::as_ref) else {
            return;
        };

        let mut lo = 0usize;
        let mut hi = bucket.entries;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if membership_index_key(&bucket.index, mid) < (g_id, sid) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= bucket.entries || membership_index_key(&bucket.index, lo) != (g_id, sid) {
            return;
        }

        let start = lo * CLASS_MEMBERSHIP_INDEX_BYTES;
        let offset =
            u64::from_le_bytes(bucket.index[start + 16..start + 24].try_into().unwrap()) as usize;
        let count =
            u32::from_le_bytes(bucket.index[start + 24..start + 28].try_into().unwrap()) as usize;
        let byte_start = offset * 8;
        let byte_end = byte_start + count * 8;
        if byte_end > bucket.data.len() {
            tracing::warn!(
                g_id,
                sid,
                offset,
                count,
                data_bytes = bucket.data.len(),
                "class membership bucket entry points outside data mmap"
            );
            return;
        }

        out.reserve(count);
        for chunk in bucket.data[byte_start..byte_end].chunks_exact(8) {
            out.push(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
    }
}

/// Subject→class membership lookup used to attribute reference *targets* to
/// their classes when building `class_prop_refs` stats.
///
/// Uses the compact 64-class [`ClassBitsetTable`] for the common case
/// (≤ 64 classes) and transparently transitions to a [`SparseClassMembership`]
/// above that — uncapped and memory-bounded (flat in class count) for large
/// ontologies. The previous implementation hard-capped at 64 classes and
/// silently truncated ref-class rollups; this never truncates on class count.
pub enum ClassMembership {
    Bitset(ClassBitsetTable),
    Sparse(SparseClassMembership),
    Disk(DiskClassMembership),
}

impl ClassMembership {
    /// Append the classes of `(g_id, sid)` into `out` (cleared first).
    /// No allocation once `out` has capacity.
    #[inline]
    fn collect_classes(&self, g_id: u16, sid: u64, out: &mut Vec<u64>) {
        out.clear();
        match self {
            Self::Bitset(b) => {
                let mut bits = b.get(g_id, sid);
                while bits != 0 {
                    let bit_idx = bits.trailing_zeros() as usize;
                    out.push(b.bit_to_class[bit_idx]);
                    bits &= bits - 1;
                }
            }
            Self::Sparse(s) => out.extend_from_slice(s.classes_of(g_id, sid)),
            Self::Disk(d) => d.classes_of(g_id, sid, out),
        }
    }

    /// Diagnostic summary for memory attribution logs.
    ///
    /// `subject_entries` is dense table slots for the bitset representation and
    /// distinct typed subjects for the sparse representation.
    fn summary(&self) -> (&'static str, usize, usize) {
        match self {
            Self::Bitset(b) => {
                let subject_slots = b
                    .graph_bitsets
                    .values()
                    .flat_map(|ns| ns.values())
                    .map(std::vec::Vec::len)
                    .sum::<usize>();
                ("bitset", b.bit_to_class.len(), subject_slots)
            }
            Self::Sparse(s) => ("sparse", s.distinct_classes, s.membership.len()),
            Self::Disk(d) => ("disk", d.distinct_classes, d.subjects),
        }
    }

    /// Build from per-chunk `.types` sidecars whose IDs are chunk-local and are
    /// remapped to global via each commit's `MmapSubjectRemap`. Import path.
    fn build_from_commits(commits: &[CommitInput], scratch_dir: &Path) -> io::Result<Option<Self>> {
        use std::io::{BufReader, BufWriter, Read, Write};

        let build_start = Instant::now();
        log_index_memory("class_membership:start");
        let mut saw_sidecar = false;
        let mut sidecar_files = 0usize;
        let mut type_entries = 0u64;
        let mut sidecar_bytes = 0u64;
        let mut distinct_classes = FxHashSet::default();
        let mut buf = [0u8; 18];

        if scratch_dir.exists() {
            std::fs::remove_dir_all(scratch_dir)?;
        }
        std::fs::create_dir_all(scratch_dir)?;

        let partition_dir = scratch_dir.join("partitions");
        let index_dir = scratch_dir.join("index");
        let data_dir = scratch_dir.join("data");
        std::fs::create_dir_all(&partition_dir)?;
        std::fs::create_dir_all(&index_dir)?;
        std::fs::create_dir_all(&data_dir)?;

        let mut partition_writers = Vec::with_capacity(CLASS_MEMBERSHIP_BUCKETS);
        for bucket in 0..CLASS_MEMBERSHIP_BUCKETS {
            let path = partition_dir.join(format!("bucket_{bucket:03}.typ"));
            partition_writers.push(BufWriter::new(std::fs::File::create(path)?));
        }

        for commit in commits {
            let Some(types_map_path) = &commit.types_map_path else {
                continue;
            };
            saw_sidecar = true;
            let remap = MmapSubjectRemap::open(&commit.subject_remap_path)?;
            let file = std::fs::File::open(types_map_path)?;
            let file_bytes = file.metadata()?.len();
            let entry_count = file_bytes / 18;
            sidecar_files += 1;
            type_entries += entry_count;
            sidecar_bytes += file_bytes;
            let mut reader = BufReader::new(file);

            for _ in 0..entry_count {
                reader.read_exact(&mut buf)?;
                let g_id = u16::from_le_bytes([buf[0], buf[1]]);
                let s_local = u64::from_le_bytes(buf[2..10].try_into().unwrap());
                let c_local = u64::from_le_bytes(buf[10..18].try_into().unwrap());
                let s_global = remap.get(s_local as usize)?;
                let c_global = remap.get(c_local as usize)?;
                distinct_classes.insert(c_global);
                let bucket = class_membership_bucket(g_id, s_global);
                write_type_entry(
                    &mut partition_writers[bucket],
                    TypeEntry {
                        g_id,
                        subject: s_global,
                        class: c_global,
                    },
                )?;
            }
        }

        if !saw_sidecar {
            return Ok(None);
        }
        for writer in &mut partition_writers {
            writer.flush()?;
        }
        drop(partition_writers);

        let mut buckets = Vec::with_capacity(CLASS_MEMBERSHIP_BUCKETS);
        let mut subject_entries = 0usize;
        let mut non_empty_buckets = 0usize;
        let bucket_build_start = Instant::now();
        for bucket in 0..CLASS_MEMBERSHIP_BUCKETS {
            let partition_path = partition_dir.join(format!("bucket_{bucket:03}.typ"));
            let partition_bytes = std::fs::metadata(&partition_path)?.len();
            if partition_bytes == 0 {
                buckets.push(None);
                continue;
            }

            non_empty_buckets += 1;
            let mut entries = read_type_entries(&partition_path)?;
            entries.sort_unstable();

            let index_path = index_dir.join(format!("bucket_{bucket:03}.idx"));
            let data_path = data_dir.join(format!("bucket_{bucket:03}.dat"));
            let mut index_writer = BufWriter::new(std::fs::File::create(&index_path)?);
            let mut data_writer = BufWriter::new(std::fs::File::create(&data_path)?);

            let mut i = 0usize;
            let mut data_offset = 0u64;
            let mut bucket_subjects = 0usize;
            while i < entries.len() {
                let g_id = entries[i].g_id;
                let subject = entries[i].subject;
                let start = i;
                i += 1;
                while i < entries.len() && entries[i].g_id == g_id && entries[i].subject == subject
                {
                    i += 1;
                }

                let mut last_class = None;
                let mut count = 0u32;
                for entry in &entries[start..i] {
                    if last_class == Some(entry.class) {
                        continue;
                    }
                    data_writer.write_all(&entry.class.to_le_bytes())?;
                    last_class = Some(entry.class);
                    count += 1;
                }
                if count > 0 {
                    write_membership_index_entry(
                        &mut index_writer,
                        g_id,
                        subject,
                        data_offset,
                        count,
                    )?;
                    data_offset += count as u64;
                    bucket_subjects += 1;
                }
            }

            index_writer.flush()?;
            data_writer.flush()?;
            subject_entries += bucket_subjects;

            let index_mmap = mmap_readonly(&index_path)?;
            let data_mmap = mmap_readonly(&data_path)?;
            let index_entries = index_mmap.len() / CLASS_MEMBERSHIP_INDEX_BYTES;
            buckets.push(Some(DiskClassMembershipBucket {
                index: index_mmap,
                data: data_mmap,
                entries: index_entries,
            }));

            let _ = std::fs::remove_file(&partition_path);
        }

        tracing::info!(
            buckets = non_empty_buckets,
            subjects = subject_entries,
            elapsed_ms = bucket_build_start.elapsed().as_millis(),
            "disk-backed class membership buckets built"
        );

        let membership = ClassMembership::Disk(DiskClassMembership {
            buckets,
            distinct_classes: distinct_classes.len(),
            subjects: subject_entries,
        });
        let (representation, classes, subject_entries) = membership.summary();
        if let Some(mem) = process_memory_snapshot() {
            tracing::info!(
                sidecar_files,
                type_entries,
                sidecar_mb = sidecar_bytes / (1024 * 1024),
                representation,
                classes,
                subject_entries,
                elapsed_ms = build_start.elapsed().as_millis(),
                vm_rss_mb = mem.vm_rss_mb,
                rss_anon_mb = mem.rss_anon_mb,
                rss_file_mb = mem.rss_file_mb,
                vm_swap_mb = mem.vm_swap_mb,
                "class membership built from import types sidecars"
            );
        } else {
            tracing::info!(
                sidecar_files,
                type_entries,
                sidecar_mb = sidecar_bytes / (1024 * 1024),
                representation,
                classes,
                subject_entries,
                elapsed_ms = build_start.elapsed().as_millis(),
                "class membership built from import types sidecars"
            );
        }
        Ok(Some(membership))
    }

    /// Build from `.types` sidecar files that already contain **global** IDs.
    ///
    /// Used by the full rebuild path (`rebuild.rs`) where `.types` sidecars are
    /// written after chunk-local → global remapping (no `MmapSubjectRemap` needed).
    ///
    /// Wire format: 18 bytes per entry — `(g_id: u16 LE, s_id: u64 LE, class_sid64: u64 LE)`.
    pub fn build_from_global_types(types_paths: &[PathBuf]) -> io::Result<Option<Self>> {
        use std::io::{BufReader, Read};

        let mut builder = ClassMembershipBuilder::new();
        let mut saw_sidecar = false;
        let mut buf = [0u8; 18];

        for types_path in types_paths {
            if !types_path.exists() {
                continue;
            }
            saw_sidecar = true;
            let file = std::fs::File::open(types_path)?;
            let entry_count = file.metadata()?.len() / 18;
            let mut reader = BufReader::new(file);

            for _ in 0..entry_count {
                reader.read_exact(&mut buf)?;
                let g_id = u16::from_le_bytes([buf[0], buf[1]]);
                let s_global = u64::from_le_bytes(buf[2..10].try_into().unwrap());
                let c_global = u64::from_le_bytes(buf[10..18].try_into().unwrap());
                builder.add(g_id, s_global, c_global);
            }
        }

        if !saw_sidecar {
            return Ok(None);
        }
        Ok(Some(builder.finish()))
    }
}

/// Accumulates subject→class membership, starting on the compact 64-class
/// bitset and transparently promoting to a sparse per-subject representation
/// the moment a 65th distinct class appears. The common case (≤ 64 classes)
/// pays exactly the old dense-bitset cost; only large ontologies allocate the
/// sparse map.
struct ClassMembershipBuilder {
    class_to_bit: FxHashMap<u64, u8>,
    bit_to_class: Vec<u64>,
    graph_bitsets: FxHashMap<u16, FxHashMap<u16, Vec<u64>>>,
    /// `Some` once promoted to sparse mode: `(g_id, s_global)` → class list.
    sparse: Option<FxHashMap<(u16, u64), Vec<u64>>>,
    /// Distinct classes seen after promotion (seeded from `bit_to_class`).
    sparse_classes: FxHashSet<u64>,
}

impl ClassMembershipBuilder {
    fn new() -> Self {
        Self {
            class_to_bit: FxHashMap::default(),
            bit_to_class: Vec::new(),
            graph_bitsets: FxHashMap::default(),
            sparse: None,
            sparse_classes: FxHashSet::default(),
        }
    }

    #[inline]
    fn add(&mut self, g_id: u16, s_global: u64, c_global: u64) {
        if let Some(sparse) = self.sparse.as_mut() {
            sparse.entry((g_id, s_global)).or_default().push(c_global);
            self.sparse_classes.insert(c_global);
            return;
        }

        let bit_idx = if let Some(&idx) = self.class_to_bit.get(&c_global) {
            idx
        } else if self.bit_to_class.len() < 64 {
            let idx = self.bit_to_class.len() as u8;
            self.class_to_bit.insert(c_global, idx);
            self.bit_to_class.push(c_global);
            idx
        } else {
            // 65th distinct class: promote to sparse, then re-route this entry.
            self.promote_to_sparse();
            self.add(g_id, s_global, c_global);
            return;
        };

        let ns_code = (s_global >> 48) as u16;
        let local_id = (s_global & 0x0000_FFFF_FFFF_FFFF) as usize;
        let ns_map = self.graph_bitsets.entry(g_id).or_default();
        let vec = ns_map.entry(ns_code).or_default();
        if local_id >= vec.len() {
            vec.resize(local_id + 1, 0);
        }
        vec[local_id] |= 1u64 << bit_idx;
    }

    /// Expand the dense bitset accumulated so far into the sparse map, then
    /// switch into sparse mode. One-time O(subjects-seen) cost, paid only on
    /// ledgers that exceed 64 classes.
    fn promote_to_sparse(&mut self) {
        let mut sparse: FxHashMap<(u16, u64), Vec<u64>> = FxHashMap::default();
        for (&g_id, ns_map) in &self.graph_bitsets {
            for (&ns_code, vec) in ns_map {
                for (local_id, &bits) in vec.iter().enumerate() {
                    if bits == 0 {
                        continue;
                    }
                    let s_global = ((ns_code as u64) << 48) | (local_id as u64);
                    let entry = sparse.entry((g_id, s_global)).or_default();
                    let mut b = bits;
                    while b != 0 {
                        let bit_idx = b.trailing_zeros() as usize;
                        entry.push(self.bit_to_class[bit_idx]);
                        b &= b - 1;
                    }
                }
            }
        }
        self.sparse_classes = self.bit_to_class.iter().copied().collect();
        self.graph_bitsets = FxHashMap::default();
        self.class_to_bit = FxHashMap::default();
        self.sparse = Some(sparse);
    }

    fn finish(self) -> ClassMembership {
        if let Some(sparse) = self.sparse {
            let distinct_classes = self.sparse_classes.len();
            let mut membership: FxHashMap<(u16, u64), Box<[u64]>> = FxHashMap::default();
            membership.reserve(sparse.len());
            for (key, mut classes) in sparse {
                classes.sort_unstable();
                classes.dedup();
                membership.insert(key, classes.into_boxed_slice());
            }
            tracing::info!(
                classes = distinct_classes,
                subjects = membership.len(),
                "class membership promoted to sparse (uncapped, > 64 classes)"
            );
            ClassMembership::Sparse(SparseClassMembership {
                membership,
                distinct_classes,
            })
        } else {
            tracing::info!(
                classes = self.bit_to_class.len(),
                graphs = self.graph_bitsets.len(),
                total_subjects = self
                    .graph_bitsets
                    .values()
                    .flat_map(|ns| ns.values())
                    .map(std::vec::Vec::len)
                    .sum::<usize>(),
                "class bitset table built"
            );
            ClassMembership::Bitset(ClassBitsetTable {
                bit_to_class: self.bit_to_class,
                graph_bitsets: self.graph_bitsets,
            })
        }
    }
}

/// Upper bound on the number of distinct `(src_class, prop, target_class)` leaf
/// entries `class_prop_refs` may hold for a single build. Reference-target stats
/// are worst-case `O(classes² × predicates)`; on a pathological high-class,
/// densely-connected ontology that product can explode. Past this budget the
/// collector stops recording *new* class pairs (existing counts keep
/// incrementing) and emits a truncation warning — degrade at the edge, never
/// OOM. ~64M `u64` leaves ≈ a couple GB worst case.
const MAX_CLASS_REF_LEAF_ENTRIES: usize = 64_000_000;

pub struct SpotClassStatsCollector {
    rdf_type_p_id: u32,
    current_s_id: Option<u64>,
    current_g_id: u16,
    classes: Vec<u64>,
    prop_dts: FxHashMap<(u32, u16), u64>,
    prop_langs: FxHashMap<(u32, u16), u64>,
    ref_targets: Vec<(u32, u64)>,
    class_membership: Option<ClassMembership>,
    /// Reusable scratch buffer for a ref target's class list (avoids a
    /// per-subject allocation in the ref-join inner loop).
    target_class_buf: Vec<u64>,
    /// Distinct `(src_class, prop, target_class)` leaves recorded so far.
    ref_leaf_entries: usize,
    /// Set once `class_prop_refs` hit [`MAX_CLASS_REF_LEAF_ENTRIES`].
    ref_truncated: bool,
    result: SpotClassStats,
}

impl SpotClassStatsCollector {
    pub fn new(rdf_type_p_id: u32, class_membership: Option<ClassMembership>) -> Self {
        Self {
            rdf_type_p_id,
            current_s_id: None,
            current_g_id: 0,
            classes: Vec::new(),
            prop_dts: FxHashMap::default(),
            prop_langs: FxHashMap::default(),
            ref_targets: Vec::new(),
            class_membership,
            target_class_buf: Vec::new(),
            ref_leaf_entries: 0,
            ref_truncated: false,
            result: SpotClassStats::default(),
        }
    }

    pub fn on_record(&mut self, rec: &fluree_db_binary_index::format::run_record_v2::RunRecordV2) {
        let sr = stats_record_from_v2(rec, 1);
        if self.current_s_id != Some(sr.s_id) || self.current_g_id != sr.g_id {
            self.flush_subject();
            self.current_s_id = Some(sr.s_id);
            self.current_g_id = sr.g_id;
        }

        if sr.p_id == self.rdf_type_p_id && sr.o_kind == 0x05 {
            self.classes.push(sr.o_key);
            return;
        }

        let ot = OType::from_u16(rec.o_type);
        let is_ref = ot == OType::IRI_REF;
        let dt = if is_ref {
            DT_REF_ID
        } else {
            sr.dt.as_u8() as u16
        };
        *self.prop_dts.entry((sr.p_id, dt)).or_insert(0) += 1;

        if sr.lang_id != 0 {
            *self.prop_langs.entry((sr.p_id, sr.lang_id)).or_insert(0) += 1;
        }

        if is_ref && self.class_membership.is_some() {
            self.ref_targets.push((sr.p_id, sr.o_key));
        }
    }

    pub fn flush_subject(&mut self) {
        if self.classes.is_empty() {
            self.prop_dts.clear();
            self.prop_langs.clear();
            self.ref_targets.clear();
            return;
        }

        let g_id = self.current_g_id;
        for &class_sid in &self.classes {
            *self
                .result
                .class_counts
                .entry((g_id, class_sid))
                .or_insert(0) += 1;
            let class_entry = self
                .result
                .class_prop_dts
                .entry((g_id, class_sid))
                .or_default();
            for (&(p_id, dt), &count) in &self.prop_dts {
                *class_entry.entry(p_id).or_default().entry(dt).or_insert(0) += count;
            }
            if !self.prop_langs.is_empty() {
                let lang_entry = self
                    .result
                    .class_prop_langs
                    .entry((g_id, class_sid))
                    .or_default();
                for (&(p_id, lang_id), &count) in &self.prop_langs {
                    *lang_entry
                        .entry(p_id)
                        .or_default()
                        .entry(lang_id)
                        .or_insert(0) += count;
                }
            }
        }

        if let Some(membership) = self.class_membership.as_ref() {
            // Detach the scratch buffer so it does not alias `self` while the
            // membership (also a field of `self`) is borrowed immutably below.
            let mut tbuf = std::mem::take(&mut self.target_class_buf);
            for &(p_id, target_sid) in &self.ref_targets {
                membership.collect_classes(g_id, target_sid, &mut tbuf);
                if tbuf.is_empty() {
                    continue;
                }
                for &src_class in &self.classes {
                    let ref_entry = self
                        .result
                        .class_prop_refs
                        .entry((g_id, src_class))
                        .or_default()
                        .entry(p_id)
                        .or_default();
                    for &target_class in &tbuf {
                        match ref_entry.entry(target_class) {
                            std::collections::hash_map::Entry::Occupied(mut e) => {
                                *e.get_mut() += 1;
                            }
                            std::collections::hash_map::Entry::Vacant(e) => {
                                if self.ref_leaf_entries >= MAX_CLASS_REF_LEAF_ENTRIES {
                                    self.ref_truncated = true;
                                } else {
                                    e.insert(1);
                                    self.ref_leaf_entries += 1;
                                }
                            }
                        }
                    }
                }
            }
            self.target_class_buf = tbuf;
        }

        self.classes.clear();
        self.prop_dts.clear();
        self.prop_langs.clear();
        self.ref_targets.clear();
    }

    pub fn finish(mut self) -> SpotClassStats {
        self.flush_subject();
        tracing::info!(
            class_count_entries = self.result.class_counts.len(),
            class_prop_dt_classes = self.result.class_prop_dts.len(),
            class_prop_lang_classes = self.result.class_prop_langs.len(),
            class_prop_ref_classes = self.result.class_prop_refs.len(),
            ref_leaf_entries = self.ref_leaf_entries,
            ref_truncated = self.ref_truncated,
            "SPOT class stats collector finished"
        );
        log_index_memory("spot_class_stats:finished");
        if self.ref_truncated {
            tracing::warn!(
                recorded_ref_entries = self.ref_leaf_entries,
                budget = MAX_CLASS_REF_LEAF_ENTRIES,
                "class ref-target stats hit the per-build entry budget and were truncated; \
                 stats.classes[*].properties[*].ref-classes may omit the rarest class pairs"
            );
        }
        self.result
    }
}

/// Build V3 indexes from V2-native sorted commit files.
///
/// Bulk import writes a V2-native sorted-commit artifact after chunk-local vocab
/// alignment. This function applies the global subject/string/language remaps to
/// that artifact, writes per-order run files, then k-way merges those runs into
/// final FLI3/FBR3 artifacts.
///
/// Steps:
/// 1. Build SPOT directly from V2 sorted commit artifacts and produce class stats.
/// 2. Drop SPOT's class-membership heap before secondary-order work starts.
/// 3. Generate secondary-order run files in parallel.
/// 4. K-way merge secondary runs → FLI3/FBR3.
pub fn build_indexes_from_commits(
    commits: &[CommitInput],
    config: &BuildConfig,
    mut stats_hook: Option<&mut crate::stats::IdStatsHook>,
) -> io::Result<(BuildResult, Option<SpotClassStats>)> {
    let overall_start = Instant::now();
    tracing::info!(
        chunks = commits.len(),
        worker_count = config.worker_count,
        run_budget_bytes = config.run_budget_bytes,
        g_id = config.g_id,
        "starting build_indexes_from_commits"
    );
    log_index_memory("build_indexes_from_commits:start");
    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_MERGE, Ordering::Relaxed);
    }

    // Build SPOT + class-membership stats before the secondary-order phases.
    //
    // This is deliberately sequential. ClassMembership can be the largest heap
    // object in a Wikidata-scale import; overlapping it with secondary run
    // generation buffers and concurrent PSOT/POST/OPST builders pushed the 256GB
    // import box into swap. Building SPOT first preserves required class stats
    // while letting that membership heap drop before the secondary phases begin.
    let spot_rdf_type_p_id = stats_hook.as_ref().and_then(|hook| hook.rdf_type_p_id());
    let spot_class_membership = if spot_rdf_type_p_id.is_some() {
        ClassMembership::build_from_commits(commits, &config.run_dir.join("class_membership"))?
    } else {
        None
    };
    if let Some(ref m) = spot_class_membership {
        let (representation, classes, subject_entries) = m.summary();
        tracing::debug!(
            representation,
            classes,
            subject_entries,
            "class membership built for ref-target stats"
        );
    }
    let (spot_result, spot_class_stats) =
        build_spot_index_from_commits(commits, config, spot_rdf_type_p_id, spot_class_membership)?;
    log_index_memory("spot_build:joined_before_secondary_phases");

    // Phase 1: Generate secondary-order runs in parallel.
    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_REMAP, Ordering::Relaxed);
    }
    let remap_start = Instant::now();
    let worker_count = config.worker_count.max(1).min(commits.len().max(1));
    let per_thread_budget_bytes = (config.run_budget_bytes / worker_count).max(64 * 1024 * 1024);
    log_index_memory("secondary_run_generation:start");
    tracing::info!(
        chunks = commits.len(),
        worker_count,
        per_thread_budget_bytes,
        "starting secondary-order run generation"
    );
    let mut total_remapped = 0u64;
    let mut worker_hooks: Vec<crate::stats::IdStatsHook> = Vec::new();
    let next_chunk = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| -> io::Result<()> {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let commits_ref = commits;
            let next_chunk = Arc::clone(&next_chunk);
            let run_dir = config.run_dir.clone();
            let remap_progress = config.remap_progress.clone();
            let target_g_id = config.g_id;
            let collect_stats = stats_hook.is_some();
            // IMPORTANT: worker hooks intentionally do NOT set rdf:type p_id.
            // Setting it makes `IdStatsHook::on_record` build per-subject class /
            // ref / datatype maps for every distinct subject — tens of GB across
            // workers and a >500s serial merge + finalize on large imports (and
            // an OOM risk). Class and ref-class stats are instead produced —
            // uncapped — by the `SpotClassStatsCollector` via `ClassMembership`.
            // Worker hooks keep only the cheap per-property HLL / datatype /
            // graph-flake aggregates.

            handles.push(scope.spawn(
                move || -> io::Result<(u64, Option<crate::stats::IdStatsHook>)> {
                    let mut local_total = 0u64;
                    let mut worker_hook = collect_stats.then(crate::stats::IdStatsHook::new);
                    loop {
                        let pos = next_chunk.fetch_add(1, Ordering::Relaxed);
                        if pos >= commits_ref.len() {
                            break;
                        }

                        let commit = &commits_ref[pos];
                        let chunk_run_dir = run_dir.join(format!("chunk_{pos}"));
                        std::fs::create_dir_all(&chunk_run_dir)?;

                        let s_remap = MmapSubjectRemap::open(&commit.subject_remap_path)?;
                        let str_remap = MmapStringRemap::open(&commit.string_remap_path)?;
                        let mut writer = MultiOrderRunWriter::new(MultiOrderConfig {
                            total_budget_bytes: per_thread_budget_bytes,
                            orders: RunSortOrder::secondary_orders().to_vec(),
                            base_run_dir: chunk_run_dir,
                        })?;

                        let written = remap_sorted_commit_v2_to_runs(
                            &commit.commit_path,
                            commit.record_count,
                            &s_remap,
                            &str_remap,
                            &commit.lang_remap,
                            target_g_id,
                            &mut writer,
                            worker_hook.as_mut(),
                            remap_progress.as_deref(),
                        )?;
                        writer.finish()?;

                        local_total += written;
                    }
                    Ok((local_total, worker_hook))
                },
            ));
        }

        for handle in handles {
            let (written, hook) = handle
                .join()
                .map_err(|_| io::Error::other("secondary run generation thread panicked"))??;
            total_remapped += written;
            if let Some(hook) = hook {
                worker_hooks.push(hook);
            }
        }
        Ok(())
    })?;
    let remap_elapsed = remap_start.elapsed();
    tracing::info!(
        total_remapped,
        elapsed_ms = remap_elapsed.as_millis(),
        "secondary-order run generation complete"
    );
    log_index_memory("secondary_run_generation:complete");

    if let Some(target_hook) = stats_hook.as_mut() {
        let stats_merge_start = Instant::now();
        log_index_memory("worker_id_stats_merge:start");
        tracing::info!(
            worker_hooks = worker_hooks.len(),
            "merging worker-local id stats hooks"
        );
        for hook in worker_hooks {
            target_hook.merge_from(hook);
        }
        tracing::info!(
            elapsed_ms = stats_merge_start.elapsed().as_millis(),
            "merged worker-local id stats hooks"
        );
        log_index_memory("worker_id_stats_merge:complete");
    }

    // Phase 2: Build secondary indexes from run files.
    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_LINK_RUNS, Ordering::Relaxed);
    }
    log_index_memory("secondary_run_link:start");
    for &order in RunSortOrder::secondary_orders() {
        let link_start = Instant::now();
        let flat_dir = config.run_dir.join(order.dir_name());
        if flat_dir.exists() {
            std::fs::remove_dir_all(&flat_dir)?;
        }
        let linked = link_chunk_run_files_to_flat(&config.run_dir, order, &flat_dir)?;
        tracing::info!(
            order = order.dir_name(),
            linked_runs = linked,
            elapsed_ms = link_start.elapsed().as_millis(),
            "linked secondary-order run files"
        );
    }
    log_index_memory("secondary_run_link:complete");
    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_MERGE, Ordering::Relaxed);
    }
    let build_start = Instant::now();
    log_index_memory("secondary_index_merge:start");
    tracing::info!("starting secondary index merge/build");

    let build_config = BuildAllConfig {
        base_run_dir: config.run_dir.clone(),
        index_dir: config.index_dir.clone(),
        leaflet_target_rows: config.leaflet_target_rows,
        leaf_target_rows: config.leaf_target_rows,
        zstd_level: config.zstd_level,
        skip_dedup: true,   // Fresh import: unique asserts.
        skip_history: true, // Append-only: no time-travel data.
        g_id: config.g_id,
        progress: config.build_progress.clone(),
        // Build the secondary orders (PSOT/POST/OPST) concurrently, bounded by
        // the import core budget. SPOT has already completed, so this
        // concurrency no longer overlaps with the class-membership heap.
        // build_all_indexes clamps this to the number of buildable orders.
        max_concurrency: config.worker_count,
    };

    let mut order_results = build_all_indexes(&build_config).map_err(io::Error::other)?;
    log_index_memory("secondary_index_merge:complete");
    order_results.push((RunSortOrder::Spot, spot_result));

    let _ = build_start;
    let build_elapsed = overall_start.elapsed().saturating_sub(remap_elapsed);
    tracing::info!(
        orders = order_results.len(),
        build_elapsed_ms = build_elapsed.as_millis(),
        total_elapsed_ms = overall_start.elapsed().as_millis(),
        "build_indexes_from_commits complete"
    );
    log_index_memory("build_indexes_from_commits:complete");

    // Total rows from the POST result (canonical count — avoids double-counting).
    let total_rows = order_results
        .iter()
        .find(|(o, _)| *o == RunSortOrder::Post)
        .map(|(_, r)| r.total_rows)
        .unwrap_or_else(|| {
            order_results
                .first()
                .map(|(_, r)| r.total_rows)
                .unwrap_or(0)
        });

    Ok((
        BuildResult {
            order_results,
            total_rows,
            total_remapped,
            remap_elapsed,
            build_elapsed,
        },
        spot_class_stats,
    ))
}

fn build_spot_index_from_commits(
    commits: &[CommitInput],
    config: &BuildConfig,
    rdf_type_p_id: Option<u32>,
    class_membership: Option<ClassMembership>,
) -> io::Result<(IndexBuildResult, Option<SpotClassStats>)> {
    let g_id = config.g_id;
    let index_dir = &config.index_dir;
    let leaflet_target_rows = config.leaflet_target_rows;
    let leaf_target_rows = config.leaf_target_rows;
    let zstd_level = config.zstd_level;
    let progress = config.build_progress.clone();
    let t0 = Instant::now();
    tracing::info!(
        chunks = commits.len(),
        g_id,
        "starting direct SPOT build from sorted commits"
    );
    log_index_memory("spot_build:start");
    let streams: Vec<SortedCommitMergeReaderV2<MmapSubjectRemap, MmapStringRemap>> = commits
        .iter()
        .map(|commit| {
            let s_remap = MmapSubjectRemap::open(&commit.subject_remap_path)?;
            let str_remap = MmapStringRemap::open(&commit.string_remap_path)?;
            SortedCommitMergeReaderV2::open(
                &commit.commit_path,
                commit.record_count,
                s_remap,
                str_remap,
                commit.lang_remap.clone(),
                g_id,
            )
        })
        .collect::<io::Result<Vec<_>>>()?;

    if streams.is_empty() {
        return Ok((
            IndexBuildResult {
                graphs: Vec::new(),
                total_rows: 0,
                index_dir: index_dir.to_path_buf(),
                elapsed: t0.elapsed(),
            },
            None,
        ));
    }

    let mut merge = KWayMerge::new(streams, cmp_v2_spot)?;
    let order = RunSortOrder::Spot;

    // Streams each completed leaf to disk as produced (see PersistingLeafWriter)
    // so the SPOT build — which overlaps the parallel secondary-order builds —
    // does not retain its whole compressed leaf set in RAM.
    let mut writer = PersistingLeafWriter::new(
        g_id,
        order,
        index_dir,
        leaflet_target_rows,
        leaf_target_rows,
        zstd_level,
    )?;
    writer.set_skip_history(true);

    let mut total_rows = 0u64;
    let mut progress_batch = 0u64;
    let mut class_stats_collector =
        rdf_type_p_id.map(|p_id| SpotClassStatsCollector::new(p_id, class_membership));
    while let Some((record, op)) = merge.next_record()? {
        if op == 0 {
            continue;
        }
        if let Some(ref mut collector) = class_stats_collector {
            collector.on_record(&record);
        }
        writer.push_record(record)?;
        total_rows += 1;
        progress_batch += 1;
        if progress_batch >= PROGRESS_BATCH_SIZE {
            if let Some(ref ctr) = progress {
                ctr.fetch_add(progress_batch, Ordering::Relaxed);
            }
            progress_batch = 0;
        }
    }
    if progress_batch > 0 {
        if let Some(ref ctr) = progress {
            ctr.fetch_add(progress_batch, Ordering::Relaxed);
        }
    }

    let result = writer.finish()?;
    tracing::info!(
        g_id,
        total_rows,
        elapsed_ms = t0.elapsed().as_millis(),
        "direct SPOT build complete"
    );
    log_index_memory("spot_build:complete_before_stats_finish");
    Ok((
        IndexBuildResult {
            graphs: vec![result],
            total_rows,
            index_dir: index_dir.to_path_buf(),
            elapsed: t0.elapsed(),
        },
        class_stats_collector.map(SpotClassStatsCollector::finish),
    ))
}

/// Build V3 indexes from globally-remapped sorted commit files (rebuild path).
///
/// Unlike [`build_indexes_from_commits`], which takes `CommitInput` with
/// per-chunk remap files, this function takes `SortedCommitInfo` entries whose
/// sorted commit files already contain globally-remapped IDs. Uses
/// `IdentitySubjectRemap` / `IdentityStringRemap` since
/// no disk remap files exist.
///
/// Key differences from the import path:
/// - Input: `&[SortedCommitInfo]` (not `&[CommitInput]`)
/// - Remap: identity (global IDs already in place)
/// - `skip_dedup: false` (rebuild may have retractions)
/// - `skip_history: false` (produce history sidecars for time-travel)
pub fn build_indexes_from_remapped_commits(
    commit_infos: &[crate::run_index::runs::spool::SortedCommitInfo],
    registry: &OTypeRegistry,
    config: &BuildConfig,
) -> io::Result<BuildResult> {
    use crate::run_index::runs::spool::{IdentityStringRemap, IdentitySubjectRemap};

    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_REMAP, Ordering::Relaxed);
    }

    // Phase 1: Remap sorted commits → V2 run files (with op) for all 4 orders.
    // Since records are already globally remapped, use identity remap tables.
    // The op byte (assert=1, retract=0) is preserved from V1 records so that
    // the merge/build phase can filter out retract-winners.
    let remap_start = Instant::now();

    let orders = RunSortOrder::all_build_orders().to_vec();
    let mut writer = MultiOrderRunWriterWithOp::new(MultiOrderConfig {
        total_budget_bytes: config.run_budget_bytes,
        orders: orders.clone(),
        base_run_dir: config.run_dir.clone(),
    })?;

    let s_remap = IdentitySubjectRemap;
    let str_remap = IdentityStringRemap;
    let lang_remap: &[u16] = &[]; // language IDs already global

    let mut total_remapped = 0u64;
    for info in commit_infos {
        let count = remap_commit_to_runs_with_op(
            &info.path,
            info.record_count,
            &s_remap,
            &str_remap,
            lang_remap,
            config.g_id,
            registry,
            &mut writer,
        )?;
        total_remapped += count;

        if let Some(ref ctr) = config.remap_progress {
            ctr.fetch_add(count, Ordering::Relaxed);
        }
    }

    let _run_results = writer.finish()?;
    let remap_elapsed = remap_start.elapsed();

    // Phase 2: Build V3 indexes from run files.
    if let Some(stage) = &config.stage_marker {
        stage.store(BUILD_STAGE_MERGE, Ordering::Relaxed);
    }
    let build_start = Instant::now();

    let build_config = BuildAllConfig {
        base_run_dir: config.run_dir.clone(),
        index_dir: config.index_dir.clone(),
        leaflet_target_rows: config.leaflet_target_rows,
        leaf_target_rows: config.leaf_target_rows,
        zstd_level: config.zstd_level,
        skip_dedup: false,   // Rebuild: must deduplicate (max-t wins).
        skip_history: false, // Produce history sidecars for time-travel.
        g_id: config.g_id,
        progress: config.build_progress.clone(),
        // Rebuild path builds all 4 orders here (no separate SPOT thread), so
        // concurrency can cover all of them, bounded by the core budget.
        max_concurrency: config.worker_count,
    };

    let order_results = build_all_indexes(&build_config).map_err(io::Error::other)?;

    let build_elapsed = build_start.elapsed();

    // Total rows from the POST result (canonical count).
    let total_rows = order_results
        .iter()
        .find(|(o, _)| *o == RunSortOrder::Post)
        .map(|(_, r)| r.total_rows)
        .unwrap_or_else(|| {
            order_results
                .first()
                .map(|(_, r)| r.total_rows)
                .unwrap_or(0)
        });

    Ok(BuildResult {
        order_results,
        total_rows,
        total_remapped,
        remap_elapsed,
        build_elapsed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::runs::spool::SortedCommitWriterV2;
    use fluree_db_binary_index::format::leaf::{decode_leaf_dir_v3, decode_leaf_header_v3};
    use fluree_db_binary_index::format::run_record::RunRecord;
    use fluree_db_binary_index::format::run_record_v2::RunRecordV2;

    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};

    #[test]
    fn end_to_end_v3_build() {
        let dir = std::env::temp_dir().join("fluree_test_v3_e2e");
        let _ = std::fs::remove_dir_all(&dir);
        let run_dir = dir.join("runs");
        let index_dir = dir.join("index");
        std::fs::create_dir_all(&run_dir).unwrap();
        std::fs::create_dir_all(&index_dir).unwrap();

        // Create a minimal V2-native sorted commit file with a few records.
        let commit_path = dir.join("commit_00000.fsv2");

        // For this test, we write records directly in sorted order.
        // We need identity remap files too (just identity: index → same value).
        let records = vec![
            RunRecord::new(
                0,
                SubjectId(1),
                1,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(10),
                1,
                true,
                3,
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId(2),
                1,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(20),
                2,
                true,
                3,
                0,
                None,
            ),
            RunRecord::new(
                0,
                SubjectId(3),
                2,
                ObjKind::LEX_ID,
                ObjKey::encode_u32_id(5),
                3,
                true,
                1,
                0,
                None,
            ),
        ];

        let registry = OTypeRegistry::builtin_only();

        // Write V2-native sorted commit file.
        let mut spool = SortedCommitWriterV2::new(&commit_path, 0).unwrap();
        for rec in &records {
            spool.push(&RunRecordV2::from_v1(rec, &registry)).unwrap();
        }
        let spool_info = spool.finish().unwrap();

        // Write identity remap files.
        // Subject remap: 4 entries (0-indexed), identity mapping.
        let subj_remap_path = dir.join("subjects_00000.rmp");
        let subj_data: Vec<u8> = (0u64..4).flat_map(u64::to_le_bytes).collect();
        std::fs::write(&subj_remap_path, &subj_data).unwrap();

        // String remap: 10 entries, identity mapping.
        let str_remap_path = dir.join("strings_00000.rmp");
        let str_data: Vec<u8> = (0u32..10).flat_map(u32::to_le_bytes).collect();
        std::fs::write(&str_remap_path, &str_data).unwrap();

        let commits = vec![CommitInput {
            commit_path,
            record_count: spool_info.record_count,
            subject_remap_path: subj_remap_path,
            string_remap_path: str_remap_path,
            lang_remap: vec![],
            types_map_path: None,
        }];

        let config = BuildConfig {
            run_dir,
            index_dir: index_dir.clone(),
            g_id: 0,
            leaflet_target_rows: 100,
            leaf_target_rows: 1000,
            zstd_level: 1,
            run_budget_bytes: 256 * 1024,
            worker_count: 1,
            remap_progress: None,
            build_progress: None,
            stage_marker: None,
        };

        let (result, _spot_class_stats) =
            build_indexes_from_commits(&commits, &config, None).unwrap();

        // Should have results for all 4 orders.
        assert_eq!(result.order_results.len(), 4);
        assert_eq!(result.total_rows, 3);
        assert_eq!(result.total_remapped, 3);

        // Verify POST has predicate-homogeneous leaflets.
        let post_result = result
            .order_results
            .iter()
            .find(|(o, _)| *o == RunSortOrder::Post)
            .unwrap();
        let post_graphs = &post_result.1.graphs;
        assert_eq!(post_graphs.len(), 1);
        assert_eq!(post_graphs[0].g_id, 0);

        // Check the POST leaf has 2 leaflets (p_id=1 and p_id=2).
        let post_leaf = &post_graphs[0].leaf_infos[0];
        let leaf_bytes = std::fs::read(&post_leaf.leaf_path).unwrap();
        let header = decode_leaf_header_v3(&leaf_bytes).unwrap();
        assert_eq!(header.order, RunSortOrder::Post);
        let leaf_dir = decode_leaf_dir_v3(&leaf_bytes, &header).unwrap();
        assert_eq!(leaf_dir.len(), 2); // p_id=1 and p_id=2
        assert_eq!(leaf_dir[0].p_const, Some(1));
        assert_eq!(leaf_dir[1].p_const, Some(2));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Helper: write `.types` sidecar entries (g_id: u16, s_id: u64, class_sid64: u64).
    fn write_types_sidecar(path: &std::path::Path, entries: &[(u16, u64, u64)]) {
        use std::io::Write;
        let mut file = std::fs::File::create(path).unwrap();
        for (g_id, s_id, c_id) in entries {
            file.write_all(&g_id.to_le_bytes()).unwrap();
            file.write_all(&s_id.to_le_bytes()).unwrap();
            file.write_all(&c_id.to_le_bytes()).unwrap();
        }
    }

    fn write_identity_subject_remap(path: &std::path::Path, len: u64) {
        let bytes: Vec<u8> = (0..len).flat_map(u64::to_le_bytes).collect();
        std::fs::write(path, bytes).unwrap();
    }

    fn write_identity_string_remap(path: &std::path::Path, len: u32) {
        let bytes: Vec<u8> = (0..len).flat_map(u32::to_le_bytes).collect();
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn import_class_membership_uses_disk_backing() {
        let dir = tempfile::tempdir().unwrap();
        let types0 = dir.path().join("chunk_00000.types");
        let types1 = dir.path().join("chunk_00001.types");
        let subj0 = dir.path().join("subjects_00000.rmp");
        let subj1 = dir.path().join("subjects_00001.rmp");
        let str0 = dir.path().join("strings_00000.rmp");
        let str1 = dir.path().join("strings_00001.rmp");

        write_identity_subject_remap(&subj0, 6000);
        write_identity_subject_remap(&subj1, 6000);
        write_identity_string_remap(&str0, 1);
        write_identity_string_remap(&str1, 1);

        // Duplicate classes for subject 42 should dedupe; separate chunks should
        // merge into the same disk-backed lookup table.
        write_types_sidecar(&types0, &[(0, 42, 1000), (0, 42, 1000), (0, 42, 2000)]);
        write_types_sidecar(&types1, &[(0, 77, 3000), (1, 42, 4000)]);

        let commits = vec![
            CommitInput {
                commit_path: dir.path().join("unused0.fsv2"),
                record_count: 0,
                subject_remap_path: subj0,
                string_remap_path: str0,
                lang_remap: vec![],
                types_map_path: Some(types0),
            },
            CommitInput {
                commit_path: dir.path().join("unused1.fsv2"),
                record_count: 0,
                subject_remap_path: subj1,
                string_remap_path: str1,
                lang_remap: vec![],
                types_map_path: Some(types1),
            },
        ];

        let membership =
            ClassMembership::build_from_commits(&commits, &dir.path().join("membership"))
                .unwrap()
                .expect("membership");
        assert!(matches!(membership, ClassMembership::Disk(_)));
        assert_eq!(membership.summary().1, 4);
        assert_eq!(membership.summary().2, 3);

        let mut buf = Vec::new();
        membership.collect_classes(0, 42, &mut buf);
        assert_eq!(buf, vec![1000, 2000]);
        membership.collect_classes(0, 77, &mut buf);
        assert_eq!(buf, vec![3000]);
        membership.collect_classes(1, 42, &mut buf);
        assert_eq!(buf, vec![4000]);
        membership.collect_classes(0, 999, &mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn class_membership_bitset_for_few_classes() {
        let dir = tempfile::tempdir().unwrap();
        let types_path = dir.path().join("chunk_00000.types");

        // Subject 100 is class A (sid=1000); subject 200 is class A and class B (sid=2000).
        let class_a: u64 = 1000;
        let class_b: u64 = 2000;
        write_types_sidecar(
            &types_path,
            &[(0, 100, class_a), (0, 200, class_a), (0, 200, class_b)],
        );

        let membership = ClassMembership::build_from_global_types(&[types_path])
            .unwrap()
            .expect("should produce membership");

        // ≤ 64 classes ⇒ dense bitset fast path.
        let ClassMembership::Bitset(table) = &membership else {
            panic!("expected bitset variant for ≤ 64 classes");
        };
        assert_eq!(membership.summary().1, 2);
        assert!(table.bit_to_class.contains(&class_a));
        assert!(table.bit_to_class.contains(&class_b));

        let mut buf = Vec::new();
        // Subject 100: only class A.
        membership.collect_classes(0, 100, &mut buf);
        assert_eq!(buf, vec![class_a]);
        // Subject 200: both classes (collect_classes order follows bit order).
        membership.collect_classes(0, 200, &mut buf);
        buf.sort_unstable();
        assert_eq!(buf, vec![class_a, class_b]);
        // Unknown subject / graph ⇒ empty.
        membership.collect_classes(0, 999, &mut buf);
        assert!(buf.is_empty());
        membership.collect_classes(5, 100, &mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn class_membership_promotes_to_sparse_above_64() {
        let dir = tempfile::tempdir().unwrap();
        let types_path = dir.path().join("overflow.types");

        // 65 distinct classes, one unique subject each. The old bitset capped at
        // 64 and dropped the 65th; the sparse promotion must retain ALL of them.
        let mut entries: Vec<(u16, u64, u64)> = Vec::new();
        for class_idx in 0u64..65 {
            entries.push((0, class_idx + 1, 10_000 + class_idx));
        }
        write_types_sidecar(&types_path, &entries);

        let membership = ClassMembership::build_from_global_types(&[types_path])
            .unwrap()
            .expect("should produce membership");

        assert!(
            matches!(membership, ClassMembership::Sparse(_)),
            "should promote to sparse above 64 classes"
        );
        assert_eq!(membership.summary().1, 65);

        // Every subject (including the 65th, which the old code truncated) maps
        // to exactly its class.
        let mut buf = Vec::new();
        for class_idx in 0u64..65 {
            let s_id = class_idx + 1;
            let c_id = 10_000 + class_idx;
            membership.collect_classes(0, s_id, &mut buf);
            assert_eq!(buf, vec![c_id], "subject {s_id} should map to class {c_id}");
        }
        membership.collect_classes(0, 9999, &mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn class_membership_promotion_carries_early_bitset_rows() {
        let dir = tempfile::tempdir().unwrap();
        let types_path = dir.path().join("carry.types");

        // Subject 1 gets an early class (bitset mode), then 64 more distinct
        // classes force promotion, then subject 1 gets another class (sparse
        // mode). After promotion the early bitset bit must be carried over so
        // subject 1 ends up with BOTH classes.
        let mut entries: Vec<(u16, u64, u64)> = vec![(0, 1, 500)];
        for class_idx in 0u64..64 {
            entries.push((0, 1000 + class_idx, 20_000 + class_idx));
        }
        entries.push((0, 1, 999)); // subject 1's second class, added in sparse mode
        write_types_sidecar(&types_path, &entries);

        let membership = ClassMembership::build_from_global_types(&[types_path])
            .unwrap()
            .expect("should produce membership");
        assert!(matches!(membership, ClassMembership::Sparse(_)));

        let mut buf = Vec::new();
        membership.collect_classes(0, 1, &mut buf);
        buf.sort_unstable();
        assert_eq!(
            buf,
            vec![500, 999],
            "promotion must carry the early bitset class (500) plus the sparse-mode class (999)"
        );
    }

    #[test]
    fn class_membership_no_types_files_returns_none() {
        let result = ClassMembership::build_from_global_types(&[]).unwrap();
        assert!(result.is_none());
    }
}
