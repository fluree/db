//! On-disk, content-addressed catalog cache (PR-8 slice 2).
//!
//! Persists the SECRET-FREE, IMMUTABLE catalog layers across process restarts:
//! parsed [`TableMetadata`], the manifest-derived scan file list
//! ([`CachedScanFiles`]), and the `COUNT(*)` manifest stats — all keyed by the
//! `metadata_location` (a content-addressed S3 path, so a given key's value can
//! never go stale; a table commit yields a NEW location = a NEW key = a clean
//! miss, no TTL or invalidation logic needed). **No credentials or tokens are
//! persisted:** a cold process still issues one `loadTable` GET for fresh vended
//! credentials — this only removes the metadata + manifest S3 round-trips that
//! follow it.
//!
//! Stored in a **dedicated directory**, a sibling of the binary-index / Parquet
//! [`fluree_db_iceberg::DiskArtifactCache`] (never inside it), so the cold
//! benchmark protocol can clear the data artifact cache while KEEPING catalog
//! persistence — that "cold-data / warm-catalog" state is slice 2's DoD gate.

use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fluree_db_iceberg::metadata::TableMetadata;
use fluree_db_iceberg::DataFile;
use serde::{Deserialize, Serialize};

use super::cache::CachedScanFiles;

/// Master switch (defaults on). `0`/`false`/`off`/`no` disables all disk-catalog
/// read/write, restoring the "every cold process re-reads metadata + manifests
/// from S3" behavior. Read once, cached for the process.
pub(crate) fn disk_catalog_cache_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(
        || match std::env::var("FLUREE_ICEBERG_CATALOG_DISK_CACHE") {
            Ok(v) => !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            ),
            Err(_) => true,
        },
    )
}

/// Own switch for the loadTable `metadata_location` POINTER cache (default on),
/// gated ADDITIONALLY by the master [`disk_catalog_cache_enabled`] and a positive
/// TTL. Off restores "every cold process issues a loadTable GET for the pointer".
/// `FLUREE_ICEBERG_LOADTABLE_PTR_CACHE`. Read once, cached for the process.
pub(crate) fn loadtable_ptr_cache_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(
        || match std::env::var("FLUREE_ICEBERG_LOADTABLE_PTR_CACHE") {
            Ok(v) => !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            ),
            Err(_) => true,
        },
    )
}

/// TTL (seconds) for the persisted `lt_key → metadata_location` pointer. Unlike
/// the content-addressed entries (whose key IS the immutable snapshot, so they can
/// never go stale), the pointer CAN go stale — a table commit moves it — so it
/// carries a freshness bound: **the max cross-process snapshot staleness for a
/// latest-snapshot REST read.** The same tradeoff class the 60s in-memory
/// cross-query cache already accepts, extended deliberately and consistent with
/// the disk-cache-is-steady-state ruling. Default 300s; **`0` disables pointer
/// persistence entirely.** `FLUREE_ICEBERG_LOADTABLE_PTR_TTL_SECS`. Read once.
pub(crate) fn loadtable_ptr_ttl_secs() -> u64 {
    use std::sync::OnceLock;
    static TTL: OnceLock<u64> = OnceLock::new();
    *TTL.get_or_init(|| {
        std::env::var("FLUREE_ICEBERG_LOADTABLE_PTR_TTL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(300)
    })
}

/// Wall-clock milliseconds since the Unix epoch (saturating; a pre-epoch clock
/// reads 0, which only makes a pointer look older = a conservative miss).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The dedicated catalog-cache directory sibling to the Parquet/binary artifact
/// dir `artifact_dir`: same parent, name suffixed `-catalog`. A sibling (not a
/// child) so clearing the artifact dir — the cold protocol's data clear — leaves
/// catalog persistence intact.
pub(crate) fn catalog_cache_dir(artifact_dir: &Path) -> PathBuf {
    let name = artifact_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("fluree_cache");
    let mut dir = artifact_dir.to_path_buf();
    dir.set_file_name(format!("{name}-catalog"));
    dir
}

/// Delete the oldest entries (by mtime) until `dir` is under [`MAX_CACHE_BYTES`].
/// Best-effort: any stat/remove failure is ignored. Called once per process from
/// [`DiskCatalogCache::for_dir`].
fn prune_dir(dir: &Path) {
    let Ok(read_dir) = std::fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<(PathBuf, u64, std::time::SystemTime)> = Vec::new();
    let mut total: u64 = 0;
    for entry in read_dir.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let mtime = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        total += meta.len();
        entries.push((entry.path(), meta.len(), mtime));
    }
    if total <= MAX_CACHE_BYTES {
        return;
    }
    entries.sort_by_key(|(_, _, mtime)| *mtime); // oldest first
    for (path, size, _) in entries {
        if total <= MAX_CACHE_BYTES {
            break;
        }
        if std::fs::remove_file(&path).is_ok() {
            total = total.saturating_sub(size);
        }
    }
}

/// On-disk form of [`CachedScanFiles`] — a plain `Vec` (serde's `rc` feature is
/// off, so `Arc` can't derive `Serialize`; the loader re-wraps in `Arc`).
#[derive(Serialize, Deserialize)]
struct PersistedScanFiles {
    data_files: Vec<DataFile>,
    estimated_row_count: i64,
    files_selected: usize,
    files_pruned: usize,
}

/// On-disk form of the `COUNT(*)` manifest read
/// (`send_read_snapshot_data_files`): the live data files (carrying
/// `record_count`) and whether the snapshot has merge-on-read delete manifests.
#[derive(Serialize, Deserialize)]
struct PersistedCountStats {
    data_files: Vec<DataFile>,
    has_delete_manifests: bool,
}

/// On-disk form of the loadTable `metadata_location` POINTER, keyed by `lt_key`
/// (graph source + namespace + table) rather than the content-addressed
/// `metadata_location` — because the pointer's whole job is to RESOLVE that
/// location without a REST GET. **CREDENTIAL-FREE by construction:** the only S3
/// path here is the immutable, non-secret `metadata_location`; no vended
/// credential, OAuth token, or catalog config is a field, so none can be
/// persisted (the AJ hard constraint, enforced structurally + asserted by
/// `pointer_persists_no_credential_bytes`).
#[derive(Serialize, Deserialize)]
struct PersistedMetadataPointer {
    /// The immutable `metadata_location` (an `s3://…` content-addressed path).
    metadata_location: String,
    /// The current snapshot's `timestamp_ms` at cache time — the `as_of_t` rider
    /// compares a time-travel request against this (see [`pointer_is_usable`]).
    snapshot_ms: i64,
    /// Wall-clock ms when cached — the TTL freshness bound is measured from here.
    cached_at_ms: u64,
}

/// Whether a persisted pointer may be served for a request, given the current
/// time, the TTL, and an optional minimum-snapshot bound. Pure (no I/O, no env,
/// no clock) so the TTL + `as_of_t`-rider logic is unit-testable without racing
/// the process-wide env/clock. A pointer is usable iff BOTH hold:
/// - **fresh**: `now_ms - cached_at_ms ≤ ttl_ms` (bounded staleness); and
/// - **rider**: `min_snapshot_ms` is `None` (a latest-snapshot read) OR the cached
///   snapshot is at-or-after it (`snapshot_ms ≥ min`). A time-travel query asking
///   for a snapshot NEWER than the cached one is NOT served — it must force a
///   fresh GET, so bounded staleness can never answer from an older snapshot than
///   requested (the lead's non-negotiable correctness rider).
fn pointer_is_usable(
    p: &PersistedMetadataPointer,
    now_ms: u64,
    ttl_ms: u64,
    min_snapshot_ms: Option<i64>,
) -> bool {
    let fresh = now_ms.saturating_sub(p.cached_at_ms) <= ttl_ms;
    let rider_ok = min_snapshot_ms.is_none_or(|min| p.snapshot_ms >= min);
    fresh && rider_ok
}

/// On-disk value-schema version. Content-addressing the KEY (by
/// `metadata_location`) guarantees a stale table never returns old data, but it
/// does NOT protect against the VALUE layout changing across releases: a future
/// field added to [`DataFile`] (or these persisted structs) could silently
/// misread an old entry (a defaulted field) instead of refetching. **BUMP THIS
/// whenever any persisted payload type changes** — an entry whose stored version
/// differs is dropped and refetched.
const CACHE_FORMAT_VERSION: u32 = 2;

/// Versioned on-disk envelope. The version is checked before the payload is
/// trusted; a mismatch (or any deserialize failure) is a miss, never an error.
#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    format_version: u32,
    /// The FULL cache key (an `s3://…` metadata_location, or an `lt_key`). The
    /// filename stem is only a 64-bit `DefaultHasher` of the key, so two distinct
    /// keys can collide onto one path; verifying the stored key on read turns a
    /// collision into a clean miss instead of silently serving another entry's
    /// payload — a wrong *table*'s metadata for the pointer entry (#1491/#1503
    /// review). Hash quality is then irrelevant to correctness.
    key: String,
    payload: T,
}

/// Total-size cap for the catalog cache dir. Metadata entries are small, but a
/// ~7,670-file table's `scan_files` entry is non-trivial, so an unbounded dir in
/// `~/.fluree` would eventually be a support ticket. Pruned oldest-first at
/// process startup (see [`DiskCatalogCache::for_dir`]).
const MAX_CACHE_BYTES: u64 = 512 * 1024 * 1024;

/// Content-addressed on-disk catalog cache. A pure optimization: any I/O, parse,
/// or version failure degrades to a miss (the caller reads from S3), never an
/// error.
pub(crate) struct DiskCatalogCache {
    dir: PathBuf,
    enabled: bool,
}

impl DiskCatalogCache {
    /// Open (creating if needed) a catalog cache rooted at `dir`. If the switch is
    /// off or the dir can't be created, returns a disabled cache whose every op is
    /// a no-op miss. Prunes the dir to [`MAX_CACHE_BYTES`] ONCE per process (the
    /// first call), oldest-first — this is called per-query, but the prune runs
    /// only at startup.
    pub(crate) fn for_dir(dir: &Path) -> Self {
        let enabled = disk_catalog_cache_enabled() && std::fs::create_dir_all(dir).is_ok();
        if enabled {
            use std::sync::OnceLock;
            static PRUNED: OnceLock<()> = OnceLock::new();
            PRUNED.get_or_init(|| prune_dir(dir));
        }
        Self {
            dir: dir.to_path_buf(),
            enabled,
        }
    }

    /// File path for `metadata_location`'s `suffix` entry. The location is an
    /// `s3://…` path; hash it to a filesystem-safe, fixed-length stem.
    fn path(&self, metadata_location: &str, suffix: &str) -> PathBuf {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        metadata_location.hash(&mut h);
        self.dir.join(format!("{:016x}.{suffix}.json", h.finish()))
    }

    /// Read + version-check an entry. A deserialize failure (corrupt, truncated by
    /// a crash mid-write, or an old value layout) OR a version mismatch is a miss;
    /// a stale-version file is deleted so it stops occupying the cap.
    fn read<T: for<'de> Deserialize<'de>>(&self, key: &str, suffix: &str) -> Option<T> {
        let path = self.path(key, suffix);
        let bytes = std::fs::read(&path).ok()?;
        let env: Envelope<T> = match serde_json::from_slice(&bytes) {
            Ok(e) => e,
            Err(_) => {
                let _ = std::fs::remove_file(&path);
                return None;
            }
        };
        if env.format_version != CACHE_FORMAT_VERSION {
            let _ = std::fs::remove_file(&path);
            return None;
        }
        // Hash-collision guard: the filename is only a 64-bit hash of `key`, so a
        // colliding entry deserializes but carries a DIFFERENT key. Verify it and
        // treat a mismatch as a miss (do NOT delete — the entry is valid for its
        // own key, which a future read under that key will want).
        if env.key != key {
            return None;
        }
        Some(env.payload)
    }

    /// Write an entry via temp-file + atomic rename, so a crash mid-write can't
    /// leave a torn file a later read would trust (a torn temp is just orphaned).
    /// Best-effort: any failure just means a future miss.
    fn write<T: Serialize>(&self, key: &str, suffix: &str, value: &T) {
        let path = self.path(key, suffix);
        let env = Envelope {
            format_version: CACHE_FORMAT_VERSION,
            key: key.to_string(),
            payload: value,
        };
        let Ok(bytes) = serde_json::to_vec(&env) else {
            return;
        };
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, &bytes).is_ok() && std::fs::rename(&tmp, &path).is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
    }

    pub(crate) fn get_metadata(&self, metadata_location: &str) -> Option<Arc<TableMetadata>> {
        if !self.enabled {
            return None;
        }
        self.read::<TableMetadata>(metadata_location, "metadata")
            .map(Arc::new)
    }

    pub(crate) fn put_metadata(&self, metadata_location: &str, metadata: &TableMetadata) {
        if !self.enabled {
            return;
        }
        self.write(metadata_location, "metadata", metadata);
    }

    pub(crate) fn get_scan_files(&self, metadata_location: &str) -> Option<Arc<CachedScanFiles>> {
        if !self.enabled {
            return None;
        }
        let p: PersistedScanFiles = self.read(metadata_location, "scanfiles")?;
        Some(Arc::new(CachedScanFiles {
            data_files: Arc::new(p.data_files),
            estimated_row_count: p.estimated_row_count,
            files_selected: p.files_selected,
            files_pruned: p.files_pruned,
        }))
    }

    pub(crate) fn put_scan_files(&self, metadata_location: &str, sf: &CachedScanFiles) {
        if !self.enabled {
            return;
        }
        let p = PersistedScanFiles {
            data_files: (*sf.data_files).clone(),
            estimated_row_count: sf.estimated_row_count,
            files_selected: sf.files_selected,
            files_pruned: sf.files_pruned,
        };
        self.write(metadata_location, "scanfiles", &p);
    }

    pub(crate) fn get_count_stats(&self, metadata_location: &str) -> Option<(Vec<DataFile>, bool)> {
        if !self.enabled {
            return None;
        }
        let p: PersistedCountStats = self.read(metadata_location, "countstats")?;
        Some((p.data_files, p.has_delete_manifests))
    }

    pub(crate) fn put_count_stats(
        &self,
        metadata_location: &str,
        data_files: &[DataFile],
        has_delete_manifests: bool,
    ) {
        if !self.enabled {
            return;
        }
        let p = PersistedCountStats {
            data_files: data_files.to_vec(),
            has_delete_manifests,
        };
        self.write(metadata_location, "countstats", &p);
    }

    /// Resolve the persisted `metadata_location` for `lt_key` WITHOUT a REST GET,
    /// or `None` (a miss ⇒ the caller does the GET). Gated by the master switch,
    /// the pointer switch, and a positive TTL; then the freshness + `as_of_t`-rider
    /// check ([`pointer_is_usable`]). `min_snapshot_ms` is the caller's
    /// requested-snapshot lower bound (`None` = latest, the current default; a
    /// future time-travel read passes the requested `timestamp_ms`). An expired or
    /// rider-rejected entry is a miss; an expired one is also deleted so it stops
    /// occupying the cap.
    pub(crate) fn get_metadata_location(
        &self,
        lt_key: &str,
        min_snapshot_ms: Option<i64>,
    ) -> Option<String> {
        let ttl_secs = loadtable_ptr_ttl_secs();
        if !self.enabled || !loadtable_ptr_cache_enabled() || ttl_secs == 0 {
            return None;
        }
        let p: PersistedMetadataPointer = self.read(lt_key, "pointer")?;
        let ttl_ms = ttl_secs.saturating_mul(1000);
        if pointer_is_usable(&p, now_ms(), ttl_ms, min_snapshot_ms) {
            Some(p.metadata_location)
        } else {
            // A TTL-expired entry is dead weight; drop it. (A rider miss keeps the
            // entry — it is still valid for a latest-snapshot read.)
            if now_ms().saturating_sub(p.cached_at_ms) > ttl_ms {
                let _ = std::fs::remove_file(self.path(lt_key, "pointer"));
            }
            None
        }
    }

    /// Persist the credential-free `lt_key → metadata_location` pointer with the
    /// current snapshot's `timestamp_ms` (for the rider) and the cache time (for
    /// the TTL). No-op when disabled or TTL=0.
    pub(crate) fn put_metadata_location(
        &self,
        lt_key: &str,
        metadata_location: &str,
        snapshot_ms: i64,
    ) {
        if !self.enabled || !loadtable_ptr_cache_enabled() || loadtable_ptr_ttl_secs() == 0 {
            return;
        }
        let p = PersistedMetadataPointer {
            metadata_location: metadata_location.to_string(),
            snapshot_ms,
            cached_at_ms: now_ms(),
        };
        self.write(lt_key, "pointer", &p);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> PathBuf {
        let d =
            std::env::temp_dir().join(format!("fluree-catcache-test-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    fn data_file(path: &str, rows: i64) -> DataFile {
        DataFile {
            file_path: path.to_string(),
            file_format: fluree_db_iceberg::manifest::FileFormat::Parquet,
            record_count: rows,
            file_size_in_bytes: 1024,
            partition: fluree_db_iceberg::manifest::PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            split_offsets: None,
            sort_order_id: None,
        }
    }

    /// The single `.json` entry the cache wrote under `dir` (test helper).
    fn only_entry(dir: &Path) -> PathBuf {
        std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.extension().and_then(|x| x.to_str()) == Some("json"))
            .expect("one cache entry")
    }

    #[test]
    fn corrupt_entry_is_a_miss() {
        let dir = tmp_dir("corrupt");
        let cache = DiskCatalogCache::for_dir(&dir);
        let loc = "s3://b/m.json";
        cache.put_count_stats(loc, &[data_file("s3://b/f.parquet", 1)], false);
        assert!(cache.get_count_stats(loc).is_some(), "valid entry hits");
        // Simulate a torn/garbage file (e.g. a crash mid-write on a non-atomic FS).
        std::fs::write(only_entry(&dir), b"{ not valid json").unwrap();
        assert!(
            cache.get_count_stats(loc).is_none(),
            "a corrupt entry is a miss, never a surfaced error"
        );
    }

    #[test]
    fn version_mismatch_is_a_miss() {
        let dir = tmp_dir("version");
        let cache = DiskCatalogCache::for_dir(&dir);
        let loc = "s3://b/m.json";
        cache.put_count_stats(loc, &[data_file("s3://b/f.parquet", 1)], false);
        // Rewrite the envelope with a bumped version, payload untouched — models a
        // future release whose value schema changed.
        let path = only_entry(&dir);
        let mut v: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        v["format_version"] = serde_json::json!(CACHE_FORMAT_VERSION + 1);
        std::fs::write(&path, serde_json::to_vec(&v).unwrap()).unwrap();
        assert!(
            cache.get_count_stats(loc).is_none(),
            "a version-mismatched entry is dropped and refetched, never misread"
        );
    }

    #[test]
    fn scan_files_round_trip_by_metadata_location() {
        let cache = DiskCatalogCache::for_dir(&tmp_dir("scanfiles"));
        let loc = "s3://bucket/warehouse/t/metadata/00042-abc.metadata.json";
        assert!(cache.get_scan_files(loc).is_none(), "empty is a miss");
        let sf = CachedScanFiles {
            data_files: Arc::new(vec![
                data_file("s3://b/f1.parquet", 23),
                data_file("s3://b/f2.parquet", 7),
            ]),
            estimated_row_count: 30,
            files_selected: 2,
            files_pruned: 5,
        };
        cache.put_scan_files(loc, &sf);
        let got = cache.get_scan_files(loc).expect("hit after put");
        assert_eq!(got.data_files.len(), 2);
        assert_eq!(got.estimated_row_count, 30);
        assert_eq!(got.files_selected, 2);
        assert_eq!(got.files_pruned, 5);
        assert_eq!(got.data_files[0].record_count, 23);
        // A different (content-addressed) location is a clean miss.
        assert!(cache
            .get_scan_files("s3://bucket/warehouse/t/metadata/00043-def.metadata.json")
            .is_none());
    }

    #[test]
    fn count_stats_round_trip() {
        let cache = DiskCatalogCache::for_dir(&tmp_dir("countstats"));
        let loc = "s3://bucket/t/metadata/00001-x.metadata.json";
        assert!(cache.get_count_stats(loc).is_none());
        cache.put_count_stats(loc, &[data_file("s3://b/a.parquet", 100)], true);
        let (files, has_deletes) = cache.get_count_stats(loc).expect("hit");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].record_count, 100);
        assert!(has_deletes);
    }

    #[test]
    fn collided_entry_carrying_another_key_is_a_miss() {
        // Hash-collision guard (#1491/#1503 review): the filename is only a 64-bit
        // DefaultHasher of the key, so two distinct keys can collide onto one path. A
        // read must verify the stored FULL key and MISS on a mismatch — never serve
        // another table's payload. Simulate the collision deterministically by
        // placing entry A's stored file at B's path.
        let cache = DiskCatalogCache::for_dir(&tmp_dir("collision"));
        let a = "s3://bucket/TABLE_A/metadata/00001-a.metadata.json";
        let b = "s3://bucket/TABLE_B/metadata/00002-b.metadata.json";
        cache.put_count_stats(a, &[data_file("s3://b/a.parquet", 100)], true);
        std::fs::copy(cache.path(a, "countstats"), cache.path(b, "countstats")).unwrap();
        assert!(
            cache.get_count_stats(b).is_none(),
            "a collided entry whose stored key mismatches is a clean miss, not a misread"
        );
        assert!(
            cache.get_count_stats(a).is_some(),
            "A's own entry still hits"
        );
    }

    #[test]
    fn metadata_location_pointer_round_trip() {
        let cache = DiskCatalogCache::for_dir(&tmp_dir("pointer"));
        let lt_key = "gs:enterprise::DW_SF01::FACT_INVENTORY_SNAPSHOT";
        let loc = "s3://bucket/warehouse/t/metadata/00042-abc.metadata.json";
        assert!(
            cache.get_metadata_location(lt_key, None).is_none(),
            "empty is a miss"
        );
        cache.put_metadata_location(lt_key, loc, 1_700_000_000_000);
        assert_eq!(
            cache.get_metadata_location(lt_key, None).as_deref(),
            Some(loc),
            "latest-snapshot read hits the persisted pointer"
        );
        assert!(
            cache
                .get_metadata_location("gs:enterprise::DW_SF01::OTHER_TABLE", None)
                .is_none(),
            "a different lt_key is a clean miss"
        );
    }

    #[test]
    fn pointer_ttl_and_as_of_rider() {
        // Pure freshness + rider logic, independent of the process env/clock.
        let p = PersistedMetadataPointer {
            metadata_location: "s3://b/m.json".to_string(),
            snapshot_ms: 100,
            cached_at_ms: 1_000_000,
        };
        let ttl_ms = 300_000;
        // Fresh + latest read (no bound) ⇒ usable.
        assert!(pointer_is_usable(&p, 1_100_000, ttl_ms, None));
        // Past the TTL ⇒ NOT usable (bounded staleness).
        assert!(!pointer_is_usable(&p, 1_000_000 + ttl_ms + 1, ttl_ms, None));
        // as_of_t rider: a request for a snapshot NEWER than cached (min > 100) ⇒
        // NOT usable, must force a fresh GET.
        assert!(!pointer_is_usable(&p, 1_100_000, ttl_ms, Some(150)));
        // A request satisfiable by the cached snapshot (min ≤ 100) ⇒ usable.
        assert!(pointer_is_usable(&p, 1_100_000, ttl_ms, Some(100)));
        assert!(pointer_is_usable(&p, 1_100_000, ttl_ms, Some(50)));
        // Rider is checked AND-wise with freshness: newer-request on a stale entry
        // is still a miss.
        assert!(!pointer_is_usable(&p, 1_000_000 + ttl_ms + 1, ttl_ms, Some(50)));
    }

    #[test]
    fn pointer_persists_no_credential_bytes() {
        // The AJ hard constraint, asserted structurally: the on-disk pointer entry
        // contains ONLY the location + timestamps — never a vended credential,
        // token, secret, or catalog config.
        let dir = tmp_dir("pointer-nocreds");
        let cache = DiskCatalogCache::for_dir(&dir);
        cache.put_metadata_location(
            "gs:x::ns::tbl",
            "s3://bucket/warehouse/t/metadata/00001-x.metadata.json",
            1_700_000_000_000,
        );
        let bytes = std::fs::read(only_entry(&dir)).expect("pointer entry written");
        let text = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
        for forbidden in [
            "credential",
            "token",
            "secret",
            "access_key",
            "session",
            "password",
            "sig=",
        ] {
            assert!(
                !text.contains(forbidden),
                "pointer payload must not persist `{forbidden}`: {text}"
            );
        }
        // And the payload envelope carries exactly the three expected fields.
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let payload = &v["payload"];
        assert!(payload["metadata_location"].is_string());
        assert!(payload["snapshot_ms"].is_number());
        assert!(payload["cached_at_ms"].is_number());
        assert_eq!(
            payload.as_object().unwrap().len(),
            3,
            "exactly {{metadata_location, snapshot_ms, cached_at_ms}} — no extra fields"
        );
    }

    #[test]
    fn disabled_cache_is_always_a_miss() {
        // A dir that cannot be created (a path under a file) disables the cache.
        let file = tmp_dir("asfile");
        std::fs::write(&file, b"x").ok();
        let cache = DiskCatalogCache::for_dir(&file.join("child"));
        cache.put_count_stats("s3://b/m.json", &[data_file("s3://b/f.parquet", 1)], false);
        assert!(cache.get_count_stats("s3://b/m.json").is_none());
    }
}
