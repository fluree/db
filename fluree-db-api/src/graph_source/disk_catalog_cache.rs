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

/// On-disk value-schema version. Content-addressing the KEY (by
/// `metadata_location`) guarantees a stale table never returns old data, but it
/// does NOT protect against the VALUE layout changing across releases: a future
/// field added to [`DataFile`] (or these persisted structs) could silently
/// misread an old entry (a defaulted field) instead of refetching. **BUMP THIS
/// whenever any persisted payload type changes** — an entry whose stored version
/// differs is dropped and refetched.
const CACHE_FORMAT_VERSION: u32 = 1;

/// Versioned on-disk envelope. The version is checked before the payload is
/// trusted; a mismatch (or any deserialize failure) is a miss, never an error.
#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    format_version: u32,
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
    fn read<T: for<'de> Deserialize<'de>>(&self, path: &Path) -> Option<T> {
        let bytes = std::fs::read(path).ok()?;
        let env: Envelope<T> = match serde_json::from_slice(&bytes) {
            Ok(e) => e,
            Err(_) => {
                let _ = std::fs::remove_file(path);
                return None;
            }
        };
        if env.format_version != CACHE_FORMAT_VERSION {
            let _ = std::fs::remove_file(path);
            return None;
        }
        Some(env.payload)
    }

    /// Write an entry via temp-file + atomic rename, so a crash mid-write can't
    /// leave a torn file a later read would trust (a torn temp is just orphaned).
    /// Best-effort: any failure just means a future miss.
    fn write<T: Serialize>(&self, path: &Path, value: &T) {
        let env = Envelope {
            format_version: CACHE_FORMAT_VERSION,
            payload: value,
        };
        let Ok(bytes) = serde_json::to_vec(&env) else {
            return;
        };
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, &bytes).is_ok() && std::fs::rename(&tmp, path).is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
    }

    pub(crate) fn get_metadata(&self, metadata_location: &str) -> Option<Arc<TableMetadata>> {
        if !self.enabled {
            return None;
        }
        self.read::<TableMetadata>(&self.path(metadata_location, "metadata"))
            .map(Arc::new)
    }

    pub(crate) fn put_metadata(&self, metadata_location: &str, metadata: &TableMetadata) {
        if !self.enabled {
            return;
        }
        self.write(&self.path(metadata_location, "metadata"), metadata);
    }

    pub(crate) fn get_scan_files(&self, metadata_location: &str) -> Option<Arc<CachedScanFiles>> {
        if !self.enabled {
            return None;
        }
        let p: PersistedScanFiles = self.read(&self.path(metadata_location, "scanfiles"))?;
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
        self.write(&self.path(metadata_location, "scanfiles"), &p);
    }

    pub(crate) fn get_count_stats(&self, metadata_location: &str) -> Option<(Vec<DataFile>, bool)> {
        if !self.enabled {
            return None;
        }
        let p: PersistedCountStats = self.read(&self.path(metadata_location, "countstats"))?;
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
        self.write(&self.path(metadata_location, "countstats"), &p);
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
    fn disabled_cache_is_always_a_miss() {
        // A dir that cannot be created (a path under a file) disables the cache.
        let file = tmp_dir("asfile");
        std::fs::write(&file, b"x").ok();
        let cache = DiskCatalogCache::for_dir(&file.join("child"));
        cache.put_count_stats("s3://b/m.json", &[data_file("s3://b/f.parquet", 1)], false);
        assert!(cache.get_count_stats("s3://b/m.json").is_none());
    }
}
