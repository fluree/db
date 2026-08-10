//! Local-filesystem storage backend for Iceberg tables.
//!
//! Reads Iceberg tables that live on the local filesystem — the catalog-less
//! local workflow: write a table with pyiceberg/Spark into a local directory,
//! point a `Direct { table_location: "file:///..." }` graph source at it, and
//! query it with zero services (no REST catalog, no object store).
//!
//! Paths are resolved as the metadata wrote them: `file:///abs/path` URIs
//! (what pyiceberg emits for a local warehouse) and bare absolute paths are
//! accepted verbatim. A table that was COPIED or MOVED — whose manifests still
//! reference the original location (possibly an `s3://` URI) — reads through a
//! location remap ([`FileIcebergStorage::with_remap`]): the provider infers it
//! by comparing the metadata's own `location` with the configured
//! `table_location`, so "copy the table directory, point at it" needs no
//! configuration. An object-store URI that does NOT match the remap prefix is
//! rejected with an error naming the copied-table cause.

use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::error::{IcebergError, Result};
use crate::io::storage::{IcebergStorage, SendIcebergStorage};

/// Storage backend that reads Iceberg files from the local filesystem.
///
/// Every read resolves the path it is handed (a `file://` URI or a bare
/// absolute path), optionally through a location remap (see
/// [`Self::with_remap`]) for tables that were copied or moved. `Clone` is
/// required by the Parquet reader, which clones the storage into per-range
/// read tasks — trivially cheap here.
#[derive(Clone, Debug)]
pub struct FileIcebergStorage {
    /// `(from_prefix, to_root)`: file references starting with `from_prefix`
    /// (as the manifests wrote them — possibly an `s3://` URI) resolve under
    /// `to_root` instead. `None` = paths resolve as written.
    remap: Option<(String, String)>,
    /// Directories this storage may read under, captured once at construction
    /// from [`crate::local_guard::LOCAL_ROOTS_ENV`]. **Empty means read
    /// nothing** — local tables are fail-closed, so a storage built while the
    /// allowlist is unset refuses every path rather than reading the filesystem
    /// unconfined. Holding the roots here keeps the per-read path free of
    /// global state.
    roots: Arc<[PathBuf]>,
}

impl Default for FileIcebergStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl FileIcebergStorage {
    pub fn new() -> Self {
        Self {
            remap: None,
            roots: configured_roots(),
        }
    }

    /// A storage whose reads remap the location prefix `from` (the table root
    /// the metadata/manifests were WRITTEN under — the metadata's own
    /// `location`, possibly an `s3://` URI) to the local root `to` (where the
    /// table sits NOW — the configured `table_location`).
    ///
    /// This is what makes "copy the table directory, point at it" work:
    /// Iceberg manifests reference data files by absolute URI, so a relocated
    /// table's references all carry the ORIGINAL prefix. Only reads whose path
    /// starts with `from` (on a `/` boundary) are rewritten, and they are
    /// rewritten toward the operator-configured `to` — never toward anything
    /// derived from the (untrusted) metadata itself.
    pub fn with_remap(from: impl Into<String>, to: impl Into<String>) -> Self {
        let from = from.into().trim_end_matches('/').to_string();
        let to = to.into().trim_end_matches('/').to_string();
        Self {
            remap: Some((from, to)),
            roots: configured_roots(),
        }
    }

    /// A storage confined to an explicit allowlist instead of the process
    /// environment — lets tests exercise real reads (and real refusals) without
    /// mutating shared process state or racing other tests.
    #[cfg(test)]
    fn with_roots(roots: Vec<PathBuf>) -> Self {
        Self {
            remap: None,
            roots: crate::local_guard::expand_roots(roots.into_iter()).into(),
        }
    }

    /// Apply the location remap, if configured and the prefix matches on a
    /// path-segment boundary; otherwise the path passes through unchanged.
    fn apply_remap<'a>(&self, path: &'a str) -> std::borrow::Cow<'a, str> {
        if let Some((from, to)) = &self.remap {
            if let Some(rest) = path.strip_prefix(from.as_str()) {
                if rest.is_empty() {
                    return std::borrow::Cow::Owned(to.clone());
                }
                if let Some(rest) = rest.strip_prefix('/') {
                    return std::borrow::Cow::Owned(format!("{to}/{rest}"));
                }
                // Prefix matched mid-segment (e.g. `/tab` vs `/table2`):
                // not this table's root — fall through unchanged.
            }
        }
        std::borrow::Cow::Borrowed(path)
    }

    /// Whether `location` addresses the local filesystem: a `file://` URI or a
    /// bare absolute path. The dispatch predicate used when choosing a storage
    /// backend for a `Direct` table location — pure syntax, carrying no
    /// permission decision (see [`crate::local_guard`]).
    pub fn is_local_location(location: &str) -> bool {
        crate::local_guard::is_local_location(location)
    }

    /// Resolve a metadata/data-file reference to a local path, confined to the
    /// operator's allowlist ([`crate::local_guard::LOCAL_ROOTS_ENV`]).
    ///
    /// Every read and listing goes through here, so a manifest reference that
    /// climbs out of the table directory is refused rather than followed —
    /// manifest content is only as trustworthy as whoever supplied the table.
    fn resolve(&self, path: &str) -> Result<PathBuf> {
        crate::local_guard::resolve_local_path_within(path, &self.roots)
    }

    async fn read_impl(&self, path: &str) -> Result<Bytes> {
        let p = self.resolve(path)?;
        let bytes = tokio::fs::read(&p)
            .await
            .map_err(|e| storage_io_err("read", &p, &e))?;
        Ok(Bytes::from(bytes))
    }

    async fn read_range_impl(&self, path: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        let p = self.resolve(path)?;
        let mut file = tokio::fs::File::open(&p)
            .await
            .map_err(|e| storage_io_err("open", &p, &e))?;
        file.seek(std::io::SeekFrom::Start(range.start))
            .await
            .map_err(|e| storage_io_err("seek", &p, &e))?;
        let len = (range.end - range.start) as usize;
        let mut buf = Vec::with_capacity(len);
        // `take` + `read_to_end` mirrors an object-store range GET: a range
        // running past EOF returns the bytes that exist rather than erroring.
        file.take(len as u64)
            .read_to_end(&mut buf)
            .await
            .map_err(|e| storage_io_err("read range", &p, &e))?;
        Ok(Bytes::from(buf))
    }

    async fn file_size_impl(&self, path: &str) -> Result<u64> {
        let p = self.resolve(path)?;
        let meta = tokio::fs::metadata(&p)
            .await
            .map_err(|e| storage_io_err("stat", &p, &e))?;
        Ok(meta.len())
    }

    async fn list_files_impl(&self, prefix: &str) -> Result<Vec<String>> {
        let dir = self.resolve(prefix)?;
        let mut entries = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| storage_io_err("list", &dir, &e))?;
        let mut names = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| storage_io_err("list", &dir, &e))?
        {
            let ft = entry
                .file_type()
                .await
                .map_err(|e| storage_io_err("list", &dir, &e))?;
            if ft.is_file() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        names.sort();
        Ok(names)
    }

    async fn list_dir_impl(&self, prefix: &str) -> Result<Vec<String>> {
        let dir = self.resolve(prefix)?;
        let mut entries = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| storage_io_err("list", &dir, &e))?;
        let mut names = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| storage_io_err("list", &dir, &e))?
        {
            let ft = entry
                .file_type()
                .await
                .map_err(|e| storage_io_err("list", &dir, &e))?;
            if ft.is_dir() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        names.sort();
        Ok(names)
    }
}

/// The operator's allowlist as an `Arc<[PathBuf]>`, or empty when local tables
/// are disabled (the default). Shared by every storage this process builds.
fn configured_roots() -> Arc<[PathBuf]> {
    crate::local_guard::local_roots().unwrap_or(&[]).into()
}

fn storage_io_err(op: &str, path: &Path, e: &std::io::Error) -> IcebergError {
    IcebergError::storage(format!("Failed to {op} {}: {e}", path.display()))
}

#[async_trait(?Send)]
impl IcebergStorage for FileIcebergStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.read_impl(&self.apply_remap(path)).await
    }

    async fn read_range(&self, path: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        self.read_range_impl(&self.apply_remap(path), range).await
    }

    async fn file_size(&self, path: &str) -> Result<u64> {
        self.file_size_impl(&self.apply_remap(path)).await
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>> {
        self.list_files_impl(&self.apply_remap(prefix)).await
    }
}

#[async_trait]
impl SendIcebergStorage for FileIcebergStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.read_impl(&self.apply_remap(path)).await
    }

    async fn read_range(&self, path: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        self.read_range_impl(&self.apply_remap(path), range).await
    }

    async fn file_size(&self, path: &str) -> Result<u64> {
        self.file_size_impl(&self.apply_remap(path)).await
    }

    async fn list_dir(&self, prefix: &str) -> Result<Vec<String>> {
        self.list_dir_impl(&self.apply_remap(prefix)).await
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>> {
        self.list_files_impl(&self.apply_remap(prefix)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_tree(root: &Path) {
        std::fs::create_dir_all(root.join("metadata")).unwrap();
        std::fs::write(root.join("metadata/a.txt"), b"hello world").unwrap();
        std::fs::create_dir_all(root.join("data")).unwrap();
    }

    #[test]
    fn remap_rewrites_only_boundary_prefixed_paths() {
        let s = FileIcebergStorage::with_remap("s3://bucket/wh/table", "file:///local/copy");
        // Data-file reference under the old root → under the new root.
        assert_eq!(
            s.apply_remap("s3://bucket/wh/table/data/00001.parquet"),
            "file:///local/copy/data/00001.parquet"
        );
        // Exact root match.
        assert_eq!(s.apply_remap("s3://bucket/wh/table"), "file:///local/copy");
        // Mid-segment prefix (`table2` is a DIFFERENT table): untouched.
        assert_eq!(
            s.apply_remap("s3://bucket/wh/table2/data/x.parquet"),
            "s3://bucket/wh/table2/data/x.parquet"
        );
        // Unrelated path: untouched.
        assert_eq!(s.apply_remap("/elsewhere/f"), "/elsewhere/f");
        // Trailing slashes normalize away on both ends.
        let s = FileIcebergStorage::with_remap("file:///old/root/", "/new/root/");
        assert_eq!(
            s.apply_remap("file:///old/root/m/v1.json"),
            "/new/root/m/v1.json"
        );
        // No remap configured: identity.
        assert_eq!(FileIcebergStorage::new().apply_remap("/x/y"), "/x/y");
    }

    #[test]
    fn resolve_accepts_file_uris_and_absolute_paths() {
        // `/tmp/t` does not exist, so resolution stays lexical — which is the
        // case that matters here (form parsing, not canonicalization).
        let s = FileIcebergStorage::with_roots(vec![PathBuf::from("/tmp")]);
        assert_eq!(
            s.resolve("file:///tmp/t/metadata/v1.json").unwrap(),
            PathBuf::from("/tmp/t/metadata/v1.json")
        );
        assert_eq!(
            s.resolve("file:/tmp/t/x").unwrap(),
            PathBuf::from("/tmp/t/x")
        );
        assert_eq!(s.resolve("/tmp/t/x").unwrap(), PathBuf::from("/tmp/t/x"));
        // An object-store URI names the copied-table cause in its error.
        let err = s.resolve("s3://bucket/t/x").unwrap_err();
        assert!(err.to_string().contains("copied from an object store"));
        // Relative paths are rejected (metadata must carry absolute locations).
        assert!(s.resolve("relative/path").is_err());
    }

    #[test]
    fn resolve_refuses_paths_outside_the_allowlist() {
        let s = FileIcebergStorage::with_roots(vec![PathBuf::from("/tmp/wh")]);
        // Inside the root: fine.
        assert!(s.resolve("/tmp/wh/people/metadata/v1.json").is_ok());
        // Traversal out of the root — the manifest-supplied escape this guards.
        let err = s
            .resolve("/tmp/wh/people/../../../etc/passwd")
            .unwrap_err()
            .to_string();
        assert!(err.contains("outside every directory allowed"), "{err}");
        assert!(err.contains(crate::local_guard::LOCAL_ROOTS_ENV), "{err}");
        // A sibling sharing a name prefix is not inside the root.
        assert!(s.resolve("/tmp/wh2/people/x").is_err());
        // Plain outside path.
        assert!(s.resolve("/etc/passwd").is_err());
    }

    #[test]
    fn storage_with_no_allowlist_reads_nothing() {
        // Fail-closed: a storage built while `FLUREE_ICEBERG_LOCAL_ROOTS` is
        // unset refuses every path rather than reading unconfined.
        let s = FileIcebergStorage::with_roots(Vec::new());
        let err = s.resolve("/tmp/wh/people").unwrap_err().to_string();
        assert!(err.contains("are disabled"), "{err}");
        assert!(err.contains(crate::local_guard::LOCAL_ROOTS_ENV), "{err}");
    }

    #[tokio::test]
    async fn read_range_and_size_behave_like_an_object_store() {
        let tmp = std::env::temp_dir().join(format!("fluree-file-storage-{}", std::process::id()));
        write_tree(&tmp);
        // Confine to the fixture's own directory rather than the process env, so
        // this exercises real reads without depending on (or racing) the
        // allowlist other tests see.
        let storage = FileIcebergStorage::with_roots(vec![tmp.clone()]);
        let path = format!("file://{}", tmp.join("metadata/a.txt").display());

        let all = SendIcebergStorage::read(&storage, &path).await.unwrap();
        assert_eq!(&all[..], b"hello world");
        assert_eq!(
            SendIcebergStorage::file_size(&storage, &path)
                .await
                .unwrap(),
            11
        );
        let mid = SendIcebergStorage::read_range(&storage, &path, 6..11)
            .await
            .unwrap();
        assert_eq!(&mid[..], b"world");
        // Past-EOF range returns the bytes that exist (object-store semantics).
        let over = SendIcebergStorage::read_range(&storage, &path, 6..100)
            .await
            .unwrap();
        assert_eq!(&over[..], b"world");

        let files =
            SendIcebergStorage::list_files(&storage, &format!("{}/metadata", tmp.display()))
                .await
                .unwrap();
        assert_eq!(files, vec!["a.txt".to_string()]);
        // list_dir returns directories only.
        let dirs = SendIcebergStorage::list_dir(&storage, &format!("file://{}", tmp.display()))
            .await
            .unwrap();
        assert_eq!(dirs, vec!["data".to_string(), "metadata".to_string()]);

        std::fs::remove_dir_all(&tmp).ok();
    }
}
