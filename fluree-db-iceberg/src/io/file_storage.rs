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
#[derive(Clone, Debug, Default)]
pub struct FileIcebergStorage {
    /// `(from_prefix, to_root)`: file references starting with `from_prefix`
    /// (as the manifests wrote them — possibly an `s3://` URI) resolve under
    /// `to_root` instead. `None` = paths resolve as written.
    remap: Option<(String, String)>,
}

impl FileIcebergStorage {
    pub fn new() -> Self {
        Self::default()
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
    /// backend for a `Direct` table location.
    pub fn is_local_location(location: &str) -> bool {
        location.starts_with("file://") || location.starts_with('/')
    }

    /// Resolve a metadata/data-file reference to a local path.
    ///
    /// Accepts `file:///abs/path` (pyiceberg's local-warehouse form,
    /// including the `file:/abs/path` single-slash variant some writers emit)
    /// and bare absolute paths. Object-store URIs are rejected by name — the
    /// usual cause is a table copied from S3 whose manifests still reference
    /// the original bucket.
    fn resolve(path: &str) -> Result<PathBuf> {
        if let Some(rest) = path.strip_prefix("file://") {
            // `file:///abs` → `/abs`; `file://host/abs` is not supported (no
            // remote hosts), but `file://` + `/abs` parses as empty host + path.
            if let Some(p) = rest.strip_prefix('/') {
                // Guard the `file:////`-ish degenerate forms down to one root slash.
                return Ok(PathBuf::from(format!("/{}", p.trim_start_matches('/'))));
            }
            return Err(IcebergError::storage(format!(
                "Unsupported file:// URI (expected file:///absolute/path): {path}"
            )));
        }
        if let Some(rest) = path.strip_prefix("file:/") {
            // Single-slash variant: `file:/abs/path`.
            return Ok(PathBuf::from(format!("/{rest}")));
        }
        if path.starts_with('/') {
            return Ok(PathBuf::from(path));
        }
        if path.starts_with("s3://") || path.starts_with("s3a://") || path.starts_with("gs://") {
            return Err(IcebergError::storage(format!(
                "Local file storage cannot read an object-store URI: {path}. This usually \
                 means the table was copied from an object store and its manifests still \
                 reference the original location; local reads need the table written with \
                 local paths"
            )));
        }
        Err(IcebergError::storage(format!(
            "Local file storage requires a file:// URI or an absolute path, got: {path}"
        )))
    }

    async fn read_impl(path: &str) -> Result<Bytes> {
        let p = Self::resolve(path)?;
        let bytes = tokio::fs::read(&p)
            .await
            .map_err(|e| storage_io_err("read", &p, &e))?;
        Ok(Bytes::from(bytes))
    }

    async fn read_range_impl(path: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        let p = Self::resolve(path)?;
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

    async fn file_size_impl(path: &str) -> Result<u64> {
        let p = Self::resolve(path)?;
        let meta = tokio::fs::metadata(&p)
            .await
            .map_err(|e| storage_io_err("stat", &p, &e))?;
        Ok(meta.len())
    }

    async fn list_files_impl(prefix: &str) -> Result<Vec<String>> {
        let dir = Self::resolve(prefix)?;
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

    async fn list_dir_impl(prefix: &str) -> Result<Vec<String>> {
        let dir = Self::resolve(prefix)?;
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

fn storage_io_err(op: &str, path: &Path, e: &std::io::Error) -> IcebergError {
    IcebergError::storage(format!("Failed to {op} {}: {e}", path.display()))
}

#[async_trait(?Send)]
impl IcebergStorage for FileIcebergStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        Self::read_impl(&self.apply_remap(path)).await
    }

    async fn read_range(&self, path: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        Self::read_range_impl(&self.apply_remap(path), range).await
    }

    async fn file_size(&self, path: &str) -> Result<u64> {
        Self::file_size_impl(&self.apply_remap(path)).await
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>> {
        Self::list_files_impl(&self.apply_remap(prefix)).await
    }
}

#[async_trait]
impl SendIcebergStorage for FileIcebergStorage {
    async fn read(&self, path: &str) -> Result<Bytes> {
        Self::read_impl(&self.apply_remap(path)).await
    }

    async fn read_range(&self, path: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        Self::read_range_impl(&self.apply_remap(path), range).await
    }

    async fn file_size(&self, path: &str) -> Result<u64> {
        Self::file_size_impl(&self.apply_remap(path)).await
    }

    async fn list_dir(&self, prefix: &str) -> Result<Vec<String>> {
        Self::list_dir_impl(&self.apply_remap(prefix)).await
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>> {
        Self::list_files_impl(&self.apply_remap(prefix)).await
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
        assert_eq!(
            FileIcebergStorage::resolve("file:///tmp/t/metadata/v1.json").unwrap(),
            PathBuf::from("/tmp/t/metadata/v1.json")
        );
        assert_eq!(
            FileIcebergStorage::resolve("file:/tmp/t/x").unwrap(),
            PathBuf::from("/tmp/t/x")
        );
        assert_eq!(
            FileIcebergStorage::resolve("/tmp/t/x").unwrap(),
            PathBuf::from("/tmp/t/x")
        );
        // An object-store URI names the copied-table cause in its error.
        let err = FileIcebergStorage::resolve("s3://bucket/t/x").unwrap_err();
        assert!(err.to_string().contains("copied from an object store"));
        // Relative paths are rejected (metadata must carry absolute locations).
        assert!(FileIcebergStorage::resolve("relative/path").is_err());
    }

    #[tokio::test]
    async fn read_range_and_size_behave_like_an_object_store() {
        let tmp = std::env::temp_dir().join(format!("fluree-file-storage-{}", std::process::id()));
        write_tree(&tmp);
        let storage = FileIcebergStorage::new();
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
