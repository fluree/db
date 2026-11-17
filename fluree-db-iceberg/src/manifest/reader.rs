//! Concurrent, order-preserving manifest reads.
//!
//! Every path that walks a snapshot's manifests (the scan planners, the
//! incremental planner, and the manifest-only statistics reads) reads them
//! through [`read_manifests`]. A manifest read is one object GET, so reading
//! them one await at a time costs one round trip per manifest; on an object
//! store that latency, not the Avro decode, dominates planning a table with
//! many commits. Reads run `concurrency` at a time, and results are yielded in
//! the order the manifest list gives, so everything downstream (task order,
//! counters, the first error reported) is the same as a sequential walk.

use std::future::Future;

use bytes::Bytes;
use futures::stream::{self, BoxStream, Stream, StreamExt};

use super::{parse_manifest, ManifestEntry, ManifestListEntry};
use crate::error::Result;
use crate::io::SendIcebergStorage;

/// How many manifests are read at once when the caller does not say.
///
/// Each in-flight read holds one manifest's bytes and, once parsed, its
/// entries, so this also bounds the planner's transient memory to that many
/// manifests beyond the files it keeps. Sixteen hides most of an object store's
/// per-request latency without opening an unreasonable number of connections
/// for a single query.
pub const DEFAULT_MANIFEST_READ_CONCURRENCY: usize = 16;

/// Read and parse each manifest in `manifests`, at most `concurrency` at a
/// time, yielding `(manifest, entries)` in the order of `manifests`.
///
/// `read` fetches one manifest's bytes by path (a storage backend's `read`).
/// The caller chooses which manifests to read: skipping delete manifests, or
/// manifests older than an incremental window, happens before this call.
///
/// A failed read or parse is yielded at that manifest's position, so a caller
/// that stops at the first error reports the same error a sequential walk
/// would. Reads still in flight when the stream is dropped are dropped with
/// it. A `concurrency` of 0 is treated as 1.
pub fn read_manifests<'m, I, F, Fut>(
    manifests: I,
    concurrency: usize,
    mut read: F,
) -> impl Stream<Item = Result<(&'m ManifestListEntry, Vec<ManifestEntry>)>> + 'm
where
    I: IntoIterator<Item = &'m ManifestListEntry>,
    I::IntoIter: 'm,
    F: FnMut(&'m str) -> Fut + 'm,
    Fut: Future<Output = Result<Bytes>> + 'm,
{
    stream::iter(manifests)
        .map(move |manifest| {
            let bytes = read(&manifest.manifest_path);
            async move {
                let bytes = bytes.await?;
                Ok((manifest, parse_manifest(&bytes)?))
            }
        })
        // `buffered`, not `buffer_unordered`: completion order varies run to
        // run, and the plan's order must not.
        .buffered(concurrency.max(1))
}

/// [`read_manifests`] over a `Send` storage, as a boxed `Send` stream.
///
/// Boxing erases the reader's closure types. A caller awaiting the stream
/// inside a future that must be `Send` for every lifetime (an `async_trait`
/// method) cannot otherwise prove it: the compiler's auto-trait check over
/// those closure types is not general enough.
pub fn send_read_manifests<'m, S, I>(
    storage: &'m S,
    manifests: I,
    concurrency: usize,
) -> BoxStream<'m, Result<(&'m ManifestListEntry, Vec<ManifestEntry>)>>
where
    S: SendIcebergStorage + ?Sized,
    I: IntoIterator<Item = &'m ManifestListEntry>,
    I::IntoIter: Send + 'm,
{
    read_manifests(manifests, concurrency, move |path| storage.read(path)).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::IcebergError;
    use crate::test_support::{list_entry, single_file_manifest};
    use futures::TryStreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Reads finish in REVERSE of their start order (the first manifest is the
    /// slowest), so a completion-ordered reader would yield them backwards.
    #[tokio::test]
    async fn yields_manifests_in_list_order_whatever_order_reads_finish() {
        let entries: Vec<_> = (0..8u64)
            .map(|i| list_entry(&format!("m{i}.avro"), 1))
            .collect();
        let out: Vec<_> = read_manifests(&entries, 8, |path: &str| {
            let i: u64 = path[1..path.len() - 5].parse().unwrap();
            let path = path.to_string();
            async move {
                tokio::time::sleep(Duration::from_millis(5 * (8 - i))).await;
                Ok(single_file_manifest(&format!("data-{path}.parquet")))
            }
        })
        .try_collect()
        .await
        .unwrap();

        let got: Vec<_> = out
            .iter()
            .map(|(m, files)| {
                assert_eq!(files.len(), 1);
                (
                    m.manifest_path.clone(),
                    files[0].data_file.file_path.clone(),
                )
            })
            .collect();
        let want: Vec<_> = (0..8)
            .map(|i| (format!("m{i}.avro"), format!("data-m{i}.avro.parquet")))
            .collect();
        assert_eq!(got, want);
    }

    /// No more than `concurrency` reads are ever in flight, and the reader does
    /// use the headroom (a sequential walk would peak at 1).
    #[tokio::test]
    async fn never_exceeds_the_concurrency_bound() {
        let entries: Vec<_> = (0..40)
            .map(|i| list_entry(&format!("m{i}.avro"), 1))
            .collect();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let n: usize = read_manifests(&entries, 4, |path: &str| {
            let in_flight = Arc::clone(&in_flight);
            let peak = Arc::clone(&peak);
            let path = path.to_string();
            async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(2)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(single_file_manifest(&path))
            }
        })
        .try_fold(0, |n, _| async move { Ok(n + 1) })
        .await
        .unwrap();
        assert_eq!(n, 40);
        assert_eq!(peak.load(Ordering::SeqCst), 4, "peak in-flight reads");
    }

    /// The error reported is the FIRST failing manifest in list order, even when
    /// a later manifest fails sooner.
    #[tokio::test]
    async fn reports_the_first_failure_in_list_order() {
        let entries: Vec<_> = (0..4)
            .map(|i| list_entry(&format!("m{i}.avro"), 1))
            .collect();
        let err = read_manifests(&entries, 4, |path: &str| {
            let path = path.to_string();
            async move {
                match path.as_str() {
                    "m1.avro" => {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Err(IcebergError::storage("m1 failed"))
                    }
                    "m3.avro" => Err(IcebergError::storage("m3 failed")),
                    _ => Ok(single_file_manifest(&path)),
                }
            }
        })
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
        assert!(err.to_string().contains("m1 failed"), "{err}");
    }
}
