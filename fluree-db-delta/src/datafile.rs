//! Read one Parquet data file.
//!
//! Kernel's own file read takes no row selection and fetches every footer
//! again on every query. This read keeps footers (with their page indexes)
//! across queries — a data file never changes — and decodes only the row
//! groups and pages [`crate::prune`] cannot rule out. Matching the file's
//! columns to the table's is still Kernel's.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_utils::{fixup_parquet_read, parquet_read_plan};
use delta_kernel::engine::reader_options;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{DynObjectStore, ObjectStoreExt as _};
use delta_kernel::parquet::arrow::arrow_reader::ArrowReaderMetadata;
use delta_kernel::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use delta_kernel::parquet::file::metadata::PageIndexPolicy;
use delta_kernel::schema::SchemaRef;
use delta_kernel::{DeltaResult, FileMeta};
use futures::stream::BoxStream;
use futures::StreamExt;
use lru::LruCache;

use crate::prune::Term;

/// Fetched with the footer in one request; usually covers the page index too.
const FOOTER_HINT_BYTES: usize = 128 * 1024;
const DEFAULT_FOOTER_BUDGET_MB: usize = 128;

static ROWS_DECODED: AtomicU64 = AtomicU64::new(0);

/// Rows decoded from data files by this process, before row filtering. A
/// diagnostic: it moves only by what scans actually read.
pub fn rows_decoded() -> u64 {
    ROWS_DECODED.load(Ordering::Relaxed)
}

struct Footers {
    entries: LruCache<String, (u64, ArrowReaderMetadata, usize)>,
    bytes: usize,
}

fn footers() -> &'static Mutex<Footers> {
    static FOOTERS: std::sync::OnceLock<Mutex<Footers>> = std::sync::OnceLock::new();
    FOOTERS.get_or_init(|| {
        Mutex::new(Footers {
            entries: LruCache::unbounded(),
            bytes: 0,
        })
    })
}

/// `FLUREE_DELTA_FOOTER_CACHE_MB`; `0` keeps no footer.
fn footer_budget() -> usize {
    std::env::var("FLUREE_DELTA_FOOTER_CACHE_MB")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_FOOTER_BUDGET_MB)
        .saturating_mul(1024 * 1024)
}

fn lock(footers: &Mutex<Footers>) -> std::sync::MutexGuard<'_, Footers> {
    footers
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

async fn footer(
    reader: &mut ParquetObjectReader,
    location: &str,
    size: u64,
) -> DeltaResult<ArrowReaderMetadata> {
    if let Some((held_size, metadata, _)) = lock(footers()).entries.get(location) {
        if *held_size == size {
            return Ok(metadata.clone());
        }
    }
    // Optional: not every writer records a page index.
    let options = reader_options().with_page_index_policy(PageIndexPolicy::Optional);
    let metadata = ArrowReaderMetadata::load_async(reader, options).await?;
    let budget = footer_budget();
    let bytes = metadata.metadata().memory_size();
    if bytes <= budget {
        let mut held = lock(footers());
        if let Some((_, _, old)) = held
            .entries
            .put(location.to_string(), (size, metadata.clone(), bytes))
        {
            held.bytes -= old;
        }
        held.bytes += bytes;
        while held.bytes > budget {
            match held.entries.pop_lru() {
                Some((_, (_, _, evicted))) => held.bytes -= evicted,
                None => break,
            }
        }
    }
    Ok(metadata)
}

/// Stream `file`'s rows in `physical_schema`'s shape. `terms` may leave out
/// rows no filter can pass; pass none for a file whose rows are addressed by
/// position (a deletion vector).
pub(crate) async fn open(
    store: Arc<DynObjectStore>,
    file: FileMeta,
    physical_schema: SchemaRef,
    terms: &[Term],
    batch_rows: NonZeroUsize,
) -> DeltaResult<BoxStream<'static, DeltaResult<Box<ArrowEngineData>>>> {
    let location = file.location.to_string();
    let path = Path::from_url_path(file.location.path())?;
    // The log should record every file's size; without it, ask, because not
    // every store can serve a read from the end of an object.
    let size = match file.size {
        0 => store.head(&path).await?.size,
        size => size,
    };
    let mut reader = ParquetObjectReader::new(store, path)
        .with_file_size(size)
        .with_footer_size_hint(FOOTER_HINT_BYTES);
    let metadata = footer(&mut reader, &location, size).await?;
    let (ordering, mask) = parquet_read_plan(&physical_schema, &metadata)?;
    let plan = crate::prune::plan(metadata.metadata(), terms);

    let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata)
        .with_batch_size(batch_rows.get());
    if let Some(mask) = mask {
        builder = builder.with_projection(mask);
    }
    if let Some(plan) = plan {
        builder = builder.with_row_groups(plan.row_groups);
        if let Some(rows) = plan.rows {
            builder = builder.with_row_selection(rows);
        }
    }
    let stream = builder.build()?;
    Ok(stream
        .map(move |batch| {
            let batch: RecordBatch = batch?;
            ROWS_DECODED.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            fixup_parquet_read(
                batch,
                &ordering,
                None,
                Some(&location),
                Some(&physical_schema),
            )
            .map(Box::new)
        })
        .boxed())
}
