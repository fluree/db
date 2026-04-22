//! Index store: loads FIR6 roots and provides value decoding via `o_type`.
//!
//! - `BranchManifest` for routing
//! - `o_type` table for decode dispatch
//! - `ColumnBatch` output

use std::cmp::Ordering;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fluree_db_core::ids::DatatypeDictId;
use fluree_db_core::ns_encoding::{canonical_split, NsLookup, NsSplitMode};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::o_type_registry::OTypeRegistry;
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::GraphId;
use fluree_db_core::{
    ContentId, ContentStore, FlakeMeta, FlakeValue, PrefixTrie, RuntimeSmallDicts, Sid,
};
use fluree_vocab::{geo_names, jsonld_names, namespaces, rdf_names, xsd_names};
use parking_lot::RwLock;

use crate::dict::forward_pack::{KIND_STRING_FWD, KIND_SUBJECT_FWD};
use crate::dict::global_dict::{LanguageTagDict, PredicateDict};
use crate::dict::pack_reader::ForwardPackReader;
use crate::dict::DictTreeReader;
use crate::format::branch::{read_branch_from_bytes, BranchManifest};
use crate::format::index_root::{IndexRoot, OTypeTableEntry};
use crate::format::leaf::DecodedLeafDirV3;
use crate::format::run_record::RunSortOrder;

use super::artifact_cache::{fetch_cached_bytes, fetch_cached_bytes_cid};
use super::leaflet_cache::LeafletCache;

const HOT_REMOTE_LEAF_PROMOTION_TOUCHES: usize = 2;

// ============================================================================
// Shared dictionary / CAS utilities
// ============================================================================

/// All dictionary and encoding state needed to translate between
/// human-readable IRIs/strings and compact integer IDs.
pub(crate) struct DictionarySet {
    pub(crate) predicates: PredicateDict,
    pub(crate) predicate_reverse: HashMap<String, u32>,
    /// Graph IRI → dict_index (0-based). g_id = dict_index + 1.
    pub(crate) graphs_reverse: HashMap<String, GraphId>,
    /// Subject forward packs keyed by ns_code.
    pub(crate) subject_forward_packs: std::collections::BTreeMap<u16, ForwardPackReader>,
    pub(crate) subject_reverse_tree: Option<DictTreeReader>,
    /// String forward pack reader (all string IDs in one stream).
    pub(crate) string_forward_packs: ForwardPackReader,
    pub(crate) string_reverse_tree: Option<DictTreeReader>,
    // Kept for: DictOverlay watermark computation (query overlay resolution).
    // Use when: DictOverlay is wired into V3 query execution for overlay transactions.
    #[expect(dead_code)]
    pub(crate) subject_count: u32,
    /// Total string count (for DictOverlay watermark).
    pub(crate) string_count: u32,
    pub(crate) namespace_codes: HashMap<u16, String>,
    pub(crate) namespace_reverse: HashMap<String, u16>,
    pub(crate) prefix_trie: PrefixTrie,
    pub(crate) language_tags: LanguageTagDict,
    pub(crate) dt_sids: Vec<Sid>,
}

fn cas_sync_timeout() -> Option<Duration> {
    std::env::var("FLUREE_CAS_SYNC_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn run_sync_on_runtime<T, Fut>(fut: Fut) -> io::Result<T>
where
    T: Send + 'static,
    Fut: std::future::Future<Output = io::Result<T>> + Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                // Safe on multithread runtimes: allow blocking in-place while we
                // drive the async future to completion.
                tokio::task::block_in_place(|| handle.block_on(fut))
            }
            tokio::runtime::RuntimeFlavor::CurrentThread => {
                // Avoid deadlock:
                // - We're on the single runtime thread.
                // - If we block here waiting for another thread that calls
                //   `handle.block_on(...)`, the runtime can't make progress.
                // Instead, run the future on a self-contained runtime in a helper thread.
                let (tx, rx) = std::sync::mpsc::sync_channel::<io::Result<T>>(1);
                std::thread::spawn(move || {
                    let result = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            io::Error::other(format!("failed to build helper runtime: {e}"))
                        })
                        .and_then(|rt| rt.block_on(fut));
                    let _ = tx.send(result);
                });
                rx.recv()
                    .map_err(|_| io::Error::other("helper runtime thread panicked"))?
            }
            _ => {
                // Future-proofing: treat unknown flavors conservatively.
                let (tx, rx) = std::sync::mpsc::sync_channel::<io::Result<T>>(1);
                std::thread::spawn(move || {
                    let result = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            io::Error::other(format!("failed to build helper runtime: {e}"))
                        })
                        .and_then(|rt| rt.block_on(fut));
                    let _ = tx.send(result);
                });
                rx.recv()
                    .map_err(|_| io::Error::other("helper runtime thread panicked"))?
            }
        },
        Err(_) => {
            // No runtime context available. Create a local runtime to run the future.
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| io::Error::other(format!("failed to build helper runtime: {e}")))?
                .block_on(fut)
        }
    }
}

// ============================================================================
// Per-graph V3 index data
// ============================================================================

struct GraphIndex {
    orders: HashMap<RunSortOrder, Arc<BranchManifest>>,
    numbig: HashMap<u32, crate::arena::numbig::NumBigArena>,
    vectors: HashMap<u32, crate::arena::vector::LazyVectorArena>,
    spatial: HashMap<u32, Arc<dyn fluree_db_spatial::SpatialIndexProvider>>,
    /// Fulltext arenas keyed by `(p_id, lang_id)` — one arena per language
    /// on each property. `@fulltext`-datatype and configured-English content
    /// both resolve to the dict-assigned id for `"en"` and share a bucket.
    fulltext: HashMap<(u32, u16), Arc<crate::arena::fulltext::FulltextArena>>,
}

// ============================================================================
// BinaryIndexStore
// ============================================================================

/// Index store — reads FLI3/FBR3/FHS1 artifacts via FIR6 root.
///
/// - Routing via `BranchManifest` (sidecar CIDs for history)
/// - Value decoding via `o_type` table
/// - Dict/arena infrastructure (same types, same loading)
pub struct BinaryIndexStore {
    dicts: DictionarySet,
    graph_indexes: HashMap<GraphId, GraphIndex>,
    /// o_type table: full list (for iteration / serialization).
    o_type_table: Vec<OTypeTableEntry>,
    /// O(1) lookup: o_type value → index into o_type_table.
    o_type_index: HashMap<u16, usize>,
    cas: Option<Arc<dyn ContentStore>>,
    cache_dir: PathBuf,
    /// Shared disk artifact cache — kept alive here so the global `CACHE_REGISTRY`
    /// weak ref survives across calls, avoiding repeated dir scans on every write.
    disk_cache: Arc<super::artifact_cache::DiskArtifactCache>,
    leaflet_cache: Option<Arc<LeafletCache>>,
    /// Remote leaf metadata cache keyed by leaf CID.
    ///
    /// This avoids re-fetching the same header+directory ranges when repeated
    /// scans reopen the same remote leaf within a query/session.
    remote_leaf_metadata: RwLock<HashMap<ContentId, DecodedLeafDirV3>>,
    /// Remote leaf open counts keyed by leaf CID.
    ///
    /// Once a remote leaf is touched repeatedly, we promote it to the local
    /// disk cache so subsequent opens use `FullBlobLeafHandle`.
    remote_leaf_open_counts: RwLock<HashMap<ContentId, usize>>,
    max_t: i64,
    base_t: i64,
    language_tags: Vec<String>,
    lex_sorted_string_ids: bool,
    /// Ledger-fixed split mode for canonical IRI encoding.
    /// Set from the snapshot's `ns_split_mode` via `set_ns_split_mode()`.
    ns_split_mode: NsSplitMode,
    /// Whether `set_ns_split_mode` was called. Debug-asserted on first encode.
    ns_split_mode_set: bool,
}

impl BinaryIndexStore {
    /// Decode FIR6 bytes and load the store.
    pub async fn load_from_root_bytes(
        cs: Arc<dyn ContentStore>,
        bytes: &[u8],
        cache_dir: &Path,
        leaflet_cache: Option<Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        let root = IndexRoot::decode(bytes)?;
        Self::load_from_root_v6(cs, &root, cache_dir, leaflet_cache).await
    }

    /// Load from a parsed IndexRoot.
    pub async fn load_from_root_v6(
        cs: Arc<dyn ContentStore>,
        root: &IndexRoot,
        cache_dir: &Path,
        leaflet_cache: Option<Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        tracing::debug!("BinaryIndexStore::load_from_root_v6 starting");
        std::fs::create_dir_all(cache_dir)?;

        // ── Dict loading ──────────────────────────────────────────────────────────────
        let dicts =
            build_dictionary_set(Arc::clone(&cs), root, cache_dir, leaflet_cache.as_ref()).await?;

        // ── Per-graph specialty arenas ───────────────────────────────
        let mut per_graph_arenas = load_per_graph_arenas(
            Arc::clone(&cs),
            &root.graph_arenas,
            cache_dir,
            leaflet_cache.as_ref(),
        )
        .await?;

        // ── Graph index routing ────────────────────────────────────
        let mut graph_indexes: HashMap<GraphId, GraphIndex> = HashMap::new();

        // Default graph (g_id=0): inline leaf entries from root.
        for dgo in &root.default_graph_orders {
            let branch = BranchManifest {
                leaves: dgo.leaves.clone(),
            };
            let gi = graph_indexes.entry(0).or_insert_with(|| GraphIndex {
                orders: HashMap::new(),
                numbig: HashMap::new(),
                vectors: HashMap::new(),
                spatial: HashMap::new(),
                fulltext: HashMap::new(),
            });
            gi.orders.insert(dgo.order, Arc::new(branch));
        }

        // Named graphs: fetch FBR3 branch manifests from CAS.
        for ng in &root.named_graphs {
            for (order, branch_cid) in &ng.orders {
                let branch_bytes =
                    fetch_cached_bytes_cid(cs.as_ref(), branch_cid, cache_dir).await?;
                let branch = read_branch_from_bytes(&branch_bytes)?;
                let gi = graph_indexes.entry(ng.g_id).or_insert_with(|| GraphIndex {
                    orders: HashMap::new(),
                    numbig: HashMap::new(),
                    vectors: HashMap::new(),
                    spatial: HashMap::new(),
                    fulltext: HashMap::new(),
                });
                gi.orders.insert(*order, Arc::new(branch));
            }
        }

        // Inject per-graph arenas into graph indexes.
        for (g_id, arenas) in per_graph_arenas.drain() {
            let gi = graph_indexes.entry(g_id).or_insert_with(|| GraphIndex {
                orders: HashMap::new(),
                numbig: HashMap::new(),
                vectors: HashMap::new(),
                spatial: HashMap::new(),
                fulltext: HashMap::new(),
            });
            gi.numbig = arenas.numbig;
            gi.vectors = arenas.vectors;
            gi.spatial = arenas.spatial;
            gi.fulltext = arenas.fulltext;
        }

        let leaf_count: usize = graph_indexes
            .values()
            .flat_map(|gi| gi.orders.values())
            .map(|b| b.leaves.len())
            .sum();
        tracing::debug!(
            graphs = graph_indexes.len(),
            leaves = leaf_count,
            "loaded V6 graph indexes"
        );

        let o_type_table = root.o_type_table.clone();
        let o_type_index: HashMap<u16, usize> = o_type_table
            .iter()
            .enumerate()
            .map(|(i, e)| (e.o_type, i))
            .collect();

        let disk_cache = super::artifact_cache::DiskArtifactCache::for_dir(cache_dir);
        Ok(Self {
            dicts,
            graph_indexes,
            o_type_table,
            o_type_index,
            cas: Some(cs),
            cache_dir: cache_dir.to_path_buf(),
            disk_cache,
            leaflet_cache,
            remote_leaf_metadata: RwLock::new(HashMap::new()),
            remote_leaf_open_counts: RwLock::new(HashMap::new()),
            max_t: root.index_t,
            base_t: root.base_t,
            language_tags: root.language_tags.clone(),
            lex_sorted_string_ids: root.lex_sorted_string_ids,
            ns_split_mode: root.ns_split_mode,
            ns_split_mode_set: true,
        })
    }

    // ── Public accessors ───────────────────────────────────────────

    pub fn max_t(&self) -> i64 {
        self.max_t
    }

    pub fn base_t(&self) -> i64 {
        self.base_t
    }

    /// Set the ledger's split mode for canonical IRI encoding.
    ///
    /// Called after loading to sync with the snapshot's `ns_split_mode`.
    pub fn set_ns_split_mode(&mut self, mode: NsSplitMode) {
        self.ns_split_mode = mode;
        self.ns_split_mode_set = true;
    }

    /// True if `StringId` / `LEX_ID` ordering is lexicographic by UTF-8 bytes.
    #[inline]
    pub fn lex_sorted_string_ids(&self) -> bool {
        self.lex_sorted_string_ids
    }

    /// Get the branch manifest for a graph + sort order.
    pub fn branch_for_order(
        &self,
        g_id: GraphId,
        order: RunSortOrder,
    ) -> Option<&Arc<BranchManifest>> {
        self.graph_indexes
            .get(&g_id)
            .and_then(|gi| gi.orders.get(&order))
    }

    pub fn leaflet_cache(&self) -> Option<&Arc<LeafletCache>> {
        self.leaflet_cache.as_ref()
    }

    fn note_remote_leaf_open(&self, leaf_cid: &ContentId) -> usize {
        let mut counts = self.remote_leaf_open_counts.write();
        let count = counts.entry(leaf_cid.clone()).or_insert(0);
        *count += 1;
        *count
    }

    /// Fetch leaf bytes by CID: local path first, then CAS with caching.
    pub fn get_leaf_bytes_sync(&self, leaf_cid: &ContentId) -> io::Result<Vec<u8>> {
        let cs = self
            .cas
            .as_ref()
            .ok_or_else(|| io::Error::other("no content store"))?;

        // Try local path first.
        if let Some(local_path) = cs.resolve_local_path(leaf_cid) {
            match std::fs::read(&local_path) {
                Ok(bytes) => return Ok(bytes),
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    tracing::debug!(
                        path = %local_path.display(),
                        leaf = %leaf_cid,
                        "local leaf path disappeared during read; falling back to remote fetch"
                    );
                }
                Err(err) => return Err(err),
            }
        }

        // Check cache.
        let cache_path = self.cache_dir.join(leaf_cid.to_string());
        match std::fs::read(&cache_path) {
            Ok(bytes) => return Ok(bytes),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }

        // Fetch from CAS via sync bridge: capture the Tokio handle on the caller's
        // sync bridge: run the async CAS request without deadlocking current-thread runtimes.
        let cs = Arc::clone(cs);
        let cid = leaf_cid.clone();
        let cache_path_owned = cache_path.clone();
        let disk_cache = Arc::clone(&self.disk_cache);
        let timeout = cas_sync_timeout();
        run_sync_on_runtime(async move {
            let fut = cs.get(&cid);
            let data = if let Some(dur) = timeout {
                tokio::time::timeout(dur, fut)
                    .await
                    .map_err(|_| {
                        io::Error::other(format!(
                            "CAS fetch timed out after {}ms (cid={})",
                            dur.as_millis(),
                            cid
                        ))
                    })?
                    .map_err(|e| io::Error::other(format!("CAS fetch failed: {e}")))?
            } else {
                fut.await
                    .map_err(|e| io::Error::other(format!("CAS fetch failed: {e}")))?
            };
            disk_cache.best_effort_write(&cache_path_owned, &data);
            Ok(data)
        })
    }

    // ── LeafHandle-based access ──────────────────────────────────────

    /// Open a leaf for reading, choosing the optimal access strategy.
    ///
    /// - Local filesystem: returns `FullBlobLeafHandle` (OS page cache is optimal)
    /// - Cached locally: returns `FullBlobLeafHandle` (read from disk cache)
    /// - Remote (S3/etc): returns `RangeReadLeafHandle` (header+dir only, lazy
    ///   column fetch via byte-range reads)
    pub fn open_leaf_handle(
        &self,
        leaf_cid: &ContentId,
        sidecar_cid: Option<&ContentId>,
        need_replay: bool,
    ) -> io::Result<Box<dyn super::leaf_access::LeafHandle>> {
        use super::leaf_access::{
            fetch_header_and_directory, FullBlobLeafHandle, RangeReadLeafHandle,
        };
        let cs = self
            .cas
            .as_ref()
            .ok_or_else(|| io::Error::other("no content store"))?;

        let leaf_id = xxhash_rust::xxh3::xxh3_128(leaf_cid.to_bytes().as_ref());

        // Fast path 1: local filesystem — full read is optimal (OS page cache).
        if let Some(local_path) = cs.resolve_local_path(leaf_cid) {
            let bytes = std::fs::read(local_path)?;
            let sidecar = if need_replay {
                self.fetch_sidecar_bytes_sync(sidecar_cid)?
            } else {
                None
            };
            return Ok(Box::new(FullBlobLeafHandle::new(bytes, sidecar, leaf_id)?));
        }

        // Fast path 2: locally cached — full read from disk cache.
        let cache_path = self.cache_dir.join(leaf_cid.to_string());
        match std::fs::read(&cache_path) {
            Ok(bytes) => {
                let sidecar = if need_replay {
                    self.fetch_sidecar_bytes_sync(sidecar_cid)?
                } else {
                    None
                };
                return Ok(Box::new(FullBlobLeafHandle::new(bytes, sidecar, leaf_id)?));
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }

        let touch_count = self.note_remote_leaf_open(leaf_cid);

        if let Some(dir) = self.remote_leaf_metadata.read().get(leaf_cid).cloned() {
            if touch_count >= HOT_REMOTE_LEAF_PROMOTION_TOUCHES {
                tracing::debug!(
                    leaf = %leaf_cid,
                    need_replay,
                    source = "remote_promote_disk",
                    touch_count,
                    "promoting hot remote leaf to disk cache"
                );
                let bytes = self.get_leaf_bytes_sync(leaf_cid)?;
                let sidecar = if need_replay {
                    self.fetch_sidecar_bytes_sync(sidecar_cid)?
                } else {
                    None
                };
                return Ok(Box::new(FullBlobLeafHandle::new(bytes, sidecar, leaf_id)?));
            }
            let sc_cid = if need_replay {
                sidecar_cid.cloned()
            } else {
                None
            };
            return Ok(Box::new(RangeReadLeafHandle::new(
                leaf_cid.clone(),
                dir.clone(),
                dir.payload_base as u64,
                leaf_id,
                Arc::new(ContentStoreRangeFetcher::new(
                    Arc::clone(cs),
                    self.cache_dir.clone(),
                )) as Arc<dyn super::leaf_access::RangeReadFetcher>,
                sc_cid,
            )));
        }

        // Slow path: remote — use range reads for column-selective access.
        tracing::debug!(
            leaf = %leaf_cid,
            need_replay,
            source = "remote_range",
            touch_count,
            "binary leaf open"
        );
        let fetcher = Arc::new(ContentStoreRangeFetcher::new(
            Arc::clone(cs),
            self.cache_dir.clone(),
        ));

        let (dir, payload_base) = fetch_header_and_directory(fetcher.as_ref(), leaf_cid)?;
        self.remote_leaf_metadata
            .write()
            .insert(leaf_cid.clone(), dir.clone());

        let sc_cid = if need_replay {
            sidecar_cid.cloned()
        } else {
            None
        };

        Ok(Box::new(RangeReadLeafHandle::new(
            leaf_cid.clone(),
            dir,
            payload_base,
            leaf_id,
            fetcher as Arc<dyn super::leaf_access::RangeReadFetcher>,
            sc_cid,
        )))
    }

    /// Fetch sidecar bytes by CID (full object, sync).
    fn fetch_sidecar_bytes_sync(
        &self,
        sidecar_cid: Option<&ContentId>,
    ) -> io::Result<Option<Vec<u8>>> {
        let sc_cid = match sidecar_cid {
            Some(cid) => cid,
            None => return Ok(None),
        };
        let bytes = self.get_leaf_bytes_sync(sc_cid)?;
        Ok(Some(bytes))
    }

    // ── Value decoding ─────────────────────────────────────────────

    /// Decode a value from `(o_type, o_key)` to `FlakeValue`.
    ///
    /// Routes via `OType::decode_kind()` for static dispatch. Per-graph arenas
    /// (NumBig, Vector) require `(g_id, p_id)` context. Dict-backed types
    /// (String, IRI, JSON, langString) go through forward pack lookups.
    ///
    /// Uses the same decode helpers as V5's `decode_value_no_graph` (ObjKey methods,
    /// chrono conversions, temporal parsing) to ensure output compatibility.
    pub fn decode_value_v3(
        &self,
        o_type: u16,
        o_key: u64,
        p_id: u32,
        g_id: GraphId,
    ) -> io::Result<FlakeValue> {
        let ot = OType::from_u16(o_type);
        let key = ObjKey::from_u64(o_key);

        match ot.decode_kind() {
            DecodeKind::Sentinel | DecodeKind::Null => Ok(FlakeValue::Null),
            DecodeKind::Bool => Ok(FlakeValue::Boolean(o_key != 0)),
            DecodeKind::I64 => Ok(FlakeValue::Long(key.decode_i64())),
            DecodeKind::F64 => Ok(FlakeValue::Double(key.decode_f64())),
            DecodeKind::Date => {
                let days = key.decode_date();
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163).unwrap_or(
                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is valid"),
                );
                let iso = date.format("%Y-%m-%d").to_string();
                match fluree_db_core::temporal::Date::parse(&iso) {
                    Ok(d) => Ok(FlakeValue::Date(Box::new(d))),
                    Err(_) => Ok(FlakeValue::String(iso)),
                }
            }
            DecodeKind::Time => {
                let micros = key.decode_time();
                let secs = (micros / 1_000_000) as u32;
                let frac_micros = (micros % 1_000_000) as u32;
                let time =
                    chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, frac_micros * 1000)
                        .unwrap_or(
                            chrono::NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is valid"),
                        );
                let iso = time.format("%H:%M:%S%.6f").to_string();
                match fluree_db_core::temporal::Time::parse(&iso) {
                    Ok(t) => Ok(FlakeValue::Time(Box::new(t))),
                    Err(_) => Ok(FlakeValue::String(iso)),
                }
            }
            DecodeKind::DateTime => {
                let epoch_micros = key.decode_datetime();
                let dt = chrono::DateTime::from_timestamp_micros(epoch_micros).unwrap_or_default();
                let iso = dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
                match fluree_db_core::temporal::DateTime::parse(&iso) {
                    Ok(d) => Ok(FlakeValue::DateTime(Box::new(d))),
                    Err(_) => Ok(FlakeValue::String(iso)),
                }
            }
            DecodeKind::GYear => Ok(FlakeValue::GYear(Box::new(
                fluree_db_core::temporal::GYear::from_year(key.decode_g_year()),
            ))),
            DecodeKind::GYearMonth => {
                let (year, month) = key.decode_g_year_month();
                Ok(FlakeValue::GYearMonth(Box::new(
                    fluree_db_core::temporal::GYearMonth::from_components(year, month),
                )))
            }
            DecodeKind::GMonth => Ok(FlakeValue::GMonth(Box::new(
                fluree_db_core::temporal::GMonth::from_month(key.decode_g_month()),
            ))),
            DecodeKind::GDay => Ok(FlakeValue::GDay(Box::new(
                fluree_db_core::temporal::GDay::from_day(key.decode_g_day()),
            ))),
            DecodeKind::GMonthDay => {
                let (month, day) = key.decode_g_month_day();
                Ok(FlakeValue::GMonthDay(Box::new(
                    fluree_db_core::temporal::GMonthDay::from_components(month, day),
                )))
            }
            DecodeKind::YearMonthDuration => Ok(FlakeValue::YearMonthDuration(Box::new(
                fluree_db_core::temporal::YearMonthDuration::from_months(
                    key.decode_year_month_dur(),
                ),
            ))),
            DecodeKind::DayTimeDuration => Ok(FlakeValue::DayTimeDuration(Box::new(
                fluree_db_core::temporal::DayTimeDuration::from_micros(key.decode_day_time_dur()),
            ))),
            DecodeKind::Duration => {
                // Compound duration — not yet fully supported in V5 either.
                Ok(FlakeValue::Null)
            }
            DecodeKind::GeoPoint => Ok(FlakeValue::GeoPoint(fluree_db_core::GeoPointBits(o_key))),
            DecodeKind::BlankNode => {
                // Blank node: o_key is an opaque bnode integer, not a subject dict ID.
                // Synthesize a blank node IRI `_:b{o_key}` and encode as a Sid.
                let bnode_iri = format!("_:b{o_key}");
                Ok(FlakeValue::Ref(Sid::new(0, &bnode_iri)))
            }
            DecodeKind::IriRef => {
                let iri = self.resolve_subject_iri(o_key).map_err(|e| {
                    tracing::debug!(
                        g_id,
                        o_key,
                        error = %e,
                        "binary index failed to resolve IRI ref subject"
                    );
                    e
                })?;
                Ok(FlakeValue::Ref(self.encode_iri(&iri)))
            }
            DecodeKind::StringDict => {
                let s = self.resolve_string_value(o_key as u32).map_err(|e| {
                    tracing::debug!(
                        g_id,
                        str_id = o_key as u32,
                        error = %e,
                        "binary index failed to resolve string dictionary value"
                    );
                    e
                })?;
                Ok(FlakeValue::String(s))
            }
            DecodeKind::JsonArena => {
                // Despite the "Arena" name in DecodeKind, JSON values are currently
                // stored in the string dictionary (same as ObjKind::JSON_ID).
                // A dedicated JSON arena may be introduced later.
                let json_str = self.resolve_string_value(o_key as u32).map_err(|e| {
                    tracing::debug!(
                        g_id,
                        str_id = o_key as u32,
                        error = %e,
                        "binary index failed to resolve JSON dictionary value"
                    );
                    e
                })?;
                Ok(FlakeValue::Json(json_str))
            }
            DecodeKind::NumBigArena => {
                let handle = o_key as u32;
                let arena = self
                    .graph_indexes
                    .get(&g_id)
                    .and_then(|gi| gi.numbig.get(&p_id))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("no NumBig arena for g_id={g_id}, p_id={p_id}"),
                        )
                    })?;
                let stored = arena.get_by_handle(handle).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("NumBig handle {handle} not found for g_id={g_id}, p_id={p_id}"),
                    )
                })?;
                Ok(stored.to_flake_value())
            }
            DecodeKind::VectorArena => {
                let handle = o_key as u32;
                let arena = self
                    .graph_indexes
                    .get(&g_id)
                    .and_then(|gi| gi.vectors.get(&p_id))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("no vector arena for g_id={g_id}, p_id={p_id}"),
                        )
                    })?;
                let vs = arena.lookup_vector(handle)?.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("vector handle {handle} not found for g_id={g_id}, p_id={p_id}"),
                    )
                })?;
                let f64_vec: Vec<f64> = vs.as_f32().iter().map(|&x| x as f64).collect();
                Ok(FlakeValue::Vector(f64_vec))
            }
            DecodeKind::SpatialArena => Err(io::Error::other(
                "spatial arena decode not yet implemented in V6",
            )),
        }
    }

    /// Resolve a subject ID (u64) to its full IRI string.
    pub fn resolve_subject_iri(&self, s_id: u64) -> io::Result<String> {
        let sid = fluree_db_core::subject_id::SubjectId::from_u64(s_id);
        let ns_code = sid.ns_code();
        let local_id = sid.local_id();

        let reader = self
            .dicts
            .subject_forward_packs
            .get(&ns_code)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no subject forward pack for ns_code={ns_code}"),
                )
            })?;

        let suffix = reader.forward_lookup_str(local_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("subject local_id {local_id} not found in ns {ns_code}"),
            )
        })?;
        if ns_code == namespaces::EMPTY || ns_code == namespaces::OVERFLOW {
            return Ok(suffix);
        }
        let prefix = self.dicts.namespace_codes.get(&ns_code).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no namespace prefix for code={ns_code}"),
            )
        })?;

        Ok(format!("{prefix}{suffix}"))
    }

    /// Compare two subject IDs by lexicographic full IRI order without allocating.
    ///
    /// This avoids constructing `String`s for MIN/MAX comparisons over `Binding::EncodedSid`.
    pub fn compare_subject_iri_lex(&self, a: u64, b: u64) -> io::Result<Ordering> {
        if a == b {
            return Ok(Ordering::Equal);
        }

        let a_sid = fluree_db_core::subject_id::SubjectId::from_u64(a);
        let b_sid = fluree_db_core::subject_id::SubjectId::from_u64(b);

        let a_prefix = self
            .dicts
            .namespace_codes
            .get(&a_sid.ns_code())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no namespace prefix for code={}", a_sid.ns_code()),
                )
            })?;
        let b_prefix = self
            .dicts
            .namespace_codes
            .get(&b_sid.ns_code())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no namespace prefix for code={}", b_sid.ns_code()),
                )
            })?;

        let a_reader = self
            .dicts
            .subject_forward_packs
            .get(&a_sid.ns_code())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no subject forward pack for ns_code={}", a_sid.ns_code()),
                )
            })?;
        let b_reader = self
            .dicts
            .subject_forward_packs
            .get(&b_sid.ns_code())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no subject forward pack for ns_code={}", b_sid.ns_code()),
                )
            })?;

        let mut a_suffix = Vec::new();
        let mut b_suffix = Vec::new();
        let a_found = a_reader.forward_lookup_into(a_sid.local_id(), &mut a_suffix)?;
        let b_found = b_reader.forward_lookup_into(b_sid.local_id(), &mut b_suffix)?;
        if !a_found || !b_found {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "subject local_id not found in forward pack",
            ));
        }

        Ok(compare_prefix_suffix_bytes(
            a_prefix.as_bytes(),
            &a_suffix,
            b_prefix.as_bytes(),
            &b_suffix,
        ))
    }

    /// Compare two string dictionary IDs by lexicographic string value without allocating.
    ///
    /// Useful for MIN/MAX over `Binding::EncodedLit` values of string-like kinds.
    pub fn compare_string_lex(&self, a: u32, b: u32) -> io::Result<Ordering> {
        if a == b {
            return Ok(Ordering::Equal);
        }
        let mut a_bytes = Vec::new();
        let mut b_bytes = Vec::new();
        let a_found = self
            .dicts
            .string_forward_packs
            .forward_lookup_into(a as u64, &mut a_bytes)?;
        let b_found = self
            .dicts
            .string_forward_packs
            .forward_lookup_into(b as u64, &mut b_bytes)?;
        if !a_found || !b_found {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "string id not found in forward packs",
            ));
        }
        Ok(a_bytes.cmp(&b_bytes))
    }

    /// Hot-path: lookup string bytes into `out`. Returns `true` if ID found.
    pub fn string_lookup_into(&self, str_id: u32, out: &mut Vec<u8>) -> io::Result<bool> {
        self.dicts
            .string_forward_packs
            .forward_lookup_into(str_id as u64, out)
    }

    /// Resolve a string dictionary ID to its value.
    pub fn resolve_string_value(&self, str_id: u32) -> io::Result<String> {
        let result = self
            .dicts
            .string_forward_packs
            .forward_lookup_str(str_id as u64)?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("string id {str_id} not found in forward packs"),
                )
            });
        if let Err(err) = &result {
            tracing::debug!(
                str_id,
                error = %err,
                "resolve_string_value failed"
            );
        }
        result
    }

    /// Resolve a predicate ID to its IRI.
    pub fn resolve_predicate_iri(&self, p_id: u32) -> Option<&str> {
        self.dicts.predicates.resolve(p_id)
    }

    /// Lookup a predicate IRI → p_id.
    pub fn find_predicate_id(&self, iri: &str) -> Option<u32> {
        self.dicts.predicate_reverse.get(iri).copied()
    }

    /// Encode an IRI to a namespaced `Sid` using canonical splitting and exact-prefix lookup.
    ///
    /// Uses `canonical_split` + exact-prefix lookup via reverse map. If the
    /// canonical prefix is not registered, returns `Sid(EMPTY, iri)`.
    /// No longest-prefix-match — canonical encoding prohibits `starts_with` matching.
    pub fn encode_iri(&self, iri: &str) -> Sid {
        debug_assert!(
            self.ns_split_mode_set,
            "BinaryIndexStore::encode_iri called before ns_split_mode was set"
        );
        let (canonical_prefix, canonical_suffix) = canonical_split(iri, self.ns_split_mode);
        if let Some(&code) = self.dicts.namespace_reverse.get(canonical_prefix) {
            return Sid::new(code, canonical_suffix);
        }
        Sid::new(0, iri)
    }

    /// Resolve language ID to BCP 47 tag string.
    ///
    /// `lang_id` in the OType payload is 1-based (lang_id=1 is the first tag).
    /// `language_tags` is a 0-based Vec, so we subtract 1.
    pub fn resolve_lang_tag(&self, o_type: u16) -> Option<&str> {
        let ot = OType::from_u16(o_type);
        if ot.is_lang_string() {
            let lang_id = ot.lang_id()? as usize;
            if lang_id == 0 {
                return None; // lang_id=0 means "no tag"
            }
            self.language_tags
                .get(lang_id - 1)
                .map(std::string::String::as_str)
        } else {
            None
        }
    }

    /// Resolve o_type to a datatype Sid (for materializing datatype IRIs in output).
    /// O(1) via the pre-built o_type index.
    pub fn resolve_datatype_sid(&self, o_type: u16) -> Option<Sid> {
        let ot = OType::from_u16(o_type);

        // Customer-defined datatypes encode the DatatypeDictId directly in the payload.
        // We can resolve it without any IRI parsing/encoding round-trip.
        if ot.is_customer_datatype() {
            let dt_id = ot.payload() as usize;
            return self.dicts.dt_sids.get(dt_id).cloned();
        }

        // rdf:langString encodes the language tag in the payload (lang_id). The datatype
        // itself is constant and must be rdf:langString (not tag-qualified).
        if ot.is_lang_string() {
            return Some(Sid::new(namespaces::RDF, rdf_names::LANG_STRING));
        }

        // Built-ins: construct SIDs directly in reserved namespace code space.
        // This avoids relying on the FIR6 root's `o_type_table.datatype_iri` strings,
        // which historically used compact forms like "xsd:string" (not full IRIs),
        // and avoids an encode/decode round-trip in a hot path.
        match ot {
            OType::NULL => Some(Sid::new(namespaces::XSD, xsd_names::STRING)),
            OType::XSD_BOOLEAN => Some(Sid::new(namespaces::XSD, xsd_names::BOOLEAN)),
            OType::XSD_INTEGER => Some(Sid::new(namespaces::XSD, xsd_names::INTEGER)),
            OType::XSD_LONG => Some(Sid::new(namespaces::XSD, xsd_names::LONG)),
            OType::XSD_INT => Some(Sid::new(namespaces::XSD, xsd_names::INT)),
            OType::XSD_SHORT => Some(Sid::new(namespaces::XSD, xsd_names::SHORT)),
            OType::XSD_BYTE => Some(Sid::new(namespaces::XSD, xsd_names::BYTE)),
            OType::XSD_UNSIGNED_LONG => Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_LONG)),
            OType::XSD_UNSIGNED_INT => Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_INT)),
            OType::XSD_UNSIGNED_SHORT => Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_SHORT)),
            OType::XSD_UNSIGNED_BYTE => Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_BYTE)),
            OType::XSD_NON_NEGATIVE_INTEGER => {
                Some(Sid::new(namespaces::XSD, xsd_names::NON_NEGATIVE_INTEGER))
            }
            OType::XSD_POSITIVE_INTEGER => {
                Some(Sid::new(namespaces::XSD, xsd_names::POSITIVE_INTEGER))
            }
            OType::XSD_NON_POSITIVE_INTEGER => {
                Some(Sid::new(namespaces::XSD, xsd_names::NON_POSITIVE_INTEGER))
            }
            OType::XSD_NEGATIVE_INTEGER => {
                Some(Sid::new(namespaces::XSD, xsd_names::NEGATIVE_INTEGER))
            }
            OType::XSD_DOUBLE => Some(Sid::new(namespaces::XSD, xsd_names::DOUBLE)),
            OType::XSD_FLOAT => Some(Sid::new(namespaces::XSD, xsd_names::FLOAT)),
            OType::XSD_DECIMAL => Some(Sid::new(namespaces::XSD, xsd_names::DECIMAL)),
            OType::XSD_DATE => Some(Sid::new(namespaces::XSD, xsd_names::DATE)),
            OType::XSD_TIME => Some(Sid::new(namespaces::XSD, xsd_names::TIME)),
            OType::XSD_DATE_TIME => Some(Sid::new(namespaces::XSD, xsd_names::DATE_TIME)),
            OType::XSD_G_YEAR => Some(Sid::new(namespaces::XSD, xsd_names::G_YEAR)),
            OType::XSD_G_YEAR_MONTH => Some(Sid::new(namespaces::XSD, xsd_names::G_YEAR_MONTH)),
            OType::XSD_G_MONTH => Some(Sid::new(namespaces::XSD, xsd_names::G_MONTH)),
            OType::XSD_G_DAY => Some(Sid::new(namespaces::XSD, xsd_names::G_DAY)),
            OType::XSD_G_MONTH_DAY => Some(Sid::new(namespaces::XSD, xsd_names::G_MONTH_DAY)),
            OType::XSD_YEAR_MONTH_DURATION => {
                Some(Sid::new(namespaces::XSD, xsd_names::YEAR_MONTH_DURATION))
            }
            OType::XSD_DAY_TIME_DURATION => {
                Some(Sid::new(namespaces::XSD, xsd_names::DAY_TIME_DURATION))
            }
            OType::XSD_DURATION => Some(Sid::new(namespaces::XSD, xsd_names::DURATION)),

            // Dict/arena-backed built-ins
            OType::XSD_STRING => Some(Sid::new(namespaces::XSD, xsd_names::STRING)),
            OType::XSD_ANY_URI => Some(Sid::new(namespaces::XSD, xsd_names::ANY_URI)),
            OType::XSD_NORMALIZED_STRING => {
                Some(Sid::new(namespaces::XSD, xsd_names::NORMALIZED_STRING))
            }
            OType::XSD_TOKEN => Some(Sid::new(namespaces::XSD, xsd_names::TOKEN)),
            OType::XSD_LANGUAGE => Some(Sid::new(namespaces::XSD, xsd_names::LANGUAGE)),
            OType::XSD_BASE64_BINARY => Some(Sid::new(namespaces::XSD, xsd_names::BASE64_BINARY)),
            OType::XSD_HEX_BINARY => Some(Sid::new(namespaces::XSD, xsd_names::HEX_BINARY)),
            OType::IRI_REF | OType::BLANK_NODE => {
                Some(Sid::new(namespaces::JSON_LD, jsonld_names::ID))
            }
            OType::RDF_JSON => Some(Sid::new(namespaces::RDF, rdf_names::JSON)),
            OType::VECTOR => Some(Sid::new(namespaces::FLUREE_DB, "embeddingVector")),
            OType::FULLTEXT => Some(Sid::new(namespaces::FLUREE_DB, "fullText")),
            OType::GEO_POINT => Some(Sid::new(namespaces::OGC_GEO, geo_names::WKT_LITERAL)),

            // Types without a stable datatype (or not representable as typed literals)
            // return None so callers can either skip constraints or use a safe fallback.
            _ => None,
        }
    }

    /// Look up an o_type table entry by o_type value. O(1).
    pub fn lookup_o_type(&self, o_type: u16) -> Option<&OTypeTableEntry> {
        self.o_type_index
            .get(&o_type)
            .map(|&idx| &self.o_type_table[idx])
    }

    /// Reconstruct the full IRI string from a `Sid` (strict decode).
    ///
    /// - `EMPTY (0)` / `OVERFLOW (0xFFFE)`: returns `Some(sid.name)`.
    /// - Registered code: returns `Some(prefix + name)`.
    /// - Unknown code: returns `None`.
    pub fn sid_to_iri(&self, sid: &Sid) -> Option<String> {
        // EMPTY (0) and OVERFLOW (0xFFFE) store the full IRI as sid.name
        if sid.namespace_code == 0 || sid.namespace_code == 0xFFFE {
            return Some(sid.name.to_string());
        }
        self.dicts
            .namespace_codes
            .get(&sid.namespace_code)
            .map(|prefix| format!("{}{}", prefix, sid.name))
    }

    fn reverse_lookup_subject_key(&self, ns_code: u16, suffix: &[u8]) -> io::Result<Option<u64>> {
        match &self.dicts.subject_reverse_tree {
            Some(tree) => {
                let key = crate::dict::reverse_leaf::subject_reverse_key(ns_code, suffix);
                tree.reverse_lookup(&key)
            }
            None => Ok(None),
        }
    }

    fn find_full_iri_subject_fallback(&self, iri: &str) -> io::Result<Option<(u16, u64)>> {
        for ns_code in [namespaces::EMPTY, namespaces::OVERFLOW] {
            if !self.dicts.subject_forward_packs.contains_key(&ns_code) {
                continue;
            }
            if let Some(s_id) = self.reverse_lookup_subject_key(ns_code, iri.as_bytes())? {
                return Ok(Some((ns_code, s_id)));
            }
        }
        Ok(None)
    }

    /// Reverse subject lookup: find the u64 s_id for a given IRI.
    ///
    /// Uses canonical encoding when the IRI's canonical prefix is registered.
    /// If not, it only consults full-IRI subject namespaces that are actually
    /// present in the persisted dictionaries.
    pub fn find_subject_id(&self, iri: &str) -> io::Result<Option<u64>> {
        let (canonical_prefix, canonical_suffix) = canonical_split(iri, self.ns_split_mode);
        if let Some(&ns_code) = self.dicts.namespace_reverse.get(canonical_prefix) {
            if let Some(s_id) =
                self.reverse_lookup_subject_key(ns_code, canonical_suffix.as_bytes())?
            {
                return Ok(Some(s_id));
            }
        }
        Ok(self
            .find_full_iri_subject_fallback(iri)?
            .map(|(_ns_code, s_id)| s_id))
    }

    /// Resolve the exact persisted subject SID for a full IRI, if present.
    pub fn find_subject_sid(&self, iri: &str) -> io::Result<Option<Sid>> {
        let (canonical_prefix, canonical_suffix) = canonical_split(iri, self.ns_split_mode);
        if let Some(&ns_code) = self.dicts.namespace_reverse.get(canonical_prefix) {
            if self
                .reverse_lookup_subject_key(ns_code, canonical_suffix.as_bytes())?
                .is_some()
            {
                return Ok(Some(Sid::new(ns_code, canonical_suffix)));
            }
        }
        Ok(self
            .find_full_iri_subject_fallback(iri)?
            .map(|(ns_code, _s_id)| Sid::new(ns_code, iri)))
    }

    /// Translate a `Sid` to `p_id` via the predicate reverse map.
    ///
    /// Returns `None` if the namespace code is unknown or the predicate
    /// is not in the persisted dictionary.
    pub fn sid_to_p_id(&self, sid: &Sid) -> Option<u32> {
        let iri = self.sid_to_iri(sid)?;
        self.find_predicate_id(&iri)
    }

    /// Access the datatype SIDs vector.
    pub fn dt_sids(&self) -> &[Sid] {
        &self.dicts.dt_sids
    }

    /// Reverse subject lookup by namespace parts (avoids IRI construction).
    pub fn find_subject_id_by_parts(&self, ns_code: u16, suffix: &str) -> io::Result<Option<u64>> {
        match &self.dicts.subject_reverse_tree {
            Some(tree) => {
                let key =
                    crate::dict::reverse_leaf::subject_reverse_key(ns_code, suffix.as_bytes());
                tree.reverse_lookup(&key)
            }
            None => Ok(None),
        }
    }

    /// Find all subject IDs whose suffix starts with `prefix` within a namespace.
    ///
    /// Uses a range scan on the reverse subject tree: scans the key range
    /// `[ns_code || prefix, ns_code || prefix~)` where `~` (0x7E) sorts after
    /// all printable ASCII. Returns the matching `(suffix_bytes, s_id)` pairs.
    pub fn find_subjects_by_prefix(&self, ns_code: u16, prefix: &str) -> io::Result<Vec<u64>> {
        match &self.dicts.subject_reverse_tree {
            Some(tree) => {
                let start_key =
                    crate::dict::reverse_leaf::subject_reverse_key(ns_code, prefix.as_bytes());
                // End key: prefix followed by 0xFF byte (sorts after all valid UTF-8).
                let mut end_suffix = prefix.as_bytes().to_vec();
                end_suffix.push(0xFF);
                let end_key = crate::dict::reverse_leaf::subject_reverse_key(ns_code, &end_suffix);

                let entries = tree.reverse_range_scan(&start_key, &end_key)?;
                Ok(entries.into_iter().map(|(_, id)| id).collect())
            }
            None => Ok(Vec::new()),
        }
    }

    /// Reverse string lookup: value → string_id.
    pub fn find_string_id(&self, value: &str) -> io::Result<Option<u32>> {
        match &self.dicts.string_reverse_tree {
            Some(tree) => tree
                .reverse_lookup(value.as_bytes())
                .map(|opt| opt.map(|id| id as u32)),
            None => Ok(None),
        }
    }

    /// Find all string IDs whose value starts with `prefix`.
    ///
    /// Uses a range scan on the reverse string tree over the key range
    /// `[prefix, prefix || 0xFF)`. (0xFF sorts after all valid UTF-8 bytes.)
    pub fn find_strings_by_prefix(&self, prefix: &str) -> io::Result<Vec<u32>> {
        match &self.dicts.string_reverse_tree {
            Some(tree) => {
                let start_key = prefix.as_bytes();
                let mut end_key = prefix.as_bytes().to_vec();
                end_key.push(0xFF);
                let entries = tree.reverse_range_scan(start_key, &end_key)?;
                Ok(entries
                    .into_iter()
                    .filter_map(|(_, id)| u32::try_from(id).ok())
                    .collect())
            }
            None => Ok(Vec::new()),
        }
    }

    /// Number of predicates in the persisted dictionary.
    pub fn predicate_count(&self) -> u32 {
        self.dicts.predicates.len()
    }

    /// Build a runtime predicate/datatype ID layer seeded from the persisted root.
    pub fn runtime_small_dicts(&self) -> RuntimeSmallDicts {
        RuntimeSmallDicts::from_seeded_sids(
            (0..self.predicate_count()).filter_map(|p_id| self.predicate_sid(p_id)),
            self.dt_sids().iter().cloned(),
        )
    }

    /// Number of strings in the persisted forward dictionary.
    pub fn string_count(&self) -> u32 {
        self.dicts.string_count
    }

    /// Number of language tags in the persisted dictionary.
    pub fn language_tag_count(&self) -> u16 {
        self.dicts.language_tags.len()
    }

    /// Look up a predicate Sid by p_id, returning the full IRI as a Sid.
    pub fn predicate_sid(&self, p_id: u32) -> Option<Sid> {
        let iri = self.dicts.predicates.resolve(p_id)?;
        Some(self.encode_iri(iri))
    }

    /// Look up a datatype ID by its Sid. Returns `None` if not found.
    pub fn find_dt_id(&self, dt_sid: &Sid) -> Option<u16> {
        self.dicts
            .dt_sids
            .iter()
            .position(|s| s == dt_sid)
            .map(|i| i as u16)
    }

    /// Find the 1-based lang_id for a language tag string. Returns None if not found.
    pub fn resolve_lang_id(&self, tag: &str) -> Option<u16> {
        self.dicts.language_tags.find(tag)
    }

    /// Look up the BCP-47 tag string for a 1-based `lang_id`. Returns
    /// `None` for `lang_id == 0` (sentinel "no literal lang tag") and
    /// for IDs beyond the persisted dictionary.
    pub fn resolve_language_tag(&self, lang_id: u16) -> Option<String> {
        self.dicts
            .language_tags
            .resolve(lang_id)
            .map(std::string::ToString::to_string)
    }

    /// Augment namespace codes with entries from novelty commits.
    ///
    /// Validates namespace bimap uniqueness: a delta entry is rejected
    /// if the code already maps to a different prefix or the prefix already
    /// maps to a different code.
    ///
    /// # Panics
    ///
    /// Returns `Err` on a namespace bimap conflict so the caller
    /// can reject the invalid state rather than crashing the process.
    pub fn augment_namespace_codes(
        &mut self,
        codes: &std::collections::HashMap<u16, String>,
    ) -> io::Result<()> {
        // Validate and collect genuinely new entries using bidirectional checks
        // against both forward (namespace_codes) and reverse (namespace_reverse) maps.
        let mut new_entries: Vec<(u16, String)> = Vec::new();
        for (&code, prefix) in codes {
            // code → prefix direction
            if let Some(existing) = self.dicts.namespace_codes.get(&code) {
                if existing != prefix {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "namespace conflict: code {code} maps to {existing:?} but augment has {prefix:?}"
                        ),
                    ));
                }
                continue; // already present and matching
            }
            // prefix → code direction
            if let Some(&existing_code) = self.dicts.namespace_reverse.get(prefix.as_str()) {
                if existing_code != code {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "namespace conflict: prefix {prefix:?} has code {existing_code} but augment has code {code}"
                        ),
                    ));
                }
                continue; // already present and matching
            }
            new_entries.push((code, prefix.clone()));
        }

        // Apply validated new entries to all three structures.
        for (code, prefix) in new_entries {
            self.dicts.namespace_codes.insert(code, prefix.clone());
            if !prefix.is_empty() {
                self.dicts.prefix_trie.insert(&prefix, code);
            }
            self.dicts.namespace_reverse.insert(prefix, code);
        }
        Ok(())
    }

    /// Access the namespace codes table.
    pub fn namespace_codes(&self) -> &HashMap<u16, String> {
        &self.dicts.namespace_codes
    }

    /// Access the o_type table.
    pub fn o_type_table(&self) -> &[OTypeTableEntry] {
        &self.o_type_table
    }

    /// Spatial provider map for query context configuration.
    pub fn spatial_provider_map(
        &self,
    ) -> HashMap<String, Arc<dyn fluree_db_spatial::SpatialIndexProvider>> {
        let mut map = HashMap::new();
        for (g_id, gi) in &self.graph_indexes {
            for (p_id, provider) in &gi.spatial {
                let key = format!("{g_id}:{p_id}");
                map.insert(key, Arc::clone(provider));
            }
        }
        map
    }

    /// Fulltext provider map for query context configuration.
    ///
    /// Returns a map keyed by `(g_id, p_id, lang_id)` triple, matching the
    /// `ContextConfig::fulltext_providers` expected type. Each arena is a
    /// language-specific BoW bucket — `@fulltext`-datatype and configured
    /// English content share the same bucket under `"en"`'s dict-assigned
    /// lang_id.
    pub fn fulltext_provider_map(
        &self,
    ) -> HashMap<(GraphId, u32, u16), Arc<crate::arena::fulltext::FulltextArena>> {
        let mut map = HashMap::new();
        for (g_id, gi) in &self.graph_indexes {
            for (&(p_id, lang_id), arena) in &gi.fulltext {
                map.insert((*g_id, p_id, lang_id), Arc::clone(arena));
            }
        }
        map
    }

    /// Returns the IRI prefix for a namespace code.
    pub fn namespace_prefix(&self, ns_code: u16) -> io::Result<String> {
        self.dicts
            .namespace_codes
            .get(&ns_code)
            .cloned()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no namespace prefix for code={ns_code}"),
                )
            })
    }

    /// Returns all graph IDs present in the index.
    pub fn graph_ids(&self) -> Vec<GraphId> {
        self.graph_indexes.keys().copied().collect()
    }

    /// Lookup graph IRI → `GraphId`. Returns `None` if the IRI is not known.
    pub fn graph_id_for_iri(&self, iri: &str) -> Option<GraphId> {
        self.dicts.graphs_reverse.get(iri).copied()
    }

    /// Lookup `GraphId` → graph IRI. Returns `None` if the ID is not known.
    pub fn graph_iri_for_id(&self, g_id: GraphId) -> Option<&str> {
        // graphs_reverse maps IRI→g_id. We need the reverse.
        self.dicts
            .graphs_reverse
            .iter()
            .find(|(_, &id)| id == g_id)
            .map(|(iri, _)| iri.as_str())
    }

    /// Returns an iterator over `(g_id, iri)` pairs for all named graphs.
    pub fn graph_entries(&self) -> Vec<(GraphId, &str)> {
        self.dicts
            .graphs_reverse
            .iter()
            .map(|(iri, &g_id)| (g_id, iri.as_str()))
            .collect()
    }

    /// Write the raw UTF-8 bytes of a string value into the provided buffer.
    ///
    /// Resolves the string ID from the forward dictionary and appends the
    /// bytes to `out`. Returns an error if the string ID is not found.
    pub fn write_string_value_bytes(&self, str_id: u32, out: &mut Vec<u8>) -> io::Result<()> {
        let s = self.resolve_string_value(str_id)?;
        out.extend_from_slice(s.as_bytes());
        Ok(())
    }

    /// Preload dictionary tree leaves into the leaflet cache.
    ///
    /// This is a warm-up optimization: the first query after loading an index
    /// would otherwise pay cold-start I/O to read dict tree leaves from CAS.
    /// Calling this eagerly populates the cache. Returns the number of leaves loaded.
    ///
    /// Returns `Ok(0)` if there is no leaflet cache or no reverse trees to preload.
    pub fn preload_dict_leaves(&self) -> io::Result<usize> {
        // TODO(V3 migration): implement cache warming for dict tree leaves.
        // The V3 format uses ForwardPack readers (already mmap'd) for forward dicts
        // and CoW trees for reverse dicts. Reverse-tree leaf preloading can be
        // added when cold-start latency is observed in production.
        Ok(0)
    }

    /// Create a `BinaryGraphView` for a specific graph (no novelty).
    pub fn graph(self: &Arc<Self>, g_id: GraphId) -> BinaryGraphView {
        BinaryGraphView::new(Arc::clone(self), g_id)
    }

    /// Create a novelty-aware `BinaryGraphView` for a specific graph.
    pub fn graph_with_novelty(
        self: &Arc<Self>,
        g_id: GraphId,
        dict_novelty: Option<Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    ) -> BinaryGraphView {
        BinaryGraphView::with_novelty(Arc::clone(self), g_id, dict_novelty)
    }

    /// Find the 1-based lang_id for a language tag string. Returns `None` if not found.
    ///
    /// Delegates to [`resolve_lang_id`](Self::resolve_lang_id).
    pub fn find_lang_id(&self, tag: &str) -> Option<u16> {
        self.resolve_lang_id(tag)
    }

    /// Decode metadata from `lang_id` and `i_val` fields into `FlakeMeta`.
    ///
    /// `lang_id` is 1-based (0 means "no language tag"). `i_val` of `i32::MIN`
    /// means "no list index". Returns `None` if neither language nor index is set.
    pub fn decode_meta(&self, lang_id: u16, i_val: i32) -> Option<FlakeMeta> {
        let has_lang = lang_id != 0;
        let has_idx = i_val != i32::MIN; // i32::MIN is the ListIndex::none() sentinel

        if !has_lang && !has_idx {
            return None;
        }

        let mut meta = FlakeMeta::new();
        if has_lang {
            // language_tags is 0-based; lang_id is 1-based
            if let Some(tag) = self.language_tags.get((lang_id as usize).wrapping_sub(1)) {
                meta = FlakeMeta::with_lang(tag.clone());
            }
        }
        if has_idx {
            meta.i = Some(i_val);
        }
        Some(meta)
    }

    /// Decode a value from `(o_type, o_key)` without graph/predicate context.
    ///
    /// This is a convenience wrapper around [`decode_value_v3`](Self::decode_value_v3)
    /// that uses default graph (0) and predicate (0). Arena-backed types
    /// (NumBig, Vector) will fail if they require per-graph/per-predicate arenas,
    /// but dict-backed types (String, IRI, numeric, temporal, etc.) work correctly.
    pub fn decode_value_no_graph(&self, o_type: u16, o_key: u64) -> io::Result<FlakeValue> {
        self.decode_value_v3(o_type, o_key, 0, 0)
    }

    /// Decode a value from `(o_kind, dt_id, lang_id)` fields.
    ///
    /// Converts the triple to `OType` using `OTypeRegistry::builtin_only()`
    /// and delegates to [`decode_value_v3`](Self::decode_value_v3).
    ///
    /// Used by `Binding::EncodedLit` which stores `(o_kind, dt_id, lang_id)`.
    pub fn decode_value_from_kind(
        &self,
        o_kind: u8,
        o_key: u64,
        p_id: u32,
        dt_id: u16,
        lang_id: u16,
        g_id: GraphId,
    ) -> io::Result<FlakeValue> {
        let obj_kind = ObjKind::from_u8(o_kind);
        let registry = OTypeRegistry::builtin_only();
        let o_type = registry.resolve(obj_kind, DatatypeDictId::from_u16(dt_id), lang_id);

        // Handle legacy data where integral doubles were stored as NUM_INT but
        // the property's datatype is float/double. The OType resolves to F64
        // decode, but the key was encoded with encode_i64. Decode as integer
        // and convert to f64 to avoid bit-reinterpretation corruption. (fluree/db-r#142)
        if obj_kind == ObjKind::NUM_INT && o_type.decode_kind() == DecodeKind::F64 {
            let int_val = ObjKey::from_u64(o_key).decode_i64();
            return Ok(FlakeValue::Double(int_val as f64));
        }

        self.decode_value_v3(o_type.as_u16(), o_key, p_id, g_id)
    }
}

#[inline]
fn compare_prefix_suffix_bytes(
    a_prefix: &[u8],
    a_suffix: &[u8],
    b_prefix: &[u8],
    b_suffix: &[u8],
) -> Ordering {
    let a_total = a_prefix.len() + a_suffix.len();
    let b_total = b_prefix.len() + b_suffix.len();
    let n = a_total.min(b_total);

    for i in 0..n {
        let ab = if i < a_prefix.len() {
            a_prefix[i]
        } else {
            a_suffix[i - a_prefix.len()]
        };
        let bb = if i < b_prefix.len() {
            b_prefix[i]
        } else {
            b_suffix[i - b_prefix.len()]
        };
        if ab != bb {
            return ab.cmp(&bb);
        }
    }
    a_total.cmp(&b_total)
}

// ============================================================================
// NsLookup implementation
// ============================================================================

impl NsLookup for BinaryIndexStore {
    fn code_for_prefix(&self, prefix: &str) -> Option<u16> {
        self.dicts.namespace_reverse.get(prefix).copied()
    }

    fn prefix_for_code(&self, code: u16) -> Option<&str> {
        self.dicts
            .namespace_codes
            .get(&code)
            .map(std::string::String::as_str)
    }
}

// ============================================================================
// BinaryGraphView — graph-scoped wrapper
// ============================================================================

/// Graph-scoped view for V6 store. Binds a specific `g_id` for value decoding.
///
/// When `dict_novelty` is present, all decode methods automatically perform
/// watermark-based routing: IDs at or below the watermark delegate to the
/// persisted store; IDs above the watermark resolve from `DictNovelty`.
/// This makes every caller novelty-safe by default.
///
/// When `dict_novelty` is `None`, all methods delegate straight to the store
/// with zero overhead (single well-predicted branch).
pub struct BinaryGraphView {
    store: Arc<BinaryIndexStore>,
    g_id: GraphId,
    dict_novelty: Option<Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    namespace_codes_fallback: Option<Arc<HashMap<u16, String>>>,
    /// Optional fuel tracker. When set, each forward-pack dict touch (a call
    /// into the persisted dictionary that wasn't satisfied by in-memory
    /// novelty) charges 1 fuel = 1000 micro-fuel.
    tracker: Option<fluree_db_core::Tracker>,
}

impl BinaryGraphView {
    pub fn new(store: Arc<BinaryIndexStore>, g_id: GraphId) -> Self {
        Self {
            store,
            g_id,
            dict_novelty: None,
            namespace_codes_fallback: None,
            tracker: None,
        }
    }

    /// Create a novelty-aware graph view.
    ///
    /// When `dict_novelty` is `Some`, decode methods use watermark routing
    /// so that novel string/subject IDs (above the persisted watermark) are
    /// resolved from `DictNovelty` instead of failing with "not found in
    /// forward packs".
    pub fn with_novelty(
        store: Arc<BinaryIndexStore>,
        g_id: GraphId,
        dict_novelty: Option<Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    ) -> Self {
        Self {
            store,
            g_id,
            dict_novelty,
            namespace_codes_fallback: None,
            tracker: None,
        }
    }

    /// Attach a fuel tracker. When set, each forward-pack dict touch (a call
    /// into the persisted dict that didn't short-circuit through novelty)
    /// charges 1 fuel.
    pub fn with_tracker(mut self, tracker: fluree_db_core::Tracker) -> Self {
        if tracker.is_enabled() {
            self.tracker = Some(tracker);
        }
        self
    }

    #[inline]
    fn charge_dict_touch(&self) -> io::Result<()> {
        if let Some(t) = &self.tracker {
            t.consume_fuel(1000).map_err(io::Error::other)?;
        }
        Ok(())
    }

    /// Provide snapshot-derived namespace codes for novelty subject decoding when
    /// the attached store predates a namespace-adding commit.
    pub fn with_namespace_codes_fallback(
        mut self,
        namespace_codes_fallback: Option<Arc<HashMap<u16, String>>>,
    ) -> Self {
        self.namespace_codes_fallback = namespace_codes_fallback;
        self
    }

    pub fn namespace_codes_fallback(&self) -> Option<Arc<HashMap<u16, String>>> {
        self.namespace_codes_fallback.clone()
    }

    /// Decode a value from `(o_type, o_key)`. Novelty-aware when `dict_novelty`
    /// is set: dict-backed types (IriRef, StringDict, JsonArena) route through
    /// watermark checks; all other types delegate directly to the store.
    pub fn decode_value(&self, o_type: u16, o_key: u64, p_id: u32) -> io::Result<FlakeValue> {
        let ot = OType::from_u16(o_type);
        let kind = ot.decode_kind();
        if let Some(ref dn) = self.dict_novelty {
            if dn.is_initialized() {
                match kind {
                    DecodeKind::IriRef => {
                        if let Some(sid) = self.resolve_novel_subject_sid(dn, o_key) {
                            return Ok(FlakeValue::Ref(sid));
                        }
                    }
                    DecodeKind::StringDict => {
                        if let Some(s) = self.resolve_novel_string(dn, o_key as u32) {
                            return Ok(FlakeValue::String(s));
                        }
                    }
                    DecodeKind::JsonArena => {
                        if let Some(s) = self.resolve_novel_string(dn, o_key as u32) {
                            return Ok(FlakeValue::Json(s));
                        }
                    }
                    _ => {} // Non-dict types: straight to store
                }
            }
        }
        // Charge a dict touch only when the value is dict-backed (not inline-
        // encoded) and we didn't already satisfy it from novelty above.
        if matches!(
            kind,
            DecodeKind::IriRef | DecodeKind::StringDict | DecodeKind::JsonArena
        ) {
            self.charge_dict_touch()?;
        }
        self.store.decode_value_v3(o_type, o_key, p_id, self.g_id)
    }

    /// Decode a value from `(o_kind, dt_id, lang_id)` fields. Novelty-aware.
    ///
    /// See [`BinaryIndexStore::decode_value_from_kind`] for the persisted path.
    pub fn decode_value_from_kind(
        &self,
        o_kind: u8,
        o_key: u64,
        p_id: u32,
        dt_id: u16,
        lang_id: u16,
    ) -> io::Result<FlakeValue> {
        let novelty_initialized = self
            .dict_novelty
            .as_ref()
            .is_some_and(|dn| dn.is_initialized());
        if let Some(ref dn) = self.dict_novelty {
            if dn.is_initialized() {
                // Route dict-backed ObjKinds through watermark checks.
                if o_kind == ObjKind::REF_ID.as_u8() {
                    if let Some(sid) = self.resolve_novel_subject_sid(dn, o_key) {
                        return Ok(FlakeValue::Ref(sid));
                    }
                } else if o_kind == ObjKind::LEX_ID.as_u8() {
                    if let Some(s) = self.resolve_novel_string(dn, o_key as u32) {
                        return Ok(FlakeValue::String(s));
                    }
                } else if o_kind == ObjKind::JSON_ID.as_u8() {
                    if let Some(s) = self.resolve_novel_string(dn, o_key as u32) {
                        return Ok(FlakeValue::Json(s));
                    }
                }
            }
        }
        // Charge a dict touch for dict-backed ObjKinds when novelty didn't satisfy.
        if o_kind == ObjKind::REF_ID.as_u8()
            || o_kind == ObjKind::LEX_ID.as_u8()
            || o_kind == ObjKind::JSON_ID.as_u8()
        {
            self.charge_dict_touch()?;
        }
        let result = self
            .store
            .decode_value_from_kind(o_kind, o_key, p_id, dt_id, lang_id, self.g_id);
        if let Err(err) = &result {
            tracing::debug!(
                g_id = self.g_id,
                o_kind,
                o_key,
                p_id,
                dt_id,
                lang_id,
                has_dict_novelty = self.dict_novelty.is_some(),
                novelty_initialized,
                error = %err,
                "BinaryGraphView decode_value_from_kind failed"
            );
        }
        result
    }

    /// Resolve a subject ID to its full IRI string. Novelty-aware.
    pub fn resolve_subject_iri(&self, s_id: u64) -> io::Result<String> {
        if let Some(ref dn) = self.dict_novelty {
            if dn.is_initialized() {
                if let Some(result) = self.resolve_novel_subject_iri(dn, s_id) {
                    if let Err(err) = &result {
                        tracing::debug!(
                            g_id = self.g_id,
                            s_id,
                            error = %err,
                            "BinaryGraphView novelty subject lookup failed"
                        );
                    }
                    return result;
                }
            }
        }
        // Persisted-store path: this is a forward-pack touch. Charge fuel.
        self.charge_dict_touch()?;
        let result = self.store.resolve_subject_iri(s_id).or_else(|store_err| {
            let Some(dn) = self.dict_novelty.as_ref() else {
                return Err(store_err);
            };
            match dn.subjects.resolve_subject(s_id) {
                Some((ns_code, suffix)) => self
                    .namespace_prefix(ns_code)
                    .map(|prefix| format!("{prefix}{suffix}")),
                None => Err(store_err),
            }
        });
        if let Err(err) = &result {
            tracing::debug!(
                g_id = self.g_id,
                s_id,
                has_dict_novelty = self.dict_novelty.is_some(),
                error = %err,
                "BinaryGraphView persisted subject lookup failed"
            );
        }
        result
    }

    /// Resolve a subject ID to a `Sid`. Novelty-aware.
    ///
    /// More efficient than `resolve_subject_iri` + `encode_iri` because the
    /// novelty path returns `Sid::new(ns_code, suffix)` directly without
    /// building the full IRI string or doing a prefix trie lookup.
    pub fn resolve_subject_sid(&self, s_id: u64) -> io::Result<Sid> {
        if let Some(ref dn) = self.dict_novelty {
            if dn.is_initialized() {
                if let Some(sid) = self.resolve_novel_subject_sid(dn, s_id) {
                    return Ok(sid);
                }
            }
        }
        // Persisted forward-pack lookup; charge before the I/O.
        self.charge_dict_touch()?;
        let iri = self.store.resolve_subject_iri(s_id)?;
        if let Some(sid) = self.store.find_subject_sid(&iri)? {
            Ok(sid)
        } else {
            Ok(self.store.encode_iri(&iri))
        }
    }

    pub fn store(&self) -> &BinaryIndexStore {
        &self.store
    }

    pub fn clone_store(&self) -> Arc<BinaryIndexStore> {
        Arc::clone(&self.store)
    }

    pub fn g_id(&self) -> GraphId {
        self.g_id
    }

    /// Check whether this view has DictNovelty attached.
    pub fn has_dict_novelty(&self) -> bool {
        self.dict_novelty.is_some()
    }

    // ── Internal watermark helpers ──────────────────────────────────────

    /// If `s_id` is above the watermark for its namespace, resolve from
    /// DictNovelty to a `Sid` directly (no IRI string allocation, no prefix
    /// trie lookup). Returns `None` if the ID is persisted (below watermark).
    #[inline]
    fn resolve_novel_subject_sid(
        &self,
        dn: &fluree_db_core::dict_novelty::DictNovelty,
        s_id: u64,
    ) -> Option<Sid> {
        use fluree_db_core::subject_id::SubjectId;
        let sid64 = SubjectId::from_u64(s_id);
        let wm = dn.subjects.watermark_for_ns(sid64.ns_code());
        if sid64.local_id() <= wm {
            return None; // Persisted — let the store handle it
        }
        // Novel — resolve (ns_code, suffix) directly to Sid.
        // This avoids format!("{prefix}{suffix}") + encode_iri() trie lookup.
        dn.subjects
            .resolve_subject(s_id)
            .map(|(ns_code, suffix)| Sid::new(ns_code, suffix))
    }

    /// If `s_id` is above the watermark, resolve from DictNovelty to a full
    /// IRI string. Returns `None` if the ID is persisted (below watermark).
    ///
    /// Use `resolve_novel_subject_sid` when you need a `Sid` (avoids the
    /// prefix concatenation).
    #[inline]
    fn resolve_novel_subject_iri(
        &self,
        dn: &fluree_db_core::dict_novelty::DictNovelty,
        s_id: u64,
    ) -> Option<io::Result<String>> {
        use fluree_db_core::subject_id::SubjectId;
        let sid64 = SubjectId::from_u64(s_id);
        let wm = dn.subjects.watermark_for_ns(sid64.ns_code());
        if sid64.local_id() <= wm {
            return None; // Persisted — let the store handle it
        }
        // Novel — need full IRI string (prefix + suffix).
        match dn.subjects.resolve_subject(s_id) {
            Some((ns_code, suffix)) => match self.namespace_prefix(ns_code) {
                Ok(prefix) => Some(Ok(format!("{prefix}{suffix}"))),
                Err(e) => Some(Err(e)),
            },
            None => None, // Not in DictNovelty either — fall through to store
        }
    }

    pub fn namespace_prefix(&self, ns_code: u16) -> io::Result<String> {
        match self.store.namespace_prefix(ns_code) {
            Ok(prefix) => Ok(prefix),
            Err(err) => self
                .namespace_codes_fallback
                .as_ref()
                .and_then(|codes| codes.get(&ns_code).cloned())
                .ok_or(err),
        }
    }

    /// If `str_id` is above the string watermark, resolve from DictNovelty.
    /// Returns `None` if the ID is persisted (below watermark).
    #[inline]
    fn resolve_novel_string(
        &self,
        dn: &fluree_db_core::dict_novelty::DictNovelty,
        str_id: u32,
    ) -> Option<String> {
        if str_id <= dn.strings.watermark() {
            return None; // Persisted — let the store handle it
        }
        dn.strings
            .resolve_string(str_id)
            .map(std::string::ToString::to_string)
    }
}

// ============================================================================
// Dict loading (reuses V5 infrastructure)
// ============================================================================

/// Build the dictionary set from an IndexRoot.
///
/// This mirrors the dict-loading portion of `BinaryIndexStore::load_from_root_v5`
/// but takes fields from `IndexRoot`. The dict infrastructure is identical
/// (same `DictRefs`, same pack/tree formats).
async fn build_dictionary_set(
    cs: Arc<dyn ContentStore>,
    root: &IndexRoot,
    cache_dir: &Path,
    leaflet_cache: Option<&Arc<LeafletCache>>,
) -> io::Result<DictionarySet> {
    // Predicates (inline in root).
    let (predicates, predicate_reverse) = {
        let mut dict = PredicateDict::new();
        let mut rev = HashMap::with_capacity(root.predicate_sids.len());
        for (p_id, (ns_code, suffix)) in root.predicate_sids.iter().enumerate() {
            let prefix = root.namespace_codes.get(ns_code).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("predicate[{p_id}]: unknown ns_code {ns_code}"),
                )
            })?;
            let iri = format!("{prefix}{suffix}");
            dict.get_or_insert(&iri);
            rev.insert(iri, p_id as u32);
        }
        (dict, rev)
    };

    // Subject forward packs.
    let mut subject_forward_packs = std::collections::BTreeMap::new();
    for (ns_code, ns_refs) in &root.dict_refs.forward_packs.subject_fwd_ns_packs {
        let reader = ForwardPackReader::from_pack_refs(
            Arc::clone(&cs),
            cache_dir,
            ns_refs,
            KIND_SUBJECT_FWD,
            *ns_code,
        )
        .await?;
        subject_forward_packs.insert(*ns_code, reader);
    }

    // Subject reverse tree.
    let subject_reverse_tree = Some(
        DictTreeReader::from_refs(
            &cs,
            &root.dict_refs.subject_reverse,
            leaflet_cache,
            Some(cache_dir),
        )
        .await?,
    );

    // String forward packs.
    let string_forward_packs = ForwardPackReader::from_pack_refs(
        Arc::clone(&cs),
        cache_dir,
        &root.dict_refs.forward_packs.string_fwd_packs,
        KIND_STRING_FWD,
        0,
    )
    .await?;

    // String reverse tree.
    let string_reverse_tree = Some(
        DictTreeReader::from_refs(
            &cs,
            &root.dict_refs.string_reverse,
            leaflet_cache,
            Some(cache_dir),
        )
        .await?,
    );

    // Namespace codes.
    let namespace_codes: HashMap<u16, String> = root
        .namespace_codes
        .iter()
        .map(|(&k, v)| (k, v.clone()))
        .collect();
    let namespace_reverse: HashMap<String, u16> = namespace_codes
        .iter()
        .map(|(&code, prefix)| (prefix.clone(), code))
        .collect();
    let prefix_trie = PrefixTrie::from_namespace_codes(&namespace_codes);

    // Language tags.
    let mut language_tags = LanguageTagDict::new();
    for tag in &root.language_tags {
        language_tags.get_or_insert(Some(tag));
    }

    // Datatype SIDs.
    if root.datatype_iris.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index root missing datatype_iris",
        ));
    }
    let dt_sids: Vec<Sid> = root
        .datatype_iris
        .iter()
        .map(|iri| {
            let (canonical_prefix, canonical_suffix) = canonical_split(iri, root.ns_split_mode);
            if let Some(&code) = namespace_reverse.get(canonical_prefix) {
                Sid::new(code, canonical_suffix)
            } else {
                Sid::new(0, iri)
            }
        })
        .collect();

    // Graph IRIs → GraphId (1-based).
    //
    // `IndexRoot.graph_iris` is stored 0-based in the root (vector index),
    // but all query/index code treats `GraphId` as 1-based:
    // - g_id=0 is the default graph (not present in `graph_iris`)
    // - g_id=1 is `#txn-meta`
    // - g_id=2 is `#config`
    // - g_id>=3 are user named graphs
    let graphs_reverse: HashMap<String, GraphId> = root
        .graph_iris
        .iter()
        .enumerate()
        .map(|(idx, iri)| (iri.to_string(), (idx as GraphId) + 1))
        .collect();

    // Subject count from watermarks.
    let subject_count = root.subject_watermarks.iter().sum::<u64>() as u32;

    Ok(DictionarySet {
        predicates,
        predicate_reverse,
        graphs_reverse,
        subject_forward_packs,
        subject_reverse_tree,
        string_forward_packs,
        string_reverse_tree,
        subject_count,
        string_count: root.string_watermark,
        namespace_codes,
        namespace_reverse,
        prefix_trie,
        language_tags,
        dt_sids,
    })
}

// ============================================================================
// Arena loading (reuses V5 infrastructure)
// ============================================================================

/// Per-graph arenas (before injection into GraphIndex).
struct LoadedArenas {
    numbig: HashMap<u32, crate::arena::numbig::NumBigArena>,
    vectors: HashMap<u32, crate::arena::vector::LazyVectorArena>,
    spatial: HashMap<u32, Arc<dyn fluree_db_spatial::SpatialIndexProvider>>,
    /// Keyed by `(p_id, lang_id)` — one bucket per language on each property.
    fulltext: HashMap<(u32, u16), Arc<crate::arena::fulltext::FulltextArena>>,
}

/// Load per-graph specialty arenas from GraphArenaRefs.
async fn load_per_graph_arenas(
    cs: Arc<dyn ContentStore>,
    graph_arenas: &[crate::format::wire_helpers::GraphArenaRefs],
    cache_dir: &Path,
    leaflet_cache: Option<&Arc<LeafletCache>>,
) -> io::Result<HashMap<GraphId, LoadedArenas>> {
    let mut result = HashMap::new();

    for ga in graph_arenas {
        let mut numbig = HashMap::new();
        for (p_id, cid) in &ga.numbig {
            let bytes = fetch_cached_bytes(cs.as_ref(), cid, cache_dir, "nba").await?;
            let arena = crate::arena::numbig::read_numbig_arena_from_bytes(&bytes)?;
            numbig.insert(*p_id, arena);
        }

        let mut vectors = HashMap::new();
        for entry in &ga.vectors {
            let manifest_bytes =
                fetch_cached_bytes(cs.as_ref(), &entry.manifest, cache_dir, "vam").await?;
            let manifest = crate::arena::vector::read_vector_manifest(&manifest_bytes)?;

            let mut shard_sources = Vec::with_capacity(entry.shards.len());
            for shard_cid in &entry.shards {
                let cid_hash = LeafletCache::cid_cache_key(&shard_cid.to_bytes());
                if let Some(local) = cs.resolve_local_path(shard_cid) {
                    shard_sources.push(crate::arena::vector::ShardSource {
                        cid_hash,
                        cid: None,
                        path: local,
                        on_disk: std::sync::atomic::AtomicBool::new(true),
                    });
                } else {
                    let cache_path = cache_dir.join(format!("{shard_cid}.vas"));
                    let exists = cache_path.exists();
                    shard_sources.push(crate::arena::vector::ShardSource {
                        cid_hash,
                        cid: Some(shard_cid.clone()),
                        path: cache_path,
                        on_disk: std::sync::atomic::AtomicBool::new(exists),
                    });
                }
            }

            // LazyVectorArena needs a LeafletCache for shard caching and
            // an optional ContentStore for remote shard fetching.
            let shard_cache = leaflet_cache
                .cloned()
                .unwrap_or_else(|| Arc::new(LeafletCache::with_max_mb(64)));
            let arena = crate::arena::vector::LazyVectorArena::new(
                manifest,
                shard_sources,
                shard_cache,
                Some(Arc::clone(&cs)),
            );
            vectors.insert(entry.p_id, arena);
        }

        // Spatial and fulltext arenas.
        let mut spatial = HashMap::new();
        for sp_ref in &ga.spatial {
            let root_bytes =
                fetch_cached_bytes(cs.as_ref(), &sp_ref.root_cid, cache_dir, "spr").await?;
            let spatial_root: fluree_db_spatial::SpatialIndexRoot =
                serde_json::from_slice(&root_bytes).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("spatial root: {e}"))
                })?;

            // Pre-fetch all spatial blobs, keyed by digest_hex.
            let mut blob_cache: HashMap<String, Vec<u8>> = HashMap::new();
            for cid in [&sp_ref.manifest, &sp_ref.arena]
                .into_iter()
                .chain(sp_ref.leaflets.iter())
            {
                let bytes = fetch_cached_bytes(cs.as_ref(), cid, cache_dir, "spa").await?;
                blob_cache.insert(cid.digest_hex(), bytes);
            }
            let blob_cache = Arc::new(blob_cache);

            let snapshot =
                fluree_db_spatial::SpatialIndexSnapshot::load_from_cas(spatial_root, move |hash| {
                    blob_cache.get(hash).cloned().ok_or_else(|| {
                        fluree_db_spatial::SpatialError::ChunkNotFound(hash.to_string())
                    })
                })
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("spatial snapshot load: {e}"),
                    )
                })?;
            let provider: Arc<dyn fluree_db_spatial::SpatialIndexProvider> =
                Arc::new(fluree_db_spatial::EmbeddedSpatialProvider::new(snapshot));
            spatial.insert(sp_ref.p_id, provider);
        }

        let mut fulltext = HashMap::new();
        for ft_ref in &ga.fulltext {
            let bytes =
                fetch_cached_bytes(cs.as_ref(), &ft_ref.arena_cid, cache_dir, "fta").await?;
            let arena = crate::arena::fulltext::FulltextArena::decode(&bytes)?;
            fulltext.insert((ft_ref.p_id, ft_ref.lang_id), Arc::new(arena));
        }

        result.insert(
            ga.g_id,
            LoadedArenas {
                numbig,
                vectors,
                spatial,
                fulltext,
            },
        );
    }

    Ok(result)
}

// ============================================================================
// ContentStoreRangeFetcher
// ============================================================================

/// Sync-safe range fetcher backed by a `ContentStore`.
///
/// Bridges async `ContentStore::get_range()` to the synchronous cursor world
/// using the same thread-spawn + `Handle::block_on()` pattern proven in
/// `BinaryIndexStore::get_leaf_bytes_sync()`.
struct ContentStoreRangeFetcher {
    cs: Arc<dyn ContentStore>,
    cache_dir: PathBuf,
}

impl ContentStoreRangeFetcher {
    fn new(cs: Arc<dyn ContentStore>, cache_dir: PathBuf) -> Self {
        Self { cs, cache_dir }
    }
}

impl super::leaf_access::RangeReadFetcher for ContentStoreRangeFetcher {
    fn fetch_range(&self, id: &ContentId, range: std::ops::Range<u64>) -> io::Result<Vec<u8>> {
        fn read_range_from_file(
            path: &Path,
            range: std::ops::Range<u64>,
        ) -> io::Result<Option<Vec<u8>>> {
            let file = match std::fs::File::open(path) {
                Ok(file) => file,
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(err) => return Err(err),
            };
            let len = (range.end - range.start) as usize;
            let mut buf = vec![0u8; len];
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                let n = file.read_at(&mut buf, range.start)?;
                buf.truncate(n);
            }
            #[cfg(not(unix))]
            {
                use std::io::{Read, Seek, SeekFrom};
                let mut file = file;
                file.seek(SeekFrom::Start(range.start))?;
                let n = file.read(&mut buf)?;
                buf.truncate(n);
            }
            Ok(Some(buf))
        }

        // Try local path first — positional read.
        if let Some(local_path) = self.cs.resolve_local_path(id) {
            match read_range_from_file(&local_path, range.clone())? {
                Some(buf) => return Ok(buf),
                None => {
                    tracing::debug!(
                        path = %local_path.display(),
                        id = %id,
                        "local artifact path disappeared during range read; falling back to remote fetch"
                    );
                }
            }
        }

        // Check cache.
        let cache_path = self.cache_dir.join(id.to_string());
        if let Some(buf) = read_range_from_file(&cache_path, range.clone())? {
            return Ok(buf);
        }

        // Remote CAS: use async get_range via sync bridge.
        let cs = Arc::clone(&self.cs);
        let cid = id.clone();
        let timeout = cas_sync_timeout();
        let bytes = run_sync_on_runtime(async move {
            let fut = cs.get_range(&cid, range.clone());
            if let Some(dur) = timeout {
                tokio::time::timeout(dur, fut)
                    .await
                    .map_err(|_| {
                        io::Error::other(format!(
                            "CAS range fetch timed out after {}ms (cid={}, range={:?})",
                            dur.as_millis(),
                            cid,
                            range
                        ))
                    })?
                    .map_err(|e| io::Error::other(format!("CAS range fetch failed: {e}")))
            } else {
                fut.await
                    .map_err(|e| io::Error::other(format!("CAS range fetch failed: {e}")))
            }
        })?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use fluree_db_core::content_kind::ContentKind;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::MemoryContentStore;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};

    use crate::dict::builder;
    use crate::dict::forward_pack::{encode_forward_pack, KIND_SUBJECT_FWD};
    use crate::dict::pack_reader::ForwardPackReader;
    use crate::dict::reader::DictTreeReader;
    use crate::dict::reverse_leaf::ReverseEntry;
    use crate::format::leaf::LeafWriter;
    use crate::format::run_record_v2::RunRecordV2;

    #[derive(Debug, Clone)]
    struct CountingContentStore {
        inner: MemoryContentStore,
        get_calls: Arc<AtomicUsize>,
        range_calls: Arc<AtomicUsize>,
    }

    impl CountingContentStore {
        fn new() -> Self {
            Self {
                inner: MemoryContentStore::new(),
                get_calls: Arc::new(AtomicUsize::new(0)),
                range_calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get_calls(&self) -> usize {
            self.get_calls.load(AtomicOrdering::Relaxed)
        }

        fn range_calls(&self) -> usize {
            self.range_calls.load(AtomicOrdering::Relaxed)
        }
    }

    #[async_trait]
    impl ContentStore for CountingContentStore {
        async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
            self.inner.has(id).await
        }

        async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
            self.get_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.get(id).await
        }

        async fn put(&self, kind: ContentKind, bytes: &[u8]) -> fluree_db_core::Result<ContentId> {
            self.inner.put(kind, bytes).await
        }

        async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
            self.inner.put_with_id(id, bytes).await
        }

        async fn release(&self, id: &ContentId) -> fluree_db_core::Result<()> {
            self.inner.release(id).await
        }

        async fn get_range(
            &self,
            id: &ContentId,
            range: std::ops::Range<u64>,
        ) -> fluree_db_core::Result<Vec<u8>> {
            self.range_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.get_range(id, range).await
        }
    }

    fn temp_cache_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let suffix = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fluree-binary-index-remote-meta-cache-{}-{}",
            std::process::id(),
            suffix
        ));
        std::fs::create_dir_all(&path).expect("create temp cache dir");
        path
    }

    fn empty_store(cs: Arc<dyn ContentStore>, cache_dir: PathBuf) -> BinaryIndexStore {
        BinaryIndexStore {
            dicts: DictionarySet {
                predicates: PredicateDict::new(),
                predicate_reverse: HashMap::new(),
                graphs_reverse: HashMap::new(),
                subject_forward_packs: BTreeMap::new(),
                subject_reverse_tree: None,
                string_forward_packs: crate::dict::pack_reader::ForwardPackReader::empty(),
                string_reverse_tree: None,
                subject_count: 0,
                string_count: 0,
                namespace_codes: HashMap::new(),
                namespace_reverse: HashMap::new(),
                prefix_trie: PrefixTrie::new(),
                language_tags: LanguageTagDict::new(),
                dt_sids: Vec::new(),
            },
            graph_indexes: HashMap::new(),
            o_type_table: Vec::new(),
            o_type_index: HashMap::new(),
            cas: Some(cs),
            disk_cache: crate::read::artifact_cache::DiskArtifactCache::for_dir(&cache_dir),
            cache_dir,
            leaflet_cache: None,
            remote_leaf_metadata: RwLock::new(HashMap::new()),
            remote_leaf_open_counts: RwLock::new(HashMap::new()),
            max_t: 1,
            base_t: 0,
            language_tags: Vec::new(),
            lex_sorted_string_ids: false,
            ns_split_mode: NsSplitMode::default(),
            ns_split_mode_set: true,
        }
    }

    fn make_rec(s_id: u64, p_id: u32, o_type: u16, o_key: u64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i: u32::MAX,
            o_type,
            g_id: 0,
        }
    }

    fn build_reverse_reader(entries: Vec<ReverseEntry>) -> DictTreeReader {
        let result =
            builder::build_reverse_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES).unwrap();

        let mut leaf_map = HashMap::new();
        for (leaf_artifact, branch_leaf) in result.leaves.iter().zip(result.branch.leaves.iter()) {
            leaf_map.insert(branch_leaf.address.clone(), leaf_artifact.bytes.clone());
        }

        DictTreeReader::from_memory(result.branch, leaf_map)
    }

    fn make_subject_pack_bytes(entries: &[(u64, &[u8])]) -> Vec<u8> {
        encode_forward_pack(entries, KIND_SUBJECT_FWD, 0, 256 * 1024).unwrap()
    }

    fn build_test_leaf_bytes() -> Vec<u8> {
        let mut writer = LeafWriter::new(RunSortOrder::Post, 100, 1000, 1);
        writer.set_skip_history(true);
        for i in 0..5u64 {
            writer
                .push_record(make_rec(i + 1, 1, OType::XSD_INTEGER.as_u16(), i * 10, 1))
                .unwrap();
        }
        writer.finish().unwrap().remove(0).leaf_bytes
    }

    #[test]
    fn open_leaf_handle_caches_remote_metadata() {
        let store = CountingContentStore::new();
        let leaf_bytes = build_test_leaf_bytes();
        let leaf_cid = run_sync_on_runtime({
            let store = store.clone();
            async move {
                store
                    .put(ContentKind::IndexLeaf, &leaf_bytes)
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            }
        })
        .expect("store leaf bytes");
        let cache_dir = temp_cache_dir();
        let binary_store = empty_store(Arc::new(store.clone()), cache_dir.clone());

        let handle = binary_store
            .open_leaf_handle(&leaf_cid, None, false)
            .expect("first remote open");
        assert_eq!(handle.dir().entries.len(), 1);
        let first_range_calls = store.range_calls();
        assert!(
            first_range_calls >= 2,
            "expected initial remote open to fetch header+directory"
        );
        drop(handle);

        let handle = binary_store
            .open_leaf_handle(&leaf_cid, None, false)
            .expect("second remote open");
        assert_eq!(handle.dir().entries.len(), 1);
        let second_range_calls = store.range_calls();
        assert_eq!(
            second_range_calls, first_range_calls,
            "cached remote metadata should avoid extra range reads"
        );
        assert_eq!(
            store.get_calls(),
            1,
            "hot remote leaf should be promoted to full local cache on second open"
        );
        assert!(
            cache_dir.join(leaf_cid.to_string()).exists(),
            "promoted remote leaf should be written to disk cache"
        );
        drop(handle);

        let handle = binary_store
            .open_leaf_handle(&leaf_cid, None, false)
            .expect("third remote open");
        assert_eq!(handle.dir().entries.len(), 1);
        assert_eq!(
            store.range_calls(),
            second_range_calls,
            "once promoted, repeated opens should not perform remote range reads"
        );
        assert_eq!(
            store.get_calls(),
            1,
            "once promoted, repeated opens should not refetch the full blob"
        );

        let _ = std::fs::remove_dir_all(cache_dir);
    }

    #[test]
    fn find_subject_id_uses_full_iri_fallback_when_store_has_it() {
        let cache_dir = temp_cache_dir();
        let mut store = empty_store(Arc::new(MemoryContentStore::new()), cache_dir);

        let full_iri = "https://dblp.org/streams/conf/IEEEpact";
        let s_id = SubjectId::new(namespaces::OVERFLOW, 7).as_u64();

        store.dicts.subject_reverse_tree = Some(build_reverse_reader(vec![ReverseEntry {
            key: crate::dict::reverse_leaf::subject_reverse_key(
                namespaces::OVERFLOW,
                full_iri.as_bytes(),
            ),
            id: s_id,
        }]));
        store.dicts.subject_forward_packs.insert(
            namespaces::OVERFLOW,
            ForwardPackReader::from_memory(vec![Arc::from(
                make_subject_pack_bytes(&[(7, full_iri.as_bytes())]).into_boxed_slice(),
            )])
            .unwrap(),
        );

        assert_eq!(store.find_subject_id(full_iri).unwrap(), Some(s_id));
        assert_eq!(
            store.find_subject_sid(full_iri).unwrap(),
            Some(Sid::new(namespaces::OVERFLOW, full_iri))
        );
    }

    /// Regression test for fluree/db-r#142: legacy data where integral doubles
    /// were stored as NUM_INT but the property datatype is xsd:double/float.
    /// decode_value_from_kind must detect the mismatch and convert i64 → f64
    /// instead of reinterpreting integer bits as IEEE 754.
    #[test]
    fn decode_value_from_kind_num_int_with_float_datatype() {
        use fluree_db_core::ids::DatatypeDictId;

        let cache_dir = temp_cache_dir();
        let cs: Arc<dyn ContentStore> = Arc::new(CountingContentStore::new());
        let store = empty_store(cs, cache_dir);

        let int_key = ObjKey::encode_i64(1_350_000).as_u64();

        // NUM_INT + dt=DOUBLE → should return Double(1350000.0), not garbage.
        let val = store
            .decode_value_from_kind(
                ObjKind::NUM_INT.as_u8(),
                int_key,
                0, // p_id
                DatatypeDictId::DOUBLE.as_u16(),
                0, // lang_id
                0, // g_id
            )
            .unwrap();
        assert_eq!(val, FlakeValue::Double(1_350_000.0));

        // NUM_INT + dt=FLOAT → same fix applies.
        let val = store
            .decode_value_from_kind(
                ObjKind::NUM_INT.as_u8(),
                int_key,
                0,
                DatatypeDictId::FLOAT.as_u16(),
                0,
                0,
            )
            .unwrap();
        assert_eq!(val, FlakeValue::Double(1_350_000.0));

        // NUM_INT + dt=INTEGER → should still decode as Long (no mismatch).
        let val = store
            .decode_value_from_kind(
                ObjKind::NUM_INT.as_u8(),
                int_key,
                0,
                DatatypeDictId::INTEGER.as_u16(),
                0,
                0,
            )
            .unwrap();
        assert_eq!(val, FlakeValue::Long(1_350_000));
    }
}
