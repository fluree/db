//! Indexer configuration

use crate::gc::{DEFAULT_MAX_OLD_INDEXES, DEFAULT_MIN_TIME_GARBAGE_MINS};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;

/// Resolves the ledger's effective configured full-text property list at
/// index-build time.
///
/// Background / incremental indexing runs can't see the live `LedgerConfig`
/// through static `IndexerConfig` alone — a caller (typically the api layer)
/// plugs in a concrete resolver via
/// [`IndexerConfig::with_fulltext_config_provider`] so each build refreshes
/// the set from the current ledger state.
///
/// Implementations should be cheap on the happy path (one privileged read of
/// the config graph) and return an empty list when the ledger has no
/// `f:fullTextDefaults`. Failures should log + return empty rather than
/// propagate — a bad config read shouldn't block the whole indexing run.
#[async_trait]
pub trait FulltextConfigProvider: std::fmt::Debug + Send + Sync {
    async fn fulltext_configured_properties(
        &self,
        ledger_id: &str,
    ) -> Vec<ConfiguredFulltextProperty>;
}

/// Per-ledger attachment-events resolver for arena sealing on the
/// background-indexer path.
///
/// Implementations (typically in the api layer) resolve a ledger ID
/// to the running `AttachmentNovelty.iter_event_pairs()` snapshot at
/// job-dispatch time. The indexer takes the returned vec and stamps
/// it into a per-job `IndexerConfig.attachment_events`.
///
/// **Return semantics — same as `attachment_events`:**
/// - `Some(events)`: caller produced the delta; arena gets sealed.
/// - `None`: delta unknown; the indexer drops any base-root arena to
///   force the scan-fallback hydration path until the next seal.
///
/// Implementations should be cheap on the happy path (one privileged
/// read of the running ledger state) and return `None` rather than
/// panic when the ledger isn't loaded — a missing ledger shouldn't
/// block the indexing run; the defensive drop covers correctness.
#[async_trait]
pub trait AttachmentEventsProvider: std::fmt::Debug + Send + Sync {
    async fn attachment_events(
        &self,
        ledger_id: &str,
    ) -> Option<Vec<(fluree_db_core::EdgeKey, fluree_db_core::Sid, i64, bool)>>;
}

/// Scope of a configured full-text property entry.
///
/// Mirrors the `f:targetGraph` sentinels used in config graph writes:
/// - `AnyGraph` comes from a ledger-wide `f:fullTextDefaults` — the
///   property applies to every graph in the ledger.
/// - `DefaultGraph` is a per-graph override whose `f:targetGraph` is
///   `f:defaultGraph` (or omitted) — scoped to `g_id = 0` only.
/// - `TxnMetaGraph` is a per-graph override whose `f:targetGraph` is
///   `f:txnMetaGraph` — scoped to the ledger's txn-meta graph
///   (`g_id = 1`, pre-reserved in the graph dict).
/// - `NamedGraph(iri)` is a per-graph override targeting a user graph by
///   its canonical IRI.
///
/// Keeping these distinct lets the indexer route scoping precisely:
/// the hook's `any_graph` tier covers `AnyGraph`; the `per_graph` tier
/// covers the rest with the correct `GraphId`.
#[derive(Debug, Clone)]
pub enum ConfiguredFulltextScope {
    AnyGraph,
    DefaultGraph,
    TxnMetaGraph,
    NamedGraph(String),
}

/// One entry in the per-indexing-run configured full-text property set.
#[derive(Debug, Clone)]
pub struct ConfiguredFulltextProperty {
    pub scope: ConfiguredFulltextScope,
    pub property_iri: String,
}

/// Configuration for index building
#[derive(Debug, Clone)]
pub struct IndexerConfig {
    /// Target estimated bytes per leaf node
    ///
    /// Leaves will be sized to approximately this many bytes during splits.
    /// Default: 187,500 (half of the legacy default overflow-bytes)
    pub leaf_target_bytes: u64,

    /// Maximum estimated bytes per leaf node
    ///
    /// Leaves split when they exceed this threshold.
    /// Default: 375,000 (legacy default overflow-bytes)
    pub leaf_max_bytes: u64,

    /// Target number of children per branch node
    ///
    /// Branches will split when they exceed this threshold.
    /// Default: 100
    pub branch_target_children: usize,

    /// Maximum number of children per branch node
    ///
    /// Hard limit to prevent oversized branches.
    /// Default: 200
    pub branch_max_children: usize,

    /// Maximum number of old index versions to retain before garbage collection.
    ///
    /// After each index refresh, if there are more than this many old index
    /// versions in the prev-index chain, the oldest ones become eligible for GC.
    /// Default: 5
    pub gc_max_old_indexes: u32,

    /// Minimum age in minutes before an index version can be garbage collected.
    ///
    /// Even if an index exceeds `gc_max_old_indexes`, it won't be deleted until
    /// it's at least this old. This prevents deleting indexes that concurrent
    /// queries might still be using.
    /// Default: 30 minutes
    pub gc_min_time_mins: u32,

    /// Memory budget (bytes) for the run-sort buffer during index building.
    ///
    /// This total is split evenly across all sort orders (SPOT, PSOT, POST, OPST).
    /// Larger budgets produce fewer spill files and speed up the merge phase at
    /// the cost of higher peak memory. For bulk imports of 1 GB+, 1–2 GB is
    /// recommended.
    ///
    /// Default: 256 MB.
    pub run_budget_bytes: usize,

    /// Base directory for binary index artifacts.
    ///
    /// Ephemeral build artifacts (run files, dicts) are stored under:
    /// `{data_dir}/{alias_path}/tmp_import/{session_id}/`
    ///
    /// Durable index files are stored under:
    /// `{data_dir}/{alias_path}/index/`
    ///
    /// If `None`, defaults to `{system_temp_dir}/fluree-index`. For production
    /// deployments, this should always be set to a persistent directory.
    pub data_dir: Option<PathBuf>,

    /// Whether incremental indexing is enabled.
    ///
    /// When `true`, `build_index_for_ledger` will attempt to incrementally
    /// update the existing index by merging only new commits into affected
    /// leaves. Falls back to full rebuild on failure.
    ///
    /// Default: `true`
    pub incremental_enabled: bool,

    /// Maximum number of commits to process incrementally.
    ///
    /// If the gap between `index_t` and `commit_t` exceeds this, a full
    /// rebuild is used instead. Larger windows increase the number of
    /// touched leaves and reduce the incremental advantage.
    ///
    /// Default: 10,000
    pub incremental_max_commits: usize,

    /// Maximum number of concurrent (graph, order) branch updates during
    /// incremental indexing.
    ///
    /// Each branch update fetches affected leaves from CAS, merges novelty,
    /// and uploads new blobs. Higher concurrency speeds up multi-graph
    /// ledgers at the cost of more peak memory (one decoded leaf set per
    /// in-flight task).
    ///
    /// Default: 4 (one per sort order in a single-graph workload)
    pub incremental_max_concurrency: usize,

    /// Target rows per leaflet (FLI3).
    ///
    /// This is primarily a build-format tuning knob. Smaller values produce
    /// more leaflets (and therefore more leaves) for the same dataset, which
    /// can be useful for tests that need multi-leaf coverage with small data.
    ///
    /// Default: 25,000.
    pub leaflet_rows: usize,

    /// Leaflets per leaf file (FLI3).
    ///
    /// Default: 10.
    pub leaflets_per_leaf: usize,

    /// Maximum cumulative commit bytes to load during an incremental
    /// commit-chain walk. If the walk exceeds this budget, incremental
    /// indexing aborts and the caller falls back to a full rebuild.
    ///
    /// Typically set to the ledger's `reindex_max_bytes` so that the
    /// in-memory commit buffer never grows beyond the novelty threshold.
    ///
    /// `None` means no limit (backwards-compatible default).
    pub incremental_max_commit_bytes: Option<usize>,

    /// Edge-annotation attachment events to seal into the new arena.
    ///
    /// `(edge, ann_sid, t, op)` tuples covering every `f:reifies*`
    /// event observed since the base index — sourced from the running
    /// ledger's `AttachmentNovelty.iter_event_pairs()`. The indexer
    /// merges these with the base root's arena (when present) and
    /// writes the resulting forward + reverse arena blobs to CAS.
    ///
    /// **Semantics — `None` means delta is unknown, not empty.**
    ///
    /// - `Some(events)` (including `Some(vec![])`): the caller has
    ///   determined the exact attachment delta since the base root.
    ///   The indexer seals an authoritative arena. Readers downstream
    ///   prefer this arena over the scan path.
    /// - `None`: the caller could not produce the delta this pass.
    ///   The indexer treats this as **delta unknown**: when the
    ///   base root carries an `annotation_index`, the new root drops
    ///   it (recording the old branch + leaf CIDs as replaced for GC)
    ///   so hydration falls back to the scan path. Publishing a
    ///   carried-forward arena would risk hiding newly-indexed
    ///   annotations whose `f:reifies*` events landed in novelty
    ///   between this pass and the previous arena seal.
    ///
    /// Direct callers (CLI tools, tests, custom orchestrators with a
    /// snapshot in hand) populate this field directly. The
    /// `BackgroundIndexerWorker` path uses
    /// [`Self::attachment_events_provider`] to resolve events
    /// per-ledger at job-dispatch time.
    pub attachment_events: Option<Vec<(fluree_db_core::EdgeKey, fluree_db_core::Sid, i64, bool)>>,

    /// Per-ledger attachment-events resolver for orchestrator paths.
    ///
    /// `BackgroundIndexerWorker` holds a single `IndexerConfig` for
    /// its lifetime, so `attachment_events` (a per-job value) can't
    /// be set on the static config. The worker calls this provider
    /// at job dispatch time to fetch the running ledger's
    /// `AttachmentNovelty.iter_event_pairs()` and stamps the result
    /// into a per-job clone of the config before invoking the
    /// indexer. `None` (the default) yields the M2a behavior:
    /// arenas are not sealed via the background path.
    ///
    /// See `attachment_events` for the delta-unknown semantics that
    /// apply when this provider returns `None` for a given ledger.
    pub attachment_events_provider: Option<Arc<dyn AttachmentEventsProvider>>,

    /// Configured full-text properties for this indexing run.
    ///
    /// Caller-computed (typically by `fluree-db-api` resolving the ledger's
    /// `f:fullTextDefaults`) and passed in so the indexer can seed its
    /// `FulltextHookConfig` without cross-layer config reads. Empty by
    /// default — when empty, only the `@fulltext` datatype path contributes
    /// entries, preserving the pre-config behavior.
    ///
    /// For steady-state (background / CLI incremental) indexing, prefer
    /// [`fulltext_config_provider`](Self::fulltext_config_provider) so each
    /// run refreshes this list from the live ledger config.
    pub fulltext_configured_properties: Vec<ConfiguredFulltextProperty>,

    /// Optional callback that re-resolves full-text configured properties
    /// at the start of each index build. When present,
    /// [`build_index_for_record`](crate::build_index_for_record) calls this
    /// first and overwrites `fulltext_configured_properties` with the
    /// result, so background/incremental runs pick up config changes that
    /// happened after the process started.
    ///
    /// `None` by default. The api layer wires its own resolver via
    /// [`IndexerConfig::with_fulltext_config_provider`].
    pub fulltext_config_provider: Option<Arc<dyn FulltextConfigProvider>>,
}

/// Default run-sort budget: 256 MB.
pub const DEFAULT_RUN_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// Default max commits for incremental indexing.
pub const DEFAULT_INCREMENTAL_MAX_COMMITS: usize = 10_000;

/// Default max concurrency for incremental branch updates.
pub const DEFAULT_INCREMENTAL_MAX_CONCURRENCY: usize = 4;

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            leaf_target_bytes: 187_500,
            leaf_max_bytes: 375_000,
            branch_target_children: 100,
            branch_max_children: 200,
            gc_max_old_indexes: DEFAULT_MAX_OLD_INDEXES,
            gc_min_time_mins: DEFAULT_MIN_TIME_GARBAGE_MINS,
            run_budget_bytes: DEFAULT_RUN_BUDGET_BYTES,
            data_dir: None,
            incremental_enabled: true,
            incremental_max_commits: DEFAULT_INCREMENTAL_MAX_COMMITS,
            incremental_max_concurrency: DEFAULT_INCREMENTAL_MAX_CONCURRENCY,
            leaflet_rows: 25_000,
            leaflets_per_leaf: 10,
            incremental_max_commit_bytes: None,
            fulltext_configured_properties: Vec::new(),
            fulltext_config_provider: None,
            attachment_events: None,
            attachment_events_provider: None,
        }
    }
}

impl IndexerConfig {
    /// Create a new configuration with custom values
    pub fn new(
        leaf_target_bytes: u64,
        leaf_max_bytes: u64,
        branch_target_children: usize,
        branch_max_children: usize,
    ) -> Self {
        Self {
            leaf_target_bytes,
            leaf_max_bytes,
            branch_target_children,
            branch_max_children,
            gc_max_old_indexes: DEFAULT_MAX_OLD_INDEXES,
            gc_min_time_mins: DEFAULT_MIN_TIME_GARBAGE_MINS,
            run_budget_bytes: DEFAULT_RUN_BUDGET_BYTES,
            data_dir: None,
            incremental_enabled: true,
            incremental_max_commits: DEFAULT_INCREMENTAL_MAX_COMMITS,
            incremental_max_concurrency: DEFAULT_INCREMENTAL_MAX_CONCURRENCY,
            leaflet_rows: 25_000,
            leaflets_per_leaf: 10,
            incremental_max_commit_bytes: None,
            fulltext_configured_properties: Vec::new(),
            fulltext_config_provider: None,
            attachment_events: None,
            attachment_events_provider: None,
        }
    }

    /// Create a configuration optimized for small datasets
    pub fn small() -> Self {
        Self {
            leaf_target_bytes: 50_000,
            leaf_max_bytes: 100_000,
            branch_target_children: 20,
            branch_max_children: 40,
            gc_max_old_indexes: DEFAULT_MAX_OLD_INDEXES,
            gc_min_time_mins: DEFAULT_MIN_TIME_GARBAGE_MINS,
            run_budget_bytes: DEFAULT_RUN_BUDGET_BYTES,
            data_dir: None,
            incremental_enabled: true,
            incremental_max_commits: DEFAULT_INCREMENTAL_MAX_COMMITS,
            incremental_max_concurrency: DEFAULT_INCREMENTAL_MAX_CONCURRENCY,
            leaflet_rows: 25_000,
            leaflets_per_leaf: 10,
            incremental_max_commit_bytes: None,
            fulltext_configured_properties: Vec::new(),
            fulltext_config_provider: None,
            attachment_events: None,
            attachment_events_provider: None,
        }
    }

    /// Create a configuration optimized for large datasets
    pub fn large() -> Self {
        Self {
            leaf_target_bytes: 750_000,
            leaf_max_bytes: 1_500_000,
            branch_target_children: 200,
            branch_max_children: 400,
            gc_max_old_indexes: DEFAULT_MAX_OLD_INDEXES,
            gc_min_time_mins: DEFAULT_MIN_TIME_GARBAGE_MINS,
            run_budget_bytes: DEFAULT_RUN_BUDGET_BYTES,
            data_dir: None,
            incremental_enabled: true,
            incremental_max_commits: DEFAULT_INCREMENTAL_MAX_COMMITS,
            incremental_max_concurrency: DEFAULT_INCREMENTAL_MAX_CONCURRENCY,
            leaflet_rows: 25_000,
            leaflets_per_leaf: 10,
            incremental_max_commit_bytes: None,
            fulltext_configured_properties: Vec::new(),
            fulltext_config_provider: None,
            attachment_events: None,
            attachment_events_provider: None,
        }
    }

    /// Attach a full-text config provider so each index build re-resolves
    /// `fulltext_configured_properties` from the live ledger state.
    ///
    /// Prefer this over [`fulltext_configured_properties`](Self::fulltext_configured_properties)
    /// for long-lived indexer handles (background worker / CLI), which
    /// otherwise carry a stale snapshot of the config across the whole
    /// process lifetime.
    pub fn with_fulltext_config_provider(
        mut self,
        provider: Arc<dyn FulltextConfigProvider>,
    ) -> Self {
        self.fulltext_config_provider = Some(provider);
        self
    }

    /// Attach a per-ledger attachment-events resolver. The
    /// `BackgroundIndexerWorker` calls the provider at job dispatch
    /// time; direct callers (CLI, tests) typically populate
    /// `attachment_events` instead.
    pub fn with_attachment_events_provider(
        mut self,
        provider: Arc<dyn AttachmentEventsProvider>,
    ) -> Self {
        self.attachment_events_provider = Some(provider);
        self
    }

    pub fn with_leaflet_rows(mut self, rows: usize) -> Self {
        self.leaflet_rows = rows.max(1);
        self
    }

    pub fn with_leaflets_per_leaf(mut self, n: usize) -> Self {
        self.leaflets_per_leaf = n.max(1);
        self
    }

    /// Builder method to set GC max old indexes
    pub fn with_gc_max_old_indexes(mut self, max_old: u32) -> Self {
        self.gc_max_old_indexes = max_old;
        self
    }

    /// Builder method to set GC min time in minutes
    pub fn with_gc_min_time_mins(mut self, min_time: u32) -> Self {
        self.gc_min_time_mins = min_time;
        self
    }

    /// Builder method to set the run-sort memory budget.
    ///
    /// For bulk imports of 1 GB+, use 1–2 GB (e.g., `1024 * 1024 * 1024`).
    pub fn with_run_budget_bytes(mut self, bytes: usize) -> Self {
        self.run_budget_bytes = bytes;
        self
    }

    /// Builder method to set the data directory for binary index artifacts
    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(data_dir.into());
        self
    }

    /// Builder method to enable or disable incremental indexing
    pub fn with_incremental_enabled(mut self, enabled: bool) -> Self {
        self.incremental_enabled = enabled;
        self
    }

    /// Builder method to set the maximum commit window for incremental indexing
    pub fn with_incremental_max_commits(mut self, max_commits: usize) -> Self {
        self.incremental_max_commits = max_commits;
        self
    }

    /// Builder method to set the maximum concurrency for incremental branch updates
    pub fn with_incremental_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.incremental_max_concurrency = max_concurrency.max(1);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = IndexerConfig::default();
        assert_eq!(config.leaf_target_bytes, 187_500);
        assert_eq!(config.leaf_max_bytes, 375_000);
        assert_eq!(config.branch_target_children, 100);
        assert_eq!(config.branch_max_children, 200);
        assert_eq!(config.gc_max_old_indexes, DEFAULT_MAX_OLD_INDEXES);
        assert_eq!(config.gc_min_time_mins, DEFAULT_MIN_TIME_GARBAGE_MINS);
        assert!(config.incremental_enabled);
        assert_eq!(
            config.incremental_max_commits,
            DEFAULT_INCREMENTAL_MAX_COMMITS
        );
        assert_eq!(
            config.incremental_max_concurrency,
            DEFAULT_INCREMENTAL_MAX_CONCURRENCY
        );
    }

    #[test]
    fn test_small_config() {
        let config = IndexerConfig::small();
        assert_eq!(config.leaf_target_bytes, 50_000);
        assert_eq!(config.gc_max_old_indexes, DEFAULT_MAX_OLD_INDEXES);
        assert!(config.incremental_enabled);
    }

    #[test]
    fn test_large_config() {
        let config = IndexerConfig::large();
        assert_eq!(config.leaf_target_bytes, 750_000);
        assert_eq!(config.gc_max_old_indexes, DEFAULT_MAX_OLD_INDEXES);
        assert!(config.incremental_enabled);
    }

    #[test]
    fn test_gc_config_builders() {
        let config = IndexerConfig::default()
            .with_gc_max_old_indexes(10)
            .with_gc_min_time_mins(60);
        assert_eq!(config.gc_max_old_indexes, 10);
        assert_eq!(config.gc_min_time_mins, 60);
    }

    #[test]
    fn test_incremental_config_builders() {
        let config = IndexerConfig::default()
            .with_incremental_enabled(false)
            .with_incremental_max_commits(500)
            .with_incremental_max_concurrency(8);
        assert!(!config.incremental_enabled);
        assert_eq!(config.incremental_max_commits, 500);
        assert_eq!(config.incremental_max_concurrency, 8);

        // Concurrency is clamped to at least 1.
        let config2 = IndexerConfig::default().with_incremental_max_concurrency(0);
        assert_eq!(config2.incremental_max_concurrency, 1);
    }
}
