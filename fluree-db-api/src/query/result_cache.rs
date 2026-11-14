//! Query-result cache.
//!
//! A query's formatted answer, kept under a key that covers the request and
//! the state of the ledger it read. A request opts in.
//!
//! A hit is the answer an uncached run against the same state produces, so it
//! is never staler than running the query again: a commit, an index build or
//! anything else that changes what the view reads changes the key. Answers
//! for a state no longer read are never looked up again and age out.
//!
//! Answers live in a pool of their own with its own ceiling, beside the
//! leaflet cache's budget rather than inside it, and the pool is built on the
//! first request that asks: a process that never opts in keeps no answers and
//! loses no index cache to them.

use crate::format::FormatterConfig;
use crate::view::query::{always_execute, LoweredViewQuery, ViewRun};
use crate::view::{GraphDb, QueryInput};
use crate::{Fluree, QueryExecutionOptions, QueryResult, Result};
#[cfg(target_arch = "wasm32")]
use fluree_db_core::wasm_cache::Cache;
use fluree_db_query::ir::result_cache::result_cache_bypass;
#[cfg(not(target_arch = "wasm32"))]
use moka::sync::Cache;
use serde_json::Value as JsonValue;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};
use xxhash_rust::xxh3::Xxh3;

/// Bumped when what the key covers changes.
const KEY_VERSION: u32 = 1;

/// One answer may take at most this fraction of the pool, so a single large
/// answer cannot flush the rest.
const MAX_ENTRY_DIVISOR: u64 = 16;

/// Estimated bytes per entry beyond the answer itself (key, `Arc` header,
/// the pool's own node), charged so small answers are not counted as free.
const ENTRY_OVERHEAD_BYTES: u64 = 128;

/// The pool of opted-in answers.
pub(crate) struct ResultCache {
    max_bytes: u64,
    pool: OnceLock<Cache<u128, Arc<[u8]>>>,
}

impl ResultCache {
    /// A pool with a ceiling of `mb`; `0` disables result caching.
    pub(crate) fn with_max_mb(mb: usize) -> Self {
        Self {
            max_bytes: (mb as u64).saturating_mul(1024 * 1024),
            pool: OnceLock::new(),
        }
    }

    fn enabled(&self) -> bool {
        self.max_bytes > 0
    }

    fn max_entry_bytes(&self) -> u64 {
        (self.max_bytes / MAX_ENTRY_DIVISOR).min(u64::from(u32::MAX) - ENTRY_OVERHEAD_BYTES)
    }

    /// The pool, built on first use.
    fn pool(&self) -> &Cache<u128, Arc<[u8]>> {
        self.pool.get_or_init(|| {
            let builder = Cache::builder()
                .weigher(|_key: &u128, bytes: &Arc<[u8]>| {
                    (bytes.len() as u64 + ENTRY_OVERHEAD_BYTES) as u32
                })
                .max_capacity(self.max_bytes);
            // An answer for a superseded state is never read again, so
            // recency alone retires it. The wasm32 stand-in is LRU already.
            #[cfg(not(target_arch = "wasm32"))]
            let builder = builder.eviction_policy(moka::policy::EvictionPolicy::lru());
            builder.build()
        })
    }

    /// Entries and weighted bytes; `None` until a query has opted in.
    pub(crate) fn usage(&self) -> Option<(u64, u64)> {
        self.pool
            .get()
            .map(|pool| (pool.entry_count(), pool.weighted_size()))
    }
}

/// What the cache did for one query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultCacheStatus {
    /// Answered from the cache.
    Hit,
    /// Executed, and its answer cached.
    Miss,
    /// Executed and not cached, for the reason given.
    Bypass(&'static str),
}

impl ResultCacheStatus {
    /// Value for an `x-fdb-cache` response header.
    pub fn header_value(&self) -> String {
        match self {
            Self::Hit => "hit".to_string(),
            Self::Miss => "miss".to_string(),
            Self::Bypass(reason) => format!("bypass; reason=\"{reason}\""),
        }
    }
}

/// Opts a query into the result cache and reports what the cache did.
///
/// Pass one to a builder's `result_cache`, then read [`Self::status`] after
/// the query returns.
#[derive(Debug, Default)]
pub struct ResultCacheOutcome(Mutex<Option<ResultCacheStatus>>);

impl ResultCacheOutcome {
    pub fn new() -> Self {
        Self::default()
    }

    /// `None` until a query has run with this outcome.
    pub fn status(&self) -> Option<ResultCacheStatus> {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn set(&self, status: ResultCacheStatus) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(status);
    }
}

/// Whether a JSON-LD query asks for the result cache itself
/// (`"opts": {"cache": true}`).
pub fn jsonld_requests_result_cache(query: &JsonValue) -> bool {
    query
        .get("opts")
        .and_then(|opts| opts.get("cache"))
        .and_then(JsonValue::as_bool)
        == Some(true)
}

/// The form a cached answer is kept in. Each terminal reads back the form it
/// writes, so a hit returns exactly what a miss would have.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Rendering {
    /// The formatted JSON DOM serialized by `serde_json` — what
    /// `execute_formatted` returns, and what the server writes for it.
    JsonDom,
    /// What the string formatter writes.
    Text,
}

/// Where a fresh answer goes once the terminal has formatted it.
pub(crate) struct PendingInsert {
    key: u128,
}

pub(crate) enum ViewAnswer {
    Cached(Arc<[u8]>),
    Fresh {
        result: Box<QueryResult>,
        insert: Option<PendingInsert>,
    },
}

/// Everything about `db` that decides what a query sees. `Err` names what
/// the cache cannot key.
fn hash_view<H: Hasher>(db: &GraphDb, h: &mut H) -> std::result::Result<(), &'static str> {
    if db.graph_source_id.is_some() || db.over_graph_source {
        return Err("graph source");
    }
    if !db.cross_ledger_resolved_heads.is_empty() {
        return Err("cross-ledger governance");
    }
    view_state(db)?.hash(h);
    db.graph_id.hash(h);
    db.union_default_graph.hash(h);
    match &db.default_context {
        None => 0u8.hash(h),
        Some(ctx) => {
            1u8.hash(h);
            serde_json::to_vec(ctx)
                .map_err(|_| "default context")?
                .hash(h);
        }
    }
    match db.policy() {
        None => 0u8.hash(h),
        Some(policy) => {
            1u8.hash(h);
            if !policy.hash_view_policy(h) {
                return Err("f:query policy");
            }
        }
    }
    Ok(())
}

/// The ledger state `db` reads. A new commit or index build changes it: the
/// overlay's content version is unique to its contents, and the index root
/// is content-addressed.
fn view_state(db: &GraphDb) -> std::result::Result<u128, &'static str> {
    let content_version = db
        .overlay
        .content_version()
        .ok_or("overlay without a content version")?;
    let index_root = db.snapshot.index_root_id();
    if db.snapshot.t > 0 && index_root.is_none() {
        return Err("index root unknown");
    }
    let mut h = Xxh3::new();
    db.ledger_id.hash(&mut h);
    db.t.hash(&mut h);
    db.snapshot.t.hash(&mut h);
    index_root.map(ToString::to_string).hash(&mut h);
    content_version.hash(&mut h);
    Ok(h.digest128())
}

/// The query text, without the opts that only control the cache or tracking.
fn hash_input<H: Hasher>(
    input: &QueryInput<'_>,
    h: &mut H,
) -> std::result::Result<(), &'static str> {
    match input {
        QueryInput::Sparql(sparql) => {
            0u8.hash(h);
            sparql.hash(h);
        }
        QueryInput::JsonLd(json) => {
            1u8.hash(h);
            let mut json = (*json).clone();
            if let Some(opts) = json.get_mut("opts").and_then(JsonValue::as_object_mut) {
                for key in ["cache", "meta", "max-fuel", "maxFuel", "max_fuel"] {
                    opts.remove(key);
                }
            }
            serde_json::to_vec(&json).map_err(|_| "query")?.hash(h);
        }
    }
    Ok(())
}

impl Fluree {
    /// Run `input` against `db` through the result cache.
    ///
    /// The lookup runs once the executable is built, inside the same round
    /// as execution: every per-request check the build makes runs on a hit
    /// too, and a miss executes the executable the lookup was keyed on.
    pub(crate) async fn query_view_for_cache(
        &self,
        db: &GraphDb,
        input: QueryInput<'_>,
        options: QueryExecutionOptions,
        format: &FormatterConfig,
        rendering: Rendering,
    ) -> Result<(ViewAnswer, ResultCacheStatus)> {
        let fresh = |result| ViewAnswer::Fresh {
            result: Box::new(result),
            insert: None,
        };
        let mut h = Xxh3::new();
        KEY_VERSION.hash(&mut h);
        std::mem::discriminant(&rendering).hash(&mut h);
        let keyed = if self.result_cache.enabled() {
            hash_input(&input, &mut h)
                .and_then(|()| options.hash_for_result_cache(&mut h))
                .and_then(|()| format.hash_for_result_cache(&mut h))
                .and_then(|()| hash_view(db, &mut h))
        } else {
            Err("result cache disabled")
        };
        if let Err(reason) = keyed {
            let result = self.query_with_options(db, input, options).await?;
            return Ok((fresh(result), ResultCacheStatus::Bypass(reason)));
        }

        let lowered = match self.lower_view_query(db, input, &options)? {
            LoweredViewQuery::Dataset { dataset, ast } => {
                let result = self
                    .query_dataset_with_prepared_ast(&dataset, input, options, *ast)
                    .await?;
                return Ok((fresh(result), ResultCacheStatus::Bypass("FROM dataset")));
            }
            LoweredViewQuery::View(lowered) => *lowered,
        };
        if let Some(reason) = result_cache_bypass(&lowered.parsed) {
            let result = self
                .run_lowered_view_query(db, lowered, &options, always_execute)
                .await?
                .into_executed();
            return Ok((fresh(result), ResultCacheStatus::Bypass(reason)));
        }

        let pool = self.result_cache.pool();
        let mut reasoning = false;
        let mut key = 0;
        let run = self
            .run_lowered_view_query(db, lowered, &options, |executable| {
                reasoning = executable.reasoning.modes.has_any_enabled();
                if reasoning {
                    return None;
                }
                let mut h = h.clone();
                executable.query.union_default_graph.hash(&mut h);
                key = h.digest128();
                pool.get(&key)
            })
            .await?;
        Ok(match run {
            ViewRun::Answered(bytes) => {
                // As execution's first checkpoint would have.
                if let Some(reason) = options
                    .cancellation
                    .as_ref()
                    .and_then(fluree_db_core::QueryCancellation::reason)
                {
                    return Err(fluree_db_query::QueryError::Cancelled { reason }.into());
                }
                (ViewAnswer::Cached(bytes), ResultCacheStatus::Hit)
            }
            ViewRun::Executed(result) if reasoning => {
                (fresh(result), ResultCacheStatus::Bypass("reasoning"))
            }
            ViewRun::Executed(result) => (
                ViewAnswer::Fresh {
                    result: Box::new(result),
                    insert: Some(PendingInsert { key }),
                },
                ResultCacheStatus::Miss,
            ),
        })
    }

    /// Keep a fresh answer the terminal has formatted. Returns `false` when
    /// it is too large for one entry.
    pub(crate) fn insert_result_cache(&self, pending: PendingInsert, bytes: &[u8]) -> bool {
        if bytes.len() as u64 > self.result_cache.max_entry_bytes() {
            return false;
        }
        self.result_cache
            .pool()
            .insert(pending.key, Arc::from(bytes));
        true
    }
}
