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
use serde::ser::{Serialize, SerializeMap, Serializer};
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
    /// Seeds every key, so a request cannot be built offline to collide
    /// with another's key and be served its answer.
    seed: u64,
    pool: OnceLock<Cache<u128, Arc<[u8]>>>,
}

impl ResultCache {
    /// A pool with a ceiling of `mb`; `0` disables result caching.
    pub(crate) fn with_max_mb(mb: usize) -> Self {
        Self {
            max_bytes: (mb as u64).saturating_mul(1024 * 1024),
            seed: uuid::Uuid::new_v4().as_u64_pair().0,
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

/// Opts that control the cache or tracking, not the answer.
const UNKEYED_OPTS: [&str; 5] = ["cache", "meta", "max-fuel", "maxFuel", "max_fuel"];

/// The query text, without its [`UNKEYED_OPTS`].
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
            hash_json(&KeyedQuery(json), h).map_err(|_| "query")?;
        }
    }
    Ok(())
}

/// Hash `value`'s JSON text, written straight into `h`.
pub(crate) fn hash_json<H: Hasher>(value: &impl Serialize, h: &mut H) -> serde_json::Result<()> {
    struct Writer<'a, H>(&'a mut H);
    impl<H: Hasher> std::io::Write for Writer<'_, H> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.write(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Writer(h), value)
}

/// A JSON-LD query as the key reads it: skips the [`UNKEYED_OPTS`] in
/// place, so the remaining opts keep their order, and drops `opts` when
/// nothing else is in it. A request opted in by its body or by the
/// `Fluree-Cache` header shares one entry.
struct KeyedQuery<'a>(&'a JsonValue);

impl Serialize for KeyedQuery<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let JsonValue::Object(query) = self.0 else {
            return self.0.serialize(s);
        };
        let keyed = |(key, _): &(&String, &JsonValue)| !UNKEYED_OPTS.contains(&key.as_str());
        let mut map = s.serialize_map(None)?;
        for (key, value) in query {
            match value {
                JsonValue::Object(opts) if key == "opts" => {
                    if opts.iter().any(|entry| keyed(&entry)) {
                        map.serialize_entry(key, &KeyedOpts(opts, keyed))?;
                    }
                }
                _ => map.serialize_entry(key, value)?,
            }
        }
        map.end()
    }
}

struct KeyedOpts<'a, F>(&'a serde_json::Map<String, JsonValue>, F);

impl<F: Fn(&(&String, &JsonValue)) -> bool> Serialize for KeyedOpts<'_, F> {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(None)?;
        for (key, value) in self.0.iter().filter(|entry| (self.1)(entry)) {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
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
        let mut h = Xxh3::with_seed(self.result_cache.seed);
        KEY_VERSION.hash(&mut h);
        std::mem::discriminant(&rendering).hash(&mut h);
        // The query text last: a request that bypasses is refused before
        // its largest input is hashed.
        let keyed = if self.result_cache.enabled() {
            format
                .hash_for_result_cache(&mut h)
                .and_then(|()| options.hash_for_result_cache(&mut h))
                .and_then(|()| db.hash_for_result_cache(&mut h))
                .and_then(|()| hash_input(&input, &mut h))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key(query: &JsonValue) -> u128 {
        let mut h = Xxh3::new();
        hash_input(&QueryInput::JsonLd(query), &mut h).unwrap();
        h.digest128()
    }

    #[test]
    fn unkeyed_opts_are_skipped_wherever_they_sit() {
        let base = json!({"select": ["?s"], "where": {"@id": "?s"}});
        let mut in_body = base.clone();
        in_body["opts"] = json!({"cache": true, "format": "x", "ledger": "y"});
        // The `Fluree-Cache` header appends `cache` after the body's opts.
        let mut from_header = base.clone();
        from_header["opts"] = json!({"format": "x", "ledger": "y", "cache": true});
        assert_eq!(key(&in_body), key(&from_header));

        let mut only_cache = base.clone();
        only_cache["opts"] = json!({"cache": true, "meta": true, "max-fuel": 5});
        assert_eq!(key(&base), key(&only_cache));

        let mut other = base.clone();
        other["opts"] = json!({"cache": true, "format": "z", "ledger": "y"});
        assert_ne!(key(&in_body), key(&other));
    }
}
