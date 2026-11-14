//! Query-result cache.
//!
//! A query's formatted answer, kept in the shared cache pool under a key that
//! covers the request and the state of the ledger it read. A request opts in.
//!
//! A hit is the answer an uncached run against the same state produces, so it
//! is never staler than running the query again: a commit, an index build or
//! anything else that changes what the view reads changes the key. Entries for
//! a superseded state are only dropped to free the pool, never for
//! correctness.

use crate::format::FormatterConfig;
use crate::view::query::LoweredViewQuery;
use crate::view::{GraphDb, QueryInput};
use crate::{Fluree, QueryExecutionOptions, QueryResult, Result};
use fluree_db_query::ir::result_cache::result_cache_bypass;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use xxhash_rust::xxh3::Xxh3;

/// Bumped when what the key covers changes.
const KEY_VERSION: u32 = 1;

/// States of one ledger whose entries are tracked; an older state's entries
/// are dropped from the pool when a new one arrives.
const LIVE_STATES_PER_LEDGER: usize = 4;

/// Entries tracked per state. Untracked ones still age out of the pool.
const KEYS_PER_STATE: usize = 4096;

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
    ledger_id: Arc<str>,
    state: u128,
}

pub(crate) enum ViewAnswer {
    Cached(Arc<[u8]>),
    Fresh {
        result: Box<QueryResult>,
        insert: Option<PendingInsert>,
    },
}

/// A ledger's recent states, oldest first, each with the keys it inserted.
type LedgerStates = VecDeque<(u128, Vec<u128>)>;

/// Per-ledger record of which entries each recent state put in the pool.
#[derive(Default)]
pub(crate) struct ResultCacheIndex {
    ledgers: Mutex<HashMap<Arc<str>, LedgerStates>>,
}

impl ResultCacheIndex {
    /// Note that `state` of `ledger_id` is in use, and that it put `key` in
    /// the pool. Returns the keys of the states this pushes out.
    fn observe(&self, ledger_id: &Arc<str>, state: u128, key: Option<u128>) -> Vec<u128> {
        let mut ledgers = self
            .ledgers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let states = ledgers.entry(Arc::clone(ledger_id)).or_default();
        let mut dropped = Vec::new();
        let at = match states.iter().position(|(s, _)| *s == state) {
            Some(at) => at,
            None => {
                states.push_back((state, Vec::new()));
                while states.len() > LIVE_STATES_PER_LEDGER {
                    if let Some((_, keys)) = states.pop_front() {
                        dropped.extend(keys);
                    }
                }
                states.len() - 1
            }
        };
        if let Some(key) = key {
            let keys = &mut states[at].1;
            if keys.len() < KEYS_PER_STATE {
                keys.push(key);
            }
        }
        dropped
    }
}

/// Everything about `db` that decides what a query sees. `Err` names what
/// the cache cannot key.
fn hash_view<H: Hasher>(db: &GraphDb, h: &mut H) -> std::result::Result<u128, &'static str> {
    if db.graph_source_id.is_some() || db.over_graph_source {
        return Err("graph source");
    }
    if !db.cross_ledger_resolved_heads.is_empty() {
        return Err("cross-ledger governance");
    }
    let state = view_state(db)?;
    state.hash(h);
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
    Ok(state)
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
    /// The executable is built before the lookup, so every per-request check
    /// it makes runs on a hit too.
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
        let keyed = hash_input(&input, &mut h)
            .and_then(|()| options.hash_for_result_cache(&mut h))
            .and_then(|()| format.hash_for_result_cache(&mut h))
            .and_then(|()| hash_view(db, &mut h));
        let state = match keyed {
            Ok(state) => state,
            Err(reason) => {
                let result = self.query_with_options(db, input, options).await?;
                return Ok((fresh(result), ResultCacheStatus::Bypass(reason)));
            }
        };

        let lowered = match self.lower_view_query(db, input, &options)? {
            LoweredViewQuery::Dataset { dataset, ast } => {
                let result = self
                    .query_dataset_with_prepared_ast(&dataset, input, options, *ast)
                    .await?;
                return Ok((fresh(result), ResultCacheStatus::Bypass("FROM dataset")));
            }
            LoweredViewQuery::View(lowered) => *lowered,
        };
        let bypass = match result_cache_bypass(&lowered.parsed) {
            Some(reason) => Some(reason),
            None => {
                let executable = self
                    .build_executable_for_view(
                        db,
                        &lowered.parsed,
                        options.server_identity.as_ref(),
                    )
                    .await?;
                if executable.reasoning.modes.has_any_enabled() {
                    Some("reasoning")
                } else {
                    executable.query.union_default_graph.hash(&mut h);
                    None
                }
            }
        };
        if let Some(reason) = bypass {
            let result = self.run_lowered_view_query(db, lowered, &options).await?;
            return Ok((fresh(result), ResultCacheStatus::Bypass(reason)));
        }

        let key = h.digest128();
        let pool = self.leaflet_cache();
        for superseded in self.result_cache_index.observe(&db.ledger_id, state, None) {
            pool.invalidate_query_result(superseded);
        }
        if let Some(bytes) = pool.get_query_result(key) {
            return Ok((ViewAnswer::Cached(bytes), ResultCacheStatus::Hit));
        }
        let result = self.run_lowered_view_query(db, lowered, &options).await?;
        let insert = PendingInsert {
            key,
            ledger_id: Arc::clone(&db.ledger_id),
            state,
        };
        Ok((
            ViewAnswer::Fresh {
                result: Box::new(result),
                insert: Some(insert),
            },
            ResultCacheStatus::Miss,
        ))
    }

    /// Keep a fresh answer the terminal has formatted. Returns `false` when
    /// it is too large for the pool.
    pub(crate) fn insert_result_cache(&self, pending: PendingInsert, bytes: &[u8]) -> bool {
        let pool = self.leaflet_cache();
        if bytes.len() as u64 > pool.max_query_result_bytes()
            || !pool.insert_query_result(pending.key, Arc::from(bytes))
        {
            return false;
        }
        for superseded in
            self.result_cache_index
                .observe(&pending.ledger_id, pending.state, Some(pending.key))
        {
            pool.invalidate_query_result(superseded);
        }
        true
    }
}
