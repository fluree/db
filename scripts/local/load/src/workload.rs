//! Workload composition: pick the next op to issue.
//!
//! A [`Workload`] knows two things: the *mix* of operation kinds
//! to issue (with whatever scheduling logic the shape needs), and
//! how to generate a concrete request body for each kind. The
//! runner calls [`Workload::next`] every time a worker is ready
//! to dispatch.

use crate::ledger_state::LedgerState;
use crate::ops::{Op, OpKind};
use serde_json::json;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use ulid::Ulid;
use xxhash_rust::xxh64::xxh64;

/// Whether the workload attaches an `Idempotency-Key` HTTP header to
/// its write requests, and how. Read ops (`Query`) and `CreateLedger`
/// don't carry keys either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyMode {
    /// No key sent. Every request goes through the full Raft
    /// propose → apply cycle. Baseline for measuring raw consensus
    /// throughput.
    Anonymous,
    /// A fresh key per request (no repeats within the run). Exercises
    /// the keyed-happy-path: `CachingCommitter` records each outcome,
    /// the state machine writes an `ApplyRecord`, but nothing ever
    /// dedups. Isolates the overhead of idempotency accounting.
    Unique,
    /// Keys drawn round-robin from a fixed pool of size
    /// `idempotency_pool_size`. Same pool slot always emits the same
    /// body (derived from `idx % pool_size`), so repeats trigger the
    /// dedup path — `CachingCommitter`'s moka cache on the leader,
    /// or the replicated `ApplyRecord` after a leader transition.
    /// First `pool_size` ops per ledger fill the cache; the rest hit
    /// it.
    Pooled,
}

impl FromStr for IdempotencyMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "anonymous" => Ok(Self::Anonymous),
            "unique" => Ok(Self::Unique),
            "pooled" => Ok(Self::Pooled),
            other => Err(format!(
                "unknown idempotency mode: {other} (try: anonymous, unique, pooled)"
            )),
        }
    }
}

/// Named workload shapes. See `--help` on the binary or the README
/// for the per-shape semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadShape {
    /// One CreateLedger at the start, then transact-only against it.
    SinglePound,
    /// Pure CreateLedger stream — every op is a create. Used to
    /// characterize Command::CreateLedger apply throughput in
    /// isolation.
    CreateOnly,
    /// Transact-only against ledgers the runner expects to exist
    /// already. Useful when targeting a pre-seeded cluster.
    TransactOnly,
    /// Query-only against ledgers the runner expects to exist
    /// already. Exercises the read path — no consensus involvement —
    /// so it characterizes local snapshot / cache-refresh behavior
    /// and stays available during chaos even when writes are
    /// refusing.
    QueryOnly,
    /// Interleaved reads + writes against ledgers the runner expects
    /// to exist already: 1 in `mixed_write_every` ops is a transact,
    /// the rest are queries. Exercises read/write concurrency on the
    /// same ledger — query-time cache refresh while writes advance
    /// heads, read availability during commit staging, and
    /// query behavior during chaos with real ongoing write load.
    MixedRw,
    /// Schedule [`WideFanoutTuning::target_ledger_count`] CreateLedger
    /// ops over the run, interleaved with transacts against whichever
    /// ledgers have landed. Exercises per-branch work queues and
    /// rendezvous-hash ownership recalculation under failure.
    WideFanout,
    /// Continuous mix: 1 in `multitenant_create_every` ops is a
    /// CreateLedger, the rest are transacts against the existing
    /// pool. Models multi-tenant onboarding.
    Multitenant,
}

impl FromStr for WorkloadShape {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "single-pound" => Ok(Self::SinglePound),
            "create-only" => Ok(Self::CreateOnly),
            "transact-only" => Ok(Self::TransactOnly),
            "query-only" => Ok(Self::QueryOnly),
            "mixed-rw" => Ok(Self::MixedRw),
            "wide-fanout" => Ok(Self::WideFanout),
            "multitenant" => Ok(Self::Multitenant),
            other => Err(format!(
                "unknown workload: {other} (try: single-pound, create-only, \
                 transact-only, query-only, mixed-rw, wide-fanout, multitenant)"
            )),
        }
    }
}

/// Per-shape tuning knobs. Defaults match the CLI defaults so the
/// runner can construct a Workload from a shape + tuning struct
/// without remembering which fields each shape reads.
#[derive(Debug, Clone)]
pub struct WorkloadTuning {
    /// `wide-fanout`: total number of ledgers to create over the run.
    pub target_ledger_count: u64,
    /// `wide-fanout`: every Nth dispatched op is a CreateLedger
    /// (between op #0 and op #(target_ledger_count * N)). Defaults
    /// keep creates ~rare so transact dominates the load.
    pub wide_fanout_create_every: u64,
    /// `multitenant`: every Nth op is a CreateLedger; the rest are
    /// transacts. Independent of total — the rate of new ledgers is
    /// implicitly throttled by the op rate.
    pub multitenant_create_every: u64,
    /// `mixed-rw`: every Nth op is a transact; the rest are queries.
    /// Default 5 gives roughly 20% writes / 80% reads — enough
    /// write load to keep the consensus path busy without drowning
    /// the query side.
    pub mixed_write_every: u64,
    /// How idempotency keys are (or aren't) attached to write ops.
    /// See [`IdempotencyMode`] for the per-mode semantics.
    pub idempotency_mode: IdempotencyMode,
    /// `IdempotencyMode::Pooled`: the number of distinct
    /// (key, body) pairs the workload rotates through. First
    /// `idempotency_pool_size` ops per ledger populate the cache;
    /// subsequent ops hit it.
    pub idempotency_pool_size: u64,
    /// Prefix used when generating ledger names. Combined with a
    /// ULID for the suffix so collisions across runs are impossible.
    pub ledger_prefix: String,
    /// `transact-only` / `query-only` / `mixed-rw`: ledger names the
    /// workload assumes already exist. The workload picks from this
    /// list round-robin. Empty is an error at CLI-parse time for
    /// those shapes.
    pub seeded_ledgers: Vec<String>,
}

impl Default for WorkloadTuning {
    fn default() -> Self {
        Self {
            target_ledger_count: 50,
            wide_fanout_create_every: 50,
            multitenant_create_every: 100,
            mixed_write_every: 5,
            idempotency_mode: IdempotencyMode::Anonymous,
            idempotency_pool_size: 100,
            ledger_prefix: "load".to_string(),
            seeded_ledgers: Vec::new(),
        }
    }
}

/// Workload state shared across worker tasks.
///
/// The `Arc`/atomic interior is so all workers see the same monotonic
/// op counter — the workload mix is deterministic in op-index, not
/// wall-clock, so concurrent workers can race without producing
/// duplicate target ledgers etc.
#[derive(Clone)]
pub struct Workload {
    shape: WorkloadShape,
    tuning: WorkloadTuning,
    ledgers: LedgerState,
    op_counter: Arc<AtomicU64>,
    /// Count of `CreateLedger` ops the workload has *dispatched*
    /// against `wide-fanout`'s cap — separate from the landed count in
    /// `ledgers`, which trails by the create-op latency. Under
    /// concurrency + non-trivial create latency, many creates can
    /// be in flight before the first lands; a landed-count cap check
    /// would let them all through and overshoot `target_ledger_count`.
    creates_dispatched: Arc<AtomicU64>,
    /// Run-unique prefix the workload mixes into every generated
    /// ledger name + IRI. Stops collisions if two runs share a
    /// cluster.
    run_id: String,
}

impl Workload {
    pub fn new(shape: WorkloadShape, tuning: WorkloadTuning, ledgers: LedgerState) -> Self {
        // Seed the ledger state with whatever transact-only expects to
        // already exist. The runner will still let CreateLedger ops
        // add to the pool, so transact-only + create-only mixes are
        // valid combos via repeated runs.
        for name in &tuning.seeded_ledgers {
            ledgers.register(name);
        }
        Self {
            shape,
            tuning,
            ledgers,
            op_counter: Arc::new(AtomicU64::new(0)),
            creates_dispatched: Arc::new(AtomicU64::new(0)),
            run_id: Ulid::new().to_string(),
        }
    }

    /// Compute the next op to issue. Returns `None` when the workload
    /// has nothing eligible (e.g. transact-only with an empty ledger
    /// pool, or wide-fanout that's reached its create cap and has
    /// no ledgers landed yet). The runner drops the slot and moves
    /// on rather than blocking, so a transient empty pool doesn't
    /// jam open-loop dispatch.
    pub fn next(&self) -> Option<Op> {
        let idx = self.op_counter.fetch_add(1, Ordering::Relaxed);
        match self.shape {
            WorkloadShape::SinglePound => self.next_single_pound(idx),
            WorkloadShape::CreateOnly => Some(self.gen_create(idx)),
            WorkloadShape::TransactOnly => self.next_transact_against_pool(idx),
            WorkloadShape::QueryOnly => self.next_query_against_pool(idx),
            WorkloadShape::MixedRw => self.next_mixed_rw(idx),
            WorkloadShape::WideFanout => self.next_wide_fanout(idx),
            WorkloadShape::Multitenant => self.next_multitenant(idx),
        }
    }

    fn next_single_pound(&self, idx: u64) -> Option<Op> {
        if idx == 0 {
            // First op seeds the single ledger; every subsequent op
            // transacts against it.
            Some(self.gen_create(0))
        } else {
            self.next_transact_against_pool(idx)
        }
    }

    fn next_wide_fanout(&self, idx: u64) -> Option<Op> {
        let max_creates = self.tuning.target_ledger_count;
        let create_every = self.tuning.wide_fanout_create_every.max(1);
        if idx.is_multiple_of(create_every) {
            // Claim a slot under the cap atomically: only bump the
            // counter (and dispatch) if the pre-increment value is
            // still below the target. A concurrent worker that races
            // in gets a stale `current` from the CAS failure and
            // retries with the fresh value.
            let mut current = self.creates_dispatched.load(Ordering::Relaxed);
            while current < max_creates {
                match self.creates_dispatched.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Some(self.gen_create(idx)),
                    Err(actual) => current = actual,
                }
            }
        }
        self.next_transact_against_pool(idx)
    }

    fn next_multitenant(&self, idx: u64) -> Option<Op> {
        let create_every = self.tuning.multitenant_create_every.max(1);
        if idx.is_multiple_of(create_every) {
            Some(self.gen_create(idx))
        } else {
            self.next_transact_against_pool(idx)
        }
    }

    fn next_transact_against_pool(&self, idx: u64) -> Option<Op> {
        let ledger = self.ledgers.pick(idx as usize)?;
        Some(self.gen_transact(idx, ledger))
    }

    fn next_query_against_pool(&self, idx: u64) -> Option<Op> {
        let ledger = self.ledgers.pick(idx as usize)?;
        Some(self.gen_query(ledger))
    }

    fn next_mixed_rw(&self, idx: u64) -> Option<Op> {
        let write_every = self.tuning.mixed_write_every.max(1);
        let ledger = self.ledgers.pick(idx as usize)?;
        if idx.is_multiple_of(write_every) {
            Some(self.gen_transact(idx, ledger))
        } else {
            Some(self.gen_query(ledger))
        }
    }

    fn gen_create(&self, idx: u64) -> Op {
        let name = self.ledger_name(idx);
        let body = json!({ "ledger": name });
        Op {
            kind: OpKind::CreateLedger,
            ledger: name,
            body,
            // The Committer trait has no `create_ledger` method, so
            // idempotency keys don't apply to this op even in
            // `unique` / `pooled` modes.
            idempotency_key: None,
        }
    }

    fn gen_transact(&self, idx: u64, ledger: String) -> Op {
        // Body seed varies by idempotency mode. Anonymous / unique
        // use `idx` directly so every request emits a distinct IRI
        // (no NamespaceConflict). Pooled derives from (ledger, slot)
        // so the same slot on the same ledger emits the same body
        // (required for dedup to trigger rather than `KeyCollision`),
        // and the same slot on a different ledger emits a different
        // body (so cross-ledger ops don't share cache entries).
        let body_seed = match self.tuning.idempotency_mode {
            IdempotencyMode::Anonymous | IdempotencyMode::Unique => idx,
            IdempotencyMode::Pooled => {
                let slot = idx % self.tuning.idempotency_pool_size.max(1);
                xxh64(ledger.as_bytes(), slot)
            }
        };
        let subject_id = format!("http://load.fluree/{}/s{}", self.run_id, body_seed);
        let body = json!({
            "@graph": [{
                "@id": subject_id,
                "http://load.fluree/idx": body_seed,
                "http://load.fluree/run": self.run_id,
            }]
        });
        let idempotency_key = self.idempotency_key_for(idx, &ledger);
        Op {
            kind: OpKind::Transact,
            ledger,
            body,
            idempotency_key,
        }
    }

    /// Bounded triple scan targeting the predicate the transact
    /// workload writes. Returns real bindings on ledgers that were
    /// populated by a prior transact-only / single-pound / wide-fanout
    /// / multitenant run; returns an empty result set on fresh
    /// ledgers (still 200 OK, still exercises the query path).
    ///
    /// Kept deliberately shape-fixed for now — a per-request cursor
    /// (varying IRI, varying LIMIT) can go in later once we know
    /// whether tail-latency measurement wants that. First cut: one
    /// stable query so cache warmth is honest across the run.
    fn gen_query(&self, ledger: String) -> Op {
        let body = json!({
            "select": ["?s"],
            "where": {
                "@id": "?s",
                "http://load.fluree/idx": "?idx"
            },
            "limit": 10
        });
        Op {
            kind: OpKind::Query,
            ledger,
            body,
            idempotency_key: None,
        }
    }

    /// Idempotency key for a write op at index `idx` against `ledger`,
    /// per the configured mode. Keys are prefixed with the run id so
    /// concurrent runs against the same cluster don't collide, and
    /// pooled keys are scoped by ledger so the same slot on different
    /// ledgers produces different keys (avoiding cross-ledger cache
    /// collisions regardless of how the server keys its cache). Keys
    /// stay well under the `MAX_IDEMPOTENCY_KEY_LEN` cap (128 bytes).
    fn idempotency_key_for(&self, idx: u64, ledger: &str) -> Option<String> {
        match self.tuning.idempotency_mode {
            IdempotencyMode::Anonymous => None,
            IdempotencyMode::Unique => Some(format!("load-{}-u{}", self.run_id, idx)),
            IdempotencyMode::Pooled => {
                let slot = idx % self.tuning.idempotency_pool_size.max(1);
                let ledger_digest = xxh64(ledger.as_bytes(), 0);
                Some(format!(
                    "load-{}-l{:x}-p{}",
                    self.run_id, ledger_digest, slot
                ))
            }
        }
    }

    fn ledger_name(&self, idx: u64) -> String {
        // Suffix with run-id + index so two concurrent CreateLedger ops
        // in the same run never collide, and runs against the same
        // cluster never collide either.
        format!("{}-{}-{}", self.tuning.ledger_prefix, self.run_id, idx)
    }
}
