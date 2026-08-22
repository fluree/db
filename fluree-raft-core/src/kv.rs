//! A replicated key/value fragment an application embeds in its own
//! state machine.
//!
//! Not a service and not its own Raft group: [`KvFragment`] is a piece
//! of application state, [`apply`] is a pure reduction over it, and the
//! application decides where it sits in `AppStateMachine::State` and
//! which of its commands route here.
//!
//! ## Why a fragment rather than a group
//!
//! A lease fences the work it guards only if both are ordered by the
//! *same* log. Put the lease in a second group and you are back to
//! reasoning about two independent clocks. Same-group also lets one
//! application command touch app state and KV state atomically.
//!
//! ## The fence
//!
//! **An entry's version is the Raft log index of the write that created
//! it** — not a per-key counter. Per-key counters reset on delete or
//! expiry, so a token can repeat, and a repeating fencing token is not
//! a fence. A log index is globally monotonic and comparable across
//! keys, which is what lets a holder embed it in external effects
//! (persisted run records, pre-commit re-checks).
//!
//! Every successful [`KvCommand::Put`] gets a new version, renewals
//! included. One key mutation per command, so `log_index` identifies
//! the write unambiguously — batching would force `(index, ordinal)`
//! versions and is deliberately not supported.
//!
//! ## Expiry is logical absence
//!
//! An entry is expired when `expires_at_ms <= now_ms`, and an expired
//! entry is absent to *every* observer: reads miss it, `expect: Absent`
//! succeeds against it, `expect: Version(v)` fails against it, and
//! [`KvCommand::TakeOnce`] does not take it. Whether the sweep has run
//! yet is therefore invisible, which is what keeps takeover from
//! depending on ticker timing.
//!
//! ## CAS failures return the current record
//!
//! Every conflict answers with what is there now
//! ([`KvResponse::Conflict`]). Two independent reasons: without it each
//! consumer bolts a read onto every failed propose, racing the state it
//! just observed; and it is the recovery path for a **lost response** —
//! a renewal can commit and lose its reply, after which retrying with
//! the old version conflicts, and a holder whose value carries a unique
//! holder id recognizes its own successful renewal in the record it gets
//! back.
//!
//! ## Clocks
//!
//! `now_ms` comes from the proposer and `apply` has nothing to check it
//! against, so a TTL maximum bounds the *duration* but not `now_ms +
//! ttl`. That is deliberate: **TTL is a liveness knob, not a safety
//! knob.** Once the fence rides in a consumer's external effects, a
//! skewed clock delays takeover by the skew; it never admits two fenced
//! writers. The proposer is a cluster node, which is the same trust
//! assumption a stamped `applied_at_millis` already makes.
//!
//! ## Determinism
//!
//! Everything here is ordered (`BTreeMap` / `BTreeSet`) and takes its
//! clock from the command, so every replica reducing the same log
//! reaches byte-identical state. Do not introduce a `HashMap`, a
//! `SystemTime` read, or a floating-point computation.
//!
//! ## Wire stability
//!
//! [`KvCommand`] rides in replicated log entries and [`KvFragment`]
//! rides in snapshots, so both are wire formats under postcard, which
//! is positional and non-self-describing:
//!
//! - Enum variants may be **appended** only. Reordering or removing
//!   them does not error — it silently decodes old data as the wrong
//!   variant. `wire_format_is_pinned` holds the discriminants.
//! - Struct and struct-variant fields may **not** be appended. Old
//!   snapshots decode short and fail with "Hit the end of buffer".
//!
//! ## Tenancy is the application's composition
//!
//! This module has no tenant concept. Its unit is one [`KvFragment`],
//! and [`apply`] never sees anything else. An application that wants
//! several isolated namespaces keys a map by its own **append-only**
//! enum:
//!
//! ```ignore
//! #[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
//! enum Tenant { Config, OAuth, Webhook, Lease }   // APPEND ONLY
//!
//! struct AppState { kv: BTreeMap<Tenant, KvFragment>, /* ... */ }
//! enum AppCommand { Kv(Tenant, KvCommand), /* ... */ }
//! ```
//!
//! Not separate struct fields (`kv_config`, `kv_oauth`): appending one
//! breaks decode of every deployed snapshot, so each new tenant becomes
//! a format migration. An enum key costs one variant and one
//! policy-registry entry, and old snapshots simply lack the new key.
//!
//! The sharp edge is the same one as above, and it is worth restating:
//! **reordering or removing a `Tenant` variant does not error** — it
//! silently reassigns one tenant's data to another. Pin the
//! discriminants with a golden-bytes test, the way
//! `wire_format_is_pinned` pins this module's.
//!
//! The per-tenant [`KvPolicy`] registry lives in code, matched
//! exhaustively on the enum, because a policy that differs between
//! nodes diverges the cluster. Per-tenant policy also means a churny
//! tenant cannot starve another's sweep.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

/// Hard ceiling on records examined by one [`KvCommand::Evict`],
/// regardless of policy. A sweep runs inside `apply`, which blocks the
/// state machine, so this is a bound on how long one entry can stall
/// every other apply behind it.
pub const HARD_MAX_EVICT_LIMIT: u32 = 1024;

/// Default sweep batch — well under [`HARD_MAX_EVICT_LIMIT`] so a
/// backlog drains over several applies rather than in one long one.
pub const DEFAULT_EVICT_LIMIT: u32 = 256;

/// Policy default for key size. Keys are held in memory, in the expiry
/// index, and in every snapshot.
pub const DEFAULT_MAX_KEY_BYTES: usize = 1024;

/// Policy default for value size.
pub const DEFAULT_MAX_VALUE_BYTES: usize = 1024 * 1024;

// ---------------------------------------------------------------------
// Commands and responses
// ---------------------------------------------------------------------

/// The precondition a mutation is conditional on.
///
/// Evaluated against *logical* state: an expired record is `Absent`.
///
/// Wire format — append variants only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expect {
    /// No precondition.
    Any,
    /// Succeeds only when the key is logically absent.
    Absent,
    /// Succeeds only when a live record carries exactly this version.
    Version(u64),
}

/// One key mutation, or one bounded sweep.
///
/// Wire format — append variants only, and never append a field to an
/// existing variant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    /// Write `value` at `key`, subject to `expect`.
    ///
    /// `ttl_ms` is `None` for an entry with no expiry, which
    /// [`KvPolicy::allow_immortal`] must permit. `now_ms` is the
    /// proposer's clock; `expires_at_ms` is derived from it at apply
    /// time so every replica computes the same instant.
    Put {
        key: String,
        value: Vec<u8>,
        expect: Expect,
        ttl_ms: Option<u64>,
        now_ms: u64,
    },
    /// Remove `key`, subject to `expect`. Creates no new version.
    Delete {
        key: String,
        expect: Expect,
        now_ms: u64,
    },
    /// Remove `key` and return what was there, atomically. Exactly one
    /// proposer sees `Some`; every other sees `None`.
    TakeOnce { key: String, now_ms: u64 },
    /// Reclaim records expired at or before `cutoff_ms`, examining at
    /// most `limit` of them.
    Evict { cutoff_ms: u64, limit: u32 },
}

/// A record as an observer sees it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvRecord {
    pub value: Vec<u8>,
    /// The Raft log index of the write that created this record.
    pub version: u64,
    pub expires_at_ms: Option<u64>,
}

/// A record borrowed from the fragment, so reads do not clone values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvRecordRef<'a> {
    pub value: &'a [u8],
    pub version: u64,
    pub expires_at_ms: Option<u64>,
}

impl KvRecordRef<'_> {
    pub fn to_record(self) -> KvRecord {
        KvRecord {
            value: self.value.to_vec(),
            version: self.version,
            expires_at_ms: self.expires_at_ms,
        }
    }
}

/// Why a command was refused before it could touch state.
///
/// A rejection is deterministic — every replica reaches the same one —
/// so it is a normal response, not a storage error.
///
/// Wire format — append variants only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum KvRejection {
    #[error("key is {bytes} bytes, over the {max}-byte maximum")]
    KeyTooLarge { bytes: u64, max: u64 },
    #[error("value is {bytes} bytes, over the {max}-byte maximum")]
    ValueTooLarge { bytes: u64, max: u64 },
    #[error("a zero ttl is not a write; use Delete")]
    TtlZero,
    #[error("ttl {ttl_ms}ms is over the {max_ttl_ms}ms maximum")]
    TtlAboveMax { ttl_ms: u64, max_ttl_ms: u64 },
    #[error("now_ms {now_ms} + ttl {ttl_ms} overflows")]
    TtlOverflow { now_ms: u64, ttl_ms: u64 },
    #[error("this fragment does not allow entries without a ttl")]
    ImmortalNotAllowed,
    #[error("fragment holds {entries} entries, at the {max} maximum")]
    EntryQuotaExceeded { entries: u64, max: u64 },
    #[error("fragment holds {bytes} bytes; {max} is the maximum")]
    ByteQuotaExceeded { bytes: u64, max: u64 },
}

/// The outcome of one [`apply`].
///
/// Wire format — append variants only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvResponse {
    /// The write landed. `version` is the fence.
    Written {
        version: u64,
        expires_at_ms: Option<u64>,
    },
    /// The delete was permitted. `removed` is whether a *live* record
    /// was actually taken away — reclaiming an already-expired record
    /// reports `false`, because it was already logically absent.
    Deleted { removed: bool },
    /// `None` means the key was logically absent; someone else took it,
    /// or it expired.
    Taken { record: Option<KvRecord> },
    /// `more_expired` means the sweep hit its limit with work left.
    /// Propose again immediately with the *same* cutoff rather than
    /// waiting out the interval.
    Evicted { removed: u32, more_expired: bool },
    /// The precondition did not hold. `current` is what is there now,
    /// so the caller does not have to race a follow-up read.
    Conflict { current: Option<KvRecord> },
    /// The command never reached the state.
    Rejected(KvRejection),
}

// ---------------------------------------------------------------------
// Policy
// ---------------------------------------------------------------------

/// Limits one fragment is reduced under.
///
/// Every replica must hold the same policy for the same fragment, so it
/// belongs in code — a registry matched exhaustively on the
/// application's fragment key — not in per-node configuration. A policy
/// that differs between nodes diverges the cluster.
///
/// Apply-time checks are the last line, not the only one: by the time
/// `apply` runs, the command has already been allocated, serialized,
/// replicated, and buffered. Enforce at the transport body caps, again
/// client-side via [`KvPolicy::check_put`] before proposing, and here.
/// The apply-time check is what makes a proposer that skipped the
/// client-side one unable to diverge the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvPolicy {
    pub max_key_bytes: u64,
    pub max_value_bytes: u64,
    /// Largest `ttl_ms` a [`KvCommand::Put`] may ask for.
    pub max_ttl_ms: u64,
    /// Whether `ttl_ms: None` is allowed. Configuration fragments
    /// generally yes; lease fragments never — an immortal lease is a
    /// permanently stuck one.
    pub allow_immortal: bool,
    /// Cap on records physically held, expired-but-unswept included.
    pub max_entries: u64,
    /// Cap on `key.len() + value.len()` summed over records physically
    /// held.
    pub max_bytes: u64,
    /// Per-apply sweep cap, itself capped by [`HARD_MAX_EVICT_LIMIT`].
    pub max_evict_per_apply: u32,
}

impl Default for KvPolicy {
    /// Hand-written: a derived `Default` would zero every limit, which
    /// rejects all writes and reads as a mysterious outage rather than
    /// a compile error.
    fn default() -> Self {
        Self {
            max_key_bytes: DEFAULT_MAX_KEY_BYTES as u64,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES as u64,
            max_ttl_ms: 24 * 60 * 60 * 1000,
            allow_immortal: false,
            max_entries: 100_000,
            max_bytes: 64 * 1024 * 1024,
            max_evict_per_apply: DEFAULT_EVICT_LIMIT,
        }
    }
}

impl KvPolicy {
    /// Validate a put's shape and resolve its expiry instant.
    ///
    /// Call this before proposing. [`apply`] calls the same function,
    /// so a client-side pre-check and the replicated decision cannot
    /// disagree.
    ///
    /// TTLs are **rejected, never clamped**: silently shortening one
    /// turns "why did my entry vanish" into an incident, and a clamp on
    /// `ttl_ms` would not bound `now_ms + ttl` anyway.
    pub fn check_put(
        &self,
        key: &str,
        value: &[u8],
        ttl_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<Option<u64>, KvRejection> {
        let key_bytes = key.len() as u64;
        if key_bytes > self.max_key_bytes {
            return Err(KvRejection::KeyTooLarge {
                bytes: key_bytes,
                max: self.max_key_bytes,
            });
        }
        let value_bytes = value.len() as u64;
        if value_bytes > self.max_value_bytes {
            return Err(KvRejection::ValueTooLarge {
                bytes: value_bytes,
                max: self.max_value_bytes,
            });
        }
        match ttl_ms {
            None if !self.allow_immortal => Err(KvRejection::ImmortalNotAllowed),
            None => Ok(None),
            Some(0) => Err(KvRejection::TtlZero),
            Some(ttl) if ttl > self.max_ttl_ms => Err(KvRejection::TtlAboveMax {
                ttl_ms: ttl,
                max_ttl_ms: self.max_ttl_ms,
            }),
            Some(ttl) => now_ms
                .checked_add(ttl)
                .map(Some)
                .ok_or(KvRejection::TtlOverflow {
                    now_ms,
                    ttl_ms: ttl,
                }),
        }
    }

    /// The sweep batch actually used for a requested `limit`.
    ///
    /// Clamped rather than rejected, unlike a TTL: the batch size has
    /// no observable semantics — only throughput — while rejecting an
    /// over-large one would stop eviction entirely. The lower clamp
    /// matters too: a caller following the `more_expired` retry
    /// protocol with a zero limit would spin forever.
    pub fn evict_batch(&self, limit: u32) -> u32 {
        limit.clamp(1, self.max_evict_per_apply.clamp(1, HARD_MAX_EVICT_LIMIT))
    }
}

// ---------------------------------------------------------------------
// State
// ---------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    value: Vec<u8>,
    version: u64,
    expires_at_ms: Option<u64>,
}

impl Record {
    fn expired_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms.is_some_and(|at| at <= now_ms)
    }

    fn footprint(&self, key: &str) -> u64 {
        key.len() as u64 + self.value.len() as u64
    }

    fn as_ref(&self) -> KvRecordRef<'_> {
        KvRecordRef {
            value: &self.value,
            version: self.version,
            expires_at_ms: self.expires_at_ms,
        }
    }
}

/// The replicated state. Embed one per fragment in the application's
/// state machine.
///
/// `expiring` and `bytes` are denormalizations of `entries` — a
/// secondary index so a sweep is a range query rather than O(n) in the
/// map, and a running byte total so a quota check is O(1). Both are
/// maintained by [`apply`] and nothing else may write them;
/// [`KvFragment::check_invariants`] recomputes both and is what a test
/// should assert after a mutation sequence.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvFragment {
    entries: BTreeMap<String, Record>,
    /// `(expires_at_ms, key, version)` for every record that has an
    /// expiry. Immortal records are absent.
    expiring: BTreeSet<(u64, String, u64)>,
    bytes: u64,
}

/// A denormalization that drifted from `entries`. Never expected;
/// surfaced by [`KvFragment::check_invariants`] in tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvariantViolation {
    ByteTotal {
        stored: u64,
        actual: u64,
    },
    ExpiryIndex {
        stored: Vec<(u64, String, u64)>,
        actual: Vec<(u64, String, u64)>,
    },
}

impl KvFragment {
    pub fn new() -> Self {
        Self::default()
    }

    /// The live record at `key`, or `None` if absent or expired.
    ///
    /// Observers must be able to read the current fence without
    /// proposing, which is what this is for.
    pub fn get_at(&self, key: &str, now_ms: u64) -> Option<KvRecordRef<'_>> {
        self.entries
            .get(key)
            .filter(|record| !record.expired_at(now_ms))
            .map(Record::as_ref)
    }

    /// Every live record, in key order.
    pub fn iter_at(&self, now_ms: u64) -> impl Iterator<Item = (&str, KvRecordRef<'_>)> {
        self.entries
            .iter()
            .filter(move |(_, record)| !record.expired_at(now_ms))
            .map(|(key, record)| (key.as_str(), record.as_ref()))
    }

    /// Records physically held, expired-but-unswept included. This is
    /// what the entry quota counts, because it is what actually
    /// occupies memory and snapshot bytes.
    pub fn physical_len(&self) -> usize {
        self.entries.len()
    }

    /// Bytes physically held, on the same basis as
    /// [`Self::physical_len`].
    pub fn physical_bytes(&self) -> u64 {
        self.bytes
    }

    /// Whether any record has expired at or before `cutoff_ms`. Lets a
    /// ticker skip proposing a sweep that would do nothing.
    pub fn has_expired_at(&self, cutoff_ms: u64) -> bool {
        self.expired_range(cutoff_ms).next().is_some()
    }

    /// Recompute both denormalizations and compare. Test aid — an
    /// application's own tests can assert this after a mutation
    /// sequence to catch an index leak that would otherwise only
    /// surface as a sweep removing a live record.
    pub fn check_invariants(&self) -> Result<(), InvariantViolation> {
        let actual_bytes: u64 = self
            .entries
            .iter()
            .map(|(key, record)| record.footprint(key))
            .sum();
        if actual_bytes != self.bytes {
            return Err(InvariantViolation::ByteTotal {
                stored: self.bytes,
                actual: actual_bytes,
            });
        }
        let actual: BTreeSet<(u64, String, u64)> = self
            .entries
            .iter()
            .filter_map(|(key, record)| {
                record
                    .expires_at_ms
                    .map(|at| (at, key.clone(), record.version))
            })
            .collect();
        if actual != self.expiring {
            return Err(InvariantViolation::ExpiryIndex {
                stored: self.expiring.iter().cloned().collect(),
                actual: actual.into_iter().collect(),
            });
        }
        Ok(())
    }

    fn expired_range(&self, cutoff_ms: u64) -> impl Iterator<Item = &(u64, String, u64)> {
        // Everything with `expires_at_ms <= cutoff_ms`. Expressed as an
        // exclusive bound at the next millisecond because the tuple's
        // trailing fields have no representable maximum.
        let end = match cutoff_ms.checked_add(1) {
            Some(next) => Bound::Excluded((next, String::new(), 0)),
            None => Bound::Unbounded,
        };
        self.expiring.range((Bound::Unbounded, end))
    }

    /// Drop a record and every trace of it. Returns it.
    fn take(&mut self, key: &str) -> Option<Record> {
        let record = self.entries.remove(key)?;
        self.bytes -= record.footprint(key);
        if let Some(at) = record.expires_at_ms {
            self.expiring.remove(&(at, key.to_string(), record.version));
        }
        Some(record)
    }

    fn insert(&mut self, key: &str, record: Record) {
        self.bytes += record.footprint(key);
        if let Some(at) = record.expires_at_ms {
            self.expiring.insert((at, key.to_string(), record.version));
        }
        self.entries.insert(key.to_string(), record);
    }

    /// Physically reclaim `key` if it is present but expired.
    ///
    /// Every mutation path calls this first, so a key a caller touches
    /// never lingers waiting for the sweep. It cannot change any
    /// answer — an expired record is already logically absent — so it
    /// keeps sweep timing invisible rather than making it observable.
    fn reclaim_if_expired(&mut self, key: &str, now_ms: u64) {
        if self
            .entries
            .get(key)
            .is_some_and(|record| record.expired_at(now_ms))
        {
            self.take(key);
        }
    }

    fn live(&self, key: &str, now_ms: u64) -> Option<&Record> {
        self.entries
            .get(key)
            .filter(|record| !record.expired_at(now_ms))
    }

    fn conflict(&self, key: &str, now_ms: u64) -> KvResponse {
        KvResponse::Conflict {
            current: self.live(key, now_ms).map(|r| r.as_ref().to_record()),
        }
    }
}

// ---------------------------------------------------------------------
// Apply
// ---------------------------------------------------------------------

/// Reduce one command into the fragment.
///
/// Pure and deterministic: the only clock is the one carried by the
/// command, and `log_index` — the index of the entry that carried it —
/// becomes the version of whatever it writes.
///
/// Takes the command by reference to match `AppStateMachine::apply`,
/// which costs one clone of the key and value on a successful put.
pub fn apply(
    fragment: &mut KvFragment,
    command: &KvCommand,
    policy: &KvPolicy,
    log_index: u64,
) -> KvResponse {
    match command {
        KvCommand::Put {
            key,
            value,
            expect,
            ttl_ms,
            now_ms,
        } => put(
            fragment,
            PutArgs {
                key,
                value,
                expect,
                ttl_ms: *ttl_ms,
                now_ms: *now_ms,
            },
            policy,
            log_index,
        ),
        KvCommand::Delete {
            key,
            expect,
            now_ms,
        } => delete(fragment, key, expect, *now_ms),
        KvCommand::TakeOnce { key, now_ms } => take_once(fragment, key, *now_ms),
        KvCommand::Evict { cutoff_ms, limit } => {
            evict(fragment, *cutoff_ms, policy.evict_batch(*limit))
        }
    }
}

/// [`KvCommand::Put`]'s fields, borrowed. A struct rather than eight
/// positional arguments.
struct PutArgs<'a> {
    key: &'a str,
    value: &'a [u8],
    expect: &'a Expect,
    ttl_ms: Option<u64>,
    now_ms: u64,
}

fn put(
    fragment: &mut KvFragment,
    args: PutArgs<'_>,
    policy: &KvPolicy,
    log_index: u64,
) -> KvResponse {
    let PutArgs {
        key,
        value,
        expect,
        ttl_ms,
        now_ms,
    } = args;
    let expires_at_ms = match policy.check_put(key, value, ttl_ms, now_ms) {
        Ok(at) => at,
        Err(rejection) => return KvResponse::Rejected(rejection),
    };

    let satisfied = match expect {
        Expect::Any => true,
        Expect::Absent => fragment.live(key, now_ms).is_none(),
        Expect::Version(want) => fragment
            .live(key, now_ms)
            .is_some_and(|r| r.version == *want),
    };
    if !satisfied {
        return fragment.conflict(key, now_ms);
    }

    // Reclaim before the quota check so replacing an expired record is
    // never rejected for space the record was already not entitled to.
    fragment.reclaim_if_expired(key, now_ms);

    let displaced = fragment
        .entries
        .get(key)
        .map(|record| record.footprint(key))
        .unwrap_or(0);
    let record = Record {
        value: value.to_vec(),
        version: log_index,
        expires_at_ms,
    };
    let after_bytes = fragment.bytes - displaced + record.footprint(key);
    if after_bytes > policy.max_bytes {
        return KvResponse::Rejected(KvRejection::ByteQuotaExceeded {
            bytes: after_bytes,
            max: policy.max_bytes,
        });
    }
    let after_entries =
        fragment.entries.len() as u64 + u64::from(!fragment.entries.contains_key(key));
    if after_entries > policy.max_entries {
        return KvResponse::Rejected(KvRejection::EntryQuotaExceeded {
            entries: after_entries,
            max: policy.max_entries,
        });
    }

    fragment.take(key);
    fragment.insert(key, record);
    KvResponse::Written {
        version: log_index,
        expires_at_ms,
    }
}

fn delete(fragment: &mut KvFragment, key: &str, expect: &Expect, now_ms: u64) -> KvResponse {
    let live_version = fragment.live(key, now_ms).map(|record| record.version);
    let satisfied = match expect {
        Expect::Any => true,
        Expect::Absent => live_version.is_none(),
        Expect::Version(want) => live_version == Some(*want),
    };
    if !satisfied {
        return fragment.conflict(key, now_ms);
    }
    // Reclaim regardless: an expired record the caller just asked about
    // is free to drop, and dropping it here keeps a delete-heavy
    // workload from depending on the sweep.
    fragment.reclaim_if_expired(key, now_ms);
    let removed = live_version.is_some() && fragment.take(key).is_some();
    KvResponse::Deleted { removed }
}

fn take_once(fragment: &mut KvFragment, key: &str, now_ms: u64) -> KvResponse {
    fragment.reclaim_if_expired(key, now_ms);
    let record = fragment
        .live(key, now_ms)
        .is_some()
        .then(|| fragment.take(key))
        .flatten()
        .map(|record| KvRecord {
            value: record.value,
            version: record.version,
            expires_at_ms: record.expires_at_ms,
        });
    KvResponse::Taken { record }
}

fn evict(fragment: &mut KvFragment, cutoff_ms: u64, limit: u32) -> KvResponse {
    // Collect first: the range borrows `expiring`, which the removals
    // mutate. `limit` bounds the collection, so this is bounded work.
    let doomed: Vec<String> = fragment
        .expired_range(cutoff_ms)
        .take(limit as usize)
        .map(|(_, key, _)| key.clone())
        .collect();
    let mut removed = 0;
    for key in doomed {
        if fragment.take(&key).is_some() {
            removed += 1;
        }
    }
    KvResponse::Evicted {
        removed,
        more_expired: fragment.has_expired_at(cutoff_ms),
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 60 * 60 * 1000;

    fn policy() -> KvPolicy {
        KvPolicy::default()
    }

    /// Every mutation goes through here, so a path that leaks an expiry
    /// index entry or miscounts bytes fails at the point it happens
    /// rather than as a mystery later.
    fn step(f: &mut KvFragment, cmd: KvCommand, p: &KvPolicy, index: u64) -> KvResponse {
        let response = apply(f, &cmd, p, index);
        f.check_invariants()
            .unwrap_or_else(|v| panic!("{cmd:?} at index {index} broke an invariant: {v:?}"));
        response
    }

    fn put(key: &str, value: &[u8], expect: Expect, ttl_ms: Option<u64>, now_ms: u64) -> KvCommand {
        KvCommand::Put {
            key: key.into(),
            value: value.to_vec(),
            expect,
            ttl_ms,
            now_ms,
        }
    }

    fn acquire(f: &mut KvFragment, key: &str, holder: &[u8], now_ms: u64, index: u64) -> u64 {
        match step(
            f,
            put(key, holder, Expect::Absent, Some(HOUR), now_ms),
            &policy(),
            index,
        ) {
            KvResponse::Written { version, .. } => version,
            other => panic!("acquire failed: {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // The fence
    // -----------------------------------------------------------------

    /// A per-key counter would reset here and hand out a token that had
    /// already been used, which is what makes log index the version.
    #[test]
    fn version_is_the_log_index_and_never_repeats_across_a_delete() {
        let mut f = KvFragment::new();
        let p = policy();
        let first = acquire(&mut f, "lease", b"a", 0, 7);
        assert_eq!(first, 7);

        step(
            &mut f,
            KvCommand::Delete {
                key: "lease".into(),
                expect: Expect::Any,
                now_ms: 0,
            },
            &p,
            8,
        );
        let second = acquire(&mut f, "lease", b"b", 0, 9);
        assert_eq!(second, 9, "a re-acquire must not reuse a retired version");
        assert!(second > first);
    }

    #[test]
    fn every_renewal_gets_a_new_version() {
        let mut f = KvFragment::new();
        let p = policy();
        let v1 = acquire(&mut f, "lease", b"holder-1", 0, 10);
        let KvResponse::Written { version: v2, .. } = step(
            &mut f,
            put("lease", b"holder-1", Expect::Version(v1), Some(HOUR), 1_000),
            &p,
            11,
        ) else {
            panic!("renewal must be written");
        };
        assert_eq!(v2, 11);
        assert_eq!(f.get_at("lease", 1_000).unwrap().version, 11);
    }

    // -----------------------------------------------------------------
    // Expiry is logical absence — the two named regressions
    // -----------------------------------------------------------------

    /// The lapse path. A holder whose lease expired must re-acquire
    /// with `Absent` and race fairly, never renew — otherwise a stalled
    /// holder reclaims a lease a rival already believes is theirs.
    #[test]
    fn expired_lease_cannot_renew_with_old_version() {
        let mut f = KvFragment::new();
        let p = policy();
        let held = acquire(&mut f, "lease", b"holder-1", 0, 5);

        let response = step(
            &mut f,
            put(
                "lease",
                b"holder-1",
                Expect::Version(held),
                Some(HOUR),
                HOUR,
            ),
            &p,
            6,
        );
        assert_eq!(
            response,
            KvResponse::Conflict { current: None },
            "an expired lease must not be renewable with the old version",
        );

        // And a rival can take it, which is the point of the lapse.
        let rival = acquire(&mut f, "lease", b"holder-2", HOUR, 7);
        assert_eq!(rival, 7);
        assert_eq!(f.get_at("lease", HOUR).unwrap().value, b"holder-2");
    }

    /// If CAS against expired-but-present ever diverged from CAS
    /// against evicted, sweep timing would become observable and
    /// takeover would depend on when the ticker last ran.
    #[test]
    fn eviction_is_semantically_invisible() {
        let probes: Vec<KvCommand> = vec![
            put("k", b"new", Expect::Absent, Some(HOUR), HOUR),
            put("k", b"new", Expect::Version(5), Some(HOUR), HOUR),
            put("k", b"new", Expect::Any, Some(HOUR), HOUR),
            KvCommand::Delete {
                key: "k".into(),
                expect: Expect::Any,
                now_ms: HOUR,
            },
            KvCommand::Delete {
                key: "k".into(),
                expect: Expect::Absent,
                now_ms: HOUR,
            },
            KvCommand::Delete {
                key: "k".into(),
                expect: Expect::Version(5),
                now_ms: HOUR,
            },
            KvCommand::TakeOnce {
                key: "k".into(),
                now_ms: HOUR,
            },
        ];

        for probe in probes {
            let p = policy();

            // (a) expired, still physically present.
            let mut unswept = KvFragment::new();
            assert_eq!(acquire(&mut unswept, "k", b"old", 0, 5), 5);
            assert_eq!(unswept.physical_len(), 1);

            // (b) expired and already swept away.
            let mut swept = unswept.clone();
            step(
                &mut swept,
                KvCommand::Evict {
                    cutoff_ms: HOUR,
                    limit: 16,
                },
                &p,
                6,
            );
            assert_eq!(swept.physical_len(), 0, "the sweep must have reclaimed it");

            let unswept_response = step(&mut unswept, probe.clone(), &p, 9);
            let swept_response = step(&mut swept, probe.clone(), &p, 9);
            assert_eq!(
                unswept_response, swept_response,
                "{probe:?} must not be able to tell whether the sweep has run",
            );
            assert_eq!(
                unswept.get_at("k", HOUR).map(KvRecordRef::to_record),
                swept.get_at("k", HOUR).map(KvRecordRef::to_record),
                "{probe:?} must leave the same live state either way",
            );
        }
    }

    #[test]
    fn expired_is_absent_to_reads_and_take_once() {
        let mut f = KvFragment::new();
        let p = policy();
        acquire(&mut f, "k", b"v", 0, 5);

        assert!(f.get_at("k", HOUR - 1).is_some());
        assert!(f.get_at("k", HOUR).is_none(), "expiry is inclusive");
        assert_eq!(f.iter_at(HOUR).count(), 0);
        assert_eq!(
            step(
                &mut f,
                KvCommand::TakeOnce {
                    key: "k".into(),
                    now_ms: HOUR
                },
                &p,
                6
            ),
            KvResponse::Taken { record: None },
        );
    }

    #[test]
    fn take_once_hands_the_value_to_exactly_one_caller() {
        let mut f = KvFragment::new();
        let p = policy();
        acquire(&mut f, "claim", b"payload", 0, 4);

        let first = step(
            &mut f,
            KvCommand::TakeOnce {
                key: "claim".into(),
                now_ms: 0,
            },
            &p,
            5,
        );
        let KvResponse::Taken { record: Some(rec) } = first else {
            panic!("the first take must get the record, got {first:?}");
        };
        assert_eq!(rec.value, b"payload");
        assert_eq!(rec.version, 4);

        assert_eq!(
            step(
                &mut f,
                KvCommand::TakeOnce {
                    key: "claim".into(),
                    now_ms: 0
                },
                &p,
                6
            ),
            KvResponse::Taken { record: None },
        );
    }

    // -----------------------------------------------------------------
    // Conflicts carry the current record
    // -----------------------------------------------------------------

    /// The lost-response recovery path: a renewal commits, its reply is
    /// lost, and the retry conflicts. The holder has to be able to
    /// recognize its own successful write in what comes back.
    #[test]
    fn a_conflict_returns_enough_to_recognize_your_own_lost_write() {
        let mut f = KvFragment::new();
        let p = policy();
        let v1 = acquire(&mut f, "lease", b"holder-1", 0, 20);
        // The renewal that commits but whose response never arrives.
        step(
            &mut f,
            put("lease", b"holder-1", Expect::Version(v1), Some(HOUR), 100),
            &p,
            21,
        );

        // The retry, still carrying the stale version.
        let retry = step(
            &mut f,
            put("lease", b"holder-1", Expect::Version(v1), Some(HOUR), 200),
            &p,
            22,
        );
        let KvResponse::Conflict {
            current: Some(current),
        } = retry
        else {
            panic!("expected a conflict carrying the current record, got {retry:?}");
        };
        assert_eq!(
            current.value, b"holder-1",
            "the holder must be able to see the lease is still its own",
        );
        assert_eq!(current.version, 21, "and pick up the fence it lost");
    }

    #[test]
    fn absent_conflicts_report_absence_rather_than_a_stale_record() {
        let mut f = KvFragment::new();
        let p = policy();
        assert_eq!(
            step(
                &mut f,
                put("k", b"v", Expect::Version(3), Some(HOUR), 0),
                &p,
                9
            ),
            KvResponse::Conflict { current: None },
        );
    }

    // -----------------------------------------------------------------
    // Delete algebra
    // -----------------------------------------------------------------

    #[test]
    fn delete_algebra() {
        let p = policy();

        // Any — idempotent, reports whether a live entry went away.
        let mut f = KvFragment::new();
        acquire(&mut f, "k", b"v", 0, 5);
        let any = KvCommand::Delete {
            key: "k".into(),
            expect: Expect::Any,
            now_ms: 0,
        };
        assert_eq!(
            step(&mut f, any.clone(), &p, 6),
            KvResponse::Deleted { removed: true }
        );
        assert_eq!(
            step(&mut f, any, &p, 7),
            KvResponse::Deleted { removed: false }
        );

        // Absent — succeeds only when logically absent.
        let mut f = KvFragment::new();
        acquire(&mut f, "k", b"v", 0, 5);
        let absent = KvCommand::Delete {
            key: "k".into(),
            expect: Expect::Absent,
            now_ms: 0,
        };
        assert!(matches!(
            step(&mut f, absent.clone(), &p, 6),
            KvResponse::Conflict { current: Some(_) }
        ));
        assert!(
            f.get_at("k", 0).is_some(),
            "a refused delete removes nothing"
        );

        // Version — removes only a matching live version.
        let mut f = KvFragment::new();
        let v = acquire(&mut f, "k", b"v", 0, 5);
        assert!(matches!(
            step(
                &mut f,
                KvCommand::Delete {
                    key: "k".into(),
                    expect: Expect::Version(v + 1),
                    now_ms: 0
                },
                &p,
                6
            ),
            KvResponse::Conflict { current: Some(_) }
        ));
        assert_eq!(
            step(
                &mut f,
                KvCommand::Delete {
                    key: "k".into(),
                    expect: Expect::Version(v),
                    now_ms: 0
                },
                &p,
                7
            ),
            KvResponse::Deleted { removed: true }
        );
    }

    #[test]
    fn delete_creates_no_version() {
        let mut f = KvFragment::new();
        let p = policy();
        acquire(&mut f, "k", b"v", 0, 5);
        step(
            &mut f,
            KvCommand::Delete {
                key: "k".into(),
                expect: Expect::Any,
                now_ms: 0,
            },
            &p,
            6,
        );
        // Index 6 was consumed by a delete; the next write takes 7, and
        // nothing anywhere claims version 6.
        let KvResponse::Written { version, .. } = step(
            &mut f,
            put("k", b"v2", Expect::Absent, Some(HOUR), 0),
            &p,
            7,
        ) else {
            panic!("write must land");
        };
        assert_eq!(version, 7);
    }

    // -----------------------------------------------------------------
    // TTL: reject, do not clamp
    // -----------------------------------------------------------------

    #[test]
    fn ttl_is_rejected_never_clamped() {
        let mut f = KvFragment::new();
        let p = KvPolicy {
            max_ttl_ms: 1_000,
            ..policy()
        };

        assert_eq!(
            step(&mut f, put("k", b"v", Expect::Any, Some(0), 0), &p, 1),
            KvResponse::Rejected(KvRejection::TtlZero),
        );
        assert_eq!(
            step(&mut f, put("k", b"v", Expect::Any, Some(1_001), 0), &p, 2),
            KvResponse::Rejected(KvRejection::TtlAboveMax {
                ttl_ms: 1_001,
                max_ttl_ms: 1_000,
            }),
        );
        assert_eq!(
            step(&mut f, put("k", b"v", Expect::Any, None, 0), &p, 3),
            KvResponse::Rejected(KvRejection::ImmortalNotAllowed),
        );
        assert!(
            f.get_at("k", 0).is_none(),
            "a rejected put must not have written anything",
        );

        // The overflow guard is separate from the maximum: a policy
        // permissive enough to allow the ttl still must not wrap.
        let wide = KvPolicy {
            max_ttl_ms: u64::MAX,
            ..policy()
        };
        assert_eq!(
            step(
                &mut f,
                put("k", b"v", Expect::Any, Some(u64::MAX), 10),
                &wide,
                4
            ),
            KvResponse::Rejected(KvRejection::TtlOverflow {
                now_ms: 10,
                ttl_ms: u64::MAX,
            }),
        );
    }

    #[test]
    fn immortal_entries_are_allowed_where_policy_says_so_and_never_expire() {
        let mut f = KvFragment::new();
        let p = KvPolicy {
            allow_immortal: true,
            ..policy()
        };
        let KvResponse::Written {
            expires_at_ms: None,
            ..
        } = step(&mut f, put("cfg", b"v", Expect::Any, None, 0), &p, 1)
        else {
            panic!("an immortal put must report no expiry");
        };
        assert!(f.get_at("cfg", u64::MAX).is_some());
        assert_eq!(
            step(
                &mut f,
                KvCommand::Evict {
                    cutoff_ms: u64::MAX,
                    limit: 64
                },
                &p,
                2
            ),
            KvResponse::Evicted {
                removed: 0,
                more_expired: false,
            },
            "an immortal entry is not in the expiry index and cannot be swept",
        );
    }

    /// The client-side pre-check and the replicated decision are the
    /// same function, so they cannot drift apart and let a proposer
    /// that skipped one diverge the cluster.
    #[test]
    fn check_put_agrees_with_apply() {
        let p = KvPolicy {
            max_ttl_ms: 1_000,
            max_key_bytes: 4,
            max_value_bytes: 4,
            ..policy()
        };
        let cases: Vec<(&str, &[u8], Option<u64>)> = vec![
            ("ok", b"v", Some(500)),
            ("toolong", b"v", Some(500)),
            ("ok", b"toolong", Some(500)),
            ("ok", b"v", Some(0)),
            ("ok", b"v", Some(5_000)),
            ("ok", b"v", None),
        ];
        for (key, value, ttl) in cases {
            let mut f = KvFragment::new();
            let applied = step(&mut f, put(key, value, Expect::Any, ttl, 0), &p, 1);
            match (p.check_put(key, value, ttl, 0), applied) {
                (Ok(_), KvResponse::Written { .. }) => {}
                (Err(pre), KvResponse::Rejected(post)) => assert_eq!(
                    pre, post,
                    "check_put and apply must reject {key:?}/{ttl:?} identically",
                ),
                (pre, post) => panic!("check_put said {pre:?} but apply said {post:?}"),
            }
        }
    }

    // -----------------------------------------------------------------
    // Bounded eviction
    // -----------------------------------------------------------------

    #[test]
    fn eviction_is_bounded_and_reports_remaining_work() {
        let mut f = KvFragment::new();
        let p = policy();
        for i in 0..10u64 {
            step(
                &mut f,
                put(&format!("k{i:02}"), b"v", Expect::Absent, Some(HOUR), 0),
                &p,
                100 + i,
            );
        }

        let first = step(
            &mut f,
            KvCommand::Evict {
                cutoff_ms: HOUR,
                limit: 4,
            },
            &p,
            200,
        );
        assert_eq!(
            first,
            KvResponse::Evicted {
                removed: 4,
                more_expired: true,
            },
        );
        assert_eq!(f.physical_len(), 6);

        // Same cutoff, immediately — the retry protocol.
        step(
            &mut f,
            KvCommand::Evict {
                cutoff_ms: HOUR,
                limit: 4,
            },
            &p,
            201,
        );
        assert_eq!(
            step(
                &mut f,
                KvCommand::Evict {
                    cutoff_ms: HOUR,
                    limit: 4
                },
                &p,
                202
            ),
            KvResponse::Evicted {
                removed: 2,
                more_expired: false,
            },
        );
        assert_eq!(f.physical_bytes(), 0);
    }

    #[test]
    fn eviction_leaves_unexpired_records_alone() {
        let mut f = KvFragment::new();
        let p = policy();
        step(
            &mut f,
            put("soon", b"v", Expect::Absent, Some(1_000), 0),
            &p,
            1,
        );
        step(
            &mut f,
            put("later", b"v", Expect::Absent, Some(HOUR), 0),
            &p,
            2,
        );

        assert_eq!(
            step(
                &mut f,
                KvCommand::Evict {
                    cutoff_ms: 1_000,
                    limit: 64
                },
                &p,
                3
            ),
            KvResponse::Evicted {
                removed: 1,
                more_expired: false,
            },
        );
        assert!(f.get_at("later", 1_000).is_some());
    }

    #[test]
    fn the_evict_batch_is_clamped_at_both_ends() {
        let p = KvPolicy {
            max_evict_per_apply: 32,
            ..policy()
        };
        assert_eq!(p.evict_batch(8), 8);
        assert_eq!(p.evict_batch(1_000), 32, "policy caps the batch");
        assert_eq!(
            p.evict_batch(0),
            1,
            "a zero batch would spin a caller that honors more_expired",
        );

        let reckless = KvPolicy {
            max_evict_per_apply: u32::MAX,
            ..policy()
        };
        assert_eq!(
            reckless.evict_batch(u32::MAX),
            HARD_MAX_EVICT_LIMIT,
            "the hard maximum is not a policy knob",
        );
    }

    #[test]
    fn has_expired_at_lets_a_ticker_skip_a_pointless_sweep() {
        let mut f = KvFragment::new();
        let p = policy();
        step(&mut f, put("k", b"v", Expect::Absent, Some(HOUR), 0), &p, 1);
        assert!(!f.has_expired_at(HOUR - 1));
        assert!(f.has_expired_at(HOUR));
    }

    // -----------------------------------------------------------------
    // Quotas
    // -----------------------------------------------------------------

    #[test]
    fn quotas_reject_rather_than_evict_live_data() {
        let p = KvPolicy {
            max_entries: 2,
            ..policy()
        };
        let mut f = KvFragment::new();
        step(&mut f, put("a", b"v", Expect::Absent, Some(HOUR), 0), &p, 1);
        step(&mut f, put("b", b"v", Expect::Absent, Some(HOUR), 0), &p, 2);
        assert_eq!(
            step(&mut f, put("c", b"v", Expect::Absent, Some(HOUR), 0), &p, 3),
            KvResponse::Rejected(KvRejection::EntryQuotaExceeded { entries: 3, max: 2 }),
        );
        // Overwriting an existing key is not a new entry.
        assert!(matches!(
            step(&mut f, put("a", b"w", Expect::Any, Some(HOUR), 0), &p, 4),
            KvResponse::Written { .. }
        ));
    }

    #[test]
    fn the_byte_quota_counts_the_replacement_not_the_sum() {
        // Room for exactly one 1-byte key with a 9-byte value.
        let p = KvPolicy {
            max_bytes: 10,
            ..policy()
        };
        let mut f = KvFragment::new();
        step(
            &mut f,
            put("k", b"123456789", Expect::Absent, Some(HOUR), 0),
            &p,
            1,
        );
        assert_eq!(f.physical_bytes(), 10);
        // A same-size replacement fits; the displaced record's bytes
        // must come off the total before the new ones go on.
        assert!(matches!(
            step(
                &mut f,
                put("k", b"987654321", Expect::Any, Some(HOUR), 0),
                &p,
                2
            ),
            KvResponse::Written { .. }
        ));
        assert_eq!(
            step(
                &mut f,
                put("k", b"0123456789", Expect::Any, Some(HOUR), 0),
                &p,
                3
            ),
            KvResponse::Rejected(KvRejection::ByteQuotaExceeded { bytes: 11, max: 10 }),
        );
    }

    /// Replacing an expired record must not be refused for space that
    /// record was no longer entitled to — otherwise a full fragment
    /// could never be written again until the ticker happened to run.
    #[test]
    fn a_full_fragment_still_accepts_a_write_over_an_expired_key() {
        let p = KvPolicy {
            max_entries: 1,
            max_bytes: 10,
            ..policy()
        };
        let mut f = KvFragment::new();
        step(
            &mut f,
            put("k", b"123456789", Expect::Absent, Some(HOUR), 0),
            &p,
            1,
        );
        assert!(
            matches!(
                step(
                    &mut f,
                    put("k", b"987654321", Expect::Absent, Some(HOUR), HOUR),
                    &p,
                    2
                ),
                KvResponse::Written { .. }
            ),
            "an expired record must not hold its own key's quota hostage",
        );
    }

    // -----------------------------------------------------------------
    // Determinism and wire stability
    // -----------------------------------------------------------------

    /// Replicas reduce the same log independently, so identical command
    /// sequences must produce byte-identical state — a `HashMap` or a
    /// clock read anywhere in here would break this.
    #[test]
    fn the_same_log_reduces_to_the_same_bytes() {
        let p = policy();
        let script: Vec<KvCommand> = vec![
            put("zeta", b"1", Expect::Any, Some(HOUR), 0),
            put("alpha", b"2", Expect::Any, Some(2 * HOUR), 0),
            put("mu", b"3", Expect::Any, Some(HOUR), 0),
            KvCommand::Delete {
                key: "zeta".into(),
                expect: Expect::Any,
                now_ms: 0,
            },
            put("alpha", b"4", Expect::Any, Some(3 * HOUR), 10),
            KvCommand::Evict {
                cutoff_ms: HOUR,
                limit: 2,
            },
            KvCommand::TakeOnce {
                key: "alpha".into(),
                now_ms: HOUR,
            },
        ];

        let run = || {
            let mut f = KvFragment::new();
            for (i, cmd) in script.iter().enumerate() {
                apply(&mut f, cmd, &p, 100 + i as u64);
            }
            f.check_invariants().expect("invariants hold");
            postcard::to_allocvec(&f).expect("fragment encodes")
        };
        assert_eq!(run(), run());
    }

    /// `KvCommand` rides in replicated log entries and postcard writes
    /// enum variants as a bare varint discriminant, so reordering these
    /// does not error — it silently decodes an old entry as a different
    /// command. Appending is safe; anything else is not.
    #[test]
    fn wire_format_is_pinned() {
        let encode = |c: &KvCommand| postcard::to_allocvec(c).expect("encodes");

        // Leading byte is the variant discriminant.
        assert_eq!(
            encode(&KvCommand::Put {
                key: "k".into(),
                value: vec![7],
                expect: Expect::Any,
                ttl_ms: Some(1),
                now_ms: 2,
            })[0],
            0,
        );
        assert_eq!(
            encode(&KvCommand::Delete {
                key: "k".into(),
                expect: Expect::Any,
                now_ms: 0,
            })[0],
            1,
        );
        assert_eq!(
            encode(&KvCommand::TakeOnce {
                key: "k".into(),
                now_ms: 0,
            })[0],
            2,
        );
        assert_eq!(
            encode(&KvCommand::Evict {
                cutoff_ms: 0,
                limit: 0,
            })[0],
            3,
        );

        // And the whole encoding of one command of each shape, so a
        // field reorder inside a variant is caught too.
        assert_eq!(
            encode(&KvCommand::Put {
                key: "ab".into(),
                value: vec![0xff],
                expect: Expect::Version(9),
                ttl_ms: Some(300),
                now_ms: 7,
            }),
            vec![0, 2, b'a', b'b', 1, 0xff, 2, 9, 1, 0xac, 0x02, 7],
        );
        assert_eq!(
            encode(&KvCommand::Delete {
                key: "ab".into(),
                expect: Expect::Absent,
                now_ms: 7,
            }),
            vec![1, 2, b'a', b'b', 1, 7],
        );
        assert_eq!(
            encode(&KvCommand::Evict {
                cutoff_ms: 300,
                limit: 4,
            }),
            vec![3, 0xac, 0x02, 4],
        );

        // `Expect` is its own wire format, embedded above.
        assert_eq!(postcard::to_allocvec(&Expect::Any).unwrap(), vec![0]);
        assert_eq!(postcard::to_allocvec(&Expect::Absent).unwrap(), vec![1]);
        assert_eq!(
            postcard::to_allocvec(&Expect::Version(9)).unwrap(),
            vec![2, 9]
        );
    }

    /// `KvFragment` rides in application snapshots, so its field order
    /// is a format too — and a struct field append fails to decode
    /// rather than degrading.
    #[test]
    fn a_fragment_round_trips_through_a_snapshot() {
        let mut f = KvFragment::new();
        let p = KvPolicy {
            allow_immortal: true,
            ..policy()
        };
        step(&mut f, put("a", b"1", Expect::Any, Some(HOUR), 0), &p, 1);
        step(&mut f, put("b", b"2", Expect::Any, None, 0), &p, 2);

        let bytes = postcard::to_allocvec(&f).expect("encodes");
        let restored: KvFragment = postcard::from_bytes(&bytes).expect("decodes");
        assert_eq!(restored, f);
        restored
            .check_invariants()
            .expect("a restored fragment carries its denormalizations");
    }

    #[test]
    fn oversized_keys_and_values_are_refused_at_apply_time() {
        let p = KvPolicy {
            max_key_bytes: 3,
            max_value_bytes: 3,
            ..policy()
        };
        let mut f = KvFragment::new();
        assert_eq!(
            step(&mut f, put("abcd", b"v", Expect::Any, Some(HOUR), 0), &p, 1),
            KvResponse::Rejected(KvRejection::KeyTooLarge { bytes: 4, max: 3 }),
        );
        assert_eq!(
            step(&mut f, put("a", b"vvvv", Expect::Any, Some(HOUR), 0), &p, 2),
            KvResponse::Rejected(KvRejection::ValueTooLarge { bytes: 4, max: 3 }),
        );
        assert_eq!(f.physical_len(), 0);
    }
}
