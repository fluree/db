//! One retention pass over one branch: discover new schedules from the
//! commits since the watermark, expire what is due through one ordinary
//! commit made against the head it was verified at, and checkpoint.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fluree_db_core::graph_registry::{CONFIG_GRAPH_ID, TXN_META_GRAPH_ID};
use fluree_db_core::{
    collect_first_parent_cids, load_commit_by_id, load_commit_envelope_by_id, Commit, ContentId,
    ContentKind, ContentStore, Flake, FlakeValue, GraphId, IndexType, RangeMatch, RangeOptions,
    RangeTest, Sid, TxnMetaEntry, TxnMetaValue,
};
use fluree_db_ledger::LedgerState;
use fluree_vocab::db;
use fluree_vocab::namespaces::FLUREE_DB;

use super::policy::{CompiledPolicy, IriCache, NoHistoryRule};
use super::registry::{self, Registration};
use super::state::{Entry, FactKey, Retirement, Schedule, Source};
use super::writer::{RetentionBatch, RetentionWriter, SubmitOutcome};
use super::{is_ttl_predicate, RetentionLimits};
use crate::{ApiError, Fluree, Result};

/// What one pass over one branch did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PassReport {
    /// The branch was not registered for retention.
    pub inactive: bool,
    /// Commits discovered this pass.
    pub discovered_commits: usize,
    /// The discovery watermark after the pass.
    pub watermark_t: i64,
    /// Scheduled occurrences after the pass.
    pub pending: usize,
    /// Occurrences retracted by this pass's commit.
    pub expired: usize,
    /// Superseded TTL annotations retired by this pass's commit.
    pub retired: usize,
    /// The commit this pass made, if any.
    pub committed_t: Option<i64>,
    /// The batch was not submitted: novelty is near its limit.
    pub deferred: bool,
    /// The head moved before the batch committed; it is recomputed next pass.
    pub head_moved: bool,
    /// What blocks or degrades processing.
    pub problems: Vec<String>,
}

/// Schedules kept in memory between passes, by branch, with the blob each
/// was loaded from or saved as.
#[derive(Default)]
pub(crate) struct ScheduleCache {
    schedules: parking_lot::Mutex<HashMap<String, CachedSchedule>>,
}

struct CachedSchedule {
    /// The persisted blob the schedule extends (what the registration names).
    blob: Option<String>,
    schedule: Schedule,
    /// When the schedule was last persisted (epoch ms).
    persisted_ms: i64,
}

impl ScheduleCache {
    fn take(&self, ledger_id: &str, blob: Option<&str>) -> Option<(Schedule, i64)> {
        let mut map = self.schedules.lock();
        match map.remove(ledger_id) {
            Some(cached) if cached.blob.as_deref() == blob => {
                Some((cached.schedule, cached.persisted_ms))
            }
            _ => None,
        }
    }

    fn put(&self, ledger_id: &str, blob: Option<String>, schedule: Schedule, persisted_ms: i64) {
        self.schedules.lock().insert(
            ledger_id.to_string(),
            CachedSchedule {
                blob,
                schedule,
                persisted_ms,
            },
        );
    }

    /// The watermark and pending count of a schedule with nothing to do at
    /// `now_ms`: discovered through `head_t`, nothing due, nothing to retire.
    fn idle(
        &self,
        ledger_id: &str,
        blob: Option<&str>,
        head: (i64, Option<&ContentId>),
        now_ms: i64,
    ) -> Option<(i64, usize)> {
        let map = self.schedules.lock();
        let cached = map.get(ledger_id)?;
        let s = &cached.schedule;
        (cached.blob.as_deref() == blob
            && s.watermark_t == head.0
            && (s.watermark_id.is_none() || s.watermark_id.as_ref() == head.1)
            && s.retire.is_empty()
            && s.next_due_ms().is_none_or(|due| due > now_ms))
        .then(|| (s.watermark_t, s.len()))
    }

    pub(crate) fn peek<T>(&self, ledger_id: &str, f: impl FnOnce(&Schedule) -> T) -> Option<T> {
        self.schedules.lock().get(ledger_id).map(|c| f(&c.schedule))
    }
}

/// A lifetime from one instruction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lifetime {
    Seconds(u64),
    /// Absolute deadline, epoch ms.
    At(i64),
}

impl Lifetime {
    /// The lifetime an `f:ttl` or `f:expiresAt` instruction states.
    fn of_instruction(flake: &Flake) -> Option<Self> {
        match (&*flake.p.name, &flake.o) {
            (db::TTL, o) => fluree_db_transact::ttl_input::ttl_seconds_from_value(o)
                .ok()
                .map(Lifetime::Seconds),
            (_, FlakeValue::DateTime(dt)) => Some(Lifetime::At(dt.epoch_millis())),
            _ => None,
        }
    }

    fn of_txn_meta(entry: &TxnMetaEntry) -> Option<Self> {
        match (&*entry.predicate_name, &entry.value) {
            (db::TTL, TxnMetaValue::Long(n)) => u64::try_from(*n).ok().map(Lifetime::Seconds),
            (
                db::EXPIRES_AT,
                TxnMetaValue::TypedLiteral { value, .. } | TxnMetaValue::String(value),
            ) => chrono::DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|d| Lifetime::At(d.timestamp_millis())),
            _ => None,
        }
    }

    /// `None` with no expiry (`0` seconds); `Err` when a relative lifetime
    /// has no commit time to start from.
    fn due(self, commit_ms: Option<i64>) -> std::result::Result<Option<i64>, ()> {
        match self {
            Lifetime::Seconds(0) => Ok(None),
            Lifetime::Seconds(s) => {
                let start = commit_ms.ok_or(())?;
                Ok(Some(
                    i64::try_from(s)
                        .ok()
                        .and_then(|s| s.checked_mul(1000))
                        .and_then(|ms| start.checked_add(ms))
                        .unwrap_or(i64::MAX),
                ))
            }
            Lifetime::At(ms) => Ok(Some(ms)),
        }
    }
}

/// A per-value instruction in one commit.
#[derive(Clone, Debug)]
struct Instruction {
    reifier: Sid,
    lifetime: Lifetime,
}

/// Whether a commit's txn-meta carries retention control input: the marker,
/// or a transaction TTL default.
pub(crate) fn carries_retention_input(txn_meta: &[TxnMetaEntry]) -> bool {
    txn_meta
        .iter()
        .any(|e| fluree_vocab::txn_meta::is_retention_input(e.predicate_ns, &e.predicate_name))
}

/// The earliest commit after `after_t` on the first-parent chain from
/// `head` that carries retention input, walking envelopes only.
pub(crate) async fn probe_activation(
    store: &dyn ContentStore,
    head: &ContentId,
    after_t: i64,
) -> Result<Option<i64>> {
    let mut found = None;
    let mut next = Some(head.clone());
    while let Some(cid) = next {
        let envelope = load_commit_envelope_by_id(store, &cid).await?;
        if envelope.t <= after_t {
            break;
        }
        if carries_retention_input(&envelope.txn_meta) {
            found = Some(envelope.t);
        }
        next = envelope.parents.first().cloned();
    }
    Ok(found)
}

fn rfc3339_ms(text: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|d| d.timestamp_millis())
}

/// When `commit`'s relative lifetimes start (epoch ms): when its write
/// arrived. That is the original's on a replay ([`db::LIFETIME_START`]), the
/// commit's `db:receivedAt` where it records one (a write with a backdated
/// event time counts from its arrival, not from that time), and otherwise
/// its time.
pub(crate) fn lifetime_start_ms(commit: &Commit) -> Option<i64> {
    let entry = |name: &str| {
        commit
            .txn_meta
            .iter()
            .find(|e| e.predicate_ns == FLUREE_DB && *e.predicate_name == *name)
            .map(|e| &e.value)
    };
    let replayed = entry(db::LIFETIME_START).and_then(|v| match v {
        TxnMetaValue::TypedLiteral { value, .. } | TxnMetaValue::String(value) => rfc3339_ms(value),
        _ => None,
    });
    let received = || match entry(db::RECEIVED_AT) {
        Some(TxnMetaValue::Long(ms)) => Some(*ms),
        _ => None,
    };
    replayed
        .or_else(received)
        .or_else(|| commit.time.as_deref().and_then(rfc3339_ms))
}

fn is_marked(commit: &Commit) -> bool {
    commit
        .txn_meta
        .iter()
        .any(|e| e.predicate_ns == FLUREE_DB && &*e.predicate_name == db::RETENTION_CONTROL)
}

fn txn_default(commit: &Commit) -> Option<Lifetime> {
    commit
        .txn_meta
        .iter()
        .filter(|e| e.predicate_ns == FLUREE_DB)
        .find_map(Lifetime::of_txn_meta)
}

/// Where a value a merge brought in began, on the line it merged.
#[derive(Clone, Copy, Debug)]
struct Origin {
    start_ms: Option<i64>,
    txn_default: Option<Lifetime>,
}

/// For each of `wanted` (a fact a merge brought in, with the reifier of
/// the instruction it carries), the commit on the line from `from` that
/// began its current occurrence or last renewed it with an instruction,
/// read back along that line's first parents. A fact that commit took from
/// a merge of its own is followed onto that merge's line. A fact the line
/// never asserted is left out. Commit-level: a lifetime is close, not exact.
fn merged_origins(
    store: &dyn ContentStore,
    from: ContentId,
    wanted: HashMap<FactKey, Option<Sid>>,
) -> futures::future::BoxFuture<'_, Result<HashMap<FactKey, Origin>>> {
    Box::pin(async move {
        // A fact's origin, and the line it came from when that commit merged it.
        type Found = (Origin, Option<ContentId>);
        let by_reifier: HashMap<Sid, FactKey> = wanted
            .iter()
            .filter_map(|(k, r)| r.clone().map(|r| (r, k.clone())))
            .collect();
        let mut open: HashSet<FactKey> = wanted.keys().cloned().collect();
        let mut run: HashMap<FactKey, Found> = HashMap::new();
        let mut found: HashMap<FactKey, Found> = HashMap::new();
        let mut next = Some(from);
        while let Some(cid) = next.filter(|_| !open.is_empty()) {
            let commit = load_commit_by_id(store, &cid).await?;
            let here: Found = (
                Origin {
                    start_ms: lifetime_start_ms(&commit),
                    txn_default: txn_default(&commit),
                },
                commit.parents.get(1).cloned(),
            );
            let mut asserted = HashSet::new();
            let mut retracted = HashSet::new();
            let mut renewed = HashSet::new();
            let marked = is_marked(&commit);
            for flake in &commit.flakes {
                if marked && flake.op && is_ttl_predicate(&flake.p) {
                    if let Some(key) = by_reifier.get(&flake.s).filter(|k| open.contains(*k)) {
                        renewed.insert(key.clone());
                    }
                    continue;
                }
                let key = FactKey::of(flake);
                if open.contains(&key) {
                    if flake.op {
                        asserted.insert(key);
                    } else {
                        retracted.insert(key);
                    }
                }
            }
            for key in renewed {
                open.remove(&key);
                run.remove(&key);
                found.insert(key, here.clone());
            }
            for key in asserted {
                if open.contains(&key) {
                    run.insert(key, here.clone());
                }
            }
            for key in retracted {
                if let Some(origin) = run.remove(&key) {
                    open.remove(&key);
                    found.insert(key, origin);
                }
            }
            next = commit.parents.first().cloned();
        }
        found.extend(run);

        let mut lines: HashMap<ContentId, HashMap<FactKey, Option<Sid>>> = HashMap::new();
        for (key, (_, line)) in &found {
            if let Some(line) = line {
                lines
                    .entry(line.clone())
                    .or_default()
                    .insert(key.clone(), wanted.get(key).cloned().flatten());
            }
        }
        let mut origins: HashMap<FactKey, Origin> =
            found.into_iter().map(|(k, (o, _))| (k, o)).collect();
        for (line, keys) in lines {
            origins.extend(merged_origins(store, line, keys).await?);
        }
        Ok(origins)
    })
}

fn is_system_graph(g_id: GraphId) -> bool {
    g_id == TXN_META_GRAPH_ID || g_id == CONFIG_GRAPH_ID
}

/// State one pass reads with.
struct Ctx<'a> {
    state: &'a LedgerState,
    reverse_graph: HashMap<Sid, GraphId>,
    iris: IriCache,
}

impl Ctx<'_> {
    fn g_id(&self, g: &Option<Sid>) -> Option<GraphId> {
        match g {
            None => Some(0),
            Some(sid) => self.reverse_graph.get(sid).copied(),
        }
    }

    async fn range(&self, g_id: GraphId, pattern: RangeMatch, to_t: i64) -> Result<Vec<Flake>> {
        if to_t < 1 {
            return Ok(Vec::new());
        }
        Ok(fluree_db_core::range_with_overlay(
            &self.state.snapshot,
            g_id,
            self.state.novelty.as_ref(),
            IndexType::Spot,
            RangeTest::Eq,
            pattern,
            RangeOptions {
                to_t: Some(to_t),
                ..Default::default()
            },
        )
        .await?)
    }

    /// The stored flakes stating `key` as of `to_t`.
    async fn current(&self, g_id: GraphId, key: &FactKey, to_t: i64) -> Result<Vec<Flake>> {
        let pattern = RangeMatch {
            s: Some(key.s.clone()),
            p: Some(key.p.clone()),
            o: Some(key.o.clone()),
            dt: Some(key.dt.clone()),
            t: None,
        };
        Ok(self
            .range(g_id, pattern, to_t)
            .await?
            .into_iter()
            .map(|f| Flake {
                g: key.g.clone(),
                ..f
            })
            .filter(|f| key.matches(f))
            .collect())
    }

    async fn reifier_links(&self, g_id: GraphId, reifier: &Sid, to_t: i64) -> Result<Vec<Flake>> {
        self.range(
            g_id,
            RangeMatch::subject_predicate(
                reifier.clone(),
                fluree_db_core::rdf_reifies_sid().clone(),
            ),
            to_t,
        )
        .await
    }
}

/// A value a merge commit brought in, scheduled once its origin on the
/// merged line is known.
struct Merged {
    key: FactKey,
    /// Its slot, for a new occurrence; `None` renews a current one.
    slot: Option<u32>,
    instruction: Option<Instruction>,
}

/// Discover one commit's retention input into `schedule`. A value a merge
/// brought in keeps the lifetime it began on the merged line: it counts from
/// when it was written there, not from the merge (see [`merged_origins`]).
async fn discover_commit(
    schedule: &mut Schedule,
    commit: &Commit,
    store: &dyn ContentStore,
    ctx: &mut Ctx<'_>,
    policy: &CompiledPolicy,
) -> Result<()> {
    let t = commit.t;
    let time_ms = lifetime_start_ms(commit);
    let marked = is_marked(commit);
    let txn_default = txn_default(commit);
    let merged_from = commit.parents.get(1).cloned();
    let mut merged: Vec<Merged> = Vec::new();

    // A graph's drop ends what it held, before the commit's own writes:
    // nothing it held is due to expire, and a value written to it again is
    // a new occurrence.
    if !commit.graph_drops.is_empty() {
        let mut dropped = |g: &Option<Sid>| {
            let iri = match g {
                None => Some(fluree_db_core::DEFAULT_GRAPH_IRI.to_string()),
                Some(g) => ctx.iris.iri(&ctx.state.snapshot, g),
            };
            iri.is_some_and(|iri| commit.graph_drops.contains(&iri))
        };
        let ended: Vec<FactKey> = schedule
            .entries()
            .map(|(k, _)| k.clone())
            .filter(|k| dropped(&k.g))
            .collect();
        for key in &ended {
            schedule.remove(key);
        }
        schedule.retire.retain(|r| !dropped(&r.g));
    }

    // Per-value instructions are honored only in commits the transactor
    // marked: annotations written before TTL support are not instructions.
    let mut instructions: HashMap<FactKey, Instruction> = HashMap::new();
    let mut withdrawn: Vec<(Option<Sid>, Sid)> = Vec::new();
    if marked {
        for flake in &commit.flakes {
            if !is_ttl_predicate(&flake.p) {
                continue;
            }
            let Some(g_id) = ctx.g_id(&flake.g) else {
                continue;
            };
            if is_system_graph(g_id) {
                continue;
            }
            if !flake.op {
                withdrawn.push((flake.g.clone(), flake.s.clone()));
                continue;
            }
            let Some(lifetime) = Lifetime::of_instruction(flake) else {
                schedule.note_problem(format!("t {t}: a TTL instruction with an invalid value"));
                continue;
            };
            let mut links: Vec<FlakeValue> = commit
                .flakes
                .iter()
                .filter(|f| {
                    f.op && f.g == flake.g && f.s == flake.s && fluree_db_core::is_rdf_reifies(&f.p)
                })
                .map(|f| f.o.clone())
                .collect();
            if links.is_empty() {
                links = ctx
                    .reifier_links(g_id, &flake.s, t)
                    .await?
                    .into_iter()
                    .map(|f| f.o)
                    .collect();
            }
            let target = match links.as_slice() {
                [FlakeValue::TripleTerm(tt)] => FactKey::of_triple_term(flake.g.clone(), tt),
                _ => {
                    schedule.note_problem(format!(
                        "t {t}: a TTL instruction whose annotation does not identify one triple"
                    ));
                    continue;
                }
            };
            instructions.entry(target).or_insert(Instruction {
                reifier: flake.s.clone(),
                lifetime,
            });
        }
    }

    // A TTL instruction retracted without a replacement withdraws it: the
    // occurrence it governed stops being scheduled.
    for (g, reifier) in withdrawn {
        schedule.retire.remove(&Retirement {
            g: g.clone(),
            reifier: reifier.clone(),
        });
        if instructions.values().any(|i| i.reifier == reifier) {
            continue;
        }
        let governed: Vec<FactKey> = schedule
            .entries()
            .filter(|(k, e)| k.g == g && e.reifier.as_ref() == Some(&reifier))
            .map(|(k, _)| k.clone())
            .collect();
        for key in governed {
            schedule.remove(&key);
        }
    }

    let mut asserted: HashSet<FactKey> = HashSet::new();
    for (slot, flake) in commit.occurrences() {
        let Some(g_id) = ctx.g_id(&flake.g) else {
            continue;
        };
        if is_system_graph(g_id)
            || is_ttl_predicate(&flake.p)
            || fluree_db_core::is_rdf_reifies(&flake.p)
        {
            continue;
        }
        let key = FactKey::of(flake);
        if !flake.op {
            if let Some(entry) = schedule.remove(&key) {
                if let Some(reifier) = entry.reifier {
                    schedule.retire.insert(Retirement {
                        g: key.g.clone(),
                        reifier,
                    });
                }
            }
            continue;
        }
        let instruction = instructions.get(&key).cloned();
        if instruction.is_some() {
            asserted.insert(key.clone());
        }
        // Reasserting a current value is not a new occurrence: only an
        // explicit instruction renews it.
        if merged_from.is_some() {
            let existed =
                schedule.contains(&key) || !ctx.current(g_id, &key, t - 1).await?.is_empty();
            if !existed || instruction.is_some() {
                merged.push(Merged {
                    slot: (!existed).then_some(slot),
                    key,
                    instruction,
                });
            }
            continue;
        }
        let source = match &instruction {
            Some(i) => Some((i.lifetime, Source::Value)),
            None => txn_default
                .map(|lifetime| (lifetime, Source::Transaction))
                .or_else(|| property_lifetime(ctx, policy, &key)),
        };
        let Some((lifetime, source)) = source else {
            continue;
        };
        if schedule.contains(&key) || !ctx.current(g_id, &key, t - 1).await?.is_empty() {
            if let Some(instruction) = instruction {
                renew(schedule, &key, &instruction, t, time_ms);
            }
            continue;
        }
        occur(
            schedule,
            key,
            (t, slot),
            (lifetime, source),
            instruction,
            time_ms,
        );
    }

    // Instructions on values this commit did not assert: a renewal (or
    // cancellation) of the current occurrence, or nothing for a triple that
    // is only reified, not asserted.
    for (key, instruction) in instructions {
        if asserted.contains(&key) {
            continue;
        }
        let Some(g_id) = ctx.g_id(&key.g) else {
            continue;
        };
        if schedule.contains(&key) || !ctx.current(g_id, &key, t).await?.is_empty() {
            if merged_from.is_some() {
                merged.push(Merged {
                    key,
                    slot: None,
                    instruction: Some(instruction),
                });
            } else {
                renew(schedule, &key, &instruction, t, time_ms);
            }
        }
    }

    let Some(from) = merged_from.filter(|_| !merged.is_empty()) else {
        return Ok(());
    };
    let wanted = merged
        .iter()
        .map(|m| {
            (
                m.key.clone(),
                m.instruction.as_ref().map(|i| i.reifier.clone()),
            )
        })
        .collect();
    let origins = merged_origins(store, from, wanted).await?;
    for Merged {
        key,
        slot,
        instruction,
    } in merged
    {
        let origin = origins.get(&key);
        let start_ms = origin.and_then(|o| o.start_ms).or(time_ms);
        let Some(slot) = slot else {
            if let Some(instruction) = instruction {
                renew(schedule, &key, &instruction, t, start_ms);
            }
            continue;
        };
        let source = match &instruction {
            Some(i) => Some((i.lifetime, Source::Value)),
            None => origin
                .and_then(|o| o.txn_default)
                .map(|lifetime| (lifetime, Source::Transaction))
                .or_else(|| property_lifetime(ctx, policy, &key)),
        };
        if let Some(source) = source {
            occur(schedule, key, (t, slot), source, instruction, start_ms);
        }
    }
    Ok(())
}

/// The lifetime a property policy gives `key`.
fn property_lifetime(
    ctx: &mut Ctx<'_>,
    policy: &CompiledPolicy,
    key: &FactKey,
) -> Option<(Lifetime, Source)> {
    let graph = match &key.g {
        None => None,
        Some(g) => ctx.iris.iri(&ctx.state.snapshot, g),
    };
    let predicate = ctx.iris.iri(&ctx.state.snapshot, &key.p)?;
    policy
        .duration(graph.as_deref(), &predicate)
        .map(|s| (Lifetime::Seconds(s), Source::Property))
}

/// Schedule a new occurrence of `key` at `(t, slot)`, its lifetime counted
/// from `start_ms`.
fn occur(
    schedule: &mut Schedule,
    key: FactKey,
    (t, slot): (i64, u32),
    (lifetime, source): (Lifetime, Source),
    instruction: Option<Instruction>,
    start_ms: Option<i64>,
) {
    match lifetime.due(start_ms) {
        Ok(Some(due_ms)) => schedule.insert(
            key,
            Entry {
                due_ms,
                occurrence_t: t,
                occurrence_slot: slot,
                source,
                generation_t: t,
                reifier: instruction.map(|i| i.reifier),
            },
        ),
        Ok(None) => {}
        Err(()) => schedule.note_problem(format!(
            "t {t} has no commit time; its relative TTLs need an explicit backfill time basis"
        )),
    }
}

/// Apply an explicit instruction to a value's current occurrence. The
/// annotation it supersedes is queued for retirement.
fn renew(
    schedule: &mut Schedule,
    key: &FactKey,
    instruction: &Instruction,
    t: i64,
    time_ms: Option<i64>,
) {
    let previous = schedule.get(key).cloned();
    if let Some(old) = previous.as_ref().and_then(|e| e.reifier.clone()) {
        if old != instruction.reifier {
            schedule.retire.insert(Retirement {
                g: key.g.clone(),
                reifier: old,
            });
        }
    }
    match instruction.lifetime.due(time_ms) {
        Ok(Some(due_ms)) => schedule.insert(
            key.clone(),
            Entry {
                due_ms,
                occurrence_t: previous.as_ref().map_or(0, |e| e.occurrence_t),
                occurrence_slot: previous.as_ref().map_or(0, |e| e.occurrence_slot),
                source: Source::Value,
                generation_t: t,
                reifier: Some(instruction.reifier.clone()),
            },
        ),
        Ok(None) => {
            schedule.remove(key);
        }
        Err(()) => schedule.note_problem(format!(
            "t {t} has no commit time; its relative TTLs need an explicit backfill time basis"
        )),
    }
}

/// The retractions that retire an annotation's TTL instruction: its TTL
/// facts, and the whole annotation when it carries nothing else.
async fn retire_annotation(
    ctx: &Ctx<'_>,
    g: &Option<Sid>,
    g_id: GraphId,
    reifier: &Sid,
    to_t: i64,
) -> Result<Vec<Flake>> {
    let body: Vec<Flake> = ctx
        .range(g_id, RangeMatch::subject(reifier.clone()), to_t)
        .await?
        .into_iter()
        .map(|f| Flake { g: g.clone(), ..f })
        .collect();
    if !body.iter().any(|f| is_ttl_predicate(&f.p)) {
        return Ok(Vec::new());
    }
    let managed = body
        .iter()
        .all(|f| is_ttl_predicate(&f.p) || fluree_db_core::is_rdf_reifies(&f.p));
    Ok(body
        .into_iter()
        .filter(|f| managed || is_ttl_predicate(&f.p))
        .map(|f| Flake { op: false, ..f })
        .collect())
}

/// Whether more than one annotation or relationship that carries more than
/// a TTL instruction shares the triple: expiring it would end all of them.
async fn shared(ctx: &Ctx<'_>, g_id: GraphId, key: &FactKey, to_t: i64) -> Result<bool> {
    let target = FlakeValue::TripleTerm(Box::new(fluree_db_core::TripleTermValue {
        s: key.s.clone(),
        p: key.p.clone(),
        o: key.o.clone(),
        dt: key.dt.clone(),
        lang: key.lang.clone(),
    }));
    let links = fluree_db_core::range_with_overlay(
        &ctx.state.snapshot,
        g_id,
        ctx.state.novelty.as_ref(),
        IndexType::Post,
        RangeTest::Eq,
        RangeMatch::predicate_object(fluree_db_core::rdf_reifies_sid().clone(), target),
        RangeOptions {
            to_t: Some(to_t),
            ..Default::default()
        },
    )
    .await?;
    let mut substantive = 0;
    let mut seen = HashSet::new();
    for link in links {
        if !seen.insert(link.s.clone()) {
            continue;
        }
        let body = ctx.range(g_id, RangeMatch::subject(link.s), to_t).await?;
        if body
            .iter()
            .any(|f| !is_ttl_predicate(&f.p) && !fluree_db_core::is_rdf_reifies(&f.p))
        {
            substantive += 1;
        }
    }
    Ok(substantive > 1)
}

impl Fluree {
    /// Run one retention pass over `ledger_id` at `now_ms`, committing due
    /// expirations through `writer`.
    pub(crate) async fn retention_pass(
        &self,
        writer: &dyn RetentionWriter,
        cache: &ScheduleCache,
        ledger_id: &str,
        now_ms: i64,
        limits: &RetentionLimits,
    ) -> Result<PassReport> {
        let ns = self.publisher()?;
        let (_, stored) = registry::read_stored(ns, ledger_id).await?;
        let Some(stored) = stored else {
            return Ok(PassReport {
                inactive: true,
                ..Default::default()
            });
        };
        let Some(record) = ns.lookup(ledger_id).await? else {
            return Err(ApiError::NotFound(ledger_id.to_string()));
        };
        // Its scalars decide an idle pass, without its lists being read.
        let shell = registry::shell(&stored)?;
        let heartbeat_due = shell
            .last_pass_ms
            .is_none_or(|last| now_ms - last >= limits.heartbeat_ms);
        let published =
            registry::journal_empty(&stored) || shell.checkpointed == shell.journal.generation;
        // Nothing committed, due, waiting or unpublished since the last
        // pass: done, with no ledger load.
        if !heartbeat_due && published {
            if let Some((watermark_t, pending)) = cache.idle(
                ledger_id,
                shell.schedule.as_deref(),
                (record.commit_t, record.commit_head_id.as_ref()),
                now_ms,
            ) {
                return Ok(PassReport {
                    watermark_t,
                    pending,
                    problems: shell.problems,
                    ..Default::default()
                });
            }
        }
        let registration = registry::resolve(&stored, self.registry_objects()).await?;
        let store = self
            .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
            .await?;
        let state = self
            .ledger_cached(ledger_id)
            .await?
            .snapshot()
            .await
            .to_ledger_state();
        let head_t = state.t();

        let (mut schedule, persisted_ms) =
            match cache.take(ledger_id, registration.schedule.as_deref()) {
                Some(cached) => cached,
                None => (load_schedule(store.as_ref(), &registration).await, now_ms),
            };
        let loaded_blob = registration.schedule.clone();
        let before = schedule_fingerprint(&schedule);
        let mut report = PassReport::default();

        let mut ctx = Ctx {
            state: &state,
            reverse_graph: state.snapshot.build_reverse_graph()?,
            iris: IriCache::default(),
        };

        // Discovery. Every valid `f:noHistory` policy is recorded at the
        // commit that set it, so one enabled and disabled between passes
        // still covers what it was in force for.
        let mut config_problems = Vec::new();
        let mut rule_transitions: Vec<(i64, Vec<NoHistoryRule>)> = Vec::new();
        let mut record_rules = |t: i64, policy: &CompiledPolicy| {
            if policy.problems.is_empty() {
                rule_transitions.push((t, policy.no_history.clone()));
            }
        };
        // A line rewritten under the schedule (a rebase, a reset) leaves it
        // holding what commits the branch no longer has decided, and passes
        // over what the new line brought in below its watermark: it is
        // rediscovered from the activation boundary.
        // The published head decides it: a cached state behind it is not a
        // rewrite.
        let fresh = || Schedule::starting_after(registration.active_since_t - 1);
        if schedule.watermark_id.is_some()
            && (schedule.watermark_t > record.commit_t
                || (schedule.watermark_t == record.commit_t
                    && schedule.watermark_id != record.commit_head_id))
        {
            schedule = fresh();
        }
        if schedule.watermark_t < head_t {
            if let Some(head) = state.head_commit_id.clone() {
                let mut cids =
                    collect_first_parent_cids(store.as_ref(), &head, schedule.watermark_t).await?;
                cids.reverse();
                if let (Some(watermark_id), Some((_, oldest))) =
                    (schedule.watermark_id.clone(), cids.first())
                {
                    let oldest = load_commit_envelope_by_id(store.as_ref(), oldest).await?;
                    if oldest.parents.first() != Some(&watermark_id) {
                        schedule = fresh();
                        cids =
                            collect_first_parent_cids(store.as_ref(), &head, schedule.watermark_t)
                                .await?;
                        cids.reverse();
                    }
                }
                cids.truncate(limits.max_commits_per_pass);
                let mut policy = self
                    .retention_policy_at(&state, schedule.watermark_t)
                    .await?;
                record_rules(schedule.watermark_t, &policy);
                for (t, cid) in cids {
                    let commit = load_commit_by_id(store.as_ref(), &cid).await?;
                    discover_commit(&mut schedule, &commit, store.as_ref(), &mut ctx, &policy)
                        .await?;
                    schedule.watermark_id = Some(cid);
                    let writes_config = commit.flakes.iter().any(|f| {
                        f.g.as_ref()
                            .is_some_and(|g| ctx.reverse_graph.get(g) == Some(&CONFIG_GRAPH_ID))
                    });
                    if writes_config {
                        policy = self.retention_policy_at(&state, t).await?;
                        record_rules(t, &policy);
                    }
                    schedule.watermark_t = t;
                    report.discovered_commits += 1;
                }
                config_problems = policy.problems.clone();
            }
        } else {
            let policy = self.retention_policy_at(&state, head_t).await?;
            record_rules(head_t, &policy);
            config_problems = policy.problems.clone();
        }

        // Expiration, once discovery has caught up with the head it verifies
        // against, and never while the retention config is invalid.
        if schedule.watermark_t >= head_t && config_problems.is_empty() {
            let batch = self
                .build_batch(&mut schedule, &ctx, head_t, now_ms, limits)
                .await?;
            if !batch.retractions.is_empty() {
                let (expired, retired) = (batch.expired, batch.retired);
                match writer.submit(ledger_id, batch).await? {
                    SubmitOutcome::Committed { t, .. } => {
                        report.committed_t = Some(t);
                        report.expired = expired;
                        report.retired = retired;
                    }
                    SubmitOutcome::HeadMoved => report.head_moved = true,
                    SubmitOutcome::Deferred => report.deferred = true,
                }
            }
        }

        report.watermark_t = schedule.watermark_t;
        report.pending = schedule.len();
        report.problems = config_problems.clone();
        report.problems.extend(schedule.problems.iter().cloned());

        // History rules follow each policy discovered, in commit order and in
        // the same write as the watermark that passed it.
        let apply_rules = |journal: &mut super::journal::Journal| {
            rule_transitions
                .iter()
                .fold(false, |changed, (t, desired)| {
                    journal.apply_history_rules(desired, *t) | changed
                })
        };
        let journal_changed = apply_rules(&mut registration.journal.clone());

        // Checkpoint. The schedule is rebuilt from the commits after the
        // persisted watermark, so persisting it is throttled; the journal,
        // which serving reads, is published at once.
        let changed = before != schedule_fingerprint(&schedule);
        let counters_changed = report.committed_t.is_some()
            || report.head_moved
            || registration.problems != report.problems;
        let persist = changed
            && (journal_changed || heartbeat_due || now_ms - persisted_ms >= limits.checkpoint_ms);
        let mut blob = loaded_blob.clone();
        let mut persisted_at = persisted_ms;
        let mut watermark_t = registration.watermark_t;
        if persist {
            let bytes = schedule.encode().map_err(ApiError::internal)?;
            let cid = store.put(ContentKind::RetentionState, &bytes).await?;
            blob = Some(cid.to_string());
            persisted_at = now_ms;
            watermark_t = schedule.watermark_t;
        }
        if persist || heartbeat_due || counters_changed || journal_changed {
            let next = Registration {
                watermark_t,
                schedule: blob.clone(),
                next_due_ms: schedule.next_due_ms(),
                pending: schedule.len() as u64,
                last_pass_ms: Some(now_ms),
                expired_total: registration.expired_total + report.expired as u64,
                retries: registration.retries + u64::from(report.head_moved),
                problems: report.problems.clone(),
                ..registration.clone()
            };
            if journal_changed {
                self.retention.watch.journal_changing(ledger_id);
            }
            registry::update(ns, self.registry_objects(), ledger_id, |current| {
                // Another owner (or a re-activation) may have moved it on.
                match current {
                    // A rebuilt schedule lowers the watermark this pass read.
                    Some(c)
                        if c.watermark_t > next.watermark_t
                            && c.watermark_t != registration.watermark_t =>
                    {
                        None
                    }
                    Some(c) if c.active_since_t < next.active_since_t => None,
                    _ => {
                        // The journal as it now stands (a removal may have
                        // landed meanwhile), with the rules brought in line.
                        let mut updated = next.clone();
                        updated.journal = current.map(|c| c.journal.clone()).unwrap_or_default();
                        #[cfg(feature = "native")]
                        {
                            updated.erasures =
                                current.map(|c| c.erasures.clone()).unwrap_or_default();
                        }
                        apply_rules(&mut updated.journal);
                        Some(updated)
                    }
                }
            })
            .await?;
            if persist {
                if let Some(old) = loaded_blob
                    .as_deref()
                    .and_then(|s| s.parse::<ContentId>().ok())
                {
                    let _ = store.release(&old).await;
                }
            }
        }
        cache.put(ledger_id, blob, schedule, persisted_at);
        let current = registry::read(ns, self.registry_objects(), ledger_id)
            .await?
            .1;
        // Also a journal an earlier pass or removal could not publish.
        if current
            .as_ref()
            .is_some_and(|c| !c.journal.is_empty() && c.checkpointed != c.journal.generation)
        {
            self.checkpoint_journal(ledger_id).await?;
        }
        Ok(report)
    }

    /// The property policies in force for commits after `t`.
    async fn retention_policy_at(&self, state: &LedgerState, t: i64) -> Result<CompiledPolicy> {
        if t < 1 {
            return Ok(CompiledPolicy::default());
        }
        let config = crate::config_resolver::resolve_ledger_config(
            &state.snapshot,
            state.novelty.as_ref(),
            t,
        )
        .await?;
        Ok(CompiledPolicy::compile(config.as_ref()))
    }

    /// Build the next expiration batch against `head_t`: each due occurrence
    /// verified still current, its stored terms retracted with the TTL
    /// annotation that governed it, plus superseded annotations to retire.
    async fn build_batch(
        &self,
        schedule: &mut Schedule,
        ctx: &Ctx<'_>,
        head_t: i64,
        now_ms: i64,
        limits: &RetentionLimits,
    ) -> Result<RetentionBatch> {
        let mut retractions: Vec<Flake> = Vec::new();
        let mut expired = 0;
        for (key, entry) in schedule.due(now_ms, limits.max_expire_per_batch) {
            let Some(g_id) = ctx.g_id(&key.g) else {
                schedule.remove(&key);
                continue;
            };
            let current = ctx.current(g_id, &key, head_t).await?;
            if current.is_empty() {
                schedule.remove(&key);
                continue;
            }
            if shared(ctx, g_id, &key, head_t).await? {
                schedule.note_problem(
                    "a due value is shared by several annotations or relationships; it was \
                     left in place"
                        .to_string(),
                );
                schedule.postpone(&key, now_ms + limits.conflict_backoff_ms);
                continue;
            }
            retractions.extend(current.into_iter().map(|f| Flake { op: false, ..f }));
            if let Some(reifier) = &entry.reifier {
                retractions.extend(retire_annotation(ctx, &key.g, g_id, reifier, head_t).await?);
            }
            expired += 1;
            if retractions.len() >= limits.max_flakes_per_batch {
                break;
            }
        }
        let mut retired = 0;
        let pending: Vec<Retirement> = schedule
            .retire
            .iter()
            .take(limits.max_expire_per_batch)
            .cloned()
            .collect();
        for retirement in pending {
            if retractions.len() >= limits.max_flakes_per_batch {
                break;
            }
            let Some(g_id) = ctx.g_id(&retirement.g) else {
                schedule.retire.remove(&retirement);
                continue;
            };
            let flakes =
                retire_annotation(ctx, &retirement.g, g_id, &retirement.reifier, head_t).await?;
            if flakes.is_empty() {
                schedule.retire.remove(&retirement);
                continue;
            }
            retractions.extend(flakes);
            retired += 1;
        }
        retractions
            .sort_by(|a, b| (&a.g, &a.s, &a.p, &a.dt, &a.o).cmp(&(&b.g, &b.s, &b.p, &b.dt, &b.o)));
        retractions.dedup_by(|a, b| FactKey::of(a) == FactKey::of(b));
        Ok(RetentionBatch {
            expected_t: head_t,
            retractions,
            expired,
            retired,
            control: Vec::new(),
        })
    }
}

/// A cheap summary that changes whenever the schedule does.
/// What tells whether a pass changed `schedule`, read without its entries.
fn schedule_fingerprint(schedule: &Schedule) -> (i64, usize, Option<i64>, usize, usize, u64) {
    (
        schedule.watermark_t,
        schedule.len(),
        schedule.next_due_ms(),
        schedule.retire.len(),
        schedule.problems.len(),
        schedule.revision(),
    )
}

/// The persisted schedule, or a fresh one from the activation boundary when
/// there is none or it cannot be read (it is rebuilt from the commits).
async fn load_schedule(store: &dyn ContentStore, registration: &Registration) -> Schedule {
    let fresh = || Schedule::starting_after(registration.active_since_t - 1);
    let Some(blob) = registration.schedule.as_deref() else {
        return fresh();
    };
    let loaded = async {
        let cid: ContentId = blob
            .parse()
            .map_err(|e| format!("schedule id {blob}: {e:?}"))?;
        let bytes = store.get(&cid).await.map_err(|e| e.to_string())?;
        Schedule::decode(&bytes)
    }
    .await;
    match loaded {
        Ok(schedule) => schedule,
        Err(e) => {
            tracing::warn!(error = %e, "retention schedule unreadable; rebuilding from the activation boundary");
            let mut schedule = fresh();
            schedule.note_problem(format!("schedule rebuilt: {e}"));
            schedule
        }
    }
}

/// Retention pass entry used by tests and embedders: activates a branch
/// whose commits carry retention input (scanning its whole chain), then runs
/// one pass committing directly.
impl Fluree {
    pub(crate) async fn probe_and_activate(
        &self,
        ledger_id: &str,
        after_t: i64,
    ) -> Result<Option<i64>> {
        let ns = self.publisher()?;
        let Some(record) = ns.lookup(ledger_id).await? else {
            return Err(ApiError::NotFound(ledger_id.to_string()));
        };
        let Some(head) = record.commit_head_id.clone() else {
            return Ok(None);
        };
        let store: Arc<dyn ContentStore> = self
            .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
            .await?;
        let found = probe_activation(store.as_ref(), &head, after_t).await?;
        if let Some(since) = found {
            registry::activate(ns, self.registry_objects(), ledger_id, since).await?;
        }
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_lifetimes_start_at_the_commit_time() {
        assert_eq!(Lifetime::Seconds(60).due(Some(1_000)), Ok(Some(61_000)));
        assert_eq!(Lifetime::Seconds(0).due(Some(1_000)), Ok(None));
        // No commit time: a relative lifetime has nothing to start from, and
        // is not silently given the time of replay.
        assert_eq!(Lifetime::Seconds(60).due(None), Err(()));
        assert_eq!(Lifetime::At(5).due(None), Ok(Some(5)));
        assert_eq!(
            Lifetime::Seconds(u64::MAX).due(Some(1)),
            Ok(Some(i64::MAX)),
            "an overflowing deadline never comes"
        );
    }

    #[test]
    fn txn_meta_defaults_read_their_canonical_forms() {
        let ttl = TxnMetaEntry::new(FLUREE_DB, db::TTL, TxnMetaValue::Long(30));
        assert_eq!(Lifetime::of_txn_meta(&ttl), Some(Lifetime::Seconds(30)));
        let at = TxnMetaEntry::new(
            FLUREE_DB,
            db::EXPIRES_AT,
            TxnMetaValue::TypedLiteral {
                value: "2030-01-01T00:00:00Z".into(),
                dt_ns: fluree_vocab::namespaces::XSD,
                dt_name: "dateTime".into(),
            },
        );
        assert_eq!(
            Lifetime::of_txn_meta(&at),
            Some(Lifetime::At(1_893_456_000_000))
        );
        assert!(carries_retention_input(&[ttl]));
        assert!(!carries_retention_input(&[TxnMetaEntry::new(
            FLUREE_DB,
            db::MESSAGE,
            TxnMetaValue::String("hi".into())
        )]));
    }
}
