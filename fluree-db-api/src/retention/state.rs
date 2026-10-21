//! The derived expiry schedule of one ledger branch.
//!
//! Rebuilt from committed inputs, never the source of truth: a schedule plus
//! the commit watermark it covers. Each entry is one assertion occurrence
//! (the fact, asserted at `occurrence_t` and never retracted since) with the
//! time it becomes due and the instruction that set it. Persisted as one blob
//! whose keys use the commit codec's exact flake encoding.

use std::collections::{BTreeSet, HashMap};

use fluree_db_core::commit::codec::{decode_flake_batch, encode_flake_batch};
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
use serde::{Deserialize, Serialize};

/// One fact: graph, subject, predicate and the complete object term.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct FactKey {
    pub(crate) g: Option<Sid>,
    pub(crate) s: Sid,
    pub(crate) p: Sid,
    pub(crate) o: FlakeValue,
    pub(crate) dt: Sid,
    pub(crate) lang: Option<String>,
}

impl FactKey {
    pub(crate) fn of(flake: &Flake) -> Self {
        Self {
            g: flake.g.clone(),
            s: flake.s.clone(),
            p: flake.p.clone(),
            o: flake.o.clone(),
            dt: flake.dt.clone(),
            lang: flake.m.as_ref().and_then(|m| m.lang.clone()),
        }
    }

    pub(crate) fn of_triple_term(g: Option<Sid>, tt: &fluree_db_core::TripleTermValue) -> Self {
        Self {
            g,
            s: tt.s.clone(),
            p: tt.p.clone(),
            o: tt.o.clone(),
            dt: tt.dt.clone(),
            lang: tt.lang.clone(),
        }
    }

    /// The fact as a flake at `t` with `op`.
    pub(crate) fn flake(&self, t: i64, op: bool) -> Flake {
        Flake {
            g: self.g.clone(),
            s: self.s.clone(),
            p: self.p.clone(),
            o: self.o.clone(),
            dt: self.dt.clone(),
            t,
            op,
            m: self.lang.clone().map(FlakeMeta::with_lang),
        }
    }

    /// Whether `flake` states this fact (ignoring `t` and `op`).
    pub(crate) fn matches(&self, flake: &Flake) -> bool {
        flake.g == self.g
            && flake.s == self.s
            && flake.p == self.p
            && flake.dt == self.dt
            && flake.o == self.o
            && flake.m.as_ref().and_then(|m| m.lang.as_deref()) == self.lang.as_deref()
    }
}

/// Where an entry's lifetime came from, highest precedence first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum Source {
    /// A per-value instruction on the value's annotation.
    Value,
    /// The transaction's TTL default (txn-meta).
    Transaction,
    /// The predicate's `f:retentionDefaults` entry.
    Property,
}

/// A scheduled occurrence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    /// Epoch milliseconds the occurrence becomes due.
    pub(crate) due_ms: i64,
    /// Commit `t` that asserted the occurrence; `0` when it was asserted
    /// before the ledger's activation boundary (and later given a TTL).
    pub(crate) occurrence_t: i64,
    /// Position of the assertion in its commit's operations.
    pub(crate) occurrence_slot: u32,
    pub(crate) source: Source,
    /// Commit `t` of the instruction in force (the schedule generation).
    pub(crate) generation_t: i64,
    /// The annotation carrying the instruction in force, for a
    /// [`Source::Value`] entry.
    pub(crate) reifier: Option<Sid>,
}

/// An annotation whose TTL instruction no longer governs anything: its
/// instruction is retracted in a maintenance commit.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Retirement {
    pub(crate) g: Option<Sid>,
    pub(crate) reifier: Sid,
}

/// The schedule and the commit watermark it covers.
#[derive(Clone, Debug, Default)]
pub(crate) struct Schedule {
    /// Commits with `t <= watermark_t` have been discovered.
    pub(crate) watermark_t: i64,
    entries: HashMap<FactKey, Entry>,
    by_due: BTreeSet<(i64, FactKey)>,
    pub(crate) retire: BTreeSet<Retirement>,
    /// Discovery problems that need an operator (missing commit time, an
    /// ambiguous instruction), newest last, capped.
    pub(crate) problems: Vec<String>,
}

const MAX_PROBLEMS: usize = 32;

impl Schedule {
    /// An empty schedule whose discovery starts after commit `watermark_t`.
    pub(crate) fn starting_after(watermark_t: i64) -> Self {
        Self {
            watermark_t,
            ..Default::default()
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn get(&self, key: &FactKey) -> Option<&Entry> {
        self.entries.get(key)
    }

    pub(crate) fn contains(&self, key: &FactKey) -> bool {
        self.entries.contains_key(key)
    }

    pub(crate) fn insert(&mut self, key: FactKey, entry: Entry) {
        if let Some(old) = self.entries.remove(&key) {
            self.by_due.remove(&(old.due_ms, key.clone()));
        }
        self.by_due.insert((entry.due_ms, key.clone()));
        self.entries.insert(key, entry);
    }

    pub(crate) fn remove(&mut self, key: &FactKey) -> Option<Entry> {
        let entry = self.entries.remove(key)?;
        self.by_due.remove(&(entry.due_ms, key.clone()));
        Some(entry)
    }

    /// Entries due at `now_ms`, earliest first, at most `limit`.
    pub(crate) fn due(&self, now_ms: i64, limit: usize) -> Vec<(FactKey, Entry)> {
        self.by_due
            .iter()
            .take_while(|(due, _)| *due <= now_ms)
            .take(limit)
            .map(|(_, key)| (key.clone(), self.entries[key].clone()))
            .collect()
    }

    pub(crate) fn next_due_ms(&self) -> Option<i64> {
        self.by_due.iter().next().map(|(due, _)| *due)
    }

    pub(crate) fn overdue(&self, now_ms: i64) -> usize {
        self.by_due
            .iter()
            .take_while(|(due, _)| *due <= now_ms)
            .count()
    }

    /// Postpone an entry that cannot be processed now.
    pub(crate) fn postpone(&mut self, key: &FactKey, until_ms: i64) {
        if let Some(mut entry) = self.entries.get(key).cloned() {
            entry.due_ms = until_ms.max(entry.due_ms);
            self.insert(key.clone(), entry);
        }
    }

    pub(crate) fn note_problem(&mut self, problem: String) {
        if self.problems.last() == Some(&problem) {
            return;
        }
        self.problems.push(problem);
        if self.problems.len() > MAX_PROBLEMS {
            self.problems.remove(0);
        }
    }

    /// Every entry, for tests and status.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&FactKey, &Entry)> {
        self.entries.iter()
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, String> {
        let mut keys: Vec<(&FactKey, &Entry)> = self.entries.iter().collect();
        keys.sort_by(|a, b| a.0.cmp(b.0));
        let flakes: Vec<Flake> = keys
            .iter()
            .map(|(key, entry)| key.flake(entry.occurrence_t, true))
            .collect();
        let header = Header {
            version: 1,
            watermark_t: self.watermark_t,
            entries: keys
                .iter()
                .map(|(_, e)| EntryForm {
                    due_ms: e.due_ms,
                    slot: e.occurrence_slot,
                    source: e.source,
                    generation_t: e.generation_t,
                    reifier: e.reifier.as_ref().map(SidForm::of),
                })
                .collect(),
            retire: self
                .retire
                .iter()
                .map(|r| RetireForm {
                    g: r.g.as_ref().map(SidForm::of),
                    reifier: SidForm::of(&r.reifier),
                })
                .collect(),
            problems: self.problems.clone(),
        };
        let header = serde_json::to_vec(&header).map_err(|e| e.to_string())?;
        let flakes = encode_flake_batch(&flakes).map_err(|e| e.to_string())?;
        let mut out = Vec::with_capacity(header.len() + flakes.len() + 12);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&(header.len() as u64).to_le_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(&flakes);
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        let bad = || "not a retention schedule".to_string();
        let rest = bytes.strip_prefix(MAGIC.as_slice()).ok_or_else(bad)?;
        if rest.len() < 8 {
            return Err(bad());
        }
        let len = u64::from_le_bytes(rest[..8].try_into().map_err(|_| bad())?) as usize;
        let rest = &rest[8..];
        if rest.len() < len {
            return Err(bad());
        }
        let header: Header = serde_json::from_slice(&rest[..len]).map_err(|e| e.to_string())?;
        if header.version != 1 {
            return Err(format!(
                "unsupported retention schedule version {}",
                header.version
            ));
        }
        let flakes = decode_flake_batch(&rest[len..]).map_err(|e| e.to_string())?;
        if flakes.len() != header.entries.len() {
            return Err(bad());
        }
        let mut schedule = Schedule {
            watermark_t: header.watermark_t,
            problems: header.problems,
            ..Default::default()
        };
        for (flake, form) in flakes.iter().zip(header.entries) {
            schedule.insert(
                FactKey::of(flake),
                Entry {
                    due_ms: form.due_ms,
                    occurrence_t: flake.t,
                    occurrence_slot: form.slot,
                    source: form.source,
                    generation_t: form.generation_t,
                    reifier: form.reifier.map(SidForm::sid),
                },
            );
        }
        schedule.retire = header
            .retire
            .into_iter()
            .map(|r| Retirement {
                g: r.g.map(SidForm::sid),
                reifier: r.reifier.sid(),
            })
            .collect();
        Ok(schedule)
    }
}

const MAGIC: &[u8; 4] = b"FRS1";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Header {
    version: u32,
    watermark_t: i64,
    entries: Vec<EntryForm>,
    retire: Vec<RetireForm>,
    problems: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EntryForm {
    due_ms: i64,
    slot: u32,
    source: Source,
    generation_t: i64,
    reifier: Option<SidForm>,
}

#[derive(Serialize, Deserialize)]
struct RetireForm {
    g: Option<SidForm>,
    reifier: SidForm,
}

#[derive(Serialize, Deserialize)]
struct SidForm(u16, String);

impl SidForm {
    fn of(sid: &Sid) -> Self {
        Self(sid.namespace_code, sid.name.to_string())
    }
    fn sid(self) -> Sid {
        Sid::new(self.0, self.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(o: FlakeValue, dt: &str) -> FactKey {
        FactKey {
            g: None,
            s: Sid::new(100, "s"),
            p: Sid::new(100, "p"),
            o,
            dt: Sid::new(2, dt),
            lang: None,
        }
    }

    fn entry(due_ms: i64) -> Entry {
        Entry {
            due_ms,
            occurrence_t: 3,
            occurrence_slot: 1,
            source: Source::Value,
            generation_t: 4,
            reifier: Some(Sid::new(10, "fdb-1")),
        }
    }

    #[test]
    fn due_order_and_round_trip() {
        let mut s = Schedule {
            watermark_t: 7,
            ..Default::default()
        };
        s.insert(key(FlakeValue::Long(1), "long"), entry(30));
        s.insert(key(FlakeValue::Double(1.0), "double"), entry(10));
        s.insert(key(FlakeValue::String("x".into()), "string"), entry(20));
        s.retire.insert(Retirement {
            g: None,
            reifier: Sid::new(10, "old"),
        });
        assert_eq!(
            s.due(25, 10)
                .iter()
                .map(|(_, e)| e.due_ms)
                .collect::<Vec<_>>(),
            vec![10, 20]
        );
        assert_eq!(s.next_due_ms(), Some(10));
        let back = Schedule::decode(&s.encode().unwrap()).unwrap();
        assert_eq!(back.watermark_t, 7);
        assert_eq!(back.len(), 3);
        assert_eq!(
            back.get(&key(FlakeValue::Double(1.0), "double")),
            Some(&entry(10))
        );
        assert_eq!(back.retire, s.retire);
        assert_eq!(back.next_due_ms(), Some(10));
    }

    #[test]
    fn reinsert_moves_the_due_time() {
        let mut s = Schedule::default();
        let k = key(FlakeValue::Long(1), "long");
        s.insert(k.clone(), entry(10));
        s.insert(k.clone(), entry(50));
        assert!(s.due(20, 10).is_empty());
        assert_eq!(s.overdue(60), 1);
        s.remove(&k);
        assert_eq!(s.next_due_ms(), None);
    }
}
