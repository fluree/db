//! Synthesizes the RDF 1.2 link record for every `f:reifies*` bundle the
//! resolver sees, so rebuilds carry the same `_:r rdf:reifies <term>` flakes
//! bulk import writes.
//!
//! A bundle's three required slots arrive as consecutive records about the
//! reifier subject (the writer emits them together, and a SPOT-sorted commit
//! keeps one subject's records adjacent). The assembler keys a pending bundle
//! on `(g_id, reifier, t, op)`, fills the subject, predicate and object slots
//! as their records pass, and on completion appends the base edge as a
//! pseudo-record to the chunk's term table and a link record, whose `o_key`
//! is that entry's ordinal, to the chunk's records. The build remaps the
//! entry to global ids and interns it, replacing the ordinal with the handle.
//!
//! Disabled unless a build path opts in: a path that has not learned to
//! resolve the ordinals must not see link records.

use super::global_dict::PredicateDict;
use super::resolver::RebuildChunk;
use fluree_db_binary_index::format::run_record::{RunRecord, LIST_INDEX_NONE};
use fluree_db_core::commit::codec::raw_reader::{RawObject, RawOp};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKind;
use fluree_vocab::{db, fluree};
use std::collections::HashMap;

/// Per-build state for link synthesis.
#[derive(Debug, Default)]
pub struct LinkSynth {
    enabled: bool,
    /// `[f:reifiesSubject, f:reifiesPredicate, f:reifiesObject]` predicate ids,
    /// each looked up (never allocated) as soon as the dictionary holds it.
    /// Resolved per slot because the first bundle's records enter the
    /// dictionary one at a time, and each must already match its slot.
    slots: [Option<u32>; 3],
    /// Predicate-dictionary length at the last slot lookup, so the lookup
    /// repeats only when new predicates appeared.
    slots_checked_at: u32,
    rdf_reifies: Option<u32>,
    triple_term_dt: Option<u16>,
    pending: Option<Pending>,
    /// Link records emitted so far.
    pub links_emitted: u64,
}

#[derive(Debug)]
struct Pending {
    g_id: u16,
    ann: u64,
    t: u32,
    op: u8,
    s: Option<u64>,
    p: Option<u32>,
    /// `(o_kind, o_key, dt, lang_id)` of the base edge's object.
    o: Option<(u8, u64, u16, u16)>,
}

impl LinkSynth {
    /// A disabled assembler; see [`Self::enable`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Turn synthesis on. Only a build path that resolves term ordinals may
    /// do this.
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Refresh the reserved-slot predicate ids when the dictionary grew.
    fn refresh_slots(&mut self, predicates: &PredicateDict) {
        if self.slots.iter().all(Option::is_some) || predicates.len() == self.slots_checked_at {
            return;
        }
        self.slots_checked_at = predicates.len();
        let names = [
            db::REIFIES_SUBJECT,
            db::REIFIES_PREDICATE,
            db::REIFIES_OBJECT,
        ];
        for (slot, name) in self.slots.iter_mut().zip(names) {
            if slot.is_none() {
                *slot = predicates.get(&format!("{}{}", fluree::DB, name));
            }
        }
    }

    /// Feed one resolved record (with its raw op, for the predicate slot's
    /// IRI). Must be called for every record in commit order.
    pub fn observe(
        &mut self,
        raw: &RawOp<'_>,
        record: &RunRecord,
        predicates: &mut PredicateDict,
        datatypes: &mut PredicateDict,
        ns_prefixes: &HashMap<u16, String>,
        chunk: &mut RebuildChunk,
    ) {
        if !self.enabled {
            return;
        }
        self.refresh_slots(predicates);
        let Some(slot) = self.slots.iter().position(|s| *s == Some(record.p_id)) else {
            return;
        };

        let key = (record.g_id, record.s_id.as_u64(), record.t, record.op);
        let same = self
            .pending
            .as_ref()
            .is_some_and(|p| (p.g_id, p.ann, p.t, p.op) == key);
        if !same {
            self.flush(chunk, predicates, datatypes);
            self.pending = Some(Pending {
                g_id: record.g_id,
                ann: record.s_id.as_u64(),
                t: record.t,
                op: record.op,
                s: None,
                p: None,
                o: None,
            });
        }
        let pending = self.pending.as_mut().expect("pending bundle set above");
        match slot {
            0 => {
                if ObjKind::from_u8(record.o_kind) == ObjKind::REF_ID {
                    pending.s = Some(record.o_key);
                }
            }
            1 => {
                if let RawObject::Ref { ns_code, name } = raw.o {
                    let prefix = ns_prefixes
                        .get(&ns_code)
                        .map(std::string::String::as_str)
                        .unwrap_or("");
                    pending.p = Some(predicates.get_or_insert_parts(prefix, name));
                }
            }
            _ => {
                // Resolved under `f:reifiesObject`, which matches the base
                // edge's encoding for every kind except the per-predicate
                // arenas; those bundles are left to the bundle path.
                let kind = ObjKind::from_u8(record.o_kind);
                if kind != ObjKind::NUM_BIG && kind != ObjKind::VECTOR_ID {
                    pending.o = Some((record.o_kind, record.o_key, record.dt, record.lang_id));
                }
            }
        }
    }

    /// Emit the pending bundle if complete. Called on a key change and at
    /// the end of each commit.
    pub fn flush(
        &mut self,
        chunk: &mut RebuildChunk,
        predicates: &mut PredicateDict,
        datatypes: &mut PredicateDict,
    ) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        let (Some(s), Some(p), Some((o_kind, o_key, dt, lang_id))) =
            (pending.s, pending.p, pending.o)
        else {
            return;
        };
        let link_p = *self
            .rdf_reifies
            .get_or_insert_with(|| predicates.get_or_insert(fluree_vocab::rdf::REIFIES));
        let link_dt = match self.triple_term_dt {
            Some(d) => d,
            None => {
                let raw = datatypes.get_or_insert(fluree::TRIPLE_TERM);
                let Ok(d) = u16::try_from(raw) else {
                    tracing::warn!(
                        dt_id = raw,
                        "f:tripleTerm datatype id exceeds u16; link skipped"
                    );
                    return;
                };
                self.triple_term_dt = Some(d);
                d
            }
        };
        let ordinal = chunk.terms.len() as u64;
        chunk.terms.push(RunRecord {
            g_id: pending.g_id,
            s_id: SubjectId::from_u64(s),
            p_id: p,
            dt,
            o_kind,
            op: 1,
            o_key,
            t: pending.t,
            lang_id,
            i: LIST_INDEX_NONE,
        });
        chunk.records.push(RunRecord {
            g_id: pending.g_id,
            s_id: SubjectId::from_u64(pending.ann),
            p_id: link_p,
            dt: link_dt,
            o_kind: ObjKind::TRIPLE_TERM.as_u8(),
            op: pending.op,
            o_key: ordinal,
            t: pending.t,
            lang_id: 0,
            i: LIST_INDEX_NONE,
        });
        chunk.flake_count += 1;
        self.links_emitted += 1;
    }
}
