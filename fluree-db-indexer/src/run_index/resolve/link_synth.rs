//! Synthesizes the RDF 1.2 link record, `_:r rdf:reifies <term>`, for every
//! reifier a rebuild or incremental build sees, so both carry the flakes
//! bulk import writes.
//!
//! A commit's `f:reifies*` ops are not a bundle. A re-point that changes one
//! slot writes only that slot's retract and assert (sync and upsert cancel
//! the unchanged slots), and a full re-point's six ops interleave under the
//! per-commit sort, where predicate and object precede `op`. So the resolver
//! only *collects* attachment ops ([`LinkSynth::observe`]), and the build
//! replays them per reifier once ids are global ([`replay_attachments`]):
//! the reifier's ops in `t` order, emitting a link retract and assert
//! whenever its attachment changes from or to a complete edge. An
//! incremental build seeds each reifier with the attachment the base index
//! holds for it; a rebuild replays the whole history from nothing.
//!
//! Disabled unless a build path opts in: a path that does not replay must
//! not collect.

use super::global_dict::PredicateDict;
use super::resolver::RebuildChunk;
use fluree_db_binary_index::format::run_record::{RunRecord, LIST_INDEX_NONE};
use fluree_db_core::commit::codec::raw_reader::{RawObject, RawOp};
use fluree_db_core::o_type::OType;
use fluree_db_core::o_type_registry::OTypeRegistry;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::triple_term::{lexical_term_object, TermKey};
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::{DatatypeDictId, FlakeValue};
use fluree_vocab::{db, fluree};
use std::collections::HashMap;
use std::io;

/// The base edge's object, as the resolver saw it or as the base index
/// stores it. The two compare through [`ObjectId::typed`]; on both sides an
/// arena kind's key is the string id of its canonical form.
#[derive(Debug, Clone, Copy)]
pub enum ObjectId {
    /// Kind, key, datatype and tag of a resolved op; the `o_type` needs the
    /// registry, which knows every custom datatype only after resolving.
    Raw {
        o_kind: u8,
        o_key: u64,
        dt: u16,
        lang_id: u16,
    },
    /// An index row's `o_type` and key.
    Typed { o_type: u16, o_key: u64 },
}

impl ObjectId {
    fn typed(self, registry: &OTypeRegistry) -> (u16, u64) {
        match self {
            ObjectId::Raw {
                o_kind,
                o_key,
                dt,
                lang_id,
            } => (
                registry
                    .resolve(
                        ObjKind::from_u8(o_kind),
                        DatatypeDictId::from_u16(dt),
                        lang_id,
                    )
                    .as_u16(),
                o_key,
            ),
            ObjectId::Typed { o_type, o_key } => (o_type, o_key),
        }
    }
}

/// One slot of an attachment.
#[derive(Debug, Clone, Copy)]
pub enum SlotValue {
    /// `f:reifiesSubject`: the base edge's subject id.
    Subject(u64),
    /// `f:reifiesPredicate`: the base edge's predicate id.
    Predicate(u32),
    /// `f:reifiesObject`.
    Object(ObjectId),
}

impl SlotValue {
    fn slot(self) -> usize {
        match self {
            SlotValue::Subject(_) => 0,
            SlotValue::Predicate(_) => 1,
            SlotValue::Object(_) => 2,
        }
    }

    fn same(self, other: SlotValue, registry: &OTypeRegistry) -> bool {
        match (self, other) {
            (SlotValue::Subject(a), SlotValue::Subject(b)) => a == b,
            (SlotValue::Predicate(a), SlotValue::Predicate(b)) => a == b,
            (SlotValue::Object(a), SlotValue::Object(b)) => a.typed(registry) == b.typed(registry),
            _ => false,
        }
    }
}

/// An `f:reifies*` op as the resolver saw it. Subject and string ids are
/// chunk-local until [`AttachmentOp::remap`]; predicate ids are global.
#[derive(Debug, Clone, Copy)]
pub struct AttachmentOp {
    pub g_id: u16,
    pub ann: u64,
    pub t: u32,
    pub op: u8,
    pub value: SlotValue,
}

impl AttachmentOp {
    /// Chunk-local subject and string ids → global, with the build's remap
    /// tables.
    pub fn remap(&mut self, s_remap: &[u64], str_remap: &[u32]) -> Result<(), String> {
        let subject = |local: u64| -> Result<u64, String> {
            s_remap
                .get(local as usize)
                .copied()
                .ok_or_else(|| format!("attachment subject remap miss: local {local}"))
        };
        self.ann = subject(self.ann)?;
        match &mut self.value {
            SlotValue::Subject(s) => *s = subject(*s)?,
            SlotValue::Predicate(_) | SlotValue::Object(ObjectId::Typed { .. }) => {}
            SlotValue::Object(ObjectId::Raw { o_kind, o_key, .. }) => {
                let kind = ObjKind::from_u8(*o_kind);
                if kind == ObjKind::REF_ID {
                    *o_key = subject(*o_key)?;
                } else if kind == ObjKind::LEX_ID || kind == ObjKind::JSON_ID || is_arena_kind(kind)
                {
                    let local = ObjKey::from_u64(*o_key).decode_u32_id() as usize;
                    let global = *str_remap
                        .get(local)
                        .ok_or_else(|| format!("attachment string remap miss: local {local}"))?;
                    *o_key = ObjKey::encode_u32_id(global).as_u64();
                }
            }
        }
        Ok(())
    }
}

/// The link flake's predicate and datatype ids.
#[derive(Debug, Clone, Copy)]
pub struct LinkIds {
    pub p_id: u32,
    pub dt: u16,
}

/// Per-build collector for attachment ops.
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
    link: Option<LinkIds>,
}

impl LinkSynth {
    /// A disabled collector; see [`Self::enable`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Turn collection on. Only a build path that replays may do this.
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// The reserved-slot predicate ids, in slot order.
    pub fn slots(&self) -> [Option<u32>; 3] {
        self.slots
    }

    /// The link's predicate and datatype ids, allocated on the first
    /// attachment op seen so they enter the dictionaries with the commits.
    pub fn link_ids(&self) -> Option<LinkIds> {
        self.link
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

    /// Record one resolved op if it is an attachment slot (with its raw op,
    /// for the predicate slot's IRI).
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
        let value = match slot {
            0 => {
                if ObjKind::from_u8(record.o_kind) != ObjKind::REF_ID {
                    return;
                }
                SlotValue::Subject(record.o_key)
            }
            1 => {
                let RawObject::Ref { ns_code, name } = raw.o else {
                    return;
                };
                let prefix = ns_prefixes
                    .get(&ns_code)
                    .map(std::string::String::as_str)
                    .unwrap_or("");
                SlotValue::Predicate(predicates.get_or_insert_parts(prefix, name))
            }
            _ => {
                let mut o_key = record.o_key;
                if is_arena_kind(ObjKind::from_u8(record.o_kind)) {
                    let Some((_, form)) = FlakeValue::try_from(raw.o.clone())
                        .ok()
                        .and_then(|o| lexical_term_object(&o))
                    else {
                        return;
                    };
                    o_key = ObjKey::encode_u32_id(chunk.strings.get_or_insert(form.as_bytes()))
                        .as_u64();
                }
                SlotValue::Object(ObjectId::Raw {
                    o_kind: record.o_kind,
                    o_key,
                    dt: record.dt,
                    lang_id: record.lang_id,
                })
            }
        };
        if self.link.is_none() {
            let p_id = predicates.get_or_insert(fluree_vocab::rdf::REIFIES);
            let raw_dt = datatypes.get_or_insert(fluree::TRIPLE_TERM);
            let Ok(dt) = u16::try_from(raw_dt) else {
                tracing::warn!(
                    dt_id = raw_dt,
                    "f:tripleTerm datatype id exceeds u16; links skipped"
                );
                return;
            };
            self.link = Some(LinkIds { p_id, dt });
        }
        chunk.attachments.push(AttachmentOp {
            g_id: record.g_id,
            ann: record.s_id.as_u64(),
            t: record.t,
            op: record.op,
            value,
        });
    }
}

/// The term a complete attachment names, or `None` while a slot is missing.
fn term_of(state: &[Option<SlotValue>; 3], registry: &OTypeRegistry) -> Option<TermKey> {
    let (
        Some(SlotValue::Subject(s_id)),
        Some(SlotValue::Predicate(p_id)),
        Some(SlotValue::Object(o)),
    ) = (state[0], state[1], state[2])
    else {
        return None;
    };
    let (o_type, o_key) = o.typed(registry);
    Some(TermKey {
        s_id,
        p_id,
        o_type: OType::from_u16(o_type),
        o_key,
    })
}

/// Kinds whose `o_key` is an arena handle scoped to a graph and predicate. An
/// attachment carries such an object as the string id of its canonical form
/// ([`lexical_term_object`]), the key its term holds.
fn is_arena_kind(kind: ObjKind) -> bool {
    kind == ObjKind::NUM_BIG || kind == ObjKind::VECTOR_ID
}

/// Replay every reifier's attachment ops (ids global) and hand its link
/// records to `sink`: at each `t` where the attachment changes from or to a
/// complete edge, a retract of the old term's link and an assert of the new
/// one. `prior` is the attachment the base index holds for `(g_id, reifier)`
/// before these ops; `handle_for` interns a term. Records reach the sink in
/// `(g_id, SPOT)` order, one reifier at a time, so a rebuild can stream
/// them to a sorted spool without holding them all. Returns the number of
/// link records emitted.
pub fn replay_attachments(
    ops: &mut [AttachmentOp],
    mut prior: impl FnMut(u16, u64) -> [Option<SlotValue>; 3],
    registry: &OTypeRegistry,
    link: LinkIds,
    handle_for: &mut dyn FnMut(TermKey) -> io::Result<u64>,
    sink: &mut dyn FnMut(RunRecord) -> io::Result<()>,
) -> io::Result<u64> {
    // Within one `t`, retracts apply before asserts so a re-pointed slot
    // passes through its old value on the way to the new one.
    ops.sort_by_key(|o| (o.g_id, o.ann, o.t, o.op, o.value.slot()));
    let mut emitted = 0u64;
    let mut group: Vec<RunRecord> = Vec::new();
    let mut i = 0;
    while i < ops.len() {
        let (g_id, ann) = (ops[i].g_id, ops[i].ann);
        let mut state = prior(g_id, ann);
        while i < ops.len() && ops[i].g_id == g_id && ops[i].ann == ann {
            let t = ops[i].t;
            let before = term_of(&state, registry);
            while i < ops.len() && ops[i].g_id == g_id && ops[i].ann == ann && ops[i].t == t {
                let op = ops[i];
                let slot = op.value.slot();
                if op.op == 0 {
                    if state[slot].is_some_and(|v| v.same(op.value, registry)) {
                        state[slot] = None;
                    }
                } else {
                    state[slot] = Some(op.value);
                }
                i += 1;
            }
            let after = term_of(&state, registry);
            if before != after {
                if let Some(old) = before {
                    group.push(link_record(g_id, ann, link, handle_for(old)?, t, 0));
                }
                if let Some(new) = after {
                    group.push(link_record(g_id, ann, link, handle_for(new)?, t, 1));
                }
            }
        }
        // One reifier's records share `g_id` and subject; sorting them puts
        // the stream as a whole in `(g_id, SPOT)` order.
        group.sort_unstable_by(fluree_db_binary_index::format::run_record::cmp_g_spot);
        emitted += group.len() as u64;
        for record in group.drain(..) {
            sink(record)?;
        }
    }
    Ok(emitted)
}

fn link_record(g_id: u16, ann: u64, link: LinkIds, handle: u64, t: u32, op: u8) -> RunRecord {
    RunRecord {
        g_id,
        s_id: SubjectId::from_u64(ann),
        p_id: link.p_id,
        dt: link.dt,
        o_kind: ObjKind::TRIPLE_TERM.as_u8(),
        op,
        o_key: handle,
        t,
        lang_id: 0,
        i: LIST_INDEX_NONE,
    }
}
