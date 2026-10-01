//! Reification links derived from attachment ops.
//!
//! Commits carry an annotation's attachment as `f:reifies*` slot flakes; the
//! RDF 1.2 link `_:r rdf:reifies <<( s p o )>>` is derived from them. The index
//! build derives it from commit blobs, and novelty derives it for commits the
//! index has not covered yet. Both replay a reifier's slot ops in `t` order,
//! retracts before asserts within a `t`, and emit a link retract and assert at
//! each `t` where the attachment changes from or to a complete triple; this
//! module is the novelty side of that rule.

use crate::flake::Flake;
use crate::ids::GraphId;
use crate::namespaces::{
    is_reifies_object, is_reifies_predicate, is_reifies_subject, rdf_reifies_sid,
    triple_term_datatype_sid,
};
use crate::sid::Sid;
use crate::value::{FlakeValue, TripleTermValue};
use std::fmt::Debug;
use std::io;

/// The three slots that name a reifier's triple, as an index or novelty holds
/// them for one reifier in one graph.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AttachmentSlots {
    pub subject: Option<Sid>,
    pub predicate: Option<Sid>,
    /// The base object with its datatype and language tag.
    pub object: Option<(FlakeValue, Sid, Option<String>)>,
}

/// One slot value of an attachment op.
#[derive(Clone, Debug, PartialEq)]
enum SlotValue {
    Subject(Sid),
    Predicate(Sid),
    Object(FlakeValue, Sid, Option<String>),
}

impl SlotValue {
    /// The slot a flake writes, or `None` for anything but the subject,
    /// predicate and object slots (a non-reference subject or predicate value
    /// included, which the index build ignores too).
    fn of(flake: &Flake) -> Option<Self> {
        if is_reifies_subject(&flake.p) {
            match &flake.o {
                FlakeValue::Ref(s) => Some(SlotValue::Subject(s.clone())),
                _ => None,
            }
        } else if is_reifies_predicate(&flake.p) {
            match &flake.o {
                FlakeValue::Ref(p) => Some(SlotValue::Predicate(p.clone())),
                _ => None,
            }
        } else if is_reifies_object(&flake.p) {
            let lang = flake.m.as_ref().and_then(|m| m.lang.clone());
            Some(SlotValue::Object(flake.o.clone(), flake.dt.clone(), lang))
        } else {
            None
        }
    }

    fn rank(&self) -> u8 {
        match self {
            SlotValue::Subject(_) => 0,
            SlotValue::Predicate(_) => 1,
            SlotValue::Object(..) => 2,
        }
    }
}

/// True for the flakes that write an attachment slot.
#[inline]
pub fn is_attachment_slot(p: &Sid) -> bool {
    is_reifies_subject(p) || is_reifies_predicate(p) || is_reifies_object(p)
}

/// Whether a term with this object can be interned. Objects keyed by a
/// per-(graph, predicate) arena have no graph-independent identity, so the
/// index build gives their attachments no link, and novelty must not either.
#[inline]
pub fn term_object_is_internable(o: &FlakeValue) -> bool {
    match o {
        FlakeValue::Decimal(_) | FlakeValue::Vector(_) => false,
        FlakeValue::BigInt(b) => num_traits::ToPrimitive::to_i64(b.as_ref()).is_some(),
        _ => true,
    }
}

impl AttachmentSlots {
    /// The triple term a complete attachment names.
    pub fn term(&self) -> Option<TripleTermValue> {
        let (Some(s), Some(p), Some((o, dt, lang))) =
            (&self.subject, &self.predicate, &self.object)
        else {
            return None;
        };
        if !term_object_is_internable(o) {
            return None;
        }
        Some(TripleTermValue {
            s: s.clone(),
            p: p.clone(),
            o: o.clone(),
            dt: dt.clone(),
            lang: lang.clone(),
        })
    }

    /// Fold an index or novelty row into the slots; the last row per slot
    /// wins, as in the index build's base lookup.
    pub fn observe(&mut self, flake: &Flake) {
        if let Some(value) = SlotValue::of(flake) {
            self.set(value);
        }
    }

    fn set(&mut self, value: SlotValue) {
        match value {
            SlotValue::Subject(s) => self.subject = Some(s),
            SlotValue::Predicate(p) => self.predicate = Some(p),
            SlotValue::Object(o, dt, lang) => self.object = Some((o, dt, lang)),
        }
    }

    /// A retract clears its slot only when the slot holds the retracted value.
    fn clear(&mut self, value: &SlotValue) {
        match value {
            SlotValue::Subject(s) if self.subject.as_ref() == Some(s) => self.subject = None,
            SlotValue::Predicate(p) if self.predicate.as_ref() == Some(p) => self.predicate = None,
            SlotValue::Object(o, dt, lang)
                if self
                    .object
                    .as_ref()
                    .is_some_and(|(so, sdt, slang)| so == o && sdt == dt && slang == lang) =>
            {
                self.object = None;
            }
            _ => {}
        }
    }
}

/// The attachments an index holds, read when novelty first meets a reifier.
pub trait AttachmentBase: Send + Sync + Debug {
    /// The slots of each reifier in `g_id`, in input order. A reifier the
    /// index does not hold has empty slots.
    fn attachments(&self, g_id: GraphId, reifiers: &[Sid]) -> io::Result<Vec<AttachmentSlots>>;
}

/// Replay one reifier's slot ops from `state` and append its link flakes to
/// `out`. `ops` may arrive in any order; ops with `t <= emit_after` only
/// advance the state (they are history whose links already exist).
pub fn replay_links(
    state: &mut AttachmentSlots,
    ops: &mut [&Flake],
    emit_after: i64,
    out: &mut Vec<Flake>,
) {
    let mut keyed: Vec<(i64, bool, u8, SlotValue, &Flake)> = ops
        .iter()
        .filter_map(|f| SlotValue::of(f).map(|v| (f.t, f.op, v.rank(), v, *f)))
        .collect();
    // Within one `t`, retracts apply before asserts so a re-pointed slot
    // passes through its old value on the way to the new one.
    keyed.sort_by_key(|k| (k.0, k.1, k.2));
    let mut i = 0;
    while i < keyed.len() {
        let t = keyed[i].0;
        let before = state.term();
        let anchor = keyed[i].4;
        while i < keyed.len() && keyed[i].0 == t {
            let (_, op, _, value, _) = &keyed[i];
            if *op {
                state.set(value.clone());
            } else {
                state.clear(value);
            }
            i += 1;
        }
        if t <= emit_after {
            continue;
        }
        let after = state.term();
        if before != after {
            if let Some(old) = before {
                out.push(link_flake(anchor, old, t, false));
            }
            if let Some(new) = after {
                out.push(link_flake(anchor, new, t, true));
            }
        }
    }
}

/// The link flake for `term`, in the graph and on the reifier of `slot`.
fn link_flake(slot: &Flake, term: TripleTermValue, t: i64, op: bool) -> Flake {
    let o = FlakeValue::TripleTerm(Box::new(term));
    let dt = triple_term_datatype_sid().clone();
    let p = rdf_reifies_sid().clone();
    match &slot.g {
        Some(g) => Flake::new_in_graph(g.clone(), slot.s.clone(), p, o, dt, t, op, None),
        None => Flake::new(slot.s.clone(), p, o, dt, t, op, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flake::FlakeMeta;
    use crate::namespaces::{reifies_object_sid, reifies_predicate_sid, reifies_subject_sid};

    fn sid(name: &str) -> Sid {
        Sid::new(100, name)
    }

    fn slot(p: &Sid, o: FlakeValue, t: i64, op: bool) -> Flake {
        let dt = if matches!(o, FlakeValue::Ref(_)) {
            crate::edge::id_datatype_sid()
        } else {
            crate::edge::xsd_string_datatype_sid()
        };
        Flake::new(sid("r"), p.clone(), o, dt, t, op, None)
    }

    fn bundle(s: &str, p: &str, o: &str, t: i64, op: bool) -> Vec<Flake> {
        vec![
            slot(reifies_subject_sid(), FlakeValue::Ref(sid(s)), t, op),
            slot(reifies_predicate_sid(), FlakeValue::Ref(sid(p)), t, op),
            slot(reifies_object_sid(), FlakeValue::String(o.into()), t, op),
        ]
    }

    fn links(
        state: &mut AttachmentSlots,
        flakes: &[Flake],
        emit_after: i64,
    ) -> Vec<(i64, bool, String)> {
        let mut ops: Vec<&Flake> = flakes.iter().collect();
        let mut out = Vec::new();
        replay_links(state, &mut ops, emit_after, &mut out);
        out.iter()
            .map(|f| match &f.o {
                FlakeValue::TripleTerm(t) => {
                    (f.t, f.op, format!("{} {} {:?}", t.s.name, t.p.name, t.o))
                }
                other => panic!("link object {other:?}"),
            })
            .collect()
    }

    #[test]
    fn a_partial_repoint_retracts_the_old_term_and_asserts_the_new() {
        let mut state = AttachmentSlots::default();
        let mut flakes = bundle("a", "p", "x", 1, true);
        flakes.push(slot(
            reifies_object_sid(),
            FlakeValue::String("x".into()),
            2,
            false,
        ));
        flakes.push(slot(
            reifies_object_sid(),
            FlakeValue::String("y".into()),
            2,
            true,
        ));
        assert_eq!(
            links(&mut state, &flakes, 0),
            vec![
                (1, true, "a p String(\"x\")".to_string()),
                (2, false, "a p String(\"x\")".to_string()),
                (2, true, "a p String(\"y\")".to_string()),
            ]
        );
    }

    #[test]
    fn history_at_or_before_the_watermark_only_seeds_the_state() {
        let mut state = AttachmentSlots::default();
        let mut flakes = bundle("a", "p", "x", 1, true);
        flakes.push(slot(
            reifies_subject_sid(),
            FlakeValue::Ref(sid("a")),
            3,
            false,
        ));
        assert_eq!(
            links(&mut state, &flakes, 1),
            vec![(3, false, "a p String(\"x\")".to_string())]
        );
    }

    #[test]
    fn a_base_attachment_is_retracted_by_a_full_retract() {
        let mut state = AttachmentSlots {
            subject: Some(sid("a")),
            predicate: Some(sid("p")),
            object: Some((
                FlakeValue::String("x".into()),
                crate::edge::xsd_string_datatype_sid(),
                None,
            )),
        };
        let flakes = bundle("a", "p", "x", 5, false);
        assert_eq!(
            links(&mut state, &flakes, 4),
            vec![(5, false, "a p String(\"x\")".to_string())]
        );
        assert_eq!(state, AttachmentSlots::default());
    }

    #[test]
    fn a_retract_of_a_value_the_slot_does_not_hold_changes_nothing() {
        let mut state = AttachmentSlots::default();
        let mut flakes = bundle("a", "p", "x", 1, true);
        flakes.push(slot(
            reifies_object_sid(),
            FlakeValue::String("z".into()),
            2,
            false,
        ));
        assert_eq!(
            links(&mut state, &flakes, 0),
            vec![(1, true, "a p String(\"x\")".to_string())]
        );
    }

    #[test]
    fn language_tags_distinguish_object_slots() {
        let mut state = AttachmentSlots::default();
        let mut flakes = bundle("a", "p", "x", 1, true);
        let mut tagged = slot(
            reifies_object_sid(),
            FlakeValue::String("x".into()),
            2,
            false,
        );
        tagged.m = Some(FlakeMeta::with_lang("en"));
        flakes.push(tagged);
        assert_eq!(links(&mut state, &flakes, 0).len(), 1);
    }

    #[test]
    fn arena_kind_objects_get_no_term() {
        let state = AttachmentSlots {
            subject: Some(sid("a")),
            predicate: Some(sid("p")),
            object: Some((
                FlakeValue::Decimal(Box::new("1.5".parse().unwrap())),
                Sid::new(2, "decimal"),
                None,
            )),
        };
        assert!(state.term().is_none());
    }
}
