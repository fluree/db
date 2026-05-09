//! `EdgeKey` — a stable identifier for a base triple that has (or could
//! have) annotations attached to it.
//!
//! Annotations in Fluree reify a specific edge: the `(graph, subject,
//! predicate, object, datatype, language, list-index)` tuple of a base
//! flake. `EdgeKey` captures exactly that tuple in a form suitable for:
//!
//! - keying the in-memory [`AttachmentNovelty`](../../fluree-db-novelty)
//!   forward multimap (`EdgeKey -> Vec<ann_sid>`),
//! - keying the on-disk forward arena once the M2 indexer lands,
//! - encoding to a durable system-fact bundle via the seven `f:reifies*`
//!   predicates (the M1 source of truth), and
//! - decoding back from those facts at warmup or read time.
//!
//! Total ordering and serde derive cleanly from the field types.
//!
//! See `EDGE_ANNOTATIONS_IMPL_PLAN.md` M1 (durable attachment encoding)
//! for the contract this type implements.

use crate::flake::{Flake, FlakeMeta};
use crate::namespaces::{
    is_reifies_datatype, is_reifies_graph, is_reifies_lang, is_reifies_list_index,
    is_reifies_object, is_reifies_predicate, is_reifies_subject,
};
use crate::sid::Sid;
use crate::value::FlakeValue;
use fluree_vocab::db as fluree_db_predicates;
use fluree_vocab::namespaces::{FLUREE_DB, JSON_LD, XSD};
use fluree_vocab::xsd_names;
use serde::{Deserialize, Serialize};

/// Datatype SID for IRI-ref objects (`$id`).
///
/// Inlined helper rather than a `pub const` because `Sid::new` allocates an
/// `Arc<str>`. Callers that need it on a hot path should cache the result.
#[inline]
pub fn id_datatype_sid() -> Sid {
    Sid::new(JSON_LD, "id")
}

/// Datatype SID for `xsd:string` literals (used for `f:reifiesLang`).
#[inline]
pub fn xsd_string_datatype_sid() -> Sid {
    Sid::new(XSD, xsd_names::STRING)
}

/// A stable identifier for a base triple eligible to carry annotations.
///
/// Fields mirror [`Flake`] one-for-one (minus `t`/`op`/`ann`-side bits) so
/// the conversion from a base flake is mechanical and the round-trip
/// encoding via `f:reifies*` system facts is lossless.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EdgeKey {
    /// Named graph the edge lives in. `None` = default graph.
    pub g: Option<Sid>,
    /// Subject SID.
    pub s: Sid,
    /// Predicate SID.
    pub p: Sid,
    /// Object value (any [`FlakeValue`]: refs, literals, etc.).
    pub o: FlakeValue,
    /// Datatype SID of the object.
    pub dt: Sid,
    /// Language tag for langString objects, when applicable.
    pub lang: Option<String>,
    /// List index for list-element flakes. v1 always `None` —
    /// list-occurrence annotations are deferred (see decisions section).
    pub list_i: Option<i32>,
}

impl EdgeKey {
    /// Construct an `EdgeKey` from a base flake.
    ///
    /// The flake's `t` and `op` are intentionally discarded — the key is
    /// time-agnostic; attachment lifecycle (assert / retract) is tracked
    /// separately on the attachment row itself.
    pub fn from_flake(flake: &Flake) -> Self {
        let (lang, list_i) = match &flake.m {
            Some(meta) => (meta.lang.clone(), meta.i),
            None => (None, None),
        };
        Self {
            g: flake.g.clone(),
            s: flake.s.clone(),
            p: flake.p.clone(),
            o: flake.o.clone(),
            dt: flake.dt.clone(),
            lang,
            list_i,
        }
    }

    /// True iff `flake` represents the same edge as this key.
    ///
    /// Compares every position structurally; ignores `t` / `op` /
    /// metadata fields that aren't part of the edge identity.
    pub fn matches(&self, flake: &Flake) -> bool {
        if self.g != flake.g
            || self.s != flake.s
            || self.p != flake.p
            || self.dt != flake.dt
            || self.o != flake.o
        {
            return false;
        }
        let (flake_lang, flake_list_i) = match &flake.m {
            Some(meta) => (meta.lang.as_deref(), meta.i),
            None => (None, None),
        };
        self.lang.as_deref() == flake_lang && self.list_i == flake_list_i
    }

    /// Encode this edge as the durable `f:reifies*` system-fact bundle
    /// for annotation subject `ann` at transaction time `t`, with the
    /// given assertion `op` (`true` = assert, `false` = retract).
    ///
    /// All bundle facts share the same `t`/`op` and graph context as the
    /// caller's transaction. The bundle is **complete by construction**:
    /// every `EdgeKey` shape produces exactly one `f:reifiesSubject`,
    /// `f:reifiesPredicate`, `f:reifiesObject`, and `f:reifiesDatatype`
    /// flake, plus optional `f:reifiesGraph` (when the edge lives in a
    /// named graph) and `f:reifiesLang` (when applicable). v1 never
    /// emits `f:reifiesListIndex` (list-occurrence annotations deferred).
    ///
    /// Callers must not split or reorder the bundle — partial bundles
    /// are rejected by the replay validator (see
    /// `EDGE_ANNOTATIONS_IMPL_PLAN.md` M1).
    pub fn to_reifies_facts(&self, ann: &Sid, t: i64, op: bool) -> Vec<Flake> {
        let mut facts = Vec::with_capacity(7);
        let id_dt = id_datatype_sid();
        let str_dt = xsd_string_datatype_sid();

        // f:reifiesGraph — present iff edge is in a named graph.
        if let Some(g) = &self.g {
            facts.push(Flake::new(
                ann.clone(),
                Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_GRAPH),
                FlakeValue::Ref(g.clone()),
                id_dt.clone(),
                t,
                op,
                None,
            ));
        }

        // f:reifiesSubject — required.
        facts.push(Flake::new(
            ann.clone(),
            Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_SUBJECT),
            FlakeValue::Ref(self.s.clone()),
            id_dt.clone(),
            t,
            op,
            None,
        ));

        // f:reifiesPredicate — required.
        facts.push(Flake::new(
            ann.clone(),
            Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_PREDICATE),
            FlakeValue::Ref(self.p.clone()),
            id_dt.clone(),
            t,
            op,
            None,
        ));

        // f:reifiesObject — required. Preserves the original object's
        // datatype on the flake so typed-equality lookups round-trip.
        facts.push(Flake::new(
            ann.clone(),
            Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_OBJECT),
            self.o.clone(),
            self.dt.clone(),
            t,
            op,
            None,
        ));

        // f:reifiesDatatype — required. Names the dt SID itself so
        // queries can filter on the original object's datatype without
        // inspecting the object value.
        facts.push(Flake::new(
            ann.clone(),
            Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_DATATYPE),
            FlakeValue::Ref(self.dt.clone()),
            id_dt,
            t,
            op,
            None,
        ));

        // f:reifiesLang — optional, only when the original object
        // carried a language tag.
        if let Some(lang) = &self.lang {
            facts.push(Flake::new(
                ann.clone(),
                Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_LANG),
                FlakeValue::String(lang.clone()),
                str_dt,
                t,
                op,
                None,
            ));
        }

        // f:reifiesListIndex — deferred (v1 always omitted).

        facts
    }

    /// Inverse of [`Self::to_reifies_facts`]: reconstruct an `EdgeKey`
    /// from its bundle of `f:reifies*` flakes.
    ///
    /// Validates **bundle completeness**:
    /// - exactly one each of `Subject`, `Predicate`, `Object`, `Datatype`
    /// - at most one `Graph` (absent = default graph)
    /// - at most one `Lang`
    /// - never any `ListIndex` (v1)
    /// - all flakes share the same annotation subject (caller's
    ///   responsibility; this fn doesn't cross-validate `s` across the
    ///   slice)
    ///
    /// Returns `Err` with a structured [`EdgeKeyDecodeError`] when the
    /// bundle is malformed; the replay validator surfaces this through
    /// telemetry.
    pub fn from_reifies_facts(facts: &[Flake]) -> Result<Self, EdgeKeyDecodeError> {
        let mut g: Option<Sid> = None;
        let mut s_pos: Option<Sid> = None;
        let mut p_pos: Option<Sid> = None;
        let mut o_pos: Option<(FlakeValue, Sid)> = None;
        let mut dt_pos: Option<Sid> = None;
        let mut lang: Option<String> = None;

        for f in facts {
            if is_reifies_graph(&f.p) {
                if g.is_some() {
                    return Err(EdgeKeyDecodeError::Duplicate("f:reifiesGraph"));
                }
                let FlakeValue::Ref(sid) = &f.o else {
                    return Err(EdgeKeyDecodeError::WrongType("f:reifiesGraph"));
                };
                g = Some(sid.clone());
            } else if is_reifies_subject(&f.p) {
                if s_pos.is_some() {
                    return Err(EdgeKeyDecodeError::Duplicate("f:reifiesSubject"));
                }
                let FlakeValue::Ref(sid) = &f.o else {
                    return Err(EdgeKeyDecodeError::WrongType("f:reifiesSubject"));
                };
                s_pos = Some(sid.clone());
            } else if is_reifies_predicate(&f.p) {
                if p_pos.is_some() {
                    return Err(EdgeKeyDecodeError::Duplicate("f:reifiesPredicate"));
                }
                let FlakeValue::Ref(sid) = &f.o else {
                    return Err(EdgeKeyDecodeError::WrongType("f:reifiesPredicate"));
                };
                p_pos = Some(sid.clone());
            } else if is_reifies_object(&f.p) {
                if o_pos.is_some() {
                    return Err(EdgeKeyDecodeError::Duplicate("f:reifiesObject"));
                }
                o_pos = Some((f.o.clone(), f.dt.clone()));
            } else if is_reifies_datatype(&f.p) {
                if dt_pos.is_some() {
                    return Err(EdgeKeyDecodeError::Duplicate("f:reifiesDatatype"));
                }
                let FlakeValue::Ref(sid) = &f.o else {
                    return Err(EdgeKeyDecodeError::WrongType("f:reifiesDatatype"));
                };
                dt_pos = Some(sid.clone());
            } else if is_reifies_lang(&f.p) {
                if lang.is_some() {
                    return Err(EdgeKeyDecodeError::Duplicate("f:reifiesLang"));
                }
                let FlakeValue::String(s) = &f.o else {
                    return Err(EdgeKeyDecodeError::WrongType("f:reifiesLang"));
                };
                lang = Some(s.clone());
            } else if is_reifies_list_index(&f.p) {
                // v1 deferral: even seeing one is malformed.
                return Err(EdgeKeyDecodeError::DeferredFeature("f:reifiesListIndex"));
            }
            // Non-`f:reifies*` flakes (annotation metadata) are ignored
            // — they describe the annotation subject, not the edge.
        }

        let s = s_pos.ok_or(EdgeKeyDecodeError::Missing("f:reifiesSubject"))?;
        let p = p_pos.ok_or(EdgeKeyDecodeError::Missing("f:reifiesPredicate"))?;
        let (o, o_dt_from_flake) = o_pos.ok_or(EdgeKeyDecodeError::Missing("f:reifiesObject"))?;

        // `f:reifiesDatatype` is optional: when present it must agree
        // with the flake-level `dt` of the `f:reifiesObject` row (both
        // encode the same value). When absent, the flake-level dt is
        // canonical — the bundle is still complete because object
        // datatype round-trips via the flake's own `dt` field. The
        // pre-expansion JSON-LD lowering path emits the optional
        // form; the in-Rust `to_reifies_facts` builder emits both for
        // diagnostic clarity.
        let dt = match dt_pos {
            Some(separate) if separate != o_dt_from_flake => {
                return Err(EdgeKeyDecodeError::DatatypeMismatch);
            }
            Some(separate) => separate,
            None => o_dt_from_flake,
        };

        Ok(Self {
            g,
            s,
            p,
            o,
            dt,
            lang,
            // v1: always None.
            list_i: None,
        })
    }
}

/// Errors decoding an [`EdgeKey`] from a bundle of `f:reifies*` flakes.
///
/// All variants are recoverable — the replay validator skips the
/// malformed annotation, increments a telemetry counter, and continues.
/// The annotation's *non*-`f:reifies` metadata facts remain visible as
/// ordinary RDF (just without the attachment binding).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeKeyDecodeError {
    /// A required `f:reifies*` predicate was absent from the bundle.
    Missing(&'static str),
    /// A required `f:reifies*` predicate appeared more than once.
    Duplicate(&'static str),
    /// The flake's object had the wrong [`FlakeValue`] type for the
    /// predicate (e.g. `f:reifiesSubject` carried a literal).
    WrongType(&'static str),
    /// `f:reifiesObject`'s flake-level datatype did not match the
    /// `f:reifiesDatatype` value. Indicates a tampered or buggy bundle.
    DatatypeMismatch,
    /// A predicate that v1 explicitly defers was present.
    DeferredFeature(&'static str),
}

impl std::fmt::Display for EdgeKeyDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(p) => write!(f, "missing {p}"),
            Self::Duplicate(p) => write!(f, "duplicate {p}"),
            Self::WrongType(p) => write!(f, "wrong object type for {p}"),
            Self::DatatypeMismatch => {
                write!(f, "f:reifiesObject dt mismatch with f:reifiesDatatype")
            }
            Self::DeferredFeature(p) => write!(f, "{p} is deferred to a future milestone"),
        }
    }
}

impl std::error::Error for EdgeKeyDecodeError {}

/// Convert an `EdgeKey` to a fresh [`FlakeMeta`] capturing the lang/list
/// fields, returning `None` when both fields are absent (matching the
/// existing `Flake.m: Option<FlakeMeta>` convention).
fn edge_key_to_flake_meta(lang: Option<&str>, list_i: Option<i32>) -> Option<FlakeMeta> {
    if lang.is_none() && list_i.is_none() {
        return None;
    }
    Some(FlakeMeta {
        lang: lang.map(String::from),
        i: list_i,
    })
}

impl EdgeKey {
    /// Reconstruct a base [`Flake`] equivalent to the one this key was
    /// derived from. Useful for cascade-retract paths that need to emit
    /// an inverse flake matching the original by structure.
    ///
    /// `t` and `op` are caller-supplied — the cascade decides whether
    /// it is asserting or retracting.
    pub fn to_base_flake(&self, t: i64, op: bool) -> Flake {
        let m = edge_key_to_flake_meta(self.lang.as_deref(), self.list_i);
        match &self.g {
            Some(g) => Flake::new_in_graph(
                g.clone(),
                self.s.clone(),
                self.p.clone(),
                self.o.clone(),
                self.dt.clone(),
                t,
                op,
                m,
            ),
            None => Flake::new(
                self.s.clone(),
                self.p.clone(),
                self.o.clone(),
                self.dt.clone(),
                t,
                op,
                m,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_flake() -> Flake {
        Flake::new(
            Sid::new(13, "alice"),
            Sid::new(13, "worksFor"),
            FlakeValue::Ref(Sid::new(13, "acme")),
            id_datatype_sid(),
            42,
            true,
            None,
        )
    }

    #[test]
    fn from_flake_round_trips_through_matches() {
        let f = sample_flake();
        let key = EdgeKey::from_flake(&f);
        assert!(key.matches(&f));
    }

    #[test]
    fn matches_ignores_t_and_op() {
        let f = sample_flake();
        let key = EdgeKey::from_flake(&f);
        let mut other = f.clone();
        other.t = 99;
        other.op = false;
        assert!(
            key.matches(&other),
            "EdgeKey identity is t/op-agnostic by design"
        );
    }

    #[test]
    fn matches_distinguishes_graph() {
        let mut f = sample_flake();
        f.g = Some(Sid::new(13, "graph_a"));
        let key = EdgeKey::from_flake(&f);
        let mut other = sample_flake();
        other.g = Some(Sid::new(13, "graph_b"));
        assert!(!key.matches(&other));
    }

    #[test]
    fn reifies_round_trip_default_graph_no_lang() {
        let f = sample_flake();
        let key = EdgeKey::from_flake(&f);
        let ann = Sid::new(13, "ann1");
        let bundle = key.to_reifies_facts(&ann, 42, true);
        // 4 required + 0 optional = 4 facts (no graph, no lang, no list_i).
        assert_eq!(bundle.len(), 4);
        let decoded = EdgeKey::from_reifies_facts(&bundle).expect("decode succeeds");
        assert_eq!(decoded, key);
    }

    #[test]
    fn reifies_round_trip_named_graph_with_lang() {
        let mut f = sample_flake();
        f.g = Some(Sid::new(13, "graph_a"));
        f.o = FlakeValue::String("Engineer".into());
        f.dt = Sid::new(2, "string"); // xsd:string
        f.m = Some(FlakeMeta {
            lang: Some("fr".into()),
            i: None,
        });
        let key = EdgeKey::from_flake(&f);
        let ann = Sid::new(13, "ann_named");
        let bundle = key.to_reifies_facts(&ann, 42, true);
        assert_eq!(bundle.len(), 6, "graph + S + P + O + Dt + lang");
        let decoded = EdgeKey::from_reifies_facts(&bundle).expect("decode succeeds");
        assert_eq!(decoded, key);
    }

    #[test]
    fn decode_rejects_missing_required() {
        let f = sample_flake();
        let key = EdgeKey::from_flake(&f);
        let ann = Sid::new(13, "ann1");
        let mut bundle = key.to_reifies_facts(&ann, 42, true);
        bundle.retain(|f| !is_reifies_subject(&f.p));
        let err = EdgeKey::from_reifies_facts(&bundle).unwrap_err();
        assert_eq!(err, EdgeKeyDecodeError::Missing("f:reifiesSubject"));
    }

    #[test]
    fn decode_rejects_duplicate_required() {
        let f = sample_flake();
        let key = EdgeKey::from_flake(&f);
        let ann = Sid::new(13, "ann1");
        let mut bundle = key.to_reifies_facts(&ann, 42, true);
        let dup = bundle
            .iter()
            .find(|f| is_reifies_predicate(&f.p))
            .expect("predicate flake exists")
            .clone();
        bundle.push(dup);
        let err = EdgeKey::from_reifies_facts(&bundle).unwrap_err();
        assert_eq!(err, EdgeKeyDecodeError::Duplicate("f:reifiesPredicate"));
    }

    #[test]
    fn decode_rejects_list_index_in_v1() {
        let f = sample_flake();
        let key = EdgeKey::from_flake(&f);
        let ann = Sid::new(13, "ann1");
        let mut bundle = key.to_reifies_facts(&ann, 42, true);
        bundle.push(Flake::new(
            ann.clone(),
            Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_LIST_INDEX),
            FlakeValue::Long(0),
            Sid::new(XSD, xsd_names::INTEGER),
            42,
            true,
            None,
        ));
        let err = EdgeKey::from_reifies_facts(&bundle).unwrap_err();
        assert_eq!(
            err,
            EdgeKeyDecodeError::DeferredFeature("f:reifiesListIndex")
        );
    }

    #[test]
    fn decode_ignores_unrelated_flakes() {
        // Annotation metadata flakes (e.g. `ann ex:role "Engineer"`)
        // share the bundle but must be passed through transparently.
        let f = sample_flake();
        let key = EdgeKey::from_flake(&f);
        let ann = Sid::new(13, "ann1");
        let mut bundle = key.to_reifies_facts(&ann, 42, true);
        bundle.push(Flake::new(
            ann,
            Sid::new(13, "role"),
            FlakeValue::String("Engineer".into()),
            Sid::new(XSD, xsd_names::STRING),
            42,
            true,
            None,
        ));
        let decoded = EdgeKey::from_reifies_facts(&bundle).expect("metadata is ignored");
        assert_eq!(decoded, key);
    }

    #[test]
    fn to_base_flake_round_trips_with_from_flake() {
        let f = sample_flake();
        let key = EdgeKey::from_flake(&f);
        let rebuilt = key.to_base_flake(f.t, f.op);
        // EdgeKey-identity components should match.
        assert_eq!(rebuilt.s, f.s);
        assert_eq!(rebuilt.p, f.p);
        assert_eq!(rebuilt.o, f.o);
        assert_eq!(rebuilt.dt, f.dt);
        assert_eq!(rebuilt.g, f.g);
    }
}
