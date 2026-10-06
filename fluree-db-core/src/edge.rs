//! `EdgeKey` — a stable identifier for a base triple that has (or could
//! have) annotations attached to it.
//!
//! Annotations in Fluree reify a specific edge: the `(graph, subject,
//! predicate, object, datatype, language, list-index)` tuple of a base
//! flake. `EdgeKey` captures exactly that tuple; export keys its
//! edge → reifier lookup by it.

use crate::flake::Flake;
use crate::sid::Sid;
use crate::value::FlakeValue;
use fluree_vocab::namespaces::{JSON_LD, XSD};
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

/// Datatype SID for `xsd:string` literals.
#[inline]
pub fn xsd_string_datatype_sid() -> Sid {
    Sid::new(XSD, xsd_names::STRING)
}

/// A stable identifier for a base triple eligible to carry annotations.
///
/// Fields mirror [`Flake`] one-for-one (minus `t`/`op`/`ann`-side bits) so
/// the conversion from a base flake is mechanical.
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
    /// List index for list-element flakes; always `None`, as triple terms
    /// carry no list position.
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
}
