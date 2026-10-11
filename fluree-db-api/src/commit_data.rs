//! Fold a sequence of commits into the flakes, `namespace_delta` and named
//! graphs of ONE new commit.
//!
//! Used by the merge and revert paths to bundle multiple source commits into
//! a single commit. Because that commit lands at a single `t`, and novelty
//! resolves a same-`t` assert and retract of one fact as "retracted", the
//! range's flakes are **netted per fact** first: a fact the range asserted,
//! retracted, and asserted again folds to one assert; a fact it replaced and
//! then restored folds to nothing. The netting contract is
//! [`NetChangeAccumulator`]'s.

use fluree_db_core::{Commit, ConflictKey, ContentId, Flake};
use fluree_db_novelty::NetChangeAccumulator;
use rustc_hash::FxHashSet;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Which flakes on one side of a branch comparison are that side's own
/// changes, tracked as its commits are walked oldest-first.
///
/// A merge whose merged-in history the other side already holds is not one
/// of the side's own commits. Its flakes on keys this side never changed are
/// copies of the other side's changes. Applying them again brings back
/// values the other side has since replaced, and comparing them reports
/// conflicts on keys this side never touched. Its flakes on keys this side
/// did change are how that merge resolved the overlap, and dropping those
/// would undo the resolution.
pub(crate) struct OwnChanges<'a> {
    own: HashSet<&'a ContentId>,
    keys: FxHashSet<ConflictKey>,
}

impl<'a> OwnChanges<'a> {
    /// `own` is the side's own commits, as `BranchSide::own` reports them.
    pub(crate) fn new(own: &'a [ContentId]) -> Self {
        Self {
            own: own.iter().collect(),
            keys: FxHashSet::default(),
        }
    }

    /// Whether the side made commit `cid` itself.
    pub(crate) fn is_own(&self, cid: &ContentId) -> bool {
        self.own.contains(cid)
    }

    /// Record `cid`'s changes, dropping the flakes that copy the other
    /// side's. Commits must arrive oldest-first, because a merge's
    /// resolution covers only keys the side changed before it.
    pub(crate) fn retain_changes(&mut self, cid: &ContentId, flakes: &mut Vec<Flake>) {
        if self.is_own(cid) {
            self.keys.extend(flakes.iter().map(key_of));
        } else {
            flakes.retain(|flake| self.keys.contains(&key_of(flake)));
        }
    }

    /// Every key the side has changed so far.
    pub(crate) fn into_keys(self) -> FxHashSet<ConflictKey> {
        self.keys
    }
}

/// The (subject, predicate, graph) key `flake` changes.
pub(crate) fn key_of(flake: &Flake) -> ConflictKey {
    ConflictKey::new(flake.s.clone(), flake.p.clone(), flake.g.clone())
}

/// Flakes and metadata accumulated from a sequence of commits.
#[derive(Default)]
pub(crate) struct CollectedCommitData {
    /// The range's net change, one flake per surviving fact. Unordered.
    pub(crate) flakes: Vec<Flake>,
    /// Union of namespace deltas; earlier commits win on key collisions.
    pub(crate) namespace_delta: HashMap<u16, String>,
    /// Named graphs the range's commits list, by IRI. Unioned by IRI: each
    /// commit keys its graphs by its own branch's ids (older commits by
    /// transaction-local numbers), so keys from different commits can
    /// collide while naming different graphs.
    pub(crate) graph_iris: BTreeSet<String>,
}

/// Which direction the new commit applies the range in.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Fold {
    /// Apply the commits as they were (merge): the newest commit's flakes
    /// are the newest change.
    Replay,
    /// Apply the commits' inverses (revert): undoing the range means the
    /// oldest commit's inverse is applied last, so it is the newest change.
    Undo,
}

/// Fold `commits` into a [`CollectedCommitData`].
///
/// Commits must be supplied in **oldest-first** order so that earlier
/// commits take precedence on namespace delta keys (matches the historical
/// `or_insert` semantics in `merge.rs::collect_commit_data`).
///
/// Flake `t` is not a concern here: `StagedLedger::new` restamps every
/// staged flake.
pub(crate) fn collect_from_commits<I>(commits: I, fold: Fold) -> CollectedCommitData
where
    I: IntoIterator<Item = Commit>,
{
    let mut data = CollectedCommitData::default();
    let mut ordered: Vec<Vec<Flake>> = Vec::new();
    for commit in commits {
        ordered.push(commit.flakes);
        for (code, prefix) in commit.namespace_delta {
            data.namespace_delta.entry(code).or_insert(prefix);
        }
        data.graph_iris.extend(commit.graph_delta.into_values());
    }

    // The accumulator wants the range newest-change-first. For a replay
    // that is the newest commit, last flake first. For an undo the inverse
    // of the oldest commit is applied last, so the oldest commit's inverse
    // comes first and each commit's own flakes keep their order.
    let mut acc = NetChangeAccumulator::default();
    match fold {
        Fold::Replay => {
            for flakes in ordered.iter().rev() {
                for flake in flakes.iter().rev() {
                    acc.push_newest_first(flake);
                }
            }
        }
        Fold::Undo => {
            let mut skipped = 0usize;
            for flakes in &ordered {
                for flake in flakes {
                    if is_malformed_retraction(flake) {
                        skipped += 1;
                        continue;
                    }
                    acc.push_newest_first(&flake.invert());
                }
            }
            if skipped > 0 {
                tracing::warn!(
                    skipped,
                    "revert skipped retractions no stored fact could match (an \
                     rdf:langString without a language tag, or a tag on another \
                     datatype); undoing one would assert an invalid literal"
                );
            }
        }
    }
    data.flakes = acc.finish();
    data
}

/// A retraction whose shape no stored fact has: an `rdf:langString`
/// without a language tag, or a tag on any other datatype.
///
/// Upserts written before the retraction resolver rebuilt each retraction
/// from a query binding and dropped the tag, so `"a"@en` was retracted as a
/// tagless `"a"^^rdf:langString`. Such a retraction matched nothing and is
/// inert for reads and indexing, but a revert inverts every flake of the
/// reverted commits, and inverting it asserts a literal that cannot exist.
/// Legacy retractions of list entries without their position are
/// well-formed plain literals, so no shape check can tell them apart.
fn is_malformed_retraction(flake: &Flake) -> bool {
    if flake.op {
        return false;
    }
    let lang_string = flake.dt.namespace_code == fluree_vocab::namespaces::RDF
        && flake.dt.name.as_ref() == "langString";
    let tagged = flake.m.as_ref().is_some_and(|m| m.lang.is_some());
    lang_string != tagged
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeMeta, FlakeValue, Sid};

    fn label(v: &str, dt: &str, lang: Option<&str>, t: i64, op: bool) -> Flake {
        Flake::new(
            Sid::new(100, "s"),
            Sid::new(100, "label"),
            FlakeValue::String(v.to_string()),
            Sid::new(fluree_vocab::namespaces::RDF, dt),
            t,
            op,
            lang.map(FlakeMeta::with_lang),
        )
    }

    /// Reverting an upsert written before the retraction resolver: its
    /// retraction of `"a"@en` lost the tag. Inverting that retraction would
    /// assert `"a"^^rdf:langString` with no tag, so the undo skips it; the
    /// well-formed flakes of the same commit still invert.
    #[test]
    fn undo_skips_retractions_no_stored_fact_could_match() {
        let legacy_upsert = Commit::new(
            2,
            vec![
                // The phantom: tagless langString.
                label("a", "langString", None, 2, false),
                // Well-formed: the upsert's assertion.
                label("b", "langString", Some("en"), 2, true),
                // A tag on a non-langString datatype is the other malformed
                // shape.
                label("c", "HTML", Some("en"), 2, false),
            ],
        );
        let undone = collect_from_commits([legacy_upsert], Fold::Undo).flakes;
        assert_eq!(undone.len(), 1, "{undone:?}");
        let f = &undone[0];
        assert!(!f.op, "the assertion of \"b\"@en inverts to its retraction");
        assert_eq!(f.o, FlakeValue::String("b".to_string()));
        assert_eq!(f.m.as_ref().and_then(|m| m.lang.as_deref()), Some("en"));
    }
}
