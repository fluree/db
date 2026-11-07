//! Reasoned heads: persisted results of a registered reasoning program.
//!
//! A head is a complete merged index of one branch for one program reasoning
//! against one graph: every graph's asserted facts plus the derived ones,
//! pinned to a basis commit. A reasoned query that matches a head reads only
//! the head, so every binding stays in the head's dictionary scope.
//!
//! This module holds the types shared by the nameservice (which stores one
//! [`ReasonedHeads`] document per branch), the worker that builds and
//! publishes generations, and the query path that serves them:
//!
//! - [`ReasonedHeads`] / [`HeadEntry`] / [`Registration`]: the per-branch
//!   document, updated by compare-and-swap on `v` under the branch fence.
//! - [`ReasonedManifest`]: one published generation, content-addressed.
//! - [`admit`]: the publication rules every publisher applies against the
//!   document it last read.

use crate::content_id::ContentId;
use crate::content_kind::ContentKind;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

// ============================================================================
// Identity
// ============================================================================

/// A head's id: a ULID assigned when a registration creates the head, never
/// reused. It names the head's storage folder, so it is never user text.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct HeadId(String);

impl HeadId {
    pub fn parse(id: &str) -> Result<Self, String> {
        let crockford =
            |c: char| c.is_ascii_digit() || (c.is_ascii_uppercase() && !"ILOU".contains(c));
        if id.len() != 26 || !id.chars().all(crockford) {
            return Err(format!(
                "Invalid reasoned head id '{id}': expected a 26-character ULID"
            ));
        }
        Ok(Self(id.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for HeadId {
    type Error = String;
    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::parse(&id)
    }
}

impl From<HeadId> for String {
    fn from(id: HeadId) -> Self {
        id.0
    }
}

impl fmt::Display for HeadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for HeadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HeadId({})", self.0)
    }
}

/// Increases whenever a branch's readable content changes without a new
/// commit (an erasure replacement activating). Until erasure lands on this
/// line every branch is at epoch 0.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ErasureEpoch(pub u64);

// ============================================================================
// Semantics
// ============================================================================

/// What a rule that reads the clock sees.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "camelCase")]
pub enum ClockPolicy {
    /// The commit time of the basis: query-time reasoning's default when a
    /// query names no evaluation time.
    SnapshotCommitTime,
    /// A fixed evaluation time, as an `xsd:dateTime` lexical form.
    Fixed { time: String },
}

/// Everything a generation's content depends on besides its basis. Two
/// generations computed for equal keys at equal bases hold the same facts.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SemanticKey {
    /// Content id of the program's canonical encoding (profile, rules,
    /// rule sources, options): [`program_id`].
    pub program: ContentId,
    /// The graph the program reasons against; `None` is the default graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoned_graph: Option<String>,
    pub clock: ClockPolicy,
}

/// The content id a canonical program encoding is known by.
pub fn program_id(canonical: &[u8]) -> ContentId {
    ContentId::new(ContentKind::LedgerConfig, canonical)
}

/// Which semantics an entry is meant to compute now. Every registration
/// operation (create, change, retire) bumps `revision` by compare-and-swap;
/// only registration operations change it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Registration {
    pub revision: u64,
    pub semantics: SemanticKey,
    /// The program itself, in the canonical JSON form `semantics.program`
    /// names.
    pub program: serde_json::Value,
    pub state: RegistrationState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum RegistrationState {
    Active,
    /// Logically removed: workers are refused and readers stop serving it.
    /// The entry stays (a GC participant) until its files are cleaned up.
    Retired {
        at_ms: i64,
    },
}

/// The inputs a result was computed from, on the head's own branch.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Basis {
    pub commit: ContentId,
    /// Locates `commit` on its chain. Not an order: heads can be reset.
    pub t: i64,
    pub epoch: ErasureEpoch,
}

/// One head's place in the per-branch document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeadEntry {
    pub registration: Registration,
    /// Publication refuses candidates whose epoch is below this.
    #[serde(default)]
    pub floor: ErasureEpoch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published: Option<HeadPointer>,
}

/// The generation an entry currently publishes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeadPointer {
    pub manifest: ContentId,
    pub generation: u64,
    /// Copied from the manifest so admission and read paths need no fetch.
    pub revision: u64,
    pub basis: Basis,
    pub settled: ContentId,
}

/// The per-branch document: every head of the branch, read in one
/// consistent read.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReasonedHeads {
    /// Compare-and-swap version. 0 = never written.
    pub v: i64,
    #[serde(default)]
    pub heads: BTreeMap<HeadId, HeadEntry>,
}

impl ReasonedHeads {
    pub fn is_empty(&self) -> bool {
        self.heads.is_empty()
    }
}

// ============================================================================
// Manifest
// ============================================================================

/// How a generation relates to its predecessor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GenerationKind {
    /// Advances the predecessor's basis.
    Logical,
    /// Bootstrap, an obsolete basis, or an erasure: may replace an equal
    /// basis.
    Rebuild,
}

/// Counts that describe a generation, for status and measurements.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerationStats {
    /// Facts present only because they were derived (|D|).
    pub derived_only: u64,
    /// Facts the program derived, including those also asserted (|C|).
    pub derived_total: u64,
    /// Facts the fold added and removed relative to the predecessor.
    pub added: u64,
    pub removed: u64,
}

/// One published generation: content-addressed, kind
/// [`ContentKind::ReasonedManifest`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReasonedManifest {
    pub head_id: HeadId,
    pub ledger_id: String,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_manifest: Option<ContentId>,
    /// When the compare-and-swap that published it was attempted. A
    /// generation is superseded at its successor's `published_at_ms`.
    pub published_at_ms: i64,
    pub kind: GenerationKind,
    pub revision: u64,
    pub semantics: SemanticKey,
    /// Digest of the reasoning the worker settled the program to, against the
    /// branch's configuration at the basis. A query is served only when its
    /// own settled reasoning has the same digest.
    pub settled: ContentId,
    pub basis: Basis,
    /// The merged index root, in the head's namespace.
    pub root: ContentId,
    /// The derived-only set D, kind [`ContentKind::DerivedFacts`].
    pub derived: ContentId,
    #[serde(default)]
    pub stats: GenerationStats,
}

const MANIFEST_MAGIC: &[u8; 4] = b"FRM1";

impl ReasonedManifest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = MANIFEST_MAGIC.to_vec();
        serde_json::to_writer(&mut out, self).expect("manifest serializes");
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let body = bytes
            .strip_prefix(MANIFEST_MAGIC.as_slice())
            .ok_or_else(|| "not a reasoned-head manifest (bad magic)".to_string())?;
        serde_json::from_slice(body).map_err(|e| format!("reasoned-head manifest: {e}"))
    }

    pub fn content_id(bytes: &[u8]) -> ContentId {
        ContentId::new(ContentKind::ReasonedManifest, bytes)
    }

    pub fn pointer(&self, manifest: ContentId) -> HeadPointer {
        HeadPointer {
            manifest,
            generation: self.generation,
            revision: self.revision,
            basis: self.basis.clone(),
            settled: self.settled.clone(),
        }
    }
}

/// Magic prefix of one bucket of a derived-only set; the body is a
/// commit-codec blob holding the bucket's facts.
pub const DERIVED_FACTS_MAGIC: &[u8; 4] = b"FDS1";

/// A generation's derived-only set: buckets of facts by subject, each its
/// own content-addressed blob, so a generation that changes a few subjects
/// writes only their buckets. Kind [`ContentKind::DerivedFacts`], like its
/// buckets.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedIndex {
    pub facts: u64,
    pub buckets: Vec<ContentId>,
}

const DERIVED_INDEX_MAGIC: &[u8; 4] = b"FDI1";

impl DerivedIndex {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = DERIVED_INDEX_MAGIC.to_vec();
        serde_json::to_writer(&mut out, self).expect("derived index serializes");
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let body = bytes
            .strip_prefix(DERIVED_INDEX_MAGIC.as_slice())
            .ok_or_else(|| "not a derived-only set index (bad magic)".to_string())?;
        serde_json::from_slice(body).map_err(|e| format!("derived-only set index: {e}"))
    }
}

// ============================================================================
// Admission
// ============================================================================

/// What a publisher knows about a candidate's basis relative to the branch.
pub trait BasisOracle {
    /// Whether `basis` is live: its commit is an ancestor of, or equal to,
    /// the branch's current head.
    fn is_live(&self, basis: &Basis) -> bool;
    /// Whether `ancestor`'s commit is an ancestor of, or equal to,
    /// `descendant`'s.
    fn is_ancestor(&self, ancestor: &Basis, descendant: &Basis) -> bool;
}

/// `b2 ⪰ b1`: b1's commit is an ancestor of, or equal to, b2's, and b2's
/// epoch is at least b1's.
pub fn basis_dominates(oracle: &dyn BasisOracle, b2: &Basis, b1: &Basis) -> bool {
    b2.epoch >= b1.epoch && oracle.is_ancestor(b1, b2)
}

/// A generation ready to publish, before its place in the chain is known.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub head_id: HeadId,
    pub kind: GenerationKind,
    pub revision: u64,
    pub basis: Basis,
}

/// Why a candidate was not published. A discard never deletes files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Discard {
    /// The entry is gone, retired, or registered at another revision.
    NotEligible(String),
    /// The basis is no longer on the branch's line, or below the floor.
    BasisInvalid(String),
    /// The candidate does not advance the published generation.
    NoProgress(String),
}

impl fmt::Display for Discard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Discard::NotEligible(m) => write!(f, "not eligible: {m}"),
            Discard::BasisInvalid(m) => write!(f, "basis invalid: {m}"),
            Discard::NoProgress(m) => write!(f, "no progress: {m}"),
        }
    }
}

/// The chain position an admitted candidate takes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Admitted {
    pub generation: u64,
    pub prev_manifest: Option<ContentId>,
}

/// Apply the publication rules to `candidate` against `actual`, the document
/// as last read: eligibility first, then the basis, then progress.
pub fn admit(
    actual: &ReasonedHeads,
    candidate: &Candidate,
    oracle: &dyn BasisOracle,
) -> Result<Admitted, Discard> {
    let entry = actual
        .heads
        .get(&candidate.head_id)
        .ok_or_else(|| Discard::NotEligible("the head is not registered".into()))?;
    if !matches!(entry.registration.state, RegistrationState::Active) {
        return Err(Discard::NotEligible("the head is retired".into()));
    }
    if entry.registration.revision != candidate.revision {
        return Err(Discard::NotEligible(format!(
            "computed for revision {}, registered at {}",
            candidate.revision, entry.registration.revision
        )));
    }
    if candidate.basis.epoch < entry.floor {
        return Err(Discard::BasisInvalid(format!(
            "epoch {} is below the floor {}",
            candidate.basis.epoch.0, entry.floor.0
        )));
    }
    if !oracle.is_live(&candidate.basis) {
        return Err(Discard::BasisInvalid(
            "the basis commit is no longer on the branch".into(),
        ));
    }

    let Some(current) = &entry.published else {
        return Ok(Admitted {
            generation: 1,
            prev_manifest: None,
        });
    };
    let next = Admitted {
        generation: current.generation + 1,
        prev_manifest: Some(current.manifest.clone()),
    };
    let current_obsolete = current.basis.epoch < entry.floor || !oracle.is_live(&current.basis);
    if current_obsolete || current.revision != entry.registration.revision {
        return Ok(next);
    }
    let dominates = basis_dominates(oracle, &candidate.basis, &current.basis);
    match candidate.kind {
        GenerationKind::Logical if dominates && candidate.basis != current.basis => Ok(next),
        GenerationKind::Rebuild if dominates => Ok(next),
        _ if basis_dominates(oracle, &current.basis, &candidate.basis) => {
            Err(Discard::NoProgress(format!(
                "the published generation's basis (t={}) is at or past the candidate's (t={})",
                current.basis.t, candidate.basis.t
            )))
        }
        _ => Err(Discard::NoProgress(
            "the candidate's basis is incomparable with the published one".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cid(tag: &str) -> ContentId {
        ContentId::new(ContentKind::Commit, tag.as_bytes())
    }

    fn basis(tag: &str, t: i64) -> Basis {
        Basis {
            commit: cid(tag),
            t,
            epoch: ErasureEpoch(0),
        }
    }

    /// Commits on one line, plus an optional side line.
    struct Line {
        order: HashMap<ContentId, (u8, i64)>,
        head: ContentId,
    }

    impl Line {
        fn new(main: &[(&str, i64)], side: &[(&str, i64)], head: &str) -> Self {
            let mut order = HashMap::new();
            for (tag, t) in main {
                order.insert(cid(tag), (0, *t));
            }
            for (tag, t) in side {
                order.insert(cid(tag), (1, *t));
            }
            Self {
                order,
                head: cid(head),
            }
        }
    }

    impl BasisOracle for Line {
        fn is_live(&self, b: &Basis) -> bool {
            let head = self.order[&self.head];
            let this = self.order[&b.commit];
            this.0 == head.0 && this.1 <= head.1
        }
        fn is_ancestor(&self, a: &Basis, d: &Basis) -> bool {
            let a = self.order[&a.commit];
            let d = self.order[&d.commit];
            a.0 == d.0 && a.1 <= d.1
        }
    }

    fn key() -> SemanticKey {
        SemanticKey {
            program: cid("program"),
            reasoned_graph: None,
            clock: ClockPolicy::SnapshotCommitTime,
        }
    }

    fn doc(revision: u64, published: Option<(&str, i64, u64)>) -> (ReasonedHeads, HeadId) {
        let id = HeadId::parse("01JC0000000000000000000000").unwrap();
        let mut heads = BTreeMap::new();
        heads.insert(
            id.clone(),
            HeadEntry {
                registration: Registration {
                    revision,
                    semantics: key(),
                    program: serde_json::json!({}),
                    state: RegistrationState::Active,
                },
                floor: ErasureEpoch(0),
                published: published.map(|(tag, t, rev)| HeadPointer {
                    manifest: cid(&format!("manifest-{tag}")),
                    generation: 3,
                    revision: rev,
                    basis: basis(tag, t),
                    settled: cid("settled"),
                }),
            },
        );
        (ReasonedHeads { v: 1, heads }, id)
    }

    fn candidate(id: &HeadId, kind: GenerationKind, revision: u64, b: Basis) -> Candidate {
        Candidate {
            head_id: id.clone(),
            kind,
            revision,
            basis: b,
        }
    }

    #[test]
    fn a_first_generation_starts_the_chain() {
        let line = Line::new(&[("c1", 1)], &[], "c1");
        let (doc, id) = doc(1, None);
        let c = candidate(&id, GenerationKind::Rebuild, 1, basis("c1", 1));
        assert_eq!(
            admit(&doc, &c, &line),
            Ok(Admitted {
                generation: 1,
                prev_manifest: None
            })
        );
    }

    #[test]
    fn eligibility_is_checked_before_progress() {
        let line = Line::new(&[("c1", 1), ("c2", 2)], &[], "c2");
        let (doc, id) = doc(2, Some(("c1", 1, 2)));
        // Newer inputs, but computed for the old program.
        let c = candidate(&id, GenerationKind::Logical, 1, basis("c2", 2));
        assert!(matches!(
            admit(&doc, &c, &line),
            Err(Discard::NotEligible(_))
        ));
    }

    #[test]
    fn a_logical_generation_must_advance() {
        let line = Line::new(&[("c1", 1), ("c2", 2)], &[], "c2");
        let (doc, id) = doc(1, Some(("c2", 2, 1)));
        let same = candidate(&id, GenerationKind::Logical, 1, basis("c2", 2));
        assert!(matches!(
            admit(&doc, &same, &line),
            Err(Discard::NoProgress(_))
        ));
        let older = candidate(&id, GenerationKind::Logical, 1, basis("c1", 1));
        assert!(matches!(
            admit(&doc, &older, &line),
            Err(Discard::NoProgress(_))
        ));
        let (doc, id) = self::doc(1, Some(("c1", 1, 1)));
        let newer = candidate(&id, GenerationKind::Logical, 1, basis("c2", 2));
        assert_eq!(admit(&doc, &newer, &line).unwrap().generation, 4);
    }

    #[test]
    fn a_rebuild_may_replace_an_equal_basis() {
        let line = Line::new(&[("c1", 1)], &[], "c1");
        let (doc, id) = doc(1, Some(("c1", 1, 1)));
        let c = candidate(&id, GenerationKind::Rebuild, 1, basis("c1", 1));
        let admitted = admit(&doc, &c, &line).unwrap();
        assert_eq!(admitted.generation, 4);
        assert_eq!(admitted.prev_manifest, Some(cid("manifest-c1")));
    }

    #[test]
    fn an_obsolete_generation_is_replaced_by_any_live_candidate() {
        // The published basis sits on a line the branch was reset away from.
        let line = Line::new(&[("c1", 1), ("c2", 2)], &[("x3", 3)], "c2");
        let (doc, id) = doc(1, Some(("x3", 3, 1)));
        let c = candidate(&id, GenerationKind::Rebuild, 1, basis("c2", 2));
        assert!(admit(&doc, &c, &line).is_ok());
    }

    #[test]
    fn a_candidate_off_the_branch_is_refused() {
        let line = Line::new(&[("c1", 1), ("c2", 2)], &[("x3", 3)], "c2");
        let (doc, id) = doc(1, None);
        let c = candidate(&id, GenerationKind::Rebuild, 1, basis("x3", 3));
        assert!(matches!(
            admit(&doc, &c, &line),
            Err(Discard::BasisInvalid(_))
        ));
    }

    #[test]
    fn the_floor_refuses_stale_epochs() {
        let line = Line::new(&[("c1", 1)], &[], "c1");
        let (mut doc, id) = doc(1, None);
        doc.heads.get_mut(&id).unwrap().floor = ErasureEpoch(2);
        let c = candidate(&id, GenerationKind::Rebuild, 1, basis("c1", 1));
        assert!(matches!(
            admit(&doc, &c, &line),
            Err(Discard::BasisInvalid(_))
        ));
    }

    #[test]
    fn a_retired_head_admits_nothing() {
        let line = Line::new(&[("c1", 1)], &[], "c1");
        let (mut doc, id) = doc(1, None);
        doc.heads.get_mut(&id).unwrap().registration.state =
            RegistrationState::Retired { at_ms: 5 };
        let c = candidate(&id, GenerationKind::Rebuild, 1, basis("c1", 1));
        assert!(matches!(
            admit(&doc, &c, &line),
            Err(Discard::NotEligible(_))
        ));
    }

    #[test]
    fn the_manifest_round_trips() {
        let m = ReasonedManifest {
            head_id: HeadId::parse("01JC0000000000000000000000").unwrap(),
            ledger_id: "mydb:main".into(),
            generation: 2,
            prev_manifest: Some(cid("m1")),
            published_at_ms: 99,
            kind: GenerationKind::Logical,
            revision: 1,
            semantics: key(),
            settled: cid("settled"),
            basis: basis("c1", 1),
            root: cid("root"),
            derived: cid("derived"),
            stats: GenerationStats::default(),
        };
        let bytes = m.encode();
        assert_eq!(ReasonedManifest::decode(&bytes).unwrap(), m);
        assert!(ReasonedManifest::decode(b"nope").is_err());
    }

    #[test]
    fn the_document_round_trips() {
        let (doc, _) = doc(1, Some(("c1", 1, 1)));
        let json = serde_json::to_string(&doc).unwrap();
        assert_eq!(serde_json::from_str::<ReasonedHeads>(&json).unwrap(), doc);
    }
}
