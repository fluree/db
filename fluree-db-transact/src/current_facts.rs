//! Current facts: the one read every staged retraction comes from.
//!
//! **Invariant.** A retraction staged for commit names a fact that is
//! currently stored: its `(g, s, p, o, dt, m)` is read from storage (the
//! persisted index plus novelty, lifecycle-resolved), never rebuilt from a
//! query binding, a template, or an edge key. An intent that matches no
//! stored fact stages nothing.
//!
//! The second sentence is what makes the accumulator's cancellation sound.
//! It cancels a retraction against a same-transaction assertion of the same
//! fact, which implements SPARQL 1.1 Update's `(DS − D) ∪ I` only if every
//! retraction cancels a stored fact: a retraction of an absent `x` would
//! cancel an `INSERT x` in the same transaction and `x` would never be
//! written.
//!
//! The types carry the invariant. [`CurrentFacts`] is the only producer of
//! [`StoredFact`], whose fields are private, and [`StoredFact::retract`] is
//! the way staging turns one into a [`Retraction`] — the only item type
//! [`FlakeAccumulator::push_retractions`](crate::generate::FlakeAccumulator::push_retractions)
//! accepts.
//!
//! An intent names a stored fact when it holds the same term: value, exact
//! datatype, and language tag compared case-insensitively. A fact read from
//! the persisted index is also named by its index key, which keeps less
//! than some terms carry (see [`IndexKeys`]).
//!
//! ## The read
//!
//! Per `(graph, subject, predicate)` slot (or subject): one seek into the
//! persisted index through `NoOverlay` (no novelty translation, no shared
//! LRU entry), plus a Sid-space seek into novelty bounded by the slot's
//! sentinels. The ops are then resolved the way storage resolves them. Ops
//! that share an index key are one stored fact, whose newest op wins
//! ([`IndexKeys::merge`]); the rest resolve by term identity
//! ([`resolve_current_flakes`]): the newest op per fact wins and
//! retractions drop out. Index rows come back without their graph, so
//! every survivor is stamped with the slot's graph Sid.
//!
//! When a graph's novelty is small relative to the number of slots asked
//! about in it, one filtered walk over that graph's novelty answers every
//! slot at once instead (see [`NOVELTY_WALK_RATIO`]).
//!
//! Not view-policy filtered, matching the whole-graph scans of sync and
//! CLEAR: modify policy still runs on the staged flakes.

use crate::error::{Result, TransactError};
use fluree_db_core::comparator::IndexType;
use fluree_db_core::range::resolve_current_flakes;
use fluree_db_core::{
    range_with_overlay, Flake, FlakeMeta, FlakeValue, GraphId, NoOverlay, OverlayProvider,
    RangeMatch, RangeOptions, RangeTest, Sid, Tracker, DEFAULT_GRAPH_ID,
};
use fluree_db_ledger::LedgerState;
use fluree_db_query::fast_path_outcome::{stamp_fast_path, FastPathFallback, FastPathOutcome};
use fluree_db_query::BinaryRangeProvider;
use num_bigint::BigInt;
use rustc_hash::{FxHashMap, FxHasher};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// Seek-versus-walk break-even for a graph's novelty. Slots are answered by
/// one filtered walk over the graph's novelty when
/// `slots × segments × NOVELTY_WALK_RATIO > novelty flakes`, and by one
/// bounded seek per slot otherwise. A seek costs `O(segments · log N)`
/// comparisons, a walk `O(N)` hash probes; 16 puts the switch where a
/// seek's log factor and per-call overhead catch up with a probe per flake.
pub const NOVELTY_WALK_RATIO: usize = 16;

/// Facts in a slot beyond which [`DeleteResolver`] matches the slot through
/// an index and keeps it for the rest of the transaction. At or below it a
/// scan costs less than building one, and a later batch that names the slot
/// reads it again.
const NARROW_SLOT: usize = 16;

/// What reading one value of a slot from the index costs, in slot facts: a
/// value read is a dictionary lookup of the value and a seek, a slot read
/// the seek and a decode per fact. Measured 2026-10-08 over deletes naming
/// `m` values of one slot of `k` values; where the values lie decides it:
/// - close together (each read forced, `m` up to `k / 2`, `k` = 1,000 to
///   100,000): value reads cost as much as the slot read at `m` = `k / 12.2`
///   to `k / 13.5`, in instructions and in wall time alike;
/// - spread over the whole slot (every seventh value, `k` = 20,000 and
///   40,000, wall time): at `m` = `k / 19` to `k / 21`.
///
/// 16 sits between the two. At the bound, value reads cost 6 to 7% less
/// than the slot read when the values lie together and 5 to 7% more when
/// they are spread; 20 would make the spread case even and cost up to 12%
/// on the other. At `k <= 4` the slot read costs less for every `m`
/// (2026-10-07).
const POINT_READ_FACTS: u64 = 16;

/// The routing stamp of [`DeleteResolver`]'s value reads: `proceed` when a
/// batch read some slot by the values its intents name, else
/// `fallback:gate_declined`.
pub const DELETE_POINT_READ_SITE: &str = "delete_point_read";

/// Where a stored fact's surviving op was read from.
///
/// Index rows are decodes, and some decodes are lossy — a big integer's
/// declared XSD subtype is not stored, so it decodes as `xsd:integer`. The
/// origin tells the resolver which facts it may name by index key
/// ([`IndexKeys`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// The persisted index.
    Index,
    /// Novelty (commits since the last index).
    Novelty,
}

/// A fact as currently stored in its graph, graph Sid stamped (`None` for
/// the default graph). Built only by [`CurrentFacts`].
#[derive(Clone, Debug)]
pub struct StoredFact {
    flake: Flake,
    origin: Origin,
}

impl StoredFact {
    /// The stored fact (an assertion; `t` is the transaction that wrote it).
    pub fn flake(&self) -> &Flake {
        &self.flake
    }

    /// Where the fact was read from.
    pub fn origin(&self) -> Origin {
        self.origin
    }

    /// Retract this fact at `t`. The one way staging retracts a stored fact.
    pub fn retract(self, t: i64) -> Retraction {
        Retraction(Flake {
            t,
            op: false,
            ..self.flake
        })
    }

    /// [`Self::retract`] for a DELETE intent that named this fact: when the
    /// intent holds the stored value written alike, its value moves into the
    /// retraction, which takes everything else from the fact; otherwise the
    /// fact is copied.
    fn retract_named(&self, intent: Flake, t: i64) -> Retraction {
        let f = &self.flake;
        if !written_alike(&f.o, &intent.o) {
            return self.clone().retract(t);
        }
        Retraction(Flake {
            g: f.g.clone(),
            s: f.s.clone(),
            p: f.p.clone(),
            o: intent.o,
            dt: f.dt.clone(),
            t,
            op: false,
            m: f.m.clone(),
        })
    }
}

/// Whether two equal values are also written alike, so either stands for the
/// other in a flake. For these kinds equality is identity; numbers compare
/// across kind and scale, and temporals as instants, so those are copied.
fn written_alike(a: &FlakeValue, b: &FlakeValue) -> bool {
    match (a, b) {
        (FlakeValue::String(x), FlakeValue::String(y)) => x == y,
        (FlakeValue::Ref(x), FlakeValue::Ref(y)) => x == y,
        (FlakeValue::Long(x), FlakeValue::Long(y)) => x == y,
        (FlakeValue::Boolean(x), FlakeValue::Boolean(y)) => x == y,
        _ => false,
    }
}

/// A retraction of a currently stored fact.
///
/// The field is private to this module: a `Retraction` comes from
/// [`StoredFact::retract`] or from a WHERE row proven to be the decode of a
/// stored fact (see [`DeleteResolver`]), and from nowhere else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retraction(Flake);

impl Retraction {
    /// The retraction flake.
    pub fn flake(&self) -> &Flake {
        &self.0
    }

    /// Unwrap into the retraction flake, for the staged flake set.
    pub fn into_flake(self) -> Flake {
        self.0
    }

    /// Any flake, for unit tests of the retraction consumers.
    #[cfg(test)]
    pub(crate) fn for_test(flake: Flake) -> Self {
        Retraction(flake)
    }
}

/// What one DELETE template instantiation asks to retract: the instantiated
/// `(g, s, p, o, dt, m)` at the transaction's `t`. Not a retraction —
/// [`DeleteResolver::resolve`] turns it into one only if it names a stored
/// fact, or it is a WHERE row proven to be the decode of one.
#[derive(Clone, Debug)]
pub(crate) struct RetractIntent {
    flake: Flake,
    /// The template is witnessed by a WHERE triple (`delete_witness`), so
    /// the row is the decode of a fact that WHERE matched at the same `t`.
    witnessed: bool,
}

impl RetractIntent {
    pub(crate) fn new(flake: Flake, witnessed: bool) -> Self {
        debug_assert!(!flake.op, "a retraction intent is op = false");
        Self { flake, witnessed }
    }

    #[cfg(test)]
    pub(crate) fn flake(&self) -> &Flake {
        &self.flake
    }
}

/// Proof that no `@list` position is stored anywhere in the ledger: the
/// persisted index recorded none (`Some(false)` is an exact observation by
/// the indexer; `None`, a legacy root or a bulk import, proves nothing) and
/// novelty holds none. Only [`ListFree::check`] makes one.
pub(crate) struct ListFree {
    /// Novelty holds a term whose index key keeps less than the term
    /// ([`FlakeValue::index_key_loses_detail`](fluree_db_core::FlakeValue::index_key_loses_detail)):
    /// a WHERE row of such a kind may be its decode rather than the stored
    /// term.
    lossy_terms: bool,
}

impl ListFree {
    pub(crate) fn check(ledger: &LedgerState) -> Option<ListFree> {
        (ledger.snapshot.has_list_meta == Some(false) && !ledger.novelty.has_list_meta).then(|| {
            ListFree {
                lossy_terms: ledger.novelty.has_lossy_index_terms,
            }
        })
    }

    /// Whether witnessed `row` is the stored fact it decodes, so it can be
    /// retracted as decoded with no read: no list position can be stored,
    /// and the row is not of a kind whose decode can differ from a stored
    /// term novelty holds.
    fn row_is_stored(&self, row: &Flake) -> bool {
        !(self.lossy_terms && row.o.index_key_may_merge_terms())
    }
}

/// What resolving a batch of DELETE intents did, for tracing.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ResolveStats {
    /// Witnessed rows retracted as decoded, with no read.
    pub(crate) witnessed: usize,
    /// Witnessed rows that read their slot, matched to the stored fact they
    /// decode: on a list-bearing ledger, or of a kind whose decode may differ
    /// from a term novelty holds.
    pub(crate) enriched: usize,
    /// Intents matched to a stored fact.
    pub(crate) matched: usize,
    /// Intents that named no stored fact and staged nothing.
    pub(crate) phantom_intents: usize,
    /// `(graph, subject, predicate)` slots read.
    pub(crate) slot_reads: usize,
    /// Slots read only for the values their intents name.
    pub(crate) point_reads: usize,
}

impl ResolveStats {
    pub(crate) fn add(&mut self, other: ResolveStats) {
        self.witnessed += other.witnessed;
        self.enriched += other.enriched;
        self.matched += other.matched;
        self.phantom_intents += other.phantom_intents;
        self.slot_reads += other.slot_reads;
        self.point_reads += other.point_reads;
    }
}

/// Datatype identity for a DELETE term: exact. A DELETE names a term, and
/// RDF term identity includes the datatype, so `DELETE DATA { … 1 }` does
/// not remove `"1"^^xsd:int`. The one place this rule lives.
fn dt_matches(stored: &Sid, intent: &Sid) -> bool {
    stored == intent
}

/// Language tags compare case-insensitively (RDF 1.1): the retraction then
/// carries the stored tag verbatim, which is what a pre-normalization tag
/// in the index needs.
fn lang_matches(stored: Option<&str>, intent: Option<&str>) -> bool {
    match (stored, intent) {
        (None, None) => true,
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        _ => false,
    }
}

fn lang_of(f: &Flake) -> Option<&str> {
    f.m.as_ref().and_then(|m| m.lang.as_deref())
}

fn list_index_of(f: &Flake) -> Option<i32> {
    f.m.as_ref().and_then(|m| m.i)
}

/// The same RDF term: value, datatype and language tag.
fn same_term(stored: &Flake, intent: &Flake) -> bool {
    stored.o == intent.o
        && dt_matches(&stored.dt, &intent.dt)
        && lang_matches(lang_of(stored), lang_of(intent))
}

/// Whether `a` and `b` hold one big integer, whatever XSD subtype each
/// carries: storage holds it as one fact per value ([`FactIdentity`]).
fn same_value(a: &Flake, b: &Flake) -> bool {
    a.o.index_keys_by_value() && a.o == b.o
}

/// The hash a big integer is grouped by value with ([`same_value`]); `None`
/// for any other value.
fn value_hash(f: &Flake) -> Option<u64> {
    if !f.o.index_keys_by_value() {
        return None;
    }
    let mut h = FxHasher::default();
    f.o.hash(&mut h);
    Some(h.finish())
}

/// Whether every stored fact a DELETE intent can name is found by reading
/// the intent's value alone: an IRI or blank-node reference, or a plain
/// `xsd:string` with no tag. The index keys either by one dictionary id
/// (`IRI_REF`, which blank nodes share, or `XSD_STRING`), and one key is
/// one term, so a range read bound to the value returns exactly the facts
/// that hold it. Other kinds read their slot: a tagged or otherwise typed
/// string has its own key type per tag or datatype, a bare number spans
/// numeric types, and the kinds whose key can merge terms
/// ([`IndexKeys`]) need the slot's keys.
fn point_keyable(f: &Flake) -> bool {
    match &f.o {
        FlakeValue::Ref(_) => true,
        FlakeValue::String(_) => lang_of(f).is_none() && f.dt == *crate::generate::DT_STRING,
        _ => false,
    }
}

/// A hash of a flake's term that agrees with [`same_term`]: `FlakeValue`'s
/// `Hash` agrees with its `Eq`, and the tag is hashed lowercased. In memory
/// only; a collision costs a comparison, never a wrong match.
fn term_hash(f: &Flake) -> u64 {
    let mut h = FxHasher::default();
    f.o.hash(&mut h);
    f.dt.hash(&mut h);
    match lang_of(f) {
        None => h.write_u8(0),
        Some(lang) => {
            h.write_u8(1);
            for b in lang.bytes() {
                h.write_u8(b.to_ascii_lowercase());
            }
        }
    }
    h.finish()
}

/// A fact's persisted index key, `(o_type, o_key)`.
type IndexKey = (u16, u64);

/// Index keys, for the kinds of value whose key two terms can share.
///
/// The index keeps less than some terms carry: a big integer keeps no XSD
/// subtype. (A `dateTime` or `time` is keyed to the microsecond, which is
/// all it holds; see `fluree_db_core::DateTime`.) Terms with one index
/// key are one stored fact to storage, which merges every op by that key,
/// in the index and in queries over novelty alike (`resolve_overlay_ops`).
/// So an indexed fact's decode can differ from the term that was written,
/// and from the term a query row carries for it, and an op on one term can
/// retract a fact stored as another.
///
/// Only the kinds
/// [`FlakeValue::index_key_may_merge_terms`](fluree_db_core::FlakeValue::index_key_may_merge_terms)
/// names can do that; for every other kind one term is one key. Keys are
/// computed for those kinds alone, so a slot of strings, IRIs or integers
/// never pays for one. With no index there are no keys, and storage
/// resolves novelty by term too.
struct IndexKeys<'l> {
    provider: Option<&'l BinaryRangeProvider>,
}

impl<'l> IndexKeys<'l> {
    fn new(ledger: &'l LedgerState) -> Self {
        Self {
            provider: ledger
                .snapshot
                .range_provider
                .as_ref()
                .and_then(|rp| rp.as_any().downcast_ref::<BinaryRangeProvider>()),
        }
    }

    /// The persisted id of predicate `p`, for [`Self::key`].
    fn p_id(&self, p: &Sid) -> Option<u32> {
        self.provider?.persisted_p_id(p)
    }

    /// `f`'s index key in graph `g_id` under predicate `p_id` (`f.p`'s
    /// persisted id, if any), when its kind of value can share one with
    /// another term and the index can key it. A value no persisted dictionary
    /// holds has none: storage resolves it by term as well.
    fn key(&self, g_id: GraphId, p_id: Option<u32>, f: &Flake) -> Option<IndexKey> {
        if !f.o.index_key_may_merge_terms() {
            return None;
        }
        self.provider?.persisted_object_key_in(g_id, p_id, f)
    }

    /// Merge the ops of one read that share an index key, the way storage
    /// does: per predicate, key and list position the newest op wins, and on
    /// one `t` the retraction; the others are dropped, so the term fold that
    /// follows sees one op per key. Index rows are one per key already (the
    /// index merged them), so a read changes only when it holds a novelty op
    /// whose kind of value can share a key; any other read is returned as
    /// is, with no key computed.
    fn merge(&self, g_id: GraphId, flakes: Vec<Flake>, index_t: i64) -> Vec<Flake> {
        if self.provider.is_none()
            || !flakes
                .iter()
                .any(|f| f.t > index_t && f.o.index_key_may_merge_terms())
        {
            return flakes;
        }
        let dropped = {
            let mut p_ids: HashMap<&Sid, Option<u32>> = HashMap::new();
            let mut winner: HashMap<(&Sid, IndexKey, Option<i32>), usize> = HashMap::new();
            let mut dropped = vec![false; flakes.len()];
            for (n, f) in flakes.iter().enumerate() {
                if !f.o.index_key_may_merge_terms() {
                    continue;
                }
                let p_id = *p_ids.entry(&f.p).or_insert_with(|| self.p_id(&f.p));
                let Some(key) = self.key(g_id, p_id, f) else {
                    continue;
                };
                match winner.entry((&f.p, key, list_index_of(f))) {
                    Entry::Vacant(e) => {
                        e.insert(n);
                    }
                    Entry::Occupied(mut e) => {
                        let best = *e.get();
                        if newer(f, &flakes[best]) {
                            dropped[best] = true;
                            e.insert(n);
                        } else {
                            dropped[n] = true;
                        }
                    }
                }
            }
            dropped
        };
        if !dropped.contains(&true) {
            return flakes;
        }
        flakes
            .into_iter()
            .zip(dropped)
            .filter_map(|(f, dropped)| (!dropped).then_some(f))
            .collect()
    }
}

/// Storage's rule for two ops on one fact (`resolve_overlay_ops`,
/// [`resolve_current_flakes`]): the higher `t` wins, and on one `t` the
/// retraction.
fn newer(a: &Flake, b: &Flake) -> bool {
    a.t > b.t || (a.t == b.t && !a.op && b.op)
}

/// How storage tells two facts of one `(graph, subject, predicate)` apart.
///
/// A value whose index key can merge terms ([`IndexKeys`]) is one fact per
/// key and list position, whatever its term, when it has a key
/// ([`FactKey`]): storage merges every op by that key, in the index and in
/// queries over novelty. Any other value is its term (value, datatype, and
/// metadata: language tag and list position), as [`Flake`]'s equality has
/// it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FactTerm {
    /// The term of a value storage tells by term.
    Term(FlakeValue, Sid, Option<FlakeMeta>),
    /// The key and list position of a value storage tells by key.
    Key(FactKey, Option<i32>),
}

/// The key storage merges the ops of a value by, for the kinds of value
/// whose key two terms can share ([`IndexKeys`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FactKey {
    /// An integer too large for 64 bits, by value. The index keys one by its
    /// `(graph, predicate)` arena's handle for the value, whatever XSD
    /// subtype it was written with, so the value is its key before the arena
    /// holds it as well as after: a value first written since the last index
    /// build is merged by value at the next.
    BigInt(Box<BigInt>),
    /// A persisted index key `(o_type, o_key)`.
    Index((u16, u64)),
}

/// A [`FactKey`] and list position: how storage tells apart the facts of one
/// `(graph, subject, predicate)` whose kind of value can merge terms
/// ([`FactTerm::Key`]).
pub type MergeKey = (FactKey, Option<i32>);

/// The identity storage gives each fact of one ledger state ([`FactTerm`]).
///
/// One rule decides when a retraction and an assertion are the same fact:
/// within one operation (the flake accumulator), across the operations of
/// one request (the sequential fold), and against what the state stores
/// ([`CurrentFacts::named_by`]). Storage resolves a retraction and an
/// assertion of one fact at one `t` as the retraction, so wherever the two
/// should cancel they must be told apart by this identity, not by term.
pub struct FactIdentity<'l> {
    ledger: &'l LedgerState,
    keys: IndexKeys<'l>,
}

impl<'l> FactIdentity<'l> {
    /// Fact identities in `ledger`: index keys from its persisted index (none
    /// without one), graph ids from its registry.
    pub fn new(ledger: &'l LedgerState) -> Self {
        Self {
            ledger,
            keys: IndexKeys::new(ledger),
        }
    }

    /// The id the state gives graph `g`; see [`graph_id_in`].
    pub fn graph_id(&self, g: &Option<Sid>) -> Option<GraphId> {
        graph_id_in(self.ledger, g)
    }

    /// `flake`'s key and list position in graph `g_id` (`None` for a graph
    /// the state does not have, which holds nothing indexed), when storage
    /// tells it by key ([`FactTerm::Key`]).
    pub fn key(&self, g_id: Option<GraphId>, flake: &Flake) -> Option<MergeKey> {
        self.key_under(|| g_id, || self.keys.p_id(&flake.p), flake)
    }

    /// [`Self::key`], with the graph id from `g_id` and `flake.p`'s persisted
    /// id from `p_id`, each asked only for an index key: most values have
    /// none, and a caller keying many facts of one predicate looks it up
    /// once.
    fn key_under(
        &self,
        g_id: impl FnOnce() -> Option<GraphId>,
        p_id: impl FnOnce() -> Option<u32>,
        flake: &Flake,
    ) -> Option<MergeKey> {
        if !flake.o.index_key_may_merge_terms() {
            return None;
        }
        let key = match &flake.o {
            FlakeValue::BigInt(v) if flake.o.index_keys_by_value() => FactKey::BigInt(v.clone()),
            _ => FactKey::Index(self.keys.key(g_id()?, p_id(), flake)?),
        };
        Some((key, list_index_of(flake)))
    }

    /// `flake`'s identity in its own graph (`flake.g`). The graph is resolved
    /// only for a value with an index key.
    pub fn of_flake(&self, flake: &Flake) -> FactTerm {
        let key = self.key_under(
            || self.graph_id(&flake.g),
            || self.keys.p_id(&flake.p),
            flake,
        );
        Self::term(key, flake)
    }

    /// `flake`'s identity in graph `g_id` (`None` for a graph the state does
    /// not have), whatever graph it names: the identity a fact copied into
    /// that graph has there.
    pub fn in_graph(&self, g_id: Option<GraphId>, flake: &Flake) -> FactTerm {
        Self::term(self.key(g_id, flake), flake)
    }

    fn term(key: Option<MergeKey>, flake: &Flake) -> FactTerm {
        match key {
            Some((key, at)) => FactTerm::Key(key, at),
            None => FactTerm::Term(flake.o.clone(), flake.dt.clone(), flake.m.clone()),
        }
    }
}

/// The id `ledger` gives graph `g` (`None` is the default graph), or `None`
/// for a graph it does not have.
pub fn graph_id_in(ledger: &LedgerState, g: &Option<Sid>) -> Option<GraphId> {
    match g {
        None => Some(DEFAULT_GRAPH_ID),
        Some(g_sid) => ledger
            .snapshot
            .decode_sid(g_sid)
            .and_then(|iri| ledger.snapshot.graph_registry.graph_id_for_iri(&iri)),
    }
}

/// A `(graph, subject, predicate)` slot to read.
#[derive(Clone, Debug)]
pub struct Slot {
    /// Ledger graph id to read (0 = default graph).
    pub g_id: GraphId,
    /// Graph Sid stamped on every fact read (`None` = default graph).
    pub g_sid: Option<Sid>,
    /// Subject.
    pub s: Sid,
    /// Predicate.
    pub p: Sid,
}

impl Slot {
    /// The slot `(g, s, p)` names in `ledger`, or `None` for a graph `ledger`
    /// does not have, which holds nothing.
    pub fn in_ledger(ledger: &LedgerState, g: &Option<Sid>, s: &Sid, p: &Sid) -> Option<Self> {
        Some(Self {
            g_id: graph_id_in(ledger, g)?,
            g_sid: g.clone(),
            s: s.clone(),
            p: p.clone(),
        })
    }
}

/// Reads a ledger's current facts as of its current `t`.
///
/// Each read refuses with [`crate::TransactError::IndexNotLoaded`] when this
/// process has not loaded the ledger's index ([`crate::check_index_loaded`]):
/// merge and rebase read here before they commit.
pub struct CurrentFacts<'l> {
    ledger: &'l LedgerState,
    to_t: i64,
    keys: IndexKeys<'l>,
}

impl<'l> CurrentFacts<'l> {
    /// Current facts of `ledger` as of `ledger.t()`.
    pub fn new(ledger: &'l LedgerState) -> Self {
        Self {
            ledger,
            to_t: ledger.t(),
            keys: IndexKeys::new(ledger),
        }
    }

    /// The current facts of every slot, in slot order: one `Vec` per slot,
    /// each in SPOT order.
    ///
    /// Each slot is read on its own (callers pass distinct slots). Slots of
    /// one graph share one decision between per-slot novelty seeks and a
    /// single filtered novelty walk.
    pub async fn of_slots(&self, slots: &[Slot]) -> Result<Vec<Vec<StoredFact>>> {
        let novelty = NoveltyPlan::for_slots(self.ledger, slots, self.to_t);
        let mut out = Vec::with_capacity(slots.len());
        for slot in slots {
            out.push(self.read_slot(slot, None, &novelty).await?);
        }
        Ok(out)
    }

    /// The current fact each `(slot, flake)` names, in order: the fact of
    /// `slot` with `flake`'s identity ([`FactIdentity`]), its own term first.
    /// So a retraction written elsewhere, which may carry a term as written
    /// where the index keeps less of it, names the fact as stored. See
    /// [`Self::all_named_by`] for every term of it.
    pub async fn named_by(&self, items: &[(Slot, &Flake)]) -> Result<Vec<Option<StoredFact>>> {
        let (slots, item_slots) = distinct_slots(items);
        let facts = self.of_slots(&slots).await?;
        let ids = FactIdentity::new(self.ledger);
        let index = self.index_slots(&slots, &facts, &ids);
        Ok(items
            .iter()
            .zip(item_slots)
            .map(|((slot, flake), i)| {
                let named = index[i].term(flake).or_else(|| {
                    let key = ids.key(Some(slot.g_id), flake)?;
                    index[i].first.get(&key).copied()
                });
                named.map(|n| facts[i][n].clone())
            })
            .collect())
    }

    /// Every current fact of each `(slot, flake)`'s identity
    /// ([`FactIdentity`]), in order: `flake`'s own term first, then the
    /// slot's other terms of that fact. Novelty holds the subtypes of one big
    /// integer apart until the next index build, so one fact can be current
    /// under several terms; a retraction of the fact retracts each.
    pub async fn all_named_by(&self, items: &[(Slot, &Flake)]) -> Result<Vec<Vec<StoredFact>>> {
        let (slots, item_slots) = distinct_slots(items);
        let facts = self.of_slots(&slots).await?;
        let ids = FactIdentity::new(self.ledger);
        let index = self.index_slots(&slots, &facts, &ids);
        Ok(items
            .iter()
            .zip(item_slots)
            .map(|((slot, flake), i)| {
                let term = index[i].term(flake);
                let key = ids.key(Some(slot.g_id), flake);
                let same_key = key.iter().flat_map(|key| index[i].same_key(key));
                term.into_iter()
                    .chain(same_key.filter(|&n| Some(n) != term))
                    .map(|n| facts[i][n].clone())
                    .collect()
            })
            .collect())
    }

    /// Each slot's facts indexed by term and by identity key ([`SlotIndex`]).
    /// Each distinct slot is read once and each item matched by hash, so
    /// naming k facts of an n-fact slot costs one read of the slot and
    /// O(k + n) matching.
    fn index_slots<'f>(
        &self,
        slots: &[Slot],
        facts: &'f [Vec<StoredFact>],
        ids: &FactIdentity<'_>,
    ) -> Vec<SlotIndex<'f>> {
        slots
            .iter()
            .zip(facts)
            .map(|(slot, slot_facts)| {
                let mut slot_p_id = None;
                let mut index = SlotIndex {
                    terms: HashMap::with_capacity(slot_facts.len()),
                    first: HashMap::new(),
                    next: None,
                };
                for (n, fact) in slot_facts.iter().enumerate() {
                    index.terms.insert(&fact.flake, n);
                    if !fact.flake.o.index_key_may_merge_terms() {
                        continue;
                    }
                    let p_id = || *slot_p_id.get_or_insert_with(|| self.keys.p_id(&slot.p));
                    let Some(key) = ids.key_under(|| Some(slot.g_id), p_id, &fact.flake) else {
                        continue;
                    };
                    match index.first.entry(key) {
                        Entry::Vacant(e) => {
                            e.insert(n);
                        }
                        // Two terms of one fact: chain them. Only novelty
                        // holding one big integer under several subtypes
                        // gets here, so the chain is rarely allocated.
                        Entry::Occupied(e) => {
                            let next = index
                                .next
                                .get_or_insert_with(|| vec![usize::MAX; slot_facts.len()]);
                            let mut at = *e.get();
                            while next[at] != usize::MAX {
                                at = next[at];
                            }
                            next[at] = n;
                        }
                    }
                }
                index
            })
            .collect()
    }

    /// [`Self::of_slots`], except that a slot given `Some(values)` is read
    /// for those values alone: per value, the index rows of `(s, p, value)`
    /// and the novelty ops in its bounds. Its answer is then the slot's
    /// current facts that hold one of the values, under any datatype, tag
    /// or list position, each value's in slot order.
    async fn of_slots_or_values(
        &self,
        slots: &[Slot],
        values: &[Option<Vec<FlakeValue>>],
    ) -> Result<Vec<Vec<StoredFact>>> {
        let novelty = NoveltyPlan::for_slots(self.ledger, slots, self.to_t);
        let mut out = Vec::with_capacity(slots.len());
        for (slot, values) in slots.iter().zip(values) {
            match values {
                None => out.push(self.read_slot(slot, None, &novelty).await?),
                Some(values) => {
                    let mut facts = Vec::new();
                    for o in values {
                        facts.extend(self.read_slot(slot, Some(o), &novelty).await?);
                    }
                    out.push(facts);
                }
            }
        }
        Ok(out)
    }

    /// The current facts of one slot, or of its facts whose value is `o`.
    async fn read_slot(
        &self,
        slot: &Slot,
        o: Option<&FlakeValue>,
        novelty: &NoveltyPlan,
    ) -> Result<Vec<StoredFact>> {
        let rm = RangeMatch {
            o: o.cloned(),
            ..RangeMatch::subject_predicate(slot.s.clone(), slot.p.clone())
        };
        let mut flakes = self.base(slot.g_id, rm).await?;
        let index_rows = flakes.len();
        novelty.collect_slot(self.ledger, slot, o, self.to_t, &mut flakes);
        let index_only = flakes.len() == index_rows;
        Ok(self.resolve(slot.g_id, flakes, slot.g_sid.as_ref(), index_only))
    }

    /// The current facts of one subject in graph `g_id`, in SPOT order.
    pub async fn of_subject(
        &self,
        g_id: GraphId,
        g_sid: Option<&Sid>,
        s: &Sid,
    ) -> Result<Vec<StoredFact>> {
        let mut flakes = self
            .base(g_id, RangeMatch::new().with_subject(s.clone()))
            .await?;
        let index_rows = flakes.len();
        let lo = Flake::min_for_subject(s.clone());
        let hi = Flake::max_for_subject(s.clone());
        self.ledger.novelty.for_each_overlay_flake(
            g_id,
            IndexType::Spot,
            Some(&lo),
            Some(&hi),
            false,
            self.to_t,
            &mut |f| flakes.push(f.clone()),
        );
        let index_only = flakes.len() == index_rows;
        Ok(self.resolve(g_id, flakes, g_sid, index_only))
    }

    /// Every current fact of graph `g_id`, graph Sid stamped.
    ///
    /// The whole-graph verbs (graph sync, CLEAR, DROP, COPY, MOVE) read
    /// through this. `limit` bounds what is materialized, not just what is
    /// returned: the provider's drain loop stops at `limit + 1` and the scan
    /// then fails with [`TransactError::WholeGraphScanTooLarge`].
    pub async fn whole_graph(
        &self,
        g_id: GraphId,
        g_sid: Option<&Sid>,
        limit: Option<usize>,
        tracker: Option<&Tracker>,
    ) -> Result<Vec<StoredFact>> {
        crate::check_index_loaded(&self.ledger.snapshot)?;
        let db_ref = match tracker {
            Some(t) => self.ledger.as_graph_db_ref(g_id).with_tracker(t),
            None => self.ledger.as_graph_db_ref(g_id),
        };
        // `Eq` with an empty match is the whole-graph scan on both range
        // paths: the V3 provider treats "nothing bound" as a full-index
        // cursor and rejects every other `RangeTest`, and the genesis
        // (overlay-only) path matches an empty `Eq` against every flake.
        let opts = RangeOptions {
            flake_limit: limit.map(|l| l.saturating_add(1)),
            ..Default::default()
        };
        let flakes = db_ref
            .range_with_opts(IndexType::Spot, RangeTest::Eq, RangeMatch::new(), opts)
            .await
            .map_err(|e| TransactError::FlakeGeneration(format!("graph scan failed: {e}")))?;
        if let Some(l) = limit {
            if flakes.len() > l {
                return Err(TransactError::WholeGraphScanTooLarge { limit: l });
            }
        }
        // Already lifecycle-resolved by the range read, which merges novelty
        // into the index by key.
        let index_t = self.ledger.snapshot.t;
        Ok(flakes
            .into_iter()
            .map(|flake| stored(flake, g_sid, index_t))
            .collect())
    }

    /// Persisted-index rows matching `rm` in graph `g_id`, read through
    /// `NoOverlay`: one leaf seek, no novelty translation. At genesis (no
    /// index yet) there is no provider and the answer is empty.
    ///
    /// Every reader that reads the index reads it here, so this is where an
    /// index this process has not loaded is refused.
    async fn base(&self, g_id: GraphId, rm: RangeMatch) -> Result<Vec<Flake>> {
        crate::check_index_loaded(&self.ledger.snapshot)?;
        Ok(range_with_overlay(
            &self.ledger.snapshot,
            g_id,
            &NoOverlay,
            IndexType::Spot,
            RangeTest::Eq,
            rm,
            RangeOptions::new().with_to_t(self.to_t),
        )
        .await?)
    }

    /// Resolve base rows plus novelty ops of graph `g_id` to the current
    /// facts, stamping the graph and the origin: ops that share an index key
    /// first ([`IndexKeys::merge`]), then by term.
    ///
    /// Index rows alone (`index_only`, every one an assertion) are already
    /// the index's current facts, one row each in SPOT order, as a range read
    /// returns them ([`Self::whole_graph`] relies on the same): there is
    /// nothing to fold.
    fn resolve(
        &self,
        g_id: GraphId,
        flakes: Vec<Flake>,
        g_sid: Option<&Sid>,
        index_only: bool,
    ) -> Vec<StoredFact> {
        if flakes.is_empty() {
            return Vec::new();
        }
        let index_t = self.ledger.snapshot.t;
        let flakes = if index_only && flakes.iter().all(|f| f.op) {
            flakes
        } else {
            let flakes = self.keys.merge(g_id, flakes, index_t);
            resolve_current_flakes(flakes, IndexType::Spot)
        };
        flakes
            .into_iter()
            .map(|flake| stored(flake, g_sid, index_t))
            .collect()
    }
}

/// The distinct slots of `items`, in first-seen order, and each item's
/// place among them.
fn distinct_slots(items: &[(Slot, &Flake)]) -> (Vec<Slot>, Vec<usize>) {
    let mut slot_index: HashMap<(GraphId, &Sid, &Sid), usize> = HashMap::new();
    let mut slots: Vec<Slot> = Vec::new();
    let item_slots = items
        .iter()
        .map(|(slot, _)| {
            *slot_index
                .entry((slot.g_id, &slot.s, &slot.p))
                .or_insert_with(|| {
                    slots.push(slot.clone());
                    slots.len() - 1
                })
        })
        .collect();
    (slots, item_slots)
}

/// One slot's current facts indexed for naming: by term, and by identity
/// key ([`MergeKey`]) through the first fact holding each key and, when
/// several terms of one fact are current, a chain to the next.
struct SlotIndex<'f> {
    terms: HashMap<&'f Flake, usize>,
    first: HashMap<MergeKey, usize>,
    next: Option<Vec<usize>>,
}

impl SlotIndex<'_> {
    /// The fact holding `flake`'s term exactly.
    fn term(&self, flake: &Flake) -> Option<usize> {
        self.terms.get(flake).copied()
    }

    /// Every fact with identity key `key`, in slot order.
    fn same_key<'a>(&'a self, key: &MergeKey) -> impl Iterator<Item = usize> + 'a {
        std::iter::successors(self.first.get(key).copied(), move |&n| {
            self.next
                .as_ref()
                .map(|next| next[n])
                .filter(|&m| m != usize::MAX)
        })
    }
}

/// Wrap a resolved flake: stamp the graph (index rows decode with `g:
/// None`, whatever graph they live in) and record where it came from.
/// Novelty holds only commits after the index, so a fact whose surviving op
/// is at or below the index `t` was read from the index.
fn stored(mut flake: Flake, g_sid: Option<&Sid>, index_t: i64) -> StoredFact {
    flake.g = g_sid.cloned();
    let origin = if flake.t <= index_t {
        Origin::Index
    } else {
        Origin::Novelty
    };
    StoredFact { flake, origin }
}

/// A slot, as the resolver keys it: `(graph, subject, predicate)`.
type SlotKey = (GraphId, Sid, Sid);

/// Resolves a transaction's DELETE intents to retractions, one WHERE batch
/// at a time.
///
/// - A witnessed row on a list-free ledger ([`ListFree`]) is retracted as
///   decoded, with no read: the WHERE just matched that stored fact. Unless
///   novelty holds a term its index key keeps less of, and the row is of a
///   kind that can be such a term's decode: then the row reads its slot as
///   on a list-bearing ledger, and names the stored term.
/// - A witnessed row on a list-bearing ledger reads its slot for what a
///   WHERE binding does not carry, the list position. It is matched to the
///   stored facts its decode can be: those with the same index key when its
///   kind of value can share one ([`IndexKeys`]), else those with the same
///   term; list entries in slot order, then the plain value. The k-th
///   identical row of a slot, across batches, takes the k-th of them, so a
///   value stored at several positions loses one per row that matched it;
///   a row beyond that (a join repeating rows) takes the last again, which
///   the accumulator collapses. The fact is retracted as read; a row that
///   matches none is retracted as decoded.
/// - Every other intent — a template constant, `DELETE DATA`, a value bound
///   by another pattern — is matched against the stored facts of its slot:
///   by term, else by the fact's identity ([`FactIdentity`]), whichever op
///   of it survived, read from the index or from novelty: a big integer by
///   value, whatever XSD subtype each term carries, and another kind whose
///   key can merge terms by its persisted index key. Novelty holds the terms
///   of one big integer apart until the next index build, so the intent
///   retracts every term of the fact it names, not just one. An intent
///   with a list position names that position. One without names the first
///   list entry holding the term, in slot order, else the plain value: the
///   contract hydration had, under which a value asserted at N list
///   positions loses exactly one entry per distinct DELETE row. The fact is
///   retracted as read. An intent that names no stored fact stages
///   nothing; with `unmatched`, it is also appended there as instantiated,
///   for the caller's modify-policy check.
///
/// A slot with more than [`NARROW_SLOT`] facts is read once per
/// transaction and matched through an index ([`SlotFacts`]), so retracting
/// n values of one slot costs O(n) however the WHERE batches its rows. Such
/// slots are held until staging ends; narrower ones are scanned and read
/// again by a later batch that names them.
///
/// A slot that a batch's intents name a few times, every one of them not
/// witnessed and [`point_keyable`], is read only for the values they name,
/// so naming one value of a k-value slot costs a seek, not a read of k
/// facts. "A few" is what the slot's expected width pays for
/// ([`DeleteResolver::point_bound`]). Those facts are every stored fact the
/// intents can name, in slot order, so matching gives the answer the slot
/// would; they are not the slot, so they are dropped with the batch.
/// Witnessed rows read their slot: the k-th identical row is counted
/// against the slot's facts.
pub(crate) struct DeleteResolver<'l> {
    current: CurrentFacts<'l>,
    list_free: Option<ListFree>,
    /// Wide slots read so far, with their keys, and where each key's slot
    /// is in them.
    wide: Vec<(SlotKey, SlotFacts)>,
    wide_index: HashMap<SlotKey, usize>,
    /// Witnessed rows matched so far, by slot and by the first stored fact
    /// of the facts they can be.
    taken: HashMap<(SlotKey, u32), usize>,
    /// [`NARROW_SLOT`], or another bound for tests.
    narrow_slot: usize,
    /// A fixed [`Self::point_bound`] for tests (`Some(0)`: never).
    point_bound_override: Option<usize>,
}

impl<'l> DeleteResolver<'l> {
    /// A resolver over `ledger`'s current facts; `list_free` from
    /// [`ListFree::check`].
    pub(crate) fn new(ledger: &'l LedgerState, list_free: Option<ListFree>) -> Self {
        Self {
            current: CurrentFacts::new(ledger),
            list_free,
            wide: Vec::new(),
            wide_index: HashMap::new(),
            taken: HashMap::new(),
            narrow_slot: NARROW_SLOT,
            point_bound_override: None,
        }
    }

    /// For each slot this batch reads (`at`'s `Read` places), the values
    /// to read it for, or `None` to read the slot (see [`DeleteResolver`]).
    fn point_values(
        &self,
        pending: &[RetractIntent],
        at: &[SlotAt],
        slots: &[Slot],
    ) -> Vec<Option<Vec<FlakeValue>>> {
        let mut named = vec![0usize; slots.len()];
        let mut keyable = vec![true; slots.len()];
        for (intent, place) in pending.iter().zip(at) {
            if let SlotAt::Read(i) = *place {
                named[i] += 1;
                keyable[i] &= !intent.witnessed && point_keyable(&intent.flake);
            }
        }
        // One bound per graph and predicate; slots of one predicate tend to
        // come in runs, so the previous slot's is checked before hashing.
        let mut bounds: HashMap<(GraphId, &Sid), usize> = HashMap::new();
        let mut previous: Option<(GraphId, &Sid, usize)> = None;
        let mut any = false;
        let mut values: Vec<Option<Vec<FlakeValue>>> = slots
            .iter()
            .zip(named.iter().zip(&keyable))
            .map(|(slot, (&named, &keyable))| {
                if !keyable {
                    return None;
                }
                let bound = match previous {
                    Some((g_id, p, bound)) if g_id == slot.g_id && *p == slot.p => bound,
                    _ => {
                        let bound = *bounds
                            .entry((slot.g_id, &slot.p))
                            .or_insert_with(|| self.point_bound(slot.g_id, &slot.p));
                        previous = Some((slot.g_id, &slot.p, bound));
                        bound
                    }
                };
                any |= named <= bound;
                (named <= bound).then(Vec::new)
            })
            .collect();
        if !any {
            return values;
        }
        // Each slot's distinct values, in the order its intents name them.
        let mut seen: HashSet<(usize, &FlakeValue)> = HashSet::new();
        for (intent, place) in pending.iter().zip(at) {
            if let SlotAt::Read(i) = *place {
                if let Some(slot_values) = &mut values[i] {
                    if seen.insert((i, &intent.flake.o)) {
                        slot_values.push(intent.flake.o.clone());
                    }
                }
            }
        }
        values
    }

    /// How many intents naming one slot of predicate `p` in graph `g_id`
    /// are read by value rather than as the slot ([`POINT_READ_FACTS`]):
    /// as many as the slot's expected width pays for, from the index's
    /// statistics for `p` in the graph (current facts over distinct
    /// subjects). With no index row of `p` possible, a value read is a
    /// narrower seek into novelty than the slot's, and one intent takes it.
    /// With an index but no statistics for `p`, the slot is read. The width
    /// is the predicate's average over its subjects, so one wide subject
    /// among narrow ones reads its slot and the reverse reads by value: the
    /// choice costs time when it is wrong, never results.
    fn point_bound(&self, g_id: GraphId, p: &Sid) -> usize {
        if let Some(bound) = self.point_bound_override {
            return bound;
        }
        let Some(p_id) = self.current.keys.p_id(p) else {
            return 1;
        };
        let width = self
            .current
            .ledger
            .snapshot
            .stats
            .as_ref()
            .and_then(|stats| stats.graphs.as_ref())
            .and_then(|graphs| graphs.iter().find(|graph| graph.g_id == g_id))
            .and_then(|graph| graph.properties.iter().find(|prop| prop.p_id == p_id))
            .map_or(0, |prop| prop.count / prop.ndv_subjects.max(1));
        usize::try_from(width / POINT_READ_FACTS).unwrap_or(usize::MAX)
    }

    /// Resolve one batch of DELETE intents (see [`DeleteResolver`]).
    /// `graph_of` gives the ledger graph id of an intent's graph Sid.
    pub(crate) async fn resolve(
        &mut self,
        intents: Vec<RetractIntent>,
        graph_of: impl Fn(&Flake) -> Result<GraphId>,
        mut unmatched: Option<&mut Vec<Flake>>,
    ) -> Result<(Vec<Retraction>, ResolveStats)> {
        let mut stats = ResolveStats::default();
        let mut out = Vec::with_capacity(intents.len());
        let mut pending = Vec::new();
        for intent in intents {
            if intent.witnessed
                && self
                    .list_free
                    .as_ref()
                    .is_some_and(|proof| proof.row_is_stored(&intent.flake))
            {
                stats.witnessed += 1;
                out.push(Retraction(intent.flake));
            } else {
                pending.push(intent);
            }
        }
        if pending.is_empty() {
            return Ok((out, stats));
        }

        // Each intent's slot: a wide slot an earlier batch read, or one read
        // now. One hash per intent, as many as the read itself takes.
        let mut read_index: HashMap<SlotKey, usize> = HashMap::new();
        let mut to_read: Vec<Slot> = Vec::new();
        let mut at: Vec<SlotAt> = Vec::with_capacity(pending.len());
        // The previous intent's graph and place: a run of intents naming one
        // slot (a delete of many values of one property) is placed without
        // hashing its key again.
        let mut previous: Option<(GraphId, &Flake, SlotAt)> = None;
        for intent in &pending {
            let f = &intent.flake;
            let g_id = graph_of(f)?;
            if let Some((prev_g, prev, place)) = previous {
                if prev_g == g_id && prev.s == f.s && prev.p == f.p {
                    at.push(place);
                    continue;
                }
            }
            let key: SlotKey = (g_id, f.s.clone(), f.p.clone());
            let wide = if self.wide_index.is_empty() {
                None
            } else {
                self.wide_index.get(&key).copied()
            };
            let place = match wide {
                Some(w) => SlotAt::Wide(w),
                None => {
                    let next = to_read.len();
                    match read_index.entry(key) {
                        Entry::Occupied(e) => SlotAt::Read(*e.get()),
                        Entry::Vacant(e) => {
                            to_read.push(Slot {
                                g_id,
                                g_sid: f.g.clone(),
                                s: f.s.clone(),
                                p: f.p.clone(),
                            });
                            e.insert(next);
                            SlotAt::Read(next)
                        }
                    }
                }
            };
            at.push(place);
            previous = Some((g_id, f, place));
        }
        let values = self.point_values(&pending, &at, &to_read);
        stats.point_reads = values.iter().filter(|v| v.is_some()).count();
        stats.slot_reads = to_read.len() - stats.point_reads;
        if !to_read.is_empty() {
            stamp_fast_path(
                DELETE_POINT_READ_SITE,
                if stats.point_reads > 0 {
                    FastPathOutcome::Proceed
                } else {
                    FastPathOutcome::Fallback(FastPathFallback::GateDeclined)
                },
            );
        }
        let read_facts = self.current.of_slots_or_values(&to_read, &values).await?;
        let mut read: Vec<ReadSlot> = Vec::with_capacity(to_read.len());
        let mut narrow: Vec<(SlotKey, SlotFacts)> = Vec::with_capacity(to_read.len());
        for ((slot, facts), by_value) in to_read.into_iter().zip(read_facts).zip(&values) {
            let facts = SlotFacts::new(slot.g_id, slot.p.clone(), facts, self.narrow_slot);
            let key = (slot.g_id, slot.s, slot.p);
            // Facts read by value are not the slot: they stay with the batch.
            if by_value.is_none() && facts.is_wide() {
                let w = self.wide.len();
                self.wide_index.insert(key.clone(), w);
                self.wide.push((key, facts));
                read.push(ReadSlot::Wide(w));
            } else {
                read.push(ReadSlot::Narrow(narrow.len()));
                narrow.push((key, facts));
            }
        }

        let keys = &self.current.keys;
        for (intent, at) in pending.into_iter().zip(at) {
            let (slot, facts) = match at {
                SlotAt::Wide(w) => {
                    let (key, facts) = &mut self.wide[w];
                    (&*key, facts)
                }
                SlotAt::Read(i) => match read[i] {
                    ReadSlot::Narrow(n) => {
                        let (key, facts) = &mut narrow[n];
                        (&*key, facts)
                    }
                    ReadSlot::Wide(w) => {
                        let (key, facts) = &mut self.wide[w];
                        (&*key, facts)
                    }
                },
            };
            let t = intent.flake.t;
            if intent.witnessed {
                // A row that carries its list position names it already.
                let matched = if list_index_of(&intent.flake).is_none() {
                    let taken = &mut self.taken;
                    facts.witnessed(&intent.flake, keys, |first| {
                        let seen = taken.entry((slot.clone(), first)).or_insert(0);
                        *seen += 1;
                        *seen - 1
                    })
                } else {
                    None
                };
                match matched {
                    // The row holds the stored term: it takes the fact's tag
                    // and list position, with no copy of its value.
                    Some(n) if same_term(&facts.facts[n].flake, &intent.flake) => {
                        stats.enriched += 1;
                        let mut flake = intent.flake;
                        flake.m.clone_from(&facts.facts[n].flake.m);
                        out.push(Retraction(flake));
                    }
                    Some(n) => {
                        stats.enriched += 1;
                        out.push(facts.facts[n].clone().retract(t));
                    }
                    None => out.push(Retraction(intent.flake)),
                }
            } else if let Some(n) = facts.named(&intent.flake, keys) {
                stats.matched += 1;
                for m in facts.same_fact(n) {
                    out.push(facts.facts[m].clone().retract(t));
                }
                out.push(facts.facts[n].retract_named(intent.flake, t));
            } else {
                stats.phantom_intents += 1;
                if let Some(unmatched) = unmatched.as_deref_mut() {
                    unmatched.push(intent.flake);
                }
            }
        }
        Ok((out, stats))
    }
}

/// Where an intent's slot is: a wide slot an earlier batch read, by its
/// place in [`DeleteResolver`]'s wide slots, or a slot this batch reads.
#[derive(Clone, Copy)]
enum SlotAt {
    Wide(usize),
    Read(usize),
}

/// A slot this batch read: narrow, kept for the batch, or wide, moved to the
/// resolver's wide slots; each by its place.
#[derive(Clone, Copy)]
enum ReadSlot {
    Narrow(usize),
    Wide(usize),
}

/// The current facts of one slot, with what matching intents against them
/// needs. A wide slot (more than the resolver's narrow bound) is grouped by
/// term when read, and by index key when an intent first needs keys; a
/// narrow one is scanned.
struct SlotFacts {
    g_id: GraphId,
    /// The slot's predicate.
    p: Sid,
    /// Its persisted id, for index keys, looked up on first need.
    p_id: Option<Option<u32>>,
    facts: Vec<StoredFact>,
    /// The term hash the term groups are keyed by: [`term_hash`], or one
    /// that collides, for tests.
    hash: fn(&Flake) -> u64,
    /// A wide slot's facts by term.
    terms: Option<Groups<u64>>,
    /// A wide slot's list entries, by `(position, fact)`.
    positions: Vec<(i32, u32)>,
    /// Each fact's index key, computed when an intent first needs one.
    keys: Option<Vec<Option<IndexKey>>>,
    /// A wide slot's facts by index key, grouped with `keys`.
    by_key: Option<Groups<IndexKey>>,
    /// A wide slot's big integers by value ([`value_hash`]), built when an
    /// intent first needs them. Boxed: most slots never build it, and every
    /// slot a batch reads carries the field.
    by_value: Option<Box<Groups<u64>>>,
}

impl SlotFacts {
    fn new(g_id: GraphId, p: Sid, facts: Vec<StoredFact>, narrow_slot: usize) -> Self {
        Self::with_hash(g_id, p, facts, narrow_slot, term_hash)
    }

    fn with_hash(
        g_id: GraphId,
        p: Sid,
        facts: Vec<StoredFact>,
        narrow_slot: usize,
        hash: fn(&Flake) -> u64,
    ) -> Self {
        let wide = facts.len() > narrow_slot;
        let terms = wide.then(|| Groups::build(&facts, |_, f| Some(hash(&f.flake)), same_term));
        let mut positions = Vec::new();
        if wide {
            positions = facts
                .iter()
                .enumerate()
                .filter_map(|(n, f)| Some((list_index_of(&f.flake)?, n as u32)))
                .collect();
            positions.sort_unstable();
        }
        Self {
            g_id,
            p,
            p_id: None,
            facts,
            hash,
            terms,
            positions,
            keys: None,
            by_key: None,
            by_value: None,
        }
    }

    fn is_wide(&self) -> bool {
        self.terms.is_some()
    }

    /// The persisted id of the slot's predicate.
    fn p_id(&mut self, keys: &IndexKeys) -> Option<u32> {
        let p = &self.p;
        *self.p_id.get_or_insert_with(|| keys.p_id(p))
    }

    /// `f`'s index key, when its kind of value can share one (see
    /// [`IndexKeys::key`]).
    fn key_of(&mut self, keys: &IndexKeys, f: &Flake) -> Option<IndexKey> {
        if !f.o.index_key_may_merge_terms() {
            return None;
        }
        let p_id = self.p_id(keys);
        keys.key(self.g_id, p_id, f)
    }

    /// Compute every fact's index key, once.
    fn ensure_keys(&mut self, keys: &IndexKeys) {
        if self.keys.is_some() {
            return;
        }
        let p_id = self.p_id(keys);
        let fact_keys: Vec<Option<IndexKey>> = self
            .facts
            .iter()
            .map(|f| keys.key(self.g_id, p_id, &f.flake))
            .collect();
        if self.is_wide() {
            self.by_key = Some(Groups::build(&self.facts, |n, _| fact_keys[n], |_, _| true));
        }
        self.keys = Some(fact_keys);
    }

    /// Group a wide slot's big integers by value, once.
    fn ensure_by_value(&mut self) {
        if self.is_wide() && self.by_value.is_none() {
            self.by_value = Some(Box::new(Groups::build(
                &self.facts,
                |_, f| value_hash(&f.flake),
                same_value,
            )));
        }
    }

    /// The slot's other facts that are the same stored fact as fact `n`:
    /// terms of one big integer at one list position, which novelty holds
    /// apart until the next index build. Every other kind is one term per
    /// fact once the read is resolved.
    fn same_fact(&mut self, n: usize) -> Vec<usize> {
        if !self.facts[n].flake.o.index_keys_by_value() {
            return Vec::new();
        }
        self.ensure_by_value();
        let fact = &self.facts[n].flake;
        let at = list_index_of(fact);
        let sibling = |m: usize| {
            m != n
                && same_value(&self.facts[m].flake, fact)
                && list_index_of(&self.facts[m].flake) == at
        };
        match (&self.by_value, value_hash(fact)) {
            (Some(groups), Some(h)) => groups
                .get(h)
                .map(|g| groups.members(g))
                .unwrap_or_default()
                .iter()
                .map(|&m| m as usize)
                .filter(|&m| sibling(m))
                .collect(),
            _ => (0..self.facts.len()).filter(|&m| sibling(m)).collect(),
        }
    }

    /// The stored fact a DELETE intent names (see [`DeleteResolver`]).
    fn named(&mut self, intent: &Flake, keys: &IndexKeys) -> Option<usize> {
        let position = list_index_of(intent);
        let by_term = |_: usize, f: &StoredFact| same_term(&f.flake, intent);
        let hit = match &self.terms {
            Some(terms) => self.first_in(terms.get((self.hash)(intent)), terms, position, by_term),
            None => self.first(position, by_term),
        };
        if hit.is_some() {
            return hit;
        }
        // Else by the fact's identity, whichever op of it survived: an index
        // row and a later novelty op of one key are one stored fact, which
        // the novelty op stands for, and an intent naming the index's term
        // names it too. A big integer is one fact per value, indexed or not.
        if intent.o.index_keys_by_value() {
            self.ensure_by_value();
            let same = |_: usize, f: &StoredFact| same_value(&f.flake, intent);
            return match &self.by_value {
                Some(groups) => {
                    let group = value_hash(intent).and_then(|h| groups.get(h));
                    self.first_in(group, groups, position, same)
                }
                None => self.first(position, same),
            };
        }
        let key = self.key_of(keys, intent)?;
        self.ensure_keys(keys);
        let fact_keys = self.keys.as_deref()?;
        let by_key = |n: usize, _: &StoredFact| fact_keys[n] == Some(key);
        match &self.by_key {
            Some(groups) => self.first_in(groups.get(key), groups, position, by_key),
            None => self.first(position, by_key),
        }
    }

    /// The stored fact a witnessed row decodes (see [`DeleteResolver`]).
    /// `take(first)` counts the rows matched so far to the facts whose first
    /// is fact `first`, and gives this row's place among them.
    fn witnessed(
        &mut self,
        row: &Flake,
        keys: &IndexKeys,
        mut take: impl FnMut(u32) -> usize,
    ) -> Option<usize> {
        if let Some(key) = self.key_of(keys, row) {
            self.ensure_keys(keys);
            let fact_keys = self.keys.as_deref()?;
            let by_key = |n: usize, _: &StoredFact| fact_keys[n] == Some(key);
            let wide = self.by_key.as_ref().map(|g| (g, g.get(key)));
            if let Some(n) = self.kth(wide, by_key, &mut take) {
                return Some(n);
            }
        }
        let by_term = |_: usize, f: &StoredFact| same_term(&f.flake, row);
        let wide = self.terms.as_ref().map(|g| (g, g.get((self.hash)(row))));
        self.kth(wide, by_term, &mut take)
    }

    /// Narrow slot: the first fact satisfying `pred` that `position` admits.
    /// With a list position, a fact at that position; without one, the
    /// first list entry, else the first plain value, in slot order.
    fn first(
        &self,
        position: Option<i32>,
        pred: impl Fn(usize, &StoredFact) -> bool,
    ) -> Option<usize> {
        let at = |admits: &dyn Fn(Option<i32>) -> bool| {
            self.facts
                .iter()
                .enumerate()
                .position(|(n, f)| admits(list_index_of(&f.flake)) && pred(n, f))
        };
        match position {
            Some(i) => at(&|p| p == Some(i)),
            None => at(&|p| p.is_some()).or_else(|| at(&|p| p.is_none())),
        }
    }

    /// Wide slot: [`Self::first`] through `groups` (`group` is the one the
    /// intent's key selects), or through the list positions.
    fn first_in<K: Copy + Eq + Hash + Ord>(
        &self,
        group: Option<Group>,
        groups: &Groups<K>,
        position: Option<i32>,
        pred: impl Fn(usize, &StoredFact) -> bool,
    ) -> Option<usize> {
        match position {
            Some(i) => {
                let from = self.positions.partition_point(|&(p, _)| p < i);
                self.positions[from..]
                    .iter()
                    .take_while(|&&(p, _)| p == i)
                    .map(|&(_, n)| n as usize)
                    .find(|&n| pred(n, &self.facts[n]))
            }
            // A group lists its entries first, so its first member that
            // satisfies `pred` is the rule's answer.
            None => groups
                .members(group?)
                .iter()
                .map(|&n| n as usize)
                .find(|&n| pred(n, &self.facts[n])),
        }
    }

    /// The facts satisfying `pred` — list entries in slot order, then plain
    /// values — and of those, the one at this row's place (`take`), or the
    /// last when the row is past them. `wide` is the slot's groups and the
    /// row's group, for a wide slot. A single candidate is the answer
    /// without counting.
    fn kth<K: Copy + Eq + Hash + Ord>(
        &self,
        wide: Option<(&Groups<K>, Option<Group>)>,
        pred: impl Fn(usize, &StoredFact) -> bool,
        take: &mut impl FnMut(u32) -> usize,
    ) -> Option<usize> {
        let (facts, pred) = (&self.facts, &pred);
        match wide {
            Some((groups, group)) => {
                let group = group?;
                let members = groups.members(group);
                if group.pure {
                    let first = *members.first()? as usize;
                    if !pred(first, &facts[first]) {
                        return None;
                    }
                    if members.len() == 1 {
                        return Some(first);
                    }
                    let k = take(first as u32);
                    return members.get(k).or(members.last()).map(|&n| n as usize);
                }
                // A hash collision: filter the group's members.
                pick(
                    || {
                        members
                            .iter()
                            .map(|&n| n as usize)
                            .filter(move |&n| pred(n, &facts[n]))
                    },
                    take,
                )
            }
            None => {
                let is_entry = move |n: usize| list_index_of(&facts[n].flake).is_some();
                pick(
                    || {
                        let candidate = move |entry: bool| {
                            move |&n: &usize| is_entry(n) == entry && pred(n, &facts[n])
                        };
                        (0..facts.len())
                            .filter(candidate(true))
                            .chain((0..facts.len()).filter(candidate(false)))
                    },
                    take,
                )
            }
        }
    }
}

/// Of the candidates `candidates()` lists, the one at the row's place
/// (`take`), or the last when the row is past them; a single candidate
/// without counting.
fn pick<I: Iterator<Item = usize>>(
    candidates: impl Fn() -> I,
    take: &mut impl FnMut(u32) -> usize,
) -> Option<usize> {
    let mut it = candidates();
    let first = it.next()?;
    if it.next().is_none() {
        return Some(first);
    }
    let k = take(first as u32);
    let mut last = first;
    for (j, n) in candidates().enumerate() {
        if j == k {
            return Some(n);
        }
        last = n;
    }
    Some(last)
}

/// Fact positions of a wide slot grouped by a key — a term's hash, or an
/// index key. A group holds its list entries in slot order, then its plain
/// values in slot order.
struct Groups<K> {
    order: Vec<u32>,
    groups: FxHashMap<K, Group>,
}

/// One group of [`Groups`]: its members' range in `order`.
#[derive(Clone, Copy, Debug)]
struct Group {
    start: u32,
    end: u32,
    /// Every member holds the same term (`false` only for a hash collision,
    /// whose members are then filtered).
    pure: bool,
}

impl<K: Copy + Eq + Hash + Ord> Groups<K> {
    /// Group `facts` by `key` (facts it gives none are left out). `same`
    /// says whether two members hold the same term.
    fn build(
        facts: &[StoredFact],
        key: impl Fn(usize, &StoredFact) -> Option<K>,
        same: impl Fn(&Flake, &Flake) -> bool,
    ) -> Self {
        let mut keyed: Vec<(K, bool, u32)> = facts
            .iter()
            .enumerate()
            .filter_map(|(n, f)| Some((key(n, f)?, list_index_of(&f.flake).is_none(), n as u32)))
            .collect();
        keyed.sort_unstable();
        let mut order = Vec::with_capacity(keyed.len());
        let mut groups = FxHashMap::default();
        let mut i = 0;
        while i < keyed.len() {
            let k = keyed[i].0;
            let start = order.len() as u32;
            let first = keyed[i].2 as usize;
            let mut pure = true;
            while i < keyed.len() && keyed[i].0 == k {
                let n = keyed[i].2;
                pure = pure
                    && (n as usize == first || same(&facts[first].flake, &facts[n as usize].flake));
                order.push(n);
                i += 1;
            }
            let end = order.len() as u32;
            groups.insert(k, Group { start, end, pure });
        }
        Self { order, groups }
    }

    fn get(&self, key: K) -> Option<Group> {
        self.groups.get(&key).copied()
    }

    fn members(&self, group: Group) -> &[u32] {
        &self.order[group.start as usize..group.end as usize]
    }
}

/// How each graph's novelty is read for a batch of slots.
struct NoveltyPlan {
    /// Graphs whose novelty was walked once, keyed `(graph, s, p)`.
    walked: HashMap<(GraphId, Sid, Sid), Vec<Flake>>,
    walked_graphs: HashSet<GraphId>,
    /// Graphs with no novelty at all: nothing to read.
    empty_graphs: HashSet<GraphId>,
}

impl NoveltyPlan {
    fn for_slots(ledger: &LedgerState, slots: &[Slot], to_t: i64) -> Self {
        let novelty = ledger.novelty.as_ref();
        let mut per_graph: HashMap<GraphId, usize> = HashMap::new();
        for slot in slots {
            *per_graph.entry(slot.g_id).or_default() += 1;
        }
        let mut plan = NoveltyPlan {
            walked: HashMap::new(),
            walked_graphs: HashSet::new(),
            empty_graphs: HashSet::new(),
        };
        for (g_id, slot_count) in per_graph {
            let flakes = novelty.overlay_flake_count(g_id).unwrap_or(usize::MAX);
            if flakes == 0 {
                plan.empty_graphs.insert(g_id);
                continue;
            }
            let segments = novelty.segment_count(g_id).max(1);
            if slot_count
                .saturating_mul(segments)
                .saturating_mul(NOVELTY_WALK_RATIO)
                <= flakes
            {
                continue; // per-slot seeks
            }
            let wanted: HashSet<(&Sid, &Sid)> = slots
                .iter()
                .filter(|slot| slot.g_id == g_id)
                .map(|slot| (&slot.s, &slot.p))
                .collect();
            let walked = &mut plan.walked;
            novelty.for_each_overlay_flake(
                g_id,
                IndexType::Spot,
                None,
                None,
                true,
                to_t,
                &mut |f| {
                    if wanted.contains(&(&f.s, &f.p)) {
                        walked
                            .entry((g_id, f.s.clone(), f.p.clone()))
                            .or_default()
                            .push(f.clone());
                    }
                },
            );
            plan.walked_graphs.insert(g_id);
        }
        plan
    }

    /// Append the novelty ops of `slot` to `out`, or with `o` only those
    /// whose value is `o`.
    fn collect_slot(
        &self,
        ledger: &LedgerState,
        slot: &Slot,
        o: Option<&FlakeValue>,
        to_t: i64,
        out: &mut Vec<Flake>,
    ) {
        if self.empty_graphs.contains(&slot.g_id) {
            return;
        }
        if self.walked_graphs.contains(&slot.g_id) {
            if let Some(ops) = self
                .walked
                .get(&(slot.g_id, slot.s.clone(), slot.p.clone()))
            {
                out.extend(ops.iter().filter(|f| o.is_none_or(|o| f.o == *o)).cloned());
            }
            return;
        }
        let mut lo = Flake::min_for_subject_predicate(slot.s.clone(), slot.p.clone());
        let mut hi = Flake::max_for_subject_predicate(slot.s.clone(), slot.p.clone());
        // SPOT orders a value's ops by datatype, `t`, op and meta after the
        // value, so bounds that fix it hold every one of them.
        if let Some(o) = o {
            lo.o = o.clone();
            hi.o = o.clone();
        }
        ledger.novelty.for_each_overlay_flake(
            slot.g_id,
            IndexType::Spot,
            Some(&lo),
            Some(&hi),
            false,
            to_t,
            &mut |f| out.push(f.clone()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::flakes::DT_ID;
    use fluree_db_core::{FlakeMeta, FlakeValue, LedgerSnapshot};
    use fluree_db_novelty::Novelty;

    fn sid(name: &str) -> Sid {
        Sid::new(100, name)
    }

    fn string_dt() -> Sid {
        Sid::new(fluree_vocab::namespaces::XSD, "string")
    }

    fn lang_dt() -> Sid {
        Sid::new(fluree_vocab::namespaces::RDF, "langString")
    }

    fn flake(s: &str, p: &str, o: &str, t: i64, op: bool, m: Option<FlakeMeta>) -> Flake {
        let dt = if m.as_ref().is_some_and(|m| m.lang.is_some()) {
            lang_dt()
        } else {
            string_dt()
        };
        Flake::new(
            sid(s),
            sid(p),
            FlakeValue::String(o.to_string()),
            dt,
            t,
            op,
            m,
        )
    }

    fn in_graph(mut f: Flake, g: &Sid) -> Flake {
        f.g = Some(g.clone());
        f
    }

    /// A genesis ledger (nothing indexed) whose novelty holds `commits`, one
    /// commit per inner vector at t = 1, 2, …
    fn ledger(commits: Vec<Vec<Flake>>, reverse_graph: &HashMap<Sid, GraphId>) -> LedgerState {
        let mut novelty = Novelty::new(0);
        for (i, flakes) in commits.into_iter().enumerate() {
            novelty
                .apply_commit(flakes, i as i64 + 1, reverse_graph)
                .expect("apply commit");
        }
        LedgerState::new(LedgerSnapshot::genesis("test:main"), novelty)
    }

    /// A ledger with an index this process has not loaded refuses every
    /// read here with `IndexNotLoaded`; merge and rebase read here first.
    #[tokio::test]
    async fn an_index_that_is_not_loaded_refuses_every_read() {
        let mut snapshot = LedgerSnapshot::genesis("test:main");
        snapshot.t = 1;
        let ledger = LedgerState::new(snapshot, Novelty::new(1));
        let facts = CurrentFacts::new(&ledger);
        let refused = |r: Result<Vec<StoredFact>>| {
            assert!(
                matches!(r, Err(crate::TransactError::IndexNotLoaded(_))),
                "{r:?}"
            );
        };
        refused(
            facts
                .of_slots(&[slot(0, None, "a", "b")])
                .await
                .map(|mut v| v.pop().unwrap_or_default()),
        );
        refused(facts.of_subject(0, None, &sid("a")).await);
        refused(facts.whole_graph(0, None, None, None).await);
        let named = flake("a", "b", "x", 1, true, None);
        let items = [(slot(0, None, "a", "b"), &named)];
        refused(
            facts
                .named_by(&items)
                .await
                .map(|v| v.into_iter().flatten().collect()),
        );
        refused(
            facts
                .all_named_by(&items)
                .await
                .map(|mut v| v.pop().unwrap_or_default()),
        );
    }

    fn slot(g_id: GraphId, g_sid: Option<&Sid>, s: &str, p: &str) -> Slot {
        Slot {
            g_id,
            g_sid: g_sid.cloned(),
            s: sid(s),
            p: sid(p),
        }
    }

    fn objects(facts: &[StoredFact]) -> Vec<(String, Option<FlakeMeta>)> {
        facts
            .iter()
            .map(|f| match &f.flake().o {
                FlakeValue::String(s) => (s.clone(), f.flake().m.clone()),
                other => (format!("{other:?}"), f.flake().m.clone()),
            })
            .collect()
    }

    /// Lang-tagged and list values come back as stored — with their tag and
    /// position — and a value retracted in a later commit is gone.
    #[tokio::test]
    async fn slot_facts_keep_lang_and_list_metadata_and_drop_retracted_values() {
        let en = FlakeMeta::with_lang("en");
        let fr = FlakeMeta::with_lang("fr");
        let ledger = ledger(
            vec![
                vec![
                    flake("a", "label", "x", 1, true, Some(en.clone())),
                    flake("a", "label", "x", 1, true, Some(fr.clone())),
                    flake("a", "items", "v", 1, true, Some(FlakeMeta::with_index(0))),
                    flake("a", "items", "v", 1, true, Some(FlakeMeta::with_index(1))),
                    flake("a", "items", "w", 1, true, Some(FlakeMeta::with_index(2))),
                    flake("a", "other", "o", 1, true, None),
                ],
                vec![flake(
                    "a",
                    "items",
                    "w",
                    2,
                    false,
                    Some(FlakeMeta::with_index(2)),
                )],
            ],
            &HashMap::new(),
        );
        let facts = CurrentFacts::new(&ledger)
            .of_slots(&[slot(0, None, "a", "label"), slot(0, None, "a", "items")])
            .await
            .unwrap();
        let mut labels = objects(&facts[0]);
        labels.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(
            labels,
            vec![("x".to_string(), Some(en)), ("x".to_string(), Some(fr))]
        );
        assert_eq!(
            objects(&facts[1]),
            vec![
                ("v".to_string(), Some(FlakeMeta::with_index(0))),
                ("v".to_string(), Some(FlakeMeta::with_index(1))),
            ],
            "list duplicates stay separate facts; the retracted entry is gone"
        );
        assert!(facts
            .iter()
            .flatten()
            .all(|f| f.origin() == Origin::Novelty && f.flake().g.is_none()));
    }

    /// A slot in a named graph reads that graph only, and every fact is
    /// stamped with the graph Sid the caller gives, which is what lets the
    /// retraction cancel a same-transaction assertion in that graph.
    #[tokio::test]
    async fn named_graph_slots_read_their_graph_and_stamp_it() {
        let g1 = sid("g1");
        let reverse: HashMap<Sid, GraphId> = [(g1.clone(), 3)].into_iter().collect();
        let ledger = ledger(
            vec![vec![
                flake("a", "p", "default", 1, true, None),
                in_graph(flake("a", "p", "named", 1, true, None), &g1),
            ]],
            &reverse,
        );
        let facts = CurrentFacts::new(&ledger)
            .of_slots(&[slot(3, Some(&g1), "a", "p"), slot(0, None, "a", "p")])
            .await
            .unwrap();
        assert_eq!(objects(&facts[0]), vec![("named".to_string(), None)]);
        assert_eq!(facts[0][0].flake().g.as_ref(), Some(&g1));
        assert_eq!(objects(&facts[1]), vec![("default".to_string(), None)]);
        assert_eq!(facts[1][0].flake().g, None);
    }

    /// The per-slot seek and the single filtered walk must give the same
    /// answer: which one runs depends only on how many slots a graph gets
    /// relative to its novelty size.
    #[tokio::test]
    async fn seek_and_walk_agree() {
        let mut commit = Vec::new();
        for i in 0..40 {
            commit.push(flake(
                &format!("s{i:02}"),
                "p",
                &format!("v{i}"),
                1,
                true,
                None,
            ));
            commit.push(flake(
                &format!("s{i:02}"),
                "q",
                &format!("w{i}"),
                1,
                true,
                None,
            ));
        }
        let retract = vec![flake("s07", "p", "v7", 2, false, None)];
        let ledger = ledger(vec![commit, retract], &HashMap::new());
        let facts = CurrentFacts::new(&ledger);

        // One slot against 81 novelty flakes: seek.
        let one = facts.of_slots(&[slot(0, None, "s07", "q")]).await.unwrap();
        // Every slot: walk.
        let all_slots: Vec<Slot> = (0..40)
            .flat_map(|i| {
                [
                    slot(0, None, &format!("s{i:02}"), "p"),
                    slot(0, None, &format!("s{i:02}"), "q"),
                ]
            })
            .collect();
        let all = facts.of_slots(&all_slots).await.unwrap();
        assert_eq!(objects(&one[0]), vec![("w7".to_string(), None)]);
        assert_eq!(objects(&all[15]), vec![("w7".to_string(), None)]);
        assert!(all[14].is_empty(), "s07 p was retracted");
        assert_eq!(
            all.iter().map(Vec::len).sum::<usize>(),
            79,
            "80 values minus the retracted one"
        );
    }

    #[tokio::test]
    async fn subject_facts_cover_every_predicate() {
        let ledger = ledger(
            vec![vec![
                flake("a", "p", "1", 1, true, None),
                flake("a", "q", "2", 1, true, None),
                flake("b", "p", "3", 1, true, None),
            ]],
            &HashMap::new(),
        );
        let facts = CurrentFacts::new(&ledger)
            .of_subject(0, None, &sid("a"))
            .await
            .unwrap();
        assert_eq!(
            objects(&facts),
            vec![("1".to_string(), None), ("2".to_string(), None)]
        );
    }

    /// `retract` keeps the stored identity — graph, datatype, tag, list
    /// position — and changes only `t` and the op.
    #[tokio::test]
    async fn retract_keeps_the_stored_identity() {
        let g1 = sid("g1");
        let reverse: HashMap<Sid, GraphId> = [(g1.clone(), 3)].into_iter().collect();
        let stored_flake = in_graph(
            flake(
                "a",
                "label",
                "x",
                1,
                true,
                FlakeMeta::from_parts(Some("en"), Some(4)),
            ),
            &g1,
        );
        let ledger = ledger(vec![vec![stored_flake.clone()]], &reverse);
        let mut facts = CurrentFacts::new(&ledger)
            .of_slots(&[slot(3, Some(&g1), "a", "label")])
            .await
            .unwrap();
        let retraction = facts.remove(0).remove(0).retract(9).into_flake();
        // `Flake`'s equality ignores `g`, `t` and `op`, so check them apart.
        assert_eq!(retraction, stored_flake);
        assert_eq!((retraction.t, retraction.op), (9, false));
        assert_eq!(retraction.g, stored_flake.g);
        assert_eq!(retraction.m, stored_flake.m);
        assert_eq!(retraction.dt, stored_flake.dt);
    }

    /// A named intent's retraction is the stored fact's: from an intent
    /// holding the value written alike only the value is taken (the stored
    /// tag's case and list position stay), and a value written otherwise (a
    /// decimal's scale) is copied from the fact.
    #[tokio::test]
    async fn retract_named_keeps_the_stored_identity() {
        let g1 = sid("g1");
        let reverse: HashMap<Sid, GraphId> = [(g1.clone(), 3)].into_iter().collect();
        let decimal_dt = Sid::new(fluree_vocab::namespaces::XSD, "decimal");
        let dec = |v: &str| FlakeValue::Decimal(Box::new(v.parse().expect("decimal")));
        let tagged = in_graph(
            flake(
                "a",
                "label",
                "x",
                1,
                true,
                FlakeMeta::from_parts(Some("en"), Some(4)),
            ),
            &g1,
        );
        let scaled = in_graph(
            Flake::new(
                sid("a"),
                sid("amount"),
                dec("1.00"),
                decimal_dt.clone(),
                1,
                true,
                None,
            ),
            &g1,
        );
        let ledger = ledger(vec![vec![tagged, scaled]], &reverse);
        let facts = CurrentFacts::new(&ledger)
            .of_slots(&[
                slot(3, Some(&g1), "a", "label"),
                slot(3, Some(&g1), "a", "amount"),
            ])
            .await
            .unwrap();
        let intents = [
            flake(
                "a",
                "label",
                "x",
                9,
                false,
                FlakeMeta::from_parts(Some("EN"), None),
            ),
            Flake::new(
                sid("a"),
                sid("amount"),
                dec("1.0"),
                decimal_dt,
                9,
                false,
                None,
            ),
        ];
        for (fact, intent) in facts.iter().map(|slot| &slot[0]).zip(intents) {
            let named = fact.retract_named(intent, 9).into_flake();
            let copied = fact.clone().retract(9).into_flake();
            // `Flake`'s equality ignores `g`, `t` and `op`, and a decimal's
            // scale, so check them apart.
            assert_eq!(named, copied);
            assert_eq!(format!("{:?}", named.o), format!("{:?}", copied.o));
            assert_eq!(
                (named.t, named.op, &named.g, &named.m, &named.dt),
                (copied.t, copied.op, &copied.g, &copied.m, &copied.dt)
            );
        }
    }

    // =========================================================================
    // DeleteResolver
    // =========================================================================

    /// A retraction intent on `ex:a <p> <o>` at t = 9: witnessed by a WHERE
    /// triple or not, with an optional list position or tag.
    fn intent(p: &str, o: &str, witnessed: bool, m: Option<FlakeMeta>) -> RetractIntent {
        RetractIntent::new(flake("a", p, o, 9, false, m), witnessed)
    }

    fn term_of(f: &Flake) -> (String, Option<FlakeMeta>) {
        match &f.o {
            FlakeValue::String(s) => (s.clone(), f.m.clone()),
            other => (format!("{other:?}"), f.m.clone()),
        }
    }

    /// Resolve `batches` in order with one resolver, in the default graph;
    /// slots with more than `narrow_slot` facts take the indexed path.
    async fn resolve_batches(
        ledger: &LedgerState,
        batches: Vec<Vec<RetractIntent>>,
        narrow_slot: usize,
    ) -> Vec<(String, Option<FlakeMeta>)> {
        let mut resolver = DeleteResolver::new(ledger, ListFree::check(ledger));
        resolver.narrow_slot = narrow_slot;
        let mut out = Vec::new();
        for batch in batches {
            let (retractions, _) = resolver
                .resolve(batch, |_| Ok(0), None)
                .await
                .expect("resolve");
            out.extend(retractions.iter().map(|r| term_of(r.flake())));
        }
        out
    }

    fn at(o: &str, i: i32) -> (String, Option<FlakeMeta>) {
        (o.to_string(), Some(FlakeMeta::with_index(i)))
    }

    fn plain(o: &str) -> (String, Option<FlakeMeta>) {
        (o.to_string(), None)
    }

    /// Identical witnessed rows take successive stored entries: a list
    /// `["v", "v", "w"]` loses all three to three rows, in one batch or
    /// across batches, on the scanned and on the indexed path.
    #[tokio::test]
    async fn identical_witnessed_rows_take_successive_list_entries() {
        let ledger = ledger(
            vec![vec![
                flake("a", "items", "v", 1, true, Some(FlakeMeta::with_index(0))),
                flake("a", "items", "v", 1, true, Some(FlakeMeta::with_index(1))),
                flake("a", "items", "w", 1, true, Some(FlakeMeta::with_index(2))),
            ]],
            &HashMap::new(),
        );
        let row = |o: &str| intent("items", o, true, None);
        for narrow_slot in [usize::MAX, 0] {
            let one_batch = resolve_batches(
                &ledger,
                vec![vec![row("v"), row("v"), row("w")]],
                narrow_slot,
            )
            .await;
            let batched = resolve_batches(
                &ledger,
                vec![vec![row("v")], vec![row("v"), row("w")]],
                narrow_slot,
            )
            .await;
            let expected = vec![at("v", 0), at("v", 1), at("w", 2)];
            assert_eq!(one_batch, expected, "narrow_slot={narrow_slot}");
            assert_eq!(
                batched, expected,
                "narrow_slot={narrow_slot}, across batches"
            );
        }
    }

    /// A value stored as a list entry and as a plain value: identical
    /// witnessed rows take the entry, then the plain value, and a row past
    /// them takes the plain value again.
    #[tokio::test]
    async fn witnessed_rows_take_the_list_entries_then_the_plain_value() {
        let ledger = ledger(
            vec![vec![
                flake("a", "items", "v", 1, true, None),
                flake("a", "items", "v", 1, true, Some(FlakeMeta::with_index(0))),
            ]],
            &HashMap::new(),
        );
        let row = || intent("items", "v", true, None);
        for narrow_slot in [usize::MAX, 0] {
            assert_eq!(
                resolve_batches(&ledger, vec![vec![row(), row(), row()]], narrow_slot).await,
                vec![at("v", 0), plain("v"), plain("v")],
                "narrow_slot={narrow_slot}"
            );
        }
    }

    /// An intent that is not witnessed names the first list entry holding
    /// its term, so identical intents name one entry (the accumulator then
    /// collapses them); an intent with a position names that position.
    #[tokio::test]
    async fn named_intents_name_the_first_entry_or_their_position() {
        let ledger = ledger(
            vec![vec![
                flake("a", "items", "v", 1, true, None),
                flake("a", "items", "v", 1, true, Some(FlakeMeta::with_index(0))),
                flake("a", "items", "v", 1, true, Some(FlakeMeta::with_index(1))),
            ]],
            &HashMap::new(),
        );
        let named = |m: Option<FlakeMeta>| intent("items", "v", false, m);
        for narrow_slot in [usize::MAX, 0] {
            assert_eq!(
                resolve_batches(
                    &ledger,
                    vec![vec![
                        named(None),
                        named(None),
                        named(Some(FlakeMeta::with_index(1))),
                        named(Some(FlakeMeta::with_index(7))),
                    ]],
                    narrow_slot,
                )
                .await,
                vec![at("v", 0), at("v", 0), at("v", 1)],
                "narrow_slot={narrow_slot}"
            );
        }
    }

    /// A slot with plain values, language variants, a value repeated in a
    /// list and stored plain, and intents that name nothing.
    fn mixed_slot() -> LedgerState {
        let mut commit = Vec::new();
        for i in 0..12 {
            commit.push(flake("a", "p", &format!("v{i}"), 1, true, None));
        }
        for i in 0..8 {
            commit.push(flake(
                "a",
                "p",
                "x",
                1,
                true,
                Some(FlakeMeta::with_index(i)),
            ));
        }
        commit.push(flake("a", "p", "x", 1, true, None));
        commit.push(flake(
            "a",
            "p",
            "y",
            1,
            true,
            Some(FlakeMeta::with_lang("en")),
        ));
        commit.push(flake(
            "a",
            "p",
            "y",
            1,
            true,
            Some(FlakeMeta::with_lang("EN-us")),
        ));
        ledger(vec![commit], &HashMap::new())
    }

    fn mixed_intents() -> Vec<RetractIntent> {
        let lang = |tag: &str| Some(FlakeMeta::with_lang(tag));
        let mut intents = vec![
            intent("p", "v3", false, None),
            intent("p", "v11", false, None),
            intent("p", "nope", false, None),
            intent("p", "x", false, None),
            intent("p", "x", false, Some(FlakeMeta::with_index(5))),
            intent("p", "x", false, Some(FlakeMeta::with_index(42))),
            intent("p", "y", false, lang("en-US")),
            intent("p", "y", false, lang("fr")),
            intent("p", "v7", true, None),
            intent("p", "y", true, lang("en")),
        ];
        // Eight entries, the plain value, and one row past them.
        intents.extend((0..10).map(|_| intent("p", "x", true, None)));
        intents
    }

    /// The indexed path answers every intent the way the scan does.
    #[tokio::test]
    async fn wide_and_narrow_slots_resolve_alike() {
        let ledger = mixed_slot();
        let narrow = resolve_batches(&ledger, vec![mixed_intents()], usize::MAX).await;
        let wide = resolve_batches(&ledger, vec![mixed_intents()], 0).await;
        assert_eq!(narrow, wide);
        let mut expected = vec![
            plain("v3"),
            plain("v11"),
            at("x", 0),
            at("x", 5),
            ("y".to_string(), Some(FlakeMeta::with_lang("EN-us"))),
            plain("v7"),
            ("y".to_string(), Some(FlakeMeta::with_lang("en"))),
        ];
        expected.extend((0..8).map(|i| at("x", i)));
        expected.extend([plain("x"), plain("x")]);
        assert_eq!(narrow, expected);
    }

    /// Facts whose terms collide in the term index are still told apart:
    /// with every fact in one group, the indexed path answers as the scan.
    #[tokio::test]
    async fn colliding_term_hashes_still_match_by_term() {
        let ledger = mixed_slot();
        let facts = CurrentFacts::new(&ledger)
            .of_slots(&[slot(0, None, "a", "p")])
            .await
            .unwrap()
            .remove(0);
        let keys = IndexKeys::new(&ledger);
        let mut narrow = SlotFacts::new(0, sid("p"), facts.clone(), usize::MAX);
        let mut colliding = SlotFacts::with_hash(0, sid("p"), facts, 0, |_| 0);
        assert!(
            colliding
                .terms
                .as_ref()
                .and_then(|t| t.get(0))
                .is_some_and(|g| !g.pure),
            "every fact in one impure group"
        );
        let mut taken: [HashMap<u32, usize>; 2] = Default::default();
        for intent in mixed_intents() {
            let f = &intent.flake;
            let mut answers = Vec::new();
            for (facts, taken) in [&mut narrow, &mut colliding]
                .into_iter()
                .zip(taken.iter_mut())
            {
                answers.push(if intent.witnessed {
                    facts.witnessed(f, &keys, |first| {
                        let seen = taken.entry(first).or_insert(0);
                        *seen += 1;
                        *seen - 1
                    })
                } else {
                    facts.named(f, &keys)
                });
            }
            assert_eq!(answers[0], answers[1], "{:?}", term_of(f));
        }
    }

    // =========================================================================
    // Value reads
    // =========================================================================

    /// Every way a slot of strings and references can be stored in novelty:
    /// a value with a tag and with another datatype beside the plain one, a
    /// value at two list positions and plain, one retracted, one retracted
    /// and asserted again, an IRI and a blank-node reference; and a value of
    /// the same name in a named graph. With `filler`, novelty also holds
    /// facts of another subject, enough that a slot is read by a seek into
    /// novelty rather than one walk over it ([`NoveltyPlan`]).
    fn value_read_ledger(g1: &Sid, filler: bool) -> LedgerState {
        let reverse: HashMap<Sid, GraphId> = [(g1.clone(), 3)].into_iter().collect();
        let typed = |o: &str, t: i64, op: bool| {
            Flake::new(
                sid("a"),
                sid("p"),
                FlakeValue::String(o.to_string()),
                sid("myType"),
                t,
                op,
                None,
            )
        };
        let reference = |o: Sid, t: i64, op: bool| {
            Flake::new(
                sid("a"),
                sid("p"),
                FlakeValue::Ref(o),
                DT_ID.clone(),
                t,
                op,
                None,
            )
        };
        let bnode = Sid::new(fluree_vocab::namespaces::BLANK_NODE, "fdb-b1");
        ledger(
            vec![
                vec![
                    flake("a", "p", "x", 1, true, None),
                    flake("a", "p", "x", 1, true, Some(FlakeMeta::with_lang("en"))),
                    typed("x", 1, true),
                    flake("a", "p", "y", 1, true, Some(FlakeMeta::with_index(0))),
                    flake("a", "p", "y", 1, true, Some(FlakeMeta::with_index(2))),
                    flake("a", "p", "y", 1, true, None),
                    flake("a", "p", "z", 1, true, None),
                    flake("a", "p", "w", 1, true, None),
                    reference(sid("r"), 1, true),
                    reference(bnode.clone(), 1, true),
                    in_graph(flake("a", "p", "x", 1, true, None), g1),
                ],
                vec![
                    flake("a", "p", "z", 2, false, None),
                    flake("a", "p", "w", 2, false, None),
                ],
                vec![flake("a", "p", "w", 3, true, None)],
                (0..if filler { 200 } else { 0 })
                    .map(|i| flake("b", "q", &format!("f{i}"), 4, true, None))
                    .collect(),
            ],
            &reverse,
        )
    }

    /// Intents naming the [`value_read_ledger`] slot, keyable and not.
    fn value_read_intents(g1: &Sid) -> Vec<RetractIntent> {
        let named = |o: FlakeValue, dt: Sid, m: Option<FlakeMeta>| {
            RetractIntent::new(Flake::new(sid("a"), sid("p"), o, dt, 9, false, m), false)
        };
        let s = |o: &str| FlakeValue::String(o.to_string());
        vec![
            intent("p", "x", false, None),
            intent("p", "x", false, Some(FlakeMeta::with_lang("EN"))),
            named(s("x"), sid("myType"), None),
            intent("p", "y", false, None),
            intent("p", "y", false, Some(FlakeMeta::with_index(2))),
            intent("p", "y", false, Some(FlakeMeta::with_index(1))),
            intent("p", "z", false, None),
            intent("p", "w", false, None),
            intent("p", "absent", false, None),
            named(FlakeValue::Ref(sid("r")), DT_ID.clone(), None),
            named(
                FlakeValue::Ref(Sid::new(fluree_vocab::namespaces::BLANK_NODE, "fdb-b1")),
                DT_ID.clone(),
                None,
            ),
            named(FlakeValue::Ref(sid("other")), DT_ID.clone(), None),
            RetractIntent::new(in_graph(flake("a", "p", "x", 9, false, None), g1), false),
        ]
    }

    /// Resolve `batches` with value reads bounded by `point_max` (`Some(0)`:
    /// none; `None`: the resolver's own bound),
    /// returning each retraction and unmatched intent in full (`Flake`'s
    /// equality ignores `g` and `t`), and how many slots were read by value.
    async fn resolve_reading_values(
        ledger: &LedgerState,
        batches: &[Vec<RetractIntent>],
        point_max: Option<usize>,
        narrow_slot: usize,
    ) -> (Vec<String>, Vec<String>, usize) {
        let routing: HashMap<Sid, GraphId> = [(sid("g1"), 3)].into_iter().collect();
        let mut resolver = DeleteResolver::new(ledger, ListFree::check(ledger));
        resolver.point_bound_override = point_max;
        resolver.narrow_slot = narrow_slot;
        let (mut out, mut unmatched, mut point_reads) = (Vec::new(), Vec::new(), 0);
        for batch in batches {
            let graph_of = |f: &Flake| Ok(f.g.as_ref().map_or(0, |g| routing[g]));
            let (retractions, stats) = resolver
                .resolve(batch.clone(), graph_of, Some(&mut unmatched))
                .await
                .expect("resolve");
            point_reads += stats.point_reads;
            out.extend(retractions.iter().map(|r| format!("{:?}", r.flake())));
        }
        let unmatched = unmatched.iter().map(|f| format!("{f:?}")).collect();
        (out, unmatched, point_reads)
    }

    /// Reading a slot for the values its intents name answers every intent
    /// as reading the slot does, whatever the batches: one intent each, a
    /// few, all at once (past the bound, so the slot is read), and with the
    /// slot held as wide by an earlier batch.
    #[tokio::test]
    async fn value_reads_answer_as_slot_reads_do() {
        let g1 = sid("g1");
        for filler in [false, true] {
            value_reads_answer_alike(&value_read_ledger(&g1, filler), &g1).await;
        }
    }

    async fn value_reads_answer_alike(ledger: &LedgerState, g1: &Sid) {
        let intents = value_read_intents(g1);
        let mut groupings: Vec<(&str, Vec<Vec<RetractIntent>>)> = vec![
            (
                "one each",
                intents.iter().map(|i| vec![i.clone()]).collect(),
            ),
            ("pairs", intents.chunks(2).map(<[_]>::to_vec).collect()),
            ("all at once", vec![intents.clone()]),
        ];
        let mut wide_first = vec![intents.clone()];
        wide_first.extend(intents.iter().map(|i| vec![i.clone()]));
        groupings.push(("slot held as wide", wide_first));
        for (what, batches) in groupings {
            for narrow_slot in [NARROW_SLOT, 1] {
                let scanned = resolve_reading_values(ledger, &batches, Some(0), narrow_slot).await;
                let by_value =
                    resolve_reading_values(ledger, &batches, Some(usize::MAX), narrow_slot).await;
                let bounded = resolve_reading_values(ledger, &batches, None, narrow_slot).await;
                assert_eq!(scanned.2, 0, "{what}");
                assert_eq!(
                    (&scanned.0, &scanned.1),
                    (&by_value.0, &by_value.1),
                    "{what}"
                );
                assert_eq!((&scanned.0, &scanned.1), (&bounded.0, &bounded.1), "{what}");
                if what == "one each" || what == "pairs" {
                    assert!(by_value.2 > 0, "{what}: some slot is read by value");
                }
            }
        }
    }

    /// Facts read by value are not the slot: a later batch naming another
    /// value of the slot still finds it, even when the values read were
    /// more than a narrow slot holds.
    #[tokio::test]
    async fn facts_read_by_value_are_not_kept_as_the_slot() {
        let g1 = sid("g1");
        let ledger = value_read_ledger(&g1, false);
        let batches = vec![
            vec![intent("p", "y", false, None)],
            vec![intent("p", "x", false, None)],
        ];
        let (out, unmatched, point_reads) =
            resolve_reading_values(&ledger, &batches, Some(usize::MAX), 1).await;
        assert_eq!(point_reads, 2);
        assert!(unmatched.is_empty(), "{unmatched:?}");
        assert_eq!(out.len(), 2, "{out:?}");
    }

    /// Witnessed rows read their slot even when one row names it: the k-th
    /// identical row is counted against the slot's facts, and a batch that
    /// read only the row's value would count against other indices than a
    /// later batch that reads the slot.
    #[tokio::test]
    async fn witnessed_rows_read_their_slot_even_alone() {
        let at_ = |o: &str, i: i32| flake("a", "p", o, 1, true, Some(FlakeMeta::with_index(i)));
        let ledger = ledger(
            vec![vec![at_("a", 0), at_("v", 1), at_("v", 2)]],
            &HashMap::new(),
        );
        let batches = vec![
            vec![intent("p", "v", true, None)],
            vec![intent("p", "v", true, None), intent("p", "a", true, None)],
        ];
        let mut out = resolve_batches(&ledger, batches, NARROW_SLOT).await;
        out.sort_by(|x, y| format!("{x:?}").cmp(&format!("{y:?}")));
        assert_eq!(out, vec![at("a", 0), at("v", 1), at("v", 2)]);
    }

    /// A retraction names a stored fact by its term and metadata: not an
    /// absent or retracted value, nor a stored value under another tag or
    /// list position. (Without an index there are no keys to match by.) Each
    /// item is matched against its own slot, whatever the order of the
    /// items: a value another slot holds is not named.
    #[tokio::test]
    async fn named_by_names_a_stored_fact_and_nothing_else() {
        let en = || Some(FlakeMeta::with_lang("en"));
        let ledger = ledger(
            vec![
                vec![
                    flake("a", "p", "x", 1, true, None),
                    flake("a", "p", "y", 1, true, en()),
                    flake("a", "p", "z", 1, true, None),
                    flake("b", "p", "q", 1, true, None),
                ],
                vec![flake("a", "p", "z", 2, false, None)],
            ],
            &HashMap::new(),
        );
        let current = CurrentFacts::new(&ledger);
        let ap = slot(0, None, "a", "p");
        let bp = slot(0, None, "b", "p");
        let retract = |o: &str, m: Option<FlakeMeta>| flake("a", "p", o, 3, false, m);
        let retract_b = |o: &str| flake("b", "p", o, 3, false, None);
        let cases = [
            ("a stored value", &ap, retract("x", None), true),
            ("a value of another slot", &bp, retract_b("q"), true),
            (
                "a value only another slot holds",
                &bp,
                retract_b("x"),
                false,
            ),
            ("a stored tagged value", &ap, retract("y", en()), true),
            ("a tagged value untagged", &ap, retract("y", None), false),
            (
                "a value at a list position it is not at",
                &ap,
                retract("x", Some(FlakeMeta::with_index(0))),
                false,
            ),
            ("a retracted value", &ap, retract("z", None), false),
            ("an absent value", &ap, retract("w", None), false),
        ];
        let items: Vec<(Slot, &Flake)> = cases
            .iter()
            .map(|(_, slot, flake, _)| ((*slot).clone(), flake))
            .collect();
        let named = current.named_by(&items).await.unwrap();
        for ((what, _, flake, held), fact) in cases.iter().zip(&named) {
            assert_eq!(fact.is_some(), *held, "{what}");
            if let Some(fact) = fact {
                assert_eq!(fact.flake(), flake, "{what}: names the stored fact");
            }
        }
    }

    /// `named_by` names an integer too large for 64 bits by value with no
    /// index yet, as the first build will key it, and by term first: of two
    /// terms of one value that novelty holds side by side, an item naming one
    /// of them exactly names that one.
    #[tokio::test]
    async fn named_by_names_a_big_integer_by_value_its_own_term_first() {
        let big = |n: i64, dt: &str, t: i64, m: Option<FlakeMeta>| {
            let v: num_bigint::BigInt = "123456789012345678901234567890".parse().unwrap();
            Flake::new(
                sid("a"),
                sid("p"),
                FlakeValue::BigInt(Box::new(v + n)),
                Sid::new(fluree_vocab::namespaces::XSD, dt),
                t,
                t < 3,
                m,
            )
        };
        let ledger = ledger(
            vec![
                vec![big(0, "nonNegativeInteger", 1, None)],
                vec![big(0, "positiveInteger", 2, None)],
            ],
            &HashMap::new(),
        );
        let current = CurrentFacts::new(&ledger);
        let cases = [
            (
                "one stored term",
                big(0, "nonNegativeInteger", 3, None),
                Some("nonNegativeInteger"),
            ),
            (
                "the other",
                big(0, "positiveInteger", 3, None),
                Some("positiveInteger"),
            ),
            ("a third subtype", big(0, "integer", 3, None), Some("")),
            ("another value", big(1, "nonNegativeInteger", 3, None), None),
            (
                "a list position",
                big(0, "nonNegativeInteger", 3, Some(FlakeMeta::with_index(0))),
                None,
            ),
        ];
        let ap = slot(0, None, "a", "p");
        let items: Vec<(Slot, &Flake)> = cases.iter().map(|(_, f, _)| (ap.clone(), f)).collect();
        let named = current.named_by(&items).await.unwrap();
        for ((what, _, want), fact) in cases.iter().zip(&named) {
            let got = fact.as_ref().map(|f| f.flake().dt.name.to_string());
            match want {
                Some("") => assert!(got.is_some(), "{what}: {got:?}"),
                want => assert_eq!(got.as_deref(), *want, "{what}"),
            }
        }
    }
}
