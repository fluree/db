//! The vector lengths a ledger can store.
//!
//! The index keeps each property's vectors in each graph at one length, set
//! by the first vector the property received there
//! ([`fluree_db_core::vector`]). Every path that creates new commits checks
//! its vectors against that length before the write: `build_commit`, push,
//! and bulk import, whose vector pool applies the same rule as it reads.
//!
//! Pull, clone and restore (from a `.flpack` archive) do not check. They
//! create no commits: they copy a ledger's existing commits, and its index,
//! byte for byte, so the copy holds what the source holds and refuses what
//! the source refuses.

use crate::commit::binary_store;
use crate::error::{Result, TransactError};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::vector::{check_vector_f64, VectorError};
use fluree_db_core::{Flake, FlakeValue, GraphId, IndexType, Sid, DEFAULT_GRAPH_ID};
use fluree_db_ledger::LedgerState;
use fluree_db_novelty::Novelty;
use std::collections::HashMap;
use std::sync::Arc;

/// Refuse a commit on `base` that holds a vector the ledger cannot store.
///
/// `graph_ids` maps each named graph `base` holds to its id; `iri` names a
/// property or graph in the refusal.
///
/// A commit lands only on the base it was checked against. The handle path
/// holds the ledger's write lock; `commit()`, which holds none, checks that
/// the base is the published head (`verify_sequencing`) and publishes with a
/// compare-and-set on that head. A write staged on an older base, or racing
/// another on the same one, fails instead of storing a second length.
pub fn check_commit(
    base: &LedgerState,
    flakes: &[Flake],
    graph_ids: impl FnOnce() -> Result<HashMap<Sid, GraphId>>,
    iri: impl Fn(&Sid) -> String,
) -> Result<()> {
    if !flakes.iter().any(is_vector_assertion) {
        return Ok(());
    }
    check_vectors(base, &base.novelty, flakes, &graph_ids()?, iri)
}

/// Refuse `flakes` if they hold a vector the ledger cannot store, given the
/// index of `base` and the commits in `novelty`.
///
/// Push passes the novelty that holds the earlier commits of the push.
pub fn check_vectors(
    base: &LedgerState,
    novelty: &Novelty,
    flakes: &[Flake],
    graph_ids: &HashMap<Sid, GraphId>,
    iri: impl Fn(&Sid) -> String,
) -> Result<()> {
    let store = binary_store(base);
    let mut lengths: HashMap<(Option<&Sid>, &Sid), u16> = HashMap::new();
    for flake in flakes.iter().filter(|f| is_vector_assertion(f)) {
        let FlakeValue::Vector(vector) = &flake.o else {
            continue;
        };
        let key = (flake.g.as_ref(), &flake.p);
        let dims = match lengths.get(&key) {
            Some(&dims) => Some(dims),
            None => {
                let g_id = match &flake.g {
                    None => Some(DEFAULT_GRAPH_ID),
                    Some(g) => graph_ids.get(g).copied(),
                };
                match g_id {
                    Some(g_id) => stored_length(base, store.as_deref(), novelty, g_id, &flake.p)?,
                    // A graph this commit creates holds no vectors yet.
                    None => None,
                }
            }
        };
        let dims = check_vector_f64(dims, vector).map_err(|e| refusal(flake, &iri, &e))?;
        lengths.insert(key, dims);
    }
    Ok(())
}

fn is_vector_assertion(flake: &Flake) -> bool {
    flake.op && matches!(flake.o, FlakeValue::Vector(_))
}

/// The length of the vectors `p` holds in graph `g_id`: the index's, or else
/// that of the first vector asserted since the index.
fn stored_length(
    base: &LedgerState,
    store: Option<&BinaryIndexStore>,
    novelty: &Novelty,
    g_id: GraphId,
    p: &Sid,
) -> Result<Option<u16>> {
    if let Some(store) = readable_index(base, store)? {
        let indexed = store
            .sid_to_p_id(p)
            .and_then(|p_id| store.vector_dims(g_id, p_id));
        if indexed.is_some() {
            return Ok(indexed);
        }
    }
    Ok(first_length_since_index(novelty, g_id, p))
}

/// The index store to read lengths from, if the ledger has an index.
///
/// Refuses, with [`TransactError::IndexNotLoaded`], when the ledger has an
/// index this check cannot read lengths from ([`crate::check_index_loaded`]),
/// or one read through no binary store. Such an index could hold the length,
/// so the write is refused rather than certified.
fn readable_index<'s>(
    base: &LedgerState,
    store: Option<&'s BinaryIndexStore>,
) -> Result<Option<&'s BinaryIndexStore>> {
    crate::check_index_loaded(&base.snapshot)?;
    match store {
        Some(store) if store.max_t() >= base.index_t() => Ok(Some(store)),
        None if base.index_t() == 0 => Ok(None),
        _ => Err(TransactError::IndexNotLoaded(format!(
            "no binary index at t={} to read vector lengths from",
            base.index_t()
        ))),
    }
}

/// The length of the first vector asserted on `p` in `novelty`.
///
/// Vectors sort after every other value, and every vector the arena can
/// hold sorts after one whose first element is `-INF`, so the read starts
/// at `p`'s first vector.
fn first_length_since_index(novelty: &Novelty, g_id: GraphId, p: &Sid) -> Option<u16> {
    let below = Flake::new(
        Sid::min(),
        p.clone(),
        FlakeValue::Vector(Arc::from([f64::NEG_INFINITY])),
        Sid::min(),
        0,
        true,
        None,
    );
    let top = Flake::new(
        Sid::max(),
        p.clone(),
        FlakeValue::max(),
        Sid::max(),
        0,
        true,
        None,
    );
    novelty
        .range_flakes(g_id, IndexType::Post, Some(&below), Some(&top), false)
        .find_map(|f| match &f.o {
            FlakeValue::Vector(v) if f.op => u16::try_from(v.len()).ok(),
            _ => None,
        })
}

fn refusal(flake: &Flake, iri: &impl Fn(&Sid) -> String, error: &VectorError) -> TransactError {
    invalid_vector(&iri(&flake.p), flake.g.as_ref().map(iri).as_deref(), error)
}

/// The refusal of a vector of `property`, in the named graph `graph` (or the
/// default graph), that the ledger cannot store. Every write path words it
/// this way.
pub(crate) fn invalid_vector(
    property: &str,
    graph: Option<&str>,
    error: &VectorError,
) -> TransactError {
    let graph = graph
        .map(|g| format!(" in graph <{g}>"))
        .unwrap_or_default();
    TransactError::InvalidVector(format!("<{property}>{graph}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flake(p: &Sid, o: FlakeValue, t: i64, op: bool) -> Flake {
        Flake::new(
            Sid::new(100, "s"),
            p.clone(),
            o,
            Sid::new(3, "dt"),
            t,
            op,
            None,
        )
    }

    fn vector(len: usize, first: f64) -> FlakeValue {
        FlakeValue::Vector(
            (0..len)
                .map(|i| first + i as f64)
                .collect::<Vec<_>>()
                .into(),
        )
    }

    /// A ledger with an index this check cannot read refuses a vector write
    /// rather than certify it; without an index there is nothing to read.
    #[test]
    fn an_index_that_is_not_loaded_refuses_a_vector() {
        let p = Sid::new(100, "embedding");
        let flakes = vec![flake(&p, vector(3, 0.5), 2, true)];
        let check = |index_t: i64| {
            let mut snapshot = fluree_db_core::LedgerSnapshot::genesis("test:main");
            snapshot.t = index_t;
            let base = LedgerState::new(snapshot, Novelty::new(index_t));
            check_commit(&base, &flakes, || Ok(HashMap::new()), ToString::to_string)
        };
        check(0).expect("no index: the first vector sets the length");
        match check(1) {
            Err(TransactError::IndexNotLoaded(msg)) => {
                assert!(msg.contains("index (t=1) is not loaded"), "{msg}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn the_first_vector_since_the_index_sets_the_length() {
        let before = Sid::new(100, "a");
        let p = Sid::new(100, "m");
        let after = Sid::new(100, "z");
        let mut novelty = Novelty::new(0);
        let flakes = vec![
            // Other values of `p` sort before its vectors.
            flake(&p, FlakeValue::String("text".into()), 1, true),
            flake(&p, FlakeValue::Double(-1.0e300), 1, true),
            flake(&p, FlakeValue::Long(-5), 1, true),
            // A retraction never sets the length; it is skipped.
            flake(&p, vector(4, -1.0e30), 1, false),
            flake(&p, vector(2, -5.0), 1, true),
            flake(&p, vector(2, 7.0), 1, true),
            flake(&before, vector(5, 0.0), 1, true),
            flake(&after, vector(6, 0.0), 1, true),
        ];
        novelty.apply_commit(flakes, 1, &HashMap::new()).unwrap();
        assert_eq!(first_length_since_index(&novelty, 0, &p), Some(2));
        assert_eq!(first_length_since_index(&novelty, 0, &before), Some(5));
        assert_eq!(first_length_since_index(&novelty, 0, &after), Some(6));
        assert_eq!(
            first_length_since_index(&novelty, 0, &Sid::new(100, "q")),
            None
        );
        assert_eq!(
            first_length_since_index(&novelty, 1, &p),
            None,
            "another graph"
        );
    }
}
