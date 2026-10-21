//! Reading RDF lists.
//!
//! A list has two stored shapes. A Turtle `( … )` or JSON-LD `@list` in object
//! position is stored compactly: its members are values of the owning subject
//! and predicate, each carrying its position in [`FlakeMeta::i`]. Anything
//! else — a list written out as `rdf:first`/`rdf:rest` triples, a second list
//! on one property, a nested list — is stored as that explicit chain. Both
//! are the same RDF, so a reader of a list-valued property reads both.
//!
//! [`FlakeMeta::i`]: crate::flake::FlakeMeta::i

use crate::comparator::IndexType;
use crate::error::Result;
use crate::flake::Flake;
use crate::graph_db_ref::GraphDbRef;
use crate::namespaces::is_rdf_nil;
use crate::query_bounds::{RangeMatch, RangeTest};
use crate::sid::Sid;
use crate::value::FlakeValue;
use fluree_vocab::namespaces::RDF;
use fluree_vocab::predicates::{RDF_FIRST, RDF_REST};
use std::collections::HashSet;

/// Cells walked before a chain is taken to be malformed.
const MAX_CHAIN_LEN: usize = 100_000;

/// The RDF lists among the values of one subject and predicate.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Lists {
    /// Each list's members in order: the compact list first, then each chain
    /// in value order. The empty list, `rdf:nil`, has no members.
    pub lists: Vec<Vec<FlakeValue>>,
    /// Values heading an `rdf:first`/`rdf:rest` chain that does not reach
    /// `rdf:nil`: a cell without exactly one `rdf:first` and one `rdf:rest`,
    /// or a cycle.
    pub malformed: usize,
    /// The values that are not lists, in value order.
    pub values: Vec<FlakeValue>,
}

impl Lists {
    /// No values at all.
    pub fn is_missing(&self) -> bool {
        self.lists.is_empty() && self.malformed == 0 && self.values.is_empty()
    }

    /// The members, when the values are exactly one well-formed list.
    pub fn single(mut self) -> Option<Vec<FlakeValue>> {
        (self.lists.len() == 1 && self.malformed == 0 && self.values.is_empty())
            .then(|| self.lists.pop())
            .flatten()
    }
}

/// Read `values`, the flakes of one subject and predicate, as RDF lists,
/// walking any explicit chain through `db`. Retractions are ignored.
pub async fn read_lists(db: GraphDbRef<'_>, values: &[Flake]) -> Result<Lists> {
    let mut members: Vec<(i32, &FlakeValue)> = Vec::new();
    let mut out = Lists::default();
    for flake in values.iter().filter(|f| f.op) {
        if let Some(index) = flake.m.as_ref().and_then(|m| m.i) {
            members.push((index, &flake.o));
        }
    }
    if !members.is_empty() {
        members.sort_by_key(|(index, _)| *index);
        out.lists
            .push(members.into_iter().map(|(_, o)| o.clone()).collect());
    }
    for flake in values.iter().filter(|f| f.op) {
        if flake.m.as_ref().and_then(|m| m.i).is_some() {
            continue;
        }
        match &flake.o {
            FlakeValue::Ref(head) => match walk_chain(db, head).await? {
                Chain::List(items) => out.lists.push(items),
                Chain::Malformed => out.malformed += 1,
                Chain::NotAList => out.values.push(flake.o.clone()),
            },
            other => out.values.push(other.clone()),
        }
    }
    Ok(out)
}

enum Chain {
    List(Vec<FlakeValue>),
    Malformed,
    NotAList,
}

/// Follow `rdf:rest` from `head` to `rdf:nil`, collecting each `rdf:first`.
async fn walk_chain(db: GraphDbRef<'_>, head: &Sid) -> Result<Chain> {
    let first = Sid::new(RDF, RDF_FIRST);
    let rest = Sid::new(RDF, RDF_REST);
    let mut items = Vec::new();
    let mut seen: HashSet<Sid> = HashSet::new();
    let mut cell = head.clone();
    loop {
        if is_rdf_nil(&cell) {
            return Ok(Chain::List(items));
        }
        if !seen.insert(cell.clone()) || items.len() >= MAX_CHAIN_LEN {
            return Ok(Chain::Malformed);
        }
        let firsts = objects(db, &cell, &first).await?;
        let rests = objects(db, &cell, &rest).await?;
        match (firsts.as_slice(), rests.as_slice()) {
            ([], []) if items.is_empty() => return Ok(Chain::NotAList),
            ([item], [FlakeValue::Ref(next)]) => {
                items.push(item.clone());
                cell = next.clone();
            }
            _ => return Ok(Chain::Malformed),
        }
    }
}

async fn objects(db: GraphDbRef<'_>, s: &Sid, p: &Sid) -> Result<Vec<FlakeValue>> {
    Ok(db
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch {
                s: Some(s.clone()),
                p: Some(p.clone()),
                ..Default::default()
            },
        )
        .await?
        .into_iter()
        .filter(|f| f.op)
        .map(|f| f.o)
        .collect())
}

/// Group flakes by subject, keeping their order within a subject. For the
/// flakes of one predicate, each group is one subject's values to pass to
/// [`read_lists`].
pub fn by_subject(mut flakes: Vec<Flake>) -> Vec<(Sid, Vec<Flake>)> {
    flakes.sort_by(|a, b| a.s.cmp(&b.s));
    let mut groups: Vec<(Sid, Vec<Flake>)> = Vec::new();
    for flake in flakes {
        match groups.last_mut() {
            Some((s, group)) if *s == flake.s => group.push(flake),
            _ => groups.push((flake.s.clone(), vec![flake])),
        }
    }
    groups
}
