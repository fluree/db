//! Semi-naive re-evaluation inside a recursive group.
//!
//! After a recursive group's first pass, a rule can derive something new
//! only from a match that uses at least one fact the group derived in the
//! previous pass. When every read of the group's facts in a rule body is a
//! top-level triple pattern, the rule re-runs once per such pattern, with
//! that pattern seeded by the previous pass's facts and the rest of the body
//! reading everything derived so far. A rule with two recursive patterns
//! (`?x anc ?y . ?y anc ?z`) runs twice, so a new fact in either position is
//! extended. Any other read of the group's facts — in a UNION branch, an
//! EXISTS, a property path, a subquery — re-runs the whole body every pass.

use super::dependency::{pattern_reads, query_tail_reads, Access};
use crate::binding::Binding;
use crate::ir::{Pattern, Query, Ref, Term, TriplePattern};
use crate::parse::IriEncoder;
use crate::var_registry::VarId;
use fluree_db_core::{Flake, Sid};
use std::collections::HashSet;

/// How a rule in a recursive group runs after the group's first pass.
#[derive(Debug, Clone)]
pub(super) enum Reevaluation {
    /// The whole body, over everything derived so far.
    Full,
    /// Once per recursive pattern, that pattern seeded with the facts the
    /// group derived in the previous pass.
    Delta(Vec<DeltaAtom>),
}

impl Reevaluation {
    /// Plan a rule whose body is `query`, in a group whose rules write
    /// `group_writes`.
    pub(super) fn plan<E: IriEncoder + ?Sized>(
        query: &Query,
        group_writes: &[Access],
        encoder: &E,
    ) -> Self {
        let observes_group = |reads: Vec<Access>| {
            reads
                .iter()
                .any(|read| group_writes.iter().any(|write| read.may_observe(write)))
        };
        if observes_group(query_tail_reads(query, encoder)) {
            return Reevaluation::Full;
        }
        let mut atoms = Vec::new();
        for (position, pattern) in query.patterns.iter().enumerate() {
            if !observes_group(pattern_reads(std::slice::from_ref(pattern), encoder)) {
                continue;
            }
            match pattern {
                Pattern::Triple(triple) => atoms.push(DeltaAtom::new(position, triple, encoder)),
                _ => return Reevaluation::Full,
            }
        }
        if atoms.is_empty() {
            Reevaluation::Full
        } else {
            Reevaluation::Delta(atoms)
        }
    }
}

/// One position of a recursive triple pattern.
#[derive(Debug, Clone)]
enum AtomTerm {
    Var(VarId),
    /// A constant node. An IRI the body left for the scan to encode is
    /// encoded the way rule heads are, so it equals the SID of any derived
    /// fact naming it.
    Node(Sid),
    /// A literal: matched by the pattern, not here.
    Other,
}

impl AtomTerm {
    fn of_ref<E: IriEncoder + ?Sized>(r: &Ref, encoder: &E) -> Self {
        match r {
            Ref::Var(v) => AtomTerm::Var(*v),
            Ref::Sid(sid) => AtomTerm::Node(sid.clone()),
            Ref::Iri(iri) => Self::encoded(iri, encoder),
        }
    }

    fn of_term<E: IriEncoder + ?Sized>(t: &Term, encoder: &E) -> Self {
        match t {
            Term::Var(v) => AtomTerm::Var(*v),
            Term::Sid(sid) => AtomTerm::Node(sid.clone()),
            Term::Iri(iri) => Self::encoded(iri, encoder),
            Term::Value(_) => AtomTerm::Other,
        }
    }

    fn encoded<E: IriEncoder + ?Sized>(iri: &str, encoder: &E) -> Self {
        encoder
            .encode_iri(iri)
            .map_or(AtomTerm::Other, AtomTerm::Node)
    }
}

/// A top-level triple pattern that may match facts its group derives.
#[derive(Debug, Clone)]
pub(super) struct DeltaAtom {
    /// Index among the body's top-level patterns.
    position: usize,
    terms: [AtomTerm; 3],
    /// The seed binds the pattern's variables, in first-occurrence order.
    vars: Vec<VarId>,
    /// The seed replaces the pattern only when it matches facts exactly as
    /// a scan would. A literal, an IRI the scan encodes itself, a datatype
    /// constraint, or a variable predicate (whose scan hides system
    /// predicates) keeps the pattern after the seed to do that matching.
    keep_pattern: bool,
}

impl DeltaAtom {
    fn new<E: IriEncoder + ?Sized>(position: usize, triple: &TriplePattern, encoder: &E) -> Self {
        let terms = [
            AtomTerm::of_ref(&triple.s, encoder),
            AtomTerm::of_ref(&triple.p, encoder),
            AtomTerm::of_term(&triple.o, encoder),
        ];
        let mut vars = Vec::new();
        for term in &terms {
            if let AtomTerm::Var(v) = term {
                if !vars.contains(v) {
                    vars.push(*v);
                }
            }
        }
        let keep_pattern = triple.dtc.is_some()
            || matches!(triple.p, Ref::Var(_) | Ref::Iri(_))
            || matches!(triple.s, Ref::Iri(_))
            || matches!(triple.o, Term::Iri(_) | Term::Value(_));
        Self {
            position,
            terms,
            vars,
            keep_pattern,
        }
    }

    /// `query` with this atom restricted to `delta`, or `None` when no fact
    /// in `delta` can match it.
    pub(super) fn seeded(&self, query: &Query, delta: &[Flake]) -> Option<Query> {
        // Distinct rows, in fact order: two facts that differ only in a
        // position the pattern leaves to its own matching bind the same row.
        let mut distinct: HashSet<Vec<Binding>> = HashSet::new();
        let rows: Vec<Vec<Binding>> = delta
            .iter()
            .filter_map(|fact| self.row(fact))
            .filter(|row| distinct.insert(row.clone()))
            .collect();
        if rows.is_empty() {
            return None;
        }
        let seed = Pattern::Values {
            vars: self.vars.clone(),
            rows,
        };
        let mut seeded = query.clone();
        if self.keep_pattern {
            seeded.patterns.insert(self.position, seed);
        } else {
            seeded.patterns[self.position] = seed;
        }
        Some(seeded)
    }

    /// The variable bindings `fact` gives this pattern, or `None` when it
    /// cannot match. A top-level pattern reads the graph the rules run
    /// against, so a fact derived into a named graph never matches it.
    fn row(&self, fact: &Flake) -> Option<Vec<Binding>> {
        if fact.g.is_some() {
            return None;
        }
        let values = [
            Binding::sid(fact.s.clone()),
            Binding::sid(fact.p.clone()),
            Binding::from_object_with_meta(fact.o.clone(), fact.dt.clone(), fact.m.clone()),
        ];
        let mut row = vec![Binding::Unbound; self.vars.len()];
        for (term, value) in self.terms.iter().zip(values) {
            match term {
                AtomTerm::Var(v) => {
                    let slot = &mut row[self.vars.iter().position(|x| x == v)?];
                    if matches!(slot, Binding::Unbound) {
                        *slot = value;
                    } else if *slot != value {
                        return None;
                    }
                }
                AtomTerm::Node(sid) => {
                    if !matches!(&value, Binding::Sid { sid: bound, .. } if bound == sid) {
                        return None;
                    }
                }
                AtomTerm::Other => {}
            }
        }
        Some(row)
    }
}
