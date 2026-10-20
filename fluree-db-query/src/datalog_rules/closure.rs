//! Recursive rules that compute a transitive closure, evaluated as one
//! property path.
//!
//! A recursive group made of one rule shaped as a transitive closure derives,
//! at its fixpoint, exactly what one run of a body with a one-or-more
//! property path derives from the facts that existed when the group started.
//! With `R₀` those `R` facts:
//!
//! - right-linear `T(x,y) . R(y,z) → R(x,z)` derives `T⁺ ∘ R₀`, which is
//!   `T+(x,y) . R(y,z)`;
//! - left-linear `R(x,y) . T(y,z) → R(x,z)` derives `R₀ ∘ T⁺`, which is
//!   `R(x,y) . T+(y,z)`;
//! - nonlinear `R(x,y) . R(y,z) → R(x,z)` derives the pairs two or more `R₀`
//!   hops apart, which is `R+(x,y) . R(y,z)`.
//!
//! `T` is a triple pattern with a constant predicate other than `R`, written
//! in either direction. Nothing else qualifies: one head, a body of exactly
//! the two triple patterns over three distinct variables, constant
//! predicates, no datatype constraint.
//!
//! A path follows only IRI-valued objects. In the right-linear and nonlinear
//! shapes every node the path passes, its ends included, is the subject of
//! some fact, so none is a literal. In the left-linear shape the path ends at
//! the head's object, and a literal object of the path's predicate would end
//! a derivation the path cannot follow, so that shape runs as a path only
//! while the predicate has no literal object.

use super::{DatalogRule, HeadTerm};
use crate::ir::{
    PathModifier, Pattern, PropertyPathPattern, Query, QueryOutput, Ref, Term, TriplePattern,
};
use crate::var_registry::VarId;
use fluree_db_core::Sid;

/// A transitive-closure rule and the body that derives its whole closure in
/// one run.
#[derive(Debug, Clone)]
pub(super) struct Closure {
    /// The rule body with its recursion replaced by a one-or-more path.
    pub(super) body: Query,
    /// The path's predicate, when a literal object of it would end a
    /// derivation the path cannot follow.
    pub(super) requires_node_objects: Option<Sid>,
}

impl Closure {
    /// The closure form of `rule`, when it has one. Only sound for the sole
    /// rule of its recursive group.
    pub(super) fn compile(rule: &DatalogRule) -> Option<Self> {
        let [head] = rule.heads.as_slice() else {
            return None;
        };
        if head.graph.is_some() {
            return None;
        }
        let (HeadTerm::Var(x), HeadTerm::Sid(r), HeadTerm::Var(z)) =
            (&head.subject, &head.predicate, &head.object)
        else {
            return None;
        };
        let (x, z) = (*x, *z);
        let query = &rule.body.query;
        if query.post_values.is_some() || !query.order_binds.is_empty() {
            return None;
        }
        let [Pattern::Triple(first), Pattern::Triple(second)] = query.patterns.as_slice() else {
            return None;
        };
        let (first, second) = (Edge::of(first)?, Edge::of(second)?);

        let (patterns, requires_node_objects) = match (first.p == *r, second.p == *r) {
            (true, true) => {
                let (left, right) = if first.s == x {
                    (first, second)
                } else {
                    (second, first)
                };
                let y = left.o;
                if left.s != x || right.s != y || right.o != z || !distinct(x, y, z) {
                    return None;
                }
                (vec![path(x, r, y), right.triple(r)], None)
            }
            (true, false) | (false, true) => {
                let (recursive, step) = if first.p == *r {
                    (first, second)
                } else {
                    (second, first)
                };
                let (s, o) = (recursive.s, recursive.o);
                if o == z && distinct(x, s, z) && step.links(x, s) {
                    (
                        vec![path(step.s, &step.p, step.o), recursive.triple(r)],
                        None,
                    )
                } else if s == x && distinct(x, o, z) && step.links(o, z) {
                    (
                        vec![recursive.triple(r), path(step.s, &step.p, step.o)],
                        Some(step.p),
                    )
                } else {
                    return None;
                }
            }
            (false, false) => return None,
        };

        let mut body = query.clone();
        body.patterns = patterns;
        body.output = QueryOutput::select_all(vec![x, z]);
        Some(Self {
            body,
            requires_node_objects,
        })
    }
}

/// A triple pattern `?s p ?o` over two variables with a constant predicate
/// and no datatype constraint.
struct Edge {
    s: VarId,
    p: Sid,
    o: VarId,
}

impl Edge {
    fn of(triple: &TriplePattern) -> Option<Self> {
        match (&triple.s, &triple.p, &triple.o, &triple.dtc) {
            (Ref::Var(s), Ref::Sid(p), Term::Var(o), None) if s != o => Some(Self {
                s: *s,
                p: p.clone(),
                o: *o,
            }),
            _ => None,
        }
    }

    /// Whether the edge joins `a` and `b`, in either direction.
    fn links(&self, a: VarId, b: VarId) -> bool {
        (self.s == a && self.o == b) || (self.s == b && self.o == a)
    }

    fn triple(&self, p: &Sid) -> Pattern {
        Pattern::Triple(TriplePattern::new(
            Ref::Var(self.s),
            Ref::Sid(p.clone()),
            Term::Var(self.o),
        ))
    }
}

fn path(s: VarId, p: &Sid, o: VarId) -> Pattern {
    Pattern::PropertyPath(PropertyPathPattern::new(
        Ref::Var(s),
        p.clone(),
        PathModifier::OneOrMore,
        Ref::Var(o),
    ))
}

fn distinct(a: VarId, b: VarId, c: VarId) -> bool {
    a != b && b != c && a != c
}
