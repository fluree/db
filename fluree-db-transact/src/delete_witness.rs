//! Which DELETE templates a WHERE triple witnesses.
//!
//! A DELETE template instantiated from a WHERE row usually names the very
//! fact a WHERE triple just matched: `DELETE { ?s ex:p ?o } WHERE { ?s ex:p
//! ?o }`. Such a row is the decode of a stored fact read at the same `t`,
//! so it may be retracted without reading the fact again — the zero-lookup
//! path bulk deletes depend on. Everything this analysis cannot prove goes
//! through the resolver, which retracts only facts it finds stored.
//!
//! A template `T` is **witnessed** by a WHERE triple `W` when:
//! - (a) `W` sits at the top level of the WHERE, or directly inside a
//!   top-level `GRAPH`; or it is the only pattern of an OPTIONAL there, or
//!   of one branch of a top-level UNION (or of a UNION a top-level OPTIONAL
//!   holds alone). Never under MINUS, EXISTS, a subquery, SERVICE or a
//!   property path. An OPTIONAL or UNION row that binds `W`'s object is the
//!   decode of the fact `W` matched; one that does not leaves the object
//!   unbound (by (d) nothing else binds it), and a template with an unbound
//!   variable emits no intent;
//! - (b) `T`'s subject and predicate are `W`'s — the same variable or the
//!   same Sid;
//! - (c) `T`'s object is a variable `v` and `W`'s object is the same `v`,
//!   and `T` carries no datatype, language or list constraint of its own.
//!   A constant object is never witnessed: matching is looser than term
//!   identity (a bare number matches any numeric datatype);
//! - (d) nothing else in the WHERE mentions `v` — no other pattern,
//!   VALUES, BIND or UNWIND — except a FILTER that reads `v` and no other
//!   variable. A join across two patterns unifies literals looser than
//!   term identity, so the row could carry the other fact's datatype or
//!   language tag; a filter equating `v` with another variable could be
//!   folded into exactly such a join;
//! - (e) `T` writes to the graph `W` read, compared as ledger graph ids: the
//!   graph the WHERE resolved for `W` (its single default graph, or the
//!   dataset's graph for `GRAPH <name>`) is the graph the ledger has under
//!   the template's IRI (the default graph for a default-graph template).
//!   A graph name read one way and written another — a dataset alias, or a
//!   name the WHERE maps to the default graph — is therefore never a
//!   witness. `GRAPH ?g` templates need `W` inside `GRAPH ?g`.
//!
//! Subject and predicate are always nodes, and joins compare nodes
//! exactly, so (b) needs no condition like (d).

use crate::ir::{TemplateGraph, TemplateTerm, TripleTemplate};
use fluree_db_core::GraphId;
use fluree_db_query::ir::GraphName;
use fluree_db_query::{Pattern, Ref, Term, TriplePattern, VarId};

/// What the rule needs to know about the WHERE's dataset, in ledger graph
/// ids: the same resolution the WHERE reads through.
pub(crate) struct WitnessContext<'a> {
    /// The graph the WHERE's default graph reads, when it reads exactly one
    /// (`None`: several, or none — USING NAMED alone).
    pub(crate) default: Option<GraphId>,
    /// The graph `GRAPH <name>` reads in the WHERE dataset.
    pub(crate) read: &'a dyn Fn(&str) -> Option<GraphId>,
    /// The graph a template's `GRAPH <iri>` writes, when the ledger has it.
    pub(crate) written: &'a dyn Fn(&str) -> Option<GraphId>,
}

/// Where a candidate witness reads.
#[derive(Clone, Copy)]
enum ReadGraph<'p> {
    Default,
    Named(&'p GraphName),
}

/// Where a candidate witness sits, so the (d) check can skip it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Site {
    /// Top-level pattern `i`: the triple, or an OPTIONAL holding only it.
    Top(usize),
    /// Pattern `j` of the top-level `GRAPH` at `i`: the triple, or an
    /// OPTIONAL holding only it.
    InGraph(usize, usize),
    /// Branch `b` of the UNION at top-level `i` (see [`union_branches`]),
    /// which holds only the triple.
    UnionBranch(usize, usize),
}

/// The branches of a top-level UNION, or of a UNION a top-level OPTIONAL
/// holds alone.
fn union_branches(p: &Pattern) -> Option<&[Vec<Pattern>]> {
    match p {
        Pattern::Union(branches) => Some(branches),
        Pattern::Optional(group) => match group.as_slice() {
            [Pattern::Union(branches)] => Some(branches),
            _ => None,
        },
        _ => None,
    }
}

/// A candidate witness.
struct Candidate<'p> {
    triple: &'p TriplePattern,
    graph: ReadGraph<'p>,
    at: Site,
}

/// The triple an OPTIONAL group or UNION branch holds, when it holds only
/// that.
fn sole_triple(group: &[Pattern]) -> Option<&TriplePattern> {
    match group {
        [Pattern::Triple(triple)] => Some(triple),
        _ => None,
    }
}

/// For each template, whether a WHERE triple witnesses it.
pub(crate) fn witnessed_templates(
    patterns: &[Pattern],
    templates: &[TripleTemplate],
    cx: &WitnessContext<'_>,
) -> Vec<bool> {
    let candidates = candidates(patterns);
    templates
        .iter()
        .map(|t| candidates.iter().any(|w| witnesses(w, t, patterns, cx)))
        .collect()
}

fn candidates(patterns: &[Pattern]) -> Vec<Candidate<'_>> {
    let mut out = Vec::new();
    for (i, p) in patterns.iter().enumerate() {
        match p {
            Pattern::Triple(triple) => out.push(Candidate {
                triple,
                graph: ReadGraph::Default,
                at: Site::Top(i),
            }),
            Pattern::Optional(group) if sole_triple(group).is_some() => {
                if let Some(triple) = sole_triple(group) {
                    out.push(Candidate {
                        triple,
                        graph: ReadGraph::Default,
                        at: Site::Top(i),
                    });
                }
            }
            Pattern::Optional(_) | Pattern::Union(_) => {
                for (b, branch) in union_branches(p).into_iter().flatten().enumerate() {
                    if let Some(triple) = sole_triple(branch) {
                        out.push(Candidate {
                            triple,
                            graph: ReadGraph::Default,
                            at: Site::UnionBranch(i, b),
                        });
                    }
                }
            }
            Pattern::Graph { name, patterns } => {
                for (j, inner) in patterns.iter().enumerate() {
                    let triple = match inner {
                        Pattern::Triple(triple) => Some(triple),
                        Pattern::Optional(group) => sole_triple(group),
                        _ => None,
                    };
                    if let Some(triple) = triple {
                        out.push(Candidate {
                            triple,
                            graph: ReadGraph::Named(name),
                            at: Site::InGraph(i, j),
                        });
                    }
                }
            }
            _ => {}
        }
    }
    out
}

fn witnesses(
    w: &Candidate<'_>,
    t: &TripleTemplate,
    patterns: &[Pattern],
    cx: &WitnessContext<'_>,
) -> bool {
    // (c): a variable object, the same variable, no constraint of T's own.
    let (TemplateTerm::Var(v), Term::Var(wv)) = (&t.object, &w.triple.o) else {
        return false;
    };
    if v != wv || t.dtc.is_some() || t.list_index.is_some() {
        return false;
    }
    // (b)
    if !same_node(&t.subject, &w.triple.s) || !same_node(&t.predicate, &w.triple.p) {
        return false;
    }
    // (e)
    let same_graph = match (w.graph, &t.graph) {
        (ReadGraph::Default, TemplateGraph::Default) => cx.default == Some(0),
        (ReadGraph::Default, TemplateGraph::Iri(g)) => {
            cx.default.is_some() && cx.default == (cx.written)(g)
        }
        (ReadGraph::Named(GraphName::Iri(read)), TemplateGraph::Iri(g)) => {
            let read = (cx.read)(read);
            read.is_some() && read == (cx.written)(g)
        }
        (ReadGraph::Named(GraphName::Var(read)), TemplateGraph::Var(g)) => read == g,
        _ => false,
    };
    // (d)
    same_graph && !mentioned_elsewhere(*v, patterns, w.at)
}

fn same_node(t: &TemplateTerm, r: &Ref) -> bool {
    match (t, r) {
        (TemplateTerm::Var(a), Ref::Var(b)) => a == b,
        (TemplateTerm::Sid(a), Ref::Sid(b)) => a == b,
        _ => false,
    }
}

/// Whether anything but the witness at `skip` mentions `v`, other than a
/// FILTER reading `v` alone (see (d)).
fn mentioned_elsewhere(v: VarId, patterns: &[Pattern], skip: Site) -> bool {
    patterns.iter().enumerate().any(|(i, p)| match p {
        _ if skip == Site::Top(i) => false,
        Pattern::Graph { name, patterns } => {
            matches!(name, GraphName::Var(g) if *g == v)
                || patterns
                    .iter()
                    .enumerate()
                    .any(|(j, inner)| skip != Site::InGraph(i, j) && mentions(inner, v))
        }
        other => match union_branches(other) {
            Some(branches) => branches.iter().enumerate().any(|(b, branch)| {
                skip != Site::UnionBranch(i, b) && branch.iter().any(|inner| mentions(inner, v))
            }),
            None => mentions(other, v),
        },
    })
}

fn mentions(p: &Pattern, v: VarId) -> bool {
    match p {
        Pattern::Filter(expr) => {
            let vars = expr.referenced_vars();
            vars.contains(&v) && vars.iter().any(|x| *x != v)
        }
        other => other.referenced_vars().contains(&v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
    use fluree_db_query::{Expression, Function};
    use std::sync::Arc;

    const S: VarId = VarId(0);
    const P: VarId = VarId(1);
    const O: VarId = VarId(2);
    const X: VarId = VarId(3);
    const G: VarId = VarId(4);
    const GRAPH_IRI: &str = "http://example.org/g1";

    fn pred() -> Sid {
        Sid::new(100, "p")
    }

    fn triple(s: Ref, p: Ref, o: Term) -> Pattern {
        Pattern::Triple(TriplePattern::new(s, p, o))
    }

    /// `?s ex:p ?o`
    fn where_sp_o() -> Pattern {
        triple(Ref::Var(S), Ref::Sid(pred()), Term::Var(O))
    }

    /// DELETE template `?s ex:p ?o`.
    fn t_sp_o() -> TripleTemplate {
        TripleTemplate::new(
            TemplateTerm::Var(S),
            TemplateTerm::Sid(pred()),
            TemplateTerm::Var(O),
        )
    }

    /// The ledger has one named graph, `GRAPH_IRI`, as graph id 1.
    const G1: GraphId = 1;

    fn written(iri: &str) -> Option<GraphId> {
        (iri == GRAPH_IRI).then_some(G1)
    }

    /// A WHERE dataset that reads every name as the graph the ledger has
    /// under it.
    fn cx(default: Option<GraphId>) -> WitnessContext<'static> {
        WitnessContext {
            default,
            read: &written,
            written: &written,
        }
    }

    fn check(patterns: &[Pattern], t: TripleTemplate, cx: &WitnessContext<'_>) -> bool {
        witnessed_templates(patterns, &[t], cx)[0]
    }

    #[test]
    fn delete_where_and_same_shape_updates_are_witnessed() {
        let ledger = cx(Some(0));
        assert!(check(&[where_sp_o()], t_sp_o(), &ledger));
        // DELETE WHERE { ?s ?p ?o }
        let spo = triple(Ref::Var(S), Ref::Var(P), Term::Var(O));
        let t = TripleTemplate::new(
            TemplateTerm::Var(S),
            TemplateTerm::Var(P),
            TemplateTerm::Var(O),
        );
        assert!(check(&[spo], t, &ledger));
        // A join on the subject keeps it witnessed: nodes compare exactly.
        let other = triple(
            Ref::Var(S),
            Ref::Sid(Sid::new(100, "q")),
            Term::Value(FlakeValue::Long(1)),
        );
        assert!(check(&[other, where_sp_o()], t_sp_o(), &ledger));
        // VALUES on the subject only.
        let values = Pattern::Values {
            vars: vec![S],
            rows: vec![vec![fluree_db_query::Binding::sid(Sid::new(100, "a"))]],
        };
        assert!(check(&[values, where_sp_o()], t_sp_o(), &ledger));
        // A FILTER that reads only the object.
        let filter = Pattern::Filter(Expression::Call {
            func: Function::Gt,
            args: vec![Expression::Var(O), Expression::Const(FlakeValue::Long(5))],
        });
        assert!(check(&[where_sp_o(), filter], t_sp_o(), &ledger));
    }

    #[test]
    fn constants_and_constraints_are_not_witnessed() {
        let ledger = cx(Some(0));
        // DELETE DATA: no WHERE at all.
        assert!(!check(&[], t_sp_o(), &ledger));
        // A constant object.
        let t = TripleTemplate::new(
            TemplateTerm::Var(S),
            TemplateTerm::Sid(pred()),
            TemplateTerm::Value(FlakeValue::Long(1)),
        );
        let w = triple(
            Ref::Var(S),
            Ref::Sid(pred()),
            Term::Value(FlakeValue::Long(1)),
        );
        assert!(!check(&[w], t, &ledger));
        // A template with its own language tag or list position.
        let tagged = t_sp_o().with_dtc(DatatypeConstraint::LangTag(Arc::from("fr")));
        assert!(!check(&[where_sp_o()], tagged, &ledger));
        let listed = t_sp_o().with_list_index(0);
        assert!(!check(&[where_sp_o()], listed, &ledger));
        // A different predicate.
        let t = TripleTemplate::new(
            TemplateTerm::Var(S),
            TemplateTerm::Sid(Sid::new(100, "q")),
            TemplateTerm::Var(O),
        );
        assert!(!check(&[where_sp_o()], t, &ledger));
    }

    #[test]
    fn an_object_bound_twice_is_not_witnessed() {
        let ledger = cx(Some(0));
        // Cross-pattern: the object is joined with another predicate's value.
        let other = triple(Ref::Var(X), Ref::Sid(Sid::new(100, "q")), Term::Var(O));
        assert!(!check(&[where_sp_o(), other], t_sp_o(), &ledger));
        // Repeated: the same pattern twice.
        assert!(!check(&[where_sp_o(), where_sp_o()], t_sp_o(), &ledger));
        // VALUES / BIND on the object.
        let values = Pattern::Values {
            vars: vec![O],
            rows: vec![vec![fluree_db_query::Binding::lit(
                FlakeValue::Long(1),
                Sid::new(2, "integer"),
            )]],
        };
        assert!(!check(&[values, where_sp_o()], t_sp_o(), &ledger));
        let bind = Pattern::Bind {
            var: O,
            expr: Expression::Var(X),
        };
        assert!(!check(&[where_sp_o(), bind], t_sp_o(), &ledger));
        // A FILTER equating the object with another variable could fold into
        // a join.
        let filter = Pattern::Filter(Expression::Call {
            func: Function::Eq,
            args: vec![Expression::Var(O), Expression::Var(X)],
        });
        let x = triple(Ref::Var(X), Ref::Sid(Sid::new(100, "q")), Term::Var(X));
        assert!(!check(&[where_sp_o(), x, filter], t_sp_o(), &ledger));
    }

    #[test]
    fn a_sole_optional_or_union_triple_witnesses() {
        let ledger = cx(Some(0));
        // `?s ex:q ?x OPTIONAL { ?s ex:p ?o }`: the Cypher SET shape.
        let q = triple(Ref::Var(S), Ref::Sid(Sid::new(100, "q")), Term::Var(X));
        let optional = Pattern::Optional(vec![where_sp_o()]);
        assert!(check(&[q.clone(), optional.clone()], t_sp_o(), &ledger));
        assert!(check(&[optional], t_sp_o(), &ledger));
        // One UNION branch, the object bound in no other.
        let union = Pattern::Union(vec![vec![where_sp_o()], vec![q.clone()]]);
        assert!(check(&[union], t_sp_o(), &ledger));
        // A UNION an OPTIONAL holds alone: the Cypher DETACH DELETE shape,
        // whose inbound branch binds the object elsewhere.
        let s_p_n = triple(Ref::Var(X), Ref::Var(P), Term::Var(S));
        let detach = Pattern::Optional(vec![Pattern::Union(vec![
            vec![triple(Ref::Var(S), Ref::Var(P), Term::Var(O))],
            vec![s_p_n],
        ])]);
        let out_t = TripleTemplate::new(
            TemplateTerm::Var(S),
            TemplateTerm::Var(P),
            TemplateTerm::Var(O),
        );
        assert!(check(&[detach], out_t, &ledger));
        // Inside a top-level GRAPH.
        let in_graph = Pattern::Graph {
            name: GraphName::Iri(Arc::from(GRAPH_IRI)),
            patterns: vec![q, Pattern::Optional(vec![where_sp_o()])],
        };
        assert!(check(&[in_graph], t_sp_o().in_graph(GRAPH_IRI), &ledger));
    }

    #[test]
    fn optional_union_and_minus_do_not_witness_otherwise() {
        let ledger = cx(Some(0));
        let q_o = triple(Ref::Var(S), Ref::Sid(Sid::new(100, "q")), Term::Var(O));
        // An OPTIONAL holding more than the triple.
        let optional = Pattern::Optional(vec![where_sp_o(), q_o.clone()]);
        assert!(!check(&[optional], t_sp_o(), &ledger));
        // Another UNION branch binds the object from another predicate.
        let union = Pattern::Union(vec![vec![where_sp_o()], vec![q_o.clone()]]);
        assert!(!check(&[union], t_sp_o(), &ledger));
        // A UNION branch holding more than the triple.
        let union = Pattern::Union(vec![vec![where_sp_o(), q_o], vec![]]);
        assert!(!check(&[union], t_sp_o(), &ledger));
        // A nested OPTIONAL.
        let nested = Pattern::Optional(vec![Pattern::Optional(vec![where_sp_o()])]);
        assert!(!check(&[nested], t_sp_o(), &ledger));
        // A top-level witness plus an OPTIONAL that mentions the object.
        let optional = Pattern::Optional(vec![triple(
            Ref::Var(S),
            Ref::Sid(Sid::new(100, "q")),
            Term::Var(O),
        )]);
        assert!(!check(&[where_sp_o(), optional], t_sp_o(), &ledger));
        let minus = Pattern::Minus(vec![triple(
            Ref::Var(S),
            Ref::Sid(Sid::new(100, "q")),
            Term::Var(O),
        )]);
        assert!(!check(&[where_sp_o(), minus], t_sp_o(), &ledger));
    }

    #[test]
    fn graph_contexts_must_agree() {
        let in_graph = t_sp_o().in_graph(GRAPH_IRI);
        // WITH <g> / a single USING <g>.
        assert!(check(&[where_sp_o()], in_graph.clone(), &cx(Some(G1))));
        // ... but the ledger default read by a named-graph template is not.
        assert!(!check(&[where_sp_o()], in_graph.clone(), &cx(Some(0))));
        // USING <g> with a default-graph template.
        assert!(!check(&[where_sp_o()], t_sp_o(), &cx(Some(G1))));
        // Several USING graphs.
        assert!(!check(&[where_sp_o()], t_sp_o(), &cx(None)));
        // A graph the ledger does not have: the WHERE reads some graph, the
        // template writes a new one.
        let unknown = t_sp_o().in_graph("http://example.org/nope");
        assert!(!check(&[where_sp_o()], unknown, &cx(Some(0))));
        // One name, read as the default graph and written as a named graph
        // the ledger has under it: not the same graph.
        assert!(!check(&[where_sp_o()], in_graph.clone(), &cx(Some(0))));

        // GRAPH <g> { … }
        let graph_block = |name: GraphName| Pattern::Graph {
            name,
            patterns: vec![where_sp_o()],
        };
        let ledger = cx(Some(0));
        assert!(check(
            &[graph_block(GraphName::Iri(Arc::from(GRAPH_IRI)))],
            in_graph.clone(),
            &ledger
        ));
        assert!(!check(
            &[graph_block(GraphName::Iri(Arc::from(GRAPH_IRI)))],
            t_sp_o(),
            &ledger
        ));
        // A name the dataset aliases to another graph reads that graph.
        let alias_of_g2 = |iri: &str| (iri == GRAPH_IRI).then_some(2);
        let aliased = WitnessContext {
            read: &alias_of_g2,
            ..cx(Some(0))
        };
        assert!(!check(
            &[graph_block(GraphName::Iri(Arc::from(GRAPH_IRI)))],
            in_graph,
            &aliased
        ));
        // GRAPH ?g { … } with a GRAPH ?g template.
        assert!(check(
            &[graph_block(GraphName::Var(G))],
            t_sp_o().with_graph_var(G),
            &ledger
        ));
        assert!(!check(&[graph_block(GraphName::Var(G))], t_sp_o(), &ledger));
    }
}
