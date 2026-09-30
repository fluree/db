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
//!   top-level `GRAPH`: never under OPTIONAL, UNION, MINUS, EXISTS, a
//!   subquery, SERVICE or a property path;
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
//! - (e) `T` writes to the graph `W` read: the default-graph template with
//!   the ledger's own default graph as the WHERE default; `GRAPH <g>`
//!   templates with a WHERE default of exactly `g` (WITH, or a single
//!   USING / JSON-LD `graph`/`from`) or with `W` inside `GRAPH <g>`, `g`
//!   a graph the ledger has; `GRAPH ?g` templates with `W` inside
//!   `GRAPH ?g`. A `GRAPH <iri>` whose name the WHERE dataset aliases to
//!   another graph is never a witness: it reads that graph, while a
//!   template naming the same IRI writes to the IRI's own graph.
//!
//! Subject and predicate are always nodes, and joins compare nodes
//! exactly, so (b) needs no condition like (d).

use crate::ir::{TemplateGraph, TemplateTerm, TripleTemplate};
use fluree_db_query::ir::GraphName;
use fluree_db_query::{Pattern, Ref, Term, TriplePattern, VarId};

/// The WHERE clause's default graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WhereDefault<'a> {
    /// No USING, WITH or JSON-LD `graph`/`from`: the ledger's default graph.
    Ledger,
    /// Exactly one graph, named by this IRI.
    Graph(&'a str),
    /// Anything else: several graphs, or none (USING NAMED alone).
    Other,
}

/// What the rule needs to know about the WHERE's dataset.
pub(crate) struct WitnessContext<'a> {
    pub(crate) default: WhereDefault<'a>,
    /// Whether the WHERE dataset resolves this graph name to a different
    /// graph (a dataset alias).
    pub(crate) aliased: &'a dyn Fn(&str) -> bool,
    /// Whether an IRI names a graph the ledger has.
    pub(crate) registered: &'a dyn Fn(&str) -> bool,
}

/// Where a candidate witness reads.
#[derive(Clone, Copy)]
enum ReadGraph<'p> {
    Default,
    Named(&'p GraphName),
}

/// A candidate witness and its position: top-level index, and index inside
/// a top-level `GRAPH` when it sits in one.
struct Candidate<'p> {
    triple: &'p TriplePattern,
    graph: ReadGraph<'p>,
    at: (usize, Option<usize>),
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
                at: (i, None),
            }),
            Pattern::Graph { name, patterns } => {
                for (j, inner) in patterns.iter().enumerate() {
                    if let Pattern::Triple(triple) = inner {
                        out.push(Candidate {
                            triple,
                            graph: ReadGraph::Named(name),
                            at: (i, Some(j)),
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
        (ReadGraph::Default, TemplateGraph::Default) => cx.default == WhereDefault::Ledger,
        (ReadGraph::Default, TemplateGraph::Iri(g)) => {
            cx.default == WhereDefault::Graph(g) && (cx.registered)(g)
        }
        (ReadGraph::Named(GraphName::Iri(read)), TemplateGraph::Iri(g)) => {
            read == g && !(cx.aliased)(g) && (cx.registered)(g)
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
/// top-level FILTER reading `v` alone (see (d)).
fn mentioned_elsewhere(v: VarId, patterns: &[Pattern], skip: (usize, Option<usize>)) -> bool {
    patterns.iter().enumerate().any(|(i, p)| match p {
        Pattern::Triple(_) if skip == (i, None) => false,
        Pattern::Graph { name, patterns } => {
            matches!(name, GraphName::Var(g) if *g == v)
                || patterns
                    .iter()
                    .enumerate()
                    .any(|(j, inner)| skip != (i, Some(j)) && mentions(inner, v))
        }
        other => mentions(other, v),
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

    fn registered(iri: &str) -> bool {
        iri == GRAPH_IRI
    }

    fn not_aliased(_: &str) -> bool {
        false
    }

    fn cx(default: WhereDefault<'_>) -> WitnessContext<'_> {
        WitnessContext {
            default,
            aliased: &not_aliased,
            registered: &registered,
        }
    }

    fn check(patterns: &[Pattern], t: TripleTemplate, cx: &WitnessContext<'_>) -> bool {
        witnessed_templates(patterns, &[t], cx)[0]
    }

    #[test]
    fn delete_where_and_same_shape_updates_are_witnessed() {
        let ledger = cx(WhereDefault::Ledger);
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
        let ledger = cx(WhereDefault::Ledger);
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
        let ledger = cx(WhereDefault::Ledger);
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
    fn optional_union_and_minus_do_not_witness() {
        let ledger = cx(WhereDefault::Ledger);
        let optional = Pattern::Optional(vec![where_sp_o()]);
        assert!(!check(&[optional], t_sp_o(), &ledger));
        let union = Pattern::Union(vec![vec![where_sp_o()], vec![]]);
        assert!(!check(&[union], t_sp_o(), &ledger));
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
        assert!(check(
            &[where_sp_o()],
            in_graph.clone(),
            &cx(WhereDefault::Graph(GRAPH_IRI))
        ));
        // ... but the ledger default read by a named-graph template is not.
        assert!(!check(
            &[where_sp_o()],
            in_graph.clone(),
            &cx(WhereDefault::Ledger)
        ));
        // USING <g> with a default-graph template.
        assert!(!check(
            &[where_sp_o()],
            t_sp_o(),
            &cx(WhereDefault::Graph(GRAPH_IRI))
        ));
        // Several USING graphs.
        assert!(!check(&[where_sp_o()], t_sp_o(), &cx(WhereDefault::Other)));
        // A graph the ledger does not have.
        let unknown = t_sp_o().in_graph("http://example.org/nope");
        assert!(!check(
            &[where_sp_o()],
            unknown,
            &cx(WhereDefault::Graph("http://example.org/nope"))
        ));

        // GRAPH <g> { … }
        let graph_block = |name: GraphName| Pattern::Graph {
            name,
            patterns: vec![where_sp_o()],
        };
        let ledger = cx(WhereDefault::Ledger);
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
        let alias_of_g1 = |iri: &str| iri == GRAPH_IRI;
        let aliased = WitnessContext {
            aliased: &alias_of_g1,
            ..cx(WhereDefault::Ledger)
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
