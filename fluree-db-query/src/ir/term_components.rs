//! The components of a triple term, as a relation the planner can join on.
//!
//! `TermComponents(?t, s, p, o)` holds when `?t` is the triple term
//! `<<( s p o )>>`. The reified-edge lowering emits it beside the link
//! `?r rdf:reifies ?t`, so a component variable shared with another pattern
//! is a join the planner sees, and a bound subject can drive the term
//! dictionary's subject-first reverse tree instead of reading every link.

use crate::binding::Binding;
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::ir::{Expression, Function, Pattern};
use crate::parse::encode::IriEncoder;
use crate::var_registry::{VarId, VarRegistry};
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};

/// One component position of a [`TermComponentsPattern`].
#[derive(Debug, Clone, PartialEq)]
pub enum Component {
    /// Unconstrained: nothing reads it.
    Any,
    /// Bound by the pattern, or joined on when an earlier pattern bound it.
    Var(VarId),
    /// A constant node: subject, predicate, or IRI object.
    Node(Sid),
    /// A constant literal object, with its datatype or language tag.
    Literal(FlakeValue, DatatypeConstraint),
}

impl Component {
    pub fn var(&self) -> Option<VarId> {
        match self {
            Component::Var(v) => Some(*v),
            _ => None,
        }
    }
}

/// `TermComponents(term, subject, predicate, object)`.
#[derive(Debug, Clone, PartialEq)]
pub struct TermComponentsPattern {
    pub term: VarId,
    pub subject: Component,
    pub predicate: Component,
    pub object: Component,
}

impl TermComponentsPattern {
    pub fn components(&self) -> [&Component; 3] {
        [&self.subject, &self.predicate, &self.object]
    }

    /// The term variable, then each component variable.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        std::iter::once(self.term)
            .chain(self.components().into_iter().filter_map(Component::var))
            .collect()
    }

    pub fn produced_vars(&self) -> Vec<VarId> {
        self.referenced_vars()
    }
}

/// Lower a reified-triple pattern to the link form: one
/// `annotation rdf:reifies ?__term` triple whose object is a triple-term
/// handle, and `TermComponents(?__term, s, p, o)` relating the term to its
/// components. A variable component is bound by that relation, which joins
/// on it when another pattern bound it first, in every scope, without the
/// lowering tracking who binds what; a bound subject lets the planner
/// drive the relation through the dictionary's subject prefix. A constant
/// component is also a filter on the link
/// (`FILTER(sameTerm(PREDICATE(?__term), <p>))`), which the planner turns
/// into the scan's handle interval and key check when the link leads. A
/// fully constant edge composes to a constant term the scan looks up
/// directly.
pub fn lower_reified_link<E: IriEncoder + ?Sized>(
    annotation_ref: Ref,
    edge: TriplePattern,
    encoder: &E,
    vars: &mut VarRegistry,
    out: &mut Vec<Pattern>,
) {
    link_patterns(
        annotation_ref,
        edge,
        encoder.encode_ref(fluree_vocab::rdf::REIFIES),
        || fresh_term_var(vars),
        &|iri| encoder.encode_iri(iri),
        out,
    );
}

/// Lower `subject predicate <<( s p o )>>`, a triple-term value under any
/// predicate: the triple with a term variable (or a composed constant term)
/// and the term's components, as [`lower_reified_link`] lowers a link.
pub fn lower_term_value<E: IriEncoder + ?Sized>(
    subject: Ref,
    predicate: Ref,
    term: TriplePattern,
    encoder: &E,
    vars: &mut VarRegistry,
    out: &mut Vec<Pattern>,
) {
    link_patterns(
        subject,
        term,
        predicate,
        || fresh_term_var(vars),
        &|iri| encoder.encode_iri(iri),
        out,
    );
}

/// The patterns of [`lower_reified_link`], given the `rdf:reifies` ref, the
/// term variable (asked for only when the edge is not constant), and how to
/// encode an IRI.
pub(crate) fn link_patterns(
    annotation_ref: Ref,
    edge: TriplePattern,
    reifies: Ref,
    term_var: impl FnOnce() -> VarId,
    encode_iri: &dyn Fn(&str) -> Option<Sid>,
    out: &mut Vec<Pattern>,
) {
    // Fully constant edge: compose the term itself.
    if let Some(term) = constant_term(&edge) {
        out.push(Pattern::Triple(TriplePattern {
            s: annotation_ref,
            p: reifies,
            o: Term::Value(FlakeValue::TripleTerm(Box::new(term))),
            dtc: None,
        }));
        return;
    }

    let t = term_var();
    out.push(Pattern::Triple(TriplePattern {
        s: annotation_ref,
        p: reifies,
        o: Term::Var(t),
        dtc: None,
    }));

    let accessor = |f: Function| Expression::call(f, vec![Expression::Var(t)]);
    let same_term = |f: Function, constant: Expression| {
        Pattern::Filter(Expression::call(
            Function::SameTerm,
            vec![accessor(f), constant],
        ))
    };
    let TriplePattern { s, p, o, dtc } = edge;
    // Constant positions are filters on the link (the scan narrows on
    // them) and constants of the components relation (a constant subject
    // anchors it). Variable positions are bound by the components relation,
    // which joins on them when another pattern bound them first.
    let component = |func: Function, term: Term, out: &mut Vec<Pattern>| match term {
        Term::Var(v) => Component::Var(v),
        Term::Sid(sid) => {
            out.push(same_term(
                func,
                Expression::Const(FlakeValue::Ref(sid.clone())),
            ));
            Component::Node(sid)
        }
        Term::Iri(iri) => match encode_iri(&iri) {
            Some(sid) => {
                out.push(same_term(
                    func,
                    Expression::Const(FlakeValue::Ref(sid.clone())),
                ));
                Component::Node(sid)
            }
            // Not encodable here (no ledger at hand, or not this one's):
            // compared as the query runs, against the ledger it reads.
            None => {
                out.push(same_term(
                    func,
                    Expression::call(
                        Function::Iri,
                        vec![Expression::Const(FlakeValue::String(iri.to_string()))],
                    ),
                ));
                Component::Any
            }
        },
        // A literal matches by term identity: its datatype or language
        // tag is part of what `<< ?s :p "chat"@fr >>` asks for.
        Term::Value(v) => match &dtc {
            Some(dtc) => {
                out.push(same_term(
                    func,
                    Expression::Resolved(Box::new(Binding::Lit {
                        val: v.clone(),
                        dtc: dtc.clone(),
                        t: None,
                        op: None,
                        p_id: None,
                    })),
                ));
                Component::Literal(v, dtc.clone())
            }
            None => {
                out.push(Pattern::Filter(Expression::call(
                    Function::Eq,
                    vec![accessor(func), Expression::Const(v)],
                )));
                Component::Any
            }
        },
    };
    let tc = TermComponentsPattern {
        term: t,
        subject: component(Function::TripleSubject, Term::from(s), out),
        predicate: component(Function::TriplePredicate, Term::from(p), out),
        object: component(Function::TripleObject, o, out),
    };
    if tc.components().into_iter().any(|c| c.var().is_some())
        || matches!(tc.subject, Component::Node(_))
    {
        out.push(Pattern::TermComponents(tc));
    }
}

/// A `?__term_N` variable no pattern uses yet.
pub fn fresh_term_var(vars: &mut VarRegistry) -> VarId {
    let name = (vars.len()..)
        .map(|n| format!("?__term_{n}"))
        .find(|name| vars.get(name).is_none())
        .expect("an unused name");
    vars.get_or_insert(&name)
}

/// The materialized term for an edge whose three positions are constants
/// and whose object datatype is known; `None` otherwise.
fn constant_term(edge: &TriplePattern) -> Option<fluree_db_core::TripleTermValue> {
    let s = match &edge.s {
        Ref::Sid(s) => s.clone(),
        _ => return None,
    };
    let p = match &edge.p {
        Ref::Sid(p) => p.clone(),
        _ => return None,
    };
    let (o, dt, lang) = match (&edge.o, &edge.dtc) {
        (Term::Sid(sid), _) => (
            FlakeValue::Ref(sid.clone()),
            fluree_db_core::edge::id_datatype_sid(),
            None,
        ),
        (Term::Value(v), Some(DatatypeConstraint::Explicit(dt))) => (v.clone(), dt.clone(), None),
        (Term::Value(v), Some(DatatypeConstraint::LangTag(tag))) => (
            v.clone(),
            fluree_db_core::Sid::new(
                fluree_vocab::namespaces::RDF,
                fluree_vocab::rdf_names::LANG_STRING,
            ),
            Some(tag.to_string()),
        ),
        _ => return None,
    };
    Some(fluree_db_core::TripleTermValue { s, p, o, dt, lang })
}
