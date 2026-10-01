//! `TermComponents(?t, s, p, o)`: a triple term's components as a relation.
//!
//! Per input row the operator finds the candidate terms, then checks and binds
//! each component:
//!
//! - the term variable is bound: its dictionary key, one forward lookup, which
//!   supplies every component at once;
//! - else the subject is bound (a constant, or a value an earlier pattern
//!   bound): the reverse tree's `(s_id[, p_id])` prefix range, which yields
//!   keys and handles together; distinct prefixes are read once per batch;
//! - else every term of the bound predicate, or of the whole dictionary.
//!
//! A component variable already bound is joined on with the same equality a
//! `BIND` clobber check applies. Dictionary membership says nothing about a
//! live, visible annotation: the link `?r rdf:reifies ?t` that follows keeps
//! the graph, time, policy and novelty checks.
//!
//! Terms the index has not interned yet appear only in novelty's links; they
//! are collected once per open and offered alongside the dictionary's.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Component, TermComponentsPattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::triple_term::{novelty_term_index, TermKey};
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid, TripleTermValue};
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::Arc;

/// One candidate term: its dictionary key with its handle, or a materialized
/// term a row carried.
enum Candidate {
    Encoded { handle: u64, key: TermKey },
    Materialized(Box<TripleTermValue>),
}

/// The pattern's constant components in the store's encoding. `None` when a
/// dictionary cannot name one, so no interned term can match.
#[derive(Clone, Copy)]
struct EncodedConstants {
    s_id: Option<u64>,
    p_id: Option<u32>,
    o: Option<(u16, u64)>,
}

/// A term only novelty's links name: encoded under its provisional handle
/// when dictionary novelty gives it one, else materialized.
struct NoveltyTerm {
    term: Box<TripleTermValue>,
    encoded: Option<(u64, TermKey)>,
}

pub struct TermComponentsOperator {
    child: BoxedOperator,
    pattern: TermComponentsPattern,
    schema: Arc<[VarId]>,
    child_width: usize,
    state: OperatorState,
    store: Option<Arc<BinaryIndexStore>>,
    constants: Option<EncodedConstants>,
    reifies_p_id: u32,
    novelty_terms: Vec<NoveltyTerm>,
}

impl TermComponentsOperator {
    pub fn new(child: BoxedOperator, pattern: TermComponentsPattern) -> Self {
        let mut schema = child.schema().to_vec();
        let child_width = schema.len();
        for v in pattern.produced_vars() {
            if !schema.contains(&v) {
                schema.push(v);
            }
        }
        Self {
            child,
            pattern,
            schema: Arc::from(schema.into_boxed_slice()),
            child_width,
            state: OperatorState::Created,
            store: None,
            constants: None,
            reifies_p_id: 0,
            novelty_terms: Vec::new(),
        }
    }

    /// The terms novelty's links assert that the dictionary does not hold.
    fn collect_novelty_terms(&self, ctx: &ExecutionContext<'_>) -> Result<Vec<NoveltyTerm>> {
        let reifies = fluree_db_core::rdf_reifies_sid();
        let (first, rhs) = crate::fast_path_common::predicate_walk_bounds(reifies);
        let mut seen: HashSet<TripleTermValue> = HashSet::new();
        let mut visit = |overlay: &dyn fluree_db_core::OverlayProvider, g_id, to_t| {
            overlay.for_each_overlay_flake(
                g_id,
                fluree_db_core::IndexType::Post,
                Some(&first),
                Some(&rhs),
                false,
                to_t,
                &mut |f| {
                    if f.op && f.p == *reifies {
                        if let FlakeValue::TripleTerm(term) = &f.o {
                            seen.insert((**term).clone());
                        }
                    }
                },
            );
        };
        match ctx.active_graphs() {
            crate::dataset::ActiveGraphs::Single => {
                visit(ctx.overlay(), ctx.binary_g_id, ctx.to_t);
            }
            crate::dataset::ActiveGraphs::Many(graphs) => {
                for g in graphs {
                    visit(g.overlay, g.g_id, g.to_t);
                }
            }
        }
        let dict_novelty = ctx.dict_novelty.as_ref();
        let mut out = Vec::new();
        for term in seen {
            let encoded = match &self.store {
                Some(store) => {
                    let handle = missing(crate::binary_scan::compose_term_handle(
                        &term,
                        store,
                        dict_novelty,
                    ))
                    .map_err(|e| QueryError::from_io("term components: novelty", e))?
                    .map(|(_, handle)| handle);
                    match handle {
                        // The dictionary offers it already.
                        Some(h) if novelty_term_index(h).is_none() => continue,
                        Some(h) => crate::binary_scan::term_key_for_handle(h, store, dict_novelty)
                            .map_err(|e| QueryError::from_io("term components: novelty", e))?
                            .map(|key| (h, key)),
                        None => None,
                    }
                }
                None => None,
            };
            out.push(NoveltyTerm {
                term: Box::new(term),
                encoded,
            });
        }
        Ok(out)
    }

    /// Whether a novelty term can match the row's subject anchor. A cheap
    /// filter only: `apply` still checks every component.
    fn novelty_anchor_matches(&self, row: &[Binding], nt: &NoveltyTerm) -> bool {
        match &self.pattern.subject {
            Component::Node(sid) => nt.term.s == *sid,
            Component::Var(v) => match self.value(row, *v) {
                Some(Binding::EncodedSid { s_id, .. }) => {
                    nt.encoded.is_none_or(|(_, key)| key.s_id == *s_id)
                }
                Some(Binding::Sid { sid, .. }) => nt.term.s == *sid,
                _ => true,
            },
            _ => true,
        }
    }

    fn encode_constants(
        &self,
        store: &BinaryIndexStore,
        ctx: &ExecutionContext<'_>,
    ) -> std::io::Result<Option<EncodedConstants>> {
        let dict_novelty = ctx.dict_novelty.as_ref();
        let s_id = match &self.pattern.subject {
            Component::Node(sid) => {
                match missing(crate::binary_scan::resolve_subject_v3(
                    sid,
                    store,
                    dict_novelty,
                ))? {
                    Some(id) => Some(id),
                    None => return Ok(None),
                }
            }
            _ => None,
        };
        let p_id = match &self.pattern.predicate {
            Component::Node(sid) => match store.sid_to_p_id(sid) {
                Some(id) => Some(id),
                None => return Ok(None),
            },
            _ => None,
        };
        let o = match &self.pattern.object {
            Component::Node(sid) => {
                match missing(crate::binary_scan::resolve_subject_v3(
                    sid,
                    store,
                    dict_novelty,
                ))? {
                    Some(id) => Some((OType::IRI_REF.as_u16(), id)),
                    None => return Ok(None),
                }
            }
            Component::Literal(value, dtc) => {
                let (dt, lang) = match dtc {
                    DatatypeConstraint::Explicit(dt) => (dt.clone(), None),
                    DatatypeConstraint::LangTag(tag) => (
                        Sid::new(
                            fluree_vocab::namespaces::RDF,
                            fluree_vocab::rdf_names::LANG_STRING,
                        ),
                        Some(tag.as_ref()),
                    ),
                };
                match missing(crate::binary_scan::term_object_key(
                    value,
                    &dt,
                    lang,
                    store,
                    dict_novelty,
                ))? {
                    Some((ot, key)) => Some((ot.as_u16(), key)),
                    None => return Ok(None),
                }
            }
            _ => None,
        };
        Ok(Some(EncodedConstants { s_id, p_id, o }))
    }

    /// The subject's `s_id` for an anchored lookup, from a constant or a
    /// bound value; `Err(())` when the bound value names no interned subject.
    fn anchor_subject(
        &self,
        row: &[Binding],
        store: &BinaryIndexStore,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<std::result::Result<u64, ()>>> {
        match &self.pattern.subject {
            Component::Node(_) => Ok(self.constants.and_then(|c| c.s_id).map(Ok)),
            Component::Var(v) => match self.value(row, *v) {
                Some(binding) => Ok(Some(subject_id(binding, store, ctx)?.ok_or(()))),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }

    /// The predicate's `p_id` when it is fixed for this row.
    fn fixed_predicate(&self, row: &[Binding], store: &BinaryIndexStore) -> Option<u32> {
        match &self.pattern.predicate {
            Component::Node(_) => self.constants.and_then(|c| c.p_id),
            Component::Var(v) => match self.value(row, *v)? {
                Binding::EncodedPid { p_id } => Some(*p_id),
                Binding::Sid { sid, .. } => store.sid_to_p_id(sid),
                _ => None,
            },
            _ => None,
        }
    }

    /// The row's value for `var`, if bound.
    fn value<'r>(&self, row: &'r [Binding], var: VarId) -> Option<&'r Binding> {
        let pos = self.schema.iter().position(|v| *v == var)?;
        row.get(pos)
            .filter(|b| !matches!(b, Binding::Unbound | Binding::Poisoned))
    }

    /// Check the candidate against the pattern and fill in its variables on
    /// `row` (a copy of the input row, widened to the output schema).
    fn apply(
        &self,
        candidate: &Candidate,
        t: i64,
        row: &mut [Binding],
        ctx: &ExecutionContext<'_>,
    ) -> Result<bool> {
        // Constants first: they need no binding built.
        if let Candidate::Encoded { key, .. } = candidate {
            let Some(c) = self.constants else {
                return Ok(false);
            };
            if c.s_id.is_some_and(|s| s != key.s_id)
                || c.p_id.is_some_and(|p| p != key.p_id)
                || c.o
                    .is_some_and(|(ot, ok)| ot != key.o_type.as_u16() || ok != key.o_key)
            {
                return Ok(false);
            }
        }
        for (position, component) in self.pattern.components().into_iter().enumerate() {
            let computed = match (component, candidate) {
                (Component::Any, _) => continue,
                (Component::Node(_) | Component::Literal(..), Candidate::Encoded { .. }) => {
                    continue
                }
                (Component::Node(_) | Component::Literal(..), Candidate::Materialized(term)) => {
                    if !materialized_matches(component, position, term) {
                        return Ok(false);
                    }
                    continue;
                }
                (Component::Var(_), Candidate::Encoded { key, .. }) => match position {
                    0 => Binding::encoded_sid(key.s_id),
                    1 => Binding::EncodedPid { p_id: key.p_id },
                    _ => crate::eval::rdf::term_object_binding(key, t, ctx)?,
                },
                (Component::Var(_), Candidate::Materialized(term)) => match position {
                    0 => Binding::sid(term.s.clone()),
                    1 => Binding::sid(term.p.clone()),
                    _ => crate::eval::rdf::materialized_term_object(term),
                },
            };
            let Component::Var(v) = component else {
                continue;
            };
            let pos = self
                .schema
                .iter()
                .position(|x| x == v)
                .expect("component variable is in the schema");
            match &row[pos] {
                Binding::Unbound | Binding::Poisoned => row[pos] = computed,
                existing => {
                    if !crate::object_binding::bind_unifies(existing, &computed, Some(ctx)) {
                        return Ok(false);
                    }
                }
            }
        }
        let term_pos = self
            .schema
            .iter()
            .position(|x| *x == self.pattern.term)
            .expect("term variable is in the schema");
        if matches!(row[term_pos], Binding::Unbound | Binding::Poisoned) {
            row[term_pos] = match candidate {
                Candidate::Encoded { handle, .. } => Binding::EncodedLit {
                    o_kind: ObjKind::TRIPLE_TERM.as_u8(),
                    o_key: *handle,
                    p_id: self.reifies_p_id,
                    dt_id: 0,
                    lang_id: 0,
                    i_val: i32::MIN,
                    t,
                },
                // As a scan binds a link object novelty holds.
                Candidate::Materialized(term) => Binding::Lit {
                    val: FlakeValue::TripleTerm(term.clone()),
                    dtc: DatatypeConstraint::Explicit(
                        fluree_db_core::triple_term_datatype_sid().clone(),
                    ),
                    t: None,
                    op: None,
                    p_id: None,
                },
            };
        }
        Ok(true)
    }
}

/// A component no dictionary can name is a miss, not an error.
fn missing<T>(r: std::io::Result<T>) -> std::io::Result<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::Unsupported) => Ok(None),
        Err(e) => Err(e),
    }
}

/// A materialized term's component against a constant.
fn materialized_matches(component: &Component, position: usize, term: &TripleTermValue) -> bool {
    match (component, position) {
        (Component::Node(sid), 0) => &term.s == sid,
        (Component::Node(sid), 1) => &term.p == sid,
        (Component::Node(sid), _) => matches!(&term.o, FlakeValue::Ref(o) if o == sid),
        (Component::Literal(value, dtc), _) => {
            &term.o == value
                && match dtc {
                    DatatypeConstraint::Explicit(dt) => term.lang.is_none() && &term.dt == dt,
                    DatatypeConstraint::LangTag(tag) => term.lang.as_deref() == Some(tag.as_ref()),
                }
        }
        _ => false,
    }
}

/// The interned `s_id` a subject binding names, or `None` when it names none
/// (a literal, or an IRI no dictionary holds).
fn subject_id(
    binding: &Binding,
    store: &BinaryIndexStore,
    ctx: &ExecutionContext<'_>,
) -> Result<Option<u64>> {
    let sid = match binding {
        Binding::EncodedSid { s_id, .. } => return Ok(Some(*s_id)),
        Binding::Sid { sid, .. } => sid.clone(),
        Binding::IriMatch { primary_sid, .. } => primary_sid.clone(),
        Binding::Iri(iri) => {
            return store
                .find_subject_id(iri)
                .map_err(|e| QueryError::from_io("term components: subject", e))
        }
        _ => return Ok(None),
    };
    match crate::binary_scan::resolve_subject_v3(&sid, store, ctx.dict_novelty.as_ref()) {
        Ok(id) => Ok(Some(id)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(QueryError::from_io("term components: subject", e)),
    }
}

#[async_trait]
impl Operator for TermComponentsOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let bound = self.child.schema();
        let access = if bound.contains(&self.pattern.term) {
            "term"
        } else if match &self.pattern.subject {
            Component::Node(_) => true,
            Component::Var(v) => bound.contains(v),
            _ => false,
        } {
            "subject"
        } else {
            "scan"
        };
        let mut m = serde_json::Map::new();
        m.insert("term".into(), format!("?v{}", self.pattern.term.0).into());
        m.insert("access".into(), access.into());
        m
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.store = ctx.binary_store.clone();
        if let Some(store) = self.store.clone() {
            self.constants = self
                .encode_constants(&store, ctx)
                .map_err(|e| QueryError::from_io("term components: constants", e))?;
            self.reifies_p_id = store
                .sid_to_p_id(&Sid::new(
                    fluree_vocab::namespaces::RDF,
                    fluree_vocab::rdf_names::REIFIES,
                ))
                .unwrap_or(0);
        }
        self.novelty_terms = self.collect_novelty_terms(ctx)?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        loop {
            let input = match self.child.next_batch(ctx).await? {
                Some(b) if !b.is_empty() => b,
                Some(_) => continue,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };
            let mut columns: Vec<Vec<Binding>> = vec![Vec::new(); self.schema.len()];
            // Distinct prefixes and predicates are read once per batch.
            let mut by_prefix: HashMap<(u64, Option<u32>), Arc<Vec<(TermKey, u64)>>> =
                HashMap::new();
            let mut by_predicate: HashMap<Option<u32>, Arc<Vec<(TermKey, u64)>>> = HashMap::new();
            for row_idx in 0..input.len() {
                let mut row: Vec<Binding> = (0..self.schema.len())
                    .map(|col| {
                        if col < self.child_width {
                            input.get_by_col(row_idx, col).clone()
                        } else {
                            Binding::Unbound
                        }
                    })
                    .collect();
                let (candidates, t): (Vec<Candidate>, i64) = match self
                    .value(&row, self.pattern.term)
                    .cloned()
                {
                    Some(Binding::EncodedLit {
                        o_kind, o_key, t, ..
                    }) if o_kind == ObjKind::TRIPLE_TERM.as_u8() => {
                        let Some(store) = &self.store else {
                            continue;
                        };
                        let key = crate::binary_scan::term_key_for_handle(
                            o_key,
                            store,
                            ctx.dict_novelty.as_ref(),
                        )
                        .map_err(|e| QueryError::from_io("resolve_term_key", e))?;
                        // A provisional handle whose components no dictionary
                        // encodes is offered as its novelty term.
                        let candidate = match key {
                            Some(key) => Candidate::Encoded { handle: o_key, key },
                            None => match novelty_term_index(o_key)
                                .zip(ctx.dict_novelty.as_ref())
                                .and_then(|(index, dn)| dn.terms.resolve(index))
                            {
                                Some(term) => Candidate::Materialized(Box::new(term.clone())),
                                None => {
                                    return Err(QueryError::Internal(format!(
                                        "triple-term handle {o_key:#x} has no dictionary entry"
                                    )))
                                }
                            },
                        };
                        (vec![candidate], t)
                    }
                    Some(Binding::Lit {
                        val: FlakeValue::TripleTerm(term),
                        ..
                    }) => (vec![Candidate::Materialized(term)], 0),
                    // Bound to something that is not a term.
                    Some(_) => continue,
                    None => {
                        let mut candidates = Vec::new();
                        if let Some(store) = self.store.clone() {
                            if let Some(terms) = store.term_dict() {
                                let p_id = self.fixed_predicate(&row, &store);
                                let found = match self.anchor_subject(&row, &store, ctx)? {
                                    Some(Ok(s_id)) => match by_prefix.get(&(s_id, p_id)) {
                                        Some(found) => Some(Arc::clone(found)),
                                        None => {
                                            let found = Arc::new(
                                                terms.terms_with_subject(s_id, p_id).map_err(
                                                    |e| {
                                                        QueryError::from_io(
                                                            "term components: subject",
                                                            e,
                                                        )
                                                    },
                                                )?,
                                            );
                                            by_prefix.insert((s_id, p_id), Arc::clone(&found));
                                            Some(found)
                                        }
                                    },
                                    // No interned subject: only novelty can match.
                                    Some(Err(())) => None,
                                    None => match by_predicate.get(&p_id) {
                                        Some(found) => Some(Arc::clone(found)),
                                        None => {
                                            let predicates: Vec<u32> = match p_id {
                                                Some(p) => vec![p],
                                                None => terms.predicates().collect(),
                                            };
                                            let mut all = Vec::new();
                                            for p in predicates {
                                                all.extend(terms.terms_of_predicate(p).map_err(
                                                    |e| {
                                                        QueryError::from_io(
                                                            "term components: scan",
                                                            e,
                                                        )
                                                    },
                                                )?);
                                            }
                                            let found = Arc::new(all);
                                            by_predicate.insert(p_id, Arc::clone(&found));
                                            Some(found)
                                        }
                                    },
                                };
                                if let Some(found) = found {
                                    candidates.extend(found.iter().map(|(key, handle)| {
                                        Candidate::Encoded {
                                            handle: *handle,
                                            key: *key,
                                        }
                                    }));
                                }
                            }
                        }
                        candidates.extend(
                            self.novelty_terms
                                .iter()
                                .filter(|nt| self.novelty_anchor_matches(&row, nt))
                                .map(|nt| match nt.encoded {
                                    Some((handle, key)) => Candidate::Encoded { handle, key },
                                    None => Candidate::Materialized(nt.term.clone()),
                                }),
                        );
                        (candidates, 0)
                    }
                };
                for candidate in &candidates {
                    let mut out = row.clone();
                    if self.apply(candidate, t, &mut out, ctx)? {
                        for (col, value) in out.into_iter().enumerate() {
                            columns[col].push(value);
                        }
                    }
                }
                row.clear();
            }
            if columns.first().is_some_and(Vec::is_empty) {
                continue;
            }
            return Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        None
    }
}
