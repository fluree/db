//! The cells of compact lists a write edits in the RDF view of lists
//! (`# PRAGMA rdf-lists: true`, JSON-LD `opts.rdfLists`).
//!
//! In that view the WHERE reads a compact list as an owner edge to a chain of
//! cells, blank nodes the request names and nothing stores
//! ([`ListView`]), so a template triple naming one is not written as it is.
//! The `rdf:first` triples a write deletes from and inserts on a cell are
//! taken together, across every solution, against the cell as the WHERE read
//! it: a cell left with one value other than its member's replaces that
//! member, in the same list at the same position, and a cell left with its
//! member is unchanged. Every other edit naming a cell is refused: a cell left
//! with no `rdf:first` or with two, an `rdf:rest` or owner edge written, a
//! triple about a cell on any other predicate or in another graph, and a cell
//! as a value, a predicate or a graph. So is a list in a DELETE template,
//! which in this view is a chain of new blank nodes. Lists stored as explicit
//! `rdf:first`/`rdf:rest` chains are stored nodes, written as any triple is.
//!
//! Outside the view, a DELETE WHERE's collections alone read lists so, and
//! delete the compact lists they match whole ([`MatchedLists`]).

use crate::current_facts::{RetractIntent, Retraction};
use crate::error::{Result, TransactError};
use crate::ir::TripleTemplate;
use crate::lists::PendingList;
use fluree_db_core::namespaces::{is_rdf_first, is_rdf_rest};
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, GraphId, Sid};
use fluree_db_query::list_view::{ListView, Owner};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The cell edits of one write, held until its WHERE has been read.
pub(crate) struct CellEdits {
    view: Arc<ListView>,
    /// Each cell a template names, in the order first named.
    cells: Vec<(Sid, CellEdit)>,
    index: HashMap<Sid, usize>,
}

/// The `rdf:first` values a write deletes from and inserts on one cell.
struct CellEdit {
    /// The graph the templates name the cell in (`None` for the default).
    graph: Option<Sid>,
    deleted: Vec<Term>,
    inserted: Vec<Term>,
}

/// An `rdf:first` value.
#[derive(Clone, PartialEq, Eq)]
struct Term {
    o: FlakeValue,
    dt: Sid,
    lang: Option<String>,
}

impl Term {
    fn of(flake: &Flake) -> Self {
        Self {
            o: flake.o.clone(),
            dt: flake.dt.clone(),
            lang: flake.m.as_ref().and_then(|m| m.lang.clone()),
        }
    }
}

/// What the held edits write.
#[derive(Default)]
pub(crate) struct CellWrites {
    pub retractions: Vec<Retraction>,
    pub assertions: Vec<Flake>,
    /// Members the write deleted that the cells did not hold, as retractions
    /// of the owner's property, for the modify-policy check.
    pub unmatched: Vec<Flake>,
}

impl CellEdits {
    /// The edits of a write whose WHERE reads lists in `view`.
    pub(crate) fn new(view: Arc<ListView>) -> Self {
        Self {
            view,
            cells: Vec::new(),
            index: HashMap::new(),
        }
    }

    /// The retraction intents naming no cell; those of a cell's `rdf:first`
    /// are held. `graph_of` gives a flake's ledger graph.
    pub(crate) fn take_deletes(
        &mut self,
        intents: Vec<RetractIntent>,
        graph_of: impl Fn(&Flake) -> Result<GraphId>,
    ) -> Result<Vec<RetractIntent>> {
        let mut rest = Vec::with_capacity(intents.len());
        for intent in intents {
            if self.names_cell(intent.flake()) {
                self.hold(intent.flake(), false, &graph_of)?;
            } else {
                rest.push(intent);
            }
        }
        Ok(rest)
    }

    /// The assertions naming no cell; those of a cell's `rdf:first` are
    /// held.
    pub(crate) fn take_inserts(
        &mut self,
        flakes: Vec<Flake>,
        graph_of: impl Fn(&Flake) -> Result<GraphId>,
    ) -> Result<Vec<Flake>> {
        let mut rest = Vec::with_capacity(flakes.len());
        for flake in flakes {
            if self.names_cell(&flake) {
                self.hold(&flake, true, &graph_of)?;
            } else {
                rest.push(flake);
            }
        }
        Ok(rest)
    }

    /// Refuse a list the write states that a cell owns or holds.
    pub(crate) fn check_lists(&self, lists: &[PendingList]) -> Result<()> {
        for member in lists.iter().flat_map(PendingList::members) {
            if self.names_cell(member) {
                return Err(refused(
                    "a list the write states names a list cell; a cell holds one rdf:first \
                     and is no value",
                ));
            }
        }
        Ok(())
    }

    fn is_cell(&self, sid: &Sid) -> bool {
        self.view.decode(sid).is_some()
    }

    fn value_names_cell(&self, o: &FlakeValue) -> bool {
        match o {
            FlakeValue::Ref(sid) => self.is_cell(sid),
            FlakeValue::TripleTerm(t) => {
                self.is_cell(&t.s) || self.is_cell(&t.p) || self.value_names_cell(&t.o)
            }
            _ => false,
        }
    }

    fn names_cell(&self, flake: &Flake) -> bool {
        self.is_cell(&flake.s)
            || self.is_cell(&flake.p)
            || flake.g.as_ref().is_some_and(|g| self.is_cell(g))
            || self.value_names_cell(&flake.o)
    }

    fn hold(
        &mut self,
        flake: &Flake,
        insert: bool,
        graph_of: &impl Fn(&Flake) -> Result<GraphId>,
    ) -> Result<()> {
        if self.is_cell(&flake.p) {
            return Err(refused("a list cell is not a property"));
        }
        let owner = match self.view.decode(&flake.s) {
            Some((owner, _)) if !self.value_names_cell(&flake.o) => owner,
            _ if is_rdf_rest(&flake.p) => return Err(rest_refused()),
            _ => {
                return Err(refused(
                    "a list cell is not a value: it cannot be written as the object of a \
                     triple, so a list's owner edge cannot change and nothing else can \
                     refer to a cell",
                ))
            }
        };
        if flake.g.as_ref().is_some_and(|g| self.is_cell(g)) {
            return Err(refused("a list cell names no graph"));
        }
        if is_rdf_rest(&flake.p) {
            return Err(rest_refused());
        }
        if !is_rdf_first(&flake.p) {
            return Err(refused(
                "a list cell holds only rdf:first and rdf:rest; store other properties on \
                 the member or the list's owner",
            ));
        }
        if graph_of(flake)? != owner.0 {
            return Err(refused(
                "a list cell is in the graph of the list it belongs to; write its rdf:first \
                 in that graph",
            ));
        }
        let n = match self.index.get(&flake.s) {
            Some(&n) => n,
            None => {
                let edit = CellEdit {
                    graph: flake.g.clone(),
                    deleted: Vec::new(),
                    inserted: Vec::new(),
                };
                self.cells.push((flake.s.clone(), edit));
                self.index.insert(flake.s.clone(), self.cells.len() - 1);
                self.cells.len() - 1
            }
        };
        let edit = &mut self.cells[n].1;
        let term = Term::of(flake);
        let terms = if insert {
            &mut edit.inserted
        } else {
            &mut edit.deleted
        };
        if !terms.contains(&term) {
            terms.push(term);
        }
        Ok(())
    }

    /// What the held edits write at `t` (see the [module docs](self)).
    pub(crate) fn resolve(self, t: i64) -> Result<CellWrites> {
        let mut out = CellWrites::default();
        if self.cells.is_empty() {
            return Ok(out);
        }
        let place = |cell: &Sid| {
            self.view.decode(cell).ok_or_else(|| {
                TransactError::InvariantViolation(format!("{cell:?} is no longer a list cell"))
            })
        };
        let owners: HashSet<Owner> = self
            .cells
            .iter()
            .map(|(cell, _)| place(cell).map(|(owner, _)| owner))
            .collect::<Result<_>>()?;
        let lists = self.view.read_members(&owners);
        for (cell, edit) in &self.cells {
            let (owner, k) = place(cell)?;
            let member = lists.get(&owner).and_then(|m| m.get(k)).ok_or_else(|| {
                TransactError::InvariantViolation(format!(
                    "cell {k} of a list the write read is missing"
                ))
            })?;
            let at = |term: &Term, op: bool| member_at(member, edit.graph.as_ref(), term, t, op);
            let current = Term::of(member);
            for deleted in edit.deleted.iter().filter(|d| **d != current) {
                out.unmatched.push(at(deleted, false)?);
            }
            let mut left: Vec<&Term> = Vec::with_capacity(1 + edit.inserted.len());
            if !edit.deleted.contains(&current) {
                left.push(&current);
            }
            for inserted in &edit.inserted {
                if !left.contains(&inserted) {
                    left.push(inserted);
                }
            }
            match left.as_slice() {
                [] => {
                    return Err(refused(&format!(
                        "the write deletes the rdf:first of cell {k} of a list and inserts \
                         none; replace the member instead (removing a list member through its \
                         cell is not supported yet)"
                    )))
                }
                [only] if **only == current => {}
                [only] => {
                    out.retractions
                        .push(Retraction::of_list_member(member, edit.graph.clone(), t));
                    out.assertions.push(at(only, true)?);
                }
                _ => {
                    return Err(refused(&format!(
                        "the write leaves cell {k} of a list with {} rdf:first values; a cell \
                         holds one, so delete its member to replace it",
                        left.len()
                    )))
                }
            }
        }
        Ok(out)
    }
}

/// The compact lists a DELETE WHERE's collections matched
/// ([`crate::ir::Txn::collection_cells`]), each deleted whole: an owner
/// edge to a list's head cell names the list, and a cell's own triples are
/// the list's.
pub(crate) struct MatchedLists {
    view: Arc<ListView>,
    /// Each list an owner edge named, with the graph its template names.
    lists: Vec<(Owner, Option<Sid>)>,
    seen: HashSet<Owner>,
}

impl MatchedLists {
    pub(crate) fn new(view: Arc<ListView>) -> Self {
        Self {
            view,
            lists: Vec::new(),
            seen: HashSet::new(),
        }
    }

    /// The retraction intents naming no cell; an owner edge's list is held.
    pub(crate) fn take_deletes(&mut self, intents: Vec<RetractIntent>) -> Vec<RetractIntent> {
        let mut rest = Vec::with_capacity(intents.len());
        for intent in intents {
            let flake = intent.flake();
            if self.view.decode(&flake.s).is_some() {
                continue;
            }
            let list = match &flake.o {
                FlakeValue::Ref(head) => self.view.decode(head),
                _ => None,
            };
            match list {
                Some((owner, _)) => {
                    if self.seen.insert(owner.clone()) {
                        self.lists.push((owner, flake.g.clone()));
                    }
                }
                None => rest.push(intent),
            }
        }
        rest
    }

    /// The held lists' members, retracted at `t`.
    pub(crate) fn resolve(self, t: i64) -> Vec<Retraction> {
        let members = self.view.read_members(&self.seen);
        self.lists
            .iter()
            .flat_map(|(owner, graph)| {
                members
                    .get(owner)
                    .into_iter()
                    .flatten()
                    .map(move |m| Retraction::of_list_member(m, graph.clone(), t))
            })
            .collect()
    }
}

/// `term` at the place of list `member`, in `graph`.
fn member_at(member: &Flake, graph: Option<&Sid>, term: &Term, t: i64, op: bool) -> Result<Flake> {
    let place = member
        .m
        .as_ref()
        .and_then(FlakeMeta::place)
        .ok_or_else(|| {
            TransactError::InvariantViolation(
                "a list member the write read has no place".to_string(),
            )
        })?;
    let m = FlakeMeta::from_place(term.lang.as_deref(), Some(place));
    let (s, p, o, dt) = (
        member.s.clone(),
        member.p.clone(),
        term.o.clone(),
        term.dt.clone(),
    );
    Ok(match graph {
        Some(g) => Flake::new_in_graph(g.clone(), s, p, o, dt, t, op, m),
        None => Flake::new(s, p, o, dt, t, op, m),
    })
}

/// Refuse a list in a DELETE template: read as RDF it is a chain of new
/// blank nodes, which no stored list is.
pub(crate) fn refuse_deleted_lists(templates: &[TripleTemplate]) -> Result<()> {
    if templates.iter().any(|t| t.list_index.is_some()) {
        return Err(refused(
            "a list in a DELETE template is a chain of new blank nodes, which matches no \
             stored list; edit a list through its cells (removing a list member is not \
             supported yet)",
        ));
    }
    Ok(())
}

fn refused(why: &str) -> TransactError {
    TransactError::ListCellEdit(why.to_string())
}

fn rest_refused() -> TransactError {
    refused(
        "changing a list's rdf:rest chain is not supported yet; write the list again to \
         insert, remove or reorder its members",
    )
}
