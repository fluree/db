//! The compact lists a DELETE WHERE's collections match, deleted whole.
//!
//! A collection in a DELETE WHERE (`DELETE WHERE { ex:s ex:p (1 2 3) }`)
//! reads lists as the RDF lists they denote, an owner edge to a chain of
//! cells: blank nodes the request names and nothing stores ([`ListView`]).
//! The retractions naming those cells are the lists'; each list an owner edge
//! names is deleted whole.

use crate::current_facts::{RetractIntent, Retraction};
use fluree_db_core::{FlakeValue, Sid};
use fluree_db_query::list_view::{ListView, Owner};
use std::collections::HashSet;
use std::sync::Arc;

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
