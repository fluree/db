//! A ledger head's graph names, read in one order everywhere.

use crate::read::binary_index_store::BinaryIndexStore;
use fluree_db_core::{GraphId, GraphNameTable, GraphRegistry};

/// The graph names of a ledger head: its graph registry, then its binary
/// index store's, the order every reader of a graph name resolves it in. The
/// store is a fallback a consistent ledger never needs; it is read in one
/// place so the order cannot drift between readers.
///
/// It borrows both, so a lookup copies nothing and costs one hash probe (two
/// on a miss) however many graphs the ledger holds.
#[derive(Clone, Copy)]
pub struct HeadGraphNames<'a> {
    registry: &'a GraphRegistry,
    store: Option<&'a BinaryIndexStore>,
}

impl<'a> HeadGraphNames<'a> {
    pub fn new(registry: &'a GraphRegistry, store: Option<&'a BinaryIndexStore>) -> Self {
        Self { registry, store }
    }

    /// The graph registered under exactly `iri`.
    pub fn graph_id(&self, iri: &str) -> Option<GraphId> {
        self.registry
            .graph_id_for_iri(iri)
            .or_else(|| self.store.and_then(|store| store.graph_id_for_iri(iri)))
    }
}

impl GraphNameTable for HeadGraphNames<'_> {
    fn graph_id(&self, iri: &str) -> Option<GraphId> {
        HeadGraphNames::graph_id(self, iri)
    }

    fn reserved_iri(&self, g_id: GraphId) -> Option<&str> {
        self.registry
            .iri_for_graph_id(g_id)
            .or_else(|| self.store.and_then(|store| store.graph_iri_for_id(g_id)))
    }
}
