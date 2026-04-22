//! Ledger-level registry mapping graph IRIs to deterministic GraphIds.
//!
//! `GraphRegistry` lives on `LedgerSnapshot` so that all query paths
//! (GraphDb, GraphDbRef, HistoricalLedgerView, etc.) can resolve graph
//! IRIs without depending on a binary index.
//!
//! ## System graph ID layout
//!
//! - GraphId 0 = default graph (implicit, never stored in registry)
//! - GraphId 1 = txn-meta graph (`urn:fluree:{ledger_id}#txn-meta`)
//! - GraphId 2 = config graph (`urn:fluree:{ledger_id}#config`)
//! - GraphId 3+ = user-defined named graphs
//!
//! **New ledgers** (via [`new_for_ledger`]) always seed both g_id=1 and
//! g_id=2. **Existing roots** decoded via [`seed_from_root_iris`] are
//! accepted permissively — a legacy root with only txn-meta is valid.
//! User-defined graphs always start at g_id >= 3.
//!
//! ## Other invariants
//!
//! - Assignment is deterministic: new IRIs are deduped, sorted lexicographically,
//!   and assigned sequential IDs from `next_id`
//! - Registry is only mutated at commit-apply time; staging uses `provisional_ids()`

use crate::ids::GraphId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::{Error, Result};

/// Well-known GraphId for the default (implicit) graph.
pub const DEFAULT_GRAPH_ID: GraphId = 0;

/// Well-known GraphId for the txn-meta graph.
pub const TXN_META_GRAPH_ID: GraphId = 1;

/// Well-known GraphId for the ledger config graph.
pub const CONFIG_GRAPH_ID: GraphId = 2;

/// First GraphId available for user-defined named graphs.
pub const FIRST_USER_GRAPH_ID: GraphId = 3;

/// Construct the ledger-scoped txn-meta graph IRI from a ledger ID.
///
/// Each ledger has its own txn-meta named graph. The IRI follows the pattern
/// `urn:fluree:{ledger_id}#txn-meta`, making it globally unique per ledger
/// while staying deterministic and predictable.
///
/// # Examples
///
/// ```
/// use fluree_db_core::graph_registry::txn_meta_graph_iri;
/// assert_eq!(txn_meta_graph_iri("mydb:main"), "urn:fluree:mydb:main#txn-meta");
/// ```
pub fn txn_meta_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#txn-meta")
}

/// Construct the ledger-scoped config graph IRI from a ledger ID.
///
/// Each ledger has its own config graph. The IRI follows the pattern
/// `urn:fluree:{ledger_id}#config`, making it globally unique per ledger
/// while staying deterministic and predictable.
///
/// # Examples
///
/// ```
/// use fluree_db_core::graph_registry::config_graph_iri;
/// assert_eq!(config_graph_iri("mydb:main"), "urn:fluree:mydb:main#config");
/// ```
pub fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

/// Ledger-level registry mapping graph IRIs to deterministic GraphIds.
///
/// In production, use `new_for_ledger(ledger_id)` or `seed_from_root_iris()`
/// to ensure g_id=1 (txn-meta) and g_id=2 (config) are always mapped.
/// `Default` creates an empty registry (no system graphs seeded) suitable
/// for tests that don't exercise named-graph resolution.
#[derive(Debug, Clone)]
pub struct GraphRegistry {
    /// Forward map: graph IRI → GraphId
    iri_to_id: HashMap<Arc<str>, GraphId>,
    /// Reverse map: index = GraphId, value = IRI.
    /// Dense and sequential. id_to_iri[0] = None (default graph, no IRI).
    /// id_to_iri[1] = Some(txn-meta IRI) when properly seeded.
    /// id_to_iri[2] = Some(config IRI) when properly seeded.
    id_to_iri: Vec<Option<Arc<str>>>,
    /// Next available GraphId for assignment (always >= FIRST_USER_GRAPH_ID).
    next_id: GraphId,
}

impl Default for GraphRegistry {
    /// Creates an empty registry with no system graphs seeded.
    ///
    /// Suitable for tests that don't exercise named-graph resolution.
    /// Production code should use `new_for_ledger()` or `seed_from_root_iris()`.
    fn default() -> Self {
        Self {
            iri_to_id: HashMap::new(),
            // slot 0 = default graph, slot 1 = txn-meta (empty), slot 2 = config (empty)
            id_to_iri: vec![None, None, None],
            next_id: FIRST_USER_GRAPH_ID,
        }
    }
}

impl GraphRegistry {
    /// Create a registry for a specific ledger, seeding the system graphs:
    /// - g_id=1 → txn-meta (`urn:fluree:{ledger_id}#txn-meta`)
    /// - g_id=2 → config (`urn:fluree:{ledger_id}#config`)
    ///
    /// This is the canonical constructor for production use.
    pub fn new_for_ledger(ledger_id: &str) -> Self {
        let txn_meta: Arc<str> = Arc::from(txn_meta_graph_iri(ledger_id));
        let config: Arc<str> = Arc::from(config_graph_iri(ledger_id));
        let mut iri_to_id = HashMap::with_capacity(2);
        iri_to_id.insert(txn_meta.clone(), TXN_META_GRAPH_ID);
        iri_to_id.insert(config.clone(), CONFIG_GRAPH_ID);
        Self {
            iri_to_id,
            id_to_iri: vec![None, Some(txn_meta), Some(config)],
            next_id: FIRST_USER_GRAPH_ID,
        }
    }

    /// Populate from index root graph IRIs.
    ///
    /// Accepts the raw root format: a list of IRIs where **index 0 = g_id 1**
    /// (txn-meta), **index 1 = g_id 2** (config), **index 2 = g_id 3**
    /// (first user graph), etc. This matches the encoding in
    /// `IndexRoot.graph_iris` and the FIR6 binary root.
    ///
    /// The method builds the internal padded representation:
    /// `[None, Some(iris[0]), Some(iris[1]), ...]`
    ///
    /// Legacy roots may contain only a txn-meta IRI (no config graph).
    /// The method accepts any non-empty list — it does not enforce the
    /// system graph layout. The config graph will be seeded on first
    /// write if absent.
    ///
    /// # Errors
    ///
    /// - Rejects empty `iris` list (must have at least txn-meta)
    /// - Rejects empty strings in the iris list
    /// - Rejects duplicate IRIs across different slots
    pub fn seed_from_root_iris(iris: &[String]) -> Result<Self> {
        if iris.is_empty() {
            return Err(Error::invalid_index(
                "GraphRegistry: root iris must not be empty (at minimum txn-meta required)",
            ));
        }

        let mut iri_to_id: HashMap<Arc<str>, GraphId> = HashMap::with_capacity(iris.len());
        // Capacity: slot 0 (default) + iris.len() slots
        let mut id_to_iri: Vec<Option<Arc<str>>> = Vec::with_capacity(iris.len() + 1);
        id_to_iri.push(None); // slot 0 = default graph

        for (root_idx, iri_str) in iris.iter().enumerate() {
            let g_id = (root_idx as GraphId) + 1; // root index 0 → g_id 1
            if iri_str.is_empty() {
                return Err(Error::invalid_index(format!(
                    "GraphRegistry: empty IRI at root index {root_idx} (g_id={g_id})"
                )));
            }
            let arc: Arc<str> = Arc::from(iri_str.as_str());
            if let Some(&existing_id) = iri_to_id.get(&arc) {
                return Err(Error::invalid_index(format!(
                    "GraphRegistry: duplicate IRI '{iri_str}' at g_id={g_id} (already at g_id={existing_id})"
                )));
            }
            iri_to_id.insert(arc.clone(), g_id);
            id_to_iri.push(Some(arc));
        }

        // next_id = number of slots used, but always >= FIRST_USER_GRAPH_ID
        let next_id = (id_to_iri.len() as GraphId).max(FIRST_USER_GRAPH_ID);

        Ok(Self {
            iri_to_id,
            id_to_iri,
            next_id,
        })
    }

    /// Populate from `(GraphId, IRI)` pairs (e.g., from `BinaryIndexStore.graph_entries()`).
    ///
    /// # Errors
    ///
    /// - Rejects any entry with `g_id == 0`
    /// - Rejects duplicate GraphIds or duplicate IRIs
    pub fn seed_from_entries(entries: &[(GraphId, &str)]) -> Result<Self> {
        if entries.is_empty() {
            return Ok(Self::default());
        }

        let max_id = entries.iter().map(|(g_id, _)| *g_id).max().unwrap();
        let capacity = (max_id as usize) + 1;
        let mut id_to_iri: Vec<Option<Arc<str>>> = vec![None; capacity];
        let mut iri_to_id: HashMap<Arc<str>, GraphId> = HashMap::with_capacity(entries.len());

        for &(g_id, iri) in entries {
            if g_id == 0 {
                return Err(Error::invalid_index(
                    "GraphRegistry: g_id=0 (default graph) must not be stored",
                ));
            }
            let arc: Arc<str> = Arc::from(iri);
            if let Some(&existing_id) = iri_to_id.get(&arc) {
                return Err(Error::invalid_index(format!(
                    "GraphRegistry: duplicate IRI '{iri}' at g_id={g_id} (already at g_id={existing_id})"
                )));
            }
            if id_to_iri[g_id as usize].is_some() {
                return Err(Error::invalid_index(format!(
                    "GraphRegistry: duplicate g_id={g_id} with different IRIs"
                )));
            }
            iri_to_id.insert(arc.clone(), g_id);
            id_to_iri[g_id as usize] = Some(arc);
        }

        // Always >= FIRST_USER_GRAPH_ID even if entries only contain g_id=1
        let next_id = (max_id + 1).max(FIRST_USER_GRAPH_ID);

        Ok(Self {
            iri_to_id,
            id_to_iri,
            next_id,
        })
    }

    /// Apply a delta of graph IRIs from a commit envelope.
    ///
    /// New IRIs (not already in registry) are deduped, sorted lexicographically,
    /// and assigned sequential GraphIds from `next_id`. Returns the newly assigned
    /// `(GraphId, Arc<str>)` pairs.
    ///
    /// This is the **only mutation path** — called at commit-apply time only.
    pub fn apply_delta(
        &mut self,
        iris: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Vec<(GraphId, Arc<str>)> {
        // Collect only IRIs not already registered
        let mut new_iris: Vec<Arc<str>> = Vec::new();
        let mut seen: HashSet<Arc<str>> = HashSet::new();

        for iri in iris {
            let iri_ref = iri.as_ref();
            if !self.iri_to_id.contains_key(iri_ref) {
                let arc: Arc<str> = Arc::from(iri_ref);
                if seen.insert(arc.clone()) {
                    new_iris.push(arc);
                }
            }
        }

        if new_iris.is_empty() {
            return Vec::new();
        }

        // Sort lexicographically for deterministic assignment
        new_iris.sort();

        let mut assigned = Vec::with_capacity(new_iris.len());
        for arc in new_iris {
            let g_id = self.next_id;
            assert!(
                g_id >= FIRST_USER_GRAPH_ID,
                "apply_delta must never assign system graph IDs (0, 1, or 2)"
            );
            self.next_id += 1;

            // Extend id_to_iri to accommodate the new g_id
            while self.id_to_iri.len() <= g_id as usize {
                self.id_to_iri.push(None);
            }
            self.id_to_iri[g_id as usize] = Some(arc.clone());
            self.iri_to_id.insert(arc.clone(), g_id);
            assigned.push((g_id, arc));
        }

        assigned
    }

    /// Simulate allocation without mutating the registry.
    ///
    /// Returns a map of IRI→GraphId including both existing entries and
    /// what `apply_delta` would assign for new IRIs. Used during staging.
    pub fn provisional_ids(&self, new_iris: &[String]) -> HashMap<Arc<str>, GraphId> {
        let mut result: HashMap<Arc<str>, GraphId> = self.iri_to_id.clone();

        // Collect truly new IRIs (not already in registry)
        let mut truly_new: Vec<Arc<str>> = Vec::new();
        let mut seen: HashSet<&str> = HashSet::new();

        for iri in new_iris {
            if !self.iri_to_id.contains_key(iri.as_str()) && seen.insert(iri.as_str()) {
                truly_new.push(Arc::from(iri.as_str()));
            }
        }

        // Same deterministic sort as apply_delta
        truly_new.sort();

        for (next, arc) in (self.next_id..).zip(truly_new) {
            result.insert(arc, next);
        }

        result
    }

    /// Forward lookup: IRI → GraphId.
    pub fn graph_id_for_iri(&self, iri: &str) -> Option<GraphId> {
        self.iri_to_id.get(iri).copied()
    }

    /// Reverse lookup: GraphId → IRI.
    pub fn iri_for_graph_id(&self, g_id: GraphId) -> Option<&str> {
        self.id_to_iri
            .get(g_id as usize)
            .and_then(|opt| opt.as_deref())
    }

    /// Iterate all registered (g_id, iri) pairs. Skips empty slots.
    pub fn iter_entries(&self) -> impl Iterator<Item = (GraphId, &str)> {
        self.id_to_iri
            .iter()
            .enumerate()
            .filter_map(|(idx, opt)| opt.as_deref().map(|iri| (idx as GraphId, iri)))
    }

    /// Number of registered graphs (excluding default graph).
    pub fn len(&self) -> usize {
        self.iri_to_id.len()
    }

    /// True if no named graphs are registered.
    pub fn is_empty(&self) -> bool {
        self.iri_to_id.is_empty()
    }

    /// Next GraphId that would be assigned.
    pub fn next_id(&self) -> GraphId {
        self.next_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_meta_graph_iri() {
        assert_eq!(
            txn_meta_graph_iri("mydb:main"),
            "urn:fluree:mydb:main#txn-meta"
        );
        assert_eq!(
            txn_meta_graph_iri("test/ledger"),
            "urn:fluree:test/ledger#txn-meta"
        );
    }

    #[test]
    fn test_config_graph_iri() {
        assert_eq!(config_graph_iri("mydb:main"), "urn:fluree:mydb:main#config");
        assert_eq!(
            config_graph_iri("test/ledger"),
            "urn:fluree:test/ledger#config"
        );
    }

    #[test]
    fn test_default_registry() {
        let reg = GraphRegistry::default();
        assert_eq!(reg.next_id(), FIRST_USER_GRAPH_ID);
        // Default has no system graphs seeded
        assert_eq!(reg.iri_for_graph_id(0), None); // default graph
        assert_eq!(reg.iri_for_graph_id(1), None); // txn-meta not seeded
        assert_eq!(reg.iri_for_graph_id(2), None); // config not seeded
        assert_eq!(reg.len(), 0);
        assert!(reg.is_empty());
    }

    #[test]
    fn test_new_for_ledger() {
        let reg = GraphRegistry::new_for_ledger("mydb:main");
        assert_eq!(reg.next_id(), FIRST_USER_GRAPH_ID); // user graphs start at 3
        let txn_meta_iri = "urn:fluree:mydb:main#txn-meta";
        let config_iri = "urn:fluree:mydb:main#config";
        // txn-meta at g_id=1
        assert_eq!(reg.graph_id_for_iri(txn_meta_iri), Some(TXN_META_GRAPH_ID));
        assert_eq!(reg.iri_for_graph_id(TXN_META_GRAPH_ID), Some(txn_meta_iri));
        // config at g_id=2
        assert_eq!(reg.graph_id_for_iri(config_iri), Some(CONFIG_GRAPH_ID));
        assert_eq!(reg.iri_for_graph_id(CONFIG_GRAPH_ID), Some(config_iri));
        // default graph still None
        assert_eq!(reg.iri_for_graph_id(0), None);
        // 2 system graphs registered
        assert_eq!(reg.len(), 2);
        assert!(!reg.is_empty());
    }

    #[test]
    fn test_apply_delta_deterministic() {
        let mut reg1 = GraphRegistry::new_for_ledger("test:a");
        let mut reg2 = GraphRegistry::new_for_ledger("test:a");

        let assigned1 = reg1.apply_delta(["http://b.org/g", "http://a.org/g"]);
        let assigned2 = reg2.apply_delta(["http://a.org/g", "http://b.org/g"]);

        assert_eq!(assigned1, assigned2);
        // User graphs start at g_id=3
        assert_eq!(reg1.graph_id_for_iri("http://a.org/g"), Some(3));
        assert_eq!(reg1.graph_id_for_iri("http://b.org/g"), Some(4));
    }

    #[test]
    fn test_apply_delta_dedup() {
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        let assigned = reg.apply_delta([
            "http://example.org/g1",
            "http://example.org/g1",
            "http://example.org/g1",
        ]);
        assert_eq!(assigned.len(), 1);
        assert_eq!(
            reg.graph_id_for_iri("http://example.org/g1"),
            Some(FIRST_USER_GRAPH_ID)
        );
    }

    #[test]
    fn test_apply_delta_idempotent() {
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        reg.apply_delta(["http://example.org/g1"]);
        let assigned = reg.apply_delta(["http://example.org/g1"]);
        assert!(assigned.is_empty());
        assert_eq!(reg.next_id(), FIRST_USER_GRAPH_ID + 1); // 4
    }

    #[test]
    fn test_apply_delta_system_graphs_already_registered() {
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        let txn_meta = txn_meta_graph_iri("test:a");
        let config = config_graph_iri("test:a");
        // Applying system graph IRIs is a no-op (already seeded)
        let assigned = reg.apply_delta([txn_meta.as_str(), config.as_str()]);
        assert!(assigned.is_empty());
        assert_eq!(reg.next_id(), FIRST_USER_GRAPH_ID);
    }

    #[test]
    fn test_apply_delta_sequential() {
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        reg.apply_delta(["http://example.org/g1"]);
        reg.apply_delta(["http://example.org/g2"]);

        let txn_meta = txn_meta_graph_iri("test:a");
        let config = config_graph_iri("test:a");
        assert_eq!(reg.graph_id_for_iri("http://example.org/g1"), Some(3));
        assert_eq!(reg.graph_id_for_iri("http://example.org/g2"), Some(4));
        assert_eq!(reg.iri_for_graph_id(1), Some(txn_meta.as_str()));
        assert_eq!(reg.iri_for_graph_id(2), Some(config.as_str()));
        assert_eq!(reg.iri_for_graph_id(3), Some("http://example.org/g1"));
        assert_eq!(reg.iri_for_graph_id(4), Some("http://example.org/g2"));
    }

    #[test]
    fn test_seed_from_root_iris() {
        let txn_meta = txn_meta_graph_iri("test:a");
        let config = config_graph_iri("test:a");
        let iris = vec![
            txn_meta.clone(),                    // root[0] → g_id 1
            config.clone(),                      // root[1] → g_id 2
            "http://example.org/g1".to_string(), // root[2] → g_id 3
        ];
        let reg = GraphRegistry::seed_from_root_iris(&iris).unwrap();

        assert_eq!(reg.graph_id_for_iri(&txn_meta), Some(1));
        assert_eq!(reg.graph_id_for_iri(&config), Some(2));
        assert_eq!(reg.graph_id_for_iri("http://example.org/g1"), Some(3));
        assert_eq!(reg.next_id(), 4);
    }

    #[test]
    fn test_seed_from_root_iris_rejects_empty() {
        let err = GraphRegistry::seed_from_root_iris(&[]).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn test_seed_from_root_iris_trusts_any_txn_meta() {
        // seed_from_root_iris trusts whatever IRI is at iris[0]
        let iris = vec!["http://custom.org/txn-meta".to_string()];
        let reg = GraphRegistry::seed_from_root_iris(&iris).unwrap();
        assert_eq!(reg.graph_id_for_iri("http://custom.org/txn-meta"), Some(1));
    }

    #[test]
    fn test_seed_from_root_iris_rejects_empty_string() {
        let iris = vec![
            "urn:fluree:test:a#txn-meta".to_string(),
            String::new(), // empty IRI at root[1]
        ];
        let err = GraphRegistry::seed_from_root_iris(&iris).unwrap_err();
        assert!(err.to_string().contains("empty IRI"));
    }

    #[test]
    fn test_seed_from_root_iris_rejects_duplicate_iris() {
        let iris = vec![
            "urn:fluree:test:a#txn-meta".to_string(),
            "http://example.org/dup".to_string(),
            "http://example.org/dup".to_string(),
        ];
        let err = GraphRegistry::seed_from_root_iris(&iris).unwrap_err();
        assert!(err.to_string().contains("duplicate IRI"));
    }

    #[test]
    fn test_seed_from_root_iris_next_id_floor() {
        // Single entry (just txn-meta) → next_id must be FIRST_USER_GRAPH_ID
        let iris = vec!["urn:fluree:test:a#txn-meta".to_string()];
        let reg = GraphRegistry::seed_from_root_iris(&iris).unwrap();
        assert_eq!(reg.next_id(), FIRST_USER_GRAPH_ID);
    }

    #[test]
    fn test_seed_from_entries() {
        let txn_meta = txn_meta_graph_iri("test:a");
        let config = config_graph_iri("test:a");
        let entries = vec![
            (1u16, txn_meta.as_str()),
            (2u16, config.as_str()),
            (3u16, "http://example.org/g1"),
        ];
        let reg = GraphRegistry::seed_from_entries(&entries).unwrap();

        assert_eq!(reg.graph_id_for_iri(&txn_meta), Some(1));
        assert_eq!(reg.graph_id_for_iri(&config), Some(2));
        assert_eq!(reg.graph_id_for_iri("http://example.org/g1"), Some(3));
        assert_eq!(reg.next_id(), 4);
    }

    #[test]
    fn test_seed_from_entries_empty_returns_default() {
        let reg = GraphRegistry::seed_from_entries(&[]).unwrap();
        // Empty entries → default (no system graphs seeded)
        assert_eq!(reg.iri_for_graph_id(1), None);
        assert_eq!(reg.next_id(), FIRST_USER_GRAPH_ID);
    }

    #[test]
    fn test_seed_from_entries_single_gid1() {
        let txn_meta = txn_meta_graph_iri("test:a");
        let entries = vec![(1u16, txn_meta.as_str())];
        let reg = GraphRegistry::seed_from_entries(&entries).unwrap();
        assert_eq!(reg.next_id(), FIRST_USER_GRAPH_ID);
    }

    #[test]
    fn test_seed_from_entries_rejects_gid0() {
        let entries = vec![(0u16, "bad")];
        let err = GraphRegistry::seed_from_entries(&entries).unwrap_err();
        assert!(err.to_string().contains("g_id=0"));
    }

    #[test]
    fn test_provisional_ids() {
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        reg.apply_delta(["http://example.org/existing"]);

        let prov = reg.provisional_ids(&[
            "http://example.org/existing".into(),
            "http://example.org/new_b".into(),
            "http://example.org/new_a".into(),
        ]);

        // User graphs start at g_id=3 (existing got 3, new ones get 4 and 5)
        assert_eq!(prov.get("http://example.org/existing").copied(), Some(3));
        assert_eq!(prov.get("http://example.org/new_a").copied(), Some(4));
        assert_eq!(prov.get("http://example.org/new_b").copied(), Some(5));
        // System graphs always present
        let txn_meta = txn_meta_graph_iri("test:a");
        let config = config_graph_iri("test:a");
        assert_eq!(prov.get(txn_meta.as_str()).copied(), Some(1));
        assert_eq!(prov.get(config.as_str()).copied(), Some(2));

        // Registry unchanged
        assert_eq!(reg.next_id(), 4);
        assert_eq!(reg.graph_id_for_iri("http://example.org/new_a"), None);
    }

    #[test]
    fn test_iter_entries() {
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        reg.apply_delta(["http://b.org/g", "http://a.org/g"]);

        let entries: Vec<(GraphId, &str)> = reg.iter_entries().collect();
        let txn_meta = txn_meta_graph_iri("test:a");
        let config = config_graph_iri("test:a");
        // 4 entries: txn-meta at 1, config at 2, user graphs at 3 and 4
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0], (1, txn_meta.as_str()));
        assert_eq!(entries[1], (2, config.as_str()));
        assert_eq!(entries[2], (3, "http://a.org/g"));
        assert_eq!(entries[3], (4, "http://b.org/g"));
    }

    #[test]
    fn test_seed_then_apply_delta() {
        let txn_meta = txn_meta_graph_iri("test:a");
        let config = config_graph_iri("test:a");
        let iris = vec![
            txn_meta.clone(),
            config.clone(),
            "http://example.org/g1".to_string(),
        ];
        let mut reg = GraphRegistry::seed_from_root_iris(&iris).unwrap();

        let assigned = reg.apply_delta(["http://example.org/g2"]);
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, 4);
        assert_eq!(reg.graph_id_for_iri("http://example.org/g2"), Some(4));
        assert_eq!(reg.graph_id_for_iri("http://example.org/g1"), Some(3));
        assert_eq!(reg.graph_id_for_iri(&config), Some(2));
        assert_eq!(reg.graph_id_for_iri(&txn_meta), Some(1));
    }

    #[test]
    fn test_system_graph_id_constants() {
        assert_eq!(TXN_META_GRAPH_ID, 1);
        assert_eq!(CONFIG_GRAPH_ID, 2);
        assert_eq!(FIRST_USER_GRAPH_ID, 3);
    }
}
