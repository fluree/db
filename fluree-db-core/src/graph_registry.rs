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

/// Whether a drop may take `g_id`: the default graph or a user graph, never a
/// system graph. `DROP` refuses one, but a drop record is read again wherever
/// it replays, and an IRI it names as an ordinary graph can name a system
/// graph there: another ledger's config graph IRI, or the branch's own once
/// the record came from a branch that held it as an ordinary graph. Every
/// reader of a record keeps only these.
pub const fn drop_may_take(g_id: GraphId) -> bool {
    g_id == DEFAULT_GRAPH_ID || g_id >= FIRST_USER_GRAPH_ID
}

/// The name ledger info gives the default graph, which has no IRI of its own.
///
/// It names the default graph wherever a graph is read or chosen as a default
/// graph: a query's dataset and `GRAPH` patterns, an update's `USING` /
/// `USING NAMED` / `WITH` and `GRAPH` patterns in its WHERE, and the JSON-LD
/// twins. The default graph is no named graph, so a write that names a graph
/// by it (a `GRAPH` template or quad, a TriG block, an import, a graph
/// management or sync target) is refused rather than creating one.
pub const DEFAULT_GRAPH_IRI: &str = "urn:default";

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

/// Validate that `value` is an absolute IRI acceptable as a graph target.
///
/// A minimal check, not a full RFC 3987 parser: no whitespace / C0 / DEL /
/// RFC 3987-excluded characters (`<`, `>`, `"`, `{`, `}`, `|`, `\\`, `^`,
/// `` ` ``), and a `<scheme>:<rest>` head per RFC 3986 §3.1. Returns the
/// error message a caller surfaces as a bad request.
pub fn validate_absolute_graph_iri(value: &str) -> std::result::Result<(), String> {
    if value.is_empty() {
        return Err("graph IRI is required and cannot be empty".to_string());
    }
    if value.chars().any(|c| {
        matches!(c, '<' | '>' | '"' | '{' | '}' | '|' | '\\' | '^' | '`')
            || c.is_whitespace()
            || c <= '\u{20}'
            || c == '\u{7F}'
    }) {
        return Err(format!(
            "Invalid graph IRI '{value}': contains whitespace, a control character, \
             or a character not allowed in an IRI \
             (one of `<`, `>`, `\"`, `{{`, `}}`, `|`, `\\`, `^`, `` ` ``)"
        ));
    }
    let (scheme, rest) = value.split_once(':').ok_or_else(|| {
        format!("Invalid graph IRI '{value}': missing scheme (expected an absolute IRI like 'urn:...' or 'http://...')")
    })?;
    let mut sc = scheme.chars();
    let first = sc
        .next()
        .ok_or_else(|| format!("Invalid graph IRI '{value}': scheme is empty"))?;
    if !first.is_ascii_alphabetic() {
        return Err(format!(
            "Invalid graph IRI '{value}': scheme must start with an ASCII letter"
        ));
    }
    if !sc.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')) {
        return Err(format!(
            "Invalid graph IRI '{value}': scheme may only contain letters, digits, '+', '-', '.'"
        ));
    }
    if rest.is_empty() {
        return Err(format!(
            "Invalid graph IRI '{value}': nothing follows the scheme"
        ));
    }
    Ok(())
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

/// What the latest drop of a graph left of it.
///
/// Every fact the graph held before `floor` (the `t` of the dropping commit)
/// is gone, at the head and as of any earlier `t`. A named graph stays
/// dropped until a commit writes it again (`revived`); the default graph is
/// never without a graph, so its drop revives it at the same `t`. A graph
/// keeps its g_id across drops: the floor, not a new id, separates what it
/// holds now from what it held before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphDrop {
    /// `t` of the commit that dropped the graph last.
    pub floor: i64,
    /// `t` of the first commit at or after `floor` that wrote the graph again,
    /// if any.
    pub revived: Option<i64>,
}

/// Wire form of a root's graph drops (`IndexRoot::graph_drops` in
/// `fluree-db-binary-index`): `u32` count, then per graph
/// `g_id: u16`, `floor: i64`, `revived: u8 presence + i64`, all little-endian.
/// Shared by both FIR6 decoders.
pub fn encode_graph_drops(drops: &[(GraphId, GraphDrop)], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(drops.len() as u32).to_le_bytes());
    for (g_id, drop) in drops {
        buf.extend_from_slice(&g_id.to_le_bytes());
        buf.extend_from_slice(&drop.floor.to_le_bytes());
        match drop.revived {
            Some(t) => {
                buf.push(1);
                buf.extend_from_slice(&t.to_le_bytes());
            }
            None => {
                buf.push(0);
                buf.extend_from_slice(&0i64.to_le_bytes());
            }
        }
    }
}

/// Inverse of [`encode_graph_drops`], reading at `pos`.
pub fn decode_graph_drops(
    data: &[u8],
    pos: &mut usize,
) -> std::io::Result<Vec<(GraphId, GraphDrop)>> {
    const ENTRY_LEN: usize = 2 + 8 + 1 + 8;
    let bad = |msg: &str| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("graph_drops: {msg}"),
        )
    };
    let count_bytes = data
        .get(*pos..*pos + 4)
        .ok_or_else(|| bad("truncated count"))?;
    let count = u32::from_le_bytes(count_bytes.try_into().expect("4 bytes")) as usize;
    *pos += 4;
    let body = data
        .get(*pos..*pos + count.saturating_mul(ENTRY_LEN))
        .ok_or_else(|| bad("truncated entries"))?;
    let mut drops: Vec<(GraphId, GraphDrop)> = Vec::with_capacity(count);
    for entry in body.chunks_exact(ENTRY_LEN) {
        let g_id = u16::from_le_bytes([entry[0], entry[1]]);
        let floor = i64::from_le_bytes(entry[2..10].try_into().expect("8 bytes"));
        let revived = match entry[10] {
            0 => None,
            1 => Some(i64::from_le_bytes(
                entry[11..19].try_into().expect("8 bytes"),
            )),
            other => return Err(bad(&format!("revived presence byte {other}"))),
        };
        if drops.last().is_some_and(|(last, _)| *last >= g_id) {
            return Err(bad("entries must be ascending by g_id"));
        }
        drops.push((g_id, GraphDrop { floor, revived }));
    }
    *pos += body.len();
    Ok(drops)
}

/// Whether `iri` is a ledger's config graph IRI ([`config_graph_iri`]),
/// whichever ledger it names. A commit carries the IRI of the ledger it was
/// written in, so a branch replaying its source's history, or a copy under
/// another name, reads the source's config graph as its own.
pub fn names_config_graph(iri: &str) -> bool {
    config_graph_ledger(iri).is_some()
}

/// The ledger whose config graph IRI ([`config_graph_iri`]) `iri` is.
pub fn config_graph_ledger(iri: &str) -> Option<crate::LedgerId> {
    system_graph_ledger(iri, "config")
}

/// Whether `iri` is a ledger's txn-meta graph IRI ([`txn_meta_graph_iri`]),
/// whichever ledger it names.
pub fn names_txn_meta_graph(iri: &str) -> bool {
    txn_meta_graph_ledger(iri).is_some()
}

/// The ledger whose txn-meta graph IRI ([`txn_meta_graph_iri`]) `iri` is.
pub fn txn_meta_graph_ledger(iri: &str) -> Option<crate::LedgerId> {
    system_graph_ledger(iri, "txn-meta")
}

fn system_graph_ledger(iri: &str, graph: &str) -> Option<crate::LedgerId> {
    // The last `#`: names from before the ledger-id grammar may hold one.
    iri.strip_prefix("urn:fluree:")
        .and_then(|rest| rest.rsplit_once('#'))
        .filter(|(_, name)| *name == graph)
        .and_then(|(ledger, _)| crate::LedgerId::parse_persisted(ledger).ok())
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
    /// Other ledgers' config graph IRIs the commits carry, each the config
    /// graph here ([`names_config_graph`]); also in `iri_to_id`.
    aliases: Vec<Arc<str>>,
    /// Next available GraphId for assignment (always >= FIRST_USER_GRAPH_ID).
    next_id: GraphId,
    /// The graphs a commit has dropped, with what each drop left. A registry
    /// entry is never removed: a dropped graph keeps its IRI and id.
    drops: std::collections::BTreeMap<GraphId, GraphDrop>,
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
            aliases: Vec::new(),
            next_id: FIRST_USER_GRAPH_ID,
            drops: std::collections::BTreeMap::new(),
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
            aliases: Vec::new(),
            next_id: FIRST_USER_GRAPH_ID,
            drops: std::collections::BTreeMap::new(),
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
    /// A root from before config graphs were read across ledgers may list
    /// another ledger's config graph as a user graph, its rows under that
    /// graph's id. It stays the ordinary graph the index holds until a
    /// rebuild from the commits ([`Self::has_config_in_user_slot`]).
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
            aliases: Vec::new(),
            next_id,
            drops: std::collections::BTreeMap::new(),
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
            aliases: Vec::new(),
            next_id,
            drops: std::collections::BTreeMap::new(),
        })
    }

    /// Apply a delta of graph IRIs from a commit envelope.
    ///
    /// New IRIs (not already in registry) are deduped, sorted lexicographically,
    /// and assigned sequential GraphIds from `next_id`. Returns the newly assigned
    /// `(GraphId, Arc<str>)` pairs. Another ledger's config graph IRI is
    /// assigned none: it is the config graph here.
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
                if names_config_graph(iri_ref) {
                    self.iri_to_id.insert(arc.clone(), CONFIG_GRAPH_ID);
                    self.aliases.push(arc);
                } else if seen.insert(arc.clone()) {
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
            if self.iri_to_id.contains_key(iri.as_str()) {
                continue;
            }
            if names_config_graph(iri) {
                result.insert(Arc::from(iri.as_str()), CONFIG_GRAPH_ID);
            } else if seen.insert(iri.as_str()) {
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

    /// The id assigned to `iri`, which routes a write to it: any ledger's
    /// config graph IRI is the config graph. A dropped graph keeps its id, so
    /// this answers for it too. A reader that needs the graph to exist (a
    /// configured source, say) asks [`Self::existing_graph_id`].
    pub fn assigned_graph_id(&self, iri: &str) -> Option<GraphId> {
        self.iri_to_id
            .get(iri)
            .copied()
            .or_else(|| names_config_graph(iri).then_some(CONFIG_GRAPH_ID))
    }

    /// The graph `iri` names as of `t`, for a reader that needs it to exist
    /// then: `Ok(None)` when it does not (never assigned, or dropped and not
    /// written since), and `Err(floor)` when a drop at `floor`, after `t`,
    /// took what it held as of `t`.
    pub fn existing_graph_id(
        &self,
        iri: &str,
        t: i64,
    ) -> std::result::Result<Option<GraphId>, i64> {
        let Some(g_id) = self.assigned_graph_id(iri) else {
            return Ok(None);
        };
        if let Some(floor) = self.drop_after(g_id, t) {
            return Err(floor);
        }
        Ok(self.visible_at(g_id, t).then_some(g_id))
    }

    /// Whether any of `iris` is not yet registered, or is dropped — that is,
    /// whether a commit writing to them would register a graph, or bring a
    /// dropped one back. Any ledger's config graph IRI is the config graph,
    /// registered.
    pub fn has_unregistered<'a>(&self, iris: impl IntoIterator<Item = &'a str>) -> bool {
        iris.into_iter().any(|iri| match self.iri_to_id.get(iri) {
            Some(g_id) => self.is_dropped(*g_id),
            None => !names_config_graph(iri),
        })
    }

    /// Record that a commit at `t` dropped `g_ids`: what they held before `t`
    /// is gone. The default graph is revived at once, since there is always
    /// a default graph; a named graph stays dropped until [`Self::revive`].
    pub fn apply_drops(&mut self, g_ids: &[GraphId], t: i64) {
        for &g_id in g_ids {
            let revived = (g_id == DEFAULT_GRAPH_ID).then_some(t);
            self.drops.insert(g_id, GraphDrop { floor: t, revived });
        }
    }

    /// Record that a commit at `t` wrote `iris`, after its drops applied:
    /// each of them that is dropped exists again from `t`.
    pub fn revive<'a>(&mut self, iris: impl IntoIterator<Item = &'a str>, t: i64) {
        if self.drops.is_empty() {
            return;
        }
        for iri in iris {
            let Some(g_id) = self.iri_to_id.get(iri) else {
                continue;
            };
            if let Some(drop) = self.drops.get_mut(g_id) {
                if drop.revived.is_none() && drop.floor <= t {
                    drop.revived = Some(t);
                }
            }
        }
    }

    /// Whether a commit writing `iris` would bring a dropped graph back.
    pub fn would_revive<'a>(&self, iris: impl IntoIterator<Item = &'a str>) -> bool {
        !self.drops.is_empty()
            && iris
                .into_iter()
                .any(|iri| self.iri_to_id.get(iri).is_some_and(|g| self.is_dropped(*g)))
    }

    /// The ids of the graphs a commit's drop record names (`Commit::
    /// graph_drops`): [`DEFAULT_GRAPH_IRI`] for the default graph, else a
    /// registered IRI. An IRI the registry does not hold, or holds as a
    /// system graph ([`drop_may_take`]), names nothing here.
    pub fn graph_ids_of_drops<'a>(
        &self,
        iris: impl IntoIterator<Item = &'a String>,
    ) -> Vec<GraphId> {
        iris.into_iter()
            .filter_map(|iri| {
                if iri == DEFAULT_GRAPH_IRI {
                    Some(DEFAULT_GRAPH_ID)
                } else {
                    self.assigned_graph_id(iri)
                }
            })
            .filter(|g_id| drop_may_take(*g_id))
            .collect()
    }

    /// The drop record naming `g_ids`, as `Commit::graph_drops` holds it:
    /// IRIs, ascending.
    pub fn drop_record(&self, g_ids: &[GraphId]) -> Vec<String> {
        let mut iris: Vec<String> = g_ids
            .iter()
            .filter_map(|g_id| {
                if *g_id == DEFAULT_GRAPH_ID {
                    Some(DEFAULT_GRAPH_IRI.to_string())
                } else {
                    self.iri_for_graph_id(*g_id).map(str::to_string)
                }
            })
            .collect();
        iris.sort_unstable();
        iris.dedup();
        iris
    }

    /// What the latest drop of `g_id` left, if it was ever dropped.
    pub fn graph_drop(&self, g_id: GraphId) -> Option<GraphDrop> {
        self.drops.get(&g_id).copied()
    }

    /// Every dropped graph with what its latest drop left, ascending by id.
    pub fn graph_drops(&self) -> impl Iterator<Item = (GraphId, GraphDrop)> + '_ {
        self.drops.iter().map(|(g_id, drop)| (*g_id, *drop))
    }

    /// Whether any graph was ever dropped.
    pub fn has_drops(&self) -> bool {
        !self.drops.is_empty()
    }

    /// Add `other`'s drop records for the same ledger, keeping for each graph
    /// the later drop, and its revival when either knows of one.
    pub fn merge_graph_drops(&mut self, other: impl IntoIterator<Item = (GraphId, GraphDrop)>) {
        for (g_id, theirs) in other {
            let entry = self.drops.entry(g_id).or_insert(theirs);
            if theirs.floor > entry.floor {
                *entry = theirs;
            } else if theirs.floor == entry.floor && entry.revived.is_none() {
                entry.revived = theirs.revived;
            }
        }
    }

    /// [`Self::merge_graph_drops`] for the drop records of `other`, which
    /// may number the same ledger's graphs differently (a rebuild numbers
    /// them as it meets them): each graph is matched by IRI. A graph `self`
    /// does not register, or registers as a system graph, is left out.
    pub fn merge_graph_drops_by_iri(&mut self, other: &GraphRegistry) {
        let drops: Vec<(GraphId, GraphDrop)> = other
            .graph_drops()
            .filter_map(|(g_id, drop)| {
                let ours = if g_id == DEFAULT_GRAPH_ID {
                    DEFAULT_GRAPH_ID
                } else {
                    self.assigned_graph_id(other.iri_for_graph_id(g_id)?)?
                };
                drop_may_take(ours).then_some((ours, drop))
            })
            .collect();
        self.merge_graph_drops(drops);
    }

    /// Replace the drop records, as an index root or another registry for the
    /// same ledger carries them.
    pub fn set_graph_drops(&mut self, drops: impl IntoIterator<Item = (GraphId, GraphDrop)>) {
        self.drops = drops.into_iter().collect();
    }

    /// Whether `g_id` is dropped at the head and not written since.
    pub fn is_dropped(&self, g_id: GraphId) -> bool {
        self.drops
            .get(&g_id)
            .is_some_and(|drop| drop.revived.is_none())
    }

    /// Whether `g_id`'s state as of `t` is visible: false when a drop after
    /// `t` took it, or when `t` falls between a drop and the graph's revival.
    /// Says nothing of whether the graph was registered yet at `t`.
    pub fn visible_at(&self, g_id: GraphId, t: i64) -> bool {
        self.drops
            .get(&g_id)
            .is_none_or(|drop| drop.revived.is_some_and(|revived| revived <= t))
    }

    /// The `t` of `g_id`'s latest drop when that drop came after `t`: what the
    /// graph held as of `t` is gone, which a read naming it as of `t` should
    /// hear about rather than read as empty.
    pub fn drop_after(&self, g_id: GraphId, t: i64) -> Option<i64> {
        self.drops
            .get(&g_id)
            .map(|drop| drop.floor)
            .filter(|floor| *floor > t)
    }

    /// The graphs whose facts before some `t` greater than `index_t` are
    /// gone: what an index covering only `index_t` still holds of them must
    /// not be read.
    pub fn dropped_after(&self, index_t: i64) -> Vec<GraphId> {
        self.drops
            .iter()
            .filter(|(_, drop)| drop.floor > index_t)
            .map(|(g_id, _)| *g_id)
            .collect()
    }

    /// Reverse lookup: GraphId → IRI.
    pub fn iri_for_graph_id(&self, g_id: GraphId) -> Option<&str> {
        self.id_to_iri
            .get(g_id as usize)
            .and_then(|opt| opt.as_deref())
    }

    /// Iterate all registered (g_id, iri) pairs. Skips empty slots. Dropped
    /// graphs are included: this is the id assignment, which routes writes and
    /// keeps a dropped graph's id. What a query or a listing sees is
    /// [`Self::iter_visible_entries`].
    pub fn iter_entries(&self) -> impl Iterator<Item = (GraphId, &str)> {
        self.id_to_iri
            .iter()
            .enumerate()
            .filter_map(|(idx, opt)| opt.as_deref().map(|iri| (idx as GraphId, iri)))
    }

    /// [`Self::iter_entries`] less the graphs not visible as of `t` (see
    /// [`Self::visible_at`]): the graphs a dataset, a `GRAPH ?g` or a
    /// listing at `t` names.
    pub fn iter_visible_entries(&self, t: i64) -> impl Iterator<Item = (GraphId, &str)> {
        self.iter_entries()
            .filter(move |(g_id, _)| self.visible_at(*g_id, t))
    }

    /// Whether the index root holds a config graph IRI as an ordinary graph:
    /// a root built before any ledger's config graph IRI named the config
    /// graph. That config is read only once the index is rebuilt from the
    /// commits.
    pub fn has_config_in_user_slot(&self) -> bool {
        self.iter_entries()
            .any(|(g_id, iri)| g_id >= FIRST_USER_GRAPH_ID && names_config_graph(iri))
    }

    /// Other ledgers' config graph IRIs the commits carry, each the config
    /// graph here. Not in [`Self::iter_entries`].
    pub fn iter_aliases(&self) -> impl Iterator<Item = (GraphId, &str)> {
        self.aliases
            .iter()
            .map(|iri| (CONFIG_GRAPH_ID, iri.as_ref()))
    }

    /// Number of registered graphs (excluding default graph).
    pub fn len(&self) -> usize {
        self.iri_to_id.len() - self.aliases.len()
    }

    /// True if no named graphs are registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        assert_eq!(reg.assigned_graph_id(txn_meta_iri), Some(TXN_META_GRAPH_ID));
        assert_eq!(reg.iri_for_graph_id(TXN_META_GRAPH_ID), Some(txn_meta_iri));
        // config at g_id=2
        assert_eq!(reg.assigned_graph_id(config_iri), Some(CONFIG_GRAPH_ID));
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
        assert_eq!(reg1.assigned_graph_id("http://a.org/g"), Some(3));
        assert_eq!(reg1.assigned_graph_id("http://b.org/g"), Some(4));
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
            reg.assigned_graph_id("http://example.org/g1"),
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
        assert_eq!(reg.assigned_graph_id("http://example.org/g1"), Some(3));
        assert_eq!(reg.assigned_graph_id("http://example.org/g2"), Some(4));
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

        assert_eq!(reg.assigned_graph_id(&txn_meta), Some(1));
        assert_eq!(reg.assigned_graph_id(&config), Some(2));
        assert_eq!(reg.assigned_graph_id("http://example.org/g1"), Some(3));
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
        assert_eq!(reg.assigned_graph_id("http://custom.org/txn-meta"), Some(1));
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

        assert_eq!(reg.assigned_graph_id(&txn_meta), Some(1));
        assert_eq!(reg.assigned_graph_id(&config), Some(2));
        assert_eq!(reg.assigned_graph_id("http://example.org/g1"), Some(3));
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
        assert_eq!(reg.assigned_graph_id("http://example.org/new_a"), None);
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
        assert_eq!(reg.assigned_graph_id("http://example.org/g2"), Some(4));
        assert_eq!(reg.assigned_graph_id("http://example.org/g1"), Some(3));
        assert_eq!(reg.assigned_graph_id(&config), Some(2));
        assert_eq!(reg.assigned_graph_id(&txn_meta), Some(1));
    }

    #[test]
    fn a_dropped_graph_keeps_its_id_until_written_again() {
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        reg.apply_delta(["http://example.org/g"]);
        let g = reg.assigned_graph_id("http://example.org/g").unwrap();
        assert!(!reg.has_unregistered(["http://example.org/g"]));

        reg.apply_drops(&[g], 5);
        assert!(reg.is_dropped(g));
        assert_eq!(reg.assigned_graph_id("http://example.org/g"), Some(g));
        // Writing it again would bring it back, as a new graph would register.
        assert!(reg.has_unregistered(["http://example.org/g"]));
        assert!(reg.would_revive(["http://example.org/g"]));
        assert!(!reg.visible_at(g, 4));
        assert!(!reg.visible_at(g, 5));
        assert_eq!(
            reg.iter_visible_entries(5)
                .filter(|(id, _)| *id == g)
                .count(),
            0
        );
        assert_eq!(reg.dropped_after(4), vec![g]);
        assert!(reg.dropped_after(5).is_empty());

        reg.revive(["http://example.org/g"], 7);
        assert!(!reg.is_dropped(g));
        assert_eq!(
            reg.graph_drop(g),
            Some(GraphDrop {
                floor: 5,
                revived: Some(7)
            })
        );
        assert!(!reg.visible_at(g, 6));
        assert!(reg.visible_at(g, 7));
        // Assignment is unchanged.
        assert_eq!(reg.apply_delta(["http://example.org/g"]), vec![]);
        assert_eq!(reg.next_id(), g + 1);
    }

    /// A graph exists as of `t` when written by then and not dropped since;
    /// a drop after `t` took what it held then.
    #[test]
    fn an_existing_graph_is_one_no_drop_took_as_of_t() {
        let g = "http://example.org/g";
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        assert_eq!(reg.existing_graph_id(g, 1), Ok(None));
        reg.apply_delta([g]);
        let g_id = reg.assigned_graph_id(g).unwrap();
        assert_eq!(reg.existing_graph_id(g, 1), Ok(Some(g_id)));

        reg.apply_drops(&[g_id], 5);
        assert_eq!(reg.existing_graph_id(g, 4), Err(5));
        assert_eq!(reg.existing_graph_id(g, 5), Ok(None));
        assert_eq!(reg.existing_graph_id(g, 9), Ok(None));

        reg.revive([g], 7);
        assert_eq!(reg.existing_graph_id(g, 4), Err(5));
        assert_eq!(reg.existing_graph_id(g, 6), Ok(None));
        assert_eq!(reg.existing_graph_id(g, 7), Ok(Some(g_id)));
        assert_eq!(
            reg.existing_graph_id(&config_graph_iri("test:a"), 4),
            Ok(Some(CONFIG_GRAPH_ID))
        );
    }

    #[test]
    fn a_commit_can_drop_a_graph_and_write_it_afresh() {
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        reg.apply_delta(["http://example.org/g"]);
        let g = reg.assigned_graph_id("http://example.org/g").unwrap();
        reg.apply_drops(&[g], 5);
        reg.revive(["http://example.org/g"], 5);
        assert_eq!(
            reg.graph_drop(g),
            Some(GraphDrop {
                floor: 5,
                revived: Some(5)
            })
        );
        assert!(!reg.visible_at(g, 4));
        assert!(reg.visible_at(g, 5));
    }

    #[test]
    fn the_default_graph_survives_its_drop() {
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        reg.apply_drops(&[DEFAULT_GRAPH_ID], 3);
        assert!(!reg.is_dropped(DEFAULT_GRAPH_ID));
        assert!(!reg.visible_at(DEFAULT_GRAPH_ID, 2));
        assert!(reg.visible_at(DEFAULT_GRAPH_ID, 3));
        assert_eq!(reg.dropped_after(2), vec![DEFAULT_GRAPH_ID]);
    }

    #[test]
    fn a_later_drop_replaces_the_earlier_one() {
        let mut reg = GraphRegistry::new_for_ledger("test:a");
        reg.apply_delta(["http://example.org/g"]);
        let g = reg.assigned_graph_id("http://example.org/g").unwrap();
        reg.apply_drops(&[g], 5);
        reg.revive(["http://example.org/g"], 6);
        reg.apply_drops(&[g], 9);
        assert_eq!(
            reg.graph_drop(g),
            Some(GraphDrop {
                floor: 9,
                revived: None
            })
        );
        // A write before the latest drop does not revive past it.
        reg.revive(["http://example.org/g"], 8);
        assert!(reg.is_dropped(g));
        assert!(!reg.visible_at(g, 7));
    }

    #[test]
    fn drops_merge_by_iri_across_numberings() {
        let (a, z) = ("http://example.org/a", "http://example.org/z");
        let mut head = GraphRegistry::new_for_ledger("test:a");
        head.apply_delta([a, z]);
        head.apply_drops(&[head.assigned_graph_id(a).unwrap(), DEFAULT_GRAPH_ID], 5);

        // A rebuild met z first.
        let mut rebuilt = GraphRegistry::new_for_ledger("test:a");
        rebuilt.apply_delta([z]);
        rebuilt.apply_delta([a]);
        assert_ne!(rebuilt.assigned_graph_id(a), head.assigned_graph_id(a));

        rebuilt.merge_graph_drops_by_iri(&head);
        assert!(rebuilt.is_dropped(rebuilt.assigned_graph_id(a).unwrap()));
        assert_eq!(
            rebuilt.graph_drop(rebuilt.assigned_graph_id(z).unwrap()),
            None
        );
        assert_eq!(
            rebuilt.graph_drop(DEFAULT_GRAPH_ID).map(|d| d.floor),
            Some(5)
        );
    }

    /// A drop record replayed where an IRI it names as an ordinary graph is a
    /// system graph drops nothing there: another ledger's config graph IRI,
    /// which an index from before every such IRI named the config graph held
    /// as an ordinary graph, or the ledger's own system graph IRIs.
    #[test]
    fn a_drop_record_never_drops_a_system_graph() {
        let g = "http://example.org/g";
        let source = config_graph_iri("a:main");
        let record = [
            DEFAULT_GRAPH_IRI.to_string(),
            source.clone(),
            config_graph_iri("b:main"),
            txn_meta_graph_iri("b:main"),
            g.to_string(),
        ];

        let legacy_iris = vec![
            txn_meta_graph_iri("b:main"),
            config_graph_iri("b:main"),
            source.clone(),
            g.to_string(),
        ];
        let mut legacy = GraphRegistry::seed_from_root_iris(&legacy_iris).unwrap();
        assert_eq!(
            legacy.graph_ids_of_drops(&record),
            vec![DEFAULT_GRAPH_ID, 3, 4]
        );
        legacy.apply_drops(&[3, 4], 5);

        let mut rebuilt = GraphRegistry::new_for_ledger("b:main");
        rebuilt.apply_delta([source.as_str(), g]);
        let g_id = rebuilt.assigned_graph_id(g).unwrap();
        assert_eq!(
            rebuilt.graph_ids_of_drops(&record),
            vec![DEFAULT_GRAPH_ID, g_id]
        );
        rebuilt.merge_graph_drops_by_iri(&legacy);
        assert_eq!(rebuilt.graph_drop(CONFIG_GRAPH_ID), None);
        assert!(rebuilt.is_dropped(g_id));
    }

    #[test]
    fn any_ledgers_config_graph_iri_is_recognized() {
        assert!(names_config_graph(&config_graph_iri("a:main")));
        assert!(names_config_graph(&config_graph_iri("org/a:dev")));
        // A name from before the ledger-id grammar, holding `#`.
        assert!(names_config_graph("urn:fluree:a#b:main#config"));
        assert!(names_txn_meta_graph(&txn_meta_graph_iri("a:main")));
        assert!(!names_txn_meta_graph(&config_graph_iri("a:main")));
        for other in [
            "urn:fluree:a:main#configs",
            "urn:fluree:#config",
            "urn:fluree:a/../b:main#config",
            "http://example.org/a:main#config",
            "urn:fluree:materialize#appliedSequence",
        ] {
            assert!(!names_config_graph(other), "{other}");
        }
    }

    #[test]
    fn another_ledgers_config_graph_is_the_config_graph() {
        let mut reg = GraphRegistry::new_for_ledger("b:main");
        let source = config_graph_iri("a:main");
        assert_eq!(reg.assigned_graph_id(&source), Some(CONFIG_GRAPH_ID));
        assert!(!reg.has_unregistered([source.as_str()]));
        let prov = reg.provisional_ids(&[source.clone(), "http://example.org/g".into()]);
        assert_eq!(prov.get(source.as_str()).copied(), Some(CONFIG_GRAPH_ID));
        assert_eq!(
            prov.get("http://example.org/g").copied(),
            Some(FIRST_USER_GRAPH_ID)
        );

        let assigned = reg.apply_delta([source.as_str(), "http://example.org/g"]);
        assert_eq!(assigned.len(), 1);
        assert_eq!(
            reg.assigned_graph_id("http://example.org/g"),
            Some(FIRST_USER_GRAPH_ID)
        );
        assert_eq!(reg.assigned_graph_id(&source), Some(CONFIG_GRAPH_ID));
        assert_eq!(
            reg.iri_for_graph_id(CONFIG_GRAPH_ID),
            Some(config_graph_iri("b:main").as_str())
        );
        assert_eq!(
            reg.iter_aliases().collect::<Vec<_>>(),
            vec![(CONFIG_GRAPH_ID, source.as_str())]
        );
        assert_eq!(reg.len(), 3);
    }

    #[test]
    fn a_root_listing_another_ledgers_config_graph_as_a_user_graph_keeps_it() {
        let source = config_graph_iri("a:main");
        let iris = vec![
            txn_meta_graph_iri("b:main"),
            config_graph_iri("b:main"),
            source.clone(),
            "http://example.org/g".to_string(),
        ];
        let entries: Vec<(GraphId, &str)> = iris
            .iter()
            .enumerate()
            .map(|(i, iri)| (i as GraphId + 1, iri.as_str()))
            .collect();
        for mut reg in [
            GraphRegistry::seed_from_root_iris(&iris).unwrap(),
            GraphRegistry::seed_from_entries(&entries).unwrap(),
        ] {
            assert!(reg.has_config_in_user_slot());
            assert_eq!(reg.assigned_graph_id(&source), Some(3));
            assert_eq!(reg.iri_for_graph_id(3), Some(source.as_str()));
            assert_eq!(reg.next_id(), 5);
            // Commits replayed over the index route to it as the index does.
            assert!(reg.apply_delta([source.as_str()]).is_empty());
            assert_eq!(reg.assigned_graph_id(&source), Some(3));
            // Another ledger's config graph IRI the root does not list is
            // still the config graph.
            let other = config_graph_iri("c:main");
            reg.apply_delta([other.as_str()]);
            assert_eq!(reg.assigned_graph_id(&other), Some(CONFIG_GRAPH_ID));
        }
        assert!(!GraphRegistry::new_for_ledger("b:main").has_config_in_user_slot());
    }

    #[test]
    fn test_system_graph_id_constants() {
        assert_eq!(TXN_META_GRAPH_ID, 1);
        assert_eq!(CONFIG_GRAPH_ID, 2);
        assert_eq!(FIRST_USER_GRAPH_ID, 3);
    }
}
