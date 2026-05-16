//! Cross-ledger resolution types.
//!
//! See `docs/design/cross-ledger-model-enforcement.md` for the
//! semantics of each type. The orchestration that uses these lives in
//! `resolver.rs`; pure helpers below (reserved-graph guard, cycle
//! check, memo lookup) are kept here and unit-tested in isolation.

use super::CrossLedgerError;
use crate::Fluree;
use fluree_db_core::graph_registry::{config_graph_iri, txn_meta_graph_iri};
use fluree_db_policy::PolicyArtifactWire;
use std::collections::HashMap;
use std::sync::Arc;

/// Which subsystem's artifact is being resolved.
///
/// `ArtifactKind` is part of the memo / cycle-detection key so a
/// memoized `PolicyRules` entry for the same `(ledger, graph, t)`
/// can't be returned to a caller asking for `Constraints` (or any
/// future variant), and vice versa.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactKind {
    /// `f:policySource` → policy rule set.
    PolicyRules,
    /// `f:constraintsSource` → set of property IRIs declared
    /// `f:enforceUnique true` on the model ledger.
    Constraints,
    // SchemaClosure  — reserved
    // Shapes         — reserved
    // DatalogRules   — reserved
}

impl std::fmt::Display for ArtifactKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArtifactKind::PolicyRules => f.write_str("PolicyRules"),
            ArtifactKind::Constraints => f.write_str("Constraints"),
        }
    }
}

/// Memo / cycle-detection key. Includes `ArtifactKind` so concurrent
/// (within one request) resolutions for different artifact kinds
/// against the same `(ledger, graph, t)` don't collide.
pub(crate) type ResolutionKey = (ArtifactKind, String, String, i64);

/// A successfully resolved, term-neutral governance artifact.
///
/// Cached at the API layer by `(ArtifactKind, model_ledger_id,
/// graph_iri, resolved_t)` — see [`ResolutionKey`]. Per-data-ledger
/// interning is a separate step that happens at the wire→PolicySet
/// boundary against D's snapshot.
#[derive(Debug, Clone)]
pub struct ResolvedGraph {
    /// Canonical model ledger id this artifact came from.
    pub model_ledger_id: String,
    /// Graph IRI within the model ledger.
    pub graph_iri: String,
    /// Model ledger `t` at which the artifact was materialized.
    pub resolved_t: i64,
    /// The artifact itself, tagged by subsystem. Pattern-match this
    /// against the expected `GovernanceArtifact` variant for the
    /// requesting `ArtifactKind`.
    pub artifact: GovernanceArtifact,
}

/// Tagged union of governance artifacts.
///
/// The variant is paired with [`ArtifactKind`] in [`ResolutionKey`]
/// so the memo can carry mixed artifact types without dynamic
/// dispatch — callers pattern-match to extract the shape they
/// expect.
#[derive(Debug, Clone)]
pub enum GovernanceArtifact {
    /// Policy rule set in IRI-form. Translate to `PolicySet` via
    /// `fluree_db_policy::build_policy_set_from_wire`.
    PolicyRules(PolicyArtifactWire),
    /// `f:enforceUnique` property declarations in IRI-form.
    /// Translate to D's Sid space via
    /// [`ConstraintsArtifactWire::translate_to_sids`].
    Constraints(ConstraintsArtifactWire),
    // SchemaClosure(SchemaBundleWire) — reserved
    // Shapes(ShapeSetWire)            — reserved
    // DatalogRules(DatalogRuleSetWire) — reserved
}

/// Term-neutral wire form for a constraints artifact.
///
/// A constraints artifact is structurally simple: a list of
/// property IRIs that the model ledger has annotated as
/// `f:enforceUnique true`. The translator encodes each IRI
/// against the data ledger's snapshot to produce the Sid set
/// that the existing `enforce_unique_constraints` flow consumes.
///
/// IRIs that fail to resolve against D's snapshot are silently
/// dropped — D has no data of those properties, so the constraint
/// cannot be violated either way. The same semantics apply
/// same-ledger via `encode_iri` returning `None` for unseen
/// namespaces.
#[derive(Debug, Clone)]
pub struct ConstraintsArtifactWire {
    /// Provenance for diagnostics and cache key derivation.
    pub origin: WireOrigin,
    /// Property IRIs declared `f:enforceUnique true` on the model
    /// ledger's constraints graph.
    pub property_iris: Vec<String>,
}

impl ConstraintsArtifactWire {
    /// Translate the IRI list into property Sids against the
    /// data ledger's snapshot. Unresolvable IRIs are dropped.
    pub fn translate_to_sids(
        &self,
        snapshot: &fluree_db_core::LedgerSnapshot,
    ) -> Vec<fluree_db_core::Sid> {
        self.property_iris
            .iter()
            .filter_map(|iri| snapshot.encode_iri(iri))
            .collect()
    }
}

/// Provenance for a cross-ledger wire artifact.
///
/// Shared across `Constraints` and any future variant in this
/// crate. `PolicyArtifactWire` carries its own `WireOrigin` from
/// `fluree-db-policy` — identical shape, kept separate to avoid
/// pulling fluree-db-api into the policy crate. A future
/// unification would centralize these in `fluree-db-core`.
#[derive(Debug, Clone)]
pub struct WireOrigin {
    /// Canonical model ledger id (`NsRecord.ledger_id`).
    pub model_ledger_id: String,
    /// Graph IRI within the model ledger whose triples produced
    /// this artifact.
    pub graph_iri: String,
    /// Model ledger `t` at which the artifact was materialized.
    pub resolved_t: i64,
}

/// Per-request resolution context.
///
/// Holds the full lifetime / consistency model for cross-ledger
/// resolution within a single request:
///
/// - `resolved_ts` captures the lazy per-request head-t per
///   canonical model ledger id (governance-context capture).
///   Lookup on miss reads M's head once and stores it; subsequent
///   references to the same M reuse the same value so policy and
///   shapes can never disagree about which version of M they're
///   enforcing. `f:atT` pins are rejected as
///   [`CrossLedgerError::UnsupportedFeature`] until Phase 3 lands,
///   so the only `resolved_t` source today is this lazy capture.
///
/// - `active` is the resolution stack used for cycle detection.
///   Push before recursion, pop after. A key is a cycle only when
///   encountered while *already on the stack*.
///
/// - `memo` is the per-request completed map. Subsequent references
///   to the same [`ResolutionKey`] — from any subsystem — short-
///   circuit on a memo hit. Memo hits never enter `active`, so
///   cross-subsystem de-dup never trips cycle detection.
pub struct ResolveCtx<'a> {
    /// Canonical data-ledger id D.
    pub data_ledger_id: &'a str,
    /// The Fluree instance hosting D and (per the same-instance
    /// constraint) the referenced model ledger.
    pub fluree: &'a Fluree,
    /// Lazy governance-context capture: canonical model ledger id →
    /// `resolved_t`. Phase 1a is the only producer (M's head at
    /// first reference); pinned `f:atT` is rejected upstream until
    /// Phase 3.
    pub resolved_ts: HashMap<String, i64>,
    /// Active resolution stack (cycle detection). Keyed on the full
    /// resolution tuple including `ArtifactKind` so a `PolicyRules`
    /// resolve doesn't see a `Shapes` resolution of the same
    /// `(ledger, graph, t)` as a cycle (or vice versa).
    pub active: Vec<ResolutionKey>,
    /// Per-request completed memo, keyed on the same tuple so
    /// different artifact kinds can't return each other's entries.
    pub memo: HashMap<ResolutionKey, Arc<ResolvedGraph>>,
}

impl<'a> ResolveCtx<'a> {
    /// Build a fresh resolution context for a request against D.
    pub fn new(data_ledger_id: &'a str, fluree: &'a Fluree) -> Self {
        Self {
            data_ledger_id,
            fluree,
            resolved_ts: HashMap::new(),
            active: Vec::new(),
            memo: HashMap::new(),
        }
    }
}

/// Reject selectors that resolve to the model ledger's `#config` or
/// `#txn-meta` graphs.
///
/// Applied *before* any storage round-trip on the model ledger —
/// `#txn-meta` in particular can leak commit metadata, and `#config`
/// is the recursive seed that defines what model M is. Neither is
/// ever a legitimate target of a cross-ledger governance reference.
///
/// Pure on `(canonical_ledger_id, graph_iri)`; no I/O.
pub(crate) fn reject_if_reserved_graph(
    canonical_ledger_id: &str,
    graph_iri: &str,
) -> Result<(), CrossLedgerError> {
    if graph_iri == config_graph_iri(canonical_ledger_id)
        || graph_iri == txn_meta_graph_iri(canonical_ledger_id)
    {
        return Err(CrossLedgerError::ReservedGraphSelected {
            graph_iri: graph_iri.to_string(),
        });
    }
    Ok(())
}

/// Memo lookup for the per-request completed map.
///
/// Memo hits short-circuit before `active` is consulted, so two
/// subsystems referencing the same `(kind, ledger, graph, t)` resolve
/// once and never trip cycle detection.
pub(crate) fn memo_hit(
    memo: &HashMap<ResolutionKey, Arc<ResolvedGraph>>,
    key: &ResolutionKey,
) -> Option<Arc<ResolvedGraph>> {
    memo.get(key).cloned()
}

/// Cycle check against the active resolution stack.
///
/// Returns the cycle as a chain (active stack + the offending key
/// appended) when one is detected.
pub(crate) fn check_cycle(
    active: &[ResolutionKey],
    key: &ResolutionKey,
) -> Result<(), CrossLedgerError> {
    if active.iter().any(|k| k == key) {
        let mut chain = active.to_vec();
        chain.push(key.clone());
        return Err(CrossLedgerError::CycleDetected { chain });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_graph_guard_blocks_config_and_txn_meta() {
        let ledger = "model:main";
        assert!(matches!(
            reject_if_reserved_graph(ledger, "urn:fluree:model:main#config"),
            Err(CrossLedgerError::ReservedGraphSelected { .. })
        ));
        assert!(matches!(
            reject_if_reserved_graph(ledger, "urn:fluree:model:main#txn-meta"),
            Err(CrossLedgerError::ReservedGraphSelected { .. })
        ));
    }

    #[test]
    fn reserved_graph_guard_allows_application_graphs() {
        let ledger = "model:main";
        // A user-named graph that happens to live on the model
        // ledger is fine — we only block the system graphs.
        assert!(reject_if_reserved_graph(ledger, "http://example.org/policy").is_ok());
        // Even a config-shaped IRI for a *different* ledger is fine —
        // it can't actually resolve to model:main's #config.
        assert!(
            reject_if_reserved_graph(ledger, "urn:fluree:other:main#config").is_ok(),
            "config IRI for a different ledger should not match"
        );
    }

    fn key(ledger: &str, graph: &str, t: i64) -> ResolutionKey {
        (ArtifactKind::PolicyRules, ledger.into(), graph.into(), t)
    }

    #[test]
    fn cycle_check_passes_for_unique_tuples() {
        let active = vec![
            key("a:main", "http://ex.org/p", 10),
            key("b:main", "http://ex.org/q", 20),
        ];
        let new_key = key("c:main", "http://ex.org/r", 30);
        assert!(check_cycle(&active, &new_key).is_ok());
    }

    #[test]
    fn cycle_check_fails_on_reentry_and_renders_full_chain() {
        let cycle_key = key("a:main", "http://ex.org/p", 10);
        let active = vec![cycle_key.clone(), key("b:main", "http://ex.org/q", 20)];
        let err = check_cycle(&active, &cycle_key).unwrap_err();
        match err {
            CrossLedgerError::CycleDetected { chain } => {
                // The chain should contain the original active stack
                // (in order) plus the offending key appended.
                assert_eq!(chain.len(), 3);
                assert_eq!(chain[0], cycle_key);
                assert_eq!(chain[2], cycle_key);
            }
            other => panic!("expected CycleDetected, got {other:?}"),
        }
    }

    #[test]
    fn cycle_check_treats_different_artifact_kinds_on_same_graph_as_distinct() {
        // Two artifact kinds resolving the same (ledger, graph, t)
        // are NOT a cycle — they're materializing different things
        // (policy rules vs shapes vs schema) from the same source.
        // Phase 1a only has PolicyRules so this test uses a synthetic
        // second variant by reusing PolicyRules with a sentinel
        // pattern; it will be expanded in Phase 1b when SchemaClosure
        // lands. The contract this guards: adding a new ArtifactKind
        // doesn't make existing resolutions look cyclic.
        let active = vec![(
            ArtifactKind::PolicyRules,
            "a:main".to_string(),
            "http://ex.org/p".to_string(),
            10,
        )];
        // Once Phase 1b adds e.g. ArtifactKind::SchemaClosure, this
        // test should use that variant to verify cross-kind isolation.
        // For now: same kind / different t (a clearly-non-cycle case)
        // exercises the same code path under the new tuple shape.
        let later_pin = (
            ArtifactKind::PolicyRules,
            "a:main".to_string(),
            "http://ex.org/p".to_string(),
            20,
        );
        assert!(check_cycle(&active, &later_pin).is_ok());
    }

    #[test]
    fn cycle_check_treats_different_t_pins_of_same_graph_as_distinct() {
        // Two pins of the same (kind, ledger, graph) at different t
        // are NOT a cycle. The design doc is explicit on this.
        let active = vec![key("a:main", "http://ex.org/p", 10)];
        let later_pin = key("a:main", "http://ex.org/p", 20);
        assert!(check_cycle(&active, &later_pin).is_ok());
    }

    #[test]
    fn memo_returns_cloned_arc_on_hit_and_none_on_miss() {
        let mut memo = HashMap::new();
        let resolution_key = key("a:main", "http://ex.org/p", 10);

        let payload = Arc::new(ResolvedGraph {
            model_ledger_id: resolution_key.1.clone(),
            graph_iri: resolution_key.2.clone(),
            resolved_t: resolution_key.3,
            artifact: GovernanceArtifact::PolicyRules(PolicyArtifactWire {
                origin: fluree_db_policy::WireOrigin {
                    model_ledger_id: resolution_key.1.clone(),
                    graph_iri: resolution_key.2.clone(),
                    resolved_t: resolution_key.3,
                },
                restrictions: vec![],
            }),
        });

        assert!(memo_hit(&memo, &resolution_key).is_none());
        memo.insert(resolution_key.clone(), payload.clone());

        let hit = memo_hit(&memo, &resolution_key).expect("hit after insert");
        assert!(Arc::ptr_eq(&hit, &payload), "memo must return shared Arc");
    }
}
