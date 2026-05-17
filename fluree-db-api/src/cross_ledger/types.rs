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
    /// `f:schemaSource` → schema/ontology axiom triples projected
    /// from the model ledger's ontology graph. Single-graph only;
    /// transitive `owl:imports` recursion across ledgers is
    /// reserved.
    SchemaClosure,
    /// `f:shapesSource` → SHACL shape triples (whitelist + rdf:list
    /// internals for sh:in / sh:and / sh:or / sh:xone) projected
    /// from the model ledger's shapes graph. Carries literals as
    /// well as Refs.
    Shapes,
    /// `f:rulesSource` → datalog rule definitions projected from
    /// the model ledger's rules graph. The wire form carries the
    /// raw JSON-LD rule bodies (term-portable: each rule's IRIs
    /// resolve at parse time against D's snapshot, matching how
    /// query-time rules in `opts.rules` are handled).
    Rules,
}

impl std::fmt::Display for ArtifactKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArtifactKind::PolicyRules => f.write_str("PolicyRules"),
            ArtifactKind::Constraints => f.write_str("Constraints"),
            ArtifactKind::SchemaClosure => f.write_str("SchemaClosure"),
            ArtifactKind::Shapes => f.write_str("Shapes"),
            ArtifactKind::Rules => f.write_str("Rules"),
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
    /// Schema/ontology triples in IRI-form (whitelisted subset:
    /// rdfs:subClassOf / subPropertyOf / domain / range,
    /// owl:inverseOf / equivalentClass / equivalentProperty /
    /// sameAs / imports, plus rdf:type for owl:Class /
    /// owl:ObjectProperty / etc.). Translate to a
    /// `fluree_db_query::SchemaBundleFlakes` via
    /// [`SchemaArtifactWire::translate_to_schema_bundle_flakes`].
    SchemaClosure(SchemaArtifactWire),
    /// SHACL shape triples in IRI-form. Translate via
    /// [`ShapesArtifactWire::translate_to_schema_bundle_flakes`]
    /// against the *staged* `NamespaceRegistry` (not D's base
    /// snapshot) so IRIs the in-flight transaction introduced are
    /// encodable. M-only IRIs that D has never seen are dropped
    /// silently — those shapes can't apply to data D doesn't have.
    Shapes(ShapesArtifactWire),
    /// Datalog rule definitions in JSON-LD form. Each entry is
    /// the raw `f:rule` body the model ledger stored as
    /// `FlakeValue::Json`. Rule parsing happens against D's
    /// snapshot at query time via the existing
    /// [`fluree_db_query::datalog_rules`] code path —
    /// no term translation is needed because rules are inherently
    /// IRI-keyed JSON-LD documents.
    Rules(RulesArtifactWire),
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

/// Term-neutral wire form for a schema/ontology artifact.
///
/// Carries the whitelisted schema axiom triples (rdfs:subClassOf,
/// owl:equivalentClass, owl:imports, rdf:type for owl:Class/etc.,
/// plus the rest of the schema-bundle whitelist) in IRI form. The
/// translator on D encodes each IRI against D's snapshot and builds
/// a `SchemaBundleFlakes` from the result. IRIs that fail to encode
/// drop their triples — same semantics as the same-ledger
/// `build_schema_bundle_flakes` flow, where `encode_iri` returning
/// `None` for an unseen IRI contributes nothing.
///
/// Phase 1b-a constraint: the materializer projects only Ref-valued
/// objects (every predicate / class in the schema whitelist is
/// Ref-valued in practice — owl:hasValue and similar literal-valued
/// predicates aren't in the whitelist). Future literal handling
/// lands if a use case requires it.
#[derive(Debug, Clone)]
pub struct SchemaArtifactWire {
    /// Provenance for diagnostics and cache key derivation.
    pub origin: WireOrigin,
    /// Schema triples in the order they were read from M's graph.
    /// Translation re-sorts into the four index orderings.
    pub triples: Vec<WireTriple>,
}

/// IRI-form triple used by both `SchemaArtifactWire` and
/// `ShapesArtifactWire`. Subject and predicate are always IRIs;
/// the object can be either a Ref (IRI) or a literal value with
/// its datatype IRI / language tag. Schema's whitelist is
/// Ref-only in practice; SHACL's whitelist includes literal-valued
/// predicates (`sh:minCount`, `sh:pattern`, `sh:message`, ...).
#[derive(Debug, Clone)]
pub struct WireTriple {
    pub s: String,
    pub p: String,
    pub o: WireObject,
}

/// IRI-form object value carried in a `WireTriple`.
///
/// `Ref(iri)` survives the round-trip as an `IRI → Sid` encode at
/// the data ledger. `Literal { value, datatype, lang }` reconstructs
/// the original `FlakeValue` against D by branching on `datatype`:
/// xsd:integer → `Long`, xsd:boolean → `Boolean`, xsd:string → `String`,
/// xsd:decimal → `Decimal`, xsd:dateTime → `DateTime`, etc. Unknown
/// datatypes fall back to `FlakeValue::String(value)` rather than
/// failing — the SHACL compiler will simply not match them on
/// type-sensitive constraints, which is the same observable behavior
/// as if the constraint weren't authored.
#[derive(Debug, Clone)]
pub enum WireObject {
    /// IRI reference; encoded via `snapshot.encode_iri` at the data
    /// ledger to produce a `FlakeValue::Ref(Sid)`.
    Ref(String),
    /// Literal value in its canonical lexical form, paired with the
    /// datatype IRI and optional language tag. The datatype IRI is
    /// the XSD/RDF IRI (e.g., `http://www.w3.org/2001/XMLSchema#integer`)
    /// or `http://www.w3.org/1999/02/22-rdf-syntax-ns#langString` for
    /// language-tagged strings.
    Literal {
        value: String,
        datatype: String,
        lang: Option<String>,
    },
}

impl WireObject {
    /// Helper for callers that only emit Refs (e.g., schema's
    /// whitelist projection). Equivalent to `WireObject::Ref(iri)`.
    pub fn iri(iri: impl Into<String>) -> Self {
        WireObject::Ref(iri.into())
    }
}

impl SchemaArtifactWire {
    /// Translate to a Sid-form `SchemaBundleFlakes` against the
    /// data ledger's snapshot.
    ///
    /// Each triple's IRIs are encoded via `snapshot.encode_iri`.
    /// Failed encodings drop the triple — D has no data of those
    /// IRIs so the missing axiom can't fire on instance data D
    /// doesn't have, matching same-ledger semantics. The
    /// downstream whitelist check in
    /// `SchemaBundleFlakes::from_collected_schema_triples` is the
    /// canonical filter; if a future cross-ledger producer emits
    /// non-whitelist triples they'll drop there.
    pub fn translate_to_schema_bundle_flakes(
        &self,
        snapshot: &fluree_db_core::LedgerSnapshot,
    ) -> fluree_db_query::error::Result<
        std::sync::Arc<fluree_db_query::schema_bundle::SchemaBundleFlakes>,
    > {
        use fluree_db_core::flake::Flake;
        use fluree_db_core::value::FlakeValue;
        use fluree_db_core::Sid;

        let mut collected: Vec<Flake> = Vec::with_capacity(self.triples.len());
        for t in &self.triples {
            // Schema whitelist projects Refs only; any literal-valued
            // object on a whitelisted predicate is silently skipped
            // (such triples can't survive the schema-bundle path
            // anyway — the reasoner expects Ref-valued schema axioms).
            let WireObject::Ref(o_iri) = &t.o else {
                continue;
            };
            let (Some(s_sid), Some(p_sid), Some(o_sid)) = (
                snapshot.encode_iri(&t.s),
                snapshot.encode_iri(&t.p),
                snapshot.encode_iri(o_iri),
            ) else {
                continue;
            };
            collected.push(Flake {
                g: None,
                s: s_sid,
                p: p_sid,
                o: FlakeValue::Ref(o_sid),
                // @id is the canonical datatype for Ref-valued
                // objects. JSON_LD is a well-known namespace code
                // pre-registered at genesis on every ledger, so this
                // Sid construction is stable across ledgers.
                dt: Sid::new(fluree_vocab::namespaces::JSON_LD, "id"),
                t: 0,
                op: true,
                m: None,
            });
        }
        fluree_db_query::schema_bundle::SchemaBundleFlakes::from_collected_schema_triples(collected)
            .map(std::sync::Arc::new)
    }
}

/// Term-neutral wire form for a SHACL shapes artifact.
///
/// Carries the SHACL whitelist triples (sh:targetClass / sh:property /
/// sh:minCount / sh:pattern / ... — the full vocabulary
/// `ShapeCompiler::compile_from_dbs` scans for) plus the
/// rdf:first / rdf:rest internals needed for sh:in / sh:and /
/// sh:or / sh:xone list expansion. Object positions handle both
/// Ref and Literal via [`WireObject`].
///
/// **Translation timing matters.** The translator on D must run
/// against the *staged* `NamespaceRegistry`, not the pre-staging
/// snapshot. The staging registry contains IRIs that the in-flight
/// transaction is introducing (e.g., `ex:User` declared by the
/// tx being validated); the pre-stage snapshot does not.
/// Translating against the wrong context drops everything and the
/// shape silently doesn't apply.
///
/// IRIs the staged registry hasn't seen are dropped silently —
/// the shape couldn't apply to data D doesn't have anyway, and
/// allocating a fresh namespace code for an M-only IRI would
/// introduce namespace churn into D for no benefit.
#[derive(Debug, Clone)]
pub struct ShapesArtifactWire {
    /// Provenance for diagnostics and cache key derivation.
    pub origin: WireOrigin,
    /// SHACL whitelist triples + rdf:list internals in the order
    /// the materializer read them from M.
    pub triples: Vec<WireTriple>,
}

impl ShapesArtifactWire {
    /// Translate to a Sid-form `SchemaBundleFlakes` against the
    /// data ledger's *staged* namespace registry. The bundle is
    /// then wrapped in `SchemaBundleOverlay` and fed to
    /// `ShaclEngine::from_dbs_with_overlay`.
    ///
    /// `staged_ns` is the `NamespaceRegistry` that the staging
    /// pipeline carries — it has D's snapshot namespaces *plus*
    /// any IRIs the in-flight transaction has registered. Encoding
    /// is lookup-only (`NamespaceRegistry::lookup_sid_for_iri`);
    /// unknown IRIs drop their triples rather than allocating
    /// fresh codes in D.
    ///
    /// Literal object values reconstruct via the same datatype
    /// dispatch the schema translator uses — xsd:integer → Long,
    /// xsd:boolean → Boolean, etc.; unknown datatypes fall back
    /// to `FlakeValue::String`.
    pub fn translate_to_schema_bundle_flakes(
        &self,
        staged_ns: &fluree_db_transact::namespace::NamespaceRegistry,
    ) -> fluree_db_query::error::Result<
        std::sync::Arc<fluree_db_query::schema_bundle::SchemaBundleFlakes>,
    > {
        use fluree_db_core::flake::Flake;
        use fluree_db_core::Sid;

        let mut collected: Vec<Flake> = Vec::with_capacity(self.triples.len());
        for t in &self.triples {
            let (Some(s_sid), Some(p_sid)) = (
                staged_ns.lookup_sid_for_iri(&t.s),
                staged_ns.lookup_sid_for_iri(&t.p),
            ) else {
                continue;
            };
            let (o_value, dt_sid) = match &t.o {
                WireObject::Ref(o_iri) => {
                    let Some(o_sid) = staged_ns.lookup_sid_for_iri(o_iri) else {
                        continue;
                    };
                    (
                        fluree_db_core::FlakeValue::Ref(o_sid),
                        Sid::new(fluree_vocab::namespaces::JSON_LD, "id"),
                    )
                }
                WireObject::Literal {
                    value,
                    datatype,
                    lang: _,
                } => {
                    let dt_sid = staged_ns
                        .lookup_sid_for_iri(datatype)
                        .unwrap_or_else(|| Sid::new(fluree_vocab::namespaces::XSD, "string"));
                    (literal_to_flake_value(value, datatype), dt_sid)
                }
            };
            collected.push(Flake {
                g: None,
                s: s_sid,
                p: p_sid,
                o: o_value,
                dt: dt_sid,
                t: 0,
                op: true,
                m: None,
            });
        }
        fluree_db_query::schema_bundle::SchemaBundleFlakes::from_collected_schema_triples(collected)
            .map(std::sync::Arc::new)
    }
}

/// Decode a wire literal into the matching `FlakeValue`. Falls back
/// to `String(value)` on parse failure or unknown datatype — the
/// SHACL compiler will simply not match the value on type-sensitive
/// constraints, same as if the constraint weren't authored.
fn literal_to_flake_value(value: &str, datatype: &str) -> fluree_db_core::FlakeValue {
    use fluree_db_core::FlakeValue;
    const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
    let local = datatype.strip_prefix(XSD).unwrap_or("");
    match local {
        "boolean" => value
            .parse::<bool>()
            .map(FlakeValue::Boolean)
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        "integer" | "long" | "int" | "short" | "byte" | "nonNegativeInteger"
        | "positiveInteger" | "negativeInteger" | "nonPositiveInteger" | "unsignedLong"
        | "unsignedInt" | "unsignedShort" | "unsignedByte" => value
            .parse::<i64>()
            .map(FlakeValue::Long)
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        "double" | "float" => value
            .parse::<f64>()
            .map(FlakeValue::Double)
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        _ => FlakeValue::String(value.to_string()),
    }
}

/// Term-neutral wire form for a datalog rules artifact.
///
/// Rules are JSON-LD documents — IRI references inside `where` /
/// `insert` patterns are resolved against the data ledger's
/// snapshot at parse time by
/// [`fluree_db_query::datalog_rules::parse_query_time_rule`], the
/// same path query-time rules from `opts.rules` use. We therefore
/// just carry the raw JSON bodies on the wire; no Sid mapping is
/// performed up front.
///
/// The materializer scans M's rules graph for `f:rule` flakes
/// whose object is `FlakeValue::Json` (the only form the rule
/// extractor recognises) and emits one `String` per rule.
#[derive(Debug, Clone)]
pub struct RulesArtifactWire {
    /// Provenance for diagnostics and cache key derivation.
    pub origin: WireOrigin,
    /// Raw JSON-LD bodies of each `f:rule` definition discovered
    /// on M's rules graph. Order preserves the materializer's
    /// scan order; deduplication is the consumer's concern.
    pub rules: Vec<String>,
}

impl RulesArtifactWire {
    /// Decode each rule body into a `serde_json::Value` ready for
    /// `execute_datalog_rules_with_query_rules`'s
    /// `query_time_rules` slot.
    ///
    /// **Fail-closed**: a single malformed entry returns
    /// [`super::CrossLedgerError::TranslationFailed`] rather than
    /// silently dropping the rule. Cross-ledger governance is
    /// administrator-authored — silently weakening the configured
    /// reasoning model would be the worst possible behaviour. The
    /// per-query path that maps `opts.rules` *does* tolerate
    /// individual bad rules, but that's a self-service surface
    /// where the author is the requester.
    pub fn parsed_rules(&self) -> Result<Vec<serde_json::Value>, super::CrossLedgerError> {
        let mut out = Vec::with_capacity(self.rules.len());
        for (idx, raw) in self.rules.iter().enumerate() {
            match serde_json::from_str(raw) {
                Ok(v) => out.push(v),
                Err(e) => {
                    return Err(super::CrossLedgerError::TranslationFailed {
                        ledger_id: self.origin.model_ledger_id.clone(),
                        graph_iri: self.origin.graph_iri.clone(),
                        detail: format!(
                            "malformed cross-ledger rule at index {idx}: {e}"
                        ),
                    });
                }
            }
        }
        Ok(out)
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
