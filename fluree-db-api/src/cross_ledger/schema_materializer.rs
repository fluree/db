//! Cross-ledger schema/ontology materialization.
//!
//! Reads M's schema graph at `resolved_t` and projects the
//! whitelisted ontology axiom triples (rdfs:subClassOf,
//! owl:equivalentClass, owl:imports, rdf:type for owl:Class /
//! owl:ObjectProperty / etc., and the rest of the schema-bundle
//! whitelist) into a term-neutral [`SchemaArtifactWire`]. The
//! translator on D encodes each IRI against D's snapshot and
//! builds a `SchemaBundleFlakes` for the reasoner.
//!
//! Phase 1b-a scope: single graph only. The `owl:imports` triples
//! are projected (so a future reader can see what imports M
//! declared) but the resolver does NOT recursively walk them
//! across ledgers. Transitive cross-ledger imports land in a
//! follow-up.

use super::types::{SchemaArtifactWire, WireOrigin, WireTriple};
use super::CrossLedgerError;
use crate::Fluree;
use fluree_db_core::{
    is_rdf_type, is_schema_class, is_schema_predicate, FlakeValue, IndexType, LedgerSnapshot,
    RangeMatch, RangeOptions, RangeTest, Sid,
};

/// Materialize the schema graph at `graph_iri` in model ledger `M`
/// at `resolved_t` into a `SchemaArtifactWire`.
pub(super) async fn materialize_schema(
    canonical_model_ledger_id: &str,
    graph_iri: &str,
    resolved_t: i64,
    fluree: &Fluree,
) -> Result<SchemaArtifactWire, CrossLedgerError> {
    use fluree_vocab::{owl, rdf, rdfs};

    // 1. Open M at resolved_t.
    let m_db = fluree
        .db_at_t(canonical_model_ledger_id, resolved_t)
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!(
                "failed to open model ledger snapshot at t={resolved_t}: {e}"
            ),
        })?;

    // 2. Resolve graph_iri → g_id in M's graph registry.
    let g_id = m_db
        .snapshot
        .graph_registry
        .graph_id_for_iri(graph_iri)
        .ok_or_else(|| CrossLedgerError::GraphMissingAtT {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        })?;

    // 3. Encode the whitelist IRIs against M's namespace map. Same
    //    whitelist as fluree_db_query::schema_bundle::build_schema_bundle_flakes
    //    so cross-ledger and same-ledger see the same axiom subset.
    //    IRIs M's namespace map has never seen are silently skipped
    //    (they contribute nothing to the projection).
    let schema_predicate_iris: &[&str] = &[
        rdfs::SUB_CLASS_OF,
        rdfs::SUB_PROPERTY_OF,
        rdfs::DOMAIN,
        rdfs::RANGE,
        owl::INVERSE_OF,
        owl::EQUIVALENT_CLASS,
        owl::EQUIVALENT_PROPERTY,
        owl::SAME_AS,
        owl::IMPORTS,
    ];
    let schema_class_iris: &[&str] = &[
        owl::CLASS,
        owl::OBJECT_PROPERTY,
        owl::DATATYPE_PROPERTY,
        owl::SYMMETRIC_PROPERTY,
        owl::TRANSITIVE_PROPERTY,
        owl::FUNCTIONAL_PROPERTY,
        owl::INVERSE_FUNCTIONAL_PROPERTY,
        owl::ONTOLOGY,
        rdf::PROPERTY,
    ];

    let schema_predicates: Vec<Sid> = schema_predicate_iris
        .iter()
        .filter_map(|iri| m_db.snapshot.encode_iri(iri))
        .collect();
    let schema_classes: Vec<Sid> = schema_class_iris
        .iter()
        .filter_map(|iri| m_db.snapshot.encode_iri(iri))
        .collect();
    let rdf_type_sid = m_db.snapshot.encode_iri(rdf::TYPE);

    // 4. Build M's view at the requested graph + t.
    let m_view = fluree_db_core::GraphDbRef::new(
        &m_db.snapshot,
        g_id,
        m_db.overlay.as_ref(),
        m_db.t,
    );
    let opts = RangeOptions::default().with_to_t(m_db.t);

    // 5. Per-predicate PSOT scans for hierarchy / OWL axioms; per-
    //    class OPST scans for declarations like `?p a owl:Class`.
    //    For each matching flake, decode its s/p/o Sids to IRIs and
    //    push a WireTriple. A Sid that fails to decode surfaces as
    //    TranslationFailed — dropping the axiom would weaken the
    //    schema observably.
    let mut triples: Vec<WireTriple> = Vec::new();

    for p_sid in &schema_predicates {
        let flakes = fluree_db_core::range_with_overlay(
            &m_db.snapshot,
            g_id,
            m_db.overlay.as_ref(),
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch::predicate(p_sid.clone()),
            opts.clone(),
        )
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("schema predicate scan failed: {e}"),
        })?;
        for f in flakes.into_iter().filter(|f| f.op) {
            if !is_schema_predicate(&f.p) {
                continue; // defense in depth
            }
            push_ref_triple(
                &mut triples,
                &m_db.snapshot,
                &f.s,
                &f.p,
                &f.o,
                canonical_model_ledger_id,
                graph_iri,
            )?;
        }
    }

    if let Some(ref rdf_type) = rdf_type_sid {
        for cls in &schema_classes {
            let flakes = fluree_db_core::range_with_overlay(
                &m_db.snapshot,
                g_id,
                m_db.overlay.as_ref(),
                IndexType::Opst,
                RangeTest::Eq,
                RangeMatch {
                    p: Some(rdf_type.clone()),
                    o: Some(FlakeValue::Ref(cls.clone())),
                    ..Default::default()
                },
                opts.clone(),
            )
            .await
            .map_err(|e| CrossLedgerError::TranslationFailed {
                ledger_id: canonical_model_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                detail: format!("schema class rdf:type scan failed: {e}"),
            })?;
            for f in flakes.into_iter().filter(|f| f.op) {
                if !is_rdf_type(&f.p) {
                    continue;
                }
                let FlakeValue::Ref(ref obj) = f.o else { continue };
                if !is_schema_class(obj) {
                    continue;
                }
                push_ref_triple(
                    &mut triples,
                    &m_db.snapshot,
                    &f.s,
                    &f.p,
                    &f.o,
                    canonical_model_ledger_id,
                    graph_iri,
                )?;
            }
        }
    }

    let _ = m_view; // ensures the GraphDbRef binding is consumed; the
                   // per-predicate scans hit range_with_overlay directly
                   // for parity with build_schema_bundle_flakes.

    Ok(SchemaArtifactWire {
        origin: WireOrigin {
            model_ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        },
        triples,
    })
}

/// Decode a (s, p, o-as-Ref) flake into a `WireTriple` of IRIs and
/// append to `out`. Returns `TranslationFailed` if any of the three
/// Sids can't be decoded — losing a schema axiom would weaken
/// reasoning observably, so silent drop is wrong here even though
/// it's accepted at the encode-side (unseen IRIs on M's namespace
/// map are filtered earlier by `encode_iri` returning `None`).
fn push_ref_triple(
    out: &mut Vec<WireTriple>,
    snapshot: &LedgerSnapshot,
    s: &Sid,
    p: &Sid,
    o: &FlakeValue,
    canonical_model_ledger_id: &str,
    graph_iri: &str,
) -> Result<(), CrossLedgerError> {
    let FlakeValue::Ref(o_sid) = o else {
        // Schema whitelist is Ref-valued in practice (rdfs:domain,
        // rdfs:range, owl:equivalentClass, owl:imports, etc., all
        // point at classes/properties/ontology resources). A
        // literal-valued axiom in the whitelist is structurally
        // unexpected and silently skipped for Phase 1b-a.
        return Ok(());
    };
    let s_iri = snapshot.decode_sid(s).ok_or_else(|| {
        CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("could not decode schema subject Sid {s:?}"),
        }
    })?;
    let p_iri = snapshot.decode_sid(p).ok_or_else(|| {
        CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("could not decode schema predicate Sid {p:?}"),
        }
    })?;
    let o_iri = snapshot.decode_sid(o_sid).ok_or_else(|| {
        CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("could not decode schema object Sid {o_sid:?}"),
        }
    })?;
    out.push(WireTriple {
        s: s_iri,
        p: p_iri,
        o: o_iri,
    });
    Ok(())
}
