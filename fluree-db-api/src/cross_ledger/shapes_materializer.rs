//! Cross-ledger SHACL shapes materialization.
//!
//! Reads M's shapes graph at `resolved_t` and projects the SHACL
//! vocabulary triples (the same whitelist `ShapeCompiler` uses
//! same-ledger) plus the `rdf:first` / `rdf:rest` internals used by
//! `sh:in` / `sh:and` / `sh:or` / `sh:xone` list expansion. Object
//! positions handle both `Ref` (sh:targetClass, sh:path, sh:class,
//! sh:datatype, ...) and `Literal` (sh:minCount, sh:pattern,
//! sh:message, ...) via [`WireObject`].
//!
//! The translation step on the data ledger side is in
//! `ShapesArtifactWire::translate_to_schema_bundle_flakes` and
//! must use the *staged* `NamespaceRegistry`, not D's pre-staging
//! snapshot. See that method's docs for why.

use super::types::{ShapesArtifactWire, WireObject, WireOrigin, WireTriple};
use super::CrossLedgerError;
use crate::Fluree;
use fluree_db_core::{
    FlakeValue, IndexType, LedgerSnapshot, RangeMatch, RangeOptions, RangeTest, Sid,
};

const SHACL: &str = "http://www.w3.org/ns/shacl#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

pub(super) async fn materialize_shapes(
    canonical_model_ledger_id: &str,
    graph_iri: &str,
    resolved_t: i64,
    fluree: &Fluree,
) -> Result<ShapesArtifactWire, CrossLedgerError> {
    let m_db = fluree
        .db_at_t(canonical_model_ledger_id, resolved_t)
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("failed to open model ledger snapshot at t={resolved_t}: {e}"),
        })?;

    let g_id = super::resolve_selector_g_id(&m_db.snapshot, graph_iri)?.ok_or_else(|| {
        CrossLedgerError::GraphMissingAtT {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        }
    })?;

    // SHACL whitelist — mirrors fluree_db_shacl::ShapeCompiler::compile_from_dbs
    let shacl_predicate_names: &[&str] = &[
        "targetClass",
        "targetNode",
        "targetSubjectsOf",
        "targetObjectsOf",
        "property",
        "path",
        "minCount",
        "maxCount",
        "datatype",
        "nodeKind",
        "class",
        "minInclusive",
        "maxInclusive",
        "minExclusive",
        "maxExclusive",
        "pattern",
        "flags",
        "minLength",
        "maxLength",
        "hasValue",
        "in",
        "equals",
        "disjoint",
        "lessThan",
        "lessThanOrEquals",
        "closed",
        "ignoredProperties",
        "uniqueLang",
        "languageIn",
        "not",
        "and",
        "or",
        "xone",
        "severity",
        "message",
        "name",
    ];

    let mut shacl_predicate_sids: Vec<Sid> = Vec::new();
    for name in shacl_predicate_names {
        if let Some(sid) = m_db.snapshot.encode_iri(&format!("{SHACL}{name}")) {
            shacl_predicate_sids.push(sid);
        }
    }
    let rdf_first_sid = m_db.snapshot.encode_iri(&format!("{RDF}first"));
    let rdf_rest_sid = m_db.snapshot.encode_iri(&format!("{RDF}rest"));

    let opts = RangeOptions::default().with_to_t(m_db.t);
    let mut triples: Vec<WireTriple> = Vec::new();

    for p_sid in &shacl_predicate_sids {
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
            detail: format!("SHACL predicate scan failed: {e}"),
        })?;
        for f in flakes.into_iter().filter(|f| f.op) {
            push_triple(
                &mut triples,
                &m_db.snapshot,
                &f,
                canonical_model_ledger_id,
                graph_iri,
            )?;
        }
    }

    for opt_sid in [rdf_first_sid, rdf_rest_sid]
        .iter()
        .filter_map(|s| s.as_ref())
    {
        let flakes = fluree_db_core::range_with_overlay(
            &m_db.snapshot,
            g_id,
            m_db.overlay.as_ref(),
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch::predicate(opt_sid.clone()),
            opts.clone(),
        )
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("rdf:first/rdf:rest scan failed: {e}"),
        })?;
        for f in flakes.into_iter().filter(|f| f.op) {
            push_triple(
                &mut triples,
                &m_db.snapshot,
                &f,
                canonical_model_ledger_id,
                graph_iri,
            )?;
        }
    }

    Ok(ShapesArtifactWire {
        origin: WireOrigin {
            model_ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        },
        triples,
    })
}

fn push_triple(
    out: &mut Vec<WireTriple>,
    snapshot: &LedgerSnapshot,
    f: &fluree_db_core::flake::Flake,
    canonical_model_ledger_id: &str,
    graph_iri: &str,
) -> Result<(), CrossLedgerError> {
    let s_iri = snapshot
        .decode_sid(&f.s)
        .ok_or_else(|| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("could not decode shape subject Sid {:?}", f.s),
        })?;
    let p_iri = snapshot
        .decode_sid(&f.p)
        .ok_or_else(|| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("could not decode shape predicate Sid {:?}", f.p),
        })?;
    let lang = f.m.as_ref().and_then(|m| m.lang.clone());
    let o = encode_object(
        &f.o,
        &f.dt,
        lang,
        snapshot,
        canonical_model_ledger_id,
        graph_iri,
    )?;
    out.push(WireTriple {
        s: s_iri,
        p: p_iri,
        o,
    });
    Ok(())
}

fn encode_object(
    o: &FlakeValue,
    dt: &Sid,
    lang: Option<String>,
    snapshot: &LedgerSnapshot,
    canonical_model_ledger_id: &str,
    graph_iri: &str,
) -> Result<WireObject, CrossLedgerError> {
    if let FlakeValue::Ref(o_sid) = o {
        let iri =
            snapshot
                .decode_sid(o_sid)
                .ok_or_else(|| CrossLedgerError::TranslationFailed {
                    ledger_id: canonical_model_ledger_id.to_string(),
                    graph_iri: graph_iri.to_string(),
                    detail: format!("could not decode shape object Sid {o_sid:?}"),
                })?;
        return Ok(WireObject::Ref(iri));
    }
    // Fail-closed on datatype decode. M's snapshot owns the dt Sid
    // it stored; if `decode_sid` returns None the snapshot is
    // corrupt or the dt Sid is otherwise unresolvable — silently
    // coercing every such literal to `xsd:string` would change
    // the shape's `sh:datatype` semantics (a `sh:datatype
    // xsd:integer` would collapse into a string-typed value that
    // matches differently on D).
    let datatype_iri =
        snapshot
            .decode_sid(dt)
            .ok_or_else(|| CrossLedgerError::TranslationFailed {
                ledger_id: canonical_model_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                detail: format!("could not decode shape literal datatype Sid {dt:?}"),
            })?;
    let value = flake_value_to_lexical(o, canonical_model_ledger_id, graph_iri)?;
    Ok(WireObject::Literal {
        value,
        datatype: datatype_iri,
        lang,
    })
}

/// Render a [`FlakeValue`] into its canonical xsd lexical form for
/// the wire. Every variant has an explicit case so the wire can be
/// round-tripped on D without fidelity loss. Variants without a
/// canonical lexical form for SHACL purposes (`Vector`, `GeoPoint`)
/// surface as `TranslationFailed` — `sh:hasValue` on a non-XSD
/// custom Fluree datatype is out of scope for cross-ledger shapes
/// and must be flagged rather than fall through to a
/// `Debug`-formatted lossy string.
fn flake_value_to_lexical(
    o: &FlakeValue,
    canonical_model_ledger_id: &str,
    graph_iri: &str,
) -> Result<String, CrossLedgerError> {
    Ok(match o {
        FlakeValue::String(s) => s.clone(),
        FlakeValue::Boolean(b) => b.to_string(),
        FlakeValue::Long(n) => n.to_string(),
        FlakeValue::Double(f) => f.to_string(),
        FlakeValue::BigInt(b) => b.to_string(),
        FlakeValue::Decimal(d) => d.to_string(),
        FlakeValue::DateTime(v) => v.to_string(),
        FlakeValue::Date(v) => v.to_string(),
        FlakeValue::Time(v) => v.to_string(),
        FlakeValue::GYear(v) => v.to_string(),
        FlakeValue::GYearMonth(v) => v.to_string(),
        FlakeValue::GMonth(v) => v.to_string(),
        FlakeValue::GDay(v) => v.to_string(),
        FlakeValue::GMonthDay(v) => v.to_string(),
        FlakeValue::YearMonthDuration(v) => v.to_string(),
        FlakeValue::DayTimeDuration(v) => v.to_string(),
        FlakeValue::Duration(v) => v.to_string(),
        FlakeValue::Json(s) => s.clone(),
        FlakeValue::Null => String::new(),
        FlakeValue::Ref(_) => unreachable!("Ref handled at encode_object"),
        FlakeValue::Vector(_) | FlakeValue::GeoPoint(_) => {
            return Err(CrossLedgerError::TranslationFailed {
                ledger_id: canonical_model_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                detail: format!(
                    "shape literal uses a Fluree-specific datatype \
                     (`{}`) that has no canonical xsd lexical form for \
                     cross-ledger transport",
                    match o {
                        FlakeValue::Vector(_) => "Vector",
                        FlakeValue::GeoPoint(_) => "GeoPoint",
                        _ => unreachable!(),
                    }
                ),
            });
        }
    })
}
