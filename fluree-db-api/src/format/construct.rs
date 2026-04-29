//! CONSTRUCT query output formatter
//!
//! Transforms query results into JSON-LD graph format using the Graph IR:
//! 1. Instantiate template patterns with result bindings -> Graph (expanded IRIs)
//! 2. Format Graph to JSON-LD (compacting at output time)
//!
//! The Graph IR stores all IRIs in expanded form. Compaction to prefixed form
//! happens only at the final JSON-LD formatting step.

use super::iri::IriCompactor;
use super::{FormatError, Result};
use crate::QueryResult;
use fluree_db_core::FlakeValue;
use fluree_db_query::binding::Binding;
use fluree_db_query::parse::ConstructTemplate;
use fluree_db_query::triple::{Ref, Term};
use fluree_db_query::Batch;
use fluree_graph_format::{format_jsonld, JsonLdFormatConfig};
use fluree_graph_ir::{BlankId, Datatype, Graph, LiteralValue, Term as IrTerm, Triple};
use fluree_vocab::{geo, rdf, xsd};
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// Format CONSTRUCT query results as JSON-LD graph
///
/// This function:
/// 1. Instantiates the CONSTRUCT template with query bindings to produce a Graph
/// 2. Formats the Graph to JSON-LD using the original @context for compaction
///
/// # Arguments
///
/// * `result` - Query result with construct_template populated
/// * `compactor` - IRI compactor for decoding Sids and compacting output
///
/// # Returns
///
/// JSON-LD graph: `{"@context": ..., "@graph": [...]}`
pub fn format(result: &QueryResult, compactor: &IriCompactor) -> Result<JsonValue> {
    // 1. Build Graph from template instantiation
    let mut graph = instantiate_construct_graph(result, compactor)?;

    // Sort for deterministic output
    graph.sort();

    // 2. Format to JSON-LD using CONSTRUCT parity settings.
    //    Use the precomputed ContextCompactor so we don't rebuild the
    //    reverse lookup for every IRI.
    let ctx_compactor = compactor.ctx_compactor().clone();
    let ctx_compactor_id = ctx_compactor.clone();
    let config = JsonLdFormatConfig::construct_parity(
        result.orig_context.clone(),
        // Predicates and @type values: vocab=true (allow @vocab compaction)
        move |iri| ctx_compactor.compact_vocab(iri),
    )
    // Node identifiers (@id): vocab=false (do NOT compact via @vocab)
    .with_id_compactor(move |iri| ctx_compactor_id.compact_id(iri));

    // CONSTRUCT output singleton wrapping isn't semantically important for us.
    // We keep a single consistent policy (currently: always use arrays).

    Ok(format_jsonld(&graph, &config))
}

/// Instantiate CONSTRUCT template patterns with query bindings.
///
/// Produces a Graph with EXPANDED IRIs (not compact). Compaction/serialization is done
/// at the final output step, not here.
pub(super) fn instantiate_construct_graph(
    result: &QueryResult,
    compactor: &IriCompactor,
) -> Result<Graph> {
    let template = result
        .output
        .construct_template()
        .ok_or_else(|| FormatError::InvalidBinding("CONSTRUCT missing template".into()))?;

    let mut graph = Graph::new();

    for batch in &result.batches {
        for row_idx in 0..batch.len() {
            instantiate_row(result, template, batch, row_idx, compactor, &mut graph)?;
        }
    }

    Ok(graph)
}

/// Process a single result row through the template patterns
fn instantiate_row(
    result: &QueryResult,
    template: &ConstructTemplate,
    batch: &Batch,
    row_idx: usize,
    compactor: &IriCompactor,
    graph: &mut Graph,
) -> Result<()> {
    for pattern in &template.patterns {
        // Resolve template terms with bindings (all IRIs are EXPANDED)
        let subject = resolve_subject_term(result, &pattern.s, batch, row_idx, compactor)?;
        let predicate = resolve_predicate_term(result, &pattern.p, batch, row_idx, compactor)?;
        let object = resolve_object_term(result, &pattern.o, batch, row_idx, compactor)?;

        // Skip if any term is unbound (incomplete triple)
        let (Some(s), Some(p), Some(o)) = (subject, predicate, object) else {
            continue;
        };

        graph.add(Triple::new(s, p, o));
    }

    Ok(())
}

/// Resolve a Ref to an IR Term for subject position
///
/// Subjects must be IRIs or blank nodes, never literals.
/// Returns expanded IRI (via decode_sid, not compact_sid).
fn resolve_subject_term(
    result: &QueryResult,
    term: &Ref,
    batch: &Batch,
    row_idx: usize,
    compactor: &IriCompactor,
) -> Result<Option<IrTerm>> {
    match term {
        Ref::Var(var_id) => match batch.get(row_idx, *var_id) {
            Some(binding) => {
                let materialized;
                let binding = if binding.is_encoded() {
                    materialized = super::materialize::materialize_binding(result, binding)?;
                    &materialized
                } else {
                    binding
                };

                match binding {
                    Binding::Sid { sid, .. } => {
                        let expanded_iri = compactor.decode_sid(sid)?;
                        Ok(Some(IrTerm::iri(expanded_iri)))
                    }
                    Binding::IriMatch { iri, .. } => {
                        // IriMatch: use canonical IRI (already decoded)
                        if let Some(bnode_id) = iri.strip_prefix("_:") {
                            Ok(Some(IrTerm::BlankNode(BlankId::new(bnode_id))))
                        } else {
                            Ok(Some(IrTerm::iri(iri)))
                        }
                    }
                    Binding::Iri(iri) => {
                        // Raw IRI from graph source - check for blank node prefix
                        if let Some(bnode_id) = iri.strip_prefix("_:") {
                            Ok(Some(IrTerm::BlankNode(BlankId::new(bnode_id))))
                        } else {
                            Ok(Some(IrTerm::iri(iri)))
                        }
                    }
                    Binding::Unbound | Binding::Poisoned => Ok(None),
                    Binding::Lit { .. } => Ok(None), // Literals can't be subjects
                    Binding::EncodedLit { .. }
                    | Binding::EncodedSid { .. }
                    | Binding::EncodedPid { .. } => unreachable!(
                        "Encoded bindings should have been materialized before CONSTRUCT subject resolution"
                    ),
                    Binding::Grouped(_) => Err(FormatError::InvalidBinding(
                        "CONSTRUCT does not support GROUP BY (Binding::Grouped encountered)"
                            .to_string(),
                    )),
                }
            }
            None => Ok(None),
        },
        Ref::Sid(sid) => {
            let expanded_iri = compactor.decode_sid(sid)?;
            Ok(Some(IrTerm::iri(expanded_iri)))
        }
        Ref::Iri(iri) => {
            // IRI term (from cross-ledger joins) - use directly
            if let Some(bnode_id) = iri.strip_prefix("_:") {
                Ok(Some(IrTerm::BlankNode(BlankId::new(bnode_id))))
            } else {
                Ok(Some(IrTerm::iri(iri)))
            }
        }
    }
}

/// Resolve a Ref to an IR Term for predicate position
///
/// Predicates must be IRIs, never literals or blank nodes.
/// Returns expanded IRI (via decode_sid, not compact_sid).
fn resolve_predicate_term(
    result: &QueryResult,
    term: &Ref,
    batch: &Batch,
    row_idx: usize,
    compactor: &IriCompactor,
) -> Result<Option<IrTerm>> {
    match term {
        Ref::Var(var_id) => match batch.get(row_idx, *var_id) {
            Some(binding) => {
                let materialized;
                let binding = if binding.is_encoded() {
                    materialized = super::materialize::materialize_binding(result, binding)?;
                    &materialized
                } else {
                    binding
                };

                match binding {
                    Binding::Sid { sid, .. } => {
                        let expanded_iri = compactor.decode_sid(sid)?;
                        Ok(Some(IrTerm::iri(expanded_iri)))
                    }
                    Binding::IriMatch { iri, .. } => {
                        // IriMatch: use canonical IRI - blank nodes not allowed as predicates
                        if iri.starts_with("_:") {
                            Ok(None)
                        } else {
                            Ok(Some(IrTerm::iri(iri)))
                        }
                    }
                    Binding::Iri(iri) => {
                        // Raw IRI from graph source - blank nodes not allowed as predicates
                        if iri.starts_with("_:") {
                            Ok(None)
                        } else {
                            Ok(Some(IrTerm::iri(iri)))
                        }
                    }
                    Binding::Unbound | Binding::Poisoned => Ok(None),
                    Binding::Lit { .. } => Ok(None), // Literals can't be predicates
                    Binding::EncodedLit { .. }
                    | Binding::EncodedSid { .. }
                    | Binding::EncodedPid { .. } => unreachable!(
                        "Encoded bindings should have been materialized before CONSTRUCT predicate resolution"
                    ),
                    Binding::Grouped(_) => Err(FormatError::InvalidBinding(
                        "CONSTRUCT does not support GROUP BY (Binding::Grouped encountered)"
                            .to_string(),
                    )),
                }
            }
            None => Ok(None),
        },
        Ref::Sid(sid) => {
            let expanded_iri = compactor.decode_sid(sid)?;
            Ok(Some(IrTerm::iri(expanded_iri)))
        }
        Ref::Iri(iri) => {
            // IRI term (from cross-ledger joins) - blank nodes not allowed as predicates
            if iri.starts_with("_:") {
                Ok(None)
            } else {
                Ok(Some(IrTerm::iri(iri)))
            }
        }
    }
}

/// Resolve a Term to an IR Term for object position
///
/// Objects can be IRIs, blank nodes, or literals.
/// Returns expanded IRI for references (via decode_sid, not compact_sid).
fn resolve_object_term(
    result: &QueryResult,
    term: &Term,
    batch: &Batch,
    row_idx: usize,
    compactor: &IriCompactor,
) -> Result<Option<IrTerm>> {
    match term {
        Term::Var(var_id) => match batch.get(row_idx, *var_id) {
            Some(binding) => binding_to_ir_term(result, binding, compactor),
            None => Ok(None),
        },
        Term::Sid(sid) => {
            let expanded_iri = compactor.decode_sid(sid)?;
            Ok(Some(IrTerm::iri(expanded_iri)))
        }
        Term::Iri(iri) => {
            // IRI term (from cross-ledger joins) - use directly
            if let Some(bnode_id) = iri.strip_prefix("_:") {
                Ok(Some(IrTerm::BlankNode(BlankId::new(bnode_id))))
            } else {
                Ok(Some(IrTerm::iri(iri)))
            }
        }
        Term::Value(fv) => flake_value_to_ir_term(fv),
    }
}

/// Convert a Binding to an IR Term
fn binding_to_ir_term(
    result: &QueryResult,
    binding: &Binding,
    compactor: &IriCompactor,
) -> Result<Option<IrTerm>> {
    if binding.is_encoded() {
        let materialized = super::materialize::materialize_binding(result, binding)?;
        return binding_to_ir_term(result, &materialized, compactor);
    }

    match binding {
        Binding::Unbound | Binding::Poisoned => Ok(None),

        // Reference - IRI (expanded)
        Binding::Sid { sid, .. } => {
            let expanded_iri = compactor.decode_sid(sid)?;
            Ok(Some(IrTerm::iri(expanded_iri)))
        }

        // IriMatch: use canonical IRI (already decoded)
        Binding::IriMatch { iri, .. } => {
            if let Some(bnode_id) = iri.strip_prefix("_:") {
                Ok(Some(IrTerm::BlankNode(BlankId::new(bnode_id))))
            } else {
                Ok(Some(IrTerm::iri(iri)))
            }
        }

        // Raw IRI from graph source - check for blank node prefix
        Binding::Iri(iri) => {
            if let Some(bnode_id) = iri.strip_prefix("_:") {
                Ok(Some(IrTerm::BlankNode(BlankId::new(bnode_id))))
            } else {
                Ok(Some(IrTerm::iri(iri)))
            }
        }

        // Literal value with explicit datatype
        Binding::Lit { val, dtc, .. } => {
            let dt = dtc.datatype();
            // Decode datatype SID to expanded IRI
            let dt_iri = compactor.decode_sid(dt)?;

            match val {
                FlakeValue::String(s) => {
                    if let Some(lang_tag) = dtc.lang_tag() {
                        // Language-tagged string
                        Ok(Some(IrTerm::Literal {
                            value: LiteralValue::String(Arc::from(s.as_str())),
                            datatype: Datatype::rdf_lang_string(),
                            language: Some(Arc::from(lang_tag)),
                        }))
                    } else {
                        // Plain or typed string
                        Ok(Some(IrTerm::Literal {
                            value: LiteralValue::String(Arc::from(s.as_str())),
                            datatype: Datatype::from_iri(&dt_iri),
                            language: None,
                        }))
                    }
                }
                FlakeValue::Long(n) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::Integer(*n),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::Double(d) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::Double(*d),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::Boolean(b) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::Boolean(*b),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::Vector(_) => Err(FormatError::InvalidBinding(
                    "CONSTRUCT formatting does not support fluree:vector literals yet".to_string(),
                )),
                FlakeValue::Json(json_str) => {
                    // @json datatype: format as string with @json datatype
                    Ok(Some(IrTerm::Literal {
                        value: LiteralValue::String(Arc::from(json_str.as_str())),
                        datatype: Datatype::from_iri(rdf::JSON),
                        language: None,
                    }))
                }
                FlakeValue::Null => Ok(None),
                // Invariant: Binding::Lit never contains FlakeValue::Ref
                FlakeValue::Ref(_) => Err(FormatError::InvalidBinding(
                    "Binding::Lit invariant violated: contains Ref".to_string(),
                )),
                // Extended numeric types - serialize as string with appropriate datatype
                FlakeValue::BigInt(n) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(n.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::Decimal(d) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(d.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                // Temporal types - serialize as original string with appropriate datatype
                FlakeValue::DateTime(dt_val) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(dt_val.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::Date(d) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(d.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::Time(t) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(t.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                // Additional temporal types
                FlakeValue::GYear(v) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(v.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::GYearMonth(v) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(v.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::GMonth(v) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(v.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::GDay(v) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(v.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::GMonthDay(v) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(v.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::YearMonthDuration(v) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(v.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::DayTimeDuration(v) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(v.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::Duration(v) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(v.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
                FlakeValue::GeoPoint(bits) => Ok(Some(IrTerm::Literal {
                    value: LiteralValue::String(Arc::from(bits.to_string())),
                    datatype: Datatype::from_iri(&dt_iri),
                    language: None,
                })),
            }
        }

        Binding::EncodedLit { .. } | Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            unreachable!(
                "Encoded bindings should have been materialized before CONSTRUCT IR conversion"
            )
        }

        // GROUP BY + CONSTRUCT is not supported (semantics undefined)
        Binding::Grouped(_) => Err(FormatError::InvalidBinding(
            "CONSTRUCT does not support GROUP BY (Binding::Grouped encountered)".to_string(),
        )),
    }
}

// NOTE: encoded binding materialization is centralized in `format::materialize`.

/// Convert a FlakeValue constant to an IR Term
fn flake_value_to_ir_term(val: &FlakeValue) -> Result<Option<IrTerm>> {
    Ok(match val {
        FlakeValue::String(s) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(s.as_str())),
            datatype: Datatype::xsd_string(),
            language: None,
        }),
        FlakeValue::Long(n) => Some(IrTerm::Literal {
            value: LiteralValue::Integer(*n),
            datatype: Datatype::xsd_integer(),
            language: None,
        }),
        FlakeValue::Double(d) => Some(IrTerm::Literal {
            value: LiteralValue::Double(*d),
            datatype: Datatype::xsd_double(),
            language: None,
        }),
        FlakeValue::Boolean(b) => Some(IrTerm::Literal {
            value: LiteralValue::Boolean(*b),
            datatype: Datatype::xsd_boolean(),
            language: None,
        }),
        FlakeValue::Vector(_) => {
            return Err(FormatError::InvalidBinding(
                "Vector in constant term not supported".to_string(),
            ))
        }
        FlakeValue::Json(json_str) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(json_str.as_str())),
            datatype: Datatype::from_iri(rdf::JSON),
            language: None,
        }),
        FlakeValue::Null => None,
        FlakeValue::Ref(_) => {
            return Err(FormatError::InvalidBinding(
                "Ref in constant term not supported".to_string(),
            ))
        }
        // Extended numeric types - serialize as string with XSD datatype
        FlakeValue::BigInt(n) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(n.to_string())),
            datatype: Datatype::xsd_integer(),
            language: None,
        }),
        FlakeValue::Decimal(d) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(d.to_string())),
            datatype: Datatype::xsd_decimal(),
            language: None,
        }),
        // Temporal types - serialize as original string with XSD datatype
        FlakeValue::DateTime(dt) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(dt.to_string())),
            datatype: Datatype::xsd_date_time(),
            language: None,
        }),
        FlakeValue::Date(d) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(d.to_string())),
            datatype: Datatype::xsd_date(),
            language: None,
        }),
        FlakeValue::Time(t) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(t.to_string())),
            datatype: Datatype::from_iri(xsd::TIME),
            language: None,
        }),
        // Additional temporal types
        FlakeValue::GYear(v) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(v.to_string())),
            datatype: Datatype::from_iri(xsd::G_YEAR),
            language: None,
        }),
        FlakeValue::GYearMonth(v) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(v.to_string())),
            datatype: Datatype::from_iri(xsd::G_YEAR_MONTH),
            language: None,
        }),
        FlakeValue::GMonth(v) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(v.to_string())),
            datatype: Datatype::from_iri(xsd::G_MONTH),
            language: None,
        }),
        FlakeValue::GDay(v) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(v.to_string())),
            datatype: Datatype::from_iri(xsd::G_DAY),
            language: None,
        }),
        FlakeValue::GMonthDay(v) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(v.to_string())),
            datatype: Datatype::from_iri(xsd::G_MONTH_DAY),
            language: None,
        }),
        FlakeValue::YearMonthDuration(v) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(v.to_string())),
            datatype: Datatype::from_iri(xsd::YEAR_MONTH_DURATION),
            language: None,
        }),
        FlakeValue::DayTimeDuration(v) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(v.to_string())),
            datatype: Datatype::from_iri(xsd::DAY_TIME_DURATION),
            language: None,
        }),
        FlakeValue::Duration(v) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(v.to_string())),
            datatype: Datatype::from_iri(xsd::DURATION),
            language: None,
        }),
        FlakeValue::GeoPoint(bits) => Some(IrTerm::Literal {
            value: LiteralValue::String(Arc::from(bits.to_string())),
            datatype: Datatype::from_iri(geo::WKT_LITERAL),
            language: None,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_ir::datatype::iri as dt_iri;

    // Note: Full integration tests require QueryResult with populated batches.
    // Unit tests for the helper functions can use mock data.

    #[test]
    fn test_flake_value_to_ir_term_string() {
        let result = flake_value_to_ir_term(&FlakeValue::String("hello".to_string()))
            .unwrap()
            .unwrap();
        match result {
            IrTerm::Literal {
                value,
                datatype,
                language,
            } => {
                assert!(matches!(value, LiteralValue::String(s) if s.as_ref() == "hello"));
                assert!(datatype.is_xsd_string());
                assert!(language.is_none());
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_flake_value_to_ir_term_integer() {
        let result = flake_value_to_ir_term(&FlakeValue::Long(42))
            .unwrap()
            .unwrap();
        match result {
            IrTerm::Literal {
                value,
                datatype,
                language,
            } => {
                assert!(matches!(value, LiteralValue::Integer(42)));
                assert_eq!(datatype.as_iri(), dt_iri::XSD_INTEGER);
                assert!(language.is_none());
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_flake_value_to_ir_term_double() {
        let result = flake_value_to_ir_term(&FlakeValue::Double(3.13))
            .unwrap()
            .unwrap();
        match result {
            IrTerm::Literal {
                value,
                datatype,
                language,
            } => {
                assert!(
                    matches!(value, LiteralValue::Double(d) if (d - 3.13).abs() < f64::EPSILON)
                );
                assert_eq!(datatype.as_iri(), dt_iri::XSD_DOUBLE);
                assert!(language.is_none());
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_flake_value_to_ir_term_boolean() {
        let result = flake_value_to_ir_term(&FlakeValue::Boolean(true))
            .unwrap()
            .unwrap();
        match result {
            IrTerm::Literal {
                value,
                datatype,
                language,
            } => {
                assert!(matches!(value, LiteralValue::Boolean(true)));
                assert_eq!(datatype.as_iri(), dt_iri::XSD_BOOLEAN);
                assert!(language.is_none());
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_flake_value_to_ir_term_null() {
        let result = flake_value_to_ir_term(&FlakeValue::Null).unwrap();
        assert!(result.is_none());
    }
}
