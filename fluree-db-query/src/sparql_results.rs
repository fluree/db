//! SPARQL 1.1 Query Results JSON parser.
//!
//! Parses W3C `application/sparql-results+json` responses into `Binding` values
//! for use by the remote SERVICE executor. URIs are mapped to `Binding::Iri`,
//! literals to `Binding::Lit`, and blank nodes to `Binding::Iri` with `_:` prefix.
//!
//! This is the inverse of `fluree-db-api/src/format/sparql.rs`.

use crate::binding::Binding;
use crate::error::{QueryError, Result};
use crate::remote_service::RemoteBindingRow;
use crate::remote_service::RemoteQueryResult;
use fluree_db_core::{DatatypeConstraint, Date, DateTime, FlakeValue, Sid, Time};
use fluree_vocab::{namespaces, xsd_names};
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// Parse a SPARQL Results JSON response into a `RemoteQueryResult`.
///
/// Expects the standard W3C format:
/// ```json
/// {
///   "head": {"vars": ["s", "name"]},
///   "results": {"bindings": [{
///     "s": {"type": "uri", "value": "http://example.org/alice"},
///     "name": {"type": "literal", "value": "Alice"}
///   }]}
/// }
/// ```
pub fn parse_sparql_results_json(json: &JsonValue) -> Result<RemoteQueryResult> {
    let head = json
        .get("head")
        .ok_or_else(|| QueryError::InvalidQuery("SPARQL results missing 'head'".into()))?;
    let vars_arr = head
        .get("vars")
        .and_then(|v| v.as_array())
        .ok_or_else(|| QueryError::InvalidQuery("SPARQL results missing 'head.vars'".into()))?;

    let vars: Vec<Arc<str>> = vars_arr
        .iter()
        .filter_map(|v| v.as_str().map(Arc::from))
        .collect();

    let results = json
        .get("results")
        .ok_or_else(|| QueryError::InvalidQuery("SPARQL results missing 'results'".into()))?;
    let bindings_arr = results
        .get("bindings")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            QueryError::InvalidQuery("SPARQL results missing 'results.bindings'".into())
        })?;

    let mut rows = Vec::with_capacity(bindings_arr.len());
    for binding_obj in bindings_arr {
        let obj = binding_obj.as_object().ok_or_else(|| {
            QueryError::InvalidQuery("SPARQL results binding is not an object".into())
        })?;
        let mut row = RemoteBindingRow::new();
        for (var_name, term) in obj {
            let binding = parse_rdf_term(term)?;
            row.insert(Arc::from(var_name.as_str()), binding);
        }
        rows.push(row);
    }

    Ok(RemoteQueryResult { vars, rows })
}

/// Parse a single RDF term from SPARQL Results JSON.
///
/// Handles `{"type": "uri"|"literal"|"bnode", "value": "...", ...}`.
fn parse_rdf_term(term: &JsonValue) -> Result<Binding> {
    let typ = term
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| QueryError::InvalidQuery("SPARQL term missing 'type'".into()))?;
    let value = term
        .get("value")
        .and_then(|v| v.as_str())
        .ok_or_else(|| QueryError::InvalidQuery("SPARQL term missing 'value'".into()))?;

    match typ {
        "uri" => Ok(Binding::Iri(Arc::from(value))),

        "bnode" => {
            let bnode_iri = format!("_:{value}");
            Ok(Binding::Iri(Arc::from(bnode_iri.as_str())))
        }

        "literal" => {
            let lang = term.get("xml:lang").and_then(|l| l.as_str());
            let datatype = term.get("datatype").and_then(|d| d.as_str());

            if let Some(lang_tag) = lang {
                Ok(Binding::Lit {
                    val: FlakeValue::String(value.to_string()),
                    dtc: DatatypeConstraint::LangTag(Arc::from(lang_tag)),
                    t: None,
                    op: None,
                    p_id: None,
                })
            } else if let Some(dt) = datatype {
                parse_typed_literal(value, dt)
            } else {
                // Plain literal — xsd:string
                Ok(Binding::Lit {
                    val: FlakeValue::String(value.to_string()),
                    dtc: DatatypeConstraint::Explicit(Sid::new(namespaces::XSD, xsd_names::STRING)),
                    t: None,
                    op: None,
                    p_id: None,
                })
            }
        }

        other => Err(QueryError::InvalidQuery(format!(
            "Unknown SPARQL term type: '{other}'"
        ))),
    }
}

/// Parse a typed literal with an explicit datatype IRI.
fn parse_typed_literal(value: &str, datatype: &str) -> Result<Binding> {
    let xsd = namespaces::XSD;
    let (fv, sid) = match datatype {
        "http://www.w3.org/2001/XMLSchema#string" => (
            FlakeValue::String(value.to_string()),
            Sid::new(xsd, xsd_names::STRING),
        ),
        "http://www.w3.org/2001/XMLSchema#integer"
        | "http://www.w3.org/2001/XMLSchema#int"
        | "http://www.w3.org/2001/XMLSchema#long"
        | "http://www.w3.org/2001/XMLSchema#short"
        | "http://www.w3.org/2001/XMLSchema#byte"
        | "http://www.w3.org/2001/XMLSchema#nonNegativeInteger"
        | "http://www.w3.org/2001/XMLSchema#positiveInteger"
        | "http://www.w3.org/2001/XMLSchema#unsignedLong"
        | "http://www.w3.org/2001/XMLSchema#unsignedInt"
        | "http://www.w3.org/2001/XMLSchema#unsignedShort"
        | "http://www.w3.org/2001/XMLSchema#unsignedByte" => {
            let n: i64 = value.parse().map_err(|_| {
                QueryError::InvalidQuery(format!("Cannot parse '{value}' as {datatype}"))
            })?;
            (FlakeValue::Long(n), Sid::new(xsd, xsd_names::INTEGER))
        }
        "http://www.w3.org/2001/XMLSchema#decimal" => {
            let n: bigdecimal::BigDecimal = value.parse().map_err(|_| {
                QueryError::InvalidQuery(format!("Cannot parse '{value}' as xsd:decimal"))
            })?;
            (
                FlakeValue::Decimal(Box::new(n)),
                Sid::new(xsd, xsd_names::DECIMAL),
            )
        }
        "http://www.w3.org/2001/XMLSchema#double" | "http://www.w3.org/2001/XMLSchema#float" => {
            let n: f64 = value.parse().map_err(|_| {
                QueryError::InvalidQuery(format!("Cannot parse '{value}' as {datatype}"))
            })?;
            (FlakeValue::Double(n), Sid::new(xsd, xsd_names::DOUBLE))
        }
        "http://www.w3.org/2001/XMLSchema#boolean" => {
            let b = match value {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => {
                    return Err(QueryError::InvalidQuery(format!(
                        "Cannot parse '{value}' as xsd:boolean"
                    )))
                }
            };
            (FlakeValue::Boolean(b), Sid::new(xsd, xsd_names::BOOLEAN))
        }
        "http://www.w3.org/2001/XMLSchema#dateTime" => {
            let dt = DateTime::parse(value).map_err(|e| {
                QueryError::InvalidQuery(format!("Cannot parse '{value}' as xsd:dateTime: {e}"))
            })?;
            (
                FlakeValue::DateTime(Box::new(dt)),
                Sid::new(xsd, xsd_names::DATE_TIME),
            )
        }
        "http://www.w3.org/2001/XMLSchema#date" => {
            let d = Date::parse(value).map_err(|e| {
                QueryError::InvalidQuery(format!("Cannot parse '{value}' as xsd:date: {e}"))
            })?;
            (
                FlakeValue::Date(Box::new(d)),
                Sid::new(xsd, xsd_names::DATE),
            )
        }
        "http://www.w3.org/2001/XMLSchema#time" => {
            let t = Time::parse(value).map_err(|e| {
                QueryError::InvalidQuery(format!("Cannot parse '{value}' as xsd:time: {e}"))
            })?;
            (
                FlakeValue::Time(Box::new(t)),
                Sid::new(xsd, xsd_names::TIME),
            )
        }
        _ => {
            // Preserve the original datatype IRI — don't remap to xsd:string.
            // Use namespace 0 (EMPTY) with the full IRI as the name, which is
            // the standard pattern for IRIs not in the local namespace table.
            (
                FlakeValue::String(value.to_string()),
                Sid::new(namespaces::EMPTY, datatype),
            )
        }
    };

    Ok(Binding::Lit {
        val: fv,
        dtc: DatatypeConstraint::Explicit(sid),
        t: None,
        op: None,
        p_id: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_basic_results() {
        let input = json!({
            "head": {"vars": ["s", "name"]},
            "results": {"bindings": [
                {
                    "s": {"type": "uri", "value": "http://example.org/alice"},
                    "name": {"type": "literal", "value": "Alice"}
                },
                {
                    "s": {"type": "uri", "value": "http://example.org/bob"},
                    "name": {"type": "literal", "value": "Bob"}
                }
            ]}
        });

        let result = parse_sparql_results_json(&input).unwrap();
        assert_eq!(result.vars.len(), 2);
        assert_eq!(&*result.vars[0], "s");
        assert_eq!(&*result.vars[1], "name");
        assert_eq!(result.rows.len(), 2);

        let row0 = &result.rows[0];
        assert!(
            matches!(row0.get("s").unwrap(), Binding::Iri(iri) if &**iri == "http://example.org/alice")
        );
        assert!(
            matches!(row0.get("name").unwrap(), Binding::Lit { val: FlakeValue::String(s), .. } if s == "Alice")
        );
    }

    #[test]
    fn parse_typed_literals() {
        let input = json!({
            "head": {"vars": ["n", "d", "b"]},
            "results": {"bindings": [{
                "n": {"type": "literal", "value": "42", "datatype": "http://www.w3.org/2001/XMLSchema#integer"},
                "d": {"type": "literal", "value": "3.15", "datatype": "http://www.w3.org/2001/XMLSchema#double"},
                "b": {"type": "literal", "value": "true", "datatype": "http://www.w3.org/2001/XMLSchema#boolean"}
            }]}
        });

        let result = parse_sparql_results_json(&input).unwrap();
        let row = &result.rows[0];
        assert!(matches!(
            row.get("n").unwrap(),
            Binding::Lit {
                val: FlakeValue::Long(42),
                ..
            }
        ));
        assert!(
            matches!(row.get("d").unwrap(), Binding::Lit { val: FlakeValue::Double(d), .. } if (*d - 3.15).abs() < 0.001)
        );
        assert!(matches!(
            row.get("b").unwrap(),
            Binding::Lit {
                val: FlakeValue::Boolean(true),
                ..
            }
        ));
    }

    #[test]
    fn parse_language_tagged() {
        let input = json!({
            "head": {"vars": ["label"]},
            "results": {"bindings": [{
                "label": {"type": "literal", "value": "Hola", "xml:lang": "es"}
            }]}
        });

        let result = parse_sparql_results_json(&input).unwrap();
        let row = &result.rows[0];
        match row.get("label").unwrap() {
            Binding::Lit {
                val: FlakeValue::String(s),
                dtc,
                ..
            } => {
                assert_eq!(s, "Hola");
                assert_eq!(dtc.lang_tag(), Some("es"));
            }
            other => panic!("expected lang-tagged literal, got {other:?}"),
        }
    }

    #[test]
    fn parse_blank_node() {
        let input = json!({
            "head": {"vars": ["x"]},
            "results": {"bindings": [{
                "x": {"type": "bnode", "value": "b0"}
            }]}
        });

        let result = parse_sparql_results_json(&input).unwrap();
        let row = &result.rows[0];
        assert!(matches!(row.get("x").unwrap(), Binding::Iri(iri) if &**iri == "_:b0"));
    }

    #[test]
    fn parse_empty_results() {
        let input = json!({
            "head": {"vars": ["s", "p", "o"]},
            "results": {"bindings": []}
        });

        let result = parse_sparql_results_json(&input).unwrap();
        assert_eq!(result.vars.len(), 3);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn parse_unbound_variable() {
        let input = json!({
            "head": {"vars": ["s", "name"]},
            "results": {"bindings": [{
                "s": {"type": "uri", "value": "http://example.org/alice"}
            }]}
        });

        let result = parse_sparql_results_json(&input).unwrap();
        let row = &result.rows[0];
        assert!(row.get("s").is_some());
        assert!(row.get("name").is_none());
    }

    #[test]
    fn parse_temporal_types() {
        let input = json!({
            "head": {"vars": ["dt", "d", "t"]},
            "results": {"bindings": [{
                "dt": {"type": "literal", "value": "2024-01-15T10:30:00Z", "datatype": "http://www.w3.org/2001/XMLSchema#dateTime"},
                "d": {"type": "literal", "value": "2024-01-15", "datatype": "http://www.w3.org/2001/XMLSchema#date"},
                "t": {"type": "literal", "value": "10:30:00Z", "datatype": "http://www.w3.org/2001/XMLSchema#time"}
            }]}
        });

        let result = parse_sparql_results_json(&input).unwrap();
        let row = &result.rows[0];
        assert!(matches!(
            row.get("dt").unwrap(),
            Binding::Lit {
                val: FlakeValue::DateTime(_),
                ..
            }
        ));
        assert!(matches!(
            row.get("d").unwrap(),
            Binding::Lit {
                val: FlakeValue::Date(_),
                ..
            }
        ));
        assert!(matches!(
            row.get("t").unwrap(),
            Binding::Lit {
                val: FlakeValue::Time(_),
                ..
            }
        ));
    }

    #[test]
    fn parse_decimal_type() {
        let input = json!({
            "head": {"vars": ["v"]},
            "results": {"bindings": [{
                "v": {"type": "literal", "value": "3.14159265358979323846", "datatype": "http://www.w3.org/2001/XMLSchema#decimal"}
            }]}
        });

        let result = parse_sparql_results_json(&input).unwrap();
        let row = &result.rows[0];
        assert!(matches!(
            row.get("v").unwrap(),
            Binding::Lit {
                val: FlakeValue::Decimal(_),
                ..
            }
        ));
    }

    #[test]
    fn parse_boolean_rejects_garbage() {
        let input = json!({
            "head": {"vars": ["v"]},
            "results": {"bindings": [{
                "v": {"type": "literal", "value": "maybe", "datatype": "http://www.w3.org/2001/XMLSchema#boolean"}
            }]}
        });

        assert!(parse_sparql_results_json(&input).is_err());
    }

    #[test]
    fn parse_custom_datatype_preserves_iri() {
        let input = json!({
            "head": {"vars": ["v"]},
            "results": {"bindings": [{
                "v": {"type": "literal", "value": "custom-value", "datatype": "http://example.org/myType"}
            }]}
        });

        let result = parse_sparql_results_json(&input).unwrap();
        let row = &result.rows[0];
        match row.get("v").unwrap() {
            Binding::Lit { val, dtc, .. } => {
                assert!(matches!(val, FlakeValue::String(s) if s == "custom-value"));
                let sid = dtc.datatype();
                assert_eq!(sid.namespace_code, 0);
                assert_eq!(&*sid.name, "http://example.org/myType");
            }
            other => panic!("expected Lit, got {other:?}"),
        }
    }
}
