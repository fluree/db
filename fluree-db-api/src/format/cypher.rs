//! Cypher JSON result format — a Neo4j-compatible tabular envelope with
//! **native scalar** values (NOT RDF-faithful JSON-LD).
//!
//! Shape:
//! ```json
//! {"results":[{"columns":["firstName","birthday"],
//!              "data":[{"row":["Alice","1990-11-23"],"meta":[null,null]}]}]}
//! ```
//!
//! Scalar rules (the Cypher / Neo4j profile, not RDF):
//! - long / int / float / double → JSON number
//! - string → JSON string
//! - `xsd:date` / `xsd:dateTime` → bare ISO string (NOT a `{"@value":…,"@type":…}`
//!   value-object — the difference from JSON-LD that openCypher / LDBC tooling
//!   needs)
//! - boolean → JSON boolean
//! - IRI / node ref → string IRI
//! - list → JSON array
//! - unbound → null
//! - `xsd:decimal` → bare **string** (it is arbitrary-precision / string-backed
//!   for accounting accuracy and may not fit a JSON number, so we preserve the
//!   exact lexical form rather than lose precision)
//!
//! There is no openCypher result-serialization standard; this is Fluree's
//! Neo4j-compatible profile, chosen for openCypher / LDBC interop. The per-cell
//! `meta` array is `null` for scalars (rich node/relationship metadata is
//! deferred).

use super::iri::IriCompactor;
use super::Result;
use crate::query::QueryResult;
use fluree_db_query::binding::Binding;
use fluree_db_query::VarId;
use serde_json::{json, Value as JsonValue};

pub fn format(
    result: &QueryResult,
    compactor: &IriCompactor,
    _config: &super::config::FormatterConfig,
) -> Result<JsonValue> {
    // Column order = the projected vars (RETURN aliases); for a wildcard, the
    // first batch's schema. Internal helper vars are dropped.
    let projected = (!result.output.is_wildcard()).then(|| result.output.projected_vars_or_empty());
    let fallback: &[VarId] = result.batches.first().map_or(&[], |b| b.schema());
    let col_vars: Vec<VarId> = projected
        .as_deref()
        .unwrap_or(fallback)
        .iter()
        .copied()
        .filter(|&v| !super::is_internal_var_name(result.vars.name(v)))
        .collect();

    let columns: Vec<JsonValue> = col_vars
        .iter()
        .map(|&v| JsonValue::String(result.vars.name(v).to_string()))
        .collect();

    let mut data = Vec::new();
    for batch in &result.batches {
        for row_idx in 0..batch.len() {
            let mut row = Vec::with_capacity(col_vars.len());
            for &var_id in &col_vars {
                let cell = match batch.get(row_idx, var_id) {
                    Some(b) if !matches!(b, Binding::Unbound | Binding::Poisoned) => cypherify(
                        super::jsonld::format_binding_with_result(result, b, compactor)?,
                    ),
                    _ => JsonValue::Null,
                };
                row.push(cell);
            }
            let meta = vec![JsonValue::Null; row.len()];
            data.push(json!({ "row": row, "meta": meta }));
        }
    }

    Ok(json!({ "results": [ { "columns": columns, "data": data } ] }))
}

/// Flatten an RDF-faithful JSON-LD value to a Cypher native scalar: a
/// `{"@value": v, "@type": …}` literal becomes bare `v` (so `xsd:date` is a
/// plain ISO string), `{"@id": iri}` becomes the IRI string, bare
/// numbers/strings/booleans pass through, and lists recurse.
fn cypherify(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(mut m) => {
            if let Some(v) = m.remove("@value") {
                v
            } else if let Some(id) = m.remove("@id") {
                id
            } else {
                // A non-literal/non-ref object (rare) — pass through unchanged.
                JsonValue::Object(m)
            }
        }
        JsonValue::Array(items) => JsonValue::Array(items.into_iter().map(cypherify).collect()),
        other => other,
    }
}
