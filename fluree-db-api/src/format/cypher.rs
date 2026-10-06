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
    let (columns, rows) = table(result, compactor)?;
    let columns: Vec<JsonValue> = columns.into_iter().map(JsonValue::String).collect();
    let data: Vec<JsonValue> = rows
        .into_iter()
        .map(|row| {
            let meta = vec![JsonValue::Null; row.len()];
            let row: Vec<JsonValue> = row.into_iter().map(cypherify).collect();
            json!({ "row": row, "meta": meta })
        })
        .collect();
    Ok(json!({ "results": [ { "columns": columns, "data": data } ] }))
}

/// The Cypher tabular result before scalar flattening: column names plus
/// per-cell **RDF-faithful** JSON values (`{"@value":…,"@type":…}` literals,
/// `{"@id":…}` refs). Value-typed transports — Bolt/PackStream — consume this
/// so datatype decisions (decimal, temporal) are made once per transport
/// instead of re-derived from flattened JSON. The JSON envelope above is the
/// same table with [`cypherify`] applied per cell.
pub fn table(
    result: &QueryResult,
    compactor: &IriCompactor,
) -> Result<(Vec<String>, Vec<Vec<JsonValue>>)> {
    let col_vars = column_vars(result);
    let columns: Vec<String> = col_vars
        .iter()
        .map(|&v| result.vars.name(v).to_string())
        .collect();

    let mut rows = Vec::new();
    for batch in &result.batches {
        for row_idx in 0..batch.len() {
            let mut row = Vec::with_capacity(col_vars.len());
            for &var_id in &col_vars {
                let cell = match batch.get(row_idx, var_id) {
                    Some(b) if !matches!(b, Binding::Unbound | Binding::Poisoned) => {
                        super::jsonld::format_binding_with_result(result, b, compactor)?
                    }
                    _ => JsonValue::Null,
                };
                row.push(cell);
            }
            rows.push(row);
        }
    }

    Ok((columns, rows))
}

/// Column order for the Cypher tabular formats. An explicit projection
/// (RETURN list) names exactly the user's columns — emit them verbatim,
/// mirroring the JSON-LD array formatter. Only the wildcard path falls back
/// to the batch schema, where synthetic helper vars must be filtered out.
pub(crate) fn column_vars(result: &QueryResult) -> Vec<VarId> {
    let projected = (!result.output.is_wildcard()).then(|| result.output.projected_vars_or_empty());
    match projected {
        Some(vars) => vars,
        None => result
            .batches
            .first()
            .map_or(&[][..], |b| b.schema())
            .iter()
            .copied()
            .filter(|&v| !super::is_internal_var_name(result.vars.name(v)))
            .collect(),
    }
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
                // A plain object — a Cypher map value (`{a: n.name}`) or
                // `properties(n)`. Recurse so each value is itself cypherified
                // (RDF value-objects → native scalars).
                JsonValue::Object(m.into_iter().map(|(k, v)| (k, cypherify(v))).collect())
            }
        }
        JsonValue::Array(items) => JsonValue::Array(items.into_iter().map(cypherify).collect()),
        other => other,
    }
}
