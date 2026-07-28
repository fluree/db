//! Canonicalization + SHA-256 of a SPARQL-results-JSON document.
//!
//! Both the native ledger and the R2RML/Iceberg virtual engine are asked to
//! render results as SPARQL JSON (`FormatterConfig::sparql_json()`), so a single
//! canonicalizer serves both sides. The result is an **order-independent
//! multiset hash**: rows are canonicalized cell-by-cell, then the whole row-set
//! is sorted before hashing, so two engines that emit the same bindings in a
//! different order still hash equal.
//!
//! Cell canonicalization keys off the RDF term kind and (for literals) the
//! datatype:
//! - IRIs / bnodes: the IRI verbatim (bnodes collapse to a constant — they are
//!   not stable across engines and none of the seed queries project them).
//! - integer-family literals: reparsed and re-emitted (strips leading zeros /
//!   whitespace).
//! - decimal literals: shortest round-trip of the parsed value (so `100` and
//!   `100.0` collapse). **Blind spot:** the round-trip goes through `f64`, so
//!   two decimals that differ only past ~15 significant digits false-equate.
//!   No corpus query projects such a value today; an exact-decimal
//!   canonicalization would change every blessed decimal cell, so it is
//!   deliberately deferred until a query needs it.
//! - float/double literals: quantized to 12 significant digits.
//! - language-tagged literals: the lexical plus its (case-folded) tag, so a
//!   lang divergence between engines fails the hash gate.
//! - everything else (string, date, dateTime, boolean, ...): the lexical form
//!   verbatim.
//!
//! A document that is neither a JSON-LD graph, an ASK `boolean`, nor a
//! well-formed `results.bindings` table is an **error**, not an empty result —
//! otherwise a formatter shape change could bless 0-row oracles.

use anyhow::Result;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Unit separator between cells of a row (cannot appear in canonical cells).
const CELL_SEP: char = '\u{1f}';
/// Record separator between rows.
const ROW_SEP: char = '\u{1e}';
/// Sentinel for an unbound variable in a row.
const UNBOUND: &str = "\u{0}UNBOUND";
/// Separator between a literal's canonical lexical and its language tag
/// (a control char, so no lexical form can collide with a tagged one).
const LANG_SEP: char = '\u{1d}';

/// Canonicalized result: row count, hex hash, and the sorted canonical rows.
pub struct Canonical {
    pub rows: usize,
    pub hash: String,
    /// Sorted canonical row strings (cells joined by the unit separator).
    pub canonical_rows: Vec<String>,
}

impl Canonical {
    /// The first `n` canonical rows (for `--keep-heads`).
    pub fn heads(&self, n: usize) -> Vec<String> {
        self.canonical_rows.iter().take(n).cloned().collect()
    }
}

/// Canonicalize a query result, dispatching on shape: a JSON-LD graph
/// (`{"@graph":[...]}` or a bare node array — CONSTRUCT/DESCRIBE) is a node
/// multiset; anything else is SPARQL-results JSON (SELECT/ASK). Errors on a
/// document matching none of those shapes rather than treating it as an empty
/// success.
pub fn canonicalize(doc: &Value) -> Result<Canonical> {
    if doc.get("@graph").is_some() || doc.is_array() {
        Ok(canonicalize_graph(doc))
    } else {
        canonicalize_sparql_json(doc)
    }
}

/// Canonicalize a JSON-LD graph: each `@graph` node is one canonical row (its
/// key-ordered serialization), so `rows` is the node count and the hash is an
/// order-independent multiset over nodes. The CONSTRUCT formatter already sorts
/// the graph, so serialization is deterministic within one engine. Full
/// cross-engine RDF isomorphism (blank-node relabeling) is a later refinement —
/// native-vs-virtual CONSTRUCT hash equality is not yet asserted, only the
/// node count and single-engine hash stability.
fn canonicalize_graph(doc: &Value) -> Canonical {
    let nodes: Vec<Value> = doc
        .get("@graph")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| doc.as_array().cloned())
        .unwrap_or_default();
    let rows: Vec<String> = nodes
        .iter()
        .map(|n| serde_json::to_string(n).unwrap_or_default())
        .collect();
    finish(rows)
}

/// Canonicalize a SPARQL-results-JSON document (SELECT or ASK). A document
/// with neither an ASK `boolean` nor a `results.bindings` array is malformed
/// and errors — an empty SELECT still carries `results.bindings: []`.
pub fn canonicalize_sparql_json(doc: &Value) -> Result<Canonical> {
    // ASK: a single boolean.
    if let Some(b) = doc.get("boolean").and_then(Value::as_bool) {
        let row = format!("BOOLEAN{CELL_SEP}{b}");
        return Ok(finish(vec![row]));
    }

    // SELECT: head.vars drives a stable column order; results.bindings are rows.
    let mut vars: Vec<String> = doc
        .get("head")
        .and_then(|h| h.get("vars"))
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    vars.sort();

    let Some(bindings) = doc
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(Value::as_array)
    else {
        anyhow::bail!(
            "unrecognized result document (no `boolean`, no `results.bindings` array): \
             refusing to canonicalize as an empty result"
        );
    };

    let mut rows = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let mut cells = Vec::with_capacity(vars.len());
        for var in &vars {
            cells.push(canonical_cell(binding.get(var)));
        }
        rows.push(cells.join(&CELL_SEP.to_string()));
    }
    Ok(finish(rows))
}

/// Sort, join, and hash a set of canonical row strings.
fn finish(mut rows: Vec<String>) -> Canonical {
    rows.sort();
    let mut hasher = Sha256::new();
    for row in &rows {
        hasher.update(row.as_bytes());
        hasher.update([ROW_SEP as u8]);
    }
    let hash = hex::encode(hasher.finalize());
    Canonical {
        rows: rows.len(),
        hash,
        canonical_rows: rows,
    }
}

/// Canonical string for one SPARQL-JSON binding cell (or an unbound variable).
fn canonical_cell(cell: Option<&Value>) -> String {
    let Some(cell) = cell else {
        return UNBOUND.to_string();
    };
    let kind = cell.get("type").and_then(Value::as_str).unwrap_or("");
    let value = cell.get("value").and_then(Value::as_str).unwrap_or("");
    match kind {
        "uri" => format!("<{value}>"),
        "bnode" => "_:b".to_string(),
        _ => {
            let datatype = cell.get("datatype").and_then(Value::as_str);
            let lit = canonical_literal(value, datatype);
            // A language-tagged literal must not false-equate with its plain
            // (or differently-tagged) twin. BCP-47 tags are case-insensitive,
            // so the tag is case-folded before it joins the canonical form.
            match cell.get("xml:lang").and_then(Value::as_str) {
                Some(lang) if !lang.is_empty() => {
                    format!("{lit}{LANG_SEP}@{}", lang.to_ascii_lowercase())
                }
                _ => lit,
            }
        }
    }
}

/// Canonical lexical for a typed/untyped literal.
fn canonical_literal(value: &str, datatype: Option<&str>) -> String {
    let Some(dt) = datatype else {
        return value.to_string();
    };
    let local = dt.rsplit(['#', '/']).next().unwrap_or(dt);
    match local {
        "integer" | "int" | "long" | "short" | "byte" | "nonNegativeInteger"
        | "nonPositiveInteger" | "negativeInteger" | "positiveInteger" | "unsignedLong"
        | "unsignedInt" | "unsignedShort" | "unsignedByte" => match value.trim().parse::<i128>() {
            Ok(n) => n.to_string(),
            Err(_) => value.to_string(),
        },
        // Shortest round-trip collapses `100` / `100.0` / `100.00`. Blind
        // spot: `f64` carries ~15-17 significant digits, so decimals that
        // differ only beyond that false-equate (see the module doc for why
        // exact-decimal canonicalization is deferred).
        "decimal" => match value.trim().parse::<f64>() {
            Ok(f) => format!("{f}"),
            Err(_) => value.to_string(),
        },
        "double" | "float" => match value.trim().parse::<f64>() {
            Ok(f) => quantize(f),
            Err(_) => value.to_string(),
        },
        _ => value.to_string(),
    }
}

/// Quantize a float to 12 significant digits (canonical scientific form).
fn quantize(f: f64) -> String {
    if !f.is_finite() {
        return format!("{f}");
    }
    if f == 0.0 {
        return "0".to_string();
    }
    // 11 fractional digits in scientific form == 12 significant digits.
    format!("{f:.11e}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sparql_doc(vars: &[&str], rows: Vec<Value>) -> Value {
        json!({
            "head": { "vars": vars },
            "results": { "bindings": rows }
        })
    }

    #[test]
    fn identical_bindings_hash_equal_regardless_of_row_order() {
        let a = sparql_doc(
            &["s", "n"],
            vec![
                json!({"s": {"type":"uri","value":"http://x/1"}, "n": {"type":"literal","value":"5","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}),
                json!({"s": {"type":"uri","value":"http://x/2"}, "n": {"type":"literal","value":"6","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}),
            ],
        );
        let b = sparql_doc(
            &["s", "n"],
            vec![
                json!({"s": {"type":"uri","value":"http://x/2"}, "n": {"type":"literal","value":"6","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}),
                json!({"s": {"type":"uri","value":"http://x/1"}, "n": {"type":"literal","value":"5","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}),
            ],
        );
        let ca = canonicalize_sparql_json(&a).unwrap();
        let cb = canonicalize_sparql_json(&b).unwrap();
        assert_eq!(ca.rows, 2);
        assert_eq!(ca.hash, cb.hash, "row order must not affect the hash");
    }

    #[test]
    fn integer_and_decimal_lexical_variants_collapse() {
        // `100` (integer) rendered by one engine, `100.0` (decimal) by another.
        let a = sparql_doc(
            &["v"],
            vec![
                json!({"v": {"type":"literal","value":"0100","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}),
            ],
        );
        let b = sparql_doc(
            &["v"],
            vec![
                json!({"v": {"type":"literal","value":"100","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}),
            ],
        );
        assert_eq!(
            canonicalize_sparql_json(&a).unwrap().hash,
            canonicalize_sparql_json(&b).unwrap().hash
        );

        let d1 = sparql_doc(
            &["v"],
            vec![
                json!({"v": {"type":"literal","value":"100.0","datatype":"http://www.w3.org/2001/XMLSchema#decimal"}}),
            ],
        );
        let d2 = sparql_doc(
            &["v"],
            vec![
                json!({"v": {"type":"literal","value":"100.00","datatype":"http://www.w3.org/2001/XMLSchema#decimal"}}),
            ],
        );
        assert_eq!(
            canonicalize_sparql_json(&d1).unwrap().hash,
            canonicalize_sparql_json(&d2).unwrap().hash
        );
    }

    #[test]
    fn floats_within_12_sig_digits_hash_equal() {
        let a = sparql_doc(
            &["v"],
            vec![
                json!({"v": {"type":"literal","value":"3.14159265358979","datatype":"http://www.w3.org/2001/XMLSchema#double"}}),
            ],
        );
        // Differs only past the 12th significant digit.
        let b = sparql_doc(
            &["v"],
            vec![
                json!({"v": {"type":"literal","value":"3.141592653589792","datatype":"http://www.w3.org/2001/XMLSchema#double"}}),
            ],
        );
        assert_eq!(
            canonicalize_sparql_json(&a).unwrap().hash,
            canonicalize_sparql_json(&b).unwrap().hash
        );
    }

    #[test]
    fn unbound_variable_is_stable_and_distinct() {
        let bound = sparql_doc(
            &["a", "b"],
            vec![
                json!({"a": {"type":"uri","value":"http://x/1"}, "b": {"type":"literal","value":"x"}}),
            ],
        );
        let unbound = sparql_doc(
            &["a", "b"],
            vec![json!({"a": {"type":"uri","value":"http://x/1"}})],
        );
        assert_ne!(
            canonicalize_sparql_json(&bound).unwrap().hash,
            canonicalize_sparql_json(&unbound).unwrap().hash
        );
    }

    #[test]
    fn lang_tags_join_the_canonical_form_case_folded() {
        let plain = sparql_doc(
            &["v"],
            vec![json!({"v": {"type":"literal","value":"chat"}})],
        );
        let fr = sparql_doc(
            &["v"],
            vec![json!({"v": {"type":"literal","value":"chat","xml:lang":"fr"}})],
        );
        let fr_upper = sparql_doc(
            &["v"],
            vec![json!({"v": {"type":"literal","value":"chat","xml:lang":"FR"}})],
        );
        let en = sparql_doc(
            &["v"],
            vec![json!({"v": {"type":"literal","value":"chat","xml:lang":"en"}})],
        );
        let h = |d: &Value| canonicalize_sparql_json(d).unwrap().hash;
        assert_ne!(h(&plain), h(&fr), "a lang tag must change the hash");
        assert_ne!(h(&fr), h(&en), "different lang tags must not collide");
        assert_eq!(h(&fr), h(&fr_upper), "BCP-47 tags are case-insensitive");
    }

    #[test]
    fn construct_graph_counts_nodes_and_is_order_independent() {
        // A CONSTRUCT/DESCRIBE JSON-LD graph: rows == @graph node count, and the
        // multiset hash ignores node order.
        let a = json!({"@graph": [
            {"@id":"http://x/1","http://p":"a"},
            {"@id":"http://x/2","http://p":"b"},
        ]});
        let b = json!({"@graph": [
            {"@id":"http://x/2","http://p":"b"},
            {"@id":"http://x/1","http://p":"a"},
        ]});
        let ca = canonicalize(&a).unwrap();
        assert_eq!(ca.rows, 2, "two graph nodes");
        assert_eq!(
            ca.hash,
            canonicalize(&b).unwrap().hash,
            "node order must not affect the hash"
        );
        // A SELECT doc still routes to the tabular canonicalizer.
        let sel = sparql_doc(
            &["v"],
            vec![json!({"v":{"type":"uri","value":"http://x/1"}})],
        );
        assert_eq!(canonicalize(&sel).unwrap().rows, 1);
    }

    #[test]
    fn ask_boolean_canonicalizes() {
        let t = json!({"head": {}, "boolean": true});
        let f = json!({"head": {}, "boolean": false});
        assert_eq!(canonicalize_sparql_json(&t).unwrap().rows, 1);
        assert_ne!(
            canonicalize_sparql_json(&t).unwrap().hash,
            canonicalize_sparql_json(&f).unwrap().hash
        );
    }

    #[test]
    fn malformed_results_error_instead_of_blessing_empty() {
        // No boolean, no results.bindings — a formatter shape change must be
        // a loud error, never a 0-row success that could be blessed.
        assert!(canonicalize(&json!({"status": "ok"})).is_err());
        assert!(canonicalize(&json!({"head": {"vars": ["v"]}, "results": {}})).is_err());
        // A genuinely empty SELECT still carries results.bindings: [] — ok.
        let empty = json!({"head": {"vars": ["v"]}, "results": {"bindings": []}});
        assert_eq!(canonicalize(&empty).unwrap().rows, 0);
        // An empty JSON-LD graph is a recognized shape — ok.
        assert_eq!(canonicalize(&json!({"@graph": []})).unwrap().rows, 0);
    }
}
