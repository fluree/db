//! RDF term materialization
//!
//! Functions for generating RDF terms from tabular column values.
//!
//! This module provides two APIs:
//!
//! 1. **HashMap-based API** (`materialize_subject`, `materialize_object`): For testing
//!    and simple use cases. Takes `HashMap<String, Option<String>>` as input.
//!
//! 2. **ColumnBatch API** (`materialize_subject_from_batch`, `materialize_object_from_batch`):
//!    For efficient production use. Works directly with `&ColumnBatch` and row indices,
//!    avoiding per-row allocations.

use std::collections::HashMap;

use fluree_db_tabular::{Column, ColumnBatch};
use fluree_vocab::UnresolvedDatatypeConstraint;
use once_cell::sync::Lazy;
use regex::Regex;

use crate::error::{R2rmlError, R2rmlResult};
use crate::mapping::{ConstantValue, ObjectMap, SubjectMap, TermType};

/// Materialized RDF term
///
/// Represents an RDF term generated from tabular data according to
/// an R2RML term map specification.
#[derive(Debug, Clone, PartialEq)]
pub enum RdfTerm {
    /// An IRI
    Iri(String),
    /// A blank node with local identifier
    BlankNode(String),
    /// A literal with optional datatype constraint
    Literal {
        value: String,
        dtc: Option<UnresolvedDatatypeConstraint>,
    },
}

impl RdfTerm {
    /// Create an IRI term
    pub fn iri(iri: impl Into<String>) -> Self {
        RdfTerm::Iri(iri.into())
    }

    /// Create a blank node term
    pub fn blank_node(id: impl Into<String>) -> Self {
        RdfTerm::BlankNode(id.into())
    }

    /// Create a plain string literal
    pub fn string(value: impl Into<String>) -> Self {
        RdfTerm::Literal {
            value: value.into(),
            dtc: None,
        }
    }

    /// Create a typed literal
    pub fn typed(value: impl Into<String>, datatype: impl Into<String>) -> Self {
        RdfTerm::Literal {
            value: value.into(),
            dtc: Some(UnresolvedDatatypeConstraint::Explicit(
                datatype.into().into(),
            )),
        }
    }

    /// Create a language-tagged string
    pub fn lang_string(value: impl Into<String>, lang: impl Into<String>) -> Self {
        RdfTerm::Literal {
            value: value.into(),
            dtc: Some(UnresolvedDatatypeConstraint::LangTag(lang.into().into())),
        }
    }

    /// Check if this is an IRI
    pub fn is_iri(&self) -> bool {
        matches!(self, RdfTerm::Iri(_))
    }

    /// Check if this is a blank node
    pub fn is_blank_node(&self) -> bool {
        matches!(self, RdfTerm::BlankNode(_))
    }

    /// Check if this is a literal
    pub fn is_literal(&self) -> bool {
        matches!(self, RdfTerm::Literal { .. })
    }

    /// Get as IRI string if this is an IRI
    pub fn as_iri(&self) -> Option<&str> {
        match self {
            RdfTerm::Iri(iri) => Some(iri),
            _ => None,
        }
    }
}

/// Convert separate `ObjectMap` datatype/language fields into an
/// `UnresolvedDatatypeConstraint`, preferring language over datatype
/// (per R2RML spec, they are mutually exclusive).
fn object_map_dtc(
    datatype: Option<&str>,
    language: Option<&str>,
) -> Option<UnresolvedDatatypeConstraint> {
    language
        .map(|l| UnresolvedDatatypeConstraint::LangTag(l.into()))
        .or_else(|| datatype.map(|d| UnresolvedDatatypeConstraint::Explicit(d.into())))
}

/// Expand a template by substituting column placeholders with values
///
/// Template placeholders are in the form `{column_name}`. Values are
/// IRI-escaped according to R2RML rules.
///
/// # Arguments
///
/// * `template` - Template string with `{column}` placeholders
/// * `values` - Map from column names to values
///
/// # Returns
///
/// The expanded template string, or an error if a required column is null.
pub fn expand_template(
    template: &str,
    values: &HashMap<String, Option<String>>,
) -> R2rmlResult<String> {
    static PLACEHOLDER_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"\{([^}]+)\}").expect("valid regex"));

    let mut result = template.to_string();
    let mut error: Option<R2rmlError> = None;

    // Find all placeholders and replace them
    for cap in PLACEHOLDER_RE.captures_iter(template) {
        let full_match = cap.get(0).unwrap().as_str();
        let column = &cap[1];

        match values.get(column) {
            Some(Some(value)) => {
                // IRI-escape the value
                let escaped = iri_escape(value);
                result = result.replace(full_match, &escaped);
            }
            Some(None) | None => {
                // Column is null or not present - this produces no term
                error = Some(R2rmlError::Materialization(format!(
                    "Column '{column}' is null, cannot expand template"
                )));
                break;
            }
        }
    }

    if let Some(e) = error {
        return Err(e);
    }

    Ok(result)
}

/// IRI-escape a string value for use in templates
///
/// Escapes characters that are not allowed in IRIs according to RFC 3987.
fn iri_escape(value: &str) -> String {
    let mut result = String::with_capacity(value.len());

    for c in value.chars() {
        match c {
            // Safe characters that don't need escaping
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '.' | '_' | '~' => {
                result.push(c);
            }
            // Sub-delims that are often allowed
            '!' | '$' | '&' | '\'' | '(' | ')' | '*' | '+' | ',' | ';' | '=' => {
                result.push(c);
            }
            // Colon and @ are allowed in path segments
            ':' | '@' => {
                result.push(c);
            }
            // Encode space as %20
            ' ' => {
                result.push_str("%20");
            }
            // Percent-encode other characters
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{byte:02X}"));
                }
            }
        }
    }

    result
}

/// Materialize a subject term from a SubjectMap and column values
///
/// # Arguments
///
/// * `subject_map` - The subject map specification
/// * `values` - Map from column names to values
/// * `row_id` - Optional row identifier for generating unique blank nodes
///
/// # Returns
///
/// The materialized RDF term, or None if any required column is null.
pub fn materialize_subject(
    subject_map: &SubjectMap,
    values: &HashMap<String, Option<String>>,
    row_id: Option<&str>,
) -> R2rmlResult<Option<RdfTerm>> {
    // Check for constant subject
    if let Some(ref constant) = subject_map.constant {
        return Ok(Some(RdfTerm::iri(constant.clone())));
    }

    // Check for column-based subject
    if let Some(ref column) = subject_map.column {
        match values.get(column) {
            Some(Some(value)) => {
                return match subject_map.term_type {
                    TermType::Iri => Ok(Some(RdfTerm::iri(value.clone()))),
                    TermType::BlankNode => Ok(Some(RdfTerm::blank_node(value.clone()))),
                    TermType::Literal => Err(R2rmlError::InvalidValue {
                        property: "rr:termType".to_string(),
                        message: "Subject cannot be a literal".to_string(),
                    }),
                };
            }
            _ => return Ok(None), // Null column produces no subject
        }
    }

    // Check for template-based subject
    if let Some(ref template) = subject_map.template {
        match expand_template(template, values) {
            Ok(expanded) => {
                return match subject_map.term_type {
                    TermType::Iri => Ok(Some(RdfTerm::iri(expanded))),
                    TermType::BlankNode => Ok(Some(RdfTerm::blank_node(expanded))),
                    TermType::Literal => Err(R2rmlError::InvalidValue {
                        property: "rr:termType".to_string(),
                        message: "Subject cannot be a literal".to_string(),
                    }),
                };
            }
            Err(_) => return Ok(None), // Null column in template produces no subject
        }
    }

    // No subject specification - generate blank node if possible
    if subject_map.term_type == TermType::BlankNode {
        let id = row_id.unwrap_or("row");
        return Ok(Some(RdfTerm::blank_node(format!("_:gen_{id}"))));
    }

    Err(R2rmlError::MissingProperty(
        "Subject map must have rr:template, rr:column, or rr:constant".to_string(),
    ))
}

/// Materialize an object term from an ObjectMap and column values
///
/// # Arguments
///
/// * `object_map` - The object map specification
/// * `values` - Map from column names to values
///
/// # Returns
///
/// The materialized RDF term, or None if any required column is null.
/// RefObjectMaps return None (must be resolved separately via join).
pub fn materialize_object(
    object_map: &ObjectMap,
    values: &HashMap<String, Option<String>>,
) -> R2rmlResult<Option<RdfTerm>> {
    match object_map {
        ObjectMap::Column {
            column,
            datatype,
            language,
            term_type,
        } => {
            match values.get(column) {
                Some(Some(value)) => match term_type {
                    TermType::Iri => Ok(Some(RdfTerm::iri(value.clone()))),
                    TermType::BlankNode => Ok(Some(RdfTerm::blank_node(value.clone()))),
                    TermType::Literal => Ok(Some(RdfTerm::Literal {
                        value: value.clone(),
                        dtc: object_map_dtc(datatype.as_deref(), language.as_deref()),
                    })),
                },
                _ => Ok(None), // Null produces no object
            }
        }

        ObjectMap::Constant { value } => match value {
            ConstantValue::Iri(iri) => Ok(Some(RdfTerm::iri(iri.clone()))),
            ConstantValue::Literal {
                value,
                datatype,
                language,
            } => Ok(Some(RdfTerm::Literal {
                value: value.clone(),
                dtc: object_map_dtc(datatype.as_deref(), language.as_deref()),
            })),
        },

        ObjectMap::Template {
            template,
            term_type,
            datatype,
            language,
            ..
        } => {
            match expand_template(template, values) {
                Ok(expanded) => match term_type {
                    TermType::Iri => Ok(Some(RdfTerm::iri(expanded))),
                    TermType::BlankNode => Ok(Some(RdfTerm::blank_node(expanded))),
                    TermType::Literal => Ok(Some(RdfTerm::Literal {
                        value: expanded,
                        dtc: object_map_dtc(datatype.as_deref(), language.as_deref()),
                    })),
                },
                Err(_) => Ok(None), // Null in template produces no object
            }
        }

        ObjectMap::RefObjectMap(_) => {
            // RefObjectMaps must be resolved via join, not direct materialization
            Ok(None)
        }
    }
}

/// Infer XSD datatype from column value type
///
/// Used when no explicit datatype is specified in the object map.
#[cfg(test)]
pub fn infer_datatype(value: &str) -> Option<&'static str> {
    // Try to parse as integer
    if value.parse::<i64>().is_ok() {
        return Some(fluree_vocab::xsd::INTEGER);
    }

    // Try to parse as decimal/double
    if value.parse::<f64>().is_ok() {
        return Some(fluree_vocab::xsd::DECIMAL);
    }

    // Try to parse as boolean
    if value == "true" || value == "false" {
        return Some(fluree_vocab::xsd::BOOLEAN);
    }

    // Default: xsd:string (no need to specify)
    None
}

// =============================================================================
// ColumnBatch API - Efficient production materialization
// =============================================================================

/// Get column value as string at a given row index
///
/// This avoids allocation for numeric types by formatting directly.
/// Returns None if the column is not found or the value is null.
fn column_value_as_string(
    batch: &ColumnBatch,
    column_name: &str,
    row_idx: usize,
) -> Option<String> {
    let col = batch.column_by_name(column_name)?;
    column_to_string(col, row_idx)
}

/// Convert a Column value at a row index to a String
fn column_to_string(col: &Column, row_idx: usize) -> Option<String> {
    match col {
        Column::Boolean(v) => v.get(row_idx).and_then(|v| *v).map(|b| b.to_string()),
        Column::Int32(v) => v.get(row_idx).and_then(|v| *v).map(|n| n.to_string()),
        Column::Int64(v) => v.get(row_idx).and_then(|v| *v).map(|n| n.to_string()),
        Column::Float32(v) => v.get(row_idx).and_then(|v| *v).map(|n| n.to_string()),
        Column::Float64(v) => v.get(row_idx).and_then(|v| *v).map(|n| n.to_string()),
        Column::String(v) => v.get(row_idx).and_then(std::clone::Clone::clone),
        Column::Bytes(v) => v
            .get(row_idx)
            .and_then(|v| v.as_ref())
            .map(|b| base64_encode(b)),
        Column::Date(v) => v.get(row_idx).and_then(|v| *v).map(format_date),
        Column::Timestamp(v) | Column::TimestampTz(v) => {
            v.get(row_idx).and_then(|v| *v).map(format_timestamp)
        }
        Column::Decimal { values, scale, .. } => values
            .get(row_idx)
            .and_then(|v| *v)
            .map(|n| format_decimal(n, *scale)),
    }
}

/// Base64 encode bytes (simple implementation)
fn base64_encode(bytes: &[u8]) -> String {
    // Simple hex encoding for now; can be replaced with proper base64
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Format date (days since epoch) as ISO 8601
fn format_date(days: i32) -> String {
    // Simple implementation: 1970-01-01 + days
    // For production, use chrono or similar
    let epoch = 719_163i64; // Days from year 0 to 1970-01-01 (Julian day offset)
    let total_days = epoch + days as i64;

    // Simplified Gregorian calendar calculation
    let (year, month, day) = days_to_ymd(total_days);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Format timestamp (microseconds since epoch) as ISO 8601
fn format_timestamp(micros: i64) -> String {
    let seconds = micros / 1_000_000;
    let micros_part = (micros % 1_000_000).abs();

    // Days since epoch
    let days_since_epoch = seconds / 86400;
    let time_of_day = (seconds % 86400 + 86400) % 86400; // Handle negative

    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let secs = time_of_day % 60;

    let epoch = 719_163i64;
    let total_days = epoch + days_since_epoch;
    let (year, month, day) = days_to_ymd(total_days);

    if micros_part > 0 {
        format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{secs:02}.{micros_part:06}Z")
    } else {
        format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{secs:02}Z")
    }
}

/// Convert total days (from year 0) to (year, month, day)
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    // Simplified algorithm - for production use chrono
    let mut remaining = days;
    let mut year = 1i32;

    // Handle years
    while remaining >= 365 {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining >= days_in_year {
            remaining -= days_in_year;
            year += 1;
        } else {
            break;
        }
    }

    // Handle months
    let days_in_months = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1u32;
    for days_in_month in &days_in_months {
        if remaining >= *days_in_month as i64 {
            remaining -= *days_in_month as i64;
            month += 1;
        } else {
            break;
        }
    }

    (year, month, remaining as u32 + 1)
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Format decimal with scale
fn format_decimal(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        // No decimal point needed
        let multiplier = 10i128.pow((-scale) as u32);
        (unscaled * multiplier).to_string()
    } else {
        // Insert decimal point
        let divisor = 10i128.pow(scale as u32);
        let integer_part = unscaled / divisor;
        let fractional_part = (unscaled % divisor).abs();
        format!(
            "{}.{:0>width$}",
            integer_part,
            fractional_part,
            width = scale as usize
        )
    }
}

/// Expand a template using values from a ColumnBatch at a specific row
///
/// This is the efficient batch-aware version that avoids HashMap allocation.
pub fn expand_template_from_batch(
    template: &str,
    batch: &ColumnBatch,
    row_idx: usize,
) -> R2rmlResult<String> {
    static PLACEHOLDER_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"\{([^}]+)\}").expect("valid regex"));

    let mut result = template.to_string();
    let mut error: Option<R2rmlError> = None;

    for cap in PLACEHOLDER_RE.captures_iter(template) {
        let full_match = cap.get(0).unwrap().as_str();
        let column = &cap[1];

        match column_value_as_string(batch, column, row_idx) {
            Some(value) => {
                let escaped = iri_escape(&value);
                result = result.replace(full_match, &escaped);
            }
            None => {
                error = Some(R2rmlError::Materialization(format!(
                    "Column '{column}' is null or not found at row {row_idx}, cannot expand template"
                )));
                break;
            }
        }
    }

    if let Some(e) = error {
        return Err(e);
    }

    Ok(result)
}

/// Materialize a subject term from a SubjectMap and a ColumnBatch row
///
/// This is the efficient batch-aware version that avoids HashMap allocation.
///
/// # Arguments
///
/// * `subject_map` - The subject map specification
/// * `batch` - The column batch containing data
/// * `row_idx` - The row index to materialize from
///
/// # Returns
///
/// The materialized RDF term, or None if any required column is null.
pub fn materialize_subject_from_batch(
    subject_map: &SubjectMap,
    batch: &ColumnBatch,
    row_idx: usize,
) -> R2rmlResult<Option<RdfTerm>> {
    // Check for constant subject
    if let Some(ref constant) = subject_map.constant {
        return Ok(Some(RdfTerm::iri(constant.clone())));
    }

    // Check for column-based subject
    if let Some(ref column) = subject_map.column {
        match column_value_as_string(batch, column, row_idx) {
            Some(value) => {
                return match subject_map.term_type {
                    TermType::Iri => Ok(Some(RdfTerm::iri(value))),
                    TermType::BlankNode => Ok(Some(RdfTerm::blank_node(value))),
                    TermType::Literal => Err(R2rmlError::InvalidValue {
                        property: "rr:termType".to_string(),
                        message: "Subject cannot be a literal".to_string(),
                    }),
                };
            }
            None => return Ok(None),
        }
    }

    // Check for template-based subject
    if let Some(ref template) = subject_map.template {
        match expand_template_from_batch(template, batch, row_idx) {
            Ok(expanded) => {
                return match subject_map.term_type {
                    TermType::Iri => Ok(Some(RdfTerm::iri(expanded))),
                    TermType::BlankNode => Ok(Some(RdfTerm::blank_node(expanded))),
                    TermType::Literal => Err(R2rmlError::InvalidValue {
                        property: "rr:termType".to_string(),
                        message: "Subject cannot be a literal".to_string(),
                    }),
                };
            }
            Err(_) => return Ok(None),
        }
    }

    // No subject specification - generate blank node if possible
    if subject_map.term_type == TermType::BlankNode {
        return Ok(Some(RdfTerm::blank_node(format!("_:gen_row_{row_idx}"))));
    }

    Err(R2rmlError::MissingProperty(
        "Subject map must have rr:template, rr:column, or rr:constant".to_string(),
    ))
}

/// Materialize an object term from an ObjectMap and a ColumnBatch row
///
/// This is the efficient batch-aware version that avoids HashMap allocation.
///
/// # Arguments
///
/// * `object_map` - The object map specification
/// * `batch` - The column batch containing data
/// * `row_idx` - The row index to materialize from
///
/// # Returns
///
/// The materialized RDF term, or None if any required column is null.
/// RefObjectMaps return None (must be resolved separately via join).
pub fn materialize_object_from_batch(
    object_map: &ObjectMap,
    batch: &ColumnBatch,
    row_idx: usize,
) -> R2rmlResult<Option<RdfTerm>> {
    match object_map {
        ObjectMap::Column {
            column,
            datatype,
            language,
            term_type,
        } => match column_value_as_string(batch, column, row_idx) {
            Some(value) => match term_type {
                TermType::Iri => Ok(Some(RdfTerm::iri(value))),
                TermType::BlankNode => Ok(Some(RdfTerm::blank_node(value))),
                TermType::Literal => Ok(Some(RdfTerm::Literal {
                    value,
                    dtc: object_map_dtc(datatype.as_deref(), language.as_deref()),
                })),
            },
            None => Ok(None),
        },

        ObjectMap::Constant { value } => match value {
            ConstantValue::Iri(iri) => Ok(Some(RdfTerm::iri(iri.clone()))),
            ConstantValue::Literal {
                value,
                datatype,
                language,
            } => Ok(Some(RdfTerm::Literal {
                value: value.clone(),
                dtc: object_map_dtc(datatype.as_deref(), language.as_deref()),
            })),
        },

        ObjectMap::Template {
            template,
            term_type,
            datatype,
            language,
            ..
        } => match expand_template_from_batch(template, batch, row_idx) {
            Ok(expanded) => match term_type {
                TermType::Iri => Ok(Some(RdfTerm::iri(expanded))),
                TermType::BlankNode => Ok(Some(RdfTerm::blank_node(expanded))),
                TermType::Literal => Ok(Some(RdfTerm::Literal {
                    value: expanded,
                    dtc: object_map_dtc(datatype.as_deref(), language.as_deref()),
                })),
            },
            Err(_) => Ok(None),
        },

        ObjectMap::RefObjectMap(_) => {
            // RefObjectMaps must be resolved via join, not direct materialization
            Ok(None)
        }
    }
}

/// Get the join key values for a row (used for RefObjectMap joins)
///
/// Returns the child column values as strings for hash-based join matching.
pub fn get_join_key_from_batch(
    child_columns: &[String],
    batch: &ColumnBatch,
    row_idx: usize,
) -> Option<Vec<String>> {
    let mut key_values = Vec::with_capacity(child_columns.len());
    for col_name in child_columns {
        match column_value_as_string(batch, col_name, row_idx) {
            Some(v) => key_values.push(v),
            None => return None, // Null in join key means no match
        }
    }
    Some(key_values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_template_simple() {
        let mut values = HashMap::new();
        values.insert("id".to_string(), Some("123".to_string()));

        let result = expand_template("http://example.org/item/{id}", &values).unwrap();
        assert_eq!(result, "http://example.org/item/123");
    }

    #[test]
    fn test_expand_template_multiple() {
        let mut values = HashMap::new();
        values.insert("ns".to_string(), Some("test".to_string()));
        values.insert("id".to_string(), Some("42".to_string()));

        let result = expand_template("http://example.org/{ns}/{id}", &values).unwrap();
        assert_eq!(result, "http://example.org/test/42");
    }

    #[test]
    fn test_expand_template_escaping() {
        let mut values = HashMap::new();
        values.insert("name".to_string(), Some("hello world".to_string()));

        let result = expand_template("http://example.org/{name}", &values).unwrap();
        assert_eq!(result, "http://example.org/hello%20world");
    }

    #[test]
    fn test_expand_template_null_column() {
        let mut values = HashMap::new();
        values.insert("id".to_string(), None);

        let result = expand_template("http://example.org/{id}", &values);
        assert!(result.is_err());
    }

    #[test]
    fn test_materialize_subject_template() {
        let sm = SubjectMap::template("http://example.org/airline/{id}");

        let mut values = HashMap::new();
        values.insert("id".to_string(), Some("42".to_string()));

        let result = materialize_subject(&sm, &values, None).unwrap();
        assert_eq!(result, Some(RdfTerm::iri("http://example.org/airline/42")));
    }

    #[test]
    fn test_materialize_subject_constant() {
        let sm = SubjectMap::constant("http://example.org/singleton");

        let values = HashMap::new();
        let result = materialize_subject(&sm, &values, None).unwrap();
        assert_eq!(result, Some(RdfTerm::iri("http://example.org/singleton")));
    }

    #[test]
    fn test_materialize_subject_null_produces_none() {
        let sm = SubjectMap::template("http://example.org/{id}");

        let mut values = HashMap::new();
        values.insert("id".to_string(), None);

        let result = materialize_subject(&sm, &values, None).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_materialize_object_column() {
        let om = ObjectMap::column("name");

        let mut values = HashMap::new();
        values.insert("name".to_string(), Some("Alice".to_string()));

        let result = materialize_object(&om, &values).unwrap();
        assert_eq!(result, Some(RdfTerm::string("Alice")));
    }

    #[test]
    fn test_materialize_object_typed() {
        let om = ObjectMap::column_typed("age", "http://www.w3.org/2001/XMLSchema#integer");

        let mut values = HashMap::new();
        values.insert("age".to_string(), Some("30".to_string()));

        let result = materialize_object(&om, &values).unwrap();
        assert_eq!(
            result,
            Some(RdfTerm::typed(
                "30",
                "http://www.w3.org/2001/XMLSchema#integer"
            ))
        );
    }

    #[test]
    fn test_materialize_object_iri() {
        let om = ObjectMap::column_iri("homepage");

        let mut values = HashMap::new();
        values.insert(
            "homepage".to_string(),
            Some("http://example.org/alice".to_string()),
        );

        let result = materialize_object(&om, &values).unwrap();
        assert_eq!(result, Some(RdfTerm::iri("http://example.org/alice")));
    }

    #[test]
    fn test_materialize_object_constant() {
        let om = ObjectMap::constant_iri("http://example.org/constant");

        let values = HashMap::new();
        let result = materialize_object(&om, &values).unwrap();
        assert_eq!(result, Some(RdfTerm::iri("http://example.org/constant")));
    }

    #[test]
    fn test_materialize_object_template() {
        let om = ObjectMap::template("http://example.org/category/{cat}", vec!["cat".to_string()]);

        let mut values = HashMap::new();
        values.insert("cat".to_string(), Some("books".to_string()));

        let result = materialize_object(&om, &values).unwrap();
        assert_eq!(
            result,
            Some(RdfTerm::iri("http://example.org/category/books"))
        );
    }

    #[test]
    fn test_materialize_object_null_produces_none() {
        let om = ObjectMap::column("missing");

        let values = HashMap::new();
        let result = materialize_object(&om, &values).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_iri_escape() {
        assert_eq!(iri_escape("simple"), "simple");
        assert_eq!(iri_escape("with space"), "with%20space");
        assert_eq!(iri_escape("test/path"), "test%2Fpath");
        assert_eq!(iri_escape("你好"), "%E4%BD%A0%E5%A5%BD");
    }

    #[test]
    fn test_infer_datatype() {
        assert_eq!(
            infer_datatype("42"),
            Some("http://www.w3.org/2001/XMLSchema#integer")
        );
        assert_eq!(
            infer_datatype("3.14"),
            Some("http://www.w3.org/2001/XMLSchema#decimal")
        );
        assert_eq!(
            infer_datatype("true"),
            Some("http://www.w3.org/2001/XMLSchema#boolean")
        );
        assert_eq!(infer_datatype("hello"), None);
    }

    #[test]
    fn test_rdf_term_constructors() {
        let iri = RdfTerm::iri("http://example.org");
        assert!(iri.is_iri());
        assert_eq!(iri.as_iri(), Some("http://example.org"));

        let blank = RdfTerm::blank_node("b0");
        assert!(blank.is_blank_node());

        let lit = RdfTerm::string("hello");
        assert!(lit.is_literal());
    }

    // =========================================================================
    // ColumnBatch API tests
    // =========================================================================

    use fluree_db_tabular::{BatchSchema, FieldInfo, FieldType};
    use std::sync::Arc;

    fn sample_batch() -> ColumnBatch {
        let schema = Arc::new(BatchSchema::new(vec![
            FieldInfo {
                name: "id".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                field_id: 1,
            },
            FieldInfo {
                name: "name".to_string(),
                field_type: FieldType::String,
                nullable: true,
                field_id: 2,
            },
            FieldInfo {
                name: "age".to_string(),
                field_type: FieldType::Int32,
                nullable: true,
                field_id: 3,
            },
        ]));

        let columns = vec![
            Column::Int64(vec![Some(1), Some(2), Some(3)]),
            Column::String(vec![
                Some("Alice".to_string()),
                Some("Bob".to_string()),
                None,
            ]),
            Column::Int32(vec![Some(30), Some(25), Some(40)]),
        ];

        ColumnBatch::new(schema, columns).unwrap()
    }

    #[test]
    fn test_expand_template_from_batch() {
        let batch = sample_batch();

        // Row 0: id=1, name="Alice"
        let result =
            expand_template_from_batch("http://example.org/person/{id}", &batch, 0).unwrap();
        assert_eq!(result, "http://example.org/person/1");

        // Row 1: id=2, name="Bob"
        let result =
            expand_template_from_batch("http://example.org/{name}/{id}", &batch, 1).unwrap();
        assert_eq!(result, "http://example.org/Bob/2");

        // Row 2: name is null - should error
        let result = expand_template_from_batch("http://example.org/{name}", &batch, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_materialize_subject_from_batch() {
        let batch = sample_batch();
        let sm = SubjectMap::template("http://example.org/person/{id}");

        let result = materialize_subject_from_batch(&sm, &batch, 0).unwrap();
        assert_eq!(result, Some(RdfTerm::iri("http://example.org/person/1")));

        let result = materialize_subject_from_batch(&sm, &batch, 1).unwrap();
        assert_eq!(result, Some(RdfTerm::iri("http://example.org/person/2")));
    }

    #[test]
    fn test_materialize_object_from_batch_column() {
        let batch = sample_batch();
        let om = ObjectMap::column("name");

        let result = materialize_object_from_batch(&om, &batch, 0).unwrap();
        assert_eq!(result, Some(RdfTerm::string("Alice")));

        let result = materialize_object_from_batch(&om, &batch, 1).unwrap();
        assert_eq!(result, Some(RdfTerm::string("Bob")));

        // Row 2 has null name
        let result = materialize_object_from_batch(&om, &batch, 2).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_materialize_object_from_batch_int() {
        let batch = sample_batch();
        let om = ObjectMap::column_typed("age", "http://www.w3.org/2001/XMLSchema#integer");

        let result = materialize_object_from_batch(&om, &batch, 0).unwrap();
        assert_eq!(
            result,
            Some(RdfTerm::typed(
                "30",
                "http://www.w3.org/2001/XMLSchema#integer"
            ))
        );
    }

    #[test]
    fn test_get_join_key_from_batch() {
        let batch = sample_batch();

        // Single column key
        let key = get_join_key_from_batch(&["id".to_string()], &batch, 0);
        assert_eq!(key, Some(vec!["1".to_string()]));

        // Composite key
        let key = get_join_key_from_batch(&["id".to_string(), "name".to_string()], &batch, 1);
        assert_eq!(key, Some(vec!["2".to_string(), "Bob".to_string()]));

        // Null in key returns None
        let key = get_join_key_from_batch(&["name".to_string()], &batch, 2);
        assert_eq!(key, None);
    }

    #[test]
    fn test_format_decimal() {
        assert_eq!(format_decimal(12345, 2), "123.45");
        assert_eq!(format_decimal(100, 2), "1.00");
        assert_eq!(format_decimal(5, 3), "0.005");
        assert_eq!(format_decimal(1000, 0), "1000");
        assert_eq!(format_decimal(5, -2), "500"); // scale -2 = multiply by 100
    }
}
