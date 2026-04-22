//! RDF term types: IRI, blank node, and literal
//!
//! Terms are the building blocks of triples. A term can be:
//! - An IRI (always expanded, never prefixed)
//! - A blank node (with stable identifier)
//! - A literal (value + explicit datatype + optional language tag)

use crate::Datatype;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Blank node identifier
///
/// Blank node IDs are stable within a graph but have no global meaning.
/// Different sources may use different ID schemes:
/// - Turtle: `_:b0`, `_:b1`, ...
/// - Fluree: `_:fdb-<ulid>`
/// - JSON-LD: `_:label` or generated
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlankId(Arc<str>);

impl BlankId {
    /// Create a blank node ID from a label
    ///
    /// The label should NOT include the `_:` prefix.
    pub fn new(label: impl AsRef<str>) -> Self {
        Self(Arc::from(label.as_ref()))
    }

    /// Get the label (without `_:` prefix)
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the full N-Triples representation (`_:label`)
    pub fn to_ntriples(&self) -> String {
        format!("_:{}", self.0)
    }
}

impl std::fmt::Display for BlankId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "_:{}", self.0)
    }
}

/// Literal value storage
///
/// Stores the actual value in a type-appropriate format. The `Json` variant
/// stores a **canonical normalized JSON string** (not `serde_json::Value`)
/// to enable total ordering and hashing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LiteralValue {
    /// String value (UTF-8)
    String(Arc<str>),
    /// Boolean value
    Boolean(bool),
    /// Integer value (i64 range)
    Integer(i64),
    /// Floating point value (f64)
    Double(f64),
    /// JSON value stored as canonical normalized string
    ///
    /// Use `json_ld::normalize_data()` to canonicalize before storing.
    /// The formatter will parse this back to a Value for output.
    Json(Arc<str>),
}

impl LiteralValue {
    /// Create a string literal value
    pub fn string(s: impl AsRef<str>) -> Self {
        LiteralValue::String(Arc::from(s.as_ref()))
    }

    /// Create a JSON literal value from a canonical string
    ///
    /// The string should already be canonicalized (e.g., via `json_ld::normalize_data()`).
    pub fn json_canonical(canonical: impl AsRef<str>) -> Self {
        LiteralValue::Json(Arc::from(canonical.as_ref()))
    }

    /// Get the lexical representation of this value
    pub fn lexical(&self) -> String {
        match self {
            LiteralValue::String(s) => s.to_string(),
            LiteralValue::Boolean(b) => b.to_string(),
            LiteralValue::Integer(i) => i.to_string(),
            LiteralValue::Double(d) => {
                if d.is_nan() {
                    "NaN".to_string()
                } else if d.is_infinite() {
                    if d.is_sign_positive() {
                        "INF".to_string()
                    } else {
                        "-INF".to_string()
                    }
                } else {
                    d.to_string()
                }
            }
            LiteralValue::Json(s) => s.to_string(),
        }
    }

    /// Check if this is a string value
    pub fn is_string(&self) -> bool {
        matches!(self, LiteralValue::String(_))
    }

    /// Try to get as string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            LiteralValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            LiteralValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as integer
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            LiteralValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to get as double
    pub fn as_double(&self) -> Option<f64> {
        match self {
            LiteralValue::Double(d) => Some(*d),
            LiteralValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to get as JSON string
    pub fn as_json_str(&self) -> Option<&str> {
        match self {
            LiteralValue::Json(s) => Some(s),
            _ => None,
        }
    }
}

impl PartialEq for LiteralValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LiteralValue::String(a), LiteralValue::String(b)) => a == b,
            (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => a == b,
            (LiteralValue::Integer(a), LiteralValue::Integer(b)) => a == b,
            (LiteralValue::Double(a), LiteralValue::Double(b)) => a.to_bits() == b.to_bits(),
            (LiteralValue::Json(a), LiteralValue::Json(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for LiteralValue {}

impl Hash for LiteralValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            LiteralValue::String(s) => s.hash(state),
            LiteralValue::Boolean(b) => b.hash(state),
            LiteralValue::Integer(i) => i.hash(state),
            LiteralValue::Double(d) => d.to_bits().hash(state),
            LiteralValue::Json(s) => s.hash(state),
        }
    }
}

impl PartialOrd for LiteralValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LiteralValue {
    fn cmp(&self, other: &Self) -> Ordering {
        // Type discriminant ordering: String < Boolean < Integer < Double < Json
        let type_ord = |v: &LiteralValue| -> u8 {
            match v {
                LiteralValue::String(_) => 0,
                LiteralValue::Boolean(_) => 1,
                LiteralValue::Integer(_) => 2,
                LiteralValue::Double(_) => 3,
                LiteralValue::Json(_) => 4,
            }
        };

        match type_ord(self).cmp(&type_ord(other)) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // Same type: compare values
        match (self, other) {
            (LiteralValue::String(a), LiteralValue::String(b)) => a.cmp(b),
            (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => a.cmp(b),
            (LiteralValue::Integer(a), LiteralValue::Integer(b)) => a.cmp(b),
            (LiteralValue::Double(a), LiteralValue::Double(b)) => a
                .partial_cmp(b)
                .unwrap_or_else(|| a.to_bits().cmp(&b.to_bits())),
            (LiteralValue::Json(a), LiteralValue::Json(b)) => a.cmp(b),
            _ => Ordering::Equal, // Should not happen
        }
    }
}

/// An RDF term (subject, predicate, or object position)
///
/// # Invariants
///
/// - `Term::Iri` always contains an **expanded** IRI, never a prefixed form.
/// - For `Term::Literal` with a language tag, the datatype must be `rdf:langString`.
/// - The predicate position of a triple can only be `Term::Iri`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Term {
    /// Full expanded IRI (e.g., "http://schema.org/Person")
    Iri(Arc<str>),

    /// Blank node with stable identifier
    BlankNode(BlankId),

    /// Literal value with explicit datatype
    Literal {
        /// The actual value
        value: LiteralValue,
        /// Datatype (always present, never None)
        datatype: Datatype,
        /// Language tag (only valid when datatype is rdf:langString)
        language: Option<Arc<str>>,
    },
}

impl Term {
    /// Create an IRI term from an expanded IRI string
    pub fn iri(iri: impl AsRef<str>) -> Self {
        Term::Iri(Arc::from(iri.as_ref()))
    }

    /// Create a blank node term
    pub fn blank(label: impl AsRef<str>) -> Self {
        Term::BlankNode(BlankId::new(label))
    }

    /// Create a plain string literal (xsd:string)
    pub fn string(value: impl AsRef<str>) -> Self {
        Term::Literal {
            value: LiteralValue::string(value),
            datatype: Datatype::xsd_string(),
            language: None,
        }
    }

    /// Create a boolean literal (xsd:boolean)
    pub fn boolean(value: bool) -> Self {
        Term::Literal {
            value: LiteralValue::Boolean(value),
            datatype: Datatype::xsd_boolean(),
            language: None,
        }
    }

    /// Create an integer literal (xsd:integer)
    pub fn integer(value: i64) -> Self {
        Term::Literal {
            value: LiteralValue::Integer(value),
            datatype: Datatype::xsd_integer(),
            language: None,
        }
    }

    /// Create a long literal (xsd:long)
    pub fn long(value: i64) -> Self {
        Term::Literal {
            value: LiteralValue::Integer(value),
            datatype: Datatype::xsd_long(),
            language: None,
        }
    }

    /// Create a double literal (xsd:double)
    pub fn double(value: f64) -> Self {
        Term::Literal {
            value: LiteralValue::Double(value),
            datatype: Datatype::xsd_double(),
            language: None,
        }
    }

    /// Create a language-tagged string literal (rdf:langString)
    pub fn lang_string(value: impl AsRef<str>, lang: impl AsRef<str>) -> Self {
        Term::Literal {
            value: LiteralValue::string(value),
            datatype: Datatype::rdf_lang_string(),
            language: Some(Arc::from(lang.as_ref())),
        }
    }

    /// Create a typed literal with a custom datatype
    pub fn typed(value: impl AsRef<str>, datatype: Datatype) -> Self {
        Term::Literal {
            value: LiteralValue::string(value),
            datatype,
            language: None,
        }
    }

    /// Create a JSON literal (@json / rdf:JSON)
    ///
    /// The value should be a canonical JSON string.
    pub fn json(canonical_json: impl AsRef<str>) -> Self {
        Term::Literal {
            value: LiteralValue::json_canonical(canonical_json),
            datatype: Datatype::rdf_json(),
            language: None,
        }
    }

    /// Check if this is an IRI term
    pub fn is_iri(&self) -> bool {
        matches!(self, Term::Iri(_))
    }

    /// Check if this is a blank node
    pub fn is_blank(&self) -> bool {
        matches!(self, Term::BlankNode(_))
    }

    /// Check if this is a literal
    pub fn is_literal(&self) -> bool {
        matches!(self, Term::Literal { .. })
    }

    /// Try to get as IRI string
    pub fn as_iri(&self) -> Option<&str> {
        match self {
            Term::Iri(iri) => Some(iri),
            _ => None,
        }
    }

    /// Try to get as blank node ID
    pub fn as_blank(&self) -> Option<&BlankId> {
        match self {
            Term::BlankNode(id) => Some(id),
            _ => None,
        }
    }

    /// Try to get literal components
    pub fn as_literal(&self) -> Option<(&LiteralValue, &Datatype, Option<&str>)> {
        match self {
            Term::Literal {
                value,
                datatype,
                language,
            } => Some((value, datatype, language.as_deref())),
            _ => None,
        }
    }

    /// Get the IRI for use as subject or predicate
    ///
    /// Returns None for blank nodes and literals.
    /// For blank nodes in subject position, use the blank node's string representation.
    pub fn subject_iri(&self) -> Option<&str> {
        match self {
            Term::Iri(iri) => Some(iri),
            _ => None,
        }
    }
}

impl PartialEq for Term {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Term::Iri(a), Term::Iri(b)) => a == b,
            (Term::BlankNode(a), Term::BlankNode(b)) => a == b,
            (
                Term::Literal {
                    value: v1,
                    datatype: d1,
                    language: l1,
                },
                Term::Literal {
                    value: v2,
                    datatype: d2,
                    language: l2,
                },
            ) => v1 == v2 && d1 == d2 && l1 == l2,
            _ => false,
        }
    }
}

impl Eq for Term {}

impl Hash for Term {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Term::Iri(iri) => iri.hash(state),
            Term::BlankNode(id) => id.hash(state),
            Term::Literal {
                value,
                datatype,
                language,
            } => {
                value.hash(state);
                datatype.hash(state);
                language.hash(state);
            }
        }
    }
}

impl PartialOrd for Term {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Term {
    fn cmp(&self, other: &Self) -> Ordering {
        // Type ordering: BlankNode < Iri < Literal
        let type_ord = |t: &Term| -> u8 {
            match t {
                Term::BlankNode(_) => 0,
                Term::Iri(_) => 1,
                Term::Literal { .. } => 2,
            }
        };

        match type_ord(self).cmp(&type_ord(other)) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // Same type: compare content
        match (self, other) {
            (Term::Iri(a), Term::Iri(b)) => a.cmp(b),
            (Term::BlankNode(a), Term::BlankNode(b)) => a.cmp(b),
            (
                Term::Literal {
                    value: v1,
                    datatype: d1,
                    language: l1,
                },
                Term::Literal {
                    value: v2,
                    datatype: d2,
                    language: l2,
                },
            ) => (d1, l1, v1).cmp(&(d2, l2, v2)),
            _ => Ordering::Equal, // Should not happen
        }
    }
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Iri(iri) => write!(f, "<{iri}>"),
            Term::BlankNode(id) => write!(f, "{id}"),
            Term::Literal {
                value,
                datatype,
                language,
            } => {
                write!(f, "\"{}\"", value.lexical())?;
                if let Some(lang) = language {
                    write!(f, "@{lang}")
                } else if !datatype.is_xsd_string() {
                    write!(f, "^^<{}>", datatype.as_iri())
                } else {
                    Ok(())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blank_id() {
        let id = BlankId::new("b0");
        assert_eq!(id.as_str(), "b0");
        assert_eq!(id.to_ntriples(), "_:b0");
        assert_eq!(format!("{id}"), "_:b0");
    }

    #[test]
    fn test_term_constructors() {
        let iri = Term::iri("http://example.org/foo");
        assert!(iri.is_iri());
        assert_eq!(iri.as_iri(), Some("http://example.org/foo"));

        let blank = Term::blank("b0");
        assert!(blank.is_blank());

        let string = Term::string("hello");
        assert!(string.is_literal());

        let lang = Term::lang_string("bonjour", "fr");
        assert!(lang.is_literal());
        let (_, dt, l) = lang.as_literal().unwrap();
        assert!(dt.is_lang_string());
        assert_eq!(l, Some("fr"));
    }

    #[test]
    fn test_literal_values() {
        let s = LiteralValue::string("test");
        assert_eq!(s.lexical(), "test");

        let b = LiteralValue::Boolean(true);
        assert_eq!(b.lexical(), "true");

        let i = LiteralValue::Integer(42);
        assert_eq!(i.lexical(), "42");

        let d = LiteralValue::Double(3.13);
        assert!(d.lexical().starts_with("3.13"));

        let nan = LiteralValue::Double(f64::NAN);
        assert_eq!(nan.lexical(), "NaN");

        let inf = LiteralValue::Double(f64::INFINITY);
        assert_eq!(inf.lexical(), "INF");

        let neg_inf = LiteralValue::Double(f64::NEG_INFINITY);
        assert_eq!(neg_inf.lexical(), "-INF");
    }

    #[test]
    fn test_term_ordering() {
        // Blank nodes < IRIs < Literals
        let blank = Term::blank("b0");
        let iri = Term::iri("http://example.org");
        let lit = Term::string("hello");

        assert!(blank < iri);
        assert!(iri < lit);
        assert!(blank < lit);

        // IRIs ordered lexicographically
        let iri_a = Term::iri("http://a.org");
        let iri_b = Term::iri("http://b.org");
        assert!(iri_a < iri_b);
    }

    #[test]
    fn test_term_display() {
        assert_eq!(
            format!("{}", Term::iri("http://example.org")),
            "<http://example.org>"
        );
        assert_eq!(format!("{}", Term::blank("b0")), "_:b0");
        assert_eq!(format!("{}", Term::string("hello")), "\"hello\"");
        assert_eq!(
            format!("{}", Term::lang_string("bonjour", "fr")),
            "\"bonjour\"@fr"
        );
        assert_eq!(
            format!("{}", Term::integer(42)),
            "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
    }

    #[test]
    fn test_double_equality_with_nan() {
        // NaN values should be equal using bit comparison
        let nan1 = LiteralValue::Double(f64::NAN);
        let nan2 = LiteralValue::Double(f64::NAN);
        assert_eq!(nan1, nan2);

        let t1 = Term::double(f64::NAN);
        let t2 = Term::double(f64::NAN);
        assert_eq!(t1, t2);
    }
}
