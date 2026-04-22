//! Polymorphic query input type
//!
//! Allows `query` to accept either JSON-LD or SPARQL queries
//! through a single entrypoint with ergonomic `From` conversions.

use serde_json::Value as JsonValue;

/// Polymorphic query input: JSON-LD or SPARQL.
///
/// This enum enables a single query entrypoint that accepts multiple syntaxes.
/// Use the `From` implementations for ergonomic construction.
///
/// # Examples
///
/// ```ignore
/// use fluree_db_api::view::QueryInput;
/// use serde_json::json;
///
/// // JSON-LD query (explicit)
/// let input = QueryInput::JsonLd(&json!({"select": ["?s"], "where": [["?s", "?p", "?o"]]}));
///
/// // Via From<&JsonValue>
/// let query = json!({"select": ["?s"], "where": [["?s", "?p", "?o"]]});
/// let input: QueryInput = (&query).into();
///
/// // SPARQL query (explicit)
/// let input = QueryInput::Sparql("SELECT * WHERE { ?s ?p ?o }");
///
/// // Via From<&str>
/// let input: QueryInput = "SELECT * WHERE { ?s ?p ?o }".into();
/// ```
#[derive(Debug, Clone, Copy)]
pub enum QueryInput<'a> {
    /// JSON-LD query
    JsonLd(&'a JsonValue),
    /// SPARQL query string
    Sparql(&'a str),
}

impl<'a> QueryInput<'a> {
    /// Check if this is a JSON-LD query
    pub fn is_jsonld(&self) -> bool {
        matches!(self, Self::JsonLd(_))
    }

    /// Check if this is a SPARQL query
    pub fn is_sparql(&self) -> bool {
        matches!(self, Self::Sparql(_))
    }

    /// Get the JSON-LD value if this is a JSON-LD query
    pub fn as_jsonld(&self) -> Option<&'a JsonValue> {
        match self {
            Self::JsonLd(v) => Some(v),
            _ => None,
        }
    }

    /// Get the SPARQL string if this is a SPARQL query
    pub fn as_sparql(&self) -> Option<&'a str> {
        match self {
            Self::Sparql(s) => Some(s),
            _ => None,
        }
    }
}

impl<'a> From<&'a JsonValue> for QueryInput<'a> {
    fn from(v: &'a JsonValue) -> Self {
        Self::JsonLd(v)
    }
}

impl<'a> From<&'a str> for QueryInput<'a> {
    fn from(s: &'a str) -> Self {
        Self::Sparql(s)
    }
}

impl<'a> From<&'a String> for QueryInput<'a> {
    fn from(s: &'a String) -> Self {
        Self::Sparql(s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_jsonld_input() {
        let query = json!({"select": ["?s"]});
        let input = QueryInput::JsonLd(&query);

        assert!(input.is_jsonld());
        assert!(!input.is_sparql());
        assert!(input.as_jsonld().is_some());
        assert!(input.as_sparql().is_none());
    }

    #[test]
    fn test_sparql_input() {
        let input = QueryInput::Sparql("SELECT * WHERE { ?s ?p ?o }");

        assert!(!input.is_jsonld());
        assert!(input.is_sparql());
        assert!(input.as_jsonld().is_none());
        assert_eq!(input.as_sparql(), Some("SELECT * WHERE { ?s ?p ?o }"));
    }

    #[test]
    fn test_from_jsonvalue() {
        let query = json!({"select": ["?s"]});
        let input: QueryInput = (&query).into();

        assert!(input.is_jsonld());
    }

    #[test]
    fn test_from_str() {
        let input: QueryInput = "SELECT * WHERE { ?s ?p ?o }".into();

        assert!(input.is_sparql());
    }

    #[test]
    fn test_from_string() {
        let sparql = String::from("SELECT * WHERE { ?s ?p ?o }");
        let input: QueryInput = (&sparql).into();

        assert!(input.is_sparql());
    }
}
