//! Graph references: which graph of one ledger a name denotes.
//!
//! [`GraphSel`] is the single keyword table for graph names. Every surface
//! that reads a graph name as a keyword-or-IRI (`default`, `txn-meta`,
//! `config`, or an absolute graph IRI) parses it here, so the three keywords
//! have one spelling and one meaning everywhere.

use crate::graph_registry::validate_absolute_graph_iri;
use std::fmt;

/// Why a graph reference did not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RefError {
    /// Not a keyword and not an absolute IRI.
    #[error("{0}")]
    InvalidGraphIri(String),
}

/// A user graph, by absolute IRI. Constructed only through
/// [`GraphIri::parse`], so it never holds a relative or empty IRI.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GraphIri(String);

impl GraphIri {
    /// Accept `s` when it is an absolute IRI acceptable as a graph target
    /// (the rule of [`validate_absolute_graph_iri`]).
    pub fn parse(s: &str) -> Result<Self, RefError> {
        validate_absolute_graph_iri(s).map_err(RefError::InvalidGraphIri)?;
        Ok(Self(s.to_string()))
    }

    /// The IRI.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The IRI, owned.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for GraphIri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Which graph of one ledger (or graph source). The only graph-selector
/// grammar.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GraphSel {
    /// The default graph.
    Default,
    /// The ledger's transaction-metadata graph (`urn:fluree:{ledger}#txn-meta`).
    TxnMeta,
    /// The ledger's configuration graph (`urn:fluree:{ledger}#config`).
    Config,
    /// A user graph by absolute IRI.
    Named(GraphIri),
}

impl GraphSel {
    /// The keyword spellings: `default`, `txn-meta`, `config`.
    pub fn keyword(s: &str) -> Option<Self> {
        match s {
            "default" => Some(GraphSel::Default),
            "txn-meta" => Some(GraphSel::TxnMeta),
            "config" => Some(GraphSel::Config),
            _ => None,
        }
    }

    /// `"default" | "txn-meta" | "config" | <absolute IRI>`.
    pub fn parse(s: &str) -> Result<Self, RefError> {
        match Self::keyword(s) {
            Some(sel) => Ok(sel),
            None => GraphIri::parse(s).map(GraphSel::Named),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keywords_and_absolute_iris_parse() {
        assert_eq!(GraphSel::parse("default"), Ok(GraphSel::Default));
        assert_eq!(GraphSel::parse("txn-meta"), Ok(GraphSel::TxnMeta));
        assert_eq!(GraphSel::parse("config"), Ok(GraphSel::Config));
        assert_eq!(
            GraphSel::parse("http://example.org/g"),
            Ok(GraphSel::Named(GraphIri("http://example.org/g".into())))
        );
        assert_eq!(
            GraphSel::parse("urn:fluree:mydb:main#config"),
            Ok(GraphSel::Named(GraphIri(
                "urn:fluree:mydb:main#config".into()
            )))
        );
    }

    #[test]
    fn relative_and_empty_names_are_refused() {
        for bad in ["", "g1", "graphs/g1", "#frag", "1http://x"] {
            assert!(GraphSel::parse(bad).is_err(), "{bad:?} must not parse");
        }
    }
}
