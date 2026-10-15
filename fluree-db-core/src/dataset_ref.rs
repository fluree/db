//! Typed references to graphs: which graph of one ledger a name denotes.
//!
//! A graph name is parsed once, at the surface edge, into a [`GraphSel`]: one
//! of the keywords `default`, `txn-meta` and `config`, or an absolute
//! [`GraphIri`]. [`GraphSel::keyword`] is the only place the keywords are
//! spelled. Resolution against a ledger's graph registry is not done here.

use crate::graph_registry::{
    validate_absolute_graph_iri, CONFIG_GRAPH_ID, DEFAULT_GRAPH_ID, TXN_META_GRAPH_ID,
};
use crate::ids::GraphId;
use crate::ledger_id::LedgerIdParseError;
use std::fmt;
use std::sync::Arc;

/// The error every parser in this module returns: a caller mistake, reported
/// as a bad request.
pub type RefError = LedgerIdParseError;

// ============================================================================
// GraphIri
// ============================================================================

/// An absolute IRI naming a graph.
///
/// Constructed only through [`GraphIri::parse`], so a relative or empty IRI
/// cannot name a graph (`validate_absolute_graph_iri` is the rule).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GraphIri(Arc<str>);

impl GraphIri {
    /// Accept `s` if it is an absolute IRI.
    pub fn parse(s: &str) -> Result<Self, RefError> {
        validate_absolute_graph_iri(s).map_err(RefError::new)?;
        Ok(Self(s.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The IRI as a shared string, for runtime maps keyed by graph name.
    pub fn as_arc(&self) -> &Arc<str> {
        &self.0
    }
}

impl std::ops::Deref for GraphIri {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for GraphIri {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GraphIri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for GraphIri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

// ============================================================================
// GraphSel
// ============================================================================

/// Which graph of one ledger (or graph source). The only graph-name keyword
/// table: `default`, `txn-meta` and `config` are spelled here and nowhere else.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GraphSel {
    /// The ledger's default graph (g_id 0).
    Default,
    /// The ledger's transaction-metadata graph (g_id 1).
    TxnMeta,
    /// The ledger's config graph (g_id 2).
    Config,
    /// A graph named by an absolute IRI, resolved against the ledger's graph
    /// registry. It may name a reserved slot too: a branch inherits its
    /// source's `urn:fluree:<source>#config` registration verbatim.
    Named(GraphIri),
}

impl GraphSel {
    /// The text that names this graph: its keyword, or its IRI.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Default => "default",
            Self::TxnMeta => "txn-meta",
            Self::Config => "config",
            Self::Named(iri) => iri.as_str(),
        }
    }

    /// The keyword spellings, in the order [`GraphSel::keyword`] matches them.
    pub const KEYWORDS: [&'static str; 3] = ["default", "txn-meta", "config"];

    /// The keyword `s` spells, if it is one.
    pub fn keyword(s: &str) -> Option<Self> {
        match s {
            "default" => Some(Self::Default),
            "txn-meta" => Some(Self::TxnMeta),
            "config" => Some(Self::Config),
            _ => None,
        }
    }

    /// A graph of an already-named ledger: a keyword or an absolute IRI.
    ///
    /// A relative name (`#products`) is refused rather than looked up as the
    /// literal string: the registry holds absolute IRIs, so it could only miss.
    pub fn parse(s: &str) -> Result<Self, RefError> {
        if let Some(sel) = Self::keyword(s) {
            return Ok(sel);
        }
        GraphIri::parse(s).map(Self::Named).map_err(|e| {
            RefError::new(format!(
                "Invalid graph '{s}': expected 'default', 'txn-meta', 'config' or an \
                 absolute graph IRI ({e})"
            ))
        })
    }

    /// The fixed graph id of a keyword; `None` for a named graph, whose id
    /// comes from the registry.
    pub fn reserved_g_id(&self) -> Option<GraphId> {
        match self {
            Self::Default => Some(DEFAULT_GRAPH_ID),
            Self::TxnMeta => Some(TXN_META_GRAPH_ID),
            Self::Config => Some(CONFIG_GRAPH_ID),
            Self::Named(_) => None,
        }
    }

    pub fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }
}

impl fmt::Display for GraphSel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
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
