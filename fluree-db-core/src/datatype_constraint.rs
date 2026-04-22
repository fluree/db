//! Resolved datatype constraint for literal values.
//!
//! [`DatatypeConstraint`] uses resolved [`Sid`]s rather than IRI strings,
//! making it suitable for use in the query execution and transaction layers
//! where namespace codes have already been resolved.

use crate::db::LedgerSnapshot;
use crate::sid::Sid;
use fluree_vocab::{namespaces, rdf_names, UnresolvedDatatypeConstraint};
use std::sync::{Arc, LazyLock};

/// Canonical Sid for `rdf:langString`, used by [`DatatypeConstraint::LangTag`].
static RDF_LANG_STRING_SID: LazyLock<Sid> =
    LazyLock::new(|| Sid::new(namespaces::RDF, rdf_names::LANG_STRING));

/// Constraint on the datatype of a literal value, using resolved Sids.
///
/// Either an explicit datatype or a language tag. Setting a language tag
/// implies that the datatype is `rdf:langString` (per RDF 1.1); this sum
/// type makes the illegal state (both an explicit non-`rdf:langString`
/// datatype and a language tag) unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DatatypeConstraint {
    /// Explicitly specified datatype (e.g. `xsd:integer`, `xsd:dateTime`)
    Explicit(Sid),
    /// Language tag (implies the datatype is `rdf:langString`)
    LangTag(Arc<str>),
}

impl DatatypeConstraint {
    /// The effective datatype Sid.
    ///
    /// Returns the explicit Sid for [`Explicit`](Self::Explicit), or the
    /// canonical `rdf:langString` Sid for [`LangTag`](Self::LangTag).
    pub fn datatype(&self) -> &Sid {
        match self {
            DatatypeConstraint::Explicit(sid) => sid,
            DatatypeConstraint::LangTag(_) => &RDF_LANG_STRING_SID,
        }
    }

    /// The language tag, if this is a [`LangTag`](Self::LangTag) constraint.
    pub fn lang_tag(&self) -> Option<&str> {
        match self {
            DatatypeConstraint::LangTag(tag) => Some(tag),
            DatatypeConstraint::Explicit(_) => None,
        }
    }

    /// Convert to an IRI-based constraint by resolving the Sid to a full IRI.
    ///
    /// Returns `None` if the [`Explicit`](Self::Explicit) Sid's namespace is
    /// not registered in the snapshot. [`LangTag`](Self::LangTag) always succeeds.
    pub fn to_unresolved(&self, snapshot: &LedgerSnapshot) -> Option<UnresolvedDatatypeConstraint> {
        match self {
            DatatypeConstraint::Explicit(sid) => snapshot
                .decode_sid(sid)
                .map(|iri| UnresolvedDatatypeConstraint::Explicit(Arc::from(iri.as_str()))),
            DatatypeConstraint::LangTag(tag) => {
                Some(UnresolvedDatatypeConstraint::LangTag(tag.clone()))
            }
        }
    }
}
