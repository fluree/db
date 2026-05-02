//! Triple pattern types for query representation
//!
//! Defines [`Term`] (variable or constant for any position), [`Ref`] (variable or
//! constant for subject/predicate positions where literal values are invalid), and
//! [`TriplePattern`] (subject–predicate–object) used to match flakes in the database index.

use crate::var_registry::VarId;
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use fluree_vocab::rdf;
use std::sync::Arc;

/// A reference term in a triple pattern — variable, constant SID, or IRI.
///
/// Used for subject and predicate positions where literal values are not valid.
/// This makes the invariant compile-time enforced: only `Ref` can appear in s/p positions,
/// while `Term` (which additionally includes `Value`) is used for the object position.
#[derive(Clone, Debug, PartialEq)]
pub enum Ref {
    /// Variable binding
    Var(VarId),
    /// Constant SID (subject or predicate)
    Sid(Sid),
    /// Constant IRI (for cross-ledger joins where SID must be encoded per-graph)
    ///
    /// Used when an IriMatch binding is substituted into a pattern during join.
    /// The scan operator will encode this IRI for each target ledger's namespace table.
    Iri(Arc<str>),
}

impl Ref {
    /// Check if this ref is a variable
    pub fn is_var(&self) -> bool {
        matches!(self, Ref::Var(_))
    }

    /// Check if this ref is bound (not a variable)
    pub fn is_bound(&self) -> bool {
        !self.is_var()
    }

    /// Get the variable if this is a Var ref
    pub fn as_var(&self) -> Option<VarId> {
        match self {
            Ref::Var(v) => Some(*v),
            _ => None,
        }
    }

    /// Check if this ref is a SID
    pub fn is_sid(&self) -> bool {
        matches!(self, Ref::Sid(_))
    }

    /// Get the SID if this is a Sid ref
    pub fn as_sid(&self) -> Option<&Sid> {
        match self {
            Ref::Sid(s) => Some(s),
            _ => None,
        }
    }

    /// Check if this ref is an IRI
    pub fn is_iri(&self) -> bool {
        matches!(self, Ref::Iri(_))
    }

    /// Get the IRI if this is an Iri ref
    pub fn as_iri(&self) -> Option<&str> {
        match self {
            Ref::Iri(iri) => Some(iri),
            _ => None,
        }
    }

    /// Check if this ref represents the rdf:type predicate
    ///
    /// Handles both Ref::Sid (checks namespace code) and Ref::Iri (compares IRI string).
    /// Returns false for variables.
    pub fn is_rdf_type(&self) -> bool {
        match self {
            Ref::Sid(sid) => fluree_db_core::is_rdf_type(sid),
            Ref::Iri(iri) => iri.as_ref() == rdf::TYPE,
            _ => false,
        }
    }
}

impl From<Ref> for Term {
    fn from(r: Ref) -> Self {
        match r {
            Ref::Var(v) => Term::Var(v),
            Ref::Sid(s) => Term::Sid(s),
            Ref::Iri(i) => Term::Iri(i),
        }
    }
}

impl TryFrom<Term> for Ref {
    type Error = Term;

    /// Convert a Term to a Ref, failing if the Term is a Value.
    ///
    /// Returns `Err(term)` when the term is `Term::Value`, since literal values
    /// are not valid in subject or predicate positions.
    fn try_from(term: Term) -> std::result::Result<Self, Self::Error> {
        match term {
            Term::Var(v) => Ok(Ref::Var(v)),
            Term::Sid(s) => Ok(Ref::Sid(s)),
            Term::Iri(i) => Ok(Ref::Iri(i)),
            other @ Term::Value(_) => Err(other),
        }
    }
}

/// A term in a triple pattern - variable, constant SID, IRI, or constant value
#[derive(Clone, Debug, PartialEq)]
pub enum Term {
    /// Variable binding
    Var(VarId),
    /// Constant SID (subject, predicate, or ref object)
    Sid(Sid),
    /// Constant IRI (for cross-ledger joins where SID must be encoded per-graph)
    ///
    /// Used when an IriMatch binding is substituted into a pattern during join.
    /// The scan operator will encode this IRI for each target ledger's namespace table.
    Iri(Arc<str>),
    /// Constant value (literal object)
    Value(FlakeValue),
}

impl Term {
    /// Check if this term is a variable
    pub fn is_var(&self) -> bool {
        matches!(self, Term::Var(_))
    }

    /// Check if this term is bound (not a variable)
    pub fn is_bound(&self) -> bool {
        !self.is_var()
    }

    /// Get the variable if this is a Var term
    pub fn as_var(&self) -> Option<VarId> {
        match self {
            Term::Var(v) => Some(*v),
            _ => None,
        }
    }

    /// Get the SID if this is a Sid term
    pub fn as_sid(&self) -> Option<&Sid> {
        match self {
            Term::Sid(s) => Some(s),
            _ => None,
        }
    }

    /// Get the IRI if this is an Iri term
    pub fn as_iri(&self) -> Option<&str> {
        match self {
            Term::Iri(iri) => Some(iri),
            _ => None,
        }
    }

    /// Get the value if this is a Value term
    pub fn as_value(&self) -> Option<&FlakeValue> {
        match self {
            Term::Value(v) => Some(v),
            _ => None,
        }
    }

    /// Check if this term represents the rdf:type predicate
    ///
    /// Handles both Term::Sid (checks namespace code) and Term::Iri (compares IRI string).
    /// Returns false for variables and values.
    pub fn is_rdf_type(&self) -> bool {
        match self {
            Term::Sid(sid) => fluree_db_core::is_rdf_type(sid),
            Term::Iri(iri) => iri.as_ref() == rdf::TYPE,
            _ => false,
        }
    }
}

/// A triple pattern for matching flakes
///
/// Subject and predicate use [`Ref`] (variable, SID, or IRI — never a literal value).
/// Object uses [`Term`] which additionally allows literal values.
#[derive(Clone, Debug, PartialEq)]
pub struct TriplePattern {
    /// Subject ref (variable, SID, or IRI)
    pub s: Ref,
    /// Predicate ref (variable, SID, or IRI)
    pub p: Ref,
    /// Object term (variable, SID, IRI, or literal value)
    pub o: Term,
    /// Optional datatype or language-tag constraint for the object
    pub dtc: Option<DatatypeConstraint>,
}

impl TriplePattern {
    /// Create a new triple pattern with no datatype/language constraint
    pub fn new(s: Ref, p: Ref, o: Term) -> Self {
        Self { s, p, o, dtc: None }
    }

    /// Create with an explicit datatype constraint
    pub fn with_dt(s: Ref, p: Ref, o: Term, dt: Sid) -> Self {
        Self {
            s,
            p,
            o,
            dtc: Some(DatatypeConstraint::Explicit(dt)),
        }
    }

    /// Create with a language tag constraint (implies `rdf:langString` datatype)
    pub fn with_lang(s: Ref, p: Ref, o: Term, lang: impl AsRef<str>) -> Self {
        Self {
            s,
            p,
            o,
            dtc: Some(DatatypeConstraint::LangTag(Arc::from(lang.as_ref()))),
        }
    }

    /// Get the variables in this pattern (in order: s, p, o)
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = Vec::with_capacity(3);
        if let Ref::Var(v) = &self.s {
            vars.push(*v);
        }
        if let Ref::Var(v) = &self.p {
            vars.push(*v);
        }
        if let Term::Var(v) = &self.o {
            vars.push(*v);
        }
        vars
    }

    /// Check if subject is bound (not a variable)
    pub fn s_bound(&self) -> bool {
        self.s.is_bound()
    }

    /// Check if predicate is bound (not a variable)
    pub fn p_bound(&self) -> bool {
        self.p.is_bound()
    }

    /// Check if object is bound (not a variable)
    pub fn o_bound(&self) -> bool {
        self.o.is_bound()
    }

    /// Check if object is a reference (Sid, Iri, or Ref value type)
    ///
    /// Used for index selection - OPST index is preferred for ref lookups.
    /// Term::Iri is treated as a ref because it represents an IRI that will
    /// be encoded to a Sid for each target ledger.
    pub fn o_is_ref(&self) -> bool {
        matches!(
            &self.o,
            Term::Sid(_) | Term::Iri(_) | Term::Value(FlakeValue::Ref(_))
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ref_is_var() {
        let var = Ref::Var(VarId(0));
        let sid = Ref::Sid(Sid::new(1, "test"));

        assert!(var.is_var());
        assert!(!sid.is_var());

        assert!(!var.is_bound());
        assert!(sid.is_bound());
    }

    #[test]
    fn test_term_is_var() {
        let var = Term::Var(VarId(0));
        let sid = Term::Sid(Sid::new(1, "test"));
        let val = Term::Value(FlakeValue::Long(42));

        assert!(var.is_var());
        assert!(!sid.is_var());
        assert!(!val.is_var());

        assert!(!var.is_bound());
        assert!(sid.is_bound());
        assert!(val.is_bound());
    }

    #[test]
    fn test_ref_to_term_conversion() {
        let r = Ref::Sid(Sid::new(1, "test"));
        let t: Term = r.into();
        assert_eq!(t, Term::Sid(Sid::new(1, "test")));
    }

    #[test]
    fn test_term_to_ref_conversion() {
        let t = Term::Var(VarId(0));
        let r: Ref = t.try_into().unwrap();
        assert_eq!(r, Ref::Var(VarId(0)));

        let t = Term::Value(FlakeValue::Long(42));
        assert!(Ref::try_from(t).is_err());
    }

    #[test]
    fn test_triple_pattern_variables() {
        let pattern = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(1, "name")),
            Term::Var(VarId(1)),
        );

        let vars = pattern.variables();
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0], VarId(0));
        assert_eq!(vars[1], VarId(1));
    }

    #[test]
    fn test_triple_pattern_bound_checks() {
        let pattern = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(1, "name")),
            Term::Value(FlakeValue::Long(42)),
        );

        assert!(!pattern.s_bound());
        assert!(pattern.p_bound());
        assert!(pattern.o_bound());
    }
}
