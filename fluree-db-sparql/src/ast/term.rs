//! SPARQL term types with source spans.
//!
//! These types represent the different kinds of terms that can appear
//! in SPARQL queries: variables, IRIs, literals, and blank nodes.
//! All types carry source spans for precise diagnostics.

use crate::span::SourceSpan;
use std::sync::Arc;

/// A spanned node that wraps a value with its source location.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Spanned<T> {
    /// The wrapped value
    pub value: T,
    /// Source location
    pub span: SourceSpan,
}

impl<T> Spanned<T> {
    /// Create a new spanned value.
    pub fn new(value: T, span: SourceSpan) -> Self {
        Self { value, span }
    }

    /// Map the inner value, preserving the span.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Spanned<U> {
        Spanned {
            value: f(self.value),
            span: self.span,
        }
    }
}

/// A SPARQL variable (e.g., `?name` or `$name`).
///
/// The name does not include the leading `?` or `$`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Var {
    /// Variable name (without the `?` or `$` prefix)
    pub name: Arc<str>,
    /// Source span (includes the prefix)
    pub span: SourceSpan,
}

impl Var {
    /// Create a new variable.
    pub fn new(name: impl AsRef<str>, span: SourceSpan) -> Self {
        Self {
            name: Arc::from(name.as_ref()),
            span,
        }
    }
}

/// An IRI reference.
///
/// This can be either a full IRI (`<http://example.org/foo>`) or
/// a prefixed name (`ex:foo`). After parsing, the IRI is in its
/// expanded form (either the raw IRI or prefix:local for later resolution).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Iri {
    /// The IRI value (may be absolute or prefixed)
    pub value: IriValue,
    /// Source span
    pub span: SourceSpan,
}

impl Iri {
    /// Create a full IRI (from `<...>` syntax).
    pub fn full(iri: impl AsRef<str>, span: SourceSpan) -> Self {
        Self {
            value: IriValue::Full(Arc::from(iri.as_ref())),
            span,
        }
    }

    /// Create a prefixed IRI (from `prefix:local` syntax).
    pub fn prefixed(prefix: impl AsRef<str>, local: impl AsRef<str>, span: SourceSpan) -> Self {
        Self {
            value: IriValue::Prefixed {
                prefix: Arc::from(prefix.as_ref()),
                local: Arc::from(local.as_ref()),
            },
            span,
        }
    }

    /// Create a reference to `rdf:type` (the `a` keyword).
    pub fn rdf_type(span: SourceSpan) -> Self {
        Self {
            value: IriValue::Full(Arc::from(fluree_vocab::rdf::TYPE)),
            span,
        }
    }
}

/// The value of an IRI reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IriValue {
    /// Full IRI (already expanded)
    Full(Arc<str>),
    /// Prefixed name (needs expansion using PREFIX declarations)
    Prefixed {
        /// The prefix (empty string for default prefix `:local`)
        prefix: Arc<str>,
        /// The local part
        local: Arc<str>,
    },
}

/// A blank node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlankNode {
    /// The blank node value
    pub value: BlankNodeValue,
    /// Source span
    pub span: SourceSpan,
}

impl BlankNode {
    /// Create a labeled blank node (e.g., `_:label`).
    pub fn labeled(label: impl AsRef<str>, span: SourceSpan) -> Self {
        Self {
            value: BlankNodeValue::Labeled(Arc::from(label.as_ref())),
            span,
        }
    }

    /// Create an anonymous blank node (e.g., `[]` or `[ :p :o ]`).
    pub fn anon(span: SourceSpan) -> Self {
        Self {
            value: BlankNodeValue::Anon,
            span,
        }
    }
}

/// The value of a blank node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlankNodeValue {
    /// Labeled blank node (`_:label`)
    Labeled(Arc<str>),
    /// Anonymous blank node (`[]`)
    Anon,
}

/// A literal value.
#[derive(Clone, Debug, PartialEq)]
pub struct Literal {
    /// The literal value
    pub value: LiteralValue,
    /// Source span
    pub span: SourceSpan,
}

impl Literal {
    /// Create a simple string literal.
    pub fn string(value: impl AsRef<str>, span: SourceSpan) -> Self {
        Self {
            value: LiteralValue::Simple(Arc::from(value.as_ref())),
            span,
        }
    }

    /// Create a language-tagged string.
    pub fn lang_string(value: impl AsRef<str>, lang: impl AsRef<str>, span: SourceSpan) -> Self {
        Self {
            value: LiteralValue::LangTagged {
                value: Arc::from(value.as_ref()),
                lang: Arc::from(lang.as_ref()),
            },
            span,
        }
    }

    /// Create a typed literal with an IRI datatype.
    pub fn typed(value: impl AsRef<str>, datatype: Iri, span: SourceSpan) -> Self {
        Self {
            value: LiteralValue::Typed {
                value: Arc::from(value.as_ref()),
                datatype: Box::new(datatype),
            },
            span,
        }
    }

    /// Create an integer literal.
    pub fn integer(value: i64, span: SourceSpan) -> Self {
        Self {
            value: LiteralValue::Integer(value),
            span,
        }
    }

    /// Create a decimal literal.
    pub fn decimal(value: impl AsRef<str>, span: SourceSpan) -> Self {
        Self {
            value: LiteralValue::Decimal(Arc::from(value.as_ref())),
            span,
        }
    }

    /// Create a double literal.
    pub fn double(value: f64, span: SourceSpan) -> Self {
        Self {
            value: LiteralValue::Double(value),
            span,
        }
    }

    /// Create a boolean literal.
    pub fn boolean(value: bool, span: SourceSpan) -> Self {
        Self {
            value: LiteralValue::Boolean(value),
            span,
        }
    }
}

/// The value of a literal.
#[derive(Clone, Debug, PartialEq)]
pub enum LiteralValue {
    /// Simple string literal (no language tag or datatype)
    Simple(Arc<str>),
    /// Language-tagged string (e.g., `"hello"@en`)
    LangTagged {
        /// The string value
        value: Arc<str>,
        /// The language tag (e.g., "en", "en-US")
        lang: Arc<str>,
    },
    /// Typed literal (e.g., `"42"^^xsd:integer`)
    Typed {
        /// The lexical form
        value: Arc<str>,
        /// The datatype IRI
        datatype: Box<Iri>,
    },
    /// Integer literal (syntactic shorthand, implicitly xsd:integer)
    Integer(i64),
    /// Decimal literal (syntactic shorthand, implicitly xsd:decimal)
    ///
    /// Stored as string to preserve exact representation.
    Decimal(Arc<str>),
    /// Double literal (syntactic shorthand, implicitly xsd:double)
    Double(f64),
    /// Boolean literal (`true` or `false`)
    Boolean(bool),
}

/// A term in a SPARQL query (variable, IRI, literal, or blank node).
///
/// This is the unresolved form - IRIs may be prefixed and need expansion.
#[derive(Clone, Debug, PartialEq)]
pub enum Term {
    /// Variable (`?x` or `$x`)
    Var(Var),
    /// IRI (full or prefixed)
    Iri(Iri),
    /// Literal value
    Literal(Literal),
    /// Blank node
    BlankNode(BlankNode),
}

impl Term {
    /// Get the source span of this term.
    pub fn span(&self) -> SourceSpan {
        match self {
            Term::Var(v) => v.span,
            Term::Iri(i) => i.span,
            Term::Literal(l) => l.span,
            Term::BlankNode(b) => b.span,
        }
    }

    /// Check if this term is a variable.
    pub fn is_var(&self) -> bool {
        matches!(self, Term::Var(_))
    }

    /// Get the variable if this is a variable term.
    pub fn as_var(&self) -> Option<&Var> {
        match self {
            Term::Var(v) => Some(v),
            _ => None,
        }
    }

    /// Check if this term is an IRI.
    pub fn is_iri(&self) -> bool {
        matches!(self, Term::Iri(_))
    }

    /// Get the IRI if this is an IRI term.
    pub fn as_iri(&self) -> Option<&Iri> {
        match self {
            Term::Iri(i) => Some(i),
            _ => None,
        }
    }
}

/// A quoted triple (RDF-star).
///
/// Represents a triple that can be used as a subject, enabling metadata
/// annotations on statements. Syntax: `<< subject predicate object >>`
///
/// Used in history queries:
/// ```sparql
/// << ex:alice ex:age ?age >> f:t ?t ; f:op ?op .
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct QuotedTriple {
    /// The subject of the quoted triple
    pub subject: Box<SubjectTerm>,
    /// The predicate of the quoted triple
    pub predicate: PredicateTerm,
    /// The object of the quoted triple
    pub object: Box<Term>,
    /// Source span (including << and >>)
    pub span: SourceSpan,
}

impl QuotedTriple {
    /// Create a new quoted triple.
    pub fn new(
        subject: SubjectTerm,
        predicate: PredicateTerm,
        object: Term,
        span: SourceSpan,
    ) -> Self {
        Self {
            subject: Box::new(subject),
            predicate,
            object: Box::new(object),
            span,
        }
    }
}

/// A term that can appear in the subject position.
///
/// In SPARQL, subjects can be variables, IRIs, or blank nodes (not literals).
/// With RDF-star extension, subjects can also be quoted triples.
#[derive(Clone, Debug, PartialEq)]
pub enum SubjectTerm {
    Var(Var),
    Iri(Iri),
    BlankNode(BlankNode),
    /// RDF-star quoted triple
    QuotedTriple(QuotedTriple),
}

impl SubjectTerm {
    /// Get the source span.
    pub fn span(&self) -> SourceSpan {
        match self {
            SubjectTerm::Var(v) => v.span,
            SubjectTerm::Iri(i) => i.span,
            SubjectTerm::BlankNode(b) => b.span,
            SubjectTerm::QuotedTriple(q) => q.span,
        }
    }
}

impl SubjectTerm {
    /// Try to convert this SubjectTerm to a Term.
    ///
    /// Returns `None` if this is a `QuotedTriple`, which cannot be directly
    /// represented as a Term (it requires pattern expansion at the BGP level).
    ///
    /// Use this method when you need to safely convert a SubjectTerm that might
    /// be a QuotedTriple. For guaranteed non-QuotedTriple subjects, you can use
    /// the `From` impl which will panic on QuotedTriple.
    pub fn try_into_term(self) -> Option<Term> {
        match self {
            SubjectTerm::Var(v) => Some(Term::Var(v)),
            SubjectTerm::Iri(i) => Some(Term::Iri(i)),
            SubjectTerm::BlankNode(b) => Some(Term::BlankNode(b)),
            SubjectTerm::QuotedTriple(_) => None,
        }
    }
}

impl From<SubjectTerm> for Term {
    /// Convert a SubjectTerm to a Term.
    ///
    /// # Panics
    ///
    /// Panics if the SubjectTerm is a `QuotedTriple`. QuotedTriples cannot be
    /// directly converted to Terms because they require pattern expansion
    /// (the inner triple becomes a pattern, with `f:t`/`f:op` annotations
    /// becoming BIND expressions).
    ///
    /// Use [`SubjectTerm::try_into_term`] if you need safe handling of
    /// potentially QuotedTriple subjects.
    fn from(s: SubjectTerm) -> Self {
        s.try_into_term()
            .expect("QuotedTriple cannot be converted to Term directly; use try_into_term() or handle at pattern level")
    }
}

/// A term that can appear in the predicate position.
///
/// In SPARQL, predicates can be variables or IRIs (not blank nodes or literals).
/// The special keyword `a` is shorthand for `rdf:type`.
#[derive(Clone, Debug, PartialEq)]
pub enum PredicateTerm {
    Var(Var),
    Iri(Iri),
}

impl PredicateTerm {
    /// Get the source span.
    pub fn span(&self) -> SourceSpan {
        match self {
            PredicateTerm::Var(v) => v.span,
            PredicateTerm::Iri(i) => i.span,
        }
    }
}

impl From<PredicateTerm> for Term {
    fn from(p: PredicateTerm) -> Self {
        match p {
            PredicateTerm::Var(v) => Term::Var(v),
            PredicateTerm::Iri(i) => Term::Iri(i),
        }
    }
}

/// A term that can appear in the object position.
///
/// In SPARQL, objects can be any term type.
pub type ObjectTerm = Term;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_var_creation() {
        let v = Var::new("name", SourceSpan::new(0, 5));
        assert_eq!(v.name.as_ref(), "name");
        assert_eq!(v.span, SourceSpan::new(0, 5));
    }

    #[test]
    fn test_iri_full() {
        let iri = Iri::full("http://example.org/foo", SourceSpan::new(0, 25));
        assert!(matches!(iri.value, IriValue::Full(_)));
    }

    #[test]
    fn test_iri_prefixed() {
        let iri = Iri::prefixed("ex", "foo", SourceSpan::new(0, 6));
        match &iri.value {
            IriValue::Prefixed { prefix, local } => {
                assert_eq!(prefix.as_ref(), "ex");
                assert_eq!(local.as_ref(), "foo");
            }
            _ => panic!("Expected prefixed IRI"),
        }
    }

    #[test]
    fn test_literal_types() {
        let s = Literal::string("hello", SourceSpan::new(0, 7));
        assert!(matches!(s.value, LiteralValue::Simple(_)));

        let lang = Literal::lang_string("bonjour", "fr", SourceSpan::new(0, 12));
        assert!(matches!(lang.value, LiteralValue::LangTagged { .. }));

        let int = Literal::integer(42, SourceSpan::new(0, 2));
        assert!(matches!(int.value, LiteralValue::Integer(42)));

        let dec = Literal::decimal("3.14", SourceSpan::new(0, 4));
        assert!(matches!(dec.value, LiteralValue::Decimal(_)));

        let dbl = Literal::double(1.5e10, SourceSpan::new(0, 6));
        assert!(matches!(dbl.value, LiteralValue::Double(_)));

        let b = Literal::boolean(true, SourceSpan::new(0, 4));
        assert!(matches!(b.value, LiteralValue::Boolean(true)));
    }

    #[test]
    fn test_blank_node() {
        let labeled = BlankNode::labeled("b1", SourceSpan::new(0, 4));
        assert!(matches!(labeled.value, BlankNodeValue::Labeled(_)));

        let anon = BlankNode::anon(SourceSpan::new(0, 2));
        assert!(matches!(anon.value, BlankNodeValue::Anon));
    }

    #[test]
    fn test_term_span() {
        let v = Term::Var(Var::new("x", SourceSpan::new(0, 2)));
        assert_eq!(v.span(), SourceSpan::new(0, 2));

        let i = Term::Iri(Iri::full("http://example.org", SourceSpan::new(5, 25)));
        assert_eq!(i.span(), SourceSpan::new(5, 25));
    }

    #[test]
    fn test_term_conversions() {
        let v = Var::new("x", SourceSpan::new(0, 2));
        let subject = SubjectTerm::Var(v.clone());
        let term: Term = subject.into();
        assert!(term.is_var());

        let iri = Iri::full("http://example.org", SourceSpan::new(0, 20));
        let pred = PredicateTerm::Iri(iri);
        let term: Term = pred.into();
        assert!(term.is_iri());
    }
}
