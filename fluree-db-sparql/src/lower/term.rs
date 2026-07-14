//! Term and IRI lowering.
//!
//! Handles lowering of SPARQL terms (variables, IRIs, literals, blank nodes)
//! to the query engine's `Term` type, as well as IRI expansion and variable
//! registration.

use crate::ast::term::{
    BlankNodeValue, Iri, IriValue, Literal, LiteralValue, ObjectTerm, PredicateTerm, SubjectTerm,
    Term as SparqlTerm, Var,
};
use crate::ast::TriplePattern as SparqlTriplePattern;

use fluree_db_core::ns_encoding::STABLE_BLANK_NODE_LABEL_PREFIX;
use fluree_db_core::temporal::{
    DayTimeDuration, Duration, GDay, GMonth, GMonthDay, GYear, GYearMonth, YearMonthDuration,
};
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use fluree_db_query::binding::Binding;
use fluree_db_query::ir::triple::{Ref, Term, TriplePattern};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::VarId;
use fluree_vocab::namespaces::{FLUREE_DB, XSD};
use fluree_vocab::{fluree, xsd, xsd_names};
use std::sync::Arc;

use super::{LowerError, LoweringContext, Result};

/// Parse an `xsd:decimal` lexical form into an exact `FlakeValue::Decimal`.
///
/// Decimals must never round-trip through `f64`: values like `19.99` have no
/// exact binary representation, and the storage layer keys decimals on the
/// exact `BigDecimal` value.
pub(super) fn parse_decimal_value(
    value: &str,
    span: crate::span::SourceSpan,
) -> Result<FlakeValue> {
    value
        .parse::<bigdecimal::BigDecimal>()
        .map(|d| FlakeValue::Decimal(Box::new(d)))
        .map_err(|_| LowerError::invalid_decimal(value, span))
}

/// Parse an integer lexical beyond i64 into an exact `FlakeValue::BigInt`
/// (xsd:integer is unbounded).
pub(super) fn parse_big_integer_value(
    value: &str,
    span: crate::span::SourceSpan,
) -> Result<FlakeValue> {
    value
        .parse::<num_bigint::BigInt>()
        .map(|n| FlakeValue::BigInt(Box::new(n)))
        .map_err(|_| LowerError::invalid_integer(value, span))
}

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Register a SPARQL variable with the variable registry.
    pub(super) fn register_var(&mut self, v: &Var) -> VarId {
        self.vars.get_or_insert(&format!("?{}", v.name))
    }

    pub(super) fn lower_triple_pattern(
        &mut self,
        tp: &SparqlTriplePattern,
    ) -> Result<TriplePattern> {
        let s = self.lower_subject(&tp.subject)?;
        let p = self.lower_predicate(&tp.predicate)?;
        let o = self.lower_object(&tp.object)?;
        Ok(TriplePattern::new(s, p, o))
    }

    pub(super) fn lower_subject(&mut self, term: &SubjectTerm) -> Result<Ref> {
        match term {
            SubjectTerm::Var(v) => Ok(self.lower_var_ref(v)),
            SubjectTerm::Iri(iri) => self.lower_iri_ref(iri),
            SubjectTerm::BlankNode(bn) => match &bn.value {
                BlankNodeValue::Labeled(label) => {
                    // Stable Fluree blank-node ids (`_:fdb-...`) denote the
                    // stored node, so they lower to a constant like an IRI;
                    // other labels are non-distinguished variables per spec.
                    if label.starts_with(STABLE_BLANK_NODE_LABEL_PREFIX) {
                        let full_iri = format!("_:{label}");
                        return Ok(match self.encoder.encode_iri_strict(&full_iri) {
                            Some(sid) => Ref::Sid(sid),
                            None => Ref::Iri(Arc::from(full_iri)),
                        });
                    }
                    let var_id = self.vars.get_or_insert(&format!("_:{label}"));
                    Ok(Ref::Var(var_id))
                }
                BlankNodeValue::Anon => {
                    let var_id = self.vars.get_or_insert(&format!("_:[]{}", self.vars.len()));
                    Ok(Ref::Var(var_id))
                }
            },
            SubjectTerm::QuotedTriple(_qt) => {
                // This path is reached when a quoted triple appears in a
                // context without a reified-triple desugaring hook.
                //
                // Supported cases (handled elsewhere):
                //   - legacy history form `<< s p ?o >> f:t ?t` and RDF 1.2
                //     reified-triple subjects/objects in BGPs
                //     (lower_bgp_with_rdf_star / lower/annotation.rs);
                //   - standalone reified triples (`GraphPattern::
                //     AnnotationTarget`).
                //
                // Unsupported cases that reach this error (deferred per
                // burn-down decision D-1, accept-then-defer):
                //   - quoted triples as property-path subjects;
                //   - quoted triples as the reifier subject of rdf:reifies;
                //   - CONSTRUCT/UPDATE template positions.
                Err(LowerError::not_implemented(
                    "RDF-star quoted triples in this position",
                    term.span(),
                ))
            }
        }
    }

    pub(super) fn lower_predicate(&mut self, term: &PredicateTerm) -> Result<Ref> {
        match term {
            PredicateTerm::Var(v) => Ok(self.lower_var_ref(v)),
            PredicateTerm::Iri(iri) => self.lower_iri_ref(iri),
        }
    }

    pub(super) fn lower_object(&mut self, term: &ObjectTerm) -> Result<Term> {
        match term {
            SparqlTerm::Var(v) => Ok(self.lower_var(v)),
            SparqlTerm::Iri(iri) => self.lower_iri(iri),
            SparqlTerm::Literal(lit) => self.lower_literal(lit),
            SparqlTerm::BlankNode(bn) => match &bn.value {
                BlankNodeValue::Labeled(label) => {
                    // Stable Fluree blank-node ids lower to a constant
                    // (see lower_subject).
                    if label.starts_with(STABLE_BLANK_NODE_LABEL_PREFIX) {
                        let full_iri = format!("_:{label}");
                        return Ok(match self.encoder.encode_iri_strict(&full_iri) {
                            Some(sid) => Term::Sid(sid),
                            None => Term::Iri(Arc::from(full_iri)),
                        });
                    }
                    let var_id = self.vars.get_or_insert(&format!("_:{label}"));
                    Ok(Term::Var(var_id))
                }
                BlankNodeValue::Anon => {
                    let var_id = self.vars.get_or_insert(&format!("_:[]{}", self.vars.len()));
                    Ok(Term::Var(var_id))
                }
            },
            SparqlTerm::QuotedTriple(qt) => {
                // Reified-triple objects are desugared by
                // `lower_object_desugared` before this is reached on
                // the BGP/annotation paths; positions without a
                // desugaring context (e.g. property-path objects)
                // defer cleanly.
                Err(LowerError::not_implemented(
                    "RDF 1.2 reified triples (`<< s p o >>`) in this position",
                    qt.span,
                ))
            }
        }
    }

    /// Constraint-preserving sibling to [`Self::lower_object`].
    ///
    /// Returns `(Term, Option<DatatypeConstraint>)` so callers that
    /// build `TriplePattern { dtc, .. }` can attach a precise scan
    /// filter for literal-typed objects. The constraint is `Some` only
    /// for literal terms — variable, IRI, and blank-node objects
    /// return `None` (their identity is fully captured by `Term`).
    ///
    /// Used by edge-annotation lowering paths (annotation block body
    /// and triple-term) so that:
    /// - plain strings match only `xsd:string` flakes,
    /// - typed literals match only their exact datatype,
    /// - language-tagged strings match only the same language tag.
    ///
    /// `lower_object`'s existing behavior (no constraint) is preserved
    /// for non-annotated triples to avoid changing scan semantics on
    /// the broader query surface.
    pub(super) fn lower_object_with_constraint(
        &mut self,
        term: &ObjectTerm,
    ) -> Result<(Term, Option<DatatypeConstraint>)> {
        match term {
            SparqlTerm::Var(v) => Ok((self.lower_var(v), None)),
            SparqlTerm::Iri(iri) => Ok((self.lower_iri(iri)?, None)),
            SparqlTerm::Literal(lit) => self.lower_literal_with_constraint(lit),
            SparqlTerm::BlankNode(_) => Ok((self.lower_object(term)?, None)),
            SparqlTerm::QuotedTriple(_) => Ok((self.lower_object(term)?, None)),
        }
    }

    /// Like [`Self::lower_literal`] but also returns the
    /// datatype/language constraint that pins the lexical value to a
    /// specific RDF datatype or language tag.
    fn lower_literal_with_constraint(
        &mut self,
        lit: &Literal,
    ) -> Result<(Term, Option<DatatypeConstraint>)> {
        let (value, dtc) = match &lit.value {
            LiteralValue::Simple(s) => (
                FlakeValue::String(s.to_string()),
                DatatypeConstraint::Explicit(Sid::new(XSD, xsd_names::STRING)),
            ),
            LiteralValue::LangTagged { value, lang } => (
                FlakeValue::String(value.to_string()),
                DatatypeConstraint::LangTag(lang.clone()),
            ),
            LiteralValue::Typed { value, datatype } => {
                let fv = self.lower_typed_literal(value, datatype)?;
                // Resolve the datatype IRI to its canonical SID via the
                // encoder. For custom (unencoded) datatypes the encoder
                // returns None — fall back to `xsd:string`, matching
                // the storage-side fallback in `term_to_binding`.
                let dt_iri = self.expand_iri(datatype)?;
                let dt_sid = self
                    .encoder
                    .encode_iri_strict(&dt_iri)
                    .unwrap_or_else(|| Sid::new(XSD, xsd_names::STRING));
                (fv, DatatypeConstraint::Explicit(dt_sid))
            }
            LiteralValue::Integer(i) => (
                FlakeValue::Long(*i),
                DatatypeConstraint::Explicit(Sid::new(XSD, xsd_names::INTEGER)),
            ),
            LiteralValue::BigInteger(s) => (
                parse_big_integer_value(s, lit.span)?,
                DatatypeConstraint::Explicit(Sid::new(XSD, xsd_names::INTEGER)),
            ),
            LiteralValue::Decimal(d) => (
                // Exact: never round-trip xsd:decimal through f64 (see
                // `parse_decimal_value`). Matches the sibling `lower_literal`.
                parse_decimal_value(d, lit.span)?,
                DatatypeConstraint::Explicit(Sid::new(XSD, xsd_names::DECIMAL)),
            ),
            LiteralValue::Double(d) => (
                FlakeValue::Double(*d),
                DatatypeConstraint::Explicit(Sid::new(XSD, xsd_names::DOUBLE)),
            ),
            LiteralValue::Boolean(b) => (
                FlakeValue::Boolean(*b),
                DatatypeConstraint::Explicit(Sid::new(XSD, xsd_names::BOOLEAN)),
            ),
        };
        Ok((Term::Value(value), Some(dtc)))
    }

    pub(super) fn lower_var(&mut self, var: &Var) -> Term {
        Term::Var(self.register_var(var))
    }

    pub(super) fn lower_var_ref(&mut self, var: &Var) -> Ref {
        Ref::Var(self.register_var(var))
    }

    pub(super) fn lower_iri(&mut self, iri: &Iri) -> Result<Term> {
        let full_iri = self.expand_iri(iri)?;
        if let Some(sid) = self.encoder.encode_iri_strict(&full_iri) {
            Ok(Term::Sid(sid))
        } else {
            Ok(Term::Iri(Arc::from(full_iri)))
        }
    }

    pub(super) fn lower_iri_ref(&mut self, iri: &Iri) -> Result<Ref> {
        let full_iri = self.expand_iri(iri)?;
        if let Some(sid) = self.encoder.encode_iri_strict(&full_iri) {
            Ok(Ref::Sid(sid))
        } else {
            Ok(Ref::Iri(Arc::from(full_iri)))
        }
    }

    fn lower_literal(&self, lit: &Literal) -> Result<Term> {
        let value = match &lit.value {
            LiteralValue::Simple(s) => FlakeValue::String(s.to_string()),
            LiteralValue::LangTagged { value, .. } => {
                // Language-tagged strings become plain strings for now
                FlakeValue::String(value.to_string())
            }
            LiteralValue::Typed { value, datatype } => self.lower_typed_literal(value, datatype)?,
            LiteralValue::Integer(i) => FlakeValue::Long(*i),
            LiteralValue::BigInteger(s) => parse_big_integer_value(s, lit.span)?,
            LiteralValue::Decimal(d) => parse_decimal_value(d, lit.span)?,
            LiteralValue::Double(d) => FlakeValue::Double(*d),
            LiteralValue::Boolean(b) => FlakeValue::Boolean(*b),
        };
        Ok(Term::Value(value))
    }

    pub(super) fn lower_typed_literal(&self, value: &str, datatype: &Iri) -> Result<FlakeValue> {
        let dt_iri = self.expand_iri(datatype)?;

        match dt_iri.as_str() {
            xsd::STRING => Ok(FlakeValue::String(value.to_string())),
            // xsd:integer is unbounded: promote past i64 instead of erroring.
            xsd::INTEGER => match value.parse::<i64>() {
                Ok(i) => Ok(FlakeValue::Long(i)),
                Err(_) => parse_big_integer_value(value, datatype.span),
            },
            // xsd:int / xsd:long are bounded; out-of-range is a lexical error.
            xsd::INT | xsd::LONG => {
                let i: i64 = value
                    .parse()
                    .map_err(|_| LowerError::invalid_integer(value, datatype.span))?;
                Ok(FlakeValue::Long(i))
            }
            xsd::DECIMAL => parse_decimal_value(value, datatype.span),
            xsd::DOUBLE | xsd::FLOAT => {
                let d: f64 = value
                    .parse()
                    .map_err(|_| LowerError::invalid_decimal(value, datatype.span))?;
                Ok(FlakeValue::Double(d))
            }
            xsd::BOOLEAN => {
                let b = value == "true" || value == "1";
                Ok(FlakeValue::Boolean(b))
            }
            // Temporal types: dateTime, date, time
            xsd::DATE_TIME => {
                let dt = fluree_db_core::temporal::DateTime::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:dateTime", &e, datatype.span)
                })?;
                Ok(FlakeValue::DateTime(Box::new(dt)))
            }
            xsd::DATE => {
                let d = fluree_db_core::temporal::Date::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:date", &e, datatype.span)
                })?;
                Ok(FlakeValue::Date(Box::new(d)))
            }
            xsd::TIME => {
                let t = fluree_db_core::temporal::Time::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:time", &e, datatype.span)
                })?;
                Ok(FlakeValue::Time(Box::new(t)))
            }
            // Calendar fragment types
            xsd::G_YEAR => {
                let g = GYear::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:gYear", &e, datatype.span)
                })?;
                Ok(FlakeValue::GYear(Box::new(g)))
            }
            xsd::G_YEAR_MONTH => {
                let g = GYearMonth::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:gYearMonth", &e, datatype.span)
                })?;
                Ok(FlakeValue::GYearMonth(Box::new(g)))
            }
            xsd::G_MONTH => {
                let g = GMonth::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:gMonth", &e, datatype.span)
                })?;
                Ok(FlakeValue::GMonth(Box::new(g)))
            }
            xsd::G_DAY => {
                let g = GDay::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:gDay", &e, datatype.span)
                })?;
                Ok(FlakeValue::GDay(Box::new(g)))
            }
            xsd::G_MONTH_DAY => {
                let g = GMonthDay::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:gMonthDay", &e, datatype.span)
                })?;
                Ok(FlakeValue::GMonthDay(Box::new(g)))
            }
            // Duration types
            xsd::DURATION => {
                let d = Duration::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:duration", &e, datatype.span)
                })?;
                Ok(FlakeValue::Duration(Box::new(d)))
            }
            xsd::DAY_TIME_DURATION => {
                let d = DayTimeDuration::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:dayTimeDuration", &e, datatype.span)
                })?;
                Ok(FlakeValue::DayTimeDuration(Box::new(d)))
            }
            xsd::YEAR_MONTH_DURATION => {
                let d = YearMonthDuration::parse(value).map_err(|e| {
                    LowerError::invalid_literal(value, "xsd:yearMonthDuration", &e, datatype.span)
                })?;
                Ok(FlakeValue::YearMonthDuration(Box::new(d)))
            }
            fluree::EMBEDDING_VECTOR => {
                // Parse JSON array string "[0.1, 0.2, ...]" into Vec<f64>
                let arr: Vec<f64> = serde_json::from_str(value).map_err(|e| {
                    LowerError::invalid_literal(
                        value,
                        "f:embeddingVector",
                        e.to_string(),
                        datatype.span,
                    )
                })?;
                Ok(FlakeValue::Vector(arr.into()))
            }
            _ => {
                // Default to string for unknown datatypes
                Ok(FlakeValue::String(value.to_string()))
            }
        }
    }

    pub(super) fn expand_iri(&self, iri: &Iri) -> Result<String> {
        expand_iri_with(&self.prefixes, self.base.as_deref(), iri)
    }

    /// Convert a SPARQL term to a Binding (for VALUES rows).
    pub(super) fn term_to_binding(&mut self, term: &SparqlTerm) -> Result<Binding> {
        match term {
            SparqlTerm::Iri(iri) => {
                let full_iri = self.expand_iri(iri)?;
                let sid = self
                    .encoder
                    .encode_iri(&full_iri)
                    .ok_or_else(|| LowerError::unknown_namespace(&full_iri, iri.span))?;
                Ok(Binding::sid(sid))
            }
            SparqlTerm::Literal(lit) => match &lit.value {
                LiteralValue::Simple(s) => Ok(Binding::lit(
                    FlakeValue::String(s.to_string()),
                    Sid::new(XSD, xsd_names::STRING),
                )),
                LiteralValue::LangTagged { value, lang } => Ok(Binding::lit_lang(
                    FlakeValue::String(value.to_string()),
                    lang.clone(),
                )),
                // RDF 1.1: a bare integer literal is xsd:integer. Storage,
                // the Turtle parser, arithmetic results, and triple-pattern
                // lowering all agree on xsd:integer — tagging VALUES rows
                // xsd:long made the same number two distinct terms in
                // term-identity contexts (DISTINCT/GROUP BY/sameTerm), #1319.
                LiteralValue::Integer(i) => Ok(Binding::lit(
                    FlakeValue::Long(*i),
                    Sid::new(XSD, xsd_names::INTEGER),
                )),
                LiteralValue::Double(d) => Ok(Binding::lit(
                    FlakeValue::Double(*d),
                    Sid::new(XSD, xsd_names::DOUBLE),
                )),
                LiteralValue::Boolean(b) => Ok(Binding::lit(
                    FlakeValue::Boolean(*b),
                    Sid::new(XSD, xsd_names::BOOLEAN),
                )),
                LiteralValue::BigInteger(s) => Ok(Binding::lit(
                    parse_big_integer_value(s, lit.span)?,
                    Sid::new(XSD, xsd_names::INTEGER),
                )),
                LiteralValue::Decimal(d) => Ok(Binding::lit(
                    parse_decimal_value(d, lit.span)?,
                    Sid::new(XSD, xsd_names::DECIMAL),
                )),
                LiteralValue::Typed { value, datatype } => {
                    let fv = self.lower_typed_literal(value, datatype)?;
                    let dt_iri = self.expand_iri(datatype)?;
                    // Bind the DECLARED datatype: Binding::Lit equality
                    // includes the datatype, so labeling every typed literal
                    // xsd:string made VALUES constants like
                    // "…"^^xsd:integer unable to match stored values.
                    let dt_sid = if dt_iri == fluree::EMBEDDING_VECTOR {
                        Sid::new(FLUREE_DB, "vector")
                    } else if let Some(sid) = self.encoder.encode_iri_strict(&dt_iri) {
                        sid
                    } else {
                        Sid::new(XSD, xsd_names::STRING)
                    };
                    Ok(Binding::lit(fv, dt_sid))
                }
            },
            SparqlTerm::Var(_) => {
                // Variables shouldn't appear in VALUES data
                Ok(Binding::Unbound)
            }
            SparqlTerm::BlankNode(_) => {
                // Blank nodes in VALUES treated as unbound
                Ok(Binding::Unbound)
            }
            SparqlTerm::QuotedTriple(qt) => Err(LowerError::not_implemented(
                "RDF 1.2 reified triples (`<< s p o >>`) as VALUES data",
                qt.span,
            )),
        }
    }
}

/// Expand a SPARQL IRI (full or prefixed) to an absolute IRI string using a
/// prologue environment (prefix map + optional BASE).
///
/// Free function so callers without a full [`LoweringContext`] (e.g. dataset
/// clause resolution, which runs before/without an encoder) share the exact
/// same expansion semantics:
///
/// - Prefixed names expand against `prefixes` (whose namespaces the caller
///   must already have base-resolved — see `prologue_environment`).
/// - Full IRI references resolve against `base` per RFC 3986 §5: `<>` → the
///   base itself, `<#x>` → base + fragment, `<data.ttl>` → sibling of the
///   base document. Absolute references (any valid scheme, including `urn:` /
///   `did:` — not just `://` forms) pass through verbatim.
/// - Without a BASE, relative references stay as written (Fluree accepts
///   them as ledger-local names).
pub(super) fn expand_iri_with(
    prefixes: &std::collections::HashMap<Arc<str>, Arc<str>>,
    base: Option<&str>,
    iri: &Iri,
) -> Result<String> {
    match &iri.value {
        IriValue::Full(s) => {
            // Check for common mistake: <prefix:local> instead of prefix:local
            // This happens when users wrap a prefixed name in angle brackets.
            // We detect this by checking if the IRI looks like "prefix:local"
            // where "prefix" matches a declared PREFIX.
            if !s.contains("://") {
                if let Some(colon_pos) = s.find(':') {
                    let potential_prefix = &s[..colon_pos];
                    if let Some(ns) = prefixes.get(potential_prefix) {
                        let local = &s[colon_pos + 1..];
                        let expanded = format!("{ns}{local}");
                        return Err(LowerError::misused_prefix_syntax(
                            s.to_string(),
                            expanded,
                            iri.span,
                        ));
                    }
                }
            }

            // Resolve relative IRI references against the query BASE.
            if let Some(base) = base {
                if !fluree_vocab::iri::is_absolute_iri(s) {
                    return Ok(fluree_vocab::iri::resolve_iri(base, s));
                }
            }
            Ok(s.to_string())
        }
        IriValue::Prefixed { prefix, local } => {
            let ns = prefixes
                .get(prefix.as_ref())
                .ok_or_else(|| LowerError::undefined_prefix(prefix.clone(), iri.span))?;
            Ok(format!("{ns}{local}"))
        }
    }
}
