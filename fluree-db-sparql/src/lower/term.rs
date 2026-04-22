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

use fluree_db_core::temporal::{
    DayTimeDuration, Duration, GDay, GMonth, GMonthDay, GYear, GYearMonth, YearMonthDuration,
};
use fluree_db_core::{FlakeValue, Sid};
use fluree_db_query::binding::Binding;
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::triple::{Ref, Term, TriplePattern};
use fluree_db_query::var_registry::VarId;
use fluree_vocab::namespaces::{FLUREE_DB, XSD};
use fluree_vocab::{fluree, xsd, xsd_names};
use std::sync::Arc;

use super::{LowerError, LoweringContext, Result};

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
                    let var_id = self.vars.get_or_insert(&format!("_:{label}"));
                    Ok(Ref::Var(var_id))
                }
                BlankNodeValue::Anon => {
                    let var_id = self.vars.get_or_insert(&format!("_:b{}", self.vars.len()));
                    Ok(Ref::Var(var_id))
                }
            },
            SubjectTerm::QuotedTriple(_qt) => {
                // This path is reached when a quoted triple appears in a context
                // other than a top-level BGP subject with f:t/f:op predicates.
                //
                // Supported case (handled in lower_bgp_with_rdf_star):
                //   << ex:s ex:p ?o >> f:t ?t ; f:op ?op .
                //
                // Unsupported cases that reach this error:
                //   - Nested quoted triples: << << ex:s ex:p ?o >> ex:annotatedBy ?who >> ...
                //   - Quoted triples in property paths: ?s ex:path+/<< ex:s ex:p ?o >> ...
                //   - Quoted triples converted to generic Term in unsupported contexts
                //
                // Full RDF-star support would require reifying quoted triples.
                Err(LowerError::not_implemented(
                    "RDF-star quoted triples in this context (only top-level BGP with f:t/f:op annotations supported)",
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
                    let var_id = self.vars.get_or_insert(&format!("_:{label}"));
                    Ok(Term::Var(var_id))
                }
                BlankNodeValue::Anon => {
                    let var_id = self.vars.get_or_insert(&format!("_:b{}", self.vars.len()));
                    Ok(Term::Var(var_id))
                }
            },
        }
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
            LiteralValue::Decimal(d) => {
                let val: f64 = d
                    .parse()
                    .map_err(|_| LowerError::invalid_decimal(d.as_ref(), lit.span))?;
                FlakeValue::Double(val)
            }
            LiteralValue::Double(d) => FlakeValue::Double(*d),
            LiteralValue::Boolean(b) => FlakeValue::Boolean(*b),
        };
        Ok(Term::Value(value))
    }

    pub(super) fn lower_typed_literal(&self, value: &str, datatype: &Iri) -> Result<FlakeValue> {
        let dt_iri = self.expand_iri(datatype)?;

        match dt_iri.as_str() {
            xsd::STRING => Ok(FlakeValue::String(value.to_string())),
            xsd::INTEGER | xsd::INT | xsd::LONG => {
                let i: i64 = value
                    .parse()
                    .map_err(|_| LowerError::invalid_integer(value, datatype.span))?;
                Ok(FlakeValue::Long(i))
            }
            xsd::DECIMAL | xsd::DOUBLE | xsd::FLOAT => {
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
                Ok(FlakeValue::Vector(arr))
            }
            _ => {
                // Default to string for unknown datatypes
                Ok(FlakeValue::String(value.to_string()))
            }
        }
    }

    pub(super) fn expand_iri(&self, iri: &Iri) -> Result<String> {
        match &iri.value {
            IriValue::Full(s) => {
                // Check for common mistake: <prefix:local> instead of prefix:local
                // This happens when users wrap a prefixed name in angle brackets.
                // We detect this by checking if the IRI looks like "prefix:local"
                // where "prefix" matches a declared PREFIX.
                if !s.contains("://") {
                    if let Some(colon_pos) = s.find(':') {
                        let potential_prefix = &s[..colon_pos];
                        if let Some(ns) = self.prefixes.get(potential_prefix) {
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

                // Handle relative IRIs
                if let Some(base) = &self.base {
                    if !s.contains("://") && !s.starts_with('#') {
                        return Ok(format!("{base}{s}"));
                    }
                }
                Ok(s.to_string())
            }
            IriValue::Prefixed { prefix, local } => {
                let ns = self
                    .prefixes
                    .get(prefix.as_ref())
                    .ok_or_else(|| LowerError::undefined_prefix(prefix.clone(), iri.span))?;
                Ok(format!("{ns}{local}"))
            }
        }
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
                Ok(Binding::Sid(sid))
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
                LiteralValue::Integer(i) => Ok(Binding::lit(
                    FlakeValue::Long(*i),
                    Sid::new(XSD, xsd_names::LONG),
                )),
                LiteralValue::Double(d) => Ok(Binding::lit(
                    FlakeValue::Double(*d),
                    Sid::new(XSD, xsd_names::DOUBLE),
                )),
                LiteralValue::Boolean(b) => Ok(Binding::lit(
                    FlakeValue::Boolean(*b),
                    Sid::new(XSD, xsd_names::BOOLEAN),
                )),
                LiteralValue::Decimal(d) => {
                    let val: f64 = d
                        .parse()
                        .map_err(|_| LowerError::invalid_decimal(d.as_ref(), lit.span))?;
                    Ok(Binding::lit(
                        FlakeValue::Double(val),
                        Sid::new(XSD, xsd_names::DECIMAL),
                    ))
                }
                LiteralValue::Typed { value, datatype } => {
                    let fv = self.lower_typed_literal(value, datatype)?;
                    let dt_iri = self.expand_iri(datatype)?;
                    let dt_sid = if dt_iri == fluree::EMBEDDING_VECTOR {
                        Sid::new(FLUREE_DB, "vector")
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
        }
    }
}
