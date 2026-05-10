//! Function dispatch - routes Function to specialized implementations
//!
//! This module provides the `Function::eval` method and `eval_function_to_bool` helper.

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function};

use super::value::ComparableValue;
use crate::ir::{ArithmeticOp, CompareOp};

use super::{
    arithmetic, cast, conditional, datetime, fluree, fulltext, geo, hash, logical, numeric, rdf,
    string, types, uuid, vector,
};

impl Function {
    /// Evaluate this function to its value.
    ///
    /// This is THE entry point for function evaluation. All functions go through here.
    /// For boolean context, use `eval_to_bool` which calls this and applies EBV.
    ///
    /// Generic over `RowAccess` to support both `RowView` (batch rows) and
    /// `BindingRow` (pre-batch filtering).
    pub fn eval<R: RowAccess>(
        &self,
        args: &[Expression],
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<Option<ComparableValue>> {
        match self {
            // Comparison operators
            Function::Eq => CompareOp::Eq.eval(args, row, ctx),
            Function::Ne => CompareOp::Ne.eval(args, row, ctx),
            Function::Lt => CompareOp::Lt.eval(args, row, ctx),
            Function::Le => CompareOp::Le.eval(args, row, ctx),
            Function::Gt => CompareOp::Gt.eval(args, row, ctx),
            Function::Ge => CompareOp::Ge.eval(args, row, ctx),

            // Arithmetic operators
            Function::Add => ArithmeticOp::Add.eval(args, row, ctx),
            Function::Sub => ArithmeticOp::Sub.eval(args, row, ctx),
            Function::Mul => ArithmeticOp::Mul.eval(args, row, ctx),
            Function::Div => ArithmeticOp::Div.eval(args, row, ctx),
            Function::Negate => arithmetic::eval_negate(args, row, ctx),

            // Logical operators
            Function::And => logical::eval_and(args, row, ctx),
            Function::Or => logical::eval_or(args, row, ctx),
            Function::Not => logical::eval_not(args, row, ctx),
            Function::In => logical::eval_in(args, row, ctx),
            Function::NotIn => logical::eval_not_in(args, row, ctx),

            // String functions
            Function::Str => string::eval_str(args, row, ctx),
            Function::Lang => string::eval_lang(args, row, ctx),
            Function::Lcase => string::eval_lcase(args, row, ctx),
            Function::Ucase => string::eval_ucase(args, row, ctx),
            Function::Strlen => string::eval_strlen(args, row, ctx),
            Function::Contains => string::eval_contains(args, row, ctx),
            Function::StrStarts => string::eval_str_starts(args, row, ctx),
            Function::StrEnds => string::eval_str_ends(args, row, ctx),
            Function::Regex => string::eval_regex(args, row, ctx),
            Function::Concat => string::eval_concat(args, row, ctx),
            Function::StrBefore => string::eval_str_before(args, row, ctx),
            Function::StrAfter => string::eval_str_after(args, row, ctx),
            Function::Replace => string::eval_replace(args, row, ctx),
            Function::Substr => string::eval_substr(args, row, ctx),
            Function::EncodeForUri => string::eval_encode_for_uri(args, row, ctx),
            Function::StrDt => string::eval_str_dt(args, row, ctx),
            Function::StrLang => string::eval_str_lang(args, row, ctx),

            // Numeric functions
            Function::Abs => numeric::eval_abs(args, row, ctx),
            Function::Round => numeric::eval_round(args, row, ctx),
            Function::Ceil => numeric::eval_ceil(args, row, ctx),
            Function::Floor => numeric::eval_floor(args, row, ctx),
            Function::Rand => numeric::eval_rand(args),

            // DateTime functions
            Function::Now => datetime::eval_now(args),
            Function::Year => datetime::eval_year(args, row, ctx),
            Function::Month => datetime::eval_month(args, row, ctx),
            Function::Day => datetime::eval_day(args, row, ctx),
            Function::Hours => datetime::eval_hours(args, row, ctx),
            Function::Minutes => datetime::eval_minutes(args, row, ctx),
            Function::Seconds => datetime::eval_seconds(args, row, ctx),
            Function::Tz => datetime::eval_tz(args, row, ctx),
            Function::Timezone => datetime::eval_timezone(args, row, ctx),

            // Type-checking functions
            Function::Bound => types::eval_bound(args, row),
            Function::IsIri => types::eval_is_iri(args, row, ctx),
            Function::IsLiteral => types::eval_is_literal(args, row, ctx),
            Function::IsNumeric => types::eval_is_numeric(args, row, ctx),
            Function::IsBlank => types::eval_is_blank(args, row, ctx),

            // RDF term functions
            Function::Datatype => rdf::eval_datatype(args, row, ctx),
            Function::LangMatches => rdf::eval_lang_matches(args, row, ctx),
            Function::SameTerm => rdf::eval_same_term(args, row, ctx),
            Function::Iri => rdf::eval_iri(args, row, ctx),
            Function::Bnode => rdf::eval_bnode(args, row, ctx),

            // Conditional functions
            Function::If => conditional::eval_if(args, row, ctx),
            Function::Coalesce => conditional::eval_coalesce(args, row, ctx),

            // Hash functions
            Function::Md5 => hash::eval_md5(args, row, ctx),
            Function::Sha1 => hash::eval_sha1(args, row, ctx),
            Function::Sha256 => hash::eval_sha256(args, row, ctx),
            Function::Sha384 => hash::eval_sha384(args, row, ctx),
            Function::Sha512 => hash::eval_sha512(args, row, ctx),

            // UUID functions — 1 micro-fuel each
            Function::Uuid => {
                if let Some(ctx) = ctx {
                    ctx.tracker.consume_fuel(1)?;
                }
                uuid::eval_uuid(args)
            }
            Function::StrUuid => {
                if let Some(ctx) = ctx {
                    ctx.tracker.consume_fuel(1)?;
                }
                uuid::eval_struuid(args)
            }

            // Vector similarity — 2 micro-fuel each (per-dim float math)
            Function::DotProduct => {
                if let Some(ctx) = ctx {
                    ctx.tracker.consume_fuel(2)?;
                }
                vector::eval_dot_product(args, row, ctx)
            }
            Function::CosineSimilarity => {
                if let Some(ctx) = ctx {
                    ctx.tracker.consume_fuel(2)?;
                }
                vector::eval_cosine_similarity(args, row, ctx)
            }
            Function::EuclideanDistance => {
                if let Some(ctx) = ctx {
                    ctx.tracker.consume_fuel(2)?;
                }
                vector::eval_euclidean_distance(args, row, ctx)
            }

            // Geospatial distance — 1 micro-fuel
            Function::GeofDistance => {
                if let Some(ctx) = ctx {
                    ctx.tracker.consume_fuel(1)?;
                }
                geo::eval_geof_distance(args, row, ctx)
            }

            // Fulltext scoring — 5 micro-fuel (touches multiple corpus stats per match)
            Function::Fulltext => {
                if let Some(ctx) = ctx {
                    ctx.tracker.consume_fuel(5)?;
                }
                fulltext::eval_fulltext(args, row, ctx)
            }

            // Fluree-specific functions
            Function::T => fluree::eval_t(args, row),
            Function::Op => fluree::eval_op(args, row),

            // XSD datatype constructor (cast) functions — W3C SPARQL 1.1 §17.5
            // SPARQL-only: JSON-LD queries do not produce these (casts are a SPARQL concept).
            Function::XsdBoolean => cast::eval_xsd_boolean(args, row, ctx),
            Function::XsdInteger => cast::eval_xsd_integer(args, row, ctx),
            Function::XsdFloat => cast::eval_xsd_float(args, row, ctx),
            Function::XsdDouble => cast::eval_xsd_double(args, row, ctx),
            Function::XsdDecimal => cast::eval_xsd_decimal(args, row, ctx),
            Function::XsdString => cast::eval_xsd_string(args, row, ctx),

            // Unknown function
            Function::Custom(name) => Err(QueryError::InvalidFilter(format!(
                "Unknown function: {name}"
            ))),
        }
    }

    /// Evaluate this function in boolean context using EBV.
    ///
    /// This calls `eval` and applies Effective Boolean Value (EBV) rules.
    ///
    /// Generic over `RowAccess` to support both `RowView` (batch rows) and
    /// `BindingRow` (pre-batch filtering).
    pub fn eval_to_bool<R: RowAccess>(
        &self,
        args: &[Expression],
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<bool> {
        let value = self.eval(args, row, ctx)?;
        Ok(value.is_some_and(Into::into))
    }
}
