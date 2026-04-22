//! Expression lowering.
//!
//! Converts SPARQL expressions (comparisons, arithmetic, function calls, etc.)
//! to the query engine's `Expression` representation.

use crate::ast::expr::{BinaryOp, Expression as AstExpression, FunctionName, UnaryOp};
use crate::ast::term::{Literal, LiteralValue};
use fluree_db_core::FlakeValue;
use fluree_db_query::ir::{Expression, FilterValue, Function};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_vocab::xsd;

use super::{LowerError, LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    pub(super) fn lower_expression(&mut self, expr: &AstExpression) -> Result<Expression> {
        match expr {
            AstExpression::Var(v) => {
                let var_id = self.register_var(v);
                Ok(Expression::Var(var_id))
            }

            AstExpression::Literal(lit) => {
                let value = self.lower_filter_value(lit)?;
                Ok(Expression::Const(value))
            }

            AstExpression::Iri(iri) => {
                // Wrap the expanded IRI string in IRI() so it evaluates to
                // ComparableValue::Iri, which in turn produces Binding::Sid
                // (or Binding::Iri for unknown IRIs).  Without this wrapper
                // the IRI was lowered as a plain string literal, causing BIND
                // to produce a Binding::Lit that can't substitute into subject
                // or predicate positions of OPTIONAL patterns.
                let full_iri = self.expand_iri(iri)?;
                Ok(Expression::Call {
                    func: Function::Iri,
                    args: vec![Expression::Const(FilterValue::String(full_iri))],
                })
            }

            AstExpression::Binary {
                op, left, right, ..
            } => match op {
                BinaryOp::And => {
                    let l = self.lower_expression(left)?;
                    let r = self.lower_expression(right)?;
                    Ok(Expression::and(vec![l, r]))
                }
                BinaryOp::Or => {
                    let l = self.lower_expression(left)?;
                    let r = self.lower_expression(right)?;
                    Ok(Expression::or(vec![l, r]))
                }
                BinaryOp::Eq => self.lower_comparison(Function::Eq, left, right),
                BinaryOp::Ne => self.lower_comparison(Function::Ne, left, right),
                BinaryOp::Lt => self.lower_comparison(Function::Lt, left, right),
                BinaryOp::Le => self.lower_comparison(Function::Le, left, right),
                BinaryOp::Gt => self.lower_comparison(Function::Gt, left, right),
                BinaryOp::Ge => self.lower_comparison(Function::Ge, left, right),
                BinaryOp::Add => self.lower_arithmetic(Function::Add, left, right),
                BinaryOp::Sub => self.lower_arithmetic(Function::Sub, left, right),
                BinaryOp::Mul => self.lower_arithmetic(Function::Mul, left, right),
                BinaryOp::Div => self.lower_arithmetic(Function::Div, left, right),
            },

            AstExpression::Unary { op, operand, .. } => match op {
                UnaryOp::Not => {
                    let inner = self.lower_expression(operand)?;
                    Ok(Expression::not(inner))
                }
                UnaryOp::Pos => self.lower_expression(operand),
                UnaryOp::Neg => {
                    let inner = self.lower_expression(operand)?;
                    Ok(Expression::negate(inner))
                }
            },

            AstExpression::FunctionCall { name, args, .. } => self.lower_function_call(name, args),

            agg @ AstExpression::Aggregate { function, span, .. } => {
                if let Some(aliases) = &self.aggregate_aliases {
                    let key = self.aggregate_key(agg)?;
                    if let Some(var_id) = aliases.get(&key) {
                        return Ok(Expression::Var(*var_id));
                    }
                }
                Err(LowerError::not_implemented(
                    format!("Aggregate function {function:?}"),
                    *span,
                ))
            }

            AstExpression::Exists { pattern, .. } => {
                let inner_patterns = self.lower_graph_pattern(pattern)?;
                Ok(Expression::Exists {
                    patterns: inner_patterns,
                    negated: false,
                })
            }
            AstExpression::NotExists { pattern, .. } => {
                let inner_patterns = self.lower_graph_pattern(pattern)?;
                Ok(Expression::Exists {
                    patterns: inner_patterns,
                    negated: true,
                })
            }

            AstExpression::In {
                expr,
                list,
                negated,
                ..
            } => {
                let lowered_expr = self.lower_expression(expr)?;
                let lowered_values: Vec<Expression> = list
                    .iter()
                    .map(|v| self.lower_expression(v))
                    .collect::<Result<Vec<_>>>()?;
                if *negated {
                    Ok(Expression::not_in_list(lowered_expr, lowered_values))
                } else {
                    Ok(Expression::in_list(lowered_expr, lowered_values))
                }
            }

            AstExpression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                let cond = self.lower_expression(condition)?;
                let then_e = self.lower_expression(then_expr)?;
                let else_e = self.lower_expression(else_expr)?;
                Ok(Expression::if_then_else(cond, then_e, else_e))
            }

            AstExpression::Coalesce { args, .. } => {
                let lowered_args: Vec<Expression> = args
                    .iter()
                    .map(|a| self.lower_expression(a))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::call(Function::Coalesce, lowered_args))
            }

            AstExpression::Bracketed { inner, .. } => {
                // Bracketed expressions just unwrap to their inner expression
                self.lower_expression(inner)
            }
        }
    }

    fn lower_comparison(
        &mut self,
        func: Function,
        left: &AstExpression,
        right: &AstExpression,
    ) -> Result<Expression> {
        let l = self.lower_expression(left)?;
        let r = self.lower_expression(right)?;
        Ok(Expression::compare(func, l, r))
    }

    fn lower_arithmetic(
        &mut self,
        func: Function,
        left: &AstExpression,
        right: &AstExpression,
    ) -> Result<Expression> {
        let l = self.lower_expression(left)?;
        let r = self.lower_expression(right)?;
        Ok(Expression::arithmetic(func, l, r))
    }

    fn lower_filter_value(&self, lit: &Literal) -> Result<FilterValue> {
        match &lit.value {
            LiteralValue::Simple(s) => Ok(FilterValue::String(s.to_string())),
            LiteralValue::LangTagged { value, .. } => Ok(FilterValue::String(value.to_string())),
            LiteralValue::Integer(i) => Ok(FilterValue::Long(*i)),
            LiteralValue::Double(d) => Ok(FilterValue::Double(*d)),
            LiteralValue::Decimal(d) => {
                let val: f64 = d
                    .parse()
                    .map_err(|_| LowerError::invalid_decimal(d.as_ref(), lit.span))?;
                Ok(FilterValue::Double(val))
            }
            LiteralValue::Boolean(b) => Ok(FilterValue::Bool(*b)),
            LiteralValue::Typed { value, datatype } => {
                let fv = self.lower_typed_literal(value, datatype)?;
                match fv {
                    FlakeValue::Long(n) => Ok(FilterValue::Long(n)),
                    FlakeValue::Double(d) => Ok(FilterValue::Double(d)),
                    FlakeValue::Boolean(b) => Ok(FilterValue::Bool(b)),
                    FlakeValue::String(s) => Ok(FilterValue::String(s)),
                    fv if fv.is_temporal() || fv.is_duration() => Ok(FilterValue::Temporal(fv)),
                    _ => Ok(FilterValue::String(value.to_string())),
                }
            }
        }
    }

    fn lower_function_call(
        &mut self,
        name: &FunctionName,
        args: &[AstExpression],
    ) -> Result<Expression> {
        let func = match name {
            // Type checking functions
            FunctionName::Bound => Function::Bound,
            FunctionName::IsIri | FunctionName::IsUri => Function::IsIri,
            FunctionName::IsBlank => Function::IsBlank,
            FunctionName::IsLiteral => Function::IsLiteral,
            FunctionName::IsNumeric => Function::IsNumeric,

            // RDF term functions
            FunctionName::Lang => Function::Lang,
            FunctionName::Datatype => Function::Datatype,

            // String functions
            FunctionName::Strlen => Function::Strlen,
            FunctionName::Substr => Function::Substr,
            FunctionName::Ucase => Function::Ucase,
            FunctionName::Lcase => Function::Lcase,
            FunctionName::Contains => Function::Contains,
            FunctionName::StrStarts => Function::StrStarts,
            FunctionName::StrEnds => Function::StrEnds,
            FunctionName::Regex => Function::Regex,
            FunctionName::Concat => Function::Concat,
            FunctionName::StrBefore => Function::StrBefore,
            FunctionName::StrAfter => Function::StrAfter,
            FunctionName::Replace => Function::Replace,
            FunctionName::StrDt => Function::StrDt,
            FunctionName::StrLang => Function::StrLang,

            // Constructor functions
            FunctionName::Iri | FunctionName::Uri => Function::Iri,
            FunctionName::BNode => Function::Bnode,

            // Numeric functions
            FunctionName::Abs => Function::Abs,
            FunctionName::Round => Function::Round,
            FunctionName::Ceil => Function::Ceil,
            FunctionName::Floor => Function::Floor,
            FunctionName::Rand => Function::Rand,

            // DateTime functions
            FunctionName::Now => Function::Now,
            FunctionName::Year => Function::Year,
            FunctionName::Month => Function::Month,
            FunctionName::Day => Function::Day,
            FunctionName::Hours => Function::Hours,
            FunctionName::Minutes => Function::Minutes,
            FunctionName::Seconds => Function::Seconds,
            FunctionName::Timezone => Function::Timezone,
            FunctionName::Tz => Function::Tz,

            // Accessor functions
            FunctionName::Str => Function::Str,
            FunctionName::EncodeForUri => Function::EncodeForUri,

            // RDF term comparison
            FunctionName::LangMatches => Function::LangMatches,
            FunctionName::SameTerm => Function::SameTerm,

            // Hash functions
            FunctionName::Md5 => Function::Md5,
            FunctionName::Sha1 => Function::Sha1,
            FunctionName::Sha256 => Function::Sha256,
            FunctionName::Sha384 => Function::Sha384,
            FunctionName::Sha512 => Function::Sha512,

            // UUID functions
            FunctionName::Uuid => Function::Uuid,
            FunctionName::StrUuid => Function::StrUuid,

            // Control flow (usually handled as special expression forms)
            FunctionName::If => Function::If,
            FunctionName::Coalesce => Function::Coalesce,

            // Vector similarity functions
            FunctionName::DotProduct => Function::DotProduct,
            FunctionName::CosineSimilarity => Function::CosineSimilarity,
            FunctionName::EuclideanDistance => Function::EuclideanDistance,

            // Extension functions
            FunctionName::Extension(iri) => {
                let full_iri = self.expand_iri(iri)?;
                match full_iri.as_str() {
                    "http://www.opengis.net/def/function/geosparql/distance" => {
                        Function::GeofDistance
                    }
                    // XSD datatype constructor (cast) functions — W3C SPARQL 1.1 §17.5
                    xsd::BOOLEAN => Function::XsdBoolean,
                    xsd::INTEGER => Function::XsdInteger,
                    xsd::FLOAT => Function::XsdFloat,
                    xsd::DOUBLE => Function::XsdDouble,
                    xsd::DECIMAL => Function::XsdDecimal,
                    xsd::STRING => Function::XsdString,
                    _ => Function::Custom(full_iri),
                }
            }
        };

        let lowered_args: Vec<Expression> = args
            .iter()
            .map(|a| self.lower_expression(a))
            .collect::<Result<Vec<_>>>()?;

        Ok(Expression::call(func, lowered_args))
    }
}
