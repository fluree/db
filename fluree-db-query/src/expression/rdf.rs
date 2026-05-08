//! RDF term function implementations
//!
//! Implements SPARQL RDF term functions: DATATYPE, LANGMATCHES, SAMETERM, IRI, BNODE

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::DatatypeDictId;
use std::sync::Arc;
use uuid::Uuid;

use super::helpers::{check_arity, format_datatype_sid};
use super::value::ComparableValue;

#[inline]
fn format_reserved_datatype(dt_id: DatatypeDictId) -> Option<ComparableValue> {
    // Real datatypes return as `Sid` to match the form produced when an IRI
    // literal like `xsd:integer` is lowered through `eval_iri` (which
    // resolves recognized IRIs to `ComparableValue::Sid`). The
    // Fluree-internal pseudo-types (`@id`, `@json`, `@vector`, `@fulltext`)
    // have no IRI form and remain compact strings.
    use fluree_db_core::Sid;
    Some(match dt_id {
        DatatypeDictId::ID => ComparableValue::String(Arc::from("@id")),
        DatatypeDictId::JSON => ComparableValue::String(Arc::from("@json")),
        DatatypeDictId::VECTOR => ComparableValue::String(Arc::from("@vector")),
        DatatypeDictId::FULL_TEXT => ComparableValue::String(Arc::from("@fulltext")),
        DatatypeDictId::STRING => ComparableValue::Sid(Sid::xsd_string()),
        DatatypeDictId::BOOLEAN => ComparableValue::Sid(Sid::new(
            fluree_vocab::namespaces::XSD,
            fluree_vocab::xsd_names::BOOLEAN,
        )),
        DatatypeDictId::INTEGER => ComparableValue::Sid(Sid::xsd_integer()),
        DatatypeDictId::LONG => ComparableValue::Sid(Sid::new(
            fluree_vocab::namespaces::XSD,
            fluree_vocab::xsd_names::LONG,
        )),
        DatatypeDictId::DECIMAL => ComparableValue::Sid(Sid::xsd_decimal()),
        DatatypeDictId::DOUBLE => ComparableValue::Sid(Sid::xsd_double()),
        DatatypeDictId::FLOAT => ComparableValue::Sid(Sid::xsd_float()),
        DatatypeDictId::DATE_TIME => ComparableValue::Sid(Sid::new(
            fluree_vocab::namespaces::XSD,
            fluree_vocab::xsd_names::DATE_TIME,
        )),
        DatatypeDictId::DATE => ComparableValue::Sid(Sid::new(
            fluree_vocab::namespaces::XSD,
            fluree_vocab::xsd_names::DATE,
        )),
        DatatypeDictId::TIME => ComparableValue::Sid(Sid::new(
            fluree_vocab::namespaces::XSD,
            fluree_vocab::xsd_names::TIME,
        )),
        DatatypeDictId::LANG_STRING => ComparableValue::Sid(Sid::new(
            fluree_vocab::namespaces::RDF,
            fluree_vocab::rdf_names::LANG_STRING,
        )),
        _ => return None,
    })
}

pub fn eval_datatype<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "DATATYPE")?;
    if let Expression::Var(var_id) = &args[0] {
        return match row.get(*var_id) {
            Some(binding) => match binding {
                Binding::Lit { dtc, .. } => Ok(Some(format_datatype_sid(dtc.datatype()))),
                Binding::EncodedLit { dt_id, .. } => {
                    let dt_id = DatatypeDictId::from_u16(*dt_id);
                    if let Some(v) = format_reserved_datatype(dt_id) {
                        return Ok(Some(v));
                    }

                    let Some(store) = ctx.and_then(|c| c.binary_store.as_deref()) else {
                        return Err(QueryError::InvalidExpression(
                            "DATATYPE requires a literal or IRI argument".to_string(),
                        ));
                    };
                    let dt_sid = store
                        .dt_sids()
                        .get(dt_id.as_u16() as usize)
                        .cloned()
                        .ok_or_else(|| {
                            QueryError::InvalidExpression(format!(
                                "DATATYPE could not resolve datatype id {}",
                                dt_id.as_u16()
                            ))
                        })?;
                    Ok(Some(format_datatype_sid(&dt_sid)))
                }
                Binding::Sid { sid: _, .. } | Binding::IriMatch { .. } | Binding::Iri(_) => {
                    Ok(Some(ComparableValue::String(Arc::from("@id"))))
                }
                Binding::Unbound | Binding::Poisoned => Ok(None),
                _ => Err(QueryError::InvalidExpression(
                    "DATATYPE requires a literal or IRI argument".to_string(),
                )),
            },
            None => Ok(None), // unbound variable
        };
    }

    // Non-Var argument: evaluate the inner expression to a ComparableValue
    // and infer its datatype IRI from the result type combined with input
    // datatype tiers. Per W3C SPARQL §17.4.1.7, arithmetic on numeric
    // operands promotes to the widest tier seen across operands:
    //   xsd:integer → xsd:decimal → xsd:float → xsd:double
    //
    // The W3C `type-promotion-01..30` tests in SPARQL 1.0 exercise this via
    // `FILTER(datatype(?l + ?r) = xsd:integer)` etc. on integer-subtype
    // operands like xsd:byte and xsd:short.
    let value = match args[0].eval_to_comparable(row, ctx)? {
        Some(v) => v,
        None => return Ok(None),
    };

    use fluree_db_core::Sid;
    let dt_sid: Sid = match &value {
        // Numeric results: pick the widest input tier seen in the
        // expression tree, falling back to the result-value tier when no
        // input tier is determinable (e.g. all-Const expression).
        ComparableValue::Long(_) | ComparableValue::BigInt(_) => {
            arithmetic_result_sid(&args[0], row, ctx, NumericResultTier::Integer)
        }
        ComparableValue::Decimal(_) => {
            arithmetic_result_sid(&args[0], row, ctx, NumericResultTier::Decimal)
        }
        ComparableValue::Double(_) => {
            arithmetic_result_sid(&args[0], row, ctx, NumericResultTier::Double)
        }
        ComparableValue::Bool(_) => Sid::new(
            fluree_vocab::namespaces::XSD,
            fluree_vocab::xsd_names::BOOLEAN,
        ),
        ComparableValue::String(_) => Sid::xsd_string(),
        ComparableValue::DateTime(_) => Sid::new(
            fluree_vocab::namespaces::XSD,
            fluree_vocab::xsd_names::DATE_TIME,
        ),
        ComparableValue::Date(_) => {
            Sid::new(fluree_vocab::namespaces::XSD, fluree_vocab::xsd_names::DATE)
        }
        ComparableValue::Time(_) => {
            Sid::new(fluree_vocab::namespaces::XSD, fluree_vocab::xsd_names::TIME)
        }
        // Iri / Sid / Vector / GeoPoint / TypedLiteral aren't legal DATATYPE
        // inputs (DATATYPE is for literals only); fall through to error.
        _ => {
            return Err(QueryError::InvalidExpression(
                "DATATYPE requires a literal argument".to_string(),
            ));
        }
    };
    Ok(Some(ComparableValue::Sid(dt_sid)))
}

/// Tier of an arithmetic result, used to widen a Long/Decimal/Double
/// `ComparableValue` to its appropriate xsd: IRI considering the *input*
/// datatypes (which may be wider than the runtime numeric value would
/// suggest — e.g. `xsd:float + xsd:integer` produces an `f64` value but
/// the W3C-correct datatype is `xsd:float`, not `xsd:double`).
#[derive(Debug, Clone, Copy)]
enum NumericResultTier {
    Integer,
    Decimal,
    Float,
    Double,
}

impl NumericResultTier {
    fn widen(self, other: NumericResultTier) -> NumericResultTier {
        use NumericResultTier::*;
        match (self, other) {
            (Double, _) | (_, Double) => Double,
            (Float, _) | (_, Float) => Float,
            (Decimal, _) | (_, Decimal) => Decimal,
            (Integer, Integer) => Integer,
        }
    }

    fn sid(self) -> fluree_db_core::Sid {
        use fluree_db_core::Sid;
        match self {
            NumericResultTier::Integer => Sid::xsd_integer(),
            NumericResultTier::Decimal => Sid::xsd_decimal(),
            NumericResultTier::Float => Sid::xsd_float(),
            NumericResultTier::Double => Sid::xsd_double(),
        }
    }
}

/// Walk an arithmetic expression tree, gather the widest input tier across
/// all leaves, and return the appropriate xsd: Sid. `fallback` is the tier
/// to use when no input has a determinable datatype (e.g. literal-only
/// arithmetic where every leaf is a `Const`).
fn arithmetic_result_sid<R: RowAccess>(
    expr: &Expression,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    fallback: NumericResultTier,
) -> fluree_db_core::Sid {
    walk_input_tier(expr, row, ctx).unwrap_or(fallback).sid()
}

fn walk_input_tier<R: RowAccess>(
    expr: &Expression,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Option<NumericResultTier> {
    use crate::ir::Function;
    match expr {
        Expression::Var(var_id) => match row.get(*var_id) {
            Some(Binding::Lit { dtc, val, .. }) => {
                let dt_name = dtc.datatype().name.as_ref();
                Some(tier_from_dt_name(dt_name).unwrap_or_else(|| tier_from_value(val)))
            }
            Some(Binding::EncodedLit { dt_id, .. }) => {
                tier_from_reserved_dt_id(DatatypeDictId::from_u16(*dt_id))
                    .or_else(|| ctx_dt_tier(ctx, *dt_id))
            }
            _ => None,
        },
        Expression::Const(fv) => Some(tier_from_filter_value(fv)),
        Expression::Call { func, args } => match func {
            Function::Add | Function::Sub | Function::Mul | Function::Div => {
                let mut acc: Option<NumericResultTier> = None;
                for arg in args {
                    if let Some(t) = walk_input_tier(arg, row, ctx) {
                        acc = Some(match acc {
                            Some(prev) => prev.widen(t),
                            None => t,
                        });
                    }
                }
                acc
            }
            _ => None,
        },
        _ => None,
    }
}

/// Tier lookup for a parse-time literal in a FILTER expression. Used when an
/// arithmetic operand is a constant (e.g. `?l + 2.5` where `2.5` is parsed
/// as `FilterValue::Double`).
fn tier_from_filter_value(fv: &crate::ir::FilterValue) -> NumericResultTier {
    use crate::ir::FilterValue;
    match fv {
        FilterValue::Long(_) | FilterValue::Bool(_) => NumericResultTier::Integer,
        FilterValue::Double(_) => NumericResultTier::Double,
        // Strings/Sids/Iris fall here — for the type-promotion use case the
        // integer tier default is fine; the tree will be widened by another
        // operand if anything else applies.
        _ => NumericResultTier::Integer,
    }
}

fn tier_from_dt_name(name: &str) -> Option<NumericResultTier> {
    use NumericResultTier::*;
    Some(match name {
        "decimal" => Decimal,
        "float" => Float,
        "double" => Double,
        // All integer subtypes promote to xsd:integer per W3C §17.4.1.7.
        "integer" | "long" | "int" | "short" | "byte" | "nonNegativeInteger"
        | "positiveInteger" | "nonPositiveInteger" | "negativeInteger" | "unsignedLong"
        | "unsignedInt" | "unsignedShort" | "unsignedByte" => Integer,
        _ => return None,
    })
}

fn tier_from_value(val: &fluree_db_core::FlakeValue) -> NumericResultTier {
    match val {
        fluree_db_core::FlakeValue::Long(_)
        | fluree_db_core::FlakeValue::BigInt(_)
        | fluree_db_core::FlakeValue::Boolean(_) => NumericResultTier::Integer,
        fluree_db_core::FlakeValue::Decimal(_) => NumericResultTier::Decimal,
        fluree_db_core::FlakeValue::Double(_) => NumericResultTier::Double,
        // Default to integer for non-numeric — caller falls back to the
        // result-value tier in that case anyway.
        _ => NumericResultTier::Integer,
    }
}

fn tier_from_reserved_dt_id(dt_id: DatatypeDictId) -> Option<NumericResultTier> {
    use NumericResultTier::*;
    Some(match dt_id {
        DatatypeDictId::INTEGER | DatatypeDictId::LONG => Integer,
        DatatypeDictId::DECIMAL => Decimal,
        DatatypeDictId::FLOAT => Float,
        DatatypeDictId::DOUBLE => Double,
        _ => return None,
    })
}

fn ctx_dt_tier(ctx: Option<&ExecutionContext<'_>>, dt_id: u16) -> Option<NumericResultTier> {
    let store = ctx.and_then(|c| c.binary_store.as_deref())?;
    let dt_sid = store.dt_sids().get(dt_id as usize)?;
    tier_from_dt_name(dt_sid.name.as_ref())
}

pub fn eval_lang_matches<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "LANGMATCHES")?;
    let tag = args[0].eval_to_comparable(row, ctx)?;
    let range = args[1].eval_to_comparable(row, ctx)?;
    match (tag, range) {
        (Some(ComparableValue::String(t)), Some(ComparableValue::String(r))) => {
            let result = if r.as_ref() == "*" {
                !t.is_empty()
            } else {
                let t_lower = t.to_lowercase();
                let r_lower = r.to_lowercase();
                t_lower == r_lower
                    || (t_lower.starts_with(&r_lower)
                        && t_lower.chars().nth(r_lower.len()) == Some('-'))
            };
            Ok(Some(ComparableValue::Bool(result)))
        }
        (None, _) | (_, None) => Ok(None),
        _ => Err(QueryError::InvalidExpression(
            "LANGMATCHES requires string arguments".to_string(),
        )),
    }
}

pub fn eval_same_term<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "SAMETERM")?;

    // Fast path: avoid decoding EncodedSid/EncodedPid to IRI strings.
    if let Some(ctx) = ctx {
        if let Some(store) = ctx.binary_store.as_deref() {
            if let Some(b) = fast_same_term_encoded_ids(args, row, ctx, store)? {
                return Ok(Some(ComparableValue::Bool(b)));
            }
        }
    }

    let v1 = args[0].eval_to_comparable(row, ctx)?;
    let v2 = args[1].eval_to_comparable(row, ctx)?;
    let same = matches!((v1, v2), (Some(a), Some(b)) if a == b);
    Ok(Some(ComparableValue::Bool(same)))
}

fn fast_same_term_encoded_ids<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: &ExecutionContext<'_>,
    store: &BinaryIndexStore,
) -> Result<Option<bool>> {
    if args.len() != 2 {
        return Ok(None);
    }

    let try_side = |var_expr: &Expression, other_expr: &Expression| -> Result<Option<bool>> {
        let Expression::Var(v) = var_expr else {
            return Ok(None);
        };
        let Some(binding) = row.get(*v) else {
            return Ok(Some(false));
        };

        match binding {
            Binding::EncodedSid { s_id, .. } => {
                // If both sides are vars and both are EncodedSid, compare directly.
                if let Expression::Var(v2) = other_expr {
                    if let Some(Binding::EncodedSid { s_id: s2, .. }) = row.get(*v2) {
                        return Ok(Some(*s_id == *s2));
                    }
                }

                let Some(other) = other_expr.eval_to_comparable(row, Some(ctx))? else {
                    return Ok(Some(false));
                };
                let rhs_s_id_opt = match other {
                    ComparableValue::Sid(sid) => store
                        .find_subject_id_by_parts(sid.namespace_code, sid.name.as_ref())
                        .map_err(|e| QueryError::Internal(format!("find_subject_id: {e}")))?,
                    ComparableValue::Iri(iri) => store
                        .find_subject_id(iri.as_ref())
                        .map_err(|e| QueryError::Internal(format!("find_subject_id: {e}")))?,
                    _ => return Ok(None),
                };
                let same = rhs_s_id_opt.is_some_and(|rhs| rhs == *s_id);
                log_same_term_fastpath_hit_once("EncodedSid");
                Ok(Some(same))
            }
            Binding::EncodedPid { p_id } => {
                if let Expression::Var(v2) = other_expr {
                    if let Some(Binding::EncodedPid { p_id: p2 }) = row.get(*v2) {
                        return Ok(Some(*p_id == *p2));
                    }
                }

                let Some(other) = other_expr.eval_to_comparable(row, Some(ctx))? else {
                    return Ok(Some(false));
                };
                let rhs_p_id_opt = match other {
                    ComparableValue::Sid(sid) => store.sid_to_p_id(&sid),
                    ComparableValue::Iri(iri) => store.find_predicate_id(iri.as_ref()),
                    _ => return Ok(None),
                };
                let same = rhs_p_id_opt.is_some_and(|rhs| rhs == *p_id);
                log_same_term_fastpath_hit_once("EncodedPid");
                Ok(Some(same))
            }
            _ => Ok(None),
        }
    };

    if let Some(v) = try_side(&args[0], &args[1])? {
        return Ok(Some(v));
    }
    if let Some(v) = try_side(&args[1], &args[0])? {
        return Ok(Some(v));
    }
    Ok(None)
}

fn log_same_term_fastpath_hit_once(kind: &'static str) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    static HIT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !HIT.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::debug!(kind, "SAMETERM: used encoded-id fast path (logged once)");
    }
}

pub fn eval_iri<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "IRI")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::String(s)) => {
            // Try to resolve the IRI string to a Sid using the execution context.
            // This is critical for FILTER comparisons like `?type = ex:Reptile`
            // where the variable binding is a Sid but the constant IRI would
            // otherwise become ComparableValue::Iri — an incomparable type pair.
            //
            // Use encode_iri_strict so unknown namespaces stay as IRI strings
            // rather than silently mapping to the EMPTY namespace (code 0).
            if let Some(sid) = ctx.and_then(|c| c.encode_iri_strict(&s)) {
                Ok(Some(ComparableValue::Sid(sid)))
            } else {
                Ok(Some(ComparableValue::Iri(s)))
            }
        }
        Some(ComparableValue::Iri(iri)) => Ok(Some(ComparableValue::Iri(iri))),
        Some(ComparableValue::Sid(sid)) => Ok(Some(ComparableValue::Sid(sid))),
        Some(_) => Ok(None),
        None => Ok(None),
    }
}

pub fn eval_bnode<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    match args.len() {
        0 => {
            // No args: generate a fresh blank node
            Ok(Some(ComparableValue::Iri(Arc::from(format!(
                "_:fdb-{}",
                Uuid::new_v4()
            )))))
        }
        1 => {
            // Label arg: deterministic blank node for the same label within a query
            match args[0].eval_to_comparable(row, ctx)? {
                Some(v) => match v.as_str() {
                    Some(label) => {
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut hasher = DefaultHasher::new();
                        label.hash(&mut hasher);
                        let hash = hasher.finish();
                        Ok(Some(ComparableValue::Iri(Arc::from(format!(
                            "_:b{hash:x}"
                        )))))
                    }
                    None => Ok(None),
                },
                None => Ok(None),
            }
        }
        _ => Err(QueryError::InvalidExpression(
            "BNODE requires 0 or 1 arguments".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use crate::ir::FilterValue;
    use crate::var_registry::VarId;
    use fluree_db_core::Sid;

    #[test]
    fn eval_iri_string_without_context_returns_iri() {
        // Without an ExecutionContext, IRI() on a string returns ComparableValue::Iri
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::Iri(Arc::from("http://example.org/ns/Reptile"))];
        let batch = Batch::new(schema, vec![col]).unwrap();
        let row = batch.row_view(0).unwrap();

        // Evaluate IRI(?x) where ?x is an IRI string
        let args = [Expression::Const(FilterValue::String(
            "http://unknown.org/ns/Foo".to_string(),
        ))];
        let result = eval_iri(&args, &row, None).unwrap();
        assert!(
            matches!(result, Some(ComparableValue::Iri(_))),
            "IRI of unknown namespace without context should return Iri, got: {result:?}"
        );
    }

    #[test]
    fn eval_iri_sid_passthrough() {
        // IRI() of a Sid should return the Sid unchanged
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let sid = Sid::new(100, "x");
        let col = vec![Binding::sid(sid.clone())];
        let batch = Batch::new(schema, vec![col]).unwrap();
        let row = batch.row_view(0).unwrap();

        let args = [Expression::Var(VarId(0))];
        let result = eval_iri(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Sid(sid)));
    }

    #[test]
    fn eval_iri_none_returns_none() {
        // IRI() of an unbound var returns None
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::Unbound];
        let batch = Batch::new(schema, vec![col]).unwrap();
        let row = batch.row_view(0).unwrap();

        let args = [Expression::Var(VarId(0))];
        let result = eval_iri(&args, &row, None).unwrap();
        assert_eq!(result, None);
    }
}
