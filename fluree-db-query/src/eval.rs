//! Function evaluation module
//!
//! This module provides unified evaluation of SPARQL expressions and functions.
//! It contains all function implementations organized by category, as well as
//! the core expression evaluation logic on `Expression` (`eval_to_bool`,
//! `eval_to_binding*`, `eval_to_comparable`).
//!
//! # Module Structure
//!
//! - `value`: ComparableValue type and conversions
//! - `compare`: Value comparison logic
//! - `helpers`: Shared utilities (regex caching, arity checks, etc.)
//! - `dispatch`: Main function dispatcher
//! - Category submodules: `string`, `numeric`, `datetime`, `hash`, `uuid`,
//!   `vector`, `geo`, `types`, `rdf`, `conditional`, `fluree`, `arithmetic`, `logical`

mod arithmetic;
mod cast;
mod compare;
mod conditional;
mod datetime;
mod dispatch;
mod fluree;
mod fulltext;
mod geo;
mod hash;
mod helpers;
mod iter;
pub(crate) use iter::eval_single_node_predicate;
mod list;
mod logical;
mod metadata;
pub(crate) mod metadata_resolve;
mod numeric;
mod path;
mod rdf;
mod string;
mod types;
mod uuid;
mod value;
mod vector;
pub mod vector_math;

pub use metadata::cypher_name_from_iri;

pub(crate) use helpers::build_regex_with_flags;
pub use helpers::PreparedBoolExpression;
pub use value::{ArithmeticError, ComparableValue, ComparisonError, NullValueError};

use crate::binding::{Binding, BindingRow, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, FlakeValue};
use crate::var_registry::VarId;
use fluree_db_core::DatatypeConstraint;
use helpers::eval_cached_bool_predicate;
use num_traits::Zero;
use std::sync::Arc;

impl Expression {
    pub(crate) fn eval_to_bool_uncached<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<bool> {
        match self {
            Expression::Var(var) => binding_effective_bool(row.get(*var), ctx),

            Expression::Const(val) => {
                // Constant as boolean
                match val {
                    FlakeValue::Boolean(b) => Ok(*b),
                    _ => Ok(true), // Non-bool constants are truthy
                }
            }

            Expression::Call { func, args } => func.eval_to_bool(args, row, ctx),

            // A map's effective boolean value: non-empty is truthy.
            Expression::Map(_) => Ok((&self.try_eval_to_binding(row, ctx)?).into()),

            // A list predicate is already boolean (null → false in EBV).
            Expression::ListPredicate {
                kind,
                var,
                list,
                predicate,
            } => Ok(
                iter::eval_list_predicate(*kind, *var, list, predicate, row, ctx)?.unwrap_or(false),
            ),

            // Comprehension / reduce / member / a resolved value — EBV of it.
            Expression::ListComprehension { .. }
            | Expression::Reduce { .. }
            | Expression::Member { .. }
            | Expression::Resolved(_) => Ok((&self.try_eval_to_binding(row, ctx)?).into()),

            // EXISTS / pattern comprehensions are pre-resolved per row by the
            // Filter/Bind operators (replaced with Const(Bool) / Resolved). If we
            // reach here, it means resolution didn't run (bug).
            Expression::Exists { .. } => {
                tracing::warn!("EXISTS subexpression not pre-evaluated; treating as false");
                Ok(false)
            }
            Expression::PatternComprehension { .. } => {
                tracing::warn!("pattern comprehension not pre-resolved; treating as false");
                Ok(false)
            }
        }
    }

    /// Evaluate a filter expression against a row.
    ///
    /// Returns `true` if the row passes the filter, `false` otherwise.
    /// Type mismatches and unbound variables result in `false`.
    ///
    /// The `ctx` parameter provides access to the execution context for resolving
    /// `Binding::EncodedLit` values (late materialization). Pass `None` if no
    /// context is available (e.g., in tests).
    ///
    /// This method is generic over `RowAccess`, allowing it to work with both
    /// `RowView` (batch rows) and `BindingRow` (pre-batch filtering).
    pub fn eval_to_bool<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<bool> {
        if let Some(pass) =
            eval_cached_bool_predicate(self, row, ctx, || self.eval_to_bool_uncached(row, ctx))?
        {
            return Ok(pass);
        }

        self.eval_to_bool_uncached(row, ctx)
    }

    /// Evaluate expression to a comparable value.
    ///
    /// The `ctx` parameter provides access to the execution context for resolving
    /// `Binding::EncodedLit` values (late materialization). Pass `None` if no
    /// context is available.
    ///
    /// This method is generic over `RowAccess`, allowing it to work with both
    /// `RowView` (batch rows) and `BindingRow` (pre-batch filtering).
    pub fn eval_to_comparable<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<Option<ComparableValue>> {
        match self {
            Expression::Var(var) => match row.get(*var) {
                Some(Binding::Lit { val, dtc, .. }) => Ok(lit_to_comparable(val, dtc, ctx)),
                Some(Binding::EncodedLit {
                    o_kind,
                    o_key,
                    p_id,
                    dt_id,
                    lang_id,
                    ..
                }) => {
                    let Some(decoded) = ctx.and_then(|c| {
                        c.decode_encoded_value(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
                    }) else {
                        return Ok(None);
                    };
                    let val = decoded.map_err(|e| {
                        decode_lookup_error(
                            "decode encoded literal",
                            format!(
                                "o_kind={o_kind}, o_key={o_key}, p_id={p_id}, dt_id={dt_id}, lang_id={lang_id}"
                            ),
                            e,
                        )
                    })?;
                    Ok(ComparableValue::try_from(&val).ok())
                }
                Some(Binding::Sid { sid, .. }) => Ok(Some(ComparableValue::Sid(sid.clone()))),
                Some(Binding::IriMatch { iri, .. }) => {
                    Ok(Some(ComparableValue::Iri(Arc::clone(iri))))
                }
                Some(Binding::Iri(iri)) => Ok(Some(ComparableValue::Iri(Arc::clone(iri)))),
                Some(Binding::EncodedSid { s_id, .. }) => {
                    let Some(resolved) = ctx.and_then(|c| c.resolve_subject_iri(*s_id)) else {
                        return Ok(None);
                    };
                    match resolved {
                        Ok(iri) => Ok(Some(ComparableValue::Iri(Arc::from(iri)))),
                        Err(e) => Err(decode_lookup_error(
                            "resolve subject IRI",
                            format!("s_id={s_id}"),
                            e,
                        )),
                    }
                }
                Some(Binding::EncodedPid { p_id }) => {
                    let Some(store) = ctx.and_then(|c| c.binary_store.as_deref()) else {
                        return Ok(None);
                    };
                    match store.resolve_predicate_iri(*p_id) {
                        Some(iri) => Ok(Some(ComparableValue::Iri(Arc::from(iri)))),
                        None => Err(QueryError::dictionary_lookup(format!(
                            "resolve predicate IRI: unknown p_id={p_id}"
                        ))),
                    }
                }
                Some(Binding::Unbound | Binding::Poisoned) | None => Ok(None),
                Some(Binding::Grouped(_)) => {
                    debug_assert!(false, "Grouped binding in filter evaluation");
                    Ok(None)
                }
                // A path or list is not a scalar — no comparable value. The
                // relevant functions (`length`, `size`/`head`/…) read the
                // binding directly via dispatch / the binding-producing path.
                Some(
                    Binding::Path { .. } | Binding::Rel(_) | Binding::List(_) | Binding::Map(_),
                ) => Ok(None),
            },

            // FlakeValue::Null is the only variant TryFrom rejects (with
            // NullValueError); a constant Null evaluates to "no value".
            Expression::Const(val) => Ok(val.try_into().ok()),

            Expression::Call { func, args } => func.eval(args, row, ctx),

            // A map / comprehension / reduce is a structured value — no scalar
            // form; consumers read the binding via `try_eval_to_binding`.
            Expression::Map(_)
            | Expression::ListComprehension { .. }
            | Expression::Reduce { .. }
            | Expression::PatternComprehension { .. } => Ok(None),

            // A resolved value (pattern-comprehension list) — its comparable form.
            Expression::Resolved(b) => Ok(list::element_to_comparable(b)),

            // A list predicate is a boolean scalar.
            Expression::ListPredicate {
                kind,
                var,
                list,
                predicate,
            } => Ok(
                iter::eval_list_predicate(*kind, *var, list, predicate, row, ctx)?
                    .map(ComparableValue::Bool),
            ),

            // Member access yields a value; expose its comparable form (a scalar
            // property is comparable; a map/list value collapses to None).
            Expression::Member {
                target,
                key,
                predicate_iri,
            } => {
                let b = iter::eval_member(target, key, predicate_iri, row, ctx)?;
                Ok(list::element_to_comparable(&b))
            }

            // EXISTS: pre-evaluated by FilterOperator; shouldn't reach here
            Expression::Exists { .. } => {
                tracing::warn!("EXISTS subexpression not pre-evaluated; returning false");
                Ok(Some(ComparableValue::Bool(false)))
            }
        }
    }

    /// Evaluate expression and return a Binding value.
    ///
    /// This is used by BIND operator to compute values for binding to variables.
    /// Returns `Binding::Unbound` on evaluation errors (type mismatches, unbound vars, etc.)
    /// rather than `Binding::Poisoned` - Poisoned is reserved for OPTIONAL semantics.
    ///
    /// The `ctx` parameter provides access to the execution context for resolving
    /// `Binding::EncodedLit` values (late materialization).
    pub fn eval_to_binding<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Binding {
        match self.try_eval_to_binding(row, ctx) {
            Ok(binding) => binding,
            Err(err) if err.can_demote_in_expression() => Binding::Unbound,
            Err(_) => Binding::Unbound,
        }
    }

    /// Evaluate to binding under SPARQL 1.1 §18.5 `Extend` semantics: a dynamic
    /// value error (arithmetic/comparison) leaves the variable unbound for this
    /// solution, while structural errors (wrong arity, unknown datatype IRI) and
    /// fatal execution errors (dictionary lookup) still propagate.
    pub fn try_eval_to_binding_non_strict<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<Binding> {
        match self.try_eval_to_binding(row, ctx) {
            Ok(binding) => Ok(binding),
            Err(err) if err.demotes_to_unbound_in_extend() => Ok(Binding::Unbound),
            Err(err) => Err(err),
        }
    }

    /// Evaluate a filter in normal SPARQL mode while preserving fatal execution
    /// errors such as dictionary lookup failures.
    pub fn eval_to_bool_non_strict<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<bool> {
        match self.eval_to_bool(row, ctx) {
            Ok(pass) => Ok(pass),
            Err(err) if err.can_demote_in_expression() => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Evaluate to binding with strict error handling.
    ///
    /// Unlike [`eval_to_binding`], this returns errors rather than converting
    /// them to `Binding::Unbound`.
    pub fn try_eval_to_binding<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<Binding> {
        // A bare variable may hold a `List` or `Map` binding, which can't
        // round-trip through `ComparableValue` (it would collapse to Unbound).
        // Return it directly so reuse preserves the structured value — e.g.
        // `UNWIND ?listVar`, the collect→unwind round-trip, and nesting a map
        // var inside another value (`WITH properties(n) AS p RETURN {props: p}`).
        // Scalars fall through to the comparable path so normalization is
        // unchanged.
        if let Expression::Var(v) = self {
            if let Some(b @ (Binding::List(_) | Binding::Map(_))) = row.get(*v) {
                return Ok(b.clone());
            }
        }

        // A map literal builds a `Binding::Map` directly (values evaluated per
        // row, insertion order preserved, duplicate keys resolved last-wins).
        if let Expression::Map(entries) = self {
            let mut out: Vec<(Arc<str>, Binding)> = Vec::with_capacity(entries.len());
            for (key, value_expr) in entries {
                let value = value_expr.try_eval_to_binding(row, ctx)?;
                if let Some(slot) = out.iter_mut().find(|(k, _)| k == key) {
                    slot.1 = value; // last-wins
                } else {
                    out.push((Arc::clone(key), value));
                }
            }
            return Ok(Binding::Map(out));
        }

        // A pre-resolved value (a pattern-comprehension list) is returned as-is.
        if let Expression::Resolved(b) = self {
            return Ok((**b).clone());
        }

        // Scoped list-iteration and eval-time member access produce structured
        // values directly (a List / the accumulator / a looked-up value).
        match self {
            Expression::ListComprehension {
                var,
                list,
                filter,
                map,
            } => {
                return iter::eval_list_comprehension(
                    *var,
                    list,
                    filter.as_deref(),
                    map.as_deref(),
                    row,
                    ctx,
                );
            }
            Expression::Reduce {
                acc,
                init,
                var,
                list,
                body,
            } => return iter::eval_reduce(*acc, init, *var, list, body, row, ctx),
            Expression::Member {
                target,
                key,
                predicate_iri,
            } => return iter::eval_member(target, key, predicate_iri, row, ctx),
            _ => {}
        }

        // List-*returning* functions (tail, list-reverse) and list literals
        // can't be a `ComparableValue` — evaluate them straight to a `Binding`.
        if let Expression::Call { func, args } = self {
            if let Some(binding) = list::eval_list_fn_to_binding(func, args, row, ctx)? {
                return Ok(binding);
            }
        }

        let comparable = match self.eval_to_comparable(row, ctx) {
            Ok(Some(val)) => val,
            Ok(None) => {
                // Expression evaluated to no value — treat as unbound.
                // This covers: unbound variables, type mismatches that
                // return Ok(None) per W3C SPARQL §17.3, and functions
                // like vector/fulltext that return None for undefined cases.
                return Ok(Binding::Unbound);
            }
            Err(err) => return Err(err),
        };
        comparable.to_binding(ctx)
    }
}

/// Check whether a row of bindings passes all inline filter expressions.
///
/// Returns `true` if `filters` is empty or every expression evaluates to `true`.
/// Any expression that errors or evaluates to `false` causes the entire check
/// to return `false`.
///
/// This is the single point of inline-filter evaluation shared by
/// `BinaryScanOperator`, `NestedLoopJoinOperator`, and any future operator that
/// supports inline filters.
pub fn passes_filters(
    filters: &[PreparedBoolExpression],
    schema: &[VarId],
    bindings: &[Binding],
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<bool> {
    for expr in filters {
        let row = BindingRow::new(schema, bindings);
        if !expr.eval_to_bool_non_strict(&row, ctx)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Convert a literal binding's value to a `ComparableValue`, carrying the
/// datatype for the datatype-sensitive cases:
/// - xsd:float (stored as an f64, tagged only by its datatype) becomes `Float`
///   so numeric promotion keeps a float result float;
/// - a string literal with a NON-xsd:string datatype or a language tag becomes
///   a `TypedLiteral` so `=`/`!=` can be datatype-aware (D5/D7).
///
/// The Long fast path is byte-identical to `TryFrom<&FlakeValue>`; the
/// xsd:double and xsd:string/plain-string paths yield the same `ComparableValue`
/// but each pay one cheap datatype check (float-vs-double, resp.
/// xsd:string-vs-foreign/lang). Foreign string literals are rare (BSBM has none).
fn lit_to_comparable(
    val: &FlakeValue,
    dtc: &DatatypeConstraint,
    ctx: Option<&ExecutionContext<'_>>,
) -> Option<ComparableValue> {
    match val {
        FlakeValue::Long(n) => Some(ComparableValue::Long(*n)),
        FlakeValue::Double(d) if is_xsd_float(dtc) => Some(ComparableValue::Float(*d as f32)),
        FlakeValue::Double(d) => Some(ComparableValue::Double(*d)),
        FlakeValue::String(s) if is_xsd_string(dtc) => {
            Some(ComparableValue::String(Arc::from(s.as_str())))
        }
        // A string literal with a foreign *datatype* becomes a `TypedLiteral` so
        // `=`/`!=` can distinguish it (D5). A language-tagged literal stays a
        // plain String: no greenable equality test needs language-aware `=`, and
        // carrying the tag would break the string builtins (CONTAINS/REPLACE/…)
        // that operate on a String. Resolving the datatype Sid to an IRI needs
        // the snapshot; without it, degrade to a bare string.
        FlakeValue::String(s) => match dtc {
            DatatypeConstraint::LangTag(_) => Some(ComparableValue::String(Arc::from(s.as_str()))),
            DatatypeConstraint::Explicit(_) => {
                match ctx.and_then(|c| dtc.to_unresolved(c.active_snapshot)) {
                    Some(u) => Some(ComparableValue::TypedLiteral {
                        val: FlakeValue::String(s.clone()),
                        dtc: Some(u),
                    }),
                    None => Some(ComparableValue::String(Arc::from(s.as_str()))),
                }
            }
        },
        _ => ComparableValue::try_from(val).ok(),
    }
}

/// Whether a datatype constraint is exactly xsd:float.
fn is_xsd_float(dtc: &DatatypeConstraint) -> bool {
    matches!(
        dtc,
        DatatypeConstraint::Explicit(sid)
            if sid.namespace_code == fluree_vocab::namespaces::XSD
                && sid.name.as_ref() == fluree_vocab::xsd_names::FLOAT
    )
}

/// Whether a datatype constraint is exactly xsd:string.
fn is_xsd_string(dtc: &DatatypeConstraint) -> bool {
    matches!(
        dtc,
        DatatypeConstraint::Explicit(sid)
            if sid.namespace_code == fluree_vocab::namespaces::XSD
                && sid.name.as_ref() == fluree_vocab::xsd_names::STRING
    )
}

/// SPARQL Effective Boolean Value of a bound term (§17.2.2), as a fallible
/// result: a value with no EBV — a language-tagged or foreign-datatype literal,
/// an IRI/blank node, an ill-typed literal, or unbound — is a type error, not
/// silently truthy. The error is a demotable Comparison error, so a FILTER
/// excludes the row and a BIND/Extend leaves the variable unbound
/// (dawg-bev-1..6, not-not). Cypher structural truthiness (lists/maps/paths/
/// relationships) is preserved; the lenient `From<&Binding>`/`From<Comparable
/// Value>` EBVs stay in place for the non-SPARQL surfaces that use them.
fn binding_effective_bool(
    binding: Option<&Binding>,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<bool> {
    match binding {
        Some(Binding::Lit { val, dtc, .. }) => lit_effective_bool(val, dtc),
        Some(Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            ..
        }) => {
            let decoded =
                ctx.and_then(|c| c.decode_encoded_value(*o_kind, *o_key, *p_id, *dt_id, *lang_id));
            match decoded {
                Some(Ok(val)) => match ComparableValue::try_from(&val) {
                    Ok(cv) => comparable_effective_bool(&cv),
                    Err(_) => Err(ebv_type_error()),
                },
                _ => Err(ebv_type_error()),
            }
        }
        // Cypher structural truthiness (non-SPARQL surface).
        Some(Binding::List(items)) => Ok(!items.is_empty()),
        Some(Binding::Map(entries)) => Ok(!entries.is_empty()),
        Some(Binding::Path { .. } | Binding::Rel(_)) => Ok(true),
        // IRI/blank node/ref and unbound/poisoned have no effective boolean value.
        _ => Err(ebv_type_error()),
    }
}

/// Full XSD IRI when `dtc` names a numeric or boolean XSD datatype — the
/// family whose string-backed lexical forms still have an EBV (§17.2.2) —
/// `None` for everything else (string is handled separately; a lang-tagged or
/// foreign-datatype literal has no EBV).
fn numeric_or_boolean_xsd_iri(dtc: &DatatypeConstraint) -> Option<&'static str> {
    use fluree_vocab::{namespaces::XSD, xsd, xsd_names as n};
    let DatatypeConstraint::Explicit(sid) = dtc else {
        return None;
    };
    if sid.namespace_code != XSD {
        return None;
    }
    Some(match sid.name.as_ref() {
        n::INTEGER => xsd::INTEGER,
        n::LONG => xsd::LONG,
        n::INT => xsd::INT,
        n::SHORT => xsd::SHORT,
        n::BYTE => xsd::BYTE,
        n::UNSIGNED_LONG => xsd::UNSIGNED_LONG,
        n::UNSIGNED_INT => xsd::UNSIGNED_INT,
        n::UNSIGNED_SHORT => xsd::UNSIGNED_SHORT,
        n::UNSIGNED_BYTE => xsd::UNSIGNED_BYTE,
        n::NON_NEGATIVE_INTEGER => xsd::NON_NEGATIVE_INTEGER,
        n::POSITIVE_INTEGER => xsd::POSITIVE_INTEGER,
        n::NON_POSITIVE_INTEGER => xsd::NON_POSITIVE_INTEGER,
        n::NEGATIVE_INTEGER => xsd::NEGATIVE_INTEGER,
        n::DECIMAL => xsd::DECIMAL,
        n::FLOAT => xsd::FLOAT,
        n::DOUBLE => xsd::DOUBLE,
        n::BOOLEAN => xsd::BOOLEAN,
        _ => return None,
    })
}

/// The IRI-keyed twin of [`numeric_or_boolean_xsd_iri`], for
/// `UnresolvedDatatypeConstraint::Explicit` (which carries a full IRI string).
fn iri_is_numeric_or_boolean_xsd(iri: &str) -> bool {
    use fluree_vocab::xsd;
    [
        xsd::INTEGER,
        xsd::LONG,
        xsd::INT,
        xsd::SHORT,
        xsd::BYTE,
        xsd::UNSIGNED_LONG,
        xsd::UNSIGNED_INT,
        xsd::UNSIGNED_SHORT,
        xsd::UNSIGNED_BYTE,
        xsd::NON_NEGATIVE_INTEGER,
        xsd::POSITIVE_INTEGER,
        xsd::NON_POSITIVE_INTEGER,
        xsd::NEGATIVE_INTEGER,
        xsd::DECIMAL,
        xsd::FLOAT,
        xsd::DOUBLE,
        xsd::BOOLEAN,
    ]
    .contains(&iri)
}

/// EBV of a parsed (coerced) boolean/numeric value; `None` when the coercion
/// produced something with no direct numeric/boolean EBV.
fn coerced_effective_bool(v: &FlakeValue) -> Option<bool> {
    match v {
        FlakeValue::Boolean(b) => Some(*b),
        FlakeValue::Long(n) => Some(*n != 0),
        FlakeValue::Double(d) => Some(!d.is_nan() && *d != 0.0),
        FlakeValue::BigInt(n) => Some(!n.is_zero()),
        FlakeValue::Decimal(d) => Some(!d.is_zero()),
        _ => None,
    }
}

/// EBV of a literal value + its datatype constraint (the common, non-encoded
/// path). Numeric → non-zero and non-NaN; xsd:string/plain → non-empty; a
/// language-tagged or foreign-datatype literal has no EBV.
fn lit_effective_bool(val: &FlakeValue, dtc: &DatatypeConstraint) -> Result<bool> {
    match val {
        FlakeValue::Boolean(b) => Ok(*b),
        FlakeValue::Long(n) => Ok(*n != 0),
        FlakeValue::Double(d) => Ok(!d.is_nan() && *d != 0.0),
        FlakeValue::BigInt(n) => Ok(!n.is_zero()),
        FlakeValue::Decimal(d) => Ok(!d.is_zero()),
        FlakeValue::String(s) if is_xsd_string(dtc) => Ok(!s.is_empty()),
        // §17.2.2: a numeric- or boolean-typed literal that arrives
        // string-backed — a cast/computed value like `xsd:float("1.5")`
        // (BIND stores it as a String tagged xsd:float), or a STRDT/stored
        // lexical form — still has an EBV. Parse the lexical form: a
        // well-formed value follows the numeric/boolean rule, and an
        // ILL-FORMED boolean/numeric lexical form is EBV FALSE per the
        // spec's rule 1, not a type error.
        FlakeValue::String(s) => {
            let Some(dt_iri) = numeric_or_boolean_xsd_iri(dtc) else {
                return Err(ebv_type_error());
            };
            match fluree_db_core::coerce_value(FlakeValue::String(s.clone()), dt_iri) {
                Ok(v) => Ok(coerced_effective_bool(&v).unwrap_or(false)),
                Err(_) => Ok(false),
            }
        }
        _ => Err(ebv_type_error()),
    }
}

/// EBV of an already-materialized comparable value (the late-materialized
/// encoded path and direct expression results). It cannot observe an encoded
/// language tag, so an encoded lang-string reads as a string here — an
/// untested corner no register test exercises.
pub(crate) fn comparable_effective_bool(cv: &ComparableValue) -> Result<bool> {
    match cv {
        ComparableValue::Bool(b) => Ok(*b),
        ComparableValue::Long(n) => Ok(*n != 0),
        ComparableValue::Double(d) => Ok(!d.is_nan() && *d != 0.0),
        ComparableValue::Float(f) => Ok(!f.is_nan() && *f != 0.0),
        ComparableValue::BigInt(n) => Ok(!n.is_zero()),
        ComparableValue::Decimal(d) => Ok(!d.is_zero()),
        ComparableValue::String(s) => Ok(!s.is_empty()),
        // §17.2.2 for string-backed typed literals (cast/STRDT results that
        // reach EBV directly, without a BIND round-trip): numeric/boolean
        // datatypes parse to their value's EBV, ill-formed lexical forms are
        // EBV false (rule 1); a plain-string TypedLiteral is string EBV; a
        // lang-tagged or foreign-datatype literal has no EBV.
        ComparableValue::TypedLiteral {
            val: FlakeValue::String(s),
            dtc,
        } => match dtc {
            Some(crate::parse::UnresolvedDatatypeConstraint::Explicit(iri))
                if iri_is_numeric_or_boolean_xsd(iri) =>
            {
                match fluree_db_core::coerce_value(FlakeValue::String(s.clone()), iri) {
                    Ok(v) => Ok(coerced_effective_bool(&v).unwrap_or(false)),
                    Err(_) => Ok(false),
                }
            }
            None => Ok(!s.is_empty()),
            _ => Err(ebv_type_error()),
        },
        _ => Err(ebv_type_error()),
    }
}

/// A value with no effective boolean value is a (demotable) type error.
fn ebv_type_error() -> QueryError {
    ComparisonError::TypeMismatch {
        operator: "EBV",
        left_type: "term",
        right_type: "xsd:boolean",
    }
    .into()
}

fn decode_lookup_error(
    kind: &'static str,
    details: impl Into<String>,
    err: impl std::fmt::Display,
) -> QueryError {
    let details = details.into();
    tracing::debug!(
        kind,
        details = %details,
        error = %err,
        "dictionary lookup failure during expression evaluation"
    );
    QueryError::dictionary_lookup(format!("{kind}: {details}: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use crate::var_registry::VarId;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_test_batch() -> Batch {
        let schema: Arc<[crate::var_registry::VarId]> =
            Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());

        let age_col = vec![
            Binding::lit(FlakeValue::Long(25), Sid::new(2, "long")),
            Binding::lit(FlakeValue::Long(30), Sid::new(2, "long")),
            Binding::lit(FlakeValue::Long(18), Sid::new(2, "long")),
            Binding::Unbound,
        ];

        let name_col = vec![
            Binding::lit(
                FlakeValue::String("Alice".to_string()),
                Sid::new(2, "string"),
            ),
            Binding::lit(FlakeValue::String("Bob".to_string()), Sid::new(2, "string")),
            Binding::lit(
                FlakeValue::String("Carol".to_string()),
                Sid::new(2, "string"),
            ),
            Binding::lit(
                FlakeValue::String("Dave".to_string()),
                Sid::new(2, "string"),
            ),
        ];

        Batch::new(schema, vec![age_col, name_col]).unwrap()
    }

    #[test]
    fn test_evaluate_comparison_gt() {
        let batch = make_test_batch();

        // ?age > 20
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(20)),
        );

        // Row 0: age=25 > 20 → true
        let row0 = batch.row_view(0).unwrap();
        assert!(expr.eval_to_bool::<_>(&row0, None).unwrap());

        // Row 2: age=18 > 20 → false
        let row2 = batch.row_view(2).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row2, None).unwrap());

        // Row 3: age=Unbound → false
        let row3 = batch.row_view(3).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row3, None).unwrap());
    }

    #[test]
    fn test_evaluate_and() {
        let batch = make_test_batch();

        // ?age > 20 AND ?age < 28
        let expr = Expression::and(vec![
            Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(20)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(28)),
            ),
        ]);

        // Row 0: age=25 → true (25 > 20 AND 25 < 28)
        let row0 = batch.row_view(0).unwrap();
        assert!(expr.eval_to_bool::<_>(&row0, None).unwrap());

        // Row 1: age=30 → false (30 > 20 but 30 < 28 is false)
        let row1 = batch.row_view(1).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row1, None).unwrap());
    }

    #[test]
    fn test_evaluate_or() {
        let batch = make_test_batch();

        // ?age < 20 OR ?age > 28
        let expr = Expression::or(vec![
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(20)),
            ),
            Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(28)),
            ),
        ]);

        // Row 0: age=25 → false
        let row0 = batch.row_view(0).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row0, None).unwrap());

        // Row 1: age=30 → true (30 > 28)
        let row1 = batch.row_view(1).unwrap();
        assert!(expr.eval_to_bool::<_>(&row1, None).unwrap());

        // Row 2: age=18 → true (18 < 20)
        let row2 = batch.row_view(2).unwrap();
        assert!(expr.eval_to_bool::<_>(&row2, None).unwrap());
    }

    #[test]
    fn test_evaluate_not() {
        let batch = make_test_batch();

        // NOT(?age > 25)
        let expr = Expression::not(Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(25)),
        ));

        // Row 0: age=25 → NOT(25 > 25) = NOT(false) = true
        let row0 = batch.row_view(0).unwrap();
        assert!(expr.eval_to_bool::<_>(&row0, None).unwrap());

        // Row 1: age=30 → NOT(30 > 25) = NOT(true) = false
        let row1 = batch.row_view(1).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row1, None).unwrap());
    }
}
