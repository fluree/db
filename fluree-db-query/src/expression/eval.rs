//! Core filter expression evaluation
//!
//! This module provides the main evaluation methods on Expression:
//! - `eval_to_bool()` - evaluate to boolean
//! - `eval_to_binding*()` - evaluate to Binding for BIND operator
//! - `eval_to_comparable()` - evaluate to ComparableValue

use super::helpers::{eval_cached_bool_predicate, PreparedBoolExpression};
use super::value::ComparableValue;
use crate::binding::{Binding, BindingRow, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, FilterValue};
use crate::parse::UnresolvedDatatypeConstraint;
use crate::var_registry::VarId;
use fluree_db_core::{FlakeValue, Sid};
use std::sync::Arc;

/// Convert a `Binding::Lit { val, dtc }` to a `ComparableValue`, preserving
/// custom-datatype information so cross-type equality comparisons can be
/// detected per W3C SPARQL §17.4.1.2 (RDFterm-equal).
///
/// The default `String → ComparableValue::String` conversion drops datatype
/// info — fine for `xsd:string` (the canonical plain literal) but wrong for
/// `"zzz"^^:myType`, which would then compare equal to a plain `"zzz"`. The
/// W3C `expr-equals` tests `eq-4`, `eq-2-1`, and `eq-2-2` exercise this.
///
/// When the value is a string with a non-`xsd:string` datatype, wrap as
/// `ComparableValue::TypedLiteral` carrying a structural-key dtc derived
/// from the resolved Sid (`[<ns_code>:<name>]`). The compare engine then
/// emits `None` for cross-type comparisons (→ FILTER false / type error)
/// and matches lexically when both sides share the same datatype key.
fn comparable_from_lit(val: &FlakeValue, dt_sid: &Sid) -> Option<ComparableValue> {
    let xsd_string = Sid::xsd_string();
    // String literals only wrap as TypedLiteral when their datatype is a
    // genuine *custom* type (e.g. `:myType`). Both `xsd:string` (the plain-
    // literal default) and `rdf:langString` (lang-tagged strings) flow as
    // `ComparableValue::String` so the SPARQL string functions
    // (CONTAINS, SUBSTR, REGEX, REPLACE, STRSTARTS, ...) can operate on
    // them. The wrap exists to enable the W3C `eq-2-1`/`eq-2-2` value
    // -equality semantics that distinguish a typed literal from a plain
    // literal of the same lexical form.
    let is_default_string_class = *dt_sid == xsd_string
        || (dt_sid.namespace_code == fluree_vocab::namespaces::RDF
            && dt_sid.name.as_ref() == fluree_vocab::rdf_names::LANG_STRING);
    match val {
        FlakeValue::String(s) if !is_default_string_class => {
            let dt_key = format!("[{}:{}]", dt_sid.namespace_code, dt_sid.name);
            Some(ComparableValue::TypedLiteral {
                val: FlakeValue::String(s.clone()),
                dtc: Some(UnresolvedDatatypeConstraint::Explicit(Arc::from(dt_key))),
            })
        }
        _ => ComparableValue::try_from(val).ok(),
    }
}

impl Expression {
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

    pub(crate) fn eval_to_bool_uncached<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<bool> {
        match self {
            Expression::Var(var) => Ok(row.get(*var).is_some_and(Into::into)),

            Expression::Const(val) => {
                // Constant as boolean
                match val {
                    FilterValue::Bool(b) => Ok(*b),
                    _ => Ok(true), // Non-bool constants are truthy
                }
            }

            Expression::Call { func, args } => func.eval_to_bool(args, row, ctx),

            // EXISTS subexpressions in compound filters are pre-evaluated by the
            // FilterOperator and replaced with Const(Bool) before this is called.
            // If we reach here, it means the EXISTS was not pre-evaluated (bug).
            Expression::Exists { .. } => {
                tracing::warn!("EXISTS subexpression not pre-evaluated; treating as false");
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
                Some(Binding::Lit { val, dtc, .. }) => {
                    Ok(comparable_from_lit(val, dtc.datatype()))
                }
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
                        Self::decode_lookup_error(
                            "decode encoded literal",
                            format!(
                                "o_kind={o_kind}, o_key={o_key}, p_id={p_id}, dt_id={dt_id}, lang_id={lang_id}"
                            ),
                            e,
                        )
                    })?;
                    // For string-valued literals with a custom (non-xsd:string)
                    // datatype, look up the Sid and route through the same
                    // TypedLiteral wrapper used for `Binding::Lit`. Without
                    // this, `"zzz"^^:myType` would decode to `String("zzz")`
                    // and compare equal to a plain `"zzz"` (W3C eq-4 violation).
                    if let FlakeValue::String(_) = &val {
                        if let Some(dt_sid) = ctx
                            .and_then(|c| c.binary_store.as_deref())
                            .and_then(|store| store.dt_sids().get(*dt_id as usize).cloned())
                        {
                            return Ok(comparable_from_lit(&val, &dt_sid));
                        }
                    }
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
                        Err(e) => Err(Self::decode_lookup_error(
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
            },

            Expression::Const(val) => Ok(Some(val.into())),

            Expression::Call { func, args } => func.eval(args, row, ctx),

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

    /// Evaluate to binding in non-strict mode while preserving fatal execution
    /// errors such as dictionary lookup failures.
    pub fn try_eval_to_binding_non_strict<R: RowAccess>(
        &self,
        row: &R,
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<Binding> {
        match self.try_eval_to_binding(row, ctx) {
            Ok(binding) => Ok(binding),
            Err(err) if err.can_demote_in_expression() => Ok(Binding::Unbound),
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
            Expression::Const(FilterValue::Long(20)),
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
                Expression::Const(FilterValue::Long(20)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(28)),
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
                Expression::Const(FilterValue::Long(20)),
            ),
            Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(28)),
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
            Expression::Const(FilterValue::Long(25)),
        ));

        // Row 0: age=25 → NOT(25 > 25) = NOT(false) = true
        let row0 = batch.row_view(0).unwrap();
        assert!(expr.eval_to_bool::<_>(&row0, None).unwrap());

        // Row 1: age=30 → NOT(30 > 25) = NOT(true) = false
        let row1 = batch.row_view(1).unwrap();
        assert!(!expr.eval_to_bool::<_>(&row1, None).unwrap());
    }
}
