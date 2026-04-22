//! Type-checking function implementations
//!
//! Implements SPARQL type-checking functions: BOUND, isIRI, isLiteral, isNumeric, isBlank

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;

use super::helpers::check_arity;
use super::value::ComparableValue;
use fluree_db_core::subject_id::SubjectId;
use fluree_vocab::namespaces;

pub fn eval_bound<R: RowAccess>(args: &[Expression], row: &R) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "BOUND")?;
    match &args[0] {
        Expression::Var(var) => Ok(Some(ComparableValue::Bool(!matches!(
            row.get(*var),
            Some(Binding::Unbound | Binding::Poisoned) | None
        )))),
        _ => Err(QueryError::InvalidFilter(
            "BOUND argument must be a variable".to_string(),
        )),
    }
}

pub fn eval_is_iri<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "isIRI")?;
    let val = args[0].eval_to_comparable(row, ctx)?;
    Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
        matches!(v, ComparableValue::Sid(_) | ComparableValue::Iri(_))
    }))))
}

pub fn eval_is_literal<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "isLiteral")?;
    let val = args[0].eval_to_comparable(row, ctx)?;
    Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
        // In SPARQL, a term is a literal iff it is not an IRI and not a blank node.
        // At this layer, non-literals are represented as `Sid` (node ref) or `Iri`.
        !matches!(v, ComparableValue::Sid(_) | ComparableValue::Iri(_))
    }))))
}

pub fn eval_is_numeric<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "isNumeric")?;
    let val = args[0].eval_to_comparable(row, ctx)?;
    Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
        matches!(
            v,
            ComparableValue::Long(_)
                | ComparableValue::Double(_)
                | ComparableValue::Decimal(_)
                | ComparableValue::BigInt(_)
        )
    }))))
}

pub fn eval_is_blank<R: RowAccess>(
    args: &[Expression],
    row: &R,
    _ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "isBlank")?;
    match &args[0] {
        Expression::Var(v) => {
            let is_blank = match row.get(*v) {
                Some(Binding::Sid(s)) => s.namespace_code == namespaces::BLANK_NODE,
                Some(Binding::IriMatch {
                    iri, primary_sid, ..
                }) => {
                    primary_sid.namespace_code == namespaces::BLANK_NODE
                        || iri.as_ref().starts_with("_:")
                }
                Some(Binding::Iri(iri)) => iri.as_ref().starts_with("_:"),
                Some(Binding::EncodedSid { s_id }) => {
                    SubjectId::from_u64(*s_id).ns_code() == namespaces::BLANK_NODE
                }
                _ => false,
            };
            Ok(Some(ComparableValue::Bool(is_blank)))
        }
        _ => Err(QueryError::InvalidFilter(
            "isBlank argument must be a variable".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use crate::var_registry::VarId;
    use fluree_db_core::{FlakeValue, Sid};
    use std::sync::Arc;

    fn make_string_batch() -> Batch {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::lit(
            FlakeValue::String("Hello World".to_string()),
            Sid::new(2, "string"),
        )];
        Batch::new(schema, vec![col]).unwrap()
    }

    #[test]
    fn test_bound() {
        let batch = make_string_batch();
        let row = batch.row_view(0).unwrap();
        let result = eval_bound(&[Expression::Var(VarId(0))], &row).unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }
}
