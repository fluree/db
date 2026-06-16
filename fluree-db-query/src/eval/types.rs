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
    Ok(Some(ComparableValue::Bool(val.is_some_and(|v| match v {
        // Per SPARQL, isIRI/isURI is true for IRIs only — blank nodes are NOT IRIs.
        // Fluree skolemizes blank nodes into the node-ref (`Sid`/`Iri`) space, so a
        // `Sid` in the BLANK_NODE namespace or an `Iri` lexically prefixed with "_:"
        // is a blank node and must be excluded here. This mirrors `eval_is_blank`'s
        // detection so isIRI/isBlank stay mutually exclusive (a term is never both).
        ComparableValue::Sid(sid) => sid.namespace_code != namespaces::BLANK_NODE,
        ComparableValue::Iri(s) => !s.starts_with("_:"),
        _ => false,
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
                Some(Binding::Sid { sid: s, .. }) => s.namespace_code == namespaces::BLANK_NODE,
                Some(Binding::IriMatch {
                    iri, primary_sid, ..
                }) => {
                    primary_sid.namespace_code == namespaces::BLANK_NODE
                        || iri.as_ref().starts_with("_:")
                }
                Some(Binding::Iri(iri)) => iri.as_ref().starts_with("_:"),
                Some(Binding::EncodedSid { s_id, .. }) => {
                    SubjectId::from_u64(*s_id).ns_code() == namespaces::BLANK_NODE.as_u16()
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
    use fluree_db_core::NsCode;
    use fluree_db_core::{FlakeValue, Sid};
    use std::sync::Arc;

    fn make_string_batch() -> Batch {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::lit(
            FlakeValue::String("Hello World".to_string()),
            Sid::new(NsCode(2), "string"),
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

    /// A node-ref binding: an ordinary IRI Sid vs a blank-node Sid (BLANK_NODE
    /// namespace). isIRI must be true for the former and FALSE for the latter,
    /// and isBlank the inverse — they must be mutually exclusive (regression for
    /// the DBLP `isIRI(?s)` quirk where skolemized bnodes read as both isIRI and
    /// isBlank). isLiteral must be false for both.
    fn make_node_batch() -> Batch {
        let schema: Arc<[VarId]> =
            Arc::from(vec![VarId(0), VarId(1), VarId(2), VarId(3)].into_boxed_slice());
        // Two representations of each, covering both branches of eval_is_iri:
        //   ns_code branch (Binding::Sid -> ComparableValue::Sid):
        //     VarId(0): ordinary IRI Sid; VarId(1): blank-node Sid (BLANK_NODE ns).
        //   "_:" prefix branch (Binding::Iri -> ComparableValue::Iri), which is how a
        //     scanned EncodedSid blank node resolves (BLANK_NODE ns prefix is "_:"):
        //     VarId(2): ordinary IRI string; VarId(3): "_:"-prefixed blank node.
        let iri_sid = vec![Binding::sid(Sid::new(NsCode(7), "Person"))];
        let bnode_sid = vec![Binding::sid(Sid::new(namespaces::BLANK_NODE, "b0"))];
        let iri_str = vec![Binding::Iri(Arc::from("http://example.org/ns/Person"))];
        let bnode_str = vec![Binding::Iri(Arc::from("_:b0"))];
        Batch::new(schema, vec![iri_sid, bnode_sid, iri_str, bnode_str]).unwrap()
    }

    #[test]
    fn is_iri_excludes_blank_node_and_is_disjoint_from_is_blank() {
        let batch = make_node_batch();
        let row = batch.row_view(0).unwrap();

        // (expr, expect_is_iri, expect_is_blank) — isLiteral must be false for all (node refs).
        let cases = [
            (Expression::Var(VarId(0)), true, false), // IRI as Sid
            (Expression::Var(VarId(1)), false, true), // bnode as Sid (ns_code branch)
            (Expression::Var(VarId(2)), true, false), // IRI as Iri string
            (Expression::Var(VarId(3)), false, true), // bnode as "_:" Iri (prefix branch)
        ];

        for (expr, want_iri, want_blank) in cases {
            assert_eq!(
                eval_is_iri(std::slice::from_ref(&expr), &row, None).unwrap(),
                Some(ComparableValue::Bool(want_iri)),
                "isIRI mismatch for {expr:?} (blank nodes are not IRIs per SPARQL)"
            );
            assert_eq!(
                eval_is_blank(std::slice::from_ref(&expr), &row, None).unwrap(),
                Some(ComparableValue::Bool(want_blank)),
                "isBlank mismatch for {expr:?}"
            );
            assert_eq!(
                eval_is_literal(std::slice::from_ref(&expr), &row, None).unwrap(),
                Some(ComparableValue::Bool(false)),
                "isLiteral must be false for node ref {expr:?}"
            );
            // Mutual exclusivity invariant: a term is never both isIRI and isBlank.
            assert!(want_iri != want_blank, "isIRI and isBlank must be disjoint");
        }
    }
}
