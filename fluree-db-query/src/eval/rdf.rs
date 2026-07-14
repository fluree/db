//! RDF term function implementations
//!
//! Implements SPARQL RDF term functions: DATATYPE, LANGMATCHES, SAMETERM, IRI, BNODE

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::{DatatypeDictId, Sid};
use std::sync::Arc;
use uuid::Uuid;

use super::helpers::{check_arity, WELL_KNOWN_DATATYPES};
use super::value::ComparableValue;

/// Canonical datatype `Sid` for a reserved datatype id, resolved without
/// consulting the binary store's datatype dictionary. Returns `None` for
/// datatype ids that aren't pinned to a well-known Sid (the caller then
/// resolves those via `store.dt_sids()`).
#[inline]
fn reserved_datatype_sid(dt_id: DatatypeDictId) -> Option<Sid> {
    let dts = &*WELL_KNOWN_DATATYPES;
    let sid = match dt_id {
        DatatypeDictId::ID => &dts.id_type,
        DatatypeDictId::STRING => &dts.xsd_string,
        DatatypeDictId::BOOLEAN => &dts.xsd_boolean,
        DatatypeDictId::INTEGER => &dts.xsd_integer,
        DatatypeDictId::LONG => &dts.xsd_long,
        DatatypeDictId::DECIMAL => &dts.xsd_decimal,
        DatatypeDictId::DOUBLE => &dts.xsd_double,
        DatatypeDictId::FLOAT => &dts.xsd_float,
        DatatypeDictId::DATE_TIME => &dts.xsd_datetime,
        DatatypeDictId::DATE => &dts.xsd_date,
        DatatypeDictId::TIME => &dts.xsd_time,
        DatatypeDictId::LANG_STRING => &dts.rdf_lang_string,
        DatatypeDictId::JSON => &dts.rdf_json,
        DatatypeDictId::VECTOR => &dts.fluree_vector,
        DatatypeDictId::FULL_TEXT => &dts.fluree_full_text,
        _ => return None,
    };
    Some(sid.clone())
}

/// SPARQL 1.1 §17.4.2.3 `DATATYPE`: returns the datatype IRI of a literal.
///
/// The result is an IRI term (a `Sid`), not a string — so it compares equal to
/// the same datatype IRI written elsewhere in a query, e.g.
/// `FILTER(DATATYPE(?v) = xsd:decimal)` where `xsd:decimal` also lowers to a
/// `Sid`. SPARQL results render it as the full IRI; JSON-LD compacts it against
/// the active `@context`.
/// `strict` selects the SPARQL behavior for non-literal arguments: a type
/// error, evaluated as "no value" (`Ok(None)`) so a FILTER excludes the row
/// and a project-expression/BIND leaves the variable unbound (§17.2/§18.5).
/// The lenient (JSON-LD surface) mode keeps the deliberate Fluree extension:
/// `DATATYPE(<iri>)` reports the `@id` ref type. See decision D-12.
pub fn eval_datatype<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    strict: bool,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "DATATYPE")?;
    // Fast path: a bare variable reads the binding's datatype directly,
    // without materializing the value.
    if let Expression::Var(var_id) = &args[0] {
        return match row.get(*var_id) {
            Some(binding) => datatype_of_binding(binding, ctx, strict),
            None => Ok(None), // unbound variable
        };
    }
    // General case: evaluate the argument expression (arithmetic, casts,
    // constants, nested builtins) and classify the resulting value.
    match args[0].eval_to_comparable(row, ctx)? {
        Some(v) => datatype_of_comparable(&v, ctx, strict),
        None => Ok(None), // evaluation error / unbound → no value
    }
}

/// Datatype of a bound value (the `DATATYPE(?var)` path).
fn datatype_of_binding(
    binding: &Binding,
    ctx: Option<&ExecutionContext<'_>>,
    strict: bool,
) -> Result<Option<ComparableValue>> {
    match binding {
        Binding::Lit { dtc, .. } => Ok(Some(ComparableValue::Sid(dtc.datatype().clone()))),
        Binding::EncodedLit { dt_id, .. } => {
            let dt_id = DatatypeDictId::from_u16(*dt_id);
            if let Some(sid) = reserved_datatype_sid(dt_id) {
                return Ok(Some(ComparableValue::Sid(sid)));
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
            Ok(Some(ComparableValue::Sid(dt_sid)))
        }
        Binding::Sid { .. }
        | Binding::IriMatch { .. }
        | Binding::Iri(_)
        | Binding::EncodedSid { .. }
        | Binding::EncodedPid { .. } => {
            if strict {
                // SPARQL §17.4.2.3: DATATYPE of a non-literal is a type error.
                Ok(None)
            } else {
                // Fluree extension (JSON-LD surface): DATATYPE of an IRI/ref
                // reports the `@id` ref type.
                Ok(Some(ComparableValue::Sid(
                    WELL_KNOWN_DATATYPES.id_type.clone(),
                )))
            }
        }
        Binding::Unbound | Binding::Poisoned => Ok(None),
        _ => Err(QueryError::InvalidExpression(
            "DATATYPE requires a literal or IRI argument".to_string(),
        )),
    }
}

/// Datatype of a computed value (the `DATATYPE(<expr>)` path): the datatype
/// the value would carry if bound — matching storage/arithmetic tagging
/// (a plain integer result is `xsd:integer`, RDF 1.1).
fn datatype_of_comparable(
    v: &ComparableValue,
    ctx: Option<&ExecutionContext<'_>>,
    strict: bool,
) -> Result<Option<ComparableValue>> {
    use fluree_vocab::UnresolvedDatatypeConstraint;

    let dts = &*WELL_KNOWN_DATATYPES;
    let sid = match v {
        ComparableValue::Long(_) | ComparableValue::BigInt(_) => dts.xsd_integer.clone(),
        ComparableValue::Double(_) => dts.xsd_double.clone(),
        ComparableValue::Decimal(_) => dts.xsd_decimal.clone(),
        ComparableValue::Bool(_) => dts.xsd_boolean.clone(),
        ComparableValue::String(_) => dts.xsd_string.clone(),
        ComparableValue::DateTime(_) => dts.xsd_datetime.clone(),
        ComparableValue::Date(_) => dts.xsd_date.clone(),
        ComparableValue::Time(_) => dts.xsd_time.clone(),
        ComparableValue::Vector(_) => dts.fluree_vector.clone(),
        ComparableValue::GeoPoint(_) => dts.geo_wkt_literal.clone(),
        ComparableValue::TypedLiteral { dtc, .. } => match dtc {
            Some(UnresolvedDatatypeConstraint::LangTag(_)) => dts.rdf_lang_string.clone(),
            Some(UnresolvedDatatypeConstraint::Explicit(iri)) => {
                // Prefer a Sid (comparable to a datatype IRI written in the
                // query); fall back to the raw IRI for datatypes the ledger
                // has never seen.
                match ctx.and_then(|c| c.active_snapshot.encode_iri_strict(iri)) {
                    Some(sid) => sid,
                    None => return Ok(Some(ComparableValue::Iri(iri.clone()))),
                }
            }
            // `TypedLiteral` without a constraint carries a plain value;
            // mirror the binding conversion (`ComparableValue → Binding`),
            // which treats it as `xsd:string`.
            None => dts.xsd_string.clone(),
        },
        ComparableValue::Sid(_) | ComparableValue::Iri(_) => {
            return if strict {
                // SPARQL §17.4.2.3: DATATYPE of a non-literal is a type error.
                Ok(None)
            } else {
                Ok(Some(ComparableValue::Sid(dts.id_type.clone())))
            };
        }
    };
    Ok(Some(ComparableValue::Sid(sid)))
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

/// Commutative hash of a solution's bound values, identifying the solution
/// for `BNODE(label)`.
///
/// - Blank-node-valued bindings are skipped, so a BNODE-produced column bound
///   between two `BNODE(label)` calls on the same solution does not change
///   the solution's identity (`bnode01` binds two per-row bnodes that must
///   agree within a row). Generated bnodes reach the row either as
///   `Binding::Iri("_:…")` or, once the snapshot encodes them, as a
///   `Binding::Sid` in the blank-node namespace — both forms are excluded
///   (mirroring `eval_is_blank`).
/// - Unbound/poisoned slots are skipped for the same reason (schema widening
///   without new values must not change identity).
/// - The per-pair hashes are XOR-folded, so operator schema order is
///   irrelevant.
///
/// Runs only for `BNODE(label)` — never on general filter/bind paths.
fn solution_identity_hash<R: RowAccess>(row: &R) -> u64 {
    use fluree_db_core::SubjectId;
    use fluree_vocab::namespaces;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut acc = 0u64;
    row.for_each_binding(&mut |var, b| {
        match b {
            Binding::Unbound | Binding::Poisoned => return,
            Binding::Iri(iri) if iri.starts_with("_:") => return,
            Binding::Sid { sid, .. } if sid.namespace_code == namespaces::BLANK_NODE => return,
            Binding::IriMatch {
                iri, primary_sid, ..
            } if primary_sid.namespace_code == namespaces::BLANK_NODE
                || iri.as_ref().starts_with("_:") =>
            {
                return;
            }
            Binding::EncodedSid { s_id, .. }
                if SubjectId::from_u64(*s_id).ns_code() == namespaces::BLANK_NODE =>
            {
                return;
            }
            _ => {}
        }
        let mut h = DefaultHasher::new();
        var.hash(&mut h);
        b.hash(&mut h);
        acc ^= h.finish();
    });
    acc
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
            // Label arg (SPARQL 1.1 §17.4.2.9): the same label maps to the
            // same blank node WITHIN one solution, but to distinct blank
            // nodes ACROSS solutions. Fold the solution's identity (its bound
            // values) into the label hash.
            match args[0].eval_to_comparable(row, ctx)? {
                Some(v) => match v.as_str() {
                    Some(label) => {
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut hasher = DefaultHasher::new();
                        label.hash(&mut hasher);
                        solution_identity_hash(row).hash(&mut hasher);
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
    use crate::var_registry::VarId;
    use fluree_db_core::value::FlakeValue;
    use fluree_db_core::Sid;

    #[test]
    fn eval_iri_string_without_context_returns_iri() {
        // Without an ExecutionContext, IRI() on a string returns ComparableValue::Iri
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::Iri(Arc::from("http://example.org/ns/Reptile"))];
        let batch = Batch::new(schema, vec![col]).unwrap();
        let row = batch.row_view(0).unwrap();

        // Evaluate IRI(?x) where ?x is an IRI string
        let args = [Expression::Const(FlakeValue::String(
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
