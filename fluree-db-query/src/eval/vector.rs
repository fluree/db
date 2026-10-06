//! Vector function implementations
//!
//! Implements vector/embedding functions: dotProduct, cosineSimilarity, euclideanDistance.
//!
//! These run once per bound row (flat ranking over every value of a vector
//! property), so the per-row path is kept allocation-free: arguments are
//! resolved to borrowed `&[f64]` slices straight from the row's bindings
//! (or the expression's constant) instead of round-tripping through
//! `ComparableValue`, which would alloc + copy the full vector — including
//! the query vector, which never changes across rows. The math itself goes
//! through the runtime-dispatched SIMD kernels in [`super::vector_math`].

use std::sync::Arc;

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;

use fluree_db_core::{FlakeValue, ObjKind};

use super::helpers::check_arity;
use super::value::ComparableValue;
use super::vector_math;

pub fn eval_dot_product<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_binary_vector_fn(args, row, ctx, "dotProduct", |a, b| {
        Some(vector_math::dot_f64(a, b))
    })
}

pub fn eval_cosine_similarity<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    // Zero-magnitude input is mathematically undefined, not a type error →
    // the kernel returns None and the row's score is unbound.
    eval_binary_vector_fn(args, row, ctx, "cosineSimilarity", vector_math::cosine_f64)
}

pub fn eval_euclidean_distance<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_binary_vector_fn(args, row, ctx, "euclideanDistance", |a, b| {
        Some(vector_math::l2_f64(a, b))
    })
}

/// A resolved vector argument: borrowed from the row/expression wherever
/// possible, a shared handle when the value came from a nested expression,
/// or a zero-copy f32 shard slice for indexed (arena) vectors.
enum VecArg<'a> {
    Slice(&'a [f64]),
    Shared(Arc<[f64]>),
    /// Indexed vector borrowed straight from a packed f32 shard. Widened
    /// into a reusable scratch buffer at compute time — same values and
    /// same f64 kernels as the decode path, but without its per-row
    /// `Vec<f64>` + `Arc` allocations.
    F32(fluree_db_binary_index::arena::vector::VectorSlice),
}

impl VecArg<'_> {
    /// View as `&[f64]`, widening f32 shard data into `scratch` if needed.
    fn as_f64<'s>(&'s self, scratch: &'s mut Vec<f64>) -> &'s [f64] {
        match self {
            VecArg::Slice(s) => s,
            VecArg::Shared(a) => a,
            VecArg::F32(slice) => {
                let f32s = slice.as_f32();
                scratch.clear();
                scratch.extend(f32s.iter().map(|&x| f64::from(x)));
                &scratch[..]
            }
        }
    }
}

thread_local! {
    /// Reused widening buffers for `VecArg::F32` arguments (one per side)
    /// — avoids a per-row allocation on the indexed-vector path.
    static WIDEN_SCRATCH: std::cell::RefCell<(Vec<f64>, Vec<f64>)> =
        const { std::cell::RefCell::new((Vec::new(), Vec::new())) };
}

/// Resolve one argument to a vector without copying when avoidable.
///
/// Returns `Ok(None)` for unbound variables and non-vector values (the
/// caller maps that to an unbound result, mirroring the other eval fns).
fn resolve_vector_arg<'a, R: RowAccess>(
    expr: &'a Expression,
    row: &'a R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<VecArg<'a>>> {
    match expr {
        // Row/VALUES-bound variable: borrow the vector in place.
        Expression::Var(var) => match row.get(*var) {
            Some(Binding::Lit {
                val: FlakeValue::Vector(v),
                ..
            }) => Ok(Some(VecArg::Slice(v))),
            Some(Binding::EncodedLit {
                o_kind,
                o_key,
                p_id,
                dt_id,
                lang_id,
                ..
            }) => {
                // Indexed vector fast path: borrow the packed f32 shard data
                // zero-copy. Misses (no arena for the predicate, ephemeral
                // novelty handle) fall through to the generic decode below.
                if ObjKind::from_u8(*o_kind) == ObjKind::VECTOR_ID {
                    if let Some(ctx) = ctx {
                        if let Some(slice) = ctx.binary_store.as_ref().and_then(|store| {
                            store
                                .vector_slice(ctx.binary_g_id, *p_id, *o_key as u32)
                                .ok()
                                .flatten()
                        }) {
                            return Ok(Some(VecArg::F32(slice)));
                        }
                    }
                }
                let Some(decoded) = ctx
                    .and_then(|c| c.decode_encoded_value(*o_kind, *o_key, *p_id, *dt_id, *lang_id))
                else {
                    return Ok(None);
                };
                match decoded {
                    Ok(FlakeValue::Vector(v)) => Ok(Some(VecArg::Shared(v))),
                    Ok(_) => Ok(None),
                    Err(e) => Err(QueryError::execution(format!(
                        "decode encoded vector (o_kind={o_kind}, o_key={o_key}, p_id={p_id}): {e}"
                    ))),
                }
            }
            _ => Ok(None),
        },
        // Inline constant vector: borrow from the expression itself.
        Expression::Const(FlakeValue::Vector(v)) => Ok(Some(VecArg::Slice(v))),
        // Anything else (nested expression): evaluate and keep the Arc.
        _ => match expr.eval_to_comparable(row, ctx)? {
            Some(ComparableValue::Vector(v)) => Ok(Some(VecArg::Shared(v))),
            _ => Ok(None),
        },
    }
}

/// Evaluate a binary vector function over borrowed slices.
fn eval_binary_vector_fn<R: RowAccess, F>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    fn_name: &str,
    compute: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&[f64], &[f64]) -> Option<f64>,
{
    check_arity(args, 2, fn_name)?;
    let v1 = resolve_vector_arg(&args[0], row, ctx)?;
    let v2 = resolve_vector_arg(&args[1], row, ctx)?;
    match (v1, v2) {
        (Some(a), Some(b)) => WIDEN_SCRATCH.with(|cell| {
            let scratch = &mut *cell.borrow_mut();
            let a = a.as_f64(&mut scratch.0);
            let b = b.as_f64(&mut scratch.1);
            if a.len() != b.len() {
                Err(QueryError::InvalidFilter(format!(
                    "{} requires vectors of equal length (got {} and {})",
                    fn_name,
                    a.len(),
                    b.len()
                )))
            } else {
                Ok(compute(a, b).map(ComparableValue::Double))
            }
        }),
        // Type mismatch or unbound -> return None (SPARQL-style graceful handling)
        _ => Ok(None),
    }
}
