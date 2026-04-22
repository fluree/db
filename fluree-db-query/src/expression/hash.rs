//! Hash function implementations
//!
//! Implements SPARQL hash functions: MD5, SHA1, SHA256, SHA384, SHA512

use crate::binding::RowAccess;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use md5::{Digest as Md5Digest, Md5};
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};
use std::sync::Arc;

use super::helpers::check_arity;
use super::value::ComparableValue;

pub fn eval_md5<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_hash(args, row, ctx, "MD5", |s| {
        let mut hasher = Md5::new();
        hasher.update(s.as_bytes());
        format!("{:x}", hasher.finalize())
    })
}

pub fn eval_sha1<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_hash(args, row, ctx, "SHA1", |s| {
        let mut hasher = Sha1::new();
        hasher.update(s.as_bytes());
        format!("{:x}", hasher.finalize())
    })
}

pub fn eval_sha256<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_hash(args, row, ctx, "SHA256", |s| {
        let mut hasher = Sha256::new();
        hasher.update(s.as_bytes());
        format!("{:x}", hasher.finalize())
    })
}

pub fn eval_sha384<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_hash(args, row, ctx, "SHA384", |s| {
        let mut hasher = Sha384::new();
        hasher.update(s.as_bytes());
        format!("{:x}", hasher.finalize())
    })
}

pub fn eval_sha512<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    eval_hash(args, row, ctx, "SHA512", |s| {
        let mut hasher = Sha512::new();
        hasher.update(s.as_bytes());
        format!("{:x}", hasher.finalize())
    })
}

/// Evaluate a hash function with the given hasher
fn eval_hash<R: RowAccess, F>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    fn_name: &str,
    hash_fn: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&str) -> String,
{
    check_arity(args, 1, fn_name)?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(v) => match v.as_str() {
            Some(s) => {
                if let Some(ctx) = ctx {
                    ctx.tracker.consume_fuel(1)?;
                }
                Ok(Some(ComparableValue::String(Arc::from(hash_fn(s)))))
            }
            None => Err(QueryError::InvalidFilter(format!(
                "{fn_name} requires a string argument"
            ))),
        },
        None => Ok(None),
    }
}
