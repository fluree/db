//! UUID function implementations
//!
//! Implements SPARQL UUID functions: UUID, STRUUID

use crate::error::Result;
use crate::ir::Expression;
use std::sync::Arc;
use uuid::Uuid;

use super::helpers::check_arity;
use super::value::ComparableValue;

pub fn eval_uuid(args: &[Expression]) -> Result<Option<ComparableValue>> {
    check_arity(args, 0, "UUID")?;
    Ok(Some(ComparableValue::Iri(Arc::from(format!(
        "urn:uuid:{}",
        Uuid::new_v4()
    )))))
}

pub fn eval_struuid(args: &[Expression]) -> Result<Option<ComparableValue>> {
    check_arity(args, 0, "STRUUID")?;
    Ok(Some(ComparableValue::String(Arc::from(
        Uuid::new_v4().to_string(),
    ))))
}
