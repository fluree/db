//! Path function implementations (Cypher path values).
//!
//! Path values are produced by the shortest-path operator and carried in
//! [`Binding::Path`]. `length(p)` returns the hop count (`nodes - 1`).

use crate::binding::{Binding, RowAccess};
use crate::error::{QueryError, Result};
use crate::ir::Expression;

use super::value::ComparableValue;

/// `length(p)` — number of relationships (hops) in a path value.
///
/// Returns `None` (→ null / unbound) when the argument is unbound, which is
/// how `OPTIONAL MATCH p = shortestPath(...)` surfaces "no path" to a
/// `CASE p IS NULL` guard.
pub fn eval_path_length<R: RowAccess>(
    args: &[Expression],
    row: &R,
) -> Result<Option<ComparableValue>> {
    if args.len() != 1 {
        return Err(QueryError::InvalidFilter(format!(
            "length() expects 1 argument, got {}",
            args.len()
        )));
    }
    let Expression::Var(var) = &args[0] else {
        return Err(QueryError::InvalidFilter(
            "length() argument must be a path variable".to_string(),
        ));
    };
    match row.get(*var) {
        Some(Binding::Path(nodes)) => {
            // Hop count = edges = nodes - 1 (a single-node path has length 0).
            let hops = nodes.len().saturating_sub(1) as i64;
            Ok(Some(ComparableValue::Long(hops)))
        }
        Some(Binding::Unbound | Binding::Poisoned) | None => Ok(None),
        Some(_) => Err(QueryError::InvalidFilter(
            "length() argument is not a path value".to_string(),
        )),
    }
}
