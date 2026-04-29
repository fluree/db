//! Conflict key for rebase conflict detection
//!
//! A `ConflictKey` identifies a unique (subject, predicate, graph) tuple
//! used to detect overlapping modifications between branches during rebase.

use crate::sid::Sid;
use serde::Serialize;

/// A (subject, predicate, graph) tuple identifying a data point that may conflict
/// between branch and source during rebase.
///
/// `Ord`/`PartialOrd` are derived to support stable, lexicographic ordering
/// of conflict sets — important for capped/paginated conflict previews where
/// `HashSet::intersection` order is otherwise unspecified.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct ConflictKey {
    pub s: Sid,
    pub p: Sid,
    pub g: Option<Sid>,
}

impl ConflictKey {
    pub fn new(s: Sid, p: Sid, g: Option<Sid>) -> Self {
        Self { s, p, g }
    }
}
