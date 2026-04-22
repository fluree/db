//! Conflict key for rebase conflict detection
//!
//! A `ConflictKey` identifies a unique (subject, predicate, graph) tuple
//! used to detect overlapping modifications between branches during rebase.

use crate::sid::Sid;

/// A (subject, predicate, graph) tuple identifying a data point that may conflict
/// between branch and source during rebase.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
