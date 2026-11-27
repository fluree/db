//! What a build hands a query server in the same process, so installing the
//! index it built reads none of it back.

use fluree_db_binary_index::dict::branch::{BRANCH_MAGIC, SCHEMED_BRANCH_MAGIC};
pub use fluree_db_binary_index::read::written::IndexHandoff;
use fluree_db_core::ContentKind;

/// What one build can hold for a reader beyond the root, at most.
pub(crate) const WRITTEN_CAP_BYTES: usize = 64 << 20;

/// Whether an install reads objects of `kind` with these `bytes` whole:
/// branch manifests, dictionary branches and statistics store objects.
/// Leaves are left to the read cache the build warms.
pub(crate) fn install_reads(kind: ContentKind, bytes: &[u8]) -> bool {
    match kind {
        ContentKind::IndexBranch | ContentKind::StatsSketch => true,
        ContentKind::DictBlob { .. } => bytes.get(..4).is_some_and(|magic| {
            magic == BRANCH_MAGIC.as_slice() || magic == SCHEMED_BRANCH_MAGIC.as_slice()
        }),
        _ => false,
    }
}
