//! What a build hands a query server in the same process, so installing the
//! index it built reads none of it back.

use fluree_db_binary_index::dict::branch::{BRANCH_MAGIC, SCHEMED_BRANCH_MAGIC};
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_binary_index::read::written::WrittenObjects;
use fluree_db_core::ContentKind;

/// What one build can hold for a reader beyond the root, at most.
pub(crate) const WRITTEN_CAP_BYTES: usize = 64 << 20;

/// A built root as an install needs it: the root decoded, and the objects
/// the build wrote that an install reads (branch manifests, dictionary
/// branches, statistics store objects).
#[derive(Clone)]
pub struct IndexHandoff {
    /// The root as decoding its stored bytes gives it.
    pub root: IndexRoot,
    pub written: WrittenObjects,
}

impl std::fmt::Debug for IndexHandoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexHandoff")
            .field("index_t", &self.root.index_t)
            .field("written", &self.written)
            .finish()
    }
}

impl IndexHandoff {
    /// `root` as [`IndexRoot::encode_canonical`] left it, with that
    /// encoding `bytes`, and no written objects yet.
    pub(crate) fn new(root: IndexRoot, bytes: &[u8]) -> Self {
        #[cfg(debug_assertions)]
        {
            let decoded = format!(
                "{:?}",
                IndexRoot::decode(bytes).expect("a root decodes from its encoding")
            );
            let held = format!("{root:?}");
            if decoded != held {
                let at = decoded
                    .bytes()
                    .zip(held.bytes())
                    .position(|(a, b)| a != b)
                    .unwrap_or(decoded.len().min(held.len()));
                let around = |s: &str| {
                    let b = s.as_bytes();
                    String::from_utf8_lossy(&b[at.saturating_sub(300)..(at + 200).min(b.len())])
                        .into_owned()
                };
                panic!(
                    "a root handed to an install must be what decoding it gives; at byte {at}\n\
                     decoded: {}\n   held: {}",
                    around(&decoded),
                    around(&held)
                );
            }
        }
        #[cfg(not(debug_assertions))]
        let _ = bytes;
        Self {
            root,
            written: WrittenObjects::default(),
        }
    }
}

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
