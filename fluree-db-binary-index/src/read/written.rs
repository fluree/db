//! Objects an index build just wrote, held for a reader in the same process.
//!
//! A query server that shares a process with the indexer installs each new
//! root moments after the build wrote it. The build keeps the small objects
//! an install reads (branch manifests, dictionary branches, statistics store
//! objects) and hands them over with the root, so the install takes them
//! from memory instead of reading them back from storage.

use std::collections::HashMap;
use std::sync::Arc;

use fluree_db_core::{ContentBytes, ContentId, ContentStore};

use crate::format::index_root::IndexRoot;

/// Bytes by content id: exactly what storage holds under each id.
#[derive(Clone, Default)]
pub struct WrittenObjects {
    objects: HashMap<ContentId, Arc<[u8]>>,
    bytes: usize,
}

impl std::fmt::Debug for WrittenObjects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WrittenObjects")
            .field("objects", &self.objects.len())
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl WrittenObjects {
    /// Keep `bytes` as `id`'s. Returns false, keeping nothing, when that
    /// would take the held bytes past `cap`.
    pub fn insert(&mut self, id: ContentId, bytes: &[u8], cap: usize) -> bool {
        if self.objects.contains_key(&id) {
            return true;
        }
        if self.bytes + bytes.len() > cap {
            return false;
        }
        self.bytes += bytes.len();
        self.objects.insert(id, Arc::from(bytes));
        true
    }

    pub fn ids(&self) -> impl Iterator<Item = &ContentId> {
        self.objects.keys()
    }

    pub fn get(&self, id: &ContentId) -> Option<ContentBytes> {
        self.objects
            .get(id)
            .map(|b| ContentBytes::Shared(Arc::clone(b)))
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// The bytes held.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

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
    pub fn new(root: IndexRoot, bytes: &[u8]) -> Self {
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

/// `id`'s bytes from `written` when it holds them, else from `cs`.
pub async fn fetch(
    cs: &dyn ContentStore,
    written: Option<&WrittenObjects>,
    id: &ContentId,
) -> fluree_db_core::Result<ContentBytes> {
    match written.and_then(|w| w.get(id)) {
        Some(bytes) => Ok(bytes),
        None => cs.get(id).await,
    }
}
