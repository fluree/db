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
