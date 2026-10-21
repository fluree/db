//! The hashes this instance mints and admits commit and transaction CIDs
//! with.
//!
//! SHA2-256 is the default and always admitted. An alternative takes effect
//! only while every reader of this instance's content is known to support
//! it: a standalone instance is its own reader, while a cluster member's
//! readers are its members, whose capabilities are not yet established, so
//! a member mints SHA2-256 and refuses content hashed otherwise (a push, a
//! pull, a clone, a restored archive). Verification never depends on this choice: it
//! follows the CID being verified.

use std::sync::atomic::{AtomicBool, Ordering};

use fluree_db_core::{ContentId, HashAlgorithm};

use crate::Fluree;

/// The configured algorithm and whether its readers are confirmed.
#[derive(Debug)]
pub(crate) struct ContentHashPolicy {
    configured: HashAlgorithm,
    confirmed: AtomicBool,
}

impl ContentHashPolicy {
    pub(crate) fn new(configured: HashAlgorithm) -> Self {
        Self {
            configured,
            confirmed: AtomicBool::new(true),
        }
    }

    fn confirmed(&self) -> bool {
        self.confirmed.load(Ordering::Acquire)
    }

    fn effective(&self) -> HashAlgorithm {
        if self.confirmed() {
            self.configured
        } else {
            HashAlgorithm::Sha2_256
        }
    }
}

impl Default for ContentHashPolicy {
    fn default() -> Self {
        Self::new(HashAlgorithm::Sha2_256)
    }
}

impl Fluree {
    /// The algorithm new commit and transaction CIDs are minted with.
    pub fn content_hash(&self) -> HashAlgorithm {
        self.content_hash.effective()
    }

    /// The algorithm this instance was configured with, in effect or not.
    pub fn configured_content_hash(&self) -> HashAlgorithm {
        self.content_hash.configured
    }

    /// Whether content under `id` may enter this instance: every reader of
    /// its content must be able to verify it.
    pub fn admits_content_id(&self, id: &ContentId) -> bool {
        match id.hash_algorithm() {
            Some(HashAlgorithm::Sha2_256) => true,
            Some(_) => self.content_hash.confirmed(),
            None => false,
        }
    }

    /// Refuse content under `id` this instance does not admit.
    pub(crate) fn admit_content_id(&self, id: &ContentId) -> crate::Result<()> {
        if self.admits_content_id(id) {
            Ok(())
        } else {
            Err(crate::ApiError::http(
                422,
                format!("{id} is not hashed with SHA2-256, the only hash this cluster admits"),
            ))
        }
    }

    /// Whether every reader of this instance's content supports any
    /// algorithm it supports. A cluster member sets this from its members'
    /// capabilities; while it is `false`, the instance mints SHA2-256 and
    /// admits nothing else.
    pub fn set_content_hash_readers_confirmed(&self, confirmed: bool) {
        let was = self
            .content_hash
            .confirmed
            .swap(confirmed, Ordering::AcqRel);
        if was != confirmed {
            tracing::info!(
                configured = %self.content_hash.configured,
                in_effect = %self.content_hash(),
                readers_confirmed = confirmed,
                "content hash for new commits"
            );
        }
    }
}
