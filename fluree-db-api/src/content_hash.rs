//! The hash this instance mints commit and transaction CIDs with.
//!
//! SHA2-256 is the default and always in effect. An alternative takes effect
//! only once every reader of this instance's writes is known to support it:
//! a standalone instance is its own reader, while a cluster node waits until
//! each member has reported support. Until then, writes use SHA2-256, which
//! every reader verifies. Verification never depends on this choice: it
//! follows the CID being verified.

use std::sync::atomic::{AtomicBool, Ordering};

use fluree_db_core::HashAlgorithm;

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

    fn effective(&self) -> HashAlgorithm {
        if self.confirmed.load(Ordering::Acquire) {
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

    /// Whether every reader of this instance's writes supports the
    /// configured algorithm. A cluster node sets this from its members'
    /// capabilities; while it is `false`, writes use SHA2-256.
    pub fn set_content_hash_readers_confirmed(&self, confirmed: bool) {
        let confirmed = confirmed || self.content_hash.configured == HashAlgorithm::Sha2_256;
        let was = self
            .content_hash
            .confirmed
            .swap(confirmed, Ordering::AcqRel);
        if was != confirmed {
            tracing::info!(
                configured = %self.content_hash.configured,
                in_effect = %self.content_hash(),
                "content hash for new commits"
            );
        }
    }
}
