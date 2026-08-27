//! Shared plumbing for the residency-mode drain/fetch/re-run loop.
//!
//! On targets with no sync→async bridge (`wasm32`, or native under the
//! `residency` feature) the binary-index read path serves bytes only from a
//! store's resident tier and records what it could not serve into the store's
//! miss register. Any frame that can be re-run then recovers by draining the
//! register, fetching the wants, and trying again.
//!
//! Two such frames exist, and they must BOTH be wrapped, because encoded
//! bindings are materialized late:
//!
//! - **execution** — `Fluree::query`'s plan+execute round
//!   (`crate::view::query`), and
//! - **formatting** — [`crate::format::format_results_async`], where a
//!   `Binding::Encoded*` is finally resolved to an IRI or a literal through
//!   the forward packs. Those packs are frequently NOT touched during
//!   execution, so a peer whose execution round succeeded can still take its
//!   first miss here.
//!
//! Both frames are pure functions of resident state, so a re-run is always
//! safe; both use this module's [`content_store`] to find the store to drain.

use std::sync::Arc;

use fluree_db_core::{ContentStore, LedgerSnapshot};

/// The content store behind a snapshot's range provider, if it participates
/// in residency (i.e. exposes a miss register). `None` on every ordinary
/// native store, which is what keeps the retry loops inert off-wasm.
pub(crate) fn content_store(snapshot: &LedgerSnapshot) -> Option<Arc<dyn ContentStore>> {
    let provider = snapshot.range_provider.as_ref()?;
    let brp = provider
        .as_any()
        .downcast_ref::<fluree_db_query::BinaryRangeProvider>()?;
    let cs = brp.store().content_store()?.clone();
    cs.miss_register().is_some().then_some(cs)
}
