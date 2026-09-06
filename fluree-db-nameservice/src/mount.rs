//! Remote mounts: alias-namespaced composition of nameservices.
//!
//! A [`CompositeNameService`] presents one nameservice built from a local
//! read-write nameservice plus any number of read-only remote mounts. Each
//! mount claims an alias prefix: with mount `acme`, the remote's ledger
//! `inventory:main` appears locally as `acme/inventory:main`.
//!
//! Reads route by prefix — a mounted alias is stripped of its prefix,
//! resolved against the mount's lookup (typically a `ProxyNameService` from
//! `fluree-db-nameservice-sync`), and the returned records are re-localized
//! so every downstream consumer (ledger cache, content-store namespacing,
//! branched-store ancestry walks) keys on the local alias. Writes to mounted
//! aliases are rejected with a clear error; writes to everything else
//! delegate to the local publisher. This gives per-alias write capability on
//! top of [`NameServiceMode`]'s instance-level split.
//!
//! Mount prefixes shadow local ledgers whose names start with the same
//! segment — pick mount names that don't collide with local ledger name
//! prefixes (enforced by [`CompositeNameService::new`] only against other
//! mounts, since local ledgers can be created later).

use crate::{
    AdminPublisher, BranchLifecycle, CasResult, CommitPublisher, ConfigCasResult, ConfigLookup,
    ConfigPublisher, ConfigValue, GraphSourceLookup, GraphSourcePublisher, GraphSourceRecord,
    GraphSourceType, IndexPublisher, LedgerHeads, LedgerLifecycle, NameServiceError,
    NameServiceLookup, NameServicePublisher, NsLookupResult, NsRecord, NsRecordSnapshot, RefKind,
    RefLookup, RefPublisher, RefValue, Result, StatusCasResult, StatusLookup, StatusPublisher,
    StatusValue,
};
use async_trait::async_trait;
use fluree_db_core::ContentId;
use std::fmt::Debug;
use std::sync::Arc;

/// One read-only remote mount: an alias prefix and the lookup that serves it.
#[derive(Clone)]
pub struct RemoteMount {
    prefix: String,
    lookup: Arc<dyn NameServiceLookup>,
}

impl Debug for RemoteMount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteMount")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl RemoteMount {
    /// Create a mount serving aliases under `prefix` (no trailing slash).
    pub fn new(prefix: impl Into<String>, lookup: Arc<dyn NameServiceLookup>) -> Self {
        Self {
            prefix: prefix.into(),
            lookup,
        }
    }

    /// The alias prefix this mount claims.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Strip this mount's prefix from a local alias, returning the remote
    /// alias. `None` if the alias is not under this mount.
    fn remote_alias<'a>(&self, local: &'a str) -> Option<&'a str> {
        local
            .strip_prefix(self.prefix.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
    }

    /// Rewrite a remote record so all identity fields carry the local
    /// (prefixed) alias. Branch names are unprefixed and stay as-is.
    fn localize_record(&self, mut record: NsRecord) -> NsRecord {
        record.ledger_id = format!("{}/{}", self.prefix, record.ledger_id);
        record.name = format!("{}/{}", self.prefix, record.name);
        record
    }

    /// Rewrite a remote graph-source record onto the local alias namespace.
    fn localize_graph_source(&self, mut record: GraphSourceRecord) -> GraphSourceRecord {
        record.graph_source_id = format!("{}/{}", self.prefix, record.graph_source_id);
        record.name = format!("{}/{}", self.prefix, record.name);
        record
    }
}

/// A nameservice composed of a local read-write nameservice and N read-only
/// remote mounts, routed by alias prefix.
///
/// Implements the full [`NameServicePublisher`] surface: reads route to the
/// owning mount (or the local nameservice), writes to mounted aliases fail
/// with a "read-only remote mount" error, and writes to local aliases
/// delegate to the local publisher.
#[derive(Clone)]
pub struct CompositeNameService {
    local: Arc<dyn NameServicePublisher>,
    mounts: Vec<RemoteMount>,
}

impl Debug for CompositeNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeNameService")
            .field("mounts", &self.mounts)
            .finish_non_exhaustive()
    }
}

impl CompositeNameService {
    /// Compose a local publisher with remote mounts.
    ///
    /// # Errors
    /// Returns an error if two mounts claim the same prefix.
    pub fn new(local: Arc<dyn NameServicePublisher>, mounts: Vec<RemoteMount>) -> Result<Self> {
        for (i, a) in mounts.iter().enumerate() {
            for b in mounts.iter().skip(i + 1) {
                if a.prefix == b.prefix {
                    return Err(NameServiceError::storage(format!(
                        "duplicate remote mount prefix '{}'",
                        a.prefix
                    )));
                }
            }
        }
        Ok(Self { local, mounts })
    }

    /// The mounts this composite routes to.
    pub fn mounts(&self) -> &[RemoteMount] {
        &self.mounts
    }

    /// Find the mount owning `alias`, with the remote-side alias.
    fn mount_for<'a>(&self, alias: &'a str) -> Option<(&RemoteMount, &'a str)> {
        self.mounts
            .iter()
            .find_map(|m| m.remote_alias(alias).map(|remote| (m, remote)))
    }

    fn reject_mounted_write(&self, alias: &str) -> Option<NameServiceError> {
        self.mount_for(alias).map(|(mount, _)| {
            NameServiceError::storage(format!(
                "'{alias}' is a read-only remote mount ('{}'); writes must go to the origin server",
                mount.prefix
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// Read surface: route by prefix
// ---------------------------------------------------------------------------

#[async_trait]
impl NameServiceLookup for CompositeNameService {
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        match self.mount_for(ledger_id) {
            Some((mount, remote)) => Ok(mount
                .lookup
                .lookup(remote)
                .await?
                .map(|r| mount.localize_record(r))),
            None => self.local.lookup(ledger_id).await,
        }
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        // Local records plus whatever each mount can enumerate. Proxy-backed
        // mounts return an empty list by design (discovery is per-alias), so
        // this is typically just the local set.
        let mut records = self.local.all_records().await?;
        for mount in &self.mounts {
            let remote = mount.lookup.all_records().await?;
            records.extend(remote.into_iter().map(|r| mount.localize_record(r)));
        }
        Ok(records)
    }

    async fn list_branches(&self, ledger_name: &str) -> Result<Vec<NsRecord>> {
        match self.mount_for(ledger_name) {
            Some((mount, remote)) => Ok(mount
                .lookup
                .list_branches(remote)
                .await?
                .into_iter()
                .map(|r| mount.localize_record(r))
                .collect()),
            None => self.local.list_branches(ledger_name).await,
        }
    }

    async fn heads(&self, ledger_id: &str) -> Result<Option<LedgerHeads>> {
        match self.mount_for(ledger_id) {
            Some((mount, remote)) => mount.lookup.heads(remote).await,
            None => self.local.heads(ledger_id).await,
        }
    }
}

#[async_trait]
impl GraphSourceLookup for CompositeNameService {
    async fn lookup_graph_source(
        &self,
        graph_source_id: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        match self.mount_for(graph_source_id) {
            Some((mount, remote)) => Ok(mount
                .lookup
                .lookup_graph_source(remote)
                .await?
                .map(|r| mount.localize_graph_source(r))),
            None => self.local.lookup_graph_source(graph_source_id).await,
        }
    }

    async fn lookup_any(&self, resource_id: &str) -> Result<NsLookupResult> {
        match self.mount_for(resource_id) {
            Some((mount, remote)) => Ok(match mount.lookup.lookup_any(remote).await? {
                NsLookupResult::Ledger(r) => NsLookupResult::Ledger(mount.localize_record(r)),
                NsLookupResult::GraphSource(r) => {
                    NsLookupResult::GraphSource(mount.localize_graph_source(r))
                }
                NsLookupResult::NotFound => NsLookupResult::NotFound,
            }),
            None => self.local.lookup_any(resource_id).await,
        }
    }

    async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
        let mut records = self.local.all_graph_source_records().await?;
        for mount in &self.mounts {
            let remote = mount.lookup.all_graph_source_records().await?;
            records.extend(remote.into_iter().map(|r| mount.localize_graph_source(r)));
        }
        Ok(records)
    }
}

#[async_trait]
impl RefLookup for CompositeNameService {
    async fn get_ref(&self, ledger_id: &str, kind: RefKind) -> Result<Option<RefValue>> {
        match self.mount_for(ledger_id) {
            Some((mount, remote)) => mount.lookup.get_ref(remote, kind).await,
            None => self.local.get_ref(ledger_id, kind).await,
        }
    }
}

#[async_trait]
impl StatusLookup for CompositeNameService {
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>> {
        match self.mount_for(ledger_id) {
            Some((mount, remote)) => mount.lookup.get_status(remote).await,
            None => self.local.get_status(ledger_id).await,
        }
    }
}

#[async_trait]
impl ConfigLookup for CompositeNameService {
    async fn get_config(&self, ledger_id: &str) -> Result<Option<ConfigValue>> {
        match self.mount_for(ledger_id) {
            Some((mount, remote)) => mount.lookup.get_config(remote).await,
            None => self.local.get_config(ledger_id).await,
        }
    }
}

// ---------------------------------------------------------------------------
// Write surface: reject mounted aliases, delegate the rest to local
// ---------------------------------------------------------------------------

#[async_trait]
impl CommitPublisher for CompositeNameService {
    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local
            .publish_commit(ledger_id, commit_t, commit_id)
            .await
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        if self.mount_for(ledger_id).is_some() {
            return None;
        }
        self.local.publishing_ledger_id(ledger_id)
    }
}

#[async_trait]
impl IndexPublisher for CompositeNameService {
    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local.publish_index(ledger_id, index_t, index_id).await
    }
}

#[async_trait]
impl LedgerLifecycle for CompositeNameService {
    async fn init(&self, ledger_id: &str) -> Result<()> {
        // Also prevents creating a local ledger that would shadow a mount.
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local.init(ledger_id).await
    }

    async fn retract(&self, ledger_id: &str) -> Result<()> {
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local.retract(ledger_id).await
    }

    async fn purge(&self, ledger_id: &str) -> Result<()> {
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local.purge(ledger_id).await
    }
}

#[async_trait]
impl BranchLifecycle for CompositeNameService {
    async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: &str,
        at_commit: Option<(ContentId, i64)>,
    ) -> Result<()> {
        if let Some(err) = self.reject_mounted_write(ledger_name) {
            return Err(err);
        }
        self.local
            .create_branch(ledger_name, new_branch, source_branch, at_commit)
            .await
    }

    async fn drop_branch(&self, ledger_id: &str) -> Result<Option<u32>> {
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local.drop_branch(ledger_id).await
    }

    async fn reset_head(&self, ledger_id: &str, snapshot: NsRecordSnapshot) -> Result<()> {
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local.reset_head(ledger_id, snapshot).await
    }

    async fn pending_commit_cids(
        &self,
        ledger_id: &str,
        since_t: i64,
    ) -> Result<Option<Vec<(i64, ContentId)>>> {
        if self.mount_for(ledger_id).is_some() {
            // No commit-CID index for mounts; callers fall back to the DAG walk.
            return Ok(None);
        }
        self.local.pending_commit_cids(ledger_id, since_t).await
    }

    async fn prune_commit_index(&self, ledger_id: &str, up_to_t: i64) -> Result<()> {
        if self.mount_for(ledger_id).is_some() {
            return Ok(());
        }
        self.local.prune_commit_index(ledger_id, up_to_t).await
    }
}

#[async_trait]
impl AdminPublisher for CompositeNameService {
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local
            .publish_index_allow_equal(ledger_id, index_t, index_id)
            .await
    }
}

#[async_trait]
impl RefPublisher for CompositeNameService {
    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local
            .compare_and_set_ref(ledger_id, kind, expected, new)
            .await
    }
}

#[async_trait]
impl GraphSourcePublisher for CompositeNameService {
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()> {
        if let Some(err) = self.reject_mounted_write(name) {
            return Err(err);
        }
        self.local
            .publish_graph_source(name, branch, source_type, config, dependencies)
            .await
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &ContentId,
        index_t: i64,
    ) -> Result<()> {
        if let Some(err) = self.reject_mounted_write(name) {
            return Err(err);
        }
        self.local
            .publish_graph_source_index(name, branch, index_id, index_t)
            .await
    }

    async fn retract_graph_source(&self, name: &str, branch: &str) -> Result<()> {
        if let Some(err) = self.reject_mounted_write(name) {
            return Err(err);
        }
        self.local.retract_graph_source(name, branch).await
    }
}

#[async_trait]
impl StatusPublisher for CompositeNameService {
    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult> {
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local.push_status(ledger_id, expected, new).await
    }
}

#[async_trait]
impl ConfigPublisher for CompositeNameService {
    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        if let Some(err) = self.reject_mounted_write(ledger_id) {
            return Err(err);
        }
        self.local.push_config(ledger_id, expected, new).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryNameService;

    fn mounted_composite() -> (Arc<MemoryNameService>, CompositeNameService) {
        let local = Arc::new(MemoryNameService::new());
        let remote = Arc::new(MemoryNameService::new());
        let composite = CompositeNameService::new(
            local.clone(),
            vec![RemoteMount::new("acme", remote.clone())],
        )
        .expect("composite");
        (remote, composite)
    }

    #[tokio::test]
    async fn lookup_routes_by_prefix_and_localizes() {
        let (remote, composite) = mounted_composite();
        remote.init("inventory:main").await.expect("init remote");

        let record = composite
            .lookup("acme/inventory:main")
            .await
            .expect("lookup")
            .expect("mounted record found");
        assert_eq!(record.ledger_id, "acme/inventory:main");
        assert_eq!(record.name, "acme/inventory");
        assert_eq!(record.branch, "main");

        // The local nameservice does not see mounted aliases.
        assert!(composite
            .lookup("inventory:main")
            .await
            .expect("local lookup")
            .is_none());
    }

    #[tokio::test]
    async fn local_aliases_route_to_local_publisher() {
        let (_remote, composite) = mounted_composite();
        composite.init("books:main").await.expect("init local");
        let record = composite
            .lookup("books:main")
            .await
            .expect("lookup")
            .expect("local record");
        assert_eq!(record.ledger_id, "books:main");
    }

    #[tokio::test]
    async fn writes_to_mounted_aliases_are_rejected() {
        let (remote, composite) = mounted_composite();
        remote.init("inventory:main").await.expect("init remote");

        let err = composite
            .init("acme/other:main")
            .await
            .expect_err("init on mount must fail");
        assert!(
            err.to_string().contains("read-only remote mount"),
            "unexpected error: {err}"
        );

        let err = composite
            .publish_commit(
                "acme/inventory:main",
                1,
                &ContentId::new(fluree_db_core::ContentKind::Commit, b"c"),
            )
            .await
            .expect_err("publish_commit on mount must fail");
        assert!(err.to_string().contains("read-only remote mount"));
    }

    #[tokio::test]
    async fn prefix_requires_separator() {
        let (_remote, composite) = mounted_composite();
        // "acmecorp:main" starts with "acme" but is not under the mount.
        composite.init("acmecorp:main").await.expect("local init");
        let record = composite
            .lookup("acmecorp:main")
            .await
            .expect("lookup")
            .expect("local record");
        assert_eq!(record.ledger_id, "acmecorp:main");
    }

    #[test]
    fn duplicate_prefixes_rejected() {
        let local = Arc::new(MemoryNameService::new());
        let remote = Arc::new(MemoryNameService::new());
        let result = CompositeNameService::new(
            local,
            vec![
                RemoteMount::new("acme", remote.clone()),
                RemoteMount::new("acme", remote),
            ],
        );
        assert!(result.is_err());
    }
}
