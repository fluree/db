//! Transaction head reads and independent index publication; all other writes fenced.
use super::*;
use fluree_db_nameservice::*;

fn ns_err(e: impl std::fmt::Display) -> NameServiceError {
    NameServiceError::Storage(e.to_string())
}
fn unsupported<T>() -> fluree_db_nameservice::Result<T> {
    Err(NameServiceError::ApplyRejected(
        "unsupported operation on experimental journal ledger".into(),
    ))
}
impl Surface {
    fn record(&self, id: &str) -> fluree_db_nameservice::Result<Option<NsRecord>> {
        let id = fluree_db_core::ledger_id::normalize_ledger_id(id)?;
        let record = self.read().map_err(ns_err)?.record;
        Ok((record.ledger_id == id).then_some(record))
    }
}
#[async_trait]
impl NameServiceLookup for Surface {
    async fn lookup(&self, id: &str) -> fluree_db_nameservice::Result<Option<NsRecord>> {
        self.record(id)
    }
    async fn all_records(&self) -> fluree_db_nameservice::Result<Vec<NsRecord>> {
        Ok(vec![self.read().map_err(ns_err)?.record])
    }
}
#[async_trait]
impl RefLookup for Surface {
    async fn get_ref(
        &self,
        id: &str,
        kind: RefKind,
    ) -> fluree_db_nameservice::Result<Option<RefValue>> {
        Ok(self.record(id)?.map(|r| match kind {
            RefKind::CommitHead => RefValue {
                id: r.commit_head_id,
                t: r.commit_t,
            },
            RefKind::IndexHead => RefValue {
                id: r.index_head_id,
                t: r.index_t,
            },
        }))
    }
}
#[async_trait]
impl StatusLookup for Surface {
    async fn get_status(&self, id: &str) -> fluree_db_nameservice::Result<Option<StatusValue>> {
        Ok(self
            .record(id)?
            .map(|_| StatusValue::new(1, StatusPayload::new("ready"))))
    }
}
#[async_trait]
impl ConfigLookup for Surface {
    async fn get_config(&self, id: &str) -> fluree_db_nameservice::Result<Option<ConfigValue>> {
        Ok(self.record(id)?.map(|r| match r.default_context {
            Some(cid) => ConfigValue::new(1, Some(ConfigPayload::with_default_context(cid))),
            None => ConfigValue::unborn(),
        }))
    }
}
#[async_trait]
impl GraphSourceLookup for Surface {
    async fn lookup_graph_source(
        &self,
        _: &str,
    ) -> fluree_db_nameservice::Result<Option<GraphSourceRecord>> {
        Ok(None)
    }
    async fn lookup_any(&self, id: &str) -> fluree_db_nameservice::Result<NsLookupResult> {
        Ok(match self.record(id)? {
            Some(r) => NsLookupResult::Ledger(r),
            None => NsLookupResult::NotFound,
        })
    }
    async fn all_graph_source_records(
        &self,
    ) -> fluree_db_nameservice::Result<Vec<GraphSourceRecord>> {
        Ok(vec![])
    }
}
#[async_trait]
impl IndexPublisher for Surface {
    async fn publish_index(
        &self,
        id: &str,
        t: i64,
        cid: &ContentId,
    ) -> fluree_db_nameservice::Result<()> {
        let record = self
            .record(id)?
            .ok_or_else(|| NameServiceError::NotFound(id.into()))?;
        if t <= 0 || t > record.commit_t || cid.content_kind() != Some(ContentKind::IndexRoot) {
            return unsupported();
        }
        // Independent worker I/O, never the transaction gate. Serialize index
        // publishers only; transaction installs merge the last advertised index.
        let _gate = self.index_gate.lock().await;
        let bytes = self.outputs.get(cid).await.map_err(ns_err)?;
        if !cid.verify(&bytes) {
            return Err(ns_err("index CID mismatch"));
        }
        let root = fluree_db_binary_index::IndexRoot::decode(&bytes).map_err(ns_err)?;
        if root.ledger_id != record.ledger_id || root.index_t != t {
            return unsupported();
        }
        self.index_ns.publish_index(id, t, cid).await?;
        let mut published = self.published.write().unwrap();
        if t > published.record.index_t {
            published.record.index_t = t;
            published.record.index_head_id = Some(cid.clone());
        }
        Ok(())
    }
}
#[async_trait]
impl CommitPublisher for Surface {
    async fn publish_commit(
        &self,
        _: &str,
        _: i64,
        _: &ContentId,
    ) -> fluree_db_nameservice::Result<()> {
        unsupported()
    }
    fn publishing_ledger_id(&self, id: &str) -> Option<String> {
        Some(id.into())
    }
}
#[async_trait]
impl RefPublisher for Surface {
    async fn compare_and_set_ref(
        &self,
        _: &str,
        _: RefKind,
        _: Option<&RefValue>,
        _: &RefValue,
    ) -> fluree_db_nameservice::Result<CasResult> {
        unsupported()
    }
}
#[async_trait]
impl BranchLifecycle for Surface {
    async fn create_branch(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: Option<(ContentId, i64)>,
    ) -> fluree_db_nameservice::Result<()> {
        unsupported()
    }
    async fn drop_branch(&self, _: &str) -> fluree_db_nameservice::Result<Option<u32>> {
        unsupported()
    }
    async fn reset_head(&self, _: &str, _: NsRecordSnapshot) -> fluree_db_nameservice::Result<()> {
        unsupported()
    }
}
#[async_trait]
impl LedgerLifecycle for Surface {
    async fn init(&self, _: &str) -> fluree_db_nameservice::Result<()> {
        unsupported()
    }
    async fn retract(&self, _: &str) -> fluree_db_nameservice::Result<()> {
        unsupported()
    }
    async fn purge(&self, _: &str) -> fluree_db_nameservice::Result<()> {
        unsupported()
    }
}
#[async_trait]
impl AdminPublisher for Surface {
    async fn publish_index_allow_equal(
        &self,
        _: &str,
        _: i64,
        _: &ContentId,
    ) -> fluree_db_nameservice::Result<()> {
        unsupported()
    }
}
#[async_trait]
impl GraphSourcePublisher for Surface {
    async fn publish_graph_source(
        &self,
        _: &str,
        _: &str,
        _: GraphSourceType,
        _: &str,
        _: &[String],
    ) -> fluree_db_nameservice::Result<()> {
        unsupported()
    }
    async fn publish_graph_source_index(
        &self,
        _: &str,
        _: &str,
        _: &ContentId,
        _: i64,
    ) -> fluree_db_nameservice::Result<()> {
        unsupported()
    }
    async fn retract_graph_source(&self, _: &str, _: &str) -> fluree_db_nameservice::Result<()> {
        unsupported()
    }
}
#[async_trait]
impl StatusPublisher for Surface {
    async fn push_status(
        &self,
        _: &str,
        _: Option<&StatusValue>,
        _: &StatusValue,
    ) -> fluree_db_nameservice::Result<StatusCasResult> {
        unsupported()
    }
}
#[async_trait]
impl ConfigPublisher for Surface {
    async fn push_config(
        &self,
        _: &str,
        _: Option<&ConfigValue>,
        _: &ConfigValue,
    ) -> fluree_db_nameservice::Result<ConfigCasResult> {
        unsupported()
    }
}
