//! A read-only nameservice that records every id it is asked about and
//! delegates to the real one, so a test can see which ledgers a query read.

use async_trait::async_trait;
use fluree_db_nameservice::{
    ConfigLookup, ConfigValue, GraphSourceLookup, GraphSourceRecord, LedgerHeads,
    NameServiceLookup, NameServicePublisher, NsLookupResult, NsRecord, RefKind, RefLookup,
    RefValue, Result, StatusLookup, StatusValue,
};
use std::sync::{Arc, Mutex};

/// What a lookup asked about a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Asked {
    /// A read of a ledger's (or any resource's) own record or heads.
    Record,
    /// A probe for a graph-source record under the id.
    GraphSource,
}

#[derive(Debug)]
pub struct RecordingLookups {
    inner: Arc<dyn NameServicePublisher>,
    asked: Mutex<Vec<(Asked, String)>>,
}

impl RecordingLookups {
    pub fn new(inner: Arc<dyn NameServicePublisher>) -> Self {
        Self {
            inner,
            asked: Mutex::new(Vec::new()),
        }
    }

    /// Whether any lookup named an id containing `needle`.
    pub fn asked_about(&self, needle: &str) -> bool {
        self.asked
            .lock()
            .unwrap()
            .iter()
            .any(|(_, id)| id.contains(needle))
    }

    /// Every id whose own record a lookup read (graph-source probes aside).
    pub fn records_read(&self) -> Vec<String> {
        self.asked
            .lock()
            .unwrap()
            .iter()
            .filter(|(kind, _)| *kind == Asked::Record)
            .map(|(_, id)| id.clone())
            .collect()
    }

    /// Everything asked, in order.
    pub fn asked(&self) -> Vec<(Asked, String)> {
        self.asked.lock().unwrap().clone()
    }

    fn record(&self, kind: Asked, id: &str) {
        self.asked.lock().unwrap().push((kind, id.to_string()));
    }
}

#[async_trait]
impl GraphSourceLookup for RecordingLookups {
    async fn lookup_graph_source(&self, id: &str) -> Result<Option<GraphSourceRecord>> {
        self.record(Asked::GraphSource, id);
        self.inner.lookup_graph_source(id).await
    }
    async fn lookup_any(&self, id: &str) -> Result<NsLookupResult> {
        self.record(Asked::Record, id);
        self.inner.lookup_any(id).await
    }
    async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
        self.inner.all_graph_source_records().await
    }
}

#[async_trait]
impl RefLookup for RecordingLookups {
    async fn get_ref(&self, id: &str, kind: RefKind) -> Result<Option<RefValue>> {
        self.record(Asked::Record, id);
        self.inner.get_ref(id, kind).await
    }
}

#[async_trait]
impl StatusLookup for RecordingLookups {
    async fn get_status(&self, id: &str) -> Result<Option<StatusValue>> {
        self.record(Asked::Record, id);
        self.inner.get_status(id).await
    }
}

#[async_trait]
impl ConfigLookup for RecordingLookups {
    async fn get_config(&self, id: &str) -> Result<Option<ConfigValue>> {
        self.record(Asked::Record, id);
        self.inner.get_config(id).await
    }
}

#[async_trait]
impl NameServiceLookup for RecordingLookups {
    async fn lookup(&self, id: &str) -> Result<Option<NsRecord>> {
        self.record(Asked::Record, id);
        self.inner.lookup(id).await
    }
    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        self.inner.all_records().await
    }
    async fn list_branches(&self, name: &str) -> Result<Vec<NsRecord>> {
        self.inner.list_branches(name).await
    }
    async fn heads(&self, id: &str) -> Result<Option<LedgerHeads>> {
        self.record(Asked::Record, id);
        self.inner.heads(id).await
    }
}
