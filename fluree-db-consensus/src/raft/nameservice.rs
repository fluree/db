//! Read-side `NameService` adapter over the replicated state machine.
//!
//! Followers (and the leader, before its local cache catches up) need
//! a way to resolve current ledger heads without going through
//! openraft's RPC surface. [`RaftNameService`] holds the same shared
//! [`SharedState`](super::state_machine_adapter::SharedState) the
//! state-machine adapter writes to under apply, so reads always
//! observe committed state.
//!
//! # Scope
//!
//! Reads only. The trait surface is wide because
//! [`NameService`](fluree_db_nameservice::NameService) bundles several
//! supertraits, but the write-shaped methods
//! ([`create_branch`](fluree_db_nameservice::NameService::create_branch),
//! [`drop_branch`](fluree_db_nameservice::NameService::drop_branch),
//! [`reset_head`](fluree_db_nameservice::NameService::reset_head))
//! return [`NameServiceError::Storage`] pointing the caller at the
//! openraft proposal path. The state machine is the source of truth
//! for refs and ledger metadata; mutations must flow through Raft.
//!
//! # What's NOT tracked here
//!
//! The state machine only carries ledger lifecycle and branch heads —
//! it does not replicate index roots, default contexts, ledger
//! configuration, or graph-source records. The lookup methods report
//! those as absent (None / 0 / empty), which is what
//! [`LedgerState::load`] needs for a follower reload: it falls back
//! to genesis-snapshot replay from the content store using the
//! branch head walked from `commit_head_id`.
//!
//! [`LedgerState::load`]: fluree_db_ledger::LedgerState::load

use crate::raft::state_machine::{NameServiceState, RefKey};
use crate::raft::state_machine_adapter::SharedState;
use async_trait::async_trait;
use fluree_db_core::ledger_id::split_ledger_id;
use fluree_db_nameservice::{
    ConfigLookup, ConfigValue, GraphSourceLookup, GraphSourceRecord, NameServiceLookup,
    NsLookupResult, NsRecord, RefKind, RefLookup, RefValue, Result, StatusLookup, StatusValue,
};
use std::fmt;

/// Read-side `NameService` adapter over the replicated state machine.
///
/// Construct with the same [`SharedState`] handle the state machine
/// adapter was constructed with — both then observe the same
/// committed log.
pub struct RaftNameService {
    state: SharedState,
}

impl RaftNameService {
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }
}

impl fmt::Debug for RaftNameService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftNameService").finish()
    }
}

/// Construct an [`NsRecord`] from the state machine's view of a
/// single branch. Returns `None` if the base ledger has no
/// [`LedgerRecord`](crate::raft::state_machine::LedgerRecord).
///
/// Fields the state machine doesn't track
/// (`index_head_id`/`index_t`/`default_context`/`config_id`/`source_branch`/`branches`)
/// fall back to their `NsRecord::new` defaults — see the module docs
/// for why that's enough for follower reload.
fn record_from_state(state: &NameServiceState, ledger_name: &str, branch: &str) -> Option<NsRecord> {
    if !state.ledgers.contains_key(ledger_name) {
        return None;
    }
    let mut record = NsRecord::new(ledger_name, branch);
    let ref_key = RefKey::new(ledger_name, branch);
    if let Some(entry) = state.refs.get(&ref_key) {
        record.commit_head_id = Some(entry.head.clone());
        record.commit_t = entry.t;
    }
    Some(record)
}

#[async_trait]
impl NameServiceLookup for RaftNameService {
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        let state = self.state.read().await;
        Ok(record_from_state(&state, &name, &branch))
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        let state = self.state.read().await;
        let mut records = Vec::new();
        for (ledger_name, ledger) in &state.ledgers {
            for branch in &ledger.branches {
                if let Some(record) = record_from_state(&state, ledger_name, branch) {
                    records.push(record);
                }
            }
        }
        Ok(records)
    }
}

#[async_trait]
impl RefLookup for RaftNameService {
    async fn get_ref(&self, ledger_id: &str, kind: RefKind) -> Result<Option<RefValue>> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        let state = self.state.read().await;
        if !state.ledgers.contains_key(&name) {
            return Ok(None);
        }
        match kind {
            RefKind::CommitHead => {
                let entry = state.refs.get(&RefKey::new(&name, &branch));
                Ok(Some(RefValue {
                    id: entry.map(|e| e.head.clone()),
                    t: entry.map(|e| e.t).unwrap_or(0),
                }))
            }
            RefKind::IndexHead => Ok(Some(RefValue { id: None, t: 0 })),
        }
    }
}

#[async_trait]
impl GraphSourceLookup for RaftNameService {
    async fn lookup_graph_source(
        &self,
        _graph_source_id: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        Ok(None)
    }

    async fn lookup_any(&self, resource_id: &str) -> Result<NsLookupResult> {
        match self.lookup(resource_id).await? {
            Some(record) => Ok(NsLookupResult::Ledger(record)),
            None => Ok(NsLookupResult::NotFound),
        }
    }

    async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl StatusLookup for RaftNameService {
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>> {
        let (name, _branch) = split_ledger_id(ledger_id)?;
        let state = self.state.read().await;
        if state.ledgers.contains_key(&name) {
            Ok(Some(StatusValue::initial()))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl ConfigLookup for RaftNameService {
    async fn get_config(&self, _ledger_id: &str) -> Result<Option<ConfigValue>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::{
        AdvanceRefArgs, Command, CreateLedgerArgs, NameServiceState, Response,
    };
    use fluree_db_api::{ContentId, ContentKind};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    fn fresh_state() -> SharedState {
        Arc::new(RwLock::new(NameServiceState::default()))
    }

    async fn apply_cmd(state: &SharedState, cmd: Command, index: u64) -> Response {
        let mut guard = state.write().await;
        crate::raft::state_machine::apply(&mut guard, cmd, index)
    }

    #[tokio::test]
    async fn lookup_returns_none_when_ledger_missing() {
        let ns = RaftNameService::new(fresh_state());
        assert!(ns.lookup("test/db:main").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn lookup_returns_record_with_head_after_advance_ref() {
        let state = fresh_state();
        let _ = apply_cmd(
            &state,
            Command::CreateLedger(CreateLedgerArgs {
                ledger_id: "test/db".into(),
                initial_branch: "main".into(),
                initial_head: cid(0),
                initial_t: 0,
                governance: cid(0xAA),
                created_at_millis: 1_000,
            }),
            1,
        )
        .await;
        let _ = apply_cmd(
            &state,
            Command::AdvanceRef(AdvanceRefArgs {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                expected_prev: Some(cid(0)),
                new_head: cid(5),
                t: 7,
                applied_at_millis: 2_000,
                idempotency: None,
                release: Vec::new(),
                tally: None,
            }),
            2,
        )
        .await;

        let ns = RaftNameService::new(state);
        let record = ns.lookup("test/db:main").await.unwrap().expect("record");
        assert_eq!(record.ledger_id, "test/db:main");
        assert_eq!(record.commit_head_id, Some(cid(5)));
        assert_eq!(record.commit_t, 7);
        assert_eq!(record.index_head_id, None);
    }

    #[tokio::test]
    async fn get_ref_returns_head_for_commit_kind() {
        let state = fresh_state();
        apply_cmd(
            &state,
            Command::CreateLedger(CreateLedgerArgs {
                ledger_id: "test/db".into(),
                initial_branch: "main".into(),
                initial_head: cid(0),
                initial_t: 0,
                governance: cid(0xAA),
                created_at_millis: 1_000,
            }),
            1,
        )
        .await;
        apply_cmd(
            &state,
            Command::AdvanceRef(AdvanceRefArgs {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                expected_prev: Some(cid(0)),
                new_head: cid(9),
                t: 3,
                applied_at_millis: 2_000,
                idempotency: None,
                release: Vec::new(),
                tally: None,
            }),
            2,
        )
        .await;

        let ns = RaftNameService::new(state);
        let ref_value = ns
            .get_ref("test/db:main", RefKind::CommitHead)
            .await
            .unwrap()
            .expect("ref value");
        assert_eq!(ref_value.id, Some(cid(9)));
        assert_eq!(ref_value.t, 3);

        let index_ref = ns
            .get_ref("test/db:main", RefKind::IndexHead)
            .await
            .unwrap()
            .expect("index ref");
        assert!(index_ref.id.is_none());
        assert_eq!(index_ref.t, 0);
    }

    #[tokio::test]
    async fn all_records_enumerates_every_branch() {
        let state = fresh_state();
        apply_cmd(
            &state,
            Command::CreateLedger(CreateLedgerArgs {
                ledger_id: "a/db".into(),
                initial_branch: "main".into(),
                initial_head: cid(0),
                initial_t: 0,
                governance: cid(0xAA),
                created_at_millis: 0,
            }),
            1,
        )
        .await;
        apply_cmd(
            &state,
            Command::AdvanceRef(AdvanceRefArgs {
                ledger_id: "a/db".into(),
                branch: "feat".into(),
                expected_prev: None,
                new_head: cid(1),
                t: 1,
                applied_at_millis: 0,
                idempotency: None,
                release: Vec::new(),
                tally: None,
            }),
            2,
        )
        .await;

        let ns = RaftNameService::new(state);
        let mut ids: Vec<_> = ns
            .all_records()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.ledger_id)
            .collect();
        ids.sort();
        assert_eq!(
            ids,
            vec!["a/db:feat".to_string(), "a/db:main".to_string()]
        );
    }
}
