//! An application that embeds KV fragments — the composition the `kv`
//! module documents but cannot demonstrate on its own.
//!
//! `kv` has no tenant concept: its unit is one `KvFragment`, and
//! `kv::apply` never sees anything else. Tenancy is entirely this
//! layer's job, and this is what the documented shape looks like when
//! it has to compile: an append-only enum key, a `BTreeMap` of
//! fragments, one app-command variant carrying `(Tenant, KvCommand)`,
//! and a policy registry in code that every replica evaluates
//! identically.

use fluree_raft_core::kv::{self, KvCommand, KvFragment, KvPolicy, KvResponse};
use fluree_raft_core::node::{ClusterNode, NodeId};
use fluree_raft_core::state_machine::{codec, AppStateMachine, SnapshotCodecError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Cursor;

/// **APPEND ONLY.** Reordering or removing a variant does not error —
/// postcard writes a bare varint discriminant, so old data silently
/// decodes as a different tenant, handing one tenant's entries to
/// another. `tenant_discriminants_are_pinned` is the guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Tenant {
    Lease,
    Config,
}

/// The per-tenant policy registry: in code, exhaustively matched.
///
/// Not configuration — a policy that differs between nodes diverges the
/// cluster, because `apply` is supposed to be a pure function of the
/// log.
pub fn policy(tenant: Tenant) -> KvPolicy {
    match tenant {
        // A lease that never expires is a permanently stuck one.
        Tenant::Lease => KvPolicy {
            max_ttl_ms: 60_000,
            allow_immortal: false,
            ..KvPolicy::default()
        },
        Tenant::Config => KvPolicy {
            allow_immortal: true,
            ..KvPolicy::default()
        },
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LeaseCommand {
    Kv(Tenant, KvCommand),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaseResponse {
    Kv(KvResponse),
    NoOp,
}

impl LeaseResponse {
    /// The KV answer, or a panic naming what came back instead — every
    /// call site in the tests wants exactly this.
    pub fn kv(self) -> KvResponse {
        match self {
            LeaseResponse::Kv(response) => response,
            other => panic!("expected a kv response, got {other:?}"),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseState {
    pub kv: BTreeMap<Tenant, KvFragment>,
}

impl LeaseState {
    pub fn fragment(&self, tenant: Tenant) -> Option<&KvFragment> {
        self.kv.get(&tenant)
    }
}

openraft::declare_raft_types!(
    pub LeaseConfig:
        D = LeaseCommand,
        R = LeaseResponse,
        NodeId = NodeId,
        Node = ClusterNode,
        Entry = openraft::Entry<LeaseConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub struct Leases;

pub const SNAPSHOT_V1: u16 = 1;

impl AppStateMachine for Leases {
    type Config = LeaseConfig;
    type Command = LeaseCommand;
    type Response = LeaseResponse;
    type State = LeaseState;

    fn initial_state() -> Self::State {
        LeaseState::default()
    }

    fn apply(state: &mut Self::State, command: &Self::Command, log_index: u64) -> Self::Response {
        match command {
            LeaseCommand::Kv(tenant, cmd) => {
                let fragment = state.kv.entry(*tenant).or_default();
                // `log_index` becomes the version of whatever this
                // writes — the fence.
                LeaseResponse::Kv(kv::apply(fragment, cmd, &policy(*tenant), log_index))
            }
        }
    }

    fn noop_response() -> Self::Response {
        LeaseResponse::NoOp
    }

    fn encode_snapshot(state: &Self::State) -> Result<Vec<u8>, SnapshotCodecError> {
        codec::encode(SNAPSHOT_V1, state)
    }

    fn decode_snapshot(bytes: &[u8]) -> Result<Self::State, SnapshotCodecError> {
        match codec::peek_version(bytes)? {
            SNAPSHOT_V1 => codec::decode(bytes, SNAPSHOT_V1),
            found => Err(SnapshotCodecError::UnsupportedVersion {
                found,
                supported: "1".to_string(),
            }),
        }
    }
}

// Convenience constructors, so the tests read as lease operations
// rather than as struct literals.

pub fn acquire(key: &str, holder: &[u8], ttl_ms: u64, now_ms: u64) -> LeaseCommand {
    LeaseCommand::Kv(
        Tenant::Lease,
        KvCommand::Put {
            key: key.into(),
            value: holder.to_vec(),
            expect: kv::Expect::Absent,
            ttl_ms: Some(ttl_ms),
            now_ms,
        },
    )
}

pub fn renew(key: &str, holder: &[u8], fence: u64, ttl_ms: u64, now_ms: u64) -> LeaseCommand {
    LeaseCommand::Kv(
        Tenant::Lease,
        KvCommand::Put {
            key: key.into(),
            value: holder.to_vec(),
            expect: kv::Expect::Version(fence),
            ttl_ms: Some(ttl_ms),
            now_ms,
        },
    )
}

pub fn evict(tenant: Tenant, cutoff_ms: u64, limit: u32) -> LeaseCommand {
    LeaseCommand::Kv(tenant, KvCommand::Evict { cutoff_ms, limit })
}
