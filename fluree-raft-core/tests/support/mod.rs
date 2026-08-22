//! The toy application both integration tests drive.
//!
//! A counter is enough to exercise every part of the seam — state
//! reduction, a membership-derived field, a snapshot format with a
//! version to migrate from — without any of it being about counting.

use fluree_raft_core::node::{ClusterNode, NodeId};
use fluree_raft_core::state_machine::{codec, AppStateMachine, MembershipView, SnapshotCodecError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::Cursor;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CounterCommand {
    Add(i64),
    Reset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CounterResponse {
    /// Value after the command, and the log index that produced it.
    Value { value: i64, at_index: u64 },
    /// Blank / membership entry.
    NoOp,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterState {
    pub value: i64,
    pub applies: u64,
    /// Mirrored from membership entries — the case that cannot be
    /// maintained through `Command`.
    pub voters: BTreeSet<NodeId>,
}

openraft::declare_raft_types!(
    pub CounterConfig:
        D = CounterCommand,
        R = CounterResponse,
        NodeId = NodeId,
        Node = ClusterNode,
        Entry = openraft::Entry<CounterConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub struct Counter;

/// Current snapshot format version.
pub const SNAPSHOT_V2: u16 = 2;

/// The shape this state had in a hypothetical earlier release, kept to
/// prove the versioned codec gives a real migration path rather than a
/// version number nobody can act on.
#[derive(Serialize, Deserialize)]
pub struct CounterStateV1 {
    pub value: i64,
}

impl AppStateMachine for Counter {
    type Config = CounterConfig;
    type Command = CounterCommand;
    type Response = CounterResponse;
    type State = CounterState;

    fn initial_state() -> Self::State {
        CounterState::default()
    }

    fn apply(state: &mut Self::State, command: &Self::Command, log_index: u64) -> Self::Response {
        match command {
            CounterCommand::Add(n) => state.value += n,
            CounterCommand::Reset => state.value = 0,
        }
        state.applies += 1;
        CounterResponse::Value {
            value: state.value,
            at_index: log_index,
        }
    }

    fn noop_response() -> Self::Response {
        CounterResponse::NoOp
    }

    fn apply_membership(state: &mut Self::State, membership: &MembershipView, _log_index: u64) {
        state.voters = membership.voters.clone();
    }

    fn encode_snapshot(state: &Self::State) -> Result<Vec<u8>, SnapshotCodecError> {
        codec::encode(SNAPSHOT_V2, state)
    }

    fn decode_snapshot(bytes: &[u8]) -> Result<Self::State, SnapshotCodecError> {
        match codec::peek_version(bytes)? {
            1 => {
                let old: CounterStateV1 = codec::decode(bytes, 1)?;
                Ok(CounterState {
                    value: old.value,
                    applies: 0,
                    voters: BTreeSet::new(),
                })
            }
            SNAPSHOT_V2 => codec::decode(bytes, SNAPSHOT_V2),
            found => Err(SnapshotCodecError::UnsupportedVersion {
                found,
                supported: "1, 2".to_string(),
            }),
        }
    }
}
