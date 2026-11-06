//! Delta runs of the pooled tree's secondary orders.
//!
//! The pooled tree keeps GSPO current every build. Its graph-last orders
//! (SpotG, PsotG, PostG, OpstG) instead keep a base branch plus the events
//! written since, as immutable sorted runs: an incremental build appends one
//! run rather than rewriting the shared leaves its rows scatter across, and a
//! compaction folds the runs into the base. Readers merge base, runs and
//! novelty (see [`crate::BinaryCursor`]).
//!
//! A run holds every event of the window that wrote it — assert or retract,
//! with its `t` and graph — sorted by the order's key.

use std::io;

#[cfg(test)]
use crate::format::run_record::LIST_INDEX_NONE;
use crate::format::run_record::{widen_list_index, RunSortOrder};
use crate::read::types::OverlayOp;

/// Delta runs read at once, across every order a reader loads or a build
/// folds: a store near the run cap otherwise waits on each read in turn.
pub const RUN_FETCH_WIDTH: usize = 32;

const MAGIC: &[u8; 4] = b"FPD1";
const VERSION: u8 = 2;
/// An earlier v5 run, whose events store a 32-bit `o_i`. Read only.
const VERSION_NARROW: u8 = 1;
const HEADER_LEN: usize = 4 + 1 + 1 + 4;

/// Encode `events` (sorted by `order`'s key) as a delta run.
pub fn encode_delta_run(order: RunSortOrder, events: &[OverlayOp]) -> io::Result<Vec<u8>> {
    let mut body = Vec::with_capacity(events.len() * EVENT_LEN);
    for e in events {
        body.extend_from_slice(&e.s_id.to_le_bytes());
        body.extend_from_slice(&e.p_id.to_le_bytes());
        body.extend_from_slice(&e.o_type.to_le_bytes());
        body.extend_from_slice(&e.o_key.to_le_bytes());
        body.extend_from_slice(&e.o_i.to_le_bytes());
        let t = u32::try_from(e.t)
            .map_err(|_| io::Error::other(format!("delta run: t {} exceeds u32", e.t)))?;
        body.extend_from_slice(&t.to_le_bytes());
        body.push(u8::from(e.op));
        body.extend_from_slice(&e.g_id.to_le_bytes());
    }
    let compressed = zstd::bulk::compress(&body, 3)?;
    let mut out = Vec::with_capacity(HEADER_LEN + compressed.len());
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.push(order.to_wire_id());
    out.extend_from_slice(&(events.len() as u32).to_le_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

const EVENT_LEN: usize = 8 + 4 + 2 + 8 + 8 + 4 + 1 + 4;
const EVENT_LEN_NARROW: usize = EVENT_LEN - 4;

/// Decode a delta run: its order and events.
pub fn decode_delta_run(bytes: &[u8]) -> io::Result<(RunSortOrder, Vec<OverlayOp>)> {
    let bad = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, format!("delta run: {msg}"));
    if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC {
        return Err(bad("bad magic"));
    }
    let narrow = match bytes[4] {
        VERSION => false,
        VERSION_NARROW => true,
        v => return Err(bad(&format!("unsupported version {v}"))),
    };
    let event_len = if narrow { EVENT_LEN_NARROW } else { EVENT_LEN };
    let order = RunSortOrder::from_wire_id(bytes[5])
        .filter(|o| o.is_pooled() && *o != RunSortOrder::Gspo)
        .ok_or_else(|| bad(&format!("bad order id {}", bytes[5])))?;
    let count = u32::from_le_bytes(bytes[6..10].try_into().expect("4 bytes")) as usize;
    let body = zstd::bulk::decompress(&bytes[HEADER_LEN..], count * event_len)?;
    if body.len() != count * event_len {
        return Err(bad("length mismatch"));
    }
    let mut events = Vec::with_capacity(count);
    for e in body.chunks_exact(event_len) {
        let u64_at = |at: usize| u64::from_le_bytes(e[at..at + 8].try_into().expect("8 bytes"));
        let u32_at = |at: usize| u32::from_le_bytes(e[at..at + 4].try_into().expect("4 bytes"));
        // The fields after `o_i` sit 4 bytes earlier in a narrow event.
        let (o_i, after) = if narrow {
            (widen_list_index(u32_at(22)), 26)
        } else {
            (u64_at(22), 30)
        };
        events.push(OverlayOp {
            s_id: u64_at(0),
            p_id: u32_at(8),
            o_type: u16::from_le_bytes(e[12..14].try_into().expect("2 bytes")),
            o_key: u64_at(14),
            o_i,
            t: i64::from(u32_at(after)),
            op: e[after + 4] != 0,
            g_id: u32_at(after + 5),
        });
    }
    Ok((order, events))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_runs_round_trip() {
        let events = vec![
            OverlayOp {
                s_id: 7,
                p_id: 3,
                o_type: 5,
                o_key: 42,
                o_i: LIST_INDEX_NONE,
                t: 9,
                op: true,
                g_id: 70_000,
            },
            OverlayOp {
                s_id: 8,
                p_id: 3,
                o_type: 5,
                o_key: 43,
                o_i: 0,
                t: 10,
                op: false,
                g_id: 3,
            },
        ];
        let bytes = encode_delta_run(RunSortOrder::PostG, &events).unwrap();
        let (order, decoded) = decode_delta_run(&bytes).unwrap();
        assert_eq!(order, RunSortOrder::PostG);
        assert_eq!(decoded.len(), 2);
        for (a, b) in events.iter().zip(&decoded) {
            assert_eq!(
                (a.s_id, a.p_id, a.o_type, a.o_key, a.o_i, a.t, a.op, a.g_id),
                (b.s_id, b.p_id, b.o_type, b.o_key, b.o_i, b.t, b.op, b.g_id)
            );
        }
        assert!(encode_delta_run(RunSortOrder::Gspo, &events)
            .and_then(|b| decode_delta_run(&b))
            .is_err());
    }
}
