//! Experimental local journal foundation. No transaction path uses this module.
//!
//! One owner appends exact, opaque transitions and obtains a receipt only after
//! `sync_all` succeeds. This receipt proves journal persistence under the I/O
//! contract, NOT readable database state or authorization to acknowledge a txn.
//! The integration must reserve heads before append, validate dependency closure,
//! replay before access, materialize objects/head, and install state before ACK.
//!
//! Recovery accepts only a complete, checksummed, ordered stream. It never silently
//! drops a malformed suffix: without an independently durable acceptance boundary
//! that suffix could contain an acknowledged record. A complete record whose sync
//! failed may recover, so an error means an unknown outcome. All I/O errors poison
//! the writer. Reopen/reconcile is required before another append.
//!
//! Assumptions: successful sync preserves prior bytes; later appends do not destroy
//! already durable bytes; the directory entry was durably created. Checksums detect
//! accidental damage, not malicious modification or loss of a whole valid suffix.
//! Shared-block damage is detected when it changes a retained frame, but cannot be
//! repaired here. Power-loss qualification, sealing, root ownership, checkpoints,
//! and production replay/materialization remain integration work.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io;

mod file;
pub use file::FileIo;
#[cfg(test)]
mod tests;

const MAGIC: &[u8; 8] = b"FLWAL001";
const HEADER: usize = 56; // magic/version, identity, SHA-256
const FRAME_HEADER: usize = 48; // magic/version, payload length, sequence, prev hash
const HASH: usize = 32;
const FRAME_MAGIC: &[u8; 4] = b"FR01";
pub const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_JOURNAL_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("journal I/O: {0}")]
    Io(#[from] io::Error),
    #[error("invalid journal: {0}")]
    Invalid(&'static str),
    #[error("journal capacity exceeded; checkpoint required")]
    Capacity,
    #[error("journal writer poisoned; reopen and reconcile the unknown outcome")]
    Poisoned,
}

pub type Result<T> = std::result::Result<T, Error>;

/// Positioned I/O seam shared by the file backend and deterministic fault tests.
/// A successful sync must persist ALL earlier writes and length changes in order.
/// Short reads/writes and Interrupted are permitted. Exclusive ownership and
/// durable file/directory creation are the backend/caller's responsibility.
pub trait JournalIo {
    fn len(&mut self) -> io::Result<u64>;
    fn is_empty(&mut self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
    fn read_at(&mut self, offset: u64, out: &mut [u8]) -> io::Result<usize>;
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<usize>;
    fn sync_all(&mut self) -> io::Result<()>;
}

/// Exact immutable bytes. Keys are relative, portable storage keys, not addresses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub key: String,
    pub bytes: Vec<u8>,
}

/// An opaque accepted transition; this layer deliberately has no nameservice
/// dependency. The owner must verify generation, CAS, content identities and the
/// complete dependency closure BEFORE calling append. No encryption is provided.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transition {
    pub ledger: String,
    pub generation: String,
    pub head_key: String,
    pub expected_head: Option<Vec<u8>>,
    pub resulting_head: Vec<u8>,
    pub objects: Vec<Object>,
}

impl Transition {
    fn validate(&self) -> Result<()> {
        if self.ledger.is_empty() || self.generation.is_empty() || self.resulting_head.is_empty() {
            return Err(Error::Invalid("missing transition identity/head"));
        }
        let valid_key = |key: &str| {
            !key.is_empty()
                && !key.contains(['\\', '\0', ':'])
                && key.split('/').all(|part| !matches!(part, "" | "." | ".."))
        };
        let mut keys = BTreeSet::new();
        keys.insert(self.head_key.as_str());
        if !valid_key(&self.head_key)
            || self
                .objects
                .iter()
                .any(|o| !valid_key(&o.key) || !keys.insert(&o.key))
        {
            return Err(Error::Invalid("unsafe or duplicate storage key"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub sequence: u64,
    pub end: u64,
    pub digest: [u8; HASH],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub receipt: Receipt,
    pub transition: Transition,
}

/// Materialization seam for a single ledger during exclusive startup recovery.
/// Implementations must reject differing existing immutable bytes, publish the
/// head atomically with the supplied expected bytes, and make newly written
/// objects readable before publication. This does not authorize journal retirement
/// or skip object/directory syncs needed by a later durable checkpoint.
pub trait ReplayTarget {
    fn read_head(&mut self, key: &str) -> Result<Option<Vec<u8>>>;
    fn put_immutable(&mut self, object: &Object) -> Result<()>;
    fn publish_head(&mut self, key: &str, expected: Option<&[u8]>, bytes: &[u8]) -> Result<()>;
}

/// Replay one contiguous ledger generation from fully validated `Journal::open`
/// records. Supports restart at any head in this chain, including its final head.
/// Reinstall/check every object even if the head is already current: an interrupted
/// materialization must not hide missing content. No effects occur for a wrong
/// generation, a broken head chain, or an unrelated existing head.
///
/// The owner supplies the independently established ledger generation; this helper
/// cannot decide whether a dropped/recreated ledger or a stale CAS was authorized.
pub fn replay_chain(
    records: &[Record],
    ledger: &str,
    generation: &str,
    target: &mut impl ReplayTarget,
) -> Result<()> {
    let Some(first) = records.first() else {
        return Ok(());
    };
    let head_key = &first.transition.head_key;
    let mut expected = first.transition.expected_head.as_deref();
    for record in records {
        let t = &record.transition;
        t.validate()?;
        if t.ledger != ledger
            || t.generation != generation
            || &t.head_key != head_key
            || t.expected_head.as_deref() != expected
        {
            return Err(Error::Invalid("replay ledger/generation/head chain"));
        }
        expected = Some(&t.resulting_head);
    }
    let current = target.read_head(head_key)?;
    if current != first.transition.expected_head
        && !records
            .iter()
            .any(|r| current.as_deref() == Some(&r.transition.resulting_head))
    {
        return Err(Error::Invalid("replay head outside accepted chain"));
    }
    for record in records {
        for object in &record.transition.objects {
            target.put_immutable(object)?;
        }
    }
    let final_head = &records.last().unwrap().transition.resulting_head;
    if current.as_deref() != Some(final_head) {
        target.publish_head(head_key, current.as_deref(), final_head)?;
    }
    Ok(())
}

/// Bounded single-writer stream. No truncate, rotation, batching or public ACK API.
pub struct Journal<I> {
    io: I,
    end: u64,
    sequence: u64,
    digest: [u8; HASH],
    poisoned: bool,
}

fn digest(bytes: &[u8]) -> [u8; HASH] {
    Sha256::digest(bytes).into()
}

fn read_exact(io: &mut impl JournalIo, mut offset: u64, mut out: &mut [u8]) -> io::Result<()> {
    while !out.is_empty() {
        match io.read_at(offset, out) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) if n <= out.len() => {
                offset += n as u64;
                out = &mut out[n..];
            }
            Ok(_) => return Err(io::ErrorKind::InvalidData.into()),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn write_all(io: &mut impl JournalIo, mut offset: u64, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        match io.write_at(offset, bytes) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) if n <= bytes.len() => {
                offset += n as u64;
                bytes = &bytes[n..];
            }
            Ok(_) => return Err(io::ErrorKind::InvalidData.into()),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

impl<I: JournalIo> Journal<I> {
    /// Initialize an empty, exclusively owned file in an already durable directory.
    /// Identity must be fresh for every journal generation; never reuse a journal.
    pub fn create(mut io: I, identity: [u8; 16]) -> Result<Self> {
        if !io.is_empty()? {
            return Err(Error::Invalid("create requires empty journal"));
        }
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&identity);
        let hash = digest(&bytes);
        bytes.extend_from_slice(&hash);
        write_all(&mut io, 0, &bytes)?;
        io.sync_all()?;
        Ok(Self {
            io,
            end: HEADER as u64,
            sequence: 0,
            digest: hash,
            poisoned: false,
        })
    }

    /// Validate the WHOLE stream before returning any replay records. The caller
    /// must reconcile these records before using the returned writer. Complete
    /// unacknowledged records are included. No automatic suffix truncation occurs.
    pub fn open(mut io: I) -> Result<(Self, Vec<Record>)> {
        let len = io.len()?;
        if len > MAX_JOURNAL_BYTES {
            return Err(Error::Capacity);
        }
        if len < HEADER as u64 {
            return Err(Error::Invalid("incomplete journal header"));
        }
        let mut header = [0; HEADER];
        read_exact(&mut io, 0, &mut header)?;
        if &header[..8] != MAGIC || digest(&header[..24]) != header[24..] {
            return Err(Error::Invalid("header version/checksum"));
        }
        let mut journal = Self {
            io,
            end: HEADER as u64,
            sequence: 0,
            digest: digest(&header[..24]),
            poisoned: false,
        };
        let mut records = Vec::new();
        while journal.end < len {
            if len - journal.end < (FRAME_HEADER + HASH) as u64 {
                return Err(Error::Invalid("incomplete frame; outcome unknown"));
            }
            let mut frame = vec![0; FRAME_HEADER];
            read_exact(&mut journal.io, journal.end, &mut frame)?;
            let payload_len = u32::from_le_bytes(frame[4..8].try_into().unwrap()) as usize;
            let sequence = u64::from_le_bytes(frame[8..16].try_into().unwrap());
            if &frame[..4] != FRAME_MAGIC
                || sequence != journal.sequence + 1
                || frame[16..48] != journal.digest
                || payload_len > MAX_PAYLOAD_BYTES
            {
                return Err(Error::Invalid("frame version/length/order/chain"));
            }
            let frame_len = FRAME_HEADER + payload_len + HASH;
            if frame_len as u64 > len - journal.end {
                return Err(Error::Invalid("incomplete payload; outcome unknown"));
            }
            frame.resize(frame_len, 0);
            read_exact(
                &mut journal.io,
                journal.end + FRAME_HEADER as u64,
                &mut frame[FRAME_HEADER..],
            )?;
            let hash = digest(&frame[..frame_len - HASH]);
            if hash != frame[frame_len - HASH..] {
                return Err(Error::Invalid("frame checksum; outcome unknown"));
            }
            let transition: Transition =
                serde_json::from_slice(&frame[FRAME_HEADER..frame_len - HASH])
                    .map_err(|_| Error::Invalid("transition encoding"))?;
            transition.validate()?;
            journal.end += frame_len as u64;
            journal.sequence = sequence;
            journal.digest = hash;
            records.push(Record {
                receipt: journal.receipt(),
                transition,
            });
        }
        // Recovered complete-but-unsynced records must themselves be durable before
        // any later receipt can imply durable coverage of their prefix.
        journal.io.sync_all()?;
        Ok((journal, records))
    }

    fn receipt(&self) -> Receipt {
        Receipt {
            sequence: self.sequence,
            end: self.end,
            digest: self.digest,
        }
    }

    pub fn append_and_sync(&mut self, transition: &Transition) -> Result<Receipt> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        transition.validate()?;
        // A capped serializer avoids allocating an unbounded encoded payload.
        let mut payload = CappedPayload(Vec::new());
        serde_json::to_writer(&mut payload, transition).map_err(|_| Error::Capacity)?;
        let payload = payload.0;
        let frame_len = FRAME_HEADER + payload.len() + HASH;
        if self.end + frame_len as u64 > MAX_JOURNAL_BYTES {
            return Err(Error::Capacity);
        }
        let sequence = self.sequence.checked_add(1).ok_or(Error::Capacity)?;
        let mut frame = Vec::with_capacity(frame_len);
        frame.extend_from_slice(FRAME_MAGIC);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&sequence.to_le_bytes());
        frame.extend_from_slice(&self.digest);
        frame.extend_from_slice(&payload);
        let hash = digest(&frame);
        frame.extend_from_slice(&hash);
        self.poisoned = true;
        write_all(&mut self.io, self.end, &frame)?;
        self.io.sync_all()?;
        self.end += frame_len as u64;
        self.sequence = sequence;
        self.digest = hash;
        self.poisoned = false;
        Ok(self.receipt())
    }
}

struct CappedPayload(Vec<u8>);
impl io::Write for CappedPayload {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_PAYLOAD_BYTES - self.0.len() {
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
