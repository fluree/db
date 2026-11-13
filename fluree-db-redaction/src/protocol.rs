//! What Fluree and the prover exchange for one redaction step: a request on
//! the prover's stdin, a response on its stdout. The prover is a separate
//! binary, so these frames are its whole interface.
//!
//! ```text
//! request:  "FPQ1" | input (u32 len, bytes) | slot count u32 | slots u32...
//!           | ledger id (u16 len, UTF-8) | signatures u8 (0 verify, 1 expose)
//! response: "FPS1" | replacement (u32 len, bytes) | receipt (u32 len, bincode)
//!           | user cycles u64 | segments u32
//! ```

use crate::{Error, Result, Signatures};

const REQUEST_MAGIC: &[u8; 4] = b"FPQ1";
const RESPONSE_MAGIC: &[u8; 4] = b"FPS1";

/// One redaction step to prove.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProveRequest {
    /// The original commit, or the replacement a previous step produced.
    pub input: Vec<u8>,
    /// Slots to remove, ascending.
    pub removed_slots: Vec<u32>,
    /// The ledger the original's signatures were made for.
    pub ledger_id: String,
    pub signatures: Signatures,
}

/// A proved step: the replacement and the receipt proving it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProveResponse {
    pub replacement: Vec<u8>,
    /// The succinct receipt, bincode-encoded.
    pub receipt: Vec<u8>,
    pub user_cycles: u64,
    pub segments: u32,
}

fn take<'a>(data: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    let end = pos
        .checked_add(n)
        .filter(|end| *end <= data.len())
        .ok_or(Error::Malformed("frame cut short"))?;
    let out = &data[*pos..end];
    *pos = end;
    Ok(out)
}

fn u32_at(data: &[u8], pos: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(take(data, pos, 4)?.try_into().unwrap()))
}

fn bytes_u32<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    let len = u32_at(data, pos)? as usize;
    take(data, pos, len)
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn finish<T>(data: &[u8], pos: usize, value: T) -> Result<T> {
    if pos == data.len() {
        Ok(value)
    } else {
        Err(Error::Malformed("frame trailing bytes"))
    }
}

impl ProveRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.input.len() + 64);
        out.extend_from_slice(REQUEST_MAGIC);
        put_bytes(&mut out, &self.input);
        out.extend_from_slice(&(self.removed_slots.len() as u32).to_le_bytes());
        for slot in &self.removed_slots {
            out.extend_from_slice(&slot.to_le_bytes());
        }
        out.extend_from_slice(&(self.ledger_id.len() as u16).to_le_bytes());
        out.extend_from_slice(self.ledger_id.as_bytes());
        out.push(match self.signatures {
            Signatures::Verify => 0,
            Signatures::Expose => 1,
        });
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut pos = 0;
        if take(data, &mut pos, 4)? != REQUEST_MAGIC {
            return Err(Error::Malformed("not a prove request"));
        }
        let input = bytes_u32(data, &mut pos)?.to_vec();
        let count = u32_at(data, &mut pos)? as usize;
        let mut removed_slots = Vec::with_capacity(count.min(data.len() / 4));
        for _ in 0..count {
            removed_slots.push(u32_at(data, &mut pos)?);
        }
        let ledger_len = u16::from_le_bytes(take(data, &mut pos, 2)?.try_into().unwrap());
        let ledger_id = core::str::from_utf8(take(data, &mut pos, ledger_len as usize)?)
            .map_err(|_| Error::Malformed("ledger id is not UTF-8"))?
            .into();
        let signatures = match take(data, &mut pos, 1)?[0] {
            0 => Signatures::Verify,
            1 => Signatures::Expose,
            _ => return Err(Error::Malformed("signature mode")),
        };
        finish(
            data,
            pos,
            ProveRequest {
                input,
                removed_slots,
                ledger_id,
                signatures,
            },
        )
    }
}

impl ProveResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.replacement.len() + self.receipt.len() + 32);
        out.extend_from_slice(RESPONSE_MAGIC);
        put_bytes(&mut out, &self.replacement);
        put_bytes(&mut out, &self.receipt);
        out.extend_from_slice(&self.user_cycles.to_le_bytes());
        out.extend_from_slice(&self.segments.to_le_bytes());
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut pos = 0;
        if take(data, &mut pos, 4)? != RESPONSE_MAGIC {
            return Err(Error::Malformed("not a prove response"));
        }
        let replacement = bytes_u32(data, &mut pos)?.to_vec();
        let receipt = bytes_u32(data, &mut pos)?.to_vec();
        let user_cycles = u64::from_le_bytes(take(data, &mut pos, 8)?.try_into().unwrap());
        let segments = u32_at(data, &mut pos)?;
        finish(
            data,
            pos,
            ProveResponse {
                replacement,
                receipt,
                user_cycles,
                segments,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_exactly() {
        let request = ProveRequest {
            input: vec![1, 2, 3],
            removed_slots: vec![0, 7],
            ledger_id: "acme/people:main".into(),
            signatures: Signatures::Expose,
        };
        assert_eq!(ProveRequest::decode(&request.encode()).unwrap(), request);
        let response = ProveResponse {
            replacement: vec![9; 40],
            receipt: vec![5; 12],
            user_cycles: 1 << 33,
            segments: 3,
        };
        assert_eq!(ProveResponse::decode(&response.encode()).unwrap(), response);
        let mut long = request.encode();
        long.push(0);
        assert!(ProveRequest::decode(&long).is_err());
        let encoded = response.encode();
        assert!(ProveResponse::decode(&encoded[..encoded.len() - 1]).is_err());
    }
}
