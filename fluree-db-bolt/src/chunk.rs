//! Chunked message transport.
//!
//! After the handshake, every Bolt message travels as a sequence of chunks:
//! a `u16` big-endian size header followed by that many payload bytes, with
//! the message terminated by a zero-size chunk (`0x00 0x00`). A single
//! message may span many chunks; a chunk never spans messages.

use crate::DecodeError;

/// Hard ceiling on a single assembled message. Statements and parameter
/// maps are small; anything near this size is hostile or misdirected
/// traffic (e.g. an HTTP client talking to the Bolt port).
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Largest chunk payload the writer emits (the u16 header maximum).
const MAX_CHUNK: usize = 0xFFFF;

/// Incremental chunk decoder: push raw socket bytes in, take complete
/// message payloads out. Handles partial headers, chunks split across
/// reads, and multiple messages per read.
#[derive(Debug, Default)]
pub struct ChunkAssembler {
    /// Unconsumed inbound bytes (partial header or partial chunk payload).
    pending: Vec<u8>,
    /// Payload of the message currently being assembled.
    message: Vec<u8>,
    /// Complete messages ready for the caller.
    complete: std::collections::VecDeque<Vec<u8>>,
}

impl ChunkAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed raw bytes from the transport. Complete messages become
    /// available via [`Self::next_message`].
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), DecodeError> {
        self.pending.extend_from_slice(bytes);
        let mut pos = 0;
        loop {
            let avail = &self.pending[pos..];
            if avail.len() < 2 {
                break;
            }
            let size = u16::from_be_bytes([avail[0], avail[1]]) as usize;
            if size == 0 {
                // End-of-message marker.
                pos += 2;
                let payload = std::mem::take(&mut self.message);
                // A bare 0x0000 with no preceding chunks is a NOOP keep-alive
                // (drivers send them on idle connections); swallow it.
                if !payload.is_empty() {
                    self.complete.push_back(payload);
                }
                continue;
            }
            if avail.len() < 2 + size {
                break;
            }
            if self.message.len() + size > MAX_MESSAGE_BYTES {
                return Err(DecodeError::new("bolt message exceeds size limit"));
            }
            self.message.extend_from_slice(&avail[2..2 + size]);
            pos += 2 + size;
        }
        self.pending.drain(..pos);
        Ok(())
    }

    /// Take the next complete message payload, if any.
    pub fn next_message(&mut self) -> Option<Vec<u8>> {
        self.complete.pop_front()
    }

    /// Discard any partially assembled state (used on RESET/error paths).
    pub fn clear(&mut self) {
        self.pending.clear();
        self.message.clear();
        self.complete.clear();
    }
}

/// Append `payload` to `out` as a chunked message (chunks + terminator).
pub fn write_message(payload: &[u8], out: &mut Vec<u8>) {
    for chunk in payload.chunks(MAX_CHUNK) {
        out.extend_from_slice(&(chunk.len() as u16).to_be_bytes());
        out.extend_from_slice(chunk);
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assemble_all(assembler: &mut ChunkAssembler) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(m) = assembler.next_message() {
            out.push(m);
        }
        out
    }

    #[test]
    fn single_message_roundtrip() {
        let payload = b"\xb1\x01\xa0".to_vec(); // HELLO {}
        let mut wire = Vec::new();
        write_message(&payload, &mut wire);
        assert_eq!(
            wire,
            [&[0x00, 0x03][..], &payload[..], &[0x00, 0x00][..]].concat()
        );

        let mut asm = ChunkAssembler::new();
        asm.push(&wire).unwrap();
        assert_eq!(assemble_all(&mut asm), vec![payload]);
    }

    #[test]
    fn message_split_across_pushes_and_chunks() {
        let payload: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        let mut wire = Vec::new();
        write_message(&payload, &mut wire);
        // >3 chunks on the wire; feed it one byte at a time.
        let mut asm = ChunkAssembler::new();
        for b in &wire {
            asm.push(std::slice::from_ref(b)).unwrap();
        }
        assert_eq!(assemble_all(&mut asm), vec![payload]);
    }

    #[test]
    fn multiple_messages_in_one_push() {
        let mut wire = Vec::new();
        write_message(b"one", &mut wire);
        write_message(b"two", &mut wire);
        let mut asm = ChunkAssembler::new();
        asm.push(&wire).unwrap();
        assert_eq!(
            assemble_all(&mut asm),
            vec![b"one".to_vec(), b"two".to_vec()]
        );
    }

    #[test]
    fn noop_keepalive_is_swallowed() {
        let mut wire = vec![0x00, 0x00, 0x00, 0x00]; // two NOOPs
        write_message(b"real", &mut wire);
        let mut asm = ChunkAssembler::new();
        asm.push(&wire).unwrap();
        assert_eq!(assemble_all(&mut asm), vec![b"real".to_vec()]);
    }

    #[test]
    fn oversized_message_rejected() {
        let mut asm = ChunkAssembler::new();
        // Keep pushing max-size chunks of one message until the cap trips.
        let chunk = [&0xFFFFu16.to_be_bytes()[..], &vec![0u8; 0xFFFF][..]].concat();
        let mut tripped = false;
        for _ in 0..1100 {
            if asm.push(&chunk).is_err() {
                tripped = true;
                break;
            }
        }
        assert!(tripped, "size cap must reject a never-ending message");
    }
}
