//! Streaming commit-v2 writer: encode ops one at a time through a tempfile spool.
//!
//! Unlike [`super::writer::write_commit`] which takes a completed `Commit` and
//! encodes all ops in one pass, this writer accepts individual flakes via
//! [`push_flake`], streaming them through a zstd encoder into a tempfile.
//! Memory stays bounded regardless of commit size.
//!
//! # Usage
//!
//! ```ignore
//! let mut writer = StreamingCommitWriter::new(true)?;   // compress=true
//! for flake in flakes {
//!     writer.push_flake(&flake)?;
//! }
//! let result = writer.finish(&envelope)?;
//! // result.bytes is the complete v4 blob (no embedded hash)
//! ```

use fluree_db_core::commit::codec::envelope::encode_envelope_fields;
use fluree_db_core::commit::codec::format::{
    CommitFooter, CommitHeader, DictLocation, FLAG_ZSTD, FOOTER_LEN, HEADER_LEN, VERSION,
};
use fluree_db_core::commit::codec::op_codec::{encode_op, CommitDicts};
use fluree_db_core::commit::codec::{CodecEnvelope, CommitCodecError};
use fluree_db_core::Flake;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use super::CommitWriteResult;

// ---------------------------------------------------------------------------
// OpsSink — compressed or raw file spool
// ---------------------------------------------------------------------------

/// Ops destination: either a zstd encoder wrapping a file, or a raw file.
enum OpsSink {
    Compressed(zstd::Encoder<'static, File>),
    Raw(File),
}

impl OpsSink {
    fn write_all(&mut self, buf: &[u8]) -> Result<(), CommitCodecError> {
        match self {
            OpsSink::Compressed(enc) => enc
                .write_all(buf)
                .map_err(CommitCodecError::CompressionFailed),
            OpsSink::Raw(file) => file
                .write_all(buf)
                .map_err(CommitCodecError::CompressionFailed),
        }
    }

    /// Finalize the sink and return the underlying file + whether compression was used.
    fn finish(self) -> Result<(File, bool), CommitCodecError> {
        match self {
            OpsSink::Compressed(enc) => {
                let file = enc.finish().map_err(CommitCodecError::CompressionFailed)?;
                Ok((file, true))
            }
            OpsSink::Raw(file) => Ok((file, false)),
        }
    }
}

// ---------------------------------------------------------------------------
// StreamingCommitWriter
// ---------------------------------------------------------------------------

/// A streaming commit-v2 writer that spools encoded ops to a tempfile.
///
/// Ops are encoded one at a time via [`push_flake`], optionally compressed
/// through zstd, and written to a tempfile spool. At [`finish`] time the
/// spool is read back and assembled into the final blob.
pub struct StreamingCommitWriter {
    dicts: CommitDicts,
    sink: OpsSink,
    op_count: u32,
    /// Reusable buffer for encoding one op (avoids per-op allocation).
    temp_op: Vec<u8>,
}

impl StreamingCommitWriter {
    /// Create a new streaming writer.
    ///
    /// If `compress` is true, ops stream through a zstd encoder (level 3)
    /// into the spool file. If false, raw encoded ops are written directly.
    pub fn new(compress: bool) -> Result<Self, CommitCodecError> {
        let file = tempfile::tempfile().map_err(CommitCodecError::CompressionFailed)?;
        let sink = if compress {
            let encoder =
                zstd::Encoder::new(file, 3).map_err(CommitCodecError::CompressionFailed)?;
            OpsSink::Compressed(encoder)
        } else {
            OpsSink::Raw(file)
        };

        Ok(Self {
            dicts: CommitDicts::new(),
            sink,
            op_count: 0,
            temp_op: Vec::with_capacity(256),
        })
    }

    /// Encode one flake as an op and write it to the spool.
    pub fn push_flake(&mut self, flake: &Flake) -> Result<(), CommitCodecError> {
        self.temp_op.clear();
        encode_op(flake, &mut self.dicts, &mut self.temp_op)?;
        self.sink.write_all(&self.temp_op)?;
        self.op_count += 1;
        Ok(())
    }

    /// Number of ops pushed so far.
    pub fn op_count(&self) -> u32 {
        self.op_count
    }

    /// Finalize the commit blob.
    ///
    /// Flushes the encoder (if compressing), reads the spool back, encodes the
    /// envelope and dictionaries, and assembles the final blob:
    /// `[header|envelope|ops|dicts|footer|hash]`.
    pub fn finish(self, envelope: &CodecEnvelope) -> Result<CommitWriteResult, CommitCodecError> {
        let op_count = self.op_count;

        // 1. Finalize the ops sink and read back the spool
        let (mut file, is_compressed) = self.sink.finish()?;
        let ops_section = {
            let _span = tracing::debug_span!("v2_spool_readback", op_count).entered();
            file.seek(SeekFrom::Start(0))
                .map_err(CommitCodecError::CompressionFailed)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)
                .map_err(CommitCodecError::CompressionFailed)?;
            buf
        };

        // 2. Encode envelope (binary)
        let envelope_bytes = {
            let _span = tracing::debug_span!("v2_encode_envelope").entered();
            let mut buf = Vec::new();
            encode_envelope_fields(envelope, &mut buf)?;
            buf
        };

        // 3. Serialize dictionaries
        let dict_bytes = {
            let _span = tracing::debug_span!(
                "v2_serialize_dicts",
                subject_entries = self.dicts.subject.len(),
                predicate_entries = self.dicts.predicate.len(),
                object_ref_entries = self.dicts.object_ref.len(),
            )
            .entered();
            let dicts = self.dicts;
            let bytes: Vec<Vec<u8>> = [
                &dicts.graph,
                &dicts.subject,
                &dicts.predicate,
                &dicts.datatype,
                &dicts.object_ref,
            ]
            .iter()
            .map(|d| d.serialize())
            .collect();
            bytes
        };

        // 4. Calculate total size and allocate output (v4: no embedded hash)
        let total_size = HEADER_LEN
            + envelope_bytes.len()
            + ops_section.len()
            + dict_bytes.iter().map(std::vec::Vec::len).sum::<usize>()
            + FOOTER_LEN;
        let mut output = Vec::with_capacity(total_size);

        // 5. Write header
        let mut flags = 0u8;
        if is_compressed {
            flags |= FLAG_ZSTD;
        }
        let header = CommitHeader {
            version: VERSION,
            flags,
            t: envelope.t,
            op_count,
            envelope_len: envelope_bytes.len() as u32,
            sig_block_len: 0,
        };
        let mut header_buf = [0u8; HEADER_LEN];
        header.write_to(&mut header_buf);
        output.extend_from_slice(&header_buf);

        // 6. Write envelope
        output.extend_from_slice(&envelope_bytes);

        // 7. Write ops section
        output.extend_from_slice(&ops_section);

        // 8. Write dictionaries, recording locations
        let mut dict_locations = [DictLocation::default(); 5];
        for (i, bytes) in dict_bytes.iter().enumerate() {
            dict_locations[i] = DictLocation {
                offset: output.len() as u64,
                len: bytes.len() as u32,
            };
            output.extend_from_slice(bytes);
        }

        // 9. Write footer
        let footer = CommitFooter {
            dicts: dict_locations,
            ops_section_len: ops_section.len() as u32,
        };
        let mut footer_buf = [0u8; FOOTER_LEN];
        footer.write_to(&mut footer_buf);
        output.extend_from_slice(&footer_buf);

        debug_assert_eq!(output.len(), total_size);

        tracing::debug!(
            blob_bytes = output.len(),
            op_count,
            envelope_bytes = envelope_bytes.len(),
            ops_bytes = ops_section.len(),
            compressed = is_compressed,
            "v4 streaming commit written"
        );

        Ok(CommitWriteResult { bytes: output })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::commit::codec::read_commit;
    use fluree_db_core::{FlakeMeta, FlakeValue, Sid};
    use std::collections::HashMap;

    fn make_envelope(t: i64) -> CodecEnvelope {
        CodecEnvelope {
            t,
            previous_refs: Vec::new(),
            namespace_delta: HashMap::new(),
            txn: None,
            time: None,
            txn_signature: None,
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        }
    }

    fn make_flake(s_name: &str, p_name: &str, value: FlakeValue, dt_name: &str, t: i64) -> Flake {
        Flake::new(
            Sid::new(101, s_name),
            Sid::new(101, p_name),
            value,
            Sid::new(2, dt_name),
            t,
            true,
            None,
        )
    }

    #[test]
    fn test_streaming_round_trip_basic() {
        let mut writer = StreamingCommitWriter::new(true).unwrap();
        writer
            .push_flake(&make_flake(
                "Alice",
                "name",
                FlakeValue::String("Alice Smith".into()),
                "string",
                1,
            ))
            .unwrap();
        writer
            .push_flake(&make_flake(
                "Alice",
                "age",
                FlakeValue::Long(30),
                "integer",
                1,
            ))
            .unwrap();

        assert_eq!(writer.op_count(), 2);

        let envelope = make_envelope(1);
        let result = writer.finish(&envelope).unwrap();
        assert!(!result.bytes.is_empty());

        // Round-trip through reader
        let decoded = read_commit(&result.bytes).unwrap();
        assert_eq!(decoded.t, 1);
        assert_eq!(decoded.flakes.len(), 2);
        assert_eq!(decoded.flakes[0].s.name.as_ref(), "Alice");
        assert!(matches!(&decoded.flakes[0].o, FlakeValue::String(s) if s == "Alice Smith"));
        assert!(matches!(&decoded.flakes[1].o, FlakeValue::Long(30)));
    }

    #[test]
    fn test_streaming_round_trip_uncompressed() {
        let mut writer = StreamingCommitWriter::new(false).unwrap();
        writer
            .push_flake(&make_flake("x", "v", FlakeValue::Long(42), "integer", 1))
            .unwrap();

        let envelope = make_envelope(1);
        let result = writer.finish(&envelope).unwrap();

        let decoded = read_commit(&result.bytes).unwrap();
        assert_eq!(decoded.flakes.len(), 1);
        assert!(matches!(&decoded.flakes[0].o, FlakeValue::Long(42)));
    }

    #[test]
    fn test_streaming_empty_commit() {
        let writer = StreamingCommitWriter::new(true).unwrap();
        assert_eq!(writer.op_count(), 0);

        let envelope = make_envelope(1);
        let result = writer.finish(&envelope).unwrap();

        let decoded = read_commit(&result.bytes).unwrap();
        assert_eq!(decoded.flakes.len(), 0);
        assert_eq!(decoded.t, 1);
    }

    #[test]
    fn test_streaming_large_commit() {
        let mut writer = StreamingCommitWriter::new(true).unwrap();
        for i in 0..1000 {
            let value = if i % 3 == 0 {
                FlakeValue::Long(i)
            } else if i % 3 == 1 {
                FlakeValue::String(format!("value_{i}"))
            } else {
                FlakeValue::Ref(Sid::new(101, format!("ref_{i}")))
            };
            let dt = if i % 3 == 2 {
                "id"
            } else if i % 3 == 0 {
                "integer"
            } else {
                "string"
            };
            writer
                .push_flake(&Flake::new(
                    Sid::new(101, format!("s_{i}")),
                    Sid::new(101, format!("p_{}", i % 10)),
                    value,
                    Sid::new(if i % 3 == 2 { 1 } else { 2 }, dt),
                    42,
                    i % 5 != 0,
                    None,
                ))
                .unwrap();
        }

        assert_eq!(writer.op_count(), 1000);

        let envelope = make_envelope(42);
        let result = writer.finish(&envelope).unwrap();

        let decoded = read_commit(&result.bytes).unwrap();
        assert_eq!(decoded.flakes.len(), 1000);
        assert_eq!(decoded.t, 42);
    }

    #[test]
    fn test_streaming_mixed_value_types() {
        let mut writer = StreamingCommitWriter::new(true).unwrap();

        writer
            .push_flake(&make_flake(
                "x",
                "str",
                FlakeValue::String("hello".into()),
                "string",
                1,
            ))
            .unwrap();
        writer
            .push_flake(&make_flake("x", "num", FlakeValue::Long(-42), "long", 1))
            .unwrap();
        writer
            .push_flake(&make_flake(
                "x",
                "dbl",
                FlakeValue::Double(3.13),
                "double",
                1,
            ))
            .unwrap();
        writer
            .push_flake(&make_flake(
                "x",
                "flag",
                FlakeValue::Boolean(true),
                "boolean",
                1,
            ))
            .unwrap();
        writer
            .push_flake(&Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "ref"),
                FlakeValue::Ref(Sid::new(101, "y")),
                Sid::new(1, "id"),
                1,
                true,
                None,
            ))
            .unwrap();

        let result = writer.finish(&make_envelope(1)).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();
        assert_eq!(decoded.flakes.len(), 5);
    }

    #[test]
    fn test_streaming_with_metadata() {
        let mut writer = StreamingCommitWriter::new(true).unwrap();

        // Language-tagged literal
        writer
            .push_flake(&Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "name"),
                FlakeValue::String("Alice".into()),
                Sid::new(3, "langString"),
                1,
                true,
                Some(FlakeMeta::with_lang("en")),
            ))
            .unwrap();

        // List item
        writer
            .push_flake(&Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "items"),
                FlakeValue::Long(42),
                Sid::new(2, "integer"),
                1,
                true,
                Some(FlakeMeta::with_index(0)),
            ))
            .unwrap();

        let result = writer.finish(&make_envelope(1)).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();
        assert_eq!(decoded.flakes.len(), 2);
        assert_eq!(
            decoded.flakes[0].m.as_ref().unwrap().lang.as_deref(),
            Some("en")
        );
        assert_eq!(decoded.flakes[1].m.as_ref().unwrap().i, Some(0));
    }

    #[test]
    fn test_streaming_envelope_fields() {
        use fluree_db_core::{ContentId, ContentKind};
        use fluree_db_novelty::CommitRef;

        let mut writer = StreamingCommitWriter::new(true).unwrap();
        writer
            .push_flake(&make_flake("x", "v", FlakeValue::Long(1), "integer", 5))
            .unwrap();

        let prev_cid = ContentId::new(ContentKind::Commit, b"prev-commit-bytes");
        let envelope = CodecEnvelope {
            t: 5,
            previous_refs: vec![CommitRef::new(prev_cid.clone())],
            namespace_delta: HashMap::from([(200, "ex:".to_string())]),
            txn: None,
            time: Some("2024-01-01T00:00:00Z".into()),
            txn_signature: None,
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };

        let result = writer.finish(&envelope).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        assert_eq!(decoded.t, 5);
        assert_eq!(decoded.previous_refs.first().unwrap().id, prev_cid);
        assert_eq!(decoded.namespace_delta.get(&200), Some(&"ex:".to_string()));
        assert_eq!(decoded.time.as_deref(), Some("2024-01-01T00:00:00Z"));
    }
}
