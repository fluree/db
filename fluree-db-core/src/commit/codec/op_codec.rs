//! Op encode/decode: Flake <-> binary op with commit-local Sid encoding.
//!
//! Each Sid field is stored as (namespace_code: varint, name_id: varint)
//! where namespace_code is a u16 and name_id is a reference into a per-field
//! string dictionary.
//!
//! Op field order:
//! ```text
//! g_ns_code, g_name_id,
//! s_ns_code, s_name_id,
//! p_ns_code, p_name_id,
//! dt_ns_code, dt_name_id,
//! o_tag, o_payload,
//! flags, [lang], [i]
//! ```

use super::error::CommitCodecError;
use super::format::{OTag, OP_FLAG_ASSERT, OP_FLAG_HAS_I, OP_FLAG_HAS_LANG};
use super::string_dict::{StringDict, StringDictBuilder};
use super::varint::{decode_varint, encode_varint, read_exact, read_u8, zigzag_encode};
use crate::{Flake, FlakeMeta, FlakeValue, Sid};

// =============================================================================
// CommitDicts — dictionary set for writing
// =============================================================================

/// The five commit-local string dictionaries used during encoding.
/// Each stores Sid name parts (not full IRIs).
pub struct CommitDicts {
    pub graph: StringDictBuilder,
    pub subject: StringDictBuilder,
    pub predicate: StringDictBuilder,
    pub datatype: StringDictBuilder,
    pub object_ref: StringDictBuilder,
}

impl Default for CommitDicts {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitDicts {
    pub fn new() -> Self {
        Self {
            graph: StringDictBuilder::new(),
            subject: StringDictBuilder::new(),
            predicate: StringDictBuilder::new(),
            datatype: StringDictBuilder::new(),
            object_ref: StringDictBuilder::new(),
        }
    }
}

/// Read-only dictionary set for decoding.
pub struct ReadDicts {
    pub graph: StringDict,
    pub subject: StringDict,
    pub predicate: StringDict,
    pub datatype: StringDict,
    pub object_ref: StringDict,
}

// =============================================================================
// Encode
// =============================================================================

/// Encode a single flake as a binary op, appending to `buf`.
///
/// Sids are encoded directly as (namespace_code, name dict entry) pairs.
/// No IRI reconstruction or NamespaceRegistry needed.
///
/// Returns an error if the flake contains a value type not supported
/// by the v2 format (e.g. `FlakeValue::Vector`).
pub fn encode_op(
    flake: &Flake,
    dicts: &mut CommitDicts,
    buf: &mut Vec<u8>,
) -> Result<(), CommitCodecError> {
    // Graph: None = default graph (0, 0), Some(Sid) = named graph
    if let Some(ref g) = flake.g {
        encode_varint(g.namespace_code as u64, buf);
        let g_name_id = dicts.graph.insert(g.name.as_ref());
        encode_varint(g_name_id as u64, buf);
    } else {
        encode_varint(0, buf); // g_ns_code = 0
        encode_varint(0, buf); // g_name_id = 0 (empty string = default graph)
    }

    // Subject
    encode_varint(flake.s.namespace_code as u64, buf);
    let s_name_id = dicts.subject.insert(flake.s.name.as_ref());
    encode_varint(s_name_id as u64, buf);

    // Predicate
    encode_varint(flake.p.namespace_code as u64, buf);
    let p_name_id = dicts.predicate.insert(flake.p.name.as_ref());
    encode_varint(p_name_id as u64, buf);

    // Datatype
    encode_varint(flake.dt.namespace_code as u64, buf);
    let dt_name_id = dicts.datatype.insert(flake.dt.name.as_ref());
    encode_varint(dt_name_id as u64, buf);

    // Object (tag + payload)
    encode_object(&flake.o, dicts, buf)?;

    // Flags
    let mut flags: u8 = 0;
    if flake.op {
        flags |= OP_FLAG_ASSERT;
    }
    let has_lang = flake.m.as_ref().and_then(|m| m.lang.as_ref()).is_some();
    let has_i = flake.m.as_ref().and_then(|m| m.i).is_some();
    if has_lang {
        flags |= OP_FLAG_HAS_LANG;
    }
    if has_i {
        flags |= OP_FLAG_HAS_I;
    }
    buf.push(flags);

    // Optional lang
    if let Some(lang) = flake.m.as_ref().and_then(|m| m.lang.as_ref()) {
        let lang_bytes = lang.as_bytes();
        encode_varint(lang_bytes.len() as u64, buf);
        buf.extend_from_slice(lang_bytes);
    }

    // Optional list index (unsigned varint — negative indices not supported)
    if let Some(i) = flake.m.as_ref().and_then(|m| m.i) {
        if i < 0 {
            return Err(CommitCodecError::NegativeListIndex(i));
        }
        encode_varint(i as u64, buf);
    }

    Ok(())
}

fn encode_object(
    value: &FlakeValue,
    dicts: &mut CommitDicts,
    buf: &mut Vec<u8>,
) -> Result<(), CommitCodecError> {
    match value {
        FlakeValue::Ref(sid) => {
            buf.push(OTag::Ref as u8);
            encode_varint(sid.namespace_code as u64, buf);
            let name_id = dicts.object_ref.insert(sid.name.as_ref());
            encode_varint(name_id as u64, buf);
        }
        FlakeValue::Long(n) => {
            buf.push(OTag::Long as u8);
            encode_varint(zigzag_encode(*n), buf);
        }
        FlakeValue::Double(d) => {
            buf.push(OTag::Double as u8);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        FlakeValue::String(s) => {
            buf.push(OTag::String as u8);
            encode_len_prefixed_str(s, buf);
        }
        FlakeValue::Boolean(b) => {
            buf.push(OTag::Boolean as u8);
            buf.push(u8::from(*b));
        }
        FlakeValue::DateTime(dt) => {
            buf.push(OTag::DateTime as u8);
            encode_len_prefixed_display(dt.as_ref(), buf);
        }
        FlakeValue::Date(d) => {
            buf.push(OTag::Date as u8);
            encode_len_prefixed_display(d.as_ref(), buf);
        }
        FlakeValue::Time(t) => {
            buf.push(OTag::Time as u8);
            encode_len_prefixed_display(t.as_ref(), buf);
        }
        FlakeValue::BigInt(n) => {
            buf.push(OTag::BigInt as u8);
            encode_len_prefixed_display(n.as_ref(), buf);
        }
        FlakeValue::Decimal(d) => {
            buf.push(OTag::Decimal as u8);
            encode_len_prefixed_display(d.as_ref(), buf);
        }
        FlakeValue::Json(s) => {
            buf.push(OTag::Json as u8);
            encode_len_prefixed_str(s, buf);
        }
        FlakeValue::Null => {
            buf.push(OTag::Null as u8);
        }
        FlakeValue::GYear(v) => {
            buf.push(OTag::GYear as u8);
            encode_len_prefixed_display(v.as_ref(), buf);
        }
        FlakeValue::GYearMonth(v) => {
            buf.push(OTag::GYearMonth as u8);
            encode_len_prefixed_display(v.as_ref(), buf);
        }
        FlakeValue::GMonth(v) => {
            buf.push(OTag::GMonth as u8);
            encode_len_prefixed_display(v.as_ref(), buf);
        }
        FlakeValue::GDay(v) => {
            buf.push(OTag::GDay as u8);
            encode_len_prefixed_display(v.as_ref(), buf);
        }
        FlakeValue::GMonthDay(v) => {
            buf.push(OTag::GMonthDay as u8);
            encode_len_prefixed_display(v.as_ref(), buf);
        }
        FlakeValue::YearMonthDuration(v) => {
            buf.push(OTag::YearMonthDuration as u8);
            encode_len_prefixed_display(v.as_ref(), buf);
        }
        FlakeValue::DayTimeDuration(v) => {
            buf.push(OTag::DayTimeDuration as u8);
            encode_len_prefixed_display(v.as_ref(), buf);
        }
        FlakeValue::Duration(v) => {
            buf.push(OTag::Duration as u8);
            encode_len_prefixed_display(v.as_ref(), buf);
        }
        FlakeValue::GeoPoint(bits) => {
            buf.push(OTag::GeoPoint as u8);
            // Encode as (lat, lng) pair for human-readable commit inspection
            let lat = bits.lat();
            let lng = bits.lng();
            buf.extend_from_slice(&lat.to_le_bytes());
            buf.extend_from_slice(&lng.to_le_bytes());
        }
        FlakeValue::Vector(v) => {
            buf.push(OTag::Vector as u8);
            encode_varint(v.len() as u64, buf);
            for &element in v {
                buf.extend_from_slice(&element.to_le_bytes());
            }
        }
    }
    Ok(())
}

fn encode_len_prefixed_str(s: &str, buf: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    encode_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}

/// Encode a `Display` value as a length-prefixed UTF-8 string without heap
/// allocation. Formats into the output buffer directly by reserving space for
/// the length prefix, writing the value, then backfilling the length.
fn encode_len_prefixed_display(value: &impl std::fmt::Display, buf: &mut Vec<u8>) {
    use std::fmt::Write;

    // Reserve a placeholder for the varint length (we'll overwrite it).
    // Most temporal/numeric strings are <128 bytes, so 1 byte suffices for the varint.
    // Strategy: format into the buf starting after a 1-byte gap, then check if the
    // length fits in 1 varint byte. If not, shift and use multi-byte varint.
    let len_pos = buf.len();
    buf.push(0); // placeholder for 1-byte varint
    let data_start = buf.len();

    // Write the display value directly into the Vec<u8> via a UTF-8 adapter.
    struct VecWriter<'a>(&'a mut Vec<u8>);
    impl std::fmt::Write for VecWriter<'_> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            self.0.extend_from_slice(s.as_bytes());
            Ok(())
        }
    }
    // Display::fmt is infallible for our temporal/numeric types.
    write!(VecWriter(buf), "{value}").expect("Display::fmt failed");

    let data_len = buf.len() - data_start;
    if data_len < 128 {
        // Fits in 1-byte varint — just fill in the placeholder.
        buf[len_pos] = data_len as u8;
    } else {
        // Need multi-byte varint. Encode the length, then shift the data.
        let mut varint_buf = Vec::new();
        encode_varint(data_len as u64, &mut varint_buf);
        let extra = varint_buf.len() - 1; // how many extra bytes beyond our 1-byte placeholder
                                          // Make room by inserting `extra` bytes at data_start.
        buf.splice(len_pos..=len_pos, varint_buf);
        // Data was shifted by `extra` bytes, which splice handles automatically.
        let _ = extra; // splice already handled the shift
    }
}

// =============================================================================
// Decode
// =============================================================================

/// Decode a length-prefixed UTF-8 string, returning an owned `String`.
fn decode_len_prefixed_str(data: &[u8], pos: &mut usize) -> Result<String, CommitCodecError> {
    let len = decode_varint(data, pos)? as usize;
    let bytes = read_exact(data, pos, len)?;
    std::str::from_utf8(bytes)
        .map(std::string::ToString::to_string)
        .map_err(|e| CommitCodecError::InvalidOp(format!("invalid UTF-8: {e}")))
}

/// Decode a varint as a u16 namespace code, returning an error if the value
/// exceeds `u16::MAX`.
fn decode_ns_code(data: &[u8], pos: &mut usize) -> Result<u16, CommitCodecError> {
    let raw = decode_varint(data, pos)?;
    u16::try_from(raw)
        .map_err(|_| CommitCodecError::InvalidOp(format!("namespace code {raw} exceeds u16::MAX")))
}

/// Decode a single op from `data` starting at `*pos`, returning a `Flake`.
///
/// Sids are reconstructed directly from (namespace_code, name) pairs
/// without needing a NamespaceRegistry.
pub fn decode_op(
    data: &[u8],
    pos: &mut usize,
    dicts: &ReadDicts,
    t: i64,
) -> Result<Flake, CommitCodecError> {
    // Graph: (0, 0) = default graph; otherwise named graph Sid encoded as (ns_code, name_id)
    let g_ns_code = decode_ns_code(data, pos)?;
    let g_name_id = decode_varint(data, pos)? as u32;
    let g = if g_ns_code == 0 && g_name_id == 0 {
        None
    } else {
        if g_name_id == 0 {
            return Err(CommitCodecError::InvalidOp(
                "graph name_id 0 is reserved (use (0,0) for default graph)".into(),
            ));
        }
        let g_name = dicts.graph.get(g_name_id)?;
        Some(Sid::new(g_ns_code, g_name))
    };

    // Subject
    let s_ns_code = decode_ns_code(data, pos)?;
    let s_name_id = decode_varint(data, pos)? as u32;
    let s_name = dicts.subject.get(s_name_id)?;
    let s = Sid::new(s_ns_code, s_name);

    // Predicate
    let p_ns_code = decode_ns_code(data, pos)?;
    let p_name_id = decode_varint(data, pos)? as u32;
    let p_name = dicts.predicate.get(p_name_id)?;
    let p = Sid::new(p_ns_code, p_name);

    // Datatype
    let dt_ns_code = decode_ns_code(data, pos)?;
    let dt_name_id = decode_varint(data, pos)? as u32;
    let dt_name = dicts.datatype.get(dt_name_id)?;
    let dt = Sid::new(dt_ns_code, dt_name);

    // Object (tag + payload)
    let o_tag = OTag::from_u8(read_u8(data, pos)?)?;
    let o = decode_object(o_tag, data, pos, dicts)?;

    // Flags
    let flags = read_u8(data, pos)?;
    let op = flags & OP_FLAG_ASSERT != 0;

    // Optional lang
    let lang = if flags & OP_FLAG_HAS_LANG != 0 {
        Some(decode_len_prefixed_str(data, pos)?)
    } else {
        None
    };

    // Optional list index (unsigned varint)
    let i = if flags & OP_FLAG_HAS_I != 0 {
        let raw = decode_varint(data, pos)?;
        if raw > i32::MAX as u64 {
            return Err(CommitCodecError::InvalidOp(format!(
                "list index {raw} exceeds i32::MAX"
            )));
        }
        Some(raw as i32)
    } else {
        None
    };

    let meta = match (&lang, i) {
        (Some(l), Some(idx)) => Some(FlakeMeta {
            lang: Some(l.clone()),
            i: Some(idx),
        }),
        (Some(l), None) => Some(FlakeMeta::with_lang(l)),
        (None, Some(idx)) => Some(FlakeMeta::with_index(idx)),
        (None, None) => None,
    };

    Ok(match g {
        Some(g) => Flake::new_in_graph(g, s, p, o, dt, t, op, meta),
        None => Flake::new(s, p, o, dt, t, op, meta),
    })
}

/// Decode a binary object value into a [`FlakeValue`].
///
/// Delegates binary parsing to [`raw_reader::decode_raw_object`] (shared with
/// the zero-copy raw reader) and converts via `TryFrom<RawObject> for FlakeValue`.
fn decode_object(
    tag: OTag,
    data: &[u8],
    pos: &mut usize,
    dicts: &ReadDicts,
) -> Result<FlakeValue, CommitCodecError> {
    let raw = super::raw_reader::decode_raw_object(tag, data, pos, dicts)?;
    FlakeValue::try_from(raw).map_err(|e| CommitCodecError::InvalidOp(e.to_string()))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_flake_long(
        s_code: u16,
        s_name: &str,
        p_code: u16,
        p_name: &str,
        val: i64,
        t: i64,
    ) -> Flake {
        Flake::new(
            Sid::new(s_code, s_name),
            Sid::new(p_code, p_name),
            FlakeValue::Long(val),
            Sid::new(2, "integer"),
            t,
            true,
            None,
        )
    }

    fn round_trip_dicts(dicts: &CommitDicts) -> ReadDicts {
        ReadDicts {
            graph: StringDict::deserialize(&dicts.graph.serialize()).unwrap(),
            subject: StringDict::deserialize(&dicts.subject.serialize()).unwrap(),
            predicate: StringDict::deserialize(&dicts.predicate.serialize()).unwrap(),
            datatype: StringDict::deserialize(&dicts.datatype.serialize()).unwrap(),
            object_ref: StringDict::deserialize(&dicts.object_ref.serialize()).unwrap(),
        }
    }

    #[test]
    fn test_round_trip_long() {
        let flake = make_flake_long(101, "Alice", 101, "age", 30, 1);
        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
        assert_eq!(pos, buf.len());

        assert_eq!(decoded.s.namespace_code, 101);
        assert_eq!(decoded.s.name.as_ref(), "Alice");
        assert_eq!(decoded.p.namespace_code, 101);
        assert_eq!(decoded.p.name.as_ref(), "age");
        assert_eq!(decoded.dt.namespace_code, 2);
        assert_eq!(decoded.dt.name.as_ref(), "integer");
        assert!(decoded.op); // assert
        assert!(decoded.m.is_none());
        assert!(matches!(decoded.o, FlakeValue::Long(30)));
    }

    #[test]
    fn test_round_trip_with_lang() {
        let flake = Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "name"),
            FlakeValue::String("Alice".to_string()),
            Sid::new(3, "langString"),
            1,
            true,
            Some(FlakeMeta::with_lang("en")),
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();

        assert!(decoded.op);
        assert!(matches!(&decoded.o, FlakeValue::String(s) if s == "Alice"));
        let meta = decoded.m.unwrap();
        assert_eq!(meta.lang.as_deref(), Some("en"));
        assert_eq!(meta.i, None);
    }

    #[test]
    fn test_round_trip_with_list_index() {
        let flake = Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "scores"),
            FlakeValue::Long(42),
            Sid::new(2, "integer"),
            1,
            true,
            Some(FlakeMeta::with_index(3)),
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();

        let meta = decoded.m.unwrap();
        assert_eq!(meta.i, Some(3));
        assert_eq!(meta.lang, None);
    }

    #[test]
    fn test_round_trip_retract() {
        let flake = Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "age"),
            FlakeValue::Long(30),
            Sid::new(2, "integer"),
            1,
            false, // retract
            None,
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
        assert!(!decoded.op); // retract
    }

    #[test]
    fn test_round_trip_ref() {
        let flake = Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "knows"),
            FlakeValue::Ref(Sid::new(101, "Bob")),
            Sid::new(1, "id"),
            1,
            true,
            None,
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();

        // Ref objects are decoded directly as FlakeValue::Ref(Sid)
        match &decoded.o {
            FlakeValue::Ref(sid) => {
                assert_eq!(sid.namespace_code, 101);
                assert_eq!(sid.name.as_ref(), "Bob");
            }
            other => panic!("expected Ref, got {other:?}"),
        }
    }

    #[test]
    fn test_round_trip_double() {
        let flake = Flake::new(
            Sid::new(101, "x"),
            Sid::new(101, "val"),
            FlakeValue::Double(3.13159),
            Sid::new(2, "double"),
            1,
            true,
            None,
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
        match decoded.o {
            FlakeValue::Double(d) => assert!((d - 3.13159).abs() < f64::EPSILON),
            _ => panic!("expected Double"),
        }
    }

    #[test]
    fn test_round_trip_boolean() {
        for val in [true, false] {
            let flake = Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "active"),
                FlakeValue::Boolean(val),
                Sid::new(2, "boolean"),
                1,
                true,
                None,
            );

            let mut dicts = CommitDicts::new();
            let mut buf = Vec::new();
            encode_op(&flake, &mut dicts, &mut buf).unwrap();

            let read_dicts = round_trip_dicts(&dicts);
            let mut pos = 0;
            let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
            assert_eq!(decoded.o, FlakeValue::Boolean(val));
        }
    }

    #[test]
    fn test_round_trip_multiple_ops() {
        let flakes = vec![
            make_flake_long(101, "Alice", 101, "age", 30, 1),
            make_flake_long(101, "Bob", 101, "age", 25, 1),
            make_flake_long(101, "Alice", 101, "score", 100, 1),
        ];

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        for f in &flakes {
            encode_op(f, &mut dicts, &mut buf).unwrap();
        }

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        for original in &flakes {
            let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
            assert_eq!(decoded.s.namespace_code, original.s.namespace_code);
            assert_eq!(decoded.s.name.as_ref(), original.s.name.as_ref());
            assert_eq!(decoded.p.name.as_ref(), original.p.name.as_ref());
        }
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn test_round_trip_vector() {
        let flake = Flake::new(
            Sid::new(101, "x"),
            Sid::new(101, "embedding"),
            FlakeValue::Vector(vec![1.0, 2.5, -3.7]),
            Sid::new(2, "vector"),
            1,
            true,
            None,
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
        assert_eq!(pos, buf.len());
        match &decoded.o {
            FlakeValue::Vector(v) => {
                assert_eq!(v.len(), 3);
                assert!((v[0] - 1.0).abs() < f64::EPSILON);
                assert!((v[1] - 2.5).abs() < f64::EPSILON);
                assert!((v[2] - (-3.7)).abs() < f64::EPSILON);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn test_round_trip_empty_vector() {
        let flake = Flake::new(
            Sid::new(101, "x"),
            Sid::new(101, "embedding"),
            FlakeValue::Vector(vec![]),
            Sid::new(2, "vector"),
            1,
            true,
            None,
        );

        let mut dicts = CommitDicts::new();
        let mut buf = Vec::new();
        encode_op(&flake, &mut dicts, &mut buf).unwrap();

        let read_dicts = round_trip_dicts(&dicts);
        let mut pos = 0;
        let decoded = decode_op(&buf, &mut pos, &read_dicts, 1).unwrap();
        assert_eq!(pos, buf.len());
        assert_eq!(decoded.o, FlakeValue::Vector(vec![]));
    }
}
