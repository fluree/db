//! PackStream binary encoding and decoding.
//!
//! Spec: <https://neo4j.com/docs/bolt/current/packstream/>. Integers use the
//! smallest representation; strings are UTF-8; maps/lists/strings have tiny
//! (<16), 8-, 16-, and 32-bit length forms; structures carry a signature
//! byte and at most 15 fields.

use crate::value::{MapValue, Structure, Value};
use crate::DecodeError;

// Marker bytes.
const NULL: u8 = 0xC0;
const FLOAT64: u8 = 0xC1;
const FALSE: u8 = 0xC2;
const TRUE: u8 = 0xC3;
const INT8: u8 = 0xC8;
const INT16: u8 = 0xC9;
const INT32: u8 = 0xCA;
const INT64: u8 = 0xCB;
const BYTES8: u8 = 0xCC;
const BYTES16: u8 = 0xCD;
const BYTES32: u8 = 0xCE;
const TINY_STRING: u8 = 0x80;
const STRING8: u8 = 0xD0;
const STRING16: u8 = 0xD1;
const STRING32: u8 = 0xD2;
const TINY_LIST: u8 = 0x90;
const LIST8: u8 = 0xD4;
const LIST16: u8 = 0xD5;
const LIST32: u8 = 0xD6;
const TINY_MAP: u8 = 0xA0;
const MAP8: u8 = 0xD8;
const MAP16: u8 = 0xD9;
const MAP32: u8 = 0xDA;
const TINY_STRUCT: u8 = 0xB0;

/// Guards against hostile length headers: no single decoded collection may
/// claim more entries/bytes than this. Inbound Bolt messages are metadata,
/// statements, and parameter maps — far below this bound in practice.
const MAX_DECODE_LEN: usize = 64 * 1024 * 1024;

/// Maximum nesting depth for decoded values (lists/maps/structures). Bolt
/// parameter payloads are shallow; deep nesting is a stack-overflow attack.
const MAX_DECODE_DEPTH: usize = 128;

// ============================================================================
// Encoding
// ============================================================================

pub fn encode(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(NULL),
        Value::Boolean(true) => out.push(TRUE),
        Value::Boolean(false) => out.push(FALSE),
        Value::Integer(i) => encode_int(*i, out),
        Value::Float(f) => {
            out.push(FLOAT64);
            out.extend_from_slice(&f.to_be_bytes());
        }
        Value::Bytes(b) => {
            match b.len() {
                n if n <= 0xFF => {
                    out.push(BYTES8);
                    out.push(n as u8);
                }
                n if n <= 0xFFFF => {
                    out.push(BYTES16);
                    out.extend_from_slice(&(n as u16).to_be_bytes());
                }
                n => {
                    out.push(BYTES32);
                    out.extend_from_slice(&(n as u32).to_be_bytes());
                }
            }
            out.extend_from_slice(b);
        }
        Value::String(s) => encode_str(s, out),
        Value::List(items) => {
            encode_collection_header(TINY_LIST, LIST8, LIST16, LIST32, items.len(), out);
            for item in items {
                encode(item, out);
            }
        }
        Value::Map(map) => {
            encode_collection_header(TINY_MAP, MAP8, MAP16, MAP32, map.0.len(), out);
            for (k, v) in &map.0 {
                encode_str(k, out);
                encode(v, out);
            }
        }
        Value::Structure(s) => {
            debug_assert!(s.fields.len() <= 15, "bolt structures cap at 15 fields");
            out.push(TINY_STRUCT | (s.fields.len() as u8 & 0x0F));
            out.push(s.signature);
            for field in &s.fields {
                encode(field, out);
            }
        }
    }
}

fn encode_int(i: i64, out: &mut Vec<u8>) {
    match i {
        -16..=127 => out.push(i as u8),
        -128..=127 => {
            out.push(INT8);
            out.push(i as u8);
        }
        -32_768..=32_767 => {
            out.push(INT16);
            out.extend_from_slice(&(i as i16).to_be_bytes());
        }
        -2_147_483_648..=2_147_483_647 => {
            out.push(INT32);
            out.extend_from_slice(&(i as i32).to_be_bytes());
        }
        _ => {
            out.push(INT64);
            out.extend_from_slice(&i.to_be_bytes());
        }
    }
}

fn encode_str(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    match bytes.len() {
        n if n <= 15 => out.push(TINY_STRING | n as u8),
        n if n <= 0xFF => {
            out.push(STRING8);
            out.push(n as u8);
        }
        n if n <= 0xFFFF => {
            out.push(STRING16);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        n => {
            out.push(STRING32);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        }
    }
    out.extend_from_slice(bytes);
}

fn encode_collection_header(tiny: u8, m8: u8, m16: u8, m32: u8, len: usize, out: &mut Vec<u8>) {
    match len {
        n if n <= 15 => out.push(tiny | n as u8),
        n if n <= 0xFF => {
            out.push(m8);
            out.push(n as u8);
        }
        n if n <= 0xFFFF => {
            out.push(m16);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        n => {
            out.push(m32);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        }
    }
}

// ============================================================================
// Decoding
// ============================================================================

/// A cursor over a byte slice holding one complete message payload.
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn decode_value(&mut self) -> Result<Value, DecodeError> {
        self.decode_at_depth(0)
    }

    fn decode_at_depth(&mut self, depth: usize) -> Result<Value, DecodeError> {
        if depth > MAX_DECODE_DEPTH {
            return Err(DecodeError::new("value nesting too deep"));
        }
        let marker = self.take_u8()?;
        match marker {
            // TINY_INT: the marker byte is the value (-16..=127, two's complement).
            m @ 0x00..=0x7F => Ok(Value::Integer(m as i64)),
            m @ 0xF0..=0xFF => Ok(Value::Integer(m as i8 as i64)),
            NULL => Ok(Value::Null),
            TRUE => Ok(Value::Boolean(true)),
            FALSE => Ok(Value::Boolean(false)),
            FLOAT64 => {
                let bytes = self.take(8)?;
                Ok(Value::Float(f64::from_be_bytes(bytes.try_into().unwrap())))
            }
            INT8 => Ok(Value::Integer(self.take_u8()? as i8 as i64)),
            INT16 => {
                let b = self.take(2)?;
                Ok(Value::Integer(
                    i16::from_be_bytes(b.try_into().unwrap()) as i64
                ))
            }
            INT32 => {
                let b = self.take(4)?;
                Ok(Value::Integer(
                    i32::from_be_bytes(b.try_into().unwrap()) as i64
                ))
            }
            INT64 => {
                let b = self.take(8)?;
                Ok(Value::Integer(i64::from_be_bytes(b.try_into().unwrap())))
            }
            BYTES8 | BYTES16 | BYTES32 => {
                let len = self.take_len(marker - BYTES8)?;
                Ok(Value::Bytes(self.take(len)?.to_vec()))
            }
            m if m & 0xF0 == TINY_STRING => self.decode_string((m & 0x0F) as usize),
            STRING8 | STRING16 | STRING32 => {
                let len = self.take_len(marker - STRING8)?;
                self.decode_string(len)
            }
            m if m & 0xF0 == TINY_LIST => self.decode_list((m & 0x0F) as usize, depth),
            LIST8 | LIST16 | LIST32 => {
                let len = self.take_len(marker - LIST8)?;
                self.decode_list(len, depth)
            }
            m if m & 0xF0 == TINY_MAP => self.decode_map((m & 0x0F) as usize, depth),
            MAP8 | MAP16 | MAP32 => {
                let len = self.take_len(marker - MAP8)?;
                self.decode_map(len, depth)
            }
            m if m & 0xF0 == TINY_STRUCT => {
                let n_fields = (m & 0x0F) as usize;
                let signature = self.take_u8()?;
                let mut fields = Vec::with_capacity(n_fields);
                for _ in 0..n_fields {
                    fields.push(self.decode_at_depth(depth + 1)?);
                }
                Ok(Value::Structure(Structure { signature, fields }))
            }
            m => Err(DecodeError::new(format!(
                "unknown packstream marker 0x{m:02X}"
            ))),
        }
    }

    /// Read a big-endian length of 1, 2, or 4 bytes (`size_exp` 0, 1, 2).
    fn take_len(&mut self, size_exp: u8) -> Result<usize, DecodeError> {
        let len = match size_exp {
            0 => self.take_u8()? as usize,
            1 => {
                let b = self.take(2)?;
                u16::from_be_bytes(b.try_into().unwrap()) as usize
            }
            _ => {
                let b = self.take(4)?;
                u32::from_be_bytes(b.try_into().unwrap()) as usize
            }
        };
        if len > MAX_DECODE_LEN {
            return Err(DecodeError::new(format!(
                "length {len} exceeds decode limit"
            )));
        }
        Ok(len)
    }

    fn decode_string(&mut self, len: usize) -> Result<Value, DecodeError> {
        let bytes = self.take(len)?;
        let s =
            std::str::from_utf8(bytes).map_err(|_| DecodeError::new("invalid utf-8 in string"))?;
        Ok(Value::String(s.into()))
    }

    fn decode_list(&mut self, len: usize, depth: usize) -> Result<Value, DecodeError> {
        // Cap pre-allocation: a hostile header must not reserve memory the
        // payload can't actually contain (each entry is >= 1 byte).
        let mut items = Vec::with_capacity(len.min(self.remaining()));
        for _ in 0..len {
            items.push(self.decode_at_depth(depth + 1)?);
        }
        Ok(Value::List(items))
    }

    fn decode_map(&mut self, len: usize, depth: usize) -> Result<Value, DecodeError> {
        let mut entries = Vec::with_capacity(len.min(self.remaining()));
        for _ in 0..len {
            let key = match self.decode_at_depth(depth + 1)? {
                Value::String(s) => s,
                other => {
                    return Err(DecodeError::new(format!(
                        "map key must be a string, got {other:?}"
                    )))
                }
            };
            let value = self.decode_at_depth(depth + 1)?;
            entries.push((key, value));
        }
        Ok(Value::Map(MapValue(entries)))
    }

    fn take_u8(&mut self) -> Result<u8, DecodeError> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| DecodeError::new("unexpected end of input"))?;
        self.pos += 1;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.remaining() < n {
            return Err(DecodeError::new("unexpected end of input"));
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }
}

/// Encode a single value to a fresh buffer (test/one-shot convenience).
pub fn encode_to_vec(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode(value, &mut out);
    out
}

/// Decode a single value that must consume the whole buffer.
pub fn decode_exact(buf: &[u8]) -> Result<Value, DecodeError> {
    let mut d = Decoder::new(buf);
    let v = d.decode_value()?;
    if d.remaining() != 0 {
        return Err(DecodeError::new(format!(
            "{} trailing bytes after value",
            d.remaining()
        )));
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::sig;

    fn roundtrip(v: Value) -> Value {
        decode_exact(&encode_to_vec(&v)).expect("roundtrip decode")
    }

    #[test]
    fn spec_fixtures_scalars() {
        // Fixtures from the PackStream specification.
        assert_eq!(encode_to_vec(&Value::Null), [0xC0]);
        assert_eq!(encode_to_vec(&Value::Boolean(true)), [0xC3]);
        assert_eq!(encode_to_vec(&Value::Boolean(false)), [0xC2]);
        assert_eq!(encode_to_vec(&Value::Integer(1)), [0x01]);
        assert_eq!(encode_to_vec(&Value::Integer(42)), [0x2A]);
        assert_eq!(encode_to_vec(&Value::Integer(-16)), [0xF0]);
        assert_eq!(encode_to_vec(&Value::Integer(-1)), [0xFF]);
        assert_eq!(encode_to_vec(&Value::Integer(-17)), [0xC8, 0xEF]);
        assert_eq!(encode_to_vec(&Value::Integer(-128)), [0xC8, 0x80]);
        assert_eq!(encode_to_vec(&Value::Integer(128)), [0xC9, 0x00, 0x80]);
        assert_eq!(
            encode_to_vec(&Value::Integer(-32769)),
            [0xCA, 0xFF, 0xFF, 0x7F, 0xFF]
        );
        assert_eq!(
            encode_to_vec(&Value::Integer(2_147_483_648)),
            [0xCB, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            encode_to_vec(&Value::Float(1.23)),
            [0xC1, 0x3F, 0xF3, 0xAE, 0x14, 0x7A, 0xE1, 0x47, 0xAE]
        );
    }

    #[test]
    fn spec_fixtures_strings_lists_maps() {
        assert_eq!(encode_to_vec(&Value::String("".into())), [0x80]);
        assert_eq!(
            encode_to_vec(&Value::from("hello")),
            [0x85, 0x68, 0x65, 0x6C, 0x6C, 0x6F]
        );
        assert_eq!(
            encode_to_vec(&Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3)
            ])),
            [0x93, 0x01, 0x02, 0x03]
        );
        let mut m = MapValue::new();
        m.insert("one", "eins");
        assert_eq!(
            encode_to_vec(&Value::Map(m)),
            [0xA1, 0x83, 0x6F, 0x6E, 0x65, 0x84, 0x65, 0x69, 0x6E, 0x73]
        );
    }

    #[test]
    fn boundary_integers_roundtrip() {
        for i in [
            i64::MIN,
            i64::MAX,
            -2_147_483_649,
            -2_147_483_648,
            -32_769,
            -32_768,
            -129,
            -128,
            -17,
            -16,
            0,
            127,
            128,
            32_767,
            32_768,
            2_147_483_647,
            2_147_483_648,
        ] {
            assert_eq!(roundtrip(Value::Integer(i)), Value::Integer(i), "int {i}");
        }
    }

    #[test]
    fn long_strings_and_collections_roundtrip() {
        let s16 = "x".repeat(16);
        let s256 = "y".repeat(256);
        let s70k = "z".repeat(70_000);
        for s in [s16, s256, s70k] {
            let encoded = encode_to_vec(&Value::from(s.clone()));
            assert_eq!(decode_exact(&encoded).unwrap(), Value::from(s));
        }
        let list: Vec<Value> = (0..300).map(Value::Integer).collect();
        assert_eq!(roundtrip(Value::List(list.clone())), Value::List(list));
        let map: MapValue = (0..300)
            .map(|i| (format!("k{i}"), Value::Integer(i)))
            .collect();
        assert_eq!(roundtrip(Value::Map(map.clone())), Value::Map(map));
    }

    #[test]
    fn bytes_roundtrip() {
        let b = Value::Bytes(vec![1, 2, 3]);
        assert_eq!(encode_to_vec(&b), [0xCC, 0x03, 0x01, 0x02, 0x03]);
        assert_eq!(roundtrip(b.clone()), b);
        let big = Value::Bytes(vec![0xAB; 70_000]);
        assert_eq!(roundtrip(big.clone()), big);
    }

    #[test]
    fn structure_roundtrip() {
        let node = Value::Structure(Structure {
            signature: sig::NODE,
            fields: vec![
                Value::Integer(1),
                Value::List(vec![Value::from("Person")]),
                Value::Map(MapValue::new()),
                Value::from("4:abc:1"),
            ],
        });
        let bytes = encode_to_vec(&node);
        assert_eq!(bytes[0], 0xB4);
        assert_eq!(bytes[1], sig::NODE);
        assert_eq!(roundtrip(node.clone()), node);
    }

    #[test]
    fn float_roundtrip_preserves_bits() {
        for f in [0.0, -0.0, 1.23, f64::MIN, f64::MAX, f64::INFINITY] {
            assert_eq!(roundtrip(Value::Float(f)), Value::Float(f));
        }
        match roundtrip(Value::Float(f64::NAN)) {
            Value::Float(f) => assert!(f.is_nan()),
            other => panic!("expected float, got {other:?}"),
        }
    }

    #[test]
    fn truncated_input_errors() {
        // String header claims 5 bytes, only 2 present.
        assert!(decode_exact(&[0x85, 0x68, 0x65]).is_err());
        // INT64 with missing payload.
        assert!(decode_exact(&[0xCB, 0x00]).is_err());
        // Empty input.
        assert!(decode_exact(&[]).is_err());
    }

    #[test]
    fn hostile_length_header_rejected() {
        // STRING32 claiming 4 GiB.
        let mut buf = vec![STRING32];
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_exact(&buf).is_err());
    }

    #[test]
    fn deep_nesting_rejected() {
        // 200 nested single-element lists overflow MAX_DECODE_DEPTH.
        let mut buf = vec![0x91; 200];
        buf.push(0x01);
        assert!(decode_exact(&buf).is_err());
    }

    #[test]
    fn non_string_map_key_rejected() {
        // Map with one entry whose key is an integer.
        assert!(decode_exact(&[0xA1, 0x01, 0x01]).is_err());
    }

    #[test]
    fn trailing_bytes_rejected() {
        assert!(decode_exact(&[0x01, 0x02]).is_err());
    }
}
