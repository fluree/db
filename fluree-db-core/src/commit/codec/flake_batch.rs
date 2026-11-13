//! An exact, standalone encoding of a list of flakes.
//!
//! The commit codec's per-op encoding and its five dictionaries, without a
//! commit around them, plus each flake's `t`. Serde's encoding of
//! [`FlakeValue`](crate::FlakeValue) is untagged and does not round-trip
//! every value (a `Long` and a `Double` of the same magnitude, a date-time
//! and its lexical string), so flakes that must come back exactly — a
//! retraction batch crossing a consensus queue, a persisted retention
//! schedule — use this instead.

use super::op_codec::{decode_op, encode_op, CommitDicts, ReadDicts};
use super::string_dict::StringDict;
use super::varint::{decode_varint, encode_varint, read_exact, zigzag_decode, zigzag_encode};
use super::CommitCodecError;
use crate::Flake;

const MAGIC: &[u8; 4] = b"FFB1";

/// Encode `flakes`, in order. Fails on a value the commit format cannot
/// carry (as `encode_op` does).
pub fn encode_flake_batch(flakes: &[Flake]) -> Result<Vec<u8>, CommitCodecError> {
    let mut dicts = CommitDicts::new();
    let mut ops = Vec::new();
    for flake in flakes {
        encode_varint(zigzag_encode(flake.t), &mut ops);
        encode_op(flake, &mut dicts, &mut ops)?;
    }
    let mut out = Vec::with_capacity(ops.len() + 64);
    out.extend_from_slice(MAGIC);
    encode_varint(flakes.len() as u64, &mut out);
    for dict in [
        &dicts.graph,
        &dicts.subject,
        &dicts.predicate,
        &dicts.datatype,
        &dicts.object_ref,
    ] {
        let bytes = dict.serialize();
        encode_varint(bytes.len() as u64, &mut out);
        out.extend_from_slice(&bytes);
    }
    out.extend_from_slice(&ops);
    Ok(out)
}

/// Decode a batch written by [`encode_flake_batch`].
pub fn decode_flake_batch(data: &[u8]) -> Result<Vec<Flake>, CommitCodecError> {
    if data.len() < MAGIC.len() || &data[..MAGIC.len()] != MAGIC {
        return Err(CommitCodecError::InvalidOp("not a flake batch".into()));
    }
    let mut pos = MAGIC.len();
    let count = decode_varint(data, &mut pos)? as usize;
    let mut dict = || -> Result<StringDict, CommitCodecError> {
        let len = decode_varint(data, &mut pos)? as usize;
        StringDict::deserialize(read_exact(data, &mut pos, len)?)
    };
    let dicts = ReadDicts {
        graph: dict()?,
        subject: dict()?,
        predicate: dict()?,
        datatype: dict()?,
        object_ref: dict()?,
    };
    let mut flakes = Vec::with_capacity(count.min(data.len()));
    for _ in 0..count {
        let t = zigzag_decode(decode_varint(data, &mut pos)?);
        flakes.push(decode_op(data, &mut pos, &dicts, t)?);
    }
    if pos != data.len() {
        return Err(CommitCodecError::InvalidOp(
            "trailing bytes after flake batch".into(),
        ));
    }
    Ok(flakes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FlakeMeta, FlakeValue, Sid};

    #[test]
    fn round_trips_values_serde_would_conflate() {
        let s = Sid::new(100, "s");
        let p = Sid::new(100, "p");
        let g = Sid::new(101, "g");
        let flakes = vec![
            Flake::new(
                s.clone(),
                p.clone(),
                FlakeValue::Long(3),
                Sid::new(2, "long"),
                5,
                true,
                None,
            ),
            Flake::new(
                s.clone(),
                p.clone(),
                FlakeValue::Double(3.0),
                Sid::new(2, "double"),
                -7,
                false,
                None,
            ),
            Flake::new_in_graph(
                g,
                s.clone(),
                p.clone(),
                FlakeValue::String("chat".into()),
                Sid::new(3, "langString"),
                9,
                true,
                Some(FlakeMeta::with_lang("fr")),
            ),
            Flake::new(
                s,
                p,
                FlakeValue::Ref(Sid::new(100, "o")),
                Sid::new(1, "id"),
                9,
                true,
                None,
            ),
        ];
        let bytes = encode_flake_batch(&flakes).unwrap();
        let back = decode_flake_batch(&bytes).unwrap();
        assert_eq!(back.len(), flakes.len());
        for (a, b) in flakes.iter().zip(&back) {
            assert_eq!(a.g, b.g);
            assert_eq!(a.s, b.s);
            assert_eq!(a.p, b.p);
            assert_eq!(a.o, b.o);
            assert_eq!(a.dt, b.dt);
            assert_eq!(a.t, b.t);
            assert_eq!(a.op, b.op);
            assert_eq!(a.m, b.m);
        }
        assert!(matches!(back[1].o, FlakeValue::Double(_)));
    }

    #[test]
    fn rejects_trailing_bytes_and_foreign_input() {
        let mut bytes = encode_flake_batch(&[]).unwrap();
        assert!(decode_flake_batch(&bytes).unwrap().is_empty());
        bytes.push(0);
        assert!(decode_flake_batch(&bytes).is_err());
        assert!(decode_flake_batch(b"FCV2").is_err());
    }
}
