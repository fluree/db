use crate::binding::Binding;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ids::DatatypeDictId;
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use std::sync::Arc;

fn encoded_i_val(o_i: u32) -> i32 {
    if o_i == u32::MAX {
        i32::MIN
    } else {
        o_i as i32
    }
}

/// Encoded representation for an inline numeric o_type, or `None` if the type
/// has no well-known `dt_id` and must stay on the materialized path.
///
/// Returns `(o_kind, dt_id)` for `EncodedLit`. The `dt_id` is the well-known
/// `DatatypeDictId` whose registry slot maps back to exactly this o_type
/// (`resolve(o_kind, dt_id, 0) == o_type`), so `DATATYPE()` and terminal
/// materialization reconstruct the correct datatype. Restricted to the four
/// numeric types with reserved dict ids — xsd:int / xsd:short / etc. have no
/// well-known id and fall through to materialization unchanged.
fn inline_numeric_encoding(o_type: u16) -> Option<(u8, u16)> {
    let ot = OType::from_u16(o_type);
    if ot == OType::XSD_INTEGER {
        Some((ObjKind::NUM_INT.as_u8(), DatatypeDictId::INTEGER.as_u16()))
    } else if ot == OType::XSD_LONG {
        Some((ObjKind::NUM_INT.as_u8(), DatatypeDictId::LONG.as_u16()))
    } else if ot == OType::XSD_DOUBLE {
        Some((ObjKind::NUM_F64.as_u8(), DatatypeDictId::DOUBLE.as_u16()))
    } else if ot == OType::XSD_FLOAT {
        Some((ObjKind::NUM_F64.as_u8(), DatatypeDictId::FLOAT.as_u16()))
    } else {
        None
    }
}

/// Encoded representation for embedded temporal `OType`s whose `o_key` is already
/// order-preserving and whose datatype has a stable dictionary id.
fn embedded_temporal_encoding(o_type: u16) -> Option<(u8, u16)> {
    let ot = OType::from_u16(o_type);
    if ot == OType::XSD_DATE {
        Some((ObjKind::DATE.as_u8(), DatatypeDictId::DATE.as_u16()))
    } else if ot == OType::XSD_TIME {
        Some((ObjKind::TIME.as_u8(), DatatypeDictId::TIME.as_u16()))
    } else if ot == OType::XSD_DATE_TIME {
        Some((
            ObjKind::DATE_TIME.as_u8(),
            DatatypeDictId::DATE_TIME.as_u16(),
        ))
    } else {
        None
    }
}

/// Build an `EncodedLit` for an inline numeric, or `None` for types that must
/// stay materialized (see [`inline_numeric_encoding`]).
pub(crate) fn inline_numeric_encoded_lit(
    o_type: u16,
    o_key: u64,
    p_id: u32,
    o_i: u32,
    t: i64,
) -> Option<Binding> {
    inline_numeric_encoding(o_type).map(|(o_kind, dt_id)| Binding::EncodedLit {
        o_kind,
        o_key,
        p_id,
        dt_id,
        lang_id: 0,
        i_val: encoded_i_val(o_i),
        t,
    })
}

/// Build a late-materialized object binding for the binary scan path.
///
/// `op` is `Some(true|false)` only in history mode (assert/retract) — it
/// then flows onto ref-valued bindings (`EncodedSid` / blank-node `Sid`)
/// alongside `t`, mirroring how literal-valued objects already carry the
/// metadata. Callers outside history mode pass `None`.
pub(crate) fn late_materialized_object_binding(
    o_type: u16,
    o_key: u64,
    p_id: u32,
    t: i64,
    o_i: u32,
    op: Option<bool>,
) -> Option<Binding> {
    let ot = OType::from_u16(o_type);
    match ot.decode_kind() {
        DecodeKind::IriRef => Some(Binding::EncodedSid {
            s_id: o_key,
            t: Some(t),
            op,
        }),
        DecodeKind::BlankNode => Some(Binding::Sid {
            sid: Sid::new(0, format!("_:b{o_key}")),
            t: Some(t),
            op,
        }),
        DecodeKind::StringDict => {
            let (dt_id, lang_id) = if ot.is_lang_string() {
                (DatatypeDictId::LANG_STRING.as_u16(), ot.payload())
            } else if o_type == OType::FULLTEXT.as_u16() {
                (DatatypeDictId::FULL_TEXT.as_u16(), 0)
            } else {
                (DatatypeDictId::STRING.as_u16(), 0)
            };
            Some(Binding::EncodedLit {
                o_kind: ObjKind::LEX_ID.as_u8(),
                o_key,
                p_id,
                dt_id,
                lang_id,
                i_val: encoded_i_val(o_i),
                t,
            })
        }
        DecodeKind::JsonArena => Some(Binding::EncodedLit {
            o_kind: ObjKind::JSON_ID.as_u8(),
            o_key,
            p_id,
            dt_id: DatatypeDictId::JSON.as_u16(),
            lang_id: 0,
            i_val: encoded_i_val(o_i),
            t,
        }),
        DecodeKind::VectorArena => Some(Binding::EncodedLit {
            o_kind: ObjKind::VECTOR_ID.as_u8(),
            o_key,
            p_id,
            dt_id: DatatypeDictId::VECTOR.as_u16(),
            lang_id: 0,
            i_val: encoded_i_val(o_i),
            t,
        }),
        DecodeKind::NumBigArena => Some(Binding::EncodedLit {
            o_kind: ObjKind::NUM_BIG.as_u8(),
            o_key,
            p_id,
            dt_id: DatatypeDictId::DECIMAL.as_u16(),
            lang_id: 0,
            i_val: encoded_i_val(o_i),
            t,
        }),
        // Inline integer/float values whose datatype has a reserved dict id:
        // keep them encoded so they hash/compare/clone as cheap ints through
        // DISTINCT and joins, with materialization deferred to projection.
        DecodeKind::I64 | DecodeKind::F64 => {
            inline_numeric_encoded_lit(o_type, o_key, p_id, o_i, t)
        }
        // Embedded temporal values are also order-preserving `o_key`s. Keep them
        // late-materialized so cyclic/path joins do not decode and re-intern them
        // for every intermediate row. Only date/time/dateTime have reserved
        // datatype dictionary ids today; the other temporal subtypes stay
        // materialized until their datatype ids are represented in EncodedLit.
        DecodeKind::Date | DecodeKind::Time | DecodeKind::DateTime => {
            embedded_temporal_encoding(o_type).map(|(o_kind, dt_id)| Binding::EncodedLit {
                o_kind,
                o_key,
                p_id,
                dt_id,
                lang_id: 0,
                i_val: encoded_i_val(o_i),
                t,
            })
        }
        _ => None,
    }
}

/// Build a materialized object binding for the binary scan path.
///
/// `op` mirrors the meaning in `late_materialized_object_binding`: it is
/// `Some(...)` only in history mode and is threaded onto the ref- and
/// literal-valued binding alike, so downstream `T(?v)` / `OP(?v)`
/// resolves uniformly across object types.
pub(crate) fn materialized_object_binding(
    store: &BinaryIndexStore,
    o_type: u16,
    p_id: u32,
    val: FlakeValue,
    t: Option<i64>,
    op: Option<bool>,
) -> Binding {
    match val {
        FlakeValue::Ref(sid) => Binding::Sid { sid, t, op },
        other => {
            let dtc = match store.resolve_lang_tag(o_type).map(Arc::from) {
                Some(lang) => DatatypeConstraint::LangTag(lang),
                None => DatatypeConstraint::Explicit(
                    store
                        .resolve_datatype_sid(o_type)
                        .unwrap_or_else(|| Sid::new(0, "")),
                ),
            };
            Binding::Lit {
                val: other,
                dtc,
                t,
                op,
                p_id: Some(p_id),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_materialized_object_binding_keeps_dates_encoded() {
        let binding = late_materialized_object_binding(
            OType::XSD_DATE.as_u16(),
            12_345,
            7,
            0,
            u32::MAX,
            None,
        )
        .expect("xsd:date should stay encoded");

        assert!(matches!(
            binding,
            Binding::EncodedLit {
                o_kind,
                o_key: 12_345,
                p_id: 7,
                dt_id,
                ..
            } if o_kind == ObjKind::DATE.as_u8()
                && dt_id == DatatypeDictId::DATE.as_u16()
        ));
    }

    #[test]
    fn late_materialized_object_binding_keeps_datetime_encoded() {
        let binding = late_materialized_object_binding(
            OType::XSD_DATE_TIME.as_u16(),
            98_765,
            11,
            0,
            u32::MAX,
            None,
        )
        .expect("xsd:dateTime should stay encoded");

        assert!(matches!(
            binding,
            Binding::EncodedLit {
                o_kind,
                o_key: 98_765,
                p_id: 11,
                dt_id,
                ..
            } if o_kind == ObjKind::DATE_TIME.as_u8()
                && dt_id == DatatypeDictId::DATE_TIME.as_u16()
        ));
    }
}
