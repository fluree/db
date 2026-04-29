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
