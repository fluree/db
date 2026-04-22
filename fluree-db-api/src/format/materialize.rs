//! Shared late-materialization helpers for encoded bindings.
//!
//! Binary index scan operators can emit `Binding::Encoded*` values for performance (late
//! materialization). Formatters should materialize these bindings using the `BinaryIndexStore`
//! attached to `QueryResult` before attempting to interpret them.

use super::{FormatError, Result};
use crate::QueryResult;
use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use fluree_db_query::binding::Binding;

/// Materialize an encoded binding to a concrete `Binding` (Sid/Lit/etc).
///
/// If `binding` is not encoded, this returns a clone of the binding.
///
/// # Errors
///
/// - If an encoded binding is encountered but `result.binary_graph` is `None`
/// - If the binary store cannot resolve the encoded IDs
pub(crate) fn materialize_binding(result: &QueryResult, binding: &Binding) -> Result<Binding> {
    if !binding.is_encoded() {
        return Ok(binding.clone());
    }

    let gv = result.binary_graph.as_ref().ok_or_else(|| {
        FormatError::InvalidBinding(
            "Encountered encoded binding during formatting but QueryResult has no binary_graph"
                .to_string(),
        )
    })?;

    materialize_encoded_binding(binding, gv).map_err(|e| {
        FormatError::InvalidBinding(format!("Failed to materialize encoded binding: {e}"))
    })
}

fn materialize_encoded_binding(
    binding: &Binding,
    gv: &BinaryGraphView,
) -> std::io::Result<Binding> {
    let store = gv.store();
    match binding {
        Binding::EncodedSid { s_id } => {
            let iri = store.resolve_subject_iri(*s_id)?;
            let sid = store.encode_iri(&iri);
            Ok(Binding::Sid(sid))
        }
        Binding::EncodedPid { p_id } => match store.resolve_predicate_iri(*p_id) {
            Some(iri) => Ok(Binding::Sid(store.encode_iri(iri))),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Unknown predicate ID: {p_id}"),
            )),
        },
        Binding::EncodedLit { .. } => materialize_encoded_lit(binding, gv),
        _ => Ok(binding.clone()),
    }
}

fn materialize_encoded_lit(binding: &Binding, gv: &BinaryGraphView) -> std::io::Result<Binding> {
    let Binding::EncodedLit {
        o_kind,
        o_key,
        p_id,
        dt_id,
        lang_id,
        i_val,
        t,
    } = binding
    else {
        return Ok(binding.clone());
    };

    let store = gv.store();
    let val = gv.decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)?;
    match val {
        FlakeValue::Ref(sid) => Ok(Binding::Sid(sid)),
        other => {
            let dt_sid = store
                .dt_sids()
                .get(*dt_id as usize)
                .cloned()
                .unwrap_or_else(|| Sid::new(0, ""));
            let meta = store.decode_meta(*lang_id, *i_val);
            let dtc = match meta.and_then(|m| m.lang.map(std::sync::Arc::from)) {
                Some(lang) => DatatypeConstraint::LangTag(lang),
                None => DatatypeConstraint::Explicit(dt_sid),
            };
            Ok(Binding::Lit {
                val: other,
                dtc,
                t: Some(*t),
                op: None,
                p_id: Some(*p_id),
            })
        }
    }
}
