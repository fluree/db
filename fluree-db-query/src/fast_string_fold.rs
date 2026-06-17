//! Fast-path: per-distinct-string folds over a single predicate.
//!
//! Targets aggregates that apply a string function to every object of one
//! predicate and fold the results:
//! - `SELECT (COUNT(*) AS ?c) { ?s <p> ?o FILTER REGEX(?o, "c.m") }` (also CONTAINS)
//! - `SELECT (SUM(STRLEN(?o)) AS ?n) { ?s <p> ?o }` (also STRLEN∘STRBEFORE,
//!   STRLEN∘STRAFTER, and xsd:integer∘STRENDS)
//!
//! POST orders rows by `(o_type, o_key)`, so equal strings are adjacent: the
//! function is evaluated once per distinct value and the result reused for
//! every following duplicate row — O(distinct) evaluations instead of O(rows).
//! Distinct values are materialized in ascending dictionary-ID order
//! (sequential forward-pack access). Unlike MIN/MAX, only equality/adjacency
//! matter, which hold on any index — no `lex_sorted_string_ids` gate.
//!
//! Rows whose objects are not plain strings (numerics, refs, dates, …)
//! contribute 0 and are skipped without IO when a leaflet is homogeneous,
//! matching the fallback: the expression errors on them, FILTER excludes the
//! row, and SUM skips the unbound input. String-dict kinds the evaluator does
//! NOT treat as plain strings (fulltext, custom datatypes, anyURI-family,
//! rdf:JSON) decline the whole fast path instead of risking a semantic
//! mismatch.

use crate::binding::Batch;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    bail_if_cancelled, build_count_batch, build_i64_singleton_batch, count_rows_for_predicate_psot,
    count_to_i64, cursor_fast_path_for_predicate, fast_path_store_policy_cleared,
    leaf_entries_for_predicate, normalize_pred_sid, parallel_leaf_chunk_count, projection_okey_only,
    projection_otype_okey, FastPathOperator, PredicateFastPath,
};
use crate::ir::triple::Ref;
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::GraphId;
use std::sync::Arc;

/// A string-function aggregate the fold supports. Needles/patterns are the
/// constant second argument from the query.
#[derive(Clone, Debug)]
pub enum StringFoldAgg {
    /// `COUNT(*)` of rows where `REGEX(?o, pattern[, flags])` holds.
    CountRegex { pattern: Arc<str>, flags: Arc<str> },
    /// `COUNT(*)` of rows where `CONTAINS(?o, needle)` holds.
    CountContains { needle: Arc<str> },
    /// `SUM(STRLEN(?o))`
    SumStrlen,
    /// `SUM(STRLEN(STRBEFORE(?o, needle)))`
    SumStrlenBefore { needle: Arc<str> },
    /// `SUM(STRLEN(STRAFTER(?o, needle)))`
    SumStrlenAfter { needle: Arc<str> },
    /// `SUM(xsd:integer(STRENDS(?o, needle)))`
    SumStrEnds { needle: Arc<str> },
}

impl StringFoldAgg {
    pub fn label(&self) -> &'static str {
        match self {
            Self::CountRegex { .. } => "COUNT(REGEX)",
            Self::CountContains { .. } => "COUNT(CONTAINS)",
            Self::SumStrlen => "SUM(STRLEN)",
            Self::SumStrlenBefore { .. } => "SUM(STRBEFORE)",
            Self::SumStrlenAfter { .. } => "SUM(STRAFTER)",
            Self::SumStrEnds { .. } => "SUM(STRENDS)",
        }
    }

    fn is_count(&self) -> bool {
        matches!(self, Self::CountRegex { .. } | Self::CountContains { .. })
    }

    /// Compile to the per-string evaluator. `None` ⇒ the regex does not
    /// compile; the fallback FILTER errors per row (excluding every row), so
    /// declining keeps behavior identical.
    fn compile(&self) -> Option<CompiledFold> {
        Some(match self {
            Self::CountRegex { pattern, flags } => {
                CompiledFold::Regex(crate::eval::build_regex_with_flags(pattern, flags).ok()?)
            }
            Self::CountContains { needle } => CompiledFold::Contains(Arc::clone(needle)),
            Self::SumStrlen => CompiledFold::Strlen,
            Self::SumStrlenBefore { needle } => CompiledFold::StrlenBefore(Arc::clone(needle)),
            Self::SumStrlenAfter { needle } => CompiledFold::StrlenAfter(Arc::clone(needle)),
            Self::SumStrEnds { needle } => CompiledFold::StrEnds(Arc::clone(needle)),
        })
    }
}

/// Compiled per-string evaluator. Each variant mirrors the expression
/// evaluator in `eval/string.rs` exactly (byte-level search, codepoint
/// STRLEN, empty string when STRBEFORE/STRAFTER needles are absent).
enum CompiledFold {
    Regex(regex::Regex),
    Contains(Arc<str>),
    Strlen,
    StrlenBefore(Arc<str>),
    StrlenAfter(Arc<str>),
    StrEnds(Arc<str>),
}

impl CompiledFold {
    fn eval(&self, s: &str) -> u64 {
        match self {
            Self::Regex(re) => u64::from(re.is_match(s)),
            Self::Contains(n) => u64::from(s.contains(n.as_ref())),
            Self::Strlen => strlen_codepoints(s),
            Self::StrlenBefore(n) => {
                strlen_codepoints(s.find(n.as_ref()).map(|p| &s[..p]).unwrap_or(""))
            }
            Self::StrlenAfter(n) => {
                strlen_codepoints(s.find(n.as_ref()).map(|p| &s[p + n.len()..]).unwrap_or(""))
            }
            Self::StrEnds(n) => u64::from(s.ends_with(n.as_ref())),
        }
    }
}

/// SPARQL STRLEN counts codepoints (matches `eval_strlen`).
fn strlen_codepoints(s: &str) -> u64 {
    if s.is_ascii() {
        s.len() as u64
    } else {
        s.chars().count() as u64
    }
}

/// Which object kinds the fold evaluates — set by the query shape's
/// fallback semantics.
#[derive(Clone, Copy, Debug)]
enum StringKindPolicy {
    /// Expression shapes (FILTER/SUM of a string function): only xsd:string
    /// and rdf:langString evaluate; other non-string kinds error in the
    /// fallback (FILTER excludes the row, SUM skips it) so they contribute 0;
    /// string-dict kinds the evaluator may treat differently (fulltext,
    /// custom datatypes, anyURI family, rdf:JSON) decline.
    PlainOnly,
    /// GROUP_CONCAT stringification: every string-dict-backed kind evaluates
    /// on its dictionary value; anything else (inline numerics, refs, …)
    /// declines.
    AnyStringDict,
}

/// How a leaflet/row object participates in the fold.
enum ObjClass {
    /// Evaluate the function on the dictionary value.
    Evaluate,
    /// Contributes 0 (fallback expression-error semantics).
    Skip,
    /// Decline the fast path.
    Unsupported,
}

fn classify_otype(o_type: u16, policy: StringKindPolicy) -> ObjClass {
    let ot = OType::from_u16(o_type);
    let is_string_dict = ot.decode_kind() == DecodeKind::StringDict;
    match policy {
        StringKindPolicy::PlainOnly => {
            if o_type == OType::XSD_STRING.as_u16() || ot.is_lang_string() {
                ObjClass::Evaluate
            } else if is_string_dict || o_type == OType::RDF_JSON.as_u16() {
                ObjClass::Unsupported
            } else {
                ObjClass::Skip
            }
        }
        StringKindPolicy::AnyStringDict => {
            if is_string_dict {
                ObjClass::Evaluate
            } else {
                ObjClass::Unsupported
            }
        }
    }
}

/// `Σ STRLEN(?o)` over every row of one predicate, evaluating any
/// string-dict-backed kind (GROUP_CONCAT stringification semantics).
///
/// Returns `Ok(None)` when an object is not string-dict-backed — the caller
/// must fall back.
pub(crate) fn sum_strlen_any_string_dict(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    cancellation: &fluree_db_core::QueryCancellation,
) -> Result<Option<u64>> {
    scan_string_fold(
        store,
        g_id,
        p_id,
        &CompiledFold::Strlen,
        StringKindPolicy::AnyStringDict,
        cancellation,
    )
}

/// Create a fused operator that outputs the folded aggregate as one row.
pub fn predicate_string_fold_operator(
    predicate: Ref,
    agg: StringFoldAgg,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    let label = agg.label();
    FastPathOperator::new(
        out_var,
        move |ctx| {
            // O1: keep the fast path only when the scanned predicate is provably
            // uncovered by the view policy; otherwise defer to the fallback, which
            // computes the correct aggregate over the policy-filtered input.
            if let Some(store) = ctx.binary_store.as_ref() {
                let pred_sid = normalize_pred_sid(store, &predicate)?;
                if !matches!(
                    cursor_fast_path_for_predicate(ctx, &pred_sid),
                    PredicateFastPath::Allow
                ) {
                    return Ok(None);
                }
            }
            let Some(store) = fast_path_store_policy_cleared(ctx) else {
                return Ok(None);
            };
            // Persisted index rows only — defer when novelty is present.
            if ctx
                .overlay
                .map(fluree_db_core::OverlayProvider::epoch)
                .unwrap_or(0)
                != 0
            {
                return Ok(None);
            }
            let pred_sid = normalize_pred_sid(store, &predicate)?;
            let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                // Predicate absent -> empty input -> COUNT/SUM are 0.
                return Ok(Some(fold_output(&agg, out_var, 0)?));
            };
            let Some(compiled) = agg.compile() else {
                return Ok(None);
            };
            let Some(total) = scan_string_fold(
                store,
                ctx.binary_g_id,
                p_id,
                &compiled,
                StringKindPolicy::PlainOnly,
                &ctx.cancellation,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(fold_output(&agg, out_var, total)?))
        },
        fallback,
        label,
    )
}

fn fold_output(agg: &StringFoldAgg, out_var: VarId, total: u64) -> Result<Batch> {
    let v = count_to_i64(total, agg.label())?;
    if agg.is_count() {
        build_count_batch(out_var, v)
    } else {
        build_i64_singleton_batch(out_var, v, agg.label())
    }
}

/// Scan the predicate's POST range, evaluating `fold` once per distinct
/// `(o_type, o_key)` and charging the cached result to every duplicate row.
///
/// Parallelized over contiguous leaf chunks (rows fold independently, so no
/// seam stitching is needed; a value spanning a chunk seam just costs one
/// extra evaluation). Returns `Ok(None)` on an unsupported object kind.
fn scan_string_fold(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    fold: &CompiledFold,
    policy: StringKindPolicy,
    cancellation: &fluree_db_core::QueryCancellation,
) -> Result<Option<u64>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    if leaves.is_empty() {
        return Ok(Some(0));
    }
    let total_rows = count_rows_for_predicate_psot(store, g_id, p_id)?;

    let reducer = |chunk: &[LeafEntry]| -> Result<Option<u64>> {
        let mut sum: u64 = 0;
        let mut buf: Vec<u8> = Vec::new();
        // (o_type, o_key) -> per-row contribution of the current run.
        let mut cached: Option<(u16, u64, u64)> = None;

        for leaf_entry in chunk {
            bail_if_cancelled(cancellation)?;
            let dir = store
                .open_leaf_dir(&leaf_entry.leaf_cid)
                .map_err(|e| QueryError::Internal(format!("leaf dir open: {e}")))?;
            let mut handle = None;

            for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
                if entry.row_count == 0 || entry.p_const != Some(p_id) {
                    continue;
                }
                let const_class = entry.o_type_const.map(|ot| classify_otype(ot, policy));
                match const_class {
                    Some(ObjClass::Skip) => continue, // contributes 0, no IO
                    Some(ObjClass::Unsupported) => return Ok(None),
                    Some(ObjClass::Evaluate) | None => {}
                }

                if handle.is_none() {
                    handle = Some(
                        store
                            .open_leaf_handle(
                                &leaf_entry.leaf_cid,
                                leaf_entry.sidecar_cid.as_ref(),
                                false,
                            )
                            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                    );
                }
                let projection = if entry.o_type_const.is_some() {
                    projection_okey_only()
                } else {
                    projection_otype_okey()
                };
                let batch = handle
                    .as_ref()
                    .expect("handle opened above")
                    .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

                for row in 0..batch.row_count {
                    let o_type = entry
                        .o_type_const
                        .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                    let o_key = batch.o_key.get(row);

                    if let Some((ct, ck, cv)) = cached {
                        if ct == o_type && ck == o_key {
                            sum = sum.saturating_add(cv);
                            continue;
                        }
                    }
                    let contribution = match classify_otype(o_type, policy) {
                        ObjClass::Skip => 0,
                        ObjClass::Unsupported => return Ok(None),
                        ObjClass::Evaluate => {
                            let Ok(str_id) = u32::try_from(o_key) else {
                                return Ok(None);
                            };
                            buf.clear();
                            let found = store
                                .string_lookup_into(str_id, &mut buf)
                                .map_err(|e| QueryError::Internal(format!("string lookup: {e}")))?;
                            if !found {
                                return Err(QueryError::Internal(format!(
                                    "string id {str_id} missing from dictionary"
                                )));
                            }
                            let s = std::str::from_utf8(&buf).map_err(|e| {
                                QueryError::Internal(format!("non-UTF8 dictionary string: {e}"))
                            })?;
                            fold.eval(s)
                        }
                    };
                    cached = Some((o_type, o_key, contribution));
                    sum = sum.saturating_add(contribution);
                }
            }
        }
        Ok(Some(sum))
    };

    parallel_leaf_chunk_count(leaves, total_rows, reducer)
}
