//! Binary scan operator — eagerly materializes `ColumnBatch` rows into `Binding` values.
//!
//! - Uses `BinaryCursor` (leaflet-at-a-time columnar batches)
//! - Uses `o_type` for value dispatch
//! - Eagerly materializes all values (no EncodedLit/EncodedSid)
//!
//! The eager approach trades some allocation for simplicity. Deferred decoding
//! can be added in a follow-up when perf requires it.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::read::column_types::ColumnSet;
use fluree_db_binary_index::{
    resolve_overlay_ops, sort_overlay_ops, BinaryCursor, BinaryFilter, BinaryGraphView,
    BinaryIndexStore, ColumnBatch, ColumnProjection, OverlayOp,
};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKey;
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{
    dt_compatible, range_with_overlay, Flake, FlakeMeta, FlakeValue, GraphId, IndexType,
    LedgerSnapshot, NoOverlay, ObjectBounds, OverlayProvider, RangeMatch, RangeOptions, RangeTest,
    RuntimePredicateId, RuntimeSmallDicts, Sid,
};

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::contiguous_id_range;
use crate::ir::{Expression, Function};
use crate::object_binding::{late_materialized_object_binding, materialized_object_binding};
use crate::operator::inline::{apply_inline, extend_schema, InlineOperator};
use crate::operator::{Operator, OperatorState};
use crate::sid_iri;
use crate::stats_cache::cached_stats_view_for_db;
use crate::triple::{Ref, Term, TriplePattern};
use crate::var_registry::VarId;
use fluree_vocab::{namespaces, rdf_names, xsd_names};

// ============================================================================
// Shared types and utilities
// ============================================================================

use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
/// Mask indicating which triple components should be emitted as columns.
///
/// Used by plan-time optimizations to prune unused output columns.
#[derive(Debug, Clone, Copy)]
pub struct EmitMask {
    pub s: bool,
    pub p: bool,
    pub o: bool,
}

impl EmitMask {
    pub const ALL: EmitMask = EmitMask {
        s: true,
        p: true,
        o: true,
    };
}

/// Convert `IndexType` (query-layer) to `RunSortOrder` (binary-index-layer).
pub fn index_type_to_sort_order(index: IndexType) -> RunSortOrder {
    match index {
        IndexType::Spot => RunSortOrder::Spot,
        IndexType::Psot => RunSortOrder::Psot,
        IndexType::Post => RunSortOrder::Post,
        IndexType::Opst => RunSortOrder::Opst,
    }
}

/// Build a schema vector from a triple pattern, respecting `EmitMask` pruning.
///
/// Returns `(schema, s_var_pos, p_var_pos, o_var_pos)` where positions are
/// indices into the schema if the corresponding variable is emitted.
pub fn schema_from_pattern_with_emit(
    pattern: &TriplePattern,
    emit: EmitMask,
) -> (Vec<VarId>, Option<usize>, Option<usize>, Option<usize>) {
    let mut schema = Vec::new();
    let mut s_pos = None;
    let mut p_pos = None;
    let mut o_pos = None;

    let mut find_or_push = |v: VarId| -> usize {
        if let Some(idx) = schema.iter().position(|x| *x == v) {
            idx
        } else {
            let idx = schema.len();
            schema.push(v);
            idx
        }
    };

    if emit.s {
        if let Ref::Var(v) = &pattern.s {
            s_pos = Some(find_or_push(*v));
        }
    }
    if emit.p {
        if let Ref::Var(v) = &pattern.p {
            p_pos = Some(find_or_push(*v));
        }
    }
    if emit.o {
        if let Term::Var(v) = &pattern.o {
            o_pos = Some(find_or_push(*v));
        }
    }

    (schema, s_pos, p_pos, o_pos)
}

#[inline]
fn expr_needs_t(expr: &Expression) -> bool {
    match expr {
        Expression::Var(_) | Expression::Const(_) => false,
        Expression::Call { func, args } => {
            matches!(func, Function::T) || args.iter().any(expr_needs_t)
        }
        Expression::Exists { .. } => false,
    }
}

#[inline]
fn inline_ops_need_t(ops: &[InlineOperator]) -> bool {
    ops.iter().any(|op| match op {
        InlineOperator::Filter(e) => expr_needs_t(e.expr()),
        InlineOperator::Bind { expr, .. } => expr_needs_t(expr),
    })
}

// `translate_overlay_flakes` lives below, after BinaryScanOperator.

// ============================================================================
// BinaryScanOperator
// ============================================================================

/// Scan operator: streams leaflets from `BinaryCursor`, eagerly decoding
/// `ColumnBatch` rows into `Binding::Sid` / `Binding::Lit` values.
pub struct BinaryScanOperator {
    pattern: TriplePattern,
    index: IndexType,
    schema: Arc<[VarId]>,
    s_var_pos: Option<usize>,
    p_var_pos: Option<usize>,
    o_var_pos: Option<usize>,
    state: OperatorState,
    /// Set in `open()` from `ExecutionContext`.
    store: Option<Arc<BinaryIndexStore>>,
    g_id: GraphId,
    cursor: Option<BinaryCursor>,
    /// Pre-computed p_id → Sid (all predicates, done once at open).
    p_sids: Vec<Sid>,
    /// Cached s_id → Sid for amortized IRI resolution.
    sid_cache: HashMap<u64, Sid>,
    /// Whether predicate is a variable (for internal predicate filtering).
    p_is_var: bool,
    inline_ops: Vec<InlineOperator>,
    /// Encoded pre-filters compiled from inline filter expressions.
    ///
    /// Evaluated on `(s_id, o_type, o_key)` before any value decoding.
    encoded_pre_filters: Vec<EncodedPreFilter>,
    // Kept for: plan-time emit pruning and index override during query optimization.
    // Use when: planner emits BinaryScanOperator with pruned columns or forced index.
    #[expect(dead_code)]
    emit: EmitMask,
    #[expect(dead_code)]
    index_hint: Option<IndexType>,
    object_bounds: Option<ObjectBounds>,
    /// Bound object value, if the triple pattern's object is a constant.
    bound_o: Option<FlakeValue>,
    /// Pre-computed repeated-variable flags from the triple pattern.
    check_s_eq_o: bool,
    check_s_eq_p: bool,
    check_p_eq_o: bool,
    /// Range-scan fallback iterator (used when no binary store is attached).
    range_iter: Option<std::vec::IntoIter<Flake>>,
    /// When a bound subject IRI cannot be translated to a persisted `s_id`,
    /// keep a widened base scan correct by checking the resolved subject IRI row-by-row.
    unresolved_bound_subject_iri: Option<Arc<str>>,
}

/// A filter that can be evaluated on encoded index columns (no term decoding).
#[derive(Clone, Debug)]
enum EncodedPreFilter {
    /// `FILTER(LANG(?o) = "<tag>")` for the object var `?o` in this scan.
    LangEqualsOType { required_otype: u16 },
    /// `FILTER(ISBLANK(?o))` for the object var `?o` in this scan.
    ///
    /// Blank nodes are currently encoded as `OType::IRI_REF` with a `sid64` whose
    /// `SubjectId.ns_code == namespaces::BLANK_NODE` (not as `OType::BLANK_NODE`).
    ObjectIsBlankNode,
    /// `FILTER(?s = ?o)` where `?o` is a REF (IRI or bnode) and equals the subject id.
    SubjectEqObjectRef,
    /// `FILTER(?s != ?o)` under two-valued logic: false only when both sides are comparable+equal.
    SubjectNeObjectRef,
    /// `FILTER(STRSTARTS(?o, "..."))` or anchored literal `REGEX(?o, "^...")`
    /// on dictionary-backed string objects.
    ObjectStringPrefix { id_ranges: Arc<[(u32, u32)]> },
}

impl EncodedPreFilter {
    #[inline]
    fn eval_row(&self, s_id: u64, o_type: u16, o_key: u64) -> bool {
        match self {
            EncodedPreFilter::LangEqualsOType { required_otype } => o_type == *required_otype,
            EncodedPreFilter::ObjectIsBlankNode => {
                if o_type != fluree_db_core::o_type::OType::IRI_REF.as_u16() {
                    return false;
                }
                fluree_db_core::subject_id::SubjectId::from_u64(o_key).ns_code()
                    == fluree_vocab::namespaces::BLANK_NODE
            }
            EncodedPreFilter::SubjectEqObjectRef => {
                let is_ref = o_type == fluree_db_core::o_type::OType::IRI_REF.as_u16()
                    || o_type == fluree_db_core::o_type::OType::BLANK_NODE.as_u16();
                is_ref && s_id == o_key
            }
            EncodedPreFilter::SubjectNeObjectRef => {
                let is_ref = o_type == fluree_db_core::o_type::OType::IRI_REF.as_u16()
                    || o_type == fluree_db_core::o_type::OType::BLANK_NODE.as_u16();
                !(is_ref && s_id == o_key)
            }
            EncodedPreFilter::ObjectStringPrefix { id_ranges } => {
                let ot = OType::from_u16(o_type);
                if ot.decode_kind() != DecodeKind::StringDict {
                    return false;
                }
                let Ok(str_id) = u32::try_from(o_key) else {
                    return false;
                };
                range_contains(id_ranges, str_id)
            }
        }
    }
}

fn compile_encoded_pre_filters_and_prune_inline_ops(
    inline_ops: &[InlineOperator],
    pattern: &TriplePattern,
    store: &BinaryIndexStore,
    allow_string_prefix_pushdown: bool,
) -> (Vec<EncodedPreFilter>, Vec<InlineOperator>) {
    use crate::ir::{Expression, FilterValue, Function};

    let obj_var = match &pattern.o {
        Term::Var(v) => Some(*v),
        _ => None,
    };
    let subj_var = match &pattern.s {
        Ref::Var(v) => Some(*v),
        _ => None,
    };

    let mut out = Vec::new();
    let mut pruned = Vec::with_capacity(inline_ops.len());
    for op in inline_ops {
        let InlineOperator::Filter(expr) = op else {
            pruned.push(op.clone());
            continue;
        };
        let Expression::Call { func, args } = expr.expr() else {
            pruned.push(op.clone());
            continue;
        };
        if allow_string_prefix_pushdown {
            if let Some(prefix) =
                extract_object_string_prefix(expr.expr(), obj_var, pattern.dtc.as_ref())
            {
                match build_prefix_id_ranges(store, prefix.as_ref()) {
                    Ok(id_ranges) => {
                        out.push(EncodedPreFilter::ObjectStringPrefix { id_ranges });
                        continue;
                    }
                    Err(_) => {
                        pruned.push(op.clone());
                        continue;
                    }
                }
            }
        }
        if args.len() == 1 {
            // FILTER(ISBLANK(?o))
            if *func == Function::IsBlank {
                if let (Some(ov), Expression::Var(v)) = (obj_var, &args[0]) {
                    if *v == ov {
                        out.push(EncodedPreFilter::ObjectIsBlankNode);
                        continue;
                    }
                }
            }
            pruned.push(op.clone());
            continue;
        }
        if args.len() != 2 {
            pruned.push(op.clone());
            continue;
        }

        // FILTER(LANG(?o) = "en")  (either side order)
        let is_lang_o = |e: &Expression| match (e, obj_var) {
            (Expression::Call { func, args }, Some(ov)) => {
                *func == Function::Lang
                    && args.len() == 1
                    && matches!(&args[0], Expression::Var(v) if *v == ov)
            }
            _ => false,
        };
        if is_lang_o(&args[0]) {
            if let Expression::Const(FilterValue::String(tag)) = &args[1] {
                if let Some(lang_id) = store.resolve_lang_id(tag) {
                    let required_otype =
                        fluree_db_core::o_type::OType::lang_string(lang_id).as_u16();
                    out.push(EncodedPreFilter::LangEqualsOType { required_otype });
                    continue;
                }
            }
            pruned.push(op.clone());
            continue;
        }
        if is_lang_o(&args[1]) {
            if let Expression::Const(FilterValue::String(tag)) = &args[0] {
                if let Some(lang_id) = store.resolve_lang_id(tag) {
                    let required_otype =
                        fluree_db_core::o_type::OType::lang_string(lang_id).as_u16();
                    out.push(EncodedPreFilter::LangEqualsOType { required_otype });
                    continue;
                }
            }
            pruned.push(op.clone());
            continue;
        }

        // FILTER(?s = ?o) / FILTER(?s != ?o) (either side order)
        let (Some(sv), Some(ov)) = (subj_var, obj_var) else {
            pruned.push(op.clone());
            continue;
        };
        let is_s = |e: &Expression| matches!(e, Expression::Var(v) if *v == sv);
        let is_o = |e: &Expression| matches!(e, Expression::Var(v) if *v == ov);
        if !(is_s(&args[0]) && is_o(&args[1]) || is_o(&args[0]) && is_s(&args[1])) {
            pruned.push(op.clone());
            continue;
        }
        match func {
            Function::Eq => out.push(EncodedPreFilter::SubjectEqObjectRef),
            Function::Ne => out.push(EncodedPreFilter::SubjectNeObjectRef),
            _ => {
                pruned.push(op.clone());
            }
        }
    }
    (out, pruned)
}

fn string_literal_str_wrapper_safe(dtc: Option<&DatatypeConstraint>) -> bool {
    match dtc {
        Some(DatatypeConstraint::LangTag(_)) => true,
        Some(DatatypeConstraint::Explicit(dt)) => {
            (dt.namespace_code == namespaces::XSD && dt.name_str() == xsd_names::STRING)
                || (dt.namespace_code == namespaces::RDF && dt.name_str() == rdf_names::LANG_STRING)
        }
        None => false,
    }
}

fn object_prefix_input_matches(expr: &Expression, obj_var: VarId, allow_str_wrapper: bool) -> bool {
    match expr {
        Expression::Var(v) => *v == obj_var,
        Expression::Call { func, args }
            if allow_str_wrapper
                && *func == Function::Str
                && args.len() == 1
                && matches!(&args[0], Expression::Var(v) if *v == obj_var) =>
        {
            true
        }
        _ => false,
    }
}

fn extract_object_string_prefix(
    expr: &Expression,
    obj_var: Option<VarId>,
    pattern_dtc: Option<&DatatypeConstraint>,
) -> Option<Arc<str>> {
    use crate::ir::{FilterValue, Function};

    let ov = obj_var?;
    let allow_str_wrapper = string_literal_str_wrapper_safe(pattern_dtc);
    let is_object_input = |e: &Expression| object_prefix_input_matches(e, ov, allow_str_wrapper);

    match expr {
        Expression::Call { func, args } if *func == Function::StrStarts && args.len() == 2 => {
            if !is_object_input(&args[0]) {
                return None;
            }
            let Expression::Const(FilterValue::String(prefix)) = &args[1] else {
                return None;
            };
            (!prefix.is_empty()).then(|| Arc::from(prefix.as_str()))
        }
        Expression::Call { func, args } if *func == Function::Regex => {
            if args.len() != 2 && args.len() != 3 {
                return None;
            }
            if !is_object_input(&args[0]) {
                return None;
            }
            let Expression::Const(FilterValue::String(pattern)) = &args[1] else {
                return None;
            };
            if args.len() == 3 {
                let Expression::Const(FilterValue::String(flags)) = &args[2] else {
                    return None;
                };
                if !flags.is_empty() {
                    return None;
                }
            }
            anchored_literal_regex_prefix(pattern)
        }
        _ => None,
    }
}

pub(crate) fn preferred_index_hint_for_prefix_filters(
    pattern: &TriplePattern,
    inline_ops: &[InlineOperator],
) -> Option<IndexType> {
    if !pattern.p_bound() || pattern.s_bound() {
        return None;
    }
    let Term::Var(ov) = &pattern.o else {
        return None;
    };
    let obj_var = Some(*ov);

    inline_ops
        .iter()
        .any(|op| match op {
            InlineOperator::Filter(expr) => {
                extract_object_string_prefix(expr.expr(), obj_var, pattern.dtc.as_ref()).is_some()
            }
            InlineOperator::Bind { .. } => false,
        })
        .then_some(IndexType::Opst)
}

fn anchored_literal_regex_prefix(pattern: &str) -> Option<Arc<str>> {
    let prefix = pattern.strip_prefix('^')?;
    if prefix.is_empty() {
        return None;
    }
    if prefix.bytes().any(|b| {
        matches!(
            b,
            b'.' | b'+'
                | b'*'
                | b'?'
                | b'('
                | b')'
                | b'['
                | b']'
                | b'{'
                | b'}'
                | b'|'
                | b'\\'
                | b'^'
                | b'$'
        )
    }) {
        return None;
    }
    Some(Arc::from(prefix))
}

fn build_prefix_id_ranges(
    store: &BinaryIndexStore,
    prefix: &str,
) -> std::io::Result<Arc<[(u32, u32)]>> {
    let ids = store.find_strings_by_prefix(prefix)?;
    if ids.is_empty() {
        return Ok(Arc::from(Vec::<(u32, u32)>::new()));
    }
    let ranges = contiguous_id_range(&ids).map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok(Arc::from(ranges.into_boxed_slice()))
}

fn range_contains(ranges: &[(u32, u32)], value: u32) -> bool {
    ranges
        .binary_search_by(|(start, end)| {
            if value < *start {
                std::cmp::Ordering::Greater
            } else if value > *end {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

impl BinaryScanOperator {
    /// Create a new scan operator. The `store` and `g_id` are resolved from
    /// `ExecutionContext` during `open()`.
    pub fn new(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
    ) -> Self {
        Self::new_with_emit_and_index(pattern, object_bounds, inline_ops, EmitMask::ALL, None)
    }

    /// Create a scan operator with explicit emit mask and index hint.
    pub fn new_with_emit_and_index(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
        emit: EmitMask,
        index_hint: Option<IndexType>,
    ) -> Self {
        let s_bound = pattern.s_bound();
        let p_bound = pattern.p_bound();
        let o_bound = pattern.o_bound();
        let o_is_ref = pattern.o_is_ref();

        let mut index = IndexType::for_query(s_bound, p_bound, o_bound, o_is_ref);
        if object_bounds.is_some() && index == IndexType::Psot {
            index = IndexType::Post;
        }
        // Generic optimization: if the object is a constant (and can be safely encoded),
        // prefer the object-leading OPST index when the subject is unbound. This avoids
        // pathological scans like PSOT(p, *, o_const) that can't narrow by o_key.
        //
        // IMPORTANT: plain strings without a datatype constraint are ambiguous (xsd:string
        // vs rdf:langString). In that case we don't force OPST because we may be unable to
        // encode (o_type, o_key) during open(), and OPST would devolve into a wide scan.
        if index_hint.is_none()
            && object_bounds.is_none()
            && !s_bound
            && o_bound
            && (pattern.dtc.is_some() || !matches!(&pattern.o, Term::Value(FlakeValue::String(_))))
        {
            index = IndexType::Opst;
        }
        if let Some(hint) = index_hint {
            index = hint;
        }

        let (base_schema, s_var_pos, p_var_pos, o_var_pos) =
            schema_from_pattern_with_emit(&pattern, emit);
        let p_is_var = pattern.p.is_var();
        let schema: Arc<[VarId]> = extend_schema(&base_schema, &inline_ops).into();
        let (check_s_eq_o, check_s_eq_p, check_p_eq_o) = repeated_var_flags(&pattern);

        Self {
            pattern,
            index,
            schema,
            s_var_pos,
            p_var_pos,
            o_var_pos,
            state: OperatorState::Created,
            store: None,
            g_id: 0,
            cursor: None,
            p_sids: Vec::new(),
            sid_cache: HashMap::new(),
            p_is_var,
            inline_ops,
            encoded_pre_filters: Vec::new(),
            emit,
            index_hint,
            object_bounds,
            bound_o: None,
            check_s_eq_o,
            check_s_eq_p,
            check_p_eq_o,
            range_iter: None,
            unresolved_bound_subject_iri: None,
        }
    }

    /// Helper to get the store ref, panics if not yet set (before open).
    fn store(&self) -> &Arc<BinaryIndexStore> {
        self.store.as_ref().expect("store set in open()")
    }

    /// Base schema length (max var position + 1).
    fn base_schema_len(&self) -> usize {
        [self.s_var_pos, self.p_var_pos, self.o_var_pos]
            .into_iter()
            .flatten()
            .max()
            .map_or(0, |m| m + 1)
    }

    /// Convert collected columns into a Batch, handling empty-schema and exhaustion.
    fn finalize_columns(
        &mut self,
        columns: Vec<Vec<Binding>>,
        produced: usize,
    ) -> Result<Option<Batch>> {
        if produced == 0 {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        if self.schema.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(produced)));
        }
        Ok(Some(Batch::new(self.schema.clone(), columns)?))
    }

    /// Convert Flakes from the range_iter fallback into bindings, mirroring
    /// `batch_to_bindings` but for pre-decoded Flake values.
    fn flakes_to_bindings(
        &mut self,
        columns: &mut [Vec<Binding>],
        ctx: &ExecutionContext<'_>,
        batch_size: usize,
    ) -> Result<usize> {
        let base_len = self.base_schema_len();
        let num_vars = columns.len();
        let mut produced = 0;

        while produced < batch_size {
            let Some(flake) = self.range_iter.as_mut().and_then(std::iter::Iterator::next) else {
                break;
            };

            if let Some(target_iri) = self.unresolved_bound_subject_iri.as_ref() {
                let subject_iri = ctx
                    .active_snapshot
                    .decode_sid(&flake.s)
                    .unwrap_or_else(|| flake.s.to_string());
                if subject_iri != target_iri.as_ref() {
                    continue;
                }
            }

            // Repeated-variable checks.
            if self.check_s_eq_p && flake.s != flake.p {
                continue;
            }
            if self.check_s_eq_o {
                match &flake.o {
                    FlakeValue::Ref(o) if *o == flake.s => {}
                    _ => continue,
                }
            }
            if self.check_p_eq_o {
                match &flake.o {
                    FlakeValue::Ref(o) if *o == flake.p => {}
                    _ => continue,
                }
            }

            // Datatype / language constraint checks (range fallback path).
            if let Some(dtc) = &self.pattern.dtc {
                if !dt_compatible(dtc.datatype(), &flake.dt) {
                    continue;
                }
                if let Some(tag) = dtc.lang_tag() {
                    let flake_lang = flake.m.as_ref().and_then(|m| m.lang.as_ref());
                    if flake_lang.map(std::string::String::as_str) != Some(tag) {
                        continue;
                    }
                }
            }

            let mut bindings: Vec<Binding> = vec![Binding::Unbound; base_len];

            if let Some(pos) = self.s_var_pos.filter(|p| *p < base_len) {
                bindings[pos] = Binding::Sid(flake.s.clone());
            }
            if let Some(pos) = self.p_var_pos.filter(|p| *p < base_len) {
                bindings[pos] = Binding::Sid(flake.p.clone());
            }
            if let Some(pos) = self.o_var_pos.filter(|p| *p < base_len) {
                bindings[pos] = match &flake.o {
                    FlakeValue::Ref(r) => Binding::Sid(r.clone()),
                    v => {
                        let dtc = match flake
                            .m
                            .as_ref()
                            .and_then(|m| m.lang.as_ref())
                            .map(|s| Arc::<str>::from(s.as_str()))
                        {
                            Some(lang) => DatatypeConstraint::LangTag(lang),
                            None => DatatypeConstraint::Explicit(flake.dt.clone()),
                        };
                        Binding::Lit {
                            val: v.clone(),
                            dtc,
                            t: Some(flake.t),
                            op: if ctx.history_mode {
                                Some(flake.op)
                            } else {
                                None
                            },
                            p_id: None,
                        }
                    }
                };
            }

            if !apply_inline(&self.inline_ops, &self.schema, &mut bindings, Some(ctx))? {
                continue;
            }
            if bindings.len() < num_vars {
                bindings.resize(num_vars, Binding::Unbound);
            }

            for (col, binding) in columns.iter_mut().zip(bindings) {
                col.push(binding);
            }
            produced += 1;
        }

        Ok(produced)
    }

    async fn open_range_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let no_overlay = NoOverlay;
        let mut out: Vec<Flake> = Vec::new();

        // Single-graph range fallback. Multi-graph fanout is handled by
        // DatasetOperator which wraps this operator at all scan sites.
        let overlay: &dyn OverlayProvider = ctx.overlay.unwrap_or(&no_overlay);
        let match_val = build_match_val_for_snapshot(ctx, ctx.active_snapshot, &self.pattern)?;
        let opts = RangeOptions {
            to_t: Some(ctx.to_t),
            from_t: ctx.from_t,
            object_bounds: self.object_bounds.clone(),
            history_mode: ctx.history_mode,
            ..Default::default()
        };
        let mut flakes = range_with_overlay(
            ctx.active_snapshot,
            self.g_id,
            overlay,
            self.index,
            RangeTest::Eq,
            match_val,
            opts,
        )
        .await
        .map_err(|e| QueryError::Internal(format!("range_with_overlay: {e}")))?;

        // Apply policy filtering (including f:query) when present.
        flakes = Self::filter_flakes_by_policy(
            ctx,
            ctx.active_snapshot,
            overlay,
            ctx.to_t,
            self.g_id,
            flakes,
        )
        .await?;
        out.extend(flakes);

        self.range_iter = Some(out.into_iter());
        self.cursor = None;
        self.state = OperatorState::Open;
        Ok(())
    }

    /// Apply policy filtering to a batch of flakes for a specific graph.
    ///
    /// When a policy enforcer is present on the execution context, we:
    /// 1) Populate the class cache for subjects in this batch (required for f:onClass)
    /// 2) Filter flakes (async, supports f:query) using the graph's snapshot/overlay/to_t.
    async fn filter_flakes_by_policy(
        ctx: &ExecutionContext<'_>,
        snapshot: &LedgerSnapshot,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        g_id: GraphId,
        flakes: Vec<Flake>,
    ) -> Result<Vec<Flake>> {
        let Some(enforcer) = ctx.policy_enforcer.as_ref() else {
            return Ok(flakes);
        };
        if enforcer.is_root() || flakes.is_empty() {
            return Ok(flakes);
        }

        // Populate class cache for all subjects in this batch (deduped).
        let mut subjects: Vec<Sid> = flakes.iter().map(|f| f.s.clone()).collect();
        subjects.sort();
        subjects.dedup();
        let db = fluree_db_core::GraphDbRef::new(snapshot, g_id, overlay, to_t);
        enforcer
            .populate_class_cache_for_graph(db, &subjects)
            .await
            .map_err(|e| QueryError::Policy(e.to_string()))?;

        enforcer
            .filter_flakes_for_graph(snapshot, overlay, to_t, &ctx.tracker, flakes)
            .await
            .map_err(|e| QueryError::Policy(e.to_string()))
    }

    /// Extract bound terms from the pattern in the *snapshot* namespace space.
    ///
    /// Important invariants:
    /// - Novelty / overlay flakes carry `Sid`s in the snapshot's namespace-code space.
    /// - The binary index store carries its own namespace table and prefix trie (from the index root).
    ///
    /// Therefore, we keep the bound SIDs in snapshot space for overlay matching, and only
    /// translate into store space (via full IRI strings) when constructing persisted ID filters.
    fn extract_bound_terms_snapshot(
        snapshot: &LedgerSnapshot,
        pattern: &TriplePattern,
    ) -> (Option<Sid>, Option<Sid>, Option<FlakeValue>) {
        // Re-encode Sids through the snapshot so that uncompressed Sids
        // (e.g., Sid(0, "http://example.org/s")) are normalized to the
        // compressed form (Sid(ex_code, "s")) matching novelty flakes.
        let normalize = |sid: &Sid| -> Sid {
            // Fast path: already compressed in snapshot namespace space.
            // Avoid allocating a full IRI string just to feed it back into encode_iri().
            if sid.namespace_code != fluree_vocab::namespaces::EMPTY {
                return sid.clone();
            }

            // EMPTY namespace: treat name as a full IRI and try to compress.
            // If it doesn't match any known prefix, encode_iri will keep it in EMPTY.
            snapshot
                .encode_iri(sid.name.as_ref())
                .unwrap_or_else(|| sid.clone())
        };
        let s_sid = match &pattern.s {
            Ref::Sid(s) => Some(normalize(s)),
            Ref::Iri(iri) => snapshot.encode_iri(iri),
            _ => None,
        };
        let p_sid = match &pattern.p {
            Ref::Sid(p) => Some(normalize(p)),
            Ref::Iri(iri) => snapshot.encode_iri(iri),
            _ => None,
        };
        let o_val = match &pattern.o {
            Term::Sid(sid) => Some(FlakeValue::Ref(normalize(sid))),
            Term::Iri(iri) => snapshot.encode_iri(iri).map(FlakeValue::Ref),
            Term::Value(v) => Some(v.clone()),
            Term::Var(_) => None,
        };
        (s_sid, p_sid, o_val)
    }

    /// Build a `BinaryFilter` from bound pattern terms.
    fn build_filter_from_snapshot_sids(
        snapshot: &LedgerSnapshot,
        pattern: &TriplePattern,
        store: &BinaryIndexStore,
        s_sid: &Option<Sid>,
        p_sid: &Option<Sid>,
    ) -> std::io::Result<BinaryFilter> {
        let s_id = match (&pattern.s, s_sid.as_ref()) {
            (Ref::Iri(iri), _) => store.find_subject_id(iri)?,
            (Ref::Sid(query_sid), Some(sid)) => {
                if let Some(s_id) = sid_iri::sid_to_store_s_id(store, sid)? {
                    Some(s_id)
                } else if let Some(iri) = snapshot.decode_sid(query_sid) {
                    store.find_subject_id(&iri)?
                } else {
                    None
                }
            }
            _ => None,
        };
        let p_id = match p_sid.as_ref() {
            Some(sid) => sid_iri::sid_to_store_p_id(store, sid),
            None => None,
        };

        Ok(BinaryFilter {
            s_id,
            p_id,
            o_type: None,
            o_key: None,
            o_i: None,
        })
    }

    /// Resolve s_id → Sid with caching.
    fn resolve_s_id(&mut self, s_id: u64) -> Result<Sid> {
        if let Some(sid) = self.sid_cache.get(&s_id) {
            return Ok(sid.clone());
        }
        let iri = self
            .store()
            .resolve_subject_iri(s_id)
            .map_err(|e| QueryError::Internal(format!("resolve s_id={s_id}: {e}")))?;
        let sid = self
            .store()
            .find_subject_sid(&iri)
            .map_err(|e| QueryError::Internal(format!("find_subject_sid: {e}")))?
            .unwrap_or_else(|| self.store().encode_iri(&iri));
        self.sid_cache.insert(s_id, sid.clone());
        Ok(sid)
    }

    /// Resolve p_id → Sid (pre-computed, O(1)).
    #[inline]
    fn resolve_p_id(&self, p_id: u32) -> Sid {
        self.p_sids
            .get(p_id as usize)
            .cloned()
            .unwrap_or_else(|| Sid::new(0, ""))
    }

    /// Filter: skip internal db: predicates when predicate is a variable.
    #[inline]
    fn is_internal_predicate(&self, p_id: u32) -> bool {
        if !self.p_is_var || self.g_id != 0 {
            return false;
        }
        self.p_sids
            .get(p_id as usize)
            .is_some_and(|s| s.namespace_code == fluree_vocab::namespaces::FLUREE_DB)
    }

    /// Enforce within-pattern repeated-variable constraints.
    ///
    /// SPARQL allows the same variable to appear in multiple positions of a triple pattern,
    /// e.g. `?x <p> ?x` or `?x ?x ?o`. These are equality constraints that must be applied
    /// even when emission pruning omits one of the positions.
    fn within_row_var_equality_ok(
        &mut self,
        s_id: u64,
        p_id: u32,
        o_type: u16,
        o_key: u64,
    ) -> Result<bool> {
        // Fast path: no repeated variables in this pattern (the common case).
        if !self.check_s_eq_o && !self.check_s_eq_p && !self.check_p_eq_o {
            return Ok(true);
        }

        if self.check_s_eq_o {
            // s==o only possible if o is a ref pointing at s_id.
            let ot = fluree_db_core::o_type::OType::from_u16(o_type);
            if !(ot.is_iri_ref() || ot.is_blank_node()) {
                return Ok(false);
            }
            if o_key != s_id {
                return Ok(false);
            }
        }

        // Comparisons involving predicate IDs require Sid materialization (different ID domains).
        if self.check_s_eq_p {
            let s = self.resolve_s_id(s_id)?;
            let p = self.resolve_p_id(p_id);
            if s != p {
                return Ok(false);
            }
        }
        if self.check_p_eq_o {
            let ot = fluree_db_core::o_type::OType::from_u16(o_type);
            if !(ot.is_iri_ref() || ot.is_blank_node()) {
                return Ok(false);
            }
            let o_sid = self.resolve_s_id(o_key)?;
            let p_sid = self.resolve_p_id(p_id);
            if o_sid != p_sid {
                return Ok(false);
            }
        }

        Ok(true)
    }

    #[inline]
    fn set_binding_at(slots: &mut [Binding], pos: usize, b: Binding) -> bool {
        match &slots[pos] {
            Binding::Unbound => {
                slots[pos] = b;
                true
            }
            existing => existing == &b,
        }
    }

    /// Check whether this row matches the triple pattern's datatype constraint (if any).
    #[inline]
    fn matches_datatype_constraint(&self, o_type: u16) -> bool {
        let Some(dtc) = &self.pattern.dtc else {
            return true;
        };

        let Some(dt_sid) = self.store().resolve_datatype_sid(o_type) else {
            return false;
        };
        if !dt_compatible(dtc.datatype(), &dt_sid) {
            return false;
        }

        if let Some(tag) = dtc.lang_tag() {
            self.store().resolve_lang_tag(o_type) == Some(tag)
        } else {
            true
        }
    }

    /// Convert a ColumnBatch into columnar Bindings.
    fn batch_to_bindings(
        &mut self,
        batch: &ColumnBatch,
        columns: &mut [Vec<Binding>],
        ctx: Option<&ExecutionContext<'_>>,
    ) -> Result<usize> {
        let mut produced = 0;
        let ncols = self.schema.len();
        let base_len = self.base_schema_len();
        let mut bindings = Vec::with_capacity(ncols.max(base_len));
        let store_arc: Arc<BinaryIndexStore> = Arc::clone(self.store());
        let dict_novelty_arc = ctx.and_then(|c| c.dict_novelty.clone());
        let view = {
            let mut v = BinaryGraphView::with_novelty(
                Arc::clone(&store_arc),
                self.g_id,
                dict_novelty_arc.clone(),
            )
            .with_namespace_codes_fallback(ctx.and_then(|c| c.namespace_codes_fallback.clone()));
            if let Some(c) = ctx {
                v = v.with_tracker(c.tracker.clone());
            }
            v
        };
        // DictOverlay is no longer needed here for decoding — BinaryGraphView
        // handles watermark routing internally. DictOverlay is still used for
        // overlay translation (translate_overlay_flakes) in BinaryScanOperator::open.

        // Late materialization is safe only when the BinaryIndexStore is authoritative
        // for decoding (no novelty overlay with ephemeral IDs).
        //
        // Note: ExecutionContext always carries an overlay provider; `NoOverlay` has epoch=0.
        // When `eager_materialization` is set (via `GraphDbRef::eager()`), always resolve
        // bindings eagerly — infrastructure queries (config, policy) need concrete
        // `Binding::Sid`/`Lit`, not `EncodedSid`/`EncodedLit`.
        let late_materialize = ctx.is_some_and(|c| {
            c.overlay.map(fluree_db_core::OverlayProvider::epoch).unwrap_or(0) == 0 && !c.eager_materialization
        })
            // If a repeated variable forces two components into the same output slot,
            // late-materialization must produce comparable binding representations.
            // In particular, `?x ?x ?o` would otherwise compare EncodedSid vs EncodedPid.
            && !self.check_s_eq_p
            && !self.check_p_eq_o;

        for row in 0..batch.row_count {
            let s_id = batch.s_id.get(row);
            let p_id = batch.p_id.get_or(row, 0);
            let o_type = batch.o_type.get_or(row, 0);
            let o_key = batch.o_key.get(row);
            let o_i = batch.o_i.get_or(row, u32::MAX);
            let t_opt = if batch.t.is_absent() {
                None
            } else {
                Some(batch.t.get(row) as i64)
            };
            let t_enc = t_opt.unwrap_or(0);

            // Skip internal db: predicates on wildcard scans.
            if self.is_internal_predicate(p_id) {
                continue;
            }

            // Encoded pre-filters: run before any decoding work.
            if !self
                .encoded_pre_filters
                .iter()
                .all(|f| f.eval_row(s_id, o_type, o_key))
            {
                continue;
            }

            if !self.within_row_var_equality_ok(s_id, p_id, o_type, o_key)? {
                continue;
            }

            // Enforce datatype constraints before decoding into bindings.
            if !self.matches_datatype_constraint(o_type) {
                continue;
            }

            if let Some(target_iri) = self.unresolved_bound_subject_iri.as_ref() {
                let subject_iri = view
                    .resolve_subject_iri(s_id)
                    .map_err(|e| QueryError::from_io("resolve_subject_iri", e))?;
                if subject_iri != target_iri.as_ref() {
                    continue;
                }
            }

            // Decode object when needed:
            // - object is bound (must filter)
            // - object bounds are present (must filter)
            // - object is emitted but late-materialization is disabled (e.g., overlay)
            let needs_o_decode = self.bound_o.is_some()
                || self.object_bounds.is_some()
                || (!late_materialize && self.o_var_pos.is_some());
            // BinaryGraphView::decode_value is novelty-aware: dict-backed types
            // (IriRef, StringDict, JsonArena) automatically route through
            // watermark checks when dict_novelty is present.
            let decode_value = |o_type: u16, o_key: u64, p_id: u32| -> Result<FlakeValue> {
                view.decode_value(o_type, o_key, p_id)
                    .map_err(|e| QueryError::from_io("decode_value", e))
            };

            let decoded_o = if needs_o_decode {
                Some(decode_value(o_type, o_key, p_id)?)
            } else {
                None
            };

            if let Some(bound) = &self.bound_o {
                let Some(val) = decoded_o.as_ref() else {
                    return Err(QueryError::Internal(
                        "bound object requires object decoding".to_string(),
                    ));
                };
                if val != bound {
                    continue;
                }
            }

            if let Some(bounds) = &self.object_bounds {
                let Some(val) = decoded_o.as_ref() else {
                    return Err(QueryError::Internal(
                        "object bounds require object decoding".to_string(),
                    ));
                };
                if !bounds.matches(val) {
                    continue;
                }
            }

            bindings.clear();
            bindings.resize(base_len, Binding::Unbound);

            // Subject binding.
            if let Some(pos) = self.s_var_pos {
                let binding = if late_materialize {
                    Binding::EncodedSid { s_id }
                } else {
                    // BinaryGraphView::resolve_subject_sid is novelty-aware:
                    // novel subjects return Sid directly without IRI round-trip.
                    let sid = view
                        .resolve_subject_sid(s_id)
                        .map_err(|e| QueryError::from_io("resolve_subject_sid", e))?;
                    Binding::Sid(sid)
                };
                if !Self::set_binding_at(&mut bindings, pos, binding) {
                    continue;
                }
            }

            // Predicate binding.
            if let Some(pos) = self.p_var_pos {
                let binding = if late_materialize {
                    Binding::EncodedPid { p_id }
                } else {
                    Binding::Sid(self.resolve_p_id(p_id))
                };
                if !Self::set_binding_at(&mut bindings, pos, binding) {
                    continue;
                }
            }

            // Object binding.
            if let Some(pos) = self.o_var_pos {
                let binding = if needs_o_decode || !late_materialize {
                    let val = decoded_o.expect("decoded object required");
                    materialized_object_binding(self.store(), o_type, p_id, val, t_opt)
                } else if let Some(encoded) =
                    late_materialized_object_binding(o_type, o_key, p_id, t_enc, o_i)
                {
                    encoded
                } else {
                    // Fallback: decode if we don't have a safe encoded representation.
                    // This preserves correctness for uncommon/custom OTypes.
                    match decode_value(o_type, o_key, p_id) {
                        Ok(val) => {
                            materialized_object_binding(self.store(), o_type, p_id, val, t_opt)
                        }
                        Err(e) => {
                            return Err(QueryError::dictionary_lookup(format!(
                                "binary scan object decode fallback failed: o_type={o_type}, o_key={o_key}, p_id={p_id}: {e}"
                            )));
                        }
                    }
                };
                if !Self::set_binding_at(&mut bindings, pos, binding) {
                    continue;
                }
            }

            // Apply inline operators.
            if !apply_inline(&self.inline_ops, &self.schema, &mut bindings, ctx)? {
                continue;
            }

            // Push to columns.
            for (i, binding) in bindings.drain(..).enumerate() {
                columns[i].push(binding);
            }
            produced += 1;
        }

        Ok(produced)
    }
}

impl BinaryScanOperator {
    async fn open_overlay_only_fallback(
        &mut self,
        ctx: &ExecutionContext<'_>,
        s_sid: &Option<Sid>,
        p_sid: &Option<Sid>,
    ) -> Result<()> {
        let Some(overlay) = ctx.overlay else {
            self.range_iter = Some(Vec::<Flake>::new().into_iter());
            self.cursor = None;
            self.state = OperatorState::Open;
            return Ok(());
        };

        let to_t = ctx.to_t;
        let from_t = ctx.from_t;
        let cmp = self.index.comparator();

        // Collect all overlay flakes for this graph+index (novelty is expected to be small),
        // then narrow by equality match.
        let mut flakes: Vec<Flake> = Vec::new();
        overlay.for_each_overlay_flake(self.g_id, self.index, None, None, true, to_t, &mut |f| {
            if f.t <= to_t && from_t.is_none_or(|ft| f.t >= ft) {
                flakes.push(f.clone());
            }
        });

        flakes.sort_by(cmp);
        flakes = resolve_overlay_retractions(flakes);

        // Apply equality match (subject/predicate/object).
        if s_sid.is_some() || p_sid.is_some() || self.bound_o.is_some() {
            flakes.retain(|f| {
                if let Some(s) = s_sid.as_ref() {
                    if &f.s != s {
                        return false;
                    }
                }
                if let Some(p) = p_sid.as_ref() {
                    if &f.p != p {
                        return false;
                    }
                }
                if let Some(o) = self.bound_o.as_ref() {
                    if &f.o != o {
                        return false;
                    }
                }
                true
            });
        }

        // Apply object bounds (post-filter) when present.
        if let Some(bounds) = self.object_bounds.as_ref() {
            flakes.retain(|f| bounds.matches(&f.o));
        }

        self.range_iter = Some(flakes.into_iter());
        self.cursor = None;
        self.state = OperatorState::Open;
        Ok(())
    }
}

/// Resolve assert/retract pairs in overlay flakes.
///
/// For each distinct fact `(s, p, o, dt, m)`, the latest entry (highest `t`)
/// determines the current state: if it's an assertion, the fact is kept;
/// if it's a retraction, the fact is excluded from query results.
///
/// Novelty enforces RDF set semantics at write time (`apply_commit`), so
/// duplicate assertions for the same fact cannot exist. This function only
/// needs to resolve assert/retract lifecycles, not deduplicate.
fn resolve_overlay_retractions(flakes: Vec<Flake>) -> Vec<Flake> {
    use std::collections::HashSet;

    // Full fact identity includes metadata (lang tags, list indices).
    // Two flakes with same (s, p, o, dt) but different `m` are distinct facts.
    #[derive(Clone, Copy, Hash, PartialEq, Eq)]
    struct FactKeyRef<'a> {
        s: &'a Sid,
        p: &'a Sid,
        o: &'a FlakeValue,
        dt: &'a Sid,
        m: &'a Option<FlakeMeta>,
    }

    let mut seen: HashSet<FactKeyRef<'_>> = HashSet::new();
    let mut keep = vec![false; flakes.len()];

    // Walk in reverse (highest t first). First occurrence per fact key is
    // the latest state. Keep it only if it's an assertion.
    for (idx, f) in flakes.iter().enumerate().rev() {
        let key = FactKeyRef {
            s: &f.s,
            p: &f.p,
            o: &f.o,
            dt: &f.dt,
            m: &f.m,
        };
        if !seen.insert(key) {
            continue;
        }
        if f.op {
            keep[idx] = true;
        }
    }

    flakes
        .into_iter()
        .zip(keep)
        .filter_map(|(f, k)| k.then_some(f))
        .collect()
}

fn build_match_val_for_snapshot(
    ctx: &ExecutionContext<'_>,
    snapshot: &fluree_db_core::LedgerSnapshot,
    pattern: &TriplePattern,
) -> Result<RangeMatch> {
    let mut match_val = RangeMatch::new();

    let reencode_iri_subject = |iri: &Arc<str>| -> Result<Option<Sid>> {
        if let Some(store) = ctx.binary_store.as_deref() {
            if let Some(sid) = store
                .find_subject_sid(iri.as_ref())
                .map_err(|e| QueryError::Internal(format!("find_subject_sid: {e}")))?
            {
                return Ok(Some(sid));
            }
        }
        Ok(snapshot.encode_iri(iri))
    };

    let reencode_sid = |sid: &Sid| -> Option<Sid> {
        // Pattern SIDs are encoded in the primary snapshot's namespace space.
        // Decode to canonical IRI and re-encode into the target snapshot.
        // Use `original_snapshot` (the primary) rather than `snapshot`
        // (which may be a per-graph snapshot with different namespace codes).
        if let Some(iri) = ctx.original_snapshot.decode_sid(sid) {
            if let Some(store) = ctx.binary_store.as_deref() {
                if let Ok(Some(persisted_sid)) = store.find_subject_sid(&iri) {
                    return Some(persisted_sid);
                }
            }
            snapshot.encode_iri(&iri)
        } else {
            // If the SID can't be decoded (namespace code missing), preserve the
            // raw SID. This is important when the namespace table has been
            // extended in novelty but the snapshot's namespace map is not yet
            // able to decode the SID. Range scans can still match by raw SID.
            Some(sid.clone())
        }
    };

    match &pattern.s {
        Ref::Sid(s) => match_val.s = reencode_sid(s),
        Ref::Var(_) => {}
        Ref::Iri(iri) => match_val.s = reencode_iri_subject(iri)?,
    }

    match &pattern.p {
        Ref::Sid(p) => match_val.p = reencode_sid(p),
        Ref::Var(_) => {}
        Ref::Iri(iri) => match_val.p = snapshot.encode_iri(iri),
    }

    match &pattern.o {
        Term::Sid(o) => match_val.o = reencode_sid(o).map(FlakeValue::Ref),
        Term::Value(v) => match_val.o = Some(v.clone()),
        Term::Var(_) => {}
        Term::Iri(iri) => match_val.o = reencode_iri_subject(iri)?.map(FlakeValue::Ref),
    }

    Ok(match_val)
}

/// Pre-compute which repeated-variable equality checks are needed for a pattern.
///
/// Returns `(s==o, s==p, p==o)` flags.
fn repeated_var_flags(pattern: &TriplePattern) -> (bool, bool, bool) {
    let s_var = match &pattern.s {
        Ref::Var(v) => Some(*v),
        _ => None,
    };
    let p_var = match &pattern.p {
        Ref::Var(v) => Some(*v),
        _ => None,
    };
    let o_var = match &pattern.o {
        Term::Var(v) => Some(*v),
        _ => None,
    };
    (
        s_var.is_some_and(|v| o_var == Some(v)),
        s_var.is_some_and(|v| p_var == Some(v)),
        p_var.is_some_and(|v| o_var == Some(v)),
    )
}

#[async_trait]
impl Operator for BinaryScanOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        // Resolve store and g_id from context.
        self.store = ctx.binary_store.clone();
        self.g_id = ctx.binary_g_id;

        // Multi-graph fanout is handled by DatasetOperator, which wraps
        // BinaryScanOperator at the scan construction sites (where_plan.rs,
        // join.rs, etc.). By the time open() is called here, the context is
        // always single-graph.

        if self.store.is_none() {
            return self.open_range_fallback(ctx).await;
        }
        // Policy enforcement requires async per-flake checks (including f:query)
        // and class-cache population. The binary cursor path currently does not
        // apply policy filtering, so force the range fallback when a non-root
        // policy enforcer is present.
        if ctx.policy_enforcer.as_ref().is_some_and(|p| !p.is_root()) {
            return self.open_range_fallback(ctx).await;
        }

        // Pre-compute p_id → Sid table.
        let mut p_sids = Vec::new();
        let store = self.store.as_ref().ok_or_else(|| {
            QueryError::Internal(
                "BinaryScanOperator::open: no binary_store on ExecutionContext".into(),
            )
        })?;
        let store_ref = store.as_ref();
        for p_id in 0u32.. {
            match store_ref.resolve_predicate_iri(p_id) {
                Some(iri) => p_sids.push(store_ref.encode_iri(iri)),
                None => break,
            }
        }
        self.p_sids = p_sids;

        // Extract bound terms in snapshot namespace space and build the persisted-ID filter
        // by translating through full IRIs into store namespace space.
        let (s_sid, p_sid, o_val) =
            Self::extract_bound_terms_snapshot(ctx.active_snapshot, &self.pattern);
        self.bound_o = o_val;
        let mut filter = Self::build_filter_from_snapshot_sids(
            ctx.active_snapshot,
            &self.pattern,
            store_ref,
            &s_sid,
            &p_sid,
        )
        .map_err(|e| QueryError::Internal(format!("build_filter: {e}")))?;
        tracing::debug!(
            ?self.pattern,
            s_bound = s_sid.is_some(),
            p_bound = p_sid.is_some(),
            ?filter.s_id,
            ?filter.p_id,
            "BinaryScanOperator::open"
        );

        self.unresolved_bound_subject_iri = if s_sid.is_some() && filter.s_id.is_none() {
            match &self.pattern.s {
                Ref::Iri(iri) => Some(Arc::clone(iri)),
                Ref::Sid(sid) => ctx.original_snapshot.decode_sid(sid).map(Arc::from),
                Ref::Var(_) => None,
            }
        } else {
            None
        };

        // Bound-object fast path: if the triple pattern has a constant object, encode it into
        // (o_type, o_key) so the cursor can seek directly to the relevant leaf range.
        //
        // This is safe for graph pattern matching (RDF term equality is type-aware), and avoids
        // pathological full-predicate scans like `?paper <publishedIn> "SIGIR"`.
        //
        // For overlay/novelty queries, we keep this conservative: if the value isn't present in
        // the persisted dictionaries, fall back to overlay-only to avoid a wide base scan.
        if let Some(bound_o) = self.bound_o.as_ref() {
            let dtc = self.pattern.dtc.as_ref();
            let lang = dtc.and_then(|d| d.lang_tag());
            let dt_sid = dtc.map(fluree_db_core::DatatypeConstraint::datatype);
            let dict_novelty = ctx.dict_novelty.as_ref();
            let stats_view = cached_stats_view_for_db(
                fluree_db_core::GraphDbRef::new(
                    ctx.active_snapshot,
                    self.g_id,
                    ctx.overlay(),
                    ctx.to_t,
                )
                .with_runtime_small_dicts_opt(ctx.runtime_small_dicts),
                self.store.as_ref(),
            );
            let inferred_dt_sid = if dt_sid.is_none() && lang.is_none() {
                filter.p_id.and_then(|p_id| {
                    infer_exact_datatype_sid_from_stats(
                        stats_view.as_deref(),
                        self.g_id,
                        RuntimePredicateId::from_u32(p_id),
                        bound_o,
                    )
                })
            } else {
                None
            };

            let encoded = match (dt_sid.or(inferred_dt_sid.as_ref()), lang) {
                (Some(dt_sid), lang) => {
                    value_to_otype_okey(bound_o, dt_sid, lang, store_ref, dict_novelty)
                }
                (None, None) => {
                    // Without a datatype constraint, we can only safely encode non-string
                    // values. String values are ambiguous — could be xsd:string or
                    // rdf:langString — so skip them to avoid type mismatch.
                    match bound_o {
                        FlakeValue::String(_) => Err(std::io::Error::other(
                            "string without dtc: type ambiguous (could be langString)",
                        )),
                        _ => value_to_otype_okey_simple(bound_o, store_ref)
                            .map_err(|e| std::io::Error::other(e.to_string())),
                    }
                }
                (None, Some(_lang)) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "lang tag requires datatype constraint",
                )),
            };

            match encoded {
                Ok((ot, key)) => {
                    filter.o_type = Some(ot.as_u16());
                    filter.o_key = Some(key);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Novelty may contain the value, but base index can't; avoid wide base scan.
                    return self.open_overlay_only_fallback(ctx, &s_sid, &p_sid).await;
                }
                Err(_) => {
                    // If encoding fails, keep correctness by leaving the filter un-narrowed.
                }
            }
        }

        // If a subject/predicate is bound but not present in the binary dictionaries
        // (common when querying freshly-inserted novelty before the next index build),
        // a binary scan would devolve into a full index scan because the filter can't
        // constrain by IDs.
        //
        // IMPORTANT: RangeProvider-based scans also require dictionary IDs to constrain
        // base index scans. If a SID isn't in the base dictionaries, a range scan can
        // still devolve into a wide base scan. In this case we want an **overlay-only**
        // fallback to return only novelty matches.
        if p_sid.is_some() && filter.p_id.is_none() {
            return self.open_overlay_only_fallback(ctx, &s_sid, &p_sid).await;
        }
        if s_sid.is_some() && filter.s_id.is_none() && self.unresolved_bound_subject_iri.is_none() {
            return self.open_overlay_only_fallback(ctx, &s_sid, &p_sid).await;
        }
        if self.unresolved_bound_subject_iri.is_some() && filter.p_id.is_some() {
            self.index = IndexType::Psot;
        }

        let order = index_type_to_sort_order(self.index);
        // Decode only columns needed for correctness:
        // - `o_i` is part of the V3 identity model (list semantics)
        // - `t` is only required for history mode
        let mut output = ColumnSet::CORE;
        output.insert(ColumnId::OI);
        if ctx.history_mode || inline_ops_need_t(&self.inline_ops) {
            output.insert(ColumnId::T);
        }
        let projection = ColumnProjection {
            output,
            internal: ColumnSet::EMPTY,
        };

        // Get branch manifest (clone into Arc for cursor ownership).
        let store_arc = Arc::clone(self.store.as_ref().expect("store set above"));
        let store_ref = store_arc.as_ref();
        let Some(branch_ref) = store_ref.branch_for_order(self.g_id, order) else {
            // The index root may omit graphs (or specific sort orders) that have
            // zero indexed rows. Queries over such graphs should return empty
            // results (or overlay-only results when novelty is present), not
            // fail with an internal error.
            return self.open_overlay_only_fallback(ctx, &s_sid, &p_sid).await;
        };
        let branch: Arc<fluree_db_binary_index::format::branch::BranchManifest> =
            Arc::clone(branch_ref);

        // If this scan has range bounds on the object variable and we're scanning in POST order,
        // narrow the cursor's leaf range by object-key range.
        //
        // IMPORTANT: SPARQL numeric comparisons are cross-type (integer bounds match double
        // values), and ObjKey encodings differ between types. For correctness, we only apply
        // range narrowing for temporal types where cross-type comparison does not apply.
        let mut range_min_okey: Option<u64> = None;
        let mut range_max_okey: Option<u64> = None;
        let mut range_o_type: Option<u16> = None;
        if order == RunSortOrder::Post && filter.p_id.is_some() && self.bound_o.is_none() {
            if let Some(bounds) = self.object_bounds.as_ref() {
                let supports_range = |ot: OType| -> bool {
                    matches!(
                        ot,
                        OType::XSD_DATE
                            | OType::XSD_DATE_TIME
                            | OType::XSD_TIME
                            | OType::XSD_G_YEAR
                            | OType::XSD_G_YEAR_MONTH
                            | OType::XSD_G_MONTH
                            | OType::XSD_G_DAY
                            | OType::XSD_G_MONTH_DAY
                    )
                };

                let encode = |v: &FlakeValue| -> Option<(u16, u64)> {
                    let (ot, key) = value_to_otype_okey_simple(v, store_ref).ok()?;
                    supports_range(ot).then_some((ot.as_u16(), key))
                };

                let mut ot: Option<u16> = None;
                if let Some((v, _inclusive)) = bounds.lower.as_ref() {
                    if let Some((o_type, key)) = encode(v) {
                        ot = Some(o_type);
                        range_min_okey = Some(key);
                    }
                }
                if let Some((v, _inclusive)) = bounds.upper.as_ref() {
                    if let Some((o_type, key)) = encode(v) {
                        if ot.is_some() && ot != Some(o_type) {
                            // Mixed type bounds; don't attempt range narrowing.
                            ot = None;
                            range_min_okey = None;
                            range_max_okey = None;
                        } else {
                            ot = Some(o_type);
                            range_max_okey = Some(key);
                        }
                    }
                }

                if let Some(o_type) = ot {
                    range_o_type = Some(o_type);
                    // Also set the filter o_type so directory-level pre-skip can eliminate non-matching leaflets.
                    filter.o_type = Some(o_type);
                }
            }
        }

        // Create cursor. If any of (s_id, p_id, o_type, o_key) are bound OR we have a
        // temporal object-key range (POST + bounds), construct a narrow min/max key range
        // so we can seek into the branch manifest rather than scanning all leaves.
        let use_range = filter.s_id.is_some()
            || filter.p_id.is_some()
            || filter.o_type.is_some()
            || filter.o_key.is_some()
            || range_min_okey.is_some()
            || range_max_okey.is_some();

        let mut cursor = if use_range {
            let min_key = RunRecordV2 {
                s_id: SubjectId(filter.s_id.unwrap_or(0)),
                o_key: filter.o_key.or(range_min_okey).unwrap_or(0),
                p_id: filter.p_id.unwrap_or(0),
                t: 0,
                o_i: 0,
                o_type: filter.o_type.or(range_o_type).unwrap_or(0),
                g_id: self.g_id,
            };
            let max_key = RunRecordV2 {
                s_id: SubjectId(filter.s_id.unwrap_or(u64::MAX)),
                o_key: filter.o_key.or(range_max_okey).unwrap_or(u64::MAX),
                p_id: filter.p_id.unwrap_or(u32::MAX),
                t: u32::MAX,
                o_i: u32::MAX,
                o_type: filter.o_type.or(range_o_type).unwrap_or(u16::MAX),
                g_id: self.g_id,
            };
            BinaryCursor::new(
                Arc::clone(&store_arc),
                order,
                branch,
                &min_key,
                &max_key,
                filter,
                projection,
            )
            .with_tracker(ctx.tracker.clone())
        } else {
            BinaryCursor::scan_all(Arc::clone(&store_arc), order, branch, filter, projection)
                .with_tracker(ctx.tracker.clone())
        };

        // Overlay: translate novelty flakes to OverlayOp and attach to cursor.
        if ctx.overlay.is_some() {
            let (mut ops, mut untranslated, ephemeral_preds) =
                translate_overlay_flakes_with_untranslated(
                    ctx.overlay(),
                    store_ref,
                    ctx.dict_novelty.as_ref(),
                    ctx.runtime_small_dicts,
                    ctx.to_t,
                    self.g_id,
                );

            // Extend p_sids table with novelty-only predicates so that ephemeral
            // p_ids from overlay ops can be decoded back to Sids during row binding.
            for (sid, ep_id) in &ephemeral_preds {
                let idx = *ep_id as usize;
                if idx >= self.p_sids.len() {
                    self.p_sids.resize(idx + 1, Sid::new(0, ""));
                }
                self.p_sids[idx] = sid.clone();
            }

            if !ops.is_empty() {
                sort_overlay_ops(&mut ops, order);
                resolve_overlay_ops(&mut ops);
                let epoch = ctx.overlay().epoch();
                cursor.set_overlay_ops(ops);
                cursor.set_epoch(epoch);
            }

            // Some overlay flakes cannot be represented in V3 overlay ops (e.g., @vector).
            // Keep them as materialized flakes and stream them after the cursor completes.
            if !untranslated.is_empty() {
                let cmp = self.index.comparator();
                untranslated.sort_by(cmp);
                untranslated = resolve_overlay_retractions(untranslated);

                // Apply equality match (subject/predicate/object) against pattern constants.
                let s_sid = match &self.pattern.s {
                    Ref::Sid(s) => Some(s.clone()),
                    _ => None,
                };
                let p_sid = match &self.pattern.p {
                    Ref::Sid(p) => Some(p.clone()),
                    _ => None,
                };

                if s_sid.is_some() || p_sid.is_some() || self.bound_o.is_some() {
                    untranslated.retain(|f| {
                        if let Some(s) = s_sid.as_ref() {
                            if &f.s != s {
                                return false;
                            }
                        }
                        if let Some(p) = p_sid.as_ref() {
                            if &f.p != p {
                                return false;
                            }
                        }
                        if let Some(o) = self.bound_o.as_ref() {
                            if &f.o != o {
                                return false;
                            }
                        }
                        true
                    });
                }

                if let Some(bounds) = self.object_bounds.as_ref() {
                    untranslated.retain(|f| bounds.matches(&f.o));
                }

                if !untranslated.is_empty() {
                    self.range_iter = Some(untranslated.into_iter());
                }
            }
        }
        cursor.set_to_t(ctx.to_t);

        self.cursor = Some(cursor);
        self.state = OperatorState::Open;

        // Compile pre-filters that can run on encoded columns (no decoding).
        let (encoded, pruned) = compile_encoded_pre_filters_and_prune_inline_ops(
            &self.inline_ops,
            &self.pattern,
            store_ref,
            ctx.overlay
                .map(fluree_db_core::OverlayProvider::epoch)
                .unwrap_or(0)
                == 0,
        );
        self.encoded_pre_filters = encoded;
        self.inline_ops = pruned;

        tracing::debug!(
            index = ?self.index,
            order = ?order,
            g_id = self.g_id,
            pattern = ?self.pattern,
            "BinaryScanOperator::open"
        );

        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        let batch_size = ctx.batch_size;
        let num_vars = self.schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_vars)
            .map(|_| Vec::with_capacity(batch_size))
            .collect();

        let mut produced = 0usize;

        // Prefer binary cursor (indexed data), then drain any overlay-only fallback flakes.
        while produced < batch_size {
            let Some(cursor) = &mut self.cursor else {
                break;
            };

            match cursor.next_batch() {
                Ok(Some(batch)) => {
                    // Per-leaflet fuel charge happens inside cursor.next_batch.
                    let n = self.batch_to_bindings(&batch, &mut columns, Some(ctx))?;
                    produced += n;
                }
                Ok(None) => {
                    // Cursor exhausted — drop it so we can proceed to `range_iter`.
                    self.cursor = None;
                    break;
                }
                Err(e) => {
                    return Err(QueryError::from_io("V3 cursor", e));
                }
            }
        }

        if produced < batch_size && self.range_iter.is_some() {
            // Overlay/novelty rows are in-memory; charge per row at 1 micro-fuel.
            let n = self.flakes_to_bindings(&mut columns, ctx, batch_size - produced)?;
            for _ in 0..n {
                ctx.tracker.consume_fuel(1)?;
            }
            produced += n;
        }

        self.finalize_columns(columns, produced)
    }

    fn close(&mut self) {
        self.cursor = None;
        self.range_iter = None;
        self.store = None;
        self.sid_cache.clear();
        self.p_sids.clear();
        self.unresolved_bound_subject_iri = None;
        self.state = OperatorState::Closed;
    }
}

// ============================================================================
// Overlay translation: Flake → OverlayOp
// ============================================================================

/// Translate overlay flakes to V3 integer-ID space.
///
/// Uses the V6 store for persisted dictionary lookups and DictNovelty for
/// ephemeral IDs from uncommitted transactions.
/// Ephemeral predicate mapping: IRI → ephemeral p_id for predicates that only
/// exist in novelty. Callers must use this to extend p_id resolution so that
/// novelty-only predicates can be resolved back to IRIs during decode.
pub type EphemeralPredicateMap = HashMap<Sid, u32>;

pub fn translate_overlay_flakes(
    overlay: &dyn OverlayProvider,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    runtime_small_dicts: Option<&RuntimeSmallDicts>,
    to_t: i64,
    g_id: GraphId,
) -> (Vec<OverlayOp>, EphemeralPredicateMap) {
    let mut ops = Vec::new();
    let mut ephemeral_preds: EphemeralPredicateMap = HashMap::new();
    let mut next_ephemeral_p_id = runtime_small_dicts
        .map(|dicts| dicts.predicate_count().max(store.predicate_count()))
        .unwrap_or_else(|| store.predicate_count());

    overlay.for_each_overlay_flake(
        g_id,
        fluree_db_core::IndexType::Spot,
        None,
        None,
        true,
        to_t,
        &mut |flake| match translate_one_flake_v3_pub(
            flake,
            store,
            dict_novelty,
            runtime_small_dicts,
            &mut ephemeral_preds,
            &mut next_ephemeral_p_id,
        ) {
            Ok(op) => ops.push(op),
            Err(e) => {
                tracing::warn!(error = %e, "failed to translate overlay flake to V3");
            }
        },
    );

    (ops, ephemeral_preds)
}

/// Translate overlay flakes to V3 overlay ops, also returning flakes that cannot be translated
/// and the mapping of novelty-only predicate IRIs to ephemeral p_ids.
///
/// Some FlakeValue variants (notably `FlakeValue::Vector`) are not representable in the V3
/// overlay encoding. Those flakes are returned as fully materialized overlay-only rows so the
/// query engine can still see them (after the indexed cursor is exhausted).
///
/// The `ephemeral_preds` map contains predicate IRI → ephemeral p_id for predicates that
/// don't exist in the persisted index dictionary. Callers must use this to extend their
/// p_id → Sid lookup tables so that novelty-only predicates can be resolved during decode.
fn translate_overlay_flakes_with_untranslated(
    overlay: &dyn OverlayProvider,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    runtime_small_dicts: Option<&RuntimeSmallDicts>,
    to_t: i64,
    g_id: GraphId,
) -> (Vec<OverlayOp>, Vec<Flake>, HashMap<Sid, u32>) {
    let mut ops = Vec::new();
    let mut untranslated = Vec::new();
    let mut ephemeral_preds: HashMap<Sid, u32> = HashMap::new();
    let mut next_ephemeral_p_id = runtime_small_dicts
        .map(|dicts| dicts.predicate_count().max(store.predicate_count()))
        .unwrap_or_else(|| store.predicate_count());

    overlay.for_each_overlay_flake(
        g_id,
        fluree_db_core::IndexType::Spot,
        None,
        None,
        true,
        to_t,
        &mut |flake| match translate_one_flake_v3_pub(
            flake,
            store,
            dict_novelty,
            runtime_small_dicts,
            &mut ephemeral_preds,
            &mut next_ephemeral_p_id,
        ) {
            Ok(op) => ops.push(op),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::Unsupported {
                    untranslated.push(flake.clone());
                } else {
                    tracing::warn!(error = %e, "failed to translate overlay flake to V3");
                }
            }
        },
    );

    (ops, untranslated, ephemeral_preds)
}

/// Translate a single Flake to an OverlayOp.
///
/// `pub(crate)` so `binary_range` can reuse it for overlay translation.
pub(crate) fn translate_one_flake_v3_pub(
    flake: &fluree_db_core::Flake,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
    runtime_small_dicts: Option<&RuntimeSmallDicts>,
    ephemeral_preds: &mut HashMap<Sid, u32>,
    next_ephemeral_p_id: &mut u32,
) -> std::io::Result<OverlayOp> {
    // Subject: persisted → DictNovelty → error
    let s_id = resolve_subject_v3(&flake.s, store, dict_novelty)?;

    // Predicate: persisted → ephemeral (keyed by Sid to avoid namespace decode issues).
    //
    // For novelty-only predicates (not present in the persisted predicate dictionary),
    // we allocate ephemeral p_ids and later extend `p_sids` so decode produces the
    // original Sid (in snapshot namespace space).
    let p_id = match store.sid_to_p_id(&flake.p) {
        Some(id) => id,
        None => runtime_small_dicts
            .and_then(|dicts| dicts.predicate_id(&flake.p))
            .map(|id| {
                *ephemeral_preds
                    .entry(flake.p.clone())
                    .or_insert(id.as_u32())
            })
            .unwrap_or_else(|| {
                *ephemeral_preds.entry(flake.p.clone()).or_insert_with(|| {
                    let id = *next_ephemeral_p_id;
                    *next_ephemeral_p_id += 1;
                    id
                })
            }),
    };

    // Object value → (o_type, o_key), using flake.dt + lang for proper OType.
    let lang = flake.m.as_ref().and_then(|m| m.lang.as_deref());
    let (o_type, o_key) = value_to_otype_okey(&flake.o, &flake.dt, lang, store, dict_novelty)?;

    // List index
    let o_i = flake
        .m
        .as_ref()
        .and_then(|m| m.i)
        .map(|i| i as u32)
        .unwrap_or(u32::MAX);

    Ok(OverlayOp {
        s_id,
        p_id,
        o_type: o_type.as_u16(),
        o_key,
        o_i,
        t: flake.t,
        op: flake.op,
    })
}

/// Resolve a subject Sid to s_id using persisted dict then DictNovelty.
fn resolve_subject_v3(
    sid: &Sid,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
) -> std::io::Result<u64> {
    // 1. Persisted (canonical encoding guarantees exact-parts match)
    if let Some(id) = store.find_subject_id_by_parts(sid.namespace_code, &sid.name)? {
        return Ok(id);
    }
    // 2. DictNovelty
    if let Some(dn) = dict_novelty {
        if dn.is_initialized() {
            if let Some(id) = dn.subjects.find_subject(sid.namespace_code, &sid.name) {
                return Ok(id);
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "subject not found in persisted or novelty dict: ns={} name={}",
            sid.namespace_code, sid.name
        ),
    ))
}

/// Resolve a string value to a string_id using persisted dict then DictNovelty.
fn resolve_string_v3(
    value: &str,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
) -> std::io::Result<u32> {
    // 1. Persisted
    if let Some(id) = store.find_string_id(value)? {
        return Ok(id);
    }
    // 2. DictNovelty
    if let Some(dn) = dict_novelty {
        if dn.is_initialized() {
            if let Some(id) = dn.strings.find_string(value) {
                return Ok(id);
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "string not found in dict: {}",
            &value[..value.len().min(50)]
        ),
    ))
}

/// Convert a FlakeValue to `(OType, o_key)` in V3 encoding.
///
/// Uses `dt_sid` (the flake's datatype Sid) and `lang` (from FlakeMeta) to derive
/// the correct OType, rather than inferring purely from the FlakeValue variant.
/// This is critical for:
/// - langString: OType must embed the lang_id, not use XSD_STRING
/// - numeric subtypes: xsd:int vs xsd:integer can share the same FlakeValue::Long
/// - string subtypes: xsd:anyURI vs xsd:string share FlakeValue::String
fn value_to_otype_okey(
    val: &FlakeValue,
    dt_sid: &Sid,
    lang: Option<&str>,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
) -> std::io::Result<(OType, u64)> {
    // If the value has a language tag, it's rdf:langString — encode lang_id into OType.
    if let Some(lang_tag) = lang {
        let str_id = resolve_string_v3(
            match val {
                FlakeValue::String(s) => s,
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "langString value must be FlakeValue::String",
                    ))
                }
            },
            store,
            dict_novelty,
        )?;
        let lang_id = store.resolve_lang_id(lang_tag).unwrap_or_else(|| {
            tracing::warn!(
                tag = lang_tag,
                "language tag not found in persisted dict, using 1"
            );
            1
        });
        return Ok((OType::lang_string(lang_id), str_id as u64));
    }

    // For value types that are dt-dependent (Long, Double, String), resolve
    // the exact OType from the datatype Sid. For value types with 1:1
    // OType mapping (Bool, Date, Ref, etc.), the FlakeValue variant suffices.
    //
    // If the datatype cannot be resolved (novelty-only custom type not yet in
    // the persisted dict), return Unsupported so callers decline the fast path
    // rather than silently encoding under a wrong base-XSD type.
    let dt_otype = otype_from_dt_sid(dt_sid, store);

    match val {
        FlakeValue::Null => Ok((OType::NULL, 0)),
        FlakeValue::Boolean(b) => Ok((OType::XSD_BOOLEAN, *b as u64)),
        FlakeValue::Long(n) => {
            let ot = dt_otype.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "datatype not resolvable to OType for Long value",
                )
            })?;
            Ok((ot, ObjKey::encode_i64(*n).as_u64()))
        }
        FlakeValue::Double(d) => {
            let ot = dt_otype.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "datatype not resolvable to OType for Double value",
                )
            })?;
            if d.is_finite() && d.fract() == 0.0 {
                let as_i64 = *d as i64;
                if (as_i64 as f64) == *d {
                    return Ok((ot, ObjKey::encode_i64(as_i64).as_u64()));
                }
            }
            if d.is_finite() {
                match ObjKey::encode_f64(*d) {
                    Ok(key) => Ok((ot, key.as_u64())),
                    Err(_) => Ok((OType::NULL, 0)),
                }
            } else {
                Ok((OType::NULL, 0))
            }
        }
        FlakeValue::Ref(sid) => {
            let s_id = resolve_subject_v3(sid, store, dict_novelty)?;
            Ok((OType::IRI_REF, s_id))
        }
        FlakeValue::String(s) => {
            let str_id = resolve_string_v3(s, store, dict_novelty)?;
            let ot = dt_otype.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "datatype not resolvable to OType for String value",
                )
            })?;
            Ok((ot, str_id as u64))
        }
        FlakeValue::Json(s) => {
            let str_id = resolve_string_v3(s, store, dict_novelty)?;
            Ok((OType::RDF_JSON, str_id as u64))
        }
        FlakeValue::Date(d) => {
            let days = d.days_since_epoch();
            Ok((OType::XSD_DATE, ObjKey::encode_date(days).as_u64()))
        }
        FlakeValue::DateTime(dt) => {
            let micros = dt.epoch_micros();
            Ok((
                OType::XSD_DATE_TIME,
                ObjKey::encode_datetime(micros).as_u64(),
            ))
        }
        FlakeValue::Time(t) => {
            let micros = t.micros_since_midnight();
            Ok((OType::XSD_TIME, ObjKey::encode_time(micros).as_u64()))
        }
        FlakeValue::GYear(g) => Ok((OType::XSD_G_YEAR, ObjKey::encode_g_year(g.year()).as_u64())),
        FlakeValue::GYearMonth(g) => Ok((
            OType::XSD_G_YEAR_MONTH,
            ObjKey::encode_g_year_month(g.year(), g.month()).as_u64(),
        )),
        FlakeValue::GMonth(g) => Ok((
            OType::XSD_G_MONTH,
            ObjKey::encode_g_month(g.month()).as_u64(),
        )),
        FlakeValue::GDay(g) => Ok((OType::XSD_G_DAY, ObjKey::encode_g_day(g.day()).as_u64())),
        FlakeValue::GMonthDay(g) => Ok((
            OType::XSD_G_MONTH_DAY,
            ObjKey::encode_g_month_day(g.month(), g.day()).as_u64(),
        )),
        FlakeValue::YearMonthDuration(d) => Ok((
            OType::XSD_YEAR_MONTH_DURATION,
            ObjKey::encode_year_month_dur(d.months()).as_u64(),
        )),
        FlakeValue::DayTimeDuration(d) => Ok((
            OType::XSD_DAY_TIME_DURATION,
            ObjKey::encode_day_time_dur(d.micros()).as_u64(),
        )),
        FlakeValue::GeoPoint(bits) => Ok((OType::GEO_POINT, bits.0)),
        // Types not yet handled: BigInt, Decimal, Vector, Duration
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("unsupported FlakeValue variant for V3 overlay: {val:?}"),
        )),
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EncodedObjectPrefilter {
    pub o_type: Option<OType>,
    pub o_key: u64,
}

/// Build the narrowest safe binary prefilter for a bound object.
///
/// When the query does not specify a numeric datatype, we intentionally leave
/// `o_type` unset and rely on post-decode equality checks. This preserves the
/// broader integer/float family semantics instead of forcing `Long` through
/// `xsd:integer` on the binary path.
pub(crate) fn encode_bound_object_prefilter(
    val: &FlakeValue,
    dt_sid: Option<&Sid>,
    lang: Option<&str>,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<fluree_db_core::dict_novelty::DictNovelty>>,
) -> std::io::Result<EncodedObjectPrefilter> {
    use fluree_db_core::value_id::ObjKey;

    match (dt_sid, lang) {
        (Some(dt_sid), lang) => {
            let (ot, key) = value_to_otype_okey(val, dt_sid, lang, store, dict_novelty)?;
            Ok(EncodedObjectPrefilter {
                o_type: Some(ot),
                o_key: key,
            })
        }
        (None, Some(_)) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "lang tag requires datatype constraint",
        )),
        (None, None) => match val {
            // Without a datatype constraint, plain strings are ambiguous:
            // could be xsd:string or rdf:langString.
            FlakeValue::String(_) => Err(std::io::Error::other(
                "string without dtc: type ambiguous (could be langString)",
            )),
            // Untyped numerics should not pre-commit to a specific numeric OType.
            FlakeValue::Long(n) => Ok(EncodedObjectPrefilter {
                o_type: None,
                o_key: ObjKey::encode_i64(*n).as_u64(),
            }),
            FlakeValue::Double(d) => {
                if d.is_finite() {
                    ObjKey::encode_f64(*d)
                        .map(|key| EncodedObjectPrefilter {
                            o_type: None,
                            o_key: key.as_u64(),
                        })
                        .map_err(|_| std::io::Error::other("cannot encode f64 for V6 index"))
                } else {
                    Err(std::io::Error::other("non-finite double in bound object"))
                }
            }
            _ => {
                let (ot, key) = value_to_otype_okey_simple(val, store)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                Ok(EncodedObjectPrefilter {
                    o_type: Some(ot),
                    o_key: key,
                })
            }
        },
    }
}

fn infer_exact_datatype_sid_from_stats(
    stats_view: Option<&fluree_db_core::StatsView>,
    g_id: GraphId,
    p_id: RuntimePredicateId,
    value: &FlakeValue,
) -> Option<Sid> {
    let stats = stats_view?.get_graph_property(g_id, p_id)?;
    let mut tags = stats
        .datatypes
        .iter()
        .filter_map(|(tag, count)| (*count > 0).then_some(*tag))
        .collect::<Vec<_>>();
    tags.sort();
    tags.dedup();
    if tags.len() != 1 {
        return None;
    }
    datatype_sid_for_untyped_value(value, tags[0])
}

fn datatype_sid_for_untyped_value(
    value: &FlakeValue,
    tag: fluree_db_core::ValueTypeTag,
) -> Option<Sid> {
    match value {
        FlakeValue::Long(_) => match tag {
            fluree_db_core::ValueTypeTag::INTEGER => {
                Some(Sid::new(namespaces::XSD, xsd_names::INTEGER))
            }
            fluree_db_core::ValueTypeTag::LONG => Some(Sid::new(namespaces::XSD, xsd_names::LONG)),
            fluree_db_core::ValueTypeTag::INT => Some(Sid::new(namespaces::XSD, xsd_names::INT)),
            fluree_db_core::ValueTypeTag::SHORT => {
                Some(Sid::new(namespaces::XSD, xsd_names::SHORT))
            }
            fluree_db_core::ValueTypeTag::BYTE => Some(Sid::new(namespaces::XSD, xsd_names::BYTE)),
            fluree_db_core::ValueTypeTag::UNSIGNED_LONG => {
                Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_LONG))
            }
            fluree_db_core::ValueTypeTag::UNSIGNED_INT => {
                Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_INT))
            }
            fluree_db_core::ValueTypeTag::UNSIGNED_SHORT => {
                Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_SHORT))
            }
            fluree_db_core::ValueTypeTag::UNSIGNED_BYTE => {
                Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_BYTE))
            }
            fluree_db_core::ValueTypeTag::NON_NEGATIVE_INTEGER => {
                Some(Sid::new(namespaces::XSD, xsd_names::NON_NEGATIVE_INTEGER))
            }
            fluree_db_core::ValueTypeTag::POSITIVE_INTEGER => {
                Some(Sid::new(namespaces::XSD, xsd_names::POSITIVE_INTEGER))
            }
            fluree_db_core::ValueTypeTag::NON_POSITIVE_INTEGER => {
                Some(Sid::new(namespaces::XSD, xsd_names::NON_POSITIVE_INTEGER))
            }
            fluree_db_core::ValueTypeTag::NEGATIVE_INTEGER => {
                Some(Sid::new(namespaces::XSD, xsd_names::NEGATIVE_INTEGER))
            }
            _ => None,
        },
        FlakeValue::Double(_) => match tag {
            fluree_db_core::ValueTypeTag::DOUBLE => {
                Some(Sid::new(namespaces::XSD, xsd_names::DOUBLE))
            }
            fluree_db_core::ValueTypeTag::FLOAT => {
                Some(Sid::new(namespaces::XSD, xsd_names::FLOAT))
            }
            fluree_db_core::ValueTypeTag::DECIMAL => {
                Some(Sid::new(namespaces::XSD, xsd_names::DECIMAL))
            }
            fluree_db_core::ValueTypeTag::INTEGER => {
                Some(Sid::new(namespaces::XSD, xsd_names::INTEGER))
            }
            fluree_db_core::ValueTypeTag::LONG => Some(Sid::new(namespaces::XSD, xsd_names::LONG)),
            fluree_db_core::ValueTypeTag::INT => Some(Sid::new(namespaces::XSD, xsd_names::INT)),
            _ => None,
        },
        _ => None,
    }
}

/// Resolve a datatype Sid to its exact OType.
///
/// Resolution order:
/// 1. Well-known: Sid → IRI → `resolve_iri_to_otype_option` (XSD, rdf:JSON, etc.)
/// 2. Custom persisted: Sid → positional `dt_id` → `OType::customer_datatype(dt_id)`
/// 3. Unknown (novelty-only custom type): `None`
fn otype_from_dt_sid(dt_sid: &Sid, store: &BinaryIndexStore) -> Option<OType> {
    // Well-known datatypes: resolve via IRI string matching.
    if let Some(iri) = store.sid_to_iri(dt_sid) {
        if let Some(ot) = fluree_db_core::o_type_registry::resolve_iri_to_otype_option(&iri) {
            return Some(ot);
        }
    }
    // Custom persisted datatypes: look up the Sid's position in the datatype dict.
    let dt_id = store.find_dt_id(dt_sid)?;
    Some(OType::customer_datatype(dt_id))
}

/// Simplified FlakeValue → (OType, o_key) translation for fast-path operators.
///
/// Uses default OType for each value variant (no dt_sid/lang context needed).
/// Works for the common cases in bound-object count queries. Does not handle
/// langString (no language tag available) or custom datatypes.
pub(crate) fn value_to_otype_okey_simple(
    val: &FlakeValue,
    store: &BinaryIndexStore,
) -> Result<(OType, u64)> {
    use fluree_db_core::value_id::ObjKey;

    match val {
        FlakeValue::Null => Ok((OType::NULL, 0)),
        FlakeValue::Boolean(b) => Ok((OType::XSD_BOOLEAN, *b as u64)),
        FlakeValue::Long(n) => Ok((OType::XSD_INTEGER, ObjKey::encode_i64(*n).as_u64())),
        FlakeValue::Double(d) => {
            if d.is_finite() {
                ObjKey::encode_f64(*d)
                    .map(|key| (OType::XSD_DOUBLE, key.as_u64()))
                    .map_err(|_| {
                        QueryError::execution("cannot encode f64 for V6 index".to_string())
                    })
            } else {
                Err(QueryError::execution(
                    "non-finite double in bound object".to_string(),
                ))
            }
        }
        FlakeValue::Ref(sid) => {
            let s_id = store
                .find_subject_id_by_parts(sid.namespace_code, &sid.name)
                .map_err(|e| QueryError::execution(format!("find_subject_id_by_parts: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("ref object not found in V6 dict".to_string())
                })?;
            Ok((OType::IRI_REF, s_id))
        }
        FlakeValue::String(s) => {
            let str_id = store
                .find_string_id(s)
                .map_err(|e| QueryError::execution(format!("find_string_id: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("string value not found in V6 dict".to_string())
                })?;
            Ok((OType::XSD_STRING, str_id as u64))
        }
        FlakeValue::Json(s) => {
            let str_id = store
                .find_string_id(s)
                .map_err(|e| QueryError::execution(format!("find_string_id: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("JSON value not found in V6 dict".to_string())
                })?;
            Ok((OType::RDF_JSON, str_id as u64))
        }
        FlakeValue::Date(d) => Ok((
            OType::XSD_DATE,
            ObjKey::encode_date(d.days_since_epoch()).as_u64(),
        )),
        FlakeValue::DateTime(dt) => Ok((
            OType::XSD_DATE_TIME,
            ObjKey::encode_datetime(dt.epoch_micros()).as_u64(),
        )),
        FlakeValue::Time(t) => Ok((
            OType::XSD_TIME,
            ObjKey::encode_time(t.micros_since_midnight()).as_u64(),
        )),
        FlakeValue::GYear(g) => Ok((OType::XSD_G_YEAR, ObjKey::encode_g_year(g.year()).as_u64())),
        FlakeValue::GYearMonth(g) => Ok((
            OType::XSD_G_YEAR_MONTH,
            ObjKey::encode_g_year_month(g.year(), g.month()).as_u64(),
        )),
        FlakeValue::GMonth(g) => Ok((
            OType::XSD_G_MONTH,
            ObjKey::encode_g_month(g.month()).as_u64(),
        )),
        FlakeValue::GDay(g) => Ok((OType::XSD_G_DAY, ObjKey::encode_g_day(g.day()).as_u64())),
        FlakeValue::GMonthDay(g) => Ok((
            OType::XSD_G_MONTH_DAY,
            ObjKey::encode_g_month_day(g.month(), g.day()).as_u64(),
        )),
        _ => Err(QueryError::execution(format!(
            "unsupported FlakeValue variant for V6 fast-path: {val:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{stats_view::GraphPropertyStatData, StatsView, ValueTypeTag};

    #[test]
    fn infer_exact_datatype_for_integer_family() {
        let mut stats = StatsView::default();
        stats.graph_properties.insert(
            0,
            HashMap::from([(
                RuntimePredicateId::from_u32(7),
                GraphPropertyStatData {
                    count: 10,
                    ndv_values: 0,
                    ndv_subjects: 0,
                    datatypes: vec![(ValueTypeTag::INT, 10)],
                },
            )]),
        );

        let inferred = infer_exact_datatype_sid_from_stats(
            Some(&stats),
            0,
            RuntimePredicateId::from_u32(7),
            &FlakeValue::Long(42),
        )
        .expect("datatype");
        assert_eq!(inferred.namespace_code, namespaces::XSD);
        assert_eq!(inferred.name, xsd_names::INT.into());
    }

    #[test]
    fn does_not_infer_when_multiple_datatypes_present() {
        let mut stats = StatsView::default();
        stats.graph_properties.insert(
            0,
            HashMap::from([(
                RuntimePredicateId::from_u32(7),
                GraphPropertyStatData {
                    count: 10,
                    ndv_values: 0,
                    ndv_subjects: 0,
                    datatypes: vec![(ValueTypeTag::INT, 5), (ValueTypeTag::LONG, 5)],
                },
            )]),
        );

        assert!(infer_exact_datatype_sid_from_stats(
            Some(&stats),
            0,
            RuntimePredicateId::from_u32(7),
            &FlakeValue::Long(42),
        )
        .is_none());
    }
}
