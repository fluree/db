//! Index-accelerated geographic proximity search operator.
//!
//! Implements `Pattern::GeoSearch` by using the POST index to efficiently scan
//! for GeoPoint values within a latitude band, then applying haversine post-filter
//! for exact distance calculations.
//!
//! # Algorithm
//!
//! 1. Convert center point + radius to latitude-band bounds via `geo_proximity_bounds()`
//! 2. Create POST cursor for the predicate with `o_kind = GEO_POINT`
//! 3. Iterate GeoPoints in the latitude band
//! 4. Apply haversine post-filter to compute exact distance
//! 5. Emit subject and distance bindings for points within radius

use crate::binding::{Batch, Binding, RowAccess};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::ir::{GeoSearchCenter, GeoSearchPattern};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::branch::BranchManifest;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{
    resolve_overlay_ops, sort_overlay_ops, BinaryCursor, BinaryFilter, BinaryGraphView,
    BinaryIndexStore, ColumnProjection, OverlayOp, RunSortOrder,
};
use fluree_db_core::geo::{geo_proximity_bounds, haversine_distance};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{FlakeValue, GeoPointBits, GraphId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Operator for index-accelerated geographic proximity search.
///
/// Uses the POST index to scan for GeoPoint values within a latitude band,
/// then applies haversine distance filtering for exact results.
pub struct GeoSearchOperator {
    /// Child operator (for correlated queries, typically EmptySeedOperator)
    child: BoxedOperator,
    /// The geo search pattern specification
    pattern: GeoSearchPattern,
    /// Output schema (variables from child + result variables)
    in_schema: Arc<[VarId]>,
    /// Column position for each variable in output
    out_pos: HashMap<VarId, usize>,
    /// Well-known datatypes for binding construction
    datatypes: WellKnownDatatypes,
    /// Predicate ID for the geo-indexed predicate in binary index space.
    ///
    /// When an overlay is present, this may be an ephemeral predicate ID assigned
    /// by `DictOverlay`.
    p_id: Option<u32>,
    /// Pre-translated novelty overlay operations, sorted by POST order.
    overlay_ops: Vec<OverlayOp>,
    /// Overlay epoch for cursor cache keying.
    overlay_epoch: u64,
    /// Dict overlay for ephemeral ID translation and forward resolution.
    dict_overlay: Option<crate::dict_overlay::DictOverlay>,
    /// Operator lifecycle state
    state: OperatorState,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl GeoSearchOperator {
    /// Create a new geo search operator.
    pub fn new(child: BoxedOperator, pattern: GeoSearchPattern) -> Self {
        // Build output schema: child vars + result vars
        let mut schema_vars: Vec<VarId> = child.schema().to_vec();
        let mut seen: HashSet<VarId> = schema_vars.iter().copied().collect();

        // Add subject variable
        if seen.insert(pattern.subject_var) {
            schema_vars.push(pattern.subject_var);
        }

        // Add distance variable if present
        if let Some(v) = pattern.distance_var {
            if seen.insert(v) {
                schema_vars.push(v);
            }
        }

        let schema: Arc<[VarId]> = Arc::from(schema_vars.into_boxed_slice());
        let out_pos: HashMap<VarId, usize> =
            schema.iter().enumerate().map(|(i, v)| (*v, i)).collect();

        Self {
            child,
            pattern,
            in_schema: schema,
            out_pos,
            datatypes: WellKnownDatatypes::new(),
            p_id: None,
            overlay_ops: Vec::new(),
            overlay_epoch: 0,
            dict_overlay: None,
            state: OperatorState::Created,
            out_schema: None,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    /// Resolve center point coordinates from pattern (constant or variable binding).
    fn resolve_center(&self, row: Option<&crate::binding::RowView>) -> Option<(f64, f64)> {
        match &self.pattern.center {
            GeoSearchCenter::Const { lat, lng } => Some((*lat, *lng)),
            GeoSearchCenter::Var(var_id) => {
                // Look up variable binding in the row
                let row = row?;
                let binding = row.get(*var_id)?;

                // Extract coordinates from binding
                match binding {
                    Binding::Lit { val, .. } => match val {
                        FlakeValue::GeoPoint(bits) => Some((bits.lat(), bits.lng())),
                        FlakeValue::String(s) => fluree_db_core::geo::try_extract_point(s),
                        _ => None,
                    },
                    _ => None,
                }
            }
        }
    }

    /// Execute geo search and produce bindings.
    #[allow(clippy::too_many_arguments)]
    fn execute_search(
        &self,
        store: &Arc<BinaryIndexStore>,
        g_id: GraphId,
        to_t: i64,
        center_lat: f64,
        center_lng: f64,
        child_schema: &[VarId],
        child_cols: &[&[Binding]],
        row_idx: usize,
    ) -> Result<Vec<Vec<Binding>>> {
        let radius = self.pattern.radius_meters;
        let limit = self.pattern.limit;

        // Get latitude-band bounds for POST scan
        let bounds = geo_proximity_bounds(center_lat, center_lng, radius);
        if bounds.is_empty() {
            return Ok(vec![]);
        }

        let Some(p_id) = self.p_id else {
            // Predicate not present (and no overlay to supply it).
            return Ok(vec![]);
        };

        let mut results: Vec<(u64, f64)> = Vec::new(); // (s_id, distance)

        // Get branch manifest for POST order.
        let branch_ref = store
            .branch_for_order(g_id, RunSortOrder::Post)
            .ok_or_else(|| {
                QueryError::Internal(format!("GeoSearch: no POST branch for g_id={g_id}"))
            })?;
        let branch: Arc<BranchManifest> = Arc::clone(branch_ref);

        let geo_otype = OType::GEO_POINT.as_u16();

        // Scan each latitude band range
        for (min_o_key, max_o_key) in bounds {
            // Build RunRecordV2 bounds for POST scan
            // POST sort order: p_id, o_type, o_key, o_i, s_id
            let min_key = RunRecordV2 {
                s_id: SubjectId::from_u64(0),
                p_id,
                o_type: geo_otype,
                o_key: min_o_key,
                o_i: 0,
                t: 0,
                g_id,
            };

            let max_key = RunRecordV2 {
                s_id: SubjectId::from_u64(u64::MAX),
                p_id,
                o_type: geo_otype,
                o_key: max_o_key,
                o_i: u32::MAX,
                t: u32::MAX,
                g_id,
            };

            // Create filter for p_id and GEO_POINT
            let filter = BinaryFilter {
                s_id: None,
                p_id: Some(p_id),
                o_type: Some(geo_otype),
                o_key: None, // Range filtering done by cursor bounds
                o_i: None,
            };

            // Create cursor for POST order
            let mut cursor = BinaryCursor::new(
                store.clone(),
                RunSortOrder::Post,
                branch.clone(),
                &min_key,
                &max_key,
                filter,
                ColumnProjection::all(),
            );

            // Time-travel and overlay support (must match binary scan semantics).
            cursor.set_to_t(to_t);
            if !self.overlay_ops.is_empty() {
                cursor.set_epoch(self.overlay_epoch);
                cursor.set_overlay_ops(self.overlay_ops.clone());
            }

            // Iterate results - collect ALL points within radius first
            // (limit is applied after sorting by distance for correct "nearest k" semantics)
            while let Some(batch) = cursor
                .next_batch()
                .map_err(|e| QueryError::Internal(e.to_string()))?
            {
                for i in 0..batch.row_count {
                    let o_key = batch.o_key.get(i);
                    let s_id = batch.s_id.get(i);

                    // Decode GeoPoint from o_key
                    let bits = GeoPointBits(o_key);
                    let point_lat = bits.lat();
                    let point_lng = bits.lng();

                    // Apply haversine post-filter
                    let distance = haversine_distance(center_lat, center_lng, point_lat, point_lng);
                    if distance <= radius {
                        results.push((s_id, distance));
                    }
                }
            }
        }

        // Deduplicate by subject_id, keeping min distance.
        // This handles:
        // 1. Antimeridian crossing (same subject in multiple latitude bands)
        // 2. Multiple GeoPoint values per subject for the same predicate
        let mut deduped: HashMap<u64, f64> = HashMap::new();
        for (s_id, distance) in results {
            deduped
                .entry(s_id)
                .and_modify(|existing| {
                    if distance < *existing {
                        *existing = distance;
                    }
                })
                .or_insert(distance);
        }
        let mut results: Vec<(u64, f64)> = deduped.into_iter().collect();

        // Sort by distance and apply limit
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        // Build output columns
        let num_cols = self.in_schema.len();
        let mut output_rows: Vec<Vec<Binding>> = Vec::with_capacity(results.len());

        for (s_id, distance) in results {
            let mut row = vec![Binding::Unbound; num_cols];

            // Copy child columns
            for (col_idx, &var) in child_schema.iter().enumerate() {
                if let Some(&out_idx) = self.out_pos.get(&var) {
                    row[out_idx] = child_cols[col_idx][row_idx].clone();
                }
            }

            // Add subject binding: s_id → Sid
            // DictOverlay delegates to novelty-aware BinaryGraphView::resolve_subject_sid
            // which returns Sid directly (no IRI string + trie round-trip).
            // When no overlay, store is the only dict (no novelty), so resolve+encode is safe.
            let subject_sid = match &self.dict_overlay {
                Some(dict_ov) => dict_ov
                    .resolve_subject_sid(s_id)
                    .map_err(|e| QueryError::Internal(e.to_string()))?,
                None => {
                    let iri = store
                        .resolve_subject_iri(s_id)
                        .map_err(|e| QueryError::Internal(e.to_string()))?;
                    store.encode_iri(&iri)
                }
            };
            let subject_pos = *self.out_pos.get(&self.pattern.subject_var).unwrap();
            row[subject_pos] = Binding::sid(subject_sid);

            // Add distance binding if requested
            if let Some(dist_var) = self.pattern.distance_var {
                let dist_pos = *self.out_pos.get(&dist_var).unwrap();
                row[dist_pos] = Binding::lit(
                    FlakeValue::Double(distance),
                    self.datatypes.xsd_double.clone(),
                );
            }

            output_rows.push(row);
        }

        Ok(output_rows)
    }
}

#[async_trait]
impl Operator for GeoSearchOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;

        // Verify binary store is available
        let Some(store) = ctx.binary_store.as_ref() else {
            return Err(QueryError::InvalidQuery(
                "GeoSearch requires binary index (binary_store not configured)".to_string(),
            ));
        };

        // Check time-travel coverage: GeoSearch requires to_t >= base_t
        // For historical queries before base_t, the binary index does not have coverage.
        let base_t = store.base_t();
        if ctx.to_t < base_t {
            return Err(QueryError::TimeRangeNotCovered {
                requested_t: ctx.to_t,
                base_t,
            });
        }

        // Validate variable target is available from child if needed
        if let GeoSearchCenter::Var(v) = &self.pattern.center {
            if !self.child.schema().iter().any(|vv| vv == v) {
                return Err(QueryError::InvalidQuery(format!(
                    "GeoSearch center variable {v:?} is not bound by previous patterns"
                )));
            }
        }

        // Overlay translation + DictOverlay setup (matches BinaryScanOperator's binary path).
        let g_id = ctx.binary_g_id;
        if let Some(ovl) = ctx.overlay {
            // Correctness first: GeoSearch depends on a predicate+o_type constrained overlay.
            // If we cannot translate a geo overlay flake into V3 ID space, returning
            // silently-wrong results is unacceptable; fail the query instead.
            let mut ops: Vec<OverlayOp> = Vec::new();
            let mut ephemeral_preds = std::collections::HashMap::new();
            let mut next_ep = store.predicate_count();
            let mut translate_failed = false;
            ovl.for_each_overlay_flake(
                g_id,
                fluree_db_core::IndexType::Post,
                None,
                None,
                true,
                ctx.to_t,
                &mut |flake| match crate::binary_scan::translate_one_flake_v3_pub(
                    flake,
                    store,
                    ctx.dict_novelty.as_ref(),
                    ctx.runtime_small_dicts,
                    &mut ephemeral_preds,
                    &mut next_ep,
                ) {
                    Ok(op) => ops.push(op),
                    Err(e) => {
                        // Hard fail: GeoSearch must be exact.
                        tracing::error!(
                            error = %e,
                            s = %flake.s,
                            p = %flake.p,
                            t = flake.t,
                            op = flake.op,
                            "GeoSearch: failed to translate overlay flake to V3"
                        );
                        translate_failed = true;
                    }
                },
            );
            if translate_failed {
                return Err(QueryError::Internal(
                    "GeoSearch: failed to translate overlay flake to V3".to_string(),
                ));
            }
            let epoch = ovl.epoch();
            if !ops.is_empty() {
                sort_overlay_ops(&mut ops, RunSortOrder::Post);
                resolve_overlay_ops(&mut ops);
                self.overlay_ops = ops;
                self.overlay_epoch = epoch;
            }
            // Build DictOverlay for ephemeral ID translation.
            if let Some(dict_nov) = ctx.dict_novelty.as_ref() {
                let gv = BinaryGraphView::with_novelty(store.clone(), g_id, Some(dict_nov.clone()))
                    .with_namespace_codes_fallback(ctx.namespace_codes_fallback.clone());
                let dict_ov = crate::dict_overlay::DictOverlay::new(gv, dict_nov.clone());
                self.dict_overlay = Some(dict_ov);
            }
        }

        // Resolve predicate ID once at open (may allocate ephemeral ID if overlay present).
        self.p_id = match self.dict_overlay.as_mut() {
            Some(dict_ov) => {
                let iri = ctx
                    .active_snapshot
                    .decode_sid(&self.pattern.predicate)
                    .ok_or_else(|| {
                        QueryError::Internal(
                            "GeoSearch predicate Sid could not be decoded to an IRI".to_string(),
                        )
                    })?;
                Some(dict_ov.assign_predicate_id(&iri))
            }
            None => store.sid_to_p_id(&self.pattern.predicate),
        };

        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // Get binary store from context
        let store = ctx.binary_store.as_ref().ok_or_else(|| {
            QueryError::InvalidQuery("GeoSearch requires binary_store".to_string())
        })?;
        let g_id = ctx.binary_g_id;

        // Pull child batch
        let input_batch = match self.child.next_batch(ctx).await? {
            Some(b) => b,
            None => {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
        };

        if input_batch.is_empty() {
            return Ok(Some(Batch::empty(self.in_schema.clone())?));
        }

        let child_schema = self.child.schema();
        let child_cols: Vec<&[Binding]> = (0..child_schema.len())
            .map(|i| input_batch.column_by_idx(i).expect("schema mismatch"))
            .collect();

        // Accumulate output columns
        let num_cols = self.in_schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();

        // Process each input row
        for row_idx in 0..input_batch.len() {
            // Resolve center point for this row
            let Some(row_view) = input_batch.row_view(row_idx) else {
                continue; // Should not happen since we iterate within bounds
            };
            let Some((center_lat, center_lng)) = self.resolve_center(Some(&row_view)) else {
                continue; // Skip if center cannot be resolved
            };

            // Execute geo search
            let result_rows = self.execute_search(
                store,
                g_id,
                ctx.to_t,
                center_lat,
                center_lng,
                child_schema,
                &child_cols,
                row_idx,
            )?;

            // Add results to output columns
            for result_row in result_rows {
                for (col_idx, binding) in result_row.into_iter().enumerate() {
                    columns[col_idx].push(binding);
                }
            }
        }

        let batch = Batch::new(self.in_schema.clone(), columns)?;
        Ok(trim_batch(&self.out_schema, batch))
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Geo search typically returns a small subset
        Some(100)
    }
}
