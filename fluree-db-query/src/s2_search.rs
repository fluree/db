//! S2 spatial index search operator.
//!
//! Implements spatial predicate queries using the S2 sidecar index:
//! - `within`: subjects whose geometry is within query geometry
//! - `contains`: subjects whose geometry contains query geometry
//! - `intersects`: subjects whose geometry intersects query geometry
//! - `nearby`: proximity queries with distance ordering
//!
//! # Algorithm
//!
//! 1. Parse query geometry (WKT or point)
//! 2. Generate S2 covering for query geometry
//! 3. Scan S2 index cells in covering ranges
//! 4. Apply exact geometry test (bbox pre-filter + exact test)
//! 5. Dedup across cells by subject_id
//! 6. Emit bindings for matching subjects

use crate::binding::{Batch, Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{S2QueryGeom, S2SearchPattern, S2SpatialOp};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_spatial::SpatialIndexProvider;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Operator for S2 spatial index searches.
///
/// Uses the S2 sidecar index for efficient spatial predicate queries
/// on complex geometries (polygons, linestrings, etc.).
pub struct S2SearchOperator {
    /// Child operator (for correlated queries)
    child: BoxedOperator,
    /// The S2 search pattern specification
    pattern: S2SearchPattern,
    /// Output schema (variables from child + result variables)
    in_schema: Arc<[VarId]>,
    /// Column position for each variable in output
    out_pos: HashMap<VarId, usize>,
    /// Operator lifecycle state
    state: OperatorState,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl S2SearchOperator {
    /// Create a new S2 search operator.
    pub fn new(child: BoxedOperator, pattern: S2SearchPattern) -> Self {
        // Build output schema: child vars + result vars
        let mut schema_vars: Vec<VarId> = child.schema().to_vec();
        let mut seen: HashSet<VarId> = schema_vars.iter().copied().collect();

        // Add subject variable
        if seen.insert(pattern.subject_var) {
            schema_vars.push(pattern.subject_var);
        }

        // Add distance variable if present (for nearby queries)
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
            state: OperatorState::Created,
            out_schema: None,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    /// Resolve query geometry from pattern (constant or variable binding).
    ///
    /// For `EncodedLit` bindings, requires `binary_store` in context to decode.
    fn resolve_query_geom(
        &self,
        row: Option<&crate::binding::RowView>,
        ctx: &ExecutionContext<'_>,
    ) -> Option<QueryGeomResolved> {
        match &self.pattern.query_geom {
            S2QueryGeom::Wkt(wkt) => Some(QueryGeomResolved::Wkt(wkt.clone())),
            S2QueryGeom::Point { lat, lng } => Some(QueryGeomResolved::Point(*lat, *lng)),
            S2QueryGeom::Var(var_id) => {
                // Resolve variable binding to WKT string
                let row = row?;
                let binding = row.get(*var_id)?;

                // Extract geometry from the binding
                match binding {
                    Binding::Lit { val, dtc, .. } => {
                        let dt = dtc.datatype();
                        match val {
                            fluree_db_core::FlakeValue::String(s) => {
                                // Check datatype - warn if not geo:wktLiteral
                                // (geo:wktLiteral uses namespace OGC_GEO=11 with local name "wktLiteral")
                                if dt.namespace_code != fluree_vocab::namespaces::OGC_GEO {
                                    tracing::debug!(
                                        ns = dt.namespace_code,
                                        name = %dt.name,
                                        "S2Search: Lit binding has non-geo:wktLiteral datatype, attempting WKT parse anyway"
                                    );
                                }
                                Some(QueryGeomResolved::Wkt(s.to_string()))
                            }
                            fluree_db_core::FlakeValue::GeoPoint(bits) => {
                                // Inline GeoPoint - extract lat/lng directly
                                let (lat, lng) = fluree_db_core::ObjKey::from_u64(bits.as_u64())
                                    .decode_geo_point();
                                Some(QueryGeomResolved::Point(lat, lng))
                            }
                            _ => None, // Not a string or GeoPoint literal
                        }
                    }
                    // Handle encoded literals (late materialization)
                    Binding::EncodedLit {
                        o_kind,
                        o_key,
                        p_id,
                        dt_id,
                        lang_id,
                        ..
                    } => {
                        // Check if it's an inline GeoPoint (ObjKind::GEO_POINT = 0x14)
                        if *o_kind == fluree_db_core::ObjKind::GEO_POINT.as_u8() {
                            // Decode GeoPoint directly from o_key
                            let (lat, lng) =
                                fluree_db_core::ObjKey::from_u64(*o_key).decode_geo_point();
                            return Some(QueryGeomResolved::Point(lat, lng));
                        }

                        // Decode the literal using the binary store
                        let gv = ctx.graph_view()?;
                        match gv.decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id) {
                            Ok(fluree_db_core::FlakeValue::String(s)) => {
                                Some(QueryGeomResolved::Wkt(s.to_string()))
                            }
                            Ok(fluree_db_core::FlakeValue::GeoPoint(bits)) => {
                                // Decoded to GeoPoint
                                let (lat, lng) = fluree_db_core::ObjKey::from_u64(bits.as_u64())
                                    .decode_geo_point();
                                Some(QueryGeomResolved::Point(lat, lng))
                            }
                            Ok(_) => {
                                tracing::debug!(
                                    o_kind = *o_kind,
                                    "S2Search: EncodedLit decoded to non-geometry value"
                                );
                                None
                            }
                            Err(e) => {
                                tracing::debug!(
                                    o_kind = *o_kind,
                                    o_key = *o_key,
                                    error = %e,
                                    "S2Search: failed to decode EncodedLit"
                                );
                                None
                            }
                        }
                    }
                    _ => None, // IRI, Sid, etc. are not valid geometry values
                }
            }
        }
    }
}

/// Resolved query geometry for execution.
#[derive(Debug)]
enum QueryGeomResolved {
    Wkt(String),
    Point(f64, f64),
}

impl QueryGeomResolved {
    /// Convert to geo_types::Geometry.
    fn to_geometry(&self) -> std::result::Result<geo_types::Geometry<f64>, String> {
        use std::str::FromStr;
        match self {
            QueryGeomResolved::Wkt(wkt) => wkt::Wkt::from_str(wkt)
                .map_err(|e| format!("WKT parse error: {e:?}"))
                .and_then(|w| {
                    w.try_into()
                        .map_err(|e: wkt::conversion::Error| format!("WKT conversion error: {e:?}"))
                }),
            QueryGeomResolved::Point(lat, lng) => Ok(geo_types::Geometry::Point(
                geo_types::Point::new(*lng, *lat),
            )),
        }
    }
}

#[async_trait]
impl Operator for S2SearchOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;

        // Validate variable target is available from child if needed
        if let S2QueryGeom::Var(v) = &self.pattern.query_geom {
            if !self.child.schema().iter().any(|vv| vv == v) {
                return Err(QueryError::InvalidQuery(format!(
                    "S2Search query geometry variable {v:?} is not bound by previous patterns"
                )));
            }
        }

        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

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

        // Get spatial index provider from context based on predicate
        let providers = match ctx.spatial_providers {
            Some(p) => p,
            None => {
                let op_name = match self.pattern.operation {
                    S2SpatialOp::Within => "within",
                    S2SpatialOp::Contains => "contains",
                    S2SpatialOp::Intersects => "intersects",
                    S2SpatialOp::Nearby { .. } => "nearby",
                };

                tracing::debug!(
                    operation = op_name,
                    "S2Search operator: spatial index providers not available, returning empty results"
                );
                return Ok(Some(Batch::empty(self.in_schema.clone())?));
            }
        };

        // Route to provider based on (graph, predicate) key
        // Key format: "g{g_id}:{predicate_iri}" for graph-scoped routing
        let g_id = ctx.binary_g_id;
        let provider: &dyn SpatialIndexProvider = match &self.pattern.predicate {
            Some(pred_iri) => {
                // Look up by graph-scoped key: "g{g_id}:{predicate}"
                let key = format!("g{g_id}:{pred_iri}");
                if let Some(p) = providers.get(&key) {
                    p.as_ref()
                } else if g_id != 0 {
                    // Fallback: try default graph (g_id=0) for backwards compat
                    let fallback_key = format!("g0:{pred_iri}");
                    if let Some(p) = providers.get(&fallback_key) {
                        tracing::debug!(
                            key = %key,
                            fallback_key = %fallback_key,
                            "S2Search: using fallback to default graph index"
                        );
                        p.as_ref()
                    } else {
                        return Err(QueryError::InvalidQuery(format!(
                            "S2Search: no spatial index for key '{}'. Available: {:?}",
                            key,
                            providers.keys().collect::<Vec<_>>()
                        )));
                    }
                } else {
                    return Err(QueryError::InvalidQuery(format!(
                        "S2Search: no spatial index for key '{}'. Available: {:?}",
                        key,
                        providers.keys().collect::<Vec<_>>()
                    )));
                }
            }
            None => {
                // No predicate specified - filter to current graph first
                let graph_prefix = format!("g{g_id}:");
                let graph_keys: Vec<_> = providers
                    .keys()
                    .filter(|k| k.starts_with(&graph_prefix))
                    .collect();

                if graph_keys.len() == 1 {
                    // Only one provider for this graph, use it
                    providers.get(graph_keys[0]).unwrap().as_ref()
                } else if graph_keys.is_empty() {
                    // No providers for this graph - try fallback to default graph (g_id=0)
                    if g_id != 0 {
                        let default_prefix = "g0:";
                        let default_keys: Vec<_> = providers
                            .keys()
                            .filter(|k| k.starts_with(default_prefix))
                            .collect();
                        if default_keys.len() == 1 {
                            tracing::debug!(
                                g_id = g_id,
                                fallback = default_keys[0],
                                "S2Search: no providers for graph, using default graph"
                            );
                            providers.get(default_keys[0]).unwrap().as_ref()
                        } else if default_keys.is_empty() {
                            tracing::debug!(
                                "S2Search operator: no spatial providers for graph, returning empty results"
                            );
                            return Ok(Some(Batch::empty(self.in_schema.clone())?));
                        } else {
                            // Multiple default providers - sort and use first
                            let mut sorted: Vec<_> = default_keys.into_iter().collect();
                            sorted.sort();
                            providers.get(sorted[0]).unwrap().as_ref()
                        }
                    } else {
                        tracing::debug!(
                            "S2Search operator: no spatial providers for graph, returning empty results"
                        );
                        return Ok(Some(Batch::empty(self.in_schema.clone())?));
                    }
                } else {
                    // Multiple providers for this graph - sort keys deterministically and use first
                    let mut sorted: Vec<_> = graph_keys.into_iter().collect();
                    sorted.sort();
                    let first_key = sorted[0];
                    tracing::debug!(
                        selected_key = %first_key,
                        available_count = sorted.len(),
                        "S2Search: no property specified, using first provider alphabetically"
                    );
                    providers.get(first_key).unwrap().as_ref()
                }
            }
        };

        let child_schema = self.child.schema();
        let child_cols: Vec<&[Binding]> = (0..child_schema.len())
            .map(|i| input_batch.column_by_idx(i).expect("schema mismatch"))
            .collect();

        // Accumulate output columns
        let num_cols = self.in_schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();

        // Get output positions
        let subject_pos = *self.out_pos.get(&self.pattern.subject_var).unwrap();
        let distance_pos = self
            .pattern
            .distance_var
            .and_then(|v| self.out_pos.get(&v).copied());

        // Process each input row.
        // We use `enumerate()` to keep `row_idx` for indexing all `child_cols` uniformly.
        for (row_idx, row_view) in input_batch.rows().enumerate() {
            // Resolve query geometry for this row
            let Some(query_geom) = self.resolve_query_geom(Some(&row_view), ctx) else {
                continue;
            };

            // Execute the spatial query based on operation type
            match &self.pattern.operation {
                S2SpatialOp::Nearby { radius_meters } => {
                    // For nearby, query geometry must be a point
                    let (lat, lng) = match &query_geom {
                        QueryGeomResolved::Point(lat, lng) => (*lat, *lng),
                        QueryGeomResolved::Wkt(_) => {
                            // Try to extract centroid for nearby query
                            match query_geom.to_geometry() {
                                Ok(geom) => {
                                    use geo::Centroid;
                                    if let Some(c) = geom.centroid() {
                                        (c.y(), c.x())
                                    } else {
                                        continue;
                                    }
                                }
                                Err(_) => continue,
                            }
                        }
                    };

                    // Query radius
                    let results = provider
                        .query_radius(lat, lng, *radius_meters, ctx.to_t, self.pattern.limit)
                        .await
                        .map_err(|e| {
                            QueryError::Internal(format!("S2 radius query failed: {e}"))
                        })?;

                    // Build output rows
                    for result in results {
                        let mut row = vec![Binding::Unbound; num_cols];

                        // Copy child columns
                        for (col_idx, &var) in child_schema.iter().enumerate() {
                            if let Some(&out_idx) = self.out_pos.get(&var) {
                                row[out_idx] = child_cols[col_idx][row_idx].clone();
                            }
                        }

                        // Add subject binding (encoded for late materialization)
                        row[subject_pos] = Binding::encoded_sid(result.subject_id);

                        // Add distance binding if requested
                        if let Some(dist_pos) = distance_pos {
                            row[dist_pos] = Binding::lit(
                                fluree_db_core::FlakeValue::Double(result.distance),
                                fluree_db_core::Sid::new(
                                    fluree_vocab::namespaces::XSD,
                                    fluree_vocab::xsd_names::DOUBLE,
                                ),
                            );
                        }

                        for (col_idx, binding) in row.into_iter().enumerate() {
                            columns[col_idx].push(binding);
                        }
                    }
                }

                S2SpatialOp::Within => {
                    let geom = query_geom.to_geometry().map_err(|e| {
                        QueryError::InvalidQuery(format!("Invalid query geometry: {e}"))
                    })?;

                    let results = provider
                        .query_within(&geom, ctx.to_t, self.pattern.limit)
                        .await
                        .map_err(|e| {
                            QueryError::Internal(format!("S2 within query failed: {e}"))
                        })?;

                    for result in results {
                        let mut row = vec![Binding::Unbound; num_cols];

                        for (col_idx, &var) in child_schema.iter().enumerate() {
                            if let Some(&out_idx) = self.out_pos.get(&var) {
                                row[out_idx] = child_cols[col_idx][row_idx].clone();
                            }
                        }

                        row[subject_pos] = Binding::encoded_sid(result.subject_id);

                        for (col_idx, binding) in row.into_iter().enumerate() {
                            columns[col_idx].push(binding);
                        }
                    }
                }

                S2SpatialOp::Contains => {
                    let geom = query_geom.to_geometry().map_err(|e| {
                        QueryError::InvalidQuery(format!("Invalid query geometry: {e}"))
                    })?;

                    let results = provider
                        .query_contains(&geom, ctx.to_t, self.pattern.limit)
                        .await
                        .map_err(|e| {
                            QueryError::Internal(format!("S2 contains query failed: {e}"))
                        })?;

                    for result in results {
                        let mut row = vec![Binding::Unbound; num_cols];

                        for (col_idx, &var) in child_schema.iter().enumerate() {
                            if let Some(&out_idx) = self.out_pos.get(&var) {
                                row[out_idx] = child_cols[col_idx][row_idx].clone();
                            }
                        }

                        row[subject_pos] = Binding::encoded_sid(result.subject_id);

                        for (col_idx, binding) in row.into_iter().enumerate() {
                            columns[col_idx].push(binding);
                        }
                    }
                }

                S2SpatialOp::Intersects => {
                    let geom = query_geom.to_geometry().map_err(|e| {
                        QueryError::InvalidQuery(format!("Invalid query geometry: {e}"))
                    })?;

                    let results = provider
                        .query_intersects(&geom, ctx.to_t, self.pattern.limit)
                        .await
                        .map_err(|e| {
                            QueryError::Internal(format!("S2 intersects query failed: {e}"))
                        })?;

                    for result in results {
                        let mut row = vec![Binding::Unbound; num_cols];

                        for (col_idx, &var) in child_schema.iter().enumerate() {
                            if let Some(&out_idx) = self.out_pos.get(&var) {
                                row[out_idx] = child_cols[col_idx][row_idx].clone();
                            }
                        }

                        row[subject_pos] = Binding::encoded_sid(result.subject_id);

                        for (col_idx, binding) in row.into_iter().enumerate() {
                            columns[col_idx].push(binding);
                        }
                    }
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
        // S2 search typically returns a subset based on spatial filtering
        Some(100)
    }
}

#[cfg(test)]
mod tests {

    use crate::ir::{S2QueryGeom, S2SearchPattern, S2SpatialOp};
    use crate::var_registry::VarRegistry;

    #[test]
    fn test_s2_search_pattern_variables() {
        let mut reg = VarRegistry::new();
        let subject = reg.get_or_insert("?building");
        let distance = reg.get_or_insert("?dist");

        let pattern = S2SearchPattern::nearby(
            subject,
            S2QueryGeom::Point {
                lat: 40.7128,
                lng: -74.0060,
            },
            1000.0,
        )
        .with_distance_var(distance);

        let vars = pattern.variables();
        assert!(vars.contains(&subject));
        assert!(vars.contains(&distance));
    }

    #[test]
    fn test_s2_search_pattern_creation() {
        let mut reg = VarRegistry::new();
        let subject = reg.get_or_insert("?place");

        let within = S2SearchPattern::within(
            subject,
            S2QueryGeom::Wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))".into()),
        );
        assert_eq!(within.operation, S2SpatialOp::Within);

        let contains =
            S2SearchPattern::contains(subject, S2QueryGeom::Point { lat: 0.5, lng: 0.5 });
        assert_eq!(contains.operation, S2SpatialOp::Contains);

        let intersects =
            S2SearchPattern::intersects(subject, S2QueryGeom::Wkt("LINESTRING(0 0, 1 1)".into()));
        assert_eq!(intersects.operation, S2SpatialOp::Intersects);
    }
}
