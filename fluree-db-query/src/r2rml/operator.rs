//! R2RML Scan Operator
//!
//! This operator executes R2RML scans against Iceberg tables and emits
//! RDF term bindings according to the mapping specification.
//!
//! # Design
//!
//! The operator is correlated: it consumes a child stream (often an EmptyOperator seed)
//! and for each input row, scans the appropriate Iceberg table(s) and materializes
//! RDF terms according to the TriplesMap definition.
//!
//! # Execution Flow
//!
//! 1. `open()`: Load the compiled R2RML mapping from the provider
//! 2. `next_batch()`: For each input row:
//!    - Scan the logical table from the TriplesMap
//!    - For RefObjectMap joins, scan parent tables and build lookup indexes
//!    - Materialize subject/predicate/object terms
//!    - Emit bindings for query variables
//! 3. `close()`: Release resources
//!
//! # RefObjectMap Join Execution
//!
//! When a PredicateObjectMap contains a RefObjectMap (referencing a parent TriplesMap),
//! the operator:
//!
//! 1. Scans the parent TriplesMap's table
//! 2. Builds a hash lookup: parent join key → parent subject IRI
//! 3. For each child row, extracts child join key values
//! 4. Looks up the parent subject IRI from the hash map
//! 5. Emits the parent IRI as the object binding

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::group_aggregate::{binding_to_group_key_normalized, GroupKeyOwned};
use crate::ir::R2rmlPattern;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, ObjectMap, PredicateObjectMap, TriplesMap};
use fluree_db_r2rml::materialize::{
    get_join_key_from_batch, materialize_object_from_batch, materialize_subject_from_batch, RdfTerm,
};
use fluree_db_tabular::ColumnBatch;
use fluree_vocab::xsd;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Lookup table for RefObjectMap joins.
///
/// Maps parent join key (as `Vec<String>`) to materialized parent subject IRI.
/// The key is a composite key of all parent columns specified in join conditions.
pub type ParentLookup = HashMap<Vec<String>, RdfTerm>;

/// R2RML scan operator for `Pattern::R2rml`.
///
/// Scans an Iceberg table through an R2RML mapping and produces RDF term bindings.
pub struct R2rmlScanOperator {
    /// Child operator providing input solutions (may be EmptyOperator seed)
    child: BoxedOperator,
    /// R2RML pattern from the query IR
    pattern: R2rmlPattern,
    /// Output schema (child schema + new vars from R2RML scan)
    schema: Arc<[VarId]>,
    /// Mapping from variables to output column positions
    out_pos: HashMap<VarId, usize>,
    /// Cached compiled mapping (loaded once in open)
    mapping: Option<Arc<CompiledR2rmlMapping>>,
    /// Pending output rows from current scan
    pending: VecDeque<Vec<Binding>>,
    /// State
    state: OperatorState,
}

impl R2rmlScanOperator {
    /// Create a new R2RML scan operator.
    pub fn new(child: BoxedOperator, pattern: R2rmlPattern) -> Self {
        let child_schema = child.schema();

        // Build output schema: start with child vars, then add R2RML pattern vars
        let mut schema_vars: Vec<VarId> = child_schema.to_vec();
        let mut seen: HashSet<VarId> = schema_vars.iter().copied().collect();

        // Add subject variable if new
        if seen.insert(pattern.subject_var) {
            schema_vars.push(pattern.subject_var);
        }

        // Add object variable if present and new
        if let Some(obj_var) = pattern.object_var {
            if seen.insert(obj_var) {
                schema_vars.push(obj_var);
            }
        }

        // Add same-subject star object variables if new
        for (_, var) in &pattern.star_bindings {
            if seen.insert(*var) {
                schema_vars.push(*var);
            }
        }

        // Build output position map
        let out_pos: HashMap<VarId, usize> = schema_vars
            .iter()
            .enumerate()
            .map(|(i, &v)| (v, i))
            .collect();

        let schema = Arc::from(schema_vars);

        Self {
            child,
            pattern,
            schema,
            out_pos,
            mapping: None,
            pending: VecDeque::new(),
            state: OperatorState::Created,
        }
    }

    /// All predicate IRIs this pattern materializes: the base `predicate_filter`
    /// plus any same-subject star members. Used for projection and parent-lookup
    /// building so a star scan reads every needed column in one pass.
    fn pattern_predicates(&self) -> Vec<&str> {
        let mut preds: Vec<&str> = Vec::new();
        if let Some(p) = self.pattern.predicate_filter.as_deref() {
            preds.push(p);
        }
        for (pred, _) in &self.pattern.star_bindings {
            preds.push(pred.as_str());
        }
        preds
    }

    /// Resolve this pattern's pushdown predicates (keyed by query variable) to
    /// table columns for the given TriplesMap, producing scan filters. A
    /// variable maps to a column via its predicate IRI; predicates backed by a
    /// RefObjectMap (an IRI join, not a scalar column) are skipped.
    fn build_scan_filters(&self, triples_map: &TriplesMap) -> Vec<crate::r2rml::ScanFilter> {
        let mut out = Vec::new();
        for pd in &self.pattern.scan_filters {
            let pred_iri = if Some(pd.var) == self.pattern.object_var {
                self.pattern.predicate_filter.as_deref()
            } else {
                self.pattern
                    .star_bindings
                    .iter()
                    .find(|(_, v)| *v == pd.var)
                    .map(|(p, _)| p.as_str())
            };
            let Some(pred_iri) = pred_iri else { continue };

            // The predicate's values come from EVERY matching object map, so a
            // file-level prune is only sound when the predicate maps to exactly
            // one scalar object map backed by exactly one column. Otherwise a row
            // could match via a column we didn't prune on — skip the pushdown and
            // let the in-engine FILTER handle it.
            let mut matching = triples_map
                .predicate_object_maps
                .iter()
                .filter(|p| p.predicate_map.as_constant() == Some(pred_iri));
            let (Some(pom), None) = (matching.next(), matching.next()) else {
                continue;
            };
            if matches!(pom.object_map, ObjectMap::RefObjectMap(_)) {
                continue;
            }
            let cols = pom.object_map.referenced_columns();
            let [col] = cols.as_slice() else {
                continue;
            };
            out.push(crate::r2rml::ScanFilter {
                column: (*col).to_string(),
                op: pd.op,
                value: pd.value.clone(),
            });
        }
        out
    }

    /// Materialize all produced rows of a TriplesMap (independent of the child
    /// stream) so the result can be hash-joined against the child. Datatype Sids
    /// are resolved once into a `LiteralEncoder` (not per cell) and the data
    /// files are materialized in parallel on the rayon pool.
    fn materialize_produced_rows(
        &self,
        triples_map: &TriplesMap,
        batches: &[ColumnBatch],
        parent_lookups: &HashMap<(String, Vec<String>), ParentLookup>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Vec<(VarId, Binding)>>> {
        use rayon::prelude::*;
        let encoder = LiteralEncoder::build(triples_map, ctx.active_snapshot);
        let pattern = &self.pattern;
        let per_batch: Vec<Vec<Vec<(VarId, Binding)>>> = batches
            .par_iter()
            .map(|batch| materialize_batch(pattern, triples_map, batch, parent_lookups, &encoder))
            .collect::<Result<Vec<_>>>()?;
        Ok(per_batch.into_iter().flatten().collect())
    }
}

/// Datatype Sids resolved once per scan instead of per literal cell — the
/// per-cell `encode_iri` was a large share of materialization cost.
struct LiteralEncoder {
    dt_sids: HashMap<String, fluree_db_core::Sid>,
    xsd_string: fluree_db_core::Sid,
}

impl LiteralEncoder {
    fn build(triples_map: &TriplesMap, snapshot: &fluree_db_core::LedgerSnapshot) -> Self {
        let fallback = fluree_db_core::Sid::new(2, "string");
        let mut dt_sids: HashMap<String, fluree_db_core::Sid> = HashMap::new();
        for pom in &triples_map.predicate_object_maps {
            if let Some(dt) = object_map_datatype(&pom.object_map) {
                dt_sids
                    .entry(dt.to_string())
                    .or_insert_with(|| snapshot.encode_iri(dt).unwrap_or_else(|| fallback.clone()));
            }
        }
        let xsd_string = snapshot.encode_iri(xsd::STRING).unwrap_or(fallback);
        Self {
            dt_sids,
            xsd_string,
        }
    }

    /// Convert an RdfTerm to a Binding without touching the snapshot (datatype
    /// Sids are pre-resolved). IRIs are kept as raw strings — graph source IRIs
    /// are independent of any Fluree namespace table.
    fn encode(&self, term: &RdfTerm) -> Binding {
        use fluree_db_core::FlakeValue;
        use fluree_vocab::UnresolvedDatatypeConstraint as Udc;
        match term {
            RdfTerm::Iri(iri) => Binding::iri(iri.as_str()),
            RdfTerm::BlankNode(id) => Binding::iri(format!("_:{id}")),
            RdfTerm::Literal { value, dtc } => match dtc {
                Some(Udc::LangTag(lang)) => {
                    Binding::lit_lang(FlakeValue::String(value.clone()), lang.as_ref())
                }
                Some(Udc::Explicit(dt_iri)) => {
                    let dt_sid = self
                        .dt_sids
                        .get(dt_iri.as_ref())
                        .cloned()
                        .unwrap_or_else(|| self.xsd_string.clone());
                    // Coerce numeric XSD literals from string to typed FlakeValue
                    // (arithmetic reads the value, not the datatype Sid);
                    // non-numeric datatypes keep their string form.
                    let val = match fluree_db_core::coerce_value(
                        FlakeValue::String(value.clone()),
                        dt_iri.as_ref(),
                    ) {
                        Ok(
                            c @ (FlakeValue::Long(_)
                            | FlakeValue::Double(_)
                            | FlakeValue::BigInt(_)
                            | FlakeValue::Decimal(_)),
                        ) => c,
                        _ => FlakeValue::String(value.clone()),
                    };
                    Binding::lit(val, dt_sid)
                }
                _ => Binding::lit(FlakeValue::String(value.clone()), self.xsd_string.clone()),
            },
        }
    }
}

/// Datatype IRI declared by an ObjectMap, if any (column/template/constant).
fn object_map_datatype(om: &ObjectMap) -> Option<&str> {
    use fluree_db_r2rml::mapping::ConstantValue;
    match om {
        ObjectMap::Column { datatype, .. } | ObjectMap::Template { datatype, .. } => {
            datatype.as_deref()
        }
        ObjectMap::Constant {
            value: ConstantValue::Literal { datatype, .. },
        } => datatype.as_deref(),
        _ => None,
    }
}

/// Materialize the object term for one POM at a table row, resolving a
/// RefObjectMap through the pre-built parent lookup. Free fn so it runs off the
/// operator inside a rayon worker.
fn materialize_pom_object(
    pom: &PredicateObjectMap,
    iceberg_batch: &ColumnBatch,
    table_row_idx: usize,
    parent_lookups: &HashMap<(String, Vec<String>), ParentLookup>,
) -> Result<Option<RdfTerm>> {
    if let ObjectMap::RefObjectMap(ref rom) = pom.object_map {
        let child_columns: Vec<String> = rom
            .child_columns()
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect();
        let child_key = match get_join_key_from_batch(&child_columns, iceberg_batch, table_row_idx)
        {
            Some(k) => k,
            None => return Ok(None),
        };
        let mut parent_join_cols: Vec<String> = rom
            .parent_columns()
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect();
        parent_join_cols.sort();
        let lookup_key = (rom.parent_triples_map.clone(), parent_join_cols);
        Ok(parent_lookups
            .get(&lookup_key)
            .and_then(|l| l.get(&child_key))
            .cloned())
    } else {
        Ok(materialize_object_from_batch(
            &pom.object_map,
            iceberg_batch,
            table_row_idx,
        )?)
    }
}

/// Materialize one column batch into produced variable assignments (subject +
/// object vars) — the per-batch unit of the parallel scan. Mirrors the previous
/// per-row logic (star cross product, subject-only, single-object).
fn materialize_batch(
    pattern: &R2rmlPattern,
    triples_map: &TriplesMap,
    iceberg_batch: &ColumnBatch,
    parent_lookups: &HashMap<(String, Vec<String>), ParentLookup>,
    encoder: &LiteralEncoder,
) -> Result<Vec<Vec<(VarId, Binding)>>> {
    let mut produced: Vec<Vec<(VarId, Binding)>> = Vec::new();
    for table_row_idx in 0..iceberg_batch.num_rows {
        let subject_term = match materialize_subject_from_batch(
            &triples_map.subject_map,
            iceberg_batch,
            table_row_idx,
        )? {
            Some(t) => t,
            None => continue,
        };
        let subject_binding = encoder.encode(&subject_term);

        if !pattern.star_bindings.is_empty() {
            let mut members: Vec<(VarId, &str)> = Vec::new();
            if let (Some(ov), Some(pf)) = (pattern.object_var, pattern.predicate_filter.as_deref())
            {
                members.push((ov, pf));
            }
            for (pred, var) in &pattern.star_bindings {
                members.push((*var, pred.as_str()));
            }

            let mut binding_lists: Vec<(VarId, Vec<Binding>)> = Vec::with_capacity(members.len());
            let mut row_ok = true;
            for (var, pred) in &members {
                let mut vals: Vec<Binding> = Vec::new();
                for pom in triples_map
                    .predicate_object_maps
                    .iter()
                    .filter(|p| p.predicate_map.as_constant() == Some(*pred))
                {
                    if let Some(t) =
                        materialize_pom_object(pom, iceberg_batch, table_row_idx, parent_lookups)?
                    {
                        vals.push(encoder.encode(&t));
                    }
                }
                if vals.is_empty() {
                    row_ok = false;
                    break;
                }
                binding_lists.push((*var, vals));
            }
            if !row_ok {
                continue;
            }

            let mut rows: Vec<Vec<(VarId, Binding)>> =
                vec![vec![(pattern.subject_var, subject_binding.clone())]];
            for (var, vals) in &binding_lists {
                if vals.len() == 1 {
                    for r in &mut rows {
                        r.push((*var, vals[0].clone()));
                    }
                } else {
                    let mut next = Vec::with_capacity(rows.len() * vals.len());
                    for r in &rows {
                        for v in vals {
                            let mut nr = r.clone();
                            nr.push((*var, v.clone()));
                            next.push(nr);
                        }
                    }
                    rows = next;
                }
            }
            produced.extend(rows);
            continue;
        }

        let Some(obj_var) = pattern.object_var else {
            produced.push(vec![(pattern.subject_var, subject_binding)]);
            continue;
        };

        for pom in triples_map.predicate_object_maps.iter().filter(|pom| {
            pattern
                .predicate_filter
                .as_deref()
                .is_none_or(|pf| pom.predicate_map.as_constant() == Some(pf))
        }) {
            if let Some(t) =
                materialize_pom_object(pom, iceberg_batch, table_row_idx, parent_lookups)?
            {
                let object_binding = encoder.encode(&t);
                produced.push(vec![
                    (pattern.subject_var, subject_binding.clone()),
                    (obj_var, object_binding),
                ]);
            }
        }
    }
    Ok(produced)
}

/// Build a parent lookup table for RefObjectMap joins.
///
/// Scans the parent TriplesMap's table and builds a HashMap mapping
/// parent join key → parent subject IRI.
///
/// # Arguments
///
/// * `parent_tm` - The parent TriplesMap
/// * `parent_columns` - Column names used in join conditions (from parent side)
/// * `batches` - Column batches from scanning the parent table
///
/// # Returns
///
/// HashMap mapping join key (as `Vec<String>`) to parent subject `RdfTerm`.
fn build_parent_lookup(
    parent_tm: &TriplesMap,
    parent_columns: &[String],
    batches: Vec<ColumnBatch>,
) -> Result<ParentLookup> {
    let mut lookup = ParentLookup::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows {
            // Materialize parent subject
            let subject_term =
                match materialize_subject_from_batch(&parent_tm.subject_map, &batch, row_idx) {
                    Ok(Some(term)) => term,
                    Ok(None) => continue, // Null subject - skip
                    Err(e) => {
                        tracing::warn!(
                            parent_tm = %parent_tm.iri,
                            row_idx,
                            error = %e,
                            "Failed to materialize parent subject, skipping row"
                        );
                        continue;
                    }
                };

            // Extract join key from parent row
            let key = match get_join_key_from_batch(parent_columns, &batch, row_idx) {
                Some(k) => k,
                None => continue, // Null in join key - skip
            };

            // Insert into lookup (last wins for duplicate keys)
            lookup.insert(key, subject_term);
        }
    }

    tracing::debug!(
        parent_tm = %parent_tm.iri,
        lookup_size = lookup.len(),
        "Built parent lookup table for RefObjectMap join"
    );

    Ok(lookup)
}

#[async_trait]
impl Operator for R2rmlScanOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        // Open child first
        self.child.open(ctx).await?;

        // Load the compiled mapping from the provider
        let provider = ctx
            .r2rml_provider
            .ok_or_else(|| QueryError::InvalidQuery("R2RML provider not configured".to_string()))?;

        // IMPORTANT: In dataset mode, there is no meaningful dataset-level `to_t`.
        // Passing `None` avoids inventing a cross-ledger time and lets the provider
        // select the latest snapshot (or apply its own semantics).
        let as_of_t = if ctx.dataset.is_some() {
            None
        } else {
            Some(ctx.to_t)
        };
        let mapping = provider
            .compiled_mapping(&self.pattern.graph_source_id, as_of_t)
            .await?;

        self.mapping = Some(mapping);
        self.state = OperatorState::Open;

        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state == OperatorState::Exhausted {
            return Ok(None);
        }

        let mapping = self
            .mapping
            .as_ref()
            .ok_or_else(|| QueryError::Internal("R2RML mapping not loaded".to_string()))?;

        let child_schema = self.child.schema().to_vec();
        let num_cols = self.schema.len();

        // Prepare output columns
        let mut columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(ctx.batch_size))
            .collect();

        // Process pending rows first
        while !self.pending.is_empty() && columns[0].len() < ctx.batch_size {
            if let Some(row) = self.pending.pop_front() {
                for (col_idx, binding) in row.into_iter().enumerate() {
                    columns[col_idx].push(binding);
                }
            }
        }

        // If we've filled the batch from pending, return it
        if columns[0].len() >= ctx.batch_size {
            return Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?));
        }

        // Pull more input from child
        while columns[0].len() < ctx.batch_size {
            let child_batch = match self.child.next_batch(ctx).await? {
                Some(batch) => batch,
                None => {
                    // Child exhausted
                    if columns[0].is_empty() {
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                    break;
                }
            };

            // Get the TriplesMap(s) for this pattern (same for all child rows)
            let triples_maps: Vec<_> = if let Some(ref tm_iri) = self.pattern.triples_map_iri {
                // Explicit TriplesMap IRI specified
                let tm = mapping.get(tm_iri).ok_or_else(|| {
                    QueryError::InvalidQuery(format!(
                        "TriplesMap '{tm_iri}' not found in R2RML mapping"
                    ))
                })?;
                vec![tm]
            } else {
                // Find TriplesMap(s) by class and/or predicate filter
                let all_maps: Vec<_> = mapping.triples_maps.values().collect();

                let filtered: Vec<_> = all_maps
                    .into_iter()
                    .filter(|tm| {
                        // Apply class_filter: only include maps that produce this class
                        if let Some(ref class_filter) = self.pattern.class_filter {
                            if !tm.classes().contains(class_filter) {
                                return false;
                            }
                        }
                        // Apply predicate_filter: only include maps that have this predicate
                        if let Some(ref pred_filter) = self.pattern.predicate_filter {
                            let has_pred = tm.predicate_object_maps.iter().any(|pom| {
                                pom.predicate_map.as_constant() == Some(pred_filter.as_str())
                            });
                            if !has_pred {
                                return false;
                            }
                        }
                        true
                    })
                    .collect();

                filtered
            };

            if triples_maps.is_empty() {
                // No matching TriplesMap found - skip this batch
                continue;
            }

            // Get the table provider (same for all child rows)
            let table_provider = ctx.r2rml_table_provider.ok_or_else(|| {
                QueryError::InvalidQuery("R2RML table provider not configured".to_string())
            })?;

            // Type for composite lookup cache key: (parent_tm_iri, sorted_parent_join_cols)
            // This ensures different join conditions on the same parent TM get separate lookups.
            type LookupCacheKey = (String, Vec<String>);

            // Pre-scan tables and build lookups for each TriplesMap (outside row loop)
            // Stores: (TriplesMap IRI) -> (batches, parent_lookups)
            let mut tm_scan_cache: HashMap<
                String,
                (Vec<ColumnBatch>, HashMap<LookupCacheKey, ParentLookup>),
            > = HashMap::new();

            for triples_map in &triples_maps {
                if tm_scan_cache.contains_key(&triples_map.iri) {
                    continue;
                }

                let table_name = triples_map.table_name().ok_or_else(|| {
                    QueryError::InvalidQuery("TriplesMap has no logical table".to_string())
                })?;

                // Determine projection columns. For a same-subject star, project
                // the union of columns needed for every star predicate so the
                // whole star is satisfied by one scan.
                let projection: Vec<String> = if self.pattern.star_bindings.is_empty() {
                    triples_map
                        .columns_for_predicate(self.pattern.predicate_filter.as_deref())
                        .into_iter()
                        .map(std::string::ToString::to_string)
                        .collect()
                } else {
                    let mut cols: Vec<String> = Vec::new();
                    for pred in self.pattern_predicates() {
                        cols.extend(
                            triples_map
                                .columns_for_predicate(Some(pred))
                                .into_iter()
                                .map(std::string::ToString::to_string),
                        );
                    }
                    cols.sort();
                    cols.dedup();
                    cols
                };

                // Scan the table, pushing resolved FILTER predicates for file
                // pruning (column resolution needs the mapping, so it happens here).
                let scan_filters = self.build_scan_filters(triples_map);
                let as_of_t = if ctx.dataset.is_some() {
                    None
                } else {
                    Some(ctx.to_t)
                };
                let batches = table_provider
                    .scan_table(
                        &self.pattern.graph_source_id,
                        table_name,
                        &projection,
                        &scan_filters,
                        as_of_t,
                    )
                    .await?;

                // Build parent lookup tables for RefObjectMap POMs that match predicate_filter
                // Key: (parent_tm_iri, parent_join_cols) -> ParentLookup
                let mut parent_lookups: HashMap<LookupCacheKey, ParentLookup> = HashMap::new();

                // Only process POMs that pass the predicate filter. For a star,
                // include POMs for any of the star predicates so RefObjectMap
                // lookups are available during the wide-row emit below.
                let star_preds = self.pattern_predicates();
                let filtered_poms: Vec<_> = triples_map
                    .predicate_object_maps
                    .iter()
                    .filter(|pom| {
                        if !self.pattern.star_bindings.is_empty() {
                            pom.predicate_map
                                .as_constant()
                                .is_some_and(|p| star_preds.contains(&p))
                        } else if let Some(ref pred_filter) = self.pattern.predicate_filter {
                            pom.predicate_map.as_constant() == Some(pred_filter.as_str())
                        } else {
                            true
                        }
                    })
                    .collect();

                for pom in &filtered_poms {
                    if let ObjectMap::RefObjectMap(ref rom) = pom.object_map {
                        // Build composite cache key: (parent_tm_iri, sorted_parent_join_cols)
                        let mut parent_join_cols: Vec<String> = rom
                            .parent_columns()
                            .into_iter()
                            .map(std::string::ToString::to_string)
                            .collect();
                        parent_join_cols.sort(); // Normalize for consistent key
                        let lookup_key: LookupCacheKey =
                            (rom.parent_triples_map.clone(), parent_join_cols.clone());

                        // Skip if we already built this exact lookup
                        if parent_lookups.contains_key(&lookup_key) {
                            continue;
                        }

                        // Get the parent TriplesMap
                        let parent_tm = match mapping.get(&rom.parent_triples_map) {
                            Some(tm) => tm,
                            None => {
                                tracing::warn!(
                                    parent = %rom.parent_triples_map,
                                    "Parent TriplesMap not found for RefObjectMap, skipping"
                                );
                                continue;
                            }
                        };

                        // Get parent table name
                        let parent_table = match parent_tm.table_name() {
                            Some(name) => name,
                            None => {
                                tracing::warn!(
                                    parent = %rom.parent_triples_map,
                                    "Parent TriplesMap has no logical table, skipping"
                                );
                                continue;
                            }
                        };

                        // Determine columns needed from parent table:
                        // - Parent columns from join conditions
                        // - Subject template columns
                        // - Subject column (if using rr:column instead of template)
                        let mut parent_projection: Vec<String> = parent_join_cols.clone();
                        parent_projection
                            .extend(parent_tm.subject_map.template_columns.iter().cloned());
                        // Include rr:column if subject map uses column instead of template
                        if let Some(ref col) = parent_tm.subject_map.column {
                            parent_projection.push(col.clone());
                        }
                        parent_projection.sort();
                        parent_projection.dedup();

                        // Scan the parent table
                        let as_of_t = if ctx.dataset.is_some() {
                            None
                        } else {
                            Some(ctx.to_t)
                        };
                        let parent_batches = table_provider
                            .scan_table(
                                &self.pattern.graph_source_id,
                                parent_table,
                                &parent_projection,
                                &[],
                                as_of_t,
                            )
                            .await?;

                        // Build the lookup
                        let lookup =
                            build_parent_lookup(parent_tm, &parent_join_cols, parent_batches)?;

                        parent_lookups.insert(lookup_key, lookup);
                    }
                }

                tm_scan_cache.insert(triples_map.iri.clone(), (batches, parent_lookups));
            }

            // Materialize each TriplesMap's produced rows ONCE, then hash-join
            // them against the child on the shared variables, instead of
            // re-scanning/re-materializing the table per child row (O(N*M)).
            for triples_map in &triples_maps {
                let (batches, parent_lookups) = match tm_scan_cache.get(&triples_map.iri) {
                    Some(cached) => cached,
                    None => continue,
                };

                let produced =
                    self.materialize_produced_rows(triples_map, batches, parent_lookups, ctx)?;
                if produced.is_empty() {
                    continue;
                }

                // Join vars = pattern-produced vars the child already binds.
                let join_vars: Vec<VarId> = self
                    .pattern
                    .produced_vars()
                    .into_iter()
                    .filter(|v| child_schema.contains(v))
                    .collect();

                // Emit child[row_idx] combined with a produced assignment.
                macro_rules! emit_combined {
                    ($row_idx:expr, $prod:expr) => {{
                        ctx.tracker.consume_fuel(1)?;
                        let mut out_row: Vec<Binding> = vec![Binding::Unbound; num_cols];
                        for (col_idx, &var) in child_schema.iter().enumerate() {
                            let out_idx = *self.out_pos.get(&var).unwrap();
                            out_row[out_idx] =
                                child_batch.column_by_idx(col_idx).unwrap()[$row_idx].clone();
                        }
                        for (var, binding) in $prod {
                            out_row[*self.out_pos.get(var).unwrap()] = binding.clone();
                        }
                        if columns[0].len() < ctx.batch_size {
                            for (col_idx, binding) in out_row.into_iter().enumerate() {
                                columns[col_idx].push(binding);
                            }
                        } else {
                            self.pending.push_back(out_row);
                        }
                    }};
                }

                if join_vars.is_empty() {
                    // No shared vars: cross product (child is usually the seed row).
                    for row_idx in 0..child_batch.len() {
                        for prod in &produced {
                            emit_combined!(row_idx, prod);
                        }
                    }
                } else {
                    // Hash join: index produced rows by the join-var values, then
                    // probe once per child row. O(N + M) instead of O(N * M).
                    let store = ctx.binary_store.as_deref();
                    let gv = ctx.graph_view();
                    let gv = gv.as_ref();

                    // Per-join-var keys for each produced row, aligned with
                    // `produced`. A produced row always binds every pattern var,
                    // so a complete key has len == join_vars.len().
                    let produced_keys: Vec<Vec<GroupKeyOwned>> = produced
                        .iter()
                        .map(|prod| {
                            join_vars
                                .iter()
                                .filter_map(|jv| {
                                    prod.iter()
                                        .find(|(v, _)| v == jv)
                                        .map(|(_, b)| binding_to_group_key_normalized(b, store, gv))
                                })
                                .collect()
                        })
                        .collect();

                    let mut index: HashMap<Vec<GroupKeyOwned>, Vec<usize>> = HashMap::new();
                    for (pi, keys) in produced_keys.iter().enumerate() {
                        if keys.len() == join_vars.len() {
                            index.entry(keys.clone()).or_default().push(pi);
                        }
                    }
                    for row_idx in 0..child_batch.len() {
                        // Build the child's join key. An unbound join var is a
                        // wildcard (SPARQL compatibility: unbound is compatible
                        // with any value, binding from the produced side), so we
                        // keep `None` for it and match the bound positions only.
                        // A poisoned binding can never match, so the row is
                        // dropped.
                        let mut child_key: Vec<Option<GroupKeyOwned>> =
                            Vec::with_capacity(join_vars.len());
                        let mut all_bound = true;
                        let mut poisoned = false;
                        for &jv in &join_vars {
                            let pos = child_schema.iter().position(|&v| v == jv).unwrap();
                            let b = &child_batch.column_by_idx(pos).unwrap()[row_idx];
                            if b.is_poisoned() {
                                poisoned = true;
                                break;
                            }
                            if b.is_bound() {
                                child_key.push(Some(binding_to_group_key_normalized(b, store, gv)));
                            } else {
                                all_bound = false;
                                child_key.push(None);
                            }
                        }
                        if poisoned {
                            continue;
                        }
                        if all_bound {
                            // Fast path: full key, hash probe.
                            let full: Vec<GroupKeyOwned> =
                                child_key.into_iter().map(Option::unwrap).collect();
                            if let Some(matches) = index.get(&full) {
                                for &pi in matches {
                                    emit_combined!(row_idx, &produced[pi]);
                                }
                            }
                        } else {
                            // Partial key: match produced rows agreeing on every
                            // bound join var (unbound positions are wildcards).
                            for (pi, keys) in produced_keys.iter().enumerate() {
                                if keys.len() != join_vars.len() {
                                    continue;
                                }
                                let agrees = child_key
                                    .iter()
                                    .zip(keys.iter())
                                    .all(|(c, p)| c.as_ref().is_none_or(|c| c == p));
                                if agrees {
                                    emit_combined!(row_idx, &produced[pi]);
                                }
                            }
                        }
                    }
                }
            }
        }

        if columns[0].is_empty() {
            self.state = OperatorState::Exhausted;
            Ok(None)
        } else {
            Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?))
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.mapping = None;
        self.pending.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Could use Iceberg table statistics in the future
        None
    }
}
