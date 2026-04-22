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
use crate::ir::R2rmlPattern;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, ObjectMap, TriplesMap};
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

    /// Convert an RdfTerm to a Binding.
    ///
    /// This is where we bridge R2RML materialized terms to the query engine's
    /// binding representation.
    ///
    /// # Graph Source IRI Handling
    ///
    /// IRIs generated from R2RML templates are kept as raw strings (`Binding::Iri`)
    /// rather than being encoded to SIDs. This is because:
    ///
    /// 1. Graph source IRIs are dynamically generated and may not exist in any Fluree ledger
    /// 2. Cross-ledger SIDs don't match anyway (different namespace codes)
    /// 3. Encoding would silently drop rows for IRIs not in namespace table
    ///
    /// This matches the legacy implementation which uses `match-iri` for graph source results.
    fn term_to_binding(&self, term: &RdfTerm, ctx: &ExecutionContext<'_>) -> Result<Binding> {
        match term {
            RdfTerm::Iri(iri) => {
                // Keep IRI as raw string - don't try to encode to SID
                // Graph source IRIs are independent of Fluree's namespace table
                Ok(Binding::iri(iri.as_str()))
            }
            RdfTerm::BlankNode(id) => {
                // Blank nodes use _: prefix convention
                let blank_iri = format!("_:{id}");
                Ok(Binding::iri(blank_iri))
            }
            RdfTerm::Literal { value, dtc } => {
                use fluree_db_core::FlakeValue;
                use fluree_vocab::UnresolvedDatatypeConstraint;

                let val = FlakeValue::String(value.clone());

                if let Some(UnresolvedDatatypeConstraint::LangTag(lang)) = dtc {
                    return Ok(Binding::lit_lang(val, lang.as_ref()));
                }

                let xsd_string_fallback = fluree_db_core::Sid::new(2, "string");
                let dt_sid = match dtc {
                    Some(UnresolvedDatatypeConstraint::Explicit(dt_iri)) => {
                        ctx.active_snapshot.encode_iri(dt_iri).unwrap_or_else(|| {
                            ctx.active_snapshot
                                .encode_iri(xsd::STRING)
                                .unwrap_or(xsd_string_fallback)
                        })
                    }
                    _ => ctx
                        .active_snapshot
                        .encode_iri(xsd::STRING)
                        .unwrap_or(xsd_string_fallback),
                };
                Ok(Binding::lit(val, dt_sid))
            }
        }
    }
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

                // Determine projection columns needed based on predicate filter
                // When a predicate filter is present, only project columns needed for that predicate
                let projection: Vec<String> = triples_map
                    .columns_for_predicate(self.pattern.predicate_filter.as_deref())
                    .into_iter()
                    .map(std::string::ToString::to_string)
                    .collect();

                // Scan the table
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
                        as_of_t,
                    )
                    .await?;

                // Build parent lookup tables for RefObjectMap POMs that match predicate_filter
                // Key: (parent_tm_iri, parent_join_cols) -> ParentLookup
                let mut parent_lookups: HashMap<LookupCacheKey, ParentLookup> = HashMap::new();

                // Only process POMs that pass the predicate filter
                let filtered_poms: Vec<_> = triples_map
                    .predicate_object_maps
                    .iter()
                    .filter(|pom| {
                        if let Some(ref pred_filter) = self.pattern.predicate_filter {
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

            // For each input row, process the pre-scanned table data
            for row_idx in 0..child_batch.len() {
                // Process each matching TriplesMap
                for triples_map in &triples_maps {
                    let (batches, parent_lookups) = match tm_scan_cache.get(&triples_map.iri) {
                        Some(cached) => cached,
                        None => continue,
                    };

                    // Check if subject_var was already bound in the child
                    let subject_was_bound = child_schema.contains(&self.pattern.subject_var);
                    // Check if object_var was already bound in the child
                    let object_was_bound = self
                        .pattern
                        .object_var
                        .map(|v| child_schema.contains(&v))
                        .unwrap_or(false);

                    // Process each batch from the table scan
                    for iceberg_batch in batches {
                        for table_row_idx in 0..iceberg_batch.num_rows {
                            // Materialize subject
                            let subject_term = materialize_subject_from_batch(
                                &triples_map.subject_map,
                                iceberg_batch,
                                table_row_idx,
                            )?;

                            let subject_term = match subject_term {
                                Some(t) => t,
                                None => continue, // Null subject - skip row
                            };

                            let subject_binding = self.term_to_binding(&subject_term, ctx)?;
                            // Note: term_to_binding always returns Binding::Iri or Binding::Lit,
                            // never Unbound (IRIs are kept as raw strings, not encoded to SIDs)

                            // SPARQL join semantics: if subject_var was already bound,
                            // check if the existing binding matches the new one
                            if subject_was_bound {
                                let subj_child_pos = child_schema
                                    .iter()
                                    .position(|&v| v == self.pattern.subject_var)
                                    .unwrap();
                                let existing_binding =
                                    &child_batch.column_by_idx(subj_child_pos).unwrap()[row_idx];

                                // Skip if existing is poisoned (can't join with poisoned)
                                if existing_binding.is_poisoned() {
                                    continue;
                                }

                                // Skip if existing is bound but doesn't match
                                if existing_binding.is_bound()
                                    && *existing_binding != subject_binding
                                {
                                    continue;
                                }
                            }

                            // Helper function to emit an output row (inlined to avoid borrow issues)
                            macro_rules! emit_row {
                                ($object_binding:expr) => {{
                                    // R2RML rows are tabular (Parquet/Arrow), not
                                    // FLI3 leaflets — charge per row at 1 micro-fuel.
                                    ctx.tracker.consume_fuel(1)?;

                                    // Build output row
                                    let mut out_row: Vec<Binding> =
                                        vec![Binding::Unbound; num_cols];

                                    // Copy child columns
                                    for (col_idx, &var) in child_schema.iter().enumerate() {
                                        let out_idx = *self.out_pos.get(&var).unwrap();
                                        out_row[out_idx] =
                                            child_batch.column_by_idx(col_idx).unwrap()[row_idx]
                                                .clone();
                                    }

                                    // Set subject binding (may overwrite if subject was in child,
                                    // but we've already verified it matches above)
                                    let subj_pos =
                                        *self.out_pos.get(&self.pattern.subject_var).unwrap();
                                    out_row[subj_pos] = subject_binding.clone();

                                    // Set object binding if variable
                                    if let Some(obj_var) = self.pattern.object_var {
                                        if let Some(obj_bind) = $object_binding {
                                            let obj_pos = *self.out_pos.get(&obj_var).unwrap();
                                            out_row[obj_pos] = obj_bind;
                                        }
                                    }

                                    // Add to output or pending
                                    if columns[0].len() < ctx.batch_size {
                                        for (col_idx, binding) in out_row.into_iter().enumerate() {
                                            columns[col_idx].push(binding);
                                        }
                                    } else {
                                        self.pending.push_back(out_row);
                                    }
                                }};
                            }

                            // Fast path: if object_var is None, this is a subject-only pattern
                            // (e.g., rdf:type pattern). Emit one row per subject, don't iterate POMs.
                            if self.pattern.object_var.is_none() {
                                emit_row!(None::<Binding>);
                                continue; // Next table row
                            }

                            // Normal path: iterate POMs and emit rows for each matching predicate-object
                            // Filter predicate-object maps by predicate_filter if specified
                            let poms_to_process: Vec<_> = triples_map
                                .predicate_object_maps
                                .iter()
                                .filter(|pom| {
                                    if let Some(ref pred_filter) = self.pattern.predicate_filter {
                                        // Only process POMs where predicate matches the filter
                                        pom.predicate_map.as_constant()
                                            == Some(pred_filter.as_str())
                                    } else {
                                        // No filter - process all POMs
                                        true
                                    }
                                })
                                .collect();

                            // Handle predicate-object maps
                            for pom in poms_to_process {
                                // Materialize object - handle RefObjectMap specially
                                let object_term = if let ObjectMap::RefObjectMap(ref rom) =
                                    pom.object_map
                                {
                                    // RefObjectMap: look up parent subject via join
                                    let child_columns: Vec<String> = rom
                                        .child_columns()
                                        .into_iter()
                                        .map(std::string::ToString::to_string)
                                        .collect();

                                    // Extract child join key from current row
                                    let child_key = match get_join_key_from_batch(
                                        &child_columns,
                                        iceberg_batch,
                                        table_row_idx,
                                    ) {
                                        Some(k) => k,
                                        None => continue, // Null in child join key - skip
                                    };

                                    // Build composite lookup key: (parent_tm_iri, sorted_parent_join_cols)
                                    let mut parent_join_cols: Vec<String> = rom
                                        .parent_columns()
                                        .into_iter()
                                        .map(std::string::ToString::to_string)
                                        .collect();
                                    parent_join_cols.sort(); // Must match the key used when building
                                    let lookup_key =
                                        (rom.parent_triples_map.clone(), parent_join_cols);

                                    // Look up parent subject in the pre-built lookup
                                    let lookup = match parent_lookups.get(&lookup_key) {
                                        Some(l) => l,
                                        None => {
                                            tracing::debug!(
                                                parent = %rom.parent_triples_map,
                                                "No parent lookup found for RefObjectMap, skipping"
                                            );
                                            continue;
                                        }
                                    };

                                    match lookup.get(&child_key) {
                                        Some(parent_subject) => Some(parent_subject.clone()),
                                        None => {
                                            // No matching parent row - this is normal for
                                            // orphaned foreign keys. Skip silently.
                                            continue;
                                        }
                                    }
                                } else {
                                    // Regular object map - materialize from current row
                                    materialize_object_from_batch(
                                        &pom.object_map,
                                        iceberg_batch,
                                        table_row_idx,
                                    )?
                                };

                                let object_term = match object_term {
                                    Some(t) => t,
                                    None => continue, // Null object - skip
                                };

                                let object_binding = self.term_to_binding(&object_term, ctx)?;
                                // Note: term_to_binding always returns Binding::Iri or Binding::Lit

                                // SPARQL join semantics: if object_var was already bound,
                                // check if the existing binding matches the new one
                                if object_was_bound {
                                    if let Some(obj_var) = self.pattern.object_var {
                                        let obj_child_pos = child_schema
                                            .iter()
                                            .position(|&v| v == obj_var)
                                            .unwrap();
                                        let existing_binding = &child_batch
                                            .column_by_idx(obj_child_pos)
                                            .unwrap()[row_idx];

                                        // Skip if existing is poisoned
                                        if existing_binding.is_poisoned() {
                                            continue;
                                        }

                                        // Skip if existing is bound but doesn't match
                                        if existing_binding.is_bound()
                                            && *existing_binding != object_binding
                                        {
                                            continue;
                                        }
                                    }
                                }

                                emit_row!(Some(object_binding));
                            }
                        }
                    }
                } // End for triples_map in triples_maps
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
