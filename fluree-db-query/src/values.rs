//! VALUES operator - injects constant solutions
//!
//! The ValuesOperator injects constant value rows into the solution stream.
//! It implements SPARQL's VALUES clause semantics:
//!
//! - For each input row × each value row, produce an output row
//! - Overlapping variables: filter out rows where values mismatch
//! - Non-clobbering: only adds NEW variables, never overwrites
//!
//! For example, `VALUES ?x ?y { (1 "a") (2 "b") }` joins each value row with input solutions, filtering on overlapping variables.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::sync::Arc;

/// VALUES operator - injects constant solutions into the stream
///
/// Takes value rows with their variables and joins them with input solutions.
/// Implements SPARQL VALUES semantics with overlap compatibility checking.
pub struct ValuesOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// Constant rows to inject (each row has one binding per value_var)
    value_rows: Vec<Vec<Binding>>,
    /// Output schema (union of child schema + new value vars)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Mapping: for each value_var, its position in child schema (None if new var)
    overlap_positions: Vec<Option<usize>>,
    /// Reverse mapping: child column index → index into `overlap_positions` / `value_rows`.
    /// Pre-computed at construction for O(1) lookup in `merge_rows`.
    child_col_to_val_idx: std::collections::HashMap<usize, usize>,
}

impl ValuesOperator {
    /// Create a new VALUES operator
    ///
    /// # Arguments
    ///
    /// * `child` - Child operator providing input solutions
    /// * `value_vars` - Variables being defined by VALUES
    /// * `value_rows` - Constant rows (each row has bindings for value_vars in order)
    pub fn new(
        child: BoxedOperator,
        value_vars: Vec<VarId>,
        value_rows: Vec<Vec<Binding>>,
    ) -> Self {
        let child_schema = child.schema();

        // Compute overlap positions: for each value_var, where is it in child schema?
        let overlap_positions: Vec<Option<usize>> = value_vars
            .iter()
            .map(|var| child_schema.iter().position(|v| v == var))
            .collect();

        // Build output schema: child schema + any new vars from value_vars
        let mut output_vars: Vec<VarId> = child_schema.to_vec();

        for (i, &var) in value_vars.iter().enumerate() {
            if overlap_positions[i].is_none() {
                // This is a new variable
                output_vars.push(var);
            }
        }

        let schema = Arc::from(output_vars.into_boxed_slice());

        // Pre-compute reverse map: child column → value_rows index.
        // Used in `merge_rows` to fill unbound overlap vars in O(1).
        let child_col_to_val_idx: std::collections::HashMap<usize, usize> = overlap_positions
            .iter()
            .enumerate()
            .filter_map(|(val_idx, pos)| pos.map(|col| (col, val_idx)))
            .collect();

        Self {
            child,
            value_rows,
            schema,
            state: OperatorState::Created,
            overlap_positions,
            child_col_to_val_idx,
        }
    }

    /// Check if a value row is compatible with an input row
    ///
    /// Returns false if any overlapping variable has a mismatched value.
    fn is_compatible(
        &self,
        ctx: &ExecutionContext<'_>,
        input_row: &[&Binding],
        value_row: &[Binding],
    ) -> bool {
        for (val_idx, overlap_pos) in self.overlap_positions.iter().enumerate() {
            if let Some(child_pos) = overlap_pos {
                // This value var exists in child schema - check compatibility
                let child_val = input_row[*child_pos];
                let values_val = &value_row[val_idx];

                // Skip if either side is effectively unbound (compatible with anything).
                // Poisoned arises from failed OPTIONAL — semantically unbound for VALUES.
                if child_val.is_unbound_or_poisoned() || values_val.is_unbound_or_poisoned() {
                    continue;
                }

                // Both bound - must be compatible.
                //
                // Special case: VALUES may contain SID-encoded IRIs (single-ledger lowering),
                // while dataset execution can produce `IriMatch` bindings. We treat
                // Sid vs IriMatch/Iri as comparable by decoding the SID using the
                // primary db in the execution context.
                if !bindings_compatible_for_values(ctx, child_val, values_val) {
                    return false;
                }
            }
        }
        true
    }

    /// Merge an input row with a compatible value row
    ///
    /// Produces an output row with all child columns plus new value columns.
    /// For overlap variables, if the child has an unbound/poisoned value but the
    /// VALUES row has a concrete value, the VALUES value is used (fills in the gap).
    fn merge_rows(&self, input_row: &[&Binding], value_row: &[Binding]) -> Vec<Binding> {
        let mut output = Vec::with_capacity(self.schema.len());

        // Copy child columns, filling unbound overlap vars from VALUES row.
        // The `child_col_to_val_idx` map gives O(1) lookup for overlap vars.
        for (col, binding) in input_row.iter().enumerate() {
            if binding.is_unbound_or_poisoned() {
                if let Some(&val_idx) = self.child_col_to_val_idx.get(&col) {
                    let values_val = &value_row[val_idx];
                    if !values_val.is_unbound_or_poisoned() {
                        output.push(values_val.clone());
                        continue;
                    }
                }
            }
            output.push((*binding).clone());
        }

        // Then, add new value columns (vars not in child schema)
        for (i, overlap_pos) in self.overlap_positions.iter().enumerate() {
            if overlap_pos.is_none() {
                // This is a new variable - add its value
                output.push(value_row[i].clone());
            }
        }

        output
    }
}

#[async_trait]
impl Operator for ValuesOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // Get next batch from child
        let input_batch = match self.child.next_batch(ctx).await? {
            Some(b) => b,
            None => {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
        };

        if input_batch.is_empty() {
            // Return empty batch with our schema
            return Ok(Some(Batch::empty(self.schema.clone())?));
        }

        // Handle special case: no value rows means no output
        if self.value_rows.is_empty() {
            return Ok(Some(Batch::empty(self.schema.clone())?));
        }

        // Build output: for each input row × each value row, check compatibility
        let num_cols = self.schema.len();
        let child_num_cols = self.child.schema().len();
        let max_rows = input_batch.len() * self.value_rows.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(max_rows))
            .collect();

        for row_idx in 0..input_batch.len() {
            // Get input row as slice of references
            let input_row: Vec<&Binding> = (0..child_num_cols)
                .map(|col| input_batch.get_by_col(row_idx, col))
                .collect();

            for value_row in &self.value_rows {
                if self.is_compatible(ctx, &input_row, value_row) {
                    // Merge and add to output
                    let merged = self.merge_rows(&input_row, value_row);
                    for (col_idx, binding) in merged.into_iter().enumerate() {
                        columns[col_idx].push(binding);
                    }
                }
            }
        }

        if columns.first().map(std::vec::Vec::is_empty).unwrap_or(true) {
            // No compatible rows - return empty batch
            return Ok(Some(Batch::empty(self.schema.clone())?));
        }

        Ok(Some(Batch::new(self.schema.clone(), columns)?))
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Estimate: child rows * value rows (upper bound, actual may be less due to filtering)
        self.child
            .estimated_rows()
            .map(|r| r * self.value_rows.len())
    }
}

fn bindings_compatible_for_values(ctx: &ExecutionContext<'_>, a: &Binding, b: &Binding) -> bool {
    if a == b {
        return true;
    }

    match (a, b) {
        // Compare SID to IRI-bearing bindings by decoding SID via primary db.
        (Binding::Sid { sid, .. }, Binding::Iri(iri) | Binding::IriMatch { iri, .. })
        | (Binding::Iri(iri) | Binding::IriMatch { iri, .. }, Binding::Sid { sid, .. }) => ctx
            .active_snapshot
            .decode_sid(sid)
            .map(|decoded| decoded == iri.as_ref())
            .unwrap_or(false),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn xsd_long() -> Sid {
        Sid::new(2, "long")
    }

    fn xsd_string() -> Sid {
        Sid::new(2, "string")
    }

    #[test]
    fn test_values_operator_schema_no_overlap() {
        // Child has ?a, VALUES adds ?x ?y
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let value_vars = vec![VarId(1), VarId(2)];
        let value_rows = vec![vec![
            Binding::lit(FlakeValue::Long(1), xsd_long()),
            Binding::lit(FlakeValue::String("a".into()), xsd_string()),
        ]];

        let op = ValuesOperator::new(child, value_vars, value_rows);

        // Output schema should be [?a, ?x, ?y]
        assert_eq!(op.schema().len(), 3);
        assert_eq!(op.schema()[0], VarId(0));
        assert_eq!(op.schema()[1], VarId(1));
        assert_eq!(op.schema()[2], VarId(2));
    }

    #[test]
    fn test_values_operator_schema_with_overlap() {
        // Child has ?a ?b, VALUES uses ?a ?c (overlap on ?a)
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let child = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let value_vars = vec![VarId(0), VarId(2)]; // ?a overlaps, ?c is new
        let value_rows = vec![vec![
            Binding::lit(FlakeValue::Long(1), xsd_long()),
            Binding::lit(FlakeValue::Long(100), xsd_long()),
        ]];

        let op = ValuesOperator::new(child, value_vars, value_rows);

        // Output schema should be [?a, ?b, ?c] - no duplicates
        assert_eq!(op.schema().len(), 3);
        assert_eq!(op.schema()[0], VarId(0)); // ?a
        assert_eq!(op.schema()[1], VarId(1)); // ?b
        assert_eq!(op.schema()[2], VarId(2)); // ?c
    }

    #[test]
    fn test_values_operator_compatibility_check() {
        // Test the compatibility logic directly
        use crate::var_registry::VarRegistry;
        use fluree_db_core::LedgerSnapshot;

        let vars = VarRegistry::new();
        let snapshot = LedgerSnapshot::genesis("values-test/main");
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let value_vars = vec![VarId(0), VarId(1)]; // ?a overlaps
        let value_rows = vec![vec![
            Binding::lit(FlakeValue::Long(10), xsd_long()),
            Binding::lit(FlakeValue::Long(20), xsd_long()),
        ]];

        let op = ValuesOperator::new(child, value_vars, value_rows);

        // Compatible: child has 10, values has 10
        let binding_10 = Binding::lit(FlakeValue::Long(10), xsd_long());
        let input_row = vec![&binding_10];
        assert!(op.is_compatible(
            &ctx,
            &input_row,
            &[
                Binding::lit(FlakeValue::Long(10), xsd_long()),
                Binding::lit(FlakeValue::Long(20), xsd_long())
            ]
        ));

        // Incompatible: child has 10, values has 99
        let binding_10 = Binding::lit(FlakeValue::Long(10), xsd_long());
        let input_row = vec![&binding_10];
        assert!(!op.is_compatible(
            &ctx,
            &input_row,
            &[
                Binding::lit(FlakeValue::Long(99), xsd_long()),
                Binding::lit(FlakeValue::Long(20), xsd_long())
            ]
        ));

        // Compatible: child is Unbound (matches anything)
        let unbound = Binding::Unbound;
        let input_row = vec![&unbound];
        assert!(op.is_compatible(
            &ctx,
            &input_row,
            &[
                Binding::lit(FlakeValue::Long(99), xsd_long()),
                Binding::lit(FlakeValue::Long(20), xsd_long())
            ]
        ));
    }

    // Helper struct for testing: an operator with a specific schema that returns empty
    struct TestEmptyWithSchema {
        schema: Arc<[VarId]>,
    }

    #[async_trait]
    impl Operator for TestEmptyWithSchema {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }

        async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
            Ok(())
        }

        async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            Ok(None)
        }

        fn close(&mut self) {}
    }
}
