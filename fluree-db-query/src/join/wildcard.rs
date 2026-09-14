//! Wildcard-predicate bind joins over the shared streaming index cursor.
//!
//! The cursor sorts distinct probe keys; this layer restores driving-row
//! multiplicity, without collecting the (potentially enormous) expanded join.
//! As with other unordered physical plans, output order is not guaranteed.

use super::*;
use fluree_db_binary_index::read::batched_lookup::{
    BatchedWildcardCursor, WildcardDirection, WildcardMatches,
};

pub(super) fn eligible(
    binds: &[BindInstruction],
    pattern: &TriplePattern,
    has_bounds: bool,
) -> Option<WildcardDirection> {
    let (Ref::Var(s), Ref::Var(p), Term::Var(o)) = (&pattern.s, &pattern.p, &pattern.o) else {
        return None;
    };
    // Predicate must be a new variable. Repeated variables require cross-ID
    // equality checks and remain on the ordinary scan path.
    if s == p || s == o || p == o || has_bounds || pattern.dtc.is_some() || binds.len() != 1 {
        return None;
    }
    match binds[0].position {
        PatternPosition::Subject => Some(WildcardDirection::Outgoing),
        PatternPosition::Object => Some(WildcardDirection::IncomingRefs),
        PatternPosition::Predicate => None,
    }
}

pub(super) struct WildcardJoin {
    cursor: BatchedWildcardCursor,
    left_batches: Vec<Batch>,
    /// A lookup key can drive several rows, including exact duplicate rows.
    left_rows: FxHashMap<u64, Vec<(usize, usize)>>,
    matches: Option<WildcardMatches>,
    match_idx: usize,
    left_idx: usize,
    right: Option<Vec<Binding>>,
}

impl WildcardJoin {
    pub(super) fn new(join: &mut NestedLoopJoinOperator, ctx: &ExecutionContext<'_>) -> Self {
        let mut left_rows: FxHashMap<u64, Vec<(usize, usize)>> = FxHashMap::default();
        for (batch, row, key) in join.batched_accumulator.drain(..) {
            left_rows.entry(key).or_default().push((batch, row));
        }
        let keys: Vec<u64> = left_rows.keys().copied().collect();
        let cursor = BatchedWildcardCursor::new(
            Arc::clone(ctx.binary_store.as_ref().unwrap()),
            ctx.binary_g_id,
            &keys,
            ctx.to_t,
            join.wildcard_direction.unwrap(),
            fluree_db_binary_index::ColumnProjection::all(),
        );
        let left_batches = std::mem::take(&mut join.stored_left_batches);
        join.current_left_batch_stored_idx = None;
        Self {
            cursor,
            left_batches,
            left_rows,
            matches: None,
            match_idx: 0,
            left_idx: 0,
            right: None,
        }
    }

    pub(super) fn next_batch(
        &mut self,
        join: &NestedLoopJoinOperator,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<Batch>> {
        let store = ctx.binary_store.as_ref().unwrap();
        let mut columns: Vec<Vec<Binding>> = (0..join.combined_schema.len())
            .map(|_| Vec::with_capacity(ctx.batch_size))
            .collect();
        let mut count = 0;
        while count < ctx.batch_size {
            ctx.check_cancelled()?;
            if self
                .matches
                .as_ref()
                .is_none_or(|m| self.match_idx == m.rows.len())
            {
                self.matches = self
                    .cursor
                    .next_batch()
                    .map_err(|e| QueryError::from_io("batched wildcard probe", e))?;
                self.match_idx = 0;
                self.left_idx = 0;
                self.right = None;
                let Some(matches) = &self.matches else {
                    break;
                };
                charge_probe_rows(ctx, matches.rows.len())?;
                if matches.rows.is_empty() {
                    continue;
                }
            }
            let matches = self.matches.as_ref().unwrap();
            let (key, row) = matches.rows[self.match_idx];
            if self.right.is_none() {
                let batch = &matches.batch;
                let p_id = batch.p_id.get_or(row, 0);
                // Match BinaryScan's wildcard visibility rule, including the
                // explicit inspection escape. Never hide the whole f: namespace.
                if !ctx.include_system_facts
                    && store
                        .p_sid_table()
                        .get(p_id as usize)
                        .is_some_and(fluree_db_core::is_reserved_reifies_predicate)
                {
                    self.match_idx += 1;
                    continue;
                }
                let mut right = Vec::with_capacity(join.right_new_vars.len());
                for var in &join.right_new_vars {
                    let value = if Some(*var) == join.right_pattern.s.as_var() {
                        Binding::encoded_sid(batch.s_id.get(row))
                    } else if Some(*var) == join.right_pattern.p.as_var() {
                        Binding::EncodedPid { p_id }
                    } else {
                        build_probe_object_binding(
                            ctx,
                            store,
                            None,
                            p_id,
                            batch.o_type.get_or(row, 0),
                            batch.o_key.get(row),
                            batch.o_i.get_or(row, u32::MAX),
                            batch.t.get_or(row, 0) as i64,
                        )?
                    };
                    right.push(value);
                }
                if !join.apply_right_scan_inline_ops(ctx, &mut right)? {
                    self.match_idx += 1;
                    continue;
                }
                self.right = Some(right);
            }
            let left_rows = &self.left_rows[&key];
            let (batch_idx, row_idx) = left_rows[self.left_idx];
            self.left_idx += 1;
            let left = &self.left_batches[batch_idx];
            let right = self.right.as_ref().unwrap();
            if join.inline_has_bind() {
                let mut combined: Vec<Binding> = (0..join.left_schema.len())
                    .map(|col| left.get_by_col(row_idx, col).clone())
                    .collect();
                combined.extend(right.iter().cloned());
                if apply_inline(
                    &join.inline_ops,
                    &join.combined_schema,
                    &mut combined,
                    Some(ctx),
                )? {
                    for (col, val) in columns.iter_mut().zip(combined) {
                        col.push(val);
                    }
                    count += 1;
                }
            } else {
                // Share the fixed-predicate join's row view: filters need no
                // per-output-row allocation and left columns are copied once.
                let view = CombinedRowView {
                    left_batch: left,
                    left_row: row_idx,
                    left_len: join.left_schema.len(),
                    right,
                    schema: &join.combined_schema,
                };
                if apply_inline_filters_view(&join.inline_ops, &view, ctx)? {
                    for (i, col) in columns.iter_mut().enumerate() {
                        col.push(if i < join.left_schema.len() {
                            left.get_by_col(row_idx, i).clone()
                        } else {
                            right[i - join.left_schema.len()].clone()
                        });
                    }
                    count += 1;
                }
            }
            if self.left_idx == left_rows.len() {
                self.left_idx = 0;
                self.match_idx += 1;
                self.right = None;
            }
        }
        if count == 0 {
            return Ok(None);
        }
        if columns.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(count)));
        }
        Ok(Some(Batch::new(join.combined_schema.clone(), columns)?))
    }
}
