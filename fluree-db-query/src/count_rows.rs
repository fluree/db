//! Generic COUNT(*) / COUNT(?var) row-count operator.
//!
//! This operator wraps a child operator, drains it, and returns a single-row batch
//! containing the total number of rows produced by the child.
//!
//! Used by fast-path planning when a query can be satisfied by a scan that emits
//! no bindings (empty schema) plus encoded pre-filter pushdown in `BinaryScanOperator`.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::fast_path_store;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{FlakeValue, Sid};
use std::sync::Arc;

pub struct CountRowsOperator {
    fast_child: BoxedOperator,
    out_var: VarId,
    state: OperatorState,
    done: bool,
    count: i64,
    fallback: Option<BoxedOperator>,
    use_fallback: bool,
}

impl CountRowsOperator {
    pub fn new(fast_child: BoxedOperator, out_var: VarId, fallback: Option<BoxedOperator>) -> Self {
        Self {
            fast_child,
            out_var,
            state: OperatorState::Created,
            done: false,
            count: 0,
            fallback,
            use_fallback: false,
        }
    }

    fn schema_arc(&self) -> Arc<[VarId]> {
        Arc::from(vec![self.out_var].into_boxed_slice())
    }

    fn build_output_batch(&self, count: i64) -> Result<Batch> {
        let schema = self.schema_arc();
        let col = vec![Binding::lit(FlakeValue::Long(count), Sid::xsd_integer())];
        Batch::new(schema, vec![col])
            .map_err(|e| QueryError::execution(format!("count rows batch build: {e}")))
    }
}

#[async_trait]
impl Operator for CountRowsOperator {
    fn schema(&self) -> &[VarId] {
        std::slice::from_ref(&self.out_var)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        if fast_path_store(ctx).is_some() {
            self.use_fallback = false;
            self.fast_child.open(ctx).await?;
        } else if let Some(fallback) = self.fallback.as_mut() {
            self.use_fallback = true;
            fallback.open(ctx).await?;
        } else {
            // No fallback was provided; attempt the fast plan anyway.
            self.use_fallback = false;
            self.fast_child.open(ctx).await?;
        }

        self.state = OperatorState::Open;
        self.done = false;
        self.count = 0;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        if self.use_fallback {
            return self
                .fallback
                .as_mut()
                .expect("use_fallback implies fallback exists")
                .next_batch(ctx)
                .await;
        }
        if self.done {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        while let Some(batch) = self.fast_child.next_batch(ctx).await? {
            self.count += batch.len() as i64;
        }

        self.done = true;
        Ok(Some(self.build_output_batch(self.count)?))
    }

    fn close(&mut self) {
        if self.use_fallback {
            if let Some(fallback) = self.fallback.as_mut() {
                fallback.close();
            }
        } else {
            self.fast_child.close();
        }
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }
}
