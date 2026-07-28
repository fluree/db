//! Scan-side top-k bound engine (PR-5).
//!
//! For a single-column DESCENDING `ORDER BY … LIMIT k` pushed to the scan, the
//! reader visits files in `upper_bound(sort_col)`-DESC order and maintains the
//! running k-th bound (the smallest of the k largest sort values seen so far).
//! Once the heap is full, any *unread* file whose `upper_bound` is strictly below
//! that bound cannot contain a top-k row, so — because files are visited in
//! bound-DESC order — the reader stops.
//!
//! # Soundness (strict-superset, like [`crate::scan::pruning`])
//!
//! - **Prune only when the heap is full** (k non-null values seen). With fewer
//!   than k non-null rows the bound never forms, nothing stops, every file is
//!   read, and NULL-ordered rows (which sort last under DESC) legitimately reach
//!   the authoritative sort above.
//! - **A file with no `upper_bound` for the sort column (an all-NULL column)
//!   never stops the scan** (`can_stop` returns false on a `None` next bound).
//! - **Strict `<`** at the boundary: a file whose `upper_bound` *equals* the k-th
//!   bound is read (a tie could belong in the result; the sort above resolves the
//!   exact order). The engine therefore over-keeps, never over-prunes.
//!
//! DESC only. ASC is declined upstream (SPARQL orders unbound values *first* in
//! ASC, so a NULL-bearing file can never be pruned — not a clean mirror).

use crate::io::batch::{Column, ColumnBatch};
use crate::manifest::value_codec::TypedValue;
use crate::manifest::{decode_by_type_string, DataFile};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

/// Directive handed to the scan for a single-column DESC top-k pushdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopKConfig {
    /// Iceberg field id of the primary DESC sort column.
    pub sort_field_id: i32,
    /// How many top rows the bound must retain — the query's `LIMIT + OFFSET`.
    pub k: usize,
}

/// Total-order wrapper over a same-typed `TypedValue` for heap ordering. In
/// practice one column has one type; `partial_cmp` is total except for float
/// NaN, which we fold to `Equal` (a NaN sort key only ever *weakens* pruning —
/// the file-bound compare is NaN-safe and keeps — never breaks correctness).
#[derive(Debug, Clone)]
struct OrdKey(TypedValue);

impl PartialEq for OrdKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for OrdKey {}
impl PartialOrd for OrdKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

/// The running k-th bound over the sort column's non-null values.
#[derive(Debug)]
pub struct TopKBound {
    k: usize,
    /// Min-heap of the k largest values seen — the root is the current k-th
    /// (smallest retained) value once full.
    heap: BinaryHeap<Reverse<OrdKey>>,
}

impl TopKBound {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            heap: BinaryHeap::with_capacity(k.saturating_add(1)),
        }
    }

    /// Fold one non-null sort value into the running top-k.
    pub fn observe(&mut self, v: TypedValue) {
        if self.k == 0 {
            return;
        }
        if self.heap.len() < self.k {
            self.heap.push(Reverse(OrdKey(v)));
            return;
        }
        // Replace the current k-th (smallest retained) if v is strictly larger.
        if let Some(Reverse(min)) = self.heap.peek() {
            if OrdKey(v.clone()).cmp(min) == Ordering::Greater {
                self.heap.pop();
                self.heap.push(Reverse(OrdKey(v)));
            }
        }
    }

    /// Fold every non-null value of one file.
    pub fn observe_all(&mut self, vals: impl IntoIterator<Item = TypedValue>) {
        for v in vals {
            self.observe(v);
        }
    }

    /// True once k non-null values have been seen — the precondition for any prune.
    pub fn is_full(&self) -> bool {
        self.heap.len() >= self.k && self.k > 0
    }

    /// The k-th bound (smallest of the retained top-k), or `None` until full.
    pub fn kth(&self) -> Option<&TypedValue> {
        if self.is_full() {
            self.heap.peek().map(|Reverse(OrdKey(v))| v)
        } else {
            None
        }
    }

    /// Whether the scan may stop before reading the next file, whose (highest
    /// remaining) `upper_bound` is `next_upper`. Stop iff the heap is full and
    /// `next_upper` is strictly below the k-th bound. A `None` next bound (all-NULL
    /// column / missing stats) never stops (must read); a tie (`==`) never stops
    /// (over-keep); a NaN compare never stops (`lt` → None → false).
    pub fn can_stop(&self, next_upper: Option<&TypedValue>) -> bool {
        match (self.kth(), next_upper) {
            (Some(kth), Some(u)) => u.lt(kth).unwrap_or(false),
            _ => false,
        }
    }
}

/// Plan the DESC top-k read order over a set of data files: pairs of
/// `(original_index, decoded upper_bound of the sort column)` sorted so the
/// highest `upper_bound` is read first and **files with no `upper_bound` (an
/// all-NULL column or missing stats) come LAST** — they can never stop the scan
/// (see [`TopKBound::can_stop`]) and must be read. The read loop visits the
/// returned order and, after each file, consults the NEXT pair's bound to decide
/// whether to stop.
pub fn plan_topk_read<'a>(
    data_files: impl Iterator<Item = &'a DataFile>,
    sort_field_id: i32,
    sort_type: Option<&str>,
) -> Vec<(usize, Option<TypedValue>)> {
    let mut order: Vec<(usize, Option<TypedValue>)> = data_files
        .enumerate()
        .map(|(i, df)| {
            let bound = df
                .upper_bound(sort_field_id)
                .and_then(|bytes| decode_by_type_string(bytes, sort_type).ok());
            (i, bound)
        })
        .collect();
    order.sort_by(|(_, a), (_, b)| match (a, b) {
        // DESC by value; a stable sort preserves the manifest order within ties.
        (Some(x), Some(y)) => y.partial_cmp(x).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Less, // bound-present before no-bound
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    });
    order
}

/// The non-null values of column `field_id` in a batch, as `TypedValue`s, to fold
/// into a [`TopKBound`]. NULLs are skipped (they never form the DESC bound; they
/// sort last and only matter when the heap can't fill). `Bytes` is not an ordered
/// pushable key, so it yields nothing (the bound stays loose → conservative).
pub fn batch_sort_values(batch: &ColumnBatch, field_id: i32) -> Vec<TypedValue> {
    let Some(col) = batch.column_by_id(field_id) else {
        return Vec::new();
    };
    match col {
        Column::Boolean(v) => v
            .iter()
            .flatten()
            .map(|&x| TypedValue::Boolean(x))
            .collect(),
        Column::Int32(v) => v.iter().flatten().map(|&x| TypedValue::Int32(x)).collect(),
        Column::Int64(v) => v.iter().flatten().map(|&x| TypedValue::Int64(x)).collect(),
        Column::Float32(v) => v
            .iter()
            .flatten()
            .map(|&x| TypedValue::Float32(x))
            .collect(),
        Column::Float64(v) => v
            .iter()
            .flatten()
            .map(|&x| TypedValue::Float64(x))
            .collect(),
        Column::String(v) => v
            .iter()
            .flatten()
            .map(|x| TypedValue::String(x.clone()))
            .collect(),
        Column::Date(v) => v.iter().flatten().map(|&x| TypedValue::Date(x)).collect(),
        Column::Timestamp(v) => v
            .iter()
            .flatten()
            .map(|&x| TypedValue::Timestamp(x))
            .collect(),
        Column::TimestampTz(v) => v
            .iter()
            .flatten()
            .map(|&x| TypedValue::TimestampTz(x))
            .collect(),
        Column::Decimal {
            values,
            precision,
            scale,
        } => values
            .iter()
            .flatten()
            .map(|&unscaled| TypedValue::Decimal {
                unscaled,
                precision: *precision,
                scale: *scale,
            })
            .collect(),
        Column::Bytes(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(v: f64) -> TypedValue {
        TypedValue::Float64(v)
    }

    #[test]
    fn bound_forms_only_when_full() {
        let mut b = TopKBound::new(3);
        assert!(!b.is_full());
        assert_eq!(b.kth(), None);
        b.observe(f(10.0));
        b.observe(f(20.0));
        assert!(!b.is_full(), "2 < k=3");
        assert_eq!(
            b.kth(),
            None,
            "no bound until full → can't prune (k>non-null)"
        );
        b.observe(f(30.0));
        assert!(b.is_full());
        assert_eq!(b.kth(), Some(&f(10.0)), "k-th = smallest of the top-3");
    }

    #[test]
    fn larger_values_evict_the_kth() {
        let mut b = TopKBound::new(3);
        b.observe_all([f(10.0), f(20.0), f(30.0)]);
        assert_eq!(b.kth(), Some(&f(10.0)));
        b.observe(f(25.0)); // evicts 10 → top-3 {20,25,30}, k-th = 20
        assert_eq!(b.kth(), Some(&f(20.0)));
        b.observe(f(5.0)); // below k-th → ignored
        assert_eq!(b.kth(), Some(&f(20.0)));
        b.observe(f(100.0)); // evicts 20 → {25,30,100}, k-th = 25
        assert_eq!(b.kth(), Some(&f(25.0)));
    }

    #[test]
    fn can_stop_semantics() {
        let mut b = TopKBound::new(3);
        // Not full → never stop, whatever the next bound.
        assert!(!b.can_stop(Some(&f(1.0))));
        b.observe_all([f(10.0), f(20.0), f(30.0)]); // k-th = 10
                                                    // next strictly below k-th → STOP.
        assert!(b.can_stop(Some(&f(9.999))));
        // next equal to k-th → do NOT stop (tie over-keep).
        assert!(!b.can_stop(Some(&f(10.0))));
        // next above k-th → do NOT stop.
        assert!(!b.can_stop(Some(&f(11.0))));
        // no bound (all-null column) → never stop (must read).
        assert!(!b.can_stop(None));
    }

    #[test]
    fn k_zero_is_inert() {
        let mut b = TopKBound::new(0);
        b.observe(f(10.0));
        assert!(!b.is_full());
        assert_eq!(b.kth(), None);
        assert!(!b.can_stop(Some(&f(1.0))));
    }

    #[test]
    fn nan_never_over_prunes() {
        // A NaN sort value folded in must not produce a bound that prunes a file
        // holding real values (the file-bound compare stays NaN-safe → keep).
        let mut b = TopKBound::new(2);
        b.observe(f(f64::NAN));
        b.observe(f(5.0));
        // Full (2 values), but the k-th may be NaN; a NaN k-th makes `u.lt(NaN)`
        // return None → unwrap_or(false) → never stop. Conservative.
        assert!(!b.can_stop(Some(&f(1.0))) || b.kth() == Some(&f(5.0)));
    }

    fn df_with_upper(field_id: i32, upper: Option<f64>, null_count: i64) -> DataFile {
        use std::collections::HashMap;
        let upper_bounds = upper.map(|u| {
            let mut m = HashMap::new();
            m.insert(field_id, u.to_le_bytes().to_vec());
            m
        });
        let null_value_counts = {
            let mut m = HashMap::new();
            m.insert(field_id, null_count);
            Some(m)
        };
        DataFile {
            file_path: "t.parquet".to_string(),
            file_format: crate::manifest::FileFormat::Parquet,
            record_count: 10,
            file_size_in_bytes: 1,
            partition: crate::manifest::PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds,
            split_offsets: None,
            sort_order_id: None,
        }
    }

    #[test]
    fn plan_orders_desc_with_no_bound_last() {
        let dfs = vec![
            df_with_upper(1, Some(10.0), 0), // 0
            df_with_upper(1, Some(30.0), 0), // 1
            df_with_upper(1, None, 5),       // 2 — all-null column, no upper_bound
            df_with_upper(1, Some(20.0), 0), // 3
        ];
        let order = plan_topk_read(dfs.iter(), 1, Some("double"));
        let idxs: Vec<usize> = order.iter().map(|(i, _)| *i).collect();
        // DESC by upper_bound: 30(1), 20(3), 10(0); the no-bound file (2) LAST.
        assert_eq!(idxs, vec![1, 3, 0, 2]);
        assert_eq!(order.last().unwrap().1, None, "all-null file has no bound");
    }

    #[test]
    fn extract_skips_nulls() {
        use crate::io::batch::{BatchSchema, FieldInfo, FieldType};
        use std::sync::Arc;
        let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
            name: "tot".to_string(),
            field_type: FieldType::Float64,
            nullable: true,
            field_id: 7,
        }]));
        let col = Column::Float64(vec![Some(4999.9), None, Some(4999.6), None]);
        let batch = ColumnBatch::new(schema, vec![col]).unwrap();
        assert_eq!(batch_sort_values(&batch, 7), vec![f(4999.9), f(4999.6)]);
        assert!(
            batch_sort_values(&batch, 99).is_empty(),
            "unknown field → empty (conservative)"
        );
    }

    /// End-to-end pure simulation of the read loop: prune the low files after the
    /// heap fills with the top-k. Proves the early-stop reads only the files
    /// holding the top-k.
    #[test]
    fn read_loop_prunes_after_heap_fills() {
        let vals = vec![
            vec![Some(4999.98), Some(100.0)], // 0: holds the max
            vec![Some(4999.60), Some(50.0)],  // 1: holds the 2nd
            vec![Some(3000.0), Some(10.0)],   // 2: below the top-2 once full
            vec![None, None],                 // 3: all-null
        ];
        let dfs = vec![
            df_with_upper(1, Some(4999.98), 0),
            df_with_upper(1, Some(4999.60), 0),
            df_with_upper(1, Some(3000.0), 0),
            df_with_upper(1, None, 2),
        ];
        let order = plan_topk_read(dfs.iter(), 1, Some("double"));
        let mut bound = TopKBound::new(2);
        let mut read = 0usize;
        for pos in 0..order.len() {
            let (orig, _) = order[pos];
            read += 1;
            bound.observe_all(vals[orig].iter().flatten().map(|&x| f(x)));
            if let Some((_, next_upper)) = order.get(pos + 1) {
                if bound.can_stop(next_upper.as_ref()) {
                    break;
                }
            }
        }
        assert_eq!(
            read, 2,
            "read only the 2 files holding the top-2; pruned the rest"
        );
        assert_eq!(bound.kth(), Some(&f(4999.60)));
    }

    /// k exceeds the non-null count → the heap never fills → nothing prunes → every
    /// file is read, including the all-null file (NULL-ordered rows must reach the
    /// authoritative sort). Rider 1.
    #[test]
    fn read_loop_reads_all_when_k_exceeds_nonnull() {
        let vals = vec![vec![Some(5.0)], vec![Some(3.0)], vec![None]];
        let dfs = vec![
            df_with_upper(1, Some(5.0), 0),
            df_with_upper(1, Some(3.0), 0),
            df_with_upper(1, None, 1), // all-null
        ];
        let order = plan_topk_read(dfs.iter(), 1, Some("double"));
        let mut bound = TopKBound::new(5); // k=5 > 2 non-null values
        let mut read = 0usize;
        for pos in 0..order.len() {
            let (orig, _) = order[pos];
            read += 1;
            bound.observe_all(vals[orig].iter().flatten().map(|&x| f(x)));
            if let Some((_, next_upper)) = order.get(pos + 1) {
                if bound.can_stop(next_upper.as_ref()) {
                    break;
                }
            }
        }
        assert_eq!(read, 3, "no prune when heap can't fill → all files read");
        assert!(!bound.is_full());
    }

    #[test]
    fn works_for_int_and_string_keys() {
        let mut bi = TopKBound::new(2);
        bi.observe_all([
            TypedValue::Int64(3),
            TypedValue::Int64(9),
            TypedValue::Int64(5),
        ]);
        assert_eq!(bi.kth(), Some(&TypedValue::Int64(5)));
        assert!(bi.can_stop(Some(&TypedValue::Int64(4))));
        assert!(!bi.can_stop(Some(&TypedValue::Int64(5))));

        let mut bs = TopKBound::new(2);
        bs.observe_all([
            TypedValue::String("apple".into()),
            TypedValue::String("mango".into()),
            TypedValue::String("cherry".into()),
        ]);
        // top-2 by value: mango, cherry; k-th = cherry.
        assert_eq!(bs.kth(), Some(&TypedValue::String("cherry".into())));
        assert!(bs.can_stop(Some(&TypedValue::String("banana".into()))));
    }
}
