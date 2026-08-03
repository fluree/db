//! Scan-side top-k bound engine (PR-5; ASC added in item 8, F-AUD-6).
//!
//! For a single-column `ORDER BY … LIMIT k` pushed to the scan, the reader visits
//! files in best-first bound order (DESC: `upper_bound(sort_col)` descending; ASC:
//! `lower_bound(sort_col)` ascending) and maintains the running k-th bound (the
//! worst of the k best sort values seen so far — smallest of the k largest for
//! DESC, largest of the k smallest for ASC). Once the heap is full, any *unread*
//! file whose best-possible bound is strictly worse than the k-th bound cannot
//! contain a top-k row, so — because files are visited best-first — the reader
//! stops.
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
//! Both directions are supported, gated at admission:
//! - **DESC** is admitted for any scalar column. SPARQL orders unbound (NULL)
//!   values LAST under DESC, so they never form the descending bound and a
//!   NULL-bearing file is handled by the `is_full`/no-bound-last rules.
//! - **ASC** (item 8, F-AUD-6) is admitted ONLY when the sort column is REQUIRED
//!   (non-nullable per the Iceberg schema) — the provider enforces this gate. A
//!   required column has no NULL rows, so the SPARQL "unbound first" ordering
//!   cannot place an unread NULL ahead of the top-k, and the ASC mirror (read by
//!   `lower_bound` ASC, stop when the next `lower_bound` is strictly ABOVE the
//!   k-th bound) is sound. Ignoring the directive is always correct — the
//!   `SortOperator` above applies the exact order + LIMIT.

use crate::io::batch::{Column, ColumnBatch};
use crate::manifest::value_codec::TypedValue;
use crate::manifest::{decode_by_type_string, DataFile};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Directive handed to the scan for a single-column top-k pushdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopKConfig {
    /// Iceberg field id of the primary sort column.
    pub sort_field_id: i32,
    /// How many top rows the bound must retain — the query's `LIMIT + OFFSET`.
    pub k: usize,
    /// `true` for `ASC` (read by `lower_bound`), `false` for `DESC` (by
    /// `upper_bound`). ASC is only ever set for a REQUIRED column (provider gate).
    pub ascending: bool,
}

/// Total-order wrapper over a same-typed `TypedValue`, oriented **worst-first**:
/// the heap root is the value the bound should evict — the current k-th bound.
/// For DESC (retain the k LARGEST) the worst is the smallest; for ASC (retain the
/// k SMALLEST) the worst is the largest, so the value order is reversed. In
/// practice one column has one type; `partial_cmp` is total except for float NaN,
/// which folds to `Equal` (a NaN sort key only ever *weakens* pruning — the
/// file-bound compare is NaN-safe and keeps — never breaks correctness).
#[derive(Debug, Clone)]
struct OrdKey {
    v: TypedValue,
    ascending: bool,
}

impl OrdKey {
    fn new(v: TypedValue, ascending: bool) -> Self {
        Self { v, ascending }
    }
}

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
        let base = self.v.partial_cmp(&other.v).unwrap_or(Ordering::Equal);
        // Worst-first: DESC evicts the smallest value (→ smallest is the "greatest"
        // key → reverse); ASC evicts the largest value (→ largest is "greatest" →
        // as-is).
        if self.ascending {
            base
        } else {
            base.reverse()
        }
    }
}

/// The running k-th bound over the sort column's non-null values.
#[derive(Debug)]
pub struct TopKBound {
    k: usize,
    ascending: bool,
    /// Max-heap of `OrdKey`s in worst-first order — the root is the worst retained
    /// value (the current k-th bound once full).
    heap: BinaryHeap<OrdKey>,
}

impl TopKBound {
    pub fn new(k: usize, ascending: bool) -> Self {
        Self {
            k,
            ascending,
            heap: BinaryHeap::with_capacity(k.saturating_add(1)),
        }
    }

    /// Fold one non-null sort value into the running top-k.
    pub fn observe(&mut self, v: TypedValue) {
        if self.k == 0 {
            return;
        }
        let key = OrdKey::new(v, self.ascending);
        if self.heap.len() < self.k {
            self.heap.push(key);
            return;
        }
        // Replace the worst retained (heap root) if the new value is strictly
        // BETTER — i.e. a "smaller" worst-first key (larger for DESC, smaller for
        // ASC). A tie never replaces.
        if let Some(worst) = self.heap.peek() {
            if key.cmp(worst) == Ordering::Less {
                self.heap.pop();
                self.heap.push(key);
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

    /// The k-th bound (worst of the retained top-k), or `None` until full.
    pub fn kth(&self) -> Option<&TypedValue> {
        if self.is_full() {
            self.heap.peek().map(|k| &k.v)
        } else {
            None
        }
    }

    /// Whether the scan may stop before reading the next file, whose best-possible
    /// sort value in the read direction (`upper_bound` for DESC, `lower_bound` for
    /// ASC) is `next_bound`. Stop iff the heap is full and `next_bound` is strictly
    /// WORSE than the k-th bound (below it for DESC, above it for ASC). A `None`
    /// next bound (all-NULL column / missing stats) never stops (must read); a tie
    /// (`==`) never stops (over-keep); a NaN compare never stops.
    pub fn can_stop(&self, next_bound: Option<&TypedValue>) -> bool {
        match (self.kth(), next_bound) {
            (Some(kth), Some(nb)) => {
                OrdKey::new(nb.clone(), self.ascending)
                    .cmp(&OrdKey::new(kth.clone(), self.ascending))
                    == Ordering::Greater
            }
            _ => false,
        }
    }
}

/// Plan the top-k read order over a set of data files: pairs of `(original_index,
/// decoded best-possible sort bound)` sorted so the best-possible file is read
/// first — highest `upper_bound` first for DESC, lowest `lower_bound` first for
/// ASC — and **files with no bound (an all-NULL column or missing stats) come
/// LAST**: they can never stop the scan (see [`TopKBound::can_stop`]) and must be
/// read. The read loop visits the returned order and, after each file, consults
/// the NEXT pair's bound to decide whether to stop.
pub fn plan_topk_read<'a>(
    data_files: impl Iterator<Item = &'a DataFile>,
    sort_field_id: i32,
    sort_type: Option<&str>,
    ascending: bool,
) -> Vec<(usize, Option<TypedValue>)> {
    let mut order: Vec<(usize, Option<TypedValue>)> = data_files
        .enumerate()
        .map(|(i, df)| {
            let bytes = if ascending {
                df.lower_bound(sort_field_id)
            } else {
                df.upper_bound(sort_field_id)
            };
            let bound = bytes.and_then(|b| decode_by_type_string(b, sort_type).ok());
            (i, bound)
        })
        .collect();
    order.sort_by(|(_, a), (_, b)| match (a, b) {
        // Best-first: ASC smallest lower_bound first, DESC largest upper_bound
        // first. A stable sort preserves the manifest order within ties.
        (Some(x), Some(y)) => {
            let base = x.partial_cmp(y).unwrap_or(Ordering::Equal);
            if ascending {
                base
            } else {
                base.reverse()
            }
        }
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
        let mut b = TopKBound::new(3, false);
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
        let mut b = TopKBound::new(3, false);
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
        let mut b = TopKBound::new(3, false);
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
        let mut b = TopKBound::new(0, false);
        b.observe(f(10.0));
        assert!(!b.is_full());
        assert_eq!(b.kth(), None);
        assert!(!b.can_stop(Some(&f(1.0))));
    }

    #[test]
    fn nan_never_over_prunes() {
        // A NaN sort value folded in must not produce a bound that prunes a file
        // holding real values (the file-bound compare stays NaN-safe → keep).
        let mut b = TopKBound::new(2, false);
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
        let dfs = [
            df_with_upper(1, Some(10.0), 0), // 0
            df_with_upper(1, Some(30.0), 0), // 1
            df_with_upper(1, None, 5),       // 2 — all-null column, no upper_bound
            df_with_upper(1, Some(20.0), 0), // 3
        ];
        let order = plan_topk_read(dfs.iter(), 1, Some("double"), false);
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
        let vals = [
            vec![Some(4999.98), Some(100.0)], // 0: holds the max
            vec![Some(4999.60), Some(50.0)],  // 1: holds the 2nd
            vec![Some(3000.0), Some(10.0)],   // 2: below the top-2 once full
            vec![None, None],                 // 3: all-null
        ];
        let dfs = [
            df_with_upper(1, Some(4999.98), 0),
            df_with_upper(1, Some(4999.60), 0),
            df_with_upper(1, Some(3000.0), 0),
            df_with_upper(1, None, 2),
        ];
        let order = plan_topk_read(dfs.iter(), 1, Some("double"), false);
        let mut bound = TopKBound::new(2, false);
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
        let vals = [vec![Some(5.0)], vec![Some(3.0)], vec![None]];
        let dfs = [
            df_with_upper(1, Some(5.0), 0),
            df_with_upper(1, Some(3.0), 0),
            df_with_upper(1, None, 1), // all-null
        ];
        let order = plan_topk_read(dfs.iter(), 1, Some("double"), false);
        let mut bound = TopKBound::new(5, false); // k=5 > 2 non-null values
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
        let mut bi = TopKBound::new(2, false);
        bi.observe_all([
            TypedValue::Int64(3),
            TypedValue::Int64(9),
            TypedValue::Int64(5),
        ]);
        assert_eq!(bi.kth(), Some(&TypedValue::Int64(5)));
        assert!(bi.can_stop(Some(&TypedValue::Int64(4))));
        assert!(!bi.can_stop(Some(&TypedValue::Int64(5))));

        let mut bs = TopKBound::new(2, false);
        bs.observe_all([
            TypedValue::String("apple".into()),
            TypedValue::String("mango".into()),
            TypedValue::String("cherry".into()),
        ]);
        // top-2 by value: mango, cherry; k-th = cherry.
        assert_eq!(bs.kth(), Some(&TypedValue::String("cherry".into())));
        assert!(bs.can_stop(Some(&TypedValue::String("banana".into()))));
    }

    // ---- Item 8 (F-AUD-6): ASC top-k (the DESC mirror) ----

    fn df_with_lower(field_id: i32, lower: Option<f64>, null_count: i64) -> DataFile {
        use std::collections::HashMap;
        let lower_bounds = lower.map(|l| {
            let mut m = HashMap::new();
            m.insert(field_id, l.to_le_bytes().to_vec());
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
            lower_bounds,
            upper_bounds: None,
            split_offsets: None,
            sort_order_id: None,
        }
    }

    #[test]
    fn asc_bound_retains_smallest_and_stops_above_kth() {
        // ASC retains the k SMALLEST; the k-th bound is the LARGEST retained.
        let mut b = TopKBound::new(3, true);
        b.observe_all([f(10.0), f(20.0), f(30.0)]);
        assert_eq!(b.kth(), Some(&f(30.0)), "k-th = largest of the bottom-3");
        b.observe(f(5.0)); // evicts 30 → {5,10,20}, k-th = 20
        assert_eq!(b.kth(), Some(&f(20.0)));
        b.observe(f(25.0)); // above k-th (worse for ASC) → ignored
        assert_eq!(b.kth(), Some(&f(20.0)));
        // Next file's lowest value strictly ABOVE the k-th → STOP; a tie / below
        // never stops.
        assert!(b.can_stop(Some(&f(20.001))));
        assert!(!b.can_stop(Some(&f(20.0))));
        assert!(!b.can_stop(Some(&f(1.0))));
        assert!(!b.can_stop(None));
    }

    #[test]
    fn asc_plan_orders_by_lower_bound_ascending_no_bound_last() {
        let dfs = [
            df_with_lower(1, Some(10.0), 0), // 0
            df_with_lower(1, Some(30.0), 0), // 1
            df_with_lower(1, None, 0),       // 2 — no lower_bound (missing stats)
            df_with_lower(1, Some(20.0), 0), // 3
        ];
        let order = plan_topk_read(dfs.iter(), 1, Some("double"), true);
        let idxs: Vec<usize> = order.iter().map(|(i, _)| *i).collect();
        // ASC by lower_bound: 10(0), 20(3), 30(1); the no-bound file (2) LAST.
        assert_eq!(idxs, vec![0, 3, 1, 2]);
        assert_eq!(order.last().unwrap().1, None);
    }

    #[test]
    fn asc_read_loop_prunes_high_files_after_heap_fills() {
        // Files ordered by ascending lower_bound; each of the first two holds one
        // of the two smallest values (plus a high decoy). Once the bottom-2 are
        // seen, the higher file is provably not in the ASC top-2 and is pruned.
        let vals = [
            vec![Some(1.0), Some(5000.0)], // 0: holds the min (+ high decoy)
            vec![Some(2.0), Some(6000.0)], // 1: holds the 2nd (+ high decoy)
            vec![Some(3000.0)],            // 2: above the bottom-2 once full
        ];
        let dfs = [
            df_with_lower(1, Some(1.0), 0),
            df_with_lower(1, Some(2.0), 0),
            df_with_lower(1, Some(3000.0), 0),
        ];
        let order = plan_topk_read(dfs.iter(), 1, Some("double"), true);
        let mut bound = TopKBound::new(2, true);
        let mut read = 0usize;
        for pos in 0..order.len() {
            let (orig, _) = order[pos];
            read += 1;
            bound.observe_all(vals[orig].iter().flatten().map(|&x| f(x)));
            if let Some((_, next_lower)) = order.get(pos + 1) {
                if bound.can_stop(next_lower.as_ref()) {
                    break;
                }
            }
        }
        assert_eq!(read, 2, "read only the 2 files holding the ASC top-2");
        assert_eq!(bound.kth(), Some(&f(2.0)), "k-th = larger of {{1,2}}");
    }
}
