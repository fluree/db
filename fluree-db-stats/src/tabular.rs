//! Profiling face for [`fluree_db_tabular`] column batches: the shape a
//! lake-table scan yields. One call per column per batch; batches from
//! different files or row groups fold into the same profile.

use fluree_db_tabular::Column;

use crate::grouped::GroupedProfile;
use crate::profile::{ColumnProfile, ProfileValue};

const MILLIS_PER_DAY: i64 = 86_400_000;

/// The cell at `row` as the profiler sees it. Decimals read as floats
/// (unscaled value over ten to the scale); dates and timestamps become
/// epoch milliseconds; bytes are counted but not inspected.
pub fn value_at(col: &Column, row: usize) -> ProfileValue<'_> {
    match col {
        Column::Boolean(v) => v[row].map_or(ProfileValue::Null, ProfileValue::Bool),
        Column::Int32(v) => v[row].map_or(ProfileValue::Null, |x| ProfileValue::Int(i64::from(x))),
        Column::Int64(v) => v[row].map_or(ProfileValue::Null, ProfileValue::Int),
        Column::Float32(v) => {
            v[row].map_or(ProfileValue::Null, |x| ProfileValue::Float(f64::from(x)))
        }
        Column::Float64(v) => v[row].map_or(ProfileValue::Null, ProfileValue::Float),
        Column::String(v) => v[row]
            .as_deref()
            .map_or(ProfileValue::Null, ProfileValue::Str),
        Column::Bytes(v) => v[row]
            .as_ref()
            .map_or(ProfileValue::Null, |_| ProfileValue::Other("<bytes>")),
        Column::Date(v) => v[row].map_or(ProfileValue::Null, |d| {
            ProfileValue::Temporal(i64::from(d) * MILLIS_PER_DAY)
        }),
        Column::Timestamp(v) | Column::TimestampTz(v) => {
            v[row].map_or(ProfileValue::Null, |us| ProfileValue::Temporal(us / 1000))
        }
        Column::Decimal { values, scale, .. } => values[row].map_or(ProfileValue::Null, |u| {
            ProfileValue::Float(u as f64 / 10f64.powi(i32::from(*scale)))
        }),
    }
}

/// The cell at `row` as text, for building group keys. Nulls read as the
/// empty string.
pub fn display_at(col: &Column, row: usize) -> String {
    match value_at(col, row) {
        ProfileValue::Null => String::new(),
        ProfileValue::Bool(b) => b.to_string(),
        ProfileValue::Int(i) | ProfileValue::Temporal(i) => i.to_string(),
        ProfileValue::Float(f) => f.to_string(),
        ProfileValue::Str(s) | ProfileValue::Ref(s) | ProfileValue::Other(s) => s.to_string(),
    }
}

/// Fold every cell of `col` into `profile`.
pub fn profile_column(profile: &mut ColumnProfile, col: &Column) {
    for row in 0..col.len() {
        profile.observe(value_at(col, row));
    }
}

/// Fold every cell of `col` into `grouped`, keyed by the same row of the
/// `key_cols`, joined with `" | "` when there are several.
pub fn profile_column_grouped(grouped: &mut GroupedProfile, col: &Column, key_cols: &[&Column]) {
    let mut key = String::new();
    for row in 0..col.len() {
        key.clear();
        for (i, k) in key_cols.iter().enumerate() {
            if i > 0 {
                key.push_str(" | ");
            }
            key.push_str(&display_at(k, row));
        }
        grouped.observe(&key, value_at(col, row));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_reads_as_scaled_float() {
        let col = Column::Decimal {
            values: vec![Some(12_345), None],
            precision: 10,
            scale: 2,
        };
        assert_eq!(value_at(&col, 0), ProfileValue::Float(123.45));
        assert_eq!(value_at(&col, 1), ProfileValue::Null);
    }

    #[test]
    fn date_and_timestamp_become_millis() {
        assert_eq!(
            value_at(&Column::Date(vec![Some(1)]), 0),
            ProfileValue::Temporal(MILLIS_PER_DAY)
        );
        assert_eq!(
            value_at(&Column::Timestamp(vec![Some(5_000)]), 0),
            ProfileValue::Temporal(5)
        );
    }

    #[test]
    fn grouped_by_two_columns() {
        let price = Column::Float64(vec![Some(1.0), Some(2.0), Some(30.0)]);
        let part = Column::String(vec![Some("a".into()), Some("a".into()), Some("b".into())]);
        let div = Column::String(vec![Some("rome".into()); 3]);
        let mut g = GroupedProfile::default();
        profile_column_grouped(&mut g, &price, &[&part, &div]);
        assert_eq!(g.group_count(), 2);
        assert_eq!(g.group("a | rome").unwrap().count(), 2);
        assert_eq!(g.group("b | rome").unwrap().numeric().mean(), Some(30.0));
    }
}
