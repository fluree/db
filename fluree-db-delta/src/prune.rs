//! Which rows of a data file to decode.
//!
//! File skipping works from the log's per-file statistics. Inside a file that
//! is read, the Parquet footer's row-group statistics and its page index bound
//! each column again, at finer grain. A row group or page is left out only when
//! those bounds prove no row in it can pass a pushed filter; anything missing,
//! or of a type not compared here, keeps its rows.

use std::cmp::Ordering;

use delta_kernel::expressions::Scalar;
use delta_kernel::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use delta_kernel::parquet::basic::{LogicalType, TimeUnit, Type as PhysicalType};
use delta_kernel::parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use delta_kernel::parquet::file::page_index::column_index::ColumnIndexMetaData;
use delta_kernel::parquet::file::statistics::Statistics;
use delta_kernel::parquet::schema::types::ColumnDescriptor;

use crate::filter::FilterOp;

/// One pushed filter, against a column by the name it has inside data files.
#[derive(Debug, Clone)]
pub(crate) struct Term {
    pub(crate) column: String,
    pub(crate) op: FilterOp,
    pub(crate) values: Vec<Scalar>,
}

/// A bound read from statistics, in the few shapes compared here.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Bound<'a> {
    Int(i64),
    Double(f64),
    Bytes(&'a [u8]),
}

/// The row groups to read and, when the page index rules pages out within
/// them, the rows to read of those groups.
pub(crate) struct Plan {
    pub(crate) row_groups: Vec<usize>,
    pub(crate) rows: Option<RowSelection>,
}

pub(crate) fn plan(file: &ParquetMetaData, terms: &[Term]) -> Option<Plan> {
    let terms: Vec<(usize, &Term)> = terms
        .iter()
        .filter(|term| term.op != FilterOp::NotEq)
        .filter_map(|term| Some((leaf(file, &term.column)?, term)))
        .collect();
    if terms.is_empty() {
        return None;
    }
    let row_groups: Vec<usize> = file
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(_, group)| {
            terms
                .iter()
                .all(|(leaf, term)| group_may_match(group, *leaf, term))
        })
        .map(|(ordinal, _)| ordinal)
        .collect();
    let rows = terms
        .iter()
        .filter_map(|(leaf, term)| page_selection(file, &row_groups, *leaf, term))
        .reduce(|a, b| a.intersection(&b));
    if row_groups.len() == file.num_row_groups() && rows.is_none() {
        return None;
    }
    Some(Plan { row_groups, rows })
}

/// The top-level primitive column called `name`.
fn leaf(file: &ParquetMetaData, name: &str) -> Option<usize> {
    file.file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.path().parts() == [name])
}

fn group_may_match(group: &RowGroupMetaData, leaf: usize, term: &Term) -> bool {
    let chunk = group.column(leaf);
    // Writers before PARQUET-686 (parquet-mr < 1.10) put string bounds in the
    // deprecated fields in signed byte order, which are not bounds under the
    // unsigned order compared here.
    let Some(stats) = chunk.statistics().filter(|s| !s.is_min_max_deprecated()) else {
        return true;
    };
    let descriptor = chunk.column_descr();
    let bounds = match stats {
        Statistics::Int32(s) => s
            .min_opt()
            .zip(s.max_opt())
            .map(|(min, max)| (Bound::Int((*min).into()), Bound::Int((*max).into()))),
        Statistics::Int64(s) => s
            .min_opt()
            .zip(s.max_opt())
            .map(|(min, max)| (Bound::Int(*min), Bound::Int(*max))),
        Statistics::Double(s) => s
            .min_opt()
            .zip(s.max_opt())
            .map(|(min, max)| (Bound::Double(*min), Bound::Double(*max))),
        Statistics::ByteArray(s) => s
            .min_opt()
            .zip(s.max_opt())
            .map(|(min, max)| (Bound::Bytes(min.data()), Bound::Bytes(max.data()))),
        _ => None,
    };
    bounds.is_none_or(|(min, max)| may_match(descriptor, term, min, max))
}

/// The rows of `row_groups` whose pages may hold a match for `term`, or `None`
/// when the page index rules nothing out.
fn page_selection(
    file: &ParquetMetaData,
    row_groups: &[usize],
    leaf: usize,
    term: &Term,
) -> Option<RowSelection> {
    let column_index = file.column_index()?;
    let offset_index = file.offset_index()?;
    let mut selectors = Vec::new();
    let mut ruled_out = false;
    for &ordinal in row_groups {
        let group = file.row_group(ordinal);
        let rows = usize::try_from(group.num_rows()).ok()?;
        let keep = column_index
            .get(ordinal)
            .and_then(|columns| columns.get(leaf))
            .and_then(|index| pages_may_match(group.column(leaf).column_descr(), index, term));
        let starts: Option<Vec<usize>> = offset_index
            .get(ordinal)
            .and_then(|columns| columns.get(leaf))
            .map(|pages| {
                pages
                    .page_locations()
                    .iter()
                    .map(|page| usize::try_from(page.first_row_index).unwrap_or(usize::MAX))
                    .collect()
            });
        match (keep, starts) {
            (Some(keep), Some(starts))
                if keep.len() == starts.len()
                    && starts.first() == Some(&0)
                    && starts.windows(2).all(|w| w[0] < w[1])
                    && starts.last().is_some_and(|last| *last < rows) =>
            {
                for (page, kept) in keep.iter().enumerate() {
                    let end = starts.get(page + 1).copied().unwrap_or(rows);
                    let len = end - starts[page];
                    ruled_out |= !kept;
                    selectors.push(if *kept {
                        RowSelector::select(len)
                    } else {
                        RowSelector::skip(len)
                    });
                }
            }
            _ => selectors.push(RowSelector::select(rows)),
        }
    }
    ruled_out.then(|| RowSelection::from(selectors))
}

/// Per page, whether it may hold a match. `None` when the index carries no
/// bounds of a kind compared here.
fn pages_may_match(
    descriptor: &ColumnDescriptor,
    index: &ColumnIndexMetaData,
    term: &Term,
) -> Option<Vec<bool>> {
    fn each<'a>(
        pages: u64,
        bound: impl Fn(usize) -> Option<(Bound<'a>, Bound<'a>)>,
        all_null: impl Fn(usize) -> bool,
        descriptor: &ColumnDescriptor,
        term: &Term,
    ) -> Option<Vec<bool>> {
        let pages = usize::try_from(pages).ok()?;
        Some(
            (0..pages)
                .map(|page| {
                    // A comparison never matches a null.
                    !all_null(page)
                        && bound(page)
                            .is_none_or(|(min, max)| may_match(descriptor, term, min, max))
                })
                .collect(),
        )
    }
    match index {
        ColumnIndexMetaData::INT32(i) => each(
            i.num_pages(),
            |p| {
                Some((
                    Bound::Int((*i.min_value(p)?).into()),
                    Bound::Int((*i.max_value(p)?).into()),
                ))
            },
            |p| i.is_null_page(p),
            descriptor,
            term,
        ),
        ColumnIndexMetaData::INT64(i) => each(
            i.num_pages(),
            |p| Some((Bound::Int(*i.min_value(p)?), Bound::Int(*i.max_value(p)?))),
            |p| i.is_null_page(p),
            descriptor,
            term,
        ),
        ColumnIndexMetaData::DOUBLE(i) => each(
            i.num_pages(),
            |p| {
                Some((
                    Bound::Double(*i.min_value(p)?),
                    Bound::Double(*i.max_value(p)?),
                ))
            },
            |p| i.is_null_page(p),
            descriptor,
            term,
        ),
        ColumnIndexMetaData::BYTE_ARRAY(i) => each(
            i.num_pages(),
            |p| Some((Bound::Bytes(i.min_value(p)?), Bound::Bytes(i.max_value(p)?))),
            |p| i.is_null_page(p),
            descriptor,
            term,
        ),
        _ => None,
    }
}

/// Whether a value in `[min, max]` can pass `term`. Bounds may be looser than
/// the true extremes (writers truncate long strings), never tighter, so every
/// answer errs towards `true`.
fn may_match(descriptor: &ColumnDescriptor, term: &Term, min: Bound<'_>, max: Bound<'_>) -> bool {
    let mut values = term
        .values
        .iter()
        .map(|value| comparable(descriptor, value));
    match term.op {
        FilterOp::In => values.any(|value| value.is_none_or(|v| within(min, v, max))),
        FilterOp::NotEq => true,
        op => match values.next().flatten() {
            None => true,
            Some(v) => match op {
                FilterOp::Eq => within(min, v, max),
                FilterOp::Lt => !matches!(order(min, v), Some(Ordering::Equal | Ordering::Greater)),
                FilterOp::LtEq => order(min, v) != Some(Ordering::Greater),
                FilterOp::Gt => !matches!(order(max, v), Some(Ordering::Equal | Ordering::Less)),
                FilterOp::GtEq => order(max, v) != Some(Ordering::Less),
                FilterOp::In | FilterOp::NotEq => true,
            },
        },
    }
}

fn within(min: Bound<'_>, value: Bound<'_>, max: Bound<'_>) -> bool {
    order(min, value) != Some(Ordering::Greater) && order(max, value) != Some(Ordering::Less)
}

/// `None` when the two cannot be ordered: different kinds, or a NaN.
fn order(bound: Bound<'_>, value: Bound<'_>) -> Option<Ordering> {
    match (bound, value) {
        (Bound::Int(a), Bound::Int(b)) => Some(a.cmp(&b)),
        (Bound::Double(a), Bound::Double(b)) => a.partial_cmp(&b),
        // Parquet orders strings by unsigned byte, which is UTF-8 order.
        (Bound::Bytes(a), Bound::Bytes(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// `value` in the form this column's statistics take, when the column stores
/// it the plain way: a timestamp only as INT64 microseconds (INT96 and other
/// units are left alone), a string only as a byte array.
fn comparable<'a>(descriptor: &ColumnDescriptor, value: &'a Scalar) -> Option<Bound<'a>> {
    let physical = descriptor.physical_type();
    let integer = matches!(physical, PhysicalType::INT32 | PhysicalType::INT64);
    match value {
        Scalar::Long(v) if integer => Some(Bound::Int(*v)),
        Scalar::Integer(v) | Scalar::Date(v) if integer => Some(Bound::Int((*v).into())),
        Scalar::Short(v) if integer => Some(Bound::Int((*v).into())),
        Scalar::Byte(v) if integer => Some(Bound::Int((*v).into())),
        Scalar::Timestamp(v) | Scalar::TimestampNtz(v)
            if physical == PhysicalType::INT64
                && matches!(
                    descriptor.logical_type_ref(),
                    Some(LogicalType::Timestamp {
                        unit: TimeUnit::MICROS,
                        ..
                    })
                ) =>
        {
            Some(Bound::Int(*v))
        }
        Scalar::Double(v) if physical == PhysicalType::DOUBLE => Some(Bound::Double(*v)),
        Scalar::String(v) if physical == PhysicalType::BYTE_ARRAY => {
            Some(Bound::Bytes(v.as_bytes()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::arrow::array::{
        ArrayRef, Int64Array, RecordBatch, TimestampMillisecondArray,
    };
    use delta_kernel::parquet::arrow::ArrowWriter;
    use delta_kernel::parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
    use delta_kernel::parquet::file::properties::{EnabledStatistics, WriterProperties};

    use super::*;

    /// Three row groups of 100 rows in pages of 10: `id` 0..300, `amount` ten
    /// times it, `at` the same as `id` in milliseconds.
    fn file(statistics: EnabledStatistics) -> ParquetMetaData {
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..300)) as ArrayRef,
            ),
            (
                "amount",
                Arc::new(Int64Array::from_iter_values((0..300).map(|id| id * 10))) as ArrayRef,
            ),
            (
                "at",
                Arc::new(TimestampMillisecondArray::from_iter_values(0..300)) as ArrayRef,
            ),
        ])
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(100))
            .set_data_page_row_count_limit(10)
            .set_write_batch_size(10)
            .set_statistics_enabled(statistics)
            .build();
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&bytes::Bytes::from(bytes))
            .unwrap()
    }

    fn term(column: &str, op: FilterOp, value: Scalar) -> Term {
        Term {
            column: column.to_string(),
            op,
            values: vec![value],
        }
    }

    #[test]
    fn a_file_without_a_page_index_is_pruned_by_row_group() {
        let file = file(EnabledStatistics::Chunk);
        assert!(file
            .column_index()
            .into_iter()
            .flatten()
            .flatten()
            .all(|index| matches!(index, ColumnIndexMetaData::NONE)));
        let plan = plan(&file, &[term("id", FilterOp::Eq, Scalar::Long(150))]).expect("a plan");
        assert_eq!(plan.row_groups, [1]);
        assert!(plan.rows.is_none());
        let plan = plan_of(&file, FilterOp::Gt, 199);
        assert_eq!(plan.row_groups, [2]);
        let plan = plan_of(&file, FilterOp::LtEq, 100);
        assert_eq!(plan.row_groups, [0, 1]);
    }

    fn plan_of(file: &ParquetMetaData, op: FilterOp, id: i64) -> Plan {
        plan(file, &[term("id", op, Scalar::Long(id))]).expect("a plan")
    }

    /// Each filter rules out pages the other admits; only rows both admit are read.
    #[test]
    fn filters_on_two_columns_keep_the_pages_both_admit() {
        let file = file(EnabledStatistics::Page);
        let terms = [
            term("id", FilterOp::GtEq, Scalar::Long(130)),
            term("amount", FilterOp::LtEq, Scalar::Long(1590)),
        ];
        let plan = plan(&file, &terms).expect("a plan");
        assert_eq!(plan.row_groups, [1]);
        assert_eq!(
            plan.rows,
            Some(RowSelection::from(vec![
                RowSelector::skip(30),
                RowSelector::select(30),
                RowSelector::skip(40),
            ]))
        );
    }

    /// Statistics in another unit are numbers on another scale: 150 000 µs is
    /// inside this file, though no bound in milliseconds reaches it.
    #[test]
    fn a_timestamp_not_stored_as_microseconds_is_not_compared() {
        for statistics in [EnabledStatistics::Chunk, EnabledStatistics::Page] {
            let file = file(statistics);
            let instant = term("at", FilterOp::Eq, Scalar::Timestamp(150_000));
            assert!(plan(&file, &[instant]).is_none());
        }
    }

    #[test]
    fn a_column_the_file_lacks_rules_nothing_out() {
        let file = file(EnabledStatistics::Page);
        assert!(plan(&file, &[term("absent", FilterOp::Eq, Scalar::Long(1))]).is_none());
    }

    /// One row group of `label` in {"apple", "banana", "émile"}, its chunk
    /// statistics replaced by the given bounds.
    fn labels_with_bounds(min: &str, max: &str, deprecated: bool) -> ParquetMetaData {
        use delta_kernel::arrow::array::StringArray;
        use delta_kernel::parquet::data_type::ByteArray;

        let batch = RecordBatch::try_from_iter([(
            "label",
            Arc::new(StringArray::from(vec!["apple", "banana", "émile"])) as ArrayRef,
        )])
        .unwrap();
        let properties = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = ParquetMetaDataReader::new()
            .parse_and_finish(&bytes::Bytes::from(bytes))
            .unwrap();

        let stats = Statistics::byte_array(
            Some(ByteArray::from(min)),
            Some(ByteArray::from(max)),
            None,
            Some(0),
            deprecated,
        );
        let mut group = file.row_group(0).clone().into_builder();
        let column = group.take_columns().remove(0);
        let column = column.into_builder().set_statistics(stats).build().unwrap();
        let group = group.set_column_metadata(vec![column]).build().unwrap();
        file.into_builder().set_row_groups(vec![group]).build()
    }

    /// A pre-PARQUET-686 writer ordered "émile" before "banana" (signed bytes),
    /// so its bounds exclude "apple", which the row group holds.
    #[test]
    fn deprecated_signed_order_string_bounds_rule_nothing_out() {
        let apple = [term("label", FilterOp::Eq, Scalar::String("apple".into()))];
        let legacy = labels_with_bounds("émile", "banana", true);
        assert!(plan(&legacy, &apple).is_none());

        // The same bounds, not marked deprecated, would be trusted.
        let trusted = labels_with_bounds("émile", "banana", false);
        assert_eq!(
            plan(&trusted, &apple).expect("a plan").row_groups,
            [] as [usize; 0]
        );
    }
}
