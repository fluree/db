//! A triples map's row filter over an Iceberg table.
//!
//! An Iceberg source has no SQL engine, so an `rr:sqlQuery` logical table is
//! accepted only in the restricted form [`RestrictedSelect`] parses: one table,
//! narrowed by a conjunction of column comparisons. This module resolves such a
//! logical table to the table it scans and types its conditions against that
//! table's schema, producing the [`ExactRowFilter`] the reader enforces on every
//! row.
//!
//! Every reader of a triples map's rows goes through
//! [`FlureeR2rmlProvider`](super::FlureeR2rmlProvider)'s scan, which resolves
//! the logical table here, so the filter cannot be bypassed by one consumer and
//! honored by another. The consumers that answer without reading rows (the
//! manifest `COUNT` shortcut, `/info` class counts, the trusted crawl's foreign
//! key shortcut) decline for a filtered map instead, since no metadata knows
//! which rows a filter keeps.
//!
//! [`check_query_map`] is every check a read makes of a map's query, as one
//! function: the validate dry-run reports its refusal as a diagnostic, and
//! registration refuses the mapping with it, against the schema an unpinned
//! read of the table reads now
//! ([`unpinned_read_schema`](super::r2rml::unpinned_read_schema)). The read
//! checks again against the schema of the snapshot it reads
//! ([`LogicalScan::resolve`], then [`LogicalScan::row_filter`]), since the
//! table can change after registration and a mapping registered by an address
//! that could not be read then was never checked. The three run the same
//! checks over the same schema, so they cannot disagree.

use std::sync::Arc;

use fluree_db_iceberg::metadata::{Schema, SchemaField};
use fluree_db_iceberg::scan::{ComparisonOp, ExactPredicate, ExactRowFilter, LiteralValue};
use fluree_db_r2rml::mapping::{
    CompiledR2rmlMapping, LogicalTable, RestrictedSelect, RowCmpOp, RowCondition, SqlLiteral,
    TriplesMap, UnsupportedSql,
};

/// Why a triples map's `rr:sqlQuery` is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryRefusal {
    /// What every surface reports: names the map, the query and the reason.
    pub(crate) message: String,
    /// The column the query names that the table does not have, when that is
    /// the reason.
    pub(crate) missing_column: Option<String>,
}

impl std::fmt::Display for QueryRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl QueryRefusal {
    fn other(message: String) -> Self {
        Self {
            message,
            missing_column: None,
        }
    }

    /// The same refusal, naming the triples map it is of.
    fn of_map(self, tm: &TriplesMap) -> Self {
        Self {
            message: format!("triples map <{}>: {}", tm.iri, self.message),
            ..self
        }
    }
}

/// Every check a read makes of triples map `tm`'s `rr:sqlQuery`, with the
/// message a read would give, naming the map:
///
/// - the query is in the restricted subset;
/// - every key declared on it (`f:key`) is over columns it selects
///   ([`check_declared_keys_selected`]);
/// - its select list holds every column the map reads ([`check_select_list`]);
/// - given `schema`, its `FROM` table's, every column it names exists, exactly
///   as written, and each literal types against its column
///   ([`LogicalScan::row_filter`]).
///
/// `schema` is `None` when the table could not be loaded; only the checks
/// that need no schema run then. `Ok(None)` for a map over a table.
pub(crate) fn check_query_map(
    compiled: &CompiledR2rmlMapping,
    tm: &TriplesMap,
    schema: Option<&Schema>,
) -> Result<Option<RestrictedSelect>, QueryRefusal> {
    let Some(sql) = tm.sql_query() else {
        return Ok(None);
    };
    let select = parse_restricted(sql).map_err(|e| QueryRefusal::other(e).of_map(tm))?;
    check_declared_keys_selected(tm, sql, &select).map_err(QueryRefusal::other)?;
    check_select_list(compiled, tm, &select).map_err(QueryRefusal::other)?;
    if let Some(schema) = schema {
        typed_row_filter(&select, schema).map_err(|e| e.of_map(tm))?;
    }
    Ok(Some(select))
}

/// A logical table as an Iceberg source reads it: the table to scan, and the
/// query that narrows it when the logical table is an `rr:sqlQuery`.
#[derive(Debug, Clone)]
pub(crate) struct LogicalScan {
    /// The table to scan: the `rr:tableName`, or the query's `FROM` table.
    pub(crate) table: String,
    /// The parsed query, for an `rr:sqlQuery` logical table.
    pub(crate) select: Option<Arc<RestrictedSelect>>,
}

impl LogicalScan {
    /// A plain `rr:tableName`.
    pub(crate) fn table(name: &str) -> Self {
        Self {
            table: name.to_string(),
            select: None,
        }
    }

    /// Resolve a name from [`TriplesMap::table_name`](fluree_db_r2rml::mapping::TriplesMap::table_name):
    /// a table, or the alias of one of `mapping`'s `rr:sqlQuery` logical tables.
    pub(crate) fn resolve(mapping: &CompiledR2rmlMapping, name: &str) -> Result<Self, String> {
        if !LogicalTable::is_sql_query_alias(name) {
            return Ok(Self::table(name));
        }
        let sql = mapping
            .sql_query_for_table(name)
            .ok_or_else(|| format!("no triples map has the logical table '{name}'"))?;
        let select = parse_restricted(sql)?;
        // Checked here too, not only at registration: a mapping registered by
        // an address that could not be read then is first checked now.
        for tm in mapping
            .triples_maps
            .values()
            .filter(|tm| tm.table_name() == Some(name))
        {
            check_select_list(mapping, tm, &select)?;
        }
        Ok(Self {
            table: select.table.clone(),
            select: Some(Arc::new(select)),
        })
    }

    /// Whether the logical table keeps only some of its table's rows.
    pub(crate) fn is_filtered(&self) -> bool {
        self.select
            .as_ref()
            .is_some_and(|select| !select.conditions.is_empty())
    }

    /// Whether the logical table's rows carry `column`: every column of the
    /// table for an `rr:tableName` or `SELECT *`, and only the select list's
    /// otherwise. A key over a column the query does not select is not one of
    /// the logical table's keys.
    pub(crate) fn selects(&self, column: &str) -> bool {
        match self
            .select
            .as_ref()
            .and_then(|select| select.columns.as_ref())
        {
            Some(columns) => columns.iter().any(|c| c == column),
            None => true,
        }
    }

    /// The row filter typed against `schema`, or `None` when the logical
    /// table keeps every row. See [`typed_row_filter`].
    pub(crate) fn row_filter(
        &self,
        schema: &Schema,
    ) -> Result<Option<ExactRowFilter>, QueryRefusal> {
        match &self.select {
            Some(select) => typed_row_filter(select, schema),
            None => Ok(None),
        }
    }
}

/// `select`'s row filter typed against its table's `schema`, or `None` when
/// it keeps every row. Checks every column the query names, select list
/// included, so a query naming a column the table lacks is refused even when
/// it filters nothing.
fn typed_row_filter(
    select: &RestrictedSelect,
    schema: &Schema,
) -> Result<Option<ExactRowFilter>, QueryRefusal> {
    for column in select.named_columns() {
        field(schema, &select.table, column)?;
    }
    let predicates = select
        .conditions
        .iter()
        .map(|condition| typed_predicate(schema, &select.table, condition))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ExactRowFilter::new(predicates))
}

/// Parse `sql` as the restricted subset, with a message saying what an
/// Iceberg source accepts.
fn parse_restricted(sql: &str) -> Result<RestrictedSelect, String> {
    RestrictedSelect::parse(sql).map_err(|e| unsupported_query_message(sql, &e))
}

/// Why `sql` is refused, and what an Iceberg source accepts instead.
pub(crate) fn unsupported_query_message(sql: &str, why: &UnsupportedSql) -> String {
    format!(
        "rr:sqlQuery \"{}\" is not one an Iceberg source can evaluate: {why}. It accepts \
         SELECT * | <columns> FROM <table> [WHERE <column> <op> <literal> [AND ...]], with \
         = <> != < <= > >= and IS [NOT] NULL",
        sql.trim()
    )
}

/// Refuse a mapping whose `rr:sqlQuery` logical tables are not all in the
/// restricted subset, whose select lists leave out a column their maps read,
/// or that declare a key (`f:key`) over a column the query does not select:
/// the checks of [`check_query_map`] that need no schema. Registration runs
/// them before anything is stored, then the rest once it has loaded the
/// tables.
pub(crate) fn check_restricted_queries(compiled: &CompiledR2rmlMapping) -> Result<(), String> {
    for tm in query_maps(compiled) {
        check_query_map(compiled, tm, None).map_err(|e| e.message)?;
    }
    Ok(())
}

/// `compiled`'s `rr:sqlQuery` triples maps, by IRI, so the first refusal
/// reported is always the same one.
pub(crate) fn query_maps(compiled: &CompiledR2rmlMapping) -> Vec<&TriplesMap> {
    let mut maps: Vec<&TriplesMap> = compiled
        .triples_maps
        .values()
        .filter(|tm| tm.sql_query().is_some())
        .collect();
    maps.sort_unstable_by(|a, b| a.iri.cmp(&b.iri));
    maps
}

/// Refuse a key declared (`f:key`) on an `rr:sqlQuery` over a column the
/// query does not select: it is no key of the logical table.
fn check_declared_keys_selected(
    tm: &TriplesMap,
    sql: &str,
    select: &RestrictedSelect,
) -> Result<(), String> {
    let Some(columns) = &select.columns else {
        return Ok(());
    };
    match tm
        .declared_keys
        .iter()
        .flatten()
        .find(|name| !columns.contains(name))
    {
        Some(name) => Err(format!(
            "triples map <{}>: f:key on rr:sqlQuery \"{}\" names column \"{name}\", which the \
             query does not select",
            tm.iri,
            sql.trim()
        )),
        None => Ok(()),
    }
}

/// Refuse a select list that leaves out a column the map reads.
///
/// In R2RML a query's result columns are the logical table's columns, so a
/// term map, or a join condition of a map that references this one, can only
/// name a column the query selects. `SELECT *` selects every column.
fn check_select_list(
    compiled: &CompiledR2rmlMapping,
    tm: &TriplesMap,
    select: &RestrictedSelect,
) -> Result<(), String> {
    let Some(selected) = &select.columns else {
        return Ok(());
    };
    let mut read: Vec<&str> = tm.referenced_columns();
    for other in compiled.triples_maps.values() {
        for pom in &other.predicate_object_maps {
            if let fluree_db_r2rml::mapping::ObjectMap::RefObjectMap(rom) = &pom.object_map {
                if rom.parent_triples_map == tm.iri {
                    read.extend(
                        rom.join_conditions
                            .iter()
                            .map(|jc| jc.parent_column.as_str()),
                    );
                }
            }
        }
    }
    read.sort_unstable();
    read.dedup();
    let missing: Vec<&str> = read
        .into_iter()
        .filter(|column| !selected.iter().any(|s| s == column))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(format!(
        "triples map <{}>: rr:sqlQuery selects {}, but the map reads {}, which it does not \
         select. A query's result columns are its logical table's columns: add them to the \
         select list, or use SELECT *",
        tm.iri,
        selected.join(", "),
        missing.join(", "),
    ))
}

/// The schema field `column` names, exactly as written.
fn field<'s>(
    schema: &'s Schema,
    table: &str,
    column: &str,
) -> Result<&'s SchemaField, QueryRefusal> {
    if let Some(field) = schema.field_by_name(column) {
        return Ok(field);
    }
    let hint = schema
        .fields
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(column))
        .map(|f| {
            format!(
                "; identifiers match the table's columns exactly, case included: did you mean \
                 \"{}\"?",
                f.name
            )
        })
        .unwrap_or_default();
    Err(QueryRefusal {
        message: format!(
            "rr:sqlQuery names column '{column}', which table '{table}' does not have{hint}"
        ),
        missing_column: Some(column.to_string()),
    })
}

fn typed_predicate(
    schema: &Schema,
    table: &str,
    condition: &RowCondition,
) -> Result<ExactPredicate, QueryRefusal> {
    let field = field(schema, table, condition.column())?;
    if field.is_nested() {
        return Err(QueryRefusal::other(format!(
            "rr:sqlQuery condition `{condition}`: '{}' is a nested column, which a row filter \
             cannot test",
            field.name
        )));
    }
    let (field_id, column) = (field.id, field.name.clone());
    Ok(match condition {
        RowCondition::IsNull { .. } => ExactPredicate::IsNull { field_id, column },
        RowCondition::IsNotNull { .. } => ExactPredicate::IsNotNull { field_id, column },
        RowCondition::Compare { op, value, .. } => ExactPredicate::Compare {
            field_id,
            op: comparison_op(*op),
            value: typed_literal(field, value).map_err(|why| {
                QueryRefusal::other(format!("rr:sqlQuery condition `{condition}`: {why}"))
            })?,
            column,
        },
    })
}

fn comparison_op(op: RowCmpOp) -> ComparisonOp {
    match op {
        RowCmpOp::Eq => ComparisonOp::Eq,
        RowCmpOp::NotEq => ComparisonOp::NotEq,
        RowCmpOp::Lt => ComparisonOp::Lt,
        RowCmpOp::LtEq => ComparisonOp::LtEq,
        RowCmpOp::Gt => ComparisonOp::Gt,
        RowCmpOp::GtEq => ComparisonOp::GtEq,
    }
}

/// `value` as a literal of `field`'s type that compares with it exactly, or
/// why there is none. Only same-kind comparisons are accepted: an integer or
/// decimal against an integer or decimal column, a string against a string
/// column, a boolean against a boolean column.
///
/// A float or double column compares as IEEE numbers, the way a SPARQL
/// `FILTER` over the mapped triples does: a NaN cell satisfies only `<>`.
/// SQL engines that treat NaN as equal to itself and above every number
/// (PostgreSQL, Spark) answer differently there; the filter agrees with the
/// query language over the triples instead.
fn typed_literal(field: &SchemaField, value: &SqlLiteral) -> Result<LiteralValue, String> {
    let ty = field.type_string().unwrap_or("a nested type");
    let column = &field.name;
    let is_decimal = ty.starts_with("decimal(");
    match (ty, value) {
        ("int", SqlLiteral::Integer(n)) => i32::try_from(*n)
            .map(LiteralValue::Int32)
            .map_err(|_| format!("{value} is out of range for column '{column}' (int)")),
        ("long", SqlLiteral::Integer(n)) => i64::try_from(*n)
            .map(LiteralValue::Int64)
            .map_err(|_| format!("{value} is out of range for column '{column}' (long)")),
        ("int" | "long", SqlLiteral::Decimal { unscaled, scale }) => {
            match integral(*unscaled, *scale) {
                Some(n) => typed_literal(field, &SqlLiteral::Integer(n)),
                None => Err(format!(
                    "column '{column}' ({ty}) holds integers; compare it with an integer, not \
                     {value}"
                )),
            }
        }
        (_, SqlLiteral::Integer(n)) if is_decimal => Ok(LiteralValue::Decimal {
            unscaled: *n,
            scale: 0,
        }),
        (_, SqlLiteral::Decimal { unscaled, scale }) if is_decimal => Ok(LiteralValue::Decimal {
            unscaled: *unscaled,
            // The parser caps the scale at 38.
            scale: i8::try_from(*scale).unwrap_or(i8::MAX),
        }),
        ("string", SqlLiteral::String(s)) => Ok(LiteralValue::String(s.clone())),
        ("boolean", SqlLiteral::Boolean(b)) => Ok(LiteralValue::Boolean(*b)),
        // An integer within ±2^53 has an exact `f64`, and the reader compares a
        // float column in `f64` as IEEE numbers. A decimal literal is refused:
        // `0.1` has no exact binary value, so `= 0.1` would test a neighbor.
        ("float" | "double", SqlLiteral::Integer(n)) if n.unsigned_abs() <= FLOAT_EXACT_INTEGER => {
            Ok(LiteralValue::Float64(*n as f64))
        }
        ("float" | "double", SqlLiteral::Integer(_)) => Err(format!(
            "{value} is beyond ±2^53, the integers column '{column}' ({ty}) compares exactly"
        )),
        ("float" | "double", _) => Err(format!(
            "column '{column}' is {ty}; a row filter compares it only with an integer within \
             ±2^53 (a decimal literal such as 0.1 has no exact binary value), or tests it \
             with IS [NOT] NULL"
        )),
        ("boolean", _) => Err(format!(
            "column '{column}' is boolean; compare it with TRUE or FALSE, not {value}"
        )),
        ("string", _) => Err(format!(
            "column '{column}' is a string; compare it with a quoted string, not {value}"
        )),
        ("int" | "long", _) => Err(format!(
            "column '{column}' ({ty}) holds integers; compare it with an integer, not {value}"
        )),
        _ if is_decimal => Err(format!(
            "column '{column}' is {ty}; compare it with a number, not {value}"
        )),
        _ => Err(format!(
            "column '{column}' is {ty}; a row filter compares integer, decimal, \
             floating-point, string and boolean columns, and tests any column with IS [NOT] \
             NULL (comparing {ty} columns may be accepted by a later version)"
        )),
    }
}

/// Every integer of at most this magnitude is exactly an `f64`: 2^53.
const FLOAT_EXACT_INTEGER: u128 = 1 << 53;

/// `unscaled × 10^-scale` as an integer, when it is one.
fn integral(unscaled: i128, scale: u8) -> Option<i128> {
    let divisor = 10i128.checked_pow(u32::from(scale))?;
    (unscaled % divisor == 0).then(|| unscaled / divisor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_r2rml::loader::R2rmlLoader;

    fn schema() -> Schema {
        let fields = serde_json::json!([
            {"id": 1, "name": "customer_id", "required": true, "type": "long"},
            {"id": 2, "name": "is_current", "required": false, "type": "int"},
            {"id": 3, "name": "name", "required": false, "type": "string"},
            {"id": 4, "name": "active", "required": false, "type": "boolean"},
            {"id": 5, "name": "balance", "required": false, "type": "decimal(10, 2)"},
            {"id": 6, "name": "score", "required": false, "type": "double"},
            {"id": 7, "name": "valid_from", "required": false, "type": "date"},
            {"id": 8, "name": "Region", "required": false, "type": "string"},
            {"id": 9, "name": "tags", "required": false,
             "type": {"type": "list", "element-id": 10, "element": "string", "element-required": false}},
        ]);
        serde_json::from_value(serde_json::json!({
            "type": "struct", "schema-id": 0, "fields": fields
        }))
        .expect("schema")
    }

    fn scan(sql: &str) -> LogicalScan {
        let select = RestrictedSelect::parse(sql).expect("parse");
        LogicalScan {
            table: select.table.clone(),
            select: Some(Arc::new(select)),
        }
    }

    fn filter(sql: &str) -> Result<Vec<ExactPredicate>, String> {
        scan(sql)
            .row_filter(&schema())
            .map(|f| f.map(|f| f.predicates().to_vec()).unwrap_or_default())
            .map_err(|e| e.message)
    }

    fn compare(
        field_id: i32,
        column: &str,
        op: ComparisonOp,
        value: LiteralValue,
    ) -> ExactPredicate {
        ExactPredicate::Compare {
            field_id,
            column: column.to_string(),
            op,
            value,
        }
    }

    #[test]
    fn literals_are_typed_by_their_column() {
        let predicates = filter(
            "SELECT * FROM t WHERE is_current = 1 AND customer_id >= 10 AND name <> 'x' \
             AND active = TRUE AND balance < 9.5 AND balance > -3 AND is_current <= 2.00 \
             AND valid_from IS NULL AND score IS NOT NULL AND score >= -9007199254740992",
        )
        .expect("typed");
        assert_eq!(
            predicates,
            vec![
                compare(2, "is_current", ComparisonOp::Eq, LiteralValue::Int32(1)),
                compare(
                    1,
                    "customer_id",
                    ComparisonOp::GtEq,
                    LiteralValue::Int64(10)
                ),
                compare(
                    3,
                    "name",
                    ComparisonOp::NotEq,
                    LiteralValue::String("x".into())
                ),
                compare(4, "active", ComparisonOp::Eq, LiteralValue::Boolean(true)),
                compare(
                    5,
                    "balance",
                    ComparisonOp::Lt,
                    LiteralValue::Decimal {
                        unscaled: 95,
                        scale: 1
                    }
                ),
                compare(
                    5,
                    "balance",
                    ComparisonOp::Gt,
                    LiteralValue::Decimal {
                        unscaled: -3,
                        scale: 0
                    }
                ),
                compare(2, "is_current", ComparisonOp::LtEq, LiteralValue::Int32(2)),
                ExactPredicate::IsNull {
                    field_id: 7,
                    column: "valid_from".into()
                },
                ExactPredicate::IsNotNull {
                    field_id: 6,
                    column: "score".into()
                },
                compare(
                    6,
                    "score",
                    ComparisonOp::GtEq,
                    LiteralValue::Float64(-9_007_199_254_740_992.0)
                ),
            ]
        );
    }

    #[test]
    fn no_where_is_no_filter() {
        let scan = scan("SELECT customer_id, name FROM t");
        assert!(!scan.is_filtered());
        assert_eq!(scan.row_filter(&schema()), Ok(None));
    }

    #[test]
    fn refusals_name_the_column_type_and_literal() {
        let cases: &[(&str, &str)] = &[
            ("SELECT * FROM t WHERE missing = 1", "does not have"),
            ("SELECT missing FROM t", "names column 'missing'"),
            (
                "SELECT * FROM t WHERE region = 'x'",
                "did you mean \"Region\"",
            ),
            (
                "SELECT * FROM t WHERE is_current = 3000000000",
                "out of range for column 'is_current' (int)",
            ),
            (
                "SELECT * FROM t WHERE is_current = 1.5",
                "compare it with an integer, not 1.5",
            ),
            (
                "SELECT * FROM t WHERE is_current = '1'",
                "compare it with an integer, not '1'",
            ),
            (
                "SELECT * FROM t WHERE active = 1",
                "compare it with TRUE or FALSE, not 1",
            ),
            (
                "SELECT * FROM t WHERE name = 1",
                "compare it with a quoted string, not 1",
            ),
            (
                "SELECT * FROM t WHERE balance = 'x'",
                "compare it with a number",
            ),
            (
                "SELECT * FROM t WHERE score > 0.5",
                "a decimal literal such as 0.1",
            ),
            (
                "SELECT * FROM t WHERE score = 9007199254740993",
                "beyond ±2^53",
            ),
            (
                "SELECT * FROM t WHERE valid_from > '2024-01-01'",
                "column 'valid_from' is date",
            ),
            (
                "SELECT * FROM t WHERE valid_from = 1",
                "may be accepted by a later version",
            ),
            ("SELECT * FROM t WHERE tags IS NULL", "nested column"),
        ];
        for (sql, needle) in cases {
            match filter(sql) {
                Ok(p) => panic!("{sql} typed as {p:?}"),
                Err(message) => assert!(
                    message.contains(needle),
                    "{sql}: expected {needle:?} in {message:?}"
                ),
            }
        }
    }

    fn mapping(sql: &str) -> CompiledR2rmlMapping {
        keyed_mapping(sql, "")
    }

    /// `key` follows the query on the logical table (`; f:key ( ... )`).
    fn keyed_mapping(sql: &str, key: &str) -> CompiledR2rmlMapping {
        let ttl = format!(
            r#"@base <http://example.org/> .
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .
            @prefix f: <https://ns.flur.ee/db#> .
            <#Customer> a rr:TriplesMap ;
                rr:logicalTable [ rr:sqlQuery """{sql}""" {key} ] ;
                rr:subjectMap [ rr:template "http://example.org/customer/{{customer_id}}" ] ;
                rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ."#
        );
        R2rmlLoader::from_turtle(&ttl)
            .expect("turtle")
            .compile()
            .expect("compile")
    }

    #[test]
    fn an_alias_resolves_to_its_table_and_query() {
        let compiled = mapping("SELECT * FROM crm.customers_history WHERE is_current = 1");
        let alias = compiled
            .triples_maps
            .values()
            .next()
            .unwrap()
            .table_name()
            .unwrap();
        let resolved = LogicalScan::resolve(&compiled, alias).expect("resolve");
        assert_eq!(resolved.table, "crm.customers_history");
        assert!(resolved.is_filtered());
        let plain = LogicalScan::resolve(&compiled, "crm.orders").expect("plain");
        assert_eq!(plain.table, "crm.orders");
        assert!(plain.select.is_none());
        assert!(LogicalScan::resolve(&compiled, "sqlQuery:0000000000000000").is_err());
    }

    /// A select list is the logical table's columns: a map reading a column it
    /// leaves out is refused, at registration and when the alias resolves.
    /// `SELECT *`, and a list naming every column read, register.
    #[test]
    fn a_select_list_must_cover_the_columns_the_map_reads() {
        assert!(check_restricted_queries(&mapping("SELECT * FROM t WHERE a = 1")).is_ok());
        assert!(
            check_restricted_queries(&mapping("SELECT customer_id, name FROM t WHERE a = 1"))
                .is_ok()
        );
        let compiled = mapping("SELECT customer_id FROM t WHERE a = 1");
        let message = check_restricted_queries(&compiled).expect_err("name is not selected");
        assert!(
            message.contains("selects customer_id, but the map reads name"),
            "{message}"
        );
        let alias = compiled
            .triples_maps
            .values()
            .next()
            .unwrap()
            .table_name()
            .unwrap();
        let at_resolve = LogicalScan::resolve(&compiled, alias).expect_err("refused at resolve");
        assert_eq!(at_resolve, message);
    }

    /// A map referenced by another reads its join's parent columns too.
    #[test]
    fn a_select_list_must_cover_a_referencing_join_parent_column() {
        let ttl = r#"@base <http://example.org/> .
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .
            <#Customer> a rr:TriplesMap ;
                rr:logicalTable [ rr:sqlQuery "SELECT id FROM crm.customers WHERE is_current = 1" ] ;
                rr:subjectMap [ rr:template "http://example.org/customer/{id}" ] .
            <#Order> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "crm.orders" ] ;
                rr:subjectMap [ rr:template "http://example.org/order/{order_id}" ] ;
                rr:predicateObjectMap [ rr:predicate ex:customer ; rr:objectMap [
                    rr:parentTriplesMap <#Customer> ;
                    rr:joinCondition [ rr:child "customer_ref" ; rr:parent "customer_key" ] ] ] ."#;
        let compiled = R2rmlLoader::from_turtle(ttl).unwrap().compile().unwrap();
        let message = check_restricted_queries(&compiled).expect_err("customer_key not selected");
        assert!(
            message.contains("selects id, but the map reads customer_key"),
            "{message}"
        );
    }

    #[test]
    fn registration_refuses_a_key_the_query_does_not_select() {
        let key = r#"; f:key ( "customer_id" )"#;
        for sql in [
            "SELECT * FROM t WHERE a = 1",
            "SELECT customer_id, name FROM t WHERE a = 1",
            "SELECT name, customer_id FROM t",
        ] {
            assert!(
                check_restricted_queries(&keyed_mapping(sql, key)).is_ok(),
                "{sql}"
            );
        }
        let message =
            check_restricted_queries(&keyed_mapping("SELECT name FROM t WHERE a = 1", key))
                .expect_err("customer_id is not selected");
        assert!(
            message.contains("\"customer_id\", which the query does not select")
                && message.contains("Customer>"),
            "{message}"
        );
    }

    #[test]
    fn registration_refuses_a_query_outside_the_subset() {
        assert!(check_restricted_queries(&mapping("SELECT * FROM t WHERE a = 1")).is_ok());
        let message = check_restricted_queries(&mapping("SELECT * FROM t WHERE a = 1 OR b = 2"))
            .expect_err("OR is outside the subset");
        assert!(message.contains("OR is not supported"), "{message}");
        assert!(
            message.contains("<#Customer>") || message.contains("Customer>"),
            "{message}"
        );
    }
}
