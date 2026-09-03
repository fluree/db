//! SQL rendering of a single-table scan.
//!
//! The query engine never sends SPARQL here. The R2RML operator asks a
//! provider for one table at a time — a projection, conjunctive filters, and
//! optionally a single-column `ORDER BY … LIMIT` — and does joins, OPTIONAL,
//! UNION and aggregation itself over the returned column batches. So what gets
//! rendered is exactly one `SELECT … FROM … WHERE …`.
//!
//! Filters are pushed **typed**: every predicate is rendered against the
//! column's known type (from a cached `LIMIT 0` probe), and a predicate whose
//! literal cannot be rendered safely for that type is dropped rather than
//! guessed — a mistyped comparison would fail the whole statement in Trino
//! ("Cannot apply operator: bigint = varchar"), and the in-engine FILTER stays
//! the authority either way, so a dropped push only costs I/O.
//!
//! The same valve carries the escaping rule: string literals are rendered with
//! standard-SQL quote doubling, and a value that a dialect might read as
//! carrying escapes is declined rather than escaped for a server mode we cannot
//! observe. See [`sql_string`].

use fluree_db_tabular::{BatchSchema, FieldType};
use serde::{Deserialize, Serialize};

use crate::error::{Result, SqlError};

/// Identifier quoting and literal syntax family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqlDialect {
    #[default]
    Trino,
    Postgres,
    Mysql,
    Sqlite,
}

impl SqlDialect {
    fn quote_char(self) -> char {
        match self {
            SqlDialect::Mysql => '`',
            _ => '"',
        }
    }

    /// Quote one identifier part, doubling any embedded quote character.
    pub fn quote_ident(self, ident: &str) -> String {
        let q = self.quote_char();
        let mut out = String::with_capacity(ident.len() + 2);
        out.push(q);
        for c in ident.chars() {
            if c == q {
                out.push(q);
            }
            out.push(c);
        }
        out.push(q);
        out
    }

    /// Quote a dotted table name part by part (`ns.table` → `"ns"."table"`).
    pub fn quote_table(self, table: &str) -> String {
        table
            .split('.')
            .map(|p| self.quote_ident(p))
            .collect::<Vec<_>>()
            .join(".")
    }

    /// Whether typed literal prefixes (`DATE '…'`, `TIMESTAMP '…'`) are valid.
    fn typed_literals(self) -> bool {
        !matches!(self, SqlDialect::Sqlite)
    }

    /// Whether the server may treat `\\` as live inside a string literal.
    ///
    /// Trino, SQLite and Postgres (with `standard_conforming_strings`, on by
    /// default since 9.1) leave backslash inert, so doubling `'` is the whole
    /// escaping rule. MySQL under its default `sql_mode` does not: there a
    /// trailing `\\` escapes the closing quote and the rest of the value parses
    /// as SQL.
    fn backslash_may_escape(self) -> bool {
        matches!(self, SqlDialect::Mysql)
    }
}

/// Where the rows come from: a table, or an `rr:sqlQuery` used as a derived table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalSource {
    /// Dotted table name; each part is quoted.
    Table(String),
    /// Verbatim SQL from the mapping, wrapped as `(…) AS "__fluree_q"`. The
    /// mapping author is trusted (a mapping is root-equivalent by design).
    Query(String),
}

impl LogicalSource {
    pub fn render(&self, dialect: SqlDialect) -> String {
        match self {
            LogicalSource::Table(t) => dialect.quote_table(t),
            LogicalSource::Query(q) => {
                format!(
                    "({}) AS {}",
                    q.trim().trim_end_matches(';'),
                    dialect.quote_ident("__fluree_q")
                )
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    In,
}

impl CmpOp {
    fn sql(self) -> &'static str {
        match self {
            CmpOp::Eq => "=",
            CmpOp::NotEq => "<>",
            CmpOp::Lt => "<",
            CmpOp::LtEq => "<=",
            CmpOp::Gt => ">",
            CmpOp::GtEq => ">=",
            CmpOp::In => "IN",
        }
    }
}

/// A filter literal. Mirrors the engine's `ScanValue` without depending on the
/// query crate.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Bool(bool),
    Int(i64),
    Str(String),
    /// Days since 1970-01-01.
    Date(i32),
    Double(f64),
    Decimal {
        unscaled: i128,
        scale: i8,
    },
    /// Micros since the epoch; `tz` = the source literal carried an offset.
    Timestamp {
        micros: i64,
        tz: bool,
    },
    /// A raw column value recovered by reversing a subject template. Its type
    /// is whatever the column's type is; rendered only for int/string columns.
    TemplateKey(String),
    Set(Vec<Literal>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    pub column: String,
    pub op: CmpOp,
    pub value: Literal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScanRequest {
    pub source: LogicalSource,
    /// Empty = every column.
    pub projection: Vec<String>,
    pub predicates: Vec<Predicate>,
}

/// One rendered scan plus what was dropped, so callers can log declined pushes.
#[derive(Debug, Clone)]
pub struct RenderedScan {
    pub sql: String,
    pub declined_predicates: Vec<Predicate>,
}

/// `SELECT * FROM <source> LIMIT 0` — the schema probe.
pub fn render_probe(source: &LogicalSource, dialect: SqlDialect) -> String {
    format!("SELECT * FROM {} LIMIT 0", source.render(dialect))
}

/// `SELECT COUNT(*) FROM <source> WHERE c1 IS NOT NULL AND …`
pub fn render_count(
    source: &LogicalSource,
    non_null_cols: &[String],
    dialect: SqlDialect,
) -> String {
    let mut sql = format!("SELECT COUNT(*) FROM {}", source.render(dialect));
    if !non_null_cols.is_empty() {
        let conds: Vec<String> = non_null_cols
            .iter()
            .map(|c| format!("{} IS NOT NULL", dialect.quote_ident(c)))
            .collect();
        sql.push_str(" WHERE ");
        sql.push_str(&conds.join(" AND "));
    }
    sql
}

/// Render the scan against the probed schema. Unknown projected columns are
/// an error (the mapping names a column the table does not have); predicates
/// on unknown columns or with unrenderable literals are declined, not errors.
pub fn render_scan(
    req: &ScanRequest,
    schema: &BatchSchema,
    dialect: SqlDialect,
) -> Result<RenderedScan> {
    let select_list = if req.projection.is_empty() {
        schema
            .fields
            .iter()
            .map(|f| render_projected_column(&f.name, f.field_type, dialect))
            .collect::<Vec<_>>()
    } else {
        let mut cols = Vec::with_capacity(req.projection.len());
        for name in &req.projection {
            let field = schema.field_by_name(name).ok_or_else(|| {
                SqlError::Config(format!(
                    "projected column '{name}' does not exist in {}; available: {:?}",
                    describe(&req.source),
                    schema
                        .fields
                        .iter()
                        .map(|f| f.name.as_str())
                        .collect::<Vec<_>>()
                ))
            })?;
            cols.push(render_projected_column(name, field.field_type, dialect));
        }
        cols
    };

    let mut sql = format!(
        "SELECT {} FROM {}",
        select_list.join(", "),
        req.source.render(dialect)
    );

    let mut conds = Vec::new();
    let mut declined = Vec::new();
    for pred in &req.predicates {
        match schema.field_by_name(&pred.column) {
            Some(field) => match render_predicate(pred, field.field_type, dialect) {
                Some(c) => conds.push(c),
                None => declined.push(pred.clone()),
            },
            None => declined.push(pred.clone()),
        }
    }
    if !conds.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conds.join(" AND "));
    }

    Ok(RenderedScan {
        sql,
        declined_predicates: declined,
    })
}

fn describe(source: &LogicalSource) -> String {
    match source {
        LogicalSource::Table(t) => format!("table '{t}'"),
        LogicalSource::Query(_) => "the rr:sqlQuery".to_string(),
    }
}

/// A `timestamp with time zone` column is re-rendered in UTC so the wire form
/// is decodable without a zone database (Trino otherwise prints the value's
/// own zone, which may be a named region).
fn render_projected_column(name: &str, ty: FieldType, dialect: SqlDialect) -> String {
    let q = dialect.quote_ident(name);
    match (ty, dialect) {
        (FieldType::TimestampTz, SqlDialect::Trino) => format!("{q} AT TIME ZONE 'UTC' AS {q}"),
        _ => q,
    }
}

fn render_predicate(pred: &Predicate, ty: FieldType, dialect: SqlDialect) -> Option<String> {
    let col = dialect.quote_ident(&pred.column);
    match (&pred.value, pred.op) {
        (Literal::Set(members), CmpOp::In) => {
            if members.is_empty() {
                return None;
            }
            let rendered: Option<Vec<String>> = members
                .iter()
                .map(|m| render_literal(m, ty, dialect))
                .collect();
            rendered.map(|r| format!("{col} IN ({})", r.join(", ")))
        }
        (Literal::Set(_), _) | (_, CmpOp::In) => None,
        (lit, op) => render_literal(lit, ty, dialect).map(|l| format!("{col} {} {l}", op.sql())),
    }
}

/// Render `s` as a string literal, or decline when no rendering is safe.
///
/// Doubling `'` is the standard-SQL rule and is sufficient wherever backslash
/// is inert. On MySQL it is not (see [`SqlDialect::backslash_may_escape`]), and
/// escaping instead of declining would be wrong in both directions: the engine
/// cannot observe the endpoint's `sql_mode`, and `dialect` names the database
/// *behind* an endpoint that need not be a bridge we configured. So a value
/// carrying a backslash is declined, which costs a pushdown and nothing else —
/// the in-engine FILTER enforces the predicate either way.
fn sql_string(s: &str, dialect: SqlDialect) -> Option<String> {
    if dialect.backslash_may_escape() && s.contains('\\') {
        return None;
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    Some(out)
}

fn is_numeric(ty: FieldType) -> bool {
    matches!(
        ty,
        FieldType::Int32
            | FieldType::Int64
            | FieldType::Float32
            | FieldType::Float64
            | FieldType::Decimal { .. }
    )
}

/// Render a literal for comparison against a column of type `ty`, or `None`
/// when no rendering is safe for that pairing.
fn render_literal(lit: &Literal, ty: FieldType, dialect: SqlDialect) -> Option<String> {
    match lit {
        Literal::Bool(b) => {
            matches!(ty, FieldType::Boolean).then(|| if *b { "TRUE" } else { "FALSE" }.to_string())
        }
        Literal::Int(i) => is_numeric(ty).then(|| i.to_string()),
        Literal::Str(s) => matches!(ty, FieldType::String).then(|| sql_string(s, dialect))?,
        Literal::Date(days) => {
            if !matches!(ty, FieldType::Date) {
                return None;
            }
            let date = chrono::DateTime::from_timestamp(i64::from(*days) * 86_400, 0)?.date_naive();
            let text = date.format("%Y-%m-%d").to_string();
            if dialect.typed_literals() {
                Some(format!("DATE '{text}'"))
            } else {
                sql_string(&text, dialect)
            }
        }
        Literal::Double(d) => {
            if !d.is_finite() || !is_numeric(ty) {
                return None;
            }
            // `{:E}` gives `1.5E0`, a valid double literal in every dialect here
            // and unambiguous (a bare `1.5` is a DECIMAL literal in Trino).
            Some(format!("{d:E}"))
        }
        Literal::Decimal { unscaled, scale } => {
            is_numeric(ty).then(|| render_decimal(*unscaled, *scale))
        }
        Literal::Timestamp { micros, tz } => {
            let matches_col = match ty {
                FieldType::Timestamp => !*tz,
                FieldType::TimestampTz => *tz,
                _ => false,
            };
            if !matches_col {
                return None;
            }
            let dt = chrono::DateTime::from_timestamp_micros(*micros)?;
            let text = dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            match (dialect.typed_literals(), *tz) {
                (true, true) => Some(format!("TIMESTAMP '{text} UTC'")),
                (true, false) => Some(format!("TIMESTAMP '{text}'")),
                (false, _) => sql_string(&text, dialect),
            }
        }
        Literal::TemplateKey(raw) => match ty {
            FieldType::String => sql_string(raw, dialect),
            FieldType::Int32 | FieldType::Int64 => raw.parse::<i64>().ok().map(|i| i.to_string()),
            _ => None,
        },
        Literal::Set(_) => None,
    }
}

fn render_decimal(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        let mut s = unscaled.to_string();
        s.extend(std::iter::repeat_n('0', (-scale) as usize));
        return s;
    }
    let scale = scale as usize;
    let negative = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let (int_part, frac_part) = padded.split_at(padded.len() - scale);
    format!("{}{int_part}.{frac_part}", if negative { "-" } else { "" })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_tabular::FieldInfo;

    fn schema() -> BatchSchema {
        let f = |name: &str, ty: FieldType, id: i32| FieldInfo {
            name: name.to_string(),
            field_type: ty,
            nullable: true,
            field_id: id,
        };
        BatchSchema::new(vec![
            f("id", FieldType::Int64, 1),
            f("name", FieldType::String, 2),
            f("born", FieldType::Date, 3),
            f("score", FieldType::Float64, 4),
            f(
                "price",
                FieldType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                5,
            ),
            f("at", FieldType::TimestampTz, 6),
            f("local_at", FieldType::Timestamp, 7),
            f("ok", FieldType::Boolean, 8),
        ])
    }

    fn pred(column: &str, op: CmpOp, value: Literal) -> Predicate {
        Predicate {
            column: column.into(),
            op,
            value,
        }
    }

    #[test]
    fn quoting_doubles_embedded_quotes_and_splits_dotted_names() {
        assert_eq!(
            SqlDialect::Trino.quote_table("hive.sales.orders"),
            r#""hive"."sales"."orders""#
        );
        assert_eq!(SqlDialect::Trino.quote_ident(r#"we"ird"#), r#""we""ird""#);
        assert_eq!(SqlDialect::Mysql.quote_table("db.t"), "`db`.`t`");
        assert_eq!(
            sql_string("O'Brien", SqlDialect::Trino).unwrap(),
            "'O''Brien'"
        );
    }

    #[test]
    fn probe_and_count_render() {
        let src = LogicalSource::Table("s.t".into());
        assert_eq!(
            render_probe(&src, SqlDialect::Trino),
            r#"SELECT * FROM "s"."t" LIMIT 0"#
        );
        assert_eq!(
            render_count(&src, &["id".into(), "name".into()], SqlDialect::Trino),
            r#"SELECT COUNT(*) FROM "s"."t" WHERE "id" IS NOT NULL AND "name" IS NOT NULL"#
        );
        let q = LogicalSource::Query("select 1 as id;".into());
        assert_eq!(
            render_count(&q, &[], SqlDialect::Trino),
            r#"SELECT COUNT(*) FROM (select 1 as id) AS "__fluree_q""#
        );
    }

    #[test]
    fn typed_predicates_render_and_mismatches_decline() {
        let req = ScanRequest {
            source: LogicalSource::Table("t".into()),
            projection: vec!["id".into(), "name".into(), "at".into()],
            predicates: vec![
                pred("id", CmpOp::Eq, Literal::Int(7)),
                pred("name", CmpOp::Eq, Literal::Str("O'Brien".into())),
                pred("born", CmpOp::GtEq, Literal::Date(19_723)),
                pred("score", CmpOp::Gt, Literal::Double(1.5)),
                pred(
                    "price",
                    CmpOp::Lt,
                    Literal::Decimal {
                        unscaled: -1234,
                        scale: 2,
                    },
                ),
                pred(
                    "at",
                    CmpOp::Lt,
                    Literal::Timestamp {
                        micros: 1_700_000_000_000_000,
                        tz: true,
                    },
                ),
                pred(
                    "local_at",
                    CmpOp::Lt,
                    Literal::Timestamp {
                        micros: 0,
                        tz: false,
                    },
                ),
                pred("ok", CmpOp::Eq, Literal::Bool(true)),
                pred(
                    "id",
                    CmpOp::In,
                    Literal::Set(vec![Literal::Int(1), Literal::Int(2)]),
                ),
                // Declined: string against an int column, tz mismatch, unknown column,
                // NaN, template key that is not an integer.
                pred("id", CmpOp::Eq, Literal::Str("x".into())),
                pred(
                    "at",
                    CmpOp::Eq,
                    Literal::Timestamp {
                        micros: 0,
                        tz: false,
                    },
                ),
                pred("nope", CmpOp::Eq, Literal::Int(1)),
                pred("score", CmpOp::Eq, Literal::Double(f64::NAN)),
                pred("id", CmpOp::Eq, Literal::TemplateKey("abc".into())),
            ],
        };
        let r = render_scan(&req, &schema(), SqlDialect::Trino).unwrap();
        assert_eq!(
            r.sql,
            concat!(
                r#"SELECT "id", "name", "at" AT TIME ZONE 'UTC' AS "at" FROM "t" WHERE "#,
                r#""id" = 7 AND "name" = 'O''Brien' AND "born" >= DATE '2024-01-01' AND "score" > 1.5E0 "#,
                r#"AND "price" < -12.34 AND "at" < TIMESTAMP '2023-11-14 22:13:20.000000 UTC' "#,
                r#"AND "local_at" < TIMESTAMP '1970-01-01 00:00:00.000000' AND "ok" = TRUE AND "id" IN (1, 2)"#
            )
        );
        assert_eq!(r.declined_predicates.len(), 5);
    }

    #[test]
    fn template_key_is_typed_by_the_column() {
        let req = ScanRequest {
            source: LogicalSource::Table("t".into()),
            projection: vec![],
            predicates: vec![
                pred("id", CmpOp::Eq, Literal::TemplateKey("42".into())),
                pred("name", CmpOp::Eq, Literal::TemplateKey("42".into())),
                pred("born", CmpOp::Eq, Literal::TemplateKey("2024-01-01".into())),
            ],
        };
        let r = render_scan(&req, &schema(), SqlDialect::Trino).unwrap();
        assert!(
            r.sql.contains(r#""id" = 42 AND "name" = '42'"#),
            "{}",
            r.sql
        );
        assert_eq!(r.declined_predicates.len(), 1);
        assert!(r
            .sql
            .starts_with(r#"SELECT "id", "name", "born", "score", "price", "at" AT TIME ZONE"#));
    }

    /// A backslash is inert on Trino/Postgres/SQLite and live on MySQL under
    /// its default `sql_mode`, where quote-doubling alone would let the value
    /// close its own literal. Rendering must decline there, and only there.
    #[test]
    fn mysql_declines_string_literals_carrying_a_backslash() {
        // `a\' UNION SELECT …` — the shape that escapes its closing quote when
        // the server reads `\'` as an escaped quote rather than as two chars.
        let hostile = r"a\' UNION SELECT price FROM other -- ";
        let req = |lit: Literal| ScanRequest {
            source: LogicalSource::Table("t".into()),
            projection: vec!["name".into()],
            predicates: vec![pred("name", CmpOp::Eq, lit)],
        };

        for lit in [
            Literal::Str(hostile.into()),
            Literal::TemplateKey(hostile.into()),
            // A lone trailing backslash is the minimal case, and ordinary data.
            Literal::Str(r"c:\".into()),
        ] {
            let r = render_scan(&req(lit.clone()), &schema(), SqlDialect::Mysql).unwrap();
            assert!(
                !r.sql.contains("WHERE"),
                "MySQL must decline `{lit:?}`, rendered: {}",
                r.sql
            );
            assert_eq!(r.declined_predicates.len(), 1, "{lit:?}");

            // Every other dialect leaves backslash inert, so the push stands.
            for dialect in [SqlDialect::Trino, SqlDialect::Postgres, SqlDialect::Sqlite] {
                let r = render_scan(&req(lit.clone()), &schema(), dialect).unwrap();
                assert!(
                    r.declined_predicates.is_empty(),
                    "{dialect:?} should push `{lit:?}`"
                );
            }
        }

        // The escaping that is applied stays standard: `'` doubles, `\` is
        // passed through as the single character it is.
        let r = render_scan(
            &req(Literal::Str(hostile.into())),
            &schema(),
            SqlDialect::Trino,
        )
        .unwrap();
        assert_eq!(
            r.sql,
            r#"SELECT "name" FROM "t" WHERE "name" = 'a\'' UNION SELECT price FROM other -- '"#
        );
    }

    /// Values with no backslash are unaffected on MySQL: quote doubling and
    /// backtick identifier quoting still apply.
    #[test]
    fn mysql_still_pushes_ordinary_string_literals() {
        let req = ScanRequest {
            source: LogicalSource::Table("t".into()),
            projection: vec!["name".into()],
            predicates: vec![pred("name", CmpOp::Eq, Literal::Str("O'Brien".into()))],
        };
        let r = render_scan(&req, &schema(), SqlDialect::Mysql).unwrap();
        assert_eq!(r.sql, "SELECT `name` FROM `t` WHERE `name` = 'O''Brien'");
        assert!(r.declined_predicates.is_empty());
    }

    #[test]
    fn unknown_projection_is_an_error() {
        let req = ScanRequest {
            source: LogicalSource::Table("t".into()),
            projection: vec!["missing".into()],
            predicates: vec![],
        };
        let err = render_scan(&req, &schema(), SqlDialect::Trino).unwrap_err();
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn sqlite_uses_plain_string_literals() {
        let req = ScanRequest {
            source: LogicalSource::Table("t".into()),
            projection: vec!["born".into()],
            predicates: vec![pred("born", CmpOp::Eq, Literal::Date(0))],
        };
        let r = render_scan(&req, &schema(), SqlDialect::Sqlite).unwrap();
        assert_eq!(
            r.sql,
            r#"SELECT "born" FROM "t" WHERE "born" = '1970-01-01'"#
        );
    }

    #[test]
    fn decimal_rendering() {
        assert_eq!(render_decimal(1234, 2), "12.34");
        assert_eq!(render_decimal(-5, 2), "-0.05");
        assert_eq!(render_decimal(5, 0), "5");
        assert_eq!(render_decimal(5, -2), "500");
        assert_eq!(render_decimal(0, 3), "0.000");
    }
}
