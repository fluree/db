//! Tables from SQL DDL: `CREATE TABLE` with its keys, checks and comments.
//!
//! Read: column types; primary keys (column or table constraint); foreign
//! keys (`REFERENCES`, or `FOREIGN KEY ... REFERENCES`); enumerated columns
//! (`CHECK (status IN ('open', 'closed'))`, a MySQL `ENUM(...)` type, or a
//! PostgreSQL `CREATE TYPE ... AS ENUM` used as a column's type); and
//! descriptions (`COMMENT ON TABLE|COLUMN`, an inline `COMMENT '...'`, a
//! table's `COMMENT = '...'`). Views, indexes, grants and everything else are
//! passed over.
//!
//! The text is parsed with a generic SQL dialect, then PostgreSQL, MySQL and
//! Snowflake's. When no dialect parses it whole, each statement is tried on
//! its own and the ones none can parse are skipped and counted.

use super::{Column, Model, Table};
use sqlparser::ast::{
    AlterTableOperation, ColumnOption, CommentDef, CommentObject, CreateTable, CreateTableOptions,
    DataType, Expr, ObjectName, SqlOption, Statement, TableConstraint,
    UserDefinedTypeRepresentation,
};
use sqlparser::dialect::{
    Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
#[error("no CREATE TABLE statement could be read{}", .0.as_ref().map(|e| format!(" ({e})")).unwrap_or_default())]
pub struct SqlError(pub Option<String>);

/// The model, and how many statements no dialect could parse.
pub fn read(sql: &str) -> Result<(Model, usize), SqlError> {
    let dialects: [&dyn Dialect; 4] = [
        &GenericDialect {},
        &PostgreSqlDialect {},
        &MySqlDialect {},
        &SnowflakeDialect {},
    ];
    let mut first_error = None;
    let mut statements = None;
    for d in dialects {
        match Parser::parse_sql(d, sql) {
            Ok(s) => {
                statements = Some((s, 0));
                break;
            }
            Err(e) => {
                first_error.get_or_insert(e.to_string());
            }
        }
    }
    let (statements, skipped) = statements.unwrap_or_else(|| {
        let mut out = Vec::new();
        let mut skipped = 0;
        for piece in statements_of(sql).iter().filter(|p| !p.trim().is_empty()) {
            match dialects
                .iter()
                .find_map(|d| Parser::parse_sql(*d, piece).ok())
            {
                Some(s) => out.extend(s),
                None => skipped += 1,
            }
        }
        (out, skipped)
    });
    let model = model(&statements);
    if model.tables.is_empty() {
        return Err(SqlError(first_error));
    }
    Ok((model, skipped))
}

/// The text of each statement, split at the semicolons the SQL tokenizer
/// finds, so one inside a string literal (`COMMENT 'open; or closed'`) or a
/// quoted name doesn't cut a statement short. Text the tokenizer can't read
/// at all is split on every `;`.
fn statements_of(sql: &str) -> Vec<String> {
    let dialect = GenericDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return sql.split(';').map(str::to_string).collect();
    };
    let mut out = vec![String::new()];
    for token in tokens {
        if token == Token::SemiColon {
            out.push(String::new());
        } else if let Some(current) = out.last_mut() {
            current.push_str(&token.to_string());
        }
    }
    out
}

/// A name's parts, unquoted and joined with `.` (`crm.users`).
fn qualified(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
            other => other.to_string(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// The column a key's index entry names (`id`, `` `id` ``, `"Id"`).
fn key_column(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(ids) => ids.last().map(|i| i.value.clone()).unwrap_or_default(),
        other => other
            .to_string()
            .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']')
            .to_string(),
    }
}

/// The quoted strings in a piece of SQL text, `''` unescaped.
fn quoted_strings(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut value = String::new();
        loop {
            match chars.next() {
                Some('\'') if chars.peek() == Some(&'\'') => {
                    chars.next();
                    value.push('\'');
                }
                Some('\'') | None => break,
                Some(ch) => value.push(ch),
            }
        }
        out.push(value);
    }
    out
}

/// The last part of a (possibly qualified, possibly quoted) name.
fn last(name: &ObjectName) -> String {
    let text = name.to_string();
    let part = text.rsplit('.').next().unwrap_or(&text);
    part.trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']')
        .to_string()
}

fn unquote(text: &str) -> String {
    text.trim()
        .trim_matches(|c| c == '\'' || c == '"')
        .replace("''", "'")
}

/// An XSD datatype for a SQL type.
fn xsd(data_type: &DataType) -> Option<&'static str> {
    xsd_for(&data_type.to_string())
}

/// An XSD datatype's local name for a SQL type name (`VARCHAR(200)`,
/// `TIMESTAMP_NTZ`, `NUMBER(38,0)`), as warehouses and semantic layers write
/// them.
pub fn xsd_for(type_name: &str) -> Option<&'static str> {
    let text = type_name.trim().to_ascii_uppercase();
    let base = text.split(['(', ' ']).next().unwrap_or_default();
    Some(match base {
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "MEDIUMINT" | "SERIAL"
        | "BIGSERIAL" | "SMALLSERIAL" | "INT2" | "INT4" | "INT8" => "integer",
        "DECIMAL" | "NUMERIC" | "NUMBER" | "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE"
        | "MONEY" => "decimal",
        "DATE" => "date",
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATETIME" | "DATETIME2" | "TIMESTAMP_NTZ"
        | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => "dateTime",
        "TIME" => "time",
        "BOOLEAN" | "BOOL" | "BIT" => "boolean",
        "CHAR" | "VARCHAR" | "CHARACTER" | "TEXT" | "STRING" | "NVARCHAR" | "NCHAR" | "CLOB"
        | "UUID" | "CITEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" => "string",
        _ => return None,
    })
}

/// `col IN ('a', 'b')` in a check: the column and its values.
fn in_list(expr: &Expr) -> Option<(String, Vec<String>)> {
    match expr {
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let column = match expr.as_ref() {
                Expr::Identifier(id) => id.value.clone(),
                Expr::CompoundIdentifier(ids) => ids.last()?.value.clone(),
                _ => return None,
            };
            Some((
                column,
                list.iter().map(|v| unquote(&v.to_string())).collect(),
            ))
        }
        // pg_dump: ((status)::text = ANY ((ARRAY['a'::character varying, ...])::text[]))
        Expr::AnyOp {
            left,
            compare_op: sqlparser::ast::BinaryOperator::Eq,
            right,
            ..
        } => {
            let column = column_of(left)?;
            let values = quoted_strings(&right.to_string());
            (!values.is_empty()).then_some((column, values))
        }
        Expr::Nested(inner) => in_list(inner),
        _ => None,
    }
}

/// The column an expression names, through parentheses and casts.
fn column_of(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(ids) => ids.last().map(|i| i.value.clone()),
        Expr::Nested(inner) => column_of(inner),
        Expr::Cast { expr, .. } => column_of(expr),
        _ => None,
    }
}

fn comment_text(c: &CommentDef) -> String {
    match c {
        CommentDef::WithEq(s) | CommentDef::WithoutEq(s) => s.clone(),
    }
}

fn model(statements: &[Statement]) -> Model {
    // enum types first: a column may use one declared later in the file
    let mut enums: HashMap<String, Vec<String>> = HashMap::new();
    for s in statements {
        if let Statement::CreateType {
            name,
            representation: Some(UserDefinedTypeRepresentation::Enum { labels }),
        } = s
        {
            enums.insert(
                last(name).to_lowercase(),
                labels.iter().map(|l| l.value.clone()).collect(),
            );
        }
    }
    // each table with its qualified name (`crm.users`), and the checks its
    // constraints carry until every constraint has been read
    let mut tables: Vec<(String, Table, Vec<Expr>)> = Vec::new();
    for s in statements {
        if let Statement::CreateTable(create) = s {
            let (table, checks) = table(create, &enums);
            tables.push((qualified(&create.name), table, checks));
        }
    }
    // pg_dump adds keys, foreign keys and checks after the tables
    let mut unplaced = 0;
    for s in statements {
        let Statement::AlterTable(alter) = s else {
            continue;
        };
        let name = qualified(&alter.name);
        let short = last(&alter.name);
        let at = tables
            .iter()
            .position(|(q, _, _)| q.eq_ignore_ascii_case(&name))
            .or_else(|| {
                tables
                    .iter()
                    .position(|(_, t, _)| t.name.eq_ignore_ascii_case(&short))
            });
        let Some((_, table, checks)) = at.map(|i| &mut tables[i]) else {
            unplaced += 1;
            continue;
        };
        for op in &alter.operations {
            if let AlterTableOperation::AddConstraint { constraint, .. } = op {
                apply_constraint(table, constraint, checks);
            }
        }
    }
    let mut notes = Vec::new();
    if unplaced > 0 {
        notes.push(format!(
            "{unplaced} ALTER TABLE statement(s) name tables the input doesn't create, and were passed over"
        ));
    }
    for (_, table, checks) in &mut tables {
        for check in checks.drain(..) {
            if let Some((name, values)) = in_list(&check) {
                if let Some(col) = table
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(&name))
                {
                    col.values = values;
                }
            }
        }
    }
    // tables of one name in two schemas stay apart, named with the schema
    let mut by_short: HashMap<String, usize> = HashMap::new();
    for (_, t, _) in &tables {
        *by_short.entry(t.name.to_lowercase()).or_default() += 1;
    }
    for (q, t, _) in &mut tables {
        if by_short[&t.name.to_lowercase()] > 1 {
            notes.push(format!(
                "{q}: named with its schema, since another schema has a table {:?}",
                t.name
            ));
            t.name = q.clone();
        }
    }
    // references resolve to the qualified table when one is named, else to
    // the one table of that name
    let names: Vec<(String, String)> = tables
        .iter()
        .map(|(q, t, _)| (q.to_lowercase(), t.name.clone()))
        .collect();
    for (_, t, _) in &mut tables {
        for c in &mut t.columns {
            let Some(r) = c.references.clone() else {
                continue;
            };
            let short = r.rsplit('.').next().unwrap_or(&r).to_lowercase();
            let resolved = names
                .iter()
                .find(|(q, _)| *q == r.to_lowercase())
                .or_else(|| {
                    let matching: Vec<_> = names
                        .iter()
                        .filter(|(q, _)| q.rsplit('.').next() == Some(short.as_str()))
                        .collect();
                    (matching.len() == 1).then(|| matching[0])
                })
                .map(|(_, name)| name.clone());
            c.references = Some(resolved.unwrap_or(short));
        }
    }
    let mut tables: Vec<Table> = tables.into_iter().map(|(_, t, _)| t).collect();
    for s in statements {
        let Statement::Comment {
            object_type,
            object_name,
            comment: Some(text),
            ..
        } = s
        else {
            continue;
        };
        let full = object_name.to_string();
        let parts: Vec<String> = full
            .split('.')
            .map(|p| p.trim_matches(|c| c == '"' || c == '`').to_string())
            .collect();
        match object_type {
            CommentObject::Table => {
                // the table named with its schema, else the one of that name
                let full = parts.join(".");
                let at = tables
                    .iter()
                    .position(|t| t.name.eq_ignore_ascii_case(&full))
                    .or_else(|| {
                        tables.iter().position(|t| {
                            parts.last().is_some_and(|n| t.name.eq_ignore_ascii_case(n))
                        })
                    });
                if let Some(i) = at {
                    tables[i].description = Some(text.clone());
                }
            }
            CommentObject::Column if parts.len() >= 2 => {
                let (table_name, column_name) = (&parts[parts.len() - 2], &parts[parts.len() - 1]);
                if let Some(c) = tables
                    .iter_mut()
                    .filter(|t| t.name.eq_ignore_ascii_case(table_name))
                    .flat_map(|t| t.columns.iter_mut())
                    .find(|c| c.name.eq_ignore_ascii_case(column_name))
                {
                    c.description = Some(text.clone());
                }
            }
            _ => {}
        }
    }
    Model { tables, notes }
}

/// A table constraint, in CREATE TABLE or added by ALTER TABLE.
fn apply_constraint(t: &mut Table, constraint: &TableConstraint, checks: &mut Vec<Expr>) {
    match constraint {
        TableConstraint::PrimaryKey(pk) => {
            for c in &pk.columns {
                t.primary_key.push(key_column(&c.column.expr));
            }
        }
        TableConstraint::ForeignKey(fk) => {
            let target = qualified(&fk.foreign_table);
            for c in &fk.columns {
                if let Some(col) = t
                    .columns
                    .iter_mut()
                    .find(|x| x.name.eq_ignore_ascii_case(&c.value))
                {
                    col.references = Some(target.clone());
                }
            }
        }
        TableConstraint::Check(check) => checks.push((*check.expr).clone()),
        _ => {}
    }
}

fn table(create: &CreateTable, enums: &HashMap<String, Vec<String>>) -> (Table, Vec<Expr>) {
    let mut t = Table {
        name: last(&create.name),
        description: create.comment.as_ref().map(comment_text),
        ..Table::default()
    };
    // MySQL writes a table's comment among its options
    if t.description.is_none() {
        if let CreateTableOptions::Plain(options)
        | CreateTableOptions::Options(options)
        | CreateTableOptions::With(options)
        | CreateTableOptions::TableProperties(options) = &create.table_options
        {
            t.description = options.iter().find_map(|o| match o {
                SqlOption::Comment(c) => Some(comment_text(c)),
                _ => None,
            });
        }
    }
    let mut checks = Vec::new();
    for def in &create.columns {
        let mut column = Column {
            name: def.name.value.clone(),
            datatype: xsd(&def.data_type),
            ..Column::default()
        };
        match &def.data_type {
            DataType::Enum(members, _) => {
                column.values = members
                    .iter()
                    .map(|m| match m {
                        sqlparser::ast::EnumMember::Name(n)
                        | sqlparser::ast::EnumMember::NamedValue(n, _) => n.clone(),
                    })
                    .collect();
            }
            DataType::Custom(name, _) => {
                if let Some(values) = enums.get(&last(name).to_lowercase()) {
                    column.values = values.clone();
                    column.datatype = Some("string");
                }
            }
            _ => {}
        }
        for option in &def.options {
            match &option.option {
                ColumnOption::PrimaryKey(_) => t.primary_key.push(column.name.clone()),
                ColumnOption::ForeignKey(fk) => {
                    column.references = Some(qualified(&fk.foreign_table));
                }
                ColumnOption::Check(check) => checks.push((*check.expr).clone()),
                ColumnOption::Comment(text) => column.description = Some(text.clone()),
                _ => {}
            }
        }
        t.columns.push(column);
    }
    for constraint in &create.constraints {
        apply_constraint(&mut t, constraint, &mut checks);
    }
    (t, checks)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DDL: &str = r"
        CREATE TYPE ticket_priority AS ENUM ('low', 'normal', 'urgent');
        CREATE TABLE companies (
            company_id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            founded DATE
        );
        CREATE TABLE contacts (
            id INTEGER,
            employer_id INTEGER REFERENCES companies(company_id),
            full_name TEXT,
            status VARCHAR(10) CHECK (status IN ('active', 'left')),
            PRIMARY KEY (id)
        );
        CREATE TABLE tickets (
            id INT PRIMARY KEY,
            contact_id INT,
            priority ticket_priority,
            opened_at TIMESTAMP,
            CONSTRAINT fk_contact FOREIGN KEY (contact_id) REFERENCES contacts(id)
        );
        CREATE INDEX idx_name ON companies(name);
        COMMENT ON TABLE companies IS 'Customer accounts and partners.';
        COMMENT ON COLUMN contacts.full_name IS 'The contact''s name as they sign it.';
    ";

    #[test]
    fn ddl() {
        let (m, skipped) = read(DDL).unwrap();
        assert_eq!(skipped, 0);
        assert_eq!(m.tables.len(), 3);
        let t = |n: &str| m.tables.iter().find(|t| t.name == n).unwrap();
        let c = |t: &str, n: &str| {
            m.tables
                .iter()
                .find(|x| x.name == t)
                .unwrap()
                .columns
                .iter()
                .find(|c| c.name == n)
                .unwrap()
                .clone()
        };
        assert_eq!(t("companies").primary_key, vec!["company_id"]);
        assert_eq!(t("contacts").primary_key, vec!["id"]);
        assert_eq!(
            t("companies").description.as_deref(),
            Some("Customer accounts and partners.")
        );
        assert_eq!(
            c("contacts", "employer_id").references.as_deref(),
            Some("companies")
        );
        assert_eq!(
            c("tickets", "contact_id").references.as_deref(),
            Some("contacts")
        );
        assert_eq!(c("contacts", "status").values, vec!["active", "left"]);
        assert_eq!(
            c("tickets", "priority").values,
            vec!["low", "normal", "urgent"]
        );
        assert_eq!(c("tickets", "opened_at").datatype, Some("dateTime"));
        assert_eq!(c("companies", "founded").datatype, Some("date"));
        assert_eq!(
            c("contacts", "full_name").description.as_deref(),
            Some("The contact's name as they sign it.")
        );
    }

    #[test]
    fn mysql_enum_and_inline_comment() {
        let (m, _) = read(
            "CREATE TABLE `orders` (`id` int NOT NULL AUTO_INCREMENT, `state` enum('new','paid') COMMENT 'Where the order is', PRIMARY KEY (`id`)) COMMENT='Customer orders';",
        )
        .unwrap();
        let t = &m.tables[0];
        assert_eq!(t.name, "orders");
        assert_eq!(t.description.as_deref(), Some("Customer orders"));
        assert_eq!(t.primary_key, ["id"], "a backticked key matches its column");
        assert_eq!(t.columns[1].values, vec!["new", "paid"]);
        assert_eq!(
            t.columns[1].description.as_deref(),
            Some("Where the order is")
        );
    }

    #[test]
    fn nothing_readable() {
        assert!(read("SELECT 1;").is_err());
    }

    #[test]
    fn mysql_backticked_primary_key() {
        let (m, _) = read(
            "CREATE TABLE `orders` (`id` int NOT NULL, `state` varchar(10), PRIMARY KEY (`id`));",
        )
        .unwrap();
        assert_eq!(m.tables[0].primary_key, ["id"]);
    }

    #[test]
    fn pg_dump_constraints_after_the_tables() {
        let (m, skipped) = read(
            r"
CREATE TABLE public.companies (id integer NOT NULL, name text);
CREATE TABLE public.contacts (
    id integer NOT NULL,
    employer_id integer,
    status character varying(10),
    CONSTRAINT contacts_status_check CHECK (((status)::text = ANY ((ARRAY['active'::character varying, 'left'::character varying])::text[])))
);
ALTER TABLE ONLY public.companies ADD CONSTRAINT companies_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.contacts ADD CONSTRAINT contacts_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.contacts
    ADD CONSTRAINT contacts_employer_id_fkey FOREIGN KEY (employer_id) REFERENCES public.companies(id);
",
        )
        .unwrap();
        assert_eq!(skipped, 0);
        let t = |n: &str| m.tables.iter().find(|t| t.name == n).unwrap();
        assert_eq!(t("companies").primary_key, ["id"]);
        assert_eq!(t("contacts").primary_key, ["id"]);
        let c = |n: &str| {
            t("contacts")
                .columns
                .iter()
                .find(|c| c.name == n)
                .unwrap()
                .clone()
        };
        assert_eq!(c("employer_id").references.as_deref(), Some("companies"));
        assert_eq!(c("status").values, ["active", "left"]);
    }

    #[test]
    fn a_semicolon_in_a_string_doesnt_split_a_statement() {
        // DELIMITER makes every dialect refuse the whole text, so statements
        // are read one by one
        let (m, skipped) = read(
            "DELIMITER ;;\nCREATE TABLE a (id int PRIMARY KEY, state varchar(9) COMMENT 'open; or closed');\nCREATE TABLE b (id int);\nCOMMENT ON TABLE b IS 'Notes; misc';\n",
        )
        .unwrap();
        assert!(skipped >= 1, "DELIMITER is skipped");
        let a = m
            .tables
            .iter()
            .find(|t| t.name == "a")
            .expect("table a is read");
        assert_eq!(a.columns[1].description.as_deref(), Some("open; or closed"));
        let b = m.tables.iter().find(|t| t.name == "b").unwrap();
        assert_eq!(b.description.as_deref(), Some("Notes; misc"));
    }

    #[test]
    fn same_table_name_in_two_schemas() {
        let (m, _) = read(
            "CREATE TABLE crm.users (id int PRIMARY KEY, status text CHECK (status IN ('a', 'b')));\nCREATE TABLE billing.users (id int PRIMARY KEY, plan_id int);\nCREATE TABLE billing.invoices (id int PRIMARY KEY, user_id int REFERENCES billing.users(id));",
        )
        .unwrap();
        let names: Vec<&str> = m.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["crm.users", "billing.users", "invoices"]);
        let invoices = m.tables.iter().find(|t| t.name == "invoices").unwrap();
        assert_eq!(
            invoices.columns[1].references.as_deref(),
            Some("billing.users")
        );
        assert!(
            m.notes.iter().any(|n| n.contains("crm.users")),
            "{:?}",
            m.notes
        );
    }
}
