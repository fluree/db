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
    ColumnOption, CommentDef, CommentObject, CreateTable, CreateTableOptions, DataType, Expr,
    ObjectName, SqlOption, Statement, TableConstraint, UserDefinedTypeRepresentation,
};
use sqlparser::dialect::{
    Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;
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
        for piece in sql.split(';').filter(|p| !p.trim().is_empty()) {
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
        Expr::Nested(inner) => in_list(inner),
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
    let mut tables: Vec<Table> = Vec::new();
    for s in statements {
        if let Statement::CreateTable(create) = s {
            tables.push(table(create, &enums));
        }
    }
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
                if let Some(t) = tables
                    .iter_mut()
                    .find(|t| parts.last().is_some_and(|n| t.name.eq_ignore_ascii_case(n)))
                {
                    t.description = Some(text.clone());
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
    Model { tables }
}

fn table(create: &CreateTable, enums: &HashMap<String, Vec<String>>) -> Table {
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
                ColumnOption::ForeignKey(fk) => column.references = Some(last(&fk.foreign_table)),
                ColumnOption::Check(check) => checks.push((*check.expr).clone()),
                ColumnOption::Comment(text) => column.description = Some(text.clone()),
                _ => {}
            }
        }
        t.columns.push(column);
    }
    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey(pk) => {
                for c in &pk.columns {
                    t.primary_key.push(unquote(&c.column.expr.to_string()));
                }
            }
            TableConstraint::ForeignKey(fk) => {
                let target = last(&fk.foreign_table);
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
    for check in checks {
        if let Some((name, values)) = in_list(&check) {
            if let Some(col) = t
                .columns
                .iter_mut()
                .find(|c| c.name.eq_ignore_ascii_case(&name))
            {
                col.values = values;
            }
        }
    }
    t
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
}
