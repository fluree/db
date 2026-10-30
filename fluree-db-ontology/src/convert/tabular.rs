//! Tables from a CSV file or a workbook: a header row and the rows under it.
//!
//! A sheet is one table, named for the sheet (a CSV for its file). Column
//! types, keys and enumerated columns are read from the rows:
//!
//! - a column whose values are all integers, decimals, ISO dates or
//!   date-times, or booleans gets that datatype (a number with a leading
//!   zero is a code, and stays text);
//! - a column named `id`, or the table's name in the singular with `_id`
//!   (`company_id` in `companies`), is the primary key;
//! - any other column ending in `id` (`employer_id`, `CustomerID`) is a
//!   foreign key when it names another sheet (`customer_id` and a
//!   `customers` sheet); otherwise it stays a value (`tax_id`, "tax id");
//! - a text column with at most `enum_max` distinct short values, each
//!   repeated (at least twice as many rows as values, and five rows), is
//!   enumerated.

use super::{class_label, strip_word_suffix, Column, Model, Table};
use crate::words::name_words;
use std::collections::BTreeSet;

/// One sheet: its name, its header and its rows (each as long as the
/// header).
#[derive(Clone, Debug, Default)]
pub struct Sheet {
    pub name: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, thiserror::Error)]
pub enum TabularError {
    #[error("invalid CSV: {0}")]
    Csv(#[from] csv::Error),
    #[error("invalid workbook: {0}")]
    Xlsx(String),
    #[error("{0} has no header row")]
    Empty(String),
}

/// Values longer than this are not enumerated.
const ENUM_VALUE_CHARS: usize = 40;

/// A CSV file's sheet. The delimiter is the first of `,`, `;`, tab or `|`
/// that the header line contains.
pub fn read_csv(text: &str, name: &str) -> Result<Sheet, TabularError> {
    let first = text.lines().next().unwrap_or_default();
    let delimiter = b",;\t|"
        .iter()
        .copied()
        .find(|d| first.as_bytes().contains(d))
        .unwrap_or(b',');
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .flexible(true)
        .from_reader(text.trim_start_matches('\u{feff}').as_bytes());
    let headers: Vec<String> = reader
        .headers()?
        .iter()
        .map(|h| h.trim().to_string())
        .collect();
    if headers.iter().all(String::is_empty) {
        return Err(TabularError::Empty(name.to_string()));
    }
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        let mut row: Vec<String> = record.iter().map(|v| v.trim().to_string()).collect();
        row.resize(headers.len(), String::new());
        rows.push(row);
    }
    Ok(Sheet {
        name: name.to_string(),
        headers,
        rows,
    })
}

/// A workbook's sheets: for each, the first table under the sheet's title.
pub fn read_xlsx(bytes: &[u8]) -> Result<Vec<Sheet>, TabularError> {
    let elements = fluree_doc_xlsx::parse(bytes).map_err(|e| TabularError::Xlsx(e.to_string()))?;
    let mut sheets: Vec<Sheet> = Vec::new();
    let mut current: Option<String> = None;
    let mut taken = BTreeSet::new();
    for e in elements {
        if e.kind == "doco:SectionTitle" && e.level == Some(1) {
            current = Some(e.text.trim().to_string());
            continue;
        }
        let Some(cells) = e.cells.as_ref().filter(|_| e.kind == "doco:Table") else {
            continue;
        };
        let name = current
            .clone()
            .unwrap_or_else(|| format!("sheet {}", e.page + 1));
        if !taken.insert(e.page) {
            continue;
        }
        let header_at = e.header_rows.unwrap_or(1).max(1) - 1;
        let Some(headers) = cells.get(header_at) else {
            continue;
        };
        let headers: Vec<String> = headers.iter().map(|h| h.trim().to_string()).collect();
        let rows = cells
            .iter()
            .skip(header_at + 1)
            .map(|r| {
                let mut row: Vec<String> = r.iter().map(|v| v.trim().to_string()).collect();
                row.resize(headers.len(), String::new());
                row
            })
            .collect();
        sheets.push(Sheet {
            name,
            headers,
            rows,
        });
    }
    Ok(sheets)
}

/// The XSD datatype every non-empty value fits, if any.
pub fn infer_datatype(values: &[&str]) -> Option<&'static str> {
    if values.is_empty() {
        return None;
    }
    let all = |f: fn(&str) -> bool| values.iter().all(|v| f(v));
    Some(if all(is_integer) {
        "integer"
    } else if all(is_decimal) {
        "decimal"
    } else if all(is_date) {
        "date"
    } else if all(is_date_time) {
        "dateTime"
    } else if all(is_boolean) {
        "boolean"
    } else {
        "string"
    })
}

fn is_integer(v: &str) -> bool {
    let digits = v.strip_prefix(['-', '+']).unwrap_or(v);
    !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && !(digits.len() > 1 && digits.starts_with('0'))
}

fn is_decimal(v: &str) -> bool {
    let digits = v.strip_prefix(['-', '+']).unwrap_or(v);
    match digits.split_once('.') {
        Some((a, b)) => {
            !b.is_empty()
                && (a.is_empty() || is_integer(a))
                && b.bytes().all(|c| c.is_ascii_digit())
        }
        None => is_integer(digits),
    }
}

fn is_date(v: &str) -> bool {
    let b = v.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b.iter()
            .enumerate()
            .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit())
}

fn is_date_time(v: &str) -> bool {
    // by bytes: the 10th may fall inside a multi-byte character
    v.get(..10).is_some_and(is_date) && matches!(v.as_bytes().get(10), Some(b'T' | b' '))
}

fn is_boolean(v: &str) -> bool {
    matches!(
        v.to_ascii_lowercase().as_str(),
        "true" | "false" | "yes" | "no"
    )
}

/// The stem of a key column's name: `employer_id` and `employerId` →
/// "employer". `None` when the name doesn't end in "id".
fn key_stem(name: &str) -> Option<String> {
    let words = name_words(name)?;
    let stem = strip_word_suffix(&words, "id")?;
    Some(stem.to_string())
}

/// The model for a set of sheets.
pub fn model(sheets: &[Sheet], enum_max: usize) -> Model {
    let labels: Vec<(String, String)> = sheets
        .iter()
        .map(|s| (class_label(&s.name), s.name.clone()))
        .collect();
    let mut tables = Vec::new();
    for sheet in sheets {
        let own = class_label(&sheet.name);
        let mut table = Table {
            name: sheet.name.clone(),
            ..Table::default()
        };
        for (i, header) in sheet.headers.iter().enumerate() {
            if header.is_empty() {
                continue;
            }
            let values: Vec<&str> = sheet
                .rows
                .iter()
                .filter_map(|r| r.get(i).map(String::as_str))
                .filter(|v| !v.is_empty())
                .collect();
            let mut column = Column {
                name: header.clone(),
                datatype: infer_datatype(&values),
                ..Column::default()
            };
            let lower = header.to_ascii_lowercase();
            match key_stem(header) {
                _ if lower == "id" => table.primary_key.push(header.clone()),
                Some(stem) if class_label(&stem) == own => table.primary_key.push(header.clone()),
                // a key to another sheet is a relation; `tax_id`, naming no
                // sheet, stays a value ("tax id")
                Some(stem) => {
                    let wanted = class_label(&stem);
                    column.references = labels
                        .iter()
                        .find(|(label, _)| *label == wanted)
                        .map(|(_, name)| name.clone());
                }
                None => {}
            }
            if column.references.is_none() && column.datatype == Some("string") && enum_max > 0 {
                let distinct: BTreeSet<&str> = values.iter().copied().collect();
                if (2..=enum_max).contains(&distinct.len())
                    && values.len() >= (2 * distinct.len()).max(5)
                    && distinct
                        .iter()
                        .all(|v| v.chars().count() <= ENUM_VALUE_CHARS)
                {
                    column.values = distinct.into_iter().map(str::to_string).collect();
                }
            }
            table.columns.push(column);
        }
        tables.push(table);
    }
    Model {
        tables,
        ..Model::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_types_keys_and_enums() {
        let sheet = read_csv(
            "id;name;employer_id;status;joined;zip\n\
             1;Ada;10;active;2024-01-02;02139\n\
             2;Bo;10;active;2024-02-03;02140\n\
             3;Cy;11;left;2024-03-04;10001\n\
             4;Di;12;active;2024-04-05;10002\n\
             5;Ed;12;left;2024-05-06;10003\n",
            "people",
        )
        .unwrap();
        assert_eq!(sheet.headers.len(), 6);
        let m = model(&[sheet], 12);
        let t = &m.tables[0];
        assert_eq!(t.primary_key, vec!["id"]);
        let col = |n: &str| t.columns.iter().find(|c| c.name == n).unwrap();
        assert_eq!(col("employer_id").references, None, "no sheet named for it");
        assert_eq!(col("status").values, vec!["active", "left"]);
        assert_eq!(col("joined").datatype, Some("date"));
        assert_eq!(
            col("zip").datatype,
            Some("string"),
            "leading zeros are codes"
        );
        assert!(col("name").values.is_empty(), "names don't repeat");
    }

    #[test]
    fn keys_link_sheets() {
        let companies = Sheet {
            name: "companies".into(),
            headers: vec!["company_id".into(), "name".into()],
            rows: vec![],
        };
        let people = Sheet {
            name: "people".into(),
            headers: vec!["id".into(), "companyId".into()],
            rows: vec![],
        };
        let m = model(&[companies, people], 12);
        assert_eq!(m.tables[0].primary_key, vec!["company_id"]);
        assert_eq!(m.tables[1].primary_key, vec!["id"]);
        assert_eq!(
            m.tables[1].columns[1].references.as_deref(),
            Some("companies")
        );
    }

    #[test]
    fn datatypes() {
        assert_eq!(infer_datatype(&["1", "-2"]), Some("integer"));
        assert_eq!(infer_datatype(&["1.5", "2"]), Some("decimal"));
        assert_eq!(infer_datatype(&["2026-10-29T10:00:00Z"]), Some("dateTime"));
        assert_eq!(infer_datatype(&["yes", "No"]), Some("boolean"));
        assert_eq!(infer_datatype(&["007"]), Some("string"));
        assert_eq!(infer_datatype(&[]), None);
    }

    #[test]
    fn multi_byte_text_and_upper_case_ids() {
        let sheet = read_csv(
            "OrderID,CustomerID,city,tax_id\n1,7,Kraków Główny,PL-1\n2,8,東京都千代田区,PL-2\n",
            "orders",
        )
        .unwrap();
        let customers = Sheet {
            name: "customers".into(),
            headers: vec!["CustomerID".into()],
            rows: vec![],
        };
        let m = model(&[sheet, customers], 12);
        let orders = &m.tables[0];
        assert_eq!(orders.primary_key, ["OrderID"]);
        let col = |n: &str| orders.columns.iter().find(|c| c.name == n).unwrap();
        assert_eq!(col("CustomerID").references.as_deref(), Some("customers"));
        assert_eq!(col("city").datatype, Some("string"));
        assert_eq!(col("tax_id").references, None);
        assert_eq!(m.tables[1].primary_key, ["CustomerID"]);
    }
}
