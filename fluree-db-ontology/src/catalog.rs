//! A catalog of records from rows you already have.
//!
//! The extraction model links the names it reads to catalog records by their
//! SKOS text, so each row becomes one record of a class:
//!
//! - `skos:prefLabel` from the label column;
//! - `skos:altLabel` from alias columns (several aliases in one cell split
//!   on `;` or `|`), except that an email address goes to `skos:hiddenLabel`;
//! - `skos:hiddenLabel` from code columns (account numbers, tickers): names
//!   a document may use that aren't shown as names;
//! - `skos:definition` from a description column, or written from other
//!   columns as `industry: retail; city: Leeds`.
//!
//! A record's IRI comes from an id column, or else from its label. Rows with
//! the same IRI are one record, their labels pooled.

use crate::convert::slug;
use crate::convert::tabular::Sheet;
use crate::estimate::{self, Estimate, Sizes};
use crate::read::Quad;
use crate::vocab::*;
use crate::words::name_words;
use fluree_graph_ir::Term;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default)]
pub struct CatalogOptions {
    /// The class every record is typed with (an IRI).
    pub class: String,
    pub label: String,
    pub alt: Vec<String>,
    pub code: Vec<String>,
    pub definition: Option<String>,
    /// Columns written into a definition, where there is no `definition`.
    pub describe: Vec<String>,
    pub id: Option<String>,
    /// Namespace for record IRIs.
    pub base: String,
}

#[derive(Debug, thiserror::Error)]
#[error("{sheet} has no column {column:?} (its columns: {})", .columns.join(", "))]
pub struct MissingColumn {
    pub sheet: String,
    pub column: String,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct Catalog {
    pub quads: Vec<Quad>,
    pub records: usize,
    /// Rows with no label, left out.
    pub without_label: usize,
    /// Rows folded into an earlier record with the same IRI.
    pub merged: usize,
    pub estimate: Estimate,
}

#[derive(Default)]
struct Record {
    pref: Option<String>,
    alt: BTreeSet<String>,
    hidden: BTreeSet<String>,
    definition: Option<String>,
}

fn split_cell(cell: &str) -> impl Iterator<Item = &str> {
    cell.split([';', '|'])
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

/// The catalog for a sheet's rows.
pub fn from_sheet(sheet: &Sheet, options: &CatalogOptions) -> Result<Catalog, MissingColumn> {
    let index = |column: &str| -> Result<usize, MissingColumn> {
        sheet
            .headers
            .iter()
            .position(|h| h == column)
            .or_else(|| {
                sheet
                    .headers
                    .iter()
                    .position(|h| h.eq_ignore_ascii_case(column))
            })
            .ok_or_else(|| MissingColumn {
                sheet: sheet.name.clone(),
                column: column.to_string(),
                columns: sheet.headers.clone(),
            })
    };
    let label = index(&options.label)?;
    let alt = options
        .alt
        .iter()
        .map(|c| index(c))
        .collect::<Result<Vec<_>, _>>()?;
    let code = options
        .code
        .iter()
        .map(|c| index(c))
        .collect::<Result<Vec<_>, _>>()?;
    let describe = options
        .describe
        .iter()
        .map(|c| index(c).map(|i| (i, name_words(c).unwrap_or_else(|| c.clone()))))
        .collect::<Result<Vec<_>, _>>()?;
    let definition = options.definition.as_deref().map(index).transpose()?;
    let id = options.id.as_deref().map(index).transpose()?;

    let mut out = Catalog::default();
    let mut records: BTreeMap<String, Record> = BTreeMap::new();
    for row in &sheet.rows {
        let cell = |i: usize| row.get(i).map(|v| v.trim()).unwrap_or_default();
        let name = cell(label);
        if name.is_empty() {
            out.without_label += 1;
            continue;
        }
        let key = id.map(cell).filter(|v| !v.is_empty()).unwrap_or(name);
        let iri = format!("{}{}", options.base, slug(key));
        let fresh = !records.contains_key(&iri);
        if !fresh {
            out.merged += 1;
        }
        let record = records.entry(iri).or_default();
        match &record.pref {
            None => record.pref = Some(name.to_string()),
            Some(p) if p != name => {
                record.alt.insert(name.to_string());
            }
            Some(_) => {}
        }
        for &i in &alt {
            for value in split_cell(cell(i)) {
                if value.contains('@') {
                    record.hidden.insert(value.to_string());
                } else if Some(value) != record.pref.as_deref() {
                    record.alt.insert(value.to_string());
                }
            }
        }
        for &i in &code {
            for value in split_cell(cell(i)) {
                record.hidden.insert(value.to_string());
            }
        }
        if record.definition.is_none() {
            record.definition = match definition {
                Some(i) => Some(cell(i).to_string()).filter(|d| !d.is_empty()),
                None => {
                    let parts: Vec<String> = describe
                        .iter()
                        .filter(|(i, _)| !cell(*i).is_empty())
                        .map(|(i, words)| format!("{words}: {}", cell(*i)))
                        .collect();
                    (!parts.is_empty()).then(|| parts.join("; "))
                }
            };
        }
    }
    let mut catalog_chars = 0;
    for (iri, record) in &records {
        let s = Term::iri(iri);
        out.quads
            .push(Quad::new(s.clone(), RDF_TYPE, Term::iri(&options.class)));
        let mut text = |p: &str, v: &str| {
            catalog_chars += v.chars().count() + 2;
            out.quads.push(Quad::new(s.clone(), p, Term::string(v)));
        };
        if let Some(p) = &record.pref {
            text(SKOS_PREF_LABEL, p);
        }
        for a in &record.alt {
            text(SKOS_ALT_LABEL, a);
        }
        for h in &record.hidden {
            text(SKOS_HIDDEN_LABEL, h);
        }
        if let Some(d) = &record.definition {
            catalog_chars += estimate::definition_chars(d);
            out.quads
                .push(Quad::new(s.clone(), SKOS_DEFINITION, Term::string(d)));
        }
    }
    out.records = records.len();
    out.estimate = estimate::estimate(Sizes {
        terms: 0,
        ontology_chars: 0,
        records: out.records,
        catalog_chars,
    });
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::tabular::read_csv;

    #[test]
    fn rows_become_skos_records() {
        let sheet = read_csv(
            "account,name,aliases,ticker,industry,city\n\
             A-1,Daxin Scientific Supply Co.,Daxin; daxin.example@mail.test,DXS,lab supplies,Taipei\n\
             A-2,Dexin Instruments,,,instruments,\n\
             A-1,Daxin Scientific,,DXN,,\n\
             ,,,,,\n",
            "accounts",
        )
        .unwrap();
        let options = CatalogOptions {
            class: "https://ex.com/Organization".into(),
            label: "name".into(),
            alt: vec!["aliases".into()],
            code: vec!["ticker".into()],
            describe: vec!["industry".into(), "city".into()],
            id: Some("account".into()),
            base: "https://ex.com/accounts/".into(),
            ..CatalogOptions::default()
        };
        let c = from_sheet(&sheet, &options).unwrap();
        assert_eq!(c.records, 2);
        assert_eq!(c.merged, 1);
        assert_eq!(c.without_label, 1);
        let values = |p: &str| -> Vec<String> {
            c.quads
                .iter()
                .filter(|q| q.subject.as_iri() == Some("https://ex.com/accounts/a-1") && q.is(p))
                .filter_map(|q| q.object.as_literal().map(|(v, ..)| v.lexical()))
                .collect()
        };
        assert_eq!(values(SKOS_PREF_LABEL), ["Daxin Scientific Supply Co."]);
        assert_eq!(values(SKOS_ALT_LABEL), ["Daxin", "Daxin Scientific"]);
        assert_eq!(
            values(SKOS_HIDDEN_LABEL),
            ["DXN", "DXS", "daxin.example@mail.test"]
        );
        assert_eq!(
            values(SKOS_DEFINITION),
            ["industry: lab supplies; city: Taipei"]
        );
        assert!(c.estimate.catalog_tokens > 0);
    }

    #[test]
    fn a_missing_column_names_the_ones_there_are() {
        let sheet = read_csv("name,city\nAcme,Leeds\n", "accounts").unwrap();
        let err = from_sheet(
            &sheet,
            &CatalogOptions {
                label: "title".into(),
                ..CatalogOptions::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("name, city"), "{err}");
    }
}
