//! CSV bulk import — a front-end that converts node / relationship CSV files
//! (the **neo4j-admin import** header convention) into JSON-LD objects, which
//! then feed the existing JSON-LD import path. By emitting JSON-LD we reuse the
//! whole downstream pipeline: `@annotation` → `f:reifies*` lowering, datatype
//! typing, namespace allocation, chunked commit, and annotation-arena sealing.
//!
//! # Header convention
//!
//! Columns are typed by their header cell (`name:type`), with reserved
//! `:`-prefixed system columns:
//!
//! | Header | Meaning |
//! |--------|---------|
//! | `:ID` / `id:ID(Person)` | Node identity → minted IRI. A `name:ID(space)` form *also* stores `name` as a property. `(space)` namespaces ids so `Person/0` ≠ `Comment/0`. |
//! | `:LABEL` | Node `rdf:type` (`;`-separated for multiple labels). |
//! | `:START_ID(space)` / `:END_ID(space)` | Relationship endpoints (reference a node id space). |
//! | `:TYPE` | Relationship predicate. |
//! | `name:int` / `name:double` / `name:date` / `name:string[]` | A typed property; `[]` is an array (split on `;`). |
//! | `:IGNORE` | Column skipped. |
//!
//! # RDF vs RDF 1.2 (the edge-property fork)
//!
//! A node row and a property-less edge row are always **plain RDF 1.1**. An edge
//! row that carries property columns is the only place reification enters, and
//! [`EdgePolicy`] (the CLI's `--edge-properties annotated|nary|plain`) picks the
//! encoding:
//! - [`EdgePolicy::Annotated`] (default) — property-bearing edges become an
//!   `@annotation` (RDF 1.2 / LPG): Cypher `(a)-[r:T]->(b)` + `r.prop` and
//!   SPARQL `{| … |}` both read the property; property-less edges stay plain.
//! - [`EdgePolicy::Nary`] — edge properties become an intermediate node (pure
//!   RDF 1.1, no reification). *Deferred* — the n-ary predicate convention isn't
//!   pinned yet, so this errors rather than guess.
//! - [`EdgePolicy::Plain`] — edge properties are dropped; every edge is a plain
//!   triple (pure RDF 1.1).

use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde_json::{Map, Value};
use std::borrow::Cow;
use std::io::{Read, Write};

/// Characters percent-encoded when turning a CSV cell value into an IRI
/// segment. Covers whitespace/controls, the RFC 3987-forbidden ASCII, and the
/// structural delimiters (`/ # ? [ ] @`) a value could use to break IRI syntax
/// or climb out of `base_iri`'s namespace (an embedded `http://evil/p`,
/// `../../x`). Unreserved chars and `:` are left intact so ordinary ids and
/// compact-looking values round-trip unchanged.
const IRI_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'\\')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(b'%')
    .add(b'/')
    .add(b'#')
    .add(b'?')
    .add(b'[')
    .add(b']')
    .add(b'@');

/// Percent-encode a CSV cell value so it is safe to append after `base_iri`.
fn iri_segment(s: &str) -> Cow<'_, str> {
    utf8_percent_encode(s, IRI_SEGMENT).into()
}

/// XSD namespace for typed literals emitted as JSON-LD `@value`/`@type`.
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// How a relationship that carries property columns is encoded in RDF — the
/// CLI's `--edge-properties` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EdgePolicy {
    /// Property-bearing edges → `@annotation` (RDF 1.2 / LPG); property-less
    /// edges → plain triple. Reads from both Cypher (`r.prop`) and SPARQL
    /// (`{| |}`).
    #[default]
    Annotated,
    /// Edge properties → an intermediate "statement" node (pure RDF 1.1, no
    /// reification). Deferred until the n-ary predicate convention is fixed.
    Nary,
    /// Drop edge properties — every edge is a plain RDF 1.1 triple.
    Plain,
}

/// Options for [`csv_to_jsonld`].
#[derive(Debug, Clone)]
pub struct CsvImportOptions {
    /// Edge-property encoding ([`EdgePolicy`]).
    pub edge_policy: EdgePolicy,
    /// Namespace prepended to every minted IRI (ids, predicates, classes).
    pub base_iri: String,
    /// Field delimiter (neo4j-admin's `FIELDTERMINATOR`).
    pub delimiter: u8,
    /// Separator for `name:type[]` array-valued columns.
    pub array_delimiter: char,
}

impl Default for CsvImportOptions {
    fn default() -> Self {
        Self {
            edge_policy: EdgePolicy::default(),
            base_iri: "http://example.org/".to_string(),
            delimiter: b',',
            array_delimiter: ';',
        }
    }
}

/// CSV-import failure (header convention or row-shape error).
#[derive(Debug, thiserror::Error)]
pub enum CsvImportError {
    #[error("CSV parse error: {0}")]
    Csv(#[from] csv::Error),
    #[error("CSV header is empty")]
    EmptyHeader,
    #[error("invalid header column `{col}`: {reason}")]
    BadHeader { col: String, reason: String },
    #[error(
        "ambiguous CSV: a header has both node (`:ID`) and relationship (`:START_ID`) columns"
    )]
    MixedHeader,
    #[error("relationship CSV needs a `:TYPE` column (or a fixed type), found none")]
    MissingType,
    #[error("node CSV needs an `:ID` column, found none")]
    MissingId,
    #[error("row {row} has {got} fields but the header has {want}")]
    RowWidth { row: usize, got: usize, want: usize },
    #[error("invalid value `{value}` for {kind} column `{col}` on row {row}")]
    BadValue {
        row: usize,
        col: String,
        kind: &'static str,
        value: String,
    },
    #[error(
        "`--edge-properties nary` is not implemented yet (relationship `{rel_type}` carries \
         properties); use `annotated` or `plain` for now"
    )]
    NaryDeferred { rel_type: String },
    #[error("io error writing JSON-LD: {0}")]
    Io(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, CsvImportError>;

/// A property column's datatype, parsed from the `:type` suffix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CsvType {
    String,
    Long,
    Double,
    Boolean,
    Date,
    DateTime,
}

impl CsvType {
    fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "int" | "long" | "short" | "byte" | "integer" => CsvType::Long,
            "float" | "double" => CsvType::Double,
            "boolean" | "bool" => CsvType::Boolean,
            "date" => CsvType::Date,
            "datetime" | "localdatetime" | "zoneddatetime" => CsvType::DateTime,
            _ => CsvType::String,
        }
    }

    /// JSON-LD value for a raw cell. Numbers/booleans use native JSON (JSON-LD
    /// infers xsd:integer/decimal/boolean); dates use a typed `@value` object.
    fn coerce(self, raw: &str, row: usize, col: &str) -> Result<Value> {
        let bad = |kind: &'static str| CsvImportError::BadValue {
            row,
            col: col.to_string(),
            kind,
            value: raw.to_string(),
        };
        Ok(match self {
            CsvType::String => Value::String(raw.to_string()),
            CsvType::Long => Value::Number(
                raw.trim()
                    .parse::<i64>()
                    .map_err(|_| bad("integer"))?
                    .into(),
            ),
            CsvType::Double => {
                let f = raw.trim().parse::<f64>().map_err(|_| bad("double"))?;
                serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .ok_or_else(|| bad("double"))?
            }
            CsvType::Boolean => Value::Bool(match raw.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => true,
                "false" | "0" | "no" => false,
                _ => return Err(bad("boolean")),
            }),
            // Validate against the same parsers the downstream pipeline uses,
            // so a malformed cell is rejected here (like `:int`/`:double`)
            // rather than silently stored as an invalid typed literal.
            CsvType::Date => {
                let v = raw.trim();
                fluree_db_core::Date::parse(v).map_err(|_| bad("date"))?;
                typed_literal(v, "date")
            }
            CsvType::DateTime => {
                let v = raw.trim();
                fluree_db_core::DateTime::parse(v).map_err(|_| bad("dateTime"))?;
                typed_literal(v, "dateTime")
            }
        })
    }
}

/// A `{"@value": v, "@type": xsd:<name>}` typed literal.
fn typed_literal(value: &str, xsd_name: &str) -> Value {
    let mut m = Map::new();
    m.insert("@value".to_string(), Value::String(value.to_string()));
    m.insert(
        "@type".to_string(),
        Value::String(format!("{XSD}{xsd_name}")),
    );
    Value::Object(m)
}

/// A parsed header column.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Column {
    /// `:ID` / `name:ID(space)` — `prop` set ⇒ also store as a property.
    Id {
        prop: Option<String>,
        space: Option<String>,
    },
    Label,
    StartId {
        space: Option<String>,
    },
    EndId {
        space: Option<String>,
    },
    Type,
    Property {
        name: String,
        ty: CsvType,
        array: bool,
    },
    Ignore,
}

/// Split a header cell into `(name, type_or_system, paren_arg, is_array)`.
/// `name:TYPE(arg)` / `:SYSTEM(arg)` / `name:type[]`.
fn parse_header_column(raw: &str) -> Result<Column> {
    let cell = raw.trim();
    // Split off a trailing `(space)` argument, if any.
    let (head, space) = match (cell.find('('), cell.strip_suffix(')')) {
        (Some(open), Some(_)) => (
            &cell[..open],
            Some(cell[open + 1..cell.len() - 1].to_string()),
        ),
        _ => (cell, None),
    };

    // `name:tag` — the part before the first `:` is the property name (may be
    // empty for bare system columns like `:ID`).
    let (name, tag) = match head.split_once(':') {
        Some((n, t)) => (n.trim(), t.trim()),
        None => (head.trim(), ""),
    };

    // System columns are case-insensitive on the tag.
    match tag.to_ascii_uppercase().as_str() {
        "ID" => Ok(Column::Id {
            prop: (!name.is_empty()).then(|| name.to_string()),
            space,
        }),
        "LABEL" => Ok(Column::Label),
        "START_ID" => Ok(Column::StartId { space }),
        "END_ID" => Ok(Column::EndId { space }),
        "TYPE" => Ok(Column::Type),
        "IGNORE" => Ok(Column::Ignore),
        _ => {
            if name.is_empty() {
                return Err(CsvImportError::BadHeader {
                    col: raw.to_string(),
                    reason: "property column needs a name (`name:type`)".to_string(),
                });
            }
            let (tag, array) = match tag.strip_suffix("[]") {
                Some(t) => (t, true),
                None => (tag, false),
            };
            Ok(Column::Property {
                name: name.to_string(),
                ty: CsvType::parse(tag),
                array,
            })
        }
    }
}

/// Whether a parsed header describes nodes or relationships.
enum Shape {
    Node,
    Rel,
}

fn classify(cols: &[Column]) -> Result<Shape> {
    let has_id = cols.iter().any(|c| matches!(c, Column::Id { .. }));
    let has_ends = cols
        .iter()
        .any(|c| matches!(c, Column::StartId { .. } | Column::EndId { .. }));
    match (has_id, has_ends) {
        (true, true) => Err(CsvImportError::MixedHeader),
        (true, false) => Ok(Shape::Node),
        (false, true) => Ok(Shape::Rel),
        (false, false) => Err(CsvImportError::MissingId),
    }
}

/// Mint an absolute IRI for an id value, namespaced by its id space.
fn mint_iri(base: &str, space: Option<&str>, value: &str) -> String {
    let value = iri_segment(value);
    match space {
        Some(s) if !s.is_empty() => format!("{base}{s}/{value}"),
        _ => format!("{base}{value}"),
    }
}

/// Stream one CSV file (a reader), invoking `sink` once per produced JSON-LD
/// object — without materializing the whole file in memory. The header
/// determines whether the rows are nodes or relationships. This is the core
/// shared by [`csv_to_jsonld`] (collects) and [`write_csv_ndjson`] (streams to
/// a writer).
fn for_each_object<R: Read>(
    csv: R,
    opts: &CsvImportOptions,
    mut sink: impl FnMut(Value) -> Result<()>,
) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(opts.delimiter)
        .has_headers(false)
        .flexible(true)
        .from_reader(csv);

    let mut records = rdr.records();
    let header_rec = match records.next() {
        Some(r) => r?,
        None => return Err(CsvImportError::EmptyHeader),
    };
    let cols: Vec<Column> = header_rec
        .iter()
        .map(parse_header_column)
        .collect::<Result<_>>()?;
    if cols.is_empty() {
        return Err(CsvImportError::EmptyHeader);
    }
    let shape = classify(&cols)?;

    let pred = |name: &str| format!("{}{}", opts.base_iri, name);

    for (i, rec) in records.enumerate() {
        let rec = rec?;
        let row = i + 2; // 1-based, header is row 1
        if rec.len() != cols.len() {
            return Err(CsvImportError::RowWidth {
                row,
                got: rec.len(),
                want: cols.len(),
            });
        }

        match shape {
            Shape::Node => sink(node_object(&cols, &rec, row, opts, &pred)?)?,
            Shape::Rel => {
                if let Some(obj) = rel_object(&cols, &rec, row, opts, &pred)? {
                    sink(obj)?;
                }
            }
        }
    }
    Ok(())
}

/// Convert the text of one CSV file into a list of JSON-LD objects. Collects in
/// memory — for large datasets prefer [`write_csv_ndjson`] + the chunked bulk
/// import.
pub fn csv_to_jsonld(csv_text: &str, opts: &CsvImportOptions) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    for_each_object(csv_text.as_bytes(), opts, |obj| {
        out.push(obj);
        Ok(())
    })?;
    Ok(out)
}

/// Stream a CSV file (a reader) as **newline-delimited JSON-LD** (one object per
/// line) to `out`, returning the number of objects written. Each object uses
/// absolute IRIs, so no `@context` line is needed. This is the scalable path:
/// the output `.jsonl` feeds the chunked, parallel bulk-import pipeline instead
/// of one giant in-memory document / transaction.
pub fn write_csv_ndjson<R: Read, W: Write>(
    csv: R,
    opts: &CsvImportOptions,
    out: &mut W,
) -> Result<usize> {
    let mut count = 0usize;
    for_each_object(csv, opts, |obj| {
        serde_json::to_writer(&mut *out, &obj)
            .map_err(|e| CsvImportError::Io(std::io::Error::other(e)))?;
        out.write_all(b"\n")?;
        count += 1;
        Ok(())
    })?;
    Ok(count)
}

/// Convert several CSV files (node and relationship files, any order) into a
/// single JSON-LD `{"@graph": [...]}` document. Collects in memory — used by
/// tests and small inserts; bulk imports use [`write_csv_ndjson`].
pub fn csv_files_to_jsonld(files: &[&str], opts: &CsvImportOptions) -> Result<Value> {
    let mut graph = Vec::new();
    for text in files {
        graph.extend(csv_to_jsonld(text, opts)?);
    }
    let mut doc = Map::new();
    doc.insert("@graph".to_string(), Value::Array(graph));
    Ok(Value::Object(doc))
}

/// Build a JSON-LD node object from one node row.
fn node_object(
    cols: &[Column],
    rec: &csv::StringRecord,
    row: usize,
    opts: &CsvImportOptions,
    pred: &impl Fn(&str) -> String,
) -> Result<Value> {
    let mut obj = Map::new();
    let mut types: Vec<Value> = Vec::new();

    for (idx, col) in cols.iter().enumerate() {
        let raw = rec.get(idx).unwrap_or("");
        match col {
            Column::Id { prop, space } => {
                obj.insert(
                    "@id".to_string(),
                    Value::String(mint_iri(&opts.base_iri, space.as_deref(), raw)),
                );
                // A named id column keeps the raw value as a property too — its
                // type is the value as written (numeric ids → integers).
                if let Some(name) = prop {
                    obj.insert(pred(name), id_property_value(raw));
                }
            }
            Column::Label => {
                for label in raw.split(opts.array_delimiter).filter(|s| !s.is_empty()) {
                    types.push(Value::String(pred(&iri_segment(label.trim()))));
                }
            }
            Column::Property { name, ty, array } => {
                if raw.is_empty() {
                    continue; // an empty cell is an absent property
                }
                let value = property_value(*ty, *array, raw, row, name, opts)?;
                obj.insert(pred(name), value);
            }
            Column::Ignore => {}
            // A node header is classified before we get here, so endpoint /
            // type columns cannot appear; ignore defensively.
            Column::StartId { .. } | Column::EndId { .. } | Column::Type => {}
        }
    }
    if !types.is_empty() {
        obj.insert(
            "@type".to_string(),
            if types.len() == 1 {
                types.pop().unwrap()
            } else {
                Value::Array(types)
            },
        );
    }
    Ok(Value::Object(obj))
}

/// Build a JSON-LD `{subject → {object [+ @annotation]}}` object from one
/// relationship row. Returns `None` only if a required endpoint is empty.
fn rel_object(
    cols: &[Column],
    rec: &csv::StringRecord,
    row: usize,
    opts: &CsvImportOptions,
    pred: &impl Fn(&str) -> String,
) -> Result<Option<Value>> {
    let mut start: Option<(Option<String>, String)> = None;
    let mut end: Option<(Option<String>, String)> = None;
    let mut rel_type: Option<String> = None;
    let mut props = Map::new();

    for (idx, col) in cols.iter().enumerate() {
        let raw = rec.get(idx).unwrap_or("");
        match col {
            Column::StartId { space } => start = Some((space.clone(), raw.to_string())),
            Column::EndId { space } => end = Some((space.clone(), raw.to_string())),
            Column::Type => {
                if !raw.is_empty() {
                    rel_type = Some(raw.to_string());
                }
            }
            Column::Property { name, ty, array } => {
                if raw.is_empty() {
                    continue;
                }
                props.insert(
                    pred(name),
                    property_value(*ty, *array, raw, row, name, opts)?,
                );
            }
            Column::Ignore | Column::Id { .. } | Column::Label => {}
        }
    }

    let (Some((s_space, s_val)), Some((e_space, e_val))) = (start, end) else {
        return Ok(None);
    };
    if s_val.is_empty() || e_val.is_empty() {
        return Ok(None);
    }
    let rel_type = rel_type.ok_or(CsvImportError::MissingType)?;

    let s_iri = mint_iri(&opts.base_iri, s_space.as_deref(), &s_val);
    let o_iri = mint_iri(&opts.base_iri, e_space.as_deref(), &e_val);

    // Nary encoding is deferred (its intermediate-node predicate convention is
    // not fixed); error rather than silently mis-model property-bearing edges.
    if !props.is_empty() && opts.edge_policy == EdgePolicy::Nary {
        return Err(CsvImportError::NaryDeferred {
            rel_type: rel_type.clone(),
        });
    }

    let mut object = Map::new();
    object.insert("@id".to_string(), Value::String(o_iri));
    // A property-bearing edge reifies under `Annotated`; under `Plain` the
    // properties are dropped and the edge stays a plain triple.
    if !props.is_empty() && opts.edge_policy == EdgePolicy::Annotated {
        object.insert("@annotation".to_string(), Value::Object(props));
    }

    let mut subj = Map::new();
    subj.insert("@id".to_string(), Value::String(s_iri));
    subj.insert(pred(&iri_segment(&rel_type)), Value::Object(object));
    Ok(Some(Value::Object(subj)))
}

/// Resolve a property cell to a JSON-LD value, honoring `[]` array columns.
fn property_value(
    ty: CsvType,
    array: bool,
    raw: &str,
    row: usize,
    name: &str,
    opts: &CsvImportOptions,
) -> Result<Value> {
    if array {
        let items = raw
            .split(opts.array_delimiter)
            .filter(|s| !s.is_empty())
            .map(|item| ty.coerce(item, row, name))
            .collect::<Result<Vec<_>>>()?;
        Ok(Value::Array(items))
    } else {
        ty.coerce(raw, row, name)
    }
}

/// Value for a `name:ID` column kept as a property — numeric strings become
/// JSON numbers (so `person.id` is queryable as an integer), else a string.
fn id_property_value(raw: &str) -> Value {
    if let Ok(n) = raw.parse::<i64>() {
        Value::Number(n.into())
    } else {
        Value::String(raw.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn opts() -> CsvImportOptions {
        CsvImportOptions {
            base_iri: "http://ex/".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn node_row_to_jsonld_with_typed_props_and_labels() {
        let csv = "id:ID(Person),name:string,age:int,joined:date,:LABEL\n\
                   10,Alice,30,2020-01-02,Person;Admin\n";
        let out = csv_to_jsonld(csv, &opts()).unwrap();
        assert_eq!(
            out,
            vec![json!({
                "@id": "http://ex/Person/10",
                "http://ex/id": 10,
                "http://ex/name": "Alice",
                "http://ex/age": 30,
                "http://ex/joined": {"@value": "2020-01-02", "@type": "http://www.w3.org/2001/XMLSchema#date"},
                "@type": ["http://ex/Person", "http://ex/Admin"]
            })]
        );
    }

    #[test]
    fn plain_edge_row_to_jsonld() {
        let csv = ":START_ID(Person),:END_ID(Person),:TYPE\n10,20,KNOWS\n";
        let out = csv_to_jsonld(csv, &opts()).unwrap();
        assert_eq!(
            out,
            vec![json!({
                "@id": "http://ex/Person/10",
                "http://ex/KNOWS": {"@id": "http://ex/Person/20"}
            })]
        );
    }

    #[test]
    fn property_edge_reifies_under_default_policy() {
        let csv =
            ":START_ID(Person),:END_ID(Person),:TYPE,creationDate:long\n10,20,KNOWS,1577934245\n";
        let out = csv_to_jsonld(csv, &opts()).unwrap();
        assert_eq!(
            out,
            vec![json!({
                "@id": "http://ex/Person/10",
                "http://ex/KNOWS": {
                    "@id": "http://ex/Person/20",
                    "@annotation": {"http://ex/creationDate": 1_577_934_245}
                }
            })]
        );
    }

    #[test]
    fn property_edge_drops_props_under_plain_policy() {
        let csv =
            ":START_ID(Person),:END_ID(Person),:TYPE,creationDate:long\n10,20,KNOWS,1577934245\n";
        let out = csv_to_jsonld(
            csv,
            &CsvImportOptions {
                edge_policy: EdgePolicy::Plain,
                ..opts()
            },
        )
        .unwrap();
        assert_eq!(
            out,
            vec![json!({
                "@id": "http://ex/Person/10",
                "http://ex/KNOWS": {"@id": "http://ex/Person/20"}
            })]
        );
    }

    #[test]
    fn property_edge_under_nary_policy_is_deferred() {
        let csv =
            ":START_ID(Person),:END_ID(Person),:TYPE,creationDate:long\n10,20,KNOWS,1577934245\n";
        let err = csv_to_jsonld(
            csv,
            &CsvImportOptions {
                edge_policy: EdgePolicy::Nary,
                ..opts()
            },
        )
        .unwrap_err();
        assert!(matches!(err, CsvImportError::NaryDeferred { .. }), "{err}");
        // A property-LESS edge is a plain triple under any policy — no n-ary node.
        let plainish = csv_to_jsonld(
            ":START_ID(Person),:END_ID(Person),:TYPE\n10,20,KNOWS\n",
            &CsvImportOptions {
                edge_policy: EdgePolicy::Nary,
                ..opts()
            },
        )
        .unwrap();
        assert_eq!(
            plainish,
            vec![
                json!({"@id": "http://ex/Person/10", "http://ex/KNOWS": {"@id": "http://ex/Person/20"}})
            ]
        );
    }

    #[test]
    fn array_property_splits_on_delimiter() {
        let csv = "id:ID,email:string[]\n5,a@x;b@y\n";
        let out = csv_to_jsonld(csv, &opts()).unwrap();
        assert_eq!(out[0]["http://ex/email"], json!(["a@x", "b@y"]));
        // No id space → flat IRI.
        assert_eq!(out[0]["@id"], json!("http://ex/5"));
    }

    #[test]
    fn header_classification_and_errors() {
        assert!(matches!(
            csv_to_jsonld(":ID,:START_ID\n", &opts()),
            Err(CsvImportError::MixedHeader)
        ));
        assert!(matches!(
            csv_to_jsonld("name:string\nx\n", &opts()),
            Err(CsvImportError::MissingId)
        ));
        assert!(matches!(
            csv_to_jsonld(":START_ID,:END_ID\n1,2\n", &opts()),
            Err(CsvImportError::MissingType)
        ));
    }

    #[test]
    fn empty_property_cell_is_absent() {
        let csv = "id:ID,name:string,nick:string\n1,Bob,\n";
        let out = csv_to_jsonld(csv, &opts()).unwrap();
        assert!(out[0].get("http://ex/nick").is_none(), "{out:?}");
        assert_eq!(out[0]["http://ex/name"], json!("Bob"));
    }

    #[test]
    fn bad_typed_value_errors() {
        let csv = "id:ID,age:int\n1,notanumber\n";
        assert!(matches!(
            csv_to_jsonld(csv, &opts()),
            Err(CsvImportError::BadValue {
                kind: "integer",
                ..
            })
        ));
    }

    #[test]
    fn bad_date_value_errors() {
        let csv = "id:ID,joined:date\n1,not-a-date\n";
        assert!(matches!(
            csv_to_jsonld(csv, &opts()),
            Err(CsvImportError::BadValue { kind: "date", .. })
        ));
    }

    #[test]
    fn bad_datetime_value_errors() {
        let csv = "id:ID,seen:datetime\n1,2020-13-99\n";
        assert!(matches!(
            csv_to_jsonld(csv, &opts()),
            Err(CsvImportError::BadValue {
                kind: "dateTime",
                ..
            })
        ));
    }

    #[test]
    fn id_value_is_percent_encoded_into_namespace() {
        // A value that would otherwise climb out of the namespace (`/`, an
        // embedded URL) stays a single segment under base_iri.
        let csv = "id:ID\nhttp://evil/p\n";
        let out = csv_to_jsonld(csv, &opts()).unwrap();
        assert_eq!(out[0]["@id"], json!("http://ex/http:%2F%2Fevil%2Fp"));
    }

    #[test]
    fn rel_type_with_unsafe_chars_is_encoded() {
        let csv = ":START_ID(Person),:END_ID(Person),:TYPE\n10,20,a b/c\n";
        let out = csv_to_jsonld(csv, &opts()).unwrap();
        // Predicate stays inside base_iri; space and slash are encoded.
        assert!(out[0].get("http://ex/a%20b%2Fc").is_some(), "{out:?}");
    }

    #[test]
    fn well_formed_datetime_is_accepted() {
        let csv = "id:ID,seen:datetime\n1,2020-01-02T03:04:05Z\n";
        let out = csv_to_jsonld(csv, &opts()).unwrap();
        assert_eq!(
            out[0]["http://ex/seen"],
            json!({"@value": "2020-01-02T03:04:05Z", "@type": "http://www.w3.org/2001/XMLSchema#dateTime"})
        );
    }
}
