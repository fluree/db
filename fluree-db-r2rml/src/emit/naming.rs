//! Pure, deterministic naming derivations: identifier case conversions, the
//! `FieldType` → `xsd:` datatype map, and the single-`base_namespace` → two-base
//! IRI derivation. Every function here is total and side-effect-free so that
//! identical input yields byte-identical output.

use fluree_db_tabular::FieldType;

/// Split a Snowflake `UPPER_SNAKE` identifier into its `_`-separated words,
/// dropping empty segments.
fn words(ident: &str) -> Vec<&str> {
    ident.split('_').filter(|w| !w.is_empty()).collect()
}

/// Capitalize the first character and lowercase the rest of an ASCII word.
fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => {
            let mut out = first.to_ascii_uppercase().to_string();
            out.push_str(&chars.as_str().to_ascii_lowercase());
            out
        }
    }
}

/// `DIM_ORDER_LINE` → `DimOrderLine` (PascalCase over `_`-separated words).
pub fn pascal_case(ident: &str) -> String {
    words(ident).into_iter().map(capitalize).collect()
}

/// `GEOGRAPHY_KEY` → `geographyKey` (camelCase: first word lowercased).
pub fn camel_case(ident: &str) -> String {
    let mut out = String::new();
    for (i, word) in words(ident).into_iter().enumerate() {
        if i == 0 {
            out.push_str(&word.to_ascii_lowercase());
        } else {
            out.push_str(&capitalize(word));
        }
    }
    out
}

/// `ORDER_LINE` → `order-line` (kebab-case, lowercased).
pub fn kebab_case(ident: &str) -> String {
    words(ident)
        .into_iter()
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>()
        .join("-")
}

/// Strip a leading `DIM_` / `FACT_` dimension/fact marker from a table stem.
///
/// `DIM_GEOGRAPHY` → `GEOGRAPHY`, `FACT_ORDER_LINE` → `ORDER_LINE`, and any stem
/// without a recognized marker is returned unchanged.
pub fn strip_table_marker(stem: &str) -> &str {
    stem.strip_prefix("DIM_")
        .or_else(|| stem.strip_prefix("FACT_"))
        .unwrap_or(stem)
}

/// The TriplesMap node local name for a table stem (`DIM_DATE` → `DimDate`).
pub fn triples_map_node(stem: &str) -> String {
    pascal_case(stem)
}

/// The class local name for a table stem (`DIM_GEOGRAPHY` → `Geography`,
/// `FACT_ORDER` → `Order`).
pub fn class_local_name(stem: &str) -> String {
    pascal_case(strip_table_marker(stem))
}

/// The subject-template slug for a table stem (`DIM_GEOGRAPHY` → `geography`,
/// `FACT_ORDER_LINE` → `order-line`).
pub fn class_slug(stem: &str) -> String {
    kebab_case(strip_table_marker(stem))
}

/// Strip a trailing `_KEY` / `_ID` from a key column name, for readable
/// relationship predicate derivation (`DEST_GEOGRAPHY_KEY` → `DEST_GEOGRAPHY`).
/// A bare `KEY` / `ID` (nothing left after stripping) is returned unchanged.
pub fn strip_key_suffix(column: &str) -> &str {
    for suffix in ["_KEY", "_ID"] {
        if let Some(stem) = column.strip_suffix(suffix) {
            if !stem.is_empty() {
                return stem;
            }
        }
    }
    column
}

/// The `xsd:` datatype CURIE for a `FieldType`, or `None` for strings (plain
/// literal) — matching `enterprise.ttl` and the lexical output of
/// `materialize/term.rs`.
///
/// `xsd_long_as_integer` (default `true`) picks `xsd:integer` for `Int32`/`Int64`
/// to match the reference; `false` uses `xsd:int` / `xsd:long`. Both share the
/// decimal-string lexical space, so either round-trips.
///
/// `Bytes` maps to **`xsd:hexBinary`** — `term.rs`'s `base64_encode` is a
/// misnomer that emits lowercase hex, so hex is the datatype whose lexical space
/// matches the materializer (guarded by a regression test).
pub fn xsd_datatype(field_type: FieldType, xsd_long_as_integer: bool) -> Option<&'static str> {
    let curie = match field_type {
        FieldType::Boolean => "xsd:boolean",
        FieldType::Int32 => {
            if xsd_long_as_integer {
                "xsd:integer"
            } else {
                "xsd:int"
            }
        }
        FieldType::Int64 => {
            if xsd_long_as_integer {
                "xsd:integer"
            } else {
                "xsd:long"
            }
        }
        FieldType::Float32 => "xsd:float",
        FieldType::Float64 => "xsd:double",
        FieldType::Decimal { .. } => "xsd:decimal",
        FieldType::Date => "xsd:date",
        FieldType::Timestamp | FieldType::TimestampTz => "xsd:dateTime",
        FieldType::Bytes => "xsd:hexBinary",
        FieldType::String => return None,
    };
    Some(curie)
}

/// Derive the subject-IRI base from the single `base_namespace`.
///
/// A trailing `#` is normalized to `/`; otherwise `/` is appended unless the
/// base already ends in `/`. `http://x/edw#` → `http://x/edw/`.
pub fn subject_base(base_namespace: &str) -> String {
    if let Some(stripped) = base_namespace.strip_suffix('#') {
        format!("{stripped}/")
    } else if base_namespace.ends_with('/') {
        base_namespace.to_string()
    } else {
        format!("{base_namespace}/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_conversions() {
        assert_eq!(pascal_case("DIM_DATE"), "DimDate");
        assert_eq!(pascal_case("FACT_ORDER_LINE"), "FactOrderLine");
        assert_eq!(pascal_case("GL_JOURNAL"), "GlJournal");
        assert_eq!(camel_case("GEOGRAPHY_KEY"), "geographyKey");
        assert_eq!(camel_case("DAY_OF_WEEK"), "dayOfWeek");
        assert_eq!(camel_case("IS_WEEKEND"), "isWeekend");
        assert_eq!(camel_case("DATE"), "date");
        assert_eq!(kebab_case("ORDER_LINE"), "order-line");
        assert_eq!(kebab_case("INVENTORY_SNAPSHOT"), "inventory-snapshot");
    }

    #[test]
    fn table_stem_derivations() {
        assert_eq!(strip_table_marker("DIM_GEOGRAPHY"), "GEOGRAPHY");
        assert_eq!(strip_table_marker("FACT_ORDER_LINE"), "ORDER_LINE");
        assert_eq!(strip_table_marker("WEATHER"), "WEATHER");
        assert_eq!(triples_map_node("DIM_DATE"), "DimDate");
        assert_eq!(triples_map_node("FACT_ORDER_LINE"), "FactOrderLine");
        assert_eq!(class_local_name("DIM_GEOGRAPHY"), "Geography");
        assert_eq!(class_local_name("FACT_ORDER"), "Order");
        assert_eq!(class_slug("DIM_DATE"), "date");
        assert_eq!(class_slug("FACT_ORDER_LINE"), "order-line");
    }

    #[test]
    fn key_suffix_stripping() {
        assert_eq!(strip_key_suffix("DEST_GEOGRAPHY_KEY"), "DEST_GEOGRAPHY");
        assert_eq!(strip_key_suffix("GEOGRAPHY_KEY"), "GEOGRAPHY");
        assert_eq!(strip_key_suffix("SESSION_ID"), "SESSION");
        assert_eq!(strip_key_suffix("KEY"), "KEY");
    }

    #[test]
    fn datatype_map_pins_hexbinary_and_datetime() {
        // The load-bearing cases: bytes are hex (not base64), both timestamp
        // flavors are dateTime, strings are untyped.
        assert_eq!(xsd_datatype(FieldType::Bytes, true), Some("xsd:hexBinary"));
        assert_eq!(
            xsd_datatype(FieldType::Timestamp, true),
            Some("xsd:dateTime")
        );
        assert_eq!(
            xsd_datatype(FieldType::TimestampTz, true),
            Some("xsd:dateTime")
        );
        assert_eq!(xsd_datatype(FieldType::String, true), None);
        assert_eq!(xsd_datatype(FieldType::Boolean, true), Some("xsd:boolean"));
        assert_eq!(xsd_datatype(FieldType::Float64, true), Some("xsd:double"));
        assert_eq!(xsd_datatype(FieldType::Float32, true), Some("xsd:float"));
        assert_eq!(
            xsd_datatype(
                FieldType::Decimal {
                    precision: 18,
                    scale: 2
                },
                true
            ),
            Some("xsd:decimal")
        );
        assert_eq!(xsd_datatype(FieldType::Date, true), Some("xsd:date"));
    }

    #[test]
    fn long_as_integer_knob() {
        assert_eq!(xsd_datatype(FieldType::Int64, true), Some("xsd:integer"));
        assert_eq!(xsd_datatype(FieldType::Int32, true), Some("xsd:integer"));
        assert_eq!(xsd_datatype(FieldType::Int64, false), Some("xsd:long"));
        assert_eq!(xsd_datatype(FieldType::Int32, false), Some("xsd:int"));
    }

    #[test]
    fn subject_base_derivation() {
        assert_eq!(subject_base("http://x/edw#"), "http://x/edw/");
        assert_eq!(subject_base("http://x/edw/"), "http://x/edw/");
        assert_eq!(subject_base("http://x/edw"), "http://x/edw/");
    }
}
