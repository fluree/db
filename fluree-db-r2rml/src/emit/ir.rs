//! The authoritative wire IR: solo's `StructuredR2rmlMapping` (camelCase).
//!
//! This is the object PR-2 persists verbatim, PR-3 renders in the review step,
//! and PR-4 IRI-rewrites. It is the emitter's authoritative artifact — the
//! Turtle is rendered FROM this IR, and the `fluree-db-r2rml` compiled
//! `Vec<TriplesMap>` is produced only for the internal round-trip check and is
//! never serialized onto the wire.
//!
//! Field names are Rust snake_case; `#[serde(rename_all = "camelCase")]` makes
//! the JSON exactly `{ baseNamespace, prefixes[], tableMappings[]{ tableName,
//! classIri, subjectTemplate, columns[]{ columnName, predicateIri, datatype,
//! isSubjectId, foreignKey{ targetTable, childColumn, parentColumn }, isIri,
//! iriTemplate } } }`.

use serde::{Deserialize, Serialize};

/// A prefix declaration carried on the wire (for renderers and reviewers).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrefixDecl {
    /// The prefix label (e.g. `"rr"`, `"xsd"`, or the vocab prefix).
    pub prefix: String,
    /// The namespace IRI the label expands to.
    pub namespace: String,
}

/// A foreign-key edge: the child column joins to the parent table's key column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForeignKey {
    /// The parent (referenced) table, byte-for-byte (`"DW.DIM_GEOGRAPHY"`).
    pub target_table: String,
    /// The child (referencing) column in this table.
    pub child_column: String,
    /// The parent key column in the target table.
    pub parent_column: String,
}

/// A single predicate-object mapping for a table, keyed on a source column.
///
/// A resolved FK column contributes TWO `ColumnMapping`s: one literal
/// (`datatype` set, `foreign_key` `None`) for pushdown, and one join
/// (`foreign_key` set, `datatype` `None`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMapping {
    /// The source column name, byte-for-byte.
    pub column_name: String,
    /// The predicate IRI this mapping generates.
    pub predicate_iri: String,
    /// The XSD datatype (a CURIE such as `"xsd:integer"`) for literal objects;
    /// `None` for strings (plain literal) and for FK/join objects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datatype: Option<String>,
    /// Whether this column is (part of) the subject key.
    pub is_subject_id: bool,
    /// The foreign-key edge, when this mapping is a resolved join.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foreign_key: Option<ForeignKey>,
    /// Whether the object is an IRI produced from `iri_template` (unused in Phase-1).
    pub is_iri: bool,
    /// A template producing an IRI object (unused in Phase-1; FKs use `foreign_key`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iri_template: Option<String>,
}

impl ColumnMapping {
    /// A literal (typed or plain) predicate-object mapping.
    pub fn literal(
        column_name: impl Into<String>,
        predicate_iri: impl Into<String>,
        datatype: Option<String>,
        is_subject_id: bool,
    ) -> Self {
        Self {
            column_name: column_name.into(),
            predicate_iri: predicate_iri.into(),
            datatype,
            is_subject_id,
            foreign_key: None,
            is_iri: false,
            iri_template: None,
        }
    }

    /// A join (RefObjectMap) predicate-object mapping.
    pub fn join(
        column_name: impl Into<String>,
        predicate_iri: impl Into<String>,
        foreign_key: ForeignKey,
    ) -> Self {
        Self {
            column_name: column_name.into(),
            predicate_iri: predicate_iri.into(),
            datatype: None,
            is_subject_id: false,
            foreign_key: Some(foreign_key),
            is_iri: false,
            iri_template: None,
        }
    }
}

/// One TriplesMap's worth of mapping — a base table, its class, subject
/// template, and column mappings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMapping {
    /// The logical table name, byte-for-byte (`"DW.DIM_STORE"`).
    pub table_name: String,
    /// The `rr:class` IRI for generated subjects.
    pub class_iri: String,
    /// The `rr:subjectMap` template (`"{subjectBase}{slug}/{KEY}"`).
    ///
    /// Empty when no safe subject key exists (a `NoSafeSubjectKey` diagnostic is
    /// emitted and the table produces no subject).
    pub subject_template: String,
    /// Predicate-object mappings (literals first in `field_id` order, then joins).
    pub columns: Vec<ColumnMapping>,
}

/// The authoritative structured mapping — solo's `StructuredR2rmlMapping`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructuredR2rmlMapping {
    /// The single base namespace all IRIs derive from.
    pub base_namespace: String,
    /// Prefix declarations (`rr`, `xsd`, and the vocab prefix).
    pub prefixes: Vec<PrefixDecl>,
    /// One entry per requested table, in request order.
    pub table_mappings: Vec<TableMapping>,
}

impl StructuredR2rmlMapping {
    /// Look up a table mapping by its logical table name.
    pub fn table_mapping(&self, table_name: &str) -> Option<&TableMapping> {
        self.table_mappings
            .iter()
            .find(|t| t.table_name == table_name)
    }

    /// The vocab prefix label (the prefix whose namespace equals `base_namespace`).
    ///
    /// Falls back to `"ns"` if — impossibly — no such prefix was declared.
    pub fn vocab_prefix(&self) -> &str {
        self.prefixes
            .iter()
            .find(|p| p.namespace == self.base_namespace)
            .map_or("ns", |p| p.prefix.as_str())
    }
}
