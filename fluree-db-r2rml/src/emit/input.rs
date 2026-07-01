//! Self-contained INPUT model for the deterministic Iceberg-metadata → R2RML
//! emitter.
//!
//! This is a minimal stand-in for PR-1's `TableSchema` / `ColumnInfo` /
//! `ColumnStats` (parts a/b, owned by other tracks). It carries exactly the
//! schema + metadata-stats signals the deterministic heuristic needs:
//!
//! - the fully-qualified table identifier (`namespace` + `name`),
//! - per-column type / nullability / nesting,
//! - Iceberg's `identifier_field_ids` PK hint, and
//! - the Tier-B per-column stats (`null_fraction`, typed `min`/`max`) used for
//!   range-containment FK confirmation.
//!
//! TODO(track-a): map the real `preview_iceberg_table` Tier-A schema + Tier-B
//! stats onto this model so the emitter consumes the live preview types instead
//! of this stand-in. The [`FieldType`] here is already the shared
//! `fluree-db-tabular` enum, so the type axis will unify for free.

use fluree_db_tabular::FieldType;

/// A comparable typed bound for range-containment FK confirmation.
///
/// Integers suffice for the surrogate `*_KEY` spaces exercised by this spike;
/// the enum is kept open so temporal / decimal bounds can be added later
/// without touching call sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TypedBound {
    /// An integer bound (surrogate keys, counts, ...).
    Int(i64),
}

impl TypedBound {
    /// Return the integer payload, if this bound is integer-typed.
    pub fn as_int(self) -> Option<i64> {
        match self {
            TypedBound::Int(v) => Some(v),
        }
    }
}

/// Tier-B per-column statistics (metadata-only; no Parquet scan).
///
/// `distinct_count` is intentionally absent — NDV is unavailable metadata-only
/// (Puffin/theta-sketch reader is deferred to PR-5), which is exactly why the
/// subject-key uniqueness check stays a heuristic (`SubjectKeyUnverified`).
#[derive(Debug, Clone, Default)]
pub struct EmitColumnStats {
    /// Fraction of NULL values in `[0.0, 1.0]`, if known.
    pub null_fraction: Option<f64>,
    /// Typed minimum bound (value_codec-decoded upstream), if known.
    pub min: Option<TypedBound>,
    /// Typed maximum bound (value_codec-decoded upstream), if known.
    pub max: Option<TypedBound>,
}

/// A single column of a table (Tier-A schema + Tier-B stats).
#[derive(Debug, Clone)]
pub struct EmitColumn {
    /// Iceberg field id — the canonical identifier and the emission order key.
    pub field_id: i32,
    /// Byte-for-byte Iceberg field name (Snowflake folds to UPPERCASE).
    pub name: String,
    /// Raw Iceberg type string (`"long"`, `"decimal(18,2)"`, ...) — informational.
    pub iceberg_type: String,
    /// Parsed field type (shared `fluree-db-tabular` enum).
    pub field_type: FieldType,
    /// Whether the column is declared `required` (NOT NULL) in the schema.
    pub required: bool,
    /// Whether the column is a nested struct/list/map (skipped by R2RML).
    pub nested: bool,
    /// Optional column documentation.
    pub doc: Option<String>,
    /// Tier-B statistics for this column.
    pub stats: EmitColumnStats,
}

impl EmitColumn {
    /// A column is a NOT-NULL signal if declared `required` or proven null-free
    /// by stats (`null_fraction == 0`).
    pub fn is_non_null(&self) -> bool {
        self.required || self.stats.null_fraction == Some(0.0)
    }

    /// Whether this column is integer-typed (FK candidacy is integer-only).
    pub fn is_integer(&self) -> bool {
        matches!(self.field_type, FieldType::Int32 | FieldType::Int64)
    }

    /// Whether the column name looks like a key by convention (`*_KEY` / `*_ID`).
    pub fn is_key_like(&self) -> bool {
        self.name.ends_with("_KEY") || self.name.ends_with("_ID")
    }
}

/// A table's schema + PK hint + per-column stats — one emitter input unit.
#[derive(Debug, Clone)]
pub struct EmitTableSchema {
    /// Catalog namespace (e.g. `"DW"`).
    pub namespace: String,
    /// Table name within the namespace (e.g. `"DIM_STORE"`).
    pub name: String,
    /// Columns in `field_id` order (the deterministic emission order).
    pub columns: Vec<EmitColumn>,
    /// Iceberg's declared row-identity hint — the primary PK signal.
    pub identifier_field_ids: Vec<i32>,
}

impl EmitTableSchema {
    /// Fully-qualified logical table name, byte-for-byte (`"DW.DIM_STORE"`).
    ///
    /// The namespace prefix is included deliberately — it is what the scan path
    /// matches against, and `LogicalTable::normalize_table_name` leaves an
    /// already-dotted identifier untouched.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }

    /// The table stem (name without namespace), e.g. `"DIM_STORE"`.
    pub fn stem(&self) -> &str {
        &self.name
    }

    /// Whether this is a fact table (`FACT_*`) — used for the child-fact→hub
    /// join advisory.
    pub fn is_fact(&self) -> bool {
        self.name.starts_with("FACT_")
    }

    /// Look up a column by field id.
    pub fn column_by_field_id(&self, field_id: i32) -> Option<&EmitColumn> {
        self.columns.iter().find(|c| c.field_id == field_id)
    }
}
