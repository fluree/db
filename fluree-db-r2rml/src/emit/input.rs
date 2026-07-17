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

use crate::emit::SubjectStrategy;

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

    /// Whether this column is strictly integer-typed (`Int32`/`Int64`).
    pub fn is_integer(&self) -> bool {
        matches!(self.field_type, FieldType::Int32 | FieldType::Int64)
    }

    /// Whether this column's TYPE is key-like — an integer OR a scale-0 decimal.
    ///
    /// This is the FK-candidacy type gate. Snowflake `NUMBER(38,0)` surrogate
    /// keys arrive as Iceberg `decimal(38,0)` (scale 0), NOT `long`; gating FK
    /// inference on [`Self::is_integer`] alone silently drops every such key
    /// before the name match. A scale-0 decimal is an exact integer, so it is a
    /// valid key both as an FK child and as a parent PK. A scaled decimal
    /// (`scale > 0`, e.g. money) is NOT key-like.
    pub fn is_key_type(&self) -> bool {
        matches!(
            self.field_type,
            FieldType::Int32 | FieldType::Int64 | FieldType::Decimal { scale: 0, .. }
        )
    }

    /// Whether the column name looks like a key by convention (`*_KEY` / `*_ID`).
    pub fn is_key_like(&self) -> bool {
        self.name.ends_with("_KEY") || self.name.ends_with("_ID")
    }
}

/// A table's `{namespace, name}` identity — the key for [`crate::emit::EmitOptions::per_table_overrides`].
///
/// Mirrors PR-1's `TableIdentifier`: the same identity the input [`EmitTableSchema`]
/// carries, so a `HashMap<TableKey, TableOverride>` keys on exactly what the
/// emitter iterates. Case-sensitive and byte-for-byte (Snowflake folds UPPERCASE).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableKey {
    /// Catalog namespace (e.g. `"DW"`).
    pub namespace: String,
    /// Table name within the namespace (e.g. `"DIM_STORE"`).
    pub name: String,
}

impl TableKey {
    /// Build a table key from its namespace and name parts.
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

/// A per-table override for the subject key and/or class name (PR-1's `TableOverride`).
///
/// Every field is optional and additive: an all-`None` override (or no entry at
/// all) leaves the deterministic stem-derived defaults untouched.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableOverride {
    /// REPLACES `identifier_field_ids` as the subject key K — a list of one or
    /// more columns. A single column yields a simple key (indexed as an FK
    /// parent); multiple columns yield a composite subject template (joined by
    /// `/`, never indexed as a single-column PK — composite rendering already
    /// works, so the only prior blocker was this being a single `Option<String>`).
    /// Every named column must exist and (under [`SubjectStrategy::Identifier`])
    /// pass the `required` / `null_fraction == 0` gate (else `NoSafeSubjectKey`);
    /// because uniqueness is unprovable metadata-only, an override key ALWAYS
    /// attaches a `SubjectKeyUnverified` diagnostic.
    pub primary_key: Option<Vec<String>>,
    /// Overrides the stem-derived class name (`rr:class` local name) and the
    /// subject-template `classSlug` for the table.
    pub class_name: Option<String>,
    /// Per-table override of the global [`crate::emit::EmitOptions::subject_strategy`].
    /// `None` inherits the global strategy; `Some(..)` forces this table to
    /// `Auto` (always emit) or `Identifier` (strict) regardless of the global.
    pub subject_strategy: Option<SubjectStrategy>,
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

    /// This table's `{namespace, name}` identity — its per-table-override key.
    pub fn key(&self) -> TableKey {
        TableKey {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
        }
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
