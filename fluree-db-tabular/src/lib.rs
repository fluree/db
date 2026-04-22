//! Tabular column batch types for Fluree DB.
//!
//! This crate provides format-agnostic columnar batch types used by graph source
//! backends (Iceberg, SQL, etc.) and the R2RML materialization layer.
//!
//! # Design
//!
//! - **Columnar storage**: Data is stored in typed `Vec` per column, not per-row
//! - **Strongly typed**: All column access is through the `Column` enum, no `dyn Any`
//! - **Field ID canonical**: Field IDs are the canonical identifier for columns
//! - **Lambda-friendly**: No Arrow dependency (small binary size)

pub mod batch;
pub mod error;

pub use batch::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
pub use error::{Result, TabularError};
