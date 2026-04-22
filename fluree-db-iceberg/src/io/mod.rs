//! IO module for Iceberg data file reading.
//!
//! This module provides:
//! - [`batch`] - Columnar batch format for efficient data access
//! - [`storage`] - Storage abstraction for S3 and other backends
//! - [`parquet`] - Range-read Parquet file reader
//! - [`send_parquet`] - Send-safe Parquet reader for AWS SDK integration
//! - [`chunk_reader`] - Range-backed ChunkReader for large files (>64MB)

pub mod batch;
#[cfg(feature = "aws")]
pub mod chunk_reader;
pub mod parquet;
#[cfg(feature = "aws")]
pub mod send_parquet;
pub mod storage;

pub use batch::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
pub use storage::{IcebergStorage, MemoryStorage, RangeOnlyStorage};

#[cfg(feature = "aws")]
pub use chunk_reader::RangeBackedChunkReader;
#[cfg(feature = "aws")]
pub use send_parquet::SendParquetReader;
#[cfg(feature = "aws")]
pub use storage::{S3IcebergStorage, SendIcebergStorage};
