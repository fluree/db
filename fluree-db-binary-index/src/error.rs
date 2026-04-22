//! Error types for binary index operations.

use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BinaryIndexError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("decode error: {0}")]
    Decode(String),
}

pub type Result<T> = std::result::Result<T, BinaryIndexError>;
