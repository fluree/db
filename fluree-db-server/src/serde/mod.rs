//! Server-specific serialization formats
//!
//! This module re-exports internal transport formats from fluree-db-core used for
//! communication between transaction servers and peers. These formats are NOT
//! public API and may change between versions.

pub use fluree_db_core::serde::flakes_transport::{
    decode_flakes, encode_flakes, FlakesTransportError, TransportFlake, TransportValue,
};
