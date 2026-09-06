//! Schema derivation: one builder, fed by each tier's own description of the
//! ledger.

pub mod bootstrap;
pub mod build;
pub mod curated;
pub mod datatype;
pub mod inferred;
pub mod model;
pub mod shaped;

pub use build::build;
pub use model::SchemaModel;
