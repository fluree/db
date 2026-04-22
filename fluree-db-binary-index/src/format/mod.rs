//! Binary index wire formats: FIR6 (index root), FBR3 (branch), FLI3 (leaf),
//! leaflet column encoding, run record layout, and stats/schema encoding.

pub mod branch;
pub mod column_block;
pub mod history_sidecar;
pub mod index_root;
pub mod leaf;
pub mod leaflet;
pub mod run_record;
pub mod run_record_v2;
pub mod stats_wire;
pub mod wire_helpers;
