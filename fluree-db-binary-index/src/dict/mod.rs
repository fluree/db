//! Dictionary storage for subject and string dictionaries.
//!
//! Forward lookups (id → value) use **FPK1 packs** — flat, paged files with
//! O(1) offset-indexed pages and pre-parsed metadata for zero-alloc lookups.
//! See [`forward_pack`] for the binary format.
//!
//! Reverse lookups (value → id) use **CoW B-trees**: one branch manifest
//! pointing to multiple leaf files, all content-addressed for CAS storage.
//!
//! ## Dictionaries
//!
//! - **Subject forward** (packs): `sid64 → suffix` per namespace, ns-compressed
//! - **Subject reverse** (tree): `[ns_code BE][suffix] → sid64`, sorted lexicographically
//! - **String forward** (packs): `string_id → value`
//! - **String reverse** (tree): `value → string_id`
//!
//! ## Reverse Leaf Format
//!
//! Leaves use an offset-table design for O(log n) binary search on
//! variable-length entries. Each leaf is a self-contained blob:
//!
//! ```text
//! [magic: 4B][entry_count: u32][offset_table: u32 × entry_count][entries...]
//! ```
//!
//! ## Branch Format
//!
//! Branches map key ranges to leaf CAS addresses:
//!
//! ```text
//! [magic: 4B][leaf_count: u32][offset_table: u32 × leaf_count][leaf_entries...]
//! ```

pub mod branch;
pub mod builder;
pub mod dict_io;
pub mod forward_pack;
pub mod global_dict;
pub mod incremental;
pub mod pack_builder;
pub mod pack_reader;
pub mod reader;
pub mod reverse_leaf;
pub mod varint;

pub use branch::DictBranch;
pub use builder::TreeBuildResult;
pub use forward_pack::ForwardPack;
pub use global_dict::{LanguageTagDict, PredicateDict};
pub use pack_reader::ForwardPackReader;
pub use reader::DictTreeReader;
