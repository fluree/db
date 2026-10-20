//! Decryption counters, recorded into `fluree_db_core::io_stats` when
//! `FLUREE_IO_STATS` is set.

use fluree_db_core::io_stats::record_work;

/// One version-1 envelope decrypted whole.
pub(crate) fn v1_object_decrypted(bytes: usize) {
    record_work("decrypt v1 object", bytes);
}

/// One segment of a segmented envelope decrypted.
pub(crate) fn segment_decrypted(bytes: usize) {
    record_work("decrypt v2 segment", bytes);
}

/// One segmented envelope decrypted whole; its segments count separately.
pub(crate) fn whole_object_decrypted(bytes: usize) {
    record_work("decrypt v2 object", bytes);
}
