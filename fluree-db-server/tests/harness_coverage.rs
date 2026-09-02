//! Guards the two invariants that `autotests = false` plus grouped harnesses
//! rely on and the compiler does not check: every `tests/*.rs` is reachable,
//! and nothing compiled into a shared harness mutates process-global env.
//!
//! The logic lives in `fluree-test-support` so it is shared with the other
//! crates using this layout rather than copied between them.

#[test]
fn every_test_file_is_reachable() {
    fluree_test_support::assert_every_test_file_is_reachable(env!("CARGO_MANIFEST_DIR"));
}

#[test]
fn grouped_tests_do_not_mutate_process_env() {
    fluree_test_support::assert_grouped_tests_do_not_mutate_env(env!("CARGO_MANIFEST_DIR"));
}
