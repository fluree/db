//! Guards the invariant that `autotests = false` relies on and the compiler
//! does not check: every `tests/*.rs` must be reachable, or it is silently
//! never compiled and never run while `cargo test` still reports success.
//!
//! Its own `[[test]]` target rather than a harness member, so that deleting the
//! line that wires it in cannot quietly disable it — the guard would then be an
//! orphan of exactly the kind it exists to catch.
//!
//! The logic lives in `fluree-test-support`, shared with the other crate using
//! this layout rather than copied between them.

#[test]
fn every_test_file_is_reachable() {
    fluree_test_support::assert_every_test_file_is_reachable(env!("CARGO_MANIFEST_DIR"));
}
