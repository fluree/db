//! Tests for the execute module.
//!
//! This module is organized by rule category:
//! - `data_structure_tests` - DeltaSet and DerivedSet tests
//! - `property_rule_tests` - Property rules (prp-*)
//! - `class_rule_tests` - Class hierarchy rules (cax-*)
//! - `restriction_rule_tests` - OWL restriction rules (cls-*)

mod class_rule_tests;
mod data_structure_tests;
mod property_rule_tests;
mod restriction_rule_tests;

use super::*;
use crate::owl;

/// Create a test SID from a namespace code.
pub(crate) fn sid(n: u16) -> Sid {
    Sid::new(n, format!("test:{n}"))
}

/// Create a reference flake for testing.
pub(crate) fn make_ref_flake(s: u16, p: u16, o: u16, t: i64) -> Flake {
    Flake::new(
        sid(s),
        sid(p),
        FlakeValue::Ref(sid(o)),
        sid(0),
        t,
        true,
        None,
    )
}
