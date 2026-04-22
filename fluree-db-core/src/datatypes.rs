//! Datatype utilities.
//!
//! Centralizes datatype matching semantics shared across query execution paths.

use crate::Sid;
use fluree_vocab::namespaces::XSD;
use fluree_vocab::xsd_names;

/// Datatype match semantics for value objects.
///
/// Query parsing normalizes some numeric datatype IRIs (e.g., xsd:int → xsd:integer),
/// so matching must treat numeric "families" as compatible at execution time:
/// - xsd:integer matches integer-family stored datatypes (xsd:int, xsd:long, ...)
/// - xsd:double matches xsd:float
#[inline]
pub fn dt_compatible(expected: &Sid, actual: &Sid) -> bool {
    if expected == actual {
        return true;
    }
    if expected.namespace_code != XSD || actual.namespace_code != XSD {
        return false;
    }
    match expected.name.as_ref() {
        xsd_names::INTEGER => matches!(
            actual.name.as_ref(),
            xsd_names::INTEGER
                | xsd_names::INT
                | xsd_names::SHORT
                | xsd_names::BYTE
                | xsd_names::LONG
        ),
        xsd_names::DOUBLE => matches!(actual.name.as_ref(), xsd_names::DOUBLE | xsd_names::FLOAT),
        _ => false,
    }
}
