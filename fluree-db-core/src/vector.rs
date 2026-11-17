//! The `f:embeddingVector` values a ledger can store.
//!
//! The index keeps the vectors of each property in each graph in one arena
//! of fixed-length f32 rows. The first vector the property receives in that
//! graph sets the length, and the length is stored as a `u16`. Retracting
//! that vector does not reset it, because the index keeps history.
//!
//! [`check_vector`] is that rule. The vector arena applies it when it stores
//! a vector, and the write paths apply it before they commit one.

use thiserror::Error;

/// The most elements a vector can have.
pub const MAX_VECTOR_DIMS: usize = u16::MAX as usize;

/// Why a vector cannot be stored.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum VectorError {
    #[error("vector must not be empty")]
    Empty,
    #[error("vector dims {len} exceeds the maximum of {MAX_VECTOR_DIMS}")]
    TooLong { len: usize },
    #[error("vector dims mismatch: expected {expected}, got {len}")]
    LengthMismatch { expected: u16, len: usize },
    #[error("vector element [{index}] is not finite: {value}")]
    NotFinite { index: usize, value: f32 },
}

/// Check that a vector with `elements` can be stored beside vectors of
/// length `dims`, or as the first vector when `dims` is `None`.
///
/// Returns the length the vectors have once it is stored.
pub fn check_vector<I>(dims: Option<u16>, elements: I) -> Result<u16, VectorError>
where
    I: ExactSizeIterator<Item = f32>,
{
    let len = elements.len();
    if len == 0 {
        return Err(VectorError::Empty);
    }
    // Too long before mismatched: no stored length could admit it.
    let stored = u16::try_from(len).map_err(|_| VectorError::TooLong { len })?;
    let dims = match dims {
        Some(expected) if expected != stored => {
            return Err(VectorError::LengthMismatch { expected, len });
        }
        _ => stored,
    };
    for (index, value) in elements.enumerate() {
        if !value.is_finite() {
            return Err(VectorError::NotFinite { index, value });
        }
    }
    Ok(dims)
}

/// [`check_vector`] for a vector held as `f64`, as `FlakeValue::Vector`
/// holds it: each element is stored as the `f32` it narrows to.
pub fn check_vector_f64(dims: Option<u16>, elements: &[f64]) -> Result<u16, VectorError> {
    check_vector(dims, elements.iter().map(|&x| x as f32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_vector_sets_the_length() {
        assert_eq!(check_vector_f64(None, &[1.0, 2.0, 3.0]), Ok(3));
        assert_eq!(check_vector_f64(Some(3), &[4.0, 5.0, 6.0]), Ok(3));
        assert_eq!(
            check_vector_f64(Some(3), &[1.0, 2.0]),
            Err(VectorError::LengthMismatch {
                expected: 3,
                len: 2
            })
        );
    }

    #[test]
    fn a_vector_has_one_to_65535_elements() {
        assert_eq!(check_vector_f64(None, &[]), Err(VectorError::Empty));
        let longest = vec![0.5; MAX_VECTOR_DIMS];
        assert_eq!(check_vector_f64(None, &longest), Ok(u16::MAX));
        assert_eq!(check_vector_f64(Some(u16::MAX), &longest), Ok(u16::MAX));
        let too_long = vec![0.5; MAX_VECTOR_DIMS + 1];
        for dims in [None, Some(3), Some(u16::MAX)] {
            assert_eq!(
                check_vector_f64(dims, &too_long),
                Err(VectorError::TooLong {
                    len: MAX_VECTOR_DIMS + 1
                }),
                "{dims:?}"
            );
        }
    }

    #[test]
    fn every_element_is_a_finite_f32() {
        assert!(matches!(
            check_vector_f64(None, &[1.0, f64::NAN]),
            Err(VectorError::NotFinite { index: 1, .. })
        ));
        // Finite as f64, but beyond the f32 range.
        assert!(matches!(
            check_vector_f64(None, &[f64::MAX]),
            Err(VectorError::NotFinite { index: 0, .. })
        ));
        assert!(matches!(
            check_vector(Some(2), [0.0, f32::NEG_INFINITY].into_iter()),
            Err(VectorError::NotFinite { index: 1, .. })
        ));
    }
}
