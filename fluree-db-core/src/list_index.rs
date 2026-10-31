//! A list member's place: which list of its graph, subject and predicate it
//! is in, and its position in that list.
//!
//! A position is at most `i32::MAX`. The index and the commit format carry
//! both as one `u64`, the list in the high 32 bits and the position in the
//! low 32, so a property's list members sort by list, then position, and a
//! position written before lists had identities (list 0) reads unchanged.
//! [`NONE`] is the place of a value that is no list member.

/// The packed place of a value that is no list member.
pub const NONE: u64 = u64::MAX;

/// `list` and `position` as one `u64`. `position` must not be negative.
#[inline]
pub const fn pack(list: u32, position: i32) -> u64 {
    debug_assert!(position >= 0, "a list position is never negative");
    ((list as u64) << 32) | (position as u32 as u64)
}

/// The list and position `packed` holds: `None` for [`NONE`], and for a
/// position past `i32::MAX`, which [`pack`] never writes.
#[inline]
pub fn unpack(packed: u64) -> Option<(u32, i32)> {
    if packed == NONE {
        return None;
    }
    let position = i32::try_from(packed & 0xffff_ffff).ok()?;
    Some(((packed >> 32) as u32, position))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_place_packs_list_high_and_position_low() {
        assert_eq!(pack(0, 7), 7, "list 0 is the position alone");
        assert_eq!(unpack(pack(3, 7)), Some((3, 7)));
        assert_eq!(unpack(pack(u32::MAX, i32::MAX)), Some((u32::MAX, i32::MAX)));
        assert!(
            pack(u32::MAX, i32::MAX) < NONE,
            "every place sorts below none"
        );
        assert!(
            pack(0, i32::MAX) < pack(1, 0),
            "lists sort before positions"
        );
        assert_eq!(unpack(NONE), None);
        assert_eq!(
            unpack(u64::from(u32::MAX)),
            None,
            "a position past i32::MAX"
        );
    }
}
