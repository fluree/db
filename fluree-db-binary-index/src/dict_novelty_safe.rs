//! Unified, "safe" DictNovelty population helpers.
//!
//! Goal: ensure we **never** allocate a novelty ID for an entry that already exists in the
//! persisted dictionaries (`BinaryIndexStore`). This prevents multiple internal IDs from
//! decoding to the same logical IRI / string.

use crate::BinaryIndexStore;
use fluree_db_core::{DictNovelty, Flake, FlakeValue, Sid};
use std::io;

#[inline]
fn subject_is_persisted(store: &BinaryIndexStore, sid: &Sid) -> io::Result<bool> {
    // Canonical encoding guarantees exact-parts match: if the SID was encoded
    // correctly, `find_subject_id_by_parts` will find it. No IRI-reconstruction
    // fallback is needed (no legacy data to accommodate).
    Ok(store
        .find_subject_id_by_parts(sid.namespace_code, &sid.name)?
        .is_some())
}

#[inline]
fn string_is_persisted(store: &BinaryIndexStore, s: &str) -> io::Result<bool> {
    Ok(store.find_string_id(s)?.is_some())
}

/// Populate `DictNovelty` from a flake iterator, without shadowing persisted entries.
///
/// Contract:
/// - persisted dict wins (no novelty allocation)
/// - then novelty dict wins (no duplicate novelty allocation)
/// - then allocate
pub fn populate_dict_novelty_safe<'a>(
    dict_novelty: &mut DictNovelty,
    store: Option<&BinaryIndexStore>,
    flakes: impl IntoIterator<Item = &'a Flake>,
) -> io::Result<()> {
    dict_novelty.ensure_initialized();

    for flake in flakes {
        // Subject
        let s = &flake.s;
        let persisted = match store {
            Some(store) => subject_is_persisted(store, s)?,
            None => false,
        };
        if !persisted
            && dict_novelty
                .subjects
                .find_subject(s.namespace_code, &s.name)
                .is_none()
        {
            dict_novelty
                .subjects
                .assign_or_lookup(s.namespace_code, &s.name);
        }

        // Object references
        if let FlakeValue::Ref(ref sid) = flake.o {
            let persisted = match store {
                Some(store) => subject_is_persisted(store, sid)?,
                None => false,
            };
            if !persisted
                && dict_novelty
                    .subjects
                    .find_subject(sid.namespace_code, &sid.name)
                    .is_none()
            {
                dict_novelty
                    .subjects
                    .assign_or_lookup(sid.namespace_code, &sid.name);
            }
        }

        // String-ish
        match &flake.o {
            FlakeValue::String(s) | FlakeValue::Json(s) => {
                let persisted = match store {
                    Some(store) => string_is_persisted(store, s)?,
                    None => false,
                };
                if !persisted && dict_novelty.strings.find_string(s).is_none() {
                    dict_novelty.strings.assign_or_lookup(s);
                }
            }
            _ => {}
        }
    }

    Ok(())
}
