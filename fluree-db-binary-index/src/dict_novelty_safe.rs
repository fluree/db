//! Unified, "safe" DictNovelty population and lookup.
//!
//! Goal: ensure we **never** allocate a novelty ID for an entry that already exists in the
//! persisted dictionaries (`BinaryIndexStore`). This prevents multiple internal IDs from
//! decoding to the same logical IRI / string. [`DictLookup`] reads in the same order, so a
//! value maps to the one id that encoded bindings carry for it.

use crate::BinaryIndexStore;
use fluree_db_core::{DictNovelty, Flake, FlakeValue, Sid};
use std::collections::HashSet;
use std::io;

/// Value → id lookups over the persisted dictionaries, then the novelty layer.
#[derive(Clone, Copy)]
pub struct DictLookup<'a> {
    store: &'a BinaryIndexStore,
    novelty: Option<&'a DictNovelty>,
}

impl<'a> DictLookup<'a> {
    /// An uninitialized novelty layer allocates no ids, so it is skipped.
    pub fn new(store: &'a BinaryIndexStore, novelty: Option<&'a DictNovelty>) -> Self {
        Self {
            store,
            novelty: novelty.filter(|dn| dn.is_initialized()),
        }
    }

    pub fn store(&self) -> &'a BinaryIndexStore {
        self.store
    }

    pub fn subject_id(&self, ns_code: u16, name: &str) -> io::Result<Option<u64>> {
        if let Some(id) = self.store.find_subject_id_by_parts(ns_code, name)? {
            return Ok(Some(id));
        }
        Ok(self
            .novelty
            .and_then(|dn| dn.subjects.find_subject(ns_code, name)))
    }

    pub fn subject_id_for_iri(&self, iri: &str) -> io::Result<Option<u64>> {
        if let Some(id) = self.store.find_subject_id(iri)? {
            return Ok(Some(id));
        }
        let Some(dn) = self.novelty else {
            return Ok(None);
        };
        let sid = self.store.encode_iri(iri);
        Ok(dn.subjects.find_subject(sid.namespace_code, &sid.name))
    }

    pub fn string_id(&self, value: &str) -> io::Result<Option<u32>> {
        if let Some(id) = self.store.find_string_id(value)? {
            return Ok(Some(id));
        }
        Ok(self.novelty.and_then(|dn| dn.strings.find_string(value)))
    }
}

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

    // A commit names the same subject and the same strings many times over;
    // each persisted-dictionary probe is a tree lookup, so an entry is probed
    // once per call. An entry the novelty layer already knows was settled by
    // an earlier probe (or minted here), so it is not probed at all.
    let mut persisted_subjects: HashSet<(u16, &'a str)> = HashSet::new();
    let mut persisted_strings: HashSet<&'a str> = HashSet::new();

    let mut subject = |dict_novelty: &mut DictNovelty, sid: &'a Sid, t: i64| -> io::Result<()> {
        if dict_novelty
            .subjects
            .find_subject(sid.namespace_code, &sid.name)
            .is_some()
            || persisted_subjects.contains(&(sid.namespace_code, &*sid.name))
        {
            return Ok(());
        }
        let persisted = match store {
            Some(store) => subject_is_persisted(store, sid)?,
            None => false,
        };
        if persisted {
            persisted_subjects.insert((sid.namespace_code, &sid.name));
        } else {
            dict_novelty
                .subjects
                .assign_or_lookup_at(sid.namespace_code, &sid.name, t);
        }
        Ok(())
    };

    for flake in flakes {
        subject(dict_novelty, &flake.s, flake.t)?;
        match &flake.o {
            FlakeValue::Ref(sid) => subject(dict_novelty, sid, flake.t)?,
            FlakeValue::String(s) | FlakeValue::Json(s) => {
                if dict_novelty.strings.find_string(s).is_some()
                    || persisted_strings.contains(s.as_str())
                {
                    continue;
                }
                let persisted = match store {
                    Some(store) => string_is_persisted(store, s)?,
                    None => false,
                };
                if persisted {
                    persisted_strings.insert(s);
                } else {
                    dict_novelty.strings.assign_or_lookup_at(s, flake.t);
                }
            }
            _ => {}
        }
    }

    Ok(())
}
