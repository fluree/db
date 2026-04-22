use crate::flake::Flake;
use crate::ids::{RuntimeDatatypeId, RuntimePredicateId};
use crate::sid::Sid;
use std::collections::HashMap;

/// Ledger-scoped runtime identity for low-cardinality predicate/datatype spaces.
///
/// Persisted dictionary positions are preserved when the dict is seeded from an
/// index root. Novelty-only entries are appended above the persisted counts and
/// remain stable for the lifetime of the owning ledger state.
#[derive(Debug, Clone, Default)]
pub struct RuntimeSmallDicts {
    predicates: RuntimeSidDict<RuntimePredicateId>,
    datatypes: RuntimeSidDict<RuntimeDatatypeId>,
}

impl RuntimeSmallDicts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_seeded_sids(
        predicate_sids: impl IntoIterator<Item = Sid>,
        datatype_sids: impl IntoIterator<Item = Sid>,
    ) -> Self {
        let mut dicts = Self::new();
        for sid in predicate_sids {
            dicts.predicates.seed(sid, RuntimePredicateId::from_u32);
        }
        for sid in datatype_sids {
            dicts.datatypes.seed(sid, runtime_datatype_id_from_index);
        }
        dicts
    }

    pub fn populate_from_flakes(&mut self, flakes: &[Flake]) {
        self.populate_from_flakes_iter(flakes.iter());
    }

    pub fn populate_from_flakes_iter<'a>(&mut self, flakes: impl IntoIterator<Item = &'a Flake>) {
        for flake in flakes {
            self.assign_or_lookup_predicate(&flake.p);
            self.assign_or_lookup_datatype(&flake.dt);
        }
    }

    pub fn predicate_id(&self, sid: &Sid) -> Option<RuntimePredicateId> {
        self.predicates.id_for_sid(sid)
    }

    pub fn datatype_id(&self, sid: &Sid) -> Option<RuntimeDatatypeId> {
        self.datatypes.id_for_sid(sid)
    }

    pub fn assign_or_lookup_predicate(&mut self, sid: &Sid) -> RuntimePredicateId {
        self.predicates
            .assign_or_lookup(sid, RuntimePredicateId::from_u32)
    }

    pub fn assign_or_lookup_datatype(&mut self, sid: &Sid) -> RuntimeDatatypeId {
        self.datatypes
            .assign_or_lookup(sid, runtime_datatype_id_from_index)
    }

    pub fn predicate_sid(&self, id: RuntimePredicateId) -> Option<&Sid> {
        self.predicates.sid_for_id(id.as_u32() as usize)
    }

    pub fn datatype_sid(&self, id: RuntimeDatatypeId) -> Option<&Sid> {
        self.datatypes.sid_for_id(id.as_u16() as usize)
    }

    pub fn predicate_count(&self) -> u32 {
        self.predicates.len() as u32
    }

    pub fn datatype_count(&self) -> u16 {
        self.datatypes.len() as u16
    }

    pub fn persisted_predicate_count(&self) -> u32 {
        self.predicates.persisted_len() as u32
    }

    pub fn persisted_datatype_count(&self) -> u16 {
        self.datatypes.persisted_len() as u16
    }

    pub fn is_persisted_predicate_id(&self, id: RuntimePredicateId) -> bool {
        (id.as_u32() as usize) < self.predicates.persisted_len()
    }

    pub fn is_persisted_datatype_id(&self, id: RuntimeDatatypeId) -> bool {
        (id.as_u16() as usize) < self.datatypes.persisted_len()
    }
}

#[derive(Debug, Clone)]
struct RuntimeSidDict<Id> {
    persisted_len: usize,
    by_id: Vec<Sid>,
    by_sid: HashMap<Sid, usize>,
    _phantom: std::marker::PhantomData<Id>,
}

impl<Id> Default for RuntimeSidDict<Id> {
    fn default() -> Self {
        Self {
            persisted_len: 0,
            by_id: Vec::new(),
            by_sid: HashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<Id> RuntimeSidDict<Id> {
    fn seed(&mut self, sid: Sid, wrap: impl FnOnce(u32) -> Id) -> Id {
        let id = self.assign_or_lookup(&sid, wrap);
        self.persisted_len = self.by_id.len();
        id
    }

    fn assign_or_lookup(&mut self, sid: &Sid, wrap: impl FnOnce(u32) -> Id) -> Id {
        if let Some(id) = self.by_sid.get(sid).copied() {
            return wrap(id as u32);
        }
        let id = self.by_id.len();
        self.by_id.push(sid.clone());
        self.by_sid.insert(sid.clone(), id);
        wrap(id as u32)
    }

    fn id_for_sid(&self, sid: &Sid) -> Option<Id>
    where
        Id: FromU32Id,
    {
        self.by_sid.get(sid).map(|id| Id::from_u32_id(*id as u32))
    }

    fn sid_for_id(&self, id: usize) -> Option<&Sid> {
        self.by_id.get(id)
    }

    fn len(&self) -> usize {
        self.by_id.len()
    }

    fn persisted_len(&self) -> usize {
        self.persisted_len
    }
}

trait FromU32Id {
    fn from_u32_id(v: u32) -> Self;
}

impl FromU32Id for RuntimePredicateId {
    fn from_u32_id(v: u32) -> Self {
        Self::from_u32(v)
    }
}

impl FromU32Id for RuntimeDatatypeId {
    fn from_u32_id(v: u32) -> Self {
        runtime_datatype_id_from_index(v)
    }
}

fn runtime_datatype_id_from_index(v: u32) -> RuntimeDatatypeId {
    debug_assert!(
        u16::try_from(v).is_ok(),
        "runtime datatype id overflow: {v}"
    );
    RuntimeDatatypeId::from_u16(v as u16)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlakeValue;

    fn sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    #[test]
    fn seeded_ids_preserve_persisted_positions() {
        let dicts = RuntimeSmallDicts::from_seeded_sids(
            [sid(10, "name"), sid(10, "age")],
            [sid(2, "string"), sid(2, "integer")],
        );

        assert_eq!(
            dicts.predicate_id(&sid(10, "name")),
            Some(RuntimePredicateId::from_u32(0))
        );
        assert_eq!(
            dicts.predicate_id(&sid(10, "age")),
            Some(RuntimePredicateId::from_u32(1))
        );
        assert!(dicts.is_persisted_predicate_id(RuntimePredicateId::from_u32(1)));
        assert_eq!(
            dicts.datatype_id(&sid(2, "integer")),
            Some(RuntimeDatatypeId::from_u16(1))
        );
    }

    #[test]
    fn novelty_entries_append_after_persisted_counts() {
        let mut dicts = RuntimeSmallDicts::from_seeded_sids([sid(10, "name")], [sid(2, "string")]);
        let novel_flake = Flake::new(
            sid(10, "alice"),
            sid(10, "score"),
            FlakeValue::Long(42),
            sid(2, "int"),
            2,
            true,
            None,
        );

        dicts.populate_from_flakes(&[novel_flake]);

        assert_eq!(
            dicts.predicate_id(&sid(10, "score")),
            Some(RuntimePredicateId::from_u32(1))
        );
        assert_eq!(
            dicts.datatype_id(&sid(2, "int")),
            Some(RuntimeDatatypeId::from_u16(1))
        );
        assert!(!dicts.is_persisted_predicate_id(RuntimePredicateId::from_u32(1)));
    }
}
