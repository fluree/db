//! Accumulated derived facts with deduplication.
//!
//! The DerivedSet tracks all facts derived during reasoning, providing
//! efficient deduplication and predicate-indexed lookups for rule joins.

use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::Sid;
use hashbrown::{HashMap, HashSet};

/// Accumulated derived facts with deduplication
#[derive(Debug, Default)]
pub struct DerivedSet {
    /// All derived flakes
    flakes: Vec<Flake>,
    /// Set of (s, p, o) tuples for deduplication
    /// We use a simplified key: (s, p, o_hash) where o_hash is computed from FlakeValue
    seen: HashSet<(Sid, Sid, u64)>,
    /// Index by predicate for join lookups
    by_p: HashMap<Sid, Vec<usize>>,
    /// Index by (predicate, subject) for join lookups
    by_ps: HashMap<(Sid, Sid), Vec<usize>>,
    /// Index by (predicate, object) for join lookups (Ref objects only)
    by_po: HashMap<(Sid, Sid), Vec<usize>>,
}

impl DerivedSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Compute a hash key for a flake's object
    fn object_hash(o: &FlakeValue) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match o {
            FlakeValue::Ref(sid) => {
                0u8.hash(&mut hasher);
                sid.namespace_code.hash(&mut hasher);
                sid.name.hash(&mut hasher);
            }
            FlakeValue::Long(v) => {
                1u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::Double(v) => {
                2u8.hash(&mut hasher);
                v.to_bits().hash(&mut hasher);
            }
            FlakeValue::Boolean(v) => {
                3u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::String(v) => {
                4u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::Json(v) => {
                5u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::Vector(v) => {
                6u8.hash(&mut hasher);
                for f in v {
                    f.to_bits().hash(&mut hasher);
                }
            }
            FlakeValue::Null => {
                7u8.hash(&mut hasher);
            }
            FlakeValue::BigInt(v) => {
                8u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::Decimal(v) => {
                9u8.hash(&mut hasher);
                v.to_string().hash(&mut hasher);
            }
            FlakeValue::DateTime(v) => {
                10u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::Date(v) => {
                11u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::Time(v) => {
                12u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::GYear(v) => {
                13u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::GYearMonth(v) => {
                14u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::GMonth(v) => {
                15u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::GDay(v) => {
                16u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::GMonthDay(v) => {
                17u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::YearMonthDuration(v) => {
                18u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::DayTimeDuration(v) => {
                19u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::Duration(v) => {
                20u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            FlakeValue::GeoPoint(v) => {
                21u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    /// Try to add a flake, returns true if it was new
    pub fn try_add(&mut self, flake: Flake) -> bool {
        let key = (
            flake.s.clone(),
            flake.p.clone(),
            Self::object_hash(&flake.o),
        );

        if self.seen.contains(&key) {
            return false;
        }

        self.seen.insert(key);
        let idx = self.flakes.len();

        // Index by predicate
        self.by_p.entry(flake.p.clone()).or_default().push(idx);

        // Index by (predicate, subject)
        self.by_ps
            .entry((flake.p.clone(), flake.s.clone()))
            .or_default()
            .push(idx);

        // Index by (predicate, object) if object is a Ref
        if let FlakeValue::Ref(o) = &flake.o {
            self.by_po
                .entry((flake.p.clone(), o.clone()))
                .or_default()
                .push(idx);
        }

        self.flakes.push(flake);
        true
    }

    /// Check if a flake already exists
    pub fn contains(&self, s: &Sid, p: &Sid, o: &FlakeValue) -> bool {
        let key = (s.clone(), p.clone(), Self::object_hash(o));
        self.seen.contains(&key)
    }

    /// Get all flakes with a specific predicate
    pub fn get_by_p(&self, p: &Sid) -> impl Iterator<Item = &Flake> {
        self.by_p
            .get(p)
            .into_iter()
            .flat_map(|indices| indices.iter().map(|&i| &self.flakes[i]))
    }

    /// Get all flakes with a specific (predicate, subject)
    pub fn get_by_ps(&self, p: &Sid, s: &Sid) -> impl Iterator<Item = &Flake> {
        self.by_ps
            .get(&(p.clone(), s.clone()))
            .into_iter()
            .flat_map(|indices| indices.iter().map(|&i| &self.flakes[i]))
    }

    /// Get all flakes with a specific (predicate, object)
    pub fn get_by_po(&self, p: &Sid, o: &Sid) -> impl Iterator<Item = &Flake> {
        self.by_po
            .get(&(p.clone(), o.clone()))
            .into_iter()
            .flat_map(|indices| indices.iter().map(|&i| &self.flakes[i]))
    }

    /// Get number of derived flakes
    pub fn len(&self) -> usize {
        self.flakes.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.flakes.is_empty()
    }

    /// Consume and return all flakes
    pub fn into_flakes(self) -> Vec<Flake> {
        self.flakes
    }
}
