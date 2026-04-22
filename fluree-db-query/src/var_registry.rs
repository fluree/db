//! Variable registry for query execution
//!
//! Maps variable names (e.g., "?s", "?name") to compact `VarId` indices
//! used throughout the query execution pipeline.

use std::collections::HashMap;
use std::sync::Arc;

/// Compact variable identifier - index into batch columns
///
/// u16 supports up to 65K variables per query (sufficient for any realistic query).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VarId(pub u16);

impl VarId {
    /// Get the underlying index value
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Registry mapping variable names to compact VarId indices
///
/// Uses `Arc<str>` for cheap cloning and deduplication, aligning with `Sid.name`.
#[derive(Debug, Default)]
pub struct VarRegistry {
    name_to_id: HashMap<Arc<str>, VarId>,
    id_to_name: Vec<Arc<str>>,
}

impl VarRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a registry with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            name_to_id: HashMap::with_capacity(capacity),
            id_to_name: Vec::with_capacity(capacity),
        }
    }

    /// Get existing VarId or insert a new one
    ///
    /// Returns the VarId for the given variable name, creating a new
    /// entry if the name hasn't been seen before.
    pub fn get_or_insert(&mut self, name: &str) -> VarId {
        if let Some(&id) = self.name_to_id.get(name) {
            return id;
        }

        // Guardrail: VarId is u16; exceeding this would silently wrap and corrupt bindings.
        //
        // This runs only when introducing a *new* variable name (planning time), not in the
        // hot query execution loop.
        assert!(
            self.id_to_name.len() < (u16::MAX as usize),
            "VarRegistry capacity exceeded ({}). VarId is u16; refusing to wrap.",
            self.id_to_name.len()
        );

        let id = VarId(self.id_to_name.len() as u16);
        let arc_name: Arc<str> = Arc::from(name);
        self.name_to_id.insert(arc_name.clone(), id);
        self.id_to_name.push(arc_name);
        id
    }

    /// Get the VarId for a name, if it exists
    pub fn get(&self, name: &str) -> Option<VarId> {
        self.name_to_id.get(name).copied()
    }

    /// Get the name for a VarId
    ///
    /// # Panics
    ///
    /// Panics if the VarId is not in the registry (indicates a bug).
    pub fn name(&self, id: VarId) -> &str {
        &self.id_to_name[id.index()]
    }

    /// Get the name for a VarId, returning None if invalid
    pub fn try_name(&self, id: VarId) -> Option<&str> {
        self.id_to_name
            .get(id.index())
            .map(std::convert::AsRef::as_ref)
    }

    /// Get the number of registered variables
    pub fn len(&self) -> usize {
        self.id_to_name.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.id_to_name.is_empty()
    }

    /// Iterate over all (name, VarId) pairs
    pub fn iter(&self) -> impl Iterator<Item = (&str, VarId)> {
        self.id_to_name
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_ref(), VarId(i as u16)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_or_insert() {
        let mut reg = VarRegistry::new();

        let s = reg.get_or_insert("?s");
        let p = reg.get_or_insert("?p");
        let o = reg.get_or_insert("?o");

        assert_eq!(s.0, 0);
        assert_eq!(p.0, 1);
        assert_eq!(o.0, 2);

        // Second insert returns same id
        let s2 = reg.get_or_insert("?s");
        assert_eq!(s, s2);
    }

    #[test]
    fn test_name_lookup() {
        let mut reg = VarRegistry::new();

        let s = reg.get_or_insert("?s");
        let name = reg.get_or_insert("?name");

        assert_eq!(reg.name(s), "?s");
        assert_eq!(reg.name(name), "?name");
    }

    #[test]
    fn test_get() {
        let mut reg = VarRegistry::new();

        assert!(reg.get("?s").is_none());

        reg.get_or_insert("?s");

        assert!(reg.get("?s").is_some());
        assert!(reg.get("?other").is_none());
    }

    #[test]
    fn test_len() {
        let mut reg = VarRegistry::new();

        assert_eq!(reg.len(), 0);
        assert!(reg.is_empty());

        reg.get_or_insert("?a");
        reg.get_or_insert("?b");

        assert_eq!(reg.len(), 2);
        assert!(!reg.is_empty());

        // Duplicate doesn't increase count
        reg.get_or_insert("?a");
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn test_iter() {
        let mut reg = VarRegistry::new();
        reg.get_or_insert("?s");
        reg.get_or_insert("?p");
        reg.get_or_insert("?o");

        let pairs: Vec<_> = reg.iter().collect();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("?s", VarId(0)));
        assert_eq!(pairs[1], ("?p", VarId(1)));
        assert_eq!(pairs[2], ("?o", VarId(2)));
    }
}
