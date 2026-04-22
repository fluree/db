use crate::types::MemoryKind;

/// Generate a unique memory ID using ULID.
///
/// Format: `mem:<kind>-<ulid>` (e.g., `mem:fact-01JDXYZ...`)
pub fn generate_memory_id(kind: MemoryKind) -> String {
    let ulid = ulid::Ulid::new();
    format!("mem:{}-{}", kind.as_str(), ulid.to_string().to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_format() {
        let id = generate_memory_id(MemoryKind::Fact);
        assert!(id.starts_with("mem:fact-"));
        assert!(id.len() > 15); // ULID is 26 chars + prefix
    }

    #[test]
    fn ids_are_unique() {
        let a = generate_memory_id(MemoryKind::Decision);
        let b = generate_memory_id(MemoryKind::Decision);
        assert_ne!(a, b);
    }
}
