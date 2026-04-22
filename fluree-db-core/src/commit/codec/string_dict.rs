//! Commit-local string dictionary: builder (write path) and reader (decode path).
//!
//! Each dictionary maps strings to sequential `local_id: u32` values.
//! local_id 0 is reserved (absent / default). Assignments start at 1.
//!
//! Wire format: `[count: varint][len: varint, utf8_bytes...]*`

use super::error::CommitCodecError;
use super::varint::{decode_varint, encode_varint, read_exact};
use rustc_hash::FxHashMap;

// =============================================================================
// Write path
// =============================================================================

/// Builder for a commit-local string dictionary.
///
/// Accepts strings, assigns sequential local IDs (starting at 1),
/// and deduplicates so the same string always gets the same ID.
#[derive(Debug)]
pub struct StringDictBuilder {
    /// string -> local_id mapping for O(1) dedup.
    /// Uses FxHashMap for faster hashing than SipHash on short strings.
    map: FxHashMap<String, u32>,
    /// Ordered list of strings (index 0 = local_id 1).
    entries: Vec<String>,
}

impl StringDictBuilder {
    pub fn new() -> Self {
        Self {
            map: FxHashMap::default(),
            entries: Vec::new(),
        }
    }

    /// Insert a string and return its local_id (>= 1). Deduplicates.
    pub fn insert(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = (self.entries.len() as u32) + 1; // 1-based
        self.map.insert(s.to_string(), id);
        self.entries.push(s.to_string());
        id
    }

    /// Number of unique strings in the dictionary.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the dictionary contains no strings.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Serialize the dictionary to bytes.
    ///
    /// Format: `[count: varint][len: varint, utf8_bytes...]*`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_varint(self.entries.len() as u64, &mut buf);
        for s in &self.entries {
            let bytes = s.as_bytes();
            encode_varint(bytes.len() as u64, &mut buf);
            buf.extend_from_slice(bytes);
        }
        buf
    }
}

impl Default for StringDictBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Read path
// =============================================================================

/// Deserialized string dictionary for the read path.
///
/// Provides O(1) lookup by local_id. local_id 0 is reserved (returns error).
#[derive(Debug)]
pub struct StringDict {
    /// Entries indexed by (local_id - 1).
    entries: Vec<String>,
}

impl StringDict {
    /// Deserialize from bytes produced by `StringDictBuilder::serialize()`.
    pub fn deserialize(data: &[u8]) -> Result<Self, CommitCodecError> {
        let mut pos = 0;
        let count = decode_varint(data, &mut pos)? as usize;
        let mut entries = Vec::with_capacity(count);

        for _ in 0..count {
            let len = decode_varint(data, &mut pos)? as usize;
            let bytes = read_exact(data, &mut pos, len).map_err(|_| {
                CommitCodecError::InvalidDictionary(
                    "string bytes extend past dictionary end".into(),
                )
            })?;
            let s = std::str::from_utf8(bytes)
                .map_err(|e| CommitCodecError::InvalidDictionary(format!("invalid UTF-8: {e}")))?;
            entries.push(s.to_string());
        }

        Ok(Self { entries })
    }

    /// Look up a string by local_id. Returns error for local_id 0 (reserved).
    pub fn get(&self, local_id: u32) -> Result<&str, CommitCodecError> {
        if local_id == 0 {
            return Err(CommitCodecError::InvalidDictionary(
                "local_id 0 is reserved".into(),
            ));
        }
        let idx = (local_id - 1) as usize;
        self.entries
            .get(idx)
            .map(std::string::String::as_str)
            .ok_or_else(|| {
                CommitCodecError::InvalidDictionary(format!(
                    "local_id {} out of range (dict has {} entries)",
                    local_id,
                    self.entries.len()
                ))
            })
    }

    /// Number of entries in the dictionary.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the dictionary is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_dedup() {
        let mut builder = StringDictBuilder::new();
        let id1 = builder.insert("Alice");
        let id2 = builder.insert("Bob");
        let id1_again = builder.insert("Alice");

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id1, id1_again);
        assert_eq!(builder.len(), 2);
    }

    #[test]
    fn test_round_trip_empty() {
        let builder = StringDictBuilder::new();
        let bytes = builder.serialize();
        let dict = StringDict::deserialize(&bytes).unwrap();
        assert_eq!(dict.len(), 0);
        assert!(dict.is_empty());
    }

    #[test]
    fn test_round_trip_single() {
        let mut builder = StringDictBuilder::new();
        builder.insert("thing");
        let bytes = builder.serialize();
        let dict = StringDict::deserialize(&bytes).unwrap();

        assert_eq!(dict.len(), 1);
        assert_eq!(dict.get(1).unwrap(), "thing");
    }

    #[test]
    fn test_round_trip_multiple() {
        let mut builder = StringDictBuilder::new();
        let names = [
            "Alice",
            "name",
            "string",
            "z6MkqtpqKGs4Et8mqBLBBAitDC1DPBiTJEbu26AcBX75B5rR",
        ];
        for name in &names {
            builder.insert(name);
        }

        let bytes = builder.serialize();
        let dict = StringDict::deserialize(&bytes).unwrap();

        assert_eq!(dict.len(), 4);
        for (i, name) in names.iter().enumerate() {
            assert_eq!(dict.get((i + 1) as u32).unwrap(), *name);
        }
    }

    #[test]
    fn test_reserved_zero() {
        let mut builder = StringDictBuilder::new();
        builder.insert("thing");
        let bytes = builder.serialize();
        let dict = StringDict::deserialize(&bytes).unwrap();
        assert!(dict.get(0).is_err());
    }

    #[test]
    fn test_out_of_range() {
        let mut builder = StringDictBuilder::new();
        builder.insert("thing");
        let bytes = builder.serialize();
        let dict = StringDict::deserialize(&bytes).unwrap();
        assert!(dict.get(2).is_err());
        assert!(dict.get(100).is_err());
    }

    #[test]
    fn test_unicode() {
        let mut builder = StringDictBuilder::new();
        builder.insert("\u{540d}\u{524d}");
        let bytes = builder.serialize();
        let dict = StringDict::deserialize(&bytes).unwrap();
        assert_eq!(dict.get(1).unwrap(), "\u{540d}\u{524d}");
    }

    #[test]
    fn test_large_dict() {
        let mut builder = StringDictBuilder::new();
        for i in 0..1000 {
            builder.insert(&format!("item/{i}"));
        }
        assert_eq!(builder.len(), 1000);

        let bytes = builder.serialize();
        let dict = StringDict::deserialize(&bytes).unwrap();
        assert_eq!(dict.len(), 1000);

        assert_eq!(dict.get(1).unwrap(), "item/0");
        assert_eq!(dict.get(1000).unwrap(), "item/999");
    }
}
