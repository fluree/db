//! Per-predicate numeric shape classification and persistence.
//!
//! Tracks which numeric `ObjKind` variants appear for each predicate:
//! - `IntOnly`: only `NumInt`
//! - `FloatOnly`: only `NumF64`
//! - `Mixed`: both `NumInt` and `NumF64`
//!
//! Used by the binary scan path to decide whether a single POST scan
//! suffices, or whether a multi-range fallback is needed.

use std::collections::HashMap;
use std::io;

// ============================================================================
// NumericShape -- per-predicate numeric tag classification
// ============================================================================

/// Per-predicate classification of numeric value kinds.
///
/// Used by the binary scan path to decide whether a single POST scan
/// (`IntOnly` or `FloatOnly`) suffices, or whether a fallback is needed (`Mixed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericShape {
    /// All numeric values for this predicate are `NumInt` (kind 0x03).
    IntOnly,
    /// All numeric values for this predicate are `NumF64` (kind 0x04).
    FloatOnly,
    /// Predicate has both `NumInt` and `NumF64` values.
    Mixed,
}

/// Write per-predicate numeric shapes to JSON.
///
/// Format: `{ "3": "int", "7": "float", "12": "mixed" }`
/// String-keyed for JSON compatibility (p_id as string key).
pub fn write_numeric_shapes(
    path: &std::path::Path,
    shapes: &HashMap<u32, NumericShape>,
) -> io::Result<()> {
    let map: HashMap<String, &str> = shapes
        .iter()
        .map(|(&p_id, shape)| {
            let label = match shape {
                NumericShape::IntOnly => "int",
                NumericShape::FloatOnly => "float",
                NumericShape::Mixed => "mixed",
            };
            (p_id.to_string(), label)
        })
        .collect();

    let json = serde_json::to_string_pretty(&map).map_err(io::Error::other)?;
    std::fs::write(path, json)?;
    Ok(())
}

/// Read per-predicate numeric shapes from JSON.
pub fn read_numeric_shapes(path: &std::path::Path) -> io::Result<HashMap<u32, NumericShape>> {
    let json = std::fs::read_to_string(path)?;
    let map: HashMap<String, String> =
        serde_json::from_str(&json).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut shapes = HashMap::with_capacity(map.len());
    for (key, val) in map {
        let p_id: u32 = key.parse().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid p_id key '{key}': {e}"),
            )
        })?;
        let shape = match val.as_str() {
            "int" => NumericShape::IntOnly,
            "float" => NumericShape::FloatOnly,
            "mixed" => NumericShape::Mixed,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown numeric shape '{other}' for p_id={p_id}"),
                ))
            }
        };
        shapes.insert(p_id, shape);
    }
    Ok(shapes)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_shapes_round_trip() {
        let mut shapes = HashMap::new();
        shapes.insert(3, NumericShape::IntOnly);
        shapes.insert(7, NumericShape::FloatOnly);
        shapes.insert(12, NumericShape::Mixed);

        let dir = std::env::temp_dir().join("fluree_shapes_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("numeric_shapes.json");

        write_numeric_shapes(&path, &shapes).unwrap();
        let loaded = read_numeric_shapes(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[&3], NumericShape::IntOnly);
        assert_eq!(loaded[&7], NumericShape::FloatOnly);
        assert_eq!(loaded[&12], NumericShape::Mixed);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
