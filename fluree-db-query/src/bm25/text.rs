//! Text extraction
//!
//! Extracts text from JSON-LD values:
//! - Concatenates all string values with space separators
//! - Stringifies non-nil scalars (numbers, bools) and includes them
//! - Handles nested maps (referred nodes) and sequences recursively
//! - Skips `@id` keys and `null` values only

use serde_json::Value;

/// Extract text from a JSON-LD value.
///
/// Recursively extracts text content from:
/// - Strings: included directly
/// - Numbers: stringified and included
/// - Booleans: stringified ("true"/"false") and included
/// - Objects: all values extracted except for `@id` keys
/// - Arrays: all elements extracted
/// - Null: skipped
///
/// This matches the legacy `extract-text` behavior.
pub fn extract_text(item: &Value) -> String {
    let mut result = String::new();
    extract_text_recursive(item, &mut result);
    result
}

fn extract_text_recursive(value: &Value, result: &mut String) {
    match value {
        Value::String(s) => {
            if !result.is_empty() && !s.is_empty() {
                result.push(' ');
            }
            result.push_str(s);
        }
        Value::Number(n) => {
            // Stringify numbers and include them
            if !result.is_empty() {
                result.push(' ');
            }
            result.push_str(&n.to_string());
        }
        Value::Bool(b) => {
            // Stringify bools and include them
            if !result.is_empty() {
                result.push(' ');
            }
            result.push_str(if *b { "true" } else { "false" });
        }
        Value::Object(map) => {
            for (key, val) in map {
                // Skip @id keys (they're identifiers, not searchable text)
                if key != "@id" {
                    extract_text_recursive(val, result);
                }
            }
        }
        Value::Array(arr) => {
            for val in arr {
                extract_text_recursive(val, result);
            }
        }
        Value::Null => {
            // Skip null values
        }
    }
}

/// Extract text and analyze it into term frequencies.
///
/// Combines text extraction with the analyzer for indexing.
pub fn extract_and_analyze(
    item: &Value,
    analyzer: &super::analyzer::Analyzer,
) -> std::collections::HashMap<String, u32> {
    let text = extract_text(item);
    analyzer.analyze_to_term_freqs(&text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_string() {
        let value = json!("Hello World");
        assert_eq!(extract_text(&value), "Hello World");
    }

    #[test]
    fn test_extract_number() {
        let value = json!(42);
        assert_eq!(extract_text(&value), "42");

        let value = json!(3.13);
        assert_eq!(extract_text(&value), "3.13");
    }

    #[test]
    fn test_extract_bool() {
        assert_eq!(extract_text(&json!(true)), "true");
        assert_eq!(extract_text(&json!(false)), "false");
    }

    #[test]
    fn test_extract_null() {
        let value = json!(null);
        assert_eq!(extract_text(&value), "");
    }

    #[test]
    fn test_extract_array() {
        let value = json!(["hello", "world", 42]);
        assert_eq!(extract_text(&value), "hello world 42");
    }

    #[test]
    fn test_extract_object() {
        let value = json!({
            "name": "Alice",
            "age": 30,
            "active": true
        });
        let text = extract_text(&value);

        // Should contain all values
        assert!(text.contains("Alice"));
        assert!(text.contains("30"));
        assert!(text.contains("true"));
    }

    #[test]
    fn test_extract_object_skips_id() {
        let value = json!({
            "@id": "http://example.org/person/1",
            "name": "Alice",
            "description": "A person"
        });
        let text = extract_text(&value);

        // Should NOT contain the @id value
        assert!(!text.contains("http://example.org/person/1"));
        // Should contain other values
        assert!(text.contains("Alice"));
        assert!(text.contains("A person"));
    }

    #[test]
    fn test_extract_nested() {
        let value = json!({
            "name": "Product X",
            "details": {
                "description": "A great product",
                "price": 99,
                "inStock": true
            },
            "tags": ["sale", "popular"]
        });
        let text = extract_text(&value);

        assert!(text.contains("Product X"));
        assert!(text.contains("A great product"));
        assert!(text.contains("99"));
        assert!(text.contains("true"));
        assert!(text.contains("sale"));
        assert!(text.contains("popular"));
    }

    #[test]
    fn test_extract_deeply_nested() {
        let value = json!({
            "level1": {
                "level2": {
                    "level3": {
                        "text": "deep value"
                    }
                }
            }
        });
        let text = extract_text(&value);
        assert!(text.contains("deep value"));
    }

    #[test]
    fn test_extract_mixed_types_in_array() {
        let value = json!([
            "text",
            123,
            true,
            null,
            {"nested": "value"}
        ]);
        let text = extract_text(&value);

        assert!(text.contains("text"));
        assert!(text.contains("123"));
        assert!(text.contains("true"));
        assert!(text.contains("value"));
    }

    #[test]
    fn test_extract_empty_values() {
        // Empty string
        assert_eq!(extract_text(&json!("")), "");

        // Empty array
        assert_eq!(extract_text(&json!([])), "");

        // Empty object
        assert_eq!(extract_text(&json!({})), "");
    }

    #[test]
    fn test_extract_jsonld_like_document() {
        // Simulate a typical JSON-LD document structure
        let value = json!({
            "@context": {"@vocab": "http://schema.org/"},
            "@id": "http://example.org/article/1",
            "@type": "Article",
            "headline": "Breaking News: Rust is Awesome",
            "articleBody": "Rust provides memory safety without garbage collection.",
            "author": {
                "@id": "http://example.org/person/1",
                "name": "Jane Doe"
            },
            "datePublished": "2024-01-15",
            "keywords": ["rust", "programming", "safety"]
        });
        let text = extract_text(&value);

        // Should contain content but not IDs
        assert!(text.contains("Breaking News: Rust is Awesome"));
        assert!(text.contains("Rust provides memory safety"));
        assert!(text.contains("Jane Doe"));
        assert!(text.contains("rust"));
        assert!(text.contains("programming"));
        assert!(!text.contains("http://example.org/article/1"));
        assert!(!text.contains("http://example.org/person/1"));
    }

    #[test]
    fn test_extract_with_type_annotation() {
        // @type is NOT @id, so it should be included
        let value = json!({
            "@id": "http://example.org/1",
            "@type": "Person",
            "name": "Alice"
        });
        let text = extract_text(&value);

        assert!(text.contains("Person"));
        assert!(text.contains("Alice"));
        assert!(!text.contains("http://example.org/1"));
    }
}
