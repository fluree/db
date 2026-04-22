//! Data model markdown formatting for LLM consumption
//!
//! Converts the JSON output of `build_ledger_info` into a markdown document
//! optimized for LLM context. Follows the Solo MCP server's formatting style.

use serde_json::Value as JsonValue;

/// Format ledger info JSON as markdown for LLM consumption.
///
/// Output structure:
/// ```markdown
/// # Data Model Overview for "ledger-alias"
///
/// ## Prefixes
/// Use these prefixes in SPARQL queries:
///   rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
///   ...
///
/// ## Dataset Statistics
/// - Classes: N
/// - Total instances: N
/// - Properties: N
/// - Triples (flakes): N
/// - Size: N bytes
///
/// ## Classes
///
/// ### ClassName — N instances
/// Subclass of: ParentClass
///
/// Properties:
/// 1. propertyName
///    Type: xsd:string (N)
/// 2. propertyName
///    Type: → ClassName (N)
/// ```
pub fn format_data_model_markdown(alias: &str, info: &JsonValue) -> String {
    let mut lines = vec![
        format!("# Data Model Overview for \"{}\"", alias),
        String::new(),
    ];

    // Note: Prefixes section intentionally omitted.
    // Future: accept prefixes as a parameter if the client wants them included.

    // Dataset statistics section
    if let Some(stats) = info.get("stats").and_then(|v| v.as_object()) {
        lines.push("## Dataset Statistics".to_string());

        // Count classes
        if let Some(classes) = stats.get("classes").and_then(|v| v.as_object()) {
            lines.push(format!("- Classes: {}", classes.len()));

            // Total instances (sum of all class counts)
            let total_instances: i64 = classes
                .values()
                .filter_map(|c| c.get("count").and_then(serde_json::Value::as_i64))
                .sum();
            lines.push(format!(
                "- Total instances: {}",
                format_number(total_instances)
            ));
        }

        // Count properties
        if let Some(properties) = stats.get("properties").and_then(|v| v.as_object()) {
            lines.push(format!("- Properties: {}", properties.len()));
        }

        // Flakes and size
        if let Some(flakes) = stats.get("flakes").and_then(serde_json::Value::as_i64) {
            lines.push(format!("- Triples (flakes): {}", format_number(flakes)));
        }
        if let Some(size) = stats.get("size").and_then(serde_json::Value::as_i64) {
            lines.push(format!("- Size: {}", format_bytes(size)));
        }

        lines.push(String::new());

        // Classes section (limited to avoid unbounded output)
        const MAX_CLASSES: usize = 50;
        const MAX_PROPERTIES_PER_CLASS: usize = 20;

        if let Some(classes) = stats.get("classes").and_then(|v| v.as_object()) {
            lines.push("## Classes".to_string());
            lines.push(String::new());

            // Sort classes by instance count descending
            let mut class_vec: Vec<_> = classes.iter().collect();
            class_vec.sort_by(|a, b| {
                let count_a =
                    a.1.get("count")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or(0);
                let count_b =
                    b.1.get("count")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or(0);
                count_b.cmp(&count_a)
            });

            let total_classes = class_vec.len();
            for (class_iri, class_data) in class_vec.iter().take(MAX_CLASSES) {
                format_class(&mut lines, class_iri, class_data, MAX_PROPERTIES_PER_CLASS);
            }

            if total_classes > MAX_CLASSES {
                lines.push(format!(
                    "... and {} more classes (showing top {} by instance count)",
                    total_classes - MAX_CLASSES,
                    MAX_CLASSES
                ));
                lines.push(String::new());
            }
        }
    }

    lines.join("\n")
}

/// Format a single class and its properties
fn format_class(
    lines: &mut Vec<String>,
    class_iri: &str,
    class_data: &JsonValue,
    max_properties: usize,
) {
    let count = class_data
        .get("count")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);

    lines.push(format!(
        "### {} — {} instances",
        class_iri,
        format_number(count)
    ));

    // Subclass relationships
    if let Some(subclass_of) = class_data.get("subclass-of").and_then(|v| v.as_array()) {
        if !subclass_of.is_empty() {
            let parents: Vec<_> = subclass_of.iter().filter_map(|v| v.as_str()).collect();
            if !parents.is_empty() {
                lines.push(format!("Subclass of: {}", parents.join(", ")));
            }
        }
    }

    lines.push(String::new());

    // Properties for this class (limited)
    if let Some(properties) = class_data.get("properties").and_then(|v| v.as_object()) {
        if !properties.is_empty() {
            lines.push("Properties:".to_string());

            // Sort properties by count descending
            let mut prop_vec: Vec<_> = properties.iter().collect();
            prop_vec.sort_by(|a, b| {
                let count_a = get_property_count(a.1);
                let count_b = get_property_count(b.1);
                count_b.cmp(&count_a)
            });

            let total_props = prop_vec.len();
            for (i, (prop_iri, prop_data)) in prop_vec.iter().take(max_properties).enumerate() {
                format_property(lines, i + 1, prop_iri, prop_data);
            }

            if total_props > max_properties {
                lines.push(format!(
                    "   ... and {} more properties",
                    total_props - max_properties
                ));
            }
        }
    }

    lines.push(String::new());
}

/// Get total count for a property (sum of all type counts)
fn get_property_count(prop_data: &JsonValue) -> i64 {
    let types_count: i64 = prop_data
        .get("types")
        .and_then(|v| v.as_object())
        .map(|types| types.values().filter_map(serde_json::Value::as_i64).sum())
        .unwrap_or(0);

    let refs_count: i64 = prop_data
        .get("ref-classes")
        .and_then(|v| v.as_object())
        .map(|refs| refs.values().filter_map(serde_json::Value::as_i64).sum())
        .unwrap_or(0);

    types_count + refs_count
}

/// Format a single property within a class
fn format_property(lines: &mut Vec<String>, index: usize, prop_iri: &str, prop_data: &JsonValue) {
    lines.push(format!("{index}. {prop_iri}"));

    let mut type_info: Vec<String> = Vec::new();

    // Data types
    if let Some(types) = prop_data.get("types").and_then(|v| v.as_object()) {
        for (dtype, count) in types {
            if let Some(c) = count.as_i64() {
                type_info.push(format!("{} ({})", dtype, format_number(c)));
            }
        }
    }

    // Reference classes
    let refs_obj = prop_data
        .get("ref-classes")
        .or_else(|| prop_data.get("refs"))
        .and_then(|v| v.as_object());
    if let Some(refs) = refs_obj {
        for (ref_class, count) in refs {
            if let Some(c) = count.as_i64() {
                type_info.push(format!("→ {} ({})", ref_class, format_number(c)));
            }
        }
    }

    // Language tags
    if let Some(langs) = prop_data.get("langs").and_then(|v| v.as_object()) {
        let lang_info: Vec<_> = langs
            .iter()
            .filter_map(|(lang, count)| count.as_i64().map(|c| format!("@{lang} ({c})")))
            .collect();
        if !lang_info.is_empty() {
            type_info.push(format!("langString: {}", lang_info.join(", ")));
        }
    }

    if !type_info.is_empty() {
        lines.push(format!("   Type: {}", type_info.join(", ")));
    }
}

/// Format a number with comma separators
fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();

    for (count, c) in s.chars().rev().enumerate() {
        if count > 0 && count % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }

    result.chars().rev().collect()
}

/// Format bytes in human-readable form
fn format_bytes(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = KB * 1024;
    const GB: i64 = MB * 1024;

    if bytes < KB {
        format!("{bytes} B")
    } else if bytes < MB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else if bytes < GB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1_000_000), "1,000,000");
        assert_eq!(format_number(1_234_567_890), "1,234,567,890");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
    }

    #[test]
    fn test_format_data_model_basic() {
        let info = json!({
            "stats": {
                "flakes": 1234,
                "size": 56789,
                "classes": {
                    "http://example.org/Person": {
                        "count": 100,
                        "properties": {
                            "http://example.org/name": {
                                "types": {
                                    "http://www.w3.org/2001/XMLSchema#string": 100
                                }
                            }
                        }
                    }
                },
                "properties": {
                    "http://example.org/name": {
                        "count": 100
                    }
                }
            }
        });

        let markdown = format_data_model_markdown("test:main", &info);

        assert!(markdown.contains("# Data Model Overview for \"test:main\""));
        // Prefixes section intentionally omitted
        assert!(!markdown.contains("## Prefixes"));
        assert!(markdown.contains("## Dataset Statistics"));
        assert!(markdown.contains("- Classes: 1"));
        assert!(markdown.contains("- Total instances: 100"));
        assert!(markdown.contains("- Triples (flakes): 1,234"));
        assert!(markdown.contains("## Classes"));
        assert!(markdown.contains("### http://example.org/Person — 100 instances"));
        assert!(markdown.contains("1. http://example.org/name"));
    }
}
