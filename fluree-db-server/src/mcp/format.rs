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

            // Total instances (sum of all class counts). A virtual dataset
            // reports `count: null` for a class it cannot count exactly; the
            // total then covers the counted classes only, and says so.
            let total_instances: i64 = classes
                .values()
                .filter_map(|c| c.get("count").and_then(serde_json::Value::as_i64))
                .sum();
            let uncounted = classes
                .values()
                .filter(|c| c.get("count").and_then(serde_json::Value::as_i64).is_none())
                .count();
            if uncounted == 0 {
                lines.push(format!(
                    "- Total instances: {}",
                    format_number(total_instances)
                ));
            } else {
                lines.push(format!(
                    "- Total instances: {} (counted classes only; {uncounted} not counted exactly)",
                    format_number(total_instances)
                ));
            }
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

        // Virtual (query-in-place) datasets derive their counts from source table
        // metadata. A count is shown only where the metadata proves it exact;
        // elsewhere the reading model sees an upper bound ("at most") or
        // "not counted". Only emitted for virtual sources — native output stays
        // byte-identical.
        if is_virtual_source(info) {
            lines.push(String::new());
            lines.push(
                "> Note: This is a virtual (query-in-place) dataset. Counts come from \
                 source table metadata: a number is exact; \"at most N\" is an upper bound \
                 where the metadata cannot prove the exact count (the true count may be \
                 lower); \"not counted\" means no bound is known either."
                    .to_string(),
            );
        }

        lines.push(String::new());

        // Classes section (limited to avoid unbounded output)
        const MAX_CLASSES: usize = 50;
        const MAX_PROPERTIES_PER_CLASS: usize = 20;

        if let Some(classes) = stats.get("classes").and_then(|v| v.as_object()) {
            lines.push("## Classes".to_string());
            lines.push(String::new());

            // Sort classes by instance count descending (an upper bound where
            // the count is not known exactly)
            let mut class_vec: Vec<_> = classes.iter().collect();
            class_vec.sort_by(|a, b| {
                let count_a = count_or_bound(a.1).unwrap_or(0);
                let count_b = count_or_bound(b.1).unwrap_or(0);
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

    // Degenerate config state (ledger info's `configDiagnostics`), such as
    // shapes no config enforces: worth knowing before writing or querying.
    if let Some(diagnostics) = info
        .get("configDiagnostics")
        .and_then(|v| v.as_array())
        .filter(|d| !d.is_empty())
    {
        lines.push("## Config Diagnostics".to_string());
        for diagnostic in diagnostics {
            if let Some(message) = diagnostic.get("message").and_then(|m| m.as_str()) {
                lines.push(format!("- {message}"));
            }
        }
        lines.push(String::new());
    }

    lines.join("\n")
}

/// Detect a virtual (query-in-place) dataset from the `/info` `source` block that
/// `build_virtual_ledger_info` emits. Native `/info` omits `source` entirely, so a
/// present `source.virtual == true` (or `source.type == "Iceberg"`) is the signal
/// that the reported instance/property counts are coarse metadata estimates.
fn is_virtual_source(info: &JsonValue) -> bool {
    info.get("source").is_some_and(|source| {
        source.get("virtual").and_then(JsonValue::as_bool) == Some(true)
            || source.get("type").and_then(JsonValue::as_str) == Some("Iceberg")
    })
}

/// An entry's `count`, else its `count-upper-bound` (a virtual count that is
/// not known exactly).
fn count_or_bound(entry: &JsonValue) -> Option<i64> {
    entry
        .get("count")
        .and_then(JsonValue::as_i64)
        .or_else(|| entry.get("count-upper-bound").and_then(JsonValue::as_i64))
}

/// A class's instance count as text: the number when known, else "at most
/// `bound`", else "not counted". A native count is always known.
fn describe_instances(count: Option<i64>, bound: Option<i64>) -> String {
    match (count, bound) {
        (Some(c), _) => format!("{} instances", format_number(c)),
        (None, Some(b)) => format!("at most {} instances", format_number(b)),
        (None, None) => "instances not counted".to_string(),
    }
}

/// Format a single class and its properties
fn format_class(
    lines: &mut Vec<String>,
    class_iri: &str,
    class_data: &JsonValue,
    max_properties: usize,
) {
    let count = class_data.get("count").and_then(JsonValue::as_i64);
    let bound = class_data
        .get("count-upper-bound")
        .and_then(JsonValue::as_i64);

    lines.push(format!(
        "### {} — {}",
        class_iri,
        describe_instances(count, bound)
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

    // Data types (a virtual count that is not known exactly is `null`: the
    // datatype is still listed)
    if let Some(types) = prop_data.get("types").and_then(|v| v.as_object()) {
        for (dtype, count) in types {
            match count.as_i64() {
                Some(c) => type_info.push(format!("{} ({})", dtype, format_number(c))),
                None => type_info.push(dtype.clone()),
            }
        }
    }

    // Reference classes (a virtual reference is never counted exactly: its
    // entry is `null`, with a bound in `ref-classes-upper-bound` when it has one)
    let refs_obj = prop_data
        .get("ref-classes")
        .or_else(|| prop_data.get("refs"))
        .and_then(|v| v.as_object());
    let ref_bounds = prop_data
        .get("ref-classes-upper-bound")
        .and_then(|v| v.as_object());
    if let Some(refs) = refs_obj {
        for (ref_class, count) in refs {
            let bound = ref_bounds
                .and_then(|b| b.get(ref_class))
                .and_then(JsonValue::as_i64);
            match (count.as_i64(), bound) {
                (Some(c), _) => {
                    type_info.push(format!("→ {} ({})", ref_class, format_number(c)));
                }
                (None, Some(b)) => {
                    type_info.push(format!("→ {} (at most {})", ref_class, format_number(b)));
                }
                (None, None) => type_info.push(format!("→ {ref_class}")),
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
        // Native (non-virtual) info has no `source` block, so no virtual
        // counts note is emitted — native markdown stays byte-identical.
        assert!(!markdown.contains("virtual (query-in-place) dataset"));
    }

    #[test]
    fn test_format_data_model_config_diagnostics() {
        let info = json!({
            "stats": {"flakes": 1},
            "configDiagnostics": [{
                "code": "shacl-not-configured",
                "message": "shapes present; SHACL enforcement not configured"
            }]
        });
        let markdown = format_data_model_markdown("test:main", &info);
        assert!(markdown.contains("## Config Diagnostics"), "{markdown}");
        assert!(
            markdown.contains("- shapes present; SHACL enforcement not configured"),
            "{markdown}"
        );
        assert!(
            !format_data_model_markdown("test:main", &json!({"stats": {"flakes": 1}}))
                .contains("## Config Diagnostics")
        );
    }

    #[test]
    fn test_format_data_model_virtual_note() {
        // A virtual dataset carries a `source` block with `virtual: true`.
        let info = json!({
            "source": { "virtual": true, "type": "Iceberg" },
            "stats": {
                "flakes": 150,
                "classes": {
                    "http://ex.org/Airline": { "count": 100, "properties": {} }
                },
                "properties": {
                    "http://ex.org/name": { "count": 100 }
                }
            }
        });

        let markdown = format_data_model_markdown("sales:main", &info);

        // The note on how to read virtual counts appears within the
        // statistics section.
        assert!(
            markdown.contains("\"at most N\" is an upper bound"),
            "virtual dataset markdown missing the counts note:\n{markdown}"
        );
        assert!(markdown.contains("virtual (query-in-place) dataset"));
    }

    #[test]
    fn test_format_data_model_virtual_counts_not_known_exactly() {
        let info = json!({
            "source": { "virtual": true, "type": "Iceberg" },
            "stats": {
                "classes": {
                    "http://ex.org/Customer": {
                        "count": null,
                        "count-upper-bound": 30,
                        "properties": {
                            "http://ex.org/name": { "types": { "xsd:string": null } }
                        }
                    },
                    "http://ex.org/Order": {
                        "count": 7,
                        "properties": {
                            "http://ex.org/customer": {
                                "types": { "@id": null },
                                "ref-classes": { "http://ex.org/Customer": null },
                                "ref-classes-upper-bound": { "http://ex.org/Customer": 7 }
                            }
                        }
                    },
                    "http://ex.org/Tag": { "count": null, "properties": {} }
                }
            }
        });
        let markdown = format_data_model_markdown("crm:main", &info);
        for needle in [
            "### http://ex.org/Customer — at most 30 instances",
            "### http://ex.org/Order — 7 instances",
            "### http://ex.org/Tag — instances not counted",
            "- Total instances: 7 (counted classes only; 2 not counted exactly)",
            "xsd:string",
            "→ http://ex.org/Customer (at most 7)",
        ] {
            assert!(markdown.contains(needle), "missing {needle:?}:\n{markdown}");
        }
    }

    #[test]
    fn test_is_virtual_source() {
        assert!(is_virtual_source(&json!({ "source": { "virtual": true } })));
        assert!(is_virtual_source(
            &json!({ "source": { "type": "Iceberg" } })
        ));
        // Native `/info` omits `source` entirely.
        assert!(!is_virtual_source(&json!({ "stats": {} })));
        assert!(!is_virtual_source(&json!({ "source": null })));
    }
}
