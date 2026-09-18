use serde_json::Value as JsonValue;

/// Options for normalization
#[derive(Debug, Clone, Default)]
pub struct NormalizeOptions {
    pub algorithm: Algorithm,
    pub format: Format,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub enum Algorithm {
    #[default]
    Basic, // RFC 8785
}

#[derive(Debug, Clone, Default, PartialEq)]
pub enum Format {
    #[default]
    ApplicationJson,
}

/// Normalize JSON data to canonical form (RFC 8785)
pub fn normalize(data: &JsonValue) -> String {
    normalize_with_options(data, &NormalizeOptions::default())
}

/// Normalize with options
pub fn normalize_with_options(data: &JsonValue, _opts: &NormalizeOptions) -> String {
    // Every ingest path shares one implementation of the scheme. Two that
    // disagreed anywhere would split one value into two terms.
    fluree_graph_ir::canonicalize_json_value(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_normalize_map_sorting() {
        let data = json!({
            "peach": "This sorting order",
            "péché": "is wrong according to French",
            "pêche": "but canonicalization MUST",
            "sin": "ignore locale"
        });

        let result = normalize(&data);
        assert_eq!(
            result,
            r#"{"peach":"This sorting order","péché":"is wrong according to French","pêche":"but canonicalization MUST","sin":"ignore locale"}"#
        );
    }

    #[test]
    fn test_normalize_nested_with_newline() {
        let data = json!({
            "1": {"f": {"f": "hi", "F": 5}, "\n": 56.0},
            "10": {},
            "": "empty",
            "a": {},
            "111": [{"e": "yes", "E": "no"}],
            "A": {}
        });

        let result = normalize(&data);
        assert_eq!(
            result,
            r#"{"":"empty","1":{"\n":56,"f":{"F":5,"f":"hi"}},"10":{},"111":[{"E":"no","e":"yes"}],"A":{},"a":{}}"#
        );
    }

    #[test]
    fn test_normalize_unicode() {
        let data = json!({"Unnormalized Unicode": "A\u{030a}"});
        let result = normalize(&data);
        // RFC 8785 does not require Unicode normalization, so combining characters stay as-is
        assert_eq!(result, "{\"Unnormalized Unicode\":\"A\u{030a}\"}");
    }

    #[test]
    fn test_normalize_numbers_and_literals() {
        let data = json!({
            "numbers": [333_333_333.333_333_3, 1E30, 4.50, 2e-3, 0.000_000_000_000_000_000_000_000_001],
            "literals": [null, true, false]
        });

        let result = normalize(&data);
        assert_eq!(
            result,
            r#"{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27]}"#
        );
    }

    #[test]
    fn test_normalize_sequence() {
        let data = json!([56, {"d": true, "10": null, "1": []}]);
        let result = normalize(&data);
        assert_eq!(result, r#"[56,{"1":[],"10":null,"d":true}]"#);
    }

    #[test]
    fn test_integer_from_float() {
        assert_eq!(normalize(&json!([56.0, 5.0, -10.0])), "[56,5,-10]");
    }

    #[test]
    fn test_trim_trailing_zeros() {
        assert_eq!(normalize(&json!([4.50, 0.002])), "[4.5,0.002]");
    }

    #[test]
    fn test_exponential_notation() {
        assert_eq!(normalize(&json!([1e30, 1e-27])), "[1e+30,1e-27]");
    }
}
