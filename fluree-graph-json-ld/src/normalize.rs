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
    normalize_value(data)
}

fn normalize_value(data: &JsonValue) -> String {
    match data {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        JsonValue::Number(n) => normalize_number(n),
        JsonValue::String(s) => format!("\"{}\"", escape_string(s)),
        JsonValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(normalize_value).collect();
            format!("[{}]", items.join(","))
        }
        JsonValue::Object(map) => {
            // Sort keys lexicographically (by UTF-16 code units as per RFC 8785)
            let mut pairs: Vec<(&String, &JsonValue)> = map.iter().collect();
            pairs.sort_by(|a, b| compare_strings(a.0, b.0));

            let items: Vec<String> = pairs
                .iter()
                .map(|(k, v)| format!("\"{}\":{}", escape_string(k), normalize_value(v)))
                .collect();
            format!("{{{}}}", items.join(","))
        }
    }
}

/// Compare strings by UTF-16 code units (RFC 8785 requirement)
fn compare_strings(a: &str, b: &str) -> std::cmp::Ordering {
    let a_codes: Vec<u16> = a.encode_utf16().collect();
    let b_codes: Vec<u16> = b.encode_utf16().collect();
    a_codes.cmp(&b_codes)
}

/// Normalize a number according to RFC 8785
fn normalize_number(n: &serde_json::Number) -> String {
    // Try to get as i64 first (integers)
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }

    // Try to get as u64
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }

    // Must be a float
    if let Some(f) = n.as_f64() {
        return normalize_float(f);
    }

    // Fallback
    n.to_string()
}

/// Normalize a floating point number according to RFC 8785
fn normalize_float(f: f64) -> String {
    // Handle special cases
    if f == 0.0 {
        return "0".to_string();
    }

    // Check if it's actually an integer
    if f.fract() == 0.0 && f.abs() < 1e15 {
        return (f as i64).to_string();
    }

    // Use scientific notation for very large or very small numbers
    let abs = f.abs();
    if abs >= 1e21 || (abs < 1e-6 && abs != 0.0) {
        // Use exponential notation
        let exp_str = format!("{f:e}");
        return normalize_exponential(&exp_str);
    }

    // Regular decimal representation
    let s = format!("{f}");

    // Remove unnecessary trailing zeros after decimal point
    // but keep at least one digit after decimal if needed
    if s.contains('.') && !s.contains('e') && !s.contains('E') {
        let trimmed = s.trim_end_matches('0');
        if trimmed.ends_with('.') {
            // Was like "5.0" -> "5"
            return trimmed.trim_end_matches('.').to_string();
        }
        return trimmed.to_string();
    }

    s
}

/// Normalize exponential notation
fn normalize_exponential(s: &str) -> String {
    // Parse the exponential string
    let parts: Vec<&str> = s.split(['e', 'E']).collect();
    if parts.len() != 2 {
        return s.to_string();
    }

    let mantissa = parts[0];
    let exponent: i32 = parts[1].parse().unwrap_or(0);

    // Normalize mantissa (remove trailing zeros after decimal)
    let norm_mantissa = if mantissa.contains('.') {
        let trimmed = mantissa.trim_end_matches('0');
        if trimmed.ends_with('.') {
            trimmed.trim_end_matches('.').to_string()
        } else {
            trimmed.to_string()
        }
    } else {
        mantissa.to_string()
    };

    // Format exponent with sign
    if exponent >= 0 {
        format!("{norm_mantissa}e+{exponent}")
    } else {
        format!("{norm_mantissa}e{exponent}")
    }
}

/// Escape a string according to JSON rules
fn escape_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            '\u{0008}' => result.push_str("\\b"), // backspace
            '\u{000C}' => result.push_str("\\f"), // form feed
            c if c < '\u{0020}' => {
                // Other control characters as \uXXXX
                result.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => result.push(c),
        }
    }
    result
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
        // 56.0 should become 56
        assert_eq!(normalize_float(56.0), "56");
        assert_eq!(normalize_float(5.0), "5");
        assert_eq!(normalize_float(-10.0), "-10");
    }

    #[test]
    fn test_trim_trailing_zeros() {
        assert_eq!(normalize_float(4.5), "4.5");
        assert_eq!(normalize_float(0.002), "0.002");
    }

    #[test]
    fn test_exponential_notation() {
        // Very large numbers
        let result = normalize_float(1e30);
        assert_eq!(result, "1e+30");

        // Very small numbers
        let result = normalize_float(1e-27);
        assert_eq!(result, "1e-27");
    }
}
