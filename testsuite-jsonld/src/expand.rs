//! Expand tests: the expander's output against `expect`, by JSON-LD object
//! comparison (tests/README.md).

use anyhow::{bail, Result};
use fluree_graph_json_ld::{expand_with_context, parse_context_with_base, ParsedContext};
use serde_json::Value;

use crate::manifest::{Kind, TestCase};
use crate::read_json;

/// The context a test's input is expanded against: the document base, then
/// `option.expandContext`. The permissive expander is the spec algorithm;
/// the transaction parser's strict mode additionally refuses a compact-looking
/// IRI whose prefix is undefined.
pub fn initial_context(test: &TestCase) -> Result<ParsedContext> {
    let initial = ParsedContext {
        base: Some(test.base().to_string()),
        ..ParsedContext::new()
    };
    match test.expand_context() {
        None => Ok(initial),
        Some(path) => Ok(parse_context_with_base(&initial, &read_json(&path)?)?),
    }
}

/// The input, expanded. `expand_with_context` is the Expansion Algorithm;
/// the readers that call it unwrap its result themselves, so this applies
/// the steps `JsonLdProcessor.expand()` adds (JSON-LD 1.1 API §9.1, 8.1-8.3).
pub fn expanded_input(test: &TestCase) -> Result<Value> {
    let input = read_json(&test.input)?;
    let context = initial_context(test)?;
    let output = match expand_with_context(&input, &context)? {
        Value::Object(mut map) if map.len() == 1 && map.contains_key("@graph") => {
            map.remove("@graph").expect("checked")
        }
        other => other,
    };
    Ok(match output {
        Value::Array(items) => Value::Array(items),
        Value::Null => Value::Array(vec![]),
        node => Value::Array(vec![node]),
    })
}

pub fn run(test: &TestCase) -> Result<()> {
    let result = expanded_input(test);
    match test.kind {
        Kind::Negative => match result {
            Ok(output) => bail!(
                "expected the error '{}', got {}",
                test.expect_error_code.as_deref().unwrap_or("?"),
                serde_json::to_string(&output)?
            ),
            Err(_) => Ok(()),
        },
        Kind::PositiveSyntax => result.map(drop),
        Kind::Positive => {
            let output = result?;
            let expected = read_json(test.expect.as_ref().expect("positive test has expect"))?;
            if equivalent(&output, &expected, false) {
                Ok(())
            } else {
                bail!(
                    "output differs\n  expected: {}\n  actual:   {}",
                    serde_json::to_string(&expected)?,
                    serde_json::to_string(&output)?
                )
            }
        }
    }
}

/// JSON-LD object comparison: members regardless of order, arrays regardless
/// of order except a `@list`'s, scalars strictly, language tags regardless of
/// case.
pub fn equivalent(a: &Value, b: &Value, ordered: bool) -> bool {
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter().all(|(key, va)| {
                    b.get(key).is_some_and(|vb| match (key.as_str(), va, vb) {
                        ("@language", Value::String(x), Value::String(y)) => {
                            x.eq_ignore_ascii_case(y)
                        }
                        _ => equivalent(va, vb, key == "@list"),
                    })
                })
        }
        (Value::Array(a), Value::Array(b)) if ordered => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| equivalent(x, y, false))
        }
        (Value::Array(a), Value::Array(b)) => {
            // Equivalence is an equivalence relation, so matching each item to
            // the first unused equivalent one cannot miss a pairing.
            let mut used = vec![false; b.len()];
            a.len() == b.len()
                && a.iter().all(|x| {
                    let found = b
                        .iter()
                        .enumerate()
                        .find(|(i, y)| !used[*i] && equivalent(x, y, false));
                    found.map(|(i, _)| used[i] = true).is_some()
                })
        }
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use super::equivalent;
    use serde_json::json;

    #[test]
    fn arrays_compare_without_order_except_a_list() {
        let a = json!([{"p": [{"@value": 1}, {"@value": 2}]}]);
        let b = json!([{"p": [{"@value": 2}, {"@value": 1}]}]);
        assert!(equivalent(&a, &b, false));
        let la = json!([{"p": [{"@list": [{"@value": 1}, {"@value": 2}]}]}]);
        let lb = json!([{"p": [{"@list": [{"@value": 2}, {"@value": 1}]}]}]);
        assert!(!equivalent(&la, &lb, false));
    }

    #[test]
    fn duplicates_must_match_one_for_one() {
        let a = json!([{"@value": 1}, {"@value": 1}]);
        let b = json!([{"@value": 1}, {"@value": 2}]);
        assert!(!equivalent(&a, &b, false));
    }

    #[test]
    fn language_tags_compare_without_case_and_numbers_strictly() {
        assert!(equivalent(
            &json!({"@value": "x", "@language": "en-US"}),
            &json!({"@value": "x", "@language": "en-us"}),
            false
        ));
        assert!(!equivalent(
            &json!({"@value": 1}),
            &json!({"@value": 1.0}),
            false
        ));
    }
}
