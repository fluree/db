//! The shape of a JSON-LD-star triple term, `{"@id": {"@id": s, p: o}}`,
//! shared by every surface that reads one; each lowers the parts its own way.

use serde_json::{Map, Value};

/// A triple term's parts: the subject's `@id` value, its one predicate, and
/// that predicate's one object.
pub struct TripleTermParts<'a> {
    pub subject: &'a Value,
    pub predicate: &'a str,
    pub object: &'a Value,
}

/// Split the node inside a triple term's `@id` into its parts. The node
/// describes exactly one triple: keywords aside, one predicate with one
/// object, which is a reference, a value or a nested term, never a node with
/// properties of its own (those would be asserted). The error is the reason.
pub fn triple_term_parts(term: &Map<String, Value>) -> Result<TripleTermParts<'_>, &'static str> {
    let subject = term.get("@id").ok_or("@id must name the subject")?;
    let mut pairs = term.iter().filter(|(k, _)| !k.starts_with('@'));
    let (Some((predicate, object)), None) = (pairs.next(), pairs.next()) else {
        return Err("it must describe exactly one triple");
    };
    let object = match object {
        Value::Array(items) if items.len() == 1 => &items[0],
        Value::Array(_) => return Err("it must describe exactly one triple"),
        object => object,
    };
    if let Value::Object(o) = object {
        if !(o.contains_key("@value") || (o.len() == 1 && o.contains_key("@id"))) {
            return Err("its object must be a reference, a value or a triple term");
        }
    }
    Ok(TripleTermParts {
        subject,
        predicate,
        object,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parts(v: Value) -> Result<(String, String), &'static str> {
        let Value::Object(map) = v else {
            unreachable!()
        };
        triple_term_parts(&map).map(|p| (p.predicate.to_string(), p.object.to_string()))
    }

    #[test]
    fn one_predicate_with_one_object() {
        assert_eq!(
            parts(json!({"@id": "ex:s", "ex:p": [{"@id": "ex:o"}]})).unwrap(),
            ("ex:p".to_string(), r#"{"@id":"ex:o"}"#.to_string())
        );
        assert!(parts(json!({"@id": "ex:s", "ex:p": "v", "@type": "ex:T"})).is_ok());
        assert!(parts(json!({"@id": "ex:s", "ex:p": {"@id": {"@id": "ex:a", "ex:q": 1}}})).is_ok());
        for bad in [
            json!({"ex:p": "v"}),
            json!({"@id": "ex:s"}),
            json!({"@id": "ex:s", "ex:p": "v", "ex:q": "w"}),
            json!({"@id": "ex:s", "ex:p": ["v", "w"]}),
            json!({"@id": "ex:s", "ex:p": {"@id": "ex:o", "ex:q": "w"}}),
            json!({"@id": "ex:s", "ex:p": {"@list": ["v"]}}),
        ] {
            assert!(parts(bad.clone()).is_err(), "{bad}");
        }
    }
}
