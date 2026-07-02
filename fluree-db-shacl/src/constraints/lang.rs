//! Language-tag constraint validators (sh:uniqueLang, sh:languageIn)
//!
//! Language tags live in flake metadata (`FlakeMeta::lang`), threaded into
//! validation as a `langs` slice parallel to `values` / `datatypes`.

use super::{Constraint, ConstraintViolation};
use fluree_db_core::FlakeValue;
use std::collections::HashMap;

/// Validate sh:uniqueLang: no two values may share a language tag.
///
/// Values without a language tag are ignored (the constraint only concerns
/// language-tagged literals). Returns one violation per duplicated tag.
pub fn validate_unique_lang(
    values: &[FlakeValue],
    langs: &[Option<String>],
) -> Vec<ConstraintViolation> {
    // BCP 47 language tags are case-insensitive ("en" and "EN" collide).
    let mut counts: HashMap<String, usize> = HashMap::new();
    for lang in langs.iter().flatten() {
        *counts.entry(lang.to_ascii_lowercase()).or_default() += 1;
    }

    let mut out = Vec::new();
    let mut duplicated: Vec<String> = counts
        .into_iter()
        .filter(|(_, n)| *n > 1)
        .map(|(lang, _)| lang)
        .collect();
    duplicated.sort_unstable();
    for lang in duplicated {
        // Report the first value carrying the duplicated tag.
        let index = langs
            .iter()
            .position(|l| l.as_deref().is_some_and(|t| t.eq_ignore_ascii_case(&lang)));
        out.push(ConstraintViolation {
            constraint: Constraint::UniqueLang(true),
            value: index.and_then(|i| values.get(i)).cloned(),
            value_index: index,
            message: format!("Language tag \"{lang}\" is used by more than one value"),
        });
    }
    out
}

/// Validate sh:languageIn for a single value: the value must be a
/// language-tagged literal whose tag matches one of the allowed basic
/// language ranges.
pub fn validate_language_in(
    value: &FlakeValue,
    lang: Option<&str>,
    allowed: &[String],
) -> Option<ConstraintViolation> {
    let violation = |message: String| {
        Some(ConstraintViolation {
            constraint: Constraint::LanguageIn(allowed.to_vec()),
            value: Some(value.clone()),
            value_index: None,
            message,
        })
    };

    let Some(lang) = lang else {
        return violation(format!(
            "Value {value:?} has no language tag (sh:languageIn requires one of {allowed:?})"
        ));
    };
    if allowed.iter().any(|range| lang_matches(lang, range)) {
        None
    } else {
        violation(format!(
            "Language tag \"{lang}\" is not in the allowed set {allowed:?}"
        ))
    }
}

/// SPARQL `langMatches` basic filtering (RFC 4647 §3.3.1): case-insensitive;
/// the range matches the tag exactly or as a prefix followed by `-`, and `*`
/// matches any tag.
fn lang_matches(tag: &str, range: &str) -> bool {
    if range == "*" {
        return !tag.is_empty();
    }
    let tag = tag.to_ascii_lowercase();
    let range = range.to_ascii_lowercase();
    tag == range
        || (tag.len() > range.len()
            && tag.starts_with(&range)
            && tag.as_bytes()[range.len()] == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> FlakeValue {
        FlakeValue::String(v.to_string())
    }

    #[test]
    fn unique_lang_flags_duplicates() {
        let values = [s("colour"), s("color"), s("couleur")];
        let langs = [
            Some("en".to_string()),
            Some("en".to_string()),
            Some("fr".to_string()),
        ];
        let violations = validate_unique_lang(&values, &langs);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("\"en\""));
    }

    #[test]
    fn unique_lang_is_case_insensitive() {
        let values = [s("colour"), s("color")];
        let langs = [Some("en".to_string()), Some("EN".to_string())];
        assert_eq!(validate_unique_lang(&values, &langs).len(), 1);
    }

    #[test]
    fn unique_lang_ignores_untagged_values() {
        let values = [s("a"), s("b")];
        let langs = [None, None];
        assert!(validate_unique_lang(&values, &langs).is_empty());
    }

    #[test]
    fn language_in_matches_exact_and_subtag() {
        let allowed = vec!["en".to_string(), "fr".to_string()];
        assert!(validate_language_in(&s("hi"), Some("en"), &allowed).is_none());
        // Basic language range: "en" matches "en-US".
        assert!(validate_language_in(&s("hi"), Some("en-US"), &allowed).is_none());
        assert!(validate_language_in(&s("hallo"), Some("de"), &allowed).is_some());
        // Prefix without a subtag separator must NOT match.
        assert!(validate_language_in(&s("hi"), Some("eng"), &allowed).is_some());
    }

    #[test]
    fn language_in_rejects_untagged_values() {
        let allowed = vec!["en".to_string()];
        assert!(validate_language_in(&s("plain"), None, &allowed).is_some());
    }
}
