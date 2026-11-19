//! Names in words: what a label made from an IRI or a column name reads as.

/// An IRI's local name: the part after its last `#`, `/` or `:`, trailing
/// separators ignored.
pub fn local_name(iri: &str) -> &str {
    let trimmed = iri.trim_end_matches(['#', '/', ':']);
    trimmed.rsplit(['#', '/', ':']).next().unwrap_or(trimmed)
}

/// An IRI's local name in words, as the input policy makes a missing label:
/// `employer_id` and `employerId` read "employer id", `DAMAGE_REPORTS`
/// "damage reports", `hasLEI` "has LEI". No suffix is removed and nothing is
/// inferred. `None` when the local name has no letters, which the policy
/// cannot make a label from.
pub fn term_words(iri: &str) -> Option<String> {
    name_words(local_name(iri))
}

/// A name (a local name, a table or column name) in words, by the rules of
/// [`term_words`].
pub fn name_words(name: &str) -> Option<String> {
    let chars: Vec<char> = name.chars().collect();
    let mut spaced = String::with_capacity(name.len() + 8);
    for (i, &c) in chars.iter().enumerate() {
        if i > 0 && c.is_ascii_uppercase() {
            let prev = chars[i - 1];
            // aB, 1B: a lower-case letter or digit before a capital
            let camel = prev.is_ascii_lowercase() || prev.is_ascii_digit();
            // ABc: the last capital of a run, before a lower-case letter
            let acronym_end =
                prev.is_ascii_uppercase() && chars.get(i + 1).is_some_and(char::is_ascii_lowercase);
            if camel || acronym_end {
                spaced.push(' ');
            }
        }
        spaced.push(c);
    }
    let all_upper = is_upper(&spaced);
    let words: Vec<String> = spaced
        .split(|c: char| c.is_whitespace() || c == '_' || c == '-' || c == '.')
        .filter(|w| !w.is_empty())
        .map(|w| {
            if w.chars().count() > 1 && is_upper(w) && !all_upper {
                w.to_string()
            } else {
                w.to_lowercase()
            }
        })
        .collect();
    let text = words.join(" ");
    text.chars().any(char::is_alphabetic).then_some(text)
}

/// Python's `str.isupper`: at least one cased character, and every cased
/// character upper case.
fn is_upper(s: &str) -> bool {
    let mut cased = false;
    for c in s.chars() {
        if c.is_lowercase() {
            return false;
        }
        if c.is_uppercase() {
            cased = true;
        }
    }
    cased
}

/// Whether a label reads as an identifier rather than words: it has an
/// underscore, or camel case, or ends in "id" as a column name does.
pub fn reads_as_identifier(label: &str) -> bool {
    let has_camel = label
        .chars()
        .zip(label.chars().skip(1))
        .any(|(a, b)| a.is_ascii_lowercase() && b.is_ascii_uppercase());
    label.contains('_') || has_camel || ends_with_id(label)
}

/// Whether a name in words ends with the word "id" (`employer id`), as a
/// key column does.
pub fn ends_with_id(text: &str) -> bool {
    let words = name_words(text).unwrap_or_default();
    let mut it = words.split(' ').rev();
    matches!(it.next(), Some(last) if last.eq_ignore_ascii_case("id")) && it.next().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_examples() {
        assert_eq!(
            term_words("urn:x:employer_id").as_deref(),
            Some("employer id")
        );
        assert_eq!(
            term_words("https://ex.com/employerId").as_deref(),
            Some("employer id")
        );
        assert_eq!(
            term_words("https://ex.com/#DAMAGE_REPORTS").as_deref(),
            Some("damage reports")
        );
        assert_eq!(
            term_words("https://ex.com/hasLEI").as_deref(),
            Some("has LEI")
        );
        assert_eq!(
            term_words("https://ex.com/countryCode").as_deref(),
            Some("country code")
        );
        assert_eq!(
            term_words("https://ex.com/LEICode").as_deref(),
            Some("LEI code")
        );
        assert_eq!(term_words("https://ex.com/C123").as_deref(), Some("c123"));
        assert_eq!(term_words("https://ex.com/123"), None);
        assert_eq!(term_words("https://ex.com/Thing/"), Some("thing".into()));
    }

    #[test]
    fn identifiers() {
        assert!(reads_as_identifier("employer_id"));
        assert!(reads_as_identifier("employerId"));
        assert!(reads_as_identifier("employer id"));
        assert!(!reads_as_identifier("employer"));
        assert!(!reads_as_identifier("id"));
        assert!(!reads_as_identifier("paid"));
    }
}
