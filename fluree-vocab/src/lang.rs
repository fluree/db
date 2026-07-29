//! Language-tag well-formedness (BCP 47 shape, as the RDF grammars spell it).
//!
//! Shared by every surface that accepts a language-tagged literal. Like
//! [`crate::iri`], this is a pure string predicate — no I/O, no allocation on
//! the passing path — so it is safe to run at parse time.
//!
//! # What "well-formed" means here
//!
//! The Turtle and N-Triples grammars both spell the tag as
//!
//! ```text
//! LANGTAG ::= '@' [a-zA-Z]+ ('-' [a-zA-Z0-9]+)*
//! ```
//!
//! which is BCP 47's *shape* without its length rules and without any
//! registry lookup. This module checks exactly that production, on purpose:
//!
//! - It is what the W3C suites test (`turtle-syntax-bad-lang-01`,
//!   `nt-syntax-bad-lang-01` — both `"string"@1`).
//! - Going further would over-reject. BCP 47 constrains subtag *lengths*
//!   (a primary subtag is 2-3, 4, or 5-8 letters), and a parser enforcing
//!   that would reject tags the RDF grammars accept — and over-rejection is
//!   the standing hazard for a conformance parser, invisible to negative
//!   tests and caught only by the positive ones.
//! - Registry *validity* (is `zz` a real language?) is a data question, not a
//!   grammar one, and needs a registry this crate has no business carrying.
//!
//! So: well-formed per the RDF grammars, not valid per the IANA registry.
//! [`language_tag_violation`] is named for what it does.

/// Why a string is not a well-formed language tag.
///
/// Indices are byte offsets into the tag, not character counts. The tag
/// excludes the leading `@`.
///
/// Which of these a given caller can actually see depends on its lexer, and
/// Turtle's is NARROWER than this grammar in characters — its `take_while`
/// admits only `[a-zA-Z0-9-]`, and is wider only in SHAPE (digits first,
/// leading, trailing or doubled `-`). From Turtle, therefore, only
/// [`Self::NonAlphabeticPrimary`] and [`Self::EmptySubtag`] are reachable;
/// `@en_GB`, an accented tag and a bare `@` are lexical errors that never
/// arrive here. The other two variants exist because this is a shared
/// predicate and a looser reader — N-Triples has its own — can reach them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LangTagViolation {
    /// The tag is empty.
    ///
    /// Not reachable from Turtle: `"x"@` is a lexical error, because the
    /// lexer requires at least one tag character before it will produce a
    /// `LangTag` token at all. Kept for callers whose lexer hands over an
    /// empty tag rather than refusing it.
    Empty,
    /// The primary subtag holds something other than a letter.
    ///
    /// This is the `"string"@1` case: a language tag must *begin* with a
    /// language, and a language is letters.
    NonAlphabeticPrimary {
        /// Byte offset of the offending character within the tag.
        index: usize,
        /// The offending character.
        ch: char,
    },
    /// A `-` is followed by nothing, or by another `-` (`"x"@en-`, `"x"@en--gb`).
    EmptySubtag {
        /// Byte offset of the empty subtag within the tag.
        index: usize,
    },
    /// A subtag after the first holds something other than a letter or digit.
    ///
    /// Also not reachable from Turtle, for the same reason as
    /// [`Self::Empty`]: the lexer admits only `[a-zA-Z0-9-]`, so `@en_GB`
    /// never becomes a tag.
    NonAlphanumericSubtag {
        /// Byte offset of the offending character within the tag.
        index: usize,
        /// The offending character.
        ch: char,
    },
}

impl std::fmt::Display for LangTagViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LangTagViolation::Empty => f.write_str("language tag is empty"),
            LangTagViolation::NonAlphabeticPrimary { index, ch } => write!(
                f,
                "language tag starts with '{ch}' at position {index}; \
                 its first subtag must be letters"
            ),
            LangTagViolation::EmptySubtag { index } => write!(
                f,
                "language tag has an empty subtag at position {index}; \
                 every '-' must be followed by letters or digits"
            ),
            LangTagViolation::NonAlphanumericSubtag { index, ch } => write!(
                f,
                "language tag contains '{ch}' at position {index}; \
                 subtags after the first must be letters or digits"
            ),
        }
    }
}

/// Check a language tag (without its leading `@`) for well-formedness.
///
/// Returns `None` when the tag matches the RDF grammars' `LANGTAG`
/// production; see the module docs for what that does and does not cover.
///
/// # Example
///
/// ```
/// use fluree_vocab::lang::{language_tag_violation, LangTagViolation};
///
/// assert_eq!(language_tag_violation("en"), None);
/// assert_eq!(language_tag_violation("en-GB-oed"), None);
/// assert_eq!(language_tag_violation("x-whatever-42"), None);
///
/// // `"string"@1` — the W3C case.
/// assert!(matches!(
///     language_tag_violation("1"),
///     Some(LangTagViolation::NonAlphabeticPrimary { ch: '1', .. })
/// ));
/// assert_eq!(language_tag_violation("en-"), Some(LangTagViolation::EmptySubtag { index: 3 }));
/// ```
#[must_use]
pub fn language_tag_violation(tag: &str) -> Option<LangTagViolation> {
    if tag.is_empty() {
        return Some(LangTagViolation::Empty);
    }

    let mut subtags = tag.split('-');

    // `split` on a non-empty string always yields at least one item.
    let primary = subtags.next().unwrap_or("");
    if primary.is_empty() {
        return Some(LangTagViolation::EmptySubtag { index: 0 });
    }
    if let Some((index, ch)) = primary
        .char_indices()
        .find(|(_, c)| !c.is_ascii_alphabetic())
    {
        return Some(LangTagViolation::NonAlphabeticPrimary { index, ch });
    }

    // Byte offset of the subtag under inspection: past the primary and its
    // separator to start with, then advanced per subtag.
    let mut offset = primary.len() + 1;
    for subtag in subtags {
        if subtag.is_empty() {
            return Some(LangTagViolation::EmptySubtag { index: offset });
        }
        if let Some((index, ch)) = subtag
            .char_indices()
            .find(|(_, c)| !c.is_ascii_alphanumeric())
        {
            return Some(LangTagViolation::NonAlphanumericSubtag {
                index: offset + index,
                ch,
            });
        }
        offset += subtag.len() + 1;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_what_the_grammar_accepts() {
        for tag in [
            "en",
            "EN",
            "fr",
            "en-GB",
            "en-GB-oed",
            "x-whatever",
            "zh-Hans-CN",
            "i",            // one letter is legal per the production
            "abcdefghijkl", // no length rule — deliberately not enforced
            "de-1901",      // digits are legal in a non-primary subtag
            "a-0",
        ] {
            assert_eq!(language_tag_violation(tag), None, "{tag:?}");
        }
    }

    #[test]
    fn the_w3c_case_is_a_non_alphabetic_primary() {
        // turtle-syntax-bad-lang-01 and nt-syntax-bad-lang-01 are both
        // `"string"@1`.
        assert_eq!(
            language_tag_violation("1"),
            Some(LangTagViolation::NonAlphabeticPrimary { index: 0, ch: '1' })
        );
        assert_eq!(
            language_tag_violation("1en"),
            Some(LangTagViolation::NonAlphabeticPrimary { index: 0, ch: '1' })
        );
        // A digit later in the PRIMARY subtag is equally wrong.
        assert_eq!(
            language_tag_violation("e1"),
            Some(LangTagViolation::NonAlphabeticPrimary { index: 1, ch: '1' })
        );
    }

    #[test]
    fn rejects_empty_and_dangling_subtags() {
        assert_eq!(language_tag_violation(""), Some(LangTagViolation::Empty));
        assert_eq!(
            language_tag_violation("en-"),
            Some(LangTagViolation::EmptySubtag { index: 3 })
        );
        assert_eq!(
            language_tag_violation("en--gb"),
            Some(LangTagViolation::EmptySubtag { index: 3 })
        );
        // A leading `-` makes the primary subtag empty.
        assert_eq!(
            language_tag_violation("-en"),
            Some(LangTagViolation::EmptySubtag { index: 0 })
        );
        assert_eq!(
            language_tag_violation("-"),
            Some(LangTagViolation::EmptySubtag { index: 0 })
        );
    }

    #[test]
    fn rejects_non_alphanumeric_subtags() {
        assert_eq!(
            language_tag_violation("en-G_B"),
            Some(LangTagViolation::NonAlphanumericSubtag { index: 4, ch: '_' })
        );
        // Offsets keep counting past several subtags.
        assert_eq!(
            language_tag_violation("en-gb-oe$d"),
            Some(LangTagViolation::NonAlphanumericSubtag { index: 8, ch: '$' })
        );
    }

    #[test]
    fn offsets_point_at_the_offending_byte() {
        // No Turtle document can deliver a non-ASCII tag — the lexer stops
        // at `[a-zA-Z0-9-]` — but this is a shared predicate and offsets are
        // BYTES, so a looser caller must not make it panic or mislead.
        let v = language_tag_violation("é");
        assert_eq!(
            v,
            Some(LangTagViolation::NonAlphabeticPrimary { index: 0, ch: 'é' })
        );
        let v = language_tag_violation("en-gé");
        assert_eq!(
            v,
            Some(LangTagViolation::NonAlphanumericSubtag { index: 4, ch: 'é' })
        );
    }

    #[test]
    fn messages_name_the_problem() {
        let render = |tag: &str| language_tag_violation(tag).unwrap().to_string();
        assert!(render("1").contains("must be letters"), "{}", render("1"));
        assert!(render("en-").contains("empty subtag"), "{}", render("en-"));
        assert!(render("").contains("empty"), "{}", render(""));
    }
}
