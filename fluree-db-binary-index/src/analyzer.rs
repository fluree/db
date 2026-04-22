//! Text analyzer (multi-language).
//!
//! Implements the text analysis pipeline used for BM25 scoring and fulltext
//! arena building:
//! 1. Lowercase (in tokenizer)
//! 2. Split on `[^\w]+` (regex word split)
//! 3. Stopword filtering (language-specific)
//! 4. Snowball stemming (language-specific)
//!
//! The tokenizer is language-agnostic (Unicode `\w` semantics). Stopword
//! filtering and stemming are driven by [`Language`], which maps BCP-47
//! language tags to stopword lists + Snowball stemmer algorithms. The
//! `Unknown` language variant tokenizes + lowercases only (no stopwords,
//! no stemming) so unrecognized tags still produce consistent scoring
//! between index-time and query-time.
//!
//! This module is the single source of truth for text analysis. Both the
//! query-time BM25 scorer (`fluree-db-query`) and the index-time fulltext
//! arena builder (`fluree-db-indexer`) consume it, ensuring identical
//! tokenization/stemming so that indexed and novelty data produce consistent
//! BM25 scores.

use std::collections::{HashMap, HashSet};

use once_cell::sync::Lazy;
use regex::Regex;
use rust_stemmers::{Algorithm, Stemmer};

// ============================================================================
// Language
// ============================================================================

/// Supported languages for text analysis.
///
/// Each variant corresponds to a Snowball stemmer algorithm plus a bundled
/// stopword list. `Unknown` is a safe default for BCP-47 tags we do not
/// recognize — the analyzer tokenizes + lowercases the input but does not
/// remove stopwords or stem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Language {
    Arabic,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Greek,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
    Unknown,
}

impl Language {
    /// Map a BCP-47 tag (e.g. `"en"`, `"en-US"`, `"fr-CA"`) to a `Language`.
    ///
    /// Only the primary subtag is inspected; regional subtags are ignored.
    /// Unrecognized primary subtags return `Unknown`.
    pub fn from_bcp47(tag: &str) -> Self {
        let primary = tag.split('-').next().unwrap_or("");
        let lower = primary.to_ascii_lowercase();
        match lower.as_str() {
            "ar" => Self::Arabic,
            "da" => Self::Danish,
            "nl" => Self::Dutch,
            "en" => Self::English,
            "fi" => Self::Finnish,
            "fr" => Self::French,
            "de" => Self::German,
            "el" => Self::Greek,
            "hu" => Self::Hungarian,
            "it" => Self::Italian,
            // Norwegian: Bokmål / Nynorsk / macrolanguage all stem via Norwegian.
            "no" | "nb" | "nn" => Self::Norwegian,
            "pt" => Self::Portuguese,
            "ro" => Self::Romanian,
            "ru" => Self::Russian,
            "es" => Self::Spanish,
            "sv" => Self::Swedish,
            "ta" => Self::Tamil,
            "tr" => Self::Turkish,
            _ => Self::Unknown,
        }
    }

    fn snowball_algorithm(self) -> Option<Algorithm> {
        match self {
            Language::Arabic => Some(Algorithm::Arabic),
            Language::Danish => Some(Algorithm::Danish),
            Language::Dutch => Some(Algorithm::Dutch),
            Language::English => Some(Algorithm::English),
            Language::Finnish => Some(Algorithm::Finnish),
            Language::French => Some(Algorithm::French),
            Language::German => Some(Algorithm::German),
            Language::Greek => Some(Algorithm::Greek),
            Language::Hungarian => Some(Algorithm::Hungarian),
            Language::Italian => Some(Algorithm::Italian),
            Language::Norwegian => Some(Algorithm::Norwegian),
            Language::Portuguese => Some(Algorithm::Portuguese),
            Language::Romanian => Some(Algorithm::Romanian),
            Language::Russian => Some(Algorithm::Russian),
            Language::Spanish => Some(Algorithm::Spanish),
            Language::Swedish => Some(Algorithm::Swedish),
            Language::Tamil => Some(Algorithm::Tamil),
            Language::Turkish => Some(Algorithm::Turkish),
            Language::Unknown => None,
        }
    }

    fn stopwords(self) -> Option<&'static HashSet<String>> {
        match self {
            Language::Arabic => Some(&ARABIC_STOPWORDS),
            Language::Danish => Some(&DANISH_STOPWORDS),
            Language::Dutch => Some(&DUTCH_STOPWORDS),
            Language::English => Some(&ENGLISH_STOPWORDS),
            Language::Finnish => Some(&FINNISH_STOPWORDS),
            Language::French => Some(&FRENCH_STOPWORDS),
            Language::German => Some(&GERMAN_STOPWORDS),
            Language::Greek => Some(&GREEK_STOPWORDS),
            Language::Hungarian => Some(&HUNGARIAN_STOPWORDS),
            Language::Italian => Some(&ITALIAN_STOPWORDS),
            Language::Norwegian => Some(&NORWEGIAN_STOPWORDS),
            Language::Portuguese => Some(&PORTUGUESE_STOPWORDS),
            Language::Romanian => Some(&ROMANIAN_STOPWORDS),
            Language::Russian => Some(&RUSSIAN_STOPWORDS),
            Language::Spanish => Some(&SPANISH_STOPWORDS),
            Language::Swedish => Some(&SWEDISH_STOPWORDS),
            Language::Tamil => Some(&TAMIL_STOPWORDS),
            Language::Turkish => Some(&TURKISH_STOPWORDS),
            Language::Unknown => None,
        }
    }
}

// ============================================================================
// Token
// ============================================================================

/// A token produced by the tokenizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub text: String,
}

impl Token {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

// ============================================================================
// Tokenizer
// ============================================================================

/// Trait for tokenizers that split text into tokens.
pub trait Tokenizer: Send + Sync {
    fn tokenize(&self, text: &str) -> Vec<Token>;
}

/// Regex for splitting on non-word characters.
static WORD_SPLIT: Lazy<Regex> = Lazy::new(|| {
    // `[^\w]+` in regex means one or more non-word characters
    // Note: `\w` semantics depend on the regex engine's Unicode configuration.
    Regex::new(r"[^\w]+").expect("Invalid regex")
});

/// Default Unicode-aware tokenizer: lowercase, then split on `[^\w]+`.
///
/// The tokenizer is language-agnostic — it relies only on Unicode word
/// boundaries and is reused across every supported [`Language`]. Language-
/// specific behavior lives in the stopword filter and stemmer further down
/// the pipeline.
#[derive(Debug, Default, Clone)]
pub struct DefaultEnglishTokenizer;

impl Tokenizer for DefaultEnglishTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        // Lowercase first.
        let lowercased = text.to_lowercase();

        WORD_SPLIT
            .split(&lowercased)
            .filter(|s| !s.is_empty())
            .map(|s| Token::new(s.to_string()))
            .collect()
    }
}

// ============================================================================
// Token Filters
// ============================================================================

/// Trait for filters that transform or remove tokens.
pub trait TokenFilter: Send + Sync {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token>;
}

/// Stopword filter that removes common words.
#[derive(Debug, Clone)]
pub struct StopwordFilter {
    stopwords: HashSet<String>,
}

impl StopwordFilter {
    /// Create a new stopword filter with the given stopwords.
    pub fn new(stopwords: HashSet<String>) -> Self {
        Self { stopwords }
    }

    /// Create an English stopword filter.
    pub fn english() -> Self {
        Self::new(ENGLISH_STOPWORDS.clone())
    }

    /// Check if a word is a stopword.
    pub fn is_stopword(&self, word: &str) -> bool {
        self.stopwords.contains(word)
    }
}

impl TokenFilter for StopwordFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .filter(|t| !self.stopwords.contains(&t.text))
            .collect()
    }
}

/// Snowball stemmer filter.
pub struct SnowballStemmerFilter {
    stemmer: Stemmer,
    algorithm: Algorithm,
}

impl Clone for SnowballStemmerFilter {
    fn clone(&self) -> Self {
        // Recreate the stemmer since Stemmer doesn't implement Clone
        Self {
            stemmer: Stemmer::create(self.algorithm),
            algorithm: self.algorithm,
        }
    }
}

impl std::fmt::Debug for SnowballStemmerFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowballStemmerFilter")
            .field("algorithm", &self.algorithm)
            .finish()
    }
}

impl SnowballStemmerFilter {
    /// Create a new Snowball stemmer filter for the given language.
    pub fn new(algorithm: Algorithm) -> Self {
        Self {
            stemmer: Stemmer::create(algorithm),
            algorithm,
        }
    }

    /// Create an English Snowball stemmer filter.
    pub fn english() -> Self {
        Self::new(Algorithm::English)
    }

    /// Stem a single word.
    pub fn stem(&self, word: &str) -> String {
        self.stemmer.stem(word).into_owned()
    }
}

impl TokenFilter for SnowballStemmerFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .map(|t| Token::new(self.stemmer.stem(&t.text).into_owned()))
            .collect()
    }
}

// ============================================================================
// Analyzer
// ============================================================================

/// Text analyzer combining a tokenizer and filters.
pub struct Analyzer {
    tokenizer: Box<dyn Tokenizer>,
    filters: Vec<Box<dyn TokenFilter>>,
}

impl Analyzer {
    /// Create a new analyzer with the given tokenizer and filters.
    pub fn new(tokenizer: Box<dyn Tokenizer>, filters: Vec<Box<dyn TokenFilter>>) -> Self {
        Self { tokenizer, filters }
    }

    /// Create the default English analyzer.
    ///
    /// Pipeline:
    /// 1. Default tokenizer (lowercase + word split)
    /// 2. StopwordFilter (English stopwords)
    /// 3. SnowballStemmerFilter (English stemmer)
    ///
    /// This is the path used by the `@fulltext` datatype shortcut, which is
    /// always English regardless of configuration.
    pub fn english_default() -> Self {
        Self::for_language(Language::English)
    }

    /// Create an analyzer for the given language.
    ///
    /// Pipeline:
    /// 1. Default tokenizer (lowercase + word split) — language-agnostic.
    /// 2. Stopword filter — if the language has a bundled stopword list.
    /// 3. Snowball stemmer — if the language has a Snowball algorithm.
    ///
    /// [`Language::Unknown`] builds an analyzer with just the tokenizer —
    /// no stopword removal, no stemming. This guarantees consistent behavior
    /// on the index and query sides for unrecognized BCP-47 tags.
    pub fn for_language(lang: Language) -> Self {
        let tokenizer: Box<dyn Tokenizer> = Box::new(DefaultEnglishTokenizer);
        let mut filters: Vec<Box<dyn TokenFilter>> = Vec::new();
        if let Some(stopwords) = lang.stopwords() {
            filters.push(Box::new(StopwordFilter::new(stopwords.clone())));
        }
        if let Some(algorithm) = lang.snowball_algorithm() {
            filters.push(Box::new(SnowballStemmerFilter::new(algorithm)));
        }
        Self { tokenizer, filters }
    }

    /// Analyze text into tokens.
    pub fn analyze(&self, text: &str) -> Vec<Token> {
        let mut tokens = self.tokenizer.tokenize(text);

        for filter in &self.filters {
            tokens = filter.filter(tokens);
        }

        tokens
    }

    /// Analyze text and return just the token strings.
    pub fn analyze_to_strings(&self, text: &str) -> Vec<String> {
        self.analyze(text).into_iter().map(|t| t.text).collect()
    }

    /// Analyze text and compute term frequencies.
    pub fn analyze_to_term_freqs(&self, text: &str) -> HashMap<String, u32> {
        let mut freqs = HashMap::new();
        for token in self.analyze(text) {
            *freqs.entry(token.text).or_insert(0) += 1;
        }
        freqs
    }
}

// ============================================================================
// Standalone analysis function (no trait dispatch)
// ============================================================================

/// English Snowball stemmer (shared static instance).
static ENGLISH_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::English));

/// Analyze text into term frequencies using the default English pipeline.
///
/// This is the non-trait-dispatch equivalent of
/// `Analyzer::english_default().analyze_to_term_freqs(text)`. It produces
/// identical results but avoids `Box<dyn>` overhead, making it suitable for
/// hot paths such as bulk index building.
///
/// Pipeline: lowercase → split on `[^\w]+` → stopword removal → Snowball stem → count.
pub fn analyze_to_term_freqs(text: &str) -> HashMap<String, u32> {
    let lowered = text.to_lowercase();
    let mut freqs = HashMap::new();
    for token in WORD_SPLIT.split(&lowered) {
        if token.is_empty() {
            continue;
        }
        if ENGLISH_STOPWORDS.contains(token) {
            continue;
        }
        let stemmed = ENGLISH_STEMMER.stem(token).into_owned();
        if stemmed.is_empty() {
            continue;
        }
        *freqs.entry(stemmed).or_insert(0) += 1;
    }
    freqs
}

// ============================================================================
// Stopword resource files
// ============================================================================
//
// Each language's stopword file is compile-time-included so the analyzer has
// no runtime filesystem dependency. Files live under
// `fluree-db-binary-index/resources/stopwords/{code}.txt` (one word per line,
// `#` starts a comment, blank lines allowed). Adding a language means:
//   1. Create the resource file.
//   2. Add an include_str! + Lazy static below.
//   3. Wire it into `Language::stopwords()`.

fn parse_stopwords(file: &str) -> HashSet<String> {
    file.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_lowercase)
        .collect()
}

static ENGLISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/en.txt")));
static ARABIC_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/ar.txt")));
static DANISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/da.txt")));
static DUTCH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/nl.txt")));
static FINNISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/fi.txt")));
static FRENCH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/fr.txt")));
static GERMAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/de.txt")));
static GREEK_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/el.txt")));
static HUNGARIAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/hu.txt")));
static ITALIAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/it.txt")));
static NORWEGIAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/no.txt")));
static PORTUGUESE_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/pt.txt")));
static ROMANIAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/ro.txt")));
static RUSSIAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/ru.txt")));
static SPANISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/es.txt")));
static SWEDISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/sv.txt")));
static TAMIL_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/ta.txt")));
static TURKISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/tr.txt")));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_english_tokenizer() {
        let tokenizer = DefaultEnglishTokenizer;

        let tokens = tokenizer.tokenize("Hello, World!");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[1].text, "world");
    }

    #[test]
    fn test_tokenizer_unicode() {
        let tokenizer = DefaultEnglishTokenizer;

        // Rust regex \w includes Unicode letters by default (better for i18n)
        let tokens = tokenizer.tokenize("café résumé");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "café");
        assert_eq!(tokens[1].text, "résumé");
    }

    #[test]
    fn test_tokenizer_numbers() {
        let tokenizer = DefaultEnglishTokenizer;

        let tokens = tokenizer.tokenize("test123 hello_world foo42bar");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "test123");
        assert_eq!(tokens[1].text, "hello_world");
        assert_eq!(tokens[2].text, "foo42bar");
    }

    #[test]
    fn test_stopword_filter() {
        let filter = StopwordFilter::english();

        let tokens = vec![
            Token::new("the"),
            Token::new("quick"),
            Token::new("brown"),
            Token::new("fox"),
        ];

        let filtered = filter.filter(tokens);
        assert_eq!(filtered.len(), 3);
        assert!(!filtered.iter().any(|t| t.text == "the"));
    }

    #[test]
    fn test_stemmer_filter() {
        let filter = SnowballStemmerFilter::english();

        let tokens = vec![
            Token::new("running"),
            Token::new("jumped"),
            Token::new("foxes"),
        ];

        let stemmed = filter.filter(tokens);
        assert_eq!(stemmed[0].text, "run");
        assert_eq!(stemmed[1].text, "jump");
        assert_eq!(stemmed[2].text, "fox");
    }

    #[test]
    fn test_analyzer_full_pipeline() {
        let analyzer = Analyzer::english_default();

        let terms = analyzer.analyze_to_strings("The quick brown foxes are running!");

        // "the" and "are" should be filtered as stopwords
        // "foxes" -> "fox", "running" -> "run"
        assert!(terms.contains(&"quick".to_string()));
        assert!(terms.contains(&"brown".to_string()));
        assert!(terms.contains(&"fox".to_string()));
        assert!(terms.contains(&"run".to_string()));
        assert!(!terms.contains(&"the".to_string()));
        assert!(!terms.contains(&"are".to_string()));
    }

    #[test]
    fn test_analyzer_term_freqs() {
        let analyzer = Analyzer::english_default();

        let freqs = analyzer.analyze_to_term_freqs("fox fox fox dog dog cat");

        assert_eq!(freqs.get("fox"), Some(&3));
        assert_eq!(freqs.get("dog"), Some(&2));
        assert_eq!(freqs.get("cat"), Some(&1));
    }

    #[test]
    fn test_analyzer_empty_input() {
        let analyzer = Analyzer::english_default();

        let terms = analyzer.analyze_to_strings("");
        assert!(terms.is_empty());

        let terms = analyzer.analyze_to_strings("   ");
        assert!(terms.is_empty());
    }

    #[test]
    fn test_analyzer_only_stopwords() {
        let analyzer = Analyzer::english_default();

        let terms = analyzer.analyze_to_strings("the a an is are");
        assert!(terms.is_empty());
    }

    // ========================================================================
    // Multi-language support
    // ========================================================================

    #[test]
    fn test_language_from_bcp47_primary() {
        assert_eq!(Language::from_bcp47("en"), Language::English);
        assert_eq!(Language::from_bcp47("fr"), Language::French);
        assert_eq!(Language::from_bcp47("de"), Language::German);
        assert_eq!(Language::from_bcp47("es"), Language::Spanish);
        assert_eq!(Language::from_bcp47("ta"), Language::Tamil);
    }

    #[test]
    fn test_language_from_bcp47_region_subtag_ignored() {
        assert_eq!(Language::from_bcp47("en-US"), Language::English);
        assert_eq!(Language::from_bcp47("fr-CA"), Language::French);
        assert_eq!(Language::from_bcp47("pt-BR"), Language::Portuguese);
    }

    #[test]
    fn test_language_from_bcp47_norwegian_variants() {
        assert_eq!(Language::from_bcp47("no"), Language::Norwegian);
        assert_eq!(Language::from_bcp47("nb"), Language::Norwegian);
        assert_eq!(Language::from_bcp47("nn"), Language::Norwegian);
    }

    #[test]
    fn test_language_from_bcp47_case_insensitive() {
        assert_eq!(Language::from_bcp47("EN"), Language::English);
        assert_eq!(Language::from_bcp47("Fr-CA"), Language::French);
    }

    #[test]
    fn test_language_from_bcp47_unknown() {
        assert_eq!(Language::from_bcp47("zz"), Language::Unknown);
        assert_eq!(Language::from_bcp47(""), Language::Unknown);
        assert_eq!(Language::from_bcp47("jp"), Language::Unknown); // "ja" is correct for Japanese
        assert_eq!(Language::from_bcp47("ja"), Language::Unknown); // no Snowball algo for Japanese
    }

    #[test]
    fn test_analyzer_for_unknown_tokenizes_only() {
        // Unknown language: tokenize + lowercase, no stopwords, no stemming.
        let analyzer = Analyzer::for_language(Language::Unknown);
        let terms = analyzer.analyze_to_strings("The Running Foxes");
        assert_eq!(terms, vec!["the", "running", "foxes"]);
    }

    #[test]
    fn test_analyzer_for_french_applies_stopwords_and_stemmer() {
        let analyzer = Analyzer::for_language(Language::French);
        // "de", "la", "les" are French stopwords — must be removed.
        // "grandes" should stem to the same form as "grand".
        let freqs = analyzer.analyze_to_term_freqs("de la maison les grandes grand");
        assert!(!freqs.contains_key("de"));
        assert!(!freqs.contains_key("la"));
        assert!(!freqs.contains_key("les"));
        // "maison" survives as a content word.
        assert!(freqs.keys().any(|k| k.starts_with("maison")));
        // "grand" and "grandes" stem to the same form (French stemmer removes -es).
        let grand_stems: Vec<&String> = freqs.keys().filter(|k| k.starts_with("grand")).collect();
        assert_eq!(
            grand_stems.len(),
            1,
            "grand/grandes should share a stem, got: {grand_stems:?}"
        );
        assert_eq!(freqs[grand_stems[0]], 2);
    }

    #[test]
    fn test_analyzer_english_equivalent_to_english_default() {
        let a = Analyzer::english_default();
        let b = Analyzer::for_language(Language::English);
        let text = "The quick brown foxes are running!";
        assert_eq!(a.analyze_to_term_freqs(text), b.analyze_to_term_freqs(text));
    }

    // ========================================================================
    // Standalone function tests
    // ========================================================================

    #[test]
    fn test_standalone_analyze_basic() {
        let freqs = analyze_to_term_freqs("The quick brown fox jumps over the lazy dog");
        // "the" and "over" are stopwords
        assert!(!freqs.contains_key("the"));
        assert!(!freqs.contains_key("over"));
        // "quick", "brown", "fox", "jump" (stemmed), "lazi" (stemmed), "dog"
        assert!(freqs.contains_key("quick"));
        assert!(freqs.contains_key("fox"));
        // "jumps" stems to "jump"
        assert!(freqs.contains_key("jump"));
    }

    #[test]
    fn test_standalone_analyze_stemming() {
        let freqs = analyze_to_term_freqs("indexing indexed indexes");
        // All should stem to "index"
        assert_eq!(freqs.len(), 1);
        assert_eq!(freqs["index"], 3);
    }

    #[test]
    fn test_standalone_analyze_empty() {
        let freqs = analyze_to_term_freqs("");
        assert!(freqs.is_empty());

        // All stopwords
        let freqs = analyze_to_term_freqs("the a an is are was");
        assert!(freqs.is_empty());
    }

    #[test]
    fn test_standalone_matches_analyzer() {
        // The standalone function must produce identical output to the trait-based Analyzer.
        let analyzer = Analyzer::english_default();
        let texts = [
            "The quick brown fox jumps over the lazy dog",
            "indexing indexed indexes",
            "Rust programming language is awesome",
            "",
            "the a an is are was",
            "Hello, World! This is a test of the analyzer.",
        ];
        for text in &texts {
            let trait_freqs = analyzer.analyze_to_term_freqs(text);
            let fn_freqs = analyze_to_term_freqs(text);
            assert_eq!(trait_freqs, fn_freqs, "mismatch for input: {text:?}");
        }
    }
}
