//! What an ontology and catalog cost in an extraction request, roughly.
//!
//! The model reads each window as one serialized prompt: the ontology, the
//! whole catalog and the window's text. A window holds at most
//! [`TOKENS_PER_WINDOW`] serialized tokens, and the catalog is repeated in
//! every window, so a large catalog leaves less room for text.
//!
//! The model's tokenizer is not part of this crate, so the count is an
//! estimate from character counts. Its coefficients were fitted to the token
//! counts an extraction service reported for saved requests: catalogs of 68
//! to 3,250 records (about 59 tokens a record at about 80 characters of
//! labels and definition each), ontologies of 25 terms with and without
//! comments, and document text (about 0.77 tokens a character, since the
//! text is shown twice). Treat it as good to about a tenth.

/// Serialized tokens one window may hold.
pub const TOKENS_PER_WINDOW: usize = 200_000;
/// Characters of document text one request may carry.
pub const MAX_TEXT_CHARS: usize = 48_000;
/// Bytes one request may carry.
pub const MAX_BODY_BYTES: usize = 2_000_000;
/// Windows one request may have.
pub const MAX_WINDOWS: usize = 16;
/// Characters of a catalog record's definition the model reads; the rest is
/// cut at a word boundary.
pub const DEFINITION_CHARS: usize = 300;

/// The prompt's fixed part.
const FIXED: usize = 800;
const PER_TERM: usize = 20;
/// Per hundred characters of labels and comments.
const PER_100_ONTOLOGY_CHARS: usize = 27;
const PER_RECORD: usize = 35;
/// Per hundred characters of record labels and definitions.
const PER_100_CATALOG_CHARS: usize = 30;
/// Per hundred characters of document text.
const PER_100_TEXT_CHARS: usize = 77;

/// The sizes an estimate is made from.
#[derive(Clone, Copy, Debug, Default)]
pub struct Sizes {
    /// Classes and properties shown.
    pub terms: usize,
    /// Characters of their labels and comments.
    pub ontology_chars: usize,
    pub records: usize,
    /// Characters of record labels and definitions (each definition counted
    /// to [`DEFINITION_CHARS`]).
    pub catalog_chars: usize,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct Estimate {
    /// The ontology's share, with the prompt's fixed part.
    pub ontology_tokens: usize,
    pub catalog_tokens: usize,
    /// Both, before any document text.
    pub tokens: usize,
    pub limit: usize,
    /// Characters of document text a window has room for (at most
    /// [`MAX_TEXT_CHARS`]; zero when the window is already full).
    pub document_chars: usize,
}

pub fn estimate(sizes: Sizes) -> Estimate {
    let ontology_tokens =
        FIXED + sizes.terms * PER_TERM + sizes.ontology_chars * PER_100_ONTOLOGY_CHARS / 100;
    let catalog_tokens = if sizes.records == 0 {
        0
    } else {
        sizes.records * PER_RECORD + sizes.catalog_chars * PER_100_CATALOG_CHARS / 100
    };
    let tokens = ontology_tokens + catalog_tokens;
    let document_chars =
        (TOKENS_PER_WINDOW.saturating_sub(tokens) * 100 / PER_100_TEXT_CHARS).min(MAX_TEXT_CHARS);
    Estimate {
        ontology_tokens,
        catalog_tokens,
        tokens,
        limit: TOKENS_PER_WINDOW,
        document_chars,
    }
}

/// Characters of a definition the model reads.
pub fn definition_chars(text: &str) -> usize {
    text.chars().count().min(DEFINITION_CHARS)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The saved requests the coefficients were fitted to, within a tenth.
    #[test]
    fn calibration_points() {
        // linking benchmark: a 35-term ontology (3,810 characters of labels
        // and comments) and catalogs of 68 to 3,250 records; reported tokens
        // are medians over 37 short documents
        for (records, chars, reported) in [
            (68, 5_288, 6_310),
            (256, 20_390, 17_185),
            (1_700, 136_371, 101_948),
            (3_250, 261_798, 193_577),
        ] {
            let e = estimate(Sizes {
                terms: 35,
                ontology_chars: 3_810,
                records,
                catalog_chars: chars,
            });
            let error = e.tokens.abs_diff(reported) as f64 / reported as f64;
            assert!(error < 0.1, "{records} records: {} vs {reported}", e.tokens);
        }
        // grounding benchmark: 25 terms (2,474 characters), 34 records
        // (5,377 characters); reported 4,944 tokens + 0.77 per text character
        let e = estimate(Sizes {
            terms: 25,
            ontology_chars: 2_474,
            records: 34,
            catalog_chars: 5_377,
        });
        let error = e.tokens.abs_diff(4_944) as f64 / 4_944.0;
        assert!(error < 0.1, "grounding: {} vs 4944", e.tokens);
    }

    #[test]
    fn a_full_window_leaves_no_text() {
        let e = estimate(Sizes {
            terms: 10,
            ontology_chars: 500,
            records: 5_000,
            catalog_chars: 400_000,
        });
        assert!(e.tokens > TOKENS_PER_WINDOW);
        assert_eq!(e.document_chars, 0);
    }
}
