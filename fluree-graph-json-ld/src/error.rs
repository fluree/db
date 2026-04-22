use serde_json::Value as JsonValue;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum JsonLdError {
    #[error("Invalid context: {message}")]
    InvalidContext { message: String },

    #[error("Invalid IRI: {iri}")]
    InvalidIri { iri: String },

    #[error("Invalid IRI mapping for term '{term}'")]
    InvalidIriMapping { term: String, context: JsonValue },

    #[error("@language cannot be used for values with a specified @type")]
    LanguageWithType,

    #[error("Sequential values within sequential values not allowed at index: {idx:?}")]
    NestedSequence { idx: Vec<JsonValue> },

    #[error("Unexpected error: {message}")]
    Unexpected { message: String },

    #[error(
        "Unresolved compact IRI '{value}': prefix '{prefix}' is not defined in @context. \
             If this is intended as an absolute IRI, use a full form (e.g. http://...) \
             or add the prefix to @context."
    )]
    UnresolvedCompactIri { value: String, prefix: String },
}

pub type Result<T> = std::result::Result<T, JsonLdError>;
