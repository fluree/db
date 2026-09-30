//! RDF text, located once and parsed once.
//!
//! A TriG document is Turtle statements interleaved with graph blocks. The
//! locator (the `locate` module) reads tokens only: it finds each block's
//! label and extent and never interprets a term, so it cannot disagree with
//! the parser about what the text means.

mod locate;

pub use locate::has_graph_blocks;
