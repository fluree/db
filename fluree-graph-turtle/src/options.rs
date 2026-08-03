//! Parser conformance options.
//!
//! The Turtle grammar is one thing; what a *consumer* wants out of it is not.
//! Fluree's ingest paths want collections flattened into indexed list items
//! and numeric literals canonicalized into native values, because that is
//! what a flake stores. A conversion tool wants neither: it has to reproduce
//! the RDF the document actually denotes, `rdf:first`/`rdf:rest` spine and
//! source lexical forms included.
//!
//! Rather than fork the grammar, both shapes come out of the same parser
//! under [`ParserOptions`]. [`ParserOptions::default`] is exactly today's
//! ingest behavior, so every existing caller is unchanged.

/// How RDF collections (`( a b c )`) reach the sink.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum CollectionStyle {
    /// Object-position collections become indexed
    /// [`emit_list_item`](fluree_graph_ir::GraphSink::emit_list_item) events
    /// on the enclosing subject/predicate, and an object-position `()` emits
    /// nothing.
    ///
    /// This is Fluree's storage shape — a list is metadata on the edge, not a
    /// chain of blank nodes — and it is lossy as RDF: the W3C-defined three
    /// triples of a one-item collection arrive as one event, and the empty
    /// collection's `rdf:nil` triple is dropped entirely. Subject-position
    /// collections are unaffected; they have always emitted a spine.
    #[default]
    IndexedItems,

    /// Collections become an `rdf:first`/`rdf:rest` blank-node spine
    /// terminated by `rdf:nil`, in every position, and `()` denotes
    /// `rdf:nil`. This is what RDF says a collection *is*, and what the W3C
    /// Turtle suite tests.
    ///
    /// In this mode a collection has a single object term (the spine head),
    /// so an RDF 1.2 annotation may follow one — the deferral that applies in
    /// [`CollectionStyle::IndexedItems`] exists only because indexed items
    /// leave nothing to reify.
    Spine,
}

/// How numeric literal tokens reach the sink.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum NumericStyle {
    /// `Integer` and `Double` tokens are parsed into native
    /// [`LiteralValue`](fluree_graph_ir::LiteralValue) values, discarding the
    /// source spelling. `+1`, `01`, and `1` all become `Integer(1)`; `1e0`
    /// and `1.0E0` both become `Double(1.0)`.
    ///
    /// Correct for ingest, where the value is what gets stored, and lossy for
    /// conversion, where the lexical form is part of the term's identity.
    #[default]
    Canonicalize,

    /// Numeric literals keep their source lexical form, typed as
    /// `xsd:integer` / `xsd:double`, exactly as `Decimal` and
    /// i64-overflowing integer tokens already do.
    PreserveLexical,
}

/// Conformance knobs for the Turtle parser.
///
/// The default is today's ingest behavior in every field; opting in is
/// per-parse and affects nothing else.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct ParserOptions {
    /// How collections reach the sink.
    pub collections: CollectionStyle,
    /// How numeric literals reach the sink.
    pub numerics: NumericStyle,
}

impl ParserOptions {
    /// Ingest defaults: [`CollectionStyle::IndexedItems`] +
    /// [`NumericStyle::Canonicalize`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Faithful-RDF preset: spine collections and preserved numeric
    /// lexical forms — what a syntax-conformant reader/converter wants.
    pub fn conformant() -> Self {
        Self {
            collections: CollectionStyle::Spine,
            numerics: NumericStyle::PreserveLexical,
        }
    }

    /// Set the collection style.
    pub fn with_collections(mut self, collections: CollectionStyle) -> Self {
        self.collections = collections;
        self
    }

    /// Set the numeric style.
    pub fn with_numerics(mut self, numerics: NumericStyle) -> Self {
        self.numerics = numerics;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_the_ingest_shape() {
        let o = ParserOptions::default();
        assert_eq!(o.collections, CollectionStyle::IndexedItems);
        assert_eq!(o.numerics, NumericStyle::Canonicalize);
        assert_eq!(o, ParserOptions::new());
    }

    #[test]
    fn conformant_preset_opts_into_both() {
        let o = ParserOptions::conformant();
        assert_eq!(o.collections, CollectionStyle::Spine);
        assert_eq!(o.numerics, NumericStyle::PreserveLexical);
        assert_eq!(
            o,
            ParserOptions::new()
                .with_collections(CollectionStyle::Spine)
                .with_numerics(NumericStyle::PreserveLexical)
        );
    }
}
