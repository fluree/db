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

/// How value-typed literal tokens reach the sink.
///
/// "Numeric" is the historical name and the majority of the lane; the switch
/// governs every token Turtle spells as a bare value rather than a quoted
/// string — `Integer`, `Double`, and the `true`/`false` keywords. They move
/// together because they share one hazard: a canonicalized token and the same
/// term written longhand (`"1"^^xsd:integer`, `"true"^^xsd:boolean`) produce
/// DIFFERENT [`Term`](fluree_graph_ir::Term)s, since `Term`'s equality and
/// hash are variant-sensitive. Splitting the knob would let a caller pick a
/// combination in which one spelling of one datatype silently fails to
/// deduplicate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum NumericStyle {
    /// `Integer`, `Double` and the boolean keywords are parsed into native
    /// [`LiteralValue`](fluree_graph_ir::LiteralValue) values, discarding the
    /// source spelling. `+1`, `01`, and `1` all become `Integer(1)`; `1e0`
    /// and `1.0E0` both become `Double(1.0)`; `true` becomes `Boolean(true)`.
    ///
    /// Correct for ingest, where the value is what gets stored, and lossy for
    /// conversion, where the lexical form is part of the term's identity.
    #[default]
    Canonicalize,

    /// Value-typed literals keep their source lexical form, typed as
    /// `xsd:integer` / `xsd:double` / `xsd:boolean`, exactly as `Decimal` and
    /// i64-overflowing integer tokens already do.
    ///
    /// This is what makes `true` and `"true"^^xsd:boolean` — one RDF term
    /// written two ways — one IR term, so a graph containing both holds one
    /// triple rather than two.
    PreserveLexical,
}

/// Which grammar the parser accepts.
///
/// TriG is Turtle plus named-graph blocks, sharing one lexer and one set of
/// term/statement productions — so it is a mode of the same parser rather
/// than a fork, for the same reason [`CollectionStyle`] is: one grammar, one
/// conformance surface.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Dialect {
    /// Turtle. A `{` or a `GRAPH` keyword is a syntax error.
    #[default]
    Turtle,
    /// TriG: `GRAPH label { … }`, bare `label { … }`, and bare `{ … }`
    /// default-graph blocks, alongside ordinary Turtle statements.
    ///
    /// Named-graph output requires a quad-capable sink
    /// ([`GraphSink::supports_quads`](fluree_graph_ir::GraphSink::supports_quads));
    /// the parser refuses named graphs against a triple-only sink rather than
    /// folding them into the default graph.
    TriG,
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
    /// Which grammar to accept.
    pub dialect: Dialect,
    /// Whether to check that the terms a document denotes are RDF terms —
    /// IRIs that are IRIs after escape expansion and base resolution, and
    /// well-formed language tags.
    ///
    /// Off by default, and that is the load-bearing part. The grammar checks
    /// happen in the lexer regardless; what this adds is a scan of every
    /// resolved IRI and every language tag, which ingest does not need — it
    /// consumes documents this database wrote — and which sits directly on
    /// the bulk-import hot path. A conversion tool has the opposite need:
    /// it must not emit a document asserting a term that is not a term.
    ///
    /// [`ParserOptions::conformant`] turns it on, so "conformant" means
    /// conformant, and the benchmark cell that compares against a validating
    /// reader validates too.
    pub validate: bool,
}

impl ParserOptions {
    /// Ingest defaults: [`CollectionStyle::IndexedItems`] +
    /// [`NumericStyle::Canonicalize`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Faithful-RDF preset: spine collections, preserved numeric lexical
    /// forms, and term validation — what a syntax-conformant
    /// reader/converter wants.
    pub fn conformant() -> Self {
        Self {
            collections: CollectionStyle::Spine,
            numerics: NumericStyle::PreserveLexical,
            dialect: Dialect::Turtle,
            validate: true,
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

    /// Set the dialect.
    pub fn with_dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Turn term validation on or off.
    pub fn with_validation(mut self, validate: bool) -> Self {
        self.validate = validate;
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

    /// The ingest contract in one assertion. Validation costs a scan of every
    /// resolved IRI, and bulk import runs on this default — if it ever flips,
    /// the import path silently takes that cost.
    #[test]
    fn validation_is_off_by_default() {
        assert!(!ParserOptions::default().validate);
        assert!(!ParserOptions::new().validate);
        // And the other knobs do not drag it along.
        assert!(
            !ParserOptions::new()
                .with_collections(CollectionStyle::Spine)
                .with_numerics(NumericStyle::PreserveLexical)
                .validate
        );
    }

    #[test]
    fn conformant_preset_opts_into_all_three() {
        let o = ParserOptions::conformant();
        assert_eq!(o.collections, CollectionStyle::Spine);
        assert_eq!(o.numerics, NumericStyle::PreserveLexical);
        assert!(o.validate, "conformant must mean conformant");
        assert_eq!(
            o,
            ParserOptions::new()
                .with_collections(CollectionStyle::Spine)
                .with_numerics(NumericStyle::PreserveLexical)
                .with_validation(true)
        );
    }
}
