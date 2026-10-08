# RDF documents

Fluree reads and writes RDF documents without a ledger. You can parse a document
into quads, work with them, and write them out in any of five syntaxes. RDF 1.2
triple terms and reifications are carried through. Writing the quads to a ledger
is an ordinary [insert](../transactions/insert.md).

- **Python:** `fluree.parse()` and `fluree.serialize()`, with quads as
  `fluree.Quad`. See [RDF documents](https://fluree.github.io/db/python/guide.html#rdf-documents)
  in the Python guide.
- **Rust:** `fluree_db_api::rdf::parse()` and `serialize()`, with a document
  as a `Dataset`. See [Parse and Serialize RDF](../getting-started/rust-api.md#parse-and-serialize-rdf).

## Formats

| Format | Name | File extensions | Named graphs | Media type |
|--------|------|-----------------|--------------|------------|
| Turtle | `turtle` | `.ttl` | No | `text/turtle` |
| TriG | `trig` | `.trig` | Yes | `application/trig` |
| N-Triples | `ntriples` | `.nt` | No | `application/n-triples` |
| N-Quads | `nquads` | `.nq` | Yes | `application/n-quads` |
| JSON-LD | `jsonld` | `.jsonld`, `.json` | Yes | `application/ld+json` |

The Rust `RdfFormat` also accepts common aliases such as `ttl`, `n-quads` and
`json-ld`.

## Reading

- **Turtle and TriG** follow the W3C RDF 1.2 grammars. Relative IRIs resolve
  against the `base` argument or the document's own `@base`. A relative IRI
  with no base is an error.
- **N-Triples and N-Quads** are read strictly:
  - every IRI is absolute;
  - each statement fills exactly one line;
  - a triple term appears only as an object.
- **JSON-LD** is expanded with its own `@context`. The `base` argument resolves
  relative `@id`s. See [JSON-LD limits](#json-ld-limits).
- **Literals** keep the document's lexical form. Python converts them to
  Python values by default, which loses both the spelling and any narrower
  datatype: `"01"^^xsd:long` becomes the `int` `1` and is written back as
  `"1"^^xsd:integer`. `literals="lexical"` keeps each typed literal as a
  `fluree.Literal` with its spelling and datatype, so a document read and
  written back keeps its literals exactly.
- **Blank nodes** keep the document's labels. An anonymous blank node (`[]`, a
  collection, an annotation) is labeled `bN`, avoiding any label the document
  uses. Every parsed document can therefore be written in any format.
- **Errors:** a malformed document raises an error naming its line and column.

The readers run the W3C RDF 1.1 and RDF 1.2 Turtle, TriG, N-Triples and N-Quads
test suites in CI. For the few known gaps, see
[Standards and feature flags](compatibility.md#rdf-12).

## RDF 1.2 in each syntax

| | Turtle / TriG | N-Triples / N-Quads | JSON-LD |
|---|---|---|---|
| Named graph | `GRAPH ex:g { … }` (TriG) | the fourth term on each line (N-Quads) | `{"@id": "ex:g", "@graph": [ … ]}` |
| Annotation: asserts the triple and gives it a reifier | `ex:a ex:p ex:b ~ _:r .` or `{\| … \|}` | the triple, plus `_:r rdf:reifies <<( … )>>` | `"@annotation": {"@id": "_:r"}` on the value |
| Reified triple, not asserted | `ex:r rdf:reifies <<( ex:a ex:p ex:b )>> .` or `<< … >>` | `ex:r rdf:reifies <<( … )>>` | `{"@id": "ex:r", "@reifies": {"@id": "ex:a", "ex:p": …}}` |
| Triple term as a value | `ex:doc ex:says <<( ex:a ex:p ex:b )>> .` | `<<( … )>>` in object position | `{"@id": {"@id": "ex:a", "ex:p": …}}` |

The annotation's reifier describes the triple through its own properties. It
can be an IRI or a blank node.

Here is the same dataset as TriG, then as JSON-LD:

```trig
@prefix ex: <http://example.org/> .

ex:alice ex:knows ex:bob ~ _:b1 .
_:b1 ex:since 2020 .
ex:claim <http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies> <<( ex:carol ex:age 30 )>> .
ex:doc ex:says <<( ex:dave ex:likes ex:tea )>> .

GRAPH ex:g {
    ex:erin ex:name "Erin"@en .
}
```

```json
{
  "@context": {"ex": "http://example.org/"},
  "@graph": [
    {"@id": "_:b1", "ex:since": 2020},
    {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob", "@annotation": {"@id": "_:b1"}}},
    {"@id": "ex:claim", "@reifies": {"@id": "ex:carol", "ex:age": 30}},
    {"@id": "ex:doc", "ex:says": {"@id": {"@id": "ex:dave", "ex:likes": {"@id": "ex:tea"}}}},
    {"@id": "ex:g", "@graph": [{"@id": "ex:erin", "ex:name": {"@value": "Erin", "@language": "en"}}]}
  ]
}
```

Python represents every reification as the quad
`(reifier, rdf:reifies, fluree.Triple(…))`. An annotation adds the quad for the
triple it asserts. Rust keeps reifications apart from triples, in
`Graph::reifications`. `Dataset::add_quad` turns an `rdf:reifies` quad whose
object is a triple term into a reification.

## Writing

- **Default graph only:** Turtle and N-Triples hold just the default graph.
  Writing a named graph in either is an error; use TriG, N-Quads or JSON-LD.
- **Prefixes:** Turtle and TriG declare the prefixes you pass and shorten IRIs
  with them. JSON-LD uses them as its `@context` and compacts IRIs with them.
- **Reifications:** when the triple is asserted, the reification is written as
  an annotation where the syntax has one (`~` in Turtle and TriG,
  `@annotation` in JSON-LD). Otherwise it is written as `rdf:reifies` or
  `@reifies`.
- **Literals:** each literal is written in its lexical form. A Python `float`
  is written in its shortest form (`0.9957`, not `9.957E-1`).

## JSON-LD limits

- **Remote contexts are not fetched.** A string `@context` such as
  `"https://schema.org/"` is taken as the vocabulary IRI: `name` expands to
  `https://schema.org/name`. Term definitions published at that URL are never
  loaded, so a context that maps terms elsewhere must be given inline as an
  object.
- **An IRI left relative after expansion is an error.** This is usually a
  property with no `@context` entry. JSON-LD processors usually drop such a
  property without telling you; Fluree's reader names it instead.
- **`@direction` is not read.** Write the base direction in `@language`
  (`"ar--rtl"`), as the writer does.
- **A JSON number can lose its declared datatype in Rust.** A number typed
  anything but `xsd:integer` or `xsd:double`, such as
  `{"@value": 5, "@type": "xsd:long"}`, is written back by `rdf::serialize`
  as a bare `5`, which reads as `xsd:integer`. From Python,
  `literals="lexical"` keeps the datatype.
- **No framing:** output is compacted only with the prefixes you pass.
