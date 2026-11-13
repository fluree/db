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
- **JSON-LD numbers** take the datatype JSON-LD's conversion to RDF gives
  them ([JSON-LD 1.1 API § 8.2.2](https://www.w3.org/TR/json-ld11-api/#object-to-rdf-conversion)),
  which goes by the number's value and not its spelling: a number with a
  fractional part, or of 10^21 and more, is an `xsd:double`, and any other an
  `xsd:integer`. So `1.0` and `1e3` read as the integers `1` and `1000`, as
  JSON itself makes no difference between `1.0` and `1`. For a double, give
  the value its type: `{"@value": 1.0, "@type": "xsd:double"}`, or a term
  whose `@type` is `xsd:double`. A number keeps every digit it is written
  with, up to the precision of a double. A ledger write reads numbers
  the same way.
- **Blank nodes** keep the document's labels. An anonymous blank node (`[]`, a
  collection, an annotation, a JSON-LD node with no `@id`) is labeled `bN`,
  avoiding any label the document uses. Every parsed document can therefore be
  written in any format. A JSON-LD label that starts with `-`, which no other
  syntax allows, gets one more `-`: `_:-b1` is read as `--b1`.
- **Putting two documents together:** every parse numbers its anonymous blank
  nodes from `b1`, so two parsed documents share those labels, and their
  statements put together would make one node of two. Give each parse its own
  prefix (`ParseOptions::blank_prefix` with `rdf::parse_document_with`; in
  Python, `fluree.parse(…, blank_prefix="a")`), and its anonymous nodes are
  labeled `a1`, `a2`, … instead. Labels a document writes itself are kept
  either way, so a label two documents both write still names one node.
- **Order:** statements come in the order the document writes them
  (`rdf::parse_document` in Rust; `fluree.parse` in Python). A TriG or
  N-Quads document's graphs interleave as written, an annotation stands where
  it is written, and a JSON-LD node's statement comes before those of the node
  nested in it, with a list's cells where the list is.
- **Prefixes:** the prefixes a Turtle or TriG document declares are kept, in
  order (`Document::prefixes`; in Python, the `prefixes` of the list
  `fluree.parse` returns).
- **Text:** Python also takes UTF-8 `bytes`, such as a request body.
- **Errors:** a malformed document raises an error naming its line and column.
- **Memory:** a document is read whole, and parsing holds the text, the parsed
  dataset and, in Python, every `Quad` at once: roughly ten times the
  document's size from Python. To load a large file into a ledger, use
  [bulk import](../cli/create.md) rather than parsing it and inserting the
  quads.

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
- **Order:** `rdf::serialize_document` (and `fluree.serialize`) keeps the
  statements' order. N-Triples and N-Quads are written one line per statement,
  in order, so the statements of several graphs interleave as given, and an
  N-Quads document that is read and written back is the same lines. The other
  formats write the graphs in the order the statements first name them, the
  default graph among them, and Turtle and TriG gather each subject's
  statements under one subject, with `;` and `,`, subjects in the order they
  first appear. `rdf::serialize_ordered` takes a dataset and the order of its
  graphs, and writes each graph's statements together in every format.
  `rdf::serialize` writes the default graph first and the named graphs by
  name.
- **Expanded JSON-LD:** `rdf::serialize_expanded_jsonld` (in Python,
  `fluree.serialize(quads, "jsonld", expanded=True)`) writes the
  `json-ld#expanded` profile: an array of node objects with full IRIs, no
  context, and every value in an array.
- **Reifications:** when the triple is asserted, the reification is written as
  an annotation where the syntax has one (`~` in Turtle and TriG,
  `@annotation` in JSON-LD). Otherwise it is written as `rdf:reifies` or
  `@reifies`.
- **Literals:** each literal is written in its lexical form. A Python `float`
  is written in its shortest form (`0.9957`, not `9.957E-1`).
- **JSON-LD numbers and booleans:** a literal that holds a number or a
  boolean, as one read from a JSON-LD number does, is written as a bare JSON
  value only where JSON-LD reads the same literal back. An `xsd:double` with
  no fractional part is written
  `{"@value": 1.0, "@type": "http://www.w3.org/2001/XMLSchema#double"}`, since
  a bare `1.0` is an integer in JSON-LD, and a number or boolean under another
  datatype (`xsd:long`, a datatype of your own) is written with its `@type`.
  Python writes every `float` as a typed value.
- **Collections:** Turtle and TriG write a collection as `( … )` when its
  nodes are blank nodes used nowhere else and its `rdf:rest` chain ends in
  `rdf:nil`. Otherwise its `rdf:first` and `rdf:rest` triples are written as
  they are. Writing parsed quads to a ledger therefore stores a collection as
  a list, as inserting the document's text does.

## JSON-LD limits

- **Remote contexts are not fetched.** A string `@context` such as
  `"https://schema.org/"` is taken as the vocabulary IRI: `name` expands to
  `https://schema.org/name`. Term definitions published at that URL are never
  loaded, so a context that maps terms elsewhere must be given inline as an
  object.
- **An IRI left relative after expansion is an error.** This is usually a
  property with no `@context` entry. JSON-LD processors usually drop such a
  property without telling you; Fluree's reader names it instead.
- **`@direction` is read from a value object only.** It joins the language
  tag (`ar` and `rtl` read as `"…"@ar--rtl`), and the writer gives it back as
  `@language` and `@direction`. A default `@direction` in a `@context` is not
  read, and a direction with no language is dropped.
- **No framing:** output is compacted only with the prefixes you pass.

## Canonicalization

`rdf::canonicalize` (in Python, `fluree.canonicalize(quads)`) applies
[RDF Dataset Canonicalization](https://www.w3.org/TR/rdf-canon/) (RDFC-1.0).
It labels a dataset's blank nodes `_:c14n0`, `_:c14n1`, … from its
statements alone, drops duplicate statements, and writes canonical N-Quads
with the lines in code point order. Two datasets that hold the same
statements give the same text, whatever their blank node labels and
statement order, so the text can be compared, hashed or signed.

- **Hash:** SHA-256 by default; SHA-384 on request.
- **Poison graphs:** some datasets have blank nodes that no statement tells
  apart, and canonicalizing them can take time exponential in their size.
  Such a dataset is refused once it needs more deep comparisons (calls to the
  algorithm's Hash N-Degree Quads step) than its limit: by default 16 per
  blank node, at least 1,024. `CanonicalizeOptions::max_deep_calls` (in
  Python, `max_deep_calls=`) sets the limit; lower it for input from an
  untrusted source.
- **Literals:** a literal is written with the spelling and datatype it has in
  the dataset. In Python, parse with `literals="lexical"` to keep them: by
  default a literal becomes its Python value, and `"01"^^xsd:long` and
  `"1"^^xsd:integer` then canonicalize alike.
- **RDF 1.2:** RDFC-1.0 defines RDF 1.1 datasets. A triple term is written
  `<<( s p o )>>`, as N-Quads 1.2 writes it, and its blank nodes are labeled
  with the quad's.
- **When it runs:** only when called. Reading, writing and transactions never
  canonicalize.

The implementation passes the W3C RDFC-1.0 test suite, its SHA-384,
identifier-map and poison-graph tests included: all 86 tests of the suite
run in CI.

