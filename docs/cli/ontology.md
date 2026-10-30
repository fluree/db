# fluree ontology

Ontology tools for document extraction. An extraction model reads documents
against your ontology (the classes and properties to look for) and, optionally,
a catalog of records you already have. How the ontology is written changes the
reading a lot: labels in words, and descriptions that say what counts, find far
more than column names do. These commands show what the extraction model will
and won't read of an ontology, and rewrite one into the shape it reads well.

They read files only: no `.fluree/` directory, no ledger, and nothing is sent
anywhere.

## Usage

```bash
fluree ontology <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `check` | Report what the extraction model will and won't read of an ontology (and catalog) |
| `from` | Write an ontology in the shape the extraction model reads well: from tables (CSV, a workbook, SQL DDL), a semantic layer (Snowflake, Genie, dbt, Apache Ossie), a Neo4j schema, or RDF, optionally sliced |

Ontologies are read as Turtle (`.ttl`) or JSON-LD (`.jsonld`, `.json`); `-`
reads stdin, and `--input-format turtle|jsonld` says which when the extension
doesn't. TriG is not read yet: pass the ontology as Turtle, or a saved request
as JSON-LD. `from` also reads tables (`.csv`/`.tsv`, `.xlsx`, `.sql`) and
semantic layers: a Snowflake semantic view (`.yaml`), a Databricks Genie
export (`.json`), a dbt project (its `.yml` files or its directory) and an
Apache Ossie semantic model or ontology (`.yaml`, `.json`), and a Neo4j
schema export (`.json`). YAML and JSON are told apart by what they hold.

For the records an ontology's names link to, see [`fluree catalog`](catalog.md).

## What the extraction model reads

Of each class and property, the extraction model sees:

- its `rdfs:label`;
- its `rdfs:comment`;
- one `rdfs:subClassOf` parent (the first by IRI);
- the SHACL node shapes that target a class (their supported constraints);
- the IRI's local name (the namespace is hidden).

It does not see `rdfs:domain` or `rdfs:range` (unless a shape on the class
restates them), a second parent, `skos:altLabel`, `owl:equivalentClass`, OWL
restrictions, or other axioms.

Input it has no place for is adapted rather than refused (input policy
`ontology-adapter-v1`):

- blank nodes outside SHACL (OWL restrictions, `owl:unionOf` lists, anonymous
  classes) are dropped, with every triple that points at one;
- a class or property without `rdfs:label` takes its `skos:prefLabel`, and one
  without `rdfs:comment` its `skos:definition` (English or untagged first);
- a term still without a label takes its IRI's local name in words
  (`employer_id` reads "employer id"). A local name with no letters can't make
  a label, and the request is refused.

Catalog records are read by their SKOS text: `skos:prefLabel`, `altLabel` and
`hiddenLabel` as names, and the first 300 characters of `skos:definition`. A
record needs a SKOS label and an IRI.

A request carries at most 2,000,000 bytes, 48,000 characters of text in up to
16 windows, and 200,000 serialized tokens per window. The ontology and the
whole catalog are repeated in every window, so a large catalog leaves less
room for text.

These are the rules of the extraction model `check` mirrors; another extractor
may read more of an ontology.

## How a file is split

A file that uses an extraction request's graph roles (`xt:ontology`,
`xt:instances` in the `https://ns.flur.ee/extract#` namespace) is split by
graph, so a saved request can be checked as it is. Any other file is read as
one graph: declared classes and properties (`rdfs:Class`, `owl:Class`,
`rdf:Property`, the OWL property types), SHACL shapes and the blank nodes they
point at are the ontology; document nodes are set aside; **everything else is
a catalog record**. A vocabulary file's enumeration members (schema.org's
`schema:Female`, say) are therefore catalog records, and need SKOS labels.

## fluree ontology check

```bash
fluree ontology check <FILE> [--catalog <FILE>] [--seed <CLASSES>] [--depth <N>] [--json]
```

Applies the input policy locally and reports, in four parts:

- **Will fail**: what a request carrying this ontology and catalog would be
  refused for (no class, a term that can't be labelled, a catalog record
  without a SKOS label or an IRI, a window over its token limit, a file over
  the byte limit).
- **Read another way**: what the policy adapted, counted by kind.
- **Not read**: the ontology's triples the extraction model never sees,
  counted by predicate (`rdfs:domain`, `owl:equivalentClass`, `rdfs:subClassOf
  (a second parent)`, `(not a shown term)` for triples about something that is
  neither a class nor a property, ...).
- **Advice**: what improves a reading: descriptions where there are none,
  labels written in words rather than as column names (`employer`, not
  `employer_id`), IRIs minted from their labels, a smaller vocabulary, what
  each property connects said in its description.

It also estimates the serialized tokens the ontology and catalog cost per
window. The extraction model's tokenizer isn't part of the CLI, so this is an
estimate from character counts, good to about a tenth.

| Option | Description |
|--------|-------------|
| `--catalog <FILE>` | A catalog of SKOS records to check with the ontology |
| `--seed <CLASSES>` | Check the slice these classes reach instead (see [Slicing](#slicing)) |
| `--depth <N>` | Steps `--seed` follows (default: 1) |
| `--json` | Emit the report as JSON |
| `--input-format <FORMAT>` | `turtle` or `jsonld`, when the extension doesn't say |

Exits 1 when a request would be refused, 0 otherwise (adaptations and advice
don't fail it).

```bash
fluree ontology check ontology.ttl
fluree ontology check ontology.ttl --catalog accounts.ttl
fluree ontology check schemaorg.ttl --seed schema:Person,schema:Organization --json
```

```text
fund.ttl: 5 classes, 1 property
About 942 serialized tokens per window before document text (a window holds 200,000).

Read another way by the extraction model
  blank-node triples dropped              4  e.g. ex:Custodian
  rdfs:comment from skos:definition       1  e.g. ex:Custodian
  rdfs:label from skos:prefLabel          2  e.g. ex:Bank, ex:Custodian
  rdfs:label from the IRI                 1  e.g. ex:Asset

Not read by the extraction model
  owl:onProperty                          1
  owl:someValuesFrom                      1
  rdf:type owl:Restriction                1
  rdfs:domain                             1  e.g. ex:employer_id
  rdfs:range                              1  e.g. ex:employer_id
  rdfs:subClassOf                         1  e.g. ex:Custodian
  rdfs:subClassOf (a second parent)       1  e.g. ex:Custodian

Advice
  • 5 of 6 classes and properties have no description (rdfs:comment or skos:definition). After
    labels, descriptions help the extraction model most: say what counts and what doesn't, and
    name the words people use for it.
      ex:Asset, ex:Bank, ex:Person and 2 more
  • The extraction model reads an IRI's local name beside its label, and a local name like
    "employer_id" reads as a key column even when the label says "employer". Mint these IRIs
    from their labels.
      ex:employer_id
  ...

Nothing here would make a request fail.
```

### JSON report

```json
{
  "policy": "ontology-adapter-v1",
  "classes": 5,
  "properties": 1,
  "records": 0,
  "by_graph": false,
  "will_fail": [{"code": "no-label", "message": "...", "count": 1, "examples": ["https://..."]}],
  "adapted": {"rdfs:label from skos:prefLabel": 2},
  "not_shown": {"rdfs:domain": 1},
  "examples": {"adapted": {"rdfs:label from skos:prefLabel": ["https://..."]}, "not_shown": {}},
  "advice": [{"code": "no-description", "message": "...", "count": 3, "examples": []}],
  "estimate": {"ontology_tokens": 942, "catalog_tokens": 0, "tokens": 942, "limit": 200000, "document_chars": 48000},
  "slice": {"seeds": [], "depth": 1, "classes_kept": 0, "classes_total": 0, "properties_kept": 0, "properties_total": 0, "without_domain": 0}
}
```

`adapted` and `not_shown` use the keys an extraction response reports the same
counts under (`X-Fluree-Ontology-Adapted`, `X-Fluree-Ontology-Not-Shown`), so
the two compare line for line. `slice` appears only with `--seed`.
`estimate.document_chars` is the room left for text in one window, at most
48,000.

Finding codes: `no-ontology`, `no-class`, `no-label`, `catalog-blank-node`,
`catalog-no-label`, `window-full` and `too-many-bytes` fail a request;
`no-description`, `label-not-words`, `local-name-not-words`,
`opaque-local-name`, `large-vocabulary`, `domain-range-not-shown`,
`one-parent-shown`, `catalog-class-not-in-ontology`,
`catalog-long-definition` and `little-room-for-text` are advice.

## fluree ontology from

```bash
fluree ontology from <FILE> [--base <IRI>] [--enum-max <N>] [--catalog-out <FILE>] [--comments <FILE>] [--format turtle|jsonld] [-o <FILE>]
fluree ontology from <FILE.ttl|.jsonld> [--seed <CLASSES>] [--depth <N>] [--format turtle|jsonld] [-o <FILE>]
```

The ontology goes to stdout, or to the file `-o` names; a summary of what was
made goes to stderr. Run `fluree ontology check` on the result to see what the
extraction model will read of it.

### From tables and semantic layers

A CSV file (`.csv`, `.tsv`: one table, named for the file), a workbook
(`.xlsx`: a table per sheet, the first row its header) or SQL DDL (`.sql`:
`CREATE TABLE` statements) becomes an ontology by the rules that measurably
help the extraction model:

- **labels in words**: a table is a class labelled with its name in words,
  singular (`DAMAGE_REPORTS` becomes "damage report"), and a column a
  property labelled with its name in words (`opened_at` becomes "opened at");
- **IRIs minted from the labels** (`DamageReport`, `openedAt`), since the
  model reads the local name beside the label;
- **foreign keys become relations** from the table's class to the class of
  the table referenced, labelled without the key suffix (`employer_id`
  becomes "employer"), with a short description ("A contact's employer: a
  company.");
- **enumerated columns become a class with a catalog record per value**, the
  shape the model reads best for a closed set of kinds;
- **descriptions are copied** from `COMMENT ON TABLE|COLUMN`, inline
  `COMMENT '...'` and a table's `COMMENT = '...'`;
- **primary keys are left out**, as are views, indexes and everything that
  describes the database rather than the things in it.

Foreign keys are read from `REFERENCES` and `FOREIGN KEY` in DDL. In a CSV or a
workbook, a column named `id`, or the table's name in the singular with `_id`
(`company_id` in `companies`), is the primary key, and any other column ending
in `id` is a foreign key, to the sheet it names if there is one.

Enumerated columns come from `CHECK (col IN (...))`, a MySQL `ENUM(...)` type
or a PostgreSQL `CREATE TYPE ... AS ENUM` in DDL. In a CSV or a workbook, a
text column is enumerated when it has at most `--enum-max` distinct short
values (default 12), each repeated (at least twice as many rows as values, and
five rows). Column datatypes come from the DDL, or from the values (integers,
decimals, ISO dates and date-times, booleans; a number with a leading zero is a
code and stays text).

A **Snowflake semantic view** (or Cortex Analyst semantic model) as YAML
(`.yaml`, `.yml`) is a data dictionary, and converts the same way: tables'
descriptions and synonyms are copied ("Also called: riders, clients."),
dimensions, time dimensions and facts become properties typed from their
`data_type`, a dimension with `is_enum: true` and `sample_values` is
enumerated, and each relationship is a foreign key from its left column to the
right table (`CUSTOMER_ID` in `orders_to_customers` becomes the relation
"customer"). Metrics, filters and verified queries describe queries, not
things, and are left out.

A **Databricks Genie space** export (`.json`, the `serialized_space` its API
returns) gives tables, columns and join specs, but no descriptions: those are
Unity Catalog's table and column comments. Pass them with `--comments` as JSON,
`{"table": {"": "table comment", "column": "column comment"}}` (one query on
`information_schema` gives them). SQL snippets and instructions are left out.

A **dbt project** gives two kinds of YAML, and `from` reads both, from one file
or from the project's directory (every `.yml`/`.yaml` under it, `target/` and
`dbt_packages/` passed over):

- **semantic models** (MetricFlow): a semantic model is a class; its primary
  entity is its key; a foreign entity is a relation to the semantic model whose
  primary entity has that name (`customer` relates to the model keyed by
  `customer`); dimensions are properties (a `time` dimension a date, or a
  date-time below a day's granularity); a measure that aggregates a column
  (`sum`, `average`, `min`, `max`, `median`, `percentile`) is a decimal
  property, while counts, metrics and saved queries are left out;
- **model properties** (`schema.yml`): column descriptions are copied, an
  `accepted_values` test makes a column enumerated, a `relationships` test or
  `foreign_key` constraint makes it a foreign key, and a `primary_key`
  constraint, or `unique` with `not_null` on `id`, makes it the key.

A semantic model built on a model (`model: ref('fct_orders')`) stands for it:
it takes the model's descriptions and accepted values for the columns it uses,
and the model isn't repeated. Of the model's other columns, the ones that carry
a relation (a `relationships` test or `foreign_key` constraint) or accepted
values are kept, since a relation a metric didn't need is still something
documents talk about; the rest (audit columns, surrogate keys, columns only a
measure's expression uses) are left out, and the summary says how many. A
`relationships` test that names a replaced model (`ref('fct_orders')`) points
at the semantic model standing for it.

An **Apache Ossie** document (0.2, `.yaml` or `.json`) is read by its layer:

- a **semantic model** (`datasets`, `relationships`) converts like a Snowflake
  semantic view: fields' `description`, `datatype` and `ai_context` synonyms
  are copied, relationships are foreign keys from their `from` fields to the
  `to` dataset, and metrics are left out;
- an **ontology** (`ontology:` a list of concepts) is already conceptual, and
  is written as RDF directly, **keeping the IRIs it gives** (`iri: schema:Airport`,
  expanded with its `prefixes`). An `EntityType` concept is a class (with its
  `extends` as parents); a binary relationship is a property from the concept
  that declares it to its role's concept, or to the datatype a `ValueType`
  resolves to, described by its `description` or else its verbalization
  (`'{Flight} uses {Aircraft}'` becomes "A flight uses an aircraft."); a unary
  relationship (`'{Flight} was diverted'`) is a boolean property; one with
  three or more roles is a class with a property per role; and a value type
  whose `requires` lists its values (`Code == 'A' OR Code == 'B'`) is a class
  with a catalog record per value. `derived_by`, other `requires` and
  `identify_by` are logic, not what documents say, and are left out.

A **Neo4j graph's schema** (`.json`) is close to an ontology already. Export it
with APOC (`CALL apoc.meta.schema() YIELD value RETURN value`, saved as JSON),
which gives labels, their typed properties and each relationship type's
direction and end labels; or with the built-in `CALL db.schema.visualization()`,
which gives labels and relationship types only. A label is a class (`MovieGenre`
becomes "movie genre"); a node property is a property typed from its Neo4j type
(a unique `id`, `uuid` or `<label>Id` is a key, and left out); an outgoing
relationship type is a relation labelled in words (`ACTED_IN` becomes "acted
in") and described as the sentence it reads ("A person acted in a movie."), with
no range when it reaches several labels. Relationship properties have no place
on an RDF property: they are left out, and the summary counts them.

A foreign key named for a code or number (`HIRE_CODE` into `PRODUCTS`) takes
its target's label ("product"), since the name says what it is, not what role
it plays. A key used by several tables is one relation, described for them
all ("The account of a contact, booking or quote.").

The DDL is parsed with a generic SQL dialect, then PostgreSQL's, MySQL's and
Snowflake's; statements none of them can parse are skipped and counted.

| Option | Description |
|--------|-------------|
| `--base <IRI>` | Namespace for minted IRIs (default: `https://example.com/<file name>/`) |
| `--enum-max <N>` | Most distinct values an enumerated column may have, for CSV and workbooks (default: 12; 0 never enumerates) |
| `--catalog-out <FILE>` | Write enumerated columns' records here instead of with the ontology |
| `--comments <FILE>` | Unity Catalog comments for a Genie export (JSON) |
| `--input-format <FORMAT>` | `turtle`, `jsonld`, `csv`, `xlsx`, `sql`, `snowflake`, `genie`, `dbt`, `ossie` or `neo4j`, when the extension or contents don't say |
| `--format <FORMAT>` | `turtle` (default) or `jsonld` |
| `-o, --output <FILE>` | Write to this file instead of stdout |

```bash
fluree ontology from schema.sql --base https://example.com/crm/ > ontology.ttl
fluree ontology from accounts.csv -o ontology.ttl
fluree ontology from crm.xlsx --catalog-out kinds.ttl -o ontology.ttl
fluree ontology from sales_view.yaml --base https://example.com/sales/ > ontology.ttl
fluree ontology from genie_space.json --comments uc_comments.json > ontology.ttl
fluree ontology from ./my_dbt_project --base https://example.com/shop/ > ontology.ttl
fluree ontology from flights.ossie.yaml > ontology.ttl
fluree ontology from neo4j-schema.json --base https://example.com/movies/ > ontology.ttl
```

From this DDL:

```sql
CREATE TABLE companies (company_id SERIAL PRIMARY KEY, name VARCHAR(200));
CREATE TABLE contacts (
    id INTEGER PRIMARY KEY,
    employer_id INTEGER REFERENCES companies(company_id),
    status VARCHAR(10) CHECK (status IN ('active', 'left'))
);
COMMENT ON TABLE companies IS 'Customer accounts and partners.';
```

`fluree ontology from crm.sql --base https://example.com/crm/` writes (in part):

```turtle
crm:Company a rdfs:Class ;
    rdfs:comment "Customer accounts and partners." ;
    rdfs:label "company" .

crm:employer a rdf:Property ;
    rdfs:comment "A contact's employer: a company." ;
    rdfs:domain crm:Contact ;
    rdfs:label "employer" ;
    rdfs:range crm:Company .

crm:Status a rdfs:Class ;
    rdfs:label "status" .

<https://example.com/crm/Status/active> a crm:Status ;
    skos:prefLabel "active" .
```

What tables can't say is what the extraction model gains most from next:
descriptions that say what counts and name the words people use ("the desk"
for the hire desk). `check` lists the terms without one.

### From RDF

An RDF ontology (Turtle or JSON-LD) is written back with what the extraction
model reads made explicit:

- SKOS labels and definitions copied into `rdfs:label` and `rdfs:comment`;
- a label made from the IRI where there is none;
- labels that read as identifiers rewritten in words (`worksFor` becomes
  "works for", `employer_id` "employer id");
- `owl:Class` and the OWL property types also declared `rdfs:Class` and
  `rdf:Property`;
- OWL restrictions and other blank-node constructs dropped.

Domains, ranges and other axioms are kept: the extraction model doesn't see
them, but a reading can still be checked against them. Catalog records in the
file are kept, and one with an `rdfs:label` but no SKOS label gets it as
`skos:prefLabel` (and its `rdfs:comment` as `skos:definition`).

| Option | Description |
|--------|-------------|
| `--seed <CLASSES>` | Keep only what these classes reach (see [Slicing](#slicing)) |
| `--depth <N>` | Steps `--seed` follows (default: 1) |
| `--format <FORMAT>` | `turtle` (default) or `jsonld` |
| `-o, --output <FILE>` | Write to this file instead of stdout |
| `--input-format <FORMAT>` | `turtle` or `jsonld`, when the extension doesn't say |

```bash
fluree ontology from fibo-module.ttl > ontology.ttl
fluree ontology from schemaorg.ttl --seed Person,Organization -o people.ttl
fluree ontology check people.ttl
```

## Slicing

Every class and property in the ontology is shown to the extraction model in
every window. A whole public vocabulary costs tens of thousands of tokens and
blurs which of several overlapping terms applies (schema.org alone is over a
thousand classes and 1,600 properties), so slice it to what your documents
need. `--seed` takes classes as IRIs (`<https://schema.org/Person>`), prefixed
names (the file's prefixes, or `rdf`, `rdfs`, `owl`, `skos`, `sh`, `xsd`), or
labels and local names (`Person`), comma-separated or repeated. The slice
keeps:

1. the seed classes and all their ancestors;
2. the properties whose domain is one of them (`rdfs:domain`, schema.org's
   `domainIncludes`, or a member of an `owl:unionOf` domain), and the classes
   in those properties' ranges, with their ancestors;
3. step 2 again from the classes just added, `--depth` times in all.

A property with no domain is left out (the report counts them). Catalog
records are kept when they are typed with a kept class or have no type.

## See also

- [`catalog`](catalog.md): a catalog of records from rows you already have
- [`doc`](doc.md): ingest documents and extract entities and relations with an ontology
- [`validate`](validate.md): validate data against SHACL shapes
