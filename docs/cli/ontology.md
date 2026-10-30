# fluree ontology

Ontology tools for document extraction. An extraction model reads documents
against your ontology (the classes and properties to look for) and, optionally,
a catalog of records you already have. How the ontology is written changes the
reading a lot: labels in words, and descriptions that say what counts, find far
more than column names do. `check` shows what an **HTTP reader** will and won't
read of an ontology, and `from` rewrites one into the shape extraction models
read well.

An HTTP reader is an extraction model reached over HTTP that reads a whole
document against your ontology: the `[doc.reader]` that `fluree doc ingest`
can read with. `check` mirrors the ontology input policy such a reader applies.
`fluree doc ingest`'s language-model extractor (`[doc.llm]`) reads more of an
ontology than an HTTP reader does (domains and ranges, `schema:domainIncludes`,
every parent class), so the "not read" counts and the advice about domains,
ranges and parents describe an HTTP reader only.

They read files only: no `.fluree/` directory, no ledger, and nothing is sent
anywhere.

## Usage

```bash
fluree ontology <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `check` | Report what an HTTP reader will and won't read of an ontology (and catalog) |
| `from` | Write an ontology in the shape extraction models read well: from tables (CSV, a workbook, SQL DDL), or from RDF, optionally sliced |

Ontologies are read as Turtle (`.ttl`) or JSON-LD (`.jsonld`, `.json`); `-`
reads stdin, and `--input-format turtle|jsonld` says which when the extension
doesn't. TriG is not read yet: pass the ontology as Turtle, or a saved request
as JSON-LD. `from` also reads tables (`.csv`/`.tsv`, `.xlsx`, `.sql`) and
semantic layers (a Snowflake semantic view as `.yaml`, a Databricks Genie
export as `.json`).

For the records an ontology's names link to, see [`fluree catalog`](catalog.md).

## What an HTTP reader reads

Of each class and property, an HTTP reader sees:

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

These are the rules of an HTTP reader using input policy `ontology-adapter-v1`;
`fluree doc ingest`'s language-model extractor reads more of an ontology (see
above).

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
- **Not read**: the ontology's triples the reader never sees,
  counted by predicate (`rdfs:domain`, `owl:equivalentClass`, `rdfs:subClassOf
  (a second parent)`, `(not a shown term)` for triples about something that is
  neither a class nor a property, ...).
- **Advice**: what improves a reading: descriptions where there are none,
  labels written in words rather than as column names (`employer`, not
  `employer_id`), IRIs minted from their labels, a smaller vocabulary, what
  each property connects said in its description.

It also estimates the serialized tokens the ontology and catalog cost per
window. The reader's tokenizer isn't part of the CLI, so this is an
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

Read another way by the reader
  blank-node triples dropped              4  e.g. ex:Custodian
  rdfs:comment from skos:definition       1  e.g. ex:Custodian
  rdfs:label from skos:prefLabel          2  e.g. ex:Bank, ex:Custodian
  rdfs:label from the IRI                 1  e.g. ex:Asset

Not read by the reader
  owl:onProperty                          1
  owl:someValuesFrom                      1
  rdf:type owl:Restriction                1
  rdfs:domain                             1  e.g. ex:employer_id
  rdfs:range                              1  e.g. ex:employer_id
  rdfs:subClassOf                         1  e.g. ex:Custodian
  rdfs:subClassOf (a second parent)       1  e.g. ex:Custodian

Advice
  • 5 of 6 classes and properties have no description (rdfs:comment or skos:definition). After
    labels, descriptions help the reader most: say what counts and what doesn't, and
    name the words people use for it.
      ex:Asset, ex:Bank, ex:Person and 2 more
  • The reader reads an IRI's local name beside its label, and a local name like
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

Finding codes: `no-ontology`, `no-class`, `no-label`, `owl-machinery`, `catalog-blank-node`,
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
made goes to stderr. Run `fluree ontology check` on the result to see what an
HTTP reader will read of it.

### From tables and semantic layers

A CSV file (`.csv`, `.tsv`: one table, named for the file), a workbook
(`.xlsx`: a table per sheet, the first row its header) or SQL DDL (`.sql`:
`CREATE TABLE` statements) becomes an ontology by the rules that measurably
help an extraction model:

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

Keys and foreign keys are read from `PRIMARY KEY`, `REFERENCES` and
`FOREIGN KEY` in `CREATE TABLE`, and from `ALTER TABLE … ADD CONSTRAINT`, which
is where pg_dump writes them. In a CSV or a workbook, a column named `id`, or
the table's name in the singular with `_id` (`company_id` in `companies`,
`CustomerID` in `Customers`), is the primary key; any other column ending in
`id` is a foreign key when it names another sheet (`customer_id` beside a
`customers` sheet), and otherwise stays a value (`tax_id` reads "tax id").

Enumerated columns come from `CHECK (col IN (...))` (or pg_dump's
`(col)::text = ANY (ARRAY[...])`), a MySQL `ENUM(...)` type or a PostgreSQL
`CREATE TYPE ... AS ENUM` in DDL. In a CSV or a workbook, a
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
enumerated, and each relationship is a foreign key from its many side to its
one side (`CUSTOMER_ID` in `orders_to_customers` becomes the relation
"customer"). Metrics, filters and verified queries describe queries, not
things, and are left out. Values are read as written: a dimension named `NO`
or a sample value `007` stays that, rather than YAML's `false` or `7`, and a
key given twice keeps its last value.

A **Databricks Genie space** export (`.json`, the `serialized_space` its API
returns) gives tables, columns (with any synonyms and descriptions the space
sets, as strings or lists of strings; columns it excludes are left out) and
join specs, read in the direction their relationship type gives (a one-to-many
join is turned round; a many-to-many one makes no key). Table and column
descriptions mostly live in Unity Catalog's comments instead: pass them with
`--comments` as JSON, `{"table": {"": "table comment", "column": "column
comment"}}` (one query on `information_schema` gives them). Columns the comments
name that the export doesn't list are added, since an export may list only the
columns that have settings. SQL snippets and instructions are left out. Joins
naming a table the export doesn't have, join SQL with no `` `alias`.`column` ``
pairs, and comments for tables the export doesn't have are counted in the
summary.

A foreign key keeps its words without the key suffix (`origin_airport_code`
becomes "origin airport", `employer_no` "employer"), and takes the target's
label only when nothing is left (`id`). A key with the same name in several
tables is one relation, described for them all ("The account of a contact,
booking or quote.").

The DDL is parsed with a generic SQL dialect, then PostgreSQL's, MySQL's and
Snowflake's. When none parses the whole file (mysqldump's `DELIMITER`, say),
each statement is tried on its own, split where the SQL tokenizer finds a
semicolon (so one inside a string doesn't cut a statement short), and the
statements none can parse are skipped and counted. Tables of the same name in
two schemas (`crm.users`, `billing.users`) stay apart, named with their schema.

Two source names never become one term. Two columns of the same name in
different tables (`name`) share one property; any other clash gets a suffixed
IRI, and a label already taken is qualified with the column's full name
(`employer id` beside `employer`) or the table's name in front (`ticket status`
beside `status`). Enumerated values that read alike in an IRI (`Net 30`,
`net-30`) keep records of their own. Everything left out or qualified is
counted in the summary on stderr, and a file that yields no classes is an error,
not an empty ontology.

| Option | Description |
|--------|-------------|
| `--base <IRI>` | Namespace for minted IRIs (default: `https://example.com/<file name>/`) |
| `--enum-max <N>` | Most distinct values an enumerated column may have, for CSV and workbooks (default: 12; 0 never enumerates) |
| `--catalog-out <FILE>` | Write enumerated columns' records here instead of with the ontology |
| `--comments <FILE>` | Unity Catalog comments for a Genie export (JSON) |
| `--input-format <FORMAT>` | `turtle`, `jsonld`, `csv`, `xlsx`, `sql`, `snowflake` or `genie`, when the extension doesn't say |
| `--format <FORMAT>` | `turtle` (default) or `jsonld` |
| `-o, --output <FILE>` | Write to this file instead of stdout |

```bash
fluree ontology from schema.sql --base https://example.com/crm/ > ontology.ttl
fluree ontology from accounts.csv -o ontology.ttl
fluree ontology from crm.xlsx --catalog-out kinds.ttl -o ontology.ttl
fluree ontology from sales_view.yaml --base https://example.com/sales/ > ontology.ttl
fluree ontology from genie_space.json --comments uc_comments.json > ontology.ttl
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

What tables can't say is what an extraction model gains most from next:
descriptions that say what counts and name the words people use ("the desk"
for the hire desk). `check` lists the terms without one.

### From RDF

An RDF ontology (Turtle or JSON-LD) is written back with what an HTTP reader
reads made explicit:

- SKOS labels and definitions moved into `rdfs:label` and `rdfs:comment` (the
  SKOS statement a fallback would read is replaced by the RDFS one);
- a label made from the IRI where there is none;
- labels that read as identifiers rewritten in words (`worksFor` becomes
  "works for", `employer_id` "employer id");
- `owl:Class` and the OWL property types also declared `rdfs:Class` and
  `rdf:Property`;
- OWL restrictions and other blank-node constructs dropped;
- OWL axioms written as nodes of their own (`[] a owl:AllDisjointClasses`) and
  declarations of annotation properties and datatypes dropped, since a reader
  would read them as catalog records and refuse the request; the summary counts
  them.

Domains, ranges and other axioms are kept: an HTTP reader doesn't see
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

Every class and property in the ontology is shown to the reader in
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
- [`doc`](doc.md): `fluree doc ingest` extracts with an ontology; its
  language-model extractor reads more of an ontology than an HTTP reader does
- [`validate`](validate.md): validate data against SHACL shapes
