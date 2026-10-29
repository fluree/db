# fluree catalog

Catalog tools for document extraction. An extraction model reading documents
against an ontology can also link the names it finds to records you already
have (customers, suppliers, products, staff): a **catalog**. Given one, "Daxin"
in an email becomes a link to your account record for Daxin Scientific Supply
Co., not just a mention of some organization. On linking, the catalog does most
of the work, more than how the ontology is written.

The extraction model reads catalog records by their SKOS text: their names
(`skos:prefLabel`, `altLabel`, `hiddenLabel`) and a short description
(`skos:definition`, the first 300 characters). The whole catalog goes into
every extraction window, so its size matters: about 58 serialized tokens a
record, so roughly 3,000 records fill a 200,000-token window on their own.

These commands read files only: no `.fluree/` directory, no ledger, and nothing
is sent anywhere.

## Usage

```bash
fluree catalog <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `from` | Write a catalog of SKOS records from a CSV file or a workbook sheet |

## fluree catalog from

```bash
fluree catalog from <FILE> --class <CLASS> --label <COLUMN> [--alt <COLUMN>]... [--code <COLUMN>]...
    [--definition <COLUMN> | --describe <COLUMNS>] [--id <COLUMN>] [--base <IRI>]
    [--ontology <FILE>] [--sheet <NAME>] [--format turtle|jsonld] [-o <FILE>]
```

Each row of a CSV file (`.csv`, `.tsv`) or a workbook sheet (`.xlsx`) becomes
one record of `--class`:

- `skos:prefLabel` from the `--label` column;
- `skos:altLabel` from `--alt` columns: other names, nicknames, short forms.
  Several names in one cell are split on `;` or `|`. An email address goes to
  `skos:hiddenLabel`;
- `skos:hiddenLabel` from `--code` columns: account numbers, tickers and other
  codes a document may use for the record, which aren't names;
- `skos:definition` from the `--definition` column, or written from the
  `--describe` columns as `industry: Lab supplies; city: Taipei`.

A record's IRI is `--base` followed by its `--id` value (or its label), as a
slug. Rows with the same IRI are one record, their names pooled.

The class is an IRI, or a name. With `--ontology`, a name is looked up in that
ontology (by label, local name or prefixed name), so the records are typed with
the class the extraction model will read. Without it, a name is minted under
`--base`.

The catalog goes to stdout, or to the file `-o` names. A summary goes to
stderr: how many records, rows folded together or left out, and the catalog's
size in serialized tokens, with a warning when it leaves little room for text
in a window, or doesn't fit one at all.

| Option | Description |
|--------|-------------|
| `--class <CLASS>` | The class every record is typed with |
| `--label <COLUMN>` | The column holding each record's name |
| `--alt <COLUMN>` | Columns holding other names for it (repeatable) |
| `--code <COLUMN>` | Columns holding codes a document may use for it (repeatable) |
| `--definition <COLUMN>` | The column holding a short description |
| `--describe <COLUMNS>` | Columns to write a description from (comma-separated) |
| `--id <COLUMN>` | The column holding each record's id, for its IRI (default: the label) |
| `--base <IRI>` | Namespace for record IRIs (default: `https://example.com/<file name>/`) |
| `--ontology <FILE>` | An ontology to find `--class` in |
| `--sheet <NAME>` | The workbook sheet to read (default: the first) |
| `--format <FORMAT>` | `turtle` (default) or `jsonld` |
| `-o, --output <FILE>` | Write to this file instead of stdout |

```bash
fluree catalog from accounts.csv --class organization --ontology ontology.ttl \
  --label name --alt aliases --code account_no --describe industry,city --id account_no \
  --base https://example.com/crm/account/ > accounts.ttl
fluree ontology check ontology.ttl --catalog accounts.ttl
```

From

```text
account_no,name,aliases,industry,city
A-100,Daxin Scientific Supply Co.,Daxin; DSS,Lab supplies,Taipei
```

it writes

(with `crm:` declared in `ontology.ttl`, and `accounts:` named for the file)

```turtle
accounts:a-100 a crm:Organization ;
    skos:altLabel "DSS", "Daxin" ;
    skos:definition "industry: Lab supplies; city: Taipei" ;
    skos:hiddenLabel "A-100" ;
    skos:prefLabel "Daxin Scientific Supply Co." .
```

Names people actually use ("the desk", "Daxin") are worth more than any other
column: aliases are how a catalog resolves nicknames and short forms.

## See also

- [`ontology`](ontology.md): check what the extraction model reads of an ontology and catalog, and make an ontology from tables
