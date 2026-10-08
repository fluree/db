# Types in signatures

Method signatures name these types. They describe what an argument accepts;
pass the plain values shown.

(type-query)=
## Query

`str | dict`: SPARQL or Cypher text, or a JSON-LD query.

(type-data)=
## Data

`str | dict | list | os.PathLike`: JSON-LD (a dict, a list, or JSON text),
Turtle or TriG text, a path to a file holding any of them, or a list of
{py:class}`fluree.Quad`.

(type-format)=
## Format

`"jsonld" | "turtle" | "trig"`: the format of written data, when the text or
file extension does not settle it.

(type-exportformat)=
## ExportFormat

`"turtle" | "trig" | "ntriples" | "nquads" | "jsonld"`.

(type-rdfformat)=
## RdfFormat

`"turtle" | "trig" | "ntriples" | "nquads" | "jsonld"`: the formats
{py:func}`fluree.parse` reads and {py:func}`fluree.serialize` writes.

(type-language)=
## Language

`"sparql" | "cypher" | "jsonld"`: a query's language, when its text does not
settle it.

(type-selectlanguage)=
## SelectLanguage

`"sparql" | "cypher"`: the languages whose queries return a table.

(type-sqldialect)=
## SqlDialect

`"trino" | "postgres" | "mysql" | "sqlite"`.

(type-mergestrategy)=
## MergeStrategy

`"take-both" | "abort" | "take-source" | "take-branch"`: how a merge settles a
property both branches changed; see {py:meth}`fluree.Ledger.merge`.

(type-rebasestrategy)=
## RebaseStrategy

`"take-both" | "abort" | "take-source" | "take-branch" | "skip"`; see
{py:meth}`fluree.Ledger.rebase`.

(type-revertstrategy)=
## RevertStrategy

`"abort" | "take-source" | "take-branch"`; see {py:meth}`fluree.Ledger.revert`.

(type-commitref)=
## CommitRef

`int | str | Commit`: a commit's `t`, its id, a prefix of its hex digest, or a
{py:class}`fluree.Commit`.

(type-secret)=
## Secret

`str | EnvVar`: a value, or a {py:class}`fluree.EnvVar` naming the environment
variable to read it from.

(type-auth)=
## Auth

{py:class}`fluree.Bearer` or {py:class}`fluree.OAuth2`.
