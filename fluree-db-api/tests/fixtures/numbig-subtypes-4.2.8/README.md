Ledgers written and indexed by the v4.2.8 CLI (`fluree` from Homebrew). A v4
index types every NumBig arena value by its decoded value, so an integer beyond
`i64` reads back as `xsd:integer` whatever XSD subtype it was written with, and
two subtypes of one value are one fact. A v5 binary serves these indexes the
same way until the ledger's first v5 build, a full rebuild from commits, which
keeps each declared subtype.

- `v4/numbig:main`: `seed.ttl` below, indexed once (a full build). No commit
  follows the index.
- `v4/numbig-changed:main`: a big `xsd:unsignedLong`, indexed, then replaced by
  `5` in an update, and indexed again (an incremental build). The update's
  commit (t=2) retracts the value under `xsd:integer`, the type the v4 index
  read it back with, not the `xsd:unsignedLong` it was written with.

`storage/` is the file store the run below wrote, without its lock files and
WAL directory.

```sh
fluree init
cat > seed.ttl <<'EOF'
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:s ex:n "18446744073709551615"^^xsd:unsignedLong , "18446744073709551615"^^xsd:nonNegativeInteger .
ex:s ex:m "18446744073709551615"^^xsd:unsignedLong .
ex:t ex:n "18446744073709551615"^^xsd:integer .
ex:d ex:n "123456789012345678.5"^^xsd:decimal , 1.5 .
ex:i ex:n "9223372036854775807"^^xsd:unsignedLong .
EOF
fluree create v4/numbig
fluree insert v4/numbig -f seed.ttl
fluree reindex v4/numbig

cat > changed.ttl <<'EOF'
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:a ex:n "18446744073709551615"^^xsd:unsignedLong .
EOF
fluree create v4/numbig-changed
fluree insert v4/numbig-changed -f changed.ttl
fluree reindex v4/numbig-changed
fluree update v4/numbig-changed 'PREFIX ex: <http://example.org/> DELETE { ex:a ex:n ?o } INSERT { ex:a ex:n 5 } WHERE { ex:a ex:n ?o }'
fluree index v4/numbig-changed
```

What v4.2.8 answers on `v4/numbig:main` (`PREFIX ex: <http://example.org/>`,
`PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>`):

| Query | v4.2.8 |
|---|---|
| `SELECT ?s ?o (DATATYPE(?o) AS ?dt) WHERE { ?s ex:n ?o }` | 5 rows: `ex:d` 123456789012345678.5 and 1.5 `xsd:decimal`; `ex:i` 9223372036854775807 `xsd:unsignedLong`; `ex:s` and `ex:t` 18446744073709551615 `xsd:integer` |
| `SELECT (COUNT(*) AS ?c) WHERE { ?s ex:n ?o }` | 5 |
| `SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o }` | 4 |
| `SELECT ?s WHERE { ?s ex:n "18446744073709551615"^^xsd:unsignedLong }` | no rows |
| `SELECT ?s WHERE { ?s ex:n 18446744073709551615 }` | `ex:s`, `ex:t` |

On `v4/numbig-changed:main`, `SELECT ?o (DATATYPE(?o) AS ?dt) WHERE { ex:a ex:n ?o }`
answers one row, `5` `xsd:integer`.
