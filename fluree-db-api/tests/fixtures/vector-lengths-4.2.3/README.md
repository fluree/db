A ledger written by the v4.2.3 CLI, in a project made by `fluree --direct init`:

```sh
fluree --direct create ex
fluree --direct insert -l ex -e '{"@context":{"ex":"http://example.org/","ex:embedding":{"@type":"@vector"}},"@graph":[{"@id":"ex:a","ex:name":"a","ex:embedding":[0.125,0.25,0.375]}]}'
fluree --direct index ex
fluree --direct insert -l ex -e '{"@context":{"ex":"http://example.org/","ex:embedding":{"@type":"@vector"}},"@graph":[{"@id":"ex:b","ex:name":"b","ex:embedding":[0.5,0.625]}]}'
```

`storage/` is the project's `.fluree/storage`, without its lock files: the
ledger `ex:main`, indexed at t=1, with a two-element vector on
`ex:embedding` at t=2 beside the three-element one the index holds.
