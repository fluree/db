A ledger bulk-imported by the v4.2.3 CLI from `source.ttl`, in an empty
directory:

```sh
fluree --direct init
fluree --direct create ex:main --from source.ttl
```

`storage/` is the project's `.fluree/storage`, without its lock files. The
import indexes as it reads, so the ledger is fully indexed at t=1.
