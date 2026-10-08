# Fluree for Python

The `fluree` package runs [Fluree](https://flur.ee), a graph database with time
travel, history and fine-grained policy, inside your Python process. There is
no server to start.

```sh
pip install fluree                # or "fluree[pandas]", "fluree[polars]"
```

- [Guide](guide.md): connecting, writing, querying, transactions, search,
  branches, policy, asyncio and the rest, with examples.
- [API reference](api/fluree/index): every class, method and function, and
  the [types](types.md) their signatures name.
- [Fluree documentation](https://fluree.github.io/db/): concepts, the query
  languages and ledger configuration. Its queries run unchanged through
  `ledger.query()`; [Using Fluree from Python](https://fluree.github.io/db/getting-started/python.html)
  maps the rest to this package.

```{toctree}
:hidden:

guide
api/fluree/index
types
```
