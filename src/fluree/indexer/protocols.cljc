(ns fluree.indexer.protocols)

(defprotocol Indexer
  (init [idxr ledger-name opts] "Takes a ledger name and returns a db-address.")
  (stage [idxr db-address data] "Takes a db-address and some data and returns a db-info.")
  (query [idxr db-address query] "Takes a query and a db-address and returns the results.")
  (explain [idxr db-address query] "Takes a query and returns the query plan."))
