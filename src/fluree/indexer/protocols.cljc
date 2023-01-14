(ns fluree.indexer.protocols)

(defprotocol Indexer
  (init [idxr ledger-name opts] "Creates a db and returns a db-address.")
  (discard [idxr db-address] "Discards the db associated with the db-address.")
  (stage [idxr db-address data] "Takes a db-address and some data and returns a db-info.")
  (query [idxr db-address query] "Takes a query and a db-address and returns the results.")
  (explain [idxr db-address query] "Takes a query and returns the query plan."))
