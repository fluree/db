(ns fluree.indexer.protocols
  (:refer-clojure :exclude [load resolve merge]))

(defprotocol Indexer
  (init [idxr ledger-name opts] "Creates a db and returns a db-address.")
  (load [idxr db-address opts] "Loads the db for db-address and prepares it for staging and querying.")
  (resolve [idxr db-address] "Returns the db-block associated with the db-address")

  (stage [idxr db-address data opts] "Takes a db-address and some data and returns a db-info.")
  (merge [idxr db-address indexed-summary opts] "Directly merge the index summary into the db.")
  (query [idxr db-address query] "Takes a query and a db-address and returns the results.")
  (explain [idxr db-address query] "Takes a query and returns the query plan."))
