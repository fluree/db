(ns fluree.connector.protocols
  (:refer-clojure :exclude [load list]))

(defprotocol Connection
  (list [_] "List the ledgers the conn knows about.")
  (create [_ ledger-name opts] "Create a ledger an initialize an index, returns a ledger-address.")
  (transact [_ ledger-address tx opts] "Transact data, indexing it, checking it, and then commiting it.")
  (query [_ ledger-address query opts] "Query a db and get results.")
  (load [_ ledger-address opts] "Prepare a ledger for querying and transacting.")
  #_(subscribe [_ query fn]))
