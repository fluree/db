(ns fluree.connector.protocols
  (:refer-clojure :exclude [load]))

(defprotocol Connection
  (create [_ ledger-name opts] "Create a ledger an initialize an index, returns a ledger-address.")
  (transact [_ ledger-address tx opts] "Transact data, indexing it, checking it, and then commiting it.")
  (query [_ db-address query opts] "Query a db and get results.")
  (load [_ query opts])
  (subscribe [_ query]))
