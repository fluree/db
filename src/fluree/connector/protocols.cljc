(ns fluree.connector.protocols
  (:refer-clojure :exclude [load list]))

(defprotocol Connection
  (list [_] "List the ledgers the conn knows about.")
  (create [_ ledger-name opts] "Create a ledger an initialize an index, returns a ledger-address.")
  (transact [_ ledger-name tx opts] "Transact data, indexing it, checking it, and then commiting it.")
  (query [_ ledger-name query opts] "Query a db and get results.")
  (load [_ ledger-name opts] "Prepare a ledger for querying and transacting.")

  (subscribe [idxr ledger-name cb opts] "Register a listener with a ledger to receive new db-blocks and new db-root notifications.")
  (unsubscribe [idxr ledger-name subscription-key] "Unregister the listener to stop receiving updates."))
