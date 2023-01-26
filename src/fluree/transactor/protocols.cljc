(ns fluree.transactor.protocols
  (:refer-clojure :exclude [resolve load]))

(defprotocol Transactor
  (init [txr ledger-name] "Establish a new transaction head for the given ledger.")
  (head [txr ledger-name] "Return a summary of the head transaction for the given ledger.")
  (transact [txr ledger-name tx] "Persist the transaction and return a tx summary.")
  (resolve [txr tx-address] "Returns the transaction that corresponds to the tx-address."))
