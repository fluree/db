(ns fluree.publisher.protocols
  (:refer-clojure :exclude [list resolve]))

(defprotocol Publisher
  (init [_ ledger-name opts] "Initialize a ledger, returning a ledger address.")
  (list [_] "Lists ledgers available on the publisher.")
  (resolve [_ ledger-path] "Return the ledger for the given address.")
  (publish [_ ledger-path info] "Update the head of the ledger-address "))
