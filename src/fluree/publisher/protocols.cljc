(ns fluree.publisher.protocols
  (:refer-clojure :exclude [list]))

(defprotocol Publisher
  (init [_ ledger-name opts] "Initialize a ledger, returning a ledger address.")
  (delete [_ ledger-address] "Delete a ledger and all of its entries.")
  (list [_] "Lists ledgers available on the publisher.")


  (push [_ ledger-address info] "Update the head of the ledger-address ")
  (pull [_ ledger-address] "Return the ledger for the given address."))
