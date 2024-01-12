(ns fluree.store.proto
  (:refer-clojure :exclude [read list]))

(defprotocol Store
  (write [store k v opts] "Writes `v` to Store associated with `k`. Returns value's address.")
  (exists? [store address] "Returns true when address exists in Store.")
  (delete [store address] "Remove value associated with `address` from the Store.")
  (read [store address] "Returns value associated with `address`.")
  (list [store prefix] "Returns sequence of keys that match prefix."))
