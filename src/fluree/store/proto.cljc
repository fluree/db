(ns fluree.store.proto
  (:refer-clojure :exclude [read list]))

(defprotocol Store
  (exists? [store k] "Returns true when `k` exists in Store.")
  (write [store k v opts] "Writes `v` to Store associated with `k`.")
  (read [store k] "Returns value associated with `k`.")
  (list [store prefix] "Returns sequence of keys that match prefix.")
  (delete [store k] "Remove `k` and it's value from the Store."))
