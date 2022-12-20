(ns fluree.store.protocols
  (:refer-clojure :exclude [read list]))

(defprotocol Store
  ;; async
  (exists? [_ k] "Returns true when `k` exists in Store.")
  (write [_ k data] "Writes `data` as bytes to Store associated with `k`.")
  (delete [_ k] "Delete data from Store associated with `k`.")
  (read [_ k] "Reads data from Store associated with `k`.")
  (list [_ prefix] "Returns collection containing the keys with the prefix `prefix` in Store.")
  (rename [_ old-key new-key] "Remove `old-key` and associate its data to `new-key`.")
  ;; sync
  (address [_ type k] "Given a key and the key's type, returns an address for that key."))
