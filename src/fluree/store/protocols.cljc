(ns fluree.store.protocols
  (:refer-clojure :exclude [read list]))

(defprotocol Store
  ;; async
  (exists? [_ k] "Returns true when `k` exists in Store.")
  (write [_ k data] [_ k data opts] "Writes `data` as bytes to Store associated with
    `k`. Returns the `k` as `:path` and the data hash as `:hash`.
  Opts:
    :serializer - overrides default Store serializer.
    :content-address? - caculates the sha256 hash of the data after serializing and appends it to the `k` before storing.")
  (delete [_ k] "Delete data from Store associated with `k`.")
  (read [_ k] [_ k opts] "Reads data from Store associated with `k`.
  Opts:
    :deserializer - override the default Store deserializer.")
  (list [_ prefix] "Returns sequence containing the keys with the prefix `prefix` in Store.")
  (rename [_ old-key new-key] "Remove `old-key` and associate its data to `new-key`.")
  ;; sync
  (address [_ type k] "Given a key and the key's type, returns an address for that key."))
