(ns fluree.store.protocols
  (:refer-clojure :exclude [read list]))

(defprotocol Store
  ;; async
  (exists? [_ path] "Returns true when `path` exists in Store.")
  (write [_ path data] [_ path data opts] "Writes `data` as bytes to Store associated with
    `path`. Returns the `path` as `:path` and the data hash as `:hash`.
  Opts:
    :serializer - overrides default Store serializer.
    :content-address? - caculates the sha256 hash of the data after serializing and appends it to the `path` before storing.")
  (delete [_ path] "Delete data from Store associated with `path`.")
  (read [_ path] [_ path opts] "Reads data from Store associated with `path`.
  Opts:
    :deserializer - override the default Store deserializer.")
  (list [_ prefix] "Returns sequence containing the keys with the prefix `prefix` in Store.")
  (rename [_ old-path new-path] "Remove `old-path` and associate its data to `new-path`.")
  ;; sync
  (address [_ type path] "Given a path and the path's type, returns an address for that path."))
