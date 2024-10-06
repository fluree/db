(ns fluree.db.storage
  (:require [clojure.string :as str])
  (:refer-clojure :exclude [read list exists?]))

(defn hashable?
  [x]
  (or (string? x)
      #?(:clj (bytes? x))))

(defn sanitize-path
  [path]
  (if (str/starts-with? path "//")
    path
    (str "//" path)))

(defn build-address
  [ns method path]
  (let [path* (sanitize-path path)]
    (str/join ":" [ns method path*])))

(def fluree-namespace "fluree")

(defn build-fluree-address
  [method path]
  (build-address fluree-namespace method path))

(defn parse-address
  [address]
  (let [[ns method path] (str/split address #":")
        local            (if (str/starts-with? path "//")
                           (subs path 2)
                           path)]
    {:ns     ns
     :method method
     :local  local}))

(defn parse-local-path
  [address]
  (-> address parse-address :local))

(defprotocol ContentAddressedStore
  (write [store k v] "Writes `v` to store associated with `k`. Returns value's address.")
  (exists? [store address] "Returns true when address exists in store.")
  (delete [store address] "Remove value associated with `address` from the store.")
  (read [store address] "Returns value associated with `address`.")
  (list [store prefix] "Returns sequence of keys that match prefix."))

(defprotocol ByteStore
  "ByteStore is used by consensus to replicate files across servers"
  (write-bytes [store path bytes] "Async writes bytes to path in store.")
  (read-bytes [store path] "Async read bytes from path in store."))
