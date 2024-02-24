(ns fluree.db.storage
  (:require [clojure.string :as str])
  (:refer-clojure :exclude [read list exists?]))

(defn hashable?
  [x]
  (or (string? x)
      #?(:clj (bytes? x))))

(defn parse-address
  [address]
  (let [[ns method path] (str/split address #":")
        local            (if (str/starts-with? path "//")
                           (subs path 2)
                           path)]
    {:ns     ns
     :method method
     :local  local}))

(defprotocol Store
  (address [store k] "Returns the address that would be constructed by writing to `k`.")
  (write [store k v opts] "Writes `v` to Store associated with `k`. Returns value's address.")
  (exists? [store address] "Returns true when address exists in Store.")
  (delete [store address] "Remove value associated with `address` from the Store.")
  (read [store address] "Returns value associated with `address`.")
  (list [store prefix] "Returns sequence of keys that match prefix."))
