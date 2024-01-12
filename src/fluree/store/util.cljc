(ns fluree.store.util
  (:require [clojure.string :as str]))

(defn hashable?
  [x]
  (or (string? x)
      (bytes? x)))

(defn address-parts
  [address]
  (let [[ns method path] (str/split address #":")]
    {:ns ns
     :method method
     :local path}))
