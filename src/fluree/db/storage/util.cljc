(ns fluree.db.storage.util
  (:require [clojure.string :as str]))

(defn hashable?
  [x]
  (or (string? x)
      #?(:clj (bytes? x))))

(defn address-parts
  [address]
  (let [[ns method path] (str/split address #":")
        local            (if (str/starts-with? path "//")
                           (subs path 2)
                           path)]
    {:ns ns
     :method method
     :local local}))
