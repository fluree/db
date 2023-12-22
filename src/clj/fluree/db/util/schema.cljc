(ns fluree.db.util.schema
  (:require [fluree.db.flake :as flake]))

#?(:clj (set! *warn-on-reflection* true))

(defn is-schema-flake?
  "Returns true if flake is a schema flake."
  [db f]
  (let [pred-map (:pred db)
        s (flake/s f)]
    (contains? pred-map s)))
