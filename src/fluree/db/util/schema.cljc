(ns fluree.db.util.schema
  (:require [fluree.db.flake :as flake]
            [fluree.db.constants :as const]))

#?(:clj (set! *warn-on-reflection* true))

(def ^:const schema-sid-start (flake/min-subject-id const/$_predicate))
(def ^:const schema-sid-end (flake/max-subject-id const/$_collection))

(defn is-schema-flake?
  "Returns true if flake is a schema flake."
  [f]
  (<= schema-sid-start (flake/s f) schema-sid-end))
