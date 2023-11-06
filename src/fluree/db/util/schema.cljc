(ns fluree.db.util.schema
  (:require [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]))

#?(:clj (set! *warn-on-reflection* true))

(def ^:const schema-sid-start (flake/min-subject-id const/$_predicate))
(def ^:const schema-sid-end (flake/max-subject-id const/$_collection))

(defn is-schema-flake?
  "Returns tru if flake is a schema flake."
  [db f]
  (boolean (dbproto/-p-prop db :id (flake/s f))))
