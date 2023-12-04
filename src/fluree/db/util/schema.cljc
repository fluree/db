(ns fluree.db.util.schema
  (:require [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]))

#?(:clj (set! *warn-on-reflection* true))

(def ^:const schema-sid-start (flake/min-subject-id const/$_predicate))
(def ^:const schema-sid-end (flake/max-subject-id const/$_collection))

(defn is-schema-flake?
  "Returns true if flake is a schema flake."
  [db f]
  (boolean (dbproto/-p-prop db :id (flake/s f))))
