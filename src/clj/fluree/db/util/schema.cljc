(ns fluree.db.util.schema
  (:require [fluree.db.flake :as flake]
            [fluree.db.dbproto :as dbproto]))

#?(:clj (set! *warn-on-reflection* true))

(defn is-schema-flake?
  "Returns true if flake is a schema flake."
  [db f]
  (boolean (dbproto/-p-prop db :id (flake/s f))))
