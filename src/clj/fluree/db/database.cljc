(ns fluree.db.database)

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Database
  (query [db q])
  (stage [db tx]))
