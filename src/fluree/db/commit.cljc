(ns fluree.db.commit)

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iCommit
  ;; retrieving/updating DBs
  (-commit! [db] [ledger-or-db db-or-opts] [ledger db opts] "Commits a db to a ledger."))

(defn normalize-opts
  "Normalizes commit options"
  [opts]
  (if (string? opts)
    {:message opts}
    opts))