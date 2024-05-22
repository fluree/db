(ns fluree.db.ledger
  (:require [fluree.db.json-ld.commit-data :as commit-data]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iCommit
  ;; retrieving/updating DBs
  (-commit! [ledger db] [ledger db opts] "Commits a db to a ledger.")
  (-notify [ledger commit-notification] "Notifies of an updated commit for a given ledger, will attempt cached ledger."))

(defprotocol iLedger
  ;; retrieving/updating DBs
  (-db [ledger] [ledger opts] "Returns queryable db with specified options")
  ;; branching
  ;; committing
  (-commit-update! [ledger branch db] "Once a commit completes, update ledger state to reflect.")
  (-status [ledger] [ledger branch] "Returns status for branch (default branch if nil)")
  ;; ledger data across time
  ;; default did
  (-did [ledger] "Returns default did configuration map")
  ;; alias name for graph
  (-alias [ledger] "Returns the ledger local alias / graph name")
  (-address [ledger] "Returns the permanent ledger address")
  (-close [ledger] "Shuts down ledger processes and clears used resources."))

(defn latest-commit
  "Returns latest commit info from branch-data"
  [ledger branch]
  (-> ledger
      (-status branch)
      :commit))

(defn latest-commit-t
  "Returns the latest commit 't' value from branch-data, or 0 (zero) if no commit yet."
  [ledger branch]
  (-> ledger
      (latest-commit branch)
      commit-data/t
      (or 0)))
